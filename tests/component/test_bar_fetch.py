"""The evening bar fetch: one session's bars, gated before they land.

Every test here builds a lake on disk, runs the sweep against a cassette-backed fake vendor
and a manual clock, and reads the result back off the files a reader would read. Nothing
touches the network, and the fake refuses a window it has no recording for, so a drifted
fetch fails visibly rather than replaying somebody else's window.

Marketlake #280 names twenty-four behaviours and they are the specification. Marketlake
#333's span check ships inside this gate, and tests 19, 21 and 23 are its.

Two properties of the fixtures are worth naming before the tests.

1. **The vendor records what it was asked for.** ``_RecordingVendor`` wraps the replay and
   keeps every call, which is what lets a test assert that a skipped ticker-day reached no
   vendor at all, that a run stopped before a later ticker, and which window each frequency
   asked for. The cassette key carries the window, so a drifted fetch would miss every
   recording, and asserting the recorded call says which window rather than only that one
   was found.
2. **The daily stamps are the fixture's own convention, not a recorded one.** No live
   ``freq=1d`` recording exists yet, so these stamp a daily candle at Eastern midnight of
   its session. That is a fixture choice, and the code reads the session off the stamp
   rather than off the window precisely so the choice can be corrected by a recording
   without moving the rule. Test 24 is written around selection-by-stamp, which is what
   survives whichever instant Schwab turns out to use.
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from lake import bars, journal, report
from lake.actions import CHECK_INSTRUMENT_RESOLUTION, SWEEP_SOURCE
from lake.bars import (
    CHECK_BAR_CLOSE,
    CHECK_BAR_RESPONSE,
    CHECK_BAR_SPAN,
    CLOSE_CROSS_TOLERANCE,
    UnsupportedBarFreq,
    bar_window,
    fetch_session_bars,
)
from lake.calendar import NotASession
from lake.cassette import Cassette
from lake.manifest import manifest_path, read_manifest
from lake.paths import LakePaths, temp_write_path
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.schwab import VendorAuthError
from lake.security_master import KIND_EQUITY, SecurityMaster, master_path
from lake.tickers import Roster
from lake.vendor import DAILY_FREQ, MINUTE_FREQ, VendorError, bars_params
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.vendor import CassetteVendor, bars_candle, bars_interactions

# The session the sweep fetches, and the one after it whose sealed quotes carry the settled
# close the daily bar is judged against. 2026-09-14 is a Monday, so the week is an ordinary
# one and the calendar-next session is the following day.
SESSION = date(2026, 9, 14)
FOLLOWING = date(2026, 9, 15)
MONDAY = date(2026, 9, 14)

# 20:00 ET on the session, which is when the evening sweep runs. The second night is a day
# later and a minute further into the evening, not the same minute: a withheld file is named
# by its Eastern time of day, so two runs holding the same finding for one ticker-day at the
# same time of day would collide on one name. A real clock separates them by microseconds and
# a manual one does not.
FIRST_NIGHT = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
SECOND_NIGHT = datetime(2026, 9, 16, 0, 1, tzinfo=UTC)

# The session's own bounds, spelled the way the fake calendar serves them.
OPEN_ET = datetime.fromisoformat("2026-09-14T09:30:00-04:00")
CLOSE_ET = datetime.fromisoformat("2026-09-14T16:00:00-04:00")
DAY_MARGIN = timedelta(days=1)

# The close the lake settled for the session, carried on the *next* session's rows. The bar's
# own close matches it exactly here, so a test that wants a disagreement moves one of the two.
SETTLED_CLOSE = 650.00

RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# The two ends of the bracket the tolerance was measured from, as fractions rather than as
# multiples of the constant. A test that built its disagreement out of ``CLOSE_CROSS_TOLERANCE``
# would move with the constant and so could never catch a tolerance widened past what it must
# refuse, which is the whole point of having one.
#
# ``SETTLED_DRIFT`` is the largest drift measured between the two close cycles, QQQ's 1.13
# basis points on 2026-09-14, and the check has to absorb it. ``PER_EVENT_DIVIDEND`` is the
# smallest adjustment the check has to catch, QQQ's per-event dividend at 11.47 basis points
# off the lake's own rows.
SETTLED_DRIFT = 1.13e-4
PER_EVENT_DIVIDEND = 11.47e-4

# The quotes columns the loader needs plus the one the close cross-check reads. A fixture
# schema rather than the pinned capture schema, carrying what these tests read and no more.
QUOTES_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("ticker", pa.string()),
        ("last", pa.float64()),
        ("close_price", pa.float64()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("close_tag", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)


def _quote_row(
    day: date,
    *,
    ticker: str = "SPY",
    close_price: float | None = SETTLED_CLOSE,
    close_tag: str | None = "spot_close",
    row_kind: str = "data",
) -> dict:
    """One quotes row at the session's equity close, carrying the settled close."""
    return {
        "snap_ts": f"{day.isoformat()}T20:00:00+00:00",
        "fetch_ts": f"{day.isoformat()}T20:00:00.300+00:00",
        "ticker": ticker,
        "last": 649.0,
        "close_price": close_price,
        "row_kind": row_kind,
        "error_class": None if row_kind == "data" else "vendor_auth_error",
        "close_tag": close_tag,
        "schema_version": 1,
        "extra": None,
    }


def _gap_row(day: date, ticker: str = "SPY") -> dict:
    """A gap row: a minute the cycle attempted and missed, every vendor column null.

    A day of these is what the lake's 2026-09-08 through 2026-09-11 hold from a real auth
    outage, and it is what makes ``load_quotes`` raise ``NoSpotClose``.
    """
    return _quote_row(day, ticker=ticker, close_price=None, close_tag=None, row_kind="gap")


def _quotes_table(rows: list[dict]) -> pa.Table:
    columns = {name: [row.get(name) for row in rows] for name in QUOTES_SCHEMA.names}
    return pa.table(columns, schema=QUOTES_SCHEMA)


def _ledger_table() -> pa.Table:
    """The schema-version ledger at the shape the running code writes."""
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _master(tickers: tuple[str, ...] = ("SPY",), *, valid_from: date = date(2026, 9, 8)):
    """A master holding each ticker from ``valid_from``, the way the live lake's does."""
    master = SecurityMaster()
    for ticker in tickers:
        master.register(
            kind=KIND_EQUITY,
            capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
            valid_from=valid_from,
            ticker=ticker,
        )
    return master


def _minute_candles(
    *,
    first: datetime = OPEN_ET,
    last: datetime = CLOSE_ET - timedelta(minutes=1),
    close: float = SETTLED_CLOSE,
) -> list[dict]:
    """A minute response's candles: the window's first minute and its last.

    Two candles, not 390. A real Schwab response's density is unknown, which is why the span
    check compares the ends rather than counting, and the committed cassette carries three
    candles for the same 390-minute window.
    """
    return [
        bars_candle(first, open_=648.0, high=649.0, low=647.5, close=648.5, volume=1_400_000),
        bars_candle(last, open_=649.5, high=650.5, low=649.0, close=close, volume=2_100_000),
    ]


def _daily_candle(session: date = SESSION, *, close: float = SETTLED_CLOSE) -> dict:
    """One daily candle, stamped at Eastern midnight of its session.

    The stamp is the fixture's convention, since no live daily recording exists yet. What the
    code reads off it is the session, so a recording that shows a different instant moves this
    line and nothing in ``lake.bars``.
    """
    when = datetime.fromisoformat(f"{session.isoformat()}T00:00:00-04:00")
    return bars_candle(when, open_=645.0, high=651.0, low=644.0, close=close, volume=70_000_000)


def _daily_window() -> tuple[datetime, datetime]:
    """The bracket a ``1d`` fetch asks for, spelled the way the production window builder is."""
    return OPEN_ET - DAY_MARGIN, CLOSE_ET + DAY_MARGIN


def _cassette(
    *,
    minute: dict | None = None,
    daily: dict | None = None,
    tickers: tuple[str, ...] = ("SPY",),
) -> Cassette:
    """A cassette holding one recording per ticker and frequency.

    ``minute`` and ``daily`` name the keyword arguments each frequency's window is recorded
    with, so a test that wants a short response, an empty one or a non-2xx passes them here
    rather than assembling interactions by hand.
    """
    minute = {"candles": _minute_candles()} if minute is None else minute
    daily = {"candles": [_daily_candle()]} if daily is None else daily
    start, end = _daily_window()
    interactions: list = []
    for ticker in tickers:
        interactions.extend(
            bars_interactions(
                ticker,
                MINUTE_FREQ,
                [(OPEN_ET, CLOSE_ET, minute["candles"])],
                **{k: v for k, v in minute.items() if k != "candles"},
            )
        )
        interactions.extend(
            bars_interactions(
                ticker,
                DAILY_FREQ,
                [(start, end, daily["candles"])],
                **{k: v for k, v in daily.items() if k != "candles"},
            )
        )
    return Cassette(interactions=tuple(interactions))


class _RecordingVendor:
    """A ``Vendor`` that replays a cassette and keeps every call it was asked for.

    The recorded calls are what several tests assert against: that a skipped ticker-day
    reached no vendor, that a stopped run never reached a later ticker, and which window each
    frequency asked for. The cassette key carries the window, so a drifted fetch would raise a
    ``CassetteError`` rather than replaying the wrong recording, and this makes the window an
    assertion rather than only a precondition.

    ``fail_with`` raises for a named ticker instead of replaying, which is how the auth-death
    and vendor-failure tests drive their conditions.
    """

    def __init__(self, cassette: Cassette, *, fail_with: dict | None = None) -> None:
        self._replay = CassetteVendor(cassette)
        self._fail_with = dict(fail_with or {})
        self.calls: list[dict] = []
        self.lock_free: list[bool] = []

    def _record(self, symbol: str, freq: str, start, end, root: Path | None = None) -> None:
        self.calls.append(bars_params(symbol, freq, start=start, end=end))

    def get_minute_bars(self, symbol, *, start, end, extended_hours=None, previous_close=None):
        self._record(symbol, MINUTE_FREQ, start, end)
        if symbol in self._fail_with:
            raise self._fail_with[symbol]
        return self._replay.get_minute_bars(
            symbol,
            start=start,
            end=end,
            extended_hours=extended_hours,
            previous_close=previous_close,
        )

    def get_daily_bars(self, symbol, *, start, end, extended_hours=None, previous_close=None):
        self._record(symbol, DAILY_FREQ, start, end)
        if symbol in self._fail_with:
            raise self._fail_with[symbol]
        return self._replay.get_daily_bars(
            symbol,
            start=start,
            end=end,
            extended_hours=extended_hours,
            previous_close=previous_close,
        )

    def get_chain(self, *args, **kwargs):  # pragma: no cover - the sweep never fetches a chain
        raise AssertionError("the bar sweep fetched a chain")

    def get_quotes(self, *args, **kwargs):  # pragma: no cover - nor a quote
        raise AssertionError("the bar sweep fetched a quote")

    def token_mint_time(self):  # pragma: no cover - nor the token
        raise AssertionError("the bar sweep read the token mint time")


def _roster(mapping: dict[str, list[str]], *, enabled: dict[str, bool] | None = None) -> Roster:
    """A roster naming each ticker's bar frequencies."""
    enabled = enabled or {}
    return Roster.from_mapping(
        {
            ticker: {"options": True, "bars": freqs, "enabled": enabled.get(ticker, True)}
            for ticker, freqs in mapping.items()
        }
    )


def _lake(
    fixture_lake: FixtureLake,
    *,
    quotes: dict[tuple[str, date], list[dict]] | None = None,
    master: SecurityMaster | None = None,
    bars_partitions: tuple[tuple[str, str, date, pa.Table], ...] = (),
) -> Path:
    """A lake holding the next session's sealed quotes, the ledger, and the master.

    ``quotes`` defaults to SPY's following session carrying the settled close, which is what
    the close cross-check reads. ``bars_partitions`` seals bars beside them, for the tests
    about the manifested skip.
    """
    if quotes is None:
        quotes = {("SPY", FOLLOWING): [_quote_row(FOLLOWING)]}
    for (ticker, day), rows in quotes.items():
        fixture_lake.with_quotes(ticker, day, _quotes_table(rows))
    for ticker, freq, day, table in bars_partitions:
        fixture_lake.with_bars(ticker, freq, day, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    (master if master is not None else _master()).write(master_path(root))
    return root


def _run(
    root: Path,
    vendor,
    *,
    roster: Roster | None = None,
    now: datetime = FIRST_NIGHT,
    session: date | None = SESSION,
):
    """One sweep run over a fixture lake, with every seam injected."""
    return fetch_session_bars(
        lake_root=root,
        vendor=vendor,
        clock=ManualClock(now),
        calendar=weekday_sessions(MONDAY),
        roster=roster if roster is not None else _roster({"SPY": ["1d"]}),
        session=session,
    )


def _findings(root: Path, day: date = SESSION) -> list[dict]:
    """Every withheld finding filed for one ticker-day, read back off the files."""
    directory = report.withheld_dir(root, day)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


def _partition(root: Path, ticker: str, freq: str, day: date = SESSION) -> Path:
    return LakePaths(root).bars_partition_path(ticker, freq, day)


def _entries(root: Path, rel: str) -> list[dict]:
    return [entry for entry in read_manifest(root) if entry["partition"] == rel]


def _bars_table(rows: int = 1) -> pa.Table:
    """A already-sealed bars partition, in the pinned schema, for the skip tests."""
    built = journal.bars_rows(
        {"candles": [_daily_candle()], "symbol": "SPY", "empty": False},
        ticker="SPY",
        freq=DAILY_FREQ,
        instrument_id=1,
        fetch_ts=FIRST_NIGHT,
        fetch_end_ts=FIRST_NIGHT,
        window_start=OPEN_ET,
        window_end=CLOSE_ET,
        extended_hours=None,
    )
    return pa.Table.from_batches([journal.bars_data_batch(built[:rows])])


# -- 1 and 2. the manifested skip -------------------------------------------------------


def test_a_manifested_ticker_day_is_skipped_with_no_vendor_call(fixture_lake: FixtureLake):
    """#280 test 1.

    The skip is what keeps a landed partition from being re-fetched, and the vendor call is
    the cost it saves. ``guard_row_count`` catches nothing here, since it refuses only an
    append that would shrink a manifested partition, so a re-fetch with the same row count and
    different values would supersede the entry silently.
    """
    root = _lake(fixture_lake, bars_partitions=(("SPY", DAILY_FREQ, SESSION, _bars_table()),))
    vendor = _RecordingVendor(_cassette())

    result = _run(root, vendor)

    assert vendor.calls == [], "a manifested ticker-day reached the vendor"
    assert result.skipped == 1
    assert result.attempted == 0
    assert result.landed == () and result.held == ()


def test_a_second_run_lands_nothing_and_says_so(fixture_lake: FixtureLake):
    """#280 test 2.

    The count is what separates a run that had already done the work from a run that found
    nothing to walk. Both land nothing and hold nothing, and only ``skipped`` tells them apart.
    """
    root = _lake(fixture_lake)
    first = _run(root, _RecordingVendor(_cassette()))
    assert len(first.landed) == 1 and first.skipped == 0

    vendor = _RecordingVendor(_cassette())
    second = _run(root, vendor, now=SECOND_NIGHT)

    assert second.landed == () and second.held == ()
    assert second.skipped == 1, "a second run cannot be told from one that walked nothing"
    assert vendor.calls == []
    assert "skipped: 1" in second.render()
    # An empty roster walks nothing at all, which is the run the count above separates from.
    nothing = _run(root, _RecordingVendor(_cassette()), roster=_roster({}), now=SECOND_NIGHT)
    assert nothing.landed == () and nothing.held == () and nothing.skipped == 0


# -- 3, 4 and 5. the close cross-check --------------------------------------------------


def test_a_close_disagreeing_past_the_tolerance_is_held_and_lands_nothing(
    fixture_lake: FixtureLake,
):
    """#280 test 3.

    A pre-adjusted bar disagrees with minutely quotes the lake sealed before any adjustment
    could reach them, which is what this check exists to catch. The partition never lands,
    because the shape is gate-before-land.
    """
    drifted = SETTLED_CLOSE * (1 + PER_EVENT_DIVIDEND)
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette(daily={"candles": [_daily_candle(close=drifted)]}))

    result = _run(root, vendor)

    assert result.landed == ()
    assert not _partition(root, "SPY", DAILY_FREQ).exists()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_CLOSE
    assert held.finding.computed == pytest.approx(drifted)
    assert held.finding.against == pytest.approx(SETTLED_CLOSE)
    (filed,) = _findings(root)
    assert filed["check"] == CHECK_BAR_CLOSE
    assert filed["day"] == SESSION.isoformat()
    assert filed["event"] == DAILY_FREQ, "the finding's key is the frequency, not an event"


def test_a_close_inside_the_tolerance_lands_and_is_manifested_under_the_sweep_source(
    fixture_lake: FixtureLake,
):
    """#280 test 4.

    The entry's sha is read off the bytes on disk at record time, so a partition whose file
    and entry disagree is a torn write rather than a passing test.
    """
    inside = SETTLED_CLOSE * (1 + SETTLED_DRIFT)
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette(daily={"candles": [_daily_candle(close=inside)]}))

    result = _run(root, vendor)

    (landed,) = result.landed
    assert result.held == ()
    partition = _partition(root, "SPY", DAILY_FREQ)
    assert partition.exists()
    rel = partition.relative_to(root).as_posix()
    assert landed.partition == rel
    (entry,) = _entries(root, rel)
    assert entry["source"] == SWEEP_SOURCE
    assert entry["rows"] == 1
    import hashlib

    assert entry["sha256"] == hashlib.sha256(partition.read_bytes()).hexdigest()


def test_a_held_bar_files_again_on_the_next_run(fixture_lake: FixtureLake):
    """#280 test 5.

    Nothing settles the disagreement, so the next night re-derives it from the same rows. The
    repetition is the record: one file says a finding was held at some point, and two say it
    was held again last night. The partition never lands, so the manifested skip never starts
    passing over it, which is what makes the repeat possible at all.

    The second night's clock is a minute further into the evening, not the same minute. A
    withheld file's name carries its Eastern time of day to the microsecond, and a manual
    clock at one slot time composes the same name twice, whose second write raises
    ``FileExistsError``.
    """
    drifted = SETTLED_CLOSE * (1 + PER_EVENT_DIVIDEND)
    root = _lake(fixture_lake)
    cassette = _cassette(daily={"candles": [_daily_candle(close=drifted)]})

    first = _run(root, _RecordingVendor(cassette))
    second = _run(root, _RecordingVendor(cassette), now=SECOND_NIGHT)

    assert len(first.held) == len(second.held) == 1
    assert first.held[0].filed_at != second.held[0].filed_at
    assert len(_findings(root)) == 2, "the repeat is the record, so the second night files too"
    assert not _partition(root, "SPY", DAILY_FREQ).exists()


# -- 6 and 7. the no-source rule --------------------------------------------------------


def test_a_session_whose_quotes_have_no_spot_close_is_held_and_the_run_goes_on(
    fixture_lake: FixtureLake,
):
    """#280 test 6.

    A session with gap rows and no data row has no comparison at all. The rule is written over
    ``LoadError`` rather than over this refusal, because three causes reach it and each would
    otherwise need its own branch.

    The run continues to the next ticker, which is the containment: one unreadable session is
    not the other tickers' bars to lose.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_gap_row(FOLLOWING)],
            ("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")],
        },
        master=_master(("SPY", "QQQ")),
    )
    vendor = _RecordingVendor(_cassette(tickers=("SPY", "QQQ")))

    result = _run(root, vendor, roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}))

    (held,) = result.held
    assert held.finding.symbol == "SPY"
    assert held.finding.check == CHECK_BAR_CLOSE
    assert "NoSpotClose" in (held.finding.exception or "")
    (landed,) = result.landed
    assert landed.ticker == "QQQ", "one unreadable session cost the rest of the run"
    assert not _partition(root, "SPY", DAILY_FREQ).exists()


def test_a_session_with_no_sealed_quotes_partition_is_contained_the_same_way(
    fixture_lake: FixtureLake,
):
    """#280 test 7.

    Tonight's session has no next partition yet, which this job's first real run meets: the
    following session has not happened, so compaction has sealed nothing for it. The rule is
    the same one, written once over ``LoadError``, and this is the half that settles itself:
    tomorrow's seal makes the comparison available and the next run lands the bar.
    """
    root = _lake(
        fixture_lake,
        quotes={("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")]},
        master=_master(("SPY", "QQQ")),
    )
    vendor = _RecordingVendor(_cassette(tickers=("SPY", "QQQ")))

    result = _run(root, vendor, roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}))

    (held,) = result.held
    assert held.finding.symbol == "SPY"
    assert "PartitionAbsent" in (held.finding.exception or "")
    assert [landed.ticker for landed in result.landed] == ["QQQ"]


# -- 8. the instrument resolution -------------------------------------------------------


def test_a_ticker_the_master_cannot_resolve_files_and_still_lands_its_row(
    fixture_lake: FixtureLake,
):
    """#280 test 8.

    ``instrument_id`` is nullable, so the schema already answers what becomes of a row whose
    ticker the master cannot place. The finding is filed because the master and the capture
    spans disagreeing is a reference-data fault worth reading, and the bar is not held for it
    because holding a bar over a null join key would cost the price to save the key.
    """
    root = _lake(fixture_lake, master=_master(("QQQ",)))
    vendor = _RecordingVendor(_cassette())

    result = _run(root, vendor)

    (landed,) = result.landed
    assert landed.ticker == "SPY"
    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert table.column("instrument_id").to_pylist() == [None]
    (held,) = result.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION
    assert "UnresolvedSymbol" in (held.finding.exception or "")


# -- 9. the empty window ----------------------------------------------------------------


def test_an_empty_window_lands_nothing_and_is_refused_under_the_response_check(
    fixture_lake: FixtureLake,
):
    """#280 test 9.

    A session that yields no bars has two wrong answers and this picks one. A zero-row
    partition satisfies the manifested skip forever, so the session's bars would never be
    fetched again and nothing would say the file is empty because the vendor sent nothing.
    Landing nothing leaves the ticker-day to the next run, and the second run below is what
    shows the rule's implication: it fetches again rather than passing over a sealed file.

    This walk only ever fetches sessions, so an empty response here is a session that traded
    and came back with nothing. ``Calendar.is_session`` told a holiday apart before the
    request went out, which is what the payload cannot do.
    """
    root = _lake(fixture_lake)
    cassette = _cassette(daily={"candles": []})

    first = _run(root, _RecordingVendor(cassette))

    assert first.landed == ()
    assert not _partition(root, "SPY", DAILY_FREQ).exists()
    (held,) = first.held
    assert held.finding.check == CHECK_BAR_RESPONSE
    assert held.finding.computed == 0.0

    second_vendor = _RecordingVendor(cassette)
    second = _run(root, second_vendor, now=SECOND_NIGHT)
    assert second.skipped == 0, "an empty window left a partition the skip passed over"
    assert second.attempted == 1 and len(second_vendor.calls) == 1


# -- 10. the non-2xx response -----------------------------------------------------------


def test_a_non_2xx_response_lands_no_partition_and_is_refused_by_name(
    fixture_lake: FixtureLake,
):
    """#280 test 10.

    Nothing raises on a non-2xx: the seam returns the vendor's status verbatim, so a 401 or a
    429 arrives as a ``VendorResponse`` whose body carries no candles. Capture's answer is a
    gap row carrying ``http_<status>``, and bars have no gap rows, so there is no row for one
    to land in. It is refused before anything reads a body that is not a payload, which is why
    the finding names the status rather than a coverage figure.
    """
    from lake.cassette import Interaction

    start, end = _daily_window()
    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="bars",
                params=bars_params("SPY", DAILY_FREQ, start=start, end=end),
                status=429,
                body={},
            ),
        )
    )
    root = _lake(fixture_lake)

    result = _run(root, _RecordingVendor(cassette))

    assert result.landed == ()
    assert not _partition(root, "SPY", DAILY_FREQ).exists()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_RESPONSE
    assert "http_429" in (held.finding.exception or "")


# -- 11. auth death ---------------------------------------------------------------------


def test_a_vendor_auth_error_stops_the_run_before_a_later_ticker(fixture_lake: FixtureLake):
    """#280 test 11.

    A dead refresh token fails every remaining ticker-day identically, so containing it per
    ticker would turn one auth death into a page of held findings under an exit code reading
    "some findings held" rather than "the token is dead". ``VendorAuthError`` is not a
    ``VendorError`` subclass, so a catch that named only the base class would already miss it,
    and this asserts the run stops rather than that it merely does not file.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
            ("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")],
        },
        master=_master(("SPY", "QQQ")),
    )
    vendor = _RecordingVendor(
        _cassette(tickers=("SPY", "QQQ")), fail_with={"SPY": VendorAuthError("refresh failed")}
    )

    with pytest.raises(VendorAuthError):
        _run(root, vendor, roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}))

    assert [call["symbol"] for call in vendor.calls] == ["SPY"], "the run reached a later ticker"
    assert _findings(root) == [], "auth death filed a finding instead of stopping the run"


def test_an_ordinary_vendor_failure_is_contained_to_its_ticker(fixture_lake: FixtureLake):
    """The other side of the same catch.

    A ``VendorError`` is this request failing rather than the credentials dying, so it is
    contained and the walk goes on. Without both tests the catch could be narrowed to nothing
    or widened to everything and one of them would still pass.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
            ("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")],
        },
        master=_master(("SPY", "QQQ")),
    )
    vendor = _RecordingVendor(
        _cassette(tickers=("SPY", "QQQ")), fail_with={"SPY": VendorError("one bad request")}
    )

    result = _run(root, vendor, roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}))

    assert [call["symbol"] for call in vendor.calls] == ["SPY", "QQQ"]
    assert [landed.ticker for landed in result.landed] == ["QQQ"]
    assert len(result.held) == 1 and result.held[0].finding.symbol == "SPY"


# -- 12. a frequency the seam cannot fetch ----------------------------------------------


def test_a_roster_frequency_outside_the_seam_is_refused_without_reaching_the_vendor(
    fixture_lake: FixtureLake,
):
    """#280 test 12.

    ``onboard --bars`` takes any string and ``TickerConfig.bars`` stores it unvalidated, so a
    roster is free to carry a frequency nothing can fetch. It is refused before the walk
    starts rather than when the walk reaches it: the fault is identical for every ticker-day
    it names, so holding it per ticker-day would file the same finding every night forever,
    and raising it mid-walk would leave a run half done for a fault nothing in that run caused.

    Checking up front is also what makes "without reaching the vendor" true for every ticker
    rather than only for the one carrying the bad frequency.
    """
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette())

    with pytest.raises(UnsupportedBarFreq) as caught:
        _run(root, vendor, roster=_roster({"SPY": ["1d"], "QQQ": ["5m"]}))

    assert caught.value.freq == "5m" and caught.value.ticker == "QQQ"
    assert vendor.calls == [], "a roster the run refuses still reached the vendor"
    assert not _partition(root, "SPY", DAILY_FREQ).exists()


# -- 13. the lock and the atomic write --------------------------------------------------


def _lock_is_free(root: Path) -> bool:
    """Whether the lake-root lock can be taken right now, without waiting for it.

    ``LOCK_NB`` is what makes this a question rather than a wait. ``flock`` conflicts between
    two descriptors even inside one process, so asking the blocking way from inside the run
    would park this process behind itself.
    """
    fd = os.open(manifest_path(root), os.O_RDONLY | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


def test_the_write_and_its_entry_land_inside_one_lock_hold_and_the_fetch_outside_it(
    fixture_lake: FixtureLake, monkeypatch
):
    """#280 test 13.

    Both halves matter and neither alone carries it. The write and the manifest entry are
    inside, because ``manifest.py`` rests the backup watermark on every lake write appending
    its entry under this lock, and a writer outside it would make that false. The fetch is
    outside, because ``lake_lock`` blocks and a vendor round trip inside it would put a
    network call in front of capture's per-minute append.

    The temp path is asserted too, because "atomic" is the rename and a writer that wrote
    straight to the partition would leave a torn Parquet on a crash with nothing to say so.
    """
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette())
    seen: dict[str, object] = {}

    real_write = pa.parquet.write_table

    def spy_write(table, where, *args, **kwargs):
        seen["write_path"] = Path(where)
        seen["write_locked"] = not _lock_is_free(root)
        return real_write(table, where, *args, **kwargs)

    real_record = bars.record_partition

    def spy_record(lake_root, partition, **kwargs):
        seen["record_locked"] = not _lock_is_free(root)
        return real_record(lake_root, partition, **kwargs)

    real_fetch = bars._fetch

    def spy_fetch(vendor_, ticker, window):
        seen["fetch_locked"] = not _lock_is_free(root)
        return real_fetch(vendor_, ticker, window)

    monkeypatch.setattr(bars.pq, "write_table", spy_write)
    monkeypatch.setattr(bars, "record_partition", spy_record)
    monkeypatch.setattr(bars, "_fetch", spy_fetch)

    result = _run(root, vendor)

    assert len(result.landed) == 1
    assert seen["fetch_locked"] is False, "the vendor was called while the lake lock was held"
    assert seen["write_locked"] is True, "the partition was written outside the lock"
    assert seen["record_locked"] is True, "the manifest entry was appended outside the lock"
    partition = _partition(root, "SPY", DAILY_FREQ)
    assert seen["write_path"] == temp_write_path(partition, os.getpid())
    assert seen["write_path"] != partition, "the write went straight at the partition path"
    assert not seen["write_path"].exists(), "the temp file outlived the rename"
    assert _lock_is_free(root), "the run kept the lock after it finished"


# -- 14. the command --------------------------------------------------------------------


def _tickers_file(tmp_path: Path, body: str = "SPY: {options: true, bars: [1d]}\n") -> Path:
    path = tmp_path / "tickers.yaml"
    path.write_text(body)
    return path


def test_the_command_against_a_lake_with_no_master_exits_two_with_a_named_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#280 test 14, first half.

    An unseeded lake is an operator mistake with a one-command fix, and a traceback names the
    wrong thing. The line names the command that fixes it.
    """
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    config = write_config(tmp_path, root)

    code = bars.main(
        [
            "--config",
            str(config),
            "--tickers",
            str(_tickers_file(tmp_path)),
        ],
        clock=ManualClock(FIRST_NIGHT),
        vendor_factory=lambda *a, **k: _RecordingVendor(_cassette()),
    )

    assert code == 2
    printed = capsys.readouterr()
    assert printed.out == ""
    assert printed.err.startswith("bars: no security master at")
    assert "python -m lake.onboard" in printed.err
    assert "Traceback" not in printed.err


def test_the_command_exits_one_when_a_finding_could_not_be_filed(
    fixture_lake: FixtureLake, tmp_path: Path, monkeypatch, capsys
):
    """#280 test 14, second half.

    The walk contains an unwritable report so one file does not cost the other tickers their
    bars, and this is where that stops being silent. A run that held something and filed
    nothing reads exactly like a run that found nothing, which is the silence the producer
    exists to break.
    """
    drifted = SETTLED_CLOSE * (1 + PER_EVENT_DIVIDEND)
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)

    def refuse(*args, **kwargs):
        raise PermissionError("reports/ is read-only")

    monkeypatch.setattr(bars, "write_withheld", refuse)

    code = bars.main(
        [
            "--config",
            str(config),
            "--tickers",
            str(_tickers_file(tmp_path)),
        ],
        clock=ManualClock(FIRST_NIGHT),
        vendor_factory=lambda *a, **k: _RecordingVendor(
            _cassette(daily={"candles": [_daily_candle(close=drifted)]})
        ),
    )

    assert code == 1
    printed = capsys.readouterr()
    assert "NOT filed: PermissionError" in printed.out
    assert "could not be filed: PermissionError" in printed.err


def test_the_command_runs_the_sweep_and_reports_what_it_did(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """The ordinary run, which is the only way to exercise this until #281 schedules it."""
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)

    code = bars.main(
        [
            "--config",
            str(config),
            "--tickers",
            str(_tickers_file(tmp_path)),
        ],
        clock=ManualClock(FIRST_NIGHT),
        vendor_factory=lambda *a, **k: _RecordingVendor(_cassette()),
    )

    assert code == 0
    printed = capsys.readouterr().out
    assert "landed:  1" in printed
    assert "held:    0" in printed
    assert "skipped: 0" in printed
    assert "SPY 1d" in printed


def test_the_command_refuses_a_day_that_is_not_a_session(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """An empty response cannot tell a holiday apart from a session that yielded nothing.

    ``Calendar.is_session`` can, so the refusal happens before the request rather than after
    the payload, which is what makes the empty-window rule above mean a session that traded.

    The session comes from the clock, so a clock sitting on the Sunday is what drives it.
    """
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)
    # 20:00 ET on 2026-09-13, a Sunday.
    sunday_evening = datetime(2026, 9, 14, 0, 0, tzinfo=UTC)

    code = bars.main(
        ["--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(sunday_evening),
        vendor_factory=lambda *a, **k: _RecordingVendor(_cassette()),
    )

    assert code == 2
    assert "is not a session" in capsys.readouterr().err


def test_the_command_names_no_session_of_its_own(fixture_lake: FixtureLake, tmp_path: Path):
    """There is no ``--date``, and its absence is the decision rather than an omission.

    A flag naming the session reads as a convenience and is a backfill selector. Nothing in
    this job consults a capture-span floor, and the security master is not the barrier it
    looks like: a session it cannot place files a finding and lands the row anyway with a null
    ``instrument_id``. So the flag would have let one typo land bars for a session the lake
    never captured, and the design says there is no backfill anywhere in the implementation.
    marketlake #319 owns the floor and the span of sessions a run covers.
    """
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)
    with pytest.raises(SystemExit):
        bars.main(
            [
                "--config",
                str(config),
                "--tickers",
                str(_tickers_file(tmp_path)),
                "--date",
                "2026-09-14",
            ],
            clock=ManualClock(FIRST_NIGHT),
            vendor_factory=lambda *a, **k: _RecordingVendor(_cassette()),
        )


def test_a_day_that_is_not_a_session_raises_from_the_core_too(fixture_lake: FixtureLake):
    """The same refusal under the core, so the rule is not the command's alone."""
    root = _lake(fixture_lake)
    with pytest.raises(NotASession):
        _run(root, _RecordingVendor(_cassette()), session=date(2026, 9, 13))


# -- 15. what judges a 1m partition -----------------------------------------------------


def test_a_minute_partition_is_judged_by_the_span_check_and_not_the_daily_verdict(
    fixture_lake: FixtureLake,
):
    """#280 test 15, said plainly.

    The close cross-check speaks to ``1d`` alone: it compares the official daily close, which
    is the closing-auction print, and a single minute has no such figure. So a ``1m`` partition
    is not judged on the daily verdict and it does not land unjudged either. The span check is
    what judges it, and it is a row-count check in all but name: it says the partition covers
    exactly the window that was asked for, ending at 15:59 because a candle is stamped at its
    minute's open.

    Both halves are asserted. A short ``1m`` response is refused with no daily bar anywhere in
    the run, so nothing inherited a verdict, and a complete one lands.
    """
    root = _lake(fixture_lake)
    short = {"candles": _minute_candles(last=CLOSE_ET - timedelta(minutes=30))}
    result = _run(root, _RecordingVendor(_cassette(minute=short)), roster=_roster({"SPY": ["1m"]}))

    assert result.landed == ()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN
    assert held.finding.event == MINUTE_FREQ

    whole = _run(
        root, _RecordingVendor(_cassette()), roster=_roster({"SPY": ["1m"]}), now=SECOND_NIGHT
    )
    (landed,) = whole.landed
    assert landed.freq == MINUTE_FREQ and whole.held == ()


# -- 16. the windows --------------------------------------------------------------------


def test_each_frequency_asks_for_exactly_the_window_this_issue_names(fixture_lake: FixtureLake):
    """#280 test 16, asserted off the vendor's recorded request.

    The ``1m`` window is the session itself. That is a choice rather than a default: both
    vendor flags are left unset, so Schwab decides whether a price-history response covers the
    regular session or the extended one, and nobody has recorded which it picks. At the
    session's own bounds it does not matter, because the request's bounds clip the response
    either way. Wider, it would matter: Schwab answering with the regular session would be
    correct and short at once, and the span check would stall the run pointing at the wrong
    cause.

    The ``1d`` window is a day wider on each side, because a daily candle stamped at 00:00 UTC
    is 20:00 Eastern on the previous day, so even a whole-Eastern-day window can miss the
    session it asked for.
    """
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette())

    _run(root, vendor, roster=_roster({"SPY": ["1m", "1d"]}))

    minute, daily = vendor.calls
    assert minute["freq"] == MINUTE_FREQ
    assert minute["start"] == OPEN_ET.astimezone(UTC).isoformat()
    assert minute["end"] == CLOSE_ET.astimezone(UTC).isoformat()
    # Both flags are omitted from the key, exactly as the real vendor omits them from the
    # request, so the recording a live run takes replays for this fetch.
    assert set(minute) == {"symbol", "freq", "start", "end"}
    start, end = _daily_window()
    assert daily["freq"] == DAILY_FREQ
    assert daily["start"] == start.astimezone(UTC).isoformat()
    assert daily["end"] == end.astimezone(UTC).isoformat()


def test_the_window_builder_names_the_same_two_windows(fixture_lake: FixtureLake):
    """The same rule under the builder, so a test of it does not have to run a whole sweep."""
    calendar = weekday_sessions(MONDAY)
    from lake.session import SessionClock

    bounds = SessionClock(ManualClock(FIRST_NIGHT), calendar).bounds(SESSION)

    minute = bar_window(MINUTE_FREQ, bounds)
    assert (minute.start, minute.end) == (OPEN_ET, CLOSE_ET)
    assert (minute.end - minute.start) == timedelta(minutes=390)
    daily = bar_window(DAILY_FREQ, bounds)
    assert (daily.start, daily.end) == _daily_window()
    assert daily.session == SESSION


# -- 17. a refused candle stamp ---------------------------------------------------------


def test_a_candle_whose_stamp_the_transform_refuses_never_lands_a_row(
    fixture_lake: FixtureLake,
):
    """#280 test 17.

    ``bar_ts`` is the one non-null column in the journal module, and ``pq.write_table`` refuses
    a table rather than a row, so a single null stamp would cost the whole partition's write.
    The rule this issue picks is to drop the candle. The chains trade of nulling the stamp and
    overflowing the raw value under its vendor name is the one option the schema forbids.

    The stamp here is a boolean, which is the shape that matters: ``float(True)`` is ``1.0``,
    so a check or a builder doing its own arithmetic would land it as a plausible 1970
    timestamp with nothing raised and nothing to read it by.
    """
    good = _daily_candle()
    bad = dict(_daily_candle(), datetime=True)
    root = _lake(fixture_lake)

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [bad, good]})))

    (landed,) = result.landed
    assert landed.rows == 1, "the refused candle became a row"
    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert table.column("bar_ts").null_count == 0
    assert table.num_rows == 1


# -- 18. the columns --------------------------------------------------------------------


def test_a_written_partition_carries_exactly_the_pinned_columns(fixture_lake: FixtureLake):
    """#280 test 18, asserted against ``journal.schema_for`` rather than a list restated here.

    A list restated in a test drifts from the schema silently, and the schema is the thing two
    other deliverables read.
    """
    root = _lake(fixture_lake)

    _run(root, _RecordingVendor(_cassette()))

    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert table.schema.equals(journal.schema_for(journal.BARS_SURFACE))
    row = table.to_pylist()[0]
    assert row["ticker"] == "SPY" and row["freq"] == DAILY_FREQ
    assert row["schema_version"] == journal.SCHEMA_VERSION
    # The window and the flag are recorded, which is what turns this issue's window choice
    # from a decision that has to be right forever into one that can change.
    start, end = _daily_window()
    assert row["window_start"] == start.isoformat()
    assert row["window_end"] == end.isoformat()
    assert row["extended_hours"] is None
    assert row["fetch_ts"] is not None and row["fetch_end_ts"] is not None


# -- 19. the span check -----------------------------------------------------------------


def test_a_short_response_files_covered_against_requested_and_repeats(
    fixture_lake: FixtureLake,
):
    """#280 test 19 and #333's own check.

    ``lake.vendor`` and ``lake.schwab`` return the vendor's body verbatim by design, and
    ``schwab-py`` sends ``period`` alongside the explicit bounds against its own documented
    rule, so without this nothing asks whether the candles that came back cover the window
    that went out.

    The pair filed is minutes, because ``report.Withheld`` types the two fields as floats and
    a pair of instants does not fit them. The repeat is unbounded, deliberately: if the vendor
    is honouring ``period``, every request is short and stalling is better than landing a
    wrong answer quietly.
    """
    root = _lake(fixture_lake)
    short = {"candles": _minute_candles(last=CLOSE_ET - timedelta(minutes=31))}
    cassette = _cassette(minute=short)
    roster = _roster({"SPY": ["1m"]})

    result = _run(root, _RecordingVendor(cassette), roster=roster)

    assert result.landed == ()
    assert not _partition(root, "SPY", MINUTE_FREQ).exists()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN
    assert held.finding.computed == 360.0, "the pair is minutes covered"
    assert held.finding.against == 390.0, "against minutes requested"

    again = _run(root, _RecordingVendor(cassette), roster=roster, now=SECOND_NIGHT)
    assert len(again.held) == 1 and again.landed == ()
    assert len(_findings(root)) == 2


def test_a_response_covering_more_than_was_asked_for_is_refused_too(
    fixture_lake: FixtureLake,
):
    """#280 test 19, the other direction.

    The two frequencies fail opposite ways. The 1-min wrapper sends ``period=1,
    periodType=day``, narrower than any multi-session window, so its failure is short
    coverage. The daily wrapper sends ``period=20, periodType=year``, wider than any window
    this lake will ever ask for, so its failure is a response covering twenty years. A check
    that only refused short coverage would never see the second.
    """
    root = _lake(fixture_lake)
    over = {
        "candles": _minute_candles()
        + [
            bars_candle(
                CLOSE_ET + timedelta(minutes=5),
                open_=650.0,
                high=650.5,
                low=649.9,
                close=650.2,
                volume=10_000,
            )
        ]
    }

    result = _run(root, _RecordingVendor(_cassette(minute=over)), roster=_roster({"SPY": ["1m"]}))

    assert result.landed == ()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN


def test_a_daily_response_reaching_outside_the_bracket_is_refused(fixture_lake: FixtureLake):
    """#280 test 19 on the daily side, which is the half the ends comparison cannot reach.

    The daily wrapper sends ``period=20, periodType=year``, wider than any window this lake
    will ever ask for, so if Schwab honours it the response covers twenty years rather than
    fewer sessions. The session filter would quietly trim that to the one candle it wanted and
    land a partition off a response that ignored the bounds entirely. The bounds half of the
    check is what refuses it, and nothing else in the gate would.
    """
    root = _lake(fixture_lake)
    far_past = _daily_candle(date(2026, 8, 14), close=600.0)
    wanted = _daily_candle(SESSION)

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [far_past, wanted]})))

    assert result.landed == (), "a response ignoring the requested bounds landed a partition"
    assert not _partition(root, "SPY", DAILY_FREQ).exists()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN


def test_the_span_check_does_not_refuse_an_empty_window(fixture_lake: FixtureLake):
    """#280 test 19's carve-out, and the reason it is a carve-out.

    An empty window covers zero of its span, so a span check reaching it would refuse every
    empty response as a coverage failure and the empty-window rule would be dead code. The two
    file under different names, which is how a reader tells "the vendor sent nothing" from
    "the vendor sent less than was asked for".
    """
    root = _lake(fixture_lake)

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": []})))

    (held,) = result.held
    assert held.finding.check == CHECK_BAR_RESPONSE
    assert held.finding.check != CHECK_BAR_SPAN


# -- 20. the response-level fields ------------------------------------------------------


def test_the_four_response_fields_are_dropped_and_none_overflows(fixture_lake: FixtureLake):
    """#280 test 20.

    ``symbol``, ``empty``, ``previousClose`` and ``previousCloseDate`` are response-level
    rather than per-candle, so none takes a column. If they overflowed instead, ``extra`` would
    be non-null on every bars row in the lake and the column would stop meaning "a field the
    schema does not name".

    Two of the four could not be put in front of the builder before this issue widened
    ``price_history_body``: they appear nowhere under ``tests/`` and no live recording carries
    them, because ``record.py``'s ``--bars`` takes four fields with no flag arguments. A test
    of the drop rule that could only show two of the four would pass while covering half of
    what it claimed.
    """
    root = _lake(fixture_lake)
    daily = {
        "candles": [_daily_candle()],
        "body_previous_close": 648.10,
        "body_previous_close_date": 1789012800000,
    }
    vendor = _RecordingVendor(_cassette(daily=daily))

    result = _run(root, vendor)

    assert len(result.landed) == 1
    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert table.column("extra").to_pylist() == [None]
    assert "previousClose" not in table.schema.names
    # The fixture really did carry all four, so the assertion above is about the builder
    # rather than about a body that never held them.
    body = vendor._replay._cassette.find(
        "bars", bars_params("SPY", DAILY_FREQ, start=_daily_window()[0], end=_daily_window()[1])
    ).body
    assert set(body) == {"candles", "symbol", "empty", "previousClose", "previousCloseDate"}


def test_an_unrecognized_response_field_overflows_rather_than_vanishing(
    fixture_lake: FixtureLake,
):
    """The other side of the same known-set, which is what makes it load-bearing.

    Dropping the whole response level would mean a field Schwab adds there is captured
    nowhere at all. An unrecognized one overflows onto every row instead, which is loud:
    ``extra`` non-null across a whole partition is the schema-drift signature, where a silent
    drop is nothing a reader could notice.
    """
    root = _lake(fixture_lake)
    daily = {"candles": [_daily_candle()]}
    from lake.cassette import Interaction

    start, end = _daily_window()
    body = dict({"candles": [_daily_candle()], "symbol": "SPY", "empty": False}, newVendorField=7)
    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="bars",
                params=bars_params("SPY", DAILY_FREQ, start=start, end=end),
                status=200,
                body=body,
            ),
        )
    )
    assert daily  # the default body above is what this one departs from

    _run(root, _RecordingVendor(cassette))

    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert json.loads(table.column("extra").to_pylist()[0]) == {"newVendorField": 7}


# -- 21. the order of the two checks ----------------------------------------------------


def test_a_response_missing_the_session_files_the_span_check_not_the_close_check(
    fixture_lake: FixtureLake,
):
    """#280 test 21, which is what fails if the two run in the other order.

    The close cross-check compares the candle whose stamp maps to the session, so on a
    response that dropped that session it has no candle to compare. Running it first would
    file a close-cross-check finding for a comparison that never had a bar, while the real
    cause, a response short of what was asked for, went unnamed.

    The response here carries the neighbouring session's candle, which is exactly what a check
    reading the payload rather than the selected rows would call covered.
    """
    root = _lake(fixture_lake)
    neighbour = {"candles": [_daily_candle(date(2026, 9, 15))]}

    result = _run(root, _RecordingVendor(_cassette(daily=neighbour)))

    assert result.landed == ()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN, "the close check ran on a session with no bar"
    assert held.finding.computed == 0.0 and held.finding.against == 1.0


# -- 22. the two tokens -----------------------------------------------------------------


def test_each_gate_check_files_under_its_own_token_and_the_two_differ(
    fixture_lake: FixtureLake,
):
    """#280 test 22, asserted against the constants rather than their strings.

    A test asserting the string would keep passing through a rename that left one filing site
    behind. The two tokens differing is the property: filing both gate checks under one name
    would leave a reader unable to tell a vendor that sent less than was asked for from a
    vendor whose close disagrees with the lake's own quotes.
    """
    assert CHECK_BAR_SPAN != CHECK_BAR_CLOSE

    root = _lake(fixture_lake)
    short = {"candles": _minute_candles(last=CLOSE_ET - timedelta(minutes=31))}
    drifted = {"candles": [_daily_candle(close=SETTLED_CLOSE * (1 + PER_EVENT_DIVIDEND))]}

    result = _run(
        root,
        _RecordingVendor(_cassette(minute=short, daily=drifted)),
        roster=_roster({"SPY": ["1m", "1d"]}),
    )

    filed = {finding["event"]: finding["check"] for finding in _findings(root)}
    assert filed == {MINUTE_FREQ: CHECK_BAR_SPAN, DAILY_FREQ: CHECK_BAR_CLOSE}
    assert {held.finding.check for held in result.held} == {CHECK_BAR_SPAN, CHECK_BAR_CLOSE}


# -- 23. the span is measured after the drop --------------------------------------------


def test_an_edge_candle_with_a_refused_stamp_shortens_the_span_that_is_measured(
    fixture_lake: FixtureLake,
):
    """#280 test 23.

    A candle whose stamp the epoch transform refuses never becomes a row, and one at an edge
    of the window takes that end of the coverage with it. Measuring the payload instead would
    describe a partition that was never written, and it would pass while the partition landed
    short, which is the failure the span check exists to catch arriving through the back door.

    The price is worth naming: on this branch the finding says short coverage when the cause
    was an unreadable stamp. The symptom is what is filed, not the cause.
    """
    root = _lake(fixture_lake)
    candles = _minute_candles()
    candles[-1] = dict(candles[-1], datetime=True)
    result = _run(
        root,
        _RecordingVendor(_cassette(minute={"candles": candles})),
        roster=_roster({"SPY": ["1m"]}),
    )

    assert result.landed == (), "a partition landed short of the window it recorded"
    assert not _partition(root, "SPY", MINUTE_FREQ).exists()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN
    assert held.finding.computed == 1.0, "the dropped edge candle still counted toward coverage"
    assert held.finding.against == 390.0


# -- 24. the daily selection ------------------------------------------------------------


def test_a_daily_response_lands_the_candle_whose_stamp_maps_to_the_session(
    fixture_lake: FixtureLake,
):
    """#280 test 24.

    The job selects the candle whose stamp maps to the session rather than assuming one comes
    back. The ``1d`` window is deliberately wider than the session, so a response carrying its
    neighbours is expected, and taking ``candles[0]`` would land the wrong session's bar under
    this session's date with nothing to say so.

    The stamps here are the fixture's convention, since no live daily recording exists yet.
    What the code reads off them is the session, which is the rule ``journal.py`` pins on the
    bars schema: a bar's session is decided by ``bar_ts``, never by when the sweep fetched it.
    A recording showing a different instant moves this fixture and nothing in ``lake.bars``.
    """
    root = _lake(fixture_lake)
    # The neighbour comes first in the list, so an implementation taking ``candles[0]`` lands
    # the following session's bar under this session's date. Both stamps sit inside the
    # bracket, which is what makes this a selection test rather than a bounds test: a candle
    # outside the bracket is refused by the span check's other half.
    after = _daily_candle(date(2026, 9, 15), close=700.0)
    wanted = _daily_candle(SESSION, close=SETTLED_CLOSE)

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [after, wanted]})))

    (landed,) = result.landed
    assert landed.rows == 1, "more than the session's own candle landed"
    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    row = table.to_pylist()[0]
    assert row["close"] == SETTLED_CLOSE, "the first candle landed rather than the session's"
    assert bars.session_of(row["bar_ts"]) == SESSION


def test_a_stamp_names_its_eastern_session_rather_than_its_utc_date(fixture_lake: FixtureLake):
    """The session a stamp names is its Eastern date, and the two differ in the evening.

    A bar stamped at 22:00 Eastern on the session is 02:00 UTC the next day, so a reading that
    took the UTC date would file it under the wrong session. The convention question the live
    recording settles is which instant Schwab stamps a daily candle at, and this is the rule
    that reads it either way: the zone comes from ``lake.calendar``, so nothing here names a
    session time of its own.

    The sweep is driven with the same stamp, so the rule is pinned where it is used rather
    than only on the helper.
    """
    evening = datetime.fromisoformat("2026-09-14T22:00:00-04:00")
    assert evening.astimezone(UTC).date() == date(2026, 9, 15)
    assert bars.session_of(evening.astimezone(UTC).isoformat()) == SESSION

    root = _lake(fixture_lake)
    candle = bars_candle(evening, open_=645.0, high=651.0, low=644.0, close=SETTLED_CLOSE, volume=1)
    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [candle]})))

    (landed,) = result.landed
    assert landed.session == SESSION and landed.rows == 1


def test_a_candle_with_no_stamp_at_all_lands_no_row_either(fixture_lake: FixtureLake):
    """An absent stamp is the vendor sending nothing rather than something unusable.

    The transform returns ``None`` for it rather than raising, so it reaches the builder by a
    different path than a refused stamp does and needs its own refusal. It costs the row for
    the same reason: ``bar_ts`` is non-null, and ``pq.write_table`` refuses a table rather than
    a row, so one such candle would cost the whole partition's write.
    """
    stampless = {key: value for key, value in _daily_candle().items() if key != "datetime"}
    root = _lake(fixture_lake)

    result = _run(
        root,
        _RecordingVendor(_cassette(daily={"candles": [stampless, _daily_candle()]})),
    )

    (landed,) = result.landed
    assert landed.rows == 1
    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    assert table.column("bar_ts").null_count == 0


def test_the_close_is_read_off_the_next_session_rather_than_the_session_itself(
    fixture_lake: FixtureLake,
):
    """The session's own ``spot_close`` is the pre-auction book, not the closing print.

    Measured on the live lake, ``regular_market_last_price`` is still moving after 16:00. What
    the lake holds settled is the next session's ``close_price``, carried on every row of it.
    So the lake here holds both sessions and they disagree: a check reading the session's own
    partition would compare against the wrong figure and hold a bar that agrees.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", SESSION): [_quote_row(SESSION, close_price=600.0)],
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING, close_price=SETTLED_CLOSE)],
        },
    )

    result = _run(root, _RecordingVendor(_cassette()))

    assert len(result.landed) == 1 and result.held == ()


def test_a_daily_response_carrying_no_candle_for_the_session_lands_no_row(
    fixture_lake: FixtureLake,
):
    """#280 test 24's second half.

    A response carrying none for the session lands no row for it, which is the span check
    refusing rather than a zero-row partition satisfying the skip forever.
    """
    root = _lake(fixture_lake)
    others = {"candles": [_daily_candle(date(2026, 9, 15))]}

    result = _run(root, _RecordingVendor(_cassette(daily=others)))

    assert result.landed == ()
    assert not _partition(root, "SPY", DAILY_FREQ).exists()
    assert result.held[0].finding.check == CHECK_BAR_SPAN


# -- the review's findings, each held ---------------------------------------------------


def test_a_close_of_record_that_disagrees_is_contained_to_its_ticker(fixture_lake: FixtureLake):
    """A disagreement holds the bar rather than ending the run.

    The close of record is one cycle and still more than one row when the partition holds two
    spellings of that instant. Those rows have to agree, and a disagreement raises rather than
    taking the first, because taking the first lets the file's own row order decide what a bar
    is judged against. What it must not do is escape: a bare ``ValueError`` falls through the
    walk's catch, which names only what this job contains, and ends the run on a traceback with
    no report at all while every later ticker loses its bars.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [
                _quote_row(FOLLOWING, close_price=650.0),
                _quote_row(FOLLOWING, close_price=650.01),
            ],
            ("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")],
        },
        master=_master(("SPY", "QQQ")),
    )

    result = _run(
        root,
        _RecordingVendor(_cassette(tickers=("SPY", "QQQ"))),
        roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}),
    )

    (held,) = result.held
    assert held.finding.symbol == "SPY"
    assert "CloseOfRecordDisagrees" in (held.finding.exception or "")
    assert [landed.ticker for landed in result.landed] == ["QQQ"]


def test_a_null_or_nan_close_is_an_absence_rather_than_a_disagreement(fixture_lake: FixtureLake):
    """A null is not a competing answer, and a NaN is not a figure.

    A partition sealed before ``close_price`` carried a value would otherwise read as a
    disagreement with the one row that does carry it. A NaN never equals itself, so two rows
    carrying one would read as two answers. Either way the comparison is left with no number
    and the bar is held for a missing input, which is what a missing column already gives.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [
                _quote_row(FOLLOWING, close_price=None),
                _quote_row(FOLLOWING, close_price=SETTLED_CLOSE),
            ]
        },
    )

    result = _run(root, _RecordingVendor(_cassette()))

    assert len(result.landed) == 1, "a null beside a real figure read as a disagreement"
    assert result.held == ()

    nan_root = _lake(
        FixtureLake(root.parent / "nanlake"),
        quotes={
            ("SPY", FOLLOWING): [
                _quote_row(FOLLOWING, close_price=float("nan")),
                _quote_row(FOLLOWING, close_price=float("nan")),
            ]
        },
    )
    nan_result = _run(nan_root, _RecordingVendor(_cassette()))
    assert nan_result.landed == ()
    (held,) = nan_result.held
    assert held.finding.check == CHECK_BAR_CLOSE
    assert held.finding.against is None, "a NaN reached the comparison as a figure"


def test_a_retyped_vendor_close_holds_the_bar_rather_than_ending_the_run(
    fixture_lake: FixtureLake,
):
    """Reading a retyped close is not this job handing the seam a bad argument.

    So it reads as no figure and the check holds the bar for a missing input, rather than
    ending the run on a traceback and costing every ticker after it.
    """
    root = _lake(fixture_lake)
    retyped = dict(_daily_candle(), close="n/a")

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [retyped]})))

    assert result.landed == ()
    (held,) = result.held
    assert held.finding.check == CHECK_BAR_CLOSE
    assert held.finding.computed is None


def test_a_retyped_candle_field_is_contained_to_its_ticker(fixture_lake: FixtureLake):
    """A vendor retyping a candle field costs one ticker-day, not the rest of the run.

    The batch build refuses the value, which is the right refusal on this surface: routing
    exists to keep a perishable capture minute, and a bars partition is re-fetchable. But the
    refusal has to be contained, or the loud failure costs every ticker after it rather than
    costing a re-run, which is the opposite of what the module promises.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
            ("QQQ", FOLLOWING): [_quote_row(FOLLOWING, ticker="QQQ")],
        },
        master=_master(("SPY", "QQQ")),
    )
    retyped = dict(_daily_candle(), volume="70000000")
    start, end = _daily_window()
    from lake.cassette import Interaction

    interactions = [
        Interaction(
            endpoint="bars",
            params=bars_params("SPY", DAILY_FREQ, start=start, end=end),
            status=200,
            body={"candles": [retyped], "symbol": "SPY", "empty": False},
        ),
        *bars_interactions("QQQ", DAILY_FREQ, [(start, end, [_daily_candle()])]),
    ]

    result = _run(
        root,
        _RecordingVendor(Cassette(interactions=tuple(interactions))),
        roster=_roster({"SPY": ["1d"], "QQQ": ["1d"]}),
    )

    assert [landed.ticker for landed in result.landed] == ["QQQ"], "one retyped field ended the run"
    (held,) = result.held
    assert held.finding.symbol == "SPY"


def test_a_candle_field_sharing_a_response_level_name_still_overflows(
    fixture_lake: FixtureLake,
):
    """Each level is measured against its own known-set, not against one union of the two.

    Unioning them reads as the same rule and is not. A candle carrying ``symbol`` would be
    measured against the response's set and dropped silently, and a response carrying
    ``volume`` would be measured against the candle's set and dropped too. Those are the names
    most likely to signal a real vendor change, so they are exactly the ones that must not go
    quiet.
    """
    root = _lake(fixture_lake)
    odd_candle = dict(_daily_candle(), symbol="XYZ")
    start, end = _daily_window()
    from lake.cassette import Interaction

    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="bars",
                params=bars_params("SPY", DAILY_FREQ, start=start, end=end),
                status=200,
                body={
                    "candles": [odd_candle],
                    "symbol": "SPY",
                    "empty": False,
                    "volume": 999,
                },
            ),
        )
    )

    _run(root, _RecordingVendor(cassette))

    table = pa.parquet.read_table(_partition(root, "SPY", DAILY_FREQ))
    overflow = json.loads(table.column("extra").to_pylist()[0])
    assert overflow == {"symbol": "XYZ", "volume": 999}


def test_a_refused_daily_fetch_files_a_pair_that_is_not_full_coverage(
    fixture_lake: FixtureLake,
):
    """A held fetch must not file "1.0 against 1.0".

    The typed pair exists to say what the check measured. Setting covered from the selected
    rows alone would report full coverage on a bar that never landed, and a reader could not
    tell a bounds violation from a coverage failure.
    """
    root = _lake(fixture_lake)
    far_past = _daily_candle(date(2026, 8, 14), close=600.0)

    result = _run(root, _RecordingVendor(_cassette(daily={"candles": [far_past, _daily_candle()]})))

    (held,) = result.held
    assert held.finding.check == CHECK_BAR_SPAN
    assert held.finding.against == 1.0
    assert held.finding.computed is not None and held.finding.computed > 1.0


def test_one_frequency_named_twice_is_fetched_once(fixture_lake: FixtureLake):
    """A roster is free to name a frequency twice, and the run must not spend two requests.

    The manifest is read once before the walk, so the second pass would not see the first
    one's entry: the partition would be written twice and manifested twice for one path.
    """
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette())

    result = _run(root, vendor, roster=_roster({"SPY": ["1d", "1d"]}))

    assert len(vendor.calls) == 1
    assert len(result.landed) == 1
    rel = _partition(root, "SPY", DAILY_FREQ).relative_to(root).as_posix()
    assert len(_entries(root, rel)) == 1


def test_a_bad_frequency_on_a_disabled_ticker_does_not_halt_the_run(
    fixture_lake: FixtureLake,
):
    """The refusal covers exactly the entries the walk would fetch, and no more.

    Checking the whole roster reaches past this run's scope: a stale ``bars:`` line on a
    retired ticker, which nothing here would ever fetch, would halt the nightly run every
    night until someone edited a file for a ticker that is not being captured.
    """
    root = _lake(fixture_lake)
    roster = _roster({"SPY": ["1d"], "OLD": ["5m"]}, enabled={"OLD": False})

    result = _run(root, _RecordingVendor(_cassette()), roster=roster)

    assert len(result.landed) == 1
    # An enabled ticker carrying the same frequency is still refused, so the narrowing did not
    # delete the rule.
    with pytest.raises(UnsupportedBarFreq):
        _run(root, _RecordingVendor(_cassette()), roster=_roster({"SPY": ["1d"], "NEW": ["5m"]}))


def test_a_vendor_failure_files_under_the_response_token_not_the_close_one(
    fixture_lake: FixtureLake,
):
    """The token follows the cause rather than being one name for every contained failure.

    A vendor that refused the request never reached the gate, so filing it under the close
    cross-check would tell an operator the close disagreed when no close was ever read.
    """
    root = _lake(fixture_lake)
    vendor = _RecordingVendor(_cassette(), fail_with={"SPY": VendorError("refused")})

    result = _run(root, vendor)

    (held,) = result.held
    assert held.finding.check == CHECK_BAR_RESPONSE
    assert held.finding.check != CHECK_BAR_CLOSE


def test_the_span_check_refuses_to_answer_for_an_empty_candle_list():
    """The check is defined over a non-empty list, which is #333's own rule.

    An empty window covers zero of its span, so a check reaching one would refuse every empty
    response as a coverage failure and the empty-window rule that owns that case would be dead
    code. The caller refuses an empty response first, and this refuses loudly rather than
    quietly answering for a case it does not decide.
    """
    from lake.session import SessionClock

    bounds = SessionClock(ManualClock(FIRST_NIGHT), weekday_sessions(MONDAY)).bounds(SESSION)
    with pytest.raises(ValueError, match="non-empty"):
        bars.check_bar_span([], [], bar_window(MINUTE_FREQ, bounds))


def test_the_next_session_search_matches_the_bound_oi_already_uses():
    """A third spelling of one step, with its bound deliberately not chosen again.

    ``oi`` and ``control_plane`` already disagree, 30 against 14, and marketlake #334 owns
    collapsing the three. A third number would have left that issue two values to settle
    instead of one.
    """
    from lake import oi

    assert bars.NEXT_SESSION_SEARCH_DAYS == oi._NEXT_SESSION_SEARCH_DAYS
    calendar = weekday_sessions(MONDAY)
    assert bars._calendar_next_session(calendar, SESSION) == FOLLOWING
    # The bound is what the search gives up at, so a horizon of one cannot reach past a weekend.
    assert bars._calendar_next_session(calendar, date(2026, 9, 18), horizon=1) is None


def test_the_daily_margin_is_an_argument_the_window_builder_reads():
    """The bracket's width is a parameter rather than a literal, so a recording can narrow it.

    marketlake #362 is what narrows it once a live daily recording says which instant Schwab
    stamps a candle at.
    """
    from lake.session import SessionClock

    bounds = SessionClock(ManualClock(FIRST_NIGHT), weekday_sessions(MONDAY)).bounds(SESSION)
    narrowed = bar_window(DAILY_FREQ, bounds, margin=timedelta(hours=6))
    assert narrowed.start == OPEN_ET - timedelta(hours=6)
    assert narrowed.end == CLOSE_ET + timedelta(hours=6)


def test_the_close_cross_check_reads_the_tolerance_it_is_handed():
    """The tolerance is an argument, so the battery in #138 can ask the same question wider."""
    assert bars.check_close_cross(650.0 * 1.01, 650.0, tolerance=0.05).agrees is True
    assert bars.check_close_cross(650.0 * 1.01, 650.0, tolerance=1e-6).agrees is False


# -- the two check functions on their own -----------------------------------------------


def test_the_close_cross_check_refuses_a_comparison_missing_either_number():
    """A check missing an input has not agreed, which is what makes a partial payload held."""
    assert bars.check_close_cross(None, 650.0).agrees is False
    assert bars.check_close_cross(650.0, None).agrees is False
    assert bars.check_close_cross(None, None).agrees is False


def test_the_close_cross_check_handles_a_settled_close_of_zero():
    """A zero reference has no relative scale, so the two agree only when both are zero.

    A division that returned "agrees" for every bar against a zeroed reference is the one
    failure nobody would notice.
    """
    assert bars.check_close_cross(0.0, 0.0).agrees is True
    assert bars.check_close_cross(650.0, 0.0).agrees is False


def test_the_close_cross_tolerance_sits_inside_the_bracket_it_was_measured_from():
    """The floor is the drift it absorbs and the ceiling is the adjustment it must catch.

    The measured window is QQQ's, 1.13 basis points of drift between the two close cycles up
    to 11.47 basis points for one per-event dividend. A tolerance outside it either files a
    finding every night on ordinary drift or passes a dividend-sized adjustment silently.
    """
    assert SETTLED_DRIFT < CLOSE_CROSS_TOLERANCE < PER_EVENT_DIVIDEND
    assert bars.check_close_cross(650.0 * (1 + SETTLED_DRIFT), 650.0).agrees is True
    assert bars.check_close_cross(650.0 * (1 + PER_EVENT_DIVIDEND), 650.0).agrees is False
