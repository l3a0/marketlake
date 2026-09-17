"""The bar backfill: every session the capture spans cover, fetched one ticker-day at a time.

Marketlake #319. The by-hand fetch beside it lands one session; this walks a range, and since
marketlake #422 it is what the 18:30 job runs. What
decides the range is three rules the tests below drive one at a time. A session is in range when
the calendar says it traded, when its own window intersects a capture span, and when its equity
close has already passed.

Every test builds a lake on disk, runs the walk against a cassette-backed fake vendor and a manual
clock, and reads the result back off the files a reader would read. Nothing touches the network.
The fake refuses a window it has no recording for, so a drifted fetch fails visibly rather than
replaying somebody else's window, and it keeps every call, which is what lets a skipped ticker-day
be shown to have reached no vendor at all.

Two things about the fixture calendar are worth naming before the tests.

1. **The week is a real one and the arithmetic is the live lake's.** 2026-09-07 is Labor Day, so
   the fixture weeks run 2026-09-07 with that holiday and 2026-09-14 whole. That gives the same
   seven sessions from the live lake's span start to 2026-09-16 that the issue's own count is
   taken against.
2. **The daily stamps are the fixture's convention.** They sit at Eastern midnight of the session,
   which is what ``tests/component/test_bar_fetch.py`` uses. The code reads the session off the
   stamp rather than off the window, so the convention can move without moving a rule.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import bars, journal, report
from lake.bars import (
    CHECK_BAR_CLOSE,
    CHECK_BAR_RESPONSE,
    MINUTE_EXTENDED_HOURS,
    GateSkip,
    SpansAbsent,
    TickerDay,
    UnsupportedBarFreq,
    backfill_bars,
    plan_backfill,
)
from lake.capture_spans import SPANS_SCHEMA, SPANS_SCHEMA_VERSION, CaptureSpans, spans_path
from lake.cassette import Cassette
from lake.config import GuardConstants
from lake.extra_projection import ExtraProjection, ExtraProjectionError
from lake.loader import LoadError, NoSpotClose, PartialRead
from lake.manifest import ManifestError, read_manifest
from lake.paths import LakePaths
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.schwab import VendorAuthError
from lake.security_master import ID_TYPE_TICKER, KIND_EQUITY, SecurityMaster, master_path
from lake.tickers import Roster
from lake.vendor import DAILY_FREQ, MINUTE_FREQ, VendorError
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.vendor import RecordingVendor, bars_candle, bars_interactions

# The live lake's own span start, 13:07 Eastern on 2026-09-08, which is mid-session. Every floor
# test is driven from this instant rather than from a rounder one, because the floor rule exists
# for exactly this shape.
SPAN_START = datetime(2026, 9, 8, 17, 7, tzinfo=UTC)

# The three weeks the fixture calendar holds, and the holiday inside the second. Labor Day 2026
# falls on 2026-09-07, which is why the range's first session is the Tuesday. The week before it
# exists so a test can put the holiday *inside* a range rather than before its floor, which is the
# only arrangement where ``is_session`` rather than the floor rule is what removes it.
WEEK_ZERO = date(2026, 8, 31)
WEEK_ONE = date(2026, 9, 7)
WEEK_TWO = date(2026, 9, 14)
LABOR_DAY = date(2026, 9, 7)

# The seven sessions from the span start to 2026-09-16, which is what the issue counts against.
SESSIONS = (
    date(2026, 9, 8),
    date(2026, 9, 9),
    date(2026, 9, 10),
    date(2026, 9, 11),
    date(2026, 9, 14),
    date(2026, 9, 15),
    date(2026, 9, 16),
)

# The evening of 2026-09-16, after that session's close, which is when a backfill is hand-run.
# Every session in SESSIONS is complete at this instant.
TONIGHT = datetime(2026, 9, 16, 22, 0, tzinfo=UTC)

DAY_MARGIN = timedelta(days=1)
SETTLED_CLOSE = 650.00
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

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


def _calendar():
    """The fixture calendar: three real weeks with Labor Day out of the middle one."""
    return weekday_sessions(WEEK_ZERO, WEEK_ONE, WEEK_TWO, holidays=[LABOR_DAY])


def _bounds(day: date) -> tuple[datetime, datetime]:
    """One session's open and equity close, the way the fixture calendar serves them."""
    return (
        datetime.fromisoformat(f"{day.isoformat()}T09:30:00-04:00"),
        datetime.fromisoformat(f"{day.isoformat()}T16:00:00-04:00"),
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

    A day of these is what the live lake's 2026-09-08 through 2026-09-11 hold from a real auth
    outage, and it is what makes ``load_quotes`` raise ``NoSpotClose``.
    """
    return _quote_row(day, ticker=ticker, close_price=None, close_tag=None, row_kind="gap")


def _quotes_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {name: [row.get(name) for row in rows] for name in QUOTES_SCHEMA.names},
        schema=QUOTES_SCHEMA,
    )


def _ledger_table() -> pa.Table:
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _master(
    tickers: tuple[str, ...] = ("SPY",), *, valid_from: date = date(2026, 9, 8)
) -> SecurityMaster:
    """A master holding each ticker from ``valid_from``, the way the live lake's does."""
    master = SecurityMaster()
    for ticker in tickers:
        master.register(
            kind=KIND_EQUITY,
            capture_start=SPAN_START,
            valid_from=valid_from,
            ticker=ticker,
        )
    return master


def _spans(*windows: tuple[int, datetime, datetime | None]) -> CaptureSpans:
    """A spans set built from explicit ``(instrument_id, start, end)`` triples."""
    spans = CaptureSpans()
    for instrument_id, start, end in windows:
        spans.open_span(instrument_id, start, True)
        if end is not None:
            spans.close_span(instrument_id, end)
    return spans


def _open_span(*instrument_ids: int, start: datetime = SPAN_START) -> CaptureSpans:
    """One open span per instrument, all starting at the live lake's own mid-session instant."""
    return _spans(*((instrument_id, start, None) for instrument_id in instrument_ids))


def _roster(mapping: dict[str, list[str]], *, enabled: dict[str, bool] | None = None) -> Roster:
    enabled = enabled or {}
    return Roster.from_mapping(
        {
            ticker: {"options": True, "bars": freqs, "enabled": enabled.get(ticker, True)}
            for ticker, freqs in mapping.items()
        }
    )


def _minute_candles(day: date, close: float = SETTLED_CLOSE) -> list[dict]:
    """A minute response's candles for one session: the window's first minute and its last."""
    first, last = _bounds(day)
    return [
        bars_candle(first, open_=648.0, high=649.0, low=647.5, close=648.5, volume=1_400_000),
        bars_candle(
            last - timedelta(minutes=1),
            open_=649.5,
            high=650.5,
            low=649.0,
            close=close,
            volume=2_100_000,
        ),
    ]


def _daily_candle(day: date, close: float = SETTLED_CLOSE) -> dict:
    """One daily candle, stamped at Eastern midnight of its session."""
    when = datetime.fromisoformat(f"{day.isoformat()}T00:00:00-04:00")
    return bars_candle(when, open_=645.0, high=651.0, low=644.0, close=close, volume=70_000_000)


def _cassette(
    sessions: tuple[date, ...] = SESSIONS,
    *,
    tickers: tuple[str, ...] = ("SPY",),
    freqs: tuple[str, ...] = (MINUTE_FREQ, DAILY_FREQ),
) -> Cassette:
    """A cassette holding one recording per ticker, frequency and session.

    A window it was not built for raises rather than replaying a neighbour's, which is what makes
    the range itself an assertion: a walk reaching a session this does not cover fails loudly.
    """
    interactions: list = []
    for ticker in tickers:
        for day in sessions:
            open_et, close_et = _bounds(day)
            if MINUTE_FREQ in freqs:
                interactions.extend(
                    bars_interactions(
                        ticker,
                        MINUTE_FREQ,
                        [(open_et, close_et, _minute_candles(day))],
                        # Keyed on the flag the minute fetch sets. The daily interactions below
                        # key on none, matching the daily call, which leaves it unset.
                        extended_hours=MINUTE_EXTENDED_HOURS,
                    )
                )
            if DAILY_FREQ in freqs:
                interactions.extend(
                    bars_interactions(
                        ticker,
                        DAILY_FREQ,
                        [
                            (
                                open_et - DAY_MARGIN,
                                close_et + DAY_MARGIN,
                                [_daily_candle(day)],
                            )
                        ],
                    )
                )
    return Cassette(interactions=tuple(interactions))


def _lake(
    fixture_lake: FixtureLake,
    *,
    quotes: dict[tuple[str, date], list[dict]] | None = None,
    master: SecurityMaster | None = None,
    spans: CaptureSpans | None = None,
    bars_partitions: tuple[tuple[str, str, date, pa.Table], ...] = (),
) -> Path:
    """A lake holding sealed quotes, the ledger, the master and the capture spans.

    ``quotes`` defaults to a settled close on every session in the range and the session after it,
    which is what lets a daily bar pass the close cross-check on every day the walk reaches. A
    test about the gate's blind spots names its own.
    """
    if quotes is None:
        quotes = {("SPY", day): [_quote_row(day)] for day in (*SESSIONS, date(2026, 9, 17))}
    for (ticker, day), rows in quotes.items():
        fixture_lake.with_quotes(ticker, day, _quotes_table(rows))
    for ticker, freq, day, table in bars_partitions:
        fixture_lake.with_bars(ticker, freq, day, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    (master if master is not None else _master()).write(master_path(root))
    (spans if spans is not None else _open_span(1)).write(spans_path(root))
    return root


def _run(
    root: Path,
    vendor,
    *,
    roster: Roster | None = None,
    spans: CaptureSpans | None = None,
    now: datetime = TONIGHT,
    guards: GuardConstants | None = None,
):
    """One backfill run over a fixture lake, with every seam injected.

    ``guards`` is left ``None`` by default so the run resolves the design's pinned budget, which
    is 100 and never binds at these fixture scales. A test about the budget passes its own.
    """
    return backfill_bars(
        lake_root=root,
        vendor=vendor,
        clock=ManualClock(now),
        calendar=_calendar(),
        roster=roster if roster is not None else _roster({"SPY": [MINUTE_FREQ]}),
        spans=spans if spans is not None else _open_span(1),
        guards=guards,
    )


def _plan(
    *,
    spans: CaptureSpans | None = None,
    master: SecurityMaster | None = None,
    roster: Roster | None = None,
    now: datetime = TONIGHT,
):
    """One plan, with no lake at all. The range rules are pure over the four references."""
    return plan_backfill(
        spans=spans if spans is not None else _open_span(1),
        master=master if master is not None else _master(),
        roster=roster if roster is not None else _roster({"SPY": [MINUTE_FREQ]}),
        clock=ManualClock(now),
        calendar=_calendar(),
    )


def _tickers_file(tmp_path: Path, body: str = "SPY: {options: true, bars: [1m]}\n") -> Path:
    """A roster on disk, which the command loads rather than taking as an argument."""
    path = tmp_path / "tickers.yaml"
    path.write_text(body)
    return path


def _findings(root: Path, day: date) -> list[dict]:
    directory = report.withheld_dir(root, day)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


def _partition(root: Path, ticker: str, freq: str, day: date) -> Path:
    return LakePaths(root).bars_partition_path(ticker, freq, day)


def _bars_table(day: date) -> pa.Table:
    """An already-sealed bars partition, in the pinned schema, for the skip tests."""
    open_et, close_et = _bounds(day)
    built = journal.bars_rows(
        {"candles": [_daily_candle(day)], "symbol": "SPY", "empty": False},
        ticker="SPY",
        freq=DAILY_FREQ,
        instrument_id=1,
        fetch_ts=TONIGHT,
        fetch_end_ts=TONIGHT,
        window_start=open_et,
        window_end=close_et,
        extended_hours=None,
    )
    return pa.Table.from_batches([journal.bars_data_batch(built)])


# -- 1. the walk enumerates sessions, through the calendar ------------------------------


def test_the_walk_enumerates_sessions_and_skips_weekends_and_holidays():
    """#319 test 1.

    The range from 2026-09-08 to 2026-09-16 is nine calendar days and seven sessions. What is
    missing is the weekend of the 12th and 13th, and 2026-09-07 is the holiday that keeps the
    Monday before the span start out whatever the floor rule says.

    A date arithmetic of its own would answer nine, and a weekday arithmetic would answer seven
    for the wrong reason, because it would keep a holiday inside the range. The mutation the
    module docstring names is walking calendar days instead of sessions, and it fails here.
    """
    plan = _plan()
    assert plan.sessions == SESSIONS
    assert date(2026, 9, 12) not in plan.sessions
    assert date(2026, 9, 13) not in plan.sessions
    assert LABOR_DAY not in plan.sessions


def test_a_holiday_inside_the_range_is_not_walked():
    """A holiday the calendar names mid-range drops out, and nothing else moves.

    2026-09-07 sits before the span start, so the first test cannot tell a floor rule from a
    calendar one. This puts the holiday inside the range by moving the span start back to the
    Friday before it, where only ``is_session`` can remove it.
    """
    plan = _plan(spans=_open_span(1, start=datetime(2026, 9, 4, 13, 30, tzinfo=UTC)))
    assert LABOR_DAY not in plan.sessions
    assert plan.floor == date(2026, 9, 4)
    assert plan.sessions == (date(2026, 9, 4), *SESSIONS)


# -- 2. the floor at a mid-session span start ------------------------------------------


def test_a_span_opening_mid_session_puts_that_session_in_range():
    """#319 test 2.

    The live lake's span opens at 13:07 Eastern on 2026-09-08, inside that session. The floor is
    that session, not the first whole one after it, because 2026-09-08 is one of the four the
    recovery exists for and the window is not clipped to the span.

    The session before it, 2026-09-04, is out of range: its own close is long before the span
    opened, so nothing of it was ever captured.
    """
    plan = _plan()
    assert plan.floor == date(2026, 9, 8)
    assert date(2026, 9, 4) not in plan.sessions


def test_a_span_opening_after_a_sessions_close_leaves_that_session_out():
    """The floor predicate is an intersection, so a span opening after the close excludes the day.

    This is the other side of the test above and it is what keeps the floor from reading as "the
    span start's calendar date". A span opening at 18:00 Eastern on 2026-09-08 holds none of that
    session, so the range starts the next day.
    """
    plan = _plan(spans=_open_span(1, start=datetime(2026, 9, 8, 22, 0, tzinfo=UTC)))
    assert plan.floor == date(2026, 9, 9)


def test_the_window_a_mid_session_floor_fetches_is_the_whole_session(fixture_lake: FixtureLake):
    """The floor session is fetched at the session's own bounds, never clipped to the span start.

    A clipped window would land a partition covering three hours under a manifested entry that the
    skip never re-fetches, with nothing marking it short. The cassette is keyed on the full-session
    window, so a clipped fetch would raise rather than replay, and the recorded call says which
    window went out rather than only that one did.

    **The floor session's call is selected by its bounds rather than taken from the front of the
    list.** It used to be ``calls[0]``, which held only while the walk ran oldest session first.
    Marketlake #478 walks newest first, so the floor is the *last* session reached, and a test
    reading a position would now be asserting about 2026-09-16 while claiming to be about the
    floor. Selecting it says what the test is about and is indifferent to the order, which is what
    it was always trying to express.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    _run(root, vendor)
    open_et, close_et = _bounds(date(2026, 9, 8))
    start = open_et.astimezone(UTC).isoformat()
    floor_calls = [call for call in vendor.calls if call["start"] == start]
    assert len(floor_calls) == 1, "the floor session is fetched exactly once"
    assert floor_calls[0]["end"] == close_et.astimezone(UTC).isoformat()


# -- 3. every span, not only the open one ----------------------------------------------


def test_a_closed_span_is_walked():
    """#319 test 3.

    Scope is every span, which is ``CaptureSpans.in_scope``'s own definition. A retired ticker's
    closed span is a stretch the lake captured, so its bars are worth what an open span's are.

    The mutation the module docstring names is taking only open spans, and it fails here: this
    lake has no open span at all, so a walk over open spans alone plans nothing.
    """
    closed = _spans((1, SPAN_START, datetime(2026, 9, 11, 20, 0, tzinfo=UTC)))
    plan = _plan(spans=closed)
    assert plan.sessions == SESSIONS[:4]
    assert plan.ceiling == date(2026, 9, 11)


def test_a_closed_spans_final_session_takes_the_same_intersection_rule():
    """The far end of a closed span is decided by the rule that decided the near end of an open one.

    A span closed at 11:00 Eastern holds part of that session, so the session is in range. One
    closed at 09:00, before the open, holds none of it and is not.
    """
    mid = _spans((1, SPAN_START, datetime(2026, 9, 11, 15, 0, tzinfo=UTC)))
    before = _spans((1, SPAN_START, datetime(2026, 9, 11, 13, 0, tzinfo=UTC)))
    assert _plan(spans=mid).ceiling == date(2026, 9, 11)
    assert _plan(spans=before).ceiling == date(2026, 9, 10)


# -- 4. the roster lookup, past enabled ------------------------------------------------


def test_a_disabled_roster_entry_still_resolves_its_frequencies():
    """#319 test 4, first half.

    ``retire`` flips ``enabled`` and preserves ``bars``, so a retired ticker's frequencies survive
    exactly for this walk. Reading them through ``Roster.enabled`` would plan nothing at all for
    the one case this issue exists to cover.
    """
    roster = _roster({"SPY": [MINUTE_FREQ]}, enabled={"SPY": False})
    plan = _plan(roster=roster)
    assert len(plan.days) == len(SESSIONS)
    assert plan.unwalked == ()


def test_a_ticker_absent_from_the_roster_is_reported_and_the_walk_goes_on():
    """#319 test 4, second half.

    A span whose ticker has no roster entry has nothing saying which frequencies it took, so there
    is nothing to ask the vendor for. Raising would cost every other ticker its bars for a file
    edit this run did not cause. Passing over it silently would drop a ticker out of a backfill
    unremarked. So it is named on the report and the other ticker is planned in full.
    """
    plan = _plan(
        spans=_open_span(1, 2),
        master=_master(("SPY", "QQQ")),
        roster=_roster({"SPY": [MINUTE_FREQ]}),
    )
    assert {day.ticker for day in plan.days} == {"SPY"}
    assert len(plan.unwalked) == len(SESSIONS)
    assert all("QQQ" in line and "not in roster" in line for line in plan.unwalked)


def test_a_roster_entry_with_no_bars_plans_nothing_and_is_not_an_error():
    """An empty ``bars`` tuple says the ticker takes no bars, which is an answer rather than a gap.

    It is told apart from the missing entry above by leaving ``unwalked`` empty: the roster
    answered, and what it said was nothing.
    """
    plan = _plan(roster=_roster({"SPY": []}))
    assert plan.days == ()
    assert plan.unwalked == ()


# -- 5 and 6. the skip, and the second run ---------------------------------------------


def test_a_manifested_ticker_day_is_skipped_with_no_vendor_call(fixture_lake: FixtureLake):
    """#319 test 5.

    The per-ticker-day fetch shape is what makes the skip avoid the call as well as the write. It
    is the shape's main payoff and the only test that can show it, because a per-span request
    would have gone out before any ticker-day was considered.
    """
    sealed = date(2026, 9, 8)
    root = _lake(fixture_lake, bars_partitions=(("SPY", DAILY_FREQ, sealed, _bars_table(sealed)),))
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))
    assert result.skipped == 1
    assert result.attempted == len(SESSIONS) - 1
    # The window is taken from the production builder rather than spelled by hand, because a
    # daily bracket reaches a day either side and a hand-written instant would compare the
    # sealed session's window against a neighbour's.
    session_clock = bars.SessionClock(ManualClock(TONIGHT), _calendar())
    skipped_window = bars.bar_window(DAILY_FREQ, session_clock.bounds(sealed))
    assert len(vendor.calls) == len(SESSIONS) - 1
    assert skipped_window.start.astimezone(UTC).isoformat() not in {
        call["start"] for call in vendor.calls
    }


def test_a_second_run_lands_nothing_and_reaches_no_vendor(fixture_lake: FixtureLake):
    """#319 test 6.

    A second run over the same lake costs zero vendor requests, which is the claim the fetch shape
    was chosen on. ``skipped`` against ``landed`` is what tells a run that did the work already
    apart from one that found nothing to walk.
    """
    root = _lake(fixture_lake)
    first = _run(root, RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))))
    assert len(first.landed) == len(SESSIONS)

    second_vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    second = _run(root, second_vendor)
    assert second.landed == ()
    assert second.attempted == 0
    assert second.skipped == len(SESSIONS)
    assert second_vendor.calls == []


# -- 7 and 8. the days the gate cannot judge -------------------------------------------


def test_a_session_whose_quotes_hold_only_gap_rows_is_abandoned_without_a_request(
    fixture_lake: FixtureLake,
):
    """#319 test 7, re-pointed by marketlake #434.

    This is the live lake's own condition: 2026-09-08 through 2026-09-11 hold gap rows and no data
    row, so ``load_quotes`` raises ``NoSpotClose`` for them and the close cross-check has no
    comparison. Those quotes are sealed and nothing can rebuild a data row that never existed, so
    no later run changes the verdict.

    **This used to fetch, hold and file, on every run for ever.** The bar was never landed, so it
    was never manifested, so the manifested skip never reached it. Six ticker-days on the live
    lake were costing a vendor request and a withheld file a night with no end. The walk reads the
    reference first now and abandons them, and ``calls`` is the assertion that matters: a test
    reading ``held`` alone would pass with the request still going out.

    They land in ``abandoned`` rather than ``unsettled`` because their following sessions sealed
    days ago. ``test_the_newest_session_is_unsettled_rather_than_abandoned`` holds the other side
    on the same run.
    """
    quotes: dict[tuple[str, date], list[dict]] = {}
    for day in (*SESSIONS, date(2026, 9, 17)):
        rows = [_gap_row(day)] if day <= date(2026, 9, 11) else [_quote_row(day)]
        quotes[("SPY", day)] = rows
    root = _lake(fixture_lake, quotes=quotes)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    # 09-08, 09-09 and 09-10 each read a following session holding only gap rows. 09-11 reads
    # 09-14, which settled, so it lands like every session after it.
    #
    # **Newest session first, which is marketlake #478's walk order.** The equality is on the whole
    # tuple rather than a set on purpose: the order a reader meets these in is the order the walk
    # produced them, and the sign-off block renders them in exactly this sequence.
    assert result.abandoned == tuple(
        GateSkip("SPY", DAILY_FREQ, day, "NoSpotClose")
        for day in (date(2026, 9, 10), date(2026, 9, 9), date(2026, 9, 8))
    )
    # ``skipped`` is the manifested count and nothing else, which is what ``BarsReport`` rests its
    # second-run argument on and what the digest prints. Counting a gate skip there too would read
    # as "the lake already has these bars".
    assert result.skipped == 0, "a gate skip was counted as a manifested one"
    assert result.held == ()
    assert {entry.session for entry in result.landed} == set(SESSIONS[3:])
    assert [call["symbol"] for call in vendor.calls] == ["SPY"] * len(SESSIONS[3:]), (
        "an abandoned ticker-day still spent a vendor request"
    )
    assert result.attempted == len(SESSIONS[3:])
    assert not _partition(root, "SPY", DAILY_FREQ, date(2026, 9, 8)).exists()
    assert _findings(root, date(2026, 9, 8)) == [], "an abandoned ticker-day filed a finding"


def test_the_newest_session_is_unsettled_rather_than_abandoned(fixture_lake: FixtureLake):
    """#319 test 8, re-pointed by marketlake #434.

    The newest session in range has no settled close until the following session's partition
    exists, so its read raises ``PartitionAbsent``. That one settles itself: tomorrow's seal makes
    the comparison available and the next run lands the bar.

    **So it is the case the abandoned line must not swallow.** Both reach the same skip and both
    save the same request, and only the clock separates them: this session's following day has not
    reached its own close+15, while the outage's have. Recording them together would put a
    ticker-day that lands tomorrow into the count of ones the lake has given up on, and that count
    is the one an operator is meant to act on.

    It happens on every healthy run, on a lake with nothing wrong with it, which is why the sweep
    reports ``abandoned`` and leaves this one to the command's own output.
    """
    quotes = {("SPY", day): [_quote_row(day)] for day in SESSIONS}
    root = _lake(fixture_lake, quotes=quotes)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))
    assert result.unsettled == (GateSkip("SPY", DAILY_FREQ, date(2026, 9, 16), "PartitionAbsent"),)
    assert result.abandoned == ()
    assert result.held == ()
    assert len(vendor.calls) == len(SESSIONS) - 1
    assert len(result.landed) == len(SESSIONS) - 1


def test_a_repaired_reference_makes_an_abandoned_ticker_day_fetchable_again(
    fixture_lake: FixtureLake,
):
    """The verdict is derived on every run, so nothing has to be cleared to undo it.

    This is what the skip rests on. An abandoned ticker-day is not recorded anywhere and no
    manifest entry stands in for the bar, so the walk re-asks the same question of the same
    partition every night and answers it from whatever the partition holds *now*. The word
    describes what this run did rather than a verdict about the future.

    It matters because one of the reasons has a remedy. A quarantined following session clears
    through ``python -m lake.signoff`` and a partition can be rebuilt from its segments, and
    either changes what ``load_quotes`` returns. If the skip kept state, each of those would
    need a second thing signed off on the bars side, and the ticker-day would stay abandoned
    until somebody found it.

    The mutation this pins is recording the verdict instead of deriving it: a run that
    remembered 09-08 was abandoned would land nothing here on the second pass.
    """
    gapped = {("SPY", day): [_gap_row(day)] for day in SESSIONS}
    root = _lake(fixture_lake, quotes=gapped)
    first_vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    first = _run(root, first_vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    assert len(first.abandoned) == len(SESSIONS) - 1
    assert first.landed == ()
    assert first_vendor.calls == []

    # The 09-09 quotes are repaired, which is the only thing that changes between the runs.
    fixture_lake.with_quotes("SPY", date(2026, 9, 9), _quotes_table([_quote_row(date(2026, 9, 9))]))
    fixture_lake.build()

    second_vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    second = _run(root, second_vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    assert [entry.session for entry in second.landed] == [date(2026, 9, 8)], (
        "the repaired reference did not make its bar fetchable again"
    )
    assert len(second.abandoned) == len(SESSIONS) - 2
    assert [call["symbol"] for call in second_vendor.calls] == ["SPY"]


def test_a_lake_that_contradicts_its_own_writers_is_held_rather_than_abandoned(
    fixture_lake: FixtureLake, monkeypatch
):
    """The other half of the line ``_reference`` draws, and the reason it is drawn there.

    ``_load_surface`` raises a bare ``LoadError`` at three sites, all of which say this lake's
    files disagree with what wrote them rather than that a session cannot be read. Marketlake
    #365 put those on the filing side and named ``lake.bars`` as its own precedent, so folding
    them into a counted skip would have moved a corruption into a number that never returns to
    zero.

    The request is still saved, because a gate with no readable reference cannot pass whichever
    of the two it is. That is the whole of marketlake #434, and it is separable from what gets
    written down.

    The refusal is forced at the loader rather than built as a fixture, because the three sites
    want three malformed partitions and what this pins is the containment rather than any one of
    them.
    """
    from lake import bars as bars_module

    quotes = {("SPY", day): [_quote_row(day)] for day in SESSIONS}
    root = _lake(fixture_lake, quotes=quotes)

    def refuse(ticker, day, *args, **kwargs):
        raise LoadError(f"{ticker} {day}: rows carry no row_kind")

    monkeypatch.setattr(bars_module, "load_quotes", refuse)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    assert result.unsettled == () and result.abandoned == ()
    # Every session in range, the newest included: with the read refusing, even the one whose
    # reference would merely have been absent yet is a lake contradicting its writers.
    assert len(result.held) == len(SESSIONS)
    assert all(finding.finding.check == CHECK_BAR_CLOSE for finding in result.held)
    assert all("LoadError" in (f.finding.exception or "") for f in result.held)
    assert vendor.calls == [], "a reference the walk could not read still spent the request"
    assert result.attempted == 0


@pytest.mark.parametrize(
    "refusal",
    [
        ExtraProjectionError("row 0 holds an extra value that is not JSON"),
        ManifestError("the quarantine ledger holds a line that is not JSON"),
        pa.lib.ArrowInvalid("Parquet magic bytes not found in footer"),
    ],
    ids=["projection", "manifest", "arrow"],
)
def test_a_partition_the_projection_cannot_present_costs_one_ticker_day(
    fixture_lake: FixtureLake, monkeypatch, refusal
):
    """Moving the read in front of the fetch is what made this reachable, so it is named.

    ``ExtraProjectionError`` and the ``UNFIT_ERRORS`` family are not ``LoadError``, so nothing
    in ``lake.bars`` caught them and ``sweep._BARS_REFUSALS`` does not name them either. They
    used to be met only by a ticker-day that had already fetched and passed its span check, so a
    run could finish without ever opening the partition that carries one. Every non-manifested
    daily ticker-day opens it now.

    Unnamed, the first such partition would end the whole sweep before one bar was fetched,
    taking the battery, the report file, the digest, the ping and the minute half with it. The
    minute half is the one the roughly 30-day lookback puts a deadline on, so the cost would
    have been measured in minutes that cannot be re-fetched.

    Marketlake #365 put this family on the filing side and named ``lake.bars`` as its precedent,
    so the blast radius is one ticker-day and the record is a held finding.

    **All three are driven, because covering one covered none of the others.** The loader reaches
    the manifest through ``latest_quarantine_by_check`` and ``withholding`` for the quarantine
    guard, so a malformed ledger raises ``ManifestError`` on this same read, and a partition whose
    footer is not Parquet raises out of ``pq.read_table`` as an ``ArrowInvalid`` the loader does
    not wrap. Each was dropped from the clause on its own and the suite stayed green while a
    sibling case held the other two.
    """
    from lake import bars as bars_module

    quotes = {("SPY", day): [_quote_row(day)] for day in SESSIONS}
    root = _lake(fixture_lake, quotes=quotes)

    def refuse(ticker, day, *args, **kwargs):
        raise refusal

    monkeypatch.setattr(bars_module, "load_quotes", refuse)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ, MINUTE_FREQ)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ, MINUTE_FREQ]}))

    assert len(result.held) == len(SESSIONS), "a broken partition ended the walk"
    named = type(refusal).__name__
    assert all(named in (f.finding.exception or "") for f in result.held)
    assert len(result.landed) == len(SESSIONS), "the minute half went down with the daily half"
    assert {call["symbol"] for call in vendor.calls} == {"SPY"}
    assert result.unsettled == () and result.abandoned == ()


# ``PartialRead`` carries the projection that could not be presented whole, so building one needs
# an ``ExtraProjection``. An empty one is enough here: what this drives is which bucket the refusal
# lands in, never what it says.
_EMPTY_PROJECTION = ExtraProjection(
    table=None, filled=0, unrecorded_versions=(), unfit=(), retyped=()
)


@pytest.mark.parametrize(
    "refusal",
    [
        PartialRead("SPY", "2026-09-09", "quotes", _EMPTY_PROJECTION),
        NoSpotClose("SPY", "2026-09-09", 1),
    ],
    ids=["partial-read", "no-spot-close"],
)
def test_every_refusal_that_means_unreadable_takes_the_skip(
    fixture_lake: FixtureLake, monkeypatch, refusal
):
    """The four ``_gate_close`` contains are one class, so each of them has to be driven.

    They are the four ``actions._observation`` contains out of the same ``load_quotes`` call, and
    what they share is that the session cannot be read. Everything else out of that read says the
    lake contradicts its own writers and is filed instead.

    ``PartialRead`` is the member that nothing reached. Dropping it from the four left the suite
    green, because it is a ``LoadError`` and so fell through to the outer catch and became a held
    finding filed every night, which is the permanent condition wearing an incident's clothes that
    marketlake #434 exists to remove. The other three each had a test.
    """
    from lake import bars as bars_module

    quotes = {("SPY", day): [_quote_row(day)] for day in SESSIONS}
    root = _lake(fixture_lake, quotes=quotes)

    def refuse(ticker, day, *args, **kwargs):
        raise refusal

    monkeypatch.setattr(bars_module, "load_quotes", refuse)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    skips = (*result.abandoned, *result.unsettled)
    assert result.held == (), "a refusal that means unreadable was filed as a finding"
    assert len(skips) == len(SESSIONS)
    assert {entry.reason for entry in skips} == {type(refusal).__name__}
    # The newest session is the one whose following quotes the lake has not sealed, so it is
    # unsettled while the rest are abandoned. Which bucket is the manifest's answer, not the
    # refusal's, so the split is asserted here rather than left to read as noise.
    assert len(result.unsettled) == 1 and len(result.abandoned) == len(SESSIONS) - 1
    assert vendor.calls == []


def test_a_quarantined_reference_is_abandoned_under_its_own_name(fixture_lake: FixtureLake):
    """The reason is what tells an operator whether anything can be done about it.

    ``load_quotes`` refuses a quarantined partition, so a following session the battery withheld
    reads as no reference at all and the bar is abandoned like a gap day. The two are not the
    same to a person: this one clears through ``python -m lake.signoff`` and a gap day clears
    through nothing, because ``recompact_ticker_day`` rebuilds from segments that hold no data
    row either.

    One word cannot carry that, so the run does not try to. It records the class it got, and
    this asserts that the class survives into the entry rather than being flattened into a
    single "no reference" token.
    """
    quotes = {("SPY", day): [_quote_row(day)] for day in SESSIONS}
    fixture_lake.with_quarantine(
        {"partition": "quotes/ticker=SPY/date=2026-09-09.parquet", "verdict": "held"}
    )
    root = _lake(fixture_lake, quotes=quotes)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}))

    assert GateSkip("SPY", DAILY_FREQ, date(2026, 9, 8), "PartitionQuarantined") in result.abandoned
    assert result.held == ()
    assert date(2026, 9, 8) not in {entry.session for entry in result.landed}


def test_the_minute_half_lands_on_a_session_the_daily_gate_refuses(fixture_lake: FixtureLake):
    """The close cross-check is a ``1d`` gate only, which is what the deadline rests on.

    The four sessions this recovery exists for are the four the gate cannot judge, and the minute
    half is the half the roughly 30-day lookback puts a deadline on. If a ``NoSpotClose`` session
    held both frequencies, the deadline-bound half would be unrecoverable and the issue would have
    no purpose.

    **Marketlake #434's skip must not reach it either, and for the same reason.** Deferring a
    daily fetch costs nothing, because Schwab serves daily bars indefinitely. Deferring a minute
    fetch spends part of a roughly 30-day budget. So the skip is keyed on the frequency whose gate
    it guards, and the calls below are what says so: every minute ticker-day still goes out on a
    lake where every daily one would be abandoned.
    """
    quotes = {("SPY", day): [_gap_row(day)] for day in (*SESSIONS, date(2026, 9, 17))}
    root = _lake(fixture_lake, quotes=quotes)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    result = _run(root, vendor)
    assert result.held == ()
    assert result.unsettled == () and result.abandoned == ()
    assert len(vendor.calls) == len(SESSIONS), "the skip reached the deadline-bound half"
    assert {entry.session for entry in result.landed} == set(SESSIONS)


# -- 9. the upper bound ----------------------------------------------------------------


def test_a_session_whose_close_has_not_passed_is_outside_the_range():
    """#319 test 9.

    A run before a session's close would fetch a partial session, land it, and the manifested skip
    would make that partial partition permanent. The bound is the equity close rather than the
    calendar date, so the same clock reading answers both directions.

    The mutation the module docstring names is dropping the close bound, and it fails here: without
    it the in-progress session joins the range.
    """
    midday = datetime(2026, 9, 16, 17, 0, tzinfo=UTC)  # 13:00 Eastern, mid-session
    assert _plan(now=midday).ceiling == date(2026, 9, 15)
    assert _plan(now=TONIGHT).ceiling == date(2026, 9, 16)


def test_the_bound_is_the_close_itself_rather_than_the_end_of_the_day():
    """The close itself separates two runs three minutes apart on the same afternoon.

    Nothing coarser than the close can tell them apart, which is what rules out a calendar-date
    bound.
    """
    just_before = datetime(2026, 9, 16, 19, 58, tzinfo=UTC)
    just_after = datetime(2026, 9, 16, 20, 1, tzinfo=UTC)
    assert _plan(now=just_before).ceiling == date(2026, 9, 15)
    assert _plan(now=just_after).ceiling == date(2026, 9, 16)


# -- 10 and 11. the frequency check ----------------------------------------------------


def test_a_frequency_with_no_vendor_call_is_refused_before_any_request(fixture_lake: FixtureLake):
    """#319 test 10.

    Checked over the plan's own ticker-days rather than over ``roster.enabled``, because this walk
    reaches retired tickers and that function's own docstring rests on nothing here ever fetching
    one. The refusal comes before any request has gone out.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette())
    roster = _roster({"SPY": ["5m"]}, enabled={"SPY": False})
    with pytest.raises(UnsupportedBarFreq) as caught:
        _run(root, vendor, roster=roster)
    assert caught.value.freq == "5m"
    assert vendor.calls == []


def test_reusing_the_per_session_check_would_let_a_retired_frequency_through():
    """#319 test 11, which is what test 10 buys.

    ``_require_supported`` reads ``roster.enabled``, so a disabled entry's unsupported frequency
    passes it. That frequency then reaches ``bar_window``, which raises a bare ``ValueError`` out
    of ``require_bar_freq`` from outside the walk's catch, ending a run on a traceback with no
    report. This drives both halves directly, so a mutation swapping the plan's check for the
    roster's fails here rather than somewhere downstream.
    """
    roster = _roster({"SPY": ["5m"]}, enabled={"SPY": False})
    bars._require_supported(roster)  # passes: the entry is disabled, so it is not checked

    plan = plan_backfill(
        spans=_open_span(1),
        master=_master(),
        roster=roster,
        clock=ManualClock(TONIGHT),
        calendar=_calendar(),
    )
    with pytest.raises(UnsupportedBarFreq):
        bars._require_supported_plan(plan)

    session_clock = bars.SessionClock(ManualClock(TONIGHT), _calendar())
    with pytest.raises(ValueError):
        bars.bar_window("5m", session_clock.bounds(SESSIONS[0]))


# -- 12. a run that stops part-way -----------------------------------------------------


def _landed_partitions(root: Path, freq: str = MINUTE_FREQ) -> list[tuple[str, date]]:
    """Every bars partition on disk, read back off the files rather than off a report."""
    return [
        (ticker, day)
        for ticker in ("SPY", "QQQ")
        for day in SESSIONS
        if _partition(root, ticker, freq, day).exists()
    ]


def test_a_run_that_dies_on_auth_resumes_from_what_it_landed(fixture_lake: FixtureLake):
    """#319 test 12.

    ``VendorAuthError`` stops the run rather than being contained, which #280 decided: every
    remaining ticker-day fails it identically. What makes that survivable over a range is the
    manifested skip, so the run after a new token costs nothing for the ticker-days already
    landed.

    **The death has to land part-way, and that is the whole difficulty of writing this.** The plan
    is sorted, so a failure keyed on a ticker falls on the very first call and leaves nothing
    behind. A test written that way asserts that the next run skipped zero and asked for all of
    them, which is true of a run that never started and says nothing about resuming. ``fail_after``
    is what puts the death in the middle instead, and the assertion below refuses a run that landed
    nothing so the tautology cannot come back.
    """
    root = _lake(fixture_lake, spans=_open_span(1, 2), master=_master(("SPY", "QQQ")))
    roster = _roster({"SPY": [MINUTE_FREQ], "QQQ": [MINUTE_FREQ]})
    total = 2 * len(SESSIONS)
    dying = RecordingVendor(
        _cassette(tickers=("SPY", "QQQ"), freqs=(MINUTE_FREQ,)),
        fail_after=5,
        failure=VendorAuthError("the refresh token is dead"),
    )
    with pytest.raises(VendorAuthError):
        _run(root, dying, roster=roster, spans=_open_span(1, 2))

    # Five calls went out and the sixth raised, so the run stopped there rather than carrying on.
    assert len(dying.calls) == 6
    landed = _landed_partitions(root)
    assert 0 < len(landed) < total

    resumed = RecordingVendor(_cassette(tickers=("SPY", "QQQ"), freqs=(MINUTE_FREQ,)))
    result = _run(root, resumed, roster=roster, spans=_open_span(1, 2))
    assert result.skipped == len(landed)
    assert len(resumed.calls) == total - len(landed)
    assert len(_landed_partitions(root)) == total


def test_the_death_stops_the_run_rather_than_being_held_per_ticker_day(fixture_lake: FixtureLake):
    """A dead token is not contained, so no ticker-day after it is even asked for.

    This is the half the resume test cannot show, and it is what ``RecordingVendor`` keeping its
    calls exists for: the assertion is about the requests that did *not* go out.
    """
    root = _lake(fixture_lake, spans=_open_span(1, 2), master=_master(("SPY", "QQQ")))
    roster = _roster({"SPY": [MINUTE_FREQ], "QQQ": [MINUTE_FREQ]})
    dying = RecordingVendor(
        _cassette(tickers=("SPY", "QQQ"), freqs=(MINUTE_FREQ,)),
        fail_after=3,
        failure=VendorAuthError("the refresh token is dead"),
    )
    with pytest.raises(VendorAuthError):
        _run(root, dying, roster=roster, spans=_open_span(1, 2))
    assert len(dying.calls) == 4
    assert len(dying.calls) < 2 * len(SESSIONS)


# -- 13. an instrument the master cannot name ------------------------------------------


def test_a_session_the_master_cannot_name_a_ticker_for_is_reported_not_fetched():
    """#319 test 13.

    A span carries an ``instrument_id`` and no ticker, so the walk asks ``symbol_at`` per session.
    ``None`` is the master's own floor arriving from the other side: a session inside a span but
    before the mapping's ``valid_from`` has no symbol to ask the vendor for. The live master sets
    ``valid_from`` to 2026-09-08 for both tickers, which is why this moves it later rather than
    earlier to reach the condition.
    """
    plan = _plan(master=_master(valid_from=date(2026, 9, 11)))
    assert plan.floor == date(2026, 9, 8)
    assert {day.session for day in plan.days} == set(SESSIONS[3:])
    assert len(plan.unwalked) == 3
    assert all("names no ticker" in line for line in plan.unwalked)


def test_a_re_symboling_inside_a_span_resolves_each_session_to_its_own_ticker():
    """Asking per session rather than once per span is what makes a rename land correctly.

    The master is asked at the observation date everywhere else in this module, and the backfill
    does the same in the opposite direction. A ticker that changed mid-span would otherwise put
    every session under whichever name one lookup happened to return.
    """
    master = _master()
    master.remap(1, ID_TYPE_TICKER, "SPYX", date(2026, 9, 14))
    plan = _plan(master=master, roster=_roster({"SPY": [MINUTE_FREQ], "SPYX": [MINUTE_FREQ]}))
    by_session = {day.session: day.ticker for day in plan.days}
    assert by_session[date(2026, 9, 11)] == "SPY"
    assert by_session[date(2026, 9, 14)] == "SPYX"


# -- 14. the spans file reaching the operator ------------------------------------------


def test_an_absent_spans_file_reaches_the_operator_as_one_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#319 test 14, first half.

    ``CaptureSpans.read`` raises ``OSError`` for an absent file, which is not a ``BarsError``, so
    without a name of its own the command would end on a stack. A lake with a master and no spans
    file is a real state rather than a corrupt one, so the line names the seeding command.
    """
    root = _lake(fixture_lake)
    spans_path(root).unlink()
    config = write_config(tmp_path, root)
    code = bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: RecordingVendor(_cassette()),
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "no capture spans" in err
    assert "python -m lake.seed_spans" in err
    assert "Traceback" not in err


def test_a_torn_spans_file_reaches_the_operator_as_one_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#319 test 14, second half.

    A torn file wants a restore rather than a seed, which is why the two are told apart. It is
    ``SpansUnreadable``, which ``capture_spans`` already separates from an absent file.
    """
    root = _lake(fixture_lake)
    spans_path(root).write_bytes(b"not parquet at all")
    config = write_config(tmp_path, root)
    code = bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: RecordingVendor(_cassette()),
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "not readable parquet" in err
    assert "Restore it from the backup" in err
    assert "Traceback" not in err


def test_the_absent_spans_error_carries_the_path_it_looked_at(fixture_lake: FixtureLake):
    """The refusal names where it looked, so an operator with two lakes knows which one is bare."""
    root = _lake(fixture_lake)
    spans_path(root).unlink()
    with pytest.raises(SpansAbsent) as caught:
        bars.read_capture_spans(root)
    assert caught.value.path == spans_path(root)


# -- 15. one ticker-day, fetched once --------------------------------------------------


def test_two_spans_covering_one_session_plan_it_once():
    """#319 test 15, first half.

    Retiring an instrument mid-session and bringing it back the same afternoon leaves two spans
    that both intersect that session, which ``open_span`` permits because it refuses only a second
    *open* span. Planned twice, that session would spend two vendor requests on one partition and
    append two manifest entries for one path, which is the cost ``_ticker_freqs`` already names
    for a roster listing one frequency twice.
    """
    rejoined = _spans(
        (1, SPAN_START, datetime(2026, 9, 9, 15, 0, tzinfo=UTC)),
        (1, datetime(2026, 9, 9, 18, 0, tzinfo=UTC), None),
    )
    plan = _plan(spans=rejoined)
    assert len(plan.days) == len(set(plan.days))
    assert TickerDay("SPY", MINUTE_FREQ, date(2026, 9, 9)) in plan.days
    assert sum(1 for day in plan.days if day.session == date(2026, 9, 9)) == 1


def test_two_instruments_naming_one_ticker_on_one_day_plan_it_once():
    """#319 test 15, second half, reaching the same hazard from the other side."""
    master = SecurityMaster()
    for _ in range(2):
        master.register(
            kind=KIND_EQUITY, capture_start=SPAN_START, valid_from=date(2026, 9, 8), ticker="SPY"
        )
    plan = _plan(spans=_open_span(1, 2), master=master)
    assert len(plan.days) == len(set(plan.days))
    assert len(plan.days) == len(SESSIONS)


def test_the_plan_is_ordered_newest_session_first_then_ticker_then_frequency():
    """The newest session in range is walked first, and two runs produce the same order.

    A set is what dedupes and a set has no order, so the sort is what keeps the sign-off block and
    the recorded calls readable rather than shuffled between runs. That half is unchanged and is
    what this test was always protecting.

    **The direction inverted under marketlake #478, and the reason is the budget.** The walk takes
    this list in order and spends a bounded number of requests over it, so whatever sits at the
    front is what a bounded run spends itself on. Ascending put the oldest session there, and the
    oldest ``1m`` ticker-days are the ones past Schwab's lookback, which can never land and are
    never manifested and so are asked for again on every later run. A budget over that order
    reaches today's bars on no run at all, while an unbudgeted walk lands them on the first.

    The tie-break inside a session is untouched, and the assertion below is what says so: ticker
    before frequency, both ascending, so ``QQQ`` at ``1m`` still precedes ``SPY`` at ``1d``.
    """
    plan = _plan(
        spans=_open_span(1, 2),
        master=_master(("SPY", "QQQ")),
        roster=_roster({"SPY": [MINUTE_FREQ, DAILY_FREQ], "QQQ": [MINUTE_FREQ, DAILY_FREQ]}),
    )
    keys = [(day.session, day.ticker, day.freq) for day in plan.days]
    # Determinism, stated as the property rather than as a literal: sessions descend, and within
    # one session the pair ascends. A `sorted(keys)` comparison cannot express that, because the
    # two halves of the key now run in opposite directions.
    assert keys == sorted(keys, key=lambda key: (-key[0].toordinal(), key[1], key[2]))
    assert keys[:4] == [
        (SESSIONS[-1], "QQQ", DAILY_FREQ),
        (SESSIONS[-1], "QQQ", MINUTE_FREQ),
        (SESSIONS[-1], "SPY", DAILY_FREQ),
        (SESSIONS[-1], "SPY", MINUTE_FREQ),
    ]
    assert keys[-1] == (SESSIONS[0], "SPY", MINUTE_FREQ)
    # The range's own ends are read off ``sessions``, which keeps its ascending sort, so inverting
    # ``days`` must not move them. A reversal of the whole sorted list would have.
    assert plan.floor == SESSIONS[0]
    assert plan.ceiling == SESSIONS[-1]


# -- what one run leaves on disk, and what the report says ------------------------------


def test_a_run_lands_a_partition_and_its_manifest_entry_per_session(fixture_lake: FixtureLake):
    """The whole range lands, one partition and one manifest entry per ticker-day.

    This is the end-to-end reading: what an operator gets from a first run over a lake whose
    quotes all settled.
    """
    root = _lake(fixture_lake)
    result = _run(root, RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))))
    assert len(result.landed) == len(SESSIONS)
    assert result.floor == date(2026, 9, 8)
    assert result.ceiling == date(2026, 9, 16)
    assert result.sessions == len(SESSIONS)
    entries = [entry["partition"] for entry in read_manifest(root)]
    for day in SESSIONS:
        partition = _partition(root, "SPY", MINUTE_FREQ, day)
        assert partition.exists()
        assert partition.relative_to(root).as_posix() in entries
        assert entries.count(partition.relative_to(root).as_posix()) == 1


def test_an_empty_range_is_reported_rather_than_read_as_a_run_that_did_nothing():
    """A span that has not covered a completed session yet plans nothing, and the report says so.

    A run landing nothing has three causes an operator has to tell apart: an empty range, a range
    already landed, and a range entirely held. ``floor`` being ``None`` is what names the first.
    """
    plan = _plan(spans=_open_span(1, start=datetime(2026, 9, 16, 22, 0, tzinfo=UTC)))
    assert plan.days == ()
    assert plan.floor is None
    assert plan.ceiling is None


def test_the_sign_off_block_names_the_range_and_every_count(fixture_lake: FixtureLake):
    """The rendered block is what a hand-run operator reads, so it carries the range it walked."""
    root = _lake(fixture_lake)
    result = _run(root, RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))))
    rendered = result.render()
    assert "2026-09-08..2026-09-16, 7 session(s)" in rendered
    assert "landed:  7" in rendered
    assert "skipped: 0" in rendered
    assert "unwalked: 0" in rendered


def test_an_unwalked_ticker_day_is_named_in_the_sign_off_block():
    """``unwalked`` is a report line rather than a raise, so the block has to show it."""
    plan = _plan(
        spans=_open_span(1, 2),
        master=_master(("SPY", "QQQ")),
        roster=_roster({"SPY": [MINUTE_FREQ]}),
    )
    rendered = bars.BackfillReport(
        floor=plan.floor,
        ceiling=plan.ceiling,
        sessions=len(plan.sessions),
        attempted=0,
        landed=(),
        held=(),
        skipped=0,
        unwalked=plan.unwalked,
    ).render()
    assert f"unwalked: {len(SESSIONS)}" in rendered
    assert "QQQ on 2026-09-08" in rendered


def test_the_command_runs_the_backfill_and_exits_zero(fixture_lake: FixtureLake, tmp_path: Path):
    """``--backfill`` end to end, which is the only thing that exercises the command's contract.

    The two refusal tests above stop inside ``_read_spans`` and never reach ``backfill_bars``, so
    without this nothing runs ``BackfillReport.unfiled``, nothing prints a backfill's sign-off
    block, and exit 0 for this flag has nothing behind it. "A run nobody can start is a library
    with no entry point" is the issue's own argument for the flag existing, and this is what says
    it can be started.
    """
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    code = bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: vendor,
    )
    assert code == 0
    assert len(vendor.calls) == len(SESSIONS)
    for day in SESSIONS:
        assert _partition(root, "SPY", MINUTE_FREQ, day).exists()


def test_the_command_prints_the_backfill_block_rather_than_the_session_one(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """The two commands print different blocks, and the flag is what picks which.

    Without this a dispatch that ran the per-session fetch under ``--backfill`` would pass every
    other test here, because those call ``backfill_bars`` directly and never go through ``main``.
    """
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)
    bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))),
    )
    out = capsys.readouterr().out
    assert "Bar backfill over 2026-09-08..2026-09-16, 7 session(s)" in out
    assert "Bar fetch for" not in out


def test_the_default_command_still_fetches_one_session(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """No flag means the evening run, unchanged. The flag adds a path rather than moving one."""
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root)
    bars.main(
        ["--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))),
    )
    out = capsys.readouterr().out
    assert "Bar fetch for 2026-09-16" in out
    assert "Bar backfill" not in out


def test_a_held_finding_is_rendered_in_the_backfill_block(fixture_lake: FixtureLake):
    """The held block is one spelling shared with the per-session report, and this is its reader.

    The findings are driven by a refused request rather than by an absent close of record.
    Marketlake #434 made the second of those a skip before the fetch, so it produces no finding
    to render and this test would otherwise have asserted an empty block.
    """
    quotes = {("SPY", day): [_quote_row(day)] for day in (*SESSIONS, date(2026, 9, 17))}
    root = _lake(fixture_lake, quotes=quotes)
    result = _run(
        root,
        RecordingVendor(_cassette(freqs=(DAILY_FREQ,)), fail_with={"SPY": VendorError("refused")}),
        roster=_roster({"SPY": [DAILY_FREQ]}),
    )
    rendered = result.render()
    assert f"held:    {len(SESSIONS)}" in rendered
    assert f"SPY 1d 2026-09-08 {CHECK_BAR_RESPONSE}" in rendered
    assert "VendorError" in rendered
    assert "filed at" in rendered


def test_the_two_gate_skip_blocks_are_rendered_in_the_backfill_block(fixture_lake: FixtureLake):
    """Both lists reach the reader of the command's own output, in full and counted.

    Marketlake #434's two skips are the run's whole answer for a daily ticker-day it did not
    fetch, so a block that did not render them would leave that ticker-day looking as though the
    walk had never planned it. The nightly digest carries one counted line instead, for its own
    byte budget, and this is the output that carries the detail.

    The lake below gaps 09-08 through 09-11 and settles the rest, so one run produces both: three
    sessions whose reference sealed empty, and the newest session whose reference is still owed.
    """
    # 2026-09-17 is deliberately absent, so the newest session in range has no reference yet
    # while the outage's three have one that sealed empty.
    quotes = {
        ("SPY", day): [_gap_row(day)] if day <= date(2026, 9, 11) else [_quote_row(day)]
        for day in SESSIONS
    }
    root = _lake(fixture_lake, quotes=quotes)
    result = _run(
        root, RecordingVendor(_cassette(freqs=(DAILY_FREQ,))), roster=_roster({"SPY": [DAILY_FREQ]})
    )
    # **The two blocks are asserted whole, in order.** Substring checks leave the structure
    # unpinned: swapping the blocks, so a reader meets the permanent list before the transient
    # one, changed nothing any of them could see.
    #
    # The entries inside a block descend by session under marketlake #478, because the walk
    # produces them in the order it reaches them and this block renders that order rather than
    # imposing one of its own.
    rendered = result.render().splitlines()
    first = rendered.index("  unsettled: 1")
    assert rendered[first : first + 6] == [
        "  unsettled: 1",
        "    - SPY 1d 2026-09-16: PartitionAbsent",
        "  abandoned: 3",
        "    - SPY 1d 2026-09-10: NoSpotClose",
        "    - SPY 1d 2026-09-09: NoSpotClose",
        "    - SPY 1d 2026-09-08: NoSpotClose",
    ]


def test_the_two_reports_render_a_held_finding_the_same_way():
    """One spelling, asserted rather than assumed, because two copies is how the two drift."""
    finding = report.Withheld(
        symbol="SPY",
        observed_on=date(2026, 9, 8),
        event=DAILY_FREQ,
        check=CHECK_BAR_CLOSE,
        computed=None,
        against=None,
        exception="NoSpotClose: nothing settled",
    )
    held = (bars.HeldFinding(finding=finding, filed_at="reports/withheld/x.json"),)
    session_lines = bars.BarsReport(
        session=date(2026, 9, 8), attempted=1, landed=(), held=held, skipped=0
    ).render()
    backfill_lines = bars.BackfillReport(
        floor=date(2026, 9, 8),
        ceiling=date(2026, 9, 8),
        sessions=1,
        attempted=1,
        landed=(),
        held=held,
        skipped=0,
        unwalked=(),
    ).render()
    shared = [line for line in session_lines.splitlines() if line.startswith(("  held", "    "))]
    assert shared
    assert shared == [
        line for line in backfill_lines.splitlines() if line.startswith(("  held", "    "))
    ]


# -- a span covering no instant ---------------------------------------------------------


def test_a_span_covering_no_instant_covers_no_session(fixture_lake: FixtureLake):
    """A zero-length span is empty by the lake's own scope answer, so the walk must agree.

    ``close_span`` refuses an end before the start and permits one equal to it, so a retire landing
    in the same microsecond as the onboard leaves ``[t, t)``. ``CaptureSpans.in_scope`` answers
    ``False`` for that instant and either side of it. Without the guard the intersection test
    answers ``True`` for the whole session, and the walk spends a request and lands a permanent
    partition for a session capture never covered, which is what taking the range from the spans
    rather than from a typed date is supposed to make impossible.
    """
    instant = datetime(2026, 9, 10, 17, 0, tzinfo=UTC)
    empty = _spans((1, instant, instant))
    assert not empty.in_scope(1, instant)
    assert empty.spans_covering(instant) == ()

    root = _lake(fixture_lake, spans=empty)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    result = _run(root, vendor, spans=empty)
    assert result.sessions == 0
    assert vendor.calls == []
    assert result.landed == ()


def test_a_span_of_one_microsecond_does_cover_its_session():
    """The guard refuses the empty span and not the short one, which is the floor rule itself.

    A span holding a single instant of a session puts that whole session in range, because a bar
    is not clipped to the span. Guarding on ``end <= start`` rather than on a duration is what
    keeps these two apart.
    """
    instant = datetime(2026, 9, 10, 17, 0, tzinfo=UTC)
    tiny = _spans((1, instant, instant + timedelta(microseconds=1)))
    assert tiny.in_scope(1, instant)
    assert _plan(spans=tiny).sessions == (date(2026, 9, 10),)


def test_a_spans_file_from_a_newer_schema_reaches_the_operator_as_one_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """``UnsupportedSpansSchemaVersion`` is a sibling of ``SpansUnreadable``, not a stack.

    ``main`` catches the ``CaptureSpansError`` class rather than naming one member, which is what
    the four other readers of this file already do. No fix is prescribed, because the exception's
    own message says what is wrong and a wrong instruction is worse than none.
    """
    root = _lake(fixture_lake)
    table = pa.table(
        {
            "instrument_id": [1],
            "span_start": [SPAN_START],
            "span_end": [None],
            "options": [True],
            "schema_version": [SPANS_SCHEMA_VERSION + 1],
        },
        schema=SPANS_SCHEMA,
    )
    pq.write_table(table, spans_path(root))
    config = write_config(tmp_path, root)
    code = bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: RecordingVendor(_cassette()),
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "capture-spans schema version 2, this code reads 1" in err
    assert "Traceback" not in err


def test_an_unreadable_spans_file_that_is_present_is_not_reported_as_absent(
    fixture_lake: FixtureLake,
):
    """``_read_spans`` names only ``FileNotFoundError``, which is where ``_read_master`` draws it.

    Catching the whole ``OSError`` family would tell an operator whose file is there but
    unreadable that there is no file, and send them to the seeder, which reads the same file and
    fails the same way.
    """
    root = _lake(fixture_lake)
    spans_path(root).chmod(0o000)
    try:
        with pytest.raises(PermissionError):
            bars.read_capture_spans(root)
    finally:
        spans_path(root).chmod(0o644)


def test_the_ceiling_takes_the_session_at_exactly_its_close():
    """The rule reads "at or past the close", and the boundary instant is where that is decided.

    At ``now == equity_close`` the ``1m`` window the fetch asks for has just finished, so the
    session is complete and belongs in the range. One microsecond earlier it does not.
    """
    close = datetime.fromisoformat("2026-09-16T16:00:00-04:00")
    assert _plan(now=close).ceiling == date(2026, 9, 16)
    assert _plan(now=close - timedelta(microseconds=1)).ceiling == date(2026, 9, 15)


def test_the_walk_asks_for_one_window_per_ticker_day(fixture_lake: FixtureLake):
    """The fetch shape, asserted off the recorded calls rather than off the plan.

    One request per ticker-day per frequency is the choice #319 made, and it is what makes the
    skip avoid the call, what sidesteps marketlake #333's ``period`` exposure, and what lets
    #280's one-session gate stand unchanged. A per-span shape would show as one call.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette())
    _run(root, vendor, roster=_roster({"SPY": [MINUTE_FREQ, DAILY_FREQ]}))
    assert len(vendor.calls) == 2 * len(SESSIONS)
    assert {call["freq"] for call in vendor.calls} == {MINUTE_FREQ, DAILY_FREQ}


# -- the per-run request budget, marketlake #478 ---------------------------------------


def _budget(value: int) -> GuardConstants:
    """The guards a run takes when only its request budget matters."""
    return GuardConstants(bars_request_budget=value)


def test_the_budget_bounds_what_one_run_spends_at_the_vendor(fixture_lake: FixtureLake):
    """#478's whole point: a run cannot spend more requests than it was allowed.

    Nothing paces this walk. ``_walk`` loops, ``_fetch`` forwards, and neither ``schwab`` nor
    ``vendor`` retries or backs off, so an unbounded run over a rebuilt manifest fires everything
    the spans cover back to back and crosses the vendor's 120-a-minute ceiling inside its first
    minute. The bound is what makes the crossing arithmetically impossible.

    ``calls`` is the assertion that matters rather than ``attempted``. A counter that stopped
    incrementing while the requests kept going out would satisfy the report and defeat the guard,
    and only the recorded vendor calls can tell those apart.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    result = _run(root, vendor, guards=_budget(3))

    assert len(vendor.calls) == 3
    assert result.attempted == 3
    assert len(result.landed) == 3
    # Seven sessions were planned and three were fetched, so four are left for the next run.
    assert len(result.deferred) == len(SESSIONS) - 3


def test_the_budget_spends_itself_on_the_newest_sessions_first(fixture_lake: FixtureLake):
    """The bound is spent at the deadline end of the range, not the oldest end.

    Schwab serves a roughly 30-day one-minute lookback and daily bars indefinitely, so a recent
    session is the one that stops being fetchable. A budget taken over the old chronological order
    would spend itself on the oldest sessions, which on a real lake are the ones that can never
    land, and today's bars would be reached on no run at all.

    This is the assertion that fails if anybody restores the ascending sort. Both halves are
    checked, because a walk could reach the right sessions and report the wrong remainder.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    result = _run(root, vendor, guards=_budget(2))

    assert [entry.session for entry in result.landed] == [SESSIONS[-1], SESSIONS[-2]]
    assert [day.session for day in result.deferred] == list(reversed(SESSIONS[:-2]))


def test_a_budgeted_run_still_counts_every_skip_behind_the_bound(fixture_lake: FixtureLake):
    """The budget refuses requests. It does not truncate the walk.

    A loop that stopped iterating at exhaustion would stop counting the manifested skips and the
    gate skips behind the stop point, so ``skipped``, ``unsettled`` and ``abandoned`` would all go
    partial. ``lake.sweep`` renders its ``bars abandoned:`` census off that last one, and
    marketlake #434 built the line to surface the permanent gaps an operator has to find, so a
    truncating budget would move the count for a reason that is not a change in the lake.

    The lake below gaps 09-08 through 09-11, so the three oldest daily ticker-days are abandoned.
    They sit at the *back* of the newest-first walk, behind a budget of one, and the run has to
    reach and count them anyway.
    """
    quotes = {
        ("SPY", day): [_gap_row(day)] if day <= date(2026, 9, 11) else [_quote_row(day)]
        for day in (*SESSIONS, date(2026, 9, 17))
    }
    root = _lake(fixture_lake, quotes=quotes)
    vendor = RecordingVendor(_cassette(freqs=(DAILY_FREQ,)))
    result = _run(root, vendor, roster=_roster({"SPY": [DAILY_FREQ]}), guards=_budget(1))

    assert len(vendor.calls) == 1
    # Counted in full despite sitting behind an exhausted budget, and in the walk's own order.
    assert [entry.session for entry in result.abandoned] == [
        date(2026, 9, 10),
        date(2026, 9, 9),
        date(2026, 9, 8),
    ]
    # A gate skip costs no request, so it is never deferred: the two records mean different things
    # and a ticker-day belongs to exactly one of them.
    deferred = {(day.ticker, day.freq, day.session) for day in result.deferred}
    abandoned = {(entry.ticker, entry.freq, entry.session) for entry in result.abandoned}
    assert deferred & abandoned == set()


def test_a_budgeted_run_still_counts_a_manifested_skip_behind_the_bound(
    fixture_lake: FixtureLake,
):
    """``skipped`` is complete too, which is the same guarantee from the cheap side.

    A manifested ticker-day costs no vendor call, so a walk that stopped at the bound would report
    a lake as holding fewer landed partitions than it does. The oldest session's partition is
    already manifested here and sits behind a budget of one.
    """
    landed_day = SESSIONS[0]
    root = _lake(
        fixture_lake,
        bars_partitions=(("SPY", DAILY_FREQ, landed_day, _bars_table(landed_day)),),
    )
    result = _run(
        root,
        RecordingVendor(_cassette(freqs=(DAILY_FREQ,))),
        roster=_roster({"SPY": [DAILY_FREQ]}),
        guards=_budget(1),
    )

    assert result.skipped == 1
    assert landed_day not in {day.session for day in result.deferred}


def test_the_next_run_picks_up_what_the_budget_deferred(fixture_lake: FixtureLake):
    """Convergence, as far as a budget alone reaches it, and with no new persistent state.

    A ticker-day the budget did not reach was never fetched, so it was never manifested, so the
    next run's plan still holds it. That is the whole mechanism: nothing is written down and
    nothing has to be cleared. What the first run landed is skipped for free the second time, so
    the second run's budget buys new sessions rather than repeating the first one's.
    """
    root = _lake(fixture_lake)
    cassette = _cassette(freqs=(MINUTE_FREQ,))

    first = _run(root, RecordingVendor(cassette), guards=_budget(2))
    second_vendor = RecordingVendor(cassette)
    second = _run(root, second_vendor, guards=_budget(2))

    assert [entry.session for entry in first.landed] == [SESSIONS[-1], SESSIONS[-2]]
    assert second.skipped == 2
    # The second run spends its whole budget on ground the first never covered.
    assert [entry.session for entry in second.landed] == [SESSIONS[-3], SESSIONS[-4]]
    assert len(second_vendor.calls) == 2
    assert len(second.deferred) == len(first.deferred) - 2


def test_a_budget_the_run_never_reaches_defers_nothing(fixture_lake: FixtureLake):
    """An ordinary evening is not throttled, and the report says nothing about a bound.

    The pinned budget is 100 against a live plan of a few ticker-days, so the bound is inert on
    every healthy run. A report that named a budget it never met would put a line in the nightly
    digest every evening for a condition that had not occurred.
    """
    root = _lake(fixture_lake)
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    result = _run(root, vendor, guards=_budget(len(SESSIONS)))

    assert len(vendor.calls) == len(SESSIONS)
    assert result.deferred == ()
    assert "deferred: 0" in result.render()


def test_the_pinned_default_is_what_a_run_takes_when_no_guards_are_passed(
    fixture_lake: FixtureLake,
):
    """``None`` resolves to the design's pinned defaults in the callee, the way ``judge`` does.

    ``lake.sweep`` passes whatever it holds straight through, so the resolution has to happen here
    or a sweep constructed without guards would walk unbounded.
    """
    root = _lake(fixture_lake)
    assert GuardConstants().bars_request_budget == 100
    result = _run(root, RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))), guards=None)

    # Seven sessions against a budget of 100, so nothing is deferred and the default was in force
    # rather than the budget being ignored: the next test's bound proves the argument is read.
    assert result.deferred == ()
    assert result.attempted == len(SESSIONS)


def test_the_deferred_block_is_rendered_for_the_by_hand_reader(fixture_lake: FixtureLake):
    """The command's own output names every deferred ticker-day, counted and in full.

    The nightly digest counts instead, because its line repeats every evening inside a byte cap.
    This is the output somebody ran and is waiting on, and ``_render_gate_skips`` makes the same
    argument about its own two lists.
    """
    root = _lake(fixture_lake)
    result = _run(root, RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))), guards=_budget(2))
    rendered = result.render().splitlines()
    first = rendered.index(f"  deferred: {len(SESSIONS) - 2}")
    assert rendered[first : first + 3] == [
        f"  deferred: {len(SESSIONS) - 2}",
        f"    - SPY 1m {SESSIONS[-3].isoformat()}",
        f"    - SPY 1m {SESSIONS[-4].isoformat()}",
    ]


def test_the_single_session_fetch_is_not_bounded_by_the_budget(fixture_lake: FixtureLake):
    """``fetch_session_bars`` reaches the vendor through the same walk and stays unbounded.

    Its scope is one session against the enabled roster, which is a handful of ticker-days, and
    the runs that have crossed the ceiling through it number zero. The budget is the backfill's,
    and a test is what says that is deliberate rather than an oversight.
    """
    quotes = {
        (ticker, day): [_quote_row(day, ticker=ticker)]
        for ticker in ("SPY", "QQQ")
        for day in (*SESSIONS, date(2026, 9, 17))
    }
    root = _lake(fixture_lake, quotes=quotes, master=_master(("SPY", "QQQ")))
    vendor = RecordingVendor(_cassette(tickers=("SPY", "QQQ"), freqs=(MINUTE_FREQ,)))
    report = bars.fetch_session_bars(
        lake_root=root,
        vendor=vendor,
        clock=ManualClock(TONIGHT),
        calendar=_calendar(),
        roster=_roster({"SPY": [MINUTE_FREQ], "QQQ": [MINUTE_FREQ]}),
        session=SESSIONS[-1],
    )
    # Both tickers reached the vendor. The walk they share carries the budget argument and this
    # entry point passes none, so nothing here can be bounded by it.
    assert report.attempted == 2
    assert len(vendor.calls) == 2
    assert not hasattr(report, "deferred")


def test_the_backfill_command_hands_the_walk_the_config_s_budget(
    fixture_lake: FixtureLake, tmp_path: Path
):
    """The one line connecting `config.yaml` to the by-hand command, driven end to end.

    ``backfill_bars_from_config`` passes `guards=config.guards`, and dropping that kwarg leaves
    every other test here green: they call ``backfill_bars`` directly and hand it guards
    themselves, so none of them crosses the wiring. The operator who recalibrates the budget is
    the whole argument for the constant living in ``config.yaml`` rather than in this module, and
    without this test their edit is silently ignored by ``--backfill``.

    The budget is set to 2 against a seven-session plan, so the assertion is on requests that did
    not go out rather than on a report field. Only the recorded calls can tell a budget that was
    read from one that was defaulted.
    """
    root = _lake(fixture_lake)
    config = write_config(tmp_path, root, guards={"bars_request_budget": 2})
    vendor = RecordingVendor(_cassette(freqs=(MINUTE_FREQ,)))
    code = bars.main(
        ["--backfill", "--config", str(config), "--tickers", str(_tickers_file(tmp_path))],
        clock=ManualClock(TONIGHT),
        vendor_factory=lambda *a, **k: vendor,
    )
    assert code == 0
    assert len(vendor.calls) == 2, "the config's budget never reached the walk"
    # The two it reached are the newest, and the five it did not are still unmanifested, so
    # tomorrow's run picks them up. ``SESSIONS`` is ascending, so this compares as a set.
    landed = {day for day in SESSIONS if _partition(root, "SPY", MINUTE_FREQ, day).exists()}
    assert landed == {SESSIONS[-1], SESSIONS[-2]}


def test_the_pinned_default_binds_a_plan_larger_than_it(fixture_lake: FixtureLake):
    """``None`` resolves to the pinned 100, proven by a plan that 100 is smaller than.

    This is the half a seven-session fixture cannot show. With the plan below the budget, a run
    that resolved ``None`` to the pinned default and a run that walked unbounded report the same
    four numbers, so a test built on it passes whether the resolution is there or not. The
    resolution is what ``lake.sweep`` depends on, because it forwards whatever it holds and the
    callee decides, so a missing one walks the nightly job unbounded.

    Fifteen tickers over seven sessions is 105 ticker-days at one frequency, which is the
    cheapest plan larger than the pinned budget. ``1m`` carries it because that half runs no close
    cross-check and so needs no quotes seeded per ticker.
    """
    tickers = tuple(f"T{index:02d}" for index in range(15))
    root = _lake(fixture_lake, master=_master(tickers), spans=_open_span(*range(1, 16)))
    vendor = RecordingVendor(_cassette(tickers=tickers, freqs=(MINUTE_FREQ,)))
    result = _run(
        root,
        vendor,
        roster=_roster({ticker: [MINUTE_FREQ] for ticker in tickers}),
        spans=_open_span(*range(1, 16)),
        guards=None,
    )

    assert len(tickers) * len(SESSIONS) == 105, "the plan has to be larger than the budget"
    assert result.attempted == GuardConstants().bars_request_budget == 100
    assert len(vendor.calls) == 100
    assert len(result.deferred) == 5


def test_the_deferred_block_renders_before_the_unwalked_one(fixture_lake: FixtureLake):
    """The sign-off block's order is pinned, not just its contents.

    ``test_the_two_gate_skip_blocks_are_rendered_in_the_backfill_block`` makes this argument for
    its own pair and its comment says why: "swapping the blocks... changed nothing any of them
    could see." The deferred block arrived without that treatment, and moving it below
    ``unwalked`` left every assertion about it green, because the one test that reads it locates
    its line by index and slices forward from there.

    A reader meets the run's own remainder before the reference data it could not name, which is
    the order the two mean something in: one is work this run left, the other is work no run can
    do.
    """
    root = _lake(fixture_lake, master=_master(("SPY", "QQQ")))
    result = _run(
        root,
        RecordingVendor(_cassette(freqs=(MINUTE_FREQ,))),
        roster=_roster({"SPY": [MINUTE_FREQ]}),
        spans=_open_span(1, 2),
        guards=_budget(2),
    )
    rendered = result.render().splitlines()
    # QQQ has a span and no roster entry, so the plan cannot name its frequencies.
    assert result.unwalked, "the fixture must produce both blocks for the order to mean anything"
    assert result.deferred
    assert rendered.index(f"  deferred: {len(result.deferred)}") < rendered.index(
        f"  unwalked: {len(result.unwalked)}"
    )


def test_the_plan_orders_by_the_whole_date_across_a_month_boundary():
    """The outer sort key is the session, not a field of it.

    Every other ordering test here lives inside September, so a key that agrees with the true date
    only within one month sorts them all correctly. ``day.session.day`` is such a key: over
    2026-08-31 to 2026-09-04 it ranks the 31st above the 4th and puts the oldest session back at
    the front, which is the exact failure the inversion exists to prevent.

    The fixture calendar's ``WEEK_ZERO`` is what makes this cost a span start and a clock.
    """
    first, last = WEEK_ZERO, date(2026, 9, 4)
    plan = _plan(
        spans=_open_span(1, start=datetime(2026, 8, 31, 12, 0, tzinfo=UTC)),
        master=_master(valid_from=first),
        now=datetime(2026, 9, 4, 22, 0, tzinfo=UTC),
    )
    sessions = [day.session for day in plan.days]
    assert sessions == sorted(sessions, reverse=True)
    assert sessions[0] == last and sessions[-1] == first
    # The range's own ends are unmoved by the inversion, and they straddle the boundary.
    assert plan.floor == first
    assert plan.ceiling == last
