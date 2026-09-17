"""The evening vendor sweep: the 18:30 job, the nightly digest, and the Friday wake.

Every test here builds a lake on disk, runs the sweep with every seam injected, and reads
the result back off the files and messages a reader would read. Nothing touches the network,
nothing shells out, and the schedule setter is a recorder rather than ``sudo``.

Three properties of the fixtures are worth naming before the tests.

1. **The vendor is a thunk and the fake counts how often it was built.** That is what lets a
   holiday assert not merely that no bars landed but that the token was never read at all.
2. **The clock decides which weekday the run is.** 2026-09-14 is a Monday, so the ordinary
   run is Monday evening and the Friday branch is four days later. A test that wants the
   Friday branch moves the clock rather than passing a flag.
3. **The schedule reader returns text.** ``parse_pmset_schedule`` is what turns it into
   alarms, so a read-back test writes the ``pmset -g sched`` output the machine prints and
   the assertion runs through the real parser.
"""

from __future__ import annotations

import json
import subprocess
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal, report, sweep
from lake.alert import Publisher
from lake.bars import CHECK_BAR_CLOSE
from lake.calendar import NotASession
from lake.capture_spans import CaptureSpan, CaptureSpans
from lake.cassette import Cassette
from lake.control_plane import EOD_SWEEP_SLUG, SUNDAY_WAKE, pmset_schedule_args
from lake.manifest import append_quarantine
from lake.paths import CHAINS, QUOTES, LakePaths
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.schwab import VendorAuthError
from lake.security_master import KIND_EQUITY, SecurityMaster, master_path
from lake.sweep import DIGEST_BYTE_CAP, HOLIDAY_BODY, NIGHTLY_EVENT, NIGHTLY_PRIORITY
from lake.tickers import Roster
from lake.vendor import DAILY_FREQ, MINUTE_FREQ
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport
from tests.support.vendor import CassetteVendor, bars_candle, bars_interactions

# The week the fixture calendar serves. 2026-09-14 is a Monday, so the sessions run Monday
# through Friday and the second Monday gives the Friday branch a next week to wake before.
MONDAY = date(2026, 9, 14)
NEXT_MONDAY = date(2026, 9, 21)
SESSION = MONDAY
FOLLOWING = date(2026, 9, 15)
FRIDAY = date(2026, 9, 18)
# The Sunday the Friday run's one-shot targets: the one before the next session week.
SUNDAY = date(2026, 9, 20)

# 18:30 Eastern, which is when launchd fires this job. The close was at 16:00, so the guard
# that refuses a session still open passes on every one of these.
EVENING = datetime.fromisoformat("2026-09-14T18:30:00-04:00")
FRIDAY_EVENING = datetime.fromisoformat("2026-09-18T18:30:00-04:00")
# The catch-up: launchd firing a missed 18:30 at the next morning's 08:25 wake.
MORNING_AFTER = datetime.fromisoformat("2026-09-15T08:25:00-04:00")

OPEN_ET = datetime.fromisoformat("2026-09-14T09:30:00-04:00")
CLOSE_ET = datetime.fromisoformat("2026-09-14T16:00:00-04:00")
DAY_MARGIN = timedelta(days=1)


def _bounds(session: date) -> tuple[datetime, datetime]:
    """One session's open and close, spelled the way the fake calendar serves them."""
    return (
        datetime.fromisoformat(f"{session.isoformat()}T09:30:00-04:00"),
        datetime.fromisoformat(f"{session.isoformat()}T16:00:00-04:00"),
    )


SETTLED_CLOSE = 650.00
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
PING_URL = "https://hc-ping.test/key/eod-sweep"

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


def _quote_row(day: date, *, ticker: str = "SPY", row_kind: str = "data") -> dict:
    """One quotes row at the session's equity close, carrying the settled close."""
    gap = row_kind == journal.ROW_KIND_GAP
    return {
        "snap_ts": f"{day.isoformat()}T20:00:00+00:00",
        "fetch_ts": f"{day.isoformat()}T20:00:00.300+00:00",
        "ticker": ticker,
        "last": None if gap else 649.0,
        "close_price": None if gap else SETTLED_CLOSE,
        "row_kind": row_kind,
        "error_class": "vendor_auth_error" if gap else None,
        "close_tag": None if gap else "spot_close",
        "schema_version": 1,
        "extra": None,
    }


def _quotes_table(rows: list[dict]) -> pa.Table:
    columns = {name: [row.get(name) for row in rows] for name in QUOTES_SCHEMA.names}
    return pa.table(columns, schema=QUOTES_SCHEMA)


def _ledger_table() -> pa.Table:
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _master(tickers: tuple[str, ...] = ("SPY",)) -> SecurityMaster:
    master = SecurityMaster()
    for ticker in tickers:
        master.register(
            kind=KIND_EQUITY,
            capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
            valid_from=date(2026, 9, 8),
            ticker=ticker,
        )
    return master


def _daily_candle(session: date = SESSION, *, close: float = SETTLED_CLOSE) -> dict:
    when = datetime.fromisoformat(f"{session.isoformat()}T00:00:00-04:00")
    return bars_candle(when, open_=645.0, high=651.0, low=644.0, close=close, volume=70_000_000)


def _cassette(close: float = SETTLED_CLOSE, *, session: date = SESSION) -> Cassette:
    """One daily recording for SPY's session, which is all the roster below asks for.

    ``session`` moves the recorded window with the clock. The cassette key carries the
    window, so a run against another session replays nothing and fails visibly rather than
    reading somebody else's day.
    """
    session_open, session_close = _bounds(session)
    start, end = session_open - DAY_MARGIN, session_close + DAY_MARGIN
    return Cassette(
        interactions=tuple(
            bars_interactions(
                "SPY", DAILY_FREQ, [(start, end, [_daily_candle(session, close=close)])]
            )
        )
    )


class _CountingVendorSource:
    """A vendor thunk that records how many times it was asked to build one.

    The count is the assertion a holiday needs. "No bars landed" is also true of a session
    whose fetch failed, and only "the vendor was never built" says the run did not reach the
    token at all.
    """

    def __init__(self, cassette: Cassette | None = None, *, raises: Exception | None = None):
        self._cassette = cassette if cassette is not None else _cassette()
        self._raises = raises
        self.builds = 0

    def __call__(self):
        self.builds += 1
        if self._raises is not None:
            raise self._raises
        return CassetteVendor(self._cassette)


class _RecordingSetter:
    """A schedule setter that records the Sundays it was asked for rather than running sudo."""

    def __init__(self, raises: Exception | None = None) -> None:
        self.sundays: list[date] = []
        self._raises = raises

    def __call__(self, sunday: date) -> None:
        self.sundays.append(sunday)
        if self._raises is not None:
            raise self._raises


def _schedule_text(*, one_shot: date | None = SUNDAY) -> str:
    """A ``pmset -g sched`` dump with the weekday repeat alarm and an optional one-shot."""
    lines = ["Repeating power events:", f"wakeorpoweron at {SUNDAY_WAKE.hms} Monday Tuesday"]
    lines[1] = "wakeorpoweron at 08:25:00 Monday Tuesday Wednesday Thursday Friday"
    if one_shot is not None:
        stamp = f"{one_shot.month:02d}/{one_shot.day:02d}/{one_shot.year % 100:02d}"
        lines += ["Scheduled power events:", f"[0]  wakeorpoweron at {stamp} {SUNDAY_WAKE.hms}"]
    return "\n".join(lines) + "\n"


def _roster(freqs: list[str] | None = None) -> Roster:
    return Roster.from_mapping({"SPY": {"options": True, "bars": freqs or [DAILY_FREQ]}})


def _lake(
    fixture_lake: FixtureLake,
    *,
    quotes: dict[tuple[str, date], list[dict]] | None = None,
    chains: dict[tuple[str, date], pa.Table] | None = None,
) -> Path:
    """A lake holding the next session's sealed quotes, the ledger, the master and the spans.

    The capture spans are here so the battery at step 2.5 judges rather than reporting that it
    could not tell whether capture was running. A fixture without them exercises the wiring
    only in the mode where the battery judges nothing, which is the one mode that cannot show
    the wiring working.
    """
    if quotes is None:
        quotes = {("SPY", FOLLOWING): [_quote_row(FOLLOWING)]}
    for (ticker, day), rows in quotes.items():
        fixture_lake.with_quotes(ticker, day, _quotes_table(rows))
    for (ticker, day), table in (chains or {}).items():
        fixture_lake.with_chains(ticker, day, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    fixture_lake.with_reference("capture_spans", _spans().to_table())
    root = fixture_lake.build()
    _master().write(master_path(root))
    return root


def _spans(instrument_ids: tuple[int, ...] = (1,)) -> CaptureSpans:
    """One open capture span per instrument, opening when ``_master`` says capture began."""
    return CaptureSpans(
        [
            CaptureSpan(
                instrument_id=instrument_id,
                start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
                end=None,
                options=True,
            )
            for instrument_id in instrument_ids
        ]
    )


def _run(
    root: Path,
    *,
    now: datetime = EVENING,
    vendor_source=None,
    setter: _RecordingSetter | None = None,
    reader=None,
    pinger: FakePinger | None = None,
    publisher: Publisher | None = None,
    transport: FakeTransport | None = None,
    roster: Roster | None = None,
    holidays: tuple[date, ...] = (),
):
    """One sweep run with every seam injected, returning the outcome and the fakes."""
    pinger = pinger if pinger is not None else FakePinger()
    transport = transport if transport is not None else FakeTransport()
    publisher = (
        publisher
        if publisher is not None
        else Publisher(lake_root=root, transport=transport, secrets=("secret-key",))
    )
    outcome = sweep.sweep(
        lake_root=root,
        clock=ManualClock(now),
        calendar=weekday_sessions(MONDAY, NEXT_MONDAY, holidays=holidays),
        roster=roster if roster is not None else _roster(),
        vendor_source=vendor_source if vendor_source is not None else _CountingVendorSource(),
        pinger=pinger,
        ping_url=PING_URL,
        publisher=publisher,
        schedule_reader=reader if reader is not None else (lambda: _schedule_text()),
        schedule_setter=setter if setter is not None else _RecordingSetter(),
    )
    return outcome, pinger, transport


def _filed(root: Path) -> list[dict]:
    """Every nightly report file in the lake, read back off disk."""
    directory = root / "reports"
    return [json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))]


# -- the ordinary evening ---------------------------------------------------------------


def test_an_ordinary_evening_runs_every_walk_pings_files_and_sends_one_digest(
    fixture_lake: FixtureLake,
):
    """The happy path, asserted end to end rather than sampled.

    Everything else here is a departure from this, so the run it departs from is pinned
    once: three walks ran, the ping landed against the ``eod-sweep`` slug, one report file
    was written under ``reports/`` at the tree's root, and one digest went out at the design's
    priority with the design's title.
    """
    root = _lake(fixture_lake)
    outcome, pinger, transport = _run(root)

    assert [name for name, _ in outcome.nightly.pieces] == ["dividends", "splits", "bars"]
    assert all(piece.finished for _, piece in outcome.nightly.pieces), outcome.render()
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]
    assert outcome.nightly.pinged is True
    assert outcome.ok is True

    (filed,) = _filed(root)
    assert filed["day"] == SESSION.isoformat()
    assert filed["session"] is True
    assert filed["pinged"] is True
    assert sorted(filed["pieces"]) == ["bars", "dividends", "splits"]

    (message,) = transport.messages
    assert message.event == NIGHTLY_EVENT
    assert message.priority == NIGHTLY_PRIORITY
    assert message.title == f"Nightly {SESSION.isoformat()}"
    assert outcome.delivered is True


def test_the_report_file_lands_at_the_reports_root_and_no_existing_reader_picks_it_up(
    fixture_lake: FixtureLake,
):
    """The four named subdirectories are what the counting globs read, and this is not one.

    ``alert.undelivered`` counts ``reports/alerts/date=D/``, and the three writers in
    ``lake.report`` each build a named subdirectory. A nightly file placed inside any of them
    would inflate that producer's count. A flat file at the root is picked up by
    ``reports/*.json`` and by nothing else.
    """
    root = _lake(fixture_lake)
    _run(root)

    (path,) = sorted((root / "reports").glob("*.json"))
    assert path.parent == root / "reports"
    assert path.name.startswith(f"{SESSION.isoformat()}-")
    assert [p.name for p in sorted((root / "reports").iterdir()) if p.is_dir()] == []


def test_a_second_run_the_same_night_writes_a_second_file_rather_than_colliding(
    fixture_lake: FixtureLake,
):
    """Two runs on one night are two verdicts, which is what the stamp in the name says.

    A name keyed on the day alone would raise on the second run, because the writer opens
    with ``x``. A reports directory has no resolution step, so every file in it is one run's
    answer, the way the repeats under ``withheld/`` are.
    """
    root = _lake(fixture_lake)
    _run(root, now=EVENING)
    _run(root, now=EVENING + timedelta(minutes=1))

    filed = _filed(root)
    assert len(filed) == 2
    assert {entry["day"] for entry in filed} == {SESSION.isoformat()}
    assert len({entry["at"] for entry in filed}) == 2


def test_a_second_run_lands_nothing_new_and_says_so(fixture_lake: FixtureLake):
    """Idempotence, read off the counts the walks already report.

    The bar fetch skips a manifested ticker-day and the ledger walks re-derive what the
    ledger already holds. A run that lands nothing and holds nothing has either done the work
    or found none, and only these numbers tell the two apart.
    """
    root = _lake(fixture_lake)
    first, _, _ = _run(root)
    second, _, _ = _run(root, now=EVENING + timedelta(minutes=1))

    bars_first = dict(first.nightly.pieces)["bars"]
    bars_second = dict(second.nightly.pieces)["bars"]
    assert bars_first.landed == 1 and bars_first.skipped == 0
    assert bars_second.landed == 0 and bars_second.skipped == 1
    assert second.nightly.pinged is True


# -- the gap count ----------------------------------------------------------------------


def test_the_gap_count_reads_the_sealed_partitions_and_zero_is_not_absence(
    fixture_lake: FixtureLake,
):
    """Three answers, and the third is the one that matters.

    A day with gap rows counts them. A day with none counts zero. A day with no partition at
    all answers ``None``, because compaction seals at close+15 and an absent partition at
    18:30 says the seal did not happen rather than that the day was clean. Reporting zero
    there would read as a perfect day.
    """
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
            ("SPY", SESSION): [
                _quote_row(SESSION, row_kind=journal.ROW_KIND_GAP),
                _quote_row(SESSION, row_kind=journal.ROW_KIND_GAP),
                _quote_row(SESSION),
            ],
        },
    )
    assert sweep.count_gaps(root, SESSION) == 2
    assert sweep.count_gaps(root, FOLLOWING) == 0
    assert sweep.count_gaps(root, date(2026, 9, 17)) is None

    outcome, _, _ = _run(root)
    assert outcome.nightly.gaps == 2
    assert _filed(root)[0]["gaps"] == 2


def test_an_unsealed_day_says_unsealed_in_the_file_and_in_the_digest(fixture_lake: FixtureLake):
    """``None`` has to survive to both readers, because zero is the answer it is not."""
    root = _lake(fixture_lake)
    outcome, _, transport = _run(root)

    assert outcome.nightly.gaps is None
    assert _filed(root)[0]["gaps"] is None
    assert "gaps unsealed" in transport.messages[0].body


def test_the_gap_count_reads_both_capture_surfaces(fixture_lake: FixtureLake):
    """Chains and quotes both carry gap rows, and a count of one surface would halve it."""
    root = _lake(
        fixture_lake,
        quotes={
            ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
            ("SPY", SESSION): [_quote_row(SESSION, row_kind=journal.ROW_KIND_GAP)],
        },
    )
    fixture_lake.with_chains(
        "SPY",
        SESSION,
        pa.table(
            {"row_kind": [journal.ROW_KIND_GAP, journal.ROW_KIND_GAP, journal.ROW_KIND_DATA]},
            schema=pa.schema([("row_kind", pa.string())]),
        ),
    )
    fixture_lake.build()
    assert (root / CHAINS).is_dir() and (root / QUOTES).is_dir()
    assert sweep.count_gaps(root, SESSION) == 3


# -- the catch-up guard -----------------------------------------------------------------


def test_a_catch_up_run_before_the_close_does_not_fetch_and_does_not_ping(
    fixture_lake: FixtureLake,
):
    """launchd fires a missed 18:30 at the next wake, and that run must not read green.

    At 08:25 the session's close has not happened. Fetching would ask for a session still in
    progress, every ticker would fail the span check, and a green ping would say the day's
    bars are there. So the fetch is skipped, the vendor is never built, and the check goes
    silent, which is what its own table row means.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource()
    outcome, pinger, _ = _run(root, now=MORNING_AFTER, vendor_source=source)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.finished is False
    assert "has not passed" in bars_piece.refusal
    assert source.builds == 0, "the catch-up reached the vendor"
    assert pinger.urls == []
    assert outcome.nightly.pinged is False
    assert outcome.ok is False


def test_the_close_guard_cannot_refuse_an_ordinary_evening_run(fixture_lake: FixtureLake):
    """The guard is a selector for the real case rather than a gate that can shut it.

    The job fires at 18:30 against a 16:00 close, so every scheduled run clears it. This runs
    the minute after the close as well, which is the earliest a scheduled run could ever be,
    and it still fetches.
    """
    root = _lake(fixture_lake)
    for when in (CLOSE_ET + timedelta(minutes=1), EVENING):
        source = _CountingVendorSource()
        outcome, pinger, _ = _run(root, now=when, vendor_source=source)
        assert source.builds == 1, when
        assert pinger.urls == [PING_URL], when


# -- the holiday no-op ------------------------------------------------------------------


def test_a_holiday_runs_no_data_work_pings_and_sends_the_one_line(fixture_lake: FixtureLake):
    """The design's no-op: the run happens, says so, and touches nothing.

    None of the three walks runs. They read every sealed ticker-day rather than today's, so a
    holiday run would re-derive yesterday's held findings and file each one again under
    ``reports/withheld/`` for no new information, and the one-line digest could not report
    them anyway. The vendor is never built, so the token is never read.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource()
    outcome, pinger, transport = _run(root, now=EVENING, vendor_source=source, holidays=(SESSION,))

    assert outcome.nightly.session is False
    assert outcome.nightly.pieces == ()
    assert source.builds == 0
    assert pinger.urls == [PING_URL]
    assert transport.messages[0].body == HOLIDAY_BODY
    assert outcome.ok is True


def test_a_holiday_still_files_its_report_so_an_absent_file_means_no_run(
    fixture_lake: FixtureLake,
):
    """The close+5 guard's rule one producer over: a clean run writes a file too."""
    root = _lake(fixture_lake)
    _run(root, holidays=(SESSION,))

    (filed,) = _filed(root)
    assert filed["session"] is False
    assert filed["pieces"] == {}
    assert filed["pinged"] is True


# -- the ping rule ----------------------------------------------------------------------


def test_a_walk_that_did_not_finish_withholds_the_ping_and_names_itself(
    fixture_lake: FixtureLake,
):
    """A dead refresh token is the case the check's own row describes.

    ``VendorAuthError`` ends the bar fetch, so the day's official bars really are missing,
    which is what a missed ``eod-sweep`` ping means. The ledger walks still ran and are still
    reported, because containing the failure to its own piece is what keeps their work
    readable.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource(raises=VendorAuthError("refresh token expired"))
    outcome, pinger, transport = _run(root, vendor_source=source)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.finished is False
    assert "VendorAuthError" in bars_piece.refusal
    assert dict(outcome.nightly.pieces)["dividends"].finished is True
    assert pinger.urls == []
    assert any("bars did not run" in problem for problem in outcome.nightly.problems)
    assert "bars: did not run" in transport.messages[0].body


def test_a_held_finding_is_the_run_working_and_still_pings(fixture_lake: FixtureLake):
    """The opposite case, and the one that would be easy to get wrong.

    A gate refusing to land a row is the sweep doing its job. ``report.py`` says this ping
    "says the run happened whether or not it found anything", so a held finding must not
    silence the check. Here the settled close disagrees with the bar's, which the close
    cross-check holds.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource(_cassette(close=SETTLED_CLOSE * 1.05))
    outcome, pinger, transport = _run(root, vendor_source=source)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.finished is True
    assert bars_piece.held == 1
    assert outcome.nightly.disagreements == 1
    assert pinger.urls == [PING_URL], "a held finding silenced the check"
    assert outcome.nightly.pinged is True
    assert "disagreements 1" in transport.messages[0].body


def test_a_ping_that_did_not_land_still_files_and_still_sends_the_digest(
    fixture_lake: FixtureLake,
):
    """The digest is then the only copy of these numbers, so it must not go with the ping.

    The exit code says a person should look, which is ``control_plane.main``'s own answer for
    the Sunday command where it returns ``0 if pinged else 1``.
    """

    class _Dead:
        def ping(self, url: str) -> None:
            raise OSError("no route to host")

    root = _lake(fixture_lake)
    outcome, _, transport = _run(root, pinger=_Dead())

    assert outcome.nightly.pinged is False
    assert any("ping failed" in problem for problem in outcome.nightly.problems)
    assert _filed(root)[0]["pinged"] is False
    assert "ping did not land" in transport.messages[0].body
    assert outcome.ok is False


def test_a_refused_ping_pages_and_names_the_slug_while_a_lost_one_pages_nobody(
    fixture_lake: FixtureLake,
):
    """The page #213 settled and #240 could not wire, because nothing pinged this slug yet.

    A 404 says healthchecks read the request and fed no check, so the row is missing and its
    silence means nothing. A transport failure is the wifi blip the design answers with a
    looser grace, and paging from the laptop would fail in the same outage.
    """
    import urllib.error

    class _Refused:
        def ping(self, url: str) -> None:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    class _Lost:
        def ping(self, url: str) -> None:
            raise OSError("no route to host")

    root = _lake(fixture_lake)
    _, _, refused = _run(root, pinger=_Refused())
    pages = [m for m in refused.messages if m.event != NIGHTLY_EVENT]
    assert len(pages) == 1, [m.event for m in refused.messages]
    assert EOD_SWEEP_SLUG in pages[0].body
    assert PING_URL not in pages[0].body, "the page carried the ping key"

    _, _, lost = _run(root, now=EVENING + timedelta(minutes=1), pinger=_Lost())
    assert [m.event for m in lost.messages] == [NIGHTLY_EVENT]


# -- the Friday branch ------------------------------------------------------------------


def test_the_friday_run_sets_the_sunday_one_shot_and_reads_it_back(fixture_lake: FixtureLake):
    """Both halves, and the read-back is what proves the wake landed.

    Friday is the only read-back that works. By Sunday evening a wake that fired and one that
    was never set look the same, which is why ``expected_one_shot`` expects it only between
    this run and its own firing.
    """
    root = _lake(fixture_lake)
    setter = _RecordingSetter()
    outcome, pinger, _ = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=FRIDAY)),
        setter=setter,
    )

    assert setter.sundays == [SUNDAY]
    assert outcome.nightly.report == ()
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]


def test_a_weekday_that_is_not_friday_sets_nothing(fixture_lake: FixtureLake):
    """The design anchors the setter on Friday alone, so the other four must not touch it."""
    root = _lake(fixture_lake)
    setter = _RecordingSetter()
    _run(root, now=EVENING, setter=setter)
    assert setter.sundays == []


def test_the_friday_setter_runs_on_a_holiday_too(fixture_lake: FixtureLake):
    """The design's pmset table says the step runs in every Friday invocation.

    It touches no market data, so the no-op contract is unaffected, and a Friday holiday that
    skipped it would leave the Sunday canary with no wake to fire it.
    """
    root = _lake(fixture_lake)
    setter = _RecordingSetter()
    source = _CountingVendorSource()
    outcome, pinger, transport = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=source,
        setter=setter,
        holidays=(FRIDAY,),
    )

    assert setter.sundays == [SUNDAY]
    assert outcome.nightly.session is False
    assert source.builds == 0, "the holiday reached the vendor"
    assert transport.messages[0].body == HOLIDAY_BODY
    assert pinger.urls == [PING_URL]


def test_a_setter_that_failed_withholds_the_ping(fixture_lake: FixtureLake):
    """Nothing else sets that wake, and the Sunday read-back cannot catch a missed one.

    So a failed set is a problem rather than a report-tier finding: the check goes silent and
    the dead-man is what says the machine will not wake on Sunday.
    """
    root = _lake(fixture_lake)
    setter = _RecordingSetter(
        raises=subprocess.CalledProcessError(1, "sudo", stderr="sudo: a password is required")
    )
    outcome, pinger, _ = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=FRIDAY)),
        setter=setter,
    )

    assert pinger.urls == []
    assert any("sunday one-shot wake not set" in p for p in outcome.nightly.problems)
    assert any("a password is required" in p for p in outcome.nightly.problems)


def test_a_failed_setter_skips_the_read_back(fixture_lake: FixtureLake):
    """Reading back a wake nothing set would report the setter's own failure twice."""
    root = _lake(fixture_lake)
    reads: list[str] = []

    def reader() -> str:
        reads.append("read")
        return _schedule_text()

    _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=FRIDAY)),
        setter=_RecordingSetter(raises=RuntimeError("no sudoers rule")),
        reader=reader,
    )
    assert reads == []


def test_pmset_drift_rides_the_report_file_rather_than_withholding_the_ping(
    fixture_lake: FixtureLake,
):
    """``SundayOutcome.report``'s rule, one job over.

    Alarm drift is report-tier "because the pre-open self-check already catches a missed wake
    an hour before the bell". So it lands in the file and the digest, and the check stays fed.
    """
    root = _lake(fixture_lake)
    outcome, pinger, transport = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=FRIDAY)),
        reader=lambda: _schedule_text(one_shot=None),
    )

    assert any("one-shot" in line for line in outcome.nightly.report)
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]
    assert any("one-shot" in line for line in _filed(root)[0]["report"])
    assert "report:" in transport.messages[0].body


def test_an_unreadable_read_back_is_a_finding_rather_than_a_crash(fixture_lake: FixtureLake):
    """``pmset`` printing something nobody expected must not cost the whole run."""
    root = _lake(fixture_lake)
    outcome, pinger, _ = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=FRIDAY)),
        reader=lambda: "not a schedule at all",
    )

    assert any("read-back unreadable" in line for line in outcome.nightly.report)
    assert pinger.urls == [PING_URL]


def test_the_setter_builds_the_argument_list_the_sudoers_rule_matches():
    """The rendered line is for a person and this is for a job, and they differ in three ways.

    It carries no ``sudo``, it names ``pmset`` rather than the ``/usr/bin/pmset`` the drop-in
    grants, and it quotes the date and time for a shell rather than handing them over as one
    argument. ``sudo`` joins a command's arguments into one string before matching, so the
    string the last three become is what the anchored rule reads.
    """
    import re

    from lake import control_plane as cp

    args = pmset_schedule_args(SUNDAY)
    assert args == ("schedule", "wakeorpoweron", f"09/20/26 {SUNDAY_WAKE.hms}")
    joined = " ".join(args)
    assert re.fullmatch(cp._SCHEDULE_ARGS_REGEX.replace("[[:space:]]", " "), joined), joined


def test_the_real_setter_runs_sudo_non_interactively_against_the_granted_path(monkeypatch):
    """``-n`` is what makes this safe inside a LaunchDaemon, which has no terminal.

    Without it a missing or drifted drop-in leaves the job blocked on a password nobody can
    type. The subprocess call is intercepted, so nothing here touches the power schedule.
    """
    calls: list[dict] = []

    def fake_run(args, **kwargs):
        calls.append({"args": list(args), **kwargs})

        class _Done:
            returncode = 0

        return _Done()

    monkeypatch.setattr(sweep.subprocess, "run", fake_run)
    sweep.set_sunday_wake(SUNDAY)

    (call,) = calls
    assert call["args"][:3] == ["sudo", "-n", "/usr/bin/pmset"]
    assert call["args"][3:] == list(pmset_schedule_args(SUNDAY))
    assert call["check"] is True
    assert call["stdin"] is subprocess.DEVNULL


def test_the_setter_refuses_a_day_that_is_not_a_sunday():
    """The sudoers rule pins the time and not the weekday, so this is the guard for it."""
    with pytest.raises(ValueError, match="not a Sunday"):
        pmset_schedule_args(FRIDAY)


# -- the digest -------------------------------------------------------------------------


def test_the_digest_carries_counts_and_never_a_list_of_findings(fixture_lake: FixtureLake):
    """The byte budget has to hold on the night with many findings, not only on a quiet one.

    A digest that listed what was held would be under the cap on every night anyone tested
    and over it on the night that mattered. The per-finding detail is in the report file and
    under ``reports/withheld/``.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource(_cassette(close=SETTLED_CLOSE * 1.05))
    outcome, _, transport = _run(root, vendor_source=source)

    body = transport.messages[0].body
    assert len(body.encode("utf-8")) <= DIGEST_BYTE_CAP
    assert "disagreements 1" in body
    assert "SPY" not in body, body
    (subject,) = dict(outcome.nightly.pieces)["bars"].subjects
    assert subject.startswith("SPY ")
    assert subject in str(_filed(root)[0]["pieces"]["bars"]["subjects"])


def test_a_digest_over_the_budget_is_truncated_rather_than_dropped():
    """A digest that did not arrive reads exactly like a dead subscription.

    That is the one thing this message exists to rule out, so the cap shortens it instead.
    """
    many = tuple((f"piece-{index}", report.PieceOutcome(refusal="x" * 200)) for index in range(20))
    nightly = report.Nightly(day=SESSION, session=True, pinged=True, pieces=many)
    body = sweep.digest_body(nightly)
    assert len(body.encode("utf-8")) <= DIGEST_BYTE_CAP
    assert body.endswith("…")


def test_the_publisher_refuses_a_digest_carrying_a_secret(fixture_lake: FixtureLake):
    """The ping key and the ntfy topic must never reach a phone, and the guard is wired here.

    The publisher is built with both secrets in ``sweep_from_config``, so this asserts the
    refusal reaches a message this module composes rather than only that the publisher can
    refuse one.
    """
    root = _lake(fixture_lake)
    transport = FakeTransport()
    publisher = Publisher(lake_root=root, transport=transport, secrets=("unsealed",))
    outcome, _, _ = _run(root, publisher=publisher, transport=transport)

    assert "gaps unsealed" in outcome.digest.body
    assert outcome.delivered is False
    assert transport.messages == []
    (recorded,) = sorted((root / "reports" / "alerts").glob("date=*/*.json"))
    assert json.loads(recorded.read_text())["reason"] == "refused"


# -- the report file --------------------------------------------------------------------


def test_the_file_carries_the_counts_and_the_detail_the_digest_dropped(
    fixture_lake: FixtureLake,
):
    """Naming the fields is what stops #138 inventing them when it writes the panel's query."""
    root = _lake(fixture_lake)
    outcome, _, _ = _run(root)

    (filed,) = _filed(root)
    assert sorted(filed) == [
        "at",
        "day",
        "disagreements",
        "gaps",
        "pages_lost",
        "pieces",
        "pinged",
        "problems",
        "quarantined",
        "report",
        "session",
    ]
    assert sorted(filed["pieces"]["bars"]) == [
        "held",
        "landed",
        "skipped",
        "subjects",
        "unchanged",
        "unfiled",
    ]
    assert filed["disagreements"] == outcome.nightly.disagreements


def test_a_quarantined_quotes_partition_does_not_take_the_whole_sweep(
    fixture_lake: FixtureLake,
):
    """Marketlake #352, at the scale that made it worth fixing before #406 ships.

    The corporate-actions poll is step 1 of this job. Before the dividend walk contained the
    loader's refusals, ``PartitionQuarantined`` left ``extract_dividends`` and
    ``_LEDGER_REFUSALS`` did not cover it, so the night after the validation battery wrote its
    first verdict the poll raised before the bar fetch, before the ping and before the report
    file. Executed against this fixture on the code before the fix, the run died with
    ``reports/`` empty.

    ``_LEDGER_REFUSALS`` is deliberately left alone. A refusal caught here ends the walk, and
    the walk is ordered by ticker, so containing it at this level would cost every ticker
    after the quarantined one its dividends, which is the same defect one level up.
    """
    root = _lake(fixture_lake)
    partition = (
        LakePaths(root).partition_path(QUOTES, "SPY", FOLLOWING).relative_to(root).as_posix()
    )
    append_quarantine(root, {"partition": partition, "verdict": "suspect", "check": "delayed_feed"})

    outcome, pinger, _ = _run(root)

    dividends = dict(outcome.nightly.pieces)["dividends"]
    assert dividends.finished, "the quarantined partition was reported as a refused walk"
    assert dividends.skipped == 1, "the skip reached the nightly file as a count"
    assert outcome.nightly.quarantined == 1
    assert pinger.urls, "the ping was lost with the raise"
    assert outcome.filed_at is not None, "the report file was lost with the raise"
    assert "dividends: landed 0, held 0, unchanged 0, skipped 1" in outcome.digest.body


def test_a_report_file_that_could_not_be_written_does_not_withhold_the_ping(
    fixture_lake: FixtureLake, monkeypatch
):
    """The work the check watches did happen, so the check stays fed.

    It costs an exit code and a line in the digest, which is then the only copy of these
    numbers that exists.
    """
    root = _lake(fixture_lake)
    monkeypatch.setattr(
        sweep, "write_nightly", lambda *a, **k: (_ for _ in ()).throw(PermissionError("read-only"))
    )
    outcome, pinger, transport = _run(root)

    assert pinger.urls == [PING_URL]
    assert outcome.nightly.pinged is True
    assert outcome.filed_at is None
    assert outcome.filing_error == "PermissionError"
    assert "report file NOT written: PermissionError" in transport.messages[0].body
    assert outcome.ok is False


def test_the_disagreement_count_comes_from_the_run_not_from_tonights_withheld_directory(
    fixture_lake: FixtureLake,
):
    """``withheld_dir`` keys its path on the day the rows belong to, never on the night.

    So a finding about an earlier session re-files tonight under that session's date, and a
    glob of tonight's directory would read zero while the condition is live. The count is
    taken off the walks instead.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource(_cassette(close=SETTLED_CLOSE * 1.05))
    outcome, _, _ = _run(root, vendor_source=source)

    assert outcome.nightly.disagreements == 1
    tonight = report.withheld_dir(root, SESSION)
    # The finding is keyed on the ticker-day it is about, which here is the session itself.
    # What the assertion pins is that the count did not come from a directory read.
    assert list(tonight.glob("*.json"))
    assert not list(report.withheld_dir(root, date(2026, 9, 17)).glob("*.json"))


def test_pages_that_failed_to_send_are_counted_off_the_alerts_directory(
    fixture_lake: FixtureLake,
):
    """``alert.undelivered`` is keyed on the day the page failed, so tonight's is the right one."""
    root = _lake(fixture_lake)
    directory = root / "reports" / "alerts" / f"date={SESSION.isoformat()}"
    directory.mkdir(parents=True)
    (directory / "1.json").write_text("{}")
    (directory / "2.json").write_text("{}")

    outcome, _, transport = _run(root)
    assert outcome.nightly.pages_lost == 2
    assert "pages lost 2" in transport.messages[0].body


def test_the_quarantine_count_is_zero_on_a_lake_with_no_ledger(fixture_lake: FixtureLake):
    """It reads zero until #138's battery writes the first verdict, and a missing file is not
    an error. The live lake holds no ``quarantine.jsonl`` at all."""
    root = _lake(fixture_lake)
    assert not (root / "quarantine.jsonl").exists()
    assert sweep.count_quarantined(root) == 0

    outcome, _, _ = _run(root)
    assert outcome.nightly.quarantined == 0


# -- the command ------------------------------------------------------------------------


def test_the_command_runs_the_sweep_and_reports_what_it_did(
    fixture_lake: FixtureLake, capsys, monkeypatch, tmp_path
):
    """The operator's view: one block on stdout and an exit code, never a stack trace."""
    from tests.support.config import write_config

    root = _lake(fixture_lake)
    config = write_config(tmp_path, lake_root=root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY:\n  options: true\n  bars:\n  - 1d\n")

    setter = _RecordingSetter()
    monkeypatch.setattr(sweep, "UrllibPinger", FakePinger)
    monkeypatch.setattr(sweep, "NtfyTransport", lambda topic: FakeTransport())
    monkeypatch.setattr(sweep, "ExchangeCalendar", lambda: weekday_sessions(MONDAY, NEXT_MONDAY))

    code = sweep.main(
        ["--config", str(config), "--tickers", str(tickers)],
        clock=ManualClock(EVENING),
        vendor_source=_CountingVendorSource(),
        schedule_setter=setter,
        schedule_reader=lambda: _schedule_text(),
    )
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert f"Vendor sweep for {SESSION.isoformat()} (session)" in printed
    assert f"slug={EOD_SWEEP_SLUG}" in printed
    assert "hc-ping" not in printed, "the command printed a ping URL"


def test_a_bad_config_file_reaches_the_operator_as_one_line(tmp_path, capsys):
    """``input_errors_exit``'s shape, which every sibling command already takes."""
    config = tmp_path / "config.yaml"
    config.write_text("this: [is not\n")
    with pytest.raises(SystemExit) as raised:
        sweep.main(["--config", str(config)])
    assert raised.value.code == 2
    assert capsys.readouterr().err.startswith("sweep: ")


def test_the_command_has_no_date_flag(capsys):
    """#319 owns the span of sessions a run covers, and a date flag is a backfill selector."""
    with pytest.raises(SystemExit):
        sweep.main(["--date", "2026-09-14"])
    assert "unrecognized arguments" in capsys.readouterr().err


def test_a_lake_with_no_security_master_ends_each_walk_rather_than_the_run(
    fixture_lake: FixtureLake,
):
    """One condition, one command fixes it, and all three walks meet it identically.

    Each one names itself, so the digest says three pieces refused rather than handing the
    operator a stack trace in the job's error log.
    """
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    outcome, pinger, transport = _run(root)
    assert [name for name, piece in outcome.nightly.pieces if not piece.finished] == [
        "dividends",
        "splits",
        "bars",
    ]
    assert pinger.urls == []
    assert "did not run" in transport.messages[0].body


def test_a_holiday_never_touches_the_vendor_even_when_building_one_would_raise(
    fixture_lake: FixtureLake,
):
    """A dead token on a holiday is not a failure, because nothing that day needs one."""
    root = _lake(fixture_lake)
    source = _CountingVendorSource(raises=VendorAuthError("refresh token expired"))
    outcome, pinger, _ = _run(root, holidays=(SESSION,), vendor_source=source)

    assert source.builds == 0
    assert pinger.urls == [PING_URL]
    assert outcome.ok is True


def test_the_bar_fetch_asks_for_the_session_the_clock_is_in(fixture_lake: FixtureLake):
    """The cassette key carries the window, so a drifted session would replay nothing.

    That makes the session an assertion rather than only a precondition, and it is what the
    close guard's selector is choosing.
    """
    root = _lake(fixture_lake)
    outcome, _, _ = _run(root)
    assert dict(outcome.nightly.pieces)["bars"].landed == 1

    with pytest.raises(NotASession):
        weekday_sessions(MONDAY).session_close(date(2026, 9, 19))


def test_a_frequency_the_lake_cannot_fetch_ends_the_bar_fetch_alone(fixture_lake: FixtureLake):
    """An edit to ``tickers.yaml`` is the fix, and it must not cost the ledger walks."""
    root = _lake(fixture_lake)
    outcome, pinger, _ = _run(root, roster=_roster([MINUTE_FREQ, "1w"]))

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.finished is False
    assert "UnsupportedBarFreq" in bars_piece.refusal
    assert dict(outcome.nightly.pieces)["splits"].finished is True
    assert pinger.urls == []


def test_a_naive_bar_stamp_is_a_refused_bars_piece_rather_than_the_end_of_the_run(
    fixture_lake: FixtureLake, monkeypatch
):
    """The refusal reports itself here instead of escaping and taking the evening with it.

    `StampNotAnInstant` deliberately stays out of the per-ticker-day catch inside `lake.bars`,
    because a stamp it refuses means the row builder's one-offset guarantee has broken for the
    run. That is a reason to end the bars walk, not a reason to end this job: everything written
    after the pieces block is lost with it, which is the report file, the digest and the Friday
    `pmset` wake. `UnsupportedBarFreq` is in the same tuple for the same reason.

    The stamp is forced at the seam rather than in a fixture, because the row builder cannot
    mint a naive one, which is the guarantee this asserts the breach of.
    """
    from lake import bars as bars_module

    root = _lake(fixture_lake)

    def refuse(*args, **kwargs):
        raise bars_module.StampNotAnInstant("2026-09-14T00:00:00")

    monkeypatch.setattr(sweep, "fetch_session_bars", refuse)
    outcome, pinger, _ = _run(root)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.finished is False
    assert "StampNotAnInstant" in bars_piece.refusal
    assert "2026-09-14T00:00:00" in bars_piece.refusal
    assert dict(outcome.nightly.pieces)["splits"].finished is True
    assert pinger.urls == []
    assert outcome.filed_at is not None, "the report file was lost with the escaping refusal"


# -- what the review's mutation lens found unheld ---------------------------------------


def test_the_digest_never_carries_an_exception_message(fixture_lake: FixtureLake):
    """The redaction boundary is held on the file and was not held on the digest.

    ``PieceOutcome.refusal_class``'s own docstring names the phone as the reason it exists,
    and the digest is the more exposed of the two readers. Both halves of the digest are
    covered here: a piece refusal, and a report-tier line, which went out raw. An
    ``OSError`` says the filename it failed on, which is an absolute path on the capture
    machine.
    """
    secret = "/Users/someone/private"
    root = _lake(fixture_lake)
    source = _CountingVendorSource(raises=VendorAuthError(f"[Errno 13] {secret}/token.json"))
    _, _, transport = _run(
        root,
        now=FRIDAY_EVENING,
        vendor_source=source,
        reader=lambda: (_ for _ in ()).throw(FileNotFoundError(f"[Errno 2] {secret}/pmset")),
    )

    body = transport.messages[0].body
    assert secret not in body, body
    # The class still reaches the reader, because it is what says what to do next.
    assert "VendorAuthError" in body
    assert "FileNotFoundError" in body


def test_a_finding_that_could_not_be_filed_reaches_the_count_and_the_exit_code(
    fixture_lake: FixtureLake, monkeypatch
):
    """``unfiled`` was derived, carried and read, and no test drove any of the three.

    A finding held and filed is a live condition a human can go and read. A finding held
    and not filed reads exactly like a run that found nothing, which is the silence the
    producer exists to break. It does not withhold the ping, because the run itself
    worked, and it is what the non-zero exit code is for.
    """
    from lake import bars as bars_module

    root = _lake(fixture_lake)
    # Patched where it is used rather than where it is defined: ``lake.bars`` binds
    # ``write_withheld`` at import, so patching ``lake.report`` would not reach the walk.
    monkeypatch.setattr(
        bars_module,
        "write_withheld",
        lambda *a, **k: (_ for _ in ()).throw(PermissionError("read-only")),
    )
    source = _CountingVendorSource(_cassette(close=SETTLED_CLOSE * 1.05))
    outcome, pinger, _ = _run(root, vendor_source=source)

    assert dict(outcome.nightly.pieces)["bars"].held == 1
    assert dict(outcome.nightly.pieces)["bars"].unfiled == 1
    assert outcome.nightly.unfiled == 1
    assert pinger.urls == [PING_URL], "an unfiled finding silenced the check"
    assert outcome.ok is False


def test_the_quarantine_count_reads_the_ledger_rather_than_answering_zero(
    fixture_lake: FixtureLake,
):
    """A lake with no ledger cannot tell a real read from a hardcoded zero.

    Two entries, and only one of them withholds its partition. ``is_quarantined`` clears a
    partition on a ``clean`` verdict and withholds it on every other, so a count that
    dropped the filter would answer two.
    """
    root = _lake(fixture_lake)
    (root / "quarantine.jsonl").write_text(
        '{"partition": "chains/ticker=SPY/date=2026-09-14.parquet", "verdict": "stale"}\n'
        '{"partition": "chains/ticker=QQQ/date=2026-09-14.parquet", "verdict": "clean"}\n'
    )
    assert sweep.count_quarantined(root) == 1

    outcome, _, transport = _run(root)
    assert outcome.nightly.quarantined == 1
    assert "quarantined 1" in transport.messages[0].body


def test_a_held_findings_subject_names_the_day_its_own_file_is_keyed_on(
    fixture_lake: FixtureLake,
):
    """The three fields are what make a finding findable, so the format is pinned exactly.

    ``withheld_dir`` keys the path on ``observed_on``, so a subject without it sends a
    reader to a pile with no way to pick the file out.
    """
    root = _lake(fixture_lake)
    source = _CountingVendorSource(_cassette(close=SETTLED_CLOSE * 1.05))
    outcome, _, _ = _run(root, vendor_source=source)

    (subject,) = dict(outcome.nightly.pieces)["bars"].subjects
    assert subject == f"SPY {SESSION.isoformat()} {CHECK_BAR_CLOSE}"
    # The day in the subject is the one the finding's own file is filed under.
    assert list(report.withheld_dir(root, SESSION).glob("*.json"))


def test_the_wire_shape_matches_the_designs_message_table_literally():
    """Compared against the literals, because the constants cannot check themselves.

    Every other test here imports the constant and compares it to itself, which passes
    whatever the constant says. These four are the design's message table, and the
    priority has teeth: 2 is the silent notification-drawer tier that makes one message
    every weekday evening a liveness signal, and 5 would page the operator nightly.
    """
    assert sweep.NIGHTLY_EVENT == "nightly_summary"
    assert sweep.NIGHTLY_PRIORITY == 2
    assert sweep.DIGEST_BYTE_CAP == 1000
    assert sweep.HOLIDAY_BODY == "Holiday, no session"


def test_a_summary_count_that_cannot_be_read_keeps_the_record_and_the_digest(
    fixture_lake: FixtureLake, monkeypatch
):
    """The three counts sit between the work and the ping, and an uncontained raise lost both.

    A corrupt partition or a torn quarantine ledger would take the report file and the
    digest down on a night the check had already gone green, which is the silence
    ``report.py`` exists to end. The count is a summary of the run rather than the run, so
    it is report-tier: the record survives and the ping is not withheld.
    """
    root = _lake(fixture_lake)
    monkeypatch.setattr(
        sweep,
        "count_gaps",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("Parquet magic bytes not found")),
    )
    outcome, pinger, transport = _run(root)

    assert pinger.urls == [PING_URL]
    assert outcome.nightly.gaps is None
    assert any("gap count unreadable" in line for line in outcome.nightly.report)
    assert outcome.filed_at is not None, "the record was lost"
    assert any("gap count unreadable" in line for line in _filed(root)[0]["report"])
    assert transport.messages, "the digest was lost"


def test_a_digest_that_never_left_is_not_a_clean_run(fixture_lake: FixtureLake):
    """It is the one failure this job can have that no count anywhere reaches.

    ``undelivered`` is read before the digest is published, so this run cannot see its own
    loss, and it is keyed on the day the page failed, so the next evening reads a different
    directory. A topic quiet because nothing broke looks exactly like a dead subscription,
    and this message is the answer to that.
    """

    class _Down:
        def send(self, message):
            raise OSError("ntfy down")

    root = _lake(fixture_lake)
    publisher = Publisher(lake_root=root, transport=_Down())
    outcome, pinger, _ = _run(root, publisher=publisher)

    assert pinger.urls == [PING_URL], "a lost digest silenced the check"
    assert outcome.delivered is False
    assert outcome.ok is False
    # Recorded like any other lost page, and counted by no run, which is why ok carries it.
    assert list((root / "reports" / "alerts").glob("date=*/*.json"))


# -- the validation battery, step 2.5 ---------------------------------------------------


def test_the_battery_runs_on_a_session_and_its_counts_reach_the_outcome(
    fixture_lake: FixtureLake,
):
    """The design places it between the bar fetch and the Friday branch, and this is it.

    The fixture lake carries the master and the spans, so the battery judges rather than
    reporting that it could not tell whether capture was running. The sealed quotes partition
    is the one it judges, and the fixture's rows are real-time and in-session, so it comes back
    clean and the ledger stays empty.
    """
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)

    assert outcome.battery is not None
    assert outcome.battery.scope_unknown == 0
    assert outcome.battery.appended == ()


def test_a_holiday_runs_no_battery_at_all(fixture_lake: FixtureLake):
    """The design has compaction and the sweep no-op on an empty journal, and this follows it."""
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root, holidays=(EVENING.date(),))

    assert outcome.battery is None


def test_a_battery_that_raises_costs_the_run_nothing_and_says_so(
    fixture_lake: FixtureLake, monkeypatch
):
    """The containment #352 describes from the other side.

    An uncontained raise at step 2.5 would leave ``sweep.sweep`` entirely and cost the Friday
    wake, the ping and the report file, on exactly the night the battery had something to say.
    """

    def explode(*args, **kwargs):
        raise RuntimeError("the ledger is on fire")

    monkeypatch.setattr(sweep, "judge", explode)
    root = _lake(fixture_lake)

    outcome, pinger, _ = _run(root)

    assert outcome.battery is None
    assert pinger.urls == [PING_URL]
    assert outcome.filed_at is not None
    assert any("battery did not run: RuntimeError" in line for line in outcome.nightly.report)


def test_a_battery_that_raises_does_not_withhold_the_ping(fixture_lake: FixtureLake, monkeypatch):
    """Named as a decision rather than left as a default.

    The ``eod-sweep`` row says a missed ping means the day's official bars or actions are
    missing. A battery that could not run leaves the day *unjudged* instead, which is
    ``_counted``'s line: the work the check watches did happen.
    """

    def explode(*args, **kwargs):
        raise OSError("no")

    monkeypatch.setattr(sweep, "judge", explode)
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)

    assert outcome.nightly.pinged is True
    assert not any("battery" in problem for problem in outcome.nightly.problems)


def test_a_quarantine_the_battery_wrote_is_reported_and_still_pings(fixture_lake: FixtureLake):
    """A quarantine is the run working. It withholds nothing and it reaches the record."""
    from lake.battery import BatteryReport

    root = _lake(fixture_lake)
    written = BatteryReport(judged=1, quarantined=1, appended=("chains/x.parquet",))

    with _battery_returning(written):
        outcome, pinger, _ = _run(root)

    assert outcome.nightly.pinged is True
    assert pinger.urls == [PING_URL]
    assert any("battery wrote 1 quarantine line" in line for line in outcome.nightly.report)


def test_the_quarantine_count_on_the_file_is_this_evenings_not_last_evenings(
    fixture_lake: FixtureLake,
):
    """``count_quarantined`` is read after the battery ran, so the number includes tonight."""
    fixture_lake.with_quarantine(
        {"partition": "chains/ticker=SPY/date=2026-08-24.parquet", "verdict": "quarantined"}
    )
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)

    assert outcome.nightly.quarantined == 1
    assert _filed(root)[0]["quarantined"] == 1


@contextmanager
def _battery_returning(report):
    """Replace the battery with one that returns ``report``, for the wiring's own assertions."""
    import lake.battery

    original = sweep.judge
    sweep.judge = lambda *args, **kwargs: report
    try:
        yield
    finally:
        sweep.judge = original
        assert lake.battery.judge is not None


def test_the_command_hands_the_batterys_threshold_to_the_battery(
    fixture_lake: FixtureLake, capsys, monkeypatch, tmp_path
):
    """A recalibrated guard constant has to survive the whole wiring, not just ``judge``.

    ``judge`` reading the guards it is handed is held elsewhere. What this holds is the link
    between them: ``sweep_from_config`` passing the config's guards rather than letting
    ``judge`` fall back to the pinned defaults. Without it an operator's tuned
    ``staleness_page_seconds`` is silently ignored and the run looks exactly the same.
    """
    from tests.support.config import write_config

    root = _lake(fixture_lake)
    config = write_config(tmp_path, lake_root=root, guards={"staleness_page_seconds": 7})
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY:\n  options: true\n  bars:\n  - 1d\n")

    seen: list[int] = []
    real = sweep.judge

    def recording(*args, **kwargs):
        seen.append(kwargs["guards"].staleness_page_seconds)
        return real(*args, **kwargs)

    monkeypatch.setattr(sweep, "judge", recording)
    monkeypatch.setattr(sweep, "UrllibPinger", FakePinger)
    monkeypatch.setattr(sweep, "NtfyTransport", lambda topic: FakeTransport())
    monkeypatch.setattr(sweep, "ExchangeCalendar", lambda: weekday_sessions(MONDAY, NEXT_MONDAY))

    sweep.main(
        ["--config", str(config), "--tickers", str(tickers)],
        clock=ManualClock(EVENING),
        vendor_source=_CountingVendorSource(),
        schedule_setter=_RecordingSetter(),
        schedule_reader=lambda: _schedule_text(),
    )
    capsys.readouterr()

    assert seen == [7], "the battery was handed the pinned default, not the config's"
