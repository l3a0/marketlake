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

import errno
import json
import os
import re
import subprocess
from contextlib import contextmanager
from dataclasses import fields
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal, report, sweep
from lake.actions import ActionsError, actions_path
from lake.alert import Publisher
from lake.bars import CHECK_BAR_CLOSE
from lake.battery import BatteryReport
from lake.calendar import NotASession
from lake.capture_spans import SPANS_SCHEMA_VERSION, CaptureSpan, CaptureSpans
from lake.cassette import Cassette
from lake.config import GuardConstants
from lake.control_plane import EOD_SWEEP_SLUG, SUNDAY_WAKE, pmset_schedule_args
from lake.manifest import append_quarantine
from lake.paths import CHAINS, QUOTES, LakePaths
from lake.schema_versions import (
    LEDGER_PARTITION,
    LedgerUnreadable,
    RecordedVersion,
    SchemaVersionLedger,
    running_fingerprints,
)
from lake.schwab import VendorAuthError
from lake.security_master import (
    KIND_EQUITY,
    MASTER_SCHEMA_VERSION,
    SecurityMaster,
    SecurityMasterError,
    master_path,
)
from lake.sweep import DIGEST_BYTE_CAP, HOLIDAY_BODY, NIGHTLY_EVENT, NIGHTLY_PRIORITY
from lake.tickers import Roster
from lake.vendor import DAILY_FREQ, MINUTE_FREQ
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport
from tests.support.vendor import CassetteVendor, bars_candle, bars_interactions

# The census's own spelling for the two ``BatteryReport`` counts it does not render under the
# field's name. Shared by the two census tests so the line has one description rather than two.
CENSUS_RENAMED = {"cleared": "clean", "appended": "wrote"}
# The ``BatteryReport`` fields that are not counts. ``report`` is the run's report-tier lines,
# which the block prints under the census, and ``findings`` its per-partition verdicts, which the
# block does not print at all. Neither is a number. ``paged`` is a tuple naming the partitions one
# delayed-feed page covered, so it is not a number either, and it is left out deliberately rather
# than missed. ``judge`` runs in-process inside the sweep, so ``page_delayed_feed``'s prints land
# in the nightly job's own log rather than a hand run's, and it prints on every delivery path, the
# refused one and the unsent one included. A page that was written down is filed under
# ``reports/alerts/`` and counted as ``pages_lost``, which the report file and the dashboard both
# carry. That in-process asymmetry is what separates it from the three counts marketlake #477
# added, which reached the hand run alone.
CENSUS_NOT_COUNTS = {"report", "findings", "paged"}

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
    """The fixture lake's schema-version ledger, holding two entries.

    The first describes the version the fixture's rows are stamped with, which is 1 until
    marketlake #360 takes the fixture to the pinned constant, so the literal stays where that
    sweep will find it.

    The second is the running version, and it is here because marketlake #130 put a check for
    it on this job. A sweep run against a lake whose running version is unrecorded files a
    report line saying so, which is the job working and which every case here would then carry.
    It is derived rather than spelled, so it cannot lag the pinned constant. When the two
    versions are the same integer the second simply replaces the first.
    """
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    running = RecordedVersion(
        version=journal.SCHEMA_VERSION,
        recorded_at=RECORDED_AT,
        fingerprints=running_fingerprints(),
    )
    return SchemaVersionLedger([entry, running]).to_table()


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
    interactions: list = []
    for day in _walked(session):
        day_open, day_close = _bounds(day)
        interactions.extend(
            bars_interactions(
                "SPY",
                DAILY_FREQ,
                [
                    (
                        day_open - DAY_MARGIN,
                        day_close + DAY_MARGIN,
                        [_daily_candle(day, close=close)],
                    )
                ],
            )
        )
    return Cassette(interactions=tuple(interactions))


def _walked(session: date) -> list[date]:
    """Every session the nightly walk reaches on a run whose clock sits on ``session``.

    The 18:30 job walks the capture spans rather than the one session the clock is in, which is
    marketlake #422, so a recording for one day alone replays nothing for the days before it. The
    span in ``_spans`` opens on 2026-09-08 and the fake calendar's first week opens on ``MONDAY``,
    so the walk reaches ``MONDAY`` through ``session`` inclusive.
    """
    days, day = [], MONDAY
    while day <= session:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


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
    tickers: tuple[str, ...] = ("SPY",),
    instrument_ids: tuple[int, ...] = (1,),
    ledger: pa.Table | None = None,
) -> Path:
    """A lake holding the next session's sealed quotes, the ledger, the master and the spans.

    The capture spans are here so the battery at step 2.5 judges rather than reporting that it
    could not tell whether capture was running. A fixture without them exercises the wiring
    only in the mode where the battery judges nothing, which is the one mode that cannot show
    the wiring working.

    ``ledger`` replaces the schema-version ledger. The default records the running version,
    because a production lake has it recorded and marketlake #130 put a check for that on this
    job. A case about that check passes one that does not.
    """
    if quotes is None:
        quotes = {("SPY", FOLLOWING): [_quote_row(FOLLOWING)]}
    for (ticker, day), rows in quotes.items():
        fixture_lake.with_quotes(ticker, day, _quotes_table(rows))
    for (ticker, day), table in (chains or {}).items():
        fixture_lake.with_chains(ticker, day, table)
    fixture_lake.with_reference("schema_versions", _ledger_table() if ledger is None else ledger)
    fixture_lake.with_reference("capture_spans", _spans(instrument_ids).to_table())
    root = fixture_lake.build()
    _master(tickers).write(master_path(root))
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
    guards: GuardConstants | None = None,
):
    """One sweep run with every seam injected, returning the outcome and the fakes.

    ``guards`` stays ``None`` by default, which is what the production wiring passes when a
    config names no ``guards:`` section. The bar walk resolves it to the design's pinned defaults
    itself, so leaving it here proves that resolution happens rather than hiding it.
    """
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
        guards=guards,
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
    # **One report-tier line, and the bars walk deliberately does not add a second.** This
    # fixture lake seals one quotes partition, so the sessions before Friday are each gated
    # against a session the lake never sealed. The manifest is what the walk asks, and it says
    # nothing was sealed for them, so they are unsettled rather than abandoned and nothing here
    # claims the lake has given up. A compaction that has not run is not a bar that is lost.
    # ``test_a_sealed_reference_that_offers_no_close_is_reported_by_reason`` drives the line
    # this run does not produce.
    #
    # The line that does print is the battery's coverage census, which prints on every session
    # run including one that found nothing wrong, because it is the only thing in the report
    # file that says the check ran. By this Friday evening the span has owed ten partitions
    # across five sessions. It is asserted whole rather than described, because a comment
    # describing it cannot be wrong in a way anything catches.
    (census,) = outcome.nightly.report
    assert census == (
        "battery: calendar coverage, 9 of 10 owed sessions have no partition, "
        "over 5 sessions, 2026-09-14 to 2026-09-18"
    )
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]


def test_a_sealed_reference_that_offers_no_close_is_reported_by_reason(
    fixture_lake: FixtureLake,
):
    """The nightly line marketlake #434 exists to produce, and the two ways it can lose its point.

    A daily ticker-day whose close of record is sealed and offers nothing is the thing an
    operator has to be able to find. The digest carries counts and never a list, so this is the
    one channel it has.

    **It is counted per reason rather than by naming an entry, and both halves are asserted.**
    ``report.redacted`` keeps two colon-separated fields and is applied to every report line on
    its way into the nightly file and again into the digest. An entry is itself a ticker-day and
    then a reason, so a line naming one arrives as a dangling "first" with the ticker, the
    session and the reason all gone. The assertion below reads the line after redaction for that
    reason, not before.

    The second half is which reasons are present. Naming a first entry is wrong whichever way the
    walk runs, and marketlake #478 inverted it: the walk now takes ``plan.days`` newest session
    first, so that slot is whichever ticker-day the newest session produced and changes every
    evening. Before #478 it was the mirror image, fixed for ever on the live lake's 2026-09-08
    outage. Either way a quarantine appearing tonight would move a count from six to seven and be
    named nowhere. A census of the classes cannot hide one.

    **The fixture puts the quarantine on one session and not the other on purpose.** A ``Counter``
    keeps insertion order, which here is the walk's session order, so without the sort the census
    could render the same facts in two orders on two nights. An earlier fixture had the two
    sessions arranged so insertion order and sorted order agree, and dropping the sort survived
    mutation because of it. The walk's order inverted under marketlake #478, which is exactly the
    kind of change that would have re-hidden the mutation had the sort not been there.
    """
    # **One reason carries a count above one, on purpose.** The live lake's line is meant to read
    # "6 NoSpotClose", and the count is the only thing separating an outage from one new
    # quarantine. A fixture with one ticker-day per reason never exercises the counting at all:
    # pinning the count to a literal 1 survived mutation against exactly such a fixture.
    gap = [_quote_row(FOLLOWING, row_kind=journal.ROW_KIND_GAP)]
    quotes = {
        ("SPY", FOLLOWING): [_quote_row(FOLLOWING)],
        ("SPY", date(2026, 9, 16)): gap,
        ("SPY", date(2026, 9, 17)): gap,
    }
    fixture_lake.with_quarantine(
        {"partition": f"quotes/ticker=SPY/date={FOLLOWING.isoformat()}.parquet", "verdict": "held"}
    )
    root = _lake(fixture_lake, quotes=quotes)
    outcome, _, _ = _run(root, now=EVENING + timedelta(days=3))

    (line,) = [entry for entry in outcome.nightly.report if entry.startswith("bars abandoned")]
    assert line == "bars abandoned: 3 ticker-day(s), 2 NoSpotClose, 1 PartitionQuarantined"
    assert report.redacted(line) == line, "the reasons were cut off before any reader saw them"
    assert line in outcome.digest.body
    # The run also holds unsettled ticker-days, and none of them is in the line. That silence is
    # the decision the comment beside the line argues for, and a fixture with nothing unsettled
    # would have left it uncovered.
    bars_walk = dict(outcome.nightly.pieces)["bars"]
    assert bars_walk.landed >= 0
    assert "unsettled" not in line and "PartitionAbsent" not in line
    assert outcome.nightly.problems == (), "an abandoned ticker-day withheld the ping"


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


@pytest.mark.parametrize(
    ("reference", "refused"),
    [
        ("security_master.parquet", ("dividends", "splits", "bars")),
        ("capture_spans.parquet", ("bars",)),
        ("schema_versions.parquet", ("dividends", "bars")),
    ],
)
def test_an_unreadable_reference_file_refuses_its_walks_and_keeps_the_evening(
    fixture_lake: FixtureLake, reference: str, refused: tuple[str, ...]
):
    """Marketlake #435. The one failure that cannot be deferred on evidence, because it eats it.

    Every walk opens a reference file before it walks anything, and both readers catch
    ``FileNotFoundError`` alone on purpose: reporting a permission failure as "no capture spans"
    would send an operator to the seeder, which reads the same file and fails the same way. That
    is right for a command, where a person is watching a terminal, and wrong for a job that runs
    unattended at 18:30.

    Measured before the fix: ``chmod 000`` on the master made ``sweep()`` raise
    ``PermissionError``, and the run wrote no report file and sent no ping.

    **What a person sees the next morning is the whole point here**, because the failure mode is
    silence. The piece refuses and says which class refused it, the report file and the digest
    both carry that, and the refusal withholds the ping so the ``eod-sweep`` check pages rather
    than the run simply going quiet. Everything after the pieces block still happens.

    The digest carries the class without the message, which is not this change's doing:
    ``PieceOutcome.refusal_class`` already reasoned about an ``OSError`` reaching it and drops the
    message because "an ``OSError`` says the filename it failed on, which is an absolute path on
    the capture machine". The refusal simply never used to arrive.
    """
    root = _lake(fixture_lake)
    target = root / "reference" / reference
    assert target.exists(), "the fixture stopped carrying the file this locks"
    os.chmod(target, 0o000)
    try:
        outcome, pinger, transport = _run(root)
    finally:
        os.chmod(target, 0o644)

    pieces = dict(outcome.nightly.pieces)
    for name in refused:
        assert pieces[name].refusal is not None, f"{name} escaped rather than refusing"
        assert pieces[name].refusal.startswith(("PermissionError", "OSError"))
    # Only the walks that read this file refuse. A tuple widened past the class would turn the
    # others into refusals too, and asserting the refused set alone cannot see that.
    for name, piece in pieces.items():
        if name not in refused:
            assert piece.finished, f"{name} refused although it never reads {reference}"

    # The evening survives: the record is written, the digest goes out, and the ping is withheld
    # so the check pages rather than the run going quiet.
    assert outcome.filed_at is not None, "no report file was written"
    assert transport.messages, "no digest went out"
    assert pinger.urls == [], "a refused piece must withhold the ping"

    # Every refusal reaches ``problems``, not just the first. The master case refuses three, and a
    # loop that stopped at one would still withhold the ping and still render every piece in the
    # digest, so nothing else here would notice two of them missing from the record.
    said = [line for line in outcome.nightly.problems if "did not run" in line]
    assert len(said) == len(refused), f"{len(refused)} pieces refused and {len(said)} were recorded"

    # The digest names the class and never the capture machine's path.
    body = transport.messages[0].body
    assert "did not run" in body
    assert str(root) not in body, "the digest leaked an absolute path"
    # And the report file, which is the half a unit test over a synthetic outcome cannot reach.
    filed = outcome.filed_at.read_text()
    assert str(root) not in filed, "the report file leaked an absolute path"
    assert json.loads(filed)["pieces"][refused[0]]["refusal"].startswith(
        ("PermissionError", "OSError")
    )


@pytest.mark.parametrize(
    ("seam", "piece"), [("extract_dividends", "dividends"), ("backfill_bars", "bars")]
)
def test_a_walk_refuses_the_whole_os_error_class_and_never_an_ordinary_bug(
    fixture_lake: FixtureLake, monkeypatch, seam: str, piece: str
):
    """The width of what marketlake #435 added, asserted from both sides.

    **Wide enough.** Every reference-file case reaches the tuple as ``PermissionError``, and a
    corrupt file does not reach it at all, because ``SecurityMaster.read`` and
    ``CaptureSpans.read`` fold ``ArrowInvalid`` into their own named classes first. So narrowing
    the entry to ``PermissionError`` passes every one of those cases, and the entry says
    ``OSError`` on purpose: ``MasterUnreadable``'s own docstring names an on-disk read error, "such
    as a bad sector, which ``pyarrow`` reports as ``ArrowIOError``", and that is an ``OSError`` and
    not a ``PermissionError``. Raising ``EIO`` at the seam is how that breadth is witnessed without
    inventing a bad sector.

    **Narrow enough.** The entry is a named class, not a blanket. Widening either tuple to
    ``Exception`` passes the whole suite otherwise, which would leave every bug inside a walk
    reported as a tidy refusal and the run reading as though it had merely been unlucky. An
    ordinary ``ValueError`` has to come out of :func:`lake.sweep.sweep` and end the job, because
    that is this code's own bug rather than the machine's.
    """
    root = _lake(fixture_lake)

    def io_error(*args, **kwargs):
        raise OSError(errno.EIO, "input/output error")

    monkeypatch.setattr(sweep, seam, io_error)
    outcome, pinger, _ = _run(root)

    refusal = dict(outcome.nightly.pieces)[piece].refusal
    assert refusal is not None, f"a plain OSError escaped the {piece} walk"
    assert refusal.startswith("OSError"), refusal
    assert not refusal.startswith("PermissionError"), "the fixture stopped testing the wider class"
    assert outcome.filed_at is not None
    assert pinger.urls == []

    def a_bug(*args, **kwargs):
        raise ValueError("this job handed the seam a bad argument")

    monkeypatch.setattr(sweep, seam, a_bug)
    with pytest.raises(ValueError):
        _run(root)


def _bump_master_schema_version(root: Path) -> None:
    """Stamp the master with a version this code does not read, leaving it valid parquet.

    A newer version of this code is the one shape of this a person actually meets, which is the
    sentence ``_BARS_REFUSALS`` already carries about the spans file. Writing bytes that are not
    parquet would test ``MasterUnreadable``, whose class both tuples already name.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(master_path(root))
    index = table.schema.get_field_index("schema_version")
    pq.write_table(
        table.set_column(index, "schema_version", [[MASTER_SCHEMA_VERSION + 1] * table.num_rows]),
        master_path(root),
    )


@pytest.mark.parametrize(
    ("seam", "piece"), [("extract_dividends", "dividends"), ("backfill_bars", "bars")]
)
@pytest.mark.parametrize(
    ("family", "raised"),
    [("ActionsError", ActionsError), ("SecurityMasterError", SecurityMasterError)],
)
def test_a_walk_refuses_each_master_family_by_its_class(
    fixture_lake: FixtureLake, monkeypatch, seam: str, piece: str, family: str, raised: type
):
    """Marketlake #497 chose the class over its members, and this is what fails if that choice
    is undone.

    The two tests below drive the two siblings that actually escaped, so they cover the fix and
    not the reasoning behind it. Measured: replacing both classes with the four members those
    tests reach leaves the whole suite green, so nothing would have noticed the tuples going
    back to a list of names.

    Raising each base class at the seam is how that breadth is witnessed without inventing a
    reachable case for a member that has none. It is the device the ``OSError`` width test above
    already uses one family over, where an ``errno.EIO`` stands in for a bad sector.

    A member reaching here is not the point and could not be driven honestly. ``UnknownInstrument``
    is raised only by ``remap`` and ``capture_start_of``, which no walk calls, and
    ``UnresolvedSymbol`` and ``AmbiguousSymbol`` are contained per ticker-day in both walks. What
    this checks is that the entry stays a family rather than shrinking back to the members
    somebody happened to meet.
    """
    root = _lake(fixture_lake)

    def refuse(*args, **kwargs):
        raise raised(f"a {family} this walk did not name")

    monkeypatch.setattr(sweep, seam, refuse)
    outcome, pinger, _ = _run(root)

    refusal = dict(outcome.nightly.pieces)[piece].refusal
    assert refusal is not None, f"a bare {family} escaped the {piece} walk"
    assert refusal.startswith(family), refusal
    assert outcome.filed_at is not None, "the report file was lost with the raise"
    assert pinger.urls == [], "a refused piece must withhold the ping"


def test_a_malformed_ledger_line_refuses_the_two_ledger_walks_and_keeps_the_evening(
    fixture_lake: FixtureLake,
):
    """Marketlake #497, and the reachable half of it.

    Both walks read the actions ledger once for the run, through ``actions.latest``, so
    ``entry_key`` raises ``LedgerLineError`` on the first entry carrying no key. It is an
    ``ActionsError`` and the tuple used to name ``MasterAbsent`` alone out of that family, so it
    escaped. Measured against `9e767ab`, one line naming no ``type`` raised out of ``sweep()``
    and the run wrote no report file and sent no ping, on a lake whose only fault was one line
    in a ledger the lake did not carry the night before.

    **The test asserts that the bar walk lands its bar, not merely that it was not refused**,
    because landing it is what the containment is for. A tuple that refused all three would pass
    an assertion about the two ledger pieces alone and still cost the night's bars.
    """
    root = _lake(fixture_lake)
    ledger = actions_path(root)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(json.dumps({"instrument_id": 1, "ex_date": SESSION.isoformat()}) + "\n")

    outcome, pinger, transport = _run(root, vendor_source=_CountingVendorSource(_cassette()))

    pieces = dict(outcome.nightly.pieces)
    for name in ("dividends", "splits"):
        assert pieces[name].refusal is not None, f"a malformed ledger line escaped the {name} walk"
        assert pieces[name].refusal.startswith("LedgerLineError")
    assert pieces["bars"].finished, "the bar walk refused although it reads no actions ledger"
    assert pieces["bars"].landed == 1, "the night's bar was lost with the ledger walks"

    assert outcome.filed_at is not None, "the report file was lost with the raise"
    assert transport.messages, "the digest was lost with the raise"
    assert pinger.urls == [], "a refused piece must withhold the ping"

    # ``LedgerLineError`` says the file it failed on, which is an absolute path on the capture
    # machine. ``refusal_class`` and ``redacted`` both cut by rule rather than by listing
    # classes, so this arrives covered, and the assertion is what says it stays covered.
    body = transport.messages[0].body
    assert "dividends: did not run, LedgerLineError" in body
    assert str(root) not in body, "the digest leaked an absolute path"
    filed = outcome.filed_at.read_text()
    assert str(root) not in filed, "the report file leaked an absolute path"
    assert json.loads(filed)["pieces"]["splits"]["refusal"] == "LedgerLineError"


def test_a_master_from_a_newer_version_refuses_every_walk_rather_than_ending_the_run(
    fixture_lake: FixtureLake,
):
    """Marketlake #497 at the other tuple, which is what makes this two changes rather than one.

    ``SecurityMaster.read`` folds a torn file into ``MasterUnreadable`` and leaves
    ``UnsupportedSchemaVersion`` alone, and neither ``actions.read_master`` nor
    ``bars._read_master`` folds it either, since both keep their ``FileNotFoundError`` arm narrow
    on purpose. So the sibling of a name both tuples already carried ended the whole run.

    **All three pieces are asserted, and the bar walk is the half the ledger tuple cannot
    reach.** Measured with only ``_LEDGER_REFUSALS`` widened, the two ledger walks refused and
    the run still died on this raise coming out of ``backfill_bars``. The bar walk gets there
    because marketlake #422 put ``read_capture_spans`` in front of its master read and this
    fixture carries the spans.
    """
    root = _lake(fixture_lake)
    _bump_master_schema_version(root)

    outcome, pinger, transport = _run(root)

    pieces = dict(outcome.nightly.pieces)
    for name in ("dividends", "splits", "bars"):
        assert pieces[name].refusal is not None, f"a newer master escaped the {name} walk"
        assert pieces[name].refusal.startswith("UnsupportedSchemaVersion")

    assert outcome.filed_at is not None, "the report file was lost with the raise"
    assert transport.messages, "the digest was lost with the raise"
    assert pinger.urls == [], "a refused piece must withhold the ping"

    said = [line for line in outcome.nightly.problems if "did not run" in line]
    assert len(said) == 3, f"three pieces refused and {len(said)} were recorded"
    assert "bars: did not run, UnsupportedSchemaVersion" in transport.messages[0].body


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

    **The level matters, and this is why the fix went into the walk.** A refusal caught at the
    sweep ends the whole walk, and the walk is ordered by ticker, so containing a per-ticker
    condition there would cost every ticker after the quarantined one its dividends, which is the
    same defect one level up.

    ``_LEDGER_REFUSALS`` was left alone for that reason and no longer is: marketlake #435 added
    ``OSError`` to it, because an unreadable reference file is opened before any ticker is walked
    and escaped the sweep entirely, costing the report file, the digest and the ping. That is a
    net under this rule rather than a replacement for it. The sentence above still decides where a
    *per-ticker* condition belongs, and marketlake #446 owns the ones now caught too high.
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
    # The digest goes to a phone and the block goes to the job's own stdout. They are two
    # separate compositions of the same counts, so each is asserted rather than one standing
    # in for the other.
    assert "dividends: landed 0, held 0, unchanged 0, skipped 1" in outcome.digest.body
    assert "dividends: landed 0 held 0 unchanged 0 skipped 1" in outcome.render()


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
    """All three walks refuse, and each names its own condition rather than a shared one.

    Each one names itself, so the digest says three pieces refused rather than handing the
    operator a stack trace in the job's error log.

    **The three no longer meet one condition, and the refusal classes are asserted for that
    reason.** This fixture has neither reference file, and marketlake #422 put a capture-spans
    read in front of the bars walk's master read. So the ledger walks still refuse on
    ``MasterAbsent``, which ``python -m lake.onboard`` fixes, while the bars walk refuses first on
    ``SpansAbsent``, which ``python -m lake.seed_spans`` fixes. Asserting ``finished`` alone let
    that difference arrive silently, and a reader of the old sentence would have gone to the wrong
    command.
    """
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    outcome, pinger, transport = _run(root)
    refused = {name: piece.refusal for name, piece in outcome.nightly.pieces if not piece.finished}
    assert list(refused) == ["dividends", "splits", "bars"]
    assert refused["dividends"].startswith("MasterAbsent")
    assert refused["splits"].startswith("MasterAbsent")
    assert refused["bars"].startswith("SpansAbsent")
    assert pinger.urls == []
    assert "did not run" in transport.messages[0].body


def test_the_bars_walk_names_a_missing_master_once_its_spans_are_there(
    fixture_lake: FixtureLake,
):
    """The master refusals the bars walk can still meet, now that a spans read runs in front.

    Marketlake #422 put ``read_capture_spans`` before the walk's own ``_read_master``, so the one
    test that used to cover ``MasterAbsent`` for this piece refuses earlier and never reaches it.
    Removing ``MasterAbsent`` or ``MasterUnreadable`` from ``_BARS_REFUSALS`` left the suite green,
    which is how that gap was found. Marketlake #497 then replaced both with ``ActionsError`` and
    ``SecurityMasterError``, so the tuple names their classes rather than the two names above.
    This fixture has the spans and no master, so the walk reaches the condition they cover.
    """
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    fixture_lake.with_reference("capture_spans", _spans().to_table())
    root = fixture_lake.build()

    outcome, pinger, _ = _run(root)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.refusal is not None, "a missing master escaped the bars walk"
    assert bars_piece.refusal.startswith("MasterAbsent")
    assert pinger.urls == [], "a refused piece must withhold the ping"

    # The sibling condition, which is the file being there and unreadable rather than absent. The
    # two are separate names because the fixes differ, a rebuild against a restore, and each needs
    # its own fixture to be held at all.
    master_path(root).write_bytes(b"not a parquet file")
    torn, torn_pinger, _ = _run(root)

    torn_piece = dict(torn.nightly.pieces)["bars"]
    assert torn_piece.refusal is not None, "an unreadable master escaped the bars walk"
    assert torn_piece.refusal.startswith("MasterUnreadable")
    assert torn_pinger.urls == []


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


def test_the_unwalked_lines_are_counted_rather_than_listed(fixture_lake: FixtureLake):
    """One bounded line, because the list grows by a ticker-day per trading day and never shrinks.

    ``retire --remove`` drops the roster entry and leaves the closed span behind, so every session
    that span covered is unresolvable for ever and the plan reports it again every night. Rendered
    one line each, that walks the nightly report into ``digest_body``'s byte cap, and what falls
    off the end first is the battery's own census, which is appended after these lines.

    A line that silences the check above it is worse than no line at all, which is why this one is
    a count and a sample rather than the list. The full list stays in the by-hand ``--backfill``
    run, which has no byte budget.
    """
    root = _lake(fixture_lake, tickers=("SPY", "QQQ"), instrument_ids=(1, 2))
    # QQQ is captured and gone from the roster, which is what ``retire --remove`` leaves.
    roster = Roster.from_mapping({"SPY": {"options": True, "bars": [DAILY_FREQ]}})

    outcome, _, _ = _run(root, roster=roster)

    unwalked = [line for line in outcome.nightly.report if line.startswith("bars unwalked")]
    assert len(unwalked) == 1, f"one line per ticker-day reached the report: {unwalked}"
    assert "ticker-day(s)" in unwalked[0]
    assert "QQQ" in unwalked[0], "the line says nothing about which ticker"
    # And the bars themselves still landed: an unresolved ticker-day is not a refusal.
    assert dict(outcome.nightly.pieces)["bars"].refusal is None


def test_a_spans_file_from_a_newer_writer_refuses_the_piece_not_the_evening(
    fixture_lake: FixtureLake,
):
    """The 18:30 job reads the capture spans for the first time, so it inherits their failures.

    ``bars.main`` already learned this one and wrote it down: naming ``SpansUnreadable`` alone
    left its sibling ``UnsupportedSpansSchemaVersion`` reaching the operator as a stack, and a
    spans file from a newer version of this code is the one shape of it a person actually meets,
    after a rollback or a half-finished deploy.

    Escaping here costs far more than it costs a command. Everything after the pieces block is
    lost with it: the battery, the report file, the digest, the ping, and on a Friday the Sunday
    one-shot wake, so the canary and the scrub do not run either and nothing says why until the
    23:30 missed check pages.

    So the tuple names ``CaptureSpansError`` rather than one member of it, and the run reports the
    refusal, withholds the ping because a refusal is a problem, and finishes everything else.
    """
    root = _lake(fixture_lake)
    table = _spans().to_table()
    bumped = table.set_column(
        table.schema.get_field_index("schema_version"),
        "schema_version",
        pa.array([SPANS_SCHEMA_VERSION + 1] * table.num_rows, type=pa.int32()),
    )
    pa.parquet.write_table(bumped, root / "reference" / "capture_spans.parquet")

    outcome, pinger, transport = _run(root, now=FRIDAY_EVENING, setter=_RecordingSetter())

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.refusal is not None, "the spans version escaped and took the evening"
    assert "SpansSchemaVersion" in bars_piece.refusal or "schema version" in bars_piece.refusal
    # Everything after the pieces block still happened.
    assert outcome.nightly.report is not None
    assert pinger.urls == [], "a refused piece must withhold the ping"
    assert transport.messages, "the digest never went out"


def test_the_walk_reaches_back_further_than_one_session(fixture_lake: FixtureLake):
    """How far back the walk goes, which the one-night-later case cannot say.

    ``test_a_daily_bar_held_tonight_is_reached_again_tomorrow`` needs a reach-back of exactly one
    session, so a walk bounded to a short lookback window satisfies it and the sessions further
    back are never recovered. Clamping each span's start to two days before the run left the whole
    suite green, which is how that gap was found.

    Here the quotes that unhold 2026-09-14's bar do not seal until three sessions later, which is
    the shape a repair takes: someone fixes the gap rows days after the outage. The walk has to
    still be asking about that session.
    """
    root = _lake(fixture_lake, quotes={})
    partition = LakePaths(root).bars_partition_path("SPY", DAILY_FREQ, SESSION)

    first, _, _ = _run(
        root, now=EVENING, vendor_source=_CountingVendorSource(_cassette(session=SESSION))
    )
    assert dict(first.nightly.pieces)["bars"].landed == 0
    assert not partition.exists()

    # Three sessions pass before anything seals the close of record SESSION is judged against.
    late = date(2026, 9, 17)
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))

    fourth, _, _ = _run(
        root,
        now=EVENING + timedelta(days=3),
        vendor_source=_CountingVendorSource(_cassette(session=late)),
    )

    assert partition.exists(), "the walk stopped short of the session it held three nights before"
    table = pa.parquet.read_table(partition)
    assert table.column("bar_ts").to_pylist() == [f"{SESSION.isoformat()}T04:00:00+00:00"]


def test_a_stale_frequency_on_a_retired_ticker_does_not_withhold_the_ping(
    fixture_lake: FixtureLake,
):
    """The nightly walk keeps the nightly's own scope, which is the enabled roster.

    ``_require_supported`` checks ``roster.enabled`` and its docstring says exactly why: "a stale
    ``bars:`` line on a retired ticker, which nothing here would ever fetch, would halt the
    nightly run at exit 2 every night until someone edited a file for a ticker that is not being
    captured." ``backfill_bars`` checks the plan instead, retired tickers included, because a
    by-hand recovery run fetches those on purpose, and ``_require_supported_plan`` records that
    the narrower sentence "stops holding" for it.

    Pointing the nightly job at that walk makes the sentence hold again, so the roster is filtered
    to enabled at the call site. Without that filter this run raises ``UnsupportedBarFreq``, ends
    the bars piece and withholds the ping, for a ticker nothing captures, every night forever.

    QQQ is retired with a frequency the seam has no call for. It has a span and a master entry, so
    the plan reaches it, which is what makes the unfiltered roster fail rather than simply skip.
    """
    root = _lake(fixture_lake, tickers=("SPY", "QQQ"), instrument_ids=(1, 2))
    roster = Roster.from_mapping(
        {
            "SPY": {"options": True, "bars": [DAILY_FREQ], "enabled": True},
            "QQQ": {"options": True, "bars": ["1w"], "enabled": False},
        }
    )

    outcome, pinger, _ = _run(root, roster=roster)

    bars_piece = dict(outcome.nightly.pieces)["bars"]
    assert bars_piece.refusal is None, "a retired ticker's stale line ended the nightly bars walk"
    assert bars_piece.landed == 1
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL], "the nightly ping was withheld"
    assert outcome.nightly.pinged is True


def test_a_daily_bar_held_tonight_is_reached_again_tomorrow(fixture_lake: FixtureLake):
    """Marketlake #422. The defect was that nothing ever came back for a held session.

    A daily bar is judged against the calendar-next session's settled close, and at 18:30 on
    session S that session has not been captured, so every daily bar is held on the night it is
    fetched. ``fetch_session_bars`` said that settles itself because "the next run lands the bar".
    It did not. The next run fetched the *next* session, met the same absence for it, and nothing
    scheduled ever asked about S again. The nightly job therefore landed no daily bar, ever.

    **This is the test the old shape could not pass.** Two runs a day apart over one lake. The
    first is the night of ``SESSION`` with no quotes sealed for the session after it, so no bar
    lands and no partition exists. The second is the following night, by which point that
    session's quotes are sealed, and it has to reach back and land the bar the first run left.

    The second run is the whole point: under a single-session fetch it would ask only about
    ``FOLLOWING`` and ``SESSION``'s partition would stay missing forever.

    **Marketlake #434 changed what night one does about it, not what night two recovers.** That
    night used to fetch the bar, gate it against a close nobody had captured, and hold a finding.
    The reference is this lake's rather than the vendor's, so the walk reads it first and the
    ticker-day is left unsettled with the request unspent. The recovery below is unchanged, which
    is what makes the skip safe: a session that is skipped tonight is a session the next run still
    walks.
    """
    root = _lake(fixture_lake, quotes={})
    partition = LakePaths(root).bars_partition_path("SPY", DAILY_FREQ, SESSION)

    # Night one, the night of SESSION. The close cross-check reads the calendar-next session's
    # settled close, and that session has not been captured yet, so nothing is fetched for it.
    first, _, _ = _run(
        root,
        now=EVENING,
        vendor_source=_CountingVendorSource(_cassette(session=SESSION)),
    )
    held = dict(first.nightly.pieces)["bars"]
    assert (held.landed, held.held) == (0, 0), "the bar landed on the night it was fetched"
    assert not partition.exists()

    # FOLLOWING's quotes seal, which is what its own compaction does the next afternoon.
    fixture_lake.with_quotes("SPY", FOLLOWING, _quotes_table([_quote_row(FOLLOWING)]))

    # Night two. Under a single-session fetch this asks only about FOLLOWING and SESSION's
    # partition stays missing forever. It has to reach back.
    second, _, _ = _run(
        root,
        now=EVENING + timedelta(days=1),
        vendor_source=_CountingVendorSource(_cassette(session=FOLLOWING)),
    )
    landed = dict(second.nightly.pieces)["bars"]
    assert landed.landed == 1, "the run did not reach back to the session it held last night"
    assert partition.exists(), "SESSION's daily partition never landed"

    # And it is SESSION's bar, not the newer session's.
    table = pa.parquet.read_table(partition)
    assert table.column("bar_ts").to_pylist() == [f"{SESSION.isoformat()}T04:00:00+00:00"]


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

    monkeypatch.setattr(sweep, "backfill_bars", refuse)
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
    reporting that it could not tell whether capture was running.

    **What it judges is nothing, and the assertions say so.** The run passes ``day=MONDAY``
    while the fixture seals its quotes partition for ``FOLLOWING``, so the walk finds no
    partition at all. The docstring here used to claim the opposite, that the quotes partition
    came back clean, and the assertions below it were satisfied by a battery that judged
    nothing, so nothing caught the claim. What this holds is the wiring: the record reaches the
    outcome, the scope resolved, and no line was written. The checks themselves are held in
    ``tests/component/test_battery.py``.
    """
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)

    assert outcome.battery is not None
    assert outcome.battery.scope_unknown == 0
    assert outcome.battery.appended == ()
    assert outcome.battery.judged == 0, "the sealed partition is not this run's session"
    assert outcome.battery.sessions_owed > 0, "coverage still walked the span"


def test_the_sweeps_census_carries_every_count_the_battery_produces(fixture_lake: FixtureLake):
    """``SweepOutcome.render``'s own rule: a night that judged nothing and a night that judged
    the lake and found it clean are different answers.

    ``insufficient_history`` was structurally zero while one check existed, so the census could
    omit it and stay true. It reads six against the live lake now, and the two coverage counts
    are the only place a permanently missing session reaches this block at all.

    **The expectation is derived from ``BatteryReport``, not listed here.** Marketlake #477 is
    why. This test hand-listed ten names while the dataclass carried thirteen, so ``deferred``,
    ``withheld`` and ``released`` reached the census's absence without ever failing the test
    that claims in its own name to carry every count. A list written beside the thing it
    describes is a second source for one fact, and this is what that costs. Read off the
    fields, the next count added and not rendered fails here.

    Both collections below fail closed. A new field named in neither one is treated as a count
    and asserted, so adding a field to ``BatteryReport`` forces a decision here rather than
    slipping past. The two assertions above the loop are the other direction: an entry naming a
    field that no longer exists is a stale map, which is the same drift one layer along.

    **The match is on the token and its number, not on the token alone.** A bare ``name in
    line`` is not the guard it reads as, because a field whose name is a substring of a token
    the census already prints passes without being rendered at all. A field named ``owed``
    rides ``sessions_owed``, ``held`` rides ``withheld``, ``scope`` rides both ``out_of_scope``
    and ``scope_unknown``, and ``missing`` rides ``sessions_missing``. Requiring a space, the
    name, and a number closes that family and holds the value's presence at the same time.

    **What deriving gives up.** The ten hand-written strings were a second source, and a second
    source is what catches a rename. Spelled off the fields, the census token and the field name
    cannot disagree, so renaming a field and its token together now passes here where the old
    list failed. That is accepted rather than unnoticed. Restoring the pin means restoring the
    list this test exists to delete, and the two the census deliberately spells differently are
    pinned anyway, in ``renamed`` below.
    """
    named = {field.name for field in fields(BatteryReport)}
    assert CENSUS_RENAMED.keys() <= named, f"stale rename: {CENSUS_RENAMED.keys() - named}"
    assert CENSUS_NOT_COUNTS <= named, f"stale exclusion: {CENSUS_NOT_COUNTS - named}"

    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)
    (line,) = [ln for ln in outcome.render().splitlines() if "battery: judged" in ln]

    for field in fields(BatteryReport):
        if field.name in CENSUS_NOT_COUNTS:
            continue
        name = CENSUS_RENAMED.get(field.name, field.name)
        assert re.search(rf"(?:^|\s){re.escape(name)} -?\d+", line), (
            f"{name} is missing from the census, or carries no number: {line}"
        )


def test_each_census_count_carries_its_own_number():
    """Every count in the census reads its own field, and not the one beside it.

    **Distinct values are the whole test.** The fixture lake judges nothing, so every count the
    sweep renders is zero there and the derived test above cannot tell one field from another:
    cross-wiring ``deferred`` to ``withheld`` in the census line leaves that test, and the whole
    suite, green. Marketlake #477's own review found that by mutation. Thirteen different
    numbers are what separate a census that reads its fields from one that reads a neighbour's.

    ``tests/component/test_battery.py``'s ``test_every_census_line_carries_its_own_number`` is
    this test for the hand run's block. The sweep's block is a second composition of the same
    counts, so it is asserted rather than left to stand on the first.

    **The expectation is read back off the record, not written out here.** Each token's number is
    parsed out of the line and compared to the field it claims to carry, so the assertion covers
    a field added to ``BatteryReport`` later without anyone editing this test. Thirteen literals
    would have held today's counts and let the fourteenth through, which is the shape of the
    defect this whole change exists to fix.

    This builds the outcome rather than running a sweep, for the reason
    ``battery.decide_partition``'s docstring gives for being a pure function: a lake fixture
    that produced thirteen distinct counts would take more setup than the property is worth,
    and the property is about the rendering rather than the walk.
    """
    from lake.alert import Message

    battery = BatteryReport(
        judged=1,
        quarantined=2,
        cleared=3,
        insufficient_history=4,
        out_of_scope=5,
        deferred=6,
        withheld=7,
        released=8,
        unreadable=9,
        scope_unknown=10,
        sessions_owed=11,
        sessions_missing=12,
        appended=("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
    )
    outcome = sweep.SweepOutcome(
        nightly=report.Nightly(day=SESSION, session=True, pinged=True),
        digest=Message(event=NIGHTLY_EVENT, title="t", body="b"),
        delivered=True,
        battery=battery,
    )

    (line,) = [ln for ln in outcome.render().splitlines() if "battery: judged" in ln]

    seen = set()
    for field in fields(BatteryReport):
        if field.name in CENSUS_NOT_COUNTS:
            continue
        token = CENSUS_RENAMED.get(field.name, field.name)
        held = getattr(battery, field.name)
        # ``appended`` is the ledger lines themselves and the census prints how many, which is
        # the one count whose field is not already the number.
        expected = len(held) if isinstance(held, tuple) else held
        printed = re.search(rf"(?:^|\s){re.escape(token)} (-?\d+)", line)
        assert printed, f"{token} is missing from the census, or carries no number: {line}"
        assert int(printed.group(1)) == expected, (
            f"the census prints {token} {printed.group(1)} where {field.name} is {expected}: {line}"
        )
        seen.add(expected)

    # Every count distinct, which is what makes the loop above able to tell one field from the
    # one beside it. A fixture that repeated a value would pass a census reading the wrong field
    # for that pair, so this holds the fixture rather than the code.
    assert len(seen) == 13, f"the fixture must give each count its own value: {sorted(seen)}"


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


def test_a_torn_ledger_is_contained_and_reaches_the_record_rather_than_killing_the_run(
    fixture_lake: FixtureLake,
):
    """Marketlake #469, end to end rather than through a monkeypatched explosion.

    Both walks read through ``lake.loader``, which resolves the quarantine ledger on every
    partition it opens, so a damaged one refuses inside ``extract_dividends``. Executed
    against `8fb1fda` that escaped ``_LEDGER_REFUSALS`` entirely and took the whole 18:30 run
    with it, including the report file and the Friday wake, which is why ``ManifestError``
    joined that tuple as the class rather than by member.

    **The ping is withheld here, and that is the designed meaning rather than a casualty.**
    The ``eod-sweep`` row says a missed ping means the day's official bars or actions are
    missing, and a ledger the loader cannot resolve is exactly a night the dividend walk did
    not run. That is the opposite of the battery's own trouble, which rides ``report`` and
    withholds nothing, because the work the check watches did still happen.

    This lake also carries the case the count is the only witness for: no partition is in
    scope, so ``judge`` never reaches its per-partition ledger read and finishes normally.
    ``count_quarantined`` still reports the damage, into the nightly report rather than into
    ``problems``, so on a night with no session that report file is the only carrier.
    """
    from lake.manifest import append_line, quarantine_path

    root = _lake(fixture_lake)
    ledger = quarantine_path(root)
    with ledger.open("a") as handle:
        handle.write('{"partition": "chains/ticker=SPY/date=2026-09-16.parq')
    append_line(ledger, {"partition": "fused", "verdict": "clean", "check": "e"})
    append_line(ledger, {"partition": "hidden", "verdict": "clean", "check": "e"})

    outcome, pinger, _ = _run(root)

    assert outcome.filed_at is not None, "the run died instead of filing its record"
    assert any(
        "dividends did not run: TornLedger" in problem for problem in outcome.nightly.problems
    ), outcome.nightly.problems
    assert any(
        "quarantine count unreadable: TornLedger" in line for line in outcome.nightly.report
    ), outcome.nightly.report
    assert any("human's job under the lock" in problem for problem in outcome.nightly.problems), (
        "the operator gets the error's name without what to do about it"
    )
    assert outcome.nightly.pinged is False
    assert pinger.urls == []


def test_the_containment_is_the_error_class_and_not_the_one_shape(fixture_lake: FixtureLake):
    """``_LEDGER_REFUSALS`` names ``ManifestError``, and this is what makes that the right word.

    The sibling shape is a quarantine line that parses and names no partition, which
    ``manifest.latest_quarantine_by_check`` raises plain ``ManifestError`` on. It reaches the
    walks by the identical path and predates marketlake #469 entirely: executed against
    `8fb1fda`, this lake took the whole run down, report file and Friday wake included.

    Narrowing the tuple to ``TornLedger`` alone passes every other test in this file, so
    without this one the comment there argues for a class the suite only holds at a member.
    """
    from lake.manifest import append_line, quarantine_path

    root = _lake(fixture_lake)
    append_line(quarantine_path(root), {"verdict": "clean", "check": "e"})
    append_line(quarantine_path(root), {"partition": "x", "verdict": "clean", "check": "e"})

    outcome, _, _ = _run(root)

    assert outcome.filed_at is not None, "the run died instead of filing its record"
    assert any(
        "dividends did not run: ManifestError" in problem for problem in outcome.nightly.problems
    ), outcome.nightly.problems


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


def test_the_split_walk_is_handed_the_run_s_own_calendar(fixture_lake: FixtureLake, monkeypatch):
    """Not one the sweep builds for itself, which would read as working and be untestable.

    ``detect_splits`` decides whether two sealed sessions are adjacent, and that is the
    calendar's answer under marketlake #431. A sweep that constructed its own would agree with
    the injected fake on every ordinary week and disagree on exactly the days a test declares,
    so the identity is the assertion rather than any verdict downstream of it.
    """
    root = _lake(fixture_lake)
    calendar = weekday_sessions(MONDAY, NEXT_MONDAY)
    seen: list[object] = []
    real = sweep.detect_splits

    def recording(**kwargs):
        seen.append(kwargs["calendar"])
        return real(**kwargs)

    monkeypatch.setattr(sweep, "detect_splits", recording)

    sweep.sweep(
        lake_root=root,
        clock=ManualClock(EVENING),
        calendar=calendar,
        roster=_roster(),
        vendor_source=_CountingVendorSource(),
        pinger=FakePinger(),
        ping_url=PING_URL,
        publisher=None,
        schedule_reader=lambda: _schedule_text(),
        schedule_setter=_RecordingSetter(),
    )

    assert seen == [calendar], "the split walk did not get the calendar the run was given"
    assert seen[0] is calendar


def test_the_nightly_reports_a_run_the_request_budget_bounded(fixture_lake: FixtureLake):
    """Marketlake #478's line, and the silence it exists to break.

    A bounded run lands fewer partitions than the lake was owed and is otherwise
    indistinguishable from a complete one: it refuses nothing, holds nothing, files no withheld
    record and pings normally. Without a line here the operator reading the next morning's digest
    would see a healthy evening.

    **It is counted rather than listed, for the reason the two lines beside it give.** The list
    repeats every evening a backfill is still catching up, so rendered whole it would walk this
    report into ``digest_body``'s byte cap and truncate the battery's own census off the end.

    **The line survives redaction, which is the half a substring check would miss.**
    ``report.redacted`` keeps two colon-separated fields and is applied on the way into the
    nightly file and again into the digest, so a line carrying a second ``": "`` would arrive with
    everything after it gone. This asserts the redacted form equals the line, and it holds for
    every count the line can carry rather than only this fixture's: measured in digest form it
    runs 58 bytes at one deferred ticker-day and 63 at five figures of them.

    **What was spent is read off the report rather than off the constant.** ``attempted`` is the
    ticker-days that reached the vendor, so a line disagreeing with the walk is impossible. Naming
    ``guards.bars_request_budget`` would be a second source for one number, and this job may hold
    ``None`` while the walk resolved its own default.
    """
    # Four sessions in range rather than the default fixture's one, because a run that plans a
    # single ticker-day can never defer and so can never produce this line at all.
    later = EVENING + timedelta(days=3)
    walked = _walked(later.date())
    quotes = {("SPY", day): [_quote_row(day)] for day in (*walked, walked[-1] + timedelta(days=1))}
    root = _lake(fixture_lake, quotes=quotes)
    outcome, pinger, _ = _run(
        root,
        now=later,
        vendor_source=_CountingVendorSource(_cassette(session=later.date())),
        guards=GuardConstants(bars_request_budget=1),
    )

    (line,) = [entry for entry in outcome.nightly.report if entry.startswith("bars deferred")]
    assert line == "bars deferred: 3 ticker-day(s), 1 request(s) spent"
    assert report.redacted(line) == line, "the budget was cut off before any reader saw it"
    assert line in outcome.digest.body
    # A bound the run was told to respect is not a failure, so the ping still goes out and the
    # bars piece does not refuse.
    assert pinger.urls == [PING_URL]
    assert outcome.nightly.pinged is True
    assert dict(outcome.nightly.pieces)["bars"].refusal is None


def test_an_unbounded_evening_writes_no_deferred_line(fixture_lake: FixtureLake):
    """The line appears only when the bound actually bit.

    The pinned budget is 100 against a nightly plan of a handful of ticker-days, so on every
    healthy evening this condition is false. A line that appeared regardless would be one the
    reader learns to skip, which is the argument ``dashboard._ping_owed`` already makes in those
    words, and it would spend digest bytes the battery's census needs.

    The condition is on the deferred list rather than on the budget being reached, so a run whose
    last ticker-day was also its last allowed request reports nothing. That case is the one a
    ``>=`` on the budget would get wrong.
    """
    root = _lake(fixture_lake)
    outcome, _, _ = _run(root)
    assert [line for line in outcome.nightly.report if line.startswith("bars deferred")] == []
    # The same run, bounded at exactly what it spends, still writes nothing.
    spent = dict(outcome.nightly.pieces)["bars"].landed
    bounded, _, _ = _run(root, guards=GuardConstants(bars_request_budget=max(spent, 1)))
    assert [line for line in bounded.nightly.report if line.startswith("bars deferred")] == []


def test_the_nightly_reports_a_run_that_deferred_exactly_one_ticker_day(
    fixture_lake: FixtureLake,
):
    """The smallest non-zero case, which is the silence the line exists to break.

    The two tests beside this one pin three deferred and zero deferred, so the boundary between
    them is never exercised and ``if walked.deferred:`` can become ``len(...) > 1`` with the whole
    suite still green. One deferred ticker-day is the case that matters most: it is the first
    evening a backfill starts falling behind, and a run that reports nothing on it looks exactly
    like a run that finished.
    """
    later = EVENING + timedelta(days=3)
    walked = _walked(later.date())
    quotes = {("SPY", day): [_quote_row(day)] for day in (*walked, walked[-1] + timedelta(days=1))}
    root = _lake(fixture_lake, quotes=quotes)
    outcome, pinger, _ = _run(
        root,
        now=later,
        vendor_source=_CountingVendorSource(_cassette(session=later.date())),
        guards=GuardConstants(bars_request_budget=len(walked) - 1),
    )

    (line,) = [entry for entry in outcome.nightly.report if entry.startswith("bars deferred")]
    assert line == f"bars deferred: 1 ticker-day(s), {len(walked) - 1} request(s) spent"
    assert line in outcome.digest.body
    assert pinger.urls == [PING_URL]


# -- the schema-version check ------------------------------------------------------------

# Marketlake #130. Nothing forces ``python -m lake.schema_versions`` to run beside a deliberate
# bump, and a version whose shape the lake has no record of makes every read of its rows
# refuse. The daemon pages once at its own startup. This job is the recurring half, and it is
# owed one because a resident daemon carries the tree it was started with: a deploy with no
# restart lands the new version from this process, since ``backfill_bars`` stamps
# ``journal.SCHEMA_VERSION`` on every bar it writes.


def _stale_ledger() -> pa.Table:
    """A ledger recording the version before the running one, which is the live 2026-09-17
    shape: version 1 recorded on 2026-09-13 and version 2 running since a bump nobody
    recorded."""
    entry = RecordedVersion(
        version=journal.SCHEMA_VERSION - 1,
        recorded_at=RECORDED_AT,
        fingerprints=running_fingerprints(),
    )
    return SchemaVersionLedger([entry]).to_table()


def _version_lines(outcome) -> list[str]:
    return [line for line in outcome.nightly.report if line.startswith("schema_version")]


def test_a_run_whose_version_the_ledger_does_not_know_files_a_line_and_still_pings(
    fixture_lake: FixtureLake,
):
    """Report-tier, not a problem, which is ``_counted``'s line from the other side.

    The work the ``eod-sweep`` check watches did happen. A check that withheld the ping over
    a reference table nobody had written would turn a job that worked into a red row, and the
    red row means the day's official bars or actions are missing.
    """
    root = _lake(fixture_lake, ledger=_stale_ledger())

    outcome, pinger, _ = _run(root)

    (line,) = _version_lines(outcome)
    assert str(journal.SCHEMA_VERSION) in line
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]


def test_the_line_repeats_on_a_holiday_because_the_condition_is_not_a_walk(
    fixture_lake: FixtureLake,
):
    """A holiday skips the three walks and this is not a walk.

    The condition does not depend on the session and the report file is written on every run,
    holiday no-op included. Put beside the walks it would go quiet on every holiday, which is
    the run most likely to follow a deploy.
    """
    root = _lake(fixture_lake, ledger=_stale_ledger())

    outcome, pinger, transport = _run(root, holidays=(SESSION,))

    assert outcome.nightly.session is False
    assert len(_version_lines(outcome)) == 1
    assert pinger.urls == [PING_URL]
    # The digest still sends the design's pinned holiday line and nothing else, so the file
    # is the only surface a holiday finding reaches.
    assert transport.messages[0].body == HOLIDAY_BODY


def test_the_line_reaches_the_report_file_and_the_digest(fixture_lake: FixtureLake):
    """Two surfaces, and the digest is the one with a rule attached.

    ``digest_body`` passes every report line through ``report.redacted``, which drops
    everything past the second colon-separated field. A line composed as a place, then a
    verdict, then a version would reach the phone with the version gone.
    """
    root = _lake(fixture_lake, ledger=_stale_ledger())

    outcome, _, transport = _run(root)

    (filed,) = _filed(root)
    (line,) = _version_lines(outcome)
    assert line in filed["report"]
    assert f"report: {line}" in transport.messages[0].body
    assert str(journal.SCHEMA_VERSION) in transport.messages[0].body


def _conflicting_ledger() -> pa.Table:
    """The running version recorded under a shape the running code does not have.

    A column the ledger holds and the code dropped, which is the direction no read-time
    refusal can see: it falls outside the projection's reachable set, so the read comes back
    whole while the dropped column's nulls read as vendor nulls.
    """
    shapes = {surface: dict(columns) for surface, columns in running_fingerprints().items()}
    shapes[journal.CHAINS_SURFACE]["gamma_impact"] = "double"
    entry = RecordedVersion(
        version=journal.SCHEMA_VERSION, recorded_at=RECORDED_AT, fingerprints=shapes
    )
    return SchemaVersionLedger([entry]).to_table()


def test_a_conflicting_ledger_files_its_own_line(fixture_lake: FixtureLake):
    """The second verdict, and it is not the one the other cases here drive.

    Every case above builds a stale ledger, so a caller that filed a line for `unrecorded`
    alone would leave the two verdicts that compound unreported and pass all of them. This is
    the worse of the two, because nothing at read time refuses it.
    """
    root = _lake(fixture_lake, ledger=_conflicting_ledger())

    outcome, pinger, _ = _run(root)

    (line,) = _version_lines(outcome)
    assert "different shape" in line
    assert outcome.nightly.problems == ()
    assert pinger.urls == [PING_URL]


def test_a_run_against_a_recorded_version_files_no_line(fixture_lake: FixtureLake):
    """The steady state, and the case that decides whether this line is noise.

    ``_ledger_table`` records the running version, so every other case in this file drives
    this branch and would go red on a line that fired unconditionally.
    """
    root = _lake(fixture_lake)

    outcome, _, _ = _run(root)

    assert _version_lines(outcome) == []


def test_the_line_carries_no_machine_path(fixture_lake: FixtureLake):
    """``report.redacted`` exists because this file sits in the directories the dashboard may
    read, so a capture-machine path stops at stderr."""
    root = _lake(fixture_lake, ledger=_stale_ledger())

    outcome, _, _ = _run(root)

    (line,) = _version_lines(outcome)
    assert LEDGER_PARTITION in line
    assert str(root) not in line


def test_an_unreadable_ledger_reaches_the_report_on_a_holiday_and_nowhere_else_yet(
    fixture_lake: FixtureLake,
):
    """The third verdict, and the bound marketlake #494 puts on it.

    A holiday opens no reference file, so the check is the only reader of the ledger and its
    line lands. A session evening does not get that far: ``_LEDGER_REFUSALS`` does not name
    ``SchemaVersionsError``, so ``extract_dividends`` lets ``LedgerUnreadable`` out and it
    escapes ``sweep`` with the report file, the digest and the ping. The line is computed and
    lost with them.

    That escape predates the check and is #494's to close. This case is here so the bound is
    written down where the next reader meets it, and so the day #494 lands turns the second
    half of this test red rather than leaving it to be noticed.

    The daemon's startup check reports the same verdict meanwhile, which is
    ``test_daemon_wiring.test_an_unreadable_ledger_pages_under_its_own_event``.
    """
    root = _lake(fixture_lake)
    (root / "reference" / "schema_versions.parquet").write_bytes(b"not parquet at all")

    outcome, pinger, _ = _run(root, holidays=(SESSION,))

    (line,) = _version_lines(outcome)
    assert "LedgerUnreadable" in line
    assert pinger.urls == [PING_URL]

    # And the session evening, which does not survive to file anything. The holiday run above
    # already filed one, so what this counts is that the second run added none.
    before = len(_filed(root))
    with pytest.raises(LedgerUnreadable):
        _run(root)
    assert len(_filed(root)) == before, "the run that raised must not have filed a report"
