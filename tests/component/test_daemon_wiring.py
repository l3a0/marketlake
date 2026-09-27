"""The daemon's production hook bindings, over real files.

``run_loop`` owns the loop and ``DaemonHooks`` owns the five seams it fires. Every
observer that plugs into a seam is covered on its own elsewhere. What these cover is the
wiring ``run_loop_from_config`` builds between the two, which is the wiring the launchd
job runs. A binding can be deleted with every isolated test still green, so each case
here drives the production entry and watches the far end of one binding, apart from the
case under item 15, which watches the reads instead.

Each runs that entry with a manual clock, a fake calendar, and a throwaway config,
roster, and lake on disk. The three seams that reach past the process are faked: the
ntfy transport, the health-check pinger, and the backup's ``rsync``. A page sent from a
test is a page a person receives, and a sync from one copies a throwaway lake onto the
machine running the suite. So the tier is component: the daemon over real files, with the
clock, the calendar, the network, and the backup still fake.

Fifteen bindings are covered here.

1. The skipped-slot hook reaches the gap marker, so a live overrun records the minutes
   it slept through.
2. The skipped-slot hook reaches the watchdog, so those same minutes charge its counters.
3. The per-tick hook feeds the capture dead-man's idle heartbeat.
4. The cycle hook feeds the same dead-man's ``captured`` signal, which arms the check on
   the first durable cycle.
5. The per-tick hook reaches the close+5 guard's dispatcher, so a daemon alive across
   close+5 runs the guard on that minute. On a tick that wakes from a stall across the
   equity close, the dispatch waits for the skipped-slot hook and runs after its markers, so
   every ordering of the guard and gap marking leaves one row at the equity close.
6. The cycle runner is the production entry that re-reads the chain plan, so a nightly
   plan rewrite takes effect the next minute.
7. The skipped-slot hook charges the counters the current roster names. It re-reads the
   file rather than closing over the startup roster, so a ticker retired mid-session
   stops paging without a restart and one onboarded mid-session starts. A file the roster
   loader refuses is fatal rather than fallen back on, and every way that file can fail
   now reaches the caller as one `TickersError`. Each entry expands into the surfaces its
   ticker is captured on, so an options ticker's chains counter is charged beside its
   quotes.
8. The configured page threshold reaches the watchdog, and a mid-session recalibration
   of ``watchdog_page_minutes`` takes effect on the next cycle without a restart. The
   value is read off the config each time the watchdog decides to page. Baking it in at
   loop start, which is what the daemon did before, would hold the old number and no
   other case here drives a config edit to catch it.
9. An empty roster keeps the loop, the dead-man, and the watchdog running, because those
   report on the daemon's own health rather than on any ticker's.
10. The per-tick hook reaches the close+15 compaction, so the daemon seals and backs up
    its own day rather than waiting for a hand-run command. The job is spawned as its own
    process, because the design gives it roughly 25 minutes and the dead-man's heartbeat
    rides this same hook under a 5-minute grace. The dispatch fires one tick past its
    moment, which puts the seal after both gap-marking writers, and never inside a capture
    window. A day with no session falls back to the regular wall-clock time, so the
    ``compaction`` check is fed on a holiday rather than paging about an idle daemon.
11. The close+5 dispatch's job reaches the reports tree, so what the guard found lands in
    a file rather than only on the stderr launchd captures and nothing reads. The write is
    wired through a factory that resolves the lake root, which the printer never had.
12. The daemon builds the dead-man with the same publisher the watchdog pages through, so a
    capture ping healthchecks refuses reaches a phone. That ping feeds no check, so the
    check never arms and nothing in the lake would ever go silent to say so.
13. The cycle hook reaches the schema-drift observer, so a vendor that retypes a known
    field pages the minute the parser sees it. The observer carries state between cycles,
    so the binding also decides whether a drift that persists pages once or once a minute,
    and only the production entry can be asked that.
14. Startup reaches the schema-version check, so a daemon started on a version the lake's
    ledger has no shape for pages instead of capturing in silence. The binding decides two
    things nothing else can: that the check reports rather than refusing to start, and that
    it speaks once for the process rather than once a tick.
15. The loop hands every helper the config, roster and token paths it was given,
    unchanged, including ``None``. The installed plist passes no path at all, so ``None``
    is what production runs on. Items 1 to 14 watch each far end under real paths, and a
    helper that mangled ``None`` would pass all of them, since most helpers answer a file
    that will not load by standing down in silence. So this case records the path each
    config, roster and token read receives, and every one has to be ``None`` or the
    default token path.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import capture, close_guard, daemon, gap, journal, report, schema_versions
from lake.alert import PAGE_PRIORITY, Message
from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.capture_spans import CaptureSpans, spans_path
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.compact import COMPACTION_SLUG, compact, write_chain_plan
from lake.config import CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH, GuardConstants
from lake.deadman import CAPTURE_SLUG
from lake.paths import LakePaths
from lake.runner import PING_REFUSED_EVENT
from lake.schema_drift import SCHEMA_DRIFT_EVENT, SCHEMA_DRIFT_TITLE
from lake.schema_versions import check_running_version, ledger_path
from lake.schwab import DEFAULT_TOKEN_PATH
from lake.security_master import SecurityMaster, master_path
from lake.session import SPOT_CLOSE, TICK
from lake.tickers import DEFAULT_TICKERS_PATH, TICKERS_PATH_ENV, TickersError
from lake.vendor import VendorResponse
from tests.support.backup import FakeBackup
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.config_guard import is_protected
from tests.support.path_reads import FROM_TOKEN, PathReads
from tests.support.pinger import FakePinger
from tests.support.schema_version import record_running_version

# The Monday of the week these runs live in. Its sessions run Monday through Friday,
# each opening at 09:30 and closing at 16:00, so the option close lands at 16:15.
WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)
NEXT_DAY = date(2026, 9, 3)

# The checks the daemon feeds, as the throwaway config addresses them. The dead-man
# takes the first every minute it is awake, and the close+15 job the second once a day.
CAPTURE_URL = f"https://hc-ping.com/{PING_KEY}/{CAPTURE_SLUG}"
COMPACTION_URL = f"https://hc-ping.com/{PING_KEY}/{COMPACTION_SLUG}"

EQUITY_ONLY = "XYZ: {options: false}\n"
WITH_OPTIONS = "SPY: {options: true, chain_cadence: 1m}\n"

# Two equity-only tickers, and the same roster after one is retired. Neither carries
# options, so each ticker owns exactly one counter. One overrun raises one page for
# every surface it charged, so the count on that page is how many counters the hook
# found on the roster it read.
TWO_TICKERS = "XYZ: {options: false}\nABC: {options: false}\n"
ONE_RETIRED = "ABC: {options: false}\n"

# The same edit gone wrong: valid YAML the roster loader still refuses, because XYZ's
# settings are a bare string rather than a mapping. Had it loaded, XYZ would be gone.
# So a run that charges XYZ anyway fell back rather than read this file.
UNLOADABLE = "XYZ: retired\nABC: {options: false}\n"

# The two tickers after a third is onboarded. Only a hook that re-reads charges DEF,
# because DEF was never in the roster the daemon started with.
THREE_TICKERS = "XYZ: {options: false}\nABC: {options: false}\nDEF: {options: false}\n"

# The same edit caught half saved, cut in the middle of a line. `yaml` refuses this one
# where it accepted UNLOADABLE, so the two together cover both ways the file can fail.
MID_LINE_TEAR = "XYZ: {options: fal"

# A stall long enough to cross the option close. The waking tick reports the window's
# tail and then runs no cycle, which is the one tick where the hook's read is the only
# read of the roster that minute.
ACROSS_THE_CLOSE = 3600

# One open-ended window, the smallest plan that tiles the offset line. The rewrite
# splits its head off, so the two plans ask for different date ranges.
ONE_WINDOW = ChainPlan(((0, None),))
SPLIT_WINDOW = ChainPlan(((0, 0), (1, None)))

# A batched-quote response the row builder can read, so the quotes leg lands beside the
# chain rather than failing for a reason the test did not ask about.
QUOTE_BODY = {
    "SPY": {
        "assetMainType": "EQUITY",
        "realtime": True,
        "quote": {
            "bidPrice": 649.98,
            "askPrice": 650.02,
            "lastPrice": 650.0,
            "quoteTime": 1787000100000,
        },
    }
}


class _Recording:
    """A ``Transport`` that keeps each page instead of posting it."""

    def __init__(self) -> None:
        self.sent: list[Message] = []

    def send(self, message: Message) -> None:
        self.sent.append(message)


class _Compactions:
    """A ``CompactionRunner`` that records each spawn instead of starting a child.

    The daemon's job is to issue the right command at the right minute. What that
    command then does is ``compact``'s own contract, covered over real files in
    ``test_compaction.py``. So this records the argv and the moment, and a test that
    wants the seal itself asks for ``run_here``.

    ``run_here`` makes the recorder perform the compaction in-process instead, with the
    test's own seams. That is how a case about *ordering* reads what is on disk at the
    moment the daemon dispatched, which is the one thing a recorded argv cannot show.
    """

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self._run_here: Callable[[], None] | None = None

    def run_here(self, run: Callable[[], None]) -> None:
        """Perform the compaction in-process on every dispatch, instead of recording only."""
        self._run_here = run

    def __call__(self, args: Sequence[str]) -> object:
        self.calls.append(list(args))
        if self._run_here is not None:
            self._run_here()
        return None


@dataclass(frozen=True)
class _Rig:
    """The throwaway machine one daemon run reads, and the fakes for its three endpoints."""

    lake_root: Path
    config: Path
    tickers: Path
    token: Path
    plan: Path
    transport: _Recording
    pinger: FakePinger
    compaction: _Compactions


def _rig(
    tmp_path: Path,
    roster: str = EQUITY_ONLY,
    *,
    guards: Mapping[str, object] | None = None,
) -> _Rig:
    """A complete config, roster, and lake under ``tmp_path``.

    ``guards`` renders a recalibrated guard section into the config, the way a hand edit
    sets one, so a test can drive a non-default constant through the production entry.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    # A production lake records the running schema version, and the daemon pages at startup
    # when it does not, which is marketlake #130. A rig without one would put that page in
    # front of every case here, so the fixture models the machine rather than the empty
    # directory. ``test_schema_versions.py`` drives the lake that has no ledger.
    record_running_version(lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(roster)
    return _Rig(
        lake_root=lake_root,
        config=write_config(tmp_path, lake_root, guards=guards),
        tickers=tickers,
        token=tmp_path / "token.json",
        plan=tmp_path / "chain_plan.json",
        transport=_Recording(),
        pinger=FakePinger(),
        compaction=_Compactions(),
    )


def _stop_after(count: int) -> Callable[[], bool]:
    """A ``should_continue`` that bounds the loop at ``count`` ticks."""
    remaining = [count]

    def should_continue() -> bool:
        if remaining[0] == 0:
            return False
        remaining[0] -= 1
        return True

    return should_continue


def _run(
    rig: _Rig,
    clock: ManualClock,
    *,
    ticks: int,
    cycle_runner=None,
    hooks: daemon.DaemonHooks | None = None,
) -> None:
    """Run the production entry over ``ticks`` minutes of the fake calendar's week.

    ``cycle_runner`` left unset is the real one, the closure over the capture entry.
    The power assertion is a seam for a reason: left to its default it spawns the real
    ``caffeinate``, which exists on macOS and not on a Linux CI runner. The backup is a
    seam for the same reason, one step further: left to its default it would ``rsync``
    the throwaway lake onto the machine running the suite.

    The compaction runner is a seam for the same reason once more: left to its default it
    spawns a real ``python -m lake.compact`` child against whatever config it is handed.
    """
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        cycle_runner=cycle_runner,
        hooks=hooks,
        should_continue=_stop_after(ticks),
    )


def _record(root: Path, surface: str, ticker: str, slot: datetime, *, kind: str = "x") -> None:
    """Put one recorded row on disk, standing for a cycle that already landed."""
    batch = journal.gap_rows(surface, ticker=ticker, slots=[slot], error_class=kind)
    stamp = slot.strftime(gap.SEGMENT_STAMP_FORMAT)
    with journal.SegmentWriter.open(root, surface, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _segments(root: Path, ticker: str, day: date = DAY) -> list[Path]:
    """The journal segments still on disk for one ticker-day's quotes.

    Compaction deletes a ticker-day's segments only after its partition is manifested,
    so an empty list beside a sealed partition says the seal finished. A segment left
    here after a run is either one the job refused or one a later writer added, and the
    two cases are told apart by which rows the partition carries.
    """
    directory = journal.segment_dir(root, journal.QUOTES_SURFACE, ticker, day)
    return sorted(directory.glob("*.arrows")) if directory.is_dir() else []


def _rows(root: Path, surface: str, ticker: str, day: date) -> list[dict]:
    """Every journalled row for one surface, ticker, and date."""
    directory = journal.segment_dir(root, surface, ticker, day)
    if not directory.is_dir():
        return []
    return [
        row
        for path in sorted(directory.glob("*.arrows"))
        for row in journal.read_segment(path).to_pylist()
    ]


class _Overrunning:
    """A cycle runner whose first call outlives its minute.

    The loop aligns to the next minute top from wherever the clock stands, so a cycle
    that takes longer than a minute is how a live daemon sleeps through a capture slot.
    Only the first call takes time, which keeps the skipped run a single stretch.
    """

    def __init__(self, clock: ManualClock, seconds: float) -> None:
        self._clock = clock
        self._seconds = seconds
        self.slots: list[datetime] = []

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        self.slots.append(slot)
        if len(self.slots) == 1:
            self._clock.advance(self._seconds)
        return CycleResult(snap_ts=slot, segments=())


def _no_cycle(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
    """A cycle runner for the minutes off the capture window, where none may run."""
    raise AssertionError("a cycle ran off the capture window")


def _segment(
    row_kind: str,
    root: Path,
    surface: str = journal.QUOTES_SURFACE,
    ticker: str = "XYZ",
    routed: tuple[str, ...] = (),
    rows: int = 1,
    data_rows: int | None = None,
) -> SegmentOutcome:
    """One journalled segment of the named kind, the shape a cycle result carries.

    ``surface`` and ``ticker`` default to quotes on XYZ, the values every caller used
    before they were parameters, so a test that only cares about the row kind passes
    neither. A cycle carrying two segments has to name both, because the watchdog keys
    its counters on the surface and ticker together. Two calls at the defaults would be
    one surface reported twice, and a cycle plans one segment per pair, so production
    never emits that.

    ``routed`` is the columns whose vendor field arrived at a type the column refused,
    empty on every ordinary segment, which is what the schema-drift page reads.

    ``data_rows`` defaults to every row on data and none on a gap. A data segment holding
    marker rows and no contract takes ``rows`` above zero with ``data_rows=0``.
    """
    if data_rows is None:
        data_rows = rows if row_kind == journal.ROW_KIND_DATA else 0
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=root / "segment.arrows",
        partition=f"{surface}/ticker={ticker}/date=2026-09-02/segment.arrows",
        row_kind=row_kind,
        rows=rows,
        error_class=None if row_kind == journal.ROW_KIND_DATA else "boom",
        fetched_at=None,
        data_rows=data_rows,
        routed_columns=routed,
    )


# -- 1. the skipped-slot hook reaches the gap marker ---------------------------------


def test_a_live_overrun_reaches_the_gap_marker(tmp_path):
    """A minute the daemon slept through has to leave a marker row.

    Completeness is counted from rows and never from holes. Startup marking covers the
    minutes lost while the daemon was dead, and the loop is the only piece that can see
    a slot a living daemon overran. So an unbound skipped-slot hook loses exactly the
    minutes nothing else can report.
    """
    rig = _rig(tmp_path)
    # A row at 09:58 stops startup marking at the daemon's own start minute, so every
    # marker past it came from the loop.
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 9, 58))
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=2, cycle_runner=_Overrunning(clock, 150))

    # The 10:00 cycle ran two and a half minutes, so the loop next woke at 10:03 and
    # slept through the two slots between.
    overran = sorted(
        row["snap_ts"][:16]
        for row in _rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY)
        if row["error_class"] == gap.SLOT_OVERRUN
    )
    assert overran == ["2026-09-02T10:01", "2026-09-02T10:02"]


# -- 2. the skipped-slot hook reaches the watchdog -----------------------------------


def test_the_minutes_a_live_overrun_slept_through_charge_the_watchdog(tmp_path):
    """The minutes the daemon was worst off have to be the ones its counters see.

    The loop runs no cycle for a slot it slept through, so the cycle observer never
    sees those minutes. An unbound skipped-slot hook leaves a daemon that overran for
    an hour looking healthy, because its counters only ever saw the cycles that ran.
    """
    rig = _rig(tmp_path)
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 9, 58))
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=2, cycle_runner=_Overrunning(clock, 200))

    # Three slept-through slots is the design's page threshold. The cycles themselves
    # produced no segment, so nothing but the missed minutes charged a counter.
    (page,) = rig.transport.sent
    assert page.event == "capture_down"
    # The stall is what the page is about, and it says how many slots it slept through.
    # One surface was charged, so there is no fold count to carry.
    assert page.title == "Capture down: loop overran"
    # No class is named, and that is the point rather than an omission. Nothing was
    # attempted in a slept-through slot, so there is no failure to name.
    assert page.body == "3 session minutes without a durable cycle"


# -- 3. the per-tick hook feeds the idle heartbeat -----------------------------------


def test_an_idle_minute_feeds_the_capture_dead_man(tmp_path):
    """A daemon awake with nothing to capture still has to be heard from.

    The check outside the machine is what catches a daemon that died, and it pages on
    silence. The minutes before the open and the whole of a holiday produce no cycle,
    so the heartbeat is the only thing feeding it then. An unbound tick hook turns
    every one of those minutes into a false page.
    """
    rig = _rig(tmp_path)
    # 08:30 is past the firmware wake and an hour before the open, so the tick is
    # inside the envelope the daemon is kept awake for and outside the capture window.
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))
    _run(rig, clock, ticks=1, cycle_runner=_no_cycle)

    assert rig.pinger.urls == [CAPTURE_URL]


# -- 12. the dead-man pages when its own ping is refused ---------------------------


class _RefusingPinger:
    """A pinger healthchecks answers with a 404: the slug has no row.

    It records the same way ``FakePinger`` does, so a test can still read which check was
    addressed. What it adds is the status, which is the whole distinction: a refused ping
    means the request was read and the check does not exist.
    """

    def __init__(self) -> None:
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)


def test_a_capture_ping_the_service_refuses_pages_through_the_daemon(tmp_path):
    """The dead-man's own slug is the one failure nothing else can report.

    Every other check goes down on its own when its job stops. This one cannot: a ping
    to a slug with no row feeds nothing, so there is no check to go silent. The daemon
    would run a whole session reporting healthy while its whole-daemon guarantee sat
    inert. The page has to come from in here, and it comes through the publisher the
    watchdog already pages through, which a wiring slip would leave unbuilt.
    """
    rig = replace(_rig(tmp_path), pinger=_RefusingPinger())
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))
    _run(rig, clock, ticks=3, cycle_runner=_no_cycle)

    assert rig.pinger.urls == [CAPTURE_URL] * 3
    pages = [m for m in rig.transport.sent if m.event == PING_REFUSED_EVENT]
    # Three minutes, three refused pings, one page. A slug with no row is refused on
    # every minute of the session, and forty of those would empty the day's cap.
    assert len(pages) == 1
    assert CAPTURE_SLUG in pages[0].body
    assert PING_KEY not in pages[0].body


def test_a_capture_ping_lost_in_transport_pages_nobody(tmp_path):
    """A wifi blip drops pings while capture keeps journaling locally.

    The design sets this check's grace looser than the watchdog's for exactly that, so
    healthchecks pages from outside if the outage lasts. A page raised from in here
    would fail in the same outage that dropped the ping.
    """

    class _Unreachable:
        urls: list[str] = []

        def ping(self, url: str) -> None:
            raise urllib.error.URLError(OSError("connection refused"))

    rig = replace(_rig(tmp_path), pinger=_Unreachable())
    _run(rig, ManualClock(start=et(2026, 9, 2, 8, 29, 30)), ticks=3, cycle_runner=_no_cycle)

    assert rig.transport.sent == []


# -- 4. the cycle hook feeds the same dead-man -------------------------------------


@pytest.mark.parametrize(
    ("row_kind", "pings"),
    [(journal.ROW_KIND_DATA, 1), (journal.ROW_KIND_GAP, 0)],
)
def test_only_a_durable_cycle_arms_the_capture_dead_man(row_kind, pings, tmp_path):
    """The durable cycle is what holds the whole-daemon guarantee through a session.

    The idle heartbeat stands down at the open, because a capture minute belongs to the
    cycle that owns it. Inside the session the cycle hook is the only feed the check
    has, so an unbound one leaves a perfectly capturing daemon silent from the open
    onward. A gap-only cycle must feed nothing, or a surface failing every minute would
    report itself healthy.
    """
    rig = _rig(tmp_path)
    result = CycleResult(et(2026, 9, 2, 11, 59), (_segment(row_kind, tmp_path),))
    clock = ManualClock(start=et(2026, 9, 2, 11, 58, 30))
    _run(rig, clock, ticks=1, cycle_runner=lambda *, slot, close_tag, session_phase: result)

    assert rig.pinger.urls == [CAPTURE_URL] * pings


@pytest.mark.parametrize("data_first", [True, False])
def test_one_live_surface_beside_a_dead_one_still_arms_the_dead_man(data_first, tmp_path):
    """A partly failing cycle is a living daemon, and the check must hear from it.

    The feed asks whether the daemon is alive, not whether every surface is well. A
    chains segment of real rows only exists because the loop ran, the vendor answered,
    and a write landed, so the dead-man has its answer whatever else that minute lost.
    The failed surface beside it belongs to the watchdog, which counts it and pages
    under that surface's own name once it stays down. Going silent here would report
    that one failure a second time, as the whole daemon being gone.

    So the feed asks whether any segment landed data, and a cycle of one segment cannot
    tell that from asking whether all of them did. This one carries two. Under ``all``
    the common partial failure would stop feeding the check, and the check pages on
    silence.

    The two orderings run because the answer is about the whole cycle, not about
    whichever segment the runner happened to write first. Reading only
    ``result.segments[0]`` passes the one ordering and is wrong.
    """
    rig = _rig(tmp_path, WITH_OPTIONS + EQUITY_ONLY)
    landed = _segment(journal.ROW_KIND_DATA, tmp_path, journal.CHAINS_SURFACE, "SPY")
    gapped = _segment(journal.ROW_KIND_GAP, tmp_path, journal.QUOTES_SURFACE, "XYZ")
    segments = (landed, gapped) if data_first else (gapped, landed)
    result = CycleResult(et(2026, 9, 2, 11, 59), segments)
    clock = ManualClock(start=et(2026, 9, 2, 11, 58, 30))
    _run(rig, clock, ticks=1, cycle_runner=lambda *, slot, close_tag, session_phase: result)

    assert rig.pinger.urls == [CAPTURE_URL]


@pytest.mark.parametrize(("data_rows", "pings"), [(0, 0), (2, 1)])
def test_a_chain_holding_no_contract_beside_a_failed_quote_leaves_the_dead_man_silent(
    data_rows, pings, tmp_path
):
    """A minute that landed no data row is not evidence of capture, whatever its kind.

    Quotes are captured for every roster ticker, so the feed reaches this only in a
    minute the batched quotes request failed as well. A chain beside it that answered 200
    with no contract lands a data segment holding only the failed window's marker, and
    read by its kind it fed the check for a daemon that captured nothing (marketlake
    #326). A chain that lost one window and landed the rest holds real contracts, so it
    still feeds the check, which is the other side of the boundary.
    """
    rig = _rig(tmp_path, WITH_OPTIONS + EQUITY_ONLY)
    chain = _segment(
        journal.ROW_KIND_DATA,
        tmp_path,
        journal.CHAINS_SURFACE,
        "SPY",
        rows=data_rows + 1,
        data_rows=data_rows,
    )
    quote = _segment(journal.ROW_KIND_GAP, tmp_path, journal.QUOTES_SURFACE, "XYZ")
    result = CycleResult(et(2026, 9, 2, 11, 59), (chain, quote))
    clock = ManualClock(start=et(2026, 9, 2, 11, 58, 30))
    _run(rig, clock, ticks=1, cycle_runner=lambda *, slot, close_tag, session_phase: result)

    assert rig.pinger.urls == [CAPTURE_URL] * pings


def test_a_cycle_that_journalled_nothing_leaves_the_dead_man_silent(tmp_path):
    """A cycle that wrote no segment at all captured nothing, and must not say it did.

    A non-empty roster whose every write failed produces this shape: no segment, and one
    error per segment the cycle could not journal. That is the daemon owing data and
    landing none, which is the outage the external check exists to page on.

    It is a separate case from the empty roster, where every ticker has retired, there
    is nothing to fetch, and the daemon is idle by design. Only that one may feed the
    check, and it does through ``nothing_to_capture``. Both arrive with no segments, so
    a reading over all of them cannot tell the two apart, because an empty run of
    segments satisfies ``all`` and fails ``any``. Under ``all`` this cycle would report
    a daemon capturing nothing as healthy.

    The watchdog page is the positive half, and it is what makes the silence evidence.
    An assertion that no ping went out is satisfied just as well by a hook that never
    ran at all, so on its own it would prove nothing. The page proves the cycle reached
    the hook, the errors reached the counters, and the dead-man then declined to feed.
    """
    rig = _rig(tmp_path)

    # ``nothing_to_capture`` is left at its default, which is the value a cycle over a
    # non-empty roster computes for itself in ``_CaptureCycle.run``. The errors are the
    # writes that failed, the way that cycle reports a segment it could not journal.
    def runner(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        return CycleResult(
            clock.now().replace(second=0, microsecond=0),
            (),
            errors=(SegmentError(journal.QUOTES_SURFACE, "XYZ", "disk_error"),),
        )

    clock = ManualClock(start=et(2026, 9, 2, 11, 58, 30))
    # Three session minutes is the watchdog's page threshold, so the run is long enough
    # for the surface to report itself down.
    _run(rig, clock, ticks=3, cycle_runner=runner)

    assert rig.pinger.urls == []
    (page,) = rig.transport.sent
    assert page.title == "Capture down: XYZ quotes"
    assert page.body == "3 session minutes without a durable cycle, failing with disk_error"


# -- 5. the per-tick hook reaches the close+5 guard ----------------------------------


class _FailsTheClose:
    """A cycle runner whose 16:00 write fails, the one case the guard's tick path marks.

    Every other minute records a row, standing for a cycle that landed. The 16:00 cycle
    returns a ``SegmentError`` and no row, the way a live cycle reports a segment it could
    not journal. Nothing marks that minute later, because the loop was alive and on time,
    so the guard's marker is the only row 16:00 can get.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        if slot == et(2026, 9, 2, 16, 0):
            return CycleResult(
                slot, (), errors=(SegmentError(journal.QUOTES_SURFACE, "XYZ", "disk_error"),)
            )
        _record(self._root, journal.QUOTES_SURFACE, "XYZ", slot)
        return CycleResult(slot, ())


def _in_scope_all_day(rig: _Rig) -> None:
    """Register XYZ and open its span at the day's open, so the guard owes its close."""
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))


def _at_close(root: Path) -> list[dict]:
    """Every quotes row XYZ holds at 16:00, of every kind and class.

    Compared as instants, because a row's ``snap_ts`` text carries whichever offset its
    writer was handed.
    """
    close = et(2026, 9, 2, 16, 0)
    return [
        row
        for row in _rows(root, journal.QUOTES_SURFACE, "XYZ", DAY)
        if datetime.fromisoformat(row["snap_ts"]) == close
    ]


# From 15:59:30 the first tick is 16:00 and the twenty-first is 16:20, which is close+5.
TO_CLOSE_PLUS_FIVE = 21


def test_a_daemon_alive_across_close_plus_five_runs_the_guard_that_minute(tmp_path):
    """The guard has to fire on the ordinary day, not only after a restart.

    launchd's calendar intervals are fixed wall clock and cannot express a
    close-relative moment, so every session-relative job is dispatched from inside the
    loop. A daemon still running at close+5 is the common case. An unbound tick hook
    leaves the guard to the startup check alone, so the equity close goes unwitnessed
    on every day the daemon does not happen to restart after it.

    The daemon here is alive across 16:00 and its 16:00 write failed, so no row records
    the close when the guard runs. Startup marking reaches only the minute the daemon
    started in, and the loop skipped nothing. Every row at 16:00 is counted, whatever its
    class, so a second row from any writer fails this.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    at_start: list[dict] = []
    hooks = daemon.DaemonHooks(
        on_start=lambda slot: at_start.extend(
            _rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY)
        )
    )
    clock = ManualClock(start=et(2026, 9, 2, 15, 59, 30))
    _run(
        rig,
        clock,
        ticks=TO_CLOSE_PLUS_FIVE,
        cycle_runner=_FailsTheClose(rig.lake_root),
        hooks=hooks,
    )

    unobserved = [row["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED for row in at_start]
    assert not any(unobserved), "the startup check ran the guard before close+5"
    (marked,) = _at_close(rig.lake_root)
    assert marked["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
    assert marked["close_tag"] == SPOT_CLOSE
    # The guard's run is what wrote it, at close+5 and not before.
    files = sorted(report.close_guard_dir(rig.lake_root, DAY).glob("*.json"))
    assert [json.loads(path.read_text())["at"][:16] for path in files] == ["2026-09-02T16:20"]


# The five orderings that reach 16:00 with another writer, and the row each leaves there.
# A second row for one minute double-counts it in every per-slot completeness read, so
# each asserts one row at the close, counted without filtering by class.


def _guard_found(root: Path) -> list[list[str]]:
    """The ``unobserved`` field of every close+5 report the day's runs filed."""
    return [
        json.loads(path.read_text())["unobserved"]
        for path in sorted(report.close_guard_dir(root, DAY).glob("*.json"))
    ]


@pytest.mark.parametrize(
    ("start", "ticks", "at_close", "reports"),
    [
        # Dead from 15:58 and relaunched at 16:10. Startup marking records 16:00, and the
        # guard at 16:20 finds it recorded.
        pytest.param(
            et(2026, 9, 2, 16, 10, 30), 10, gap.DAEMON_DEAD, [["XYZ"]], id="restart-at-16:10"
        ),
        # Relaunched past close+5. The guard runs from ``on_start``, ahead of startup
        # marking, and startup marking then counts its row.
        pytest.param(
            et(2026, 9, 2, 16, 25, 30),
            1,
            close_guard.SPOT_CLOSE_UNOBSERVED,
            [["XYZ"]],
            id="restart-at-16:25",
        ),
        # Dead across the evening. That day's guard never runs, so startup marking's row
        # is the one the next morning leaves.
        pytest.param(et(2026, 9, 3, 9, 29, 30), 1, gap.DAEMON_DEAD, [], id="overnight"),
    ],
)
def test_a_restart_leaves_one_row_at_the_equity_close(tmp_path, start, ticks, at_close, reports):
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 15, 58))
    clock = ManualClock(start=start)
    _run(rig, clock, ticks=ticks, cycle_runner=_FailsTheClose(rig.lake_root))

    (row,) = _at_close(rig.lake_root)
    assert row["error_class"] == at_close
    assert _guard_found(rig.lake_root) == reports


# The cycle a stall starts after unless a test names another, the last before the close.
BEFORE_THE_CLOSE = et(2026, 9, 2, 15, 55)


class _Stalls:
    """A cycle runner whose cycle at ``at`` lands and then sleeps for ``seconds``.

    It stands for a lid closed after that cycle, 15:55 unless a test says otherwise. The
    loop wakes at the next minute top past the stall, and the slots in between reach
    ``on_skipped`` on that tick.
    """

    def __init__(
        self,
        root: Path,
        clock: ManualClock,
        seconds: float,
        at: datetime = BEFORE_THE_CLOSE,
    ) -> None:
        self._root = root
        self._clock = clock
        self._seconds = seconds
        self._at = at

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        _record(self._root, journal.QUOTES_SURFACE, "XYZ", slot)
        if slot == self._at:
            self._clock.advance(self._seconds)
        return CycleResult(slot, ())


# Each stall runs from the 15:55 cycle to half a minute before the tick it wakes on.
STALLS = [
    # Wakes at 16:10, before close+5. The waking tick marks 16:00, and the guard at 16:20
    # finds it recorded.
    pytest.param(14 * 60 + 30, 12, id="stall-to-16:10"),
    # Wakes at 16:25, past close+5. The guard is owed on the waking tick, and it runs
    # after that tick's overrun markers rather than before them.
    pytest.param(29 * 60 + 30, 2, id="stall-to-16:25"),
    # Wakes in the evening, the same order at a later hour.
    pytest.param(3 * 3600 + 4 * 60 + 30, 2, id="stall-to-19:00"),
]


@pytest.mark.parametrize(("seconds", "ticks"), STALLS)
def test_a_stall_across_the_close_leaves_one_row_there(tmp_path, seconds, ticks):
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    clock = ManualClock(start=et(2026, 9, 2, 15, 54, 30))
    _run(rig, clock, ticks=ticks, cycle_runner=_Stalls(rig.lake_root, clock, seconds))

    (row,) = _at_close(rig.lake_root)
    assert row["error_class"] == gap.SLOT_OVERRUN
    assert _guard_found(rig.lake_root) == [["XYZ"]]


@pytest.mark.parametrize(
    ("start", "stalled_at", "seconds", "ran_before_the_markers"),
    [
        # The stall skipped 16:00, so the guard waits for that tick's markers.
        pytest.param(
            et(2026, 9, 2, 15, 54, 30), et(2026, 9, 2, 15, 55), 29 * 60 + 30, False, id="crossed"
        ),
        # The stall skipped 16:11 to 16:15 only. 16:00 already holds its row, so waiting
        # would only put the page and the lake-root lock in front of the option-close fill.
        pytest.param(
            et(2026, 9, 2, 16, 9, 30), et(2026, 9, 2, 16, 10), 9 * 60 + 30, True, id="after-16:00"
        ),
    ],
)
def test_the_guard_waits_for_the_markers_only_when_the_stall_skipped_the_close(
    tmp_path, start, stalled_at, seconds, ran_before_the_markers
):
    """The wait is as wide as its reason, a stall that skipped the equity close."""
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    at_skipped: list[list[list[str]]] = []
    clock = ManualClock(start=start)
    _run(
        rig,
        clock,
        ticks=2,
        cycle_runner=_Stalls(rig.lake_root, clock, seconds, at=stalled_at),
        hooks=daemon.DaemonHooks(
            on_skipped=lambda slots: at_skipped.append(_guard_found(rig.lake_root))
        ),
    )

    assert at_skipped == ([[["XYZ"]]] if ran_before_the_markers else [[]])
    assert _guard_found(rig.lake_root) == [["XYZ"]]
    assert len(_at_close(rig.lake_root)) == 1


def test_a_stall_into_the_next_days_evening_leaves_one_row_at_that_days_close(tmp_path):
    """The equity close a stall skipped can be the second day's, not the first's.

    The lid closes after Wednesday's 16:10 cycle, past Wednesday's close, and opens on
    Thursday at 16:25. The waking tick owes Thursday's guard and marks Thursday's 16:00 as
    ``slot_overrun``, so the guard has to wait for that day's markers too.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    clock = ManualClock(start=et(2026, 9, 2, 16, 9, 30))
    stall = et(2026, 9, 3, 16, 24, 30) - et(2026, 9, 2, 16, 10)
    _run(
        rig,
        clock,
        ticks=2,
        cycle_runner=_Stalls(
            rig.lake_root, clock, stall.total_seconds(), at=et(2026, 9, 2, 16, 10)
        ),
    )

    close = et(2026, 9, 3, 16, 0)
    (row,) = [
        row
        for row in _rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", NEXT_DAY)
        if datetime.fromisoformat(row["snap_ts"]) == close
    ]
    assert row["error_class"] == gap.SLOT_OVERRUN


def test_a_startup_that_outlives_close_plus_five_still_runs_the_guard_after_the_markers(
    tmp_path,
):
    """The guard's previous slot is the one ``run_loop`` seeded, not the first tick's.

    The daemon starts at 15:59:30 and its startup pass runs until 16:25:10, a lid closed
    during startup. The first tick then reports 16:00 to 16:15 to ``on_skipped``. A wrapper
    that began tracking only at that tick would see no skipped slots, run the guard
    first, and put its 16:00 marker in the way of the overrun markers.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 15, 58))
    clock = ManualClock(start=et(2026, 9, 2, 15, 59, 30))
    hooks = daemon.DaemonHooks(on_start=lambda slot: clock.set(et(2026, 9, 2, 16, 25, 10)))
    _run(rig, clock, ticks=1, cycle_runner=_no_cycle, hooks=hooks)

    owed = [et(2026, 9, 2, 16, 0) + TICK * minute for minute in range(16)]
    found = Counter(
        (datetime.fromisoformat(row["snap_ts"]), row["error_class"])
        for row in _rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY)
        if datetime.fromisoformat(row["snap_ts"]) >= owed[0]
    )
    assert found == Counter((slot, gap.SLOT_OVERRUN) for slot in owed)
    assert _guard_found(rig.lake_root) == [["XYZ"]]


def test_a_wake_past_close_plus_five_that_skipped_no_capture_slot_runs_the_guard_that_tick(
    tmp_path,
):
    """Nothing waits when no marker is owed, so the one tick the daemon is awake serves.

    The lid closes after the 16:15 cycle and opens at 16:25. No capture slot lies in
    between, so ``on_skipped`` never fires, and a guard waiting for it would not run at
    all if the machine slept again after that one tick.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    clock = ManualClock(start=et(2026, 9, 2, 16, 14, 30))
    stall = _Stalls(rig.lake_root, clock, 9 * 60 + 30, at=et(2026, 9, 2, 16, 15))
    _run(rig, clock, ticks=2, cycle_runner=stall)

    files = sorted(report.close_guard_dir(rig.lake_root, DAY).glob("*.json"))
    assert [json.loads(path.read_text())["at"][:16] for path in files] == ["2026-09-02T16:25"]


def test_a_raise_in_the_waking_ticks_skipped_hook_does_not_cost_the_guard_its_run(tmp_path):
    """The deferred run sits in a ``finally``, so a raise inside ``on_skipped`` still runs it.

    Before the deferral the guard had already run by the time the skipped-slot hook could
    raise. A caller's own hook runs inside the marker's, and the marker re-raises a roster
    that will not load, so either can raise on the waking tick. The run still raises, and
    the guard's report is on disk beside it.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)

    def refuse(slots: list[datetime]) -> None:
        raise RuntimeError("the caller's skipped-slot hook failed")

    clock = ManualClock(start=et(2026, 9, 2, 15, 54, 30))
    with pytest.raises(RuntimeError, match="skipped-slot hook failed"):
        _run(
            rig,
            clock,
            ticks=2,
            cycle_runner=_Stalls(rig.lake_root, clock, 29 * 60 + 30),
            hooks=daemon.DaemonHooks(on_skipped=refuse),
        )

    assert _guard_found(rig.lake_root) == [["XYZ"]], "the raise cost the guard its run"
    (row,) = _at_close(rig.lake_root)
    assert row["error_class"] == gap.SLOT_OVERRUN


# -- 6. the cycle runner re-reads the chain plan ------------------------------------


class _PlanVendor:
    """A vendor that records the date window of every chain request.

    Each chain fetch is refused with a 500. That is a non-size failure, which the
    chunker records once and never splits, so the requests are exactly the plan's
    windows and nothing else.
    """

    def __init__(self) -> None:
        self.windows: list[tuple[date | None, date | None]] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self.windows.append((from_date, to_date))
        return VendorResponse(status=500, body={})

    def get_quotes(self, symbols):
        return VendorResponse(status=200, body=QUOTE_BODY)

    def token_mint_time(self):
        return et(2026, 9, 1, 9, 0)


def _stub_schwab(vendor: _PlanVendor):
    """A stand-in for ``SchwabVendor`` whose ``from_token`` hands back ``vendor``.

    The production entry builds its vendor from the token file on every cycle. A test
    has no token, and reaching for one would put the real client in the path.
    """

    class _Stub:
        @staticmethod
        def from_token(path, *, api_key, app_secret, clock=None):
            return vendor

    return _Stub


def test_a_rewritten_chain_plan_takes_effect_on_the_next_cycle(tmp_path, monkeypatch):
    """The nightly plan rewrite is only worth writing if the daemon re-reads it.

    The daemon holds no expiration state and no cached plan. Its cycle runner reloads
    the config, the roster, the token, and the chain plan on every call, and that
    re-read is the whole reason a rewritten plan needs no restart. Binding the loop to
    anything else strands a machine on the plan it started with.
    """
    rig = _rig(tmp_path, roster=WITH_OPTIONS)
    plan_path = tmp_path / "chain_plan.json"
    write_chain_plan(ONE_WINDOW, plan_path)
    # The plan file is machine-local, so the test owns its path rather than the home
    # directory's. The loader itself stays the real one, read once per cycle.
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(plan_path))
    vendor = _PlanVendor()
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(vendor))

    cycles: list[datetime] = []
    first_cycle: list[tuple[date | None, date | None]] = []

    def rewrite_the_plan(slot: datetime, result: CycleResult) -> None:
        """Stand in for the nightly retune, which rewrites the plan between sessions."""
        cycles.append(slot)
        if len(cycles) == 1:
            first_cycle.extend(vendor.windows)
            write_chain_plan(SPLIT_WINDOW, plan_path)

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=2, hooks=daemon.DaemonHooks(on_cycle=rewrite_the_plan))

    assert len(cycles) == 2
    # The 10:00 cycle fetched the single open window the first plan named.
    assert first_cycle == [(DAY, None)]
    # The 10:01 cycle fetched the pair the rewrite left behind, which is a set of ranges
    # no plan read before the rewrite could have produced.
    # Fired concurrently (#532), so the two ranges reach the vendor in either order.
    assert Counter(vendor.windows[1:]) == Counter([(DAY, DAY), (NEXT_DAY, None)])


def test_the_production_runner_files_the_cycle_under_the_loops_slot(tmp_path, monkeypatch):
    """The real runner hands the loop's slot to the cycle, which files under it in UTC.

    The 16:00 tick's hooks take 61 seconds here, so a cycle reading the clock for its own
    minute would land in 16:01 and carry a close tag decided for 16:00 (marketlake #572).
    This goes through ``run_a_cycle``, the closure the daemon really runs, because the
    capture entry keeps the slot optional for the slice-1 runner. A closure that stopped
    passing it would fall back to the clock with every direct ``run_cycle`` test still
    passing.

    The clock here reads Eastern time, so a cycle that kept the loop's Eastern slot as it
    came would spell ``snap_ts`` with ``-04:00``. Every row a loop cycle writes is stored in
    UTC, and
    the text is what a row carries, so the text is asserted rather than the instant.
    """
    rig = _rig(tmp_path, roster=WITH_OPTIONS)
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(_PlanVendor()))
    clock = ManualClock(start=et(2026, 9, 2, 15, 59, 30))
    close = et(2026, 9, 2, 16, 0)
    cycles: list[tuple[datetime, CycleResult]] = []

    def slow_close_tick(slot: datetime) -> None:
        if slot == close:
            clock.advance(61)

    _run(
        rig,
        clock,
        ticks=1,
        hooks=daemon.DaemonHooks(
            on_tick=slow_close_tick,
            on_cycle=lambda slot, result: cycles.append((slot, result)),
        ),
    )

    ((slot, result),) = cycles
    assert slot == close
    assert clock.now() > et(2026, 9, 2, 16, 1)
    rows = [
        row for segment in result.segments for row in journal.read_segment(segment.path).to_pylist()
    ]
    assert {segment.surface for segment in result.segments} == {
        journal.CHAINS_SURFACE,
        journal.QUOTES_SURFACE,
    }
    assert {row["snap_ts"] for row in rows} == {"2026-09-02T20:00:00+00:00"}
    assert {row["close_tag"] for row in rows} == {"spot_close"}


# -- 7. the skipped-slot hook charges what the roster names --------------------------


def _rewrite_after_first_cycle(path: Path, roster: str) -> daemon.DaemonHooks:
    """Hooks that replace the roster file once, after the first cycle.

    This stands for a person editing ``tickers.yaml`` while the daemon runs. The first
    cycle is the session up to that edit. The overrun after it is what hands the fresh
    file to the skipped-slot hook.
    """
    cycles: list[datetime] = []

    def on_cycle(slot: datetime, result: CycleResult) -> None:
        cycles.append(slot)
        if len(cycles) == 1:
            path.write_text(roster)

    return daemon.DaemonHooks(on_cycle=on_cycle)


def _seed_two(rig: _Rig) -> None:
    """Put one recorded row on each ticker, so startup marking stops at the start minute.

    No assertion reads these rows. Without them the walk-back reaches its cap and prints
    a truncation line to stderr on every run.
    """
    for ticker in ("XYZ", "ABC"):
        _record(rig.lake_root, journal.QUOTES_SURFACE, ticker, et(2026, 9, 2, 9, 58))


def _run_across_an_edit(rig: _Rig, roster: str) -> None:
    """Run one cycle, rewrite the roster to ``roster``, then overrun three slots.

    The 10:00 cycle takes 200 seconds, so the loop next wakes at 10:04 and hands 10:01,
    10:02, and 10:03 to the skipped-slot hook. Three slept-through slots is the design's
    page threshold. So the stretch raises one page, and that page counts the surfaces
    the hook charged. The cycles produce no segment of their own, so nothing but the
    missed minutes charges a counter.
    """
    _seed_two(rig)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(
        rig,
        clock,
        ticks=2,
        cycle_runner=_Overrunning(clock, 200),
        hooks=_rewrite_after_first_cycle(rig.tickers, roster),
    )


def test_a_ticker_retired_mid_session_stops_charging_the_watchdog(tmp_path):
    """A ticker taken off the roster has to stop paging without a restart.

    The design has the daemon re-read ``tickers.yaml`` every capture cycle, and it names
    the per-ticker watchdog counters among the consumers reading that same snapshot.
    Closing the skipped-slot hook over the startup roster breaks the rule in the one
    place the cycle runner cannot cover, because the loop runs no cycle for a slot it
    slept through. A retired ticker would then page from a surface nobody is capturing,
    and only a restart would stop it.
    """
    rig = _rig(tmp_path, roster=TWO_TICKERS)
    _run_across_an_edit(rig, ONE_RETIRED)

    # One counter charged, ABC's, so the page carries no fold count. A hook closed over
    # the roster the daemon started with would have charged two and said so.
    (page,) = rig.transport.sent
    assert page.title == "Capture down: loop overran"
    assert page.body == "3 session minutes without a durable cycle"


def test_a_ticker_onboarded_mid_session_starts_charging_the_watchdog(tmp_path):
    """A ticker added to the roster has to start paging without a restart.

    This is the retirement rule above run the other way, and the design states it as one:
    onboarding writes the `tickers.yaml` entry itself, and a new ticker goes live
    everywhere on the next cycle. The counters are named among the consumers of that
    snapshot. A hook closed over the startup roster charges DEF never, so a chain worker
    that dies the hour after an onboarding is invisible until someone restarts the
    daemon.
    """
    rig = _rig(tmp_path, roster=TWO_TICKERS)
    _run_across_an_edit(rig, THREE_TICKERS)

    # Three counters charged, so the hook read the file the edit left rather than the
    # two-ticker roster the daemon started with.
    (page,) = rig.transport.sent
    assert page.title == "Capture down: loop overran"
    assert page.body == "3 session minutes without a durable cycle, one page for 3 surfaces"


@pytest.mark.parametrize("roster", [UNLOADABLE, MID_LINE_TEAR], ids=["refused", "torn"])
def test_a_roster_that_will_not_load_takes_the_daemon_down(tmp_path, roster):
    """A roster the loader refuses is fatal here, and it arrives as one error type.

    The hook has no fallback to reach for. A roster is the only thing it can charge
    against, and the only honest source of one is the file, so carrying a stale read
    forward would charge counters the roster no longer names. The alternative failure is
    silent and the design would rather be loud: `_alarm` refuses the same file at
    startup, and the cycle runner reads it again on this same tick and raises too.

    Both shapes must arrive as `TickersError`, because that is what `main` turns into
    one named line and exit 2. `UNLOADABLE` is valid YAML the loader refuses on shape.
    `MID_LINE_TEAR` is a half-saved file `yaml` itself refuses with a `ParserError`, and
    it used to escape past every caller guarding for a bad roster.
    """
    rig = _rig(tmp_path, roster=TWO_TICKERS)

    with pytest.raises(TickersError) as caught:
        _run_across_an_edit(rig, roster)

    assert str(rig.tickers) in str(caught.value)
    # Nothing paged. The counters never got a roster to charge against.
    assert rig.transport.sent == []


def test_a_broken_roster_off_the_capture_window_is_fatal_too(tmp_path):
    """The one tick the deleted fallback used to carry, kept so the cost stays visible.

    `run_loop` hands missed slots to the skipped-slot hook and only then checks the
    phase, so a tick off the capture window fires the hook and runs no cycle. That is
    the only tick where this hook's read is the roster's only read of the minute, and it
    is reachable: a stall spanning the option close wakes past it with the window's tail
    still to report.

    The fallback that used to sit here carried the daemon from that tick to the next
    capture minute, which across a Friday close is the whole weekend. Deleting it moves
    the exit forward to here. That is the price, and it is paid deliberately: a stale
    roster charges counters the file no longer names, and the dead-man switch is what
    reports a daemon that stopped.
    """
    rig = _rig(tmp_path, roster=TWO_TICKERS)
    _seed_two(rig)
    clock = ManualClock(start=et(2026, 9, 2, 15, 58, 30))

    def stall_across_the_close(
        *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        rig.tickers.write_text(UNLOADABLE)
        clock.advance(ACROSS_THE_CLOSE)
        return CycleResult(snap_ts=slot, segments=())

    with pytest.raises(TickersError):
        _run(rig, clock, ticks=2, cycle_runner=stall_across_the_close)

    # The waking tick is past the option close, so no cycle ran to raise first. The hook
    # is what took the daemon down.
    assert clock.now() > et(2026, 9, 2, 16, 15)
    assert rig.transport.sent == []


def test_the_hook_charges_every_surface_its_ticker_is_captured_on(tmp_path):
    """An options ticker's chains counter has to be charged beside its quotes.

    The counters are per surface, so a dead chain worker pages while that ticker's
    quotes still flow. The hook expands each roster entry through ``surfaces_for``, the
    same rule capture plans a cycle with. Charging quotes alone would leave a chain
    worker that died across an overrun invisible, on the one path where no cycle runs
    to notice it.
    """
    rig = _rig(tmp_path, roster=WITH_OPTIONS)
    for surface in (journal.CHAINS_SURFACE, journal.QUOTES_SURFACE):
        _record(rig.lake_root, surface, "SPY", et(2026, 9, 2, 9, 58))
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=2, cycle_runner=_Overrunning(clock, 200))

    # Two counters on one ticker, so charging quotes alone would have said one surface.
    (page,) = rig.transport.sent
    assert page.title == "Capture down: loop overran"
    assert page.body == "3 session minutes without a durable cycle, one page for 2 surfaces"


# -- 8. a mid-session recalibration reaches the watchdog ----------------------------


# The threshold starts high, above anything this run reaches, then drops mid-session.
# Both the start value and the value the counter trips at are non-default, so neither is
# the pinned default a frozen threshold would fall back to.
START_PAGE_MINUTES = 8
RECALIBRATED_PAGE_MINUTES = 4


class _FailingCycles:
    """A cycle runner whose every cycle fails one surface, and which rewrites the config
    once, at a chosen call.

    Each call returns a gap for XYZ quotes, so the watchdog charges that counter every
    minute. On the ``recalibrate_at`` call it rewrites ``config.yaml`` first, so the edit
    is on disk before the watchdog reads the threshold for that minute.
    """

    def __init__(
        self, rig: _Rig, clock: ManualClock, *, recalibrate_at: int, new_page_minutes: int
    ):
        self._rig = rig
        self._clock = clock
        self._recalibrate_at = recalibrate_at
        self._new_page_minutes = new_page_minutes
        self.calls = 0

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        self.calls += 1
        if self.calls == self._recalibrate_at:
            write_config(
                self._rig.config.parent,
                self._rig.lake_root,
                guards={"watchdog_page_minutes": self._new_page_minutes},
            )
        return CycleResult(
            snap_ts=slot, segments=(_segment(journal.ROW_KIND_GAP, self._rig.lake_root),)
        )


def test_a_mid_session_recalibration_reaches_the_watchdog(tmp_path):
    """A recalibrated ``watchdog_page_minutes`` has to take effect without a restart.

    The daemon reads the threshold off the config each time the watchdog decides to page,
    so a hand edit mid-session moves the minute a surface pages on. Baking the value in at
    loop start, which is what the daemon did before, would hold the old number until a
    restart, and no other case here drives a config edit to catch it.

    The threshold starts at eight and a surface fails every minute. Two failing minutes
    raise nothing. The config is then rewritten to four. The counter keeps climbing from
    where it stood, so the fourth failing minute is the one that trips it, at the new
    number.
    """
    assert START_PAGE_MINUTES != RECALIBRATED_PAGE_MINUTES
    assert RECALIBRATED_PAGE_MINUTES != GuardConstants().watchdog_page_minutes
    rig = _rig(tmp_path, guards={"watchdog_page_minutes": START_PAGE_MINUTES})
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(
        rig,
        clock,
        ticks=4,
        cycle_runner=_FailingCycles(
            rig, clock, recalibrate_at=3, new_page_minutes=RECALIBRATED_PAGE_MINUTES
        ),
    )

    (page,) = rig.transport.sent
    assert page.event == "capture_down"
    assert page.title == "Capture down: XYZ quotes"
    assert page.body == (
        f"{RECALIBRATED_PAGE_MINUTES} session minutes without a durable cycle, failing with boom"
    )


# -- 9. an empty roster keeps the daemon running ---------------------------------------


class _RateLimited:
    """A cycle runner whose every cycle gaps one surface with a rate-limit class."""

    def __init__(self, rig: _Rig, clock: ManualClock):
        self._rig = rig
        self._clock = clock

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        segment = SegmentOutcome(
            surface=journal.QUOTES_SURFACE,
            ticker="XYZ",
            path=self._rig.lake_root / "segment.arrows",
            partition="quotes/ticker=XYZ/date=2026-09-02/segment.arrows",
            row_kind=journal.ROW_KIND_GAP,
            rows=1,
            error_class="http_429",
            fetched_at=None,
            data_rows=0,
        )
        return CycleResult(snap_ts=slot, segments=(segment,))


def test_a_surface_page_names_the_class_it_is_failing_with(tmp_path):
    """The title says what went quiet, and the body has to say why.

    A rate limit that starves one ticker while another still lands rows is not a
    whole-daemon cause, so the page that goes out names the ticker. Without the class in
    the body, that page sends the operator to look at one dead surface when the budget is
    what is failing. The watchdog has held the class all along and dropped it here.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=4, cycle_runner=_RateLimited(rig, clock))

    (page,) = rig.transport.sent
    assert page.event == "capture_down"
    assert page.title == "Capture down: XYZ quotes"
    assert page.body == "3 session minutes without a durable cycle, failing with http_429"


class _WholeDaemonFailure:
    """A cycle runner whose every cycle fails both surfaces with one auth class."""

    def __init__(self, rig: _Rig, clock: ManualClock):
        self._rig = rig
        self._clock = clock

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        segments = tuple(
            SegmentOutcome(
                surface=surface,
                ticker="XYZ",
                path=self._rig.lake_root / "segment.arrows",
                partition=f"{surface}/ticker=XYZ/date=2026-09-02/segment.arrows",
                row_kind=journal.ROW_KIND_GAP,
                rows=1,
                error_class="http_401",
                fetched_at=None,
                data_rows=0,
            )
            for surface in (journal.QUOTES_SURFACE, "chains")
        )
        return CycleResult(snap_ts=slot, segments=segments)


def test_the_cause_page_names_its_class_on_the_wire_too(tmp_path):
    """The page that names a whole-daemon cause goes out through the same composer.

    A dead token arrives as ``http_401`` while the cached access token still works and as
    ``vendor_auth_error`` once the refresh fails. Both carry the one title, so the class
    in the body is what says which shape arrived. Only a single-surface page held the
    class before this, so a body composer that skipped multi-surface pages passed.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=4, cycle_runner=_WholeDaemonFailure(rig, clock))

    (page,) = rig.transport.sent
    assert page.event == "capture_down"
    assert page.title == "Capture down: token dead"
    # Two surfaces of one ticker, folded into this page. It counts surfaces rather than
    # tickers, because a cause takes both surfaces of every ticker down together.
    assert page.body == (
        "3 session minutes without a durable cycle, failing with http_401, one page for 2 surfaces"
    )


class _DeadSampler:
    """A cycle runner that gaps every quotes ticker in the batch, every cycle.

    The class is one no whole-daemon cause names, so the cycle reaches the sampler
    collapse rather than being reported as a cause.
    """

    def __init__(self, rig: _Rig, clock: ManualClock, tickers: tuple[str, ...]):
        self._rig = rig
        self._clock = clock
        self._tickers = tickers

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        segments = tuple(
            SegmentOutcome(
                surface=journal.QUOTES_SURFACE,
                ticker=ticker,
                path=self._rig.lake_root / "segment.arrows",
                partition=f"quotes/ticker={ticker}/date=2026-09-02/segment.arrows",
                row_kind=journal.ROW_KIND_GAP,
                rows=1,
                error_class="boom",
                fetched_at=None,
                data_rows=0,
            )
            for ticker in self._tickers
        )
        return CycleResult(snap_ts=slot, segments=segments)


@pytest.mark.parametrize("size", [2, 3, 7])
def test_the_sampler_page_says_how_many_tickers_it_stands_for(size, tmp_path):
    """A folded page has to say how much it folded.

    The design folds every quotes ticker into one page rather than sending N, and the
    same rule makes compaction's drift page name how many columns it left. Without the
    count, one page for two tickers and one page for four hundred read identically, and
    the quotes batch runs to hundreds of symbols per request.

    The count comes off the page's own surfaces rather than a constant, so a roster that
    changed mid-session reports what it is now.
    """
    tickers = tuple(f"T{index:02d}" for index in range(size))
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=4, cycle_runner=_DeadSampler(rig, clock, tickers))

    (page,) = rig.transport.sent
    assert page.title == "Capture down: quote sampler dead"
    assert page.body == (
        f"3 session minutes without a durable cycle, failing with boom, one page for {size} tickers"
    )


class _SplitSampler:
    """A cycle runner that gaps every quotes ticker, half of them with a second class."""

    def __init__(self, rig: _Rig, clock: ManualClock, tickers: tuple[str, ...]):
        self._rig = rig
        self._clock = clock
        self._tickers = tickers

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        segments = tuple(
            SegmentOutcome(
                surface=journal.QUOTES_SURFACE,
                ticker=ticker,
                path=self._rig.lake_root / "segment.arrows",
                partition=f"quotes/ticker={ticker}/date=2026-09-02/segment.arrows",
                row_kind=journal.ROW_KIND_GAP,
                rows=1,
                error_class="boom" if index % 2 else "timeout",
                fetched_at=None,
                data_rows=0,
            )
            for index, ticker in enumerate(self._tickers)
        )
        return CycleResult(snap_ts=slot, segments=segments)


def test_a_folded_page_whose_tickers_disagree_still_says_how_many(tmp_path):
    """The emptiest page the system can send is the one that most needs the count.

    A collapsed page names no class when its tickers report more than one, because there
    is no single class to honestly name. The count is then the only thing the body has
    left to say beyond the minutes, so it has to survive the class being absent.
    """
    tickers = ("T00", "T01", "T02", "T03")
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=4, cycle_runner=_SplitSampler(rig, clock, tickers))

    (page,) = rig.transport.sent
    assert page.title == "Capture down: quote sampler dead"
    assert page.body == "3 session minutes without a durable cycle, one page for 4 tickers"


def test_an_empty_roster_still_runs_the_loop_and_reports(tmp_path):
    """Retiring every ticker must not stop the daemon, only its capturing.

    An empty ``tickers.yaml`` used to be refused outright. Now the capture-spans file is
    the record of scope, and a fully retired roster is a real, supported state: nothing
    is captured, but the healthcheck ping, the dead-man feed, and the watchdog keep
    running, because those report on the daemon's own health, not on any ticker's.
    """
    rig = _rig(tmp_path, roster="")
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    calls = [0]

    def cycle(*, slot, close_tag, session_phase):
        calls[0] += 1
        # The real cycle runner stamps this when the roster it read was empty; the fake
        # here reproduces that, since ``capture.py``'s own tests cover the stamping.
        return CycleResult(clock.now(), (), nothing_to_capture=True)

    _run(rig, clock, ticks=3, cycle_runner=cycle)

    # The cycle runner still fires on every capture tick, over zero tickers.
    assert calls[0] == 3
    # The healthcheck ping still reaches the transport.
    assert rig.pinger.urls == [CAPTURE_URL] * 3
    # No page fired, since there is nothing to charge or watch.
    assert rig.transport.sent == []


def _xyz_span_closed(rig: _Rig) -> None:
    """A master and spans file naming XYZ, its one span closed before the session.

    Valid files, the kind ``retire`` and ``onboard`` write. A directory or a foreign parquet
    would stop narrowing once marketlake #551 lands, so neither is used.
    """
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 1, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 1, 9, 30), False)
    spans.close_span(xyz, et(2026, 9, 1, 16, 0))
    spans.write(spans_path(rig.lake_root))


def _capture_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("capture: ")]


def test_a_roster_the_spans_emptied_leaves_the_dead_man_unfed_and_says_why(
    tmp_path, monkeypatch, capsys
):
    """An enabled ticker outside every span owes its minutes, so nothing may say it is fine.

    Through the real cycle runner. XYZ is enabled, and the spans leave it out, the state a
    retire or an onboard leaves when it stops midway (marketlake #554). No request goes
    out, no segment is written, and the watchdog is charged nothing, so the dead-man's
    silence is the only thing that pages. Before this fix the cycle reported
    ``nothing_to_capture`` and fed it every minute, so nothing paged at all.

    The line is what names the cause, since healthchecks' page does not. It prints once,
    not once a minute.
    """
    rig = _rig(tmp_path)
    _xyz_span_closed(rig)
    vendor = _PlanVendor()
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(vendor))
    seen: list[CycleResult] = []

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=3, hooks=daemon.DaemonHooks(on_cycle=lambda s, r: seen.append(r)))

    assert len(seen) == 3
    assert all(r.segments == () and r.out_of_span == ("XYZ",) for r in seen)
    assert rig.pinger.urls == []
    # Three cycles is the watchdog's threshold, and still nothing but the dead-man pages
    # this case (marketlake #570).
    assert rig.transport.sent == []
    (line,) = _capture_lines(capsys.readouterr().err)
    assert line.startswith("capture: 2026-09-02T10:00:00-04:00: 1 enabled ticker(s)")
    assert "not captured: XYZ." in line


def test_the_line_names_a_partial_clamp_and_its_recovery_while_data_lands(
    tmp_path, monkeypatch, capsys
):
    """The line reports every change the loop sees, not only an unfed dead-man.

    SPY lands data every minute, since the master does not name it and a ticker the master
    cannot resolve is kept. XYZ starts outside every span, and its span reopens after the
    first cycle. The line names XYZ on the first minute and says so on the second, both
    in cycles that fed the dead-man. A line gated on an unfed dead-man, or on an empty
    cycle, would print neither.
    """
    rig = _rig(tmp_path, roster="SPY: {options: false}\nXYZ: {options: false}\n")
    _xyz_span_closed(rig)
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(_PlanVendor()))

    def reopen_after_first(slot: datetime, result: CycleResult) -> None:
        spans = CaptureSpans.read(spans_path(rig.lake_root))
        (xyz,) = spans.instrument_ids()
        if not spans.has_open_span(xyz):
            spans.open_span(xyz, et(2026, 9, 2, 10, 0, 30), False)
            spans.write(spans_path(rig.lake_root))

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=3, hooks=daemon.DaemonHooks(on_cycle=reopen_after_first))

    assert rig.pinger.urls == [CAPTURE_URL] * 3
    named, recovered = _capture_lines(capsys.readouterr().err)
    assert named.startswith("capture: 2026-09-02T10:00:00-04:00: 1 enabled ticker(s)")
    assert "not captured: XYZ." in named
    assert recovered.startswith("capture: 2026-09-02T10:01:00-04:00: ")


def test_a_partial_clamp_pages_the_tickers_the_spans_leave_out(tmp_path, monkeypatch):
    """Through the real cycle runner, the case the dead-man cannot see (marketlake #570).

    SPY lands data every minute, since the master does not name it and a ticker the master
    cannot resolve is kept, so the dead-man is fed. XYZ is enabled and outside every span,
    so it owes its minutes and nothing fetches it. Three cycles is the threshold.
    """
    rig = _rig(tmp_path, roster="SPY: {options: false}\nXYZ: {options: false}\n")
    _xyz_span_closed(rig)
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(_PlanVendor()))

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=4)

    assert rig.pinger.urls == [CAPTURE_URL] * 4
    (page,) = rig.transport.sent
    assert page.event == "capture_down"
    assert page.priority == PAGE_PRIORITY
    assert page.title == "Capture down: tickers outside every capture span"
    assert page.body == (
        "1 enabled ticker(s) outside every capture span, the longest for 3 session minutes, "
        "so not captured: XYZ. A retire, onboard or rejoin that stopped midway leaves this, "
        "and so does a spans file that no longer matches the lake. The daemon log's capture: "
        "lines name each change"
    )


@pytest.mark.parametrize(
    ("left_out", "named"),
    [
        (("AAA", "BBB", "CCC", "DDD"), "AAA, BBB, CCC, DDD."),
        (("AAA", "BBB", "CCC", "DDD", "EEE", "FFF"), "AAA, BBB, CCC, DDD and 2 more."),
    ],
)
def test_the_out_of_span_page_names_four_tickers_and_counts_the_rest(tmp_path, left_out, named):
    # A spans file restored from an old backup can leave out most of the roster at once,
    # and the body has a byte budget. Exactly four names every one and counts nothing.
    rig = _rig(tmp_path)
    landed = SegmentOutcome(
        surface=journal.QUOTES_SURFACE,
        ticker="XYZ",
        path=Path("seg.arrows"),
        partition="p",
        row_kind=journal.ROW_KIND_DATA,
        rows=1,
        error_class=None,
        fetched_at=None,
        data_rows=1,
    )

    def runner(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        return CycleResult(slot, (landed,), out_of_span=left_out)

    clock = ManualClock(start=et(2026, 9, 2, 11, 58, 30))
    _run(rig, clock, ticks=3, cycle_runner=runner)

    (page,) = rig.transport.sent
    assert page.body.startswith(
        f"{len(left_out)} enabled ticker(s) outside every capture span, the longest for 3 "
        f"session minutes, so not captured: {named} "
    )


def test_a_roster_whose_every_ticker_retired_still_feeds_the_dead_man(
    tmp_path, monkeypatch, capsys
):
    """The same lake with the retire finished is idle by design, and says nothing.

    The other half of the test above, through the same runner. Without it, a dead-man
    that was never fed on an empty cycle would pass that test too.
    """
    rig = _rig(tmp_path, roster="XYZ: {options: false, enabled: false}\n")
    _xyz_span_closed(rig)
    monkeypatch.setattr(capture, "SchwabVendor", _stub_schwab(_PlanVendor()))

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    _run(rig, clock, ticks=3)

    assert rig.pinger.urls == [CAPTURE_URL] * 3
    assert _capture_lines(capsys.readouterr().err) == []


# -- 10. the tick hook reaches the close+15 compaction ---------------------------------


def _compaction_argv(rig: _Rig) -> list[str]:
    """The command the daemon must issue to run the close+15 job in its own process."""
    return [sys.executable, "-m", "lake.compact", "--config", str(rig.config)]


def test_a_daemon_alive_across_close_plus_fifteen_starts_the_compaction(tmp_path):
    """The daemon has to run its own close+15 job, or nothing does.

    ``compact.compact`` was reachable only from ``python -m lake.compact``, so an unbound
    tick hook leaves every session's segments unmerged, the lake unsynced to the backup
    drive, and the ``compaction`` check unfed until someone runs that command by hand.

    The job goes in its own process. The design gives it roughly 25 minutes and gives the
    dead-man a 5-minute grace on a heartbeat riding this same hook, so a job run inline
    would page "capture down" about a healthy daemon. The child also keeps an ``rsync``
    and a whole ticker-day of Parquet out of the resident daemon's heap.
    """
    rig = _rig(tmp_path)
    seen: list[tuple[str, int]] = []
    hooks = daemon.DaemonHooks(
        on_tick=lambda slot: seen.append((slot.strftime("%H:%M"), len(rig.compaction.calls)))
    )
    # Close+15 is 16:30, so the run ticks 16:30, 16:31 and 16:32.
    clock = ManualClock(start=et(2026, 9, 2, 16, 29, 30))
    _run(rig, clock, ticks=3, cycle_runner=_no_cycle, hooks=hooks)

    # The dispatch waits one tick past its moment, and the caller's hook runs before it
    # on its own tick, so 16:32 is the first minute that sees the count go up.
    assert seen == [("16:30", 0), ("16:31", 0), ("16:32", 1)]
    assert rig.compaction.calls == [_compaction_argv(rig)]


def test_the_job_is_not_started_before_close_plus_fifteen(tmp_path):
    """Close+15 is the number this feature is named for, so it is worth pinning.

    Close+5 has passed by 16:20 and the option close by 16:16, and a dispatch bound to
    either would look right on every case that starts after the close. This one starts
    between them, where the three moments disagree.
    """
    rig = _rig(tmp_path)
    # 16:20 is close+5 exactly. The run ticks 16:20, 16:21 and 16:22, all short of 16:30.
    clock = ManualClock(start=et(2026, 9, 2, 16, 19, 30))
    _run(rig, clock, ticks=3, cycle_runner=_no_cycle)

    assert rig.compaction.calls == []


def test_a_holiday_still_runs_the_job_so_its_check_is_fed(tmp_path):
    """A job that correctly no-ops still pings, because silence means broken, not idle.

    Close+15 does not exist without a session, so the dispatcher has no bounds to derive
    from and would skip the day outright. The design has compaction run at its regular
    wall-clock time instead, and the ``compaction`` check expects a ping every weekday.
    Skipping would take that check down on every holiday while the daemon did exactly the
    right thing, which is the false page the design's rule exists to prevent.
    """
    rig = _rig(tmp_path)
    # Thursday is declared a holiday: the week's calendar names every session but it.
    holiday = date(2026, 9, 3)
    clock = ManualClock(start=et(2026, 9, 3, 16, 30, 30))
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK, holidays=(holiday,)),
        assertion_runner=lambda args: None,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        cycle_runner=_no_cycle,
        should_continue=_stop_after(2),
    )

    assert rig.compaction.calls == [_compaction_argv(rig)]


def test_a_weekend_owes_the_job_nothing(tmp_path):
    """The check expects a ping on weekdays only, so Saturday must not start a run.

    The holiday fallback is a weekday rule, not an every-day rule. Firing here would run
    a scrub-and-sync the schedule never asked for, and it would do it on the two days the
    design leaves deliberately silent.
    """
    rig = _rig(tmp_path)
    saturday = date(2026, 9, 5)
    clock = ManualClock(start=et(2026, 9, 5, 16, 30, 30))
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        cycle_runner=_no_cycle,
        should_continue=_stop_after(2),
    )

    assert saturday.weekday() == 5
    assert rig.compaction.calls == []


def test_a_stall_into_the_next_session_does_not_seal_in_front_of_a_live_minute(tmp_path):
    """Compaction must never run inside a capture window.

    A stall that begins just past one day's close+15 and ends inside the next session
    leaves the dispatch owed on a minute the loop is capturing. Starting it there puts a
    lake lock, a seal and a full-lake ``rsync`` in front of a live fetch, which is the one
    thing every other job here is arranged not to do. The day it skips is swept by the
    next run outside the window, because the job walks every date under ``journal/``.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 16, 29, 30))
    stalled: list[int] = []

    def close_the_lid(slot: datetime) -> None:
        # One stall, on the first tick: Wednesday 16:30 to Thursday 09:30. Wednesday's
        # close+15 has passed and no later tick lands that day, so the dispatch is owed
        # on the first tick of Thursday's session.
        if not stalled:
            stalled.append(1)
            clock.advance(61200)

    _run(
        rig,
        clock,
        ticks=3,
        cycle_runner=lambda *, slot, close_tag, session_phase: CycleResult(clock.now(), ()),
        hooks=daemon.DaemonHooks(on_tick=close_the_lid),
    )

    assert rig.compaction.calls == [], "the seal ran in front of a live capture minute"


def _compacts_here(rig: _Rig, clock: ManualClock) -> Callable[[], None]:
    """Perform the compaction in-process, with the test's own seams.

    The daemon spawns a child in production, and a child cannot be started from the
    offline suite. A case about *ordering* still needs the seal to actually happen, so
    this stands in for the child and does the same work the child would: the same
    ``compact`` entry, over the same lake, with a fake backup and no ping.
    """

    def run() -> None:
        compact(
            rig.lake_root,
            clock=clock,
            calendar=weekday_sessions(WEEK),
            backup=FakeBackup(),
            backup_target=rig.lake_root.parent / "ssd",
            plan_path=rig.plan,
        )

    return run


def test_a_stall_across_the_close_marks_its_minutes_before_the_day_is_sealed(tmp_path):
    """Sealing is the one act here that cannot be taken back, so it goes last.

    ``run_loop`` calls ``on_skipped`` after ``on_tick``. A compaction dispatched from the
    tick the daemon woke on would seal the day a minute before the loop wrote the markers
    it owed for it. The next run would find the partition manifested and delete those
    markers as debris, and the day would read short with no row saying why.

    A lid closed at 16:10 and opened at 17:00 is that case, and it is an ordinary laptop
    day rather than an exotic one.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 16, 9, 30))
    rig.compaction.run_here(_compacts_here(rig, clock))
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 16, 9))
    # The 16:10 cycle runs fifty minutes, so the loop next wakes past close+15.
    _run(rig, clock, ticks=3, cycle_runner=_Overrunning(clock, 3000))

    partition = LakePaths(rig.lake_root).quotes_partition_path("XYZ", DAY)
    sealed = sorted(snap[:16] for snap in pq.read_table(partition).column("snap_ts").to_pylist())
    assert sealed == [
        "2026-09-02T16:09",
        "2026-09-02T16:11",
        "2026-09-02T16:12",
        "2026-09-02T16:13",
        "2026-09-02T16:14",
        "2026-09-02T16:15",
    ]
    assert _segments(rig.lake_root, "XYZ") == [], "a marker landed after the seal"


def test_a_restart_past_close_plus_fifteen_marks_the_day_before_sealing_it(tmp_path):
    """Startup marking is the other writer the seal has to follow.

    It runs from ``on_start``, so a dispatch added there too would seal the day before
    the walk wrote a single marker, and those minutes would be deleted as debris by the
    next run. The one-tick wait is what keeps the dispatch on the far side of it: the
    first tick after a restart has no previous slot, so nothing is dispatched until the
    walk is already done.
    """
    rig = _rig(tmp_path)
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))
    # Captured through 16:09, so 16:10 to 16:15 are the minutes the day still owes.
    for minute in range(40):
        _record(
            rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 15, 30) + TICK * minute
        )
    clock = ManualClock(start=et(2026, 9, 2, 16, 59, 30))
    rig.compaction.run_here(_compacts_here(rig, clock))
    _run(rig, clock, ticks=3, cycle_runner=_no_cycle)

    partition = LakePaths(rig.lake_root).quotes_partition_path("XYZ", DAY)
    sealed = {snap[11:16] for snap in pq.read_table(partition).column("snap_ts").to_pylist()}
    owed = {"16:10", "16:11", "16:12", "16:13", "16:14", "16:15"}
    assert owed <= sealed, "startup marking's minutes were sealed away or never written"


class _Unspawnable:
    """A ``CompactionRunner`` standing for a spawn that cannot start."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, args):
        self.calls += 1
        raise OSError("no such interpreter")


def test_a_spawn_that_fails_is_reported_and_never_takes_capture_down(tmp_path):
    """A compaction that will not start must not cost tomorrow's session.

    No hook is wrapped in a try, so a raise here exits the process, and under
    ``KeepAlive`` the successor reaches the same minute and raises again. One broken
    spawn would become a crash loop with capture dead inside it, which inverts the
    design's order of precedence. The missed ``compaction`` ping pages instead.
    """
    rig = replace(_rig(tmp_path), compaction=_Unspawnable())
    clock = ManualClock(start=et(2026, 9, 2, 16, 29, 30))
    _run(rig, clock, ticks=4, cycle_runner=_no_cycle)

    # The loop ran every tick it was given, so the raise never reached it.
    assert rig.pinger.urls == [CAPTURE_URL] * 4
    # Once, not once a minute: the dispatcher marks the day served before it runs.
    assert rig.compaction.calls == 1


# -- 11. the close+5 outcome reaches the reports tree ---------------------------------


def _tagged(root: Path, ticker: str, slot: datetime, *, close_tag: str) -> None:
    """One close-tagged gap row, standing for a cycle that ran at a close and failed.

    A tagged gap row is enough to satisfy the guard. It ran and it is recorded, so the
    guard adds nothing, which is what a day with nothing to report looks like. ``_record``
    above writes an untagged row, which stops the marker too but still reports the close
    as unobserved, because nothing under the tag says a cycle ran.
    """
    batch = journal.gap_batch(
        journal.QUOTES_SURFACE,
        ticker=ticker,
        snap_ts=slot,
        error_class="http_500",
        close_tag=close_tag,
    )
    stamp = slot.strftime(gap.SEGMENT_STAMP_FORMAT)
    with journal.SegmentWriter.open(
        root, journal.QUOTES_SURFACE, ticker, slot.date(), stamp, 1
    ) as writer:
        writer.write_cycle(batch)


def test_a_guard_run_from_the_real_wiring_files_its_findings(tmp_path):
    """The report file has to come from the daemon, not only from a helper called direct.

    The reporter is a seam, and a seam only ever replaced is a seam nothing executes.
    ``pmset_assertions_probe`` and ``control_plane._spawn`` both shipped that way, where
    a bare ``return`` left the suite green. So this drives the production entry over the
    close+5 minute and reads what landed on disk.
    """
    rig = _rig(tmp_path)
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))

    # Close+5 is 16:20. The start sits before it and the second tick lands on it.
    clock = ManualClock(start=et(2026, 9, 2, 16, 18, 30))
    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    files = sorted(report.close_guard_dir(rig.lake_root, DAY).glob("*.json"))
    assert len(files) == 1, "the guard's findings reached no file"
    entry = json.loads(files[0].read_text())
    assert entry["unobserved"] == ["XYZ"], entry
    assert entry["at"].startswith("2026-09-02T16:20"), entry


def test_a_clean_day_from_the_real_wiring_files_one_too(tmp_path):
    """A day with nothing to report still has to leave a file, or absence means nothing.

    The equity close is already recorded here, so the guard owes XYZ nothing and
    ``reportable`` is false. A reporter that returned early on that would make an absent
    file ambiguous between a healthy day and a daemon that never reached close+5.
    """
    rig = _rig(tmp_path)
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))
    _tagged(rig.lake_root, "XYZ", et(2026, 9, 2, 16, 0), close_tag=SPOT_CLOSE)

    clock = ManualClock(start=et(2026, 9, 2, 16, 18, 30))
    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    files = sorted(report.close_guard_dir(rig.lake_root, DAY).glob("*.json"))
    assert len(files) == 1, "a clean run left nothing behind, so a reader cannot tell it ran"
    assert json.loads(files[0].read_text())["reportable"] is False


def test_a_report_that_cannot_be_written_costs_the_file_and_not_the_markers(tmp_path, capsys):
    """The markers are the record. The file is a copy of what the run said about them.

    A raise from the write reaches ``_dispatched``, which reports it and lets the loop
    tick on. The guard's own run is finished by then, so its rows are already down. This
    blocks the directory the day's reports go in with a file of the same name, which is
    the shape a filesystem can actually present.

    The daemon is alive across 16:00 and its 16:00 write failed, so the guard's marker is
    the one row that minute gets. A daemon started after 16:00 would have startup marking
    record the close first, and the guard would then rightly write nothing.
    """
    rig = _rig(tmp_path)
    _in_scope_all_day(rig)
    blocked = rig.lake_root / "reports" / "close_guard"
    blocked.parent.mkdir(parents=True)
    blocked.write_text("not a directory\n")

    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    clock = ManualClock(start=et(2026, 9, 2, 15, 59, 30))
    _run(
        rig,
        clock,
        ticks=TO_CLOSE_PLUS_FIVE + 1,
        cycle_runner=_FailsTheClose(rig.lake_root),
        hooks=hooks,
    )

    assert seen[-3:] == ["16:19", "16:20", "16:21"], "the loop died on the minute the write failed"
    (marked,) = _at_close(rig.lake_root)
    assert marked["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED, (
        "the failed write cost the marker the guard had already made"
    )
    reported = capsys.readouterr().err
    assert "close+5 2026-09-02:" in reported, "stderr lost the findings too"
    assert "unobserved=XYZ" in reported, "stderr lost the findings too"
    assert "close+5: 2026-09-02:" in reported, "the failed write was swallowed silently"


# -- a dispatched job that raises must not take the loop down --------------------------


def _boom(*args, **kwargs):
    """A job that fails the same way every time it is asked, which is the dangerous way."""
    raise RuntimeError("dispatched job blew up")


def test_a_guard_that_raises_costs_its_markers_and_not_the_session(tmp_path, capsys):
    """A crash loop here would trade two markers for every remaining capture minute.

    Both session-relative jobs ride the tick hook, the guard's from the skipped-slot hook on
    a tick that wakes from a stall across the equity close, and ``run_loop`` wraps no hook
    in a try. So a guard that raises exits the process, and under ``KeepAlive`` the
    successor reaches the same minute, runs the same guard against the same lake, and raises
    again. Capture is the un-buy-backable thing and every other job is arranged not to block
    it, so that trade is backwards.

    ``CloseGuard.run`` handles the failures it can foresee at a finer grain, recording
    them in ``problems`` and carrying on, which is what ``test_close_guard.py`` covers.
    The raise is injected here because after that work no reachable path escapes ``run``.
    This is the backstop for what a later edit adds, and #90's fill producer will add a
    vendor call inside ``run`` shortly.
    """
    rig = _rig(tmp_path)
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))

    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(close_guard.CloseGuard, "run", _boom)
        # Close+5 is 16:20, the second of the three ticks.
        clock = ManualClock(start=et(2026, 9, 2, 16, 18, 30))
        _run(rig, clock, ticks=3, cycle_runner=_no_cycle, hooks=hooks)

    assert seen == ["16:19", "16:20", "16:21"], "the loop died on the minute the guard failed"
    reported = capsys.readouterr().err
    assert "close+5: 2026-09-02: RuntimeError" in reported, "the failure was swallowed silently"


def test_a_compaction_spawn_that_raises_costs_the_seal_and_not_the_session(tmp_path, capsys):
    """The same rule for the other job on the same hook, which had its own catch before.

    ``_start_compaction`` carried a try of its own until this change routed both jobs
    through one wrapper, and nothing exercised it. A spawn failure is the realistic case:
    the interpreter path is wrong after an upgrade, or the machine is out of process
    slots. Sealing is missed for that day, the ``compaction`` check pages on its own
    silence, and the next run sweeps the date it skipped.
    """
    rig = _rig(tmp_path)
    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    # Close+15 is 16:30 and the dispatch waits one tick, so 16:32 is the failing minute.
    clock = ManualClock(start=et(2026, 9, 2, 16, 30, 30))
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=_boom,
        cycle_runner=_no_cycle,
        hooks=hooks,
        should_continue=_stop_after(4),
    )

    assert seen == ["16:31", "16:32", "16:33", "16:34"], "the loop died on the failed spawn"
    reported = capsys.readouterr().err
    assert "compaction: 2026-09-02: RuntimeError" in reported, "the failure was swallowed"


# -- a lake the startup walk cannot read ------------------------------------------------


def test_a_manifest_line_naming_no_partition_does_not_stop_the_daemon(tmp_path):
    """#100's reproduction, driven through the production entry.

    ``on_start`` runs before the first tick, so this crash cost the whole session rather
    than one pass's markers, and the bad line stays on disk so every ``KeepAlive``
    successor hit it again. Zero capture minutes, repeating until someone noticed.

    The daemon ticking at all is the assertion. Everything else here is setup.
    """
    rig = _rig(tmp_path)
    (rig.lake_root / "manifest.jsonl").write_text('{"source": "compaction", "rows": 406}\n')

    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    # Past the option close, so no cycle is owed and the startup walk is what this
    # exercises. ``on_start`` runs before the first tick either way.
    clock = ManualClock(start=et(2026, 9, 2, 17, 0, 30))
    _run(rig, clock, ticks=3, cycle_runner=_no_cycle, hooks=hooks)

    assert seen == ["17:01", "17:02", "17:03"], "the daemon died before its first tick"


def test_a_drifted_segment_does_not_stop_the_daemon_either(tmp_path):
    """The second trigger, the one that needs no bad ledger at all.

    A segment written before a schema change reads back cleanly and then refuses the
    column asked of it. That reaches the same unguarded hook by a different route, so it
    needs its own case rather than riding the manifest's.
    """
    rig = _rig(tmp_path)
    directory = journal.segment_dir(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("nothing_useful", pa.string())])
    with pa.ipc.new_stream(directory / "20260902T100000000000-1.arrows", schema) as writer:
        writer.write_batch(pa.record_batch([pa.array(["x"])], schema=schema))

    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    clock = ManualClock(start=et(2026, 9, 2, 17, 0, 30))
    _run(rig, clock, ticks=2, cycle_runner=_no_cycle, hooks=hooks)

    assert seen == ["17:01", "17:02"], "the daemon died on a segment it could not read"


# -- the power assertion, whose failure the machine may not live to report ---------------


class _FailingAssertions:
    """An ``AssertionRunner`` that refuses the first ``fail`` spawns, then works.

    ``BlockingIOError`` is the failure an exhausted process table gives, which is the
    realistic one: it is a property of ``fork`` rather than of ``caffeinate``, so it hits
    this spawn exactly as it hits the compaction child's.
    """

    def __init__(self, fail: int = 10_000) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._left = fail

    def __call__(self, args) -> None:
        self.calls.append(tuple(args))
        if self._left > 0:
            self._left -= 1
            raise BlockingIOError(35, "Resource temporarily unavailable")


def _assertion_run(rig, clock, *, ticks: int, runner, hooks=None) -> None:
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=runner,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        cycle_runner=_no_cycle,
        hooks=hooks,
        should_continue=_stop_after(ticks),
    )


def test_a_caffeinate_spawn_that_fails_does_not_stop_the_daemon(tmp_path):
    """The crash this closes, and the one whose blast radius is the whole session.

    The assertion is taken from the tick hook, which ``run_loop`` does not wrap, so a
    spawn that will not start exited the process. ``_held`` was never assigned, so the
    ``KeepAlive`` successor landed in the same window and spawned again. The weekday
    window runs 08:25 to 18:45, which brackets the entire session, so this was a crash
    loop across the trading day.
    """
    rig = _rig(tmp_path)
    runner = _FailingAssertions()
    seen: list[str] = []
    hooks = daemon.DaemonHooks(on_tick=lambda slot: seen.append(slot.strftime("%H:%M")))
    # Past the option close, so no cycle is owed and the assertion is what this drives.
    _assertion_run(
        rig, ManualClock(start=et(2026, 9, 2, 17, 0, 30)), ticks=3, runner=runner, hooks=hooks
    )

    assert seen == ["17:01", "17:02", "17:03"], "the daemon died on a failed spawn"


def test_a_failed_spawn_is_retried_the_next_minute(tmp_path):
    """The next minute is the retry, which is why the failure does not mark the window held.

    This is the opposite choice from the once-a-day dispatched jobs, and deliberately so.
    Those fail at a moment already past, so retrying cannot help. This one is owed every
    minute the window is open, so a transient cause such as a full process table clears
    on its own and the assertion is taken late rather than not at all.
    """
    rig = _rig(tmp_path)
    runner = _FailingAssertions(fail=1)
    # Two ticks exactly. Three would pass for a retry on either of the two minutes after
    # the failure, and "eventually" is not the claim: the machine can sleep in one.
    _assertion_run(rig, ManualClock(start=et(2026, 9, 2, 17, 0, 30)), ticks=2, runner=runner)

    assert len(runner.calls) == 2, (
        "the retry did not come on the very next minute, so the window went unasserted"
    )


def test_a_spawn_that_keeps_failing_pages_once_for_the_window(tmp_path):
    """The page has to leave while the machine is still awake to send it.

    AC idle sleep on the capture machine is one minute and the design rejected
    ``pmset disablesleep``, so this assertion is the only thing keeping the machine up. A
    minute later it can be asleep, and a sleeping machine sends nothing, the dead-man's
    ping included. So the daemon says so itself, at the moment it still can.

    Once, not once a minute: the holder retries every minute, and a page repeated for the
    length of the window is one nobody reads.
    """
    rig = _rig(tmp_path)
    runner = _FailingAssertions()
    _assertion_run(rig, ManualClock(start=et(2026, 9, 2, 17, 0, 30)), ticks=4, runner=runner)

    pages = [m for m in rig.transport.sent if m.event == "assertion_lost"]
    assert len(runner.calls) == 4, "the retry stopped"
    assert len(pages) == 1, f"one page per window, got {len(pages)}"

    page = pages[0]
    # The priority is what makes this a page rather than a notification. At anything
    # below it the transport attaches no rotating-light tag and the push arrives silently,
    # which for this message is the same as not sending it.
    assert page.priority == PAGE_PRIORITY, "the page was demoted out of the page tier"
    assert page.title == "Capture at risk: power assertion not held", page.title
    assert "BlockingIOError" in page.body, page.body
    assert "08:25" in page.body, "the page did not say which window was left unasserted"
    assert "stop capturing" in page.body, "the page did not say what it costs"


class _RefusingTransport:
    """A ``Transport`` that counts attempts and refuses every one."""

    def __init__(self) -> None:
        self.attempts = 0

    def send(self, message: Message) -> None:
        self.attempts += 1
        raise OSError("ntfy unreachable")


def test_the_page_leaves_in_the_minute_the_spawn_failed(tmp_path, capsys):
    """One tick late is the whole margin, so the timing is the assertion.

    AC idle sleep on the capture machine is one minute. A page that waits for the next
    tick is scheduled to depart exactly when the machine may already be asleep, and a
    sleeping machine sends nothing. The first draft did wait: the report ran from the
    outermost tick wrapper and ``hold`` runs from the innermost, so it asked about a
    minute the spawn had not been attempted in yet. One tick, zero pages.
    """
    rig = _rig(tmp_path)
    _assertion_run(
        rig, ManualClock(start=et(2026, 9, 2, 17, 0, 30)), ticks=1, runner=_FailingAssertions()
    )

    pages = [m for m in rig.transport.sent if m.event == "assertion_lost"]
    assert len(pages) == 1, "the page waited for a minute the machine might sleep through"
    assert "BlockingIOError" in capsys.readouterr().err, "nothing reached the daemon's own log"


def test_a_page_the_transport_refuses_is_not_retried_every_minute(tmp_path, capsys):
    """Told either way, because the alternative is a page a minute for the whole window.

    The publisher refuses for a leaked secret, the daily cap, or a POST that will not go,
    and it journals what it refused under ``reports/`` where the Now panel counts it. So
    the window counts as told once the attempt is made, and the refusal goes to stderr
    rather than turning one lost assertion into six hundred pages.
    """
    rig = _rig(tmp_path)
    refusing = _RefusingTransport()
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=ManualClock(start=et(2026, 9, 2, 17, 0, 30)),
        calendar=weekday_sessions(WEEK),
        assertion_runner=_FailingAssertions(),
        transport=refusing,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        cycle_runner=_no_cycle,
        should_continue=_stop_after(4),
    )

    assert refusing.attempts == 1, f"the page was retried every minute, {refusing.attempts} times"
    assert "page not sent" in capsys.readouterr().err, "a refused page left no trace"


class _DyingAssertions:
    """A runner whose child dies after ``alive_for`` ticks, the way a killed one would."""

    def __init__(self, alive_for: int) -> None:
        self.spawns = 0
        self._alive_for = alive_for
        self._children: list[list[int | None]] = []

    def __call__(self, args) -> object:
        self.spawns += 1
        state: list[int | None] = [None]
        self._children.append(state)
        countdown = [self._alive_for]

        class _Child:
            def poll(self_inner) -> int | None:
                if state[0] is None:
                    countdown[0] -= 1
                    if countdown[0] < 0:
                        state[0] = 0
                return state[0]

        return _Child()


def test_the_daemon_re_takes_an_assertion_whose_child_died(tmp_path, capsys):
    """The loop is awake while the machine goes unheld, so it is what should notice.

    A ``caffeinate`` killed at 11:00 used to leave the window marked held for the rest of
    the day. The daemon kept ticking, kept capturing, and the machine idled to sleep
    underneath it, which costs the session rather than a marker.

    Reported rather than paged: the lapse lasted at most the minute between two ticks and
    is over by the time anyone could read a page.
    """
    rig = _rig(tmp_path)
    runner = _DyingAssertions(alive_for=1)

    _assertion_run(rig, ManualClock(start=et(2026, 9, 2, 17, 0, 30)), ticks=3, runner=runner)

    assert runner.spawns >= 2, "the dead child was left dead for the rest of the window"
    reported = capsys.readouterr().err
    assert "re-took a caffeinate that had gone" in reported, "the re-take went unrecorded"
    assert [m for m in rig.transport.sent if m.event == "assertion_lost"] == [], (
        "a lapse that healed itself in a minute raised a page"
    )


# -- 13. the cycle hook reaches the schema-drift observer ------------------------------


class _Drifting:
    """A cycle runner whose data segment carries a drifted column on chosen ticks.

    The vendor payload that produces the column is covered in ``test_capture_cycle.py``
    and the scan that reads it in ``test_journal_schema.py``. What is left for the wiring
    is whether the daemon looks at the outcome at all, and whether the state that decides
    a second page survives from one cycle to the next.
    """

    def __init__(self, rig: _Rig, clock: ManualClock, *, drifting_on: Sequence[int]) -> None:
        self._rig = rig
        self._clock = clock
        self._drifting = set(drifting_on)
        self.cycles = 0

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        self.cycles += 1
        routed = ("open_interest",) if self.cycles in self._drifting else ()
        segment = _segment(
            journal.ROW_KIND_DATA, self._rig.lake_root, journal.CHAINS_SURFACE, "XYZ", routed
        )
        return CycleResult(snap_ts=slot, segments=(segment,))


def _drift_pages(rig: _Rig) -> list[Message]:
    """Every schema-drift page the daemon sent, in order."""
    return [page for page in rig.transport.sent if page.event == SCHEMA_DRIFT_EVENT]


def test_a_vendor_retype_reaches_a_phone_through_the_daemons_cycle_hook(tmp_path):
    """The binding this deliverable exists for.

    The parser already knew a known field had refused its column and threw the fact away
    at the end of one function call. The page is only real if the daemon reads it off the
    cycle it just ran, and nothing else in this file drives that hook to a page.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))

    _run(rig, clock, ticks=2, cycle_runner=_Drifting(rig, clock, drifting_on=[2]))

    (page,) = _drift_pages(rig)
    assert page.title == SCHEMA_DRIFT_TITLE
    assert page.priority == PAGE_PRIORITY
    assert "chains: open_interest on 1 ticker(s)" in page.body


def test_a_drift_that_persists_pages_once_rather_than_once_a_minute(tmp_path):
    """The cadence, decided by state the daemon has to keep across cycles.

    Capture runs a cycle a minute against ``alert.DEFAULT_DAILY_CAP`` of forty pages a day,
    so a per-cycle page would spend the whole cap in forty minutes and the page it
    swallowed could be the auth-death page. The observer is built in ``_alarm`` and closed
    over by the hook for exactly this reason: one built per cycle would forget every time.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    runner = _Drifting(rig, clock, drifting_on=[1, 2, 3, 4])

    _run(rig, clock, ticks=4, cycle_runner=runner)

    assert runner.cycles == 4
    assert len(_drift_pages(rig)) == 1


def test_a_drift_that_clears_and_returns_pages_a_second_time(tmp_path):
    """The reset, which is what keeps the once-on-transition rule from going silent forever."""
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))

    _run(rig, clock, ticks=4, cycle_runner=_Drifting(rig, clock, drifting_on=[1, 4]))

    assert len(_drift_pages(rig)) == 2


def test_an_ordinary_session_sends_no_schema_drift_page(tmp_path):
    """The steady state. Every cycle the lake has recorded carried an empty overflow."""
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))

    _run(rig, clock, ticks=4, cycle_runner=_Drifting(rig, clock, drifting_on=[]))

    assert _drift_pages(rig) == []


# -- 14. the startup schema-version check ---------------------------------------------


def _version_pages(rig: _Rig) -> list[Message]:
    """Every page the startup check sent, told apart from the rest by event."""
    return [
        page
        for page in rig.transport.sent
        if page.event
        in {
            schema_versions.UNRECORDED_EVENT,
            schema_versions.CONFLICT_EVENT,
            schema_versions.UNREADABLE_EVENT,
        }
    ]


def test_a_daemon_started_on_a_version_the_ledger_does_not_know_pages_and_keeps_capturing(
    tmp_path,
):
    """Marketlake #130's whole point, driven through the production entry.

    On 2026-09-17 the daemon restarted at 01:32 onto a tree whose ``SCHEMA_VERSION`` the
    ledger had never heard of, and said nothing. Every read of the rows it went on to write
    refused, and nobody knew until a person read the lake twelve hours later.

    It pages and it does not refuse to start. A daemon that will not start captures nothing,
    and under ``KeepAlive`` the successor reaches the same check, so a missing row in a
    reference table would cost the whole session. The loop running here is the half that is
    easy to break by fixing the other one.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).unlink()
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=4, cycle_runner=_no_cycle)

    (page,) = _version_pages(rig)
    assert page.event == schema_versions.UNRECORDED_EVENT
    assert page.priority == PAGE_PRIORITY
    assert str(journal.SCHEMA_VERSION) in page.body
    # The title is the first thing read on a locked phone, and ``alert._record`` keeps it on
    # every failure but a refusal. A constant title would satisfy a test that only asked for
    # the machine path to be absent from it.
    assert page.title == check_running_version(rig.lake_root).title
    assert str(journal.SCHEMA_VERSION) in page.title
    # The loop ticked every minute it was given, which is what "report, never refuse"
    # means. These ticks sit an hour before the open, so the dead-man's idle heartbeat is
    # what shows the loop is alive rather than a cycle.
    assert rig.pinger.urls == [CAPTURE_URL] * 4


def test_the_startup_page_goes_out_once_however_long_the_daemon_lives(tmp_path):
    """Once per process, because the check runs once and the condition cannot change in one.

    The recurring reminder is the vendor sweep's report line. A page a night, or a page a
    minute, until someone runs a command is its own outage.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).unlink()
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=30, cycle_runner=_no_cycle)

    assert len(_version_pages(rig)) == 1


def test_a_daemon_started_on_a_recorded_version_says_nothing(tmp_path, capsys):
    """The steady state, and the case that decides whether this page is noise.

    ``_rig`` records the running version because a production lake has it recorded, so every
    other case in this file drives this branch too. Nothing reaches stderr either, which is
    the half a check that returned on ``pages`` alone would break: ``RECORDED`` has no page.
    """
    rig = _rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=4, cycle_runner=_no_cycle)

    assert _version_pages(rig) == []
    assert "schema_version:" not in capsys.readouterr().err


def test_a_reference_line_carries_the_instant_of_the_clock_the_daemon_was_given(tmp_path, capsys):
    """Marketlake #536. The line's instant is what separates a five-second refusal from a
    five-day one, so it has to be the daemon's own clock rather than one a builder made."""
    rig = _rig(tmp_path)
    master_path(rig.lake_root).write_bytes(b"not parquet at all")
    start = et(2026, 9, 2, 8, 29, 30)

    _run(rig, ManualClock(start=start), ticks=2, cycle_runner=_no_cycle)

    assert (
        f"reference: {master_path(rig.lake_root)} could not be read at {start.isoformat()}"
        in capsys.readouterr().err
    )


def test_a_version_recorded_under_a_different_shape_pages_under_its_own_event(tmp_path):
    """The silent half of the condition, which no read-time refusal reaches.

    ``project_extra`` asks the ledger ``has_column`` and never compares the recorded shape
    against the running one, so a version recorded too wide reports nothing and the read comes
    back whole while a dropped column's nulls read as vendor nulls. The event is its own,
    because ``alert._record`` keeps no body and the three verdicts name three repairs.
    """
    rig = _rig(tmp_path)
    shapes = {
        surface: dict(columns)
        for surface, columns in schema_versions.running_fingerprints().items()
    }
    shapes[journal.CHAINS_SURFACE]["gamma_impact"] = "double"
    schema_versions.SchemaVersionLedger(
        [
            schema_versions.RecordedVersion(
                version=journal.SCHEMA_VERSION,
                recorded_at=datetime(2026, 9, 13, 15, 0, tzinfo=UTC),
                fingerprints=shapes,
            )
        ]
    ).write(ledger_path(rig.lake_root))
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=4, cycle_runner=_no_cycle)

    (page,) = _version_pages(rig)
    assert page.event == schema_versions.CONFLICT_EVENT
    assert "gamma_impact" in page.body
    assert rig.pinger.urls == [CAPTURE_URL] * 4


def test_the_startup_page_carries_no_machine_path(tmp_path):
    """A phone cannot reach a local path, and a page that fails to send is written into a
    directory the dashboard may read. The lake-relative name goes on the wire and the
    absolute one goes to the launchd log.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).unlink()
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    (page,) = _version_pages(rig)
    assert schema_versions.LEDGER_PARTITION in page.body
    assert str(rig.lake_root) not in page.body
    assert str(rig.lake_root) not in page.title


def test_an_unreadable_ledger_pages_under_its_own_event(tmp_path):
    """The third verdict, and the only site that reports it on a session day.

    ``sweep`` computes the same verdict and loses it with the rest of the run, because
    ``_LEDGER_REFUSALS`` does not name ``SchemaVersionsError``. That is marketlake #494. Until
    it lands, this binding is the whole of what says a corrupt ledger exists, and the file it
    names is the one the next backup copies over the last good copy.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).write_bytes(b"not parquet at all")
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=4, cycle_runner=_no_cycle)

    (page,) = _version_pages(rig)
    assert page.event == schema_versions.UNREADABLE_EVENT
    assert rig.pinger.urls == [CAPTURE_URL] * 4


def test_a_ledger_the_daemon_may_not_open_pages_nobody_and_says_so_on_stderr(tmp_path, capsys):
    """Marketlake #536, through the production entry.

    On 2026-09-19 this check was the daemon's first read of the lake after a reboot, a few
    seconds before the owner's login session existed. It came back ``EPERM``, it paged, and
    every later read of the same file worked. A refused open prints its detail and pages
    nobody, and capture goes on.
    """
    rig = _rig(tmp_path)
    target = ledger_path(rig.lake_root)
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    os.chmod(target, 0o000)
    try:
        _run(rig, clock, ticks=4, cycle_runner=_no_cycle)
    finally:
        os.chmod(target, 0o644)

    # Every page the run sent, not ``_version_pages``, which filters by the three paged events
    # and so could never see a page sent for a verdict that has none.
    assert rig.transport.sent == []
    assert f"schema_version: {target} could not be opened: PermissionError" in (
        capsys.readouterr().err
    )
    assert rig.pinger.urls == [CAPTURE_URL] * 4


def test_the_uncapped_detail_reaches_the_log_the_operator_is_sent_to(tmp_path, capsys):
    """stderr is the only surface carrying the absolute path and the uncapped rendering.

    The page body is cut at ``PAGE_COLUMN_CAP`` for the design's byte budget and the nightly
    report line keeps capture-machine paths out of a file the dashboard may read. So a line
    deleted here leaves the operator with the capped page and no way to reach the rest.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).unlink()
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    printed = capsys.readouterr().err
    assert str(ledger_path(rig.lake_root)) in printed
    assert str(journal.SCHEMA_VERSION) in printed


class _BrokenTransport:
    """A ``Transport`` whose POST never lands, standing for ntfy being unreachable."""

    def __init__(self) -> None:
        self.sent: list[Message] = []

    def send(self, message: Message) -> None:
        raise urllib.error.URLError("no route to host")


def test_a_startup_page_that_never_left_the_laptop_says_so_in_the_log(tmp_path, capsys):
    """The design's rule that a page which did not reach the phone must never be invisible.

    ``Publisher`` writes it down under ``reports/alerts/`` either way, and that record is what
    the Now panel counts. The line on stderr is what a person reading the launchd log after a
    deploy sees, and without it a lost page leaves that log silent about a condition every
    read of the lake is already refusing over.
    """
    rig = replace(_rig(tmp_path), transport=_BrokenTransport())
    ledger_path(rig.lake_root).unlink()
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    printed = capsys.readouterr().err
    assert "schema_version: page not sent" in printed
    assert "written down" in printed, "a page nobody recorded is a page lost twice"


def test_a_page_the_publisher_refuses_does_not_get_its_body_printed_instead(tmp_path, capsys):
    """The bargain ``_page_sunday_daemon_finding`` already makes, held here too.

    A publisher answers ``REFUSED`` when it finds one of its own secrets in a page, and it
    redacts its record for that reason. Printing the fuller detail afterwards would undo the
    redaction in the launchd log, which is a file on the same machine. So the refusal ends the
    reporting rather than falling through to stderr.

    The topic is the secret here, and it is set to a string the body already carries, which is
    how a real body could ever come to contain one.
    """
    rig = _rig(tmp_path)
    ledger_path(rig.lake_root).unlink()
    config = rig.config.read_text().replace(
        f"ntfy_topic: {NTFY_TOPIC}\n", "ntfy_topic: schema_version\n"
    )
    rig.config.write_text(config)
    clock = ManualClock(start=et(2026, 9, 2, 8, 29, 30))

    _run(rig, clock, ticks=2, cycle_runner=_no_cycle)

    printed = capsys.readouterr().err
    assert "page refused: it carried a secret" in printed
    # The detail names the absolute ledger path and, on a conflict, every column that moved.
    # Neither may follow a refusal.
    assert str(rig.lake_root) not in printed
    assert rig.transport.sent == []


# -- 15. every read under the installed plist's paths receives them unchanged -----------


def test_the_plists_unset_paths_reach_every_read_unchanged(tmp_path, monkeypatch):
    """The installed plist passes no path, so every read has to receive ``None``.

    ``python -m lake.daemon`` runs under launchd with no arguments, so ``daemon.main`` hands
    the loop ``None`` for the config, the roster and the token. The loop forwards each to
    every helper that reads its file. Most of those helpers answer a file that will not
    load by returning ``None`` and carrying on, so a helper that turned ``None`` into the
    string ``"None"`` would switch off gap marking, the close+5 guard or the idle stamp,
    and nothing would page. The cases above watch each binding's far end under real paths.
    This one watches the reads, which is where a mangled path shows.

    ``MARKETLAKE_CONFIG`` points the config loader at the rig's config, the shape
    ``control_plane render --config`` installs. No install sets ``MARKETLAKE_TICKERS``, so
    production reads the roster at its default path. The variable stands in for that path
    here, because a test must not write there. The cycle runner is the production one,
    over a stub vendor. The run starts at 15:58:30 and lasts 35 ticks. The 16:00 tick
    overruns into 16:02:30, so the skipped-slot hook reads the roster, and the run crosses
    close+5 at 16:20 and close+15's dispatch at 16:31.
    """
    rig = _rig(tmp_path, roster=WITH_OPTIONS)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(rig.config))
    monkeypatch.setenv(TICKERS_PATH_ENV, str(rig.tickers))
    # The loaders find the rig through the variables alone. A file at a default path
    # would be found without them.
    for default in (DEFAULT_CONFIG_PATH, DEFAULT_TICKERS_PATH, DEFAULT_TOKEN_PATH):
        assert not default.exists()
        assert not is_protected(default)
    reads = PathReads.install(monkeypatch)
    monkeypatch.setattr(capture, "SchwabVendor", reads.vendor_factory(_PlanVendor()))

    cycles: list[datetime] = []
    skipped: list[datetime] = []

    def overrun(slot: datetime) -> None:
        if slot == et(2026, 9, 2, 16, 0):
            clock.advance(150)

    clock = ManualClock(start=et(2026, 9, 2, 15, 58, 30))
    daemon.run_loop_from_config(
        config_path=None,
        tickers_path=None,
        token_path=None,
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=rig.transport,
        pinger=rig.pinger,
        compaction_runner=rig.compaction,
        hooks=daemon.DaemonHooks(
            on_tick=overrun,
            on_cycle=lambda slot, result: cycles.append(slot),
            on_skipped=skipped.extend,
        ),
        should_continue=_stop_after(35),
    )
    reads.assert_no_reader_escaped()

    assert cycles
    assert skipped == [et(2026, 9, 2, 16, 1), et(2026, 9, 2, 16, 2)]
    loads = reads.of("load_config", "load_tickers")
    assert [read for read in loads if read.path is not None] == []
    tokens = reads.of("read_token_mint", FROM_TOKEN)
    assert [read for read in tokens if read.path != DEFAULT_TOKEN_PATH] == []
    assert rig.compaction.calls == [daemon.compaction_command(None)]
    # Every read site the loop forwards a path to runs at least once in this run, so a
    # mangled path at any of them reaches the lists above. Each reader is checked on its
    # own, because a helper that reads two files could stop reading one and still appear
    # among the callers of the other. The close+5 fill is the exception, since this rig
    # owes no close, and ``test_close_fill.py`` drives it under the same shape.
    sites = {
        "load_config": {
            "_alarm",
            "_alarm.<locals>.<lambda>",
            "_assertion_pid_stamp",
            "_close_guard",
            "_gap_marker",
            "_guard_reporter",
            "_idle_stamp",
            "_report_schema_version",
        },
        "load_tickers": {
            "_alarm",
            "_close_guard",
            "_gap_marker",
            "_gap_marker.<locals>.<lambda>",
            "_idle_stamp.<locals>.stamp",
            "run_loop_from_config.<locals>.on_skipped",
        },
        "read_token_mint": {"_idle_stamp.<locals>.stamp"},
    }
    for reader, helpers in sites.items():
        missing = {f"lake.daemon.{helper}" for helper in helpers} - reads.callers(reader)
        assert not missing, f"{reader} never read by {sorted(missing)}"
    for reader in ("load_config", "load_tickers", FROM_TOKEN):
        assert "lake.capture.run_cycle_from_config" in reads.callers(reader), reader
