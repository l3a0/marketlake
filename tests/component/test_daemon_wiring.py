"""The daemon's production hook bindings, over real files.

``run_loop`` owns the loop and ``DaemonHooks`` owns the five seams it fires. Every
observer that plugs into a seam is covered on its own elsewhere. What these cover is the
wiring ``run_loop_from_config`` builds between the two, which is the wiring the launchd
job runs. A binding can be deleted with every isolated test still green, so each case
here drives the production entry and watches the far end of one binding.

Each runs that entry with a manual clock, a fake calendar, and a throwaway config,
roster, and lake on disk. The three seams that reach past the process are faked: the
ntfy transport, the health-check pinger, and the backup's ``rsync``. A page sent from a
test is a page a person receives, and a sync from one copies a throwaway lake onto the
machine running the suite. So the tier is component: the daemon over real files, with the
clock, the calendar, the network, and the backup still fake.

Ten bindings are covered here.

1. The skipped-slot hook reaches the gap marker, so a live overrun records the minutes
   it slept through.
2. The skipped-slot hook reaches the watchdog, so those same minutes charge its counters.
3. The per-tick hook feeds the capture dead-man's idle heartbeat.
4. The cycle hook feeds the same dead-man's ``captured`` signal, which arms the check on
   the first durable cycle.
5. The per-tick hook reaches the close+5 guard's dispatcher, so a daemon alive across
   close+5 runs the guard on that minute.
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
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake import capture, close_guard, daemon, gap, journal
from lake.alert import Message
from lake.capture import CycleResult, SegmentOutcome
from lake.capture_spans import CaptureSpans, spans_path
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.compact import COMPACTION_SLUG, compact, write_chain_plan
from lake.config import GuardConstants
from lake.deadman import CAPTURE_SLUG
from lake.paths import LakePaths
from lake.security_master import SecurityMaster, master_path
from lake.session import SPOT_CLOSE, TICK
from lake.tickers import TickersError
from lake.vendor import VendorResponse
from tests.support.backup import FakeBackup
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import PING_KEY, write_config
from tests.support.pinger import FakePinger

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
# options, so each ticker owns exactly one counter. A slept-through slot attempts no
# request, so the two quotes counters page one by one rather than collapsing into the
# single sampler page a failed live cycle would raise.
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

    def __call__(self, *, close_tag: str | None, session_phase: str | None) -> CycleResult:
        slot = self._clock.now().replace(second=0, microsecond=0)
        self.slots.append(slot)
        if len(self.slots) == 1:
            self._clock.advance(self._seconds)
        return CycleResult(snap_ts=slot, segments=())


def _no_cycle(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
    """A cycle runner for the minutes off the capture window, where none may run."""
    raise AssertionError("a cycle ran off the capture window")


def _segment(row_kind: str, root: Path) -> SegmentOutcome:
    """One journalled segment of the named kind, the shape a cycle result carries."""
    return SegmentOutcome(
        surface=journal.QUOTES_SURFACE,
        ticker="XYZ",
        path=root / "segment.arrows",
        partition="quotes/ticker=XYZ/date=2026-09-02/segment.arrows",
        row_kind=row_kind,
        rows=1,
        error_class=None if row_kind == journal.ROW_KIND_DATA else "boom",
        fetched_at=None,
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
    assert page.title == "Capture down: XYZ quotes"
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
    _run(rig, clock, ticks=1, cycle_runner=lambda *, close_tag, session_phase: result)

    assert rig.pinger.urls == [CAPTURE_URL] * pings


# -- 5. the per-tick hook reaches the close+5 guard ----------------------------------


def test_a_daemon_alive_across_close_plus_five_runs_the_guard_that_minute(tmp_path):
    """The guard has to fire on the ordinary day, not only after a restart.

    launchd's calendar intervals are fixed wall clock and cannot express a
    close-relative moment, so every session-relative job is dispatched from inside the
    loop. A daemon still running at close+5 is the common case. An unbound tick hook
    leaves the guard to the startup check alone, so the equity close goes unwitnessed
    on every day the daemon does not happen to restart after it.
    """
    rig = _rig(tmp_path)
    # XYZ is in scope for the session, so the guard finds it owes the close.
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(rig.lake_root))
    # The option close is already recorded, so startup marking has nothing to write for
    # the day and every later row came from the guard.
    _record(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", et(2026, 9, 2, 16, 15))
    at_start: list[dict] = []
    hooks = daemon.DaemonHooks(
        on_start=lambda: at_start.extend(_rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY))
    )
    # Close+5 is 16:20. The start sits before it and the second tick lands on it.
    clock = ManualClock(start=et(2026, 9, 2, 16, 18, 30))
    _run(rig, clock, ticks=2, cycle_runner=_no_cycle, hooks=hooks)

    unobserved = [row["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED for row in at_start]
    assert not any(unobserved), "the startup check ran the guard before close+5"
    marked = [
        row
        for row in _rows(rig.lake_root, journal.QUOTES_SURFACE, "XYZ", DAY)
        if row["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
    ]
    assert len(marked) == 1
    assert marked[0]["snap_ts"].startswith("2026-09-02T16:00")
    assert marked[0]["close_tag"] == SPOT_CLOSE


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
        def from_token(path, *, api_key, app_secret):
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
    assert vendor.windows[1:] == [(DAY, DAY), (NEXT_DAY, None)]


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
    page threshold. So the hook pages every surface it charges across that stretch, and
    every surface it does not charge stays silent. The cycles produce no segment of
    their own, so nothing but the missed minutes charges a counter.
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

    # Sorted, because the page order is the watchdog's own rule and not this one's.
    assert sorted(page.title for page in rig.transport.sent) == ["Capture down: ABC quotes"]


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

    assert sorted(page.title for page in rig.transport.sent) == [
        "Capture down: ABC quotes",
        "Capture down: DEF quotes",
        "Capture down: XYZ quotes",
    ]


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

    def stall_across_the_close(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
        slot = clock.now().replace(second=0, microsecond=0)
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

    # One quotes ticker, so the sampler collapse cannot fire whatever the fan-out does.
    assert sorted(page.title for page in rig.transport.sent) == [
        "Capture down: SPY chains",
        "Capture down: SPY quotes",
    ]


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

    def __call__(self, *, close_tag: str | None, session_phase: str | None) -> CycleResult:
        self.calls += 1
        if self.calls == self._recalibrate_at:
            write_config(
                self._rig.config.parent,
                self._rig.lake_root,
                guards={"watchdog_page_minutes": self._new_page_minutes},
            )
        slot = self._clock.now().replace(second=0, microsecond=0)
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
    assert page.body == f"{RECALIBRATED_PAGE_MINUTES} session minutes without a durable cycle"


# -- 9. an empty roster keeps the daemon running ---------------------------------------


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

    def cycle(*, close_tag, session_phase):
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
        cycle_runner=lambda *, close_tag, session_phase: CycleResult(clock.now(), ()),
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


# -- a dispatched job that raises must not take the loop down --------------------------


def _boom(*args, **kwargs):
    """A job that fails the same way every time it is asked, which is the dangerous way."""
    raise RuntimeError("dispatched job blew up")


def test_a_guard_that_raises_costs_its_markers_and_not_the_session(tmp_path, capsys):
    """A crash loop here would trade two markers for every remaining capture minute.

    Both session-relative jobs ride the tick hook, and ``run_loop`` wraps no hook in a
    try. So a guard that raises exits the process, and under ``KeepAlive`` the successor
    reaches the same minute, runs the same guard against the same lake, and raises again.
    Capture is the un-buy-backable thing and every other job is arranged not to block it,
    so that trade is backwards.

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
