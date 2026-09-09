"""The daemon's production hook bindings, over real files.

``run_loop`` owns the loop and ``DaemonHooks`` owns the five seams it fires. Every
observer that plugs into a seam is held on its own elsewhere. What these hold is the
wiring ``run_loop_from_config`` builds between the two, which is the wiring the launchd
job runs. A binding can be deleted with every isolated test still green, so each case
here drives the production entry and watches the far end of one binding.

Each runs that entry with a manual clock, a fake calendar, and a throwaway config,
roster, and lake on disk. The two seams that reach the public internet, the ntfy
transport and the health-check pinger, are faked, because a page sent from a test is a
page a person receives. So the tier is component: the daemon over real files, with the
clock, the calendar, and the network still fake.

Seven bindings are pinned here.

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
   stops paging without a restart. A file the roster loader refuses leaves the startup
   roster in place, so the refusal silences no counter. Each entry expands into the
   surfaces its ticker is captured on, so an options ticker's chains counter is charged
   beside its quotes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import capture, close_guard, daemon, gap, journal
from lake.alert import Message
from lake.capture import CycleResult, SegmentOutcome
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.compact import write_chain_plan
from lake.deadman import CAPTURE_SLUG
from lake.session import SPOT_CLOSE
from lake.vendor import VendorResponse
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import PING_KEY, write_config
from tests.support.pinger import FakePinger

# The Monday of the week these runs live in. Its sessions run Monday through Friday,
# each opening at 09:30 and closing at 16:00, so the option close lands at 16:15.
WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)
NEXT_DAY = date(2026, 9, 3)

# The check the daemon feeds, as the throwaway config addresses it.
CAPTURE_URL = f"https://hc-ping.com/{PING_KEY}/{CAPTURE_SLUG}"

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


@dataclass(frozen=True)
class _Rig:
    """The throwaway machine one daemon run reads, and the fakes for its two endpoints."""

    lake_root: Path
    config: Path
    tickers: Path
    token: Path
    transport: _Recording
    pinger: FakePinger


def _rig(tmp_path: Path, roster: str = EQUITY_ONLY) -> _Rig:
    """A complete config, roster, and lake under ``tmp_path``."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(roster)
    return _Rig(
        lake_root=lake_root,
        config=write_config(tmp_path, lake_root),
        tickers=tickers,
        token=tmp_path / "token.json",
        transport=_Recording(),
        pinger=FakePinger(),
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
    ``caffeinate``, which exists on macOS and not on a Linux CI runner.
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


def _run_across_an_edit(rig: _Rig, roster: str) -> None:
    """Run one cycle, rewrite the roster to ``roster``, then overrun three slots.

    The 10:00 cycle takes 200 seconds, so the loop next wakes at 10:04 and hands 10:01,
    10:02, and 10:03 to the skipped-slot hook. Three slept-through slots is the design's
    page threshold. So the hook pages every surface it charges across that stretch, and
    every surface it does not charge stays silent. The cycles produce no segment of
    their own, so nothing but the missed minutes charges a counter.
    """
    for ticker in ("XYZ", "ABC"):
        # Neither assertion reads these rows. They stop startup marking walking back to
        # its cap, which prints a truncation line to stderr on every run that lets it.
        _record(rig.lake_root, journal.QUOTES_SURFACE, ticker, et(2026, 9, 2, 9, 58))
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


def test_a_roster_that_will_not_load_leaves_the_startup_roster_charging(tmp_path):
    """A roster the loader refuses must not silence the counters that were running.

    The re-read reaches a hand-owned file, so it can land on a save that is not yet a
    roster. Refusing to count is the worse failure of the two. It turns a daemon whose
    surfaces are down into a quiet one for as long as the file stays broken. Charging a
    ticker one overrun too long only costs an early page.

    What the fallback holds is the roster loaded at startup, not the last one that
    loaded successfully. ``_alarm`` binds that roster once and nothing reassigns it. The
    two are the same roster here, because the only load this run completes is the
    startup one.

    The file here is valid YAML that the loader refuses on shape. ``load_tickers`` does
    not convert a YAML parse error into a ``TickersError``, so a file broken mid-token
    raises straight past this fallback instead of taking it. That case is not held here.
    """
    rig = _rig(tmp_path, roster=TWO_TICKERS)
    _run_across_an_edit(rig, UNLOADABLE)

    # Both still charge. The refusal silenced no counter, and the unloadable file
    # retired nobody.
    assert sorted(page.title for page in rig.transport.sent) == [
        "Capture down: ABC quotes",
        "Capture down: XYZ quotes",
    ]


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
