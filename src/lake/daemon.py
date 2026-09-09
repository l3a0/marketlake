"""The daemon loop: the market-hours resident that fires the capture cycle.

The capture primitive in ``lake.capture`` runs one cycle and returns. This module is the
loop that calls it once a minute across a trading session. It is the long-lived,
launchd-managed daemon the design names. launchd is macOS's built-in service manager.
Under its ``KeepAlive`` an exiting process is relaunched within seconds, so a daemon that
exited at the close would relaunch-loop all night. This loop therefore never exits on its
own. Outside the capture window it idles and keeps ticking.

Each minute the loop does three things, in order.

1. **Align to the minute top.** It sleeps through the injected clock until the next
   whole minute. The wait is computed from the clock's own ``now``, never from the wall
   clock, so a test with a manual clock steps the loop deterministically and a sleep
   costs no real time.
2. **Consult the session clock.** ``SessionClock.phase`` says where the current minute
   sits in the session. The capture window is the open through the option close. Off it
   the loop idles.
3. **Run one cycle** through the injected cycle runner, stamped with the two provenance
   tags the loop owns, then hand the result to the observer hook.

Five hooks let the loop-coupled deliverables plug in without touching the loop. Each
has a no-op default, so the loop ships standalone.

- ``on_start()`` is called exactly once, before the first tick. Startup gap-marking
  (D10) plugs in here. Gap-marking writes an explicit marker row for each minute a dead
  daemon missed, so the gap is recorded rather than silently absent.
- ``on_tick(slot)`` is handed every minute the loop sees, session or not. D14's power
  assertion and D13's idle heartbeat plug in here, because each needs a minute the loop is
  awake for rather than a minute it captures on.
- ``close_tag_for(slot)`` is asked, once per capture slot, what ``close_tag`` the minute
  carries. The close-tag decision (D11) plugs in here: ``spot_close`` at the equity close
  and ``option_close`` at the option close. The default answers ``None``.
- ``on_cycle(slot, result)`` is handed each cycle's ``CycleResult`` after it returns. The
  watchdog (D13), which counts consecutive session minutes without a durable data cycle,
  plugs in here.
- ``on_skipped(slots)`` is handed the capture slots the loop missed, in order, when a
  cycle overran its minute. The same gap-marking writer plugs in here, so a slot the
  loop slept through is recorded rather than left a hole.

Two provenance tags ride every row of a cycle. The loop is the first piece that consults
the session clock per minute, so it is the piece that stamps them.

- ``close_tag`` is whatever ``close_tag_for`` answered for the slot.
- ``session_phase`` is ``post_equity_close`` on a slot past the equity close and at or
  before the option close. Those are the minutes when the options still trade but the
  underlying has closed. Every other row carries null.

The daemon holds no expiration state and no cached plan. The production cycle runner
reloads the config, the roster, the token, and the chain plan on every call, so a nightly
plan rewrite takes effect the next minute and a re-auth is picked up the next cycle.

A slow cycle never shifts a later sample. When a cycle overruns its minute, the loop
aligns to the next minute top from wherever the clock stands. The overrun minute fires no
cycle and is never caught up. It must still be recorded. The design counts completeness
from rows, never from holes, and the loop is the only piece that can see the skip. So the
loop keeps exactly one datetime across ticks: the slot of the previous tick. On each tick
of a session date it walks the minutes strictly between that slot and the current one,
keeps the ones inside the capture window, the open through the option close, and hands
them to ``on_skipped`` before doing anything else. Under normal cadence the two slots are
adjacent and nothing is reported. The memory covers every tick, not only the ones that
fired a cycle, and it spans days. Within one incarnation the loop reports every capture
slot it slept through, an overrun or a stall, across as many session days as the stall
covered. A lid closed Monday afternoon and opened Tuesday morning reports Monday's tail
and Tuesday's head. Non-session days contribute nothing, and a night jump that touches
no capture slot reports nothing. A restart resets the memory to none, since the first
tick after ``on_start`` has no previous slot. From there the successor's startup
gap-marking takes over. So the two writers never overlap: the loop reports what it slept
through while alive, and startup marking reports what happened while it was dead. That
one datetime is the loop's only state. The expiration set and the chain plan stay
unheld, because the cycle reads the plan fresh from its file and the expiration set off
the journal on its failure path.

A cycle that raises propagates out of the loop. The production entry reloads config and
the token per cycle, so a raise there means a broken machine, not a vendor hiccup, and the
loop has no channel of its own to report it. The process exits non-zero, launchd logs it
and relaunches, and the successor's startup gap-marking records the minutes lost. A vendor
failure never reaches here: the cycle resolves it into gap rows and returns normally.

Two things are deliberately not here. The launchd plist that runs the daemon is D14's.
Health pings and the backup sync belong to D12 and D13, so unlike the slice-1 runner this
loop pings and backs up nothing per minute.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Protocol

from lake.alert import Message, NtfyTransport, Publisher, Transport
from lake.calendar import Calendar, ExchangeCalendar
from lake.capture import CycleResult, run_cycle_from_config
from lake.clock import Clock, SystemClock
from lake.close_guard import CloseGuard
from lake.close_guard import GuardOutcome as CloseGuardOutcome
from lake.config import ConfigError, input_errors_exit, load_config
from lake.control_plane import AssertionHolder, AssertionRunner, read_token_mint
from lake.deadman import CAPTURE_SLUG, DeadMan
from lake.gap import GapMarker, MarkingReport, surfaces_for
from lake.journal import ROW_KIND_DATA
from lake.metadata import stamp_cycle, stamp_ping
from lake.runner import Pinger, UrllibPinger
from lake.schwab import DEFAULT_TOKEN_PATH
from lake.security_master import SecurityMaster, SecurityMasterError, master_path
from lake.session import (
    CAPTURE_PHASES,
    TICK,
    SessionClock,
    SessionDispatch,
    SessionPhase,
    missed_slots,
    skipped_slots,
)
from lake.tickers import Roster, TickersError, load_tickers
from lake.watchdog import Page, Surface, Watchdog

# The loop's cadence: one tick per minute, on the minute top.

# The step of the day-walk a multi-day stall is accounted by.


class CycleRunner(Protocol):
    """Runs one capture cycle, stamped with the loop's two provenance tags.

    Both tags are passed by keyword. In production this is a closure over
    ``run_cycle_from_config``. A test injects a fake that records the call.
    """

    def __call__(self, *, close_tag: str | None, session_phase: str | None) -> CycleResult: ...


# -- the five hooks ----------------------------------------------------------


def _no_start() -> None:
    """The default startup hook. Nothing runs before the first tick."""


def _no_close_tag(slot: datetime) -> str | None:
    """The default close-tag hook. No minute carries a tag until D11 plugs in."""
    return None


def _ignore_cycle(slot: datetime, result: CycleResult) -> None:
    """The default cycle observer. The result is dropped."""


def _ignore_skipped(slots: list[datetime]) -> None:
    """The default skipped-slot hook. Bare hooks record nothing, so the slots stay holes."""


def _ignore_tick(slot: datetime) -> None:
    """The default tick observer. It fires every minute, session or not."""


@dataclass(frozen=True)
class DaemonHooks:
    """The five seams the loop-coupled deliverables plug into.

    Every field is a callable with a no-op default, so ``DaemonHooks()`` is a complete,
    standalone set. ``slot`` in each signature is the snap slot, the Eastern-time minute
    the cycle fired for, as ``SessionClock.snap_slot`` reports it. ``slots`` in
    ``on_skipped`` are the capture slots the loop missed, in order, the same kind of value.
    """

    on_start: Callable[[], None] = _no_start
    on_tick: Callable[[datetime], None] = _ignore_tick
    close_tag_for: Callable[[datetime], str | None] = _no_close_tag
    on_cycle: Callable[[datetime, CycleResult], None] = _ignore_cycle
    on_skipped: Callable[[list[datetime]], None] = _ignore_skipped


# -- the loop ------------------------------------------------------------------


def seconds_to_next_minute(now: datetime) -> float:
    """Seconds from ``now`` to the next minute top. Always positive.

    Flooring ``now`` to the minute and adding one minute gives the next top. An instant
    already on a top waits a full minute, so the loop never fires twice for one slot.
    Zeroing the seconds and microseconds is flooring, the one time-of-day construction
    the calendar-seam scanner allows outside the calendar module.
    """
    top = now.replace(second=0, microsecond=0) + TICK
    return (top - now).total_seconds()


def _forever() -> bool:
    """The default ``should_continue``. The production loop never stops on its own."""
    return True


def run_loop(
    session_clock: SessionClock,
    cycle_runner: CycleRunner,
    *,
    clock: Clock,
    hooks: DaemonHooks | None = None,
    should_continue: Callable[[], bool] = _forever,
) -> None:
    """Run the market-hours loop until ``should_continue`` says stop.

    ``on_start`` fires once, before the first tick. Then each iteration:

    1. Sleep through ``clock.sleep`` until the next minute top, computed from
       ``clock.now``.
    2. Read ``session_clock.phase()`` and the snap slot, then hand the slot to
       ``on_tick``. That fires every minute, session or not, because the power
       assertion the control plane holds is owed on holidays too.
    3. Hand any capture slots missed since the previous tick, across days if the loop
       slept that long, to ``on_skipped``, then remember this tick's slot. This is the
       loop's only state across ticks.
    4. Off the capture window, idle: nothing else runs this tick.
    5. On a capture slot, ask ``close_tag_for`` for the minute's tag, derive
       ``session_phase`` from the phase, run one cycle with both, and hand the result to
       ``on_cycle``.

    ``should_continue`` is checked at the top of each iteration. It defaults to forever.
    A test binds it to a manual clock to bound a simulated session.
    """
    hooks = hooks if hooks is not None else DaemonHooks()
    # The slot of the previous tick, seeded before ``on_start`` runs so the handoff
    # between startup marking and the loop is exact rather than a matter of timing.
    # Startup marking bounds itself at this same minute plus one. If the pass then
    # outlives its minute, the first tick lands later than that bound and the minutes
    # in between belong to neither producer. Seeding here hands them to ``on_skipped``,
    # which is true to what happened: the daemon was alive and busy. Seeding also costs
    # nothing when the pass is quick, because adjacent slots yield no missed minutes.
    last_slot: datetime | None = session_clock.snap_slot()
    hooks.on_start()
    while should_continue():
        clock.sleep(seconds_to_next_minute(clock.now()))
        phase = session_clock.phase()
        slot = session_clock.snap_slot()
        hooks.on_tick(slot)
        skipped = missed_slots(session_clock, last_slot, slot)
        if skipped:
            hooks.on_skipped(skipped)
        last_slot = slot
        if phase not in CAPTURE_PHASES:
            continue
        close_tag = hooks.close_tag_for(slot)
        session_phase = phase.value if phase is SessionPhase.POST_EQUITY_CLOSE else None
        result = cycle_runner(close_tag=close_tag, session_phase=session_phase)
        hooks.on_cycle(slot, result)


# -- the production entry ------------------------------------------------------


def _report(report: MarkingReport, pass_name: str) -> None:
    """Print what a marking pass did, so a pass that failed is not silent.

    launchd captures the daemon's stderr to its own log, which is the only place a
    startup pass can be seen from. A pass that marked the right thing, one that marked
    nothing because a segment could not be read, and one that stopped at the walk-back
    cap all look identical on disk. Only the quiet case stays quiet: a pass with rows
    and no findings prints nothing, so the ordinary restart adds no noise.
    """
    if not report.problems and not report.truncated:
        return
    parts = [f"gap {pass_name}: rows={report.rows}"]
    if report.truncated:
        parts.append(f"truncated={','.join(report.truncated)}")
    if report.problems:
        parts.append(f"problems={'; '.join(report.problems)}")
    print(" ".join(parts), file=sys.stderr)


def _gap_marker(
    config_path: str | Path | None,
    tickers_path: str | Path | None,
    session_clock: SessionClock,
) -> GapMarker | None:
    """The gap marker for this daemon, or ``None`` when it cannot be built.

    Marking is a record of what was missed, not a capture, and the security master is
    optional, so a missing one is not worth refusing to start over. Returning ``None``
    leaves the hooks bare and the loop unchanged.

    A config or roster that will not load returns ``None`` here too. Through
    ``run_loop_from_config`` that shape is never reached, because ``_alarm`` reads the
    same two files and refuses. The branch is kept for a direct caller.
    """
    try:
        config = load_config(config_path)
        roster = load_tickers(tickers_path)
    except (ConfigError, TickersError):
        return None
    master = None
    try:
        master = SecurityMaster.read(master_path(config.lake_root))
    except (OSError, SecurityMasterError):
        master = None
    return GapMarker(
        lake_root=config.lake_root,
        roster=roster,
        session_clock=session_clock,
        master=master,
    )


def _report_guard(outcome: CloseGuardOutcome) -> None:
    """Print what the guard found, so a close nobody observed is not silent.

    Three of the design's rules for this guard end in "flags the nightly report", and
    no report exists yet. launchd captures the daemon's stderr, which is where these can
    be seen until one does. A day where both closes landed prints nothing.
    """
    if not outcome.reportable:
        return
    parts = [f"close+5 {outcome.day.isoformat()}:"]
    for name, values in (
        ("unobserved", outcome.unobserved),
        ("baseline-less", outcome.baseline_less),
        ("shortfall", outcome.shortfalls),
        ("refused", outcome.refused),
        ("problems", outcome.problems),
    ):
        if values:
            parts.append(f"{name}={','.join(values)}")
    print(" ".join(parts), file=sys.stderr)


def _close_guard(
    config_path: str | Path | None,
    tickers_path: str | Path | None,
    session_clock: SessionClock,
) -> CloseGuard | None:
    """The close+5 guard for this daemon, or ``None`` when it cannot be built.

    A config or roster that will not load returns ``None``. Through
    ``run_loop_from_config`` that shape is never reached, because ``_alarm`` reads the
    same two files and refuses. The branch is kept for a direct caller.
    """
    try:
        config = load_config(config_path)
        roster = load_tickers(tickers_path)
    except (ConfigError, TickersError):
        return None
    return CloseGuard(lake_root=config.lake_root, roster=roster, session_clock=session_clock)


def _idle_stamp(
    config_path: str | Path | None,
    tickers_path: str | Path | None,
    token_path: str | Path | None,
    session_clock: SessionClock,
) -> Callable[[datetime], None] | None:
    """The idle minute's journal metadata stamp, or ``None`` when it cannot be built.

    A capture cycle stamps its own mint time off the vendor it fetched with. Off the
    capture window no cycle runs and no client exists, so the mint comes from the token
    file instead. That file is what the next cycle builds its client from, so the two
    agree. The dashboard still never reads it. One timestamp crosses, never a secret.

    Sunday evening is the minute this exists for. The re-auth ritual mints a fresh token
    on a day that captures nothing, and the design wants the panel showing that mint the
    same night rather than on Monday.

    A minute the loop captures in is left to the cycle's own stamp, which is the split
    the idle heartbeat already makes for the same reason. A roster or a token file that
    will not read costs the minute's stamp and nothing else, so a machine mid-re-auth
    never takes the daemon down.
    """
    try:
        config = load_config(config_path)
    except ConfigError:
        return None
    token = Path(token_path) if token_path is not None else DEFAULT_TOKEN_PATH

    def stamp(slot: datetime) -> None:
        if session_clock.in_capture_window():
            return
        try:
            roster = load_tickers(tickers_path)
            minted = read_token_mint(token)
        except Exception:  # noqa: BLE001 - the stamp is the least important thing here
            # Deliberately broad. `load_tickers` parses YAML and reads a file, so a
            # hand-edited roster raises `yaml.YAMLError` and an unreadable one raises
            # `OSError`, neither of which is a `TickersError`. No hook is wrapped in a
            # try, so anything escaping here exits the process, and KeepAlive relaunches
            # straight into the same tick. A typo in tickers.yaml would crash-loop the
            # daemon. The stamp is informational, so losing a minute of it is the
            # correct price and the docstring above promises exactly that.
            return
        try:
            stamp_cycle(config.lake_root, at=slot, token_minted_at=minted, roster=roster)
        except OSError:
            return

    return stamp


def _alarm(
    config_path: str | Path | None,
    tickers_path: str | Path | None,
    session_clock: SessionClock,
    transport: Transport,
    pinger: Pinger,
) -> tuple[Watchdog, Publisher, DeadMan, Roster]:
    """The watchdog, its publisher, and the dead-man feed.

    Both seams are handed in. Neither is defaulted here, because a default reaching a
    public endpoint is one a caller gets without asking, and the caller that most needs
    to be asked is a test. ``main`` is the only caller in this module that builds them.

    A config or roster that will not load raises. Standing the alarm down instead was
    the older behaviour, and it hid the failure twice over: the daemon ran on with no
    dead-man and no watchdog, and the same test took one path on a machine that had a
    config and another on a machine that did not. A loader that fails is fatal to the
    cycle runner on its first tick anyway, so raising here loses nothing and says why.
    """
    config = load_config(config_path)
    roster = load_tickers(tickers_path)
    publisher = Publisher(
        lake_root=config.lake_root,
        transport=transport,
        # The values that must never reach a phone, checked against the page itself.
        secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
    )
    lake_root = config.lake_root
    deadman = DeadMan(
        pinger=pinger,
        url=config.healthchecks_url(CAPTURE_SLUG),
        session_clock=session_clock,
        # The panel's dead-man line reads the lake, so a landed ping is written there.
        recorder=lambda at: stamp_ping(lake_root, at=at),
    )
    return Watchdog(page_minutes=config.guards.watchdog_page_minutes), publisher, deadman, roster


def run_loop_from_config(
    *,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    hooks: DaemonHooks | None = None,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
    assertion_runner: AssertionRunner | None = None,
    cycle_runner: CycleRunner | None = None,
    transport: Transport,
    pinger: Pinger,
    should_continue: Callable[[], bool] = _forever,
) -> None:
    """Run the loop wired from the real clock, calendar, and config. It never returns.

    This is the entry ``python -m lake.daemon`` calls. The clock defaults to the system
    clock and the calendar to the NYSE calendar from ``exchange_calendars``. The cycle
    runner is a closure over ``run_cycle_from_config``, which reloads the config, the
    roster, the token, and the chain plan on every call. Nothing is cached here, so the
    per-cycle re-read the design wants comes from the existing wiring.

    The caffeinate power assertion is held here rather than left to a caller. The
    design's chain is the wake alarm, then ``KeepAlive`` starting the daemon, then the
    assertion keeping an open laptop awake, and this is the link that holds it.

    ``transport`` and ``pinger`` are required, and neither has a live default. Each one
    reaches a public endpoint, so a default would hand every caller a real ntfy POST and
    a real healthchecks GET without being asked. A test that forgot to pass its own used
    to get exactly that, and a page sent from a test is a page a person receives.
    ``main`` builds the live pair; everything else supplies its own.

    ``cycle_runner`` defaults to the real capture cycle. A test passes its own, which
    is the only way to observe what this entry binds without reaching a vendor: every
    hook the loop-coupled deliverables bind fires on a capture slot.

    ``AssertionHolder`` rides ``on_tick``, so the assertion is taken when a window
    opens and again for each new day the daemon lives through. Any hook the caller
    passed still runs.

    The journal metadata stamp rides ``on_tick`` as well, for the minutes off the
    capture window. On a capture minute the cycle stamps itself, off it there is no
    vendor to ask, and the two together are what keep the Now panel's token age and
    ticker list current on every day the daemon is awake.

    Gap marking rides ``on_start`` and ``on_skipped`` the same way. Both hand their
    missed slots to one ``GapMarker``, so a restart and a live overrun leave the same
    kind of record. Marking needs the lake root, the roster, and the security master,
    which this entry did not load before, so it loads them once here rather than per
    cycle. A missing security master leaves marking off and the loop still runs, because
    a daemon that captures without marking is better than one that does not start. A
    config or roster that will not load is fatal instead, because the alarm needs both
    and a daemon with no dead-man cannot report its own death.
    """
    clock = clock if clock is not None else SystemClock()
    calendar = calendar if calendar is not None else ExchangeCalendar()
    session_clock = SessionClock(clock, calendar)

    hooks = hooks if hooks is not None else DaemonHooks()
    holder = AssertionHolder(runner=assertion_runner)
    caller_on_tick = hooks.on_tick

    def on_tick(slot: datetime) -> None:
        holder.hold(slot)
        caller_on_tick(slot)

    hooks = replace(hooks, on_tick=on_tick)

    # The idle stamp rides ``on_tick`` too, and it skips the capture window because the
    # cycle stamps there off its own vendor. So the panel's token line stays current on
    # a Sunday evening, when the ritual mints a token and no cycle runs to carry it.
    stamper = _idle_stamp(config_path, tickers_path, token_path, session_clock)
    if stamper is not None:
        stamp_on_tick = hooks.on_tick

        def on_tick_stamped(slot: datetime) -> None:
            stamper(slot)
            stamp_on_tick(slot)

        hooks = replace(hooks, on_tick=on_tick_stamped)

    marker = _gap_marker(config_path, tickers_path, session_clock)
    if marker is not None:
        caller_on_start = hooks.on_start
        caller_on_skipped = hooks.on_skipped

        def on_start() -> None:
            _report(marker.on_start(), "startup")
            caller_on_start()

        def on_skipped(slots: list[datetime]) -> None:
            _report(marker.on_skipped(slots), "skipped")
            caller_on_skipped(slots)

        hooks = replace(hooks, on_start=on_start, on_skipped=on_skipped)

    # Every session-relative job is dispatched from in here, because launchd's calendar
    # intervals are fixed wall-clock and cannot express a close-relative time. The
    # close+5 guard is the first of them. The close+15 compaction binds to the same
    # dispatcher when someone builds it.
    #
    # This wraps the gap marker's hooks rather than the other way round, and the order is
    # load-bearing. On a post-close restart the guard owns the two close minutes, and it
    # must write them before startup marking walks the day, or the day's 16:00 and 16:15
    # would carry a marker from each writer.
    guard = _close_guard(config_path, tickers_path, session_clock)
    if guard is not None:
        dispatch = SessionDispatch(
            session_clock=session_clock,
            moment=lambda bounds: bounds.option_close_deadline,
            job=lambda day: _report_guard(guard.run(day)),
        )
        guard_on_start = hooks.on_start
        guard_on_tick = hooks.on_tick

        def on_start_guarded() -> None:
            dispatch.check(clock.now())
            guard_on_start()

        def on_tick_guarded(slot: datetime) -> None:
            dispatch.check(slot)
            guard_on_tick(slot)

        hooks = replace(hooks, on_start=on_start_guarded, on_tick=on_tick_guarded)

    hooks = replace(hooks, close_tag_for=session_clock.close_tag_at)

    # The watchdog rides the cycle observer and the skipped-slot hook, because the loop
    # runs no cycle for a slot it slept through and those are the minutes the daemon was
    # worst off. The dead-man rides the same two plus every tick, so an idle weekday
    # keeps feeding the check that pages on silence.
    watchdog, publisher, deadman, roster = _alarm(
        config_path, tickers_path, session_clock, transport, pinger
    )
    alarm_on_cycle = hooks.on_cycle
    alarm_on_skipped = hooks.on_skipped
    alarm_on_tick = hooks.on_tick

    def raise_pages(pages: list[Page], now: datetime) -> None:
        for page in pages:
            publisher.publish(
                Message(
                    event="capture_down",
                    title=page.title,
                    body=f"{page.minutes} session minutes without a durable cycle",
                ),
                now=now,
            )

    def on_cycle(slot: datetime, result: CycleResult) -> None:
        raise_pages(watchdog.observe(result), slot)
        if any(seg.row_kind == ROW_KIND_DATA for seg in result.segments):
            deadman.captured(slot)
        alarm_on_cycle(slot, result)

    def on_skipped(slots: list[datetime]) -> None:
        # The roster is re-read here rather than closed over. The cycle runner
        # re-reads it every cycle, and the design has the watchdog counters read
        # that same snapshot, so a ticker retired mid-session must stop being
        # charged without a restart. A roster that will not load leaves the last
        # good one in place, since refusing to count is worse than counting a
        # ticker one cycle too long.
        try:
            current = load_tickers(tickers_path)
        except TickersError:
            current = roster
        watched = [
            Surface(surface, entry.ticker) for entry in current for surface in surfaces_for(entry)
        ]
        raise_pages(watchdog.missed(watched, slots), slots[-1])
        alarm_on_skipped(slots)

    def on_tick(slot: datetime) -> None:
        deadman.idle(slot)
        alarm_on_tick(slot)

    hooks = replace(hooks, on_cycle=on_cycle, on_skipped=on_skipped, on_tick=on_tick)

    def run_a_cycle(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
        return run_cycle_from_config(
            clock=clock,
            config_path=config_path,
            tickers_path=tickers_path,
            token_path=token_path,
            close_tag=close_tag,
            session_phase=session_phase,
        )

    run_loop(
        session_clock,
        cycle_runner if cycle_runner is not None else run_a_cycle,
        clock=clock,
        hooks=hooks,
        should_continue=should_continue,
    )


# -- the command-line entry ----------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lake.daemon",
        description="The market-hours capture daemon. It runs until stopped.",
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument(
        "--tickers", help="Path to tickers.yaml (defaults to the standard location)."
    )
    parser.add_argument("--token", help="Path to token.json (defaults to the standard location).")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.daemon`` entry. Loops forever, so it returns only when stopped."""
    args = build_parser().parse_args(argv)
    # The only construction site in this module. The config is read here as well as
    # inside the loop, because the ntfy topic names the transport and the transport is
    # wired from out here now.
    #
    # The wrapper puts this entry in the same class as every other one that reads an
    # operator file. A missing config or roster is an operator mistake, so it earns one
    # named line and exit 2 rather than a traceback. That matters more here than
    # elsewhere: launchd restarts the daemon under ``KeepAlive``, so a traceback would
    # repeat every few seconds in the log the operator is told to read.
    with input_errors_exit("daemon"):
        config = load_config(args.config)
        run_loop_from_config(
            config_path=args.config,
            tickers_path=args.tickers,
            token_path=args.token,
            transport=NtfyTransport(config.ntfy_topic.reveal()),
            pinger=UrllibPinger(),
        )
    return 0


__all__ = [
    "TICK",
    "CycleRunner",
    "DaemonHooks",
    "build_parser",
    "main",
    "run_loop",
    "run_loop_from_config",
    "seconds_to_next_minute",
    "skipped_slots",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
