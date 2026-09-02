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

Four hooks let the loop-coupled deliverables plug in without touching the loop. Each
has a no-op default, so the loop ships standalone.

- ``on_start()`` is called exactly once, before the first tick. Startup gap-marking
  (D10) plugs in here. Gap-marking writes an explicit marker row for each minute a dead
  daemon missed, so the gap is recorded rather than silently absent.
- ``close_tag_for(slot)`` is asked, once per capture slot, what ``close_tag`` the minute
  carries. The close-tag decision (D11) plugs in here: ``spot_close`` at the equity close
  and ``option_close`` at the option close. The default answers ``None``.
- ``on_cycle(slot, result)`` is handed each cycle's ``CycleResult`` after it returns. The
  watchdog (D13), which counts consecutive session minutes without a durable data cycle,
  plugs in here.
- ``on_skipped(slots)`` is handed the capture slots the loop missed, in order, when a
  cycle overran its minute. The same gap-marking writer (D10) plugs in here. Until it
  does, a skipped slot stays a hole, exactly as a loop with no such hook would leave it.

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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from lake.calendar import Calendar, ExchangeCalendar, NotASession
from lake.capture import CycleResult, run_cycle_from_config
from lake.clock import Clock, SystemClock
from lake.session import CAPTURE_PHASES, SessionBounds, SessionClock, SessionPhase

# The loop's cadence: one tick per minute, on the minute top.
TICK = timedelta(minutes=1)

# The step of the day-walk a multi-day stall is accounted by.
_ONE_DAY = timedelta(days=1)


class CycleRunner(Protocol):
    """Runs one capture cycle, stamped with the loop's two provenance tags.

    Both tags are passed by keyword. In production this is a closure over
    ``run_cycle_from_config``. A test injects a fake that records the call.
    """

    def __call__(self, *, close_tag: str | None, session_phase: str | None) -> CycleResult: ...


# -- the four hooks -----------------------------------------------------------


def _no_start() -> None:
    """The default startup hook. Nothing runs before the first tick."""


def _no_close_tag(slot: datetime) -> str | None:
    """The default close-tag hook. No minute carries a tag until D11 plugs in."""
    return None


def _ignore_cycle(slot: datetime, result: CycleResult) -> None:
    """The default cycle observer. The result is dropped."""


def _ignore_skipped(slots: list[datetime]) -> None:
    """The default skipped-slot hook. The slots stay holes until D10 plugs its writer in."""


@dataclass(frozen=True)
class DaemonHooks:
    """The four seams the loop-coupled deliverables plug into.

    Every field is a callable with a no-op default, so ``DaemonHooks()`` is a complete,
    standalone set. ``slot`` in each signature is the snap slot, the Eastern-time minute
    the cycle fired for, as ``SessionClock.snap_slot`` reports it. ``slots`` in
    ``on_skipped`` are the capture slots the loop missed, in order, the same kind of value.
    """

    on_start: Callable[[], None] = _no_start
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


def skipped_slots(bounds: SessionBounds, after: datetime, before: datetime) -> list[datetime]:
    """The capture slots strictly between two slots of one session date, in order.

    It steps one minute at a time from ``after`` toward ``before`` and keeps each slot
    inside the capture window, ``bounds.open`` through ``bounds.option_close`` inclusive.
    Adjacent slots yield nothing. So does a span that lies wholly off the window.
    """
    skipped: list[datetime] = []
    candidate = after + TICK
    while candidate < before:
        if bounds.open <= candidate <= bounds.option_close:
            skipped.append(candidate)
        candidate += TICK
    return skipped


def _skips_since(
    session_clock: SessionClock,
    last_slot: datetime | None,
    slot: datetime,
) -> list[datetime]:
    """The capture slots missed between the previous tick and this one, in order.

    Nothing is missed before the first tick or between adjacent ticks, and only past
    that short-circuit does the calendar get asked, so the normal-cadence path never
    touches it. A wider span is walked one calendar day at a time, from the previous
    tick's date through this one's. A day the calendar refuses as ``NotASession``, a
    weekend, a holiday, or a Saturday wake, contributes nothing. Each session day is
    clipped at its own edges: the first day runs from the previous slot through its
    option close, the last day from its open up to this slot, a middle day end to end,
    and a same-day span from the previous slot to this one. So a stall across days
    reports the first day's tail and the last day's head, and a night jump that touches
    no capture slot reports nothing.
    """
    if last_slot is None or slot - last_slot <= TICK:
        return []
    first_day = last_slot.date()
    last_day = slot.date()
    skipped: list[datetime] = []
    day = first_day
    while day <= last_day:
        try:
            bounds = session_clock.bounds(day)
        except NotASession:
            day += _ONE_DAY
            continue
        after = last_slot if day == first_day else bounds.open - TICK
        before = slot if day == last_day else bounds.option_close + TICK
        skipped.extend(skipped_slots(bounds, after, before))
        day += _ONE_DAY
    return skipped


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
    2. Read ``session_clock.phase()`` and the snap slot.
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
    hooks.on_start()
    # The slot of the previous tick. None before the first tick, so a fresh incarnation
    # leaves the minutes before it to startup gap-marking and the two never overlap.
    last_slot: datetime | None = None
    while should_continue():
        clock.sleep(seconds_to_next_minute(clock.now()))
        phase = session_clock.phase()
        slot = session_clock.snap_slot()
        skipped = _skips_since(session_clock, last_slot, slot)
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


def run_loop_from_config(
    *,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    hooks: DaemonHooks | None = None,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
) -> None:
    """Run the loop wired from the real clock, calendar, and config. It never returns.

    This is the entry ``python -m lake.daemon`` calls. The clock defaults to the system
    clock and the calendar to the NYSE calendar from ``exchange_calendars``. The cycle
    runner is a closure over ``run_cycle_from_config``, which reloads the config, the
    roster, the token, and the chain plan on every call. Nothing is cached here, so the
    per-cycle re-read the design wants comes from the existing wiring.
    """
    clock = clock if clock is not None else SystemClock()
    calendar = calendar if calendar is not None else ExchangeCalendar()
    session_clock = SessionClock(clock, calendar)

    def cycle_runner(*, close_tag: str | None, session_phase: str | None) -> CycleResult:
        return run_cycle_from_config(
            clock=clock,
            config_path=config_path,
            tickers_path=tickers_path,
            token_path=token_path,
            close_tag=close_tag,
            session_phase=session_phase,
        )

    run_loop(session_clock, cycle_runner, clock=clock, hooks=hooks)


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
    run_loop_from_config(
        config_path=args.config,
        tickers_path=args.tickers,
        token_path=args.token,
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
