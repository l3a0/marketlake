"""The daemon loop, decided from values alone.

Every case here drives ``run_loop`` with a manual clock, a fake calendar, and a fake
cycle runner that records each call and returns a canned result. No file, network, or
wall clock is in the path, so these are unit tests. The times named here are declared
through the fakes, which is exactly where naming a time is allowed.

They cover the loop's observable contract:

1. A regular session fires one cycle at every capture slot, the open through the option
   close, and at no other minute. The loop idles before the open and after the close.
2. A non-session day fires nothing. An early close's last cycle is its 13:15 slot.
3. Every sleep lands on a minute top, and the loop keeps ticking off the window rather
   than returning.
4. The six hooks fire as specified: ``on_start`` once before any cycle, ``on_tick`` on
   every minute the loop sees, ``close_tag_for`` once per capture slot with its answer
   passed through, ``on_cycle`` with every result in order, ``on_skipped`` with the
   capture slots a stall missed, and ``on_missed`` with the same slots once the cycles
   ahead of the stall are handed on.
5. ``session_phase`` is ``post_equity_close`` on the slots past the equity close and
   through the option close, and null elsewhere.
6. A slow cycle skips no minute, because each cycle runs on a thread of its own. A stall
   of the loop thread skips the minutes it slept through and realigns. They are never
   caught up. A skipped slot is reported, never a silent hole.
7. Skip detection reports exactly the missed capture slots: one for a one-slot stall,
   both in order for a two-slot stall, only the in-window slots when the stall crosses
   the option close, and nothing under normal cadence or on a first tick that follows
   the start minute. A startup hook that outlives its minute leaves the minutes
   up to the first tick to skip detection, starting at the minute after the one the
   daemon started in.
8. A stall spans days within one incarnation. It reports the first day's tail and the
   last day's head, in order, with weekends and holidays contributing nothing. A wake
   on a Saturday still reports Friday's tail. A night jump reports nothing.
9. Results reach ``on_cycle`` in slot order, as each cycle finishes rather than at the
   next tick. A tick at close+5 and leaving the loop each wait for every cycle in flight
   first. A tick with skipped slots waits for none of them, so later minutes still start
   at their tops, and its stall reaches ``on_missed`` behind them (marketlake #644). A
   cycle that raises ends the loop once every cycle behind it has finished
   (marketlake #565).

No fake cycle here moves the clock. The loop thread and a cycle's thread would then race
for it, and a test that passed would pass through a mechanism production no longer has,
since a cycle's length cannot delay the loop. A slow cycle is one held on an event. A
stall is a hook, a clock sleep, or ``_simulate``'s ``stall``, all on the loop thread.
"""

from __future__ import annotations

import subprocess
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import pytest

from lake import daemon
from lake.alert import NtfyTransport
from lake.calendar import MARKET_TZ
from lake.capture import CycleResult
from lake.config import CONFIG_PATH_ENV, ConfigError, default_config_path
from lake.control_plane import assertion_window, caffeinate_args
from lake.runner import UrllibPinger
from lake.session import CAPTURE_PHASES, SessionClock, SessionPhase
from lake.tickers import TickersError
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import CostlyClock, ManualClock
from tests.support.config import NTFY_TOPIC, write_config
from tests.support.config_guard import is_protected
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

ET = MARKET_TZ
FRIDAY = date(2026, 8, 21)  # the Friday before the regular Monday
SATURDAY = date(2026, 8, 22)  # not a session
REGULAR = date(2026, 8, 24)  # a summer Monday
NEXT_DAY = date(2026, 8, 25)  # the Tuesday after, a second consecutive session
LABOR_FRIDAY = date(2026, 9, 4)  # the Friday before Labor Day
LABOR_DAY = date(2026, 9, 7)  # a Monday holiday, not a session
LABOR_TUESDAY = date(2026, 9, 8)  # the session after the holiday
EARLY_CLOSE = date(2026, 11, 27)  # the day after Thanksgiving, a half day
HOLIDAY = date(2026, 12, 25)  # Christmas, not a session

POST_EQUITY_CLOSE = SessionPhase.POST_EQUITY_CLOSE.value


def et(day: date, h: int, m: int, s: int = 0, us: int = 0) -> datetime:
    """An Eastern-time instant on ``day``."""
    return datetime(day.year, day.month, day.day, h, m, s, us, tzinfo=ET)


def _slots(first: datetime, last: datetime) -> list[datetime]:
    """Every minute slot from ``first`` through ``last`` inclusive."""
    count = int((last - first) / timedelta(minutes=1)) + 1
    return [first + timedelta(minutes=i) for i in range(count)]


def _regular(day: date) -> SessionTimes:
    return SessionTimes(open=et(day, 9, 30), close=et(day, 16, 0))


@pytest.fixture
def calendar() -> FakeCalendar:
    return FakeCalendar(
        {
            FRIDAY: _regular(FRIDAY),
            REGULAR: _regular(REGULAR),
            NEXT_DAY: _regular(NEXT_DAY),
            LABOR_FRIDAY: _regular(LABOR_FRIDAY),
            LABOR_TUESDAY: _regular(LABOR_TUESDAY),
            EARLY_CLOSE: SessionTimes(
                open=et(EARLY_CLOSE, 9, 30), close=et(EARLY_CLOSE, 13, 0), early_close=True
            ),
        }
    )


# How long a held cycle waits for its release before it fails the test. A loop that stops
# releasing it, such as one calling the cycle inline, then fails in seconds rather than
# hanging the suite.
HOLD_TIMEOUT_SECONDS = 10.0

# The real grace a held-cycle case gives ``ManualClock.wait``. Each wait on a held cycle
# spends the whole grace, and the fakes here that are not held finish in microseconds, so a
# short grace keeps those cases quick. A fake that misses it is handed on a tick later, which
# no ordering assertion here depends on.
HELD_GRACE_SECONDS = 0.1


class _RecordingRunner:
    """A fake cycle runner.

    It records each call as ``(slot, close_tag, session_phase)``, with the slot the loop
    handed it, and returns a canned empty result whose ``snap_ts`` is that slot in UTC, the
    way the real cycle files it. Beside each call it records ``floors``, its own clock
    read floored to the minute, which is the minute a cycle reading the clock for itself
    would have filed under. ``hold`` maps a slot to an event its cycle waits on before it
    returns, which is how a slow cycle is modelled. It never moves the clock.
    """

    def __init__(
        self, clock: ManualClock, hold: dict[datetime, threading.Event] | None = None
    ) -> None:
        self._clock = clock
        self._hold = hold if hold is not None else {}
        self._lock = threading.Lock()
        self.calls: list[tuple[datetime, str | None, str | None]] = []
        self.floors: list[datetime] = []
        self.results: list[CycleResult] = []

    def __call__(
        self, *, slot: datetime, close_tag: str | None, session_phase: str | None
    ) -> CycleResult:
        # Read through ``ManualClock.now`` itself, so a ``CostlyClock`` does not charge this
        # read. Its reads move the clock, and a read here would be a cycle moving it.
        floor = ManualClock.now(self._clock).replace(second=0, microsecond=0).astimezone(ET)
        result = CycleResult(snap_ts=slot.astimezone(UTC), segments=())
        # Recorded before any hold, in the order the loop fired the cycles, since each
        # starts before the loop reaches the next tick.
        with self._lock:
            self.floors.append(floor)
            self.calls.append((slot, close_tag, session_phase))
            self.results.append(result)
        held = self._hold.get(slot)
        if held is not None and not held.wait(HOLD_TIMEOUT_SECONDS):
            raise AssertionError(f"the {slot:%H:%M} cycle was never released")
        return result

    @property
    def slots(self) -> list[datetime]:
        return [slot for slot, _tag, _phase in self.calls]


def _simulate(
    calendar: FakeCalendar,
    start: datetime,
    end: datetime,
    *,
    hooks: daemon.DaemonHooks | None = None,
    stall: tuple[datetime, float] | None = None,
) -> tuple[_RecordingRunner, list[datetime]]:
    """Run the loop from ``start`` until the clock reaches ``end``.

    Returns the runner and every instant ``should_continue`` observed. The first observed
    instant is ``start`` itself, before any sleep. Each later one is the clock right after
    a tick's work, so it is the instant the tick landed on when the cycle takes no time.
    ``stall`` is ``(instant, seconds)``: the first time the clock is seen at or past that
    instant between ticks, it jumps forward by that many seconds. It models a stall of the
    loop thread, like the laptop sleeping, on any tick, capture or idle. A negative count
    steps the wall clock back.
    """
    clock = ManualClock(start=start.astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock)
    instants: list[datetime] = []
    end_utc = end.astimezone(UTC)
    pending_stall = None if stall is None else (stall[0].astimezone(UTC), stall[1])

    def should_continue() -> bool:
        nonlocal pending_stall
        instants.append(clock.now())
        if pending_stall is not None and clock.now() >= pending_stall[0]:
            clock.advance(pending_stall[1])
            pending_stall = None
        return clock.now() < end_utc

    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=should_continue
    )
    return runner, instants


def _skip_recorder() -> tuple[daemon.DaemonHooks, list[list[datetime]]]:
    """Hooks whose ``on_skipped`` appends each report, as its own list, to the log."""
    reports: list[list[datetime]] = []
    return daemon.DaemonHooks(on_skipped=lambda slots: reports.append(list(slots))), reports


# -- 1. the regular session ------------------------------------------------------


def test_regular_session_fires_every_capture_slot_and_no_other_minute(calendar):
    runner, instants = _simulate(calendar, et(REGULAR, 8, 0, 17, 250000), et(REGULAR, 18, 0))

    # One cycle per minute from the open through the option close, 406 slots, in order.
    expected = _slots(et(REGULAR, 9, 30), et(REGULAR, 16, 15))
    assert len(expected) == 406
    assert runner.slots == expected

    # The loop ticked every minute from 08:01 through 18:00 and idled on the ones off the
    # window. 600 ticks, plus the pre-loop observation. So it idled before the open and
    # after the option close instead of returning.
    assert len(instants) == 601


def test_non_session_day_fires_nothing_and_keeps_ticking(calendar):
    runner, instants = _simulate(calendar, et(HOLIDAY, 8, 0), et(HOLIDAY, 18, 0))
    assert runner.calls == []
    assert len(instants) == 601


def test_early_close_last_cycle_is_the_13_15_slot(calendar):
    runner, _ = _simulate(calendar, et(EARLY_CLOSE, 9, 0), et(EARLY_CLOSE, 14, 0))
    assert runner.slots == _slots(et(EARLY_CLOSE, 9, 30), et(EARLY_CLOSE, 13, 15))
    assert runner.slots[-1] == et(EARLY_CLOSE, 13, 15)


# -- 2. minute-top alignment -----------------------------------------------------


def test_each_sleep_lands_on_a_minute_top(calendar):
    start = et(REGULAR, 9, 28, 17, 250000)
    _, instants = _simulate(calendar, start, et(REGULAR, 9, 35))

    # Before the first sleep the clock is wherever it started, mid-minute.
    assert instants[0] == start.astimezone(UTC)
    # Every tick after that lands exactly on a minute top, one minute apart.
    ticks = instants[1:]
    assert ticks[0] == et(REGULAR, 9, 29).astimezone(UTC)
    for instant in ticks:
        assert (instant.second, instant.microsecond) == (0, 0)
    for earlier, later in zip(ticks, ticks[1:], strict=False):
        assert later - earlier == timedelta(minutes=1)


def test_loop_keeps_ticking_outside_the_window_rather_than_returning(calendar):
    # Past the option close on a session day. Ten ticks, no cycles, no return until the
    # bound says so.
    runner, instants = _simulate(calendar, et(REGULAR, 17, 0), et(REGULAR, 17, 10))
    assert runner.calls == []
    assert len(instants) == 11


def test_a_slow_cycle_skips_no_minute(calendar):
    # The 09:30 cycle is still running at 09:32, two minute tops later. Before marketlake
    # #565 the loop waited for it, realigned past 09:31, and marked that minute skipped.
    # Now every minute fires on time and nothing is reported.
    release = threading.Event()
    clock = ManualClock(start=et(REGULAR, 9, 29).astimezone(UTC), grace=HELD_GRACE_SECONDS)
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock, hold={et(REGULAR, 9, 30): release})
    observed: list[datetime] = []
    reports: list[list[datetime]] = []

    def on_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 9, 32):
            release.set()

    hooks = daemon.DaemonHooks(
        on_tick=on_tick,
        on_cycle=lambda slot, result: observed.append(slot),
        on_skipped=lambda slots: reports.append(list(slots)),
    )
    end = et(REGULAR, 9, 34).astimezone(UTC)
    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=lambda: clock.now() < end
    )

    assert runner.slots == _slots(et(REGULAR, 9, 30), et(REGULAR, 9, 34))
    # Each fired at its own minute top, while the 09:30 cycle was still running.
    assert runner.floors == runner.slots
    assert reports == []
    assert observed == runner.slots


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 8, 24, 13, 30, 0, tzinfo=UTC), 60.0),  # on a top: a full minute
        (datetime(2026, 8, 24, 13, 30, 17, 250000, tzinfo=UTC), 42.75),
        (datetime(2026, 8, 24, 13, 30, 59, 999999, tzinfo=UTC), 0.000001),
    ],
)
def test_next_minute_top(now: datetime, expected: float):
    top = daemon.next_minute_top(now)
    assert top == datetime(2026, 8, 24, 13, 31, tzinfo=UTC)
    assert (top - now).total_seconds() == pytest.approx(expected)


class _ShortWakeClock(ManualClock):
    """A manual clock whose one sleep across ``short_of`` wakes ``shortfall`` before it.

    ``SystemClock.sleep`` counts elapsed time on the monotonic timer while ``now`` reads
    the wall clock, and on 2026-09-11 two sleeps ended just before their minute top
    (marketlake #572). This models one such sleep. Every other sleep lands where asked.
    """

    def __init__(self, start: datetime, short_of: datetime | None, shortfall: timedelta) -> None:
        super().__init__(start)
        self._short_of = short_of
        self._shortfall = shortfall

    def sleep(self, seconds: float) -> None:
        now = self.now()
        end = now + timedelta(seconds=seconds)
        if self._short_of is not None and now < self._short_of <= end:
            end = self._short_of - self._shortfall
            self._short_of = None
        self.advance((end - now).total_seconds())


# The 2026-09-11 wakes were short by tens of milliseconds. A microsecond is the least a
# wall clock can read short by, so a re-sleep that tolerates any shortfall at all fails it.
SHORTFALLS = [timedelta(milliseconds=5), timedelta(microseconds=1)]


def _early_wake_run(
    calendar: FakeCalendar,
    start: datetime,
    end: datetime,
    short_of: datetime | None,
    shortfall: timedelta = SHORTFALLS[0],
) -> tuple[_RecordingRunner, list[list[datetime]]]:
    """Run the loop with the real close tags, hooks that take 30 ms, and one early wake.

    Returns the runner and every report handed to ``on_skipped``. ``short_of=None`` is the
    same run with no early wake. The 30 ms stands for the hooks' real cost, which is what
    carried the 2026-09-11 cycles' own clock reads past the top the loop read short of.
    """
    clock = _ShortWakeClock(start.astimezone(UTC), short_of, shortfall)
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock)
    skipped: list[list[datetime]] = []
    hooks = daemon.DaemonHooks(
        on_tick=lambda slot: clock.advance(0.030),
        close_tag_for=session_clock.close_tag_at,
        on_skipped=lambda slots: skipped.append(list(slots)),
    )
    end_utc = end.astimezone(UTC)
    daemon.run_loop(
        session_clock,
        runner,
        clock=clock,
        hooks=hooks,
        should_continue=lambda: clock.now() < end_utc,
    )
    return runner, skipped


@pytest.mark.parametrize("shortfall", SHORTFALLS, ids=["5ms", "1us"])
@pytest.mark.parametrize(
    "hour,minute",
    [(9, 30), (16, 0), (16, 1), (16, 15), (16, 16)],
    ids=["open", "equity-close", "after-equity-close", "option-close", "after-option-close"],
)
def test_a_wake_just_short_of_a_boundary_top_changes_no_cycle(calendar, hour, minute, shortfall):
    # Before the fix, each boundary lost something different: the open minute outright,
    # a close tag, or a close tag moved onto the minute after. So every one is compared
    # with the same run left undisturbed rather than with a hand-written list.
    boundary = et(REGULAR, hour, minute)
    start = boundary - timedelta(minutes=2, seconds=30)
    end = boundary + timedelta(minutes=3)

    clean, clean_skips = _early_wake_run(calendar, start, end, None)
    early, early_skips = _early_wake_run(calendar, start, end, boundary, shortfall)

    assert clean.calls, "the window around the boundary ran no cycle"
    assert early.calls == clean.calls
    assert early_skips == clean_skips == []
    # Each cycle's own clock read agrees with the slot it was handed.
    assert early.floors == early.slots


@pytest.mark.parametrize(
    "hour,minute,tag,phase",
    [(16, 0, "spot_close", None), (16, 15, "option_close", POST_EQUITY_CLOSE)],
)
def test_a_wake_just_short_of_a_close_keeps_the_close_tag_on_its_minute(
    calendar, hour, minute, tag, phase
):
    close = et(REGULAR, hour, minute)
    runner, skipped = _early_wake_run(
        calendar, close - timedelta(minutes=2, seconds=30), close + timedelta(minutes=2), close
    )

    assert [call for call in runner.calls if call[1] == tag] == [(close, tag, phase)]
    assert skipped == []


def test_hooks_that_run_past_the_next_top_still_hand_the_cycle_its_own_slot(calendar):
    # The 16:00 tick's hooks take 61 seconds. The cycle still runs for 16:00 with its
    # close tag, although a clock read of its own would land in 16:01. The minute the
    # hooks ran through had no cycle, and it is the one reported skipped.
    clock = ManualClock(start=et(REGULAR, 15, 58, 30).astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock)
    skipped: list[list[datetime]] = []

    def slow_close_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 16, 0):
            clock.advance(61)

    hooks = daemon.DaemonHooks(
        on_tick=slow_close_tick,
        close_tag_for=session_clock.close_tag_at,
        on_skipped=lambda slots: skipped.append(list(slots)),
    )
    end = et(REGULAR, 16, 3).astimezone(UTC)
    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=lambda: clock.now() < end
    )

    assert runner.calls[:2] == [
        (et(REGULAR, 15, 59), None, None),
        (et(REGULAR, 16, 0), "spot_close", None),
    ]
    assert runner.floors[1] == et(REGULAR, 16, 1)
    assert skipped == [[et(REGULAR, 16, 1)]]


def test_a_clock_stepped_back_after_a_tick_serves_no_minute_twice(calendar):
    # Right after the 16:00 tick the wall clock steps 90 seconds back, into 15:58. A loop
    # that took its next top from that reading alone would serve 15:59 and 16:00 again,
    # and 16:00 would carry ``spot_close`` on two cycles, which the loader refuses as a
    # close of record. The loop waits for 16:01 instead.
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 15, 58, 30),
        et(REGULAR, 16, 4),
        stall=(et(REGULAR, 16, 0), -90.0),
    )

    assert runner.slots == _slots(et(REGULAR, 15, 59), et(REGULAR, 16, 4))


class _LandingClock(CostlyClock):
    """A manual clock whose first sleep lands at ``lands_at``, and whose reads cost 1 us.

    The landing models a long overshoot, like a machine that slept, waking just before a
    minute top. The cost per read is what lets two back-to-back reads fall on either side
    of that top.
    """

    def __init__(self, start: datetime, lands_at: datetime) -> None:
        super().__init__(start)
        self._lands_at: datetime | None = lands_at

    def sleep(self, seconds: float) -> None:
        if self._lands_at is not None:
            self.set(self._lands_at)
            self._lands_at = None
            return
        super().sleep(seconds)


@pytest.mark.parametrize(
    "lands_before,served,unserved",
    [
        ((9, 30), None, (9, 30)),  # the open minute is marked skipped, never lost silently
        ((16, 16), (16, 15), (16, 16)),  # the option close is served, 16:16 is not
    ],
    ids=["open", "after-option-close"],
)
def test_a_wake_landing_just_before_a_top_reads_its_phase_from_its_slot(
    calendar, lands_before, served, unserved
):
    # The loop reads the phase and the slot back to back. If each reads the clock, a wake
    # landing 2 us before a top lets the first fall before it and the second after, and
    # the loop then decides capture on one minute and serves another.
    top = et(REGULAR, *lands_before)
    clock = _LandingClock(
        (top - timedelta(minutes=5, seconds=30)).astimezone(UTC),
        (top - timedelta(microseconds=2)).astimezone(UTC),
    )
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock)
    skipped: list[datetime] = []
    hooks = daemon.DaemonHooks(
        close_tag_for=session_clock.close_tag_at, on_skipped=lambda slots: skipped.extend(slots)
    )
    end = (top + timedelta(minutes=2)).astimezone(UTC)
    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=lambda: clock.now() < end
    )

    for slot, _tag, phase in runner.calls:
        assert session_clock.phase_at(slot) in CAPTURE_PHASES, slot
        assert phase == (POST_EQUITY_CLOSE if slot > et(REGULAR, 16, 0) else None), slot
    if served is not None:
        assert et(REGULAR, *served) in runner.slots
    missing = et(REGULAR, *unserved)
    assert missing not in runner.slots
    if session_clock.phase_at(missing) in CAPTURE_PHASES:
        assert missing in skipped


# -- 3. the hooks -----------------------------------------------------------------


def test_on_start_is_called_once_before_any_cycle(calendar):
    events: list[str] = []
    hooks = daemon.DaemonHooks(
        on_start=lambda slot: events.append("start"),
        on_cycle=lambda slot, result: events.append("cycle"),
    )
    _simulate(calendar, et(REGULAR, 9, 28), et(REGULAR, 9, 33), hooks=hooks)
    assert events == ["start", "cycle", "cycle", "cycle", "cycle"]


def test_on_start_fires_even_when_no_cycle_ever_does(calendar):
    events: list[str] = []
    hooks = daemon.DaemonHooks(on_start=lambda slot: events.append("start"))
    _simulate(calendar, et(HOLIDAY, 12, 0), et(HOLIDAY, 12, 5), hooks=hooks)
    assert events == ["start"]


def test_close_tag_for_is_asked_once_per_capture_slot_and_its_answer_is_passed_through(
    calendar,
):
    asked: list[datetime] = []
    tags = {et(REGULAR, 16, 0): "spot_close", et(REGULAR, 16, 15): "option_close"}

    def close_tag_for(slot: datetime) -> str | None:
        asked.append(slot)
        return tags.get(slot)

    hooks = daemon.DaemonHooks(close_tag_for=close_tag_for)
    runner, _ = _simulate(calendar, et(REGULAR, 15, 55), et(REGULAR, 16, 20), hooks=hooks)

    # Asked exactly once per capture slot, in slot order, and never off the window.
    assert asked == _slots(et(REGULAR, 15, 56), et(REGULAR, 16, 15))
    assert asked == runner.slots
    # The answer lands on that cycle and no other.
    by_slot = {slot: tag for slot, tag, _phase in runner.calls}
    assert by_slot[et(REGULAR, 16, 0)] == "spot_close"
    assert by_slot[et(REGULAR, 16, 15)] == "option_close"
    assert all(tag is None for slot, tag in by_slot.items() if slot not in tags)


def test_on_cycle_receives_every_result_in_order(calendar):
    observed: list[tuple[datetime, CycleResult]] = []
    hooks = daemon.DaemonHooks(on_cycle=lambda slot, result: observed.append((slot, result)))
    runner, _ = _simulate(calendar, et(REGULAR, 16, 10), et(REGULAR, 16, 20), hooks=hooks)

    # The five slots 16:11 through 16:15 fired; the observer saw the same five results,
    # the very objects the runner returned, in the same order, each with its slot.
    assert [slot for slot, _ in observed] == runner.slots
    assert [result for _, result in observed] == runner.results
    assert all(a is b for (_, a), b in zip(observed, runner.results, strict=True))
    assert len(observed) == 5


def test_default_hooks_are_no_ops():
    hooks = daemon.DaemonHooks()
    assert hooks.on_start(et(REGULAR, 9, 29)) is None
    assert hooks.close_tag_for(et(REGULAR, 16, 0)) is None
    result = CycleResult(snap_ts=et(REGULAR, 16, 0), segments=())
    assert hooks.on_cycle(et(REGULAR, 16, 0), result) is None
    assert hooks.on_skipped([et(REGULAR, 16, 1)]) is None
    assert hooks.on_missed([et(REGULAR, 16, 1)]) is None


# -- 6. skipped slots --------------------------------------------------------------


def test_a_one_slot_stall_reports_exactly_that_slot(calendar):
    # The loop stalls 90 seconds after the 10:00 tick, to 10:01:30. It realigns to 10:02,
    # and that tick reports the one slot it stepped over.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 9, 59),
        et(REGULAR, 10, 4),
        hooks=hooks,
        stall=(et(REGULAR, 10, 0), 90),
    )
    assert runner.slots == [
        et(REGULAR, 10, 0),
        et(REGULAR, 10, 2),
        et(REGULAR, 10, 3),
        et(REGULAR, 10, 4),
    ]
    assert reports == [[et(REGULAR, 10, 1)]]


def test_a_two_slot_stall_reports_both_in_order(calendar):
    # The loop stalls 150 seconds after the 10:00 tick, to 10:02:30. The 10:03 tick
    # reports both.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 9, 59),
        et(REGULAR, 10, 4),
        hooks=hooks,
        stall=(et(REGULAR, 10, 0), 150),
    )
    assert runner.slots == [et(REGULAR, 10, 0), et(REGULAR, 10, 3), et(REGULAR, 10, 4)]
    assert reports == [[et(REGULAR, 10, 1), et(REGULAR, 10, 2)]]


def test_normal_cadence_never_calls_on_skipped(calendar):
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(calendar, et(REGULAR, 8, 0), et(REGULAR, 18, 0), hooks=hooks)
    assert len(runner.slots) == 406
    assert reports == []


def test_a_quick_starts_first_tick_never_calls_on_skipped(calendar):
    # Started mid-session, mid-minute. The loop seeds its previous slot with 10:00, the
    # minute it started in, and hands that minute to startup gap-marking. The first tick
    # is 10:01, adjacent to the seed, so nothing is missed. Every minute through 10:00
    # belongs to startup gap-marking, not the loop.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(calendar, et(REGULAR, 10, 0, 17), et(REGULAR, 10, 3), hooks=hooks)
    assert runner.slots == [et(REGULAR, 10, 1), et(REGULAR, 10, 2), et(REGULAR, 10, 3)]
    assert reports == []


@pytest.mark.parametrize("reads_short", [1, 2, 3])
def test_on_start_is_handed_the_slot_the_loop_seeded_and_the_first_skip_follows_it(
    calendar, reads_short
):
    # Every read costs 1 us, and the clock starts a few reads short of 10:01. The loop's
    # seed read falls inside 10:00, the minute the daemon started in. A second read taken
    # for ``on_start`` can fall past the top and hand it 10:01, and startup marking would
    # then claim 10:01 while the first tick hands it to ``on_skipped`` too. On a clock
    # that stands still between reads, nothing tells that second read from the first.
    # One read short puts the very next read past the top. Two and three leave room for
    # reads added before the handoff later.
    top = et(REGULAR, 10, 1)
    clock = CostlyClock((top - timedelta(microseconds=reads_short)).astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    handed: list[datetime] = []
    reports: list[list[datetime]] = []

    def on_start(slot: datetime) -> None:
        handed.append(slot)
        # A startup pass reads the clock as it works. These reads carry it past 10:01, so
        # the first tick lands on 10:02 and has a minute to report.
        for _ in range(reads_short):
            clock.now()

    hooks = daemon.DaemonHooks(
        on_start=on_start, on_skipped=lambda slots: reports.append(list(slots))
    )
    end = et(REGULAR, 10, 2, 30).astimezone(UTC)
    daemon.run_loop(
        session_clock,
        _RecordingRunner(clock),
        clock=clock,
        hooks=hooks,
        should_continue=lambda: clock.now() < end,
    )

    assert handed == [et(REGULAR, 10, 0)]
    assert reports == [[handed[0] + daemon.TICK]]


def test_a_run_spanning_two_session_dates_does_not_report_the_overnight_minutes(calendar):
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(calendar, et(REGULAR, 16, 9, 30), et(NEXT_DAY, 9, 35), hooks=hooks)
    assert runner.slots == _slots(et(REGULAR, 16, 10), et(REGULAR, 16, 15)) + _slots(
        et(NEXT_DAY, 9, 30), et(NEXT_DAY, 9, 35)
    )
    assert reports == []


def test_a_night_jump_reports_nothing(calendar):
    # The clock jumps from Monday 23:00 to Tuesday 00:30 between ticks. The span crosses
    # midnight but touches no capture slot on either day, so nothing is reported.
    hooks, reports = _skip_recorder()
    jump = (et(NEXT_DAY, 0, 30) - et(REGULAR, 23, 0)).total_seconds()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 22, 59),
        et(NEXT_DAY, 0, 33),
        hooks=hooks,
        stall=(et(REGULAR, 23, 0), jump),
    )
    assert runner.slots == []
    assert reports == []


def test_a_skip_past_the_option_close_reports_only_the_capture_slots_inside_the_window(
    calendar,
):
    # The loop stalls four minutes after the 16:13 tick, to 16:17. The next tick, 16:18,
    # is past the option close and fires nothing, but it still reports the skip: 16:14
    # and 16:15 are capture slots, 16:16 and 16:17 are not.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 16, 12, 30),
        et(REGULAR, 16, 20),
        hooks=hooks,
        stall=(et(REGULAR, 16, 13), 240),
    )
    assert runner.slots == [et(REGULAR, 16, 13)]
    assert reports == [[et(REGULAR, 16, 14), et(REGULAR, 16, 15)]]


def test_a_stall_across_the_open_reports_the_missed_opening_slots(calendar):
    # No cycle has fired yet today when the clock jumps from 09:10 to 09:50:30, the way
    # a sleeping laptop would. The memory covers every tick, so the 09:51 tick reports
    # 09:30 through 09:50. Nothing else can see this skip: the daemon never restarted,
    # so startup gap-marking never runs.
    hooks, reports = _skip_recorder()
    jump = (et(REGULAR, 9, 50, 30) - et(REGULAR, 9, 10)).total_seconds()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 9, 5),
        et(REGULAR, 9, 53),
        hooks=hooks,
        stall=(et(REGULAR, 9, 10), jump),
    )
    assert runner.slots == [et(REGULAR, 9, 51), et(REGULAR, 9, 52), et(REGULAR, 9, 53)]
    assert reports == [_slots(et(REGULAR, 9, 30), et(REGULAR, 9, 50))]


def _stall_from(
    calendar: FakeCalendar, asleep_at: datetime, awake_at: datetime, end: datetime
) -> tuple[_RecordingRunner, list[list[datetime]]]:
    """Run from two minutes before ``asleep_at``, jump the clock to ``awake_at`` there."""
    hooks, reports = _skip_recorder()
    jump = (awake_at - asleep_at).total_seconds()
    start = asleep_at - timedelta(minutes=2)
    runner, _ = _simulate(calendar, start, end, hooks=hooks, stall=(asleep_at, jump))
    return runner, reports


def test_a_stall_from_monday_afternoon_to_tuesday_morning_reports_both_days(calendar):
    # The lid closes at Monday 15:00 and opens at Tuesday 09:39:30 with the daemon still
    # alive. It never restarted, so startup gap-marking never runs. The 09:40 tick
    # reports Monday's tail then Tuesday's head, in order, and nothing in between.
    runner, reports = _stall_from(
        calendar, et(REGULAR, 15, 0), et(NEXT_DAY, 9, 39, 30), et(NEXT_DAY, 9, 41)
    )
    assert runner.slots == [
        et(REGULAR, 14, 59),
        et(REGULAR, 15, 0),
        et(NEXT_DAY, 9, 40),
        et(NEXT_DAY, 9, 41),
    ]
    assert reports == [
        _slots(et(REGULAR, 15, 1), et(REGULAR, 16, 15))
        + _slots(et(NEXT_DAY, 9, 30), et(NEXT_DAY, 9, 39))
    ]


def test_a_stall_across_a_weekend_reports_fridays_tail_and_mondays_head_only(calendar):
    runner, reports = _stall_from(
        calendar, et(FRIDAY, 15, 0), et(REGULAR, 9, 39, 30), et(REGULAR, 9, 40)
    )
    assert runner.slots == [et(FRIDAY, 14, 59), et(FRIDAY, 15, 0), et(REGULAR, 9, 40)]
    assert reports == [
        _slots(et(FRIDAY, 15, 1), et(FRIDAY, 16, 15))
        + _slots(et(REGULAR, 9, 30), et(REGULAR, 9, 39))
    ]
    # Nothing on the Saturday or the Sunday.
    assert {slot.date() for slot in reports[0]} == {FRIDAY, REGULAR}


def test_a_stall_across_a_monday_holiday_reports_nothing_for_the_holiday(calendar):
    runner, reports = _stall_from(
        calendar,
        et(LABOR_FRIDAY, 15, 0),
        et(LABOR_TUESDAY, 9, 39, 30),
        et(LABOR_TUESDAY, 9, 40),
    )
    assert runner.slots == [
        et(LABOR_FRIDAY, 14, 59),
        et(LABOR_FRIDAY, 15, 0),
        et(LABOR_TUESDAY, 9, 40),
    ]
    assert reports == [
        _slots(et(LABOR_FRIDAY, 15, 1), et(LABOR_FRIDAY, 16, 15))
        + _slots(et(LABOR_TUESDAY, 9, 30), et(LABOR_TUESDAY, 9, 39))
    ]
    assert LABOR_DAY not in {slot.date() for slot in reports[0]}


def test_waking_on_a_saturday_reports_fridays_tail_only(calendar):
    # The current day is not a session, and the loop still reports the prior session
    # day's tail. That is why no non-session early return may sit before the day-walk.
    runner, reports = _stall_from(
        calendar, et(FRIDAY, 15, 0), et(SATURDAY, 10, 0, 30), et(SATURDAY, 10, 3)
    )
    assert runner.slots == [et(FRIDAY, 14, 59), et(FRIDAY, 15, 0)]
    assert reports == [_slots(et(FRIDAY, 15, 1), et(FRIDAY, 16, 15))]


def test_skipped_slots_walks_the_minutes_inside_the_window(calendar):
    bounds = SessionClock(ManualClock(start=et(REGULAR, 12, 0).astimezone(UTC)), calendar)
    bounds = bounds.bounds(REGULAR)
    # Strictly between, clipped to the window on both ends, adjacent yields nothing.
    assert daemon.skipped_slots(bounds, et(REGULAR, 16, 13), et(REGULAR, 16, 18)) == [
        et(REGULAR, 16, 14),
        et(REGULAR, 16, 15),
    ]
    assert daemon.skipped_slots(bounds, et(REGULAR, 9, 27), et(REGULAR, 9, 33)) == [
        et(REGULAR, 9, 30),
        et(REGULAR, 9, 31),
        et(REGULAR, 9, 32),
    ]
    assert daemon.skipped_slots(bounds, et(REGULAR, 10, 0), et(REGULAR, 10, 1)) == []
    assert daemon.skipped_slots(bounds, et(REGULAR, 10, 0), et(REGULAR, 10, 0)) == []
    assert daemon.skipped_slots(bounds, et(REGULAR, 8, 0), et(REGULAR, 9, 0)) == []


# -- 7. each minute's cycle on its own thread ---------------------------------------


def _held_run(
    calendar: FakeCalendar,
    start: datetime,
    should_continue: Callable[[ManualClock], bool],
    *,
    hold: dict[datetime, threading.Event],
    on_tick: Callable[[datetime], None] = lambda slot: None,
    clock: ManualClock | None = None,
    log_starts: bool = False,
) -> tuple[_RecordingRunner, list[tuple[str, datetime]]]:
    """Run the loop with held cycles, and return the runner and what each hook saw, in order.

    Each entry is ``("tick", slot)``, ``("cycle", slot)``, ``("skipped", slot)`` or
    ``("missed", slot)``, so a case can say which hook ran first. ``on_tick`` runs after
    the tick is logged. ``log_starts`` adds ``("start", slot)`` as each cycle starts. That
    entry is logged on the cycle's own thread, so it is ordered against what the loop did
    before starting the cycle and against nothing after.
    """
    clock = clock or ManualClock(start=start.astimezone(UTC), grace=HELD_GRACE_SECONDS)
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock, hold=hold)
    events: list[tuple[str, datetime]] = []

    def tick(slot: datetime) -> None:
        events.append(("tick", slot))
        on_tick(slot)

    def logged(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        events.append(("start", slot))
        return runner(slot=slot, close_tag=close_tag, session_phase=session_phase)

    hooks = daemon.DaemonHooks(
        on_tick=tick,
        on_cycle=lambda slot, result: events.append(("cycle", slot)),
        on_skipped=lambda slots: events.extend(("skipped", slot) for slot in slots),
        on_missed=lambda slots: events.extend(("missed", slot) for slot in slots),
    )
    daemon.run_loop(
        session_clock,
        logged if log_starts else runner,
        clock=clock,
        hooks=hooks,
        should_continue=lambda: should_continue(clock),
    )
    return runner, events


def _until(end: datetime) -> Callable[[ManualClock], bool]:
    """A ``should_continue`` that runs until the clock reaches ``end``."""
    end_utc = end.astimezone(UTC)
    return lambda clock: clock.now() < end_utc


def test_a_held_cycle_does_not_hold_the_next_minute(calendar):
    # The 10:00 cycle is still running at 10:01 and 10:02. Each later minute fires at its
    # own top, nothing is skipped, and the 10:01 cycle, finished long before, waits for
    # the 10:00 one so the observer sees them in slot order. A loop that called the cycle
    # inline would wait on 10:00 forever, since only the 10:02 tick releases it.
    release = threading.Event()

    def on_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 10, 2):
            release.set()

    runner, events = _held_run(
        calendar,
        et(REGULAR, 9, 59, 30),
        _until(et(REGULAR, 10, 2)),
        hold={et(REGULAR, 10, 0): release},
        on_tick=on_tick,
    )

    fired = [et(REGULAR, 10, 0), et(REGULAR, 10, 1), et(REGULAR, 10, 2)]
    assert runner.slots == fired
    assert runner.floors == fired
    assert [slot for kind, slot in events if kind == "skipped"] == []
    assert [slot for kind, slot in events if kind == "cycle"] == fired
    # Nothing was handed on before the 10:00 cycle was released.
    assert events.index(("cycle", et(REGULAR, 10, 1))) > events.index(("tick", et(REGULAR, 10, 2)))


def test_a_cycle_is_handed_on_when_it_finishes_rather_than_at_the_next_tick(calendar):
    # The 10:00 cycle finishes 20 seconds into its minute. The watchdog and the dead-man
    # ride ``on_cycle``, so the loop hands the result on then, while it waits for 10:01.
    release = threading.Event()

    class _FinishesAt20s(ManualClock):
        def wait(self, futures, until):
            if not release.is_set() and until is not None:
                self.advance(20)
                release.set()
            return super().wait(futures, until)

    clock = _FinishesAt20s(start=et(REGULAR, 9, 59, 30).astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock, hold={et(REGULAR, 10, 0): release})
    seen: list[tuple[str, datetime]] = []
    hooks = daemon.DaemonHooks(
        on_tick=lambda slot: seen.append(("tick", slot)),
        on_cycle=lambda slot, result: seen.append(("cycle", clock.now())),
    )
    end = et(REGULAR, 10, 1).astimezone(UTC)
    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=lambda: clock.now() < end
    )

    assert seen == [
        ("tick", et(REGULAR, 10, 0)),
        ("cycle", et(REGULAR, 10, 0, 20).astimezone(UTC)),
        ("tick", et(REGULAR, 10, 1)),
        # The last cycle, handed on as the loop leaves.
        ("cycle", et(REGULAR, 10, 1).astimezone(UTC)),
    ]


def test_an_on_cycle_that_runs_past_the_top_fires_that_minute_late_rather_than_skipping_it(
    calendar,
):
    # The 10:00 result is handed on while the loop waits for 10:01, and its hook runs to
    # 10:01:10. Before marketlake #565 the loop computed its next top after the hook,
    # realigned to 10:02, and marked 10:01 skipped. Now the top was fixed before the hook
    # ran, so the loop reads 10:01 as soon as the hook returns and fires it ten seconds
    # late, with ten seconds less of its bound left.
    clock = ManualClock(start=et(REGULAR, 9, 59, 30).astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock)
    fired_at: list[datetime] = []
    reports: list[list[datetime]] = []

    def on_cycle(slot: datetime, result: CycleResult) -> None:
        if slot == et(REGULAR, 10, 0):
            clock.advance(70)

    def on_tick(slot: datetime) -> None:
        fired_at.append(clock.now())

    hooks = daemon.DaemonHooks(
        on_tick=on_tick,
        on_cycle=on_cycle,
        on_skipped=lambda slots: reports.append(list(slots)),
    )
    end = et(REGULAR, 10, 2).astimezone(UTC)
    daemon.run_loop(
        session_clock, runner, clock=clock, hooks=hooks, should_continue=lambda: clock.now() < end
    )

    assert runner.slots == [et(REGULAR, 10, 0), et(REGULAR, 10, 1), et(REGULAR, 10, 2)]
    assert fired_at[1] == et(REGULAR, 10, 1, 10).astimezone(UTC)
    assert reports == []


def test_a_cycle_that_raises_ends_the_loop_after_the_cycles_behind_it_finish(calendar):
    # The 10:00 cycle raises once the 10:01 tick releases it, while the 10:01 cycle is
    # still running. The loop waits for 10:01, so its rows are whole on disk for the
    # successor's startup marking, hands neither on, and raises.
    class _Boom(Exception):
        pass

    first, second, second_done = threading.Event(), threading.Event(), threading.Event()

    def cycle(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        if slot == et(REGULAR, 10, 0):
            assert first.wait(HOLD_TIMEOUT_SECONDS)
            # Released well after the raise, so a loop that raised at once would find the
            # 10:01 cycle still running.
            threading.Timer(0.5, second.set).start()
            raise _Boom
        assert second.wait(HOLD_TIMEOUT_SECONDS)
        second_done.set()
        return CycleResult(snap_ts=slot.astimezone(UTC), segments=())

    def on_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 10, 1):
            first.set()

    clock = ManualClock(start=et(REGULAR, 9, 59, 30).astimezone(UTC), grace=HELD_GRACE_SECONDS)
    handed: list[datetime] = []
    hooks = daemon.DaemonHooks(on_tick=on_tick, on_cycle=lambda slot, result: handed.append(slot))
    end = et(REGULAR, 10, 5).astimezone(UTC)

    with pytest.raises(_Boom):
        daemon.run_loop(
            SessionClock(clock, calendar),
            cycle,
            clock=clock,
            hooks=hooks,
            should_continue=lambda: clock.now() < end,
        )

    assert second_done.is_set(), "the loop raised before the 10:01 cycle finished"
    assert handed == []


def test_a_stall_reaches_on_missed_after_the_cycle_that_ran_before_it(calendar):
    # The loop stalls right after the 10:00 tick, with that cycle still running, and
    # wakes at 10:02:30. The 10:03 tick hands the skipped minutes to ``on_skipped`` and
    # starts its own cycle without waiting for 10:00. The stall reaches ``on_missed`` only
    # once the 10:00 cycle has been handed on. Handed on after the skipped minutes, a
    # cycle that landed data would reset the watchdog counters the stall raised.
    release = threading.Event()
    stalled: list[bool] = []
    end = et(REGULAR, 10, 3).astimezone(UTC)

    def should_continue(clock: ManualClock) -> bool:
        if not stalled and clock.now() >= et(REGULAR, 10, 0):
            stalled.append(True)
            clock.advance(150)
            # Released after the loop wakes, so the cycle is still running at the tick.
            threading.Timer(0.5, release.set).start()
        return clock.now() < end

    _, events = _held_run(
        calendar,
        et(REGULAR, 9, 59, 30),
        should_continue,
        hold={et(REGULAR, 10, 0): release},
        log_starts=True,
    )

    # Each start is logged on its cycle's thread, which the loop starts after the tick's
    # hooks, so the 10:03 start follows the skipped minutes and nothing else is ordered
    # against it.
    assert events.index(("skipped", et(REGULAR, 10, 2))) < events.index(
        ("start", et(REGULAR, 10, 3))
    )
    assert [event for event in events if event[0] != "start"] == [
        ("tick", et(REGULAR, 10, 0)),
        ("tick", et(REGULAR, 10, 3)),
        ("skipped", et(REGULAR, 10, 1)),
        ("skipped", et(REGULAR, 10, 2)),
        ("cycle", et(REGULAR, 10, 0)),
        ("missed", et(REGULAR, 10, 1)),
        ("missed", et(REGULAR, 10, 2)),
        ("cycle", et(REGULAR, 10, 3)),
    ]


def test_a_stall_with_nothing_ahead_of_it_reaches_on_missed_before_on_tick(calendar):
    # The loop stalls right after the 10:00 tick and wakes at 10:02:30, and the 10:00
    # cycle finishes during the wait, so nothing is in flight at the 10:03 tick. The
    # stall's entry joins the queue before the tick hands on, so ``on_missed`` runs inside
    # that tick, ahead of ``on_tick``, as the module docstring promises. The grace is long
    # so the wait always sees the 10:00 cycle finish. It costs nothing, because the wait
    # returns as soon as the cycle does.
    stalled: list[bool] = []
    end = et(REGULAR, 10, 3).astimezone(UTC)

    def should_continue(clock: ManualClock) -> bool:
        if not stalled and clock.now() >= et(REGULAR, 10, 0):
            stalled.append(True)
            clock.advance(150)
        return clock.now() < end

    _, events = _held_run(
        calendar,
        et(REGULAR, 9, 59, 30),
        should_continue,
        hold={},
        clock=ManualClock(start=et(REGULAR, 9, 59, 30).astimezone(UTC), grace=5.0),
    )

    assert events == [
        ("tick", et(REGULAR, 10, 0)),
        ("cycle", et(REGULAR, 10, 0)),
        ("missed", et(REGULAR, 10, 1)),
        ("missed", et(REGULAR, 10, 2)),
        ("tick", et(REGULAR, 10, 3)),
        ("skipped", et(REGULAR, 10, 1)),
        ("skipped", et(REGULAR, 10, 2)),
        ("cycle", et(REGULAR, 10, 3)),
    ]


def test_a_cycle_held_across_a_stall_does_not_hold_the_minutes_after_it(calendar):
    # The 10:00 cycle stands for one queued on the lake-root lock. It is held until the
    # 10:05 tick. The loop stalls after the 10:00 tick and wakes at 10:02:30, so the 10:03
    # tick has two skipped slots. A tick that waited for every cycle in flight would wait
    # on 10:00 until its hold times out, which is how capture lost twelve minutes on
    # 2026-10-05 (marketlake #644). The 10:03, 10:04 and 10:05 cycles must start at their
    # own tops instead.
    release = threading.Event()
    stalled: list[bool] = []
    end = et(REGULAR, 10, 5).astimezone(UTC)

    def on_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 10, 5):
            release.set()

    def should_continue(clock: ManualClock) -> bool:
        if not stalled and clock.now() >= et(REGULAR, 10, 0):
            stalled.append(True)
            clock.advance(150)
        return clock.now() < end

    runner, events = _held_run(
        calendar,
        et(REGULAR, 9, 59, 30),
        should_continue,
        hold={et(REGULAR, 10, 0): release},
        on_tick=on_tick,
    )

    fired = [et(REGULAR, 10, 0), et(REGULAR, 10, 3), et(REGULAR, 10, 4), et(REGULAR, 10, 5)]
    assert runner.slots == fired
    assert runner.floors == fired
    stall = [et(REGULAR, 10, 1), et(REGULAR, 10, 2)]
    assert [slot for kind, slot in events if kind == "skipped"] == stall
    assert [slot for kind, slot in events if kind == "missed"] == stall
    # The stall waited in the queue for the held cycle, then went on ahead of 10:03.
    assert events.index(("missed", et(REGULAR, 10, 1))) > events.index(
        ("cycle", et(REGULAR, 10, 0))
    )
    assert events.index(("missed", et(REGULAR, 10, 2))) < events.index(
        ("cycle", et(REGULAR, 10, 3))
    )


def test_the_close_plus_five_tick_hands_on_the_option_close_cycle_before_its_hooks(calendar):
    # The 16:15 cycle is still running at 16:20, the close+5 deadline, where ``on_tick``
    # dispatches the guard. The guard refills an option close only when no data row holds
    # it, so it must run after that cycle has finished. The minutes between are off the
    # capture window, so this tick has no skipped slot, and the close+5 deadline alone is
    # what makes it wait.
    release = threading.Event()
    stalled: list[bool] = []
    end = et(REGULAR, 16, 20).astimezone(UTC)

    def should_continue(clock: ManualClock) -> bool:
        if not stalled and clock.now() >= et(REGULAR, 16, 15):
            stalled.append(True)
            clock.advance(270)
            threading.Timer(0.5, release.set).start()
        return clock.now() < end

    _, events = _held_run(
        calendar,
        et(REGULAR, 16, 14, 30),
        should_continue,
        hold={et(REGULAR, 16, 15): release},
    )

    assert events == [
        ("tick", et(REGULAR, 16, 15)),
        ("cycle", et(REGULAR, 16, 15)),
        ("tick", et(REGULAR, 16, 20)),
    ]


def test_a_cycle_still_running_at_the_option_close_does_not_hold_the_close(calendar):
    # The 16:14 cycle is still running at 16:15. Only a tick at or past close+5 waits for
    # the cycles in flight, so the option close still fires at its own top. A wait that
    # began at the option close instead would start 16:15 only once 16:14 finished.
    release = threading.Event()

    def on_tick(slot: datetime) -> None:
        if slot == et(REGULAR, 16, 16):
            release.set()

    runner, _ = _held_run(
        calendar,
        et(REGULAR, 16, 13, 30),
        _until(et(REGULAR, 16, 17)),
        hold={et(REGULAR, 16, 14): release},
        on_tick=on_tick,
    )

    fired = [et(REGULAR, 16, 14), et(REGULAR, 16, 15)]
    assert runner.slots == fired
    assert runner.floors == fired


def test_a_cycle_that_raises_past_exception_still_ends_the_loop(calendar):
    # ``lake.capture`` lets ``SystemExit`` and ``KeyboardInterrupt`` through on purpose. A
    # thread that dropped one would leave its future unsettled forever, the queue would
    # grow a cycle a minute, and the next wait for every cycle would never return. So the
    # cycle's thread keeps any ``BaseException`` and the loop raises it. The loop runs on
    # a thread of its own here, so a regression fails the test rather than hanging it.
    class _Out(BaseException):
        pass

    def cycle(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        raise _Out

    clock = ManualClock(start=et(REGULAR, 9, 59, 30).astimezone(UTC), grace=HELD_GRACE_SECONDS)
    end = et(REGULAR, 10, 5).astimezone(UTC)
    raised: list[BaseException] = []

    def run() -> None:
        try:
            daemon.run_loop(
                SessionClock(clock, calendar),
                cycle,
                clock=clock,
                should_continue=lambda: clock.now() < end,
            )
        except BaseException as exc:  # noqa: BLE001 - the case is what escapes
            raised.append(exc)

    loop = threading.Thread(target=run, daemon=True)
    loop.start()
    loop.join(HOLD_TIMEOUT_SECONDS)

    assert not loop.is_alive(), "the loop never ended"
    assert [type(exc) for exc in raised] == [_Out]


def test_a_cycle_runs_on_a_thread_the_interpreter_waits_for_at_exit(calendar):
    # A hook that raises ends the loop without waiting for the cycles in flight. A
    # non-daemon thread is what lets such a cycle finish its writes before the process
    # exits, rather than being cut off with a segment half written.
    daemon_flags: list[bool] = []

    def cycle(*, slot: datetime, close_tag: str | None, session_phase: str | None) -> CycleResult:
        daemon_flags.append(threading.current_thread().daemon)
        return CycleResult(snap_ts=slot.astimezone(UTC), segments=())

    clock = ManualClock(start=et(REGULAR, 9, 59, 30).astimezone(UTC))
    end = et(REGULAR, 10, 1).astimezone(UTC)
    daemon.run_loop(
        SessionClock(clock, calendar),
        cycle,
        clock=clock,
        should_continue=lambda: clock.now() < end,
    )

    assert daemon_flags == [False, False]


def test_leaving_the_loop_hands_on_the_cycles_still_in_flight(calendar):
    release = threading.Event()
    ticks: list[bool] = []

    def once(clock: ManualClock) -> bool:
        if ticks:
            threading.Timer(0.3, release.set).start()
            return False
        ticks.append(True)
        return True

    _, events = _held_run(
        calendar, et(REGULAR, 9, 59, 30), once, hold={et(REGULAR, 10, 0): release}
    )

    assert events == [("tick", et(REGULAR, 10, 0)), ("cycle", et(REGULAR, 10, 0))]


@pytest.mark.parametrize("shortfall", SHORTFALLS, ids=["5ms", "1us"])
def test_a_wait_on_a_held_cycle_that_wakes_short_of_the_close_changes_no_cycle(calendar, shortfall):
    # The same short wake as above, landing while the loop waits on a cycle still running
    # rather than while it sleeps. The wait gives up at the top on the monotonic timer, so
    # it can end short of it as a sleep can, and the loop reads ``now`` again after it.
    close = et(REGULAR, 16, 0)

    def run(short_of: datetime | None) -> _RecordingRunner:
        release = threading.Event()
        clock = _ShortWakeClock(
            (close - timedelta(minutes=1, seconds=30)).astimezone(UTC), short_of, shortfall
        )
        clock._grace = HELD_GRACE_SECONDS
        session_clock = SessionClock(clock, calendar)
        runner = _RecordingRunner(clock, hold={close - daemon.TICK: release})

        def on_tick(slot: datetime) -> None:
            if slot == close:
                release.set()

        hooks = daemon.DaemonHooks(on_tick=on_tick, close_tag_for=session_clock.close_tag_at)
        end = (close + daemon.TICK).astimezone(UTC)
        daemon.run_loop(
            session_clock,
            runner,
            clock=clock,
            hooks=hooks,
            should_continue=lambda: clock.now() < end,
        )
        return runner

    clean, early = run(None), run(close)

    assert clean.calls == [
        (close - daemon.TICK, None, None),
        (close, "spot_close", None),
        (close + daemon.TICK, None, POST_EQUITY_CLOSE),
    ]
    assert early.calls == clean.calls
    assert early.floors == early.slots


# -- 4. session_phase ------------------------------------------------------------


def test_session_phase_is_post_equity_close_only_past_the_equity_close(calendar):
    runner, _ = _simulate(calendar, et(REGULAR, 9, 0), et(REGULAR, 17, 0))
    by_slot = {slot: phase for slot, _tag, phase in runner.calls}

    # 16:01 through 16:15 carry the enum's value. Every other slot, 09:30 through 16:00,
    # carries null. The 16:00 spot_close slot is still synchronous, so it is null.
    tagged = _slots(et(REGULAR, 16, 1), et(REGULAR, 16, 15))
    assert [slot for slot, phase in by_slot.items() if phase == POST_EQUITY_CLOSE] == tagged
    assert all(phase is None for slot, phase in by_slot.items() if slot not in tagged)
    assert by_slot[et(REGULAR, 16, 0)] is None
    assert POST_EQUITY_CLOSE == "post_equity_close"


def test_session_phase_follows_the_early_close(calendar):
    runner, _ = _simulate(calendar, et(EARLY_CLOSE, 12, 55), et(EARLY_CLOSE, 13, 20))
    by_slot = {slot: phase for slot, _tag, phase in runner.calls}
    assert by_slot[et(EARLY_CLOSE, 13, 0)] is None
    assert [slot for slot, phase in by_slot.items() if phase == POST_EQUITY_CLOSE] == _slots(
        et(EARLY_CLOSE, 13, 1), et(EARLY_CLOSE, 13, 15)
    )


# -- 5. the command-line entry ---------------------------------------------------


def test_build_parser_reads_the_three_paths():
    args = daemon.build_parser().parse_args(
        ["--config", "/c.yaml", "--tickers", "/t.yaml", "--token", "/tok.json"]
    )
    assert (args.config, args.tickers, args.token) == ("/c.yaml", "/t.yaml", "/tok.json")
    defaults = daemon.build_parser().parse_args([])
    assert (defaults.config, defaults.tickers, defaults.token) == (None, None, None)


def test_main_passes_the_paths_to_the_config_entry(tmp_path, monkeypatch):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    seen: dict[str, object] = {}

    def fake_run_loop_from_config(**kwargs) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(daemon, "run_loop_from_config", fake_run_loop_from_config)
    assert daemon.main(["--config", str(config), "--token", "/tok.json"]) == 0
    # Exhaustive, so an argument added to the call is seen here rather than silently.
    assert set(seen) == {
        "config_path",
        "tickers_path",
        "token_path",
        "transport",
        "pinger",
        "compaction_runner",
        "pull_runner",
        "assertion_runner",
    }
    assert seen["config_path"] == str(config)
    assert seen["tickers_path"] is None
    assert seen["token_path"] == "/tok.json"
    # ``daemon.main`` is the only caller in its module that supplies the live three, the
    # two senders from ``outbox`` and the real spawn. This config has no ``role`` key, so
    # it is a primary and the senders are live. If any stops being the real thing, some
    # entry has started defaulting a seam again, or an absent key has started reading as
    # ``shadow``. ``tests/component/test_shadow_role.py`` holds the shadow twin.
    assert isinstance(seen["transport"], NtfyTransport)
    assert isinstance(seen["pinger"], UrllibPinger)
    # The live runner is the one that actually spawns a child process.
    assert seen["compaction_runner"] is daemon._spawn_compaction
    # This config has no ``token_store`` key, so it is the ``file`` laptop, which ran the
    # re-auth itself and pulls nothing.
    assert seen["pull_runner"] is None
    # The topic, not just the class. It is the write credential for the ntfy channel, so
    # the wiring worth covering is which topic reached the transport. Asserting the class
    # alone passes a `main` that ignored --config and read the machine's own config,
    # which is the very asymmetry this PR exists to remove.
    assert seen["transport"]._topic == NTFY_TOPIC
    # The suite runs as macOS, where the holder keeps its default spawn of a real
    # ``caffeinate``. Compared equal to ``None`` rather than dropped from the call, so a
    # Mac handed the Linux no-op runner fails here.
    assert seen["assertion_runner"] is None


@pytest.mark.parametrize(
    ("token_store", "pulls"),
    [("store", True), ("stoer", True), ("both", False), ("file", False)],
)
def test_main_pulls_the_token_only_where_the_token_parameter_is_the_source(
    token_store, pulls, tmp_path, monkeypatch, capsys
):
    """``store`` and a mistyped value pull in auth death, ``file`` and ``both`` never do.

    A ``both`` laptop ran the re-auth that put the parameter, so its own file is already
    the newest token, and a pull there would only spend a ``GetParameter`` it has no grant
    for (marketlake #703). A mistyped value falls to the side that cannot cost a token,
    which is the one that pulls, and says so once at start.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, token_store=token_store)
    seen: dict[str, object] = {}
    monkeypatch.setattr(daemon, "run_loop_from_config", lambda **kwargs: seen.update(kwargs))
    assert daemon.main(["--config", str(config)]) == 0

    if pulls:
        assert seen["pull_runner"] is daemon._spawn_token_pull
    else:
        assert seen["pull_runner"] is None
    lines = capsys.readouterr().err.splitlines()
    unknown = [line for line in lines if "is not 'file', 'both' or 'store'" in line]
    if token_store == "stoer":
        (line,) = unknown
        assert line.startswith("daemon: token_store 'stoer' is not 'file', 'both' or 'store'")
    else:
        assert unknown == []


def test_main_on_linux_passes_a_runner_that_spawns_nothing(tmp_path, monkeypatch, on_linux):
    """A VM never sleeps, so the daemon holds no assertion there.

    The runner is called the way the holder calls it, with ``Popen`` replaced by a
    recorder. ``caffeinate`` is not a guarded program, so a runner that reached the real
    spawn would start one on the laptop running the suite.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    seen: dict[str, object] = {}
    monkeypatch.setattr(daemon, "run_loop_from_config", lambda **kwargs: seen.update(kwargs))
    assert daemon.main(["--config", str(config)]) == 0

    runner = seen["assertion_runner"]
    assert runner is not None
    spawned: list[object] = []
    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: spawned.append(args))
    window = assertion_window(date(2026, 9, 14))
    args = caffeinate_args(window, window.start)
    assert runner(args) is None
    assert spawned == []


# -- the power assertion ----------------------------------------------------------

# The design's chain is the wake alarm, then KeepAlive starting the daemon, then the
# caffeinate assertion keeping an open laptop awake. These cover the last link. The
# assertion is owed on a holiday too, so it rides the per-tick seam rather than the
# per-cycle one.


def test_on_tick_fires_every_minute_including_a_non_session_day(calendar):
    ticks: list[datetime] = []
    clock = ManualClock(start=datetime(2026, 8, 31, 12, 0, tzinfo=MARKET_TZ).astimezone(UTC))
    session_clock = SessionClock(clock, FakeCalendar({}))
    daemon.run_loop(
        session_clock,
        lambda **_: pytest.fail("no cycle runs on a day the calendar calls closed"),
        clock=clock,
        hooks=daemon.DaemonHooks(on_tick=ticks.append),
        should_continue=_stop_after(3),
    )
    assert len(ticks) == 3


def _stop_after(count: int) -> Callable[[], bool]:
    remaining = [count]

    def should_continue() -> bool:
        if remaining[0] == 0:
            return False
        remaining[0] -= 1
        return True

    return should_continue


def test_the_wired_daemon_holds_the_caffeinate_assertion_once_per_window(tmp_path):
    # A holiday weekday: no cycle runs, and the assertion is still owed from the 08:25
    # wake. The config and roster are fixtures rather than whatever the machine happens
    # to hold, because the alarm reads both on every path, capture slot or not. Reading
    # the machine's own is what pointed this test at the owner's live check.
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    held: list[tuple[str, ...]] = []
    pinger = FakePinger()
    clock = ManualClock(start=datetime(2026, 8, 31, 8, 25, tzinfo=MARKET_TZ).astimezone(UTC))
    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=FakeCalendar({}),
        assertion_runner=lambda args: held.append(tuple(args)),
        transport=FakeTransport(),
        pinger=pinger,
        compaction_runner=lambda args: None,
        pull_runner=None,
        should_continue=_stop_after(4),
    )
    # Four ticks, one window, one caffeinate process.
    assert len(held) == 1
    assert held[0][:3] == ("caffeinate", "-i", "-t")
    # The holiday heartbeat, one per tick, and the regression test for the leak. These
    # four pings went to the owner's live check until the seams were made required, so
    # naming the fixture URL here is what proves they now go nowhere real.
    assert pinger.urls == ["https://hc-ping.com/secret-key/capture"] * 4


def test_the_wired_daemon_refuses_to_start_without_a_config(tmp_path):
    # Standing the alarm down on a load failure was the older behaviour, and it hid two
    # things at once: the daemon ran on with no watchdog and no dead-man, and this very
    # test took one path on a machine that had a config and another on a machine that
    # did not. A loader that fails is fatal to the first cycle anyway, so it is fatal
    # here, and says which file it wanted.
    clock = ManualClock(start=datetime(2026, 8, 31, 8, 25, tzinfo=MARKET_TZ).astimezone(UTC))
    with pytest.raises(ConfigError):
        daemon.run_loop_from_config(
            config_path=str(tmp_path / "absent.yaml"),
            tickers_path=str(tmp_path / "absent-tickers.yaml"),
            clock=clock,
            calendar=FakeCalendar({}),
            assertion_runner=lambda args: None,
            transport=FakeTransport(),
            pinger=FakePinger(),
            compaction_runner=lambda args: None,
            pull_runner=None,
            should_continue=_stop_after(1),
        )


def test_the_wired_daemon_refuses_to_start_without_a_roster(tmp_path):
    # The roster half of the same rule. The alarm reads both, so both are fatal.
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    clock = ManualClock(start=datetime(2026, 8, 31, 8, 25, tzinfo=MARKET_TZ).astimezone(UTC))
    with pytest.raises(TickersError):
        daemon.run_loop_from_config(
            config_path=str(config),
            tickers_path=str(tmp_path / "absent-tickers.yaml"),
            clock=clock,
            calendar=FakeCalendar({}),
            assertion_runner=lambda args: None,
            transport=FakeTransport(),
            pinger=FakePinger(),
            compaction_runner=lambda args: None,
            pull_runner=None,
            should_continue=_stop_after(1),
        )


def test_the_wired_daemon_still_runs_a_caller_tick_hook(tmp_path):
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    ticks: list[datetime] = []
    clock = ManualClock(start=datetime(2026, 8, 31, 8, 25, tzinfo=MARKET_TZ).astimezone(UTC))
    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=FakeCalendar({}),
        assertion_runner=lambda args: None,
        hooks=daemon.DaemonHooks(on_tick=ticks.append),
        transport=FakeTransport(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        pull_runner=None,
        should_continue=_stop_after(2),
    )
    assert len(ticks) == 2


# -- the command-line entry under the installed plist ------------------------------


def test_main_with_no_arguments_forwards_three_unset_paths(tmp_path, monkeypatch):
    """The installed plist runs ``python -m lake.daemon`` with no arguments at all.

    So ``None`` for all three paths is what production hands the loop, and the loop's
    helpers each resolve ``None`` the same way. A ``main`` that filled in a default of its
    own would hand them a path the environment variable no longer overrides, which under
    ``control_plane render --config`` is a different file from the one ``main`` read.
    ``MARKETLAKE_CONFIG`` points at this test's config, the shape that install writes.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, lake_root)))
    # The config is found through the variable alone. A file at the default path would
    # be found without it.
    assert not default_config_path().exists()
    assert not is_protected(default_config_path())
    seen: dict[str, object] = {}

    def fake_run_loop_from_config(**kwargs) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(daemon, "run_loop_from_config", fake_run_loop_from_config)
    assert daemon.main([]) == 0

    assert seen["config_path"] is None
    assert seen["tickers_path"] is None
    assert seen["token_path"] is None
    # The topic came from the file the variable names, so ``main`` read that config.
    assert seen["transport"]._topic == NTFY_TOPIC


def test_a_pull_line_the_stderr_refuses_costs_nothing(monkeypatch):
    # ``_start_token_pull`` runs from ``on_cycle``, which nothing guards, so a stderr that
    # refuses the line must not raise into the loop, on either the started or the failed path.
    class _Refusing:
        def write(self, text: str) -> int:
            raise OSError(5, "Input/output error")

        def flush(self) -> None:
            raise OSError(5, "Input/output error")

    slot = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
    calls: list[list[str]] = []

    def refuse(args):
        raise OSError(12, "Cannot allocate memory")

    monkeypatch.setattr(daemon.sys, "stderr", _Refusing())
    daemon._start_token_pull(calls.append, ["pull"], slot)
    daemon._start_token_pull(refuse, ["pull"], slot)
    assert calls == [["pull"]]
