"""The daemon loop, decided from values alone.

Every case here drives ``run_loop`` with a manual clock, a fake calendar, and a fake
cycle runner that records each call and returns a canned result. No file, network, or
wall clock is in the path, so these are unit tests. The times named here are declared
through the fakes, which is exactly where naming a time is allowed.

They pin the loop's observable contract:

1. A regular session fires one cycle at every capture slot, the open through the option
   close, and at no other minute. The loop idles before the open and after the close.
2. A non-session day fires nothing. An early close's last cycle is its 13:15 slot.
3. Every sleep lands on a minute top, and the loop keeps ticking off the window rather
   than returning.
4. The four hooks fire as specified: ``on_start`` once before any cycle, ``close_tag_for``
   once per capture slot with its answer passed through, ``on_cycle`` with every result
   in order, ``on_skipped`` with the capture slots an overrun missed.
5. ``session_phase`` is ``post_equity_close`` on the slots past the equity close and
   through the option close, and null elsewhere.
6. A cycle that overruns its minute skips the overrun slot and realigns. It is never
   caught up. The skipped slot is reported, never a silent hole.
7. Skip detection reports exactly the missed capture slots: one for a one-slot overrun,
   both in order for a two-slot overrun, only the in-window slots when the overrun
   crosses the option close, and nothing under normal cadence or on the first tick.
8. A stall spans days within one incarnation. It reports the first day's tail and the
   last day's head, in order, with weekends and holidays contributing nothing. A wake
   on a Saturday still reports Friday's tail. A night jump reports nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import pytest

from lake import daemon
from lake.calendar import MARKET_TZ
from lake.capture import CycleResult
from lake.session import SessionClock, SessionPhase
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock

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


class _RecordingRunner:
    """A fake cycle runner.

    It records each call as ``(slot, close_tag, session_phase)``, with the slot read off
    the clock the way the real cycle floors it, and returns a canned empty result. An
    optional ``duration`` maps a slot to the seconds the cycle takes. The runner advances
    the clock by that much across the call, modelling a cycle that takes real time.
    """

    def __init__(
        self, clock: ManualClock, duration: Callable[[datetime], float] | None = None
    ) -> None:
        self._clock = clock
        self._duration = duration if duration is not None else _instant
        self.calls: list[tuple[datetime, str | None, str | None]] = []
        self.results: list[CycleResult] = []

    def __call__(self, *, close_tag: str | None, session_phase: str | None) -> CycleResult:
        snap = self._clock.now().replace(second=0, microsecond=0).astimezone(ET)
        self._clock.advance(self._duration(snap))
        result = CycleResult(snap_ts=snap.astimezone(UTC), segments=())
        self.calls.append((snap, close_tag, session_phase))
        self.results.append(result)
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
    duration: Callable[[datetime], float] | None = None,
    stall: tuple[datetime, float] | None = None,
) -> tuple[_RecordingRunner, list[datetime]]:
    """Run the loop from ``start`` until the clock reaches ``end``.

    Returns the runner and every instant ``should_continue`` observed. The first observed
    instant is ``start`` itself, before any sleep. Each later one is the clock right after
    a tick's work, so it is the instant the tick landed on when the cycle takes no time.
    ``stall`` is ``(instant, seconds)``: the first time the clock is seen at or past that
    instant between ticks, it jumps forward by that many seconds. It models a stall that
    is not a cycle, like the laptop sleeping, on any tick, capture or idle.
    """
    clock = ManualClock(start=start.astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    runner = _RecordingRunner(clock, duration)
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


def _instant(slot: datetime) -> float:
    """The default cycle duration: no time at all."""
    return 0.0


def _overrun(seconds_by_slot: dict[datetime, float]) -> Callable[[datetime], float]:
    """A cycle duration: the given seconds on the named slots, zero on every other."""
    return lambda slot: seconds_by_slot.get(slot, 0.0)


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


def test_a_slow_cycle_skips_the_overrun_minute_and_realigns(calendar):
    # Each cycle takes 90 seconds. The 09:30 cycle ends at 09:31:30, so the loop realigns
    # to 09:32 and the 09:31 slot fires nothing. It is never caught up.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar, et(REGULAR, 9, 29), et(REGULAR, 9, 36), hooks=hooks, duration=lambda s: 90
    )
    assert runner.slots == [
        et(REGULAR, 9, 30),
        et(REGULAR, 9, 32),
        et(REGULAR, 9, 34),
        et(REGULAR, 9, 36),
    ]
    # Each skipped minute is reported on the tick that finds it, never left a hole.
    assert reports == [[et(REGULAR, 9, 31)], [et(REGULAR, 9, 33)], [et(REGULAR, 9, 35)]]


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 8, 24, 13, 30, 0, tzinfo=UTC), 60.0),  # on a top: a full minute
        (datetime(2026, 8, 24, 13, 30, 17, 250000, tzinfo=UTC), 42.75),
        (datetime(2026, 8, 24, 13, 30, 59, 999999, tzinfo=UTC), 0.000001),
    ],
)
def test_seconds_to_next_minute(now: datetime, expected: float):
    assert daemon.seconds_to_next_minute(now) == pytest.approx(expected)


# -- 3. the three hooks ----------------------------------------------------------


def test_on_start_is_called_once_before_any_cycle(calendar):
    events: list[str] = []
    hooks = daemon.DaemonHooks(
        on_start=lambda: events.append("start"),
        on_cycle=lambda slot, result: events.append("cycle"),
    )
    _simulate(calendar, et(REGULAR, 9, 28), et(REGULAR, 9, 33), hooks=hooks)
    assert events == ["start", "cycle", "cycle", "cycle", "cycle"]


def test_on_start_fires_even_when_no_cycle_ever_does(calendar):
    events: list[str] = []
    hooks = daemon.DaemonHooks(on_start=lambda: events.append("start"))
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
    assert hooks.on_start() is None
    assert hooks.close_tag_for(et(REGULAR, 16, 0)) is None
    result = CycleResult(snap_ts=et(REGULAR, 16, 0), segments=())
    assert hooks.on_cycle(et(REGULAR, 16, 0), result) is None
    assert hooks.on_skipped([et(REGULAR, 16, 1)]) is None


# -- 6. skipped slots --------------------------------------------------------------


def test_a_one_slot_overrun_reports_exactly_that_slot(calendar):
    # The 10:00 cycle runs 90 seconds, to 10:01:30. The loop realigns to 10:02, and that
    # tick reports the one slot it stepped over.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 9, 59),
        et(REGULAR, 10, 4),
        hooks=hooks,
        duration=_overrun({et(REGULAR, 10, 0): 90}),
    )
    assert runner.slots == [
        et(REGULAR, 10, 0),
        et(REGULAR, 10, 2),
        et(REGULAR, 10, 3),
        et(REGULAR, 10, 4),
    ]
    assert reports == [[et(REGULAR, 10, 1)]]


def test_a_two_slot_overrun_reports_both_in_order(calendar):
    # The 10:00 cycle runs 150 seconds, to 10:02:30. The 10:03 tick reports both.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 9, 59),
        et(REGULAR, 10, 4),
        hooks=hooks,
        duration=_overrun({et(REGULAR, 10, 0): 150}),
    )
    assert runner.slots == [et(REGULAR, 10, 0), et(REGULAR, 10, 3), et(REGULAR, 10, 4)]
    assert reports == [[et(REGULAR, 10, 1), et(REGULAR, 10, 2)]]


def test_normal_cadence_never_calls_on_skipped(calendar):
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(calendar, et(REGULAR, 8, 0), et(REGULAR, 18, 0), hooks=hooks)
    assert len(runner.slots) == 406
    assert reports == []


def test_the_first_tick_never_calls_on_skipped(calendar):
    # Started mid-session, mid-minute. The first tick has no previous slot to compare
    # against. The minutes before it belong to startup gap-marking, not the loop.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(calendar, et(REGULAR, 10, 0, 17), et(REGULAR, 10, 3), hooks=hooks)
    assert runner.slots == [et(REGULAR, 10, 1), et(REGULAR, 10, 2), et(REGULAR, 10, 3)]
    assert reports == []


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
    # The 16:13 cycle runs four minutes, to 16:17. The next tick, 16:18, is past the
    # option close and fires nothing, but it still reports the skip: 16:14 and 16:15 are
    # capture slots, 16:16 and 16:17 are not.
    hooks, reports = _skip_recorder()
    runner, _ = _simulate(
        calendar,
        et(REGULAR, 16, 12, 30),
        et(REGULAR, 16, 20),
        hooks=hooks,
        duration=_overrun({et(REGULAR, 16, 13): 240}),
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


def test_main_passes_the_paths_to_the_config_entry(monkeypatch):
    seen: dict[str, object] = {}

    def fake_run_loop_from_config(**kwargs) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(daemon, "run_loop_from_config", fake_run_loop_from_config)
    assert daemon.main(["--config", "/c.yaml", "--token", "/tok.json"]) == 0
    assert seen == {"config_path": "/c.yaml", "tickers_path": None, "token_path": "/tok.json"}
