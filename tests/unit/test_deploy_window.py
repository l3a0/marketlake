"""``lake.deploy_window``: when a deploy of the hosted VM may start.

Each case puts a ``ManualClock`` at one moment and reads the verdict, so no case reads
the machine's clock. The spans come from the control plane's own roster, and the cases
that need a different roster build one, so a moved schedule moves the window with it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from lake import control_plane as cp
from lake import deploy_window as dw
from lake.calendar import MARKET_TZ
from tests.support.clock import ManualClock

# A week in October 2026: Saturday the 10th through Saturday the 17th.
SATURDAY = date(2026, 10, 10)
SUNDAY = date(2026, 10, 11)
MONDAY = date(2026, 10, 12)
TUESDAY = date(2026, 10, 13)
FRIDAY = date(2026, 10, 16)

HOST = cp.SystemdHost(python="/p/python", owner="o", home="/h", project_dir="/h/m")


def et(day: date, hour: int, minute: int = 0, second: int = 0, *, fold: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=MARKET_TZ, fold=fold)


def run(now: datetime, capsys) -> tuple[int, list[str]]:
    code = dw.main([], clock=ManualClock(now.astimezone(UTC)))
    return code, capsys.readouterr().out.splitlines()


def epoch(when: datetime) -> int:
    return int(when.timestamp())


def _job(label: str, at: cp.WallClockTime, days: tuple[int, ...]) -> cp.SystemdUnit:
    return HOST.job(label, "lake.x", calendar=cp.Schedule(at, days))


def _no_window(day: date) -> None:
    return None


# -- the spans -------------------------------------------------------------------------


def test_todays_roster_refuses_weekdays_and_sunday_evening():
    units = dw.default_units()
    for offset in range(5):
        day = MONDAY + timedelta(days=offset)
        span = dw.refused_span(day, units)
        assert span == dw.Span(et(day, 8, 0).astimezone(UTC), et(day, 18, 45).astimezone(UTC))
    sunday = dw.refused_span(SUNDAY, units)
    assert sunday == dw.Span(et(SUNDAY, 19, 30).astimezone(UTC), et(SUNDAY, 23, 30).astimezone(UTC))
    assert dw.refused_span(SATURDAY, units) is None


def test_the_span_is_read_from_the_roster_and_the_assertion_window():
    """A job moved in the roster moves the span, with no time restated here."""
    early = _job("a", cp.WallClockTime(6, 0), (5,))
    late = _job("b", cp.WallClockTime(11, 0), (5,))
    span = dw.refused_span(SATURDAY, [early, late], _no_window)
    start, end = et(SATURDAY, 5, 30), et(SATURDAY, 11, 15)
    assert span == dw.Span(start.astimezone(UTC), end.astimezone(UTC))

    # An assertion window that ends later than the last start plus the tail sets the end.
    def window(day: date) -> cp.AssertionWindow:
        return cp.AssertionWindow(et(day, 5, 0), et(day, 13, 0))

    span = dw.refused_span(SATURDAY, [early, late], window)
    assert span is not None
    assert span.end == et(SATURDAY, 13, 0).astimezone(UTC)

    # One that ends earlier leaves the last start plus the tail as the end.
    def short(day: date) -> cp.AssertionWindow:
        return cp.AssertionWindow(et(day, 5, 0), et(day, 7, 0))

    span = dw.refused_span(SATURDAY, [early, late], short)
    assert span is not None
    assert span.end == et(SATURDAY, 11, 15).astimezone(UTC)
    # A day with no schedule refuses nothing, whatever its window says.
    assert dw.refused_span(SUNDAY, [early, late], window) is None


def test_a_resident_never_makes_a_span():
    assert dw.refused_span(MONDAY, [cp.daemon_job(HOST), cp.dashboard_job(HOST)]) is None


def test_no_schedule_at_all_is_refused():
    with pytest.raises(ValueError, match="no unit has a schedule"):
        dw.decide(et(MONDAY, 12), [cp.daemon_job(HOST)])


def test_a_naive_moment_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        dw.decide(datetime(2026, 10, 12, 12))


# -- the verdict -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("now", "latest", "span"),
    [
        (et(MONDAY, 6, 30), "Mon 2026-10-12 06:30 EDT", et(MONDAY, 8)),
        (et(MONDAY, 18, 45), "Tue 2026-10-13 06:30 EDT", et(MONDAY + timedelta(1), 8)),
        (et(FRIDAY, 20), "Sun 2026-10-18 18:00 EDT", et(SUNDAY + timedelta(7), 19, 30)),
        (et(SATURDAY, 12), "Sun 2026-10-11 18:00 EDT", et(SUNDAY, 19, 30)),
        (et(SUNDAY, 18), "Sun 2026-10-11 18:00 EDT", et(SUNDAY, 19, 30)),
        (et(SUNDAY, 23, 30), "Mon 2026-10-12 06:30 EDT", et(MONDAY, 8)),
    ],
    ids=["weekday morning", "weekday close", "friday night", "saturday", "sunday", "sunday late"],
)
def test_a_deploy_may_start_outside_the_spans_and_their_margin(capsys, now, latest, span):
    code, lines = run(now, capsys)
    assert code == 0
    assert lines == [
        f"a deploy may start now, and until {latest}",
        f"next_span_start={epoch(span)}",
    ]


@pytest.mark.parametrize(
    ("now", "free", "span", "reason"),
    [
        (et(MONDAY, 8), "Mon 2026-10-12 18:45 EDT", et(TUESDAY, 8), "the scheduled jobs run"),
        (et(MONDAY, 18, 44, 59), "Mon 2026-10-12 18:45 EDT", et(TUESDAY, 8), "the scheduled"),
        (et(MONDAY, 6, 30, 1), "Mon 2026-10-12 18:45 EDT", et(MONDAY, 8), "less than 90"),
        (et(SUNDAY, 18, 0, 1), "Sun 2026-10-11 23:30 EDT", et(SUNDAY, 19, 30), "less than"),
        (et(SUNDAY, 21), "Sun 2026-10-11 23:30 EDT", et(MONDAY, 8), "the scheduled"),
    ],
    ids=["span opens", "span closes", "margin", "sunday margin", "sunday span"],
)
def test_a_deploy_may_not_start_inside_a_span_or_its_margin(capsys, now, free, span, reason):
    code, lines = run(now, capsys)
    assert code == 3
    assert len(lines) == 2
    assert lines[0].startswith(f"a deploy may start next at {free}, because {reason}")
    # In the margin it is the span ahead. Inside a span it is the one after, since the
    # current span's start is past and an overridden deploy's restart reads it (#987).
    assert lines[1] == f"next_span_start={epoch(span)}"


def test_the_reasons_read_in_full():
    inside = dw.decide(et(MONDAY, 12))
    assert inside.line == (
        "a deploy may start next at Mon 2026-10-12 18:45 EDT, because the scheduled jobs run"
        " until then"
    )
    margin = dw.decide(et(MONDAY, 7))
    assert margin.line == (
        "a deploy may start next at Mon 2026-10-12 18:45 EDT, because less than 90 minutes"
        " remain before the next span"
    )


def test_a_gap_shorter_than_the_margin_refuses_up_to_the_later_span():
    units = [
        _job("late", cp.WallClockTime(23, 0), (5,)),
        _job("early", cp.WallClockTime(1, 0), (6,)),
    ]
    for now, upcoming in (
        (et(SATURDAY, 23), et(SUNDAY, 0, 30)),
        (et(SATURDAY, 21, 30), et(SATURDAY, 22, 30)),
    ):
        verdict = dw.decide(now, units, _no_window)
        assert verdict.code == dw.REFUSED
        assert "next at Sun 2026-10-11 01:15 EDT" in verdict.line
        assert verdict.next_span_start == upcoming.astimezone(UTC)


def test_a_span_that_started_yesterday_is_still_found():
    units = [_job("late", cp.WallClockTime(23, 50), (5,))]
    verdict = dw.decide(et(SUNDAY, 0, 1), units, _no_window)
    assert verdict.code == dw.REFUSED
    assert "next at Sun 2026-10-11 00:05 EDT" in verdict.line
    # The only span left to open is the same job's, a week on.
    assert verdict.next_span_start == et(SATURDAY + timedelta(7), 23, 20).astimezone(UTC)


# -- a clock change --------------------------------------------------------------------

# In 2026 the clocks fall back on Sunday 1 November at 02:00 and spring forward on
# Sunday 8 March at 02:00.
FALL_BACK = date(2026, 11, 1)
SPRING_FORWARD = date(2026, 3, 8)


def test_the_spans_keep_their_eastern_times_across_a_clock_change():
    units = dw.default_units()
    before = dw.refused_span(date(2026, 10, 26), units)
    after = dw.refused_span(date(2026, 11, 2), units)
    assert before is not None and after is not None
    # 08:00 Eastern is 12:00 UTC in daylight time and 13:00 UTC in standard time.
    assert before.start.time().hour == 12
    assert after.start.time().hour == 13
    assert after.start.astimezone(MARKET_TZ).hour == 8
    sunday = dw.refused_span(FALL_BACK, units)
    assert sunday == dw.Span(
        et(FALL_BACK, 19, 30).astimezone(UTC), et(FALL_BACK, 23, 30).astimezone(UTC)
    )


def test_the_margin_is_measured_in_real_minutes_when_the_clocks_fall_back():
    """01:30 EDT to 02:30 EST is one hour on the wall and two in real time."""
    units = [_job("dawn", cp.WallClockTime(3, 0), (6,))]
    now = et(FALL_BACK, 1, 30, fold=0)
    verdict = dw.decide(now, units, _no_window)
    assert verdict.code == dw.ALLOWED
    # 02:30 EST less 90 minutes is 01:00 EST, the second 01:00 of the night.
    assert verdict.line == "a deploy may start now, and until Sun 2026-11-01 01:00 EST"
    assert verdict.next_span_start == datetime(2026, 11, 1, 7, 30, tzinfo=UTC)


def test_the_margin_is_measured_in_real_minutes_when_the_clocks_spring_forward():
    """01:00 EST to 03:00 EDT is two hours on the wall and one in real time."""
    units = [_job("dawn", cp.WallClockTime(3, 30), (6,))]
    verdict = dw.decide(et(SPRING_FORWARD, 1, 0), units, _no_window)
    assert verdict.code == dw.REFUSED
    assert verdict.next_span_start == datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
    allowed = dw.decide(et(SPRING_FORWARD, 0, 30), units, _no_window)
    assert allowed.code == dw.ALLOWED
    assert allowed.line == "a deploy may start now, and until Sun 2026-03-08 00:30 EST"


def test_a_weekly_span_a_week_ahead_is_found():
    """Decided after this Saturday's span, the next one is a full week away."""
    units = [_job("weekly", cp.WallClockTime(6, 0), (5,))]
    verdict = dw.decide(et(SATURDAY, 12), units, _no_window)
    assert verdict.code == dw.ALLOWED
    assert verdict.next_span_start == et(SATURDAY + timedelta(7), 5, 30).astimezone(UTC)


def test_a_gap_of_exactly_the_margin_frees_the_earlier_end():
    """Saturday's span ends at 23:15 and Sunday's starts 90 minutes later, at 00:45."""
    units = [
        _job("late", cp.WallClockTime(23, 0), (5,)),
        _job("early", cp.WallClockTime(1, 15), (6,)),
    ]
    verdict = dw.decide(et(SATURDAY, 23), units, _no_window)
    assert verdict.code == dw.REFUSED
    assert "next at Sat 2026-10-10 23:15 EDT" in verdict.line
    assert dw.decide(et(SATURDAY, 23, 15), units, _no_window).code == dw.ALLOWED


def test_a_span_inside_the_current_one_does_not_shorten_it():
    """Saturday's assertion window runs to 03:00 Sunday, and Sunday's span, 00:30 to 01:15,
    lies inside it, so a deploy may start only once Saturday's span ends."""
    units = [
        _job("late", cp.WallClockTime(22, 0), (5,)),
        _job("early", cp.WallClockTime(1, 0), (6,)),
    ]

    def window(day: date) -> cp.AssertionWindow | None:
        if day == SATURDAY:
            return cp.AssertionWindow(et(SATURDAY, 21, 0), et(SUNDAY, 3, 0))
        return None

    verdict = dw.decide(et(SATURDAY, 23), units, window)
    assert verdict.code == dw.REFUSED
    assert "next at Sun 2026-10-11 03:00 EDT" in verdict.line


def test_the_gap_between_spans_is_measured_in_real_minutes_when_the_clocks_fall_back():
    """Saturday's span ends at 01:00 EDT and Sunday's starts at 02:00 EST. That is one hour
    on the wall and two in real time, more than the margin, so Saturday's end frees a start."""
    saturday = FALL_BACK - timedelta(1)
    units = [
        _job("late", cp.WallClockTime(23, 0), (5,)),
        _job("dawn", cp.WallClockTime(2, 30), (6,)),
    ]

    def window(day: date) -> cp.AssertionWindow | None:
        if day == saturday:
            return cp.AssertionWindow(et(saturday, 22, 0), et(FALL_BACK, 1, 0, fold=0))
        return None

    verdict = dw.decide(et(saturday, 23), units, window)
    assert verdict.code == dw.REFUSED
    assert "next at Sun 2026-11-01 01:00 EDT" in verdict.line


# -- the entry -------------------------------------------------------------------------


def test_the_entry_takes_no_arguments(capsys):
    with pytest.raises(SystemExit) as raised:
        dw.main(["--now"], clock=ManualClock(et(MONDAY, 12)))
    assert raised.value.code == 2


# -- the session hold (#987) -----------------------------------------------------------


def _session_hold(now: datetime, capsys) -> tuple[int, list[str]]:
    code = dw.main(["--session-hold"], clock=ManualClock(now.astimezone(UTC)))
    return code, capsys.readouterr().out.splitlines()


@pytest.mark.parametrize(
    ("now", "opens", "end", "line"),
    [
        (
            et(TUESDAY, 10),
            et(TUESDAY, 9, 30),
            et(TUESDAY, 16, 30),
            "the session holds a deploy from Tue 2026-10-13 08:00 EDT until Tue 2026-10-13"
            " 16:30 EDT",
        ),
        (
            et(TUESDAY, 23, 59),
            et(TUESDAY, 9, 30),
            et(TUESDAY, 16, 30),
            "the session holds a deploy from Tue 2026-10-13 08:00 EDT until Tue 2026-10-13"
            " 16:30 EDT",
        ),
        # The day after Thanksgiving closes at 13:00, and the hold ends with it.
        (
            et(date(2026, 11, 27), 9),
            et(date(2026, 11, 27), 9, 30),
            et(date(2026, 11, 27), 13, 30),
            "the session holds a deploy from Fri 2026-11-27 08:00 EST until Fri 2026-11-27"
            " 13:30 EST",
        ),
    ],
    ids=["a session", "late on a session day", "an early close"],
)
def test_the_session_hold_runs_from_the_margin_before_the_open_to_the_compaction(
    capsys, now, opens, end, line
):
    code, lines = _session_hold(now, capsys)
    assert code == dw.ALLOWED
    refuse_from = opens - dw.MARGIN
    assert lines == [line, f"session_hold={epoch(refuse_from)},{epoch(opens)},{epoch(end)}"]


@pytest.mark.parametrize(
    "now",
    [et(SATURDAY, 12), et(SUNDAY, 17, 20), et(date(2026, 11, 26), 12)],
    ids=["saturday", "sunday", "thanksgiving"],
)
def test_no_session_holds_nothing(capsys, now):
    assert _session_hold(now, capsys) == (
        dw.ALLOWED,
        ["no session holds a deploy today", "session_hold=none"],
    )


def test_the_session_hold_leaves_the_two_line_verdict_alone(capsys):
    """vm-stop.sh and an ordinary deploy read exactly the verdict, so the hold is opt-in."""
    code, lines = run(et(TUESDAY, 10), capsys)
    assert code == dw.REFUSED
    assert len(lines) == 2 and lines[1].startswith("next_span_start=")
