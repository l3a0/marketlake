"""The pmset schedule: the two command strings, the Sunday date, and the read-back.

The commands must match the design's table character for character. The Sunday
date is the Sunday before the next session week, computed from an injected ``now``
and a fake calendar. The read-back parser turns ``pmset -g sched`` text into alarms,
and the check names what is missing or drifted. All decided from values, so unit.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from lake import control_plane as cp
from lake.calendar import MARKET_TZ
from tests.support.calendar import FakeCalendar, SessionTimes


def _sessions(mondays: list[date], holidays: set[date] = frozenset()) -> FakeCalendar:
    """Regular sessions on every weekday of each listed week, minus the holidays."""
    table = {}
    for monday in mondays:
        assert monday.weekday() == 0
        for offset in range(5):
            day = monday + timedelta(days=offset)
            if day in holidays:
                continue
            table[day] = SessionTimes(
                open=datetime(day.year, day.month, day.day, 9, 30, tzinfo=MARKET_TZ),
                close=datetime(day.year, day.month, day.day, 16, 0, tzinfo=MARKET_TZ),
            )
    return FakeCalendar(table)


# Two adjacent weeks in 2026. Labor Day, Monday September 7, is the holiday.
WEEK_AUG_31 = date(2026, 8, 31)
WEEK_SEP_7 = date(2026, 9, 7)
LABOR_DAY = date(2026, 9, 7)
CALENDAR = _sessions([date(2026, 8, 24), WEEK_AUG_31, WEEK_SEP_7], holidays={LABOR_DAY})


def _et(*args: int) -> datetime:
    return datetime(*args, tzinfo=MARKET_TZ)


# -- the command strings -----------------------------------------------------------


def test_repeat_command_matches_the_design_table():
    assert cp.pmset_repeat_command() == "pmset repeat wakeorpoweron MTWRF 08:25:00"


def test_schedule_command_matches_the_design_table():
    assert (
        cp.pmset_schedule_command(date(2026, 9, 6))
        == 'pmset schedule wakeorpoweron "09/06/26 19:55:00"'
    )


def test_schedule_command_refuses_a_non_sunday():
    with pytest.raises(ValueError):
        cp.pmset_schedule_command(date(2026, 9, 7))


# -- the Sunday date ----------------------------------------------------------------


def test_regular_week_targets_the_coming_sunday():
    # A Wednesday. The next session week starts Monday August 31.
    assert cp.next_sunday_wake(_et(2026, 8, 26, 12, 0), CALENDAR) == date(2026, 8, 30)


def test_friday_run_targets_the_sunday_two_days_ahead():
    assert cp.next_sunday_wake(_et(2026, 8, 28, 18, 30), CALENDAR) == date(2026, 8, 30)


def test_monday_holiday_leaves_the_sunday_where_it_is():
    # Friday September 4. Monday the 7th is Labor Day. The first session is Tuesday,
    # in the same week, so the wake is still Sunday the 6th.
    assert cp.next_sunday_wake(_et(2026, 9, 4, 18, 30), CALENDAR) == date(2026, 9, 6)


def test_a_fully_dark_week_pushes_the_wake_a_week_out():
    dark = _sessions([WEEK_SEP_7], holidays={LABOR_DAY})  # nothing the week of Aug 31
    assert cp.next_sunday_wake(_et(2026, 8, 28, 18, 30), dark) == date(2026, 9, 6)


def test_from_sunday_the_target_is_that_same_sunday():
    assert cp.next_sunday_wake(_et(2026, 8, 30, 20, 0), CALENDAR) == date(2026, 8, 30)


def test_sunday_wake_command_composes_the_date():
    command = cp.sunday_wake_command(_et(2026, 9, 4, 18, 30), CALENDAR)
    assert command == 'pmset schedule wakeorpoweron "09/06/26 19:55:00"'


def test_expected_one_shot_is_pending_before_the_wake_and_gone_after():
    # Friday: the Sunday wake is ahead, so it is expected.
    assert cp.expected_one_shot(_et(2026, 8, 28, 18, 30), CALENDAR) == date(2026, 8, 30)
    # Sunday 19:00, an early manual run: still ahead.
    assert cp.expected_one_shot(_et(2026, 8, 30, 19, 0), CALENDAR) == date(2026, 8, 30)
    # Sunday 20:00, the maintenance job: the one-shot fired and left the schedule.
    assert cp.expected_one_shot(_et(2026, 8, 30, 20, 0), CALENDAR) is None


# -- the read-back parser ------------------------------------------------------------

BOTH_PRESENT = """\
Repeating power events:
  wakepoweron at 8:25AM weekdays only
Scheduled power events:
 [0]  wakepoweron at 09/06/26 19:55:00 by 'pmset'
"""

REPEAT_ONLY = """\
Repeating power events:
  wakepoweron at 8:25AM weekdays only
No scheduled events.
"""

WRONG_TIME = """\
Repeating power events:
  wakepoweron at 8:30AM weekdays only
Scheduled power events:
 [0]  wakepoweron at 09/06/26 19:55:00 by 'pmset'
"""

WRONG_ONE_SHOT = """\
Repeating power events:
  wakepoweron at 8:25AM weekdays only
Scheduled power events:
 [0]  wakepoweron at 09/06/26 20:55:00 by 'pmset'
"""


def test_parser_reads_both_alarms():
    schedule = cp.parse_pmset_schedule(BOTH_PRESENT)
    assert schedule.repeats == (cp.RepeatAlarm("wakepoweron", 8, 25, frozenset({0, 1, 2, 3, 4})),)
    assert schedule.one_shots == (
        cp.OneShotAlarm("wakepoweron", datetime(2026, 9, 6, 19, 55), "pmset"),
    )


def test_parser_accepts_the_alternate_spellings():
    text = (
        "Repeating power events:\n"
        "  wakeorpoweron at 08:25:00 monday tuesday wednesday thursday friday\n"
        "Scheduled power events:\n"
        " [0]  wakeorpoweron at 09/06/2026 19:55:00\n"
    )
    schedule = cp.parse_pmset_schedule(text)
    assert schedule.repeats[0].weekdays == frozenset({0, 1, 2, 3, 4})
    assert schedule.repeats[0].hour == 8 and schedule.repeats[0].minute == 25
    assert schedule.one_shots[0].when == datetime(2026, 9, 6, 19, 55)
    assert schedule.one_shots[0].owner is None


def test_parser_reads_twelve_hour_pm_and_the_letter_day_form():
    text = "Repeating power events:\n  wakepoweron at 7:55PM MTWRF\n"
    alarm = cp.parse_pmset_schedule(text).repeats[0]
    assert (alarm.hour, alarm.minute) == (19, 55)
    assert alarm.weekdays == frozenset({0, 1, 2, 3, 4})


def test_parser_reads_empty_and_no_event_output():
    assert cp.parse_pmset_schedule("") == cp.PmsetSchedule()
    assert cp.parse_pmset_schedule("No scheduled events.\n") == cp.PmsetSchedule()


def test_parser_rejects_an_unknown_shape():
    with pytest.raises(cp.PmsetParseError):
        cp.parse_pmset_schedule("Repeating power events:\n  something odd\n")
    with pytest.raises(cp.PmsetParseError):
        cp.parse_pmset_schedule("stray line before any section\n")


# -- the alarm check -----------------------------------------------------------------

SUNDAY = date(2026, 9, 6)


def test_both_alarms_present_passes():
    check = cp.check_alarms(cp.parse_pmset_schedule(BOTH_PRESENT), one_shot_date=SUNDAY)
    assert check.ok
    assert check.problems == ()


def test_missing_one_shot_fails_when_it_is_expected():
    check = cp.check_alarms(cp.parse_pmset_schedule(REPEAT_ONLY), one_shot_date=SUNDAY)
    assert check.repeat_ok and not check.one_shot_ok
    assert not check.ok
    assert any("one-shot" in p and "missing" in p for p in check.problems)


def test_missing_one_shot_passes_when_none_is_pending():
    check = cp.check_alarms(cp.parse_pmset_schedule(REPEAT_ONLY), one_shot_date=None)
    assert check.ok


def test_missing_repeat_fails():
    text = "Scheduled power events:\n [0]  wakepoweron at 09/06/26 19:55:00 by 'pmset'\n"
    check = cp.check_alarms(cp.parse_pmset_schedule(text), one_shot_date=SUNDAY)
    assert not check.repeat_ok and check.one_shot_ok
    assert any("repeat" in p and "missing" in p for p in check.problems)


def test_wrong_repeat_time_fails_as_drift():
    check = cp.check_alarms(cp.parse_pmset_schedule(WRONG_TIME), one_shot_date=SUNDAY)
    assert not check.repeat_ok
    assert any("drifted" in p for p in check.problems)


def test_wrong_repeat_days_fail_as_drift():
    text = "Repeating power events:\n  wakepoweron at 8:25AM every day\n"
    check = cp.check_alarms(cp.parse_pmset_schedule(text), one_shot_date=None)
    assert not check.repeat_ok


def test_wrong_one_shot_time_fails_as_drift():
    check = cp.check_alarms(cp.parse_pmset_schedule(WRONG_ONE_SHOT), one_shot_date=SUNDAY)
    assert check.repeat_ok and not check.one_shot_ok
    assert any("one-shot" in p and "drifted" in p for p in check.problems)


def test_a_non_wake_event_does_not_satisfy_the_check():
    text = "Repeating power events:\n  shutdown at 8:25AM weekdays only\n"
    check = cp.check_alarms(cp.parse_pmset_schedule(text), one_shot_date=None)
    assert not check.repeat_ok
