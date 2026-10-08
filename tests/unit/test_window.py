"""The lake window: its floor, how a stored value is judged, and the edge it sets.

marketlake #786 adds ``lake_window_sessions``, the number of recent sessions a host keeps on its
lake volume. These are the value-only rules behind it. The render and the sweep that apply them
are tested in ``tests/component``.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from lake.config import GuardConstants
from lake.window import (
    EDGE_SEARCH_DAYS,
    WINDOW_KEY,
    EdgeNotFound,
    WindowRefused,
    edge_search_days,
    most_sessions_in,
    window_edge,
    window_floor,
    window_sessions,
)
from tests.support.calendar import weekday_sessions

MONDAY = date(2026, 9, 14)
FRIDAY = date(2026, 9, 18)
SATURDAY = date(2026, 9, 19)
THREE_WEEKS = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7), MONDAY)


# -- the floor -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("days", "sessions"),
    [(1, 1), (5, 5), (6, 5), (7, 5), (8, 6), (14, 10), (29, 21), (30, 22), (31, 23)],
)
def test_a_span_of_calendar_days_holds_at_most_its_weekdays(days: int, sessions: int):
    """Five sessions a whole week, and the days left over count until they reach five."""
    assert most_sessions_in(days) == sessions


def test_the_default_floor_is_22_sessions():
    """The History panel's 30 calendar days hold at most 22 sessions, which binds today."""
    assert window_floor(GuardConstants()) == 22


@pytest.mark.parametrize("trailing", [21, 30, 45])
def test_the_floor_follows_a_trailing_median_override(trailing: int):
    """A ``guards:`` override that widens the battery's trailing median raises the floor.

    The battery reads ``trailing_median_sessions`` sessions before the judged day, so it needs
    that many plus the day itself. Today's 20 sits under the History panel's 22, so a test that
    read today's value could not tell a derived floor from a literal one.
    """
    guards = GuardConstants(trailing_median_sessions=trailing)

    assert window_floor(guards) == trailing + 1


def test_a_narrower_trailing_median_does_not_lower_the_floor():
    """The History panel and the runway still need their 22 sessions."""
    assert window_floor(GuardConstants(trailing_median_sessions=5)) == 22


# -- judging a stored value ------------------------------------------------------------------


def test_an_absent_key_is_no_window():
    assert window_sessions(None, GuardConstants()) is None


@pytest.mark.parametrize("value", [22, 23, 40])
def test_a_whole_number_at_or_over_the_floor_is_the_window(value: int):
    assert window_sessions(value, GuardConstants()) == value


def test_a_value_under_the_floor_is_refused_naming_the_key_and_the_floor():
    with pytest.raises(WindowRefused) as raised:
        window_sessions(21, GuardConstants())

    message = str(raised.value)
    assert WINDOW_KEY in message
    assert "22 sessions" in message
    # The sweep files this in its report, and ``report.redacted`` keeps a line only up to its
    # second ``": "``.
    assert ": " not in message


@pytest.mark.parametrize("value", ["'22'", "True", "None", "22.0", "[22]"])
def test_a_stored_value_that_is_not_an_int_is_refused(value: str):
    """``Config`` stores any value but an ``int`` as its ``repr``, and none of them is a window."""
    with pytest.raises(WindowRefused, match="not a whole number"):
        window_sessions(value, GuardConstants())


def test_a_bool_is_refused_although_python_counts_it_an_int():
    """Refused as not a number, rather than as a window of one session under the floor."""
    with pytest.raises(WindowRefused, match="not a whole number"):
        window_sessions(True, GuardConstants())


def test_the_floor_judges_against_the_guards_it_is_given():
    """A value fine under the default floor is refused under a raised one."""
    with pytest.raises(WindowRefused, match="31 sessions"):
        window_sessions(22, GuardConstants(trailing_median_sessions=30))


# -- the edge --------------------------------------------------------------------------------


def test_the_edge_counts_tonight_as_the_first_session():
    """With tonight first, a window of five ending on a Friday starts on its Monday.

    Counting from the session before tonight would put the edge a week back, and after the
    next trim and seal the lake would hold one session more than the window.
    """
    assert window_edge(THREE_WEEKS, FRIDAY, 5) == MONDAY


def test_a_window_of_one_is_tonight():
    assert window_edge(THREE_WEEKS, FRIDAY, 1) == FRIDAY


def test_a_session_the_lake_never_captured_still_takes_a_slot():
    """The count is the calendar's, whatever the lake holds, as ``trailing_medians`` counts."""
    assert window_edge(THREE_WEEKS, FRIDAY, 6) == date(2026, 9, 11)


def test_a_holiday_takes_no_slot():
    calendar = weekday_sessions(date(2026, 9, 7), MONDAY, holidays=(date(2026, 9, 7),))

    assert window_edge(calendar, FRIDAY, 9) == date(2026, 9, 8)


def test_a_night_that_is_not_a_session_takes_no_slot():
    assert window_edge(THREE_WEEKS, SATURDAY, 1) == FRIDAY


class _CountingCalendar:
    """A calendar that answers no session, and fails a test that asks it too often."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.asked = 0

    def is_session(self, day: date) -> bool:
        self.asked += 1
        assert self.asked <= self.limit, "the edge kept counting past its stop"
        return False


def test_the_edge_stops_counting_rather_than_looping_on_a_calendar_with_no_session():
    """A calendar that answers no session refuses the edge instead of hanging the sweep."""
    calendar = _CountingCalendar(limit=EDGE_SEARCH_DAYS + 30)

    with pytest.raises(EdgeNotFound):
        window_edge(calendar, FRIDAY, 22)

    assert calendar.asked == EDGE_SEARCH_DAYS


def test_the_stop_reaches_back_a_year():
    """A session just inside the bound is still found, so the stop is not too short."""
    far = FRIDAY - timedelta(days=EDGE_SEARCH_DAYS - 1)

    class _OneSession:
        def is_session(self, day: date) -> bool:
            return day in (FRIDAY, far)

    assert window_edge(_OneSession(), FRIDAY, 2) == far


# -- review fixes ----------------------------------------------------------------------------


def test_the_history_width_matches_the_dashboards():
    """``lake.window`` restates the History panel's width so the sweep need not import DuckDB.

    The two must not drift, or the floor would protect a panel of a different width.
    """
    from lake import dashboard, window

    assert window.HISTORY_WINDOW_DAYS == dashboard.HISTORY_WINDOW_DAYS


def test_the_window_module_does_not_import_the_dashboard():
    """The render and the sweep load ``lake.window``, and DuckDB is about 17.5 MiB of import."""
    import subprocess
    import sys

    probe = (
        "import sys, lake.window, lake.config; "
        "from lake.window import window_floor; "
        "window_floor(lake.config.GuardConstants()); "
        "print('lake.dashboard' in sys.modules, 'duckdb' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout

    assert out.split() == ["False", "False"]


def test_a_year_long_window_finds_its_edge_on_the_real_calendar():
    """260 sessions span more than 366 days, so a fixed one-year search would always refuse."""
    from lake.calendar import ExchangeCalendar

    tonight = date(2026, 10, 8)

    edge = window_edge(ExchangeCalendar(), tonight, 260)

    assert tonight - edge > timedelta(days=EDGE_SEARCH_DAYS)
    assert edge_search_days(260) > EDGE_SEARCH_DAYS


def test_a_window_longer_than_the_search_says_so():
    """Too few sessions found is a window the search cannot reach, not an empty calendar."""
    with pytest.raises(EdgeNotFound, match="only 15 of 22 sessions") as raised:
        window_edge(THREE_WEEKS, FRIDAY, 22)

    assert "answers no session" not in str(raised.value)
