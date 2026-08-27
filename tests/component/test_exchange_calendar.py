"""The calendar seam, real side. The ExchangeCalendar adapter over the real
``exchange_calendars`` library. The expected times are pinned against known 2026 NYSE
sessions. These assertions name times in the test, which is exactly where naming a
time is allowed."""

from __future__ import annotations

from datetime import date

import pytest

from lake.calendar import OPTION_CLOSE_OFFSET, Calendar, ExchangeCalendar, NotASession

REGULAR = date(2026, 8, 24)  # a plain Monday
EARLY_CLOSE = date(2026, 11, 27)  # the day after Thanksgiving
GOOD_FRIDAY = date(2026, 4, 3)  # a holiday that is not a federal holiday
CHRISTMAS = date(2026, 12, 25)
WEEKEND = date(2026, 8, 22)  # a Saturday


@pytest.fixture
def real() -> ExchangeCalendar:
    return ExchangeCalendar()


def test_real_regular_session_open_and_close(real: ExchangeCalendar):
    assert real.is_session(REGULAR) is True
    assert real.is_early_close(REGULAR) is False
    opened = real.session_open(REGULAR)
    closed = real.session_close(REGULAR)
    assert (opened.hour, opened.minute) == (9, 30)
    assert (closed.hour, closed.minute) == (16, 0)


def test_real_option_close_trails_the_equity_close(real: ExchangeCalendar):
    assert real.option_close(REGULAR) == real.session_close(REGULAR) + OPTION_CLOSE_OFFSET
    option_close = real.option_close(REGULAR)
    assert (option_close.hour, option_close.minute) == (16, 15)


def test_real_early_close_is_short(real: ExchangeCalendar):
    assert real.is_session(EARLY_CLOSE) is True
    assert real.is_early_close(EARLY_CLOSE) is True
    closed = real.session_close(EARLY_CLOSE)
    option_close = real.option_close(EARLY_CLOSE)
    assert (closed.hour, closed.minute) == (13, 0)
    assert (option_close.hour, option_close.minute) == (13, 15)


@pytest.mark.parametrize("day", [GOOD_FRIDAY, CHRISTMAS, WEEKEND])
def test_real_non_sessions(real: ExchangeCalendar, day: date):
    assert real.is_session(day) is False
    assert real.is_early_close(day) is False
    with pytest.raises(NotASession):
        real.session_open(day)
    with pytest.raises(NotASession):
        real.option_close(day)


def test_real_calendar_satisfies_the_protocol(real: ExchangeCalendar):
    assert isinstance(real, Calendar)
