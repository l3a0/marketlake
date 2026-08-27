"""The calendar seam, fake side. FakeCalendar reports the sessions a test declares,
with no real library in the path."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from lake.calendar import OPTION_CLOSE_OFFSET, Calendar, NotASession
from tests.support.calendar import FakeCalendar, SessionTimes

REGULAR = date(2026, 8, 24)  # a plain Monday
EARLY_CLOSE = date(2026, 11, 27)  # the day after Thanksgiving
WEEKEND = date(2026, 8, 22)  # a Saturday


def test_option_close_offset_is_fifteen_minutes():
    assert OPTION_CLOSE_OFFSET == timedelta(minutes=15)


def test_fake_calendar_reports_declared_sessions():
    regular_open = datetime(2026, 8, 24, 9, 30, tzinfo=UTC)
    regular_close = datetime(2026, 8, 24, 16, 0, tzinfo=UTC)
    early_close = datetime(2026, 11, 27, 13, 0, tzinfo=UTC)
    cal = FakeCalendar(
        {
            REGULAR: SessionTimes(open=regular_open, close=regular_close),
            EARLY_CLOSE: SessionTimes(open=regular_open, close=early_close, early_close=True),
        }
    )
    assert cal.is_session(REGULAR) is True
    assert cal.is_session(WEEKEND) is False
    assert cal.is_early_close(REGULAR) is False
    assert cal.is_early_close(EARLY_CLOSE) is True
    assert cal.option_close(REGULAR) == regular_close + OPTION_CLOSE_OFFSET
    assert cal.option_close(EARLY_CLOSE) == early_close + OPTION_CLOSE_OFFSET


def test_fake_calendar_refuses_a_non_session():
    cal = FakeCalendar({})
    with pytest.raises(NotASession):
        cal.session_open(REGULAR)


def test_fake_calendar_satisfies_the_protocol():
    assert isinstance(FakeCalendar({}), Calendar)
