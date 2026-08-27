"""The session clock over the real calendar.

``SessionClock`` composed with the real ``ExchangeCalendar`` and a manual clock.
That crosses one real boundary, the ``exchange_calendars`` library, so it sits in
the component tier. The clock stays fake, per the tier rule. The times named here
are pinned against known 2026 NYSE sessions, which is where naming a time is allowed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from lake.calendar import MARKET_TZ, ExchangeCalendar
from lake.session import SessionClock, SessionPhase
from tests.support.clock import ManualClock

REGULAR = date(2026, 8, 24)  # a plain Monday
EARLY_CLOSE = date(2026, 11, 27)  # the day after Thanksgiving
HOLIDAY = date(2026, 12, 25)  # Christmas


def et(day: date, h: int, m: int) -> datetime:
    return datetime(day.year, day.month, day.day, h, m, tzinfo=MARKET_TZ)


def clock_at(instant_et: datetime) -> ManualClock:
    return ManualClock(start=instant_et.astimezone(UTC))


@pytest.fixture
def calendar() -> ExchangeCalendar:
    return ExchangeCalendar()


def test_real_regular_bounds(calendar: ExchangeCalendar):
    b = SessionClock(clock_at(et(REGULAR, 12, 0)), calendar).bounds(REGULAR)
    assert (b.open.hour, b.open.minute) == (9, 30)
    assert (b.equity_close.hour, b.equity_close.minute) == (16, 0)
    assert (b.option_close.hour, b.option_close.minute) == (16, 15)
    assert (b.canonical_deadline.hour, b.canonical_deadline.minute) == (16, 20)
    assert (b.compaction.hour, b.compaction.minute) == (16, 30)
    assert b.early_close is False


def test_real_early_close_bounds(calendar: ExchangeCalendar):
    b = SessionClock(clock_at(et(EARLY_CLOSE, 12, 0)), calendar).bounds(EARLY_CLOSE)
    assert (b.equity_close.hour, b.equity_close.minute) == (13, 0)
    assert (b.option_close.hour, b.option_close.minute) == (13, 15)
    assert (b.compaction.hour, b.compaction.minute) == (13, 30)
    assert b.early_close is True


@pytest.mark.parametrize(
    "h,m,expected",
    [
        (9, 30, SessionPhase.OPEN),
        (16, 0, SessionPhase.OPEN),  # the spot_close slot
        (16, 1, SessionPhase.POST_EQUITY_CLOSE),
        (16, 15, SessionPhase.POST_EQUITY_CLOSE),  # the canonical slot
        (16, 16, SessionPhase.CLOSED),
    ],
)
def test_real_phase_transitions(calendar: ExchangeCalendar, h: int, m: int, expected: SessionPhase):
    assert SessionClock(clock_at(et(REGULAR, h, m)), calendar).phase() is expected


def test_real_phase_is_non_session_on_a_holiday(calendar: ExchangeCalendar):
    sc = SessionClock(clock_at(et(HOLIDAY, 12, 0)), calendar)
    assert sc.phase() is SessionPhase.NON_SESSION
    assert sc.in_capture_window() is False
