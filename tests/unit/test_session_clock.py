"""The session clock, decided from values alone.

Every case here sets a manual clock and declares a fake calendar, then reads back
the phase, the snap slot, and the session's moments. No real library, file, or
network is in the path, so these are unit tests. The times named here are declared
through the fakes, which is exactly where naming a time is allowed.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta

import pytest

from lake.calendar import MARKET_TZ, NotASession
from lake.session import (
    CANONICAL_GUARD,
    COMPACTION_DELAY,
    SessionBounds,
    SessionClock,
    SessionPhase,
)
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock

ET = MARKET_TZ
REGULAR = date(2026, 8, 24)  # a summer Monday, Eastern daylight time (UTC-4)
EARLY_CLOSE = date(2026, 11, 27)  # day after Thanksgiving, a half day, standard time (UTC-5)
WINTER = date(2026, 12, 22)  # a regular winter Tuesday, standard time (UTC-5)
HOLIDAY = date(2026, 12, 25)  # Christmas, not a session
WEEKEND = date(2026, 8, 22)  # a Saturday


def et(day: date, h: int, m: int, s: int = 0) -> datetime:
    """An Eastern-time instant on ``day``."""
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=ET)


@pytest.fixture
def calendar() -> FakeCalendar:
    return FakeCalendar(
        {
            REGULAR: SessionTimes(open=et(REGULAR, 9, 30), close=et(REGULAR, 16, 0)),
            EARLY_CLOSE: SessionTimes(
                open=et(EARLY_CLOSE, 9, 30), close=et(EARLY_CLOSE, 13, 0), early_close=True
            ),
            WINTER: SessionTimes(open=et(WINTER, 9, 30), close=et(WINTER, 16, 0)),
        }
    )


def session_clock_at(calendar: FakeCalendar, day: date, h: int, m: int, s: int = 0) -> SessionClock:
    """A session clock whose ``now`` is the given Eastern-time instant, held as UTC."""
    return SessionClock(ManualClock(start=et(day, h, m, s).astimezone(UTC)), calendar)


def test_guard_offsets_are_pinned():
    assert CANONICAL_GUARD == timedelta(minutes=5)
    assert COMPACTION_DELAY == timedelta(minutes=15)


def test_snap_slot_floors_to_the_eastern_minute(calendar: FakeCalendar):
    sc = session_clock_at(calendar, REGULAR, 10, 31, 47)
    slot = sc.snap_slot()
    assert slot == et(REGULAR, 10, 31)
    assert slot.utcoffset() == timedelta(hours=-4)  # Eastern daylight time


def test_snap_slot_zeroes_seconds_and_microseconds(calendar: FakeCalendar):
    # 14:31:47.5 UTC is 10:31:47.5 Eastern daylight time.
    clock = ManualClock(start=datetime(2026, 8, 24, 14, 31, 47, 500000, tzinfo=UTC))
    slot = SessionClock(clock, calendar).snap_slot()
    assert (slot.hour, slot.minute, slot.second, slot.microsecond) == (10, 31, 0, 0)


def test_snap_slot_tracks_the_winter_offset(calendar: FakeCalendar):
    sc = session_clock_at(calendar, WINTER, 10, 31)
    slot = sc.snap_slot()
    assert slot == et(WINTER, 10, 31)
    assert slot.utcoffset() == timedelta(hours=-5)  # Eastern standard time


def test_session_date_is_the_eastern_date_across_midnight_utc(calendar: FakeCalendar):
    # 03:00 UTC on the 25th is 23:00 Eastern on the 24th, still the 24th's session.
    clock = ManualClock(start=datetime(2026, 8, 25, 3, 0, tzinfo=UTC))
    assert SessionClock(clock, calendar).session_date() == REGULAR


def test_bounds_reports_every_session_moment(calendar: FakeCalendar):
    b = session_clock_at(calendar, REGULAR, 12, 0).bounds(REGULAR)
    assert b.day == REGULAR
    assert b.open == et(REGULAR, 9, 30)
    assert b.equity_close == et(REGULAR, 16, 0)
    assert b.option_close == et(REGULAR, 16, 15)
    assert b.canonical_deadline == et(REGULAR, 16, 20)  # close+5
    assert b.compaction == et(REGULAR, 16, 30)  # close+15
    assert b.early_close is False


def test_bounds_on_an_early_close_is_short(calendar: FakeCalendar):
    b = session_clock_at(calendar, EARLY_CLOSE, 12, 0).bounds(EARLY_CLOSE)
    assert b.equity_close == et(EARLY_CLOSE, 13, 0)
    assert b.option_close == et(EARLY_CLOSE, 13, 15)
    assert b.canonical_deadline == et(EARLY_CLOSE, 13, 20)
    assert b.compaction == et(EARLY_CLOSE, 13, 30)
    assert b.early_close is True


def test_bounds_refuses_a_non_session(calendar: FakeCalendar):
    sc = session_clock_at(calendar, HOLIDAY, 12, 0)
    with pytest.raises(NotASession):
        sc.bounds(HOLIDAY)


def test_bounds_is_a_frozen_dataclass(calendar: FakeCalendar):
    b = session_clock_at(calendar, REGULAR, 12, 0).bounds(REGULAR)
    assert isinstance(b, SessionBounds)
    with pytest.raises(FrozenInstanceError):
        b.open = b.equity_close  # type: ignore[misc]


@pytest.mark.parametrize(
    "h,m,s,expected",
    [
        (2, 0, 0, SessionPhase.PRE_OPEN),
        (9, 29, 59, SessionPhase.PRE_OPEN),
        (9, 30, 0, SessionPhase.OPEN),  # the first cycle
        (12, 0, 0, SessionPhase.OPEN),
        (15, 59, 59, SessionPhase.OPEN),
        (16, 0, 0, SessionPhase.OPEN),  # the spot_close slot, still synchronous
        (16, 0, 30, SessionPhase.OPEN),  # still serving the 16:00 slot
        (16, 1, 0, SessionPhase.POST_EQUITY_CLOSE),
        (16, 15, 0, SessionPhase.POST_EQUITY_CLOSE),  # the canonical slot, still capturing
        (16, 15, 30, SessionPhase.POST_EQUITY_CLOSE),  # still serving the 16:15 slot
        (16, 16, 0, SessionPhase.CLOSED),
        (23, 0, 0, SessionPhase.CLOSED),
    ],
)
def test_phase_transitions_on_a_regular_day(
    calendar: FakeCalendar, h: int, m: int, s: int, expected: SessionPhase
):
    assert session_clock_at(calendar, REGULAR, h, m, s).phase() is expected


@pytest.mark.parametrize(
    "h,m,expected",
    [
        (9, 30, SessionPhase.OPEN),
        (13, 0, SessionPhase.OPEN),  # the early spot_close slot
        (13, 1, SessionPhase.POST_EQUITY_CLOSE),
        (13, 15, SessionPhase.POST_EQUITY_CLOSE),  # the early canonical slot
        (13, 16, SessionPhase.CLOSED),
    ],
)
def test_phase_transitions_on_an_early_close(
    calendar: FakeCalendar, h: int, m: int, expected: SessionPhase
):
    assert session_clock_at(calendar, EARLY_CLOSE, h, m).phase() is expected


@pytest.mark.parametrize("day", [WEEKEND, HOLIDAY])
def test_phase_is_non_session_off_the_calendar(calendar: FakeCalendar, day: date):
    assert session_clock_at(calendar, day, 12, 0).phase() is SessionPhase.NON_SESSION


def test_phase_uses_eastern_time_in_winter(calendar: FakeCalendar):
    # 21:00 UTC in December is 16:00 Eastern standard time, the spot_close slot.
    clock = ManualClock(start=datetime(2026, 12, 22, 21, 0, tzinfo=UTC))
    sc = SessionClock(clock, calendar)
    assert sc.phase() is SessionPhase.OPEN
    clock.set(datetime(2026, 12, 22, 21, 1, tzinfo=UTC))  # 16:01 Eastern
    assert sc.phase() is SessionPhase.POST_EQUITY_CLOSE


def test_phase_follows_the_injected_clock(calendar: FakeCalendar):
    clock = ManualClock(start=et(REGULAR, 9, 29).astimezone(UTC))
    sc = SessionClock(clock, calendar)
    assert sc.phase() is SessionPhase.PRE_OPEN
    clock.advance(120)  # to 09:31
    assert sc.phase() is SessionPhase.OPEN


@pytest.mark.parametrize(
    "h,m,expected",
    [
        (9, 29, False),
        (9, 30, True),  # the open
        (12, 0, True),
        (16, 0, True),  # the spot_close slot
        (16, 15, True),  # the canonical slot
        (16, 16, False),  # past the option close
    ],
)
def test_in_capture_window_spans_open_through_option_close(
    calendar: FakeCalendar, h: int, m: int, expected: bool
):
    assert session_clock_at(calendar, REGULAR, h, m).in_capture_window() is expected


def test_in_capture_window_is_false_off_a_session(calendar: FakeCalendar):
    assert session_clock_at(calendar, HOLIDAY, 12, 0).in_capture_window() is False
