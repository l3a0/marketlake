"""The fake calendar.

``FakeCalendar`` implements the ``Calendar`` seam from a table of sessions a test
declares. So a test decides which sessions and half-days exist, including the awkward
ones the real calendar only rarely produces: an early close, a holiday, a fully dark
stretch.

It reuses the real ``OPTION_CLOSE_OFFSET`` and ``NotASession`` from the calendar
module, so the fake and the real adapter agree on the option-close rule and on how a
non-session is refused. ``FakeCalendar`` carries no session-time literal of its own.
Every time it serves comes from the test through ``SessionTimes``.

``weekday_sessions`` beside it is a convenience for the common case, a run of ordinary
weeks. It supplies the regular session itself, so a test that does not care about the
clock does not have to state it. A test that does care builds its own table and hands
it to ``FakeCalendar`` directly.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from lake.calendar import MARKET_TZ, OPTION_CLOSE_OFFSET, NotASession


@dataclass(frozen=True)
class SessionTimes:
    """The open and close a test assigns to one session."""

    open: datetime
    close: datetime
    early_close: bool = False


class FakeCalendar:
    """A ``Calendar`` whose sessions a test declares."""

    def __init__(self, sessions: Mapping[date, SessionTimes]) -> None:
        self._sessions = dict(sessions)

    def is_session(self, day: date) -> bool:
        return day in self._sessions

    def is_early_close(self, day: date) -> bool:
        session = self._sessions.get(day)
        return bool(session and session.early_close)

    def _require(self, day: date) -> SessionTimes:
        try:
            return self._sessions[day]
        except KeyError:
            raise NotASession(day) from None

    def session_open(self, day: date) -> datetime:
        return self._require(day).open

    def session_close(self, day: date) -> datetime:
        return self._require(day).close

    def option_close(self, day: date) -> datetime:
        return self._require(day).close + OPTION_CLOSE_OFFSET


def et(*args: int) -> datetime:
    """A datetime in market time, from the same positional arguments ``datetime`` takes.

    Every control-plane test states its instants in Eastern time, because that is the
    zone the design pins its wall-clock moments to.
    """
    return datetime(*args, tzinfo=MARKET_TZ)


def weekday_sessions(*mondays: date, holidays: Collection[date] = ()) -> FakeCalendar:
    """Regular sessions on every weekday of each listed week, minus the holidays.

    A regular session opens at 09:30 and closes at 16:00 Eastern, which is what the
    design pins for these tickers. The option close comes from the real
    ``OPTION_CLOSE_OFFSET``, so it lands at 16:15 without this builder naming it.

    Each argument is a Monday, and the weeks need not be adjacent. Passing two builds
    the two-week span a Sunday job needs, because a run looks ahead to the following
    week's wake. Passing one and a holiday builds the short week a Good Friday makes.
    """
    table: dict[date, SessionTimes] = {}
    for monday in mondays:
        if monday.weekday() != 0:
            raise ValueError(f"{monday.isoformat()} is not a Monday")
        for offset in range(5):
            day = monday + timedelta(days=offset)
            if day in holidays:
                continue
            table[day] = SessionTimes(
                open=et(day.year, day.month, day.day, 9, 30),
                close=et(day.year, day.month, day.day, 16, 0),
            )
    return FakeCalendar(table)
