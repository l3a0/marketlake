"""The fake calendar.

``FakeCalendar`` implements the ``Calendar`` seam from a table of sessions a test
declares. So a test decides which sessions and half-days exist, including the awkward
ones the real calendar only rarely produces: an early close, a holiday, a fully dark
stretch.

It reuses the real ``OPTION_CLOSE_OFFSET`` and ``NotASession`` from the calendar
module, so the fake and the real adapter agree on the option-close rule and on how a
non-session is refused. The fake carries no session-time literal of its own. Every
time comes from the test through ``SessionTimes``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime

from lake.calendar import OPTION_CLOSE_OFFSET, NotASession


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
