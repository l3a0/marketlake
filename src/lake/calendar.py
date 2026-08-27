"""The calendar module.

This is the one place in production code that names session times. Which days are
sessions, which are half-days, and when each session opens and closes all come from
``exchange_calendars``. So a test passes a fake calendar and decides which sessions
and half-days exist. That is the injected-calendar seam.

``exchange_calendars`` is rules-plus-exceptions source code, not a feed. It learns
about schedule changes only through package releases. It knows unscheduled closures
only retroactively, so the daemon guards that gap elsewhere.

The calendar-seam enforcement test (``tests/test_seam_calendar.py``) fails the build
on any hardcoded session time anywhere under ``src/lake`` outside this file. So this
file is the only sanctioned home for a session-time literal. The adapter below
derives every time from ``exchange_calendars`` and carries none.

Definitions used throughout the doc and here.

- The equity *session close* is the closing-auction moment, 16:00 on regular days
  and 13:00 on the early-close days. It comes straight from the calendar.
- The *option close* trails it by a fixed 15 minutes for these ETFs. So it is 16:15
  on regular days and 13:15 on early closes. It is a duration past the session
  close, never a wall-clock literal.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Protocol, runtime_checkable
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

# The option close trails the equity session close by this fixed span for SPY/QQQ.
OPTION_CLOSE_OFFSET = timedelta(minutes=15)

# The exchange the ETFs settle against. The US market's holiday and half-day rules
# are shared across venues, so the NYSE calendar governs sessions for SPY and QQQ.
DEFAULT_CALENDAR = "XNYS"

# Times are reported in this zone, matching the doc's Eastern-time framing.
_MARKET_TZ = "America/New_York"

# The same zone as a tzinfo, for pure-Python conversions. The session clock floors
# ``now`` to the Eastern-time minute and asks the calendar about the Eastern date,
# so it needs the zone as an object. ``_MARKET_TZ`` stays the string the pandas
# conversions below pass to ``tz_convert``.
MARKET_TZ = ZoneInfo(_MARKET_TZ)


class NotASession(Exception):
    """Raised when a session time is asked for on a day that is not a session."""

    def __init__(self, day: date) -> None:
        super().__init__(f"{day.isoformat()} is not a session")
        self.day = day


@runtime_checkable
class Calendar(Protocol):
    """The session source the rest of the system depends on."""

    def is_session(self, day: date) -> bool:
        """Whether the market trades on ``day``."""
        ...

    def is_early_close(self, day: date) -> bool:
        """Whether ``day`` is a session with a shortened schedule (a half-day)."""
        ...

    def session_open(self, day: date) -> datetime:
        """The equity open, timezone-aware. Raises ``NotASession`` off a session."""
        ...

    def session_close(self, day: date) -> datetime:
        """The equity close (the auction moment), timezone-aware."""
        ...

    def option_close(self, day: date) -> datetime:
        """The option close: the equity close plus the fixed option-close offset."""
        ...


class ExchangeCalendar:
    """The real calendar, backed by ``exchange_calendars``.

    It defaults to the NYSE calendar. Every time it returns is converted to Eastern
    time, so callers read the same clock the doc speaks in.
    """

    def __init__(self, name: str = DEFAULT_CALENDAR) -> None:
        self._cal = xcals.get_calendar(name)

    @property
    def name(self) -> str:
        return self._cal.name

    def _session(self, day: date) -> pd.Timestamp:
        return pd.Timestamp(day)

    def is_session(self, day: date) -> bool:
        return bool(self._cal.is_session(self._session(day)))

    def is_early_close(self, day: date) -> bool:
        return self._session(day) in self._cal.early_closes

    def _require_session(self, day: date) -> pd.Timestamp:
        session = self._session(day)
        if not self._cal.is_session(session):
            raise NotASession(day)
        return session

    def session_open(self, day: date) -> datetime:
        session = self._require_session(day)
        return self._cal.session_open(session).tz_convert(_MARKET_TZ).to_pydatetime()

    def session_close(self, day: date) -> datetime:
        session = self._require_session(day)
        return self._cal.session_close(session).tz_convert(_MARKET_TZ).to_pydatetime()

    def option_close(self, day: date) -> datetime:
        return self.session_close(day) + OPTION_CLOSE_OFFSET
