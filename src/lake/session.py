"""The session clock.

This turns "what time is it" into "where are we in the trading session." It reads
the injected clock for ``now`` and the injected calendar for the day's session
boundaries. So a test sets the clock and declares the calendar, then this module
reports the phase, the current minute slot, and the session's key moments, all
decided from those two inputs alone.

Why it exists. launchd schedules on fixed wall-clock times. It cannot express a
time like "fifteen minutes after this session's option close." The design pins
every intraday time as session-relative, derived per day from the calendar. This
module is the internal dispatcher that does the deriving. It covers the four
session-relative moments the design enumerates.

1. Capture start and stop: the session open and the option close.
2. The close tags' moments: the equity close (the ``spot_close`` cycle) and the
   option close (the ``canonical`` cycle).
3. The close+5 guard: five minutes past the option close, the last moment a
   canonical fill may land.
4. The close+15 compaction: fifteen minutes past the option close.

Definitions used here, following the design doc.

- The *snap slot* is the minute a cycle fires for. It is ``now`` floored to the
  minute in Eastern time. The loop assigns it at the top of the minute. It is not
  the fetch time and not the vendor quote time. Flooring the seconds off ``now`` is
  the one time-of-day construction the calendar-seam enforcement allows outside the
  calendar module.
- The *equity close* is the closing-auction moment, 16:00 on regular days. Its
  cycle is tagged ``spot_close``. It is the last regular-session-synchronous minute.
- The *option close* is the equity close plus fifteen minutes, 16:15 on regular
  days. Its cycle is tagged ``canonical``. It is the capture stop.
- *close+5* and *close+15* are durations past the option close. close+5 is pinned
  in code, not config, because it defines canonical-close semantics.

The module reads the clock only through the injected ``Clock``. It names no session
time of its own. Every session time comes from the calendar. So both enforcement
scanners stay green.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum

from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock

# The canonical-guard window. The last moment after the option close that a
# canonical fill may land. Pinned in code, not config, because it defines
# canonical-close semantics, not alerting.
CANONICAL_GUARD = timedelta(minutes=5)

# The compaction delay. How long after the option close the close+15 compaction
# job runs. A structural session-relative offset, so it lives in code, not config.
COMPACTION_DELAY = timedelta(minutes=15)


class SessionPhase(Enum):
    """Where the current snap slot sits in the trading session.

    The phase is judged on the snap slot, the minute the loop is serving, not the
    sub-minute instant. So the whole of a boundary minute reads as that minute's
    phase. The option-close minute, for one, stays a capture slot end to end.
    """

    NON_SESSION = "non_session"  # the calendar calls today closed
    PRE_OPEN = "pre_open"  # a session day, before the open
    OPEN = "open"  # the open through the equity close, options and underlying synchronous
    POST_EQUITY_CLOSE = "post_equity_close"  # past the equity close, options still trading
    CLOSED = "closed"  # a session day, past the option close


@dataclass(frozen=True)
class SessionBounds:
    """Every session-relative moment for one session day, all Eastern-time aware."""

    day: date
    open: datetime  # capture start, the session open
    equity_close: datetime  # the spot_close moment
    option_close: datetime  # the canonical moment, the capture stop
    canonical_deadline: datetime  # close+5, the canonical-guard window end
    compaction: datetime  # close+15, when the compaction job runs
    early_close: bool


class SessionClock:
    """Session-relative time, from an injected clock and calendar."""

    def __init__(self, clock: Clock, calendar: Calendar) -> None:
        self._clock = clock
        self._calendar = calendar

    def _now_et(self) -> datetime:
        """``now`` in Eastern time. The clock returns UTC, so this converts it."""
        return self._clock.now().astimezone(MARKET_TZ)

    def snap_slot(self) -> datetime:
        """The current minute slot: ``now`` floored to the minute in Eastern time."""
        return self._now_et().replace(second=0, microsecond=0)

    def session_date(self) -> date:
        """The Eastern-time calendar date of ``now``, which names the session."""
        return self._now_et().date()

    def bounds(self, day: date) -> SessionBounds:
        """Every session-relative moment for ``day``.

        Raises ``NotASession`` off a session, the same refusal the calendar gives.
        """
        option_close = self._calendar.option_close(day)
        return SessionBounds(
            day=day,
            open=self._calendar.session_open(day),
            equity_close=self._calendar.session_close(day),
            option_close=option_close,
            canonical_deadline=option_close + CANONICAL_GUARD,
            compaction=option_close + COMPACTION_DELAY,
            early_close=self._calendar.is_early_close(day),
        )

    def phase(self) -> SessionPhase:
        """The phase of the current snap slot."""
        slot = self.snap_slot()
        day = slot.date()
        if not self._calendar.is_session(day):
            return SessionPhase.NON_SESSION
        bounds = self.bounds(day)
        if slot < bounds.open:
            return SessionPhase.PRE_OPEN
        if slot <= bounds.equity_close:
            return SessionPhase.OPEN
        if slot <= bounds.option_close:
            return SessionPhase.POST_EQUITY_CLOSE
        return SessionPhase.CLOSED

    def in_capture_window(self) -> bool:
        """Whether the current snap slot is a capture slot.

        The capture window runs from the session open through the option close. It
        is the two phases the loop captures on: ``OPEN`` and ``POST_EQUITY_CLOSE``.
        """
        return self.phase() in (SessionPhase.OPEN, SessionPhase.POST_EQUITY_CLOSE)
