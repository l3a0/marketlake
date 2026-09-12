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
   option close (the ``option_close`` cycle).
3. The close+5 guard: five minutes past the option close, the last moment an
   option-close fill may land.
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
  days. Its cycle is tagged ``option_close``. It is the capture stop.
- *close+5* and *close+15* are durations past the option close. close+5 is pinned
  in code, not config, because it defines option-close semantics.

The module reads the clock only through the injected ``Clock``. It names no session
time of its own. Every session time comes from the calendar. So both enforcement
scanners stay green.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum

from lake.calendar import MARKET_TZ, Calendar, NotASession
from lake.clock import Clock

# The option-close guard window. The last moment after the option close that an
# option-close fill may land. Pinned in code, not config, because it defines
# option-close semantics, not alerting.
OPTION_CLOSE_GUARD = timedelta(minutes=5)

# The two close tags. A tagged row is one of the day's two close cycles, and every row
# of that cycle carries the tag, fill-fetch segments included.
SPOT_CLOSE = "spot_close"
OPTION_CLOSE = "option_close"

# The compaction delay. How long after the option close the close+15 compaction
# job runs. A structural session-relative offset, so it lives in code, not config.
COMPACTION_DELAY = timedelta(minutes=15)


# One capture slot. The loop fires on the minute, so a missed span is counted in these.
TICK = timedelta(minutes=1)

_ONE_DAY = timedelta(days=1)


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


# The two phases the loop captures on: the open through the option close. The daemon
# reads ``phase()`` once per minute and decides capture against this set, so the loop and
# ``in_capture_window`` share one definition of the window.
CAPTURE_PHASES = frozenset({SessionPhase.OPEN, SessionPhase.POST_EQUITY_CLOSE})


@dataclass(frozen=True)
class SessionBounds:
    """Every session-relative moment for one session day, all Eastern-time aware."""

    day: date
    open: datetime  # capture start, the session open
    equity_close: datetime  # the spot_close moment
    option_close: datetime  # the option-close moment, the capture stop
    option_close_deadline: datetime  # close+5, the option-close guard window end
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
            option_close_deadline=option_close + OPTION_CLOSE_GUARD,
            compaction=option_close + COMPACTION_DELAY,
            early_close=self._calendar.is_early_close(day),
        )

    def close_tag_at(self, slot: datetime) -> str | None:
        """The ``close_tag`` a capture slot carries, or ``None`` for every other minute.

        Two cycles a session day are tagged, and they are ordinary loop cycles rather
        than extra fetches. The equity close carries ``spot_close``, the last cycle where
        the option marks and the underlying top of book are read at the same pre-auction
        moment. The option close carries ``option_close``, the option market's close of
        record.

        Both moments come from the calendar for that day, so an early close moves them
        together and nothing keys on a wall-clock 16:00 or 16:15.
        """
        try:
            bounds = self.bounds(slot.date())
        except NotASession:
            return None
        if slot == bounds.equity_close:
            return SPOT_CLOSE
        if slot == bounds.option_close:
            return OPTION_CLOSE
        return None

    def phase(self) -> SessionPhase:
        """The phase of the current snap slot."""
        return self.phase_at(self.snap_slot())

    def phase_at(self, slot: datetime) -> SessionPhase:
        """The phase of any slot, past or present.

        ``phase`` reads the clock. This reads the slot it is handed, so a writer
        stamping a minute that has already gone by can ask the same question. Gap
        marking needs it, because a marker for a post-equity-close minute carries the
        same ``session_phase`` a captured row would have carried.
        """
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
        is the two phases in ``CAPTURE_PHASES``: ``OPEN`` and ``POST_EQUITY_CLOSE``.
        """
        return self.phase() in CAPTURE_PHASES


# -- missed capture slots -------------------------------------------------------


def skipped_slots(bounds: SessionBounds, after: datetime, before: datetime) -> list[datetime]:
    """The capture slots strictly between two slots of one session date, in order.

    It steps one minute at a time from ``after`` toward ``before`` and keeps each slot
    inside the capture window, ``bounds.open`` through ``bounds.option_close`` inclusive.
    Adjacent slots yield nothing. So does a span that lies wholly off the window.
    """
    skipped: list[datetime] = []
    candidate = after + TICK
    while candidate < before:
        if bounds.open <= candidate <= bounds.option_close:
            skipped.append(candidate)
        candidate += TICK
    return skipped


def missed_slots(
    session_clock: SessionClock,
    last_slot: datetime | None,
    slot: datetime,
) -> list[datetime]:
    """The capture slots missed between the previous tick and this one, in order.

    Nothing is missed before the first tick or between adjacent ticks, and only past
    that short-circuit does the calendar get asked, so the normal-cadence path never
    touches it. A wider span is walked one calendar day at a time, from the previous
    tick's date through this one's. A day the calendar refuses as ``NotASession``, a
    weekend, a holiday, or a Saturday wake, contributes nothing. Each session day is
    clipped at its own edges: the first day runs from the previous slot through its
    option close, the last day from its open up to this slot, a middle day end to end,
    and a same-day span from the previous slot to this one. So a stall across days
    reports the first day's tail and the last day's head, and a night jump that touches
    no capture slot reports nothing.
    """
    if last_slot is None or slot - last_slot <= TICK:
        return []
    first_day = last_slot.date()
    last_day = slot.date()
    skipped: list[datetime] = []
    day = first_day
    while day <= last_day:
        try:
            bounds = session_clock.bounds(day)
        except NotASession:
            day += _ONE_DAY
            continue
        after = last_slot if day == first_day else bounds.open - TICK
        before = slot if day == last_day else bounds.option_close + TICK
        skipped.extend(skipped_slots(bounds, after, before))
        day += _ONE_DAY
    return skipped


def session_slots(bounds: SessionBounds) -> list[datetime]:
    """Every capture slot of a session: the open through the option close, one a minute.

    A regular 09:30 to 16:15 day yields exactly 406 slots, both ends inclusive, and an
    early close 226, with no literal here. This is the full set of minutes a session
    owed. The dashboard's completeness strip and gap marking's hole-aware walk both derive
    from it, each applying its own per-ticker scope clamp on top.
    """
    slots: list[datetime] = []
    slot = bounds.open
    while slot <= bounds.option_close:
        slots.append(slot)
        slot += TICK
    return slots


# -- session-relative dispatch ---------------------------------------------------


class SessionDispatch:
    """Fires one callback once per session day, at a moment the calendar decides.

    The design dispatches everything session-relative from inside the daemon, because
    ``StartCalendarInterval`` is fixed wall-clock and cannot express a close-relative
    time. An early close moves the option close, and with it every moment derived from
    it, which a launchd job could not follow.

    The moment is named by a function of the day's bounds rather than by a time, so a
    caller says "close plus five" and the calendar says when that is. The callback runs
    on the first observation at or after that moment and not again that day, whether the
    observation comes from a tick or from the daemon starting up late. A daemon that
    starts at 16:18 therefore still serves a close+5 job for that day.

    Nothing fires for a day the calendar refuses, and nothing fires twice. The day
    already served is the only state it keeps, which is what makes a per-minute caller
    safe.
    """

    def __init__(
        self,
        *,
        session_clock: SessionClock,
        moment: Callable[[SessionBounds], datetime],
        job: Callable[[date], None],
    ) -> None:
        self._session_clock = session_clock
        self._moment = moment
        self._job = job
        self._served: date | None = None

    def check(self, now: datetime) -> bool:
        """Run the job if its moment has passed today and it has not run yet.

        Returns whether the job ran, so a caller can order two dispatches or report
        what a startup pass did.
        """
        eastern = now.astimezone(MARKET_TZ)
        day = eastern.date()
        if day == self._served:
            return False
        try:
            bounds = self._session_clock.bounds(day)
        except NotASession:
            return False
        if eastern < self._moment(bounds):
            return False
        self._served = day
        self._job(day)
        return True
