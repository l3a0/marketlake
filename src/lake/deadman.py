"""The capture dead-man: the ping whose silence pages.

The watchdog catches one surface going quiet on a living daemon. Nothing inside a dead
daemon can report that it died, so that case is left to a check outside the machine.
The daemon pings a healthchecks slug, healthchecks expects the ping, and its absence is
what pages. A process that is gone cannot suppress it.

The ping is fed by two things. Every durable capture cycle feeds it, because a cycle
that produced data is the strongest possible evidence the daemon is alive and working.
And an idle heartbeat feeds it whenever the daemon is awake but has nothing to capture,
so a holiday or the minutes before the open do not read as death.

The heartbeat covers the whole weekday envelope rather than only the cases the design
first named. With a five-minute grace, an expectation that started at the open and a
first capture ping landing just after it sat on the edge every morning. A holiday that
heartbeat only through the morning paged shortly after noon. Covering the envelope
removes both, and costs one HTTP GET a minute on days that capture nothing.

That envelope is the caffeinate assertion window, not a second definition of it. The
daemon is kept awake for exactly the span it is expected to be heard from.

Nothing here decides whether the daemon is healthy. It reports that the daemon is
running, and healthchecks decides, because a judgement made inside the process dies
with it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from lake.calendar import MARKET_TZ
from lake.control_plane import assertion_window
from lake.session import SessionClock

# The dead-man check the daemon feeds. Slice 1's ``slice1-capture`` check retires when
# this takes over: leaving the old row in place makes it go silent and page for a job
# that no longer runs, so deleting it is an operator step in the install text.
CAPTURE_SLUG = "capture"

# How often an idle heartbeat goes out. The loop ticks every minute, and a ping a minute
# is the same rate a capturing day already sends.
HEARTBEAT_INTERVAL = timedelta(minutes=1)


def in_envelope(now: datetime) -> bool:
    """Whether ``now`` falls in the window the daemon should be awake for.

    That window is the caffeinate assertion window, and reusing it is deliberate. It is
    already defined as "whenever any healthchecks expectation window is open", it
    already runs from the firmware wake to the sweep's ping on a weekday session or not,
    and its docstring already says a holiday is exactly when the idle heartbeats must
    keep flowing. A second definition of the same window would be one more thing to keep
    in step, and the two going out of step is how a false page gets built.

    Saturday owes nothing, so nothing is expected and nothing heartbeats.
    """
    eastern = now.astimezone(MARKET_TZ)
    window = assertion_window(eastern.date())
    return window is not None and window.contains(eastern)


class DeadMan:
    """Feeds the capture check, from a cycle or from an idle minute.

    The pinger is injected and its failures are swallowed. A ping that does not land is
    what the check is for, and a daemon that crashed while reporting itself alive would
    turn a missing minute into a missing session.
    """

    def __init__(self, *, pinger, url: str, session_clock: SessionClock) -> None:
        self._pinger = pinger
        self._url = url
        self._session_clock = session_clock
        self._last: datetime | None = None

    def captured(self, now: datetime) -> bool:
        """Feed the check for a cycle that produced durable data."""
        return self._ping(now)

    def idle(self, now: datetime) -> bool:
        """Feed the check for a minute inside the envelope with nothing to capture.

        A minute the loop would capture in is left to ``captured``, so a session minute
        that produced nothing never gets a heartbeat standing in for the data it owed.
        That is the watchdog's business, and a heartbeat there would tell healthchecks
        the daemon is fine while a surface is dead.
        """
        if not in_envelope(now):
            return False
        if self._session_clock.in_capture_window():
            return False
        if self._last is not None and now - self._last < HEARTBEAT_INTERVAL:
            return False
        return self._ping(now)

    def _ping(self, now: datetime) -> bool:
        self._last = now
        try:
            self._pinger.ping(self._url)
        except Exception:  # noqa: BLE001 - a missed ping is what the check is for
            return False
        return True


__all__ = [
    "CAPTURE_SLUG",
    "HEARTBEAT_INTERVAL",
    "DeadMan",
    "in_envelope",
]
