"""The fake clock.

``ManualClock`` implements the ``Clock`` seam without ever reading the operating
system's clock. A test sets the time and advances it by hand. So a test decides what
time it is, and sleeps cost no real seconds.

``sleep`` advances virtual time. A loop that sleeps under this clock moves forward
deterministically instead of blocking, and ``monotonic`` tracks the same elapsed
span as ``now``.

``wait`` is the one place this clock spends real time, because the futures it waits on
run on real threads. It gives them a short real grace to finish. When none does, it moves
virtual time forward to ``until`` and returns empty, the way a real wait times out.

More than one thread moves this clock. The daemon runs each minute's cycle on a thread of
its own, and that cycle sleeps its stagger and waits on its bound through the clock while
the loop thread waits on the cycles (marketlake #565). ``advance`` and ``set`` are a read
then a write, so two threads moving the clock at once could lose one move. A lock covers
both, and covers ``wait``'s move forward to ``until``, so every move lands whole.
"""

from __future__ import annotations

import threading
from collections.abc import Collection
from concurrent.futures import FIRST_COMPLETED, Future, wait
from datetime import datetime, timedelta

# How long ``wait`` lets real threads run before it decides none of them will finish by
# ``until``. The fakes the suite waits on answer in milliseconds, or hold on an event until
# the test releases them, so a healthy task is done long before this and a held one is not
# done at any grace. A poll with no grace cut a healthy window in 129 of 200 runs
# (marketlake #534), so the grace is generous rather than tight.
WAIT_GRACE_SECONDS = 1.0


class ManualClock:
    """A ``Clock`` whose time a test controls."""

    def __init__(
        self, start: datetime, monotonic: float = 0.0, *, grace: float = WAIT_GRACE_SECONDS
    ) -> None:
        """``grace`` is the real time ``wait`` gives a future before it moves forward.

        A case whose held futures spend the whole grace on every wait passes a shorter one.
        """
        if start.tzinfo is None:
            raise ValueError("ManualClock start must be timezone-aware")
        self._now = start
        self._monotonic = monotonic
        self._grace = grace
        # Re-entrant, because ``wait`` holds it while it moves forward through ``sleep``,
        # which a subclass may override and which lands in ``advance``.
        self._lock = threading.RLock()

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def wait(self, futures: Collection[Future], until: datetime | None) -> set[Future]:
        """Return the futures done after a real grace, or move forward to ``until``.

        With ``until`` of ``None`` it blocks for real until the first one is done, since
        there is no instant to give up at. Otherwise it moves forward through ``sleep``, so
        a subclass that overrides ``sleep`` still applies, and never backwards.
        """
        timeout = None if until is None else self._grace
        done, _ = wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)
        if done or until is None:
            return set(done)
        with self._lock:
            remaining = (until - self.now()).total_seconds()
            if remaining > 0:
                self.sleep(remaining)
        return set()

    def advance(self, seconds: float) -> None:
        """Move both the wall clock and the monotonic timer forward."""
        with self._lock:
            self._now = self._now + timedelta(seconds=seconds)
            self._monotonic += seconds

    def set(self, when: datetime) -> None:
        """Jump the wall clock to a chosen instant. The monotonic timer is unmoved."""
        if when.tzinfo is None:
            raise ValueError("ManualClock time must be timezone-aware")
        with self._lock:
            self._now = when


class CostlyClock(ManualClock):
    """A manual clock where every ``now`` read costs ``cost`` seconds.

    A plain ``ManualClock`` stands still between two reads, so a test on it cannot tell
    one read from two taken back to back. Here each read moves the clock on, so a clock
    started a few reads short of a minute top lets two back-to-back reads fall on either
    side of it, through the real code path and with no advance placed by hand.
    """

    def __init__(self, start: datetime, cost: float = 0.000001) -> None:
        super().__init__(start)
        self._cost = cost

    def now(self) -> datetime:
        instant = super().now()
        self.advance(self._cost)
        return instant
