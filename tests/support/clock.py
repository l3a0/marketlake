"""The fake clock.

``ManualClock`` implements the ``Clock`` seam without ever reading the operating
system's clock. A test sets the time and advances it by hand. So a test decides what
time it is, and sleeps cost no real seconds.

``sleep`` advances virtual time. A loop that sleeps under this clock moves forward
deterministically instead of blocking, and ``monotonic`` tracks the same elapsed
span as ``now``.
"""

from __future__ import annotations

from datetime import datetime, timedelta


class ManualClock:
    """A ``Clock`` whose time a test controls."""

    def __init__(self, start: datetime, monotonic: float = 0.0) -> None:
        if start.tzinfo is None:
            raise ValueError("ManualClock start must be timezone-aware")
        self._now = start
        self._monotonic = monotonic

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        """Move both the wall clock and the monotonic timer forward."""
        self._now = self._now + timedelta(seconds=seconds)
        self._monotonic += seconds

    def set(self, when: datetime) -> None:
        """Jump the wall clock to a chosen instant. The monotonic timer is unmoved."""
        if when.tzinfo is None:
            raise ValueError("ManualClock time must be timezone-aware")
        self._now = when
