"""The clock module.

This is the one place in production code that reads wall-clock time. Everything
else takes a ``Clock`` and asks it. So a test passes a fake clock and decides what
time it is. That is the injected-clock seam.

A seam is an injection point where a real dependency is swapped for a fake one in a
test. The clock-seam enforcement test (``tests/test_seam_clock.py``) fails the build
on any direct wall-clock call anywhere under ``src/lake`` outside this file. So this
file is the only sanctioned caller of ``datetime.now``, ``time.monotonic``, and
``time.sleep``.

Two rules keep the seam honest.

1. ``now`` returns a timezone-aware instant in UTC. Session-relative times live in
   the calendar module, never here. A caller that needs Eastern time converts.
2. ``monotonic`` is for elapsed-time measurement only. It is not a wall clock. It
   never goes backward and has no relation to the calendar.
"""

from __future__ import annotations

import time as _time
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """The time source the rest of the system depends on."""

    def now(self) -> datetime:
        """The current instant, timezone-aware in UTC."""
        ...

    def monotonic(self) -> float:
        """A monotonic timer in seconds, for measuring elapsed time."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block for the given number of seconds."""
        ...


class SystemClock:
    """The real clock. It reads the operating system's wall clock and timer."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return _time.monotonic()

    def sleep(self, seconds: float) -> None:
        _time.sleep(seconds)
