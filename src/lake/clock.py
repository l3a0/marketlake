"""The clock module.

This is the one place in production code that reads wall-clock time. Everything
else takes a ``Clock`` and asks it. So a test passes a fake clock and decides what
time it is. That is the injected-clock seam.

A seam is an injection point where a real dependency is swapped for a fake one in a
test. The clock-seam enforcement test (``tests/test_seam_clock.py``) fails the build
on any direct wall-clock call anywhere under ``src/lake`` outside this file. So this
file is the only sanctioned caller of ``datetime.now``, ``time.monotonic``, and
``time.sleep``. It also owns the one wait on threads that gives up at an instant, since
giving up at an instant is a question about what time it is.

Two rules keep the seam honest.

1. ``now`` returns a timezone-aware instant in UTC. Session-relative times live in
   the calendar module, never here. A caller that needs Eastern time converts.
2. ``monotonic`` is for elapsed-time measurement only. It is not a wall clock. It
   never goes backward and has no relation to the calendar.
"""

from __future__ import annotations

import time as _time
from collections.abc import Collection
from concurrent.futures import FIRST_COMPLETED, Future
from concurrent.futures import wait as _wait
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

    def wait(self, futures: Collection[Future], until: datetime | None) -> set[Future]:
        """Block until any of ``futures`` is done or ``until`` arrives, and return the done ones.

        It returns as soon as one future is done, like ``concurrent.futures.wait`` with
        ``FIRST_COMPLETED``, and the set it returns is every future found done at that
        moment. With none done by ``until`` it returns an empty set. An ``until`` of ``None``
        waits for the first completion however long that takes.

        An empty return does not promise that ``now`` has reached ``until``. A caller that
        must not give up early reads ``now`` again and waits again while it is still short.
        The daemon's sleep to a minute top keeps the same rule (marketlake #572).
        """
        ...


class SystemClock:
    """The real clock. It reads the operating system's wall clock and timer."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return _time.monotonic()

    def sleep(self, seconds: float) -> None:
        _time.sleep(seconds)

    def wait(self, futures: Collection[Future], until: datetime | None) -> set[Future]:
        # The timeout runs on the monotonic timer while ``until`` is a wall-clock instant, so
        # the two can disagree by the time this returns. That is why the caller re-reads.
        timeout = None if until is None else max(0.0, (until - self.now()).total_seconds())
        done, _ = _wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)
        return set(done)
