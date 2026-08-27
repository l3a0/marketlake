"""The clock seam, real side. SystemClock reads the operating system's clock and
timer. That is one real boundary, so this sits in the component tier."""

from __future__ import annotations

from datetime import UTC

from lake.clock import Clock, SystemClock


def test_system_clock_now_is_utc_aware_and_ordered():
    clock = SystemClock()
    first = clock.now()
    second = clock.now()
    assert first.tzinfo == UTC
    assert second >= first


def test_system_clock_monotonic_never_goes_backward():
    clock = SystemClock()
    first = clock.monotonic()
    second = clock.monotonic()
    assert isinstance(first, float)
    assert second >= first


def test_system_clock_satisfies_the_clock_protocol():
    assert isinstance(SystemClock(), Clock)
