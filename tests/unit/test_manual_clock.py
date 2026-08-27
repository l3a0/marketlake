"""The clock seam, fake side. ManualClock behaves as the interface promises and never
reads the operating system's clock."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lake.clock import Clock
from tests.support.clock import ManualClock


def test_manual_clock_holds_the_time_a_test_sets():
    start = datetime(2026, 8, 24, 13, 30, tzinfo=UTC)
    clock = ManualClock(start=start)
    assert clock.now() == start
    assert clock.monotonic() == 0.0


def test_manual_clock_advance_moves_wall_and_monotonic_together():
    clock = ManualClock(start=datetime(2026, 8, 24, 13, 30, tzinfo=UTC))
    clock.advance(90)
    assert clock.now() == datetime(2026, 8, 24, 13, 31, 30, tzinfo=UTC)
    assert clock.monotonic() == 90.0


def test_manual_clock_sleep_is_virtual():
    clock = ManualClock(start=datetime(2026, 8, 24, 13, 30, tzinfo=UTC))
    before = clock.monotonic()
    clock.sleep(5)
    assert clock.monotonic() == before + 5
    assert clock.now() == datetime(2026, 8, 24, 13, 30, 5, tzinfo=UTC)


def test_manual_clock_set_jumps_wall_but_not_monotonic():
    clock = ManualClock(start=datetime(2026, 8, 24, 13, 30, tzinfo=UTC))
    clock.advance(10)
    clock.set(datetime(2026, 8, 25, 13, 30, tzinfo=UTC))
    assert clock.now() == datetime(2026, 8, 25, 13, 30, tzinfo=UTC)
    assert clock.monotonic() == 10.0


def test_manual_clock_requires_timezone_aware_time():
    with pytest.raises(ValueError):
        ManualClock(start=datetime(2026, 8, 24, 13, 30))


def test_manual_clock_satisfies_the_clock_protocol():
    clock = ManualClock(start=datetime(2026, 8, 24, 13, 30, tzinfo=UTC))
    assert isinstance(clock, Clock)
