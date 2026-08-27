"""Enforcement test: no direct wall-clock call in production code.

This test stays in continuous integration for the life of the project. It fails the
build on any direct wall-clock call anywhere under ``src/lake`` outside the clock
module. So the injected-clock seam cannot be quietly bypassed later.
"""

from __future__ import annotations

from tests.support.enforcement import find_clock_violations


def test_production_code_has_no_direct_wall_clock_calls():
    violations = find_clock_violations()
    assert not violations, "direct wall-clock calls outside the clock module:\n" + "\n".join(
        str(v) for v in violations
    )


def test_the_scanner_is_armed():
    # With no file carved out, the scanner must see the clock module's own real
    # wall-clock calls. If it does not, a passing suite would prove nothing.
    assert find_clock_violations(allow=set()), "clock scanner detected nothing; it is broken"
