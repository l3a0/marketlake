"""Enforcement test: no hardcoded session time in production code.

This test stays in continuous integration for the life of the project. It fails the
build on any hardcoded session time anywhere under ``src/lake`` outside the calendar
module. So the injected-calendar seam cannot be quietly bypassed later.

The scanner's positive detection, that it actually catches a hardcoded time, is
proven in ``test_enforcement.py``. The calendar module derives every time from
``exchange_calendars`` and carries no literal, so there is nothing in production to
arm this test against.
"""

from __future__ import annotations

from tests.support.enforcement import find_session_time_violations


def test_production_code_has_no_hardcoded_session_times():
    violations = find_session_time_violations()
    assert not violations, "hardcoded session times outside the calendar module:\n" + "\n".join(
        str(v) for v in violations
    )
