"""The real-tool helper skips on a developer's machine and fails on CI.

A skip reads as green in a summary, so a helper that skipped on CI too would leave the
systemd checks unrun on the only machine that has systemd, with nothing red to say so.
"""

from __future__ import annotations

import pytest

from tests.support.real_tools import require_tool

ABSENT = "marketlake-no-such-tool"


def test_an_absent_tool_fails_the_test_under_ci(monkeypatch):
    monkeypatch.setenv("CI", "true")
    with pytest.raises(pytest.fail.Exception, match=ABSENT):
        require_tool(ABSENT)


def test_an_absent_tool_skips_the_test_outside_ci(monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    with pytest.raises(pytest.skip.Exception, match=ABSENT):
        require_tool(ABSENT)


def test_a_present_tool_is_found_wherever_the_suite_runs(monkeypatch):
    monkeypatch.setenv("CI", "true")
    assert require_tool("sh").endswith("/sh")
