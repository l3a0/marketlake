"""Find a real system tool for a test that needs one, or say why the test cannot run.

A few checks can only be made by the tool that will read the files on the host, such as
``systemd-analyze`` reading the rendered units. That tool exists on CI's Linux runner and
on no Mac. So a test that needs it skips where the tool is absent, which is every Mac,
and fails where it is absent under ``CI``. A skip on CI would leave the check silently
unrun on the one machine that can make it.
"""

from __future__ import annotations

import os
import shutil

import pytest


def require_tool(name: str) -> str:
    """The path of ``name`` on ``PATH``. Skips the test when it is absent, or fails it
    when it is absent and ``CI`` is set, as GitHub Actions sets it."""
    path = shutil.which(name)
    if path is not None:
        return path
    if os.environ.get("CI"):
        pytest.fail(f"{name} is not on PATH, and CI is set, so this check would go unrun")
    pytest.skip(f"{name} is not on PATH here, and only CI requires it")
