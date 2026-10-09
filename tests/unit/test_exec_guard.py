"""The exec guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a test that reaches the vendor sweep's
hand-off without patching it fails, rather than replacing the pytest process with
``python -m lake.compact``. It is autouse, so every test depends on it and no test asserts
it. These do.

Three properties carry the guard.

1. ``os.execv`` and ``os.execve`` are both refused, and the refusal names the program.
2. The refusal is neither an ``OSError`` nor an ``Exception``. ``sweep.main`` catches
   ``OSError`` around its ``exec`` and returns 1, so a guard of that kind would turn into an
   exit code that four of the tests reaching the hand-off do not assert.
3. A recorder a test patches onto ``os.execv`` replaces the guard for that test only.
"""

from __future__ import annotations

import os
import sys

import pytest

from tests.conftest import ExecInTest


def test_execv_is_refused_and_names_the_program():
    with pytest.raises(ExecInTest, match="python"):
        os.execv(sys.executable, ["python", "-c", "pass"])


def test_execve_is_refused():
    with pytest.raises(ExecInTest):
        os.execve(sys.executable, ["python", "-c", "pass"], {})


def test_the_refusal_escapes_an_except_oserror_and_an_except_exception():
    # The two catches a production path could put around an exec. Neither may swallow it.
    with pytest.raises(ExecInTest):
        try:
            os.execv(sys.executable, ["python"])
        except OSError:
            pytest.fail("the guard was caught as an OSError")
    with pytest.raises(ExecInTest):
        try:
            os.execv(sys.executable, ["python"])
        except Exception:  # noqa: BLE001 - the catch under test
            pytest.fail("the guard was caught as an Exception")
    assert not issubclass(ExecInTest, Exception)


def test_a_recorder_patched_on_top_replaces_the_guard_for_its_test(monkeypatch):
    calls = []
    monkeypatch.setattr(os, "execv", lambda path, argv: calls.append((path, argv)))
    os.execv(sys.executable, ["python", "-c", "pass"])
    assert calls == [(sys.executable, ["python", "-c", "pass"])]
    monkeypatch.undo()
    # Undoing the test's own patch puts the guard back, not the real exec.
    with pytest.raises(ExecInTest):
        os.execv(sys.executable, ["python"])
