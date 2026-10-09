"""The exec guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a test that reaches the vendor sweep's
hand-off without patching it fails, rather than replacing the pytest process with
``python -m lake.compact``. It is autouse, so every test depends on it and no test asserts
it. These do.

Four properties carry the guard.

1. ``os.execv`` and ``os.execve`` are both refused, and the refusal names the program.
2. The refusal is neither an ``OSError`` nor an ``Exception``. ``sweep.main`` catches
   ``OSError`` around its ``exec`` and returns 1, so a guard of that kind would turn into an
   exit code that four of the tests reaching the hand-off do not assert.
3. A recorder a test patches onto ``os.execv`` replaces the guard for that test only.
4. ``lake.sweep`` reaches its ``exec`` through the ``os`` module. The guard patches the
   attribute on ``os``, so a ``from os import execv`` in ``lake.sweep`` would bind the real
   function when the module is imported, and the patch would never reach it.
"""

from __future__ import annotations

import os
import posix
import sys

import pytest

import lake.sweep
from tests.conftest import ExecInTest

# The real exec functions, read when this file is collected and so before any test's guard.
# ``posix`` is never patched, so its two are the real ones however this file is reached.
_REAL_EXECS = (os.execv, os.execve, posix.execv, posix.execve)

# Every exec below targets this. With the guard gone, the real exec replaces pytest with it,
# so the run ends with code 97. A target that exits 0, or an interactive interpreter, would
# instead end the run green with every later test unrun.
LOUD = [sys.executable, "-c", "import os; os._exit(97)"]


def test_execv_is_refused_and_names_the_program():
    with pytest.raises(ExecInTest, match="python"):
        os.execv(sys.executable, LOUD)


def test_execve_is_refused():
    with pytest.raises(ExecInTest):
        os.execve(sys.executable, LOUD, {})


def test_the_refusal_escapes_an_except_oserror_and_an_except_exception():
    # The two catches a production path could put around an exec. Neither may swallow it.
    with pytest.raises(ExecInTest):
        try:
            os.execv(sys.executable, LOUD)
        except OSError:
            pytest.fail("the guard was caught as an OSError")
    with pytest.raises(ExecInTest):
        try:
            os.execv(sys.executable, LOUD)
        except Exception:  # noqa: BLE001 - the catch under test
            pytest.fail("the guard was caught as an Exception")
    assert not issubclass(ExecInTest, Exception)


def test_a_recorder_patched_on_top_replaces_the_guard_for_its_test(monkeypatch):
    calls = []
    monkeypatch.setattr(os, "execv", lambda path, argv: calls.append((path, argv)))
    os.execv(sys.executable, LOUD)
    assert calls == [(sys.executable, LOUD)]
    monkeypatch.undo()
    # Undoing the test's own patch puts the guard back, not the real exec.
    with pytest.raises(ExecInTest):
        os.execv(sys.executable, LOUD)


def test_lake_sweep_binds_no_exec_function_by_name():
    # Every name in the module is checked, since an alias such as ``from os import execv as
    # run`` hides from a search for the word.
    bound = [
        name
        for name, value in vars(lake.sweep).items()
        if any(value is real for real in _REAL_EXECS)
    ]
    assert bound == []
    assert lake.sweep.os is os
