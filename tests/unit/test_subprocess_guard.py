"""The subprocess guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a forgotten seam runs a fake tool
instead of the real ``rsync``, ``launchctl``, ``pmset``, or ``tmutil``. It is autouse,
so every test depends on it and no test asserts it. These do.

Five properties carry the whole guard, and each is covered below.

1. Each of the four guarded programs is refused, whether it is named through
   ``subprocess.run`` or ``subprocess.Popen``.
2. The failure is not an ``Exception``. The Sunday self-check catches bare
   ``Exception`` around both read-backs on purpose, so a guard derived from it would
   be swallowed and the test would pass.
3. Every other program still runs for real, which is what lets the four render tests
   in ``tests/component/test_control_plane_render.py`` spawn a rendered script.
4. The six production seams that forget to fake a guarded program are themselves
   caught, not just a synthetic call naming the program directly.
5. A guarded program named behind a prefix wrapper is refused too. ``sudo`` is the one
   this repo uses, and the sixth seam is the only guarded call that writes rather than
   reads, so the gap sat under exactly the call with the largest blast radius.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date

import pytest

from lake.control_plane import (
    launchctl_probe,
    pmset_assertions_probe,
    read_exclusions,
    read_pmset_schedule,
)
from lake.runner import RsyncBackup
from lake.sweep import set_sunday_wake
from tests.conftest import SubprocessAccessInTest, _program_of

GUARDED = ("rsync", "launchctl", "pmset", "tmutil")


@pytest.mark.parametrize("program", GUARDED)
def test_run_is_refused_for_each_guarded_program(program):
    with pytest.raises(SubprocessAccessInTest):
        subprocess.run([program, "--version"], capture_output=True)


@pytest.mark.parametrize("program", GUARDED)
def test_popen_is_refused_for_each_guarded_program(program):
    with pytest.raises(SubprocessAccessInTest):
        subprocess.Popen([program, "--version"])


def test_a_bytes_argv_is_still_refused():
    # subprocess accepts a bytes argv. str(b"rsync") renders "b'rsync'", which never
    # matches a guarded name, so the guard decodes bytes before comparing.
    with pytest.raises(SubprocessAccessInTest):
        subprocess.run([b"rsync", b"--version"], capture_output=True)


def test_an_unguarded_program_still_runs_for_real():
    # None of the four render tests name rsync, launchctl, pmset, or tmutil directly,
    # only a rendered script or bash. Confirms the guard leaves everything else
    # untouched, which is what those tests depend on.
    proc = subprocess.run([sys.executable, "-c", "print('hi')"], capture_output=True, text=True)
    assert proc.returncode == 0
    assert proc.stdout.strip() == "hi"


def test_the_refusal_is_not_caught_by_a_bare_except_exception():
    # The Sunday self-check wraps `schedule_reader()` in a bare `except Exception`, so a
    # read-back it cannot parse turns into a report line rather than a raise. A guard
    # deriving from `Exception` would land in that same catch, and the forgotten fake
    # would ship green instead of failing the test.
    assert not issubclass(SubprocessAccessInTest, Exception)

    caught = False
    try:
        try:
            subprocess.run(["pmset", "-g", "sched"], capture_output=True)
        except Exception:  # noqa: BLE001 - reproducing production's swallow on purpose
            caught = True
    except SubprocessAccessInTest:
        pass
    assert not caught, "the guard was swallowed by a bare except Exception"


def test_a_test_calling_monkeypatch_undo_does_not_disarm_the_guard(monkeypatch):
    # The guard used to have no counterpart here at all; this mirrors the network
    # guard's own regression, so the same undo-stack mistake cannot recur unnoticed.
    monkeypatch.setattr(subprocess, "DEVNULL", -999)
    monkeypatch.undo()
    assert subprocess.DEVNULL != -999
    with pytest.raises(SubprocessAccessInTest):
        subprocess.run(["tmutil", "isexcluded", "/lake"], capture_output=True)


# -- the four production seams, unfaked -----------------------------------------------


def test_a_forgotten_launchctl_probe_fake_is_caught():
    with pytest.raises(SubprocessAccessInTest):
        launchctl_probe("com.marketlake.capture")


def test_a_forgotten_pmset_schedule_fake_is_caught():
    with pytest.raises(SubprocessAccessInTest):
        read_pmset_schedule()


def test_a_forgotten_pmset_assertions_fake_is_caught():
    """The fifth seam, and the reason this set is a set rather than four separate tests.

    The pre-open self-check reads ``pmset -g assertions`` to confirm the machine is being
    held awake. A test that forgets the fake would ask the machine running the suite,
    whose answer has nothing to do with the case under test and changes depending on
    whether someone is at the keyboard.
    """
    with pytest.raises(SubprocessAccessInTest):
        pmset_assertions_probe(4242)


def test_a_forgotten_exclusion_reader_fake_is_caught():
    with pytest.raises(SubprocessAccessInTest):
        read_exclusions(["/lake"])


def test_a_forgotten_rsync_backup_fake_is_caught(tmp_path):
    # RsyncBackup.sync only stats the target, never the source, so only target needs
    # to exist on disk.
    source = tmp_path / "lake"
    target = tmp_path / "backup"
    target.mkdir()
    with pytest.raises(SubprocessAccessInTest):
        RsyncBackup().sync(source, target)


# -- a guarded program behind a prefix wrapper -----------------------------------------


def test_a_forgotten_schedule_setter_fake_is_caught():
    """The sixth seam, and the only guarded call in the repo that writes.

    ``set_sunday_wake`` runs ``sudo -n /usr/bin/pmset schedule wakeorpoweron ...``, so the
    program at ``argv[0]`` is ``sudo`` and ``pmset`` sits three elements further along.
    A guard reading the head alone answered ``sudo``, which is guarded nowhere, and let
    this one through. Every other seam reads; a forgotten fake here would have
    re-scheduled the developer's own machine to wake on a Sunday.
    """
    with pytest.raises(SubprocessAccessInTest, match="pmset"):
        set_sunday_wake(date(2026, 9, 20))


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["sudo", "-n", "/usr/bin/pmset", "schedule"], "pmset"),
        (["sudo", "pmset", "-g", "sched"], "pmset"),
        (["env", "sudo", "pmset", "-g"], "pmset"),
        # A wrapper with nothing after it runs nothing guarded, so it answers itself.
        (["sudo", "-l"], "sudo"),
        (["sudo"], "sudo"),
        # An ordinary call is unaffected, and so is a program whose name merely starts
        # with a dash-free wrapper spelling.
        (["pmset", "-g", "sched"], "pmset"),
        (["/bin/bash", "./install.sh"], "bash"),
        ([b"/usr/bin/pmset", b"-g"], "pmset"),
    ],
)
def test_the_wrapper_walk_answers_the_program_that_actually_runs(argv, expected):
    """Read off ``_program_of`` directly, because the refusal only sees its answer.

    A wrapper's own options are stepped over, since an option is never the program it
    runs. ``sudo -l`` lists rules and runs nothing, so answering the wrapper is right.
    """
    assert _program_of(argv) == expected


def test_an_unguarded_program_behind_a_wrapper_still_runs_for_real():
    """The walk must not turn every wrapped call into a refusal.

    The render tests spawn a rendered script through ``bash``, and a future one wrapped
    in ``env`` has to keep working. Only the guarded names are refused.
    """
    result = subprocess.run(
        ["env", sys.executable, "-c", "print('wrapped')"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "wrapped"
