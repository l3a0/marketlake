"""The subprocess guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a forgotten seam runs a fake tool
instead of the real ``rsync``, ``launchctl``, ``pmset``, or ``tmutil``. It is autouse,
so every test depends on it and no test asserts it. These do.

Four properties carry the whole guard, and each is covered below.

1. Each of the four guarded programs is refused, whether it is named through
   ``subprocess.run`` or ``subprocess.Popen``.
2. The failure is not an ``Exception``. The Sunday self-check catches bare
   ``Exception`` around both read-backs on purpose, so a guard derived from it would
   be swallowed and the test would pass.
3. Every other program still runs for real, which is what lets the four render tests
   in ``tests/component/test_control_plane_render.py`` spawn a rendered script.
4. The four production seams that forget to fake a guarded program are themselves
   caught, not just a synthetic call naming the program directly.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from lake.control_plane import launchctl_probe, read_exclusions, read_pmset_schedule
from lake.runner import RsyncBackup
from tests.conftest import SubprocessAccessInTest

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
