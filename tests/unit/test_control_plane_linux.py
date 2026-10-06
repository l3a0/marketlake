"""The Linux host's probes, and the helper that picks the host.

A VM never sleeps, so on Linux the pre-open self-check asks systemd whether the daemon is
up and asks ``timedatectl`` whether the clock is synchronized. Both probes shell out, so
these tests patch ``subprocess.run`` and read what each one ran and how it read the answer.
No test here runs the real tools. The CI runner's own systemd would answer for itself, not
for the VM, and the shadow day on the real image is what checks the real answers.

The suite pins every test to macOS through ``control_plane.is_macos``. This module binds
the real helper at import, before any pin, so one test still asks the real question.
"""

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

import pytest

from lake import control_plane as cp
from lake.control_plane import is_macos


def test_the_real_helper_reads_the_platform():
    # Bound at import, so the suite's pin on the module attribute does not reach it. A
    # helper that answered True everywhere would fail here on CI's Linux.
    assert is_macos() == (sys.platform == "darwin")


def test_the_suite_pins_every_test_to_macos():
    assert cp.is_macos() is True


def test_a_test_can_ask_for_linux(on_linux):
    assert cp.is_macos() is False


# -- the systemd daemon probe ---------------------------------------------------------


class _Runs:
    """A ``subprocess.run`` stand-in that records each call and answers one result."""

    def __init__(self, returncode: int, stdout: str | None = None) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self._result = SimpleNamespace(returncode=returncode, stdout=stdout)

    def __call__(self, args, **kwargs):
        self.calls.append((list(args), kwargs))
        return self._result


def test_the_systemd_probe_runs_is_active_quietly_and_captures_nothing(monkeypatch):
    runs = _Runs(0)
    monkeypatch.setattr(subprocess, "run", runs)
    assert cp.systemctl_probe(cp.DAEMON_LABEL) is True
    ((args, kwargs),) = runs.calls
    assert args == ["systemctl", "is-active", "--quiet", "com.marketlake.daemon"]
    # Nothing captured, so a bus failure's stderr reaches the journal beside the summary.
    assert kwargs == {"check": False}


@pytest.mark.parametrize(
    ("returncode", "up"),
    [
        (0, True),  # active or reloading
        (1, False),  # the bus could not be reached
        (3, False),  # a stopped unit
        (4, False),  # a unit with no file
    ],
)
def test_the_systemd_probe_reads_the_exit_code(monkeypatch, returncode, up):
    monkeypatch.setattr(subprocess, "run", _Runs(returncode))
    assert cp.systemctl_probe(cp.DAEMON_LABEL) is up


# -- the clock-sync probe -------------------------------------------------------------


@pytest.mark.parametrize(
    ("stdout", "returncode", "answer"),
    [
        ("yes\n", 0, None),
        ("no\n", 0, "clock not synchronized (NTPSynchronized=no)"),
        ("maybe\n", 0, "clock sync unreadable: timedatectl printed 'maybe\\n', exit 0"),
        ("", 1, "clock sync unreadable: timedatectl printed '', exit 1"),
    ],
)
def test_the_clock_parser_answers_none_or_one_of_two_sentences(stdout, returncode, answer):
    assert cp.parse_ntp_synchronized(stdout, returncode) == answer


def test_a_yes_at_a_failing_exit_is_unreadable_rather_than_synchronized():
    # The exit decides as much as the word. A ``yes`` beside a failure is not an answer.
    assert cp.parse_ntp_synchronized("yes\n", 1) == (
        "clock sync unreadable: timedatectl printed 'yes\\n', exit 1"
    )


def test_the_clock_probe_runs_timedatectl_and_captures_only_stdout(monkeypatch):
    runs = _Runs(0, stdout="no\n")
    monkeypatch.setattr(subprocess, "run", runs)
    assert cp.timedatectl_clock_probe() == cp.CLOCK_NOT_SYNCED
    ((args, kwargs),) = runs.calls
    assert args == ["timedatectl", "show", "--property=NTPSynchronized", "--value"]
    assert kwargs == {"stdout": subprocess.PIPE, "text": True, "check": False}


def test_the_clock_probe_passes_the_exit_to_the_parser(monkeypatch):
    monkeypatch.setattr(subprocess, "run", _Runs(1, stdout=""))
    assert cp.timedatectl_clock_probe() == "clock sync unreadable: timedatectl printed '', exit 1"
