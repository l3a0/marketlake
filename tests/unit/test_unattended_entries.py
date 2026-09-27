"""Every entry launchd starts reaches ``main`` through its ``__main__`` guard.

The installed plists start ``lake`` modules with ``python -m``, and a guard that is
deleted, or that calls ``main`` with the wrong argv, fails nothing in the suite outside
this file and ``tests/component/test_unattended_entries_fresh.py``, which starts the same
jobs from a fresh interpreter. The daemon is the member that costs the most: under
``KeepAlive`` a daemon whose guard is gone imports, exits 0 and is relaunched every ten
seconds, and capture never starts.

The roster is ``control_plane.all_jobs``, the list ``render_all`` writes one plist per job
from, so a job added there is covered here with no edit. Each entry runs in process under
``runpy`` with the job's own arguments, and stops at the config load, because the suite's
config directory is an empty throwaway. That exit is the first stop between the entry and
live work. The conftest's network and subprocess guards catch some of what lies past it,
but not a server bound to a local port or a loop that never calls out. So the precondition
is asserted first, and a deadline turns an entry that runs past it into a failure rather
than a hung suite.
"""

from __future__ import annotations

import os
import runpy
import signal
import sys

import pytest

from lake import config
from lake import control_plane as cp
from tests.support.config_guard import is_protected

HOST = cp.LaunchdHost(
    python="/opt/py/bin/python",
    owner="someone",
    home="/Users/someone",
    project_dir="/Users/someone/marketlake",
    log_dir="/Users/someone/Library/Logs/marketlake",
)

JOBS = cp.all_jobs(HOST)

# Every current entry exits at the config load in well under a second. The deadline is
# for one that does not, such as the dashboard given ``--lake-root``, which skips the
# config and serves forever. The project has no timeout plugin. CI's job-level
# ``timeout-minutes`` would end the run after fifteen minutes without naming the test,
# and a local run has nothing to stop it at all.
DEADLINE_SECONDS = 30


def stderr_label(module: str, args: list[str]) -> str:
    """The label an entry prints ahead of a refusal.

    The same rule ``test_a_missing_config_names_itself_at_every_cli_entry`` applies. The
    control plane names its subcommand, since one module carries several, and
    ``probe_calendar`` spells its label with a hyphen.
    """
    if module == "lake.control_plane":
        return args[0]
    if module == "lake.probe_calendar":
        return "probe-calendar"
    return module.removeprefix("lake.")


class EntryCrashed(Exception):
    """A failure planted inside ``main``, standing in for any bug past the config load."""


def run_entry(job, monkeypatch) -> None:
    """Run one job's module as ``__main__`` with its own arguments, under a deadline."""
    python, flag, module, *args = job.program_arguments
    assert flag == "-m", f"{job.label} does not start a module: {job.program_arguments}"

    # The entry is stopped only by the missing config. A config found here would start
    # the daemon's loop, the dashboard's server, or a job's live seams.
    assert config.CONFIG_PATH_ENV not in os.environ
    assert not config.DEFAULT_CONFIG_PATH.exists()
    assert not is_protected(config.DEFAULT_CONFIG_PATH)

    def too_slow(signum, frame):
        pytest.fail(f"{job.label} ran {DEADLINE_SECONDS}s without exiting at the config load")

    monkeypatch.setattr(sys, "argv", [python, *args])
    previous = signal.signal(signal.SIGALRM, too_slow)
    signal.alarm(DEADLINE_SECONDS)
    try:
        runpy.run_module(module, run_name="__main__")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


# ``runpy`` warns that the module was already imported, which it always is here. Only
# that warning is silenced, so one the entry raises itself still shows.
RUNPY_WARNING = "ignore:.*found in sys.modules after import:RuntimeWarning"


@pytest.mark.filterwarnings(RUNPY_WARNING)
@pytest.mark.parametrize("job", JOBS, ids=[job.label for job in JOBS])
def test_the_entry_reaches_main_through_its_guard(job, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exited:
        run_entry(job, monkeypatch)

    assert exited.value.code == 2
    # The whole line, path included. A guard that handed ``main`` a ``--config`` of its
    # own would exit 2 with the right label too, and under launchd it would refuse the
    # real config on every relaunch.
    _, _, module, *args = job.program_arguments
    label = stderr_label(module, args)
    expected = f"{label}: config file not found: {config.DEFAULT_CONFIG_PATH}\n"
    assert capsys.readouterr().err == expected


@pytest.mark.filterwarnings(RUNPY_WARNING)
@pytest.mark.parametrize("job", JOBS, ids=[job.label for job in JOBS])
def test_a_crash_in_main_escapes_the_guard(job, monkeypatch):
    # The case above only ever sees ``SystemExit``, so a guard that swallowed every other
    # exception would pass it. Under launchd that turns a crash into a silent exit 0, and
    # the traceback the operator reads in the job's error log never gets written. Every
    # entry calls ``load_config`` first, and the ``runpy`` copy looks it up afresh, so a
    # failure planted there is a failure inside ``main``.
    def crash(*args, **kwargs):
        raise EntryCrashed(job.label)

    monkeypatch.setattr(config, "load_config", crash)
    with pytest.raises(EntryCrashed):
        run_entry(job, monkeypatch)
