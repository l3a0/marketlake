"""Every entry started unattended reaches ``main`` through its ``__main__`` guard.

The installed plists start ``lake`` modules with ``python -m``, and so does the daemon when
it spawns compaction. A guard that is deleted, or that calls ``main`` with the wrong argv,
fails nothing in the suite outside this file and
``tests/component/test_unattended_entries_fresh.py``, which starts the same entries from a
fresh interpreter. The daemon is the member that costs the most: under ``KeepAlive`` a
daemon whose guard is gone imports, exits 0 and is relaunched every ten seconds, and
capture never starts.

The roster is ``control_plane.all_jobs``, the list ``render_all`` writes one plist per job
from, so a job added there is covered here with no edit. The compaction child joins it
with the argv ``daemon.compaction_command`` builds, the token pull the daemon spawns in
auth death with the argv ``daemon.token_pull_command`` builds, and the evening upload the
vendor sweep execs with the argv ``sweep.evening_upload_command`` builds. Each entry runs
in process under ``runpy`` with the entry's own arguments, and stops at the config load,
because the suite's
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
import subprocess
import sys

import pytest

from lake import config, daemon, sweep
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

# The one entry the product starts for itself rather than through launchd. The daemon
# spawns it, and the installed daemon plist passes no ``--config``, so the live argv is the
# one built from ``None``. Deriving it here rather than typing it is what makes the entry
# case below cover the join: an argv the entry refuses fails there, not only in the argv
# test.
COMPACTION = "compaction"

# The second entry the daemon starts for itself: the token pull it spawns in auth death
# on a ``store`` host (marketlake #702). The installed daemon job passes neither
# ``--config`` nor ``--token``, so the live argv is the one built from two ``None``s.
TOKEN_PULL = "token-pull"

# The third entry the product starts for itself: the upload the 18:30 vendor sweep
# replaces its own process with (marketlake #833). The installed eod-sweep job passes no
# ``--config``, so the live argv is the one built from ``None``. Running it here is what
# covers the join between the flag the sweep spells and the one compaction's parser takes.
EVENING_UPLOAD = "evening-upload"

# Every unattended entry, as a label and the argv it starts with.
ENTRIES = [(job.label, job.program_arguments) for job in JOBS] + [
    (COMPACTION, tuple(daemon.compaction_command(None))),
    (TOKEN_PULL, tuple(daemon.token_pull_command(None, None))),
    (EVENING_UPLOAD, tuple(sweep.evening_upload_command(None))),
]

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


def run_entry(label, argv, monkeypatch) -> None:
    """Run one entry's module as ``__main__`` with its own arguments, under a deadline."""
    python, flag, module, *args = argv
    assert flag == "-m", f"{label} does not start a module: {argv}"

    # The entry is stopped only by the missing config. A config found here would start
    # the daemon's loop, the dashboard's server, or a job's live seams.
    assert config.CONFIG_PATH_ENV not in os.environ
    assert not config.default_config_path().exists()
    assert not is_protected(config.default_config_path())

    def too_slow(signum, frame):
        pytest.fail(f"{label} ran {DEADLINE_SECONDS}s without exiting at the config load")

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
@pytest.mark.parametrize(("label", "argv"), ENTRIES, ids=[label for label, _ in ENTRIES])
def test_the_entry_reaches_main_through_its_guard(label, argv, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exited:
        run_entry(label, argv, monkeypatch)

    assert exited.value.code == 2
    # The whole line, path included. A guard that handed ``main`` a ``--config`` of its
    # own would exit 2 with the right label too, and under launchd it would refuse the
    # real config on every relaunch.
    _, _, module, *args = argv
    label = stderr_label(module, args)
    expected = f"{label}: config file not found: {config.default_config_path()}\n"
    assert capsys.readouterr().err == expected


@pytest.mark.filterwarnings(RUNPY_WARNING)
@pytest.mark.parametrize(("label", "argv"), ENTRIES, ids=[label for label, _ in ENTRIES])
def test_a_crash_in_main_escapes_the_guard(label, argv, monkeypatch):
    # The case above only ever sees ``SystemExit``, so a guard that swallowed every other
    # exception would pass it. Under launchd that turns a crash into a silent exit 0, and
    # the traceback the operator reads in the job's error log never gets written. Every
    # entry calls ``load_config`` first, and the ``runpy`` copy looks it up afresh, so a
    # failure planted there is a failure inside ``main``.
    def crash(*args, **kwargs):
        raise EntryCrashed(label)

    monkeypatch.setattr(config, "load_config", crash)
    with pytest.raises(EntryCrashed):
        run_entry(label, argv, monkeypatch)


@pytest.mark.filterwarnings(RUNPY_WARNING)
def test_the_compaction_entry_reads_the_config_the_daemon_forwards(tmp_path, monkeypatch, capsys):
    # The other branch of ``compaction_command``: a daemon given ``--config`` forwards it.
    # The bare argv carries nothing past the module, so a guard that dropped ``sys.argv``,
    # or a ``main`` that read ``None`` as no arguments, passes every case above. Here that
    # guard sends the child to the default config instead of the one it was handed, and
    # a hand-run ``recompact`` would run the whole nightly job in place of the repair.
    given = tmp_path / "given.yaml"
    with pytest.raises(SystemExit) as exited:
        run_entry(COMPACTION, daemon.compaction_command(given), monkeypatch)

    assert exited.value.code == 2
    assert capsys.readouterr().err == f"compact: config file not found: {given}\n"


def test_the_daemon_spawns_compaction_with_the_bare_argv():
    # The argv production runs, because the installed daemon plist passes no ``--config``.
    # The daemon rig always hands the loop a config path, so no daemon test builds this
    # form. ``sys.executable`` is asserted too: the child must run the daemon's own
    # interpreter, and the in-process entry cases above never read the first item.
    assert daemon.compaction_command(None) == [sys.executable, "-m", "lake.compact"]


def test_the_compaction_child_inherits_the_daemons_environment(monkeypatch):
    # The fresh-interpreter case starts the child with the daemon job's environment and
    # working directory, because ``_spawn_compaction`` passes neither. An ``env`` built
    # here would make that stand-in false, and one that left out ``HOME`` would send the
    # child's config lookup somewhere else. The fake has no ``wait``, so a spawn that
    # waited on the child, and stalled the daemon's loop for the whole job, fails too.
    # Every keyword is refused, not only those two. ``stdout=subprocess.DEVNULL`` would
    # drop the child's ``compaction:`` line from the daemon's log, which is the only
    # record of its runs, so a new keyword is a change to review rather than to wave by.
    calls = []

    class FakePopen:
        def __init__(self, *args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.setattr(subprocess, "Popen", FakePopen)
    argv = daemon.compaction_command(None)
    child = daemon._spawn_compaction(argv)

    assert calls == [((argv,), {})]
    assert isinstance(child, FakePopen)


@pytest.mark.filterwarnings(RUNPY_WARNING)
def test_the_pull_entry_reads_the_config_the_daemon_forwards(tmp_path, monkeypatch, capsys):
    # The forwarding branch of ``token_pull_command``. Both flags belong to the ``pull``
    # subcommand, so an argv that put them before it would exit 2 on argparse's usage line
    # rather than on this config, and one that dropped ``--config`` would read the default.
    given = tmp_path / "given.yaml"
    argv = daemon.token_pull_command(given, tmp_path / "token.json")
    with pytest.raises(SystemExit) as exited:
        run_entry(TOKEN_PULL, argv, monkeypatch)

    assert exited.value.code == 2
    assert capsys.readouterr().err == f"token_store: config file not found: {given}\n"


def test_the_daemon_spawns_the_pull_with_the_bare_argv_or_the_flags_it_was_given():
    # The bare form is what production runs. The forwarded form puts both flags after
    # ``pull``, in the order ``token_store``'s parser accepts, and each only when given.
    assert daemon.token_pull_command(None, None) == [
        sys.executable,
        "-m",
        "lake.token_store",
        "pull",
    ]
    assert daemon.token_pull_command("/c.yaml", None) == [
        sys.executable,
        "-m",
        "lake.token_store",
        "pull",
        "--config",
        "/c.yaml",
    ]
    assert daemon.token_pull_command(None, "/t.json") == [
        sys.executable,
        "-m",
        "lake.token_store",
        "pull",
        "--token",
        "/t.json",
    ]


def test_the_pull_child_inherits_the_daemons_environment(monkeypatch):
    # The copy of the compaction case for the pull's spawn, for the same three reasons. An
    # ``env`` built here would make the fresh-interpreter case's stand-in false, and one that
    # left out ``HOME`` would send the child to another config and another ``token.json``.
    # The fake has no ``wait``, so a spawn that waited on the child, and held the daemon's
    # loop thread for up to two minutes of SSM retries, fails too. Every keyword is refused,
    # because ``stdout=subprocess.DEVNULL`` would drop the child's ``token_store:`` line from
    # the daemon's log, which is the only record of what the pull did.
    calls = []

    class FakePopen:
        def __init__(self, *args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.setattr(subprocess, "Popen", FakePopen)
    argv = daemon.token_pull_command(None, None)
    child = daemon._spawn_token_pull(argv)

    assert calls == [((argv,), {})]
    assert isinstance(child, FakePopen)
