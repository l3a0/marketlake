"""Every entry started unattended gets past its imports in a fresh interpreter.

``tests/unit/test_unattended_entries.py`` runs each entry in process, after the suite has
already imported the ``lake`` modules it depends on. A real ``python -m lake.daemon``
starts from an empty interpreter. Under ``-m`` the entry runs as ``__main__`` rather than
as ``lake.daemon``, so a helper that imports ``lake.daemon`` back loads a second copy of it
partway through the cycle, and that copy fails on a name the helper has not defined yet.
The suite only sees that failure if some test imports the helper before the entry. A
helper extracted from the daemon and reached only through it passes the full suite, the
in-process entry test included, and stops capture: under ``KeepAlive`` the daemon exits 1
on every relaunch.

So each job here runs as a real child, the way its service manager runs it: the job's own
argv, environment and working directory. Each runs twice, once as launchd starts it and
once as systemd does. systemd hands a unit its own fixed ``PATH`` and, from ``User=``,
``HOME``, ``USER``, ``LOGNAME`` and ``SHELL``, and then the unit's ``Environment=`` on top.
The service manager's own environment reaches the unit too, which holds the host's locale
and whatever ``DefaultEnvironment=`` or ``systemctl set-environment`` added. The child here
starts without it. An operator's shell export reaches a unit through neither source.
The suite pins ``is_macos`` to true, and that pin does not reach a child, so on CI's Linux
runner the child takes the Linux branch. Every entry still exits 2 on the missing config
before any probe. The working directory matters under ``-m``, because
Python puts it first on ``sys.path``. The compaction child and the token pull the daemon
spawns run the same way, with the argv ``daemon.compaction_command`` or
``daemon.token_pull_command`` builds and the daemon job's environment and working
directory, which each inherits because its spawn passes neither. The evening upload the
vendor sweep execs runs with the argv ``sweep.evening_upload_command`` builds and the
eod-sweep job's environment and working directory, which an ``exec`` keeps. The host is
built per test with ``HOME`` under ``tmp_path``, which makes the child's config directory
and ``sunday``'s ``--token`` throwaway without editing the job. The job's environment carries no
``MARKETLAKE_CONFIG_DIR``, so the suite's redirect does not reach the child, and the
precondition asserts that ``HOME`` is what the child resolves from instead.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake import daemon, sweep
from lake.config import CONFIG_PATH_ENV
from lake.paths import CONFIG_DIR_ENV, CONFIG_DIR_PARTS, CONFIG_FILE
from tests.support.config_guard import is_protected
from tests.unit.test_unattended_entries import (
    COMPACTION,
    EVENING_UPLOAD,
    JOBS,
    TOKEN_PULL,
    stderr_label,
)

# The repository root, two levels up from this file. launchd starts every job there.
REPO_ROOT = Path(__file__).resolve().parents[2]

# A fresh run of any current entry takes about half a second, nearly all of it importing
# ``lake``. The timeout is for an entry that runs past the missing config, and it names
# the job rather than hanging the suite.
TIMEOUT_SECONDS = 30


# The owner the units run as. systemd sets ``USER`` and ``LOGNAME`` from ``User=``.
OWNER = "someone"

# The ``PATH`` systemd gives a system service that sets none, on a host where ``/bin`` and
# ``/sbin`` are not merged into ``/usr``. The rendered units leave ``PATH`` to it.
SYSTEMD_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def fresh_job(host_kind: str, label: str, home: Path) -> cp.Job:
    """The job with this label, as rendered for a host whose home is ``home``."""
    host: cp.Host
    if host_kind == "launchd":
        host = cp.LaunchdHost(
            python=sys.executable,
            owner=OWNER,
            home=str(home),
            project_dir=str(REPO_ROOT),
            log_dir=str(home / "logs"),
        )
    else:
        host = cp.SystemdHost(
            python=sys.executable, owner=OWNER, home=str(home), project_dir=str(REPO_ROOT)
        )
    (job,) = [job for job in cp.all_jobs(host) if job.label == label]
    return job


def started_environment(host_kind: str, job: cp.Job, home: Path) -> dict[str, str]:
    """The environment the service manager starts the job with.

    launchd hands a job exactly its plist's block. systemd sets its own five first and
    lays the unit's ``Environment=`` over them. The manager's own environment, which the
    module docstring names, is left out.
    """
    if host_kind == "launchd":
        return dict(job.environment)
    return {
        "PATH": SYSTEMD_PATH,
        "HOME": str(home),
        "USER": OWNER,
        "LOGNAME": OWNER,
        "SHELL": "/bin/bash",
        **job.environment,
    }


def fresh_entry(host_kind: str, label: str, home: Path) -> tuple[list[str], dict[str, str], str]:
    """The argv, environment and working directory the entry with this label starts with.

    The compaction child, the token pull and the evening upload are not jobs of either host.
    The daemon spawns the first two with ``subprocess.Popen`` and no ``env`` or ``cwd``, so
    each starts with the daemon job's. The vendor sweep replaces its own process with the
    third through ``os.execv``, which keeps the eod-sweep job's environment and working
    directory, so that job is its parent.
    """
    children = {
        COMPACTION: (cp.DAEMON_LABEL, lambda: daemon.compaction_command(None)),
        TOKEN_PULL: (cp.DAEMON_LABEL, lambda: daemon.token_pull_command(None, None)),
        EVENING_UPLOAD: (cp.EOD_SWEEP_LABEL, lambda: sweep.evening_upload_command(None)),
    }
    if label in children:
        parent_label, argv = children[label]
        parent = fresh_job(host_kind, parent_label, home)
        env = started_environment(host_kind, parent, home)
        return argv(), env, parent.working_directory
    job = fresh_job(host_kind, label, home)
    env = started_environment(host_kind, job, home)
    return list(job.program_arguments), env, job.working_directory


@pytest.mark.parametrize("host_kind", ["launchd", "systemd"])
@pytest.mark.parametrize(
    "label", [job.label for job in JOBS] + [COMPACTION, TOKEN_PULL, EVENING_UPLOAD]
)
def test_the_entry_imports_cleanly_from_a_fresh_interpreter(host_kind, label, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    argv, env, cwd = fresh_entry(host_kind, label, home)
    python, flag, module, *args = argv
    assert flag == "-m", f"{label} does not start a module: {argv}"

    # The child is stopped only by the missing config, and it has none of the suite's
    # guards. So its config must resolve under the throwaway home, and be absent there.
    assert CONFIG_PATH_ENV not in env
    assert CONFIG_DIR_ENV not in env
    assert env["HOME"] == str(home)
    config_path = home.joinpath(*CONFIG_DIR_PARTS) / CONFIG_FILE
    assert not config_path.exists()
    assert not is_protected(config_path)

    finished = subprocess.run(
        argv,
        env=env,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
    )

    # The child's stderr is the assertion message, so a cycle's failure prints the whole
    # traceback. pytest's own diff of two tuples stops at the first differing item and
    # clips the strings, which would show ``1 != 2`` and hide the ``ImportError``. The
    # line matters as well as the code: ``argparse`` exits 2 too.
    label_printed = stderr_label(module, args)
    expected = f"{label_printed}: config file not found: {config_path}\n"
    assert (finished.returncode, finished.stderr) == (2, expected), finished.stderr
