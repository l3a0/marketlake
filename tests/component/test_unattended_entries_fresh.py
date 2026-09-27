"""Every entry launchd starts gets past its imports in a fresh interpreter.

``tests/unit/test_unattended_entries.py`` runs each entry in process, after the suite has
already imported the ``lake`` modules it depends on. A real ``python -m lake.daemon``
starts from an empty interpreter. Under ``-m`` the entry runs as ``__main__`` rather than
as ``lake.daemon``, so a helper that imports ``lake.daemon`` back loads a second copy of it
partway through the cycle, and that copy fails on a name the helper has not defined yet.
The suite only sees that failure if some test imports the helper before the entry. A
helper extracted from the daemon and reached only through it passes every in-process test
and stops capture: under ``KeepAlive`` the daemon exits 1 on every relaunch.

So each job here runs as a real child, the way launchd runs it: the job's own argv,
environment and working directory. The working directory matters under ``-m``, because
Python puts it first on ``sys.path``. The host is built per test with ``HOME`` under
``tmp_path``, which makes the child's config directory and ``sunday``'s ``--token``
throwaway without editing the job. The job's environment carries no
``MARKETLAKE_CONFIG_DIR``, so the suite's redirect does not reach the child, and the
precondition asserts that ``HOME`` is what the child resolves from instead.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake.config import CONFIG_PATH_ENV
from lake.paths import CONFIG_DIR_ENV, CONFIG_DIR_PARTS, CONFIG_FILE
from tests.support.config_guard import is_protected
from tests.unit.test_unattended_entries import JOBS, stderr_label

# The repository root, two levels up from this file. launchd starts every job there.
REPO_ROOT = Path(__file__).resolve().parents[2]

# A fresh run of any current entry takes about half a second, nearly all of it importing
# ``lake``. The timeout is for an entry that runs past the missing config, and it names
# the job rather than hanging the suite.
TIMEOUT_SECONDS = 30


def fresh_job(label: str, home: Path) -> cp.LaunchdJob:
    """The job with this label, as rendered for a host whose home is ``home``."""
    host = cp.LaunchdHost(
        python=sys.executable,
        owner="someone",
        home=str(home),
        project_dir=str(REPO_ROOT),
        log_dir=str(home / "logs"),
    )
    (job,) = [job for job in cp.all_jobs(host) if job.label == label]
    return job


@pytest.mark.parametrize("label", [job.label for job in JOBS])
def test_the_entry_imports_cleanly_from_a_fresh_interpreter(label, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    job = fresh_job(label, home)
    python, flag, module, *args = job.program_arguments
    assert flag == "-m", f"{label} does not start a module: {job.program_arguments}"

    # The child is stopped only by the missing config, and it has none of the suite's
    # guards. So its config must resolve under the throwaway home, and be absent there.
    env = job.environment
    assert CONFIG_PATH_ENV not in env
    assert CONFIG_DIR_ENV not in env
    assert env["HOME"] == str(home)
    config_path = home.joinpath(*CONFIG_DIR_PARTS) / CONFIG_FILE
    assert not config_path.exists()
    assert not is_protected(config_path)

    finished = subprocess.run(
        job.program_arguments,
        env=env,
        cwd=job.working_directory,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
    )

    # One comparison, so a cycle's failure prints the child's traceback rather than only
    # its exit code. The line matters as well as the code: ``argparse`` exits 2 too.
    label_printed = stderr_label(module, args)
    expected = f"{label_printed}: config file not found: {config_path}\n"
    assert (finished.returncode, finished.stderr) == (2, expected)
