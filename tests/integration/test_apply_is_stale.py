"""``infra/ci/apply-is-stale.sh`` against a real git remote, in three cases.

The script runs only after a merge, inside the approved apply job, so its first run
would otherwise be on ``main``. Each case builds a remote holding ``main``, a clone
checked out at an older commit the way ``actions/checkout`` leaves one, and runs the
script with ``GITHUB_SHA`` naming that older commit.

1. A newer commit on ``main`` changes ``infra/``: the run is stale.
2. A newer commit on ``main`` changes only docs: the run is fresh, so a docs-only merge
   never skips a pending apply.
3. The fetch fails: the script exits non-zero and prints nothing, so the job fails red
   rather than skipping green.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "infra" / "ci" / "apply-is-stale.sh"


def _env() -> dict[str, str]:
    """An environment that keeps the machine's own git config out of the test."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_") and key != "GITHUB_SHA"
    }
    env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_AUTHOR_NAME="test",
        GIT_AUTHOR_EMAIL="test@example.invalid",
        GIT_COMMITTER_NAME="test",
        GIT_COMMITTER_EMAIL="test@example.invalid",
    )
    return env


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=cwd, env=_env(), check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _commit(repo: Path, path: str, text: str) -> str:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    _git(repo, "add", path)
    _git(repo, "commit", "--quiet", "-m", f"change {path}")
    return _git(repo, "rev-parse", "HEAD")


def _setup(tmp_path: Path, newer_change: str) -> tuple[Path, str]:
    """A clone at an older commit, whose remote's main carries ``newer_change``."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "--quiet", "--initial-branch=main")
    older = _commit(origin, "infra/live/main.tf", "# first\n")
    _commit(origin, newer_change, "# newer\n")

    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "--quiet", "--no-local", "--depth=1", origin.as_uri(), str(clone))
    _git(clone, "fetch", "--quiet", "--depth=1", "origin", older)
    _git(clone, "checkout", "--quiet", "--detach", older)
    # actions/checkout leaves the triggering commit in origin/main, as this does.
    _git(clone, "update-ref", "refs/remotes/origin/main", older)
    return clone, older


def _run(clone: Path, sha: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=clone,
        env={**_env(), "GITHUB_SHA": sha},
        capture_output=True,
        text=True,
    )


def test_a_newer_infra_change_makes_the_run_stale(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, "infra/live/iam.tf")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "stale\n")


def test_a_newer_workflow_change_makes_the_run_stale(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, ".github/workflows/infra.yml")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "stale\n")


def test_a_newer_docs_only_change_leaves_the_run_fresh(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, "docs/design.md")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "fresh\n")


def test_a_failed_fetch_fails_and_prints_nothing(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, "infra/live/iam.tf")
    _git(clone, "remote", "set-url", "origin", (tmp_path / "missing").as_uri())
    result = _run(clone, older)
    assert result.returncode != 0
    assert result.stdout == ""


def test_a_commit_missing_from_the_clone_fails_and_prints_nothing(tmp_path: Path) -> None:
    clone, _ = _setup(tmp_path, "docs/design.md")
    result = _run(clone, "0" * 40)
    assert result.returncode != 0
    assert result.stdout == ""
