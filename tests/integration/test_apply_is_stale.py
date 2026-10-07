"""``infra/ci/apply-is-stale.sh`` against a real git remote.

The script runs only after a merge, inside the approved apply job, so its first run
would otherwise be on ``main``. Each case builds a remote holding ``main``, a clone
checked out at an older commit the way ``actions/checkout`` leaves one, and runs the
script with ``GITHUB_SHA`` naming that older commit.

A stale run skips because the newer commit started its own run, which applies the
change. So the script reads stale for exactly the changes that start a run.

1. A newer commit on ``main`` changes a file under ``infra/`` that is not Markdown, or
   changes the workflow: the run is stale. A commit that changes Markdown beside a
   ``.tf`` file is stale too, since it starts a run. So is a commit that only deletes a
   ``.tf`` file, since applying the older commit last would recreate what it deleted.
2. A newer commit on ``main`` changes only files outside ``infra/``, or only Markdown
   under ``infra/``, at the top or nested: the run is fresh. Such a merge starts no run,
   so skipping would leave the pending apply's change unapplied.
3. The fetch fails, or the older commit is missing from the clone: the script exits
   non-zero and prints nothing, so the job fails red rather than skipping green.
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


def _commit(repo: Path, paths: tuple[str, ...], text: str) -> str:
    for path in paths:
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    _git(repo, "add", *paths)
    _git(repo, "commit", "--quiet", "-m", f"change {' '.join(paths)}")
    return _git(repo, "rev-parse", "HEAD")


def _origin(tmp_path: Path) -> tuple[Path, str]:
    """A remote whose main holds one older commit, and that commit's sha."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "--quiet", "--initial-branch=main")
    return origin, _commit(origin, ("infra/live/main.tf",), "# first\n")


def _setup(tmp_path: Path, *newer_changes: str) -> tuple[Path, str]:
    """A clone at an older commit, whose remote's main carries ``newer_changes``."""
    origin, older = _origin(tmp_path)
    _commit(origin, newer_changes, "# newer\n")
    return _clone_at(tmp_path, origin, older), older


def _clone_at(tmp_path: Path, origin: Path, older: str) -> Path:
    """A clone of ``origin`` checked out at ``older``."""
    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "--quiet", "--no-local", "--depth=1", origin.as_uri(), str(clone))
    _git(clone, "fetch", "--quiet", "--depth=1", "origin", older)
    _git(clone, "checkout", "--quiet", "--detach", older)
    # actions/checkout leaves the triggering commit in origin/main, as this does.
    _git(clone, "update-ref", "refs/remotes/origin/main", older)
    return clone


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


def test_a_newer_deletion_under_infra_makes_the_run_stale(tmp_path: Path) -> None:
    origin, older = _origin(tmp_path)
    _git(origin, "rm", "--quiet", "infra/live/main.tf")
    _git(origin, "commit", "--quiet", "-m", "remove infra/live/main.tf")
    result = _run(_clone_at(tmp_path, origin, older), older)
    assert (result.returncode, result.stdout) == (0, "stale\n")


def test_a_newer_workflow_change_makes_the_run_stale(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, ".github/workflows/infra.yml")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "stale\n")


def test_a_newer_change_outside_infra_leaves_the_run_fresh(tmp_path: Path) -> None:
    clone, older = _setup(tmp_path, "docs/design.md")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "fresh\n")


def test_a_newer_change_to_only_the_infra_readme_leaves_the_run_fresh(
    tmp_path: Path,
) -> None:
    # The top level is the case a pathspec without git's glob magic gets wrong, since
    # there `**/` must match no directory at all.
    clone, older = _setup(tmp_path, "infra/README.md")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "fresh\n")


def test_a_newer_change_to_only_nested_infra_markdown_leaves_the_run_fresh(
    tmp_path: Path,
) -> None:
    clone, older = _setup(tmp_path, "infra/live/notes.md")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "fresh\n")


def test_a_newer_change_to_markdown_and_a_tf_file_makes_the_run_stale(
    tmp_path: Path,
) -> None:
    clone, older = _setup(tmp_path, "infra/README.md", "infra/live/iam.tf")
    result = _run(clone, older)
    assert (result.returncode, result.stdout) == (0, "stale\n")


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
