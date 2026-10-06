"""Checks on ``.github/workflows/infra.yml``, which only runs on GitHub.

The workflow's own checks run only when ``infra/`` changes, and none of them reads the
workflow. A broken condition or a dropped redirect shows up first on ``main``, inside
the approved apply job, so these run in ``ci.yml``'s required ``test`` job instead.

1. Every apply step after the freshness check runs only on a fresh commit. A dropped
   or misspelt condition applies a superseded commit.
2. The apply job needs the ``infra`` environment's approval, and a second run waits
   rather than cancelling one halfway, which would leave its lock behind.
3. Every ``tofu plan`` and ``tofu apply`` sends its output to ``/dev/null``, because the
   repository's logs are public and that output carries values.
4. The validate job runs ``tofu test`` on both configurations.
5. Both triggers watch ``infra/`` and the workflow itself.
6. ``infra/ci/apply-is-stale.sh`` is committed executable, or the freshness step fails.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "infra.yml"
STALE_SCRIPT = "infra/ci/apply-is-stale.sh"
FRESH = "steps.freshness.outputs.state == 'fresh'"


def _workflow() -> dict[Any, Any]:
    with WORKFLOW.open() as f:
        return yaml.safe_load(f)


def _commands(run: str) -> list[str]:
    """The script's lines, with each backslash continuation joined to its line."""
    return [line.strip() for line in run.replace("\\\n", " ").splitlines() if line.strip()]


def _tofu_subcommand(command: str) -> str | None:
    """The subcommand a ``tofu`` call names, skipping options such as ``-chdir``."""
    words = shlex.split(command)
    if "tofu" not in words:
        return None
    return next(word for word in words[words.index("tofu") + 1 :] if not word.startswith("-"))


def _tofu_calls(job: dict[str, Any]) -> list[tuple[str, str]]:
    """Each ``(subcommand, command)`` a job's ``run:`` scripts pass to ``tofu``."""
    calls = []
    for step in job["steps"]:
        for command in _commands(step.get("run", "")):
            subcommand = _tofu_subcommand(command)
            if subcommand is not None:
                calls.append((subcommand, command))
    return calls


def test_every_apply_step_after_the_freshness_check_needs_a_fresh_commit() -> None:
    steps = _workflow()["jobs"]["apply"]["steps"]
    ids = [step.get("id") for step in steps]
    after = steps[ids.index("freshness") + 1 :]
    assert "apply" in [subcommand for subcommand, _ in _tofu_calls({"steps": after})]
    for step in after:
        assert step.get("if") == FRESH, step.get("name", step.get("uses"))


def test_the_apply_job_waits_for_approval_and_never_cancels_a_run() -> None:
    job = _workflow()["jobs"]["apply"]
    assert job["environment"] == "infra"
    assert job["concurrency"]["group"]
    assert job["concurrency"]["cancel-in-progress"] is False


def test_plan_and_apply_send_their_output_to_dev_null() -> None:
    jobs = _workflow()["jobs"]
    found = {}
    for name in ("plan", "apply"):
        calls = [call for call in _tofu_calls(jobs[name]) if call[0] in ("plan", "apply")]
        found[name] = sorted(subcommand for subcommand, _ in calls)
        # A bare `>` or `1>`, since `2> /dev/null` hides only the errors.
        for _, command in calls:
            assert re.search(r"(?:^|\s)1?>\s*/dev/null$", command), command
    assert found == {"plan": ["plan"], "apply": ["apply", "plan"]}


def test_the_validate_job_runs_tofu_test_on_both_configurations() -> None:
    job = _workflow()["jobs"]["validate"]
    assert job["strategy"]["matrix"]["config"] == ["bootstrap", "live"]
    assert "test" in [subcommand for subcommand, _ in _tofu_calls(job)]


def test_both_triggers_watch_infra_and_the_workflow() -> None:
    # YAML 1.1 reads the bare key `on` as true.
    triggers = _workflow()[True]
    for event in ("push", "pull_request"):
        paths = triggers[event]["paths"]
        assert "infra/**" in paths, event
        assert ".github/workflows/infra.yml" in paths, event


def test_the_freshness_script_is_executable() -> None:
    staged = subprocess.run(
        ["git", "ls-files", "-s", STALE_SCRIPT],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert staged.split()[0] == "100755"
    assert os.access(ROOT / STALE_SCRIPT, os.X_OK)
