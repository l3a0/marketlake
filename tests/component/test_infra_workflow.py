"""Checks on ``.github/workflows/infra.yml``, which only runs on GitHub.

The workflow's own checks run only when ``infra/`` changes, and none of them reads the
workflow. A broken condition or a dropped redirect shows up first on ``main``, inside
an apply job, so these run in ``ci.yml``'s required ``test`` job instead. Two jobs
apply. ``apply-auto`` applies with no click when ``infra/ci/classify.py`` accepts the
plan, and ``apply`` waits for the owner's approval in ``infra`` otherwise.

1. Every apply step after the freshness check runs only on a fresh commit, in both
   jobs. A dropped or misspelt condition applies a superseded commit.
2. ``apply-auto``'s ``Apply`` step also needs the classifier's ``apply`` verdict.
   Without it, every plan applies with no click.
3. The classifier reads the saved plan that ``Apply`` applies, with no plan after it,
   and reads the reviewed change set from the path the lookup writes it to.
4. ``apply`` needs ``apply-auto``, keeps its push-on-``main`` guard, and runs unless
   ``apply-auto`` applied or found the commit stale. A stale run reports ``stale``.
5. ``apply`` needs the ``infra`` environment's approval. ``apply-auto`` runs in
   ``infra-auto``, in a concurrency group of its own, with ``actions: read`` for the
   lookup. In both, a second run waits rather than cancelling one halfway, which would
   leave its lock behind.
6. The pull request's change set goes up under the name the lookup reads.
7. Every ``tofu plan`` and ``tofu apply`` sends its output to ``/dev/null``, because the
   repository's logs are public and that output carries values.
8. The validate job runs ``tofu test`` on both configurations.
9. Both triggers watch ``infra/`` and the workflow itself.
10. ``infra/ci/apply-is-stale.sh`` is committed executable, or the freshness step fails.
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
APPLY_IF_SAFE = f"{FRESH} && steps.classify.outputs.verdict == 'apply'"


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


def _normalised(expression: str) -> str:
    """An expression with its whitespace collapsed, since YAML folding moves it."""
    return " ".join(expression.split())


def _step(job: dict[str, Any], step_id: str) -> dict[str, Any]:
    (step,) = [step for step in job["steps"] if step.get("id") == step_id]
    return step


def _option(command: str, option: str) -> str:
    """The value a command passes to ``option``, as ``-out=x`` or as ``--out x``."""
    words = shlex.split(command)
    for index, word in enumerate(words):
        if word.startswith(option + "="):
            return word.split("=", 1)[1]
        if word == option:
            return words[index + 1]
    raise AssertionError(f"{option} is missing from {command}")


def _classify_call(job: dict[str, Any], mode: str) -> str:
    """The one command that runs ``infra/ci/classify.py`` in ``mode``."""
    (command,) = [
        command
        for step in job["steps"]
        for command in _commands(step.get("run", ""))
        if f"infra/ci/classify.py {mode}" in command
    ]
    return command


def test_every_apply_step_after_the_freshness_check_needs_a_fresh_commit() -> None:
    steps = _workflow()["jobs"]["apply"]["steps"]
    ids = [step.get("id") for step in steps]
    after = steps[ids.index("freshness") + 1 :]
    assert "apply" in [subcommand for subcommand, _ in _tofu_calls({"steps": after})]
    for step in after:
        assert step.get("if") == FRESH, step.get("name", step.get("uses"))


def test_every_no_click_step_after_the_freshness_check_needs_a_fresh_commit() -> None:
    # The twin of the check above. Apply also needs the verdict, and the outcome step
    # runs whatever happened, so neither can be a copy of the freshness condition.
    steps = _workflow()["jobs"]["apply-auto"]["steps"]
    ids = [step.get("id") for step in steps]
    after = steps[ids.index("freshness") + 1 :]
    assert "apply" in [subcommand for subcommand, _ in _tofu_calls({"steps": after})]
    for step in after:
        if step.get("id") == "apply":
            assert _normalised(step["if"]) == APPLY_IF_SAFE
        elif step.get("id") == "outcome":
            assert step["if"] == "always()"
        else:
            assert step.get("if") == FRESH, step.get("name", step.get("uses"))


def test_the_no_click_apply_needs_a_fresh_commit_and_the_apply_verdict() -> None:
    apply = _step(_workflow()["jobs"]["apply-auto"], "apply")
    assert _normalised(apply["if"]) == APPLY_IF_SAFE
    assert [subcommand for subcommand, _ in _tofu_calls({"steps": [apply]})] == ["apply"]


def test_the_classifier_reads_the_plan_that_apply_applies() -> None:
    job = _workflow()["jobs"]["apply-auto"]
    steps = job["steps"]
    classify = [step.get("id") for step in steps].index("classify")
    (plan,) = [
        command for sub, command in _tofu_calls({"steps": steps[:classify]}) if sub == "plan"
    ]
    saved = _option(plan, "-out")
    (show,) = _tofu_calls({"steps": [steps[classify]]})
    assert show[0] == "show"
    assert _option(show[1], "-json") == saved
    assert "infra/ci/classify.py decide" in show[1]
    later = _tofu_calls({"steps": steps[classify + 1 :]})
    assert [subcommand for subcommand, _ in later] == ["apply"]
    words = shlex.split(later[0][1])
    assert words[words.index(">") - 1] == saved
    # The lookup writes the reviewed set where the classifier reads it.
    reviewed = _option(_classify_call(job, "fetch"), "--out")
    assert _option(_classify_call(job, "decide"), "--reviewed") == reviewed


def test_the_approval_job_runs_unless_the_no_click_job_applied_or_was_stale() -> None:
    job = _workflow()["jobs"]["apply"]
    assert job["needs"] == "apply-auto"
    assert _normalised(job["if"]) == _normalised(
        "(github.event_name == 'push' || github.event_name == 'workflow_dispatch')"
        " && github.ref == 'refs/heads/main'"
        " && !cancelled()"
        " && !(needs.apply-auto.result == 'success'"
        " && (needs.apply-auto.outputs.outcome == 'applied'"
        " || needs.apply-auto.outputs.outcome == 'stale'))"
    )


def _outcome(tmp_path: Path, state: str, applied: str, verdict: str) -> str:
    """What the outcome step writes, run with bash as the runner runs it."""
    job = _workflow()["jobs"]["apply-auto"]
    step = _step(job, "outcome")
    assert job["outputs"]["outcome"] == "${{ steps.outcome.outputs.outcome }}"
    assert step["env"] == {
        "STATE": "${{ steps.freshness.outputs.state }}",
        "APPLIED": "${{ steps.apply.outcome }}",
        "VERDICT": "${{ steps.classify.outputs.verdict }}",
    }
    output = tmp_path / f"{state}-{applied}-{verdict}"
    env = {"PATH": os.environ["PATH"], "GITHUB_OUTPUT": str(output)}
    env.update(STATE=state, APPLIED=applied, VERDICT=verdict)
    subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], env=env, check=True)
    return output.read_text()


def test_a_stale_run_reports_stale_so_the_approval_job_skips(tmp_path: Path) -> None:
    assert _outcome(tmp_path, "stale", "skipped", "") == "outcome=stale\n"


def test_the_outcome_names_an_apply_and_a_refusal_and_nothing_else(tmp_path: Path) -> None:
    assert _outcome(tmp_path, "fresh", "success", "apply") == "outcome=applied\n"
    assert _outcome(tmp_path, "fresh", "skipped", "refuse") == "outcome=refused\n"
    # A failed apply, or a classifier that wrote no verdict, leaves the outcome empty.
    assert _outcome(tmp_path, "fresh", "failure", "apply") == "outcome=\n"
    assert _outcome(tmp_path, "fresh", "skipped", "") == "outcome=\n"
    assert _outcome(tmp_path, "", "", "") == "outcome=\n"


def test_the_apply_job_waits_for_approval_and_never_cancels_a_run() -> None:
    job = _workflow()["jobs"]["apply"]
    assert job["environment"] == "infra"
    assert job["concurrency"]["group"]
    assert job["concurrency"]["cancel-in-progress"] is False


def test_the_no_click_job_runs_in_infra_auto_and_never_cancels_a_run() -> None:
    job = _workflow()["jobs"]["apply-auto"]
    assert job["environment"] == "infra-auto"
    assert job["concurrency"]["group"]
    assert job["concurrency"]["cancel-in-progress"] is False


def test_the_no_click_job_has_its_own_group_and_can_read_artifacts() -> None:
    jobs = _workflow()["jobs"]
    assert jobs["apply-auto"]["concurrency"]["group"] != jobs["apply"]["concurrency"]["group"]
    assert jobs["apply-auto"]["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "actions": "read",
        "pull-requests": "read",
    }


def test_the_change_set_goes_up_under_the_name_the_lookup_reads() -> None:
    workflow = _workflow()
    assert workflow["env"]["CHANGE_SET_ARTIFACT"]
    member = workflow["env"]["CHANGE_SET_FILE"]
    assert member and "/" not in member
    plan = workflow["jobs"]["plan"]
    (upload,) = [step for step in plan["steps"] if "upload-artifact" in step.get("uses", "")]
    assert upload["uses"].startswith(
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    )
    settings = upload["with"]
    assert (
        settings["name"]
        == "${{ env.CHANGE_SET_ARTIFACT }}${{ github.event.pull_request.head.sha }}"
    )
    assert settings["path"] == "${{ runner.temp }}/${{ env.CHANGE_SET_FILE }}"
    assert settings["if-no-files-found"] == "error"
    assert settings["overwrite"] is True
    # archive: false makes the action ignore the name.
    assert "archive" not in settings
    assert _option(_classify_call(plan, "change-set"), "--out") == "$RUNNER_TEMP/$CHANGE_SET_FILE"
    fetch = _classify_call(workflow["jobs"]["apply-auto"], "fetch")
    assert _option(fetch, "--name-prefix") == "$CHANGE_SET_ARTIFACT"
    assert _option(fetch, "--member") == "$CHANGE_SET_FILE"


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


def test_the_no_click_plan_and_apply_send_their_output_to_dev_null() -> None:
    calls = [
        call
        for call in _tofu_calls(_workflow()["jobs"]["apply-auto"])
        if call[0] in ("plan", "apply")
    ]
    for _, command in calls:
        assert re.search(r"(?:^|\s)1?>\s*/dev/null$", command), command
    assert sorted(subcommand for subcommand, _ in calls) == ["apply", "plan"]


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
