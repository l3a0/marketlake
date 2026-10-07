"""Checks on ``.github/workflows/infra.yml``, which only runs on GitHub.

The workflow's own checks run only when ``infra/`` changes, and none of them reads the
workflow. A broken condition or a dropped redirect shows up first on ``main``, inside
the approved apply job, so these run in ``ci.yml``'s required ``test`` job instead.

1. Every apply step after the freshness check runs only on a fresh commit. A dropped
   or misspelt condition applies a superseded commit.
2. The apply job runs only on a push or a dispatch on ``main``, needs the ``infra``
   environment's approval, and a second run waits rather than cancelling one halfway,
   which would leave its lock behind.
3. Every ``tofu plan`` and ``tofu apply`` sends its output to ``/dev/null``, because the
   repository's logs are public and that output carries values.
4. The validate job runs ``tofu test`` on both configurations.
5. Both triggers watch ``infra/`` and the workflow itself.
6. ``infra/ci/apply-is-stale.sh`` is committed executable, or the freshness step fails.
7. Every ``infra/live`` variable with no default reaches both plan steps, and each
   one's secret or repository variable is in both refusals of an empty input. A missing
   one shows only as a failed plan, or, in the apply job, as a failure after the
   approval rather than a refusal before it.
8. The ``replace_instance`` dispatch input is a boolean that is off by default, reaches
   the apply job's one ``tofu plan`` line through ``env:``, and names only
   ``aws_instance.vm``. A second address would let a dispatch replace the lake volume's
   attachment, which stops the instance and never starts it. Running the step's script
   shows that the plan gets ``-replace`` only when the input is ``true``. A push renders
   the input empty, so a condition that let the empty value through would replace the VM
   on every merge to ``infra/``.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

import hcl2
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "infra.yml"
STALE_SCRIPT = "infra/ci/apply-is-stale.sh"
FRESH = "steps.freshness.outputs.state == 'fresh'"
_HCL_OPTIONS = hcl2.SerializationOptions(
    strip_string_quotes=True, explicit_blocks=False, with_comments=False
)


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


def test_the_apply_job_runs_only_on_a_push_or_a_dispatch_on_main() -> None:
    condition = " ".join(_workflow()["jobs"]["apply"]["if"].split())
    assert condition == (
        "(github.event_name == 'push' || github.event_name == 'workflow_dispatch')"
        " && github.ref == 'refs/heads/main'"
    )


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


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    steps = [step for step in job["steps"] if step.get("name") == name]
    assert len(steps) == 1, f"expected one step named {name!r}, found {len(steps)}"
    return steps[0]


def _required_live_variables() -> set[str]:
    """The ``infra/live`` variables with no default, which a plan cannot run without."""
    names = set()
    for path in sorted((ROOT / "infra" / "live").glob("*.tf")):
        with path.open() as f:
            parsed = hcl2.load(f, serialization_options=_HCL_OPTIONS)
        for block in parsed.get("variable", []):
            for name, body in block.items():
                if "default" not in body:
                    names.add(name)
    return names


def test_every_required_variable_reaches_both_plans_and_both_refusals() -> None:
    required = _required_live_variables()
    assert {"backup_bucket", "owner_ssh_cidr", "ssh_public_key"} <= required
    jobs = _workflow()["jobs"]
    for job_name in ("plan", "apply"):
        job = jobs[job_name]
        plan_env = _step(job, "Plan")["env"]
        passed = {key.removeprefix("TF_VAR_") for key in plan_env if key.startswith("TF_VAR_")}
        assert passed == required, job_name

        refusal = _step(job, "Refuse an empty secret")
        loop = re.search(r"^for name in (.+); do$", refusal["run"], re.MULTILINE)
        assert loop, job_name
        checked = loop.group(1).split()
        for name in sorted(required):
            source = plan_env[f"TF_VAR_{name}"]
            match = re.fullmatch(r"\$\{\{ (?:secrets|vars)\.(\w+) \}\}", source)
            assert match, f"{job_name}: TF_VAR_{name} is {source!r}"
            assert refusal["env"].get(match.group(1)) == source, f"{job_name}: {name}"
            assert match.group(1) in checked, f"{job_name}: {name}"


def test_replace_instance_names_only_the_instance_and_only_on_dispatch() -> None:
    workflow = _workflow()
    # YAML 1.1 reads the bare key `on` as true.
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"replace_instance"}
    assert inputs["replace_instance"]["type"] == "boolean"
    assert inputs["replace_instance"]["default"] is False

    plan = _step(workflow["jobs"]["apply"], "Plan")
    assert plan["env"]["REPLACE_INSTANCE"] == "${{ inputs.replace_instance }}"
    # An event value reaches the script only through env:, never by interpolation.
    assert "${{" not in plan["run"]
    assert re.findall(r"-replace=(\S+?)\)?(?:\s|$)", plan["run"]) == ["aws_instance.vm"]
    plans = [command for subcommand, command in _tofu_calls({"steps": [plan]})]
    assert len(plans) == 1
    assert '"${replace[@]}"' in shlex.split(plans[0], posix=False)

    # No other script in the workflow replaces anything.
    scripts = [step.get("run", "") for job in workflow["jobs"].values() for step in job["steps"]]
    assert sum(script.count("-replace") for script in scripts) == 1


# tofu as the Plan step calls it. It writes one argument per line, since the step sends
# stdout to /dev/null.
_FAKE_TOFU = """#!/bin/bash
printf '%s\\n' "$@" > "$TOFU_ARGV"
"""


@pytest.mark.parametrize(
    ("value", "replaces"),
    [("", False), ("false", False), ("true", True)],
    ids=["push", "dispatch-false", "dispatch-true"],
)
def test_the_apply_plan_replaces_the_instance_only_when_the_input_is_true(
    tmp_path: Path, value: str, replaces: bool
) -> None:
    plan = _step(_workflow()["jobs"]["apply"], "Plan")
    script = tmp_path / "plan.sh"
    script.write_text(plan["run"])
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    tofu = bin_dir / "tofu"
    tofu.write_text(_FAKE_TOFU)
    tofu.chmod(0o755)
    runner_temp = tmp_path / "runner"
    runner_temp.mkdir()
    argv_log = tmp_path / "argv"

    env = {name: f"stub-{name}" for name in plan["env"]}
    env.update(
        REPLACE_INSTANCE=value,
        RUNNER_TEMP=str(runner_temp),
        TOFU_ARGV=str(argv_log),
        PATH=f"{bin_dir}:/usr/bin:/bin",
    )
    # GitHub runs a `shell: bash` step as `bash --noprofile --norc -eo pipefail {0}`.
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr

    argv = argv_log.read_text().splitlines()
    assert _tofu_subcommand(shlex.join(["tofu", *argv])) == "plan"
    assert f"-out={runner_temp}/live.tfplan" in argv
    # An empty array must expand to no word at all, not to an empty argument.
    assert "" not in argv
    assert [arg for arg in argv if arg.startswith("-replace")] == (
        ["-replace=aws_instance.vm"] if replaces else []
    )
