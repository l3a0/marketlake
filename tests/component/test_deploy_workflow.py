"""Checks on ``.github/workflows/deploy.yml``, which only runs on GitHub (#676).

The workflow runs only after a merge, inside an approved job, so a broken condition
shows up first on ``main``. These run in ``ci.yml``'s required ``test`` job instead.

1. The job runs only on a push or a dispatch on ``main``, needs the ``deploy``
   environment's approval, and a second run waits rather than cancelling one halfway.
2. Its permissions are exactly ``contents: read`` and ``id-token: write``, and every
   action is pinned to a 40-digit commit, because any step can mint the job's OIDC token.
3. An empty ``AWS_DEPLOY_ROLE_ARN`` stops the run before anything assumes a role, and
   the check that ``main`` still points at the run's commit gates every later step.
   Running both steps' scripts shows what each does.
4. The credentials step keeps the account id masked and asks for the session the role
   allows.

The rest checks the workflow against the two configurations, since each one's own tests
see only itself. The document name in the workflow's ``env:`` is the one ``infra/live``
declares and both grants in ``infra/bootstrap`` match. The tag in that ``env:`` is the
deploy role's condition and is on the VM. The document's timeout is the host's margin
plus ten minutes, and the job's ``timeout-minutes`` covers delivery, that timeout and ten
minutes for setup and polling.
"""

from __future__ import annotations

import fnmatch
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.component.test_infra_config import _jsonencode_argument, _resources

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
SEND = ROOT / "deploy" / "send-deploy.sh"
FRESH = "steps.head.outputs.state == 'fresh'"

# The margin lake.deploy_window keeps before a refused span, in minutes. The first pull
# request for #676 adds that module and its constant, and is not on main as this is
# written. Once it merges, read the constant from lake.deploy_window instead.
DEPLOY_MARGIN_MINUTES = 210


def _workflow() -> dict[Any, Any]:
    with WORKFLOW.open() as f:
        return yaml.safe_load(f)


def _job() -> dict[str, Any]:
    return _workflow()["jobs"]["deploy"]


def _step(name: str) -> dict[str, Any]:
    steps = [step for step in _job()["steps"] if step.get("name") == name]
    assert len(steps) == 1, f"expected one step named {name!r}, found {len(steps)}"
    return steps[0]


def _uses(prefix: str) -> dict[str, Any]:
    steps = [step for step in _job()["steps"] if step.get("uses", "").startswith(prefix + "@")]
    assert len(steps) == 1, f"expected one {prefix} step, found {len(steps)}"
    return steps[0]


# -- the job ---------------------------------------------------------------------------


def test_the_workflow_runs_on_a_push_to_main_and_on_dispatch() -> None:
    # YAML 1.1 reads the bare key `on` as true.
    triggers = _workflow()[True]
    assert set(triggers) == {"push", "workflow_dispatch"}
    assert triggers["push"] == {"branches": ["main"]}
    assert not triggers["workflow_dispatch"]


def test_the_job_runs_only_on_a_push_or_a_dispatch_on_main() -> None:
    assert set(_workflow()["jobs"]) == {"deploy"}
    condition = " ".join(_job()["if"].split())
    assert condition == (
        "(github.event_name == 'push' || github.event_name == 'workflow_dispatch')"
        " && github.ref == 'refs/heads/main'"
    )


def test_the_job_waits_for_approval_and_never_cancels_a_run() -> None:
    job = _job()
    assert job["environment"] == "deploy"
    assert job["concurrency"] == {"group": "deploy-vm", "cancel-in-progress": False}


def test_the_permissions_are_exactly_read_and_the_oidc_token() -> None:
    assert _workflow()["permissions"] == {"contents": "read"}
    assert _job()["permissions"] == {"contents": "read", "id-token": "write"}


def test_every_action_is_pinned_to_a_commit() -> None:
    uses = [step["uses"] for step in _job()["steps"] if "uses" in step]
    assert uses
    for use in uses:
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", use), use


def test_the_credentials_stay_masked_and_last_the_job() -> None:
    step = _uses("aws-actions/configure-aws-credentials")
    assert step["with"] == {
        "role-to-assume": "${{ secrets.AWS_DEPLOY_ROLE_ARN }}",
        "aws-region": "${{ env.AWS_REGION }}",
        "mask-aws-account-id": True,
        "role-duration-seconds": 18000,
    }
    # The job's timeout must fit inside the session.
    assert _job()["timeout-minutes"] * 60 <= step["with"]["role-duration-seconds"]


def test_the_checkout_keeps_no_token() -> None:
    """The head check reads ``main`` anonymously, so the checkout leaves no token in
    ``.git/config`` for a later step to find."""
    assert _uses("actions/checkout")["with"] == {"persist-credentials": False}


def test_the_region_is_the_one_the_deploy_role_names() -> None:
    region = _workflow()["env"]["AWS_REGION"]
    regions = {
        resource.split(":")[3]
        for statement in _deploy_statements()
        for resource in statement["Resource"]
        if resource != "*"
    }
    assert regions == {region}


def test_no_script_interpolates_an_expression() -> None:
    """An event value reaches a script only through ``env:``."""
    for step in _job()["steps"]:
        assert "${{" not in step.get("run", ""), step.get("name")


def test_the_last_step_runs_send_deploy() -> None:
    last = _job()["steps"][-1]
    assert last["run"].strip() == "deploy/send-deploy.sh"
    assert "env" not in last, "the document and the tag come from the workflow's env: alone"


def test_send_deploy_is_tracked_executable() -> None:
    staged = subprocess.run(
        ["git", "ls-files", "-s", str(SEND.relative_to(ROOT))],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert staged.split()[0] == "100755", staged
    assert os.access(SEND, os.X_OK)


# -- the two guard steps ---------------------------------------------------------------


def _run_step(
    tmp_path: Path, run: str, env: dict[str, str], bin_dir: Path | None = None
) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "step.sh"
    script.write_text(run)
    path = f"{bin_dir}:/usr/bin:/bin" if bin_dir else "/usr/bin:/bin"
    # GitHub runs a `shell: bash` step as `bash --noprofile --norc -eo pipefail {0}`.
    return subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
        env={**env, "PATH": path},
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_the_secret_check_comes_before_anything_that_assumes_a_role() -> None:
    steps = _job()["steps"]
    names = [step.get("name", step.get("uses", "")) for step in steps]
    refusal = names.index("Refuse an empty secret")
    credentials = next(
        i for i, step in enumerate(steps) if "configure-aws-credentials" in step.get("uses", "")
    )
    head = names.index("Check that main still points at this commit")
    assert refusal < head < credentials
    assert "if" not in steps[refusal]


@pytest.mark.parametrize(("value", "rc"), [("", 1), ("arn-stub", 0)], ids=["empty", "set"])
def test_an_empty_role_secret_stops_the_run(tmp_path: Path, value: str, rc: int) -> None:
    step = _step("Refuse an empty secret")
    assert step["env"] == {"AWS_DEPLOY_ROLE_ARN": "${{ secrets.AWS_DEPLOY_ROLE_ARN }}"}
    result = _run_step(tmp_path, step["run"], {"AWS_DEPLOY_ROLE_ARN": value})
    assert result.returncode == rc, result.stdout + result.stderr
    if rc:
        assert "::error::AWS_DEPLOY_ROLE_ARN is empty" in result.stdout


def test_the_head_check_gates_every_later_step() -> None:
    steps = _job()["steps"]
    ids = [step.get("id") for step in steps]
    assert ids.count("head") == 1
    after = steps[ids.index("head") + 1 :]
    assert [step.get("run", "").strip() for step in after][-1] == "deploy/send-deploy.sh"
    assert any("configure-aws-credentials" in step.get("uses", "") for step in after)
    for step in after:
        assert step.get("if") == FRESH, step.get("name", step.get("uses"))
    for step in steps[: ids.index("head")]:
        assert "if" not in step, step.get("name", step.get("uses"))


# git as the head check calls it. LS_REMOTE is what ls-remote prints, and LS_REMOTE_RC
# its exit. The argv goes to a file, so a test reads which ref was asked for.
_FAKE_GIT = """#!/bin/bash
printf '%s\\n' "$@" > "$GIT_ARGV"
if [[ "$1" != ls-remote ]]; then echo "unexpected git $1" >&2; exit 99; fi
printf '%s' "$LS_REMOTE"
exit "${LS_REMOTE_RC:-0}"
"""

_SHA = "0123456789abcdef0123456789abcdef01234567"
_NEWER = "89abcdef0123456789abcdef0123456789abcdef"


@pytest.mark.parametrize(
    ("remote", "remote_rc", "state"),
    [
        (f"{_SHA}\trefs/heads/main\n", 0, "fresh"),
        (f"{_NEWER}\trefs/heads/main\n", 0, "stale"),
        ("", 2, None),
        ("", 128, None),
        ("not-a-sha\trefs/heads/main\n", 0, None),
    ],
    ids=["same-commit", "newer-commit", "no-ref", "unreachable", "garbage"],
)
def test_the_head_check_skips_only_a_superseded_commit(
    tmp_path: Path, remote: str, remote_rc: int, state: str | None
) -> None:
    step = _step("Check that main still points at this commit")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    git = bin_dir / "git"
    git.write_text(_FAKE_GIT)
    git.chmod(0o755)
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    output.write_text("")
    summary.write_text("")

    result = _run_step(
        tmp_path,
        step["run"],
        {
            "GITHUB_SHA": _SHA,
            "GITHUB_OUTPUT": str(output),
            "GITHUB_STEP_SUMMARY": str(summary),
            "GIT_ARGV": str(tmp_path / "argv"),
            "LS_REMOTE": remote,
            "LS_REMOTE_RC": str(remote_rc),
        },
        bin_dir,
    )
    assert (result.returncode == 0) == (state is not None), result.stdout + result.stderr
    assert (tmp_path / "argv").read_text().split() == [
        "ls-remote",
        "--exit-code",
        "origin",
        "refs/heads/main",
    ]
    if state is None:
        # A failed read fails the run rather than skipping it.
        assert "state=" not in output.read_text()
        return
    assert output.read_text() == f"state={state}\n"
    if state == "stale":
        assert summary.read_text().startswith(f"Skipped: main has moved past {_SHA}")
    else:
        assert summary.read_text() == ""


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    ).stdout.strip()


def test_the_head_check_reads_main_and_not_a_branch_whose_name_ends_like_it(
    tmp_path: Path,
) -> None:
    """``git ls-remote origin refs/heads/main`` matches the ref's tail, so it also lists
    branches named ``a/refs/heads/main`` and ``z/refs/heads/main``, which sort before and
    after it. The step must take the line for ``refs/heads/main`` itself, neither the
    first nor the last. Real git, against a bare repository on disk."""
    source = tmp_path / "source"
    _git(tmp_path, "init", "-q", str(source))
    _git(source, "commit", "-q", "--allow-empty", "-m", "older")
    older = _git(source, "rev-parse", "HEAD")
    _git(source, "commit", "-q", "--allow-empty", "-m", "newer")
    newer = _git(source, "rev-parse", "HEAD")
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", str(origin))
    _git(source, "push", "-q", str(origin), f"{newer}:refs/heads/main")
    _git(source, "push", "-q", str(origin), f"{older}:refs/heads/a/refs/heads/main")
    _git(source, "push", "-q", str(origin), f"{older}:refs/heads/z/refs/heads/main")
    work = tmp_path / "work"
    _git(tmp_path, "init", "-q", str(work))
    _git(work, "remote", "add", "origin", str(origin))
    listed = _git(work, "ls-remote", "origin", "refs/heads/main").splitlines()
    assert [line.split()[1] for line in listed] == [
        "refs/heads/a/refs/heads/main",
        "refs/heads/main",
        "refs/heads/z/refs/heads/main",
    ]

    step = _step("Check that main still points at this commit")
    git = shutil.which("git")
    assert git
    git_dir = Path(git).parent
    for sha, state in ((newer, "fresh"), (older, "stale")):
        output = work / "output"
        output.write_text("")
        (work / "summary").write_text("")
        script = work / "step.sh"
        script.write_text(step["run"])
        result = subprocess.run(
            ["/bin/bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
            env={
                "PATH": f"{git_dir}:/usr/bin:/bin",
                "HOME": str(tmp_path),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GITHUB_SHA": sha,
                "GITHUB_OUTPUT": str(output),
                "GITHUB_STEP_SUMMARY": str(work / "summary"),
            },
            cwd=work,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert output.read_text() == f"state={state}\n", sha


# -- the workflow against the configurations -------------------------------------------


def _env() -> dict[str, str]:
    return _workflow()["env"]


def _deploy_statements() -> list[dict[str, Any]]:
    policy = _resources("bootstrap")["aws_iam_role_policy.deploy"]["policy"]
    return _jsonencode_argument(policy)["Statement"]


def _document_patterns(statements: list[dict[str, Any]], action: str) -> list[str]:
    """The document name patterns an Allow grants ``action`` on."""
    patterns = []
    for statement in statements:
        if statement["Effect"] != "Allow" or action not in statement["Action"]:
            continue
        for resource in statement["Resource"]:
            match = re.fullmatch(
                r"arn:aws:ssm:us-east-1:\$\{local\.account_id\}:document/(.+)", resource
            )
            if match:
                patterns.append(match.group(1))
    return patterns


def test_the_document_name_is_the_one_live_declares_and_both_grants_match() -> None:
    name = _env()["DEPLOY_DOCUMENT"]
    assert _resources("live")["aws_ssm_document.deploy"]["name"] == name

    apply = _resources("bootstrap")["aws_iam_role_policy.apply"]["policy"]
    granted = {
        "the deploy role's send": _document_patterns(_deploy_statements(), "ssm:SendCommand"),
        "the apply role's update": _document_patterns(
            _jsonencode_argument(apply)["Statement"], "ssm:UpdateDocument"
        ),
    }
    for grant, patterns in granted.items():
        # The prefix, so a renamed document needs no bootstrap apply, and nothing wider.
        assert patterns == ["marketlake-deploy*"], grant
        assert fnmatch.fnmatchcase(name, patterns[0]), grant


def test_the_tag_is_the_roles_condition_and_is_on_the_vm() -> None:
    env = _env()
    key, value = env["DEPLOY_TAG_KEY"], env["DEPLOY_TAG_VALUE"]
    instance_grants = [
        statement
        for statement in _deploy_statements()
        if any(":instance/" in resource for resource in statement["Resource"])
    ]
    assert len(instance_grants) == 1
    assert instance_grants[0]["Action"] == ["ssm:SendCommand"]
    assert instance_grants[0]["Condition"] == {"StringEquals": {f"ssm:resourceTag/{key}": value}}
    assert _resources("live")["aws_instance.vm"]["tags"][key] == value


def _document() -> dict[str, Any]:
    return _jsonencode_argument(_resources("live")["aws_ssm_document.deploy"]["content"])


def _delivery_seconds() -> int:
    match = re.search(r"^DELIVERY_SECONDS=(\d+)$", SEND.read_text(), re.MULTILINE)
    assert match, "send-deploy.sh names no DELIVERY_SECONDS"
    return int(match.group(1))


def test_the_timeouts_follow_from_the_margin() -> None:
    (step,) = _document()["mainSteps"]
    timeout = step["inputs"]["timeoutSeconds"]
    assert timeout == DEPLOY_MARGIN_MINUTES * 60 + 600
    delivery = _delivery_seconds()
    assert delivery == 600
    job_minutes = _job()["timeout-minutes"]
    assert job_minutes * 60 >= delivery + timeout + 600
    assert job_minutes == 240


def test_the_document_runs_the_step_file() -> None:
    (step,) = _document()["mainSteps"]
    assert step["inputs"]["runCommand"] == ['${file("${path.module}/deploy-step.sh")}']
    assert (ROOT / "infra" / "live" / "deploy-step.sh").is_file()
