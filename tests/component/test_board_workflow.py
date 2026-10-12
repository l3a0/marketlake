"""Checks on ``.github/workflows/board.yml``, which publishes the build board (#872).

No trigger the workflow carries, ``workflow_dispatch`` included, runs a branch's copy
of it before it is on ``main``, and its first run is the one the merge starts. So its
rules are checked here, in ``ci.yml``'s required ``test`` job, before then.

The workflow runs on events anyone can cause on a public repository, with a write token
on ``pull_request_target``. Two rules keep strangers out: no step reads the event
payload, and the checkout takes the default ref with no credentials kept. The deploy
job can mint an OIDC token, so every action is pinned to a commit, as
``tests/component/test_deploy_workflow.py`` requires of ``deploy.yml``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "board.yml"
JOBS = {"board-build", "board-deploy"}


def _workflow(path: Path = WORKFLOW) -> dict[Any, Any]:
    with path.open() as f:
        return yaml.safe_load(f)


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _steps() -> list[dict[str, Any]]:
    return [step for job in _jobs().values() for step in job["steps"]]


def test_the_triggers_are_the_siblings_with_infra_watched_too() -> None:
    # YAML 1.1 reads the bare key `on` as true.
    triggers = _workflow()[True]
    assert set(triggers) == {
        "issues",
        "issue_comment",
        "pull_request_target",
        "workflow_run",
        "schedule",
        "workflow_dispatch",
    }
    assert triggers["issues"] is None
    assert triggers["issue_comment"] is None
    assert triggers["workflow_dispatch"] is None
    assert triggers["pull_request_target"] == {
        "branches": ["main"],
        "types": ["opened", "synchronize", "reopened", "closed", "edited"],
    }
    assert triggers["workflow_run"] == {"workflows": ["CI", "Infra"], "types": ["completed"]}


def test_the_workflows_it_watches_exist_under_those_names() -> None:
    names = {_workflow(p).get("name") for p in WORKFLOWS.glob("*.yml") if p != WORKFLOW}
    assert {"CI", "Infra"} <= names


def test_one_cron_line_off_minute_zero() -> None:
    """Twice an hour, as the sibling's ``8,38``, at minutes of its own."""
    (entry,) = _workflow()[True]["schedule"]
    assert entry == {"cron": "19,49 * * * *"}
    minute, *rest = entry["cron"].split()
    assert rest == ["*", "*", "*", "*"]
    minutes = [int(m) for m in minute.split(",")]
    assert minutes
    assert all(0 < m < 60 for m in minutes)


def test_permissions_are_none_by_default_and_read_only_to_build() -> None:
    assert _workflow()["permissions"] == {}
    assert _jobs()["board-build"]["permissions"] == {
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
        "checks": "read",
        "statuses": "read",
    }
    deploy = _jobs()["board-deploy"]
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert deploy["environment"] == {
        "name": "github-pages",
        "url": "${{ steps.deployment.outputs.page_url }}",
    }


def test_runs_queue_in_the_pages_group_without_cancelling() -> None:
    assert _workflow()["concurrency"] == {"group": "pages", "cancel-in-progress": False}


def test_the_workflow_and_each_job_carry_exactly_these_keys() -> None:
    """A key not listed here changes where or how a job runs, so none may appear.

    ``runs-on: self-hosted`` would put a write token on a machine strangers' events can
    reach, ``continue-on-error`` would deploy past a failed build, and a job-level
    ``container``, ``env`` or ``concurrency`` would change what the steps run in or
    let a second deploy race the first. A workflow-level ``env`` or ``defaults``
    reaches every step the same way.
    """
    workflow = _workflow()
    assert "env" not in workflow
    assert "defaults" not in workflow
    assert set(workflow) == {"name", True, "permissions", "concurrency", "jobs"}
    jobs = _jobs()
    assert set(jobs["board-build"]) == {"runs-on", "timeout-minutes", "permissions", "steps"}
    assert set(jobs["board-deploy"]) == {
        "needs",
        "runs-on",
        "timeout-minutes",
        "permissions",
        "environment",
        "steps",
    }
    assert {name: job["runs-on"] for name, job in jobs.items()} == {
        "board-build": "ubuntu-latest",
        "board-deploy": "ubuntu-latest",
    }


def test_the_jobs_are_named_by_key_only_and_none_is_test() -> None:
    jobs = _jobs()
    assert set(jobs) == JOBS
    assert all("name" not in job for job in jobs.values())
    assert jobs["board-deploy"]["needs"] == "board-build"
    assert "if:" not in WORKFLOW.read_text(encoding="utf-8")


def test_every_action_is_pinned_to_a_commit_and_only_these_three_run() -> None:
    uses = [step["uses"] for step in _steps() if "uses" in step]
    for use in uses:
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", use), use
    assert sorted(use.split("@")[0] for use in uses) == [
        "actions/checkout",
        "actions/deploy-pages",
        "actions/upload-pages-artifact",
    ]


def test_one_checkout_of_the_default_ref_keeping_no_credentials() -> None:
    (checkout,) = [s for s in _steps() if s.get("uses", "").startswith("actions/checkout@")]
    assert checkout["with"] == {"persist-credentials": False}


EXPRESSION = re.compile(r"\$\{\{(.*?)\}\}", re.S)


def _strings(node: Any) -> list[str]:
    """Every key and value in the parsed workflow that is a string."""
    if isinstance(node, dict):
        return [s for k, v in node.items() for s in _strings(k) + _strings(v)]
    if isinstance(node, list):
        return [s for v in node for s in _strings(v)]
    return [node] if isinstance(node, str) else []


def test_the_only_expressions_are_the_token_and_the_page_address() -> None:
    """No expression can carry an event's text into a step.

    The file's text and every string YAML parses out of it are both searched, and an
    expression may span a line break, which a folded or literal block allows.
    """
    expected = ["github.token", "steps.deployment.outputs.page_url"]
    text = WORKFLOW.read_text(encoding="utf-8")
    assert sorted({" ".join(e.split()) for e in EXPRESSION.findall(text)}) == expected
    parsed = {" ".join(e.split()) for s in _strings(_workflow()) for e in EXPRESSION.findall(s)}
    assert sorted(parsed) == expected
    for step in _steps():
        assert "${{" not in step.get("run", ""), step


def test_the_expression_search_spans_line_breaks() -> None:
    """The control for the search above: a split expression is still found."""
    folded = yaml.safe_load("run: |\n  echo ${{\n    github.event.issue.title }}\n")
    assert [" ".join(e.split()) for e in EXPRESSION.findall(folded["run"])] == [
        "github.event.issue.title"
    ]


SHA = re.compile(r"@[0-9a-f]{40}$")


def _unpinned(step: dict[str, Any]) -> dict[str, Any]:
    """A step with its action's commit replaced, so the shape compares without it."""
    return {k: SHA.sub("@<sha>", v) if k == "uses" else v for k, v in step.items()}


def test_the_build_runs_exactly_the_generator_and_the_copy() -> None:
    """Any other step could read the payload, or publish something else."""
    steps = [_unpinned(s) for s in _jobs()["board-build"]["steps"]]
    for step in steps:
        if "run" in step:
            step["run"] = " ".join(step["run"].split())
    assert steps == [
        {"uses": "actions/checkout@<sha>", "with": {"persist-credentials": False}},
        {
            "name": "Build board.json from GitHub",
            "env": {"GH_TOKEN": "${{ github.token }}"},
            "run": "python3 .claude/skills/sync-prune-next/build_board.py build"
            ' --repo "$GITHUB_REPOSITORY" --out _site',
        },
        {
            "name": "Add the page",
            "run": "cp .claude/skills/sync-prune-next/board.html _site/index.html",
        },
        {"uses": "actions/upload-pages-artifact@<sha>", "with": {"path": "_site"}},
    ]
    for path in ("build_board.py", "board.html"):
        assert (ROOT / ".claude" / "skills" / "sync-prune-next" / path).is_file()


def test_the_deploy_runs_exactly_the_one_deploy_step() -> None:
    """``id: deployment`` is what the environment's address reads its URL from."""
    steps = [_unpinned(s) for s in _jobs()["board-deploy"]["steps"]]
    assert steps == [{"id": "deployment", "uses": "actions/deploy-pages@<sha>"}]


def test_each_job_sets_a_ten_minute_timeout() -> None:
    """Ten minutes bounds a hung step far inside GitHub's default of 360."""
    assert {name: job.get("timeout-minutes") for name, job in _jobs().items()} == {
        "board-build": 10,
        "board-deploy": 10,
    }


def test_no_other_workflow_uses_the_boards_job_names() -> None:
    """The generator drops checks named board-build and board-deploy from a rollup.

    ``deploy.yml`` and ``infra.yml`` name their jobs, so both the job keys and their
    ``name:`` values are compared.
    """
    for path in WORKFLOWS.glob("*.yml"):
        if path == WORKFLOW:
            continue
        jobs = _workflow(path).get("jobs") or {}
        used = set(jobs) | {str(job.get("name")) for job in jobs.values() if job.get("name")}
        assert not used & JOBS, f"{path.name} uses {sorted(used & JOBS)}"
