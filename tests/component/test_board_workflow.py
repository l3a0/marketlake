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

from tests.support.board_page import icon_links

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "board.yml"
JOBS = {"board-build", "board-deploy"}
SKILL_DIR = ".claude/skills/sync-prune-next"
SKILL = ROOT / SKILL_DIR


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
    (entry,) = _workflow()[True]["schedule"]
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


def test_the_only_expressions_are_the_token_and_the_page_address() -> None:
    """No expression can carry an event's text into a step."""
    text = WORKFLOW.read_text(encoding="utf-8")
    found = sorted({" ".join(e.split()) for e in re.findall(r"\$\{\{(.*?)\}\}", text)})
    assert found == ["github.token", "steps.deployment.outputs.page_url"]
    for step in _steps():
        assert "${{" not in step.get("run", ""), step


def test_the_build_runs_the_generator_and_adds_the_page() -> None:
    runs = [" ".join(s["run"].split()) for s in _jobs()["board-build"]["steps"] if "run" in s]
    assert any(
        r.startswith("python3 .claude/skills/sync-prune-next/build_board.py build") for r in runs
    )
    assert any(
        r.startswith("cp .claude/skills/sync-prune-next/board.html _site/index.html") for r in runs
    )
    (build,) = [s for s in _jobs()["board-build"]["steps"] if "build_board.py" in s.get("run", "")]
    assert build["env"] == {"GH_TOKEN": "${{ github.token }}"}
    for path in ("build_board.py", "board.html"):
        assert (ROOT / ".claude" / "skills" / "sync-prune-next" / path).is_file()


def test_the_icon_is_published_where_the_page_links_it() -> None:
    """Protects the board's tab from a blank icon (#884).

    The page links its icon by a relative address, so Pages serves it only when the
    step that adds the page puts the icon in the same folder under that name. A step
    that dropped the copy, or a page that renamed the link, would leave a browser
    asking for a file that is not there. The icon's copy runs only after the page's
    copy succeeds, so joining the two with ``||`` would skip it on every run that works.
    The link is read from the parsed head, so one inside a comment or a ``noscript``
    block, which a browser never loads, does not count.
    """
    (link,) = icon_links((SKILL / "board.html").read_text(encoding="utf-8"))
    href = link["href"]
    (step,) = [s for s in _jobs()["board-build"]["steps"] if s.get("name") == "Add the page"]
    assert " ".join(step["run"].split()) == (
        f"cp {SKILL_DIR}/board.html _site/index.html && cp {SKILL_DIR}/{href} _site/{href}"
    )
    assert (SKILL / href).is_file()


def test_each_job_sets_a_timeout() -> None:
    for name, job in _jobs().items():
        assert isinstance(job.get("timeout-minutes"), int), name


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
