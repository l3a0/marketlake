"""Checks on ``.github/workflows/board.yml``, which starts the build board's rebuild (#942).

Since #942 the generator, the page and their tests live in l3a0/repo-template, and this
workflow only calls that repository's reusable workflow, ``build-board.yml``, at a pinned
commit. The template's suite checks the template's own caller, not this one. So what this
repository still owns of the board is checked here:

1. The triggers, the schedule and the concurrency group.
2. The pin and the scopes the job grants.
3. The inputs, and the two files they name in this repository: the icon and the
   documents the note links to.
4. That no expression can carry an event's text into the run.

No trigger the workflow carries, ``workflow_dispatch`` included, runs a branch's copy of
it before it is on ``main``, so these rules are checked here, in ``ci.yml``'s required
``test`` job, before then. Nothing here makes a network read, so nothing here checks that
the pinned commit exists or that its workflow still takes these inputs. The first board
run on ``main`` after the pin moves is that check.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from lake import favicon

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "board.yml"
REUSABLE = "l3a0/repo-template/.github/workflows/build-board.yml"


def _workflow(path: Path = WORKFLOW) -> dict[Any, Any]:
    with path.open() as f:
        return yaml.safe_load(f)


def _job() -> dict[str, Any]:
    return _workflow()["jobs"]["board"]


def _inputs() -> dict[str, Any]:
    return _job()["with"]


def test_the_triggers_are_the_siblings_with_infra_watched_too() -> None:
    # YAML 1.1 reads the bare key `on` as true.
    triggers = _workflow()[True]
    assert list(triggers) == [
        "issues",
        "issue_comment",
        "pull_request_target",
        "workflow_run",
        "schedule",
        "workflow_dispatch",
    ]
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
    """Twice an hour, at minutes of its own.

    The shared generator reads this line to say when the board is stale, and it stops
    the run on any schedule that is not one list of minutes past every hour. So a line
    of another shape would pass this repository's CI and fail every board run.
    """
    (entry,) = _workflow()[True]["schedule"]
    assert entry == {"cron": "19,49 * * * *"}
    minute, *rest = entry["cron"].split()
    assert rest == ["*", "*", "*", "*"]
    minutes = [int(m) for m in minute.split(",")]
    assert minutes
    assert all(0 < m < 60 for m in minutes)


def test_runs_queue_in_the_pages_group_without_cancelling() -> None:
    assert _workflow()["concurrency"] == {"group": "pages", "cancel-in-progress": False}


def test_the_workflow_and_its_one_job_carry_exactly_these_keys() -> None:
    """A key not listed here changes where or how the run goes, so none may appear.

    A workflow-level ``env`` or ``defaults`` would reach every step of the called
    workflow's jobs. A job-level ``secrets: inherit`` would hand them every secret this
    repository holds, where the job token is all they need. The job is not named
    ``test``, because the ruleset on main requires a check by that name and ``ci.yml``
    provides it.
    """
    workflow = _workflow()
    assert set(workflow) == {"name", True, "permissions", "concurrency", "jobs"}
    assert workflow["name"] == "Board"
    assert set(workflow["jobs"]) == {"board"}
    assert set(_job()) == {"permissions", "uses", "with"}


def test_the_template_is_pinned_to_the_commit_the_header_names() -> None:
    """Protects the board from changing without a pull request here.

    A full commit SHA cannot be moved, while a branch or a tag can, so the called
    workflow, and the generator and page it checks out at its own commit, change only
    when this line does. The header names the same commit, so a reader comparing it with
    the template compares the right file.
    """
    path, _, ref = _job()["uses"].partition("@")
    assert path == REUSABLE
    assert re.fullmatch(r"[0-9a-f]{40}", ref), ref
    header = WORKFLOW.read_text(encoding="utf-8").split("\non:\n", 1)[0]
    assert re.findall(r"\b[0-9a-f]{40}\b", header) == [ref]


def test_the_job_grants_what_the_reusable_workflow_asks_for() -> None:
    """Protects the run from a job asking for a scope the caller did not grant.

    A called workflow can only narrow what its caller grants. These are the scopes the
    template's build and deploy jobs declare at the pinned commit. Moving the pin means
    reading the template's ``build-board.yml`` at the new commit, since its jobs may ask
    for more.
    """
    assert _workflow()["permissions"] == {}
    assert _job()["permissions"] == {
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
        "checks": "read",
        "statuses": "read",
        "actions": "read",
        "pages": "write",
        "id-token": "write",
    }


NOTE = (
    "What each deliverable is for lives in "
    "[the design doc](https://github.com/l3a0/marketlake/blob/main/docs/design.md), "
    "and the slice order in "
    "[the build plan](https://github.com/l3a0/marketlake/blob/main/docs/build-plan.md). "
    "The order below follows the repository's ranking rule: capture reliability first, "
    "then what makes the lake usable, and evidence from real use over any ranking made "
    "in advance."
)


def test_the_inputs_turn_on_what_the_old_page_did() -> None:
    """Protects the board from losing what marketlake's own page drew before #942.

    Every input of the reusable workflow is a string, and an empty one leaves the shared
    board's default in place. So each on-or-off input is the quoted string ``"true"``,
    since a bare ``true`` reaches GitHub as a boolean.
    """
    assert _inputs() == {
        "title": "Marketlake Build Board",
        "icon": ".github/board/favicon.ico",
        "subtitle": "the order of work on the capture-first option-chain lake",
        "note": NOTE,
        "default-branch-head": "true",
        "milestone-default": "MVP <n>",
        "ranking": "order",
        "owner-body-lines": "true",
        "review-heading": "Review complete",
        "settled-rollup": "true",
        "sort-labels": "true",
        "label-colors": "true",
    }
    assert all(isinstance(value, str) for value in _inputs().values())


def test_the_icon_input_names_the_file_lake_favicon_writes() -> None:
    """Protects the board's tab from a blank icon (#884).

    The reusable workflow copies the file the ``icon`` input names, and fails the run
    when it is missing. ``python -m lake.favicon --board`` regenerates that file, so the
    module and the input name the same path.
    """
    icon = _inputs()["icon"]
    assert icon == favicon.BOARD_ICON
    assert (ROOT / icon).is_file()


def test_the_note_links_only_to_documents_this_repository_holds() -> None:
    """Protects the note from a link that leads nowhere.

    The shared generator fails the run on a link to anything but an ``https://``
    address. A document renamed here would still pass that check and leave the board
    linking to a 404, so each link is checked against the file it names.
    """
    links = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", _inputs()["note"])
    assert [words for words, _ in links] == ["the design doc", "the build plan"]
    prefix = "https://github.com/l3a0/marketlake/blob/main/"
    for _, url in links:
        assert url.startswith(prefix), url
        assert (ROOT / url.removeprefix(prefix)).is_file(), url


EXPRESSION = re.compile(r"\$\{\{(.*?)\}\}", re.S)


def test_no_expression_can_carry_an_events_text_into_the_run() -> None:
    """Anyone can file an issue on a public repository, so its title is untrusted.

    An expression here would be the only way the event payload reaches the called
    workflow, and every input is a literal, so the file holds none. The search spans line
    breaks, which a folded block such as the note's allows.
    """
    assert EXPRESSION.findall(WORKFLOW.read_text(encoding="utf-8")) == []


def test_the_expression_search_spans_line_breaks() -> None:
    """The control for the search above: a split expression is still found."""
    folded = yaml.safe_load("run: |\n  echo ${{\n    github.event.issue.title }}\n")
    assert [" ".join(e.split()) for e in EXPRESSION.findall(folded["run"])] == [
        "github.event.issue.title"
    ]
