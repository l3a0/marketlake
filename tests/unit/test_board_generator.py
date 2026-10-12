"""The build board's generator, run on a fake GitHub.

``.claude/skills/sync-prune-next/build_board.py`` reads GitHub through GraphQL and
writes the ``board.json`` that the page on GitHub Pages draws. Two failures matter
most.

1. A board that reads as correct and is not. A stranger's review marker moving a pull
   request into the owner's queue, a failing check read as green, or an error read as
   an empty list would each publish such a board.
2. A board the page refuses or draws wrong. A milestone written in full switches the
   default view to "All", and an unknown ``kind`` writes a broken sentence.

Every test feeds the generator ``tests/support/board_github.py``'s fake, so nothing
here reaches GitHub. The command-line test replaces the real client the same way.
"""

from __future__ import annotations

import json
import random
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.support.board_github import (
    HEAD,
    REPO,
    FakeGitHub,
    blocker,
    check,
    issue,
    load_generator,
    note,
    pull,
    status,
)

bb = load_generator()
NOW = datetime(2026, 10, 12, 17, 0, tzinfo=UTC)
LATER = datetime(2026, 10, 12, 17, 30, tzinfo=UTC)
COMMITTED = "2026-10-12T12:00:00Z"
AFTER = "2026-10-12T13:00:00Z"
BEFORE = "2026-10-12T11:00:00Z"
MARKER = "## Review complete\n\nEvery lens reported."


def build(gh: FakeGitHub, now: datetime = NOW) -> dict[str, Any]:
    return bb.build(gh, REPO, now)[0]


def card(board: dict[str, Any], n: int) -> dict[str, Any]:
    (found,) = [c for c in board["tracker"] if c["n"] == n]
    return found


def entries(board: dict[str, Any]) -> list[tuple[int, int]]:
    return [(p["pr"], p["issue"]) for p in board["prs"]]


# -- the document --------------------------------------------------------------------


def test_the_document_has_the_flat_shape_the_page_reads() -> None:
    board = build(FakeGitHub([issue(1), issue(2)], [pull(10, closes=(1,))]))
    assert list(board) == ["schema", "state", "prs", "working", "planned", "next", "tracker"]
    assert board["schema"] == 1
    assert board["state"] == {
        "main": HEAD[:7],
        "updatedAt": "2026-10-12T17:00:00Z",
        "issues": {"open": 2},
    }
    assert board["working"] == board["planned"] == board["next"] == []
    assert list(board["tracker"][0]) == ["n", "label", "labels", "ms", "kind", "needs"]
    assert list(board["prs"][0]) == [
        "issue",
        "pr",
        "state",
        "review",
        "linked",
        "partOf",
        "reviewed",
        "rollup",
    ]


def test_two_runs_over_the_same_data_differ_only_in_the_stamp() -> None:
    """Lists come out sorted whatever order GitHub returns them in.

    Issue 5's labels arrive unsorted, and its blockers are 16 and 1, which a Python
    set of small integers iterates as 16 then 1, so neither list is sorted by accident.
    """
    issues = [
        issue(5, labels=("enhancement", "bug"), blocked_by=(blocker(16), blocker(1))),
        issue(16),
        issue(1),
    ]
    prs = [
        pull(
            20,
            closes=(5, 16),
            contexts=(check("test", "SUCCESS"), status("CodeQL", "SUCCESS"), check("a", "SKIPPED")),
        ),
        pull(11, body="Part of #1."),
    ]
    first = build(FakeGitHub(issues, prs), NOW)
    shuffled_issues = [dict(i) for i in issues]
    random.Random(7).shuffle(shuffled_issues)
    (five,) = [i for i in shuffled_issues if i["number"] == 5]
    five["labels"] = dict(five["labels"], nodes=list(reversed(five["labels"]["nodes"])))
    second = build(FakeGitHub(shuffled_issues, list(reversed(prs))), LATER)
    assert first["state"].pop("updatedAt") != second["state"].pop("updatedAt")
    assert json.dumps(first) == json.dumps(second)
    assert [c["n"] for c in first["tracker"]] == [1, 5, 16]
    assert card(first, 5)["labels"] == card(second, 5)["labels"] == ["bug", "enhancement"]
    assert card(first, 5)["needs"] == [1, 16]
    assert entries(first) == [(11, 1), (20, 5), (20, 16)]


def test_entries_sort_by_issue_then_by_pull_request() -> None:
    issues = [issue(1), issue(2)]
    prs = [pull(10, closes=(2,)), pull(20, closes=(1,)), pull(5, closes=(2,))]
    assert entries(build(FakeGitHub(issues, prs))) == [(20, 1), (5, 2), (10, 2)]


# -- tracker -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("MVP 2, capture on a hosted VM", "MVP 2"),
        ("MVP 3", "MVP 3"),
        ("MVP 2 , spaced before the comma", "MVP 2"),
        (None, "no milestone"),
    ],
)
def test_the_milestone_is_its_title_up_to_the_first_comma(title: str | None, expected: str) -> None:
    """The page's default view selects "MVP 2", so the full title would switch it to All."""
    assert card(build(FakeGitHub([issue(1, milestone=title)])), 1)["ms"] == expected


@pytest.mark.parametrize(
    ("labels", "sub_issues", "kind"),
    [
        (("deferred",), (2, 1), "parent"),
        (("enhancement",), (1, 0), "parent"),
        (("deferred",), (2, 2), "deferred"),
        (("enhancement", "deferred"), (0, 0), "deferred"),
        (("enhancement",), (3, 3), "ready"),
        ((), (0, 0), "ready"),
    ],
)
def test_kind_is_parent_then_deferred_then_ready(
    labels: tuple[str, ...], sub_issues: tuple[int, int], kind: str
) -> None:
    """Closed sub-issues make no parent, because ``subIssuesSummary`` counts them too."""
    board = build(FakeGitHub([issue(1, labels=labels, sub_issues=sub_issues)]))
    assert card(board, 1)["kind"] == kind


def test_needs_keeps_open_blockers_in_this_repository_once_each() -> None:
    issues = [
        issue(
            1,
            blocked_by=(
                blocker(2),
                blocker(2),
                blocker(3, "CLOSED"),
                blocker(4, repo="l3a0/quantitative-trading"),
                blocker(9),
                blocker(5, repo="L3A0/Marketlake"),
            ),
        ),
        issue(2),
        issue(3),
        issue(4),
        issue(5),
    ]
    # 3 is closed, 4 belongs to another repository though marketlake has an open #4, and
    # 9 is no open issue here. GitHub spells the owner as it likes, so case is ignored.
    assert card(build(FakeGitHub(issues)), 1)["needs"] == [2, 5]


# -- prs -----------------------------------------------------------------------------


def test_a_pull_request_naming_two_issues_gives_two_entries() -> None:
    board = build(FakeGitHub([issue(1), issue(2)], [pull(10, closes=(1,), body="Part of #2.")]))
    linked, part_of = board["prs"]
    assert (linked["issue"], linked["linked"], linked["partOf"]) == (1, True, False)
    assert (part_of["issue"], part_of["linked"], part_of["partOf"]) == (2, False, True)
    assert linked["state"] == part_of["state"] == "open"


def test_an_issue_both_linked_and_named_part_of_reads_as_linked() -> None:
    """The closing link decides, because merging closes the issue whatever else is said."""
    (entry,) = build(FakeGitHub([issue(1)], [pull(10, closes=(1,), body="Part of #1.")]))["prs"]
    assert (entry["linked"], entry["partOf"]) == (True, False)


def test_a_closing_link_to_another_repository_names_no_issue_here() -> None:
    """Its number would otherwise land on the marketlake issue that shares it."""
    elsewhere = ((2, "l3a0/quantitative-trading"), (3, "L3A0/Marketlake"))
    prs = [
        pull(10, closes=(1,), closes_elsewhere=elsewhere),
        pull(11, closes_elsewhere=((4, "l3a0/quantitative-trading"),), body="Part of #4."),
    ]
    board = build(FakeGitHub([issue(1), issue(2), issue(3), issue(4)], prs))
    # GitHub spells the owner as it likes, so 3 is in this repository.
    assert entries(board) == [(10, 1), (10, 3), (11, 4)]
    four = board["prs"][-1]
    assert (four["linked"], four["partOf"]) == (False, True)


@pytest.mark.parametrize(
    ("query", "field"),
    [("PRS_QUERY", "closingIssuesReferences"), ("ISSUES_QUERY", "blockedBy")],
)
def test_the_query_reads_each_linked_issues_repository(query: str, field: str) -> None:
    """The fake serves the repository whatever the query asks for, and GitHub does not.

    Without it every link reads as another repository's, and the board drops them all.
    """
    text = " ".join(getattr(bb, query).split())
    selection = re.search(rf"{field}\(first:100\)\{{(.*?)\}}\}}", text)
    assert selection, f"{query} reads no {field}"
    assert re.search(r"nodes\{[^}]*\brepository\{nameWithOwner", selection.group(1))


@pytest.mark.parametrize(
    "body",
    [
        "Mentions `Part of #1` in a code span.",
        "```\nPart of #1\n```",
        "~~~\nPart of #1\n~~~",
        "```\n\nPart of #1\n\n```",
        "This is part of #1, in lowercase prose.",
        "Part of #1x is no reference.",
        "Part of #2, which is closed.",
        "NotPart of #1 is another word.",
        "Part of 1 has no hash.",
        "Part of `x`#1 is split by a code span.",
        "``a` Part of #1 ``",
        "`` never closed, then `Part of #1` in a span.",
    ],
)
def test_a_part_of_reference_counts_only_outside_code_and_capitalised(body: str) -> None:
    assert build(FakeGitHub([issue(1)], [pull(10, body=body)]))["prs"] == []


@pytest.mark.parametrize(
    "body",
    [
        "```\nfenced\n```\nPart of #1.",
        "A stray ` tick.\n\nPart of #1, and another stray `.",
        "A stray ` tick.\r\n\r\nPart of #1, and another stray `.",
    ],
    ids=["after a closed fence", "ticks in two paragraphs", "the same with CRLF"],
)
def test_a_part_of_reference_outside_code_counts(body: str) -> None:
    """A fence ends at its closing line, and a code span never crosses a blank line."""
    (entry,) = build(FakeGitHub([issue(1)], [pull(10, body=body)]))["prs"]
    assert (entry["issue"], entry["partOf"]) == (1, True)


def test_a_pull_request_naming_no_open_issue_gets_no_entry() -> None:
    board = build(FakeGitHub([issue(1)], [pull(10), pull(11, closes=(7,))]))
    assert board["prs"] == []


def test_a_pull_request_from_a_fork_gets_no_entry() -> None:
    """GitHub registers a fork's closing keyword, so a stranger could move a card."""
    board = build(FakeGitHub([issue(1)], [pull(10, closes=(1,), fork=True)]))
    assert board["prs"] == []


@pytest.mark.parametrize(
    ("comments", "reviews", "reviewed"),
    [
        ((note(MARKER, AFTER),), (), True),
        ((), (note(MARKER, AFTER, review=True),), True),
        ((note("## Review complete", AFTER),), (), True),
        ((note("## Review complete\r\nAll lenses.", AFTER),), (), True),
        ((note(MARKER, AFTER, "CONTRIBUTOR"),), (), False),
        ((note(MARKER, AFTER, "COLLABORATOR"),), (), False),
        ((), (note(MARKER, AFTER, "MEMBER", review=True),), False),
        ((note(MARKER, BEFORE),), (), False),
        ((), (note(MARKER, BEFORE, review=True),), False),
        ((note(MARKER, COMMITTED),), (), False),
        ((note(MARKER, None),), (), False),
        ((note("## Review complete  \n\nTrailing blanks.", AFTER),), (), False),
        ((note("## Review completeness check\n\nStill running.", AFTER),), (), False),
        ((note("Notes first.\n## Review complete", AFTER),), (), False),
        ((note(" ## Review complete", AFTER),), (), False),
        ((note("## Review\n\nThe mutation lens is still running.", AFTER),), (), False),
    ],
)
def test_only_the_owners_marker_after_the_head_commit_counts(
    comments: tuple[dict, ...], reviews: tuple[dict, ...], reviewed: bool
) -> None:
    pr = pull(10, closes=(1,), comments=comments, reviews=reviews, committed=COMMITTED)
    (entry,) = build(FakeGitHub([issue(1)], [pr]))["prs"]
    assert entry["reviewed"] is reviewed
    assert entry["review"] == (
        "review complete" if reviewed else "no Review complete marker since the head commit"
    )


def test_a_head_commit_with_no_date_reads_as_unreviewed() -> None:
    """With nothing to compare against, no marker can be shown to be newer."""
    pr = pull(10, closes=(1,), comments=(note(MARKER, AFTER),), committed=None)
    (entry,) = build(FakeGitHub([issue(1)], [pr]))["prs"]
    assert entry["reviewed"] is False


@pytest.mark.parametrize(
    ("context", "word"),
    [
        (check("t", "SUCCESS"), "success"),
        (check("t", "SKIPPED"), "skipped"),
        (check("t", "FAILURE"), "failure"),
        (check("t", "TIMED_OUT"), "failure"),
        (check("t", "STARTUP_FAILURE"), "failure"),
        (check("t", None, status="IN_PROGRESS"), "running"),
        (check("t", "SUCCESS", status="QUEUED"), "running"),
        (check("t", "NEUTRAL"), "neutral"),
        (check("t", "CANCELLED"), "neutral"),
        (check("t", "STALE"), "neutral"),
        (check("t", "ACTION_REQUIRED"), "neutral"),
        (check("t", "SOMETHING_NEW"), "neutral"),
        (status("t", "SUCCESS"), "success"),
        (status("t", "FAILURE"), "failure"),
        (status("t", "ERROR"), "failure"),
        (status("t", "PENDING"), "running"),
        (status("t", "EXPECTED"), "running"),
    ],
)
def test_each_check_state_maps_to_one_of_the_five_words(context: dict, word: str) -> None:
    (entry,) = build(FakeGitHub([issue(1)], [pull(10, closes=(1,), contexts=(context,))]))["prs"]
    assert entry["rollup"] == [["t", word]]


def test_the_boards_own_checks_leave_the_rollup() -> None:
    """A reviewed pull request would otherwise read as running while the board rebuilds."""
    contexts = (
        check("test", "SUCCESS"),
        check("board-build", None, status="IN_PROGRESS"),
        check("board-deploy", "FAILURE"),
        check("board-build", "FAILURE", app="some-other-app"),
        status("board-deploy", "PENDING"),
        check("board-other", "FAILURE"),
    )
    (entry,) = build(FakeGitHub([issue(1)], [pull(10, closes=(1,), contexts=contexts)]))["prs"]
    assert entry["rollup"] == [
        ["board-build", "failure"],
        ["board-deploy", "running"],
        ["board-other", "failure"],
        ["test", "success"],
    ]


def test_a_head_commit_with_no_checks_has_an_empty_rollup() -> None:
    (entry,) = build(FakeGitHub([issue(1)], [pull(10, closes=(1,), contexts=None)]))["prs"]
    assert entry["rollup"] == []


# -- what stops the run --------------------------------------------------------------


def test_one_request_per_page_of_issues_and_per_page_of_pull_requests() -> None:
    """250 issues read in 3 pages of 100 and 60 pull requests in 2 pages of 50."""
    issues = [issue(n) for n in range(1, 251)]
    prs = [pull(1000 + n, closes=(n,)) for n in range(1, 61)]
    gh = FakeGitHub(issues, prs)
    board = build(gh)
    assert len(gh.calls) == 5
    assert [v["after"] for _, v in gh.calls] == [None, "100", "200", None, "50"]
    assert board["state"]["issues"]["open"] == 250
    assert len(board["prs"]) == 60


@pytest.mark.parametrize(
    "gh",
    [
        FakeGitHub([issue(1)], issue_total=2),
        FakeGitHub([issue(1), issue(2)], issue_total=1),
        FakeGitHub([issue(1)], [pull(10, closes=(1,))], pr_total=2),
    ],
    ids=["issues short", "issues long", "pull requests short"],
)
def test_a_list_that_disagrees_with_its_count_stops_the_run(gh: FakeGitHub) -> None:
    with pytest.raises(bb.BoardError, match="against a total of"):
        build(gh)


@pytest.mark.parametrize(
    "issues",
    [[issue(1), issue(1)], [issue(1), issue(1), issue(2)]],
    ids=["one short", "one extra"],
)
def test_a_page_repeating_an_issue_stops_the_run(issues: list[dict[str, Any]]) -> None:
    """A repeat with an extra issue still holds as many numbers as the count says."""
    with pytest.raises(bb.BoardError, match="against a total of"):
        build(FakeGitHub(issues, issue_total=2))


class _Moving(FakeGitHub):
    """A repository that changes after the first page: one issue fewer, or a new head."""

    def __init__(self, *args: Any, what: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.what = what

    def __call__(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        data = super().__call__(query, variables)
        if variables.get("after") and "issues(states:OPEN" in query:
            repository = data["repository"]
            if self.what == "count":
                repository["issues"]["totalCount"] -= 1
            else:
                repository["defaultBranchRef"]["target"]["oid"] = "f" * 40
        return data


def test_the_count_is_read_from_the_first_page() -> None:
    """A later page agreeing with the read does not excuse a first page that did not."""
    gh = _Moving([issue(n) for n in range(1, 151)], issue_total=151, what="count")
    with pytest.raises(bb.BoardError, match="read 150 issues against a total of 151"):
        build(gh)


def test_main_is_read_from_the_first_page() -> None:
    board = build(_Moving([issue(n) for n in range(1, 151)], what="head"))
    assert board["state"]["main"] == HEAD[:7]


def _overflow(what: str) -> FakeGitHub:
    one = issue(1)
    pr = pull(10, closes=(1,), contexts=(check("test", "SUCCESS"),))
    if what in ("labels", "blockedBy"):
        one[what]["pageInfo"]["hasNextPage"] = True
    elif what == "closingIssuesReferences":
        pr[what]["pageInfo"]["hasNextPage"] = True
    else:
        rollup = pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]
        rollup["contexts"]["pageInfo"]["hasNextPage"] = True
    return FakeGitHub([one], [pr])


@pytest.mark.parametrize("what", ["labels", "blockedBy", "closingIssuesReferences", "contexts"])
def test_a_nested_list_longer_than_one_read_stops_the_run(what: str) -> None:
    with pytest.raises(bb.BoardError, match="nested lists are not paged"):
        build(_overflow(what))


def test_a_repository_with_no_default_branch_stops_the_run() -> None:
    with pytest.raises(bb.BoardError, match="no default branch"):
        build(FakeGitHub([issue(1)], head=""))


# -- the client and the command line -------------------------------------------------


class _Done:
    def __init__(self, returncode: int, stdout: str, stderr: str = "") -> None:
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def test_the_client_counts_requests_and_points_and_passes_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []

    def run(args: list[str], **_: Any) -> _Done:
        seen.append(args)
        return _Done(0, json.dumps({"data": {"rateLimit": {"cost": 2}, "x": 1}}))

    monkeypatch.setattr(bb.subprocess, "run", run)
    gql = bb.GhGraphql()
    assert gql("query{x}", {"owner": "l3a0", "after": None, "n": 3}) == {
        "rateLimit": {"cost": 2},
        "x": 1,
    }
    gql("query{x}", {})
    assert (gql.requests, gql.cost) == (2, 4)
    assert seen[0] == [
        "gh",
        "api",
        "graphql",
        "-f",
        "query=query{x}",
        "-f",
        "owner=l3a0",
        "-F",
        "n=3",
    ]


@pytest.mark.parametrize(
    ("done", "message"),
    [
        (_Done(1, "", "HTTP 401: Bad credentials"), "gh api graphql failed: HTTP 401"),
        (_Done(0, "not json"), "returned no JSON"),
        (_Done(0, json.dumps({"errors": [{"message": "no field"}]})), "GraphQL errors"),
        (
            _Done(0, json.dumps({"data": {"x": 1}, "errors": [{"message": "partial"}]})),
            "GraphQL errors",
        ),
        (_Done(0, json.dumps({"data": None})), "returned no data"),
    ],
    ids=["failed call", "not JSON", "errors", "errors beside partial data", "no data"],
)
def test_the_client_stops_on_a_failed_call_or_an_error(
    monkeypatch: pytest.MonkeyPatch, done: _Done, message: str
) -> None:
    monkeypatch.setattr(bb.subprocess, "run", lambda *a, **k: done)
    with pytest.raises(bb.BoardError, match=message):
        bb.GhGraphql()("query{x}", {})


def _fake_client(gh: FakeGitHub) -> type:
    class Client:
        def __init__(self) -> None:
            self.requests = 0
            self.cost = 0

        def __call__(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            self.requests += 1
            data = gh(query, variables)
            self.cost += data["rateLimit"]["cost"]
            return data

    return Client


@pytest.fixture
def ci_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """CI sets both files in every step, so the command line writes into tmp_path."""
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output"))
    return summary


def test_the_command_line_writes_board_json_and_the_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ci_env: Path
) -> None:
    gh = FakeGitHub([issue(1), issue(2)], [pull(10, closes=(1, 2)), pull(11), pull(12)])
    monkeypatch.setattr(bb, "GhGraphql", _fake_client(gh))
    out = tmp_path / "site"
    assert bb.main(["build", "--repo", REPO, "--out", str(out)]) == 0
    board = json.loads((out / "board.json").read_text(encoding="utf-8"))
    assert [c["n"] for c in board["tracker"]] == [1, 2]
    summary = ci_env.read_text(encoding="utf-8")
    assert f"| main | {HEAD[:7]} |" in summary
    assert "| GraphQL requests | 2 |" in summary
    assert "| GraphQL points | 5 |" in summary
    assert "| Open issues | 2 |" in summary
    assert "| Open pull requests | 3 |" in summary
    assert "| Pull request entries written | 2 |" in summary
    assert not (tmp_path / "output").exists()


def test_the_command_line_prints_the_summary_without_a_step_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    monkeypatch.setattr(bb, "GhGraphql", _fake_client(FakeGitHub([issue(1)])))
    assert bb.main(["build", "--repo", REPO, "--out", str(tmp_path / "site")]) == 0
    assert "| GraphQL requests | 2 |" in capsys.readouterr().out


def test_the_command_line_stops_with_one_line_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ci_env: Path,
) -> None:
    gh = FakeGitHub([issue(1)], issue_total=2)
    monkeypatch.setattr(bb, "GhGraphql", _fake_client(gh))
    out = tmp_path / "site"
    assert bb.main(["build", "--repo", REPO, "--out", str(out)]) == 1
    err = capsys.readouterr().err
    assert err == "build_board: read 1 issues against a total of 2\n"
    assert not (out / "board.json").exists()
    assert not ci_env.exists()


@pytest.mark.parametrize("repo", ["marketlake", "l3a0/", "/marketlake", "l3a0/marketlake/x"])
def test_a_repo_that_is_not_owner_and_name_stops_with_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], repo: str
) -> None:
    monkeypatch.setattr(bb, "GhGraphql", _fake_client(FakeGitHub([issue(1)])))
    assert bb.main(["build", "--repo", repo, "--out", str(tmp_path / "site")]) == 1
    assert capsys.readouterr().err == f"build_board: --repo must be owner/name, not {repo!r}\n"


def test_a_missing_gh_stops_with_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The real client, with a PATH on which no ``gh`` exists."""
    monkeypatch.setenv("PATH", str(tmp_path))
    assert bb.main(["build", "--repo", REPO, "--out", str(tmp_path / "site")]) == 1
    err = capsys.readouterr().err
    assert err.startswith("build_board: gh could not be run: ")
    assert err.count("\n") == 1
    assert not (tmp_path / "site").exists()
