"""A fake GitHub for the build board's generator, and builders for what it serves.

``.claude/skills/sync-prune-next/build_board.py`` reads GitHub through ``gh api
graphql``, and its ``build`` function takes the GraphQL client as an argument. The
tests always pass this fake, so none of them reaches GitHub. ``tests/conftest.py``
guards neither ``gh`` nor the network for a child process, so that is the only thing
keeping a test off the real repository.

The generator lives beside the page rather than in the package, so it is loaded by
path, as ``tests/unit/test_plan_summary.py`` loads ``infra/ci/plan_summary.py``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SKILL = ROOT / ".claude" / "skills" / "sync-prune-next"
GENERATOR = SKILL / "build_board.py"
REPO = "l3a0/marketlake"
HEAD = "5f0b12c0123456789abcdef0123456789abcdef0"


def load_generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("build_board", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def conn(nodes: list[Any], *, more: bool = False) -> dict[str, Any]:
    """One read of a nested GraphQL list."""
    return {"nodes": nodes, "pageInfo": {"hasNextPage": more}}


def blocker(n: int, state: str = "OPEN", repo: str = REPO) -> dict[str, Any]:
    """One "blocked by" link, to an issue in ``repo``."""
    return {"number": n, "state": state, "repository": {"nameWithOwner": repo}}


def issue(
    n: int,
    *,
    title: str | None = None,
    milestone: str | None = "MVP 2, capture on a hosted VM",
    labels: tuple[str, ...] = ("enhancement",),
    blocked_by: tuple[dict[str, Any], ...] = (),
    sub_issues: tuple[int, int] = (0, 0),
    body: str = "",
    comments: tuple[str | tuple[str, str], ...] = (),
    association: str = "OWNER",
) -> dict[str, Any]:
    """An open issue as the issues query returns it. ``sub_issues`` is (total, completed).

    ``association`` is the issue author's ``authorAssociation``. Each comment is its
    body, posted by the owner, or an ``(authorAssociation, body)`` pair, oldest first
    as GitHub lists them.
    """
    notes = [("OWNER", c) if isinstance(c, str) else c for c in comments]
    return {
        "number": n,
        "title": title or f"Issue {n}",
        "body": body,
        "authorAssociation": association,
        "milestone": {"title": milestone} if milestone is not None else None,
        "labels": conn([{"name": x} for x in labels]),
        "blockedBy": conn(list(blocked_by)),
        "subIssuesSummary": {"total": sub_issues[0], "completed": sub_issues[1]},
        "comments": {"nodes": [{"authorAssociation": a, "body": b} for a, b in notes]},
    }


def check(
    name: str, conclusion: str | None, status: str = "COMPLETED", app: str = "github-actions"
) -> dict[str, Any]:
    return {
        "__typename": "CheckRun",
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "checkSuite": {"app": {"slug": app}},
    }


def status(context: str, state: str) -> dict[str, Any]:
    return {"__typename": "StatusContext", "context": context, "state": state}


def note(body: str, at: str | None, association: str = "OWNER", *, review: bool = False) -> dict:
    """A comment, or with ``review`` a review, posted at ``at``."""
    return {
        "authorAssociation": association,
        "body": body,
        "submittedAt" if review else "createdAt": at,
    }


def pull(
    n: int,
    *,
    body: str = "",
    closes: tuple[int, ...] = (),
    comments: tuple[dict[str, Any], ...] = (),
    reviews: tuple[dict[str, Any], ...] = (),
    contexts: tuple[dict[str, Any], ...] | None = (),
    committed: str | None = "2026-10-12T12:00:00Z",
    fork: bool = False,
    closes_elsewhere: tuple[tuple[int, str], ...] = (),
) -> dict[str, Any]:
    """An open pull request as the pull requests query returns it.

    ``contexts`` of None is a head commit with no status check rollup at all.
    ``closes`` names issues in this repository, and ``closes_elsewhere`` names
    ``(number, owner/name)`` pairs in others.
    """
    rollup = None if contexts is None else {"contexts": conn(list(contexts))}
    closing = [(i, REPO) for i in closes] + list(closes_elsewhere)
    return {
        "number": n,
        "body": body,
        "isCrossRepository": fork,
        "closingIssuesReferences": conn(
            [{"number": i, "repository": {"nameWithOwner": r}} for i, r in closing]
        ),
        "comments": {"nodes": list(comments)},
        "reviews": {"nodes": list(reviews)},
        "commits": {
            "nodes": [{"commit": {"committedDate": committed, "statusCheckRollup": rollup}}]
        },
    }


class FakeGitHub:
    """Answers the generator's two queries from fixtures, and records each call.

    Open issues come in pages of 100 and open pull requests in pages of 50, the sizes
    the real queries ask for. ``issue_total`` and ``pr_total`` override the
    ``totalCount`` each page reports, which is how a list that changed between pages
    looks.
    """

    def __init__(
        self,
        issues: list[dict[str, Any]],
        prs: list[dict[str, Any]] | None = None,
        *,
        issue_total: int | None = None,
        pr_total: int | None = None,
        head: str = HEAD,
    ) -> None:
        self.issues = list(issues)
        self.prs = list(prs or [])
        self.issue_total = issue_total
        self.pr_total = pr_total
        self.head = head
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @staticmethod
    def _page(nodes: list[dict], size: int, after: str | None, total: int | None) -> dict:
        start = int(after) if after else 0
        end = start + size
        return {
            "totalCount": len(nodes) if total is None else total,
            "nodes": nodes[start:end],
            "pageInfo": {"hasNextPage": end < len(nodes), "endCursor": str(end)},
        }

    def __call__(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((query, dict(variables)))
        after = variables.get("after")
        if "issues(states:OPEN" in query:
            page = self._page(self.issues, 100, after, self.issue_total)
            repository = {"defaultBranchRef": {"target": {"oid": self.head}}, "issues": page}
            return {"rateLimit": {"cost": 2}, "repository": repository}
        if "pullRequests(states:OPEN" in query:
            page = self._page(self.prs, 50, after, self.pr_total)
            return {"rateLimit": {"cost": 3}, "repository": {"pullRequests": page}}
        raise AssertionError(f"unexpected query: {query[:80]}")
