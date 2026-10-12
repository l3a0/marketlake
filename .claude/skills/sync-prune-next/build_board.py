"""Build the build board's data from GitHub, for the page on GitHub Pages.

The build board answers what is in flight and what to take next. Until issue 872 a
sync round measured every figure on it by hand and wrote them into a claude.ai
artifact's database. That cost a turn on every change, missed any change no session
saw, and tied the board to one claude.ai account. This script reads the figures GitHub
already holds and writes them as one ``board.json``. ``.github/workflows/board.yml``
runs it and publishes the file beside ``board.html`` on GitHub Pages. That workflow's
``on:`` block lists what starts a run. A submitted review starts none, so it reaches
the board with the next run. No Claude runs at any step. It is a port of
l3a0/quantitative-trading's ``scripts/build_board.py``, cut to what marketlake's page
draws.

The document keeps the flat shape the page already reads::

    {schema, state, prs, working, planned, next, tracker}

1. ``state`` carries ``main``, the default branch's head as seven characters,
   ``updatedAt``, the time GitHub was read, and ``issues.open``.
2. ``tracker`` has one card per open issue. ``ms`` is the milestone's title up to its
   first comma, so "MVP 2, capture on a hosted VM" reads "MVP 2", the name the page's
   default view selects. ``kind`` is ``parent`` while an open sub-issue exists, else
   ``deferred`` under that label, else ``ready``. ``needs`` holds the open issues in
   this repository that a native "blocked by" link names.
3. ``prs`` has one entry per open pull request and open issue it names, through
   GitHub's closing link or a ``Part of #N`` reference outside code. A closing link to
   an issue in another repository names none here.
4. ``working``, ``planned`` and ``next`` stay empty until issue 873 moves those
   judgements onto GitHub.

Two rules exist because the repository is public.

1. ``reviewed`` counts a ``## Review complete`` marker only from an author whose
   association is ``OWNER``, and a pull request from a fork gets no entry. Anyone can
   comment, and anyone can open a fork whose closing keyword GitHub registers.
2. Nothing here reads the event that started the run. Every fact comes from the API,
   so an issue title never reaches a shell, and two runs started by different events
   build the same board from the same state.

It fails closed. A failed call, a GraphQL error, a top-level list whose read count
disagrees with its ``totalCount``, or a nested list with more entries than one read
returns stops the run with a non-zero exit, and Pages keeps the last good board.
Treating an error as an empty list would publish a board that reads as correct and
is not. Comments and reviews are the exception. Each is read as its newest 100, and a
longer list never stops the run, because only a marker newer than the head commit
counts. Such a marker falls outside the newest 100 only when a hundred more comments
follow it, or for a review, a hundred more reviews.

It reads every open issue and pull request through two paged GraphQL queries, one
request per page. The job token allows 1,000 points an hour per repository, so the
request and point counts go to the job summary, and a test fixes the request count.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = 1

# A `Part of` reference, read the way GitHub reads a closing keyword: anywhere in a
# pull request's body, with `#` and the number right after it, and never inside a code
# span or a fenced code block. The one difference from GitHub, which reads a keyword in
# any case, is that the reference has to be capitalised, because lowercase prose such
# as "merged in PR 488, part of 458" names another pull request's issue.
PART_OF = re.compile(r"\bPart of #(\d+)\b")

# The marker that closes a review, per `CLAUDE.md` under "Pull requests". The whole
# first line has to be the heading, so `## Review completeness check` is not one.
REVIEW_COMPLETE = re.compile(r"\A## Review complete\r?(\n|\Z)")
REVIEWED = "review complete"
NOT_REVIEWED = "no Review complete marker since the head commit"

# The board workflow's own jobs. A `pull_request_target` run attaches its check runs to
# the pull request's head commit, so without this a reviewed pull request would read as
# still running while the board rebuilds, and a failed board run as a failed check.
BOARD_JOBS = frozenset({"board-build", "board-deploy"})
BOARD_APP = "github-actions"

# The five words step 3 of the sync-prune-next skill lists. A check run that has not
# completed is `running` whatever its conclusion says, and any state not named here,
# including one GitHub adds later, is `neutral`, which the page reads as unsettled.
CHECK_WORD = {
    "SUCCESS": "success",
    "SKIPPED": "skipped",
    "FAILURE": "failure",
    "TIMED_OUT": "failure",
    "STARTUP_FAILURE": "failure",
    "ERROR": "failure",
    "PENDING": "running",
    "EXPECTED": "running",
    "NEUTRAL": "neutral",
    "CANCELLED": "neutral",
    "STALE": "neutral",
    "ACTION_REQUIRED": "neutral",
}

# A fenced code block, opened by three or more backticks or tildes and closed by the
# same run, or left open to the end of the text.
FENCE = re.compile(
    r"^[ \t]*(`{3,}|~{3,})[^\n]*\n.*?(?:^[ \t]*\1[ \t]*$|\Z)", re.MULTILINE | re.DOTALL
)

# A run of backticks, which opens an inline code span, and the blank line that ends a
# paragraph. A span closes only at the next run of exactly as many backticks inside the
# same paragraph, and a run nothing closes is literal text. That approximates CommonMark
# rather than implementing it, as the sibling's generator says at length.
TICKS = re.compile(r"`+")
PARAGRAPH = re.compile(r"\n[ \t]*\n")

# No nested list is paged. Each is read at 100, which costs no more than 20, and a list
# with more stops the run. The largest counts measured on 2026-10-12 were 17 blocking
# links, 4 labels, 2 closing references and 11 checks. Comments and reviews are read as
# the newest 100, because only a marker newer than the head commit counts, and no pull
# request here has had more than 8 comments. An older marker cut off by that window
# leaves `reviewed` false, which keeps a card out of the owner's queue.
ISSUES_QUERY = """
query($owner:String!, $name:String!, $after:String) {
  rateLimit{cost}
  repository(owner:$owner, name:$name) {
    defaultBranchRef{target{oid}}
    issues(states:OPEN, first:100, after:$after, orderBy:{field:CREATED_AT, direction:ASC}) {
      totalCount pageInfo{hasNextPage endCursor}
      nodes {
        number title
        milestone{title}
        labels(first:100){pageInfo{hasNextPage} nodes{name}}
        blockedBy(first:100){pageInfo{hasNextPage} nodes{number state repository{nameWithOwner}}}
        subIssuesSummary{total completed}
      }
    }
  }
}
"""

PRS_QUERY = """
query($owner:String!, $name:String!, $after:String) {
  rateLimit{cost}
  repository(owner:$owner, name:$name) {
    pullRequests(states:OPEN, first:50, after:$after, orderBy:{field:CREATED_AT, direction:ASC}) {
      totalCount pageInfo{hasNextPage endCursor}
      nodes {
        number body isCrossRepository
        closingIssuesReferences(first:100){pageInfo{hasNextPage}
          nodes{number repository{nameWithOwner}}}
        comments(last:100){nodes{authorAssociation body createdAt}}
        reviews(last:100){nodes{authorAssociation body submittedAt}}
        commits(last:1){nodes{commit{committedDate statusCheckRollup{
          contexts(first:100){pageInfo{hasNextPage} nodes{__typename
            ... on CheckRun{name status conclusion checkSuite{app{slug}}}
            ... on StatusContext{context state}}}}}}}
      }
    }
  }
}
"""

Gql = Callable[[str, dict], dict]


class BoardError(RuntimeError):
    """A read that cannot be trusted, which stops the run."""


class GhGraphql:
    """Runs GraphQL through ``gh api graphql`` and counts what it spends.

    Every call is one request. ``cost`` sums the points GitHub charged, read from the
    ``rateLimit`` field each query asks for.
    """

    def __init__(self) -> None:
        self.requests = 0
        self.cost = 0

    def __call__(self, query: str, variables: dict) -> dict:
        args = ["gh", "api", "graphql", "-f", f"query={query}"]
        for key, value in variables.items():
            if value is None:
                continue
            flag = "-F" if isinstance(value, int) else "-f"
            args += [flag, f"{key}={value}"]
        self.requests += 1
        try:
            done = subprocess.run(args, capture_output=True, text=True, check=False)
        except OSError as e:
            raise BoardError(f"gh could not be run: {e}") from e
        if done.returncode != 0:
            raise BoardError(f"gh api graphql failed: {done.stderr.strip() or done.stdout}")
        try:
            body = json.loads(done.stdout)
        except json.JSONDecodeError as e:
            raise BoardError(f"gh api graphql returned no JSON: {e}") from e
        if body.get("errors"):
            raise BoardError(f"GraphQL errors: {json.dumps(body['errors'])}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise BoardError("GraphQL returned no data")
        self.cost += int((data.get("rateLimit") or {}).get("cost") or 0)
        return data


def _spans_out(paragraph: str) -> str:
    """One paragraph with each code span replaced by a NUL character.

    A NUL rather than nothing or a space, so the text either side of a span never joins
    into "Part of #", and ``\\b`` still matches beside it.
    """
    kept, start, pos = [], 0, 0
    while opener := TICKS.search(paragraph, pos):
        width = len(opener.group())
        closer = next(
            (m for m in TICKS.finditer(paragraph, opener.end()) if len(m.group()) == width), None
        )
        if closer is None:
            pos = opener.end()
            continue
        kept.append(paragraph[start : opener.start()])
        start = pos = closer.end()
    kept.append(paragraph[start:])
    return "\0".join(kept)


def outside_code(text: str) -> str:
    """The text with every fenced code block and inline code span removed.

    GitHub reads no closing keyword inside either, so a ``Part of`` reference inside
    one does not count here either.
    """
    # GitHub's web editor saves `\r\n`, which the fence's `$` and the blank line
    # between paragraphs would otherwise never match.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return "\n\n".join(_spans_out(p) for p in PARAGRAPH.split(FENCE.sub("", text)))


def _whole(conn: dict, what: str) -> list[dict]:
    """The nodes of a nested list, or a stop when it holds more than one read returns."""
    if (conn.get("pageInfo") or {}).get("hasNextPage"):
        raise BoardError(f"{what} holds more than 100 entries, and nested lists are not paged")
    return list(conn.get("nodes") or [])


def _top_pages(gql: Gql, query: str, owner: str, name: str, field: str) -> tuple[list, dict]:
    """Every node of a repository-level list, checked against its total.

    Returns the nodes and the repository object of the first page, which carries the
    fields read once per run. A list that changed between pages reads more or fewer
    entries than its count, and the count is the only sign of it, so a disagreement
    stops the run. The run the change itself starts reads the list again.
    """
    nodes: list[dict] = []
    after = None
    total = None
    first: dict | None = None
    while True:
        data = gql(query, {"owner": owner, "name": name, "after": after})
        repository = data.get("repository")
        if not isinstance(repository, dict):
            raise BoardError(f"repository {owner}/{name} could not be read")
        if first is None:
            first = repository
        conn = repository[field]
        if total is None:
            total = conn["totalCount"]
        nodes += conn["nodes"]
        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
    numbers = {n["number"] for n in nodes}
    if len(nodes) != total or len(numbers) != total:
        raise BoardError(f"read {len(nodes)} {field} against a total of {total}")
    return nodes, first or {}


def milestone_short(milestone: dict | None) -> str:
    """The milestone's title up to its first comma, or ``no milestone``."""
    title = (milestone or {}).get("title")
    if not title:
        return "no milestone"
    return title.split(",", 1)[0].strip()


def card_kind(labels: list[str], sub_issues: dict | None) -> str:
    """``parent`` while an open sub-issue exists, else ``deferred``, else ``ready``.

    ``subIssuesSummary`` counts closed sub-issues in ``total`` too, so an open one is
    the difference between ``total`` and ``completed``.
    """
    summary = sub_issues or {}
    if (summary.get("total") or 0) - (summary.get("completed") or 0) > 0:
        return "parent"
    if "deferred" in labels:
        return "deferred"
    return "ready"


def in_repo(node: dict, repo: str) -> bool:
    """Whether a linked issue is in ``repo``. GitHub spells the owner as it likes."""
    return ((node.get("repository") or {}).get("nameWithOwner") or "").lower() == repo.lower()


def tracker_card(issue: dict, open_issues: set[int], repo: str) -> dict:
    """One open issue as the card the page draws.

    ``needs`` keeps only open blockers in this repository. A blocker in another
    repository would match a marketlake issue with the same number and link to the
    wrong one.
    """
    n = issue["number"]
    labels = sorted(lb["name"] for lb in _whole(issue["labels"], f"labels on #{n}"))
    blockers = _whole(issue["blockedBy"], f"blocked-by links on #{n}")
    needs = sorted(
        {
            b["number"]
            for b in blockers
            if b.get("state") == "OPEN" and in_repo(b, repo) and b["number"] in open_issues
        }
    )
    return {
        "n": n,
        "label": issue["title"],
        "labels": labels,
        "ms": milestone_short(issue.get("milestone")),
        "kind": card_kind(labels, issue.get("subIssuesSummary")),
        "needs": needs,
    }


def check_word(context: dict) -> str:
    """The page's word for one check run or status context."""
    if context.get("__typename") == "CheckRun":
        if context.get("status") != "COMPLETED":
            return "running"
        state = context.get("conclusion")
    else:
        state = context.get("state")
    return CHECK_WORD.get(state or "", "neutral")


def is_board_check(context: dict) -> bool:
    """Whether a context is one of this board workflow's own jobs."""
    app = ((context.get("checkSuite") or {}).get("app") or {}).get("slug")
    return (
        context.get("__typename") == "CheckRun"
        and context.get("name") in BOARD_JOBS
        and app == BOARD_APP
    )


def rollup(head: dict | None, pr: int) -> list[list[str]]:
    """The head commit's checks as ``[name, word]`` pairs, without the board's own."""
    checks = (head or {}).get("statusCheckRollup")
    if not checks:
        return []
    out = []
    for c in _whole(checks["contexts"], f"checks on PR #{pr}"):
        if is_board_check(c):
            continue
        name = c.get("name") if c.get("__typename") == "CheckRun" else c.get("context")
        out.append([str(name), check_word(c)])
    return sorted(out)


def _stamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def review_complete(entries: list[dict], stamp_field: str, head_date: datetime | None) -> bool:
    """Whether the owner posted a ``## Review complete`` marker after the head commit."""
    for entry in entries:
        if entry.get("authorAssociation") != "OWNER":
            continue
        if not REVIEW_COMPLETE.match(entry.get("body") or ""):
            continue
        posted = _stamp(entry.get(stamp_field))
        if posted is not None and head_date is not None and posted > head_date:
            return True
    return False


def pr_entries(pr: dict, open_issues: set[int], repo: str) -> list[dict]:
    """One entry per open issue the pull request names, or none at all.

    A pull request names an issue through GitHub's closing link or through a ``Part
    of`` reference outside code. One that names no open issue gets no entry, because
    the page keys pull requests by issue. One from a fork gets none either. A closing
    link to an issue in another repository is dropped, as ``tracker_card`` drops such
    a blocker, because its number would match a marketlake issue it does not close.
    """
    if pr.get("isCrossRepository"):
        return []
    n = pr["number"]
    closing = {
        c["number"]
        for c in _whole(pr["closingIssuesReferences"], f"links on PR #{n}")
        if in_repo(c, repo)
    }
    part_of = {int(m) for m in PART_OF.findall(outside_code(pr.get("body") or ""))}
    named = sorted((closing | part_of) & open_issues)
    if not named:
        return []
    commits = (pr.get("commits") or {}).get("nodes") or []
    head = commits[0]["commit"] if commits else None
    head_date = _stamp((head or {}).get("committedDate"))
    comments = (pr.get("comments") or {}).get("nodes") or []
    reviews = (pr.get("reviews") or {}).get("nodes") or []
    reviewed = review_complete(comments, "createdAt", head_date) or review_complete(
        reviews, "submittedAt", head_date
    )
    checks = rollup(head, n)
    return [
        {
            "issue": issue,
            "pr": n,
            "state": "open",
            "review": REVIEWED if reviewed else NOT_REVIEWED,
            "linked": issue in closing,
            "partOf": issue not in closing,
            "reviewed": reviewed,
            "rollup": checks,
        }
        for issue in named
    ]


def build(gql: Gql, repo: str, now: datetime) -> tuple[dict, dict]:
    """The board, and what the job summary reports about the read."""
    owner, _, name = repo.partition("/")
    if not owner or not name or "/" in name:
        raise BoardError(f"--repo must be owner/name, not {repo!r}")
    issues, repository = _top_pages(gql, ISSUES_QUERY, owner, name, "issues")
    oid = ((repository.get("defaultBranchRef") or {}).get("target") or {}).get("oid")
    if not oid:
        raise BoardError(f"{repo} has no default branch head to read")
    open_issues = {i["number"] for i in issues}
    tracker = [
        tracker_card(i, open_issues, repo) for i in sorted(issues, key=lambda i: i["number"])
    ]

    pulls, _ = _top_pages(gql, PRS_QUERY, owner, name, "pullRequests")
    prs = [entry for pr in pulls for entry in pr_entries(pr, open_issues, repo)]
    prs.sort(key=lambda p: (p["issue"], p["pr"]))

    board = {
        "schema": SCHEMA,
        "state": {
            "main": oid[:7],
            "updatedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "issues": {"open": len(issues)},
        },
        "prs": prs,
        "working": [],
        "planned": [],
        "next": [],
        "tracker": tracker,
    }
    report = {
        "open_issues": len(issues),
        "open_prs": len(pulls),
        "pr_entries": len(prs),
    }
    return board, report


def _summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    text = "\n".join(lines) + "\n"
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


def cmd_build(args: argparse.Namespace) -> None:
    gql = GhGraphql()
    board, report = build(gql, args.repo, datetime.now(UTC))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "board.json").write_text(
        json.dumps(board, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    _summary(
        [
            "## Board build",
            "",
            "| Measure | Value |",
            "| --- | --- |",
            f"| GitHub read at | {board['state']['updatedAt']} |",
            f"| main | {board['state']['main']} |",
            f"| GraphQL requests | {gql.requests} |",
            f"| GraphQL points | {gql.cost} |",
            f"| Open issues | {report['open_issues']} |",
            f"| Open pull requests | {report['open_prs']} |",
            f"| Pull request entries written | {report['pr_entries']} |",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="read GitHub and write board.json")
    b.add_argument("--repo", required=True)
    b.add_argument("--out", required=True)
    b.set_defaults(func=cmd_build)
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except BoardError as e:
        print(f"build_board: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
