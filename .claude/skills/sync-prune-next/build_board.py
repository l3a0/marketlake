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
4. ``working`` has one entry per open issue carrying a ``working:build`` or
   ``working:decompose`` label, which a session sets while it holds the card.
5. ``planned`` has one entry per open issue whose owner comments carry a ``Board
   verdict:`` line, which a plan loop posts when it exits.
6. ``next`` has one entry per open issue whose body carries a ``Board order:`` line.
   ``band`` is 0 in every entry, because the order alone ranks a card. A ``Board
   after:`` line in the body sets the tracker card's ``after``.

The ``l3a0`` plugin's ``update-build-board`` skill states what a session sets and
when, and its ``## Generator compatibility`` names the commit of this file whose
formats it matches. So a change to a marker regex, a label list or a query field
ships with an l3a0/claude-plugins release that updates that skill, in the same step.
Issue 873 moved those judgements onto GitHub. A marker line the strict
grammar fails, two values that disagree, a line someone other than the owner
wrote, and a verdict or order on a card that cannot carry one never stop the run.
Each goes to the job summary, which names the issue and the marker's kind and never
the line's text, because a refused line is a stranger's and the summary reaches a
session's context.

Two rules exist because the repository is public.

1. ``reviewed`` counts a ``## Review complete`` marker only from an author whose
   association is ``OWNER``, and a pull request from a fork gets no entry. Anyone can
   comment, and anyone can open a fork whose closing keyword GitHub registers. The
   same holds for the board's markers. A comment's verdict counts only when the
   comment's association is ``OWNER``, and a body line only when the issue's is,
   since only its author or someone with write access can edit a body. Labels need
   triage access, so a ``working:`` label needs no such check.
2. Nothing here reads the event that started the run. Every fact comes from the API,
   so an issue title never reaches a shell, and two runs started by different events
   build the same board from the same state.

It fails closed. A failed call, a GraphQL error, a top-level list whose read count
disagrees with its ``totalCount``, or a nested list with more entries than one read
returns stops the run with a non-zero exit, and Pages keeps the last good board.
Treating an error as an empty list would publish a board that reads as correct and
is not. Comments and reviews are the exception. Each is read as its newest 100, and a
longer list never stops the run. On a pull request only a marker newer than the head
commit counts, and on an issue the newest verdict wins. Such a marker falls outside
the newest 100 only when a hundred more comments follow it, or for a review, a hundred
more reviews.

It reads every open issue and pull request through two paged GraphQL queries, one
request per page. The job token allows 1,000 points an hour per repository, so the
request and point counts go to the job summary, and a test fixes the request count.
Reading issue bodies and comments brought a build to about 15 points on 2026-10-12,
so the 42 runs not cancelled in one busy hour that day would spend about 630.
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

# The lines and labels a session sets for the judgements the page draws, as the
# l3a0 plugin's update-build-board skill states them. They are
# l3a0/quantitative-trading's grammar: case-sensitive, one ASCII space after the
# colon, and spaces or tabs allowed before and after. Each is anchored at the start
# of a line, so prose quoting one mid-sentence is not one. A verdict reads "Board
# verdict: build after 5 passes", or "decide" for a loop that ended on a question
# only the owner can answer.
VERDICT = re.compile(
    r"^[ \t]*Board verdict: (build|decide) after (\d+) pass(?:es)?\.?[ \t]*$", re.MULTILINE
)
ORDER = re.compile(r"^[ \t]*Board order: (-?\d+(?:\.\d+)?)[ \t]*$", re.MULTILINE)
AFTER = re.compile(r"^[ \t]*Board after:(.*)$", re.MULTILINE)
WORKING = {"working:build": "build", "working:decompose": "decompose"}

# A line that looks meant as a marker. It starts, after whitespace, any of `>*_+-` and
# any list number such as `1.` or `1)`, with "board" and the marker's kind in any case.
# The kind must then end the line, or be followed by optional `*` or `_`, optional
# whitespace, and a `:`, a digit or a `#`. So `Board order 3`, `Board Order: 3` and
# `**Board order**: 3` are near misses, while prose such as "Board orders form one
# sequence" or "Board after the merge" is not a marker at all. One the strict grammar
# above fails is reported as unread rather than ignored, so a typo never drops a marker
# silently. Matching anywhere on a line instead would report this repository's prose
# about the markers on every run.
MARKER_LINE = re.compile(
    r"^(?:[^\S\n]|[>*_+-]|\d{1,9}[.)])*board[^\S\n]+(order|verdict|after)"
    r"(?:[*_]*[^\S\n]*[:#\d]|[*_]*[^\S\n]*$)",
    re.MULTILINE | re.IGNORECASE,
)
MARKER_NAME = {"order": "Board order", "verdict": "Board verdict", "after": "Board after"}

# The most digits an order or a pass count may carry. JavaScript reads every number as
# a double, which holds any integer up to 15 digits exactly, and past that a number
# can stop being one: a 400-digit order is infinity, which JSON cannot carry, and past
# 4,300 digits Python's `int()` raises.
MAX_DIGITS = 15

# The references a `Board after:` line can name: `#N`, `owner/name#N`, and the link
# form `[#N](url)`. The `{1,15}` and the `\b` after it read a longer number as no
# reference at all, rather than one `int()` cannot parse.
LINK_REF = re.compile(r"\[#(\d{1,15})\]\(([^)\s]*)\)")
# An issue's or pull request's address, with anything after the number, such as a
# comment's `#issuecomment-5` anchor, a query or a `/files` tab.
ISSUE_URL = re.compile(r"https://github\.com/([^/\s]+/[^/\s]+)/(?:issues|pull)/(\d+)(?:[/#?]\S*)?")
REPO_REF = re.compile(r"(?<![\w.-])([\w.-]+/[\w.-]+)#(\d{1,15})\b")
BARE_REF = re.compile(r"(?<![\w/.-])#(\d{1,15})\b")

# The heading each plan-audit pass opens with, per the update-build-board skill. A
# pass newer than an issue's newest verdict is a plan that finished with no verdict.
PASS_HEADING = re.compile(r"^[ \t]*## Plan audit, pass \d+", re.MULTILINE)

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

# The five words the update-build-board skill lists under `rollup`. A check run that
# has not completed is `running` whatever its conclusion says, and any state not named here,
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
# the newest 100, and none goes through `_whole`, because a `last:` read reports more
# through `hasPreviousPage`, which `_whole` does not check. On a pull request only a
# marker newer than the head commit counts, and no pull request here has had more than
# 8 comments. An older marker cut off by that window leaves `reviewed` false, which
# keeps a card out of the owner's queue. On an issue the newest verdict wins, and no
# open issue had more than 40 comments on 2026-10-12.
ISSUES_QUERY = """
query($owner:String!, $name:String!, $after:String) {
  rateLimit{cost}
  repository(owner:$owner, name:$name) {
    defaultBranchRef{target{oid}}
    issues(states:OPEN, first:100, after:$after, orderBy:{field:CREATED_AT, direction:ASC}) {
      totalCount pageInfo{hasNextPage endCursor}
      nodes {
        number title body authorAssociation
        milestone{title}
        labels(first:100){pageInfo{hasNextPage} nodes{name}}
        blockedBy(first:100){pageInfo{hasNextPage} nodes{number state repository{nameWithOwner}}}
        subIssuesSummary{total completed}
        comments(last:100){nodes{authorAssociation body}}
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
    return "\n\n".join(_spans_out(p) for p in PARAGRAPH.split(unfenced(text)))


def unfenced(text: str) -> str:
    """The text with line endings made ``\\n`` and every fenced code block removed.

    GitHub's web editor saves ``\\r\\n``, which the fence's ``$``, the blank line
    between paragraphs and every line-anchored marker would otherwise never match, so
    an owner's edit in a browser would be ignored without a word. A marker inside a
    fence quotes the format rather than setting anything, so no line in one is read.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return FENCE.sub("", text)


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


class Notes:
    """What the job summary lists about the markers, each as ``(issue, phrase)``.

    Every phrase is the generator's own words. None carries a line's text, because a
    refused line is a stranger's, and echoing it would carry that text into a
    session's context and could break the summary's table on a ``|``.
    """

    def __init__(self) -> None:
        self.conflicts: set[tuple[int, str]] = set()
        self.unread: set[tuple[int, str]] = set()
        self.refused: set[tuple[int, str]] = set()
        self.dropped: set[tuple[int, str]] = set()


def _marker_lines(text: str) -> list[tuple[str, str, int]]:
    """Each line of already unfenced text that looks meant as a marker, with its kind
    and the offset where the line starts."""
    found = []
    offset = 0
    for line in text.split("\n"):
        m = MARKER_LINE.match(line)
        if m:
            found.append((m.group(1).lower(), line, offset))
        offset += len(line) + 1
    return found


def _short(number: str) -> bool:
    """Whether a number carries no more than ``MAX_DIGITS`` digits."""
    return sum(ch.isdigit() for ch in number) <= MAX_DIGITS


def _order_value(text: str) -> int | float:
    value = float(text)
    return int(value) if value.is_integer() else value


def after_refs(rest: str, repo: str) -> tuple[set[int], set[str]]:
    """The issues in ``repo`` that the text after ``Board after:`` names, and the
    summary phrase for each kind of reference it refused.

    A reference to another repository is refused, whether written ``owner/name#N`` or
    as a link whose address names another repository. Read as a bare ``#N``, its
    number would match a marketlake issue it does not mean, the trap ``tracker_card``
    avoids for ``needs``. A link whose text names one number and whose address names
    another is refused too, because nothing says which one was meant.
    """
    named: set[int] = set()
    refused: set[str] = set()

    def link(m: re.Match[str]) -> str:
        url = ISSUE_URL.fullmatch(m.group(2))
        if not url or url.group(1).lower() != repo.lower():
            refused.add("Board after, naming another repository")
        elif url.group(2) != m.group(1):
            refused.add("Board after, link and number disagree")
        else:
            named.add(int(m.group(1)))
        return "\0"

    rest = LINK_REF.sub(link, rest)
    for m in REPO_REF.finditer(rest):
        if m.group(1).lower() == repo.lower():
            named.add(int(m.group(2)))
        else:
            refused.add("Board after, naming another repository")
    rest = REPO_REF.sub("\0", rest)
    named |= {int(x) for x in BARE_REF.findall(rest)}
    return named, refused


def read_body(issue: dict, repo: str, notes: Notes) -> tuple[list[int | float], set[int]]:
    """The ``Board order:`` values and ``Board after:`` issues an issue's body sets.

    Only a body the owner wrote counts, read from the issue's ``authorAssociation``.
    Only the author or someone with write access can edit an issue body, so an
    owner's issue was last written by the owner. A body by anyone else sets nothing,
    and a line in it that the strict grammar reads is listed as refused. Its near
    misses are not listed at all, so a stranger cannot fill the summary.

    In an owner's body, a line that looks meant as a marker and is not read is listed
    as unread. That covers a line the strict grammar fails, an order with too many
    digits, an after line naming no issue, and a verdict line, which counts only in
    a comment.
    """
    n = issue["number"]
    text = unfenced(issue.get("body") or "")
    if issue.get("authorAssociation") != "OWNER":
        for kind in ("order", "after"):
            if (ORDER if kind == "order" else AFTER).search(text):
                notes.refused.add((n, f"{MARKER_NAME[kind]}, not the owner's"))
        return [], set()
    orders: list[int | float] = []
    after: set[int] = set()
    for kind, line, _ in _marker_lines(text):
        if kind == "order":
            m = ORDER.match(line)
            if m and _short(m.group(1)):
                orders.append(_order_value(m.group(1)))
                continue
        elif kind == "after":
            m = AFTER.match(line)
            if m:
                named, refused = after_refs(m.group(1), repo)
                after |= named
                notes.refused |= {(n, phrase) for phrase in refused}
                if named or refused:
                    continue
        notes.unread.add((n, MARKER_NAME[kind]))
    return orders, after


def read_comments(issue: dict, notes: Notes) -> tuple[tuple[str, int] | None, bool]:
    """The issue's newest owner verdict, and whether a plan-audit pass came after it.

    GitHub lists comments oldest first, so the last verdict line read is the newest.
    Only a comment whose ``authorAssociation`` is ``OWNER`` counts, for a verdict and
    for a pass heading alike. A stranger's verdict line is listed as refused, and a
    stranger's pass heading counts for nothing. In an owner's comment, a line that
    looks meant as a marker and is not read is listed as unread, which includes an
    order or after line, since those count only in a body.
    """
    n = issue["number"]
    comments = (issue.get("comments") or {}).get("nodes") or []
    verdict: tuple[str, int] | None = None
    # Positions are (comment index, offset in the comment), so a pass heading after the
    # verdict in the same comment counts as newer, and one above it does not.
    verdict_at = last_pass = (-1, -1)
    for i, c in enumerate(comments):
        text = unfenced(c.get("body") or "")
        if c.get("authorAssociation") != "OWNER":
            if VERDICT.search(text):
                notes.refused.add((n, "Board verdict, not the owner's"))
            continue
        for heading in PASS_HEADING.finditer(text):
            last_pass = (i, heading.start())
        for kind, line, offset in _marker_lines(text):
            m = VERDICT.match(line) if kind == "verdict" else None
            if m and _short(m.group(2)):
                verdict, verdict_at = (m.group(1), int(m.group(2))), (i, offset)
            else:
                notes.unread.add((n, MARKER_NAME[kind]))
    return verdict, last_pass > verdict_at


def tracker_card(issue: dict, open_issues: set[int], repo: str, after: set[int]) -> dict:
    """One open issue as the card the page draws.

    ``needs`` keeps only open blockers in this repository. A blocker in another
    repository would match a marketlake issue with the same number and link to the
    wrong one. ``after`` keeps the open issues a ``Board after:`` line names, other
    than the card itself, and is left out when none is left, as the page reads it as
    optional. The ``working:`` labels are drawn as the card's place under In flight,
    so they are left off its label chips.
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
    card = {
        "n": n,
        "label": issue["title"],
        "labels": [x for x in labels if x not in WORKING],
        "ms": milestone_short(issue.get("milestone")),
        "kind": card_kind(labels, issue.get("subIssuesSummary")),
        "needs": needs,
    }
    kept = sorted((after & open_issues) - {n})
    if kept:
        card["after"] = kept
    return card


def session_entries(
    issue: dict, card: dict, orders: list[int | float], notes: Notes
) -> tuple[dict | None, dict | None, dict | None, bool]:
    """The issue's ``working``, ``planned`` and ``next`` entries, each None when absent,
    and whether its plan finished with no verdict after it.

    A parent builds nothing, so a verdict or an order on one is dropped, and a
    deferred card is one nobody should take, so a verdict on one is dropped too. Each
    drop is listed, and dropping on every build means removing ``deferred`` brings the
    verdict back. A deferred card keeps its order, because an order may sit on any
    open issue, and the page already hides a deferred card's rank. ``ready`` comes
    from the same verdict read that writes ``planned``, so the two never disagree.
    """
    n = card["n"]
    names = [lb["name"] for lb in _whole(issue["labels"], f"labels on #{n}")]
    held = {WORKING[x] for x in names if x in WORKING}
    working = None
    if held:
        if len(held) > 1:
            notes.conflicts.add((n, "carries both working labels, read as build"))
        working = {"n": n, "kind": "build" if "build" in held else "decompose"}

    verdict, unfinished = read_comments(issue, notes)
    if verdict and card["kind"] in ("parent", "deferred"):
        on = "a parent" if card["kind"] == "parent" else "a deferred card"
        notes.dropped.add((n, f"Board verdict on {on}"))
        verdict = None
    if orders and card["kind"] == "parent":
        notes.dropped.add((n, "Board order on a parent"))
        orders = []
    if len(orders) > 1:
        notes.conflicts.add((n, f"carries {len(orders)} Board order lines, read as the first"))

    planned = {"n": n, "passes": verdict[1], "ready": verdict[0]} if verdict else None
    ranked = None
    if orders:
        ranked = {
            "issue": n,
            "band": 0,
            "order": orders[0],
            "ready": verdict[0] if verdict else "plan",
        }
    missing = unfinished and "decompose" not in held and card["kind"] not in ("parent", "deferred")
    return working, planned, ranked, missing


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
    notes = Notes()
    tracker, working, planned, ranked, unfinished = [], [], [], [], []
    for issue in sorted(issues, key=lambda i: i["number"]):
        orders, after = read_body(issue, repo, notes)
        card = tracker_card(issue, open_issues, repo, after)
        tracker.append(card)
        w, p, x, missing = session_entries(issue, card, orders, notes)
        working += [w] if w else []
        planned += [p] if p else []
        ranked += [x] if x else []
        if missing:
            unfinished.append(card["n"])
    working.sort(key=lambda w: w["n"])
    planned.sort(key=lambda p: p["n"])
    ranked.sort(key=lambda x: x["issue"])

    pulls, _ = _top_pages(gql, PRS_QUERY, owner, name, "pullRequests")
    prs = [entry for pr in pulls for entry in pr_entries(pr, open_issues, repo)]
    prs.sort(key=lambda p: (p["issue"], p["pr"]))
    # A plan with an open pull request is being carried out, so a missing verdict on
    # it is no work for the owner. The check needs `prs`, which is read after issues.
    building = {p["issue"] for p in prs}

    board = {
        "schema": SCHEMA,
        "state": {
            "main": oid[:7],
            "updatedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "issues": {"open": len(issues)},
        },
        "prs": prs,
        "working": working,
        "planned": planned,
        "next": ranked,
        "tracker": tracker,
    }
    report = {
        "open_issues": len(issues),
        "open_prs": len(pulls),
        "pr_entries": len(prs),
        "conflicts": sorted(notes.conflicts),
        "unread": sorted(notes.unread),
        "refused": sorted(notes.refused),
        "dropped": sorted(notes.dropped),
        "no_verdict": [n for n in unfinished if n not in building],
    }
    return board, report


def _listed(items: list[tuple[int, str]]) -> str:
    """Summary rows' items as ``#N phrase``, or ``none``."""
    return "; ".join(f"#{n} {phrase}" for n, phrase in items) or "none"


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
        # `allow_nan=False` refuses to write a number JavaScript's JSON.parse cannot
        # read, which would freeze the page on "board.json is not JSON".
        json.dumps(board, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n",
        encoding="utf-8",
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
            f"| Markers that disagree | {_listed(report['conflicts'])} |",
            f"| Marker lines not read | {_listed(report['unread'])} |",
            f"| Marker lines refused | {_listed(report['refused'])} |",
            f"| Markers dropped | {_listed(report['dropped'])} |",
            "| Finished plans with no verdict after them | "
            f"{', '.join(f'#{n}' for n in report['no_verdict']) or 'none'} |",
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
