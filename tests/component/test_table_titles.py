"""Every Markdown table has a title, the rule in CLAUDE.md's Markdown hygiene.

Markdown has no caption, so a table's title is a line of its own above it,
starting with a bold Table:. markdownlint has no rule for this.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DELIMITER_ROW = re.compile(r"^\s*\|(\s*:?-+:?\s*\|)+\s*$")
TABLE_TITLE = re.compile(r"^\s*\*\*Table:\*\* \S")
FENCE = re.compile(r"^ {0,3}(`{3,}(?=[^`]*$)|~{3,})(.*)$")


def untitled_tables(text: str) -> list[int]:
    """Return the 1-based line of each table header with no title above it.

    The check reads pipe tables with outer pipes, the only form these documents
    use. A table with no outer pipes, or one in a blockquote, goes unread.
    """
    found: list[int] = []
    opened: str | None = None
    lines = text.split("\n")
    for index, line in enumerate(lines):
        fence = FENCE.match(line)
        if opened is None and fence:
            opened = fence.group(1)
            continue
        if opened is not None:
            closes = (
                fence is not None
                and fence.group(1)[0] == opened[0]
                and len(fence.group(1)) >= len(opened)
                and not fence.group(2).strip()
            )
            if closes:
                opened = None
            continue
        if index == 0 or not DELIMITER_ROW.match(line):
            continue
        above = index - 2
        while above >= 0 and not lines[above].strip():
            above -= 1
        if above < 0 or not TABLE_TITLE.match(lines[above]):
            found.append(index)
    return found


def test_a_table_with_no_title_is_flagged() -> None:
    assert untitled_tables("Prose.\n\n| A |\n| --- |\n| 1 |\n") == [3]


def test_a_table_under_a_title_is_left_alone() -> None:
    assert untitled_tables("**Table:** What A holds\n\n| A |\n| --- |\n") == []


def test_a_table_inside_a_fence_is_left_alone() -> None:
    assert untitled_tables("```\n| A |\n| --- |\n```\n") == []


def test_a_table_after_a_tilde_fence_is_read() -> None:
    assert untitled_tables("~~~\ntext\n~~~\n\n| A |\n| --- |\n") == [5]


def test_a_shorter_fence_inside_a_longer_one_does_not_close_it() -> None:
    assert untitled_tables("````\n```\n| A |\n| --- |\n````\n") == []


@pytest.mark.parametrize(
    ("text", "lines"),
    [
        ("~~~\n| A |\n| --- |\n~~~\n", []),
        ("```\n~~~\n| A |\n| --- |\n```\n", []),
        ("```\n``` x\n| A |\n| --- |\n```\n", []),
        ("    ```\n\n| A |\n| --- |\n", [3]),
        ("```a``` is prose\n\n| A |\n| --- |\n", [3]),
    ],
    ids=["tilde-fence", "other-character", "closer-with-info", "indented-four", "inline-ticks"],
)
def test_what_opens_and_closes_a_fence(text: str, lines: list[int]) -> None:
    assert untitled_tables(text) == lines


@pytest.mark.parametrize(
    ("text", "lines"),
    [
        ("**Table:** One\n\n| A |\n| --- |\n\n| B |\n| --- |\n", [6]),
        ("**Table:** One\n\nProse.\n\n| A |\n| --- |\n", [5]),
        ("**Table:** One\n| A |\n| --- |\n", []),
        ("**Table:**\n\n| A |\n| --- |\n", [3]),
        ("See **Table:** One\n\n| A |\n| --- |\n", [3]),
        ("Prose.\n\n| A | B |\n| :--- | ---: |\n", [3]),
    ],
    ids=["two-under-one", "prose-between", "touching", "empty", "prefixed", "aligned"],
)
def test_a_title_covers_only_the_table_right_below_it(text: str, lines: list[int]) -> None:
    assert untitled_tables(text) == lines


def test_every_table_in_the_repo_has_a_title() -> None:
    names = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    assert names
    findings = [
        f"{name}:{line}"
        for name in names
        for line in untitled_tables((REPO_ROOT / name).read_text(encoding="utf-8"))
    ]
    assert findings == []
