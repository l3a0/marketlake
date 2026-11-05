"""Every Markdown table has a title, the rule in CLAUDE.md's Markdown hygiene.

Markdown has no caption, so a table's title is a line of its own above it,
starting with a bold Table:. markdownlint has no rule for this.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DELIMITER_ROW = re.compile(r"^\s*\|(\s*:?-+:?\s*\|)+\s*$")
TABLE_TITLE = re.compile(r"^\s*\*\*Table:\*\* \S")
FENCE = re.compile(r"^\s*(```|~~~)")


def untitled_tables(text: str) -> list[int]:
    """Return the 1-based line of each table header with no title above it."""
    found: list[int] = []
    fenced = False
    lines = text.split("\n")
    for index, line in enumerate(lines):
        if FENCE.match(line):
            fenced = not fenced
            continue
        if fenced or index == 0 or not DELIMITER_ROW.match(line):
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


def test_every_table_in_the_repo_has_a_title() -> None:
    names = subprocess.run(
        ["git", "ls-files", "*.md"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    findings = [
        f"{name}:{line}"
        for name in names
        for line in untitled_tables((REPO_ROOT / name).read_text(encoding="utf-8"))
    ]
    assert findings == []
