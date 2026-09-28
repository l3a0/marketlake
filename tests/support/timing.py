"""Reading a day's timing file the way its readers must: one kind at a time.

The file holds two kinds of line. A request line names one vendor request (marketlake
#531), and a capture cycle appends one cycle line after its request lines (marketlake
#537). Both carry the same ``v``, so a reader tells them apart by ``kind``. A test that
reads a capture cycle's file as request lines would index a field a cycle line lacks, so
it reads through these instead. The close+5 fill and onboarding write no cycle line, and
their tests still read the whole file, which is what fails if either ever writes one.
"""

from __future__ import annotations

import json
from pathlib import Path

from lake.timing import CYCLE_KIND, REQUEST_KIND


def timing_lines(path: Path) -> list[dict]:
    """Every line of one timing file, of either kind, in file order. Empty when it is absent."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def request_lines(path: Path) -> list[dict]:
    """The request lines of one timing file, in file order."""
    return [line for line in timing_lines(path) if line["kind"] == REQUEST_KIND]


def cycle_lines(path: Path) -> list[dict]:
    """The cycle lines of one timing file, in file order."""
    return [line for line in timing_lines(path) if line["kind"] == CYCLE_KIND]
