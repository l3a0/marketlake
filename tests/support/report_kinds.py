"""Read a record's report lines together with the kinds its producer set (marketlake #530)."""

from __future__ import annotations


def kind_of(record, fragment: str) -> str:
    """The one kind every line containing ``fragment`` carries.

    ``zip(strict=True)`` is the point. A producer that appended a line without its kind
    leaves the two lists unequal, and that must fail here rather than pair the kinds with
    the wrong lines.
    """
    kinds = {
        kind
        for line, kind in zip(record.report, record.report_kinds, strict=True)
        if fragment in line
    }
    assert len(kinds) == 1, f"{fragment!r} matched lines of kinds {sorted(kinds)}"
    return kinds.pop()
