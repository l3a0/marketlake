"""Write a plan's changes as a Markdown table, with no value in it.

The infra workflow pipes ``tofu show -json <plan>`` into this script and appends what it
prints to the job's step summary. A public repository's Actions logs are readable by
anyone, and a plan carries ARNs and bucket names, so the table holds only three things
per changed resource:

1. Its address.
2. Its action, marked when the change adopts an existing resource.
3. The names of its changed top-level attributes, never their values.

An import that changes nothing shows as ``no-op (import)``. The import's id holds the
bucket's name, so it is never printed. A resource that is only created or only deleted
lists no attribute names, since every attribute changes. A replacement lists them,
because they say what forced it. Standard library only, so the workflow
runs it with the runner's own ``python3``.

Usage: ``tofu show -json plan.tfplan | python3 infra/ci/plan_summary.py [title]``
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from typing import Any, TextIO

_REPLACE = (["delete", "create"], ["create", "delete"])


def _action(actions: list[str]) -> str:
    if actions in _REPLACE:
        return "replace"
    return ", ".join(actions)


def _has_unknown(marker: Any) -> bool:
    """Whether an ``after_unknown`` entry marks any part of the value as unknown."""
    if marker is True:
        return True
    if isinstance(marker, Mapping):
        return any(_has_unknown(value) for value in marker.values())
    if isinstance(marker, list):
        return any(_has_unknown(value) for value in marker)
    return False


def changed_attributes(change: Mapping[str, Any]) -> list[str]:
    """The sorted names of the top-level attributes an update changes."""
    before = change.get("before") or {}
    after = change.get("after") or {}
    unknown = change.get("after_unknown") or {}
    names = set(before) | set(after) | set(unknown)
    return sorted(
        name
        for name in names
        if before.get(name) != after.get(name) or _has_unknown(unknown.get(name))
    )


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def rows(plan: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    """One row per resource the plan changes or imports, in the plan's order."""
    out = []
    for resource in plan.get("resource_changes") or []:
        change = resource["change"]
        actions = change["actions"]
        importing = change.get("importing") is not None
        if actions == ["no-op"] and not importing:
            continue
        action = _action(actions)
        if importing:
            action += " (import)"
        if actions in (["create"], ["delete"]):
            names = ""
        else:
            names = ", ".join(changed_attributes(change))
        out.append((resource["address"], action, names))
    return out


def summary(plan: Mapping[str, Any], title: str) -> str:
    lines = [f"### {title}", ""]
    table = rows(plan)
    if not table:
        lines.append("No changes.")
    else:
        lines.append("| Address | Action | Changed attributes |")
        lines.append("| --- | --- | --- |")
        for address, action, names in table:
            lines.append(f"| `{_cell(address)}` | {action} | {_cell(names)} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str], stdin: TextIO, stdout: TextIO) -> int:
    title = argv[1] if len(argv) > 1 else "Plan"
    stdout.write(summary(json.load(stdin), title))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv, sys.stdin, sys.stdout))
