"""Which module-level constants in ``src/lake`` are built from ``paths.config_dir``.

Five constants name a file in the machine's config directory, and every one of them is
bound when its module is imported. That is what makes ``MARKETLAKE_CONFIG_DIR`` work at
all, and it is also what makes the list of them load-bearing in three places: the suite's
redirect checks that none of these modules was imported too early, one test asks a child
where each one resolved, and another asks the same of the pytest process.

All three used to type the list out. Nothing bound those spellings to the source, so a
sixth constant added to ``src/lake`` would have been outside every one of them with
nothing to say so. Verified before this existed: adding a sixth default to a probe module
and running all three left the suite green.

So the list is read out of the source instead. This scans with the ``ast`` module and
never imports the code it is reading, which matters because importing one of these
modules is the very thing that binds its default against whatever the environment said at
that moment.

``tests/support/enforcement.py`` is the neighbouring pattern, scanning the same tree for
the clock and calendar seams. It differs in what it is for. Those scanners forbid a call.
This one enumerates a declaration, so a new one is picked up rather than refused.

Two limits are named rather than hidden.

1. Only module-level assignments count. A default built inside a function is not a
   constant bound at import, so it is not the shape this is about.
2. A call is recognised by name: a bare ``config_dir(...)``, an aliased import of it, or
   any attribute call spelled ``....config_dir(...)``. A module that reached the function
   through some further indirection, such as looking it up in a dict, would be missed.
   Nothing in this repo does that, and ``test_the_scanner_finds_what_is_there_today``
   fails if the five known ones stop being found.
"""

from __future__ import annotations

import ast
from pathlib import Path

# The production package this reads. Spelled the way ``tests/support/enforcement.py``
# spells it, so the two scanners cannot disagree about which tree they scan.
LAKE_SRC = Path(__file__).resolve().parents[2] / "src" / "lake"

# The function whose call marks a constant as living in the config directory.
CONFIG_DIR_FUNC = "config_dir"


def _local_names_for_config_dir(tree: ast.Module) -> set[str]:
    """Every bare name this file could call ``config_dir`` by, aliases included."""
    names = {CONFIG_DIR_FUNC}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == CONFIG_DIR_FUNC and alias.asname:
                    names.add(alias.asname)
    return names


def _calls_config_dir(value: ast.expr, local_names: set[str]) -> bool:
    """Whether ``value`` calls ``config_dir`` anywhere inside it.

    The whole expression is walked rather than only its outermost call, because every
    real one is a division: ``config_dir() / TOKEN_FILE``.
    """
    for node in ast.walk(value):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in local_names:
            return True
        if isinstance(func, ast.Attribute) and func.attr == CONFIG_DIR_FUNC:
            return True
    return False


def _module_name(path: Path, root: Path, package: str) -> str:
    """The dotted import name of ``path`` inside ``package``."""
    relative = path.relative_to(root)
    parts = list(relative.parts[:-1])
    if relative.stem != "__init__":
        parts.append(relative.stem)
    return ".".join([package, *parts]) if parts else package


def defaults_built_from_config_dir(
    root: Path = LAKE_SRC, package: str = "lake"
) -> tuple[tuple[str, str], ...]:
    """Every ``(module, constant)`` under ``root`` assigned from a ``config_dir`` call.

    Sorted, so the order is the same on every machine and a diff of the list reads as a
    diff rather than as a reshuffle. A module defining two such constants contributes two
    pairs.
    """
    found: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        # Parsing every file costs about 84 ms on this tree, and this runs once per
        # process rather than once per suite, children included. A file that never
        # spells the name cannot call it under any of the three forms below, so it is
        # skipped before the parser sees it. That is the whole cost saved, because the
        # two files that do spell it are small.
        if CONFIG_DIR_FUNC not in source:
            continue
        tree = ast.parse(source, filename=str(path))
        local_names = _local_names_for_config_dir(tree)
        module = _module_name(path, root, package)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
                value: ast.expr | None = node.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                targets = [node.target]
                value = node.value
            else:
                continue
            if value is None or not targets or not _calls_config_dir(value, local_names):
                continue
            found.extend((module, target.id) for target in targets)
    return tuple(sorted(found))


def modules_building_a_default(root: Path = LAKE_SRC, package: str = "lake") -> tuple[str, ...]:
    """Just the module names, deduplicated and sorted.

    This is what the suite's redirect checks against ``sys.modules``, where the constant's
    own name does not matter and only whether the module was imported does.
    """
    return tuple(sorted({module for module, _ in defaults_built_from_config_dir(root, package)}))
