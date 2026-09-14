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

Three limits are named rather than hidden.

1. Only a name bound when the module is imported counts. That includes an assignment
   nested under a top-level ``if``, ``try``, ``with``, or loop, since those run at
   import too. It excludes a function body and a class body, because neither binds a
   module-level name, and the redirect's check would otherwise refuse imports that bind
   no default at all.
2. A call is recognised by name: a bare ``config_dir(...)``, an aliased import of it, or
   any attribute call spelled ``....config_dir(...)``. A module that reached the function
   through some further indirection, such as looking it up in a dict, would be missed.
   Nothing in this repo does that, and ``test_the_scanner_finds_what_is_there_today``
   fails if the five known ones stop being found.
3. Every name a tuple assignment binds is reported when any part of the value calls
   ``config_dir``, so ``A, B = config_dir() / X, something_else()`` names ``B`` as well.
   That over-reports rather than under-reports, which is the safe direction here, and
   nothing in this repo writes one.
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


def _binds_at_import(body: list[ast.stmt]) -> list[ast.stmt]:
    """Every assignment in ``body`` that runs when the module is imported.

    A statement nested under a top-level ``if``, ``try``, ``with``, or loop still runs at
    import, so an optional default guarded by one is still a default. A function body and
    a class body are not descended into, because neither binds a module-level name.
    """
    found: list[ast.stmt] = []
    for node in body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            found.append(node)
        elif isinstance(node, ast.If):
            found.extend(_binds_at_import(node.body))
            found.extend(_binds_at_import(node.orelse))
        elif isinstance(node, ast.Try):
            found.extend(_binds_at_import(node.body))
            for handler in node.handlers:
                found.extend(_binds_at_import(handler.body))
            found.extend(_binds_at_import(node.orelse))
            found.extend(_binds_at_import(node.finalbody))
        elif isinstance(node, (ast.With, ast.For, ast.While)):
            found.extend(_binds_at_import(node.body))
            found.extend(_binds_at_import(getattr(node, "orelse", [])))
    return found


def _bound_names(target: ast.expr) -> list[str]:
    """Every plain name ``target`` binds, unpacking a tuple or list target."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for element in target.elts for name in _bound_names(element)]
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return []


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
        # Parsing every file costs about 84 ms on this tree, against 20 ms with this
        # line, and it runs once per process rather than once per suite, children
        # included. A file that never spells the name cannot call it under any of the
        # three forms below, so it is skipped before the parser sees it. Seven of the
        # forty-one files spell it, so most of the tree is never parsed.
        if CONFIG_DIR_FUNC not in source:
            continue
        tree = ast.parse(source, filename=str(path))
        local_names = _local_names_for_config_dir(tree)
        module = _module_name(path, root, package)
        for node in _binds_at_import(tree.body):
            if isinstance(node, ast.Assign):
                names = [name for target in node.targets for name in _bound_names(target)]
                value: ast.expr | None = node.value
            else:
                assert isinstance(node, ast.AnnAssign)
                names = _bound_names(node.target)
                value = node.value
            if value is None or not names or not _calls_config_dir(value, local_names):
                continue
            found.extend((module, name) for name in names)
    return tuple(sorted(found))


def modules_building_a_default(root: Path = LAKE_SRC, package: str = "lake") -> tuple[str, ...]:
    """Just the module names, deduplicated and sorted.

    This is what the suite's redirect checks against ``sys.modules``, where the constant's
    own name does not matter and only whether the module was imported does.
    """
    return tuple(sorted({module for module, _ in defaults_built_from_config_dir(root, package)}))
