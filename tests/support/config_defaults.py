"""The config-directory defaults in ``src/lake``: what resolves them, and what must not.

Four files sit in the machine's config directory, and each has a default path. Every one
of those defaults is a function resolved on each call, so the environment at the moment
of a read or a write decides where it lands. A module constant would be fixed when its
module is imported instead. On 2026-10-06 a probe that pointed ``HOME`` at a throwaway
after ``import lake`` overwrote a host's live roster through exactly such a constant, and
the token, config and chain-plan defaults were built the same way until marketlake #715
converted them.

This module reads the source to keep it that way, and it answers three questions.

1. ``bindings_at_import`` lists everything in ``src/lake`` that calls ``config_dir`` or
   a resolver while its module is imported. That covers a module-level assignment, a
   class body, and a default argument, lambdas included. It should find nothing.
2. ``resolvers`` lists the resolvers themselves: every module-level ``def default_*``
   whose body calls ``config_dir`` and whose parameters all have defaults. The tests
   that ask a child process where each default resolved enumerate this list rather
   than a typed one, because a typed list already went stale once with nothing to say
   so.
3. ``config_dir_calls_outside_resolvers`` lists every call to ``config_dir`` that does
   not sit inside a module-level function named ``default_*``. It should find nothing
   too. It is what keeps the second answer complete: a resolver spelled any other way,
   or an inline ``config_dir() / X`` in a function body, fails here rather than
   dropping out of the list.

It scans with the ``ast`` module and never imports the code it reads. It scans
``src/lake`` only. ``tests/support/config_guard.REAL_CONFIG_DIR`` binds ``Path.home()``
at import on purpose, so a test that moves ``HOME`` cannot move the directory the guard
protects.

``tests/support/enforcement.py`` is the neighbouring pattern, scanning the same tree for
the clock and calendar seams.

Three limits are named rather than hidden.

1. A call is recognised by name: a bare call, an aliased import, or any attribute call
   spelled ``....config_dir(...)`` or ``....<resolver>(...)``. A module that reached the
   function through some further indirection, such as looking it up in a dict, would be
   missed. Nothing in this repo does that.
2. Only code that runs at import counts for the first answer. A function body runs when
   it is called, and so does a lambda's body, so neither is reported there. The third
   answer covers function bodies instead.
3. A resolver is recognised by its name as well as its body. A function that resolves a
   config-directory path through another resolver, rather than through ``config_dir``,
   is not on the second list. ``control_plane.default_token_path`` is one, and it takes a
   required home anyway.
"""

from __future__ import annotations

import ast
from pathlib import Path

# The production package this reads. Spelled the way ``tests/support/enforcement.py``
# spells it, so the two scanners cannot disagree about which tree they scan.
LAKE_SRC = Path(__file__).resolve().parents[2] / "src" / "lake"

# The function that resolves the config directory.
CONFIG_DIR_FUNC = "config_dir"

# The prefix every resolver's name carries.
RESOLVER_PREFIX = "default_"


def _module_name(path: Path, root: Path, package: str) -> str:
    """The dotted import name of ``path`` inside ``package``."""
    relative = path.relative_to(root)
    parts = list(relative.parts[:-1])
    if relative.stem != "__init__":
        parts.append(relative.stem)
    return ".".join([package, *parts]) if parts else package


def _local_names(tree: ast.Module, recognised: frozenset[str]) -> set[str]:
    """Every bare name this file could call a recognised function by, aliases included."""
    names = set(recognised)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in recognised and alias.asname:
                    names.add(alias.asname)
    return names


def _is_call_to(node: ast.AST, attrs: frozenset[str], local_names: set[str]) -> bool:
    """Whether ``node`` is a call to one of ``attrs`` by bare name, alias or attribute."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in local_names
    if isinstance(func, ast.Attribute):
        return func.attr in attrs
    return False


def _calls_at_import(value: ast.AST, attrs: frozenset[str], local_names: set[str]) -> bool:
    """Whether evaluating ``value`` calls a recognised function.

    The whole expression is walked, because every real one is a division:
    ``config_dir() / TOKEN_FILE``. A lambda's defaults are evaluated with it and its body
    is not, so only the defaults are walked.
    """
    stack: list[ast.AST] = [value]
    while stack:
        node = stack.pop()
        if _is_call_to(node, attrs, local_names):
            return True
        if isinstance(node, ast.Lambda):
            stack.extend(node.args.defaults)
            stack.extend(default for default in node.args.kw_defaults if default is not None)
            continue
        stack.extend(ast.iter_child_nodes(node))
    return False


def _bound_names(target: ast.expr) -> list[str]:
    """Every plain name ``target`` binds, unpacking a tuple or list target."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for element in target.elts for name in _bound_names(element)]
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return []


def _function_defaults(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.expr]:
    """The default values a ``def`` evaluates when it is defined."""
    return [*node.args.defaults, *(d for d in node.args.kw_defaults if d is not None)]


def _at_import(
    body: list[ast.stmt], prefix: str, attrs: frozenset[str], local_names: set[str]
) -> list[str]:
    """What in ``body`` calls a recognised function while the module is imported.

    A statement nested under a top-level ``if``, ``try``, ``with`` or loop still runs at
    import, and so does a class body and the default arguments of a ``def``. A function
    body does not, so it is never descended into.
    """
    found: list[str] = []
    for node in body:
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            if node.value is None or not _calls_at_import(node.value, attrs, local_names):
                continue
            if isinstance(node, ast.Assign):
                names = [name for target in node.targets for name in _bound_names(target)]
            else:
                names = _bound_names(node.target)
            found.extend(f"{prefix}{name}" for name in names or ["<assignment>"])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(_calls_at_import(d, attrs, local_names) for d in _function_defaults(node)):
                found.append(f"{prefix}{node.name}()")
        elif isinstance(node, ast.ClassDef):
            found.extend(_at_import(node.body, f"{prefix}{node.name}.", attrs, local_names))
        elif isinstance(node, ast.If):
            found.extend(_at_import(node.body, prefix, attrs, local_names))
            found.extend(_at_import(node.orelse, prefix, attrs, local_names))
        elif isinstance(node, ast.Try):
            found.extend(_at_import(node.body, prefix, attrs, local_names))
            for handler in node.handlers:
                found.extend(_at_import(handler.body, prefix, attrs, local_names))
            found.extend(_at_import(node.orelse, prefix, attrs, local_names))
            found.extend(_at_import(node.finalbody, prefix, attrs, local_names))
        elif isinstance(node, (ast.With, ast.For, ast.While)):
            found.extend(_at_import(node.body, prefix, attrs, local_names))
            found.extend(_at_import(getattr(node, "orelse", []), prefix, attrs, local_names))
        elif isinstance(node, ast.Expr) and _calls_at_import(node.value, attrs, local_names):
            found.append(f"{prefix}<expression>")
    return found


def _parsed(root: Path, package: str, needles: frozenset[str]):
    """Each ``(module, tree)`` under ``root`` whose source spells one of ``needles``.

    A file that never spells a name cannot call it under any of the recognised forms, so
    it is skipped before the parser sees it. Most of the tree is never parsed.
    """
    for path in sorted(root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if not any(needle in source for needle in needles):
            continue
        yield _module_name(path, root, package), ast.parse(source, filename=str(path))


def _all_have_defaults(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether every parameter of ``node`` can be omitted."""
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    if len(args.defaults) < len(positional):
        return False
    return all(default is not None for default in args.kw_defaults)


def resolvers(root: Path = LAKE_SRC, package: str = "lake") -> tuple[tuple[str, str], ...]:
    """Every ``(module, function)`` resolver under ``root``, sorted.

    A resolver is a module-level ``def default_*`` whose body calls ``config_dir`` and
    whose parameters all have defaults, so it can be called with nothing and resolves
    the running process's own config directory.
    """
    attrs = frozenset({CONFIG_DIR_FUNC})
    found: list[tuple[str, str]] = []
    for module, tree in _parsed(root, package, attrs):
        local_names = _local_names(tree, attrs)
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith(RESOLVER_PREFIX) or not _all_have_defaults(node):
                continue
            if any(_is_call_to(inner, attrs, local_names) for inner in ast.walk(node)):
                found.append((module, node.name))
    return tuple(sorted(found))


def bindings_at_import(root: Path = LAKE_SRC, package: str = "lake") -> tuple[tuple[str, str], ...]:
    """Every ``(module, where)`` under ``root`` that resolves a config path at import.

    ``where`` names the binding: ``NAME`` for a module-level assignment, ``Class.NAME``
    for a class attribute, and ``func()`` for a ``def`` whose default argument calls one.
    A call counts when it reaches ``config_dir`` or any resolver ``resolvers`` finds,
    since a module-level ``X = default_token_path()`` is fixed at import just the same.
    """
    names = frozenset({CONFIG_DIR_FUNC, *(name for _, name in resolvers(root, package))})
    found: list[tuple[str, str]] = []
    for module, tree in _parsed(root, package, names):
        local_names = _local_names(tree, names)
        found.extend((module, where) for where in _at_import(tree.body, "", names, local_names))
    return tuple(sorted(found))


def config_dir_calls_outside_resolvers(
    root: Path = LAKE_SRC, package: str = "lake"
) -> tuple[tuple[str, int], ...]:
    """Every ``(module, line)`` calling ``config_dir`` outside a module-level ``default_*``.

    A call is recognised the way ``resolvers`` recognises one, never by a text match,
    which would also hit ``default_config_dir(host.home)``. A call anywhere inside a
    module-level ``def default_*``'s body passes, nested helpers included. Every other call
    fails, whether it sits at module level, in a class, or in another function.
    """
    attrs = frozenset({CONFIG_DIR_FUNC})
    found: list[tuple[str, int]] = []
    for module, tree in _parsed(root, package, attrs):
        local_names = _local_names(tree, attrs)
        allowed: set[int] = set()
        for node in tree.body:
            is_def = isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            if is_def and node.name.startswith(RESOLVER_PREFIX):
                # The body only. A default argument runs at import, and
                # ``bindings_at_import`` is what reports one.
                for statement in node.body:
                    allowed.update(id(inner) for inner in ast.walk(statement))
        for node in ast.walk(tree):
            if id(node) in allowed or not _is_call_to(node, attrs, local_names):
                continue
            assert isinstance(node, ast.Call)
            found.append((module, node.lineno))
    return tuple(sorted(found))
