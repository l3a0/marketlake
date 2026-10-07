"""The config-directory defaults in ``src/lake``: what resolves them, and what must not.

Four files sit in the machine's config directory, and each has a default path. Every one
of those defaults is a function resolved on each call, so the environment at the moment
of a read or a write decides where it lands. A module constant would be fixed when its
module is imported instead. On 2026-10-06 a probe that pointed ``HOME`` at a throwaway
after ``import lake`` overwrote a host's live roster through exactly such a constant, and
the token, config and chain-plan defaults were built the same way until marketlake #715
converted them.

This module reads the source to keep it that way, and it answers three questions.

1. ``bindings_at_import`` lists everything in ``src/lake`` that builds a config path
   while its module is imported. It should find nothing.
2. ``resolvers`` lists the resolvers themselves: every module-level ``def default_*``
   whose body calls ``config_dir`` and whose parameters all have defaults. The tests
   that ask a child process where each default resolved enumerate this list rather
   than a typed one, because a typed list already went stale once with nothing to say
   so.
3. ``config_dir_calls_outside_resolvers`` lists every call to ``config_dir`` that sits
   where it should not. It should find nothing too. It is what keeps the second answer
   complete: a resolver spelled any other way, or an inline ``config_dir() / X`` in a
   function body, fails here rather than dropping out of the list.

It scans with the ``ast`` module and never imports the code it reads. It scans
``src/lake`` only. ``tests/support/config_guard.REAL_CONFIG_DIR`` binds ``Path.home()``
at import on purpose, so a test that moves ``HOME`` cannot move the directory the guard
protects.

``tests/support/enforcement.py`` is the neighbouring pattern, scanning the same tree for
the clock and calendar seams.

What runs at import
-------------------

Everything at a module's top level runs when the module is imported, except two things.

1. A function body, and a lambda body, run when called. The ``def`` line itself still
   runs at import, so its decorators, default arguments and annotations are scanned.
2. The body of ``if __name__ == "__main__":`` runs only when the module is the program
   itself, and then the environment it reads is the one the process started with.

Everything else is scanned, found by walking the syntax tree rather than by listing the
statements that count. That covers an assignment, a bare expression, a class body and
its header, a decorator, the header of an ``if``, ``for``, ``while``, ``with``,
``match`` or ``try`` and every block under one, the async forms, and ``try``/``except*``.

What builds a config path
-------------------------

Within each module, a call to any of these counts:

1. ``config_dir`` and every resolver ``resolvers`` finds, called by bare name, by an
   aliased import, or as an attribute, as in ``paths.config_dir()``.
2. ``Path.home()``, spelled through ``Path``, an alias of it imported from ``pathlib``,
   or ``pathlib.Path``.
3. A plain name bound to any of these anywhere in the module, as in ``_cd = config_dir``.
4. A function defined at the module's import level whose body calls any of these. That
   covers a private wrapper, and one that takes a required ``home``.

The last two repeat until nothing new is added, so a wrapper of a wrapper counts too.

What a resolver may do
----------------------

``config_dir`` resolves the running process's own directory when it is given no
``home``, or given one built from ``Path.home()``. Such a call may sit only inside a
listed resolver. A call that passes some other ``home`` builds another account's
directory, as the control plane's renderer does, and may sit inside any module-level
function named ``default_*``. Every other call is reported.

Limits
------

Each of these would be missed. Nothing in this repo does any of them today.

1. A wrapper defined in another module. ``P = other_module._wrap()`` is not recognised,
   because wrappers are found within the module that defines them.
2. A method used as a wrapper, as in ``P = Holder().where()``.
3. Indirection that is not a plain name: ``functools.partial(config_dir)``, a dict or
   list holding the function, ``getattr(paths, "config_dir")``, or an attribute
   assignment such as ``self.cd = config_dir``.
4. A home read some other way, such as ``os.path.expanduser("~")`` or
   ``os.environ["HOME"]``.
5. A resolver is recognised by its name as well as its body. A function that resolves a
   config-directory path through another resolver, rather than through ``config_dir``,
   is not on the second list. ``control_plane.default_token_path`` is one, and it takes a
   required home anyway.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

# The production package this reads. Spelled the way ``tests/support/enforcement.py``
# spells it, so the two scanners cannot disagree about which tree they scan.
LAKE_SRC = Path(__file__).resolve().parents[2] / "src" / "lake"

# The function that resolves the config directory.
CONFIG_DIR_FUNC = "config_dir"

# The prefix every resolver's name carries.
RESOLVER_PREFIX = "default_"

# ``Path.home``, the other way a path under this user's home is built.
HOME_METHOD = "home"
PATH_CLASS = "Path"

_DEFS = (ast.FunctionDef, ast.AsyncFunctionDef)


def _module_name(path: Path, root: Path, package: str) -> str:
    """The dotted import name of ``path`` inside ``package``."""
    relative = path.relative_to(root)
    parts = list(relative.parts[:-1])
    if relative.stem != "__init__":
        parts.append(relative.stem)
    return ".".join([package, *parts]) if parts else package


def _parsed(root: Path, package: str, needles: frozenset[str]) -> Iterator[tuple[str, ast.Module]]:
    """Each ``(module, tree)`` under ``root`` whose source spells one of ``needles``.

    A full ``bindings_at_import`` scan of the real tree costs about 570 ms with every file
    parsed, against 200 ms with this filter, measured on 2026-10-06 on a loaded machine.
    A file that never spells a name cannot call it under any of the recognised forms, so
    it is skipped before the parser sees it. Sixteen of the sixty-two files spell
    ``config_dir`` or a resolver, and twenty-one spell one of those or ``home``, so most
    of the tree is never parsed.
    """
    for path in sorted(root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if not any(needle in source for needle in needles):
            continue
        yield _module_name(path, root, package), ast.parse(source, filename=str(path))


def _evaluated(node: ast.AST) -> Iterator[ast.AST]:
    """Every node evaluated when ``node`` is.

    A lambda's defaults are evaluated with it and its body is not, so only the defaults
    are walked.
    """
    stack: list[ast.AST] = [node]
    while stack:
        current = stack.pop()
        yield current
        if isinstance(current, ast.Lambda):
            stack.extend(current.args.defaults)
            stack.extend(default for default in current.args.kw_defaults if default is not None)
            continue
        stack.extend(ast.iter_child_nodes(current))


@dataclass
class _Recogniser:
    """Which calls in one module reach a recognised function.

    ``attrs`` is matched on any owner, so ``anything.config_dir()`` counts. ``names`` is
    matched as a bare name and grows with aliases and wrappers. ``path_names`` are the
    names ``Path`` is bound to, consulted only when ``home`` is set.
    """

    attrs: frozenset[str]
    names: set[str]
    home: bool
    path_names: set[str] = field(default_factory=lambda: {PATH_CLASS})

    def refers(self, node: ast.AST) -> bool:
        """Whether ``node`` names a recognised function, called or not."""
        if isinstance(node, ast.Name):
            return node.id in self.names
        if not isinstance(node, ast.Attribute):
            return False
        if node.attr in self.attrs:
            return True
        if not self.home or node.attr != HOME_METHOD:
            return False
        owner = node.value
        if isinstance(owner, ast.Name):
            return owner.id in self.path_names
        return isinstance(owner, ast.Attribute) and owner.attr == PATH_CLASS

    def calls(self, node: ast.AST) -> bool:
        return isinstance(node, ast.Call) and self.refers(node.func)

    def evaluates_a_call(self, node: ast.AST) -> bool:
        return any(self.calls(inner) for inner in _evaluated(node))


def _bound_names(target: ast.AST) -> list[str]:
    """Every plain name ``target`` binds, unpacking a tuple or list target."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for element in target.elts for name in _bound_names(element)]
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return []


def _parts(node: ast.AST) -> tuple[list[ast.AST], list[list[ast.stmt]]]:
    """``node``'s own expressions, evaluated with it, and the blocks of statements under it.

    Read generically from the node's fields, so a statement type this does not name is
    still split correctly. A function's body is neither, because it runs when called.
    An ``except`` clause and a ``match`` arm are split the same way, so their own
    expressions join the statement's and their blocks join its blocks.
    """
    expressions: list[ast.AST] = []
    blocks: list[list[ast.stmt]] = []
    for name, value in ast.iter_fields(node):
        if name == "body" and isinstance(node, _DEFS):
            continue
        if isinstance(value, list) and value and all(isinstance(v, ast.stmt) for v in value):
            blocks.append(value)
            continue
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, (ast.excepthandler, ast.match_case)):
                inner_expressions, inner_blocks = _parts(item)
                expressions.extend(inner_expressions)
                blocks.extend(inner_blocks)
            elif isinstance(item, ast.AST):
                expressions.append(item)
    return expressions, blocks


def _is_main_guard(node: ast.stmt) -> bool:
    """Whether ``node`` is ``if __name__ == "__main__":``."""
    if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
        return False
    test = node.test
    return (
        isinstance(test.left, ast.Name)
        and test.left.id == "__name__"
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value == "__main__"
    )


def _defs_at_import(body: list[ast.stmt]) -> Iterator[ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every function defined at a module's import level, outside any class."""
    for node in body:
        if isinstance(node, _DEFS):
            yield node
        elif not isinstance(node, ast.ClassDef):
            for block in _parts(node)[1]:
                yield from _defs_at_import(block)


def _recogniser(
    tree: ast.Module, attrs: frozenset[str], *, home: bool, wrappers: bool
) -> _Recogniser:
    """The recogniser for one module: its imports, its aliases, and its wrappers."""
    recogniser = _Recogniser(attrs=attrs, names=set(attrs), home=home)
    # Every assignment is collected in the same walk as the imports, so each round of the
    # fixed point below reads two short lists rather than walking the whole tree again.
    aliases: list[tuple[list[ast.expr], ast.expr]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in attrs and alias.asname:
                    recogniser.names.add(alias.asname)
                if alias.name == PATH_CLASS:
                    recogniser.path_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Assign):
            aliases.append((node.targets, node.value))
        elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and node.value is not None:
            aliases.append(([node.target], node.value))
    callees = [
        (
            node.name,
            [
                inner.func
                for statement in node.body
                for inner in ast.walk(statement)
                if isinstance(inner, ast.Call)
            ],
        )
        for node in (_defs_at_import(tree.body) if wrappers else ())
    ]
    while True:
        before = len(recogniser.names)
        for targets, value in aliases:
            if recogniser.refers(value):
                recogniser.names.update(name for t in targets for name in _bound_names(t))
        for name, funcs in callees:
            if any(recogniser.refers(func) for func in funcs):
                recogniser.names.add(name)
        if len(recogniser.names) == before:
            return recogniser


def _store_names(expressions: list[ast.AST]) -> list[str]:
    """The names ``expressions`` bind, comprehension variables aside.

    A comprehension's variable is local to it, so it binds nothing in the module.
    """
    local = {
        id(name)
        for expression in expressions
        for node in ast.walk(expression)
        if isinstance(node, ast.comprehension)
        for name in ast.walk(node.target)
    }
    names = [
        node.id
        for expression in expressions
        for node in _evaluated(expression)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) and id(node) not in local
    ]
    return list(dict.fromkeys(names))


def _label(node: ast.stmt, expressions: list[ast.AST]) -> list[str]:
    """What a statement found building a path at import is reported as."""
    if isinstance(node, _DEFS):
        return [f"{node.name}()"]
    names = _store_names(expressions)
    if names:
        return names
    if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        return ["<assignment>"]
    if isinstance(node, ast.Expr):
        return ["<expression>"]
    return [f"<{type(node).__name__.lower()}>"]


def _at_import(body: list[ast.stmt], prefix: str, recogniser: _Recogniser) -> list[str]:
    """What in ``body`` calls a recognised function while the module is imported."""
    found: list[str] = []
    for node in body:
        if _is_main_guard(node):
            assert isinstance(node, ast.If)
            found.extend(_at_import(node.orelse, prefix, recogniser))
            continue
        if isinstance(node, ast.ClassDef):
            header = [*node.decorator_list, *node.bases, *node.keywords]
            if any(recogniser.evaluates_a_call(part) for part in header):
                found.append(f"{prefix}{node.name}")
            found.extend(_at_import(node.body, f"{prefix}{node.name}.", recogniser))
            continue
        expressions, blocks = _parts(node)
        if any(recogniser.evaluates_a_call(part) for part in expressions):
            found.extend(f"{prefix}{label}" for label in _label(node, expressions))
        for block in blocks:
            found.extend(_at_import(block, prefix, recogniser))
    return found


def _all_have_defaults(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether every parameter of ``node`` can be omitted."""
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    if len(args.defaults) < len(positional):
        return False
    return all(default is not None for default in args.kw_defaults)


def _config_dir_recogniser(tree: ast.Module) -> _Recogniser:
    """A recogniser for ``config_dir`` alone, aliases included and wrappers not."""
    return _recogniser(tree, frozenset({CONFIG_DIR_FUNC}), home=False, wrappers=False)


def _listed(node: ast.stmt, recogniser: _Recogniser) -> bool:
    """Whether a module-level statement is a resolver ``resolvers`` lists."""
    if not isinstance(node, _DEFS) or not node.name.startswith(RESOLVER_PREFIX):
        return False
    if not _all_have_defaults(node):
        return False
    return any(recogniser.calls(inner) for statement in node.body for inner in ast.walk(statement))


def resolvers(root: Path = LAKE_SRC, package: str = "lake") -> tuple[tuple[str, str], ...]:
    """Every ``(module, function)`` resolver under ``root``, sorted.

    A resolver is a module-level ``def default_*`` whose body calls ``config_dir`` and
    whose parameters all have defaults, so it can be called with nothing and resolves
    the running process's own config directory.
    """
    found: list[tuple[str, str]] = []
    for module, tree in _parsed(root, package, frozenset({CONFIG_DIR_FUNC})):
        recogniser = _config_dir_recogniser(tree)
        for node in tree.body:
            if isinstance(node, _DEFS) and _listed(node, recogniser):
                found.append((module, node.name))
    return tuple(sorted(found))


def bindings_at_import(root: Path = LAKE_SRC, package: str = "lake") -> tuple[tuple[str, str], ...]:
    """Every ``(module, where)`` under ``root`` that builds a config path at import.

    ``where`` names the binding: ``NAME`` for each name the statement binds, such as an
    assignment's targets, a ``for`` target or a ``with ... as`` name, ``Class.NAME`` for
    one inside a class body, ``Class`` for a class whose header calls one, and ``func()``
    for a ``def`` whose decorator or default argument calls one. A statement that binds no
    name is named by its kind, such as ``<expression>`` or ``<match>``.
    """
    names = frozenset({CONFIG_DIR_FUNC, *(name for _, name in resolvers(root, package))})
    found: list[tuple[str, str]] = []
    for module, tree in _parsed(root, package, names | {HOME_METHOD}):
        recogniser = _recogniser(tree, names, home=True, wrappers=True)
        found.extend((module, where) for where in _at_import(tree.body, "", recogniser))
    return tuple(sorted(found))


def _home_argument(call: ast.Call) -> ast.expr | None:
    """The ``home`` a ``config_dir`` call passes, or ``None`` when it passes none."""
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg == "home":
            return keyword.value
    return None


def _resolves_own_home(call: ast.Call, home: _Recogniser) -> bool:
    """Whether a ``config_dir`` call resolves the running process's own directory.

    It does when it passes no ``home``, passes ``None``, or passes one built from
    ``Path.home()``.
    """
    argument = _home_argument(call)
    if argument is None:
        return True
    if isinstance(argument, ast.Constant) and argument.value is None:
        return True
    return home.evaluates_a_call(argument)


def config_dir_calls_outside_resolvers(
    root: Path = LAKE_SRC, package: str = "lake"
) -> tuple[tuple[str, int], ...]:
    """Every ``(module, line)`` calling ``config_dir`` where it should not.

    A call is recognised the way ``resolvers`` recognises one, never by a text match,
    which would also hit ``default_config_dir(host.home)``. A call that resolves the
    running process's own directory passes only inside a listed resolver's body. A call
    passing another ``home`` also passes inside any module-level ``def default_*``'s body.
    Nested helpers inside either body count as that body. Every other call fails, whether
    it sits at module level, in a class, or in another function.
    """
    found: list[tuple[str, int]] = []
    for module, tree in _parsed(root, package, frozenset({CONFIG_DIR_FUNC})):
        recogniser = _config_dir_recogniser(tree)
        home = _recogniser(tree, frozenset(), home=True, wrappers=False)
        own_home_allowed: set[int] = set()
        other_home_allowed: set[int] = set()
        for node in tree.body:
            if not isinstance(node, _DEFS) or not node.name.startswith(RESOLVER_PREFIX):
                continue
            # The body only. A default argument runs at import, and
            # ``bindings_at_import`` is what reports one.
            inside = {id(inner) for statement in node.body for inner in ast.walk(statement)}
            other_home_allowed |= inside
            if _listed(node, recogniser):
                own_home_allowed |= inside
        for node in ast.walk(tree):
            if not recogniser.calls(node):
                continue
            assert isinstance(node, ast.Call)
            own = _resolves_own_home(node, home)
            if id(node) in (own_home_allowed if own else other_home_allowed):
                continue
            found.append((module, node.lineno))
    return tuple(sorted(found))
