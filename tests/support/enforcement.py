"""The two enforcement scanners.

These back the two tests that stay in continuous integration for the life of the
project. They keep the clock seam and the calendar seam from being bypassed.

1. The clock scanner fails the build on any direct wall-clock call anywhere under
   ``src/lake`` outside the clock module. A direct wall-clock call is a read of the
   operating system's clock or timer, or a real sleep: ``datetime.now`` and friends,
   ``date.today``, ``time.time`` / ``time.monotonic`` / ``time.sleep`` and their kin,
   and ``pandas.Timestamp.now`` / ``today``, including its ``Timestamp("now")`` string
   form.
2. The session-time scanner fails the build on any hardcoded session time anywhere
   under ``src/lake`` outside the calendar module. A hardcoded session time is a
   bare ``"HH:MM"`` string, a ``datetime.time(...)`` construction, or a
   ``.replace(hour=..., minute=...)`` that builds a time of day from literals.

Both scanners read the source with the ``ast`` module and resolve names through the
file's own imports, so aliased imports are caught and false positives stay rare. They
scan production code only. Tests are where injection happens by design, so a test is
free to name any time it likes through the fakes.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

# The production package this guards, and the one carved-out file per scanner.
LAKE_SRC = Path(__file__).resolve().parents[2] / "src" / "lake"
CLOCK_MODULE = "clock.py"
CALENDAR_MODULE = "calendar.py"

# Canonical tokens a name can resolve to.
TIME_MODULE = "time"
DT_MODULE = "datetime.module"
DT_DATETIME = "datetime.datetime"
DT_DATE = "datetime.date"
DT_TIME = "datetime.time"
PD_MODULE = "pandas.module"
PD_TIMESTAMP = "pandas.Timestamp"

# The wall-clock reads and the sleep the clock scanner forbids.
_TIME_FUNCS = frozenset(
    {
        "time",
        "time_ns",
        "monotonic",
        "monotonic_ns",
        "perf_counter",
        "perf_counter_ns",
        "process_time",
        "process_time_ns",
        "thread_time",
        "thread_time_ns",
        "localtime",
        "gmtime",
        "sleep",
        "clock_gettime",
        "clock_gettime_ns",
    }
)
_DATETIME_READS = frozenset({"now", "utcnow", "today"})
_DATE_READS = frozenset({"today"})
_TIMESTAMP_READS = frozenset({"now", "utcnow", "today"})

# A bare time-of-day string, whole-string only, so ISO datetimes do not match.
_TIME_LITERAL = re.compile(r"([01]?\d|2[0-3]):[0-5]\d(:[0-5]\d)?")
# Only ``hour`` and ``minute`` build a time of day. Zeroing ``second`` and
# ``microsecond`` is flooring to the minute, the loop's own way of assigning a snap
# slot, so it is allowed.
_REPLACE_TIME_FIELDS = frozenset({"hour", "minute"})
# The pandas string constructor also reads the clock: ``Timestamp("now")``.
_TIMESTAMP_STRING_READS = frozenset({"now", "today", "utcnow"})


@dataclass(frozen=True)
class Violation:
    """One offending call or literal."""

    path: str
    lineno: int
    col: int
    detail: str

    def __str__(self) -> str:
        return f"{self.path}:{self.lineno}:{self.col}: {self.detail}"


def _build_imports(tree: ast.AST) -> dict[str, object]:
    """Map each locally bound name to the canonical token it refers to."""
    imports: dict[str, object] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                token = {
                    "time": TIME_MODULE,
                    "datetime": DT_MODULE,
                    "pandas": PD_MODULE,
                }.get(alias.name)
                if token is not None:
                    imports[alias.asname or alias.name] = token
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                local = alias.asname or alias.name
                if node.module == "datetime":
                    token = {
                        "datetime": DT_DATETIME,
                        "date": DT_DATE,
                        "time": DT_TIME,
                    }.get(alias.name)
                    if token is not None:
                        imports[local] = token
                elif node.module == "time" and alias.name in _TIME_FUNCS:
                    imports[local] = ("time_func", alias.name)
                elif node.module == "pandas" and alias.name == "Timestamp":
                    imports[local] = PD_TIMESTAMP
    return imports


def _resolve(node: ast.expr, imports: dict[str, object]) -> object:
    """Resolve an expression to a canonical token, or ``None``."""
    if isinstance(node, ast.Name):
        return imports.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _resolve(node.value, imports)
        if base == DT_MODULE:
            return {"datetime": DT_DATETIME, "date": DT_DATE, "time": DT_TIME}.get(node.attr)
        if base == PD_MODULE and node.attr == "Timestamp":
            return PD_TIMESTAMP
    return None


class _ClockVisitor(ast.NodeVisitor):
    def __init__(self, path: str, imports: dict[str, object]) -> None:
        self.path = path
        self.imports = imports
        self.violations: list[Violation] = []

    def _flag(self, node: ast.AST, detail: str) -> None:
        self.violations.append(Violation(self.path, node.lineno, node.col_offset, detail))

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name):
            token = self.imports.get(func.id)
            if isinstance(token, tuple) and token[0] == "time_func":
                self._flag(node, f"direct wall-clock call: time.{token[1]}()")
        elif isinstance(func, ast.Attribute):
            base = _resolve(func.value, self.imports)
            attr = func.attr
            if base == TIME_MODULE and attr in _TIME_FUNCS:
                self._flag(node, f"direct wall-clock call: time.{attr}()")
            elif base == DT_DATETIME and attr in _DATETIME_READS:
                self._flag(node, f"direct wall-clock call: datetime.{attr}()")
            elif base == DT_DATE and attr in _DATE_READS:
                self._flag(node, f"direct wall-clock call: date.{attr}()")
            elif base == PD_TIMESTAMP and attr in _TIMESTAMP_READS:
                self._flag(node, f"direct wall-clock call: Timestamp.{attr}()")
        # The string constructor Timestamp("now") is a clock read too. It resolves to
        # the Timestamp class being called directly with a "now"-like string literal.
        if _resolve(func, self.imports) == PD_TIMESTAMP and node.args:
            first = node.args[0]
            if (
                isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and first.value.lower() in _TIMESTAMP_STRING_READS
            ):
                self._flag(node, f"direct wall-clock call: Timestamp({first.value!r})")
        self.generic_visit(node)


class _SessionTimeVisitor(ast.NodeVisitor):
    def __init__(self, path: str, imports: dict[str, object]) -> None:
        self.path = path
        self.imports = imports
        self.violations: list[Violation] = []

    def _flag(self, node: ast.AST, detail: str) -> None:
        self.violations.append(Violation(self.path, node.lineno, node.col_offset, detail))

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and _TIME_LITERAL.fullmatch(node.value):
            self._flag(node, f"hardcoded session time: {node.value!r}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if _resolve(func, self.imports) == DT_TIME:
            self._flag(node, "hardcoded session time: datetime.time(...) construction")
        elif isinstance(func, ast.Attribute) and func.attr == "replace":
            for kw in node.keywords:
                if kw.arg in _REPLACE_TIME_FIELDS and isinstance(kw.value, ast.Constant):
                    self._flag(
                        node,
                        f"hardcoded session time: .replace({kw.arg}=...) builds a time",
                    )
                    break
        self.generic_visit(node)


def scan_source(source: str, kind: str, filename: str = "<source>") -> list[Violation]:
    """Scan one source string. ``kind`` is ``"clock"`` or ``"session_time"``."""
    tree = ast.parse(source, filename=filename)
    imports = _build_imports(tree)
    if kind == "clock":
        visitor: ast.NodeVisitor = _ClockVisitor(filename, imports)
    elif kind == "session_time":
        visitor = _SessionTimeVisitor(filename, imports)
    else:
        raise ValueError(f"unknown scan kind {kind!r}")
    visitor.visit(tree)
    return visitor.violations  # type: ignore[attr-defined]


def _scan_tree(root: Path, kind: str, allow: set[str]) -> list[Violation]:
    violations: list[Violation] = []
    for path in sorted(root.rglob("*.py")):
        if path.name in allow:
            continue
        rel = path.relative_to(root.parent).as_posix()
        violations.extend(scan_source(path.read_text(), kind, rel))
    return violations


def find_clock_violations(root: Path = LAKE_SRC, allow: set[str] | None = None) -> list[Violation]:
    """Every direct wall-clock call under ``root`` outside the clock module."""
    return _scan_tree(root, "clock", allow if allow is not None else {CLOCK_MODULE})


def find_session_time_violations(
    root: Path = LAKE_SRC, allow: set[str] | None = None
) -> list[Violation]:
    """Every hardcoded session time under ``root`` outside the calendar module."""
    return _scan_tree(root, "session_time", allow if allow is not None else {CALENDAR_MODULE})
