"""What counts as the machine's real config directory, and whether a path is inside it.

This is the half of the config-directory guard that answers a question. The other half,
the autouse fixture that patches thirteen names and raises, lives in ``tests/conftest.py``
with the two guards it sits beside. The split is not tidiness. It is what lets a child
process ask this question without importing ``tests.conftest``.

That matters because importing ``tests.conftest`` has a side effect. It exports
``MARKETLAKE_CONFIG_DIR`` at a throwaway directory, which is how the suite keeps its
children off the real files. A test that spawns a child to prove the guard cannot be
disarmed by that variable has to set the variable itself and then watch what the guard
does with it. While this code lived in ``tests/conftest.py``, importing it replaced the
test's own value before the guard read anything, so the test's input never reached the
code it was aiming at and the same answer came back whatever the test passed.

Nothing here has an import side effect of its own, and ``tests/support/__init__.py``
imports no submodule, so ``from tests.support.config_guard import ...`` runs this file and
nothing else.

What counts as the real directory is settled here, at import, and deliberately not
through ``paths.config_dir``. Two reasons, and they pull the same way. ``config_dir``
honours ``MARKETLAKE_CONFIG_DIR``, so reading it would let the override disarm the guard,
when the override's whole purpose is to keep a process off this path. And ``Path.home()``
reads ``$HOME``, which a test is free to monkeypatch, so asking later would let a test
move the protected directory out from under the guard.

Both spellings are protected. A home whose ``.config`` is a symlink, which is what a
dotfile manager usually leaves behind, has two names for one directory, and a write
through the resolved one is the same write.
"""

from __future__ import annotations

import os
from pathlib import Path

from lake.paths import CONFIG_DIR_PARTS


def protected_roots(directory: str | Path) -> tuple[str, ...]:
    """Both spellings of ``directory``: as given, and fully resolved.

    It takes a directory rather than reading one so a test can drive it, since the two
    spellings collapse to one string on a machine whose ``.config`` is a real directory.
    That is every machine the suite has run on, so nothing would otherwise exercise the
    resolved one.
    """
    as_given = str(directory)
    return tuple({as_given, os.path.realpath(as_given)})


REAL_CONFIG_DIR = str(Path.home().joinpath(*CONFIG_DIR_PARTS))
PROTECTED_ROOTS = protected_roots(REAL_CONFIG_DIR)


def is_protected(target: object) -> bool:
    """Whether ``target`` names the real config directory or something inside it.

    An ``int`` is an already-open file descriptor, which carries no path to check, so it
    passes through. ``os.fsdecode`` accepts ``str``, ``bytes``, and anything with
    ``__fspath__``, which is every form these calls take, and raises ``TypeError`` on
    anything else.

    The comparison is on the path's text, expanded and made absolute, and touches no
    filesystem. It therefore catches a path spelled at the directory, under either of
    the two names in ``PROTECTED_ROOTS``, and it does not catch a path that arrives
    there through a symlink of its own: a link outside the directory pointing at a file
    inside it, or an ancestor that is a link the roots do not already name. Opening such
    a path for writing truncates the real file and this returns ``False``.

    Resolving every candidate with ``os.path.realpath`` would close that, and the price
    is the reason it does not. Measured on this machine, ``realpath`` costs 25.8 µs
    against ``abspath``'s 0.36 µs, and every ``open`` in the suite runs this, which is
    seconds per run to catch a shape nothing in this repo builds. Revisit that trade if
    anything here ever does build one.

    ``PROTECTED_ROOTS`` is read at call time rather than captured, so a test can point
    the guard at a stand-in directory for the length of one call.
    """
    if isinstance(target, int):
        return False
    try:
        text = os.fsdecode(target)
    except TypeError:
        return False
    absolute = os.path.abspath(os.path.expanduser(text))
    return any(absolute == root or absolute.startswith(root + os.sep) for root in PROTECTED_ROOTS)
