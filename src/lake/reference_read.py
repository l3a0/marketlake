"""The daemon's reads of a reference file, and the line that says when one cannot be read.

Three readers in the daemon answer a scope question from ``reference/``:
``capture._live_roster`` on every capture cycle, and the master and spans readers the gap
marker and the close+5 guard share. Each keeps the answer it has always given when the file
is not usable, which is to widen or to answer ``None``. What they used to share as well was
silence. An absent file and a file that is there and cannot be opened got the same answer
and the same nothing on stderr, so a denied read looked exactly like a fresh lake.

That happened on 2026-09-19 (marketlake #536). A reboot starts the daemon a few seconds
before the owner's login session exists, and the first read of the lake in that window came
back ``EPERM``. The one reader that said so paged. The others would have said nothing.

So :func:`read_or_none` tells the two apart by class. ``FileNotFoundError`` stays quiet,
because a fresh lake has no master and that is not a problem. Anything else the caller names
prints one line when the file first becomes unreadable, and one line when it next reads. The
second line is what the 2026-09-19 page never had: that denial cleared within seconds, and
nothing ever said so.

**The record is process-wide and keyed by path.** ``capture.run_cycle_from_config``
rebuilds everything each minute, so ``_live_roster`` has nothing that lives between cycles.
The daemon also builds its master and spans readers twice, once for the gap marker and once
for the close guard, so state held in their closures would print once per consumer. One
record here prints once per file for the whole process. It sits outside ``run_loop``, whose
one datetime is the loop's only state, beside the in-process state the watchdog and the
assertion holder already keep. It decides nothing about capture, only whether a line prints,
so a restart that forgets it costs one repeated line.

**The line never raises.** All three readers promise not to, and ``_live_roster`` is why:
``run_loop`` calls the cycle with no guard, so an exception there ends the daemon, and under
``KeepAlive`` the relaunch fails the same way on its first cycle. The daemon's plist sets
``PYTHONUNBUFFERED``, so a write to a full log volume raises at the ``print``. A line that
cannot be written is dropped.

**Each line carries its instant**, from the caller's injected clock, because without one a
reader of the log cannot tell a denial that lasted five seconds from one that lasted five days.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

# The files this process last failed to read, and has not read since. Process-wide on
# purpose, per the module docstring. ``reset`` exists for tests.
_unreadable: set[Path] = set()


def read_or_none[T](
    path: Path | str,
    read: Callable[[Path], T],
    unreadable: tuple[type[BaseException], ...],
    *,
    now: Callable[[], datetime],
) -> T | None:
    """``read(path)``, or ``None`` when the file is absent or one of ``unreadable`` is raised.

    ``unreadable`` is the caller's own list, because what counts as a file that did not come
    back differs by reader. ``FileNotFoundError`` is caught ahead of it and never reported,
    even when the list names ``OSError``. Anything outside the list propagates, as it did
    before this helper existed.

    An absent file leaves the record alone. A file that was unreadable and is then deleted
    has not been read, so no recovery line prints for it, and one prints when it next reads.
    """
    path = Path(path)
    try:
        value = read(path)
    except FileNotFoundError:
        return None
    except unreadable as exc:
        if path not in _unreadable:
            _unreadable.add(path)
            _say(f"reference: {path} could not be read at {_at(now)}: {type(exc).__name__}: {exc}")
        return None
    if path in _unreadable:
        _unreadable.discard(path)
        _say(f"reference: {path} reads again at {_at(now)}")
    return value


def reset() -> None:
    """Forget every file this process has reported. For tests, which share one process."""
    _unreadable.clear()


def _at(now: Callable[[], datetime]) -> str:
    """The instant for the line, or a placeholder when the clock itself fails.

    A clock that raises here would take the reader with it, which is the one thing this
    module must not do.
    """
    try:
        return now().isoformat()
    except Exception:  # noqa: BLE001 - the line never costs the reader its answer
        return "an unknown time"


def _say(line: str) -> None:
    """Print one line to stderr, and drop it rather than raise when it cannot be written."""
    try:
        print(line, file=sys.stderr)
    except Exception:  # noqa: BLE001 - see "The line never raises" in the module docstring
        pass


__all__ = ["read_or_none", "reset"]
