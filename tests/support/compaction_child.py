"""The compaction process integration test 4 kills between its two writes.

Compaction seals a ticker-day by appending the manifest entry and then unlinking the
segments it merged. A crash between those two writes leaves a manifested partition
standing beside its own still-present segments, and ``lake.compact._recover`` exists to
finish that cleanup on the next run. Reaching that state needs a real process to die
part way, because what the next run has to cope with is whatever the operating system
was left holding. A monkeypatch inside the pytest process cannot produce it. So the test
spawns this module and kills it.

The stop is a handshake, not a sleep. This module wraps two points inside the seal and
counts what has happened. When the run reaches the requested point the module writes one
line to stdout and then blocks on a read from stdin that the parent never answers. The
parent reads that line, which is what tells it the window is open, and sends ``SIGKILL``.
So the kill lands inside the window on every run rather than on most of them.

One argument selects the stop point, and both points sit inside the window.

1. ``--unlinks 0`` stops the moment ``append_manifest`` returns, with every segment still
   on disk.
2. ``--unlinks N`` stops once ``N`` segments have been unlinked, which is the partial
   debris a kill part way through the loop leaves behind.

Nothing here reads the machine's own configuration. Every path arrives on the command
line, and the module refuses to run unless all of them sit under the temp root the parent
names. ``load_config`` is never called, so the real ``config.yaml`` and the live Schwab
token beside it are never opened, and the real lake is unreachable because its path is
never learned. ``MARKETLAKE_CONFIG_DIR`` is checked on top of that. The suite's three
guards in ``tests/conftest.py`` are monkeypatches that hold inside the pytest process
only, so a child process is outside every one of them and has to carry its own refusal.

The backup seam is the recording fake, and no pinger is passed, so this module reaches
neither ``rsync`` nor the network even when a run is allowed to finish.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

# The exit codes for the three ways a run ends without being killed. Each is distinct, so
# the parent's failure message names which one happened rather than reporting a bare
# non-zero code.
REFUSED = 97
NOT_KILLED = 98
NOT_STOPPED = 99

# The single line stdout carries once the run reaches the stop point. The parent blocks
# on reading it and kills the process the moment it arrives.
READY = "at-stop-point"


def _under(path: Path, root: Path) -> bool:
    """Whether ``path`` is ``root`` itself or sits somewhere beneath it."""
    return path == root or root in path.parents


def _refusal(sandbox: Path, paths: Sequence[Path]) -> str | None:
    """The reason to refuse this run, or ``None`` when every check passes.

    Three things are checked, and a failure of any one of them means a path in this run
    could reach something the suite must never touch.

    1. ``MARKETLAKE_CONFIG_DIR`` is set, so the override the parent relies on is present
       rather than assumed.
    2. The directory ``lake.paths.config_dir`` resolves to is inside the sandbox and is
       not the machine's real one. Reading the resolved value rather than the raw
       variable is what proves the redirect took effect.
    3. Every path this run will write is inside the sandbox.
    """
    from lake.paths import CONFIG_DIR_ENV, CONFIG_DIR_PARTS, config_dir

    if not os.environ.get(CONFIG_DIR_ENV):
        return f"{CONFIG_DIR_ENV} is unset"
    resolved = config_dir().resolve()
    if resolved == Path.home().joinpath(*CONFIG_DIR_PARTS).resolve():
        return f"config_dir() is still the real {resolved}"
    if not _under(resolved, sandbox):
        return f"config_dir() is {resolved}, outside the sandbox {sandbox}"
    for path in paths:
        if not _under(path, sandbox):
            return f"{path} is outside the sandbox {sandbox}"
    return None


def _hold_until_killed() -> None:
    """Announce the stop point, then block until the parent kills this process.

    The read never returns while the parent holds its end of the pipe open, and
    ``SIGKILL`` cannot be caught, so nothing after it runs on the path this module is
    built for. It returns only when the parent closes stdin without killing, which the
    caller reports as its own exit code rather than letting the run carry on.
    """
    sys.stdout.write(READY + "\n")
    sys.stdout.flush()
    sys.stdin.buffer.read(1)


def _install_stop(stop_after_unlinks: int) -> None:
    """Wrap the manifest append and the segment unlink so the run stops in the window.

    ``append_manifest`` is wrapped on the ``lake.compact`` module rather than on
    ``lake.manifest``, because that is the name ``_seal`` calls. ``Path.unlink`` is
    wrapped on the class, and only a journal segment counts. The atomic partition write
    unlinks its own temp file on failure, and that file does not carry the segment
    suffix, so it never moves the count.
    """
    from lake import compact as compact_module
    from lake.paths import SEGMENT_SUFFIX

    state = {"appended": False, "unlinked": 0}
    real_append = compact_module.append_manifest
    real_unlink = Path.unlink

    def stop_if_at_point() -> None:
        if state["appended"] and state["unlinked"] == stop_after_unlinks:
            _hold_until_killed()
            raise SystemExit(NOT_KILLED)

    def append_manifest(*args: object, **kwargs: object) -> object:
        entry = real_append(*args, **kwargs)
        state["appended"] = True
        stop_if_at_point()
        return entry

    def unlink(self: Path, *args: object, **kwargs: object) -> object:
        outcome = real_unlink(self, *args, **kwargs)
        if self.name.endswith(SEGMENT_SUFFIX):
            state["unlinked"] += 1
            stop_if_at_point()
        return outcome

    compact_module.append_manifest = append_manifest  # type: ignore[assignment]
    Path.unlink = unlink  # type: ignore[assignment,method-assign]


def _calendar(day: date):
    """A calendar holding one regular session, the day this run sweeps."""
    from tests.support.calendar import FakeCalendar, SessionTimes, et

    return FakeCalendar(
        {
            day: SessionTimes(
                open=et(day.year, day.month, day.day, 9, 30),
                close=et(day.year, day.month, day.day, 16, 0),
            )
        }
    )


def build_parser() -> argparse.ArgumentParser:
    """The argument parser. Every path the run touches arrives through it."""
    parser = argparse.ArgumentParser(
        prog="python -m tests.support.compaction_child",
        description="Run compaction and stop inside the seal so the parent can kill it.",
    )
    parser.add_argument("--sandbox", required=True, help="The temp root every path must sit under.")
    parser.add_argument("--lake-root", required=True, help="The throwaway lake to compact.")
    parser.add_argument("--backup-target", required=True, help="The throwaway backup target.")
    parser.add_argument("--plan", required=True, help="The throwaway chain plan path.")
    parser.add_argument("--day", required=True, help="The session date, ISO like 2026-08-24.")
    parser.add_argument("--now", required=True, help="The manual clock's instant, ISO with a zone.")
    parser.add_argument(
        "--unlinks",
        type=int,
        required=True,
        help="Stop once this many segments have been unlinked, 0 for the manifest append.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the sweep and stop in the window. Returns a process exit code."""
    args = build_parser().parse_args(argv)
    sandbox = Path(args.sandbox).resolve()
    lake_root = Path(args.lake_root).resolve()
    backup_target = Path(args.backup_target).resolve()
    plan_path = Path(args.plan).resolve()

    reason = _refusal(sandbox, (lake_root, backup_target, plan_path))
    if reason is not None:
        print(f"refused: {reason}", file=sys.stderr)
        return REFUSED

    from lake.compact import compact
    from tests.support.backup import FakeBackup
    from tests.support.clock import ManualClock

    _install_stop(args.unlinks)
    compact(
        lake_root,
        clock=ManualClock(datetime.fromisoformat(args.now)),
        calendar=_calendar(date.fromisoformat(args.day)),
        backup=FakeBackup(),
        backup_target=backup_target,
        plan_path=plan_path,
    )
    # Reaching here means the stop point was never hit and the run finished whole, so
    # the parent never had a window to kill in.
    return NOT_STOPPED


if __name__ == "__main__":
    raise SystemExit(main())
