"""The compaction child the integration tests spawn, and how they spawn it.

Compaction seals a ticker-day by appending the manifest entry and then unlinking the
segments it merged. A crash between those two writes leaves a manifested partition
standing beside its own still-present segments, and ``lake.compact._recover`` exists to
finish that cleanup on the next run. Reaching that state needs a real process to die
partway, because the next run has to cope with whatever a dead process left on disk. A
monkeypatch inside the pytest process cannot produce that. So the test spawns this module
and kills it.

The stop is a handshake, not a sleep. This module wraps two points inside the seal and
counts what has happened. When the run reaches the requested point the module writes one
line to stdout and then blocks on a read from stdin that the parent never answers. The
parent reads that line, which is what tells it the window is open, and sends ``SIGKILL``.
So the kill lands inside the window on every run rather than on most of them.

``--stop-at`` selects where the run stops, and there are three places.

1. ``seal`` with ``--unlinks 0`` stops the moment ``append_manifest`` returns, with every
   segment still on disk.
2. ``seal`` with ``--unlinks N`` stops once ``N`` segments have been unlinked, which is
   the partial debris a kill partway through the loop leaves behind.
3. ``backup`` stops inside the backup seam instead. ``compact`` calls that seam from
   inside the lake-root lock, after the seal and the re-tune, so a parent that finds the
   lock still held while the child sits there has watched the lock span the whole run
   rather than only its front. ``--unlinks`` is not read in this mode.

Stdout carries the run's milestones in order, one line each, and a parent reads until the
one it wants.

1. ``STARTING`` goes out once every import is done and the run is about to begin.
2. ``READY`` goes out at the stop point, and the run then blocks.

``STARTING`` exists for the lock test. A parent holding the lake-root lock needs to know
the child is at the lock's door rather than still starting an interpreter, because only
then does a short window of silence say anything. The kill test reads past it.

``Milestones`` is the parent's reader for those lines, and it reads the pipe's file
descriptor rather than a buffered file object. The reason is a trap. ``select`` reports
what the descriptor holds, while ``readline`` on a buffered reader can pull a second line
into a buffer the next ``select`` cannot see. A parent waiting for that line would then
time out holding it, and a parent waiting for silence would read it as silence, which is
the one reading the lock test must never get wrong. So the buffer the waits consult has
to be the parent's own.

``spawn`` starts this module the way both tests need it started, so the argument list, the
sandbox paths, and the built environment sit here beside the checks that refuse them. A
second copy of that construction in a test file would be a second chance to get the
refusal arguments wrong. The lakes the two tests build are deliberately not shared. Those
are test data, and each test wants its own.

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
import select
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

# The exit codes for the three ways a run ends without being killed. Each is distinct so
# the parent can say which one happened. ``NOT_STOPPED`` is the one that catches a real
# regression: it means the run finished whole, so the window the parent meant to kill in
# was never open. ``NOT_KILLED`` needs the parent to close stdin without killing, which
# nothing does today, and it exists so that path cannot pass for a clean run.
REFUSED = 97
NOT_KILLED = 98
NOT_STOPPED = 99

# The line stdout carries once every import is done and the run is about to begin. A
# parent holding the lake-root lock reads it to learn the child is at the lock's door.
STARTING = "starting-compaction"

# The line stdout carries once the run reaches the stop point. The parent blocks on
# reading it and kills the process the moment it arrives.
READY = "at-stop-point"


def exit_reason(code: int | None) -> str:
    """An exit code as this module's own name for it, so a failure message reads."""
    named = {REFUSED: "REFUSED", NOT_KILLED: "NOT_KILLED", NOT_STOPPED: "NOT_STOPPED"}
    return f"{code} ({named[code]})" if code in named else str(code)


def _under(path: Path, root: Path) -> bool:
    """Whether ``path`` is ``root`` itself or sits somewhere beneath it."""
    return path == root or root in path.parents


def _refusal(sandbox: Path, forbidden: Path, paths: Sequence[Path]) -> str | None:
    """The reason to refuse this run, or ``None`` when every check passes.

    Three things are checked, and a failure of any one of them means a path in this run
    could reach something the suite must never touch.

    1. ``MARKETLAKE_CONFIG_DIR`` is set, so the override the parent relies on is present
       rather than assumed.
    2. The directory ``lake.paths.config_dir`` resolves to is inside the sandbox and is
       not ``forbidden``. Reading the resolved value rather than the raw variable is what
       proves the redirect took effect.
    3. Every path this run will write is inside the sandbox.

    The sandbox itself is checked first, because the other two checks are only ever as
    strong as it is. A caller naming ``/`` or a home directory as the sandbox would pass
    every path under it, the real lake included. Two independent floors refuse that, and
    either one alone would catch the home case.

    1. The sandbox sits under the temp directory. ``TMPDIR`` decides where that is, and
       the parent passes its own, so this floor is as good as the environment the parent
       built.
    2. The sandbox does not contain ``forbidden``. A sandbox swallowing the machine's
       real config directory is not a sandbox, whatever ``TMPDIR`` says.

    ``forbidden`` is the machine's real config directory, and it arrives as an argument
    rather than being computed from this process's ``HOME``. So the parent never has to
    put the operator's real home inside a process no guard reaches.
    """
    from lake.paths import CONFIG_DIR_ENV, config_dir

    temp_root = Path(tempfile.gettempdir()).resolve()
    if not _under(sandbox, temp_root):
        return f"the sandbox {sandbox} is not under the temp directory {temp_root}"
    if _under(forbidden.resolve(), sandbox):
        return f"the sandbox {sandbox} contains the real config directory {forbidden}"
    if not os.environ.get(CONFIG_DIR_ENV):
        return f"{CONFIG_DIR_ENV} is unset"
    resolved = config_dir().resolve()
    if resolved == forbidden.resolve():
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
    ``SIGKILL`` cannot be caught, so nothing after it runs when the parent kills as
    intended. It returns only when the parent closes stdin without killing, which the
    caller reports as its own exit code rather than letting the run carry on.
    """
    sys.stdout.write(READY + "\n")
    sys.stdout.flush()
    sys.stdin.buffer.read(1)


class _HoldingBackup:
    """A backup seam that stops the run inside itself rather than recording a sync.

    ``compact`` calls the backup from inside the lake-root lock, after the seal and the
    re-tune, so this is the last place in the run where the lock is still held. A parent
    that asks for the lock while this is blocked and is refused has watched the lock cover
    the whole run. The real fake, ``tests.support.backup.FakeBackup``, records and returns,
    and this one never returns.
    """

    def sync(self, source: Path, target: Path) -> None:
        _hold_until_killed()
        raise SystemExit(NOT_KILLED)


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
    parser.add_argument(
        "--sandbox",
        required=True,
        help="The temp root every path must sit under. Must itself be under the temp dir.",
    )
    parser.add_argument(
        "--forbidden",
        required=True,
        help="The machine's real config directory, which this run must never resolve to.",
    )
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
    parser.add_argument(
        "--stop-at",
        choices=("seal", "backup"),
        default="seal",
        help="Where to stop: inside the seal at --unlinks, or inside the backup seam.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the sweep and stop in the window. Returns a process exit code."""
    args = build_parser().parse_args(argv)
    sandbox = Path(args.sandbox).resolve()
    lake_root = Path(args.lake_root).resolve()
    backup_target = Path(args.backup_target).resolve()
    plan_path = Path(args.plan).resolve()

    reason = _refusal(sandbox, Path(args.forbidden), (lake_root, backup_target, plan_path))
    if reason is not None:
        print(f"refused: {reason}", file=sys.stderr)
        return REFUSED

    from lake.compact import compact
    from tests.support.backup import FakeBackup
    from tests.support.clock import ManualClock

    if args.stop_at == "backup":
        backup: object = _HoldingBackup()
    else:
        backup = FakeBackup()
        _install_stop(args.unlinks)
    # Every import is done and the next statement asks for the lake-root lock, so a parent
    # holding that lock can start timing its window of silence from here.
    sys.stdout.write(STARTING + "\n")
    sys.stdout.flush()
    compact(
        lake_root,
        clock=ManualClock(datetime.fromisoformat(args.now)),
        calendar=_calendar(date.fromisoformat(args.day)),
        backup=backup,  # type: ignore[arg-type]
        backup_target=backup_target,
        plan_path=plan_path,
    )
    # Reaching here means the stop point was never hit and the run finished whole, so
    # the parent never had a window to kill in.
    return NOT_STOPPED


# -- the parent side ---------------------------------------------------------


def spawn(
    *,
    sandbox: Path,
    lake_root: Path,
    day: date,
    now: datetime,
    unlinks: int = 0,
    stop_at: str = "seal",
) -> subprocess.Popen[bytes]:
    """Start this module as a child that sweeps ``lake_root`` and stops inside the seal.

    Every path the child touches is built under ``sandbox`` here, and the environment is
    built rather than inherited, the same way
    ``tests/component/test_config_dir_override.py`` builds its child's. So a
    ``MARKETLAKE_`` variable exported in the shell running the suite cannot point the
    child at the real config directory or the real lake. ``HOME`` is deliberately not
    passed. The directory the child must refuse is read here, in the guarded parent, and
    goes over as an argument, so the operator's real home never enters a process no guard
    reaches. ``TMPDIR`` goes because the child's sandbox floor asks where the temp
    directory is, and a child with none would answer ``/tmp`` while pytest hands out its
    temp directory somewhere else entirely.

    Both imports happen inside the function. Nothing here runs in the child, and importing
    either at module level would run it in the very process no guard reaches.

    ``--forbidden`` comes from ``tests.support.config_guard``, which settles the real
    config directory at its own import and says why: ``Path.home()`` reads ``$HOME``,
    which a test is free to monkeypatch, so asking later would let a test move the
    protected directory out from under the guard. Three test modules do patch ``HOME``.
    None of them calls this today, and computing the answer here rather than reading the
    frozen one would leave that as the only thing keeping a child's refusal honest.
    """
    from lake.paths import CONFIG_DIR_ENV
    from tests.support.config_guard import REAL_CONFIG_DIR

    # The repo root, three levels up from this file. The child needs it on ``PYTHONPATH``
    # to import ``tests.support``.
    repo_root = Path(__file__).resolve().parents[2]
    config_dir = sandbox / "config"
    config_dir.mkdir(exist_ok=True)
    backup_target = sandbox / "backup"
    backup_target.mkdir(exist_ok=True)
    environment = {
        "PATH": "/usr/bin:/bin",
        "TMPDIR": tempfile.gettempdir(),
        "PYTHONPATH": str(repo_root),
        CONFIG_DIR_ENV: str(config_dir),
    }
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.support.compaction_child",
            "--sandbox",
            str(sandbox),
            "--forbidden",
            REAL_CONFIG_DIR,
            "--lake-root",
            str(lake_root),
            "--backup-target",
            str(backup_target),
            "--plan",
            str(sandbox / "chain_plan.json"),
            "--day",
            day.isoformat(),
            "--now",
            now.isoformat(),
            "--unlinks",
            str(unlinks),
            "--stop-at",
            stop_at,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        cwd=sandbox,
    )


class Milestones:
    """A parent's reader for one child's milestone lines, with a bound on every wait.

    Lines are read off the pipe's file descriptor and buffered here, for the reason the
    module docstring gives: a buffered reader can hold a line that ``select`` on the
    descriptor can no longer see.

    Both methods answer in the same three ways. A line means the child announced it,
    ``b""`` means the child's stdout reached end of file, which is what a child that
    exited instead of announcing leaves behind, and ``None`` means the wait ran out.
    """

    def __init__(self, child: subprocess.Popen[bytes]) -> None:
        assert child.stdout is not None
        self._fd = child.stdout.fileno()
        self._pending = b""
        self._ended = False

    def _buffered(self) -> bytes | None:
        """The next complete line already in hand, or ``None`` when there is none."""
        line, newline, rest = self._pending.partition(b"\n")
        if not newline:
            return None
        self._pending = rest
        return line + newline

    def next_line(self, timeout: float) -> bytes | None:
        """The child's next announcement, whatever it is."""
        deadline = time.monotonic() + timeout
        while True:
            line = self._buffered()
            if line is not None:
                return line
            if self._ended:
                return b""
            remaining = max(deadline - time.monotonic(), 0.0)
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if not ready:
                return None
            chunk = os.read(self._fd, 4096)
            if not chunk:
                self._ended = True
                return b""
            self._pending += chunk

    def await_line(self, expected: str, timeout: float) -> bytes | None:
        """The ``expected`` announcement, skipping the milestones announced before it."""
        deadline = time.monotonic() + timeout
        while True:
            line = self.next_line(max(deadline - time.monotonic(), 0.0))
            if line is None or line == b"" or line.strip() == expected.encode():
                return line


if __name__ == "__main__":
    raise SystemExit(main())
