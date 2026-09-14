"""A compaction run waits for the lake-root lock, and holds it for the whole run.

Rule 1 of the close+15 job is that the whole run holds the lake-root lock, the kernel
``flock`` on ``manifest.jsonl``. Every other serialization claim rests on it. A hand-run
compaction and the scheduled one must not race, and neither must race the backup, and what
stops them is the lock rather than a schedule.

Rule 1 makes two claims, and they need separate evidence, so there are two tests.

1. A run that finds the lock held waits for it rather than proceeding, and still holds the
   lock once it is doing the work.
2. The lock it takes covers the whole run, out to the backup, rather than only the seal at
   the front of it.

Both tests ask whether the lock is held rather than only watching for silence. A run that
waited at the door and then dropped the lock before doing anything would produce exactly
the silence claim 1 watches for, so silence alone is not enough.

This file is not on the build plan's integration roster, so it claims no number from it,
the same way ``test_dashboard_http.py`` does not.

The tier is integration because the race rule 1 names is between processes. A hand-run
compaction, the scheduled daemon job, and the backup are separate processes, and ``flock``
is the kernel's arbiter between them, so the test runs the compaction in a real second
process. A thread would also work, and
``tests/component/test_schema_versions.py`` takes that route for the ledger write. What
would wedge the suite is holding the lock and calling ``compact`` in the same thread,
because ``flock`` conflicts between two descriptors even inside one process and
``lake_lock`` never asks with ``LOCK_NB``. That call would block forever with no timeout
and no message.

The first test's evidence is a pair, and neither half alone would carry it.

1. While the lock is held the child announces that it has started, and then goes quiet. It
   reaches no stop point inside the seal, writes no partition, and appends no manifest
   entry.
2. The moment the lock is released the same child proceeds and reaches its stop point. So
   the silence above was the lock, and not a child that was broken or merely slow to start.

The silence is judged over a fixed window, and a window is only as good as its margin. A
run that ignored the lock takes tens of milliseconds to reach the stop point on an idle
machine and about a second on a badly loaded one, against a window of three seconds. That
margin is comfortable rather than enormous, so the second half measures the same span on
the same machine and refuses to pass where the window has stopped being generous. Without
that check, a machine slow enough would read a lockless run as silence and pass with rule 1
deleted, which is the one way this test could fail at its job while looking healthy.

The pass direction does not depend on the margin. While the parent genuinely holds the
lock, no waiting run can proceed however long the window runs.
"""

from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import time as timing
from datetime import date, datetime, time
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal
from lake.calendar import MARKET_TZ
from lake.compact import COMPACTION_SOURCE
from lake.journal import QUOTES_SCHEMA
from lake.lock import lake_lock
from lake.manifest import manifest_path, read_manifest
from lake.paths import SEGMENT_GLOB, LakePaths
from tests.support.compaction_child import READY, REFUSED, STARTING, Milestones, exit_reason, spawn

DAY = date(2026, 8, 24)
PID = 4242
SURFACE = "quotes"
TICKER = "SPY"

# The one segment the blocked run would seal, and the rows in it. The count only has to be
# non-zero, so that a run which proceeded leaves a partition and a manifest entry behind.
SEGMENT_START = "a"
SEGMENT_ROWS = 3
SEGMENT_NAME = f"seg-{SEGMENT_START}-{PID}.arrows"

# How long the parent waits for each thing it waits for. ``START_TIMEOUT`` and
# ``REACH_TIMEOUT`` cover an interpreter start and a pyarrow import on a loaded machine.
# ``DEATH_TIMEOUT`` is generous only so a loaded machine's scheduler delay never reads as a
# failed kill. All of them exist so a mechanism that never signals fails with a message
# instead of hanging the suite.
START_TIMEOUT = 120.0
REACH_TIMEOUT = 120.0
DEATH_TIMEOUT = 30.0

# The silence a held lock has to produce, and the margin that keeps it meaningful. The
# window is short because the child has already announced that its imports are done, so
# only the run itself has to fit inside it. ``MARGIN`` is what the measured run is required
# to beat: a machine where a whole lockless run takes more than half the window is a
# machine where silence no longer proves anything, and the test says so rather than
# passing.
HELD_WINDOW = 3.0
MARGIN = 2.0


# -- the lake the blocked run would sweep -------------------------------------


def _et(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=MARKET_TZ)


def _quotes(count: int) -> pa.Table:
    rows = [
        {
            "snap_ts": _et(DAY, 9, 30).isoformat(),
            "fetch_ts": _et(DAY, 9, 30).isoformat(),
            "ticker": TICKER,
            "bid": 650.0 + index,
            "row_kind": "data",
            "suspect": False,
            "schema_version": 1,
        }
        for index in range(count)
    ]
    arrays = [pa.array([row.get(f.name) for row in rows], type=f.type) for f in QUOTES_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=QUOTES_SCHEMA)


def _build_day(lake_root: Path) -> None:
    """Write the day's one segment the way the capture loop does."""
    with journal.SegmentWriter.open(lake_root, SURFACE, TICKER, DAY, SEGMENT_START, PID) as writer:
        writer.write_cycle(_quotes(SEGMENT_ROWS))


def _segments(lake_root: Path) -> list[str]:
    """The segment filenames still on disk, sorted."""
    directory = LakePaths(lake_root).segment_dir(SURFACE, TICKER, DAY)
    if not directory.is_dir():
        return []
    return sorted(path.name for path in directory.glob(SEGMENT_GLOB))


def _compaction_entries(lake_root: Path, rel: str) -> list[dict]:
    """Every manifest line compaction has written for this partition."""
    return [
        entry
        for entry in read_manifest(lake_root)
        if entry["partition"] == rel and entry["source"] == COMPACTION_SOURCE
    ]


def _lock_is_free(lake_root: Path) -> bool:
    """Whether the lake-root lock can be taken right now, without waiting for it.

    ``LOCK_NB`` is what makes this a question rather than a wait. Asking the blocking way
    would park this process behind the child it is asking about, which is the failure mode
    this whole file is written around.
    """
    fd = os.open(manifest_path(lake_root), os.O_RDONLY | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


def _reap(child: subprocess.Popen) -> bytes:
    """Kill the child if it is still alive, wait for it, and return its stderr.

    Calling this twice is safe. The second call finds the pipes already drained and closed
    and answers with nothing more, so a ``finally`` never has to know whether the body it
    is unwinding already reaped.
    """
    if child.poll() is None:
        child.kill()
        child.wait(timeout=DEATH_TIMEOUT)
    assert child.stderr is not None
    if child.stderr.closed:
        return b""
    _, stderr = child.communicate()
    return stderr


def _require_started(child: subprocess.Popen, milestones: Milestones) -> None:
    """Wait for the child to say its run is about to begin, or fail naming what came instead."""
    line = milestones.await_line(STARTING, START_TIMEOUT)
    if line is not None and line.strip() == STARTING.encode():
        return
    if line is None:
        _reap(child)
        pytest.fail(f"the child never announced {STARTING!r} within {START_TIMEOUT}s")
    stderr = _reap(child)
    assert child.returncode != REFUSED, f"the child refused its paths: {stderr.decode()}"
    pytest.fail(
        f"the child exited {exit_reason(child.returncode)} instead of starting its run: "
        f"{stderr.decode()}"
    )


# -- 1. a run that finds the lock held waits for it ---------------------------


def test_a_compaction_run_waits_for_the_lake_root_lock(lake_root: Path, tmp_path: Path):
    _build_day(lake_root)
    partition = LakePaths(lake_root).partition_path(SURFACE, TICKER, DAY)
    rel = partition.relative_to(lake_root).as_posix()

    child = None
    try:
        # The lock is taken before the child is spawned, so the child can only ever find it
        # held. Spawning first would leave which process won up to the scheduler.
        with lake_lock(lake_root):
            child = spawn(
                sandbox=tmp_path, lake_root=lake_root, day=DAY, now=_et(DAY, 16, 30), unlinks=0
            )
            milestones = Milestones(child)
            _require_started(child, milestones)

            # Claim 1. The run is at the lock's door and it stays there. What the lake
            # looks like is captured here and judged after the lock is released. Nothing
            # inside this block may take the lake-root lock, because this process already
            # holds it and ``flock`` conflicts between two descriptors even inside one
            # process, so the ask would never return. Reading the manifest's bytes rather
            # than parsing it through ``read_manifest`` keeps that true whatever the
            # readers grow later.
            announced = milestones.next_line(HELD_WINDOW)
            manifest_while_held = manifest_path(lake_root).read_bytes()
            partition_while_held = partition.exists()
            segments_while_held = _segments(lake_root)

        if announced is not None:
            said = "exited" if announced == b"" else f"announced {announced!r}"
            pytest.fail(
                f"the child {said} while the lake-root lock was held, so the run did not "
                "wait for the lock"
            )
        assert manifest_while_held == b""
        assert partition_while_held is False
        assert segments_while_held == [SEGMENT_NAME]

        # Claim 2. The lock is free now, and the same run proceeds far enough to seal the
        # ticker-day and announce its stop point. So the silence above was the lock.
        released = timing.monotonic()
        line = milestones.await_line(READY, REACH_TIMEOUT)
        proceeded = timing.monotonic() - released
        if line is not None and line.strip() == READY.encode():
            # The run is mid-seal, between its manifest append and its unlinks, so the
            # lock has to be in its hand right now. Waiting at the door and then dropping
            # the lock before doing the work would satisfy everything above this line.
            assert not _lock_is_free(lake_root), (
                "the run waited for the lock and then did its work without holding it"
            )
        stderr = _reap(child)
        if line is None:
            pytest.fail(f"the child never announced {READY!r} within {REACH_TIMEOUT}s")
        assert line.strip() == READY.encode(), (
            f"the child exited {exit_reason(child.returncode)} rather than proceeding once "
            f"the lock was free. It said: {stderr.decode()}"
        )
        assert child.returncode == -signal.SIGKILL

        # A whole run, measured on this machine, is what the window of silence had to be
        # longer than. A machine where it is not comfortably shorter makes that silence
        # meaningless, and this says so rather than passing on it.
        assert proceeded * MARGIN < HELD_WINDOW, (
            f"a whole run took {proceeded:.2f}s on this machine against a {HELD_WINDOW}s "
            f"window of silence, so a run that ignored the lock could have gone unnoticed. "
            f"Raise HELD_WINDOW above {proceeded * MARGIN:.2f}s."
        )
    finally:
        if child is not None:
            _reap(child)

    # The run it was holding back is the real one. It sealed the ticker-day and manifested
    # it, which is what makes the silence during the held window a wait rather than a no-op.
    assert partition.exists()
    entries = _compaction_entries(lake_root, rel)
    assert len(entries) == 1
    assert entries[0]["rows"] == SEGMENT_ROWS


# -- 2. the lock covers the whole run, not just the seal ----------------------


def test_the_lake_root_lock_is_still_held_when_the_backup_runs(lake_root: Path, tmp_path: Path):
    """The backup is the last step inside the lock, so it is where the span is measured.

    ``compact`` seals, re-tunes, then backs up, and rule 1 says one lock covers all of it.
    A lock taken around the seal alone would satisfy every other test in this repo while
    leaving the backup to race a hand-run compaction, which is the torn-copy case rule 1
    names. So the child stops inside its backup seam, and the question is asked from here,
    in a second process, at the one moment that tells the two shapes apart.
    """
    _build_day(lake_root)
    partition = LakePaths(lake_root).partition_path(SURFACE, TICKER, DAY)

    assert _lock_is_free(lake_root), "the lake starts with its lock free"

    child = spawn(
        sandbox=tmp_path,
        lake_root=lake_root,
        day=DAY,
        now=_et(DAY, 16, 30),
        stop_at="backup",
    )
    try:
        milestones = Milestones(child)
        _require_started(child, milestones)
        line = milestones.await_line(READY, REACH_TIMEOUT)
        if line is None:
            pytest.fail(f"the child never reached its backup within {REACH_TIMEOUT}s")
        assert line.strip() == READY.encode(), (
            f"the child exited {exit_reason(child.poll())} rather than reaching its backup. "
            f"It said: {_reap(child).decode()}"
        )

        # The run has sealed the day and is now inside its backup. The lock it took at the
        # top of the run is still held, so this process cannot have it.
        assert partition.exists(), "the seal ran before the backup, as the job's order says"
        assert not _lock_is_free(lake_root), (
            "the run reached its backup without holding the lake-root lock, so the backup "
            "can race a hand-run compaction"
        )
    finally:
        _reap(child)

    # The kernel drops the lock when the holder dies, so the lake is left usable.
    assert _lock_is_free(lake_root)
