"""Integration test 5: a compaction run waits for the lake-root lock.

Rule 1 of the close+15 job is that the whole run holds the lake-root lock, the kernel
``flock`` on ``manifest.jsonl``. Every other serialization claim rests on it. A hand-run
compaction and the scheduled one must not race, and neither must race the backup, and what
stops them is the lock rather than a schedule.

The tier is integration because the claim needs two processes. ``flock`` is granted per
process, so a second request from inside the same process for the same file waits on the
first rather than being refused. A test that took the lock and then called ``compact`` in
the pytest process would therefore wedge the suite forever, with no timeout and no failure
message. The lock holder and the run have to be separate processes. Here the test process
holds the lock and ``tests/support/compaction_child.py`` runs the compaction.

The evidence is a pair, and neither half alone would carry it.

1. While the lock is held the child announces that it has started, and then goes quiet. It
   reaches no stop point inside the seal, writes no partition, and appends no manifest
   entry, across a window many times longer than the whole seal takes.
2. The moment the lock is released the same child proceeds and reaches its stop point
   inside the seal. So the silence above was the lock, and not a child that was broken or
   merely slow to start.

The child's ``STARTING`` line is what lets the window in claim 1 be short. Timing from the
spawn instead would mean paying for an interpreter start and a pyarrow import inside the
window, and most of the wait would be spent on a child that had not yet asked for the lock.

The pass direction cannot flake. While the parent genuinely holds the lock, no waiting run
can proceed however long the window runs. The window's length only decides how much room a
run that ignored the lock is given to give itself away, and the seal it would finish in
that case takes milliseconds.
"""

from __future__ import annotations

import select
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
from lake.manifest import read_manifest
from lake.paths import SEGMENT_GLOB, LakePaths
from tests.support.compaction_child import READY, REFUSED, STARTING, exit_reason, spawn

DAY = date(2026, 8, 24)
PID = 4242
SURFACE = "quotes"
TICKER = "SPY"

# The one segment the blocked run would seal, and the rows in it. The count only has to be
# non-zero, so that a run which proceeded leaves a partition and a manifest entry behind.
SEGMENT_START = "a"
SEGMENT_ROWS = 3

# How long the parent waits for each thing it waits for. ``START_TIMEOUT`` and
# ``REACH_TIMEOUT`` cover an interpreter start and a pyarrow import on a loaded machine.
# ``HELD_WINDOW`` is the silence the held lock has to produce, and it is short because the
# child has already announced that its imports are done. ``DEATH_TIMEOUT`` is generous only
# so a loaded machine's scheduler delay never reads as a failed kill. All four exist so a
# mechanism that never signals fails with a message instead of hanging the suite.
START_TIMEOUT = 120.0
REACH_TIMEOUT = 120.0
HELD_WINDOW = 3.0
DEATH_TIMEOUT = 30.0


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


# -- reading the child's milestones with a bound on every wait ----------------


def _await_line(child: subprocess.Popen, expected: str, timeout: float) -> bytes:
    """The child's ``expected`` milestone line, skipping the milestones announced before it.

    ``select`` is what puts a bound on the wait. ``readline`` alone would block forever if
    the child never announced, which would hang the suite rather than fail it. End of file
    comes back as empty bytes, which the caller reports with the child's own exit code.
    """
    assert child.stdout is not None
    deadline = timing.monotonic() + timeout
    while True:
        remaining = max(deadline - timing.monotonic(), 0.0)
        ready, _, _ = select.select([child.stdout], [], [], remaining)
        if not ready:
            child.kill()
            pytest.fail(f"the child never announced {expected!r} within {timeout}s")
        line = child.stdout.readline()
        if line == b"" or line.strip() == expected.encode():
            return line


def _expect_silence(child: subprocess.Popen, window: float) -> None:
    """Wait out ``window``, failing if the child announces anything or exits inside it.

    This is the negative half of the pair, and it is the assertion the whole test turns
    on. A run that ignored the lake-root lock would have sealed the ticker-day and reached
    its stop point long before the window ran out, and the line it wrote saying so is what
    lands here.
    """
    assert child.stdout is not None
    ready, _, _ = select.select([child.stdout], [], [], window)
    if not ready:
        return
    line = child.stdout.readline()
    child.kill()
    if line == b"":
        pytest.fail(
            f"the child exited {exit_reason(child.poll())} while the lake-root lock was "
            "held, so nothing here says the run waited for the lock"
        )
    pytest.fail(
        f"the child announced {line!r} while the lake-root lock was held, so the run did "
        "not wait for the lock"
    )


def _reap(child: subprocess.Popen) -> bytes:
    """Kill the child if it is still alive, wait for it, and return its stderr.

    Calling this twice is safe. The second call finds the pipes already drained and
    closed and answers with nothing more, so the ``finally`` below never has to know
    whether the body it is unwinding already reaped.
    """
    if child.poll() is None:
        child.kill()
        child.wait(timeout=DEATH_TIMEOUT)
    assert child.stderr is not None
    if child.stderr.closed:
        return b""
    _, stderr = child.communicate()
    return stderr


# -- the run that waits -------------------------------------------------------


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
                sandbox=tmp_path,
                lake_root=lake_root,
                day=DAY,
                now=_et(DAY, 16, 30),
                unlinks=0,
            )
            started = _await_line(child, STARTING, START_TIMEOUT)
            if started.strip() != STARTING.encode():
                stderr = _reap(child)
                assert child.returncode != REFUSED, (
                    f"the child refused its paths: {stderr.decode()}"
                )
                pytest.fail(
                    f"the child exited {exit_reason(child.returncode)} instead of starting "
                    f"its run: {stderr.decode()}"
                )

            # Claim 1. The run is at the lock's door and it stays there. Three independent
            # readings agree: it announced no stop point, it wrote no partition, and it
            # appended no manifest entry.
            _expect_silence(child, HELD_WINDOW)
            assert not partition.exists()
            assert _compaction_entries(lake_root, rel) == []
            assert _segments(lake_root) == [f"seg-{SEGMENT_START}-{PID}.arrows"]

        # Claim 2. The lock is free now, and the same run proceeds far enough to seal the
        # ticker-day and announce its stop point. So the silence above was the lock.
        line = _await_line(child, READY, REACH_TIMEOUT)
        stderr = _reap(child)
        assert line.strip() == READY.encode(), (
            f"the child exited {exit_reason(child.returncode)} rather than proceeding once "
            f"the lock was free. It said: {stderr.decode()}"
        )
        assert child.returncode == -signal.SIGKILL
    finally:
        if child is not None:
            _reap(child)

    # The run it was holding back is the real one. It sealed the ticker-day and manifested
    # it, which is what makes the silence during the held window a wait rather than a
    # no-op.
    assert partition.exists()
    entries = _compaction_entries(lake_root, rel)
    assert len(entries) == 1
    assert entries[0]["rows"] == SEGMENT_ROWS
