"""Integration test 4: compaction killed between its manifest append and its unlinks.

``_seal`` finishes a ticker-day in a fixed order. Every segment is read, the Parquet
partition lands and is read back once, the manifest entry is appended, and only then are
the segments unlinked. A process that dies between the append and the last unlink leaves
a manifested partition standing beside its own still-present segments. That is the one
state ``_recover`` exists to finish, and the design's compaction-failure rule says what
the next run owes it: the sealed partition survives byte-identical, the leftover segments
are cleaned or flagged, and they are never recompacted into a smaller partition.

The tier is integration because the state under test is made by a real process dying.
``tests/component/test_compaction.py`` already drives ``_recover`` from values it sets up
by hand, so it proves the recovery logic is right about the inputs it is handed. What it
cannot say is whether a killed process actually leaves those inputs behind. Only a kill
answers that, and a kill needs a second process.

The kill is ordered by a pipe handshake rather than timed by a sleep. The child reaches
the window, writes one line, and blocks on a read the parent never answers. The parent
reads that line and only then sends ``SIGKILL``. So the kill lands inside the window on
every run. ``tests/support/compaction_child.py`` holds the child and explains the
mechanism in full.

Three claims are checked at two stop points inside that window, one with every segment
still present and one with the unlink loop part way through.

1. The sealed partition after the restart is byte-identical to what the killed run wrote.
2. The leftover segments are cleaned, and the restart does not rebuild a smaller
   partition from whichever of them survived.
3. The standing invariant holds. No automatic run replaces a manifested partition with
   fewer rows than its recorded count.
"""

from __future__ import annotations

import fcntl
import os
import select
import signal
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import journal
from lake.calendar import MARKET_TZ
from lake.compact import COMPACTION_SOURCE, compact
from lake.journal import QUOTES_SCHEMA
from lake.manifest import manifest_path, read_manifest, scrub, sha256_file
from lake.paths import CONFIG_DIR_ENV, SEGMENT_GLOB, LakePaths
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.compaction_child import READY, REFUSED

DAY = date(2026, 8, 24)
PID = 4242
SURFACE = "quotes"
TICKER = "SPY"

# The three segments the killed run merges, and the rows in each. They differ so that a
# rebuild from any subset would be strictly smaller than the sealed partition, which is
# what makes the no-shrink claim checkable rather than an identity.
SEGMENT_ROWS = {"a": 2, "b": 3, "c": 4}
TOTAL_ROWS = sum(SEGMENT_ROWS.values())

# The repo root, two levels up from this file. The child needs it on ``PYTHONPATH`` to
# import ``tests.support``.
REPO_ROOT = Path(__file__).resolve().parents[2]

# How long the parent waits for the child to reach the window, and then to die. The first
# covers an interpreter start and a pyarrow import on a loaded machine. Both exist so a
# mechanism that never signals fails with a message instead of hanging the suite.
REACH_TIMEOUT = 120.0
DEATH_TIMEOUT = 30.0


# -- the lake the killed run sweeps ------------------------------------------


def _et(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=MARKET_TZ)


def _calendar() -> FakeCalendar:
    return FakeCalendar({DAY: SessionTimes(open=_et(DAY, 9, 30), close=_et(DAY, 16, 0))})


def _quotes(count: int, start: int) -> pa.Table:
    """``count`` quotes rows, each distinct, so a dropped segment shows in the bytes."""
    rows = [
        {
            "snap_ts": _et(DAY, 9, 30).isoformat(),
            "fetch_ts": _et(DAY, 9, 30).isoformat(),
            "ticker": TICKER,
            "bid": 650.0 + start + index,
            "row_kind": "data",
            "suspect": False,
            "schema_version": 1,
        }
        for index in range(count)
    ]
    arrays = [pa.array([row.get(f.name) for row in rows], type=f.type) for f in QUOTES_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=QUOTES_SCHEMA)


def _build_day(lake_root: Path) -> None:
    """Write the day's three segments the way the capture loop does."""
    start = 0
    for start_ts, count in SEGMENT_ROWS.items():
        with journal.SegmentWriter.open(lake_root, SURFACE, TICKER, DAY, start_ts, PID) as writer:
            writer.write_cycle(_quotes(count, start))
        start += count


def _segment_dir(lake_root: Path) -> Path:
    return LakePaths(lake_root).segment_dir(SURFACE, TICKER, DAY)


def _surviving_segments(lake_root: Path) -> list[str]:
    """The segment filenames still on disk, sorted. Empty once the cleanup finished."""
    directory = _segment_dir(lake_root)
    if not directory.is_dir():
        return []
    return sorted(path.name for path in directory.glob(SEGMENT_GLOB))


def _partition(lake_root: Path) -> Path:
    return LakePaths(lake_root).partition_path(SURFACE, TICKER, DAY)


def _identity(path: Path) -> tuple[int, int]:
    """A file's inode and write time, which together say whether it was replaced."""
    stat = path.stat()
    return stat.st_ino, stat.st_mtime_ns


def _compaction_entries(lake_root: Path, rel: str) -> list[dict]:
    """Every manifest line this partition has, so a second append is visible as one."""
    return [
        entry
        for entry in read_manifest(lake_root)
        if entry["partition"] == rel and entry["source"] == COMPACTION_SOURCE
    ]


# -- killing a real compaction inside the window ------------------------------


def _await_ready(child: subprocess.Popen, timeout: float) -> bytes:
    """The child's stop-point line, or a failure naming what came instead.

    ``select`` is what puts a bound on the wait. ``readline`` alone would block forever
    if the child never reached the window, which would hang the suite rather than fail
    it. The child writes the line with one flushed write, far under the pipe's atomic
    size, so a readable pipe carries the whole line.
    """
    assert child.stdout is not None
    ready, _, _ = select.select([child.stdout], [], [], timeout)
    if not ready:
        child.kill()
        pytest.fail(f"the child never reached its stop point within {timeout}s")
    return child.stdout.readline()


def _kill_mid_seal(lake_root: Path, tmp_path: Path, *, unlinks: int) -> None:
    """Run compaction in a real process and ``SIGKILL`` it inside the seal's window.

    Every path the child touches is built here under ``tmp_path``, and
    ``MARKETLAKE_CONFIG_DIR`` is pointed at a throwaway directory in the child's own
    environment. The suite's guards are monkeypatches that hold inside this process
    only, so nothing in ``tests/conftest.py`` reaches the child. The child re-checks all
    of it and refuses to run otherwise, which is the assertion below on ``REFUSED``.
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    backup_target = tmp_path / "backup"
    backup_target.mkdir(exist_ok=True)

    # The environment is built rather than inherited, the same way
    # ``tests/component/test_config_dir_override.py`` builds its child's. So a
    # ``MARKETLAKE_`` variable exported in the shell running the suite cannot point the
    # child at the real config directory or the real lake. ``HOME`` is passed because the
    # child compares the directory it resolved against the real one under this home, and
    # a child with no ``HOME`` would have nothing to compare.
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(Path.home()),
        "PYTHONPATH": str(REPO_ROOT),
        CONFIG_DIR_ENV: str(config_dir),
    }

    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.support.compaction_child",
            "--sandbox",
            str(tmp_path),
            "--lake-root",
            str(lake_root),
            "--backup-target",
            str(backup_target),
            "--plan",
            str(tmp_path / "chain_plan.json"),
            "--day",
            DAY.isoformat(),
            "--now",
            _et(DAY, 16, 30).isoformat(),
            "--unlinks",
            str(unlinks),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        cwd=tmp_path,
    )
    try:
        line = _await_ready(child, REACH_TIMEOUT)
        assert line.strip() == READY.encode(), f"unexpected line from the child: {line!r}"
        child.kill()
        child.wait(timeout=DEATH_TIMEOUT)
    finally:
        if child.poll() is None:  # pragma: no cover - only on a failed assertion above
            child.kill()
            child.wait(timeout=DEATH_TIMEOUT)
        _, stderr = child.communicate()

    # A refusal is the child's own sandbox check firing, and it means the run never
    # started. Naming it separately keeps that from reading as a failed kill.
    assert child.returncode != REFUSED, f"the child refused its paths: {stderr.decode()}"
    assert child.returncode == -signal.SIGKILL, (
        f"the child exited {child.returncode} rather than being killed: {stderr.decode()}"
    )


def _lock_is_free(lake_root: Path) -> bool:
    """Whether the lake-root lock can be taken right now, without waiting for it.

    The kernel drops a ``flock`` when the holder dies, so a killed child should strand
    nothing. This asks non-blockingly, so a lock the kill did leave behind fails the test
    instead of wedging the restart below forever.
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


def _restart(lake_root: Path, tmp_path: Path):
    """The next run over the same lake. This is the restart the design's rule names.

    It runs in the pytest process rather than in a second child. The kill is what needed
    a real process, because only a real death leaves the on-disk state under test. What
    follows the kill is an ordinary run, and running it here is what lets the assertions
    read its ``CompactionResult`` directly rather than inferring it from a printout.
    """
    return compact(
        lake_root,
        clock=ManualClock(_et(DAY, 16, 30)),
        calendar=_calendar(),
        backup=FakeBackup(),
        backup_target=tmp_path / "backup",
        plan_path=tmp_path / "chain_plan.json",
    )


# -- the two stop points inside the window ------------------------------------


@pytest.mark.parametrize(
    ("unlinks", "expected_debris"),
    [
        pytest.param(
            0, ["seg-a-4242.arrows", "seg-b-4242.arrows", "seg-c-4242.arrows"], id="at-the-append"
        ),
        pytest.param(
            1, ["seg-b-4242.arrows", "seg-c-4242.arrows"], id="part-way-through-the-unlinks"
        ),
    ],
)
def test_a_killed_compaction_recovers_byte_identical(
    lake_root: Path, tmp_path: Path, unlinks: int, expected_debris: list[str]
):
    _build_day(lake_root)
    partition = _partition(lake_root)
    rel = partition.relative_to(lake_root).as_posix()

    _kill_mid_seal(lake_root, tmp_path, unlinks=unlinks)

    # The kill landed in the window. The partition is sealed and manifested, and the
    # segments it was merged from are still there.
    assert partition.exists()
    entries = _compaction_entries(lake_root, rel)
    assert len(entries) == 1
    assert entries[0]["rows"] == TOTAL_ROWS
    assert entries[0]["sha256"] == sha256_file(partition)
    assert _surviving_segments(lake_root) == expected_debris
    # A rebuild from what survived would be smaller than what was sealed, so the claim
    # below that nothing shrank is a real check rather than an identity.
    survivors = sum(SEGMENT_ROWS[name.split("-")[1]] for name in expected_debris)
    assert survivors <= TOTAL_ROWS
    killed_bytes = partition.read_bytes()
    # The inode and the write time say whether the file was replaced, which the bytes
    # alone cannot. ``_write_partition`` renames a fresh temp file over the target, so a
    # rebuild lands on a new inode even when it happens to produce the same bytes.
    killed_identity = _identity(partition)

    # The kernel dropped the killed process's lock, so the next run is not wedged.
    assert _lock_is_free(lake_root)

    result = _restart(lake_root, tmp_path)

    # Claim 1. The sealed partition survived the restart byte for byte, on the very file
    # the killed run wrote.
    assert partition.read_bytes() == killed_bytes
    assert _identity(partition) == killed_identity

    # Claim 2. The debris is gone and nothing was rebuilt from it. A rebuild would have
    # appended a second manifest line, so the count is what catches one.
    assert _surviving_segments(lake_root) == []
    assert len(_compaction_entries(lake_root, rel)) == 1

    # Claim 3. The partition still holds every row the entry records, which is more than
    # the segments that survived the kill could have supplied. The ledger still describes
    # the bytes on disk, and both directions of the scrub agree.
    assert pq.read_table(partition).num_rows == TOTAL_ROWS
    assert sha256_file(partition) == entries[0]["sha256"]
    assert scrub(lake_root).ok

    # The run reported the ticker-day as recovered rather than sealed, so the outcome it
    # hands its caller matches what it did to the lake.
    assert result.sealed == ()
    assert len(result.verified) == 1
    recovered = result.verified[0]
    assert recovered.recovered is True
    assert recovered.partition == rel
    assert recovered.rows == TOTAL_ROWS
    assert sorted(Path(name).name for name in recovered.segments) == expected_debris

    # A third run over the recovered lake finds nothing left to do, which is the
    # idempotence the job claims.
    again = _restart(lake_root, tmp_path)
    assert again.sealed == ()
    assert again.verified == ()
    assert partition.read_bytes() == killed_bytes
    assert _identity(partition) == killed_identity
