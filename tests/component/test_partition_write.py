"""The partition writer's atomic write, over real files.

``compact._write_partition`` publishes a Parquet partition by writing a temp file beside
the target, flushing it, renaming it over the target, and flushing the directory entry.
Its docstring says what that buys. A crash mid-write leaves only the temp file, never a
torn Parquet at the partition path, a replacement is atomic or not at all, and the temp
file is cleaned up on any failure.

Nothing asserted any of that before this file. Replacing the whole body with a direct
``pq.write_table(table, partition)`` left the suite green except for one test in
``tests/unit/test_backup_exclusions.py``, and that one notices only that the temp
filename stopped appearing, not that the write stopped being atomic.

These cover nine things.

1. Replacing an existing partition lands on a new inode, so a rename published it rather
   than a write truncating the live file.
2. A write that fails partway leaves the prior partition untouched, still readable, and
   no temp file behind.
3. A first write that fails partway leaves no partition at all.
4. The temp file is flushed before the rename and the directory entry after it.
5. The rename publishes the temp file ``paths.temp_write_path`` names, pid included.
6. The temp file gets the strongest flush the platform offers.
7. The live partition is never unlinked during a replacement.
8. An interrupt mid-write takes the temp file with it, not only an ordinary exception.
9. A real seal publishes its partition through this same rename.

Every one of those nine was covered by nothing. Each was found by mutating
``src/lake/compact.py`` and watching the suite stay green, so none of them is a guess
about what might break.

The ninth is the one that keeps the other eight honest. All of them call
``_write_partition`` directly, and so does the exclusion test named above, so all of them
stay green if ``_seal`` stops calling it and writes the Parquet itself. That mutation
costs the real compaction path everything this file is about while leaving the helper
perfectly covered.

The fourth needs its own note. A rename publishes a name. It does not promise the bytes
reached the disk, so a power loss just after an unflushed rename can leave a short or
empty file where the partition should be. What that test proves is that the flushes
happen, on the right files, on the right side of the rename. It does not prove bytes
reached the platter, which nothing inside a process can observe. That limit is why these
tests record the call. The sibling writer ``reauth.write_token`` is checked the same way
in ``tests/unit/test_reauth.py``, though it flushes no directory, so the partition writer
is the stricter of the two rather than a copy of it.

The tier is component. Every one of these is read off a real file, and rule 2 of the
placement rule puts a real file here rather than in the unit suite.

The failures in the second, third, and eighth tests are patched rather than real. The
subject is the writer's own cleanup path, not Arrow's behaviour, and a real Arrow failure
cannot be steered to land after some bytes have already been written. The patch writes
bytes and then raises, which is the shape a full disk has. A failure that wrote nothing
would pass whether or not a temp file stood between, because a target nothing opened is
untouched either way.

One test elsewhere leans on the first claim. ``tests/integration/test_compaction_kill.py``
reads the partition's inode as one of its two detectors for a file the recovery never
rewrote. That detector works only because a rename always lands on a new inode. Take the
rename away and the inode check stops detecting while that test still passes, because its
other detector, the manifest line count, catches a different case.
"""

from __future__ import annotations

import fcntl
import os
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import compact, journal
from lake.calendar import MARKET_TZ
from lake.journal import CHAINS_SCHEMA, ROW_KIND_DATA
from lake.paths import CHAINS, LakePaths, temp_write_path
from tests.support.clock import ManualClock
from tests.support.lake import sample_chains_table

DAY = date(2026, 8, 24)

# The writing process id a segment carries. Any fixed number does, since the segment is
# built here rather than by a real capture loop.
PID = 4242

# What a torn write leaves at the path it was handed. These are not a Parquet file, so a
# reader finding them at the partition path fails loudly rather than reading short.
TORN = b"half a partition"


def _partition(lake_root: Path) -> Path:
    """The chains partition for one ticker-day, which is where a real seal writes."""
    return LakePaths(lake_root).chains_partition_path("SPY", DAY)


def _files(directory: Path) -> list[str]:
    """Every file name in the directory. A leftover temp shows up here by its marker."""
    return sorted(p.name for p in directory.iterdir())


def _tear_the_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the Parquet write with one that lands bytes at its path and then raises.

    Writing first is what separates a temp-and-rename from a direct write. The real
    writer puts those bytes on the temp file and unlinks it. A direct writer puts them
    at the partition path and leaves them there.
    """

    def torn(table: pa.Table, where: object, *args: object, **kwargs: object) -> None:
        Path(str(where)).write_bytes(TORN)
        raise OSError("no space left on device")

    monkeypatch.setattr(compact.pq, "write_table", torn)


@dataclass(frozen=True)
class Flush:
    """One durability flush. ``full`` says it used the platform's strongest primitive."""

    inode: int
    full: bool


@dataclass(frozen=True)
class Rename:
    """One publishing rename, with the two paths it was handed."""

    src: Path
    dst: Path


def _record_the_syscalls(monkeypatch: pytest.MonkeyPatch) -> list[Flush | Rename]:
    """Record every durability flush and every rename, in the order they happen.

    ``_durable`` calls ``fcntl(fd, F_FULLFSYNC)`` where the platform has it and
    ``os.fsync`` where it does not, so both are recorded. ``full`` keeps which one ran,
    because on macOS a plain ``fsync`` stops at the drive's own write cache and is the
    weaker of the two. ``_durable_dir`` always calls ``os.fsync``, which is what a
    directory entry takes.

    The inode says which file each flush reached. A rename keeps the inode it moves, so
    the temp file's inode is the partition's inode once the write is finished.
    """
    steps: list[Flush | Rename] = []
    real_fsync, real_fcntl, real_replace = os.fsync, fcntl.fcntl, os.replace

    def record_fsync(fd: int) -> None:
        steps.append(Flush(os.fstat(fd).st_ino, full=False))
        return real_fsync(fd)

    def record_fcntl(fd: int, cmd: int, *args: object) -> object:
        if journal.F_FULLFSYNC is not None and cmd == journal.F_FULLFSYNC:
            steps.append(Flush(os.fstat(fd).st_ino, full=True))
        return real_fcntl(fd, cmd, *args)

    def record_replace(src: object, dst: object) -> None:
        steps.append(Rename(Path(str(src)), Path(str(dst))))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", record_fsync)
    monkeypatch.setattr(fcntl, "fcntl", record_fcntl)
    monkeypatch.setattr(os, "replace", record_replace)
    return steps


# -- 1. a replacement is published by a rename -------------------------------


def test_replacing_a_partition_lands_on_a_new_inode(lake_root):
    """The property the temp-and-rename exists for, stated as something observable.

    A direct ``pq.write_table`` at the partition path truncates the live file and writes
    into it, so a reader arriving mid-write sees a short file at a path that is supposed
    to hold a whole partition. The rename instead builds the replacement elsewhere and
    swaps it in with one call, which the filesystem either did or did not do.

    The inode is what tells those two apart. Truncating keeps it. A rename brings the
    temp file's own inode along with the name, and the temp file was created before the
    old one was unlinked, so the two can never be the same number.
    """
    partition = _partition(lake_root)
    first = sample_chains_table()
    compact._write_partition(first, partition)
    before = partition.stat().st_ino

    second = pa.concat_tables([sample_chains_table(), sample_chains_table()])
    compact._write_partition(second, partition)

    assert partition.stat().st_ino != before
    assert pq.read_table(partition).num_rows == second.num_rows == 2
    # The temp file moved rather than being copied, so nothing sits beside the partition.
    assert _files(partition.parent) == [partition.name]


# -- 2. a failure leaves the prior partition alone ---------------------------


def test_a_write_that_fails_partway_leaves_the_prior_partition_untouched(lake_root, monkeypatch):
    """A refused replacement costs the day that is already sealed nothing.

    This is the half the compaction job depends on. A seal that dies partway must leave
    the manifested partition exactly as its entry describes it, because the manifest
    records a sha256 over those bytes and the integrity scrub checks it both ways. Bytes
    landing at the partition path from a failed write would break that entry, and the
    failure would surface at the next scrub rather than at the write.
    """
    partition = _partition(lake_root)
    compact._write_partition(sample_chains_table(), partition)
    before = partition.read_bytes()
    inode = partition.stat().st_ino

    _tear_the_write(monkeypatch)
    with pytest.raises(OSError):
        compact._write_partition(sample_chains_table(), partition)

    assert partition.read_bytes() == before
    assert partition.stat().st_ino == inode
    # Reading it back says the bytes are a whole Parquet file, not merely the same bytes.
    assert pq.read_table(partition).num_rows == 1
    # Nothing matching the temp marker survives, so a backup after a failed seal carries
    # no debris and the next writer finds a clean directory.
    assert _files(partition.parent) == [partition.name]


# -- 3. a failed first write leaves nothing at the path ----------------------


def test_a_first_write_that_fails_partway_leaves_no_partition_at_all(lake_root, monkeypatch):
    """With no prior file, the same failure must leave the path empty rather than torn.

    A direct write creates the partition and then fails, leaving a file that reads as a
    partition to anything walking the tree, including the compaction job's own sweep and
    the backup. The temp-and-rename never puts the name in place until the bytes are
    whole, so the ticker-day stays unsealed, which is a state the job knows how to
    resume from.
    """
    partition = _partition(lake_root)

    _tear_the_write(monkeypatch)
    with pytest.raises(OSError):
        compact._write_partition(sample_chains_table(), partition)

    assert not partition.exists()
    assert _files(partition.parent) == []


# -- 4. the flushes bracket the rename ---------------------------------------


def test_the_temp_is_flushed_before_the_rename_and_the_directory_after(lake_root, monkeypatch):
    """The durability half, which the rename alone does not buy.

    A rename publishes a name. It does not promise the bytes behind that name reached
    stable storage, so a power loss just after an unflushed rename can leave a short
    partition at a path the manifest already vouches for. The directory flush is the
    other end of it. Without it the new name itself can be lost, which puts the lake back
    at a partition the manifest describes and the tree does not hold.

    These record the order, not merely the calls. A temp flush landing after the rename
    would reach a file the temp path no longer names, and a directory flush landing only
    before the rename would miss the very entry the rename creates.

    What is asserted is that each flush falls on the right side of the rename, not that
    exactly three calls happen. An extra flush is allowed, because an extra flush is
    strictly more durable rather than less. A writer that also made the temp file's own
    directory entry durable before publishing would be one, and that is what the
    journal's segment writer already does right after it creates a segment.
    """
    partition = _partition(lake_root)
    steps = _record_the_syscalls(monkeypatch)

    compact._write_partition(sample_chains_table(), partition)

    renames = [i for i, step in enumerate(steps) if isinstance(step, Rename)]
    assert len(renames) == 1
    before = [step.inode for step in steps[: renames[0]] if isinstance(step, Flush)]
    after = [step.inode for step in steps[renames[0] + 1 :] if isinstance(step, Flush)]

    # A rename carries the moved file's inode along with the name, so the partition's
    # inode now is the temp file's inode then. Finding it among the flushes before the
    # rename says the bytes that got published are the bytes that were flushed.
    assert partition.stat().st_ino in before
    # And the directory the name landed in is made durable after it lands there.
    assert partition.parent.stat().st_ino in after


# -- 5. the rename's two operands --------------------------------------------


def test_the_rename_publishes_the_shared_helpers_temp_file(lake_root, monkeypatch):
    """The temp name comes from ``paths.temp_write_path``, pid and all.

    That function owns the one marker the backup exclusion matches, so a temp named any
    other way rides into a backup. The pid is the other half of it. Its docstring gives
    the reason: two writers never share a temp file, and a leftover names the process
    that died holding it. Dropping the pid keeps the marker, so the exclusion test stays
    green while both of those properties are gone.

    The sibling writer ``reauth.write_token`` is already checked this way, in
    ``tests/unit/test_reauth.py::test_the_write_publishes_by_renaming_the_paths_temp_file``.
    """
    partition = _partition(lake_root)
    steps = _record_the_syscalls(monkeypatch)

    compact._write_partition(sample_chains_table(), partition)

    renames = [step for step in steps if isinstance(step, Rename)]
    assert renames == [Rename(temp_write_path(partition, os.getpid()), partition)]


# -- 6. the strongest flush the platform offers ------------------------------


def test_the_temp_gets_the_full_flush_where_the_platform_has_one(lake_root, monkeypatch):
    """Plain ``fsync`` is the weaker primitive on macOS, and the design says so.

    ``journal.py`` states it: on macOS ``fsync`` stops at the drive's own write cache,
    so real durability takes ``fcntl(fd, F_FULLFSYNC)``. The daemon runs on a macOS
    laptop today. Swapping ``_durable``'s body for a plain ``os.fsync`` still flushes
    something, so the order test above cannot tell the difference, and the weakening
    would be invisible without this.

    ``tests/component/test_segment_writer.py::test_uses_f_fullfsync_on_this_platform``
    checks the constant resolves correctly. This checks the partition writer reaches for
    it. Where the platform lacks it, the fallback to ``os.fsync`` is the real equivalent
    rather than a weaker stand-in, so that is what this expects there.
    """
    partition = _partition(lake_root)
    steps = _record_the_syscalls(monkeypatch)

    compact._write_partition(sample_chains_table(), partition)

    inode = partition.stat().st_ino
    flushes = [step for step in steps if isinstance(step, Flush) and step.inode == inode]
    assert flushes
    assert all(step.full for step in flushes) is (journal.F_FULLFSYNC is not None)


# -- 7. the replacement never leaves the path empty --------------------------


def test_the_live_partition_is_never_unlinked_during_a_replacement(lake_root, monkeypatch):
    """Atomic or not at all, which an unlink before the rename quietly gives up.

    Unlinking the target first and then renaming over the empty name still lands a new
    inode, so the inode test above cannot tell the two apart. What it costs is the
    window: for the moment between the two calls the partition path holds no file, and a
    reader or the backup arriving inside it sees a ticker-day that vanished. The rename
    on its own never opens that window, because it replaces the name in one step.
    """
    partition = _partition(lake_root)
    compact._write_partition(sample_chains_table(), partition)

    unlinked: list[Path] = []
    real_unlink = os.unlink
    real_path_unlink = Path.unlink

    def record_unlink(path, *args, **kwargs):
        unlinked.append(Path(str(path)))
        return real_unlink(path, *args, **kwargs)

    def record_path_unlink(self, *args, **kwargs):
        unlinked.append(Path(str(self)))
        return real_path_unlink(self, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", record_unlink)
    monkeypatch.setattr(Path, "unlink", record_path_unlink)
    compact._write_partition(pa.concat_tables([sample_chains_table()] * 2), partition)

    assert partition not in unlinked
    assert pq.read_table(partition).num_rows == 2


# -- 8. cleanup on a failure that is not an Exception ------------------------


def test_an_interrupt_mid_write_still_takes_the_temp_file_with_it(lake_root, monkeypatch):
    """``except BaseException`` is deliberate, and narrowing it is invisible otherwise.

    ``KeyboardInterrupt`` and ``SystemExit`` do not inherit from ``Exception``. A writer
    catching only ``Exception`` leaves a part-written Parquet beside the partition when
    the job is interrupted, and that file then rides into the next backup. The kill test
    in ``tests/integration/test_compaction_kill.py`` cannot reach this, because
    ``SIGKILL`` is uncatchable and never runs the clause at all.

    The sibling guard is checked the same way in
    ``tests/unit/test_config_dir_guard.py::test_the_refusal_is_not_caught_by_a_bare_except_exception``.
    """
    partition = _partition(lake_root)

    def interrupted(table: pa.Table, where: object, *args: object, **kwargs: object) -> None:
        Path(str(where)).write_bytes(TORN)
        raise KeyboardInterrupt

    monkeypatch.setattr(compact.pq, "write_table", interrupted)
    with pytest.raises(KeyboardInterrupt):
        compact._write_partition(sample_chains_table(), partition)

    assert not partition.exists()
    assert _files(partition.parent) == []


# -- 9. the seal publishes through this writer -------------------------------


def _one_segment(lake_root: Path) -> Path:
    """One closed chains segment for the ticker-day, the way the capture loop writes it."""
    rows = [
        {"snap_ts": f"2026-08-24T09:3{index}:00-04:00", "ticker": "SPY", "row_kind": ROW_KIND_DATA}
        for index in range(2)
    ]
    arrays = [pa.array([row.get(f.name) for row in rows], type=f.type) for f in CHAINS_SCHEMA]
    table = pa.Table.from_arrays(arrays, schema=CHAINS_SCHEMA)
    start = "20260824T093000000000"
    with journal.SegmentWriter.open(lake_root, CHAINS, "SPY", DAY, start, PID) as writer:
        writer.write_cycle(table)
    return journal.segment_path(lake_root, CHAINS, "SPY", DAY, start, PID)


def test_the_seal_publishes_its_partition_through_the_same_rename(lake_root, monkeypatch):
    """The caller is bound to the writer, which binding the writer to itself does not do.

    Every test above calls ``_write_partition`` directly, and so does the one exclusion
    test in ``tests/unit/test_backup_exclusions.py``. All of them stay green if ``_seal``
    stops calling it and writes the Parquet itself. That mutation costs the real
    compaction path its atomicity, both flushes, and the temp cleanup, and it leaves
    ``_write_partition`` as dead code that the tests keep exercising.

    Recording the rename during a real seal is what closes that. The seal is called
    directly rather than through the whole close+15 job, which keeps this at the same
    tier as the rest of the file and out of the job's own suite.
    """
    segment = _one_segment(lake_root)
    paths = LakePaths(lake_root)
    partition = paths.chains_partition_path("SPY", DAY)
    clock = ManualClock(datetime.combine(DAY, time(16, 30), tzinfo=MARKET_TZ))
    steps = _record_the_syscalls(monkeypatch)

    sealed = compact._seal(
        lake_root, paths, CHAINS, "SPY", DAY, [segment], clock=clock, guard=False
    )

    assert sealed.rows == 2
    renames = [step for step in steps if isinstance(step, Rename)]
    assert Rename(temp_write_path(partition, os.getpid()), partition) in renames
