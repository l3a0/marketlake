"""A segment whose bytes were damaged after it was written, read by the journal's readers.

One flipped bit in a segment used to reach its readers four ways the readers could not
contain (marketlake #552). pyarrow raised ``ArrowNotImplementedError`` or ``SystemError``,
which no reader catches. It crashed the process outright on a column read. Or it raised
nothing, because a batch that failed to decode read as a torn tail and the segment read
as zero rows, which startup gap-marking then marked as a missing minute.

The reader now validates every batch in full and reads a stop inside a file that still
ends in its end-of-stream marker as damage. Both raise ``journal.SegmentDamaged``, an
``ArrowInvalid``, so every reader that tells a bad file from an absent one catches it as
it already did. The writer removes a file whose write raised before any batch was durable,
and leaves the marker off one whose earlier batch was, which keeps that file reading as the
torn tail it is.

These cover that contract:

1. A flip that fails validation raises, and ``recorded_slots`` files the segment corrupt.
2. A flip that used to read as zero rows raises, and a truncated copy still reads as a
   torn tail.
3. Whatever the stream raises is folded: into ``SegmentDamaged`` on a finished file and
   into ``ArrowInvalid`` on a torn one, and a missing file is still missing.
4. A flip that crashed the unvalidated read is caught, read in a forked child.
5. ``latest_expirations`` walks past a damaged segment to an older batch.
6. A write that raised before any batch was durable removes the file, whether or not the
   caller caught it, and one that raised after a durable batch leaves no marker. An
   interrupt keeps the file, and a failure while the writer is built leaves nothing.
7. A real short write, made in a forked child under a file-size limit, raises rather than
   reading as a durable batch (marketlake #769).

Every flipped fixture is built from a literal offset into the ``spy_minimal`` segment,
never from a constant the code reads. Each offset was found by flipping every bit 0 of
that segment with the old reader and the new one side by side, each read in a forked
child.
"""

from __future__ import annotations

import errno
import os
import resource
import signal
import time as time_module
from datetime import date, datetime, time
from pathlib import Path

import pyarrow as pa
import pytest

from lake import capture, journal
from lake.calendar import MARKET_TZ
from lake.cassette import load_cassette
from tests.conftest import CASSETTES

DAY = date(2026, 8, 24)
PID = 7

_CHAIN = load_cassette(CASSETTES / "spy_minimal.json").find("chains", {"symbol": "SPY"}).body

# What one ``spy_minimal`` chains segment measures. The size is asserted, so a cassette
# that changes shape fails loudly rather than moving a flip somewhere it no longer does
# what its name says.
SEGMENT_BYTES = 9736
ROWS_PER_SEGMENT = 2
# Reads back with both rows and fails full validation.
FAILS_VALIDATION = 5936
# Read as zero rows with no error before this change: the batch fails to decode, and the
# old reader took that for a torn tail. The file still ends in its marker.
READ_AS_ZERO_ROWS = 4632
# Killed the process with SIGSEGV on the dev Mac when every column was read.
CRASHED_THE_READ = 4025
# Raised ``ArrowNotImplementedError`` at the open, which no reader caught.
NOT_IMPLEMENTED_AT_OPEN = 525
# Raise ``OSError`` from pyarrow, at the open and mid-stream. They stand for the two most
# common classes: of every single-bit flip of this segment, 18,687 raised ``OSError`` at the
# open and 15,856 mid-stream, against 455 and 571 for the classes the offsets above hit.
OSERROR_AT_OPEN = 1860
OSERROR_MID_STREAM = 6100
# How many bytes of the batch a torn copy keeps: the file is cut this far short of its
# marker, inside the batch body.
TORN_SHORT = 400

EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def _et(hour: int, minute: int) -> datetime:
    return datetime.combine(DAY, time(hour, minute), tzinfo=MARKET_TZ)


def _captured(lake_root: Path, minute: int = 0) -> Path:
    """One chains segment written the way the capture loop writes one."""
    at = _et(10, minute)
    outcome = capture.journal_snapshot(
        lake_root,
        journal.CHAINS_SURFACE,
        "SPY",
        body=_CHAIN,
        cycle_start=at,
        fetch_ts=at,
        fetch_end_ts=at,
        pid=PID,
    )
    assert outcome.path.stat().st_size == SEGMENT_BYTES
    return outcome.path


def _flip(path: Path, offset: int) -> None:
    """Flip bit 0 of one byte, the smallest damage a disk can do."""
    data = bytearray(path.read_bytes())
    data[offset] ^= 0x01
    path.write_bytes(bytes(data))


# -- 1. validation -----------------------------------------------------------


def test_a_batch_that_fails_validation_is_refused(lake_root):
    path = _captured(lake_root)
    _flip(path, FAILS_VALIDATION)
    assert path.read_bytes().endswith(EOS)

    with pytest.raises(journal.SegmentDamaged, match="failed validation") as info:
        journal.read_segment(path)

    assert info.value.path == path
    assert isinstance(info.value, pa.ArrowInvalid)
    assert isinstance(info.value.__cause__, pa.ArrowInvalid)


def test_a_batch_that_fails_validation_is_refused_in_a_file_without_its_marker(lake_root):
    """Validation does not wait on the marker, so a torn or unclosed file is checked too."""
    path = _captured(lake_root)
    _flip(path, FAILS_VALIDATION)
    data = path.read_bytes()
    path.write_bytes(data[: -len(EOS)])

    with pytest.raises(journal.SegmentDamaged, match="failed validation"):
        journal.read_segment(path)


def test_recorded_slots_files_a_segment_that_fails_validation_as_corrupt(lake_root):
    path = _captured(lake_root)
    _flip(path, FAILS_VALIDATION)

    present = journal.recorded_slots(lake_root, journal.CHAINS_SURFACE, "SPY", DAY)

    assert present.slots == frozenset()
    assert present.unreadable == (journal.UnusableSegment(path, journal.SEGMENT_CORRUPT),)


def test_an_intact_segment_reads_every_row(lake_root):
    """The healthy side. A check that refused a sound batch would refuse every segment."""
    path = _captured(lake_root)

    table = journal.read_segment(path)

    assert table.num_rows == ROWS_PER_SEGMENT
    present = journal.recorded_slots(lake_root, journal.CHAINS_SURFACE, "SPY", DAY)
    assert present.slots == frozenset({_et(10, 0)})
    assert present.unreadable == ()


# -- 2. a stop inside a finished file ----------------------------------------


def test_a_stop_inside_a_file_that_ends_in_its_marker_is_damage(lake_root):
    path = _captured(lake_root)
    _flip(path, READ_AS_ZERO_ROWS)
    assert path.read_bytes().endswith(EOS)

    with pytest.raises(journal.SegmentDamaged, match="stopped inside a finished file") as info:
        journal.read_segment(path)
    assert isinstance(info.value.__cause__, OSError | pa.ArrowInvalid)

    # The minute it holds is no longer read as missing. Startup marking refuses the pair
    # rather than writing a gap marker over a minute that was captured.
    present = journal.recorded_slots(lake_root, journal.CHAINS_SURFACE, "SPY", DAY)
    assert present.unreadable == (journal.UnusableSegment(path, journal.SEGMENT_CORRUPT),)


def test_an_os_error_mid_stream_in_a_finished_file_is_damage(lake_root):
    path = _captured(lake_root)
    _flip(path, OSERROR_MID_STREAM)

    with pytest.raises(journal.SegmentDamaged, match="stopped inside a finished file") as info:
        journal.read_segment(path)

    assert type(info.value.__cause__) is OSError


def _written(lake_root: Path, start: str, cycles: int) -> Path:
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, start, PID
    ) as writer:
        for _ in range(cycles):
            writer.write_cycle(_batch())
    return writer.path


def test_damage_in_a_later_batch_of_a_finished_file_is_damage(lake_root):
    """The rule holds past the first batch, though every writer today writes one."""
    one = _written(lake_root, "a", 1)
    two = _written(lake_root, "b", 2)
    batch_bytes = two.stat().st_size - one.stat().st_size
    _flip(two, READ_AS_ZERO_ROWS + batch_bytes)

    with pytest.raises(journal.SegmentDamaged, match="stopped inside a finished file"):
        journal.read_segment(two)


def test_a_torn_copy_of_the_same_segment_still_reads_as_a_torn_tail(lake_root):
    path = _captured(lake_root)
    data = path.read_bytes()
    # Cut inside the one batch, the way a power loss does, so no marker follows it.
    path.write_bytes(data[: len(data) - len(EOS) - TORN_SHORT])

    table = journal.read_segment(path)

    assert table.num_rows == 0
    assert table.schema == journal.CHAINS_SCHEMA


def test_a_copy_cut_just_before_its_marker_keeps_its_batch(lake_root):
    path = _captured(lake_root)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) - len(EOS)])

    assert journal.read_segment(path).num_rows == ROWS_PER_SEGMENT


# -- 3. every class the stream raises ----------------------------------------


def _open_raises(monkeypatch, exc: Exception) -> None:
    def refuse(source):
        raise exc

    monkeypatch.setattr(pa.ipc, "open_stream", refuse)


def test_whatever_the_open_raises_on_a_finished_file_is_damage(lake_root, monkeypatch):
    path = _captured(lake_root)
    original = pa.ArrowNotImplementedError("unsupported flatbuffer")
    _open_raises(monkeypatch, original)

    with pytest.raises(journal.SegmentDamaged) as info:
        journal.read_segment(path)

    assert info.value.__cause__ is original
    assert str(info.value).startswith(f"{path}: the stream will not open: ")
    assert "unsupported flatbuffer" in str(info.value)
    assert journal._open_failure_kind(info.value) == journal.SEGMENT_CORRUPT


def test_an_os_error_at_the_open_of_a_finished_file_is_damage(lake_root):
    path = _captured(lake_root)
    _flip(path, OSERROR_AT_OPEN)

    with pytest.raises(journal.SegmentDamaged, match="will not open") as info:
        journal.read_segment(path)

    assert type(info.value.__cause__) is OSError


def test_a_file_that_is_only_the_marker_is_damage(lake_root):
    path = _captured(lake_root)
    path.write_bytes(EOS)

    with pytest.raises(journal.SegmentDamaged):
        journal.read_segment(path)


def test_whatever_the_open_raises_on_a_torn_file_is_arrow_invalid(lake_root, monkeypatch):
    """No class escapes a torn file either, and it is not called damage.

    Compaction reads ``ArrowInvalid`` from a torn file as a segment torn before its first
    batch, which holds no rows. ``SegmentDamaged`` would refuse the ticker-day instead.
    """
    path = _captured(lake_root)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) - len(EOS)])
    original = pa.ArrowNotImplementedError("unsupported flatbuffer")
    _open_raises(monkeypatch, original)

    with pytest.raises(pa.ArrowInvalid) as info:
        journal.read_segment(path)

    assert not isinstance(info.value, journal.SegmentDamaged)
    assert info.value.__cause__ is original


class _StopsWith:
    """A stream reader whose first ``read_next_batch`` raises ``exc``."""

    schema = journal.CHAINS_SCHEMA

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def read_next_batch(self):
        raise self._exc


def _read_raises(monkeypatch, exc: BaseException) -> None:
    monkeypatch.setattr(pa.ipc, "open_stream", lambda source: _StopsWith(exc))


def _unmarked(lake_root: Path) -> Path:
    """A captured segment cut just before its marker, as a power loss leaves one."""
    path = _captured(lake_root)
    data = path.read_bytes()
    path.write_bytes(data[: len(data) - len(EOS)])
    return path


@pytest.mark.parametrize("stage", ["open", "read"])
def test_a_failure_that_is_not_about_the_bytes_is_never_a_tear(lake_root, monkeypatch, stage):
    """On a file with no marker a tear drops rows silently, so only byte failures are one.

    A programming error raised on every read would otherwise read every unmarked segment
    as torn, and compaction would seal each ticker-day without those rows.
    """
    path = _unmarked(lake_root)
    original = RuntimeError("not about the bytes")
    if stage == "open":
        _open_raises(monkeypatch, original)
    else:
        _read_raises(monkeypatch, original)

    with pytest.raises(RuntimeError) as info:
        journal.read_segment(path)

    assert info.value is original


def test_a_byte_failure_mid_stream_on_an_unmarked_file_is_a_tear(lake_root, monkeypatch):
    path = _unmarked(lake_root)
    _read_raises(monkeypatch, pa.ArrowNotImplementedError("unsupported"))

    assert journal.read_segment(path).num_rows == 0


def test_any_failure_mid_stream_on_a_finished_file_is_damage(lake_root, monkeypatch):
    path = _captured(lake_root)
    original = RuntimeError("not about the bytes")
    _read_raises(monkeypatch, original)

    with pytest.raises(journal.SegmentDamaged) as info:
        journal.read_segment(path)

    assert info.value.__cause__ is original


@pytest.mark.parametrize("stage", ["open", "read"])
@pytest.mark.parametrize("marked", [True, False], ids=["finished", "unmarked"])
def test_running_out_of_memory_is_never_damage_or_a_tear(lake_root, monkeypatch, stage, marked):
    path = _captured(lake_root) if marked else _unmarked(lake_root)
    original = pa.ArrowMemoryError("out of memory")
    if stage == "open":
        _open_raises(monkeypatch, original)
    else:
        _read_raises(monkeypatch, original)

    with pytest.raises(MemoryError) as info:
        journal.read_segment(path)

    assert info.value is original


class _ValidateRaises:
    """A reader whose one batch raises ``exc`` from its validation."""

    schema = journal.CHAINS_SCHEMA

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def read_next_batch(self):
        exc = self._exc

        class Batch:
            def validate(self, full=False):
                raise exc

        return Batch()


def test_running_out_of_memory_in_validation_is_not_damage(lake_root, monkeypatch):
    path = _captured(lake_root)
    original = pa.ArrowMemoryError("out of memory")
    monkeypatch.setattr(pa.ipc, "open_stream", lambda source: _ValidateRaises(original))

    with pytest.raises(MemoryError) as info:
        journal.read_segment(path)

    assert info.value is original


def test_a_missing_segment_is_still_vanished(lake_root):
    path = _captured(lake_root)
    path.unlink()

    with pytest.raises(FileNotFoundError) as info:
        journal.read_segment(path)

    assert journal._open_failure_kind(info.value) == journal.SEGMENT_VANISHED


def test_a_flip_that_raised_not_implemented_at_the_open_is_damage(lake_root):
    path = _captured(lake_root)
    _flip(path, NOT_IMPLEMENTED_AT_OPEN)

    with pytest.raises(journal.SegmentDamaged) as info:
        journal.read_segment(path)

    assert isinstance(info.value.__cause__, NotImplementedError)


# -- 4. a crash -------------------------------------------------------------


# pyarrow keeps a thread pool, and Python warns that forking a threaded process can
# deadlock the child. The child here only reads a local file and exits.
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_flip_that_crashed_the_unvalidated_read_is_caught(lake_root):
    """Read in a forked child, so a crash fails this test rather than ending the run.

    A segfault is undefined behavior. The same flip may crash, raise ``SystemError``, or
    read back with wrong rows on another platform, and every one of those fails the
    assertion below. Only a child that caught ``SegmentDamaged`` exits 0.
    """
    path = _captured(lake_root)
    _flip(path, CRASHED_THE_READ)

    child = os.fork()
    if child == 0:  # pragma: no cover - runs in the child, which coverage does not follow
        code = 3
        try:
            table = journal.read_segment(path)
            for column in table.columns:
                column.to_pylist()
            code = 1
        except journal.SegmentDamaged:
            code = 0
        except BaseException:
            code = 2
        finally:
            os._exit(code)
    # Bounded, so a child that hangs fails the test rather than holding the job to its
    # ceiling. The read takes milliseconds.
    for _ in range(600):
        done, status = os.waitpid(child, os.WNOHANG)
        if done:
            break
        time_module.sleep(0.1)
    else:
        os.kill(child, 9)
        os.waitpid(child, 0)
        pytest.fail("the forked read did not finish within 60 seconds")

    assert not os.WIFSIGNALED(status), f"the read died on signal {os.WTERMSIG(status)}"
    assert os.WEXITSTATUS(status) == 0, {1: "read without error", 2: "raised another class"}.get(
        os.WEXITSTATUS(status)
    )


# -- 5. the walk past a damaged segment --------------------------------------


def test_latest_expirations_walks_past_a_damaged_segment(lake_root):
    older = _captured(lake_root, minute=0)
    newer = _captured(lake_root, minute=1)
    intact = journal.latest_expirations(lake_root, "SPY")
    assert intact
    _flip(newer, NOT_IMPLEMENTED_AT_OPEN)

    assert journal.latest_expirations(lake_root, "SPY") == intact
    # The older segment answered, and it is unchanged.
    assert older.exists()


# -- 6. the writer after a failed write --------------------------------------


class _FullDisk:
    """A file that takes ``allow`` bytes, raises ``ENOSPC`` once, and then recovers.

    That is a disk that fills partway through a batch and frees up before the close. The
    close then succeeds, which is the case that used to write the marker behind the
    half-written batch.
    """

    def __init__(self, inner, allow: int) -> None:
        self._inner = inner
        self._left = allow
        self._failed = False

    def write(self, data) -> int:
        data = bytes(data)
        if not self._failed and len(data) > self._left:
            written = self._inner.write(data[: self._left])
            self._left -= written
            self._failed = True
            raise OSError(errno.ENOSPC, "No space left on device")
        if not self._failed:
            self._left -= len(data)
        return self._inner.write(data)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _Interrupted(_FullDisk):
    """The same partial write, cut short by Ctrl-C rather than a full disk."""

    def write(self, data) -> int:
        data = bytes(data)
        if not self._failed and len(data) > self._left:
            self._inner.write(data[: self._left])
            self._failed = True
            raise KeyboardInterrupt
        if not self._failed:
            self._left -= len(data)
        return self._inner.write(data)


def _full_disk_after(monkeypatch, allow: int, sink: type[_FullDisk] = _FullDisk) -> None:
    real = pa.PythonFile

    def wrapped(handle, mode=None):
        return real(sink(handle, allow), mode=mode)

    monkeypatch.setattr(pa, "PythonFile", wrapped)


def _batch():
    return journal.chains_data_batch(
        _CHAIN, ticker="SPY", snap_ts=_et(10, 0).isoformat(), fetch_ts=_et(10, 0).isoformat()
    )


def _segment_files(path: Path) -> list[Path]:
    """What the segment's directory holds, so a removal is checked as an absent entry."""
    return sorted(path.parent.iterdir())


def test_a_write_that_raised_removes_the_file(lake_root, monkeypatch):
    """Nothing was durable, so the file held no row a reader could use (marketlake #769).

    Left in place, the half-written bytes read as a torn header or a torn batch. The first
    makes startup marking refuse the whole surface and ticker for the day, and the second
    reads as no rows, so a restart marks a minute the daemon was alive for.
    """
    _full_disk_after(monkeypatch, 5000)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    with pytest.raises(OSError, match="No space"):
        with writer:
            writer.write_cycle(_batch())

    assert writer.closed
    assert writer.durable_syncs == 0
    assert not writer.path.exists()
    assert _segment_files(writer.path) == []


def test_a_write_that_raised_and_was_caught_removes_the_file(lake_root, monkeypatch):
    """The caller that catches inside the block, which ``__exit__`` cannot see."""
    _full_disk_after(monkeypatch, 5000)

    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID
    ) as writer:
        with pytest.raises(OSError, match="No space"):
            writer.write_cycle(_batch())

    assert not writer.path.exists()


def test_a_write_that_finished_still_gets_its_marker(lake_root, monkeypatch):
    """The healthy side, through the same wrapped sink with room to spare."""
    _full_disk_after(monkeypatch, 10**9)

    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID
    ) as writer:
        writer.write_cycle(_batch())

    assert writer.path.read_bytes().endswith(EOS)
    assert writer.durable_syncs == 2
    assert journal.read_segment(writer.path).num_rows == ROWS_PER_SEGMENT


def test_an_interrupted_write_leaves_no_marker(lake_root, monkeypatch):
    """Ctrl-C is not an ``Exception``, and ``__exit__`` still closes the file behind it."""
    _full_disk_after(monkeypatch, 5000, _Interrupted)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    with pytest.raises(KeyboardInterrupt):
        with writer:
            writer.write_cycle(_batch())

    assert not writer.path.read_bytes().endswith(EOS)


def _then_interrupt(real):
    """A flush that does its real work and is then interrupted, once."""
    interrupted: list[bool] = []

    def call(*args):
        result = real(*args)
        if not interrupted:
            interrupted.append(True)
            raise KeyboardInterrupt
        return result

    return call


def test_an_interrupt_just_after_the_flush_keeps_the_batch(lake_root, monkeypatch):
    """Where a Ctrl-C almost always lands, so its file is kept rather than removed.

    Python raises a pending interrupt right after the flush returns and before the writer
    counts the flush durable, so the batch is whole and on disk while ``durable_syncs`` still
    reads 0. Removing the file on an interrupt would delete those rows.
    """
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    monkeypatch.setattr(journal.fcntl, "fcntl", _then_interrupt(journal.fcntl.fcntl))
    monkeypatch.setattr(journal.os, "fsync", _then_interrupt(journal.os.fsync))

    with pytest.raises(KeyboardInterrupt):
        with writer:
            writer.write_cycle(_batch())

    assert writer.durable_syncs == 0
    assert not writer.path.read_bytes().endswith(EOS)
    assert journal.read_segment(writer.path).num_rows == ROWS_PER_SEGMENT


def test_a_failed_durability_flush_removes_the_file(lake_root, monkeypatch):
    """Every byte reached the file, but none was made durable, so the write did not finish.

    The flush is ``F_FULLFSYNC`` on macOS and ``os.fsync`` elsewhere, so both fail here,
    once each, and only after the writer has opened and made its directory durable. The
    rows might read back, and nothing proves they reached the disk.
    """
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    real_fcntl, real_fsync = journal.fcntl.fcntl, journal.os.fsync
    failed: list[str] = []

    def once(name, real):
        def call(*args):
            if name not in failed:
                failed.append(name)
                raise OSError(errno.EIO, "I/O error")
            return real(*args)

        return call

    monkeypatch.setattr(journal.fcntl, "fcntl", once("fcntl", real_fcntl))
    monkeypatch.setattr(journal.os, "fsync", once("fsync", real_fsync))

    with pytest.raises(OSError, match="I/O"):
        with writer:
            writer.write_cycle(_batch())

    assert not writer.path.exists()


def test_a_write_after_a_removal_is_refused(lake_root, monkeypatch):
    """The writer counts as closed once its file is gone, so nothing writes into the void."""
    _full_disk_after(monkeypatch, 5000)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    with pytest.raises(OSError):
        writer.write_cycle(_batch())
    assert writer.closed

    with pytest.raises(ValueError, match="cannot write to a closed segment"):
        writer.write_cycle(_batch())

    assert not writer.path.exists()


def test_a_later_batch_that_fails_keeps_the_durable_one_without_a_marker(lake_root, monkeypatch):
    """Where an earlier batch was durable, the file stays and the marker stays off (#552)."""
    whole = _written(lake_root, "a", 1).stat().st_size
    # Room for the first batch and part of the second.
    _full_disk_after(monkeypatch, whole - len(EOS) + 5000)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    with pytest.raises(OSError, match="No space"):
        with writer:
            writer.write_cycle(_batch())
            writer.write_cycle(_batch())

    assert writer.durable_syncs == 1
    assert not writer.path.read_bytes().endswith(EOS)
    assert journal.read_segment(writer.path).num_rows == ROWS_PER_SEGMENT


def test_a_failed_write_still_closes_the_file(lake_root, monkeypatch):
    _full_disk_after(monkeypatch, 5000)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    with pytest.raises(OSError, match="No space"):
        with writer:
            writer.write_cycle(_batch())

    with pytest.raises(OSError):
        os.fstat(writer._fd)


def test_a_marker_that_failed_is_not_written_by_a_second_close(lake_root, monkeypatch):
    """The marker is tried once, so a second ``close`` cannot put a second one behind it.

    The disk here refuses the marker and then frees up, so a retry would land. The reader
    refuses a file with bytes after its marker as a shadow-append.
    """
    whole = _written(lake_root, "a", 1).stat().st_size
    _full_disk_after(monkeypatch, whole - len(EOS))
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    writer.write_cycle(_batch())

    with pytest.raises(OSError, match="No space"):
        writer.close()
    after_the_first = writer.path.read_bytes()
    writer.close()

    assert writer.closed
    assert writer.path.read_bytes() == after_the_first
    assert journal.read_segment(writer.path).num_rows == ROWS_PER_SEGMENT


def _open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


class _Refused(Exception):
    """A failure while the writer is being built, after its file exists."""


@pytest.mark.parametrize(
    ("target", "raised"),
    [
        ("fsync", OSError(errno.EIO, "I/O error")),
        ("new_stream", _Refused("the stream would not start")),
        ("new_stream", KeyboardInterrupt()),
    ],
    ids=["directory-fsync", "new-stream", "interrupt"],
)
def test_a_failure_after_the_create_leaves_no_file_and_no_descriptor(
    lake_root, monkeypatch, target, raised
):
    """The file is empty at this point, so it goes whatever was raised, and so does its fd."""
    path = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    path.parent.mkdir(parents=True)

    def refuse(*args, **kwargs):
        raise raised

    if target == "fsync":
        monkeypatch.setattr(journal.os, "fsync", refuse)
    else:
        monkeypatch.setattr(pa.ipc, "new_stream", refuse)
    before = _open_descriptors()

    with pytest.raises(type(raised)):
        journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    assert not path.exists()
    assert _open_descriptors() == before


def test_a_collision_leaves_the_other_writers_file_alone(lake_root):
    """A failed create never removes anything, because the path is someone else's file."""
    path = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"another writer's durable rows")

    with pytest.raises(FileExistsError):
        journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    assert path.read_bytes() == b"another writer's durable rows"


def test_a_removal_that_fails_still_raises_the_write_failure(lake_root, monkeypatch):
    """The cleanup never replaces the failure, and a file it cannot remove stays."""
    _full_disk_after(monkeypatch, 5000)
    writer = journal.SegmentWriter.open(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "s", PID)

    def refuse(self, missing_ok=False):
        raise PermissionError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(Path, "unlink", refuse)

    with pytest.raises(OSError) as info:
        with writer:
            writer.write_cycle(_batch())

    assert info.value.errno == errno.ENOSPC
    assert writer.closed
    assert writer.path.exists()


# -- 7. a real short write ---------------------------------------------------


def _snapshot_batch():
    """The batch ``_captured`` writes, built the way ``journal_snapshot`` builds it."""
    at = _et(10, 0)
    return capture._build_snapshot_batch(
        journal.CHAINS_SURFACE, "SPY", _CHAIN, snap_ts=at, fetch_ts=at, fetch_end_ts=at
    )


def _write_under_a_size_limit(lake_root: Path, limit: int) -> tuple[int, Path]:
    """Write ``_snapshot_batch`` in a forked child whose files may not pass ``limit`` bytes.

    ``RLIMIT_FSIZE`` makes the kernel write short and then refuse with ``EFBIG``, which is a
    real short write on macOS and Linux alike. ``SIGXFSZ`` is ignored, or the signal would
    kill the child instead. The limit applies to every file the child writes, so it prints
    nothing and reports by exit code:

    1. 10: ``write_cycle`` raised with nothing durable.
    2. 20: ``write_cycle`` returned and ``close`` raised.
    3. 30: both returned.
    4. 40: ``write_cycle`` returned over nothing durable, or raised over a durable batch.
    5. 50: anything else.
    """
    path = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "f", PID)
    batch = _snapshot_batch()
    child = os.fork()
    if child == 0:  # pragma: no cover - runs in the child, which coverage does not follow
        code = 50
        try:
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            _, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
            resource.setrlimit(resource.RLIMIT_FSIZE, (limit, hard))
            writer = journal.SegmentWriter.open(
                lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "f", PID
            )
            try:
                writer.write_cycle(batch)
            except OSError:
                code = 10 if writer.durable_syncs == 0 else 40
            else:
                if writer.durable_syncs != 1:
                    code = 40
                else:
                    try:
                        writer.close()
                        code = 30
                    except OSError:
                        code = 20
        except BaseException:
            code = 50
        finally:
            os._exit(code)
    # Bounded, so a child that hangs fails the test rather than holding the job to its
    # ceiling. The write takes milliseconds.
    for _ in range(600):
        done, status = os.waitpid(child, os.WNOHANG)
        if done:
            break
        time_module.sleep(0.1)
    else:
        os.kill(child, 9)
        os.waitpid(child, 0)
        pytest.fail("the forked write did not finish within 60 seconds")
    assert not os.WIFSIGNALED(status), f"the write died on signal {os.WTERMSIG(status)}"
    return os.WEXITSTATUS(status), path


def test_the_snapshot_batch_is_the_measured_segment(lake_root):
    """The budgets below are offsets from ``SEGMENT_BYTES``, so the batch must fill it."""
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "f", PID
    ) as writer:
        writer.write_cycle(_snapshot_batch())

    assert writer.path.stat().st_size == SEGMENT_BYTES


# pyarrow keeps a thread pool, and Python warns that forking a threaded process can
# deadlock the child. The child here only writes a local file and exits.
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_short_write_inside_the_batch_raises_rather_than_counting_it_durable(lake_root):
    """pyarrow ignores the count ``write`` returns, so the writer has to (marketlake #769).

    Eleven bytes short of the segment, the batch's last write is short and nothing after
    it in the batch is written. Before the write loop, ``write_cycle`` returned there with
    ``durable_syncs`` at 1 over a batch that reads as no rows.
    """
    code, path = _write_under_a_size_limit(lake_root, SEGMENT_BYTES - 11)

    assert code == 10, {20: "write_cycle returned", 30: "both returned"}.get(code, code)
    assert not path.exists()


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_short_end_of_stream_write_raises_from_close_and_keeps_the_rows(lake_root):
    """One byte short, the batch is durable and only the marker is short.

    The file stays with its rows, which is a captured minute the cycle still reports as a
    write failure. Before the write loop, ``close`` returned with seven of the marker's
    eight bytes on disk and the segment reported as landed.
    """
    code, path = _write_under_a_size_limit(lake_root, SEGMENT_BYTES - 1)

    assert code == 20, {10: "write_cycle raised", 30: "close returned"}.get(code, code)
    assert not path.read_bytes().endswith(EOS)
    assert journal.read_segment(path).num_rows == ROWS_PER_SEGMENT
