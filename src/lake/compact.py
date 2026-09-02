"""The close+15 compaction, backup, and nightly window re-tune.

The capture loop writes a day's cycles into journal segments, one Arrow IPC file per
surface, ticker, and writer session. A *segment* is that file. Reading a day back from
dozens of segments is slow and fragile, so once the day is final the segments are merged
into one Parquet *partition* per surface and ticker, checksummed into the manifest, and
deleted. That merge is *compaction*. This module is the close+15 job that does it, then
copies the lake to the backup SSD, then re-sizes the chain chunk plan from what the day
captured.

The job's rules, each glossed at first use.

1. *One lock for the whole run.* Every lake-mutating job takes the lake-root ``flock``
   first, the kernel file lock on ``manifest.jsonl``. So a hand-run compaction and a
   scheduled one never race, and neither races the backup. Capture workers stay outside
   the lock by design, so blocking a cycle behind compaction never drops a minute.
2. *Sweep every date, but only past the guard.* The job walks every date directory under
   ``journal/``, so a segment orphaned by an earlier failed run is recovered. A ticker-day
   is eligible only once its *option-close deadline* has passed. That is close+5, five
   minutes past the option close, the last moment the option-close fill may still write a
   journal batch. Eligibility is decided from the injected clock and calendar alone, so
   the job never seals the live day and never unlinks a segment the daemon holds open.
3. *Verify before manifest, manifest before delete.* Each ticker-day's segments are read
   to their last complete batch, concatenated, and written as one Parquet file. The file
   is then re-read and its row count checked against the sum across the segments. Only
   after that does the manifest entry land, and only after the manifest append are the
   segments unlinked. A crash at any point re-runs with nothing lost.
4. *A torn tail is dropped, a shadow-append is refused.* A torn tail is a segment cut
   mid-batch by a power loss. Its complete batches are kept and the cut bytes dropped,
   never an error. A *shadow-append* is bytes after a segment's end-of-stream marker, the
   signature of a second writer appending past a closed stream. Standard readers never
   see those rows, so the job refuses to bless the file and fails the run loudly.
5. *Manifest-aware recovery.* If the manifest already holds a last entry for a partition,
   no automatic run ever recompacts it. The job verifies the partition's sha256 against
   the entry and finishes the interrupted cleanup by deleting the debris segments. Any
   mismatch raises to human review. The one repair is ``recompact_ticker_day``, a
   deliberate, human-invoked rebuild that appends a superseding entry. The standing
   invariant holds throughout: no automatic run ever replaces a manifested partition with
   fewer rows than its recorded count.
6. *Backup, then ping.* After every eligible ticker-day is sealed, the lake is synced to
   the backup target. The health-check ping fires only after the backup succeeds, so the
   one ping attests both. An unmounted target raises before any ping. A holiday or an
   empty journal is a correct no-op and still backs up and pings.
7. *The nightly re-tune.* The chain is fetched in date windows so each response stays
   under Schwab's gateway body limit. After the seal, the job groups the day's chains
   rows by ``window_start`` and ``window_end``, takes each plan window's peak per-cycle
   contract count, and compares it to two guard constants. A window over the max splits
   at its midpoint offset. Two adjacent finite windows both under the min merge. The open
   tail is never split and never merged. A window that failed all day has no measured
   size, so it never moves and no merge crosses it. A day with no windowed data row says
   nothing about the plan and rewrites nothing. The rebuilt plan is written to
   ``chain_plan.json`` atomically, and only when it changed.

This module reads no wall clock. ``clock`` and ``calendar`` are injected, and every
session-relative moment comes from the session clock over them.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lake import journal
from lake.calendar import Calendar, ExchangeCalendar
from lake.chain_plan import DEFAULT_CHAIN_PLAN_PATH, ChainPlan, Window, load_chain_plan
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, load_config
from lake.journal import ROW_KIND_DATA, ShadowAppendError
from lake.lock import lake_lock
from lake.manifest import guard_row_count, latest_entries, record_partition, sha256_file
from lake.paths import CHAINS, LakePaths
from lake.runner import BackupRunner, Pinger, RsyncBackup, UrllibPinger
from lake.session import SessionClock

# The health-check slug the compaction job pings. It is the compaction-plus-backup check
# from the design's steady-state set. Log the slug, never the ping URL, which carries the
# secret ping key.
COMPACTION_SLUG = "compaction"

# The manifest ``source`` for a compacted partition entry.
COMPACTION_SOURCE = "compaction"

# The three journal path levels, as their directory-name prefixes.
_DATE_PREFIX = "date="
_SURFACE_PREFIX = "surface="
_TICKER_PREFIX = "ticker="
_SEGMENT_GLOB = "seg-*.arrows"

# The chains columns the re-tune profile reads. Everything else stays on disk.
_PROFILE_COLUMNS = ("ticker", "snap_ts", "row_kind", "window_start", "window_end")


# -- the named failures ------------------------------------------------------


class PartitionMismatch(Exception):
    """Raised when a manifested partition does not match its last manifest entry.

    The design routes this to human review rather than repairing it. An automatic run
    that rebuilt the partition from whatever segments remain could replace a full day
    with a fragment, and every integrity layer would bless the loss. ``actual`` is
    ``None`` when the partition file is missing altogether.
    """

    def __init__(self, partition: str, expected: str, actual: str | None) -> None:
        state = "missing" if actual is None else f"sha256 {actual}"
        super().__init__(f"{partition}: manifest records sha256 {expected}, file is {state}")
        self.partition = partition
        self.expected = expected
        self.actual = actual


class CompactionVerifyError(Exception):
    """Raised when a freshly written partition re-reads with the wrong row count."""

    def __init__(self, partition: str, expected: int, actual: int) -> None:
        super().__init__(f"{partition}: wrote {expected} rows, re-read {actual}")
        self.partition = partition
        self.expected = expected
        self.actual = actual


class RecompactionRefused(Exception):
    """Raised when a human-invoked recompaction has no segments left to rebuild from."""


# -- the result types --------------------------------------------------------


@dataclass(frozen=True)
class SealedPartition:
    """One ticker-day the run sealed or recovered.

    ``partition`` is the lake-relative Parquet path. ``segments`` are the lake-relative
    segment paths merged into it and then unlinked. ``recovered`` is true when the
    partition already had a manifest entry, so the run verified it and deleted the
    debris instead of rewriting it. ``rows`` and ``sha256`` then come from the entry.
    """

    surface: str
    ticker: str
    day: date
    partition: str
    rows: int
    sha256: str
    segments: tuple[str, ...]
    recovered: bool = False


@dataclass(frozen=True)
class SkippedDay:
    """A journal date directory the run left untouched, and why.

    ``guard_open`` means the day's option-close deadline has not passed on the injected
    clock, so the daemon may still append. ``not_a_session`` means the calendar calls the
    date closed, so no session bounds exist to judge it by. ``unparseable`` means the
    directory name is not ``date=YYYY-MM-DD``. Nothing is deleted in any of the three.
    """

    day: str
    reason: str


@dataclass(frozen=True)
class RetuneResult:
    """What the nightly window re-tune decided for one day's chains profile.

    ``counts`` is the peak per-cycle contract count for each window of ``before``, in
    order, ``None`` for a window that failed all day and so has no measured size.
    ``splits`` names each window that split. ``merges`` names each adjacent pair that
    merged. ``written`` is true only when the plan changed and the file landed.
    ``skipped_reason`` is set when the profile could not be compared to the plan, either
    because the day had no windowed data row or because its rows carried windows outside
    the current plan. Nothing is rewritten in either case.
    """

    day: date
    before: ChainPlan
    after: ChainPlan
    counts: tuple[int | None, ...]
    splits: tuple[Window, ...]
    merges: tuple[tuple[Window, Window], ...]
    written: bool
    skipped_reason: str | None = None

    @property
    def changed(self) -> bool:
        """Whether the rebuilt plan differs from the one it started from."""
        return self.after != self.before


@dataclass(frozen=True)
class CompactionResult:
    """What one close+15 run did.

    ``sealed`` lists the partitions written this run. ``verified`` lists the partitions
    that already had a manifest entry and were sha-checked, with their debris deleted.
    ``skipped`` lists the date directories left alone. ``retune`` is the window re-tune
    verdict, or ``None`` when no chains partition of an eligible day was available to
    profile. ``backed_up`` and ``pinged`` record the two post-seal steps.
    """

    sealed: tuple[SealedPartition, ...]
    verified: tuple[SealedPartition, ...]
    skipped: tuple[SkippedDay, ...]
    retune: RetuneResult | None
    backed_up: bool
    pinged: bool

    @property
    def changed(self) -> bool:
        """Whether the run changed the lake or the plan file at all.

        A second run over an already-sealed lake reports ``False``: nothing sealed, no
        debris deleted, no plan rewritten.
        """
        debris = any(item.segments for item in self.verified)
        rewrote = self.retune is not None and self.retune.written
        return bool(self.sealed) or debris or rewrote

    def render(self) -> str:
        """A human-readable summary. It names slugs and paths, never a ping URL."""
        lines = [
            f"compaction: sealed={len(self.sealed)} verified={len(self.verified)} "
            f"skipped={len(self.skipped)} backed_up={self.backed_up} pinged={self.pinged} "
            f"slug={COMPACTION_SLUG}"
        ]
        for item in self.sealed:
            lines.append(f"  sealed   {item.partition} rows={item.rows} from {len(item.segments)}")
        for item in self.verified:
            lines.append(
                f"  verified {item.partition} rows={item.rows} debris={len(item.segments)}"
            )
        for item in self.skipped:
            lines.append(f"  skipped  {item.day} ({item.reason})")
        if self.retune is None:
            lines.append("  retune   no chains partition to profile")
        elif self.retune.skipped_reason is not None:
            lines.append(f"  retune   skipped ({self.retune.skipped_reason})")
        else:
            verdict = "rewrote plan" if self.retune.written else "plan unchanged"
            lines.append(
                f"  retune   {self.retune.day.isoformat()} {verdict}: "
                f"splits={list(self.retune.splits)} merges={list(self.retune.merges)}"
            )
        return "\n".join(lines)


# -- the sweep scope ---------------------------------------------------------


def _sweep_scope(
    paths: LakePaths, session: SessionClock, calendar: Calendar
) -> tuple[list[tuple[date, Path]], list[SkippedDay]]:
    """Every journal date directory, split into the eligible and the skipped.

    A date is eligible once the current snap slot is past its option-close deadline. The
    deadline minute itself stays closed, matching how the session clock judges every
    boundary on the whole minute, so the close+5 fill's own minute is never swept.
    """
    eligible: list[tuple[date, Path]] = []
    skipped: list[SkippedDay] = []
    journal_dir = paths.journal_dir
    if not journal_dir.is_dir():
        return eligible, skipped
    slot = session.snap_slot()
    for entry in sorted(journal_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith(_DATE_PREFIX):
            continue
        raw = entry.name[len(_DATE_PREFIX) :]
        try:
            day = date.fromisoformat(raw)
        except ValueError:
            skipped.append(SkippedDay(entry.name, "unparseable"))
            continue
        if not calendar.is_session(day):
            skipped.append(SkippedDay(raw, "not_a_session"))
            continue
        if slot <= session.bounds(day).option_close_deadline:
            skipped.append(SkippedDay(raw, "guard_open"))
            continue
        eligible.append((day, entry))
    return eligible, skipped


def _ticker_days(date_dir: Path) -> list[tuple[str, str, Path]]:
    """Every ``(surface, ticker, directory)`` under one journal date directory."""
    found: list[tuple[str, str, Path]] = []
    for surface_dir in sorted(date_dir.iterdir()):
        if not surface_dir.is_dir() or not surface_dir.name.startswith(_SURFACE_PREFIX):
            continue
        surface = surface_dir.name[len(_SURFACE_PREFIX) :]
        for ticker_dir in sorted(surface_dir.iterdir()):
            if not ticker_dir.is_dir() or not ticker_dir.name.startswith(_TICKER_PREFIX):
                continue
            found.append((surface, ticker_dir.name[len(_TICKER_PREFIX) :], ticker_dir))
    return found


def _prune_empty(date_dir: Path) -> None:
    """Remove the now-empty ticker, surface, and date directories under a sealed date.

    Only an empty directory goes. A directory holding anything at all, a stray file or
    a segment the run did not seal, stays. So this never deletes data, only the shells
    the unlinked segments left behind.
    """
    for surface_dir in sorted(date_dir.iterdir()):
        if surface_dir.is_dir():
            for ticker_dir in sorted(surface_dir.iterdir()):
                if ticker_dir.is_dir():
                    _rmdir_if_empty(ticker_dir)
            _rmdir_if_empty(surface_dir)
    _rmdir_if_empty(date_dir)


def _rmdir_if_empty(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        pass  # not empty, or already gone


# -- reading and sealing one ticker-day --------------------------------------


def _read_complete(path: Path) -> pa.Table | None:
    """A segment's complete batches, or ``None`` when no batch survived.

    This is ``journal.read_segment`` with one more tolerated case. A segment torn before
    its first complete batch, or torn inside its stream header, holds no durable cycle at
    all. It reads as no rows rather than an error, the same torn-tail rule applied at the
    front of the file. A shadow-append still raises: bytes after an end-of-stream marker
    are never dropped silently.
    """
    try:
        return journal.read_segment(path)
    except ShadowAppendError:
        raise
    except (pa.ArrowInvalid, OSError):
        return None


def _durable(path: Path) -> None:
    """Flush a file to stable storage, past the drive cache where the platform allows."""
    fd = os.open(path, os.O_RDONLY)
    try:
        if journal.F_FULLFSYNC is not None:
            fcntl.fcntl(fd, journal.F_FULLFSYNC)
        else:  # pragma: no cover - non-macOS path
            os.fsync(fd)
    finally:
        os.close(fd)


def _durable_dir(path: Path) -> None:
    """Make a directory entry durable after a rename into it."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_partition(table: pa.Table, partition: Path) -> None:
    """Write a Parquet partition atomically: a temp file, a flush, then one rename.

    A crash mid-write leaves only the temp file, never a torn Parquet at the partition
    path. The rename replaces any prior file in one step, so a replacement is atomic or
    not at all. The temp file is cleaned up on any failure.
    """
    partition.parent.mkdir(parents=True, exist_ok=True)
    tmp = partition.with_name(f"{partition.name}.tmp-{os.getpid()}")
    try:
        pq.write_table(table, tmp)
        _durable(tmp)
        os.replace(tmp, partition)
        _durable_dir(partition.parent)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _seal(
    root: Path,
    paths: LakePaths,
    surface: str,
    ticker: str,
    day: date,
    segments: Sequence[Path],
    *,
    clock: Clock,
    guard: bool,
) -> SealedPartition:
    """Merge one ticker-day's segments into its partition, verify, manifest, unlink.

    The order is the design's compaction-failure rule. Every segment is read before
    anything is written, so a shadow-append raises with the ticker-day untouched. The
    Parquet lands, is re-read, and its row count is checked against the sum across the
    segments. The manifest entry is appended. Only then are the segments unlinked.

    With ``guard`` on, the no-shrink invariant is checked before the partition file is
    replaced, not only at the manifest append. A refused rebuild must leave the larger
    partition on disk, untouched, beside its still-valid entry.
    """
    tables: list[pa.Table] = []
    expected = 0
    for path in segments:
        table = _read_complete(path)
        if table is not None:
            tables.append(table)
            expected += table.num_rows
    if tables:
        # A mid-day vendor change rotates to a new segment with a new schema. Unifying by
        # name adds the new column as nulls on the older rows. A retyped column still
        # raises, which is the loud failure the schema policy wants for a retyped field.
        merged = pa.concat_tables(tables, promote_options="default")
    else:
        merged = journal.schema_for(surface).empty_table()

    partition = paths.partition_path(surface, ticker, day)
    rel = partition.relative_to(root).as_posix()
    if guard:
        guard_row_count(root, rel, expected)

    _write_partition(merged, partition)
    actual = pq.read_table(partition).num_rows
    if actual != expected:
        raise CompactionVerifyError(rel, expected, actual)

    entry = record_partition(
        root,
        rel,
        source=COMPACTION_SOURCE,
        rows=expected,
        fetched_at=clock.now().isoformat(),
        guard=guard,
    )
    for path in segments:
        path.unlink()
    return SealedPartition(
        surface=surface,
        ticker=ticker,
        day=day,
        partition=rel,
        rows=expected,
        sha256=entry["sha256"],
        segments=tuple(path.relative_to(root).as_posix() for path in segments),
    )


def _recover(
    root: Path,
    paths: LakePaths,
    surface: str,
    ticker: str,
    day: date,
    segments: Sequence[Path],
    entry: Mapping[str, object],
) -> SealedPartition:
    """Finish an interrupted cleanup: sha-verify the manifested partition, delete debris.

    Debris segments are the only reachable leftover once the sweep bound is close+5.
    They come from a crash between the manifest append and the last unlink. Any mismatch
    raises ``PartitionMismatch`` with the debris intact, so a human can decide.
    """
    partition = paths.partition_path(surface, ticker, day)
    rel = partition.relative_to(root).as_posix()
    expected = str(entry["sha256"])
    if not partition.exists():
        raise PartitionMismatch(rel, expected, None)
    actual = sha256_file(partition)
    if actual != expected:
        raise PartitionMismatch(rel, expected, actual)
    for path in segments:
        path.unlink()
    return SealedPartition(
        surface=surface,
        ticker=ticker,
        day=day,
        partition=rel,
        rows=int(entry["rows"]),
        sha256=expected,
        segments=tuple(path.relative_to(root).as_posix() for path in segments),
        recovered=True,
    )


# -- the window re-tune ------------------------------------------------------


@dataclass(frozen=True)
class WindowProfile:
    """What one day's chains rows say about each plan window.

    ``peaks`` maps a window to its peak per-cycle contract count across the day's data
    rows. ``failed`` names every window that carried a gap row that day, the absence
    markers the chunker writes for a window it gave up on. A window in ``failed`` with
    no entry in ``peaks`` never succeeded that day, so its true size is unknown.
    """

    peaks: Mapping[Window, int]
    failed: frozenset[Window]

    @property
    def unknown(self) -> frozenset[Window]:
        """The windows that failed all day and so have no measured size."""
        return self.failed - frozenset(self.peaks)


def _offsets(row: Mapping[str, object], session_date: date) -> Window:
    """A row's ISO window bounds as day offsets, the reverse of ``windows_for``."""
    start = (date.fromisoformat(str(row["window_start"])) - session_date).days
    end_iso = row["window_end"]
    end = None if end_iso is None else (date.fromisoformat(str(end_iso)) - session_date).days
    return (start, end)


def window_profile(table: pa.Table, session_date: date) -> WindowProfile:
    """Profile one day's chains rows by plan window.

    A cycle is one ``(ticker, snap_ts)``. Its contracts in a window are its data rows
    whose ``window_start`` and ``window_end`` name that window. The peak across cycles
    is what the body limit constrains, since each window is one request per cycle. Gap
    rows carrying a window are that window's absence markers, so they mark it failed
    for the day rather than counting toward it. The ISO window bounds are turned back
    into day offsets from ``session_date``, the same arithmetic ``ChainPlan.windows_for``
    runs forward. Rows with no window, from a one-shot whole-chain fetch, a whole-chain
    gap, or an expiration before the session date, are left out.
    """
    if table.num_rows == 0:
        return WindowProfile({}, frozenset())
    windowed = table.filter(pc.is_valid(table.column("window_start")))
    if windowed.num_rows == 0:
        return WindowProfile({}, frozenset())
    kinds = windowed.column("row_kind")
    data = windowed.filter(pc.equal(kinds, ROW_KIND_DATA))
    gaps = windowed.filter(pc.not_equal(kinds, ROW_KIND_DATA))

    peaks: dict[Window, int] = {}
    if data.num_rows:
        per_cycle = data.group_by(["ticker", "snap_ts", "window_start", "window_end"]).aggregate(
            [([], "count_all")]
        )
        by_window = per_cycle.group_by(["window_start", "window_end"]).aggregate(
            [("count_all", "max")]
        )
        for row in by_window.to_pylist():
            peaks[_offsets(row, session_date)] = int(row["count_all_max"])

    failed: set[Window] = set()
    if gaps.num_rows:
        distinct = gaps.group_by(["window_start", "window_end"]).aggregate([])
        for row in distinct.to_pylist():
            failed.add(_offsets(row, session_date))
    return WindowProfile(peaks, frozenset(failed))


def retune_plan(
    plan: ChainPlan,
    counts: Mapping[Window, int],
    *,
    max_contracts: int,
    min_contracts: int,
    unknown: frozenset[Window] | set[Window] = frozenset(),
) -> tuple[ChainPlan, tuple[Window, ...], tuple[tuple[Window, Window], ...]]:
    """Rebuild a plan from each window's peak contract count.

    Two passes, in order.

    1. *Split.* A finite window whose count is over ``max_contracts`` and that spans at
       least two days splits at its midpoint offset, ``(start, mid)`` and
       ``(mid + 1, end)``. Each half is credited half the count, rounded up, since the
       true split is only known after a day of capture. The open tail never splits.
    2. *Merge.* Walking left to right, two adjacent finite windows both under
       ``min_contracts`` merge into one, credited the sum. A merged window may merge
       again while its sum stays under the min. The open tail never merges.

    A window absent from ``counts`` had no contracts on any ticker and counts zero. A
    window in ``unknown`` failed all day, so its size is unmeasured. It neither splits
    nor merges, and it stops a merge from crossing it. Folding a failing range into a
    healthy neighbour would only spread the failure to the neighbour's contracts.

    The two triggers are disjoint, since a window over the max is never under the min,
    and a fresh half is credited at least half of a count over the max, which the pinned
    constants keep above the min. So the pass order cannot undo itself. The result is
    validated by ``ChainPlan``, so the rebuilt windows still tile ``[0, ∞)``.
    """
    windows = list(plan.windows)
    counted: list[tuple[Window, int | None]] = [
        (window, None if window in unknown else int(counts.get(window, 0))) for window in windows
    ]

    split: list[tuple[Window, int | None]] = []
    splits: list[Window] = []
    for window, count in counted:
        start, end = window
        if end is not None and count is not None and count > max_contracts and end - start >= 1:
            mid = (start + end) // 2
            half = -(-count // 2)
            split.append(((start, mid), half))
            split.append(((mid + 1, end), half))
            splits.append(window)
        else:
            split.append((window, count))

    merged: list[tuple[Window, int | None]] = []
    merges: list[tuple[Window, Window]] = []
    for window, count in split:
        if merged:
            previous, previous_count = merged[-1]
            finite = previous[1] is not None and window[1] is not None
            small = (
                previous_count is not None
                and count is not None
                and previous_count < min_contracts
                and count < min_contracts
            )
            if finite and small:
                merged[-1] = ((previous[0], window[1]), previous_count + count)
                merges.append((previous, window))
                continue
        merged.append((window, count))

    return ChainPlan(tuple(window for window, _ in merged)), tuple(splits), tuple(merges)


def write_chain_plan(plan: ChainPlan, path: Path | str) -> None:
    """Write a plan file atomically: a temp file beside it, a flush, then one rename.

    The shape is the one ``load_chain_plan`` reads back:
    ``{"windows": [{"start": 0, "end": 9}, ..., {"start": 366, "end": null}]}``. A crash
    mid-write leaves the prior file intact and the temp file is removed on any failure,
    so the daemon's next per-cycle read never meets a partial plan.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"windows": [{"start": start, "end": end} for start, end in plan.windows]}
    text = json.dumps(payload, indent=2) + "\n"
    tmp = target.with_name(f"{target.name}.tmp-{os.getpid()}")
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _retune(
    root: Path,
    partitions: Sequence[SealedPartition],
    day: date,
    *,
    guards: GuardConstants,
    plan_path: Path | str,
) -> RetuneResult:
    """Profile the day's chains partitions and rewrite the plan if the profile drifted.

    One plan serves the whole roster, so the profile is the peak across every ticker's
    partition for the day, and a window that failed on any ticker is failed for the
    day. A window with no rows on any ticker counts zero.

    Two cases report a reason and write nothing. A day with no windowed data row at all,
    from a dead daemon or a one-shot whole-chain fetch, carries no evidence about the
    plan. And a row whose window is not in the current plan means the plan file changed
    since capture, so the counts do not describe the plan's windows.
    """
    before = load_chain_plan(plan_path)
    peaks: dict[Window, int] = {}
    failed: set[Window] = set()
    for item in partitions:
        path = root / item.partition
        if not set(_PROFILE_COLUMNS) <= set(pq.read_schema(path).names):
            # A partition without the window columns predates the windowed fetch. It
            # carries no profile, so it has nothing to say about the plan.
            continue
        table = pq.read_table(path, columns=list(_PROFILE_COLUMNS))
        profile = window_profile(table, day)
        for window, count in profile.peaks.items():
            peaks[window] = max(peaks.get(window, 0), count)
        failed |= profile.failed
    unknown = failed - set(peaks)
    counts = tuple(None if window in unknown else peaks.get(window, 0) for window in before.windows)

    def skipped(reason: str) -> RetuneResult:
        return RetuneResult(
            day=day,
            before=before,
            after=before,
            counts=counts,
            splits=(),
            merges=(),
            written=False,
            skipped_reason=reason,
        )

    if not peaks:
        return skipped("no windowed data rows to profile")
    known = set(before.windows)
    foreign = sorted((window for window in set(peaks) | failed if window not in known), key=str)
    if foreign:
        return skipped(f"rows carry windows outside the current plan: {foreign}")
    after, splits, merges = retune_plan(
        before,
        peaks,
        max_contracts=guards.chain_window_max_contracts,
        min_contracts=guards.chain_window_min_contracts,
        unknown=unknown,
    )
    written = False
    if after != before:
        write_chain_plan(after, plan_path)
        written = True
    return RetuneResult(
        day=day,
        before=before,
        after=after,
        counts=counts,
        splits=splits,
        merges=merges,
        written=written,
    )


# -- the job -----------------------------------------------------------------


def compact(
    lake_root: Path | str,
    *,
    clock: Clock,
    calendar: Calendar,
    backup: BackupRunner,
    backup_target: Path | str,
    pinger: Pinger | None = None,
    ping_url: str | None = None,
    guards: GuardConstants | None = None,
    plan_path: Path | str = DEFAULT_CHAIN_PLAN_PATH,
) -> CompactionResult:
    """Run the close+15 job: sweep, seal, re-tune, back up, ping.

    The whole run holds the lake-root lock. The sweep covers every date under
    ``journal/`` whose option-close deadline has passed on the injected clock. Each
    eligible ticker-day is sealed, or, if its partition is already manifested,
    sha-verified with its debris deleted. The window re-tune then profiles the latest
    sealed day's chains partitions. The backup runs last, and the ping only after it.

    The run is idempotent. A second run over the same lake seals nothing, deletes
    nothing, rewrites no plan, and reports ``changed`` false. It still backs up and
    pings, because a job that correctly no-ops is healthy.

    ``pinger`` is optional so a caller without a health check, like a test, can skip
    it. When given, ``ping_url`` is required.
    """
    if pinger is not None and ping_url is None:
        raise ValueError("a pinger needs a ping_url")
    root = Path(lake_root)
    paths = LakePaths(root)
    guards = guards if guards is not None else GuardConstants()
    session = SessionClock(clock, calendar)

    with lake_lock(root):
        eligible, skipped = _sweep_scope(paths, session, calendar)
        latest = latest_entries(root)
        sealed: list[SealedPartition] = []
        verified: list[SealedPartition] = []
        chains_by_day: dict[date, list[SealedPartition]] = {}
        for day, date_dir in eligible:
            for surface, ticker, ticker_dir in _ticker_days(date_dir):
                segments = sorted(ticker_dir.glob(_SEGMENT_GLOB))
                if not segments:
                    continue
                rel = paths.partition_path(surface, ticker, day).relative_to(root).as_posix()
                entry = latest.get(rel)
                if entry is not None:
                    outcome = _recover(root, paths, surface, ticker, day, segments, entry)
                    verified.append(outcome)
                else:
                    outcome = _seal(
                        root, paths, surface, ticker, day, segments, clock=clock, guard=True
                    )
                    latest[rel] = {"sha256": outcome.sha256, "rows": outcome.rows}
                    sealed.append(outcome)
                if surface == CHAINS:
                    chains_by_day.setdefault(day, []).append(outcome)
            _prune_empty(date_dir)

        retune: RetuneResult | None = None
        if chains_by_day:
            latest_day = max(chains_by_day)
            retune = _retune(
                root, chains_by_day[latest_day], latest_day, guards=guards, plan_path=plan_path
            )

        # Backup first. A raised backup propagates before the ping, so a single-copy
        # window pages through the missed ping rather than being reported as healthy.
        backup.sync(root, Path(backup_target))
        backed_up = True
        pinged = False
        if pinger is not None:
            pinger.ping(str(ping_url))
            pinged = True

    return CompactionResult(
        sealed=tuple(sealed),
        verified=tuple(verified),
        skipped=tuple(skipped),
        retune=retune,
        backed_up=backed_up,
        pinged=pinged,
    )


def recompact_ticker_day(
    lake_root: Path | str,
    surface: str,
    ticker: str,
    day: date,
    *,
    clock: Clock,
    allow_shrink: bool = False,
) -> SealedPartition:
    """The human-invoked repair: rebuild one manifested partition from its segments.

    This is the one path that replaces a manifested partition. It runs under the
    lake-root lock, only while the day's segments still exist, and appends a
    superseding manifest entry. With ``allow_shrink`` off, the no-shrink guard still
    refuses a rebuild with fewer rows than the recorded count, before the partition
    file is touched. A human who has established that the recorded count is the wrong
    one passes ``allow_shrink=True`` to supersede it on their own authority.
    """
    root = Path(lake_root)
    paths = LakePaths(root)
    with lake_lock(root):
        ticker_dir = paths.segment_path(surface, ticker, day, "", 0).parent
        segments = sorted(ticker_dir.glob(_SEGMENT_GLOB))
        if not segments:
            raise RecompactionRefused(
                f"no segments remain for {surface}/{ticker}/{day.isoformat()}; "
                "a recompaction needs the day's segments"
            )
        outcome = _seal(
            root, paths, surface, ticker, day, segments, clock=clock, guard=not allow_shrink
        )
        _prune_empty(ticker_dir.parent.parent)
        return outcome


# -- the command-line entry --------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The ``python -m lake.compact`` argument parser.

    With no subcommand it runs the close+15 job. The ``recompact`` subcommand is the
    human-invoked repair for one ticker-day.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.compact",
        description="Compact the day's journal segments, back up the lake, re-tune the plan.",
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    parser.add_argument(
        "--plan", help="Path to chain_plan.json (defaults to the standard machine-owned file)."
    )
    sub = parser.add_subparsers(dest="command")
    repair = sub.add_parser(
        "recompact",
        help="Human-invoked repair: rebuild one manifested partition from its segments.",
    )
    repair.add_argument("surface", help="The surface, chains or quotes.")
    repair.add_argument("ticker", help="The ticker, like SPY.")
    repair.add_argument("day", help="The session date, ISO like 2026-08-24.")
    repair.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Supersede the recorded row count even when the rebuild has fewer rows.",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
    backup: BackupRunner | None = None,
    pinger: Pinger | None = None,
) -> int:
    """The ``python -m lake.compact`` entry. Returns a process exit code.

    The four seams default to the real ones: the system clock, the exchange calendar,
    ``rsync``, and ``urllib``. A test passes fakes. The daemon's internal close+15
    dispatch of this job is a later wiring. This entry runs it standalone.
    """
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    clock = clock if clock is not None else SystemClock()

    if args.command == "recompact":
        outcome = recompact_ticker_day(
            config.lake_root,
            args.surface,
            args.ticker,
            date.fromisoformat(args.day),
            clock=clock,
            allow_shrink=args.allow_shrink,
        )
        print(
            f"recompacted {outcome.partition} rows={outcome.rows} "
            f"from {len(outcome.segments)} segment(s)"
        )
        return 0

    result = compact(
        config.lake_root,
        clock=clock,
        calendar=calendar if calendar is not None else ExchangeCalendar(),
        backup=backup if backup is not None else RsyncBackup(),
        backup_target=config.backup_target,
        pinger=pinger if pinger is not None else UrllibPinger(),
        ping_url=config.healthchecks_url(COMPACTION_SLUG),
        guards=config.guards,
        plan_path=args.plan if args.plan is not None else DEFAULT_CHAIN_PLAN_PATH,
    )
    print(result.render())
    return 0


__all__ = [
    "COMPACTION_SLUG",
    "COMPACTION_SOURCE",
    "CompactionResult",
    "CompactionVerifyError",
    "PartitionMismatch",
    "RecompactionRefused",
    "RetuneResult",
    "SealedPartition",
    "SkippedDay",
    "WindowProfile",
    "build_parser",
    "compact",
    "main",
    "recompact_ticker_day",
    "retune_plan",
    "window_profile",
    "write_chain_plan",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
