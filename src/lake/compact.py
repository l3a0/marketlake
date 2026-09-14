"""The close+15 compaction, backup, and nightly window re-tune.

The capture loop writes a day's cycles into journal segments, one Arrow IPC file per
surface, ticker, and writer session. A *segment* is that file. Reading a day back from
dozens of segments is slow and fragile, so once the day is final the segments are merged
into one Parquet *partition* per surface and ticker, checksummed into the manifest, and
deleted. That merge is *compaction*. This module is the close+15 job that does it, then
copies the lake to the backup SSD, then re-sizes the chain chunk plan from what the day
captured.

Every ``close+N`` here counts from the *option* close, the capture stop at 16:15 ET on a
regular day and 13:15 on an early close. It never counts from the 16:00 equity close, even
though the option close is itself fifteen minutes past that. So close+15 is 16:30 and
close+5 is 16:20.

The job's rules, each glossed at first use.

1. *One lock for the whole run.* Every lake-mutating job takes the lake-root ``flock``
   first, the kernel file lock on ``manifest.jsonl``. So a hand-run compaction and a
   scheduled one never race, and neither races the backup. Capture workers stay outside
   the lock by design, so blocking a cycle behind compaction never drops a minute.
2. *Sweep every date, but only past the guard.* The job walks every date directory under
   ``journal/``, so a segment orphaned by an earlier failed run is recovered. A ticker-day
   is eligible only once its *option-close deadline* has passed. That is close+5, the last
   moment the option-close fill may still write a journal batch. The job's own run time is
   close+15, ten minutes past the deadline, so it starts well clear of that boundary.
   Eligibility is decided from the injected clock and calendar alone, so the job never
   seals the live day and never unlinks a segment the daemon holds open.
3. *Verify before manifest, manifest before delete.* Each ticker-day's segments are read
   to their last complete batch, concatenated, and written as one Parquet file. The file
   is then read back, once, and that one read serves both checks. Its row count is
   compared to the sum across the segments, and its digest is what the manifest records.
   Only after that does the manifest entry land, and only after the manifest append are
   the segments unlinked. A crash at any point re-runs with nothing lost.
4. *Drift that survives the merge is reported, never raised.* Segments in one ticker-day
   can disagree about columns only when the daemon restarted onto different code
   mid-session, because every production segment takes its schema from
   ``journal.schema_for`` and a vendor that stops sending a field yields a null column
   rather than a dropped one. So the check guards this project's own release process
   rather than the vendor. The merged schema is compared to the pinned one at the merge,
   which is the last moment the segments exist, and what moved is filed under
   ``reports/`` once the seal has committed. One page per run carries it to a phone,
   folding every finding the run made into a single message that names the columns. The
   finding never raises, because the job seals every ticker-day bare and a raise would
   cost the rest of the sweep, the re-tune, the backup, and the ping. One disagreement
   never reaches this check at all. A column two segments hold at different types is
   refused by ``concat_tables`` before the comparison runs, and that refusal does raise
   and does cost the run. Whether it should
   is [#184](https://github.com/l3a0/marketlake/issues/184), which weighs for a retype the
   trade this rule declines for a dropped column. The durable remedy is
   ``schema_version`` enforcement, which is
   [#128](https://github.com/l3a0/marketlake/issues/128) and not compaction's business.
   This check is a detector and secondary to it.
5. *A torn tail is dropped, a shadow-append is refused.* A torn tail is a segment cut
   mid-batch by a power loss. Its complete batches are kept and the cut bytes dropped,
   never an error. A *shadow-append* is bytes after a segment's end-of-stream marker, the
   signature of a second writer appending past a closed stream. Standard readers never
   see those rows, so the job refuses to bless the file and fails the run loudly.
6. *Manifest-aware recovery.* If the manifest already holds a last entry for a partition,
   no automatic run ever recompacts it. The job verifies the partition's sha256 against
   the entry and finishes the interrupted cleanup by deleting the debris segments. Any
   mismatch raises to human review. The one repair is ``recompact_ticker_day``, a
   deliberate, human-invoked rebuild that appends a superseding entry. The standing
   invariant holds throughout: no automatic run ever replaces a manifested partition with
   fewer rows than its recorded count.
7. *Backup, then ping.* After every eligible ticker-day is sealed, the lake is synced to
   the backup target. The health-check ping fires only after the backup succeeds, so the
   one ping attests both. An unmounted target raises before any ping. A holiday or an
   empty journal is a correct no-op and still backs up and pings. The drift page above
   goes out ahead of both, from a ``finally`` around the sweep, because an unmounted
   target or a failed seal must not be able to swallow it.
8. *The nightly re-tune.* The chain is fetched in date windows so each response stays
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
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lake import journal
from lake.alert import REFUSED, Message, NtfyTransport, Publisher
from lake.calendar import Calendar, ExchangeCalendar
from lake.chain_plan import DEFAULT_CHAIN_PLAN_PATH, ChainPlan, Window, load_chain_plan
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, input_errors_exit, load_config
from lake.journal import ROW_KIND_DATA, ShadowAppendError
from lake.lock import lake_lock
from lake.manifest import (
    append_manifest,
    guard_row_count,
    latest_entries,
    sha256_bytes,
    sha256_file,
)
from lake.paths import (
    CHAINS,
    DATE_PREFIX,
    REPORTS_DIR,
    SEGMENT_GLOB,
    SURFACE_PREFIX,
    TICKER_PREFIX,
    LakePaths,
    parse_date_dir,
    temp_write_path,
)
from lake.report import SCHEMA_DRIFT_DIR, SchemaDrift, write_schema_drift
from lake.runner import PING_FAILURES, BackupRunner, Pinger, RsyncBackup, UrllibPinger
from lake.session import SessionClock

# The health-check slug the compaction job pings. It is the compaction-plus-backup check
# from the design's steady-state set. Log the slug, never the ping URL, which carries the
# secret ping key.
COMPACTION_SLUG = "compaction"

# The manifest ``source`` for a compacted partition entry.
COMPACTION_SOURCE = "compaction"

# The chains columns the re-tune profile reads. Everything else stays on disk.
_PROFILE_COLUMNS = ("ticker", "snap_ts", "row_kind", "window_start", "window_end")

# The event and title on compaction's schema-drift page. The design's message table gives
# schema drift one row and names two producers for it, the parser mid-day and the nightly
# battery. Compaction is a third, and it reports a different fact. The other two read what
# the vendor sent. This one reports what this project's own release shipped mid-session,
# which is legible at the merge and nowhere after it. The producer is in the event name so
# a reader of ``reports/alerts/`` can tell the three apart without opening a file.
SCHEMA_DRIFT_EVENT = "compaction_schema_drift"
SCHEMA_DRIFT_TITLE = "Schema drift at the merge"

# How many column names the page prints per kind before it stops and says how many are
# left. ntfy's default body limit is 4096 bytes and it answers an oversize POST with a
# 400, which ``NtfyTransport`` does not retry. A whole-schema rename across both surfaces
# names about 150 columns and runs to roughly 4000 bytes, so the widest drift, the one
# that matters most, is the one that would not reach the phone. The count survives the
# cut, and the files under ``reports/schema_drift/`` name every column either way.
PAGE_COLUMN_CAP = 12


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
    profile. ``backed_up`` and ``pinged`` record the two post-seal steps. ``problem``
    names a ping that failed, which leaves ``pinged`` false. The seal and the backup
    already happened, so the run's report is worth more than the lost ping.
    """

    sealed: tuple[SealedPartition, ...]
    verified: tuple[SealedPartition, ...]
    skipped: tuple[SkippedDay, ...]
    retune: RetuneResult | None
    backed_up: bool
    pinged: bool
    problem: str | None = None

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
        if self.problem is not None:
            lines.append(f"  {self.problem}")
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
        if not entry.is_dir() or not entry.name.startswith(DATE_PREFIX):
            continue
        # ``parse_date_dir`` is the one decision about what a date directory is. The
        # dashboard's panels ask it the same question, so both halves of the system now
        # agree on the answer. It is stricter than the bare ``date.fromisoformat`` this
        # used to call. On Python 3.12 that parser also reads ``date=20260824`` and
        # ``date=2026-W35-1`` as 2026-08-24, so the sweep used to seal three differently
        # named directories into one partition while the panels showed only the one
        # spelled ``date=2026-08-24``.
        #
        # Stricter is the correct direction. Every writer hands ``_day_str`` a ``date``,
        # which it renders as ISO, so a directory in any other spelling was never written
        # by this pipeline. The helper passes a string through verbatim, so the guarantee
        # rests on the writers rather than on the helper. Sealing a foreign directory is
        # worse than declining to. Compaction would fold it into a partition the panels
        # cannot show, and the segments would be unlinked afterward.
        #
        # This module has no logger. The ``SkippedDay`` below is how a foreign directory
        # stays discoverable. It carries the full directory name, the run result holds it,
        # and ``CompactionResult.render`` prints it.
        day = parse_date_dir(entry.name)
        if day is None:
            skipped.append(SkippedDay(entry.name, "unparseable"))
            continue
        if not calendar.is_session(day):
            skipped.append(SkippedDay(day.isoformat(), "not_a_session"))
            continue
        if slot <= session.bounds(day).option_close_deadline:
            skipped.append(SkippedDay(day.isoformat(), "guard_open"))
            continue
        eligible.append((day, entry))
    return eligible, skipped


def _ticker_days(date_dir: Path) -> list[tuple[str, str, Path]]:
    """Every ``(surface, ticker, directory)`` under one journal date directory."""
    found: list[tuple[str, str, Path]] = []
    for surface_dir in sorted(date_dir.iterdir()):
        if not surface_dir.is_dir() or not surface_dir.name.startswith(SURFACE_PREFIX):
            continue
        surface = surface_dir.name[len(SURFACE_PREFIX) :]
        for ticker_dir in sorted(surface_dir.iterdir()):
            if not ticker_dir.is_dir() or not ticker_dir.name.startswith(TICKER_PREFIX):
                continue
            found.append((surface, ticker_dir.name[len(TICKER_PREFIX) :], ticker_dir))
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


def _pinned_order(table: pa.Table, pinned: pa.Schema) -> pa.Table:
    """The merged table with its pinned columns back in the pinned schema's order.

    Promotion appends. A segment written before a column was added has no place to put
    it, so ``concat_tables`` adds the promoted column at the end of the merged table
    rather than where the pinned schema holds it. The columns are then right and the
    order is not, and a comparison that read order as drift would file a finding on every
    legitimate column addition. Reordering first is what makes the comparison a plain
    equality.

    Order itself is not a correctness concern. Every read in ``src/`` is by name, and the
    documented multi-day path is DuckDB's ``union_by_name``. This exists to make the
    check cheap, not to fix a read.

    A column the pinned schema does not name keeps its place at the end rather than being
    dropped, so the reorder moves columns and never loses one.
    """
    present = set(table.schema.names)
    named = set(pinned.names)
    order = [name for name in pinned.names if name in present]
    order += [name for name in table.schema.names if name not in named]
    if order == table.schema.names:
        return table
    return table.select(order)


def _schema_drift(
    merged: pa.Schema,
    pinned: pa.Schema,
    *,
    surface: str,
    ticker: str,
    day: date,
    partition: str,
    segments: Sequence[str],
) -> SchemaDrift:
    """What the merged schema carries that the pinned one does not.

    Called only once the two schemas have already been found unequal, so this explains a
    difference rather than deciding there is one. The three fields name columns, because
    a human reading the file wants the column and not a count.
    """
    pinned_names = set(pinned.names)
    merged_types = {field.name: str(field.type) for field in merged}
    return SchemaDrift(
        surface=surface,
        ticker=ticker,
        day=day,
        partition=partition,
        schema_version=journal.SCHEMA_VERSION,
        missing=tuple(name for name in pinned.names if name not in merged_types),
        unexpected=tuple(name for name in merged.names if name not in pinned_names),
        retyped=tuple(
            f"{name}: {pinned.field(name).type} -> {merged_types[name]}"
            for name in pinned.names
            if name in merged_types and merged_types[name] != str(pinned.field(name).type)
        ),
        segments=tuple(segments),
    )


def _file_drift(
    root: Path,
    drift: SchemaDrift,
    *,
    clock: Clock,
    found: list[SchemaDrift] | None = None,
) -> None:
    """File one finding, remember it for the run's page, and never let either cost the run.

    The sweep calls ``_seal`` bare, once per ticker-day, and ``compact``'s only
    ``try/except`` wraps the health-check ping. So anything raised here would cost every
    ticker-day still to be sealed, the window re-tune, the backup, and the ping. Trading
    a null column on one ticker for a lake-wide backup outage is a bad trade, which is
    why the finding is reported and never raised.

    A write that itself fails leaves stderr, which launchd files. That is ``alert._record``'s
    rule for the same situation: the record is the last line of defence, and when it
    fails the one place left to say so is the log.

    ``found`` is the list the run collects its findings in, and ``_page_drift`` turns that
    list into one page. This helper appends the finding before it attempts the write. The
    order matters: a run that could not write the file down has only the page and the log
    left as a trace, so the page has to go out even then.
    """
    if found is not None:
        found.append(drift)
    try:
        write_schema_drift(root, drift, now=clock.now())
    except OSError as exc:
        print(
            f"compaction: schema drift on {drift.partition} could not be filed: "
            f"{type(exc).__name__}",
            file=sys.stderr,
        )


def _named(kind: str, names: Sequence[str]) -> str:
    """One kind's columns, capped, as ``missing bid, ask and 3 more``.

    The cap is what keeps the page inside ntfy's body limit. Past it the page prints how
    many were dropped rather than dropping them silently, because the count is what tells
    a reader whether they are looking at one moved column or a whole-schema rename.
    """
    shown = ", ".join(names[:PAGE_COLUMN_CAP])
    rest = len(names) - PAGE_COLUMN_CAP
    return f"{kind} {shown}" if rest <= 0 else f"{kind} {shown} and {rest} more"


def _drift_body(drifted: Sequence[SchemaDrift]) -> str:
    """Compose what the run's page says.

    The page carries how many ticker-days drifted, over which session dates, and which
    columns moved. The columns are a union across the findings, so one drifted column is
    named once however many ticker-days carried it. The body counts ticker-days and never
    names the tickers, because one bad release drifts every ticker-day still in flight and
    a page listing them all would be unreadable on a phone while saying no more than the
    count does.

    ``SchemaDrift`` allows its three column lists, ``missing``, ``unexpected`` and
    ``retyped``, to be empty at once. A difference the names and the types do not show
    still files a record, and a nullability change is the difference that reaches here. The
    page has to say that plainly rather than trailing off after "did not carry".
    """
    days = sorted({drift.day.isoformat() for drift in drifted})
    moved = "; ".join(
        _named(kind, names)
        for kind, names in (
            ("missing", sorted({name for drift in drifted for name in drift.missing})),
            ("unexpected", sorted({name for drift in drifted for name in drift.unexpected})),
            ("retyped", sorted({name for drift in drifted for name in drift.retyped})),
        )
        if names
    )
    if not moved:
        moved = "no column named, so the schemas differ some other way"
    return (
        f"{len(drifted)} ticker-day(s) over {', '.join(days)} did not carry the pinned "
        f"schema at the merge. {moved}. Findings under "
        f"{REPORTS_DIR}/{SCHEMA_DRIFT_DIR}/. Correct the schema and bump schema_version."
    )


def _page_drift(
    publisher: Publisher | None, drifted: Sequence[SchemaDrift], *, now: datetime
) -> None:
    """Page once for the whole run, naming every column that moved.

    **One page, not one per finding.** A wide drift has one cause. The check guards this
    project's own release process rather than the vendor, so a daemon that restarted
    mid-session onto code carrying a different schema drifts every ticker-day still in
    flight, and each finding then restates the same columns. Paging per finding would
    scale the page count with the roster while the fact stayed one fact, and one bad
    release would spend the publisher's forty-a-day cap on its own. The page the cap
    swallowed could be the auth-death page, so this producer must not cause that storm.
    Nothing is lost by folding: ``reports/schema_drift/`` holds one file per finding, with
    the ticker, the partition, and the segments in it.

    The finding reaches stderr as well as the phone, which is what the daemon's assertion
    page already does. launchd files that log and the restart script sends the operator to
    it. A publisher that refused the page found one of its own secrets in the body, and it
    redacted its record for that reason, so stderr must not undo the redaction. That is
    the one case where the body stops here.

    ``publish`` never raises, so this cannot cost the backup that runs after it. A page
    that did not reach the phone is written down under ``reports/alerts/`` by the
    publisher itself, and the reason is named on stderr too.
    """
    if not drifted:
        return
    body = _drift_body(drifted)
    delivery = None
    if publisher is not None:
        delivery = publisher.publish(
            Message(event=SCHEMA_DRIFT_EVENT, title=SCHEMA_DRIFT_TITLE, body=body), now=now
        )
        if delivery.reason == REFUSED:
            print("compaction: schema-drift page refused: it carried a secret", file=sys.stderr)
            return
    print(f"compaction: {SCHEMA_DRIFT_TITLE}: {body}", file=sys.stderr)
    if delivery is not None and not delivery.sent:
        kept = "written down" if delivery.recorded else "lost"
        print(
            f"compaction: schema-drift page not sent: {delivery.reason}, {kept}",
            file=sys.stderr,
        )


def _write_partition(table: pa.Table, partition: Path) -> None:
    """Write a Parquet partition atomically: a temp file, a flush, then one rename.

    A crash mid-write leaves only the temp file, never a torn Parquet at the partition
    path. The rename replaces any prior file in one step, so a replacement is atomic or
    not at all. The temp file is cleaned up on any failure.
    """
    partition.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_write_path(partition, os.getpid())
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
    found: list[SchemaDrift] | None = None,
) -> SealedPartition:
    """Merge one ticker-day's segments into its partition, verify, manifest, unlink.

    The order is the design's compaction-failure rule. Every segment is read before
    anything is written, so a shadow-append raises with the ticker-day untouched. The
    Parquet lands and is read back once. That read yields both the row count, checked
    against the sum across the segments, and the digest the manifest entry carries. The
    entry is appended. Only then are the segments unlinked.

    With ``guard`` on, the no-shrink invariant is checked before the partition file is
    replaced. A refused rebuild must leave the larger partition on disk, untouched,
    beside its still-valid entry. ``append_manifest`` checks the same invariant again,
    and the comment at that call says why that second check can never be the one that
    refuses a seal started here.

    The merged schema is compared to the surface's pinned one on the way past. The
    comparison happens at the merge, because that is the last moment the merged schema
    exists and the reorder it rests on has to run before the write either way. The
    finding is filed after the manifest append, so only a seal that committed files one.
    It is reported rather than raised, because a raise from this function costs the rest
    of the sweep.

    ``found`` is the list the caller collects this run's findings in. ``compact`` passes
    one and pages once from it. The repair passes one too and pages nobody from it, so the
    operator who started it reads the drift on their own terminal instead.
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

    # The merge is the last moment the segments still exist, so it is the only moment a
    # mid-day drop is plain. Unifying by name fills a column one segment lacks with
    # nulls, which is right for a rotation that adds a column and indistinguishable from
    # one that drops it. What separates the two is the pinned schema this code is running
    # with. A dropped column is one the merged table still carries and the pinned schema
    # no longer names. An added one is named by both. After the seal neither is legible:
    # the partition holds the column with nulls on the post-rotation rows either way.
    #
    # ``schema_for`` raises on a surface it does not know, and that raise is reachable
    # from neither caller. Both build the partition path first, and ``partition_path``
    # refuses exactly the surfaces the schemas have no entry for.
    pinned = journal.schema_for(surface)
    merged = _pinned_order(merged, pinned)
    drift = (
        None
        if merged.schema.equals(pinned)
        else _schema_drift(
            merged.schema,
            pinned,
            surface=surface,
            ticker=ticker,
            day=day,
            partition=rel,
            segments=[path.relative_to(root).as_posix() for path in segments],
        )
    )

    if guard:
        guard_row_count(root, rel, expected)

    _write_partition(merged, partition)
    # One read of the sealed file serves both post-write checks. The row count proves
    # every page decodes, and the digest attests the very bytes the count came from.
    written = partition.read_bytes()
    actual = pq.read_table(pa.BufferReader(written)).num_rows
    if actual != expected:
        raise CompactionVerifyError(rel, expected, actual)

    # ``guard`` is passed on, but not because this append re-checks anything reachable.
    # It arrives with the same row count, against the same manifest, and nothing appends
    # a line in between, because every appender takes the lake-root lock this run already
    # holds. So a guard the pre-write check passed passes here too, always. What the
    # argument does decide is the other direction: a human recompaction arrives with
    # ``guard`` off, and the append has to be told, or it would refuse the very
    # supersession that was asked for. #170 established both halves by mutation. Forcing
    # ``guard=False`` here changed no test in the suite, and forcing ``guard=True`` failed
    # the deliberate-recompaction test. So the check is live for every other caller of
    # ``append_manifest`` and unreachable from this one.
    #
    # Which caller got here decides whether either check has anything to refuse.
    # ``compact`` reaches ``_seal`` only for a ticker-day with no manifest entry, and
    # ``guard_row_count`` has nothing to compare against for such a path, so both checks
    # are inert on that route. ``recompact_ticker_day`` is the one caller that rebuilds a
    # manifested partition, so it is the only one either check can refuse, and the
    # pre-write one above is where that refusal lands.
    entry = append_manifest(
        root,
        partition=rel,
        source=COMPACTION_SOURCE,
        sha256=sha256_bytes(written),
        rows=expected,
        fetched_at=clock.now().isoformat(),
        guard=guard,
    )
    # Filed once the seal has committed, and never before. A refused rebuild and a write
    # that fails both leave the partition alone, so a finding filed earlier would name a
    # file that does not carry the drift. Nothing is lost by waiting: a run that raises
    # leaves the segments on disk, and the next one merges them again and finds the same
    # difference.
    if drift is not None:
        _file_drift(root, drift, clock=clock, found=found)
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
    tmp = temp_write_path(target, os.getpid())
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
    publisher: Publisher | None = None,
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

    ``publisher`` is the schema-drift page and it follows ``pinger`` exactly. Both reach
    past this process, so ``main`` builds them and never accepts them, and a test drives
    this helper with a fake instead. It is optional for the same reason ``pinger`` is: a
    caller with nowhere to page skips it, and the default is ``None`` rather than a live
    object, so omitting it can never reach a real phone. What a run without one loses is
    only the page. The finding is still filed under ``reports/schema_drift/`` and still
    named on stderr.
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
        problem: str | None = None
        drifted: list[SchemaDrift] = []
        chains_by_day: dict[date, list[SealedPartition]] = {}
        # The page goes out in a ``finally``, so no raise anywhere can swallow it. The
        # sweep itself raises on a row-count regression, a failed verify, a partition that
        # does not match its manifest entry, and any OSError from the write or the unlink.
        # A ticker-day that already drifted and sealed has had its segments unlinked, so
        # the next run finds nothing to merge for it and never runs the check again. The
        # finding would then be filed and never paged, for good. The re-tune and the
        # backup sit after the sweep and raise too, which is the same loss one step later.
        # The design's schema policy says a missing or retyped known field pages, and a
        # page any later step can swallow does not satisfy it.
        try:
            for day, date_dir in eligible:
                for surface, ticker, ticker_dir in _ticker_days(date_dir):
                    segments = sorted(ticker_dir.glob(SEGMENT_GLOB))
                    if not segments:
                        continue
                    rel = paths.partition_path(surface, ticker, day).relative_to(root).as_posix()
                    entry = latest.get(rel)
                    if entry is not None:
                        outcome = _recover(root, paths, surface, ticker, day, segments, entry)
                        verified.append(outcome)
                    else:
                        outcome = _seal(
                            root,
                            paths,
                            surface,
                            ticker,
                            day,
                            segments,
                            clock=clock,
                            guard=True,
                            found=drifted,
                        )
                        latest[rel] = {"sha256": outcome.sha256, "rows": outcome.rows}
                        sealed.append(outcome)
                    if surface == CHAINS:
                        chains_by_day.setdefault(day, []).append(outcome)
                _prune_empty(date_dir)
        finally:
            _page_drift(publisher, drifted, now=clock.now())

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
            try:
                pinger.ping(str(ping_url))
                pinged = True
            except PING_FAILURES as exc:
                problem = f"ping failed: {type(exc).__name__}"

    return CompactionResult(
        sealed=tuple(sealed),
        verified=tuple(verified),
        skipped=tuple(skipped),
        retune=retune,
        backed_up=backed_up,
        pinged=pinged,
        problem=problem,
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

    The merge runs the same schema check the scheduled job does, so the rebuild can find
    drift. It files the finding and writes it to stderr, and it pages nobody. The operator
    started this run and is reading its output, which is the reader a page exists to reach.
    """
    root = Path(lake_root)
    paths = LakePaths(root)
    with lake_lock(root):
        ticker_dir = paths.segment_dir(surface, ticker, day)
        segments = sorted(ticker_dir.glob(SEGMENT_GLOB))
        if not segments:
            raise RecompactionRefused(
                f"no segments remain for {surface}/{ticker}/{day.isoformat()}; "
                "a recompaction needs the day's segments"
            )
        drifted: list[SchemaDrift] = []
        try:
            outcome = _seal(
                root,
                paths,
                surface,
                ticker,
                day,
                segments,
                clock=clock,
                guard=not allow_shrink,
                found=drifted,
            )
            _prune_empty(ticker_dir.parent.parent)
        finally:
            # No publisher, so this pages nobody and only writes the drift to stderr. A
            # repair prints the partition and the row count and nothing else, so without
            # this line an operator reads a clean-looking success over a ticker-day whose
            # merged schema was not the pinned one.
            _page_drift(None, drifted, now=clock.now())
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
) -> int:
    """The ``python -m lake.compact`` entry. Returns a process exit code.

    ``backup``, ``pinger`` and the drift page's ``Publisher`` are built here, not
    accepted. Each reaches past this process. ``rsync`` shells out to copy the lake, the
    healthchecks GET goes to the network, and the publisher POSTs to ntfy, which reaches
    a phone. A ``main`` that accepted them let a test omit one and reach the real effect,
    so ``main`` builds them and a test drives the ``compact`` helper directly instead.

    ``clock`` and ``calendar`` stay injectable. A system clock and an exchange calendar
    never reach past this process, so a test injects them with no live effect.

    The daemon dispatches this job itself at close+15, so the scheduled run comes from
    in there rather than from here. This entry stays for the hand run: a catch-up after
    a machine was off for a day, or a run under the operator's eye. Both reach the same
    ``compact`` below, and its lake-root lock is what keeps the two from racing.
    """
    args = build_parser().parse_args(argv)
    with input_errors_exit("compact"):
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
        backup=RsyncBackup(),
        backup_target=config.backup_target,
        pinger=UrllibPinger(),
        ping_url=config.healthchecks_url(COMPACTION_SLUG),
        publisher=Publisher(
            lake_root=config.lake_root,
            transport=NtfyTransport(config.ntfy_topic.reveal()),
            # The values that must never reach a phone, checked against the page itself.
            secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
        ),
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
    "SCHEMA_DRIFT_EVENT",
    "SCHEMA_DRIFT_TITLE",
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
