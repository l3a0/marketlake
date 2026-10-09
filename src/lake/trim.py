"""The trim: drop chains partitions older than the window once each is verified in the bucket.

The hosted VM keeps its lake on a 30 GiB volume, and sealed chains partitions grow it by about
0.585 GB a session. Every sealed partition is already in the backup bucket, so the VM keeps a
window of recent chains sessions and drops older partitions once the bucket's copy is proven.
This module is the job that drops them (marketlake #787). It runs inside the close+15
compaction, after the upload and the ping, under the lock hold the ping sits in, so it needs
no lock of its own. ``lake.compact`` decides whether it runs at all and hands it an integer
window. This module decides which partitions go.

It lives outside ``lake.bucket`` because that module's rule is that only the range restore
writes under the lake root. The bucket read it makes, ``bucket.current_digest``, writes
nothing.

**A partition is dropped only when all seven clauses hold.**

1. It is a chains partition, ``chains/ticker=T/date=D.parquet``, present on disk. One already
   absent is done or lost, and either way no ``GetObject`` is sent for it.
2. Its day is at or before the window edge, ``window.window_edge(calendar, tonight, W + 1)``.
   Tonight's seal has landed since the sweep, hence W + 1, and the edge is computed here from
   the window rather than read from the checkpoint, whose edge goes stale when the window
   grows.
3. Its latest manifest entry sits below the watermark the nightly upload started from, so the
   bucket held it before tonight.
4. Its ticker-day's journal directory holds no segments. ``compact._recover`` raises on a
   missing partition with leftover segments, so one such trim would stop every later seal. A
   directory that cannot be listed counts as holding segments.
5. A ``GetObject`` of the current version hashes to the manifest's sha. The response's
   ``VersionId`` is what the trim line records.
6. Its day is at or before the latest checkpoint's cutoff for its ticker, and tonight is a
   session strictly after the checkpoint's own session day whose option-close deadline has
   passed. The checkpoint's sha has to match its manifest entry. A partition whose latest
   trimmed-ledger line is a restore line waits until a checkpoint from a session after the
   restore's day, so a range restore survives until the walk that needs it.
7. It is not withheld by ``manifest.latest_quarantine``.

**The write order** for each partition is the trim line, appended and read back by
``trimmed.append_trimmed``, then an fsync of the ledger and the lake root, then the unlink. The
reverse order would leave an absence nothing explains. A crash before the unlink leaves the
file present beside its trim line, which every reader treats as present. The next run then
re-runs the whole selection over it: a file that still qualifies gets a fresh trim line with
the ``VersionId`` just read, and one that a clause answers with a definite no gets a restore
line that supersedes the trim line.

**Failures.** One that would repeat on every partition stops the run: a bucket that refuses or
cannot be reached, a ``GetObject`` with no usable ``VersionId``, any exception from the ledger
step, and an unlink error other than a missing file or a permission refusal. A permission
refusal on the unlink stops only that ticker, because the fault is the ticker's directory. So
does a partition whose presence cannot be checked, which an unsearchable ticker directory
causes, and so does a ticker directory the trim cannot write and search, which it checks
before the ``GetObject``. That check writes no line, so a directory left unwritable does not
gain a fresh trim line every night, and the unlink's refusal stays as the backstop. Each of
those lines names the directory relative to the lake root and the ``chown`` and ``chmod``
that repair it, and no line the trim prints names an absolute path. A hash mismatch is rot:
the partition is kept, the lake's own copy is hashed so the page can say which copy is good,
and nothing is ever written to the bucket. Anything else skips only its partition. The
deadline the upload ran under is checked between partitions.

**This never raises.** ``compact.main`` prints the night's result only when ``compact``
returns, so a raise here would lose the night's sealed lines. Every outcome is a field of
:class:`TrimResult`, and an outer ``except Exception`` turns anything unforeseen into a stopped
result. The caller holds ``lake.lock.lake_lock``.
"""

from __future__ import annotations

import errno
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Any

from lake import trimmed
from lake.bucket import (
    BucketReadError,
    UploadSummary,
    current_digest,
    read_ledger,
    usable_version_id,
)
from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock
from lake.config import BucketTarget
from lake.manifest import is_quarantined, latest_entries, latest_quarantine, sha256_file
from lake.paths import CHAINS, SEGMENT_GLOB, LakePaths, PartitionRef, parse_partition_rel
from lake.session import SessionClock
from lake.split_checkpoint import (
    CHECKPOINT_PARTITION,
    Checkpoint,
    CheckpointUnreadable,
    checkpoint_path,
    read_checkpoint,
)
from lake.window import EdgeNotFound, window_edge

# The manifest ``source`` on the trimmed ledger's entry when the trim refreshes it.
TRIM_SOURCE = "trim"

# How many partitions the rot page names before it says how many are left. A page body stays
# under 1,000 bytes, and a lake-relative chains path runs to about 45 bytes plus its notes.
ROT_PAGE_CAP = 4

# The unlink errors that stop only their ticker. Unlinking needs write permission on the
# ticker's directory, which a range restore run with ``sudo`` can leave owned by root.
_TICKER_ERRNOS = frozenset({errno.EACCES, errno.EPERM})

# What a lake copy hashed to when the bucket's copy did not match.
LOCAL_GOOD = "the lake's copy matches the manifest"
LOCAL_ROTTED = "the lake's copy differs too"


@dataclass(frozen=True)
class RotFinding:
    """A partition whose bucket copy did not hash to its manifest sha, so it was kept.

    ``local`` says what the lake's own copy hashed to, :data:`LOCAL_GOOD`, :data:`LOCAL_ROTTED`,
    or the class of the error that stopped the read.
    """

    partition: str
    manifest_sha256: str
    bucket_sha256: str
    version_id: str | None
    local: str


@dataclass(frozen=True)
class TrimResult:
    """What one trim did.

    ``trimmed`` lists the partitions whose trim line landed and whose file is gone.
    ``lined`` lists those whose trim line landed and whose file stayed, because the unlink was
    refused. ``restored`` lists the partitions recovery wrote a restore line for. ``skipped``
    names each partition skipped for a reason worth reading, such as a key missing from the
    bucket. ``rot`` holds each rotted bucket copy, which ``compact`` pages once for the run.
    ``held`` names each ticker an unlink refusal stopped, with its repair.

    ``refused`` is set when a gate refused before any partition was looked at. ``stopped`` is
    set when a failure that would repeat on every partition ended the run. ``deadline`` is
    set when the upload's deadline came first. Each names its repair.

    ``window`` and ``edge`` are the window the run judged by and the last day it could drop.
    """

    window: int | None = None
    edge: date | None = None
    trimmed: tuple[str, ...] = ()
    lined: tuple[str, ...] = ()
    restored: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()
    rot: tuple[RotFinding, ...] = ()
    held: tuple[str, ...] = ()
    refused: str | None = None
    stopped: str | None = None
    deadline: str | None = None

    @property
    def changed(self) -> bool:
        """Whether a trim or restore line was appended or a file unlinked.

        A refusal, a stop, a skip and a rot finding alone write nothing to the lake.
        """
        return bool(self.trimmed or self.lined or self.restored)

    def render(self) -> list[str]:
        """The lines ``CompactionResult.render`` prints for the trim, indented to match."""
        if self.refused is not None:
            return [f"  trim     refused: {self.refused}"]
        lines = [
            f"  trim     window={self.window} edge={self.edge} trimmed={len(self.trimmed)} "
            f"restored={len(self.restored)} skipped={len(self.skipped)} rot={len(self.rot)}"
        ]
        for rel in self.lined:
            lines.append(f"  trim     line written, file kept: {rel}")
        for rel in self.restored:
            lines.append(f"  trim     restore line: {rel}")
        for line in self.skipped:
            lines.append(f"  trim     skipped {line}")
        for finding in self.rot:
            lines.append(
                f"  trim     rot {finding.partition}: the bucket's version {finding.version_id} "
                f"hashes to {finding.bucket_sha256}, and {finding.local}"
            )
        for line in self.held:
            lines.append(f"  trim     held {line}")
        if self.deadline is not None:
            lines.append(f"  trim     deadline: {self.deadline}")
        if self.stopped is not None:
            lines.append(f"  trim     stopped: {self.stopped}")
        return lines


def rot_page_body(findings: Sequence[RotFinding]) -> str:
    """The one page a run sends for every rotted bucket copy it met.

    One page rather than one per partition, for the reason ``compact._page_drift`` gives: a
    wide fault would spend the publisher's daily cap on one fact. It names up to
    :data:`ROT_PAGE_CAP` partitions, each with its bucket version and whether the lake's
    copy matches the manifest, counts the rest, and gives the repair, which is to put the
    lake's good copy back as a new version. The trim itself never writes to the bucket.
    """
    named = []
    for finding in findings[:ROT_PAGE_CAP]:
        named.append(f"{finding.partition} (version {finding.version_id}, {finding.local})")
    more = len(findings) - ROT_PAGE_CAP
    tail = f" and {more} more" if more > 0 else ""
    return (
        f"{len(findings)} chains partition(s) in the bucket no longer hash to their manifest "
        f"sha256, so the trim kept them: {'; '.join(named)}{tail}. Where the lake's copy "
        "matches, put it back as a new version with the README's put-object repair. Where it "
        "differs too, no good copy is left on either side."
    )


# -- the clauses -------------------------------------------------------------------


def tonight_refusal(clock: Clock, calendar: Calendar) -> str | None:
    """Why tonight is not a night to trim, or ``None`` when it is.

    Tonight has to be a session whose option-close deadline has passed, judged on the whole
    minute the way ``compact._sweep_scope`` judges a day eligible to seal. ``is_session`` is
    asked first, because ``SessionClock.bounds`` raises off a session.
    """
    session = SessionClock(clock, calendar)
    today = session.session_date()
    if not calendar.is_session(today):
        return f"{today.isoformat()} is not a session, so nothing was sealed tonight to count from"
    deadline = session.bounds(today).option_close_deadline
    if session.snap_slot() <= deadline:
        return (
            f"tonight's option-close deadline, {deadline.isoformat()}, has not passed, so "
            "tonight is not sealed yet. The next close+15 compaction trims"
        )
    return None


def _restored_before(line: Mapping[str, Any], session_day: date) -> bool:
    """Whether a restore line's stamp, as an Eastern date, falls before ``session_day``.

    A stamp that is missing, not text, unparseable or without an offset answers no, which
    keeps the partition.
    """
    stamp = line.get(trimmed.RESTORED_AT_FIELD)
    if not isinstance(stamp, str):
        return False
    try:
        moment = datetime.fromisoformat(stamp)
    except ValueError:
        return False
    if moment.tzinfo is None or moment.utcoffset() is None:
        return False
    return moment.astimezone(MARKET_TZ).date() < session_day


def segments_remain(paths: LakePaths, ref: PartitionRef) -> bool:
    """Whether the ticker-day's journal directory holds a segment. Fails closed.

    A directory that is absent holds none. One that exists and cannot be listed answers yes,
    the rule ``splits._segments_remain`` keeps, because nothing then says the segments are
    gone.
    """
    directory = paths.segment_dir(ref.surface, ref.ticker, ref.day)
    try:
        with os.scandir(directory) as entries:
            names = [entry.name for entry in entries]
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return any(fnmatchcase(name, SEGMENT_GLOB) for name in names)


def _present(path: Path) -> bool:
    """Whether a file exists at ``path``. A path that cannot be checked raises ``OSError``."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


def describe(root: Path, exc: BaseException) -> str:
    """An exception as ``Class: message``, with each path under the lake root made relative.

    The compaction log is read beside other hosts' output, and ``runway.walk`` already names a
    refused path relative to the lake root rather than absolutely. An ``OSError`` prints its
    absolute filename, so the root's own spellings are stripped from the text, the longest
    first, because a resolved ``/private/tmp/x`` contains the unresolved ``/tmp/x``.
    """
    text = f"{type(exc).__name__}: {exc}"
    bases = {str(root)}
    try:
        bases.add(str(root.resolve()))
    except OSError:
        pass
    for base in sorted(bases, key=len, reverse=True):
        text = text.replace(base + os.sep, "").replace(base, "the lake root")
    return text


def _ticker_repair(ticker_dir: str) -> str:
    """The hand repair for a ticker directory the trim cannot search or write."""
    return (
        f"Give the directory back to the lake's owner, as in chown <lake owner> "
        f"<lake_root>/{ticker_dir} and chmod u+rwx <lake_root>/{ticker_dir}"
    )


# -- the run -----------------------------------------------------------------------


class _Stop(Exception):
    """A failure that would repeat on every partition, carrying the result line."""


@dataclass
class _Run:
    """The state of one trim, built up partition by partition."""

    root: Path
    clock: Clock
    trimmed: list[str] = field(default_factory=list)
    lined: list[str] = field(default_factory=list)
    restored: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    rot: list[RotFinding] = field(default_factory=list)
    held: list[str] = field(default_factory=list)
    held_tickers: set[str] = field(default_factory=set)

    def stamp(self) -> str:
        return self.clock.now().astimezone(MARKET_TZ).isoformat()

    def append(self, line: Mapping[str, Any]) -> None:
        """Append one ledger line and make it durable, or stop the run.

        Every exception counts, because every one leaves the ledger in a state nothing may act
        on: a torn tail, a line that did not read back, a manifest that will not read, a row
        count that went backwards, or a disk that refused the write.
        """
        from lake.compact import _durable, _durable_dir

        try:
            trimmed.append_trimmed(
                self.root, line, source=TRIM_SOURCE, fetched_at=self.clock.now().isoformat()
            )
            _durable(trimmed.trimmed_path(self.root))
            _durable_dir(self.root)
        except Exception as exc:
            raise _Stop(
                f"the trimmed ledger refused a line for {line.get(trimmed.PARTITION_FIELD)} "
                f"({describe(self.root, exc)}). Nothing more is trimmed until the ledger is "
                "repaired by hand under the lock, then the next close+15 carries on"
            ) from exc

    def hold(self, ticker: str, line: str) -> None:
        """Hold the rest of ``ticker`` for this run, naming why and the repair."""
        self.held_tickers.add(ticker)
        self.held.append(line)

    def restore(self, rel: str, sha256: str) -> None:
        """Supersede a trim line beside a present file that a clause now keeps."""
        self.append(trimmed.restore_line(rel, sha256=sha256, restored_at=self.stamp()))
        self.restored.append(rel)


def trim(
    lake_root: Path | str,
    *,
    window: int,
    client: Any,
    target: BucketTarget,
    upload: UploadSummary,
    clock: Clock,
    calendar: Calendar,
) -> TrimResult:
    """Drop every chains partition the seven clauses allow, oldest first. Never raises.

    ``window`` is the judged window key. ``upload`` is the nightly upload's summary, whose
    ``deadline`` the run stops at and whose ``watermark`` clause 3 compares with. The caller
    holds the lake-root lock.
    """
    root = Path(lake_root)
    run = _Run(root=root, clock=clock)
    edge: date | None = None

    def result(**kwargs: Any) -> TrimResult:
        return TrimResult(
            window=window,
            edge=edge,
            trimmed=tuple(run.trimmed),
            lined=tuple(run.lined),
            restored=tuple(run.restored),
            skipped=tuple(run.skipped),
            rot=tuple(run.rot),
            held=tuple(run.held),
            **kwargs,
        )

    try:
        refusal = tonight_refusal(clock, calendar)
        if refusal is not None:
            return TrimResult(window=window, refused=refusal)
        if upload.deadline is None or upload.watermark is None:
            return TrimResult(
                window=window,
                refused="the upload carried no deadline or watermark, so nothing proves what "
                "the bucket held before tonight",
            )
        today = SessionClock(clock, calendar).session_date()
        selection = _select(root, today=today, window=window, calendar=calendar, upload=upload)
        if isinstance(selection, str):
            return TrimResult(window=window, refused=selection)
        edge = selection.edge
        for ticker, line in selection.held:
            run.hold(ticker, line)
        for candidate in selection.candidates:
            if clock.now() >= upload.deadline:
                return result(
                    deadline=f"reached at {upload.deadline.isoformat()}, the upload's own, so "
                    "the rest waits for the next night"
                )
            if candidate.ref.ticker in run.held_tickers:
                continue
            _one(run, candidate, selection, client=client, target=target)
    except _Stop as stop:
        return result(stopped=str(stop))
    except Exception as exc:
        return result(stopped=f"an unforeseen {describe(root, exc)}. The next close+15 tries again")
    return result()


@dataclass(frozen=True)
class _Candidate:
    """A present chains partition the trim looks at, with what decides it."""

    rel: str
    ref: PartitionRef
    sha256: str
    # Whether its latest trimmed line is a trim line, which a crash before the unlink leaves.
    recovering: bool
    # The first clause that answered a definite no without a bucket read, or ``None``.
    kept_by: str | None


@dataclass(frozen=True)
class _Selection:
    edge: date
    candidates: tuple[_Candidate, ...]
    paths: LakePaths
    # Each ticker whose directory could not be checked, with the line that names its repair.
    held: tuple[tuple[str, str], ...] = ()


def _select(
    root: Path, *, today: date, window: int, calendar: Calendar, upload: UploadSummary
) -> _Selection | str:
    """Read the ledgers once and list the partitions to look at, oldest first, or refuse.

    The ledgers are read here, after the upload, because compaction's own manifest read holds
    partial entries for tonight's seals and predates the ledger repair.
    """
    try:
        latest = latest_entries(root)
        ledger = read_ledger(root)
    except Exception as exc:
        return f"the manifest could not be read ({describe(root, exc)}). Repair it by hand"
    try:
        checkpoint = read_checkpoint(root)
    except CheckpointUnreadable as exc:
        return (
            f"the split checkpoint at {CHECKPOINT_PARTITION} {exc.reason}. The 18:30 sweep "
            "rewrites it, and the trim waits for that"
        )
    if checkpoint is None:
        return "no split checkpoint exists yet. The 18:30 sweep writes one, and the trim waits"
    refusal = _checkpoint_refusal(root, checkpoint, latest, today)
    if refusal is not None:
        return refusal
    try:
        edge = window_edge(calendar, today, window + 1)
    except EdgeNotFound as exc:
        return f"the window edge could not be counted: {exc}"
    try:
        trimmed_latest = trimmed.latest_trimmed(root)
        quarantine = latest_quarantine(root)
    except Exception as exc:
        return (
            f"a ledger could not be read ({describe(root, exc)}). Repair it by hand, "
            "and the next close+15 trims"
        )
    paths = LakePaths(root)
    cutoffs = checkpoint.cutoffs()
    candidates: list[_Candidate] = []
    held: dict[str, str] = {}
    for rel, entry in latest.items():
        ref = parse_partition_rel(rel)
        if ref is None or ref.surface != CHAINS or ref.ticker in held:
            continue
        try:
            present = _present(root / rel)
        except OSError as exc:
            # The fault is the ticker's directory, so it holds that ticker and no other, the
            # way a refused unlink does.
            held[ref.ticker] = _unsearchable(rel, ref, exc)
            continue
        if not present:
            continue
        line = trimmed_latest.get(rel)
        kind = None if line is None else line.get(trimmed.KIND_FIELD)
        if kind is not None and kind != trimmed.TRIM_KIND:
            # A restore line holds the partition until a checkpoint from a session after the
            # restore's day. A line of any other kind is not one this code wrote, so it holds
            # the partition too.
            if kind != trimmed.RESTORE_KIND or not _restored_before(line, checkpoint.session_day):
                continue
        position = ledger.last.get(rel)
        if position is None or position >= upload.watermark:
            continue
        cutoff = cutoffs.get(ref.ticker)
        kept_by = None
        if ref.day > edge:
            kept_by = "inside the window"
        elif cutoff is None or ref.day > cutoff:
            kept_by = "past its ticker's checkpoint cutoff"
        elif is_quarantined(quarantine.get(rel)):
            kept_by = "withheld by the quarantine ledger"
        recovering = kind == trimmed.TRIM_KIND
        if kept_by is not None and not recovering:
            continue
        candidates.append(
            _Candidate(
                rel=rel,
                ref=ref,
                sha256=str(entry.get("sha256")),
                recovering=recovering,
                kept_by=kept_by,
            )
        )
    candidates = [item for item in candidates if item.ref.ticker not in held]
    candidates.sort(key=lambda item: (item.ref.day, item.ref.ticker))
    return _Selection(
        edge=edge, candidates=tuple(candidates), paths=paths, held=tuple(held.items())
    )


def _unsearchable(rel: str, ref: PartitionRef, exc: OSError) -> str:
    """The held line for a ticker whose partition could not be checked for presence."""
    ticker_dir = PurePosixPath(rel).parent.as_posix()
    code = errno.errorcode.get(exc.errno or 0, type(exc).__name__)
    if exc.errno in _TICKER_ERRNOS:
        repair = _ticker_repair(ticker_dir)
    else:
        repair = "Check the volume, and the next close+15 carries on"
    return f"{ticker_dir}/: {rel} could not be checked ({code}), so {ref.ticker} waits. {repair}"


def _checkpoint_refusal(
    root: Path, checkpoint: Checkpoint, latest: Mapping[str, Mapping], today: date
) -> str | None:
    """Why the checkpoint cannot be trimmed by tonight, or ``None``.

    ``read_checkpoint`` does not verify the sha, so the file's bytes are compared with its
    latest manifest entry here. A missing entry drops nothing. Tonight has to be strictly after
    the checkpoint's own session, so a second compaction after tonight's sweep cannot trim by
    tonight's checkpoint again.
    """
    entry = latest.get(CHECKPOINT_PARTITION)
    if entry is None:
        return (
            f"{CHECKPOINT_PARTITION} has no manifest entry, so nothing proves its bytes. The "
            "18:30 sweep records one when it writes the file"
        )
    try:
        found = sha256_file(checkpoint_path(root))
    except OSError as exc:
        return f"{CHECKPOINT_PARTITION} could not be hashed ({type(exc).__name__})"
    if found != entry.get("sha256"):
        return (
            f"{CHECKPOINT_PARTITION} does not hash to its manifest entry, so its cutoffs are "
            "not the ones the sweep wrote. The next 18:30 sweep rewrites it"
        )
    if not today > checkpoint.session_day:
        return (
            f"the checkpoint is tonight's, {checkpoint.session_day.isoformat()}, and a trim runs "
            "only on a session after the checkpoint's own. The next close+15 trims"
        )
    return None


def _one(
    run: _Run, candidate: _Candidate, selection: _Selection, *, client: Any, target: BucketTarget
) -> None:
    """Decide and act on one candidate. Raises ``_Stop`` for a failure that would repeat."""
    rel = candidate.rel
    kept_by = candidate.kept_by
    if kept_by is None and segments_remain(selection.paths, candidate.ref):
        kept_by = "its journal still holds segments"
    if kept_by is not None:
        # A recovery supersedes its trim line with a restore line. Any other candidate has no
        # trim line to supersede, so the file stays and nothing is written for it.
        if candidate.recovering:
            run.restore(rel, candidate.sha256)
        return
    ticker_dir = (run.root / rel).parent
    if not os.access(ticker_dir, os.W_OK | os.X_OK):
        # The unlink would be refused after the line landed, and the next night would append
        # another line for the same file. Checking first writes nothing until it is repaired.
        name = ticker_dir.relative_to(run.root).as_posix()
        run.hold(
            candidate.ref.ticker,
            f"{name}/: the directory is not writable and searchable, so the rest of "
            f"{candidate.ref.ticker} waits. {_ticker_repair(name)}",
        )
        return
    try:
        found = current_digest(client, target, rel)
    except BucketReadError as exc:
        if exc.kind in ("refused", "unreachable"):
            raise _Stop(
                f"the bucket {exc.kind} the read of {rel} ({exc.code}). Every partition would "
                "meet the same, so the trim stopped. Check the role's s3:GetObject and the "
                "network, and the next close+15 carries on"
            ) from exc
        if exc.absent:
            run.skipped.append(
                f"{rel}: the bucket holds no such key, so the lake keeps its only copy. The "
                "Sunday bucket scrub names it"
            )
            if candidate.recovering:
                run.restore(rel, candidate.sha256)
            return
        run.skipped.append(f"{rel}: the bucket answered the read with {exc.code}")
        return
    if not usable_version_id(found.version_id):
        raise _Stop(
            f"the bucket's read of {rel} named no version, so the bucket is not versioned and a "
            "trim line would record nothing a repair could use. Turn versioning on"
        )
    if found.sha256 != candidate.sha256:
        try:
            local = sha256_file(run.root / rel)
        except OSError as exc:
            verdict = f"the lake's copy could not be read ({type(exc).__name__})"
        else:
            verdict = LOCAL_GOOD if local == candidate.sha256 else LOCAL_ROTTED
        run.rot.append(
            RotFinding(
                partition=rel,
                manifest_sha256=candidate.sha256,
                bucket_sha256=found.sha256,
                version_id=found.version_id,
                local=verdict,
            )
        )
        if candidate.recovering:
            run.restore(rel, candidate.sha256)
        return
    verified = run.stamp()
    run.append(
        trimmed.trim_line(
            rel,
            sha256=candidate.sha256,
            version_id=found.version_id,
            verified_at=verified,
            trimmed_at=run.stamp(),
        )
    )
    try:
        (run.root / rel).unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        run.lined.append(rel)
        ticker_dir = (run.root / rel).parent.relative_to(run.root).as_posix()
        if exc.errno in _TICKER_ERRNOS:
            code = errno.errorcode.get(exc.errno, str(exc.errno))
            run.hold(
                candidate.ref.ticker,
                f"{ticker_dir}/: the unlink of {rel} was refused ({code}), "
                f"so the rest of {candidate.ref.ticker} waits. {_ticker_repair(ticker_dir)}",
            )
            return
        raise _Stop(
            f"the unlink of {rel} failed ({describe(run.root, exc)}), so the file stays "
            "beside its trim line and reads as present. Repair the volume, and the next "
            "close+15 carries on"
        ) from exc
    run.trimmed.append(rel)


__all__ = [
    "LOCAL_GOOD",
    "LOCAL_ROTTED",
    "ROT_PAGE_CAP",
    "TRIM_SOURCE",
    "RotFinding",
    "TrimResult",
    "describe",
    "rot_page_body",
    "segments_remain",
    "tonight_refusal",
    "trim",
]
