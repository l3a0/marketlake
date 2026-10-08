"""The trimmed ledger: the record that tells a partition dropped on purpose from one lost.

Marketlake #755 keeps only a window of recent sessions on the hosted VM's lake volume, and
drops an older partition once its copy in the backup bucket is verified. Chains partitions are
trimmed first, and the owner means every dated surface to be trimmed at the same cutoff in time,
so nothing here assumes a surface. A line is keyed by the lake-relative ``partition``, which is
any path ``paths.parse_partition_rel`` reads, ``chains`` and ``quotes`` today. Before
anything deletes a sealed partition, the lake has to be able to say that a missing file was
removed on purpose. Without a record, every reader that walks the manifest reads a trimmed
partition as loss: the Sunday scrub withholds its ping every week, the battery's coverage check
reports the session missing every night, and the first upload refuses to re-baseline the bucket.

This module is that record and nothing that writes into it on a schedule. Marketlake #787 trims
and writes a trim line. Marketlake #784 restores a range and writes a restore line. Both write
through :func:`append_trimmed`, and every reader decides through :func:`is_designed_absence`.

The ledger is ``trimmed.jsonl`` at the lake root, the fourth append-only ledger beside
``manifest.jsonl``, ``quarantine.jsonl`` and ``actions/corporate_actions.jsonl``. It follows
the manifest's line rules: one entry is one line in a single ``O_APPEND`` write, and the last
line per partition wins. It carries its own manifest entry, the way ``quarantine.jsonl`` does,
so the Sunday scrub checks its sha like any sealed file.

It holds two kinds of line, told apart by ``kind``.

1. A **trim line** records that a partition was removed on purpose. It carries the partition,
   the sha256 of the bytes removed, the bucket ``version_id`` that was verified to hold them,
   when that copy was verified, and when the file was removed.
2. A **restore line** supersedes a trim line for the same partition. It is never a deletion of
   the trim line, for the reason un-quarantine is a superseding entry.

There is no kind that holds a partition back from the next trim. The owner decided on
2026-10-07 (decision 5 on marketlake #755) that reading trimmed data restores the range into a
directory outside the live lake, so nothing in the live lake needs such a hold.

A **designed absence** is a partition whose latest manifest sha equals the sha on its latest
trimmed line, where that latest line is a trim line. A trim line beside a file that is still
present, which a crash between the line and the unlink leaves, reads as present, because callers
ask only about a file they already found absent. Any other missing file is still lost.

Times are injected. Every stamp is passed in by the caller as text. Nothing here reads a clock.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from lake.manifest import (
    ManifestError,
    RowCountRegression,
    _append_once,
    _decode,
    _line,
    _refuse_hidden_entries,
    latest_entries,
    parse_jsonl,
    record_partition,
    sha256_bytes,
)
from lake.paths import MANIFEST_FILE, TRIMMED_FILE

# The field that tells the two kinds of line apart, and its two values. A line whose kind is
# neither is not a trim line, so it never makes an absence designed, which fails closed.
KIND_FIELD = "kind"
TRIM_KIND = "trim"
RESTORE_KIND = "restore"

# The fields a line carries beside ``kind``. ``partition`` and ``sha256`` are on both kinds.
PARTITION_FIELD = "partition"
SHA256_FIELD = "sha256"
VERSION_ID_FIELD = "version_id"
VERIFIED_AT_FIELD = "verified_at"
TRIMMED_AT_FIELD = "trimmed_at"
RESTORED_AT_FIELD = "restored_at"


# What this ledger loses to each refusal its reader shares with the quarantine ledger. The
# refusal texts in ``lake.manifest`` name verdicts by default, and a refusal's text becomes a
# Sunday problem line word for word, so this ledger supplies its own.
_UTF8_CONSEQUENCE = (
    "Every line in this file is unreadable until that byte is repaired, so this ledger cannot "
    "say which partitions were trimmed on purpose, and every absent partition reads as lost."
)
_HIDDEN_CONSEQUENCE = (
    "Every line behind that line is invisible, so this ledger cannot say which partitions were "
    "trimmed on purpose, and every absent partition reads as lost."
)
_MARK_CONSEQUENCE = (
    "Read past it, a line is either discarded or filed under a name no reader asks about, so "
    "this ledger cannot say which partitions were trimmed on purpose."
)


class TrimmedAppendRefused(ManifestError):
    """Raised when a strict append to the trimmed ledger cannot prove its line landed whole.

    It is a ``ManifestError`` so that a writer's existing containment of a damaged ledger
    catches it with no new tuple.
    """


class TrimmedTornTail(TrimmedAppendRefused):
    """The ledger's last line has no terminating newline, so the next line would fuse onto it."""


class TrimmedLineLost(TrimmedAppendRefused):
    """The line was written and does not read back as the ledger's last line."""


class TrimmedRepairRefused(ManifestError):
    """Raised when :func:`repair_trimmed_entry` cannot bring the ledger's entry back into step.

    It is a ``ManifestError`` so that a caller's existing containment of a damaged ledger
    catches it with no new tuple. Its text names the hand repair.
    """


def trimmed_path(lake_root: Path) -> Path:
    """The trimmed-ledger path for a lake, derived from its root."""
    return Path(lake_root) / TRIMMED_FILE


# -- the line schema ---------------------------------------------------------


def trim_line(
    partition: str,
    *,
    sha256: str,
    version_id: str,
    verified_at: str,
    trimmed_at: str,
) -> dict:
    """A trim line: ``partition``, whose bytes hashed to ``sha256``, was removed on purpose.

    ``version_id`` is the bucket version verified to hold those bytes, and ``verified_at`` is
    when. ``trimmed_at`` is when the file was removed.
    """
    return {
        KIND_FIELD: TRIM_KIND,
        PARTITION_FIELD: partition,
        SHA256_FIELD: sha256,
        VERSION_ID_FIELD: version_id,
        VERIFIED_AT_FIELD: verified_at,
        TRIMMED_AT_FIELD: trimmed_at,
    }


def restore_line(partition: str, *, sha256: str, restored_at: str) -> dict:
    """A restore line: ``partition`` is back on disk with these bytes, superseding its trim."""
    return {
        KIND_FIELD: RESTORE_KIND,
        PARTITION_FIELD: partition,
        SHA256_FIELD: sha256,
        RESTORED_AT_FIELD: restored_at,
    }


# -- reading -----------------------------------------------------------------


def read_trimmed(lake_root: Path) -> list[dict]:
    """Every trimmed-ledger line in file order, with a torn trailing line discarded.

    It refuses a damaged file the way ``manifest.read_quarantine`` does, with this ledger's own
    words. A read that stops in the body raises ``TornLedger``, bytes that do not decode raise
    ``LedgerNotUtf8``, and a byte-order mark raises ``LedgerHasByteOrderMark``. All three are a
    ``ManifestError``.

    A torn trailing line is discarded rather than refused. Every writer goes through
    :func:`append_trimmed`, which reads its line back before returning, so a trim line whose
    write tore never reaches the unlink that would make its partition absent. Discarding it
    therefore leaves that partition present, which is what the disk says.

    A missing ledger reads as no lines, which keeps every reader byte-identical on a host that
    never trims.
    """
    path = trimmed_path(lake_root)
    if not path.exists():
        return []
    text = _decode(
        path,
        path.read_bytes(),
        consequence=_UTF8_CONSEQUENCE,
        mark_consequence=_MARK_CONSEQUENCE,
    )
    entries = parse_jsonl(text)
    _refuse_hidden_entries(path, text, entries, consequence=_HIDDEN_CONSEQUENCE)
    return entries


def latest_by_partition(lines: Sequence[Mapping], path: Path | str = TRIMMED_FILE) -> dict:
    """Each partition's latest line, from lines in file order. Last line wins.

    A line that names no partition, or a partition that cannot be a key, raises
    ``ManifestError`` naming the line's position, for the reason
    ``manifest._latest_by_partition`` gives: a reader that stepped over damage in a ledger a
    guard resolves would make every check downstream weaker than it reads.

    It takes lines rather than a lake root, so a reader of the bucket's copy of the ledger
    resolves it the same way.
    """
    latest: dict = {}
    for position, line in enumerate(lines, start=1):
        try:
            partition = line[PARTITION_FIELD]
        except (KeyError, TypeError) as exc:
            raise ManifestError(f"{path}: line {position} names no partition") from exc
        try:
            latest[partition] = line
        except TypeError as exc:
            raise ManifestError(
                f"{path}: line {position} has a partition that cannot be a key: {partition!r}"
            ) from exc
    return latest


def latest_trimmed(lake_root: Path) -> dict:
    """Each partition's latest trimmed-ledger line, read from the lake."""
    return latest_by_partition(read_trimmed(lake_root), trimmed_path(lake_root))


def is_designed_absence(
    partition: str,
    manifest_latest: Mapping[str, Mapping],
    trimmed_latest: Mapping[str, Mapping],
) -> bool:
    """Whether an absent ``partition`` was trimmed on purpose rather than lost.

    It is, exactly when its latest trimmed line is a trim line whose sha256 equals the sha256 on
    its latest manifest entry. A later restore line, a sha that differs, no manifest entry and no
    trimmed line each read as lost.

    **This takes data and never stats a file.** ``manifest_latest`` is the manifest's latest
    entry per partition and ``trimmed_latest`` is :func:`latest_by_partition`'s answer. So a
    rebuild can ask it about the bucket's copies of both ledgers, where no local file exists.
    Callers ask it only about a file they already found absent. A trim line beside a present file
    is what a crash between the line and the unlink leaves, and that file is present.

    The sha comparison is what keeps a re-sealed partition honest. Should a partition be trimmed
    and later receive a new manifest entry with different bytes, the old trim line no longer
    explains its absence.
    """
    line = trimmed_latest.get(partition)
    if line is None or line.get(KIND_FIELD) != TRIM_KIND:
        return False
    entry = manifest_latest.get(partition)
    if entry is None:
        return False
    sha256 = line.get(SHA256_FIELD)
    return sha256 is not None and sha256 == entry.get(SHA256_FIELD)


# -- the strict append -------------------------------------------------------


def _refuse_torn_tail(path: Path, action: str) -> None:
    """Raise :class:`TrimmedTornTail` when the ledger's last line is not one whole entry.

    Two shapes refuse, and both are a line the next append would fuse onto or a refresh would
    certify.

    1. A non-empty file that does not end in a newline. That is the shape a crash mid-write
       leaves.
    2. A last non-blank line that ends in a newline and does not parse as a JSON object. That is
       the shape a write that lost bytes mid-line and kept its newline leaves. ``parse_jsonl``
       drops an unparseable last line without a word, so nothing else would notice it: a refresh
       would certify it and the Sunday scrub would then pass over a garbage line.

    An empty file refuses nothing, because it holds no line to fuse onto. ``action`` names what
    was refused, so the message says whether a line or an entry refresh stopped.
    """
    if not path.exists():
        return
    raw = path.read_bytes()
    if not raw:
        return
    if not raw.endswith(b"\n"):
        torn = len(raw) - raw.rfind(b"\n") - 1
        raise TrimmedTornTail(
            f"{path}: the last line has no terminating newline, so it is a torn write, and "
            f"{action} would fuse onto it or certify it. The {torn} bytes after the last "
            "newline are the torn line. Repair by hand under the lock: delete those bytes, then "
            "re-record the ledger's manifest entry."
        )
    lines = [line for line in raw.splitlines() if line.strip()]
    if not lines:
        return
    try:
        last = json.loads(lines[-1])
    except ValueError:
        # ``json.loads`` raises ``JSONDecodeError`` on text that does not parse and
        # ``UnicodeDecodeError`` on bytes that do not decode. Both are a ``ValueError``.
        last = None
    if not isinstance(last, dict):
        raise TrimmedTornTail(
            f"{path}: the last line ends in a newline and does not parse as one JSON object, so "
            f"it is a write that lost bytes, and {action} would certify it. The reader discards "
            "that line without a word. Repair by hand under the lock: make the last line whole "
            "or remove it, then re-record the ledger's manifest entry."
        )


def append_trimmed(lake_root: Path, line: Mapping, *, source: str, fetched_at: str | None) -> dict:
    """Append one trim or restore line, read it back, and refresh the ledger's manifest entry.

    **The caller holds the lake-root lock.** That is what makes this stricter than
    ``manifest.append_line`` safe. ``append_line`` reads nothing first, because another writer
    could interleave between the read and the write. Every writer of this ledger holds
    ``lake_lock``, so no one can. Two checks follow from that.

    1. A ledger whose last line is not one whole entry refuses before anything is written,
       which :func:`_refuse_torn_tail` decides. ``read_trimmed`` discards a torn trailing line,
       so a line appended behind one fuses onto it and is lost while the append reports success.
       On this ledger the lost line would be a trim line whose unlink then runs, an absence
       nothing explains.
    2. The line has to read back as one new last line before this returns, the way
       ``lake.signoff`` reads its sign-off back: the ledger holds exactly one line more than it
       did, and the last one is this line. A write that landed short, changed, or not at all is
       refused here, before the caller unlinks anything. The count matters when an identical
       line is already last, because that line alone would read back as this one.

    Then the ledger's manifest entry is refreshed by :func:`refresh_trimmed_entry`, so the
    Sunday scrub and the nightly upload see a sha that matches the bytes. ``source`` names the
    writer on that entry and ``fetched_at`` stamps it.
    """
    root = Path(lake_root)
    path = trimmed_path(root)
    entry = dict(line)
    _refuse_torn_tail(path, "a new line")
    before = len(read_trimmed(root))
    _append_once(path, _line(entry))
    lines = read_trimmed(root)
    # The count is what tells this line from an identical one already last. A write that landed
    # nothing, or landed a fragment the reader discards, leaves the earlier line last.
    if len(lines) != before + 1 or lines[-1] != entry:
        raise TrimmedLineLost(
            f"{path}: a line for {entry.get(PARTITION_FIELD)!r} was appended and does not read "
            "back as one new last line, so the write landed short or changed. Nothing may act "
            "on it. Repair by hand under the lock: make the last line whole or remove it, then "
            "re-record the ledger's manifest entry."
        )
    # The read-back stops at the first line it cannot parse, so bytes landed behind the line
    # would pass it. ``_record`` checks no tail of its own, so the tail is checked once more
    # here rather than certified.
    _refuse_torn_tail(path, "recording the ledger's manifest entry")
    _record(root, rows=len(lines), source=source, fetched_at=fetched_at)
    return entry


def refresh_trimmed_entry(lake_root: Path, *, source: str, fetched_at: str | None) -> dict:
    """Re-record the trimmed ledger's manifest entry from its bytes on disk. Caller holds the lock.

    The entry's ``rows`` is the number of lines, which only grows, so ``record_partition``'s
    row-count guard refuses a ledger that lost lines.

    **A torn or garbled tail refuses here too**, by :func:`_refuse_torn_tail`'s two shapes.
    Re-recording the entry over a torn last line would certify the fragment, and the next append
    would fuse onto it with a matching sha. Re-recording it over a whole line that does not parse
    would certify a line the reader drops without a word. So a repair that refreshes the entry has
    to make the last line whole first.
    """
    root = Path(lake_root)
    _refuse_torn_tail(trimmed_path(root), "re-recording its manifest entry")
    lines = read_trimmed(root)
    return _record(root, rows=len(lines), source=source, fetched_at=fetched_at)


# What the operator does about a ledger whose entry could not be repaired, for every cause that
# does not carry its own repair. ``TrimmedTornTail`` and ``TrimmedLineLost`` already name one.
_REPAIR_BY_HAND = (
    "Repair by hand under the lock: make the ledger's last line whole or remove it, then "
    "re-record its manifest entry with lake.trimmed.refresh_trimmed_entry."
)


def _prefix_end(raw: bytes, rows: int) -> int | None:
    """The byte offset just past the ``rows``-th non-blank line of ``raw``, or ``None``.

    Lines are counted the way the reader counts entries, a blank line counting for nothing, so
    the offset is where the ledger stood when an entry recording ``rows`` lines was written.
    ``None`` means ``raw`` holds fewer than ``rows`` non-blank lines.
    """
    if rows <= 0:
        return 0
    count = 0
    offset = 0
    for piece in raw.split(b"\n"):
        offset += len(piece) + 1
        if piece.strip():
            count += 1
            if count == rows:
                return min(offset, len(raw))
    return None


def _lost_lines(path: Path, held: int, recorded: int) -> TrimmedRepairRefused:
    return TrimmedRepairRefused(
        f"{path}: the ledger holds {held} line(s) and its manifest entry records {recorded}, "
        "so lines were lost and the entry was not re-recorded. Repair by hand under the lock: "
        "recover the ledger from the bucket's copy, then re-record its manifest entry with "
        "lake.trimmed.refresh_trimmed_entry."
    )


def repair_trimmed_entry(lake_root: Path, *, source: str, fetched_at: str | None) -> bool:
    """Re-record the ledger's manifest entry when lines landed after it and nothing else changed.

    **The caller holds the lake-root lock.** This is the one repair for a ledger whose line
    landed without its entry, which a crash inside :func:`append_trimmed` leaves, between the
    append and the record. Left alone, the Sunday scrub reports the ledger's sha as mismatched,
    and the nightly upload either sends the old bytes' entry without an error, when that entry
    is behind the bucket's watermark, or raises ``ChecksumRefused`` when it is past it. The
    range restore in :mod:`lake.bucket` calls it at the start of every run, and marketlake #787's
    compaction calls it before the backup.

    It returns ``True`` when it re-recorded the entry and ``False`` when nothing needed it.

    1. **A host with no ledger reads nothing and writes nothing.** The function returns before
       the manifest is read, so such a host stays byte-identical and fails exactly as it did.
    2. **An entry whose sha matches the bytes is left alone.**
    3. **A ledger with no entry at all is re-recorded.** A crash between the ledger's first
       append and its first record leaves that.
    4. **Otherwise only an append is re-recorded.** The bytes through the entry's ``rows``-th
       line have to hash to the entry's sha, because every crash this repairs only adds lines
       after the recorded ones. A ledger edited in place, or rotted, fails that and refuses.
       Re-recording it would bless the damage and quiet the Sunday scrub's sha check on the
       ledger for good, and compaction would do that every night. A ledger holding fewer lines
       than its entry records lost lines and refuses too.

    The re-record is :func:`refresh_trimmed_entry`, which refuses a torn or garbled last line
    rather than certify it, so this never re-records a torn tail. Re-recording one would bless
    the fragment and guarantee the next append fuses onto it with a matching sha.

    Every failure raises :class:`TrimmedRepairRefused` naming the hand repair: an edit in place,
    lost lines, a torn tail, ``TornLedger``, ``LedgerNotUtf8``, ``LedgerHasByteOrderMark``, an
    ``OSError``, and ``record_partition``'s row-count guard, which raises
    ``RowCountRegression`` rather than a ``ManifestError``. A ``manifest.jsonl`` that cannot be
    read refuses with its own text, since its repair is not the ledger's.
    """
    root = Path(lake_root)
    path = trimmed_path(root)
    try:
        if not path.exists():
            return False
    except OSError as exc:
        raise TrimmedRepairRefused(
            f"{path}: the trimmed ledger's manifest entry was not checked "
            f"({type(exc).__name__}: {exc}). {_REPAIR_BY_HAND}"
        ) from exc
    try:
        entry = latest_entries(root).get(TRIMMED_FILE)
    except (ManifestError, OSError) as exc:
        manifest = root / MANIFEST_FILE
        raise TrimmedRepairRefused(
            f"{manifest}: the manifest could not be read ({type(exc).__name__}: {exc}), so the "
            "trimmed ledger's entry was not checked. Repair manifest.jsonl by hand under the "
            "lock first, which the Sunday scrub's problem line names."
        ) from exc
    try:
        if entry is not None:
            raw = path.read_bytes()
            recorded = entry.get("sha256")
            if recorded == sha256_bytes(raw):
                return False
            rows = int(entry.get("rows") or 0)
            end = _prefix_end(raw, rows)
            if end is None:
                held = sum(1 for piece in raw.split(b"\n") if piece.strip())
                raise _lost_lines(path, held, rows)
            if sha256_bytes(raw[:end]) != recorded:
                raise TrimmedRepairRefused(
                    f"{path}: the ledger's first {rows} line(s) no longer hash to its manifest "
                    "entry, so a line was edited in place or the bytes rotted, and the entry was "
                    "not re-recorded. Repair by hand under the lock: recover the ledger from the "
                    "bucket's copy, then re-record its manifest entry with "
                    "lake.trimmed.refresh_trimmed_entry."
                )
        refresh_trimmed_entry(root, source=source, fetched_at=fetched_at)
    except TrimmedRepairRefused:
        raise
    except RowCountRegression as exc:
        raise _lost_lines(path, exc.proposed, exc.recorded) from exc
    except TrimmedAppendRefused as exc:
        raise TrimmedRepairRefused(
            f"the trimmed ledger's manifest entry was not re-recorded: {exc}"
        ) from exc
    except (ManifestError, OSError, ValueError, TypeError) as exc:
        raise TrimmedRepairRefused(
            f"{path}: the trimmed ledger's manifest entry was not re-recorded "
            f"({type(exc).__name__}: {exc}). {_REPAIR_BY_HAND}"
        ) from exc
    return True


def _record(root: Path, *, rows: int, source: str, fetched_at: str | None) -> dict:
    return record_partition(root, TRIMMED_FILE, source=source, rows=rows, fetched_at=fetched_at)


__all__ = [
    "KIND_FIELD",
    "PARTITION_FIELD",
    "RESTORED_AT_FIELD",
    "RESTORE_KIND",
    "SHA256_FIELD",
    "TRIMMED_AT_FIELD",
    "TRIM_KIND",
    "VERIFIED_AT_FIELD",
    "VERSION_ID_FIELD",
    "TrimmedAppendRefused",
    "TrimmedLineLost",
    "TrimmedRepairRefused",
    "TrimmedTornTail",
    "append_trimmed",
    "is_designed_absence",
    "latest_by_partition",
    "latest_trimmed",
    "read_trimmed",
    "refresh_trimmed_entry",
    "repair_trimmed_entry",
    "restore_line",
    "trim_line",
    "trimmed_path",
]
