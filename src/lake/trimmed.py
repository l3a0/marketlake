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

from collections.abc import Mapping, Sequence
from pathlib import Path

from lake.manifest import (
    ManifestError,
    _append_once,
    _decode,
    _line,
    _refuse_hidden_entries,
    parse_jsonl,
    record_partition,
)
from lake.paths import TRIMMED_FILE

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
    """Raise :class:`TrimmedTornTail` when the ledger is non-empty and does not end in a newline.

    ``action`` names what was refused, so the message says whether a line or an entry refresh
    stopped.
    """
    if not path.exists():
        return
    raw = path.read_bytes()
    if raw and not raw.endswith(b"\n"):
        torn = len(raw) - raw.rfind(b"\n") - 1
        raise TrimmedTornTail(
            f"{path}: the last line has no terminating newline, so it is a torn write, and "
            f"{action} would fuse onto it or certify it. The {torn} bytes after the last "
            "newline are the torn line. Repair by hand under the lock: delete those bytes, then "
            "re-record the ledger's manifest entry."
        )


def append_trimmed(lake_root: Path, line: Mapping, *, source: str, fetched_at: str | None) -> dict:
    """Append one trim or restore line, read it back, and refresh the ledger's manifest entry.

    **The caller holds the lake-root lock.** That is what makes this stricter than
    ``manifest.append_line`` safe. ``append_line`` reads nothing first, because another writer
    could interleave between the read and the write. Every writer of this ledger holds
    ``lake_lock``, so no one can. Two checks follow from that.

    1. A non-empty ledger that does not end in a newline refuses before anything is written.
       ``read_trimmed`` discards a torn trailing line, so a line appended behind one fuses onto
       it and is lost while the append reports success. On this ledger the lost line would be a
       trim line whose unlink then runs, an absence nothing explains.
    2. The line has to read back as the ledger's last line before this returns, the way
       ``lake.signoff`` reads its sign-off back. A write that landed short or changed is refused
       here, before the caller unlinks anything.

    Then the ledger's manifest entry is refreshed by :func:`refresh_trimmed_entry`, so the
    Sunday scrub and the nightly upload see a sha that matches the bytes. ``source`` names the
    writer on that entry and ``fetched_at`` stamps it.
    """
    root = Path(lake_root)
    path = trimmed_path(root)
    entry = dict(line)
    _refuse_torn_tail(path, "a new line")
    _append_once(path, _line(entry))
    lines = read_trimmed(root)
    if not lines or lines[-1] != entry:
        raise TrimmedLineLost(
            f"{path}: a line for {entry.get(PARTITION_FIELD)!r} was appended and does not read "
            "back as the last line, so the write landed short or changed. Nothing may act on "
            "it. Repair by hand under the lock: make the last line whole or remove it, then "
            "re-record the ledger's manifest entry."
        )
    _record(root, rows=len(lines), source=source, fetched_at=fetched_at)
    return entry


def refresh_trimmed_entry(lake_root: Path, *, source: str, fetched_at: str | None) -> dict:
    """Re-record the trimmed ledger's manifest entry from its bytes on disk. Caller holds the lock.

    The entry's ``rows`` is the number of lines, which only grows, so ``record_partition``'s
    row-count guard refuses a ledger that lost lines.

    **A torn tail refuses here too.** Re-recording the entry over a torn last line would
    certify the fragment, and the next append would fuse onto it with a matching sha. So a repair
    that refreshes the entry has to remove the torn bytes first.
    """
    root = Path(lake_root)
    _refuse_torn_tail(trimmed_path(root), "re-recording its manifest entry")
    lines = read_trimmed(root)
    return _record(root, rows=len(lines), source=source, fetched_at=fetched_at)


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
    "TrimmedTornTail",
    "append_trimmed",
    "is_designed_absence",
    "latest_by_partition",
    "latest_trimmed",
    "read_trimmed",
    "refresh_trimmed_entry",
    "restore_line",
    "trim_line",
    "trimmed_path",
]
