"""The manifest ledger.

The manifest is the integrity root. It records one entry per lake file that carries
data. Each entry names the file, where it came from, its sha256 checksum, its row
count, and when it was fetched. So the lake's true contents are pinned in one place,
and drift or loss becomes detectable rather than silent.

The ledger lives at ``manifest.jsonl`` at the lake root. Its rules are few and exact.

1. *One entry is one line.* An entry is appended with a single ``O_APPEND`` write.
   ``O_APPEND`` is the kernel's atomic append mode. Concurrent writers cannot
   interleave within one write, so a line is never half from one writer and half from
   another. A reader that meets a torn trailing line discards it. A crash can only
   tear the last line, never an earlier one.
2. *Last entry wins*, keyed by the file's path. A re-run legitimately appends a second
   entry for the same path. The current truth is the last entry for that path.
3. *Two-way scrub.* Every entry's file must exist and match its last recorded sha.
   And every data file in the lake must have an entry. The second direction catches a
   crash between writing a file and appending its entry.

The quarantine ledger at ``quarantine.jsonl`` follows the same three rules. It records
data-quality verdicts per partition. Un-quarantine is a superseding entry, never a
deletion. This module gives it the same append and read helpers.

The scrub reads. It never writes. Repair is a separate, deliberate, human-invoked
step under the lake-root lock. This module supplies the primitives that step and the
daily compaction job call.

Times are injected. ``fetched_at`` is passed in by the caller. Nothing here reads a
wall clock.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

# The two ledgers, both at the lake root.
MANIFEST_NAME = "manifest.jsonl"
QUARANTINE_NAME = "quarantine.jsonl"

# What the reverse scrub excludes, enumerated and not implied. The manifest cannot
# cover itself. Journal segments are manifest-less by rule, so the whole ``journal/``
# tree is out. An entry ending in ``/`` is a directory prefix. Any other entry is an
# exact filename at the lake root. The lock adds no file to skip, because it locks the
# manifest itself.
SCRUB_EXCLUSIONS: tuple[str, ...] = (MANIFEST_NAME, "journal/")


class RowCountRegression(Exception):
    """Raised when an append would shrink a manifested partition's row count.

    The standing invariant is that no automatic run ever replaces a manifested
    partition with fewer rows than its recorded count. A late segment beside a sealed
    day could otherwise rebuild a full partition from one segment, and every integrity
    layer would bless the loss.
    """

    def __init__(self, partition: str, recorded: int, proposed: int) -> None:
        super().__init__(f"{partition}: proposed {proposed} rows is fewer than recorded {recorded}")
        self.partition = partition
        self.recorded = recorded
        self.proposed = proposed


# -- paths -------------------------------------------------------------------


def manifest_path(lake_root: Path) -> Path:
    """The manifest path for a lake, derived from its root."""
    return Path(lake_root) / MANIFEST_NAME


def quarantine_path(lake_root: Path) -> Path:
    """The quarantine-ledger path for a lake, derived from its root."""
    return Path(lake_root) / QUARANTINE_NAME


# -- checksums ---------------------------------------------------------------


def sha256_file(path: Path) -> str:
    """The sha256 hex digest of a file's bytes."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# -- reading -----------------------------------------------------------------


def _parse_jsonl(text: str) -> list[dict]:
    """Parse ledger text into entries, discarding a torn trailing line.

    A blank line is skipped. The first line that does not parse ends the read. By the
    append rule only the last line can be torn, so this discards exactly the torn tail.
    """
    entries: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            # A torn trailing line is discarded, the same rule as the journal tail.
            break
    return entries


def _read_jsonl(path: Path) -> list[dict]:
    """Read a ledger file into entries. A missing file reads as empty."""
    path = Path(path)
    if not path.exists():
        return []
    return _parse_jsonl(path.read_text())


def _latest_by_partition(entries: Sequence[dict]) -> dict[str, dict]:
    """Resolve last-entry-wins per partition path over entries in file order."""
    latest: dict[str, dict] = {}
    for entry in entries:
        latest[entry["partition"]] = entry
    return latest


def read_manifest(lake_root: Path) -> list[dict]:
    """Every manifest entry in file order, with the torn trailing line discarded."""
    return _read_jsonl(manifest_path(lake_root))


def latest_entries(lake_root: Path) -> dict[str, dict]:
    """The current authoritative manifest entry per partition path."""
    return _latest_by_partition(read_manifest(lake_root))


def read_quarantine(lake_root: Path) -> list[dict]:
    """Every quarantine entry in file order, with the torn trailing line discarded."""
    return _read_jsonl(quarantine_path(lake_root))


def latest_quarantine(lake_root: Path) -> dict[str, dict]:
    """The current authoritative quarantine verdict per partition path."""
    return _latest_by_partition(read_quarantine(lake_root))


# -- appending ---------------------------------------------------------------


def _append_line(path: Path, entry: dict) -> None:
    """Append one entry as exactly one line via a single ``O_APPEND`` write.

    ``sort_keys`` keeps the on-disk bytes stable across callers. The line is written
    in one ``os.write`` so it cannot interleave with a concurrent append.
    """
    line = (json.dumps(entry, sort_keys=True) + "\n").encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def would_shrink(lake_root: Path, partition: str, rows: int) -> bool:
    """Whether recording ``rows`` for ``partition`` is fewer than its recorded count."""
    current = latest_entries(lake_root).get(partition)
    return current is not None and rows < current["rows"]


def guard_row_count(lake_root: Path, partition: str, rows: int) -> None:
    """Refuse an append that would shrink a manifested partition. Raises on regression."""
    current = latest_entries(lake_root).get(partition)
    if current is not None and rows < current["rows"]:
        raise RowCountRegression(partition, current["rows"], rows)


def append_manifest(
    lake_root: Path,
    *,
    partition: str,
    source: str,
    sha256: str,
    rows: int,
    fetched_at: str | None,
    guard: bool = True,
) -> dict:
    """Append one manifest entry and return it.

    The entry shape matches the fixture-lake builder: ``partition``, ``source``,
    ``sha256``, ``rows``, ``fetched_at``. With ``guard`` on, the standing row-count
    invariant is enforced first. A deliberate human recompaction passes ``guard=False``
    to supersede an entry on its own authority.
    """
    if guard:
        guard_row_count(lake_root, partition, rows)
    entry = {
        "partition": partition,
        "source": source,
        "sha256": sha256,
        "rows": rows,
        "fetched_at": fetched_at,
    }
    _append_line(manifest_path(lake_root), entry)
    return entry


def record_partition(
    lake_root: Path,
    partition: str,
    *,
    source: str,
    rows: int,
    fetched_at: str | None,
    guard: bool = True,
) -> dict:
    """Checksum a partition file on disk and append its manifest entry.

    This is the compaction path in one call. ``partition`` is the lake-relative path
    of a file that already exists under ``lake_root``. Its sha256 is read from disk, so
    the entry always matches the bytes on disk at record time.
    """
    sha256 = sha256_file(Path(lake_root) / partition)
    return append_manifest(
        lake_root,
        partition=partition,
        source=source,
        sha256=sha256,
        rows=rows,
        fetched_at=fetched_at,
        guard=guard,
    )


def append_quarantine(lake_root: Path, entry: dict) -> dict:
    """Append one quarantine entry as a single ``O_APPEND`` line and return it.

    The entry is keyed by ``partition`` like the manifest. Last entry wins, so an
    un-quarantine is a superseding row, never a deletion of history.
    """
    _append_line(quarantine_path(lake_root), entry)
    return entry


# -- the two-way scrub -------------------------------------------------------


@dataclass(frozen=True)
class ScrubResult:
    """The verdict of a two-way scrub.

    Three tuples of partition paths name what is wrong, and in which direction.

    - ``missing``: a manifest entry whose file is gone. A forward-pass failure.
    - ``sha_mismatches``: a file present but not matching its last recorded sha. A
      forward-pass failure.
    - ``orphans``: a data file with no manifest entry. A reverse-pass failure.
    """

    missing: tuple[str, ...]
    sha_mismatches: tuple[str, ...]
    orphans: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """Whether the scrub found nothing wrong in either direction."""
        return not (self.missing or self.sha_mismatches or self.orphans)


def _compacted_partition_for_segment(rel: str) -> str | None:
    """The compacted partition path a journal segment merges into, or ``None``.

    A slice-1 cron run may append a manifest entry keyed by a segment path under
    ``journal/date=D/surface=S/ticker=T/seg-<...>.arrows``. That segment merges into
    the compacted partition ``S/ticker=T/date=D.parquet``. This maps one to the other
    so the scrub can tell when the segment entry has been superseded.
    """
    parts = rel.split("/")
    if len(parts) != 5 or parts[0] != "journal":
        return None
    date_part, surface_part, ticker_part, name = parts[1:]
    if not (
        date_part.startswith("date=")
        and surface_part.startswith("surface=")
        and ticker_part.startswith("ticker=")
        and name.endswith(".arrows")
    ):
        return None
    day = date_part[len("date=") :]
    surface = surface_part[len("surface=") :]
    ticker = ticker_part[len("ticker=") :]
    return f"{surface}/ticker={ticker}/date={day}.parquet"


def _is_excluded(rel: str, exclusions: Sequence[str]) -> bool:
    """Whether a lake-relative path is exempt from the reverse pass."""
    for item in exclusions:
        if item.endswith("/"):
            if rel.startswith(item):
                return True
        elif rel == item:
            return True
    return False


def scrub(lake_root: Path) -> ScrubResult:
    """Run the two-way integrity scrub over a lake. Read-only, never mutating.

    Forward pass: every manifest entry's file must exist and match its last recorded
    sha. A slice-1 segment entry is treated as superseded once an entry exists for its
    matching compacted partition, so compaction's verify-then-delete never strands it.

    Reverse pass: every data file under the lake root must have a manifest entry. The
    enumerated exclusion set is skipped. The lock adds nothing to skip, because it
    locks the manifest, which is already excluded.
    """
    root = Path(lake_root)
    latest = latest_entries(root)

    missing: list[str] = []
    sha_mismatches: list[str] = []
    for partition, entry in latest.items():
        compacted = _compacted_partition_for_segment(partition)
        if compacted is not None and compacted in latest:
            # The segment entry is superseded by its compacted partition. Its file may
            # already be deleted, so it is not a forward-pass failure.
            continue
        path = root / partition
        if not path.exists():
            missing.append(partition)
        elif sha256_file(path) != entry["sha256"]:
            sha_mismatches.append(partition)

    orphans: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if _is_excluded(rel, SCRUB_EXCLUSIONS):
            continue
        if rel not in latest:
            orphans.append(rel)

    return ScrubResult(
        tuple(sorted(missing)),
        tuple(sorted(sha_mismatches)),
        tuple(sorted(orphans)),
    )
