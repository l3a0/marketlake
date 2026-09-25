"""The schema-version ledger, and the hand-invoked tool that writes it.

``schema_version`` is the only provenance that survives a seal. Compaction unlinks a
ticker-day's segments once the partition is manifested, so after that moment the
per-segment schemas are gone and the integer stamped on every row is the sole record of
which code shape wrote it. The integer alone says nothing. Somewhere there has to be a
map from it to the shape it names, and where that map lives decides whether the lake can
still be read after a disk-loss restore.

This module puts the map inside the lake, at ``reference/schema_versions.parquet``. The
precedent is the quarantine ledger, which sits inside the backup sync root by
construction, so a restore carries the battery's verdicts *with* the data. A verdict
ledger outside the backup would silently un-quarantine corrupted partitions on restore. A
version-to-shape map outside the lake loses the ability to interpret the lake in exactly
the same way. Held only in the repo, reading a sealed partition would need the git history
of the machine that wrote it.

The question the map answers is a column lookup. A null on a version N+1 row means one of
two different things, and they call for opposite handling. Either the vendor sent a null,
which is an observation, or the column was gone from the shape that wrote the row, so the
value was never captured at all. Asking whether version N+1 has a ``bid`` column separates
the two. So the table is long rather than wide: one row per version, surface, and column,
which is a plain SQL predicate from DuckDB rather than a JSON blob to pick apart.

Three properties follow from where the writer sits, and each is deliberate.

1. *No new lock-taker, and nothing on the capture path.* The tool reads the ledger,
   writes it, and appends its manifest entry under one hold of the existing lake-root
   ``flock``, the same lock ``onboard``, ``retire`` and ``seed_spans`` take. Capture
   workers stay outside that lock by design, because blocking a cycle behind compaction
   would drop perishable minutes.
2. *The integrity scrub covers the file with nothing added.* The scrub's reverse pass
   asks every file under the lake root for a manifest entry, and its exclusion set is
   ``{manifest.jsonl, journal/, reports/}``. ``reference/`` is not on it, so a file
   written without its entry is reported as an orphan.
3. *Re-running is harmless.* A second run finds the running version already recorded
   with the same shape and writes nothing at all, so it adds no manifest entry either.

The shape written is derived, never restated. ``journal.schema_fingerprint`` reads each
pinned surface's column names and types straight off the schema, and this module records
what that returns. A second derivation would be a second source of truth for the same
fact, which is the failure the whole schema-version line exists to end.

Pinned rather than journaled. ``bars`` never reaches a segment, and its rows carry the same
version integer as the two surfaces that do, so the ledger describes it under that version
like any other.

What the tool cannot do is reconstruct a version it was never run for. It records the
version the running code carries, and it preserves every version already in the file. So a
bump made without a run leaves that version's shape recorded nowhere, and a later run
cannot fill the hole, because the code that knew the old shape is gone. So the run has to
happen, and what makes it happen is ``check_running_version`` below, which the daemon asks
at startup and the vendor sweep asks every evening. It reports rather than repairs, for
the reason its own docstring gives. What this module does do is refuse to paper over the
evidence: a running version already in the ledger under a different shape raises rather
than overwriting.

Run it beside the deliberate version bump::

    python -m lake.schema_versions
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lake import journal
from lake.clock import Clock, SystemClock
from lake.config import input_errors_exit, load_config
from lake.manifest import record_partition
from lake.paths import REFERENCE_DIR, temp_write_path

# The pinned schema version for this reference table itself. Every row stamps it. It is
# this file's own shape, not the journal shape a row describes, and the two move
# independently.
LEDGER_SCHEMA_VERSION = 1

# The file's name under ``reference/``, and its lake-relative path for the manifest.
LEDGER_FILENAME = "schema_versions.parquet"
LEDGER_PARTITION = f"{REFERENCE_DIR}/{LEDGER_FILENAME}"

# The manifest ``source`` for the reference entry, matching onboard, retire and seed_spans.
REFERENCE_SOURCE = "reference"

# The pinned pyarrow schema, one row per journal version, surface, and column.
#
# ``journal_schema_version`` is the version being described, the integer stamped on every
# journal row and on every row of a pinned surface that never journals. One version line
# covers both, because the integer names the code shape that wrote a row rather than the
# file it landed in, and a second line would need a second version column this table does
# not have. ``schema_version`` is this table's own, which is the name every reference
# table here uses for that, so the described version needs the longer one. ``column_type``
# is the type as pyarrow renders it, which is what ``journal.schema_fingerprint`` returns,
# so ``pa.float64()`` reads as ``double``. ``recorded_at`` is when the version was written
# into the ledger, which is not when the version was minted and does not claim to be.
LEDGER_SCHEMA = pa.schema(
    [
        ("journal_schema_version", pa.int32()),
        ("surface", pa.string()),
        ("column_name", pa.string()),
        ("column_type", pa.string()),
        ("recorded_at", pa.timestamp("us", tz="UTC")),
        ("schema_version", pa.int32()),
    ]
)


class SchemaVersionsError(Exception):
    """Base class for every schema-version ledger failure."""


class UnsupportedLedgerSchemaVersion(SchemaVersionsError):
    """Raised when a file on disk carries a table version this code cannot read."""

    def __init__(self, found: int) -> None:
        super().__init__(
            f"schema-versions ledger schema version {found}, this code reads "
            f"{LEDGER_SCHEMA_VERSION}"
        )
        self.found = found


class LedgerUnreadable(SchemaVersionsError):
    """Raised when the ledger file is present but truncated or not valid parquet.

    A torn write leaves fewer bytes than a whole file. ``pyarrow`` refuses those with
    ``ArrowInvalid``, whose class tree is ``ArrowInvalid -> ValueError``, not a
    ``SchemaVersionsError``. ``read`` folds it into this class so a caller guarding this
    module's own errors catches it. An absent file raises ``OSError`` instead, and callers
    guard that beside this class, because an absent ledger is not a corrupt one.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(f"schema-versions ledger at {path} is not readable parquet")
        self.path = path


class SchemaVersionConflict(SchemaVersionsError):
    """Raised when the running version is already recorded under a different shape.

    Reaching this means a column was added, dropped, or retyped without a bump, which the
    suite's own fingerprint check refuses, so the suite was bypassed to get here. The
    refusal is loud rather than an overwrite because rows already written at this version
    carry the integer alone. Rewriting what the version meant would make every one of them
    say something the code that wrote them never said, and nothing in the lake could tell
    afterwards that it had happened.
    """

    def __init__(self, version: int, detail: str) -> None:
        super().__init__(detail)
        self.version = version


@dataclass(frozen=True)
class RecordedVersion:
    """One journal schema version's recorded shape, across every surface it covers.

    ``fingerprints`` maps a surface to its column-name-to-type mapping, which is exactly
    what ``journal.schema_fingerprint`` returns for that surface. ``recorded_at`` is when
    the version was written into the ledger.
    """

    version: int
    recorded_at: datetime
    fingerprints: Mapping[str, Mapping[str, str]]

    @property
    def row_count(self) -> int:
        """How many ledger rows this version occupies, one per surface column."""
        return sum(len(columns) for columns in self.fingerprints.values())

    def has_column(self, surface: str, column: str) -> bool:
        """Whether this version's ``surface`` carried ``column``.

        This is the question the ledger exists for. A null under a column this returns
        false for was never observed, rather than observed as null.
        """
        return column in self.fingerprints.get(surface, {})


class SchemaVersionLedger:
    """Every journal schema version the lake has recorded a shape for.

    Versions come back in ascending order, sorted on the way out rather than relied on
    from insertion. Nothing here mutates an entry already present, because a recorded
    version is what sealed rows are read through.
    """

    def __init__(self, versions: Iterable[RecordedVersion] = ()) -> None:
        self._versions: dict[int, RecordedVersion] = {}
        for entry in versions:
            self._versions[entry.version] = entry

    def versions(self) -> tuple[int, ...]:
        """Every recorded version, ascending."""
        return tuple(sorted(self._versions))

    def get(self, version: int) -> RecordedVersion | None:
        """The recorded entry for ``version``, or ``None`` when it has none."""
        return self._versions.get(version)

    @property
    def row_count(self) -> int:
        """The ledger's total row count, which is what the manifest entry records."""
        return sum(entry.row_count for entry in self._versions.values())

    def with_version(self, entry: RecordedVersion) -> SchemaVersionLedger:
        """A new ledger holding every current entry plus ``entry``.

        Adding a version already present raises, rather than replacing it. Superseding a
        recorded shape is never a thing this module does on its own, for the reason
        ``SchemaVersionConflict`` gives. The caller checks for the version first, so this
        is the guard behind that check rather than the path a run takes.
        """
        if entry.version in self._versions:
            raise SchemaVersionsError(
                f"journal schema version {entry.version} is already recorded, and a "
                "recorded shape is never replaced in place"
            )
        return SchemaVersionLedger([*self._versions.values(), entry])

    # -- parquet round-trip --------------------------------------------------

    def to_table(self) -> pa.Table:
        """Render the ledger as a pyarrow table in the pinned schema.

        Rows come out grouped by version ascending, then by the surface and column order
        each entry holds. A fingerprint read off a schema keeps that schema's order, so a
        freshly recorded version reads top to bottom the way the schema is written, and a
        version read back from the file keeps the order it was written in. Order carries
        no meaning either way, since the fingerprint is a mapping on purpose.
        """
        versions: list[int] = []
        surfaces: list[str] = []
        names: list[str] = []
        types: list[str] = []
        stamps: list[datetime] = []
        for version in self.versions():
            entry = self._versions[version]
            for surface, columns in entry.fingerprints.items():
                for name, column_type in columns.items():
                    versions.append(version)
                    surfaces.append(surface)
                    names.append(name)
                    types.append(column_type)
                    stamps.append(entry.recorded_at)
        return pa.table(
            {
                "journal_schema_version": versions,
                "surface": surfaces,
                "column_name": names,
                "column_type": types,
                "recorded_at": stamps,
                "schema_version": [LEDGER_SCHEMA_VERSION] * len(versions),
            },
            schema=LEDGER_SCHEMA,
        )

    @classmethod
    def from_table(cls, table: pa.Table) -> SchemaVersionLedger:
        """Build a ledger from a pyarrow table in the pinned schema.

        A row stamped with a table version this code does not read raises, the same way
        the security master and the capture spans refuse one. Every row of a version
        shares one ``recorded_at``, written in a single run, so the first row's stamp is
        the version's.
        """
        entries: dict[int, dict[str, dict[str, str]]] = {}
        stamps: dict[int, datetime] = {}
        for row in table.to_pylist():
            if row["schema_version"] != LEDGER_SCHEMA_VERSION:
                raise UnsupportedLedgerSchemaVersion(row["schema_version"])
            version = row["journal_schema_version"]
            surfaces = entries.setdefault(version, {})
            surfaces.setdefault(row["surface"], {})[row["column_name"]] = row["column_type"]
            stamps.setdefault(version, row["recorded_at"])
        return cls(
            RecordedVersion(version=version, recorded_at=stamps[version], fingerprints=surfaces)
            for version, surfaces in entries.items()
        )

    def write(self, path: Path | str) -> Path:
        """Write the ledger to parquet at ``path``, through a temp file and a rename.

        The write goes to a temp file beside the target, flushes, then renames over the
        target in one step, so a reader sees the whole old file or the whole new one. The
        security master, the capture spans and the roster are written the same way. The
        temp path comes from ``paths.temp_write_path``, which owns the marker the backup
        exclusion matches. Parent dirs are created if absent.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = temp_write_path(path, os.getpid())
        try:
            pq.write_table(self.to_table(), tmp)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return path

    @classmethod
    def read(cls, path: Path | str) -> SchemaVersionLedger:
        """Read a ledger from parquet at ``path``.

        A truncated or torn file raises ``pyarrow``'s ``ArrowInvalid``, folded into
        ``LedgerUnreadable`` so a caller guarding ``SchemaVersionsError`` catches it. An
        absent file raises ``OSError`` instead, and callers guard that apart.
        """
        path = Path(path)
        try:
            table = pq.read_table(path)
        except pa.ArrowInvalid as exc:
            raise LedgerUnreadable(path) from exc
        return cls.from_table(table)


def ledger_path(lake_root: Path | str) -> Path:
    """The ledger's path under a lake root: ``reference/schema_versions.parquet``."""
    return Path(lake_root) / REFERENCE_DIR / LEDGER_FILENAME


def running_fingerprints() -> dict[str, dict[str, str]]:
    """The shape the running code writes, per pinned surface.

    This is the whole derivation, and it is ``journal.schema_fingerprint``'s. Nothing here
    restates a column set, so the ledger cannot record a shape the code does not have.
    """
    return {surface: journal.schema_fingerprint(surface) for surface in journal.PINNED_SURFACES}


def _as_plain(
    fingerprints: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    """A plain-dict copy of a surface-to-fingerprint mapping, so ``==`` compares content.

    Comparison is by content and never by order, which is what ``schema_fingerprint``
    returning a mapping already means.
    """
    return {surface: dict(columns) for surface, columns in fingerprints.items()}


def _conflict_detail(
    version: int,
    derived: Mapping[str, Mapping[str, str]],
    recorded: Mapping[str, Mapping[str, str]],
) -> str:
    """Name every surface and column that disagrees, then name the fix.

    Naming the columns is the reason the fingerprint is a column list rather than a
    digest. A digest would say only that something moved.
    """
    lines = [
        f"journal schema version {version} is already recorded in the ledger with a "
        "different shape.",
    ]
    for surface in sorted(set(derived) | set(recorded)):
        diff = journal.fingerprint_diff(derived.get(surface, {}), recorded.get(surface, {}))
        if not diff.moved:
            continue
        retyped = ", ".join(f"{name} {was} -> {now}" for name, was, now in diff.retyped)
        lines.extend(
            [
                f"  {surface} dropped: {', '.join(diff.dropped) or 'none'}",
                f"  {surface} added: {', '.join(diff.added) or 'none'}",
                f"  {surface} retyped: {retyped or 'none'}",
            ]
        )
    lines.extend(
        [
            "Fix: bump journal.SCHEMA_VERSION and re-run this tool, so the new shape is "
            "recorded under a new version.",
            "The recorded entry is not overwritten. Rows already written at this version "
            "carry the integer alone, so rewriting what it meant makes those rows say "
            "something the code that wrote them never said.",
        ]
    )
    return "\n".join(lines)


@dataclass(frozen=True)
class SchemaVersionReport:
    """What a recording run did, for the sign-off block.

    ``already_recorded`` says the running version was in the ledger with the same shape,
    so the run wrote nothing at all, the file included. ``versions`` is every version the
    ledger holds after the run, which is where a reader sees a version that was never
    recorded: a gap in the sequence is a bump whose tool run never happened.
    """

    recorded: RecordedVersion
    already_recorded: bool
    versions: tuple[int, ...]
    rows: int
    path: Path

    def render(self) -> str:
        version = self.recorded.version
        head = (
            f"journal schema_version {version} is already recorded with this shape. Nothing to do."
            if self.already_recorded
            else f"Recorded journal schema_version {version}."
        )
        shapes = ", ".join(
            f"{surface} ({len(columns)} columns)"
            for surface, columns in self.recorded.fingerprints.items()
        )
        return "\n".join(
            [
                head,
                f"  surfaces:        {shapes}",
                f"  recorded at:     {self.recorded.recorded_at.isoformat()}",
                f"  versions:        {', '.join(str(v) for v in self.versions)}",
                f"  ledger rows:     {self.rows}",
                f"  ledger:          {self.path}",
            ]
        )


def record_schema_version(*, clock: Clock, lake_root: Path | str) -> SchemaVersionReport:
    """Record the running ``journal.SCHEMA_VERSION``'s shape in the lake's ledger.

    Every dependency is injected, so this runs offline. The read, the decision, and the
    write all happen inside one hold of the lake-root lock, so the version a run decided
    was absent cannot be written by something else in between. Versions already in the file
    are preserved exactly, since a recorded shape is what sealed rows are read through.

    Three outcomes, and only one of them writes:

    1. The running version is absent, so its shape is added and the file is written.
    2. The running version is present with the same shape, so nothing is written, not even
       a manifest entry. That is what makes a re-run a true no-op.
    3. The running version is present with a different shape, so this raises
       ``SchemaVersionConflict`` rather than overwriting what the version meant.
    """
    lake_root = Path(lake_root)
    target = ledger_path(lake_root)
    version = journal.SCHEMA_VERSION
    derived = running_fingerprints()

    # Local to keep this module free of the lock unless it uses it, the same reason
    # onboard.py, retire.py and seed_spans.py import it here rather than at module scope.
    from lake.lock import lake_lock

    with lake_lock(lake_root):
        ledger = SchemaVersionLedger.read(target) if target.exists() else SchemaVersionLedger()
        existing = ledger.get(version)
        if existing is not None:
            if _as_plain(existing.fingerprints) != derived:
                raise SchemaVersionConflict(
                    version, _conflict_detail(version, derived, existing.fingerprints)
                )
            return SchemaVersionReport(
                recorded=existing,
                already_recorded=True,
                versions=ledger.versions(),
                rows=ledger.row_count,
                path=target,
            )

        now = clock.now()
        entry = RecordedVersion(version=version, recorded_at=now, fingerprints=derived)
        ledger = ledger.with_version(entry)
        ledger.write(target)
        # The manifest entry is what keeps the reverse integrity scrub from flagging the
        # ledger as an orphan. ``reference/`` is not in the scrub's exclusion set, so a
        # file written without one is reported.
        record_partition(
            lake_root,
            LEDGER_PARTITION,
            source=REFERENCE_SOURCE,
            rows=ledger.row_count,
            fetched_at=now.isoformat(),
        )

    return SchemaVersionReport(
        recorded=entry,
        already_recorded=False,
        versions=ledger.versions(),
        rows=ledger.row_count,
        path=target,
    )


def record_schema_version_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
) -> SchemaVersionReport:
    """Record the running version wired from the real config. This is what ``main`` calls.

    It loads the machine-local config for the lake root. No vendor is needed, because
    recording a shape fetches nothing.
    """
    config = load_config(config_path)
    return record_schema_version(
        clock=clock if clock is not None else SystemClock(),
        lake_root=config.lake_root,
    )


# -- is the running version in the ledger --------------------------------------

# The five answers :func:`check_running_version` gives. ``RECORDED`` is the healthy one and
# the only one nothing reports. ``INACCESSIBLE`` is a ledger this process was refused
# permission to open, which is reported and never paged, for the reason
# :func:`_inaccessible_check` gives.
RECORDED = "recorded"
UNRECORDED = "unrecorded"
CONFLICTING = "conflicting"
UNREADABLE = "unreadable"
INACCESSIBLE = "inaccessible"

# One page event per paged verdict, because the three name three different repairs.
# ``INACCESSIBLE`` has none, because it does not page.
# ``alert._record`` keeps the event, the reason and the priority when a page fails to send,
# keeps the title unless the page was refused, and never keeps the body. So the event is the
# only field guaranteed to say which of the three went quiet.
UNRECORDED_EVENT = "schema_version_unrecorded"
CONFLICT_EVENT = "schema_version_conflict"
UNREADABLE_EVENT = "schema_version_ledger_unreadable"

# How many column names one surface contributes to a page body before the rendering stops and
# says how many are left. The model is ``schema_drift.PAGE_COLUMN_CAP`` and so is the number,
# because the reason is the same: the design pins a page body at plain text under 1,000 bytes,
# and the uncapped rendering grows with the column count. Measured against the pinned schemas
# this code carries, ``_conflict_detail`` runs to 596 bytes when one surface is missing from
# the record, 2,777 when every column of every surface is added, and 5,677 when every one is
# retyped. The last is past ntfy's own 4,096-byte body limit, which ``NtfyTransport`` answers
# with a 400 and does not retry, so the page saying the most would be the page that never
# arrives.
#
# The number is repeated rather than imported, and ``compact`` already carries a third copy for
# the same reason. Reaching ``schema_drift`` from here would pull ``capture`` and the vendor
# stack in behind it, into a module the read layer imports: ``loader``, ``extra_projection`` and
# ``battery`` all import this one and none of them imports ``capture`` today. The daemon pays
# that cost already, so it is the reader this protects rather than the writer.
PAGE_COLUMN_CAP = 12

# The hard bound on a page body, in bytes, which the column cap above does not supply on its
# own. ``_page_moved`` emits up to three clauses per surface, dropped, added and retyped, across
# three pinned surfaces, so nine capped lists can land in one body. Measured against the real
# pinned schemas, twelve columns in each of those nine lists renders 1,547 bytes, every one of
# them inside the column cap. ``schema_drift``'s cap bounds its body because it emits one list
# per surface, and that does not carry over.
#
# The design pins page bodies at plain text under 1,000 bytes, so the assembled body is cut to
# fit, the way ``sweep.digest_body`` cuts to ``DIGEST_BYTE_CAP``. The two caps do different
# work: the column cap keeps an ordinary conflict readable and counted, and this one is the
# guarantee. Truncating rather than dropping is the same trade the digest makes, because a page
# that did not arrive is worse than a page that says less.
PAGE_BODY_BYTE_CAP = 1000


@dataclass(frozen=True)
class RunningVersionCheck:
    """Where the running ``journal.SCHEMA_VERSION`` stands in one lake's ledger.

    ``state`` is one of the five above. ``recorded`` is every version the ledger holds, empty
    when it holds none or could not be read.

    ``summary`` and ``detail`` are ``None`` on a ``RECORDED`` verdict and set on every other,
    which is what makes "say nothing when healthy" a property of this object rather than a
    rule each caller has to remember. ``page_body``, ``event`` and ``title`` are set on every
    verdict that pages, and :attr:`pages` says which those are. ``INACCESSIBLE`` is reported
    and carries none of the three.

    ``summary`` is one line, safe for the nightly report. It carries no absolute path, because
    ``report.redacted`` exists to keep capture-machine paths out of a file the dashboard may
    read, and it holds at most two colon-separated fields, because that same function drops
    everything past the second one and the version number has to survive the trip to a phone.

    ``page_body`` is the capped rendering, under the design's body budget. ``detail`` is the
    uncapped one for stderr, where the operator who has the log is the only reader, so it is
    the one that may name the absolute path.
    """

    version: int
    state: str
    recorded: tuple[int, ...]
    summary: str | None = None
    page_body: str | None = None
    detail: str | None = None
    event: str | None = None
    title: str | None = None

    @property
    def ok(self) -> bool:
        """Whether the running version is recorded under the shape the running code derives."""
        return self.state == RECORDED

    @property
    def pages(self) -> bool:
        """Whether this verdict is one a caller pages for. False on ``RECORDED`` too."""
        return self.event is not None


def _capped(names: Sequence[str]) -> str:
    """Column names for a page body, cut at :data:`PAGE_COLUMN_CAP` with the rest counted.

    The count survives the cut for ``schema_drift``'s reason: it is what separates one moved
    column from a wholesale retype, and stderr names every column either way.

    ``names`` is never empty. :func:`_page_moved` is the only caller and it asks only for a
    list ``journal.fingerprint_diff`` reported as non-empty. A guard for the empty case would
    be a line that cannot fire, which is a line the next reader has to reason about.
    """
    shown = list(names[:PAGE_COLUMN_CAP])
    left = len(names) - len(shown)
    if left:
        shown.append(f"and {left} more")
    return ", ".join(shown)


def _page_moved(
    derived: Mapping[str, Mapping[str, str]],
    recorded: Mapping[str, Mapping[str, str]],
) -> str:
    """Every surface that disagrees, one capped clause each.

    This is ``_conflict_detail``'s fact set under the body budget. The difference comes off
    ``journal.fingerprint_diff`` in both, so the page and the stderr line can never disagree
    about what moved.
    """
    clauses: list[str] = []
    for surface in sorted(set(derived) | set(recorded)):
        diff = journal.fingerprint_diff(derived.get(surface, {}), recorded.get(surface, {}))
        if not diff.moved:
            continue
        parts = []
        if diff.dropped:
            parts.append(f"dropped {_capped(diff.dropped)}")
        if diff.added:
            parts.append(f"added {_capped(diff.added)}")
        if diff.retyped:
            parts.append(f"retyped {_capped([name for name, _, _ in diff.retyped])}")
        clauses.append(f"{surface} {'; '.join(parts)}")
    return ". ".join(clauses)


def _within_body_cap(head: str, middle: str, tail: str) -> str:
    """``head + middle + tail``, with ``middle`` cut so the whole fits :data:`PAGE_BODY_BYTE_CAP`.

    Only the middle is cut. The head carries the version and the tail carries what the reader
    loses, and both are short and fixed, so the part that grows is the part that gives way.
    """
    room = PAGE_BODY_BYTE_CAP - len(head.encode("utf-8")) - len(tail.encode("utf-8"))
    encoded = middle.encode("utf-8")
    if len(encoded) > room:
        ellipsis = "\u2026"
        keep = room - len(ellipsis.encode("utf-8"))
        middle = encoded[:keep].decode("utf-8", "ignore") + ellipsis
    return f"{head}{middle}{tail}"


def _unrecorded_check(version: int, target: Path, recorded: tuple[int, ...]) -> RunningVersionCheck:
    """The verdict for a running version the ledger holds no shape for.

    ``recorded`` is empty when there is no ledger at all, which the body renders as "none"
    rather than leaving blank: a reader on a phone needs to tell a lake that was never recorded
    from one whose recording stopped at an earlier bump.
    """
    held = ", ".join(str(v) for v in recorded) or "none"
    return RunningVersionCheck(
        version=version,
        state=UNRECORDED,
        recorded=recorded,
        summary=f"schema_version: {version} is not recorded in {LEDGER_PARTITION}",
        page_body=(
            f"journal schema_version {version} has no shape in {LEDGER_PARTITION}, "
            f"which holds {held}. Every read of a row at this version refuses."
        ),
        detail=f"journal schema_version {version} is not recorded in {target}, which holds {held}",
        event=UNRECORDED_EVENT,
        title=f"Schema version {version} is not in the lake's ledger",
    )


def _unreadable_check(version: int, target: Path, exc: BaseException) -> RunningVersionCheck:
    """The verdict for a ledger that is there and did not come back as one."""
    return RunningVersionCheck(
        version=version,
        state=UNREADABLE,
        recorded=(),
        summary=f"schema_version: {LEDGER_PARTITION} could not be read, {type(exc).__name__}",
        page_body=(
            f"{LEDGER_PARTITION} is present and could not be read "
            f"({type(exc).__name__}). Every read of the lake refuses, and the next "
            "backup copies this file over the last good one."
        ),
        detail=f"{target} could not be read: {type(exc).__name__}: {exc}",
        event=UNREADABLE_EVENT,
        title="The lake's schema-version ledger cannot be read",
    )


def _inaccessible_check(version: int, target: Path, exc: BaseException) -> RunningVersionCheck:
    """The verdict for a ledger this process was refused permission to open. It never pages.

    The unreadable verdict pages because a torn ledger gets worse by waiting: the design
    has the next close+15 backup copy it over the last good one. A refused open does not.
    The backup runs ``rsync -a`` as the same user, which cannot open the file either, so it
    leaves the backup's copy alone and exits non-zero. The backup then raises before the
    compaction ping, the ``compaction`` check goes silent, and that pages. So a refusal that
    lasts to close+15 already pages, and one that clears first needs no page. Marketlake #536
    is the second kind: a reboot starts the daemon a few seconds before the owner's login
    session exists, and on 2026-09-19 its first read of this file came back ``EPERM`` and
    paged, for a condition that had cleared before anyone could read the page.

    ``PermissionError`` alone, not the wider ``OSError``, because pyarrow reports most
    corruption as a bare ``OSError``. Of 400 random byte flips in a real ledger, 262 raised
    ``OSError: Corrupt snappy compressed data``. That is the torn ledger the page exists for,
    so it stays with :func:`_unreadable_check`.

    The summary still reaches the sweep's nightly report, which appends it for any verdict
    but ``RECORDED``, so a refusal that outlasts the evening is reported there.
    """
    return RunningVersionCheck(
        version=version,
        state=INACCESSIBLE,
        recorded=(),
        summary=f"schema_version: {LEDGER_PARTITION} could not be opened, {type(exc).__name__}",
        detail=f"{target} could not be opened: {type(exc).__name__}: {exc}",
    )


def check_running_version(lake_root: Path | str) -> RunningVersionCheck:
    """Where the running ``journal.SCHEMA_VERSION`` stands in ``lake_root``'s ledger.

    This is the answer to marketlake #130: nothing forces ``record_schema_version`` to be run
    beside a deliberate bump, so a version can reach the lake with its shape recorded nowhere.
    The daemon asks this at startup and the vendor sweep asks it every evening, and what each
    does with a reportable verdict is its own.

    It never raises, and the caller is why. A daemon that will not start captures nothing, and
    under launchd's ``KeepAlive`` the successor reaches the same check and refuses again, so a
    missing row in a reference table would cost a whole session. Anything the decision raises
    becomes ``UNREADABLE`` instead. The guard is broad rather than a list of classes, because
    the list is not two long: a torn file raises ``LedgerUnreadable``, a ledger format this
    code does not read ``UnsupportedLedgerSchemaVersion``, and some other parquet file at that
    path a bare ``KeyError``. That is ``sweep._counted``'s rule, that a summary must never cost
    the record.

    The guard covers the whole decision and not the read alone, because the file decides more
    than whether it parses. Every field of :data:`LEDGER_SCHEMA` is nullable, so a ledger with a
    null ``surface`` is a file the pinned schema accepts, and it reaches ``sorted`` in
    :func:`_page_moved` as ``TypeError: '<' not supported between instances of 'str' and
    'NoneType'``. A foreign parquet whose column names match and whose types do not reaches
    ``str.join`` the same way. Both are content rather than a programming error, and a guard
    stopping at the read would hand each of them to a daemon at startup.

    Absent is the one condition that is not unreadable, and it is caught by class rather than
    by looking first. ``FileNotFoundError`` alone means no ledger. A ``PermissionError`` or an
    I/O error on a file that is there means the shape is recorded and this process cannot see
    it, which is a different sentence and a different repair. A ``PermissionError`` is split
    off once more, into ``INACCESSIBLE``, which reports and does not page, per
    :func:`_inaccessible_check`. Reporting that as "not recorded"
    would send an operator to ``python -m lake.schema_versions``, which opens the same file and
    dies the same way. The sweep's reference readers were widened for exactly that reason under
    marketlake #435.

    It takes no lock and writes nothing. :meth:`SchemaVersionLedger.write` goes through a temp
    file and a rename, so a lockless read sees the whole old file or the whole new one, and the
    operator repairing this can run the tool while the daemon is live.

    The comparison is ``record_schema_version``'s own, ``_as_plain`` against
    :func:`running_fingerprints`. A second derivation could pass here and then fail the
    operator's run, which pages nobody and then refuses the repair.

    What this cannot see is a version that reached the lake and is no longer the running one.
    A rollback leaves those rows unrecorded and this reads ``RECORDED``. Answering that needs a
    walk over every partition's distinct ``schema_version``, and the repair it would name is
    one nothing can perform, since a run records the running version alone.
    """
    target = ledger_path(lake_root)
    version = journal.SCHEMA_VERSION
    try:
        return _decide(target, version)
    except FileNotFoundError:
        # No ledger, which is not a corrupt one. ``loader._ledger`` tells the two apart for the
        # same reason. Reading straight through rather than asking first is what keeps a
        # present-but-unreadable file out of this arm, and the rule does not rest on which
        # ``exists`` a later edit reaches for. ``os.path.exists`` answers False on any
        # ``OSError``, so it would call a locked ledger an absent one. ``pathlib.Path.exists``
        # raises ``PermissionError`` on a refused stat on Python 3.12 instead, and a file whose
        # own mode refuses the read still stats, so both would answer True for it (measured
        # under marketlake #536).
        return _unrecorded_check(version, target, ())
    except PermissionError as exc:
        return _inaccessible_check(version, target, exc)
    except Exception as exc:  # noqa: BLE001 - a startup check must never cost the session
        return _unreadable_check(version, target, exc)


def _decide(target: Path, version: int) -> RunningVersionCheck:
    """The verdict itself. Every raise it can make is the caller's to turn into one."""
    ledger = SchemaVersionLedger.read(target)
    recorded = ledger.versions()
    entry = ledger.get(version)
    if entry is None:
        return _unrecorded_check(version, target, recorded)

    derived = running_fingerprints()
    if _as_plain(entry.fingerprints) != derived:
        return RunningVersionCheck(
            version=version,
            state=CONFLICTING,
            recorded=recorded,
            summary=(
                f"schema_version: {version} is recorded in {LEDGER_PARTITION} under a "
                "different shape"
            ),
            page_body=_within_body_cap(
                f"journal schema_version {version} is recorded in {LEDGER_PARTITION} under a "
                "different shape. ",
                _page_moved(derived, entry.fingerprints),
                ". Rows written now decode against the recorded shape rather than this one.",
            ),
            # The absolute path leads, because stderr is the one reader that gets it and
            # an operator with two lakes on one machine needs to know which one disagreed.
            detail="\n".join(
                [
                    f"the ledger at {target} disagrees:",
                    _conflict_detail(version, derived, entry.fingerprints),
                ]
            ),
            event=CONFLICT_EVENT,
            title=f"Schema version {version} is recorded under a different shape",
        )

    return RunningVersionCheck(version=version, state=RECORDED, recorded=recorded)


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.schema_versions",
        description=(
            "Record the running journal schema_version's column shape in the lake's "
            "reference ledger. Run it beside a deliberate version bump."
        ),
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.schema_versions`` entry. Returns a process exit code."""
    args = _build_parser().parse_args(argv)
    with input_errors_exit("schema_versions"):
        report = record_schema_version_from_config(config_path=args.config)
    print(report.render())
    return 0


__all__ = [
    "CONFLICTING",
    "CONFLICT_EVENT",
    "INACCESSIBLE",
    "LEDGER_FILENAME",
    "LEDGER_PARTITION",
    "LEDGER_SCHEMA",
    "LEDGER_SCHEMA_VERSION",
    "LedgerUnreadable",
    "RECORDED",
    "RecordedVersion",
    "RunningVersionCheck",
    "SchemaVersionConflict",
    "SchemaVersionLedger",
    "SchemaVersionReport",
    "SchemaVersionsError",
    "UNRECORDED",
    "UNRECORDED_EVENT",
    "UNREADABLE",
    "UNREADABLE_EVENT",
    "UnsupportedLedgerSchemaVersion",
    "check_running_version",
    "ledger_path",
    "main",
    "record_schema_version",
    "record_schema_version_from_config",
    "running_fingerprints",
]


if __name__ == "__main__":
    raise SystemExit(main())
