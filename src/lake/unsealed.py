"""The door onto a ticker-day's quotes before the seal.

A consumer that decides before the close cannot wait for the close+15 seal, so it reads the
capture segments the live upload put in the bucket (``lake.live_upload``). This module turns the
segments a caller names into the rows captured by a given time, marketlake #1004. It is a module
of its own rather than a door on ``lake.loader``, which reads sealed partitions only.

``README.md``'s ``## Read an unsealed day`` is the published contract: what the object and its
digest are, how late a segment may be, what a missing minute means, and how a consumer pins this
package. What follows is the read itself.

``load_unsealed_quotes`` runs eight steps. Each one finishes for every named segment, in sorted
path order, before the next starts. The first fault in that order is the one raised, and nothing
is cut by ``at`` until the last step, so a bad segment refuses whichever minute it holds.

1. *The arguments.* ``lake_root`` has no default, unlike the sealed doors', because a default
   would read ``config.yaml``. An empty mapping is refused here.
2. *The quarantine guard.* No verdict exists before the seal, and a segment stays readable for
   days after it. So a day the battery withholds is refused here by the sealed doors' own
   ``withholding`` call, or this door would be a read path past the quarantine. It has no
   argument that reads a withheld day anyway.
3. *The bytes.* Each file is read once and checked against the digest it was named with.
4. *The read*, from those same bytes. Compaction reads a torn segment as fewer rows, because a
   power loss can tear one. A manifested segment was closed before it was hashed, so a tear here
   is damage. A segment that does not end in its end-of-stream marker is refused, and the marker
   itself is tested because Arrow reads a file cut just before it as clean. So is one that is
   not consumed whole, does not decode, or holds no row.
5. *Whose rows.* Three checks, and each covers every segment before the next one starts. First
   the schema version, from 1 to the one this code writes, so an unknown version is named rather
   than surfacing as a column type. Then the columns the read stands on, at their pinned types,
   with every row under the ticker asked for. Then every ``snap_ts``, an instant on the day asked
   for. Another ticker's bytes or yesterday's, under today's path, would otherwise read back with
   no complaint, and ``at`` cannot see a day-old row.
6. *The join* the seal makes, from ``lake.segment_join``.
7. *The overflow projection* (``lake.extra_projection``), as every sealed door runs it. A value
   the vendor retyped sits in ``extra`` with its column null, so reading past it would hand back
   a null price. The shape of the running version is this code's own, never the root's ledger's,
   because that ledger is written by hand and reaches the bucket with the nightly upload. The
   ledger is opened only when a row at an older version needs it.
8. *The rows*, through ``captured_by``.

**What escapes as itself.** Whatever the data raises in steps 2 and 4 to 8 becomes
``UnsealedRefused`` with the original as its cause, except ``MemoryError``. It is not a list of
classes, because each list ``journal._complete_batches`` tried missed a member. In step 3 an
``OSError`` other than ``FileNotFoundError`` escapes, because it says nothing about the bytes,
which is the rule ``compact._read_complete`` states.

A message names a file by its lake-relative path. So where a step reads the root's ledgers, the
message carries the class of what was raised and leaves its text, which can hold an absolute
path, on the cause.

Nothing here reads a clock, a config file or the network, and nothing is written.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa

from lake import journal, loader
from lake.calendar import MARKET_TZ
from lake.extra_projection import ExtraProjection, project_extra
from lake.manifest import latest_quarantine_by_check, sha256_bytes, withholding
from lake.paths import (
    DATE_PREFIX,
    QUARANTINE_FILE,
    LakePaths,
    parse_date_dir,
    parse_segment_rel,
)
from lake.schema_versions import (
    RecordedVersion,
    SchemaVersionLedger,
    ledger_path,
    running_fingerprints,
)
from lake.segment_join import merge_by_name, pinned_order

_SURFACE = journal.QUOTES_SURFACE

_DIGEST = re.compile(r"[0-9a-f]{64}")

# A segment's file name. ``paths.parse_segment_rel`` checks the suffix and nothing else of it.
_NAME = re.compile(r"seg-[A-Za-z0-9-]+\.arrows")

_VERSION = "schema_version"
_FETCH_END = "fetch_end_ts"
_SNAP = "snap_ts"
_TICKER = "ticker"

# The columns the read stands on beside the version, which is checked ahead of them.
_REQUIRED = (journal.ROW_KIND_COLUMN, _FETCH_END, _SNAP, _TICKER, journal.EXTRA_COLUMN)

# When the running shape was recorded, which the projection never asks and no clock answers here.
_NO_STAMP = datetime(1970, 1, 1, tzinfo=UTC)


class UnsealedRefused(loader.LoadError):
    """Raised when the named segments do not give a table this module can vouch for.

    ``segment`` is the lake-relative path steps 3 to 5 or a partial projection found at fault.
    It is ``None`` otherwise, and then the message names the ticker and day, the argument, or
    for a direct ``captured_by`` call the column.
    """

    def __init__(self, message: str, *, segment: str | None = None) -> None:
        super().__init__(message)
        self.segment = segment


class SegmentAbsent(UnsealedRefused):
    """Raised when no segment is named, or when a named one is not under the root.

    The first has no ``segment``, so there is nothing to download again.
    """


class SegmentMismatch(UnsealedRefused):
    """Raised when a named segment's bytes differ from the digest it was named with."""

    def __init__(self, segment: str, *, expected: str, actual: str) -> None:
        super().__init__(
            f"{segment} hashes to {actual}, and it was named with {expected}. Download it again.",
            segment=segment,
        )
        self.expected = expected
        self.actual = actual


class SchemaVersionUnknown(UnsealedRefused):
    """Raised when a row's schema version is not one this code knows, a null one included."""

    def __init__(self, segment: str, versions: Sequence[int | None]) -> None:
        self.versions = tuple(sorted(versions, key=lambda version: (version is not None, version)))
        named = ", ".join(str(version) for version in self.versions)
        super().__init__(
            f"{segment} holds rows at schema version {named}, and this code reads 1 to "
            f"{journal.SCHEMA_VERSION}. Raise the marketlake pin.",
            segment=segment,
        )


class UnsealedArgument(UnsealedRefused, ValueError):
    """Raised for an argument no read can be made from."""


@contextmanager
def _refusing(doing: str, segment: str | None = None, *, quote: bool = True) -> Iterator[None]:
    """Turn whatever the data raises into ``UnsealedRefused``, ``MemoryError`` aside.

    ``quote=False`` leaves the original's text out of the message, where it can hold an
    absolute path.
    """
    try:
        yield
    except (UnsealedRefused, MemoryError):
        raise
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}" if quote else type(exc).__name__
        raise UnsealedRefused(f"{doing}: {detail}", segment=segment) from exc


def segment_prefix(ticker: str, day: date | str) -> str:
    """The lake-relative path prefix of a ticker-day's quotes segments, ending in ``/``.

    The ending slash is what keeps a listing for SPY from returning SPYG's segments.
    """
    directory = LakePaths("").segment_dir(_SURFACE, _check_ticker(ticker), _check_day(day))
    return f"{directory.as_posix()}/"


def load_unsealed_quotes(
    ticker: str,
    day: date | str,
    at: datetime,
    *,
    segments: Mapping[str, str],
    lake_root: Path | str,
) -> pa.Table:
    """The data rows captured by ``at`` in the named segments of one ticker-day's quotes.

    ``segments`` maps each segment's lake-relative path, which is its key after
    ``<prefix>-live/``, to its SHA-256 as 64 lowercase hex digits. The caller names every one,
    because a directory listing cannot say what is missing and holds no digest to check.

    The result keeps the seal's row order, which is not time order, and shares no memory with
    the files. No data row by ``at`` is an empty table with its columns. The module docstring
    carries the steps, and ``README.md`` the contract.
    """
    _check_ticker(ticker)
    session = _check_day(day)
    _check_at(at)
    root = _check_root(lake_root)
    named = _check_segments(segments, ticker, session)
    label = f"{ticker} {session.isoformat()}"
    if not named:
        raise SegmentAbsent(f"{label}: no segment is named.")

    _clear_quarantine(root, ticker, session, label)

    held = [(rel, _read_bytes(root, rel, expected)) for rel, expected in named]
    tables = [(rel, _decode(rel, data)) for rel, data in held]
    for check in (_check_versions, _check_ticker_rows, _check_days):
        for rel, table in tables:
            with _refusing(f"{rel} cannot be judged", rel):
                check(rel, table, ticker, session)

    with _refusing(f"{label}: the segments do not join"):
        merged = merge_by_name([table for _, table in tables])
        merged = pinned_order(merged, journal.schema_for(_SURFACE))

    with _refusing(f"{label}: the overflow cannot be projected"):
        ledger = _shapes(root, merged, label)
        projection = project_extra(merged, surface=_SURFACE, ledger=ledger)
        if not projection.complete:
            raise _partial(ticker, session, projection, tables, ledger)

    try:
        return captured_by(projection.table, at)
    except UnsealedRefused as exc:
        raise UnsealedRefused(f"{label}: {exc}") from exc


def captured_by(table: pa.Table, at: datetime) -> pa.Table:
    """The table's data rows whose ``fetch_end_ts`` is at or before ``at``.

    ``fetch_end_ts`` is when the response landed. ``snap_ts`` is the minute the cycle fired for,
    so a cut on it would admit a row fetched after ``at``. The comparison runs on instants,
    because one instant has more than one spelling.

    A row with no ``row_kind``, a data row whose ``fetch_end_ts`` names no instant, and a
    missing column are refused. It is public because a replay needs the same cut on what
    ``loader.load_quotes`` returns.
    """
    if not isinstance(table, pa.Table):
        raise UnsealedArgument(f"{table!r} is not a pyarrow.Table.")
    _check_at(at)
    with _refusing("the rows cannot be cut"):
        kinds = table.column(journal.ROW_KIND_COLUMN).to_pylist()
        stamps = table.column(_FETCH_END).to_pylist()
        keep = [_landed_by(kind, stamp, at) for kind, stamp in zip(kinds, stamps, strict=True)]
        return table.filter(pa.array(keep, type=pa.bool_()))


def _landed_by(kind: object, stamp: object, at: datetime) -> bool:
    if kind is None:
        raise UnsealedRefused(
            f"a row holds no {journal.ROW_KIND_COLUMN}, so it is neither an observation nor an "
            "absence marker."
        )
    if kind != journal.ROW_KIND_DATA:
        return False
    landed = loader._instant(stamp)
    if landed is None:
        raise UnsealedRefused(f"a data row's {_FETCH_END} names no instant: {stamp!r}.")
    return landed <= at


# -- step 1, the arguments ---------------------------------------------------


def _check_ticker(ticker: object) -> str:
    if not isinstance(ticker, str) or not ticker or "/" in ticker or "\0" in ticker:
        raise UnsealedArgument(f"{ticker!r} is not a ticker.")
    try:
        ticker.encode()
    except UnicodeEncodeError as exc:
        raise UnsealedArgument(f"{ticker!r} is not a ticker.") from exc
    return ticker


def _check_day(day: object) -> date:
    if isinstance(day, date) and not isinstance(day, datetime):
        return day
    parsed = parse_date_dir(f"{DATE_PREFIX}{day}") if isinstance(day, str) else None
    if parsed is None:
        raise UnsealedArgument(f"{day!r} is not an ISO date.")
    return parsed


def _check_at(at: object) -> None:
    if not isinstance(at, datetime) or at.utcoffset() is None:
        raise UnsealedArgument(f"{at!r} is not an instant, because it carries no UTC offset.")


def _check_root(lake_root: object) -> Path:
    if not isinstance(lake_root, str | Path) or "\0" in os.fspath(lake_root):
        raise UnsealedArgument(f"{lake_root!r} is not a lake root.")
    return Path(lake_root)


def _check_segments(segments: object, ticker: str, session: date) -> list[tuple[str, str]]:
    """The named segments and their digests, in the sorted path order every step takes."""
    if not isinstance(segments, Mapping):
        raise UnsealedArgument(f"{segments!r} is not a mapping of segments to digests.")
    for rel in segments:
        if not isinstance(rel, str):
            raise UnsealedArgument(f"{rel!r} is not a segment's path.")
    named = sorted(segments.items())
    for rel, digest in named:
        ref = parse_segment_rel(rel)
        if (
            ref is None
            or ref.surface != _SURFACE
            or ref.ticker != ticker
            or ref.day != session.isoformat()
            or not _NAME.fullmatch(ref.filename)
        ):
            raise UnsealedArgument(
                f"{rel!r} is not a quotes segment of {ticker} {session.isoformat()}."
            )
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise UnsealedArgument(
                f"{rel} is named with {digest!r}, which is not 64 lowercase hex digits."
            )
    return named


# -- steps 2 to 4, the guard, the bytes and the read -------------------------


def _clear_quarantine(root: Path, ticker: str, session: date, label: str) -> None:
    """Refuse a ticker-day whose quotes partition the quarantine ledger withholds.

    ``loader.PartitionQuarantined`` is not raised, because its message advises an argument this
    door does not have.
    """
    partition = LakePaths(root).quotes_partition_path(ticker, session)
    key = partition.relative_to(root).as_posix()
    with _refusing(f"{label}: {QUARANTINE_FILE} cannot be read", quote=False):
        held = withholding(latest_quarantine_by_check(root).get(key))
    if held:
        checks = ", ".join(sorted({str(entry.get("check")) for entry in held}))
        raise UnsealedRefused(
            f"{label}: the quarantine ledger withholds {key}, by its checks {checks}."
        )


def _read_bytes(root: Path, rel: str, expected: str) -> bytes:
    try:
        data = (root / rel).read_bytes()
    except FileNotFoundError as exc:
        raise SegmentAbsent(
            f"{rel} is not under the lake root. Download it again.", segment=rel
        ) from exc
    actual = sha256_bytes(data)
    if actual != expected:
        raise SegmentMismatch(rel, expected=expected, actual=actual)
    return data


def _decode(rel: str, data: bytes) -> pa.Table:
    """The segment's rows, refused unless the bytes are one whole closed segment."""
    source = pa.BufferReader(data)
    if not journal._ends_in_eos(source):
        raise UnsealedRefused(f"{rel} does not end in its end-of-stream marker.", segment=rel)
    with _refusing(f"{rel} cannot be read", rel):
        schema, batches, _ = journal._complete_batches(Path(rel), source)
        if source.tell() < source.size():
            raise UnsealedRefused(f"{rel} holds bytes past its end-of-stream marker.", segment=rel)
        table = pa.Table.from_batches(batches, schema=schema)
    if not table.num_rows:
        raise UnsealedRefused(f"{rel} holds no row.", segment=rel)
    return table


# -- step 5, whose rows ------------------------------------------------------


def _check_versions(rel: str, table: pa.Table, ticker: str, session: date) -> None:
    _check_column(rel, table, _VERSION)
    unknown = [
        version
        for version in table.column(_VERSION).unique().to_pylist()
        if version is None or not 1 <= version <= journal.SCHEMA_VERSION
    ]
    if unknown:
        raise SchemaVersionUnknown(rel, unknown)


def _check_ticker_rows(rel: str, table: pa.Table, ticker: str, session: date) -> None:
    for name in _REQUIRED:
        _check_column(rel, table, name)
    others = [held for held in table.column(_TICKER).unique().to_pylist() if held != ticker]
    if others:
        raise UnsealedRefused(f"{rel} holds rows of {others}, not of {ticker}.", segment=rel)


def _check_days(rel: str, table: pa.Table, ticker: str, session: date) -> None:
    for text in table.column(_SNAP).unique().to_pylist():
        snap = loader._instant(text)
        # A cycle run outside the loop files by its UTC date until marketlake #937 closes.
        # Every cycle of the loop sits where the two dates agree.
        if snap is None or session not in (
            snap.astimezone(MARKET_TZ).date(),
            snap.astimezone(UTC).date(),
        ):
            raise UnsealedRefused(
                f"{rel} holds {_SNAP} {text!r}, which is no instant on {session.isoformat()}.",
                segment=rel,
            )


def _check_column(rel: str, table: pa.Table, name: str) -> None:
    index = table.schema.get_field_index(name)  # -1 for a name held twice, as for none
    if index < 0:
        raise UnsealedRefused(f"{rel} does not hold one {name} column.", segment=rel)
    found = table.schema.field(index).type
    pinned = journal.schema_for(_SURFACE).field(name).type
    if found != pinned:
        raise UnsealedRefused(f"{rel} holds {name} as {found}, not {pinned}.", segment=rel)


# -- step 7, the projection --------------------------------------------------


def _shapes(root: Path, merged: pa.Table, label: str) -> SchemaVersionLedger:
    """The shapes the projection reads against, the running one always among them."""
    running = journal.SCHEMA_VERSION
    recorded = SchemaVersionLedger()
    if any(version != running for version in merged.column(_VERSION).unique().to_pylist()):
        rel = ledger_path("").as_posix()
        with _refusing(f"{label}: {rel} cannot be read", quote=False):
            recorded = loader._ledger(root)
    older = [recorded.get(version) for version in recorded.versions() if version < running]
    return SchemaVersionLedger(
        [*older, RecordedVersion(running, _NO_STAMP, running_fingerprints())]
    )


def _partial(
    ticker: str,
    session: date,
    projection: ExtraProjection,
    tables: Sequence[tuple[str, pa.Table]],
    ledger: SchemaVersionLedger,
) -> UnsealedRefused:
    """The refusal of a partial projection, naming each segment that is partial alone."""
    at_fault = [
        rel
        for rel, table in tables
        if not project_extra(table, surface=_SURFACE, ledger=ledger).complete
    ]
    detail = loader.PartialRead(ticker, session.isoformat(), _SURFACE, projection)
    where = ", ".join(at_fault) if at_fault else "no one segment, only in the joined rows"
    return UnsealedRefused(
        f"{detail} It is partial in {where}.", segment=at_fault[0] if at_fault else None
    )


__all__ = [
    "SchemaVersionUnknown",
    "SegmentAbsent",
    "SegmentMismatch",
    "UnsealedArgument",
    "UnsealedRefused",
    "captured_by",
    "load_unsealed_quotes",
    "segment_prefix",
]
