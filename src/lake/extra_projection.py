"""Reading a promoted field back out of ``extra`` at read time.

The parser fails open. A vendor field the pinned schema does not name is not dropped, it
is JSON-encoded into the normally-empty ``extra`` overflow column, so vendor-verbatim
stays structurally true even when a payload drifts. When such a field turns out to matter,
the answer is to promote it: give it a typed column and bump ``SCHEMA_VERSION``. From that
version on the value lands in its column. Every row already sealed below that version
still holds it in ``extra``.

So a promotion splits history in two. The same measurement reads as a column above the
boundary and as an overflow key below it, and a reader asking for the column sees nulls
across the older half that look exactly like vendor nulls. This module closes that split at
read time: given a table of journal rows, it lifts the value out of ``extra`` on the rows
whose version had no column for it and presents it as that column.

Read time is the only place this can happen. A compacted partition is immutable, and that
immutability is what the manifest protocol, the two-way integrity scrub, and the backup all
rest on. Rewriting a sealed partition to heal it would trade three guarantees for one
convenience.

Two rules keep the healing honest.

1. *One place owns the version-to-shape mapping.* Which columns a version carried is
   ``reference/schema_versions.parquet``, read through ``lake.schema_versions``. This
   module asks that ledger and never re-derives the answer, because a second reader of the
   same file is a second source of truth for what a sealed row means.
2. *One place owns the column-to-overflow mapping.* Which overflow key feeds which column
   is ``journal.extra_paths``, derived from the parser's own vendor maps. The writer and
   the reader therefore cannot disagree about where a value sits.

What this module does not do is reshape a table. It fills a column, and it adds one only
when a row actually carries a value for it. Projecting the running schema onto an arbitrary
table is the loader's job, in slice 4, and that loader reads through this rather than
reimplementing it.

The projection is opt-in. Nothing that reads journal rows today calls it, so no existing
reader's output moves.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import NamedTuple

import pyarrow as pa

from lake import journal
from lake.schema_versions import SchemaVersionLedger

# The two columns a journal table must carry for the projection to mean anything. The
# version says which shape wrote the row, and the overflow holds the value.
VERSION_COLUMN = "schema_version"
EXTRA_COLUMN = "extra"


class ExtraProjectionError(Exception):
    """Raised when a table cannot be projected at all.

    Every case is one no journal writer produces: a missing ``schema_version`` or
    ``extra`` column, a row whose version is null, or an ``extra`` value that is not JSON.
    None of these is an operational condition a read should absorb, unlike a version the
    ledger has no shape for, which is expected and is reported rather than raised.
    """


class UnfitValue(NamedTuple):
    """An overflow value the column it belongs to refuses.

    The value stays where it is, in ``extra``, and the cell stays null. ``rows`` counts
    how many rows hit this same column, version, and reason.

    This happens when the promoted column's type does not match what the vendor was
    sending at the older version, which is the one thing a promotion cannot decide for the
    past. Reporting it rather than raising keeps the rest of the table readable, and
    nothing is lost, because the raw value is still in the overflow.
    """

    column: str
    schema_version: int
    detail: str
    rows: int


class ExtraProjection(NamedTuple):
    """A projected table and everything the projection could not do silently.

    ``table`` is the input with the promoted columns filled. ``filled`` counts the rows
    each column gained, and a column that gained none is absent from it.

    ``unrecorded_versions`` names every version in the table that the ledger holds no
    shape for. Those rows come back exactly as written, because the projection cannot know
    which columns that version carried, and guessing would be worse than leaving the value
    where it sits. That gap is what marketlake #130 exists to prevent, and until it lands a
    caller reads this field to know the read was partial.

    ``unfit`` names every value a column refused.
    """

    table: pa.Table
    filled: Mapping[str, int]
    unrecorded_versions: tuple[int, ...]
    unfit: tuple[UnfitValue, ...]

    @property
    def complete(self) -> bool:
        """Whether every row was projected against a recorded shape with nothing refused."""
        return not self.unrecorded_versions and not self.unfit


def _overflow(raw: object, row: int) -> Mapping[str, object]:
    """One row's ``extra`` decoded, or an empty mapping when the row has none.

    A value that is not JSON raises. ``extra`` is written by ``json.dumps`` on both
    surfaces, so a string that will not parse did not come from the parser, and reading
    past it would present the row as whole while its overflow is unreadable.
    """
    if raw is None or raw == "":
        return {}
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ExtraProjectionError(
            f"row {row} holds an {EXTRA_COLUMN} value that is not JSON"
        ) from exc
    if not isinstance(decoded, Mapping):
        raise ExtraProjectionError(
            f"row {row} holds an {EXTRA_COLUMN} value that is not a JSON object"
        )
    return decoded


def _lookup(overflow: Mapping[str, object], path: journal.ExtraPath) -> object:
    """The overflow value at ``path``, or ``None`` when the row does not carry it.

    A flat path reads the key straight off the overflow. A nested one reads the block
    first, which is how the quotes surface keeps the field names its blocks share apart.
    """
    if path.block is None:
        return overflow.get(path.field)
    block = overflow.get(path.block)
    if not isinstance(block, Mapping):
        return None
    return block.get(path.field)


def _convert(field_type: pa.DataType, value: object) -> tuple[object, str | None]:
    """``value`` as the column's own type, or the reason it will not fit.

    Conversion goes through ``journal.typed_column``, the same builder every write site
    uses, so the projection cannot accept a value a capture would have refused. The
    converted value is taken back out rather than the raw one, so the rebuilt column holds
    one Python type throughout and a later inference has nothing to guess at.

    Arrow refuses every conversion here that would change a value, with one exception it
    performs silently: a boolean into a floating column returns ``1.0``. A boolean is
    therefore checked by hand before Arrow sees it, because ``True`` arriving where a
    price belongs is drift to report, not a number.
    """
    if isinstance(value, bool) and field_type != pa.bool_():
        return None, f"boolean value in a {field_type} column"
    try:
        converted = journal.typed_column(field_type, [value])
    # Every refusal is reported rather than raised, whichever Arrow class it arrives as.
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    return converted.to_pylist()[0], None


def _versions(table: pa.Table) -> Sequence[int]:
    """Every row's ``schema_version``, refusing a null one.

    Every row builder stamps the version, and the suite checks each of them, so a null
    here means the table did not come from the journal.
    """
    versions = table.column(VERSION_COLUMN).to_pylist()
    for row, version in enumerate(versions):
        if version is None:
            raise ExtraProjectionError(f"row {row} carries no {VERSION_COLUMN}")
    return versions


def project_extra(
    table: pa.Table,
    *,
    surface: str,
    ledger: SchemaVersionLedger,
) -> ExtraProjection:
    """Present each row's ``extra``-held values as the columns the running shape gives them.

    A row is projected column by column. For each column the running schema carries and
    the row's own recorded version did not, the value is looked up in that row's overflow
    and written into the column. A row whose version already carried the column is left
    alone, because the column is where its value already is.

    The target shape is the running code's, via ``journal.schema_fingerprint``, and the
    candidate columns are the ones a vendor field could have reached, via
    ``journal.extra_paths``. A column outside that set was never in the overflow, so there
    is nothing to lift.

    Three things are reported rather than raised, because each leaves the table readable:
    a version the ledger has no shape for, a value the column refuses, and a row that
    simply has nothing in its overflow.
    """
    fingerprint = journal.schema_fingerprint(surface)
    schema = journal.schema_for(surface)
    paths = journal.extra_paths(surface)

    present = set(table.column_names)
    for required in (VERSION_COLUMN, EXTRA_COLUMN):
        if required not in present:
            raise ExtraProjectionError(f"a {surface} table has no {required} column")

    versions = _versions(table)
    extras = table.column(EXTRA_COLUMN).to_pylist()

    # Which columns each version in the table is missing, asked of the ledger once per
    # version rather than once per row.
    targets: dict[int, frozenset[str]] = {}
    unrecorded: set[int] = set()
    for version in set(versions):
        entry = ledger.get(version)
        if entry is None:
            unrecorded.add(version)
            targets[version] = frozenset()
            continue
        targets[version] = frozenset(
            column
            for column in paths
            if column in fingerprint and not entry.has_column(surface, column)
        )

    filled: dict[str, int] = {}
    refused: dict[tuple[str, int, str], int] = {}
    rebuilt: dict[str, list[object]] = {}
    for column in sorted({name for wanted in targets.values() for name in wanted}):
        field_type = schema.field(column).type
        values = table.column(column).to_pylist() if column in present else [None] * table.num_rows
        hits = 0
        for row, version in enumerate(versions):
            if column not in targets[version]:
                continue
            raw = _lookup(_overflow(extras[row], row), paths[column])
            if raw is None:
                continue
            converted, detail = _convert(field_type, raw)
            if detail is not None:
                key = (column, version, detail)
                refused[key] = refused.get(key, 0) + 1
                continue
            values[row] = converted
            hits += 1
        if hits:
            rebuilt[column] = values
            filled[column] = hits

    projected = table
    for column, values in rebuilt.items():
        field = schema.field(column)
        array = journal.typed_column(field.type, values)
        if column in present:
            projected = projected.set_column(projected.schema.get_field_index(column), field, array)
        else:
            projected = projected.append_column(field, array)

    return ExtraProjection(
        table=projected,
        filled=filled,
        unrecorded_versions=tuple(sorted(unrecorded)),
        unfit=tuple(
            UnfitValue(column=column, schema_version=version, detail=detail, rows=rows)
            for (column, version, detail), rows in sorted(refused.items())
        ),
    )


__all__ = [
    "EXTRA_COLUMN",
    "VERSION_COLUMN",
    "ExtraProjection",
    "ExtraProjectionError",
    "UnfitValue",
    "project_extra",
]
