"""Reading a promoted field back out of ``extra`` at read time.

The parser fails open. A vendor field the pinned schema does not name is not dropped, it
is JSON-encoded into the normally-empty ``extra`` overflow column, so vendor-verbatim
stays structurally true even when a payload drifts. When such a field turns out to matter,
the answer is to promote it: give it a typed column and bump ``SCHEMA_VERSION``. From that
version on the value lands in its column. Every row already sealed below that version
still holds it in ``extra``.

Two writers put a value in that column, and this module answers them differently. The
promotion above is the one it presents as a column. The other is the parser routing a known
field whose value its column refused, which leaves the column null and the raw value in the
overflow. That is a vendor retype seen from inside a capture, and the row's recorded version
*does* carry the column, so there is no free cell to lift the value into. The version's
other rows hold that column at the type the vendor stopped sending, and one Arrow column
cannot hold those values and the shape the vendor moved to at once. So a retype is refused
rather than presented, and the refusal is a report naming the column, the version whose rows
routed the value, and the type that version recorded for it.

What finds a retype is the signature marketlake #129 pins. The parser builds ``extra`` from
the fields its vendor maps do not name, so a known field's name can never otherwise appear
there, and its presence on a row whose version carries the column says the column refused
that row's value. The signature is what says a value was routed, so a value that happens to
fit the running column is a retype too. What the value looks like decides nothing here,
because the parser writes a known field's name into the overflow for one reason only.

One payload can write that name without routing, and it costs this reader a false report. A
chains contract field named ``chain`` carrying a chain-level field's own name lands on the
key those fields nest under, so the projection names a column whose values are present and
correct and calls the read partial with nothing missing from it. Closing that is
[#156](https://github.com/l3a0/marketlake/issues/156), which is authoritative for it. The
trigger is a compound coincidence and nothing observed suggests it is coming.

Two other answers to a retype were weighed, and
[#149](https://github.com/l3a0/marketlake/issues/149) is authoritative for both. The first
is rejected outright. The second waits.

1. *Casting the overflow-held values into the running column's type.* Raw is
   vendor-verbatim, so a cast manufactures values that were never observed and hands them
   to a downstream computation with no marker, which is the one outcome nobody can detect
   afterwards. It also has no defensible direction, because the running type is the one the
   vendor stopped honouring, and some retypes have no cast at all.
2. *An opt-in that hands back both representations.* Deferred until a caller asks for it,
   rather than cut. A retype pages at severity 5 under the schema policy, so it is a thing
   a human fixes by correcting the schema and bumping the version, rather than a condition
   a reader copes with forever.

So a promotion splits history in two. The same measurement reads as a column above the
boundary and as an overflow key below it, and a reader asking for the column sees nulls
across the older half that look exactly like vendor nulls. This module closes that split at
read time: given a table of journal rows, it lifts the value out of ``extra`` on the rows
whose version had no column for it and presents it as that column.

Read time is the only place this can happen. A compacted partition is immutable, and that
immutability is what the manifest protocol, the two-way integrity scrub, and the backup all
rest on. Rewriting a sealed partition to heal it would trade those guarantees for one
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
# version says which shape wrote the row, and the overflow holds the value. The overflow's
# name comes from the writer rather than being spelled again here, so the two cannot drift
# apart on the one column this module exists to read.
VERSION_COLUMN = "schema_version"
EXTRA_COLUMN = journal.EXTRA_COLUMN


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


class RetypedColumn(NamedTuple):
    """A column whose value a version's rows carry in ``extra`` rather than in the column.

    The parser routes a known field's value into the overflow and leaves the column null
    when the column refuses that value, which is what a vendor retype looks like from
    inside a capture. The row's own version carries the column, so nothing here lifts the
    value out. The version's other rows hold that column at the recorded type, and one
    Arrow column cannot hold those values and the shape the vendor moved to at once.

    ``column`` names the column. ``schema_version`` names the version whose rows routed the
    value, and ``recorded_type`` is what that version recorded the column as, which is the
    type the vendor stopped sending. ``rows`` counts how many rows at that version routed.

    Nothing is lost by reporting rather than repairing. The raw value stays in the
    overflow, and the repair is a human correcting the schema and bumping the version.
    """

    column: str
    schema_version: int
    recorded_type: str
    rows: int


class ExtraProjection(NamedTuple):
    """A projected table and everything the projection could not do silently.

    ``table`` is the input with the promoted columns filled. ``filled`` counts the rows
    each column gained, and a column that gained none is absent from it, because a column
    nothing filled is a column the table never gets.

    ``unrecorded_versions`` names every version in the table that the ledger holds no
    shape for. Those rows come back exactly as written, because the projection cannot know
    which columns that version carried, and guessing would be worse than leaving the value
    where it sits. That gap is what marketlake #130 exists to prevent, and until it lands a
    caller reads this field to know the read was partial.

    ``unfit`` names every value a column refused.

    ``retyped`` names every column a version's rows routed into the overflow, which is a
    vendor retype. Those rows come back exactly as written, because presenting the routed
    value as its column would need a cast and a cast manufactures a value the vendor never
    sent. A caller reads this field to know the column it asked for is partial.
    """

    table: pa.Table
    filled: Mapping[str, int]
    unrecorded_versions: tuple[int, ...]
    unfit: tuple[UnfitValue, ...]
    retyped: tuple[RetypedColumn, ...]

    @property
    def complete(self) -> bool:
        """Whether every row projected against a recorded shape with nothing left behind.

        Every reported thing counts. A read that refused a value, met a version it had no
        shape for, or found a column a retype routed away is partial, and saying otherwise
        would hand a caller a table it believes is whole.
        """
        return not self.unrecorded_versions and not self.unfit and not self.retyped


def _overflow(raw: object, row: int) -> Mapping[str, object]:
    """One row's ``extra`` decoded, or an empty mapping when the row has none.

    A value that is not JSON raises. ``extra`` is written by ``json.dumps`` of a dict on
    both surfaces, so a string that will not parse did not come from the parser, and
    presenting such a row as projected would claim its overflow held nothing when the
    truth is that nothing could read it.

    Only a row the projection actually reads is decoded, which is every row at a version
    the ledger holds a shape for. A retype's signature sits in the overflow of a row whose
    version misses no column at all, so finding it means reading those overflows as well as
    the ones a promotion lifts from. A row at an unrecorded version is still left alone
    rather than validated, because the projection reads nothing on it and a read has no
    business failing over an overflow it was never going to look at.
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
    first, which is how each level that could reuse another's field names stays apart from
    it: the quotes blocks from each other, and the chains surface's chain-level fields from
    the contract's.
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
    uses, so the projection cannot accept a value a capture would have refused. What makes
    the value fit is that call, and what types the column is the rebuild at the end. Taking
    the converted value back out rather than the raw one only keeps the cells one Python
    type on the way there, which leaves the rebuild nothing to widen.

    Arrow's own conversion refuses everything here that would change a value, with one
    exception it performs silently: a boolean into a floating column returns ``1.0``. A
    boolean is therefore checked by hand before the builder sees it, because ``True``
    arriving where a price belongs is drift to report, not a number.

    ``journal.typed_column`` now refuses that boolean too, for every type the pinned schemas
    carry, so this check no longer decides whether the value lands. What it still decides is
    what a reader is told, ``boolean value in a double column`` rather than Arrow's
    ``Expected double, got bool``. It also still decides the outcome for a floating type no
    pinned column carries yet, since ``pa.array([True], type=pa.float32())`` returns ``1.0``
    and the builder's check names ``float64`` alone.
    """
    if isinstance(value, bool) and field_type != pa.bool_():
        return None, f"boolean value in a {field_type} column"
    try:
        converted = journal.typed_column(field_type, [value])
    # ``journal.UNFIT_ERRORS`` is the writer's own list of the ways that builder refuses
    # one value, and it is consumed here rather than restated, so the two sides cannot go
    # a family apart. Anything wider would let a defect in the shared builder read as
    # vendor drift.
    except journal.UNFIT_ERRORS as exc:
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
    and written into the column.

    A row whose version already carried the column is left alone, because the column is
    where its value already is. A value for such a column sitting in that row's overflow
    says the parser put it there after the column refused it, which is a vendor retype. The
    projection refuses to present it and reports it instead. The version's other rows hold
    the column at the recorded type, one Arrow column cannot hold those values and the
    retyped ones at once, and casting between them would manufacture values the vendor
    never sent.

    A retype on one version's rows never withholds a lift another version's rows earned.
    The lifted value is not in dispute, and the report is what says the column is partial.

    The target shape is the running code's, via ``journal.schema_fingerprint``, and the
    candidate columns are the ones a vendor field could have reached, via
    ``journal.extra_paths``. A column outside that set was never in the overflow, so there
    is nothing to lift and nothing the parser could have routed there.

    Three things are reported rather than raised, because each leaves the table readable.

    1. A version the ledger holds no shape for on this surface.
    2. A value the column refuses.
    3. A column a version's rows routed into the overflow, which is a retype.

    A row that simply has nothing in its overflow is left alone and named by nothing. An
    empty overflow is the column's normal state, so reporting one would report the ordinary
    case and drown the three above.

    The price of finding a retype is named rather than hidden. A promotion on its own needs
    only the rows whose version misses a column, and a retype can sit on any row at a
    recorded version, so every one of those rows is decoded and asked for each reachable
    column. A row whose overflow is absent or empty still costs nothing, because such a row
    is never decoded, which leaves a steady-state read where it was. What pays is a
    ticker-day the vendor drifted through, measured at about a second per 200,000 chains
    rows against about a fiftieth of that before. That read is the one this module exists
    for, so the cost lands where the answer does.
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

    # Every column a value in an overflow could belong to, which is every column the
    # running schema carries and a vendor field overflows into. A promotion lifts out of
    # this set and a retype is found inside it, so both questions are asked of one list.
    reachable = sorted(column for column in paths if column in fingerprint)

    # Which columns each version in the table is missing, and what shape it recorded,
    # asked of the ledger once per version rather than once per row.
    targets: dict[int, frozenset[str]] = {}
    shapes: dict[int, Mapping[str, str]] = {}
    unrecorded: set[int] = set()
    for version in set(versions):
        entry = ledger.get(version)
        # A version recorded for other surfaces and not this one is as unrecorded as one
        # that is absent outright. ``has_column`` answers false for every column of a
        # surface it holds no shape for, which reads identically to a version that carried
        # none, and that would fill every projectable column off the overflow while
        # reporting the read as whole.
        if entry is None or not entry.fingerprints.get(surface):
            unrecorded.add(version)
            targets[version] = frozenset()
            continue
        shapes[version] = entry.fingerprints[surface]
        targets[version] = frozenset(
            column for column in reachable if not entry.has_column(surface, column)
        )

    # Every column any version in the table is missing, each one's cells started from what
    # the table already holds. A column the table does not have starts all null.
    wanted = sorted({name for missing in targets.values() for name in missing})
    cells: dict[str, list[object]] = {
        column: (table.column(column).to_pylist() if column in present else [None] * table.num_rows)
        for column in wanted
    }
    # The type each filled column is rebuilt at. A column the table already has keeps its
    # own, so a rebuild never retypes a cell this projection promised to leave alone, and a
    # partition merged across a retype cannot lose the whole read to one truncating cast.
    # A column being added takes the running schema's, since the table has no opinion.
    fields: dict[str, pa.Field] = {
        column: (table.schema.field(column) if column in present else schema.field(column))
        for column in wanted
    }

    filled: dict[str, int] = {}
    refused: dict[tuple[str, int, str], int] = {}
    routed: dict[tuple[str, int], int] = {}
    # Rows outer, columns inner, so a row's overflow is decoded once however many columns
    # it feeds. A row at a version the ledger has no shape for is never decoded at all,
    # because without the shape nothing here can tell a promotion from a retype.
    for row, version in enumerate(versions):
        if version in unrecorded:
            continue
        overflow = _overflow(extras[row], row)
        if not overflow:
            continue
        missing = targets[version]
        for column in reachable:
            raw = _lookup(overflow, paths[column])
            if raw is None:
                continue
            if column not in missing:
                # The version carried this column, so the parser wrote the value here
                # after the column refused it. Presenting it would take a cast.
                routed_key = (column, version)
                routed[routed_key] = routed.get(routed_key, 0) + 1
                continue
            converted, detail = _convert(fields[column].type, raw)
            if detail is not None:
                key = (column, version, detail)
                refused[key] = refused.get(key, 0) + 1
                continue
            cells[column][row] = converted
            filled[column] = filled.get(column, 0) + 1

    projected = table
    for column in wanted:
        if column not in filled:
            continue
        field = fields[column]
        array = journal.typed_column(field.type, cells[column])
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
        retyped=tuple(
            RetypedColumn(
                column=column,
                schema_version=version,
                recorded_type=shapes[version][column],
                rows=rows,
            )
            for (column, version), rows in sorted(routed.items())
        ),
    )


__all__ = [
    "EXTRA_COLUMN",
    "VERSION_COLUMN",
    "ExtraProjection",
    "ExtraProjectionError",
    "RetypedColumn",
    "UnfitValue",
    "project_extra",
]
