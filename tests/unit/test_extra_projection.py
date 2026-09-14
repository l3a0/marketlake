"""The read-time projection of a promoted field out of ``extra``, decided from values alone.

These build tables in memory and hand-build ledgers. No file, process, or query engine is
crossed, so they sit in the unit tier. The end-to-end promotion, over real segments and a
real ledger file, is ``tests/component/test_extra_projection.py``.

Every ledger here is built from the running schema with a column removed, rather than from
a hand-typed column list. So the fixtures cannot drift from the schema they describe, and a
column renamed in the schema fails these tests at the removal rather than passing against a
shape the code no longer has.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from lake import journal
from lake.extra_projection import (
    EXTRA_COLUMN,
    VERSION_COLUMN,
    ExtraProjectionError,
    project_extra,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger

RECORDED_AT = datetime(2026, 9, 13, 15, 0, tzinfo=UTC)

# The three columns every synthetic table here carries. A real journal table carries far
# more, and the projection reads only these two plus whatever column it is filling, so a
# narrow table proves the same thing a wide one would and says what it depends on.
ROW_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        (VERSION_COLUMN, pa.int64()),
        (EXTRA_COLUMN, pa.string()),
    ]
)


def _shape() -> dict[str, dict[str, str]]:
    """The running shape of every pinned surface, which is what the newest version holds."""
    return {name: dict(journal.schema_fingerprint(name)) for name in journal.PINNED_SURFACES}


def _shape_without(surface: str, *columns: str) -> dict[str, dict[str, str]]:
    """The running shape of every pinned surface, with ``columns`` gone from ``surface``.

    This is what a version below a promotion looked like: the same schema minus the
    column the promotion added. Removing by key rather than restating the shape means a
    column that does not exist fails here loudly, through ``KeyError``.
    """
    shape = _shape()
    for column in columns:
        del shape[surface][column]
    return shape


def _ledger(*entries: tuple[int, dict[str, dict[str, str]]]) -> SchemaVersionLedger:
    """A ledger recording each ``(version, shape)`` pair."""
    return SchemaVersionLedger(
        RecordedVersion(version=version, recorded_at=RECORDED_AT, fingerprints=shape)
        for version, shape in entries
    )


def _rows(*rows: dict[str, object], schema: pa.Schema = ROW_SCHEMA) -> pa.Table:
    """A table of the given rows, typed by ``schema``."""
    return pa.Table.from_pylist(list(rows), schema=schema)


def _row(version: int, overflow: dict | None = None, **columns: object) -> dict[str, object]:
    """One synthetic journal row: a ticker, a version, and an optional overflow."""
    return {
        "ticker": "SPY",
        VERSION_COLUMN: version,
        EXTRA_COLUMN: None if overflow is None else json.dumps(overflow, sort_keys=True),
        **columns,
    }


# -- the promotion boundary ---------------------------------------------------


def test_a_field_the_row_s_version_had_no_column_for_reads_as_that_column():
    """The whole point. Version 1 held it in the overflow, and it reads as ``bid``.

    The column is absent from the input table entirely, which is what a partition written
    wholly below the promotion looks like when it is read on its own.
    """
    table = _rows(_row(1, {"bid": 4.25}))
    assert "bid" not in table.column_names

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.table.column("bid").to_pylist() == [4.25]
    assert result.table.schema.field("bid").type == journal.schema_for("chains").field("bid").type
    assert result.filled == {"bid": 1}
    assert result.complete


def test_a_row_whose_version_carried_the_column_is_left_exactly_as_written():
    """A version that had the column is authoritative, overflow or no overflow.

    A stale key in the overflow must never win over the column, because above the
    promotion the column is where the parser put the value.
    """
    table = _rows(
        _row(2, {"bid": 99.0}, bid=4.25),
        schema=ROW_SCHEMA.append(pa.field("bid", pa.float64())),
    )

    result = project_extra(table, surface="chains", ledger=_ledger((2, _shape())))

    assert result.table.column("bid").to_pylist() == [4.25]
    assert result.filled == {}


def test_one_table_spanning_the_boundary_reads_as_one_shape():
    """A merged partition holds both halves, and only the older half is filled.

    This is the mutation that matters most. Reading one version's shape for every row
    breaks in both directions: the version-1 row goes unfilled, or the version-2 row is
    clobbered from its own overflow.
    """
    schema = ROW_SCHEMA.append(pa.field("bid", pa.float64()))
    table = _rows(
        _row(1, {"bid": 4.25}),
        _row(2, {"bid": 99.0}, bid=4.30),
        _row(2, None, bid=4.31),
        schema=schema,
    )

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger(
            (1, _shape_without("chains", "bid")),
            (2, _shape()),
        ),
    )

    assert result.table.column("bid").to_pylist() == [4.25, 4.30, 4.31]
    assert result.filled == {"bid": 1}


def test_two_columns_promoted_at_once_are_both_filled():
    """The projection is per column, so one version can be missing several."""
    table = _rows(_row(1, {"bid": 4.25, "askSize": 7}))

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid", "ask_size"))),
    )

    assert result.table.column("bid").to_pylist() == [4.25]
    assert result.table.column("ask_size").to_pylist() == [7]
    assert result.filled == {"ask_size": 1, "bid": 1}


def test_a_column_no_row_carries_a_value_for_is_not_manufactured():
    """Filling a column is the job. Adding an empty one is not.

    An all-null column claims a shape the input does not have, and reshaping a table to
    the running schema belongs to the loader rather than here.
    """
    table = _rows(_row(1, {"askSize": 7}))

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid", "ask_size"))),
    )

    assert "bid" not in result.table.column_names
    assert result.filled == {"ask_size": 1}


def test_the_overflow_column_is_never_touched():
    """The raw record survives the projection, so the value is readable twice over."""
    table = _rows(_row(1, {"bid": 4.25}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.table.column(EXTRA_COLUMN).to_pylist() == ['{"bid": 4.25}']


def test_every_other_column_keeps_its_place_and_its_values():
    """Filling one column moves nothing else. A promoted column lands at the end."""
    schema = ROW_SCHEMA.append(pa.field("bid", pa.float64()))
    table = _rows(_row(1, {"ask": 4.30}, bid=4.25), schema=schema)

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "ask")))
    )

    assert result.table.column_names == [*schema.names, "ask"]
    assert result.table.column("bid").to_pylist() == [4.25]
    assert result.table.column("ticker").to_pylist() == ["SPY"]


# -- the quotes surface's nested overflow -------------------------------------


def test_a_quotes_block_key_reaches_the_right_column():
    """The quotes overflow nests under the block the field arrived in."""
    table = _rows(_row(1, {"fundamental": {"peRatio": 27.5}}))

    result = project_extra(
        table, surface="quotes", ledger=_ledger((1, _shape_without("quotes", "pe_ratio")))
    )

    assert result.table.column("pe_ratio").to_pylist() == [27.5]


def test_the_two_blocks_sharing_a_field_name_land_in_their_own_columns():
    """``quote.lastPrice`` and ``extended.lastPrice`` are different measurements.

    A flat lookup would read one of them into both columns, which is the collision the
    nested overflow exists to prevent.
    """
    table = _rows(
        _row(1, {"quote": {"lastPrice": 5.5}, "extended": {"lastPrice": 6.5}}),
    )

    result = project_extra(
        table,
        surface="quotes",
        ledger=_ledger((1, _shape_without("quotes", "last", "extended_last_price"))),
    )

    assert result.table.column("last").to_pylist() == [5.5]
    assert result.table.column("extended_last_price").to_pylist() == [6.5]


def test_a_flat_key_on_the_quotes_surface_reaches_nothing():
    """A quotes overflow is nested, so a bare key names no column."""
    table = _rows(_row(1, {"peRatio": 27.5}))

    result = project_extra(
        table, surface="quotes", ledger=_ledger((1, _shape_without("quotes", "pe_ratio")))
    )

    assert result.filled == {}
    assert "pe_ratio" not in result.table.column_names


# -- a version the ledger has no shape for ------------------------------------


def test_a_version_absent_from_the_ledger_leaves_its_rows_exactly_as_written():
    """Nothing is guessed for a version whose shape was never recorded.

    A bump made without running ``python -m lake.schema_versions`` leaves the lake
    holding rows at a version recorded nowhere, which is marketlake #130. Until that
    lands the condition is live, so the projection has to answer it. Treating the version
    as carrying no columns would fill every column from the overflow, and refusing to read
    at all would make the unrecorded rows unreadable through the one path meant to
    investigate them.
    """
    table = _rows(_row(7, {"bid": 4.25}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert "bid" not in result.table.column_names
    assert result.filled == {}
    assert result.unrecorded_versions == (7,)
    assert not result.complete


def test_an_empty_ledger_names_every_version_it_could_not_place():
    """The report is a set of versions, ascending, not a bare flag."""
    table = _rows(_row(3, {"bid": 4.25}), _row(1, {"bid": 4.30}), _row(3, {"bid": 4.31}))

    result = project_extra(table, surface="chains", ledger=SchemaVersionLedger())

    assert result.unrecorded_versions == (1, 3)
    assert result.table == table


def test_a_recorded_version_beside_an_unrecorded_one_is_still_projected():
    """One missing shape costs its own rows and no others."""
    table = _rows(_row(1, {"bid": 4.25}), _row(7, {"bid": 4.30}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.table.column("bid").to_pylist() == [4.25, None]
    assert result.unrecorded_versions == (7,)


# -- a value the column refuses -----------------------------------------------


def test_a_value_the_column_refuses_stays_in_the_overflow_and_is_reported():
    """A promotion cannot decide the past's types, so a refusal is reported, not raised.

    The rest of the table still reads, and the raw value is still in the overflow, so
    nothing is lost by continuing.
    """
    table = _rows(_row(1, {"bid": "n/a"}), _row(1, {"bid": 4.25}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.table.column("bid").to_pylist() == [None, 4.25]
    assert result.filled == {"bid": 1}
    assert [(u.column, u.schema_version, u.rows) for u in result.unfit] == [("bid", 1, 1)]
    assert "n/a" in result.table.column(EXTRA_COLUMN).to_pylist()[0]
    assert not result.complete


def test_a_boolean_is_refused_by_a_numeric_column_rather_than_landing_as_one():
    """Arrow turns ``True`` into ``1.0`` for a floating column without complaint.

    That is the one conversion here that changes a value silently, so it is checked by
    hand. A comparison against ``1.0`` would pass on the broken behaviour, so the test
    asserts the cell is null and the refusal is reported.
    """
    table = _rows(_row(1, {"bid": True}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {}
    assert [u.column for u in result.unfit] == ["bid"]
    assert "boolean" in result.unfit[0].detail


def test_a_fractional_float_is_refused_by_an_integer_column_rather_than_truncating():
    """The writer's own refusal, reached through the projection.

    ``journal.typed_column`` is the one builder both sides use, so a value the capture
    path would have refused cannot arrive through the read path instead.
    """
    table = _rows(_row(1, {"openInterest": 3.7}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "open_interest")))
    )

    assert result.filled == {}
    assert [u.column for u in result.unfit] == ["open_interest"]


def test_a_lossless_float_still_lands_in_an_integer_column_as_an_integer():
    """``1500.0`` is the same integer, so it lands, and it lands typed.

    Comparing values alone would pass on a column that came back as a double, so the
    column's type is asserted too.
    """
    table = _rows(_row(1, {"openInterest": 1500.0}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "open_interest")))
    )

    column = result.table.column("open_interest")
    assert column.type == pa.int64()
    assert column.to_pylist() == [1500]
    assert isinstance(column.to_pylist()[0], int)


def test_refusals_of_the_same_shape_are_counted_rather_than_repeated():
    """One line per column, version, and reason, with the row count beside it.

    Two rows refused for the same reason are one line counting two, and a third refused
    for a different reason is its own. The lines come back sorted, so a message built from
    them reads the same on every run.
    """
    table = _rows(
        _row(1, {"bid": "n/a"}),
        _row(1, {"bid": "n/a"}),
        _row(1, {"bid": "x"}),
        _row(1, {"bid": True}),
    )

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert [(u.column, u.schema_version, u.rows) for u in result.unfit] == [
        ("bid", 1, 2),
        ("bid", 1, 1),
        ("bid", 1, 1),
    ]
    assert [u.detail for u in result.unfit] == sorted(u.detail for u in result.unfit)
    assert "n/a" in result.unfit[0].detail
    assert "'x'" in result.unfit[1].detail
    assert "boolean" in result.unfit[2].detail


# -- what an overflow does not hold -------------------------------------------


def test_a_row_with_no_overflow_fills_nothing():
    table = _rows(_row(1, None), _row(1, {}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {}
    assert result.complete


def test_a_json_null_in_the_overflow_fills_nothing():
    """A null in the overflow says the vendor sent a null, which the cell already says."""
    table = _rows(_row(1, {"bid": None}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {}


def test_an_overflow_key_naming_no_column_is_left_alone():
    """An unrecognised field stays unrecognised until something promotes it."""
    table = _rows(_row(1, {"sigmaScore": 0.42}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {}
    assert "sigma_score" not in result.table.column_names


# -- what a journal table must carry ------------------------------------------


@pytest.mark.parametrize("missing", [VERSION_COLUMN, EXTRA_COLUMN])
def test_a_table_without_the_two_columns_the_projection_reads_refuses(missing):
    schema = pa.schema([field for field in ROW_SCHEMA if field.name != missing])
    table = pa.Table.from_pylist([{"ticker": "SPY"}], schema=schema).select(schema.names)

    with pytest.raises(ExtraProjectionError, match=missing):
        project_extra(table, surface="chains", ledger=SchemaVersionLedger())


def test_a_row_with_no_schema_version_refuses():
    """Every builder stamps the version, so a null one did not come from the journal."""
    table = _rows(
        _row(1, {"bid": 4.25}), {"ticker": "SPY", VERSION_COLUMN: None, EXTRA_COLUMN: None}
    )

    with pytest.raises(ExtraProjectionError, match="row 1"):
        project_extra(table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid"))))


@pytest.mark.parametrize("overflow", ["{not json", '"a string"', "[1, 2]"])
def test_an_overflow_that_is_not_a_json_object_refuses(overflow):
    """``extra`` is written by ``json.dumps`` of a dict on both surfaces.

    Reading past a value that will not parse would present the row as whole while its
    overflow is unreadable, which is the opposite of what the column exists for.
    """
    table = _rows({"ticker": "SPY", VERSION_COLUMN: 1, EXTRA_COLUMN: overflow})

    with pytest.raises(ExtraProjectionError, match="row 0"):
        project_extra(table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid"))))


def test_an_unknown_surface_refuses():
    table = _rows(_row(1, None))

    with pytest.raises(ValueError, match="unknown surface"):
        project_extra(table, surface="bars", ledger=SchemaVersionLedger())
