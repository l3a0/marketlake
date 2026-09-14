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


def test_every_other_column_keeps_its_place_and_promoted_ones_land_sorted():
    """Filling a column moves nothing else, and added columns arrive in a fixed order.

    Four columns are added rather than one. The set they are gathered in has no order of
    its own, so a single added column would assert nothing about the order they come back
    in, and a reader comparing two projections of the same partition needs that order not
    to move between runs.
    """
    schema = ROW_SCHEMA.append(pa.field("open_interest", pa.int64()))
    table = _rows(
        _row(
            1,
            {"symbol": "SPY   260918C00650000", "last": 4.22, "bid": 4.25, "ask": 4.30},
            open_interest=12,
        ),
        schema=schema,
    )

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid", "ask", "last", "occ_symbol"))),
    )

    assert result.table.column_names == [*schema.names, "ask", "bid", "last", "occ_symbol"]
    assert result.table.column("open_interest").to_pylist() == [12]
    assert result.table.column("ticker").to_pylist() == ["SPY"]


def test_two_versions_each_missing_a_different_column_never_cross():
    """The per-row decision is per column, not per row.

    Every other fixture here has one version missing something and the rest missing
    nothing, which makes a row-level guard look like a column-level one. Here version 1
    lacks ``bid`` and version 2 lacks ``ask``, and each row carries a stale overflow for
    the column its own version did have. A guard that stops at the row fills both from the
    overflow, which overwrites a value the parser authored.
    """
    schema = ROW_SCHEMA.append(pa.field("bid", pa.float64())).append(pa.field("ask", pa.float64()))
    table = _rows(
        _row(1, {"bid": 4.25, "ask": 99.0}, ask=4.30),
        _row(2, {"bid": 99.0, "ask": 4.35}, bid=4.31),
        schema=schema,
    )

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger(
            (1, _shape_without("chains", "bid")),
            (2, _shape_without("chains", "ask")),
        ),
    )

    assert result.table.column("bid").to_pylist() == [4.25, 4.31]
    assert result.table.column("ask").to_pylist() == [4.30, 4.35]
    assert result.filled == {"ask": 1, "bid": 1}


@pytest.mark.parametrize(
    ("column", "vendor"),
    [
        ("number_of_contracts", "numberOfContracts"),
        ("is_chain_truncated", "isChainTruncated"),
        ("snap_ts", "snapTs"),
    ],
)
def test_a_column_no_vendor_field_overflows_into_is_never_a_candidate(column, vendor):
    """A version missing a non-vendor column still projects nothing into it.

    ``extra_paths`` already refuses these, but nothing drove the projection with such a
    version. Widening the candidates from the vendor-mapped columns to every schema column
    would reach for an overflow key that does not exist, and a recomputed chain-level
    column really can be absent from an older version's shape.
    """
    table = _rows(_row(1, {vendor: 4.25, "bid": 4.30}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", column, "bid")))
    )

    assert result.filled == {"bid": 1}
    assert column not in result.table.column_names


def test_the_fill_count_is_the_rows_filled_and_not_the_columns_touched():
    """Two rows filling one column count two."""
    table = _rows(_row(1, {"bid": 4.25}), _row(1, {"bid": 4.30}), _row(1, {"bid": 4.31}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {"bid": 3}
    assert result.table.column("bid").to_pylist() == [4.25, 4.30, 4.31]


def test_a_row_at_a_fully_recorded_version_is_never_read_for_its_overflow():
    """A read has no business failing over an overflow it was never going to look at.

    Version 2 is missing nothing, so its row is skipped before its ``extra`` is decoded.
    That is what makes the JSON refusal a check on rows the projection reads rather than a
    validation pass over the whole table.
    """
    schema = ROW_SCHEMA.append(pa.field("bid", pa.float64()))
    table = _rows(
        _row(1, {"bid": 4.25}),
        {"ticker": "SPY", VERSION_COLUMN: 2, EXTRA_COLUMN: "{not json", "bid": 4.30},
        schema=schema,
    )

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid")), (2, _shape())),
    )

    assert result.table.column("bid").to_pylist() == [4.25, 4.30]
    assert result.filled == {"bid": 1}


def test_an_empty_overflow_string_is_no_overflow_rather_than_bad_json():
    """``""`` is not JSON, and it is also not a row with something in its overflow."""
    table = _rows({"ticker": "SPY", VERSION_COLUMN: 1, EXTRA_COLUMN: ""})

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.filled == {}
    assert result.complete


def test_refusals_at_different_versions_stay_apart():
    """A refusal names the version whose row hit it, not a fixed one.

    Every other refusal fixture sits at version 1, which makes a hard-coded version
    indistinguishable from the row's own.
    """
    table = _rows(_row(1, {"bid": "n/a"}), _row(2, {"bid": "n/a"}))

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger(
            (1, _shape_without("chains", "bid")),
            (2, _shape_without("chains", "bid")),
        ),
    )

    assert [(u.column, u.schema_version, u.rows) for u in result.unfit] == [
        ("bid", 1, 1),
        ("bid", 2, 1),
    ]


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


@pytest.mark.parametrize("block", [27.5, [1, 2], "text", None])
def test_a_quotes_block_that_is_not_an_object_fills_nothing(block):
    """A block key holding a scalar is drift, not a field value.

    Reading the block itself as the value would put a whole block's stand-in into one
    column, which is worse than leaving the cell null, because the null is honest and the
    raw value is still in the overflow.
    """
    table = _rows(_row(1, {"fundamental": block}))

    result = project_extra(
        table, surface="quotes", ledger=_ledger((1, _shape_without("quotes", "pe_ratio")))
    )

    assert result.filled == {}
    assert "pe_ratio" not in result.table.column_names
    assert result.complete


def test_a_flat_key_on_the_quotes_surface_reaches_nothing():
    """A quotes overflow is nested, so a bare key names no column."""
    table = _rows(_row(1, {"peRatio": 27.5}))

    result = project_extra(
        table, surface="quotes", ledger=_ledger((1, _shape_without("quotes", "pe_ratio")))
    )

    assert result.filled == {}
    assert "pe_ratio" not in result.table.column_names


def test_an_envelope_key_reaches_its_column_through_the_envelope_block():
    """The two quotes fields that belong to no captured block still read back.

    They sit under ``envelope`` rather than a vendor block key, and the reader finds them
    the same way it finds a block's, because both are one nested path in ``extra_paths``.
    """
    table = _rows(_row(1, {"envelope": {"realtime": True, "cusip": "111111111"}}))

    result = project_extra(
        table, surface="quotes", ledger=_ledger((1, _shape_without("quotes", "realtime", "cusip")))
    )

    assert result.table.column("realtime").to_pylist() == [True]
    assert result.table.column("cusip").to_pylist() == ["111111111"]
    assert result.complete


# -- the chains surface's chain-level block ------------------------------------


def test_a_chain_level_key_reaches_its_column_through_the_chain_block():
    """The chains overflow is flat for a contract field and nested for a chain-level one."""
    table = _rows(_row(1, {"bid": 4.25, "chain": {"underlyingPrice": 650.01}}))

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid", "underlying_price"))),
    )

    assert result.table.column("bid").to_pylist() == [4.25]
    assert result.table.column("underlying_price").to_pylist() == [650.01]
    assert result.filled == {"bid": 1, "underlying_price": 1}


def test_a_flat_chain_level_key_reaches_nothing():
    """A bare ``underlyingPrice`` is a contract field, and it names no chains column.

    That is what the nesting buys. A flat lookup would read an unrecognized contract field
    into the chain-level column, presenting one measurement as another.
    """
    table = _rows(_row(1, {"underlyingPrice": 1.5}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "underlying_price")))
    )

    assert result.filled == {}
    assert "underlying_price" not in result.table.column_names


def test_a_contract_key_beside_the_chain_level_one_never_wins():
    """One overflow holding both levels' ``underlyingPrice`` fills from the chain's.

    This is the row the flat shape could not represent at all: a vendor sending the name at
    both levels. The flat key belongs to no column, so the nested value is the only
    candidate and the column reads the chain's price rather than the contract's.
    """
    table = _rows(_row(1, {"underlyingPrice": 1.5, "chain": {"underlyingPrice": 650.01}}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "underlying_price")))
    )

    assert result.table.column("underlying_price").to_pylist() == [650.01]
    assert result.filled == {"underlying_price": 1}


@pytest.mark.parametrize("block", [650.01, [1, 2], "text", None])
def test_a_chain_block_that_is_not_an_object_fills_nothing(block):
    """A ``chain`` key holding a scalar is drift, not a chain-level value.

    The chains overflow gained a nested level, so it gained this case too. Reading the key
    itself as the value would put a whole level's stand-in into one column, and the null it
    would replace is honest while the raw value is still in the overflow.
    """
    table = _rows(_row(1, {"chain": block}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "underlying_price")))
    )

    assert result.filled == {}
    assert "underlying_price" not in result.table.column_names
    assert result.complete


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
    """The report is every version once, ascending, not a bare flag.

    The versions are 9, 1 and 17 rather than 1, 2 and 3 on purpose. A small-integer set
    built in ascending order iterates in that order anyway, so a fixture like that asserts
    the sort against a collection already sorted and passes with the sort removed. These
    three iterate as 9, 1, 17.
    """
    table = _rows(
        _row(9, {"bid": 4.25}),
        _row(1, {"bid": 4.30}),
        _row(17, {"bid": 4.31}),
        _row(9, {"bid": 4.32}),
    )

    result = project_extra(table, surface="chains", ledger=SchemaVersionLedger())

    assert result.unrecorded_versions == (1, 9, 17)
    assert result.table == table


def test_a_recorded_version_beside_an_unrecorded_one_is_still_projected():
    """One missing shape costs its own rows and no others."""
    table = _rows(_row(1, {"bid": 4.25}), _row(7, {"bid": 4.30}))

    result = project_extra(
        table, surface="chains", ledger=_ledger((1, _shape_without("chains", "bid")))
    )

    assert result.table.column("bid").to_pylist() == [4.25, None]
    assert result.unrecorded_versions == (7,)


def test_a_version_recorded_for_another_surface_only_is_treated_as_unrecorded():
    """A ledger holding no shape for this surface knows nothing about this surface.

    ``has_column`` answers false for every column of a surface it has no entry for, which
    reads exactly like a version that carried none. Taken at face value that fills every
    projectable column off the overflow and calls the read whole, which is the outcome the
    unrecorded branch exists to refuse. Reaching it by a second door is the same defect.
    """
    table = _rows(_row(1, {"bid": 4.25, "ask": 4.30}))
    quotes_only = _shape()
    del quotes_only["chains"]

    result = project_extra(table, surface="chains", ledger=_ledger((1, quotes_only)))

    assert result.filled == {}
    assert "bid" not in result.table.column_names
    assert result.unrecorded_versions == (1,)
    assert not result.complete


def test_a_version_recorded_with_an_empty_shape_for_the_surface_is_treated_the_same():
    """An entry naming the surface and no column is the same absence one level down."""
    table = _rows(_row(1, {"bid": 4.25}))
    empty = {**_shape(), "chains": {}}

    result = project_extra(table, surface="chains", ledger=_ledger((1, empty)))

    assert result.filled == {}
    assert result.unrecorded_versions == (1,)


def test_the_other_surface_s_shape_never_decides_this_surface_s_columns():
    """Both surfaces carry a ``bid``, so a lookup on the wrong one would pass unnoticed.

    Chains records ``bid`` and quotes does not, and the table is a chains table. Reading
    the quotes shape would fill ``bid`` from the overflow, which chains never promoted.
    """
    table = _rows(_row(1, {"bid": 4.25}))
    shape = _shape()
    del shape["quotes"]["bid"]

    result = project_extra(table, surface="chains", ledger=_ledger((1, shape)))

    assert result.filled == {}
    assert result.complete


# -- a column the table already holds at another type -------------------------


def test_a_cell_the_projection_leaves_alone_keeps_its_own_type():
    """Rebuilding a column must not retype the rows it was not asked to touch.

    A partition merged across a retype holds the column at the older type while the
    running schema names the newer one. Rebuilding at the running type would rewrite every
    untouched cell, which contradicts the rule that a row whose version carried the column
    is left alone. Comparing values alone would pass on that, since ``4 == 4.0``, so the
    column's type is asserted too.
    """
    schema = ROW_SCHEMA.append(pa.field("bid", pa.int64()))
    table = _rows(_row(2, None, bid=4), _row(1, {"bid": 5}), schema=schema)

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid")), (2, _shape())),
    )

    column = result.table.column("bid")
    assert column.type == pa.int64()
    assert column.to_pylist() == [4, 5]
    assert all(isinstance(value, int) for value in column.to_pylist())


def test_a_value_that_does_not_fit_the_column_s_own_type_is_refused_not_cast():
    """The target type is the column that is there, not the one the schema wants.

    ``bid`` is ``int64`` on this table and ``double`` in the running schema. A fractional
    overflow value fits the schema's type and not the table's, and the table's is the one
    the cell has to live in.
    """
    schema = ROW_SCHEMA.append(pa.field("bid", pa.int64()))
    table = _rows(_row(2, None, bid=4), _row(1, {"bid": 5.5}), schema=schema)

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "bid")), (2, _shape())),
    )

    assert result.table.column("bid").to_pylist() == [4, None]
    assert [u.column for u in result.unfit] == ["bid"]


def test_a_column_the_table_holds_at_a_type_the_schema_refuses_still_reads():
    """One cell the running type cannot hold must not cost the whole read.

    ``open_interest`` is ``int64`` in the running schema, and Arrow refuses ``3.7`` into
    it outright. A rebuild at the running type would raise and lose every row, including
    the ones this projection was asked about.
    """
    schema = ROW_SCHEMA.append(pa.field("open_interest", pa.float64()))
    table = _rows(
        _row(2, None, open_interest=3.7),
        _row(1, {"openInterest": 12}),
        schema=schema,
    )

    result = project_extra(
        table,
        surface="chains",
        ledger=_ledger((1, _shape_without("chains", "open_interest")), (2, _shape())),
    )

    column = result.table.column("open_interest")
    assert column.type == pa.float64()
    assert column.to_pylist() == [3.7, 12.0]


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
    # Inserted boolean first and "n/a" last, so the order they are reported in cannot be
    # the order they were counted in. A fixture already in sorted order asserts nothing.
    table = _rows(
        _row(1, {"bid": True}),
        _row(1, {"bid": "x"}),
        _row(1, {"bid": "n/a"}),
        _row(1, {"bid": "n/a"}),
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
