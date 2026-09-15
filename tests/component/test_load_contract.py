"""``load_contract`` against a fixture lake on disk.

``load_contract`` shares its whole body with ``load_chain`` and ``load_quotes`` through
``loader._open_partition`` and ``loader._fetch_selection``, per marketlake #266. That
machinery, the path build, the spelling check, the quarantine guard, the overflow
projection, and the fetch predicate's overflow half, is already exercised against the
chains surface in ``tests/component/test_load_chain.py``. This file exercises what is new:
a selection supplied directly as one OCC symbol rather than resolved off a whole-partition
scan, the ticker derived from the symbol's OCC root, the instant sort, and the absent-versus-
partial distinction a series has that a single minute does not.

The numbered tests carry the numbering marketlake #266 asks for, so a mutation the issue
names points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake.extra_projection import ExtraProjectionError
from lake.loader import (
    ContractAbsent,
    LoadError,
    PartialRead,
    PartitionAbsent,
    PartitionQuarantined,
    load_contract,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from tests.support.lake import FixtureLake, sample_chains_table

FULL_DAY = "2026-09-14"

# When the schema-version ledger recorded version 1. Any instant does, since the loader
# reads the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

CALL = "SPY   260918C00650000"
PUT = "SPY   260918P00650000"
STRADDLE = "SPY   260918C00655000"

FULL_PARTITION = f"chains/ticker=SPY/date={FULL_DAY}.parquet"


def _data(snap: str | None, occ: str = CALL, close_tag: str | None = None) -> dict:
    """One chains data row: a vendor observation at ``snap`` for ``occ``."""
    return {
        "snap_ts": snap,
        "fetch_ts": _stamped(snap, "400"),
        "vendor_quote_ts": _stamped(snap, "150"),
        "ticker": "SPY",
        "occ_symbol": occ,
        "bid": 4.20,
        "ask": 4.25,
        "last": 4.22,
        "open_interest": 1234,
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
    }


def _stamped(snap: str | None, millis: str) -> str | None:
    if snap is None:
        return None
    instant, offset = snap[:19], snap[19:]
    return f"{instant}.{millis}{offset}"


def _gap(snap: str, close_tag: str | None = None) -> dict:
    """One gap row: a minute that was attempted and missed, every vendor column null."""
    row = _data(snap, close_tag=close_tag)
    row.update(
        {
            "occ_symbol": None,
            "bid": None,
            "ask": None,
            "last": None,
            "open_interest": None,
            "row_kind": "gap",
            "error_class": "vendor_timeout",
        }
    )
    return row


def _with_extra(row: dict, overflow: dict) -> dict:
    return {**row, "extra": json.dumps(overflow)}


# The full session. CALL runs every cycle written in ascending order, so a mutation that
# skips the sort entirely would still pass a test built only on CALL. STRADDLE is what
# catches that: its three cycles are written in one order, sort as text in a second
# order, and sort by instant in a third, all three different. Its middle row is the
# issue's own illustration of the trap: written as an Eastern offset naming 17:00Z, it
# sorts as text ahead of a +00:00 spelling naming the earlier instant 14:00Z.
FULL_ROWS = [
    _data("2026-09-14T13:30:00+00:00", CALL),
    _data("2026-09-14T13:30:00+00:00", PUT),
    _data("2026-09-14T18:31:00+00:00", CALL),
    _data("2026-09-14T20:00:00+00:00", STRADDLE),
    _data("2026-09-14T13:00:00-04:00", STRADDLE),
    _data("2026-09-14T14:00:00+00:00", STRADDLE),
    _gap("2026-09-14T19:00:00+00:00"),
    _data("2026-09-14T20:00:00+00:00", CALL, close_tag="option_close"),
]


def _ledger_table():
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _lake(
    fixture_lake: FixtureLake,
    rows: list[dict] | None = None,
    *,
    quarantine: list[dict] | None = None,
) -> Path:
    table = sample_chains_table(rows if rows is not None else FULL_ROWS)
    fixture_lake.with_chains("SPY", FULL_DAY, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _snaps(table: pa.Table) -> list[str]:
    return table.column("snap_ts").to_pylist()


# -- resolving one contract's series -----------------------------------------


def test_a_contract_present_all_session_returns_one_row_per_minute_ordered_by_snap_ts(
    fixture_lake: FixtureLake,
):
    """#266 test 1. Every cycle CALL carries, in time order."""
    root = _lake(fixture_lake)

    table = load_contract(CALL, FULL_DAY, lake_root=root)

    assert set(table.column("occ_symbol").to_pylist()) == {CALL}
    assert _snaps(table) == [
        "2026-09-14T13:30:00+00:00",
        "2026-09-14T18:31:00+00:00",
        "2026-09-14T20:00:00+00:00",
    ]


def test_a_contract_absent_from_the_partition_raises_naming_symbol_and_day(
    fixture_lake: FixtureLake,
):
    """#266 test 2. The partition exists and simply never carries this symbol."""
    root = _lake(fixture_lake)
    absent = "SPY   260918C00999000"

    with pytest.raises(ContractAbsent) as caught:
        load_contract(absent, FULL_DAY, lake_root=root)

    assert caught.value.occ_symbol == absent
    assert caught.value.ticker == "SPY"
    assert caught.value.day == FULL_DAY
    assert absent in str(caught.value)
    assert FULL_DAY in str(caught.value)


def test_gap_rows_never_appear_in_the_result(fixture_lake: FixtureLake):
    """#266 test 3. Excluded by kind, not merely by a gap row's usually-null occ_symbol.

    A real gap row nulls every vendor column, occ_symbol included, so it would never
    satisfy an occ_symbol selection even without a row_kind check. This row keeps CALL's
    symbol so the guard actually being exercised is the row_kind filter itself.
    """
    rows = [*FULL_ROWS, {**_data("2026-09-14T15:00:00+00:00", CALL), "row_kind": "gap"}]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    table = load_contract(CALL, FULL_DAY, lake_root=root)

    assert set(table.column("row_kind").to_pylist()) == {"data"}
    assert table.num_rows == 3


def test_a_shuffled_partition_still_returns_the_series_in_snap_ts_order(
    fixture_lake: FixtureLake,
):
    """#266 test 4, and mutation 1. Ordering is inherited neither from the file nor from text.

    STRADDLE's three cycles are written latest-instant-first, sort as text into a second
    order, and sort by instant into a third. All three disagree, so this fails a loader
    that skips the sort and returns file order, and it fails one that sorts on the stored
    text instead of the parsed instant. The middle two rows are the issue's own example
    of that second trap: ``T13:00:00-04:00`` names 17:00Z and ``T14:00:00+00:00`` names
    the earlier 14:00Z, and a text sort puts the Eastern spelling first regardless,
    because the comparison never looks past the ``13`` and the ``14``.
    """
    root = _lake(fixture_lake)

    table = load_contract(STRADDLE, FULL_DAY, lake_root=root)

    assert _snaps(table) == [
        "2026-09-14T14:00:00+00:00",
        "2026-09-14T13:00:00-04:00",
        "2026-09-14T20:00:00+00:00",
    ]


def test_a_quarantined_partition_is_refused_by_default_and_reads_under_the_opt_in(
    fixture_lake: FixtureLake,
):
    """#266 test 5. The same fail-closed guard every read shares."""
    quarantine = [{"partition": FULL_PARTITION, "verdict": "delayed_feed"}]
    root = _lake(fixture_lake, quarantine=quarantine)

    with pytest.raises(PartitionQuarantined):
        load_contract(CALL, FULL_DAY, lake_root=root)

    table = load_contract(CALL, FULL_DAY, lake_root=root, include_quarantined=True)
    assert table.num_rows == 3


def test_the_ticker_is_derived_from_the_occ_root_and_an_explicit_ticker_overrides_it(
    fixture_lake: FixtureLake,
):
    """#266 test 6. An index root can differ from the ticker the partition is keyed by.

    ``SPXW`` is a real index root. Its symbol here is filed in the ``SPY`` partition, the
    way a root the derivation gets wrong actually shows up: deriving looks in a partition
    that does not exist, and the override is what finds the data.
    """
    index_contract = "SPXW  260918C04500000"
    rows = [*FULL_ROWS, _data("2026-09-14T13:30:00+00:00", index_contract)]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(PartitionAbsent):
        load_contract(index_contract, FULL_DAY, lake_root=root)

    table = load_contract(index_contract, FULL_DAY, ticker="SPY", lake_root=root)
    assert table.num_rows == 1


def test_a_derived_ticker_that_names_no_partition_raises_naming_symbol_and_ticker(
    fixture_lake: FixtureLake,
):
    """#266 test 7. The derivation is a guess, and this is what it looks like failing."""
    root = _lake(fixture_lake)
    qqq_contract = "QQQ   260918C00500000"

    with pytest.raises(PartitionAbsent) as caught:
        load_contract(qqq_contract, FULL_DAY, lake_root=root)

    message = str(caught.value)
    assert qqq_contract in message
    assert "QQQ" in message


def test_two_contracts_from_one_partition_share_a_column_set_that_concat_tables_accepts(
    fixture_lake: FixtureLake,
):
    """#266 test 8. The overflow half of the predicate is what keeps this true.

    Only PUT's first cycle carries an overflow value here. A fetch of CALL's rows alone
    would then lack the promoted column PUT's read gains, and ``pa.concat_tables`` over
    the two would raise.
    """
    rows = [_with_extra(_data("2026-09-14T13:30:00+00:00", PUT), {"totalVolume": 42}), *FULL_ROWS]
    fingerprints = {
        surface: {name: kind for name, kind in columns.items() if name != "volume"}
        if surface == "chains"
        else dict(columns)
        for surface, columns in running_fingerprints().items()
    }
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=fingerprints)
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", SchemaVersionLedger([entry]).to_table())
    root = fixture_lake.build()

    call_table = load_contract(CALL, FULL_DAY, lake_root=root)
    put_table = load_contract(PUT, FULL_DAY, lake_root=root)

    assert "volume" in put_table.column_names
    assert call_table.column_names == put_table.column_names
    combined = pa.concat_tables([call_table, put_table])
    assert combined.num_rows == call_table.num_rows + put_table.num_rows


def test_a_null_row_kind_among_the_contracts_own_rows_refuses_the_read(
    fixture_lake: FixtureLake,
):
    """#266 test 9, first half. The check runs on the fetched rows, which include these."""
    rows = [*FULL_ROWS, {**_data("2026-09-14T15:00:00+00:00", CALL), "row_kind": None}]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(LoadError, match="row_kind"):
        load_contract(CALL, FULL_DAY, lake_root=root)


def test_a_null_row_kind_elsewhere_in_the_partition_does_not_refuse(fixture_lake: FixtureLake):
    """#266 test 9, second half. A row neither fetched nor answering the selection is unseen.

    The damaged row here belongs to PUT and carries no overflow value, so a read for
    CALL never fetches it at all: it matches neither the occ_symbol selection nor the
    overflow half of the predicate.
    """
    rows = [*FULL_ROWS, {**_data("2026-09-14T15:00:00+00:00", PUT), "row_kind": None}]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    table = load_contract(CALL, FULL_DAY, lake_root=root)

    assert table.num_rows == 3


def test_a_null_row_kind_on_another_contracts_overflow_row_still_refuses(
    fixture_lake: FixtureLake,
):
    """A row carrying overflow is read by every fetch, the same rule ``PartialRead`` follows.

    This PUT row carries no occ_symbol match for CALL, but its overflow value puts it in
    every fetch of this partition regardless of which contract was asked for, the same
    way an unrecorded schema version on an overflow-carrying row already refuses every
    read of the day rather than only the minute that row belongs to. A null row_kind on
    such a row is therefore among "the fetched rows" for a CALL read too, not only for a
    read of PUT's own minute.
    """
    damaged_row = _with_extra(_data("2026-09-14T15:00:00+00:00", PUT), {"totalVolume": 7})
    rows = [*FULL_ROWS, {**damaged_row, "row_kind": None}]
    fingerprints = {
        surface: {name: kind for name, kind in columns.items() if name != "volume"}
        if surface == "chains"
        else dict(columns)
        for surface, columns in running_fingerprints().items()
    }
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=fingerprints)
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", SchemaVersionLedger([entry]).to_table())
    root = fixture_lake.build()

    with pytest.raises(LoadError, match="row_kind"):
        load_contract(CALL, FULL_DAY, lake_root=root)


def test_a_snap_ts_that_cannot_be_read_as_an_instant_raises_rather_than_crashing(
    fixture_lake: FixtureLake,
):
    """Every row here already belongs to the answer, so an unreadable one is not excused.

    ``load_chain`` can set an unreadable ``snap_ts`` elsewhere in the session aside,
    because it sits beside the minute that actually answers the read. A contract's rows
    have no such minute to sit beside: each one is already part of the series, so this
    raises a named error instead of a bare ``TypeError`` out of comparing an unparsed
    value against a real instant.
    """
    rows = [*FULL_ROWS, _data("2026-09-14T11:00:00", CALL)]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(LoadError, match="snap_ts"):
        load_contract(CALL, FULL_DAY, lake_root=root)


# -- rows a resolution never had to consider ----------------------------------


def test_a_partition_that_carries_no_ledger_reads_partial(fixture_lake: FixtureLake):
    """An absent schema-version ledger is still the projection's condition, not this read's."""
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(FULL_ROWS))
    root = fixture_lake.build()

    with pytest.raises(PartialRead):
        load_contract(CALL, FULL_DAY, lake_root=root)


def test_a_row_carrying_no_schema_version_refuses_the_reads_that_include_it(
    fixture_lake: FixtureLake,
):
    """The projection's own row-scoped refusal applies to a contract read the same way."""
    rows = [
        {**_data("2026-09-14T13:30:00+00:00", CALL), "schema_version": None},
        _data("2026-09-14T18:31:00+00:00", CALL),
    ]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(ExtraProjectionError, match="no schema_version"):
        load_contract(CALL, FULL_DAY, lake_root=root)
