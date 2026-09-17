"""``continuity_view`` against a fixture lake on disk.

Slice 4 fetches nothing, so a fixture lake is its whole test surface. Every test here crosses
files: sealed chains partitions, sealed daily bars partitions, the schema-version ledger, the
security master and the actions ledger. No vendor, no network, no wall clock, and the real lake
is never touched.

**The boundary is a replay and cannot be anything else.** Measured read-only on the live lake at
`21e9d21`, SPY carries the root `SPY` on every session holding data and QQQ carries `QQQ`, with
no root gained after each ticker's first. The lake holds no re-symboling, so the continuity this
view exists for has no live example. marketlake #279's own detector was exercised the same way,
and #368 shipped both adjusted bar views against an empty `bars/` for the same reason.

**This file carries its own chains schema**, as `tests/component/test_settlement_view.py` does
and for the reason that file gives: `FIXTURE_CHAINS_SCHEMA` carries neither `strike_price` nor
`mark`, and widening the shared one would leave `test_load_chain`'s promoted-column test green
while it proved nothing, since that test asserts a column is *absent* from
`sample_chains_table`.

The fabricated adjustment is a 3-for-2, which is the shape `lake.splits` can land: the
deliverable moves from 100 units to 150, the multiplier does not, there is one stock leg and no
cash, and the strike moves the other way, from 300 to 200. The underlying moves with it, 333 to
222.

The numbered tests carry the numbering marketlake #136 asks for, so a mutation the issue names
points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import actions, journal
from lake.continuity import (
    CONTINUITY_VIEW_SCHEMA,
    REASON_BOUNDARY_UNREADABLE,
    REASON_CLOSE_UNREADABLE,
    REASON_DELIVERABLE_NOT_SCALAR,
    REASON_NO_CLOSE_OF_RECORD,
    REASON_TERMS_UNREADABLE,
    VERDICT_ABSENT,
    VERDICT_INDETERMINATE,
    VERDICT_SETTLED,
    ContractDuplicated,
    ContractNeverObserved,
    ThreadAmbiguous,
    continuity_view,
)
from lake.loader import PartitionQuarantined
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.security_master import ID_TYPE_OCC, KIND_OPTION, SecurityMaster, master_path
from tests.support.lake import FixtureLake

TICKER = "SPY"
DAILY = "1d"

# The contract, before and after the adjustment. The two spellings differ in the root and in the
# strike, which is what an OCC re-symboling does to a symbol.
OLD = "SPY   261218C00300000"
NEW = "SPY1  261218C00200000"

# Three consecutive sessions. The boundary is the third: it is the first session the contract is
# observed under its adjusted spelling.
S1, S2, S3 = "2026-09-14", "2026-09-15", "2026-09-16"
BOUNDARY = date.fromisoformat(S3)

# The equity's instrument, which is the key the actions ledger is written under and the key a
# bars row carries. The contract's own instrument is minted by the master fixture below, and it
# is a different one: `occ_mapping` hangs an OCC mapping on the contract rather than on the
# underlying, since a mapping hung on the equity would tie a contract symbol to the underlying.
EQUITY = 1

# The deliverable on each side. 100 units before and 150 after is a 3-for-2, which is the shape
# `splits.require_scalar` accepts: one stock leg, the same underlying, no cash, and a multiplier
# that did not move.
UNITS_BEFORE = 100.0
UNITS_AFTER = 150.0
RATIO = UNITS_AFTER / UNITS_BEFORE
MULTIPLIER = 100.0

STRIKE_BEFORE = 300.0
STRIKE_AFTER = 200.0

# The underlying's daily closes. The third is the first session at the adjusted price.
CLOSES = {S1: 330.0, S2: 333.0, S3: 222.0}

# When the schema-version ledger recorded the running version, and when the split entry was
# learned. Any instant does for the first, since the view reads the recorded shape and never the
# time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# 02:00 UTC is when the nightly detector would run, and it is 22:00 Eastern on the *previous*
# day. `actions.as_of` counts an entry by its `recorded_at` read in market time, so this entry's
# market date is 2026-09-16 rather than 09-17, which is what the point-in-time test turns on.
SPLIT_LEARNED_AT = datetime(2026, 9, 17, 2, 0, tzinfo=UTC)

# The chains columns this view reads, plus the provenance the loader needs.
CONTINUITY_CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("occ_symbol", pa.string()),
        ("strike_price", pa.float64()),
        ("mark", pa.float64()),
        ("multiplier", pa.float64()),
        ("non_standard", pa.bool_()),
        ("option_deliverables_list", pa.string()),
        ("deliverable_note", pa.string()),
        # The chain's own underlying snapshot. A view reading the close off this rather than off
        # `bars/` gets a number that disagrees with the right answer, so taking it is visible.
        ("underlying_price", pa.float64()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)

CHAIN_UNDERLYING = 999.99


def _deliverable(units: float | None) -> str | None:
    """The vendor's typed deliverables list for one stock leg, or ``None`` for a row with none."""
    if units is None:
        return None
    return json.dumps(
        [
            {
                "assetType": "STOCK",
                "currencyType": None,
                "deliverableUnits": units,
                "symbol": TICKER,
            }
        ]
    )


def _note(units: float | None) -> str | None:
    """The vendor's free-text spelling of the same deliverable, the gate's second number."""
    return None if units is None else f"{units:g} {TICKER}"


def _contract(
    day: str,
    symbol: str,
    *,
    strike: float,
    units: float | None = UNITS_BEFORE,
    mark: float = 12.0,
    multiplier: float | None = MULTIPLIER,
    note: str | None = None,
    tag: str = "option_close",
) -> dict:
    """One close-of-record chain row for the contract."""
    return {
        "snap_ts": f"{day}T16:15:00-04:00",
        "fetch_ts": f"{day}T16:15:00.400-04:00",
        "vendor_quote_ts": f"{day}T16:15:00-04:00",
        "ticker": TICKER,
        "occ_symbol": symbol,
        "strike_price": strike,
        "mark": mark,
        "multiplier": multiplier,
        "non_standard": units != UNITS_BEFORE,
        "option_deliverables_list": _deliverable(units),
        "deliverable_note": note if note is not None else _note(units),
        "underlying_price": CHAIN_UNDERLYING,
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": tag,
        "session_phase": None,
        "schema_version": journal.SCHEMA_VERSION,
        "extra": None,
    }


def _bar(day: str, close: float, *, stamp: str | None = None, instrument: int | None = EQUITY):
    """One sealed daily candle, in the shape `lake.bars` writes."""
    row = dict.fromkeys(journal.BARS_SCHEMA.names)
    row.update(
        bar_ts=stamp if stamp is not None else f"{day}T20:00:00.000+00:00",
        instrument_id=instrument,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1_000_000,
        schema_version=journal.SCHEMA_VERSION,
    )
    return row


def _table(schema: pa.Schema, rows: list[dict]) -> pa.Table:
    return pa.table({name: [row.get(name) for row in rows] for name in schema.names}, schema=schema)


def _ledger_table() -> pa.Table:
    """The schema-version ledger recording the running version at the running shape."""
    entry = RecordedVersion(
        version=journal.SCHEMA_VERSION,
        recorded_at=RECORDED_AT,
        fingerprints=running_fingerprints(),
    )
    return SchemaVersionLedger([entry]).to_table()


def _master(
    *,
    remapped: bool = True,
    opens_on: str = S1,
    extra_holder: bool = False,
) -> SecurityMaster:
    """A master built the way `occ_mapping.write_mappings` builds one.

    It registers the equity, then registers the contract under its *old* symbol and remaps it
    onto the new one. That order is `occ_mapping`'s own, because `remap` closes an open mapping
    and nothing else registers option instruments.
    """
    master = SecurityMaster()
    master.register(
        kind="equity",
        capture_start=datetime(2026, 9, 8, tzinfo=UTC),
        valid_from=date(2026, 9, 8),
        ticker=TICKER,
        instrument_id=EQUITY,
    )
    if not remapped:
        return master
    contract = master.register(
        kind=KIND_OPTION,
        capture_start=datetime(2026, 9, 8, tzinfo=UTC),
        valid_from=date.fromisoformat(opens_on),
        occ_symbol=OLD,
    )
    master.remap(contract, ID_TYPE_OCC, NEW, effective=BOUNDARY)
    if extra_holder:
        master.register(
            kind=KIND_OPTION,
            capture_start=datetime(2026, 9, 8, tzinfo=UTC),
            valid_from=date.fromisoformat(S1),
            occ_symbol=OLD,
        )
    return master


def _lake(
    fixture_lake: FixtureLake,
    chains: dict[str, list[dict]],
    *,
    bars: dict[str, list[dict]] | None = None,
    master: SecurityMaster | None = None,
    split: bool = False,
    quarantine: list[dict] | None = None,
) -> Path:
    """A lake holding the given chains and bars partitions, the version ledger and a master."""
    for day, rows in chains.items():
        fixture_lake.with_chains(TICKER, day, _table(CONTINUITY_CHAINS_SCHEMA, rows))
    for day, rows in (
        bars if bars is not None else {d: [_bar(d, c)] for d, c in CLOSES.items()}
    ).items():
        fixture_lake.with_bars(TICKER, DAILY, day, _table(journal.BARS_SCHEMA, rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    root = fixture_lake.build()
    if master is not None:
        master.write(master_path(root))
    if split:
        _append_split(root)
    return root


def _append_split(root: Path, *, recorded_at: datetime = SPLIT_LEARNED_AT) -> None:
    """The ledger entry the split detector would land for this boundary.

    Appended after the lake is built, because `FixtureLake.build` rewrites the manifest whole
    from its own list and an append that ran first would leave the ledger on disk unrecorded.
    """
    actions.append(
        root,
        instrument_id=EQUITY,
        observed_on=BOUNDARY,
        ex_date=BOUNDARY,
        recorded_at=recorded_at,
        type=actions.TYPE_SPLIT,
        pay_date=None,
        declared_date=None,
        split_ratio=RATIO,
        provenance=actions.PROVENANCE_OBSERVED,
    )


def _unadjusted_chains() -> dict[str, list[dict]]:
    """Three sessions of one contract with no boundary in them."""
    return {day: [_contract(day, OLD, strike=STRIKE_BEFORE)] for day in (S1, S2, S3)}


def _adjusted_chains(*, units_after: float | None = UNITS_AFTER, note: str | None = None):
    """Three sessions where the third carries the contract under its adjusted spelling."""
    return {
        S1: [_contract(S1, OLD, strike=STRIKE_BEFORE)],
        S2: [_contract(S2, OLD, strike=STRIKE_BEFORE, mark=13.0)],
        S3: [
            _contract(
                S3,
                NEW,
                strike=STRIKE_AFTER,
                units=units_after,
                mark=13.5,
                note=note,
            )
        ],
    }


def _by_session(table: pa.Table) -> dict[str, dict]:
    return {row["session"]: row for row in table.to_pylist()}


# 1 ---------------------------------------------------------------------------------------


def test_a_contract_with_no_boundary_reads_every_session_at_one_scale(fixture_lake):
    """Every observed session, in session order, unscaled and settled."""
    root = _lake(fixture_lake, _unadjusted_chains(), master=_master(remapped=False))

    table = continuity_view(TICKER, OLD, lake_root=root)

    assert [row["session"] for row in table.to_pylist()] == [S1, S2, S3]
    for row in table.to_pylist():
        assert row["verdict"] == VERDICT_SETTLED
        assert row["reason"] is None
        assert row["split_ratio"] == 1.0
        assert row["adjusted_strike"] == row["strike_price"] == STRIKE_BEFORE
        assert row["instrument_id"] is None


# 2 ---------------------------------------------------------------------------------------


def test_a_landed_ratio_puts_both_halves_of_the_life_at_one_strike(fixture_lake):
    """The pre-boundary sessions under the old spelling, the post under the new, one strike."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=True)

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert [rows[day]["occ_symbol"] for day in (S1, S2, S3)] == [OLD, OLD, NEW]
    assert [rows[day]["split_ratio"] for day in (S1, S2, S3)] == [RATIO, RATIO, 1.0]
    assert {rows[day]["adjusted_strike"] for day in (S1, S2, S3)} == {STRIKE_AFTER}
    assert {rows[day]["verdict"] for day in (S1, S2, S3)} == {VERDICT_SETTLED}
    assert {rows[day]["instrument_id"] for day in (S1, S2, S3)} == {2}


def test_either_spelling_answers_the_same_series(fixture_lake):
    """The adjusted symbol and the pre-adjustment one name one contract, so they answer alike."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=True)

    assert continuity_view(TICKER, NEW, lake_root=root).equals(
        continuity_view(TICKER, OLD, lake_root=root)
    )


# 3 ---------------------------------------------------------------------------------------


def test_the_adjusted_underlying_is_continuous_where_the_as_traded_one_is_not(fixture_lake):
    """The join's two sides land in one scale, which is what makes moneyness comparable."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=True)

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert [rows[day]["underlying_close"] for day in (S1, S2, S3)] == [330.0, 333.0, 222.0]
    assert [rows[day]["adjusted_underlying_close"] for day in (S1, S2, S3)] == [220.0, 222.0, 222.0]
    moneyness = [
        rows[day]["adjusted_underlying_close"] / rows[day]["adjusted_strike"] for day in (S2, S3)
    ]
    assert moneyness[0] == pytest.approx(moneyness[1])


# 4 ---------------------------------------------------------------------------------------


def test_a_moved_deliverable_with_no_ledger_entry_marks_every_session_before_it(fixture_lake):
    """The boundary the caveat exists for: surfaced, never crossed with a factor of one."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=False)

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    for day in (S1, S2):
        assert rows[day]["verdict"] == VERDICT_ABSENT
        assert rows[day]["reason"] == REASON_DELIVERABLE_NOT_SCALAR
        assert rows[day]["adjusted_strike"] is None
        assert rows[day]["split_ratio"] is None
        assert rows[day]["strike_price"] == STRIKE_BEFORE
        assert rows[day]["underlying_close"] == CLOSES[day]
    assert rows[S3]["verdict"] == VERDICT_SETTLED


# 4a --------------------------------------------------------------------------------------


def test_an_unreadable_deliverable_at_a_boundary_is_indeterminate_not_absent(fixture_lake):
    """The view could not decide, which is a different statement from deciding there is none."""
    root = _lake(fixture_lake, _adjusted_chains(units_after=None), master=_master(), split=False)

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    for day in (S1, S2):
        assert rows[day]["verdict"] == VERDICT_INDETERMINATE
        assert rows[day]["reason"] == REASON_BOUNDARY_UNREADABLE
        assert rows[day]["adjusted_strike"] is None


# 5 ---------------------------------------------------------------------------------------


def test_a_rename_is_continuous_and_carries_no_mark(fixture_lake):
    """A spelling that moved while the deliverable did not adjusts nothing and marks nothing."""
    chains = _adjusted_chains(units_after=UNITS_BEFORE)
    # The renamed session keeps the pre-boundary strike too, because a rename moves no terms.
    chains[S3] = [_contract(S3, NEW, strike=STRIKE_BEFORE, mark=13.5)]
    root = _lake(
        fixture_lake,
        chains,
        bars={day: [_bar(day, 333.0)] for day in (S1, S2, S3)},
        master=_master(),
        split=False,
    )

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    for day in (S1, S2, S3):
        assert rows[day]["verdict"] == VERDICT_SETTLED
        assert rows[day]["reason"] is None
        assert rows[day]["split_ratio"] == 1.0
        assert rows[day]["adjusted_strike"] == STRIKE_BEFORE


# 6 ---------------------------------------------------------------------------------------


def test_a_session_with_no_close_of_record_inside_the_span_takes_a_row(fixture_lake):
    """The view could not look, and the caller cannot tell that from a session it did not trade."""
    chains = _unadjusted_chains()
    chains[S2] = [_contract(S2, OLD, strike=STRIKE_BEFORE, tag=None)]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert sorted(rows) == [S1, S2, S3]
    assert rows[S2]["verdict"] == VERDICT_ABSENT
    assert rows[S2]["reason"] == REASON_NO_CLOSE_OF_RECORD
    assert rows[S2]["strike_price"] is None
    assert rows[S2]["occ_symbol"] == OLD


# 7 ---------------------------------------------------------------------------------------


def test_a_date_with_no_chains_partition_is_not_a_session_this_view_knows(fixture_lake):
    """The row set is a property of the lake rather than of a calendar, as `load_bars` has it."""
    chains = _unadjusted_chains()
    del chains[S2]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    assert [row["session"] for row in continuity_view(TICKER, OLD, lake_root=root).to_pylist()] == [
        S1,
        S3,
    ]


# 8 ---------------------------------------------------------------------------------------


def test_an_unreadable_session_outside_the_span_takes_no_row(fixture_lake):
    """A session before the contract listed is not that contract's hole."""
    chains = _unadjusted_chains()
    del chains[S1]
    chains["2026-09-11"] = [_contract("2026-09-11", OLD, strike=STRIKE_BEFORE, tag=None)]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    assert [row["session"] for row in continuity_view(TICKER, OLD, lake_root=root).to_pylist()] == [
        S2,
        S3,
    ]


# 9 ---------------------------------------------------------------------------------------


def test_a_readable_session_the_contract_is_not_listed_in_is_stepped_over(fixture_lake):
    """It had not listed or it had expired, and that answer is known rather than missing."""
    chains = _unadjusted_chains()
    chains[S2] = [_contract(S2, "SPY   261218C00310000", strike=310.0)]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    assert [row["session"] for row in continuity_view(TICKER, OLD, lake_root=root).to_pylist()] == [
        S1,
        S3,
    ]


# 10 --------------------------------------------------------------------------------------


def test_a_session_with_no_daily_bar_marks_that_row_and_keeps_the_as_traded_strike(fixture_lake):
    """No close to pair the strike with and no scale to express it in, which is one condition."""
    root = _lake(
        fixture_lake,
        _unadjusted_chains(),
        bars={day: [_bar(day, CLOSES[day])] for day in (S1, S3)},
        master=_master(remapped=False),
    )

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert rows[S2]["verdict"] == VERDICT_ABSENT
    assert rows[S2]["reason"] == REASON_CLOSE_UNREADABLE
    assert rows[S2]["strike_price"] == STRIKE_BEFORE
    assert rows[S2]["adjusted_strike"] is None
    assert rows[S2]["underlying_close"] is None
    assert rows[S1]["verdict"] == VERDICT_SETTLED


def test_a_bar_whose_close_is_null_reads_as_the_same_condition(fixture_lake):
    """The schema permits what the evening sweep's gate refuses, and a caller can point anywhere."""
    bars = {day: [_bar(day, CLOSES[day])] for day in CLOSES}
    bars[S2] = [_bar(S2, 0.0)]
    bars[S2][0]["close"] = None
    root = _lake(fixture_lake, _unadjusted_chains(), bars=bars, master=_master(remapped=False))

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert rows[S2]["reason"] == REASON_CLOSE_UNREADABLE


# 11 --------------------------------------------------------------------------------------


def test_a_quarantined_session_refuses_the_whole_read(fixture_lake):
    """A hole in a re-fetchable surface is ordinary and a verdict is not."""
    root = _lake(
        fixture_lake,
        _unadjusted_chains(),
        master=_master(remapped=False),
        quarantine=[
            {
                "partition": f"chains/ticker={TICKER}/date={S2}.parquet",
                "quarantined": True,
                "reason": "battery",
            }
        ],
    )

    with pytest.raises(PartitionQuarantined):
        continuity_view(TICKER, OLD, lake_root=root)

    table = continuity_view(TICKER, OLD, lake_root=root, include_quarantined=True)
    assert [row["session"] for row in table.to_pylist()] == [S1, S2, S3]


# 12 --------------------------------------------------------------------------------------


def test_a_range_the_contract_is_never_observed_in_raises(fixture_lake):
    """An empty answer and an absent one read the same and mean opposite things."""
    root = _lake(fixture_lake, _unadjusted_chains(), master=_master(remapped=False))

    with pytest.raises(ContractNeverObserved) as caught:
        continuity_view(TICKER, "SPY   261218P00100000", lake_root=root)

    assert "SPY   261218P00100000" in str(caught.value)


# 13 --------------------------------------------------------------------------------------


def test_as_of_reaches_both_reads(fixture_lake):
    """A join whose halves sat at two points in time produces different numbers, not an error.

    The entry's market date is 2026-09-16, per the constant above, so 09-15 is the reading that
    has not learned it yet.
    """
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=True)

    before = _by_session(continuity_view(TICKER, OLD, as_of="2026-09-15", lake_root=root))
    after = _by_session(continuity_view(TICKER, OLD, as_of="2026-09-16", lake_root=root))

    # With no factor visible, the boundary has a moved deliverable and nothing describing it, so
    # both halves of the join say so together rather than one of them scaling alone.
    assert before[S1]["adjusted_underlying_close"] is None
    assert before[S1]["reason"] == REASON_DELIVERABLE_NOT_SCALAR
    assert after[S1]["adjusted_strike"] == STRIKE_AFTER
    assert after[S1]["adjusted_underlying_close"] == 220.0


# 14 --------------------------------------------------------------------------------------


def test_an_absent_master_threads_nothing_and_raises_nothing(fixture_lake):
    """The rule `load_bars` gives an absent actions ledger.

    It adjusts nothing and refuses nothing, which is what keeps this view inert rather than
    broken on a lake whose producers have not run against it.
    """
    root = _lake(fixture_lake, _unadjusted_chains(), master=None)

    table = continuity_view(TICKER, OLD, lake_root=root)

    assert [row["session"] for row in table.to_pylist()] == [S1, S2, S3]
    assert {row["instrument_id"] for row in table.to_pylist()} == {None}


# 15 --------------------------------------------------------------------------------------


def test_a_symbol_naming_two_instruments_refuses(fixture_lake):
    """The state the master calls corrupt and `occ_mapping` refuses to write."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(extra_holder=True), split=True)

    with pytest.raises(ThreadAmbiguous):
        continuity_view(TICKER, OLD, lake_root=root)


# 16 --------------------------------------------------------------------------------------


def test_a_session_before_the_thread_opens_takes_the_earliest_spelling(fixture_lake):
    """`symbol_at` answers None there, and the caller's own spelling is the wrong fallback."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(opens_on=S2), split=True)

    rows = _by_session(continuity_view(TICKER, NEW, lake_root=root))

    assert rows[S1]["occ_symbol"] == OLD
    assert rows[S1]["adjusted_strike"] == STRIKE_AFTER


# 17 --------------------------------------------------------------------------------------


def test_two_rows_for_one_contract_in_one_cycle_refuses(fixture_lake):
    """One cycle is one observation per contract, so two leave the session's terms undecided."""
    chains = _unadjusted_chains()
    chains[S2] = [
        _contract(S2, OLD, strike=STRIKE_BEFORE),
        _contract(S2, OLD, strike=STRIKE_BEFORE + 1.0),
    ]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    with pytest.raises(ContractDuplicated):
        continuity_view(TICKER, OLD, lake_root=root)


# 18 --------------------------------------------------------------------------------------


def test_the_answer_carries_the_view_schema_exactly(fixture_lake):
    """Including on a one-row answer, where a built-from-rows table can drift into another type."""
    chains = {S1: [_contract(S1, OLD, strike=STRIKE_BEFORE)]}
    root = _lake(
        fixture_lake, chains, bars={S1: [_bar(S1, CLOSES[S1])]}, master=_master(remapped=False)
    )

    table = continuity_view(TICKER, OLD, lake_root=root)

    assert table.num_rows == 1
    assert table.schema == CONTINUITY_VIEW_SCHEMA


# 19 --------------------------------------------------------------------------------------


def test_the_mark_is_never_rescaled(fixture_lake):
    """This repo writes down no rule for carrying an option's own price across an adjustment."""
    root = _lake(fixture_lake, _adjusted_chains(), master=_master(), split=True)

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert [rows[day]["mark"] for day in (S1, S2, S3)] == [12.0, 13.0, 13.5]


# 20 --------------------------------------------------------------------------------------


def test_the_answer_is_ordered_by_session_whatever_the_listing_gives(fixture_lake):
    """A series is the one read whose order a caller will assume."""
    chains = _unadjusted_chains()
    root = _lake(
        fixture_lake,
        {day: chains[day] for day in (S3, S1, S2)},
        master=_master(remapped=False),
    )

    assert [row["session"] for row in continuity_view(TICKER, OLD, lake_root=root).to_pylist()] == [
        S1,
        S2,
        S3,
    ]


# 21 --------------------------------------------------------------------------------------


def test_a_session_holding_two_candles_takes_the_later_instant(fixture_lake):
    """The gate checks that the session came back, not that exactly one candle did."""
    bars = {day: [_bar(day, CLOSES[day])] for day in CLOSES}
    bars[S1] = [
        _bar(S1, 700.0, stamp=f"{S1}T18:00:00.000+00:00"),
        _bar(S1, CLOSES[S1], stamp=f"{S1}T16:00:00.000-04:00"),
    ]
    root = _lake(fixture_lake, _unadjusted_chains(), bars=bars, master=_master(remapped=False))

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert rows[S1]["underlying_close"] == CLOSES[S1]


# 22 --------------------------------------------------------------------------------------


def test_a_row_whose_own_terms_cannot_be_read_marks_that_row(fixture_lake):
    """One contract's problem, so refusing would take the rest of its life away."""
    chains = _unadjusted_chains()
    chains[S2] = [_contract(S2, OLD, strike=STRIKE_BEFORE, multiplier=None)]
    root = _lake(fixture_lake, chains, master=_master(remapped=False))

    rows = _by_session(continuity_view(TICKER, OLD, lake_root=root))

    assert rows[S2]["verdict"] == VERDICT_ABSENT
    assert rows[S2]["reason"] == REASON_TERMS_UNREADABLE
    assert rows[S2]["underlying_close"] == CLOSES[S2]
    assert rows[S1]["verdict"] == VERDICT_SETTLED
