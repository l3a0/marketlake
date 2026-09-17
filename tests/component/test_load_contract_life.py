"""Threading one contract through the security master, against a fixture lake on disk.

``load_contract`` and ``load_contract_life`` share every guard the other doors have, and
``tests/component/test_load_chain.py`` and ``tests/component/test_load_contract.py`` already
exercise those against the chains surface. This file exercises what the threading adds: the
symbol resolved per session out of the master's OCC mappings, the ticker settled once out of
the thread's earliest symbol, the sessions a range steps over, and the four ways the master
itself can refuse or say nothing.

The numbered tests carry the numbering marketlake #135 asks for, so a mutation the issue names
points at the test the issue names.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake.loader import (
    ContractAbsent,
    ContractAmbiguous,
    PartitionAbsent,
    PartitionQuarantined,
    load_contract,
    load_contract_life,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.security_master import (
    ID_TYPE_OCC,
    KIND_EQUITY,
    KIND_OPTION,
    Mapping,
    MasterUnreadable,
    SecurityMaster,
    master_path,
)
from tests.support.lake import FixtureLake, sample_chains_table

# One contract, three spellings. ``OLD`` is what it listed under, ``ADJUSTED`` is what the
# first re-symboling gave it, and ``AGAIN`` the second. Their OCC roots are ``SPY``, ``SPY1``
# and ``SPY2``, and only the first names a partition directory, which is the whole reason the
# ticker comes from the earliest symbol rather than the day's.
OLD = "SPY   261218C00250000"
ADJUSTED = "SPY1  261218C00250000"
AGAIN = "SPY2  261218C00250000"

# An ordinary contract no re-symboling touched, which is every contract in the live lake.
PLAIN = "SPY   260918C00650000"

# The sessions. The boundary falls on ``SEALED[3]``, so two sessions sit under the old symbol
# and two under the adjusted one, and ``BEFORE`` precedes every mapping the master holds.
BEFORE = "2026-09-09"
SEALED = ["2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15"]
BOUNDARY = date(2026, 9, 15)
OPENED = date(2026, 9, 10)

CAPTURE_START = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def _data(day: str, minute: str, occ: str, *, version: int = 1) -> dict:
    """One chains data row: a vendor observation at ``minute`` ET for ``occ``."""
    return {
        "snap_ts": f"{day}T{minute}:00-04:00",
        "fetch_ts": f"{day}T{minute}:00.400-04:00",
        "vendor_quote_ts": f"{day}T{minute}:00.150-04:00",
        "ticker": "SPY",
        "occ_symbol": occ,
        "bid": 4.20,
        "ask": 4.25,
        "last": 4.22,
        "open_interest": 1234,
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": None,
        "session_phase": None,
        "schema_version": version,
        "extra": None,
    }


def _ledger_table() -> pa.Table:
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _master(*, remaps: int = 1, extra: list[Mapping] = ()) -> SecurityMaster:
    """A master holding SPY the equity and one option contract, remapped ``remaps`` times.

    ``remaps=0`` gives a master with no option instrument at all, which is the live lake's
    shape and the one an unmapped contract reads under.
    """
    master = SecurityMaster()
    equity = master.register(
        kind=KIND_EQUITY, capture_start=CAPTURE_START, valid_from=date(2026, 9, 8), ticker="SPY"
    )
    if remaps:
        option = master.register(
            kind=KIND_OPTION,
            capture_start=master.capture_start_of(equity),
            valid_from=OPENED,
            occ_symbol=OLD,
        )
        master.remap(option, ID_TYPE_OCC, ADJUSTED, effective=BOUNDARY)
        if remaps > 1:
            master.remap(option, ID_TYPE_OCC, AGAIN, effective=date(2026, 9, 16))
    for mapping in extra:
        master._mappings.append(mapping)  # noqa: SLF001
    return master


def _lake(
    fixture_lake: FixtureLake,
    *,
    master: SecurityMaster | None = None,
    quarantine: list[dict] | None = None,
    days: dict[str, list[dict]] | None = None,
) -> Path:
    """A lake whose four sealed sessions carry the contract under the symbol it then wore."""
    written = days if days is not None else _life_rows()
    for day, rows in written.items():
        fixture_lake.with_chains("SPY", day, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    if master is not None:
        fixture_lake.with_reference("security_master", master.to_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _life_rows() -> dict[str, list[dict]]:
    """The contract under ``OLD`` before the boundary and under ``ADJUSTED`` from it."""
    return {
        day: [
            _data(day, "10:31", OLD if date.fromisoformat(day) < BOUNDARY else ADJUSTED),
            _data(day, "10:32", PLAIN),
        ]
        for day in SEALED
    }


def _symbols(table: pa.Table) -> list[str]:
    return table.column("occ_symbol").to_pylist()


def _days(table: pa.Table) -> list[str]:
    return [text[:10] for text in table.column("snap_ts").to_pylist()]


# -- what the master says nothing about --------------------------------------


def test_1_a_contract_no_mapping_names_reads_the_same_with_a_master_and_without_one(
    fixture_lake: FixtureLake,
):
    """#135 test 1. Threading is inert on every contract a re-symboling has not touched."""
    root = _lake(fixture_lake, master=_master())

    one = load_contract(PLAIN, SEALED[0], lake_root=root)
    life = load_contract_life(PLAIN, lake_root=root)

    assert _symbols(one) == [PLAIN]
    assert _days(life) == SEALED
    assert set(_symbols(life)) == {PLAIN}


def test_2_an_absent_master_threads_nothing_and_raises_nothing(fixture_lake: FixtureLake):
    """#135 test 2. The rule ``load_bars`` gives an absent actions ledger, unchanged.

    Every other loader test file writes no master at all, so this is also what keeps the
    129 shipped ones reading exactly as they did.
    """
    root = _lake(fixture_lake)
    assert not master_path(root).exists()

    assert _symbols(load_contract(OLD, SEALED[0], lake_root=root)) == [OLD]
    assert _days(load_contract_life(PLAIN, lake_root=root)) == SEALED

    # The adjusted half is unreachable without a thread, which is the state before #376.
    with pytest.raises(ContractAbsent):
        load_contract(OLD, SEALED[3], lake_root=root, ticker="SPY")


# -- threading one session ----------------------------------------------------


def test_3_the_pre_adjustment_symbol_reads_a_session_after_the_boundary(
    fixture_lake: FixtureLake,
):
    """#135 test 3. The caller's spelling is the contract, not the session's spelling."""
    root = _lake(fixture_lake, master=_master())

    table = load_contract(OLD, SEALED[3], lake_root=root)

    assert _symbols(table) == [ADJUSTED]


def test_4_the_post_adjustment_symbol_reads_a_session_before_the_boundary(
    fixture_lake: FixtureLake,
):
    """#135 test 4. The thread runs both ways, which ``resolve`` alone cannot do."""
    root = _lake(fixture_lake, master=_master())

    table = load_contract(ADJUSTED, SEALED[0], lake_root=root)

    assert _symbols(table) == [OLD]


# -- the life -----------------------------------------------------------------


def test_5_a_two_symbol_life_comes_back_as_one_table_in_instant_order(
    fixture_lake: FixtureLake,
):
    """#135 test 5. Both halves in one read, and the rows say which half they came from."""
    root = _lake(fixture_lake, master=_master())

    table = load_contract_life(OLD, lake_root=root)

    assert _days(table) == SEALED
    assert _symbols(table) == [OLD, OLD, OLD, ADJUSTED]


def test_6_a_session_the_contract_is_absent_from_is_stepped_over_and_an_empty_range_raises(
    fixture_lake: FixtureLake,
):
    """#135 test 6. A contract lists, trades and expires, so gaps in a life are ordinary."""
    days = _life_rows()
    days[SEALED[1]] = [_data(SEALED[1], "10:32", PLAIN)]
    root = _lake(fixture_lake, master=_master(), days=days)

    table = load_contract_life(OLD, lake_root=root)
    assert _days(table) == [SEALED[0], SEALED[2], SEALED[3]]

    with pytest.raises(ContractAbsent) as absent:
        load_contract_life("SPY   261218P00999000", start=SEALED[0], end=SEALED[3], lake_root=root)
    assert f"{SEALED[0]}..{SEALED[3]}" in str(absent.value)

    with pytest.raises(ContractAbsent) as open_ended:
        load_contract_life("SPY   261218P00999000", lake_root=root)
    assert "open..open" in str(open_ended.value)

    with pytest.raises(PartitionAbsent):
        load_contract_life(OLD, start="2026-10-01", lake_root=root)


def test_7_a_quarantined_session_refuses_the_whole_range_and_the_opt_in_reads_it(
    fixture_lake: FixtureLake,
):
    """#135 test 7. ``load_bars``'s rule, so one verdict has one meaning on both doors."""
    entry = {"partition": f"chains/ticker=SPY/date={SEALED[2]}.parquet", "verdict": "delayed_feed"}
    root = _lake(fixture_lake, master=_master(), quarantine=[entry])

    with pytest.raises(PartitionQuarantined):
        load_contract_life(OLD, lake_root=root)

    table = load_contract_life(OLD, lake_root=root, include_quarantined=True)
    assert _days(table) == SEALED


# -- the ticker ---------------------------------------------------------------


def test_8_the_ticker_comes_from_the_earliest_symbol_and_an_explicit_one_overrides_it(
    fixture_lake: FixtureLake,
):
    """#135 test 8. An adjusted root is not a ticker, so the day's symbol cannot name one."""
    root = _lake(fixture_lake, master=_master())

    # Entering by the adjusted spelling still opens ``ticker=SPY``, never ``ticker=SPY1``.
    assert _days(load_contract_life(ADJUSTED, lake_root=root)) == SEALED

    # Without the thread the derivation is the day's own root, which names no directory.
    bare = _lake(FixtureLake(root.parent / "bare"))
    with pytest.raises(PartitionAbsent) as absent:
        load_contract(ADJUSTED, SEALED[3], lake_root=bare)
    assert "ticker=SPY1" in str(absent.value)

    # And an explicit ticker wins over both readings.
    assert load_contract(ADJUSTED, SEALED[3], lake_root=bare, ticker="SPY").num_rows == 1


# -- what the master refuses --------------------------------------------------


def test_9_a_symbol_naming_two_instruments_refuses_rather_than_picking_one(
    fixture_lake: FixtureLake,
):
    """#135 test 9. The state the master calls corrupt, which no writer here can produce."""
    stray = Mapping(
        instrument_id=99,
        id_type=ID_TYPE_OCC,
        id_value=OLD,
        valid_from=OPENED,
        valid_to=None,
        kind=KIND_OPTION,
        capture_start=CAPTURE_START,
    )
    root = _lake(fixture_lake, master=_master(extra=[stray]))

    with pytest.raises(ContractAmbiguous) as raised:
        load_contract(OLD, SEALED[0], lake_root=root)
    assert raised.value.instrument_ids == [2, 99]

    with pytest.raises(ContractAmbiguous):
        load_contract_life(OLD, lake_root=root)


def test_10_a_torn_master_refuses_every_call_at_these_doors_with_the_masters_own_error(
    fixture_lake: FixtureLake,
):
    """#135 test 10. A file contradicting its writer raises the error of the module owning it.

    The blast radius is asserted rather than left to be discovered. Nothing can ask whether the
    master holds a symbol without reading it, so a contract it never held and a call that named
    its own ticker refuse too. ``load_bars`` is not the precedent and does not behave this way:
    it reaches the actions ledger only for a view that needs one.
    """
    root = _lake(fixture_lake, master=_master())
    path = master_path(root)
    path.write_bytes(path.read_bytes()[:40])

    with pytest.raises(MasterUnreadable):
        load_contract(OLD, SEALED[0], lake_root=root)

    with pytest.raises(MasterUnreadable):
        load_contract_life(OLD, lake_root=root)

    # A contract with no mapping of any kind, and a caller who supplied the ticker, so the
    # master has nothing to contribute to the read. It refuses anyway.
    with pytest.raises(MasterUnreadable):
        load_contract(PLAIN, SEALED[0], lake_root=root, ticker="SPY")


# -- the edges of the thread --------------------------------------------------


def test_11_a_session_the_masters_ranges_do_not_cover_is_still_read(fixture_lake: FixtureLake):
    """#135 test 11. ``valid_from`` is the first session the walk read, not the lake's first.

    The walk skips a session for seven reasons, so the lake holds sealed sessions no mapping
    range covers. The selection is every spelling the contract has worn, so a range that places
    a session wrongly cannot take that session away.
    """
    days = _life_rows()
    days[BEFORE] = [_data(BEFORE, "10:31", OLD)]
    root = _lake(fixture_lake, master=_master(), days=days)

    life = load_contract_life(ADJUSTED, lake_root=root)
    assert _days(life) == [BEFORE, *SEALED]
    assert _symbols(life)[0] == OLD

    assert _symbols(load_contract(ADJUSTED, BEFORE, lake_root=root)) == [OLD]


def test_12_a_contract_remapped_twice_threads_through_all_three_symbols(
    fixture_lake: FixtureLake,
):
    """#135 test 12. Each remap extends the chain, and the earliest still names the ticker."""
    again_day = "2026-09-16"
    days = _life_rows()
    days[again_day] = [_data(again_day, "10:31", AGAIN)]
    root = _lake(fixture_lake, master=_master(remaps=2), days=days)

    table = load_contract_life(AGAIN, lake_root=root)

    assert _symbols(table) == [OLD, OLD, OLD, ADJUSTED, AGAIN]
    assert _days(table) == [*SEALED, again_day]


# -- where the master and the lake disagree -----------------------------------


def test_13_a_master_dating_the_boundary_late_still_returns_every_session(
    fixture_lake: FixtureLake,
):
    """#135 test 13. The ranges widen the selection; they never narrow it.

    ``lake.occ_mapping``'s own docstring records this state: a boundary dated two ways leaves
    ``resolve`` answering the old symbol "on a day the sealed chains already carried the new
    one". A read that substituted the master's answer per session would refuse a session whose
    rows are on disk, and the life read's step-over rule would then turn that refusal into a
    hole that reads as a session the contract did not trade in.
    """
    # The chains turned over on 09-14 and the master dates the boundary 09-15, one late.
    days = {
        SEALED[0]: [_data(SEALED[0], "10:31", OLD)],
        SEALED[1]: [_data(SEALED[1], "10:31", OLD)],
        SEALED[2]: [_data(SEALED[2], "10:31", ADJUSTED)],
        SEALED[3]: [_data(SEALED[3], "10:31", ADJUSTED)],
    }
    root = _lake(fixture_lake, master=_master(), days=days)

    assert _days(load_contract_life(OLD, lake_root=root)) == SEALED
    assert _symbols(load_contract(ADJUSTED, SEALED[2], lake_root=root)) == [ADJUSTED]


def test_14_a_boundary_session_carrying_both_spellings_returns_both(fixture_lake: FixtureLake):
    """#135 test 14. The master's ranges are date-grained and a partition holds a day of minutes.

    So the session a mapping closes on can carry the old spelling early and the new one late,
    and one symbol per session would drop half of it without saying so.
    """
    days = _life_rows()
    days[SEALED[3]] = [
        _data(SEALED[3], "09:31", OLD),
        _data(SEALED[3], "15:31", ADJUSTED),
    ]
    root = _lake(fixture_lake, master=_master(), days=days)

    boundary = load_contract(OLD, SEALED[3], lake_root=root)
    assert _symbols(boundary) == [OLD, ADJUSTED]

    life = load_contract_life(OLD, lake_root=root)
    assert _symbols(life) == [OLD, OLD, OLD, OLD, ADJUSTED]


def test_15_the_selection_keeps_every_row_the_door_returned_before_the_master_existed(
    fixture_lake: FixtureLake,
):
    """#135 test 15. Threading is additive, which is what makes it safe on a shipped door.

    Whatever spelling a caller names, a row carrying that spelling comes back. The master can
    only add spellings to look for, so no reading of it can take a row away.
    """
    days = _life_rows()
    root = _lake(fixture_lake, master=_master(), days=days)
    bare = _lake(FixtureLake(root.parent / "bare"), days=days)

    for symbol in (OLD, ADJUSTED, PLAIN):
        for day in SEALED:
            try:
                without = _symbols(load_contract(symbol, day, lake_root=bare, ticker="SPY"))
            except ContractAbsent:
                without = []
            try:
                with_master = _symbols(load_contract(symbol, day, lake_root=root, ticker="SPY"))
            except ContractAbsent:
                with_master = []
            assert set(without) <= set(with_master), (symbol, day, without, with_master)
