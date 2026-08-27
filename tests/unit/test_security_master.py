"""The security master, value-only logic.

These tests decide everything from values alone. So they are unit tests: id
assignment, as-of resolution at validity boundaries, rename and re-symbol threading,
and the capture_start clamping rule. The parquet round-trip on disk is a component
test.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from lake.security_master import (
    ID_TYPE_OCC,
    ID_TYPE_TICKER,
    AmbiguousSymbol,
    Mapping,
    SecurityMaster,
    UnknownInstrument,
    is_in_scope,
)

# A fixed capture epoch for equity registrations: 2019-01-02 09:30 ET in UTC.
EPOCH = datetime(2019, 1, 2, 14, 30, tzinfo=UTC)


def test_register_assigns_ids_starting_at_one_and_incrementing():
    master = SecurityMaster()
    first = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY"
    )
    second = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="QQQ"
    )
    assert first == 1
    assert second == 2


def test_register_opens_a_row_per_supplied_identifier():
    master = SecurityMaster()
    iid = master.register(
        kind="equity",
        capture_start=EPOCH,
        valid_from=date(2019, 1, 2),
        ticker="SPY",
        figi="BBG000BDTBL9",
    )
    rows = [m for m in master if m.instrument_id == iid]
    assert {m.id_type for m in rows} == {"ticker", "figi"}
    assert all(m.valid_to is None for m in rows)


def test_register_requires_at_least_one_identifier():
    master = SecurityMaster()
    with pytest.raises(ValueError):
        master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2))


def test_register_rejects_a_naive_capture_start():
    master = SecurityMaster()
    with pytest.raises(ValueError):
        master.register(
            kind="equity",
            capture_start=datetime(2019, 1, 2, 9, 30),
            valid_from=date(2019, 1, 2),
            ticker="SPY",
        )


def test_register_rejects_an_unknown_kind():
    master = SecurityMaster()
    with pytest.raises(ValueError):
        master.register(
            kind="future", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY"
        )


def test_register_rejects_an_id_already_in_use():
    master = SecurityMaster()
    master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY")
    with pytest.raises(ValueError):
        master.register(
            kind="equity",
            capture_start=EPOCH,
            valid_from=date(2019, 1, 2),
            ticker="QQQ",
            instrument_id=1,
        )


def test_rename_keeps_the_instrument_id_stable():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2012, 5, 18), ticker="FB"
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))

    # The same instrument resolves under both symbols, each as of its own era.
    assert master.resolve("FB", on=date(2020, 1, 2)) == iid
    assert master.resolve("META", on=date(2023, 1, 3)) == iid


def test_rename_threads_history_and_adds_a_row_not_a_new_instrument():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2012, 5, 18), ticker="FB"
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))

    assert master.instrument_ids() == {iid}
    ticker_rows = [m for m in master if m.id_type == "ticker"]
    assert len(ticker_rows) == 2


def test_as_of_resolution_is_exact_at_the_rename_boundary():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2012, 5, 18), ticker="FB"
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))

    # The range is half-open, so the boundary day belongs to the new symbol alone.
    assert master.resolve("FB", on=date(2022, 6, 8)) == iid
    assert master.resolve("FB", on=date(2022, 6, 9)) is None
    assert master.resolve("META", on=date(2022, 6, 9)) == iid
    assert master.resolve("META", on=date(2022, 6, 8)) is None


def test_resolve_before_valid_from_is_none():
    master = SecurityMaster()
    master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY")
    assert master.resolve("SPY", on=date(2018, 12, 31)) is None
    assert master.resolve("SPY", on=date(2019, 1, 2)) == 1


def test_symbol_at_returns_the_symbol_valid_on_the_date():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2012, 5, 18), ticker="FB"
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))

    assert master.symbol_at(iid, on=date(2020, 1, 2)) == "FB"
    assert master.symbol_at(iid, on=date(2022, 6, 9)) == "META"
    assert master.symbol_at(iid, on=date(2000, 1, 1)) is None


def test_occ_resymboling_threads_one_contract_under_one_id():
    master = SecurityMaster()
    old = "FB    220617C00200000"
    new = "META  220617C00200000"
    iid = master.register(
        kind="option",
        capture_start=EPOCH,
        valid_from=date(2021, 6, 18),
        occ_symbol=old,
    )
    master.remap(iid, ID_TYPE_OCC, new, effective=date(2022, 6, 9))

    assert master.resolve(old, on=date(2022, 1, 3), id_type=ID_TYPE_OCC) == iid
    assert master.resolve(new, on=date(2022, 6, 10), id_type=ID_TYPE_OCC) == iid
    assert master.symbol_at(iid, on=date(2022, 6, 10), id_type=ID_TYPE_OCC) == new


def test_remap_rejects_an_effective_date_at_or_before_valid_from():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY"
    )
    with pytest.raises(ValueError):
        master.remap(iid, ID_TYPE_TICKER, "SPYX", effective=date(2019, 1, 2))


def test_remap_unknown_instrument_raises():
    master = SecurityMaster()
    with pytest.raises(UnknownInstrument):
        master.remap(99, ID_TYPE_TICKER, "SPY", effective=date(2019, 1, 2))


def test_resolve_raises_on_an_ambiguous_symbol():
    # A hand-built corrupt master: one symbol maps to two instruments on the same day.
    master = SecurityMaster(
        [
            Mapping(1, "ticker", "DUP", date(2019, 1, 2), None, "equity", EPOCH),
            Mapping(2, "ticker", "DUP", date(2019, 1, 2), None, "equity", EPOCH),
        ]
    )
    with pytest.raises(AmbiguousSymbol):
        master.resolve("DUP", on=date(2020, 1, 2))


def test_capture_start_clamping_at_the_epoch_boundary():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY"
    )

    # The epoch instant itself is in scope. One minute before it is not.
    assert master.in_scope(iid, EPOCH) is True
    assert master.in_scope(iid, datetime(2019, 1, 2, 14, 31, tzinfo=UTC)) is True
    assert master.in_scope(iid, datetime(2019, 1, 2, 14, 29, tzinfo=UTC)) is False


def test_is_in_scope_is_a_pure_at_or_after_comparison():
    assert is_in_scope(EPOCH, EPOCH) is True
    assert is_in_scope(datetime(2019, 1, 3, 0, 0, tzinfo=UTC), EPOCH) is True
    assert is_in_scope(datetime(2019, 1, 1, 0, 0, tzinfo=UTC), EPOCH) is False


def test_capture_start_of_unknown_instrument_raises():
    master = SecurityMaster()
    with pytest.raises(UnknownInstrument):
        master.capture_start_of(1)


def test_next_instrument_id_on_an_empty_master_is_one():
    assert SecurityMaster().next_instrument_id() == 1
