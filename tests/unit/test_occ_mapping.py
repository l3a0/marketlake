"""The OCC mapping write: pairing a re-symboled contract and threading it through the master.

Every test here drives ``lake.occ_mapping`` directly. Nothing fetches and nothing reaches the
configured lake: the writes go to a ``tmp_path`` lake whose master is built in the test.

The lake holds no adjusted contract, so every transition below is a fixture. What the live
lake supplied is the shape ``ssid`` has. Across four consecutive session pairs, SPY and QQQ
over 2026-09-14, 09-15 and 09-16, every session's values are distinct and of the 47,368
contracts carried from one session into the next not one changed its ``occ_symbol``. So the
pairing key names the contract and the symbol names the spelling.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake.manifest import record_partition, scrub
from lake.occ_mapping import (
    MappingRefused,
    Pair,
    SymbolHistory,
    instruments_holding,
    open_symbol,
    write_mappings,
)
from lake.security_master import (
    ID_TYPE_OCC,
    KIND_EQUITY,
    KIND_OPTION,
    AmbiguousSymbol,
    SecurityMaster,
    master_path,
)

EPOCH = datetime(2026, 9, 8, 17, 7, tzinfo=UTC)
NIGHT = datetime(2026, 9, 15, 23, 0, tzinfo=UTC)
DAY_ONE = date(2026, 9, 10)
DAY_TWO = date(2026, 9, 11)
DAY_THREE = date(2026, 9, 14)
BOUNDARY = date(2026, 9, 15)

OLD = "SPY   261218C00500000"
NEW = "SPY1  261218C00250000"
NEWER = "SPY2  261218C00125000"
OTHER = "SPY   261218C00600000"

MASTER_PARTITION = "reference/security_master.parquet"


def _row(ssid: int | None, occ_symbol: str | None) -> dict[str, object]:
    """One session row, reduced to the two columns this module reads."""
    return {"ssid": ssid, "occ_symbol": occ_symbol}


def _lake(tmp_path: Path, *, tickers: tuple[str, ...] = ("SPY",)) -> tuple[Path, int]:
    """A lake holding a master with one equity per ticker, manifested the way onboarding does."""
    root = tmp_path / "lake"
    root.mkdir()
    master = SecurityMaster()
    first = 0
    for ticker in tickers:
        instrument = master.register(
            kind=KIND_EQUITY, capture_start=EPOCH, valid_from=DAY_ONE, ticker=ticker
        )
        first = first or instrument
    master.write(master_path(root))
    record_partition(
        root,
        MASTER_PARTITION,
        source="reference",
        rows=len(master),
        fetched_at=NIGHT.isoformat(),
    )
    return root, first


def _write(root: Path, instrument_id: int, history: SymbolHistory, rows, *, day=BOUNDARY):
    return write_mappings(
        root,
        ticker="SPY",
        instrument_id=instrument_id,
        effective=day,
        pairing=history.inspect(rows),
        recorded_at=NIGHT,
    )


def _occ_rows(root: Path) -> list[tuple[int, str, date, date | None]]:
    master = SecurityMaster.read(master_path(root))
    return [
        (m.instrument_id, m.id_value, m.valid_from, m.valid_to)
        for m in master.mappings
        if m.id_type == ID_TYPE_OCC
    ]


# -- the history and what it pairs -------------------------------------------------------


def test_a_contract_whose_symbol_moved_is_a_pair_dated_from_when_it_was_first_read():
    """``valid_from`` is the first session the walk read the contract under the old symbol.

    The previous session's date would break the thread for every session before it, which is
    the whole point of threading a contract through a symbol change.
    """
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    history.observe(DAY_TWO, [_row(1, OLD)])
    history.observe(DAY_THREE, [_row(1, OLD)])

    pairing = history.inspect([_row(1, NEW)])

    assert pairing.recognised == 1
    assert pairing.pairs == (Pair(ssid=1, old_symbol=OLD, valid_from=DAY_ONE, new_symbol=NEW),)


def test_a_contract_absent_from_the_session_before_the_boundary_still_pairs():
    """Reading only the previous session would lose it, which is why the history is running.

    A contract can fall out of one snapshot and be back in the next without anything having
    happened to it, and the live lake shows that every day: of SPY's 12,956 contracts on
    2026-09-14, 12,646 carried into 2026-09-15 and the rest did not.
    """
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD), _row(2, OTHER)])
    history.observe(DAY_TWO, [_row(2, OTHER)])

    pairing = history.inspect([_row(1, NEW)])

    assert pairing.pairs == (Pair(ssid=1, old_symbol=OLD, valid_from=DAY_ONE, new_symbol=NEW),)


def test_a_contract_the_history_has_never_seen_is_a_new_listing_and_not_a_pair():
    """New contracts list under an adjusted root as readily as under a standard one."""
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    pairing = history.inspect([_row(9, "SPY1  261218C00300000")])

    assert pairing.recognised == 0 and pairing.pairs == ()


def test_a_row_carrying_no_ssid_is_evidence_of_nothing():
    """``ssid`` is null on both rows of the lake's 2026-09-02 partition, like ``option_root``."""
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(None, OLD)])

    assert len(history) == 0
    assert history.inspect([_row(None, NEW)]).recognised == 0


def test_the_history_resets_with_the_instrument():
    """A different security's contracts are a different history."""
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    history.reset()

    assert history.inspect([_row(1, NEW)]).pairs == ()


def test_a_symbol_that_moved_twice_dates_from_the_session_it_last_moved_in():
    """The second adjustment's old mapping opens where the first one's closed, not earlier."""
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    history.observe(DAY_TWO, [_row(1, NEW)])
    history.observe(DAY_THREE, [_row(1, NEW)])

    (pair,) = history.inspect([_row(1, NEWER)]).pairs

    assert pair.old_symbol == NEW and pair.valid_from == DAY_TWO


# -- the write ---------------------------------------------------------------------------


def test_the_write_registers_the_contract_and_then_remaps_it(tmp_path: Path):
    """``remap`` closes an open mapping, so nothing to remap means registering first.

    Against a master with no option instrument, a bare ``remap`` over ``ID_TYPE_OCC`` raises
    ``no open occ_symbol mapping to remap``. The two rows the write leaves share the boundary
    date, because the validity range is half-open.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    (written,) = _write(root, equity, history, [_row(1, NEW)])

    assert (written.old_symbol, written.new_symbol) == (OLD, NEW)
    assert written.instrument_id != equity
    assert _occ_rows(root) == [
        (written.instrument_id, OLD, DAY_ONE, BOUNDARY),
        (written.instrument_id, NEW, BOUNDARY, None),
    ]


def test_the_contract_threads_under_one_id_across_the_boundary(tmp_path: Path):
    """The whole point: one instrument answers on both sides of the symbol change."""
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    (written,) = _write(root, equity, history, [_row(1, NEW)])

    master = SecurityMaster.read(master_path(root))
    assert master.resolve(OLD, DAY_TWO, id_type=ID_TYPE_OCC) == written.instrument_id
    assert master.resolve(NEW, date(2026, 9, 16), id_type=ID_TYPE_OCC) == written.instrument_id
    assert master.resolve(NEW, DAY_TWO, id_type=ID_TYPE_OCC) is None


def test_the_option_instrument_carries_the_equity_epoch_and_the_option_kind(tmp_path: Path):
    """The master stores both on every row so the file stays self-describing.

    A contract was first recorded as part of its underlying's capture, so the epoch is the
    equity's rather than a second one invented here.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    (written,) = _write(root, equity, history, [_row(1, NEW)])

    master = SecurityMaster.read(master_path(root))
    rows = [m for m in master.mappings if m.instrument_id == written.instrument_id]
    assert {m.kind for m in rows} == {KIND_OPTION}
    assert {m.capture_start for m in rows} == {EPOCH}


def test_a_contract_adjusted_twice_remaps_the_instrument_it_already_has(tmp_path: Path):
    """Registering again would split one contract's life across two ids.

    ``resolve`` would then raise ``AmbiguousSymbol`` on the middle symbol, which is precisely
    the orphaning the master exists to prevent.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    (first,) = _write(root, equity, history, [_row(1, NEW)])
    history.observe(BOUNDARY, [_row(1, NEW)])

    (second,) = _write(root, equity, history, [_row(1, NEWER)], day=date(2026, 9, 16))

    assert second.instrument_id == first.instrument_id
    master = SecurityMaster.read(master_path(root))
    assert master.resolve(NEW, BOUNDARY, id_type=ID_TYPE_OCC) == first.instrument_id
    assert master.resolve(NEWER, date(2026, 9, 17), id_type=ID_TYPE_OCC) == first.instrument_id


def test_a_second_run_writes_nothing_and_leaves_the_file_alone(tmp_path: Path):
    """Without its own idempotence check a second night raises.

    ``remap`` requires ``effective`` to fall strictly after the open mapping's ``valid_from``,
    and the row the first night opened begins on that very date.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    _write(root, equity, history, [_row(1, NEW)])
    before = master_path(root).read_bytes()
    entries_before = (root / "manifest.jsonl").read_text()

    assert _write(root, equity, history, [_row(1, NEW)]) == ()
    assert master_path(root).read_bytes() == before
    assert (root / "manifest.jsonl").read_text() == entries_before


def test_a_root_that_moved_while_no_symbol_did_writes_nothing_and_refuses_nothing(
    tmp_path: Path,
):
    """Two zeroes that mean opposite things, and this is the harmless one.

    Every contract under the gained root is one the walk knows and none changed its symbol, so
    the vendor's root column moved and identity did not.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    assert _write(root, equity, history, [_row(1, OLD)]) == ()


def test_a_gained_root_the_walk_recognises_nothing_under_is_refused(tmp_path: Path):
    """The other zero, and it is the one that cannot be passed over in silence.

    It is what a vendor dropping ``ssid`` through a re-symboling would look like, and what a
    session sealed before the column was populated looks like. Whether Schwab carries ``ssid``
    through an adjustment cannot be measured from this lake, so the refusal is the honest
    answer rather than a guess. marketlake #369 is the second pairing path.
    """
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    with pytest.raises(MappingRefused, match="read none of the contracts"):
        _write(root, equity, history, [_row(9, NEW)])


# -- the two collisions ------------------------------------------------------------------


def test_an_old_symbol_two_instruments_already_hold_is_refused(tmp_path: Path):
    """A master that cannot say which contract a symbol is, is one this must not extend."""
    root, equity = _lake(tmp_path)
    master = SecurityMaster.read(master_path(root))
    for _ in range(2):
        master.register(kind=KIND_OPTION, capture_start=EPOCH, valid_from=DAY_ONE, occ_symbol=OLD)
    master.write(master_path(root))
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    with pytest.raises(MappingRefused, match="already names instruments"):
        _write(root, equity, history, [_row(1, NEW)])


def test_a_new_symbol_another_instrument_already_holds_is_refused(tmp_path: Path):
    """``remap`` accepts this without complaint and the master is ambiguous forever after.

    The forward collision is the one nothing in ``SecurityMaster`` looks for, so the guard
    has to live here.
    """
    root, equity = _lake(tmp_path)
    master = SecurityMaster.read(master_path(root))
    squatter = master.register(
        kind=KIND_OPTION, capture_start=EPOCH, valid_from=DAY_ONE, occ_symbol=NEW
    )
    master.register(kind=KIND_OPTION, capture_start=EPOCH, valid_from=DAY_ONE, occ_symbol=OLD)
    master.write(master_path(root))
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    with pytest.raises(MappingRefused, match="would make the symbol ambiguous"):
        _write(root, equity, history, [_row(1, NEW)])

    master = SecurityMaster.read(master_path(root))
    assert master.resolve(NEW, BOUNDARY, id_type=ID_TYPE_OCC) == squatter


def test_a_refused_boundary_leaves_the_master_exactly_as_it_was(tmp_path: Path):
    """The whole boundary is applied in memory first, so a refusal writes no partial state."""
    root, equity = _lake(tmp_path)
    master = SecurityMaster.read(master_path(root))
    master.register(kind=KIND_OPTION, capture_start=EPOCH, valid_from=DAY_ONE, occ_symbol=NEW)
    master.write(master_path(root))
    before = master_path(root).read_bytes()
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD), _row(2, OTHER)])

    with pytest.raises(MappingRefused):
        _write(root, equity, history, [_row(2, "SPY1  261218C00300000"), _row(1, NEW)])

    assert master_path(root).read_bytes() == before


def test_nothing_this_writes_can_make_a_symbol_ambiguous(tmp_path: Path):
    """Both guards together, over a symbol the market re-issued after its first holder left."""
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    _write(root, equity, history, [_row(1, NEW)])
    # The same OCC symbol lists again on a new contract and is itself re-symboled later.
    history.observe(BOUNDARY, [_row(1, NEW)])
    history.observe(date(2026, 9, 16), [_row(2, OLD)])

    _write(root, equity, history, [_row(2, NEWER)], day=date(2026, 9, 17))

    master = SecurityMaster.read(master_path(root))
    for symbol in (OLD, NEW, NEWER):
        for day in (DAY_TWO, BOUNDARY, date(2026, 9, 18)):
            master.resolve(symbol, day, id_type=ID_TYPE_OCC)


# -- the manifest entry beside the write -------------------------------------------------


def test_the_write_records_its_manifest_entry_and_the_scrub_stays_clean(tmp_path: Path):
    """Without the entry the scrub's forward pass reports a sha mismatch.

    ``onboard.py``'s comment says orphan, which is right for a first write. The master already
    has an entry, so a rewrite that records nothing fails the forward pass instead.
    """
    root, equity = _lake(tmp_path)
    assert scrub(root).ok
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])

    _write(root, equity, history, [_row(1, NEW)])

    result = scrub(root)
    assert result.ok, result
    master = SecurityMaster.read(master_path(root))
    latest = [line for line in (root / "manifest.jsonl").read_text().splitlines() if line]
    assert f'"rows": {len(master)}' in latest[-1]


# -- the small readers -------------------------------------------------------------------


def test_instruments_holding_asks_the_whole_table_rather_than_a_date(tmp_path: Path):
    """``resolve`` honours ranges, so it can miss the very mapping the write should extend."""
    root, equity = _lake(tmp_path)
    history = SymbolHistory()
    history.observe(DAY_ONE, [_row(1, OLD)])
    (written,) = _write(root, equity, history, [_row(1, NEW)])

    master = SecurityMaster.read(master_path(root))
    assert instruments_holding(master, OLD) == {written.instrument_id}
    assert master.resolve(OLD, date(2026, 9, 20), id_type=ID_TYPE_OCC) is None
    assert open_symbol(master, written.instrument_id) == NEW
    assert open_symbol(master, equity) is None


def test_an_ambiguous_master_is_what_the_old_symbol_guard_exists_to_avoid():
    """Named here so the guard's reason is executed rather than asserted in prose."""
    master = SecurityMaster()
    for _ in range(2):
        master.register(kind=KIND_OPTION, capture_start=EPOCH, valid_from=DAY_ONE, occ_symbol=OLD)

    with pytest.raises(AmbiguousSymbol):
        master.resolve(OLD, DAY_TWO, id_type=ID_TYPE_OCC)
