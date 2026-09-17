"""``settlement_view`` against a fixture lake on disk.

Slice 4 fetches nothing, so a fixture lake is its whole test surface. Every test here crosses
files: a sealed chains partition, a sealed daily bars partition, and the schema-version ledger.
No vendor, no network, no wall clock, and the real lake is never touched.

**This file carries its own chains schema, and that is deliberate.** The view reads
``expiration_date``, ``strike_price``, ``put_call`` and ``settlement_type``, and
``FIXTURE_CHAINS_SCHEMA`` carries none of the four. Widening the shared one is the obvious
move and #137 already found out why not. ``test_load_chain``'s test that a promoted value is
lifted out of the overflow asserts that ``volume`` is absent from
``sample_chains_table(rows)``, and its docstring says the projection adding a column is only
visible on a column the table lacks. A wider shared schema would leave that test green while
it proved nothing. So this file brings its own, exactly as
``tests/component/test_oi_view.py`` does, and the nine other files importing the shared one
stay out of the change.

The bars partitions are written in ``journal.BARS_SCHEMA``, the shape #336 pinned and ``lake.bars``
writes, rather than in a fixture schema, because what a bars row carries is what decides which of
the close's shapes this view has to refuse.

The session is a real exchange session and the stamps are the ones the live lake holds:
``expiration_date`` is an instant at 16:00 Eastern, spelled ``2026-09-15T20:00:00.000+00:00``, and
the daily bar is stamped at 20:00 UTC. A fixture that spelled the expiration as a plain date would
let a string comparison pass.

The numbered tests carry the numbering marketlake #382 asks for, so a mutation the issue names
points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal
from lake.loader import BarsAbsent, NoOptionClose, PartitionQuarantined
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.settle import (
    REASON_AM_SETTLED,
    REASON_DELIVERABLE_DISAGREES,
    REASON_NON_STANDARD,
    REASON_STRIKE_NOT_IN_CENTS,
    REASON_TERMS_UNREADABLE,
    SETTLEMENT_VIEW_SCHEMA,
    VERDICT_ABSENT,
    VERDICT_SETTLED,
    CloseUnreadable,
    ExpirationUnreadable,
    settlement_view,
)
from tests.support.lake import FixtureLake

TICKER = "SPY"
DAILY = "1d"
SESSION = "2026-09-15"
NEXT_SESSION = "2026-09-16"
INSTRUMENT = 41

# When the schema-version ledger recorded the running version. Any instant does, since the view
# reads the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# The session's official close, and the two strikes a cent on either side of it. 757.39 is what
# SPY's 2026-09-15 daily close would be, taken from the live lake's own chain underlying, and the
# 757.38 call is the contract a float comparison abandons and the OCC exercises.
CLOSE = 757.39
ONE_CENT_ITM_CALL_STRIKE = 757.38
ATM_STRIKE = 757.39
ONE_CENT_OTM_CALL_STRIKE = 757.40

# What the vendor writes for a plain contract: one stock deliverable, the underlying itself, a
# hundred shares, no cash leg. Both the typed list and the free-text note say it.
STANDARD_UNITS = 100.0
MULTIPLIER = 100.0

# The chains columns this view reads, plus the provenance the loader needs. It is a schema of this
# file's own for the reason the module docstring gives.
SETTLE_CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("occ_symbol", pa.string()),
        ("put_call", pa.string()),
        ("strike_price", pa.float64()),
        ("expiration_date", pa.string()),
        ("settlement_type", pa.string()),
        ("multiplier", pa.float64()),
        ("non_standard", pa.bool_()),
        ("mini", pa.bool_()),
        ("option_deliverables_list", pa.string()),
        ("deliverable_note", pa.string()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)


def _deliverables(units: float | None, symbol: str = TICKER, *, cash: bool = False) -> str | None:
    """The vendor's typed deliverables list, as the writer JSON-encodes it."""
    if units is None:
        return None
    entries: list[dict] = [
        {
            "assetType": "STOCK",
            "currencyType": None,
            "deliverableUnits": units,
            "symbol": symbol,
        }
    ]
    if cash:
        entries.append({"assetType": "CURRENCY", "currencyType": "USD", "deliverableUnits": 4.5})
    return json.dumps(entries)


def _contract(
    strike: float,
    *,
    put_call: str = "CALL",
    expires: str = SESSION,
    expiration_date: str | None = None,
    settlement_type: str = "P",
    multiplier: float | None = MULTIPLIER,
    non_standard: bool | None = False,
    units: float | None = STANDARD_UNITS,
    deliverable_symbol: str = TICKER,
    cash: bool = False,
    note: str | None = None,
    close_tag: str = "option_close",
) -> dict:
    """One chains row at the close of record, carrying what this view reads.

    ``expiration_date`` defaults to an instant at 16:00 Eastern on ``expires``, which is the
    shape the live lake holds and never a plain date.
    """
    side = "C" if put_call == "CALL" else "P"
    stamp = f"{expires}T20:00:00.000+00:00" if expiration_date is None else expiration_date
    occ_date = f"{expires[2:4]}{expires[5:7]}{expires[8:10]}"
    return {
        "snap_ts": f"{SESSION}T16:15:00-04:00",
        "fetch_ts": f"{SESSION}T16:15:00.400-04:00",
        "vendor_quote_ts": f"{SESSION}T16:15:00-04:00",
        "ticker": TICKER,
        "occ_symbol": f"SPY   {occ_date}{side}{int(strike * 1000):08d}",
        "put_call": put_call,
        "strike_price": strike,
        "expiration_date": stamp,
        "settlement_type": settlement_type,
        "multiplier": multiplier,
        "non_standard": non_standard,
        "mini": False,
        "option_deliverables_list": _deliverables(units, deliverable_symbol, cash=cash),
        "deliverable_note": note if note is not None else f"{int(units or 0)} {deliverable_symbol}",
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": journal.SCHEMA_VERSION,
        "extra": None,
    }


def _bar(close: float | None, *, day: str = SESSION, minute: str = "20:00") -> dict:
    """One daily bars row at the pinned schema's column names."""
    return {
        "bar_ts": f"{day}T{minute}:00+00:00",
        "fetch_ts": f"{day}T22:00:00+00:00",
        "fetch_end_ts": f"{day}T22:00:01+00:00",
        "ticker": TICKER,
        "instrument_id": INSTRUMENT,
        "freq": DAILY,
        "open": None if close is None else close - 1.0,
        "high": None if close is None else close + 1.0,
        "low": None if close is None else close - 2.0,
        "close": close,
        "volume": 100,
        "window_start": f"{day}T13:30:00+00:00",
        "window_end": f"{day}T20:00:00+00:00",
        "extended_hours": None,
        "schema_version": journal.SCHEMA_VERSION,
        "extra": None,
    }


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


def _lake(
    fixture_lake: FixtureLake,
    contracts: list[dict],
    bars: list[dict] | None = None,
    *,
    with_bars: bool = True,
    quarantine: list[dict] | None = None,
) -> Path:
    """A lake holding one chains partition, one daily bars partition, and the version ledger."""
    fixture_lake.with_chains(TICKER, SESSION, _table(SETTLE_CHAINS_SCHEMA, contracts))
    if with_bars:
        rows = bars if bars is not None else [_bar(CLOSE)]
        fixture_lake.with_bars(TICKER, DAILY, SESSION, _table(journal.BARS_SCHEMA, rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _view(root: Path, **kwargs) -> pa.Table:
    return settlement_view(TICKER, SESSION, lake_root=root, **kwargs)


def _rows(table: pa.Table) -> list[dict]:
    return table.to_pylist()


def _only(table: pa.Table) -> dict:
    assert table.num_rows == 1, f"expected one row, got {table.num_rows}"
    return table.to_pylist()[0]


# -- the roster ------------------------------------------------------------------------


def test_1_the_roster_is_the_session_expiries_and_nothing_else(fixture_lake):
    """Test 1. A session's expiring contracts are the rows returned, and no others.

    The chain carries four contracts and only two expire on the session. A view that returned
    the whole chain, or that filtered on something other than the expiration, moves this count.
    """
    root = _lake(
        fixture_lake,
        [
            _contract(750.0),
            _contract(760.0, put_call="PUT"),
            _contract(700.0, expires="2026-09-18"),
            _contract(800.0, put_call="PUT", expires="2026-12-18"),
        ],
    )
    answer = _view(root)
    assert answer.schema == SETTLEMENT_VIEW_SCHEMA
    assert [row["strike_price"] for row in _rows(answer)] == [750.0, 760.0]
    assert {row["session"] for row in _rows(answer)} == {SESSION}


def test_2_the_close_is_the_daily_partitions_and_not_the_chains(fixture_lake):
    """Test 2. The settlement close is the ``1d`` partition's close.

    The daily bar says 700.00 while every other number in the fixture is built around 757.39,
    so a view reading the close off anything but ``bars/`` returns the other figure.
    """
    root = _lake(fixture_lake, [_contract(650.0)], [_bar(700.0)])
    row = _only(_view(root))
    assert row["settlement_close"] == 700.0
    assert row["intrinsic_cents"] == 5000


def test_17_the_expiry_is_read_from_the_stamps_eastern_date(fixture_lake):
    """Test 17. A contract stamped ``2026-09-15T20:00:00.000+00:00`` belongs to 2026-09-15.

    The column is an instant rather than a date, so an equality against the session matches
    nothing and the view would hand back an empty table on every session without raising.
    """
    root = _lake(fixture_lake, [_contract(750.0, expiration_date=f"{SESSION}T20:00:00.000+00:00")])
    assert _only(_view(root))["verdict"] == VERDICT_SETTLED


def test_18_a_session_with_no_expiry_returns_an_empty_table(fixture_lake):
    """Test 18. A chain holding no contract expiring on the session returns no rows.

    That is a real answer rather than an ambiguous one, because every way this read can fail
    raises before the roster is built. The schema comes back either way, so a caller reading
    columns off the result does not have to branch on emptiness.
    """
    root = _lake(fixture_lake, [_contract(750.0, expires="2026-09-18")])
    answer = _view(root)
    assert answer.num_rows == 0
    assert answer.schema == SETTLEMENT_VIEW_SCHEMA


def test_the_answer_is_ordered_by_occ_symbol(fixture_lake):
    """The order is the contract's identity, not the order the partition happens to hold.

    The rows are written in descending strike, so a view returning the partition's own order
    returns them the other way round.
    """
    root = _lake(fixture_lake, [_contract(760.0), _contract(750.0), _contract(755.0)])
    assert [row["strike_price"] for row in _rows(_view(root))] == [750.0, 755.0, 760.0]


# -- the arithmetic --------------------------------------------------------------------


def test_3_a_contract_one_cent_in_the_money_is_exercised(fixture_lake):
    """Test 3. A call struck one cent below the close is exercised.

    ``757.39 - 757.38`` in IEEE doubles is ``0.009999999999990905``, which is less than
    ``0.01``, so a float comparison abandons the contract the OCC exercises. This is the whole
    reason the arithmetic runs in whole cents.
    """
    root = _lake(fixture_lake, [_contract(ONE_CENT_ITM_CALL_STRIKE)])
    row = _only(_view(root))
    assert row["verdict"] == VERDICT_SETTLED
    assert row["intrinsic_cents"] == 1
    assert row["exercised"] is True


def test_4_a_contract_at_the_money_is_not_exercised(fixture_lake):
    """Test 4. A call struck exactly at the close settles at zero and is not exercised.

    Exercise-by-exception takes a contract a cent or more in the money, so the boundary is
    between zero and one cent rather than at zero.
    """
    root = _lake(fixture_lake, [_contract(ATM_STRIKE)])
    row = _only(_view(root))
    assert row["verdict"] == VERDICT_SETTLED
    assert row["intrinsic_cents"] == 0
    assert row["exercised"] is False


def test_5_an_out_of_the_money_contract_settles_at_zero(fixture_lake):
    """Test 5. An out-of-the-money contract's intrinsic is zero, never a negative number.

    The vendor's own ``intrinsic_value`` column is the signed difference with no floor, and it
    is negative on half of every expiry roster. A view taking that column, or leaving its own
    difference unfloored, returns -207.39 for the 550 put below.
    """
    root = _lake(
        fixture_lake,
        [_contract(ONE_CENT_OTM_CALL_STRIKE), _contract(550.0, put_call="PUT")],
    )
    rows = _rows(_view(root))
    assert [row["intrinsic_cents"] for row in rows] == [0, 0]
    assert [row["exercised"] for row in rows] == [False, False]
    assert {row["verdict"] for row in rows} == {VERDICT_SETTLED}


def test_6_a_puts_intrinsic_is_the_strike_less_the_close(fixture_lake):
    """Test 6. A put struck above the close settles at strike minus close.

    A view running the call's direction on a put returns zero here, since the call difference
    is negative and floors.
    """
    root = _lake(fixture_lake, [_contract(760.0, put_call="PUT")])
    row = _only(_view(root))
    assert row["intrinsic_cents"] == 261
    assert row["exercised"] is True


def test_the_multiplier_is_carried_so_a_caller_can_value_the_contract(fixture_lake):
    """The answer carries the settlement's inputs, which is what makes the number auditable."""
    root = _lake(fixture_lake, [_contract(750.0)])
    row = _only(_view(root))
    assert row["multiplier"] == MULTIPLIER
    assert row["put_call"] == "CALL"
    assert row["ticker"] == TICKER
    assert row["occ_symbol"] == "SPY   260915C00750000"


# -- the markers -----------------------------------------------------------------------


def test_7_an_am_settled_contract_is_absent_and_the_rest_still_settles(fixture_lake):
    """Test 7. A contract that is not PM-settled carries ``am_settled`` on its own row.

    It settles against the session's opening print, which this lake holds on no surface. A
    view that refused the read instead takes the ordinary contract beside it away too.
    """
    root = _lake(
        fixture_lake,
        [_contract(750.0), _contract(755.0, settlement_type="A")],
    )
    rows = _rows(_view(root))
    assert [row["verdict"] for row in rows] == [VERDICT_SETTLED, VERDICT_ABSENT]
    assert [row["reason"] for row in rows] == [None, REASON_AM_SETTLED]
    assert rows[1]["intrinsic_cents"] is None
    assert rows[1]["exercised"] is None
    # The close belongs to the session rather than the contract, so a withheld row carries it.
    assert rows[1]["settlement_close"] == CLOSE


def test_8_a_contract_both_witnesses_call_non_standard_is_absent(fixture_lake):
    """Test 8. A contract delivering 150 shares, flagged non-standard, carries ``non_standard``.

    The design pins that such a position is modeled deliverable-exactly or force-closed, never
    marked as if it were still standard, and this view has no deliverable terms to model it
    from.
    """
    root = _lake(
        fixture_lake,
        [_contract(750.0), _contract(755.0, non_standard=True, units=150.0)],
    )
    rows = _rows(_view(root))
    assert [row["reason"] for row in rows] == [None, REASON_NON_STANDARD]


def test_9_a_flag_and_a_deliverable_that_disagree_are_absent(fixture_lake):
    """Test 9. A contract the flag calls standard and the list does not is withheld.

    This is the gate shape the split detector runs on the same column, the vendor against
    itself. A view deciding from the flag alone settles this contract at close minus strike,
    which is not what a contract delivering 150 shares is worth.
    """
    root = _lake(
        fixture_lake,
        [
            _contract(750.0, non_standard=False, units=150.0),
            _contract(755.0, non_standard=True, units=STANDARD_UNITS),
        ],
    )
    rows = _rows(_view(root))
    assert [row["reason"] for row in rows] == [
        REASON_DELIVERABLE_DISAGREES,
        REASON_DELIVERABLE_DISAGREES,
    ]


def test_9b_a_cash_leg_and_a_foreign_deliverable_are_not_shares_at_strike(fixture_lake):
    """Test 9, the other two ways a deliverable stops being the plain one.

    Shares plus cash is #136's own example of an adjustment no multiplier describes, and a
    deliverable of another security is not the underlying at all. Both are read off the list
    while the flag still says standard, so both disagree.
    """
    root = _lake(
        fixture_lake,
        [
            _contract(750.0, cash=True, note="100 SPY + 4.5 USD"),
            _contract(755.0, deliverable_symbol="IVV", note="100 IVV"),
        ],
    )
    assert [row["reason"] for row in _rows(_view(root))] == [
        REASON_DELIVERABLE_DISAGREES,
        REASON_DELIVERABLE_DISAGREES,
    ]


def test_10_a_strike_that_is_not_a_whole_cent_is_absent(fixture_lake):
    """Test 10. A strike of 750.005 carries ``strike_not_in_cents``.

    The comparison runs in whole cents, so a strike the scaling cannot represent has no
    threshold to be measured against. An adjusted strike is where one would come from.
    """
    root = _lake(fixture_lake, [_contract(750.0), _contract(750.005)])
    rows = _rows(_view(root))
    assert [row["reason"] for row in rows] == [None, REASON_STRIKE_NOT_IN_CENTS]


def test_a_row_missing_a_term_is_absent_rather_than_dropped(fixture_lake):
    """A contract whose terms cannot be read is withheld and stays in the roster.

    A null multiplier, an unreadable deliverables list and a side the view does not know are
    damaged rows rather than contract properties, so each is one reason. Dropping them would
    make the roster silently short, and refusing would take the ordinary contract beside them.
    """
    root = _lake(
        fixture_lake,
        [
            _contract(750.0, multiplier=None),
            _contract(755.0, units=None, note=None),
            _contract(760.0, put_call="PUT", non_standard=None),
        ],
    )
    rows = _rows(_view(root))
    assert [row["reason"] for row in rows] == [REASON_TERMS_UNREADABLE] * 3
    assert [row["verdict"] for row in rows] == [VERDICT_ABSENT] * 3


# -- what refuses the whole read -------------------------------------------------------


def test_11_a_session_with_no_daily_partition_raises(fixture_lake):
    """Test 11. A lake holding the chain and no bars raises ``BarsAbsent``.

    That is the live lake's own state until somebody runs ``python -m lake.bars``, so it is the
    first thing a caller will meet.
    """
    root = _lake(fixture_lake, [_contract(750.0)], with_bars=False)
    with pytest.raises(BarsAbsent):
        _view(root)


def test_12_a_session_with_no_option_close_raises(fixture_lake):
    """Test 12. A chain whose rows carry no ``option_close`` tag raises ``NoOptionClose``.

    It comes out of the loader unchanged, because this view adds no reason of its own to it.
    """
    root = _lake(fixture_lake, [_contract(750.0, close_tag=None)])
    with pytest.raises(NoOptionClose):
        _view(root)


def test_13_a_daily_bar_with_no_close_refuses_the_read(fixture_lake):
    """Test 13. A null close settles no contract on the session, so it refuses.

    ``journal.BARS_SCHEMA`` makes ``bar_ts`` the one non-null column, so this is a shape the
    lake permits and the evening sweep's gate refuses. A view computing with it raises a
    ``TypeError`` from inside the arithmetic instead of saying what was missing.
    """
    root = _lake(fixture_lake, [_contract(750.0)], [_bar(None)])
    with pytest.raises(CloseUnreadable):
        _view(root)


def test_13b_a_close_that_is_not_a_whole_cent_refuses_the_read(fixture_lake):
    """Test 13, the session-wide half. A close of 757.395 settles nothing on the session.

    It is a refusal rather than a marker on every row, because the close belongs to the session
    and a per-row reason naming the strike would point at the wrong column.
    """
    root = _lake(fixture_lake, [_contract(750.0)], [_bar(757.395)])
    with pytest.raises(CloseUnreadable):
        _view(root)


def test_an_unreadable_expiration_refuses_rather_than_dropping_the_row(fixture_lake):
    """A contract whose expiration names no session puts the roster's membership in doubt.

    It cannot be placed inside or outside the roster, and a roster silently missing an expiring
    contract is the one failure nothing downstream can detect. That is the reasoning
    ``_load_surface`` already uses to refuse a partition holding a null ``row_kind``.
    """
    root = _lake(
        fixture_lake,
        [_contract(750.0), _contract(755.0, expiration_date="not a stamp")],
    )
    with pytest.raises(ExpirationUnreadable):
        _view(root)


def test_14_a_quarantined_bars_partition_refuses_and_opts_in(fixture_lake):
    """Test 14. A flagged daily partition refuses by default and is read under the opt-in.

    The flag reaches ``load_bars`` rather than stopping at this view, which is what keeps the
    exclusion in one place.
    """
    root = _lake(
        fixture_lake,
        [_contract(750.0)],
        quarantine=[
            {
                "partition": f"bars/ticker={TICKER}/freq={DAILY}/date={SESSION}.parquet",
                "quarantined": True,
                "reason": "battery",
            }
        ],
    )
    with pytest.raises(PartitionQuarantined):
        _view(root)
    assert _only(_view(root, include_quarantined=True))["verdict"] == VERDICT_SETTLED


def test_15_a_quarantined_chains_partition_refuses(fixture_lake):
    """Test 15. A flagged chains partition refuses too, out of ``load_chain``.

    Both surfaces are behind the same flag, so a verdict on either takes the answer away.
    """
    root = _lake(
        fixture_lake,
        [_contract(750.0)],
        quarantine=[
            {
                "partition": f"chains/ticker={TICKER}/date={SESSION}.parquet",
                "quarantined": True,
                "reason": "battery",
            }
        ],
    )
    with pytest.raises(PartitionQuarantined):
        _view(root)
    assert _only(_view(root, include_quarantined=True))["verdict"] == VERDICT_SETTLED


def test_16_two_candles_for_the_session_take_the_last_by_stamp(fixture_lake):
    """Test 16. A daily partition holding two candles settles against the later one.

    The daily gate checks that the session came back rather than that exactly one candle did,
    so this is a shape that can land. ``bars._bar_close`` already decided that the last by
    stamp is the session's close, and this takes the same reading rather than the first row.
    """
    root = _lake(
        fixture_lake,
        [_contract(650.0)],
        [_bar(700.0, minute="19:00"), _bar(CLOSE, minute="20:00")],
    )
    row = _only(_view(root))
    assert row["settlement_close"] == CLOSE
    assert row["intrinsic_cents"] == 10739
