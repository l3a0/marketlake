"""The pinned schemas and the row builders, decided from values alone.

These tests build record batches in memory. No file, process, or query engine is
crossed, so they sit in the unit tier. They cover the capture schema's shape, the
vendor-field mapping, the fail-open overflow, and the gap-row nulling.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import journal, paths
from lake.vendor import BARS_ENDPOINT
from tests.support.lake import FixtureLake

# A surface the lake lays out a directory for and this module pins no capture schema for,
# which is what these tests need a stand-in for. It was ``bars`` until marketlake #336
# pinned one. ``actions`` is a single append-only ledger rather than a measurement, so it
# has no capture schema to pin, but the right response to this going stale again is to
# repoint it rather than to assume it cannot.
UNPINNED_SURFACE = paths.ACTIONS

# The two synthetic per-contract quote times, as epoch milliseconds. The call and the
# put carry different stamps so the per-contract derivation is observable. Each row's
# ``vendor_quote_ts`` is derived from its own contract's ``quoteTimeInLong``.
CALL_QUOTE_TIME_MS = 1787000099000
PUT_QUOTE_TIME_MS = 1787000098000
CALL_VQT = datetime.fromtimestamp(CALL_QUOTE_TIME_MS / 1000.0, tz=UTC).isoformat()
PUT_VQT = datetime.fromtimestamp(PUT_QUOTE_TIME_MS / 1000.0, tz=UTC).isoformat()


def _full_contract(**overrides):
    """A fully-populated Schwab contract: every field the real payload carries.

    Starting from this template keeps ``extra`` empty, since every field is known. A
    contract overrides only what makes it distinct. ``quoteTimeInLong`` is the per-row
    quote time and ``optionDeliverablesList`` is the nested deliverables list.
    """
    contract = {
        "putCall": "CALL",
        "symbol": "SPY   260918C00650000",
        "bid": 4.2,
        "ask": 4.25,
        "last": 4.22,
        "bidSize": 10,
        "askSize": 12,
        "lastSize": 3,
        "bidAskSize": "10X12",
        "openInterest": 1234,
        "totalVolume": 5555,
        "openPrice": 4.1,
        "highPrice": 4.3,
        "lowPrice": 4.0,
        "closePrice": 4.15,
        "mark": 4.23,
        "markChange": -0.1,
        "markPercentChange": -2.3,
        "netChange": -0.1,
        "percentChange": -2.3,
        "volatility": 12.5,
        "delta": 0.51,
        "gamma": 0.03,
        "theta": -0.12,
        "vega": 0.34,
        "rho": 0.08,
        "theoreticalOptionValue": 4.22,
        "theoreticalVolatility": 12.6,
        "intrinsicValue": 0.1,
        "extrinsicValue": 4.12,
        "timeValue": 4.12,
        "breakEven": 654.2,
        "high52Week": 21.9,
        "low52Week": 0.24,
        "strikePrice": 650.0,
        "multiplier": 100.0,
        "daysToExpiration": 25,
        "expirationDate": "2026-09-18T20:00:00.000+00:00",
        "expirationType": "M",
        "exerciseType": "A",
        "settlementType": "P",
        "optionRoot": "SPY",
        "deliverableNote": "100 SPY",
        "description": "SPY 09/18/2026 650.00 C",
        "exchangeName": "OPR",
        "inTheMoney": True,
        "nonStandard": False,
        "mini": False,
        "pennyPilot": True,
        "ssid": 139171819,
        "lastTradingDay": 1788220800000,
        "tradeTimeInLong": 1787000098000,
        "quoteTimeInLong": CALL_QUOTE_TIME_MS,
        "optionDeliverablesList": [
            {
                "symbol": "SPY",
                "assetType": "STOCK",
                "deliverableUnits": 100.0,
                "currencyType": None,
            }
        ],
    }
    contract.update(overrides)
    return contract


# A chain body shaped like a real Schwab response: the underlying block null, the price
# in the top-level scalar, one fully-populated call and one put.
CHAIN_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "isDelayed": False,
    "interestRate": 4.25,
    "underlyingPrice": 650.01,
    "dividendYield": 1.28,
    "isChainTruncated": False,
    "numberOfContracts": 2,
    "underlying": None,
    "callExpDateMap": {"2026-09-18:25": {"650.0": [_full_contract()]}},
    "putExpDateMap": {
        "2026-09-18:25": {
            "650.0": [
                _full_contract(
                    putCall="PUT",
                    symbol="SPY   260918P00650000",
                    bid=3.8,
                    ask=3.85,
                    last=3.82,
                    openInterest=987,
                    totalVolume=4444,
                    volatility=12.7,
                    delta=-0.49,
                    theta=-0.11,
                    rho=-0.07,
                    quoteTimeInLong=PUT_QUOTE_TIME_MS,
                    description="SPY 09/18/2026 650.00 P",
                )
            ]
        }
    },
}

# A full per-symbol quote envelope, shaped like a Schwab quote response. It carries the
# quote block, the envelope-level realtime flag and reference CUSIP, and the full
# fundamental, regular, and extended blocks. The extended block deliberately reuses the
# quote block's field names to exercise the collision handling.
QUOTE = {
    "assetMainType": "EQUITY",
    "realtime": True,
    "reference": {"cusip": "111111111"},
    "quote": {
        "bidPrice": 649.98,
        "askPrice": 650.02,
        "lastPrice": 650.0,
        "quoteTime": 1787000100000,
        "bidSize": 5,
        "askSize": 7,
        "lastSize": 3,
        "bidMICId": "XNYS",
        "askMICId": "XNAS",
        "lastMICId": "XNYS",
        "bidTime": 1787000099000,
        "askTime": 1787000099500,
        "tradeTime": 1787000098000,
        "highPrice": 655.0,
        "lowPrice": 645.0,
        "openPrice": 648.0,
        "closePrice": 649.0,
        "mark": 650.0,
        "markChange": 1.0,
        "markPercentChange": 0.15,
        "netChange": 1.2,
        "netPercentChange": 0.18,
        "postMarketChange": 0.3,
        "postMarketPercentChange": 0.05,
        "totalVolume": 90000000,
        "volatility": 12.5,
        "52WeekHigh": 705.0,
        "52WeekLow": 495.0,
        "securityStatus": "Normal",
    },
    "fundamental": {
        "divPayAmount": 1.75,
        "divExDate": "2026-09-18",
        "divAmount": 7.0,
        "divFreq": 4,
        "declarationDate": "2026-08-15",
        "nextDivExDate": "2026-12-18",
        "nextDivPayDate": "2026-12-31",
        "divPayDate": "2026-09-30",
        "divYield": 1.28,
        "peRatio": 24.5,
        "eps": 22.3,
        "high52": 700.0,
        "low52": 500.0,
        "avg10DaysVolume": 74000000.0,
        "avg1YearVolume": 80000000.0,
        "lastEarningsDate": "2026-07-30",
        "fundLeverageFactor": 1.0,
        "sharesOutstanding": 900000000,
    },
    "regular": {
        "regularMarketLastPrice": 649.5,
        "regularMarketLastSize": 100,
        "regularMarketNetChange": 1.2,
        "regularMarketPercentChange": 0.18,
        "regularMarketTradeTime": 1787000100000,
    },
    "extended": {
        "lastPrice": 651.0,
        "bidPrice": 650.9,
        "askPrice": 651.1,
        "bidSize": 5,
        "askSize": 7,
        "lastSize": 3,
        "mark": 651.0,
        "quoteTime": 1787000200000,
        "tradeTime": 1787000200500,
        "totalVolume": 2000,
    },
}

SNAP = "2026-08-24T16:15:00-04:00"
FETCH = "2026-08-24T16:15:00.400-04:00"
FETCH_END = "2026-08-24T16:15:00.812-04:00"
VENDOR = "2026-08-24T16:15:00-04:00"


# -- schema shape ------------------------------------------------------------


def test_chains_schema_names_and_types():
    schema = journal.CHAINS_SCHEMA
    assert schema.field("snap_ts").type == pa.string()
    assert schema.field("fetch_ts").type == pa.string()
    # The request-end stamp is the pair to fetch_ts, a nullable string like the others.
    assert schema.field("fetch_end_ts").type == pa.string()
    assert schema.field("vendor_quote_ts").type == pa.string()
    assert schema.field("open_interest").type == pa.int64()
    # Traded volume is a nullable int64 per-contract column, next to open interest.
    assert schema.field("volume").type == pa.int64()
    assert schema.field("suspect").type == pa.bool_()
    assert schema.field("schema_version").type == pa.int64()
    # The real-time entitlement flag is a chain-level bool, not the provenance suspect.
    assert schema.field("is_delayed").type == pa.bool_()
    # The two new chain-level fields: the truncation flag is a bool, the count an int64.
    assert schema.field("is_chain_truncated").type == pa.bool_()
    assert schema.field("number_of_contracts").type == pa.int64()
    # Vendor per-contract columns, the chain-level fields, and the provenance columns.
    for name in (
        "fetch_end_ts",
        "occ_symbol",
        "put_call",
        "bid",
        "ask",
        "last",
        "volume",
        "volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "interest_rate",
        "underlying_price",
        "dividend_yield",
        "is_delayed",
        "is_chain_truncated",
        "number_of_contracts",
        "row_kind",
        "error_class",
        "close_tag",
        "session_phase",
        "extra",
    ):
        assert name in schema.names, name
    # The full per-contract field set, calibrated to the real payload. Sizes are int64,
    # the string fields string, the classification flags bool, and the epoch-millisecond
    # stamps int64. ``expiration_date`` is an ISO string, not an epoch.
    for float_field in (
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "mark",
        "mark_change",
        "mark_percent_change",
        "net_change",
        "percent_change",
        "theoretical_option_value",
        "theoretical_volatility",
        "intrinsic_value",
        "extrinsic_value",
        "time_value",
        "break_even",
        "high_52_week",
        "low_52_week",
        "strike_price",
        "multiplier",
    ):
        assert schema.field(float_field).type == pa.float64(), float_field
    for int_field in (
        "bid_size",
        "ask_size",
        "last_size",
        "days_to_expiration",
        "ssid",
        "last_trading_day",
        "trade_time",
    ):
        assert schema.field(int_field).type == pa.int64(), int_field
    for str_field in (
        "bid_ask_size",
        "expiration_date",
        "expiration_type",
        "exercise_type",
        "settlement_type",
        "option_root",
        "deliverable_note",
        "description",
        "exchange_name",
        "option_deliverables_list",
    ):
        assert schema.field(str_field).type == pa.string(), str_field
    for bool_field in ("in_the_money", "non_standard", "mini", "penny_pilot"):
        assert schema.field(bool_field).type == pa.bool_(), bool_field


def test_quotes_schema_names_and_types():
    schema = journal.QUOTES_SCHEMA
    assert schema.names[:5] == [
        "snap_ts",
        "fetch_ts",
        "fetch_end_ts",
        "vendor_quote_ts",
        "ticker",
    ]
    for name in (
        "fetch_end_ts",
        # quote block — full field set
        "bid",
        "ask",
        "last",
        "bid_size",
        "ask_size",
        "last_size",
        "bid_mic_id",
        "ask_mic_id",
        "last_mic_id",
        "bid_time",
        "ask_time",
        "trade_time",
        "high_price",
        "low_price",
        "open_price",
        "close_price",
        "mark",
        "mark_change",
        "mark_percent_change",
        "net_change",
        "net_percent_change",
        "post_market_change",
        "post_market_percent_change",
        "total_volume",
        "volatility",
        "week_52_high",
        "week_52_low",
        "security_status",
        "realtime",
        "cusip",
        # fundamental block
        "div_pay_amount",
        "div_ex_date",
        "div_amount",
        "div_freq",
        "declaration_date",
        "next_div_ex_date",
        "next_div_pay_date",
        "div_pay_date",
        "div_yield",
        "pe_ratio",
        "eps",
        "high_52",
        "low_52",
        "avg_10_days_volume",
        "avg_1_year_volume",
        "last_earnings_date",
        "fund_leverage_factor",
        # regular block
        "regular_market_last_price",
        "regular_market_last_size",
        "regular_market_net_change",
        "regular_market_percent_change",
        "regular_market_trade_time",
        # extended block
        "extended_last_price",
        "extended_bid_price",
        "extended_ask_price",
        "extended_bid_size",
        "extended_ask_size",
        "extended_last_size",
        "extended_mark",
        "extended_quote_time",
        "extended_trade_time",
        "extended_total_volume",
        "row_kind",
        "schema_version",
        "extra",
    ):
        assert name in schema.names, name
    # The quote block: prices/mark/changes/volatility float, sizes and volume int, MIC ids
    # and times and status string. All nullable.
    for float_field in (
        "high_price",
        "low_price",
        "open_price",
        "close_price",
        "mark",
        "mark_change",
        "mark_percent_change",
        "net_change",
        "net_percent_change",
        "post_market_change",
        "post_market_percent_change",
        "volatility",
        "week_52_high",
        "week_52_low",
    ):
        assert schema.field(float_field).type == pa.float64()
    for int_field in (
        "bid_size",
        "ask_size",
        "last_size",
        "total_volume",
        "bid_time",
        "ask_time",
        "trade_time",
    ):
        assert schema.field(int_field).type == pa.int64()
    for str_field in ("bid_mic_id", "ask_mic_id", "last_mic_id", "security_status"):
        assert schema.field(str_field).type == pa.string()
    # The entitlement flag is a per-row vendor bool.
    assert schema.field("realtime").type == pa.bool_()
    # The CUSIP is a nullable string vendor column on quotes only.
    assert schema.field("cusip").type == pa.string()
    # The dividend fundamentals: amounts float, frequency int, dates string. All nullable.
    assert schema.field("div_pay_amount").type == pa.float64()
    assert schema.field("div_amount").type == pa.float64()
    assert schema.field("div_freq").type == pa.int64()
    for date_field in ("div_ex_date", "declaration_date", "next_div_ex_date", "next_div_pay_date"):
        assert schema.field(date_field).type == pa.string()
    # The rest of the fundamental block: valuation floats, volume floats, string dates.
    for float_field in (
        "div_yield",
        "pe_ratio",
        "eps",
        "high_52",
        "low_52",
        "fund_leverage_factor",
    ):
        assert schema.field(float_field).type == pa.float64()
    assert schema.field("avg_10_days_volume").type == pa.float64()
    assert schema.field("avg_1_year_volume").type == pa.float64()
    assert schema.field("shares_outstanding").type == pa.int64()
    assert schema.field("last_earnings_date").type == pa.string()
    assert schema.field("div_pay_date").type == pa.string()
    # The regular block: prices float, size int, net/percent float, trade time int (epoch-ms).
    assert schema.field("regular_market_last_price").type == pa.float64()
    assert schema.field("regular_market_last_size").type == pa.int64()
    assert schema.field("regular_market_net_change").type == pa.float64()
    assert schema.field("regular_market_percent_change").type == pa.float64()
    assert schema.field("regular_market_trade_time").type == pa.int64()
    # The extended block: prices/mark float, sizes and volume int, times int (epoch-ms).
    for float_field in (
        "extended_last_price",
        "extended_bid_price",
        "extended_ask_price",
        "extended_mark",
    ):
        assert schema.field(float_field).type == pa.float64()
    for int_field in (
        "extended_bid_size",
        "extended_ask_size",
        "extended_last_size",
        "extended_total_volume",
    ):
        assert schema.field(int_field).type == pa.int64()
    assert schema.field("extended_quote_time").type == pa.int64()
    assert schema.field("extended_trade_time").type == pa.int64()
    # Quotes never carry a per-contract vendor column.
    assert "open_interest" not in schema.names
    # Chains carry none of the equity CUSIP, dividend, regular, or extended columns.
    assert "cusip" not in journal.CHAINS_SCHEMA.names
    assert "div_amount" not in journal.CHAINS_SCHEMA.names
    assert "regular_market_last_price" not in journal.CHAINS_SCHEMA.names
    assert "extended_last_price" not in journal.CHAINS_SCHEMA.names


def test_schema_for_resolves_surfaces_and_rejects_unknown():
    assert journal.schema_for(journal.CHAINS_SURFACE) is journal.CHAINS_SCHEMA
    assert journal.schema_for(journal.QUOTES_SURFACE) is journal.QUOTES_SCHEMA
    with pytest.raises(ValueError):
        journal.schema_for("greeks")


# -- chains data rows --------------------------------------------------------


def test_chains_data_batch_one_row_per_contract():
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.schema == journal.CHAINS_SCHEMA
    assert batch.num_rows == 2
    table = pa.Table.from_batches([batch])
    symbols = table.column("occ_symbol").to_pylist()
    assert symbols == ["SPY   260918C00650000", "SPY   260918P00650000"]
    assert table.column("put_call").to_pylist() == ["CALL", "PUT"]


def test_chains_data_batch_maps_known_fields_and_header():
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    row = batch.slice(0, 1).to_pylist()[0]
    assert row["bid"] == 4.2
    assert row["ask"] == 4.25
    assert row["last"] == 4.22
    assert row["open_interest"] == 1234
    # totalVolume lands in the typed volume column, not the overflow.
    assert row["volume"] == 5555
    assert row["volatility"] == 12.5
    assert row["delta"] == 0.51
    assert row["rho"] == 0.08
    # A representative spread of the fully-typed contract fields: sizes, session prices,
    # the mark family, the value decomposition, contract terms, and the flags.
    assert row["bid_size"] == 10
    assert row["ask_size"] == 12
    assert row["bid_ask_size"] == "10X12"
    assert row["open_price"] == 4.1
    assert row["mark"] == 4.23
    assert row["net_change"] == -0.1
    assert row["intrinsic_value"] == 0.1
    assert row["break_even"] == 654.2
    assert row["strike_price"] == 650.0
    assert row["multiplier"] == 100.0
    assert row["days_to_expiration"] == 25
    # expirationDate is stored as the vendor's ISO string, not reshaped to an epoch.
    assert row["expiration_date"] == "2026-09-18T20:00:00.000+00:00"
    assert row["exercise_type"] == "A"
    assert row["option_root"] == "SPY"
    assert row["in_the_money"] is True
    assert row["penny_pilot"] is True
    # The epoch-millisecond stamps land verbatim as int64.
    assert row["ssid"] == 139171819
    assert row["last_trading_day"] == 1788220800000
    assert row["trade_time"] == 1787000098000
    # The chain-level fields repeat on every row: the headers and the entitlement flag.
    assert row["interest_rate"] == 4.25
    assert row["underlying_price"] == 650.01
    assert row["dividend_yield"] == 1.28
    assert row["is_delayed"] is False
    # The count and truncation flag are recomputed from the rows. Here the body's own
    # figures happen to match: two contracts, untruncated.
    assert row["is_chain_truncated"] is False
    assert row["number_of_contracts"] == 2


def test_chains_data_batch_recomputes_count_and_truncation_from_rows():
    # number_of_contracts and is_chain_truncated describe the captured rows, not the body's
    # declared header. A body claiming a bogus count still stores the two real contracts,
    # and an absent window forces the truncation flag true.
    body = dict(CHAIN_BODY)
    body["numberOfContracts"] = 999
    body["isChainTruncated"] = False
    batch = journal.chains_data_batch(
        body,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        absent_markers=[
            journal.AbsentMarker("2026-10-16", "2026-11-15", "chain_chunk_failed", "2026-10-16")
        ],
    )
    rows = batch.to_pylist()
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    # Two real contracts, not the bogus 999. Truncated because a window was given up.
    assert all(r["number_of_contracts"] == 2 for r in data_rows)
    assert all(r["is_chain_truncated"] is True for r in data_rows)
    # The absent-marker gap row carries its window's own class, names the expiration, keeps
    # the failed range as provenance, and nulls the chain-level fields.
    assert len(gap_rows) == 1
    assert gap_rows[0]["error_class"] == "chain_chunk_failed"
    assert gap_rows[0]["expiration_date"] == "2026-10-16"
    assert (gap_rows[0]["window_start"], gap_rows[0]["window_end"]) == ("2026-10-16", "2026-11-15")
    assert gap_rows[0]["number_of_contracts"] is None
    assert gap_rows[0]["is_chain_truncated"] is None


def test_chains_absent_markers_carry_their_own_per_window_class():
    # Each marker keeps its own error class, so a partial snapshot records why each window
    # was given up: a size give-up, a rate-limit, and a transient status side by side. The
    # per-expiration kind names its expiration. The per-window kind leaves it null.
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        absent_markers=[
            journal.AbsentMarker("2026-10-16", "2026-11-15", "chain_chunk_failed", "2026-10-30"),
            journal.AbsentMarker("2026-11-16", None, "http_429", None),
            journal.AbsentMarker("2026-09-19", "2026-09-30", "http_500", "2026-09-25"),
        ],
    )
    gap_rows = [r for r in batch.to_pylist() if r["row_kind"] == journal.ROW_KIND_GAP]
    assert {
        (r["window_start"], r["window_end"], r["error_class"], r["expiration_date"])
        for r in gap_rows
    } == {
        ("2026-10-16", "2026-11-15", "chain_chunk_failed", "2026-10-30"),
        ("2026-11-16", None, "http_429", None),
        ("2026-09-19", "2026-09-30", "http_500", "2026-09-25"),
    }
    # Every vendor column stays null on both marker kinds.
    assert all(r["bid"] is None and r["open_interest"] is None for r in gap_rows)


# -- window provenance -------------------------------------------------------


def test_chains_schema_carries_the_two_window_provenance_columns():
    # window_start and window_end are nullable ISO date strings on chains only. They sit
    # with the provenance columns, and quotes never carry them.
    schema = journal.CHAINS_SCHEMA
    assert schema.field("window_start").type == pa.string()
    assert schema.field("window_end").type == pa.string()
    assert "window_start" not in journal.QUOTES_SCHEMA.names
    assert "window_end" not in journal.QUOTES_SCHEMA.names


def test_chains_data_rows_carry_the_plan_window_holding_their_expiration():
    # The contract expires 2026-09-18. With a plan whose closed window runs to 2026-09-20 the
    # row carries that closed window. Fetch provenance is not a vendor field, so extra stays
    # empty even though the two columns are set.
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        windows=[("2026-09-01", "2026-09-20"), ("2026-09-21", None)],
    )
    rows = batch.to_pylist()
    assert {(r["window_start"], r["window_end"]) for r in rows} == {("2026-09-01", "2026-09-20")}
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_data_rows_on_the_open_tail_carry_a_null_window_end():
    # The same contract against a plan whose closed window ends before it lands on the open
    # tail, which matches anything on or after its start and has no end.
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        windows=[("2026-09-01", "2026-09-10"), ("2026-09-11", None)],
    )
    rows = batch.to_pylist()
    assert {(r["window_start"], r["window_end"]) for r in rows} == {("2026-09-11", None)}
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_data_rows_leave_the_window_null_without_a_plan():
    # The one-shot whole-chain case passes no windows, so the provenance stays null.
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("window_start").to_pylist() == [None, None]
    assert batch.column("window_end").to_pylist() == [None, None]


def test_chains_vendor_quote_ts_is_derived_per_contract():
    # Each row's vendor quote time comes from its own contract's quoteTimeInLong, not from
    # a chain-level underlying block (which is null on a real payload). The call and the
    # put carry different quote times, so the two rows carry different stamps.
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("vendor_quote_ts").to_pylist() == [CALL_VQT, PUT_VQT]
    assert CALL_VQT != PUT_VQT
    # quoteTimeInLong is consumed into the stamp, never a column and never in extra.
    assert "quote_time_in_long" not in batch.schema.names
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_option_deliverables_list_round_trips_as_json():
    # The nested deliverables list is JSON-encoded into a single string column, so the
    # design's non-standard-contract detection can read it back.
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    stored = batch.slice(0, 1).to_pylist()[0]["option_deliverables_list"]
    assert json.loads(stored) == [
        {
            "symbol": "SPY",
            "assetType": "STOCK",
            "deliverableUnits": 100.0,
            "currencyType": None,
        }
    ]


def test_chains_data_batch_provenance_and_stamps():
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        close_tag="option_close",
    )
    row = batch.slice(0, 1).to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row["error_class"] is None
    assert row["suspect"] is False
    assert row["close_tag"] == "option_close"
    assert row["session_phase"] is None
    assert row["schema_version"] == journal.SCHEMA_VERSION
    assert row["snap_ts"] == SNAP
    assert row["fetch_ts"] == FETCH
    # The stamp is derived from the call contract's own quoteTimeInLong.
    assert row["vendor_quote_ts"] == CALL_VQT
    assert row["ticker"] == "SPY"


def test_chains_extra_is_empty_for_a_fully_populated_contract():
    # Every field the real payload carries is typed or consumed, so a fully-populated
    # contract leaves the overflow empty in steady state.
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_is_delayed_lands_in_the_column_and_not_in_extra():
    # The payload's entitlement flag is now recognized. It rides the typed column on
    # every contract row, and the overflow stays empty.
    batch = journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("is_delayed").to_pylist() == [False, False]
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_unknown_contract_field_lands_in_extra():
    body = json.loads(json.dumps(CHAIN_BODY))  # deep copy
    contract = body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]
    contract["brandNewGreek"] = 1.5
    contract["anotherNewField"] = 40
    batch = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    call_extra = batch.slice(0, 1).to_pylist()[0]["extra"]
    assert json.loads(call_extra) == {"brandNewGreek": 1.5, "anotherNewField": 40}
    # Known fields still land typed, not swept into the overflow.
    assert batch.slice(0, 1).to_pylist()[0]["bid"] == 4.2
    # The put row, with no unknown field, keeps an empty overflow.
    assert batch.slice(1, 1).to_pylist()[0]["extra"] is None


# -- where a value in extra would be projected back to ------------------------


def test_every_extra_path_names_a_real_column_of_its_surface():
    """A path pointing at no column could never fill anything.

    Both sides are derived, the paths from the parser's vendor maps and the columns from
    the schema, so this is what catches a map entry that names a column the schema does
    not carry.
    """
    for surface in journal.PINNED_SURFACES:
        columns = set(journal.schema_fingerprint(surface))
        assert set(journal.extra_paths(surface)) <= columns


def test_a_contract_field_sits_flat_and_every_other_level_nests():
    """The overflow's shape follows the payload's, and the paths have to match it.

    A chains row's contract fields are the one level that cannot collide with another, so
    they sit flat. The chain-level fields repeated onto that row nest under ``chain``,
    since a contract could carry a field of the same name. Every quotes block nests under
    its own key, and the two fields read off the envelope nest under ``envelope``.
    """
    chains = journal.extra_paths("chains")
    assert chains["bid"] == journal.ExtraPath(None, "bid")
    assert chains["ask_size"] == journal.ExtraPath(None, "askSize")
    assert chains["underlying_price"] == journal.ExtraPath("chain", "underlyingPrice")
    assert chains["interest_rate"] == journal.ExtraPath("chain", "interestRate")
    assert chains["dividend_yield"] == journal.ExtraPath("chain", "dividendYield")
    assert chains["is_delayed"] == journal.ExtraPath("chain", "isDelayed")

    quotes = journal.extra_paths("quotes")
    assert quotes["bid"] == journal.ExtraPath("quote", "bidPrice")
    assert quotes["extended_last_price"] == journal.ExtraPath("extended", "lastPrice")
    assert quotes["pe_ratio"] == journal.ExtraPath("fundamental", "peRatio")
    assert quotes["realtime"] == journal.ExtraPath("envelope", "realtime")
    assert quotes["cusip"] == journal.ExtraPath("envelope", "cusip")


def test_a_chain_level_path_cannot_collide_with_a_contract_field_of_the_same_name():
    """Why the chain-level fields nest rather than sitting flat beside the contract's.

    Nothing in a flat overflow says which level a key came from, so a vendor that added a
    contract field named ``underlyingPrice`` would land it on the key the chain-level value
    uses, and the two measurements would merge with nothing able to tell them apart. The
    nesting is what makes that structurally impossible rather than merely unlikely.
    """
    chains = journal.extra_paths("chains")
    flat = {path.field for path in chains.values() if path.block is None}
    nested = {path.field for path in chains.values() if path.block is not None}
    assert nested, "the chain-level fields are what nest, so an empty set means none do"
    # Both directions of the same claim. No two paths are equal, and no chain-level field
    # would read as a flat key even where the vendor uses one name at both levels.
    assert len(set(chains.values())) == len(chains)
    for field in nested:
        assert journal.ExtraPath(None, field) not in set(chains.values())
    assert flat.isdisjoint({"chain"}), "a flat key named chain would shadow the nested block"


def test_the_two_quote_blocks_sharing_field_names_keep_separate_paths():
    """``quote.lastPrice`` and ``extended.lastPrice`` are different measurements.

    A flat quotes path map would collapse them, which is the collision the block-keyed
    overflow exists to prevent.
    """
    paths = journal.extra_paths("quotes")
    assert paths["last"] == journal.ExtraPath("quote", "lastPrice")
    assert paths["extended_last_price"] == journal.ExtraPath("extended", "lastPrice")
    assert len(set(paths.values())) == len(paths)


@pytest.mark.parametrize(
    ("surface", "column"),
    [
        # The two chain-level fields the row builder recomputes from the captured rows.
        # Neither column ever holds the vendor's own value, so neither has one to park.
        ("chains", "is_chain_truncated"),
        ("chains", "number_of_contracts"),
        # The deliverables list. The writer JSON-encodes the vendor's nested list into
        # this string column, so a raw overflow value would not fit it, and the field has
        # been known since version 1 and so can never reach ``extra`` at all.
        ("chains", "option_deliverables_list"),
        # The consumed vendor quote time, on both surfaces.
        ("chains", "vendor_quote_ts"),
        ("quotes", "vendor_quote_ts"),
        # Provenance, stamps, and the chains window pair. None is a vendor field.
        ("chains", "snap_ts"),
        ("chains", "window_start"),
        ("quotes", "schema_version"),
        ("quotes", "extra"),
    ],
)
def test_a_column_no_vendor_field_overflows_into_is_not_projectable(surface, column):
    """Projecting one of these would manufacture a value the overflow never held."""
    assert column in journal.schema_fingerprint(surface)
    assert column not in journal.extra_paths(surface)


def test_every_vendor_mapped_column_is_projectable_and_nothing_else_is():
    """Promoting a field is one edit, not two.

    The paths are read off the parser's own vendor maps, so adding the field to its
    block's map is what makes the column projectable. This walks those maps and checks
    both directions: every mapped column has its path, and the counts match, so a path
    the maps do not account for fails here too.

    The chains paths are the contract map plus the header map's verbatim fields, and the
    quotes paths are the four blocks plus the envelope map. What each exclusion leaves out
    is checked above. ``optionDeliverablesList`` is mapped nowhere because the writer
    transforms it rather than copying it, which is the one exclusion that is not about
    where a field arrives or whether the builder recomputes it.
    """
    chains = journal.extra_paths("chains")
    for vendor, column in journal._CHAINS_CONTRACT_MAP.items():
        assert chains[column] == journal.ExtraPath(None, vendor)
    headers = {
        vendor: column
        for vendor, column in journal._CHAINS_HEADER_MAP.items()
        if vendor not in journal._CHAINS_HEADER_RECOMPUTED
    }
    for vendor, column in headers.items():
        assert chains[column] == journal.ExtraPath(journal._CHAINS_HEADER_BLOCK, vendor)
    assert len(chains) == len(journal._CHAINS_CONTRACT_MAP) + len(headers)

    quotes = journal.extra_paths("quotes")
    for block, field_map, _consumed in journal._QUOTE_BLOCK_SPECS:
        for vendor, column in field_map.items():
            assert quotes[column] == journal.ExtraPath(block, vendor)
    for vendor, column in journal._QUOTES_ENVELOPE_MAP.items():
        assert quotes[column] == journal.ExtraPath(journal._QUOTES_ENVELOPE_BLOCK, vendor)
    assert len(quotes) == sum(
        len(field_map) for _, field_map, _ in journal._QUOTE_BLOCK_SPECS
    ) + len(journal._QUOTES_ENVELOPE_MAP)


def test_a_caller_editing_the_paths_it_got_back_changes_nothing():
    """The map is module state, and a reader holds a copy of it rather than the thing."""
    paths = journal.extra_paths("chains")
    paths["bid"] = journal.ExtraPath(None, "somethingElse")
    assert journal.extra_paths("chains")["bid"] == journal.ExtraPath(None, "bid")


def test_extra_paths_rejects_an_unknown_surface():
    with pytest.raises(ValueError, match="unknown surface"):
        journal.extra_paths(UNPINNED_SURFACE)


def test_a_surface_pinned_without_vendor_maps_refuses_by_name(monkeypatch):
    """Pinning a schema in one place and forgetting the maps in the other says so.

    ``schema_for`` is happy the moment a surface joins the schema map, so the refusal has
    to come from here, and it has to name the surface rather than surfacing a bare
    ``KeyError`` from a lookup the caller cannot see.
    """
    monkeypatch.setitem(journal._SCHEMAS, UNPINNED_SURFACE, journal.QUOTES_SCHEMA)

    with pytest.raises(ValueError, match=f"{UNPINNED_SURFACE}.*no vendor maps"):
        journal.extra_paths(UNPINNED_SURFACE)


def test_a_vendor_field_added_to_a_map_is_projectable_with_no_second_edit(monkeypatch):
    """Promoting a field is one edit, and this is what proves the paths are not a snapshot.

    Patching the live map and reading the paths back only works if they are derived per
    call. A map read once at import would answer from the shape the module was loaded
    with, and a promotion would silently need a second edit to become readable.
    """
    monkeypatch.setitem(journal._CHAINS_CONTRACT_MAP, "sigmaScore", "sigma_score")
    monkeypatch.setitem(journal._QUOTE_MAP, "sigmaScore", "sigma_score")

    assert journal.extra_paths("chains")["sigma_score"] == journal.ExtraPath(None, "sigmaScore")
    assert journal.extra_paths("quotes")["sigma_score"] == journal.ExtraPath("quote", "sigmaScore")


# -- the bars surface --------------------------------------------------------


def test_bars_are_pinned_and_reachable_by_name():
    """The surface has a schema and joins the pinned tuple, which is what everything reads.

    ``PINNED_SURFACES`` is derived from the schema map, so this is one fact seen from two
    sides rather than two facts. What it adds is the constant: the tuple would carry the
    string either way, and a caller reaching for ``journal.BARS_SURFACE`` needs the name.
    """
    assert journal.BARS_SURFACE == "bars"
    assert journal.schema_for(journal.BARS_SURFACE) is journal.BARS_SCHEMA
    assert journal.BARS_SURFACE in journal.PINNED_SURFACES


def test_the_bars_surface_name_matches_every_other_spelling_of_it():
    """One surface, three modules, one string.

    The storage tree names it, ``lake.vendor`` names it for a cassette interaction, and
    this module names it for a schema. A second spelling would leave a partition, a
    recording and a fingerprint that never meet.
    """
    assert journal.BARS_SURFACE == paths.BARS == BARS_ENDPOINT


def test_the_bars_schema_carries_exactly_the_columns_this_surface_decided_on():
    """The whole column list, spelled out, because the list is the deliverable.

    Deriving the expected list from the schema would compare the schema to itself. Spelling
    it means a column added or dropped fails here beside the fingerprint, and the message
    names the column rather than a shape that moved.
    """
    assert journal.BARS_SCHEMA.names == [
        "bar_ts",
        "fetch_ts",
        "fetch_end_ts",
        "ticker",
        "instrument_id",
        "freq",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "window_start",
        "window_end",
        "extended_hours",
        "schema_version",
        "extra",
    ]


def test_bars_carry_two_of_the_seven_provenance_columns_and_no_others():
    """The provenance decision, stated against the seven rather than against a literal list.

    Both sides are derived from ``_PROVENANCE_FIELDS``, so a column added to the shared
    list lands in the difference here rather than passing unexamined. The five absent ones
    are named individually, because dropping one from the set on either side of the
    comparison would otherwise pass.
    """
    every = {name for name, _ in journal._PROVENANCE_FIELDS}
    carried = every & set(journal.BARS_SCHEMA.names)
    assert carried == {"schema_version", journal.EXTRA_COLUMN}
    for absent in ("row_kind", "error_class", "suspect", "close_tag", "session_phase"):
        assert absent in every, absent
        assert absent not in journal.BARS_SCHEMA.names, absent


def test_a_provenance_column_bars_share_keeps_the_type_the_capture_surfaces_give_it():
    """The two carried columns are selected from the shared list, not restated beside it.

    A second literal would let ``schema_version`` be an ``int64`` on chains and an
    ``int32`` here, and the version ledger reads the same integer off both.
    """
    for column in ("schema_version", journal.EXTRA_COLUMN):
        assert (
            journal.BARS_SCHEMA.field(column).type
            == journal.CHAINS_SCHEMA.field(column).type
            == journal.QUOTES_SCHEMA.field(column).type
        ), column


def test_a_stamp_bars_share_with_the_capture_surfaces_carries_the_same_type():
    """Every stamp on every surface is a string, so one reader reads them all one way."""
    for column in ("fetch_ts", "fetch_end_ts", "ticker"):
        assert journal.BARS_SCHEMA.field(column).type == journal.CHAINS_SCHEMA.field(column).type, (
            column
        )
    for column in ("bar_ts", "window_start", "window_end"):
        assert journal.BARS_SCHEMA.field(column).type == pa.string(), column


def test_bar_ts_is_the_one_non_null_column_and_a_null_refuses_at_the_write():
    """The constraint, and the moment it bites.

    A bars row with no ``bar_ts`` has no identity, so the schema refuses one rather than
    leaving the rule to the writer's good behaviour. ``pa.Table`` construction does not
    check nullability, which is why the null below builds, and ``pq.write_table`` does,
    which is the call that lands a bars partition.

    The fingerprint records a column's name and type and not its nullability, so nothing
    else in the suite would notice this being relaxed.
    """
    assert journal.BARS_SCHEMA.field("bar_ts").nullable is False
    for name in journal.BARS_SCHEMA.names:
        if name != "bar_ts":
            assert journal.BARS_SCHEMA.field(name).nullable is True, name

    row = dict.fromkeys(journal.BARS_SCHEMA.names)
    table = pa.Table.from_pylist([{**row, "bar_ts": None}], schema=journal.BARS_SCHEMA)
    assert table.column("bar_ts").null_count == 1

    with pytest.raises(pa.ArrowInvalid, match="bar_ts.*non-nullable"):
        pq.write_table(table, Path(tempfile.mkdtemp()) / "bars.parquet")


def test_a_bars_row_with_a_stamp_writes_where_one_without_refuses():
    """The other side of the constraint, so the test above is not passing on a broken write."""
    row = dict.fromkeys(journal.BARS_SCHEMA.names)
    table = pa.Table.from_pylist(
        [{**row, "bar_ts": "2026-09-14T13:30:00+00:00"}], schema=journal.BARS_SCHEMA
    )

    target = Path(tempfile.mkdtemp()) / "bars.parquet"
    pq.write_table(table, target)
    assert pq.read_table(target).column("bar_ts").to_pylist() == ["2026-09-14T13:30:00+00:00"]


def test_the_bars_vendor_maps_name_the_five_columns_copied_from_a_candle():
    """The vendor maps, which pinning obliges whether or not the schema carries ``extra``.

    A bars row is built from one candle dict, so every path sits flat. The five are the
    whole of what a candle copies verbatim, and the map is derived from
    ``_BARS_CANDLE_MAP`` rather than restated, so promoting a sixth field stays one edit.
    """
    paths_for_bars = journal.extra_paths(journal.BARS_SURFACE)
    assert paths_for_bars == {
        "open": journal.ExtraPath(None, "open"),
        "high": journal.ExtraPath(None, "high"),
        "low": journal.ExtraPath(None, "low"),
        "close": journal.ExtraPath(None, "close"),
        "volume": journal.ExtraPath(None, "volume"),
    }
    assert set(paths_for_bars) == set(journal._BARS_CANDLE_MAP.values())


def test_the_consumed_candle_stamp_has_no_bars_vendor_path():
    """``bar_ts`` is consumed from ``datetime``, so no key in an overflow feeds it.

    The only value of that field ever reaching ``extra`` is one the epoch transform
    refused, and projecting it back would need the transform that already said no. This is
    the rule ``vendor_quote_ts`` already follows on the two capture surfaces.
    """
    paths_for_bars = journal.extra_paths(journal.BARS_SURFACE)
    assert "bar_ts" not in paths_for_bars
    assert journal._BARS_CANDLE_TS_FIELD == "datetime"
    assert journal._BARS_CANDLE_TS_FIELD not in journal._BARS_CANDLE_MAP
    assert "datetime" not in {path.field for path in paths_for_bars.values()}


def test_no_bars_vendor_path_names_a_column_that_is_not_a_vendor_field():
    """The request's own values and the fetch provenance are not candidates for an overflow.

    None of these is a vendor field, so a path to one would claim a key could arrive in
    ``extra`` and feed it, and a promotion would then lift a value the vendor never sent.
    """
    paths_for_bars = journal.extra_paths(journal.BARS_SURFACE)
    for column in (
        "bar_ts",
        "fetch_ts",
        "fetch_end_ts",
        "ticker",
        "instrument_id",
        "freq",
        "window_start",
        "window_end",
        "extended_hours",
        "schema_version",
        journal.EXTRA_COLUMN,
    ):
        assert column in journal.BARS_SCHEMA.names, column
        assert column not in paths_for_bars, column


def test_a_candle_field_added_to_the_bars_map_is_projectable_with_no_second_edit(monkeypatch):
    """Promoting a candle field stays one edit here too, because the paths derive per call.

    The patched pair is deliberately not an identity. All five real candle fields are their
    own column name, so a map read backwards the wrong way round produces the identical
    result and every assertion in this file would still pass. A camelCase vendor name
    against a snake_case column is the first thing that tells the two apart, and it is what
    a sixth candle field would actually look like.
    """
    monkeypatch.setitem(journal._BARS_CANDLE_MAP, "vwapPrice", "vwap")
    paths_for_bars = journal.extra_paths(journal.BARS_SURFACE)
    assert paths_for_bars["vwap"] == journal.ExtraPath(None, "vwapPrice")
    assert "vwapPrice" not in paths_for_bars


def test_the_fetch_provenance_bars_carry_records_what_the_request_asked_for():
    """The three columns that make a ``freq=1m`` partition's meaning readable off its rows.

    ``schema_version`` cannot catch a change to a fetch parameter, because it reads columns
    and types and a parameter is neither. These three put the parameter on the row.
    """
    assert [name for name, _ in journal._BARS_FETCH_FIELDS] == [
        "window_start",
        "window_end",
        "extended_hours",
    ]
    assert journal.BARS_SCHEMA.field("extended_hours").type == pa.bool_()
    # The other flag the seam takes. It adds a field outside ``candles`` and changes no
    # candle, so it cannot make a partition mean something different.
    assert "previous_close" not in journal.BARS_SCHEMA.names


def test_bars_carry_no_column_for_a_field_outside_the_candle_list():
    """``symbol``, ``empty`` and the previous close are response-level, not per-candle."""
    for column in ("symbol", "empty", "previous_close", "previous_close_date"):
        assert column not in journal.BARS_SCHEMA.names, column


def test_chains_suspect_flag_rides_every_row():
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        suspect=True,
    )
    assert batch.column("suspect").to_pylist() == [True, True]


# -- quotes data rows --------------------------------------------------------


def test_quotes_data_batch_maps_prices_and_consumes_quote_time():
    batch = journal.quotes_data_batch(
        QUOTE, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    assert batch.schema == journal.QUOTES_SCHEMA
    assert batch.num_rows == 1
    row = batch.to_pylist()[0]
    assert row["bid"] == 649.98
    assert row["ask"] == 650.02
    assert row["last"] == 650.0
    # The rest of the quote block lands in its typed columns.
    assert row["bid_size"] == 5
    assert row["ask_size"] == 7
    assert row["last_size"] == 3
    assert row["bid_mic_id"] == "XNYS"
    assert row["ask_mic_id"] == "XNAS"
    assert row["last_mic_id"] == "XNYS"
    assert row["bid_time"] == 1787000099000
    assert row["ask_time"] == 1787000099500
    assert row["trade_time"] == 1787000098000
    assert row["high_price"] == 655.0
    assert row["low_price"] == 645.0
    assert row["open_price"] == 648.0
    assert row["close_price"] == 649.0
    assert row["mark"] == 650.0
    assert row["mark_change"] == 1.0
    assert row["mark_percent_change"] == 0.15
    assert row["net_change"] == 1.2
    assert row["net_percent_change"] == 0.18
    assert row["post_market_change"] == 0.3
    assert row["post_market_percent_change"] == 0.05
    assert row["total_volume"] == 90000000
    assert row["volatility"] == 12.5
    assert row["security_status"] == "Normal"
    # The quote block's 52-week fields keep distinct columns from fundamental's high_52 /
    # low_52, so both blocks' values survive the shared concept.
    assert row["week_52_high"] == 705.0
    assert row["week_52_low"] == 495.0
    assert row["realtime"] is True
    assert row["ticker"] == "SPY"
    assert row["vendor_quote_ts"] == VENDOR
    assert row["row_kind"] == journal.ROW_KIND_DATA
    # quoteTime is consumed into vendor_quote_ts, not repeated as a column.
    assert "quote_time" not in row
    # The CUSIP lands in its typed column, kept raw for the deferred FIGI backfill.
    assert row["cusip"] == "111111111"
    # The full fundamental block lands in its typed columns.
    assert row["div_pay_amount"] == 1.75
    assert row["div_ex_date"] == "2026-09-18"
    assert row["div_amount"] == 7.0
    assert row["div_freq"] == 4
    assert row["declaration_date"] == "2026-08-15"
    assert row["next_div_ex_date"] == "2026-12-18"
    assert row["next_div_pay_date"] == "2026-12-31"
    assert row["div_pay_date"] == "2026-09-30"
    assert row["div_yield"] == 1.28
    assert row["pe_ratio"] == 24.5
    assert row["eps"] == 22.3
    assert row["high_52"] == 700.0
    assert row["low_52"] == 500.0
    assert row["avg_10_days_volume"] == 74000000.0
    assert row["avg_1_year_volume"] == 80000000.0
    assert row["last_earnings_date"] == "2026-07-30"
    assert row["fund_leverage_factor"] == 1.0
    assert row["shares_outstanding"] == 900000000
    # The regular block lands in its typed columns.
    assert row["regular_market_last_price"] == 649.5
    assert row["regular_market_last_size"] == 100
    assert row["regular_market_net_change"] == 1.2
    assert row["regular_market_percent_change"] == 0.18
    assert row["regular_market_trade_time"] == 1787000100000
    # The extended block lands in its distinctly-prefixed columns.
    assert row["extended_last_price"] == 651.0
    assert row["extended_bid_price"] == 650.9
    assert row["extended_ask_price"] == 651.1
    assert row["extended_bid_size"] == 5
    assert row["extended_ask_size"] == 7
    assert row["extended_last_size"] == 3
    assert row["extended_mark"] == 651.0
    assert row["extended_quote_time"] == 1787000200000
    assert row["extended_trade_time"] == 1787000200500
    assert row["extended_total_volume"] == 2000
    # The colliding name lands in both blocks' own columns, never overwriting: the quote
    # block's lastPrice in ``last``, the extended block's in ``extended_last_price``.
    assert (row["last"], row["extended_last_price"]) == (650.0, 651.0)
    # Every field is recognized, so the namespaced overflow stays empty. Envelope-level
    # noise like assetMainType and the reference block beyond the CUSIP are not captured
    # and do not overflow either.
    assert row["extra"] is None


def test_quotes_realtime_lands_in_the_column_and_not_in_extra():
    # A payload whose only extra-looking field is the now-recognized realtime flag
    # keeps an empty overflow.
    envelope = {"realtime": True, "quote": {"bidPrice": 1.0, "askPrice": 1.1, "lastPrice": 1.05}}
    batch = journal.quotes_data_batch(
        envelope,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    )
    row = batch.to_pylist()[0]
    assert row["realtime"] is True
    assert row["extra"] is None


def test_quotes_unknown_field_overflows_namespaced_by_block():
    # An unrecognized field in a captured block overflows into extra under that block's
    # key, so drift in any block surfaces without colliding with another block's names.
    import copy

    envelope = copy.deepcopy(QUOTE)
    envelope["quote"]["brandNewQuoteField"] = 1.5
    envelope["extended"]["someNewField"] = 42
    batch = journal.quotes_data_batch(
        envelope, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    extra = json.loads(batch.to_pylist()[0]["extra"])
    assert extra == {"quote": {"brandNewQuoteField": 1.5}, "extended": {"someNewField": 42}}


# -- gap rows ----------------------------------------------------------------


def test_chains_gap_row_nulls_every_vendor_column():
    batch = journal.gap_batch(
        journal.CHAINS_SURFACE, ticker="SPY", snap_ts=SNAP, error_class="daemon_dead"
    )
    assert batch.schema == journal.CHAINS_SCHEMA
    assert batch.num_rows == 1
    row = batch.to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["error_class"] == "daemon_dead"
    assert row["snap_ts"] == SNAP
    assert row["ticker"] == "SPY"
    assert row["vendor_quote_ts"] is None
    assert row["fetch_ts"] is None
    # Every vendor column is null on a gap, the entitlement flag and the new volume
    # column included. It is a vendor column, not the provenance suspect bool.
    for column in (
        "bid",
        "ask",
        "last",
        "open_interest",
        "volume",
        "delta",
        "underlying_price",
        "is_delayed",
        "extra",
    ):
        assert row[column] is None, column


def test_quotes_gap_row_carries_reason_and_optional_fetch():
    batch = journal.gap_batch(
        journal.QUOTES_SURFACE,
        ticker="QQQ",
        snap_ts=SNAP,
        error_class="quote_sampler_dead",
        fetch_ts=FETCH,
    )
    row = batch.to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["error_class"] == "quote_sampler_dead"
    assert row["fetch_ts"] == FETCH
    assert row["bid"] is None and row["ask"] is None and row["last"] is None
    # The entitlement flag, the CUSIP, and the fundamental, regular, and extended blocks
    # are vendor columns, so all are null on a gap.
    assert row["realtime"] is None
    assert row["cusip"] is None
    for column in (
        "div_pay_amount",
        "div_amount",
        "div_freq",
        "div_ex_date",
        "next_div_pay_date",
        "div_yield",
        "pe_ratio",
        "avg_1_year_volume",
        "regular_market_last_price",
        "regular_market_last_size",
        "regular_market_trade_time",
        "extended_last_price",
        "extended_bid_size",
        "extended_total_volume",
        "extended_quote_time",
    ):
        assert row[column] is None, column


def test_gap_row_can_carry_a_close_tag_for_an_absent_marker():
    # The close+5 guard writes a spot_close absent-marker as a gap row.
    batch = journal.gap_batch(
        journal.CHAINS_SURFACE,
        ticker="SPY",
        snap_ts=SNAP,
        error_class="spot_close_unobserved",
        close_tag="spot_close",
    )
    assert batch.to_pylist()[0]["close_tag"] == "spot_close"


# -- timestamp normalization -------------------------------------------------


def test_datetime_timestamps_are_iso_formatted():
    fetched = datetime(2026, 8, 24, 20, 15, tzinfo=UTC)
    batch = journal.quotes_data_batch(
        QUOTE, ticker="SPY", snap_ts=fetched, fetch_ts=fetched, vendor_quote_ts=fetched
    )
    assert batch.to_pylist()[0]["fetch_ts"] == fetched.isoformat()


def test_fetch_end_ts_is_stored_when_given_and_null_when_omitted():
    # Supplied on a chains data row: it lands in the typed column verbatim.
    chains = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        fetch_end_ts=FETCH_END,
    )
    assert chains.to_pylist()[0]["fetch_end_ts"] == FETCH_END

    # Omitted on a quotes data row: the row is still valid, with a null request-end.
    quotes = journal.quotes_data_batch(
        QUOTE, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    assert quotes.to_pylist()[0]["fetch_end_ts"] is None

    # A gap row carries fetch_end_ts too, so a failed fetch's duration is measurable.
    gap = journal.gap_batch(
        journal.CHAINS_SURFACE,
        ticker="SPY",
        snap_ts=SNAP,
        error_class="vendor_error",
        fetch_ts=FETCH,
        fetch_end_ts=FETCH_END,
    )
    assert gap.to_pylist()[0]["fetch_end_ts"] == FETCH_END


# -- a value its column refuses never lands coerced ---------------------------

# Every field name below is a vendor key whose value the pinned column will not take. The
# chain and the quote surface each get one, so the rule is checked on both row builders
# rather than one. What must never happen is the value landing changed. Where it goes
# instead, its column null and the raw value in ``extra``, is checked in the routing
# section further down.


def _chain_of(*contracts):
    """A chain body carrying the given contracts under one expiration and strike."""
    return dict(
        CHAIN_BODY,
        callExpDateMap={"2026-09-18:25": {"650.0": list(contracts)}},
        putExpDateMap={},
    )


def _chain_row(**overrides):
    """The one row a chain body carrying a single overridden contract builds."""
    body = _chain_of(_full_contract(**overrides))
    return journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[
        0
    ]


def test_a_fractional_float_in_an_integer_chain_column_routes_instead_of_truncating():
    """A vendor ``openInterest`` of 1234.7 must not land as 1234.

    Building the column straight from Python objects truncated it and returned the
    truncated value with no error. Nothing downstream could tell 1234 apart from a real
    1234, which is why this one shape was the silent member of the corruption class.

    The column is null and the vendor's own 1234.7 is in ``extra``, so the minute lands
    and the value survives to be read back.
    """
    row = _chain_row(openInterest=1234.7)
    assert row["open_interest"] is None
    assert json.loads(row["extra"]) == {"openInterest": 1234.7}


def test_a_fractional_float_in_an_integer_quote_column_routes_instead_of_truncating():
    """The same rule on the quotes surface, whose 18 integer columns take one route."""
    envelope = dict(QUOTE, quote=dict(QUOTE["quote"], totalVolume=88_888_888.5))
    row = journal.quotes_data_batch(
        envelope, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    ).to_pylist()[0]
    assert row["total_volume"] is None
    assert json.loads(row["extra"]) == {"quote": {"totalVolume": 88_888_888.5}}


def test_a_lossless_float_still_lands_in_an_integer_column():
    """A whole-numbered float carries no lost digit, so it lands as the integer it is.

    Schwab serializes some counts as JSON floats. Refusing those would gap a chain over a
    value that round-trips exactly, so the check rejects only a float it cannot represent.
    """
    body = dict(
        CHAIN_BODY,
        callExpDateMap={
            "2026-09-18:25": {"650.0": [_full_contract(openInterest=1500.0, bidSize=0.0)]}
        },
        putExpDateMap={},
    )
    row = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[0]
    assert row["open_interest"] == 1500
    assert row["bid_size"] == 0


def test_an_all_null_integer_column_still_lands_typed_int64():
    """An absent vendor field leaves the column null, and the null column stays int64.

    Inferring a column of nothing but nulls gives Arrow's ``null`` type, so the inferred
    array has to be cast back. Without that cast the batch would not match the pinned
    schema at all.
    """
    contract = _full_contract()
    for key in ("openInterest", "bidSize", "askSize", "lastSize", "totalVolume", "ssid"):
        contract.pop(key)
    body = dict(
        CHAIN_BODY,
        callExpDateMap={"2026-09-18:25": {"650.0": [contract]}},
        putExpDateMap={},
    )
    batch = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.schema == journal.CHAINS_SCHEMA
    assert batch.schema.field("open_interest").type == pa.int64()
    assert batch.column("open_interest").type == pa.int64()
    assert batch.to_pylist()[0]["open_interest"] is None


def test_the_other_three_types_route_every_wrong_shape_rather_than_coercing_it():
    """The rule covers every pinned type, not just the 29 integer columns.

    Each case below is a wrong shape in a ``double``, ``string``, or ``bool`` column. A
    test that only checked the integer columns would not notice the routing widening onto
    a path that already refused, or a coercion creeping into one of the other three types.
    """
    cases = (
        ("bid", "not-a-number", "bid"),  # string into double
        ("description", 7, "description"),  # int into string
        ("description", 7.5, "description"),  # float into string
        ("inTheMoney", 1, "in_the_money"),  # int into bool
        ("inTheMoney", 1.5, "in_the_money"),  # float into bool
    )
    for vendor_field, value, column in cases:
        row = _chain_row(**{vendor_field: value})
        assert row[column] is None, (vendor_field, value)
        assert json.loads(row["extra"]) == {vendor_field: value}


def test_a_bool_after_a_float_in_the_same_integer_column_never_lands_as_one():
    """Two contracts, the first a lossless float and the second a bool.

    Arrow reads a column's type from its first non-null value and widens the later ones
    into it, so the bool is already 1.0 by the time inference returns and the inferred
    type cannot tell it from a real 1. Checking the bool alone in the column misses this,
    because ``[True, 1500.0]`` raises during inference while ``[1500.0, True]`` does not.
    A vendor ``true`` recorded as an open interest of 1 is the same silent corruption the
    fractional float was, and routing must not reintroduce it.

    The contract that sent the bool loses its column and keeps its value in ``extra``. The
    contracts around it keep the integers they sent, so one drifted row costs one row.
    """
    for values in ((1500.0, True), (1500.0, False), (1500.0, True, 7)):
        contracts = [
            _full_contract(symbol=f"SPY   260918C0065000{i}", openInterest=value)
            for i, value in enumerate(values)
        ]
        batch = journal.chains_data_batch(
            _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
        )
        rows = batch.to_pylist()
        landed = batch.column("open_interest").to_pylist()
        expected = [None if isinstance(value, bool) else int(value) for value in values]
        assert landed == expected, values
        for row, value in zip(rows, values, strict=True):
            if isinstance(value, bool):
                assert json.loads(row["extra"]) == {"openInterest": value}
            else:
                assert row["extra"] is None


def test_the_routed_value_keeps_its_own_type_and_is_never_the_coercion():
    """``True`` routed as ``1`` would be the corruption this exists to prevent.

    Comparing values alone would pass on a ``1`` written where ``True`` arrived, since
    ``True == 1`` in Python. The type is what separates the vendor's boolean from the
    integer Arrow's cast would have manufactured, so the type is what this compares.
    """
    row = _chain_row(openInterest=True)
    routed = json.loads(row["extra"])["openInterest"]
    assert routed is True
    assert type(routed) is bool


def test_a_column_arrow_cannot_infer_keeps_the_direct_build_s_own_exception():
    """When inference itself fails, the caller must still see the direct build's class.

    Two values that are a bool and a string give Arrow nothing to infer, so the inference
    attempt raises before any type check runs. Its exception is not the one the direct
    build raises, and the two disagree in both directions: ``[True, "7"]`` fails inference
    with ``ArrowInvalid`` where the direct build raises ``ArrowTypeError``, and reversing
    them swaps the pair.

    The class matters past the raise. A column with no route into ``extra`` still gaps the
    cycle, and ``lake.capture`` records that gap under the exception's own name, so leaking
    the inference failure would rewrite a gap on disk from ``arrow_type_error`` to
    ``arrow_invalid`` and change what an operator reads. This asks ``typed_column``
    directly, because that is where the class is decided and where every route into it,
    the row build, the per-value scan, and the read-time projection, goes through.
    """
    for values, expected in (((True, "7"), pa.ArrowTypeError), (("7", True), pa.ArrowInvalid)):
        with pytest.raises(expected):
            journal.typed_column(pa.int64(), list(values))


def test_a_lossless_float_still_lands_when_the_column_holds_several_rows():
    """The control for the case above. Several float rows with no bool still capture.

    Without this, refusing every multi-row float column would pass the bool test while
    breaking the lossless-float rule the fix is built around.
    """
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{i}", openInterest=value)
        for i, value in enumerate((1500.0, 2, 0.0))
    ]
    body = dict(
        CHAIN_BODY,
        callExpDateMap={"2026-09-18:25": {"650.0": contracts}},
        putExpDateMap={},
    )
    batch = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("open_interest").to_pylist() == [1500, 2, 0]


def test_a_bool_or_a_string_in_an_integer_column_never_lands_as_a_number():
    """Arrow's cast would turn ``True`` into 1 and ``"7"`` into 7. Neither may land.

    Inferring the column type first opens a cast Arrow is willing to perform on shapes the
    direct build refused. Both stay refused, so neither the fractional-float fix nor the
    routing adds a silent conversion back.
    """
    for value in (True, "7"):
        row = _chain_row(openInterest=value)
        assert row["open_interest"] is None
        routed = json.loads(row["extra"])["openInterest"]
        assert routed == value and type(routed) is type(value)


def test_a_bool_in_a_double_column_never_lands_as_one_or_zero():
    """A vendor ``bid`` of ``true`` must not land as a one-dollar bid.

    Building a double column straight from Python objects accepts a bool and returns
    ``1.0`` or ``0.0`` with no error. Nothing downstream could tell that from a real
    one-dollar bid, which is why this was the silent member of the corruption class on the
    65 double columns, the way the fractional float was on the 29 integer ones.

    Both bools are checked. ``False`` landing as ``0.0`` is the same corruption, and a zero
    bid reads as an ordinary empty book, so it passes inspection just as readily.

    The null column is what catches the bug. The type-strict check on the routed value holds
    something else: that what reaches ``extra`` is the vendor's own ``True`` rather than the
    ``1.0`` the conversion would have made of it. ``1.0 == True`` is true in Python, so that
    second guarantee needs ``is`` to say anything at all.
    """
    for value in (True, False):
        row = _chain_row(bid=value)
        assert row["bid"] is None, value
        routed = json.loads(row["extra"])["bid"]
        assert routed is value
        assert type(routed) is bool


def test_a_bool_after_a_float_in_the_same_double_column_never_lands_as_a_price():
    """Two contracts, the first a real price and the second a bool.

    Arrow reads a column's type from its first non-null value and widens the later ones
    into it, so ``[True, 1500.0]`` raises during inference while ``[1500.0, True]`` does
    not. A test that offered the bool first would pass on a fix that read the inferred type
    and never scanned, which is the fix this column shape exists to rule out.

    The contract that sent the bool loses its column and keeps its value in ``extra``. The
    contracts around it keep the prices they sent, so one drifted row costs one row.
    """
    for values in ((1500.0, True), (1500.0, False), (1500.0, True, 2.5)):
        contracts = [
            _full_contract(symbol=f"SPY   260918C0065000{i}", bid=value)
            for i, value in enumerate(values)
        ]
        batch = journal.chains_data_batch(
            _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
        )
        rows = batch.to_pylist()
        landed = batch.column("bid").to_pylist()
        expected = [None if isinstance(value, bool) else value for value in values]
        assert landed == expected, values
        assert [type(cell) for cell in landed] == [type(cell) for cell in expected], values
        for row, value in zip(rows, values, strict=True):
            if isinstance(value, bool):
                routed = json.loads(row["extra"])["bid"]
                assert routed is value
            else:
                assert row["extra"] is None


def test_a_bool_in_a_double_quote_column_routes_the_same_way():
    """The same rule on the quotes surface, whose double columns take the same route.

    The issue's own reproduction is a quote envelope carrying ``"bidPrice": true``. This
    is that envelope, checked end to end through the row builder rather than through the
    column builder alone.
    """
    row = _quote_row("quote", bidPrice=True, askPrice=False)
    assert row["bid"] is None
    assert row["ask"] is None
    overflow = json.loads(row["extra"])["quote"]
    assert overflow["bidPrice"] is True
    assert overflow["askPrice"] is False


def test_a_bool_beside_a_string_in_a_double_column_is_routed_rather_than_kept():
    """The shape where the routing itself used to let the bool through.

    Two contracts, one sending ``true`` and one sending a string. The string refused the
    whole-column build, so the routing's per-value scan ran. That scan asks ``_fits``, which
    answered that the bool belonged in a double column, so only the string was nulled and
    the bool was rebuilt into the column as ``1.0``.

    That is worse than the plain case. The row carried a drift signature, a known field's
    name in ``extra``, which is the thing an operator pages on, and the value beside it was
    corrupted anyway. Both contracts must lose their column and keep their own value.
    """
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{i}", bid=value)
        for i, value in enumerate((True, "n/a"))
    ]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    assert batch.column("bid").to_pylist() == [None, None]
    routed = [json.loads(row["extra"])["bid"] for row in batch.to_pylist()]
    assert routed[0] is True
    assert routed[1] == "n/a"


def test_an_ordinary_all_float_double_column_is_unaffected():
    """The control. A column of real prices still lands, unchanged and un-routed.

    The scan added to the double route runs on every cycle, including this one, so a
    version of it that refused a legitimate price would gap every chain.

    Three of the five values are there to refuse a specific wrong scan.

    1. ``0.0`` and ``1.0`` are the floats a bool converts to, so a scan written against
       values rather than types would refuse exactly these.
    2. ``5`` is a plain integer, which is what a vendor sends for a whole-dollar price.
       ``bool`` is a subclass of ``int``, so a scan widened from ``bool`` to ``int`` looks
       like a harmless simplification and would refuse every integer price on the surface.
    """
    values = (1500.0, 0.0, 1.0, 5, 0.01)
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{i}", bid=value)
        for i, value in enumerate(values)
    ]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    landed = batch.column("bid").to_pylist()
    assert batch.column("bid").type == pa.float64()
    assert landed == [float(value) for value in values]
    # The integer lands as the float its column is, never refused and never left an integer.
    assert all(type(cell) is float for cell in landed)
    assert all(row["extra"] is None for row in batch.to_pylist())


def test_a_bool_is_refused_by_a_double_column_the_way_an_integer_column_refuses_it():
    """One shape, one answer, on both numeric types.

    The two column types disagreed before this. The same vendor ``true`` was refused in
    ``total_volume``, an ``int64`` column, and accepted in ``bid``, a ``double`` one. The
    check runs against ``typed_column`` directly, because that is the one place the answer
    is decided, and the three routes that ask it, the row build, the per-value scan behind
    the routing, and the read-time projection, all inherit whatever it says.

    The exception class is checked rather than only the refusal. ``lake.capture`` records
    a gap under the exception's own name, so a double column refusing under a different
    class than the integer column would write two names on disk for one shape.
    """
    for field_type in (pa.int64(), pa.float64()):
        for values in ([True], [False], [1500.0, True]):
            with pytest.raises(pa.ArrowTypeError):
                journal.typed_column(field_type, list(values))
            assert journal._fits(field_type, values[-1]) is False, (field_type, values)


# -- a refused known field is routed into extra -------------------------------

# Where a value its column refuses actually goes. The section above pins that it never
# lands coerced. These pin that it lands in ``extra``, under the key the reader looks for,
# with the vendor's own value, and that a column with no key to land under still costs the
# cycle the way it always did.


def _quote_with(block: str, **overrides):
    """The quote envelope with one captured block's fields overridden."""
    return dict(QUOTE, **{block: dict(QUOTE[block], **overrides)})


def _quote_row(block: str, **overrides):
    """The one row the quote envelope builds with one block's fields overridden."""
    return journal.quotes_data_batch(
        _quote_with(block, **overrides),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    ).to_pylist()[0]


def _known_names_in(surface: str, extra: str | None) -> set[journal.ExtraPath]:
    """Every path in a row's overflow that names a column of the running schema.

    This is the signature read off a row, derived rather than restated: the paths come
    from ``journal.extra_paths``, the same mapping the reader uses to find a value, and a
    path is counted only when the overflow actually carries a value at it.
    """
    overflow = json.loads(extra) if extra else {}
    found = set()
    for path in journal.extra_paths(surface).values():
        block = overflow if path.block is None else overflow.get(path.block) or {}
        if isinstance(block, dict) and path.field in block:
            found.add(path)
    return found


def test_a_known_fields_name_in_extra_is_the_signature_that_its_column_refused_a_value():
    """The invariant the read-time refusal in marketlake #149 detects on.

    ``_extra_json`` and the quotes projection both build the overflow from the fields their
    maps do *not* name, so a known vendor field's name can never reach ``extra`` any other
    way. That makes its presence self-describing, with no marker and no new machinery.

    It is what the read-time refusal keys on. Without it a routed null and a genuine vendor
    null look identical, and an operator would reasonably read the column as a field the
    vendor stopped sending.

    Both directions are checked, because only one of them would pass on a writer that put
    a known name in ``extra`` on every row.
    """
    # Steady state, with an unrecognised field present on both surfaces so the overflow is
    # populated. No known field's name is in it.
    drifted = _full_contract(brandNewGreek=1.5)
    row = journal.chains_data_batch(
        _chain_of(drifted), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    ).to_pylist()[0]
    assert json.loads(row["extra"]) == {"brandNewGreek": 1.5}
    assert _known_names_in(journal.CHAINS_SURFACE, row["extra"]) == set()

    quote_row = _quote_row("quote", brandNewStat=1.5)
    assert json.loads(quote_row["extra"]) == {"quote": {"brandNewStat": 1.5}}
    assert _known_names_in(journal.QUOTES_SURFACE, quote_row["extra"]) == set()

    # A retype, and the signature is exactly the one field that drifted.
    retyped = _chain_row(openInterest="1234")
    assert _known_names_in(journal.CHAINS_SURFACE, retyped["extra"]) == {
        journal.ExtraPath(None, "openInterest")
    }
    retyped_quote = _quote_row("quote", totalVolume="90000000")
    assert _known_names_in(journal.QUOTES_SURFACE, retyped_quote["extra"]) == {
        journal.ExtraPath("quote", "totalVolume")
    }


def test_the_reader_finds_a_routed_value_at_the_path_extra_paths_names():
    """The writer and the reader agree on where a routed value sits, by derivation.

    ``extra_paths`` is the parser's vendor maps read backwards, and it is what
    ``extra_projection`` looks a value up through. The routing writes under that same
    mapping rather than a second rule of its own, so a key it writes is a key the reader
    already knows how to reach. Walking the mapping here is what makes this cover the
    class rather than the two fields it happens to drift.
    """
    for surface, column, vendor_value, row in (
        (journal.CHAINS_SURFACE, "open_interest", "1234", _chain_row(openInterest="1234")),
        (journal.QUOTES_SURFACE, "bid", "649.98", _quote_row("quote", bidPrice="649.98")),
        (
            journal.QUOTES_SURFACE,
            "extended_last_price",
            "651.0",
            _quote_row("extended", lastPrice="651.0"),
        ),
    ):
        path = journal.extra_paths(surface)[column]
        overflow = json.loads(row["extra"])
        held = overflow if path.block is None else overflow[path.block]
        assert held[path.field] == vendor_value
        assert row[column] is None


def test_two_blocks_sharing_a_field_name_route_into_their_own_block():
    """``quote.lastPrice`` and ``extended.lastPrice`` are one name and two columns.

    A flat overflow would make a retype of either read as a retype of both, and the reader
    would fill the wrong column. The nesting is the whole reason the quotes overflow is
    block-keyed, so the routing has to keep it.
    """
    row = _quote_row("extended", lastPrice="651.0")
    assert row["extended_last_price"] is None
    # The quote block's own lastPrice is untouched, in its own column.
    assert row["last"] == 650.0
    assert json.loads(row["extra"]) == {"extended": {"lastPrice": "651.0"}}


def test_a_routed_value_joins_the_overflow_rather_than_replacing_it():
    """A contract that drifted and also carries a new field keeps both in ``extra``.

    Serializing the routed value over the top would throw away the unrecognised field the
    fail-open was built to keep, which is the same loss in the other direction.
    """
    row = _chain_row(openInterest=1234.7, brandNewGreek=1.5)
    assert json.loads(row["extra"]) == {"openInterest": 1234.7, "brandNewGreek": 1.5}


def test_a_routed_value_is_the_vendors_own_and_keeps_its_shape_through_the_json():
    """Nothing is cast, rounded, or stringified on the way into ``extra``.

    A cast would manufacture a value the vendor never sent and hand it to a downstream
    computation with no marker, which is the one outcome nobody can detect afterwards. The
    nested value is the case that would break a writer that stringified, since a dict has
    no sensible string form to fall back on.
    """
    for value in (1234.7, "1234", True, {"amount": 3, "unit": "contracts"}, [1, 2, 3], -0.5):
        row = _chain_row(openInterest=value)
        routed = json.loads(row["extra"])["openInterest"]
        assert routed == value and type(routed) is type(value), value


def test_only_the_contract_that_drifted_loses_its_column():
    """One drifted row costs one row's field, not the batch's.

    Nulling the whole column would turn a narrow vendor change into a wide one, and the
    other contracts' open interest is data the vendor did send.
    """
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{index}", openInterest=value)
        for index, value in enumerate((1234, "1235", 1236))
    ]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    assert batch.column("open_interest").to_pylist() == [1234, None, 1236]
    assert [json.loads(e) if e else None for e in batch.column("extra").to_pylist()] == [
        None,
        {"openInterest": "1235"},
        None,
    ]


def test_a_string_arrow_cannot_encode_routes_too():
    """A lone surrogate is a string the column refuses, and it reaches a different family.

    Arrow raises ``UnicodeEncodeError`` rather than one of its own errors here, so a
    routing that named only the Arrow families would still cost the cycle for it. JSON
    carries the surrogate through, so the value survives where the column cannot hold it.
    """
    row = _chain_row(description="\ud800")
    assert row["description"] is None
    assert json.loads(row["extra"]) == {"description": "\ud800"}


def test_a_column_with_no_route_into_extra_still_costs_the_cycle():
    """What still fails loudly, one case per group that has no key to land under.

    A value with nowhere honest to go must not be parked somewhere nothing reads. Each
    group below is a column ``extra_paths`` deliberately leaves out, named in its
    docstring, so this covers the reasons rather than one example of one.

    Neither case is a shape a JSON decode produces, which is the point of both. A value
    only this code could have put there failing loudly is this code's bug surfacing as
    this code's bug. ``vendor_quote_ts`` used to stand here as the second case, on a
    non-numeric quote time. It routes now, per marketlake #223, because that value is the
    vendor's own and a minute is worth more than a stamp.
    """
    # A column this module fills itself. A stamp that will not build is this code's bug,
    # not the vendor's, and parking it would hide the bug and keep writing rows.
    with pytest.raises(pa.ArrowTypeError):
        journal.gap_batch(journal.CHAINS_SURFACE, ticker=7, snap_ts=SNAP, error_class="http_429")
    # A column whose value this module transforms rather than copies. The JSON encoding
    # refuses first, before any column is built.
    with pytest.raises(TypeError):
        journal.chains_data_batch(
            _chain_of(_full_contract(optionDeliverablesList={"SPY"})),
            ticker="SPY",
            snap_ts=SNAP,
            fetch_ts=FETCH,
        )


# -- a vendor quote time the transform refuses --------------------------------

# The quote time is the one vendor field neither surface copies into a column. Both
# transform it into ``vendor_quote_ts`` and hold it out of the overflow as consumed. A
# value the transform refuses consumed nothing, so the exclusion stops applying for that
# row: the stamp lands null and the vendor's own value overflows under its own name. What
# these pin is that the refusal is decided by whether the value converts, never by its
# Python type, because a numeric string converts today and the lake captures those.


# Every shape the transform must refuse, one per reason. The bool pair is the silent one,
# where ``float(True)`` makes a plausible 1970 stamp with nothing raised. The rest raised
# out of the row builder before this, which cost the whole minute.
REFUSED_EPOCHS = (True, False, "n/a", "", " ", {"a": 1}, [1], "inf", "nan", 1e30)

# Every shape the transform must keep converting. The numeric strings are the trap: they
# convert correctly today, so a fix that refused anything not an ``int`` or a ``float``
# would drop values the lake captures right now.
ACCEPTED_EPOCHS = (
    "1758000000000",
    " 1758000000000 ",
    "1758000000000.0",
    "1.758e12",
    1758000000000.0,
    1758000000000,
)

# What all four of those name, as the stamp each must produce.
ACCEPTED_ISO = "2025-09-16T05:20:00+00:00"


def test_a_bool_quote_time_nulls_the_chains_stamp_and_routes_the_vendors_own_value():
    """A vendor ``"quoteTimeInLong": true`` must not land as a 1970 timestamp.

    ``float(True)`` is ``1.0``, so the conversion turned a bool into one millisecond past
    the epoch and handed back a well-formed ISO string. Nothing raised and ``extra`` stayed
    empty, so the row read as clean while the vendor's own value was gone. That is the
    silent member of this class, the way the fractional float was on the integer columns
    and the bool was on the double ones.

    ``False`` is checked beside ``True`` because it lands on the epoch itself, which reads
    as an ordinary stamp just as readily.

    The routed value is checked with ``is`` rather than ``==``. ``1.0 == True`` is true in
    Python, so an equality check alone would pass on a fix that parked the conversion's
    output instead of the vendor's own bool. That equality is what hid the original shape in
    marketlake #127.
    """
    for value in (True, False):
        row = _chain_row(quoteTimeInLong=value)
        assert row["vendor_quote_ts"] is None, value
        routed = json.loads(row["extra"])["quoteTimeInLong"]
        assert routed is value
        assert type(routed) is bool


def test_a_bool_quote_time_routes_under_its_block_on_the_quotes_surface():
    """The same rule on the quotes surface, where the field arrives as ``quote.quoteTime``.

    The two sites are one transform written twice, so a fix to one alone leaves the other
    corrupting rows. Here the overflow nests under the block the field came from, which is
    how a quotes row keeps two blocks' same-named fields apart.

    This covers the journal's half, the routing. The stamp is the caller's argument on this
    surface, computed in ``lake.capture``, and both halves are read off disk together in
    the cycle test in tests/component/test_capture_cycle.py.
    """
    for value in (True, False):
        row = _quote_row("quote", quoteTime=value)
        routed = json.loads(row["extra"])["quote"]["quoteTime"]
        assert routed is value
        assert type(routed) is bool


def test_every_refused_quote_time_routes_rather_than_costing_the_minute():
    """A string, an object, a list, an infinity and a NaN all land the row.

    Each of these used to raise out of the row builder, before any column of the batch
    existed. The raise reached the cycle's fail-open and gapped the whole ticker, so one
    bad field on one contract cost every contract on the chain. A minute is unrecoverable,
    which makes that the worst outcome available for a single field.

    They are driven together with the bools on purpose. Giving one vendor mistake two
    answers, a silent 1970 stamp for a bool and a lost minute for a string, is the
    one-shape-two-answers complaint marketlake #132 was filed over.

    The infinity and the NaN arrive as strings because that is how a JSON body carries
    them. ``1e30`` is the finite shape that still names no instant a platform clock holds.
    """
    for value in REFUSED_EPOCHS:
        row = _chain_row(quoteTimeInLong=value)
        assert row["vendor_quote_ts"] is None, value
        assert json.loads(row["extra"]) == {"quoteTimeInLong": value}, value

        quote_row = _quote_row("quote", quoteTime=value)
        assert json.loads(quote_row["extra"])["quote"] == {"quoteTime": value}, value


def test_a_numeric_string_quote_time_still_converts_on_both_surfaces():
    """The regression guard. A value the lake captures correctly today must keep landing.

    ``float("1758000000000")`` is the epoch the vendor meant, and ``float`` strips the
    padding of a value sent with surrounding spaces, so both convert and both are real
    captures rather than hypotheticals. The rule is refuse what converts wrongly or not at
    all, never refuse what is not a number type, and this is what separates the two.

    A type check is the fix that fails here, and it is the natural one to reach for, since
    the bool has to be excluded by name anyway. ``isinstance(True, int)`` is true in Python,
    so the guard against the bool cannot be a type check and must not become one.
    """
    for value in ACCEPTED_EPOCHS:
        row = _chain_row(quoteTimeInLong=value)
        assert row["vendor_quote_ts"] == ACCEPTED_ISO, value
        assert row["extra"] is None, value

        quote_row = _quote_row("quote", quoteTime=value)
        assert quote_row["extra"] is None, value


def test_a_quote_time_the_vendor_did_not_send_stays_absent_rather_than_refused():
    """Absent and refused have to stay distinguishable, which is why the routing exists.

    A null ``vendor_quote_ts`` means the vendor sent no quote time, and that reading is
    unambiguous only while nothing else lands null there. A refused value that landed a
    bare null would open a second meaning with nothing to tell the two apart, which is the
    reason the value is routed rather than dropped.

    Both absences are driven: the field missing from the payload, and the vendor sending an
    explicit JSON null. Neither is a mistake the vendor made, so neither leaves a signature.
    """
    contract = _full_contract()
    del contract["quoteTimeInLong"]
    row = journal.chains_data_batch(
        _chain_of(contract), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    ).to_pylist()[0]
    assert row["vendor_quote_ts"] is None
    assert row["extra"] is None

    explicit_null = _chain_row(quoteTimeInLong=None)
    assert explicit_null["vendor_quote_ts"] is None
    assert explicit_null["extra"] is None

    block = dict(QUOTE["quote"])
    del block["quoteTime"]
    envelope = dict(QUOTE, quote=block)
    quote_row = journal.quotes_data_batch(
        envelope, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=None
    ).to_pylist()[0]
    assert quote_row["vendor_quote_ts"] is None
    assert quote_row["extra"] is None

    assert _quote_row("quote", quoteTime=None)["extra"] is None


def test_a_refused_quote_time_leaves_the_rest_of_the_row_alone():
    """One refused field costs that field, and the contract beside it costs nothing.

    The routing changes which fields count as consumed for one row. Reading that decision
    off module state rather than a per-row copy would spread one contract's drift across
    every contract in the batch, and the second contract here is what catches it.
    """
    drifted = _full_contract(symbol="SPY   260918C00650000", quoteTimeInLong=True)
    clean = _full_contract(symbol="SPY   260918P00650000", quoteTimeInLong=PUT_QUOTE_TIME_MS)
    rows = journal.chains_data_batch(
        _chain_of(drifted, clean), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    ).to_pylist()

    assert rows[0]["vendor_quote_ts"] is None
    assert json.loads(rows[0]["extra"]) == {"quoteTimeInLong": True}
    # Every other column the drifted contract sent still landed.
    assert rows[0]["bid"] == 4.2
    assert rows[0]["open_interest"] == 1234

    assert rows[1]["vendor_quote_ts"] == PUT_VQT
    assert rows[1]["extra"] is None


def test_the_refusal_is_signalled_locally_and_never_through_unfit_errors():
    """``UNFIT_ERRORS`` is not this path's vocabulary and must not become it.

    That tuple is read in exactly two places, ``_fits`` and ``_routed_column``, and both
    run inside the column build. The epoch transform runs in the row builder, before any
    column of that batch exists, so nothing on this path would consult the tuple whatever
    it held. ``OverflowError`` being in it already is what makes that easy to assume and
    worth checking by execution.

    marketlake #164 is open against the tuple for the temporal-column case, which is a
    different seam. Widening it here would answer that issue by accident.
    """
    assert journal.UnfitEpochError not in journal.UNFIT_ERRORS
    assert not issubclass(journal.UnfitEpochError, journal.UNFIT_ERRORS)
    # The transform raises before any column build, which is why the tuple cannot reach it.
    with pytest.raises(journal.UnfitEpochError):
        journal.epoch_ms_to_utc("n/a")
    # And an epoch that raised OverflowError, the one member that does overlap, is refused
    # by the transform rather than by anything the tuple governs.
    assert _chain_row(quoteTimeInLong="inf")["vendor_quote_ts"] is None


def test_the_demotion_belongs_to_the_quote_block_and_not_to_every_block_named_quote_time():
    """``extended`` carries a ``quoteTime`` too, and it is an ordinary column there.

    The field name is shared. ``_EXTENDED_MAP`` maps ``quoteTime`` to
    ``extended_quote_time``, a column of its own, while the ``quote`` block's is consumed
    into the stamp. So the demotion has to key on the block's consumed set rather than on
    the field's name, or the extended column is written and overflowed at once.

    The value here is an epoch far outside any clock's range, chosen because it is the one
    shape that separates the two blocks. The int64 column takes it happily, and the epoch
    transform refuses it, so a demotion that ignored the consumed set would leave the value
    in its column and put its name in ``extra`` beside it. That name is the signature a
    reader takes to mean the column refused the row, and here the column refused nothing.
    """
    row = _quote_row("extended", quoteTime=10**18)
    assert row["extended_quote_time"] == 10**18
    assert row["extra"] is None

    # The same value in the quote block, where the demotion does belong.
    quote_row = _quote_row("quote", quoteTime=10**18)
    assert json.loads(quote_row["extra"])["quote"] == {"quoteTime": 10**18}


def test_a_quote_time_keeps_the_milliseconds_the_vendor_sent():
    """The stamp is built from an epoch in milliseconds, so the milliseconds have to survive.

    Every other epoch in this file ends in three zeros, which is what a hand-written fixture
    looks like and not what Schwab sends. A stamp that floored to the second would read as
    correct against all of them, and ``vendor_quote_ts`` is a string column, so the schema
    checks nothing either. Staleness is measured per row off this stamp, so the lost digits
    would be lost from a measurement rather than a label.
    """
    row = _chain_row(quoteTimeInLong=1758000000123)
    assert row["vendor_quote_ts"] == "2025-09-16T05:20:00.123000+00:00"

    # The shared transform, which the quotes surface reaches through ``lake.capture``.
    assert journal.epoch_ms_to_utc(1758000000123).isoformat() == "2025-09-16T05:20:00.123000+00:00"


def test_a_zero_epoch_converts_and_stays_apart_from_the_bool_that_equals_it():
    """``0`` is a timestamp and ``False`` is not, which is the whole shape of the guard.

    ``False == 0`` is true in Python, so the two are one value to an equality check. The
    guard excludes the bool by type and leaves every number alone, which is what makes the
    rule refuse what converts wrongly rather than what is not a number type. Driving the
    pair together is the only way to see that the guard reads the type and not the value.

    A zero epoch is a common vendor sentinel for unset, so which side of the line it falls
    on is a real decision. It converts, because it names a real instant and the transform
    is not in the business of judging whether the vendor meant it. A negative epoch names an
    instant before 1970 and converts for the same reason.
    """
    zero = _chain_row(quoteTimeInLong=0)
    assert zero["vendor_quote_ts"] == "1970-01-01T00:00:00+00:00"
    assert zero["extra"] is None

    false = _chain_row(quoteTimeInLong=False)
    assert false["vendor_quote_ts"] is None
    assert json.loads(false["extra"]) == {"quoteTimeInLong": False}

    negative = _chain_row(quoteTimeInLong=-1000)
    assert negative["vendor_quote_ts"] == "1969-12-31T23:59:59+00:00"
    assert negative["extra"] is None


# -- the levels outside the contract dict and the captured blocks --------------

# The six vendor fields that arrive above the level each surface's overflow was built
# from: the four chain-level body fields repeated onto every contract row, and the two
# read off the quotes envelope. Each is a verbatim copy of a vendor value, so a retype of
# one is the vendor's doing and has to route like any other rather than costing the cycle.


def _chain_row_with_header(**header):
    """The one row a chain body builds with chain-level fields overridden."""
    body = dict(_chain_of(_full_contract()), **header)
    return journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[
        0
    ]


def _quote_row_with_envelope(**envelope):
    """The one row a quote envelope builds with envelope-level fields overridden."""
    return journal.quotes_data_batch(
        dict(QUOTE, **envelope),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    ).to_pylist()[0]


@pytest.mark.parametrize(
    ("vendor", "column", "value"),
    [
        ("interestRate", "interest_rate", "four and a quarter"),
        ("underlyingPrice", "underlying_price", "six hundred and fifty"),
        ("dividendYield", "dividend_yield", "one point two eight"),
        ("isDelayed", "is_delayed", "no"),
    ],
)
def test_every_verbatim_chain_level_field_routes_under_the_chain_block(vendor, column, value):
    """All four, not just the one the design reads.

    Each is read off the top of the chain body and copied onto every contract row, so a
    retype of one used to cost the whole chain for as long as the vendor held the shape and
    threw the raw value away on top. ``underlyingPrice`` is the one the design's IV
    inversion reads and ``isDelayed`` is the entitlement flag, but the loss is the same
    shape for all four, so all four are driven rather than one standing in for the rest.
    """
    row = _chain_row_with_header(**{vendor: value})

    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row[column] is None
    assert json.loads(row["extra"]) == {"chain": {vendor: value}}
    assert _known_names_in(journal.CHAINS_SURFACE, row["extra"]) == {
        journal.ExtraPath("chain", vendor)
    }
    # One drifted chain-level field costs that field alone, not the row's contract.
    assert (row["bid"], row["open_interest"]) == (4.2, 1234)


@pytest.mark.parametrize(
    ("envelope", "column", "vendor", "value"),
    [
        ({"realtime": "yes"}, "realtime", "realtime", "yes"),
        ({"cusip": 111111111}, "cusip", "cusip", 111111111),
        ({"reference": {"cusip": 111111111}}, "cusip", "cusip", 111111111),
    ],
)
def test_a_retyped_envelope_field_routes_under_the_envelope_block(envelope, column, vendor, value):
    """The quotes surface's other two verbatim fields, which sit outside every block.

    ``realtime`` is the entitlement flag the validation battery checks. The CUSIP arrives
    either at the top of the envelope or inside ``reference``, and both route under the
    field's own name, because the overflow key is what the vendor calls the value rather
    than where on the payload it sat.
    """
    row = _quote_row_with_envelope(**envelope)

    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row[column] is None
    assert json.loads(row["extra"]) == {"envelope": {vendor: value}}
    assert _known_names_in(journal.QUOTES_SURFACE, row["extra"]) == {
        journal.ExtraPath("envelope", vendor)
    }
    # The captured blocks are untouched, so the envelope's drift costs the envelope.
    assert (row["bid"], row["pe_ratio"], row["extended_last_price"]) == (649.98, 24.5, 651.0)


@pytest.mark.parametrize(
    ("envelope", "column", "landed"),
    [
        ({"realtime": False}, "realtime", False),
        ({"cusip": ""}, "cusip", ""),
        ({"reference": {"cusip": ""}}, "cusip", ""),
    ],
)
def test_a_falsy_envelope_value_is_captured_rather_than_read_as_absent(envelope, column, landed):
    """A false entitlement flag is a fact, and an empty CUSIP is a value the vendor sent.

    Both are read off the envelope by asking whether the key is there, never whether its
    value is truthy. A truthiness test would drop `realtime: false`, which says the feed is
    delayed, and record a null that reads as a field the vendor stopped sending. That is the
    confusion the whole routing exists to keep out of the lake, one level up.
    """
    row = _quote_row_with_envelope(**envelope)

    assert row[column] == landed and type(row[column]) is type(landed)
    assert row["extra"] is None


@pytest.mark.parametrize("value", [0, ""])
def test_a_falsy_retype_of_the_entitlement_flag_routes_like_any_other(value):
    """The same rule where it costs most: a falsy value the column refuses still routes.

    A truthiness test would drop the value before the column ever saw it, so the column
    would be null with nothing in ``extra``, which is a retype that left no signature at
    all. These two are the shapes that reach that path, since every other falsy value the
    flag could take is a bool the column accepts.
    """
    row = _quote_row_with_envelope(realtime=value)

    assert row["realtime"] is None
    assert json.loads(row["extra"]) == {"envelope": {"realtime": value}}


def test_a_contract_field_named_like_a_chain_level_one_stays_apart_from_it():
    """Why the chain-level values nest rather than sitting flat beside the contract's.

    A contract carrying ``underlyingPrice`` is an unrecognized contract field, and it
    overflows flat under that name. Were the chain-level value written flat too, the two
    would be one key, and a reader could not tell a routed chain-level price from a field
    the vendor added to every contract. Nested, they sit apart and both survive.
    """
    body = dict(
        _chain_of(_full_contract(underlyingPrice=1.5)),
        underlyingPrice="six hundred and fifty",
    )
    row = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[0]

    assert json.loads(row["extra"]) == {
        "underlyingPrice": 1.5,
        "chain": {"underlyingPrice": "six hundred and fifty"},
    }
    # The signature names the chain-level field alone. The flat key is an unrecognized
    # contract field, which is not drift and must never read as it.
    assert _known_names_in(journal.CHAINS_SURFACE, row["extra"]) == {
        journal.ExtraPath("chain", "underlyingPrice")
    }


@pytest.mark.parametrize(
    "sent", ["something the vendor sent", {"foo": 1}, {}, [1, 2], 3, 0, False, None]
)
def test_a_contract_field_named_chain_refuses_the_row_rather_than_merging_into_it(sent):
    """The one key a chains overflow could hold that a routed value would land inside.

    The fail-open writes an unrecognized contract field flat, so a contract field named
    ``chain`` sits exactly where the chain-level values nest. Merging a routed value into
    whatever the vendor sent would hide one measurement inside another, and the reader
    would take the vendor's dict for the parser's. So the row refuses by name and the cycle
    gaps, which is loud where the merge would be silent.

    Every shape a JSON field can hold is driven, because the check has to read the key's
    presence rather than its value. A dict is the shape that matters most: it is the one
    that would merge cleanly and leave nothing to notice, where a scalar only ever broke
    the merge by accident. A null and a false are the shapes a truthiness test would wave
    through, and an empty dict the one an emptiness test would.
    """
    body = dict(
        _chain_of(_full_contract(chain=sent)),
        underlyingPrice="six hundred and fifty",
    )

    # The message names the vendor's key first, because that is the one an operator reads
    # off the gap reason to find the field that collided.
    with pytest.raises(ValueError, match="named 'chain' collides with the overflow block"):
        journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)


def test_two_chain_level_fields_routing_at_once_share_the_block_they_created():
    """The second value into a block this row just wrote is not a collision.

    The refusal reads the overflow the vendor handed over, before anything is merged into
    it. Checking as each value is written would take the block the first one created for a
    vendor field of that name and refuse a row with no collision in it at all.
    """
    body = dict(
        _chain_of(_full_contract()),
        underlyingPrice="six hundred and fifty",
        interestRate="four and a quarter",
    )
    row = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[0]

    assert json.loads(row["extra"]) == {
        "chain": {"interestRate": "four and a quarter", "underlyingPrice": "six hundred and fifty"}
    }


def test_a_quotes_block_the_fail_open_wrote_is_merged_into_rather_than_refused():
    """The other side of the same rule, so the refusal does not swallow an ordinary merge.

    A quotes block key at the top of an overflow was written by the parser itself, holding
    that block's unrecognized fields, and a routed value from the same block belongs inside
    it. Refusing on presence alone would turn every drift beside an unknown field into a
    lost cycle on the surface where the nesting has always been ordinary.
    """
    row = _quote_row("quote", totalVolume="90000000", brandNewStat=1.5)

    assert json.loads(row["extra"]) == {"quote": {"brandNewStat": 1.5, "totalVolume": "90000000"}}


def test_a_chain_level_field_the_vendor_did_not_send_leaves_no_signature():
    """A null is not drift here either, and the chain-level fields repeat on every row.

    Every contract row carries the same chain-level values, so a vendor that stopped
    sending one leaves that column null on the whole chain. Counting a null as unfit would
    write that field's name into every row's overflow the moment any other chain-level
    field drifted, which is the false positive the signature cannot afford.
    """
    body = dict(_chain_of(_full_contract()), underlyingPrice="six hundred and fifty")
    del body["dividendYield"]
    row = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[0]

    assert row["dividend_yield"] is None
    assert json.loads(row["extra"]) == {"chain": {"underlyingPrice": "six hundred and fifty"}}


def test_a_drifted_chain_level_field_never_signs_an_absence_marker():
    """The chain-level values sit on the data rows, and a gap row holds none of them.

    One batch carries both kinds of row, and the routing reads per row. A gap row's
    chain-level columns are null, so the scan leaves them alone and the marker keeps its
    empty overflow rather than gaining a data row's drift signature.
    """
    body = dict(_chain_of(_full_contract()), underlyingPrice="six hundred and fifty")
    batch = journal.chains_data_batch(
        body,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        absent_markers=[
            journal.AbsentMarker("2026-10-16", None, "chain_chunk_failed", "2026-10-16")
        ],
    )
    data, marker = batch.to_pylist()

    assert data["row_kind"] == journal.ROW_KIND_DATA
    assert json.loads(data["extra"]) == {"chain": {"underlyingPrice": "six hundred and fifty"}}
    assert marker["row_kind"] == journal.ROW_KIND_GAP
    assert marker["underlying_price"] is None
    assert marker["extra"] is None


def test_a_chain_level_column_the_builder_recomputes_still_fails_the_row():
    """The two chain-level columns that are not the vendor's own value keep failing loudly.

    ``is_chain_truncated`` and ``number_of_contracts`` are derived from the captured rows,
    so no payload can put a bad value in either and no chain body can reach this. The row
    builder is driven directly for that reason: what is being checked is that neither has a
    key in the overflow, so a value that refuses is this code's bug surfacing as this
    code's bug rather than being parked as vendor drift.
    """
    for column, value in (("is_chain_truncated", "maybe"), ("number_of_contracts", 2.5)):
        row = {
            "ticker": "SPY",
            "row_kind": journal.ROW_KIND_DATA,
            "schema_version": journal.SCHEMA_VERSION,
            column: value,
        }
        with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
            journal._batch(journal.CHAINS_SURFACE, [row])


def test_the_recomputed_chain_level_columns_are_what_the_rows_say():
    """The control for the test above: a chain body cannot reach either column.

    The vendor's own header figures are overwritten, the count by the captured rows' own
    length and the flag by that count's source plus whether a window was given up. So a
    body sending either as a wrong shape still lands, which is why neither needs a route
    into ``extra`` and why the refusal above had to be driven through the row builder.
    """
    body = dict(_chain_of(_full_contract()), isChainTruncated="yes", numberOfContracts="lots")
    row = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH).to_pylist()[0]

    assert row["number_of_contracts"] == 1
    assert row["is_chain_truncated"] is True  # bool("yes"), the vendor's truthiness kept
    assert row["extra"] is None


def test_every_value_a_payload_can_carry_refuses_in_one_of_the_four_named_families():
    """The routing names four exception families. A fifth would cost cycles silently.

    ``UNFIT_ERRORS`` is what decides whether a refusal is one value's doing. A pyarrow
    upgrade that starts raising something outside it would send a retyped field back to
    gapping the whole cycle, and nothing else in the suite would notice. So the families
    are checked by enumeration: every JSON-representable value against every type the two
    pinned schemas use.
    """
    values = [
        None,
        0,
        1,
        -1,
        2**63,
        2**64,
        -(2**64),
        0.0,
        1.0,
        3.7,
        1e19,
        float("nan"),
        float("inf"),
        True,
        False,
        "7",
        "a",
        "",
        "\ud800",
        "\U0001f600",
        {"a": 1},
        {},
        [1, 2],
        [],
    ]
    types = {
        field.type for surface in journal.PINNED_SURFACES for field in journal.schema_for(surface)
    }
    raised = set()
    for field_type in types:
        for value in values:
            try:
                journal.typed_column(field_type, [value])
            except journal.UNFIT_ERRORS as exc:
                raised.add(type(exc))
            except Exception as exc:  # noqa: BLE001 - the point is to catch a fifth family
                raise AssertionError(
                    f"{type(exc).__name__} from {value!r} into {field_type}, which "
                    "UNFIT_ERRORS does not name, so that shape would gap the cycle"
                ) from exc
    assert raised == set(journal.UNFIT_ERRORS), (
        "a family in UNFIT_ERRORS that no value reaches is a family that was guessed"
    )


def test_a_value_this_code_put_on_a_gap_row_never_routes():
    """A gap row carries no vendor observation, so nothing on one can be vendor drift.

    Two builders write a value on a gap row, and both write the ``expiration_date`` those
    rows exist to name. Routing it would delete the marker's only fact and leave a gap row
    carrying the signature that says a vendor retyped a field. Both are wrong, and the
    second is worse, because the read-time refusal in marketlake #149 reads that signature.

    So the refusal propagates instead, which is this code's bug surfacing as this code's
    bug.
    """
    slot = datetime.fromisoformat("2026-09-13T16:00:00-04:00")
    with pytest.raises(pa.ArrowTypeError):
        journal.absent_series_rows(
            journal.CHAINS_SURFACE,
            ticker="SPY",
            slot=slot,
            expirations=[20261016],
            error_class="option_close_series_absent",
        )
    with pytest.raises(pa.ArrowTypeError):
        journal.chains_data_batch(
            _chain_of(_full_contract()),
            ticker="SPY",
            snap_ts=SNAP,
            fetch_ts=FETCH,
            absent_markers=[
                journal.AbsentMarker("2026-09-13", None, "chain_chunk_failed", 20260918)
            ],
        )


def test_one_column_refusing_on_both_row_kinds_routes_neither():
    """The rule is every refusal on a data row, not some refusal on a data row.

    One column can refuse in two places at once, a contract's drifted value and an absence
    marker's, and then the batch carries a refusal this code owns beside one the vendor
    does. Routing on the strength of the vendor's would strip the marker of the one fact it
    exists to name and stamp a data row's drift signature on a gap. So the whole batch
    refuses, and this code's bug surfaces as this code's bug.

    Checking that any refusal sits on a data row would pass every other test in this file,
    because each of them puts every refusal on one kind of row.
    """
    with pytest.raises(pa.ArrowTypeError):
        journal.chains_data_batch(
            _chain_of(_full_contract(expirationDate=20260918)),
            ticker="SPY",
            snap_ts=SNAP,
            fetch_ts=FETCH,
            absent_markers=[
                journal.AbsentMarker("2026-09-13", None, "chain_chunk_failed", 20260918)
            ],
        )


def test_the_marker_a_gap_row_names_still_lands_when_it_is_the_right_type():
    """The control for the refusal above, so it is not a builder broken outright."""
    slot = datetime.fromisoformat("2026-09-13T16:00:00-04:00")
    batch = journal.absent_series_rows(
        journal.CHAINS_SURFACE,
        ticker="SPY",
        slot=slot,
        expirations=["2026-10-16"],
        error_class="option_close_series_absent",
    )
    row = batch.to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["expiration_date"] == "2026-10-16"
    assert row["extra"] is None


def test_a_drifted_contract_beside_a_marker_routes_the_contract_alone():
    """One batch holding both kinds of row, with the drift on the data row.

    ``chains_data_batch`` puts the chunker's absence markers in the same batch as the
    captured contracts, so one ``expiration_date`` column holds a vendor value and a value
    this code wrote. The rule has to read per row rather than per column, or a marker beside
    a drifted contract would either block the routing or be swept into it.
    """
    batch = journal.chains_data_batch(
        _chain_of(_full_contract(expirationDate=1787000000000)),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        absent_markers=[
            journal.AbsentMarker("2026-10-16", None, "chain_chunk_failed", "2026-10-16")
        ],
    )
    data, marker = batch.to_pylist()
    assert data["row_kind"] == journal.ROW_KIND_DATA
    assert data["expiration_date"] is None
    assert json.loads(data["extra"]) == {"expirationDate": 1787000000000}
    assert marker["row_kind"] == journal.ROW_KIND_GAP
    assert marker["expiration_date"] == "2026-10-16"
    assert marker["extra"] is None


def test_an_identity_column_routes_and_the_row_still_lands_as_data():
    """The sharpest edge of the trade #129 took, recorded rather than left implicit.

    ``occ_symbol``, ``strike_price``, ``expiration_date``, and ``put_call`` are ordinary
    vendor fields the parser copies verbatim, so they route like any other. A row that
    loses all four lands as data with nothing left to join it on, and every completeness
    counter reads the minute as captured.

    That is the accepted cost and not an oversight. The alternative is losing the minute
    for every contract in the chain, and the raw values are all still in ``extra``, so the
    row is repairable where a lost minute is not. What makes it findable is the signature,
    which this checks names all four.
    """
    row = _chain_row(symbol=123456, strikePrice="650.0", expirationDate=1787000000000, putCall=7)
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row["error_class"] is None
    assert (row["occ_symbol"], row["strike_price"], row["expiration_date"], row["put_call"]) == (
        None,
        None,
        None,
        None,
    )
    assert json.loads(row["extra"]) == {
        "symbol": 123456,
        "strikePrice": "650.0",
        "expirationDate": 1787000000000,
        "putCall": 7,
    }
    assert _known_names_in(journal.CHAINS_SURFACE, row["extra"]) == {
        journal.ExtraPath(None, "symbol"),
        journal.ExtraPath(None, "strikePrice"),
        journal.ExtraPath(None, "expirationDate"),
        journal.ExtraPath(None, "putCall"),
    }


def test_a_row_that_drifted_twice_keeps_both_values():
    """A contract can drift in more than one field, and both have to survive.

    Each column is built on its own, so a row accumulates one routed entry per column that
    refused it. Keeping only the first would throw the second value away with no signature
    left behind, which is the loss this whole change exists to stop, one field narrower.
    """
    row = _chain_row(openInterest=1234.7, bid="nope")
    assert row["open_interest"] is None and row["bid"] is None
    assert json.loads(row["extra"]) == {"openInterest": 1234.7, "bid": "nope"}
    assert _known_names_in(journal.CHAINS_SURFACE, row["extra"]) == {
        journal.ExtraPath(None, "openInterest"),
        journal.ExtraPath(None, "bid"),
    }


def test_two_drifted_fields_in_one_quote_block_both_land_under_it():
    """The same on the nested surface, where both entries share one block dict."""
    row = _quote_row("quote", totalVolume="90000000", bidPrice="649.98")
    assert row["total_volume"] is None and row["bid"] is None
    assert json.loads(row["extra"]) == {"quote": {"totalVolume": "90000000", "bidPrice": "649.98"}}


def test_a_routed_quote_field_joins_its_blocks_existing_overflow():
    """A block holding an unrecognized field and a drifted one keeps both.

    A chains contract field's merge writes straight onto the top-level dict. A quotes
    block's has to read that block's dict and add to it. Replacing the block instead would
    drop the unrecognized field the fail-open was built to keep, and the flat test cannot
    see that because it has no block to clobber.
    """
    row = _quote_row("quote", totalVolume="90000000", brandNewStat=1.5)
    assert json.loads(row["extra"]) == {"quote": {"brandNewStat": 1.5, "totalVolume": "90000000"}}


def test_an_integer_too_wide_for_the_column_routes_like_any_other_refusal():
    """The ``OverflowError`` family, driven through a row builder rather than the column.

    The enumeration test asks ``typed_column`` which families a value can raise, and never
    reaches the routing. So each family also needs one end-to-end case, or the ``except``
    clauses that consume ``UNFIT_ERRORS`` could narrow away from the constant and send a
    whole shape back to gapping the cycle with nothing noticing.
    """
    row = _chain_row(openInterest=2**64)
    assert row["open_interest"] is None
    assert json.loads(row["extra"]) == {"openInterest": 2**64}


def test_a_genuinely_absent_field_beside_a_drifted_one_leaves_no_signature():
    """A null is not drift, and must never be written into ``extra`` as though it were.

    Every routed value is found by offering each of a column's values to ``_fits`` on its
    own, and a null fits, because an all-null column lands typed. Were it counted unfit,
    then in any batch where one contract drifted, every other contract whose field the
    vendor simply did not send would get that field's name into its overflow. That is a
    known field's name on a row that never drifted, which is the false positive the
    signature cannot afford and which the read-time refusal in marketlake #149 keys on.

    The fixtures elsewhere fill every field, so only a batch built with one missing can see
    this.
    """
    absent = _full_contract(symbol="SPY   260918C00650001")
    absent.pop("openInterest")
    contracts = [absent, _full_contract(symbol="SPY   260918C00650002", openInterest="1235")]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    quiet, drifted = batch.to_pylist()
    assert quiet["open_interest"] is None
    assert quiet["extra"] is None, "a vendor null was recorded as drift"
    assert _known_names_in(journal.CHAINS_SURFACE, quiet["extra"]) == set()
    assert drifted["open_interest"] is None
    assert json.loads(drifted["extra"]) == {"openInterest": "1235"}


def test_the_overflow_serializes_the_same_bytes_however_the_payload_was_ordered():
    """One logical overflow is one string, so a byte comparison of segments is stable.

    Sorting the keys is what makes that true, and it has to hold once a routed value has
    been merged in as well as on the fail-open's own output. A routed key added last would
    otherwise serialize last, and the same contract arriving with its fields in a different
    order would write different bytes for the same facts.

    Both surfaces are driven, because each dumps its own overflow. The routed merge re-dumps
    a row it touches, so a quotes row that never routed is the one whose bytes come straight
    off the fail-open's dump and nothing else would see it.
    """
    # The fail-open's own output, from two payloads that differ only in key order.
    first = _chain_row(zzzNewField=1, aaaNewField=2)
    second = _chain_row(aaaNewField=2, zzzNewField=1)
    assert first["extra"] == second["extra"] == '{"aaaNewField": 2, "zzzNewField": 1}'
    # The merged output. The unrecognized key sorts after the routed one, so an unsorted
    # dump would put the routed key last instead.
    merged = _chain_row(zzzNewField=1, openInterest=1234.7)
    assert merged["extra"] == '{"openInterest": 1234.7, "zzzNewField": 1}'
    # The quotes surface, whose overflow nests, so both the block keys and the keys inside
    # one block have to sort.
    first = _quote_row("quote", zzzNewStat=1, aaaNewStat=2)
    second = _quote_row("quote", aaaNewStat=2, zzzNewStat=1)
    assert first["extra"] == second["extra"] == '{"quote": {"aaaNewStat": 2, "zzzNewStat": 1}}'
    blocks = journal.quotes_data_batch(
        dict(
            _quote_with("extended", zzzNewStat=1),
            **{"fundamental": dict(QUOTE["fundamental"], aaaNewStat=2)},
        ),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    ).to_pylist()[0]
    assert blocks["extra"] == ('{"extended": {"zzzNewStat": 1}, "fundamental": {"aaaNewStat": 2}}')


# -- the version stamp on every row ------------------------------------------

# Every builder in the module, each with the arguments that reach every stamp site it
# owns. ``chains_data_batch`` owns two, one for its contract rows and one for its
# absence-marker rows, so it appears once with both. The count is asserted against the
# module's source below, so a builder added later is covered here or the suite says so.
STAMPING_BUILDERS = {
    "chains_data_batch": lambda: journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        windows=[("2026-08-24", "2026-10-15"), ("2026-10-16", None)],
        absent_markers=[
            journal.AbsentMarker("2026-10-16", None, "chain_chunk_failed", "2026-10-16")
        ],
    ),
    "quotes_data_batch": lambda: journal.quotes_data_batch(
        QUOTE, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    ),
    "gap_batch": lambda: journal.gap_batch(
        "chains", ticker="SPY", snap_ts=SNAP, error_class="http_429"
    ),
    "gap_rows": lambda: journal.gap_rows(
        "quotes",
        ticker="SPY",
        slots=[
            datetime.fromisoformat("2026-08-24T15:58:00-04:00"),
            datetime.fromisoformat("2026-08-24T15:59:00-04:00"),
        ],
        error_class="daemon_down",
    ),
    # The bars builder splits in two, because #280's gate sits between the rows and the
    # batch. Only ``bars_rows`` stamps, so it is the stamp site, and the batch is built here
    # because the assertion below reads the column off one.
    "bars_rows": lambda: journal.bars_data_batch(
        journal.bars_rows(
            {
                "candles": [
                    {
                        "open": 648.0,
                        "high": 648.9,
                        "low": 647.8,
                        "close": 648.6,
                        "volume": 1450000,
                        "datetime": 1787578200000,
                    }
                ],
                "symbol": "SPY",
                "empty": False,
            },
            ticker="SPY",
            freq="1m",
            instrument_id=1,
            fetch_ts=FETCH,
            fetch_end_ts=None,
            window_start="2026-08-24T09:30:00-04:00",
            window_end="2026-08-24T16:00:00-04:00",
            extended_hours=None,
        )
    ),
    "absent_series_rows": lambda: journal.absent_series_rows(
        "chains",
        ticker="SPY",
        slot=datetime.fromisoformat("2026-08-24T16:00:00-04:00"),
        expirations=["2026-09-18", "2026-10-16"],
        error_class="option_close_series_absent",
    ),
}


@pytest.mark.parametrize("builder", sorted(STAMPING_BUILDERS))
def test_every_builder_stamps_the_schema_version_on_every_row(builder):
    """The stamp is the provenance, so every row of every builder carries it.

    Compaction unlinks a ticker-day's segments once the partition is sealed, so this
    integer is the only record left of which code shape wrote a row. A row that carries
    the wrong version, or a null, is a row whose shape cannot be recovered at all.
    """
    batch = STAMPING_BUILDERS[builder]()
    stamps = batch.column("schema_version").to_pylist()
    assert stamps, f"{builder} built no rows, so it stamped nothing"
    assert set(stamps) == {journal.SCHEMA_VERSION}, stamps


def test_the_builders_covered_are_every_stamp_site_in_the_module():
    """The set covered is enumerated against the module's source, not assumed.

    A test that builds five builders proves nothing about a sixth. Counting the stamp
    sites in the source is what makes this cover the class. ``chains_data_batch`` owns two
    of them, one for contract rows and one for absence markers.
    """
    source = Path(journal.__file__).read_text()
    sites = source.count('"schema_version": SCHEMA_VERSION')
    assert sites == len(STAMPING_BUILDERS) + 1, (
        f"{sites} stamp sites in journal.py against {len(STAMPING_BUILDERS)} builders "
        "covered here. A builder that stamps the version needs an entry in "
        "STAMPING_BUILDERS, and one that does not stamp it writes rows whose shape "
        "cannot be recovered after a seal."
    )


# -- path convention ---------------------------------------------------------


def test_segment_path_mirrors_the_fixture_lake_contract(tmp_path):
    root = tmp_path / "lake"
    fixture = FixtureLake(root)
    mine = journal.segment_path(root, "chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    theirs = fixture.segment_path("chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    assert mine == theirs


# -- the drift signature read back off a built batch ---------------------------

# ``journal.routed_columns`` is what the daemon's schema-drift page reads. The section
# above pins where a routed value lands. These pin that the scan finds it there, that it
# passes over everything else in the overflow, and that an ordinary cycle's all-null
# overflow costs it nothing.

# A value no pinned column type will take. All four types the two schemas use, string,
# double, int64 and bool, refuse a JSON object, so one value drives a retype of any
# column and the test needs no per-type table that could go stale.
UNFIT_VALUE = {"amount": 3, "unit": "contracts"}


def _chain_batch(**overrides):
    """The batch a chain body carrying a single overridden contract builds."""
    return journal.chains_data_batch(
        _chain_of(_full_contract(**overrides)), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )


def _chain_header_batch(**header):
    """The batch a chain body with chain-level fields overridden builds."""
    return journal.chains_data_batch(
        dict(_chain_of(_full_contract()), **header), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )


def _quote_batch(block: str, **overrides):
    """The batch a quote envelope with one captured block's fields overridden builds."""
    return journal.quotes_data_batch(
        _quote_with(block, **overrides),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    )


def _quote_envelope_batch(**envelope):
    """The batch a quote envelope with envelope-level fields overridden builds."""
    return journal.quotes_data_batch(
        dict(QUOTE, **envelope),
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    )


def _retyped_batch(surface: str, path: journal.ExtraPath):
    """The batch one surface builds when the vendor retypes the field at ``path``.

    The level a field arrives at decides which builder drives it, and the path's own
    ``block`` is what names that level. So this walks the same mapping the scan walks
    rather than a list of fields, which is what makes the caller cover every path rather
    than the handful anyone thought to write down.
    """
    if surface == journal.CHAINS_SURFACE:
        if path.block is None:
            return _chain_batch(**{path.field: UNFIT_VALUE})
        return _chain_header_batch(**{path.field: UNFIT_VALUE})
    if path.block == "envelope":
        return _quote_envelope_batch(**{path.field: UNFIT_VALUE})
    return _quote_batch(path.block, **{path.field: UNFIT_VALUE})


@pytest.mark.parametrize("surface", [journal.CHAINS_SURFACE, journal.QUOTES_SURFACE])
def test_routed_columns_names_every_column_the_routing_writes(surface):
    """The scan and the routing agree on every path, by walking the mapping itself.

    This is the guard on the cost the page's design accepted. ``_batch`` knows exactly
    which columns refused and discards that, and ``routed_columns`` re-derives it from the
    built batch, so the two could drift apart. Both read ``extra_paths`` rather than a list
    of their own, and walking every entry here is what turns that derivation into a
    checked fact. A path added by promoting a vendor field is covered the day it is added.
    """
    paths = journal.extra_paths(surface)
    assert paths, surface
    for column, path in paths.items():
        batch = _retyped_batch(surface, path)
        assert batch.column(column).to_pylist() == [None] * batch.num_rows, column
        assert journal.routed_columns(surface, batch) == (column,), column


def test_an_unrecognized_vendor_field_is_not_a_routed_column():
    """The fail-open working as designed, which belongs to the nightly report.

    A field the vendor invented lands in ``extra`` under a name no path claims. Paging on
    it would page on every new greek the vendor ships, and the design already sends that
    to the nightly report instead. Matching against ``extra_paths`` is the whole
    distinction between the two, so a scan that read the overflow's keys alone would page
    on both.
    """
    batch = _chain_batch(brandNewGreek=1.5)
    assert json.loads(batch.column("extra").to_pylist()[0]) == {"brandNewGreek": 1.5}
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()

    quotes = _quote_batch("quote", brandNewStat=1.5)
    assert json.loads(quotes.column("extra").to_pylist()[0]) == {"quote": {"brandNewStat": 1.5}}
    assert journal.routed_columns(journal.QUOTES_SURFACE, quotes) == ()


def test_an_unrecognized_field_sharing_a_blocks_name_is_still_not_a_routed_column():
    """A quotes block the fail-open filled with the vendor's own new fields.

    An unrecognized quotes field nests under the block it arrived in, so the block key a
    routed value writes under is already there in the ordinary case. What separates the
    two is the field inside it, not the block, and a scan that stopped at the block would
    page on every new field the vendor adds to a block it already sends.
    """
    batch = _quote_batch("quote", brandNewStat=1.5, anotherNewStat=2.5)
    assert json.loads(batch.column("extra").to_pylist()[0]) == {
        "quote": {"brandNewStat": 1.5, "anotherNewStat": 2.5}
    }
    assert journal.routed_columns(journal.QUOTES_SURFACE, batch) == ()


def test_a_vendor_field_named_chain_does_not_fabricate_a_chain_level_drift():
    """The one shape that reads as drift while nothing refused anything.

    The chains fail-open writes an unrecognized contract field flat, so a vendor contract
    field named ``chain`` lands on the key the chain-level values nest under, and its own
    subkeys then read as chain-level vendor names. ``_extra_with_routed`` refuses that
    collision by name, but only on a row where something actually routed, so a row that
    routed nothing never reaches that refusal.

    What settles it is the row's own column. A routed value is nulled in its column on the
    same row, so a key naming a column that still holds a value cannot be the signature.
    Here both columns hold the vendor's real header values, and a page naming them would
    send an operator to look for a retype that never happened.
    """
    row = _full_contract(chain={"underlyingPrice": 250.0, "interestRate": 4.5})
    batch = journal.chains_data_batch(_chain_of(row), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert json.loads(batch.column("extra").to_pylist()[0]) == {
        "chain": {"underlyingPrice": 250.0, "interestRate": 4.5}
    }
    # The real chain-level values are in their columns, untouched.
    assert batch.column("underlying_price").to_pylist() == [650.01]
    assert batch.column("interest_rate").to_pylist() == [4.25]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()


@pytest.mark.parametrize("value", [5, "a string", [1, 2, 3], 1.5, True, None])
def test_a_vendor_field_named_chain_of_any_shape_is_read_rather_than_raising(value):
    """The collision key can hold anything, and the scan has to survive all of it.

    A dict is the shape that would otherwise be walked as a block, so the scan asks the
    shape before walking. Without that question an integer under the key raises a
    ``TypeError`` from iterating it. The capture path catches that, so it costs the finding
    rather than the minute, but it costs every finding on every segment for as long as the
    vendor sends that field.

    Reaching the question at all takes a chain-level column the vendor left null, which is
    what puts that column among the candidates and its block key among the ones worth
    walking. A body still carrying ``underlyingPrice`` filters the column out first and the
    key is never looked at, which is how an earlier version of this test passed while the
    guard it names went unexercised.
    """
    body = dict(CHAIN_BODY, putExpDateMap={})
    body.pop("underlyingPrice")
    body["callExpDateMap"] = {"2026-09-18:25": {"650.0": [_full_contract(chain=value)]}}
    batch = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("underlying_price").to_pylist() == [None], "the guard stays unreached"
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()


def test_a_genuine_chain_level_retype_still_routes_beside_that_collision():
    """The control for the test above, so the null check cannot buy silence.

    A check that refused every chain-block key would pass the collision test and lose every
    real chain-level drift, which is half the chains surface's paths.
    """
    body = dict(_chain_of(_full_contract()), underlyingPrice="six hundred and fifty")
    batch = journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("underlying_price").to_pylist() == [None]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("underlying_price",)


def test_a_routed_column_is_named_off_the_row_that_actually_routed():
    """One contract drifts and another carries the column, in the same batch.

    The scan walks rows until it has named every path, so a check that read the column's
    nulls in bulk rather than at the routed row would answer from whichever row it reached
    first. The drifted contract is second here for that reason.
    """
    contracts = [
        _full_contract(symbol="SPY   260918C00650000", openInterest=1234),
        _full_contract(symbol="SPY   260918C00650001", openInterest=UNFIT_VALUE),
    ]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    assert batch.column("open_interest").to_pylist() == [1234, None]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("open_interest",)


def test_a_vendor_null_is_not_a_routed_column():
    """A field the vendor sent as null never routes, so it never reads as drift.

    ``None`` fits every column, because an all-null column lands typed. So a genuine
    vendor null leaves the column null with an empty overflow, which is byte-identical to
    a field that stopped arriving. Neither is this page's, and the missing half is
    marketlake #265.
    """
    batch = _chain_batch(openInterest=None)
    assert batch.column("open_interest").to_pylist() == [None]
    assert batch.column("extra").to_pylist() == [None]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()


def test_a_second_ticker_drifting_the_same_column_still_names_it_once():
    """The scan names columns, not rows. A whole chain drifting is one column.

    A vendor retype reaches every contract in the payload at once, so a scan that
    returned one entry per row would hand the page a list as long as the chain.
    """
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{index}", openInterest=UNFIT_VALUE)
        for index in range(3)
    ]
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    assert batch.num_rows == 3
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("open_interest",)


def test_two_columns_drifting_at_once_are_both_named():
    """A vendor change that moved two fields has to name both, in a stable order."""
    batch = _chain_batch(openInterest=UNFIT_VALUE, bid=UNFIT_VALUE)
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("bid", "open_interest")


def test_an_all_null_overflow_is_answered_on_the_null_count_alone(monkeypatch):
    """The ordinary cycle's cost: one null count, and nothing else at all.

    ``extra`` was non-null on zero of the lake's sealed rows, so this is the path every
    capture minute takes and it runs between the row build and the segment write. Breaking
    the two calls the scan would otherwise make is what proves the short-circuit rather
    than assuming it. A scan that skipped past the null count would pass every other test
    here while rebuilding both vendor maps and materializing the whole overflow column,
    once per segment, every minute of the session.

    ``extra_paths`` is the one that bites, because its builders run per call rather than
    once at import, so reaching it is real work and not a dict lookup.
    """

    def refuse(*args, **kwargs):
        raise AssertionError("the scan did work an all-null overflow should have skipped")

    batch = _chain_batch()
    assert batch.column("extra").to_pylist() == [None]
    monkeypatch.setattr(journal, "extra_paths", refuse)
    monkeypatch.setattr(journal.json, "loads", refuse)
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()


def test_a_populated_overflow_with_every_column_filled_parses_no_json(monkeypatch):
    """The gate that makes an ordinary populated overflow cheap, held by breaking the parse.

    An unrecognized vendor field lands in ``extra`` on every row of the payload, and the
    design calls that the fail-open working rather than a page. So a populated overflow is
    an ordinary event, and a scan that parsed every row for it would pay that cost on the
    capture path for a finding that is always empty. Measured before this gate existed, one
    new greek cost 137 ms on a 13,500-row chain.

    A routed value is nulled in its own column, so a column the vendor is still filling
    cannot have routed. Every known column here holds a value, so nothing is a candidate and
    no row is ever read.
    """

    def refuse(*args, **kwargs):
        raise AssertionError("the scan parsed a row with no candidate column to find")

    batch = _chain_batch(brandNewGreek=1.5)
    assert batch.column("extra").null_count == 0
    monkeypatch.setattr(journal.json, "loads", refuse)
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()


def test_a_retype_is_still_found_beside_a_column_the_vendor_left_null():
    """The control for the gate above, so it cannot buy its speed with silence.

    A sparse contract nulls columns the vendor simply had nothing for, which makes them
    candidates. The drifted column has to be told apart from them by the overflow key, not
    by the null alone, and the sparse ones must not page.
    """
    row = _full_contract(openInterest=UNFIT_VALUE, delta=None, gamma=None, rho=None)
    batch = journal.chains_data_batch(_chain_of(row), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
    assert batch.column("delta").to_pylist() == [None]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("open_interest",)


def test_a_drift_on_a_later_row_is_found_when_earlier_rows_are_clean():
    """A vendor that retypes a field on only some contracts, which is a partial rollout.

    Every other drift fixture retypes on every row, so a scan that stopped at the first
    clean row, or that required every row to carry an overflow, would pass all of them and
    miss this. The drifted contract is last here for that reason.
    """
    contracts = [
        _full_contract(symbol=f"SPY   260918C0065000{index}", openInterest=1234)
        for index in range(4)
    ]
    contracts.append(_full_contract(symbol="SPY   260918C00650009", openInterest=UNFIT_VALUE))
    batch = journal.chains_data_batch(
        _chain_of(*contracts), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    assert batch.column("extra").to_pylist()[:4] == [None] * 4
    assert batch.column("open_interest").to_pylist() == [1234, 1234, 1234, 1234, None]
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ("open_interest",)


def test_the_columns_come_back_in_a_stable_order_whatever_the_payload_order():
    """The page prints these and the cap cuts them, so the order cannot follow the payload.

    Without sorting, which columns survive ``PAGE_COLUMN_CAP`` would depend on the order the
    vendor happened to send fields in. Enough columns are drifted here that set iteration
    order essentially never matches sorted order by accident.
    """
    drifted = dict.fromkeys(
        ("openInterest", "totalVolume", "bid", "ask", "delta", "gamma", "theta", "rho"),
        UNFIT_VALUE,
    )
    batch = journal.chains_data_batch(
        _chain_of(_full_contract(**drifted)), ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH
    )
    got = journal.routed_columns(journal.CHAINS_SURFACE, batch)
    assert len(got) == len(drifted)
    assert list(got) == sorted(got)
    assert got == ("ask", "bid", "delta", "gamma", "open_interest", "rho", "theta", "volume")


def test_a_gap_batch_carries_no_overflow_to_scan():
    """A gap row holds no vendor observation, so a gap segment can never read as drift.

    ``_routed_column`` refuses to route onto a gap row, which makes a gap batch's overflow
    null by construction. That is why the scan needs no ``row_kind`` test of its own, and
    a second definition of that rule here is what this exists to keep out.
    """
    batch = journal.gap_rows(
        journal.CHAINS_SURFACE, ticker="SPY", slots=[SNAP], error_class="http_429"
    )
    assert batch.column("extra").to_pylist() == [None] * batch.num_rows
    assert journal.routed_columns(journal.CHAINS_SURFACE, batch) == ()
