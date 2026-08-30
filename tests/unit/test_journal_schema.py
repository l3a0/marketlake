"""The pinned schemas and the row builders, decided from values alone.

These tests build record batches in memory. No file, process, or query engine is
crossed, so they sit in the unit tier. They pin the capture schema's shape, the
vendor-field mapping, the fail-open overflow, and the gap-row nulling.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from lake import journal
from tests.support.lake import FixtureLake

# A minimal chain body, shaped like a Schwab chain response. One call and one put.
CHAIN_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "isDelayed": False,
    "interestRate": 4.25,
    "underlyingPrice": 650.01,
    "dividendYield": 1.28,
    "numberOfContracts": 2,
    "callExpDateMap": {
        "2026-09-18:25": {
            "650.0": [
                {
                    "putCall": "CALL",
                    "symbol": "SPY   260918C00650000",
                    "bid": 4.2,
                    "ask": 4.25,
                    "last": 4.22,
                    "openInterest": 1234,
                    "totalVolume": 5555,
                    "volatility": 12.5,
                    "delta": 0.51,
                    "gamma": 0.03,
                    "theta": -0.12,
                    "vega": 0.34,
                    "rho": 0.08,
                }
            ]
        }
    },
    "putExpDateMap": {
        "2026-09-18:25": {
            "650.0": [
                {
                    "putCall": "PUT",
                    "symbol": "SPY   260918P00650000",
                    "bid": 3.8,
                    "ask": 3.85,
                    "last": 3.82,
                    "openInterest": 987,
                    "totalVolume": 4444,
                    "volatility": 12.7,
                    "delta": -0.49,
                    "gamma": 0.03,
                    "theta": -0.11,
                    "vega": 0.34,
                    "rho": -0.07,
                }
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


def test_schema_version_is_one():
    assert journal.SCHEMA_VERSION == 1


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
        "row_kind",
        "error_class",
        "close_tag",
        "session_phase",
        "extra",
    ):
        assert name in schema.names, name


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
    batch = journal.chains_data_batch(
        CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    assert batch.schema == journal.CHAINS_SCHEMA
    assert batch.num_rows == 2
    table = pa.Table.from_batches([batch])
    symbols = table.column("occ_symbol").to_pylist()
    assert symbols == ["SPY   260918C00650000", "SPY   260918P00650000"]
    assert table.column("put_call").to_pylist() == ["CALL", "PUT"]


def test_chains_data_batch_maps_known_fields_and_header():
    batch = journal.chains_data_batch(
        CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
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
    # The chain-level fields repeat on every row: the headers and the entitlement flag.
    assert row["interest_rate"] == 4.25
    assert row["underlying_price"] == 650.01
    assert row["dividend_yield"] == 1.28
    assert row["is_delayed"] is False


def test_chains_data_batch_provenance_and_stamps():
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
        close_tag="canonical",
    )
    row = batch.slice(0, 1).to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row["error_class"] is None
    assert row["suspect"] is False
    assert row["close_tag"] == "canonical"
    assert row["session_phase"] is None
    assert row["schema_version"] == 1
    assert row["snap_ts"] == SNAP
    assert row["fetch_ts"] == FETCH
    assert row["vendor_quote_ts"] == VENDOR
    assert row["ticker"] == "SPY"


def test_chains_extra_is_empty_for_a_known_payload():
    batch = journal.chains_data_batch(
        CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_is_delayed_lands_in_the_column_and_not_in_extra():
    # The payload's entitlement flag is now recognized. It rides the typed column on
    # every contract row, and the overflow stays empty.
    batch = journal.chains_data_batch(
        CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    assert batch.column("is_delayed").to_pylist() == [False, False]
    assert batch.column("extra").to_pylist() == [None, None]


def test_chains_unknown_contract_field_lands_in_extra():
    body = json.loads(json.dumps(CHAIN_BODY))  # deep copy
    contract = body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]
    contract["mark"] = 4.23
    contract["bidSize"] = 40
    batch = journal.chains_data_batch(
        body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    call_extra = batch.slice(0, 1).to_pylist()[0]["extra"]
    assert json.loads(call_extra) == {"mark": 4.23, "bidSize": 40}
    # Known fields still land typed, not swept into the overflow.
    assert batch.slice(0, 1).to_pylist()[0]["bid"] == 4.2
    # The put row, with no unknown field, keeps an empty overflow.
    assert batch.slice(1, 1).to_pylist()[0]["extra"] is None


def test_chains_suspect_flag_rides_every_row():
    batch = journal.chains_data_batch(
        CHAIN_BODY,
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
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
        vendor_quote_ts=VENDOR,
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


# -- path convention ---------------------------------------------------------


def test_segment_path_mirrors_the_fixture_lake_contract(tmp_path):
    root = tmp_path / "lake"
    fixture = FixtureLake(root)
    mine = journal.segment_path(root, "chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    theirs = fixture.segment_path("chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    assert mine == theirs
