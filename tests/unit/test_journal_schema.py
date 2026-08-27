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

QUOTE = {
    "bidPrice": 649.98,
    "askPrice": 650.02,
    "lastPrice": 650.0,
    "quoteTime": 1787000100000,
    "realtime": True,
}

SNAP = "2026-08-24T16:15:00-04:00"
FETCH = "2026-08-24T16:15:00.400-04:00"
VENDOR = "2026-08-24T16:15:00-04:00"


# -- schema shape ------------------------------------------------------------


def test_schema_version_is_one():
    assert journal.SCHEMA_VERSION == 1


def test_chains_schema_names_and_types():
    schema = journal.CHAINS_SCHEMA
    assert schema.field("snap_ts").type == pa.string()
    assert schema.field("fetch_ts").type == pa.string()
    assert schema.field("vendor_quote_ts").type == pa.string()
    assert schema.field("open_interest").type == pa.int64()
    assert schema.field("suspect").type == pa.bool_()
    assert schema.field("schema_version").type == pa.int64()
    # The real-time entitlement flag is a chain-level bool, not the provenance suspect.
    assert schema.field("is_delayed").type == pa.bool_()
    # Vendor per-contract columns, the chain-level fields, and the provenance columns.
    for name in (
        "occ_symbol",
        "put_call",
        "bid",
        "ask",
        "last",
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
    assert schema.names[:4] == ["snap_ts", "fetch_ts", "vendor_quote_ts", "ticker"]
    for name in ("bid", "ask", "last", "realtime", "row_kind", "schema_version", "extra"):
        assert name in schema.names, name
    # The entitlement flag is a per-row vendor bool, placed with the price fields.
    assert schema.field("realtime").type == pa.bool_()
    assert schema.names.index("realtime") == schema.names.index("last") + 1
    # Quotes never carry a per-contract vendor column.
    assert "open_interest" not in schema.names


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
    assert row["realtime"] is True
    assert row["ticker"] == "SPY"
    assert row["vendor_quote_ts"] == VENDOR
    assert row["row_kind"] == journal.ROW_KIND_DATA
    # quoteTime is recognized and carried in vendor_quote_ts, and realtime is a typed
    # column, so neither pollutes the normally-empty overflow.
    assert row["extra"] is None


def test_quotes_realtime_lands_in_the_column_and_not_in_extra():
    # A payload whose only extra-looking field is the now-recognized realtime flag
    # keeps an empty overflow.
    batch = journal.quotes_data_batch(
        {"bidPrice": 1.0, "askPrice": 1.1, "lastPrice": 1.05, "realtime": True},
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    )
    row = batch.to_pylist()[0]
    assert row["realtime"] is True
    assert row["extra"] is None


def test_quotes_unknown_field_lands_in_extra():
    quote = dict(QUOTE, netChange=1.5)
    batch = journal.quotes_data_batch(
        quote, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR
    )
    # The unknown field overflows. The recognized realtime and quoteTime do not.
    assert json.loads(batch.to_pylist()[0]["extra"]) == {"netChange": 1.5}


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
    # Every vendor column is null on a gap, the entitlement flag included. It is a
    # vendor column, not the provenance suspect bool.
    for column in (
        "bid",
        "ask",
        "last",
        "open_interest",
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
    # The entitlement flag is a vendor column, so it is null on a gap too.
    assert row["realtime"] is None


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


# -- path convention ---------------------------------------------------------


def test_segment_path_mirrors_the_fixture_lake_contract(tmp_path):
    root = tmp_path / "lake"
    fixture = FixtureLake(root)
    mine = journal.segment_path(root, "chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    theirs = fixture.segment_path("chains", "SPY", "2026-08-24", "20260824T160000", 4242)
    assert mine == theirs
