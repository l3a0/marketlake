"""The journal segment writer.

The capture loop cannot write one Parquet file per fetch. That would leave a lake
full of tiny files. Instead each cycle appends to a *journal segment*: an on-disk
file that a whole day of one ticker's cycles is written into, one record batch at a
time. A record batch is Apache Arrow's self-contained unit of rows. The segment
format is Arrow IPC, Arrow's append-friendly on-disk stream. A file torn mid-write
therefore stays readable up to its last complete batch. Parquet, by contrast, is
invalid until its footer lands at close.

Three terms recur, defined here at first use.

- A *surface* is one kind of measurement with its own pinned schema. This module
  writes two: ``chains`` (full option chains) and ``quotes`` (batched equity
  quotes). Arrow IPC fixes one schema per file, so the surface axis is load-bearing.
  One segment can never hold both.
- A *segment* is one Arrow IPC file, created by exactly one writer session and never
  re-opened for append. An Arrow IPC stream cannot be resumed by a later writer. A
  clean close writes an *end-of-stream* marker, the EOS, that readers stop at. Rows
  appended past an EOS are silently invisible to standard readers, so this module
  refuses to create such a shadow-append and fails loudly when it reads one.
- A *snap_ts* is the minute slot a cycle fired for, assigned by the loop at the top
  of the minute. It is neither the fetch time nor the vendor quote time. Every row
  carries all three.

This module owns three jobs: the pinned capture schema for each surface, the row
builders that turn a vendor payload into a record batch, and the writer that lands
batches durably. It never reads the wall clock. Every timestamp arrives as an
argument, stamped by the caller from the injected clock.
"""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa

# The schema version stamped on every row. The vendor columns' full list is fixed by
# the first day's payload and recorded as version 1. A later payload change mints a
# new version rather than mutating this one.
SCHEMA_VERSION = 1

# The two surfaces this module writes.
CHAINS_SURFACE = "chains"
QUOTES_SURFACE = "quotes"

# The two kinds of row. A ``data`` row carries a vendor observation. A ``gap`` row is
# the surface schema with all vendor columns null. It records a minute that was
# missed and why, so completeness is counted from rows and never inferred from holes.
ROW_KIND_DATA = "data"
ROW_KIND_GAP = "gap"

# The durability primitive. A cycle is durable only once its bytes reach stable
# storage, past the drive's own write cache. On macOS that needs
# ``fcntl(fd, F_FULLFSYNC)``. Plain ``fsync`` stops at the drive cache there, per
# Apple's ``fsync(2)`` man page. On Linux ``fsync`` already flushes the device cache
# on mainstream filesystems, so ``os.fsync`` is the equivalent, not a weaker
# stand-in. ``F_FULLFSYNC`` is macOS-specific only because Apple made ``fsync``
# weaker than POSIX. The daemon runs on the macOS laptop today. A future Linux host
# keeps real durability through the fallback.
F_FULLFSYNC = getattr(fcntl, "F_FULLFSYNC", None)


# -- pinned capture schemas --------------------------------------------------

# The provenance columns every row carries, in every surface. ``row_kind`` is
# ``data`` or ``gap``. ``error_class`` is null on data rows and names the reason on a
# gap. ``suspect`` flags a response the validation battery should judge. ``close_tag``
# is ``canonical``, ``spot_close``, or null, stamped on every row of a tagged cycle.
# ``session_phase`` tags rows observed after the equity close. ``extra`` is a
# normally-empty JSON overflow column. Any vendor field the schema does not name lands
# there, so vendor-verbatim stays structurally true even when a payload drifts.
_PROVENANCE_FIELDS = [
    ("row_kind", pa.string()),
    ("error_class", pa.string()),
    ("suspect", pa.bool_()),
    ("close_tag", pa.string()),
    ("session_phase", pa.string()),
    ("schema_version", pa.int64()),
    ("extra", pa.string()),
]

# The timestamps plus the ticker every row carries. ``snap_ts`` is the minute slot.
# ``fetch_ts`` and ``fetch_end_ts`` are a pair around the vendor call. ``fetch_ts`` is
# the dispatch time, the loop's clock just before the request starts. ``fetch_end_ts``
# is when the response or the failure landed, the request end. So the request
# round-trip is ``fetch_end_ts`` minus ``fetch_ts``, measurable per row, and even a
# timeout's duration is captured. ``vendor_quote_ts`` is Schwab's own quote time, and
# staleness is ``fetch_ts`` minus it. ``fetch_end_ts`` is nullable, so a row without it
# is still valid.
_STAMP_FIELDS = [
    ("snap_ts", pa.string()),
    ("fetch_ts", pa.string()),
    ("fetch_end_ts", pa.string()),
    ("vendor_quote_ts", pa.string()),
    ("ticker", pa.string()),
]

# The chains capture schema. Besides the stamps and provenance, each row is one
# contract. Every per-contract field Schwab returns lands in a typed column, the same
# way the quotes surface fully types its blocks, so ``extra`` stays empty in steady
# state and only a genuinely new vendor field drifts into it. ``volume`` is Schwab's
# ``totalVolume``. It is a typed column, not overflow, because the OI view's comparable
# set ranks contracts on volume, so it must be queryable. Two contract fields are not
# stored verbatim in a column of their own. ``quoteTimeInLong`` is the per-contract
# quote time, consumed into ``vendor_quote_ts`` below and not repeated as a column.
# ``optionDeliverablesList`` is a nested list, JSON-encoded into the string column
# ``option_deliverables_list`` so the non-standard-contract detection can read it back.
# The ``*_in_long`` and ``last_trading_day`` and ``trade_time`` columns hold Schwab's
# epoch-millisecond stamps verbatim, as int64. ``expiration_date`` is the vendor's ISO
# string, kept as a string, not an epoch. The chain-level fields are repeated on every
# contract row, per the raw-verbatim rule: ``interest_rate``, ``underlying_price``,
# ``dividend_yield``, ``is_delayed``, ``is_chain_truncated``, and
# ``number_of_contracts``. ``is_delayed`` is the vendor's real-time entitlement flag. It
# must be false on a real-time chain response. The validation battery checks it, so it
# is captured, not dropped. Raw stores what the vendor said. Nothing here is reshaped or
# validated. The names and types are calibrated against a real Schwab chain payload
# (live check 1 follow-up).
CHAINS_SCHEMA = pa.schema(
    _STAMP_FIELDS
    + [
        # identity and the call/put flag
        ("occ_symbol", pa.string()),
        ("put_call", pa.string()),
        # top of book and its sizes
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("bid_size", pa.int64()),
        ("ask_size", pa.int64()),
        ("last_size", pa.int64()),
        ("bid_ask_size", pa.string()),
        # open interest and the day's traded volume
        ("open_interest", pa.int64()),
        ("volume", pa.int64()),
        # the session prices and the mark
        ("open_price", pa.float64()),
        ("high_price", pa.float64()),
        ("low_price", pa.float64()),
        ("close_price", pa.float64()),
        ("mark", pa.float64()),
        ("mark_change", pa.float64()),
        ("mark_percent_change", pa.float64()),
        ("net_change", pa.float64()),
        ("percent_change", pa.float64()),
        # implied volatility and the five greeks
        ("volatility", pa.float64()),
        ("delta", pa.float64()),
        ("gamma", pa.float64()),
        ("theta", pa.float64()),
        ("vega", pa.float64()),
        ("rho", pa.float64()),
        # theoreticals and the value decomposition
        ("theoretical_option_value", pa.float64()),
        ("theoretical_volatility", pa.float64()),
        ("intrinsic_value", pa.float64()),
        ("extrinsic_value", pa.float64()),
        ("time_value", pa.float64()),
        ("break_even", pa.float64()),
        # the contract's 52-week range
        ("high_52_week", pa.float64()),
        ("low_52_week", pa.float64()),
        # contract terms
        ("strike_price", pa.float64()),
        ("multiplier", pa.float64()),
        ("days_to_expiration", pa.int64()),
        ("expiration_date", pa.string()),
        ("expiration_type", pa.string()),
        ("exercise_type", pa.string()),
        ("settlement_type", pa.string()),
        ("option_root", pa.string()),
        ("deliverable_note", pa.string()),
        ("description", pa.string()),
        ("exchange_name", pa.string()),
        ("option_deliverables_list", pa.string()),
        # the contract's classification flags
        ("in_the_money", pa.bool_()),
        ("non_standard", pa.bool_()),
        ("mini", pa.bool_()),
        ("penny_pilot", pa.bool_()),
        # vendor identifiers and the epoch-millisecond stamps
        ("ssid", pa.int64()),
        ("last_trading_day", pa.int64()),
        ("trade_time", pa.int64()),
        # the chain-level fields, repeated on every contract row
        ("interest_rate", pa.float64()),
        ("underlying_price", pa.float64()),
        ("dividend_yield", pa.float64()),
        ("is_delayed", pa.bool_()),
        ("is_chain_truncated", pa.bool_()),
        ("number_of_contracts", pa.int64()),
    ]
    + _PROVENANCE_FIELDS
)

# The quotes capture schema. Each row is one equity quote for one ticker. It captures the
# full ``quote`` block, the vendor's ``realtime`` entitlement flag, the ``cusip``, and the
# full ``fundamental``, ``regular``, and ``extended`` blocks as distinctly-named typed
# columns, all vendor-verbatim. With every documented field of every block typed, ``extra``
# is empty in steady state and only a genuinely new vendor field drifts into it.
# ``realtime`` must be true on a real-time quote; the battery checks it. ``cusip`` is
# Schwab's CUSIP, kept raw so a
# later enrichment can resolve the instrument's FIGI from it; it is never a join key.
# The ``fundamental`` block carries the dividend fields plus valuation and volume stats.
# ``div_pay_amount`` is the per-event amount, never ``div_amount``, the annualized
# trailing figure; the ``next_div_*`` fields are the vendor's undocumented projections.
# The ``regular`` block is the regular-session close, prefixed ``regular_market_*`` by
# Schwab. The ``extended`` block is the extended-hours session; its columns are prefixed
# ``extended_*`` because that block reuses ``lastPrice``/``bidPrice``/``askPrice``/
# ``quoteTime``, which would otherwise collide with the ``quote`` block. The vendor quote
# time is carried in ``vendor_quote_ts``, not repeated as a column.
#
# The field names and types are calibrated against the live cassette recording (live
# check 1). The per-block maps below are the single source of truth; an unrecognized
# field in a captured block fails open into ``extra`` under that block's key.
QUOTES_SCHEMA = pa.schema(
    _STAMP_FIELDS
    + [
        # quote block — the full documented field set, vendor-verbatim. ``quoteTime`` is
        # consumed into ``vendor_quote_ts`` and is not repeated here. The 52-week fields
        # are named ``week_52_*`` so they stay distinct from fundamental's ``high_52`` /
        # ``low_52``.
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("bid_size", pa.int64()),
        ("ask_size", pa.int64()),
        ("last_size", pa.int64()),
        ("bid_mic_id", pa.string()),
        ("ask_mic_id", pa.string()),
        ("last_mic_id", pa.string()),
        ("bid_time", pa.int64()),
        ("ask_time", pa.int64()),
        ("trade_time", pa.int64()),
        ("high_price", pa.float64()),
        ("low_price", pa.float64()),
        ("open_price", pa.float64()),
        ("close_price", pa.float64()),
        ("mark", pa.float64()),
        ("mark_change", pa.float64()),
        ("mark_percent_change", pa.float64()),
        ("net_change", pa.float64()),
        ("net_percent_change", pa.float64()),
        ("post_market_change", pa.float64()),
        ("post_market_percent_change", pa.float64()),
        ("total_volume", pa.int64()),
        ("volatility", pa.float64()),
        ("week_52_high", pa.float64()),
        ("week_52_low", pa.float64()),
        ("security_status", pa.string()),
        # envelope-level
        ("realtime", pa.bool_()),
        ("cusip", pa.string()),
        # fundamental block — dividends
        ("div_pay_amount", pa.float64()),
        ("div_ex_date", pa.string()),
        ("div_amount", pa.float64()),
        ("div_freq", pa.int64()),
        ("declaration_date", pa.string()),
        ("next_div_ex_date", pa.string()),
        ("next_div_pay_date", pa.string()),
        ("div_pay_date", pa.string()),
        ("div_yield", pa.float64()),
        # fundamental block — valuation and volume stats
        ("pe_ratio", pa.float64()),
        ("eps", pa.float64()),
        ("high_52", pa.float64()),
        ("low_52", pa.float64()),
        ("avg_10_days_volume", pa.float64()),
        ("avg_1_year_volume", pa.float64()),
        ("last_earnings_date", pa.string()),
        ("fund_leverage_factor", pa.float64()),
        ("shares_outstanding", pa.int64()),
        # regular block — the regular-session close
        ("regular_market_last_price", pa.float64()),
        ("regular_market_last_size", pa.int64()),
        ("regular_market_net_change", pa.float64()),
        ("regular_market_percent_change", pa.float64()),
        ("regular_market_trade_time", pa.int64()),
        # extended block — extended-hours session, prefixed to avoid the quote collision
        ("extended_last_price", pa.float64()),
        ("extended_bid_price", pa.float64()),
        ("extended_ask_price", pa.float64()),
        ("extended_bid_size", pa.int64()),
        ("extended_ask_size", pa.int64()),
        ("extended_last_size", pa.int64()),
        ("extended_mark", pa.float64()),
        ("extended_quote_time", pa.int64()),
        ("extended_trade_time", pa.int64()),
        ("extended_total_volume", pa.int64()),
    ]
    + _PROVENANCE_FIELDS
)

_SCHEMAS = {CHAINS_SURFACE: CHAINS_SCHEMA, QUOTES_SURFACE: QUOTES_SCHEMA}


def schema_for(surface: str) -> pa.Schema:
    """The pinned capture schema for a surface. Unknown surfaces raise loudly."""
    try:
        return _SCHEMAS[surface]
    except KeyError:
        raise ValueError(f"unknown surface {surface!r}") from None


# -- vendor field maps -------------------------------------------------------

# The per-contract vendor fields that land in typed chains columns, each stored
# verbatim under a snake_case name. The vendor speaks camelCase. Only the names change,
# never the values. Two per-contract fields are handled outside this map because their
# value is transformed, not copied: ``quoteTimeInLong`` is consumed into
# ``vendor_quote_ts`` (see ``_CHAINS_CONTRACT_CONSUMED``), and ``optionDeliverablesList``
# is JSON-encoded into a string column (see ``_CHAINS_DELIVERABLES_FIELD``). Both are
# still counted as known below, so neither overflows into ``extra``.
_CHAINS_CONTRACT_MAP = {
    "symbol": "occ_symbol",
    "putCall": "put_call",
    "bid": "bid",
    "ask": "ask",
    "last": "last",
    "bidSize": "bid_size",
    "askSize": "ask_size",
    "lastSize": "last_size",
    "bidAskSize": "bid_ask_size",
    "openInterest": "open_interest",
    "totalVolume": "volume",
    "openPrice": "open_price",
    "highPrice": "high_price",
    "lowPrice": "low_price",
    "closePrice": "close_price",
    "mark": "mark",
    "markChange": "mark_change",
    "markPercentChange": "mark_percent_change",
    "netChange": "net_change",
    "percentChange": "percent_change",
    "volatility": "volatility",
    "delta": "delta",
    "gamma": "gamma",
    "theta": "theta",
    "vega": "vega",
    "rho": "rho",
    "theoreticalOptionValue": "theoretical_option_value",
    "theoreticalVolatility": "theoretical_volatility",
    "intrinsicValue": "intrinsic_value",
    "extrinsicValue": "extrinsic_value",
    "timeValue": "time_value",
    "breakEven": "break_even",
    "high52Week": "high_52_week",
    "low52Week": "low_52_week",
    "strikePrice": "strike_price",
    "multiplier": "multiplier",
    "daysToExpiration": "days_to_expiration",
    "expirationDate": "expiration_date",
    "expirationType": "expiration_type",
    "exerciseType": "exercise_type",
    "settlementType": "settlement_type",
    "optionRoot": "option_root",
    "deliverableNote": "deliverable_note",
    "description": "description",
    "exchangeName": "exchange_name",
    "inTheMoney": "in_the_money",
    "nonStandard": "non_standard",
    "mini": "mini",
    "pennyPilot": "penny_pilot",
    "ssid": "ssid",
    "lastTradingDay": "last_trading_day",
    "tradeTimeInLong": "trade_time",
}

# The per-contract quote time. It is an epoch-millisecond int on each contract. It is
# consumed into that contract's ``vendor_quote_ts`` stamp, not stored as a column, so it
# never lands in ``extra`` either. This mirrors how the quotes surface consumes
# ``quote.quoteTime``.
_CHAINS_QUOTE_TS_FIELD = "quoteTimeInLong"
_CHAINS_CONTRACT_CONSUMED = frozenset({_CHAINS_QUOTE_TS_FIELD})

# The nested deliverables list. Schwab returns it as a list of dicts. It is JSON-encoded
# into a single string column, because the design's non-standard-contract detection reads
# it back and Arrow columns hold no free-form nested list here.
_CHAINS_DELIVERABLES_FIELD = "optionDeliverablesList"
_CHAINS_DELIVERABLES_COLUMN = "option_deliverables_list"

# Every per-contract field the parser recognizes. A field outside this set overflows
# into ``extra``. The mapped fields, the consumed quote time, and the JSON-encoded
# deliverables list are all known, so a fully-populated contract leaves ``extra`` empty.
_CHAINS_CONTRACT_KNOWN = (
    set(_CHAINS_CONTRACT_MAP) | _CHAINS_CONTRACT_CONSUMED | {_CHAINS_DELIVERABLES_FIELD}
)

# The chain-level fields promoted to columns on every contract row. The entitlement flag
# and the two truncation-and-count fields are recognized here so they are captured, not
# dropped, and never mistaken for an unknown field. Every other top-level body field
# (strategy, interval, isIndex, and the rest) is neither per-contract nor captured.
_CHAINS_HEADER_MAP = {
    "interestRate": "interest_rate",
    "underlyingPrice": "underlying_price",
    "dividendYield": "dividend_yield",
    "isDelayed": "is_delayed",
    "isChainTruncated": "is_chain_truncated",
    "numberOfContracts": "number_of_contracts",
}

# The per-block quote field maps, each vendor-field to snake_case column. Each map is
# applied against its OWN block, never a merged dict, because the ``quote`` and
# ``extended`` blocks reuse field names (``lastPrice``, ``bidPrice``, ``askPrice``,
# ``quoteTime``). Applying each block's map to that block alone keeps the colliding names
# in separate columns. These maps are the single source of truth for what lands typed;
# an unrecognized field in any block fails open into ``extra`` under that block's key.
# The names and types are calibrated against the live cassette recording (live check 1).

# The ``quote`` block, fully typed. ``quoteTime`` is consumed into ``vendor_quote_ts``
# below, not made a column. The 52-week fields use ``week_52_*`` column names so they do
# not collide with fundamental's ``high_52`` / ``low_52``. The time fields
# (``bid_time``, ``ask_time``, ``trade_time``) are int64: the live cassette recording
# (live check 1) confirmed Schwab returns them as epoch milliseconds, stored verbatim.
_QUOTE_MAP = {
    "bidPrice": "bid",
    "askPrice": "ask",
    "lastPrice": "last",
    "bidSize": "bid_size",
    "askSize": "ask_size",
    "lastSize": "last_size",
    "bidMICId": "bid_mic_id",
    "askMICId": "ask_mic_id",
    "lastMICId": "last_mic_id",
    "bidTime": "bid_time",
    "askTime": "ask_time",
    "tradeTime": "trade_time",
    "highPrice": "high_price",
    "lowPrice": "low_price",
    "openPrice": "open_price",
    "closePrice": "close_price",
    "mark": "mark",
    "markChange": "mark_change",
    "markPercentChange": "mark_percent_change",
    "netChange": "net_change",
    "netPercentChange": "net_percent_change",
    "postMarketChange": "post_market_change",
    "postMarketPercentChange": "post_market_percent_change",
    "totalVolume": "total_volume",
    "volatility": "volatility",
    "52WeekHigh": "week_52_high",
    "52WeekLow": "week_52_low",
    "securityStatus": "security_status",
}

# The ``fundamental`` block: the dividend fields plus valuation and volume stats.
_FUNDAMENTAL_MAP = {
    "divPayAmount": "div_pay_amount",
    "divExDate": "div_ex_date",
    "divAmount": "div_amount",
    "divFreq": "div_freq",
    "declarationDate": "declaration_date",
    "nextDivExDate": "next_div_ex_date",
    "nextDivPayDate": "next_div_pay_date",
    "divPayDate": "div_pay_date",
    "divYield": "div_yield",
    "peRatio": "pe_ratio",
    "eps": "eps",
    "high52": "high_52",
    "low52": "low_52",
    "avg10DaysVolume": "avg_10_days_volume",
    "avg1YearVolume": "avg_1_year_volume",
    "lastEarningsDate": "last_earnings_date",
    "fundLeverageFactor": "fund_leverage_factor",
    "sharesOutstanding": "shares_outstanding",
}

# The ``regular`` block: the regular-session close. Schwab already prefixes these
# ``regularMarket*``, so they do not collide with the ``quote`` block.
_REGULAR_MAP = {
    "regularMarketLastPrice": "regular_market_last_price",
    "regularMarketLastSize": "regular_market_last_size",
    "regularMarketNetChange": "regular_market_net_change",
    "regularMarketPercentChange": "regular_market_percent_change",
    "regularMarketTradeTime": "regular_market_trade_time",
}

# The ``extended`` block: the extended-hours session. Its field names collide with the
# ``quote`` block, so every column is prefixed ``extended_``.
_EXTENDED_MAP = {
    "lastPrice": "extended_last_price",
    "bidPrice": "extended_bid_price",
    "askPrice": "extended_ask_price",
    "bidSize": "extended_bid_size",
    "askSize": "extended_ask_size",
    "lastSize": "extended_last_size",
    "mark": "extended_mark",
    "quoteTime": "extended_quote_time",
    "tradeTime": "extended_trade_time",
    "totalVolume": "extended_total_volume",
}

# The quote-block fields the parser recognizes but does not overflow into ``extra``. The
# vendor quote time is carried in ``vendor_quote_ts`` instead, so keeping it out of the
# overflow keeps that column normally empty.
_QUOTE_CONSUMED = frozenset({"quoteTime"})

# The captured blocks, each paired with its map and its consumed set. A block is
# projected against its own sub-dict; unrecognized fields overflow into ``extra`` under
# the block's key. Blocks not listed here (like ``reference`` beyond the CUSIP, or the
# envelope's own ``assetMainType``) are neither captured nor overflowed.
_QUOTE_BLOCK_SPECS = (
    ("quote", _QUOTE_MAP, _QUOTE_CONSUMED),
    ("fundamental", _FUNDAMENTAL_MAP, frozenset()),
    ("regular", _REGULAR_MAP, frozenset()),
    ("extended", _EXTENDED_MAP, frozenset()),
)


# -- row building ------------------------------------------------------------


def _iso(value: str | datetime | date | None) -> str | None:
    """Normalize a timestamp argument to an ISO-8601 string, or ``None``.

    A string passes through. A ``datetime`` or ``date`` is formatted. The caller
    stamps these from the injected clock, so this module never reads the wall clock.
    """
    if value is None or isinstance(value, str):
        return value
    return value.isoformat()


def _epoch_ms_to_iso(value: object) -> str | None:
    """A vendor epoch-millisecond stamp as a UTC ISO-8601 string, or ``None``.

    Schwab stamps a contract's quote time as ``quoteTimeInLong``, an epoch in
    milliseconds. Converting a stored epoch to a datetime is deterministic and reads no
    wall clock, the same move ``lake.capture`` makes for the quote surface's quote time.
    A missing value returns ``None``.
    """
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC).isoformat()


def _extra_json(fields: Mapping[str, object], known: set[str]) -> str | None:
    """JSON for the vendor fields the schema does not name, or ``None`` when empty.

    Keys are sorted so the same overflow always serializes identically.
    """
    overflow = {key: value for key, value in fields.items() if key not in known}
    if not overflow:
        return None
    return json.dumps(overflow, sort_keys=True)


def _batch(schema: pa.Schema, rows: Sequence[Mapping[str, object]]) -> pa.RecordBatch:
    """Build one record batch from row mappings, typed by the schema.

    A missing key becomes null. Each column is built with its schema type, so an
    all-null column still lands with the right type instead of guessing.
    """
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _iter_contracts(body: Mapping[str, object]) -> list[Mapping[str, object]]:
    """Every contract dict in a chain body, calls first then puts, in payload order.

    The chain body nests contracts as ``expDateMap[expiration][strike] -> [contract]``.
    This walks both the call and put maps and flattens them.
    """
    contracts: list[Mapping[str, object]] = []
    for map_key in ("callExpDateMap", "putExpDateMap"):
        exp_map = body.get(map_key) or {}
        for strikes in exp_map.values():
            for contract_list in strikes.values():
                contracts.extend(contract_list)
    return contracts


def chains_data_batch(
    body: Mapping[str, object],
    *,
    ticker: str,
    snap_ts: str | datetime,
    fetch_ts: str | datetime,
    fetch_end_ts: str | datetime | None = None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
    absent_expirations: Sequence[str] = (),
    absent_error_class: str | None = None,
) -> pa.RecordBatch:
    """Build a chains data batch from one chain response body.

    One row per contract. Known contract fields land in typed columns. Any unrecognized
    contract field lands in ``extra`` as JSON. The parser fails open, so a new vendor
    field never drops a cycle.

    ``vendor_quote_ts`` is per contract, derived from that contract's ``quoteTimeInLong``
    epoch-millisecond stamp. The top-level ``underlying`` block is null on a real Schwab
    chain even when the underlying quote is requested, so there is no single chain-level
    quote time to stamp. The underlying price for IV inversion still arrives, in the
    top-level ``underlyingPrice`` scalar, captured through the header map. The
    ``optionDeliverablesList`` is JSON-encoded into ``option_deliverables_list``.

    The chain-level header fields repeat on every row: the rates, the underlying price,
    the entitlement flag, and the truncation-and-count fields. The truncation flag and the
    contract count are recomputed from the reassembled rows, never read from ``body``. So a
    chunked chain reports its own captured contract count and whether any expiration was
    given up, not the strike_count=1 discovery probe's figures. The other timestamps are
    the caller's, stamped from the injected clock. ``fetch_end_ts`` is when the response
    landed, the request end, so the round-trip is ``fetch_end_ts`` minus ``fetch_ts``.

    ``absent_expirations`` supports the capture chunker's partial snapshot. When a chain
    is fetched in expiration chunks and one chunk fails past every retry, that
    expiration's contracts are absent from ``body``. Rather than lose the whole chain to a
    gap, the chunker names those expirations here. Each becomes one gap-marker row in this
    same batch: ``row_kind`` gap, ``error_class`` = ``absent_error_class``, its
    ``expiration_date`` set so the missing expiration is queryable, and every other vendor
    column null. So one segment carries the captured contracts and the absence markers
    together. The default empty sequence is the ordinary whole-chain case.
    """
    header = {column: body.get(vendor) for vendor, column in _CHAINS_HEADER_MAP.items()}
    stamps = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "ticker": ticker,
        "row_kind": ROW_KIND_DATA,
        "error_class": None,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
    }
    rows: list[dict[str, object]] = []
    for contract in _iter_contracts(body):
        row: dict[str, object] = dict(stamps)
        row.update(header)
        row["vendor_quote_ts"] = _epoch_ms_to_iso(contract.get(_CHAINS_QUOTE_TS_FIELD))
        for vendor, column in _CHAINS_CONTRACT_MAP.items():
            if vendor in contract:
                row[column] = contract[vendor]
        deliverables = contract.get(_CHAINS_DELIVERABLES_FIELD)
        if deliverables is not None:
            row[_CHAINS_DELIVERABLES_COLUMN] = json.dumps(deliverables, sort_keys=True)
        row["extra"] = _extra_json(contract, _CHAINS_CONTRACT_KNOWN)
        rows.append(row)
    # number_of_contracts and is_chain_truncated describe the whole stored chain, not any
    # single response, so recompute them from the reassembled rows. The count is the
    # contract rows actually captured. Truncation is the vendor's own flag on a one-shot
    # chain, or, on a chunked chain, whether any expiration was given up and marked absent
    # below. Both override the header value, which on a chunked chain came from the
    # strike_count=1 discovery probe and describes only that probe.
    contract_count = len(rows)
    truncated = bool(header["is_chain_truncated"]) or bool(absent_expirations)
    for row in rows:
        row["number_of_contracts"] = contract_count
        row["is_chain_truncated"] = truncated
    for expiration in absent_expirations:
        # A gap-marker for one absent expiration. Every vendor column stays null except
        # expiration_date, which names the missing expiration. suspect and close_tag ride
        # the same values as the data rows so the whole snapshot tags consistently.
        rows.append(
            {
                "snap_ts": _iso(snap_ts),
                "fetch_ts": _iso(fetch_ts),
                "fetch_end_ts": _iso(fetch_end_ts),
                "vendor_quote_ts": None,
                "ticker": ticker,
                "row_kind": ROW_KIND_GAP,
                "error_class": absent_error_class,
                "suspect": suspect,
                "close_tag": close_tag,
                "session_phase": session_phase,
                "schema_version": SCHEMA_VERSION,
                "expiration_date": expiration,
                "extra": None,
            }
        )
    return _batch(CHAINS_SCHEMA, rows)


def quote_cusip(envelope: Mapping[str, object]) -> object | None:
    """Schwab's CUSIP for one quote envelope, or ``None`` if absent.

    The CUSIP is a sibling of the ``quote`` block on the per-symbol envelope, not inside
    it. Schwab places it either as a top-level ``cusip`` field or inside a ``reference``
    block, so this checks both. It is captured raw under the vendor-verbatim rule so a
    later enrichment can resolve the instrument's FIGI from it. It is never a join key.
    """
    if "cusip" in envelope:
        return envelope["cusip"]
    reference = envelope.get("reference")
    if isinstance(reference, Mapping):
        return reference.get("cusip")
    return None


def _project_quote_envelope(envelope: Mapping[str, object]) -> tuple[dict[str, object], str | None]:
    """Project one per-symbol quote envelope into typed columns plus namespaced overflow.

    Each captured block is projected against its own sub-dict through its own map, so the
    colliding names in the ``quote`` and ``extended`` blocks (``lastPrice`` and friends)
    land in separate columns. A field a block's map does not name, and that the block does
    not consume, overflows into ``extra`` under that block's key, so drift in any block
    surfaces without key collision and ``extra`` stays empty in steady state. The
    envelope-level ``realtime`` flag and the ``cusip`` are captured too. Anything else on
    the envelope, like ``assetMainType`` or the ``reference`` block beyond the CUSIP, is
    neither captured nor overflowed.
    """
    columns: dict[str, object] = {}
    overflow: dict[str, dict[str, object]] = {}
    for block_key, field_map, consumed in _QUOTE_BLOCK_SPECS:
        block = envelope.get(block_key)
        if not isinstance(block, Mapping):
            continue
        for vendor, column in field_map.items():
            if vendor in block:
                columns[column] = block[vendor]
        known = set(field_map) | consumed
        rest = {key: value for key, value in block.items() if key not in known}
        if rest:
            overflow[block_key] = rest
    if "realtime" in envelope:
        columns["realtime"] = envelope["realtime"]
    cusip = quote_cusip(envelope)
    if cusip is not None:
        columns["cusip"] = cusip
    extra = json.dumps(overflow, sort_keys=True) if overflow else None
    return columns, extra


def quotes_data_batch(
    envelope: Mapping[str, object],
    *,
    ticker: str,
    snap_ts: str | datetime,
    fetch_ts: str | datetime,
    fetch_end_ts: str | datetime | None = None,
    vendor_quote_ts: str | datetime | None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build a one-row quotes batch from one ticker's per-symbol quote envelope.

    The batched quote sampler splits the vendor's response per ticker and hands this
    builder that ticker's whole envelope: the ``quote``, ``fundamental``, ``regular``,
    and ``extended`` blocks, the ``realtime`` flag, and the CUSIP. Each block is projected
    through its own map into distinctly-named columns, so a name shared by the ``quote``
    and ``extended`` blocks lands in both its columns, never overwriting the other. The
    quote time is carried in ``vendor_quote_ts``. A field a captured block's map does not
    name overflows into ``extra`` under that block's key, so ``extra`` stays empty in
    steady state and drift in any block still surfaces. ``fetch_end_ts`` is the
    request-end stamp, the pair to ``fetch_ts`` for round-trip.
    """
    row: dict[str, object] = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "vendor_quote_ts": _iso(vendor_quote_ts),
        "ticker": ticker,
        "row_kind": ROW_KIND_DATA,
        "error_class": None,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
    }
    columns, extra = _project_quote_envelope(envelope)
    row.update(columns)
    row["extra"] = extra
    return _batch(QUOTES_SCHEMA, [row])


def gap_batch(
    surface: str,
    *,
    ticker: str,
    snap_ts: str | datetime,
    error_class: str,
    fetch_ts: str | datetime | None = None,
    fetch_end_ts: str | datetime | None = None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build a one-row gap batch for a missed minute.

    A gap row is the surface schema with every vendor column null. It carries its
    missed minute in ``snap_ts`` and the reason in ``error_class``. It holds no market
    data. The missed sample is gone forever. The row records the absence so a hole is
    never inferred. ``fetch_ts`` is optional, since a gap for a dead daemon has no
    fetch at all. ``fetch_end_ts`` is optional too, and set for a failed fetch so the
    round-trip to the failure is measurable. There is no vendor quote, so
    ``vendor_quote_ts`` is always null. Every vendor column is null on a gap by
    construction: the row sets only the stamps and provenance below, and the batch
    builder fills each schema column the row omits with null. So the prices, greeks,
    ``open_interest``, ``volume``, ``realtime``, ``cusip``, and the whole fundamental,
    regular, and extended blocks are all null, per surface, without being enumerated here.
    """
    schema = schema_for(surface)
    row: dict[str, object] = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "vendor_quote_ts": None,
        "ticker": ticker,
        "row_kind": ROW_KIND_GAP,
        "error_class": error_class,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
        "extra": None,
    }
    return _batch(schema, [row])


# -- segment paths -----------------------------------------------------------


def _day_str(day: date | str) -> str:
    return day.isoformat() if isinstance(day, date) else str(day)


def segment_path(
    lake_root: Path | str,
    surface: str,
    ticker: str,
    day: date | str,
    start_ts: str,
    pid: int,
) -> Path:
    """The on-disk path for a segment. It mirrors the fixture-lake convention exactly.

    The path is ``journal/date=D/surface=S/ticker=T/seg-<start_ts>-<pid>.arrows``.
    The ``start_ts`` is a second-or-finer stamp of when the writer session began. The
    ``pid`` is the writer process id. Together they make the name unique per writer
    session, so a manual re-run or a crash-loop restart cannot collide with a live
    segment by accident.
    """
    return (
        Path(lake_root)
        / "journal"
        / f"date={_day_str(day)}"
        / f"surface={surface}"
        / f"ticker={ticker}"
        / f"seg-{start_ts}-{pid}.arrows"
    )


# -- the writer --------------------------------------------------------------


class SegmentWriter:
    """A one-session writer for a single Arrow IPC segment.

    It creates the file exclusively, appends one record batch per cycle, and makes
    each cycle durable before returning. A clean close writes the end-of-stream
    marker. The segment is never re-opened for append. Use it as a context manager so
    the marker always lands.
    """

    def __init__(self, path: Path | str, schema: pa.Schema, *, surface: str | None = None) -> None:
        self.path = Path(path)
        self.schema = schema
        self.surface = surface
        # Count the durability flushes. Each cycle adds one, and a clean close adds
        # one more for the end-of-stream marker. Tests read this to prove the
        # per-cycle durability contract holds.
        self.durable_syncs = 0
        self._closed = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # ``O_CREAT | O_EXCL`` makes the create fail loudly if the path already exists.
        # A collision must never truncate durable rows or shadow-append past an EOS.
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        self._fd = fd
        # Make the new segment's directory entry durable, so the file itself survives
        # a crash right after creation. This is the standard directory fsync. It uses
        # plain ``os.fsync`` even on macOS, where ``F_FULLFSYNC`` does not apply to a
        # directory, matching how SQLite and Postgres persist directory entries.
        dir_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        # Unbuffered, so every write reaches the OS before the durability flush.
        self._file = open(fd, "wb", buffering=0, closefd=True)
        self._sink = pa.PythonFile(self._file, mode="w")
        self._writer = pa.ipc.new_stream(self._sink, schema)

    @classmethod
    def open(
        cls,
        lake_root: Path | str,
        surface: str,
        ticker: str,
        day: date | str,
        start_ts: str,
        pid: int,
    ) -> SegmentWriter:
        """Open a fresh segment from a lake root and the segment's coordinates.

        The path comes from ``segment_path`` and the schema from the surface. The
        ``pid`` is the caller's, typically ``os.getpid()``.
        """
        return cls(
            segment_path(lake_root, surface, ticker, day, start_ts, pid),
            schema_for(surface),
            surface=surface,
        )

    def write_cycle(self, batch: pa.RecordBatch | pa.Table) -> None:
        """Append one cycle's batch and make it durable.

        The batch's schema must match the segment's, which Arrow enforces. So a quotes
        batch can never land in a chains segment. Durability is the design's success
        point. A cycle counts as captured only after this returns.
        """
        if self._closed:
            raise ValueError("cannot write to a closed segment")
        if isinstance(batch, pa.Table):
            self._writer.write_table(batch)
        else:
            self._writer.write_batch(batch)
        self._flush_durable()

    def _flush_durable(self) -> None:
        self._sink.flush()
        if F_FULLFSYNC is not None:
            fcntl.fcntl(self._fd, F_FULLFSYNC)
        else:  # pragma: no cover - non-macOS path, exercised on Linux CI, not the dev Mac
            os.fsync(self._fd)
        self.durable_syncs += 1

    def close(self) -> None:
        """Write the end-of-stream marker, make it durable, and close the file."""
        if self._closed:
            return
        self._writer.close()  # writes the EOS marker
        self._flush_durable()
        self._file.close()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> SegmentWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# -- the reader --------------------------------------------------------------


class ShadowAppendError(Exception):
    """Raised when bytes follow a segment's end-of-stream marker.

    That is the shadow-append signature. A later writer opened a closed segment and
    appended past the EOS. Standard readers stop at the EOS and never see those rows,
    so the write silently loses data. The reader refuses to bless such a file.
    """

    def __init__(self, path: Path, consumed: int, size: int) -> None:
        self.path = path
        self.consumed = consumed
        self.size = size
        super().__init__(
            f"{path}: {size - consumed} bytes follow the end-of-stream marker (shadow-append)"
        )


def read_segment(path: Path | str) -> pa.Table:
    """Read a segment to its last complete record batch.

    Three cases, matching the design's durability model.

    1. A cleanly closed segment reads fully, up to its end-of-stream marker.
    2. A torn tail, from a power loss mid-append, reads up to the last complete batch.
       The incomplete trailing bytes are dropped, not an error. Every durable cycle
       ends in a full flush, so the last complete batch is exactly what the writer
       believed it had.
    3. Bytes after the end-of-stream marker are a shadow-append and raise loudly.
    """
    path = Path(path)
    with pa.memory_map(str(path), "rb") as source:
        reader = pa.ipc.open_stream(source)
        schema = reader.schema
        batches: list[pa.RecordBatch] = []
        clean_eos = False
        while True:
            try:
                batches.append(reader.read_next_batch())
            except StopIteration:
                clean_eos = True
                break
            except (pa.ArrowInvalid, OSError):
                # A torn tail: the next message is incomplete. Stop at the last
                # complete batch and keep what is durable.
                break
        if clean_eos and source.tell() < source.size():
            raise ShadowAppendError(path, source.tell(), source.size())
    return pa.Table.from_batches(batches, schema=schema)
