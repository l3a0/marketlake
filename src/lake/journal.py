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
from collections import Counter
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa

from lake.manifest import read_manifest
from lake.paths import LakePaths, parse_segment_rel

# The schema version stamped on every row. The vendor columns' full list is fixed by
# the first day's payload and recorded as version 1. A later payload change mints a
# new version rather than mutating this one.
#
# This constant stays hand-set rather than derived from the column set. A version has to
# be orderable, because a reader asks whether a row sits below the version that promoted
# a field out of ``extra`` into its own column, and a derived digest answers no such
# question. A change that leaves the shape alone also needs a version, as when the vendor
# keeps ``open_interest`` an int64 and changes what it counts. Only a human knows that.
# What is derived instead is the shape, via ``schema_fingerprint`` below, and
# ``tests/unit/test_schema_fingerprint.py`` compares the two. So a column added, dropped,
# or retyped without a bump fails the suite.
SCHEMA_VERSION = 1

# The two surfaces this module writes.
CHAINS_SURFACE = "chains"
QUOTES_SURFACE = "quotes"

# The two kinds of row. A ``data`` row carries a vendor observation. A ``gap`` row is
# the surface schema with all vendor columns null. It records a minute that was
# missed and why, so completeness is counted from rows and never inferred from holes.
ROW_KIND_COLUMN = "row_kind"
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
# is ``option_close``, ``spot_close``, or null, stamped on every row of a tagged cycle.
# ``session_phase`` tags rows observed after the equity close. ``extra`` is a
# normally-empty JSON overflow column. Any vendor field the schema does not name lands
# there, so vendor-verbatim stays structurally true even when a payload drifts. A known
# field whose value its column refuses lands there too, through ``_routed_column``, and
# because the overflow otherwise holds only fields the maps do not name, a known field's
# name appearing in ``extra`` says exactly that. The name is a constant because the row
# builders, the routing, and ``lake.extra_projection`` all reach for that column by name,
# and one spelling is what keeps them reaching for the same one.
EXTRA_COLUMN = "extra"

_PROVENANCE_FIELDS = [
    ("row_kind", pa.string()),
    ("error_class", pa.string()),
    ("suspect", pa.bool_()),
    ("close_tag", pa.string()),
    ("session_phase", pa.string()),
    ("schema_version", pa.int64()),
    (EXTRA_COLUMN, pa.string()),
]

# Two more provenance columns the chains surface alone carries: the date window that
# fetched the row. ``window_start`` and ``window_end`` are the plan window's ISO dates, the
# end null on the open tail. They are fetch provenance, not vendor fields, so they never
# count as a vendor column and never leak into ``extra``. A data row carries the plan window
# whose range holds its expiration. An absence marker carries the failed range. The nightly
# re-tune groups the day's rows by these two columns to re-size the plan.
_CHAINS_WINDOW_FIELDS = [
    ("window_start", pa.string()),
    ("window_end", pa.string()),
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
    + _CHAINS_WINDOW_FIELDS
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

# The surfaces this module pins a capture schema for. ``paths.SURFACES`` is wider,
# because the lake lays out directories for surfaces nothing captures yet. Deriving this
# from the schema map keeps a third surface from being pinned in one place and forgotten
# in the other.
PINNED_SURFACES = tuple(_SCHEMAS)


def schema_for(surface: str) -> pa.Schema:
    """The pinned capture schema for a surface. Unknown surfaces raise loudly."""
    try:
        return _SCHEMAS[surface]
    except KeyError:
        raise ValueError(f"unknown surface {surface!r}") from None


def schema_fingerprint(surface: str) -> dict[str, str]:
    """The surface's pinned schema reduced to a column-name-to-type mapping.

    This is what makes ``SCHEMA_VERSION`` mean something. The version is the only
    provenance that survives a seal: compaction unlinks a ticker-day's segments once the
    partition is manifested, so after that moment the per-segment schemas are gone and the
    integer stamped on every row is the sole record of which code shape wrote it. That
    matters most for a dropped column, which has no repair, because the values were never
    written. Reading the shape off the schema rather than restating it by hand is what
    keeps the record from going stale.

    Types render through pyarrow, so ``pa.float64()`` reads as ``double``. A retyped
    column therefore moves the fingerprint the same way a dropped one does.

    The result is a mapping rather than a sequence, which leaves column order out on
    purpose. What the fingerprint records is which values a version captured, and a
    reorder captures the same values. A null on a reordered row means exactly what it
    meant before, so a reorder mints no version and moving one column past another should
    not fail the suite. Whether a reorder is safe to *ship* is a separate question with a
    separate answer. ``compact`` merges segments with ``promote_options="default"``, the
    dashboard's multi-day read uses DuckDB's ``union_by_name``, and the dashboard's
    per-segment read projects each segment into a fixed schema before it concatenates, so
    all three absorb one. ``measure.read_surface_cycles`` calls ``pa.concat_tables`` plain
    and refuses a reordered schema outright. That gap is marketlake #143 and not this
    function's to catch.

    The two reference tables that carry a version of their own, the security master and
    the capture spans, are deliberately out of scope. Neither file is ever unlinked, so
    each still carries the schema it was written with, and both refuse at read time a file
    whose version they do not recognise. The journal differs only because compaction
    unlinks its segments.

    Unknown surfaces raise loudly, through ``schema_for``.
    """
    return {field.name: str(field.type) for field in schema_for(surface)}


class FingerprintDiff(NamedTuple):
    """What moved between a recorded fingerprint and the one the code derives now.

    ``dropped`` names the columns the record has and the code no longer does, ``added``
    the ones the code has and the record does not, and ``retyped`` the columns both hold
    under different types, each as ``(name, recorded_type, derived_type)``. Every tuple is
    sorted, so a message built from one reads the same on every run.
    """

    dropped: tuple[str, ...]
    added: tuple[str, ...]
    retyped: tuple[tuple[str, str, str], ...]

    @property
    def moved(self) -> bool:
        """Whether anything moved at all."""
        return bool(self.dropped or self.added or self.retyped)


def fingerprint_diff(derived: Mapping[str, str], recorded: Mapping[str, str]) -> FingerprintDiff:
    """Compare a derived fingerprint against a recorded one and name what moved.

    Two callers ask this same question of the same pair, and what counts as a change is
    defined here once rather than in each of them. The suite compares every surface's
    derived shape against the shape recorded for the current ``SCHEMA_VERSION``. The
    schema-version ledger compares the same derived shape against what the lake already
    holds for that version. Each writes its own message, because the fix each names is
    its own, and both read the difference off this.

    Order is not compared, for the reason ``schema_fingerprint`` returns a mapping: a
    reorder captures the same values, so it moves nothing.
    """
    dropped = tuple(sorted(set(recorded) - set(derived)))
    added = tuple(sorted(set(derived) - set(recorded)))
    retyped = tuple(
        sorted(
            (name, recorded[name], derived[name])
            for name in set(recorded) & set(derived)
            if recorded[name] != derived[name]
        )
    )
    return FingerprintDiff(dropped, added, retyped)


# -- vendor field maps -------------------------------------------------------

# The per-contract vendor fields that land in typed chains columns, each stored
# verbatim under a snake_case name. The vendor speaks camelCase. Only the names change,
# never the values. Two per-contract fields are handled outside this map because their
# value is transformed, not copied: ``quoteTimeInLong`` is consumed into
# ``vendor_quote_ts`` (see ``_CHAINS_CONTRACT_CONSUMED``), and ``optionDeliverablesList``
# is JSON-encoded into a string column (see ``_CHAINS_DELIVERABLES_FIELD``). Both are
# still counted as known below, so on an ordinary row neither overflows into ``extra``. A
# quote time the epoch transform refuses is the one exception, and it overflows because a
# refused transform consumed nothing.
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
# consumed into that contract's ``vendor_quote_ts`` stamp, not stored as a column, so on
# an ordinary row it does not land in ``extra`` either. Consumed means successfully
# transformed, so a row whose stamp the transform refused overflows it like any other
# unrecognized field. This mirrors how the quotes surface consumes ``quote.quoteTime``.
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

# The two chain-level fields the row builder recomputes rather than copies. The contract
# count is the captured rows' own length, and the truncation flag also takes account of a
# window given up, so neither column ever holds the vendor's value verbatim. A value one of
# them refuses is therefore this code's bug rather than the vendor's, so they are kept out
# of the overflow below and keep failing the row. Every other field in the header map is a
# verbatim copy.
_CHAINS_HEADER_RECOMPUTED = frozenset({"isChainTruncated", "numberOfContracts"})

# The key the chain-level fields' overflow nests under. They are read off the top of the
# chain body and repeated on every contract row, so a key written flat beside the contract
# fields would be ambiguous with a contract field of the same name, and a vendor that later
# added one would silently merge the two. Nesting keeps ``chain.underlyingPrice`` distinct
# from anything a contract sends, which is the same reason the quotes overflow nests.
#
# The one name that could collide is a contract field called ``chain``, which the fail-open
# writes flat under exactly this key. A row that routes a chain-level value into it refuses
# by name in ``_extra_with_routed``, whatever the vendor's value is, rather than merging one
# measurement into another. What that refusal cannot reach is the row that routes nothing,
# where such a field's own nested keys read back as chain-level values. That residual is
# marketlake #156, which is authoritative for it.
_CHAINS_HEADER_BLOCK = "chain"

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
# overflow keeps that column normally empty. A stamp the epoch transform refuses was not
# consumed, so it overflows instead, which is what ``_project_quote_envelope`` checks.
_QUOTE_TS_FIELD = "quoteTime"
_QUOTE_CONSUMED = frozenset({_QUOTE_TS_FIELD})

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

# The two fields read off the per-symbol envelope rather than a captured block, each stored
# verbatim. ``realtime`` is the entitlement flag the validation battery checks. The CUSIP
# arrives either as a top-level ``cusip`` or inside the ``reference`` block, and
# ``quote_cusip`` takes it from wherever it sits, so the key here is the vendor's name for
# the value rather than a path through the payload.
_QUOTES_ENVELOPE_MAP = {
    "realtime": "realtime",
    "cusip": "cusip",
}

# The key the envelope fields' overflow nests under. They sit outside every captured block,
# so they need a block of their own, and the vendor's own block names are the four above.
# Only a captured block's leftovers reach the top level of a quotes overflow, so no vendor
# field can land under this key.
_QUOTES_ENVELOPE_BLOCK = "envelope"


class ExtraPath(NamedTuple):
    """Where one column's value sits inside a row's ``extra`` JSON.

    ``field`` is the vendor's own name for the value, which is the key the overflow is
    written under. ``block`` is the key that field's own level nests under, or ``None``
    when the value sits flat at the top of the overflow.

    A level nests whenever its field names could collide with another level's. A chains
    row is built from one contract dict, so the contract's own fields sit flat, and the
    chain-level fields repeated onto that row nest under ``chain`` because a vendor could
    send a contract field of the same name. A quotes row is built from several blocks that
    reuse field names, so every block nests and ``quote.lastPrice`` stays distinct from
    ``extended.lastPrice``. The two fields read off the quotes envelope nest under
    ``envelope``, since they belong to no block.
    """

    block: str | None
    field: str


def _chains_extra_paths() -> dict[str, ExtraPath]:
    """The chains overflow read backwards: the contract map flat, the header map nested.

    The two recomputed header fields are left out. The row builder derives both from the
    captured rows rather than copying the vendor's, so neither column holds a vendor value
    to park.
    """
    paths = {column: ExtraPath(None, vendor) for vendor, column in _CHAINS_CONTRACT_MAP.items()}
    paths.update(
        {
            column: ExtraPath(_CHAINS_HEADER_BLOCK, vendor)
            for vendor, column in _CHAINS_HEADER_MAP.items()
            if vendor not in _CHAINS_HEADER_RECOMPUTED
        }
    )
    return paths


def _quotes_extra_paths() -> dict[str, ExtraPath]:
    """The quotes overflow read backwards, block-keyed, off the block specs and the envelope."""
    paths = {
        column: ExtraPath(block_key, vendor)
        for block_key, field_map, _consumed in _QUOTE_BLOCK_SPECS
        for vendor, column in field_map.items()
    }
    paths.update(
        {
            column: ExtraPath(_QUOTES_ENVELOPE_BLOCK, vendor)
            for vendor, column in _QUOTES_ENVELOPE_MAP.items()
        }
    )
    return paths


# How each surface's overflow is read backwards. The builders run per call rather than
# once at import, so a vendor map edited at runtime is reflected rather than snapshotted,
# and the derivation claim holds for a test that patches a map as well as for source.
_EXTRA_PATH_BUILDERS = {
    CHAINS_SURFACE: _chains_extra_paths,
    QUOTES_SURFACE: _quotes_extra_paths,
}


def extra_paths(surface: str) -> dict[str, ExtraPath]:
    """The surface's columns that a value in ``extra`` could be projected into.

    A field the schema does not name lands in ``extra``. A later version that promotes it
    into its own column leaves the older rows holding it there, and reading those rows as
    the newer shape needs to know which key in the overflow feeds which column. That is
    what this returns: the parser's vendor maps read backwards, column name to overflow
    key.

    It is derived from those maps rather than restated, so promoting a field stays the one
    edit it always was. Add the vendor field to its block's map and the column becomes
    projectable in the same motion.

    Every column whose value the parser copies verbatim from one named vendor field is
    here, wherever on the payload that field arrives. The contract fields sit flat, the
    chain-level ones under ``chain``, each quote block's under its own key, and the
    envelope's ``realtime`` and ``cusip`` under ``envelope``.

    A column is reachable only if the parser would overflow its vendor field, which is a
    narrower set than the schema. Four groups are deliberately absent.

    1. ``is_chain_truncated`` and ``number_of_contracts``. The row builder recomputes both
       from the captured rows rather than copying the vendor's header, so neither column
       ever holds a value the vendor sent.
    2. ``option_deliverables_list``. The writer JSON-encodes the vendor's nested list into
       that string column, so a raw overflow value would not fit it, and the field has been
       known since version 1 and so can never be in ``extra`` to begin with. Projecting a
       value the writer transforms needs the transform, which is a promotion this surface
       has never made.
    3. ``vendor_quote_ts``, on both surfaces. The vendor quote time is consumed into that
       stamp rather than stored. The only value of it that reaches ``extra`` is one the
       epoch transform refused, and projecting that back would need the transform that
       already said no, so there is nothing for a path here to recover.
    4. The stamps, the provenance columns, and the chains window pair. None is a vendor
       field, so none was ever a candidate for the overflow.

    A surface with no capture schema raises through ``schema_for``. A surface that has one
    and no vendor maps raises here, naming itself, which is what a third pinned surface
    added in one place and forgotten in the other would hit.
    """
    schema_for(surface)
    try:
        build = _EXTRA_PATH_BUILDERS[surface]
    except KeyError:
        raise ValueError(f"surface {surface!r} pins a schema but no vendor maps") from None
    return build()


# -- row building ------------------------------------------------------------


def _iso(value: str | datetime | date | None) -> str | None:
    """Normalize a timestamp argument to an ISO-8601 string, or ``None``.

    A string passes through. A ``datetime`` or ``date`` is formatted. The caller
    stamps these from the injected clock, so this module never reads the wall clock.
    """
    if value is None or isinstance(value, str):
        return value
    return value.isoformat()


class UnfitEpochError(Exception):
    """A vendor epoch-millisecond stamp that does not name a usable instant.

    Raised by ``epoch_ms_to_utc``. Its callers null the stamp column and let the vendor's
    own value overflow into ``extra``, so the row records what arrived instead of either
    inventing a timestamp or costing the whole minute.

    This is deliberately not a member of ``UNFIT_ERRORS``. That tuple is read only by
    ``_fits`` and ``_routed_column``, both inside the column build, and the epoch transform
    runs in the row builder before any column of that batch exists. Nothing on this path
    would ever consult it.
    """


def epoch_ms_to_utc(value: object) -> datetime | None:
    """A vendor epoch-millisecond stamp as a UTC datetime, or ``None`` when absent.

    Schwab stamps a contract's quote time as ``quoteTimeInLong`` and an equity quote's as
    ``quote.quoteTime``, both epochs in milliseconds. Converting a stored epoch to a
    datetime is deterministic and reads no wall clock. A missing value returns ``None``,
    which is the vendor sending nothing and stays distinct from a refusal.

    The rule is refuse what converts wrongly or not at all, never refuse what is not a
    number type. A numeric string converts correctly today, padding and all, and the lake
    captures those values right now, so a type check would drop data rather than save it.

    Two shapes are refused.

    1. A bool. ``float(True)`` is ``1.0``, so a vendor ``true`` would land as a plausible
       1970 timestamp with nothing raised and nothing to read it by. A bool is an ``int``
       in Python, so it is excluded by name here, the way ``_float_column`` and
       ``_int_column`` exclude it.
    2. Anything the conversion itself rejects. That covers a non-numeric string, a dict, a
       list, a NaN, and an epoch too far out for the platform's clock.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise UnfitEpochError(f"boolean value in an epoch-millisecond stamp: {value!r}")
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise UnfitEpochError(f"{type(exc).__name__}: {exc}") from exc


def _epoch_ms_to_iso(value: object) -> str | None:
    """A vendor epoch-millisecond stamp as a UTC ISO-8601 string, or ``None``.

    The string form of ``epoch_ms_to_utc``, which the chains surface stores. A value that
    transform refuses raises ``UnfitEpochError`` through here unchanged.
    """
    stamp = epoch_ms_to_utc(value)
    return None if stamp is None else stamp.isoformat()


def _epoch_refused(value: object) -> bool:
    """Whether the epoch transform refuses this vendor stamp.

    Consumed means successfully transformed. A stamp the transform refused consumed
    nothing, so the field that carried it stops counting as consumed for that row and
    overflows into ``extra`` under its own vendor name.
    """
    try:
        epoch_ms_to_utc(value)
    except UnfitEpochError:
        return True
    return False


def _extra_json(fields: Mapping[str, object], known: set[str]) -> str | None:
    """JSON for the vendor fields the schema does not name, or ``None`` when empty.

    Keys are sorted so the same overflow always serializes identically.

    The comprehension below is what makes the routing's signature readable. It keeps only
    the keys outside ``known``, so a known vendor field's name can never reach ``extra``
    this way. When ``_routed_column`` later writes one there, its presence needs no marker
    to be recognized. The quotes surface builds its overflow the same way, per block, in
    ``_project_quote_envelope``.

    On the chains surface ``known`` is the contract's own fields, so a contract carrying a
    chain-level name like ``underlyingPrice`` overflows under it, flat. That is not the
    signature and must not read as one, because the value came off a contract rather than
    the chain. Nesting the chain-level values under ``chain`` is what keeps the two apart.
    """
    overflow = {key: value for key, value in fields.items() if key not in known}
    if not overflow:
        return None
    return json.dumps(overflow, sort_keys=True)


def _int_column(values: Sequence[object]) -> pa.Array:
    """One ``int64`` column, built so a fractional float raises instead of truncating.

    Building an integer column straight from Python objects coerces a fractional float to
    its truncated value and hands it back with no error, so a vendor ``3.7`` lands as
    ``3``. Among the 29 integer columns that is the conversion which changes a value
    without raising, so only they take this route and the other 119 keep the direct build.

    Inferring the column's type first and then casting to ``int64`` moves the check into
    Arrow, which refuses a float it cannot represent exactly and still passes a lossless
    one like ``1500.0``. Everything else falls back to the direct build, so the caller
    sees the same exception it saw before this route existed. That covers an inferred bool
    or string, where Arrow's own cast would turn ``True`` into ``1`` and ``"7"`` into
    ``7``, and inference that raises outright. It also covers the empty and all-null
    column, which the direct build already lands as a typed column of nulls.

    A column holding both a float and a bool needs the explicit scan below. Arrow reads a
    column's type from its first non-null value and widens the later ones into it, so a
    bool that follows a float is already ``1.0`` by the time inference returns and the
    inferred type alone can no longer tell it from a real 1. Ordering is what hides it:
    ``[True, 1500.0]`` raises during inference while ``[1500.0, True]`` does not. The scan
    runs only once a float is present, never on the ordinary all-integer column, which
    returns on the line above without touching it.
    """
    try:
        inferred = pa.array(values)
    except Exception:
        return pa.array(values, type=pa.int64())
    if inferred.type == pa.int64():
        return inferred
    if pa.types.is_floating(inferred.type) and not any(isinstance(value, bool) for value in values):
        return inferred.cast(pa.int64())
    return pa.array(values, type=pa.int64())


def _float_column(values: Sequence[object]) -> pa.Array:
    """One ``double`` column, built so a bool raises instead of landing as 1.0.

    Building a floating column straight from Python objects accepts a bool and hands back
    ``1.0`` or ``0.0`` with no error, so a vendor ``"bidPrice": true`` lands as a
    one-dollar bid that nothing downstream can tell from a real one. Among the 65 double
    columns that is the only conversion which changes a value without raising. A string
    and an out-of-range integer already raise, so this is one shape rather than a family.

    The same ``true`` is already refused in an ``int64`` column. Raising here is what makes
    the two column types give one answer to one shape, and ``pa.ArrowTypeError`` is the
    class Arrow itself raises for a bool where a number belongs, so a cycle that gaps on
    this records the gap under the name it already used.

    Inference cannot replace the scan, for the reason ``_int_column`` gives. Arrow reads a
    column's type from its first non-null value and widens the later ones into it, so
    ``[True]`` infers ``bool`` and is catchable while ``[1500.0, True]`` infers ``double``
    and is not. Ordering is what hides it, so every value is scanned rather than the first.

    Unlike the integer route, this scan runs on the ordinary all-float column too, because
    a double column holds floats on the path it takes every cycle and there is no earlier
    check to return on. The price is measured rather than assumed. A 20,000 contract chain
    across all 65 double columns costs roughly 35 ms of scan on top of a 9 ms build, which
    is under a tenth of a percent of the one minute a capture cycle has.
    """
    if any(isinstance(value, bool) for value in values):
        raise pa.ArrowTypeError("Expected double, got bool")
    return pa.array(values, type=pa.float64())


def typed_column(field_type: pa.DataType, values: Sequence[object]) -> pa.Array:
    """One column of ``values`` at ``field_type``, built the way every write site builds it.

    An all-null column still lands with the right type instead of guessing. An ``int64``
    column goes through ``_int_column``, which refuses a fractional float rather than
    recording it truncated. A ``double`` column goes through ``_float_column``, which
    refuses a bool rather than recording it as 1.0.

    Three callers share this. The row builders below type every column they write, the
    read-time projection in ``extra_projection`` types a value it lifts back out of
    ``extra``, and ``_fits`` asks it whether one value belongs in its column at all. None
    of them should be able to accept a value another refuses, so the rule lives here once.
    """
    if field_type == pa.int64():
        return _int_column(values)
    if field_type == pa.float64():
        return _float_column(values)
    return pa.array(values, type=field_type)


# The ways a column build refuses one value. Every JSON value, offered to every type the
# two pinned schemas use, raises one of these four and nothing else, which
# ``tests/unit/test_journal_schema.py`` checks by enumeration so a pyarrow upgrade that
# adds a fifth fails the suite rather than quietly costing a cycle.
#
# The name is public because the reader consumes it too. ``extra_projection._convert``
# offers a value to ``typed_column`` and asks the same question the writer asks, so what a
# refusal looks like has to be one list. A second copy is how the two drifted apart once
# already, and the copy was the one that went a family short.
#
# 1. ``ArrowInvalid`` is the value Arrow understands and cannot represent, like a
#    fractional float in an integer column.
# 2. ``ArrowTypeError`` is the value whose Python type the column will not take at all,
#    like a bool where an integer belongs.
# 3. ``OverflowError`` comes from the conversion rather than from Arrow, and is the integer
#    too wide for int64.
# 4. ``UnicodeEncodeError`` is the string Arrow cannot encode, which a lone surrogate is.
#
# Any other exception is not the value's doing, so it propagates and costs the cycle the
# way it always did.
UNFIT_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, UnicodeEncodeError)


def _fits(field_type: pa.DataType, value: object) -> bool:
    """Whether one value builds at a column's type on its own.

    The check runs the value through ``typed_column``, the same builder the column build
    uses, so a value this calls unfit is exactly a value that build refused. ``None``
    always fits, because an all-null column lands typed.
    """
    try:
        typed_column(field_type, [value])
    except UNFIT_ERRORS:
        return False
    return True


def _routed_column(
    field: pa.Field,
    values: Sequence[object],
    path: ExtraPath | None,
    observed: frozenset[int],
) -> tuple[pa.Array, tuple[int, ...]]:
    """One column at its schema type, with the values it refuses nulled and named.

    The fast path is the whole column built at once, which is what every cycle takes. The
    scan below runs only after that build has already refused, so the ordinary cycle pays
    nothing for it.

    A vendor that retypes a known field is the case this exists for. The old behaviour cost
    the whole chain for that cycle and threw the offending value away, and a minute is the
    one thing this lake cannot buy back. So the value is nulled in its column and handed
    back for the caller to park in ``extra``, and the cycle lands.

    Three conditions have to hold before any value is routed. Each one is what keeps a
    failure that is not the vendor's doing failing the way it did before.

    1. **The column has somewhere to put the value.** ``path`` is the column's entry in
       ``extra_paths``, which is the parser's vendor maps read backwards. A column outside
       that set holds a value this module computed, transformed, or recomputed rather than
       copied, like a stamp or the chain's contract count. The value that refused is then
       this code's own rather than the vendor's, so there is nothing honest to park and no
       reader that would find it, and the refusal propagates and the cycle gaps.
    2. **Every refused value sits on a row that carries a vendor observation.**
       ``observed`` is the data rows. A gap row holds no vendor value at all, so anything
       on one came from this code. Two builders put a value on a gap row, the chunker's
       absence marker and the close guard's absent-series marker, and both write the one
       ``expiration_date`` those rows exist to name. Routing that would delete the marker's
       only fact and stamp a data row's drift signature on a gap, so a refusal there
       propagates instead.
    3. **Nulling the values the scan named is enough.** The rebuild at the end is the
       diagnosis being checked, not a formality, so it is deliberately not caught. A
       refusal there propagates and the cycle gaps. That covers a scan that names no value
       at all, because then the rebuild is the same call that just failed and it fails
       again. So a column refused for a reason no row carries never yields a partial column
       written off a wrong diagnosis.

    The cost is named rather than hidden. A vendor map that sends a field to the wrong
    column is indistinguishable at run time from a vendor that retyped that field, and both
    route. What surfaces either one is the signature the routing writes, a known field's
    name sitting in ``extra``, which is the thing to page on.
    """
    try:
        return typed_column(field.type, values), ()
    except UNFIT_ERRORS:
        if path is None:
            raise
        unfit = tuple(index for index, value in enumerate(values) if not _fits(field.type, value))
        if not set(unfit) <= observed:
            raise
        kept = list(values)
        for index in unfit:
            kept[index] = None
        # An empty scan leaves ``kept`` equal to ``values``, so this is the call that just
        # refused and it refuses again. That is the intended outcome, and it is why the
        # empty case needs no guard of its own.
        return typed_column(field.type, kept), unfit


def _fail_open_blocks(surface: str) -> frozenset[str]:
    """The overflow keys the surface's own fail-open nests an unrecognized field under.

    A quotes row's unrecognized fields nest under the block they arrived in, so a routed
    value bound for one of those blocks joins what is already there rather than colliding
    with it. A chains row's nest under nothing, so every key at the top of a chains overflow
    is a vendor field's own name and none of them is a block this module writes.

    This is what tells a block to merge into from a key to refuse. It is derived from the
    block specs rather than restated, so a fifth captured quotes block would be mergeable in
    the same edit that captures it.
    """
    if surface == QUOTES_SURFACE:
        return frozenset(block_key for block_key, _map, _consumed in _QUOTE_BLOCK_SPECS)
    return frozenset()


def _extra_with_routed(
    raw: object, routed: Sequence[tuple[ExtraPath, object]], mergeable: frozenset[str]
) -> str:
    """One row's ``extra`` with the values its columns refused added, verbatim.

    Each value is written under the key ``extra_paths`` says feeds its column, which is the
    vendor's own name for it, nested under its block wherever the field's level nests. That
    is the same key an unrecognized field of that name would have landed under, so the
    reader in ``extra_projection`` needs no second rule to find it.

    The value is the vendor's, unchanged. Nothing is cast, rounded, or stringified on the
    way in. A cast would manufacture a value the vendor never sent and hand it to a
    downstream computation with no marker, which is the one outcome nobody can detect
    afterwards, and the design's vendor-verbatim rule is what forbids it.

    A collision on the field itself is not possible. ``_extra_json`` and the quotes
    projection both build the overflow from the fields their maps do *not* name, so a key a
    routed field writes under is a key the overflow could not already hold.

    One key above it can collide, and only on the chains surface. The chains fail-open
    writes an unrecognized contract field flat, so a contract field named ``chain`` lands on
    the key the chain-level values nest under. Merging into it would put a routed value
    inside a value the vendor sent, and no reader could then tell which level either key
    came from. So the row refuses, naming the key, and the cycle gaps the way it did before
    any of this routed. That is the loud failure, and the alternative is a silent misreading
    of two different measurements as one.

    ``mergeable`` is what separates that collision from an ordinary quotes merge, where the
    fail-open wrote the block key itself and the routed value belongs inside it. Any value
    at all under a key outside that set is the vendor's, whatever its type, so the refusal
    reads the key's presence rather than its shape. A dict is the shape that would otherwise
    merge cleanly and silently, which makes it the one the check must not let through.

    The keys are checked before any is written, so the refusal never leaves a half-merged
    overflow behind, and a second value routing into a block this call just created is not
    mistaken for a collision with the vendor.
    """
    overflow = dict(json.loads(raw)) if raw else {}
    for path, _value in routed:
        if path.block is None or path.block not in overflow:
            continue
        if path.block not in mergeable or not isinstance(overflow[path.block], Mapping):
            raise ValueError(
                f"a vendor field named {path.block!r} collides with the overflow block "
                f"{path.field!r} routes under"
            )
    for path, value in routed:
        if path.block is None:
            overflow[path.field] = value
        else:
            held = overflow.get(path.block)
            block = dict(held) if isinstance(held, Mapping) else {}
            block[path.field] = value
            overflow[path.block] = block
    return json.dumps(overflow, sort_keys=True)


def _batch(surface: str, rows: Sequence[Mapping[str, object]]) -> pa.RecordBatch:
    """Build one record batch from row mappings, typed by the surface's pinned schema.

    A missing key becomes null. Each column is built with its schema type through
    ``typed_column``. A known vendor field whose value the column refuses is routed into
    that row's ``extra`` and left null in its column, per ``_routed_column``, so a vendor
    retype costs the field rather than the cycle. Only a data row can route, since a gap
    row carries no vendor observation and anything on one came from this code.

    ``extra`` is built last, because the routing decides what goes in it.
    """
    schema = schema_for(surface)
    paths = extra_paths(surface)
    observed = frozenset(
        index for index, row in enumerate(rows) if row.get(ROW_KIND_COLUMN) == ROW_KIND_DATA
    )
    routed: dict[int, list[tuple[ExtraPath, object]]] = {}
    columns: dict[str, pa.Array] = {}
    for field in schema:
        if field.name == EXTRA_COLUMN:
            continue
        values = [row.get(field.name) for row in rows]
        columns[field.name], unfit = _routed_column(field, values, paths.get(field.name), observed)
        for index in unfit:
            routed.setdefault(index, []).append((paths[field.name], values[index]))
    extras = [row.get(EXTRA_COLUMN) for row in rows]
    mergeable = _fail_open_blocks(surface)
    for index, entries in routed.items():
        extras[index] = _extra_with_routed(extras[index], entries, mergeable)
    columns[EXTRA_COLUMN] = typed_column(schema.field(EXTRA_COLUMN).type, extras)
    return pa.RecordBatch.from_arrays([columns[field.name] for field in schema], schema=schema)


def _columns_named(
    held: Mapping[str, object],
    by_key: Mapping[tuple[str | None, str], str],
    blocks: Collection[str],
) -> Iterator[str]:
    """Every column of the surface that one row's overflow names, flat or under a block.

    Walking the row's keys rather than the surface's paths is what keeps a populated
    overflow affordable. An unrecognized vendor field is one key, so it costs one lookup
    rather than one per column, and the ordinary reason an overflow is populated at all is
    exactly that field.
    """
    for key, value in held.items():
        column = by_key.get((None, key))
        if column is not None:
            yield column
        if key in blocks and isinstance(value, Mapping):
            for sub in value:
                column = by_key.get((key, sub))
                if column is not None:
                    yield column


def routed_columns(surface: str, batch: pa.RecordBatch) -> tuple[str, ...]:
    """The surface's own columns whose vendor name sits in a built batch's ``extra``.

    This is the drift signature ``_routed_column`` names, read back off a batch. A known
    vendor field's name can only reach the overflow by being routed there, for the reason
    that docstring gives. So a name from ``extra_paths`` found in ``extra`` means that
    column refused the value the vendor sent, and the column is null on that row.

    It answers the one question a page needs, which column drifted, and deliberately not
    how many rows carried it or what the value was. Both of those are already on disk in
    the rows themselves, and ``reports/`` and the nightly report are where a reader goes
    for them.

    A key the surface's paths do not name is an unrecognized vendor field, which is the
    fail-open working as designed and belongs to the nightly report rather than to a
    phone. Matching against ``extra_paths`` is the whole distinction between the two.

    The key alone is not quite enough, and the row's own column is what settles it. A
    routed value is nulled in its column on the same row it was parked from, per
    ``_routed_column``, so a key that names a column still holding a value cannot be the
    routing's signature. One shape reaches here that way. The chains fail-open writes an
    unrecognized contract field flat, so a vendor contract field named ``chain`` lands on
    the key the chain-level values nest under, and its own subkeys then read as chain-level
    field names. ``_extra_with_routed`` refuses that collision by name, but only on a row
    where something actually routed, so a row that routed nothing never reaches it. The
    null check covers that row, and it can never cost a true finding, because a column that
    routed is null on that row by construction.

    The cost is named rather than hidden. This re-derives a fact ``_batch`` already knew
    and discarded, so a routing change that wrote under some other key would leave the two
    out of step. The guard against that is derivation: both sides read ``extra_paths``
    rather than a list of their own, and
    ``tests/unit/test_journal_schema.py::test_routed_columns_names_every_column_the_routing_writes``
    walks every path on both surfaces so a new one is covered the day it is added.

    This runs between the row build and the segment write, on every segment of every
    cycle, so what it costs when it finds nothing is the number that matters. Two gates
    answer the ordinary cycle before any JSON is parsed.

    The first is the overflow's null count. An all-null overflow is every cycle the lake
    has recorded, and it returns immediately with no path map built and no column
    materialized. The saving is real rather than a dict lookup, because ``extra_paths``
    rebuilds both vendor maps on every call. A gap batch is this case by construction,
    since a gap row carries no vendor observation and ``_routed_column`` refuses to route
    onto one, so nothing here needs to read ``row_kind``.

    The second gate is what makes a *populated* overflow cheap, and the design is why it
    has to be. An unrecognized vendor field lands in ``extra`` on every row of the payload
    and is explicitly not a page, so a populated overflow is an ordinary event rather than
    a rare one. A routed value is nulled in its own column, so a column carrying no null at
    all cannot have routed, and one cached null count per column rules out every column the
    vendor is still filling. A chain whose only overflow is a new greek clears this gate
    with nothing left to look for.

    That second gate is also what keeps one collision from fabricating a finding. The
    chains fail-open writes an unrecognized contract field flat, so a field named ``chain``
    lands on the key the chain-level values nest under and its subkeys then read as
    chain-level names. ``_extra_with_routed`` refuses that collision by name, but only on a
    row where something actually routed, so a row that routed nothing never reaches it. The
    columns such a key can name are block-level, and a block-level column holds one value
    for the whole batch, so the null count answers it: while the vendor is still sending
    that header field, the column is not a candidate and the key is never looked up. What
    is left over is a header field the vendor stopped sending on the same payload that
    invented a field named ``chain``, which is the residual marketlake #156 already names.

    Past both gates the walk is over the row's own keys rather than over every path, so a
    row costs the handful of fields it actually carries instead of the whole surface's
    column list.
    """
    overflow = batch.column(EXTRA_COLUMN)
    if overflow.null_count == len(overflow):
        return ()
    # A routed column is null on the row it routed from, so a column with no null anywhere
    # in the batch is not a candidate and never needs a key looked up for it.
    candidates = {
        column: path
        for column, path in extra_paths(surface).items()
        if batch.column(column).null_count
    }
    if not candidates:
        # Performance only, and deliberately so. The loop below breaks on its first
        # iteration when there is nothing to find, so removing this line changes no
        # answer and no test can see it. What it buys is skipping ``to_pylist`` on the
        # whole overflow column, which is 0.39 ms against 0.06 ms on a 13,500-row chain
        # and grows with the row count where this does not. It is stated as unheld by a
        # test rather than left to look like a guard that one forgot to cover.
        return ()
    by_key = {(path.block, path.field): column for column, path in candidates.items()}
    blocks = {path.block for path in candidates.values() if path.block is not None}
    found: set[str] = set()
    for raw in overflow.to_pylist():
        # A vendor retype reaches every row of the payload, so the first row usually names
        # every column that moved and the rest of the chain is walked for nothing.
        if len(found) == len(candidates):
            break
        if not raw:
            continue
        held = json.loads(raw)
        if not isinstance(held, Mapping):
            continue
        found.update(_columns_named(held, by_key, blocks))
    return tuple(sorted(found))


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


class AbsentMarker(NamedTuple):
    """One absence-marker gap row for a chunk the chunker gave up on.

    ``window_start`` and ``window_end`` are the failed range's ISO dates, the end ``None``
    on the open tail. ``error_class`` is that window's own failure class. ``expiration_date``
    names one missing expiration when the chunker could read it off the journal's latest
    prior durable batch. It is ``None`` on the per-window marker, the fallback when no prior
    batch exists or none of its expirations fall inside the failed range. Either way every
    vendor column stays null, so the marker is the design's single exception to the
    all-columns-null gap row only in ``expiration_date``.
    """

    window_start: str
    window_end: str | None
    error_class: str
    expiration_date: str | None


def _window_bounds(
    windows: Sequence[tuple[date | str, date | str | None]],
) -> list[tuple[str, str | None]]:
    """The plan windows as ISO date bounds, the open tail's end left ``None``."""
    return [(_day_str(start), None if end is None else _day_str(end)) for start, end in windows]


def _window_holding(
    exp_iso: str, bounds: Sequence[tuple[str, str | None]]
) -> tuple[str | None, str | None]:
    """The plan window whose inclusive date range holds an expiration, or nulls.

    The windows tile the date line from the session date, so at most one matches, and the
    open tail matches anything on or after its start. ISO dates compare as text, so no date
    parsing is needed. An expiration no window holds, one dated before the session date,
    leaves both bounds null.
    """
    for start, end in bounds:
        if exp_iso >= start and (end is None or exp_iso <= end):
            return start, end
    return None, None


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
    windows: Sequence[tuple[date | str, date | str | None]] = (),
    absent_markers: Sequence[AbsentMarker] = (),
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
    windowed chain reports its own captured contract count and whether any window was given
    up, not one window response's header figures. The other four are the vendor's own
    values, so a value one of their columns refuses routes into ``extra`` under ``chain``
    rather than costing the cycle, while a refusal in either recomputed column is this
    code's bug and still fails the row. The other timestamps are the caller's,
    stamped from the injected clock. ``fetch_end_ts`` is when the response landed, the
    request end, so the round-trip is ``fetch_end_ts`` minus ``fetch_ts``.

    ``windows`` is the session's concrete plan, the ``(from_date, to_date | None)`` list
    from ``ChainPlan.windows_for``. Each data row carries ``window_start`` and
    ``window_end``, the plan window whose range holds the contract's expiration date. That
    is the plan window, never a split sub-range, because the nightly re-tune groups rows by
    it. The empty default leaves both null, the one-shot whole-chain case.

    ``absent_markers`` supports the capture chunker's partial snapshot. When a chain is
    fetched by date windows and one window fails, that window's contracts are absent from
    ``body``. Rather than lose the whole chain to a gap, the chunker hands one
    ``AbsentMarker`` per gap row here. The chunker names the missing expirations by reading
    the ticker's latest prior durable chains batch off the journal, keeping those inside the
    failed range and dated on or after the session date, one marker each. With no prior
    batch, or none of its expirations inside the range, it hands one per-window marker
    instead, its ``expiration_date`` null. Every marker becomes one gap row in this same
    batch: ``row_kind`` gap, ``error_class`` the window's own class, ``window_start`` and
    ``window_end`` the failed range, ``expiration_date`` as the marker says, and every other
    vendor column null. So one segment carries the captured contracts and the absence
    markers together, and no failed window loses its class. The default empty sequence is
    the ordinary whole-chain case.
    """
    header = {column: body.get(vendor) for vendor, column in _CHAINS_HEADER_MAP.items()}
    bounds = _window_bounds(windows)
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
        # The quote time is consumed into the stamp, so it is normally held out of the
        # overflow. A value the transform refuses consumed nothing, so the exclusion stops
        # applying for this row: the stamp lands null and the vendor's own value overflows
        # under its own name, the signature a reader already knows means the column refused.
        quote_time = contract.get(_CHAINS_QUOTE_TS_FIELD)
        known = _CHAINS_CONTRACT_KNOWN
        try:
            row["vendor_quote_ts"] = _epoch_ms_to_iso(quote_time)
        except UnfitEpochError:
            row["vendor_quote_ts"] = None
            known = _CHAINS_CONTRACT_KNOWN - _CHAINS_CONTRACT_CONSUMED
        for vendor, column in _CHAINS_CONTRACT_MAP.items():
            if vendor in contract:
                row[column] = contract[vendor]
        deliverables = contract.get(_CHAINS_DELIVERABLES_FIELD)
        if deliverables is not None:
            row[_CHAINS_DELIVERABLES_COLUMN] = json.dumps(deliverables, sort_keys=True)
        row[EXTRA_COLUMN] = _extra_json(contract, known)
        # The fetch provenance: the plan window holding this contract's expiration date.
        # The vendor's expiration is an ISO datetime, so its date part is the key.
        expiration = contract.get("expirationDate")
        if expiration is not None and bounds:
            row["window_start"], row["window_end"] = _window_holding(
                str(expiration).split("T")[0], bounds
            )
        rows.append(row)
    # number_of_contracts and is_chain_truncated describe the whole stored chain, not any
    # single response, so recompute them from the reassembled rows. The count is the
    # contract rows actually captured. Truncation is the vendor's own flag on a one-shot
    # chain, or, on a windowed chain, whether any window was given up and marked absent
    # below. Both override the header value, which on a windowed chain came from one
    # window's response and describes only that window.
    contract_count = len(rows)
    truncated = bool(header["is_chain_truncated"]) or bool(absent_markers)
    for row in rows:
        row["number_of_contracts"] = contract_count
        row["is_chain_truncated"] = truncated
    for window_start, window_end, error_class, expiration_date in absent_markers:
        # One absence-marker gap row. Every vendor column stays null except expiration_date,
        # set only when the chunker could name the missing expiration off the journal. The
        # failed range rides window_start and window_end. suspect and close_tag ride the same
        # values as the data rows so the whole snapshot tags consistently.
        rows.append(
            {
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
                "expiration_date": expiration_date,
                "window_start": window_start,
                "window_end": window_end,
                EXTRA_COLUMN: None,
            }
        )
    return _batch(CHAINS_SURFACE, rows)


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
    envelope-level ``realtime`` flag and the ``cusip`` are captured too. Both belong to no
    block, so a value either column refuses routes under ``envelope`` rather than a block
    key, per ``extra_paths``. Anything else on the envelope, like ``assetMainType`` or the
    ``reference`` block beyond the CUSIP, is neither captured nor overflowed.
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
        if _QUOTE_TS_FIELD in consumed and _epoch_refused(block.get(_QUOTE_TS_FIELD)):
            known -= {_QUOTE_TS_FIELD}
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
    steady state and drift in any block still surfaces. The two envelope fields belong to
    no block, so a value either column refuses routes under ``envelope``. ``fetch_end_ts``
    is the request-end stamp, the pair to ``fetch_ts`` for round-trip.
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
    row[EXTRA_COLUMN] = extra
    return _batch(QUOTES_SURFACE, [row])


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
        EXTRA_COLUMN: None,
    }
    return _batch(surface, [row])


def gap_rows(
    surface: str,
    *,
    ticker: str,
    slots: Sequence[datetime],
    error_class: str,
    session_phase_at: Callable[[datetime], str | None] | None = None,
) -> pa.RecordBatch:
    """Build one gap batch covering many missed minutes, one row per slot.

    ``gap_batch`` is the single-minute form. A dead daemon leaves whole sessions
    missing, and a regular session is 406 capture slots, so building one batch per
    minute would put 406 record batches and 406 message headers in a segment that holds
    one fact per row. This builds one batch instead, with the same all-columns-null rule
    by construction.

    ``session_phase_at`` names the phase of a past slot, because a marker for a minute
    after the equity close carries the same ``session_phase`` a captured row would have
    carried. Passing nothing leaves every phase null.
    """
    rows = [
        {
            "snap_ts": _iso(slot),
            "fetch_ts": None,
            "fetch_end_ts": None,
            "vendor_quote_ts": None,
            "ticker": ticker,
            "row_kind": ROW_KIND_GAP,
            "error_class": error_class,
            "suspect": False,
            "close_tag": None,
            "session_phase": None if session_phase_at is None else session_phase_at(slot),
            "schema_version": SCHEMA_VERSION,
            EXTRA_COLUMN: None,
        }
        for slot in slots
    ]
    return _batch(surface, rows)


def absent_series_rows(
    surface: str,
    *,
    ticker: str,
    slot: datetime,
    expirations: Sequence[str],
    error_class: str,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build one gap batch naming several absent expirations, one row per series.

    ``gap_rows`` is the many-minutes form, one row per missed slot. This is the
    many-series form: one slot, one row per expiration that slot should have carried and
    did not. The close+5 guard writes it when the fill came back short of the day's
    intraday chain, so a series the vendor stopped offering is a row rather than a hole.

    Every vendor column stays null except ``expiration_date``, which is the same single
    exception the capture chunker's own absence markers make. ``window_start`` and
    ``window_end`` stay null on purpose. Those name the date range a *fetch* gave up on,
    and these rows exist for series whose window was fetched successfully. A reader can
    tell the two apart by that alone, and by the error class each carries.

    Chains only. An expiration is a chains-schema column, so naming one on another
    surface is a programming error rather than a field to drop quietly.
    """
    if surface != CHAINS_SURFACE:
        raise ValueError(f"an absent-series marker names an expiration, so not {surface!r}")
    rows = [
        {
            "snap_ts": _iso(slot),
            "fetch_ts": None,
            "fetch_end_ts": None,
            "vendor_quote_ts": None,
            "ticker": ticker,
            "row_kind": ROW_KIND_GAP,
            "error_class": error_class,
            "suspect": False,
            "close_tag": close_tag,
            "session_phase": session_phase,
            "schema_version": SCHEMA_VERSION,
            "expiration_date": expiration,
            "window_start": None,
            "window_end": None,
            EXTRA_COLUMN: None,
        }
        for expiration in expirations
    ]
    return _batch(surface, rows)


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

    This is the ``lake_root``-argument spelling of ``LakePaths.segment_path``. It
    delegates there, so the two can never drift apart.
    """
    return LakePaths(lake_root).segment_path(surface, ticker, day, start_ts, pid)


def segment_dir(lake_root: Path | str, surface: str, ticker: str, day: date | str) -> Path:
    """The directory holding one surface, ticker, and day's segments.

    ``segment_path`` names one file inside it. A reader that wants every segment for a
    ticker-day asks for the directory instead. Both defer to ``LakePaths``, so the
    layout is spelled in one place.
    """
    return LakePaths(lake_root).segment_dir(surface, ticker, day)


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


def _complete_batches(reader: pa.ipc.RecordBatchStreamReader) -> tuple[list[pa.RecordBatch], bool]:
    """Every complete record batch in a stream, and whether it ended at a clean EOS.

    A torn tail, from a power loss mid-append, stops the read at the last complete batch.
    The incomplete trailing bytes are dropped, never an error. Every durable cycle ends in a
    full flush, so the last complete batch is exactly what the writer believed it had. The
    flag is ``True`` only when the stream reached its end-of-stream marker.
    """
    batches: list[pa.RecordBatch] = []
    while True:
        try:
            batches.append(reader.read_next_batch())
        except StopIteration:
            return batches, True
        except (pa.ArrowInvalid, OSError):
            return batches, False


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
        batches, clean_eos = _complete_batches(reader)
        if clean_eos and source.tell() < source.size():
            raise ShadowAppendError(path, source.tell(), source.size())
    return pa.Table.from_batches(batches, schema=schema)


# What makes a segment unusable rather than absent, for a reader asking what it holds.
# Four things go wrong and they all mean one thing to the caller. The file will not open,
# which is an ``OSError``. The stream is malformed, which pyarrow raises as
# ``ArrowInvalid``, itself a ``ValueError``. Bytes sit past the end-of-stream marker, which
# is a ``ShadowAppendError``. Or the segment opens cleanly and its schema has drifted, so
# the column asked of it is gone, a ``KeyError``, or holds a value the parse refuses, a
# ``TypeError`` or a ``ValueError``.
#
# Drift is the case worth spelling out, because it does not look like a read failure. A
# segment written before a schema change reads back perfectly and then answers the wrong
# question. A reader that let that raise would take the daemon down from inside a hook
# nothing guards, which is what #100 was.
#
# Every case here means the same thing to a reader deciding whether to act: the file exists
# and cannot be trusted to say what it holds. That is never the same as the file not being
# there, which is why these land in ``unreadable`` rather than reading as an absent minute.
# They do not all mean the same thing to an operator, so each one is carried with its own
# kind below.
UNUSABLE_SEGMENT = (OSError, ShadowAppendError, KeyError, TypeError, ValueError)

# Why a segment could not be used. The first four names are ``dashboard.SegmentHealth``'s,
# reused rather than renamed so the panel and the readers under it name one failure one way.
SEGMENT_VANISHED = "vanished"  # listed and then gone, a seal landing mid-read
SEGMENT_CORRUPT = "corrupt"  # the bytes will not open: a torn header, or no Arrow stream
SEGMENT_SHADOW_APPEND = "shadow_append"  # bytes follow the end-of-stream marker
SEGMENT_DRIFTED = "drifted"  # it opened, and the column asked of it is gone or retyped
SEGMENT_UNPARSEABLE = "unparseable"  # the column is the right type and a value is not usable

# ``unparseable`` is the fifth because the panel already draws this line and drawing it
# differently here would be the conflation this reader exists to end, one layer down. A
# ``snap_ts`` column that is still ``string`` and holds ``the third minute`` is neither a
# missing field nor a retyped one, which is what the design's schema policy pages on, so the
# panel counts exactly that input under ``unparseable_stamp_rows`` and asserts it is not a
# drifted row. Folding it into ``drifted`` here would have a garbage value fire a severity-5
# ``Schema drift: retyped`` page while the panel beside it reported no drift on the same file.
# The panel counts rows and this counts segments, so the granularity differs and the line
# between the two meanings does not.

# The order a breakout prints in. Drift leads because it is the kind the design pages on,
# then the two other ways a payload can be wrong, then the two about the file itself. A
# fixed order makes the line independent of which segment happened to fail first, so two
# lakes with the same damage read alike.
_SEGMENT_KIND_ORDER = (
    SEGMENT_DRIFTED,
    SEGMENT_UNPARSEABLE,
    SEGMENT_SHADOW_APPEND,
    SEGMENT_CORRUPT,
    SEGMENT_VANISHED,
)


class UnusableSegment(NamedTuple):
    """One segment that could not be used, and why.

    The reason used to stop here. Both readers below knew it at the moment they caught it
    and returned a bare path, so every caller could report was a count. Schema drift then
    read the same as a disk error, and the design asks for a page on the one and not on the
    other. Carrying the kind with the path is what lets a producer tell them apart.
    """

    path: Path
    kind: str


def _open_failure_kind(exc: BaseException) -> str:
    """Which kind an open-stage failure is.

    Only the open stage calls this. The stage a failure came from decides the kind rather
    than the exception's type, because the types overlap and would misfile both directions.
    ``pa.ArrowInvalid`` is a ``ValueError``, so a torn header and an unparseable stamp
    arrive as one type. A ``snap_ts`` retyped to a real timestamp raises a plain
    ``TypeError`` from ``datetime.fromisoformat``, not ``pa.ArrowTypeError``, so keying
    drift on the pyarrow type would file the very case the schema policy pages on as a
    corrupt file. Where it raised carries no such ambiguity: a failure to open the file is
    about its bytes, and a failure after it opened is about its schema.
    """
    if isinstance(exc, FileNotFoundError):
        return SEGMENT_VANISHED
    if isinstance(exc, ShadowAppendError):
        return SEGMENT_SHADOW_APPEND
    return SEGMENT_CORRUPT


def _parse_failure_kind(exc: BaseException) -> str:
    """Which kind a failure after the open is.

    The file read back, so what is wrong is what it holds. Two of those are the schema and
    one is a single value, and the type separates them cleanly at this stage in a way it
    could not at the open:

    1. ``KeyError`` means the column asked for is gone, a missing known field.
    2. ``TypeError`` means its values came back as the wrong Python type, which is what a
       retyped column does. An ``int64`` or ``timestamp`` ``snap_ts`` reaches
       ``datetime.fromisoformat`` as an ``int`` or a ``datetime`` and is refused on type.
    3. Anything else, in practice a ``ValueError``, means the column is the right type and
       one of its values is not usable. That is not drift, and calling it drift would page
       on a vendor typo as though a field had changed shape.
    """
    if isinstance(exc, KeyError | TypeError):
        return SEGMENT_DRIFTED
    return SEGMENT_UNPARSEABLE


def describe_unusable(entries: Sequence[UnusableSegment]) -> str:
    """A one-line breakout of what could not be read, by kind.

    Reads ``2 unreadable (1 drifted, 1 corrupt)``. The total keeps the wording every caller
    printed before, so a reader who knows the old line reads this one, and the breakout
    after it is the part a drift page subscribes to. An empty set returns an empty string,
    which no caller asks for, since each tests the set first.
    """
    if not entries:
        return ""
    counts = Counter(entry.kind for entry in entries)
    parts = [f"{counts[kind]} {kind}" for kind in _SEGMENT_KIND_ORDER if counts[kind]]
    # A kind outside the five is not possible from the classifiers above. Printing any
    # stray one rather than dropping it keeps the breakout's total equal to the count.
    parts.extend(
        f"{count} {kind}" for kind, count in counts.items() if kind not in _SEGMENT_KIND_ORDER
    )
    return f"{len(entries)} unreadable ({', '.join(parts)})"


class RecordedSet(NamedTuple):
    """The distinct recorded minutes for a ticker-day, and what could not be read."""

    slots: frozenset[datetime]
    unreadable: tuple[UnusableSegment, ...]


def recorded_slots(
    lake_root: Path | str, surface: str, ticker: str, day: date | str
) -> RecordedSet:
    """The distinct set of minutes already recorded for one surface, ticker, and day.

    The hole-aware startup walk asks not "where does the record stop" but "which owed
    minutes are missing", so it needs the whole set of present minutes rather than the
    newest one. Counting every row kind, data and gap alike, makes a lone row, such as the
    close guard's 16:00 marker on an otherwise dark day, one present minute among the
    day's owed set rather than a false frontier that hides the rest.

    It reads the day's journal directory rather than the manifest, because marker
    segments carry no manifest entry.

    A torn tail is safe: each segment reads to its last complete batch. A segment that
    cannot be read at all is a different thing, so its path is returned in
    ``unreadable`` rather than silently dropped. Treating one as absent would let a day
    that really was captured read as dark, and a full session of markers would then be
    written over a record that exists. An empty set with nothing unreadable is the fully
    dark case.
    """
    directory = segment_dir(lake_root, surface, ticker, day)
    if not directory.is_dir():
        return RecordedSet(frozenset(), ())
    snaps: list[datetime] = []
    unreadable: list[UnusableSegment] = []
    for path in sorted(directory.glob("*.arrows")):
        try:
            if path.stat().st_size == 0:
                # Created and never written to, which is absent rather than unreadable.
                # ``SegmentWriter`` opens with ``O_CREAT|O_EXCL`` and fsyncs the directory
                # entry before the first schema bytes, so a process killed in between
                # leaves one of these durably behind, and a ``KeepAlive`` crash loop makes
                # them in quantity. It holds no batches at all, so there is nothing in it
                # to be wrong about and nothing a later writer would duplicate.
                continue
            table = read_segment(path)
        except UNUSABLE_SEGMENT as exc:
            # The file's bytes are the problem. Which of the three it is comes from the
            # exception, since the open stage is where those three are distinguishable.
            unreadable.append(UnusableSegment(path, _open_failure_kind(exc)))
            continue
        try:
            # Parsed inside a try of its own, and into a list before anything is kept, so a
            # segment that fails partway contributes none of its minutes rather than the
            # prefix that happened to parse. What raised here is about what the file holds
            # rather than its bytes, and ``_parse_failure_kind`` says which.
            parsed = [
                datetime.fromisoformat(value)
                for value in table.column("snap_ts").to_pylist()
                if value is not None
            ]
        except UNUSABLE_SEGMENT as exc:
            unreadable.append(UnusableSegment(path, _parse_failure_kind(exc)))
            continue
        snaps.extend(parsed)
    return RecordedSet(frozenset(snaps), tuple(unreadable))


def _is_chains_segment_for(rel: str, ticker: str, on: date | str | None = None) -> bool:
    """Whether a manifest partition path is one of ``ticker``'s chains journal segments.

    A capture-written segment is manifested under its own path,
    ``journal/date=D/surface=chains/ticker=T/seg-<start_ts>-<pid>.arrows``. This matches
    that shape exactly, so a compacted Parquet partition or another surface's segment never
    qualifies.

    Recognizing that shape is ``parse_segment_rel``'s job. It lives beside the builder that
    made the path. What is left here is the question only this reader asks, whether the
    parsed segment names the chains surface and the ticker in hand, and, when ``on`` is
    given, the session date too.
    """
    ref = parse_segment_rel(rel)
    if ref is None or ref.surface != CHAINS_SURFACE or ref.ticker != ticker:
        return False
    return on is None or ref.day == _day_str(on)


class CloseTagRows(NamedTuple):
    """Rows found under one close tag, and the segments that could not be read.

    ``unreadable`` exists for the same reason ``RecordedSet``'s does. A segment that
    cannot be read is not an absent one, and the close+5 guard's output is a claim that
    nothing observed a close. Counting a drifted file as zero rows would let the guard
    make that claim about a close sitting in the file it could not read.
    """

    data: int
    gaps: int
    unreadable: tuple[UnusableSegment, ...]


def close_tag_rows(
    lake_root: Path | str, surface: str, ticker: str, day: date | str, close_tag: str
) -> CloseTagRows:
    """How many data rows and gap rows a ticker-day holds under one ``close_tag``.

    The close+5 guard asks this to decide whether the day's close of record was ever
    observed. Data rows are what it wants: a cycle that ran at the option close and
    failed leaves a tagged gap row, which records the attempt but holds no marks. The
    two counts are returned apart so the caller can tell "never ran" from "ran and
    failed", which read the same to a caller that only asked whether any row exists.

    Every complete batch in every segment counts, so a torn tail is safe. Order does not
    matter here, because the answer is a sum over the whole ticker-day rather than a
    newest-wins lookup. A segment that cannot be read is counted in ``unreadable`` rather
    than as zero
    rows, so the caller can tell "this close was never recorded" from "one of these files
    will not say". The guard treats the second as a reason to stay quiet, because its
    output is a claim about what did not happen.
    """
    directory = LakePaths(lake_root).segment_dir(surface, ticker, day)
    if not directory.is_dir():
        return CloseTagRows(0, 0, ())
    unreadable: list[UnusableSegment] = []
    data = gaps = 0
    for path in sorted(directory.glob("*.arrows"), reverse=True):
        try:
            if path.stat().st_size == 0:
                # Created and never written to, which is absent rather than unreadable.
                # ``SegmentWriter`` opens with ``O_CREAT|O_EXCL`` and fsyncs the directory
                # entry before the first schema bytes, so a process killed in between
                # leaves one of these durably behind, and a ``KeepAlive`` crash loop makes
                # them in quantity. It holds no batches at all, so there is nothing in it
                # to be wrong about and nothing a later writer would duplicate.
                continue
            table = read_segment(path)
        except UNUSABLE_SEGMENT as exc:
            unreadable.append(UnusableSegment(path, _open_failure_kind(exc)))
            continue
        try:
            # The same two stages ``recorded_slots`` keeps apart, for the same reason. The
            # file opened and then answered the wrong question, which is the shape that
            # reads back perfectly and still cannot be trusted.
            tags = table.column("close_tag").to_pylist()
            kinds = table.column("row_kind").to_pylist()
        except UNUSABLE_SEGMENT as exc:
            unreadable.append(UnusableSegment(path, _parse_failure_kind(exc)))
            continue
        for tag, kind in zip(tags, kinds, strict=True):
            if tag != close_tag:
                continue
            if kind == ROW_KIND_DATA:
                data += 1
            else:
                gaps += 1
    return CloseTagRows(data, gaps, tuple(unreadable))


def _expirations_of(batches: list) -> list[str] | None:
    """The expirations of the newest batch that names any, or ``None`` for none.

    Split out so the column reads sit inside the caller's ``UNUSABLE_SEGMENT`` handler. A
    segment whose schema drifted opens cleanly and raises here, on the column, which is
    the shape that took the daemon down in #100.

    A data batch whose ``expiration_date`` is null on every row is walked past rather than
    answered with an empty list. Two shapes reach that. The vendor can send contracts that
    carry no expiration at all, and a vendor that retypes ``expirationDate`` across the
    chain has every contract's value routed into ``extra`` with the column left null.
    Either way the batch names no series. Stopping on it would hand the caller an empty
    answer that reads as this ticker having no expirations, when an older batch knows
    better. That is the same blinding the walk past a gap-only segment exists to prevent.
    """
    for batch in reversed(batches):
        kinds = batch.column(ROW_KIND_COLUMN).to_pylist()
        if ROW_KIND_DATA not in kinds:
            continue
        expirations = batch.column("expiration_date").to_pylist()
        dates = {
            str(exp).split("T")[0]
            for kind, exp in zip(kinds, expirations, strict=True)
            if kind == ROW_KIND_DATA and exp
        }
        if dates:
            return sorted(dates)
    return None


def latest_expirations(
    lake_root: Path | str, ticker: str, *, on: date | str | None = None
) -> list[str] | None:
    """The distinct expiration dates in a ticker's latest prior durable chains batch.

    This is the last-durable-batch read. The capture chunker makes it on its failure path
    to name absence markers without a live lookup, since every successful fetch already
    returned the full expiration set. It is general on purpose: it reads from disk only when
    called, and never reads the wall clock.

    Gap marking does not use this. The hole-aware startup walk needs the whole set of
    minutes already recorded for a ticker-day, counting marker rows as well as data rows,
    so it can subtract them from the minutes the day owed. ``recorded_slots`` is that read.

    The segment is located through the manifest. Capture appends one entry per segment in
    cycle order, keyed by the segment path, so the ticker's chains-segment entries in file
    order are its segments oldest to newest. The read walks them newest first, opens each
    with the Arrow IPC stream reader, and takes every complete batch, so a torn tail is
    safe. The first segment holding a batch that names an expiration wins. That batch's
    ``expiration_date`` values, reduced to their date part and de-duplicated, are the
    result, sorted. Walking back past a newer gap-only segment is what makes this the
    latest *durable data* batch rather than merely the latest segment, so a whole-chain gap
    last minute does not blind the marker. A data batch that names no expiration is walked
    past for the same reason, since it answers the question no better than a gap does.

    ``on`` narrows the walk to one session date. The capture chunker leaves it unset,
    because the expirations it names absent come from whatever batch is latest and a
    chain listed yesterday is still listed today. The close+5 guard sets it, because the
    baseline it compares a fill against is the day's own last loop-captured cycle. A
    cross-day baseline would call a series that expired yesterday missing from today's
    fill, which is a shortfall that cannot be true.

    ``None`` means nothing in scope names an expiration. That covers no manifest, no chains
    segment for the ticker, none on the named date, none of its segments holding a data
    batch, and every data batch leaving the column null. A manifested segment whose file is
    gone is skipped, never an error.
    """
    root = Path(lake_root)
    ordered: dict[str, dict] = {}
    for entry in read_manifest(root):
        rel = entry.get("partition")
        if isinstance(rel, str) and _is_chains_segment_for(rel, ticker, on):
            # Keep insertion at the last occurrence, so a re-recorded segment sorts by its
            # most recent entry while every segment still appears once.
            ordered.pop(rel, None)
            ordered[rel] = entry
    for rel in reversed(list(ordered)):
        path = root / rel
        if not path.exists():
            continue
        try:
            with pa.memory_map(str(path), "rb") as source:
                batches, _clean_eos = _complete_batches(pa.ipc.open_stream(source))
            found = _expirations_of(batches)
        except UNUSABLE_SEGMENT:
            continue
        if found is not None:
            return found
    return None
