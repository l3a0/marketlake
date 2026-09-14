"""The capture cycle across one real boundary: the filesystem.

These run one whole cycle over real journal segments and a real manifest, with the
cassette-backed fake vendor and a manual clock. No network and no wall clock are
crossed, and every write lands on a throwaway lake. So the tier is component: one
subsystem, capture, over real files, with the vendor and clock still fake.

They cover the cycle's observable contract:

1. A happy cycle writes chains and quotes segments with the right rows and the right
   ``snap_ts`` / ``fetch_ts`` / ``vendor_quote_ts`` stamps.
2. A failing chain fetch gaps only that ticker while the others still capture, and a
   known field whose value its column refuses costs that field rather than the minute.
3. A failing quote batch gaps every ticker's quotes, because the sampler is one shared
   failure unit.
4. The manifest gains one entry per segment, keyed by the segment path.
5. The journal metadata gains the cycle's token mint time and roster, and a vendor that
   cannot name its mint time costs the stamp rather than the cycle.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import capture, journal
from lake.cassette import Cassette, Interaction, load_cassette
from lake.chain_plan import ChainPlan
from lake.manifest import latest_entries, sha256_file
from lake.metadata import JournalMetadata, read_metadata
from lake.tickers import Roster
from lake.vendor import VendorError
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

CASSETTES = Path(__file__).parents[1] / "cassettes"

# A single open-ended window. It makes each ticker's chain one fetch, bounded by
# ``from_date`` = the session date and ``to_date`` = None, which the checked-in cassettes
# record. The chunking tree itself is exercised in tests/component/test_chain_chunking.py;
# here the plan just keeps the cycle to one deterministic chain request per ticker.
_ONE_WINDOW = ChainPlan(((0, None),))

# A clock whose seconds are non-zero, so flooring to the minute is observable. 09:30:45
# Eastern, expressed in UTC.
_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
_EXPECTED_SNAP = datetime(2026, 8, 24, 13, 30, 0, tzinfo=UTC)

# The synthetic vendor quote times the cassettes carry, as ISO strings. The chain stamp
# is derived per contract from ``quoteTimeInLong``; every contract in the cassette carries
# the same value, so both rows share it. The quote stamp comes from the quote block's
# ``quoteTime``.
_CHAIN_VQT = datetime.fromtimestamp(1787000099000 / 1000.0, tz=UTC).isoformat()
_QUOTE_VQT = datetime.fromtimestamp(1787000100000 / 1000.0, tz=UTC).isoformat()

# The two pinned surfaces, named through the journal.
CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE


def _both_options() -> Roster:
    """A roster of two option-bearing tickers."""
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": True, "chain_cadence": "1m"},
        }
    )


def _spy_options_qqq_equity() -> Roster:
    """A roster with one option ticker and one equity-only ticker."""
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": False},
        }
    )


def _rows(segment: capture.SegmentOutcome) -> list[dict]:
    return journal.read_segment(segment.path).to_pylist()


def test_an_empty_roster_writes_no_segments_and_stamps_nothing_to_capture(lake_root):
    """A fully retired roster is idle by design, and the cycle result says so.

    ``run_cycle`` never touches the vendor here, which the fake would fail on if it
    tried: an empty roster has no options entries to chain and no symbols to batch, so
    ``get_quotes`` is never called either.
    """
    clock = ManualClock(start=_CLOCK_START)

    class _NoCallsVendor:
        def get_chain(self, *args, **kwargs):
            raise AssertionError("no chain fetch should run for an empty roster")

        def get_quotes(self, *args, **kwargs):
            raise AssertionError("no quote fetch should run for an empty roster")

    result = capture.run_cycle(
        clock, _NoCallsVendor(), Roster(()), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    assert result.segments == ()
    assert result.errors == ()
    assert result.nothing_to_capture is True


# -- 1. the happy cycle ------------------------------------------------------


def test_happy_cycle_writes_chains_and_quotes_with_correct_stamps(cassette_vendor, lake_root):
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(
        clock, cassette_vendor, _both_options(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    assert result.errors == ()
    assert result.snap_ts == _EXPECTED_SNAP
    assert result.nothing_to_capture is False

    # One data segment per option ticker on chains, one per roster ticker on quotes.
    assert {(s.surface, s.ticker) for s in result.segments} == {
        (CHAINS, "SPY"),
        (CHAINS, "QQQ"),
        (QUOTES, "SPY"),
        (QUOTES, "QQQ"),
    }
    assert all(s.row_kind == journal.ROW_KIND_DATA for s in result.segments)

    # The SPY chain wrote one row per contract, both stamps assigned by the loop.
    spy_chain = _rows(result.segment(CHAINS, "SPY"))
    assert len(spy_chain) == 2
    assert [r["occ_symbol"] for r in spy_chain] == [
        "SPY   260918C00650000",
        "SPY   260918P00650000",
    ]
    assert [r["bid"] for r in spy_chain] == [4.2, 3.8]
    # Each contract's totalVolume lands in the typed volume column, not the overflow.
    assert [r["volume"] for r in spy_chain] == [5555, 4444]
    for row in spy_chain:
        assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()
        assert row["fetch_ts"] == _CLOCK_START.isoformat()
        # The manual clock does not advance across the fetch, so the request-end stamp
        # equals the dispatch stamp here. It is still stamped, not null.
        assert row["fetch_end_ts"] == _CLOCK_START.isoformat()
        assert row["vendor_quote_ts"] == _CHAIN_VQT
        assert row["close_tag"] is None
        assert row["suspect"] is False
        # totalVolume no longer overflows into per-contract extra.
        assert row["extra"] is None

    # The QQQ chain captured independently.
    qqq_chain = _rows(result.segment(CHAINS, "QQQ"))
    assert len(qqq_chain) == 2
    assert qqq_chain[0]["occ_symbol"] == "QQQ   260918C00600000"

    # The batched quotes split per ticker, one row each, prices and entitlement flag.
    spy_quote = _rows(result.segment(QUOTES, "SPY"))[0]
    assert (spy_quote["bid"], spy_quote["ask"], spy_quote["last"]) == (649.98, 650.02, 650.0)
    assert spy_quote["realtime"] is True
    assert spy_quote["snap_ts"] == _EXPECTED_SNAP.isoformat()
    assert spy_quote["fetch_ts"] == _CLOCK_START.isoformat()
    assert spy_quote["fetch_end_ts"] == _CLOCK_START.isoformat()
    assert spy_quote["vendor_quote_ts"] == _QUOTE_VQT
    # The full quote block lands in its typed columns. quoteTime is still consumed into
    # vendor_quote_ts, not a column.
    assert spy_quote["bid_size"] == 5
    assert spy_quote["ask_size"] == 7
    assert spy_quote["high_price"] == 655.0
    assert spy_quote["low_price"] == 645.0
    assert spy_quote["mark"] == 650.0
    assert spy_quote["total_volume"] == 90000000
    assert spy_quote["volatility"] == 12.5
    assert spy_quote["security_status"] == "Normal"
    assert spy_quote["last_mic_id"] == "XNYS"
    # The quote block's 52-week fields stay distinct from fundamental's high_52 / low_52.
    assert spy_quote["week_52_high"] == 705.0
    assert spy_quote["high_52"] == 700.0
    # Schwab's CUSIP, a sibling of the quote block in a reference envelope field, is
    # captured raw in its own column.
    assert spy_quote["cusip"] == "111111111"
    # The full fundamental block lands in its typed columns: dividends plus valuation and
    # volume stats. peRatio and eps are captured now, not left behind.
    assert spy_quote["div_pay_amount"] == 1.75
    assert spy_quote["div_ex_date"] == "2026-09-18"
    assert spy_quote["next_div_pay_date"] == "2026-12-31"
    assert spy_quote["div_yield"] == 1.28
    assert spy_quote["pe_ratio"] == 24.5
    assert spy_quote["eps"] == 22.3
    assert spy_quote["avg_10_days_volume"] == 74000000.0
    assert spy_quote["last_earnings_date"] == "2026-07-30"
    # The regular-session block lands in its regular_market_* columns.
    assert spy_quote["regular_market_last_price"] == 649.5
    assert spy_quote["regular_market_last_size"] == 100
    assert spy_quote["regular_market_trade_time"] == 1787000100000
    # The extended-hours block lands in its extended_* columns. Its lastPrice collides
    # with the quote block's, and the two land in separate columns without overwriting.
    assert spy_quote["extended_last_price"] == 651.0
    assert spy_quote["extended_bid_price"] == 650.9
    assert spy_quote["extended_total_volume"] == 2000
    assert (spy_quote["last"], spy_quote["extended_last_price"]) == (650.0, 651.0)
    # Every field is recognized, so the namespaced overflow stays empty.
    assert spy_quote["extra"] is None

    qqq_quote = _rows(result.segment(QUOTES, "QQQ"))[0]
    assert qqq_quote["bid"] == 601.48
    assert qqq_quote["cusip"] == "222222222"
    assert qqq_quote["regular_market_last_price"] == 601.0
    assert qqq_quote["extended_last_price"] == 602.0
    assert qqq_quote["extra"] is None
    assert qqq_quote["div_pay_amount"] == 0.9
    assert qqq_quote["extra"] is None


# -- 4. the manifest entry per segment ---------------------------------------


def test_manifest_gains_one_entry_per_segment_keyed_by_the_segment_path(cassette_vendor, lake_root):
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(
        clock, cassette_vendor, _both_options(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    latest = latest_entries(lake_root)
    # Exactly the four segment paths, and nothing else, are keys in the manifest.
    assert set(latest) == set(result.partitions)
    assert len(result.partitions) == 4

    for segment in result.segments:
        assert segment.partition.startswith("journal/")
        entry = latest[segment.partition]
        assert entry["source"] == capture.CAPTURE_SOURCE
        assert entry["rows"] == segment.rows
        # The recorded checksum matches the segment on disk.
        assert entry["sha256"] == sha256_file(lake_root / segment.partition)


# -- 2. a failing chain gaps only that ticker --------------------------------


def test_failing_chain_gaps_only_that_ticker(lake_root):
    vendor = CassetteVendor(load_cassette(CASSETTES / "chain_fail.json"))
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(
        clock, vendor, _both_options(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    assert result.errors == ()

    # SPY's chain still captured, one contract row.
    spy_chain = result.segment(CHAINS, "SPY")
    assert spy_chain.row_kind == journal.ROW_KIND_DATA
    assert spy_chain.rows == 1

    # QQQ's only window returned a non-2xx status. That is not a size signal, so the window
    # is recorded once with its http class and never split. It is the only window, so the
    # whole chain gaps carrying that class, http_500, not a blanket chunk-failure.
    qqq_chain = result.segment(CHAINS, "QQQ")
    assert qqq_chain.row_kind == journal.ROW_KIND_GAP
    assert qqq_chain.error_class == "http_500"
    gap_row = _rows(qqq_chain)[0]
    assert gap_row["row_kind"] == journal.ROW_KIND_GAP
    assert gap_row["error_class"] == "http_500"
    assert gap_row["snap_ts"] == _EXPECTED_SNAP.isoformat()
    # A gap holds no market data.
    assert gap_row["bid"] is None and gap_row["open_interest"] is None

    # The quote sampler was untouched by the chain failure.
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA
    assert result.segment(QUOTES, "QQQ").row_kind == journal.ROW_KIND_DATA

    # The gap segment is manifested like any other segment.
    assert qqq_chain.partition in latest_entries(lake_root)


# -- 3. a failing quote batch gaps every ticker ------------------------------


def test_failing_quote_batch_gaps_every_ticker(lake_root):
    vendor = CassetteVendor(load_cassette(CASSETTES / "quote_fail.json"))
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(
        clock, vendor, _spy_options_qqq_equity(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    assert result.errors == ()

    # Chains are a per-ticker surface, so SPY's chain still captured.
    assert result.segment(CHAINS, "SPY").row_kind == journal.ROW_KIND_DATA

    # The batched quote request failed, so every roster ticker gets a quotes gap row,
    # the equity-only QQQ included. One shared failure unit, per-ticker gap rows.
    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert quote.row_kind == journal.ROW_KIND_GAP
        assert quote.error_class == "http_503"
        assert _rows(quote)[0]["error_class"] == "http_503"

    # An equity-only ticker never gets a chains segment.
    assert {(s.surface, s.ticker) for s in result.segments} == {
        (CHAINS, "SPY"),
        (QUOTES, "SPY"),
        (QUOTES, "QQQ"),
    }


# -- 5. the request round-trip is captured -----------------------------------


class _AdvancingVendor:
    """A vendor that advances the injected clock across each call.

    The manual clock only moves when told to. This wrapper advances it by a fixed span
    on each vendor call, modelling a request that takes real time. So ``fetch_end_ts``
    lands that span past ``fetch_ts`` and the round-trip is non-zero. A ``raise_chain``
    flag models a slow failure: the clock still advances, then the call raises, so even
    a timeout's duration is captured. The single-window plan makes one chain call per
    ticker, so the chain round-trip is that one call's span.
    """

    def __init__(self, inner, clock, *, chain_seconds, quote_seconds, raise_chain=False):
        self._inner = inner
        self._clock = clock
        self._chain_seconds = chain_seconds
        self._quote_seconds = quote_seconds
        self._raise_chain = raise_chain

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self._clock.advance(self._chain_seconds)
        if self._raise_chain:
            raise VendorError("slow timeout")
        return self._inner.get_chain(
            symbol, from_date=from_date, to_date=to_date, strike_count=strike_count
        )

    def get_quotes(self, symbols):
        self._clock.advance(self._quote_seconds)
        return self._inner.get_quotes(symbols)

    def token_mint_time(self):
        return self._inner.token_mint_time()


def _round_trip(row: dict) -> float:
    start = datetime.fromisoformat(row["fetch_ts"])
    end = datetime.fromisoformat(row["fetch_end_ts"])
    return (end - start).total_seconds()


def test_round_trip_is_captured_on_success_and_on_a_slow_failure(cassette_vendor, lake_root):
    clock = ManualClock(start=_CLOCK_START)
    vendor = _AdvancingVendor(cassette_vendor, clock, chain_seconds=0.4, quote_seconds=0.25)
    result = capture.run_cycle(
        clock, vendor, _both_options(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    # A chain is fetched by its plan of date windows, one window here, so one 0.4s request.
    # The chain round-trip is that request's span, from the first window's dispatch to the
    # last window landing. The shared quote batch is one 0.25s request. fetch_ts is re-read
    # before each operation, so the round-trip is that operation's own span, not a running
    # total.
    assert _round_trip(_rows(result.segment(CHAINS, "SPY"))[0]) == pytest.approx(0.4)
    assert _round_trip(_rows(result.segment(CHAINS, "QQQ"))[0]) == pytest.approx(0.4)
    assert _round_trip(_rows(result.segment(QUOTES, "SPY"))[0]) == pytest.approx(0.25)

    # A slow failure still stamps fetch_end_ts, so the gap row carries the failed request's
    # duration. The lone window raises after advancing 0.6s. A raised window fetch is a
    # transport failure, recorded once with its own class and never split, so the whole
    # chain is one gap spanning that 0.6s, tagged vendor_error rather than a chunk-failure.
    clock2 = ManualClock(start=_CLOCK_START)
    failing = _AdvancingVendor(
        cassette_vendor, clock2, chain_seconds=0.6, quote_seconds=0.25, raise_chain=True
    )
    result2 = capture.run_cycle(
        clock2, failing, _both_options(), lake_root, pid=4243, plan=_ONE_WINDOW
    )
    spy_gap = _rows(result2.segment(CHAINS, "SPY"))[0]
    assert spy_gap["row_kind"] == journal.ROW_KIND_GAP
    assert spy_gap["error_class"] == "vendor_error"
    assert _round_trip(spy_gap) == pytest.approx(0.6)


# -- 6. the CUSIP is captured wherever Schwab puts it ------------------------


def test_top_level_envelope_cusip_lands_in_the_column(lake_root):
    # Schwab may put the CUSIP as a top-level envelope field rather than in a reference
    # block. Both sites are captured. Here the equity-only QQQ carries a top-level cusip.
    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="quotes",
                params={"symbols": ["QQQ"]},
                status=200,
                body={
                    "QQQ": {
                        "assetMainType": "EQUITY",
                        "realtime": True,
                        "cusip": "333333333",
                        "quote": {"bidPrice": 1.0, "askPrice": 1.1, "lastPrice": 1.05},
                    }
                },
            ),
        )
    )
    roster = Roster.from_mapping({"QQQ": {"options": False}})
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(cassette),
        roster,
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )
    row = _rows(result.segment(QUOTES, "QQQ"))[0]
    assert row["cusip"] == "333333333"
    assert row["extra"] is None


# -- 6b. a value its column refuses ------------------------------------------


def _chain_body_with(**contract_overrides) -> dict:
    """A one-contract SPY chain body, the contract overridden by the caller.

    Only the fields the row builder reads are set. Everything it does not find stays
    null, which is the same fail-open a sparse vendor payload gets.
    """
    contract = {
        "putCall": "CALL",
        "symbol": "SPY   260918C00650000",
        "bid": 4.2,
        "ask": 4.25,
        "openInterest": 1234,
        "quoteTimeInLong": 1787000099000,
        "expirationDate": "2026-09-18T20:00:00.000+00:00",
    }
    contract.update(contract_overrides)
    return {
        "symbol": "SPY",
        "status": "SUCCESS",
        "isDelayed": False,
        "underlyingPrice": 650.01,
        "isChainTruncated": False,
        "numberOfContracts": 1,
        "underlying": None,
        "callExpDateMap": {"2026-09-18:25": {"650.0": [contract]}},
        "putExpDateMap": {},
    }


def _one_chain_cassette(body: dict) -> Cassette:
    """A cassette serving that chain body for SPY, plus the quote batch SPY needs."""
    return Cassette(
        interactions=(
            Interaction(
                endpoint="chains",
                params={"symbol": "SPY", "from_date": "2026-08-24"},
                status=200,
                body=body,
            ),
            Interaction(
                endpoint="quotes",
                params={"symbols": ["SPY"]},
                status=200,
                body={
                    "SPY": {
                        "assetMainType": "EQUITY",
                        "realtime": True,
                        "quote": {
                            "bidPrice": 649.98,
                            "askPrice": 650.02,
                            "quoteTime": 1787000100000,
                        },
                    }
                },
            ),
        )
    )


def _spy_only() -> Roster:
    """A roster of the one option-bearing ticker."""
    return Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}})


def test_a_retyped_known_field_lands_the_cycle_and_parks_the_raw_value(lake_root):
    """A vendor integer field sent fractional must cost the field, not the minute.

    This is the whole path, driven through ``run_cycle`` rather than the row builder, so
    the routing is exercised where it actually runs and the assertion reads the segment
    back off disk. Before the routing, the raise reached ``_plan_chain``'s fail-open and
    the ticker gapped under ``arrow_invalid``, which cost every minute the vendor held the
    new shape and threw the value away on top.

    Now the chain lands as data, ``open_interest`` is null because 1234 was never what the
    vendor sent, and 1234.7 is in ``extra`` where the read-time refusal in marketlake #149
    will find it.
    """
    vendor = CassetteVendor(_one_chain_cassette(_chain_body_with(openInterest=1234.7)))
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START), vendor, _spy_only(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    assert result.errors == ()
    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert chain.error_class is None

    row = _rows(chain)[0]
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert row["open_interest"] is None
    assert json.loads(row["extra"]) == {"openInterest": 1234.7}
    # The rest of the contract is untouched, so one drifted field costs one field.
    assert (row["bid"], row["ask"]) == (4.2, 4.25)
    assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()

    # One surface drifting never takes the other down.
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


def test_the_same_chain_captures_when_the_integer_field_is_whole(lake_root):
    """The control. Only the drifted value routes, so the routing is not blanket.

    Without this the test above would still pass if the row builder had been broken
    outright, since a builder that nulled every open interest would null this one too.
    """
    vendor = CassetteVendor(_one_chain_cassette(_chain_body_with(openInterest=1234.0)))
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START), vendor, _spy_only(), lake_root, pid=4242, plan=_ONE_WINDOW
    )

    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    row = _rows(chain)[0]
    assert row["open_interest"] == 1234
    assert row["extra"] is None


def test_a_retyped_chain_level_field_lands_the_cycle_and_parks_the_raw_value(lake_root):
    """The chain-level fields route too, driven through the whole cycle.

    ``underlyingPrice`` is read off the top of the chain body and repeated onto every
    contract row, which is why it used to have no key in an overflow built from the
    contract dict alone. It gapped the ticker under ``arrow_invalid`` for as long as the
    vendor held the shape, and the price the design's IV inversion reads went in the bin
    with the minute.

    Now the chain lands as data, the column is null because a string was never a price, and
    the vendor's own value sits under ``chain`` in ``extra``. The nesting is what keeps it
    apart from anything a contract sends.
    """
    body = _chain_body_with()
    body["underlyingPrice"] = "six hundred and fifty"
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(_one_chain_cassette(body)),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    assert result.errors == ()
    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert chain.error_class is None

    row = _rows(chain)[0]
    assert row["underlying_price"] is None
    assert json.loads(row["extra"]) == {"chain": {"underlyingPrice": "six hundred and fifty"}}
    # The contract's own fields are untouched, so a chain-level retype costs one column.
    assert (row["bid"], row["ask"]) == (4.2, 4.25)
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


def test_a_retyped_envelope_field_lands_the_quote_cycle_and_parks_the_raw_value(lake_root):
    """The same on the other surface, where the two fields sit outside every block.

    ``realtime`` is the entitlement flag the validation battery checks, read off the
    per-symbol envelope rather than a captured block, so only a block's leftovers used to
    overflow and a retype cost the quote for the minute. It routes under ``envelope`` now,
    and the chain for the same cycle is untouched either way.
    """
    body = _chain_body_with()
    cassette = _one_chain_cassette(body)
    quotes = cassette.interactions[1]
    envelope = dict(quotes.body["SPY"], realtime="yes")
    cassette = Cassette(
        interactions=(
            cassette.interactions[0],
            Interaction(
                endpoint="quotes",
                params=quotes.params,
                status=quotes.status,
                body={"SPY": envelope},
            ),
        )
    )
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(cassette),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    assert result.errors == ()
    quote = result.segment(QUOTES, "SPY")
    assert quote.row_kind == journal.ROW_KIND_DATA
    assert quote.error_class is None

    row = _rows(quote)[0]
    assert row["realtime"] is None
    assert json.loads(row["extra"]) == {"envelope": {"realtime": "yes"}}
    assert row["bid"] == 649.98
    assert result.segment(CHAINS, "SPY").row_kind == journal.ROW_KIND_DATA


def test_a_column_the_builder_transforms_still_gaps_under_its_own_class(lake_root):
    """What the routing deliberately leaves alone, read at the class an operator sees.

    ``quoteTimeInLong`` is consumed into ``vendor_quote_ts`` rather than copied, so the
    column holds a value this code computed and the overflow has no key for it. The
    epoch-to-ISO conversion refuses before any column is built, the raise reaches
    ``_plan_chain``'s fail-open, and the ticker gaps under the exception's own name.

    ``chain_schema_drift`` is not that name and never was. It guards the fetch, where a body
    that will not merge is split and given up, and the row build runs well past it. So the
    class on disk here is ``value_error``, and the quote sampler is untouched.
    """
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(_one_chain_cassette(_chain_body_with(quoteTimeInLong="not-an-epoch"))),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_GAP
    assert chain.error_class == "value_error"
    gap_row = _rows(chain)[0]
    assert gap_row["error_class"] == "value_error"
    assert gap_row["vendor_quote_ts"] is None
    assert gap_row["extra"] is None
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


def test_a_retype_that_lands_stays_scoped_to_the_contract_that_drifted(lake_root):
    """A second ticker's chain is untouched, and so is the first ticker's other field."""
    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="chains",
                params={"symbol": "SPY", "from_date": "2026-08-24"},
                status=200,
                body=_chain_body_with(openInterest=1234.7),
            ),
            Interaction(
                endpoint="chains",
                params={"symbol": "QQQ", "from_date": "2026-08-24"},
                status=200,
                body=_chain_body_with(),
            ),
            Interaction(
                endpoint="quotes",
                params={"symbols": ["SPY", "QQQ"]},
                status=200,
                body={
                    sym: {
                        "assetMainType": "EQUITY",
                        "realtime": True,
                        "quote": {"bidPrice": 1.0, "askPrice": 1.1, "quoteTime": 1787000100000},
                    }
                    for sym in ("SPY", "QQQ")
                },
            ),
        )
    )
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(cassette),
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    spy = result.segment(CHAINS, "SPY")
    assert spy.row_kind == journal.ROW_KIND_DATA
    assert json.loads(_rows(spy)[0]["extra"]) == {"openInterest": 1234.7}
    qqq = result.segment(CHAINS, "QQQ")
    assert qqq.row_kind == journal.ROW_KIND_DATA
    assert _rows(qqq)[0]["open_interest"] == 1234
    assert _rows(qqq)[0]["extra"] is None


# -- 7. the journal metadata stamp -------------------------------------------


def test_a_cycle_stamps_the_token_mint_time_and_the_roster(cassette_vendor, lake_root):
    # The Now panel's token age comes from the lake, never from ``~/.config``. The stamp
    # is what carries it there, and the mint comes off the vendor the cycle fetched with.
    capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        cassette_vendor,
        _spy_options_qqq_equity(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    stamp = read_metadata(lake_root)
    assert stamp.token_minted_at == cassette_vendor.token_mint_time()
    assert stamp.stamped_at == _EXPECTED_SNAP
    # The equity-only ticker is stamped on quotes alone, matching what the cycle wrote.
    assert stamp.tickers == {"SPY": ("chains", "quotes"), "QQQ": ("quotes",)}


def test_a_vendor_that_cannot_name_its_mint_time_still_captures(lake_root):
    # A stamp is a report about the cycle, not part of it. This cassette carries no mint
    # time, so ``token_mint_time`` raises and the rows must land regardless.
    recorded = load_cassette(CASSETTES / "spy_minimal.json")
    vendor = CassetteVendor(Cassette(interactions=recorded.interactions))
    with pytest.raises(VendorError):
        vendor.token_mint_time()

    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    assert result.errors == ()
    assert len(_rows(result.segment(CHAINS, "SPY"))) == 2
    assert read_metadata(lake_root) == JournalMetadata()
