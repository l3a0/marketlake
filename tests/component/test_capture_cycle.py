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
   The column that refused rides out on the segment outcome, because the batch does not
   survive the write and the daemon's schema-drift page is what reads it.
3. A failing quote batch gaps every ticker's quotes, because the sampler is one shared
   failure unit.
4. The manifest gains one entry per segment the plan wrote, keyed by the segment path, and
   none for a failed write's gap row. The cycle reads the manifest once and hashes each
   segment as it closes (marketlake #573). A hash that fails there is taken again under
   the lock, and a cycle with nothing to record reads nothing.
5. The journal metadata gains the cycle's token mint time and roster, and a vendor that
   cannot name its mint time costs the stamp rather than the cycle.
6. A segment write the disk refuses leaves a gap row at the failed segment's path and a
   line in the daemon's log, and a diagnostic that cannot print costs nothing
   (marketlake #769).
"""

from __future__ import annotations

import errno
import gc
import json
import sys
import weakref
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from lake import capture, gap, journal, manifest
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans
from lake.cassette import Cassette, Interaction, load_cassette
from lake.chain_plan import ChainPlan
from lake.config import GuardConstants
from lake.manifest import latest_entries, manifest_path, sha256_file
from lake.metadata import JournalMetadata, read_metadata
from lake.security_master import SecurityMaster
from lake.session import SessionClock
from lake.tickers import Roster
from lake.vendor import VendorError
from tests.support.calendar import weekday_sessions
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
# The default pause between two submissions to the capture pool, written as a literal so a
# change to the constant fails these tests rather than moving with them.
_STAGGER = timedelta(milliseconds=50)
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
    # The default cap fires the cycle's three requests through one pool, 50 ms apart on the
    # injected clock: the quote request at the start, SPY's one window one stagger later,
    # QQQ's one after that. A chain's fetch_ts is when its first window was submitted. Its
    # fetch_end_ts is when its own task finished, read on the pool thread, which lands
    # somewhere in the submissions still under way, since only the stagger moves the manual
    # clock. So the round trip is stamped, not null, and it falls inside that span.
    for row in spy_chain:
        assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()
        assert row["fetch_ts"] == (_CLOCK_START + _STAGGER).isoformat()
        assert row["fetch_ts"] <= row["fetch_end_ts"] <= (_CLOCK_START + 2 * _STAGGER).isoformat()
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
    assert _CLOCK_START.isoformat() <= spy_quote["fetch_end_ts"]
    assert spy_quote["fetch_end_ts"] <= (_CLOCK_START + 2 * _STAGGER).isoformat()
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
        assert segment.fetched_at is not None
        assert entry["fetched_at"] == segment.fetched_at
        # The recorded checksum matches the segment on disk.
        assert entry["sha256"] == sha256_file(lake_root / segment.partition)


def test_a_gap_segment_entry_counts_its_gap_row(lake_root):
    # QQQ's chain gaps, so its segment holds one row and no data row. The manifest records
    # the rows in the file, which is what compaction checks the sealed partition against.
    vendor = CassetteVendor(load_cassette(CASSETTES / "chain_fail.json"))
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    gap = result.segment(CHAINS, "QQQ")
    assert (gap.rows, gap.data_rows) == (1, 0)
    assert latest_entries(lake_root)[gap.partition]["rows"] == 1


def _cycle(
    lake_root: Path,
    *,
    minute: int = 0,
    cap: int | None = None,
    cassette: str = "spy_minimal.json",
) -> capture.CycleResult:
    """One four-segment cycle, ``minute`` minutes after the clock's start.

    ``cap`` is the concurrency cap, which picks the landing path. Left unset, the cycle
    takes the default.
    """
    return capture.run_cycle(
        ManualClock(start=_CLOCK_START + timedelta(minutes=minute)),
        CassetteVendor(load_cassette(CASSETTES / cassette)),
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
        guards=None if cap is None else GuardConstants(capture_max_concurrency=cap),
    )


def test_a_four_segment_cycle_reads_the_manifest_once(lake_root, monkeypatch):
    # The first cycle leaves four entries for the second one's read to find. The second
    # reads the manifest once for all four of its segments, not once per segment, which is
    # what made the time the lock is held grow with the lake (marketlake #573).
    _cycle(lake_root)
    real = manifest.latest_entries
    reads: list[int] = []

    def counted(root):
        reads.append(1)
        return real(root)

    monkeypatch.setattr(manifest, "latest_entries", counted)

    result = _cycle(lake_root, minute=1)

    assert len(result.segments) == 4
    assert len(reads) == 1
    assert set(result.partitions) <= set(real(lake_root))


@pytest.mark.parametrize(
    "exc",
    [RuntimeError("a bug in the hash"), PermissionError(1, "Operation not permitted")],
    ids=["runtime", "permission"],
)
def test_a_hash_that_fails_at_close_still_lands_the_entry_hashed_under_the_lock(
    lake_root, monkeypatch, capsys, exc
):
    # The segment is durable when its hash fails, so the cycle must not report it as a write
    # failure. The entry is still written, with the hash the append takes from the file.
    def refuse(path):
        raise exc

    monkeypatch.setattr(capture, "sha256_file", refuse)
    real = manifest.sha256_file
    hashed_under_the_lock: list[str] = []

    def counted(path):
        hashed_under_the_lock.append(Path(path).relative_to(lake_root).as_posix())
        return real(path)

    monkeypatch.setattr(manifest, "sha256_file", counted)

    result = _cycle(lake_root)

    assert result.errors == ()
    assert len(result.segments) == 4
    assert hashed_under_the_lock == list(result.partitions)
    latest = latest_entries(lake_root)
    for segment in result.segments:
        assert latest[segment.partition]["sha256"] == real(segment.path)
    err = capsys.readouterr().err
    assert err.count("capture: hash at close failed on ") == 4
    assert type(exc).__name__ in err


def test_a_cycle_with_no_segment_to_record_returns_on_a_damaged_manifest(lake_root, monkeypatch):
    # Every planned write fails, so the cycle has no entry to append, and the gap rows it
    # writes in their place get none. It reads nothing either, so a manifest holding a byte
    # that will not decode does not end it.
    _cycle(lake_root)
    ledger = manifest_path(lake_root)
    raw = ledger.read_bytes()
    damaged = raw[:10] + b"\xff" + raw[11:]
    ledger.write_bytes(damaged)

    def refuse(self, surface, ticker, plan):
        raise OSError("disk refused")

    monkeypatch.setattr(capture._CaptureCycle, "_write", refuse)

    result = _cycle(lake_root, minute=1)

    assert result.segments == ()
    assert len(result.errors) == 4
    assert ledger.read_bytes() == damaged


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
    # is recorded with its http class and never split. A 500 is sent once more first (#558),
    # and the cassette answers it the same way. It is the only window, so the
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
    # The vendor advances the manual clock inside each call to model a request that takes
    # time, which is only well defined when one call runs at a time. So this runs at a cap
    # of 1, the sequential cycle, where each round trip is its own call's span. The
    # concurrent cycle's round trip is covered in test_capture_concurrency.py.
    sequential = GuardConstants(capture_max_concurrency=1)
    clock = ManualClock(start=_CLOCK_START)
    vendor = _AdvancingVendor(cassette_vendor, clock, chain_seconds=0.4, quote_seconds=0.25)
    result = capture.run_cycle(
        clock, vendor, _both_options(), lake_root, pid=4242, plan=_ONE_WINDOW, guards=sequential
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
        clock2, failing, _both_options(), lake_root, pid=4243, plan=_ONE_WINDOW, guards=sequential
    )
    spy_gap = _rows(result2.segment(CHAINS, "SPY"))[0]
    assert spy_gap["row_kind"] == journal.ROW_KIND_GAP
    assert spy_gap["error_class"] == "vendor_error"
    assert _round_trip(spy_gap) == pytest.approx(0.6)


def test_a_raised_quote_request_keeps_its_round_trip_at_a_cap_of_one(cassette_vendor, lake_root):
    # The sequential cycle stamps the quote's end after the raise, so the gap rows carry how
    # long the failed request took, the same as a raised chain window does above.
    clock = ManualClock(start=_CLOCK_START)

    class _SlowQuoteFailure(_AdvancingVendor):
        def get_quotes(self, symbols):
            self._clock.advance(self._quote_seconds)
            raise VendorError("quote batch timed out")

    vendor = _SlowQuoteFailure(cassette_vendor, clock, chain_seconds=0.4, quote_seconds=0.7)
    result = capture.run_cycle(
        clock,
        vendor,
        _both_options(),
        lake_root,
        pid=4244,
        plan=_ONE_WINDOW,
        guards=GuardConstants(capture_max_concurrency=1),
    )

    gap = _rows(result.segment(QUOTES, "SPY"))[0]
    assert gap["error_class"] == "vendor_error"
    assert _round_trip(gap) == pytest.approx(0.7)


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


def _one_chain_cassette(body: dict, *, quote_time: object = 1787000100000) -> Cassette:
    """A cassette serving that chain body for SPY, plus the quote batch SPY needs.

    ``quote_time`` is the quote block's own vendor stamp, overridable so a quotes-surface
    drift can be driven through a whole cycle the way a chains one is.
    """
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
                            "quoteTime": quote_time,
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


def test_a_retype_puts_its_column_on_the_segment_outcome_the_daemon_reads(lake_root):
    """The drift signature has to survive the write, because the batch does not.

    ``_batch`` knows exactly which columns refused and returns only the batch, and the
    batch is closed into a segment and dropped inside the cycle. So the outcome is what
    carries the fact out, and without it the daemon's schema-drift page has nothing to
    read and a vendor retype reaches nobody.

    Both surfaces are driven in one cycle, because the scan runs per batch and a wiring
    that only reached the chains builder would pass a chains-only test.
    """
    body = _chain_body_with(openInterest=1234.7)
    cassette = _one_chain_cassette(body)
    quotes = cassette.interactions[1]
    cassette = Cassette(
        interactions=(
            cassette.interactions[0],
            Interaction(
                endpoint="quotes",
                params=quotes.params,
                status=quotes.status,
                body={"SPY": dict(quotes.body["SPY"], realtime="yes")},
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
    assert result.segment(CHAINS, "SPY").routed_columns == ("open_interest",)
    assert result.segment(QUOTES, "SPY").routed_columns == ("realtime",)


def test_a_drift_scan_that_raises_costs_the_finding_and_never_the_minute(lake_root, capsys):
    """The scan is a page's input and the segment is a minute, so they are not equal stakes.

    A minute cannot be bought back and a page can be sent again. So a scan that raised has
    to leave the segment written, manifested and readable, carrying no column name, which
    reads as the ordinary cycle it otherwise is. Without the guard a bug in a diagnostic
    would gap every ticker on every cycle for as long as it stood.

    The failure is not silent either. It reaches the launchd log the restart script already
    sends the operator to.
    """

    def explode(surface, batch):
        raise RuntimeError("a bug in the scan")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(journal, "routed_columns", explode)
    try:
        result = capture.run_cycle(
            ManualClock(start=_CLOCK_START),
            CassetteVendor(_one_chain_cassette(_chain_body_with(openInterest=1234.7))),
            _spy_only(),
            lake_root,
            pid=4242,
            plan=_ONE_WINDOW,
        )
    finally:
        monkeypatch.undo()

    assert result.errors == ()
    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert chain.routed_columns == ()
    # The minute is on disk, manifested, and the routed value is still in the rows.
    assert chain.path.exists()
    assert chain.partition in latest_entries(lake_root)
    assert json.loads(_rows(chain)[0]["extra"]) == {"openInterest": 1234.7}
    assert "schema-drift scan failed on chains SPY: RuntimeError" in capsys.readouterr().err


def test_an_ordinary_cycle_leaves_every_outcome_carrying_no_drifted_column(lake_root):
    """The steady state, which is every cycle the lake has ever recorded.

    ``extra`` was non-null on zero of the lake's 9,846,266 sealed rows, so an outcome
    naming a column here would be a page a minute for as long as the daemon ran.
    """
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(_one_chain_cassette(_chain_body_with())),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    assert result.errors == ()
    assert [segment.routed_columns for segment in result.segments] == [(), ()]


def test_a_quote_time_the_transform_refuses_lands_the_minute_instead_of_gapping_it(lake_root):
    """A whole cycle over a non-numeric ``quoteTimeInLong``, read at the rows on disk.

    This used to gap the ticker. The epoch-to-ISO conversion raised, the raise reached
    ``_plan_chain``'s fail-open, and a minute of every contract on the chain was spent on
    one bad field. A minute is unrecoverable, so that was the worst outcome available, and
    marketlake #223 ends it.

    What lands instead is a data row per contract. ``vendor_quote_ts`` is null, because no
    stamp can be made of this value, and the vendor's own value sits in ``extra`` under its
    own name. That name in ``extra`` is the signature marketlake #129 pinned, and it is
    what keeps a null here readable: absent still means the vendor sent nothing, and
    refused says so out loud.

    Every other column on the row is untouched, so the bad field costs the field alone. The
    quote sampler is untouched too, as it was before.
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
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert chain.error_class is None
    for row in _rows(chain):
        assert row["row_kind"] == journal.ROW_KIND_DATA
        assert row["vendor_quote_ts"] is None
        assert json.loads(row["extra"]) == {"quoteTimeInLong": "not-an-epoch"}
        # The rest of the contract still landed. One refused field costs that field alone.
        assert row["bid"] is not None
        assert row["open_interest"] is not None
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


def test_a_quote_block_quote_time_the_transform_refuses_lands_the_minute_too(lake_root):
    """The same rule on the quotes surface, driven through the whole cycle.

    Both surfaces call one transform, so this test and the one above drive the same rule
    down two different paths rather than two copies of it. Both halves are read off disk
    here: ``lake.capture`` nulls the stamp, and the journal's projection
    stops counting ``quoteTime`` as consumed for this envelope and overflows it under the
    block it arrived in.

    A bool is the shape driven, because it is the one that raised nothing and so left no
    trace at all. The chains half of the same rule is the test above.
    """
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(_one_chain_cassette(_chain_body_with(), quote_time=True)),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    quotes = result.segment(QUOTES, "SPY")
    assert quotes.row_kind == journal.ROW_KIND_DATA
    row = _rows(quotes)[0]
    assert row["vendor_quote_ts"] is None
    routed = json.loads(row["extra"])["quote"]["quoteTime"]
    assert routed is True
    # The rest of the quote block still landed, so the refused field costs itself alone.
    assert (row["bid"], row["ask"]) == (649.98, 650.02)
    # The chain leg of the same cycle is untouched, stamp and all.
    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert _rows(chain)[0]["vendor_quote_ts"] == _CHAIN_VQT


def test_the_quotes_stamp_keeps_the_milliseconds_the_vendor_sent(lake_root):
    """The quotes surface builds its stamp in ``lake.capture``, so it is checked there.

    Every other epoch in this file ends in three zeros, which is what a hand-written
    cassette looks like and not what Schwab sends. A stamp that floored to the second would
    read as correct against all of them, and ``vendor_quote_ts`` is a string column, so the
    schema checks nothing either. Per-row staleness is measured off this stamp, so the lost
    digits would come out of a measurement rather than a label.
    """
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        CassetteVendor(_one_chain_cassette(_chain_body_with(), quote_time=1787000100123)),
        _spy_only(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )

    row = _rows(result.segment(QUOTES, "SPY"))[0]
    assert row["vendor_quote_ts"] == datetime.fromtimestamp(1787000100.123, tz=UTC).isoformat()
    assert row["vendor_quote_ts"].endswith(".123000+00:00")


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


# -- 8. a segment write the disk refuses -------------------------------------


class _RefusingSink:
    """The handle a segment is written through, refusing as the disk under it would.

    ``write`` refuses every write. ``marker`` refuses only what follows the batch's
    durability flush, which is the end-of-stream marker. ``None`` passes everything.
    """

    def __init__(self, inner, mode: str | None) -> None:
        self._inner = inner
        self._mode = mode
        self.flushed = False

    def write(self, data) -> int:
        if self._mode == "write" or (self._mode == "marker" and self.flushed):
            raise OSError(errno.ENOSPC, "No space left on device")
        return self._inner.write(data)

    @property
    def closed(self) -> bool:
        return self._inner.closed


def _refuse_writes(monkeypatch, refused: dict[tuple[str, str], list[str]]) -> None:
    """A disk that refuses the named segments, with the real writer on top of it.

    Each key is a surface and ticker, and its list names how each segment opened there
    fails, in order: the data segment first, then the gap row. Opening is never refused,
    because a refused open would refuse the gap row for a reason a full disk does not
    give. Every segment is written on the cycle's own thread, opened and closed before the
    next one opens, so the most recent sink is the one a flush belongs to.
    """
    queues = {key: list(modes) for key, modes in refused.items()}
    current: dict[str, object] = {"mode": None, "sink": None}
    real_open = journal.SegmentWriter.open
    real_file = pa.PythonFile
    real_flush = journal.SegmentWriter._flush_durable

    def open_(root, surface, ticker, *args, **kwargs):
        queue = queues.get((surface, ticker))
        current["mode"] = queue.pop(0) if queue else None
        return real_open(root, surface, ticker, *args, **kwargs)

    def python_file(handle, mode=None):
        sink = _RefusingSink(handle, current["mode"])
        current["sink"] = sink
        return real_file(sink, mode=mode)

    def flush(writer):
        real_flush(writer)
        current["sink"].flushed = True

    monkeypatch.setattr(journal.SegmentWriter, "open", open_)
    monkeypatch.setattr(pa, "PythonFile", python_file)
    monkeypatch.setattr(journal.SegmentWriter, "_flush_durable", flush)


def _files(lake_root: Path, surface: str, ticker: str) -> list[Path]:
    directory = journal.segment_dir(lake_root, surface, ticker, _EXPECTED_SNAP.date())
    return sorted(directory.glob("*.arrows"))


def _failure_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "segment write failed" in line]


_CAPS = pytest.mark.parametrize("cap", [1, 20])
_SEGMENT_FULL = capture.SegmentError(QUOTES, "SPY", "o_s_error")


@_CAPS
def test_a_refused_data_write_leaves_one_gap_row_at_its_own_path(lake_root, monkeypatch, cap):
    """The minute was fetched and the disk refused it, so the lake says so (marketlake #769)."""
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"]})

    result = _cycle(lake_root, cap=cap)

    assert result.errors == (_SEGMENT_FULL,)
    assert [(s.surface, s.ticker) for s in result.segments] == [
        (CHAINS, "SPY"),
        (CHAINS, "QQQ"),
        (QUOTES, "QQQ"),
    ]
    # The failed segment's own name, the cycle's start stamp and pid, as its neighbour has.
    [path] = _files(lake_root, QUOTES, "SPY")
    assert path.name == result.segment(QUOTES, "QQQ").path.name
    [row] = journal.read_segment(path).to_pylist()
    landed = _rows(result.segment(QUOTES, "QQQ"))[0]
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["error_class"] == capture.SEGMENT_WRITE_FAILED == "segment_write_failed"
    assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()
    # The quote batch was one fetch, so its stamps are the ones the landed row carries.
    assert (row["fetch_ts"], row["fetch_end_ts"]) == (landed["fetch_ts"], landed["fetch_end_ts"])
    present = journal.recorded_slots(lake_root, QUOTES, "SPY", _EXPECTED_SNAP.date())
    assert present.slots == frozenset({_EXPECTED_SNAP})
    # Like a startup marker, the row has no manifest entry, and the result leaves it out.
    partition = path.relative_to(lake_root).as_posix()
    assert partition not in latest_entries(lake_root)
    assert set(latest_entries(lake_root)) == set(result.partitions)


@_CAPS
def test_a_refused_gap_row_is_written_again_with_its_own_class(lake_root, monkeypatch, cap):
    """The fetch failed first, so its class says what lost the minute."""
    _refuse_writes(monkeypatch, {(CHAINS, "QQQ"): ["write"]})

    result = _cycle(lake_root, cap=cap, cassette="chain_fail.json")

    assert result.errors == (capture.SegmentError(CHAINS, "QQQ", "o_s_error"),)
    [path] = _files(lake_root, CHAINS, "QQQ")
    [row] = journal.read_segment(path).to_pylist()
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["error_class"] == "http_500"
    assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()


@_CAPS
def test_a_gap_plan_that_raises_does_not_end_the_cycle(lake_root, monkeypatch, capsys, cap):
    """A raise out of the cycle ends the daemon, so building the gap row is guarded too."""
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"]})

    def broken(self, *args):
        raise RuntimeError("a bug in the gap plan")

    monkeypatch.setattr(capture._CaptureCycle, "_gap_plan", broken)

    result = _cycle(lake_root, cap=cap)

    assert result.errors == (_SEGMENT_FULL,)
    assert len(result.segments) == 3
    assert _files(lake_root, QUOTES, "SPY") == []
    [line] = _failure_lines(capsys.readouterr().err)
    assert line.endswith("the gap row did not land: RuntimeError: a bug in the gap plan")


@_CAPS
def test_each_failed_write_prints_one_line_in_plan_order(lake_root, monkeypatch, capsys, cap):
    """The quotes usually land before the chains above a cap of 1, and the lines do not."""
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"], (CHAINS, "QQQ"): ["write"]})

    _cycle(lake_root, cap=cap)

    prefix = f"capture: {_EXPECTED_SNAP.isoformat()}: segment write failed: "
    reason = "(ENOSPC): OSError: [Errno 28] No space left on device; the gap row landed"
    assert _failure_lines(capsys.readouterr().err) == [
        f"{prefix}chains QQQ {reason}",
        f"{prefix}quotes SPY {reason}",
    ]


@_CAPS
def test_when_the_gap_row_fails_too_a_restart_marks_the_minute(lake_root, monkeypatch, capsys, cap):
    """The late stage of a full disk, where the small gap row fails with the data.

    The log line is then the only record, and the cycle still returns. This shows what
    startup marking does after a restart, and nothing here makes a restart happen.
    """
    _refuse_writes(monkeypatch, {(CHAINS, "QQQ"): ["write", "write"]})

    result = _cycle(lake_root, cap=cap)

    assert result.errors == (capture.SegmentError(CHAINS, "QQQ", "o_s_error"),)
    assert _files(lake_root, CHAINS, "QQQ") == []
    [line] = _failure_lines(capsys.readouterr().err)
    assert "chains QQQ (ENOSPC)" in line
    assert line.endswith("the gap row did not land: OSError: [Errno 28] No space left on device")

    monkeypatch.undo()
    slot = _EXPECTED_SNAP.astimezone(MARKET_TZ)
    master = SecurityMaster()
    spans = CaptureSpans()
    for ticker in ("SPY", "QQQ"):
        iid = master.register(
            kind="equity", capture_start=slot, valid_from=slot.date(), ticker=ticker
        )
        spans.open_span(iid, slot, True)
    marker = gap.GapMarker(
        lake_root=lake_root,
        roster=_both_options,
        session_clock=SessionClock(
            clock=ManualClock(start=slot), calendar=weekday_sessions(slot.date())
        ),
        master=lambda: master,
        spans=lambda: spans,
        pid=4243,
    )

    report = marker.on_start(slot)

    assert report.problems == ()
    [path] = _files(lake_root, CHAINS, "QQQ")
    [row] = [
        row
        for row in journal.read_segment(path).to_pylist()
        if datetime.fromisoformat(row["snap_ts"]) == _EXPECTED_SNAP
    ]
    assert row["error_class"] == gap.DAEMON_DEAD


@_CAPS
def test_a_batch_whose_marker_failed_keeps_its_rows_and_gets_no_gap_row(
    lake_root, monkeypatch, capsys, cap
):
    """The rows were durable, so the minute is captured, and the create refuses a second row.

    The cycle still reports the write failure, because counting the segment as landed would
    push a manifest entry onto a disk that just refused eight bytes.
    """
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["marker"]})

    result = _cycle(lake_root, cap=cap)

    assert result.errors == (_SEGMENT_FULL,)
    [path] = _files(lake_root, QUOTES, "SPY")
    rows = journal.read_segment(path).to_pylist()
    assert [row["row_kind"] for row in rows] == [journal.ROW_KIND_DATA]
    assert path.relative_to(lake_root).as_posix() not in latest_entries(lake_root)
    [line] = _failure_lines(capsys.readouterr().err)
    assert "the gap row did not land: FileExistsError" in line


class _RefusingStderr:
    def write(self, text):
        raise OSError(errno.EPIPE, "Broken pipe")

    def flush(self):
        raise OSError(errno.EPIPE, "Broken pipe")


class _Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError("the message cannot be built")


def _explode(failure: str):
    def explode(*args, **kwargs):
        raise _Unprintable() if failure == "message" else RuntimeError("a bug")

    return explode


@pytest.mark.parametrize("failure", ["stderr", "message"])
@pytest.mark.parametrize(
    ("module", "name"),
    [(journal, "routed_columns"), (journal, "data_rows"), (capture, "sha256_file")],
    ids=["drift-scan", "row-count", "hash"],
)
def test_a_diagnostic_that_cannot_print_never_costs_the_segment(
    lake_root, monkeypatch, failure, module, name
):
    """Each of these prints a line and lets the segment land, and printing it cannot raise.

    A refusing stderr, or an exception whose message raises, used to turn a segment that
    landed, or a write that never ran, into a ``SegmentError``.
    """
    monkeypatch.setattr(module, name, _explode(failure))
    if failure == "stderr":
        monkeypatch.setattr(sys, "stderr", _RefusingStderr())

    result = _cycle(lake_root)

    assert result.errors == ()
    assert len(result.segments) == 4


@pytest.mark.parametrize("failure", ["stderr", "message"])
def test_a_snapshot_whose_drift_scan_cannot_print_still_lands(lake_root, monkeypatch, failure):
    monkeypatch.setattr(journal, "routed_columns", _explode(failure))
    if failure == "stderr":
        monkeypatch.setattr(sys, "stderr", _RefusingStderr())
    body = load_cassette(CASSETTES / "spy_minimal.json").find("chains", {"symbol": "SPY"}).body

    outcome = capture.journal_snapshot(
        lake_root,
        CHAINS,
        "SPY",
        body=body,
        cycle_start=_CLOCK_START,
        fetch_ts=_CLOCK_START,
        fetch_end_ts=_CLOCK_START,
        pid=4242,
    )

    assert outcome.routed_columns == ()
    assert outcome.partition in latest_entries(lake_root)


@pytest.mark.parametrize("unprintable", ["write-failure", "gap-row-failure"])
def test_a_failed_write_line_prints_when_a_message_cannot_be_built(
    lake_root, monkeypatch, capsys, unprintable
):
    """The line is the only record left when the gap row fails, so a message never drops it.

    An exception whose ``__str__`` raises prints its class name in place of its message.
    """
    prefix = f"capture: {_EXPECTED_SNAP.isoformat()}: segment write failed: quotes SPY"
    if unprintable == "write-failure":
        real_write = capture._CaptureCycle._write

        def write(self, surface, ticker, plan):
            if (surface, ticker) == (QUOTES, "SPY"):
                raise _Unprintable()
            return real_write(self, surface, ticker, plan)

        monkeypatch.setattr(capture._CaptureCycle, "_write", write)
        expected = f"{prefix}: _Unprintable; the gap row landed"
    else:
        _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"]})
        monkeypatch.setattr(capture._CaptureCycle, "_gap_plan", _explode("message"))
        expected = (
            f"{prefix} (ENOSPC): OSError: [Errno 28] No space left on device; "
            "the gap row did not land: _Unprintable"
        )

    _cycle(lake_root)

    assert _failure_lines(capsys.readouterr().err) == [expected]


class _Tracked(Exception):
    """An exception a test can hold a weak reference to, which ``Exception`` itself refuses."""


def _snapshot(lake_root: Path) -> None:
    body = load_cassette(CASSETTES / "spy_minimal.json").find("chains", {"symbol": "SPY"}).body
    capture.journal_snapshot(
        lake_root,
        CHAINS,
        "SPY",
        body=body,
        cycle_start=_CLOCK_START,
        fetch_ts=_CLOCK_START,
        fetch_end_ts=_CLOCK_START,
        pid=4242,
    )


@pytest.mark.parametrize(
    ("module", "name", "run"),
    [
        (journal, "routed_columns", _cycle),
        (journal, "data_rows", _cycle),
        (capture, "sha256_file", _cycle),
        (journal, "routed_columns", _snapshot),
    ],
    ids=["drift-scan", "row-count", "hash", "snapshot-drift-scan"],
)
def test_a_printed_diagnostic_does_not_keep_its_exception_in_a_cycle(
    lake_root, monkeypatch, module, name, run
):
    """A frame that kept the exception would hold the batch until the collector ran.

    The exception's traceback holds the frame that caught it, and that frame holds the
    batch. A local in that frame holding the exception closes a reference cycle, which only
    the cyclic collector frees, so a chain's megabytes would outlive the write.
    """
    raised: list[weakref.ref] = []

    def track(error: _Tracked) -> _Tracked:
        raised.append(weakref.ref(error))
        return error

    def explode(*args):
        raise track(_Tracked("a bug"))

    monkeypatch.setattr(module, name, explode)

    gc.disable()
    try:
        run(lake_root)
        assert raised
        assert [ref() for ref in raised] == [None] * len(raised)
    finally:
        gc.enable()


@pytest.mark.parametrize("name", ["append_requests", "failures", "append_cycle"])
def test_a_timing_failure_whose_message_raises_never_ends_the_cycle(lake_root, monkeypatch, name):
    """The timing writers never raise, because a raise out of the cycle ends the daemon."""
    monkeypatch.setattr(capture, name, _explode("message"))

    result = _cycle(lake_root)

    assert result.errors == ()
    assert len(result.segments) == 4


@pytest.mark.parametrize("cleanup", ["fails", "succeeds"])
def test_a_failed_write_does_not_hold_its_chain_to_the_end_of_the_cycle(
    lake_root, monkeypatch, cleanup
):
    """Above a cap of 1 a chain's batch is dropped once it lands, and a failure keeps it no longer.

    A close that raises inside ``__exit__`` chains the write's own failure behind its own,
    and the chained failure's traceback holds the frames that held the batch.
    """
    _refuse_writes(monkeypatch, {(CHAINS, "QQQ"): ["write"]})
    batches: list[weakref.ref] = []
    alive: list[bool] = []
    real_write = capture._CaptureCycle._write
    real_close = journal.SegmentWriter.close
    real_mark = capture._CaptureCycle._mark_failed_write

    def write(self, surface, ticker, plan):
        if (surface, ticker) == (CHAINS, "QQQ"):
            batches.append(weakref.ref(plan.batch))
        return real_write(self, surface, ticker, plan)

    def close(self):
        real_close(self)
        if cleanup == "fails" and self._write_failed:
            raise RuntimeError("the cleanup failed too")

    def mark(self, failure):
        gc.collect()
        alive.append(batches[0]() is not None)
        return real_mark(self, failure)

    monkeypatch.setattr(capture._CaptureCycle, "_write", write)
    monkeypatch.setattr(journal.SegmentWriter, "close", close)
    monkeypatch.setattr(capture._CaptureCycle, "_mark_failed_write", mark)

    result = _cycle(lake_root, cap=20)

    assert [(error.surface, error.ticker) for error in result.errors] == [(CHAINS, "QQQ")]
    assert alive == [False]


def _raised(error: BaseException) -> BaseException:
    """The error, raised and caught, so it carries a traceback of its own."""
    try:
        raise error
    except BaseException as caught:
        return caught


def test_clearing_a_failure_clears_every_exception_chained_to_it():
    """The cause, the context and an exception group's members each lose their traceback.

    Each one holds its own frames, so clearing only some of them keeps the rest alive.
    """
    cause = _raised(OSError(errno.ENOSPC, "no space"))
    context = _raised(ValueError("the cleanup failed"))
    member = _raised(PermissionError("refused"))
    group = _raised(ExceptionGroup("several", [member]))
    outer = _raised(RuntimeError("the write failed"))
    outer.__cause__ = cause
    outer.__context__ = context
    context.__context__ = group

    assert capture._without_tracebacks(outer) is outer

    for link in (outer, cause, context, group, member):
        assert link.__traceback__ is None, link


def _raise_from_write(monkeypatch, surface: str, ticker: str, error: BaseException) -> None:
    """The named segment's write raises ``error`` before it touches the disk.

    Nothing else is refused, so the gap row lands.
    """
    real_write = capture._CaptureCycle._write

    def write(self, s, t, plan):
        if (s, t) == (surface, ticker):
            raise error
        return real_write(self, s, t, plan)

    monkeypatch.setattr(capture._CaptureCycle, "_write", write)


@pytest.mark.parametrize(
    ("error", "tail"),
    [
        (
            OSError(errno.ENOSPC, "No space\nleft on device"),
            " (ENOSPC): OSError: [Errno 28] No space left on device; the gap row landed",
        ),
        (
            PermissionError(errno.EACCES, "Permission denied"),
            " (EACCES): PermissionError: [Errno 13] Permission denied; the gap row landed",
        ),
    ],
    ids=["newline", "oserror-subclass"],
)
def test_a_failed_write_line_is_one_line_naming_any_errno(
    lake_root, monkeypatch, capsys, error, tail
):
    """The runbook finds the line by its phrase, so a message never splits it.

    The errno's name is there whenever the failure carries one, a subclass of ``OSError``
    such as ``PermissionError`` included.
    """
    prefix = f"capture: {_EXPECTED_SNAP.isoformat()}: segment write failed: quotes SPY"
    _raise_from_write(monkeypatch, QUOTES, "SPY", error)

    _cycle(lake_root, cap=1)

    # A message that split the line would print its second half on a line of its own.
    lines = capsys.readouterr().err.splitlines()
    assert [line for line in lines if "quotes SPY" in line or "left on" in line] == [prefix + tail]


def test_a_gap_row_refusal_prints_on_the_same_line(lake_root, monkeypatch, capsys):
    """The reason the gap row did not land is part of the one line, however it reads."""
    _raise_from_write(monkeypatch, QUOTES, "SPY", OSError(errno.ENOSPC, "No space left on device"))

    def broken(self, *args):
        raise RuntimeError("two\nlines")

    monkeypatch.setattr(capture._CaptureCycle, "_gap_plan", broken)

    _cycle(lake_root, cap=1)

    err = capsys.readouterr().err
    [line] = _failure_lines(err)
    assert line.endswith("the gap row did not land: RuntimeError: two lines")
    assert "lines" not in err.replace(line, "")


class _SlowQuotes(CassetteVendor):
    """The cassette's replies, with the quote batch taking two seconds on the cycle's clock."""

    def __init__(self, cassette, clock) -> None:
        super().__init__(cassette)
        self._clock = clock

    def get_quotes(self, *args, **kwargs):
        reply = super().get_quotes(*args, **kwargs)
        self._clock.advance(2.0)
        return reply


def test_a_gap_row_keeps_its_fetch_stamps_in_order_and_the_cycles_phase(lake_root, monkeypatch):
    """The row carries the batch's own fetch stamps, start before end, as a landed row does.

    The quote batch takes time here, so a row whose stamps were swapped would end before it
    started. The row also carries the cycle's session phase, and its writer is closed.
    """
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"]})
    clock = ManualClock(start=_CLOCK_START)

    result = capture.run_cycle(
        clock,
        _SlowQuotes(load_cassette(CASSETTES / "spy_minimal.json"), clock),
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
        guards=GuardConstants(capture_max_concurrency=1),
        session_phase="post_equity_close",
    )

    [path] = _files(lake_root, QUOTES, "SPY")
    [row] = journal.read_segment(path).to_pylist()
    landed = _rows(result.segment(QUOTES, "QQQ"))[0]
    assert row["fetch_ts"] < row["fetch_end_ts"]
    assert (row["fetch_ts"], row["fetch_end_ts"]) == (landed["fetch_ts"], landed["fetch_end_ts"])
    assert row["session_phase"] == landed["session_phase"] == "post_equity_close"
    # The writer was closed, so the row's file ends in its end-of-stream marker.
    assert path.read_bytes().endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00")


def test_a_gap_row_refusal_does_not_keep_its_exception_in_a_cycle(lake_root, monkeypatch):
    """The refusal is kept only to print it, and never in a frame its traceback holds.

    The traceback holds the frame that caught the refusal. A local there holding the
    refusal closes a reference cycle, which only the cyclic collector frees, so the frames
    that built the gap row would outlive the attempt.
    """
    _refuse_writes(monkeypatch, {(QUOTES, "SPY"): ["write"]})
    raised: list[weakref.ref] = []

    def track(error: _Tracked) -> _Tracked:
        raised.append(weakref.ref(error))
        return error

    def broken(self, *args):
        raise track(_Tracked("a bug in the gap plan"))

    monkeypatch.setattr(capture._CaptureCycle, "_gap_plan", broken)

    gc.disable()
    try:
        _cycle(lake_root)
        assert len(raised) == 1
        assert raised[0]() is None
    finally:
        gc.enable()
