"""The capture cycle across one real boundary: the filesystem.

These run one whole cycle over real journal segments and a real manifest, with the
cassette-backed fake vendor and a manual clock. No network and no wall clock are
crossed, and every write lands on a throwaway lake. So the tier is component: one
subsystem, capture, over real files, with the vendor and clock still fake.

They pin the cycle's observable contract:

1. A happy cycle writes chains and quotes segments with the right rows and the right
   ``snap_ts`` / ``fetch_ts`` / ``vendor_quote_ts`` stamps.
2. A failing chain fetch gaps only that ticker while the others still capture.
3. A failing quote batch gaps every ticker's quotes, because the sampler is one shared
   failure unit.
4. The manifest gains one entry per segment, keyed by the segment path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lake import capture, journal
from lake.cassette import load_cassette
from lake.manifest import latest_entries, sha256_file
from lake.tickers import Roster
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

CASSETTES = Path(__file__).parents[1] / "cassettes"

# A clock whose seconds are non-zero, so flooring to the minute is observable. 09:30:45
# Eastern, expressed in UTC.
_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
_EXPECTED_SNAP = datetime(2026, 8, 24, 13, 30, 0, tzinfo=UTC)

# The synthetic vendor quote times the cassettes carry, as ISO strings.
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


# -- 1. the happy cycle ------------------------------------------------------


def test_happy_cycle_writes_chains_and_quotes_with_correct_stamps(cassette_vendor, lake_root):
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(clock, cassette_vendor, _both_options(), lake_root, pid=4242)

    assert result.errors == ()
    assert result.snap_ts == _EXPECTED_SNAP

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
    for row in spy_chain:
        assert row["snap_ts"] == _EXPECTED_SNAP.isoformat()
        assert row["fetch_ts"] == _CLOCK_START.isoformat()
        assert row["vendor_quote_ts"] == _CHAIN_VQT
        assert row["close_tag"] is None
        assert row["suspect"] is False

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
    assert spy_quote["vendor_quote_ts"] == _QUOTE_VQT
    # The envelope's classification fields do not pollute the normally-empty overflow.
    assert spy_quote["extra"] is None

    qqq_quote = _rows(result.segment(QUOTES, "QQQ"))[0]
    assert qqq_quote["bid"] == 601.48


# -- 4. the manifest entry per segment ---------------------------------------


def test_manifest_gains_one_entry_per_segment_keyed_by_the_segment_path(cassette_vendor, lake_root):
    clock = ManualClock(start=_CLOCK_START)
    result = capture.run_cycle(clock, cassette_vendor, _both_options(), lake_root, pid=4242)

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
    result = capture.run_cycle(clock, vendor, _both_options(), lake_root, pid=4242)

    assert result.errors == ()

    # SPY's chain still captured, one contract row.
    spy_chain = result.segment(CHAINS, "SPY")
    assert spy_chain.row_kind == journal.ROW_KIND_DATA
    assert spy_chain.rows == 1

    # QQQ's chain failed on a non-2xx status, so it is one gap row with the error class.
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
    result = capture.run_cycle(clock, vendor, _spy_options_qqq_equity(), lake_root, pid=4242)

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
