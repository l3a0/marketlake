"""The onboarding command across real files.

These run the onboarding core against a throwaway lake and a throwaway tickers.yaml,
with a cassette-backed vendor, a fake FIGI resolver, and a manual clock. No network, no
token, and no wall clock are crossed. The tier is component: onboarding writes and reads
real files, with the clock and vendor still fake.

They pin the slice-1 contract: register with a stamped capture_start, resolve the FIGI,
verify the real-time entitlement before trusting the ticker, write the roster entry, and
persist the master with a manifest entry so the scrub stays clean. A delayed feed is
refused before anything is written.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import journal
from lake.capture import CAPTURE_SOURCE
from lake.cassette import Cassette, Interaction
from lake.manifest import latest_entries
from lake.onboard import MASTER_PARTITION, EntitlementError, onboard
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path
from lake.tickers import load_tickers
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

# A mid-session instant: 2026-08-27 15:00 UTC is 11:00 ET, the design's "onboarded
# 11:00" case.
_MID_SESSION = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)
_SPY_FIGI = "BBG000BDTBL9"


class FakeFigiResolver:
    """A ``FigiResolver`` that returns a canned FIGI, or ``None`` to model no match."""

    def __init__(self, figi: str | None) -> None:
        self._figi = figi
        self.asked: list[str] = []

    def resolve(self, ticker: str) -> str | None:
        self.asked.append(ticker)
        return self._figi


def _chain_body(*, is_delayed: bool) -> dict:
    """A minimal SPY chain body with one call and one put."""
    return {
        "symbol": "SPY",
        "isDelayed": is_delayed,
        "callExpDateMap": {
            "2026-09-18:25": {
                "650.0": [{"putCall": "CALL", "symbol": "SPY   260918C00650000", "bid": 4.2}]
            }
        },
        "putExpDateMap": {
            "2026-09-18:25": {
                "650.0": [{"putCall": "PUT", "symbol": "SPY   260918P00650000", "bid": 3.8}]
            }
        },
    }


def _chain_vendor(*, is_delayed: bool) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="chains",
                    params={"symbol": "SPY"},
                    status=200,
                    body=_chain_body(is_delayed=is_delayed),
                ),
            )
        )
    )


def _quote_vendor(ticker: str, *, realtime: bool) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="quotes",
                    params={"symbols": [ticker]},
                    status=200,
                    body={ticker: {"realtime": realtime, "quote": {"bidPrice": 1.0}}},
                ),
            )
        )
    )


def test_onboard_registers_verifies_and_writes(lake_root, tmp_path):
    clock = ManualClock(start=_MID_SESSION)
    resolver = FakeFigiResolver(_SPY_FIGI)
    tickers_path = tmp_path / "tickers.yaml"

    report = onboard(
        "SPY",
        clock=clock,
        vendor=_chain_vendor(is_delayed=False),
        figi_resolver=resolver,
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    # The report pins the day-one anchor and the stamped epoch.
    assert report.instrument_id == 1
    assert report.figi == _SPY_FIGI
    assert report.capture_start == _MID_SESSION
    assert report.contract_count == 2
    assert report.realtime_verified is True
    assert report.already_registered is False
    assert resolver.asked == ["SPY"]

    # The master persisted and resolves SPY as of the onboarding day to the same id.
    master = SecurityMaster.read(master_path(lake_root))
    instrument_id = master.resolve("SPY", report.capture_start.date(), id_type=ID_TYPE_TICKER)
    assert instrument_id == 1
    assert master.capture_start_of(1) == _MID_SESSION

    # The master has a manifest entry, so the reverse scrub will not flag it as orphan.
    assert MASTER_PARTITION in latest_entries(lake_root)

    # The roster entry is written and loads back with the expected settings.
    roster = load_tickers(tickers_path)
    spy = roster.get("SPY")
    assert spy.options is True
    assert spy.chain_cadence == "1m"
    assert spy.bars == ("1m", "1d")

    # The verification snapshot was journaled as the first captured cycle, not discarded.
    assert report.snapshot_surface == journal.CHAINS_SURFACE
    segment_path = lake_root / report.snapshot_segment
    assert report.snapshot_segment.startswith("journal/")
    assert segment_path.exists()

    # It round-trips through the reader with one data row per contract, the same shape a
    # capture cycle writes.
    table = journal.read_segment(segment_path).to_pylist()
    assert [row["occ_symbol"] for row in table] == [
        "SPY   260918C00650000",
        "SPY   260918P00650000",
    ]
    assert all(row["row_kind"] == journal.ROW_KIND_DATA for row in table)
    assert all(row["fetch_ts"] is not None and row["fetch_end_ts"] is not None for row in table)

    # A matching segment-keyed manifest entry was appended, sourced like a capture write.
    entry = latest_entries(lake_root)[report.snapshot_segment]
    assert entry["source"] == CAPTURE_SOURCE
    assert entry["rows"] == len(table)


def test_onboard_is_idempotent(lake_root, tmp_path):
    resolver = FakeFigiResolver(_SPY_FIGI)
    tickers_path = tmp_path / "tickers.yaml"

    first = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        figi_resolver=resolver,
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    # A later re-run reuses the id and the original capture_start rather than minting new.
    later = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    second = onboard(
        "SPY",
        clock=ManualClock(start=later),
        vendor=_chain_vendor(is_delayed=False),
        figi_resolver=resolver,
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    assert second.already_registered is True
    assert second.instrument_id == first.instrument_id
    assert second.capture_start == first.capture_start == _MID_SESSION

    # The roster still holds exactly one SPY entry.
    roster = load_tickers(tickers_path)
    assert roster.symbols == ("SPY",)

    # Each run journaled its own snapshot at its own moment. The two segments differ and
    # both survive on disk, so a re-onboard never discards or clobbers a sample.
    assert second.snapshot_segment != first.snapshot_segment
    assert (lake_root / first.snapshot_segment).exists()
    assert (lake_root / second.snapshot_segment).exists()


def test_delayed_feed_is_refused_and_writes_nothing(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"

    with pytest.raises(EntitlementError):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_chain_vendor(is_delayed=True),
            figi_resolver=FakeFigiResolver(_SPY_FIGI),
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=True,
        )

    # Nothing was trusted: no roster entry, no persisted master, and no journal segment.
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_equity_only_onboard_uses_a_quote(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"

    report = onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("QQQ", realtime=True),
        figi_resolver=FakeFigiResolver(None),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=False,
    )

    # No chain snapshot, so no contract anchor. The FIGI was unresolved, which does not
    # block onboarding.
    assert report.contract_count is None
    assert report.figi is None
    assert report.realtime_verified is True

    roster = load_tickers(tickers_path)
    qqq = roster.get("QQQ")
    assert qqq.options is False
    assert qqq.chain_cadence is None

    # The quotes snapshot it fetched for entitlement was journaled as the first cycle.
    assert report.snapshot_surface == journal.QUOTES_SURFACE
    segment_path = lake_root / report.snapshot_segment
    assert segment_path.exists()
    row = journal.read_segment(segment_path).to_pylist()[0]
    assert row["ticker"] == "QQQ"
    assert row["realtime"] is True
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert report.snapshot_segment in latest_entries(lake_root)


def test_equity_only_delayed_quote_is_refused(lake_root, tmp_path):
    with pytest.raises(EntitlementError):
        onboard(
            "QQQ",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_quote_vendor("QQQ", realtime=False),
            figi_resolver=FakeFigiResolver(None),
            lake_root=lake_root,
            tickers_path=Path(tmp_path / "tickers.yaml"),
            options=False,
        )
