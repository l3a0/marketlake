"""Integration test #9: onboard mid-session.

This is the named integration scenario D8 owns. It runs the whole onboarding command
against a real throwaway lake and a real tickers.yaml, wired through the security master,
the roster writer, and the manifest ledger. That is three subsystems talking through
real boundaries, so it belongs in the integration tier.

The claim it pins: onboarding a ticker in the middle of a session leaves the lake
consistent. The instrument registers with its capture_start stamped at the mid-session
instant, so coverage clamps to "onboarded now," never counts the morning as missing.
The master persists with a manifest entry, so the two-way integrity scrub stays clean.
And a second onboarding accumulates beside the first without disturbing it.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lake import journal
from lake.cassette import Cassette, Interaction
from lake.manifest import latest_entries, scrub
from lake.onboard import onboard
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path
from lake.tickers import load_tickers
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

# 11:00 ET on a session day, expressed in UTC. The design's "onboarded 11:00" moment.
_MID_SESSION = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)


def _chain_vendor(ticker: str) -> CassetteVendor:
    """A real-time chain vendor for one ticker with a single contract."""
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="chains",
                    params={"symbol": ticker},
                    status=200,
                    body={
                        "symbol": ticker,
                        "isDelayed": False,
                        "callExpDateMap": {
                            "2026-09-18:22": {
                                "650.0": [
                                    {"putCall": "CALL", "symbol": f"{ticker:<6}260918C00650000"}
                                ]
                            }
                        },
                        "putExpDateMap": {},
                    },
                ),
            )
        )
    )


def test_onboard_mid_session(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"
    clock = ManualClock(start=_MID_SESSION)

    spy = onboard(
        "SPY",
        clock=clock,
        vendor=_chain_vendor("SPY"),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    # capture_start is stamped at the mid-session instant, the clamp for coverage.
    assert spy.capture_start == _MID_SESSION
    assert spy.contract_count == 1

    # A second ticker onboards beside the first without disturbing it.
    qqq = onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor("QQQ"),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )
    assert qqq.instrument_id != spy.instrument_id

    # The master resolves both tickers as of the onboarding day.
    master = SecurityMaster.read(master_path(lake_root))
    onboard_day = _MID_SESSION.date()
    assert master.resolve("SPY", onboard_day, id_type=ID_TYPE_TICKER) == spy.instrument_id
    assert master.resolve("QQQ", onboard_day, id_type=ID_TYPE_TICKER) == qqq.instrument_id

    # The roster carries both, loadable by the same loader the daemon uses.
    roster = load_tickers(tickers_path)
    assert set(roster.symbols) == {"SPY", "QQQ"}

    # Each ticker's verification snapshot was journaled as its first captured cycle. Both
    # segments exist, round-trip through the reader, and carry a segment-keyed manifest
    # entry, exactly as a capture cycle would leave them.
    manifest = latest_entries(lake_root)
    for report in (spy, qqq):
        assert report.snapshot_surface == journal.CHAINS_SURFACE
        segment_path = lake_root / report.snapshot_segment
        assert segment_path.exists()
        rows = journal.read_segment(segment_path).to_pylist()
        assert len(rows) == 1
        assert rows[0]["ticker"] == report.ticker
        assert rows[0]["row_kind"] == journal.ROW_KIND_DATA
        assert report.snapshot_segment in manifest

    # The two-way integrity scrub is clean: the master's manifest entry and each snapshot
    # segment entry exist and match, and no lake file is left unrecorded. Journal
    # segments are manifest-tracked here yet excluded from the reverse pass by rule.
    assert scrub(lake_root).ok
