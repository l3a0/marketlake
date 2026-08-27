"""Integration test #1: the slice-1 cycle, end to end.

This is the named integration test from the build plan. It runs the whole capture
primitive against the fake vendor, then reads the journal segments and the manifest
back and asserts they agree. One cycle crosses four subsystems through real
boundaries: the vendor seam, the journal segment writer, the manifest ledger, and the
lake-root lock. That is more than one subsystem over one real boundary, so it belongs
in the integration tier, not component.

The claim it pins: what the cycle captured survives a round-trip off disk, and the
manifest is a faithful, scrub-clean record of it.
"""

from __future__ import annotations

from lake import capture, journal
from lake.manifest import latest_entries, read_manifest, scrub, sha256_file
from lake.tickers import Roster

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE


def _roster() -> Roster:
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": True, "chain_cadence": "1m"},
        }
    )


def test_slice1_cycle_end_to_end(cassette_vendor, manual_clock, lake_root):
    result = capture.run_cycle(manual_clock, cassette_vendor, _roster(), lake_root, pid=7094)

    # The cycle wrote four segments and journaled every one durably.
    assert result.errors == ()
    assert len(result.segments) == 4

    # Every segment reads back off disk, and its manifest entry agrees on both the
    # checksum and the row count. This is the round-trip the manifest exists to prove.
    manifest = latest_entries(lake_root)
    assert set(manifest) == set(result.partitions)
    for segment in result.segments:
        table = journal.read_segment(segment.path)
        entry = manifest[segment.partition]
        assert entry["rows"] == table.num_rows == segment.rows
        assert entry["sha256"] == sha256_file(segment.path)
        assert entry["source"] == capture.CAPTURE_SOURCE

    # The manifest holds exactly those four lines, one append per segment.
    assert len(read_manifest(lake_root)) == 4

    # Both directions of the integrity scrub pass: every manifest entry's segment exists
    # and matches, and no lake file is left unrecorded.
    assert scrub(lake_root).ok

    # The captured data itself survived, not just its bookkeeping. The SPY chain's call
    # contract and the SPY quote come back byte-for-byte as the vendor sent them.
    spy_chain = journal.read_segment(result.segment(CHAINS, "SPY").path).to_pylist()
    call = next(r for r in spy_chain if r["occ_symbol"] == "SPY   260918C00650000")
    assert (call["bid"], call["ask"], call["last"]) == (4.2, 4.25, 4.22)
    assert call["open_interest"] == 1234

    spy_quote = journal.read_segment(result.segment(QUOTES, "SPY").path).to_pylist()[0]
    assert (spy_quote["bid"], spy_quote["ask"], spy_quote["last"]) == (649.98, 650.02, 650.0)
    assert spy_quote["realtime"] is True
