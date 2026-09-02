"""The last-durable-batch read, across one real boundary: files on disk.

``latest_expirations`` locates a ticker's most recent chains segment through the manifest
and returns the expirations in its last data batch. The capture chunker's failure path
reads it to name absence markers, and D10's startup gap-marking reuses the same read.
These cross the filesystem, so they sit in the component tier. They pin the empty-lake
case, the manifest lookup, the torn-tail read, and the walk back past a gap-only segment.
"""

from __future__ import annotations

import pyarrow as pa

from lake import journal
from lake.manifest import record_partition

DAY = "2026-08-24"
SNAP = "2026-08-24T16:15:00-04:00"
FETCH = "2026-08-24T16:15:00.400-04:00"

# One call expiring 2026-09-18. The expiration is the field this read returns, so it is set
# here explicitly.
CHAIN_BODY = {
    "interestRate": 4.25,
    "underlyingPrice": 650.01,
    "dividendYield": 1.28,
    "isDelayed": False,
    "callExpDateMap": {
        "2026-09-18:25": {
            "650.0": [
                {
                    "putCall": "CALL",
                    "symbol": "SPY   260918C00650000",
                    "expirationDate": "2026-09-18T20:00:00.000+00:00",
                    "bid": 4.2,
                    "ask": 4.25,
                }
            ]
        }
    },
}


def _chain_batch() -> pa.RecordBatch:
    return journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)


def _stream_bytes(nbatches: int) -> bytes:
    """A closed Arrow IPC stream of ``nbatches`` identical chains batches."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, journal.CHAINS_SCHEMA) as writer:
        for _ in range(nbatches):
            writer.write_batch(_chain_batch())
    return sink.getvalue().to_pybytes()


def _manifest(lake_root, path, rows: int) -> None:
    """Record a segment the way the capture cycle does: keyed by its lake-relative path."""
    rel = path.relative_to(lake_root).as_posix()
    record_partition(lake_root, rel, source="capture", rows=rows, fetched_at=FETCH)


def test_returns_none_on_an_empty_lake(lake_root):
    # No manifest, no segment, nothing durable to read.
    assert journal.latest_expirations(lake_root, "SPY") is None


def test_reads_the_last_data_batch_of_the_latest_manifested_segment(lake_root):
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w:
        w.write_cycle(_chain_batch())
    _manifest(lake_root, w.path, 1)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"]
    # The lookup is per ticker. A ticker with no segment reads as none.
    assert journal.latest_expirations(lake_root, "QQQ") is None


def test_a_torn_tail_reads_only_the_complete_batches(lake_root):
    # A power loss mid-append leaves the stream cut inside its third batch. The prefix through
    # the second batch equals the closed two-batch stream minus its eight-byte end-of-stream
    # marker, so the cut is deterministic. The read takes the two complete batches and never
    # raises on the torn bytes.
    three = _stream_bytes(3)
    through_batch_two = len(_stream_bytes(2)) - 8
    path = journal.segment_path(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(three[: through_batch_two + 8])  # plus a stub of the third batch
    _manifest(lake_root, path, 2)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"]


def test_walks_back_past_a_gap_only_latest_segment(lake_root):
    # The most recent segment is a whole-chain gap holding no data row. The read walks back
    # to the older data segment, so a gap last minute does not blind the marker. That is what
    # makes this the latest durable data batch rather than merely the latest segment.
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w1:
        w1.write_cycle(_chain_batch())
    _manifest(lake_root, w1.path, 1)
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160100", 4242) as w2:
        w2.write_cycle(
            journal.gap_batch("chains", ticker="SPY", snap_ts=SNAP, error_class="http_401")
        )
    _manifest(lake_root, w2.path, 1)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"]
