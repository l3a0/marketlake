"""The segment writer and reader, across one real boundary: files on disk.

These cross the filesystem, so they sit in the component tier. They pin exclusive
creation, the data-row and gap-row round trips, one schema per file by surface,
torn-tail tolerance, the shadow-append refusal, and per-cycle durability.
"""

from __future__ import annotations

import fcntl

import pyarrow as pa
import pytest

from lake import journal

DAY = "2026-08-24"
START = "20260824T160000"
PID = 4242

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
}

SNAP = "2026-08-24T16:15:00-04:00"
FETCH = "2026-08-24T16:15:00.400-04:00"
VENDOR = "2026-08-24T16:15:00-04:00"


def _chain_batch(**overrides):
    kwargs = dict(ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH, vendor_quote_ts=VENDOR)
    kwargs.update(overrides)
    return journal.chains_data_batch(CHAIN_BODY, **kwargs)


# -- round trips -------------------------------------------------------------


def test_data_row_round_trip(lake_root):
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID) as writer:
        writer.write_cycle(_chain_batch())
    path = journal.segment_path(lake_root, "chains", "SPY", DAY, START, PID)
    assert path.exists()
    table = journal.read_segment(path)
    assert table.num_rows == 1
    assert table.schema == journal.CHAINS_SCHEMA
    row = table.to_pylist()[0]
    assert row["occ_symbol"] == "SPY   260918C00650000"
    assert row["bid"] == 4.2
    assert row["interest_rate"] == 4.25
    assert row["is_delayed"] is False
    assert row["row_kind"] == journal.ROW_KIND_DATA


def test_many_cycles_accumulate_in_one_segment(lake_root):
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID) as writer:
        for _ in range(5):
            writer.write_cycle(_chain_batch())
    path = journal.segment_path(lake_root, "chains", "SPY", DAY, START, PID)
    assert journal.read_segment(path).num_rows == 5


def test_gap_row_round_trip(lake_root):
    batch = journal.gap_batch("quotes", ticker="SPY", snap_ts=SNAP, error_class="daemon_dead")
    with journal.SegmentWriter.open(lake_root, "quotes", "SPY", DAY, START, PID) as writer:
        writer.write_cycle(batch)
    path = journal.segment_path(lake_root, "quotes", "SPY", DAY, START, PID)
    row = journal.read_segment(path).to_pylist()[0]
    assert row["row_kind"] == journal.ROW_KIND_GAP
    assert row["error_class"] == "daemon_dead"
    assert row["bid"] is None
    assert row["realtime"] is None


# -- one schema per file -----------------------------------------------------


def test_a_quotes_batch_cannot_land_in_a_chains_segment(lake_root):
    quote_batch = journal.quotes_data_batch(
        {"bidPrice": 1.0, "askPrice": 1.1, "lastPrice": 1.05},
        ticker="SPY",
        snap_ts=SNAP,
        fetch_ts=FETCH,
        vendor_quote_ts=VENDOR,
    )
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID) as writer:
        with pytest.raises((pa.ArrowInvalid, ValueError, TypeError)):
            writer.write_cycle(quote_batch)


def test_open_picks_the_schema_from_the_surface(lake_root):
    writer = journal.SegmentWriter.open(lake_root, "quotes", "SPY", DAY, START, PID)
    try:
        assert writer.schema == journal.QUOTES_SCHEMA
    finally:
        writer.close()


# -- exclusive create --------------------------------------------------------


def test_exclusive_create_collision_errors_loudly(lake_root):
    first = journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID)
    try:
        with pytest.raises(FileExistsError):
            journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID)
    finally:
        first.close()


# -- torn tail ---------------------------------------------------------------


def _stream_bytes(nbatches):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, journal.CHAINS_SCHEMA) as writer:
        for _ in range(nbatches):
            writer.write_batch(_chain_batch())
    return sink.getvalue().to_pybytes()


def test_torn_tail_reads_to_the_last_complete_batch(tmp_path):
    # A power loss mid-append leaves a stream truncated inside its final batch. Build
    # a clean three-batch stream, then cut it partway into the third batch. The cut is
    # deterministic: the prefix through the second batch equals the closed two-batch
    # stream minus its eight-byte end-of-stream marker. Arrow's framing is identical
    # whatever the sink, so this reproduces a real torn segment byte for byte.
    three = _stream_bytes(3)
    through_batch_two = len(_stream_bytes(2)) - 8
    torn = tmp_path / "torn.arrows"
    torn.write_bytes(three[: through_batch_two + 8])  # plus a stub of the third batch

    table = journal.read_segment(torn)
    # The third batch is gone with the torn bytes. The first two survive, and the read
    # never raises.
    assert table.num_rows == 2


# -- shadow append -----------------------------------------------------------


def test_bytes_after_the_eos_marker_fail_loudly(lake_root, tmp_path):
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID) as writer:
        writer.write_cycle(_chain_batch())
    clean = journal.segment_path(lake_root, "chains", "SPY", DAY, START, PID)
    data = clean.read_bytes()

    shadow = tmp_path / "shadow.arrows"
    shadow.write_bytes(data + b"\x01\x02\x03\x04\x05\x06\x07\x08")
    with pytest.raises(journal.ShadowAppendError):
        journal.read_segment(shadow)


def test_a_second_stream_appended_after_eos_fails_loudly(lake_root, tmp_path):
    # A naive re-append lands a whole second stream past the first stream's EOS. A
    # standard reader would silently never see it. read_segment refuses it.
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID) as writer:
        writer.write_cycle(_chain_batch())
    clean = journal.segment_path(lake_root, "chains", "SPY", DAY, START, PID)
    data = clean.read_bytes()

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, journal.CHAINS_SCHEMA) as second:
        second.write_batch(_chain_batch())
    shadow = tmp_path / "shadow2.arrows"
    shadow.write_bytes(data + sink.getvalue().to_pybytes())
    with pytest.raises(journal.ShadowAppendError):
        journal.read_segment(shadow)


# -- durability --------------------------------------------------------------


def test_uses_f_fullfsync_on_this_platform():
    # The design's durability point is macOS F_FULLFSYNC, not plain fsync.
    assert journal.F_FULLFSYNC == fcntl.F_FULLFSYNC


def test_each_cycle_is_flushed_durable_and_the_close_flushes_the_eos(lake_root):
    writer = journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID)
    writer.write_cycle(_chain_batch())
    assert writer.durable_syncs == 1
    writer.write_cycle(_chain_batch())
    assert writer.durable_syncs == 2
    writer.close()
    # The end-of-stream marker is made durable too.
    assert writer.durable_syncs == 3


def test_a_cycle_is_on_disk_before_the_segment_is_closed(lake_root):
    # Durability means "on disk," so an independent reader sees a written cycle even
    # before the writer closes and lands the end-of-stream marker.
    writer = journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID)
    try:
        writer.write_cycle(_chain_batch())
        path = journal.segment_path(lake_root, "chains", "SPY", DAY, START, PID)
        # No end-of-stream marker yet, so this is the torn-tail read path. The durable
        # batch is still there.
        assert journal.read_segment(path).num_rows == 1
    finally:
        writer.close()


def test_writing_after_close_is_refused(lake_root):
    writer = journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, START, PID)
    writer.write_cycle(_chain_batch())
    writer.close()
    assert writer.closed
    with pytest.raises(ValueError):
        writer.write_cycle(_chain_batch())
