"""The last-durable-batch read, across one real boundary: files on disk.

``latest_expirations`` locates a ticker's most recent chains segment through the manifest
and returns the expirations in its last data batch. The capture chunker's failure path
reads it to name absence markers, and D10's startup gap-marking reuses the same read.
These cross the filesystem, so they sit in the component tier. They cover the empty-lake
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


def _chain_batch(expiration: str | None = None) -> pa.RecordBatch:
    """One chains batch. ``expiration`` rewrites the single contract's expiry."""
    body = CHAIN_BODY
    if expiration is not None:
        import copy as _copy

        body = _copy.deepcopy(CHAIN_BODY)
        leg = body["callExpDateMap"].pop("2026-09-18:25")
        leg["650.0"][0]["expirationDate"] = f"{expiration}T20:00:00.000+00:00"
        body["callExpDateMap"][f"{expiration}:25"] = leg
    return journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)


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


# -- the third widened reader ----------------------------------------------------------


def test_a_drifted_segment_is_walked_past_rather_than_raising(lake_root):
    """The reader on the live cycle's failure path, which had no test for drift.

    ``latest_expirations`` is called from the chain chunker's failure path inside a
    running cycle and from the close+5 guard. A segment whose schema drifted opens
    cleanly and raises on ``row_kind``, so before this it took whichever caller asked
    with it. The newest segment here is that file, and the read must walk past it to the
    older durable batch below rather than raise.
    """
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w1:
        w1.write_cycle(_chain_batch())
    _manifest(lake_root, w1.path, 1)

    drifted = w1.path.parent / "20260824T160100-4242.arrows"
    schema = pa.schema([("nothing_useful", pa.string())])
    with pa.ipc.new_stream(drifted, schema) as writer:
        writer.write_batch(pa.record_batch([pa.array(["x"])], schema=schema))
    _manifest(lake_root, drifted, 1)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"]


def test_a_segment_whose_bytes_will_not_open_is_walked_past_too(lake_root):
    """The same tolerance, for the kinds the drift case above does not reach.

    ``latest_expirations`` catches one tuple covering five causes, and only the missing
    column above exercised it. Narrowing the catch to ``KeyError`` alone left the whole
    suite green, so a corrupt segment propagating out of here and into the chain chunker's
    failure path, mid-cycle, would have gone unnoticed.

    This reader is deliberately left tolerant rather than made to carry a kind. It runs on
    the capture failure path, where raising costs a capture minute, and it has no reporting
    channel to carry a reason into: it skips and walks on. A test pins that choice so the
    next change to the shared tuple cannot quietly undo it.
    """
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w1:
        w1.write_cycle(_chain_batch())
    _manifest(lake_root, w1.path, 1)

    unopenable = w1.path.parent / "20260824T160100-4242.arrows"
    unopenable.write_bytes(b"not an arrow stream at all")
    _manifest(lake_root, unopenable, 1)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"]


def test_the_newest_batch_with_data_wins_inside_one_segment(lake_root):
    """The docstring's claim about batch order, which nothing held.

    One segment can carry many cycles, and the read returns the newest batch that holds
    data rows. Segment order is covered elsewhere; this is the order inside a segment,
    which the extracted helper now states and so has to keep.
    """
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w:
        w.write_cycle(_chain_batch())
        w.write_cycle(_chain_batch(expiration="2026-12-18"))
    _manifest(lake_root, w.path, 2)

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-12-18"], (
        "the read took an older batch, so the newest cycle's expirations were lost"
    )


def _retyped_chain_batch() -> pa.RecordBatch:
    """One chains batch whose every contract sent ``expirationDate`` at the wrong type.

    The column refuses an epoch integer where the schema holds a string, so the routing
    nulls it on every row and parks each raw value in ``extra``. The batch is data, and it
    names no expiration at all.
    """
    import copy as _copy

    body = _copy.deepcopy(CHAIN_BODY)
    body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]["expirationDate"] = 1787000000000
    return journal.chains_data_batch(body, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)


def test_a_data_batch_naming_no_expiration_does_not_blind_the_walk(lake_root):
    """A newer batch that names nothing must not answer for one that names something.

    A whole-chain ``expirationDate`` retype used to gap the ticker, so the segment held no
    data rows and the walk stepped past it to the last good batch. The routing lands that
    cycle as data instead, with the column null on every row, which reaches the same walk
    by a new route. Answering with an empty list there would read as this ticker having no
    expirations, and the chunker would fall back to a per-window marker while an older
    batch could still name the series.

    The prior batch is written first and the drifted one after, so taking the newest batch
    outright is what this catches.
    """
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w:
        w.write_cycle(_chain_batch())
    _manifest(lake_root, w.path, 1)
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160100", 4242) as w2:
        w2.write_cycle(_retyped_chain_batch())
    _manifest(lake_root, w2.path, 1)

    # The drifted cycle really did land as data, which is what makes this a new route.
    landed = journal.read_segment(w2.path).to_pylist()[0]
    assert landed["row_kind"] == journal.ROW_KIND_DATA
    assert landed["expiration_date"] is None

    assert journal.latest_expirations(lake_root, "SPY") == ["2026-09-18"], (
        "a batch that names no expiration answered for one that does, so the absence "
        "markers would lose the series they exist to name"
    )


def test_a_lake_whose_only_batch_names_no_expiration_answers_none(lake_root):
    """With nothing older to fall back to, the read says so rather than saying nothing."""
    with journal.SegmentWriter.open(lake_root, "chains", "SPY", DAY, "20260824T160000", 4242) as w:
        w.write_cycle(_retyped_chain_batch())
    _manifest(lake_root, w.path, 1)

    assert journal.latest_expirations(lake_root, "SPY") is None
