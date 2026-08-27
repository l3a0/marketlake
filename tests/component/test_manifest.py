"""The manifest ledger across one real boundary: the filesystem.

These write real ``O_APPEND`` lines, checksum real files, and run the two-way scrub
over a lake the fixture builder put on disk. The clock stays out of it. ``fetched_at``
is passed in.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake.manifest import (
    RowCountRegression,
    append_manifest,
    append_quarantine,
    guard_row_count,
    latest_entries,
    latest_quarantine,
    manifest_path,
    quarantine_path,
    read_manifest,
    read_quarantine,
    record_partition,
    scrub,
    sha256_file,
    would_shrink,
)
from tests.support.lake import FixtureLake, sample_chains_table

DAY = date(2026, 8, 24)
CHAINS_REL = "chains/ticker=SPY/date=2026-08-24.parquet"
QUOTES_REL = "quotes/ticker=SPY/date=2026-08-24.parquet"


def _base_lake(fixture_lake: FixtureLake) -> FixtureLake:
    fixture_lake.with_chains("SPY", DAY)
    fixture_lake.with_quotes("SPY", DAY)
    fixture_lake.build()
    return fixture_lake


# -- atomic single-line append -----------------------------------------------


def test_append_writes_exactly_one_line_per_entry(lake_root):
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    append_manifest(
        lake_root, partition="b", source="capture", sha256="s2", rows=2, fetched_at=None
    )
    lines = manifest_path(lake_root).read_text().splitlines()
    assert len(lines) == 2
    assert [json.loads(line)["partition"] for line in lines] == ["a", "b"]
    assert read_manifest(lake_root)[1]["sha256"] == "s2"


def test_read_discards_a_real_torn_trailing_line(lake_root):
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    append_manifest(
        lake_root, partition="b", source="capture", sha256="s2", rows=2, fetched_at=None
    )
    # Simulate a crash mid-append: a partial line with no terminating newline.
    fd = os.open(manifest_path(lake_root), os.O_WRONLY | os.O_APPEND)
    try:
        os.write(fd, b'{"partition": "c", "sha256": "unter')
    finally:
        os.close(fd)
    entries = read_manifest(lake_root)
    assert [e["partition"] for e in entries] == ["a", "b"]


def test_concurrent_appends_never_interleave(lake_root):
    # O_APPEND makes each line atomic, so many threads appending at once still yield
    # whole, parseable lines and the exact expected count.
    per_thread = 50
    threads_n = 8

    def worker(tag: int) -> None:
        for i in range(per_thread):
            append_manifest(
                lake_root,
                partition=f"p-{tag}-{i}",
                source="capture",
                sha256="s",
                rows=1,
                fetched_at=None,
            )

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    lines = manifest_path(lake_root).read_text().splitlines()
    assert len(lines) == threads_n * per_thread
    partitions = {json.loads(line)["partition"] for line in lines}
    assert len(partitions) == threads_n * per_thread


def test_last_entry_wins_over_a_real_manifest(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="old", rows=100, fetched_at=None
    )
    append_manifest(
        lake_root,
        partition="p",
        source="recompaction",
        sha256="new",
        rows=405,
        fetched_at="2026-08-24T16:30:00-04:00",
        guard=False,
    )
    latest = latest_entries(lake_root)
    assert latest["p"]["sha256"] == "new"
    assert latest["p"]["rows"] == 405
    assert latest["p"]["source"] == "recompaction"


# -- real sha256 over real files ---------------------------------------------


def test_sha256_file_matches_hashlib(lake_root):
    import hashlib

    target = lake_root / "blob.bin"
    target.write_bytes(b"marketlake")
    assert sha256_file(target) == hashlib.sha256(b"marketlake").hexdigest()


def test_record_partition_checksums_the_file_on_disk(fixture_lake):
    lake = fixture_lake
    lake.build()  # empty manifest
    # Write a partition the builder did not record, then record it from disk.
    path = lake.partition_path("chains", "SPY", DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(sample_chains_table(), path)
    entry = record_partition(lake.root, CHAINS_REL, source="compaction", rows=1, fetched_at=None)
    assert entry["sha256"] == sha256_file(path)
    assert scrub(lake.root).ok


# -- the standing row-count invariant ----------------------------------------


def test_guard_refuses_a_shrinking_row_count(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="s", rows=405, fetched_at=None
    )
    assert would_shrink(lake_root, "p", 400)
    assert not would_shrink(lake_root, "p", 410)
    with pytest.raises(RowCountRegression):
        guard_row_count(lake_root, "p", 400)
    # A larger or equal count is allowed.
    guard_row_count(lake_root, "p", 405)
    guard_row_count(lake_root, "p", 500)


def test_append_enforces_the_invariant_by_default(lake_root):
    append_manifest(
        lake_root, partition="p", source="capture", sha256="s", rows=405, fetched_at=None
    )
    with pytest.raises(RowCountRegression):
        append_manifest(
            lake_root, partition="p", source="capture", sha256="s2", rows=1, fetched_at=None
        )
    # A deliberate override under human authority may supersede without the guard.
    append_manifest(
        lake_root,
        partition="p",
        source="human",
        sha256="s3",
        rows=1,
        fetched_at=None,
        guard=False,
    )
    assert latest_entries(lake_root)["p"]["rows"] == 1


# -- the two-way scrub -------------------------------------------------------


def test_clean_lake_scrubs_ok(fixture_lake):
    lake = _base_lake(fixture_lake)
    assert scrub(lake.root).ok


def test_forward_pass_catches_a_sha_mismatch(fixture_lake):
    lake = _base_lake(fixture_lake)
    # Corrupt a sealed partition without touching its manifest entry.
    target = lake.partition_path("chains", "SPY", DAY)
    target.write_bytes(target.read_bytes() + b"corruption")
    result = scrub(lake.root)
    assert result.sha_mismatches == (CHAINS_REL,)
    assert not result.missing
    assert not result.orphans


def test_forward_pass_catches_a_missing_file(fixture_lake):
    lake = _base_lake(fixture_lake)
    lake.partition_path("quotes", "SPY", DAY).unlink()
    result = scrub(lake.root)
    assert result.missing == (QUOTES_REL,)


def test_reverse_pass_catches_an_orphan(fixture_lake):
    lake = _base_lake(fixture_lake)
    # A journal-less surface file with no manifest entry: exactly the invisible orphan
    # the reverse pass exists to catch.
    orphan = lake.root / "bars" / "ticker=SPY" / "freq=1m" / "date=2026-08-24.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"x": [1]}), orphan)
    result = scrub(lake.root)
    assert result.orphans == ("bars/ticker=SPY/freq=1m/date=2026-08-24.parquet",)
    assert not result.missing


def test_exclusion_set_is_honored(fixture_lake):
    lake = fixture_lake
    lake.with_chains("SPY", DAY)
    # A journal segment (manifest-less by rule) and the manifest itself must not read
    # as orphans.
    lake.with_journal_segment(
        "chains", "SPY", DAY, sample_chains_table(), start_ts="20260824T160000", pid=4242
    )
    lake.build()
    assert manifest_path(lake.root).exists()
    assert lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242).exists()
    assert scrub(lake.root).ok


def test_slice1_segment_entry_is_a_failure_until_the_partition_is_compacted(fixture_lake):
    lake = fixture_lake
    lake.build()  # empty manifest
    seg_rel = (
        lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
        .relative_to(lake.root)
        .as_posix()
    )
    # A slice-1 cron entry keyed by a segment path whose file is gone, and no compacted
    # partition yet. The forward pass flags it.
    append_manifest(
        lake.root,
        partition=seg_rel,
        source="capture",
        sha256="deadbeef",
        rows=100,
        fetched_at=None,
    )
    assert scrub(lake.root).missing == (seg_rel,)


def test_slice1_segment_entry_is_superseded_by_its_compacted_partition(fixture_lake):
    lake = fixture_lake
    lake.with_chains("SPY", DAY)  # the compacted partition and its manifest entry
    lake.build()
    seg_rel = (
        lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
        .relative_to(lake.root)
        .as_posix()
    )
    # The segment entry lingers after compaction deleted its file. Because the compacted
    # partition now has an entry, the segment entry is superseded, not a failure.
    append_manifest(
        lake.root,
        partition=seg_rel,
        source="capture",
        sha256="deadbeef",
        rows=100,
        fetched_at=None,
    )
    assert scrub(lake.root).ok


# -- the quarantine ledger ---------------------------------------------------


def test_quarantine_appends_and_last_verdict_wins(lake_root):
    append_quarantine(lake_root, {"partition": CHAINS_REL, "verdict": "suspect"})
    append_quarantine(lake_root, {"partition": CHAINS_REL, "verdict": "clean"})
    assert len(read_quarantine(lake_root)) == 2
    assert quarantine_path(lake_root).read_text().splitlines()[0]
    assert latest_quarantine(lake_root)[CHAINS_REL]["verdict"] == "clean"
