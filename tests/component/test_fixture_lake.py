"""The fixture-lake builder: it writes a consistent, readable lake on disk."""

from __future__ import annotations

import hashlib
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from tests.support.lake import (
    FixtureLake,
    read_manifest,
    sample_chains_table,
    sample_quotes_table,
)

DAY = date(2026, 8, 24)


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build(lake: FixtureLake) -> FixtureLake:
    master = pa.table({"instrument_id": [1], "ticker": ["SPY"]})
    lake.with_chains("SPY", DAY)
    lake.with_quotes("SPY", DAY)
    lake.with_reference("security_master", master)
    lake.with_journal_segment(
        "chains", "SPY", DAY, sample_chains_table(), start_ts="20260824T160000", pid=4242
    )
    lake.with_quarantine(
        {"partition": "chains/ticker=SPY/date=2026-08-24.parquet", "verdict": "clean"}
    )
    lake.build()
    return lake


def test_partitions_land_at_hive_paths(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    assert lake.partition_path("chains", "SPY", DAY).exists()
    assert lake.partition_path("quotes", "SPY", DAY).exists()
    assert (lake.root / "reference" / "security_master.parquet").exists()
    assert lake.manifest_path.exists()
    assert lake.quarantine_path.exists()


def test_manifest_checksums_match_the_files_on_disk(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    entries = read_manifest(lake.root)
    by_partition = {e["partition"]: e for e in entries}

    chains_rel = "chains/ticker=SPY/date=2026-08-24.parquet"
    assert chains_rel in by_partition
    entry = by_partition[chains_rel]
    on_disk = lake.root / chains_rel
    assert entry["sha256"] == _sha256(on_disk)
    assert entry["rows"] == sample_chains_table().num_rows


def test_manifest_covers_every_parquet_and_the_quarantine_ledger(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    partitions = {e["partition"] for e in read_manifest(lake.root)}
    assert "chains/ticker=SPY/date=2026-08-24.parquet" in partitions
    assert "quotes/ticker=SPY/date=2026-08-24.parquet" in partitions
    assert "reference/security_master.parquet" in partitions
    assert "quarantine.jsonl" in partitions


def test_reverse_scrub_every_manifest_file_exists(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    for entry in read_manifest(lake.root):
        assert (lake.root / entry["partition"]).exists(), entry["partition"]


def test_journal_segments_are_manifest_less(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    partitions = {e["partition"] for e in read_manifest(lake.root)}
    assert not any(p.startswith("journal/") for p in partitions)


def test_chains_partition_reads_back(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    table = pq.read_table(lake.partition_path("chains", "SPY", DAY))
    assert table.num_rows == 1
    assert table.column("ticker")[0].as_py() == "SPY"
    assert table.column("close_tag")[0].as_py() == "canonical"


def test_journal_segment_reads_back_as_arrow_ipc(fixture_lake: FixtureLake):
    lake = _build(fixture_lake)
    seg = lake.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
    with pa.OSFile(str(seg), "rb") as source:
        reader = pa.ipc.open_stream(source)
        table = reader.read_all()
    assert table.num_rows == 1
    assert table.column("occ_symbol")[0].as_py().startswith("SPY")


def test_sample_quotes_table_has_provenance_columns():
    table = sample_quotes_table()
    for column in ("snap_ts", "fetch_ts", "vendor_quote_ts", "row_kind", "schema_version"):
        assert column in table.column_names
