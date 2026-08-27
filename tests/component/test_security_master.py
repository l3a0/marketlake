"""The security master across one real boundary: a parquet file on disk.

These tests write the master to a throwaway lake and read it back. They cross the
filesystem boundary, so they are component tests. The clock and the vendor stay out
of it entirely.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake.security_master import (
    ID_TYPE_TICKER,
    MASTER_SCHEMA,
    MASTER_SCHEMA_VERSION,
    SecurityMaster,
    UnsupportedSchemaVersion,
    master_path,
)

EPOCH = datetime(2019, 1, 2, 14, 30, tzinfo=UTC)


def _sample_master() -> SecurityMaster:
    master = SecurityMaster()
    iid = master.register(
        kind="equity",
        capture_start=EPOCH,
        valid_from=date(2012, 5, 18),
        ticker="FB",
        figi="BBG000MM2P62",
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))
    master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY")
    return master


def test_round_trip_preserves_every_mapping(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    assert path.exists()

    reloaded = SecurityMaster.read(path)
    assert set(reloaded.mappings) == set(master.mappings)


def test_write_lands_at_the_reference_convention_path(lake_root: Path):
    master = _sample_master()
    master.write(master_path(lake_root))
    assert (lake_root / "reference" / "security_master.parquet").exists()


def test_written_file_carries_the_pinned_schema_and_version(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))

    table = pq.read_table(path)
    assert table.schema == MASTER_SCHEMA
    versions = set(table.column("schema_version").to_pylist())
    assert versions == {MASTER_SCHEMA_VERSION}


def test_round_trip_preserves_as_of_resolution(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)

    assert reloaded.resolve("FB", on=date(2020, 1, 2)) == 1
    assert reloaded.resolve("META", on=date(2023, 1, 3)) == 1
    assert reloaded.resolve("FB", on=date(2022, 6, 9)) is None
    assert reloaded.symbol_at(1, on=date(2020, 1, 2)) == "FB"


def test_round_trip_preserves_capture_start_as_utc(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)
    assert reloaded.capture_start_of(1) == EPOCH


def test_empty_master_round_trips(lake_root: Path):
    path = SecurityMaster().write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)
    assert len(reloaded) == 0


def test_read_rejects_an_unsupported_schema_version(lake_root: Path):
    master = _sample_master()
    table = master.to_table()
    bumped = table.set_column(
        table.schema.get_field_index("schema_version"),
        "schema_version",
        [[MASTER_SCHEMA_VERSION + 1] * table.num_rows],
    )
    path = master_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(bumped, path)

    with pytest.raises(UnsupportedSchemaVersion):
        SecurityMaster.read(path)
