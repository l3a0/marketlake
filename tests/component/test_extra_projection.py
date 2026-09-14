"""A promotion, end to end, over real segments and the real ledger file on disk.

The unit tests hand-build tables and ledgers. These do not. A real chain payload carrying
a field the parser does not recognise is written through the real ``SegmentWriter``, the
real ``python -m lake.schema_versions`` recording tool writes the ledger into a real lake,
and the projection reads that file back through ``lake.schema_versions``. Files are
crossed, so these sit in the component tier. No vendor, no network, and no wall clock.

The promotion itself is simulated the only way it can be inside one test run. The version
below the boundary is written by the real code as it stands, where ``sigmaScore`` is an
unrecognised vendor field and lands in ``extra``. The version above it is the same code
with ``sigma_score`` promoted into a typed column, installed by patching the schema and the
parser's own contract map, which is exactly the two-line edit a real promotion is.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal
from lake.extra_projection import project_extra
from lake.schema_versions import SchemaVersionLedger, ledger_path, record_schema_version
from tests.support.clock import ManualClock

NOW = datetime(2026, 9, 13, 15, 0, tzinfo=UTC)
LATER = datetime(2026, 10, 1, 15, 0, tzinfo=UTC)

DAY = "2026-09-11"
SNAP = "2026-09-11T15:30:00-04:00"
FETCH = "2026-09-11T15:30:00.400-04:00"

# The promoted column and the vendor field it comes from. ``sigmaScore`` is not in any
# vendor map, so the code as it stands overflows it into ``extra``.
VENDOR_FIELD = "sigmaScore"
PROMOTED_COLUMN = "sigma_score"

CHAIN_BODY = {
    "interestRate": 4.25,
    "underlyingPrice": 650.01,
    "isDelayed": False,
    "callExpDateMap": {
        "2026-09-18:7": {
            "650.0": [
                {
                    "putCall": "CALL",
                    "symbol": "SPY   260918C00650000",
                    "bid": 4.2,
                    "ask": 4.25,
                    "openInterest": 1234,
                    VENDOR_FIELD: 0.42,
                }
            ]
        }
    },
}


def _promote(monkeypatch) -> None:
    """Install the shape a promotion of ``sigmaScore`` would leave behind.

    Four edits, which is what promoting a field costs in the real module.

    1. The column joins the schema.
    2. The vendor field joins the contract map, so the parser stops overflowing it.
    3. The known set widens with it.
    4. ``SCHEMA_VERSION`` goes to 2.

    Patching ``CHAINS_SCHEMA`` and the schema map is one edit rather than two, because a
    source promotion writes the column into the literal both names.

    ``extra_paths`` takes no edit at all, which is the point of deriving it per call. It
    reads the contract map, so edit 2 makes the column projectable in the same motion, and
    a fifth patch here would hide exactly that.
    """
    promoted = pa.schema([*journal.CHAINS_SCHEMA, pa.field(PROMOTED_COLUMN, pa.float64())])
    monkeypatch.setattr(journal, "CHAINS_SCHEMA", promoted)
    monkeypatch.setitem(journal._SCHEMAS, journal.CHAINS_SURFACE, promoted)
    monkeypatch.setitem(journal._CHAINS_CONTRACT_MAP, VENDOR_FIELD, PROMOTED_COLUMN)
    monkeypatch.setattr(
        journal, "_CHAINS_CONTRACT_KNOWN", journal._CHAINS_CONTRACT_KNOWN | {VENDOR_FIELD}
    )
    monkeypatch.setattr(journal, "SCHEMA_VERSION", 2)


def _write_segment(lake_root: Path, start: str, pid: int) -> Path:
    """One real chains segment from the payload above, through the real writer."""
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, start, pid
    ) as writer:
        writer.write_cycle(
            journal.chains_data_batch(CHAIN_BODY, ticker="SPY", snap_ts=SNAP, fetch_ts=FETCH)
        )
    return journal.segment_path(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, start, pid)


def _ledger_off_disk(lake_root: Path) -> SchemaVersionLedger:
    """The ledger as a reader gets it: off the file, through its own module."""
    return SchemaVersionLedger.read(ledger_path(lake_root))


def test_a_field_captured_below_the_promotion_reads_as_its_column_above_it(lake_root, monkeypatch):
    """The whole deliverable, driven end to end.

    Version 1 captures ``sigmaScore`` into ``extra``, because nothing knows the field. The
    ledger records version 1's shape. Version 2 promotes it. Reading the version-1 segment
    under version 2's code gives the column, filled off the overflow, with the raw value
    still in ``extra`` behind it.
    """
    segment = _write_segment(lake_root, "20260911T153000", 4242)
    record_schema_version(clock=ManualClock(NOW), lake_root=lake_root)

    written = journal.read_segment(segment)
    assert PROMOTED_COLUMN not in written.column_names
    assert json.loads(written.column("extra").to_pylist()[0]) == {VENDOR_FIELD: 0.42}
    assert written.column("schema_version").to_pylist() == [1]

    _promote(monkeypatch)
    result = project_extra(
        written, surface=journal.CHAINS_SURFACE, ledger=_ledger_off_disk(lake_root)
    )

    assert result.table.column(PROMOTED_COLUMN).to_pylist() == [0.42]
    assert result.table.column(PROMOTED_COLUMN).type == pa.float64()
    assert result.filled == {PROMOTED_COLUMN: 1}
    assert result.complete
    assert json.loads(result.table.column("extra").to_pylist()[0]) == {VENDOR_FIELD: 0.42}


def test_a_partition_spanning_the_promotion_reads_as_one_shape(lake_root, monkeypatch):
    """Both halves of a real merged partition, read through one column.

    The version-2 segment carries the value in its column and an empty overflow, because
    the parser now recognises the field. The version-1 segment carries it the other way
    round. Merged and projected, the column reads the same number on both rows.
    """
    below = journal.read_segment(_write_segment(lake_root, "20260911T153000", 4242))
    record_schema_version(clock=ManualClock(NOW), lake_root=lake_root)

    _promote(monkeypatch)
    above = journal.read_segment(_write_segment(lake_root, "20260911T160000", 4343))
    record_schema_version(clock=ManualClock(LATER), lake_root=lake_root)

    assert above.column(PROMOTED_COLUMN).to_pylist() == [0.42]
    assert above.column("extra").to_pylist() == [None]
    assert above.column("schema_version").to_pylist() == [2]

    merged = pa.concat_tables([below, above], promote_options="default")
    assert merged.column(PROMOTED_COLUMN).to_pylist() == [None, 0.42]

    result = project_extra(
        merged, surface=journal.CHAINS_SURFACE, ledger=_ledger_off_disk(lake_root)
    )

    assert result.table.column(PROMOTED_COLUMN).to_pylist() == [0.42, 0.42]
    assert result.filled == {PROMOTED_COLUMN: 1}
    assert result.complete


def test_the_ledger_the_projection_reads_is_the_file_the_tool_wrote(lake_root, monkeypatch):
    """The recorded shapes are what decide the projection, not the running code.

    Recording only version 2 and then reading version-1 rows leaves the projection with no
    shape for those rows, so it fills nothing and says which version it could not place.
    The rows are untouched and the value is still in the overflow, which is the condition
    marketlake #130 exists to prevent.
    """
    written = journal.read_segment(_write_segment(lake_root, "20260911T153000", 4242))

    _promote(monkeypatch)
    record_schema_version(clock=ManualClock(LATER), lake_root=lake_root)
    ledger = _ledger_off_disk(lake_root)
    assert ledger.versions() == (2,)

    result = project_extra(written, surface=journal.CHAINS_SURFACE, ledger=ledger)

    assert PROMOTED_COLUMN not in result.table.column_names
    assert result.unrecorded_versions == (1,)
    assert not result.complete
    assert json.loads(result.table.column("extra").to_pylist()[0]) == {VENDOR_FIELD: 0.42}


def test_no_ledger_file_at_all_refuses_rather_than_reading_as_empty(lake_root):
    """An absent ledger is not an empty one, and this module does not invent a reader.

    ``SchemaVersionLedger.read`` raises ``OSError`` on an absent file, which is the
    refusal a caller guards. Reading the file any other way would be the second read path
    the design keeps refusing.
    """
    with pytest.raises(OSError):
        _ledger_off_disk(lake_root)
