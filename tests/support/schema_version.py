"""The schema-version ledger a throwaway lake needs to stand in for a production one.

A lake root made with ``mkdir`` and nothing in it is not the machine any of this code runs
against. A production lake has ``reference/schema_versions.parquet`` recording the running
``journal.SCHEMA_VERSION``, because the read layer cannot interpret a row without it, and
marketlake #130 put a check for exactly that on the daemon's startup and the vendor sweep.
So a rig that drives either of those and skips this is asking them about a lake nobody runs.

Recorded by derivation rather than by literal. ``record_schema_version`` reads the shape off
``journal.schema_fingerprint`` for every pinned surface, so a rig built through here cannot
lag the pinned constant. Spelling a version number here instead is the drift marketlake #360
is cleaning up in ``tests/support/lake.py``, and this deliberately does not add to it.

It runs offline. The clock is injected, the lake-root ``flock`` is real and uncontended in a
temporary directory, and nothing reaches a vendor or a network.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lake.schema_versions import SchemaVersionReport, record_schema_version
from tests.support.clock import ManualClock

# When the fixture says the version was recorded. Any instant does, because nothing reads
# ``recorded_at`` to decide anything, so one constant keeps every rig's ledger identical.
RECORDED_AT = datetime(2026, 9, 13, 15, 0, tzinfo=UTC)  # 11:00 ET


def record_running_version(
    lake_root: Path | str, *, when: datetime = RECORDED_AT
) -> SchemaVersionReport:
    """Record the running ``journal.SCHEMA_VERSION``'s shape in ``lake_root``'s ledger."""
    return record_schema_version(clock=ManualClock(when), lake_root=lake_root)
