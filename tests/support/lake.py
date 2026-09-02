"""The fixture-lake builder.

``FixtureLake`` assembles a known lake on disk for a test to read. It writes
compacted Parquet partitions, journal segments in Arrow IPC, reference tables, and
the two append-only ledgers, with real sha256 checksums in the manifest. So a read
path can be exercised against a lake whose exact contents the test chose.

Two format notes match the design.

1. Compacted partitions and reference tables are Parquet, and each gets a manifest
   entry keyed by its path. The manifest records the partition path, its source, its
   sha256, its row count, and a fetch time. So both directions of the integrity scrub
   have something to check.
2. Journal segments are Arrow IPC. Arrow IPC appends self-contained record batches, so
   a segment torn mid-write stays readable up to its last complete batch. Segments are
   manifest-less by rule, so the builder writes them without a manifest entry.

The default schemas here are *fixture* schemas. They carry the provenance columns and
a few vendor columns, enough to stand in for real partitions. They are not the pinned
capture schema. That is pinned by the journal writer deliverable (D4).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# A fixture schema for chains rows. Provenance columns plus a few vendor columns.
FIXTURE_CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("occ_symbol", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("open_interest", pa.int64()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)

# A fixture schema for quotes rows.
FIXTURE_QUOTES_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)


def _table(schema: pa.Schema, rows: Sequence[dict] | None) -> pa.Table:
    rows = list(rows or [])
    columns = {name: [row.get(name) for row in rows] for name in schema.names}
    return pa.table(columns, schema=schema)


def sample_chains_table(rows: Sequence[dict] | None = None) -> pa.Table:
    """A small chains table in the fixture schema. Defaults to one data row."""
    if rows is None:
        rows = [
            {
                "snap_ts": "2026-08-24T16:15:00-04:00",
                "fetch_ts": "2026-08-24T16:15:00.400-04:00",
                "vendor_quote_ts": "2026-08-24T16:15:00-04:00",
                "ticker": "SPY",
                "occ_symbol": "SPY   260918C00650000",
                "bid": 4.20,
                "ask": 4.25,
                "last": 4.22,
                "open_interest": 1234,
                "row_kind": "data",
                "error_class": None,
                "suspect": False,
                "close_tag": "option_close",
                "session_phase": None,
                "schema_version": 1,
                "extra": None,
            }
        ]
    return _table(FIXTURE_CHAINS_SCHEMA, rows)


def sample_quotes_table(rows: Sequence[dict] | None = None) -> pa.Table:
    """A small quotes table in the fixture schema. Defaults to one data row."""
    if rows is None:
        rows = [
            {
                "snap_ts": "2026-08-24T16:15:00-04:00",
                "fetch_ts": "2026-08-24T16:15:00.300-04:00",
                "vendor_quote_ts": "2026-08-24T16:15:00-04:00",
                "ticker": "SPY",
                "bid": 649.98,
                "ask": 650.02,
                "last": 650.00,
                "row_kind": "data",
                "error_class": None,
                "suspect": False,
                "close_tag": "option_close",
                "session_phase": None,
                "schema_version": 1,
                "extra": None,
            }
        ]
    return _table(FIXTURE_QUOTES_SCHEMA, rows)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _day_str(day: date | str) -> str:
    return day.isoformat() if isinstance(day, date) else str(day)


@dataclass
class FixtureLake:
    """Builder for a known lake under ``root``. Chainable ``with_*`` methods."""

    root: Path
    _manifest: list[dict] = field(default_factory=list)
    _quarantine: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    # -- paths ---------------------------------------------------------------

    def partition_path(self, surface: str, ticker: str, day: date | str) -> Path:
        return self.root / surface / f"ticker={ticker}" / f"date={_day_str(day)}.parquet"

    def segment_path(
        self, surface: str, ticker: str, day: date | str, start_ts: str, pid: int
    ) -> Path:
        return (
            self.root
            / "journal"
            / f"date={_day_str(day)}"
            / f"surface={surface}"
            / f"ticker={ticker}"
            / f"seg-{start_ts}-{pid}.arrows"
        )

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.jsonl"

    @property
    def quarantine_path(self) -> Path:
        return self.root / "quarantine.jsonl"

    # -- parquet writers -----------------------------------------------------

    def _write_parquet(
        self, rel: Path, table: pa.Table, source: str, fetched_at: str | None
    ) -> FixtureLake:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        self._manifest.append(
            {
                "partition": rel.as_posix(),
                "source": source,
                "sha256": _sha256(path),
                "rows": table.num_rows,
                "fetched_at": fetched_at,
            }
        )
        return self

    def with_partition(
        self,
        surface: str,
        ticker: str,
        day: date | str,
        table: pa.Table,
        *,
        source: str = "capture",
        fetched_at: str | None = None,
    ) -> FixtureLake:
        rel = self.partition_path(surface, ticker, day).relative_to(self.root)
        return self._write_parquet(rel, table, source, fetched_at)

    def with_chains(
        self, ticker: str, day: date | str, table: pa.Table | None = None, **kwargs
    ) -> FixtureLake:
        return self.with_partition("chains", ticker, day, table or sample_chains_table(), **kwargs)

    def with_quotes(
        self, ticker: str, day: date | str, table: pa.Table | None = None, **kwargs
    ) -> FixtureLake:
        return self.with_partition("quotes", ticker, day, table or sample_quotes_table(), **kwargs)

    def with_reference(
        self, name: str, table: pa.Table, *, fetched_at: str | None = None
    ) -> FixtureLake:
        rel = Path("reference") / f"{name}.parquet"
        return self._write_parquet(rel, table, source="reference", fetched_at=fetched_at)

    # -- journal writer ------------------------------------------------------

    def with_journal_segment(
        self,
        surface: str,
        ticker: str,
        day: date | str,
        table: pa.Table,
        *,
        start_ts: str,
        pid: int,
    ) -> FixtureLake:
        path = self.segment_path(surface, ticker, day, start_ts, pid)
        path.parent.mkdir(parents=True, exist_ok=True)
        with pa.OSFile(str(path), "wb") as sink:
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
        return self

    # -- quarantine ----------------------------------------------------------

    def with_quarantine(self, entry: dict) -> FixtureLake:
        self._quarantine.append(entry)
        return self

    # -- finish --------------------------------------------------------------

    def build(self) -> Path:
        """Write the two ledgers and return the lake root."""
        self.root.mkdir(parents=True, exist_ok=True)
        if self._quarantine:
            self._write_jsonl(self.quarantine_path, self._quarantine)
            self._manifest.append(
                {
                    "partition": "quarantine.jsonl",
                    "source": "battery",
                    "sha256": _sha256(self.quarantine_path),
                    "rows": len(self._quarantine),
                    "fetched_at": None,
                }
            )
        self._write_jsonl(self.manifest_path, self._manifest)
        return self.root

    @staticmethod
    def _write_jsonl(path: Path, entries: Sequence[dict]) -> None:
        text = "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries)
        path.write_text(text)


def read_manifest(root: Path | str) -> list[dict]:
    """Read a lake's manifest as a list of entries, discarding a torn trailing line."""
    path = Path(root) / "manifest.jsonl"
    entries: list[dict] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            # A torn trailing line is discarded, the same rule as the journal tail.
            break
    return entries
