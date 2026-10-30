"""A bars partition written the way the sweep writes one, for tests that read bars back.

``write_bars`` builds its rows with ``journal.bars_rows`` from Schwab-shaped candles and writes
them with ``bars._write_partition``, the sweep's own writer, so the file holds exactly what a
real partition holds. A ``provenance`` goes through the same writer, which is the one route the
loader reads back. ``FixtureLake.with_bars`` takes a hand-built table instead, and it writes no
provenance at all.

The partition is written straight to disk and is not added to ``manifest.jsonl``, because
``load_bars`` lists sessions from the filesystem and reads no manifest. Call it after
``FixtureLake.build``, which rewrites the manifest from its own list and writes no partition.

``rewrite_metadata`` is the one way to put provenance on a partition that the writer refuses
to write. It keeps the rows and replaces only the schema metadata, which is how a damaged or
hand-written import reaches a reader.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from lake import bars, journal
from lake.loader import Provenance
from lake.paths import LakePaths
from lake.vendor import DAILY_FREQ


def _stamp(day: str, freq: str, index: int) -> datetime:
    """The candle's instant: 20:00 UTC for a daily bar, 14:30 UTC plus a minute each for 1m."""
    if freq == DAILY_FREQ:
        return datetime.fromisoformat(f"{day}T20:00:00+00:00")
    return datetime.fromisoformat(f"{day}T14:30:00+00:00") + timedelta(minutes=index)


def write_bars(
    root: Path,
    ticker: str,
    freq: str,
    day: str,
    closes: Sequence[float],
    *,
    instrument_id: int | None,
    provenance: Provenance | None = None,
) -> Path:
    """One bars partition holding a candle per close, written by the sweep's writer.

    Each candle's open, high and low sit one either side of its close, and its volume is 100.
    """
    candles = [
        {
            "datetime": int(_stamp(day, freq, index).timestamp() * 1000),
            "open": close - 1.0,
            "high": close + 1.0,
            "low": close - 2.0,
            "close": close,
            "volume": 100,
        }
        for index, close in enumerate(closes)
    ]
    rows = journal.bars_rows(
        {"candles": candles, "symbol": ticker, "empty": False},
        ticker=ticker,
        freq=freq,
        instrument_id=instrument_id,
        fetch_ts=f"{day}T22:00:00+00:00",
        fetch_end_ts=f"{day}T22:00:01+00:00",
        window_start=f"{day}T13:30:00+00:00",
        window_end=f"{day}T20:00:00+00:00",
        extended_hours=None,
    )
    path = LakePaths(root).bars_partition_path(ticker, freq, day)
    bars._write_partition(rows, path, provenance=provenance)
    return path


def rewrite_metadata(path: Path, metadata: Mapping[bytes, bytes] | None) -> None:
    """Replace the partition's schema metadata with ``metadata``, keeping every row."""
    table = pq.read_table(path)
    pq.write_table(table.replace_schema_metadata(metadata), path)
