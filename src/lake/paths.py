"""The lake path module.

This is the single production home for lake path construction. Give it a
``lake_root`` and it builds every path the lake uses: the surface partitions, the
journal segments, the two append-only ledgers, and the reference tables. It reads no
config and no clock. The root is an argument, so a test points it at a throwaway
directory and production points it at the configured ``lake_root``.

A *surface* is one kind of measurement with its own pinned schema and partitioning.
That is the design's term for each top-level directory. The surfaces are ``chains``,
``quotes``, ``bars``, and ``actions``. ``reference/`` is the one non-surface. It holds
identity tables, not measurements.

The path conventions here match the fixture-lake builder used across the test suite
(``tests/support/lake.py``) exactly. That builder is the agreed contract, and other
deliverables read paths built here. Two surfaces do not share the flat
``ticker=.../date=....parquet`` shape. ``bars`` adds a ``freq=...`` level, because one
ticker has bars at several frequencies on the same day. ``actions`` is a single
all-ticker file. Each gets its own method, so a caller cannot build a wrong path by
passing its surface name to the generic partition method.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

# Surface names: the top-level measurement directories.
CHAINS = "chains"
QUOTES = "quotes"
BARS = "bars"
ACTIONS = "actions"

# Every surface, in the storage tree's order.
SURFACES = (CHAINS, QUOTES, BARS, ACTIONS)

# The surfaces whose partition is keyed by ticker and date alone. ``bars`` adds a freq
# level and ``actions`` is a single file, so both are excluded from the generic method.
_DATE_PARTITIONED = frozenset({CHAINS, QUOTES})

# The journal top-level directory, the reference directory, the reports directory, and
# the two ledgers. ``reports/`` holds one dated file per night, written by the vendor
# sweep. It sits inside the backup sync root, so a restore carries the reports with the
# data, and outside the manifest, because a report is not a measurement.
JOURNAL_DIR = "journal"
REFERENCE_DIR = "reference"
REPORTS_DIR = "reports"
MANIFEST_FILE = "manifest.jsonl"
QUARANTINE_FILE = "quarantine.jsonl"

# The key prefix on a partition-date directory or filename, as in ``date=2026-01-05``.
DATE_PREFIX = "date="

# The key prefix on a journal surface directory, as in ``surface=chains``.
SURFACE_PREFIX = "surface="

# The key prefix on a ticker directory, as in ``ticker=SPY``.
TICKER_PREFIX = "ticker="

# The first part of a journal segment's filename. A writer-session start stamp and the
# writer's pid follow it.
SEGMENT_PREFIX = "seg-"

# The last part of a journal segment's filename. The segment format is Arrow IPC.
SEGMENT_SUFFIX = ".arrows"

# The glob matching exactly the segments a writer created. It carries the prefix too, so it
# is narrower than the suffix alone and never matches a stray ``.arrows`` file.
SEGMENT_GLOB = f"{SEGMENT_PREFIX}*{SEGMENT_SUFFIX}"

# The reference tables named in the design.
SECURITY_MASTER = "security_master"
CONTRACTS = "contracts"

# The single all-ticker corporate-actions file under ``actions/``.
CORPORATE_ACTIONS_FILE = "corporate_actions.parquet"


def _day_str(day: date | str) -> str:
    """Render a partition date. A ``date`` becomes ISO. A string passes through.

    This matches the fixture-lake builder, so a production path and a fixture path
    agree character for character.
    """
    return day.isoformat() if isinstance(day, date) else str(day)


@dataclass(frozen=True)
class LakePaths:
    """Every lake path, built from one ``lake_root``."""

    root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))

    # -- surface partitions --------------------------------------------------

    def partition_path(self, surface: str, ticker: str, day: date | str) -> Path:
        """A date-partitioned surface's Parquet partition.

        Valid for ``chains`` and ``quotes``, whose partitions are keyed by ticker and
        date. ``bars`` and ``actions`` have their own shapes and their own methods, so
        passing either here raises rather than building a wrong path.
        """
        if surface not in _DATE_PARTITIONED:
            raise ValueError(
                f"partition_path is for {sorted(_DATE_PARTITIONED)}, not {surface!r}. "
                "Use bars_partition_path or actions_path."
            )
        return (
            self.root
            / surface
            / f"{TICKER_PREFIX}{ticker}"
            / f"{DATE_PREFIX}{_day_str(day)}.parquet"
        )

    def chains_partition_path(self, ticker: str, day: date | str) -> Path:
        """The chains partition for a ticker-day."""
        return self.partition_path(CHAINS, ticker, day)

    def quotes_partition_path(self, ticker: str, day: date | str) -> Path:
        """The quotes partition for a ticker-day."""
        return self.partition_path(QUOTES, ticker, day)

    def bars_partition_path(self, ticker: str, freq: str, day: date | str) -> Path:
        """The bars partition for a ticker, bar frequency, and day.

        Bars carry an extra ``freq=`` level, like ``freq=1m`` or ``freq=1d``. One
        ticker has bars at several frequencies on the same day.
        """
        return (
            self.root
            / BARS
            / f"{TICKER_PREFIX}{ticker}"
            / f"freq={freq}"
            / f"{DATE_PREFIX}{_day_str(day)}.parquet"
        )

    @property
    def actions_path(self) -> Path:
        """The corporate-actions table: one file for splits and dividends, all tickers."""
        return self.root / ACTIONS / CORPORATE_ACTIONS_FILE

    # -- journal -------------------------------------------------------------

    @property
    def journal_dir(self) -> Path:
        """The journal root. Compaction sweeps every date present under it."""
        return self.root / JOURNAL_DIR

    def segment_dir(self, surface: str, ticker: str, day: date | str) -> Path:
        """The directory holding one surface, ticker, and day's journal segments.

        Every writer session on that day lands its segment here. A reader lists this
        directory to find the day's segments.
        """
        return (
            self.journal_dir
            / f"{DATE_PREFIX}{_day_str(day)}"
            / f"{SURFACE_PREFIX}{surface}"
            / f"{TICKER_PREFIX}{ticker}"
        )

    def segment_path(
        self, surface: str, ticker: str, day: date | str, start_ts: str, pid: int
    ) -> Path:
        """One journal segment: per surface, ticker, day, and writer session.

        ``start_ts`` is the writer session's start stamp and ``pid`` its process id.
        Together they make the name unique, so a second writer never truncates a live
        segment. The segment is Arrow IPC, hence the ``.arrows`` suffix.
        """
        return (
            self.segment_dir(surface, ticker, day)
            / f"{SEGMENT_PREFIX}{start_ts}-{pid}{SEGMENT_SUFFIX}"
        )

    # -- ledgers -------------------------------------------------------------

    @property
    def manifest_path(self) -> Path:
        """The manifest ledger, the lake's integrity root."""
        return self.root / MANIFEST_FILE

    @property
    def quarantine_path(self) -> Path:
        """The quarantine ledger. It sits inside the backup root, beside the data."""
        return self.root / QUARANTINE_FILE

    # -- reference -----------------------------------------------------------

    def reference_path(self, name: str) -> Path:
        """A reference table by name, like ``security_master`` or ``contracts``."""
        return self.root / REFERENCE_DIR / f"{name}.parquet"

    @property
    def security_master_path(self) -> Path:
        """The security master: the internal ``instrument_id`` and its mappings."""
        return self.reference_path(SECURITY_MASTER)

    @property
    def contracts_path(self) -> Path:
        """The contracts reference: ``instrument_id`` to contract terms."""
        return self.reference_path(CONTRACTS)
