"""The path module.

This is the single production home for path construction. It covers two locations.

The lake is the first. Give it a ``lake_root`` and it builds every path the lake
uses: the surface partitions, the journal segments, the two append-only ledgers, and
the reference tables. The root is an argument, so a test points it at a throwaway
directory and production points it at the configured ``lake_root``.

The machine's config directory is the second. Four files sit in
``~/.config/marketlake/``: the machine-local ``config.yaml``, the rotating
``token.json``, the portable ``tickers.yaml``, and the nightly-written
``chain_plan.json``. That directory is the one path no config can name, because it is
where ``config.yaml`` is found. So it is a code constant, and this is where it is
spelled. Four modules and the control plane's renderer each spelled it separately
before.

The two halves sit together because they answer one question, where a file lives, and
because a second spelling of either is the failure both guard against. They differ in
one way worth naming. The lake root is configured and passed in. The config directory
is resolved from a home, the current user's unless a caller names another. So this
module reads the environment for that one location. It reads no config file and no
clock.

One spelling matters beyond tidiness. The Time Machine exclusion the control plane
renders covers the config directory, not the files in it. A module that spelled the
directory its own way would put its file outside that exclusion, and the credential or
the secret inside it would ride onto a backup disk with nothing to say so.

Every path here comes back resolved, never the unexpanded ``~`` form. A path that
looks usable but is not is a footgun, because ``open`` on ``~/x`` creates a literal
``~`` directory rather than failing. Resolving here means no caller has to remember.

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

This is also the single production home for reading those paths back apart. A journal
segment path and a ``date=YYYY-MM-DD`` directory name are both parsed by more than one
module. A second reader that re-implements the split can drift from the builder that
made the path, and nothing would catch it. So each parser sits beside the builder it
inverts. ``parse_segment_rel`` inverts ``segment_path``. ``parse_date_dir`` reads the
``date=`` key that every partition path carries.
"""

from __future__ import annotations

import re
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
# sweep. The daemon also writes here, one file per page that never reached the phone,
# under `reports/alerts/date=D/`. It sits inside the backup sync root, so a restore
# carries the reports with the data, and outside the manifest, because neither a
# report nor a record of an unsent page is a measurement.
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


# -- reading a path back apart -----------------------------------------------

# ``parse_segment_rel`` is the inverse of ``LakePaths.segment_path``. That method builds
# the five-part segment path. This function takes one apart. The pair lives in one module
# so the shape is spelled once. A parser that drifts from its builder is a silent bug,
# because the caller gets a plausible answer rather than an error.


@dataclass(frozen=True)
class SegmentRef:
    """One journal segment's path, parsed into its parts."""

    day: str  # the raw text after "date=", not yet a date object
    surface: str
    ticker: str
    filename: str


def parse_segment_rel(rel: str) -> SegmentRef | None:
    """Parse a lake-relative journal segment path, or return None.

    The shape is journal/date=D/surface=S/ticker=T/seg-<start>-<pid>.arrows.
    Anything else is None. The separator is always "/", because these strings are
    manifest keys and not host paths.
    """
    parts = rel.split("/")
    if len(parts) != 5 or parts[0] != JOURNAL_DIR:
        return None
    date_part, surface_part, ticker_part, filename = parts[1:]
    if not (
        date_part.startswith(DATE_PREFIX)
        and surface_part.startswith(SURFACE_PREFIX)
        and ticker_part.startswith(TICKER_PREFIX)
        and filename.endswith(SEGMENT_SUFFIX)
    ):
        return None
    return SegmentRef(
        day=date_part[len(DATE_PREFIX) :],
        surface=surface_part[len(SURFACE_PREFIX) :],
        ticker=ticker_part[len(TICKER_PREFIX) :],
        filename=filename,
    )


# A strict ``YYYY-MM-DD``: four digits, two, two, joined by hyphens and nothing else.
_DATE_SHAPE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_date_dir(name: str) -> date | None:
    """The date in a ``date=YYYY-MM-DD`` directory name, or None.

    Strict. ``date.fromisoformat`` alone accepts ``20260824`` and ``2026-W35-1``
    on Python 3.12, so the shape is checked before the parser is asked whether the
    digits make a real date.
    """
    if not name.startswith(DATE_PREFIX):
        return None
    text = name[len(DATE_PREFIX) :]
    if not _DATE_SHAPE.fullmatch(text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


# -- the machine's config directory ------------------------------------------

# The four files that sit in the config directory. The directory plus one of these
# names a full location.
CONFIG_FILE = "config.yaml"
TOKEN_FILE = "token.json"
TICKERS_FILE = "tickers.yaml"
CHAIN_PLAN_FILE = "chain_plan.json"


def config_dir(home: str | Path | None = None) -> Path:
    """The config directory, resolved under ``home`` or the current user's home.

    A running process omits ``home`` and gets its own. The control plane's renderer
    passes one, because it builds a plist for another account.
    """
    base = Path(home) if home is not None else Path.home()
    return base / ".config" / "marketlake"


__all__ = [
    "ACTIONS",
    "BARS",
    "CHAINS",
    "CHAIN_PLAN_FILE",
    "CONFIG_FILE",
    "CONTRACTS",
    "CORPORATE_ACTIONS_FILE",
    "DATE_PREFIX",
    "JOURNAL_DIR",
    "MANIFEST_FILE",
    "QUARANTINE_FILE",
    "QUOTES",
    "REFERENCE_DIR",
    "REPORTS_DIR",
    "SECURITY_MASTER",
    "SEGMENT_GLOB",
    "SEGMENT_PREFIX",
    "SEGMENT_SUFFIX",
    "SURFACES",
    "SURFACE_PREFIX",
    "TICKERS_FILE",
    "TICKER_PREFIX",
    "TOKEN_FILE",
    "LakePaths",
    "SegmentRef",
    "config_dir",
    "parse_date_dir",
    "parse_segment_rel",
]
