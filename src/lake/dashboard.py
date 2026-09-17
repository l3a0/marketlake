"""The read-only query service: the dashboard's Now, Today and History panels.

Failures push alerts. Progress needs a pull surface. This module is that surface. It is
a small read-only query service on localhost that answers a fixed set of named queries
over the lake. Two constant files ride along: ``status.html``, which renders the
panels, and ``favicon.ico``, which the browser puts on the tab. The page runs the
queries at view time. Nothing is pre-rendered and no summary state is kept. Freshness
reads off the data's own timestamps, so a dead capture shows as an old last cycle and a
dead service shows as a page that cannot load. Neither can be mistaken for the other.

**The fixed-query contract is a security invariant, not a convenience.** DuckDB SQL is
arbitrary local file read as the owning user, and the owning user can read the Schwab
token and the alerting secrets. So client-supplied SQL never crosses the boundary, and
the sandbox holds even if the query surface drifts. Five rules, each enforced in code
here.

1. The HTTP layer maps a request path to a query *name* in ``NAMED_QUERIES``. Outside
   that map, ``/`` serves the static page and ``/favicon.ico`` serves the browser-tab
   icon. Both answer with constant bytes shipped in the package. Neither reads lake data
   and neither runs a query. Every other unmapped path is a 404. No endpoint takes SQL,
   and no request field is ever treated as SQL text.
2. A request carries at most two parameters, a ticker and a date. Each is validated
   before any query runs. The ticker must be in the lake's own roster, which is the
   daemon's roster stamp and the tickers present under ``lake_root``, never
   ``tickers.yaml``. The date must parse as strict ``YYYY-MM-DD``. A request
   that fails validation is a 400. Validation runs before the connection is touched, with
   one exception: a date the calendar cannot judge at all is refused from inside the
   query, after a cursor has been opened. Even there no statement runs and no lake data
   is read, so the parameter still never reaches SQL.
3. A validated value reaches SQL only as a DuckDB bind parameter, never interpolated
   into the statement text. Every statement text is a module constant.
4. The connection is a sandbox. ``open_lake_connection`` sets ``allowed_directories`` to
   exactly ``lake_root``, turns ``enable_external_access`` off, then locks the
   configuration. No later SQL can widen the allow-list or flip the sandbox back on.
5. The service binds to the loopback address only. It refuses any request whose ``Host``
   header names neither ``localhost`` nor the loopback address, with a 403, whatever the
   verb and before any lake data is touched. That is the standard guard against DNS
   rebinding, the trick where a malicious page re-points its own domain at the loopback
   address to reach a local service through the owner's browser.

Read-only is by construction, with one caveat. The sandbox blocks every path outside
``lake_root`` but does let DuckDB write inside it. So read-only rests on the named
queries, which are ``SELECT`` statements only, and on the connection never being handed
to anything else. A test asserts the lake tree is byte-identical after every panel runs. The History
panel reads three things that are not surface rows, and none of them goes through SQL:
the quarantine ledger and the nightly report files are read off the filesystem, which is
``alert.undelivered``'s rule, and the window aggregate is the same ``SELECT`` the Today
strip runs, keyed by file.

Three terms recur, glossed at first use.

1. A *surface* is one kind of measurement with its own pinned schema. All three panels
   read the two minute-cadence surfaces, ``chains`` and ``quotes``.
2. A *slot* is one minute of the session, the ``snap_ts`` a capture cycle fires for. The
   Today strip has one cell per slot from the session open through the option close, so
   it is denominated by the calendar's session length. An early close renders as a short
   full day, never as a half-missing one.
3. A *sealed partition* is the one Parquet file compaction writes for a ticker-day.
   Before compaction the day lives in journal segments, Arrow IPC files with one record
   batch per cycle. A query reads both, unioned by column name, so the panel is the same
   before and after the seal.

The journal segments are read through ``lake.journal.read_segment``, the one reader that
knows the durability rules: a torn tail reads to the last complete batch, and bytes after
the end-of-stream marker are refused as a shadow-append. DuckDB has no native Arrow IPC
reader, so the segment rows are registered with the connection as an Arrow view and
unioned with the Parquet read inside SQL.

Nothing here reads the wall clock or names a session time. The service takes a ``Clock``
and a ``Calendar``. Each request stamps ``now`` from the clock and hands it into the
query, so minutes-since is computed against the injected instant, never ``now()`` in SQL.
The slots come from the calendar through ``SessionClock.bounds``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from fnmatch import fnmatchcase
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
import pyarrow as pa

from lake import journal
from lake.alert import undelivered
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar, NotASession
from lake.capture_spans import CaptureSpan, CaptureSpans, CaptureSpansError, spans_path
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, input_errors_exit, load_config
from lake.control_plane import assertion_window, sunday_canary_due
from lake.deadman import in_envelope
from lake.manifest import VERDICT_FIELD, is_quarantined, latest_quarantine
from lake.metadata import read_metadata
from lake.paths import (
    CHAINS,
    DATE_PREFIX,
    QUOTES,
    REPORTS_DIR,
    SEGMENT_GLOB,
    SURFACE_PREFIX,
    TICKER_PREFIX,
    LakePaths,
    parse_date_dir,
)
from lake.security_master import (
    ID_TYPE_TICKER,
    SecurityMaster,
    SecurityMasterError,
    master_path,
)
from lake.session import SessionClock, SessionPhase, session_slots

log = logging.getLogger(__name__)

# The loopback bind. It is a constant, not an option, so the service cannot be exposed
# by a flag. The port is the one configurable thing.
BIND_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

# The two ``Host`` names a request may carry, with an optional port. Anything else is a
# rebinding attempt or a misdirected request and gets a 403.
ALLOWED_HOSTS = frozenset({"localhost", BIND_HOST})

# The surfaces every panel reads: the minute-cadence capture surfaces.
PANEL_SURFACES = (CHAINS, QUOTES)

# The capture cadence. One slot per minute, matching the design's minutely loop.
SLOT = timedelta(minutes=1)

# The connection's resource caps. The service shares a laptop with the minutely capture
# daemon, and the capture loop's minute budget owns the machine. The dashboard is the
# guest, so it takes a small fixed share rather than the machine default, which is every
# core and most of RAM. Both must be set before ``lock_configuration``, because a locked
# configuration refuses every later ``SET``.
QUERY_THREADS = 2
QUERY_MEMORY_LIMIT = "2GB"

# The most sessions the Now walk looks back for a data cycle, per ticker and surface.
# The walk stops at the first day with one, so a healthy ticker costs one day's read. A
# ticker with no data cycle in ten sessions is catastrophically dead, and the exact age
# stops mattering long before that. Without the cap a gap-only ticker walks the entire
# retained history on every request, and the page refreshes every minute.
MAX_LOOKBACK_SESSIONS = 10

# The dead-man's last-owed walk. It steps back a day at a time looking for the newest
# expectation window that has already run. The longest reach is a Monday morning before
# the expectation arms, which walks Monday, Sunday, Saturday and lands on Friday. The
# starting day spends an iteration, so four is exactly enough rather than four with room
# over. The step back from a window's end lands on the last minute inside it, because the
# window excludes its own end instant.
OWED_LOOKBACK_DAYS = 4
OWED_WALK_STEP = timedelta(minutes=1)

# How far back the Now table's walk looks for the last session whose option close has
# passed. The worst case is a Monday morning after a Friday holiday: the walk spends an
# iteration on Monday, whose own close is still ahead, then Sunday, Saturday and the
# holiday, and lands on Thursday as the fifth. Ten leaves room for a run of closures no
# calendar has produced yet.
OWED_SESSION_LOOKBACK_DAYS = 10

# The six slot statuses the Today strip reports.
STATUS_CAPTURED = "captured"  # a data cycle landed
STATUS_SUSPECT = "suspect"  # a data cycle landed, flagged for the battery to judge
STATUS_GAP = "gap"  # a gap row records the missed minute and its reason
STATUS_MISSING = "missing"  # a past slot with no row at all, not even a gap marker
STATUS_PENDING = "pending"  # a slot whose cycle may still be running, so not yet judged
STATUS_OUT_OF_SCOPE = "out_of_scope"  # a slot before the ticker's capture_start epoch
STATUSES = (
    STATUS_CAPTURED,
    STATUS_SUSPECT,
    STATUS_GAP,
    STATUS_MISSING,
    STATUS_PENDING,
    STATUS_OUT_OF_SCOPE,
)

# How long after a slot before its absence means anything. The cycle that owes a slot
# starts at the top of that minute and has to fetch, journal and fsync before any row
# exists, so the slot instant is not the moment a row was owed. A slot is judged only
# once its own minute has ended and one further minute has passed on top. That second
# minute covers two things. It covers a cycle that overruns its minute, which the loop
# treats as an ordinary slow sample rather than a failure, and it covers the page's own
# 60-second refresh landing between the write and the read. Judging any sooner makes the
# verdict flap. A slot called missing while its cycle is still running flips to captured
# on the next refresh, and a cell that goes grey and then green reads worse than the
# premature verdict it replaced. This is the span and the reasoning the static page
# already uses for a stale stamp, applied to the other question of the same shape.
SLOT_VERDICT_GRACE = timedelta(minutes=2)

# The static page, shipped inside the package so it works offline.
STATUS_PAGE = "status.html"

# The browser-tab icon, shipped beside the page. ``lake.favicon`` renders it, and the
# path is the one a browser asks the origin for without being told to.
FAVICON = "favicon.ico"
FAVICON_PATH = "/favicon.ico"

# A ticker as the lake's directory names carry it. The roster is read off directory
# names under ``lake_root``, and a name outside this shape is not a ticker.
_TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9.]{0,11}")

# A strict ``YYYY-MM-DD``. ``date.fromisoformat`` accepts looser forms, so the shape is
# checked first and the parser only decides whether the digits make a real date.
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

# A date the calendar cannot judge. ``exchange_calendars`` covers a rolling window, a
# little over twenty years wide, and refuses a date outside it with a ``ValueError``
# subclass. A year pandas cannot hold in nanoseconds raises ``OverflowError`` instead.
# Both are caught by their standard-library base classes, so no calendar implementation
# is imported here and the calendar stays a seam.
_CALENDAR_RANGE_ERRORS = (ValueError, OverflowError)

# What a failed security-master read raises, named so each can be logged for what it is.
# The master is a Parquet file. An absent one raises ``OSError``, caught just above as
# ``FileNotFoundError``. A torn or corrupt one is folded by ``SecurityMaster.read`` into
# ``MasterUnreadable``, a ``SecurityMasterError``, so this guard no longer depends on a
# ``pyarrow`` type leaking out of the read. A file whose columns drifted raises
# ``KeyError``, and the master's own refusals raise ``SecurityMasterError``. ``ValueError``
# stays as a defensive classifier: ``ArrowInvalid`` is one, so a read that skipped the
# fold would still be logged here rather than escape. This set is not the only guard.
# ``_capture_spans`` catches everything, because a file with the pinned column names and
# drifted value types raises from a comparison much later, not from the read.
_MASTER_READ_ERRORS = (OSError, KeyError, ValueError, SecurityMasterError)

# The spans-file counterpart to ``_MASTER_READ_ERRORS``, same reasoning.
_SPANS_READ_ERRORS = (OSError, KeyError, ValueError, CaptureSpansError)


class QueryParameterError(ValueError):
    """A request parameter failed validation. Raised before any SQL runs.

    The message is a fixed phrase, never the offending value, so nothing a client sent
    is reflected back.
    """


# -- the sandboxed connection ------------------------------------------------


def open_lake_connection(lake_root: Path | str) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB sandbox over one lake root and return it.

    The connection is in-memory. Six ``SET`` statements make it a capped sandbox.

    1. ``temp_directory`` is cleared. DuckDB adds its own spill directory to the
       allow-list by default, so leaving it set puts a second entry on the list beside
       the lake root. Clearing it is load-bearing, not tidiness.
    2. ``allowed_directories`` is set to exactly ``[lake_root]``.
    3. ``enable_external_access`` is turned off. File reads, extension loads, and
       attaches are refused everywhere except under the allowed directory.
    4. ``threads`` is capped at ``QUERY_THREADS``.
    5. ``memory_limit`` is capped at ``QUERY_MEMORY_LIMIT``.
    6. ``lock_configuration`` is turned on. No later ``SET`` can undo any of the five
       settings above.

    DuckDB constrains the order in three places, not one. Several orders satisfy all
    three, and the one written below is only the clearest of them.

    1. ``temp_directory`` must be cleared before external access goes off. Once it is
       off, DuckDB refuses to modify the temp directory at all.
    2. ``allowed_directories`` must be set before external access goes off, for the same
       reason: DuckDB refuses to change the list once it is off.
    3. ``lock_configuration`` must come last, because a locked configuration refuses
       every later ``SET``. That is why the two caps come before it and not after.

    The root is resolved first so the Python-side listing and DuckDB's own path check
    agree. DuckDB canonicalizes every path it opens, so a symlink inside the lake that
    points outside is refused too. The lake root itself is a trusted config value and is
    bound as a parameter, never spliced into the statement.
    """
    root = Path(lake_root).resolve()
    con = duckdb.connect()
    con.execute("SET temp_directory = ''")
    con.execute("SET allowed_directories = [?]", [str(root)])
    con.execute("SET enable_external_access = false")
    con.execute("SET threads = ?", [QUERY_THREADS])
    con.execute("SET memory_limit = ?", [QUERY_MEMORY_LIMIT])
    con.execute("SET lock_configuration = true")
    return con


# -- the roster and the parameter validators ---------------------------------


def _children(directory: Path) -> list[Path]:
    """Every entry in one directory, or nothing when the listing fails.

    Compaction prunes an emptied ticker, surface, and date directory while holding the
    lake lock, so a directory the panel just saw can be gone by the time it is listed.
    A vanished directory reads as empty. So does a path that turned out not to be a
    directory. Neither is a server error.
    """
    try:
        return list(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return []


def _tickers_in(directory: Path) -> set[str]:
    """The ticker names under one ``ticker=...`` parent, filtered to the ticker shape."""
    found: set[str] = set()
    for child in _children(directory):
        if child.is_dir() and child.name.startswith(TICKER_PREFIX):
            name = child.name[len(TICKER_PREFIX) :]
            if _TICKER_PATTERN.fullmatch(name):
                found.add(name)
    return found


def _date_dirs(journal_dir: Path) -> list[Path]:
    """Every ``date=...`` directory under the journal root, oldest first."""
    return sorted(
        child
        for child in _children(journal_dir)
        if child.is_dir() and child.name.startswith(DATE_PREFIX)
    )


def lake_roster(paths: LakePaths) -> dict[str, tuple[str, ...]]:
    """The tickers the lake knows, each with the panel surfaces it is expected on.

    The design pins that the dashboard's ticker list comes from under ``lake_root`` and
    never from ``tickers.yaml``. Two sources under the root answer that, and the roster
    is their union.

    1. The daemon's roster stamp, in the journal metadata. It names every ticker the
       daemon is capturing and the surfaces each one is captured on. So a ticker that
       journaled nothing at all still gets a row, showing its capture failing rather
       than vanishing from the panel.
    2. The lake's own layout, the ``ticker=`` directories under each surface's partition
       tree and under each journal date. That covers a lake stamped by no daemon yet,
       and it keeps a ticker retired from the roster reachable for the days it did
       capture.

    A stamped name is filtered by the same ticker shape a directory name is, and to the
    two panel surfaces, because this mapping is the allow-list a request ticker is
    validated against. The result is sorted by ticker, and each ticker's surfaces are
    sorted too.

    The journal's date directories are listed once, not once per surface, because the
    listing is the same for every surface and this walk runs on every request.
    """
    surfaces: dict[str, set[str]] = {}
    for ticker, expected in read_metadata(paths.root).tickers.items():
        if not _TICKER_PATTERN.fullmatch(ticker):
            continue
        surfaces[ticker] = {name for name in expected if name in PANEL_SURFACES}
    date_dirs = _date_dirs(paths.journal_dir)
    for surface in PANEL_SURFACES:
        present = _tickers_in(paths.root / surface)
        for date_dir in date_dirs:
            present |= _tickers_in(date_dir / f"{SURFACE_PREFIX}{surface}")
        for ticker in present:
            surfaces.setdefault(ticker, set()).add(surface)
    return {ticker: tuple(sorted(surfaces[ticker])) for ticker in sorted(surfaces)}


def parse_date(text: str) -> date:
    """A strict ``YYYY-MM-DD`` as a ``date``. Anything else raises ``QueryParameterError``."""
    if not _DATE_PATTERN.fullmatch(text):
        raise QueryParameterError("malformed date, expected YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise QueryParameterError("malformed date, expected YYYY-MM-DD") from None


def validate_ticker(text: str, roster: Mapping[str, object]) -> str:
    """A ticker that is in the roster, unchanged. Anything else raises."""
    if text not in roster:
        raise QueryParameterError("unknown ticker")
    return text


# -- reading one ticker-day's rows -------------------------------------------

# The provenance columns the panels read, with their pinned types. Every surface schema
# carries them.
_PROVENANCE_TYPES: dict[str, pa.DataType] = {
    "snap_ts": pa.string(),
    "row_kind": pa.string(),
    "error_class": pa.string(),
    "suspect": pa.bool_(),
}
_PROVENANCE_SCHEMA = pa.schema(list(_PROVENANCE_TYPES.items()))

# The provenance columns a segment must carry to be read at all. ``snap_ts`` names the
# slot and ``row_kind`` says what the row records, so a segment missing either cannot be
# placed on the strip. Nulling them would bind the union and then read as a gap, turning
# schema drift into invented gaps. The design pins the opposite: a missing or retyped
# known field pages, and a gap is data, never inferred from absence. So a segment missing
# one is counted as drifted and skipped. The other two columns are optional, because
# ``error_class`` is null on data rows anyway and ``suspect`` defaults to false, so
# nulling a missing one invents nothing.
_REQUIRED_PROVENANCE = ("snap_ts", "row_kind")

# What a failed cast of a provenance column raises. A retyped column that cannot be cast
# back to its pinned type is drift, not a fatal panel.
_CAST_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)

# What a mid-request read of a sealed partition raises when the file is gone or will not
# read. DuckDB reports a path that no longer resolves to a file as an ``IOException``,
# and bytes that are not a readable Parquet as an ``InvalidInputException``. The pair is
# enumerated rather than a bare ``duckdb.Error`` so a real SQL defect still surfaces.
_PARTITION_READ_ERRORS = (duckdb.IOException, duckdb.InvalidInputException)

# The name the day's journal rows are registered under for the duration of one query.
_JOURNAL_VIEW = "journal_rows"

# The per-slot aggregate. ``snap_ts`` is an ISO string with an offset on every row the
# writers produce, so casting it to a zoned timestamp and taking epoch milliseconds gives
# one key per instant, whatever offset a row was written in. ``TRY_CAST`` turns an
# unparseable stamp into a null slot rather than a failed panel. The null key is kept,
# not filtered away: it groups every row whose stamp will not cast, and the caller counts
# that group instead of discarding it, so a garbled stamp is reported rather than silently
# rendering the minute missing. ``NULLS LAST`` pins where that group lands whatever the
# connection's null ordering is.
# ``row_kind`` values are bound, not spelled, so the journal module stays their one home.
# ``other_rows`` counts the rows that match neither bound kind, including a null one, so
# drift is counted rather than mistaken for a gap. Every reason the slot carried is
# reported, not one representative. Several reasons in one minute is normal, because a
# partial chain carries one class per failed date window and an absence marker is written
# per series. Counting rows to pick a representative let the per-series markers outvote
# the row recording why the cycle failed, so the slot named the benign reason for a
# failed close. Reporting the set discards nothing and needs no severity rank, which
# would be a guess the operator is better placed to make.
#
# The order is alphabetical, so a given set always renders the same way. It says nothing
# about severity: ``chain_chunk_failed`` sorts above ``vendor_auth_error`` and is the
# milder of the two. What bounds the list is how many of a slot's windows failed rather
# than any enumeration of the classes, because ``capture._error_class`` snake-cases the
# exception type name and each window given up carries the class its own failure saw.
# The close slot holds the most, since the close+5 fill's classes and
# ``option_close_series_absent`` land there on top of the cycle's own. The chunk plan's
# window count and the split depth are what keep that a handful of strings, so the list
# renders whole and nothing caps it.
#
# The ``FILTER`` keeps nulls out, which is what makes the list the distinct reasons the
# slot carried. A slot whose rows all carry a null class aggregates to a null list, and
# the dataclass reads that as no reason at all.
_SLOT_SELECT = """
SELECT slot_ms,
       count(*) FILTER (WHERE row_kind = $data_kind) AS data_rows,
       count(*) FILTER (WHERE row_kind = $gap_kind) AS gap_rows,
       count(*) FILTER (
           WHERE row_kind IS DISTINCT FROM $data_kind
             AND row_kind IS DISTINCT FROM $gap_kind
       ) AS other_rows,
       bool_or(coalesce(suspect, false)) AS suspect,
       list_sort(
           array_agg(DISTINCT error_class) FILTER (WHERE error_class IS NOT NULL)
       ) AS error_classes
FROM (
    SELECT epoch_ms(TRY_CAST(snap_ts AS TIMESTAMPTZ)) AS slot_ms,
           row_kind, error_class, suspect
    FROM ({source})
)
GROUP BY slot_ms
ORDER BY slot_ms NULLS LAST
"""

# The two sources: the day's journal rows alone, or those rows unioned by name with the
# day's sealed partition. ``union_by_name`` is what keeps a drifted schema readable.
_SOURCE_JOURNAL = f"SELECT * FROM {_JOURNAL_VIEW}"
_SOURCE_JOURNAL_AND_PARTITION = (
    f"SELECT * FROM {_JOURNAL_VIEW} "
    "UNION ALL BY NAME "
    "SELECT * FROM read_parquet($partitions, union_by_name = true)"
)
_SLOT_SQL_JOURNAL = _SLOT_SELECT.format(source=_SOURCE_JOURNAL)
_SLOT_SQL_JOURNAL_AND_PARTITION = _SLOT_SELECT.format(source=_SOURCE_JOURNAL_AND_PARTITION)


@dataclass(frozen=True)
class SegmentHealth:
    """How one ticker-day's stored rows read, counted by what went wrong.

    One integer cannot carry these. A file compaction sealed away mid-read is not
    corruption. A schema that drifted is not corruption either. And a shadow-append,
    bytes written past a segment's end-of-stream marker, is the loud failure the design
    pins, so it must not vanish into a generic count. Each counter is reported on its own
    and every one reaches the payload.

    Four counters count segments, one counts the day's sealed partition, and two count
    rows. The two row counters never count the same row twice: a row with an uncastable
    stamp is counted there and nowhere else, because it has no slot to be judged in.
    """

    corrupt: int = 0  # unreadable bytes: a torn header, or not an Arrow stream at all
    vanished: int = 0  # the file was listed and then gone, the seal landing mid-read
    shadow_append: int = 0  # bytes follow the end-of-stream marker
    drifted: int = 0  # a required provenance column is missing or cannot be cast back
    unreadable_partitions: int = 0  # the sealed file went away or would not read mid-read
    drifted_rows: int = 0  # rows whose ``row_kind`` is neither ``data`` nor ``gap``
    unparseable_stamp_rows: int = 0  # rows whose ``snap_ts`` will not cast to an instant

    def __add__(self, other: SegmentHealth) -> SegmentHealth:
        """Merge two counts, so a multi-day walk reports one total."""
        return SegmentHealth(
            corrupt=self.corrupt + other.corrupt,
            vanished=self.vanished + other.vanished,
            shadow_append=self.shadow_append + other.shadow_append,
            drifted=self.drifted + other.drifted,
            unreadable_partitions=self.unreadable_partitions + other.unreadable_partitions,
            drifted_rows=self.drifted_rows + other.drifted_rows,
            unparseable_stamp_rows=self.unparseable_stamp_rows + other.unparseable_stamp_rows,
        )

    def with_row_counts(self, *, drifted_rows: int, unparseable_stamp_rows: int) -> SegmentHealth:
        """The same counts with both row totals set. Both totals come from SQL."""
        return replace(
            self,
            drifted_rows=drifted_rows,
            unparseable_stamp_rows=unparseable_stamp_rows,
        )

    def payload(self) -> dict[str, int]:
        """The counters as the panels carry them, spliced flat into a row or a strip.

        ``unreadable_segments`` is the corrupt count. It keeps both the name and the
        top-level position the payload already used for exactly that meaning. That
        matters beyond taste. ``status.html`` reads ``row.unreadable_segments`` and
        ``strip.unreadable_segments`` directly. Nesting these counters under a parent
        key would leave those reads undefined, and an undefined count is never greater
        than zero, so the page would drop the warning silently. The six other counters
        join as siblings instead.
        """
        return {
            "unreadable_segments": self.corrupt,
            "vanished_segments": self.vanished,
            "shadow_append_segments": self.shadow_append,
            "drifted_segments": self.drifted,
            "unreadable_partitions": self.unreadable_partitions,
            "drifted_rows": self.drifted_rows,
            "unparseable_stamp_rows": self.unparseable_stamp_rows,
        }


@dataclass(frozen=True)
class SlotAggregate:
    """What one group of rows adds up to, for one ticker and surface on one day.

    ``slot_ms`` is null for the one group whose rows carry a ``snap_ts`` that will not
    cast to an instant. Those rows name no minute, so they can be counted but never
    placed on the strip. Every other group is one slot.
    """

    slot_ms: int | None
    data_rows: int
    gap_rows: int
    other_rows: int
    suspect: bool
    error_classes: tuple[str, ...]

    def __post_init__(self) -> None:
        """Freeze the aggregated classes, whatever shape the query handed back.

        DuckDB returns the aggregate as a list, and as null for a slot holding no class
        at all. Both become a tuple here, so every reader sees one shape and a frozen
        aggregate holds nothing mutable.
        """
        object.__setattr__(self, "error_classes", tuple(self.error_classes or ()))

    @property
    def row_count(self) -> int:
        """Every row in the group. The three kind counters partition it."""
        return self.data_rows + self.gap_rows + self.other_rows

    @property
    def has_bound_rows(self) -> bool:
        """Whether the slot carries a row of a kind the panels recognize.

        A slot holding only unrecognized kinds is drift. It is dropped before the strip
        is built, so it renders missing rather than an invented gap.
        """
        return self.data_rows > 0 or self.gap_rows > 0

    @property
    def status(self) -> str:
        """The slot's status from its own rows, decided by an explicit ladder.

        Data rows win first. A gap needs a gap row, never merely the absence of data.
        Those two are the whole ladder, because a slot with neither has no status to
        report: it holds only drifted rows, and the design pins that a gap is data and
        is never inferred from absence.

        So this asks for a slot with bound rows and says so by raising otherwise.
        ``_slot_aggregates`` is the only source of these, and it drops every aggregate
        without bound rows before returning, so the raise cannot fire today. It is
        written as a raise rather than a third status precisely because it is
        unreachable: a returned status there would be a plausible-looking answer that
        no test can catch, while a raise cannot be mistaken for one.
        """
        if self.data_rows > 0:
            return STATUS_SUSPECT if self.suspect else STATUS_CAPTURED
        if self.gap_rows > 0:
            return STATUS_GAP
        raise ValueError("status is defined only for a slot with data or gap rows")


def _provenance_columns(table: pa.Table) -> pa.Table | None:
    """One segment's provenance columns in the pinned schema, or ``None`` on drift.

    Every returned table carries exactly ``_PROVENANCE_SCHEMA``, so the concatenation
    that follows cannot fail on a type mismatch. A retyped column is cast back to its
    pinned type. A missing optional column is nulled. A missing required column, or a
    retyped one that will not cast, returns ``None`` and the caller counts the segment
    as drifted.
    """
    if any(name not in table.column_names for name in _REQUIRED_PROVENANCE):
        return None
    columns: list[pa.ChunkedArray | pa.Array] = []
    for name, kind in _PROVENANCE_TYPES.items():
        if name not in table.column_names:
            columns.append(pa.nulls(table.num_rows, kind))
            continue
        column = table.column(name)
        if column.type != kind:
            try:
                column = column.cast(kind)
            except _CAST_ERRORS:
                return None
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=_PROVENANCE_SCHEMA)


def _load_journal_rows(segments: Sequence[Path]) -> tuple[pa.Table, SegmentHealth]:
    """The provenance rows of every readable segment, plus how the segments read.

    A segment that cannot be read is counted and skipped, so one bad file never blanks
    the panel. Four failures are counted apart, because they mean different things and
    call for different responses.

    1. The file vanished under a landing seal.
    2. Its bytes are unreadable, from a torn header or a file that is no Arrow stream.
    3. It carries a shadow-append, bytes written past the end-of-stream marker.
    4. Its schema drifted past what a cast can repair.

    The counts are reported so every skip is visible, never silent.
    """
    tables: list[pa.Table] = []
    health = SegmentHealth()
    for path in segments:
        try:
            table = journal.read_segment(path)
        except FileNotFoundError:
            health += SegmentHealth(vanished=1)
            continue
        except journal.ShadowAppendError:
            health += SegmentHealth(shadow_append=1)
            continue
        except (OSError, pa.ArrowInvalid):
            health += SegmentHealth(corrupt=1)
            continue
        view = _provenance_columns(table)
        if view is None:
            health += SegmentHealth(drifted=1)
            continue
        tables.append(view)
    if not tables:
        return _PROVENANCE_SCHEMA.empty_table(), health
    try:
        return pa.concat_tables(tables), health
    except _CAST_ERRORS:
        # Unreachable while every table above carries the pinned schema. The fallback
        # keeps a future surprise to one skipped segment instead of a dead panel.
        merged = _PROVENANCE_SCHEMA.empty_table()
        for table in tables:
            try:
                merged = pa.concat_tables([merged, table])
            except _CAST_ERRORS:
                health += SegmentHealth(drifted=1)
        return merged, health


def _journal_segments(paths: LakePaths, surface: str, ticker: str, day: date) -> list[Path]:
    """One ticker-day's journal segments, exactly the set compaction seals.

    The name filter is ``SEGMENT_GLOB``, the same pattern compaction and the measure
    queries use. Matching every ``.arrows`` file instead would let a stray file that
    compaction never sweeps show on the panel forever.
    """
    directory = paths.segment_dir(surface, ticker, day)
    return sorted(
        path
        for path in _children(directory)
        if path.is_file() and fnmatchcase(path.name, SEGMENT_GLOB)
    )


def _slot_aggregates(
    con: duckdb.DuckDBPyConnection, paths: LakePaths, surface: str, ticker: str, day: date
) -> tuple[list[SlotAggregate], SegmentHealth]:
    """Every slot with recognized rows for one ticker, surface, and day, plus its health.

    The journal rows are registered as an Arrow view for the duration of the query. The
    sealed partition, when present, is read natively by DuckDB and unioned by name. The
    partition path is built from validated parts and bound as a parameter.

    Compaction seals a ticker-day under the lake lock while this reads without one, so
    the read is lock-free and the partition can change under it in both directions. Each
    direction is handled, because each renders a captured day wrong on its own.

    1. The partition appeared. It is checked again after the segments are read, so a seal
       that landed in between does not render a fully captured day as entirely missing.
       Counting a row twice for the few seconds both copies exist is the price, and it
       changes no slot's status.
    2. The partition went away, or its bytes stopped being readable Parquet, between the
       check and the read. That is a restore, a repair, or a torn write, and DuckDB
       raises out of the read. The rows fall back to the journal alone and the loss is
       counted in ``unreadable_partitions``, so the request degrades instead of dying.

    A slot whose rows are all of an unrecognized kind is dropped here rather than
    reported, so drift renders missing instead of an invented gap. The rows whose
    ``snap_ts`` will not cast are dropped too, because they name no minute. Both are
    counted in the returned health, so neither disappears quietly.
    """
    segments = _journal_segments(paths, surface, ticker, day)
    partition = paths.partition_path(surface, ticker, day)
    has_partition = partition.is_file()
    if not segments and not has_partition:
        return [], SegmentHealth()
    rows, health = _load_journal_rows(segments)
    if not has_partition:
        has_partition = partition.is_file()
    params: dict[str, object] = {
        "data_kind": journal.ROW_KIND_DATA,
        "gap_kind": journal.ROW_KIND_GAP,
    }
    con.register(_JOURNAL_VIEW, rows)
    try:
        if has_partition:
            params["partitions"] = [str(partition)]
            try:
                result = con.execute(_SLOT_SQL_JOURNAL_AND_PARTITION, params).fetchall()
            except _PARTITION_READ_ERRORS:
                health += SegmentHealth(unreadable_partitions=1)
                del params["partitions"]
                result = con.execute(_SLOT_SQL_JOURNAL, params).fetchall()
        else:
            result = con.execute(_SLOT_SQL_JOURNAL, params).fetchall()
    finally:
        con.unregister(_JOURNAL_VIEW)
    placed, unparseable_rows = _placed_aggregates(result)
    health = health.with_row_counts(
        drifted_rows=sum(agg.other_rows for agg in placed),
        unparseable_stamp_rows=unparseable_rows,
    )
    return [agg for agg in placed if agg.has_bound_rows], health


def _placed_aggregates(result: Sequence[tuple]) -> tuple[list[SlotAggregate], int]:
    """The groups that landed on a minute, plus how many rows carried no usable stamp.

    The aggregate SQL groups by the cast stamp and keeps the null key, so exactly one
    returned group can be the unplaceable one. Splitting it out here rather than in SQL
    is what turns a garbled stamp from a silently missing minute into a reported count.
    """
    placed: list[SlotAggregate] = []
    unparseable_rows = 0
    for row in result:
        aggregate = SlotAggregate(*row)
        if aggregate.slot_ms is None:
            unparseable_rows += aggregate.row_count
            continue
        placed.append(aggregate)
    return placed, unparseable_rows


def _dates_desc(paths: LakePaths, surface: str, ticker: str) -> list[date]:
    """Every day the lake holds rows for one ticker and surface, newest first.

    Both halves of the day come through ``paths.parse_date_dir``. A journal date
    directory carries the ``date=`` key as its own name. A sealed partition carries the
    same key as its filename stem, as in ``date=2026-08-24.parquet``. One parser reads
    both, and compaction's sweep reads its date directories through that same parser, so
    a directory this panel cannot name is a directory compaction will not seal.
    """
    days: set[date] = set()
    for date_dir in _date_dirs(paths.journal_dir):
        if (date_dir / f"{SURFACE_PREFIX}{surface}" / f"{TICKER_PREFIX}{ticker}").is_dir():
            parsed = parse_date_dir(date_dir.name)
            if parsed is not None:
                days.add(parsed)
    partition_dir = paths.root / surface / f"{TICKER_PREFIX}{ticker}"
    for child in _children(partition_dir):
        if child.is_file() and child.suffix == ".parquet":
            parsed = parse_date_dir(child.stem)
            if parsed is not None:
                days.add(parsed)
    return sorted(days, reverse=True)


# -- the capture_start clamp -------------------------------------------------


def _capture_spans(
    paths: LakePaths, tickers: Iterable[str], on: date
) -> dict[str, tuple[CaptureSpan, ...]]:
    """Each ticker's capture spans, read once from the master and the spans file.

    A *capture span* is a window ``[start, end)`` during which the ticker was captured.
    A minute outside every one of a ticker's spans is out of scope, whether it falls
    before the first span, after a closed one, or between two spans following a
    retirement and a rejoin. The design clamps every session-slot denominator, coverage
    check, and gap accounting the same way: out-of-scope minutes are never gaps.
    Onboarding day renders "onboarded 11:00," not 40 percent missing. A retirement day
    renders the same way from its own end, and the days away between a retirement and a
    rejoin render out of scope rather than as gaps.

    Both files are read once per query, not once per ticker. Either is optional here. An
    absent file, an unreadable one, an ambiguous symbol, or a ticker that does not
    resolve leaves that ticker out of the mapping, and a ticker outside the mapping gets
    no clamp at all. A missing reference table must never break a panel, so nothing here
    raises out of the query.
    """
    try:
        return _read_capture_spans(paths, tickers, on)
    except Exception:
        # The guard is broad here, and only here. The master and the spans file are
        # optional reference files read off disk, so their contents are data that may be
        # malformed in ways no enumerated error set anticipates: a file with the pinned
        # column names and drifted value types raises a ``TypeError`` or an
        # ``AttributeError`` from a comparison several frames deep, not a read error.
        # The promise above is absolute, and the cost of keeping it is one panel served
        # without a clamp rather than a panel not served at all. Do not narrow this back
        # to a list of error types. The traceback is logged, so a real defect is still
        # discoverable.
        log.exception("capture spans unusable, so no scope clamp is applied")
        return {}


def _read_capture_spans(
    paths: LakePaths, tickers: Iterable[str], on: date
) -> dict[str, tuple[CaptureSpan, ...]]:
    """Resolve each ticker against the master and its spans. The caller owns the failure path.

    Every span taken from the file is validated before it is kept. A span clamps by
    comparison against aware instants, so one whose ends are not both timezone-aware
    datetimes is dropped. A dropped span costs a ticker its clamp, the same answer an
    absent spans file gives, rather than a wrong one.
    """
    try:
        master = SecurityMaster.read(master_path(paths.root))
    except FileNotFoundError:
        # The master is optional and a fresh lake has none. That is not a problem to log.
        return {}
    except _MASTER_READ_ERRORS:
        log.warning("security master unreadable, so no scope clamp is applied")
        return {}
    try:
        spans = CaptureSpans.read(spans_path(paths.root))
    except FileNotFoundError:
        # The spans file is optional too, for the same reason the master is.
        return {}
    except _SPANS_READ_ERRORS:
        log.warning("capture spans unreadable, so no scope clamp is applied")
        return {}
    result: dict[str, tuple[CaptureSpan, ...]] = {}
    unusable = 0
    for ticker in tickers:
        try:
            instrument_id = master.resolve(ticker, on=on, id_type=ID_TYPE_TICKER)
        except SecurityMasterError:
            continue
        if instrument_id is None:
            continue
        ticker_spans = tuple(s for s in spans.spans_of(instrument_id) if _valid_span(s))
        dropped = len(spans.spans_of(instrument_id)) - len(ticker_spans)
        unusable += dropped
        if ticker_spans:
            result[ticker] = ticker_spans
    if unusable:
        # The count alone, never the ticker. A ticker can arrive as a request parameter,
        # and nothing a client sent is written to a log line.
        log.warning("capture spans: %d span(s) carry an unusable end", unusable)
    return result


def _valid_span(span: CaptureSpan) -> bool:
    """Whether a span's ends are fit to clamp with: aware datetimes throughout.

    A retyped file drifts the same way the master's ``capture_start`` column can, and a
    span unfit to compare is dropped rather than raising, which costs that span its
    clamp and nothing else.
    """
    if not isinstance(span.start, datetime) or span.start.utcoffset() is None:
        return False
    if span.end is None:
        return True
    return isinstance(span.end, datetime) and span.end.utcoffset() is not None


def _in_scope(instant: datetime, spans: tuple[CaptureSpan, ...]) -> bool:
    """Whether ``instant`` falls inside any of a ticker's capture spans."""
    return any(span.contains(instant) for span in spans)


# -- time helpers ------------------------------------------------------------


def _slot_ms(instant: datetime) -> int:
    """The epoch-millisecond key of an aware instant, the same key the SQL groups by."""
    return round(instant.timestamp() * 1000)


def _iso_et(slot_ms: int) -> str:
    """An epoch-millisecond key rendered as an Eastern-time ISO string.

    Converting a stored instant is arithmetic, not a clock read. The design speaks in
    Eastern time, so the panels do too.
    """
    return datetime.fromtimestamp(slot_ms / 1000, tz=UTC).astimezone(MARKET_TZ).isoformat()


def _iso(instant: datetime) -> str:
    return instant.astimezone(MARKET_TZ).isoformat()


def _minutes(span: timedelta) -> float:
    """A span in minutes, at the tenth every other age on the panel carries.

    A negative span is a deadline already passed, and it renders as one rather than
    being clamped to zero.
    """
    return round(span.total_seconds() / 60, 1)


def _ping_owed(now: datetime, grace_minutes: int) -> bool:
    """Whether the dead-man check is owed a ping by ``now``.

    The panel's dead-man line reads the ping's age against the grace healthchecks pages
    at. That reading only means something while a ping is owed. Outside the daemon's
    weekday envelope nothing pings at all, by design, so a threshold that ran around the
    clock would go loud every night and weekend. A line that is loud every night is a
    line the reader learns to skip, which is the failure the threshold is here to fix.

    The envelope's own opening carries the same trap one step smaller. The envelope
    starts at the firmware wake, and the daemon's first heartbeat lands a moment after
    it, so the instant before that the newest ping is the previous evening's. Asking for
    the envelope one grace back rather than at ``now`` spends those minutes as slack.
    The line arms at the wake plus the grace, which is the minute healthchecks itself
    starts expecting a ping.

    The envelope is ``lake.deadman``'s, not a second copy of it. The two going out of
    step is how a page ends up contradicting the alerting, which is what this line is
    for.
    """
    return in_envelope(now) and in_envelope(now - timedelta(minutes=grace_minutes))


def _last_owed(now: datetime, grace_minutes: int) -> datetime | None:
    """The latest instant at or before ``now`` when a dead-man ping was owed.

    ``_ping_owed`` answers whether a ping is owed this minute. The panel's line needs a
    second answer, because a ping that starved while one was owed stays starved after the
    window shuts. Judging only against ``now`` would drop the alarm at 18:45 and leave it
    down until 08:30, weekends included, which is most of the week. A ping URL that broke
    at 17:00 pages healthchecks by 17:06 and would read healthy on the page all evening.
    That is the reading this whole change exists to remove, moved to a later hour.

    So the walk finds the last minute a ping was owed and the line judges the ping there.
    An evening after a healthy day compares against that day's own last owed minute,
    which a live daemon fed, so the night stays quiet.

    ``assertion_window`` only proposes each day's end. ``_ping_owed`` decides whether that
    instant was owed, so the weekday rule and the Sunday exclusion stay in
    ``lake.deadman`` rather than being restated here.
    """
    if _ping_owed(now, grace_minutes):
        return now
    eastern = now.astimezone(MARKET_TZ)
    day = eastern.date()
    for _ in range(OWED_LOOKBACK_DAYS):
        window = assertion_window(day)
        if window is not None:
            last = min(window.end, eastern) - OWED_WALK_STEP
            if _ping_owed(last, grace_minutes):
                return last
        day -= timedelta(days=1)
    return None


def _session_has_a_judged_minute(ctx: QueryContext) -> bool:
    """Whether today's session has a minute whose cycle has had time to land.

    False through the session's opening grace, when no cycle of this session could have
    written a row yet. A day the calendar will not judge answers True, so a clock inside
    the capture window is never held back by a missing calendar entry.
    """
    try:
        bounds = ctx.session.bounds(ctx.session.session_date())
    except (NotASession, *_CALENDAR_RANGE_ERRORS):
        return True
    return ctx.now - SLOT_VERDICT_GRACE >= bounds.open


def _capture_owed_through(ctx: QueryContext) -> datetime | None:
    """The last minute a capture cycle was owed, or ``None`` if none ever was.

    The Now table paints a row stale when the ticker has gone too long without a durable
    data cycle. "Too long" only means something measured against a minute a cycle was
    owed. Inside the capture window that minute is now. Outside it the age keeps growing
    on a ticker that did everything right, because the session is over and nothing is
    owed, so measuring against now would paint every row stale every evening.

    The old reading avoided that by painting nothing at all outside the window. The cost
    was the hours an operator reviews the day in: a session that captured nothing read as
    plain text from the option close onward, with only the Today strip still red. So the
    reading moves to the right reference instead of switching off.

    That reference is the last option close that has passed, which is the last minute the
    loop owed a cycle. A healthy day captured through its own close and reads clean all
    evening. A day that captured nothing reads stale from the close onward, which is when
    the reader arrives.

    A session's own first minutes are the one place inside the window where now is the
    wrong reference. The open cycle starts at the top of the open minute and has to
    fetch, journal and fsync before a row exists, so at 09:30:00 the newest cycle is
    still the previous session's and the age measured against now is the weekend rather
    than anything this session did. Every ticker would read stale from the instant the
    session opened until the first row landed. So the previous close stays the reference
    until the grace has run out past the open, and only then does now take over. The
    grace is the one the Today strip judges a slot by, which is what makes the two panels
    agree at the open instead of contradicting each other. Nothing else moves. From the
    open plus the grace onward the reference is now again, so the threshold still
    measures the same span it always did and still fires when the watchdog does.
    """
    if ctx.session.in_capture_window() and _session_has_a_judged_minute(ctx):
        return ctx.now
    day = ctx.session.session_date()
    for _ in range(OWED_SESSION_LOOKBACK_DAYS):
        try:
            bounds = ctx.session.bounds(day)
        except (NotASession, *_CALENDAR_RANGE_ERRORS):
            bounds = None
        if bounds is not None and bounds.option_close <= ctx.now:
            return bounds.option_close
        day -= timedelta(days=1)
    return None


def _surface_stale(
    ctx: QueryContext,
    owed_through: datetime | None,
    last_data_ms: int | None,
    capture_start: datetime | None,
    in_scope: bool,
) -> bool:
    """Whether a surface has gone past the watchdog's threshold without a data cycle.

    Three states owe nothing and so are never stale.

    1. A lake with no closed session behind it at all has no minute to measure against.
    2. A ticker out of scope is either retired or not yet onboarded, and neither is owed
       a cycle now. The Today strip marks those same slots out of scope, so a verdict
       here that ignored scope would contradict the strip one panel over.
    3. A ticker whose ``capture_start`` epoch falls after the owed minute was never owed
       a cycle by it, which is the ticker onboarded after today's close. Scope does not
       cover that one, because the clock has passed the epoch and the ticker really is
       in scope now.
    """
    if owed_through is None or not in_scope:
        return False
    if capture_start is not None and capture_start > owed_through:
        return False
    if last_data_ms is None:
        return True
    last_data = datetime.fromtimestamp(last_data_ms / 1000, tz=UTC)
    return owed_through - last_data > timedelta(minutes=ctx.guards.watchdog_page_minutes)


def _dead_man_starved(now: datetime, last_ping: datetime | None, grace_minutes: int) -> bool:
    """Whether the dead-man check has gone unfed past the grace it pages after.

    A ping that has never landed is a failure only while one is owed. healthchecks holds
    a check that has never been pinged in a *new* state rather than a down one, so a lake
    nothing has ever run against owes nothing on a Saturday and says so.
    """
    if last_ping is None:
        return _ping_owed(now, grace_minutes)
    owed = _last_owed(now, grace_minutes)
    return owed is not None and owed - last_ping > timedelta(minutes=grace_minutes)


# -- the named queries -------------------------------------------------------


@dataclass(frozen=True)
class QueryContext:
    """What every named query gets besides its validated parameters.

    ``now`` is the injected clock's instant, stamped by the service per request.
    ``session`` is the session clock over that same clock and the injected calendar.
    ``roster`` is the lake's roster, walked once per request and shared with validation,
    because the walk lists the whole journal tree. ``guards`` are the machine's guard
    constants, so the page colours a row stale at the threshold the watchdog pages at.
    """

    paths: LakePaths
    now: datetime
    session: SessionClock
    roster: Mapping[str, tuple[str, ...]]
    guards: GuardConstants = field(default_factory=GuardConstants)


def query_now(con: duckdb.DuckDBPyConnection, ctx: QueryContext) -> dict[str, object]:
    """The Now panel: per ticker and surface, the last successful cycle and minutes-since.

    For each ticker and surface the lake holds, the query walks the ticker's days newest
    first and stops at the first day with a data cycle. So a ticker whose latest day is
    gap-only still reports its true last success, and the latest slot's own status and
    every reason it carried ride beside it. Minutes-since is ``now`` minus that slot,
    computed here from the injected instant. The walk is bounded at
    ``MAX_LOOKBACK_SESSIONS`` days, and a row that hit the bound says so rather than
    implying the ticker never captured.

    Each row also carries the ticker's ``capture_start`` epoch and whether ``now`` is at
    or after it. A ticker onboarded later today has no in-scope slot yet, so it is not a
    stale capture.

    Each row also carries ``stale``, the verdict the page colours by, per
    ``_surface_stale``. The age and the verdict read against different instants on
    purpose. The age is against ``now``, which is the number a reader wants. The verdict
    is against ``capture_owed_through``, the last minute the loop owed a cycle, which is
    the only minute the age means anything against once the session has ended.

    Seven entries below describe the daemon and the clock rather than a ticker, and they
    carry eleven fields between them. The rest of the payload names the request itself:
    ``as_of``, ``session_date``, ``is_session``, ``phase``, ``stale_after_minutes`` and
    ``tickers``. Seven of the eleven read what another component wrote under
    ``lake_root``, and the dashboard never reads ``~/.config``. The other four, the
    grace, the expectation, the starvation verdict and the owed-through instant, come
    from the guard constants, the clock, the calendar and the ping together.

    1. ``token_minted_at``, the refresh token's mint stamp, from the journal metadata
       the daemon stamps every cycle and every idle minute.
    2. ``token_age_minutes``, ``now`` minus that same stamp.
    3. ``token_sunday_countdown_minutes``, the wait until the Sunday canary that must
       replace the token in use. ``sunday_canary_due`` owns that moment, because the
       control plane owns the ritual. It goes negative once the ritual is overdue, which
       is the honest reading of a token past its Sunday.
    4. ``dead_man_last_ping``, the instant the dead-man ping last landed, written by the
       daemon's own feed, with ``dead_man_age_minutes`` beside it. Three more fields let
       the panel judge that age rather than leaving the reader to subtract it by eye.
       ``dead_man_starved`` is the verdict, per ``_dead_man_starved``, and it is what the
       page alarms on. ``dead_man_expected`` says whether a ping is owed this minute, per
       ``_ping_owed``, and the page says so in words rather than colouring by it.
       ``dead_man_grace_minutes`` is the grace healthchecks pages after, so the page
       reads the ping against the threshold the alerting uses. The two are close rather
       than identical, because healthchecks measures from the next scheduled ping and
       this measures from the last one that landed, so the page goes loud about a minute
       early.

       Inside the capture window only a durable data cycle feeds the check, because the
       idle heartbeat stands down there by design. So a session whose every cycle fails
       starves this ping while the stamp below keeps landing, and the two together are
       what separate a running loop from working capture. Past the option close the
       heartbeat resumes and feeds the check on a day that captured nothing, so the line
       says which of the two is feeding it rather than letting a fresh ping read as
       capture.
    5. ``pages_failed_to_send``, today's count of pages that never reached the phone,
       counted from the files the publisher writes under ``reports/``. The day is the
       Eastern one, the same key the publisher files them under.
    6. ``capture_owed_through``, the last minute a capture cycle was owed, per
       ``_capture_owed_through``. Inside the capture window that is ``now``. Outside it
       that is the last option close that has passed, so a healthy evening reads clean
       while the ages beside it climb. It is null only on a lake with no closed session
       behind it at all.
    7. ``stamp_age_minutes``, ``now`` minus the stamp's own instant. The token fields are
       only as fresh as the write that produced them, and the daemon stamps every minute
       it is awake. So a stamp older than a few minutes means the writer stopped, and the
       mint, the age and the countdown are the last thing a dead daemon said rather than
       a reading of now. Without this the panel cannot tell those apart.

       The dead-man fields are the exception, and the stamp must not disclaim them. A
       stopped writer stops the ping too, so the starvation verdict reads a dead daemon
       correctly rather than going stale with it. That verdict is the one an operator
       needs at the moment the stamp itself has gone old.

    Each instant and each age is null when nothing has been written. A daemon that has
    never run leaves the token and ping stamps absent, and the panel says so rather than
    showing a zero. The page count is never null, because an ordinary day writes no file
    at all, so its absence is a true zero and reads as one. The grace, the expectation
    and the starvation verdict are never null either, because each holds whether or not
    anything has ever been stamped. The owed-through instant is null only where no
    session has closed yet, which is a lake younger than its first close.
    """
    spans_by_ticker = _capture_spans(ctx.paths, ctx.roster, ctx.session.session_date())
    owed_through = _capture_owed_through(ctx)
    surfaces: list[dict[str, object]] = []
    for ticker, present in ctx.roster.items():
        for surface in present:
            surfaces.append(
                _latest_cycle(
                    con, ctx, surface, ticker, spans_by_ticker.get(ticker, ()), owed_through
                )
            )
    phase = ctx.session.phase()
    stamp = read_metadata(ctx.paths.root)
    minted = stamp.token_minted_at
    return {
        "as_of": _iso(ctx.now),
        "session_date": ctx.session.session_date().isoformat(),
        "is_session": phase is not SessionPhase.NON_SESSION,
        "phase": phase.value,
        "stale_after_minutes": ctx.guards.watchdog_page_minutes,
        "capture_owed_through": None if owed_through is None else _iso(owed_through),
        "tickers": list(ctx.roster),
        "surfaces": surfaces,
        "token_minted_at": None if minted is None else _iso(minted),
        "token_age_minutes": None if minted is None else _minutes(ctx.now - minted),
        "token_sunday_countdown_minutes": (
            None if minted is None else _minutes(sunday_canary_due(minted) - ctx.now)
        ),
        "dead_man_last_ping": (
            None if stamp.dead_man_last_ping is None else _iso(stamp.dead_man_last_ping)
        ),
        "dead_man_age_minutes": (
            None
            if stamp.dead_man_last_ping is None
            else _minutes(ctx.now - stamp.dead_man_last_ping)
        ),
        "dead_man_expected": _ping_owed(ctx.now, ctx.guards.dead_man_grace_minutes),
        "dead_man_starved": _dead_man_starved(
            ctx.now, stamp.dead_man_last_ping, ctx.guards.dead_man_grace_minutes
        ),
        "dead_man_grace_minutes": ctx.guards.dead_man_grace_minutes,
        "stamp_age_minutes": (
            None if stamp.stamped_at is None else _minutes(ctx.now - stamp.stamped_at)
        ),
        "pages_failed_to_send": undelivered(ctx.paths.root, ctx.session.session_date()),
    }


def _latest_cycle(
    con: duckdb.DuckDBPyConnection,
    ctx: QueryContext,
    surface: str,
    ticker: str,
    spans: tuple[CaptureSpan, ...],
    owed_through: datetime | None,
) -> dict[str, object]:
    """One Now row: the latest slot of any kind and the latest data slot, with its age.

    ``spans`` empty means no scope clamp: the ticker is always in scope. Otherwise the
    reported ``capture_start`` is the most recently opened span's start, so a currently
    live ticker shows when it started and a retired one still shows its last known start.

    ``stale`` is the row's verdict, per ``_surface_stale``, and it is what the page
    colours by. The age beside it is against ``now``, because that is the number a reader
    wants to see. The verdict is against ``owed_through``, because that is the minute the
    age means anything against. The two part company the moment the session ends.
    """
    last_data_ms: int | None = None
    last_ms: int | None = None
    last_status: str | None = None
    last_error: tuple[str, ...] = ()
    health = SegmentHealth()
    days = _dates_desc(ctx.paths, surface, ticker)
    walked = days[:MAX_LOOKBACK_SESSIONS]
    for day in walked:
        aggregates, day_health = _slot_aggregates(con, ctx.paths, surface, ticker, day)
        health += day_health
        if not aggregates:
            continue
        if last_ms is None:
            latest = aggregates[-1]
            last_ms = latest.slot_ms
            last_status = latest.status
            last_error = latest.error_classes
        with_data = [agg for agg in aggregates if agg.data_rows > 0]
        if with_data:
            last_data_ms = with_data[-1].slot_ms
            break
    minutes_since: float | None = None
    if last_data_ms is not None:
        minutes_since = round((_slot_ms(ctx.now) - last_data_ms) / 60_000, 1)
    capture_start = spans[-1].start if spans else None
    in_scope = not spans or _in_scope(ctx.now, spans)
    return {
        "ticker": ticker,
        "surface": surface,
        "last_data_snap_ts": None if last_data_ms is None else _iso_et(last_data_ms),
        "minutes_since": minutes_since,
        "last_snap_ts": None if last_ms is None else _iso_et(last_ms),
        "last_status": last_status,
        "last_error_class": list(last_error),
        "last_error_class_count": len(last_error),
        "capture_start": None if capture_start is None else _iso(capture_start),
        "in_scope": in_scope,
        "stale": _surface_stale(ctx, owed_through, last_data_ms, capture_start, in_scope),
        "lookback_exhausted": last_data_ms is None and len(days) > len(walked),
        **health.payload(),
    }


def query_today(
    con: duckdb.DuckDBPyConnection,
    ctx: QueryContext,
    *,
    day: date | None = None,
    ticker: str | None = None,
) -> dict[str, object]:
    """The Today panel: a per-ticker, per-surface minute strip across the session's slots.

    ``day`` defaults to the clock's session date. ``ticker`` defaults to every ticker in
    the roster. A day the calendar calls closed returns ``is_session`` false and no
    strips, so a holiday renders as *no session*, never as zero percent. A date the
    calendar cannot judge at all, outside the window it loads, is a bad request.

    The slot list comes from the calendar through ``SessionClock.bounds``, the open
    through the option close. Each slot reports its status, its data row count, and every
    gap reason it carried. A slot with data rows beside a gap marker, a partial chain
    snapshot, reads captured, or suspect when a row carries the suspect flag, and still
    carries the marker's class among its reasons. A slot with no rows at all is pending
    until ``SLOT_VERDICT_GRACE`` has run out past it, never missing, so the minute being
    captured right now is not accused of a marker its cycle has not had time to write.
    A slot before the ticker's ``capture_start`` epoch is out of scope, neither missing
    nor pending. Data rows win
    over both of those, so a real cycle is never hidden. A gap row wins over pending but
    not over out of scope, because the design pins minutes before the epoch as out of
    scope and never gaps.
    """
    session_day = day if day is not None else ctx.session.session_date()
    payload: dict[str, object] = {
        "as_of": _iso(ctx.now),
        "date": session_day.isoformat(),
        "is_session": False,
        "early_close": None,
        "session_open": None,
        "equity_close": None,
        "option_close": None,
        "slot_count": 0,
        "strips": [],
    }
    try:
        bounds = ctx.session.bounds(session_day)
    except NotASession:
        return payload
    except _CALENDAR_RANGE_ERRORS:
        raise QueryParameterError("date outside the calendar's range") from None
    slots = session_slots(bounds)
    tickers = [ticker] if ticker is not None else list(ctx.roster)
    spans_by_ticker = _capture_spans(ctx.paths, tickers, session_day)
    strips: list[dict[str, object]] = []
    for symbol in tickers:
        for surface in ctx.roster.get(symbol, ()):
            aggregates, health = _slot_aggregates(con, ctx.paths, surface, symbol, session_day)
            strips.append(
                _strip(
                    symbol,
                    surface,
                    slots,
                    aggregates,
                    health,
                    ctx.now,
                    spans_by_ticker.get(symbol, ()),
                )
            )
    payload.update(
        is_session=True,
        early_close=bounds.early_close,
        session_open=_iso(bounds.open),
        equity_close=_iso(bounds.equity_close),
        option_close=_iso(bounds.option_close),
        slot_count=len(slots),
        strips=strips,
    )
    return payload


def _slot_status(
    agg: SlotAggregate | None,
    slot: datetime,
    judgeable_ms: int,
    spans: tuple[CaptureSpan, ...],
) -> tuple[str, int, tuple[str, ...]]:
    """One slot's status, its data-row count, and every reason it carried.

    This is the five-step ladder, and it is the only copy. :func:`_strip` renders it per
    minute for the Today panel and :func:`_day_counts` totals it per ticker-day for the
    History heatmap, so the two panels cannot drift into disagreeing about what a minute
    is. The ladder itself is documented on :func:`_strip`, where the strip that shows it
    lives.

    ``judgeable_ms`` is the last slot key a verdict is owed for. ``spans`` empty means no
    scope clamp, so every slot is in scope.
    """
    rows = 0 if agg is None else agg.data_rows
    error_classes: tuple[str, ...] = () if agg is None else agg.error_classes
    if agg is not None and agg.data_rows > 0:
        return agg.status, rows, error_classes
    if spans and not _in_scope(slot, spans):
        return STATUS_OUT_OF_SCOPE, 0, ()
    if agg is not None:
        return agg.status, rows, error_classes
    pending = _slot_ms(slot) > judgeable_ms
    return (STATUS_PENDING if pending else STATUS_MISSING), rows, error_classes


def _strip(
    ticker: str,
    surface: str,
    slots: Sequence[datetime],
    aggregates: Sequence[SlotAggregate],
    health: SegmentHealth,
    now: datetime,
    spans: tuple[CaptureSpan, ...],
) -> dict[str, object]:
    """One strip: every session slot with its status, denominated by the slot list.

    Each cell's status comes off a five-step ladder.

    1. A slot with data rows is captured, or suspect when a row carries the flag.
    2. A slot outside every one of the ticker's capture spans is out of scope. Data
       still wins above it, so a real cycle is never hidden, but a gap marker there is
       out of scope, because the design pins an out-of-scope minute as never a gap. This
       covers a slot before the first span, after a closed one, and between two spans
       following a retirement and a rejoin.
    3. A slot with gap rows is a gap.
    4. A slot whose verdict grace has not run out against the injected instant is
       pending. That covers a slot still in the future and the recent past alike, so a
       cycle still fetching is never called absent.
    5. Anything else is missing.

    ``spans`` empty means no scope clamp: every slot is in scope.
    """
    by_slot = {agg.slot_ms: agg for agg in aggregates}
    # The instant a slot must be at or before to be judged at all. Slots after it are
    # still owed rather than absent, so the strip says pending instead of accusing a
    # cycle that has not finished running.
    judgeable_ms = _slot_ms(now - SLOT_VERDICT_GRACE)
    cells: list[dict[str, object]] = []
    counts = dict.fromkeys(STATUSES, 0)
    for slot in slots:
        key = _slot_ms(slot)
        status, rows, error_classes = _slot_status(by_slot.get(key), slot, judgeable_ms, spans)
        counts[status] += 1
        cells.append(
            {
                "slot": slot.isoformat(),
                "status": status,
                "rows": rows,
                "error_class": list(error_classes),
                "error_class_count": len(error_classes),
            }
        )
    capture_start = spans[-1].start if spans else None
    return {
        "ticker": ticker,
        "surface": surface,
        "capture_start": None if capture_start is None else _iso(capture_start),
        "counts": counts,
        **health.payload(),
        "slots": cells,
    }


# -- the History panel -------------------------------------------------------

# How far back the window reaches, its end included. Calendar days, not sessions: the
# window is a span of wall time a reader names, and the calendar decides how many
# sessions fall in it. Thirty days over the live calendar is twenty-one.
#
# This is a module constant rather than a request field, and so is the report count
# below. Rule 2 above allows a request exactly two parameters, a ticker and a date, and
# ``validate_parameters`` builds exactly those two keyword arguments. A third name
# declared in ``NamedQuery.parameters`` would pass the unknown-field check and then be
# dropped on the floor, because nothing builds a keyword for it.
HISTORY_WINDOW_DAYS = 30

# How many nightly report files the panel reads, newest first. ``report.py`` pins the
# growth rule this answers: a held finding recurs every night because nothing settles
# it, and nothing under ``reports/`` is ever pruned, so one unresolved disagreement is
# thirty files in one directory after a month. The panel is served per request, so it
# reads a bounded tail rather than the directory.
HISTORY_REPORTS = 10

# The largest nightly file the panel will open. ``HISTORY_REPORTS`` bounds how many files
# are read and this bounds how much, which is not the same promise. The read is plain
# filesystem I/O, so the connection's ``QUERY_MEMORY_LIMIT`` does not reach it, and the
# service runs under ``KeepAlive``, so a single oversized file's cost would sit in the
# process for as long as it lives. A nightly file is counts and one line per failing
# surface-ticker, so a real one is kilobytes and this refuses nothing the sweep writes.
HISTORY_REPORT_MAX_BYTES = 4 * 1024 * 1024

# The window aggregate: the same per-slot grouping ``_SLOT_SELECT`` makes, keyed by the
# file each row came from so one query covers every ticker-day on a surface. The
# filename is mapped back to its ticker and day in Python, against the very paths this
# query was handed, so no pattern here has to agree with the lake's directory layout.
#
# Only the sealed partitions come through here. A ticker-day with journal segments goes
# to ``_slot_aggregates`` instead, which is the one reader that knows the durability
# rules and re-checks the partition after reading them.
_WINDOW_SELECT = """
SELECT filename,
       slot_ms,
       count(*) FILTER (WHERE row_kind = $data_kind) AS data_rows,
       count(*) FILTER (WHERE row_kind = $gap_kind) AS gap_rows,
       count(*) FILTER (
           WHERE row_kind IS DISTINCT FROM $data_kind
             AND row_kind IS DISTINCT FROM $gap_kind
       ) AS other_rows,
       bool_or(coalesce(suspect, false)) AS suspect,
       list_sort(
           array_agg(DISTINCT error_class) FILTER (WHERE error_class IS NOT NULL)
       ) AS error_classes
FROM (
    SELECT filename,
           epoch_ms(TRY_CAST(snap_ts AS TIMESTAMPTZ)) AS slot_ms,
           row_kind, error_class, suspect
    FROM read_parquet($partitions, union_by_name = true, filename = true)
)
GROUP BY filename, slot_ms
ORDER BY filename, slot_ms NULLS LAST
"""


def _window_sessions(ctx: QueryContext, end: date) -> list[tuple[date, list[datetime]]]:
    """Every session in the window ending at ``end``, oldest first, with its own slots.

    The window is ``HISTORY_WINDOW_DAYS`` calendar days wide with ``end`` inside it, and
    the calendar decides which of them are sessions. Thirty days over the live calendar
    is twenty-one. Each session carries its own slot list, from the open through the
    option close, so an early close is a short full day here exactly as it is on the
    Today strip.

    A day the calendar cannot judge at all is left out rather than refused. These dates
    are computed here and no client sent them, so there is no bad request to report.
    ``query_today`` turns that same condition into a 400 precisely because there the
    date did come from the client.
    """
    sessions: list[tuple[date, list[datetime]]] = []
    day = end - timedelta(days=HISTORY_WINDOW_DAYS - 1)
    while day <= end:
        try:
            sessions.append((day, session_slots(ctx.session.bounds(day))))
        except (NotASession, *_CALENDAR_RANGE_ERRORS):
            pass
        day += timedelta(days=1)
    return sessions


def _window_aggregates(
    con: duckdb.DuckDBPyConnection,
    paths: LakePaths,
    surface: str,
    tickers: Sequence[str],
    sessions: Sequence[date],
) -> dict[tuple[str, date], tuple[list[SlotAggregate], SegmentHealth]]:
    """Every ticker-day's slot aggregates for one surface across the window.

    The sealed partitions are read in one query and the rest per ticker-day, which is
    what keeps the window's cost growing with bytes rather than with days. A ticker-day
    holding journal segments never joins the bulk read. It goes through
    ``_slot_aggregates``, because that is the reader that unions the journal with the
    partition and re-checks the partition after the segments are read, "so a seal that
    landed in between does not render a fully captured day as entirely missing."

    The bulk read is all-or-nothing, which is the price of one query, so any failure of
    it falls back to reading each ticker-day the per-day way. That fallback adds no count
    of its own. ``_slot_aggregates`` counts a partition it cannot read, on the one cell
    that holds it, so adding a second count here would blame every healthy ticker-day on
    the surface for one bad file and count the bad one twice.

    **The catch is broad here, and this is the one place in this module where it is.**
    ``_PARTITION_READ_ERRORS`` stays narrow, because it guards a read of one file and its
    own comment says why: the pair is enumerated "so a real SQL defect still surfaces."
    This read is a different shape. It unions every partition in the window by name, so
    what it can raise depends on the data rather than on the statement: two partitions
    disagreeing on a column's type raise a ``BinderException``, and so does a column that
    no partition in the union happens to carry. Neither is reachable per-day, because
    ``_slot_aggregates`` always registers the journal view and its pinned schema beside
    the one file.

    So the fallback is not a swallow. It is a retreat to the reader that does not union,
    which is the reader that was there before this panel, and a real defect surfaces from
    it exactly as it did. The traceback is logged either way. The cost of narrowing this
    back is the whole page: ``_serve`` turns an escape into a 500, so one drifted
    partition thirty days back would blank the Now and Today panels beside this one.
    """
    result: dict[tuple[str, date], tuple[list[SlotAggregate], SegmentHealth]] = {}
    bulk: dict[str, tuple[str, date]] = {}
    for ticker in tickers:
        for day in sessions:
            if _journal_segments(paths, surface, ticker, day):
                result[(ticker, day)] = _slot_aggregates(con, paths, surface, ticker, day)
                continue
            partition = paths.partition_path(surface, ticker, day)
            if partition.is_file():
                bulk[str(partition)] = (ticker, day)
    if not bulk:
        return result
    params = {
        "data_kind": journal.ROW_KIND_DATA,
        "gap_kind": journal.ROW_KIND_GAP,
        "partitions": sorted(bulk),
    }
    try:
        rows = con.execute(_WINDOW_SELECT, params).fetchall()
    except Exception:  # noqa: BLE001 - the window must not cost the page, per the docstring
        log.exception("window read failed for %s, so the window falls back per day", surface)
        for key in bulk.values():
            ticker, day = key
            result[key] = _slot_aggregates(con, paths, surface, ticker, day)
        return result
    grouped: dict[tuple[str, date], list[tuple]] = {key: [] for key in bulk.values()}
    for row in rows:
        key = bulk.get(row[0])
        if key is not None:
            grouped[key].append(row[1:])
    for key, group in grouped.items():
        placed, unparseable_rows = _placed_aggregates(group)
        health = SegmentHealth().with_row_counts(
            drifted_rows=sum(agg.other_rows for agg in placed),
            unparseable_stamp_rows=unparseable_rows,
        )
        result[key] = ([agg for agg in placed if agg.has_bound_rows], health)
    return result


def _history_cell(
    ticker: str,
    surface: str,
    day: date,
    slots: Sequence[datetime],
    aggregates: Sequence[SlotAggregate],
    health: SegmentHealth,
    now: datetime,
    spans: tuple[CaptureSpan, ...],
) -> dict[str, object]:
    """One heatmap cell: a ticker-day's slot counts, and a percent where one is honest.

    The counts come off ``_slot_status``, the same ladder the Today strip renders per
    minute, so the two panels cannot disagree about what a minute is. They are keyed by
    the six statuses, which is what keeps this cell inside the vocabulary the page
    already renders.

    **A percent alone would collapse three different answers into zero.** Measured
    against the live lake, the window's cells include days entirely out of scope, where
    nothing was owed, days entirely gap-marked, where the minutes were owed and missed
    and the miss is recorded, and an onboarding day that is part of each. A percent
    renders all three as zero, which is the reading ``_capture_spans`` exists to
    prevent: "Onboarding day renders 'onboarded 11:00,' not 40 percent missing." So the
    counts are the cell and the percent rides beside them.

    ``judged`` is the denominator: the day's slots less the out-of-scope ones, which
    were never owed, and less the pending ones, whose verdict is not yet due. A day with
    no judged minute carries ``None`` rather than zero, because no number is a different
    answer from zero. That is ``Nightly``'s own rule for its three counts.
    """
    by_slot = {agg.slot_ms: agg for agg in aggregates}
    judgeable_ms = _slot_ms(now - SLOT_VERDICT_GRACE)
    counts = dict.fromkeys(STATUSES, 0)
    for slot in slots:
        status, _, _ = _slot_status(by_slot.get(_slot_ms(slot)), slot, judgeable_ms, spans)
        counts[status] += 1
    judged = len(slots) - counts[STATUS_OUT_OF_SCOPE] - counts[STATUS_PENDING]
    captured = counts[STATUS_CAPTURED] + counts[STATUS_SUSPECT]
    return {
        "ticker": ticker,
        "surface": surface,
        "date": day.isoformat(),
        "slot_count": len(slots),
        "judged": judged,
        "captured_pct": None if judged <= 0 else round(100.0 * captured / judged, 1),
        "counts": counts,
        **health.payload(),
    }


def _open_quarantines(root: Path) -> tuple[list[dict[str, object]], str | None]:
    """Every partition the quarantine ledger currently withholds, and what refused a read.

    Contained, and deliberately not where the ledger throws. ``_latest_by_partition``
    raises on a body line that parses and names no partition, and it names the only two
    callers allowed to survive that: "the close+5 guard's prologue and the marking pass.
    Every other caller is a place where stopping is correct." Stopping is not correct
    here, because the heatmap and the nightly reports have nothing to do with the
    ledger, and ``_serve``'s blanket catch would turn one damaged line into a 500 for
    the whole panel.

    Catching at this boundary rather than widening the ledger's rule leaves that rule and
    its docstring true. ``_capture_spans`` already keeps the same promise for the same
    reason: one panel served without a clamp rather than a panel not served at all.

    The entry's shape is marketlake #139's and #139 is unbuilt, so only the partition
    path and the ``verdict`` field are read, both defensively. The panel prints no
    sign-off command, because the tool that would run it does not exist and its spelling
    is not settled.
    """
    try:
        ledger = latest_quarantine(root)
    except Exception as exc:  # noqa: BLE001 - a summary must not cost the panel
        log.exception("quarantine ledger unreadable, so the panel reports it instead")
        return [], type(exc).__name__
    open_entries = [
        {"partition": partition, "verdict": entry.get(VERDICT_FIELD)}
        for partition, entry in sorted(ledger.items())
        if is_quarantined(entry)
    ]
    return open_entries, None


def _nightly_reports(root: Path, limit: int) -> tuple[list[dict[str, object]], int, str | None]:
    """The last ``limit`` nightly report files, newest first, with what would not read.

    Counted off the filesystem rather than queried, which is ``undelivered``'s rule:
    "the ordinary day has none and a SQL read over an empty glob raises rather than
    returning zero." An absent ``reports/`` directory is a true zero here for the same
    reason, and today it is the true answer, because the first ``eod-sweep`` run has not
    happened.

    ``nightly_path`` names a file ``{day}-{stamp}-{pid}.json``, so the day sorts first
    and the newest files are the tail of a name sort. Only ``limit`` of them are opened,
    and none over ``HISTORY_REPORT_MAX_BYTES`` is opened at all.
    The glob is on the ``reports/`` root, so the four producers' own subdirectories are
    not matched, which is the naming rule ``report.py`` chose a reader for.

    Every field is read with ``.get``. The lake's own report tree already carries two key
    sets from one writer at two versions, so a reader spelling a key outright would break
    on the older file. An absent count renders as no number rather than zero, which is
    ``Nightly``'s own rule.
    """
    directory = root / REPORTS_DIR
    if not directory.is_dir():
        # A true zero, which is today's answer: no nightly run has filed anything yet.
        return [], 0, None
    try:
        names = sorted(name for name in os.listdir(directory) if name.endswith(".json"))
    except OSError as exc:
        # ``Path.glob`` is not used for this listing, and that is the whole reason. It
        # swallows the ``OSError`` a directory it cannot read raises and yields nothing,
        # so an unreadable ``reports/`` would render as "no report has been filed yet".
        # That is a false zero on the one panel a reader opens to find out what is wrong.
        log.warning("reports directory unlistable: %s", type(exc).__name__)
        return [], 0, type(exc).__name__
    files = [directory / name for name in names[-limit:]]
    reports: list[dict[str, object]] = []
    unreadable = 0
    for path in reversed(files):
        try:
            if path.stat().st_size > HISTORY_REPORT_MAX_BYTES:
                # Counted rather than read. A file this size is not a nightly report, and
                # reading it to find that out is the cost the bound exists to refuse.
                log.warning("nightly report over the size bound, so the panel counts it")
                unreadable += 1
                continue
            entry = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - one bad file must not cost the others
            log.exception("nightly report unreadable, so the panel counts it instead")
            unreadable += 1
            continue
        if not isinstance(entry, dict):
            unreadable += 1
            continue
        reports.append(_nightly_payload(entry))
    return reports, unreadable, None


def _nightly_payload(entry: Mapping[str, object]) -> dict[str, object]:
    """One nightly file as the panel renders it, every field taken defensively.

    ``unfiled`` is summed off the pieces rather than read: ``Nightly.unfiled`` is a
    property and ``write_nightly`` puts eleven keys in the file without it.

    ``problems`` withheld the run's ping and ``report`` are the report-tier findings,
    which "send no message of their own." The second is why this panel reads these files
    at all: the design names the disk runway, ``pmset`` drift and a suspected unscheduled
    closure as the three that ride here, and this panel is their reader. It renders the
    lines the file carries and computes none of them.

    ``day`` and ``at`` both ride along, because the design pins the nightly report as
    "the one pre-written thing the dashboard shows, and it is dated, so a stale one never
    reads as now."
    """
    pieces = entry.get("pieces")
    pieces = pieces if isinstance(pieces, Mapping) else {}
    unfiled = 0
    for outcome in pieces.values():
        if isinstance(outcome, Mapping) and isinstance(outcome.get("unfiled"), int):
            unfiled += outcome["unfiled"]
    return {
        "day": entry.get("day"),
        "at": entry.get("at"),
        "session": entry.get("session"),
        "pinged": entry.get("pinged"),
        "gaps": entry.get("gaps"),
        "quarantined": entry.get("quarantined"),
        "disagreements": entry.get("disagreements"),
        "pages_lost": entry.get("pages_lost"),
        "unfiled": unfiled,
        "pieces": {
            name: outcome.get("refusal")
            for name, outcome in pieces.items()
            if isinstance(outcome, Mapping)
        },
        "problems": _lines(entry.get("problems")),
        "report": _lines(entry.get("report")),
    }


def _lines(value: object) -> list[str]:
    """A report file's string list, or an empty one for anything else it holds."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def query_history(con: duckdb.DuckDBPyConnection, ctx: QueryContext) -> dict[str, object]:
    """The History panel: the completeness window, the quarantines, and the last nights.

    One named query rather than three, which is what the design pins: "The History panel
    renders it through one named query." It takes no request parameter, so the window's
    end is the clock's session date and its width is ``HISTORY_WINDOW_DAYS``.

    **Nothing here may raise.** ``_serve`` turns any escape into a 500 for the whole
    panel, and ``status.html`` paints nothing until every payload lands, so one bad file
    would blank the Now and Today panels beside this one. Each of the three reads is
    contained at its own boundary and reports its failure as a value: the window read
    counts an unreadable partition, ``_open_quarantines`` names the class that refused
    the ledger, and ``_nightly_reports`` counts the files that would not parse. That is
    the rule ``_capture_spans``, ``_slot_aggregates`` and ``undelivered`` each already
    keep.

    The scope clamp is resolved **once, at the window's end**, not once per day.
    ``_capture_spans`` resolves each ticker through the master as of the date it is
    given, and the live master's rows begin on the onboarding day, so a per-day
    resolution returns nothing for every earlier day, leaves those tickers unclamped,
    and renders a wall of ``missing`` for minutes nothing was owed. The spans themselves
    already bound the window, so the resolution date does no work they are not doing.
    What this reading does not cover is a symbol the master no longer maps today, after
    a retirement or an OCC re-symboling. That is marketlake #405's and stays there.
    """
    end = ctx.session.session_date()
    sessions = _window_sessions(ctx, end)
    tickers = sorted(ctx.roster)
    spans_by_ticker = _capture_spans(ctx.paths, tickers, end)
    days = [day for day, _ in sessions]
    cells: list[dict[str, object]] = []
    for surface in PANEL_SURFACES:
        present = [t for t in tickers if surface in ctx.roster.get(t, ())]
        if not present:
            continue
        window = _window_aggregates(con, ctx.paths, surface, present, days)
        for day, slots in sessions:
            for ticker in present:
                aggregates, health = window.get((ticker, day), ([], SegmentHealth()))
                cells.append(
                    _history_cell(
                        ticker,
                        surface,
                        day,
                        slots,
                        aggregates,
                        health,
                        ctx.now,
                        spans_by_ticker.get(ticker, ()),
                    )
                )
    quarantines, ledger_unreadable = _open_quarantines(ctx.paths.root)
    reports, reports_unreadable, reports_error = _nightly_reports(ctx.paths.root, HISTORY_REPORTS)
    return {
        "as_of": _iso(ctx.now),
        "window_days": HISTORY_WINDOW_DAYS,
        "window_start": (end - timedelta(days=HISTORY_WINDOW_DAYS - 1)).isoformat(),
        "window_end": end.isoformat(),
        "sessions": [day.isoformat() for day in days],
        "tickers": tickers,
        "cells": cells,
        "quarantines": quarantines,
        "quarantine_count": len(quarantines),
        "quarantine_unreadable": ledger_unreadable,
        "reports_read": HISTORY_REPORTS,
        "reports": reports,
        "reports_unreadable": reports_unreadable,
        "reports_error": reports_error,
    }


@dataclass(frozen=True)
class NamedQuery:
    """One entry in the fixed-query registry.

    ``run`` takes the connection, the context, and the validated parameters as keywords.
    ``parameters`` names the request fields the query accepts. A request carrying any
    other field is refused.
    """

    name: str
    run: Callable[..., dict[str, object]]
    parameters: frozenset[str]


# The whole query surface. The HTTP layer maps a path to one of these names and nothing
# else. Adding a panel means adding an entry here, with its own validated parameters.
NAMED_QUERIES: Mapping[str, NamedQuery] = {
    "now": NamedQuery("now", query_now, frozenset()),
    "today": NamedQuery("today", query_today, frozenset({"date", "ticker"})),
    "history": NamedQuery("history", query_history, frozenset()),
}

# The route table: request path to query name. A path not here is a 404.
ROUTES: Mapping[str, str] = {
    "/api/now": "now",
    "/api/today": "today",
    "/api/history": "history",
}


def validate_parameters(
    query: NamedQuery, raw: Mapping[str, str], roster: Mapping[str, object]
) -> dict[str, object]:
    """Turn a request's raw fields into the query's typed keyword arguments.

    Every field is checked: an unknown field, a malformed date, or a ticker outside the
    roster raises ``QueryParameterError``. This runs before any connection is touched.
    """
    unknown = set(raw) - query.parameters
    if unknown:
        raise QueryParameterError("unknown parameter")
    params: dict[str, object] = {}
    if "date" in raw:
        params["day"] = parse_date(raw["date"])
    if "ticker" in raw:
        params["ticker"] = validate_ticker(raw["ticker"], roster)
    return params


# -- the service -------------------------------------------------------------


def load_status_page() -> bytes:
    """The static page's bytes, read from the package. It carries no external resource."""
    return resources.files("lake").joinpath("static").joinpath(STATUS_PAGE).read_bytes()


def load_favicon() -> bytes:
    """The tab icon's bytes, read from the package beside the page."""
    return resources.files("lake").joinpath("static").joinpath(FAVICON).read_bytes()


class DashboardService:
    """The query service: one sandboxed connection, the injected seams, and the page.

    The connection is opened once at construction and is the only one the service ever
    holds. Each request runs on a cursor over it, so requests never share statement
    state, and every cursor inherits the locked sandbox. The clock and calendar are
    injected, so a test decides what time it is and which days are sessions. The guard
    constants are injected too, so the panel reports the machine's own staleness
    threshold rather than the pinned default it may have been recalibrated away from. The
    page and the tab icon are injected on the same terms. Each defaults to the bytes
    shipped in the package, and a test that wants neither passes its own.
    """

    def __init__(
        self,
        lake_root: Path | str,
        *,
        clock: Clock,
        calendar: Calendar,
        guards: GuardConstants | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
        page: bytes | None = None,
        icon: bytes | None = None,
    ) -> None:
        self._paths = LakePaths(Path(lake_root).resolve())
        self._clock = clock
        self._calendar = calendar
        self._guards = guards if guards is not None else GuardConstants()
        self._con = connection if connection is not None else open_lake_connection(self._paths.root)
        self._page = page if page is not None else load_status_page()
        self._icon = icon if icon is not None else load_favicon()

    @property
    def page(self) -> bytes:
        return self._page

    @property
    def icon(self) -> bytes:
        return self._icon

    def roster(self) -> dict[str, tuple[str, ...]]:
        """The lake's current roster, re-read per call so a new ticker appears at once."""
        return lake_roster(self._paths)

    def run_query(self, name: str, raw: Mapping[str, str]) -> dict[str, object]:
        """Run one named query with raw request fields. Validation comes first.

        The roster is walked once and then carried on the context, because validation
        and the query itself both need it and the walk lists every journal date.

        Raises ``KeyError`` for a name outside the registry and ``QueryParameterError``
        for a bad field. Both fire before the connection is touched, with one exception:
        ``query_today`` raises ``QueryParameterError`` for a date the calendar cannot
        judge, which happens after the cursor below has opened. No statement runs on that
        path, so the date still never reaches SQL and no lake data is read.
        """
        query = NAMED_QUERIES[name]
        roster = self.roster()
        params = validate_parameters(query, raw, roster)
        ctx = QueryContext(
            paths=self._paths,
            now=self._clock.now(),
            session=SessionClock(self._clock, self._calendar),
            roster=roster,
            guards=self._guards,
        )
        cursor = self._con.cursor()
        try:
            return query.run(cursor, ctx, **params)
        finally:
            cursor.close()


# -- the HTTP layer ----------------------------------------------------------


def host_allowed(host: str | None) -> bool:
    """Whether a ``Host`` header names this service: localhost or the loopback address.

    An optional ``:port`` is allowed and must be digits. Anything else, including a
    missing header, a hostname that merely starts with ``localhost``, or an IPv6
    literal, is refused.
    """
    if host is None:
        return False
    host = host.strip()
    name, sep, port = host.rpartition(":")
    if sep:
        if not port.isdigit():
            return False
    else:
        name = host
    return name.lower() in ALLOWED_HOSTS


def _single_valued(query: str) -> dict[str, str]:
    """The query string as one value per field. A repeated field is refused."""
    fields: dict[str, str] = {}
    for key, values in parse_qs(query, keep_blank_values=True).items():
        if len(values) != 1:
            raise QueryParameterError("repeated parameter")
        fields[key] = values[0]
    return fields


class _Handler(BaseHTTPRequestHandler):
    """One request. The Host check runs first, then the method check, then the route."""

    service: DashboardService  # set on the per-server subclass by ``make_server``
    server_version = "marketlake-dashboard"
    sys_version = ""  # never advertise the interpreter version

    def __getattr__(self, name: str) -> object:
        # The base class dispatches on ``do_<METHOD>`` and answers 501 for a method it
        # cannot find. Routing every method name here means the Host check runs before
        # anything else this class does, whatever the verb, and a non-GET verb gets its
        # 405. The stdlib still answers a few malformed requests before dispatch ever
        # happens: 414 for an over-long request line, 431 for too many or too long
        # headers, 400 for a bad version. Those replies quote only the client's own
        # escaped request line and touch no lake data, so the claim is that the Host
        # check precedes every lake read, not that it is literally the first byte
        # written. Do not restate it as the latter.
        if name.startswith("do_"):
            return self._serve
        raise AttributeError(name)

    def do_GET(self) -> None:
        self._serve()

    def _serve(self) -> None:
        if not host_allowed(self.headers.get("Host")):
            self._send_json(HTTPStatus.FORBIDDEN, {"error": "forbidden host"})
            return
        if self.command != "GET":
            self._send_json(
                HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method not allowed"}, allow="GET"
            )
            return
        parts = urlsplit(self.path)
        if parts.path == "/":
            self._send_page()
            return
        if parts.path == FAVICON_PATH:
            self._send_icon()
            return
        name = ROUTES.get(parts.path)
        if name is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        try:
            payload = self.service.run_query(name, _single_valued(parts.query))
        except QueryParameterError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception:
            log.exception("named query %s failed", name)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "query failed"})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _send_page(self) -> None:
        body = self.service.page
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        # The page is self-contained by rule. The policy lets the browser enforce it: no
        # script, style, image, or fetch may leave the page except to this same origin.
        # ``frame-ancestors``, ``base-uri`` and ``form-action`` are listed because none
        # of the three falls back to ``default-src``, so omitting them leaves the page
        # framable by any origin. ``script-src`` deliberately lists ``'unsafe-inline'``
        # alone and not ``'self'``: that blocks every external script URL, same-origin
        # ones included, which is tighter than adding ``'self'`` would be. Do not "fix"
        # it by adding ``'self'``.
        # ``img-src 'self'`` is the tab icon's whole cost. It buys one image, the icon
        # served at ``/favicon.ico``, which the page declares in its head. A favicon
        # fetch is an image fetch, so the page's own policy governs it. Without this
        # directive the fetch is refused and the tab stays blank. Safari 26.6.2 and
        # Chrome 152.0.7977.82 on macOS 26.6.2 were both tested, and both behave that
        # way. The page's declaration is what makes the directive checkable. It is not
        # what makes the fetch happen: with the directive in place a browser asks the
        # origin for ``/favicon.ico`` whether or not anything declares it. What the
        # declaration buys is a reason in the markup for the directive to be here. Drop
        # the declaration and a later tidy-up drops the directive with it, and the icon
        # goes with both. Do not widen it past ``'self'``.
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; connect-src 'self'; img-src 'self'; "
            "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
            "frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
        )
        self.end_headers()
        self._write_body(body)

    def _send_icon(self) -> None:
        """The tab icon. Constant bytes, read once at construction, with no lake access.

        ``image/x-icon`` is the conventional type for this container. Safari 26.6.2 and
        Chrome 152.0.7977.82 both accept it. ``nosniff`` sits safely beside it. That
        header stops a browser guessing a type the response never declared, and this
        response declares the type it is.
        """
        body = self.service.icon
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/x-icon")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self._write_body(body)

    def _send_json(self, status: HTTPStatus, payload: object, *, allow: str | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if allow is not None:
            self.send_header("Allow", allow)
        self.end_headers()
        self._write_body(body)

    def _write_body(self, body: bytes) -> None:
        """Write the body, except on ``HEAD``.

        RFC 9110 forbids content on a HEAD response. The headers still describe the
        response the request earned, ``Content-Length`` included, so a client learns the
        length without being sent the bytes. That response is the 405, not the matching
        GET's, because ``_serve`` refuses every verb but ``GET`` before it routes.
        """
        if self.command != "HEAD":
            self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib name
        log.info("%s " + format, self.address_string(), *args)


def make_server(service: DashboardService, port: int) -> ThreadingHTTPServer:
    """A server bound to the loopback address on ``port``, ready to ``serve_forever``.

    Port ``0`` asks the kernel for a free port, which a test reads back from
    ``server.server_address``. The bind host is not a parameter.
    """
    handler = type("DashboardHandler", (_Handler,), {"service": service})
    return ThreadingHTTPServer((BIND_HOST, port), handler)


# -- the command-line entry --------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract: which lake to serve, and on which port.

    Three arguments.

    1. ``--lake-root`` names a lake to serve directly.
    2. ``--config`` names the machine-local config to read the lake root from instead.
    3. ``--port`` says where to listen.

    The bind address is not among them. It is a constant, so the service cannot be
    exposed by a flag.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.dashboard",
        description="Serve the read-only status dashboard on localhost.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port to listen on (default {DEFAULT_PORT}).",
    )
    parser.add_argument(
        "--config",
        help="Path to config.yaml (defaults to the standard location). Read once at "
        "startup, for the lake root and the guard constants.",
    )
    parser.add_argument(
        "--lake-root",
        help="Serve this lake root directly and never read config.yaml. Handy for a fixture lake.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.dashboard`` entry. Serves until interrupted.

    The lake root comes from ``--lake-root`` or, failing that, from the machine-local
    config. The config is read once here, before the connection opens, and only its
    ``lake_root`` and its guard constants are kept. Serving a lake root directly means
    no config is read, so the guards are the design's pinned defaults. The service holds
    no secret, and the connection it opens can reach nothing outside the lake tree. The
    real clock and the real calendar are wired here and nowhere else in this module.

    A malformed config exits with code 2 and a one-line message. A port already in
    use does the same. The design puts this service under launchd ``KeepAlive``, where
    an uncaught traceback becomes a restart loop instead of a readable complaint.
    """
    args = build_parser().parse_args(argv)
    if args.lake_root is not None:
        lake_root = Path(args.lake_root)
        guards = GuardConstants()
    else:
        with input_errors_exit("dashboard"):
            config = load_config(args.config)
        lake_root = config.lake_root
        guards = config.guards
    service = DashboardService(
        lake_root, clock=SystemClock(), calendar=ExchangeCalendar(), guards=guards
    )
    try:
        server = make_server(service, args.port)
    except OSError as exc:
        print(
            f"marketlake dashboard: cannot bind port {args.port} ({exc.strerror})",
            file=sys.stderr,
        )
        return 2
    print(f"marketlake dashboard: http://{BIND_HOST}:{server.server_address[1]}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


__all__ = [
    "ALLOWED_HOSTS",
    "BIND_HOST",
    "DEFAULT_PORT",
    "MAX_LOOKBACK_SESSIONS",
    "NAMED_QUERIES",
    "PANEL_SURFACES",
    "QUERY_MEMORY_LIMIT",
    "QUERY_THREADS",
    "ROUTES",
    "SLOT_VERDICT_GRACE",
    "STATUSES",
    "DashboardService",
    "NamedQuery",
    "QueryContext",
    "QueryParameterError",
    "SegmentHealth",
    "SlotAggregate",
    "build_parser",
    "host_allowed",
    "lake_roster",
    "load_favicon",
    "load_status_page",
    "main",
    "make_server",
    "open_lake_connection",
    "parse_date",
    "query_now",
    "query_today",
    "session_slots",
    "validate_parameters",
    "validate_ticker",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
