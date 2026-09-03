"""The read-only query service: the dashboard's Now and Today panels.

Failures push alerts. Progress needs a pull surface. This module is that surface. It is
a small read-only query service on localhost that answers a fixed set of named queries
over the lake, plus the one static page that renders them. ``status.html`` runs the
queries at view time. Nothing is pre-rendered and no summary state is kept. Freshness
reads off the data's own timestamps, so a dead capture shows as an old last cycle and a
dead service shows as a page that cannot load. Neither can be mistaken for the other.

**The fixed-query contract is a security invariant, not a convenience.** DuckDB SQL is
arbitrary local file read as the owning user, and the owning user can read the Schwab
token and the alerting secrets. So client-supplied SQL never crosses the boundary, and
the sandbox holds even if the query surface drifts. Five rules, each enforced in code
here.

1. The HTTP layer maps a request path to a query *name* in ``NAMED_QUERIES``. A path not
   in the map is a 404. No endpoint takes SQL, and no request field is ever treated as
   SQL text.
2. A request carries at most two parameters, a ticker and a date. Each is validated
   before any query runs. The ticker must be in the lake's own roster, the set of tickers
   present under ``lake_root``. The date must parse as strict ``YYYY-MM-DD``. A request
   that fails validation is a 400 and never touches the connection.
3. A validated value reaches SQL only as a DuckDB bind parameter, never interpolated
   into the statement text. Every statement text is a module constant.
4. The connection is a sandbox. ``open_lake_connection`` sets ``allowed_directories`` to
   exactly ``lake_root``, turns ``enable_external_access`` off, then locks the
   configuration. No later SQL can widen the allow-list or flip the sandbox back on.
5. The service binds to the loopback address only and rejects any request whose ``Host``
   header is not localhost with a 403, before doing anything else. That is the standard
   guard against DNS rebinding, the trick where a malicious page re-points its own domain
   at the loopback address to reach a local service through the owner's browser.

Read-only is by construction, with one honest caveat. The sandbox blocks every path
outside ``lake_root`` but does let DuckDB write inside it. So read-only rests on the
named queries, which are ``SELECT`` statements only, and on the connection never being
handed to anything else. A test asserts the lake tree is byte-identical after both
panels run.

Three terms recur, glossed at first use.

- A *surface* is one kind of measurement with its own pinned schema. The two panels read
  the two minute-cadence surfaces, ``chains`` and ``quotes``.
- A *slot* is one minute of the session, the ``snap_ts`` a capture cycle fires for. The
  Today strip has one cell per slot from the session open through the option close, so
  it is denominated by the calendar's session length. An early close renders as a short
  full day, never as a half-missing one.
- A *sealed partition* is the one Parquet file compaction writes for a ticker-day. Before
  compaction the day lives in journal segments, Arrow IPC files with one record batch per
  cycle. A query reads both, unioned by column name, so the panel is the same before and
  after the seal.

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
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
import pyarrow as pa

from lake import journal
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar, NotASession
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, load_config
from lake.paths import CHAINS, QUOTES, LakePaths
from lake.session import SessionBounds, SessionClock, SessionPhase

log = logging.getLogger(__name__)

# The loopback bind. It is a constant, not an option, so the service cannot be exposed
# by a flag. The port is the one configurable thing.
BIND_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

# The two ``Host`` names a request may carry, with an optional port. Anything else is a
# rebinding attempt or a misdirected request and gets a 403.
ALLOWED_HOSTS = frozenset({"localhost", BIND_HOST})

# The surfaces the two panels read: the minute-cadence capture surfaces.
PANEL_SURFACES = (CHAINS, QUOTES)

# The capture cadence. One slot per minute, matching the design's minutely loop.
SLOT = timedelta(minutes=1)

# The five slot statuses the Today strip reports.
STATUS_CAPTURED = "captured"  # a data cycle landed
STATUS_SUSPECT = "suspect"  # a data cycle landed, flagged for the battery to judge
STATUS_GAP = "gap"  # a gap row records the missed minute and its reason
STATUS_MISSING = "missing"  # a past slot with no row at all, not even a gap marker
STATUS_PENDING = "pending"  # a slot still in the future as of the injected clock
STATUSES = (STATUS_CAPTURED, STATUS_SUSPECT, STATUS_GAP, STATUS_MISSING, STATUS_PENDING)

# The static page, shipped inside the package so it works offline.
STATUS_PAGE = "status.html"

# A ticker as the lake's directory names carry it. The roster is read off directory
# names under ``lake_root``, and a name outside this shape is not a ticker.
_TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9.]{0,11}")

# A strict ``YYYY-MM-DD``. ``date.fromisoformat`` accepts looser forms, so the shape is
# checked first and the parser only decides whether the digits make a real date.
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

# The directory-name prefixes the lake layout uses.
_DATE_PREFIX = "date="
_TICKER_PREFIX = "ticker="
_SEGMENT_SUFFIX = ".arrows"


class QueryParameterError(ValueError):
    """A request parameter failed validation. Raised before any SQL runs.

    The message is a fixed phrase, never the offending value, so nothing a client sent
    is reflected back.
    """


# -- the sandboxed connection ------------------------------------------------


def open_lake_connection(lake_root: Path | str) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB sandbox over one lake root and return it.

    The connection is in-memory. Three settings make it a sandbox, applied in the one
    order DuckDB accepts.

    1. ``allowed_directories`` is set to exactly ``[lake_root]``. DuckDB refuses to
       change this list once external access is off, so it goes first. DuckDB also adds
       its own spill directory to the list by default, so ``temp_directory`` is cleared
       beforehand to keep the list at the one entry the design names.
    2. ``enable_external_access`` is turned off. File reads, extension loads, and
       attaches are refused everywhere except under the allowed directory.
    3. ``lock_configuration`` is turned on. No later ``SET`` can undo either setting.

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
    con.execute("SET lock_configuration = true")
    return con


# -- the roster and the parameter validators ---------------------------------


def _tickers_in(directory: Path) -> set[str]:
    """The ticker names under one ``ticker=...`` parent, filtered to the ticker shape."""
    if not directory.is_dir():
        return set()
    found: set[str] = set()
    for child in directory.iterdir():
        if child.is_dir() and child.name.startswith(_TICKER_PREFIX):
            name = child.name[len(_TICKER_PREFIX) :]
            if _TICKER_PATTERN.fullmatch(name):
                found.add(name)
    return found


def _date_dirs(journal_dir: Path) -> list[Path]:
    """Every ``date=...`` directory under the journal root, oldest first."""
    if not journal_dir.is_dir():
        return []
    return sorted(
        child
        for child in journal_dir.iterdir()
        if child.is_dir() and child.name.startswith(_DATE_PREFIX)
    )


def lake_roster(paths: LakePaths) -> dict[str, tuple[str, ...]]:
    """The tickers present in the lake, each with the panel surfaces it has data for.

    The design pins that the dashboard's ticker list comes from under ``lake_root`` and
    never from ``tickers.yaml``. Until the daemon journals the roster stamp, the list is
    read off the lake's own layout: the ``ticker=`` directories under each surface's
    partition tree and under each journal date. The result is sorted by ticker, and a
    ticker maps to the sorted surfaces it appears under. This is the allow-list a request
    ticker is validated against.
    """
    surfaces: dict[str, set[str]] = {}
    for surface in PANEL_SURFACES:
        present = _tickers_in(paths.root / surface)
        for date_dir in _date_dirs(paths.journal_dir):
            present |= _tickers_in(date_dir / f"surface={surface}")
        for ticker in present:
            surfaces.setdefault(ticker, set()).add(surface)
    return {ticker: tuple(sorted(surfaces[ticker])) for ticker in sorted(surfaces)}


def parse_date(text: str) -> date:
    """A strict ``YYYY-MM-DD`` as a ``date``. Anything else raises ``QueryParameterError``."""
    if not _DATE_PATTERN.fullmatch(text):
        raise QueryParameterError("malformed date; expected YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise QueryParameterError("malformed date; expected YYYY-MM-DD") from None


def validate_ticker(text: str, roster: Mapping[str, object]) -> str:
    """A ticker that is in the roster, unchanged. Anything else raises."""
    if text not in roster:
        raise QueryParameterError("unknown ticker")
    return text


# -- reading one ticker-day's rows -------------------------------------------

# The provenance columns the panels read, with their pinned types. Every surface schema
# carries them. A drifted segment missing one gets a null column so the union still binds.
_PROVENANCE_TYPES: dict[str, pa.DataType] = {
    "snap_ts": pa.string(),
    "row_kind": pa.string(),
    "error_class": pa.string(),
    "suspect": pa.bool_(),
}
_PROVENANCE_COLUMNS = tuple(_PROVENANCE_TYPES)
_PROVENANCE_SCHEMA = pa.schema(list(_PROVENANCE_TYPES.items()))

# The name the day's journal rows are registered under for the duration of one query.
_JOURNAL_VIEW = "journal_rows"

# The per-slot aggregate. ``snap_ts`` is an ISO string with an offset on every row the
# writers produce, so casting it to a zoned timestamp and taking epoch milliseconds gives
# one key per instant, whatever offset a row was written in. ``TRY_CAST`` turns an
# unparseable stamp into a null slot, dropped by the filter, rather than a failed panel.
# ``row_kind`` values are bound, not spelled, so the journal module stays their one home.
_SLOT_SELECT = """
SELECT slot_ms,
       count(*) FILTER (WHERE row_kind = $data_kind) AS data_rows,
       count(*) FILTER (WHERE row_kind = $gap_kind) AS gap_rows,
       bool_or(coalesce(suspect, false)) AS suspect,
       min(error_class) AS error_class
FROM (
    SELECT epoch_ms(TRY_CAST(snap_ts AS TIMESTAMPTZ)) AS slot_ms,
           row_kind, error_class, suspect
    FROM ({source})
)
WHERE slot_ms IS NOT NULL
GROUP BY slot_ms
ORDER BY slot_ms
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
class SlotAggregate:
    """What one slot's rows add up to, for one ticker and surface on one day."""

    slot_ms: int
    data_rows: int
    gap_rows: int
    suspect: bool
    error_class: str | None

    @property
    def status(self) -> str:
        """The slot's status from its own rows. A slot with rows is never missing."""
        if self.data_rows > 0:
            return STATUS_SUSPECT if self.suspect else STATUS_CAPTURED
        return STATUS_GAP


def _provenance_columns(table: pa.Table) -> pa.Table:
    """The provenance columns of one segment, in fixed order, missing ones nulled."""
    view = table.select([name for name in _PROVENANCE_COLUMNS if name in table.column_names])
    for name, kind in _PROVENANCE_TYPES.items():
        if name not in view.column_names:
            view = view.append_column(name, pa.nulls(table.num_rows, kind))
    return view.select(list(_PROVENANCE_COLUMNS))


def _load_journal_rows(segments: Sequence[Path]) -> tuple[pa.Table, int]:
    """The provenance rows of every readable segment, and how many were unreadable.

    A segment that cannot be read, a torn header, a shadow-append, a vanished file, is
    counted and skipped, so one bad file never blanks the panel. The count is reported so
    the skip is visible, never silent.
    """
    tables: list[pa.Table] = []
    unreadable = 0
    for path in segments:
        try:
            table = journal.read_segment(path)
        except (OSError, pa.ArrowInvalid, journal.ShadowAppendError):
            unreadable += 1
            continue
        tables.append(_provenance_columns(table))
    if not tables:
        return _PROVENANCE_SCHEMA.empty_table(), unreadable
    return pa.concat_tables(tables, promote_options="permissive"), unreadable


def _journal_segments(paths: LakePaths, surface: str, ticker: str, day: date) -> list[Path]:
    directory = paths.segment_dir(surface, ticker, day)
    if not directory.is_dir():
        return []
    return sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.name.endswith(_SEGMENT_SUFFIX)
    )


def _slot_aggregates(
    con: duckdb.DuckDBPyConnection, paths: LakePaths, surface: str, ticker: str, day: date
) -> tuple[list[SlotAggregate], int]:
    """Every slot with rows for one ticker, surface, and day, plus the unreadable count.

    The journal rows are registered as an Arrow view for the duration of the query. The
    sealed partition, when present, is read natively by DuckDB and unioned by name. The
    partition path is built from validated parts and bound as a parameter.
    """
    segments = _journal_segments(paths, surface, ticker, day)
    partition = paths.partition_path(surface, ticker, day)
    has_partition = partition.is_file()
    if not segments and not has_partition:
        return [], 0
    rows, unreadable = _load_journal_rows(segments)
    params: dict[str, object] = {
        "data_kind": journal.ROW_KIND_DATA,
        "gap_kind": journal.ROW_KIND_GAP,
    }
    con.register(_JOURNAL_VIEW, rows)
    try:
        if has_partition:
            params["partitions"] = [str(partition)]
            result = con.execute(_SLOT_SQL_JOURNAL_AND_PARTITION, params).fetchall()
        else:
            result = con.execute(_SLOT_SQL_JOURNAL, params).fetchall()
    finally:
        con.unregister(_JOURNAL_VIEW)
    return [SlotAggregate(*row) for row in result], unreadable


def _dates_desc(paths: LakePaths, surface: str, ticker: str) -> list[date]:
    """Every day the lake holds rows for one ticker and surface, newest first."""
    days: set[date] = set()
    for date_dir in _date_dirs(paths.journal_dir):
        if (date_dir / f"surface={surface}" / f"{_TICKER_PREFIX}{ticker}").is_dir():
            parsed = _parse_dir_date(date_dir.name)
            if parsed is not None:
                days.add(parsed)
    partition_dir = paths.root / surface / f"{_TICKER_PREFIX}{ticker}"
    if partition_dir.is_dir():
        for child in partition_dir.iterdir():
            if child.is_file() and child.suffix == ".parquet":
                parsed = _parse_dir_date(child.stem)
                if parsed is not None:
                    days.add(parsed)
    return sorted(days, reverse=True)


def _parse_dir_date(name: str) -> date | None:
    """The date in a ``date=YYYY-MM-DD`` directory or file stem, or ``None``."""
    if not name.startswith(_DATE_PREFIX):
        return None
    text = name[len(_DATE_PREFIX) :]
    if not _DATE_PATTERN.fullmatch(text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


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


def session_slots(bounds: SessionBounds) -> list[datetime]:
    """Every capture slot of a session: the open through the option close, one a minute.

    This is the strip's denominator, derived from the calendar's bounds for the day. A
    regular day yields about 405 slots and an early close fewer, with no literal here.
    """
    slots: list[datetime] = []
    slot = bounds.open
    while slot <= bounds.option_close:
        slots.append(slot)
        slot += SLOT
    return slots


# -- the named queries -------------------------------------------------------


@dataclass(frozen=True)
class QueryContext:
    """What every named query gets besides its validated parameters.

    ``now`` is the injected clock's instant, stamped by the service per request.
    ``session`` is the session clock over that same clock and the injected calendar.
    """

    paths: LakePaths
    now: datetime
    session: SessionClock


def query_now(con: duckdb.DuckDBPyConnection, ctx: QueryContext) -> dict[str, object]:
    """The Now panel: per ticker and surface, the last successful cycle and minutes-since.

    For each ticker and surface the lake holds, the query walks the ticker's days newest
    first and stops at the first day with a data cycle. So a ticker whose latest day is
    gap-only still reports its true last success, and the latest slot's own status and
    reason ride beside it. Minutes-since is ``now`` minus that slot, computed here from
    the injected instant.

    Two fields are null until their writers exist. The refresh-token age comes from the
    mint stamp the daemon will journal into segment metadata each cycle. Nothing writes
    that stamp yet, and the dashboard never reads the token file, so the age is null.
    The last dead-man ping is the watchdog deliverable's, also unbuilt, so it is null.
    """
    roster = lake_roster(ctx.paths)
    surfaces: list[dict[str, object]] = []
    for ticker, present in roster.items():
        for surface in present:
            surfaces.append(_latest_cycle(con, ctx, surface, ticker))
    phase = ctx.session.phase()
    return {
        "as_of": _iso(ctx.now),
        "session_date": ctx.session.session_date().isoformat(),
        "is_session": phase is not SessionPhase.NON_SESSION,
        "phase": phase.value,
        "stale_after_minutes": GuardConstants().watchdog_page_minutes,
        "tickers": list(roster),
        "surfaces": surfaces,
        "token_minted_at": None,
        "token_age_minutes": None,
        "dead_man_last_ping": None,
    }


def _latest_cycle(
    con: duckdb.DuckDBPyConnection, ctx: QueryContext, surface: str, ticker: str
) -> dict[str, object]:
    """One Now row: the latest slot of any kind and the latest data slot, with its age."""
    last_data_ms: int | None = None
    last_ms: int | None = None
    last_status: str | None = None
    last_error: str | None = None
    unreadable = 0
    for day in _dates_desc(ctx.paths, surface, ticker):
        aggregates, skipped = _slot_aggregates(con, ctx.paths, surface, ticker, day)
        unreadable += skipped
        if not aggregates:
            continue
        if last_ms is None:
            latest = aggregates[-1]
            last_ms, last_status, last_error = latest.slot_ms, latest.status, latest.error_class
        with_data = [agg for agg in aggregates if agg.data_rows > 0]
        if with_data:
            last_data_ms = with_data[-1].slot_ms
            break
    minutes_since: float | None = None
    if last_data_ms is not None:
        minutes_since = round((_slot_ms(ctx.now) - last_data_ms) / 60_000, 1)
    return {
        "ticker": ticker,
        "surface": surface,
        "last_data_snap_ts": None if last_data_ms is None else _iso_et(last_data_ms),
        "minutes_since": minutes_since,
        "last_snap_ts": None if last_ms is None else _iso_et(last_ms),
        "last_status": last_status,
        "last_error_class": last_error,
        "unreadable_segments": unreadable,
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
    strips, so a holiday renders as *no session*, never as zero percent.

    The slot list comes from the calendar through ``SessionClock.bounds``, the open
    through the option close. Each slot reports its status, its data row count, and the
    gap reason when one is present. A slot with data rows but a gap marker beside them,
    a partial chain snapshot, reads captured and still carries the marker's class. A slot
    in the future as of the injected clock is pending, never missing.
    """
    session_day = day if day is not None else ctx.session.session_date()
    roster = lake_roster(ctx.paths)
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
    slots = session_slots(bounds)
    tickers = [ticker] if ticker is not None else list(roster)
    strips: list[dict[str, object]] = []
    for symbol in tickers:
        for surface in roster.get(symbol, ()):
            aggregates, unreadable = _slot_aggregates(con, ctx.paths, surface, symbol, session_day)
            strips.append(_strip(symbol, surface, slots, aggregates, unreadable, ctx.now))
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


def _strip(
    ticker: str,
    surface: str,
    slots: Sequence[datetime],
    aggregates: Sequence[SlotAggregate],
    unreadable: int,
    now: datetime,
) -> dict[str, object]:
    """One strip: every session slot with its status, denominated by the slot list."""
    by_slot = {agg.slot_ms: agg for agg in aggregates}
    now_ms = _slot_ms(now)
    cells: list[dict[str, object]] = []
    counts = dict.fromkeys(STATUSES, 0)
    for slot in slots:
        key = _slot_ms(slot)
        agg = by_slot.get(key)
        if agg is None:
            status = STATUS_PENDING if key > now_ms else STATUS_MISSING
            rows, error_class = 0, None
        else:
            status, rows, error_class = agg.status, agg.data_rows, agg.error_class
        counts[status] += 1
        cells.append(
            {"slot": slot.isoformat(), "status": status, "rows": rows, "error_class": error_class}
        )
    return {
        "ticker": ticker,
        "surface": surface,
        "counts": counts,
        "unreadable_segments": unreadable,
        "slots": cells,
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
}

# The route table: request path to query name. A path not here is a 404.
ROUTES: Mapping[str, str] = {"/api/now": "now", "/api/today": "today"}


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


class DashboardService:
    """The query service: one sandboxed connection, the injected seams, and the page.

    The connection is opened once at construction and is the only one the service ever
    holds. Each request runs on a cursor over it, so requests never share statement
    state, and every cursor inherits the locked sandbox. The clock and calendar are
    injected, so a test decides what time it is and which days are sessions.
    """

    def __init__(
        self,
        lake_root: Path | str,
        *,
        clock: Clock,
        calendar: Calendar,
        connection: duckdb.DuckDBPyConnection | None = None,
        page: bytes | None = None,
    ) -> None:
        self._paths = LakePaths(Path(lake_root).resolve())
        self._clock = clock
        self._calendar = calendar
        self._con = connection if connection is not None else open_lake_connection(self._paths.root)
        self._page = page if page is not None else load_status_page()

    @property
    def page(self) -> bytes:
        return self._page

    def roster(self) -> dict[str, tuple[str, ...]]:
        """The lake's current roster, re-read per call so a new ticker appears at once."""
        return lake_roster(self._paths)

    def run_query(self, name: str, raw: Mapping[str, str]) -> dict[str, object]:
        """Run one named query with raw request fields. Validation comes first.

        Raises ``KeyError`` for a name outside the registry and
        ``QueryParameterError`` for a bad field, both before the connection is touched.
        """
        query = NAMED_QUERIES[name]
        params = validate_parameters(query, raw, self.roster())
        ctx = QueryContext(
            paths=self._paths,
            now=self._clock.now(),
            session=SessionClock(self._clock, self._calendar),
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
        # anything else, whatever the verb, and a non-GET verb gets its 405.
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
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; connect-src 'self'; script-src 'unsafe-inline'; "
            "style-src 'unsafe-inline'",
        )
        self.end_headers()
        self.wfile.write(body)

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
    """The command-line contract. The port is the one knob; the bind is fixed."""
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
        "startup, for lake_root alone.",
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
    ``lake_root`` is kept. The service holds no secret, and the connection it opens can
    reach nothing outside the lake tree. The real clock and the real calendar are wired
    here and nowhere else in this module.
    """
    args = build_parser().parse_args(argv)
    if args.lake_root is not None:
        lake_root = Path(args.lake_root)
    else:
        lake_root = load_config(args.config).lake_root
    service = DashboardService(lake_root, clock=SystemClock(), calendar=ExchangeCalendar())
    server = make_server(service, args.port)
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
    "NAMED_QUERIES",
    "PANEL_SURFACES",
    "ROUTES",
    "STATUSES",
    "DashboardService",
    "NamedQuery",
    "QueryContext",
    "QueryParameterError",
    "SlotAggregate",
    "build_parser",
    "host_allowed",
    "lake_roster",
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
