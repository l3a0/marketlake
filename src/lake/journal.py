"""The journal segment writer.

The capture loop cannot write one Parquet file per fetch. That would leave a lake
full of tiny files. Instead each cycle appends to a *journal segment*: an on-disk
file that a whole day of one ticker's cycles is written into, one record batch at a
time. A record batch is Apache Arrow's self-contained unit of rows. The segment
format is Arrow IPC, Arrow's append-friendly on-disk stream. A file torn mid-write
therefore stays readable up to its last complete batch. Parquet, by contrast, is
invalid until its footer lands at close.

Three terms recur, defined here at first use.

- A *surface* is one kind of measurement with its own pinned schema. This module
  writes two: ``chains`` (full option chains) and ``quotes`` (batched equity
  quotes). Arrow IPC fixes one schema per file, so the surface axis is load-bearing.
  One segment can never hold both.
- A *segment* is one Arrow IPC file, created by exactly one writer session and never
  re-opened for append. An Arrow IPC stream cannot be resumed by a later writer. A
  clean close writes an *end-of-stream* marker, the EOS, that readers stop at. Rows
  appended past an EOS are silently invisible to standard readers, so this module
  refuses to create such a shadow-append and fails loudly when it reads one.
- A *snap_ts* is the minute slot a cycle fired for, assigned by the loop at the top
  of the minute. It is neither the fetch time nor the vendor quote time. Every row
  carries all three.

This module owns three jobs: the pinned capture schema for each surface, the row
builders that turn a vendor payload into a record batch, and the writer that lands
batches durably. It never reads the wall clock. Every timestamp arrives as an
argument, stamped by the caller from the injected clock.
"""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa

# The schema version stamped on every row. The vendor columns' full list is fixed by
# the first day's payload and recorded as version 1. A later payload change mints a
# new version rather than mutating this one.
SCHEMA_VERSION = 1

# The two surfaces this module writes.
CHAINS_SURFACE = "chains"
QUOTES_SURFACE = "quotes"

# The two kinds of row. A ``data`` row carries a vendor observation. A ``gap`` row is
# the surface schema with all vendor columns null. It records a minute that was
# missed and why, so completeness is counted from rows and never inferred from holes.
ROW_KIND_DATA = "data"
ROW_KIND_GAP = "gap"

# The durability primitive. A cycle is durable only once its bytes reach stable
# storage, past the drive's own write cache. On macOS that needs
# ``fcntl(fd, F_FULLFSYNC)``. Plain ``fsync`` stops at the drive cache there, per
# Apple's ``fsync(2)`` man page. On Linux ``fsync`` already flushes the device cache
# on mainstream filesystems, so ``os.fsync`` is the equivalent, not a weaker
# stand-in. ``F_FULLFSYNC`` is macOS-specific only because Apple made ``fsync``
# weaker than POSIX. The daemon runs on the macOS laptop today. A future Linux host
# keeps real durability through the fallback.
F_FULLFSYNC = getattr(fcntl, "F_FULLFSYNC", None)


# -- pinned capture schemas --------------------------------------------------

# The provenance columns every row carries, in every surface. ``row_kind`` is
# ``data`` or ``gap``. ``error_class`` is null on data rows and names the reason on a
# gap. ``suspect`` flags a response the validation battery should judge. ``close_tag``
# is ``canonical``, ``spot_close``, or null, stamped on every row of a tagged cycle.
# ``session_phase`` tags rows observed after the equity close. ``extra`` is a
# normally-empty JSON overflow column. Any vendor field the schema does not name lands
# there, so vendor-verbatim stays structurally true even when a payload drifts.
_PROVENANCE_FIELDS = [
    ("row_kind", pa.string()),
    ("error_class", pa.string()),
    ("suspect", pa.bool_()),
    ("close_tag", pa.string()),
    ("session_phase", pa.string()),
    ("schema_version", pa.int64()),
    ("extra", pa.string()),
]

# The timestamps plus the ticker every row carries. ``snap_ts`` is the minute slot.
# ``fetch_ts`` and ``fetch_end_ts`` are a pair around the vendor call. ``fetch_ts`` is
# the dispatch time, the loop's clock just before the request starts. ``fetch_end_ts``
# is when the response or the failure landed, the request end. So the request
# round-trip is ``fetch_end_ts`` minus ``fetch_ts``, measurable per row, and even a
# timeout's duration is captured. ``vendor_quote_ts`` is Schwab's own quote time, and
# staleness is ``fetch_ts`` minus it. ``fetch_end_ts`` is nullable, so a row without it
# is still valid.
_STAMP_FIELDS = [
    ("snap_ts", pa.string()),
    ("fetch_ts", pa.string()),
    ("fetch_end_ts", pa.string()),
    ("vendor_quote_ts", pa.string()),
    ("ticker", pa.string()),
]

# The chains capture schema. Besides the stamps and provenance, each row is one
# contract. The per-contract vendor columns are fixed by the first day's payload:
# the OCC symbol, the call/put flag, bid/ask/last, open interest, the day's traded
# volume, the vendor's implied volatility, and the five greeks. ``volume`` is Schwab's
# ``totalVolume``. It is a typed column, not overflow, because the OI view's comparable
# set ranks contracts on volume, so it must be queryable. The chain-level fields are
# repeated on every contract row, per the raw-verbatim rule: ``interest_rate``,
# ``underlying_price``, ``dividend_yield``, and ``is_delayed``. ``is_delayed`` is the
# vendor's real-time entitlement flag. It must be false on a real-time chain response.
# The validation battery checks it, so it is captured, not dropped. Raw stores what the
# vendor said. Nothing here is reshaped or validated.
CHAINS_SCHEMA = pa.schema(
    _STAMP_FIELDS
    + [
        ("occ_symbol", pa.string()),
        ("put_call", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("open_interest", pa.int64()),
        ("volume", pa.int64()),
        ("volatility", pa.float64()),
        ("delta", pa.float64()),
        ("gamma", pa.float64()),
        ("theta", pa.float64()),
        ("vega", pa.float64()),
        ("rho", pa.float64()),
        ("interest_rate", pa.float64()),
        ("underlying_price", pa.float64()),
        ("dividend_yield", pa.float64()),
        ("is_delayed", pa.bool_()),
    ]
    + _PROVENANCE_FIELDS
)

# The quotes capture schema. Each row is one equity quote for one ticker: bid, ask,
# last, ``realtime``, ``cusip``, and the dividend fundamentals. ``realtime`` is the
# vendor's real-time entitlement flag. It must be true on a real-time quote. The
# validation battery checks it, so it is a captured vendor column, not dropped. ``cusip``
# is Schwab's CUSIP for the equity, kept raw under the vendor-verbatim rule. It is never
# a join key and never published, but it is captured so a later enrichment can resolve
# the instrument's FIGI from it. The seven ``div_*`` columns are Schwab's dividend
# fundamentals, captured as typed columns because the deferred greeks layer's dividend
# input reads them at valuation time. ``div_pay_amount`` is the per-event amount, never
# ``div_amount``, which is the annualized trailing figure. The ``next_div_*`` fields are
# the vendor's undocumented projections, kept raw for the declaration-vs-projection
# study. The vendor quote time is carried in ``vendor_quote_ts``, not repeated as a
# vendor column.
QUOTES_SCHEMA = pa.schema(
    _STAMP_FIELDS
    + [
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("realtime", pa.bool_()),
        ("cusip", pa.string()),
        ("div_pay_amount", pa.float64()),
        ("div_ex_date", pa.string()),
        ("div_amount", pa.float64()),
        ("div_freq", pa.int64()),
        ("declaration_date", pa.string()),
        ("next_div_ex_date", pa.string()),
        ("next_div_pay_date", pa.string()),
    ]
    + _PROVENANCE_FIELDS
)

_SCHEMAS = {CHAINS_SURFACE: CHAINS_SCHEMA, QUOTES_SURFACE: QUOTES_SCHEMA}


def schema_for(surface: str) -> pa.Schema:
    """The pinned capture schema for a surface. Unknown surfaces raise loudly."""
    try:
        return _SCHEMAS[surface]
    except KeyError:
        raise ValueError(f"unknown surface {surface!r}") from None


# -- vendor field maps -------------------------------------------------------

# The per-contract vendor fields that land in typed chains columns. The vendor speaks
# camelCase. The pinned schema names are snake_case. Only the names change. The values
# are stored verbatim.
_CHAINS_CONTRACT_MAP = {
    "symbol": "occ_symbol",
    "putCall": "put_call",
    "bid": "bid",
    "ask": "ask",
    "last": "last",
    "openInterest": "open_interest",
    "totalVolume": "volume",
    "volatility": "volatility",
    "delta": "delta",
    "gamma": "gamma",
    "theta": "theta",
    "vega": "vega",
    "rho": "rho",
}

# The chain-level fields promoted to columns on every contract row. The last is the
# real-time entitlement flag, recognized here so it is captured, not dropped, and
# never mistaken for an unknown field.
_CHAINS_HEADER_MAP = {
    "interestRate": "interest_rate",
    "underlyingPrice": "underlying_price",
    "dividendYield": "dividend_yield",
    "isDelayed": "is_delayed",
}

# The quote fields that land in typed quotes columns. ``realtime`` and ``cusip`` are
# envelope-level fields, and the seven dividend fields come from the envelope's
# ``fundamental`` block. The sampler merges all of them into the flat quote before this
# map runs, so each is captured in its own column and kept out of ``extra``. Only these
# specific dividend fields are lifted from ``fundamental``. The rest of that block, like
# ``peRatio``, is deliberately left behind so ``extra`` stays empty and the populated-
# extra report flag keeps its meaning. The exact Schwab field names are confirmed by the
# live cassette recording; the design's names are mapped here for now.
_QUOTE_MAP = {
    "bidPrice": "bid",
    "askPrice": "ask",
    "lastPrice": "last",
    "realtime": "realtime",
    "cusip": "cusip",
    "divPayAmount": "div_pay_amount",
    "divExDate": "div_ex_date",
    "divAmount": "div_amount",
    "divFreq": "div_freq",
    "declarationDate": "declaration_date",
    "nextDivExDate": "next_div_ex_date",
    "nextDivPayDate": "next_div_pay_date",
}

# Quote fields the parser recognizes but does not overflow into ``extra``. The vendor
# quote time is carried in ``vendor_quote_ts`` instead, so keeping it out of ``extra``
# keeps that column normally empty.
_QUOTE_CONSUMED = frozenset({"quoteTime"})


# -- row building ------------------------------------------------------------


def _iso(value: str | datetime | date | None) -> str | None:
    """Normalize a timestamp argument to an ISO-8601 string, or ``None``.

    A string passes through. A ``datetime`` or ``date`` is formatted. The caller
    stamps these from the injected clock, so this module never reads the wall clock.
    """
    if value is None or isinstance(value, str):
        return value
    return value.isoformat()


def _extra_json(fields: Mapping[str, object], known: set[str]) -> str | None:
    """JSON for the vendor fields the schema does not name, or ``None`` when empty.

    Keys are sorted so the same overflow always serializes identically.
    """
    overflow = {key: value for key, value in fields.items() if key not in known}
    if not overflow:
        return None
    return json.dumps(overflow, sort_keys=True)


def _batch(schema: pa.Schema, rows: Sequence[Mapping[str, object]]) -> pa.RecordBatch:
    """Build one record batch from row mappings, typed by the schema.

    A missing key becomes null. Each column is built with its schema type, so an
    all-null column still lands with the right type instead of guessing.
    """
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _iter_contracts(body: Mapping[str, object]) -> list[Mapping[str, object]]:
    """Every contract dict in a chain body, calls first then puts, in payload order.

    The chain body nests contracts as ``expDateMap[expiration][strike] -> [contract]``.
    This walks both the call and put maps and flattens them.
    """
    contracts: list[Mapping[str, object]] = []
    for map_key in ("callExpDateMap", "putExpDateMap"):
        exp_map = body.get(map_key) or {}
        for strikes in exp_map.values():
            for contract_list in strikes.values():
                contracts.extend(contract_list)
    return contracts


def chains_data_batch(
    body: Mapping[str, object],
    *,
    ticker: str,
    snap_ts: str | datetime,
    fetch_ts: str | datetime,
    fetch_end_ts: str | datetime | None = None,
    vendor_quote_ts: str | datetime | None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build a chains data batch from one chain response body.

    One row per contract. Known contract fields land in typed columns. The chain-level
    fields repeat on every row: the three header fields and the ``is_delayed``
    entitlement flag. Any unrecognized contract field lands in ``extra`` as JSON. The
    parser fails open, so a new vendor field never drops a cycle. The timestamps are
    the caller's, stamped from the injected clock. ``fetch_end_ts`` is when the response
    landed, the request end, so the round-trip is ``fetch_end_ts`` minus ``fetch_ts``.
    """
    header = {column: body.get(vendor) for vendor, column in _CHAINS_HEADER_MAP.items()}
    stamps = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "vendor_quote_ts": _iso(vendor_quote_ts),
        "ticker": ticker,
        "row_kind": ROW_KIND_DATA,
        "error_class": None,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
    }
    rows: list[dict[str, object]] = []
    for contract in _iter_contracts(body):
        row: dict[str, object] = dict(stamps)
        row.update(header)
        for vendor, column in _CHAINS_CONTRACT_MAP.items():
            if vendor in contract:
                row[column] = contract[vendor]
        row["extra"] = _extra_json(contract, set(_CHAINS_CONTRACT_MAP))
        rows.append(row)
    return _batch(CHAINS_SCHEMA, rows)


def quotes_data_batch(
    quote: Mapping[str, object],
    *,
    ticker: str,
    snap_ts: str | datetime,
    fetch_ts: str | datetime,
    fetch_end_ts: str | datetime | None = None,
    vendor_quote_ts: str | datetime | None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build a one-row quotes batch from one ticker's quote object.

    The batched quote sampler splits the vendor's response per ticker and hands this
    builder that ticker's quote fields: bid, ask, last, the vendor quote time, the
    ``realtime`` entitlement flag, the ``cusip``, and the seven dividend fundamentals the
    sampler lifted from the envelope's ``fundamental`` block. Every recognized field
    lands in its typed column. The quote time is carried in ``vendor_quote_ts``. Anything
    the map does not name lands in ``extra``, so the non-dividend fundamental fields the
    sampler left behind never reach here and ``extra`` stays empty. ``fetch_end_ts`` is
    the request-end stamp, the pair to ``fetch_ts`` for round-trip.
    """
    row: dict[str, object] = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "vendor_quote_ts": _iso(vendor_quote_ts),
        "ticker": ticker,
        "row_kind": ROW_KIND_DATA,
        "error_class": None,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
    }
    for vendor, column in _QUOTE_MAP.items():
        if vendor in quote:
            row[column] = quote[vendor]
    row["extra"] = _extra_json(quote, set(_QUOTE_MAP) | _QUOTE_CONSUMED)
    return _batch(QUOTES_SCHEMA, [row])


def gap_batch(
    surface: str,
    *,
    ticker: str,
    snap_ts: str | datetime,
    error_class: str,
    fetch_ts: str | datetime | None = None,
    fetch_end_ts: str | datetime | None = None,
    suspect: bool = False,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> pa.RecordBatch:
    """Build a one-row gap batch for a missed minute.

    A gap row is the surface schema with every vendor column null. It carries its
    missed minute in ``snap_ts`` and the reason in ``error_class``. It holds no market
    data. The missed sample is gone forever. The row records the absence so a hole is
    never inferred. ``fetch_ts`` is optional, since a gap for a dead daemon has no
    fetch at all. ``fetch_end_ts`` is optional too, and set for a failed fetch so the
    round-trip to the failure is measurable. There is no vendor quote, so
    ``vendor_quote_ts`` is always null. Every vendor column is null on a gap by
    construction: the row sets only the stamps and provenance below, and the batch
    builder fills each schema column the row omits with null. So the prices, greeks,
    ``open_interest``, ``volume``, ``realtime``, ``cusip``, and the dividend
    fundamentals are all null, per surface, without being enumerated here.
    """
    schema = schema_for(surface)
    row: dict[str, object] = {
        "snap_ts": _iso(snap_ts),
        "fetch_ts": _iso(fetch_ts),
        "fetch_end_ts": _iso(fetch_end_ts),
        "vendor_quote_ts": None,
        "ticker": ticker,
        "row_kind": ROW_KIND_GAP,
        "error_class": error_class,
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": session_phase,
        "schema_version": SCHEMA_VERSION,
        "extra": None,
    }
    return _batch(schema, [row])


# -- segment paths -----------------------------------------------------------


def _day_str(day: date | str) -> str:
    return day.isoformat() if isinstance(day, date) else str(day)


def segment_path(
    lake_root: Path | str,
    surface: str,
    ticker: str,
    day: date | str,
    start_ts: str,
    pid: int,
) -> Path:
    """The on-disk path for a segment. It mirrors the fixture-lake convention exactly.

    The path is ``journal/date=D/surface=S/ticker=T/seg-<start_ts>-<pid>.arrows``.
    The ``start_ts`` is a second-or-finer stamp of when the writer session began. The
    ``pid`` is the writer process id. Together they make the name unique per writer
    session, so a manual re-run or a crash-loop restart cannot collide with a live
    segment by accident.
    """
    return (
        Path(lake_root)
        / "journal"
        / f"date={_day_str(day)}"
        / f"surface={surface}"
        / f"ticker={ticker}"
        / f"seg-{start_ts}-{pid}.arrows"
    )


# -- the writer --------------------------------------------------------------


class SegmentWriter:
    """A one-session writer for a single Arrow IPC segment.

    It creates the file exclusively, appends one record batch per cycle, and makes
    each cycle durable before returning. A clean close writes the end-of-stream
    marker. The segment is never re-opened for append. Use it as a context manager so
    the marker always lands.
    """

    def __init__(self, path: Path | str, schema: pa.Schema, *, surface: str | None = None) -> None:
        self.path = Path(path)
        self.schema = schema
        self.surface = surface
        # Count the durability flushes. Each cycle adds one, and a clean close adds
        # one more for the end-of-stream marker. Tests read this to prove the
        # per-cycle durability contract holds.
        self.durable_syncs = 0
        self._closed = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # ``O_CREAT | O_EXCL`` makes the create fail loudly if the path already exists.
        # A collision must never truncate durable rows or shadow-append past an EOS.
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        self._fd = fd
        # Make the new segment's directory entry durable, so the file itself survives
        # a crash right after creation. This is the standard directory fsync. It uses
        # plain ``os.fsync`` even on macOS, where ``F_FULLFSYNC`` does not apply to a
        # directory, matching how SQLite and Postgres persist directory entries.
        dir_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        # Unbuffered, so every write reaches the OS before the durability flush.
        self._file = open(fd, "wb", buffering=0, closefd=True)
        self._sink = pa.PythonFile(self._file, mode="w")
        self._writer = pa.ipc.new_stream(self._sink, schema)

    @classmethod
    def open(
        cls,
        lake_root: Path | str,
        surface: str,
        ticker: str,
        day: date | str,
        start_ts: str,
        pid: int,
    ) -> SegmentWriter:
        """Open a fresh segment from a lake root and the segment's coordinates.

        The path comes from ``segment_path`` and the schema from the surface. The
        ``pid`` is the caller's, typically ``os.getpid()``.
        """
        return cls(
            segment_path(lake_root, surface, ticker, day, start_ts, pid),
            schema_for(surface),
            surface=surface,
        )

    def write_cycle(self, batch: pa.RecordBatch | pa.Table) -> None:
        """Append one cycle's batch and make it durable.

        The batch's schema must match the segment's, which Arrow enforces. So a quotes
        batch can never land in a chains segment. Durability is the design's success
        point. A cycle counts as captured only after this returns.
        """
        if self._closed:
            raise ValueError("cannot write to a closed segment")
        if isinstance(batch, pa.Table):
            self._writer.write_table(batch)
        else:
            self._writer.write_batch(batch)
        self._flush_durable()

    def _flush_durable(self) -> None:
        self._sink.flush()
        if F_FULLFSYNC is not None:
            fcntl.fcntl(self._fd, F_FULLFSYNC)
        else:  # pragma: no cover - non-macOS path, exercised on Linux CI, not the dev Mac
            os.fsync(self._fd)
        self.durable_syncs += 1

    def close(self) -> None:
        """Write the end-of-stream marker, make it durable, and close the file."""
        if self._closed:
            return
        self._writer.close()  # writes the EOS marker
        self._flush_durable()
        self._file.close()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> SegmentWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# -- the reader --------------------------------------------------------------


class ShadowAppendError(Exception):
    """Raised when bytes follow a segment's end-of-stream marker.

    That is the shadow-append signature. A later writer opened a closed segment and
    appended past the EOS. Standard readers stop at the EOS and never see those rows,
    so the write silently loses data. The reader refuses to bless such a file.
    """

    def __init__(self, path: Path, consumed: int, size: int) -> None:
        self.path = path
        self.consumed = consumed
        self.size = size
        super().__init__(
            f"{path}: {size - consumed} bytes follow the end-of-stream marker (shadow-append)"
        )


def read_segment(path: Path | str) -> pa.Table:
    """Read a segment to its last complete record batch.

    Three cases, matching the design's durability model.

    1. A cleanly closed segment reads fully, up to its end-of-stream marker.
    2. A torn tail, from a power loss mid-append, reads up to the last complete batch.
       The incomplete trailing bytes are dropped, not an error. Every durable cycle
       ends in a full flush, so the last complete batch is exactly what the writer
       believed it had.
    3. Bytes after the end-of-stream marker are a shadow-append and raise loudly.
    """
    path = Path(path)
    with pa.memory_map(str(path), "rb") as source:
        reader = pa.ipc.open_stream(source)
        schema = reader.schema
        batches: list[pa.RecordBatch] = []
        clean_eos = False
        while True:
            try:
                batches.append(reader.read_next_batch())
            except StopIteration:
                clean_eos = True
                break
            except (pa.ArrowInvalid, OSError):
                # A torn tail: the next message is incomplete. Stop at the last
                # complete batch and keep what is durable.
                break
        if clean_eos and source.tell() < source.size():
            raise ShadowAppendError(path, source.tell(), source.size())
    return pa.Table.from_batches(batches, schema=schema)
