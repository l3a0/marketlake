"""The capture primitive: one cycle.

A capture cycle is the smallest unit of capture. It fetches every option chain and
one batched equity quote, journals what the vendor sent, and records each journal
segment in the manifest. This module builds that one cycle and nothing more. The
daemon that will call it in a loop is a later deliverable. So is any market-hours,
calendar, or session logic. Here the cycle runs once and returns.

Three terms recur, defined at first use.

- A *snap_ts* is the minute slot the cycle fired for. The loop assigns it once at the
  top of the cycle by flooring the current instant to the minute. It is neither the
  fetch time nor the vendor quote time. Every row carries all three.
- A *segment* is one Arrow IPC journal file, written by exactly one writer session and
  never re-opened. This cycle is one writer session. It opens one fresh segment per
  surface and ticker, writes one record batch, and closes it. Close writes the
  end-of-stream marker that makes the segment durable and final. The segment writer
  lives in ``lake.journal``.
- A *gap* is a row that records a missed sample and its reason. A failure never
  crashes the cycle and never discards data the vendor did send. A failure resolves
  into a gap row carrying an ``error_class``, journaled beside the data like anything
  else. Completeness is then counted from rows, never inferred from holes.

Two isolation rules from the design shape the failure handling.

1. *Skip-not-block, per ticker.* One chain ticker's failure never blocks another. Each
   options ticker is fetched, planned, and written on its own. A failure gaps that one
   ticker and the cycle moves on.
2. *The quote sampler is one shared failure unit.* The batched quote request covers
   every roster ticker at once. So a failed batch gaps every ticker's quote minute, one
   gap row each, never a single shared gap.

The whole cycle is dependency-injected. It takes a ``Clock``, a ``Vendor``, the roster,
and a ``lake_root``. It reads no wall clock and names no session time. The thin
production entry ``run_cycle_from_config`` wires the real config, roster, and
Schwab-backed vendor around the same core, and keeps the ``schwab-py`` construction
lazy so the offline test suite never touches the network.

The manifest step is the one place this cycle takes the lake-root lock. After every
segment is durable, the cycle appends one manifest entry per segment, keyed by the
segment path, under ``lake_lock``. That is the slice-1 segment-keyed entry the manifest
protocol sanctions. It exists only in this single-process phase. When the daemon lands,
manifest appends move into the serialized compaction job. Capture writes segments
outside the lock, because blocking a perishable cycle behind a daily job would drop
minutes.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from lake import journal
from lake.clock import Clock, SystemClock
from lake.config import load_config
from lake.lock import lake_lock
from lake.manifest import record_partition
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor
from lake.tickers import Roster, load_tickers
from lake.vendor import Vendor, VendorError

# The manifest ``source`` for a capture-written segment entry.
CAPTURE_SOURCE = "capture"

# The two surfaces this cycle writes, named through the journal so the string lives in
# exactly one place.
CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# The writer-session stamp in a segment name. It is generated from the cycle's own
# instant, never a hardcoded time, so the clock-seam scanner stays satisfied. Microsecond
# precision keeps two fast back-to-back runs in the same process from colliding.
_SEGMENT_STAMP_FORMAT = "%Y%m%dT%H%M%S%f"

# Turns a CamelCase exception name into a snake_case error class, so a fetch failure
# reads like the design's other classes (``daemon_dead``, ``quote_sampler_dead``). A
# ``VendorError`` becomes ``vendor_error``.
_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _snake_case(name: str) -> str:
    """A CamelCase name as snake_case."""
    return _CAMEL_BOUNDARY.sub("_", name).lower()


def _error_class(exc: BaseException) -> str:
    """The gap ``error_class`` for a raised failure: the exception type, snake-cased."""
    return _snake_case(type(exc).__name__)


def _ok(status: int) -> bool:
    """Whether an HTTP status is a success. A non-2xx is a fetch failure."""
    return 200 <= status < 300


def _epoch_ms_to_datetime(value: object) -> datetime | None:
    """A vendor epoch-millisecond timestamp as a UTC datetime, or ``None``.

    Schwab stamps its quote times as epoch milliseconds. Converting a stored epoch to a
    datetime is deterministic and reads no wall clock, the same move ``lake.schwab``
    makes for the token mint time. A missing value returns ``None``.
    """
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def _chain_vendor_quote_ts(body: Mapping[str, object]) -> datetime | None:
    """The chain response's vendor quote time, from the underlying quote, or ``None``.

    A Schwab option-chain response carries the underlying's quote in an ``underlying``
    block when the underlying quote is requested. Its ``quoteTime`` is the chain-level
    vendor quote time. Absent that block, the chain carries no single quote time and the
    stamp is null.
    """
    underlying = body.get("underlying")
    if isinstance(underlying, Mapping):
        return _epoch_ms_to_datetime(underlying.get("quoteTime"))
    return None


def _flatten_quote(body: Mapping[str, object], ticker: str) -> dict | None:
    """The per-ticker flattened quote mapping from a batched quotes body, or ``None``.

    The batched response maps each ticker to an envelope. The bid, ask, and last live in
    the envelope's ``quote`` block, and the real-time entitlement flag lives on the
    envelope. This merges the flag into the quote fields, so the row builder takes one
    flat mapping. A ticker absent from a 200 batch yields ``None``, the caller's gap
    signal. This is the one place that split logic lives, shared by the loop and by the
    onboarding snapshot, so both flatten a batched quote identically.
    """
    envelope = body.get(ticker)
    quote_fields = envelope.get("quote") if isinstance(envelope, Mapping) else None
    if not isinstance(quote_fields, Mapping):
        return None
    flat = dict(quote_fields)
    if isinstance(envelope, Mapping) and "realtime" in envelope:
        flat["realtime"] = envelope["realtime"]
    return flat


def _build_snapshot_batch(
    surface: str,
    ticker: str,
    body: Mapping[str, object],
    *,
    snap_ts: datetime,
    fetch_ts: datetime,
    fetch_end_ts: datetime,
) -> object:
    """Build one surface's data batch from a vendor response, the loop's own way.

    This is the row-building step of a capture cycle, factored so another caller can
    build a batch from a response it already fetched. A chains body maps straight
    through the D4 chains builder. A batched quotes body is split to the one ticker
    first. The result is byte-for-byte what the loop would build for the same response.
    """
    if surface == CHAINS:
        return journal.chains_data_batch(
            body,
            ticker=ticker,
            snap_ts=snap_ts,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
            vendor_quote_ts=_chain_vendor_quote_ts(body),
        )
    if surface == QUOTES:
        flat = _flatten_quote(body, ticker)
        if flat is None:
            raise VendorError(f"quotes response has no quote for {ticker!r}")
        return journal.quotes_data_batch(
            flat,
            ticker=ticker,
            snap_ts=snap_ts,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
            vendor_quote_ts=_epoch_ms_to_datetime(flat.get("quoteTime")),
        )
    raise ValueError(f"unknown surface {surface!r}")


@dataclass(frozen=True)
class SegmentOutcome:
    """One journal segment this cycle wrote, and how it turned out.

    ``partition`` is the segment's lake-relative path, the key its manifest entry uses.
    ``row_kind`` is ``data`` or ``gap``. ``error_class`` names the failure on a gap and
    is ``None`` on data. ``rows`` is the batch's row count, the same count recorded in
    the manifest.
    """

    surface: str
    ticker: str
    path: Path
    partition: str
    row_kind: str
    rows: int
    error_class: str | None
    fetched_at: str | None


@dataclass(frozen=True)
class SegmentError:
    """A ticker whose segment could not be journaled at all.

    A planned batch is always either data or a gap, so this is reserved for a genuine
    write failure, like the disk refusing the segment. It is not a captured gap. It
    carries no manifest entry, because no durable segment exists to point at.
    """

    surface: str
    ticker: str
    error_class: str


@dataclass(frozen=True)
class CycleResult:
    """What one capture cycle produced.

    ``segments`` is every durable segment written, data and gap alike. ``errors`` is the
    normally-empty set of tickers that could not be journaled. ``snap_ts`` is the minute
    slot the whole cycle fired for.
    """

    snap_ts: datetime
    segments: tuple[SegmentOutcome, ...]
    errors: tuple[SegmentError, ...] = ()

    @property
    def partitions(self) -> tuple[str, ...]:
        """Every segment's manifest key, in write order."""
        return tuple(seg.partition for seg in self.segments)

    def segment(self, surface: str, ticker: str) -> SegmentOutcome:
        """The outcome for one surface and ticker. Raises ``KeyError`` if absent."""
        for seg in self.segments:
            if seg.surface == surface and seg.ticker == ticker:
                return seg
        raise KeyError((surface, ticker))


@dataclass(frozen=True)
class _Plan:
    """A batch ready to write, plus how to describe its segment.

    Planning is the fallible half of a segment: the fetch, the status check, and the
    row build. It always resolves to a batch, data or gap, and never raises. Writing is
    the durable half, kept separate so the fetch can fail open to a gap while a true
    write failure still surfaces.
    """

    batch: object
    row_kind: str
    error_class: str | None
    fetch_ts: datetime
    fetch_end_ts: datetime


@dataclass
class _CaptureCycle:
    """One run of the primitive. Holds the cycle-wide coordinates the steps share."""

    clock: Clock
    vendor: Vendor
    roster: Roster
    lake_root: Path
    pid: int
    snap_ts: datetime = field(init=False)
    day: date = field(init=False)
    start_ts: str = field(init=False)

    def __post_init__(self) -> None:
        # One instant anchors the whole cycle. The snap slot is that instant floored to
        # the minute. Zeroing seconds and microseconds is flooring, which the clock-seam
        # scanner allows. The segment stamp and the partition date derive from the same
        # instant, so every segment in the cycle files under one writer session.
        cycle_start = self.clock.now()
        self.snap_ts = cycle_start.replace(second=0, microsecond=0)
        self.day = self.snap_ts.date()
        self.start_ts = cycle_start.strftime(_SEGMENT_STAMP_FORMAT)

    # -- planning: the fallible, fail-open half ------------------------------

    def _gap_plan(
        self,
        surface: str,
        ticker: str,
        error_class: str,
        fetch_ts: datetime,
        fetch_end_ts: datetime,
    ) -> _Plan:
        batch = journal.gap_batch(
            surface,
            ticker=ticker,
            snap_ts=self.snap_ts,
            error_class=error_class,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
        )
        return _Plan(batch, journal.ROW_KIND_GAP, error_class, fetch_ts, fetch_end_ts)

    def _plan_chain(self, ticker: str) -> _Plan:
        """Fetch one chain and plan its segment. A failure plans a gap, never raises.

        ``fetch_ts`` is stamped before the request and ``fetch_end_ts`` the instant the
        response or the failure lands, so the request round-trip is captured even when
        the fetch times out.
        """
        fetch_ts = self.clock.now()
        try:
            response = self.vendor.get_chain(ticker)
        except Exception as exc:
            fetch_end_ts = self.clock.now()
            return self._gap_plan(CHAINS, ticker, _error_class(exc), fetch_ts, fetch_end_ts)
        fetch_end_ts = self.clock.now()
        try:
            if not _ok(response.status):
                return self._gap_plan(
                    CHAINS, ticker, f"http_{response.status}", fetch_ts, fetch_end_ts
                )
            batch = _build_snapshot_batch(
                CHAINS,
                ticker,
                response.body,
                snap_ts=self.snap_ts,
                fetch_ts=fetch_ts,
                fetch_end_ts=fetch_end_ts,
            )
            return _Plan(batch, journal.ROW_KIND_DATA, None, fetch_ts, fetch_end_ts)
        except Exception as exc:
            # A malformed payload fails open to a gap. Raw stays vendor-verbatim, so
            # nothing here judges a 200 it did receive. This only catches a body the row
            # builder could not read. The round-trip is the vendor call's, already
            # stamped above.
            return self._gap_plan(CHAINS, ticker, _error_class(exc), fetch_ts, fetch_end_ts)

    def _plan_quotes(self) -> list[tuple[str, _Plan]]:
        """Fetch the one batched quote request and plan a segment per roster ticker.

        The sampler is one shared failure unit. A failed batch plans a gap for every
        ticker. A success is split per ticker, each ticker planned on its own.
        """
        symbols = self.roster.symbols
        fetch_ts = self.clock.now()
        try:
            response = self.vendor.get_quotes(symbols)
        except Exception as exc:
            fetch_end_ts = self.clock.now()
            error_class = _error_class(exc)
            return [
                (sym, self._gap_plan(QUOTES, sym, error_class, fetch_ts, fetch_end_ts))
                for sym in symbols
            ]
        fetch_end_ts = self.clock.now()
        if not _ok(response.status):
            error_class = f"http_{response.status}"
            return [
                (sym, self._gap_plan(QUOTES, sym, error_class, fetch_ts, fetch_end_ts))
                for sym in symbols
            ]
        return [
            (sym, self._plan_one_quote(response.body, sym, fetch_ts, fetch_end_ts))
            for sym in symbols
        ]

    def _plan_one_quote(
        self,
        body: Mapping[str, object],
        ticker: str,
        fetch_ts: datetime,
        fetch_end_ts: datetime,
    ) -> _Plan:
        """Split one ticker out of a batched quote body and plan its segment.

        The batched response maps each ticker to an envelope. The bid, ask, and last
        live in the envelope's ``quote`` block, and the real-time entitlement flag lives
        on the envelope. The row builder takes one flat quote mapping, so this merges the
        flag into the quote fields. A ticker missing from a 200 batch is its own gap. The
        round-trip stamps are the shared batch's, since one request served every ticker.
        """
        try:
            if _flatten_quote(body, ticker) is None:
                return self._gap_plan(QUOTES, ticker, "quote_absent", fetch_ts, fetch_end_ts)
            batch = _build_snapshot_batch(
                QUOTES,
                ticker,
                body,
                snap_ts=self.snap_ts,
                fetch_ts=fetch_ts,
                fetch_end_ts=fetch_end_ts,
            )
            return _Plan(batch, journal.ROW_KIND_DATA, None, fetch_ts, fetch_end_ts)
        except Exception as exc:
            return self._gap_plan(QUOTES, ticker, _error_class(exc), fetch_ts, fetch_end_ts)

    # -- writing: the durable half -------------------------------------------

    def _write(self, surface: str, ticker: str, plan: _Plan) -> SegmentOutcome:
        """Write one planned batch to a fresh segment and make it durable.

        The writer creates the segment exclusively, appends the one batch, and closes it
        with the end-of-stream marker. Every write is a full flush, so a returned outcome
        means the segment is on disk.
        """
        writer = journal.SegmentWriter.open(
            self.lake_root, surface, ticker, self.day, self.start_ts, self.pid
        )
        with writer:
            writer.write_cycle(plan.batch)
        partition = writer.path.relative_to(self.lake_root).as_posix()
        return SegmentOutcome(
            surface=surface,
            ticker=ticker,
            path=writer.path,
            partition=partition,
            row_kind=plan.row_kind,
            rows=plan.batch.num_rows,
            error_class=plan.error_class,
            fetched_at=plan.fetch_ts.isoformat() if plan.fetch_ts is not None else None,
        )

    # -- the cycle -----------------------------------------------------------

    def run(self) -> CycleResult:
        plans: list[tuple[str, str, _Plan]] = []
        # Chains: one per options ticker, each planned on its own for skip-not-block.
        for entry in self.roster:
            if entry.options:
                plans.append((CHAINS, entry.ticker, self._plan_chain(entry.ticker)))
        # Quotes: one shared batched request, then a segment per roster ticker.
        for ticker, plan in self._plan_quotes():
            plans.append((QUOTES, ticker, plan))

        # Write every segment durably before touching the manifest. A write failure
        # after planning is recorded and the cycle keeps going, never crashing.
        outcomes: list[SegmentOutcome] = []
        errors: list[SegmentError] = []
        for surface, ticker, plan in plans:
            try:
                outcomes.append(self._write(surface, ticker, plan))
            except Exception as exc:
                errors.append(SegmentError(surface, ticker, _error_class(exc)))

        # Now the segments are durable, append one manifest entry per segment, keyed by
        # the segment path, under the lake-root lock. This is the slice-1 segment-keyed
        # entry. The lock serializes lake-mutating jobs, so the manifest append never
        # races a daily job. Capture stayed outside the lock for the perishable part.
        with lake_lock(self.lake_root):
            for outcome in outcomes:
                record_partition(
                    self.lake_root,
                    outcome.partition,
                    source=CAPTURE_SOURCE,
                    rows=outcome.rows,
                    fetched_at=outcome.fetched_at,
                )

        return CycleResult(snap_ts=self.snap_ts, segments=tuple(outcomes), errors=tuple(errors))


def run_cycle(
    clock: Clock,
    vendor: Vendor,
    roster: Roster,
    lake_root: Path | str,
    *,
    pid: int | None = None,
) -> CycleResult:
    """Run one capture cycle. The primitive the daemon will later call in a loop.

    It performs one cycle over the injected clock, vendor, and roster, writing into
    ``lake_root``. It reads no wall clock and names no session time. ``pid`` defaults to
    this process, and a test pins it so segment names are deterministic.

    The steps, in order:

    1. Assign ``snap_ts`` from the clock, floored to the minute.
    2. For each options ticker, fetch the chain and write a chains segment. A failure
       writes a chains gap row. One ticker's failure never blocks another.
    3. Fetch the batched quotes for every roster ticker, split per ticker, and write a
       quotes segment each. A failed batch gaps every ticker's quotes.
    4. Append one manifest entry per segment, keyed by the segment path, under the
       lake-root lock.
    """
    cycle = _CaptureCycle(
        clock=clock,
        vendor=vendor,
        roster=roster,
        lake_root=Path(lake_root),
        pid=os.getpid() if pid is None else pid,
    )
    return cycle.run()


def run_cycle_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    pid: int | None = None,
) -> CycleResult:
    """Run one cycle wired from the real config, roster, and Schwab-backed vendor.

    This is the thin production entry the slice-1 runner (D8) calls. It loads the
    machine-local config and the portable roster, builds the authenticated vendor from
    the token file, and runs the same core cycle. The ``schwab-py`` client is built only
    here, lazily inside ``SchwabVendor.from_token``, so importing this module and running
    the offline suite need neither the library nor a real token. A test drives
    ``run_cycle`` directly with fakes instead.
    """
    config = load_config(config_path)
    roster = load_tickers(tickers_path)
    vendor = SchwabVendor.from_token(
        token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return run_cycle(
        clock if clock is not None else SystemClock(),
        vendor,
        roster,
        config.lake_root,
        pid=pid,
    )


def journal_snapshot(
    lake_root: Path | str,
    surface: str,
    ticker: str,
    *,
    body: Mapping[str, object],
    cycle_start: datetime,
    fetch_ts: datetime,
    fetch_end_ts: datetime,
    pid: int | None = None,
    source: str = CAPTURE_SOURCE,
) -> SegmentOutcome:
    """Journal one already-fetched response as a single durable capture cycle.

    This is the durable half of the capture primitive, factored so a caller that already
    holds a response can land it exactly the way the loop lands a cycle. Onboarding uses
    it to journal its first snapshot rather than discard a perishable sample. It reuses
    the same primitives end to end, so the result is indistinguishable in shape from a
    segment ``run_cycle`` writes:

    1. Derive the coordinates from ``cycle_start`` the way a cycle does: ``snap_ts`` is
       that instant floored to the minute, ``day`` is that slot's date, and ``start_ts``
       is the writer-session stamp. The caller stamps ``cycle_start``, ``fetch_ts``, and
       ``fetch_end_ts`` from the injected clock around its own fetch.
    2. Build the surface's data batch with the D4 journal row builders.
    3. Open a fresh ``SegmentWriter``, write the one cycle, and close it, which lays down
       the end-of-stream marker.
    4. Append one segment-keyed manifest entry under the lake-root lock, keyed by the
       segment path, ``source`` defaulting to ``capture``, and ``fetched_at`` the
       dispatch time, exactly as the loop records a segment.

    Each call is its own writer session. A re-run stamps a different ``start_ts`` and
    ``pid``, so the segment name differs and the ``O_CREAT | O_EXCL`` create never
    collides. Journaling another snapshot on a re-onboard is therefore fine: each is a
    real cycle taken at its own moment, never a discarded sample.
    """
    lake_root = Path(lake_root)
    snap_ts = cycle_start.replace(second=0, microsecond=0)
    day = snap_ts.date()
    start_ts = cycle_start.strftime(_SEGMENT_STAMP_FORMAT)
    writer_pid = os.getpid() if pid is None else pid

    batch = _build_snapshot_batch(
        surface,
        ticker,
        body,
        snap_ts=snap_ts,
        fetch_ts=fetch_ts,
        fetch_end_ts=fetch_end_ts,
    )
    writer = journal.SegmentWriter.open(lake_root, surface, ticker, day, start_ts, writer_pid)
    with writer:
        writer.write_cycle(batch)
    partition = writer.path.relative_to(lake_root).as_posix()
    fetched_at = fetch_ts.isoformat()
    with lake_lock(lake_root):
        record_partition(
            lake_root,
            partition,
            source=source,
            rows=batch.num_rows,
            fetched_at=fetched_at,
        )
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=writer.path,
        partition=partition,
        row_kind=journal.ROW_KIND_DATA,
        rows=batch.num_rows,
        error_class=None,
        fetched_at=fetched_at,
    )


__all__ = [
    "CycleResult",
    "SegmentError",
    "SegmentOutcome",
    "journal_snapshot",
    "run_cycle",
    "run_cycle_from_config",
]
