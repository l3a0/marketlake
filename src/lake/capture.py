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
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lake import journal
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, load_config
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

# The error class stamped on a too-big window that could not be split any further: a single
# day, the open tail, or the depth bound reached. Far-term sparsity makes it unreachable in
# practice. A window that failed for a non-size reason (auth, rate-limit, a transient status,
# a raised exception) carries its own class instead, so the failure model keeps those apart.
CHAIN_CHUNK_FAILED = "chain_chunk_failed"

# The two chain maps every window response nests contracts under.
_CHAIN_EXP_MAPS = ("callExpDateMap", "putExpDateMap")


def _is_too_big(body: Mapping[str, object]) -> bool:
    """Whether a chain response signals it was too big for one request.

    This is the only signal that warrants a midpoint split. Two shapes carry it. A 200 body
    flags itself ``isChainTruncated`` when Schwab clipped it to fit the gateway body limit. A
    502 gateway fault carries the ``TooBigBody`` errorcode, either at the top level under
    ``errorcode`` (the shape the offline fakes use) or nested under
    ``fault.detail.errorcode`` (the real gateway fault). Both mean split and refetch. Every
    other non-2xx status, and a raised exception, is *not* a size signal. Splitting one would
    be wrong, and for a 429 rate-limit it would fan out into a burst of more throttled
    requests, so the fetcher records those with their own class instead.
    """
    if body.get("isChainTruncated"):
        return True
    top = body.get("errorcode")
    if isinstance(top, str) and "TooBigBody" in top:
        return True
    fault = body.get("fault")
    if isinstance(fault, Mapping):
        detail = fault.get("detail")
        if isinstance(detail, Mapping):
            code = detail.get("errorcode")
            if isinstance(code, str) and "TooBigBody" in code:
                return True
    return False


def _collect_contracts(
    body: Mapping[str, object],
    call_map: dict[str, dict[str, list]],
    put_map: dict[str, dict[str, list]],
) -> None:
    """Merge one chunk body's contracts into the reassembly maps, verbatim.

    The maps nest ``expiration -> strike -> [contract]``. Chunks cover disjoint expiration
    ranges, so a merge never overwrites, only accretes. The contract dicts are copied by
    reference, untouched, so the calibrated row builder still sees the vendor's payload.
    """
    for map_key, target in ((_CHAIN_EXP_MAPS[0], call_map), (_CHAIN_EXP_MAPS[1], put_map)):
        exp_map = body.get(map_key) or {}
        if not isinstance(exp_map, Mapping):
            continue
        for exp_key, strikes in exp_map.items():
            bucket = target.setdefault(str(exp_key), {})
            for strike, contracts in strikes.items():
                bucket.setdefault(str(strike), []).extend(contracts)


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


def _quote_envelope(body: Mapping[str, object], ticker: str) -> Mapping[str, object] | None:
    """One ticker's per-symbol quote envelope from a batched quotes body, or ``None``.

    The batched response maps each ticker to an envelope holding the ``quote``,
    ``fundamental``, ``regular``, and ``extended`` blocks, plus envelope-level fields like
    ``realtime`` and the CUSIP. This returns the whole envelope unchanged, so the journal
    row builder can project each block through its own map. The blocks are never merged
    here, because ``quote`` and ``extended`` share field names. A ticker absent from a 200
    batch, or one with no ``quote`` block, yields ``None``, the caller's gap signal. This
    is the one place the split lives, shared by the loop and by the onboarding snapshot.
    """
    envelope = body.get(ticker)
    if not isinstance(envelope, Mapping):
        return None
    if not isinstance(envelope.get("quote"), Mapping):
        return None
    return envelope


def _quote_vendor_quote_ts(envelope: Mapping[str, object]) -> datetime | None:
    """The vendor quote time from a quote envelope's ``quote`` block, or ``None``."""
    quote = envelope.get("quote")
    if isinstance(quote, Mapping):
        return _epoch_ms_to_datetime(quote.get("quoteTime"))
    return None


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
    through the D4 chains builder, which stamps each contract's ``vendor_quote_ts`` from
    its own ``quoteTimeInLong``. A batched quotes body is split to the one ticker first.
    The result is byte-for-byte what the loop would build for the same response.
    """
    if surface == CHAINS:
        return journal.chains_data_batch(
            body,
            ticker=ticker,
            snap_ts=snap_ts,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
        )
    if surface == QUOTES:
        envelope = _quote_envelope(body, ticker)
        if envelope is None:
            raise VendorError(f"quotes response has no quote for {ticker!r}")
        return journal.quotes_data_batch(
            envelope,
            ticker=ticker,
            snap_ts=snap_ts,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
            vendor_quote_ts=_quote_vendor_quote_ts(envelope),
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
    guards: GuardConstants
    plan: ChainPlan
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
        """Fetch one chain by its date-window plan and plan its segment, never raising.

        A full SPY chain in one request exceeds Schwab's gateway body limit (a 502 with
        errorcode ``protocol.http.TooBigBody``), so the chain is fetched in date windows
        read straight off the plan, then reassembled into one snapshot. There is no
        discovery request on the hot path. The control flow:

        1. **Read the plan.** ``self.plan.windows_for(self.day)`` turns the day-offset
           windows into concrete ``(from_date, to_date)`` ranges against the cycle's
           session date. The last range's ``to_date`` is ``None``, the open tail.
        2. **Fetch each window, sequentially.** ``_fetch_window`` fetches the range and
           merges its contracts. Only a genuine size signal, a ``TooBigBody`` 502 or a body
           flagged ``isChainTruncated``, is split at the window's date midpoint and refetched,
           bounded by ``chain_chunk_max_split_depth``. Any other failure, a non-2xx status or
           a raised exception, is recorded once with its own error class and never split.
        3. **Whole-chain gap.** If no window succeeded, nothing was captured, so the whole
           chain is one gap for this ticker, tagged with the first failed window's class. So
           an all-401 chain gaps as ``http_401`` and the failure model still sees auth death
           on the chain surface.
        4. **Reassemble.** Every collected contract is journaled as one snapshot, a single
           chains segment sharing one ``snap_ts``, through the calibrated row builder. The
           chain-level header fields (rates, underlying price, entitlement flag) come from
           the first window that returned successfully. Every window response carries the
           same top-level fields, so the first success is an unambiguous source.
        5. **Partial failure.** A window whose range failed becomes one absent-marker gap row
           in that same snapshot, carrying that window's own error class, not a whole-chain
           gap. Only a chain where every window failed is a whole-chain gap.

        The windows are fetched sequentially in slice 1. Firing them in parallel with
        per-window jitter, to cut wall-time to the slowest window, is the D9 daemon
        follow-up the design pins. It is deliberately not built here.

        ``fetch_ts`` is stamped before the first window fetch and ``fetch_end_ts`` after
        the last, so the round-trip spans the whole windowed fetch and even a timeout's
        duration is captured.
        """
        windows = self.plan.windows_for(self.day)
        fetch_ts = self.clock.now()
        call_map: dict[str, dict[str, list]] = {}
        put_map: dict[str, dict[str, list]] = {}
        failed: list[tuple[date, date | None, str]] = []
        header_holder: list[Mapping[str, object]] = []
        for from_date, to_date in windows:
            self._fetch_window(
                ticker, from_date, to_date, 0, call_map, put_map, failed, header_holder
            )
        fetch_end_ts = self.clock.now()

        # No window returned successfully, so nothing was captured. That is a whole-chain
        # gap for this ticker, tagged with the first failed window's class so auth death, a
        # rate-limit, and a transient fault stay apart. A ChainPlan always has at least one
        # window, and every window path either seeds the header or records a failure, so a
        # failure exists here; the fallback only guards the impossible empty case.
        if not header_holder:
            error_class = failed[0][2] if failed else CHAIN_CHUNK_FAILED
            return self._gap_plan(CHAINS, ticker, error_class, fetch_ts, fetch_end_ts)

        # Reassemble one snapshot. The chain-level header fields are taken from the first
        # window that succeeded. Every window response carries the same top-level
        # ``underlyingPrice``, rates, and entitlement flag, so the first success is a single
        # unambiguous source. The contracts come from every window. The contract count and
        # truncation flag are not read from the header. chains_data_batch recomputes them
        # from the reassembled rows, so they describe the captured chain.
        header_source = header_holder[0]
        merged_body: dict[str, object] = {
            key: value for key, value in header_source.items() if key not in _CHAIN_EXP_MAPS
        }
        merged_body[_CHAIN_EXP_MAPS[0]] = call_map
        merged_body[_CHAIN_EXP_MAPS[1]] = put_map

        # Name the absence markers. The daemon holds no expiration state, so the missing
        # expirations come from the journal's latest prior durable batch, read once per
        # ticker per cycle and only on the failure path. For each failed range, keep the
        # prior expirations inside it and dated on or after the session date, one marker
        # each. With no prior batch, or none inside, emit one per-window marker instead. So
        # every failed range yields at least one marker and its class is never lost.
        absent_markers: list[journal.AbsentMarker] = []
        if failed:
            prior = journal.latest_expirations(self.lake_root, ticker)
            for from_date, to_date, error_class in failed:
                start = from_date.isoformat()
                end = None if to_date is None else to_date.isoformat()
                # The range bound alone excludes an expired series from yesterday's batch.
                # Every plan window starts at offset 0 or later, so start is never before
                # the session date.
                inside = [
                    exp for exp in (prior or []) if exp >= start and (end is None or exp <= end)
                ]
                if inside:
                    absent_markers.extend(
                        journal.AbsentMarker(start, end, error_class, exp) for exp in inside
                    )
                else:
                    absent_markers.append(journal.AbsentMarker(start, end, error_class, None))
        try:
            batch = journal.chains_data_batch(
                merged_body,
                ticker=ticker,
                snap_ts=self.snap_ts,
                fetch_ts=fetch_ts,
                fetch_end_ts=fetch_end_ts,
                windows=windows,
                absent_markers=absent_markers,
            )
        except Exception as exc:
            # A body the row builder could not read fails open to a whole-chain gap, the
            # same fail-open the single-fetch path used. Raw stays vendor-verbatim.
            return self._gap_plan(CHAINS, ticker, _error_class(exc), fetch_ts, fetch_end_ts)

        # A partial snapshot still journals as a data segment. Its absence markers ride
        # inside it, each carrying its own window's class. The segment flag takes the first
        # failed window's class, the representative signal, mirroring the whole-chain gap.
        error_class = failed[0][2] if failed else None
        return _Plan(batch, journal.ROW_KIND_DATA, error_class, fetch_ts, fetch_end_ts)

    def _fetch_window(
        self,
        ticker: str,
        from_date: date,
        to_date: date | None,
        depth: int,
        call_map: dict[str, dict[str, list]],
        put_map: dict[str, dict[str, list]],
        failed: list[tuple[date, date | None, str]],
        header_holder: list[Mapping[str, object]],
    ) -> None:
        """Fetch one date window, splitting only a genuine size failure at its midpoint.

        The window is fetched with ``from_date`` / ``to_date`` and no ``strike_count``. Four
        outcomes:

        1. A **raised exception** is a transport failure, not a size signal. The range is
           recorded in ``failed`` with the exception's own class and never split. Splitting a
           network error would only multiply it.
        2. A **too-big** response, a ``TooBigBody`` 502 or a body flagged
           ``isChainTruncated`` (see ``_is_too_big``), is split at the window's date
           midpoint and each half refetched, when the window is splittable: a concrete
           ``to_date``, spanning more than one day, and the depth bound not yet reached. When
           it cannot be split, the range is given up with the size class
           ``chain_chunk_failed``.
        3. Any **other non-2xx** status, an auth 401, a rate-limit 429, a transient 500, is
           recorded once in ``failed`` with ``http_<status>`` and never split. Splitting a
           429 in particular would fan out into more throttled requests.
        4. A **successful** 2xx, untruncated response has its contracts merged into the
           reassembly maps and, on the first success, seeds the header source. A body the
           merge cannot read is treated like a too-big window, so the cycle still lands what
           the other windows returned.
        """
        try:
            response = self.vendor.get_chain(ticker, from_date=from_date, to_date=to_date)
        except Exception as exc:
            # A raised fetch is a transport failure. Record it with its own class, no split.
            failed.append((from_date, to_date, _error_class(exc)))
            return

        too_big = _is_too_big(response.body)
        if _ok(response.status) and not too_big:
            try:
                _collect_contracts(response.body, call_map, put_map)
                if not header_holder:
                    header_holder.append(response.body)
                return
            except Exception:
                # A body that would not merge is treated like a too-big window, so the cycle
                # still lands what the other windows returned.
                too_big = True
        elif not too_big:
            # A non-2xx status that is not the TooBigBody fault is not a size problem. Record
            # it once with its http class and do not split.
            failed.append((from_date, to_date, f"http_{response.status}"))
            return

        splittable = (
            to_date is not None
            and to_date > from_date
            and depth < self.guards.chain_chunk_max_split_depth
        )
        if not splittable:
            # An open-ended tail window (``to_date is None``) that comes back too big
            # cannot be midpoint-split, so it is given up with the size class. Far-term
            # sparsity makes this unreachable in practice: the open tail holds the fewest
            # expirations of any window.
            failed.append((from_date, to_date, CHAIN_CHUNK_FAILED))
            return
        mid = from_date + timedelta(days=(to_date - from_date).days // 2)
        self._fetch_window(
            ticker, from_date, mid, depth + 1, call_map, put_map, failed, header_holder
        )
        self._fetch_window(
            ticker,
            mid + timedelta(days=1),
            to_date,
            depth + 1,
            call_map,
            put_map,
            failed,
            header_holder,
        )

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

        The batched response maps each ticker to an envelope. The row builder projects the
        envelope's blocks into typed columns. A ticker missing from a 200 batch, or one
        with no quote block, is its own gap. The round-trip stamps are the shared batch's,
        since one request served every ticker.
        """
        try:
            if _quote_envelope(body, ticker) is None:
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
    guards: GuardConstants | None = None,
    plan: ChainPlan | None = None,
) -> CycleResult:
    """Run one capture cycle. The primitive the daemon will later call in a loop.

    It performs one cycle over the injected clock, vendor, and roster, writing into
    ``lake_root``. It reads no wall clock and names no session time. ``pid`` defaults to
    this process, and a test pins it so segment names are deterministic. ``guards`` carries
    the tunable thresholds the cycle reads, chiefly the chunker's date-based split-depth
    bound. It defaults to the design's pinned values. ``plan`` is the chain chunk plan, the
    set of date windows the chain is fetched by. It defaults to ``load_chain_plan()``,
    which reads the machine-owned plan file and falls back to the built-in default. A test
    injects a small plan to drive the windows exactly.

    The steps, in order:

    1. Assign ``snap_ts`` from the clock, floored to the minute.
    2. For each options ticker, fetch the chain by its date-window plan and write a chains
       segment. A chain where every window failed writes a chains gap row. A window that
       fails past every split becomes an absence marker inside the snapshot. One ticker's
       failure never blocks another.
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
        guards=guards if guards is not None else GuardConstants(),
        plan=plan if plan is not None else load_chain_plan(),
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
        guards=config.guards,
        plan=load_chain_plan(),
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
