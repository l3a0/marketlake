"""The capture primitive: one cycle.

A capture cycle is the smallest unit of capture. It fetches every option chain and
one batched equity quote, journals what the vendor sent, and records each journal
segment in the manifest. This module builds that one cycle. The daemon that calls it
once a minute is ``lake.daemon``. The market-hours, calendar, and session logic live
there and in ``lake.session``. Here the cycle runs once and returns.

The cycle's two halves are also exported on their own, because two callers outside the
loop already hold one half and want the other done the loop's way. ``fetch_chain`` is
the fetching half: it runs a chain's date-window plan and reassembles one snapshot.
``journal_snapshot`` is the landing half: it takes a response someone already fetched
and writes it as a durable cycle. Onboarding uses the landing half alone, and
``fill_option_close``, the close+5 guard's refetch, uses both. So the fill and the loop
fetch a chain by one code path rather than two.

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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lake import journal
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans, CaptureSpansError, spans_path
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, load_config
from lake.lock import lake_lock
from lake.manifest import record_partition
from lake.metadata import stamp_cycle
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, SecurityMasterError, master_path
from lake.session import OPTION_CLOSE
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


def _has_contracts(body: Mapping[str, object]) -> bool:
    """Whether a reassembled chain body holds at least one contract.

    A window that answers 200 with empty expiration maps is a successful window that
    captured nothing. The cycle still journals that as a data segment, because what the
    vendor sent is what the cycle records. The close+5 fill cannot, because a fill is a
    claim that the close of record was rescued, and a segment with no contract row
    rescues nothing.
    """
    for map_key in _CHAIN_EXP_MAPS:
        exp_map = body.get(map_key) or {}
        if not isinstance(exp_map, Mapping):
            continue
        for strikes in exp_map.values():
            for contracts in strikes.values():
                if contracts:
                    return True
    return False


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
    close_tag: str | None = None,
    session_phase: str | None = None,
    windows: Sequence[tuple[date | str, date | str | None]] = (),
    absent_markers: Sequence[journal.AbsentMarker] = (),
) -> object:
    """Build one surface's data batch from a vendor response, the loop's own way.

    This is the row-building step of a capture cycle, factored so another caller can
    build a batch from a response it already fetched. A chains body maps straight
    through the D4 chains builder, which stamps each contract's ``vendor_quote_ts`` from
    its own ``quoteTimeInLong``. A batched quotes body is split to the one ticker first.
    The result is byte-for-byte what the loop would build for the same response.
    ``close_tag`` and ``session_phase`` are the loop's two provenance tags, stamped on
    every row. Both default to null for a caller outside the loop.

    ``windows`` and ``absent_markers`` are the windowed chain fetch's own two outputs:
    the date ranges each row was fetched by, and what a failed range should have carried.
    They belong to a chains body alone, so a quotes caller that passes either is a
    programming error rather than a silently dropped argument.
    """
    if surface == CHAINS:
        return journal.chains_data_batch(
            body,
            ticker=ticker,
            snap_ts=snap_ts,
            fetch_ts=fetch_ts,
            fetch_end_ts=fetch_end_ts,
            close_tag=close_tag,
            session_phase=session_phase,
            windows=windows,
            absent_markers=absent_markers,
        )
    if windows or absent_markers:
        raise ValueError(f"windows and absent markers belong to a chains body, not {surface!r}")
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
            close_tag=close_tag,
            session_phase=session_phase,
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
    slot the whole cycle fired for. ``nothing_to_capture`` is true when the cycle ran over
    an empty roster: every ticker retired, so there was nothing to fetch and no segment to
    write. That is a different shape from a non-empty roster where every fetch failed,
    which still writes gap segments. The dead-man feed tells the two apart, because a
    fully retired daemon is alive and idle, not broken.
    """

    snap_ts: datetime
    segments: tuple[SegmentOutcome, ...]
    errors: tuple[SegmentError, ...] = ()
    nothing_to_capture: bool = False

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


@dataclass(frozen=True)
class ChainFetch:
    """One windowed chain fetch, reassembled and ready to journal.

    This is the fetching half of a chain capture, the part before any row is built. It
    is factored out of the cycle the way ``journal_snapshot`` factored the landing half
    out, so the loop and the close+5 fill share one code path rather than two.

    ``body`` is the merged snapshot, in the vendor's own shape, or ``None`` when every
    window failed and nothing was captured. ``windows`` is the concrete plan the fetch
    ran, the ``(from_date, to_date | None)`` ranges. ``absent_markers`` names what a
    failed window should have carried, one marker per expiration the prior durable batch
    places inside the failed range. ``error_class`` is the first failed window's class,
    the representative signal, and ``None`` when every window succeeded. ``fetch_ts`` and
    ``fetch_end_ts`` span the whole windowed fetch, so even a timeout's duration is in
    them.
    """

    body: Mapping[str, object] | None
    windows: tuple[tuple[date, date | None], ...]
    absent_markers: tuple[journal.AbsentMarker, ...]
    fetch_ts: datetime
    fetch_end_ts: datetime
    error_class: str | None


@dataclass(frozen=True)
class FillResult:
    """What one close+5 fill captured, and what it could not.

    The guard needs all three of these and a bare expiration list carries only the first.

    ``expirations`` are the series the landed segment holds, empty when nothing landed.
    ``absent`` are the date windows the fetch gave up on, the same markers that rode the
    snapshot, so the guard can tell a series the fetch missed from one the vendor no
    longer offers. Those two populations look identical in a plain expiration
    difference, and marking them the same way would label a fetch failure a delisting.
    ``error_class`` is the first failed window's class, the representative signal, so a
    fill that captured nothing can say ``http_401`` rather than only that it came back
    empty.

    ``landed`` is whether a segment was written. A fill that captured nothing writes no
    row, because the day already holds the gap row from the cycle that failed at the
    close.
    """

    expirations: tuple[str, ...] = ()
    absent: tuple[journal.AbsentMarker, ...] = ()
    error_class: str | None = None

    @property
    def landed(self) -> bool:
        """Whether this fill journaled a segment."""
        return bool(self.expirations)

    @property
    def absent_expirations(self) -> frozenset[str]:
        """The series the fetch's own failed windows already marked absent.

        Named off the markers rather than recomputed, so the set the guard subtracts is
        exactly the set already on disk. A marker for a window with no prior batch to
        read names no expiration, and contributes nothing here.
        """
        return frozenset(m.expiration_date for m in self.absent if m.expiration_date)


def fetch_chain(
    clock: Clock,
    vendor: Vendor,
    ticker: str,
    *,
    day: date,
    lake_root: Path | str,
    plan: ChainPlan,
    guards: GuardConstants,
) -> ChainFetch:
    """Fetch one chain by its date-window plan and reassemble it, never raising.

    A full SPY chain in one request exceeds Schwab's gateway body limit (a 502 with
    errorcode ``protocol.http.TooBigBody``), so the chain is fetched in date windows read
    straight off the plan, then reassembled into one snapshot. There is no discovery
    request on the hot path. The control flow:

    1. **Read the plan.** ``plan.windows_for(day)`` turns the day-offset windows into
       concrete ``(from_date, to_date)`` ranges against the session date. The last
       range's ``to_date`` is ``None``, the open tail.
    2. **Fetch each window, sequentially.** ``_fetch_window`` fetches the range and merges
       its contracts. Only a genuine size signal, a ``TooBigBody`` 502 or a body flagged
       ``isChainTruncated``, is split at the window's date midpoint and refetched, bounded
       by ``chain_chunk_max_split_depth``. Any other failure, a non-2xx status or a raised
       exception, is recorded once with its own error class and never split.
    3. **Nothing captured.** If no window succeeded, ``body`` is ``None`` and
       ``error_class`` carries the first failed window's class. So an all-401 chain reads
       as ``http_401`` and the failure model still sees auth death on the chain surface.
    4. **Reassemble.** Every collected contract is merged into one body, a single snapshot
       the caller journals under one ``snap_ts``. The chain-level header fields (rates,
       underlying price, entitlement flag) come from the first window that returned
       successfully. Every window response carries the same top-level fields, so the first
       success is an unambiguous source.
    5. **Name the absence.** A window whose range failed becomes an absent marker the
       caller rides inside that same snapshot, carrying that window's own error class.

    The windows are fetched sequentially. Firing them in parallel with per-window jitter,
    to cut wall-time to the slowest window, is a refinement the design pins for after the
    D9 loop. It is deliberately not built here.

    ``fetch_ts`` is stamped before the first window fetch and ``fetch_end_ts`` after the
    last, so the round trip spans the whole windowed fetch.

    ``lake_root`` is read only on the failure path, and only to name the absence markers.
    The daemon holds no expiration state, so the missing expirations come from the
    journal's latest prior durable batch, read once per ticker per fetch.
    """
    windows = plan.windows_for(day)
    fetch_ts = clock.now()
    call_map: dict[str, dict[str, list]] = {}
    put_map: dict[str, dict[str, list]] = {}
    failed: list[tuple[date, date | None, str]] = []
    header_holder: list[Mapping[str, object]] = []
    for from_date, to_date in windows:
        _fetch_window(
            vendor, guards, ticker, from_date, to_date, 0, call_map, put_map, failed, header_holder
        )
    fetch_end_ts = clock.now()

    # No window returned successfully, so nothing was captured. The caller turns that into
    # a whole-chain gap, tagged with the first failed window's class so auth death, a
    # rate-limit, and a transient fault stay apart. A ChainPlan always has at least one
    # window, and every window path either seeds the header or records a failure, so a
    # failure exists here; the fallback only guards the impossible empty case. No absence
    # markers ride a whole-chain gap: the one gap row already stands for the whole chain.
    if not header_holder:
        return ChainFetch(
            None,
            tuple(windows),
            (),
            fetch_ts,
            fetch_end_ts,
            failed[0][2] if failed else CHAIN_CHUNK_FAILED,
        )

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
    # ticker per fetch and only on the failure path. For each failed range, keep the
    # prior expirations inside it and dated on or after the session date, one marker
    # each. With no prior batch, or none inside, emit one per-window marker instead. So
    # every failed range yields at least one marker and its class is never lost.
    absent_markers: list[journal.AbsentMarker] = []
    if failed:
        prior = journal.latest_expirations(lake_root, ticker)
        for from_date, to_date, error_class in failed:
            start = from_date.isoformat()
            end = None if to_date is None else to_date.isoformat()
            # The range bound alone excludes an expired series from yesterday's batch.
            # Every plan window starts at offset 0 or later, so start is never before
            # the session date.
            inside = [exp for exp in (prior or []) if exp >= start and (end is None or exp <= end)]
            if inside:
                absent_markers.extend(
                    journal.AbsentMarker(start, end, error_class, exp) for exp in inside
                )
            else:
                absent_markers.append(journal.AbsentMarker(start, end, error_class, None))
    return ChainFetch(
        merged_body,
        tuple(windows),
        tuple(absent_markers),
        fetch_ts,
        fetch_end_ts,
        failed[0][2] if failed else None,
    )


def _fetch_window(
    vendor: Vendor,
    guards: GuardConstants,
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
       merge cannot read is treated like a too-big window, so the fetch still lands what
       the other windows returned.
    """
    try:
        response = vendor.get_chain(ticker, from_date=from_date, to_date=to_date)
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
            # A body that would not merge is treated like a too-big window, so the fetch
            # still lands what the other windows returned.
            too_big = True
    elif not too_big:
        # A non-2xx status that is not the TooBigBody fault is not a size problem. Record
        # it once with its http class and do not split.
        failed.append((from_date, to_date, f"http_{response.status}"))
        return

    splittable = (
        to_date is not None and to_date > from_date and depth < guards.chain_chunk_max_split_depth
    )
    if not splittable:
        # An open-ended tail window (``to_date is None``) that comes back too big
        # cannot be midpoint-split, so it is given up with the size class. Far-term
        # sparsity makes this unreachable in practice: the open tail holds the fewest
        # expirations of any window.
        failed.append((from_date, to_date, CHAIN_CHUNK_FAILED))
        return
    mid = from_date + timedelta(days=(to_date - from_date).days // 2)
    _fetch_window(
        vendor, guards, ticker, from_date, mid, depth + 1, call_map, put_map, failed, header_holder
    )
    _fetch_window(
        vendor,
        guards,
        ticker,
        mid + timedelta(days=1),
        to_date,
        depth + 1,
        call_map,
        put_map,
        failed,
        header_holder,
    )


@dataclass
class _CaptureCycle:
    """One run of the primitive. Holds the cycle-wide coordinates the steps share.

    ``close_tag`` and ``session_phase`` are the loop's two provenance tags. They are
    cycle-wide: every batch this cycle builds, data and gap alike, carries both.
    """

    clock: Clock
    vendor: Vendor
    roster: Roster
    lake_root: Path
    pid: int
    guards: GuardConstants
    plan: ChainPlan
    close_tag: str | None = None
    session_phase: str | None = None
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
            close_tag=self.close_tag,
            session_phase=self.session_phase,
        )
        return _Plan(batch, journal.ROW_KIND_GAP, error_class, fetch_ts, fetch_end_ts)

    def _plan_chain(self, ticker: str) -> _Plan:
        """Fetch one chain by its date-window plan and plan its segment, never raising.

        ``fetch_chain`` does the fetching. It reads the plan, fetches each window,
        reassembles one snapshot, and names the absence markers for the windows that
        failed. Its docstring carries that half's rules. What is left here is the cycle's
        half: stamp the cycle's own coordinates on the reassembled body and resolve it
        into a plan.

        1. **Nothing captured.** A fetch that returned no body is a whole-chain gap for
           this ticker, tagged with the fetch's representative error class.
        2. **A body the row builder could not read.** It fails open to a whole-chain gap,
           the same fail-open the single-fetch path used. Raw stays vendor-verbatim.
        3. **A partial snapshot.** It still journals as a data segment. Its absence
           markers ride inside it, each carrying its own window's class. The segment flag
           takes the first failed window's class, the representative signal, mirroring the
           whole-chain gap.
        """
        fetched = fetch_chain(
            self.clock,
            self.vendor,
            ticker,
            day=self.day,
            lake_root=self.lake_root,
            plan=self.plan,
            guards=self.guards,
        )
        if fetched.body is None:
            return self._gap_plan(
                CHAINS, ticker, fetched.error_class, fetched.fetch_ts, fetched.fetch_end_ts
            )
        try:
            batch = journal.chains_data_batch(
                fetched.body,
                ticker=ticker,
                snap_ts=self.snap_ts,
                fetch_ts=fetched.fetch_ts,
                fetch_end_ts=fetched.fetch_end_ts,
                close_tag=self.close_tag,
                session_phase=self.session_phase,
                windows=fetched.windows,
                absent_markers=fetched.absent_markers,
            )
        except Exception as exc:
            return self._gap_plan(
                CHAINS, ticker, _error_class(exc), fetched.fetch_ts, fetched.fetch_end_ts
            )
        return _Plan(
            batch,
            journal.ROW_KIND_DATA,
            fetched.error_class,
            fetched.fetch_ts,
            fetched.fetch_end_ts,
        )

    def _plan_quotes(self) -> list[tuple[str, _Plan]]:
        """Fetch the one batched quote request and plan a segment per roster ticker.

        The sampler is one shared failure unit. A failed batch plans a gap for every
        ticker. A success is split per ticker, each ticker planned on its own.

        An empty roster, every ticker retired, skips the request outright. Nothing is
        owed, so nothing is fetched, and no cycle wastes a batched call on zero symbols.
        """
        symbols = self.roster.symbols
        if not symbols:
            return []
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
                close_tag=self.close_tag,
                session_phase=self.session_phase,
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
        # Chains: one per options ticker, each planned on its own for skip-not-block. The
        # tickers are fetched sequentially, in roster order. The design's per-ticker
        # workers, fired in parallel at the minute top with a few tens of milliseconds of
        # stagger, are a later refinement past the D9 loop. Not built here.
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

        # Last, stamp what the rows cannot carry: the token's mint time and the roster.
        self._stamp()
        return CycleResult(
            snap_ts=self.snap_ts,
            segments=tuple(outcomes),
            errors=tuple(errors),
            nothing_to_capture=not self.roster,
        )

    def _stamp(self) -> None:
        """Stamp the cycle's token mint time and roster into the journal metadata.

        The mint time comes off the vendor this cycle actually fetched with, never from
        a separate read of the token file. So the panel shows the token capture actually
        runs on. The stamp is a timestamp. No token material reaches the lake.

        A stamp is a report about the cycle, not part of it. So a vendor that cannot say
        when its token was minted, and a write that fails, each cost the stamp and never
        the captured rows. The next cycle stamps again a minute later.
        """
        try:
            minted = self.vendor.token_mint_time()
        except Exception:  # noqa: BLE001 - any vendor failure here costs a stamp, not a cycle
            return
        try:
            stamp_cycle(self.lake_root, at=self.snap_ts, token_minted_at=minted, roster=self.roster)
        except OSError:
            return


def run_cycle(
    clock: Clock,
    vendor: Vendor,
    roster: Roster,
    lake_root: Path | str,
    *,
    pid: int | None = None,
    guards: GuardConstants | None = None,
    plan: ChainPlan | None = None,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> CycleResult:
    """Run one capture cycle. The primitive the daemon calls once a minute.

    It performs one cycle over the injected clock, vendor, and roster, writing into
    ``lake_root``. It reads no wall clock and names no session time. ``pid`` defaults to
    this process, and a test fixes it so segment names are deterministic. ``guards`` carries
    the tunable thresholds the cycle reads, chiefly the chunker's date-based split-depth
    bound. It defaults to the design's pinned values. ``plan`` is the chain chunk plan, the
    set of date windows the chain is fetched by. It defaults to ``load_chain_plan()``,
    which reads the machine-owned plan file and falls back to the built-in default. A test
    injects a small plan to drive the windows exactly.

    ``close_tag`` and ``session_phase`` are the loop's two provenance tags. The loop decides
    them per minute from the session clock and its close-tag hook. The cycle stamps both on
    every row it writes, on both surfaces, gap rows and absence markers included, so a
    tagged cycle tags consistently. A caller outside the loop leaves both null.

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
    5. Stamp the token's mint time and the roster into the journal metadata, so the
       dashboard reads both from the lake rather than from ``~/.config``.
    """
    cycle = _CaptureCycle(
        clock=clock,
        vendor=vendor,
        roster=roster,
        lake_root=Path(lake_root),
        pid=os.getpid() if pid is None else pid,
        guards=guards if guards is not None else GuardConstants(),
        plan=plan if plan is not None else load_chain_plan(),
        close_tag=close_tag,
        session_phase=session_phase,
    )
    return cycle.run()


def run_cycle_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    pid: int | None = None,
    close_tag: str | None = None,
    session_phase: str | None = None,
) -> CycleResult:
    """Run one cycle wired from the real config, roster, and Schwab-backed vendor.

    This is the thin production entry the slice-1 runner (D8) and the daemon loop (D9)
    call. It loads the machine-local config and the portable roster, builds the
    authenticated vendor from the token file, and runs the same core cycle. Every call
    reloads all four inputs: the config, the roster, the token, and the chain plan. Nothing
    is cached across calls. That is the per-cycle re-read the design wants, so a nightly
    plan rewrite takes effect the next minute and a re-auth is picked up the next cycle.
    The ``schwab-py`` client is built only here, lazily inside ``SchwabVendor.from_token``,
    so importing this module and running the offline suite need neither the library nor a
    real token. A test drives ``run_cycle`` directly with fakes instead. ``close_tag`` and
    ``session_phase`` pass straight through to ``run_cycle``.
    """
    config = load_config(config_path)
    roster = load_tickers(tickers_path)
    resolved_clock = clock if clock is not None else SystemClock()
    live_roster = _live_roster(roster, config.lake_root, resolved_clock.now())
    vendor = SchwabVendor.from_token(
        token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return run_cycle(
        resolved_clock,
        vendor,
        live_roster,
        config.lake_root,
        pid=pid,
        guards=config.guards,
        plan=load_chain_plan(),
        close_tag=close_tag,
        session_phase=session_phase,
    )


def _live_roster(roster: Roster, lake_root: Path | str, now: datetime) -> Roster:
    """The entries this cycle actually captures: enabled, and inside an open span.

    Retiring closes a ticker's capture span before it turns off the roster entry, so a
    crash between the two writes leaves a stale enabled entry with a closed span. Filtering
    on the span too, not only on ``enabled``, means that stale entry is never captured, so
    no row is ever recorded outside a span.

    A missing master or spans file widens rather than narrows: every enabled entry is
    captured, the same as before capture spans existed. A missing reference file must
    never stop capture, the same rule every other reader of these two files follows. A
    ticker the master cannot resolve is kept for the same reason: losing the clamp only
    ever widens what gets captured.
    """
    enabled = roster.enabled
    try:
        master = SecurityMaster.read(master_path(lake_root))
    except (OSError, SecurityMasterError, ValueError):
        return Roster(enabled)
    try:
        spans = CaptureSpans.read(spans_path(lake_root))
    except (OSError, CaptureSpansError, ValueError):
        return Roster(enabled)
    on = now.astimezone(MARKET_TZ).date()
    kept = []
    for entry in enabled:
        try:
            instrument_id = master.resolve(entry.ticker, on, id_type=ID_TYPE_TICKER)
            in_scope = instrument_id is None or spans.in_scope(instrument_id, now)
        except Exception:  # noqa: BLE001 - a per-ticker scope check must never crash a cycle
            # Deliberately broad. A master or a spans file that loaded but carries a
            # drifted value (a naive or retyped timestamp) raises from a comparison
            # inside resolution or the span check, not from the read that opened the
            # file. This is the live capture path, so the price of missing an
            # unenumerated error here is a crashed cycle, worse than the dashboard's
            # unclamped panel. Widen instead: keep the ticker, the same answer a
            # missing master or spans file already gives.
            kept.append(entry)
            continue
        if in_scope:
            kept.append(entry)
    return Roster(tuple(kept))


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
    slot: datetime | None = None,
    close_tag: str | None = None,
    session_phase: str | None = None,
    windows: Sequence[tuple[date | str, date | str | None]] = (),
    absent_markers: Sequence[journal.AbsentMarker] = (),
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

    ``windows`` and ``absent_markers`` are what a windowed chain fetch produces beside its
    body: the date ranges the rows were fetched by, and what a failed range should have
    carried. A caller holding a ``ChainFetch`` passes both, so the segment it lands is the
    same shape a loop cycle writes for the same fetch. Onboarding fetches a whole chain in
    one request, so it passes neither and both columns stay null.
    """
    lake_root = Path(lake_root)
    # The slot a row stands for is not always the minute it was fetched in. The close+5
    # fill observes the option close from several minutes after it, and the row must
    # carry the close, so the caller may name the slot outright.
    snap_ts = slot if slot is not None else cycle_start.replace(second=0, microsecond=0)
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
        close_tag=close_tag,
        session_phase=session_phase,
        windows=windows,
        absent_markers=absent_markers,
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
        # A partial snapshot journals as a data segment carrying absence markers inside
        # it. Its segment flag takes the first failed window's class, the representative
        # signal, which is what the loop's own partial snapshot reports. Reporting null
        # for a segment that does hold a failure would hide it from every caller.
        error_class=absent_markers[0].error_class if absent_markers else None,
        fetched_at=fetched_at,
    )


def _landed_expirations(path: Path) -> list[str]:
    """The distinct expiration dates the data rows of one chains segment carry.

    Read back off the segment the fill just wrote, rather than counted off the body it
    sent, because this answer is compared against ``journal.latest_expirations``. That
    reader takes the ``expiration_date`` column of a durable batch, so taking the same
    column here means the two sides of the membership comparison are built the same way.
    An absence-marker gap row is excluded, so a series the fill failed to fetch is never
    counted as one it captured.
    """
    table = journal.read_segment(path)
    kinds = table.column("row_kind").to_pylist()
    expirations = table.column("expiration_date").to_pylist()
    return sorted(
        {
            str(exp).split("T")[0]
            for kind, exp in zip(kinds, expirations, strict=True)
            if kind == journal.ROW_KIND_DATA and exp
        }
    )


def fill_option_close(
    clock: Clock,
    vendor: Vendor,
    ticker: str,
    *,
    slot: datetime,
    lake_root: Path | str,
    guards: GuardConstants | None = None,
    plan: ChainPlan | None = None,
    pid: int | None = None,
    session_phase: str | None = None,
) -> FillResult:
    """Refetch one ticker's option close inside the close+5 window and journal it.

    This is the close+5 guard's fill. Option quotes freeze at the option close, so a fetch
    inside the five minutes after it still observes the closing marks. That is what makes
    the refetch legitimate, and it is the only refetch this lake sanctions.

    The fetch is the loop's own. It goes through ``fetch_chain``, so the chain is fetched
    by its date-window plan and reassembled exactly as a capture cycle fetches it. One
    unchunked request would fail on the biggest chains, which trip the gateway body limit,
    and those are the same chains most worth rescuing. The landing is the loop's own too,
    through ``journal_snapshot``, so the segment is the shape a cycle writes.

    ``slot`` is the option close, and it is the whole point of the two timestamps being
    separate. The rows carry the close in ``snap_ts``, so a reader asking for the option
    close gets the close, and the fetch minute in ``fetch_ts``, so the round trip stays
    measurable. ``close_tag`` is ``option_close`` on every row, so a second guard run
    counts the fill as the close already observed rather than fetching it again.

    Returns a ``FillResult``, never a bare list. The guard needs what the fill captured,
    what its failed windows gave up, and why, and only the first of those three fits in a
    list of expirations. A result whose ``expirations`` are empty is a fill that captured
    nothing, and it wrote no row. The day already carries the gap row from the cycle that
    triggered the fill, and a second row for that one minute would double-count it in
    every per-slot completeness read. The guard records the refusal in its own outcome
    instead, naming the error class this result carries.

    ``guards`` and ``plan`` default the way ``run_cycle`` defaults them, so a caller with
    no config still fetches by the machine's own plan.
    """
    lake_root = Path(lake_root)
    cycle_start = clock.now()
    fetched = fetch_chain(
        clock,
        vendor,
        ticker,
        day=slot.date(),
        lake_root=lake_root,
        plan=plan if plan is not None else load_chain_plan(),
        guards=guards if guards is not None else GuardConstants(),
    )
    if fetched.body is None or not _has_contracts(fetched.body):
        # Nothing to land. ``body is None`` is every window having failed. An empty body
        # is the subtler case: a window that answers 200 with empty expiration maps is a
        # successful fetch that carries no contract, and Schwab serves exactly that shape
        # for a fault it reports in the body rather than the status. Writing either one
        # would leave a zero-row segment and a ``rows=0`` manifest entry standing for a
        # close nobody captured, and would report the close as filled. The check runs
        # before the write, so no segment is created to clean up. The class still rides
        # the result, so the guard can say which failure it was.
        return FillResult(error_class=fetched.error_class)
    outcome = journal_snapshot(
        lake_root,
        CHAINS,
        ticker,
        body=fetched.body,
        cycle_start=cycle_start,
        fetch_ts=fetched.fetch_ts,
        fetch_end_ts=fetched.fetch_end_ts,
        pid=pid,
        slot=slot,
        close_tag=OPTION_CLOSE,
        session_phase=session_phase,
        windows=fetched.windows,
        absent_markers=fetched.absent_markers,
    )
    return FillResult(
        tuple(_landed_expirations(outcome.path)),
        fetched.absent_markers,
        fetched.error_class,
    )


def fill_option_close_from_config(
    ticker: str,
    *,
    slot: datetime,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    token_path: str | Path | None = None,
    session_phase: str | None = None,
    pid: int | None = None,
) -> FillResult:
    """Run one close+5 fill wired from the real config and a Schwab-backed vendor.

    This is the production entry the daemon's close+5 guard reaches through. It mirrors
    ``run_cycle_from_config``: it loads the machine-local config, builds the authenticated
    vendor from the token file, reloads the chain plan, and runs the same core fill. Every
    call reloads all three, so a re-auth or a nightly plan rewrite takes effect on the next
    fill rather than the next restart. The ``schwab-py`` client is built only here, lazily
    inside ``SchwabVendor.from_token``, so importing this module and running the offline
    suite need neither the library nor a real token.

    A fill is owed only when the option close is missing, which is rare, so the vendor is
    built per call rather than held. That costs one client construction on a path that
    normally never runs, and it buys a token read as fresh as the minute the fill fires.
    """
    config = load_config(config_path)
    vendor = SchwabVendor.from_token(
        token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return fill_option_close(
        clock if clock is not None else SystemClock(),
        vendor,
        ticker,
        slot=slot,
        lake_root=config.lake_root,
        guards=config.guards,
        plan=load_chain_plan(),
        pid=pid,
        session_phase=session_phase,
    )


__all__ = [
    "ChainFetch",
    "FillResult",
    "CycleResult",
    "SegmentError",
    "SegmentOutcome",
    "fetch_chain",
    "fill_option_close",
    "fill_option_close_from_config",
    "journal_snapshot",
    "run_cycle",
    "run_cycle_from_config",
]
