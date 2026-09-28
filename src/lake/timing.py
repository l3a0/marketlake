"""The timing file: a line per vendor request and per cycle, so a slow minute can be taken apart.

A capture cycle makes one request per chain window and one for the batched quotes, plus
one for each half of a split window and a second attempt at a transient failure
(marketlake #558). The rows a cycle writes carry one ``fetch_ts`` and one ``fetch_end_ts``
per ticker-minute, so a chain whose nine windows took 70 seconds cannot say whether one
window was slow or all nine were, or whether the time went to Schwab or to the network.
On 2026-09-24 three cycles ran past their minute and lost four slots, and nothing the lake
held could say which. Marketlake #531 is that gap, and this file is where its evidence lands.

Most lines name one request. A request line is one JSON object carrying the request's own
coordinates, which are the keys that join it to the rows it produced: ``snap_ts``,
``surface``, ``ticker``, and the ``window_start`` and ``window_end`` it asked for. It
carries what capture made of the reply, ``status`` and ``error_class``, and six instants.

1. ``request_start_ts`` is when the caller called the vendor.
2. ``request_sent_ts`` is when the request left the client, after any token refresh.
3. ``request_connected_ts`` is when a new connection finished its setup, null on a reused
   one.
4. ``request_headers_ts`` is when the response headers arrived.
5. ``request_body_ts`` is when the body finished arriving.
6. ``request_end_ts`` is when the vendor call returned or raised.

The caller stamps the first and the last from the injected clock, so every request has
both, including one that raised. The transport stamps the middle four, through
``lake.schwab.attach_timing``, so they are null when no response came back. From sent to
headers is roughly Schwab's time, from headers to body roughly the network's, and from body
to end the JSON parse. ``request_bytes`` is the body's size on the wire, which is what
separates a bandwidth limit from a slow answer. A rejection adds ``request_subcode``, a
429's sub-code, and ``request_error_detail``, a bounded copy of the reply.
``request_failure`` names what went wrong while the line was being recorded, and is null
when nothing did, so a reader can tell a stamp nobody observed from one that failed. Every
line carries ``v``, the line format's version, because nothing like ``schema_version``
covers a JSON file, and ``kind``, which is ``request`` here.

A capture cycle adds one more line, ``kind`` ``cycle``, after its request lines
(marketlake #537). The request lines end at the last response, and this one says where the
rest of the cycle went. It carries the cycle's ``snap_ts`` and six instants.

1. ``cycle_start_ts`` is when the cycle started, the instant its segment stamp uses.
2. ``fetch_end_ts`` is the latest ``fetch_end_ts`` among the units the cycle planned, a
   chain or the quote batch. A unit's spans its retry. On the concurrent path a unit the
   bound cut ends at the bound. Null when the cycle planned nothing.
3. ``segments_durable_ts`` is when the last unit had landed. Earlier units land inside
   the fetch, so the tail after the fetch is this less ``fetch_end_ts``.
4. ``lock_acquired_ts`` is when the cycle got the lake-root lock for its manifest append.
   From durable to acquired is the wait, on another process or another cycle.
5. ``lock_released_ts`` is when it let the lock go. From acquired to released is the hold.
6. ``cycle_end_ts`` is read after the metadata stamp and the request lines, just before
   this line is appended. The loop's hooks run later, on the loop thread, and are not in it.

``loadavg_start`` and ``loadavg_end`` are ``os.getloadavg()`` read at the cycle's start
and end, the 1, 5 and 15 minute averages. Concurrent fetching removed the gap between one
chain's end and the next one's start that used to show the host's CPU at work, and the
load says whether the host was busy around the cycle, not within any one second. A load
read that fails is null, and ``cycle_failure`` names it, null when every field was read.
Both kinds carry the same ``v``, so a reader filters on ``kind``. Cycles overlap, so one
cycle's lines can sit between another's, and a reader groups them by ``snap_ts``, never
by position.

The file is ``journal/timing/date=YYYY-MM-DD.jsonl`` under the lake root. The journal
tree is the right home for four reasons that were already true of it.

1. The integrity scrub skips ``journal/``, so the file needs no manifest entry.
2. The nightly backup copies the whole lake, so the timing survives a disk loss.
3. Compaction walks only ``journal/``'s ``date=`` children, and prunes only emptied
   directories under a sealed date, so the file outlives the day's seal.
4. The dashboard reads only the lake, so the Today strip can name the phase that ran
   long (marketlake #538).

Two rules keep the file from ever costing a minute. Every writer appends its lines only
after its segments are durable and its manifest entries are appended. So a cycle that
raises at its manifest append writes neither kind of line. And each line is one
``O_APPEND`` write through ``manifest.append_line``, the same primitive the ledgers use,
so onboarding, which fetches from its own process, can append to the same file without
interleaving. A reader discards a torn trailing line, as the ledger readers do.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from lake.manifest import append_line
from lake.paths import LakePaths
from lake.vendor import RequestTiming

# The line format's version. A reader skips a version it does not know.
TIMING_FORMAT_VERSION = 1

# The ``kind`` of a line naming one vendor request.
REQUEST_KIND = "request"

# The ``kind`` of the line a capture cycle appends after its request lines.
CYCLE_KIND = "cycle"


@dataclass(frozen=True)
class RequestRecord:
    """One vendor request, as capture saw it.

    ``ticker`` names a chain request's underlying and is ``None`` on the batched quote
    request, whose ``symbols`` name every ticker it served. ``window_start`` and
    ``window_end`` are the date range a chain request asked for, the end ``None`` on the
    open tail, and both are ``None`` on a quote request. ``status`` is ``None`` when the
    call raised. ``error_class`` is the class capture recorded for this request, ``None``
    when it recorded none, which includes a too-big window it went on to split.
    ``start`` and ``end`` are the caller's stamps around the call. ``timing`` is what the
    transport saw, ``None`` when it saw nothing. ``failure`` names what went wrong while
    this record was being built, so a reader can tell an unrecorded field from a null one.
    """

    surface: str
    ticker: str | None
    symbols: tuple[str, ...]
    window_start: date | None
    window_end: date | None
    start: datetime
    end: datetime
    status: int | None
    error_class: str | None
    timing: RequestTiming | None = None
    subcode: str | None = None
    error_detail: str | None = None
    failure: str | None = None

    def line(self, snap_ts: datetime) -> dict[str, object]:
        """This request as one line of the timing file, stamped with its cycle's slot."""
        timing = self.timing if self.timing is not None else RequestTiming()
        return {
            "v": TIMING_FORMAT_VERSION,
            "kind": REQUEST_KIND,
            "snap_ts": snap_ts.isoformat(),
            "surface": self.surface,
            "ticker": self.ticker,
            "symbols": list(self.symbols),
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "status": self.status,
            "error_class": self.error_class,
            "request_start_ts": self.start.isoformat(),
            "request_sent_ts": _iso(timing.sent),
            "request_connected_ts": _iso(timing.connected),
            "request_headers_ts": _iso(timing.headers),
            "request_body_ts": _iso(timing.body),
            "request_end_ts": self.end.isoformat(),
            "request_bytes": timing.bytes,
            "request_subcode": self.subcode,
            "request_error_detail": self.error_detail,
            "request_failure": "; ".join(reasons(self)) or None,
        }


def _iso(value: date | datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def timing_path(lake_root: Path | str, day: date | str) -> Path:
    """Where one day's timing lines go."""
    return LakePaths(lake_root).timing_path(day)


def append_requests(
    lake_root: Path | str,
    *,
    snap_ts: datetime,
    day: date,
    records: Iterable[RequestRecord],
) -> Path:
    """Append one line per request to ``day``'s timing file, and return the file's path.

    ``day`` is the date the caller's segments file under, so a day's timing and its
    journal agree on which day a minute belongs to. Each line is its own ``O_APPEND``
    write, so a write that fails partway leaves every earlier line whole. This raises
    what the filesystem raises. Whether a failure is allowed to cost anything is the
    caller's decision, and capture's answer is that it costs the lines and nothing else.
    """
    path = timing_path(lake_root, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    for record in records:
        append_line(path, record.line(snap_ts))
    return path


# A load reading: the 1, 5 and 15 minute averages ``os.getloadavg`` returns.
Load = tuple[float, float, float]


def read_load() -> tuple[Load | None, str | None]:
    """The host's load average, or ``None`` and why it could not be read. Never raises.

    ``os.getloadavg`` raises ``OSError`` when the load is unobtainable, and a cycle reads it
    before its first request, where a raise would leave the cycle with nothing captured.
    So a failure costs the reading and is named rather than raised.
    """
    try:
        return os.getloadavg(), None
    except Exception as exc:  # noqa: BLE001 - a load reading must never cost a minute
        return None, f"load average not read: {type(exc).__name__}: {exc}"


@dataclass(frozen=True)
class CycleRecord:
    """Where one capture cycle's time went, as the cycle itself saw it.

    The module docstring defines each instant. ``fetch_end`` is ``None`` when the cycle
    planned nothing. ``load_start`` and ``load_end`` are ``None`` when the reading failed,
    and ``failures`` names why, in the order they happened.
    """

    snap_ts: datetime
    cycle_start: datetime
    fetch_end: datetime | None
    segments_durable: datetime
    lock_acquired: datetime
    lock_released: datetime
    cycle_end: datetime
    load_start: Load | None
    load_end: Load | None
    failures: tuple[str, ...] = ()

    def line(self) -> dict[str, object]:
        """This cycle as one line of the timing file."""
        return {
            "v": TIMING_FORMAT_VERSION,
            "kind": CYCLE_KIND,
            "snap_ts": self.snap_ts.isoformat(),
            "cycle_start_ts": self.cycle_start.isoformat(),
            "fetch_end_ts": _iso(self.fetch_end),
            "segments_durable_ts": self.segments_durable.isoformat(),
            "lock_acquired_ts": self.lock_acquired.isoformat(),
            "lock_released_ts": self.lock_released.isoformat(),
            "cycle_end_ts": self.cycle_end.isoformat(),
            "loadavg_start": None if self.load_start is None else list(self.load_start),
            "loadavg_end": None if self.load_end is None else list(self.load_end),
            "cycle_failure": "; ".join(self.failures) or None,
        }


def append_cycle(lake_root: Path | str, *, day: date, record: CycleRecord) -> Path:
    """Append one cycle's line to ``day``'s timing file, and return the file's path.

    ``day`` is the date the cycle's segments file under, as for ``append_requests``. The
    line is one ``O_APPEND`` write, so two cycles appending at once each land a whole line.
    This raises what the filesystem raises, and capture decides what a failure costs.
    """
    path = timing_path(lake_root, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    append_line(path, record.line())
    return path


def reasons(record: RequestRecord) -> list[str]:
    """Why one record came out incomplete: its own failure, then its timing's, if any."""
    found = [record.failure, record.timing.failure if record.timing else None]
    return [reason for reason in found if reason is not None]


def failures(records: Sequence[RequestRecord]) -> list[str]:
    """Every distinct reason a record came out incomplete, in the order they first appear."""
    found: list[str] = []
    for record in records:
        for reason in reasons(record):
            if reason not in found:
                found.append(reason)
    return found


__all__ = [
    "CYCLE_KIND",
    "REQUEST_KIND",
    "TIMING_FORMAT_VERSION",
    "CycleRecord",
    "RequestRecord",
    "append_cycle",
    "append_requests",
    "failures",
    "read_load",
    "reasons",
    "timing_path",
]
