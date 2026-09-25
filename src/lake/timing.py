"""The request timing file: one line per vendor request, so a slow minute can be taken apart.

A capture cycle makes one request per chain window and one for the batched quotes. The
rows a cycle writes carry one ``fetch_ts`` and one ``fetch_end_ts`` per ticker-minute, so
a chain whose nine windows took 70 seconds cannot say whether one window was slow or all
nine were, or whether the time went to Schwab or to the network. On 2026-09-24 three
cycles ran past their minute and lost four slots, and nothing the lake held could say
which. Marketlake #531 is that gap, and this file is where its evidence lands.

Each line is one JSON object naming one request. It carries the request's own
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
429's sub-code, and ``request_error_detail``, a bounded copy of the reply. Every line
carries ``v``, the line format's version, because nothing like ``schema_version`` covers a
JSON file, and ``kind``, which is ``request`` here.

The file is ``journal/timing/date=YYYY-MM-DD.jsonl`` under the lake root. The journal
tree is the right home for four reasons that were already true of it.

1. The integrity scrub skips ``journal/``, so the file needs no manifest entry.
2. The nightly backup copies the whole lake, so the timing survives a disk loss.
3. Compaction walks only ``journal/``'s ``date=`` children, and prunes only emptied
   directories under a sealed date, so the file outlives the day's seal.
4. The dashboard reads only the lake, so the Today strip can name the phase that ran
   long (marketlake #538).

Two rules keep the file from ever costing a minute. A cycle appends its lines only after
its segments are durable and its manifest entries are appended. And each line is one
``O_APPEND`` write through ``manifest.append_line``, the same primitive the ledgers use,
so onboarding, which fetches from its own process, can append to the same file without
interleaving. A reader discards a torn trailing line, as the ledger readers do.
"""

from __future__ import annotations

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


def failures(records: Sequence[RequestRecord]) -> list[str]:
    """Every distinct reason a record came out incomplete, in the order they first appear."""
    found: list[str] = []
    for record in records:
        for reason in (record.failure, record.timing.failure if record.timing else None):
            if reason is not None and reason not in found:
                found.append(reason)
    return found


__all__ = [
    "REQUEST_KIND",
    "TIMING_FORMAT_VERSION",
    "RequestRecord",
    "append_requests",
    "failures",
    "timing_path",
]
