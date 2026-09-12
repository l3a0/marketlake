"""Capture spans: the durable record of when each instrument was being captured.

The security master answers "what is this symbol." This file answers "when were we
capturing it." The two are kept apart on purpose. Identity (a rename, an OCC
re-symboling) is a dated event on a mapping row. Capture scope is a set of time
windows, and it can open and close more than once as a ticker is retired and brought
back.

A *capture span* is a half-open window ``[start, end)`` during which an instrument was
being captured. ``start`` is the instant capture began. ``end`` is the instant it
stopped, and it is empty while capture is still running. An instrument has a list of
spans, one per stretch of capture. Onboarding opens a span. Retiring closes it.
Bringing the ticker back opens a new one under the same ``instrument_id``.

Scope is one question asked against this list: an instant is *in scope* for an
instrument when it falls inside one of the instrument's spans. Minutes before the first
span, after a closed span's end, and between two spans are out of scope, never gaps.
This is the same clamping rule ``capture_start`` gave before, widened so a retired or
rejoined ticker is recorded exactly.

Spans store their two ends in UTC. Comparisons against a market-time instant still work
without conversion, because comparing two timezone-aware datetimes normalizes the
offset. So a consumer holding a market-time close can ask ``contains`` directly.

The file lives beside the master, at ``reference/capture_spans.parquet``. It is written
through a temp file and a rename, the same reader-safe way the master and the roster are
written, because the daemon reads it live while a command writes it.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from lake.paths import REFERENCE_DIR, temp_write_path

if TYPE_CHECKING:
    from lake.security_master import SecurityMaster

# The pinned schema version for this reference table. Every row stamps it.
SPANS_SCHEMA_VERSION = 1

# The file's name under ``reference/``, and its lake-relative path for the manifest.
SPANS_FILENAME = "capture_spans.parquet"
SPANS_PARTITION = f"{REFERENCE_DIR}/{SPANS_FILENAME}"

# The pinned pyarrow schema. Both ends are UTC instants. ``span_end`` is nullable, where
# null means the span is still open. ``options`` records whether option chains were
# captured during the span, so a scope reader knows what a span owed without the roster.
SPANS_SCHEMA = pa.schema(
    [
        ("instrument_id", pa.int64()),
        ("span_start", pa.timestamp("us", tz="UTC")),
        ("span_end", pa.timestamp("us", tz="UTC")),
        ("options", pa.bool_()),
        ("schema_version", pa.int32()),
    ]
)


class CaptureSpansError(Exception):
    """Base class for every capture-spans error."""


class NoOpenSpan(CaptureSpansError):
    """Raised when closing an instrument that has no open span to close."""

    def __init__(self, instrument_id: int) -> None:
        super().__init__(f"instrument {instrument_id} has no open capture span to close")
        self.instrument_id = instrument_id


class OpenSpanExists(CaptureSpansError):
    """Raised when opening a span for an instrument that already has one open."""

    def __init__(self, instrument_id: int) -> None:
        super().__init__(f"instrument {instrument_id} already has an open capture span")
        self.instrument_id = instrument_id


class UnsupportedSpansSchemaVersion(CaptureSpansError):
    """Raised when a file on disk carries a schema version this code cannot read."""

    def __init__(self, found: int) -> None:
        super().__init__(
            f"capture-spans schema version {found}, this code reads {SPANS_SCHEMA_VERSION}"
        )
        self.found = found


class SpansUnreadable(CaptureSpansError):
    """Raised when the spans file is present but truncated or not valid parquet.

    A torn write leaves fewer bytes than a whole file. ``pyarrow`` refuses those with
    ``ArrowInvalid``, whose class tree is ``ArrowInvalid -> ValueError``, not a
    ``CaptureSpansError``. ``read`` folds it into this class so a caller guarding the
    module's own errors catches it. An absent file raises ``OSError`` instead, and
    callers guard that beside this class.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(f"capture spans at {path} are not readable parquet")
        self.path = path


def spans_path(lake_root: Path | str) -> Path:
    """The spans file's path under a lake root: ``reference/capture_spans.parquet``."""
    return Path(lake_root) / REFERENCE_DIR / SPANS_FILENAME


def _require_utc(when: datetime, label: str) -> datetime:
    """Reject a naive datetime and normalize an aware one to UTC."""
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return when.astimezone(UTC)


@dataclass(frozen=True)
class CaptureSpan:
    """One capture window for one instrument.

    ``start`` and ``end`` are timezone-aware UTC instants. ``end`` is ``None`` while the
    span is open. ``options`` says whether option chains were captured during the span.
    """

    instrument_id: int
    start: datetime
    end: datetime | None
    options: bool

    def contains(self, instant: datetime) -> bool:
        """Whether ``instant`` falls inside this half-open span ``[start, end)``.

        ``instant`` must be timezone-aware. It may be in any zone; the comparison
        normalizes the offset, so a market-time close compares correctly against UTC
        ends.
        """
        if instant < self.start:
            return False
        return self.end is None or instant < self.end


class CaptureSpans:
    """The in-memory set of capture spans, plus its offline operations.

    Every operation is pure over values. Opening and closing mutate the set. The reads
    answer scope questions. Nothing touches the clock or disk except ``read`` and
    ``write``.
    """

    def __init__(self, spans: Iterable[CaptureSpan] = ()) -> None:
        self._spans: list[CaptureSpan] = list(spans)

    # -- inspection ----------------------------------------------------------

    @property
    def spans(self) -> tuple[CaptureSpan, ...]:
        """The spans, as an immutable snapshot."""
        return tuple(self._spans)

    def __len__(self) -> int:
        return len(self._spans)

    def __iter__(self) -> Iterator[CaptureSpan]:
        return iter(self._spans)

    def instrument_ids(self) -> set[int]:
        """Every ``instrument_id`` that has at least one span."""
        return {s.instrument_id for s in self._spans}

    def spans_of(self, instrument_id: int) -> tuple[CaptureSpan, ...]:
        """Every span for one instrument, in insertion order."""
        return tuple(s for s in self._spans if s.instrument_id == instrument_id)

    def has_open_span(self, instrument_id: int) -> bool:
        """Whether the instrument currently has an open span."""
        return any(s.instrument_id == instrument_id and s.end is None for s in self._spans)

    # -- scope questions -----------------------------------------------------

    def in_scope(self, instrument_id: int, instant: datetime) -> bool:
        """Whether ``instant`` is inside any of the instrument's spans."""
        return any(s.contains(instant) for s in self.spans_of(instrument_id))

    def spans_covering(self, instant: datetime) -> tuple[CaptureSpan, ...]:
        """Every span, across all instruments, that contains ``instant``.

        This is how the close guard and the startup walk find who owed something at a
        moment, including a ticker that has left the roster.
        """
        return tuple(s for s in self._spans if s.contains(instant))

    # -- open and close ------------------------------------------------------

    def open_span(self, instrument_id: int, start: datetime, options: bool) -> None:
        """Open a new capture span for the instrument.

        Raises ``OpenSpanExists`` if the instrument already has an open span, because an
        instrument captures under one window at a time. A rejoin opens a new span only
        after the previous one was closed.
        """
        if self.has_open_span(instrument_id):
            raise OpenSpanExists(instrument_id)
        self._spans.append(
            CaptureSpan(
                instrument_id=instrument_id,
                start=_require_utc(start, "start"),
                end=None,
                options=bool(options),
            )
        )

    def close_span(self, instrument_id: int, end: datetime) -> None:
        """Close the instrument's open span at ``end``.

        Raises ``NoOpenSpan`` if there is no open span, so a double retire is caught.
        ``end`` must be at or after the span's start.
        """
        stamped = _require_utc(end, "end")
        for index, span in enumerate(self._spans):
            if span.instrument_id == instrument_id and span.end is None:
                if stamped < span.start:
                    raise ValueError(
                        f"end {stamped.isoformat()} is before the span's start "
                        f"{span.start.isoformat()}"
                    )
                self._spans[index] = replace(span, end=stamped)
                return
        raise NoOpenSpan(instrument_id)

    # -- parquet round-trip --------------------------------------------------

    def to_table(self) -> pa.Table:
        """Render the spans as a pyarrow table in the pinned schema."""
        return pa.table(
            {
                "instrument_id": [s.instrument_id for s in self._spans],
                "span_start": [s.start for s in self._spans],
                "span_end": [s.end for s in self._spans],
                "options": [s.options for s in self._spans],
                "schema_version": [SPANS_SCHEMA_VERSION] * len(self._spans),
            },
            schema=SPANS_SCHEMA,
        )

    @classmethod
    def from_table(cls, table: pa.Table) -> CaptureSpans:
        """Build a spans set from a pyarrow table in the pinned schema."""
        rows = table.to_pylist()
        for row in rows:
            version = row["schema_version"]
            if version != SPANS_SCHEMA_VERSION:
                raise UnsupportedSpansSchemaVersion(version)
        return cls(
            CaptureSpan(
                instrument_id=row["instrument_id"],
                start=row["span_start"],
                end=row["span_end"],
                options=row["options"],
            )
            for row in rows
        )

    def write(self, path: Path | str) -> Path:
        """Write the spans to parquet at ``path``, through a temp file and a rename.

        A command writes this file while the daemon and the dashboard read it. A write
        straight onto the target truncates it first, so a reader can catch it torn. The
        write goes to a temp file beside the target, flushes, then renames over the
        target in one step. A reader sees the whole old file or the whole new one. The
        master and the roster are written the same way. The temp path comes from
        ``paths.temp_write_path``, which owns the marker the backup exclusion matches.
        Parent dirs are created if absent.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = temp_write_path(path, os.getpid())
        try:
            pq.write_table(self.to_table(), tmp)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return path

    @classmethod
    def read(cls, path: Path | str) -> CaptureSpans:
        """Read a spans set from parquet at ``path``.

        A truncated or torn file raises ``pyarrow``'s ``ArrowInvalid``, folded into
        ``SpansUnreadable`` so a caller guarding ``CaptureSpansError`` catches it. An
        absent file raises ``OSError``, and callers guard that apart, since an absent
        spans file is not a corrupt one.
        """
        path = Path(path)
        try:
            table = pq.read_table(path)
        except pa.ArrowInvalid as exc:
            raise SpansUnreadable(path) from exc
        return cls.from_table(table)


def build_from_master(
    master: SecurityMaster, roster_options: Mapping[str, bool] | None, on: date
) -> CaptureSpans:
    """Seed a spans set from an existing master, one open span per currently-rostered instrument.

    This is the one-shot seed. Before capture spans existed, scope was a single
    ``capture_start`` epoch per instrument, and the only way to stop capturing a ticker
    was to remove its entry from ``tickers.yaml`` by hand. Nothing recorded when that
    happened, so the master alone cannot say whether an instrument it still lists was
    retired years ago or is still live.

    ``roster_options`` distinguishes two different unknowns, and they are handled in
    opposite directions.

    A real, successfully-loaded mapping (possibly empty) is authoritative: an
    instrument whose current ticker is not in it was genuinely removed from
    ``tickers.yaml``, so it is skipped entirely, rather than given an open span.
    Opening one anyway would read as "still capturing," and every reader of the spans
    file, the close guard included, would then treat a long-retired instrument as owed
    forever. Skipping it instead gives it no span at all, out of scope everywhere, the
    correct answer for a ticker no longer captured. The true moment it stopped is lost,
    the same as it always was before this file existed. Every retirement from here on
    is `python -m lake.retire`, which records that moment exactly.

    ``None`` means the roster itself could not be read, not that it named nobody. That
    is not a signal to trust as "nothing is rostered." A missing or broken roster file
    must never cost every instrument its span, so every instrument gets an open one,
    with ``options`` defaulted to false, the same widen-on-a-missing-reference rule
    every other reader of these files follows.

    For an instrument that is kept, the span starts at its ``capture_start``. ``on`` is
    the market date used to resolve each instrument's current ticker.
    """
    spans = CaptureSpans()
    for instrument_id in sorted(master.instrument_ids()):
        ticker = master.symbol_at(instrument_id, on)
        if roster_options is not None and (ticker is None or ticker not in roster_options):
            continue
        options = False if roster_options is None else roster_options.get(ticker, False)
        spans.open_span(instrument_id, master.capture_start_of(instrument_id), options)
    return spans


def spans_of_ticker(
    spans: CaptureSpans | None, master: SecurityMaster | None, ticker: str, on: date
) -> tuple[CaptureSpan, ...] | None:
    """The ticker's capture spans as of ``on``, or ``None`` when scope cannot be read.

    ``None`` means no spans file, no master, an unresolvable ticker, or a refusal. Every
    caller treats that as no clamp, which only ever widens what gets marked or checked.
    This never raises, for the same reason ``capture_start_in_market_time`` does not: the
    daemon runs it from unguarded hooks under ``KeepAlive``.
    """
    if spans is None or master is None:
        return None
    try:
        instrument = master.resolve(ticker, on)
        if instrument is None:
            return None
        return spans.spans_of(instrument)
    except Exception:  # noqa: BLE001 - a reader on an unguarded hook must never raise
        return None
