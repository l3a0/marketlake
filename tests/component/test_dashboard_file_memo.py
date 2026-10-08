"""How often a page refresh reads each journal segment and sealed partition.

Marketlake #700 found that Now, Today and History each read the whole day's journal, so
one refresh read every segment three times. Marketlake #701 found that History re-counted
every sealed partition in its 30-day window every minute, although none had changed.
``FileMemo`` keeps each file's per-slot counts, keyed by the file's identity, and these
tests count the reads rather than reading the code.

Two counters do the counting. ``lake.journal.read_segment`` is wrapped, which counts
segment reads. The connection the service runs on is wrapped too, and every statement
that binds ``partitions`` counts a read of each partition it names, which covers the
window's bulk read and the per-day partition read alike.

The lake holds SPY and QQQ on both minute surfaces, each with three segments today and a
sealed partition on each of the two sessions before it. IWM holds one sealed chains
partition on Friday and nothing today, so Now's walk reaches a partition through the
per-day reader as well. The clock sits mid-session, so today is still unsealed.

Every file a test changes between two requests is published the way production publishes
it: a segment grows through ``journal.SegmentWriter``, and a partition or a repaired
segment is written beside the old one and moved over it with ``os.replace``.
"""

from __future__ import annotations

import os
import threading
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import dashboard, journal
from lake.calendar import MARKET_TZ
from lake.dashboard import DashboardService, FileMemo, open_lake_connection
from lake.paths import LakePaths
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

THURSDAY = date(2026, 8, 20)
FRIDAY = date(2026, 8, 21)
MONDAY = date(2026, 8, 24)

TICKERS = ("SPY", "QQQ")
SURFACES = ("chains", "quotes")
SEGMENTS_PER_TICKER_SURFACE = 3

# The order ``status.html``'s ``refresh`` fires them: history, lake and now together, then
# today once now has answered.
REFRESH_ORDER = ("history", "lake", "now", "today")

# The panels that read segments and partitions. Lake reads only sizes, and its free-space
# figure is the real device's, which moves between two calls, so it is not compared.
READS_FILES = ("history", "now", "today")


def et(day: date, h: int, m: int) -> datetime:
    return datetime(day.year, day.month, day.day, h, m, tzinfo=MARKET_TZ)


CALENDAR = FakeCalendar(
    {
        day: SessionTimes(open=et(day, 9, 30), close=et(day, 16, 0))
        for day in (THURSDAY, FRIDAY, MONDAY)
    }
)
NOW = et(MONDAY, 9, 40)


def _schema(surface: str) -> pa.Schema:
    return journal.CHAINS_SCHEMA if surface == "chains" else journal.QUOTES_SCHEMA


def _row(surface: str, ticker: str, snap: datetime | str, **fields: Any) -> dict:
    """A pinned-schema row, with ``snap_ts`` stamped in UTC the way capture stamps it."""
    row: dict[str, Any] = dict.fromkeys(_schema(surface).names)
    row.update(
        snap_ts=snap if isinstance(snap, str) else snap.astimezone(UTC).isoformat(),
        ticker=ticker,
        row_kind=journal.ROW_KIND_DATA,
        suspect=False,
        schema_version=journal.SCHEMA_VERSION,
    )
    row.update(fields)
    return row


def _segment_table(surface: str, rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=_schema(surface))


def _sealed(surface: str, ticker: str, *snaps: datetime) -> pa.Table:
    rows = [
        {
            "snap_ts": snap.isoformat(),
            "ticker": ticker,
            "row_kind": journal.ROW_KIND_DATA,
            "suspect": False,
            "schema_version": journal.SCHEMA_VERSION,
        }
        for snap in snaps
    ]
    return sample_chains_table(rows) if surface == "chains" else sample_quotes_table(rows)


def _start_ts(snap: datetime) -> str:
    return snap.astimezone(UTC).strftime("%Y%m%dT%H%M%S%f")


def build_lake(fixture_lake: FixtureLake) -> tuple[Path, list[Path], list[Path]]:
    """Today's segments for every ticker-surface, and the sealed days behind them."""
    segments: list[Path] = []
    for ticker in TICKERS:
        for surface in SURFACES:
            for minute in range(SEGMENTS_PER_TICKER_SURFACE):
                snap = et(MONDAY, 9, 30 + minute)
                fixture_lake.with_journal_segment(
                    surface,
                    ticker,
                    MONDAY,
                    _segment_table(surface, [_row(surface, ticker, snap)]),
                    start_ts=_start_ts(snap),
                    pid=1,
                )
                segments.append(
                    fixture_lake.segment_path(surface, ticker, MONDAY, _start_ts(snap), 1)
                )
            for day in (THURSDAY, FRIDAY):
                fixture_lake.with_partition(
                    surface, ticker, day, _sealed(surface, ticker, et(day, 9, 30))
                )
    fixture_lake.with_partition(
        "chains", "IWM", FRIDAY, _sealed("chains", "IWM", et(FRIDAY, 9, 30))
    )
    root = fixture_lake.build().resolve()
    partitions = sorted(root.glob("*/ticker=*/*.parquet"))
    return root, [path.resolve() for path in segments], partitions


# -- counting reads -----------------------------------------------------------


# Called with the partitions a statement read, once the statement has run.
AfterRead = Callable[[list[Path]], None]


class _CountingCursor:
    """A cursor that counts every partition a statement binds, then runs it as it was."""

    def __init__(
        self, cursor: duckdb.DuckDBPyConnection, reads: Counter, after: AfterRead | None
    ) -> None:
        self._cursor = cursor
        self._reads = reads
        self._after = after

    def execute(self, sql: str, params: Any = None) -> duckdb.DuckDBPyConnection:
        named: list[Path] = []
        if isinstance(params, dict):
            named = [Path(name).resolve() for name in params.get("partitions", ())]
            for path in named:
                self._reads[path] += 1
        result = self._cursor.execute(sql, params)
        if self._after is not None and named:
            self._after(named)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _CountingConnection:
    """The service's sandboxed connection, handing out counting cursors."""

    def __init__(self, root: Path, reads: Counter, after: AfterRead | None = None) -> None:
        self._con = open_lake_connection(root)
        self._reads = reads
        self._after = after

    def cursor(self) -> _CountingCursor:
        return _CountingCursor(self._con.cursor(), self._reads, self._after)


@pytest.fixture
def segment_reads(monkeypatch) -> Counter:
    """Every ``read_segment`` call, keyed by the resolved path it read."""
    calls: Counter = Counter()
    real = journal.read_segment

    def counting(path):
        calls[Path(path).resolve()] += 1
        return real(path)

    # The dashboard calls it as ``journal.read_segment`` through the module object, so
    # patching the module attribute reaches every call site in ``lake.dashboard``.
    monkeypatch.setattr(dashboard.journal, "read_segment", counting)
    return calls


@pytest.fixture
def partition_reads() -> Counter:
    return Counter()


def service_over(
    root: Path,
    partition_reads: Counter,
    memo: FileMemo | None = None,
    after: AfterRead | None = None,
):
    return DashboardService(
        root,
        clock=ManualClock(NOW.astimezone(UTC)),
        calendar=CALENDAR,
        connection=_CountingConnection(root, partition_reads, after),  # type: ignore[arg-type]
        page=b"<!doctype html>",
        icon=b"",
        memo=memo,
    )


def refresh(service: DashboardService) -> dict[str, dict[str, object]]:
    """One page refresh: the four queries, in the order the page fires them."""
    return {name: service.run_query(name, {}) for name in REFRESH_ORDER}


def _strip(payload: dict, ticker: str, surface: str) -> dict:
    return next(s for s in payload["strips"] if (s["ticker"], s["surface"]) == (ticker, surface))


def _slot(strip: dict, snap: datetime) -> dict:
    return next(cell for cell in strip["slots"] if cell["slot"] == snap.isoformat())


def _cell(payload: dict, ticker: str, surface: str, day: date) -> dict:
    return next(
        cell
        for cell in payload["cells"]
        if (cell["ticker"], cell["surface"], cell["date"]) == (ticker, surface, day.isoformat())
    )


def _publish_partition(path: Path, table: pa.Table) -> None:
    """Write a partition beside the old one and move it over, as compaction does."""
    staged = path.with_name(path.name + ".staged")
    pq.write_table(table, staged)
    os.replace(staged, path)


def _publish_segment(path: Path, table: pa.Table) -> None:
    """Write a whole segment beside the old one and move it over, as a restore does."""
    staged = path.with_name(path.name + ".staged")
    with pa.OSFile(str(staged), "wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    os.replace(staged, path)


# -- one read per file --------------------------------------------------------


def test_one_refresh_reads_each_segment_and_each_partition_once(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, segments, partitions = build_lake(fixture_lake)
    service = service_over(root, partition_reads)
    per_query: dict[str, Counter] = {}
    for name in REFRESH_ORDER:
        before = Counter(segment_reads)
        service.run_query(name, {})
        per_query[name] = segment_reads - before
    # History runs first and reads every segment. Lake opens none, and Now and Today
    # find every segment already counted. Before the memo each of the three read all.
    assert segment_reads == Counter(dict.fromkeys(segments, 1))
    assert per_query["history"] == Counter(dict.fromkeys(segments, 1))
    for name in ("lake", "now", "today"):
        assert per_query[name] == Counter(), name
    # Every sealed partition is read once too, including IWM's Friday, which History's
    # bulk read counts and Now's walk then finds remembered.
    assert len(partitions) == 9
    assert partition_reads == Counter(dict.fromkeys(partitions, 1))


def test_a_second_refresh_reads_nothing_and_answers_as_a_fresh_service_would(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, _, _ = build_lake(fixture_lake)
    service = service_over(root, partition_reads)
    refresh(service)
    segment_reads.clear()
    partition_reads.clear()
    remembered = refresh(service)
    assert segment_reads == Counter()
    assert partition_reads == Counter()
    # The remembered answer is the answer a service with nothing remembered gives.
    fresh = refresh(service_over(root, Counter()))
    for name in READS_FILES:
        assert remembered[name] == fresh[name], name
    assert _slot(_strip(remembered["today"], "SPY", "chains"), et(MONDAY, 9, 32))["rows"] == 1
    assert _cell(remembered["history"], "IWM", "chains", FRIDAY)["counts"]["captured"] == 1


def test_a_segment_the_writer_appends_to_is_read_again_and_its_new_minute_shows(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, segments, _ = build_lake(fixture_lake)
    writer = journal.SegmentWriter.open(
        root, "chains", "SPY", MONDAY, _start_ts(et(MONDAY, 9, 33)), 1
    )
    try:
        writer.write_cycle(_segment_table("chains", [_row("chains", "SPY", et(MONDAY, 9, 33))]))
        service = service_over(root, partition_reads)
        first = refresh(service)
        assert _slot(_strip(first["today"], "SPY", "chains"), et(MONDAY, 9, 34))["status"] == (
            "missing"
        )
        segment_reads.clear()
        partition_reads.clear()
        writer.write_cycle(_segment_table("chains", [_row("chains", "SPY", et(MONDAY, 9, 34))]))
        second = refresh(service)
    finally:
        writer.close()
    # The open segment grew, so its size moved and it is read again, once. Nothing else is.
    assert segment_reads == Counter({writer.path.resolve(): 1})
    assert partition_reads == Counter()
    strip = _strip(second["today"], "SPY", "chains")
    assert _slot(strip, et(MONDAY, 9, 34))["status"] == "captured"
    assert _slot(strip, et(MONDAY, 9, 33))["status"] == "captured"
    assert writer.path.resolve() not in segments


def test_a_partition_replaced_between_refreshes_changes_the_answer(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, _, _ = build_lake(fixture_lake)
    service = service_over(root, partition_reads)
    first = refresh(service)
    assert _cell(first["history"], "QQQ", "chains", THURSDAY)["counts"]["captured"] == 1
    partition = LakePaths(root).partition_path("chains", "QQQ", THURSDAY)
    _publish_partition(
        partition, _sealed("chains", "QQQ", et(THURSDAY, 9, 30), et(THURSDAY, 9, 31))
    )
    segment_reads.clear()
    partition_reads.clear()
    second = refresh(service)
    assert partition_reads == Counter({partition: 1})
    assert segment_reads == Counter()
    assert _cell(second["history"], "QQQ", "chains", THURSDAY)["counts"]["captured"] == 2


# -- the key -------------------------------------------------------------------

KEY_FIELDS = ("st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")


class _Stat:
    """A real ``stat`` result with some fields replaced."""

    def __init__(self, real: os.stat_result, fields: dict[str, int]) -> None:
        self._real = real
        self._fields = fields

    def __getattr__(self, name: str) -> Any:
        if name in self._fields:
            return self._fields[name]
        return getattr(self._real, name)


def _stat_reporting(monkeypatch, target: Path, fields: dict[str, int]) -> None:
    """Make every ``os.stat`` of ``target`` report ``fields`` in place of the real ones."""
    real_stat = os.stat

    def stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if isinstance(path, (str, os.PathLike)) and Path(path) == target:
            return _Stat(result, fields)
        return result

    monkeypatch.setattr(os, "stat", stat)


def _two_minutes_where_one_was(root: Path, kind: str) -> tuple[Path, Callable[[], None]]:
    """A file of one kind, and how to replace it with a version holding a second minute."""
    paths = LakePaths(root)
    if kind == "partition":
        partition = paths.partition_path("chains", "IWM", FRIDAY)
        return partition, lambda: _publish_partition(
            partition, _sealed("chains", "IWM", et(FRIDAY, 9, 30), et(FRIDAY, 9, 31))
        )
    segment = paths.segment_path("chains", "SPY", MONDAY, _start_ts(et(MONDAY, 9, 30)), 1)
    rows = [_row("chains", "SPY", et(MONDAY, 9, 30)), _row("chains", "SPY", et(MONDAY, 9, 35))]
    return segment, lambda: _publish_segment(segment, _segment_table("chains", rows))


def _second_minute_rows(service: DashboardService, kind: str) -> int:
    if kind == "partition":
        payload = service.run_query("history", {})
        return _cell(payload, "IWM", "chains", FRIDAY)["counts"]["captured"] - 1
    strip = _strip(service.run_query("today", {}), "SPY", "chains")
    return _slot(strip, et(MONDAY, 9, 35))["rows"]


@pytest.mark.parametrize("kind", ["partition", "segment"])
@pytest.mark.parametrize("moved", [*KEY_FIELDS, None])
def test_each_field_of_the_key_alone_sends_a_changed_file_to_be_read_again(
    fixture_lake: FixtureLake, partition_reads: Counter, monkeypatch, kind: str, moved: str | None
):
    # The file is replaced with one holding a second minute, and ``stat`` is made to
    # report the old file's identity with at most one field moved. With one field moved
    # the new minute must show, so that field is in the key. With none moved the memo
    # still answers with the old file, which is what proves the ``stat`` the memo reads is
    # the one this test controls.
    root, _, _ = build_lake(fixture_lake)
    path, replace_file = _two_minutes_where_one_was(root, kind)
    service = service_over(root, partition_reads)
    old = os.stat(path)
    assert _second_minute_rows(service, kind) == 0
    replace_file()
    reported = {name: getattr(old, name) for name in KEY_FIELDS}
    if moved is not None:
        reported[moved] += 1
    _stat_reporting(monkeypatch, path, reported)
    assert _second_minute_rows(service, kind) == (0 if moved is None else 1)


def test_the_identity_is_taken_before_the_segment_is_read(
    fixture_lake: FixtureLake, partition_reads: Counter, monkeypatch
):
    # The daemon appends to the segment it holds open, so an append can land between the
    # read and anything after it. Here one lands just after the first read. The identity
    # taken before the read names the shorter file, so the next request sees the size move
    # and reads the new minute. An identity taken after the read would name the longer
    # file, and the memo would answer for it with the shorter file's counts from then on.
    root, _, _ = build_lake(fixture_lake)
    writer = journal.SegmentWriter.open(
        root, "chains", "SPY", MONDAY, _start_ts(et(MONDAY, 9, 33)), 1
    )
    real = journal.read_segment
    appended: list[bool] = []

    def read_then_append(path):
        table = real(path)
        if Path(path) == writer.path and not appended:
            appended.append(True)
            writer.write_cycle(_segment_table("chains", [_row("chains", "SPY", et(MONDAY, 9, 34))]))
        return table

    monkeypatch.setattr(dashboard.journal, "read_segment", read_then_append)
    try:
        writer.write_cycle(_segment_table("chains", [_row("chains", "SPY", et(MONDAY, 9, 33))]))
        service = service_over(root, partition_reads)
        first = _strip(service.run_query("today", {}), "SPY", "chains")
        second = _strip(service.run_query("today", {}), "SPY", "chains")
    finally:
        writer.close()
    assert appended == [True]
    assert _slot(first, et(MONDAY, 9, 34))["status"] == "missing"
    assert _slot(second, et(MONDAY, 9, 34))["status"] == "captured"


@pytest.mark.parametrize("query", ["history", "today"])
def test_the_identity_is_taken_before_the_partition_is_read(
    fixture_lake: FixtureLake, partition_reads: Counter, query: str
):
    # A repair can publish a new partition the moment after a read of the old one. The
    # identity taken before the read names the old file, so the next request sees a new
    # inode and reads the repair. An identity taken after would name the repair, and the
    # memo would answer for it with the old file's counts. History reaches the file
    # through the window's bulk read, and Today, asked for the sealed day, through the
    # per-day read.
    root, _, _ = build_lake(fixture_lake)
    partition = LakePaths(root).partition_path("chains", "QQQ", THURSDAY)
    replaced: list[bool] = []

    def replace_after_read(read: list[Path]) -> None:
        if partition in read and not replaced:
            replaced.append(True)
            _publish_partition(
                partition, _sealed("chains", "QQQ", et(THURSDAY, 9, 30), et(THURSDAY, 9, 31))
            )

    service = service_over(root, partition_reads, after=replace_after_read)
    raw = {} if query == "history" else {"date": THURSDAY.isoformat(), "ticker": "QQQ"}

    def captured() -> int:
        payload = service.run_query(query, raw)
        if query == "history":
            return _cell(payload, "QQQ", "chains", THURSDAY)["counts"]["captured"]
        return _strip(payload, "QQQ", "chains")["counts"]["captured"]

    assert captured() == 1
    assert replaced == [True]
    assert captured() == 2


# -- failures are never remembered ------------------------------------------------


def test_a_corrupt_segment_is_read_and_counted_on_every_request(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, _, _ = build_lake(fixture_lake)
    garbage = LakePaths(root).segment_path("chains", "SPY", MONDAY, "20260824T134500000000", 9)
    garbage.write_bytes(b"not an arrow stream")
    service = service_over(root, partition_reads)
    first = _strip(service.run_query("today", {}), "SPY", "chains")
    second = _strip(service.run_query("today", {}), "SPY", "chains")
    assert first["unreadable_segments"] == 1
    assert second["unreadable_segments"] == 1
    assert segment_reads[garbage.resolve()] == 2


def test_a_segment_that_failed_to_read_once_is_read_again_though_nothing_changed(
    fixture_lake: FixtureLake, partition_reads: Counter, monkeypatch
):
    # A failure can be transient, and the file it failed on has not changed, so its
    # identity is the same on the next request. A remembered failure would hide the
    # minute for as long as the file stayed as it is.
    root, _, _ = build_lake(fixture_lake)
    target = LakePaths(root).segment_path("chains", "SPY", MONDAY, _start_ts(et(MONDAY, 9, 31)), 1)
    real = journal.read_segment
    failed: list[bool] = []

    def fail_once(path):
        if Path(path) == target and not failed:
            failed.append(True)
            raise PermissionError(13, "Permission denied", str(path))
        return real(path)

    monkeypatch.setattr(dashboard.journal, "read_segment", fail_once)
    service = service_over(root, partition_reads)
    first = _strip(service.run_query("today", {}), "SPY", "chains")
    second = _strip(service.run_query("today", {}), "SPY", "chains")
    assert first["unreadable_segments"] == 1
    assert _slot(first, et(MONDAY, 9, 31))["status"] == "missing"
    assert second["unreadable_segments"] == 0
    assert _slot(second, et(MONDAY, 9, 31))["status"] == "captured"


def test_a_corrupt_segment_repaired_by_replace_is_read_and_counted_correctly(
    fixture_lake: FixtureLake, segment_reads: Counter, partition_reads: Counter
):
    root, _, _ = build_lake(fixture_lake)
    segment = (
        LakePaths(root).segment_path("chains", "SPY", MONDAY, "20260824T134500000000", 9).resolve()
    )
    segment.write_bytes(b"not an arrow stream")
    service = service_over(root, partition_reads)
    first = _strip(service.run_query("today", {}), "SPY", "chains")
    assert first["unreadable_segments"] == 1
    _publish_segment(segment, _segment_table("chains", [_row("chains", "SPY", et(MONDAY, 9, 36))]))
    second = _strip(service.run_query("today", {}), "SPY", "chains")
    assert second["unreadable_segments"] == 0
    assert _slot(second, et(MONDAY, 9, 36))["status"] == "captured"
    assert segment_reads[segment] == 2


@pytest.mark.parametrize("query", ["history", "today"])
def test_an_unreadable_partition_is_read_and_counted_on_every_request(
    fixture_lake: FixtureLake, partition_reads: Counter, query: str
):
    # History reaches the file through the window's bulk read and its per-day fallback.
    # Today, asked for the sealed day, reaches it through the per-day read alone.
    root, _, _ = build_lake(fixture_lake)
    partition = LakePaths(root).partition_path("chains", "QQQ", THURSDAY)
    partition.write_bytes(b"not parquet at all")
    service = service_over(root, partition_reads)
    raw = {} if query == "history" else {"date": THURSDAY.isoformat(), "ticker": "QQQ"}

    def unreadable() -> int:
        payload = service.run_query(query, raw)
        if query == "history":
            return _cell(payload, "QQQ", "chains", THURSDAY)["unreadable_partitions"]
        return _strip(payload, "QQQ", "chains")["unreadable_partitions"]

    assert unreadable() == 1
    reads = partition_reads[partition]
    assert unreadable() == 1
    assert partition_reads[partition] > reads


# -- merging one day's files ------------------------------------------------------


def test_a_minute_split_across_files_merges_as_one_statement_over_them_would(
    fixture_lake: FixtureLake, partition_reads: Counter
):
    # One minute's rows sit in two segments and the day's partition, which is the few
    # seconds a seal leaves both copies. Each file is counted on its own and the counts
    # are merged, so every column has to merge as one statement over all three would.
    # The first segment's rows are not suspect and the second's are, so ``suspect`` must be
    # the OR. Each file carries a row whose stamp will not cast and a row of a kind the
    # panels do not know, and both counts must add across files rather than keep one. The
    # next minute holds a gap row in the first segment and only an unknown kind in the
    # second, so it is a gap only if the gap count survives the merge.
    minute = et(MONDAY, 9, 30)
    gap_minute = et(MONDAY, 9, 31)
    first = [
        _row("chains", "SPY", minute, error_class="b_class"),
        _row("chains", "SPY", "not a time"),
        _row("chains", "SPY", minute, row_kind="weird"),
        _row("chains", "SPY", gap_minute, row_kind=journal.ROW_KIND_GAP, error_class="c_class"),
    ]
    second = [
        _row("chains", "SPY", minute, suspect=True, error_class="a_class"),
        _row("chains", "SPY", minute, error_class="b_class"),
        _row("chains", "SPY", "also not a time"),
        _row("chains", "SPY", minute, row_kind="weird"),
        _row("chains", "SPY", gap_minute, row_kind="weird"),
    ]
    for start, rows in (("20260824T133000000000", first), ("20260824T133000500000", second)):
        fixture_lake.with_journal_segment(
            "chains", "SPY", MONDAY, _segment_table("chains", rows), start_ts=start, pid=1
        )
    fixture_lake.with_partition("chains", "SPY", MONDAY, _sealed("chains", "SPY", minute))
    root = fixture_lake.build().resolve()
    service = service_over(root, partition_reads)
    for _ in range(2):  # once read, once remembered
        strip = _strip(service.run_query("today", {}), "SPY", "chains")
        slot = _slot(strip, minute)
        assert slot["status"] == "suspect"
        assert slot["rows"] == 4
        assert slot["error_class"] == ["a_class", "b_class"]
        assert strip["unparseable_stamp_rows"] == 2
        assert strip["drifted_rows"] == 3
        gap = _slot(strip, gap_minute)
        assert gap["status"] == "gap"
        assert gap["error_class"] == ["c_class"]


# -- eviction --------------------------------------------------------------------


class _Monotonic:
    """A monotonic time a test moves by hand."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, span: timedelta) -> None:
        self.now += span.total_seconds()


def test_an_entry_unused_past_the_span_is_dropped_on_the_next_write():
    clock = _Monotonic()
    memo = FileMemo(monotonic=clock)
    identity = (1, 2, 3, 4)
    groups = ((0, 1, 0, 0, False, ()),)
    memo.put([("a", identity, groups)])
    clock.advance(timedelta(minutes=9))
    memo.put([("b", identity, groups)])
    # Nine minutes unused is inside the span, so the write keeps it.
    assert memo.get("a", identity) == groups
    clock.advance(timedelta(minutes=9))
    memo.put([("c", identity, groups)])
    # ``a`` was used nine minutes ago, by the get above, and ``b`` written then.
    assert len(memo) == 3
    clock.advance(timedelta(minutes=2))
    # Nothing is dropped until something is written.
    assert len(memo) == 3
    memo.put([("d", identity, groups)])
    # Eleven minutes unused is past the span, so the write drops ``a`` and ``b``.
    assert memo.get("a", identity) is None
    assert memo.get("b", identity) is None
    assert memo.get("c", identity) == groups
    assert len(memo) == 2


def test_a_dropped_file_is_read_again_and_a_used_one_is_not(
    fixture_lake: FixtureLake, partition_reads: Counter
):
    root, _, _ = build_lake(fixture_lake)
    clock = _Monotonic()
    service = service_over(root, partition_reads, memo=FileMemo(monotonic=clock))
    thursday = {"date": THURSDAY.isoformat(), "ticker": "QQQ"}
    friday = {"date": FRIDAY.isoformat(), "ticker": "QQQ"}
    paths = LakePaths(root)
    thursday_file = paths.partition_path("chains", "QQQ", THURSDAY)
    service.run_query("today", thursday)
    clock.advance(timedelta(minutes=11))
    service.run_query("today", friday)  # a write, eleven minutes after Thursday's last use
    service.run_query("today", thursday)
    assert partition_reads[thursday_file] == 2
    clock.advance(timedelta(minutes=9))
    service.run_query("today", {"date": MONDAY.isoformat(), "ticker": "QQQ"})  # a write
    service.run_query("today", thursday)
    assert partition_reads[thursday_file] == 2


# -- concurrency -----------------------------------------------------------------


@pytest.mark.parametrize("second", ["put", "get"])
def test_no_access_to_the_memo_runs_inside_a_write(second: str):
    # The server answers each request on its own thread. The time source is read inside
    # every write, so a time source that starts a second access and waits for it shows
    # whether the write still excludes it. With the lock the second access waits for the
    # write to finish. Without it the second runs to the end inside the first.
    memo = FileMemo(monotonic=lambda: inner())
    identity = (1, 2, 3, 4)
    groups: tuple = ()
    other: list[threading.Thread] = []
    finished_inside: list[bool] = []
    access: Callable[[], object] = (
        (lambda: memo.put([("b", identity, groups)]))
        if second == "put"
        else (lambda: memo.get("a", identity))
    )

    def inner() -> float:
        if not other:
            thread = threading.Thread(target=access)
            other.append(thread)
            thread.start()
            thread.join(timeout=0.5)
            finished_inside.append(not thread.is_alive())
        return 0.0

    memo.put([("a", identity, groups)])
    other[0].join(timeout=5)
    assert finished_inside == [False]
    assert not other[0].is_alive()
    assert len(memo) == (2 if second == "put" else 1)


def test_panels_refreshing_together_answer_as_they_do_one_at_a_time(
    fixture_lake: FixtureLake, partition_reads: Counter
):
    # ``status.html`` fires History, Lake and Now at once, and a second tab fires its own
    # set beside them. Every one of them must read the shared memo as if it were alone.
    root, _, _ = build_lake(fixture_lake)
    expected = refresh(service_over(root, Counter()))
    service = service_over(root, partition_reads)
    answers: list[tuple[str, dict]] = []
    errors: list[BaseException] = []

    def run(name: str) -> None:
        try:
            answers.append((name, service.run_query(name, {})))
        except BaseException as error:  # noqa: BLE001 - reported by the assert below
            errors.append(error)

    threads = [threading.Thread(target=run, args=(name,)) for name in REFRESH_ORDER * 3]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert errors == []
    assert len(answers) == len(threads)
    for name, payload in answers:
        if name in READS_FILES:
            assert payload == expected[name], name
