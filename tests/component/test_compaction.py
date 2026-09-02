"""The close+15 compaction job, over real files with a fake clock and calendar.

These build real journal segments on disk, run ``compact`` with a manual clock, a
declared calendar, and fake backup and ping seams, then read the lake back. No network,
no subprocess, and no wall clock are crossed. So the tier is component: one subsystem,
compaction, over the real filesystem.

They pin the job's contract:

1. A day of segments compacts to one Parquet per surface and ticker, rows equal to the
   sum, a manifest entry carrying the sha, the segments deleted. A second run no-ops.
2. A torn tail compacts to its complete batches. Bytes after an end-of-stream marker
   fail loudly and seal nothing for that ticker-day.
3. An orphaned segment from an older date is swept and sealed.
4. A ticker-day whose close+5 has not passed is never touched.
5. A manifested partition is sha-verified, its debris deleted, and never rewritten. A
   mismatch raises. The no-shrink guard refuses a recompaction with fewer rows.
6. Backup runs after the seal. An unmounted target raises before any ping. A success
   pings the ``compaction`` slug once. An empty journal no-ops and still pings.
7. The re-tune splits, merges, preserves the open tail, writes a plan that parses back,
   and leaves an unchanged profile's file untouched.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import journal
from lake.calendar import MARKET_TZ
from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan, load_chain_plan
from lake.compact import (
    COMPACTION_SLUG,
    CompactionResult,
    PartitionMismatch,
    RecompactionRefused,
    SkippedDay,
    build_parser,
    compact,
    main,
    recompact_ticker_day,
)
from lake.compact import write_chain_plan as _write_plan
from lake.journal import CHAINS_SCHEMA, QUOTES_SCHEMA, ShadowAppendError
from lake.manifest import (
    RowCountRegression,
    latest_entries,
    manifest_path,
    read_manifest,
    sha256_file,
)
from lake.paths import LakePaths
from lake.runner import BackupTargetUnavailable, RsyncBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake

FRIDAY = date(2026, 8, 21)
DAY = date(2026, 8, 24)
TUESDAY = date(2026, 8, 25)
SATURDAY = date(2026, 8, 22)

URL = "https://hc-ping.com/secret-key/compaction"
TARGET = Path("/ssd/lake")
PID = 4242


# -- the seams ---------------------------------------------------------------


def _et(day: date, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=MARKET_TZ)


def _session(day: date) -> SessionTimes:
    return SessionTimes(open=_et(day, 9, 30), close=_et(day, 16, 0))


def _calendar() -> FakeCalendar:
    return FakeCalendar({day: _session(day) for day in (FRIDAY, DAY, TUESDAY)})


def _clock_at(day: date, hour: int, minute: int, second: int = 0) -> ManualClock:
    return ManualClock(_et(day, hour, minute, second))


class FakePinger:
    """Records each ping into a shared event log."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)
        self.events.append("ping")


class FakeBackup:
    """Records each sync, plus the lake's file listing at the moment it ran."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[Path, Path]] = []
        self.seen: list[list[str]] = []

    def sync(self, source: Path, target: Path) -> None:
        self.calls.append((source, target))
        self.seen.append(
            sorted(p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file())
        )
        self.events.append("backup")


def _run(
    lake_root: Path,
    *,
    clock: ManualClock | None = None,
    calendar: FakeCalendar | None = None,
    backup=None,
    pinger=None,
    plan_path: Path | None = None,
) -> tuple[CompactionResult, list[str], FakeBackup, FakePinger]:
    events: list[str] = []
    backup = backup if backup is not None else FakeBackup(events)
    pinger = pinger if pinger is not None else FakePinger(events)
    result = compact(
        lake_root,
        clock=clock if clock is not None else _clock_at(DAY, 16, 30),
        calendar=calendar if calendar is not None else _calendar(),
        backup=backup,
        backup_target=TARGET,
        pinger=pinger,
        ping_url=URL,
        # Never the real machine-owned file. Every test points the plan at its temp dir.
        plan_path=plan_path if plan_path is not None else lake_root.parent / "chain_plan.json",
    )
    return result, events, backup, pinger


# -- building rows and segments ----------------------------------------------


def _table(schema: pa.Schema, rows: list[dict]) -> pa.Table:
    """Rows in a pinned capture schema, every unnamed column null."""
    arrays = [pa.array([row.get(f.name) for row in rows], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _snap(day: date, minute: int) -> str:
    return (_et(day, 9, 30) + timedelta(minutes=minute)).isoformat()


def _chains_rows(
    count: int,
    *,
    snap_ts: str,
    ticker: str = "SPY",
    window: tuple[str | None, str | None] = (None, None),
    row_kind: str = "data",
) -> list[dict]:
    start, end = window
    return [
        {
            "snap_ts": snap_ts,
            "fetch_ts": snap_ts,
            "ticker": ticker,
            "occ_symbol": f"{ticker} {snap_ts} {index}",
            "row_kind": row_kind,
            "suspect": False,
            "schema_version": 1,
            "window_start": start,
            "window_end": end,
        }
        for index in range(count)
    ]


def _chains(count: int, **kwargs) -> pa.Table:
    return _table(CHAINS_SCHEMA, _chains_rows(count, **kwargs))


def _quotes(count: int, *, snap_ts: str, ticker: str = "SPY") -> pa.Table:
    rows = [
        {
            "snap_ts": snap_ts,
            "fetch_ts": snap_ts,
            "ticker": ticker,
            "bid": 650.0 + index,
            "row_kind": "data",
            "suspect": False,
            "schema_version": 1,
        }
        for index in range(count)
    ]
    return _table(QUOTES_SCHEMA, rows)


def _segment(
    lake_root: Path,
    surface: str,
    ticker: str,
    day: date,
    *tables: pa.Table,
    start_ts: str,
    pid: int = PID,
) -> Path:
    """Write one closed segment, one batch per table, the way the capture loop does."""
    with journal.SegmentWriter.open(lake_root, surface, ticker, day, start_ts, pid) as writer:
        for table in tables:
            writer.write_cycle(table)
    return journal.segment_path(lake_root, surface, ticker, day, start_ts, pid)


def _stream_bytes(tables: list[pa.Table]) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, CHAINS_SCHEMA) as writer:
        for table in tables:
            writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _iso_windows(plan: ChainPlan, day: date) -> list[tuple[str, str | None]]:
    """The plan's windows for ``day`` as the ISO bounds a captured row carries."""
    return [
        (start.isoformat(), None if end is None else end.isoformat())
        for start, end in plan.windows_for(day)
    ]


def _profile_table(
    plan: ChainPlan, day: date, counts: dict[int, int], *, snap_ts: str, ticker: str = "SPY"
) -> pa.Table:
    """One cycle's chains rows with ``counts[index]`` contracts in the plan's window."""
    rows: list[dict] = []
    for index, window in enumerate(_iso_windows(plan, day)):
        rows.extend(
            _chains_rows(counts.get(index, 0), snap_ts=snap_ts, ticker=ticker, window=window)
        )
    return _table(CHAINS_SCHEMA, rows)


def _steady_day(lake_root: Path, day: date = DAY, *, ticker: str = "SPY") -> Path:
    """A one-cycle chains segment whose profile sits inside the re-tune band."""
    steady = {0: 1000, 1: 1000, 2: 1000, 3: 1000, 4: 1000}
    table = _profile_table(DEFAULT_CHAIN_PLAN, day, steady, snap_ts=_snap(day, 0), ticker=ticker)
    return _segment(lake_root, "chains", ticker, day, table, start_ts="20260824T093000000000")


def _rel(lake_root: Path, path: Path) -> str:
    return path.relative_to(lake_root).as_posix()


# -- 1. a day of segments ----------------------------------------------------


def test_a_day_of_segments_compacts_to_one_partition_per_surface_and_ticker(lake_root):
    paths = LakePaths(lake_root)
    spy_a = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    spy_b = _segment(
        lake_root,
        "chains",
        "SPY",
        DAY,
        _chains(3, snap_ts=_snap(DAY, 1)),
        _chains(4, snap_ts=_snap(DAY, 2)),
        start_ts="b",
    )
    quotes_a = _segment(
        lake_root, "quotes", "SPY", DAY, _quotes(1, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    quotes_b = _segment(
        lake_root, "quotes", "SPY", DAY, _quotes(1, snap_ts=_snap(DAY, 1)), start_ts="b"
    )
    qqq = _segment(
        lake_root,
        "chains",
        "QQQ",
        DAY,
        _chains(5, snap_ts=_snap(DAY, 0), ticker="QQQ"),
        start_ts="a",
    )
    clock = _clock_at(DAY, 16, 30)

    result, events, backup, pinger = _run(lake_root, clock=clock)

    # One Parquet per surface and ticker, rows equal to the sum across segments.
    spy_chains = paths.chains_partition_path("SPY", DAY)
    spy_quotes = paths.quotes_partition_path("SPY", DAY)
    qqq_chains = paths.chains_partition_path("QQQ", DAY)
    assert pq.read_table(spy_chains).num_rows == 9
    assert pq.read_table(spy_quotes).num_rows == 2
    assert pq.read_table(qqq_chains).num_rows == 5
    # The rows are the segments' rows, every cycle present.
    snaps = set(pq.read_table(spy_chains).column("snap_ts").to_pylist())
    assert snaps == {_snap(DAY, 0), _snap(DAY, 1), _snap(DAY, 2)}
    assert pq.read_table(spy_chains).schema == CHAINS_SCHEMA

    # The manifest carries the sha of the bytes on disk, the row count, and the seal time.
    latest = latest_entries(lake_root)
    for partition, rows in ((spy_chains, 9), (spy_quotes, 2), (qqq_chains, 5)):
        entry = latest[_rel(lake_root, partition)]
        assert entry["sha256"] == sha256_file(partition)
        assert entry["rows"] == rows
        assert entry["source"] == "compaction"
        assert entry["fetched_at"] == clock.now().isoformat()

    # The segments are gone, and so are the empty shells they left.
    for segment in (spy_a, spy_b, quotes_a, quotes_b, qqq):
        assert not segment.exists()
    assert list(paths.journal_dir.iterdir()) == []

    # The result names each seal, with the segments it merged.
    sealed = {item.partition: item for item in result.sealed}
    assert set(sealed) == {
        _rel(lake_root, spy_chains),
        _rel(lake_root, spy_quotes),
        _rel(lake_root, qqq_chains),
    }
    assert sealed[_rel(lake_root, spy_chains)].rows == 9
    assert sealed[_rel(lake_root, spy_chains)].segments == (
        _rel(lake_root, spy_a),
        _rel(lake_root, spy_b),
    )
    assert sealed[_rel(lake_root, spy_chains)].sha256 == sha256_file(spy_chains)
    assert result.verified == () and result.skipped == ()
    assert result.changed
    assert result.backed_up and result.pinged
    assert events == ["backup", "ping"]


def test_a_second_run_over_a_sealed_lake_is_a_no_op_that_still_pings(lake_root):
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    _segment(lake_root, "quotes", "SPY", DAY, _quotes(1, snap_ts=_snap(DAY, 0)), start_ts="a")
    first, _, _, _ = _run(lake_root)
    assert len(first.sealed) == 2
    manifest_before = manifest_path(lake_root).read_bytes()
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    partition_before = partition.read_bytes()

    second, events, _, pinger = _run(lake_root)

    assert second.sealed == () and second.verified == () and second.skipped == ()
    assert not second.changed
    assert manifest_path(lake_root).read_bytes() == manifest_before
    assert partition.read_bytes() == partition_before
    # A job that correctly no-ops is healthy: it still backs up and pings.
    assert events == ["backup", "ping"]
    assert pinger.urls == [URL]


def test_a_mid_day_schema_rotation_compacts_by_name(lake_root):
    # A vendor change rotates to a new segment with one more column. Compaction unifies
    # the two by name, so the older rows carry the new column as null.
    old = _chains(2, snap_ts=_snap(DAY, 0))
    extended = CHAINS_SCHEMA.append(pa.field("vendor_new_field", pa.string()))
    new_rows = _chains_rows(3, snap_ts=_snap(DAY, 1))
    for row in new_rows:
        row["vendor_new_field"] = "x"
    new = _table(extended, new_rows)
    _segment(lake_root, "chains", "SPY", DAY, old, start_ts="a")
    path = journal.segment_path(lake_root, "chains", "SPY", DAY, "b", PID)
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, extended) as writer:
        writer.write_table(new)

    result, _, _, _ = _run(lake_root)

    table = pq.read_table(LakePaths(lake_root).chains_partition_path("SPY", DAY))
    assert table.num_rows == 5
    assert table.column("vendor_new_field").null_count == 2
    assert result.sealed[0].rows == 5


# -- 2. torn tails and shadow appends ----------------------------------------


def test_a_torn_tail_compacts_to_its_complete_batches(lake_root):
    # A power loss mid-append cuts the stream inside its third batch. The prefix through
    # the second batch is the closed two-batch stream minus its eight-byte EOS marker.
    batch = _chains(1, snap_ts=_snap(DAY, 0))
    three = _stream_bytes([batch, batch, batch])
    through_two = len(_stream_bytes([batch, batch])) - 8
    torn = journal.segment_path(lake_root, "chains", "SPY", DAY, "torn", PID)
    torn.parent.mkdir(parents=True)
    torn.write_bytes(three[: through_two + 8])

    result, _, _, _ = _run(lake_root)

    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    assert pq.read_table(partition).num_rows == 2
    assert result.sealed[0].rows == 2
    assert latest_entries(lake_root)[_rel(lake_root, partition)]["rows"] == 2
    assert not torn.exists()


def test_a_segment_torn_before_its_first_batch_reads_as_no_rows(lake_root):
    # Torn inside the stream header: no complete batch at all. It merges as zero rows
    # beside a whole segment, and is unlinked with it.
    whole = _segment(
        lake_root, "chains", "SPY", DAY, _chains(3, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    stub = journal.segment_path(lake_root, "chains", "SPY", DAY, "b", PID)
    stub.write_bytes(_stream_bytes([_chains(1, snap_ts=_snap(DAY, 1))])[:12])

    result, _, _, _ = _run(lake_root)

    assert result.sealed[0].rows == 3
    assert not whole.exists() and not stub.exists()


def test_bytes_after_the_eos_fail_loudly_and_seal_nothing_for_the_ticker_day(lake_root):
    clean = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    shadow = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 1)), start_ts="b"
    )
    shadow.write_bytes(shadow.read_bytes() + b"\x01\x02\x03\x04\x05\x06\x07\x08")
    clean_before = clean.read_bytes()
    shadow_before = shadow.read_bytes()

    with pytest.raises(ShadowAppendError) as info:
        _run(lake_root)

    assert info.value.path == shadow
    # Nothing was sealed: no partition, no manifest entry, both segments byte-identical.
    assert not LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()
    assert read_manifest(lake_root) == []
    assert clean.read_bytes() == clean_before
    assert shadow.read_bytes() == shadow_before


def test_a_shadow_append_blocks_the_backup_and_the_ping(lake_root):
    shadow = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    shadow.write_bytes(shadow.read_bytes() + b"\x00" * 8)
    events: list[str] = []
    backup = FakeBackup(events)
    pinger = FakePinger(events)
    with pytest.raises(ShadowAppendError):
        _run(lake_root, backup=backup, pinger=pinger)
    assert events == []


# -- 3. orphans from older dates ---------------------------------------------


def test_an_orphaned_segment_from_an_older_date_is_swept_and_sealed(lake_root):
    orphan = _segment(
        lake_root, "chains", "SPY", FRIDAY, _chains(4, snap_ts=_snap(FRIDAY, 0)), start_ts="a"
    )
    today = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a"
    )

    result, _, _, _ = _run(lake_root)

    paths = LakePaths(lake_root)
    assert pq.read_table(paths.chains_partition_path("SPY", FRIDAY)).num_rows == 4
    assert pq.read_table(paths.chains_partition_path("SPY", DAY)).num_rows == 2
    assert [(item.day, item.rows) for item in result.sealed] == [(FRIDAY, 4), (DAY, 2)]
    assert not orphan.exists() and not today.exists()
    assert list(paths.journal_dir.iterdir()) == []


# -- 4. the option-close guard -----------------------------------------------


@pytest.mark.parametrize(
    "hour, minute, second",
    [
        (15, 0, 0),  # mid-session
        (16, 18, 0),  # past the option close, inside the guard window
        (16, 20, 0),  # the deadline minute itself
        (16, 20, 59),  # the last second of the deadline minute
    ],
)
def test_a_ticker_day_before_close_plus_5_is_never_touched(lake_root, hour, minute, second):
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    before = segment.read_bytes()

    result, events, _, _ = _run(lake_root, clock=_clock_at(DAY, hour, minute, second))

    assert result.sealed == () and result.verified == ()
    assert result.skipped == (SkippedDay(DAY.isoformat(), "guard_open"),)
    assert segment.read_bytes() == before
    assert not LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()
    assert read_manifest(lake_root) == []
    # The job still ran to completion: backed up, then pinged.
    assert events == ["backup", "ping"]


def test_the_minute_after_close_plus_5_is_eligible(lake_root):
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    result, _, _, _ = _run(lake_root, clock=_clock_at(DAY, 16, 21, 0))
    assert [item.day for item in result.sealed] == [DAY]
    assert result.skipped == ()


def test_a_catch_up_run_mid_session_sweeps_yesterday_but_never_the_live_day(lake_root):
    # A sleep-missed catch-up fires at 11:00 on Tuesday. Monday's orphan seals. Tuesday's
    # live segment, which the daemon may still be appending to, is untouched.
    monday = _segment(
        lake_root, "chains", "SPY", DAY, _chains(3, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    live = _segment(
        lake_root, "chains", "SPY", TUESDAY, _chains(1, snap_ts=_snap(TUESDAY, 0)), start_ts="a"
    )

    result, _, _, _ = _run(lake_root, clock=_clock_at(TUESDAY, 11, 0))

    assert [item.day for item in result.sealed] == [DAY]
    assert result.skipped == (SkippedDay(TUESDAY.isoformat(), "guard_open"),)
    assert not monday.exists()
    assert live.exists()


def test_a_non_session_date_directory_is_reported_and_left_alone(lake_root):
    stray = _segment(
        lake_root, "chains", "SPY", SATURDAY, _chains(1, snap_ts=_snap(SATURDAY, 0)), start_ts="a"
    )
    result, _, _, _ = _run(lake_root)
    assert result.skipped == (SkippedDay(SATURDAY.isoformat(), "not_a_session"),)
    assert stray.exists()


def test_an_unparseable_date_directory_is_reported_and_left_alone(lake_root):
    odd = LakePaths(lake_root).journal_dir / "date=not-a-date" / "surface=chains" / "ticker=SPY"
    odd.mkdir(parents=True)
    (odd / "seg-a-1.arrows").write_bytes(b"junk")
    result, _, _, _ = _run(lake_root)
    assert result.skipped == (SkippedDay("date=not-a-date", "unparseable"),)
    assert (odd / "seg-a-1.arrows").exists()


# -- 5. manifest-aware recovery and the no-shrink guard -----------------------


def _manifested_day(lake_root: Path, rows: int) -> tuple[Path, str]:
    """A chains partition for SPY on DAY already sealed and manifested."""
    fixture = FixtureLake(lake_root)
    fixture.with_partition("chains", "SPY", DAY, _chains(rows, snap_ts=_snap(DAY, 0)))
    fixture.build()
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    return partition, _rel(lake_root, partition)


def test_a_manifested_partition_is_verified_and_its_debris_deleted_never_rewritten(lake_root):
    partition, rel = _manifested_day(lake_root, rows=3)
    debris = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a"
    )
    partition_before = partition.read_bytes()
    manifest_before = manifest_path(lake_root).read_bytes()

    result, events, _, _ = _run(lake_root)

    # The partition is byte-identical and the manifest gained nothing. Only the debris went.
    assert partition.read_bytes() == partition_before
    assert manifest_path(lake_root).read_bytes() == manifest_before
    assert not debris.exists()
    assert result.sealed == ()
    assert len(result.verified) == 1
    verified = result.verified[0]
    assert verified.recovered is True
    assert verified.partition == rel
    assert verified.rows == 3
    assert verified.sha256 == sha256_file(partition)
    assert verified.segments == (_rel(lake_root, debris),)
    assert result.changed
    assert events == ["backup", "ping"]


def test_a_sha_mismatch_on_a_manifested_partition_raises_and_repairs_nothing(lake_root):
    partition, rel = _manifested_day(lake_root, rows=3)
    debris = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a"
    )
    partition.write_bytes(partition.read_bytes() + b"\x00")
    corrupted = partition.read_bytes()
    manifest_before = manifest_path(lake_root).read_bytes()
    events: list[str] = []

    with pytest.raises(PartitionMismatch) as info:
        _run(lake_root, backup=FakeBackup(events), pinger=FakePinger(events))

    assert info.value.partition == rel
    assert info.value.expected == latest_entries(lake_root)[rel]["sha256"]
    assert info.value.actual == sha256_file(partition)
    # Nothing was repaired or deleted, and neither the backup nor the ping ran.
    assert partition.read_bytes() == corrupted
    assert manifest_path(lake_root).read_bytes() == manifest_before
    assert debris.exists()
    assert events == []


def test_a_missing_manifested_partition_raises(lake_root):
    partition, rel = _manifested_day(lake_root, rows=3)
    debris = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a"
    )
    partition.unlink()
    with pytest.raises(PartitionMismatch) as info:
        _run(lake_root)
    assert info.value.partition == rel
    assert info.value.actual is None
    assert debris.exists()
    assert not partition.exists()


def test_the_no_shrink_guard_refuses_a_recompaction_with_fewer_rows(lake_root):
    partition, rel = _manifested_day(lake_root, rows=3)
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a"
    )
    partition_before = partition.read_bytes()
    manifest_before = manifest_path(lake_root).read_bytes()

    with pytest.raises(RowCountRegression) as info:
        recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=_clock_at(DAY, 17, 0))

    assert (info.value.partition, info.value.recorded, info.value.proposed) == (rel, 3, 1)
    # The larger partition is still on disk beside its still-valid entry.
    assert partition.read_bytes() == partition_before
    assert manifest_path(lake_root).read_bytes() == manifest_before
    assert segment.exists()


def test_a_deliberate_recompaction_supersedes_the_entry(lake_root):
    partition, rel = _manifested_day(lake_root, rows=3)
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a"
    )
    clock = _clock_at(DAY, 17, 0)

    outcome = recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=clock, allow_shrink=True)

    assert outcome.partition == rel and outcome.rows == 1
    assert pq.read_table(partition).num_rows == 1
    # Last entry wins: a second entry for the path, with the new sha and count.
    entries = [entry for entry in read_manifest(lake_root) if entry["partition"] == rel]
    assert len(entries) == 2
    assert entries[-1]["rows"] == 1
    assert entries[-1]["sha256"] == sha256_file(partition)
    assert entries[-1]["fetched_at"] == clock.now().isoformat()
    assert not segment.exists()


def test_a_recompaction_with_more_rows_passes_the_guard(lake_root):
    partition, rel = _manifested_day(lake_root, rows=1)
    _segment(
        lake_root,
        "chains",
        "SPY",
        DAY,
        _chains(2, snap_ts=_snap(DAY, 0)),
        _chains(2, snap_ts=_snap(DAY, 1)),
        start_ts="a",
    )
    outcome = recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=_clock_at(DAY, 17, 0))
    assert outcome.rows == 4
    assert latest_entries(lake_root)[rel]["rows"] == 4


def test_a_recompaction_needs_the_segments(lake_root):
    _manifested_day(lake_root, rows=3)
    with pytest.raises(RecompactionRefused):
        recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=_clock_at(DAY, 17, 0))


# -- 6. backup and ping ------------------------------------------------------


def test_backup_runs_after_the_seal_then_pings_the_compaction_slug_once(lake_root):
    segment = _segment(
        lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a"
    )
    result, events, backup, pinger = _run(lake_root)

    assert events == ["backup", "ping"]
    assert backup.calls == [(lake_root, TARGET)]
    # At sync time the partition was already on disk, manifested, and the segment gone.
    partition = _rel(lake_root, LakePaths(lake_root).chains_partition_path("SPY", DAY))
    assert partition in backup.seen[0]
    assert "manifest.jsonl" in backup.seen[0]
    assert _rel(lake_root, segment) not in backup.seen[0]
    assert pinger.urls == [URL]
    assert URL.endswith("/" + COMPACTION_SLUG)
    assert result.backed_up and result.pinged


def test_an_unmounted_backup_target_raises_and_no_ping_fires(lake_root, tmp_path):
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    events: list[str] = []
    pinger = FakePinger(events)
    missing = tmp_path / "ssd-not-mounted"

    with pytest.raises(BackupTargetUnavailable):
        compact(
            lake_root,
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
            backup=RsyncBackup(),
            backup_target=missing,
            pinger=pinger,
            ping_url=URL,
            plan_path=tmp_path / "chain_plan.json",
        )

    assert pinger.urls == [] and events == []
    # The seal itself stood. The day is single-copy, which the missed ping reports.
    assert LakePaths(lake_root).chains_partition_path("SPY", DAY).exists()


def test_an_empty_journal_no_ops_and_still_pings(lake_root):
    assert not LakePaths(lake_root).journal_dir.exists()
    result, events, _, pinger = _run(lake_root)
    assert result.sealed == () and result.verified == () and result.skipped == ()
    assert result.retune is None
    assert not result.changed
    assert events == ["backup", "ping"]
    assert pinger.urls == [URL]


def test_a_holiday_run_no_ops_and_still_pings(lake_root):
    result, events, _, _ = _run(lake_root, clock=_clock_at(SATURDAY, 16, 30))
    assert not result.changed
    assert events == ["backup", "ping"]


def test_a_pinger_needs_a_url(lake_root):
    with pytest.raises(ValueError):
        compact(
            lake_root,
            clock=_clock_at(DAY, 16, 30),
            calendar=_calendar(),
            backup=FakeBackup([]),
            backup_target=TARGET,
            pinger=FakePinger([]),
            plan_path=lake_root.parent / "chain_plan.json",
        )


def test_without_a_pinger_the_job_backs_up_and_reports_no_ping(lake_root):
    events: list[str] = []
    result = compact(
        lake_root,
        clock=_clock_at(DAY, 16, 30),
        calendar=_calendar(),
        backup=FakeBackup(events),
        backup_target=TARGET,
        plan_path=lake_root.parent / "chain_plan.json",
    )
    assert result.backed_up and not result.pinged
    assert events == ["backup"]


# -- 7. the nightly window re-tune -------------------------------------------


def test_the_retune_splits_a_window_over_the_max_and_writes_a_plan_that_parses(lake_root):
    plan_path = lake_root.parent / "config" / "chain_plan.json"
    # Two SPY cycles and one QQQ cycle. Only SPY's second cycle pushes the first window
    # over the max, so the peak across cycles and tickers is what triggers the split.
    calm = _profile_table(DEFAULT_CHAIN_PLAN, DAY, {0: 1000, 1: 1000}, snap_ts=_snap(DAY, 0))
    dense = _profile_table(DEFAULT_CHAIN_PLAN, DAY, {0: 2600, 1: 1000}, snap_ts=_snap(DAY, 1))
    _segment(lake_root, "chains", "SPY", DAY, calm, dense, start_ts="a")
    qqq = _profile_table(
        DEFAULT_CHAIN_PLAN, DAY, {0: 400, 1: 1000}, snap_ts=_snap(DAY, 0), ticker="QQQ"
    )
    _segment(lake_root, "chains", "QQQ", DAY, qqq, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune is not None and retune.skipped_reason is None
    assert retune.day == DAY
    assert retune.before == DEFAULT_CHAIN_PLAN
    assert retune.counts == (2600, 1000, 0, 0, 0)
    assert retune.splits == ((0, 9),)
    assert retune.written and retune.changed
    # The file landed atomically and parses back through the hot-path loader.
    assert load_chain_plan(plan_path) == retune.after
    assert retune.after.windows[:2] == ((0, 4), (5, 9))
    assert retune.after.windows[-1] == (366, None)
    assert not list(plan_path.parent.glob("*.tmp-*"))
    assert result.changed


def test_the_retune_merges_two_small_adjacent_windows(lake_root):
    plan_path = lake_root.parent / "chain_plan.json"
    table = _profile_table(
        DEFAULT_CHAIN_PLAN, DAY, {0: 1000, 1: 100, 2: 200, 3: 1000, 4: 1000}, snap_ts=_snap(DAY, 0)
    )
    _segment(lake_root, "chains", "SPY", DAY, table, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.merges == (((10, 30), (31, 90)),)
    assert retune.splits == ()
    assert load_chain_plan(plan_path).windows == ((0, 9), (10, 90), (91, 365), (366, None))


def test_the_retune_never_splits_the_open_tail(lake_root):
    plan_path = lake_root.parent / "chain_plan.json"
    table = _profile_table(
        DEFAULT_CHAIN_PLAN,
        DAY,
        {0: 1000, 1: 1000, 2: 1000, 3: 1000, 4: 9000},
        snap_ts=_snap(DAY, 0),
    )
    _segment(lake_root, "chains", "SPY", DAY, table, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.counts[-1] == 9000
    assert retune.splits == () and not retune.changed and not retune.written
    assert retune.after.windows[-1] == (366, None)
    assert not plan_path.exists()


def test_an_unchanged_profile_leaves_the_plan_file_untouched(lake_root):
    plan_path = lake_root.parent / "chain_plan.json"
    custom = ChainPlan(((0, 4), (5, 9), (10, None)))
    _write_plan(custom, plan_path)
    before = plan_path.read_bytes()
    stat_before = plan_path.stat()
    table = _profile_table(custom, DAY, {0: 1000, 1: 1000, 2: 1000}, snap_ts=_snap(DAY, 0))
    _segment(lake_root, "chains", "SPY", DAY, table, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.before == custom and retune.after == custom
    assert not retune.written
    assert plan_path.read_bytes() == before
    assert plan_path.stat().st_mtime_ns == stat_before.st_mtime_ns
    assert plan_path.stat().st_ino == stat_before.st_ino


def test_rows_carrying_windows_outside_the_current_plan_skip_the_retune(lake_root):
    # The rows follow the default plan, but the file on disk holds a different tiling.
    # The counts then describe windows the plan does not have, so nothing is rewritten.
    plan_path = lake_root.parent / "chain_plan.json"
    custom = ChainPlan(((0, 4), (5, None)))
    _write_plan(custom, plan_path)
    before = plan_path.read_bytes()
    table = _profile_table(DEFAULT_CHAIN_PLAN, DAY, {0: 5000, 1: 100}, snap_ts=_snap(DAY, 0))
    _segment(lake_root, "chains", "SPY", DAY, table, start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.skipped_reason is not None
    assert "(0, 9)" in retune.skipped_reason
    assert not retune.written and retune.after == custom
    assert plan_path.read_bytes() == before


def test_the_retune_profiles_the_latest_sealed_day(lake_root):
    # Friday's orphan carried an oversized window. Monday is steady. The re-tune reads
    # Monday, the latest day, so the plan stays as it is.
    plan_path = lake_root.parent / "chain_plan.json"
    friday = _profile_table(DEFAULT_CHAIN_PLAN, FRIDAY, {0: 6000}, snap_ts=_snap(FRIDAY, 0))
    _segment(lake_root, "chains", "SPY", FRIDAY, friday, start_ts="a")
    _steady_day(lake_root)

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    assert len(result.sealed) == 2
    assert result.retune.day == DAY
    assert result.retune.counts == (1000, 1000, 1000, 1000, 1000)
    assert not result.retune.written


def test_the_retune_counts_only_data_rows_with_a_window(lake_root):
    plan_path = lake_root.parent / "chain_plan.json"
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(1000, snap_ts=_snap(DAY, 0), window=windows[0])
    # Absence markers in the same window are gap rows. Rows with no window are a
    # one-shot whole-chain fetch. Neither counts toward the window's contracts.
    rows += _chains_rows(3000, snap_ts=_snap(DAY, 0), window=windows[0], row_kind="gap")
    rows += _chains_rows(3000, snap_ts=_snap(DAY, 0))
    _segment(lake_root, "chains", "SPY", DAY, _table(CHAINS_SCHEMA, rows), start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    assert result.retune.counts == (1000, 0, 0, 0, 0)
    assert result.retune.splits == ()


def test_a_day_with_no_windowed_data_row_skips_the_retune(lake_root):
    # A one-shot whole-chain day carries no window at all. A dead-daemon day carries only
    # whole-chain gaps. Neither says anything about the plan, so nothing is rewritten. A
    # zero count everywhere would otherwise merge every finite window on no evidence.
    plan_path = lake_root.parent / "chain_plan.json"
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(500, snap_ts=_snap(DAY, 0))
    rows += _chains_rows(1, snap_ts=_snap(DAY, 1), row_kind="gap")
    rows += _chains_rows(1, snap_ts=_snap(DAY, 2), window=windows[1], row_kind="gap")
    _segment(lake_root, "chains", "SPY", DAY, _table(CHAINS_SCHEMA, rows), start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.skipped_reason == "no windowed data rows to profile"
    assert not retune.written and retune.after == DEFAULT_CHAIN_PLAN
    assert retune.counts == (0, None, 0, 0, 0)
    assert not plan_path.exists()


def test_a_window_that_failed_all_day_is_unknown_and_never_merges(lake_root):
    # (31, 90) carried only absence markers. Its size is unmeasured, so it neither merges
    # with its small neighbour nor lets the merge cross it. Folding a failing range into
    # a healthy window would only spread the failure.
    plan_path = lake_root.parent / "chain_plan.json"
    windows = _iso_windows(DEFAULT_CHAIN_PLAN, DAY)
    rows = _chains_rows(1000, snap_ts=_snap(DAY, 0), window=windows[0])
    rows += _chains_rows(100, snap_ts=_snap(DAY, 0), window=windows[1])
    rows += _chains_rows(3, snap_ts=_snap(DAY, 0), window=windows[2], row_kind="gap")
    rows += _chains_rows(100, snap_ts=_snap(DAY, 0), window=windows[3])
    _segment(lake_root, "chains", "SPY", DAY, _table(CHAINS_SCHEMA, rows), start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    retune = result.retune
    assert retune.skipped_reason is None
    assert retune.counts == (1000, 100, None, 100, 0)
    assert retune.merges == () and retune.splits == ()
    assert not retune.written and retune.after == DEFAULT_CHAIN_PLAN
    assert not plan_path.exists()


def test_the_retune_runs_after_a_recovered_partition_too(lake_root):
    # A crash between the manifest append and the last unlink also lost the re-tune. The
    # recovery run profiles the verified partition, so the plan still catches up.
    plan_path = lake_root.parent / "chain_plan.json"
    fixture = FixtureLake(lake_root)
    fixture.with_partition(
        "chains",
        "SPY",
        DAY,
        _profile_table(DEFAULT_CHAIN_PLAN, DAY, {0: 2600, 1: 1000}, snap_ts=_snap(DAY, 0)),
    )
    fixture.build()
    _segment(lake_root, "chains", "SPY", DAY, _chains(1, snap_ts=_snap(DAY, 5)), start_ts="a")

    result, _, _, _ = _run(lake_root, plan_path=plan_path)

    assert result.verified[0].recovered
    assert result.retune.splits == ((0, 9),)
    assert load_chain_plan(plan_path) == result.retune.after


def test_a_quotes_only_day_has_no_profile(lake_root):
    _segment(lake_root, "quotes", "SPY", DAY, _quotes(1, snap_ts=_snap(DAY, 0)), start_ts="a")
    result, _, _, _ = _run(lake_root)
    assert len(result.sealed) == 1
    assert result.retune is None


# -- the command-line entry --------------------------------------------------


def _config_file(tmp_path: Path, lake_root: Path) -> Path:
    target = tmp_path / "ssd"
    target.mkdir()
    config = tmp_path / "config.yaml"
    config.write_text(
        "\n".join(
            [
                f"lake_root: {lake_root}",
                f"backup_target: {target}",
                "healthchecks_ping_key: secret-key",
                "ntfy_topic: secret-topic",
                "schwab_api_key: secret-api-key",
                "schwab_app_secret: secret-app-secret",
                "",
            ]
        )
    )
    return config


def test_main_runs_the_job_from_config_with_injected_seams(lake_root, tmp_path, capsys):
    config = _config_file(tmp_path, lake_root)
    plan_path = tmp_path / "chain_plan.json"
    _segment(lake_root, "chains", "SPY", DAY, _chains(2, snap_ts=_snap(DAY, 0)), start_ts="a")
    _segment(lake_root, "quotes", "SPY", DAY, _quotes(1, snap_ts=_snap(DAY, 0)), start_ts="a")
    events: list[str] = []
    backup = FakeBackup(events)
    pinger = FakePinger(events)

    code = main(
        ["--config", str(config), "--plan", str(plan_path)],
        clock=_clock_at(DAY, 16, 30),
        calendar=_calendar(),
        backup=backup,
        pinger=pinger,
    )

    assert code == 0
    out = capsys.readouterr().out
    assert "sealed=2" in out
    assert f"slug={COMPACTION_SLUG}" in out
    # The ping went to the config's check, and the secret key was never printed.
    assert pinger.urls == [f"https://hc-ping.com/secret-key/{COMPACTION_SLUG}"]
    assert "secret-key" not in out
    assert backup.calls == [(lake_root, tmp_path / "ssd")]
    assert events == ["backup", "ping"]
    assert not plan_path.exists()


def test_main_recompact_is_the_human_repair(lake_root, tmp_path, capsys):
    config = _config_file(tmp_path, lake_root)
    partition, rel = _manifested_day(lake_root, rows=1)
    _segment(lake_root, "chains", "SPY", DAY, _chains(3, snap_ts=_snap(DAY, 0)), start_ts="a")

    code = main(
        ["--config", str(config), "recompact", "chains", "SPY", DAY.isoformat()],
        clock=_clock_at(DAY, 17, 0),
    )

    assert code == 0
    assert "recompacted" in capsys.readouterr().out
    assert latest_entries(lake_root)[rel]["rows"] == 3


def test_build_parser_accepts_the_run_and_the_repair():
    parser = build_parser()
    run = parser.parse_args([])
    assert run.command is None and run.config is None and run.plan is None
    repair = parser.parse_args(["recompact", "chains", "SPY", "2026-08-24", "--allow-shrink"])
    assert (repair.surface, repair.ticker, repair.day) == ("chains", "SPY", "2026-08-24")
    assert repair.allow_shrink is True
    assert parser.parse_args(["recompact", "quotes", "QQQ", "2026-08-24"]).allow_shrink is False
