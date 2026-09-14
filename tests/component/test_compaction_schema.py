"""Compaction's merge-time schema check, over real segments with a fake clock.

Two segments in one ticker-day can disagree about columns only when the daemon restarts
onto different code mid-session. Every production segment takes its schema from
``journal.schema_for``, and a vendor that stops sending a known field yields a null
column rather than a dropped one, so the check guards this project's own release
process rather than the vendor.

The shape that motivates checking *at the merge* is a column dropped mid-day. The merge
unifies by name and fills the missing column with nulls, and the sealed partition then
holds that column with nulls on the post-rotation rows, which reads exactly like a vendor
that stopped sending it. Compaction runs the code that dropped the column, so the one
moment the drop is legible is the merge, where the merged table still carries a column
the pinned schema no longer names.

These cover the check's contract:

1. A mid-day drop is filed, naming the column, and the sealed partition proves the
   evidence is gone the moment the seal lands.
2. A legitimate mid-day addition is not filed. The merged table carries the promoted
   column at the end, the reorder puts it back in the pinned schema's place, and the
   comparison is then an equality that a schema bump passes.
3. A column no segment carried is filed, and so is a type every segment agreed on that
   the pinned schema does not hold.
4. Nothing is filed on an ordinary day, and no report ever reaches the manifest.
5. A finding never costs the run. Neither does a finding that cannot be written.
6. The human-invoked repair runs the same check, and a repair the no-shrink guard
   refuses files nothing, because it leaves the partition alone.
7. The writer itself: where the file lands, what it holds, and that it never overwrites.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import compact as compact_module
from lake import journal, report
from lake.alert import undelivered
from lake.calendar import MARKET_TZ
from lake.compact import CompactionResult, compact, recompact_ticker_day
from lake.journal import CHAINS_SCHEMA
from lake.manifest import RowCountRegression, append_manifest, read_manifest
from lake.paths import REPORTS_DIR, LakePaths
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger

FRIDAY = date(2026, 8, 21)
DAY = date(2026, 8, 24)
TUESDAY = date(2026, 8, 25)

URL = "https://hc-ping.com/secret-key/compaction"
TARGET = Path("/ssd/lake")
PID = 4242

# The chains column these tests move. An ordinary vendor column, chosen because it sits
# in the middle of the pinned schema, so a reorder that fails to run is visible.
COLUMN = "open_interest"


# -- the seams ---------------------------------------------------------------


def _et(day: date, hour: int, minute: int, second: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute, second), tzinfo=MARKET_TZ)


def _calendar() -> FakeCalendar:
    return FakeCalendar(
        {
            day: SessionTimes(open=_et(day, 9, 30), close=_et(day, 16, 0))
            for day in (FRIDAY, DAY, TUESDAY)
        }
    )


def _run(
    lake_root: Path,
    *,
    clock: ManualClock | None = None,
    backup=None,
    pinger=None,
) -> tuple[CompactionResult, list[str]]:
    events: list[str] = []
    backup = backup if backup is not None else FakeBackup(events)
    pinger = pinger if pinger is not None else FakePinger(events)
    result = compact(
        lake_root,
        clock=clock if clock is not None else ManualClock(_et(DAY, 16, 30)),
        calendar=_calendar(),
        backup=backup,
        backup_target=TARGET,
        pinger=pinger,
        ping_url=URL,
        plan_path=lake_root.parent / "chain_plan.json",
    )
    return result, events


# -- building rows and segments ----------------------------------------------


def _snap(day: date, minute: int) -> str:
    return (_et(day, 9, 30) + timedelta(minutes=minute)).isoformat()


def _rows(count: int, *, snap_ts: str, ticker: str = "SPY") -> list[dict]:
    return [
        {
            "vendor_new_field": "x",
            "snap_ts": snap_ts,
            "fetch_ts": snap_ts,
            "ticker": ticker,
            "occ_symbol": f"{ticker} {snap_ts} {index}",
            "row_kind": "data",
            "suspect": False,
            "schema_version": 1,
            COLUMN: 100 + index,
        }
        for index in range(count)
    ]


def _table(schema: pa.Schema, rows: list[dict]) -> pa.Table:
    """Rows in a schema, every unnamed column null."""
    arrays = [pa.array([row.get(f.name) for row in rows], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _segment(
    lake_root: Path,
    schema: pa.Schema,
    table: pa.Table,
    *,
    start_ts: str,
    ticker: str = "SPY",
    day: date = DAY,
    surface: str = "chains",
    pid: int = PID,
) -> Path:
    """Write one closed segment at a schema of the test's choosing.

    ``SegmentWriter`` takes its schema from ``journal.schema_for`` and so can only ever
    write the pinned one. A day whose segments disagree is exactly what these tests
    build, so the stream is written by hand the way the capture loop's writer would.
    """
    path = journal.segment_path(lake_root, surface, ticker, day, start_ts, pid)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return path


def _without(schema: pa.Schema, name: str) -> pa.Schema:
    """The schema with one column removed, the shape that code dropping it would pin."""
    return schema.remove(schema.get_field_index(name))


def _pin(monkeypatch: pytest.MonkeyPatch, schema: pa.Schema, surface: str = "chains") -> None:
    """Run compaction against a pinned chains schema of the test's choosing.

    The segments are written before this lands, so the lake holds a day captured under
    one code shape and compaction runs under another. That is the only way the two
    disagree in production, and it is what the check is for.
    """
    real = journal.schema_for

    def pinned(asked: str) -> pa.Schema:
        return schema if asked == surface else real(asked)

    monkeypatch.setattr(journal, "schema_for", pinned)


# -- reading the findings back -----------------------------------------------


def _findings(lake_root: Path, day: date = DAY) -> list[dict]:
    directory = report.schema_drift_dir(lake_root, day)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


def _partition(lake_root: Path, ticker: str = "SPY", day: date = DAY) -> pa.Table:
    return pq.read_table(LakePaths(lake_root).chains_partition_path(ticker, day))


# -- 1. a column dropped mid-day ---------------------------------------------


def test_a_column_dropped_mid_day_is_filed(lake_root, monkeypatch):
    # The daemon captured the morning under code carrying the column and restarted onto
    # code without it. Compaction runs the second shape.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, dropped, _table(dropped, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, dropped)

    result, _ = _run(lake_root)

    (finding,) = _findings(lake_root)
    assert finding["unexpected"] == [COLUMN]
    assert finding["missing"] == []
    assert finding["retyped"] == []
    assert finding["surface"] == "chains"
    assert finding["ticker"] == "SPY"
    assert finding["partition"] == result.sealed[0].partition


def test_the_sealed_day_cannot_be_told_from_a_vendor_that_stopped_sending(lake_root, monkeypatch):
    # Why the check sits at the merge. The seal keeps the column and nulls the rows
    # captured after the rotation, which is what a vendor dropping a field also looks
    # like. Nothing downstream can separate the two, so the finding is the only record.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, dropped, _table(dropped, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, dropped)

    _run(lake_root)

    table = _partition(lake_root)
    assert table.num_rows == 5
    assert COLUMN in table.schema.names
    assert table.column(COLUMN).null_count == 3
    assert _findings(lake_root)[0]["unexpected"] == [COLUMN]


def test_a_dropped_column_keeps_its_values_in_the_partition(lake_root, monkeypatch):
    # The reorder moves columns and never loses one. A column the pinned schema no
    # longer names still reaches the partition with the rows that captured it.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, dropped, _table(dropped, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, dropped)

    _run(lake_root)

    table = _partition(lake_root)
    assert table.column(COLUMN).to_pylist() == [100, 101, None, None, None]
    assert sorted(table.schema.names) == sorted([*dropped.names, COLUMN])


# -- 2. a legitimate column addition stays quiet -----------------------------


def _added() -> pa.Schema:
    """The pinned schema of code that added a column in the middle of the chains set."""
    index = CHAINS_SCHEMA.get_field_index(COLUMN)
    return CHAINS_SCHEMA.insert(index, pa.field("vendor_new_field", pa.string()))


def test_a_legitimate_column_addition_is_not_filed(lake_root, monkeypatch):
    # The morning's segments predate the column and the afternoon's carry it. The merge
    # fills the morning rows with nulls, which is the whole point of unifying by name.
    # A detector that cried on this would be worse than none.
    added = _added()
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, added, _table(added, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, added)

    result, events = _run(lake_root)

    assert _findings(lake_root) == []
    assert not (lake_root / REPORTS_DIR).exists()
    assert result.sealed[0].rows == 5
    assert events == ["backup", "ping"]


def test_the_reorder_is_what_lets_an_addition_pass(lake_root, monkeypatch):
    # Promotion appends. Without the reorder the merged table would carry the promoted
    # column last, the comparison would read that as drift, and every schema bump would
    # file a finding. The sealed partition is in the pinned order, not the merged one.
    added = _added()
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, added, _table(added, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, added)

    _run(lake_root)

    table = _partition(lake_root)
    assert table.schema.names == added.names
    assert table.schema.names[-1] != "vendor_new_field"
    assert table.column("vendor_new_field").null_count == 2


def test_an_unpromoted_day_keeps_the_pinned_order(lake_root, monkeypatch):
    # The reorder is a no-op on every ordinary day, because segments that agree already
    # merge in the pinned order. So it can only ever move a promoted column.
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(4, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, CHAINS_SCHEMA)

    _run(lake_root)

    assert _partition(lake_root).schema.names == CHAINS_SCHEMA.names


# -- 3. a column no segment carried, and a whole-day retype ------------------


def test_a_column_no_segment_carried_is_filed(lake_root, monkeypatch):
    # The other direction: the day was captured under code that dropped the column and
    # compaction runs the shape that still holds it. This one survives the seal, because
    # the partition carries one fewer column than the schema forever, and it is filed
    # anyway so the finding names the day rather than leaving a reader to notice.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(lake_root, dropped, _table(dropped, _rows(2, snap_ts=_snap(DAY, 0))), start_ts="a")
    _segment(lake_root, dropped, _table(dropped, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, CHAINS_SCHEMA)

    _run(lake_root)

    (finding,) = _findings(lake_root)
    assert finding["missing"] == [COLUMN]
    assert finding["unexpected"] == []
    assert COLUMN not in _partition(lake_root).schema.names


def test_a_type_every_segment_agreed_on_is_filed(lake_root, monkeypatch):
    # A retype the segments disagree on never reaches the check, because the merge
    # itself refuses it. A retype they agree on merges cleanly and is the one the pinned
    # schema has to catch.
    index = CHAINS_SCHEMA.get_field_index(COLUMN)
    retyped = CHAINS_SCHEMA.set(index, pa.field(COLUMN, pa.int32()))
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, retyped)

    _run(lake_root)

    (finding,) = _findings(lake_root)
    assert finding["retyped"] == [f"{COLUMN}: int32 -> int64"]
    assert finding["missing"] == []
    assert finding["unexpected"] == []


# -- 4. the file, and what never gets one ------------------------------------


def test_an_ordinary_day_files_nothing(lake_root):
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(4, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )

    _run(lake_root)

    assert not (lake_root / REPORTS_DIR).exists()


def test_the_finding_lands_under_its_own_dated_directory(lake_root, monkeypatch):
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    _run(lake_root)

    directory = lake_root / REPORTS_DIR / report.SCHEMA_DRIFT_DIR / f"date={DAY.isoformat()}"
    (path,) = sorted(directory.glob("*.json"))
    assert path.name.endswith(".json")
    assert "chains-SPY" in path.name


def test_the_directory_is_keyed_on_the_ticker_day_not_the_run(lake_root, monkeypatch):
    # A sweep recovers a date an earlier failed run left behind, so the day compaction
    # runs on and the day the rows belong to come apart. The finding is about the rows.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(FRIDAY, 0))),
        start_ts="a",
        day=FRIDAY,
    )
    _pin(monkeypatch, dropped)

    _run(lake_root, clock=ManualClock(_et(DAY, 16, 30)))

    assert _findings(lake_root, FRIDAY) != []
    assert _findings(lake_root, DAY) == []


def test_the_finding_names_the_segments_it_read(lake_root, monkeypatch):
    # The segments are unlinked on the way out, so their names are part of the evidence.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    first = _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    second = _segment(
        lake_root, dropped, _table(dropped, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b"
    )
    expected = [path.relative_to(lake_root).as_posix() for path in (first, second)]
    _pin(monkeypatch, dropped)

    _run(lake_root)

    assert _findings(lake_root)[0]["segments"] == expected
    assert not first.exists() and not second.exists()


def test_the_finding_carries_the_running_schema_version(lake_root, monkeypatch):
    # The version is the one piece of provenance that survives the seal, so the file
    # says which one the compactor was running when it found the difference.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    _run(lake_root)

    assert _findings(lake_root)[0]["schema_version"] == journal.SCHEMA_VERSION


def test_a_finding_never_reaches_the_manifest(lake_root, monkeypatch):
    # ``reports/`` is in the scrub's exclusion set by name, so an entry for one would
    # turn a report into a checksum failure on the following Sunday.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    _run(lake_root)

    assert _findings(lake_root) != []
    partitions = {str(entry["partition"]) for entry in read_manifest(lake_root)}
    assert not any(path.startswith(REPORTS_DIR) for path in partitions)


def test_two_drifted_ticker_days_each_file_their_own(lake_root, monkeypatch):
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    for ticker in ("QQQ", "SPY"):
        _segment(
            lake_root,
            CHAINS_SCHEMA,
            _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0), ticker=ticker)),
            start_ts="a",
            ticker=ticker,
        )
    _pin(monkeypatch, dropped)

    _run(lake_root)

    assert [finding["ticker"] for finding in _findings(lake_root)] == ["QQQ", "SPY"]


# -- 5. a finding never costs the run ----------------------------------------


def test_the_sweep_finishes_past_a_drifted_ticker_day(lake_root, monkeypatch):
    # ``compact`` seals every ticker-day bare and its only try/except wraps the ping, so
    # a raise here would cost the rest of the sweep, the backup, and the ping.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0), ticker="QQQ")),
        start_ts="a",
        ticker="QQQ",
    )
    _segment(
        lake_root,
        dropped,
        _table(dropped, _rows(3, snap_ts=_snap(DAY, 0), ticker="SPY")),
        start_ts="a",
        ticker="SPY",
    )
    _pin(monkeypatch, dropped)

    result, events = _run(lake_root)

    assert [sealed.ticker for sealed in result.sealed] == ["QQQ", "SPY"]
    assert events == ["backup", "ping"]
    assert result.problem is None
    assert _partition(lake_root, "QQQ").num_rows == 2
    assert _partition(lake_root, "SPY").num_rows == 3


def test_a_write_that_cannot_land_costs_the_file_and_nothing_else(lake_root, monkeypatch, capsys):
    # The report is the last line of defence and it just failed. The one place left to
    # say so is the daemon's own log, which launchd files.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    def refuse(*args, **kwargs):
        raise PermissionError("read-only lake")

    monkeypatch.setattr(compact_module, "write_schema_drift", refuse)

    result, events = _run(lake_root)

    assert result.sealed[0].rows == 2
    assert events == ["backup", "ping"]
    assert _findings(lake_root) == []
    captured = capsys.readouterr()
    assert "PermissionError" in captured.err
    assert result.sealed[0].partition in captured.err


# -- 6. the human-invoked repair, and a repair that is refused ---------------


def test_a_recompaction_files_the_same_finding(lake_root, monkeypatch):
    # ``recompact_ticker_day`` re-runs the same merge, so it reaches the same check.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    outcome = recompact_ticker_day(
        lake_root, "chains", "SPY", DAY, clock=ManualClock(_et(DAY, 16, 30))
    )

    (finding,) = _findings(lake_root)
    assert finding["unexpected"] == [COLUMN]
    assert finding["partition"] == outcome.partition


def test_a_repair_the_no_shrink_guard_refuses_files_nothing(lake_root, monkeypatch):
    # A refused rebuild leaves the partition on disk untouched, beside its still-valid
    # entry. A finding filed before the guard would name that file as drifted, and the
    # file is not. Nothing is lost by waiting, because the segments survive the refusal
    # and the next attempt merges them again and finds the same difference.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    segment = _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    partition = LakePaths(lake_root).chains_partition_path("SPY", DAY)
    rel = partition.relative_to(lake_root).as_posix()
    partition.parent.mkdir(parents=True, exist_ok=True)
    partition.write_bytes(b"the partition this repair must not replace")
    append_manifest(
        lake_root,
        partition=rel,
        source="compaction",
        sha256="0" * 64,
        rows=999,
        fetched_at=None,
        guard=False,
    )
    _pin(monkeypatch, dropped)

    for _ in range(3):
        with pytest.raises(RowCountRegression):
            recompact_ticker_day(
                lake_root, "chains", "SPY", DAY, clock=ManualClock(_et(DAY, 16, 30))
            )

    assert _findings(lake_root) == []
    assert partition.read_bytes() == b"the partition this repair must not replace"
    assert segment.exists(), "the evidence has to survive a refusal"


def test_a_seal_that_cannot_write_its_partition_files_nothing(lake_root, monkeypatch):
    # Same rule through the other route. The finding describes a partition, so it waits
    # until there is one.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    def refuse(table, partition):
        raise OSError("no space left on device")

    monkeypatch.setattr(compact_module, "_write_partition", refuse)

    with pytest.raises(OSError, match="no space left"):
        _run(lake_root)

    assert _findings(lake_root) == []


def test_a_writer_bug_is_not_swallowed(lake_root, monkeypatch):
    # The containment is narrowed to ``OSError`` on purpose, which is the rule
    # ``alert._record`` already follows. An environment that will not take the file is
    # contained. A bug in this code is not, because a detector that hid its own
    # exceptions would go quiet and say so nowhere.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    def bug(*args, **kwargs):
        raise RuntimeError("the writer is broken")

    monkeypatch.setattr(compact_module, "write_schema_drift", bug)

    with pytest.raises(RuntimeError, match="the writer is broken"):
        _run(lake_root)


def test_a_finding_never_counts_as_a_page_that_failed_to_send(lake_root, monkeypatch):
    # ``alert.undelivered`` counts the files under ``reports/alerts/`` as pages that
    # never reached the phone, and the Now panel shows that count. A producer filing
    # beside it rather than in it is the rule the tree is built on, and this is what
    # says the new producer follows it.
    dropped = _without(CHAINS_SCHEMA, COLUMN)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, dropped)

    _run(lake_root)

    assert _findings(lake_root) != []
    assert undelivered(lake_root, DAY) == 0


# -- 7. the writer -----------------------------------------------------------

DRIFT = report.SchemaDrift(
    surface="chains",
    ticker="SPY",
    day=DAY,
    partition="chains/ticker=SPY/date=2026-08-24.parquet",
    schema_version=1,
    missing=("open_interest",),
    unexpected=("gone",),
    retyped=("volume: int64 -> int32",),
    segments=("journal/date=2026-08-24/surface=chains/ticker=SPY/seg-a-1.arrows",),
)
AT = _et(DAY, 16, 30, 12)


def test_the_writer_files_exactly_what_it_was_given(lake_root):
    path = report.write_schema_drift(lake_root, DRIFT, now=AT, pid=11)

    assert path == report.schema_drift_dir(lake_root, DAY) / "163012000000-chains-SPY-11.json"
    assert path.read_text() == (
        '{"at": "2026-08-24T16:30:12-04:00", '
        '"day": "2026-08-24", '
        '"missing": ["open_interest"], '
        '"partition": "chains/ticker=SPY/date=2026-08-24.parquet", '
        '"retyped": ["volume: int64 -> int32"], '
        '"schema_version": 1, '
        '"segments": ['
        '"journal/date=2026-08-24/surface=chains/ticker=SPY/seg-a-1.arrows"], '
        '"surface": "chains", '
        '"ticker": "SPY", '
        '"unexpected": ["gone"]}\n'
    )


def test_a_utc_instant_is_filed_in_eastern(lake_root):
    # The stamp in the name and the ``at`` in the file are both Eastern, so a reader
    # never has to know which zone the writing process was in.
    utc = datetime(2026, 8, 24, 20, 30, 12, tzinfo=UTC)

    path = report.write_schema_drift(lake_root, DRIFT, now=utc, pid=11)

    assert path.name == "163012000000-chains-SPY-11.json"
    assert json.loads(path.read_text())["at"] == "2026-08-24T16:30:12-04:00"


def test_a_file_is_never_written_over(lake_root):
    # The stamp carries microseconds and the writing process's id, so this shape is not
    # reachable in production. It is covered because the mode that guarantees it is one
    # character, and an ``open(..., "w")`` would pass every other case here while
    # silently replacing a finding.
    report.write_schema_drift(lake_root, DRIFT, now=AT, pid=11)
    second = report.SchemaDrift(
        surface="chains",
        ticker="SPY",
        day=DAY,
        partition=DRIFT.partition,
        schema_version=1,
        missing=("something_else",),
    )

    with pytest.raises(FileExistsError):
        report.write_schema_drift(lake_root, second, now=AT, pid=11)

    entry = json.loads(
        (report.schema_drift_dir(lake_root, DAY) / "163012000000-chains-SPY-11.json").read_text()
    )
    assert entry["missing"] == ["open_interest"], "the first finding was replaced"


def test_the_default_pid_is_the_writing_process(lake_root):
    path = report.write_schema_drift(lake_root, DRIFT, now=AT)

    assert path.name.endswith(f"-{os.getpid()}.json")


def test_a_missing_lake_root_is_refused_rather_than_created(lake_root):
    # `parents=True` from a missing root would create the lake itself, and the Sunday
    # job decides whether to ping on `root.is_dir()`. A writer that conjured the root
    # would turn "lake root missing" into a green check on the following attempt.
    missing = lake_root.parent / "gone"

    with pytest.raises(FileNotFoundError):
        report.write_schema_drift(missing, DRIFT, now=AT, pid=11)

    assert not missing.exists()


def test_a_lake_root_that_is_a_file_is_refused(lake_root):
    impostor = lake_root.parent / "not-a-lake"
    impostor.write_text("")

    with pytest.raises(FileNotFoundError):
        report.write_schema_drift(impostor, DRIFT, now=AT, pid=11)

    assert impostor.read_text() == ""
