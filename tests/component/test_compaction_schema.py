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
8. One page per run carries the finding to a phone, naming every column that moved, and
   an ordinary run sends none.
9. A merge the segments' own types refused files its own finding, flagged so a reader can
   tell it from a sealed one, and reaches the same single page. It folds with a drift that
   survived the merge, it files again on every run the conflict survives, it still pages
   when the file cannot be written, and the human-invoked repair lets it out rather than
   containing it.
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
from lake.alert import Message, Publisher, undelivered
from lake.calendar import MARKET_TZ
from lake.compact import (
    SCHEMA_DRIFT_EVENT,
    SCHEMA_DRIFT_TITLE,
    CompactionResult,
    compact,
    recompact_ticker_day,
)
from lake.journal import CHAINS_SCHEMA
from lake.manifest import RowCountRegression, append_manifest, read_manifest
from lake.paths import REPORTS_DIR, LakePaths
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

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
    publisher=None,
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
        publisher=publisher,
        plan_path=lake_root.parent / "chain_plan.json",
    )
    return result, events


def _paging(lake_root: Path, transport=None) -> tuple[Publisher, FakeTransport]:
    """A publisher over a recording transport, holding the config's two secrets.

    The secrets are what the real ``main`` passes, so a page composed here is refused on
    exactly the terms a page composed in production would be.
    """
    transport = FakeTransport() if transport is None else transport
    publisher = Publisher(lake_root=lake_root, transport=transport, secrets=(PING_KEY, NTFY_TOPIC))
    return publisher, transport


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
    # itself refuses it. That refusal is covered by
    # ``test_compaction.py::test_a_mid_day_retype_refuses_the_merge_and_leaves_the_day_alone``.
    # A retype they agree on merges cleanly and is the one the pinned schema has to catch.
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
    # The sweep catches one name out of ``_seal``, the merge a column type conflict
    # refused, and nothing else. So a raise here would cost the rest of the sweep, the
    # backup, and the ping.
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
        '"refused": false, '
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


# -- 8. the page -------------------------------------------------------------


def _drifted_day(lake_root: Path, monkeypatch, tickers=("SPY",)) -> None:
    """One mid-day column drop per named ticker, with the pinned schema past the drop."""
    for ticker in tickers:
        _segment(
            lake_root,
            CHAINS_SCHEMA,
            _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0), ticker=ticker)),
            start_ts="a",
            ticker=ticker,
        )
    _pin(monkeypatch, _without(CHAINS_SCHEMA, COLUMN))


def test_a_drifted_run_pages(lake_root, monkeypatch):
    # The design's schema policy says a missing or retyped known field pages. The file
    # under `reports/schema_drift/` has no reader until D20 renders it, so until this
    # wiring the finding reached nobody on the evening it happened.
    _drifted_day(lake_root, monkeypatch)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert len(transport.messages) == 1
    page = transport.messages[0]
    assert page.event == SCHEMA_DRIFT_EVENT
    assert page.title == SCHEMA_DRIFT_TITLE
    assert page.priority == 5
    assert COLUMN in page.body
    assert DAY.isoformat() in page.body


def test_the_page_names_the_directory_the_findings_are_in(lake_root, monkeypatch):
    # The page folds the run, so the per-ticker-day detail lives only in the files. A
    # page that did not say where they are would leave the reader to guess.
    _drifted_day(lake_root, monkeypatch)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert "reports/schema_drift/" in transport.messages[0].body
    assert _findings(lake_root) != []


def test_a_wide_drift_is_one_page_and_not_one_per_finding(lake_root, monkeypatch):
    # This is the cadence the issue settles. One bad release drifts every ticker-day
    # still in flight and each finding restates the same columns, so paging per finding
    # would scale the page count with the roster while the fact stayed one fact. The
    # publisher caps the day at forty and writes the forty-first down rather than sending
    # it, so a producer that storms spends the cap the auth-death page needs.
    _drifted_day(lake_root, monkeypatch, tickers=("IWM", "QQQ", "SPY"))
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert len(_findings(lake_root)) == 3
    assert len(transport.messages) == 1
    body = transport.messages[0].body
    assert body.startswith("3 ticker-day(s)")
    # The columns are a union, so one drifted column is named once however many
    # ticker-days carried it.
    assert body.count(COLUMN) == 1


def test_the_page_counts_ticker_days_and_never_names_the_tickers(lake_root, monkeypatch):
    # A body listing every drifted ticker would be unreadable on a phone and would say
    # no more than the count does. The files carry the ticker.
    _drifted_day(lake_root, monkeypatch, tickers=("IWM", "QQQ", "SPY"))
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    body = transport.messages[0].body
    assert [finding["ticker"] for finding in _findings(lake_root)] == ["IWM", "QQQ", "SPY"]
    assert "QQQ" not in body


def test_an_ordinary_run_pages_nothing(lake_root):
    # This is the over-reach half. A lake that drifted nowhere leaves the phone silent.
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(4, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert transport.messages == []
    assert not (lake_root / REPORTS_DIR).exists()
    assert result.sealed[0].rows == 4
    assert events == ["backup", "ping"]


def test_a_legitimate_column_addition_pages_nothing(lake_root, monkeypatch):
    # The shape that would page on every schema bump if the reorder or the comparison
    # were wrong. The detector already refuses to file it, and the page must not
    # reintroduce the noise a step later.
    added = _added()
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _segment(lake_root, added, _table(added, _rows(3, snap_ts=_snap(DAY, 1))), start_ts="b")
    _pin(monkeypatch, added)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert transport.messages == []


def test_an_empty_lake_pages_nothing(lake_root):
    # Nothing to sweep is the commonest run of all, and a producer that pages on it
    # would page every night.
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert transport.messages == []


def test_the_page_never_raises_out_of_the_sweep(lake_root, monkeypatch):
    # ``publish`` never raises, which is what lets a page sit inside a job that must not
    # die. A transport that throws must still leave the seal, the backup and the ping
    # exactly as they were.
    class Broken:
        def send(self, message):
            raise ConnectionError("ntfy unreachable")

    _drifted_day(lake_root, monkeypatch)
    publisher, _ = _paging(lake_root, transport=Broken())

    result, events = _run(lake_root, publisher=publisher)

    assert result.sealed[0].rows == 2
    assert result.problem is None
    assert events == ["backup", "ping"]
    assert _findings(lake_root) != []


def test_a_page_that_could_not_be_sent_is_written_down(lake_root, monkeypatch, capsys):
    # The publisher's own rule: a page that never left the laptop is never invisible. It
    # lands under ``reports/alerts/``, which the Now panel counts, and the reason is
    # named on stderr as well.
    class Broken:
        def send(self, message):
            raise ConnectionError("ntfy unreachable")

    _drifted_day(lake_root, monkeypatch)
    publisher, _ = _paging(lake_root, transport=Broken())

    _run(lake_root, publisher=publisher)

    assert undelivered(lake_root, DAY) == 1
    directory = lake_root / REPORTS_DIR / "alerts" / f"date={DAY.isoformat()}"
    record = json.loads(next(iter(directory.glob("*.json"))).read_text())
    assert record["event"] == SCHEMA_DRIFT_EVENT
    assert record["reason"] == "post_failed"
    assert "page not sent" in capsys.readouterr().err


def test_a_raise_later_in_the_sweep_cannot_swallow_the_page(lake_root, monkeypatch):
    # The loss this guards is permanent. A drifted ticker-day that sealed has had its
    # segments unlinked, so the next run finds nothing to merge for it and never runs the
    # check again. A raise on a later ticker-day would carry the finding out of the run
    # with the phone silent and no second chance, which is why the page sits in a
    # ``finally`` around the whole sweep rather than after it.
    _drifted_day(lake_root, monkeypatch, tickers=("SPY",))
    real = compact_module._prune_empty

    def refuse(date_dir):
        real(date_dir)
        raise OSError("the journal directory went away")

    monkeypatch.setattr(compact_module, "_prune_empty", refuse)
    publisher, transport = _paging(lake_root)

    with pytest.raises(OSError, match="went away"):
        _run(lake_root, publisher=publisher)

    assert len(transport.messages) == 1
    assert COLUMN in transport.messages[0].body


def test_the_page_goes_out_before_the_backup(lake_root, monkeypatch):
    # The backup shells out to ``rsync`` and a raise from it propagates out of the run.
    # Paging after it would let an unplugged drive swallow the drift page, which is the
    # one signal the missed compaction ping cannot name.
    class Unplugged:
        def sync(self, source, target):
            raise RuntimeError("backup target not mounted")

    _drifted_day(lake_root, monkeypatch)
    publisher, transport = _paging(lake_root)

    with pytest.raises(RuntimeError, match="backup target not mounted"):
        _run(lake_root, backup=Unplugged(), publisher=publisher)

    assert len(transport.messages) == 1
    assert COLUMN in transport.messages[0].body


def test_a_drift_that_names_no_column_still_says_so(lake_root, monkeypatch):
    # ``SchemaDrift`` allows all three lists to be empty. A nullability change is the
    # difference that reaches here naming nothing, and the page has to say that plainly
    # rather than trailing off after "did not carry".
    index = CHAINS_SCHEMA.get_field_index(COLUMN)
    field = CHAINS_SCHEMA.field(index)
    renullable = CHAINS_SCHEMA.set(index, field.with_nullable(not field.nullable))
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, renullable)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    finding = _findings(lake_root)[0]
    assert (finding["missing"], finding["unexpected"], finding["retyped"]) == ([], [], [])
    assert "no column named" in transport.messages[0].body


def test_a_run_with_no_publisher_still_files_and_says_so_on_stderr(lake_root, monkeypatch, capsys):
    # The seam is optional the way ``pinger`` is, and its default is ``None`` rather
    # than a live object, so a caller that omits it can never reach a real phone. What
    # such a run loses is only the page. launchd files the log the line lands in.
    _drifted_day(lake_root, monkeypatch)

    _run(lake_root)

    assert _findings(lake_root) != []
    err = capsys.readouterr().err
    assert SCHEMA_DRIFT_TITLE in err
    assert COLUMN in err


def test_a_write_that_cannot_land_still_pages(lake_root, monkeypatch):
    # The order inside ``_file_drift``. The drift is the fact and the file is the
    # record of it, so a run that could not write the record is a run whose only
    # remaining trace is the page and the log.
    _drifted_day(lake_root, monkeypatch)

    def refuse(*args, **kwargs):
        raise PermissionError("read-only lake")

    monkeypatch.setattr(compact_module, "write_schema_drift", refuse)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    assert _findings(lake_root) == []
    assert len(transport.messages) == 1


def test_the_repair_writes_its_finding_to_the_terminal_and_pages_nobody(
    lake_root, monkeypatch, capsys
):
    # ``recompact_ticker_day`` is human-invoked and takes no publisher, because the
    # operator who started it is reading its output. That only holds if the output says
    # so. A repair prints the partition and the row count and nothing else, so without
    # the stderr line an operator reads a clean-looking success over a ticker-day whose
    # merged schema was not the pinned one.
    _drifted_day(lake_root, monkeypatch)
    clock = ManualClock(_et(DAY, 16, 30))

    outcome = recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=clock)

    assert outcome.rows == 2
    assert len(_findings(lake_root)) == 1
    assert undelivered(lake_root, DAY) == 0
    err = capsys.readouterr().err
    assert SCHEMA_DRIFT_TITLE in err
    assert COLUMN in err


def test_a_page_the_record_could_not_keep_either_says_it_was_lost(lake_root, monkeypatch, capsys):
    # ``Delivery.recorded`` separates a page written down from one lost twice. Only the
    # second leaves nothing behind, so the log has to tell them apart. A publisher
    # pointed at a lake root that is not there refuses to write the record, because
    # creating it would turn "lake root missing" into a green check on the next attempt.
    class Broken:
        def send(self, message):
            raise ConnectionError("ntfy unreachable")

    _drifted_day(lake_root, monkeypatch)
    gone = lake_root.parent / "not-a-lake"
    publisher = Publisher(lake_root=gone, transport=Broken(), secrets=(PING_KEY, NTFY_TOPIC))

    _run(lake_root, publisher=publisher)

    assert not gone.exists()
    err = capsys.readouterr().err
    assert "page not sent: post_failed, lost" in err


def test_a_page_carrying_a_secret_is_refused_and_stays_off_the_log(lake_root, monkeypatch, capsys):
    # The publisher redacts a refused page's title from its own record, because the title
    # is what the refusal objected to. A producer that printed the body first would route
    # the secret around that seam and into the launchd log.
    _drifted_day(lake_root, monkeypatch)
    publisher, transport = _paging(lake_root)
    monkeypatch.setattr(compact_module, "_drift_body", lambda drifted: f"the topic is {NTFY_TOPIC}")

    _run(lake_root, publisher=publisher)

    assert transport.messages == []
    err = capsys.readouterr().err
    assert "refused: it carried a secret" in err
    assert NTFY_TOPIC not in err


def test_a_drift_too_wide_for_one_message_is_capped_and_counted(lake_root, monkeypatch):
    # ntfy's default body limit is 4096 bytes and it answers an oversize POST with a 400,
    # which ``NtfyTransport`` does not retry. A whole-schema rename names every column,
    # so the widest drift would be the one page that never landed. The count is what
    # survives the cut, because it is what separates one moved column from a rename.
    wide = pa.schema([pa.field(f"renamed_{index:03d}", pa.string()) for index in range(60)])
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0))),
        start_ts="a",
    )
    _pin(monkeypatch, wide)
    publisher, transport = _paging(lake_root)

    _run(lake_root, publisher=publisher)

    body = transport.messages[0].body
    assert len(body.encode("utf-8")) < 4096
    assert "renamed_011" in body
    assert "renamed_012" not in body
    assert f"and {60 - compact_module.PAGE_COLUMN_CAP} more" in body


def test_main_builds_the_publisher_that_pages(lake_root, monkeypatch, tmp_path):
    # A ``main`` builds its live seams itself. The ntfy POST reaches a phone, so a
    # ``main`` that accepted it would let a test hand it a fake from inside the place
    # that looked sanctioned. Two things have to be true of what it built.
    #
    # 1. The page goes over the real POST, carrying the config's topic.
    # 2. The publisher holds both secrets, which is what makes it refuse a page that
    #    carries either one.
    topics: list[str] = []
    transport = FakeTransport()

    def fake_transport(topic: str) -> FakeTransport:
        topics.append(topic)
        return transport

    seen: dict = {}

    def fake_compact(*args, **kwargs) -> CompactionResult:
        seen.update(kwargs)
        return CompactionResult(
            sealed=(), verified=(), skipped=(), retune=None, backed_up=True, pinged=True
        )

    monkeypatch.setattr(compact_module, "NtfyTransport", fake_transport)
    monkeypatch.setattr(compact_module, "compact", fake_compact)
    config = write_config(tmp_path, lake_root)

    compact_module.main(["--config", str(config)])

    publisher = seen["publisher"]
    assert isinstance(publisher, Publisher)
    assert topics == [NTFY_TOPIC]

    page = Message(event=SCHEMA_DRIFT_EVENT, title=SCHEMA_DRIFT_TITLE, body="a column moved")
    assert publisher.publish(page, now=_et(DAY, 16, 30)).sent
    assert transport.messages == [page]

    leaked = Message(event="probe", title="t", body=f"the key is {PING_KEY}")
    assert publisher.publish(leaked, now=_et(DAY, 16, 30)).reason == "refused"


# -- 9. a merge the segment types refused ------------------------------------


def _conflicted_day(lake_root: Path, ticker: str = "SPY", day: date = DAY) -> tuple[Path, Path]:
    """One ticker-day whose two segments hold ``COLUMN`` at different types.

    This is the drift the merge never lets the pinned-schema comparison see.
    ``pa.concat_tables`` refuses it outright, so the ticker-day has no merged schema and
    no partition, and the finding has to come from the refusal itself.
    """
    index = CHAINS_SCHEMA.get_field_index(COLUMN)
    retyped = CHAINS_SCHEMA.set(index, pa.field(COLUMN, pa.float64()))
    morning = _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(day, 0), ticker=ticker)),
        start_ts="a",
        ticker=ticker,
        day=day,
    )
    afternoon = _segment(
        lake_root,
        retyped,
        _table(retyped, _rows(3, snap_ts=_snap(day, 1), ticker=ticker)),
        start_ts="b",
        ticker=ticker,
        day=day,
    )
    return morning, afternoon


def test_a_refused_merge_files_a_finding_naming_both_types(lake_root):
    # The refusal commits no seal, so ``_seal``'s own filing, which waits for the manifest
    # append, can never reach it. The sweep files this one instead. ``SchemaDrift`` already
    # carries what it has to say: the column and both its types under ``retyped``, the
    # segments still on disk, and the Parquet path that was not written.
    morning, afternoon = _conflicted_day(lake_root)

    result, _ = _run(lake_root)

    (finding,) = _findings(lake_root)
    assert finding["retyped"] == [f"{COLUMN}: int64 -> double"]
    assert finding["missing"] == [] and finding["unexpected"] == []
    assert finding["surface"] == "chains" and finding["ticker"] == "SPY"
    # The discriminator. ``retyped`` renders ``pinned -> merged`` on a sealed finding and
    # ``earlier -> later`` on this one, and the two need opposite responses. Without this
    # flag a reader holding one file would have to go and check whether the partition
    # exists to tell which kind it has.
    assert finding["refused"] is True
    assert finding["schema_version"] == journal.SCHEMA_VERSION
    assert finding["partition"] == result.refused[0].partition
    assert finding["segments"] == [
        str(morning.relative_to(lake_root)),
        str(afternoon.relative_to(lake_root)),
    ]
    # The finding names a Parquet that is not there, which is the point of naming it.
    assert not (lake_root / finding["partition"]).exists()


def test_an_all_null_segment_is_not_named_as_the_conflicting_column(lake_root):
    # ``promote_options="default"`` resolves a null-typed column against any type, so a
    # segment whose column was all nulls never causes the refusal. Counting it would name
    # the wrong pair of types and point the reader at the wrong segment. Three segments:
    # the column all-null in the first, int in the second, float in the third. The refusal
    # is between the second and the third, and only those two types belong in the finding.
    index = CHAINS_SCHEMA.get_field_index(COLUMN)
    nulled = CHAINS_SCHEMA.set(index, pa.field(COLUMN, pa.null()))
    retyped = CHAINS_SCHEMA.set(index, pa.field(COLUMN, pa.float64()))
    rows = _rows(2, snap_ts=_snap(DAY, 0))
    for row in rows:
        row[COLUMN] = None
    _segment(lake_root, nulled, _table(nulled, rows), start_ts="a")
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 1))),
        start_ts="b",
    )
    _segment(lake_root, retyped, _table(retyped, _rows(2, snap_ts=_snap(DAY, 2))), start_ts="c")

    result, _ = _run(lake_root)

    (refused,) = result.refused
    assert refused.conflicts == (f"{COLUMN}: int64 -> double",)
    assert len(refused.segments) == 3


def test_a_conflict_the_scan_cannot_explain_still_names_arrows_own_message(lake_root):
    # The scan models the refusals this code anticipates, and Arrow may refuse for one it
    # does not. The exception has to stay legible then rather than printing an empty list,
    # because that message is what reaches the operator running the repair by hand.
    conflict = compact_module.SegmentSchemaConflict(
        surface="chains",
        ticker="SPY",
        day=DAY,
        partition="chains/ticker=SPY/date=2026-08-24.parquet",
        segments=("journal/date=2026-08-24/surface=chains/ticker=SPY/seg-a-1.arrows",),
        conflicts=(),
        detail="Unable to merge: some shape this scan does not model",
    )

    assert "some shape this scan does not model" in str(conflict)
    assert conflict.partition in str(conflict)
    # The finding it files lists nothing, which ``SchemaDrift`` allows, and still names
    # the ticker-day to go and look at.
    drift = compact_module._refused_drift(conflict)
    assert drift.retyped == () and drift.missing == () and drift.unexpected == ()
    assert drift.refused is True and drift.ticker == "SPY"


def test_a_run_whose_only_drift_is_a_refusal_still_pages(lake_root):
    # The integration that makes containment safe rather than a regression. Before #188 a
    # raise was the only thing a retype sent to a human. Catching it without adding the
    # finding to what the run pages from would trade a nightly page for a file nobody
    # reads, which is the gap #188 closed. Nothing here drifts past the merge, so this
    # page exists only because the refusal reached ``_page_drift``.
    _conflicted_day(lake_root)
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert result.sealed == () and len(result.refused) == 1
    assert len(transport.messages) == 1
    page = transport.messages[0]
    assert page.event == SCHEMA_DRIFT_EVENT
    assert page.title == SCHEMA_DRIFT_TITLE
    assert page.priority == 5
    assert f"{COLUMN}: int64 -> double" in page.body
    assert DAY.isoformat() in page.body
    # The remedy has to match the producer. #184 established that correcting the pinned
    # schema does not touch a disagreement between two segments, so a page prescribing the
    # bump here would send the reader down a path that changes nothing.
    assert "refused outright" in page.body
    assert "schema_version" not in page.body
    # The page goes out and the backup and the ping still run. The refusal costs neither.
    assert events == ["backup", "ping"]


def test_a_refusal_and_a_surviving_drift_fold_into_one_page(lake_root, monkeypatch):
    # One bad release drifts every ticker-day still in flight, and which shape each one
    # takes depends only on which segments it happened to have. So the two paths file
    # separately and page together, the same fold a wide drift already gets.
    _conflicted_day(lake_root)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0), ticker="ZZZ")),
        start_ts="a",
        ticker="ZZZ",
    )
    # ZZZ merges cleanly and drifts against the pinned schema. SPY never gets that far.
    _pin(monkeypatch, _without(CHAINS_SCHEMA, COLUMN))
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    assert len(result.sealed) == 1 and len(result.refused) == 1
    assert sorted(finding["ticker"] for finding in _findings(lake_root)) == ["SPY", "ZZZ"]
    assert len(transport.messages) == 1
    body = transport.messages[0].body
    assert body.startswith("2 ticker-day(s)")
    assert f"{COLUMN}: int64 -> double" in body
    assert f"unexpected {COLUMN}" in body
    # A folded page must say which half each remedy applies to. One sealed and wants the
    # schema bump, one was refused and does not.
    assert "1 sealed, so correct the schema and bump schema_version" in body
    assert "1 was refused outright" in body


def test_a_refused_ticker_day_is_filed_again_on_every_run(lake_root):
    # The cadence. A sealed ticker-day files once and its manifest entry is what makes the
    # later silence readable. A refused one has no entry, so a writer that filed once would
    # make night two's silence consistent with three worlds at once: fixed, still broken
    # and already filed, or gone. De-duplication is the reader's policy, and a reader
    # cannot undo a record that was never written.
    _conflicted_day(lake_root)
    publisher, transport = _paging(lake_root)

    # Two nights, at different times of day. The finding's file name is a time-of-day
    # stamp, a surface, a ticker, and a pid, with no date in it, and the directory above
    # it is keyed on the session day rather than the night. So two runs at the same
    # microsecond-of-day over one drifted session day write the same name, and the second
    # is swallowed. A real clock makes that a one-in-billions coincidence, and #191 is
    # where the structural version of it lives. Moving the second run's clock keeps this
    # test on the cadence rather than on the file name.
    _run(lake_root, publisher=publisher)
    _run(lake_root, clock=ManualClock(_et(TUESDAY, 16, 31)), publisher=publisher)

    # Both land under the session day they are about, which is the day that drifted and
    # not the night that found it.
    assert len(_findings(lake_root)) == 2
    assert len(transport.messages) == 2


def test_a_refusal_that_cannot_be_filed_still_pages_and_still_seals(lake_root, monkeypatch):
    # ``_file_drift`` swallows a write that fails, and the refusal path goes through it for
    # that reason. A full disk must not turn one refused ticker-day back into a lost run.
    # The page goes out anyway, because the finding is remembered before the write is tried.
    _conflicted_day(lake_root)
    _segment(
        lake_root,
        CHAINS_SCHEMA,
        _table(CHAINS_SCHEMA, _rows(2, snap_ts=_snap(DAY, 0), ticker="ZZZ")),
        start_ts="a",
        ticker="ZZZ",
    )

    def boom(*args, **kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr(compact_module, "write_schema_drift", boom)
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert len(result.refused) == 1 and len(result.sealed) == 1
    assert _findings(lake_root) == []
    assert len(transport.messages) == 1
    assert events == ["backup", "ping"]


def test_the_repair_lets_a_refused_merge_out(lake_root):
    # The scheduled sweep contains the refusal because a raise there costs every other
    # ticker-day, the backup, and the ping. A hand-run repair has no other ticker-day to
    # protect, so the operator gets the failure named on their own terminal instead of a
    # silent no-op. It cannot clear the conflict either: it reaches the same merge.
    morning, afternoon = _conflicted_day(lake_root)
    morning_before = morning.read_bytes()

    with pytest.raises(compact_module.SegmentSchemaConflict) as raised:
        recompact_ticker_day(lake_root, "chains", "SPY", DAY, clock=ManualClock(_et(DAY, 17, 0)))

    conflict = raised.value
    assert conflict.conflicts == (f"{COLUMN}: int64 -> double",)
    assert conflict.ticker == "SPY" and conflict.day == DAY
    assert f"{COLUMN}: int64 -> double" in str(conflict)
    assert morning.read_bytes() == morning_before and afternoon.exists()
    assert read_manifest(lake_root) == []
