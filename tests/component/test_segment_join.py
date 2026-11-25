"""The join every by-name reader of a ticker-day's segments takes, and what the seal makes of it.

The seal runs on a live capture host and deletes the segments it joins, and captured data cannot
be recaptured. So each way a day's segments can disagree is sealed here and compared with the
whole table written out, which shares nothing with the code. The schema is this file's own, so a
column the lake pins later moves none of it.

``lake.segment_join`` is also a published contract (marketlake #1004), so the last tests hold that
the seal, the door and the measurements each import the one join. A caller that stopped calling it
would leave the import unused, which ruff's F401 refuses.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import compact as compact_module
from lake import journal, measure, segment_join, unsealed
from lake.calendar import MARKET_TZ
from lake.compact import CompactionResult, compact
from lake.paths import LakePaths
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger

DAY = date(2026, 8, 24)

PINNED = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("bid", pa.float64()),
        ("size", pa.int64()),
        ("row_kind", pa.string()),
    ]
)
NAMES = ["snap_ts", "bid", "size", "row_kind"]


def _snap(minute: int) -> str:
    return f"2026-08-24T13:{minute}:00+00:00"


def _segment(root: Path, surface: str, minute: int, schema: pa.Schema, *values: object) -> Path:
    """One closed one-row segment, its values in the schema's order behind ``snap_ts``."""
    row = {"snap_ts": _snap(minute), **dict(zip(_names(schema), values, strict=True))}
    path = journal.segment_path(root, surface, "SPY", DAY, f"20260824T13{minute}00", 4242)
    arrays = [pa.array([row[field.name]], type=field.type) for field in schema]
    with journal.SegmentWriter(path, schema) as writer:
        writer.write_cycle(pa.Table.from_arrays(arrays, schema=schema))
    return path


def _names(schema: pa.Schema) -> list[str]:
    return [name for name in schema.names if name != "snap_ts"]


def _without(name: str) -> pa.Schema:
    return pa.schema([field for field in PINNED if field.name != name])


def _retyped(name: str, kind: pa.DataType) -> pa.Schema:
    return PINNED.set(PINNED.get_field_index(name), pa.field(name, kind))


def _seal(root: Path) -> CompactionResult:
    events: list[str] = []
    sessions = {
        day: SessionTimes(
            open=datetime.combine(day, time(9, 30), tzinfo=MARKET_TZ),
            close=datetime.combine(day, time(16, 0), tzinfo=MARKET_TZ),
        )
        for day in (DAY - timedelta(days=3), DAY, DAY + timedelta(days=1))
    }
    return compact(
        root,
        clock=ManualClock(datetime.combine(DAY, time(16, 30), tzinfo=MARKET_TZ)),
        calendar=FakeCalendar(sessions),
        backup=FakeBackup(events),
        backup_target=Path("/ssd/lake"),
        pinger=FakePinger(events),
        ping_url="https://hc-ping.com/secret-key/compaction",
        plan_path=root.parent / "chain_plan.json",
    )


@pytest.fixture(params=["quotes", "chains"])
def surface(request, monkeypatch) -> str:
    real = journal.schema_for
    monkeypatch.setattr(
        journal, "schema_for", lambda asked: PINNED if asked == request.param else real(asked)
    )
    return request.param


def _sealed(root: Path, surface: str) -> pa.Table:
    result = _seal(root)
    assert [item.surface for item in result.sealed] == [surface]
    assert not any((root / "journal").rglob("*.arrows"))
    return pq.read_table(LakePaths(root).partition_path(surface, "SPY", DAY), partitioning=None)


def _holds(table: pa.Table, names: list[str], types: dict[str, pa.DataType], rows: list) -> None:
    assert table.schema.names == names
    assert table.schema.types == [types.get(name, PINNED.field(0).type) for name in names]
    assert [tuple(row.values()) for row in table.to_pylist()] == rows


TYPES = {"bid": pa.float64(), "size": pa.int64()}


# -- what each day seals to ---------------------------------------------------


def test_one_schema(lake_root, surface):
    _segment(lake_root, surface, 31, PINNED, 1.5, 10, "data")
    _segment(lake_root, surface, 32, PINNED, 2.5, None, "gap")
    rows = [(_snap(31), 1.5, 10, "data"), (_snap(32), 2.5, None, "gap")]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_a_column_moved(lake_root, surface):
    moved = pa.schema([PINNED.field("size"), *_without("size")])
    _segment(lake_root, surface, 31, moved, 10, 1.5, "data")
    _segment(lake_root, surface, 32, PINNED, 2.5, 20, "data")
    rows = [(_snap(31), 1.5, 10, "data"), (_snap(32), 2.5, 20, "data")]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_a_column_added(lake_root, surface):
    _segment(lake_root, surface, 31, _without("size"), 1.5, "data")
    _segment(lake_root, surface, 32, PINNED, 2.5, 20, "data")
    rows = [(_snap(31), 1.5, None, "data"), (_snap(32), 2.5, 20, "data")]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_a_column_dropped(lake_root, surface):
    _segment(lake_root, surface, 31, PINNED, 1.5, 10, "data")
    _segment(lake_root, surface, 32, _without("size"), 2.5, "data")
    rows = [(_snap(31), 1.5, 10, "data"), (_snap(32), 2.5, None, "data")]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_two_columns_nothing_pins_stay_behind_the_pinned_ones_in_the_order_met(lake_root, surface):
    wide = pa.schema([("z", pa.string()), *PINNED, ("a", pa.string())])
    _segment(lake_root, surface, 31, wide, "z1", 1.5, 10, "data", "a1")
    _segment(lake_root, surface, 32, PINNED, 2.5, 20, "data")
    rows = [
        (_snap(31), 1.5, 10, "data", "z1", "a1"),
        (_snap(32), 2.5, 20, "data", None, None),
    ]
    _holds(_sealed(lake_root, surface), [*NAMES, "z", "a"], TYPES, rows)


def test_a_null_column_takes_the_other_segments_type(lake_root, surface):
    _segment(lake_root, surface, 31, _retyped("size", pa.null()), 1.5, None, "data")
    _segment(lake_root, surface, 32, PINNED, 2.5, 20, "data")
    rows = [(_snap(31), 1.5, None, "data"), (_snap(32), 2.5, 20, "data")]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_a_torn_segment_gives_the_rows_before_the_tear(lake_root, surface):
    _segment(lake_root, surface, 31, PINNED, 1.5, 10, "data")
    torn = journal.segment_path(lake_root, surface, "SPY", DAY, "20260824T133200", 4242)
    with journal.SegmentWriter(torn, PINNED) as writer:
        for minute, bid in ((32, 2.5), (33, 3.5)):
            row = {"snap_ts": _snap(minute), "bid": bid, "size": 20, "row_kind": "data"}
            writer.write_cycle(pa.Table.from_pylist([row], schema=PINNED))
    torn.write_bytes(torn.read_bytes()[:-60])
    assert journal.read_segment(torn).num_rows == 1
    _segment(lake_root, surface, 34, PINNED, 4.5, 40, "data")
    rows = [
        (_snap(31), 1.5, 10, "data"),
        (_snap(32), 2.5, 20, "data"),
        (_snap(34), 4.5, 40, "data"),
    ]
    _holds(_sealed(lake_root, surface), NAMES, TYPES, rows)


def test_the_first_segments_metadata_is_the_partitions(lake_root, surface):
    _segment(lake_root, surface, 31, PINNED.with_metadata({b"k": b"first"}), 1.5, 10, "data")
    _segment(lake_root, surface, 32, PINNED.with_metadata({b"k": b"later"}), 2.5, 20, "data")
    # All of it, because a partition's metadata is part of what the seal writes.
    assert _sealed(lake_root, surface).schema.metadata == {b"k": b"first"}


@pytest.mark.parametrize("first", ["pinned", "retyped"])
@pytest.mark.parametrize("kind", [pa.float64(), pa.int32(), pa.string()])
def test_a_retyped_column_seals_nothing_and_keeps_both_segments(lake_root, surface, kind, first):
    value = "20" if kind == pa.string() else 20
    pinned_at, retyped_at = (31, 32) if first == "pinned" else (32, 31)
    paths = [
        _segment(lake_root, surface, pinned_at, PINNED, 1.5, 10, "data"),
        _segment(lake_root, surface, retyped_at, _retyped("size", kind), 2.5, value, "data"),
    ]
    before = [path.read_bytes() for path in paths]
    result = _seal(lake_root)
    assert result.sealed == ()
    assert [item.surface for item in result.refused] == [surface]
    assert [path.read_bytes() for path in paths] == before
    assert not LakePaths(lake_root).partition_path(surface, "SPY", DAY).exists()


# -- the two functions -------------------------------------------------------


def test_the_join_fills_a_missing_column_with_nulls_in_the_order_given():
    joined = segment_join.merge_by_name(
        [pa.table({"a": [1], "b": ["x"]}), pa.table({"c": [2.5], "a": [2]})]
    )
    assert joined.to_pydict() == {"a": [1, 2], "b": ["x", None], "c": [None, 2.5]}
    assert joined.schema.types == [pa.int64(), pa.string(), pa.float64()]


@pytest.mark.parametrize("kind", [pa.float64(), pa.int32(), pa.string(), pa.bool_()])
def test_the_join_refuses_a_column_at_two_types_in_either_order(kind):
    tables = [pa.table({"a": pa.array([1])}), pa.table({"a": pa.array([None], type=kind)})]
    for ordered in (tables, tables[::-1]):
        with pytest.raises(pa.ArrowTypeError):
            segment_join.merge_by_name(ordered)


def test_the_order_is_the_pinned_one_with_the_rest_kept_behind_it():
    pinned = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("absent", pa.int64())])
    table = pa.table({"z": [4], "b": [2], "y": [3], "a": [1]})
    ordered = segment_join.pinned_order(table, pinned)
    assert ordered.schema.names == ["a", "b", "z", "y"]
    assert ordered.to_pylist() == [{"a": 1, "b": 2, "z": 4, "y": 3}]


# -- one join, not three -----------------------------------------------------


@pytest.mark.parametrize("module", [compact_module, unsealed, measure])
def test_each_by_name_reader_imports_the_one_join(module):
    assert module.merge_by_name is segment_join.merge_by_name


@pytest.mark.parametrize("module", [compact_module, unsealed])
def test_the_seal_and_the_door_import_the_one_order(module):
    assert module.pinned_order is segment_join.pinned_order
