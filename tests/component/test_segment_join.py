"""The seal's output did not move when its join became ``lake.segment_join`` (marketlake #1004).

Captured data cannot be recaptured, and the seal runs on a live capture host and deletes the
segments it joins. So the proof is on the bytes. Each day below is sealed twice, once by the
code as it stands and once with the join the seal held before the move, which is written out
here and shares nothing with the module. The two partitions have to be the same file.

The join the seal held stays written out on purpose. ``lake.segment_join`` is a published
contract, and a change to what it does has to be made here too, in the same diff.
"""

from __future__ import annotations

import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import compact as compact_module
from lake import journal, segment_join
from lake.calendar import MARKET_TZ
from lake.compact import CompactionResult, compact
from lake.journal import CHAINS_SCHEMA, QUOTES_SCHEMA
from lake.paths import LakePaths
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger

DAY = date(2026, 8, 24)
SCHEMAS = {"chains": CHAINS_SCHEMA, "quotes": QUOTES_SCHEMA}

# One column of each surface to move about, off the middle of its pinned schema.
MOVED = {"chains": "open_interest", "quotes": "bid_size"}


def _seal_joined(tables: list[pa.Table]) -> pa.Table:
    """The line ``compact._seal`` merged with before the move."""
    return pa.concat_tables(tables, promote_options="default")


def _seal_ordered(table: pa.Table, pinned: pa.Schema) -> pa.Table:
    """The body of ``compact._pinned_order`` before the move."""
    present = set(table.schema.names)
    named = set(pinned.names)
    order = [name for name in pinned.names if name in present]
    order += [name for name in table.schema.names if name not in named]
    if order == table.schema.names:
        return table
    return table.select(order)


def _rows(surface: str, minute: int, **over: object) -> list[dict]:
    stamp = f"2026-08-24T13:{minute:02d}:00+00:00"
    row = {
        "snap_ts": stamp,
        "fetch_ts": stamp,
        "fetch_end_ts": stamp,
        "ticker": "SPY",
        "bid": 1.25 + minute,
        MOVED[surface]: 100 + minute,
        "row_kind": "data",
        "suspect": False,
        "schema_version": 2,
        **over,
    }
    if surface == "chains":
        return [{**row, "occ_symbol": f"SPY   260918C00{strike}000"} for strike in (640, 650)]
    return [row]


def _segment(
    root: Path, surface: str, minute: int, schema: pa.Schema, *, rows: list[dict] | None = None
) -> None:
    rows = _rows(surface, minute) if rows is None else rows
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in schema]
    path = journal.segment_path(root, surface, "SPY", DAY, f"20260824T13{minute:02d}00", 4242)
    with journal.SegmentWriter(path, schema) as writer:
        writer.write_cycle(pa.Table.from_arrays(arrays, schema=schema))


def _without(schema: pa.Schema, *names: str) -> pa.Schema:
    return pa.schema([field for field in schema if field.name not in names])


def _retyped(schema: pa.Schema, name: str, kind: pa.DataType) -> pa.Schema:
    return schema.set(schema.get_field_index(name), pa.field(name, kind))


def _day(root: Path, surface: str, shape: str) -> None:
    """One day's segments, in the shape named."""
    pinned = SCHEMAS[surface]
    column = MOVED[surface]
    moved = pa.schema([pinned.field(column), *_without(pinned, column)])
    extra = pinned.append(pa.field("vendor_new_field", pa.string()))
    other = {
        "one schema": pinned,
        "a column moved": moved,
        "a column added": pinned,
        "a column dropped": _without(pinned, column),
        "a column nothing pins": extra,
        "a null column": _retyped(pinned, column, pa.null()),
        "schema metadata": pinned.with_metadata({b"origin": b"later"}),
        "a gap row": pinned,
        "a torn tail": pinned,
        "a retyped column": _retyped(pinned, column, pa.float64()),
    }[shape]
    first = _without(pinned, column) if shape == "a column added" else pinned
    _segment(root, surface, 31, first)
    if shape == "a null column":
        _segment(root, surface, 32, other, rows=_rows(surface, 32, **{column: None}))
    elif shape == "a gap row":
        gap = {"row_kind": "gap", "error_class": "timeout", "bid": None, column: None}
        _segment(root, surface, 32, other, rows=_rows(surface, 32, **gap)[:1])
    else:
        _segment(root, surface, 32, other)
    _segment(root, surface, 33, pinned)
    if shape == "a torn tail":
        path = journal.segment_path(root, surface, "SPY", DAY, "20260824T133400", 4242)
        _segment(root, surface, 34, pinned)
        path.write_bytes(path.read_bytes()[:-400])


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


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


SHAPES = [
    "one schema",
    "a column moved",
    "a column added",
    "a column dropped",
    "a column nothing pins",
    "a null column",
    "schema metadata",
    "a gap row",
    "a torn tail",
]


@pytest.mark.parametrize("surface", SCHEMAS)
@pytest.mark.parametrize("shape", SHAPES)
def test_a_day_seals_to_the_bytes_it_sealed_to_before_the_move(
    tmp_path, monkeypatch, surface, shape
):
    now = tmp_path / "now" / "lake"
    _day(now, surface, shape)
    before = tmp_path / "before" / "lake"
    shutil.copytree(now, before)

    result = _seal(now)
    monkeypatch.setattr(compact_module, "merge_by_name", _seal_joined)
    monkeypatch.setattr(compact_module, "pinned_order", _seal_ordered)
    _seal(before)

    # Every file the seal left, the manifest and any finding among them, is the same.
    assert [item.surface for item in result.sealed] == [surface]
    assert _tree(now) == _tree(before)
    assert LakePaths(now).partition_path(surface, "SPY", DAY).is_file()


@pytest.mark.parametrize("surface", SCHEMAS)
def test_a_retyped_day_is_refused_as_it_was_before_the_move(tmp_path, monkeypatch, surface):
    now = tmp_path / "now" / "lake"
    _day(now, surface, "a retyped column")
    before = tmp_path / "before" / "lake"
    shutil.copytree(now, before)

    result = _seal(now)
    monkeypatch.setattr(compact_module, "merge_by_name", _seal_joined)
    _seal(before)

    assert result.sealed == ()
    assert [item.surface for item in result.refused] == [surface]
    assert _tree(now) == _tree(before)


# -- the two functions, against literals -------------------------------------


def test_the_join_fills_a_missing_column_with_nulls_in_the_order_given():
    joined = segment_join.merge_by_name(
        [pa.table({"a": [1], "b": ["x"]}), pa.table({"c": [2.5], "a": [2]})]
    )
    assert joined.to_pydict() == {"a": [1, 2], "b": ["x", None], "c": [None, 2.5]}
    assert joined.schema.types == [pa.int64(), pa.string(), pa.float64()]


@pytest.mark.parametrize("kind", [pa.float64(), pa.int32(), pa.string(), pa.bool_()])
def test_the_join_refuses_a_column_at_two_types(kind):
    with pytest.raises(pa.ArrowTypeError):
        segment_join.merge_by_name(
            [pa.table({"a": pa.array([1])}), pa.table({"a": pa.array([None], type=kind)})]
        )


def test_the_order_is_the_pinned_one_with_the_rest_kept_behind_it():
    pinned = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("absent", pa.int64())])
    table = pa.table({"z": [4], "b": [2], "y": [3], "a": [1]})
    ordered = segment_join.pinned_order(table, pinned)
    assert ordered.schema.names == ["a", "b", "z", "y"]
    assert ordered.to_pylist() == [{"a": 1, "b": 2, "z": 4, "y": 3}]


def test_a_sealed_partition_holds_the_joined_rows(tmp_path):
    root = tmp_path / "lake"
    _day(root, "quotes", "a column added")
    _seal(root)
    table = pq.read_table(LakePaths(root).quotes_partition_path("SPY", DAY))
    assert table.column("bid_size").to_pylist() == [None, 132, 133]
    assert table.column("bid").to_pylist() == [32.25, 33.25, 34.25]
    assert table.schema.names == QUOTES_SCHEMA.names
