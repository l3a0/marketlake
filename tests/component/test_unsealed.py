"""The door onto a ticker-day's quotes before the seal, over real segments (marketlake #1004).

Every segment is a real Arrow IPC file, and what the door returns is compared with what the real
``compact`` seals. ``compact`` deletes the segments, so the door always reads first. Expected
values on a day whose schema changes are literals too, because the door and the seal share the
join and would agree on a wrong one.

The sections follow the acceptance list on the issue, whose first item is one schema and whose
last is the mutations these tests have to fail. Its item 11, the measurements' reader, is in
``test_measure_queries.py``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from lake import capture, config, journal, loader, unsealed
from lake.calendar import MARKET_TZ
from lake.chain_plan import ChainPlan
from lake.compact import CompactionResult, compact
from lake.journal import QUOTES_SCHEMA
from lake.loader import SnapAbsent, load_quotes
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.schema_versions import (
    RecordedVersion,
    SchemaVersionLedger,
    ledger_path,
    running_fingerprints,
)
from lake.tickers import Roster
from lake.unsealed import (
    SchemaVersionUnknown,
    SegmentAbsent,
    SegmentMismatch,
    UnsealedArgument,
    UnsealedRefused,
    captured_by,
    load_unsealed_quotes,
    segment_prefix,
)
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger

REPO = Path(__file__).resolve().parents[2]
DAY = date(2026, 8, 24)
DAY_TEXT = "2026-08-24"
LABEL = "SPY 2026-08-24"
PID = 4242
PREFIX = "journal/date=2026-08-24/surface=quotes/ticker=SPY/"
PARTITION = "quotes/ticker=SPY/date=2026-08-24.parquet"
EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"
LATE = datetime(2026, 8, 24, 20, 30, tzinfo=UTC)
ZEROS = "0" * 64


@pytest.fixture(autouse=True)
def _running_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every row below is at version 2, so version 1 is older and 3 unknown at any later bump."""
    monkeypatch.setattr(journal, "SCHEMA_VERSION", 2)


# -- building the day --------------------------------------------------------


def _utc(hour: int, minute: int, second: int = 0, micro: int = 0) -> datetime:
    return datetime(2026, 8, 24, hour, minute, second, micro, tzinfo=UTC)


def _row(snap: datetime, end: datetime | str | None, **over: object) -> dict:
    return {
        "snap_ts": snap.isoformat(),
        "fetch_ts": snap.isoformat(),
        "fetch_end_ts": end.isoformat() if isinstance(end, datetime) else end,
        "vendor_quote_ts": snap.isoformat(),
        "ticker": "SPY",
        "bid": 649.98,
        "ask": 650.02,
        "last": 650.0,
        "bid_size": 3,
        "security_status": "Normal",
        "row_kind": "data",
        "suspect": False,
        "schema_version": 2,
        **over,
    }


def _gap(snap: datetime, end: datetime | None, **over: object) -> dict:
    return {
        "snap_ts": snap.isoformat(),
        "fetch_end_ts": None if end is None else end.isoformat(),
        "ticker": "SPY",
        "row_kind": "gap",
        "error_class": "timeout",
        "suspect": False,
        "schema_version": 2,
        **over,
    }


def _table(rows: list[dict], schema: pa.Schema = QUOTES_SCHEMA) -> pa.Table:
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _write(
    root: Path, name: str, rows: list[dict] | pa.Table, schema: pa.Schema = QUOTES_SCHEMA
) -> str:
    """One closed segment of SPY's quotes, and the lake-relative path that names it."""
    path = journal.segment_path(root, "quotes", "SPY", DAY, name, PID)
    table = rows if isinstance(rows, pa.Table) else _table(rows, schema)
    with journal.SegmentWriter(path, table.schema) as writer:
        writer.write_cycle(table)
    return path.relative_to(root).as_posix()


def _minute(root: Path, minute: int, schema: pa.Schema = QUOTES_SCHEMA, **over: object) -> str:
    """One segment holding one data row, at 13:MM and landed a second later."""
    row = _row(_utc(13, minute), _utc(13, minute, 1), **over)
    return _write(root, f"20260824T13{minute:02d}00", [row], schema)


def _digest(root: Path, rel: str) -> str:
    return hashlib.sha256((root / rel).read_bytes()).hexdigest()


def _named(root: Path, *rels: str) -> dict[str, str]:
    return {rel: _digest(root, rel) for rel in rels}


def _read(root: Path, segments: dict[str, str], at: datetime = LATE) -> pa.Table:
    return load_unsealed_quotes("SPY", DAY, at, segments=segments, lake_root=root)


def _refusal(
    root: Path, segments: object, kind: type = UnsealedRefused, at: datetime = LATE
) -> UnsealedRefused:
    """The refusal, at exactly ``kind``, with every path in its message lake-relative.

    A refusal naming no segment says which ticker and day, unless it is an argument's.
    """
    with pytest.raises(loader.LoadError) as info:
        _read(root, segments, at)
    refused = info.value
    assert type(refused) is kind
    assert isinstance(refused, UnsealedRefused)
    assert str(root) not in str(refused)
    assert "include_quarantined" not in str(refused)
    if refused.segment is not None:
        assert refused.segment in str(refused)
    elif kind is not UnsealedArgument:
        assert LABEL in str(refused)
    return refused


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


def _sealed_rows(root: Path) -> pa.Table:
    table = pq.read_table(LakePaths(root).quotes_partition_path("SPY", DAY))
    return table.filter(pc.equal(table.column("row_kind"), "data"))


def _same(left: pa.Table, right: pa.Table) -> None:
    assert left.schema.names == right.schema.names
    assert left.schema.types == right.schema.types
    assert left.to_pylist() == right.to_pylist()


def _ledger(root: Path, *versions: int, without: dict[int, tuple[str, ...]] | None = None) -> Path:
    """The root's ledger, each version at the running shape less the columns named for it."""
    entries = []
    for version in versions:
        shapes = running_fingerprints()
        for column in (without or {}).get(version, ()):
            del shapes["quotes"][column]
        entries.append(RecordedVersion(version, datetime(2026, 8, 1, tzinfo=UTC), shapes))
    return SchemaVersionLedger(entries).write(ledger_path(root))


def _withhold(
    root: Path,
    partition: str = PARTITION,
    verdict: str = "quarantined",
    check: str = "spread",
) -> None:
    entry = {"partition": partition, "check": check, "verdict": verdict}
    with (root / "quarantine.jsonl").open("a") as ledger:
        ledger.write(json.dumps(entry) + "\n")


def _without(schema: pa.Schema, *names: str) -> pa.Schema:
    return pa.schema([field for field in schema if field.name not in names])


def _retyped(schema: pa.Schema, name: str, kind: pa.DataType) -> pa.Schema:
    return schema.set(schema.get_field_index(name), pa.field(name, kind))


def _pin(monkeypatch: pytest.MonkeyPatch, schema: pa.Schema) -> None:
    real = journal.schema_for
    monkeypatch.setattr(
        journal, "schema_for", lambda asked: schema if asked == "quotes" else real(asked)
    )


def _routed(field: str = "lastPrice", value: object = "n/a") -> str:
    """An ``extra`` holding one quote-block field, as the parser routes a retyped one."""
    return json.dumps({"quote": {field: value}})


# -- 1. one schema -----------------------------------------------------------


def _ordinary(root: Path) -> list[str]:
    """Several cycles, a failed one's gap row with a ``fetch_end_ts``, and a late cycle."""
    return [
        _minute(root, 31, bid=1.0),
        _write(root, "20260824T133200", [_gap(_utc(13, 32), _utc(13, 32, 5))]),
        _write(
            root,
            "20260824T133300",
            [
                # 13:33:59Z, spelled so that its text sorts after 13:34 does.
                _row(_utc(13, 33), "2026-08-24T14:33:59+01:00", bid=2.0),
                # 13:34:01Z, spelled so that its text sorts before 13:34 does.
                _row(_utc(13, 33), "2026-08-24T09:34:01-04:00", bid=3.0),
                _row(_utc(13, 33), _utc(13, 34), bid=4.0),
                _row(_utc(13, 33), _utc(13, 34, 0, 1), bid=5.0),
            ],
        ),
        # An earlier minute than the segments before it, landed after them.
        _write(root, "20260824T133500", [_row(_utc(13, 30), _utc(13, 35, 1), bid=6.0)]),
    ]


def test_a_captured_segment_reads_the_way_the_readme_calls_the_door(lake_root, cassette_vendor):
    # Capture's own segment, under the name capture gives it, with the digest its manifest entry
    # records and the day and root as text.
    capture.run_cycle(
        ManualClock(start=datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)),
        cassette_vendor,
        Roster.from_mapping(
            {ticker: {"options": True, "chain_cadence": "1m"} for ticker in ("SPY", "QQQ")}
        ),
        lake_root,
        pid=PID,
        plan=ChainPlan(((0, None),)),
    )
    prefix = segment_prefix("SPY", DAY_TEXT)
    segments = {
        rel: entry["sha256"]
        for rel, entry in latest_entries(lake_root).items()
        if rel.startswith(prefix)
    }
    assert list(segments) == [f"{PREFIX}seg-20260824T133045000000-4242.arrows"]

    answer = load_unsealed_quotes(
        "SPY", DAY_TEXT, LATE, segments=segments, lake_root=str(lake_root)
    )
    assert answer.num_rows == 1
    assert answer.column("snap_ts").to_pylist() == ["2026-08-24T13:30:00+00:00"]
    assert answer.column("bid").to_pylist() == [649.98]
    _seal(lake_root)
    _same(answer, _sealed_rows(lake_root))


def test_every_batch_of_a_segment_is_read(lake_root):
    path = journal.segment_path(lake_root, "quotes", "SPY", DAY, "a", PID)
    with journal.SegmentWriter(path, QUOTES_SCHEMA) as writer:
        for minute in (31, 32):
            row = _row(_utc(13, minute), _utc(13, minute, 1), bid=float(minute))
            writer.write_cycle(_table([row]))
    rel = path.relative_to(lake_root).as_posix()
    assert _read(lake_root, _named(lake_root, rel)).column("bid").to_pylist() == [31.0, 32.0]


def test_a_vendor_value_is_as_received(lake_root):
    # None of the three is a reason to drop a row: a zero price, a null one and capture's flag.
    rels = [
        _minute(lake_root, 31, bid=0.0),
        _minute(lake_root, 32, bid=None),
        _minute(lake_root, 33, bid=3.0, suspect=True),
    ]
    answer = _read(lake_root, _named(lake_root, *rels))
    assert answer.column("bid").to_pylist() == [0.0, None, 3.0]
    assert answer.column("suspect").to_pylist() == [False, False, True]


def test_a_row_of_neither_kind_is_not_returned(lake_root):
    rels = [_minute(lake_root, 31, bid=1.0), _minute(lake_root, 32, row_kind="backfill")]
    assert _read(lake_root, _named(lake_root, *rels)).column("bid").to_pylist() == [1.0]


def test_the_rows_equal_the_sealed_data_rows(lake_root):
    segments = _named(lake_root, *reversed(_ordinary(lake_root)))
    answer = _read(lake_root, segments)
    _seal(lake_root)
    _same(answer, _sealed_rows(lake_root))
    assert answer.column("bid").to_pylist() == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    assert answer.schema.names == QUOTES_SCHEMA.names
    assert answer.schema.types == QUOTES_SCHEMA.types


def test_the_cut_is_on_the_instant_the_response_landed(lake_root):
    segments = _named(lake_root, *_ordinary(lake_root))
    answer = _read(lake_root, segments, _utc(13, 34))
    assert answer.column("bid").to_pylist() == [1.0, 2.0, 4.0]


def test_at_is_compared_as_an_instant_too(lake_root):
    segments = _named(lake_root, *_ordinary(lake_root))
    at = datetime(2026, 8, 24, 9, 34, tzinfo=MARKET_TZ)
    assert _read(lake_root, segments, at).column("bid").to_pylist() == [1.0, 2.0, 4.0]


def test_no_row_by_at_is_an_empty_table_with_its_columns(lake_root):
    segments = _named(lake_root, *_ordinary(lake_root))
    answer = _read(lake_root, segments, _utc(13, 0))
    assert answer.num_rows == 0
    assert answer.schema.names == QUOTES_SCHEMA.names


def test_segments_of_gap_rows_alone_are_an_empty_table(lake_root):
    rel = _write(lake_root, "a", [_gap(_utc(13, 32), _utc(13, 32, 5))])
    assert _read(lake_root, _named(lake_root, rel)).num_rows == 0


def test_the_answer_outlives_its_files(lake_root):
    segments = _named(lake_root, *_ordinary(lake_root))
    answer = _read(lake_root, segments)
    for rel in segments:
        path = lake_root / rel
        path.write_bytes(b"\xff" * path.stat().st_size)
    assert answer.column("bid").to_pylist() == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    assert answer.column("ticker").to_pylist() == ["SPY"] * 6


# -- 2. a schema change partway ----------------------------------------------

_BASE = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_end_ts", pa.string()),
        ("ticker", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("mark", pa.float64()),
        ("volatility", pa.float64()),
        ("row_kind", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)


def test_a_moved_an_added_and_a_dropped_column_read_as_the_seal_reads_them(lake_root, monkeypatch):
    # The first segment holds ``ask`` at its front. The second adds ``bid_size``, which the
    # running schema pins in the middle. The third drops ``mark``, which it still pins, and
    # ``volatility``, which it no longer does.
    moved = pa.schema([_BASE.field("ask"), *_without(_BASE, "ask")])
    added = _BASE.insert(4, pa.field("bid_size", pa.int64()))
    dropped = _without(added, "mark", "volatility")
    rels = [
        _minute(lake_root, 31, moved, bid=1.0, ask=1.5, mark=1.2, volatility=0.1),
        _minute(lake_root, 32, added, bid=2.0, ask=2.5, mark=2.2, volatility=0.2),
        _minute(lake_root, 33, dropped, bid=3.0, ask=3.5),
    ]
    _pin(monkeypatch, _without(added, "volatility"))

    answer = _read(lake_root, _named(lake_root, *rels))
    _seal(lake_root)

    names = ["snap_ts", "fetch_end_ts", "ticker", "bid", "bid_size", "ask", "mark", "row_kind"]
    for table in (answer, _sealed_rows(lake_root)):
        assert table.schema.names == [*names, "schema_version", "extra", "volatility"]
        assert table.column("bid").to_pylist() == [1.0, 2.0, 3.0]
        assert table.column("bid_size").to_pylist() == [None, 3, 3]
        assert table.column("ask").to_pylist() == [1.5, 2.5, 3.5]
        assert table.column("mark").to_pylist() == [1.2, 2.2, None]
        assert table.column("volatility").to_pylist() == [0.1, 0.2, None]
    _same(answer, _sealed_rows(lake_root))


def test_the_columns_are_those_the_named_segments_hold(lake_root):
    # ``bid_size`` arrives with the second segment. A subset before it has no such column, and
    # the sealed doors take theirs from the whole day.
    _ledger(lake_root, 2)
    rels = [_minute(lake_root, 31, _without(QUOTES_SCHEMA, "bid_size")), _minute(lake_root, 32)]
    subset = _read(lake_root, _named(lake_root, rels[0]))
    whole = _read(lake_root, _named(lake_root, *rels))
    _seal(lake_root)
    assert "bid_size" not in subset.column_names
    assert whole.column("bid_size").to_pylist() == [None, 3]
    sealed = load_quotes("SPY", DAY, "09:31", lake_root=lake_root)
    assert sealed.column("bid_size").to_pylist() == [None]


def test_a_retyped_column_refuses_here_and_at_the_seal(lake_root):
    rels = [
        _minute(lake_root, 31),
        _minute(lake_root, 32, _retyped(QUOTES_SCHEMA, "bid_size", pa.float64()), bid_size=3.0),
    ]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment is None
    assert isinstance(refused.__cause__, pa.ArrowTypeError)

    result = _seal(lake_root)
    assert [(item.surface, item.ticker, item.day) for item in result.refused] == [
        ("quotes", "SPY", DAY)
    ]
    assert result.sealed == ()


# -- 3. a promoted field -----------------------------------------------------


def _promoted(root: Path, before: int = 1, after: int = 2) -> list[str]:
    """Two minutes holding ``bidSize`` in the overflow, then two holding it as its column."""
    old = _without(QUOTES_SCHEMA, "bid_size")
    return [
        *(
            _minute(root, minute, old, schema_version=before, extra=_routed("bidSize", minute))
            for minute in (31, 32)
        ),
        *(_minute(root, minute, schema_version=after, bid_size=minute) for minute in (33, 34)),
    ]


def test_each_minute_equals_load_quotes_after_the_seal(lake_root):
    _ledger(lake_root, 1, 2, without={1: ("bid_size",)})
    answer = _read(lake_root, _named(lake_root, *_promoted(lake_root)))
    _seal(lake_root)

    assert answer.column("bid_size").to_pylist() == [31, 32, 33, 34]
    for minute in (31, 32, 33, 34):
        mine = answer.filter(pc.equal(answer.column("snap_ts"), _utc(13, minute).isoformat()))
        _same(mine, load_quotes("SPY", DAY, f"09:{minute}", lake_root=lake_root))
    # The raw partition is the one place the two differ.
    assert _sealed_rows(lake_root).column("bid_size").to_pylist() == [None, None, 33, 34]


def test_the_running_version_is_read_when_the_door_is_called(lake_root, monkeypatch):
    monkeypatch.setattr(journal, "SCHEMA_VERSION", 3)
    _ledger(lake_root, 2, without={2: ("bid_size",)})
    answer = _read(lake_root, _named(lake_root, *_promoted(lake_root, before=2, after=3)))
    assert answer.column("bid_size").to_pylist() == [31, 32, 33, 34]


@pytest.mark.parametrize("ledger", ["none", "damaged", "another shape"])
def test_rows_at_the_running_version_need_no_ledger_of_the_roots(lake_root, ledger):
    if ledger == "damaged":
        _ledger(lake_root, 2).write_bytes(b"PAR1 damaged")
    elif ledger == "another shape":
        _ledger(lake_root, 2, without={2: ("bid_size",)})
    rel = _minute(lake_root, 31, bid=7.0)
    assert _read(lake_root, _named(lake_root, rel)).column("bid").to_pylist() == [7.0]


def test_an_older_row_reads_under_a_ledger_holding_its_version_alone(lake_root):
    _ledger(lake_root, 1, without={1: ("bid_size",)})
    answer = _read(lake_root, _named(lake_root, *_promoted(lake_root)))
    assert answer.column("bid_size").to_pylist() == [31, 32, 33, 34]


def _with_an_older_gap(root: Path) -> list[str]:
    return [
        _minute(root, 31, bid=7.0),
        _write(root, "b", [_gap(_utc(13, 32), _utc(13, 32, 1), schema_version=1)]),
    ]


def test_an_older_gap_row_beside_running_rows_reads_under_its_version(lake_root):
    _ledger(lake_root, 1)
    segments = _named(lake_root, *_with_an_older_gap(lake_root))
    assert _read(lake_root, segments).column("bid").to_pylist() == [7.0]


def test_an_older_gap_row_with_no_ledger_is_refused(lake_root):
    rels = _with_an_older_gap(lake_root)
    assert _refusal(lake_root, _named(lake_root, *rels)).segment == rels[1]


def test_an_older_row_with_no_ledger_is_refused(lake_root):
    rels = [_minute(lake_root, 31), _minute(lake_root, 32, schema_version=1)]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment == rels[1]
    assert rels[0] not in str(refused)
    assert "version 1" in str(refused)


def test_the_running_shape_is_never_the_roots(lake_root):
    # Beside an older row, so the root's ledger is opened. It holds version 2 without
    # ``bid_size``, which would lift the value that this code's shape says was routed.
    _ledger(lake_root, 1, 2, without={2: ("bid_size",)})
    rels = [
        _minute(lake_root, 31, schema_version=1),
        _minute(lake_root, 32, bid_size=None, extra=_routed("bidSize", 5)),
    ]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment == rels[1]
    assert "bid_size" in str(refused)


# -- 4. each raise -----------------------------------------------------------

_NAIVE = datetime(2026, 8, 24, 15, 40)
_BAD_TICKERS = ["", "SP/Y", "S\0Y", "SP\ud800Y", None, 5]
_BAD_DAYS = [
    "20260824",
    "2026-8-24",
    "2026-W35-1",
    "2026-02-30",
    "2026-08-24\n",
    datetime(2026, 8, 24, tzinfo=UTC),
    None,
    20260824,
]
_BAD_ATS = [_NAIVE, "2026-08-24T15:40:00-04:00", DAY, None]


@pytest.mark.parametrize("ticker", _BAD_TICKERS)
def test_a_bad_ticker_is_refused(lake_root, ticker):
    with pytest.raises(ValueError) as info:
        load_unsealed_quotes(ticker, DAY, LATE, segments={}, lake_root=lake_root)
    assert type(info.value) is UnsealedArgument
    with pytest.raises(UnsealedArgument) as prefix:
        segment_prefix(ticker, DAY)
    for refused in (info.value, prefix.value):
        assert refused.segment is None
        assert repr(ticker) in str(refused)


@pytest.mark.parametrize("day", _BAD_DAYS)
def test_a_bad_day_is_refused(lake_root, day):
    with pytest.raises(ValueError) as info:
        load_unsealed_quotes("SPY", day, LATE, segments={}, lake_root=lake_root)
    assert type(info.value) is UnsealedArgument
    with pytest.raises(UnsealedArgument) as prefix:
        segment_prefix("SPY", day)
    for refused in (info.value, prefix.value):
        assert refused.segment is None
        assert repr(day) in str(refused)


@pytest.mark.parametrize("at", _BAD_ATS)
def test_a_bad_at_is_refused(lake_root, at):
    with pytest.raises(ValueError) as info:
        _read(lake_root, {}, at)
    assert type(info.value) is UnsealedArgument
    with pytest.raises(ValueError) as cut:
        captured_by(_table([_row(_utc(13, 31), _utc(13, 31, 1))]), at)
    assert type(cut.value) is UnsealedArgument
    for refused in (info.value, cut.value):
        assert refused.segment is None
        assert repr(at) in str(refused)


@pytest.mark.parametrize("segments", [None, [PREFIX + "seg-a-1.arrows"], PREFIX])
def test_segments_that_are_no_mapping_are_refused(lake_root, segments):
    _refusal(lake_root, segments, UnsealedArgument)


@pytest.mark.parametrize("root", [None, 5, b"lake", "la\0ke"])
def test_a_bad_lake_root_is_refused(root):
    with pytest.raises(ValueError) as info:
        load_unsealed_quotes("SPY", DAY, LATE, segments={}, lake_root=root)
    assert type(info.value) is UnsealedArgument
    assert repr(root) in str(info.value)


@pytest.mark.parametrize(
    "rel",
    [
        None,
        PREFIX + "part-a-4242.arrows",
        PREFIX + "seg-a-4242.parquet",
        PREFIX + "seg-.arrows",
        PREFIX + "seg-a_b-4242.arrows",
        PREFIX + "seg-é-4242.arrows",
        PREFIX + "seg-\0a-4242.arrows",
        PREFIX + "seg-a-4242.arrows\n",
        PREFIX + "seg-a-4242.arrows.arrows",
        PREFIX + "sub/seg-a-4242.arrows",
        "/" + PREFIX + "seg-a-4242.arrows",
        "../" + PREFIX + "seg-a-4242.arrows",
        "lake-live/" + PREFIX + "seg-a-4242.arrows",
        PREFIX.replace("SPY", "SPYG") + "seg-a-4242.arrows",
        PREFIX.replace("quotes", "chains") + "seg-a-4242.arrows",
        PREFIX.replace("24", "25") + "seg-a-4242.arrows",
        PARTITION,
    ],
)
def test_a_path_that_is_no_segment_of_the_ticker_day_is_refused(lake_root, rel):
    segments = {**_named(lake_root, _minute(lake_root, 31)), rel: ZEROS}
    refused = _refusal(lake_root, segments, UnsealedArgument)
    assert refused.segment is None
    assert repr(rel) in str(refused)


@pytest.mark.parametrize(
    "spoil",
    [str.upper, lambda text: text[:-1], lambda text: text + "\n", lambda text: 5],
    ids=["upper", "short", "newline", "type"],
)
def test_a_malformed_digest_is_refused(lake_root, spoil: Callable):
    rel = _minute(lake_root, 31)
    digest = spoil("ab" * 32)
    refused = _refusal(lake_root, {rel: digest}, UnsealedArgument)
    assert refused.segment is None
    assert repr(digest) in str(refused)


def test_an_absent_segment_is_refused(lake_root):
    gone = PREFIX + "seg-b-4242.arrows"
    segments = {**_named(lake_root, _minute(lake_root, 31)), gone: ZEROS}
    refused = _refusal(lake_root, segments, SegmentAbsent)
    assert refused.segment == gone
    assert "Download it again" in str(refused)


def test_an_empty_mapping_is_refused(lake_root):
    _minute(lake_root, 31)
    refused = _refusal(lake_root, {}, SegmentAbsent)
    assert refused.segment is None
    assert "no segment is named" in str(refused)


def test_one_byte_changed_that_still_decodes_is_a_mismatch(lake_root):
    rel = _minute(lake_root, 31)
    segments = _named(lake_root, rel)
    path = lake_root / rel
    changed = path.read_bytes().replace(b"Normal", b"Norman")
    path.write_bytes(changed)
    assert journal.read_segment(path).column("security_status").to_pylist() == ["Norman"]

    refused = _refusal(lake_root, segments, SegmentMismatch)
    assert refused.segment == rel
    assert refused.expected == segments[rel]
    assert refused.actual == hashlib.sha256(changed).hexdigest()
    for text in (refused.expected, refused.actual, "Download it again"):
        assert text in str(refused)


@pytest.mark.parametrize(
    "damage",
    [
        pytest.param(lambda data: b"", id="empty"),
        pytest.param(lambda data: b"not an arrow stream", id="garbage"),
        pytest.param(lambda data: b"not an arrow stream" + EOS, id="garbage ending in the marker"),
        pytest.param(lambda data: data[: len(data) // 2], id="cut in half"),
        pytest.param(lambda data: data[: -len(EOS)], id="cut just before the marker"),
        pytest.param(lambda data: data + data, id="doubled"),
        pytest.param(lambda data: data.replace(b"Normal", b"\xfformal"), id="invalid UTF-8"),
    ],
)
def test_a_damaged_segment_is_refused(lake_root, damage: Callable):
    good, rel = _minute(lake_root, 31), _minute(lake_root, 32)
    path = lake_root / rel
    before = path.read_bytes()
    assert before.endswith(EOS)
    assert before.count(b"Normal") == 1
    path.write_bytes(damage(before))
    assert path.read_bytes() != before
    assert _refusal(lake_root, _named(lake_root, good, rel)).segment == rel


def test_a_segment_holding_no_row_is_refused(lake_root):
    good = _minute(lake_root, 31)
    rel = _write(lake_root, "b", QUOTES_SCHEMA.empty_table())
    assert journal.read_segment(lake_root / rel).num_rows == 0
    refused = _refusal(lake_root, _named(lake_root, good, rel))
    assert refused.segment == rel
    assert "no row" in str(refused)


@pytest.mark.parametrize("version", [None, 0, 3])
def test_a_version_this_code_does_not_know_is_refused(lake_root, version):
    _ledger(lake_root, 0, 1, 2, 3)
    good, rel = _minute(lake_root, 31), _minute(lake_root, 32, schema_version=version)
    refused = _refusal(lake_root, _named(lake_root, good, rel), SchemaVersionUnknown)
    assert refused.segment == rel
    assert refused.versions == (version,)
    assert f"version {version}," in str(refused)
    assert "Raise the marketlake pin" in str(refused)


def test_each_unknown_version_is_named_with_none_first(lake_root):
    rows = [
        _row(_utc(13, 31), _utc(13, 31, 1), schema_version=version)
        for version in (9, None, 1, 0, 2, 9)
    ]
    rel = _write(lake_root, "a", rows)
    refused = _refusal(lake_root, _named(lake_root, rel), SchemaVersionUnknown)
    assert refused.versions == (None, 0, 9)


def test_a_gap_rows_version_is_judged_too(lake_root):
    rel = _write(lake_root, "a", [_gap(_utc(13, 31), None, schema_version=99)])
    refused = _refusal(lake_root, _named(lake_root, rel), SchemaVersionUnknown)
    assert refused.versions == (99,)


_STEP_FIVE = ["schema_version", "row_kind", "fetch_end_ts", "snap_ts", "ticker", "extra"]


@pytest.mark.parametrize("name", _STEP_FIVE)
def test_a_column_the_read_stands_on_missing_is_refused(lake_root, name):
    good, rel = _minute(lake_root, 31), _minute(lake_root, 32, _without(QUOTES_SCHEMA, name))
    refused = _refusal(lake_root, _named(lake_root, good, rel))
    assert refused.segment == rel
    assert name in str(refused)


@pytest.mark.parametrize("name", _STEP_FIVE)
def test_a_column_the_read_stands_on_retyped_is_refused(lake_root, name):
    kind = pa.int32() if name == "schema_version" else pa.large_string()
    rel = _minute(lake_root, 31, _retyped(QUOTES_SCHEMA, name, kind))
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert refused.segment == rel
    assert name in str(refused)
    assert str(kind) in str(refused)


@pytest.mark.parametrize("ticker", ["QQQ", "spy", None])
def test_another_tickers_rows_under_this_path_are_refused(lake_root, ticker):
    good, rel = _minute(lake_root, 31), _minute(lake_root, 32, ticker=ticker)
    refused = _refusal(lake_root, _named(lake_root, good, rel))
    assert refused.segment == rel
    assert repr(ticker) in str(refused)


@pytest.mark.parametrize("build", [_row, _gap])
def test_one_row_of_another_ticker_is_refused(lake_root, build):
    rows = [
        _row(_utc(13, 31), _utc(13, 31, 1)),
        build(_utc(13, 31), _utc(13, 31, 1), ticker="QQQ"),
    ]
    rel = _write(lake_root, "a", rows)
    assert _refusal(lake_root, _named(lake_root, rel)).segment == rel


def test_one_row_of_another_day_is_refused(lake_root):
    yesterday = datetime(2026, 8, 23, 19, 39, tzinfo=UTC)
    rel = _write(lake_root, "a", [_row(_utc(13, 31), _utc(13, 31, 1)), _row(yesterday, yesterday)])
    assert _refusal(lake_root, _named(lake_root, rel)).segment == rel


@pytest.mark.parametrize(
    "snap",
    [
        datetime(2026, 8, 23, 19, 39, tzinfo=UTC),  # yesterday's 15:39
        datetime(2026, 8, 25, 13, 32, tzinfo=UTC),
        datetime(2026, 8, 23, 23, 59, 59, tzinfo=UTC),
        datetime(2026, 8, 25, 4, 0, tzinfo=UTC),
    ],
)
def test_another_days_rows_under_this_path_are_refused(lake_root, snap):
    good = _minute(lake_root, 31)
    rel = _write(lake_root, "b", [_row(snap, snap)])
    refused = _refusal(lake_root, _named(lake_root, good, rel))
    assert refused.segment == rel
    assert snap.isoformat() in str(refused)


@pytest.mark.parametrize(
    "snap",
    [
        datetime(2026, 8, 24, 1, 0, tzinfo=UTC),  # 2026-08-23 in New York
        datetime(2026, 8, 25, 1, 0, tzinfo=UTC),  # 2026-08-24 in New York
    ],
)
def test_a_snap_on_the_day_by_one_of_its_two_dates_reads(lake_root, snap):
    rel = _write(lake_root, "a", [_row(snap, snap, bid=7.0)])
    answer = _read(lake_root, _named(lake_root, rel), datetime(2026, 8, 25, 4, tzinfo=UTC))
    assert answer.column("bid").to_pylist() == [7.0]


@pytest.mark.parametrize(
    "text",
    [
        None,
        "2026-08-24T13:32:00",
        "noon",
        "0001-01-01T00:00:00+00:00",
        # An instant before the first one a datetime holds, so converting it raises.
        "0001-01-01T00:00:00+14:00",
    ],
)
def test_a_snap_that_is_no_instant_on_the_day_is_refused(lake_root, text):
    good, rel = _minute(lake_root, 31), _minute(lake_root, 32, snap_ts=text)
    assert _refusal(lake_root, _named(lake_root, good, rel)).segment == rel


@pytest.mark.parametrize("name", ["bid", "not_pinned"])
def test_a_column_held_twice_is_refused(lake_root, name):
    table = _table([_row(_utc(13, 31), _utc(13, 31, 1))])
    for _ in range(table.schema.names.count(name), 2):
        table = table.append_column(name, pa.array([1.0]))
    rel = _write(lake_root, "a", table)
    assert _refusal(lake_root, _named(lake_root, rel)).segment is None


def test_a_column_name_that_is_not_utf8_is_refused(lake_root):
    rel = _minute(lake_root, 31)
    path = lake_root / rel
    path.write_bytes(path.read_bytes().replace(b"cusip", b"cusi\xff"))
    _refusal(lake_root, _named(lake_root, rel))


@pytest.mark.parametrize(
    ("over", "says"),
    [
        ({"extra": "{not json"}, "extra"),
        ({"extra": "[" * 200_000}, "RecursionError"),
        ({"row_kind": None}, "row_kind"),
        ({"fetch_end_ts": None}, "fetch_end_ts"),
        ({"fetch_end_ts": "2026-08-24T13:31:01"}, "fetch_end_ts"),
    ],
)
def test_a_row_the_read_cannot_place_is_refused(lake_root, over, says):
    rel = _minute(lake_root, 31, **over)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert refused.segment is None
    assert says in str(refused)


def test_a_gap_row_needs_no_capture_instant(lake_root):
    rels = [
        _minute(lake_root, 31, bid=7.0),
        _write(lake_root, "b", [_gap(_utc(13, 32), None)]),
    ]
    assert _read(lake_root, _named(lake_root, *rels)).column("bid").to_pylist() == [7.0]


def test_a_routed_retype_names_its_segment_among_three(lake_root):
    rels = [
        _minute(lake_root, 31),
        _minute(lake_root, 32, last=None, extra=_routed()),
        _minute(lake_root, 33),
    ]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment == rels[1]
    assert [rel in str(refused) for rel in rels] == [False, True, False]
    assert "last" in str(refused)


def test_two_routed_retypes_name_both_segments_and_the_first(lake_root):
    rels = [
        _minute(lake_root, 31),
        _minute(lake_root, 32, last=None, extra=_routed()),
        _minute(lake_root, 33, last=None, extra=_routed()),
    ]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment == rels[1]
    assert [rel in str(refused) for rel in rels] == [False, True, True]


def test_rows_partial_only_once_joined_name_no_segment(lake_root):
    # Version 1 lacked ``bid_size``, so a later value of its overflow lifts into the type the
    # column has beside it. Alone that segment adds the column at the pinned int64, and joined
    # with one holding int32 the value is out of range.
    _ledger(lake_root, 1, without={1: ("bid_size",)})
    narrow = _retyped(QUOTES_SCHEMA, "bid_size", pa.int32())
    rels = [
        _minute(
            lake_root,
            31,
            _without(QUOTES_SCHEMA, "bid_size"),
            schema_version=1,
            extra=_routed("bidSize", 2**40),
        ),
        _minute(lake_root, 32, narrow),
    ]
    refused = _refusal(lake_root, _named(lake_root, *rels))
    assert refused.segment is None
    assert "only in the joined rows" in str(refused)


def test_a_ledger_cut_short_is_refused(lake_root):
    path = _ledger(lake_root, 1, 2)
    path.write_bytes(path.read_bytes()[:-20])
    rel = _minute(lake_root, 31, schema_version=1)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert refused.segment is None
    assert "reference/schema_versions.parquet" in str(refused)


def test_a_ledger_flip_pyarrow_reports_as_oserror_is_refused(lake_root):
    path = _ledger(lake_root, 1, 2)
    whole = path.read_bytes()
    for offset in range(len(whole)):
        flipped = bytearray(whole)
        flipped[offset] ^= 0xFF
        path.write_bytes(bytes(flipped))
        try:
            SchemaVersionLedger.read(path)
        except OSError:
            break
        except Exception:  # a flip of another kind
            continue
    else:
        pytest.fail("no flip reads as an OSError")
    rel = _minute(lake_root, 31, schema_version=1)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert isinstance(refused.__cause__, OSError)
    assert "reference/schema_versions.parquet" in str(refused)


def test_a_quarantine_ledger_damaged_before_a_whole_line_is_refused(lake_root):
    whole = json.dumps({"partition": "x", "check": "y", "verdict": "clean"})
    (lake_root / "quarantine.jsonl").write_text(f"{whole[:10]}\n{whole}\n")
    rel = _minute(lake_root, 31)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert refused.segment is None
    assert "quarantine.jsonl cannot be read" in str(refused)


def test_a_quarantine_ledger_that_cannot_be_opened_is_refused(lake_root):
    (lake_root / "quarantine.jsonl").mkdir()
    rel = _minute(lake_root, 31)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert isinstance(refused.__cause__, OSError)


def test_a_torn_last_line_of_the_quarantine_ledger_reads(lake_root):
    whole = json.dumps({"partition": "x", "check": "y", "verdict": "clean"})
    (lake_root / "quarantine.jsonl").write_text(f"{whole}\n{whole[:10]}")
    rel = _minute(lake_root, 31, bid=7.0)
    assert _read(lake_root, _named(lake_root, rel)).column("bid").to_pylist() == [7.0]


def test_a_withheld_partition_is_refused(lake_root):
    _withhold(lake_root)
    rel = _minute(lake_root, 31)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert refused.segment is None
    assert PARTITION in str(refused)
    assert "spread" in str(refused)


def test_a_clean_verdict_of_another_check_buries_no_withholding_one(lake_root):
    _withhold(lake_root)
    _withhold(lake_root, verdict="clean", check="volume")
    rel = _minute(lake_root, 31)
    refused = _refusal(lake_root, _named(lake_root, rel))
    assert "spread" in str(refused)
    assert "volume" not in str(refused)


@pytest.mark.parametrize(
    ("partition", "verdict"),
    [
        (PARTITION.replace("quotes", "chains"), "quarantined"),
        (PARTITION.replace("SPY", "QQQ"), "quarantined"),
        (PARTITION, "clean"),
    ],
)
def test_a_verdict_that_withholds_nothing_here_reads(lake_root, partition, verdict):
    _withhold(lake_root)
    _withhold(lake_root, verdict="clean")
    _withhold(lake_root, partition, verdict)
    rel = _minute(lake_root, 31, bid=7.0)
    assert _read(lake_root, _named(lake_root, rel)).column("bid").to_pylist() == [7.0]


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file nobody may read")
def test_a_segment_that_may_not_be_read_escapes_as_itself(lake_root):
    rel = _minute(lake_root, 31)
    segments = _named(lake_root, rel)
    (lake_root / rel).chmod(0)
    try:
        with pytest.raises(PermissionError):
            _read(lake_root, segments)
    finally:
        (lake_root / rel).chmod(0o644)


def test_a_directory_at_a_segments_path_escapes_as_itself(lake_root):
    rel = PREFIX + "seg-a-4242.arrows"
    (lake_root / rel).mkdir(parents=True)
    with pytest.raises(IsADirectoryError):
        _read(lake_root, {rel: ZEROS})


@pytest.mark.parametrize("step", ["latest_quarantine_by_check", "merge_by_name", "project_extra"])
@pytest.mark.parametrize("escapes", [MemoryError, KeyboardInterrupt])
def test_what_is_not_about_the_data_escapes_as_itself(lake_root, monkeypatch, step, escapes):
    rel = _minute(lake_root, 31)
    segments = _named(lake_root, rel)

    def stop(*args: object, **kwargs: object) -> None:
        raise escapes

    monkeypatch.setattr(unsealed, step, stop)
    with pytest.raises(escapes):
        _read(lake_root, segments)


# -- 5. which fault wins -----------------------------------------------------


def test_the_arguments_come_before_the_guard(lake_root):
    _withhold(lake_root)
    _refusal(lake_root, {PARTITION: ZEROS}, UnsealedArgument)


def test_an_empty_mapping_on_a_withheld_day_is_absent(lake_root):
    _withhold(lake_root)
    _refusal(lake_root, {}, SegmentAbsent)


def test_the_guard_comes_before_the_bytes(lake_root):
    _withhold(lake_root)
    refused = _refusal(lake_root, {PREFIX + "seg-b-4242.arrows": ZEROS})
    assert "withholds" in str(refused)


def test_a_stale_digest_on_doubled_bytes_is_a_mismatch(lake_root):
    rel = _minute(lake_root, 31)
    segments = _named(lake_root, rel)
    path = lake_root / rel
    path.write_bytes(path.read_bytes() * 2)
    _refusal(lake_root, segments, SegmentMismatch)


def test_a_wrong_digest_on_an_unknown_version_is_a_mismatch(lake_root):
    rel = _minute(lake_root, 31, schema_version=99)
    _refusal(lake_root, {rel: ZEROS}, SegmentMismatch)


def test_one_damaged_and_one_absent_segment_is_absent(lake_root):
    rel = _minute(lake_root, 31)
    path = lake_root / rel
    path.write_bytes(path.read_bytes()[: -len(EOS)])
    gone = PREFIX + "seg-20260824T133200-4242.arrows"
    segments = {**_named(lake_root, rel), gone: ZEROS}
    assert _refusal(lake_root, segments, SegmentAbsent).segment == gone


def test_the_first_fault_in_path_order_is_the_one_raised(lake_root):
    rels = [_minute(lake_root, minute) for minute in (31, 32)]
    segments = {rel: ZEROS for rel in reversed(rels)}
    assert _refusal(lake_root, segments, SegmentMismatch).segment == rels[0]


def test_an_unknown_version_beside_a_retyped_column_is_named(lake_root):
    rels = [
        _minute(lake_root, 31),
        _minute(
            lake_root,
            32,
            _retyped(QUOTES_SCHEMA, "bid_size", pa.float64()),
            bid_size=3.0,
            schema_version=99,
        ),
    ]
    _refusal(lake_root, _named(lake_root, *rels), SchemaVersionUnknown)


@pytest.mark.parametrize("fault", ["ticker", "column", "day"])
def test_an_unknown_version_in_a_later_segment_comes_first(lake_root, fault):
    if fault == "ticker":
        first = _minute(lake_root, 31, ticker="QQQ")
    elif fault == "column":
        first = _minute(lake_root, 31, _retyped(QUOTES_SCHEMA, "fetch_end_ts", pa.large_string()))
    else:
        first = _minute(lake_root, 31, snap_ts="2026-08-23T19:39:00+00:00")
    later = _minute(lake_root, 32, schema_version=99)
    refused = _refusal(lake_root, _named(lake_root, first, later), SchemaVersionUnknown)
    assert refused.segment == later


def test_another_ticker_in_a_later_segment_comes_before_another_day(lake_root):
    first = _minute(lake_root, 31, snap_ts="2026-08-23T19:39:00+00:00")
    later = _minute(lake_root, 32, ticker="QQQ")
    assert _refusal(lake_root, _named(lake_root, first, later)).segment == later


# -- 6. judged over every named segment --------------------------------------


def test_an_unknown_version_after_at_refuses(lake_root):
    rels = [_minute(lake_root, 31), _minute(lake_root, 50, schema_version=3)]
    segments = _named(lake_root, *rels)
    refused = _refusal(lake_root, segments, SchemaVersionUnknown, _utc(13, 40))
    assert refused.segment == rels[1]


def test_a_retyped_column_after_at_refuses(lake_root):
    rels = [
        _minute(lake_root, 31),
        _minute(lake_root, 50, _retyped(QUOTES_SCHEMA, "bid_size", pa.float64()), bid_size=3.0),
    ]
    _refusal(lake_root, _named(lake_root, *rels), at=_utc(13, 40))


def test_a_routed_value_after_at_refuses(lake_root):
    rels = [_minute(lake_root, 31), _minute(lake_root, 50, last=None, extra=_routed())]
    refused = _refusal(lake_root, _named(lake_root, *rels), at=_utc(13, 40))
    assert refused.segment == rels[1]


# -- 7. only what is named ---------------------------------------------------


def test_a_segment_nobody_named_changes_nothing(lake_root):
    rel = _minute(lake_root, 31, bid=1.0)
    segments = _named(lake_root, rel)
    _minute(lake_root, 30, bid=99.0)
    (lake_root / _minute(lake_root, 32)).write_bytes(b"damaged")
    assert _read(lake_root, segments).column("bid").to_pylist() == [1.0]


def test_a_file_swapped_after_its_bytes_were_read_changes_nothing(lake_root, monkeypatch):
    rel = _minute(lake_root, 31, bid=1.0)
    segments = _named(lake_root, rel)
    path = lake_root / rel
    swapped = (lake_root / _minute(lake_root, 32, bid=99.0)).read_bytes()
    real = Path.read_bytes
    reads: list[Path] = []

    def read_then_swap(self: Path) -> bytes:
        data = real(self)
        if self == path:
            reads.append(self)
            path.write_bytes(swapped)
        return data

    monkeypatch.setattr(Path, "read_bytes", read_then_swap)
    assert _read(lake_root, segments).column("bid").to_pylist() == [1.0]
    assert reads == [path]


# -- 8. reads only its root --------------------------------------------------


def test_it_reads_only_its_root(lake_root, monkeypatch):
    rel = _minute(lake_root, 31, bid=1.0)
    segments = _named(lake_root, rel)

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("the door reached past its root")

    monkeypatch.setattr(config, "load_config", refuse)
    monkeypatch.setattr(loader, "load_config", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    assert _read(lake_root, segments).column("bid").to_pylist() == [1.0]
    assert [path.name for path in lake_root.iterdir()] == ["journal"]


# -- 9. captured_by, and the replay it is public for -------------------------


def _newest(table: pa.Table) -> dict:
    return max(table.to_pylist(), key=lambda row: datetime.fromisoformat(row["fetch_end_ts"]))


def _replay(root: Path, at: datetime) -> dict:
    """The row the published replay reaches: back a minute until one leaves a captured row."""
    minute = at.astimezone(MARKET_TZ).replace(second=0, microsecond=0)
    while (minute.hour, minute.minute) >= (9, 30):
        try:
            table = captured_by(load_quotes("SPY", DAY, f"{minute:%H:%M}", lake_root=root), at)
        except SnapAbsent:
            table = None
        if table is not None and table.num_rows:
            return _newest(table)
        minute -= timedelta(minutes=1)
    raise AssertionError("no minute held a captured row")


@pytest.mark.parametrize(
    ("at", "bid"),
    [
        # 13:40's row landed at 13:40:01, and 13:39 holds a gap row, so 13:38's is the newest.
        (_utc(13, 40), 38.0),
        (_utc(13, 40, 3), 40.0),
        (_utc(13, 39, 30), 38.0),
    ],
)
def test_the_newest_row_is_the_one_a_replay_reaches(lake_root, at, bid):
    _ledger(lake_root, 2)
    rels = [_minute(lake_root, minute, bid=float(minute)) for minute in (37, 38, 40, 41)]
    rels.append(_write(lake_root, "20260824T133900", [_gap(_utc(13, 39), _utc(13, 39, 1))]))
    newest = _newest(_read(lake_root, _named(lake_root, *rels), at))
    _seal(lake_root)
    assert newest["bid"] == bid
    assert newest == _replay(lake_root, at)


def test_a_replay_sees_a_row_that_reached_the_bucket_after_the_decision(lake_root):
    # 13:39's row was captured by ``at``, and its segment was not yet listed. The sealed day
    # cannot say so, which is why a decision's mapping is what reproduces it.
    _ledger(lake_root, 2)
    rels = [_minute(lake_root, minute, bid=float(minute)) for minute in (38, 39)]
    at = _utc(13, 39, 5)
    decided = _newest(_read(lake_root, _named(lake_root, rels[0]), at))
    _seal(lake_root)
    assert decided["bid"] == 38.0
    assert _replay(lake_root, at)["bid"] == 39.0


@pytest.mark.parametrize("name", ["row_kind", "fetch_end_ts"])
def test_captured_by_refuses_a_missing_column(name):
    table = _table([_row(_utc(13, 31), _utc(13, 31, 1))], _without(QUOTES_SCHEMA, name))
    with pytest.raises(UnsealedRefused) as info:
        captured_by(table, LATE)
    assert type(info.value) is UnsealedRefused
    assert info.value.segment is None
    assert name in str(info.value)


def test_captured_by_refuses_what_is_no_table():
    with pytest.raises(UnsealedArgument):
        captured_by([], LATE)


# -- 10. the published names -------------------------------------------------

_PUBLISHED = {
    "snap_ts",
    "fetch_ts",
    "fetch_end_ts",
    "vendor_quote_ts",
    "bid_time",
    "ask_time",
    "trade_time",
    "ticker",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "last_size",
    "total_volume",
    "security_status",
    "realtime",
    "close_tag",
    "session_phase",
    "row_kind",
    "schema_version",
}


def test_the_readmes_column_table_is_the_published_columns_at_their_pinned_types():
    text = (REPO / "README.md").read_text()
    start = text.index("\n## Read an unsealed day\n")
    section = text[start : text.index("\n## ", start + 1)]
    table = section[section.index("**Table:** The published columns") :]
    rows = re.findall(r"^ *\| `(\w+)` \| `(\w+)` \| (yes|no) \|", table, re.M)
    assert len(rows) == len(_PUBLISHED)
    types = {name: kind for name, kind, _ in rows}
    assert types == {name: str(QUOTES_SCHEMA.field(name).type) for name in _PUBLISHED}
    never_null = {name for name, _, may_be_null in rows if may_be_null == "no"}
    assert never_null == {"ticker", "snap_ts", "fetch_end_ts", "row_kind", "schema_version"}


@pytest.mark.parametrize("day", [DAY, DAY_TEXT])
def test_the_prefix_is_the_directory_a_segment_lands_in(day):
    assert segment_prefix("SPY", day) == PREFIX
    built = journal.segment_path(Path("/lake"), "quotes", "SPY", DAY, "a", PID)
    assert segment_prefix("SPY", day) == f"{built.parent.relative_to('/lake').as_posix()}/"


# -- 12. imports -------------------------------------------------------------


def _loaded(module: str) -> set[str]:
    """The ``lake`` modules a fresh interpreter holds after importing ``module``."""
    script = (
        f"import sys, {module}\n"
        "print(' '.join(name for name in sys.modules if name.split('.')[0] == 'lake'))"
    )
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    return set(done.stdout.split())


@pytest.mark.parametrize("module", ["lake.daemon", "lake.capture"])
def test_capture_loads_neither_new_module(module):
    loaded = _loaded(module)
    assert module in loaded
    assert not loaded & {"lake.segment_join", "lake.unsealed"}


def test_the_join_loads_no_other_lake_module():
    assert _loaded("lake.segment_join") == {"lake", "lake.segment_join"}


def test_the_door_loads_both_new_modules():
    assert {"lake.segment_join", "lake.unsealed"} <= _loaded("lake.unsealed")
