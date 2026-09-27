"""The three reference readers refuse every damaged file with their own class.

Marketlake #551 and #396. ``SecurityMaster.read``, ``CaptureSpans.read`` and
``SchemaVersionLedger.read`` used to fold ``ArrowInvalid`` alone. A directory at the spans path
read as an empty spans set, so the daemon captured nothing and said nothing. A parquet in some
other schema raised a bare ``KeyError`` out of the capture cycle, which ends the daemon. The
rule now is that every damaged file raises the module's unreadable class, a file from newer
code raises its unsupported class, and ``OSError`` comes through unfolded. That last is an
absent or refused file, or corruption pyarrow itself reports as ``OSError``.

Every test here runs against all three readers rather than against the shared helper alone. A
test on the helper would still pass if one reader stopped calling it.

Every fixture is built from the literal schemas below, never from ``MASTER_SCHEMA``,
``SPANS_SCHEMA`` or ``LEDGER_SCHEMA``. A fixture built from the constant under test moves with
it, so a mutation to the constant could not fail the test.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import reference_read
from lake.capture import _live_roster
from lake.capture_spans import (
    CaptureSpans,
    CaptureSpansError,
    SpansUnreadable,
    UnsupportedSpansSchemaVersion,
    spans_path,
)
from lake.schema_versions import (
    LedgerUnreadable,
    SchemaVersionLedger,
    SchemaVersionsError,
    UnsupportedLedgerSchemaVersion,
    ledger_path,
)
from lake.security_master import (
    MasterUnreadable,
    SecurityMaster,
    SecurityMasterError,
    UnsupportedSchemaVersion,
    master_path,
)
from lake.tickers import Roster, TickerConfig

STAMP = datetime(2026, 9, 8, 17, 7, tzinfo=UTC)
UTC_US = pa.timestamp("us", tz="UTC")

# The three files' shapes, spelled out here rather than imported, per the module docstring.
MASTER_LITERAL = {
    "instrument_id": pa.array([1], pa.int64()),
    "id_type": pa.array(["ticker"], pa.string()),
    "id_value": pa.array(["SPY"], pa.string()),
    "valid_from": pa.array([date(2026, 9, 8)], pa.date32()),
    "valid_to": pa.array([None], pa.date32()),
    "kind": pa.array(["equity"], pa.string()),
    "capture_start": pa.array([STAMP], UTC_US),
    "schema_version": pa.array([1], pa.int32()),
}
SPANS_LITERAL = {
    "instrument_id": pa.array([1], pa.int64()),
    "span_start": pa.array([STAMP], UTC_US),
    "span_end": pa.array([None], UTC_US),
    "options": pa.array([False], pa.bool_()),
    "schema_version": pa.array([1], pa.int32()),
}
LEDGER_LITERAL = {
    "journal_schema_version": pa.array([1], pa.int32()),
    "surface": pa.array(["chains"], pa.string()),
    "column_name": pa.array(["bid"], pa.string()),
    "column_type": pa.array(["double"], pa.string()),
    "recorded_at": pa.array([STAMP], UTC_US),
    "schema_version": pa.array([1], pa.int32()),
}


@dataclass(frozen=True)
class Reader:
    """One reference reader, with the classes it answers in and the file it reads."""

    name: str
    read: Callable[[Path], object]
    unreadable: type[Exception]
    unsupported: type[Exception]
    base: type[Exception]
    path: Callable[[Path], Path]
    columns: dict[str, pa.Array]
    # A column other than ``schema_version``, to drop or retype to a string.
    other: str
    # How many entries a table read back holds, which is one for every literal here.
    size: Callable[[object], int]


READERS = [
    Reader(
        "master",
        SecurityMaster.read,
        MasterUnreadable,
        UnsupportedSchemaVersion,
        SecurityMasterError,
        master_path,
        MASTER_LITERAL,
        "instrument_id",
        lambda master: len(list(master)),
    ),
    Reader(
        "spans",
        CaptureSpans.read,
        SpansUnreadable,
        UnsupportedSpansSchemaVersion,
        CaptureSpansError,
        spans_path,
        SPANS_LITERAL,
        "instrument_id",
        lambda spans: len(spans),
    ),
    Reader(
        "ledger",
        SchemaVersionLedger.read,
        LedgerUnreadable,
        UnsupportedLedgerSchemaVersion,
        SchemaVersionsError,
        ledger_path,
        LEDGER_LITERAL,
        "journal_schema_version",
        lambda ledger: len(ledger.versions()),
    ),
]


@pytest.fixture(params=READERS, ids=[r.name for r in READERS])
def reader(request) -> Reader:
    return request.param


@pytest.fixture
def target(reader: Reader, tmp_path: Path) -> Path:
    path = reader.path(tmp_path / "lake")
    path.parent.mkdir(parents=True)
    return path


def _table(columns: dict[str, pa.Array], **changes: pa.Array | None) -> pa.Table:
    """The literal table with some columns replaced, or dropped where the change is None."""
    merged = {**columns, **changes}
    return pa.table({name: array for name, array in merged.items() if array is not None})


def _write(table: pa.Table, path: Path) -> Path:
    pq.write_table(table, path)
    return path


# -- the control -----------------------------------------------------------------------------


def test_the_literal_file_reads(reader, target):
    """The control. Every refusal below is the shape talking, not a broken literal."""
    _write(_table(reader.columns), target)

    assert reader.size(reader.read(target)) == 1


def test_a_file_with_one_extra_column_reads(reader, target):
    """Extra columns are allowed, so dropping a pinned column later needs no version bump.

    Marketlake #96 drops ``capture_start`` from the master's pinned schema. Every live master
    still carries it, and an exact schema match would refuse each one until something
    rewrote it.
    """
    _write(_table(reader.columns, extra=pa.array(["x"], pa.string())), target)

    assert reader.size(reader.read(target)) == 1


# -- what exists and is not a regular file ---------------------------------------------------


def test_a_directory_is_unreadable_and_says_so(reader, target):
    target.mkdir()

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert caught.value.path == target
    assert caught.value.reason == "is a directory"
    assert str(caught.value).endswith(f"at {target} is a directory")


def test_a_directory_holding_only_ds_store_is_unreadable(reader, target):
    target.mkdir()
    (target / ".DS_Store").write_bytes(b"\x00\x00\x00\x01Bud1")

    with pytest.raises(reader.unreadable, match="is a directory"):
        reader.read(target)


def test_a_directory_holding_a_valid_part_is_unreadable(reader, target):
    """pyarrow reads this one as a whole dataset, so only the stat can refuse it."""
    target.mkdir()
    _write(_table(reader.columns), target / "part-0.parquet")
    assert pq.read_table(target).num_rows == 1, "the fixture must be readable as a dataset"

    with pytest.raises(reader.unreadable, match="is a directory"):
        reader.read(target)


def test_a_fifo_is_unreadable_rather_than_absent(reader, target):
    """pyarrow reports a FIFO as ``FileNotFoundError``, the quiet absent answer.

    The stat decides it before pyarrow is called, so this does not rest on pyarrow's FIFO
    behaviour on whichever platform runs it. Opening with Python's ``open`` would block.
    """
    os.mkfifo(target)

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert caught.value.reason == "is not a regular file"


# -- a regular file that is not this table ---------------------------------------------------


@pytest.mark.parametrize("rows", [1, 0], ids=["with rows", "with none"])
def test_a_foreign_parquet_is_unreadable(reader, target, rows):
    """It used to raise a bare ``KeyError`` with rows, and read as empty with none."""
    _write(pa.table({"nonsense": pa.array(["x"] * rows, pa.string())}), target)

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert "schema_version" in caught.value.reason


def test_a_string_schema_version_is_unreadable_rather_than_newer(reader, target):
    """A foreign ``schema_version`` is not a table from newer code, whatever it holds."""
    _write(_table(reader.columns, schema_version=pa.array(["1"], pa.string())), target)

    with pytest.raises(reader.unreadable, match="schema_version"):
        reader.read(target)


def test_a_retyped_column_is_unreadable_and_names_it(reader, target):
    column = reader.columns[reader.other]
    _write(_table(reader.columns, **{reader.other: column.cast(pa.string())}), target)

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert f"its {reader.other} column is string" in caught.value.reason


def test_a_missing_column_is_unreadable_and_names_it(reader, target):
    _write(_table(reader.columns, **{reader.other: None}), target)

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert f"no {reader.other} column" in caught.value.reason


@pytest.mark.parametrize("keep", [0, 0.5], ids=["zero bytes", "torn"])
def test_a_torn_file_is_unreadable(reader, target, keep):
    whole = _write(_table(reader.columns), target.with_name("whole.parquet")).read_bytes()
    target.write_bytes(whole[: int(len(whole) * keep)])

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert caught.value.reason.startswith("is not readable parquet, ")


# -- a file from newer code ------------------------------------------------------------------


def test_a_null_version_is_unreadable_rather_than_newer(reader, target):
    """Every pinned field is nullable, and a null version is damage, not newer code."""
    _write(_table(reader.columns, schema_version=pa.array([None], pa.int32())), target)

    with pytest.raises(reader.unreadable, match="null schema_version"):
        reader.read(target)


def test_a_reason_the_file_supplies_stays_on_one_line(reader, target):
    """A column type can carry a field name the file chose, and it must not forge a line."""
    forged = pa.array(
        [{"a\nreference: forged": 1}], pa.struct([("a\nreference: forged", pa.int64())])
    )
    _write(_table(reader.columns, **{reader.other: forged}), target)

    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert "reference: forged" in str(caught.value)
    assert "\n" not in str(caught.value)


def test_a_newer_version_is_unsupported(reader, target):
    _write(_table(reader.columns, schema_version=pa.array([2], pa.int32())), target)

    with pytest.raises(reader.unsupported) as caught:
        reader.read(target)
    assert caught.value.found == 2


def test_a_newer_version_that_dropped_a_column_is_still_unsupported(reader, target):
    """The version check runs first, because a newer version may change the columns.

    Checking the columns first would call a file from newer code damaged, and send the
    operator to a restore rather than to an upgrade.
    """
    changes = {"schema_version": pa.array([2], pa.int32()), reader.other: None}
    _write(_table(reader.columns, **changes), target)

    with pytest.raises(reader.unsupported):
        reader.read(target)


# -- access failures stay OSError ------------------------------------------------------------


def test_an_absent_file_is_file_not_found(reader, target):
    with pytest.raises(FileNotFoundError):
        reader.read(target)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 file")
def test_a_refused_open_is_a_permission_error(reader, target):
    _write(_table(reader.columns), target)
    os.chmod(target, 0o000)
    try:
        with pytest.raises(PermissionError):
            reader.read(target)
    finally:
        os.chmod(target, 0o644)


def test_an_os_error_from_the_read_is_not_folded(reader, target, monkeypatch):
    """Most corruption reaches pyarrow's callers as a bare ``OSError``, and it stays one.

    262 of 400 byte flips in a real ledger raised ``OSError: Corrupt snappy compressed
    data``, and the ledger check keeps that apart from a ``PermissionError`` on purpose
    (marketlake #536). Folding ``OSError`` would erase the split.
    """
    _write(_table(reader.columns), target)
    raised = OSError("Corrupt snappy compressed data")

    def corrupt(path):
        raise raised

    monkeypatch.setattr(pq, "read_table", corrupt)
    with pytest.raises(OSError) as caught:
        reader.read(target)
    assert caught.value is raised


# -- the fold is broad -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        pa.ArrowNotImplementedError("Unsupported encoding"),
        OverflowError("date value out of range"),
        KeyError("schema_version"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_anything_else_the_read_raises_is_unreadable(reader, target, monkeypatch, error):
    """A bit flip raised each of these, and none is the ``ValueError`` the old fold caught."""
    _write(_table(reader.columns), target)

    def flipped(path):
        raise error

    monkeypatch.setattr(pq, "read_table", flipped)
    with pytest.raises(reader.unreadable) as caught:
        reader.read(target)
    assert caught.value.__cause__ is error
    assert type(error).__name__ in caught.value.reason


# -- the daemon ------------------------------------------------------------------------------

AT = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
ROSTER = Roster(tickers=(TickerConfig("SPY"), TickerConfig("QQQ")))


@pytest.fixture
def clamping_lake(tmp_path: Path) -> Iterator[Path]:
    """A lake whose spans keep SPY and drop QQQ, so a widened roster is visible."""
    reference_read.reset()
    root = tmp_path / "lake"
    root.mkdir()
    master = SecurityMaster()
    spy = master.register(kind="equity", capture_start=STAMP, valid_from=STAMP.date(), ticker="SPY")
    qqq = master.register(kind="equity", capture_start=STAMP, valid_from=STAMP.date(), ticker="QQQ")
    master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(spy, STAMP, False)
    spans.open_span(qqq, STAMP, False)
    spans.close_span(qqq, datetime(2026, 9, 15, 20, 0, tzinfo=UTC))
    spans.write(spans_path(root))
    yield root
    reference_read.reset()


def _captured(roster: Roster) -> list[str]:
    return [entry.ticker for entry in roster.enabled]


def _lines(capsys) -> list[str]:
    return [line for line in capsys.readouterr().err.splitlines() if line]


def test_a_directory_at_the_spans_path_widens_the_roster_and_says_why(clamping_lake, capsys):
    """Marketlake #551. It used to empty the roster, which fed the dead-man and paged nothing."""
    assert _captured(_live_roster(ROSTER, clamping_lake, AT)) == ["SPY"]
    path = spans_path(clamping_lake)
    path.unlink()
    path.mkdir()

    assert _captured(_live_roster(ROSTER, clamping_lake, AT)) == ["SPY", "QQQ"]
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {path} could not be read at {AT.isoformat()}")
    assert line.endswith(f"SpansUnreadable: capture spans at {path} is a directory")
    assert "not readable parquet" not in line


def test_a_foreign_master_widens_the_roster_rather_than_ending_the_cycle(clamping_lake, capsys):
    """Marketlake #396. A ``KeyError`` out of this call used to end the daemon every relaunch."""
    path = master_path(clamping_lake)
    _write(pa.table({"nonsense": pa.array(["x"], pa.string())}), path)

    assert _captured(_live_roster(ROSTER, clamping_lake, AT)) == ["SPY", "QQQ"]
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {path} could not be read at ")
    assert "MasterUnreadable" in line
