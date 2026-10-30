"""Every bar's origin and price basis in ``load_bars``, and the adjusted views' refusal of imports.

Marketlake #949. A consumer must tell captured history from imported history, and an imported
row's numbers may already be adjusted, so each answer carries ``origin``, ``basis``, ``source``
and ``source_as_of``. The values come from each partition's Arrow schema metadata. The cases
follow the issue's Acceptance list.

1. A captured partition reads as captured and raw, with its two source columns null.
2. An all-captured read and a mixed read answer equal schemas.
3. A mixed range keeps each row's own origin in either order, and the answer carries no
   provenance key in its schema metadata.
4. Each of the seven malformed shapes refuses by name, and keys written to the file-level
   metadata are the negative control that shows the write route matters.
5. ``split`` and ``total`` refuse an imported row by ``OriginNotCaptured``, raw or adjusted,
   even with a damaged actions ledger in the root.
6. A ``total`` read whose prior close is imported raises ``AdjustmentIncomplete``.

Every partition is written by ``tests.support.bars.write_bars``, which goes through
``journal.bars_rows`` and ``bars._write_partition`` as the sweep does. Each test's docstring
names the mutation it catches.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake.actions import LedgerLineError
from lake.loader import (
    ADJUST_NONE,
    ADJUST_SPLIT,
    ADJUST_TOTAL,
    BASIS_ADJUSTED,
    BASIS_COLUMN,
    BASIS_KEY,
    BASIS_RAW,
    ORIGIN_CAPTURED,
    ORIGIN_COLUMN,
    ORIGIN_IMPORTED,
    ORIGIN_KEY,
    SOURCE_AS_OF_COLUMN,
    SOURCE_AS_OF_KEY,
    SOURCE_COLUMN,
    SOURCE_ID_KEY,
    SOURCE_KEY,
    AdjustmentIncomplete,
    OriginNotCaptured,
    Provenance,
    ProvenanceMalformed,
    load_bars,
    provenance_metadata,
    read_provenance,
)
from tests.component.test_load_bars import (
    BEFORE,
    DAILY,
    EX,
    INSTRUMENT,
    MINUTE,
    TICKER,
    _dividend,
    _ledger_table,
)
from tests.support.bars import rewrite_metadata, write_bars
from tests.support.lake import FixtureLake

PROVENANCE_COLUMNS = (ORIGIN_COLUMN, BASIS_COLUMN, SOURCE_COLUMN, SOURCE_AS_OF_COLUMN)

# The two kinds of import. The adjusted one names the date its source adjusted to, which the
# vintages' ``saved_date`` of 2012-04-10 is the real example of.
SOURCE = "vintage"
SOURCE_ID = "sha256:" + "ab" * 32
IMPORTED_RAW = Provenance(ORIGIN_IMPORTED, BASIS_RAW, SOURCE, SOURCE_ID)
IMPORTED_ADJUSTED = Provenance(
    ORIGIN_IMPORTED, BASIS_ADJUSTED, SOURCE, SOURCE_ID, date(2012, 4, 10)
)


def _root(fixture_lake: FixtureLake) -> Path:
    """A lake holding only the schema-version ledger, which every bars read needs."""
    fixture_lake.with_reference("schema_versions", _ledger_table())
    return fixture_lake.build()


def _column(table: pa.Table, name: str) -> list:
    return table.column(name).to_pylist()


def _damage_actions_ledger(root: Path) -> None:
    """A ledger line that is a JSON array, which ``actions`` refuses with ``LedgerLineError``."""
    path = root / "actions" / "corporate_actions.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]\n")


# -- 1 to 3. the columns ---------------------------------------------------------------


def test_a_captured_partition_reads_as_captured_and_raw(fixture_lake: FixtureLake):
    """Acceptance 1. Catches a captured default other than ``captured`` and ``raw``."""
    root = _root(fixture_lake)
    write_bars(root, TICKER, DAILY, BEFORE, [600.0], instrument_id=INSTRUMENT)

    table = load_bars(TICKER, DAILY, lake_root=root)

    assert _column(table, "close") == [600.0]
    assert _column(table, ORIGIN_COLUMN) == [ORIGIN_CAPTURED]
    assert _column(table, BASIS_COLUMN) == [BASIS_RAW]
    assert _column(table, SOURCE_COLUMN) == [None]
    assert _column(table, SOURCE_AS_OF_COLUMN) == [None]
    for name in PROVENANCE_COLUMNS:
        assert table.schema.field(name).type == pa.string(), name


def test_an_all_captured_read_and_a_mixed_read_answer_equal_schemas(
    fixture_lake: FixtureLake, tmp_path: Path
):
    """Acceptance 2. Catches a null column built without a type, which reads as Arrow's
    ``null`` type on an all-captured read and as a string once an import joins it, and a
    ``source_as_of`` built as a date rather than ISO text.
    """
    captured = _root(fixture_lake)
    write_bars(captured, TICKER, DAILY, BEFORE, [600.0], instrument_id=INSTRUMENT)
    write_bars(captured, TICKER, DAILY, EX, [300.0], instrument_id=INSTRUMENT)
    mixed = _root(FixtureLake(tmp_path / "mixed"))
    write_bars(mixed, TICKER, DAILY, BEFORE, [600.0], instrument_id=INSTRUMENT)
    write_bars(mixed, TICKER, DAILY, EX, [300.0], instrument_id=None, provenance=IMPORTED_ADJUSTED)

    all_captured = load_bars(TICKER, DAILY, lake_root=captured)
    stitched = load_bars(TICKER, DAILY, lake_root=mixed)

    assert all_captured.schema == stitched.schema
    assert _column(stitched, SOURCE_AS_OF_COLUMN) == [None, "2012-04-10"]


@pytest.mark.parametrize("imported_first", [True, False], ids=["imported-first", "captured-first"])
def test_each_row_keeps_its_own_origin_and_the_answer_carries_no_key(
    fixture_lake: FixtureLake, imported_first: bool
):
    """Acceptance 3. Catches keeping each partition's schema metadata, which the stitch would
    then carry from the first partition over every row, and a provenance read once per range
    rather than per partition.
    """
    root = _root(fixture_lake)
    imported_day, captured_day = (BEFORE, EX) if imported_first else (EX, BEFORE)
    write_bars(
        root, TICKER, DAILY, imported_day, [1.0], instrument_id=None, provenance=IMPORTED_RAW
    )
    write_bars(root, TICKER, DAILY, captured_day, [2.0], instrument_id=INSTRUMENT)

    table = load_bars(TICKER, DAILY, lake_root=root)

    imported = [ORIGIN_IMPORTED, BASIS_RAW, SOURCE, None]
    captured = [ORIGIN_CAPTURED, BASIS_RAW, None, None]
    rows = [imported, captured] if imported_first else [captured, imported]
    by_column = [list(values) for values in zip(*rows, strict=True)]
    assert [_column(table, name) for name in PROVENANCE_COLUMNS] == by_column
    metadata = table.schema.metadata or {}
    assert not [key for key in metadata if key.startswith(b"marketlake.")], metadata


# -- 4. the write route, and malformed provenance ----------------------------------------


def test_the_writer_round_trips_an_import_and_writes_a_capture_unchanged(tmp_path: Path):
    """Catches a ``provenance=None`` write whose bytes move, which would move every manifest
    checksum, and a writer whose keys ``read_provenance`` does not read back.
    """
    plain = write_bars(tmp_path / "a", TICKER, DAILY, BEFORE, [1.0], instrument_id=INSTRUMENT)
    captured = write_bars(
        tmp_path / "b",
        TICKER,
        DAILY,
        BEFORE,
        [1.0],
        instrument_id=INSTRUMENT,
        provenance=Provenance(ORIGIN_CAPTURED, BASIS_RAW),
    )
    imported = write_bars(
        tmp_path / "c",
        TICKER,
        DAILY,
        BEFORE,
        [1.0],
        instrument_id=INSTRUMENT,
        provenance=IMPORTED_ADJUSTED,
    )

    assert pq.read_schema(plain).metadata is None
    assert plain.read_bytes() == captured.read_bytes()
    direct = tmp_path / "direct.parquet"
    pq.write_table(pq.read_table(plain), direct)
    assert plain.read_bytes() == direct.read_bytes()
    assert read_provenance(pq.read_schema(imported).metadata) == IMPORTED_ADJUSTED
    assert read_provenance(pq.read_table(imported).schema.metadata) == IMPORTED_ADJUSTED


@pytest.mark.parametrize(
    "provenance",
    [
        Provenance(ORIGIN_CAPTURED, BASIS_ADJUSTED),
        Provenance(ORIGIN_CAPTURED, BASIS_RAW, SOURCE),
        Provenance(ORIGIN_IMPORTED, BASIS_RAW, SOURCE, None),
        Provenance(ORIGIN_IMPORTED, BASIS_ADJUSTED, SOURCE, SOURCE_ID),
        Provenance("provisional", BASIS_RAW, SOURCE, SOURCE_ID),
    ],
    ids=["captured-adjusted", "captured-with-source", "no-source-id", "adjusted-no-date", "other"],
)
def test_the_writer_refuses_a_provenance_no_read_would_return(provenance: Provenance):
    """Catches a writer that writes what every later read of the file refuses or misreads."""
    with pytest.raises(ValueError):
        provenance_metadata(provenance)


def test_keys_in_the_file_level_metadata_read_as_captured(fixture_lake: FixtureLake):
    """The negative control. ``ParquetWriter.add_key_value_metadata`` writes the same keys to
    the file-level metadata, which ``pq.read_table`` drops, so the import reads as captured and
    an adjusted view would take the ledger's factors on top of its source's own. The same keys
    through ``bars._write_partition`` read as imported.
    """
    root = _root(fixture_lake)
    keys = provenance_metadata(IMPORTED_ADJUSTED)
    routed = write_bars(root, TICKER, DAILY, BEFORE, [1.0], instrument_id=INSTRUMENT)
    table = pq.read_table(routed)
    with pq.ParquetWriter(routed, table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata(keys)
    write_bars(
        root, TICKER, DAILY, EX, [2.0], instrument_id=INSTRUMENT, provenance=IMPORTED_ADJUSTED
    )

    assert pq.ParquetFile(routed).metadata.metadata[ORIGIN_KEY] == ORIGIN_IMPORTED.encode()
    read = load_bars(TICKER, DAILY, lake_root=root)
    assert _column(read, ORIGIN_COLUMN) == [ORIGIN_CAPTURED, ORIGIN_IMPORTED]


_FULL = {
    ORIGIN_KEY: b"imported",
    BASIS_KEY: b"raw",
    SOURCE_KEY: SOURCE.encode(),
    SOURCE_ID_KEY: SOURCE_ID.encode(),
}


def _without(*keys: bytes, **changes: bytes) -> dict[bytes, bytes]:
    """The full raw import's keys, less ``keys``, with ``changes`` by key name."""
    names = {
        "origin": ORIGIN_KEY,
        "basis": BASIS_KEY,
        "source": SOURCE_KEY,
        "source_id": SOURCE_ID_KEY,
        "source_as_of": SOURCE_AS_OF_KEY,
    }
    metadata = {key: value for key, value in _FULL.items() if key not in keys}
    metadata.update({names[name]: value for name, value in changes.items()})
    return metadata


@pytest.mark.parametrize(
    ("metadata", "fragment"),
    [
        (_without(origin=b"captured"), "origin of 'captured'"),
        (_without(ORIGIN_KEY), "no marketlake.origin"),
        ({SOURCE_AS_OF_KEY: b"2012-04-10"}, "no marketlake.origin"),
        (_without(BASIS_KEY), "no marketlake.basis"),
        (_without(SOURCE_KEY), "no marketlake.source,"),
        (_without(source=b""), "no marketlake.source,"),
        (_without(SOURCE_ID_KEY), "no marketlake.source_id"),
        (_without(basis=b"split"), "basis of 'split'"),
        (_without(basis=b"adjusted"), "adjusted and carries no marketlake.source_as_of"),
        (_without(source=b"\xff"), "not UTF-8"),
        (_without(source_as_of=b"20120410"), "'20120410'"),
        (_without(basis=b"adjusted", source_as_of=b"2012-W15-2"), "'2012-W15-2'"),
        (_without(source_as_of=b"2012-02-30"), "'2012-02-30'"),
    ],
    ids=[
        "1-origin-captured",
        "2-no-origin",
        "2-only-a-date",
        "3-no-basis",
        "3-no-source",
        "3-empty-source",
        "3-no-source-id",
        "4-basis",
        "5-adjusted-no-date",
        "6-not-utf8",
        "7-compact-date",
        "7-week-date",
        "7-no-such-day",
    ],
)
def test_malformed_provenance_refuses_by_name(
    fixture_lake: FixtureLake, metadata: dict, fragment: str
):
    """Acceptance 4. Catches each of the seven checks removed, since each file would otherwise
    read back as captured or as an import nothing could vouch for. The view is ``'none'``,
    because a basis nobody stated refuses there too.
    """
    root = _root(fixture_lake)
    path = write_bars(root, TICKER, DAILY, BEFORE, [1.0], instrument_id=INSTRUMENT)
    rewrite_metadata(path, metadata)

    with pytest.raises(ProvenanceMalformed) as caught:
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_NONE)

    assert not isinstance(caught.value, OriginNotCaptured)
    message = str(caught.value)
    assert f"bars/ticker={TICKER}/freq={DAILY}/date={BEFORE}.parquet" in message
    assert fragment in message


def test_metadata_that_is_not_provenance_reads_as_captured(fixture_lake: FixtureLake):
    """Catches a reader that treats any schema metadata as a claim, rather than its own keys."""
    root = _root(fixture_lake)
    path = write_bars(root, TICKER, DAILY, BEFORE, [1.0], instrument_id=INSTRUMENT)
    rewrite_metadata(path, {b"pandas": b"{}", b"marketlakes.origin": b"imported"})

    table = load_bars(TICKER, DAILY, lake_root=root)

    assert _column(table, ORIGIN_COLUMN) == [ORIGIN_CAPTURED]


# -- 5 and 6. the adjusted views --------------------------------------------------------


@pytest.mark.parametrize("adjust", [ADJUST_SPLIT, ADJUST_TOTAL])
@pytest.mark.parametrize("provenance", [IMPORTED_RAW, IMPORTED_ADJUSTED], ids=["raw", "adjusted"])
def test_an_adjusted_view_refuses_an_imported_row_ahead_of_a_damaged_ledger(
    fixture_lake: FixtureLake, adjust: str, provenance: Provenance
):
    """Acceptance 5. The imported row carries no ``instrument_id`` and the actions ledger is
    damaged, so a check that ran after the ledger read raises ``LedgerLineError`` and one that
    ran after the instrument walk raises ``InstrumentUnknown``. Catches the check removed, moved
    behind either, or reading anything but ``captured`` as adjustable. ``'none'`` reads both
    rows, and the same damaged ledger refuses a captured-only range, which shows it is damaged.
    """
    root = _root(fixture_lake)
    write_bars(root, TICKER, DAILY, BEFORE, [600.0], instrument_id=INSTRUMENT)
    write_bars(root, TICKER, DAILY, EX, [300.0], instrument_id=None, provenance=provenance)
    _damage_actions_ledger(root)

    with pytest.raises(OriginNotCaptured) as caught:
        load_bars(TICKER, DAILY, lake_root=root, adjust=adjust)

    assert ORIGIN_IMPORTED in str(caught.value)
    as_stored = load_bars(TICKER, DAILY, lake_root=root)
    assert _column(as_stored, ORIGIN_COLUMN) == [ORIGIN_CAPTURED, ORIGIN_IMPORTED]
    assert _column(as_stored, BASIS_COLUMN) == [BASIS_RAW, provenance.basis]
    with pytest.raises(LedgerLineError):
        load_bars(TICKER, DAILY, BEFORE, BEFORE, lake_root=root, adjust=adjust)


@pytest.mark.parametrize("prior_imported", [True, False], ids=["imported", "captured"])
def test_total_refuses_a_prior_close_marketlake_did_not_capture(
    fixture_lake: FixtureLake, prior_imported: bool
):
    """Acceptance 6. The series asked for is captured minute bars, and the dividend's prior
    close is the ``1d`` session before the ex-date, which here is an import. Catches
    ``_prior_close`` reading that close, and the captured control shows the same layout
    otherwise prices the dividend.
    """
    root = _root(fixture_lake)
    write_bars(root, TICKER, MINUTE, BEFORE, [1000.0], instrument_id=INSTRUMENT)
    write_bars(
        root,
        TICKER,
        DAILY,
        BEFORE,
        [300.0],
        instrument_id=INSTRUMENT,
        provenance=IMPORTED_RAW if prior_imported else None,
    )
    _dividend(root, ex_date=EX)

    if not prior_imported:
        table = load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)
        assert _column(table, "close") == [pytest.approx(1000.0 * (1 - 3.0 / 300.0))]
        return
    with pytest.raises(AdjustmentIncomplete) as caught:
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)
    assert ORIGIN_IMPORTED in str(caught.value)
