"""The schema-version ledger written into a real lake on disk.

These drive the recording tool against a throwaway lake root with a manual clock. A real
parquet file is written, a real manifest entry is appended, and the real two-way integrity
scrub runs over the result. No vendor, no network, and no wall clock is crossed.

The ledger's whole purpose is read against a sealed partition, so the tests read it back
the way a reader would: off the file, and through DuckDB as well, rather than only through
the object that wrote it.
"""

from __future__ import annotations

import hashlib
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import journal
from lake.lock import lake_lock
from lake.manifest import latest_entries, read_manifest, scrub
from lake.paths import TEMP_MARKER
from lake.schema_versions import (
    LEDGER_FILENAME,
    LEDGER_PARTITION,
    LEDGER_SCHEMA,
    LEDGER_SCHEMA_VERSION,
    LedgerUnreadable,
    RecordedVersion,
    SchemaVersionConflict,
    SchemaVersionLedger,
    SchemaVersionsError,
    UnsupportedLedgerSchemaVersion,
    _conflict_detail,
    ledger_path,
    main,
    record_schema_version,
    running_fingerprints,
)
from tests.support.clock import ManualClock
from tests.support.config import write_config

NOW = datetime(2026, 9, 13, 15, 0, tzinfo=UTC)  # 11:00 ET
LATER = datetime(2026, 10, 1, 15, 0, tzinfo=UTC)


def _record(lake_root: Path, when: datetime = NOW):
    return record_schema_version(clock=ManualClock(when), lake_root=lake_root)


# -- the file's own contract --------------------------------------------------


def test_the_ledger_sits_at_the_path_a_restored_lake_reader_queries(lake_root):
    """The path is spelled out, because it is published rather than internal.

    ``docs/design.md`` names ``reference/schema_versions.parquet`` in the lake tree, and a
    reader opening a restored backup types that path. Everything else in this file routes
    through ``ledger_path`` and ``LEDGER_PARTITION``, so a renamed constant would move both
    sides of every other assertion together and none of them would notice.
    """
    assert LEDGER_FILENAME == "schema_versions.parquet"
    assert LEDGER_PARTITION == "reference/schema_versions.parquet"
    assert ledger_path(lake_root) == lake_root / "reference" / "schema_versions.parquet"


def test_the_tables_own_shape_is_pinned_against_its_own_version():
    """The column names, the types, and the stamped version move together or not at all.

    This file is meant to outlive the code that wrote it, so a reader binds to this shape.
    Comparing a written file against ``LEDGER_SCHEMA`` only proves the writer used the
    constant, never that the constant says the right thing, so the shape is spelled out
    here. Changing any of it needs a deliberate bump of ``LEDGER_SCHEMA_VERSION``, which
    is what ``from_table`` refuses an unknown value of.
    """
    assert LEDGER_SCHEMA_VERSION == 1
    assert LEDGER_SCHEMA.names == [
        "journal_schema_version",
        "surface",
        "column_name",
        "column_type",
        "recorded_at",
        "schema_version",
    ]
    assert LEDGER_SCHEMA.field("journal_schema_version").type == pa.int32()
    assert LEDGER_SCHEMA.field("surface").type == pa.string()
    assert LEDGER_SCHEMA.field("column_name").type == pa.string()
    assert LEDGER_SCHEMA.field("column_type").type == pa.string()
    assert LEDGER_SCHEMA.field("recorded_at").type == pa.timestamp("us", tz="UTC")
    assert LEDGER_SCHEMA.field("schema_version").type == pa.int32()


# -- what the first run writes -----------------------------------------------


def test_the_first_run_writes_one_row_per_surface_column_derived_from_the_schema(lake_root):
    report = _record(lake_root)

    table = pq.read_table(ledger_path(lake_root))
    assert table.schema == LEDGER_SCHEMA
    rows = table.to_pylist()
    # The shape on disk is the one the journal derives, column for column and type for
    # type. Nothing here restates a column set, so a schema edit moves this without any
    # help. The two are compared as mappings, since order carries no meaning.
    written: dict[str, dict[str, str]] = {}
    for row in rows:
        assert row["journal_schema_version"] == journal.SCHEMA_VERSION
        assert row["schema_version"] == LEDGER_SCHEMA_VERSION
        assert row["recorded_at"] == NOW
        written.setdefault(row["surface"], {})[row["column_name"]] = row["column_type"]
    # Against the derivation spelled out, rather than against ``running_fingerprints``,
    # which is the helper the writer itself called and so would compare the file to
    # itself. The helper is pinned to the same derivation on the line below.
    derived = {surface: journal.schema_fingerprint(surface) for surface in journal.PINNED_SURFACES}
    assert written == derived
    assert running_fingerprints() == derived
    assert len(rows) == sum(len(columns) for columns in written.values())
    assert report.already_recorded is False
    assert report.versions == (journal.SCHEMA_VERSION,)
    assert report.rows == len(rows)


def test_the_ledger_answers_whether_a_version_carried_a_column(lake_root):
    # This is the question the ledger exists for. A null under a column the version never
    # had was never observed, where a null under a column it did have is a vendor null.
    _record(lake_root)
    recorded = SchemaVersionLedger.read(ledger_path(lake_root)).get(journal.SCHEMA_VERSION)
    assert recorded is not None
    assert recorded.has_column(journal.CHAINS_SURFACE, "bid") is True
    assert recorded.has_column(journal.CHAINS_SURFACE, "no_such_column") is False
    assert recorded.has_column("bars", "bid") is False


def test_a_reader_answers_the_same_question_in_plain_sql(lake_root):
    # The long shape is the reason the table is one row per column rather than one row per
    # version holding a serialized mapping. The lookup is a predicate, with no JSON to
    # pick apart, which is how the dashboard and a read-time projection would ask it.
    duckdb = pytest.importorskip("duckdb")
    _record(lake_root)
    path = str(ledger_path(lake_root))
    with duckdb.connect() as con:
        held = con.execute(
            "select count(*) from read_parquet(?) "
            "where journal_schema_version = ? and surface = ? and column_name = ?",
            [path, journal.SCHEMA_VERSION, journal.CHAINS_SURFACE, "bid"],
        ).fetchone()[0]
        absent = con.execute(
            "select count(*) from read_parquet(?) "
            "where journal_schema_version = ? and surface = ? and column_name = ?",
            [path, journal.SCHEMA_VERSION, journal.CHAINS_SURFACE, "no_such_column"],
        ).fetchone()[0]
    assert held == 1
    assert absent == 0


# -- the manifest entry and the scrub ----------------------------------------


def test_the_ledger_is_manifested_so_the_reverse_scrub_finds_no_orphan(lake_root):
    report = _record(lake_root)

    entries = latest_entries(lake_root)
    assert LEDGER_PARTITION in entries
    entry = entries[LEDGER_PARTITION]
    assert entry["source"] == "reference"
    assert entry["rows"] == report.rows
    assert entry["fetched_at"] == NOW.isoformat()

    # The reverse pass asks every file under the lake root for an entry, and its exclusion
    # set is {manifest.jsonl, journal/, reports/}. ``reference/`` is not on it, so the
    # ledger is covered with nothing added to the scrub.
    result = scrub(lake_root)
    assert result.orphans == ()
    assert result.missing == ()
    assert result.sha_mismatches == ()
    assert result.ok


def test_the_recorded_sha_matches_the_bytes_on_disk(lake_root):
    # A manifest entry appended before the rename, or against a different file, would pass
    # the orphan check above and still fail here.
    _record(lake_root)
    on_disk = hashlib.sha256(ledger_path(lake_root).read_bytes()).hexdigest()
    assert latest_entries(lake_root)[LEDGER_PARTITION]["sha256"] == on_disk


# -- re-running ---------------------------------------------------------------


def test_a_second_run_writes_nothing_at_all(lake_root):
    first = _record(lake_root)
    ledger_bytes = ledger_path(lake_root).read_bytes()
    manifest_bytes = (lake_root / "manifest.jsonl").read_bytes()

    # A later clock, so a stamp rewritten on the second run would show up in the bytes.
    second = _record(lake_root, LATER)

    assert second.already_recorded is True
    assert second.recorded.recorded_at == NOW
    assert second.versions == first.versions
    assert second.rows == first.rows
    assert ledger_path(lake_root).read_bytes() == ledger_bytes
    assert (lake_root / "manifest.jsonl").read_bytes() == manifest_bytes
    assert len(read_manifest(lake_root)) == 1
    assert scrub(lake_root).ok


# -- the lock -----------------------------------------------------------------


def test_the_ledger_and_its_entry_are_written_under_the_lake_lock(lake_root):
    # Every lake-mutating job takes the one lake-root flock first. A writer that skipped it
    # would land its file while this test holds the lock, which is what the assertions
    # inside the ``with`` block catch.
    target = ledger_path(lake_root)
    failures: list[BaseException] = []
    done = threading.Event()

    def run() -> None:
        try:
            _record(lake_root)
        except BaseException as exc:  # noqa: BLE001 - reported to the main thread below
            failures.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=run)
    with lake_lock(lake_root):
        worker.start()
        # Long enough for the worker to reach the lock and block on it.
        time.sleep(0.3)
        assert not target.exists()
        assert read_manifest(lake_root) == []
        assert not done.is_set()

    assert done.wait(10)
    worker.join(10)
    assert failures == []
    assert target.exists()
    assert [entry["partition"] for entry in read_manifest(lake_root)] == [LEDGER_PARTITION]


# -- a version already recorded under a different shape -----------------------


def _reshaped(surface: str) -> pa.Schema:
    """The surface's schema with one column dropped, one added, and one retyped."""
    fields = [field for field in journal.schema_for(surface) if field.name != "bid"]
    fields = [pa.field("ask", pa.string()) if f.name == "ask" else f for f in fields]
    return pa.schema([*fields, pa.field("brand_new", pa.int64())])


def test_a_version_recorded_under_a_different_shape_refuses_rather_than_overwriting(
    lake_root, monkeypatch
):
    # Getting here means a column moved without a bump, which the suite's own fingerprint
    # check refuses, so the suite was bypassed. Overwriting would rewrite what the version
    # meant for every row already sealed at it.
    monkeypatch.setitem(journal._SCHEMAS, journal.CHAINS_SURFACE, _reshaped(journal.CHAINS_SURFACE))
    _record(lake_root)
    before = ledger_path(lake_root).read_bytes()
    manifest_before = (lake_root / "manifest.jsonl").read_bytes()
    monkeypatch.undo()

    with pytest.raises(SchemaVersionConflict) as excinfo:
        _record(lake_root, LATER)

    detail = str(excinfo.value)
    assert excinfo.value.version == journal.SCHEMA_VERSION
    assert f"journal schema version {journal.SCHEMA_VERSION} is already recorded" in detail
    assert "chains dropped: brand_new" in detail
    assert "chains added: bid" in detail
    assert "chains retyped: ask string -> double" in detail
    assert "bump journal.SCHEMA_VERSION" in detail
    # Nothing moved. The refusal is the whole behaviour.
    assert ledger_path(lake_root).read_bytes() == before
    assert (lake_root / "manifest.jsonl").read_bytes() == manifest_before


def test_the_conflict_message_names_a_surface_only_one_side_holds():
    """A surface gained or lost between the two shapes is named, not skipped.

    ``PINNED_SURFACES`` itself can move, and that is a shape change the ledger exists to
    describe. A message assembled over only the surfaces both sides hold would say a
    version disagrees and then name nothing at all.
    """
    gained = _conflict_detail(1, {"chains": {"bid": "double"}}, {})
    assert "chains added: bid" in gained
    lost = _conflict_detail(1, {}, {"quotes": {"ask": "double"}})
    assert "quotes dropped: ask" in lost


def test_the_conflict_message_names_a_retype_that_moved_nothing_else():
    """The one-category conflict, which is the vendor retyping a column and nothing else.

    The reshaped schema used above drops, adds, and retypes at once, so it cannot tell
    whether a retype alone reaches the message.
    """
    detail = _conflict_detail(
        1, {"chains": {"open_interest": "double"}}, {"chains": {"open_interest": "int64"}}
    )
    assert "chains retyped: open_interest int64 -> double" in detail
    assert "chains dropped: none" in detail
    assert "chains added: none" in detail


def test_the_conflict_message_stays_silent_about_a_surface_that_did_not_move(
    lake_root, monkeypatch
):
    monkeypatch.setitem(journal._SCHEMAS, journal.CHAINS_SURFACE, _reshaped(journal.CHAINS_SURFACE))
    _record(lake_root)
    monkeypatch.undo()

    with pytest.raises(SchemaVersionConflict) as excinfo:
        _record(lake_root, LATER)

    assert journal.QUOTES_SURFACE not in str(excinfo.value)


# -- an earlier version survives a later one ----------------------------------


def test_recording_a_new_version_preserves_every_earlier_one(lake_root, monkeypatch):
    # The lake holds rows at the old version forever, so the old shape has to stay
    # readable. A later run adds a version, it never rewrites the file's history.
    base = journal.SCHEMA_VERSION
    _record(lake_root)
    earlier_rows = pq.read_table(ledger_path(lake_root)).to_pylist()

    monkeypatch.setattr(journal, "SCHEMA_VERSION", base + 1)
    monkeypatch.setitem(journal._SCHEMAS, journal.QUOTES_SURFACE, _reshaped(journal.QUOTES_SURFACE))
    report = _record(lake_root, LATER)

    assert report.already_recorded is False
    assert report.versions == (base, base + 1)
    rows = pq.read_table(ledger_path(lake_root)).to_pylist()
    assert [row for row in rows if row["journal_schema_version"] == base] == earlier_rows

    ledger = SchemaVersionLedger.read(ledger_path(lake_root))
    old, new = ledger.get(base), ledger.get(base + 1)
    assert old is not None and new is not None
    assert old.recorded_at == NOW
    assert new.recorded_at == LATER
    assert old.has_column(journal.QUOTES_SURFACE, "bid") is True
    assert new.has_column(journal.QUOTES_SURFACE, "bid") is False
    assert new.has_column(journal.QUOTES_SURFACE, "brand_new") is True
    # The second write is a second manifest entry for the same path. Last entry wins, and
    # the row count grew, so the standing no-shrink invariant is untouched.
    assert [entry["partition"] for entry in read_manifest(lake_root)] == [LEDGER_PARTITION] * 2
    assert latest_entries(lake_root)[LEDGER_PARTITION]["rows"] == len(rows)
    assert scrub(lake_root).ok


# -- the file itself ----------------------------------------------------------


def test_the_round_trip_preserves_every_version(tmp_path):
    ledger = SchemaVersionLedger(
        [
            RecordedVersion(version=1, recorded_at=NOW, fingerprints=running_fingerprints()),
            RecordedVersion(
                version=2, recorded_at=LATER, fingerprints={"chains": {"bid": "double"}}
            ),
        ]
    )
    path = ledger.write(tmp_path / "reference" / "schema_versions.parquet")
    back = SchemaVersionLedger.read(path)

    assert back.versions() == (1, 2)
    assert back.row_count == ledger.row_count
    for version in (1, 2):
        original, restored = ledger.get(version), back.get(version)
        assert original is not None and restored is not None
        assert restored.recorded_at == original.recorded_at
        assert {s: dict(c) for s, c in restored.fingerprints.items()} == {
            s: dict(c) for s, c in original.fingerprints.items()
        }


def test_a_ledger_stamped_with_an_unknown_table_version_refuses(tmp_path):
    # The same refusal the security master and the capture spans give. A file this code
    # cannot read is never read as though it were empty.
    table = pa.table(
        {
            "journal_schema_version": [1],
            "surface": ["chains"],
            "column_name": ["bid"],
            "column_type": ["double"],
            "recorded_at": [NOW],
            "schema_version": [LEDGER_SCHEMA_VERSION + 1],
        },
        schema=LEDGER_SCHEMA,
    )
    with pytest.raises(UnsupportedLedgerSchemaVersion) as excinfo:
        SchemaVersionLedger.from_table(table)
    assert excinfo.value.found == LEDGER_SCHEMA_VERSION + 1


def test_a_torn_ledger_refuses_rather_than_reading_as_empty(lake_root):
    _record(lake_root)
    path = ledger_path(lake_root)
    path.write_bytes(path.read_bytes()[: len(path.read_bytes()) // 2])
    with pytest.raises(LedgerUnreadable):
        SchemaVersionLedger.read(path)
    # And the tool refuses too, rather than starting a fresh ledger over the torn one.
    with pytest.raises(LedgerUnreadable):
        _record(lake_root, LATER)


def test_an_empty_ledger_writes_a_valid_empty_file(tmp_path):
    path = SchemaVersionLedger().write(tmp_path / "reference" / "schema_versions.parquet")
    assert SchemaVersionLedger.read(path).versions() == ()


def test_adding_a_version_already_present_refuses(tmp_path):
    entry = RecordedVersion(version=1, recorded_at=NOW, fingerprints={"chains": {"bid": "double"}})
    ledger = SchemaVersionLedger([entry])
    with pytest.raises(SchemaVersionsError, match="already recorded"):
        ledger.with_version(entry)


# -- the command-line entry ---------------------------------------------------


def test_main_records_through_the_real_entry(lake_root, tmp_path, capsys):
    # Driving ``main`` rather than the core, so the argument parsing, the config load, and
    # the printed sign-off are all executed rather than only described.
    config = write_config(tmp_path, lake_root)
    assert main(["--config", str(config)]) == 0

    out = capsys.readouterr().out
    assert f"Recorded journal schema_version {journal.SCHEMA_VERSION}" in out
    assert str(ledger_path(lake_root)) in out
    assert ledger_path(lake_root).exists()
    assert scrub(lake_root).ok

    # Every line the sign-off renders, not only its first. The versions line is the one a
    # reader scans for a gap in the sequence, which is a bump whose tool run never ran.
    rows = len(pq.read_table(ledger_path(lake_root)))
    assert f"versions:        {journal.SCHEMA_VERSION}" in out
    assert f"ledger rows:     {rows}" in out
    for surface in journal.PINNED_SURFACES:
        assert f"{surface} ({len(journal.schema_fingerprint(surface))} columns)" in out
    assert "recorded at:     20" in out

    assert main(["--config", str(config)]) == 0
    assert "already recorded with this shape" in capsys.readouterr().out


# -- the atomic write ---------------------------------------------------------


def test_the_write_goes_through_a_temp_file_the_backup_excludes(tmp_path, monkeypatch):
    """A temp file has to carry the marker, or a crashed write rides to the backup.

    ``runner.BACKUP_EXCLUSIONS`` drops ``*{marker}*`` and nothing else temp-shaped, so a
    writer that invents its own suffix puts half a reference table on the backup disk.
    The path handed to the parquet writer is captured, since the finished write renames it
    away before anything could look.
    """
    import lake.schema_versions as module

    seen: list[Path] = []
    real_write = module.pq.write_table

    def capture(table, where, *args, **kwargs):
        seen.append(Path(where))
        return real_write(table, where, *args, **kwargs)

    monkeypatch.setattr(module.pq, "write_table", capture)
    target = tmp_path / "reference" / "schema_versions.parquet"
    SchemaVersionLedger().write(target)

    assert len(seen) == 1
    assert seen[0] != target
    assert seen[0].parent == target.parent
    assert TEMP_MARKER in seen[0].name
    assert seen[0].name.startswith(target.name)


def test_a_failed_write_leaves_no_temp_file_behind(tmp_path, monkeypatch):
    """The target is untouched and no debris is left, so a retry starts clean."""
    import lake.schema_versions as module

    def explode(table, where, *args, **kwargs):
        Path(where).write_bytes(b"half a table")
        raise RuntimeError("disk full")

    monkeypatch.setattr(module.pq, "write_table", explode)
    target = tmp_path / "reference" / "schema_versions.parquet"
    with pytest.raises(RuntimeError, match="disk full"):
        SchemaVersionLedger().write(target)

    assert not target.exists()
    assert list(target.parent.iterdir()) == []
