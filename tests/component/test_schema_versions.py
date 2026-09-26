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
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import journal, paths
from lake.lock import lake_lock
from lake.manifest import latest_entries, read_manifest, scrub
from lake.paths import TEMP_MARKER
from lake.report import redacted
from lake.schema_versions import (
    CONFLICT_EVENT,
    CONFLICTING,
    INACCESSIBLE,
    LEDGER_FILENAME,
    LEDGER_PARTITION,
    LEDGER_SCHEMA,
    LEDGER_SCHEMA_VERSION,
    PAGE_BODY_BYTE_CAP,
    PAGE_COLUMN_CAP,
    RECORDED,
    UNREADABLE,
    UNREADABLE_EVENT,
    UNRECORDED,
    UNRECORDED_EVENT,
    LedgerUnreadable,
    RecordedVersion,
    SchemaVersionConflict,
    SchemaVersionLedger,
    SchemaVersionsError,
    UnsupportedLedgerSchemaVersion,
    _conflict_detail,
    _page_moved,
    check_running_version,
    ledger_path,
    main,
    record_schema_version,
    running_fingerprints,
)
from tests.support.clock import ManualClock
from tests.support.config import write_config

NOW = datetime(2026, 9, 13, 15, 0, tzinfo=UTC)  # 11:00 ET
LATER = datetime(2026, 10, 1, 15, 0, tzinfo=UTC)

# A surface the lake lays out a directory for and ``journal`` pins no capture schema for,
# so the ledger can never hold a shape for it.
UNPINNED_SURFACE = paths.ACTIONS


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


def test_the_running_version_records_a_shape_for_the_surface_nothing_journals(lake_root):
    """Bars are pinned and never journaled, and the ledger records them anyway.

    ``journal_schema_version`` names the version the code was running, and ``bars`` rows
    stamp that same integer despite never reaching a segment. So the ledger has to carry the
    bars shape under it, or a bars row's own version would point at a record that does not
    describe its surface and ``project_extra`` would have nothing to lift against.
    """
    _record(lake_root)

    recorded = SchemaVersionLedger.read(ledger_path(lake_root)).get(journal.SCHEMA_VERSION)
    assert recorded is not None
    assert set(recorded.fingerprints) == set(journal.PINNED_SURFACES)
    assert recorded.fingerprints[journal.BARS_SURFACE] == journal.schema_fingerprint(
        journal.BARS_SURFACE
    )
    assert recorded.has_column(journal.BARS_SURFACE, "bar_ts") is True
    assert recorded.has_column(journal.BARS_SURFACE, "open") is True
    assert recorded.has_column(journal.BARS_SURFACE, "row_kind") is False


def test_the_ledger_a_journal_bump_writes_keeps_the_earlier_version_two_surfaces_wide(
    lake_root, monkeypatch
):
    """A bump moves the recorded surface set forward without rewriting what came before.

    This is the live shape of the version line this surface joined. The version below knows
    the surfaces that existed when it was minted, and the version above knows one more. A
    reader asking whether a row's own version carried a column gets the right answer on both
    sides, which is the whole reason bars were not recorded under the earlier version.
    """
    base = journal.SCHEMA_VERSION
    narrowed = {
        surface: schema
        for surface, schema in journal._SCHEMAS.items()
        if surface != journal.BARS_SURFACE
    }
    monkeypatch.setattr(journal, "_SCHEMAS", narrowed)
    monkeypatch.setattr(journal, "PINNED_SURFACES", tuple(narrowed))
    monkeypatch.setattr(journal, "SCHEMA_VERSION", base - 1)
    _record(lake_root)
    monkeypatch.undo()

    report = _record(lake_root, LATER)

    assert report.already_recorded is False
    assert report.versions == (base - 1, base)
    ledger = SchemaVersionLedger.read(ledger_path(lake_root))
    below, above = ledger.get(base - 1), ledger.get(base)
    assert below is not None and above is not None
    assert set(below.fingerprints) == {journal.CHAINS_SURFACE, journal.QUOTES_SURFACE}
    assert set(above.fingerprints) == set(journal.PINNED_SURFACES)
    assert below.has_column(journal.BARS_SURFACE, "bar_ts") is False
    assert above.has_column(journal.BARS_SURFACE, "bar_ts") is True
    assert below.fingerprints[journal.CHAINS_SURFACE] == above.fingerprints[journal.CHAINS_SURFACE]
    assert scrub(lake_root).ok


def test_the_ledger_answers_whether_a_version_carried_a_column(lake_root):
    # This is the question the ledger exists for. A null under a column the version never
    # had was never observed, where a null under a column it did have is a vendor null.
    _record(lake_root)
    recorded = SchemaVersionLedger.read(ledger_path(lake_root)).get(journal.SCHEMA_VERSION)
    assert recorded is not None
    assert recorded.has_column(journal.CHAINS_SURFACE, "bid") is True
    assert recorded.has_column(journal.CHAINS_SURFACE, "no_such_column") is False
    # A surface the running version covers where the column does not belong to it, and a
    # surface the version does not cover at all. Both read false, and each asks a different
    # question: the first is a column bars never had, the second a surface nothing pins.
    assert recorded.has_column(journal.BARS_SURFACE, "bid") is False
    assert recorded.has_column(UNPINNED_SURFACE, "bid") is False


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


# -- is the running version recorded at all -----------------------------------

# Marketlake #130. ``record_schema_version`` is hand-invoked beside a deliberate bump and
# nothing forced the run, so a version reached the lake with its shape recorded nowhere and
# every read of its rows refused. These drive the check that says so.


def _narrowed(surface: str = journal.CHAINS_SURFACE) -> dict[str, dict[str, str]]:
    """The running shape with one column taken off ``surface``.

    This is a column added without a bump, seen from the ledger's side. It is the direction
    that costs a read its diagnosis: the projection reports the retype it would otherwise have
    named as a value the column refused.
    """
    shapes = {name: dict(columns) for name, columns in running_fingerprints().items()}
    shapes[surface].pop("bid")
    return shapes


def _widened(surface: str = journal.CHAINS_SURFACE) -> dict[str, dict[str, str]]:
    """The running shape with one column the running code does not carry.

    This is a column dropped without a bump. It is the silent direction: the column sits
    outside the projection's reachable set, nothing is reported, and the read comes back
    whole while the dropped column's nulls read as vendor nulls.
    """
    shapes = {name: dict(columns) for name, columns in running_fingerprints().items()}
    shapes[surface]["gamma_impact"] = "double"
    return shapes


def _record_shape(lake_root: Path, fingerprints, version: int | None = None) -> None:
    """Put one version's shape in the ledger by hand, so a test can record a wrong one."""
    entry = RecordedVersion(
        version=journal.SCHEMA_VERSION if version is None else version,
        recorded_at=NOW,
        fingerprints=fingerprints,
    )
    SchemaVersionLedger([entry]).write(ledger_path(lake_root))


def test_the_version_a_run_recorded_reads_back_as_recorded(lake_root):
    """The healthy verdict, and the only one that says nothing anywhere."""
    _record(lake_root)

    check = check_running_version(lake_root)

    assert check.ok
    assert check.state == RECORDED
    assert check.version == journal.SCHEMA_VERSION
    assert check.recorded == (journal.SCHEMA_VERSION,)
    # Every reportable field is absent, which is what makes "say nothing when healthy" a
    # property of the verdict rather than a rule each caller has to remember.
    assert (check.summary, check.page_body, check.detail, check.event, check.title) == (
        None,
        None,
        None,
        None,
        None,
    )


def test_a_lake_whose_ledger_was_never_written_reads_as_unrecorded(lake_root):
    """The condition this issue is about: the tool was never run against this lake."""
    check = check_running_version(lake_root)

    assert not check.ok
    assert check.state == UNRECORDED
    assert check.recorded == ()
    assert check.event == UNRECORDED_EVENT
    assert str(journal.SCHEMA_VERSION) in check.page_body


def test_a_ledger_holding_only_an_earlier_version_reads_as_unrecorded(lake_root):
    """The live shape on 2026-09-17: version 1 recorded, version 2 running and absent.

    The versions the ledger does hold come back on the verdict, because a reader looking at a
    page needs to tell a lake that was never recorded from one whose recording stopped at an
    earlier bump.
    """
    _record_shape(lake_root, running_fingerprints(), version=journal.SCHEMA_VERSION - 1)

    check = check_running_version(lake_root)

    assert check.state == UNRECORDED
    assert check.recorded == (journal.SCHEMA_VERSION - 1,)
    assert str(journal.SCHEMA_VERSION - 1) in check.page_body


@pytest.mark.parametrize(
    "shape, moved",
    [(_narrowed, "added bid"), (_widened, "dropped gamma_impact")],
    ids=["narrower", "wider"],
)
def test_the_running_version_recorded_under_another_shape_reads_as_conflicting(
    lake_root, shape, moved
):
    """Both directions of the shape this check exists to catch, and the reader cannot.

    ``project_extra`` asks the ledger ``has_column`` and never compares the recorded shape
    against ``journal.schema_fingerprint``, so a wrong record either misdiagnoses a retype or,
    in the wider direction, reports nothing at all. Neither reaches a person. This does.
    """
    _record_shape(lake_root, shape())

    check = check_running_version(lake_root)

    assert not check.ok
    assert check.state == CONFLICTING
    assert check.event == CONFLICT_EVENT
    assert check.recorded == (journal.SCHEMA_VERSION,)
    # The column that moved is named, which is the whole reason the fingerprint is a column
    # list rather than a digest, and it is named under the right verb. The two arguments are
    # one swap apart, and swapped they render the right column the wrong way round, which
    # sends an operator to put back a column they should be taking out.
    assert moved in check.page_body


def test_the_check_agrees_with_the_run_it_sends_an_operator_to(lake_root):
    """A conflict the check reports is a conflict ``record_schema_version`` refuses.

    A check deriving its own comparison could pass here and then fail the operator's run,
    which pages nobody and then refuses the repair. Both read ``_as_plain`` against
    ``running_fingerprints``, so they cannot disagree.
    """
    _record_shape(lake_root, _narrowed())

    assert check_running_version(lake_root).state == CONFLICTING
    with pytest.raises(SchemaVersionConflict):
        _record(lake_root)


def test_a_run_that_was_owed_clears_the_verdict(lake_root):
    """The repair is the tool, and the check goes quiet the moment it lands."""
    assert check_running_version(lake_root).state == UNRECORDED

    _record(lake_root)

    assert check_running_version(lake_root).ok


# -- the check never raises ---------------------------------------------------


def _bytes_that_are_not_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not a parquet file")


def _truncated_parquet(path: Path) -> None:
    whole = path.parent / "whole.parquet"
    SchemaVersionLedger().write(whole)
    payload = whole.read_bytes()
    whole.unlink()
    path.write_bytes(payload[: len(payload) // 2])


def _other_columns(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"who": ["me"], "what": [1]}), path)


def _a_ledger_format_from_the_future(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "journal_schema_version": pa.array([journal.SCHEMA_VERSION], pa.int32()),
                "surface": [journal.CHAINS_SURFACE],
                "column_name": ["bid"],
                "column_type": ["double"],
                "recorded_at": pa.array([NOW], pa.timestamp("us", tz="UTC")),
                "schema_version": pa.array([LEDGER_SCHEMA_VERSION + 1], pa.int32()),
            },
            schema=LEDGER_SCHEMA,
        ),
        path,
    )


def _a_directory(path: Path) -> None:
    path.mkdir(parents=True)


def _a_null_surface(path: Path) -> None:
    """A ledger the pinned schema accepts and no run could have written.

    Every field of ``LEDGER_SCHEMA`` is nullable, so this file parses. It is the shape that
    proves the guard has to cover the decision rather than the read: a null reaches ``sorted``
    inside ``_page_moved`` as a ``TypeError``, long after ``read`` has returned.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "journal_schema_version": pa.array([journal.SCHEMA_VERSION], pa.int32()),
                "surface": pa.array([None], pa.string()),
                "column_name": ["bid"],
                "column_type": ["double"],
                "recorded_at": pa.array([NOW], pa.timestamp("us", tz="UTC")),
                "schema_version": pa.array([LEDGER_SCHEMA_VERSION], pa.int32()),
            },
            schema=LEDGER_SCHEMA,
        ),
        path,
    )


def _the_right_names_at_the_wrong_types(path: Path) -> None:
    """A foreign parquet whose column names match, which no ``KeyError`` can catch."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "journal_schema_version": pa.array([journal.SCHEMA_VERSION], pa.int32()),
                "surface": pa.array([7], pa.int64()),
                "column_name": pa.array([9], pa.int64()),
                "column_type": ["double"],
                "recorded_at": pa.array([NOW], pa.timestamp("us", tz="UTC")),
                "schema_version": pa.array([LEDGER_SCHEMA_VERSION], pa.int32()),
            }
        ),
        path,
    )


@pytest.mark.parametrize(
    "build, state",
    [
        (_bytes_that_are_not_parquet, UNREADABLE),
        (_truncated_parquet, UNREADABLE),
        (_other_columns, UNREADABLE),
        (_a_ledger_format_from_the_future, UNREADABLE),
        (_a_directory, UNRECORDED),
        (_a_null_surface, UNREADABLE),
        (_the_right_names_at_the_wrong_types, UNREADABLE),
    ],
    ids=[
        "not parquet",
        "truncated",
        "other columns",
        "future format",
        "a directory",
        "a null surface",
        "the right names at the wrong types",
    ],
)
def test_no_ledger_this_code_cannot_read_takes_the_daemon_down(lake_root, build, state):
    """Every way the read fails becomes a verdict, because the caller is a daemon at startup.

    The list is not two long, which is why the guard is broad rather than a set of named
    classes. An absent file raises ``OSError``, a torn one ``LedgerUnreadable``, a format this
    code does not read ``UnsupportedLedgerSchemaVersion``, and some other parquet file at that
    path a bare ``KeyError``. A guard naming the first three lets the fourth take the session.

    The list is not even one long past the read. Every field of ``LEDGER_SCHEMA`` is nullable,
    so a ledger with a null ``surface`` parses and then reaches ``sorted`` inside
    ``_page_moved`` as a ``TypeError``. A foreign parquet whose names match and whose types do
    not reaches ``str.join`` the same way, and no ``KeyError`` sees it. So the guard covers the
    whole decision rather than the read.

    A directory is the odd row. ``pq.read_table`` reads one as a dataset and an empty one
    yields an empty ledger, so the running version is simply absent from it.
    """
    build(ledger_path(lake_root))

    check = check_running_version(lake_root)

    assert check.state == state
    assert check.version == journal.SCHEMA_VERSION


def test_an_unreadable_ledger_says_which_file_and_what_refused_it(lake_root):
    """The operator needs the class, since the three verdicts send them to three repairs."""
    _other_columns(ledger_path(lake_root))

    check = check_running_version(lake_root)

    assert check.event == UNREADABLE_EVENT
    assert "KeyError" in check.summary
    assert str(ledger_path(lake_root)) in check.detail


def test_a_ledger_this_process_may_not_open_is_inaccessible_rather_than_unrecorded(lake_root):
    """A locked ledger is not an absent one, and saying otherwise sends an operator nowhere.

    ``python -m lake.schema_versions`` is the repair a "not recorded" page names, and it opens
    this same file and dies the same way. The sweep's reference readers were widened for
    exactly that reason under marketlake #435, whose own test locks this very file.

    ``os.path.exists`` answers False on any ``OSError``, so a check that looked with it before
    reading would fall into the absent arm. ``pathlib.Path.exists`` raises instead on Python
    3.12, and the ``chmod`` below leaves the file's own stat working, so either answers True
    here (measured under marketlake #536). The check reads straight through and tells the two
    apart by class, which does not rest on which ``exists`` a later edit reaches for.
    """
    _record(lake_root)
    target = ledger_path(lake_root)
    os.chmod(target, 0o000)
    try:
        check = check_running_version(lake_root)
    finally:
        os.chmod(target, 0o644)

    assert check.state == INACCESSIBLE == "inaccessible"
    assert check.recorded == ()
    assert "PermissionError" in check.summary
    assert str(target) in check.detail
    assert "Permission denied" in check.detail, "the detail lost the exception's own message"
    # And the shape really is recorded, so "not recorded" would have been false as well as
    # useless. A recorded verdict pages nobody either.
    healthy = check_running_version(lake_root)
    assert healthy.ok
    assert not healthy.pages


def test_a_ledger_this_process_may_not_open_pages_nobody(lake_root):
    """Marketlake #536. A refused open is not a torn file, and the page's reason is the latter.

    The unreadable page earns its tier because the next backup would copy a torn ledger over
    the last good copy. The backup cannot open a refused file either, so it copies nothing and
    fails, and the ``compaction`` check's silence pages for a refusal that lasts. On
    2026-09-19 the refusal lasted seconds and the page went out anyway.
    """
    _record(lake_root)
    target = ledger_path(lake_root)
    os.chmod(target, 0o000)
    try:
        check = check_running_version(lake_root)
    finally:
        os.chmod(target, 0o644)

    assert not check.ok
    assert not check.pages
    assert (check.page_body, check.event, check.title) == (None, None, None)
    assert redacted(check.summary) == check.summary


def test_a_permission_error_after_the_open_still_pages(lake_root, monkeypatch):
    """Only a refused open is ``INACCESSIBLE``, because only a refused open says the file is intact.

    The decision after the open does no I/O today. A later edit that adds some must not turn a
    failure there into a verdict that pages nobody.
    """
    _record(lake_root)

    def refused():
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr("lake.schema_versions.running_fingerprints", refused)
    check = check_running_version(lake_root)

    assert check.state == UNREADABLE
    assert check.pages


def test_a_corrupt_ledger_pyarrow_reports_as_a_bare_oserror_still_pages(lake_root, monkeypatch):
    """The no-page arm is ``PermissionError`` alone, because corruption arrives as ``OSError``.

    Of 400 random byte flips in a real ledger, measured under marketlake #536, 262 raised
    ``OSError: Corrupt snappy compressed data`` from pyarrow rather than ``LedgerUnreadable``.
    An arm catching ``OSError`` would have stopped the page for the torn ledger it exists for.
    """
    _record(lake_root)

    def corrupt(cls, path):
        raise OSError("Corrupt snappy compressed data.")

    monkeypatch.setattr(SchemaVersionLedger, "read", classmethod(corrupt))
    check = check_running_version(lake_root)

    assert check.state == UNREADABLE
    assert check.pages
    assert check.event == UNREADABLE_EVENT


def test_a_lake_root_that_does_not_exist_reads_as_unrecorded(tmp_path):
    """An unmounted drive is not a crash. It reads as no ledger, and the detail names a path.

    That path is the only thing separating this from a version genuinely never recorded, which
    is why the stderr detail carries it and the page and the report line do not.
    """
    missing = tmp_path / "not-mounted"

    check = check_running_version(missing)

    assert check.state == UNRECORDED
    assert str(missing) in check.detail


# -- what the page and the report line are allowed to carry -------------------


def _three_clause_conflict() -> dict[str, dict[str, str]]:
    """A recorded shape that drops, adds and retypes columns on every pinned surface.

    This is what ``_page_moved`` can actually emit at its widest: three clauses per surface
    across three surfaces, so nine capped lists in one body. The all-retyped shape below
    produces one clause per surface and is the narrow case, not the wide one.
    """
    shape = {}
    for surface, columns in running_fingerprints().items():
        kept = dict(list(columns.items())[:PAGE_COLUMN_CAP])
        for index in range(PAGE_COLUMN_CAP):
            kept[f"gone_{surface}_{index:02d}"] = "double"
        for name in list(kept)[:PAGE_COLUMN_CAP]:
            kept[name] = "RETYPED"
        shape[surface] = kept
    return shape


@pytest.mark.parametrize(
    "shape",
    [
        lambda: {
            surface: {name: "RETYPED" for name in columns}
            for surface, columns in running_fingerprints().items()
        },
        _three_clause_conflict,
    ],
    ids=["every column retyped", "dropped, added and retyped on every surface"],
)
def test_the_page_body_stays_inside_the_design_body_budget(lake_root, shape):
    """A conflict wide enough to blow the budget still sends a page.

    The design pins a page body at plain text under 1,000 bytes. ``_conflict_detail`` renders
    every column that moved on every surface and is unbounded: 2,777 bytes when every column of
    every surface is added and 5,677 when every one is retyped. The second is past ntfy's own
    4,096-byte limit, which ``NtfyTransport`` answers with a 400 and does not retry, so uncapped
    the page saying the most would be the page that never arrives.

    ``PAGE_COLUMN_CAP`` alone does not bound it, which is why both shapes are driven.
    ``_page_moved`` emits up to three clauses per surface, so nine capped lists can land in one
    body: measured at 1,547 bytes with every list inside the column cap. The all-retyped shape
    yields one clause per surface and passes on a body the column cap never had to bound, so a
    test driving it alone would hold nothing.
    """
    _record_shape(lake_root, shape())

    check = check_running_version(lake_root)

    assert check.state == CONFLICTING
    assert len(check.page_body.encode("utf-8")) <= PAGE_BODY_BYTE_CAP
    # The version leads the body and survives any cut, because the head is never what gives way.
    assert str(journal.SCHEMA_VERSION) in check.page_body
    # The uncapped rendering is still on the verdict, for the log the operator is sent to.
    assert len(check.detail.encode("utf-8")) > PAGE_BODY_BYTE_CAP


# -- what the page says moved ------------------------------------------------
#
# ``_page_moved`` is ``_conflict_detail``'s twin under the body budget, and the three cases
# below mirror the three that already hold the older one. A page reached only through the
# verdict is reached through one assertion that some column name appears in it, which a walk
# over the wrong side of the comparison still satisfies.


def test_the_page_names_a_surface_only_the_ledger_holds():
    """The walk is over the union, so a surface the running code dropped still gets named.

    Walking the derived side alone renders an empty string, and the page then says a version
    disagrees and names nothing at all.
    """
    derived = running_fingerprints()
    recorded = {**derived, "ghost_surface": {"who": "string"}}

    moved = _page_moved(derived, recorded)

    assert "ghost_surface" in moved
    assert "who" in moved


def test_the_page_says_which_way_a_column_moved():
    """The verb is the whole instruction. A column the running code has and the ledger does
    not is *added*, and calling it dropped sends an operator the opposite way.

    Swapping the two arguments is a one-word edit that renders the right column under the
    wrong verb, which is the shape a test asserting only that the name appears cannot see.
    """
    derived = running_fingerprints()
    recorded = {surface: dict(columns) for surface, columns in derived.items()}
    recorded[journal.CHAINS_SURFACE].pop("bid")

    assert "added bid" in _page_moved(derived, recorded)
    assert "dropped bid" in _page_moved(recorded, derived)


def test_the_page_stays_silent_about_a_surface_that_did_not_move():
    """One surface disagreeing names one surface. The other two are not mentioned."""
    derived = running_fingerprints()
    recorded = {surface: dict(columns) for surface, columns in derived.items()}
    recorded[journal.BARS_SURFACE].pop("volume")

    moved = _page_moved(derived, recorded)

    assert journal.BARS_SURFACE in moved
    assert journal.CHAINS_SURFACE not in moved
    assert journal.QUOTES_SURFACE not in moved


def test_the_page_names_the_cap_worth_of_columns_and_counts_the_rest(lake_root):
    """One surface's clause names exactly ``PAGE_COLUMN_CAP`` columns, then says how many are
    left.

    The count is what separates one moved column from a wholesale retype, and the number of
    names is what makes the page worth reading at all. A body bounded only by its byte cap
    would satisfy the budget while naming whatever happened to fit.
    """
    extra = PAGE_COLUMN_CAP + 5
    shapes = {surface: dict(columns) for surface, columns in running_fingerprints().items()}
    for index in range(extra):
        shapes[journal.CHAINS_SURFACE][f"gone_{index:02d}"] = "double"
    _record_shape(lake_root, shapes)

    body = check_running_version(lake_root).page_body

    assert f"gone_{PAGE_COLUMN_CAP - 1:02d}" in body
    assert f"gone_{PAGE_COLUMN_CAP:02d}" not in body
    assert f"and {extra - PAGE_COLUMN_CAP} more" in body


def test_neither_the_page_nor_the_report_line_names_an_absolute_path(lake_root):
    """``report.redacted`` exists to keep capture-machine paths out of a file the dashboard
    may read, and a phone cannot reach a local path either way. Both name the ledger by its
    lake-relative path instead, and the absolute one goes to stderr alone.
    """
    _record_shape(lake_root, _narrowed())

    check = check_running_version(lake_root)

    assert LEDGER_PARTITION in check.summary
    assert LEDGER_PARTITION in check.page_body
    assert str(lake_root) not in check.summary
    assert str(lake_root) not in check.page_body
    assert str(lake_root) in check.detail


@pytest.mark.parametrize(
    "build",
    [lambda root: None, lambda root: _record_shape(root, _narrowed()), _other_columns],
    ids=["unrecorded", "conflicting", "unreadable"],
)
def test_the_report_line_survives_redaction_whole(lake_root, build):
    """``digest_body`` passes every report line through ``report.redacted``, which drops
    everything past the second colon-separated field. A line composed as a place, then a
    verdict, then a version would reach the phone with the version gone, so each holds two.
    """
    build(ledger_path(lake_root) if build is _other_columns else lake_root)

    check = check_running_version(lake_root)

    assert not check.ok
    assert redacted(check.summary) == check.summary


def test_the_three_event_names_are_three_names():
    """Pinned as literals, because comparing the constants to themselves holds nothing.

    A set built from the three constants and compared against a set of the same three
    constants collapses on both sides the moment two of them are spelled alike, and passes.
    That is what this file's first draft asserted.
    """
    assert len({UNRECORDED_EVENT, CONFLICT_EVENT, UNREADABLE_EVENT}) == 3
    assert UNRECORDED_EVENT == "schema_version_unrecorded"
    assert CONFLICT_EVENT == "schema_version_conflict"
    assert UNREADABLE_EVENT == "schema_version_ledger_unreadable"


def test_each_paged_verdict_carries_an_event_of_its_own(lake_root):
    """``alert._record`` keeps no body, and drops the title too when a page was refused, so
    the event is the only field guaranteed to say which of the three went quiet.
    """
    events = set()
    for build in (lambda: None, lambda: _record_shape(lake_root, _narrowed())):
        ledger_path(lake_root).unlink(missing_ok=True)
        build()
        events.add(check_running_version(lake_root).event)
    ledger_path(lake_root).unlink(missing_ok=True)
    _other_columns(ledger_path(lake_root))
    events.add(check_running_version(lake_root).event)

    assert events == {UNRECORDED_EVENT, CONFLICT_EVENT, UNREADABLE_EVENT}
