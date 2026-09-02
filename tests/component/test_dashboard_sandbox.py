"""The DuckDB sandbox, across the real filesystem.

These open the dashboard's connection over a throwaway lake and try to break out of it.
The one real boundary is the filesystem DuckDB reads, so the tier is component. Every
case pins one clause of the design's sandbox: reads outside ``lake_root`` fail, reads
inside work, and once the configuration is locked nothing can widen it.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from lake.dashboard import open_lake_connection


@pytest.fixture
def outside(tmp_path: Path) -> Path:
    """A file beside the lake root, standing in for the token and the secrets."""
    path = tmp_path / "outside.txt"
    path.write_text("secret\n")
    return path


def test_reading_a_file_outside_lake_root_fails(lake_root: Path, outside: Path):
    con = open_lake_connection(lake_root)
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_text(?)", [str(outside)]).fetchall()
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_csv(?)", [str(outside)]).fetchall()
    # A relative escape through the lake root is canonicalized and refused too.
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_text(?)", [str(lake_root / ".." / outside.name)]).fetchall()


def test_external_access_cannot_be_re_enabled_after_the_lock(lake_root: Path):
    con = open_lake_connection(lake_root)
    for statement in (
        "SET enable_external_access = true",
        "SET allowed_directories = ['/']",
        "SET lock_configuration = false",
    ):
        with pytest.raises(duckdb.Error):
            con.execute(statement)


def test_a_query_inside_lake_root_works(fixture_lake):
    root = fixture_lake.with_chains("SPY", "2026-08-24").build()
    partition = fixture_lake.partition_path("chains", "SPY", "2026-08-24")
    con = open_lake_connection(root)
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(partition)]).fetchone()
    assert rows == (1,)


def test_allowed_directories_is_exactly_the_lake_root(lake_root: Path):
    con = open_lake_connection(lake_root)
    allowed, external, locked = con.execute(
        "SELECT current_setting('allowed_directories'), "
        "current_setting('enable_external_access'), "
        "current_setting('lock_configuration')"
    ).fetchone()
    assert external is False
    assert locked is True
    # One entry, the lake root. DuckDB would add its own spill directory by default,
    # so the count is the assertion, not just the membership.
    assert len(allowed) == 1
    assert Path(allowed[0]).resolve() == lake_root.resolve()


def test_a_cursor_inherits_the_locked_sandbox(lake_root: Path, outside: Path):
    # The service runs each request on a cursor. The settings are database-wide, so the
    # cursor can neither read outside nor unlock.
    cursor = open_lake_connection(lake_root).cursor()
    with pytest.raises(duckdb.Error):
        cursor.execute("SET enable_external_access = true")
    with pytest.raises(duckdb.Error):
        cursor.execute("SELECT * FROM read_text(?)", [str(outside)]).fetchall()


def test_a_symlink_inside_the_lake_cannot_escape(lake_root: Path, outside: Path):
    link = lake_root / "escape.txt"
    link.symlink_to(outside)
    con = open_lake_connection(lake_root)
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_text(?)", [str(link)]).fetchall()


def test_extension_install_and_attach_are_refused(lake_root: Path, tmp_path: Path):
    con = open_lake_connection(lake_root)
    with pytest.raises(duckdb.Error):
        con.execute("INSTALL httpfs")
    with pytest.raises(duckdb.Error):
        con.execute("ATTACH ?", [str(tmp_path / "other.duckdb")])
