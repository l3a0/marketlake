"""The DuckDB sandbox, across the real filesystem.

These open the dashboard's connection over a throwaway lake and try to break out of it.
The one real boundary is the filesystem DuckDB reads, so the tier is component. Every
case checks one clause of the design's sandbox: reads outside ``lake_root`` fail, reads
inside work, the two resource caps are applied, and once the configuration is locked
nothing can widen any of it.

The caps are part of the sandbox, not a separate concern. The service shares a laptop
with the minutely capture daemon, so the connection takes a small fixed share rather
than the machine default of every core and most of RAM. Both caps must land before the
lock, because a locked configuration refuses every later ``SET``.

Every case here must distinguish a sandboxed connection from an unsandboxed one. The
standard is a mutation: replace ``open_lake_connection`` with a bare ``duckdb.connect()``
and every test in this file must fail except ``test_a_query_inside_lake_root_works``,
the positive control. Two idioms defeat that standard and are banned here.

1. ``SELECT *`` over ``read_text``. That function returns ``last_modified`` as a
   ``TIMESTAMP WITH TIME ZONE``, and converting that column to Python needs ``pytz``,
   which this environment does not install. So the star projection raises whether or not
   the sandbox is on. Project ``content`` instead.
2. A bare ``duckdb.Error``, or a statement that any running DuckDB refuses for its own
   reasons. ``SET enable_external_access = true`` is the example: an unlocked connection
   refuses it too, because the database is already running. Match the message the lock
   itself produces.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from lake.dashboard import QUERY_MEMORY_LIMIT, QUERY_THREADS, open_lake_connection

# The read that proves a path is reachable. ``content`` is projected on purpose. The
# star projection would raise on any connection, sandboxed or not, so an assertion built
# on it proves nothing about the sandbox. See the module docstring.
_READ_TEXT = "SELECT content FROM read_text(?)"

# The fragment DuckDB puts in every refusal that comes from ``lock_configuration``. An
# unlocked connection refuses some of these statements too, for its own reasons and with
# its own wording, so the fragment is what separates the lock from the rest.
_LOCKED = "the configuration has been locked"

# The settings the sandbox pins, read back in one statement.
_SANDBOX_SQL = (
    "SELECT current_setting('allowed_directories'), "
    "current_setting('enable_external_access'), "
    "current_setting('lock_configuration')"
)

_CAPS_SQL = "SELECT current_setting('threads'), current_setting('memory_limit')"


def _rendered_memory_limit(setting: str) -> str:
    """DuckDB's own rendering of a memory limit, as ``current_setting`` reports it.

    DuckDB stores ``memory_limit`` as bytes and renders it back in binary units at one
    decimal place, so the configured ``2GB`` reads as ``1.8 GiB``. Comparing byte counts
    across the two spellings needs a tolerance, and a tolerance wide enough to absorb
    that rounding also accepts a drift away from the configured value. So the comparison
    runs in DuckDB's own spelling instead: an unsandboxed connection renders the
    constant, and the sandboxed one must render exactly the same string.
    """
    with duckdb.connect() as con:
        con.execute("SET memory_limit = ?", [setting])
        return con.execute("SELECT current_setting('memory_limit')").fetchone()[0]


@pytest.fixture
def outside(tmp_path: Path) -> Path:
    """A file beside the lake root, standing in for the token and the secrets."""
    path = tmp_path / "outside.txt"
    path.write_text("secret\n")
    return path


def test_reading_a_file_outside_lake_root_fails(lake_root: Path, outside: Path):
    con = open_lake_connection(lake_root)
    with pytest.raises(duckdb.PermissionException):
        con.execute(_READ_TEXT, [str(outside)]).fetchall()
    with pytest.raises(duckdb.PermissionException):
        con.execute("SELECT * FROM read_csv(?)", [str(outside)]).fetchall()
    # A relative escape through the lake root is canonicalized and refused too.
    with pytest.raises(duckdb.PermissionException):
        con.execute(_READ_TEXT, [str(lake_root / ".." / outside.name)]).fetchall()


def test_external_access_cannot_be_re_enabled_after_the_lock(lake_root: Path):
    con = open_lake_connection(lake_root)
    for statement in (
        "SET enable_external_access = true",
        "SET allowed_directories = ['/']",
        "SET lock_configuration = false",
    ):
        # The message is matched, not just the type. An unlocked connection refuses to
        # enable external access as well, saying the database is already running, so a
        # bare ``duckdb.Error`` on that statement would pass with no sandbox at all.
        with pytest.raises(duckdb.InvalidInputException, match=_LOCKED):
            con.execute(statement)


def test_a_query_inside_lake_root_works(fixture_lake):
    root = fixture_lake.with_chains("SPY", "2026-08-24").build()
    partition = fixture_lake.partition_path("chains", "SPY", "2026-08-24")
    con = open_lake_connection(root)
    rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(partition)]).fetchone()
    assert rows == (1,)


def test_allowed_directories_is_exactly_the_lake_root(lake_root: Path):
    con = open_lake_connection(lake_root)
    allowed, external, locked = con.execute(_SANDBOX_SQL).fetchone()
    assert external is False
    assert locked is True
    # One entry, the lake root. DuckDB would add its own spill directory by default,
    # so the count is the assertion, not just the membership.
    assert len(allowed) == 1
    assert Path(allowed[0]).resolve() == lake_root.resolve()


def test_a_cursor_inherits_the_locked_sandbox(lake_root: Path, outside: Path):
    # The service runs each request on a cursor. The settings are database-wide, so the
    # cursor sees the same allow-list, cannot widen it, and cannot read past it. Each of
    # the three is something an unsandboxed cursor does the other way: its allow-list is
    # empty, ``SET allowed_directories`` succeeds on it, and the outside read returns the
    # file's bytes.
    cursor = open_lake_connection(lake_root).cursor()
    allowed, external, locked = cursor.execute(_SANDBOX_SQL).fetchone()
    assert [Path(entry).resolve() for entry in allowed] == [lake_root.resolve()]
    assert external is False
    assert locked is True
    with pytest.raises(duckdb.InvalidInputException, match=_LOCKED):
        cursor.execute("SET allowed_directories = ['/']")
    with pytest.raises(duckdb.PermissionException):
        cursor.execute(_READ_TEXT, [str(outside)]).fetchall()


def test_a_symlink_inside_the_lake_cannot_escape(lake_root: Path, outside: Path):
    # DuckDB canonicalizes every path it opens, so the link resolves to its target and
    # the target is outside the allow-list. Without the sandbox the same statement hands
    # back the target's bytes, which is why the decoy carries content worth refusing.
    link = lake_root / "escape.txt"
    link.symlink_to(outside)
    inside = lake_root / "inside.txt"
    inside.write_text("a file the sandbox may read\n")
    con = open_lake_connection(lake_root)
    # The same statement over a real file inside the root, so the refusal below is the
    # allow-list talking and not the read itself failing.
    assert con.execute(_READ_TEXT, [str(inside)]).fetchall() == [(inside.read_text(),)]
    with pytest.raises(duckdb.PermissionException):
        con.execute(_READ_TEXT, [str(link)]).fetchall()


def test_extension_install_and_attach_are_refused(lake_root: Path, tmp_path: Path):
    con = open_lake_connection(lake_root)
    # The extension directory lives under the owner's home, outside the allow-list.
    with pytest.raises(duckdb.PermissionException):
        con.execute("INSTALL httpfs")
    # DuckDB accepts no bind parameter for ``ATTACH``, so a parameterized one is a parser
    # error on any connection and proves nothing. The path is spliced instead. It is the
    # test's own temp path, never a client value.
    outside_db = tmp_path / "other.duckdb"
    with pytest.raises(duckdb.PermissionException):
        con.execute(f"ATTACH '{outside_db}'")
    # A typed attach is refused a step earlier: the type needs an extension load, and
    # external access is off.
    with pytest.raises(duckdb.PermissionException, match="external extensions is disabled"):
        con.execute(f"ATTACH '{tmp_path / 'other.sqlite'}' (TYPE SQLITE)")


def test_the_connection_caps_threads_and_memory(lake_root: Path):
    con = open_lake_connection(lake_root)
    threads, memory = con.execute(_CAPS_SQL).fetchone()
    assert threads == QUERY_THREADS
    assert memory == _rendered_memory_limit(QUERY_MEMORY_LIMIT)


def test_a_cursor_inherits_both_caps(lake_root: Path):
    # The service runs each request on a cursor, so the caps must not stop at the parent.
    cursor = open_lake_connection(lake_root).cursor()
    threads, memory = cursor.execute(_CAPS_SQL).fetchone()
    assert threads == QUERY_THREADS
    assert memory == _rendered_memory_limit(QUERY_MEMORY_LIMIT)


@pytest.mark.parametrize("statement", ["SET threads = 16", "SET memory_limit = '64GB'"])
def test_neither_cap_can_be_raised_after_the_lock(lake_root: Path, statement: str):
    con = open_lake_connection(lake_root)
    with pytest.raises(duckdb.InvalidInputException, match=_LOCKED):
        con.execute(statement)
    with pytest.raises(duckdb.InvalidInputException, match=_LOCKED):
        con.cursor().execute(statement)
    threads, memory = con.execute(_CAPS_SQL).fetchone()
    assert threads == QUERY_THREADS
    assert memory == _rendered_memory_limit(QUERY_MEMORY_LIMIT)
