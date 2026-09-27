"""The one read every reference table shares, and the rule it enforces.

Three reference tables live under ``reference/``: the security master, the capture spans
and the schema-version ledger. Each has its own module, its own pinned schema and its own
error classes, and each used to read its file the same narrow way: ``pq.read_table``, fold
``ArrowInvalid`` into the module's unreadable class, then build. Every other damaged file
got past that fold (marketlake #551, #396). pyarrow reads a directory as a table with no
rows and no columns, so a directory at the spans path answered "no ticker is in scope" and
the daemon captured nothing, silently. A readable parquet in some other schema raised a bare
``KeyError`` from the build, which no caller guards, so the daemon died at its first cycle
and relaunched into the same failure. A bit flip in a real file raised ``KeyError``,
``ArrowNotImplementedError`` or ``OverflowError`` the same way.

The rule is one sentence. A reference reader turns every damaged file into its own
unreadable error, and lets only an access failure through as ``OSError``. It lives in one
function with three callers rather than in three copies, because a copy can keep one check
and drop another silently, the argument ``actions.read_master`` already makes for itself.

It imports only pyarrow and the standard library, so none of the three modules gains an
import cycle. ``lake.paths`` is not its home, because that module answers where a file lives
and nothing else. ``lake.reference_read`` is not either, because that is the daemon's
print-once line and runs above these readers.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# The column every reference table stamps on every row with the table's own version.
VERSION_COLUMN = "schema_version"


def read_reference_table[T](
    path: Path,
    *,
    schema: pa.Schema,
    version: int,
    build: Callable[[pa.Table], T],
    unreadable: Callable[[Path, str], BaseException],
    base: type[BaseException],
    unsupported: Callable[[object], BaseException],
) -> T:
    """Read the reference table at ``path`` and build it, or refuse it with the module's class.

    ``schema`` and ``version`` are the table's pinned ones. ``build`` is its ``from_table``.
    ``unreadable`` is its unreadable class, called with the path and a reason. ``base`` is
    the module's base error class, and ``unsupported`` its class for a newer table version.

    The steps run in this order, and each order is deliberate.

    1. **Stat the path.** ``FileNotFoundError`` and ``PermissionError`` propagate untouched,
       because an absent file is a fresh lake and a refused one is intact. A path that
       exists and is not a regular file is unreadable. That covers a directory, which
       pyarrow would read as an empty table, and a FIFO, which it would report as absent.
       The check is a stat rather than an open, because Python's ``open`` on a FIFO blocks
       for ever.
    2. **Read the table.**
    3. **Require an integer version column**, or the file is unreadable. A foreign parquet
       stops here with any number of rows, including none. The type is checked here, and
       only as far as "an integer", so a foreign ``schema_version`` of strings is not
       reported as a newer table while a newer file of some other integer width still is.
    4. **Check the version**, and raise ``unsupported`` on a mismatch. It runs before the
       check on the other columns because that class means a file from newer code, which
       is a different answer from a damaged one, and a newer version may add or change
       columns.
    5. **Require every pinned column at its pinned type**, or the file is unreadable. Extra
       columns are allowed. Exact schema equality would refuse every live master the day
       marketlake #96 drops ``capture_start`` from its pinned schema, until something
       rewrote the file.
    6. **Build.**

    Steps 2 to 6 share one broad fold. Anything they raise that is neither an ``OSError``
    nor ``base`` becomes ``unreadable``, chained with ``from``. The fold is broad because
    every list of classes so far has missed members: ``ArrowNotImplementedError`` is a
    ``NotImplementedError`` and ``OverflowError`` an ``ArithmeticError``, and neither is the
    ``ValueError`` the old fold caught. The price is that a programming error in ``build``
    reads as a damaged file. The daemon then widens and prints the chained class, which beats
    a crash loop.

    ``OSError`` stays out of the fold. pyarrow reports most corruption as a bare ``OSError``,
    262 of 400 byte flips in a real ledger, and the ledger check splits ``PermissionError``
    off it on purpose (marketlake #536). Callers already guard ``OSError`` beside the
    module's own class.
    """
    mode = os.stat(path).st_mode
    if not stat.S_ISREG(mode):
        kind = "a directory" if stat.S_ISDIR(mode) else "not a regular file"
        raise unreadable(path, f"is {kind}")
    try:
        table = pq.read_table(path)
        index = table.schema.get_field_index(VERSION_COLUMN)
        if index < 0 or not pa.types.is_integer(table.schema.field(index).type):
            raise unreadable(path, f"is not the pinned schema, it has no integer {VERSION_COLUMN}")
        for found in table.column(VERSION_COLUMN).unique().to_pylist():
            if found != version:
                raise unsupported(found)
        for name in schema.names:
            _require_column(table.schema, schema, name, path, unreadable)
        return build(table)
    except (OSError, base):
        raise
    except Exception as exc:
        raise unreadable(path, f"is not readable parquet, {type(exc).__name__}: {exc}") from exc


def _require_column(
    found: pa.Schema,
    pinned: pa.Schema,
    name: str,
    path: Path,
    unreadable: Callable[[Path, str], BaseException],
) -> None:
    """Raise ``unreadable`` unless ``found`` carries ``name`` at its pinned type."""
    expected = pinned.field(name).type
    index = found.get_field_index(name)
    if index < 0:
        raise unreadable(path, f"is not the pinned schema, it has no {name} column")
    actual = found.field(index).type
    if not actual.equals(expected):
        raise unreadable(
            path, f"is not the pinned schema, its {name} column is {actual}, not {expected}"
        )
