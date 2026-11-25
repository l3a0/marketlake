"""How a ticker-day's segments join by name into one table.

Three callers join by name: the close+15 seal (``lake.compact``), the door onto an unsealed day
(``lake.unsealed``) and the day-one measurements (``lake.measure``). Two joins can disagree, and
then a row read before the seal is not the row sealed. So the join is these two pure functions,
and each of the three takes it from here. The module imports pyarrow alone.

Two reads of a ticker-day's segments join another way on purpose. The seal's authorized-retype
repair (``compact._merge_authorized``) widens a column a human said may be widened, and the
dashboard projects each segment into one fixed schema first.

**A change here changes what the seal writes.** The seal unlinks the segments once it has written
this result, with only a row count and a footer check after it, and captured data cannot be
recaptured. So an edit made for a reader is reviewed as an edit to that path.

**A change here is also a change to a published contract.** A consumer pins this package at one
commit and reads unsealed rows through ``lake.unsealed``. Its rows equal the seal's only while
this module matches the deployed one, so a change has to reach a consumer's pin as well as the
capture host, and ``README.md``'s ``## Read an unsealed day`` says so.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa


def merge_by_name(tables: Sequence[pa.Table]) -> pa.Table:
    """The tables joined by column name, rows in the order given.

    A column one table lacks reads as nulls on that table's rows. A column two tables hold at
    two types raises ``pa.ArrowTypeError`` rather than widening, because the schema policy
    wants a type that moved inside one day to fail loudly. A null-typed column takes the other
    table's type, since it holds no value to change.
    """
    return pa.concat_tables(tables, promote_options="default")


def pinned_order(table: pa.Table, pinned: pa.Schema) -> pa.Table:
    """The merged table with its pinned columns back in the pinned schema's order.

    Promotion appends. A segment written before a column was added has no place to put
    it, so ``merge_by_name`` adds the promoted column at the end of the merged table
    rather than where the pinned schema holds it. The columns are then right and the
    order is not, and a comparison that read order as drift would file a finding on every
    legitimate column addition. Reordering first is what makes the seal's comparison a
    plain equality.

    No read inside the lake depends on the order. Every read in ``src/`` is by name, and the
    documented multi-day path is DuckDB's ``union_by_name``. ``lake.unsealed`` returns this
    order to its callers, with any column its projection adds behind it.

    A column the pinned schema does not name keeps its place at the end rather than being
    dropped, so the reorder moves columns and never loses one.
    """
    present = set(table.schema.names)
    named = set(pinned.names)
    order = [name for name in pinned.names if name in present]
    order += [name for name in table.schema.names if name not in named]
    if order == table.schema.names:
        return table
    return table.select(order)


__all__ = ["merge_by_name", "pinned_order"]
