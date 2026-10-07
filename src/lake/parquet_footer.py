"""One rule a Parquet footer must keep for a batched read of it to be trusted.

The footer records a count twice for each row group: the group's ``num_rows``, and each
column chunk's ``num_values``. For a column that is not a list, every row holds one value,
null or not, so the two are equal on a healthy file. A list column holds as many values as
its lists have elements, so it is skipped.

A batched read, ``ParquetFile.iter_batches``, trusts both counts. With a chunk's
``num_values`` lowered it stops at the shortest column, and with a group's ``num_rows``
lowered it stops the group short, and it raises nothing either way. ``pq.read_table``
refuses or reads past the same damage.

Two callers check the rule, and neither imports the other, which is why it lives here.

1. Compaction's seal refuses a partition whose footer breaks it, over every row group and
   every column, before the segments it was built from are deleted.
2. The loader's filtered read refuses a read whose kept row groups break it, over the
   columns the read decodes, so damage after the seal cannot return a short table.

Only footer metadata is read. A chunk's statistics are never read here, because reading
them from a chunk whose physical type disagrees with the schema aborts the process.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import NamedTuple

import pyarrow.parquet as pq


class CountDisagreement(NamedTuple):
    """A row group and a column whose two counts differ in the footer."""

    group: int
    leaf: int
    rows: int
    values: int


def count_disagreement(
    metadata: pq.FileMetaData, groups: Iterable[int], leaves: Sequence[int]
) -> CountDisagreement | None:
    """The first ``(group, leaf)`` whose value count differs from its group's row count.

    ``groups`` are row-group indexes and ``leaves`` column indexes into
    ``metadata.schema``, both checked in the order given. A leaf with repetition is a list
    column and is skipped. The caller names the column in its own message, because the two
    callers name it from different places.
    """
    for group in groups:
        held = metadata.row_group(group)
        for leaf in leaves:
            if metadata.schema.column(leaf).max_repetition_level:
                continue
            values = held.column(leaf).num_values
            if values != held.num_rows:
                return CountDisagreement(group, leaf, held.num_rows, values)
    return None
