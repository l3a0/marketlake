"""The footer rule ``lake.parquet_footer`` checks, against footer metadata built in memory.

A real footer whose counts disagree has to be damaged byte by byte, which the component
tests for compaction and the loader do. These build the metadata as plain objects instead,
so a case can place a list column and a damaged flat column in one row group without
writing a file. ``fake_footer`` builds them, and compaction's footer tests use it too.
"""

from __future__ import annotations

from types import SimpleNamespace

from lake.parquet_footer import CountDisagreement, count_disagreement


def fake_footer(
    groups: list[tuple[int, list[tuple[str, int]]]], leaves: list[tuple[str, int]]
) -> SimpleNamespace:
    """Footer metadata holding ``groups`` over the schema leaves ``leaves``.

    Each group is its ``num_rows`` and one ``(physical_type, num_values)`` per column
    chunk. Each leaf is its ``(physical_type, max_repetition_level)``. Column ``j`` is
    named ``c{j}``. Only the attributes ``count_disagreement`` and compaction's
    ``_footer_disagreement`` read are present.
    """
    schema = SimpleNamespace(
        column=lambda j: SimpleNamespace(
            physical_type=leaves[j][0], max_repetition_level=leaves[j][1]
        )
    )
    row_groups = [
        SimpleNamespace(
            num_rows=rows,
            num_columns=len(chunks),
            column=lambda j, chunks=chunks: SimpleNamespace(
                physical_type=chunks[j][0], num_values=chunks[j][1], path_in_schema=f"c{j}"
            ),
        )
        for rows, chunks in groups
    ]
    return SimpleNamespace(
        num_row_groups=len(groups),
        num_columns=len(leaves),
        schema=schema,
        row_group=lambda i: row_groups[i],
    )


def test_a_list_column_does_not_end_the_check_of_its_row_group():
    """A list column is skipped, and the flat column after it is still compared.

    The list's leaf holds 7 values for 3 rows, which is healthy for a list. The flat
    column after it declares 2 values for the same 3 rows. A check that stopped at the
    first list column would never reach it and would call the footer healthy.
    """
    metadata = fake_footer([(3, [("INT64", 7), ("INT64", 2)])], [("INT64", 1), ("INT64", 0)])

    found = count_disagreement(metadata, [0], [0, 1])

    assert found == CountDisagreement(group=0, leaf=1, rows=3, values=2)
