"""What ``recorded_slots`` counts as a present minute, across one real boundary: files.

The hole-aware startup walk subtracts these minutes from the ones a day owed, so a minute
wrongly counted present is a marker never written, and a segment wrongly counted absent is
a full session of markers written over a record that exists. Both directions matter, which
is why a file that will not say lands in ``unreadable`` rather than in either answer.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa

from lake import journal

DAY = date(2026, 9, 2)


def _segment(root, schema: pa.Schema, columns: list, name: str = "20260902T100000-1.arrows"):
    directory = journal.segment_dir(root, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    with pa.ipc.new_stream(directory / name, schema) as writer:
        writer.write_batch(pa.record_batch(columns, schema=schema))
    return directory / name


def test_a_snap_ts_the_parse_refuses_makes_the_segment_unreadable(tmp_path):
    """A value that is the right type and not a timestamp is damage, not a minute."""
    _segment(
        tmp_path,
        pa.schema([("snap_ts", pa.string())]),
        [pa.array(["not a timestamp at all"])],
    )

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset(), "an unparseable value was counted as a present minute"
    assert len(found.unreadable) == 1, "the segment was not reported unreadable"


def test_a_retyped_snap_ts_makes_the_segment_unreadable(tmp_path):
    """The retyped-column case the design's schema policy names, from the reader's side."""
    _segment(tmp_path, pa.schema([("snap_ts", pa.int64())]), [pa.array([1757000000])])

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset()
    assert len(found.unreadable) == 1, "a retyped column read as a present minute"


def test_a_segment_that_fails_partway_contributes_none_of_its_minutes(tmp_path):
    """All of a segment's minutes or none, never the prefix that happened to parse.

    A kept prefix would be the worst of both: the day looks partly recorded, so the walk
    marks fewer holes than it owes, and the segment is also flagged unreadable, so the two
    answers disagree about the same file. Building the whole list before keeping any of it
    is what makes the reader answer once.
    """
    _segment(
        tmp_path,
        pa.schema([("snap_ts", pa.string())]),
        [pa.array(["2026-09-02T10:00:00-04:00", "not a timestamp at all"])],
    )

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset(), "the prefix before the bad value was kept"
    assert len(found.unreadable) == 1


def test_a_readable_segment_beside_an_unreadable_one_still_counts(tmp_path):
    """One bad file does not cost the good files in the same directory their minutes."""
    _segment(
        tmp_path,
        pa.schema([("snap_ts", pa.string())]),
        [pa.array(["2026-09-02T10:00:00-04:00"])],
        name="20260902T100000-1.arrows",
    )
    _segment(
        tmp_path,
        pa.schema([("nothing_useful", pa.string())]),
        [pa.array(["x"])],
        name="20260902T100100-1.arrows",
    )

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert len(found.slots) == 1, "the good segment's minute was lost to its neighbour"
    assert len(found.unreadable) == 1
