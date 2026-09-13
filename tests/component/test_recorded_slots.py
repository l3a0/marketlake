"""What ``recorded_slots`` counts as a present minute, across one real boundary: files.

The hole-aware startup walk subtracts these minutes from the ones a day owed, so a minute
wrongly counted present is a marker never written, and a segment wrongly counted absent is
a full session of markers written over a record that exists. Both directions matter, which
is why a file that will not say lands in ``unreadable`` rather than in either answer.
"""

from __future__ import annotations

import os
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
    # Not drift. The column is still ``string``, exactly the type the schema pins, so no
    # field went missing and none was retyped. The dashboard counts this same input under
    # ``unparseable_stamp_rows`` and asserts it is not a drifted row, and calling it drift
    # here would page on a vendor typo as though the payload had changed shape.
    assert found.unreadable[0].kind == journal.SEGMENT_UNPARSEABLE


def test_a_retyped_snap_ts_makes_the_segment_unreadable(tmp_path):
    """The retyped-column case the design's schema policy names, from the reader's side."""
    _segment(tmp_path, pa.schema([("snap_ts", pa.int64())]), [pa.array([1757000000])])

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset()
    assert len(found.unreadable) == 1, "a retyped column read as a present minute"
    # The case the design's schema policy pages on, and the one a rule keyed on the
    # exception type would misfile: an int64 ``snap_ts`` raises a plain ``TypeError`` from
    # ``datetime.fromisoformat``, never ``pa.ArrowTypeError``.
    assert found.unreadable[0].kind == journal.SEGMENT_DRIFTED


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
    assert found.unreadable[0].kind == journal.SEGMENT_UNPARSEABLE


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
    # The neighbour has no ``snap_ts`` column at all, a missing known field rather than a
    # retyped one. Both are drift, and the reader does not split them further.
    assert found.unreadable[0].kind == journal.SEGMENT_DRIFTED


def test_bytes_that_are_no_arrow_stream_read_as_corrupt_rather_than_drifted(tmp_path):
    """The kind turns on which stage failed, not on the exception's type.

    ``pa.ArrowInvalid`` subclasses ``ValueError``, and so does the failure a drifted string
    column raises when the parse refuses its value. One type, two meanings. Splitting the
    open from the column read is what tells them apart, and the design asks for a page on
    one of the two and not on the other.
    """
    directory = journal.segment_dir(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "20260902T100000-1.arrows").write_bytes(b"not an arrow stream at all")

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset()
    assert len(found.unreadable) == 1
    assert found.unreadable[0].kind == journal.SEGMENT_CORRUPT
    # The path half of the pair. Nothing prints it yet, so without this the field could be
    # filled with anything and the suite would not notice.
    assert found.unreadable[0].path == directory / "20260902T100000-1.arrows"


def test_a_shadow_appended_segment_keeps_its_own_kind(tmp_path):
    """The tamper signature the design pins never collapses into a generic count.

    Bytes past the end-of-stream marker mean a later writer opened a closed segment and
    appended rows standard readers never see. That is a different thing from a schema
    change and calls for a different response, so it carries its own kind rather than
    joining drift or corruption.
    """
    path = _segment(
        tmp_path,
        pa.schema([("snap_ts", pa.string())]),
        [pa.array(["2026-09-02T10:00:00-04:00"])],
    )
    with path.open("ab") as handle:
        handle.write(b"shadow bytes past the end of stream")

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset(), "rows behind a shadow append were counted as present"
    assert len(found.unreadable) == 1
    assert found.unreadable[0].kind == journal.SEGMENT_SHADOW_APPEND


def test_a_segment_that_vanishes_under_the_read_is_its_own_kind(tmp_path, monkeypatch):
    """A file listed and then gone is the seal landing mid-read, not damage.

    Compaction deletes a ticker-day's segments once it has sealed them, so a walk that
    sized a file a moment earlier can find it gone when it opens it. Nothing is wrong with
    the lake in that case, which is why it is counted apart from a file whose bytes will
    not open. Deleting between the size check and the open is that race exactly, and the
    real reader still raises the real ``FileNotFoundError``.
    """
    directory = journal.segment_dir(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "20260902T100000-1.arrows"
    path.write_bytes(b"sized non-zero, so the empty-file skip does not take it first")

    real_read = journal.read_segment

    def vanishing_read(target):
        os.unlink(target)
        return real_read(target)

    monkeypatch.setattr(journal, "read_segment", vanishing_read)

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset()
    assert len(found.unreadable) == 1
    # Not corrupt. The bytes were never the problem, and an operator told the file was
    # corrupt would go looking for damage that is not there.
    assert found.unreadable[0].kind == journal.SEGMENT_VANISHED


def test_the_breakout_names_every_kind_it_holds(tmp_path):
    """What a caller prints, which is the line an operator actually reads.

    The total keeps the wording the callers used before, so the change does not cost a
    reader the shape they know, and the breakout after it is the part that says whether to
    look at a disk or at the vendor's payload.
    """
    entries = (
        journal.UnusableSegment(tmp_path / "a.arrows", journal.SEGMENT_CORRUPT),
        journal.UnusableSegment(tmp_path / "b.arrows", journal.SEGMENT_DRIFTED),
        journal.UnusableSegment(tmp_path / "c.arrows", journal.SEGMENT_DRIFTED),
    )

    # Drift leads whatever order the entries arrived in, because it is the kind the design
    # pages on and a fixed order keeps the line stable across runs.
    assert journal.describe_unusable(entries) == "3 unreadable (2 drifted, 1 corrupt)"
    assert journal.describe_unusable(()) == ""


def test_the_breakout_orders_every_kind_the_same_way_whatever_failed_first(tmp_path):
    """Two lakes with the same damage read alike, whichever segment failed first.

    ``Counter`` keeps insertion order, so without a fixed order the leading kind would be
    whichever file the directory walk reached first. That makes two operators comparing the
    same damage read two different lines.
    """
    kinds = (
        journal.SEGMENT_VANISHED,
        journal.SEGMENT_CORRUPT,
        journal.SEGMENT_SHADOW_APPEND,
        journal.SEGMENT_UNPARSEABLE,
        journal.SEGMENT_DRIFTED,
    )
    entries = tuple(journal.UnusableSegment(tmp_path / f"{k}.arrows", k) for k in kinds)

    expected = "5 unreadable (1 drifted, 1 unparseable, 1 shadow_append, 1 corrupt, 1 vanished)"
    assert journal.describe_unusable(entries) == expected
    # Reversing the input changes nothing, which is the property the fixed order buys.
    assert journal.describe_unusable(tuple(reversed(entries))) == expected


def test_a_kind_outside_the_known_set_still_reaches_the_line(tmp_path):
    """The breakout's total and its parts always agree, even on a kind added later.

    Dropping an unrecognised kind would print a total larger than the parts under it, which
    reads as a counting bug to whoever is looking at it during an incident.
    """
    entries = (
        journal.UnusableSegment(tmp_path / "a.arrows", journal.SEGMENT_DRIFTED),
        journal.UnusableSegment(tmp_path / "b.arrows", "something_new"),
    )

    assert journal.describe_unusable(entries) == "2 unreadable (1 drifted, 1 something_new)"


# -- the twin reader --------------------------------------------------------------------
#
# ``close_tag_rows`` splits its stages the same way and for the same reason, and the two are
# written alike, which is exactly why each needs its own cases. Covering only
# ``recorded_slots`` would leave its twin free to mislabel every kind silently, and the
# close+5 guard reads that set to decide whether to withhold a marker.

CLOSE_TAG = "spot_close"


def _tagged_segment(root, name: str = "20260902T160000-1.arrows"):
    """A readable segment holding one row under the close tag the guard asks about."""
    schema = pa.schema([("close_tag", pa.string()), ("row_kind", pa.string())])
    return _segment(root, schema, [pa.array([CLOSE_TAG]), pa.array(["data"])], name=name)


def _close_rows(root):
    return journal.close_tag_rows(root, journal.QUOTES_SURFACE, "XYZ", DAY, CLOSE_TAG)


def test_close_tag_rows_names_bytes_that_will_not_open_as_corrupt(tmp_path):
    directory = journal.segment_dir(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "20260902T160000-1.arrows").write_bytes(b"not an arrow stream at all")

    found = _close_rows(tmp_path)

    assert (found.data, found.gaps) == (0, 0)
    assert [entry.kind for entry in found.unreadable] == [journal.SEGMENT_CORRUPT]


def test_close_tag_rows_keeps_a_shadow_append_apart(tmp_path):
    path = _tagged_segment(tmp_path)
    with path.open("ab") as handle:
        handle.write(b"shadow bytes past the end of stream")

    found = _close_rows(tmp_path)

    assert found.data == 0, "a row behind a shadow append was counted"
    assert [entry.kind for entry in found.unreadable] == [journal.SEGMENT_SHADOW_APPEND]


def test_close_tag_rows_names_a_vanished_segment_rather_than_a_damaged_one(tmp_path, monkeypatch):
    path = _tagged_segment(tmp_path)
    real_read = journal.read_segment

    def vanishing_read(target):
        os.unlink(target)
        return real_read(target)

    monkeypatch.setattr(journal, "read_segment", vanishing_read)

    found = _close_rows(tmp_path)

    assert path.exists() is False
    assert [entry.kind for entry in found.unreadable] == [journal.SEGMENT_VANISHED]


def test_close_tag_rows_names_a_lost_column_as_drift(tmp_path):
    # Valid Arrow holding none of the columns the guard asks for, which is what a schema
    # rotation leaves behind. It opens cleanly and raises on the column.
    _segment(tmp_path, pa.schema([("nothing_useful", pa.string())]), [pa.array(["x"])])

    found = _close_rows(tmp_path)

    assert [entry.kind for entry in found.unreadable] == [journal.SEGMENT_DRIFTED]


def test_close_tag_rows_still_counts_the_segments_it_can_read(tmp_path):
    """One bad file costs the guard its own rows and not the rows beside it.

    The guard reads these counts to decide whether a close was ever observed, so a readable
    segment lost to its neighbour would have it claim a close was missed that was not.
    """
    _tagged_segment(tmp_path, name="20260902T160000-1.arrows")
    _segment(
        tmp_path,
        pa.schema([("nothing_useful", pa.string())]),
        [pa.array(["x"])],
        name="20260902T160100-1.arrows",
    )

    found = _close_rows(tmp_path)

    assert found.data == 1, "the good segment's row was lost to its neighbour"
    assert [entry.kind for entry in found.unreadable] == [journal.SEGMENT_DRIFTED]


def test_a_zero_byte_segment_reads_as_absent_rather_than_unreadable(tmp_path):
    """Created and never written to is absent, not damaged, and the difference is a day.

    ``SegmentWriter`` opens with ``O_CREAT|O_EXCL`` and fsyncs the directory entry before
    the first schema bytes, so a process killed in between leaves one of these durably
    behind, and a ``KeepAlive`` crash loop makes them in quantity. Counting one as corrupt
    would have the startup walk refuse the whole ticker-day and mark nothing, which is a
    full session of gap marking lost to a file holding no batches at all.

    ``close_tag_rows`` has had this case since before the kinds existed. Its twin had none,
    so the skip could be deleted here alone with the suite green.
    """
    directory = journal.segment_dir(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "20260902T100000-1.arrows").touch()

    found = journal.recorded_slots(tmp_path, journal.QUOTES_SURFACE, "XYZ", DAY)

    assert found.slots == frozenset()
    assert found.unreadable == (), "an empty file was reported as damage"


def test_the_kind_names_are_the_panel_s_own_counter_names():
    """The alignment the whole scheme rests on, which nothing connected.

    These names are reused from ``dashboard.SegmentHealth`` so the panel and the readers
    under it name one failure one way. Nothing linked the two sides, so either could be
    renamed and the alignment would break in silence, which is the drift this reader exists
    to stop, spelled with identifiers instead of data.

    ``unparseable`` is deliberately absent from the four. The panel draws that line among
    rows rather than segments, under ``unparseable_stamp_rows``, so it has no segment-level
    counter to match and is this reader's own.
    """
    from dataclasses import fields

    from lake.dashboard import SegmentHealth

    counters = {field.name for field in fields(SegmentHealth)}
    assert {
        journal.SEGMENT_CORRUPT,
        journal.SEGMENT_VANISHED,
        journal.SEGMENT_SHADOW_APPEND,
        journal.SEGMENT_DRIFTED,
    } <= counters
    assert journal.SEGMENT_UNPARSEABLE not in counters
    assert "unparseable_stamp_rows" in counters
