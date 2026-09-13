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
    # A string column holding a value the parse refuses is drift, not a damaged file. The
    # bytes opened cleanly and the column answered the wrong thing.
    assert found.unreadable[0].kind == journal.SEGMENT_DRIFTED


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
    assert found.unreadable[0].kind == journal.SEGMENT_DRIFTED


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
