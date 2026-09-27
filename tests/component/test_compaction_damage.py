"""Compaction's check of each manifested segment against the hash taken when it closed.

Every capture segment gets a manifest entry right after it closes, and the entry carries
the sha256 of the file's exact bytes. Compaction used to delete segments on the strength
of a read it never checked against that digest. One flipped bit could make the segment
read as a torn tail with no rows, or decode cleanly with one value changed, and either way
the ticker-day sealed short or wrong, the segment was unlinked, and the run pinged healthy.

The segments here come through ``capture.journal_snapshot`` from the ``spy_minimal``
cassette, one per minute, which is how the capture loop writes them, so each one carries a
real manifest entry. No older compaction test builds a segment that way, which is why the
healthy side is covered here as carefully as the damaged one. A check that compared the
wrong field or hashed the wrong file would refuse every ticker-day every night, and without
the healthy side it would pass the suite.

These cover the check's contract:

1. The healthy side. Manifested, intact segments seal exactly as they did before the check,
   beside an unmanifested marker segment the check has to skip.
2. A flipped byte in a manifested segment refuses that ticker-day. Its segments stay
   byte-identical, another ticker-day on the same date seals, the backup and the ping run,
   one page names the segment, and a finding under ``reports/damaged_segments/`` names both
   digests. That holds for the flip that used to seal the day short, the one that used to
   seal a changed value, and a shadow-append.
3. The refusal joins ``refused`` with its own reason, so the re-tune leaves the day alone and
   the summary words it as damage rather than a type conflict.
4. An unmanifested segment has no digest to compare, so the read judges it. A flip the
   read proves is damage refuses the ticker-day the same way, with the read's failure in
   place of the recorded digest (marketlake #552). A flip that decodes cleanly with a
   changed value still seals, because no structural check can see it.
5. The refusal repeats every night the damage survives, a finding that cannot be written
   still pages, and a run with drift and damage sends both pages under their own titles.
6. The page stays inside the design's body limit however many segments are damaged.
7. The human-invoked repair raises the refusal rather than sealing past it.
8. The writer itself: where the file lands, what it holds, and that it never overwrites.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import capture, journal, report
from lake import compact as compact_module
from lake.alert import Publisher
from lake.calendar import MARKET_TZ
from lake.cassette import load_cassette
from lake.compact import (
    DAMAGED_SEGMENT_EVENT,
    DAMAGED_SEGMENT_TITLE,
    PAGE_SEGMENT_CAP,
    REFUSED_SEGMENT_DAMAGED,
    SCHEMA_DRIFT_TITLE,
    CompactionResult,
    DamagedSegments,
    compact,
    recompact_ticker_day,
)
from lake.manifest import latest_entries, sha256_file
from lake.paths import LakePaths
from lake.report import DamagedSegment, SegmentDamage
from tests.conftest import CASSETTES
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

DAY = date(2026, 8, 24)
URL = "https://hc-ping.com/secret-key/compaction"
TARGET = Path("/ssd/lake")
PID = 7

_RECORDED = load_cassette(CASSETTES / "spy_minimal.json")
_CHAIN = _RECORDED.find("chains", {"symbol": "SPY"}).body

# What one ``spy_minimal`` chains segment measures. The size is asserted, so a cassette that
# changes shape fails loudly rather than moving a flip somewhere it no longer does what its
# name says.
SEGMENT_BYTES = 9736
ROWS_PER_SEGMENT = 2
# Read as a stream that fails, so the segment contributed nothing and the day sealed 8 of
# its 10 rows. The shape 896 of 2,000 flips took when marketlake #556 measured it. Found by
# flipping bit 0 at every 24th byte with no check in front of the seal.
DROPS_THE_SEGMENT = 4632
# Decodes cleanly with one value changed, so the day sealed all 10 rows and one was wrong.
# No structural check can see it. Only the digest does. The flip is bit 0 of the first
# character of the first stored stamp, located by content, so "2026" becomes "3026".
CHANGES_A_VALUE = b"2026-08-24T10:"
# One gap marker segment, the size the close guard's and the startup walk's markers write,
# and the last byte of its one batch body. Flipping it breaks the last column's buffers,
# which only full validation sees, and leaves the end-of-stream marker after it intact.
MARKER_BYTES = 9088
MARKER_FLIP = 9079


# -- the seams ---------------------------------------------------------------


def _et(hour: int, minute: int) -> datetime:
    return datetime.combine(DAY, time(hour, minute), tzinfo=MARKET_TZ)


def _calendar() -> FakeCalendar:
    return FakeCalendar({DAY: SessionTimes(open=_et(9, 30), close=_et(16, 0))})


def _paging(lake_root: Path) -> tuple[Publisher, FakeTransport]:
    transport = FakeTransport()
    publisher = Publisher(lake_root=lake_root, transport=transport, secrets=(PING_KEY, NTFY_TOPIC))
    return publisher, transport


def _run(
    lake_root: Path, *, publisher=None, at: datetime | None = None
) -> tuple[CompactionResult, list[str]]:
    events: list[str] = []
    result = compact(
        lake_root,
        clock=ManualClock(at if at is not None else _et(16, 30)),
        calendar=_calendar(),
        backup=FakeBackup(events),
        backup_target=TARGET,
        pinger=FakePinger(events),
        ping_url=URL,
        publisher=publisher,
        plan_path=lake_root.parent / "chain_plan.json",
    )
    return result, events


def _captured(lake_root: Path, ticker: str, *, count: int = 5) -> list[Path]:
    """``count`` chains segments for one ticker, one per minute, each with its manifest entry."""
    paths = []
    for minute in range(count):
        at = _et(10, minute)
        outcome = capture.journal_snapshot(
            lake_root,
            journal.CHAINS_SURFACE,
            ticker,
            body=_CHAIN,
            cycle_start=at,
            fetch_ts=at,
            fetch_end_ts=at,
            pid=PID,
        )
        paths.append(outcome.path)
    return paths


def _flip(path: Path, offset: int | bytes) -> None:
    """Flip bit 0 of one byte, the smallest damage a disk can do.

    ``offset`` is a byte position, or bytes whose first stored occurrence names the byte.
    """
    assert path.stat().st_size == SEGMENT_BYTES
    data = bytearray(path.read_bytes())
    if isinstance(offset, bytes):
        offset = data.index(offset)
    data[offset] ^= 0x01
    path.write_bytes(bytes(data))


def _rel(lake_root: Path, path: Path) -> str:
    return path.relative_to(lake_root).as_posix()


def _partition(lake_root: Path, ticker: str) -> Path:
    return LakePaths(lake_root).chains_partition_path(ticker, DAY)


def _findings(lake_root: Path) -> list[dict]:
    directory = report.damaged_segments_dir(lake_root, DAY)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


def _segments_table(paths: list[Path]) -> pa.Table:
    """The segments' rows, copied out of the files they were read from.

    ``journal.read_segment`` memory-maps the segment, so a table it returns can share pages
    with the file. On Linux a test that then rewrites the file in place changes that table
    too, and a before-and-after comparison sees the damage on both sides. A round trip
    through an in-memory stream gives the table buffers of its own.
    """
    table = pa.concat_tables([journal.read_segment(path) for path in paths])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue()).read_all()


# -- 1. the healthy side -----------------------------------------------------


def test_manifested_intact_segments_seal_as_they_always_did(lake_root):
    spy = _captured(lake_root, "SPY")
    qqq = _captured(lake_root, "QQQ")
    # A marker written the way the close guard writes one: no manifest entry, so the
    # check has nothing to compare and must read it rather than refuse it.
    marker = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "QQQ", DAY, "m", PID)
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "QQQ", DAY, "m", PID
    ) as writer:
        writer.write_cycle(
            journal.gap_batch(
                journal.CHAINS_SURFACE,
                ticker="QQQ",
                snap_ts=_et(10, 9),
                error_class="http_500",
                close_tag=None,
                session_phase=None,
            )
        )
    entries = latest_entries(lake_root)
    assert all(_rel(lake_root, path) in entries for path in spy + qqq)
    assert _rel(lake_root, marker) not in entries
    expected = {
        "SPY": _segments_table(spy),
        "QQQ": _segments_table(sorted([*qqq, marker])),
    }
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert result.refused == ()
    assert sorted(item.ticker for item in result.sealed) == ["QQQ", "SPY"]
    for item in result.sealed:
        assert item.rows == expected[item.ticker].num_rows
        assert pq.read_table(_partition(lake_root, item.ticker)).equals(expected[item.ticker])
    assert not any(path.exists() for path in [*spy, *qqq, marker])
    assert events == ["backup", "ping"]
    assert transport.messages == []
    assert not (lake_root / "reports" / report.DAMAGED_SEGMENTS_DIR).exists()


# -- 2. a damaged manifested segment -----------------------------------------


@pytest.mark.parametrize(
    "offset", [DROPS_THE_SEGMENT, CHANGES_A_VALUE], ids=["drops-the-segment", "changes-a-value"]
)
def test_a_flipped_byte_refuses_the_ticker_day_and_keeps_every_segment(lake_root, offset):
    spy = _captured(lake_root, "SPY")
    qqq = _captured(lake_root, "QQQ")
    damaged = spy[2]
    recorded = latest_entries(lake_root)[_rel(lake_root, damaged)]["sha256"]
    _flip(damaged, offset)
    before = {path: path.read_bytes() for path in spy}
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    # The other ticker-day on the same date sealed, and the damaged one did not. Without
    # the check, SPY seals here too, with 8 rows or with a changed value.
    assert [(item.ticker, item.rows) for item in result.sealed] == [("QQQ", 10)]
    assert not any(path.exists() for path in qqq)
    # Nothing of the damaged ticker-day was sealed or deleted.
    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    spy_partition = _rel(lake_root, _partition(lake_root, "SPY"))
    assert spy_partition not in latest_entries(lake_root)
    # The run went on to the end.
    assert events == ["backup", "ping"]
    assert result.backed_up and result.pinged
    # The refusal says which segment, with the digest it closed with and the one it has now.
    (refused,) = result.refused
    assert refused.reason == REFUSED_SEGMENT_DAMAGED
    assert refused.partition == spy_partition
    assert refused.segments == tuple(_rel(lake_root, path) for path in spy)
    assert refused.conflicts == ()
    assert refused.damaged == (
        DamagedSegment(
            segment=_rel(lake_root, damaged), expected=recorded, actual=sha256_file(damaged)
        ),
    )
    assert recorded != sha256_file(damaged)
    # One page, under its own title, naming the segment.
    (page,) = transport.messages
    assert page.event == DAMAGED_SEGMENT_EVENT
    assert page.title == DAMAGED_SEGMENT_TITLE
    assert _rel(lake_root, damaged) in page.body
    # One finding, in its own tree and not in the drift tree.
    (finding,) = _findings(lake_root)
    assert finding["day"] == DAY.isoformat()
    assert finding["surface"] == journal.CHAINS_SURFACE
    assert finding["ticker"] == "SPY"
    assert finding["segments"] == [_rel(lake_root, path) for path in spy]
    assert finding["partition"] == spy_partition
    assert finding["damaged"] == [
        {
            "segment": _rel(lake_root, damaged),
            "expected_sha256": recorded,
            "actual_sha256": sha256_file(damaged),
            "error": None,
        }
    ]
    assert not report.schema_drift_dir(lake_root, DAY).exists()


def test_a_shadow_append_on_a_manifested_segment_is_refused_rather_than_raised(lake_root):
    # Bytes after the end-of-stream marker used to raise ``ShadowAppendError`` and cost the
    # backup and the ping. The segment no longer matches the hash taken when it closed, so
    # the check refuses its ticker-day first and the rest of the run goes on. The raise
    # still holds for an unmanifested segment, which ``test_compaction.py`` covers.
    spy = _captured(lake_root, "SPY")
    spy[4].write_bytes(spy[4].read_bytes() + b"\x00" * 8)

    result, events = _run(lake_root)

    (refused,) = result.refused
    assert [item.segment for item in refused.damaged] == [_rel(lake_root, spy[4])]
    assert all(path.exists() for path in spy)
    assert events == ["backup", "ping"]


def test_every_damaged_segment_is_named_not_only_the_first(lake_root):
    spy = _captured(lake_root, "SPY")
    _flip(spy[1], CHANGES_A_VALUE)
    _flip(spy[3], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    (refused,) = result.refused
    assert [item.segment for item in refused.damaged] == [
        _rel(lake_root, spy[1]),
        _rel(lake_root, spy[3]),
    ]


def test_a_marker_sorting_before_the_damage_does_not_end_the_check(lake_root):
    # A gap marker has no manifest entry and takes its name from the minute it covers, so
    # in a real day it sorts among the capture segments rather than after them. The check
    # has to step past it and go on hashing the segments behind it.
    spy = _captured(lake_root, "SPY")
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "0", PID
    ) as writer:
        writer.write_cycle(
            journal.gap_batch(
                journal.CHAINS_SURFACE,
                ticker="SPY",
                snap_ts=_et(9, 30),
                error_class="http_500",
                close_tag=None,
                session_phase=None,
            )
        )
    _flip(spy[2], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    (refused,) = result.refused
    assert [item.segment for item in refused.damaged] == [_rel(lake_root, spy[2])]
    assert result.sealed == ()


def test_a_segment_that_cannot_be_read_for_its_hash_stops_the_run(lake_root, monkeypatch):
    # An access failure is not damage, and the seal's own read would swallow it as a torn
    # tail and unlink the segment. So the hash read lets it out, the run stops before any
    # write, and the missed ping pages. A rsync over the same file would fail as well.
    spy = _captured(lake_root, "SPY")
    real = compact_module.sha256_file
    locked = spy[2]

    def refuse(path):
        if Path(path) == locked:
            raise PermissionError(f"permission denied: {path}")
        return real(path)

    monkeypatch.setattr(compact_module, "sha256_file", refuse)

    with pytest.raises(PermissionError):
        _run(lake_root)

    assert all(path.exists() for path in spy)
    assert not _partition(lake_root, "SPY").exists()


def test_a_day_swept_late_files_under_the_day_the_segments_belong_to(lake_root):
    # A missed night leaves the day for the next run. The finding is about the ticker-day,
    # so it lands under that day's directory and says that day, and its stamp is Eastern
    # whatever zone the run's clock reports in, as the production clock reports UTC.
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    late = datetime(2026, 8, 25, 20, 30, tzinfo=UTC)
    calendar = FakeCalendar(
        {
            DAY: SessionTimes(open=_et(9, 30), close=_et(16, 0)),
            date(2026, 8, 25): SessionTimes(
                open=datetime(2026, 8, 25, 9, 30, tzinfo=MARKET_TZ),
                close=datetime(2026, 8, 25, 16, 0, tzinfo=MARKET_TZ),
            ),
        }
    )

    result = compact(
        lake_root,
        clock=ManualClock(late),
        calendar=calendar,
        backup=FakeBackup([]),
        backup_target=TARGET,
        plan_path=lake_root.parent / "chain_plan.json",
    )

    assert len(result.refused) == 1
    (finding,) = _findings(lake_root)
    assert finding["day"] == DAY.isoformat()
    assert finding["at"] == "2026-08-25T16:30:00-04:00"
    assert not (lake_root / "reports" / "damaged_segments" / "date=2026-08-25").exists()


def test_an_unsent_damage_page_names_itself_on_stderr(lake_root, capsys):
    class Broken:
        def send(self, message):
            raise ConnectionError("ntfy unreachable")

    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    publisher = Publisher(lake_root=lake_root, transport=Broken(), secrets=(PING_KEY, NTFY_TOPIC))

    _run(lake_root, publisher=publisher)

    assert "compaction: damaged-segment page not sent: post_failed" in capsys.readouterr().err


# -- 3. one refused list, with a reason --------------------------------------


def test_a_damaged_chains_ticker_day_keeps_the_retune_off_the_day(lake_root):
    spy = _captured(lake_root, "SPY")
    _captured(lake_root, "QQQ")
    _flip(spy[2], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    assert result.retune is not None
    assert result.retune.skipped_reason == "a chains ticker-day on this day was refused: SPY"
    assert not result.retune.written


def test_the_summary_words_damage_as_damage(lake_root):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)
    lines = result.render().splitlines()

    assert "refused=1" in lines[0]
    (line,) = [line for line in lines if line.startswith("  refused")]
    assert line == (
        "  refused  chains/ticker=SPY/date=2026-08-24.parquet segments=5 kept, "
        f"damaged, sha256 no longer matches: {spy[2].name}"
    )
    assert result.changed


# -- 4. an unmanifested segment is the reader's -------------------------------


def _unmanifested(lake_root: Path) -> list[Path]:
    """Segments with no manifest entry, the shape a crash between write and append leaves."""
    spy = _captured(lake_root, "SPY")
    (lake_root / "manifest.jsonl").unlink()
    return spy


def test_an_unmanifested_flip_the_read_proves_refuses_the_ticker_day(lake_root):
    """The flip that used to seal the day 2 rows short now refuses it (marketlake #552).

    No digest exists to compare, so the hash check skips the segment. The file still ends
    in its end-of-stream marker, so the stream that stops inside it is damage rather than a
    tear, and the read raises into the same refusal the hash check uses. Before, compaction
    read it as a segment torn before its first batch, sealed 8 of 10 rows, and unlinked it.
    """
    spy = _unmanifested(lake_root)
    damaged = spy[2]
    _flip(damaged, DROPS_THE_SEGMENT)
    before = {path: path.read_bytes() for path in spy}
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert result.sealed == ()
    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    assert events == ["backup", "ping"]
    (refused,) = result.refused
    assert refused.reason == REFUSED_SEGMENT_DAMAGED
    assert refused.segments == tuple(_rel(lake_root, path) for path in spy)
    (item,) = refused.damaged
    # No entry, so no recorded digest. The read's failure is the evidence instead.
    assert (item.segment, item.expected, item.actual) == (
        _rel(lake_root, damaged),
        None,
        sha256_file(damaged),
    )
    assert item.error is not None and "stopped inside a finished file" in item.error
    (finding,) = _findings(lake_root)
    assert finding["damaged"] == [
        {
            "segment": _rel(lake_root, damaged),
            "expected_sha256": None,
            "actual_sha256": sha256_file(damaged),
            "error": item.error,
        }
    ]
    # Worded as a failed read, never as a digest that no longer matches, since none was
    # recorded.
    (page,) = transport.messages
    assert page.title == DAMAGED_SEGMENT_TITLE
    assert "1 segment(s) failed to read as damaged" in page.body
    assert "no longer match" not in page.body
    (line,) = [line for line in result.render().splitlines() if line.startswith("  refused")]
    assert line.endswith(f"segments=5 kept, damaged, failed to read: {damaged.name}")


def test_a_damaged_marker_beside_intact_captures_refuses_the_ticker_day(lake_root):
    """A marker segment carries no entry, and the read is the only check it gets.

    The captures beside it pass their hash check, so the refusal names the marker alone.
    Sealing past it would drop the marker's gap row and unlink the file, and that row is
    the only record of why its minute is missing.
    """
    spy = _captured(lake_root, "SPY")
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "m", PID
    ) as writer:
        writer.write_cycle(
            journal.gap_batch(
                journal.CHAINS_SURFACE,
                ticker="SPY",
                snap_ts=_et(10, 9),
                error_class="http_500",
                close_tag=None,
                session_phase=None,
            )
        )
    marker = writer.path
    # The file still ends in its end-of-stream marker after the flip, which lands on the
    # body ahead of it.
    data = bytearray(marker.read_bytes())
    assert len(data) == MARKER_BYTES
    assert data[-8:] == b"\xff\xff\xff\xff\x00\x00\x00\x00"
    data[MARKER_FLIP] ^= 0x01
    marker.write_bytes(bytes(data))

    result, _ = _run(lake_root)

    assert result.sealed == ()
    (refused,) = result.refused
    assert [item.segment for item in refused.damaged] == [_rel(lake_root, marker)]
    assert refused.damaged[0].expected is None
    assert "a batch failed validation" in (refused.damaged[0].error or "")
    assert all(path.exists() for path in [*spy, marker])


def test_a_read_that_refuses_a_segment_its_digest_passed_keeps_the_digest(lake_root, monkeypatch):
    """The shape a validation false positive would take, reported rather than dropped.

    The bytes are the ones the capture hashed, so the digest check passes, and the read
    refuses them anyway. That can only mean the check is wrong or the writer wrote bad
    bytes, and either way the finding has to say the digest matched. It carries the
    recorded digest beside the one on disk, equal, so an operator reading it sees at once
    that the disk did not change them.
    """
    spy = _captured(lake_root, "SPY")
    refused_path = spy[1]
    recorded = latest_entries(lake_root)[_rel(lake_root, refused_path)]["sha256"]
    real = journal.read_segment

    def refuse_one(path):
        if Path(path) == refused_path:
            raise journal.SegmentDamaged(Path(path), "a batch failed validation: refused")
        return real(path)

    monkeypatch.setattr(journal, "read_segment", refuse_one)

    result, _ = _run(lake_root)

    assert result.sealed == ()
    (refused,) = result.refused
    (item,) = refused.damaged
    assert (item.segment, item.expected, item.actual) == (
        _rel(lake_root, refused_path),
        recorded,
        recorded,
    )
    assert item.error is not None and "a batch failed validation" in item.error
    assert all(path.exists() for path in spy)


def test_an_unmanifested_flip_that_changes_a_value_still_seals_it(lake_root):
    spy = _unmanifested(lake_root)
    healthy = _segments_table(spy)
    _flip(spy[2], CHANGES_A_VALUE)

    result, _ = _run(lake_root)

    assert result.refused == ()
    assert [item.rows for item in result.sealed] == [5 * ROWS_PER_SEGMENT]
    assert not pq.read_table(_partition(lake_root, "SPY")).equals(healthy)


# -- 5. every night, and never swallowed -------------------------------------


def test_the_refusal_repeats_every_night_the_damage_survives(lake_root):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    publisher, transport = _paging(lake_root)

    first, _ = _run(lake_root, publisher=publisher)
    # A later instant, as the next night's run would have. The finding's name carries the
    # stamp, so two runs at one instant from one process would name the same file.
    second, _ = _run(lake_root, publisher=publisher, at=_et(16, 31))

    assert len(first.refused) == len(second.refused) == 1
    assert second.changed
    assert len(_findings(lake_root)) == 2
    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE] * 2
    assert all(path.exists() for path in spy)


def test_a_finding_that_cannot_be_written_still_pages(lake_root, monkeypatch, capsys):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)

    def refuse(*args, **kwargs):
        raise PermissionError("reports is read-only")

    monkeypatch.setattr(compact_module, "write_damaged_segments", refuse)
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert len(result.refused) == 1
    assert events == ["backup", "ping"]
    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE]
    assert "could not be filed: PermissionError" in capsys.readouterr().err


def test_drift_and_damage_in_one_run_send_both_pages(lake_root):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    # A QQQ ticker-day whose one segment carries a column the pinned schema does not name,
    # written by hand with no manifest entry, so it seals and files drift.
    table = journal.read_segment(_captured(lake_root, "QQQ", count=1)[0])
    table = table.append_column("vendor_new_field", pa.array(["x"] * table.num_rows))
    qqq = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "QQQ", DAY, "z", PID)
    with pa.OSFile(str(qqq), "wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    assert [item.ticker for item in result.sealed] == ["QQQ"]
    assert sorted(page.title for page in transport.messages) == sorted(
        [DAMAGED_SEGMENT_TITLE, SCHEMA_DRIFT_TITLE]
    )


def test_a_raise_later_in_the_sweep_cannot_swallow_the_damage_page(lake_root, monkeypatch):
    # The page goes out from the ``finally`` around the whole sweep, so a raise on a later
    # step cannot carry the finding out of the run with the phone silent.
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    real = compact_module._prune_empty

    def refuse(date_dir):
        real(date_dir)
        raise OSError("the journal directory went away")

    monkeypatch.setattr(compact_module, "_prune_empty", refuse)
    publisher, transport = _paging(lake_root)

    with pytest.raises(OSError, match="went away"):
        _run(lake_root, publisher=publisher)

    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE]


def test_the_damage_page_goes_out_before_the_backup(lake_root):
    class Unplugged:
        def sync(self, source, target):
            raise RuntimeError("backup target not mounted")

    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    publisher, transport = _paging(lake_root)

    with pytest.raises(RuntimeError, match="not mounted"):
        compact(
            lake_root,
            clock=ManualClock(_et(16, 30)),
            calendar=_calendar(),
            backup=Unplugged(),
            backup_target=TARGET,
            publisher=publisher,
            plan_path=lake_root.parent / "chain_plan.json",
        )

    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE]


def test_a_drift_page_that_raises_cannot_swallow_the_damage_page(lake_root, monkeypatch):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)

    def broken(*args, **kwargs):
        raise RuntimeError("the drift page broke")

    monkeypatch.setattr(compact_module, "_page_drift", broken)
    publisher, transport = _paging(lake_root)

    with pytest.raises(RuntimeError, match="drift page broke"):
        _run(lake_root, publisher=publisher)

    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE]


# -- 6. the page's size ------------------------------------------------------


def _damage(day: date, count: int) -> SegmentDamage:
    base = f"journal/date={day.isoformat()}/surface=chains/ticker=SPY"
    return SegmentDamage(
        surface="chains",
        ticker="SPY",
        day=day,
        partition=f"chains/ticker=SPY/date={day.isoformat()}.parquet",
        segments=tuple(f"{base}/seg-{index:020d}-{index:06d}.arrows" for index in range(count)),
        damaged=tuple(
            DamagedSegment(
                segment=f"{base}/seg-{index:020d}-{index:06d}.arrows",
                expected="a" * 64,
                actual="b" * 64,
            )
            for index in range(count)
        ),
    )


def test_the_page_stays_under_the_body_limit_however_wide_the_damage():
    # The design keeps a page body under 1,000 bytes, and ntfy refuses an oversize one
    # outright. A disk going bad is the case that damages many segments at once.
    found = [_damage(date(2026, 7, 1 + index), 20) for index in range(30)]

    body = compact_module._damage_body(found)

    assert len(body.encode("utf-8")) < 1000
    assert "30 ticker-day(s)" in body
    assert "600 segment(s)" in body
    assert f"and {600 - PAGE_SEGMENT_CAP} more" in body
    assert f"and {30 - PAGE_SEGMENT_CAP} more" in body


def test_the_page_counts_and_names_only_the_damaged_segments():
    # A ticker-day of seven segments with five damaged. The page counts five, names the
    # cap's worth of them and no more, and never names an intact one.
    whole = _damage(DAY, 7)
    found = [
        SegmentDamage(
            surface=whole.surface,
            ticker=whole.ticker,
            day=whole.day,
            partition=whole.partition,
            segments=whole.segments,
            damaged=whole.damaged[2:],
        )
    ]

    body = compact_module._damage_body(found)

    assert "1 ticker-day(s)" in body
    assert "5 segment(s)" in body
    assert sum(item.segment in body for item in whole.damaged) == PAGE_SEGMENT_CAP
    assert not any(item.segment in body for item in whole.damaged[:2])
    assert f"and {5 - PAGE_SEGMENT_CAP} more" in body
    assert "reports/damaged_segments/" in body


def test_the_page_names_every_segment_up_to_the_cap():
    found = [_damage(DAY, PAGE_SEGMENT_CAP)]

    body = compact_module._damage_body(found)

    assert all(item.segment in body for item in found[0].damaged)
    assert "more" not in body


def test_the_page_and_the_summary_count_each_kind_of_damage_apart():
    """A segment refused by its read had no digest compared, so it is not counted as one.

    One ticker-day with a digest mismatch and a segment the read refused. The page counts
    them under their own words, and the summary names each under its own heading.
    """
    base = f"journal/date={DAY.isoformat()}/surface=chains/ticker=SPY"
    mismatch = DamagedSegment(segment=f"{base}/seg-a.arrows", expected="a" * 64, actual="b" * 64)
    unread = DamagedSegment(
        segment=f"{base}/seg-b.arrows", expected=None, actual="c" * 64, error="stopped"
    )
    found = [
        SegmentDamage(
            surface="chains",
            ticker="SPY",
            day=DAY,
            partition=f"chains/ticker=SPY/date={DAY.isoformat()}.parquet",
            segments=(mismatch.segment, unread.segment),
            damaged=(mismatch, unread),
        )
    ]

    body = compact_module._damage_body(found)
    kinds = compact_module._damage_kinds((mismatch, unread))
    message = str(
        DamagedSegments(
            surface="chains",
            ticker="SPY",
            day=DAY,
            partition=found[0].partition,
            segments=found[0].segments,
            damaged=(mismatch, unread),
        )
    )

    assert (
        "1 segment(s) no longer match the sha256 taken when they closed "
        "and 1 segment(s) failed to read as damaged." in body
    )
    assert kinds == "sha256 no longer matches: seg-a.arrows; failed to read: seg-b.arrows"
    assert "seg-a.arrows (sha256 no longer matches: recorded " in message
    assert "seg-b.arrows (failed to read: stopped; no sha256 recorded, now " in message


# -- 7. the human-invoked repair ---------------------------------------------


def test_the_repair_raises_the_refusal_and_leaves_the_segments(lake_root):
    spy = _captured(lake_root, "SPY")
    _flip(spy[2], DROPS_THE_SEGMENT)
    before = {path: path.read_bytes() for path in spy}

    with pytest.raises(DamagedSegments) as info:
        recompact_ticker_day(
            lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
        )

    assert [item.segment for item in info.value.damaged] == [_rel(lake_root, spy[2])]
    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()


def test_the_repair_seals_manifested_intact_segments(lake_root):
    spy = _captured(lake_root, "SPY")

    outcome = recompact_ticker_day(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
    )

    assert outcome.rows == 5 * ROWS_PER_SEGMENT
    assert not any(path.exists() for path in spy)


# -- 8. the writer -----------------------------------------------------------


def test_the_writer_files_one_json_per_finding_and_never_overwrites(lake_root):
    damage = _damage(DAY, 1)
    now = _et(16, 30)

    path = report.write_damaged_segments(lake_root, damage, now=now, pid=42)

    assert path.parent == lake_root / "reports" / "damaged_segments" / "date=2026-08-24"
    assert path.name == "163000000000-chains-SPY-42.json"
    assert json.loads(path.read_text()) == {
        "at": "2026-08-24T16:30:00-04:00",
        "day": "2026-08-24",
        "surface": "chains",
        "ticker": "SPY",
        "partition": "chains/ticker=SPY/date=2026-08-24.parquet",
        "segments": list(damage.segments),
        "damaged": [
            {
                "segment": damage.damaged[0].segment,
                "expected_sha256": "a" * 64,
                "actual_sha256": "b" * 64,
                "error": None,
            }
        ],
    }
    with pytest.raises(FileExistsError):
        report.write_damaged_segments(lake_root, damage, now=now, pid=42)


def test_the_writer_never_creates_a_missing_lake(tmp_path):
    missing = tmp_path / "gone"

    with pytest.raises(FileNotFoundError):
        report.write_damaged_segments(missing, _damage(DAY, 1), now=_et(16, 30), pid=1)

    assert not missing.exists()
