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
   seal a changed value, and a shadow-append. It holds too for a segment damaged after it
   closed and before its writer took the lock, because the digest is taken at close
   (marketlake #573).
3. The refusal joins ``refused`` with its own reason, so the re-tune leaves the day alone and
   the summary words it as damage rather than a type conflict.
4. An unmanifested segment has no digest to compare, so the read judges it. A flip the
   read proves is damage refuses the ticker-day the same way, with the read's failure in
   place of the recorded digest (marketlake #552). A flip that decodes cleanly with a
   changed value still seals, because no structural check can see it. A segment the read
   cannot open at all is an access failure rather than damage, so it ends the run, and the
   repair too, with nothing sealed or unlinked (marketlake #591).
5. The refusal repeats every night the damage survives, a finding that cannot be written
   still pages, and a run with drift and damage sends both pages under their own titles.
6. The page stays inside the design's body limit however many segments are damaged.
7. The human-invoked repair raises the refusal rather than sealing past it.
8. The writer itself: where the file lands, what it holds, and that it never overwrites.
9. A manifested segment gone from disk reaches neither the hash nor the read, so the seal
   compares the manifest with the disk (marketlake #998). The lost minute gets a gap row, the
   rest seals, one page and one finding go out, and the upload carries on. The comparison
   reads only that ticker-day's entries, and the repair of a sealed day leaves the count to
   the no-shrink guard.
"""

from __future__ import annotations

import errno
import json
import os
from contextlib import contextmanager
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
from lake.chain_plan import ChainPlan
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
from lake.manifest import (
    RowCountRegression,
    append_manifest,
    latest_entries,
    manifest_path,
    scrub,
    sha256_file,
)
from lake.paths import LakePaths
from lake.report import DamagedSegment, SegmentDamage
from lake.tickers import Roster
from tests.conftest import CASSETTES
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport
from tests.support.vendor import CassetteVendor

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
# Raises ``OSError`` at the open, the commonest class a flip raises there.
OSERROR_AT_OPEN = 1860
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


def _damage_before_the_lock(monkeypatch, lake_root: Path) -> dict[Path, str]:
    """Flip a byte in each SPY chains segment after it closes and before its writer locks.

    Returns each damaged segment with the digest it had before the flip, which is the digest
    it closed with. The flip changes a value and decodes cleanly, so only the digest can see
    it.
    """
    real = capture.lake_lock
    closed: dict[Path, str] = {}

    @contextmanager
    def damaging(root):
        for path in sorted(lake_root.rglob("*.arrows")):
            if "surface=chains" in path.as_posix() and "ticker=SPY" in path.as_posix():
                if path not in closed:
                    closed[path] = sha256_file(path)
                    data = bytearray(path.read_bytes())
                    data[data.index(b"2026-08-24")] ^= 0x01
                    path.write_bytes(bytes(data))
        with real(root) as held:
            yield held

    monkeypatch.setattr(capture, "lake_lock", damaging)
    return closed


@pytest.mark.parametrize("writer", ["cycle", "snapshot"])
def test_a_segment_damaged_while_its_writer_waits_for_the_lock_keeps_its_close_time_hash(
    lake_root, monkeypatch, writer
):
    # The hash is taken when the segment closes, outside the lock (marketlake #573). So a
    # segment that changes while the cycle waits for the lock is recorded as it closed, and
    # compaction refuses the ticker-day. Hashed under the lock, the entry would bless the
    # changed bytes and the day would seal.
    closed = _damage_before_the_lock(monkeypatch, lake_root)
    if writer == "cycle":
        capture.run_cycle(
            ManualClock(_et(10, 0)),
            CassetteVendor(_RECORDED),
            Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
            lake_root,
            pid=PID,
            plan=ChainPlan(((0, None),)),
        )
    else:
        _captured(lake_root, "SPY", count=1)
    (damaged,) = closed
    recorded = latest_entries(lake_root)[_rel(lake_root, damaged)]["sha256"]
    assert recorded == closed[damaged]
    assert recorded != sha256_file(damaged)

    result, events = _run(lake_root)

    (refused,) = result.refused
    assert refused.reason == REFUSED_SEGMENT_DAMAGED
    assert refused.partition == _rel(lake_root, _partition(lake_root, "SPY"))
    assert refused.damaged == (
        DamagedSegment(
            segment=_rel(lake_root, damaged), expected=recorded, actual=sha256_file(damaged)
        ),
    )
    assert damaged.exists()
    assert not _partition(lake_root, "SPY").exists()
    assert events == ["backup", "ping"]


def test_a_snapshot_whose_hash_at_close_raises_records_no_entry(lake_root, monkeypatch):
    # A snapshot has no fallback hash under the lock. Its caller reads the raise, so the
    # entry is not written with a digest taken after the segment could have changed.
    def refuse(path):
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(capture, "sha256_file", refuse)

    with pytest.raises(PermissionError):
        _captured(lake_root, "SPY", count=1)

    assert not manifest_path(lake_root).exists()


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
    # An access failure is not damage. So the hash read lets it out, the run stops before
    # any write, and the missed ping pages. A rsync over the same file would fail as well.
    # The seal's own read lets it out too, which section 4 covers for a segment with no
    # manifest entry.
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
    # The record already names the segment, so the error does not repeat its machine path.
    assert str(lake_root) not in item.error
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


def test_every_damaged_segment_is_named_in_one_run(lake_root):
    spy = _unmanifested(lake_root)
    _flip(spy[1], DROPS_THE_SEGMENT)
    _flip(spy[3], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    (refused,) = result.refused
    assert [bad.segment for bad in refused.damaged] == [
        _rel(lake_root, spy[1]),
        _rel(lake_root, spy[3]),
    ]


def test_a_ticker_day_whose_every_segment_is_damaged_is_still_refused(lake_root):
    """No segment survives the read, so nothing is left to merge. Sealing the empty merge
    would unlink every segment, the only copy of the day's rows."""
    spy = _unmanifested(lake_root)
    for path in spy:
        _flip(path, DROPS_THE_SEGMENT)
    before = {path: path.read_bytes() for path in spy}

    result, _ = _run(lake_root)

    assert result.sealed == ()
    (refused,) = result.refused
    assert refused.reason == REFUSED_SEGMENT_DAMAGED
    assert len(refused.damaged) == len(spy)
    assert {path: path.read_bytes() for path in spy} == before


def test_an_os_error_at_the_open_refuses_the_ticker_day(lake_root):
    """The commonest class a flip raises at the open, which compaction used to read as a
    segment torn before its first batch."""
    spy = _unmanifested(lake_root)
    _flip(spy[2], OSERROR_AT_OPEN)

    result, _ = _run(lake_root)

    assert result.sealed == ()
    (refused,) = result.refused
    assert [bad.segment for bad in refused.damaged] == [_rel(lake_root, spy[2])]
    assert spy[2].exists()


def _deny(path: Path) -> None:
    """Take every permission off a file, and prove the process can no longer open it.

    Root opens a ``chmod 000`` file anyway, so under root the denial never happens and a
    test built on it would reach none of the code it names. The skip marks that case, and
    this check refuses to go on if anything else lets the open through.
    """
    path.chmod(0o000)
    with pytest.raises(PermissionError):
        path.open("rb")


def _run_into(lake_root: Path, events: list[str]) -> CompactionResult:
    """``_run``, recording the backup and the ping into a list the caller keeps if it raises."""
    return compact(
        lake_root,
        clock=ManualClock(_et(16, 30)),
        calendar=_calendar(),
        backup=FakeBackup(events),
        backup_target=TARGET,
        pinger=FakePinger(events),
        ping_url=URL,
        plan_path=lake_root.parent / "chain_plan.json",
    )


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 file")
def test_an_unmanifested_segment_that_cannot_be_opened_stops_the_run(lake_root):
    """An access failure is not damage and not a tear, so it ends the run (marketlake #591).

    The read used to take it as a segment torn before its first batch. The day sealed 8 of
    its 10 rows, the denied segment was unlinked, and the run backed up and pinged healthy.
    Now nothing is sealed or unlinked, the backup and the ping are skipped, and the missed
    ping pages, as a manifested segment's hash read already did.
    """
    spy = _unmanifested(lake_root)
    locked = spy[2]
    before = {path: path.read_bytes() for path in spy}
    _deny(locked)
    events: list[str] = []
    try:
        with pytest.raises(PermissionError):
            _run_into(lake_root, events)
    finally:
        locked.chmod(0o644)

    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    assert latest_entries(lake_root) == {}
    assert events == []


def _dangling(path: Path) -> None:
    path.symlink_to(path.with_name("gone.arrows"))


def _directory(path: Path) -> None:
    path.mkdir()


@pytest.mark.parametrize("make", [_dangling, _directory], ids=["missing", "not-a-file"])
def test_a_segment_name_with_no_file_behind_it_stops_the_run(lake_root, make):
    """The other two access failures, a file gone after the listing and a directory named
    like a segment. Neither holds rows, but a read that folded them into no rows sealed the
    day and unlinked the real segments before it met them again at the unlink. It sorts
    after every real segment, so the real ones are read first."""
    spy = _unmanifested(lake_root)
    before = {path: path.read_bytes() for path in spy}
    odd = journal.segment_path(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "zz", PID)
    make(odd)
    assert sorted(odd.parent.glob("*.arrows"))[-1] == odd
    events: list[str] = []

    with pytest.raises(OSError):
        _run_into(lake_root, events)

    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    assert latest_entries(lake_root) == {}
    assert events == []


@pytest.mark.parametrize(
    "error",
    [OSError(errno.EMFILE, "Too many open files"), ValueError("a bug"), TypeError("a bug")],
    ids=["out-of-descriptors", "value-error", "type-error"],
)
def test_only_a_tear_reads_as_no_rows(lake_root, monkeypatch, error):
    """The read folds ``ArrowInvalid`` into no rows and nothing else. A regular file the
    system could not open, out of descriptors for one, holds rows, and so might a file a
    bug in the reader failed on. Folding either would seal the day without them. This runs
    under root too, which the ``chmod 000`` tests cannot."""
    spy = _unmanifested(lake_root)
    before = {path: path.read_bytes() for path in spy}
    real = journal.read_segment

    def fail_one(path):
        if Path(path) == spy[2]:
            raise error
        return real(path)

    monkeypatch.setattr(journal, "read_segment", fail_one)
    events: list[str] = []

    with pytest.raises(type(error)):
        _run_into(lake_root, events)

    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    assert latest_entries(lake_root) == {}
    assert events == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 file")
def test_the_repair_stops_on_a_segment_it_cannot_open(lake_root):
    """The repair seals through the same read, so it lost the segment the same way."""
    spy = _unmanifested(lake_root)
    locked = spy[2]
    before = {path: path.read_bytes() for path in spy}
    _deny(locked)
    try:
        with pytest.raises(PermissionError):
            recompact_ticker_day(
                lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
            )
    finally:
        locked.chmod(0o644)

    assert {path: path.read_bytes() for path in spy} == before
    assert not _partition(lake_root, "SPY").exists()
    assert latest_entries(lake_root) == {}


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


def test_a_refused_day_seals_once_its_good_bytes_are_back(lake_root):
    """A refusal is not remembered, which is what lets a live copy repair a quotes day."""
    spy = _captured(lake_root, "SPY")
    good = spy[2].read_bytes()
    _flip(spy[2], DROPS_THE_SEGMENT)

    refused, _ = _run(lake_root)
    spy[2].write_bytes(good)
    repaired, _ = _run(lake_root, at=_et(16, 31))

    assert len(refused.refused) == 1
    assert repaired.refused == ()
    assert [item.ticker for item in repaired.sealed] == ["SPY"]
    assert not any(path.exists() for path in spy)


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
    assert message.startswith(f"{found[0].partition}: 2 segment(s) damaged: ")
    a, b = "a" * 64, "b" * 64
    assert f"seg-a.arrows (sha256 no longer matches: recorded {a}, now {b})" in message
    assert "seg-b.arrows (failed to read: stopped; no sha256 recorded, now " in message


def test_the_words_follow_each_segment_rather_than_the_first():
    """A read refusal of a segment that did have a digest names that digest, two mismatches
    are both named, and a page with no read refusal does not mention one."""
    base = f"journal/date={DAY.isoformat()}/surface=chains/ticker=SPY"
    refused = DamagedSegment(
        segment=f"{base}/seg-r.arrows", expected="d" * 64, actual="c" * 64, error="x"
    )
    first = DamagedSegment(segment=f"{base}/seg-a.arrows", expected="a" * 64, actual="b" * 64)
    second = DamagedSegment(segment=f"{base}/seg-c.arrows", expected="a" * 64, actual="b" * 64)

    message = str(
        DamagedSegments(
            surface="chains",
            ticker="SPY",
            day=DAY,
            partition="p",
            segments=(refused.segment,),
            damaged=(refused,),
        )
    )
    kinds = compact_module._damage_kinds((first, second))
    body = compact_module._damage_body(
        [
            SegmentDamage(
                surface="chains",
                ticker="SPY",
                day=DAY,
                partition="p",
                segments=(first.segment,),
                damaged=(first,),
            )
        ]
    )

    assert f"seg-r.arrows (failed to read: x; recorded {'d' * 64}, now {'c' * 64})" in message
    assert kinds == "sha256 no longer matches: seg-a.arrows, seg-c.arrows"
    assert "failed to read" not in body
    assert "1 segment(s) no longer match the sha256 taken when they closed. " in body


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
        "gaps": [],
    }
    with pytest.raises(FileExistsError):
        report.write_damaged_segments(lake_root, damage, now=now, pid=42)


def test_the_writer_never_creates_a_missing_lake(tmp_path):
    missing = tmp_path / "gone"

    with pytest.raises(FileNotFoundError):
        report.write_damaged_segments(missing, _damage(DAY, 1), now=_et(16, 30), pid=1)

    assert not missing.exists()


# -- 9. a manifested segment gone from disk ----------------------------------

_QUOTES = _RECORDED.find("quotes", {"symbols": ["SPY", "QQQ"]}).body


def _captured_quotes(lake_root: Path, ticker: str, *, count: int = 5) -> list[Path]:
    """``count`` quotes segments for one ticker, each with its manifest entry."""
    paths = []
    for minute in range(count):
        at = _et(10, minute)
        outcome = capture.journal_snapshot(
            lake_root,
            journal.QUOTES_SURFACE,
            ticker,
            body=_QUOTES,
            cycle_start=at,
            fetch_ts=at,
            fetch_end_ts=at,
            pid=PID,
        )
        paths.append(outcome.path)
    return paths


def _row_kinds(partition: Path) -> list[tuple[str, str | None]]:
    """Each row's ``row_kind`` and ``error_class``, in file order."""
    table = pq.read_table(partition, columns=["row_kind", "error_class"])
    kinds = table.column("row_kind").to_pylist()
    return list(zip(kinds, table.column("error_class").to_pylist(), strict=True))


def _gap_minutes(partition: Path) -> list[str]:
    table = pq.read_table(partition, columns=["snap_ts", "error_class"])
    return [
        snap
        for snap, kind in zip(
            table.column("snap_ts").to_pylist(),
            table.column("error_class").to_pylist(),
            strict=True,
        )
        if kind == compact_module.SEGMENT_LOST
    ]


@pytest.mark.parametrize(
    "surface, write",
    [(journal.CHAINS_SURFACE, _captured), (journal.QUOTES_SURFACE, _captured_quotes)],
)
def test_a_manifested_segment_gone_from_disk_marks_its_minute_and_seals(lake_root, surface, write):
    """The issue's probe, under the owner's ruling to mark the gap and continue."""
    spy = write(lake_root, "SPY")
    qqq = write(lake_root, "QQQ")
    rows_per_segment = journal.read_segment(spy[0]).num_rows
    gone = spy.pop(2)
    rel = _rel(lake_root, gone)
    recorded = latest_entries(lake_root)[rel]["sha256"]
    gone.unlink()
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    # Both ticker-days sealed, and the run went on to the end.
    assert result.refused == ()
    assert events == ["backup", "ping"]
    assert sorted(item.ticker for item in result.sealed) == ["QQQ", "SPY"]
    assert not any(path.exists() for path in (*spy, *qqq))
    (sealed,) = [item for item in result.sealed if item.ticker == "SPY"]
    assert sealed.rows == 4 * rows_per_segment + 1
    partition = LakePaths(lake_root).partition_path(surface, "SPY", DAY)
    kinds = _row_kinds(partition)
    assert kinds.count((journal.ROW_KIND_GAP, compact_module.SEGMENT_LOST)) == 1
    assert sum(kind == journal.ROW_KIND_DATA for kind, _ in kinds) == 4 * rows_per_segment
    # The minute of the third capture, 10:02 Eastern, spelled the way a loop row is.
    minute = "2026-08-24T14:02:00+00:00"
    assert _gap_minutes(partition) == [minute]
    assert sealed.gaps == (minute,)
    assert sealed.lost == (
        DamagedSegment(
            segment=rel, expected=recorded, actual=None, error=compact_module.SEGMENT_MISSING
        ),
    )
    (page,) = transport.messages
    assert page.title == DAMAGED_SEGMENT_TITLE
    assert rel in page.body
    assert "sealed with gaps" in page.body
    assert "1 manifested segment(s) are missing from disk" in page.body
    assert "1 minute(s) are marked segment_lost" in page.body
    (finding,) = _findings(lake_root)
    assert finding["surface"] == surface
    assert finding["segments"] == [_rel(lake_root, path) for path in spy]
    assert finding["gaps"] == [minute]
    assert finding["damaged"] == [
        {
            "segment": rel,
            "expected_sha256": recorded,
            "actual_sha256": None,
            "error": compact_module.SEGMENT_MISSING,
        }
    ]
    # The partition's entry supersedes the segment's, so nothing stops on it again.
    assert scrub(lake_root).missing == ()


def test_a_gone_segment_leaves_the_retune_free_to_read_the_day(lake_root):
    """The day sealed, so a refusal's reason for skipping the re-tune does not apply."""
    spy = _captured(lake_root, "SPY")
    _captured(lake_root, "QQQ")
    spy[2].unlink()

    result, _ = _run(lake_root)

    assert result.retune is not None
    assert "refused" not in (result.retune.skipped_reason or "")


def test_a_changed_segment_refuses_the_day_and_names_a_gone_one_beside_it(lake_root):
    """A refusal keeps the day, so the gone segments wait with it and are named in it."""
    spy = _captured(lake_root, "SPY")
    gone = sorted(_rel(lake_root, path) for path in (spy[1], spy[3]))
    spy[1].unlink()
    spy[3].unlink()
    _flip(spy[2], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    assert result.sealed == ()
    (refused,) = result.refused
    assert [item.segment for item in refused.damaged] == [*gone, _rel(lake_root, spy[2])]
    assert [item.actual is None for item in refused.damaged] == [True, True, False]
    assert not _partition(lake_root, "SPY").exists()


def test_a_gone_segment_of_one_ticker_day_says_nothing_about_another(lake_root):
    """Only the entries under this surface, ticker and day count.

    ``SPYG`` sorts right after ``SPY`` and shares its prefix, so a comparison that left off
    the closing slash would call its neighbour's file lost from the healthy ticker-day. The
    quotes pass only if the surface is in the comparison. The next two tests do the same for
    the date.
    """
    _captured(lake_root, "SPY")
    _captured_quotes(lake_root, "SPY")
    gone = _captured(lake_root, "SPYG")[2]
    gone.unlink()

    result, _ = _run(lake_root)

    assert result.refused == ()
    lost = {(item.surface, item.ticker): item.lost for item in result.sealed}
    assert lost[(journal.CHAINS_SURFACE, "SPY")] == ()
    assert lost[(journal.QUOTES_SURFACE, "SPY")] == ()
    assert [item.segment for item in lost[(journal.CHAINS_SURFACE, "SPYG")]] == [
        _rel(lake_root, gone)
    ]


NEXT_DAY = date(2026, 8, 25)


def _at(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=MARKET_TZ)


def _captured_on(lake_root: Path, day: date) -> list[Path]:
    """Three chains segments for SPY on ``day``, each with its manifest entry."""
    paths = []
    for minute in range(3):
        at = _at(day, 10, minute)
        outcome = capture.journal_snapshot(
            lake_root,
            journal.CHAINS_SURFACE,
            "SPY",
            body=_CHAIN,
            cycle_start=at,
            fetch_ts=at,
            fetch_end_ts=at,
            pid=PID,
        )
        paths.append(outcome.path)
    return paths


def _run_on(lake_root: Path, day: date) -> CompactionResult:
    calendar = FakeCalendar(
        {
            item: SessionTimes(open=_at(item, 9, 30), close=_at(item, 16, 0))
            for item in (DAY, NEXT_DAY)
        }
    )
    return compact(
        lake_root,
        clock=ManualClock(_at(day, 16, 30)),
        calendar=calendar,
        backup=FakeBackup([]),
        backup_target=TARGET,
        plan_path=lake_root.parent / "chain_plan.json",
    )


def test_a_sealed_days_entries_say_nothing_about_the_next_day(lake_root):
    """A sealed day's segment entries stay in the manifest with their files gone.

    A comparison that lost the date would call all of them lost on the next day, and every
    ticker would carry false findings from its second sealed day on.
    """
    _captured_on(lake_root, DAY)
    assert [item.day for item in _run_on(lake_root, DAY).sealed] == [DAY]
    _captured_on(lake_root, NEXT_DAY)

    result = _run_on(lake_root, NEXT_DAY)

    assert [(item.day, item.lost) for item in result.sealed] == [(NEXT_DAY, ())]


def test_a_gone_segment_on_one_day_leaves_the_next_day_alone(lake_root):
    gone = _captured_on(lake_root, DAY)[1]
    _captured_on(lake_root, NEXT_DAY)
    gone.unlink()

    result = _run_on(lake_root, NEXT_DAY)

    assert [(item.day, len(item.lost)) for item in result.sealed] == [(DAY, 1), (NEXT_DAY, 0)]


def test_a_ticker_day_with_no_segment_left_reaches_no_refusal(lake_root):
    """The sweep skips a ticker-day it lists no segment for, so only the scrub names it.

    A bucket target also stops at the upload, while the entries have not reached the bucket.
    """
    spy = _captured(lake_root, "SPY")
    for path in spy:
        path.unlink()
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert (result.sealed, result.refused) == ((), ())
    assert events == ["backup", "ping"]
    assert transport.messages == []
    assert _findings(lake_root) == []
    assert scrub(lake_root).missing == tuple(sorted(_rel(lake_root, path) for path in spy))


@pytest.mark.parametrize("damage", ["torn", "emptied"])
def test_an_entry_the_manifest_read_does_not_return_is_not_compared(lake_root, damage):
    """A stated limit, which marketlake #447 owns, so a fix there changes this test.

    The read stops at a torn line, so the entries behind it are hidden, and an emptied
    manifest holds none. Either way the gone segment reads as never recorded, and the day
    seals short. The hash check has the same limit.
    """
    spy = _captured(lake_root, "SPY", count=2)
    manifest = manifest_path(lake_root)
    with manifest.open("ab") as file:
        file.write(b'{"torn')
    at = _et(10, 2)
    outcome = capture.journal_snapshot(
        lake_root,
        journal.CHAINS_SURFACE,
        "SPY",
        body=_CHAIN,
        cycle_start=at,
        fetch_ts=at,
        fetch_end_ts=at,
        pid=PID,
    )
    if damage == "emptied":
        manifest.write_bytes(b"")
    outcome.path.unlink()

    result, events = _run(lake_root)

    assert result.refused == ()
    assert [(item.ticker, item.rows) for item in result.sealed] == [("SPY", 2 * ROWS_PER_SEGMENT)]
    assert events == ["backup", "ping"]
    assert not any(path.exists() for path in spy)


def test_a_segment_with_no_entry_leaves_nothing_to_miss(lake_root):
    """A file the manifest never recorded cannot be missed, so the ticker-day seals."""
    spy = _unmanifested(lake_root)
    spy[2].unlink()

    result, _ = _run(lake_root)

    assert result.refused == ()
    assert [(item.ticker, item.rows) for item in result.sealed] == [("SPY", 4 * ROWS_PER_SEGMENT)]


def _sealed_with_markers_then_restored(lake_root: Path, minutes: list[int]) -> None:
    spy = _captured(lake_root, "SPY")
    saved = {path: path.read_bytes() for path in spy}
    spy[2].unlink()
    _marked(lake_root, "SPY", minutes)
    _run(lake_root)
    spy[0].parent.mkdir(parents=True, exist_ok=True)
    for path, data in saved.items():
        path.write_bytes(data)


def test_a_rebuild_with_fewer_rows_than_the_seal_needs_the_operators_say_so(lake_root):
    """Marker segments have no copy, so three of them outweigh the lost segment's two rows."""
    _sealed_with_markers_then_restored(lake_root, [30, 31, 32])
    clock = ManualClock(_et(17, 0))

    with pytest.raises(RowCountRegression):
        recompact_ticker_day(lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=clock)
    outcome = recompact_ticker_day(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=clock, allow_shrink=True
    )

    assert outcome.rows == 5 * ROWS_PER_SEGMENT


def test_a_rebuild_with_as_many_rows_drops_a_marker_with_no_say_so(lake_root):
    """The guard compares counts alone, so a chains day's many rows pass it."""
    _sealed_with_markers_then_restored(lake_root, [30])

    outcome = recompact_ticker_day(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
    )

    assert outcome.rows == 5 * ROWS_PER_SEGMENT
    assert [kind for kind, _ in _row_kinds(_partition(lake_root, "SPY"))].count("gap") == 0


def test_every_lost_minute_gets_its_own_row(lake_root):
    spy = _captured(lake_root, "SPY")
    spy[1].unlink()
    spy[3].unlink()
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    (sealed,) = result.sealed
    minutes = ["2026-08-24T14:01:00+00:00", "2026-08-24T14:03:00+00:00"]
    assert list(sealed.gaps) == minutes
    assert _gap_minutes(_partition(lake_root, "SPY")) == minutes
    (page,) = transport.messages
    assert "2 manifested segment(s) are missing from disk and 2 minute(s)" in page.body


def test_two_lost_segments_in_one_minute_get_one_row(lake_root):
    spy = _captured(lake_root, "SPY")
    for suffix in ("seg-0-", "seg-1-"):
        rel = _rel(lake_root, spy[0]).replace("seg-", suffix)
        _lost_entry(lake_root, rel, "2026-08-24T10:07:17-04:00")

    result, _ = _run(lake_root)

    (sealed,) = result.sealed
    assert len(sealed.lost) == 2
    assert sealed.gaps == ("2026-08-24T14:07:00+00:00",)


def test_a_finding_that_cannot_be_written_still_pages_a_lost_segment(
    lake_root, monkeypatch, capsys
):
    _captured(lake_root, "SPY")[2].unlink()

    def refuse(*args, **kwargs):
        raise PermissionError("reports is read-only")

    monkeypatch.setattr(compact_module, "write_damaged_segments", refuse)
    publisher, transport = _paging(lake_root)

    result, events = _run(lake_root, publisher=publisher)

    assert [item.ticker for item in result.sealed] == ["SPY"]
    assert events == ["backup", "ping"]
    assert [page.title for page in transport.messages] == [DAMAGED_SEGMENT_TITLE]
    assert "could not be filed: PermissionError" in capsys.readouterr().err


def test_a_refused_day_and_a_lost_segment_send_a_page_each(lake_root):
    _flip(_captured(lake_root, "QQQ")[2], DROPS_THE_SEGMENT)
    _captured(lake_root, "SPY")[2].unlink()
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    assert [item.ticker for item in result.refused] == ["QQQ"]
    assert [item.ticker for item in result.sealed] == ["SPY"]
    (damage, lost) = transport.messages
    assert "refused" in damage.body
    assert "sealed with gaps" in lost.body


def test_a_damage_page_that_raises_cannot_swallow_the_lost_page(lake_root, monkeypatch):
    _captured(lake_root, "SPY")[2].unlink()

    def broken(*args, **kwargs):
        raise RuntimeError("the damage page broke")

    monkeypatch.setattr(compact_module, "_page_damage", broken)
    publisher, transport = _paging(lake_root)

    with pytest.raises(RuntimeError, match="damage page broke"):
        _run(lake_root, publisher=publisher)

    (page,) = transport.messages
    assert "sealed with gaps" in page.body


def test_the_lost_page_stays_under_the_body_limit_however_wide_the_loss():
    found = [_damage(date(2026, 7, 1 + index), 20) for index in range(30)]

    body = compact_module._lost_body(found)

    assert len(body.encode("utf-8")) < 1000
    assert "600 manifested segment(s)" in body
    assert f"and {600 - PAGE_SEGMENT_CAP} more" in body


@pytest.mark.parametrize(
    "stamp",
    ["not-a-time", 5, None, "2026-08-24T10:07:17", "0001-01-01T00:00:00+14:00"],
)
def test_a_stamp_that_names_no_instant_is_none(stamp):
    assert compact_module._instant(stamp) is None


def test_an_unreadable_snap_ts_beside_a_lost_segment_costs_nothing(lake_root):
    """A bad stamp seals when nothing is lost, so a loss must not turn it into a stopped run."""
    spy = _captured(lake_root, "SPY")
    spy[2].unlink()
    _captured(lake_root, "ZZZ")
    batch = journal.gap_rows(
        journal.CHAINS_SURFACE, ticker="SPY", slots=["not-a-time"], error_class="daemon_dead"
    )
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, "20260824T235959000000", PID
    ) as writer:
        writer.write_cycle(batch)

    result, events = _run(lake_root)

    assert sorted(item.ticker for item in result.sealed) == ["SPY", "ZZZ"]
    assert events == ["backup", "ping"]


def test_a_day_sealed_with_gaps_pages_once(lake_root):
    """The day is sealed, so nothing repeats and nothing is left to page about."""
    _captured(lake_root, "SPY")[2].unlink()
    publisher, transport = _paging(lake_root)

    first, _ = _run(lake_root, publisher=publisher)
    second, _ = _run(lake_root, publisher=publisher, at=_et(16, 31))

    assert len(first.sealed) == 1
    assert (second.sealed, second.refused) == ((), ())
    assert len(transport.messages) == 1
    assert len(_findings(lake_root)) == 1


def test_a_sealed_day_rebuilds_past_its_gap_rows_once_every_segment_is_back(lake_root):
    """Sealing at once closes off no repair, which is why no copy is looked for first.

    A live copy of a quotes segment stays readable for about 97 days. With every segment back
    in place, the hand-run repair rebuilds the day from them, on a day with no marker rows.
    """
    spy = _captured_quotes(lake_root, "SPY")
    saved = {path: path.read_bytes() for path in spy}
    whole = sum(journal.read_segment(path).num_rows for path in spy)
    spy[2].unlink()
    _run(lake_root)
    partition = LakePaths(lake_root).partition_path(journal.QUOTES_SURFACE, "SPY", DAY)
    assert len(_gap_minutes(partition)) == 1
    spy[0].parent.mkdir(parents=True)
    for path, data in saved.items():
        path.write_bytes(data)

    outcome = recompact_ticker_day(
        lake_root, journal.QUOTES_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
    )

    assert (outcome.rows, outcome.lost) == (whole, ())
    assert _gap_minutes(partition) == []


def _marked(lake_root: Path, ticker: str, minutes: list[int]) -> None:
    """A startup pass's marker segment for ``minutes`` past 10:00, with no manifest entry."""
    batch = journal.gap_rows(
        journal.CHAINS_SURFACE,
        ticker=ticker,
        slots=[_et(10, minute) for minute in minutes],
        error_class="daemon_dead",
    )
    with journal.SegmentWriter.open(
        lake_root, journal.CHAINS_SURFACE, ticker, DAY, "20260824T235959000000", PID
    ) as writer:
        writer.write_cycle(batch)


def test_a_minute_startup_marking_reached_first_gets_no_second_row(lake_root):
    """Two rows for one minute would read as two records of it."""
    spy = _captured(lake_root, "SPY")
    spy[2].unlink()
    _marked(lake_root, "SPY", [2])

    result, _ = _run(lake_root)

    (sealed,) = result.sealed
    assert [item.segment for item in sealed.lost] == [_rel(lake_root, spy[2])]
    assert sealed.gaps == ()
    assert sealed.rows == 4 * ROWS_PER_SEGMENT + 1
    (finding,) = _findings(lake_root)
    assert [item["segment"] for item in finding["damaged"]] == [_rel(lake_root, spy[2])]
    assert finding["gaps"] == []


def test_a_ticker_day_with_no_segment_left_seals_once_a_daemon_start_marks_it(lake_root):
    """The stop on a wholly lost day ends at the next daemon start, with nothing to do by hand."""
    spy = _captured(lake_root, "SPY")
    for path in spy:
        path.unlink()
    _marked(lake_root, "SPY", list(range(5)))

    result, _ = _run(lake_root)

    (sealed,) = result.sealed
    assert (sealed.rows, len(sealed.lost), sealed.gaps) == (5, 5, ())
    assert scrub(lake_root).missing == ()


def _lost_entry(lake_root: Path, rel: str, fetched_at: str | None) -> None:
    append_manifest(
        lake_root, partition=rel, source="capture", sha256="a" * 64, rows=2, fetched_at=fetched_at
    )


def test_a_lost_minute_is_the_fetch_time_floored_to_the_minute(lake_root):
    spy = _captured(lake_root, "SPY")
    rel = _rel(lake_root, spy[0]).replace("seg-", "seg-0-")
    _lost_entry(lake_root, rel, "2026-08-24T10:07:17.500000-04:00")

    result, _ = _run(lake_root)

    (sealed,) = result.sealed
    assert sealed.gaps == ("2026-08-24T14:07:00+00:00",)


def test_a_fetch_time_with_no_zone_names_no_minute(lake_root):
    """A bare clock reading could land on the wrong day, so it marks nothing."""
    spy = _captured(lake_root, "SPY")
    rel = _rel(lake_root, spy[0]).replace("seg-", "seg-0-")
    _lost_entry(lake_root, rel, "2026-08-24T10:07:17")

    result, _ = _run(lake_root)

    (sealed,) = result.sealed
    assert [item.segment for item in sealed.lost] == [rel]
    assert sealed.gaps == ()


def test_a_read_refusal_names_a_gone_segment_beside_it(lake_root):
    """The same as the hash refusal, for a segment only the read can prove damaged."""
    spy = _unmanifested(lake_root)
    rel = _rel(lake_root, spy[0]).replace("seg-", "seg-0-")
    _lost_entry(lake_root, rel, "2026-08-24T10:07:00-04:00")
    _flip(spy[2], DROPS_THE_SEGMENT)

    result, _ = _run(lake_root)

    (refused,) = result.refused
    assert rel in [item.segment for item in refused.damaged]
    assert len(refused.damaged) == 2


def test_a_lost_entry_with_no_fetch_time_marks_nothing_and_is_still_named(lake_root):
    """No minute can be named for it, and the finding is what keeps it visible."""
    spy = _captured(lake_root, "SPY")
    rel = _rel(lake_root, spy[0]).replace("seg-", "seg-0-")
    append_manifest(
        lake_root, partition=rel, source="capture", sha256="a" * 64, rows=2, fetched_at=None
    )
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    (sealed,) = result.sealed
    assert [item.segment for item in sealed.lost] == [rel]
    assert sealed.gaps == ()
    assert sealed.rows == 5 * ROWS_PER_SEGMENT
    (finding,) = _findings(lake_root)
    assert [item["segment"] for item in finding["damaged"]] == [rel]
    (page,) = transport.messages
    assert "0 minute(s) are marked segment_lost" in page.body


def test_the_page_and_the_summary_count_a_gone_segment_on_its_own(lake_root):
    spy = _captured(lake_root, "SPY")
    gone = spy[1].name
    spy[1].unlink()
    _flip(spy[2], DROPS_THE_SEGMENT)
    publisher, transport = _paging(lake_root)

    result, _ = _run(lake_root, publisher=publisher)

    (line,) = [line for line in result.render().splitlines() if line.startswith("  refused")]
    assert line == (
        "  refused  chains/ticker=SPY/date=2026-08-24.parquet segments=4 kept, damaged, "
        f"sha256 no longer matches: {spy[2].name}; missing from disk: {gone}"
    )
    (page,) = transport.messages
    assert "1 segment(s) no longer match" in page.body
    assert "1 manifested segment(s) are missing from disk" in page.body
    assert "failed to read" not in page.body


def test_the_refusal_names_a_gone_segment_with_what_it_recorded(lake_root):
    spy = _captured(lake_root, "SPY")
    rel = _rel(lake_root, spy[2])
    recorded = latest_entries(lake_root)[rel]["sha256"]
    spy[2].unlink()
    _flip(spy[3], DROPS_THE_SEGMENT)

    with pytest.raises(DamagedSegments) as info:
        recompact_ticker_day(
            lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
        )

    assert f"{rel} (missing from disk, recorded {recorded})" in str(info.value)


def test_the_summary_names_a_day_sealed_with_gaps(lake_root):
    spy = _captured(lake_root, "SPY")
    spy[1].unlink()

    result, _ = _run(lake_root)

    (line,) = [line for line in result.render().splitlines() if line.startswith("  lost")]
    assert line == (
        "  lost     chains/ticker=SPY/date=2026-08-24.parquet gaps=1, "
        f"missing from disk: {spy[1].name}"
    )


def test_the_repair_marks_a_gone_segment_the_way_the_sweep_does(lake_root, capsys):
    """An operator reaches for the repair on an unsealed day, and it must not seal in silence."""
    spy = _captured(lake_root, "SPY")
    gone = spy.pop(2)
    gone.unlink()

    outcome = recompact_ticker_day(
        lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
    )

    assert outcome.rows == 4 * ROWS_PER_SEGMENT + 1
    assert [item.segment for item in outcome.lost] == [_rel(lake_root, gone)]
    assert len(_findings(lake_root)) == 1
    assert "sealed with gaps" in capsys.readouterr().err


def test_the_repair_of_a_sealed_day_leaves_the_count_to_the_no_shrink_guard(lake_root):
    """A sealed day's segment entries are superseded, so their absence refuses nothing.

    Sealing unlinks all five segments. One is put back, as a read-back from the live prefix
    would. The other four are gone by design, and refusing on them would take away the only
    repair a sealed day has. The guard refuses the short rebuild, and ``allow_shrink`` is the
    operator's say-so.
    """
    spy = _captured(lake_root, "SPY")
    back = spy[0].read_bytes()
    _run(lake_root)
    spy[0].parent.mkdir(parents=True)
    spy[0].write_bytes(back)

    with pytest.raises(RowCountRegression):
        recompact_ticker_day(
            lake_root, journal.CHAINS_SURFACE, "SPY", DAY, clock=ManualClock(_et(17, 0))
        )
    outcome = recompact_ticker_day(
        lake_root,
        journal.CHAINS_SURFACE,
        "SPY",
        DAY,
        clock=ManualClock(_et(17, 1)),
        allow_shrink=True,
    )

    assert outcome.rows == ROWS_PER_SEGMENT
