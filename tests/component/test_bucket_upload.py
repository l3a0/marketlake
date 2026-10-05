"""The nightly upload to a bucket target, against the fake S3 client.

``lake.bucket.nightly_upload`` runs inside compaction's lake-root lock wherever a bucket
target replaces the ``rsync`` copy. Marketlake #639 lists the rules it keeps. Each rule
that can fail has a test here.

1. What is new: every manifested file past the watermark, and every file with no
   manifest entry whose size differs from the bucket's.
2. No usable watermark refuses with one line naming the first-upload command.
3. Segments upload unless their compacted partition is manifested, and any other
   manifested file missing from disk refuses.
4. Every PUT carries the manifest's digest as base64, sets Standard-IA, and is never a
   multipart upload. A file past the single-PUT limit fails loudly.
5. Partitions go first and ``manifest.jsonl`` last.
6. A second run uploads nothing.
7. Nothing is ever deleted.
8. A deadline stops the upload and the lock is released.
9. Nothing is written under the lake root.

The exclusion list, rule 10, has its own file, ``tests/unit/test_bucket_exclusions.py``.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake import bucket
from lake.bucket import (
    FIRST_UPLOAD_COMMAND,
    NIGHTLY_UPLOAD_BUDGET,
    STORAGE_CLASS,
    ChecksumRefused,
    FirstUploadRefused,
    ManifestedFileMissing,
    ObjectTooLarge,
    UploadDeadline,
    WatermarkMissing,
    first_upload,
    nightly_upload,
)
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.control_plane import COMPACTION_RUN, VENDOR_SWEEP
from lake.lock import lake_lock
from lake.manifest import append_manifest, manifest_path, sha256_file
from lake.paths import LakePaths
from tests.support.bucket import FakeS3
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

DAY = date(2026, 8, 24)
NEXT = date(2026, 8, 25)
NEXT_WEEK = date(2026, 8, 31)
TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
# The week's sessions, so the next session after an evening upload is the next morning.
CALENDAR = weekday_sessions(DAY)
# A Monday evening, well clear of the Sunday window the first upload refuses.
EVENING = datetime(2026, 8, 24, 16, 40, tzinfo=MARKET_TZ)


def _clock() -> ManualClock:
    return ManualClock(EVENING)


def _lake(root: Path) -> Path:
    """A lake with sealed partitions, both other ledgers, and the unmanifested kinds."""
    lake = (
        FixtureLake(root)
        .with_chains("SPY", DAY)
        .with_quotes("SPY", DAY)
        .with_reference("security_master", sample_chains_table())
        .with_quarantine({"partition": "chains/ticker=SPY/date=2026-08-24.parquet"})
        .build()
    )
    report = lake / "reports" / f"date={DAY.isoformat()}.md"
    report.parent.mkdir(parents=True)
    report.write_text("nightly report\n")
    metadata = lake / "journal" / "metadata.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text('{"stamped_at": null}\n')
    return lake


def _seed(lake: Path, client: FakeS3) -> None:
    """Put a lake in the bucket the way the first upload would."""
    first_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    client.calls.clear()


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _key(rel: str) -> str:
    return TARGET.key(rel)


def _seal_another_day(lake: Path, day: date = NEXT) -> str:
    """Seal a new partition the way compaction would, and return its path."""
    path = LakePaths(lake).quotes_partition_path("SPY", day)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(sample_quotes_table(), path)
    rel = path.relative_to(lake).as_posix()
    append_manifest(
        lake, partition=rel, source="compaction", sha256=sha256_file(path), rows=1, fetched_at=None
    )
    return rel


# -- 2. no usable watermark ----------------------------------------------------


def test_an_empty_bucket_refuses_with_the_first_upload_command_and_puts_nothing(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    with pytest.raises(WatermarkMissing) as refused:
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    message = str(refused.value)
    assert FIRST_UPLOAD_COMMAND in message
    assert "\n" not in message
    assert client.puts() == []


def test_a_copy_that_is_not_a_prefix_refuses_and_puts_nothing(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    # A hand repair: the bucket's copy now differs in its first byte.
    raw = manifest_path(lake).read_bytes()
    client.store(_key("manifest.jsonl"), b"X" + raw[1:])
    with pytest.raises(WatermarkMissing, match="not a prefix"):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert client.puts() == []


def test_a_copy_longer_than_the_lake_is_not_a_prefix(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    client.store(_key("manifest.jsonl"), manifest_path(lake).read_bytes() + b'{"x": 1}\n')
    with pytest.raises(WatermarkMissing):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)


def test_a_copy_stored_without_a_checksum_proves_no_prefix(tmp_path):
    # A copy some other tool put there carries no SHA-256, so the length alone would be
    # trusted. It must read as unproved rather than as a prefix.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    client.store(_key("manifest.jsonl"), manifest_path(lake).read_bytes(), checksum=None)
    with pytest.raises(WatermarkMissing):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)


@pytest.mark.parametrize("length", [0, 1, 40])
def test_a_copy_with_no_whole_entry_refuses_rather_than_upload_the_whole_lake(tmp_path, length):
    # Zero bytes, or a stub shorter than the first line, is a prefix of any manifest. Its
    # watermark is 0, so without the refusal every partition would go up inside
    # compaction's lock.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    raw = manifest_path(lake).read_bytes()
    assert length < raw.index(b"\n")
    client.store(_key("manifest.jsonl"), raw[:length])
    client.calls.clear()
    with pytest.raises(WatermarkMissing) as refused:
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    message = str(refused.value)
    assert FIRST_UPLOAD_COMMAND in message and "no whole entry" in message
    assert "\n" not in message
    assert client.puts() == []


# -- 1, 5, 6. what goes up, in what order, and only once -------------------------


def test_a_new_partition_goes_up_before_the_manifest_and_nothing_else_moves(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert client.put_keys() == [_key(rel), _key("manifest.jsonl")]
    assert client.body(_key("manifest.jsonl")) == manifest_path(lake).read_bytes()


def test_a_second_run_uploads_nothing_and_adds_no_version(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    _seal_another_day(lake)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    versions = {key: len(client.versions(key)) for key in client.keys()}
    client.calls.clear()

    summary = nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert client.puts() == []
    assert summary.puts == 0
    assert {key: len(client.versions(key)) for key in client.keys()} == versions


def test_the_digest_goes_up_as_base64_of_the_raw_bytes(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    sent = next(put for put in client.puts() if put["Key"] == _key(rel))
    hexdigest = sha256_file(lake / rel)
    assert base64.b64decode(sent["ChecksumSHA256"]) == bytes.fromhex(hexdigest)


def test_the_fake_refuses_a_digest_sent_as_hex(tmp_path):
    # Guard the guard. The uploader's conversion is only covered if the fake refuses
    # the manifest's hex digest sent as it is, the way S3 does.
    client = FakeS3()
    data = b"bytes"
    with pytest.raises(Exception) as refused:
        client.put_object(
            Bucket="lake-backup",
            Key="k",
            Body=data,
            ChecksumSHA256=hashlib.sha256(data).hexdigest(),
        )
    assert refused.value.response["Error"]["Code"] == "InvalidRequest"


def test_every_put_is_standard_ia_and_carries_a_sha256(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    _seal_another_day(lake)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    puts = client.puts()
    assert puts
    assert all(put["StorageClass"] == STORAGE_CLASS for put in puts)
    assert all("ChecksumSHA256" in put for put in puts)
    # One PutObject per file. The fake has no multipart call, so reaching for one fails.
    assert not hasattr(client, "create_multipart_upload")


def test_an_object_already_holding_the_digest_is_not_sent_again(tmp_path):
    # An interrupted night left the partition in the bucket without the manifest copy
    # moving. The next night finds it by checksum and does not make a second version.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    client.store(_key(rel), (lake / rel).read_bytes())

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert client.put_keys() == [_key("manifest.jsonl")]
    assert len(client.versions(_key(rel))) == 1


def test_a_composite_checksum_in_the_bucket_is_not_a_match(tmp_path):
    # A multipart upload by another tool stores a checksum of parts. It proves nothing
    # about the whole file, so the uploader sends the file again.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    digest = base64.b64encode(hashlib.sha256((lake / rel).read_bytes()).digest()).decode()
    client.store(
        _key(rel), (lake / rel).read_bytes(), checksum=f"{digest}-2", checksum_type="COMPOSITE"
    )

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert _key(rel) in client.put_keys()


def test_an_object_with_no_stored_checksum_is_sent_again(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    client.store(_key(rel), (lake / rel).read_bytes(), checksum=None)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert _key(rel) in client.put_keys()


def test_every_unmanifested_file_goes_up_whatever_its_kind(tmp_path):
    # The rule is every unmanifested file not excluded, never a list of kinds. A file
    # kind the lake grows later, such as a database file, must go up with no edit here.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    (lake / "ledger.sqlite").write_bytes(b"a new kind of file")
    (lake / "reports" / "alerts").mkdir()
    (lake / "reports" / "alerts" / "page.json").write_text("{}\n")

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert set(client.put_keys()) == {_key("ledger.sqlite"), _key("reports/alerts/page.json")}


def test_an_unmanifested_file_goes_up_again_when_its_size_moves(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    (lake / "journal" / "metadata.json").write_text('{"stamped_at": "2026-08-24T20:31:00Z"}\n')
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert client.put_keys() == [_key("journal/metadata.json")]


def test_nothing_is_ever_deleted(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    (lake / "reports" / f"date={DAY.isoformat()}.md").unlink()
    before = client.keys()
    _seal_another_day(lake)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert set(before) <= set(client.keys())
    assert not any(name.startswith("delete") for name, _ in client.calls)


# -- 3. segments, and a manifested file missing from disk ------------------------


def _manifest_segment(lake: Path, day: date) -> str:
    """Write a journal segment for ``day`` and record it the way capture does."""
    path = FixtureLake(lake).segment_path("chains", "SPY", day, "20260824T133000Z", 4242)
    FixtureLake(lake).with_journal_segment(
        "chains", "SPY", day, sample_chains_table(), start_ts="20260824T133000Z", pid=4242
    )
    rel = path.relative_to(lake).as_posix()
    append_manifest(
        lake, partition=rel, source="capture", sha256=sha256_file(path), rows=1, fetched_at=None
    )
    return rel


def test_a_refused_days_segments_upload_because_they_are_its_only_copy(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    # No compacted partition for this day is manifested: compaction refused it.
    rel = _manifest_segment(lake, NEXT)
    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert _key(rel) in client.put_keys()


def test_a_segment_whose_partition_is_manifested_is_skipped_even_when_gone(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _manifest_segment(lake, NEXT)
    # Compaction sealed the chains day and unlinked the segment.
    sealed = LakePaths(lake).chains_partition_path("SPY", NEXT)
    sealed.parent.mkdir(parents=True, exist_ok=True)
    sealed.write_bytes(b"sealed chains")
    partition = sealed.relative_to(lake).as_posix()
    append_manifest(
        lake,
        partition=partition,
        source="compaction",
        sha256=sha256_file(lake / partition),
        rows=1,
        fetched_at=None,
    )
    (lake / rel).unlink()

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert _key(rel) not in client.put_keys()
    assert _key(partition) in client.put_keys()


def test_a_manifested_file_missing_from_disk_refuses_before_the_manifest(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    (lake / rel).unlink()
    with pytest.raises(ManifestedFileMissing, match=rel):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert _key("manifest.jsonl") not in client.put_keys()


# -- 4. refusals S3 and the size limit make --------------------------------------


def test_a_file_that_rotted_after_sealing_is_refused_and_the_manifest_stays(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    (lake / rel).write_bytes(b"rotted bytes")
    with pytest.raises(ChecksumRefused, match=rel):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert len(client.versions(_key(rel))) == 0
    assert _key("manifest.jsonl") not in client.put_keys()


def test_a_file_past_the_single_put_limit_fails_loudly(tmp_path, monkeypatch):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    rel = _seal_another_day(lake)
    monkeypatch.setattr(bucket, "MAX_PUT_BYTES", (lake / rel).stat().st_size - 1)
    with pytest.raises(ObjectTooLarge, match=rel):
        nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert client.puts() == []


# -- 8. the deadline -------------------------------------------------------------


def test_the_budget_is_derived_from_the_schedule():
    # 16:30 to 18:30 is 120 minutes, less 15 for the seal, 15 for the PUT in flight and
    # 15 of margin before the sweep.
    span = timedelta(
        hours=VENDOR_SWEEP.hour - COMPACTION_RUN.hour,
        minutes=VENDOR_SWEEP.minute - COMPACTION_RUN.minute,
    )
    assert span == timedelta(minutes=120)
    assert NIGHTLY_UPLOAD_BUDGET == timedelta(minutes=75)


def test_the_deadline_stops_the_upload_before_the_manifest_and_frees_the_lock(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    first = _seal_another_day(lake)
    (lake / "ledger.sqlite").write_bytes(b"x")
    clock = _clock()
    # The first PUT takes the whole budget, so the next request is past the deadline.
    client.on_put = lambda kwargs, data: clock.advance(NIGHTLY_UPLOAD_BUDGET.total_seconds())

    with pytest.raises(UploadDeadline):
        with lake_lock(lake):
            nightly_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)

    assert client.put_keys() == [_key(first)]
    # The lock is free: a non-blocking take succeeds at once.
    fd = os.open(manifest_path(lake), os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(fd)


def test_an_upload_inside_the_budget_finishes(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    _seal_another_day(lake)
    clock = _clock()
    client.on_put = lambda kwargs, data: clock.advance(NIGHTLY_UPLOAD_BUDGET.total_seconds() * 0.4)
    nightly_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)
    assert client.put_keys()[-1] == _key("manifest.jsonl")


# -- 8. the session bounds the deadline too ----------------------------------------


class _TimedS3(FakeS3):
    """A fake bucket that records the clock reading at every request it receives."""

    def __init__(self, clock: ManualClock, **kwargs) -> None:
        super().__init__(**kwargs)
        self.clock = clock
        self.stamps: list[datetime] = []

    def _enter(self, name, kwargs):
        self.stamps.append(self.clock.now())
        super()._enter(name, kwargs)


def _et(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=MARKET_TZ)


@pytest.mark.parametrize(
    "moment",
    [
        _et(NEXT, 11, 0),  # mid-session
        _et(NEXT, 9, 0),  # past the bound, before the open
        _et(NEXT, 16, 19),  # past the option close, before close+5
    ],
)
def test_an_upload_started_inside_a_session_sends_no_request(tmp_path, moment):
    lake = _lake(tmp_path / "lake")
    seeded = FakeS3()
    _seed(lake, seeded)
    _seal_another_day(lake)
    clock = ManualClock(moment)
    client = _TimedS3(clock)
    client.objects = seeded.objects

    with pytest.raises(UploadDeadline, match="next session's capture start"):
        nightly_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)

    assert client.calls == []


def test_the_session_bound_is_the_open_less_the_in_flight_put_and_the_margin():
    clock = ManualClock(EVENING)
    bound = bucket.session_bound(EVENING, clock=clock, calendar=CALENDAR)
    assert bound == _et(NEXT, 9, 30) - bucket.IN_FLIGHT_ALLOWANCE - bucket.PRE_OPEN_MARGIN
    assert bound == _et(NEXT, 9, 0)
    # Past the bound and up to close+5 the session itself is the answer, and its open
    # has passed. After close+5 the next session's is.
    assert bucket.session_bound(_et(NEXT, 16, 19), clock=clock, calendar=CALENDAR) == bound
    later = bucket.session_bound(_et(NEXT, 16, 21), clock=clock, calendar=CALENDAR)
    assert later == _et(NEXT, 9, 0) + timedelta(days=1)
    # A Friday evening reads Monday's open, and a calendar with no session reads none.
    friday = _et(date(2026, 8, 28), 18, 0)
    monday = bucket.session_bound(friday, clock=clock, calendar=weekday_sessions(DAY, NEXT_WEEK))
    assert monday == _et(NEXT_WEEK, 9, 0)
    assert bucket.session_bound(friday, clock=clock, calendar=weekday_sessions(DAY)) is None


def test_an_upload_started_before_the_session_stops_before_it_opens(tmp_path):
    # Ten minutes before the bound, with each PUT taking six. The deadline is the bound,
    # not the 75-minute budget, so the upload stops before 09:00 and the open is never
    # reached with the lock held. The manifest never goes up.
    lake = _lake(tmp_path / "lake")
    seeded = FakeS3()
    _seed(lake, seeded)
    for offset in range(3):
        _seal_another_day(lake, day=NEXT_WEEK + timedelta(days=offset))
    clock = ManualClock(_et(NEXT, 8, 50))
    client = _TimedS3(clock)
    client.objects = seeded.objects
    client.on_put = lambda kwargs, data: clock.advance(6 * 60)

    with pytest.raises(UploadDeadline, match="next session's capture start"):
        with lake_lock(lake):
            nightly_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)

    assert len(client.puts()) == 2
    assert _key("manifest.jsonl") not in client.put_keys()
    assert all(stamp < _et(NEXT, 9, 0) for stamp in client.stamps)
    fd = os.open(manifest_path(lake), os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(fd)


def test_the_first_upload_keeps_its_unlocked_phase_and_stops_at_the_lock(tmp_path):
    # Started mid-session, the unlocked phase still uploads the sealed files, since it
    # blocks nothing. The locked phase then finds the bound passed and sends nothing,
    # so no unmanifested file and no manifest go up.
    lake = _lake(tmp_path / "lake")
    clock = ManualClock(_et(NEXT, 11, 0))
    client = _TimedS3(clock)

    with pytest.raises(UploadDeadline, match="next session's capture start"):
        first_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)

    keys = client.put_keys()
    assert _key("chains/ticker=SPY/date=2026-08-24.parquet") in keys
    assert _key("manifest.jsonl") not in keys
    assert _key(f"reports/date={DAY.isoformat()}.md") not in keys


def _lock_held(lake: Path) -> bool:
    """Whether some descriptor holds the lake-root lock, by a non-blocking take."""
    fd = os.open(manifest_path(lake), os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def test_the_first_upload_stops_its_locked_phase_at_the_bound(tmp_path):
    # Begun ten minutes before the bound. The unlocked phase takes no time here, and
    # each PUT made under the lock takes six minutes, so the locked phase stops at the
    # bound partway through, before manifest.jsonl, and frees the lock.
    lake = _lake(tmp_path / "lake")
    clock = ManualClock(_et(NEXT, 8, 50))
    client = _TimedS3(clock)
    locked: list[datetime] = []

    def timed(kwargs, data):
        if _lock_held(lake):
            locked.append(clock.now())
            clock.advance(6 * 60)

    client.on_put = timed

    with pytest.raises(UploadDeadline, match="next session's capture start"):
        first_upload(lake, TARGET, client=client, clock=clock, calendar=CALENDAR)

    assert len(locked) == 2
    assert all(stamp < _et(NEXT, 9, 0) for stamp in locked)
    assert _key("manifest.jsonl") not in client.put_keys()
    assert not _lock_held(lake)


# -- 9. nothing under the lake root ----------------------------------------------


def test_an_upload_and_a_scrub_leave_the_lake_byte_identical(tmp_path):
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    _seal_another_day(lake)
    before = _snapshot(lake)

    nightly_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    bucket.bucket_scrub(lake, TARGET, client)
    first_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert _snapshot(lake) == before


# -- a lake whose manifest is missing ---------------------------------------------


@pytest.mark.parametrize("state", ["absent", "empty"])
def test_a_lake_without_a_manifest_refuses_beside_a_seeded_bucket(tmp_path, state):
    # A wrong lake_root reads this way. Uploading would replace the bucket's manifest
    # with an empty one, which every later night would then build on. The refusal comes
    # before the lock, because taking the lock would create manifest.jsonl right here.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    _seed(lake, client)
    wrong = tmp_path / "wrong"
    wrong.mkdir()
    if state == "empty":
        (wrong / "manifest.jsonl").write_bytes(b"")
    before = client.body(_key("manifest.jsonl"))

    with pytest.raises(FirstUploadRefused) as refused:
        first_upload(wrong, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    message = str(refused.value)
    assert "lake_root" in message and "\n" not in message
    assert client.puts() == []
    assert client.body(_key("manifest.jsonl")) == before
    assert (wrong / "manifest.jsonl").exists() == (state == "empty")


def test_a_missing_lake_root_refuses(tmp_path):
    client = FakeS3()
    with pytest.raises(FirstUploadRefused, match="not a directory"):
        first_upload(tmp_path / "nowhere", TARGET, client=client, clock=_clock(), calendar=CALENDAR)
    assert client.puts() == []


# -- the first upload's locked phase -----------------------------------------------


def test_the_first_upload_takes_the_lock_for_its_last_phase_only(tmp_path):
    # Sealed files go up with the lock free, since each PUT carries its digest. The
    # files with no manifest entry and manifest.jsonl go up under it, so no seal can
    # land between the last re-read of the manifest and the copy that claims it.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    held: dict[str, bool] = {}
    client.on_put = lambda kwargs, data: held.setdefault(kwargs["Key"], _lock_held(lake))

    first_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    assert held[_key("manifest.jsonl")] is True
    assert held[_key(f"reports/date={DAY.isoformat()}.md")] is True
    assert held[_key("chains/ticker=SPY/date=2026-08-24.parquet")] is False
    assert not _lock_held(lake)


def test_an_entry_the_manifest_gains_mid_run_goes_up_before_the_manifest(tmp_path):
    # A seal landing during the unlocked phase adds a manifest entry the first pass never
    # saw. The uploaded manifest claims it, so the locked phase must upload it first, or
    # the bucket would claim a file it does not hold.
    lake = _lake(tmp_path / "lake")
    client = FakeS3()
    sealed: list[str] = []

    def seal_once(kwargs, data):
        if not sealed:
            sealed.append(_seal_another_day(lake))

    client.on_put = seal_once

    first_upload(lake, TARGET, client=client, clock=_clock(), calendar=CALENDAR)

    keys = client.put_keys()
    assert _key(sealed[0]) in keys
    assert keys.index(_key(sealed[0])) < keys.index(_key("manifest.jsonl"))
    assert client.body(_key("manifest.jsonl")) == manifest_path(lake).read_bytes()
    assert client.body(_key(sealed[0])) == (lake / sealed[0]).read_bytes()
