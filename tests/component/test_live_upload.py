"""The live upload: each quotes segment goes to the bucket as its manifest entry lands.

``lake.live_upload`` tails ``manifest.jsonl`` on a thread of the daemon and PUTs each
manifested quotes segment under the sibling prefix ``<prefix>-live/`` (marketlake #943).
These run it over a real lake on disk, with the S3 client faked by ``FakeS3``, which models
the create-only PUT's 412 and S3's ``BadDigest`` refusal. Most cases call ``pass_once``
directly, so no thread runs and nothing waits. The daemon cases run the production entry
with a manual clock and a real upload thread, polling every millisecond.

Twelve behaviours are covered, the first eleven numbered as marketlake #943's build names
them.

1. A manifested quotes segment goes up under the live prefix, create-only, carrying its
   manifest digest as ``ChecksumSHA256``, the ``STANDARD`` class and its entry as metadata.
2. A second upload of a key is refused. A matching HEAD counts it as already there and sends
   no second PUT. A HEAD that differs, or a ``BadDigest``, refuses the key for good and pages.
   A HEAD that fails keeps the segment owed.
3. The upload never takes the lake-root lock, and the module imports no lock.
4. A bucket that fails every request for a whole session costs no cycle, pages once after
   ten minutes, PUTs nothing outside the live prefix, and prints its recovery line.
5. A bars import manifested between two segments is never sent, and neither is
   ``manifest.jsonl``.
6. A chains segment and a segment with no manifest entry stay at home.
7. A segment whose partition is sealed, or whose file is gone from a sealed day, is dropped
   as superseded. A file gone from a day not yet sealed is counted as vanished.
8. The manifest is read in whole lines, however the reads split them. A manifest cut back
   in place is read from the start again, and nothing already in the bucket is sent twice.
9. Only a primary with a prefixed bucket target starts an upload. On that host a target
   with no prefix, or a client that cannot be built, starts none and pages once while the
   loop runs on.
10. The client is built on the caller's thread before the first cycle.
11. The lag command's arithmetic, over a fake listing and through its ``main``.
12. The alarm: when it pages, at most once an Eastern date for falling behind, and never
    held up by a request in flight.
"""

from __future__ import annotations

import ast
import base64
import hashlib
import json
import os
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, daemon, journal, live_upload
from lake.alert import Message, Publisher
from lake.bucket import BucketSettingsInvalid
from lake.calendar import MARKET_TZ
from lake.capture import CycleResult, SegmentOutcome
from lake.config import BucketTarget, load_config
from lake.gap import SEGMENT_STAMP_FORMAT
from lake.live_upload import (
    BEHIND_EVENT,
    BEHIND_TITLE,
    LIVE_SUFFIX,
    NOT_STARTED_EVENT,
    NOT_STARTED_TITLE,
    REFUSED_EVENT,
    LiveUpload,
    NotStarted,
    UploadAlarm,
    live_target,
    prepare,
    retry_delay,
    segment_start,
)
from lake.lock import lake_lock
from lake.manifest import append_manifest, manifest_path
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.pinger import FakePinger
from tests.support.schema_version import record_running_version

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
LIVE = BucketTarget(bucket="lake-backup", prefix="lake-live")
S3_TARGET = "s3://lake-backup/lake"

WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)
NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)

LIVE_UPLOAD_SOURCE = Path(live_upload.__file__)


# -- helpers ------------------------------------------------------------------------


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "lake"
    root.mkdir()
    return root


def _write_segment(
    root: Path,
    *,
    ticker: str = "SPY",
    surface: str = "quotes",
    start: datetime = NOW,
    pid: int = 4242,
    body: bytes | None = None,
    source: str = "capture",
    fetched_at: str | None = "2026-09-02T14:00:01+00:00",
    manifest: bool = True,
) -> tuple[str, str]:
    """A segment file on disk, with its manifest entry unless ``manifest`` is false."""
    stamp = start.strftime(SEGMENT_STAMP_FORMAT)
    rel = (
        f"journal/date={start.date().isoformat()}/surface={surface}/ticker={ticker}/"
        f"seg-{stamp}-{pid}.arrows"
    )
    data = body if body is not None else f"{rel} bytes".encode()
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    if manifest:
        append_manifest(
            root, partition=rel, source=source, sha256=digest, rows=3, fetched_at=fetched_at
        )
    return rel, digest


def _upload(root: Path, client: FakeS3, clock: ManualClock | None = None) -> LiveUpload:
    """An upload whose passes the test runs itself, reading as alive to the alarm.

    No thread is started, so the test's own thread stands for it. A case about a thread
    that stopped starts a real one instead.
    """
    upload = LiveUpload(
        lake_root=root, target=LIVE, client=client, clock=clock or ManualClock(start=NOW)
    )
    upload._thread = threading.current_thread()
    return upload


def _b64(hexdigest: str) -> str:
    return base64.b64encode(bytes.fromhex(hexdigest)).decode()


class _Recording:
    """A ``Transport`` that keeps each page, and the clock's time it arrived at."""

    def __init__(self, clock: ManualClock | None = None) -> None:
        self.sent: list[Message] = []
        self.at: list[datetime] = []
        self._clock = clock

    def send(self, message: Message) -> None:
        self.sent.append(message)
        if self._clock is not None:
            self.at.append(self._clock.now())


def _publisher(root: Path, transport: _Recording) -> Publisher:
    return Publisher(lake_root=root, transport=transport)


def _wait_until(condition, seconds: float = 10.0) -> None:
    """Spend real time, bounded, until a condition another thread makes true holds."""
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("the upload thread never got there")
        time.sleep(0.001)


def _unreadable(monkeypatch, path: Path) -> None:
    """Make one file's read fail as a file owned by another user does, and no other's."""
    original = Path.read_bytes

    def read_bytes(self: Path) -> bytes:
        if self == path:
            raise PermissionError(13, "Permission denied", str(self))
        return original(self)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)


def _seal(root: Path, ticker: str = "SPY") -> None:
    """Record the day's quotes partition in the manifest, as compaction does when it seals."""
    append_manifest(
        root,
        partition=f"quotes/ticker={ticker}/date=2026-09-02.parquet",
        source="compaction",
        sha256="cd" * 32,
        rows=3,
        fetched_at=None,
    )


# The page bodies' shared sentences, as literals, so a constant cannot check itself.
COST = (
    "Until the upload catches up, the runtime cannot see today's session in the bucket. "
    "Capture and the nightly upload are unaffected."
)
LOG = "journalctl -u com.marketlake.daemon.service | grep live_upload"
RESTART_CAUTION = "A restart mid-session can cost a captured minute"


# -- the wire shape -----------------------------------------------------------------


def test_the_events_titles_and_timings_match_the_design_literally():
    """Compared against the literals, because the constants cannot check themselves."""
    assert live_upload.BEHIND_EVENT == "segment_upload_behind"
    assert live_upload.REFUSED_EVENT == "segment_upload_refused"
    assert live_upload.NOT_STARTED_EVENT == "segment_upload_not_started"
    assert live_upload.BEHIND_TITLE == "Live upload behind"
    assert live_upload.REFUSED_TITLE == "Live upload refused a segment"
    assert live_upload.NOT_STARTED_TITLE == "Live upload not started"
    assert live_upload.POLL_SECONDS == 5.0
    assert live_upload.BACKOFF_CAP_SECONDS == 60.0
    assert live_upload.BEHIND_AFTER == timedelta(minutes=10)


def test_the_stamp_the_lag_reads_is_the_stamp_capture_writes():
    """A segment named the way capture names one reads back as the cycle's own start."""
    assert capture._SEGMENT_STAMP_FORMAT == SEGMENT_STAMP_FORMAT
    start = datetime(2026, 9, 2, 14, 0, 3, 250000, tzinfo=UTC)
    path = journal.segment_path(
        Path("/lake"), "quotes", "SPY", DAY, start.strftime(capture._SEGMENT_STAMP_FORMAT), 77
    )
    rel = path.relative_to(Path("/lake")).as_posix()
    assert segment_start(rel) == start


# -- 1. a segment goes up under the live prefix, create-only ----------------------


def test_a_manifested_segment_goes_up_under_the_live_prefix_create_only(tmp_path):
    root = _root(tmp_path)
    rel, digest = _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)

    assert upload.pass_once() is None

    (put,) = fake.puts()
    assert put["Bucket"] == "lake-backup"
    assert put["Key"] == f"lake-live/{rel}"
    assert put["IfNoneMatch"] == "*"
    assert put["ChecksumSHA256"] == _b64(digest)
    assert put["StorageClass"] == "STANDARD"
    assert put["Metadata"] == {
        "sha256": digest,
        "rows": "3",
        "fetched_at": "2026-09-02T14:00:01+00:00",
        "source": "capture",
    }
    assert fake.body(f"lake-live/{rel}") == (root / rel).read_bytes()
    assert fake.versions(f"lake-live/{rel}")[0].metadata == put["Metadata"]
    state = upload.state()
    assert (state.uploaded, state.owed, state.already_there) == (1, 0, 0)


def test_an_entry_without_fetched_at_leaves_that_field_out_of_the_metadata(tmp_path):
    root = _root(tmp_path)
    _write_segment(root, fetched_at=None, source="close-fill")
    fake = FakeS3()

    _upload(root, fake).pass_once()

    (put,) = fake.puts()
    assert set(put["Metadata"]) == {"sha256", "rows", "source"}
    assert put["Metadata"]["source"] == "close-fill"


def test_the_metadata_keeps_a_zero_and_drops_a_bool():
    assert live_upload._metadata({"sha256": "ab", "rows": 0, "source": "capture"}) == {
        "sha256": "ab",
        "rows": "0",
        "source": "capture",
    }
    assert live_upload._metadata({"rows": True}) == {}


def test_a_segment_already_sent_this_process_is_not_sent_again(tmp_path):
    root = _root(tmp_path)
    _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)

    upload.pass_once()
    upload.pass_once()
    rel, _ = _write_segment(root, start=NOW + timedelta(minutes=1))
    upload.pass_once()

    assert len(fake.puts()) == 2
    assert fake.put_keys()[-1] == f"lake-live/{rel}"


# -- 2. a second upload of a key is refused -----------------------------------------


def test_a_key_the_bucket_already_holds_counts_as_already_there(tmp_path):
    """A restart meets every segment it sent before as a 412 whose HEAD matches."""
    root = _root(tmp_path)
    rel, digest = _write_segment(root)
    fake = FakeS3()
    fake.store(f"lake-live/{rel}", (root / rel).read_bytes())

    upload = _upload(root, fake)
    assert upload.pass_once() is None
    upload.pass_once()

    assert [name for name, _ in fake.calls] == ["put_object", "head_object"]
    head = fake.calls[1][1]
    assert head == {"Bucket": "lake-backup", "Key": f"lake-live/{rel}", "ChecksumMode": "ENABLED"}
    assert len(fake.versions(f"lake-live/{rel}")) == 1
    state = upload.state()
    assert (state.already_there, state.uploaded, state.owed, state.refused) == (1, 0, 0, ())


def test_a_key_holding_other_bytes_is_refused_for_good_and_pages_once(tmp_path, capsys):
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    fake = FakeS3()
    fake.store(f"lake-live/{rel}", b"someone else's bytes")
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)

    upload.pass_once()
    alarm.check(NOW)
    upload.pass_once()
    alarm.check(NOW + timedelta(minutes=1))

    # One PUT and one HEAD, then nothing: a refused key is never retried.
    assert [name for name, _ in fake.calls] == ["put_object", "head_object"]
    assert fake.body(f"lake-live/{rel}") == b"someone else's bytes"
    assert upload.state().refused == (rel,)
    (page,) = transport.sent
    assert page.event == "segment_upload_refused"
    assert page.title == "Live upload refused a segment"
    assert rel in page.body
    assert LOG in page.body
    assert "runtime cannot see those minutes in the bucket" in page.body
    assert "lake-backup" not in page.body
    assert f"{rel} refused: the bucket already holds other bytes" in capsys.readouterr().err


def test_bytes_that_no_longer_match_their_entry_are_refused_for_good(tmp_path):
    """Rot after the entry was written: S3 answers ``BadDigest`` and nothing lands."""
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    (root / rel).write_bytes(b"rotted")
    fake = FakeS3()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()

    upload.pass_once()
    upload.pass_once()
    UploadAlarm(upload, _publisher(root, transport), clock).check(NOW)

    assert len(fake.puts()) == 1
    assert fake.keys() == []
    assert upload.state().refused == (rel,)
    assert [message.event for message in transport.sent] == [REFUSED_EVENT]


def test_a_key_already_there_whose_head_fails_is_neither_counted_nor_refused(tmp_path):
    """A 412 says the key exists, and only the HEAD says whose bytes it holds."""
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    fake = FakeS3()
    fake.store(f"lake-live/{rel}", b"someone else's bytes")
    upload = _upload(root, fake)
    head = fake.head_object
    fake.head_object = lambda **kwargs: (_ for _ in ()).throw(unreachable())

    failed = upload.pass_once()

    assert failed is not None and "already in the bucket, and its HEAD was" in failed
    state = upload.state()
    assert (state.owed, state.already_there, state.uploaded, state.refused) == (1, 0, 0, ())

    fake.head_object = head
    assert upload.pass_once() is None
    state = upload.state()
    assert (state.owed, state.already_there, state.refused) == (0, 0, (rel,))


def test_an_entry_without_a_usable_sha256_is_refused_without_a_request(tmp_path):
    """62 hex characters are no SHA-256, and the segment after it still goes up."""
    root = _root(tmp_path)
    rel = "journal/date=2026-09-02/surface=quotes/ticker=SPY/seg-20260902T140000000000-1.arrows"
    (root / rel).parent.mkdir(parents=True)
    (root / rel).write_bytes(b"x")
    append_manifest(
        root, partition=rel, source="capture", sha256="ab" * 31, rows=1, fetched_at=None
    )
    good, _ = _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    upload = _upload(root, fake)

    assert upload.pass_once() is None

    assert fake.put_keys() == [f"lake-live/{good}"]
    assert upload.state().refused == (rel,)


def test_a_path_entered_again_under_another_digest_is_refused(tmp_path):
    """A segment is never rewritten, so a new digest for a path already sent is refused."""
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)
    upload.pass_once()
    (root / rel).write_bytes(b"rewritten")
    append_manifest(
        root,
        partition=rel,
        source="capture",
        sha256=hashlib.sha256(b"rewritten").hexdigest(),
        rows=3,
        fetched_at=None,
    )

    upload.pass_once()

    assert upload.state().refused == (rel,)
    assert fake.body(f"lake-live/{rel}") != b"rewritten"


# -- 3. the upload never takes the lake-root lock ------------------------------------


def test_the_upload_sends_while_another_thread_holds_the_lake_lock(tmp_path):
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)
    held = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with lake_lock(root):
            held.set()
            release.wait(30)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert held.wait(10)
    passed = threading.Thread(target=upload.pass_once, daemon=True)
    passed.start()
    passed.join(10)
    try:
        assert not passed.is_alive(), "the pass waited on the lock another thread held"
        assert fake.put_keys() == [f"lake-live/{rel}"]
    finally:
        release.set()
        holder.join(10)


def test_the_module_imports_no_lock():
    tree = ast.parse(LIVE_UPLOAD_SOURCE.read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert not any(name == "lake.lock" or name.startswith("lake.lock.") for name in imported)
    assert "lake_lock" not in names


# -- 5 and 6. what is never sent -----------------------------------------------------


def test_an_import_manifested_between_two_segments_is_never_sent(tmp_path):
    root = _root(tmp_path)
    first, _ = _write_segment(root)
    bars = "bars/ticker=SPY/freq=1d/date=2026-09-01.parquet"
    (root / bars).parent.mkdir(parents=True)
    (root / bars).write_bytes(b"imported bars")
    append_manifest(
        root,
        partition=bars,
        source="bar-import:sha256:" + "ab" * 32,
        sha256=hashlib.sha256(b"imported bars").hexdigest(),
        rows=1,
        fetched_at=None,
    )
    second, _ = _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()

    _upload(root, fake).pass_once()

    assert fake.put_keys() == [f"lake-live/{first}", f"lake-live/{second}"]
    assert not any("bars/" in key or key.endswith("manifest.jsonl") for key in fake.put_keys())
    for put in fake.puts():
        assert "bar-import" not in json.dumps(put["Metadata"])


def test_chains_segments_and_unmanifested_segments_stay_at_home(tmp_path):
    root = _root(tmp_path)
    _write_segment(root, surface="chains")
    _write_segment(root, ticker="QQQ", manifest=False)
    quotes, _ = _write_segment(root, ticker="IWM")
    fake = FakeS3()

    _upload(root, fake).pass_once()

    assert fake.put_keys() == [f"lake-live/{quotes}"]


def test_the_surface_is_read_off_the_path_and_not_off_the_line(tmp_path):
    """Only lines naming ``quotes/`` are parsed, which saves parsing and decides nothing.

    A chains segment whose entry mentions ``quotes/`` elsewhere is parsed, and the surface in
    its path still keeps it at home.
    """
    root = _root(tmp_path)
    _write_segment(root, surface="chains", source="copied beside quotes/ by hand")
    fake = FakeS3()

    _upload(root, fake).pass_once()

    assert fake.calls == []


def test_a_field_that_is_not_ascii_is_left_out_of_the_metadata(tmp_path):
    """S3 user metadata travels as HTTP headers, which carry ASCII only."""
    root = _root(tmp_path)
    _write_segment(root, source="capture \u00e9t\u00e9")
    fake = FakeS3()

    _upload(root, fake).pass_once()

    (put,) = fake.puts()
    assert set(put["Metadata"]) == {"sha256", "rows", "fetched_at"}


# -- 7. superseded segments ----------------------------------------------------------


def test_a_segment_whose_partition_is_sealed_is_dropped_as_superseded(tmp_path):
    root = _root(tmp_path)
    _write_segment(root)
    append_manifest(
        root,
        partition="quotes/ticker=SPY/date=2026-09-02.parquet",
        source="compaction",
        sha256="cd" * 32,
        rows=3,
        fetched_at=None,
    )
    fake = FakeS3()
    upload = _upload(root, fake)

    upload.pass_once()

    assert fake.calls == []
    state = upload.state()
    assert (state.superseded, state.owed) == (1, 0)


def test_a_segment_whose_entry_follows_its_sealed_partition_is_not_sent(tmp_path):
    """A partition read first, as a re-read can meet it, still supersedes the segment."""
    root = _root(tmp_path)
    append_manifest(
        root,
        partition="quotes/ticker=SPY/date=2026-09-02.parquet",
        source="compaction",
        sha256="cd" * 32,
        rows=3,
        fetched_at=None,
    )
    _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)

    upload.pass_once()

    assert fake.calls == []
    assert (upload.state().superseded, upload.state().owed) == (1, 0)


def test_a_segment_sealed_while_it_was_owed_is_dropped_rather_than_retried(tmp_path):
    root = _root(tmp_path)
    _write_segment(root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    upload = _upload(root, fake)
    assert upload.pass_once() is not None
    append_manifest(
        root,
        partition="quotes/ticker=SPY/date=2026-09-02.parquet",
        source="compaction",
        sha256="cd" * 32,
        rows=3,
        fetched_at=None,
    )
    fake.fail_with = None

    assert upload.pass_once() is None

    assert len(fake.calls) == 1
    assert (upload.state().superseded, upload.state().owed) == (1, 0)


def test_a_segment_whose_file_is_gone_from_a_sealed_day_is_dropped_as_superseded(
    tmp_path, monkeypatch, capsys
):
    """Compaction seals the day and then deletes it, so the seal can land mid-pass.

    Here the pass reads the manifest, and only then does compaction record the partition
    and delete the segment, so the pass meets a missing file whose seal it has not read.
    """
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    fake = FakeS3()
    upload = _upload(root, fake)
    read_tail = upload._read_tail
    reads = []

    def then_compact() -> None:
        read_tail()
        reads.append(1)
        if len(reads) == 1:
            _seal(root)
            (root / rel).unlink()

    monkeypatch.setattr(upload, "_read_tail", then_compact)

    assert upload.pass_once() is None
    assert upload.pass_once() is None

    assert fake.calls == []
    state = upload.state()
    assert (state.superseded, state.vanished, state.owed) == (1, 0, 0)
    assert "missing from the lake" not in capsys.readouterr().err


def test_a_segment_gone_before_its_day_was_sealed_is_counted_apart_and_named(tmp_path, capsys):
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    (root / rel).unlink()
    kept, _ = _write_segment(root, ticker="QQQ")
    fake = FakeS3()
    upload = _upload(root, fake)

    assert upload.pass_once() is None
    assert upload.pass_once() is None

    assert fake.put_keys() == [f"lake-live/{kept}"]
    state = upload.state()
    assert (state.vanished, state.superseded, state.owed, state.uploaded) == (1, 0, 0, 1)
    lines = [line for line in capsys.readouterr().err.splitlines() if rel in line]
    assert lines == [
        f"live_upload: {rel} is missing from the lake before its day was sealed, so it "
        "stays out of the live prefix"
    ]


# -- 8. a manifest cut back in place --------------------------------------------------


def test_a_manifest_that_shrinks_is_read_from_the_start_and_nothing_is_sent_twice(tmp_path):
    root = _root(tmp_path)
    first, _ = _write_segment(root)
    _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    upload = _upload(root, fake)
    upload.pass_once()
    assert len(fake.puts()) == 2

    # Cut back in place to the first entry, keeping the inode, as the resync does, then
    # append a new one.
    path = manifest_path(root)
    first_line = path.read_bytes().split(b"\n")[0] + b"\n"
    with open(path, "r+b") as handle:
        handle.truncate(len(first_line))
    third, _ = _write_segment(root, start=NOW + timedelta(minutes=2))
    upload.pass_once()

    assert fake.put_keys()[2:] == [f"lake-live/{third}"]
    assert fake.versions(f"lake-live/{first}")[0] == fake.versions(f"lake-live/{first}")[-1]


def test_a_manifest_cut_back_and_grown_past_the_offset_is_read_from_the_start(tmp_path):
    """The length alone cannot see this one, so the bytes before the offset are compared."""
    root = _root(tmp_path)
    _write_segment(root)
    _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    upload = _upload(root, fake)
    upload.pass_once()
    path = manifest_path(root)
    old = path.read_bytes()

    # A new entry lands at the front, and the two old ones behind it, so the file is longer
    # than the offset and the bytes after the offset are the tail of an old line.
    third, digest = _write_segment(root, start=NOW + timedelta(minutes=2), manifest=False)
    line = json.dumps(
        {
            "partition": third,
            "source": "capture",
            "sha256": digest,
            "rows": 3,
            "fetched_at": None,
        },
        sort_keys=True,
    ).encode()
    with open(path, "r+b") as handle:
        handle.truncate(0)
        handle.write(line + b"\n" + old)
    assert os.path.getsize(path) > len(old)
    upload.pass_once()

    assert fake.put_keys()[2:] == [f"lake-live/{third}"]


def test_a_line_split_across_two_reads_is_kept_whole(tmp_path, monkeypatch):
    """The first read after a start takes the whole manifest, far more than one read holds."""
    monkeypatch.setattr(live_upload, "_READ_CHUNK", 100)
    root = _root(tmp_path)
    rels = [_write_segment(root, start=NOW + timedelta(minutes=n))[0] for n in range(3)]
    fake = FakeS3()

    assert _upload(root, fake).pass_once() is None

    assert fake.put_keys() == [f"lake-live/{rel}" for rel in rels]


def test_a_line_still_being_written_waits_for_its_newline(tmp_path):
    root = _root(tmp_path)
    rel, digest = _write_segment(root, manifest=False)
    line = json.dumps(
        {"partition": rel, "source": "capture", "sha256": digest, "rows": 3, "fetched_at": None},
        sort_keys=True,
    ).encode()
    path = manifest_path(root)
    path.write_bytes(line[:40])
    fake = FakeS3()
    upload = _upload(root, fake)

    assert upload.pass_once() is None
    assert fake.calls == []
    with open(path, "ab") as handle:
        handle.write(line[40:] + b"\n")
    assert upload.pass_once() is None

    assert fake.put_keys() == [f"lake-live/{rel}"]


def test_a_lake_with_no_manifest_yet_is_not_a_failure(tmp_path):
    assert _upload(_root(tmp_path), FakeS3()).pass_once() is None


def _cut_to(root: Path, lines: int) -> None:
    """Cut the manifest back in place to its first ``lines`` lines, keeping the inode."""
    path = manifest_path(root)
    kept = b"".join(path.read_bytes().splitlines(keepends=True)[:lines])
    with open(path, "r+b") as handle:
        handle.truncate(0)
        handle.write(kept)


def test_an_owed_segment_whose_entry_a_cut_removed_is_not_sent(tmp_path):
    root = _root(tmp_path)
    _write_segment(root)
    gone, _ = _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    fake.fail_with = unreachable()
    upload = _upload(root, fake)
    upload.pass_once()
    _cut_to(root, 1)
    fake.fail_with = None

    assert upload.pass_once() is None

    assert f"lake-live/{gone}" not in fake.put_keys()
    assert upload.state().owed == 0


def test_a_partition_a_cut_removed_no_longer_supersedes(tmp_path):
    root = _root(tmp_path)
    _write_segment(root, ticker="QQQ")
    _seal(root)
    fake = FakeS3()
    upload = _upload(root, fake)
    upload.pass_once()
    _cut_to(root, 1)
    rel, _ = _write_segment(root, start=NOW + timedelta(minutes=1))

    upload.pass_once()

    assert fake.put_keys()[-1] == f"lake-live/{rel}"


def test_a_refused_key_is_not_retried_after_a_cut_back(tmp_path):
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    fake.store(f"lake-live/{rel}", b"someone else's bytes")
    upload = _upload(root, fake)
    upload.pass_once()
    calls = len(fake.calls)
    _cut_to(root, 1)

    upload.pass_once()

    assert len(fake.calls) == calls
    assert upload.state().refused == (rel,)


# -- the alarm, on its own ----------------------------------------------------------


def test_a_backlog_older_than_ten_minutes_pages_once_and_recovery_prints_a_line(tmp_path, capsys):
    root = _root(tmp_path)
    _write_segment(root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)
    upload.pass_once()

    clock.advance(600)
    alarm.check(clock.now())
    assert transport.sent == []
    clock.advance(1)
    alarm.check(clock.now())
    clock.advance(60)
    alarm.check(clock.now())
    assert [message.event for message in transport.sent] == ["segment_upload_behind"]
    assert transport.sent[0].title == "Live upload behind"
    body = transport.sent[0].body
    assert "1 quotes segments owed, the oldest first read at 10:00 ET" in body
    assert COST in body and LOG in body
    assert "lake-backup" not in body

    fake.fail_with = None
    upload.pass_once()
    alarm.check(clock.now())
    alarm.check(clock.now())
    lines = [line for line in capsys.readouterr().err.splitlines() if "caught up" in line]
    assert len(lines) == 1
    assert len(transport.sent) == 1


def test_the_alarm_ages_the_backlog_by_the_clock_not_the_slot(tmp_path):
    """A tick runs late in its minute, so the slot can trail the clock by most of one."""
    root = _root(tmp_path)
    _write_segment(root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)
    upload.pass_once()

    clock.advance(630)
    alarm.check(NOW + timedelta(minutes=10))

    assert [message.event for message in transport.sent] == [BEHIND_EVENT]


def test_a_later_entry_for_an_owed_segment_keeps_its_first_read_time(tmp_path, monkeypatch):
    """Every pass ends cleanly here, so only the owed segment's age can page."""
    root = _root(tmp_path)
    rel, digest = _write_segment(root)
    _unreadable(monkeypatch, root / rel)
    clock = ManualClock(start=NOW)
    upload = _upload(root, FakeS3(), clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)
    assert upload.pass_once() is None

    clock.advance(360)
    append_manifest(root, partition=rel, source="capture", sha256=digest, rows=3, fetched_at=None)
    assert upload.pass_once() is None
    clock.advance(241)
    assert upload.pass_once() is None
    alarm.check(clock.now())

    (page,) = transport.sent
    assert page.body.startswith("1 quotes segments owed, the oldest first read at 10:00 ET")


def test_a_refusal_page_names_four_and_counts_the_rest():
    body = live_upload._refused_body([f"seg{n}" for n in range(6)])
    assert "6 quotes segments refused" in body
    assert "seg0, seg1, seg2, seg3 and 2 more. " in body
    four = live_upload._refused_body([f"seg{n}" for n in range(4)])
    assert "seg0, seg1, seg2, seg3. " in four
    assert " more" not in four
    one = live_upload._refused_body(["seg0"])
    assert "1 quotes segment refused" in one
    assert ": seg0. " in one


def test_a_thread_that_stopped_pages_once_and_never_reads_as_caught_up(
    tmp_path, capsys, monkeypatch
):
    root = _root(tmp_path)
    clock = ManualClock(start=NOW)
    upload = _upload(root, FakeS3(), clock)

    def broken() -> None:
        raise RuntimeError("a bug")

    monkeypatch.setattr(upload, "pass_once", broken)
    upload.start()
    upload._thread.join(10)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)

    alarm.check(NOW)
    alarm.check(NOW + timedelta(minutes=1))

    assert [message.event for message in transport.sent] == [BEHIND_EVENT]
    assert transport.sent[0].title == BEHIND_TITLE
    body = transport.sent[0].body
    assert body.startswith("The live upload thread stopped.")
    assert COST in body and LOG in body and RESTART_CAUTION in body
    err = capsys.readouterr().err
    assert "live_upload: stopped: RuntimeError: a bug" in err
    assert "caught up" not in err


def test_a_thread_that_cannot_read_the_manifest_pages_once_after_ten_minutes(tmp_path, monkeypatch):
    """It owes nothing, because it never reads an entry, so only the clean pass shows it."""
    root = _root(tmp_path)
    _write_segment(root)
    clock = ManualClock(start=NOW)
    upload = _upload(root, FakeS3(), clock)

    def denied() -> None:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(upload, "_read_tail", denied)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)

    for _ in range(10):
        assert upload.pass_once() == "manifest.jsonl could not be read (PermissionError)"
        clock.advance(60)
        alarm.check(clock.now())
    assert upload.state().owed == 0
    assert transport.sent == []
    clock.advance(1)
    upload.pass_once()
    alarm.check(clock.now())
    clock.advance(60)
    upload.pass_once()
    alarm.check(clock.now())

    (page,) = transport.sent
    assert (page.event, page.title) == ("segment_upload_behind", "Live upload behind")
    assert page.body.startswith(
        "No pass of the live upload has ended cleanly since 10:00 ET, 10 minutes ago."
    )
    assert COST in page.body and LOG in page.body


def test_a_thread_started_late_is_not_behind_on_its_first_minute(tmp_path, monkeypatch):
    """The daemon builds the upload, then wires its hooks, and only then starts the thread."""
    root = _root(tmp_path)
    clock = ManualClock(start=NOW)
    upload = LiveUpload(lake_root=root, target=LIVE, client=FakeS3(), clock=clock)
    clock.advance(11 * 60)
    release = threading.Event()
    # A thread that is alive and has not finished its first pass yet.
    monkeypatch.setattr(upload, "_run", lambda: release.wait(30))
    transport = _Recording()
    upload.start()
    try:
        assert upload.state().last_clean_pass == clock.now()
        UploadAlarm(upload, _publisher(root, transport), clock).check(clock.now())
        assert transport.sent == []
    finally:
        release.set()
        upload._thread.join(10)


def _owe_one(root: Path, fake: FakeS3, upload: LiveUpload, start: datetime) -> None:
    """A new segment the bucket refuses to take, read by one pass."""
    _write_segment(root, start=start)
    fake.fail_with = unreachable()
    assert upload.pass_once() is not None


def test_a_flapping_bucket_pages_once_a_date_and_prints_each_catch_up(tmp_path, capsys):
    """24 to 28 pages a session were measured before, from a page on every catch-up."""
    root = _root(tmp_path)
    fake = FakeS3()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording(clock)
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)

    for _ in range(3):
        _owe_one(root, fake, upload, clock.now())
        clock.advance(11 * 60)
        alarm.check(clock.now())
        fake.fail_with = None
        assert upload.pass_once() is None
        alarm.check(clock.now())
        clock.advance(60)

    assert [message.event for message in transport.sent] == [BEHIND_EVENT]
    caught_up = [line for line in capsys.readouterr().err.splitlines() if "caught up" in line]
    assert len(caught_up) == 3

    # The next Eastern date pages again.
    clock.set(datetime(2026, 9, 3, 14, 0, tzinfo=UTC))
    _owe_one(root, fake, upload, clock.now())
    clock.advance(11 * 60)
    alarm.check(clock.now())

    assert [message.event for message in transport.sent] == [BEHIND_EVENT, BEHIND_EVENT]
    assert [at.astimezone(MARKET_TZ).date() for at in transport.at] == [
        date(2026, 9, 2),
        date(2026, 9, 3),
    ]


def test_a_thread_that_stops_pages_even_on_a_date_a_flap_already_paged(tmp_path, monkeypatch):
    root = _root(tmp_path)
    fake = FakeS3()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)
    _owe_one(root, fake, upload, clock.now())
    clock.advance(11 * 60)
    alarm.check(clock.now())
    fake.fail_with = None
    upload.pass_once()
    alarm.check(clock.now())

    def broken() -> None:
        raise RuntimeError("a bug")

    monkeypatch.setattr(upload, "pass_once", broken)
    upload.start()
    upload._thread.join(10)
    clock.advance(60)
    alarm.check(clock.now())

    assert [message.event for message in transport.sent] == [BEHIND_EVENT, BEHIND_EVENT]
    assert transport.sent[1].body.startswith("The live upload thread stopped.")


def test_a_hung_put_never_holds_up_the_alarm(tmp_path):
    """The alarm reads a copy of the state under a lock no request is ever made inside."""
    root = _root(tmp_path)
    _write_segment(root)
    entered = threading.Event()
    release = threading.Event()

    def hang(kwargs, data) -> None:
        entered.set()
        release.wait(30)

    fake = FakeS3(on_put=hang)
    clock = ManualClock(start=NOW)
    upload = LiveUpload(lake_root=root, target=LIVE, client=fake, clock=clock)
    upload.start()
    try:
        assert entered.wait(10)
        alarm = UploadAlarm(upload, _publisher(root, _Recording()), clock)
        checked = threading.Thread(target=alarm.check, args=(NOW,), daemon=True)
        checked.start()
        checked.join(1.0)
        assert not checked.is_alive(), "the alarm waited on a PUT in flight"
    finally:
        release.set()
        upload.stop()


def test_the_upload_thread_is_a_daemon_thread(tmp_path):
    upload = LiveUpload(
        lake_root=_root(tmp_path), target=LIVE, client=FakeS3(), clock=ManualClock(start=NOW)
    )
    upload.start()
    try:
        assert upload._thread is not None
        assert upload._thread.daemon
        assert upload._thread.name == "live-upload"
    finally:
        upload.stop()
        upload._thread.join(10)


def test_the_retry_waits_double_from_five_seconds_up_to_a_minute():
    waits = [retry_delay(n, poll=5.0, cap=60.0) for n in range(7)]
    assert waits == [5.0, 5.0, 10.0, 20.0, 40.0, 60.0, 60.0]


def _record_waits(monkeypatch, upload: LiveUpload, count: int) -> list[float]:
    """Replace the thread's wait with a recorder that ends the run after ``count`` waits."""
    waits: list[float] = []

    def wait(timeout: float) -> bool:
        waits.append(timeout)
        return len(waits) >= count

    monkeypatch.setattr(upload._stop, "wait", wait)
    return waits


def test_the_thread_waits_longer_after_each_pass_the_bucket_failed(tmp_path, monkeypatch):
    """The production poll and cap, through ``_run`` itself rather than ``retry_delay``."""
    root = _root(tmp_path)
    _write_segment(root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    upload = _upload(root, fake)
    waits = _record_waits(monkeypatch, upload, 6)

    upload._run()

    assert waits == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0]
    assert len(fake.puts()) == 6


def test_a_clean_pass_resets_the_wait_to_the_poll(tmp_path, monkeypatch):
    upload = _upload(_root(tmp_path), FakeS3())
    results = iter(["x", "x", "x", None, "x"])
    monkeypatch.setattr(upload, "pass_once", lambda: next(results))
    waits = _record_waits(monkeypatch, upload, 5)

    upload._run()

    assert waits == [5.0, 10.0, 20.0, 5.0, 5.0]


def test_the_thread_names_a_failure_again_only_when_its_cause_changes(
    tmp_path, monkeypatch, capsys
):
    upload = _upload(_root(tmp_path), FakeS3())
    results = iter(["a: unreachable", "a: unreachable", "b: refused", "b: refused", None, "c"])
    monkeypatch.setattr(upload, "pass_once", lambda: next(results))
    _record_waits(monkeypatch, upload, 6)

    upload._run()

    assert capsys.readouterr().err.splitlines() == [
        "live_upload: a: unreachable, retrying",
        "live_upload: b: refused, retrying",
        "live_upload: sending again after 4 failed passes",
        "live_upload: c, retrying",
    ]


def test_an_unreadable_segment_stays_owed_and_the_rest_go_up(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    rels = [_write_segment(root, start=NOW + timedelta(minutes=n))[0] for n in range(6)]
    _unreadable(monkeypatch, root / rels[0])
    fake = FakeS3()
    clock = ManualClock(start=NOW)
    upload = _upload(root, fake, clock)
    transport = _Recording()
    alarm = UploadAlarm(upload, _publisher(root, transport), clock)

    assert upload.pass_once() is None
    assert upload.pass_once() is None

    assert fake.put_keys() == [f"lake-live/{rel}" for rel in rels[1:]]
    state = upload.state()
    assert (state.owed, state.uploaded) == (1, 5)
    named = [line for line in capsys.readouterr().err.splitlines() if rels[0] in line]
    assert named == [
        f"live_upload: {rels[0]} could not be read (PermissionError), so it stays owed and "
        "is tried again each pass"
    ]

    clock.advance(601)
    upload.pass_once()
    alarm.check(clock.now())

    (page,) = transport.sent
    assert page.event == BEHIND_EVENT
    assert page.body.startswith("1 quotes segments owed, the oldest first read at 10:00 ET")


def test_a_failed_request_keeps_the_segment_owed_and_ends_the_pass(tmp_path):
    root = _root(tmp_path)
    _write_segment(root)
    _write_segment(root, start=NOW + timedelta(minutes=1))
    fake = FakeS3()
    fake.fail_with = client_error("ConditionalRequestConflict", "PutObject", 409)
    upload = _upload(root, fake)

    failed = upload.pass_once()

    assert failed is not None and "ConditionalRequestConflict" in failed
    assert len(fake.calls) == 1
    assert upload.state().owed == 2
    fake.fail_with = None
    assert upload.pass_once() is None
    assert upload.state().uploaded == 2


# -- 9. only a primary with a prefixed bucket target starts an upload ---------------


def _bucket_config(tmp_path: Path, root: Path, *, target: str = S3_TARGET, role=None) -> Path:
    config = write_config(tmp_path, root, role=role)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {target}"
    )
    config.write_text(text)
    return config


def _never(*args):
    raise AssertionError("a client was built")


def test_a_primary_with_a_bucket_target_prepares_an_upload_to_the_live_prefix(tmp_path):
    root = _root(tmp_path)
    config = load_config(_bucket_config(tmp_path, root))
    calls = []
    fake = FakeS3()

    def connect(config, target):
        calls.append(target)
        return target, fake

    upload = prepare(config, ManualClock(start=NOW), connect=connect)

    assert isinstance(upload, LiveUpload)
    assert calls == [TARGET]
    assert upload._target == LIVE
    assert live_target(TARGET) == LIVE
    assert LIVE.prefix == TARGET.prefix + LIVE_SUFFIX


def test_prepare_passes_its_timings_to_the_upload(tmp_path):
    root = _root(tmp_path)
    config = load_config(_bucket_config(tmp_path, root))

    upload = prepare(
        config,
        ManualClock(start=NOW),
        connect=lambda config, target: (target, FakeS3()),
        poll_seconds=0.25,
        backoff_cap_seconds=3.0,
    )

    assert isinstance(upload, LiveUpload)
    assert (upload._poll, upload._cap) == (0.25, 3.0)


@pytest.mark.parametrize(
    ("target", "role"),
    [
        pytest.param(S3_TARGET, "shadow", id="shadow"),
        pytest.param(None, None, id="path-target"),
    ],
)
def test_a_host_that_runs_no_upload_prepares_none_and_says_nothing(tmp_path, capsys, target, role):
    root = _root(tmp_path)
    if target is None:
        path = write_config(tmp_path, root)
    else:
        path = _bucket_config(tmp_path, root, target=target, role=role)

    assert prepare(load_config(path), ManualClock(start=NOW), connect=_never) is None
    assert "live_upload" not in capsys.readouterr().err


def test_a_target_with_no_prefix_is_a_start_that_failed(tmp_path, capsys):
    root = _root(tmp_path)
    path = _bucket_config(tmp_path, root, target="s3://lake-backup")

    refusal = prepare(load_config(path), ManualClock(start=NOW), connect=_never)

    assert isinstance(refusal, NotStarted)
    assert "names no key prefix" in refusal.reason
    (printed,) = capsys.readouterr().err.splitlines()
    assert printed == f"live_upload: not started: {refusal.reason}"


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        pytest.param(
            BucketSettingsInvalid("bucket_region is not set in config.yaml"),
            "bucket_region is not set in config.yaml",
            id="settings",
        ),
        pytest.param(
            RuntimeError("boom lake-backup"),
            "the client build raised RuntimeError",
            id="unexpected",
        ),
    ],
)
def test_a_client_that_cannot_be_built_is_a_start_that_failed(tmp_path, capsys, error, reason):
    root = _root(tmp_path)
    config = load_config(_bucket_config(tmp_path, root))

    def connect(config, target):
        raise error

    refusal = prepare(config, ManualClock(start=NOW), connect=connect)

    assert refusal == NotStarted(reason=reason)
    (printed,) = capsys.readouterr().err.splitlines()
    assert printed == f"live_upload: not started: {reason}"


def test_the_not_started_page_names_the_reason_the_cost_and_the_restart_risk():
    message = live_upload.not_started_message(NotStarted(reason="bucket_region is not set"))

    assert message.event == "segment_upload_not_started"
    assert message.title == "Live upload not started"
    assert message.body.startswith("The live upload did not start: bucket_region is not set.")
    assert COST in message.body and LOG in message.body and RESTART_CAUTION in message.body
    assert "A daemon restart retries the client build." in message.body


# -- 4, 9 and 10. through the daemon's production entry --------------------------------


class _Rig:
    """A primary daemon's config over a bucket target, a one-ticker roster and a lake."""

    def __init__(self, tmp_path: Path, *, role: str | None = None) -> None:
        self.root = _root(tmp_path)
        record_running_version(self.root)
        self.config = _bucket_config(tmp_path, self.root, role=role)
        self.tickers = tmp_path / "tickers.yaml"
        self.tickers.write_text("XYZ: {options: false}\n")
        self.token = tmp_path / "token.json"
        # A row at 09:58 stops startup marking at the daemon's own start minute.
        marked = et(2026, 9, 2, 9, 58)
        batch = journal.gap_rows("quotes", ticker="XYZ", slots=[marked], error_class="x")
        stamp = marked.strftime(SEGMENT_STAMP_FORMAT)
        with journal.SegmentWriter.open(self.root, "quotes", "XYZ", DAY, stamp, 1) as writer:
            writer.write_cycle(batch)


class _Cycles:
    """A cycle runner that writes one quotes segment and records it, as capture does.

    The manifest append goes through ``lake_lock`` exactly as ``_CaptureCycle.run`` takes
    it. ``seen`` makes each cycle wait, in real time, until the upload has read its entry,
    so the alarm's ages follow the manual clock rather than the thread's scheduling.
    """

    def __init__(self, rig: _Rig, clock: ManualClock, events: list[str] | None = None) -> None:
        self._rig = rig
        self._clock = clock
        self.events = events if events is not None else []
        self.rels: list[str] = []
        self.seen = None

    def __call__(self, *, slot, close_tag, session_phase) -> CycleResult:
        self.events.append("cycle")
        start = self._clock.now().astimezone(UTC)
        rel = (
            f"journal/date={start.date().isoformat()}/surface=quotes/ticker=XYZ/"
            f"seg-{start.strftime(SEGMENT_STAMP_FORMAT)}-{len(self.rels)}.arrows"
        )
        path = self._rig.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(rel.encode())
        with lake_lock(self._rig.root):
            append_manifest(
                self._rig.root,
                partition=rel,
                source="capture",
                sha256=hashlib.sha256(rel.encode()).hexdigest(),
                rows=1,
                fetched_at=None,
            )
        self.rels.append(rel)
        if self.seen is not None:
            count = len(self.rels)
            _wait_until(lambda: self.seen() >= count)
        outcome = SegmentOutcome(
            surface="quotes",
            ticker="XYZ",
            path=path,
            partition=rel,
            row_kind=journal.ROW_KIND_DATA,
            rows=1,
            error_class=None,
            fetched_at=None,
            data_rows=1,
        )
        return CycleResult(snap_ts=slot, segments=(outcome,))


def _stop_after(count: int):
    remaining = [count]

    def should_continue() -> bool:
        if remaining[0] == 0:
            return False
        remaining[0] -= 1
        return True

    return should_continue


def _run(rig: _Rig, clock: ManualClock, *, ticks, cycles, live, transport, hooks=None) -> None:
    daemon.run_loop_from_config(
        config_path=str(rig.config),
        tickers_path=str(rig.tickers),
        token_path=str(rig.token),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=transport,
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        pull_runner=None,
        live_upload=live,
        cycle_runner=cycles,
        hooks=hooks,
        should_continue=_stop_after(ticks),
    )


def _seen(state) -> int:
    return (
        state.owed
        + state.uploaded
        + state.already_there
        + state.superseded
        + state.vanished
        + len(state.refused)
    )


def test_a_bucket_down_for_the_session_costs_no_cycle_and_pages_once(tmp_path, capsys):
    rig = _Rig(tmp_path)
    # The long grace lets a cycle wait, in real time, for a thread scheduled late under load
    # without the clock moving on underneath it.
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30), grace=10.0)
    fake = FakeS3()
    fake.fail_with = unreachable()
    uploads: list[LiveUpload] = []

    def starter(config, clock):
        upload = prepare(
            config,
            clock,
            connect=lambda config, target: (target, fake),
            poll_seconds=0.001,
            backoff_cap_seconds=0.002,
        )
        uploads.append(upload)
        return upload

    cycles = _Cycles(rig, clock)
    cycles.seen = lambda: _seen(uploads[0].state())
    recover_at = et(2026, 9, 2, 10, 20)

    def on_tick(slot: datetime) -> None:
        # The bucket comes back at 10:20. The tick waits, in real time, for the thread to
        # clear the backlog, so the alarm that runs after it this same minute sees it.
        if slot == recover_at:
            fake.fail_with = None
            _wait_until(lambda: uploads[0].state().owed == 0)

    transport = _Recording(clock)
    _run(
        rig,
        clock,
        ticks=25,
        cycles=cycles,
        live=starter,
        transport=transport,
        hooks=daemon.DaemonHooks(on_tick=on_tick),
    )

    # Every cycle ran and landed its manifest entry, the bucket notwithstanding.
    assert len(cycles.rels) == 25
    entries = [json.loads(line) for line in manifest_path(rig.root).read_text().splitlines()]
    assert [entry["partition"] for entry in entries if entry["source"] == "capture"] == cycles.rels
    # One page, once the oldest owed segment was more than ten minutes old, or no pass had
    # ended cleanly for that long. The last clean pass can fall at 09:59:30, before the
    # first cycle's entry, so the page can come at 10:10 itself.
    pages = zip(transport.sent, transport.at, strict=True)
    behind = [at for message, at in pages if message.event == BEHIND_EVENT]
    assert len(behind) == 1
    first_cycle = et(2026, 9, 2, 10, 0)
    assert timedelta(minutes=10) <= behind[0] - first_cycle <= timedelta(minutes=11, seconds=30)
    assert [message.event for message in transport.sent] == [BEHIND_EVENT]
    # Nothing outside the live prefix, and never the manifest.
    assert fake.put_keys(), "the upload never tried"
    assert all(key.startswith("lake-live/journal/") for key in fake.put_keys())
    assert not any(key.endswith("manifest.jsonl") for key in fake.put_keys())
    # It caught up once the bucket answered, and said so once.
    err = capsys.readouterr().err
    assert len([line for line in err.splitlines() if "caught up" in line]) == 1
    _wait_until(lambda: uploads[0].state().owed == 0)
    # Exactly the cycles' segments. The gap segment the rig wrote at 09:58 carries no
    # manifest entry, so it stayed home.
    assert sorted(fake.keys()) == sorted(f"lake-live/{rel}" for rel in cycles.rels)
    # The loop asked the thread to stop on its way out.
    assert uploads[0]._stop.is_set()


def test_a_client_that_cannot_be_built_pages_once_and_leaves_the_loop_capturing(tmp_path, capsys):
    rig = _Rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    cycles = _Cycles(rig, clock)

    def connect(config, target):
        raise BucketSettingsInvalid("bucket_region is not set in config.yaml")

    transport = _Recording(clock)
    _run(
        rig,
        clock,
        ticks=3,
        cycles=cycles,
        live=lambda config, clock: prepare(config, clock, connect=connect),
        transport=transport,
    )

    assert len(cycles.rels) == 3
    err = capsys.readouterr().err
    assert "live_upload: not started: bucket_region is not set in config.yaml" in err
    assert not any(t.name == "live-upload" for t in threading.enumerate())
    pages = [
        (message, at)
        for message, at in zip(transport.sent, transport.at, strict=True)
        if message.event == NOT_STARTED_EVENT
    ]
    ((page, at),) = pages
    assert page.title == NOT_STARTED_TITLE
    assert "bucket_region is not set in config.yaml" in page.body
    assert RESTART_CAUTION in page.body
    assert "lake-backup" not in page.body
    # Sent at startup, before the first cycle at 10:00.
    assert at < et(2026, 9, 2, 10, 0)


def test_a_shadow_daemon_builds_no_client_starts_no_thread_and_pages_nothing(tmp_path):
    rig = _Rig(tmp_path, role="shadow")
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    cycles = _Cycles(rig, clock)
    transport = _Recording()

    _run(
        rig,
        clock,
        ticks=2,
        cycles=cycles,
        live=lambda config, clock: prepare(config, clock, connect=_never),
        transport=transport,
    )

    assert len(cycles.rels) == 2
    assert not any(t.name == "live-upload" for t in threading.enumerate())
    assert not any(message.event.startswith("segment_upload") for message in transport.sent)


def test_the_client_is_built_on_the_calling_thread_before_the_first_cycle(tmp_path):
    rig = _Rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    events: list[str] = []
    builders: list[int] = []
    fake = FakeS3()

    def connect(config, target):
        events.append("connect")
        builders.append(threading.get_ident())
        return target, fake

    cycles = _Cycles(rig, clock, events)
    _run(
        rig,
        clock,
        ticks=2,
        cycles=cycles,
        live=lambda config, clock: prepare(config, clock, connect=connect),
        transport=_Recording(),
    )

    assert events[0] == "connect"
    assert events.count("connect") == 1
    assert events.count("cycle") == 2
    assert builders == [threading.get_ident()]


# -- 11. the lag command -------------------------------------------------------------


def test_segment_start_reads_the_stamp_in_utc():
    rel = "journal/date=2026-09-02/surface=quotes/ticker=SPY/seg-20260902T140003123456-77.arrows"
    assert segment_start(rel) == datetime(2026, 9, 2, 14, 0, 3, 123456, tzinfo=UTC)
    assert segment_start("quotes/ticker=SPY/date=2026-09-02.parquet") is None
    assert segment_start("journal/date=2026-09-02/surface=quotes/ticker=SPY/seg-x-1.arrows") is None
    unprefixed = (
        "journal/date=2026-09-02/surface=quotes/ticker=SPY/xxx-20260902T140003123456-77.arrows"
    )
    assert segment_start(unprefixed) is None


def _lagged(fake: FakeS3, start: datetime, seconds: float, *, ticker: str = "SPY") -> str:
    rel = (
        f"journal/date={start.date().isoformat()}/surface=quotes/ticker={ticker}/"
        f"seg-{start.strftime(SEGMENT_STAMP_FORMAT)}-9.arrows"
    )
    fake.store(f"lake-live/{rel}", b"x", last_modified=start + timedelta(seconds=seconds))
    return rel


def test_the_lag_is_last_modified_less_the_start_its_key_carries():
    fake = FakeS3(page_size=2)
    a = _lagged(fake, NOW, 12.5)
    b = _lagged(fake, NOW + timedelta(minutes=1), 91, ticker="QQQ")
    c = _lagged(fake, NOW + timedelta(minutes=2), 90)
    # Another day's segment and a key outside the live prefix are both left out.
    _lagged(fake, NOW + timedelta(days=1), 500)
    fake.store("lake/journal/date=2026-09-02/surface=quotes/ticker=SPY/seg-x.arrows", b"x")
    # A listing that ignored the continuation token would ask for the first page forever,
    # so a repeated token fails at once rather than hanging the suite.
    tokens: list[str | None] = []
    list_objects_v2 = fake.list_objects_v2

    def once_per_token(**kwargs):
        token = kwargs.get("ContinuationToken")
        if token in tokens:
            raise AssertionError(f"the listing asked for the page at {token!r} twice")
        tokens.append(token)
        return list_objects_v2(**kwargs)

    fake.list_objects_v2 = once_per_token

    listing = live_upload.list_live(fake, LIVE, DAY)
    assert tokens == [None, "2"]
    lags = live_upload.segment_lags(listing, LIVE)

    assert [(one.rel, one.lag.total_seconds()) for one in lags] == sorted(
        [(a, 12.5), (b, 91.0), (c, 90.0)]
    )
    summary = live_upload.LagSummary.of(lags)
    assert (summary.count, summary.largest, summary.over) == (3, timedelta(seconds=91), 1)
    lines = live_upload.lag_lines(lags, DAY, LIVE)
    assert lines[-1] == (
        "lag: 3 quotes segments on 2026-09-02, largest 91.0 s, 1 over 90.0 s: "
        "s3://lake-backup/lake-live"
    )
    assert f"{a} 12.5 s" in lines


def test_a_listed_key_with_no_stamp_is_left_out_of_the_lags():
    fake = FakeS3()
    stamped = _lagged(fake, NOW, 10)
    fake.store("lake-live/journal/date=2026-09-02/surface=quotes/ticker=SPY/seg-x-1.arrows", b"x")

    lags = live_upload.segment_lags(live_upload.list_live(fake, LIVE, DAY), LIVE)

    assert [one.rel for one in lags] == [stamped]


def test_the_lag_command_reads_the_bucket_and_prints_the_summary(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    config = _bucket_config(tmp_path, root)
    fake = FakeS3()
    _lagged(fake, NOW, 30)
    monkeypatch.setattr(live_upload, "connect", lambda config, target=None: (target, fake))

    assert live_upload.main(["lag", "--date", "2026-09-02", "--config", str(config)]) == 0

    out = capsys.readouterr().out.splitlines()
    assert out[-1].startswith("lag: 1 quotes segments on 2026-09-02, largest 30.0 s, 0 over")
    assert [name for name, _ in fake.calls] == ["list_objects_v2"]
    assert fake.calls[0][1]["Prefix"] == "lake-live/journal/date=2026-09-02/surface=quotes/"


def test_the_lag_command_says_why_a_day_may_list_no_segments(tmp_path, monkeypatch, capsys):
    """A day past the lifecycle rule lists nothing, which must not read as nothing sent."""
    root = _root(tmp_path)
    config = _bucket_config(tmp_path, root)
    monkeypatch.setattr(live_upload, "connect", lambda config, target=None: (target, FakeS3()))

    assert live_upload.main(["lag", "--date", "2026-09-02", "--config", str(config)]) == 0

    assert capsys.readouterr().out.splitlines() == [
        "lag: no quotes segments on 2026-09-02, and the bucket keeps each for only a week: "
        "s3://lake-backup/lake-live"
    ]


def test_the_lag_command_names_a_bucket_that_failed(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    config = _bucket_config(tmp_path, root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    monkeypatch.setattr(live_upload, "connect", lambda config, target=None: (target, fake))

    assert live_upload.main(["lag", "--date", "2026-09-02", "--config", str(config)]) == 1
    assert "the listing was unreachable" in capsys.readouterr().err
