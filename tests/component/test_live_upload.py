"""The live upload: each quotes segment goes to the bucket as its manifest entry lands.

``lake.live_upload`` tails ``manifest.jsonl`` on a thread of the daemon and PUTs each
manifested quotes segment under the sibling prefix ``<prefix>-live/`` (marketlake #943).
These run it over a real lake on disk, with the S3 client faked by ``FakeS3``, which models
the create-only PUT's 412 and S3's ``BadDigest`` refusal. Most cases call ``pass_once``
directly, so no thread runs and nothing waits. The daemon cases run the production entry
with a manual clock and a real upload thread, polling every millisecond.

Eleven behaviours are covered, numbered as marketlake #943's build names them.

1. A manifested quotes segment goes up under the live prefix, create-only, carrying its
   manifest digest as ``ChecksumSHA256``, the ``STANDARD`` class and its entry as metadata.
2. A second upload of a key is refused. A matching HEAD counts it as already there and sends
   no second PUT. A HEAD that differs, or a ``BadDigest``, refuses the key for good and pages.
3. The upload never takes the lake-root lock, and the module imports no lock.
4. A bucket that fails every request for a whole session costs no cycle, pages once after
   ten minutes, PUTs nothing outside the live prefix, and prints its recovery line.
5. A bars import manifested between two segments is never sent, and neither is
   ``manifest.jsonl``.
6. A chains segment and a segment with no manifest entry stay at home.
7. A segment whose partition is sealed, or whose file is gone, is dropped as superseded.
8. A manifest cut back in place is read from the start again, and nothing already in the
   bucket is sent twice.
9. Only a primary with a prefixed bucket target starts an upload, and a client that cannot
   be built starts none while the loop runs on.
10. The client is built on the caller's thread before the first cycle.
11. The lag command's arithmetic, over a fake listing and through its ``main``.
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

from lake import daemon, journal, live_upload
from lake.alert import Message, Publisher
from lake.bucket import BucketSettingsInvalid
from lake.capture import CycleResult, SegmentOutcome
from lake.config import BucketTarget, load_config
from lake.gap import SEGMENT_STAMP_FORMAT
from lake.live_upload import (
    BEHIND_EVENT,
    LIVE_SUFFIX,
    REFUSED_EVENT,
    LiveUpload,
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
    assert page.event == REFUSED_EVENT
    assert rel in page.body
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


def test_a_segment_whose_file_is_gone_is_dropped_as_superseded(tmp_path):
    root = _root(tmp_path)
    rel, _ = _write_segment(root)
    (root / rel).unlink()
    fake = FakeS3()
    upload = _upload(root, fake)

    assert upload.pass_once() is None
    assert upload.pass_once() is None

    assert fake.calls == []
    assert (upload.state().superseded, upload.state().owed) == (1, 0)


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
    assert [message.event for message in transport.sent] == [BEHIND_EVENT]
    assert "1 quotes segments owed" in transport.sent[0].body

    fake.fail_with = None
    upload.pass_once()
    alarm.check(clock.now())
    alarm.check(clock.now())
    lines = [line for line in capsys.readouterr().err.splitlines() if "caught up" in line]
    assert len(lines) == 1
    assert len(transport.sent) == 1


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
    assert "stopped" in transport.sent[0].body
    err = capsys.readouterr().err
    assert "live_upload: stopped: RuntimeError: a bug" in err
    assert "caught up" not in err


def test_the_retry_waits_double_from_five_seconds_up_to_a_minute():
    waits = [retry_delay(n, poll=5.0, cap=60.0) for n in range(7)]
    assert waits == [5.0, 5.0, 10.0, 20.0, 40.0, 60.0, 60.0]


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


@pytest.mark.parametrize(
    ("target", "role", "line"),
    [
        pytest.param(S3_TARGET, "shadow", None, id="shadow"),
        pytest.param(None, None, None, id="path-target"),
        pytest.param("s3://lake-backup", None, "names no key prefix", id="empty-prefix"),
    ],
)
def test_no_upload_starts_off_a_primary_with_a_prefixed_bucket(
    tmp_path, capsys, target, role, line
):
    root = _root(tmp_path)
    if target is None:
        path = write_config(tmp_path, root)
    else:
        path = _bucket_config(tmp_path, root, target=target, role=role)

    assert prepare(load_config(path), ManualClock(start=NOW), connect=_never) is None

    err = capsys.readouterr().err
    if line is None:
        assert "live_upload" not in err
    else:
        (printed,) = err.splitlines()
        assert printed.startswith("live_upload: not started: ")
        assert line in printed


@pytest.mark.parametrize(
    "error",
    [BucketSettingsInvalid("bucket_region is not set in config.yaml"), RuntimeError("boom")],
    ids=["settings", "unexpected"],
)
def test_a_client_that_cannot_be_built_starts_no_upload(tmp_path, capsys, error):
    root = _root(tmp_path)
    config = load_config(_bucket_config(tmp_path, root))

    def connect(config, target):
        raise error

    assert prepare(config, ManualClock(start=NOW), connect=connect) is None
    (printed,) = capsys.readouterr().err.splitlines()
    assert printed.startswith("live_upload: not started: ")


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
    return state.owed + state.uploaded + state.already_there + state.superseded + len(state.refused)


def test_a_bucket_down_for_the_session_costs_no_cycle_and_pages_once(tmp_path, capsys):
    rig = _Rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
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
    # One page, once the oldest owed segment was more than ten minutes old.
    pages = zip(transport.sent, transport.at, strict=True)
    behind = [at for message, at in pages if message.event == BEHIND_EVENT]
    assert len(behind) == 1
    first_cycle = et(2026, 9, 2, 10, 0)
    assert timedelta(minutes=10) < behind[0] - first_cycle <= timedelta(minutes=11, seconds=30)
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


def test_a_client_that_cannot_be_built_leaves_the_loop_capturing(tmp_path, capsys):
    rig = _Rig(tmp_path)
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    cycles = _Cycles(rig, clock)

    def connect(config, target):
        raise BucketSettingsInvalid("bucket_region is not set in config.yaml")

    _run(
        rig,
        clock,
        ticks=3,
        cycles=cycles,
        live=lambda config, clock: prepare(config, clock, connect=connect),
        transport=_Recording(),
    )

    assert len(cycles.rels) == 3
    err = capsys.readouterr().err
    assert "live_upload: not started: bucket_region is not set in config.yaml" in err
    assert not any(t.name == "live-upload" for t in threading.enumerate())


def test_a_shadow_daemon_builds_no_client_and_starts_no_thread(tmp_path):
    rig = _Rig(tmp_path, role="shadow")
    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    cycles = _Cycles(rig, clock)

    _run(
        rig,
        clock,
        ticks=2,
        cycles=cycles,
        live=lambda config, clock: prepare(config, clock, connect=_never),
        transport=_Recording(),
    )

    assert len(cycles.rels) == 2
    assert not any(t.name == "live-upload" for t in threading.enumerate())


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

    listing = live_upload.list_live(fake, LIVE, DAY)
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


def test_the_lag_command_names_a_bucket_that_failed(tmp_path, monkeypatch, capsys):
    root = _root(tmp_path)
    config = _bucket_config(tmp_path, root)
    fake = FakeS3()
    fake.fail_with = unreachable()
    monkeypatch.setattr(live_upload, "connect", lambda config, target=None: (target, fake))

    assert live_upload.main(["lag", "--date", "2026-09-02", "--config", str(config)]) == 1
    assert "the listing was unreachable" in capsys.readouterr().err
