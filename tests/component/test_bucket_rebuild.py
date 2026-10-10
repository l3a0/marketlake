"""Rebuilding a trimmed lake from the bucket, and the journal reserve on every ``restore``.

Marketlake #785. A host that keeps a window of sessions, the hosted VM, trims old partitions
from its lake and records each trim in ``trimmed.jsonl``. Its restore leaves out each partition
the bucket's copy of that ledger says was removed on purpose, and a host without the window key
restores the whole lake. Every ``restore`` that downloads also checks that the destination keeps
the journal reserve free. The cases follow the issue's Verification list.

1. The exclusion restores everything but the designed absences, a refused day's journal
   segments and ``reports/`` included, and restores a partition trimmed on the volume after the
   bucket's ledger last uploaded.
2. With no ledger entry in the bucket's manifest, the exclusion restores everything.
3. A ledger that is missing, damaged or mismatched refuses before any download, and the
   mismatch leaves the working directory the README's repair needs.
4. Through ``main``, the window key decides the mode.
5. A designed absence the bucket lost is named and fails nothing, while a restored partition the
   bucket lost still fails.
6. The journal reserve refuses a destination that holds the plan and not the reserve.

The bucket is built by the real ``first_upload``, then the range restore tests' ``_trim_away``,
then the real ``nightly_upload``, in that order, so it holds what an uploaded trimmed lake
holds. Expected bytes, digests and sizes are read from the lake, from the fake bucket's store or
from ``hashlib`` here, never through the code under test, and the reserve's 13 sessions are
written out as a literal.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import bucket
from lake.bucket import (
    BucketLedgerMismatch,
    BucketRefusal,
    RestoreRefused,
    first_upload,
    nightly_upload,
    read_bucket_trimmed,
    restore_lake,
)
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.lock import lake_lock
from lake.manifest import append_manifest
from lake.trimmed import append_trimmed, restore_line
from tests.component.test_bucket_range_restore import _trim_away
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake, sample_chains_table

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
D1 = date(2026, 8, 24)
D2 = date(2026, 8, 25)
D3 = date(2026, 8, 26)
SPY_1 = "chains/ticker=SPY/date=2026-08-24.parquet"
SPY_2 = "chains/ticker=SPY/date=2026-08-25.parquet"
SPY_3 = "chains/ticker=SPY/date=2026-08-26.parquet"
QUOTES_1 = "quotes/ticker=SPY/date=2026-08-24.parquet"
# A dated report, which counts toward its day's sealed bytes as it does on the lake.
REPORT = "reports/close_guard/date=2026-08-24/run.json"
# A day compaction refused: its segment is manifested and no compacted partition exists, so the
# segment is that day's only copy.
SEGMENT = "journal/date=2026-08-27/surface=chains/ticker=SPY/seg-20260827T133000Z-4242.arrows"
LEDGER = "trimmed.jsonl"
WORK = ".marketlake-restoring"
STAMP = "2026-08-28T19:00:00-04:00"
FRIDAY_19 = datetime(2026, 8, 28, 19, 0, tzinfo=MARKET_TZ)
MONDAY_19 = datetime(2026, 8, 31, 19, 0, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
PLENTY = 10**12
RESERVE_SESSIONS = 13
KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _gets(client: FakeS3) -> list[str]:
    return [kwargs["Key"] for name, kwargs in client.calls if name == "get_object"]


def _record(root: Path, rel: str, source: str) -> None:
    append_manifest(
        root,
        partition=rel,
        source=source,
        sha256=_sha((root / rel).read_bytes()),
        rows=1,
        fetched_at=None,
    )


class Trimmed:
    """A lake uploaded, trimmed, and uploaded again, with what each stage left."""

    def __init__(self, root: Path, client: FakeS3, uploaded: dict[str, bytes], spy_1: bytes):
        self.root = root
        self.client = client
        # The lake's files as the nightly upload left them, which is what the bucket holds,
        # before the volume trimmed SPY_2 on its own.
        self.uploaded = uploaded
        # SPY_1's bytes, which the lake no longer holds and the bucket still does.
        self.spy_1 = spy_1


def _trimmed_bucket(tmp_path: Path) -> Trimmed:
    """A bucket holding a trimmed ledger, built in the order the issue names.

    1. The real ``first_upload`` seeds the bucket with five partitions, a report, and a refused
       day's journal segment.
    2. ``_trim_away`` trims SPY_1, so its latest ledger line is a trim line. SPY_3 is trimmed
       without its unlink and then gets a restore line, so its latest line is a restore line
       and it stays present.
    3. The real ``nightly_upload`` carries the ledger and the manifest into the bucket.
    4. The volume then trims SPY_2, which the bucket's ledger never hears about.
    """
    lake = (
        FixtureLake(tmp_path / "lake")
        .with_chains("SPY", D1)
        .with_chains("SPY", D2)
        .with_chains("SPY", D3)
        .with_quotes("SPY", D1)
    )
    lake.with_journal_segment(
        "chains", "SPY", "2026-08-27", sample_chains_table(), start_ts="20260827T133000Z", pid=4242
    )
    root = lake.build()
    (root / REPORT).parent.mkdir(parents=True)
    (root / REPORT).write_text('{"report": true}\n')
    _record(root, SEGMENT, "capture")
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    spy_1 = (root / SPY_1).read_bytes()

    _trim_away(root, client, SPY_1)
    _trim_away(root, client, SPY_3, unlink=False)
    with lake_lock(root):
        append_trimmed(
            root,
            restore_line(SPY_3, sha256=_sha((root / SPY_3).read_bytes()), restored_at=STAMP),
            source="test-restore",
            fetched_at=STAMP,
        )
    nightly_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    assert client.body(f"lake/{LEDGER}") == (root / LEDGER).read_bytes()
    uploaded = _files(root)

    _trim_away(root, client, SPY_2)
    return Trimmed(root, client, uploaded, spy_1)


def _bucket_latest(client: FakeS3) -> dict[str, dict]:
    """The bucket manifest's latest entry per partition, read with ``json`` here."""
    lines = client.body("lake/manifest.jsonl").decode().splitlines()
    return {entry["partition"]: entry for entry in map(json.loads, filter(None, lines))}


def _record_in_bucket(client: FakeS3, rel: str, data: bytes) -> None:
    """Append an entry for ``data`` to the bucket's manifest, as an upload would have."""
    entry = {"fetched_at": None, "partition": rel, "rows": 1, "sha256": _sha(data), "source": "x"}
    raw = client.body("lake/manifest.jsonl") + (json.dumps(entry, sort_keys=True) + "\n").encode()
    client.store("lake/manifest.jsonl", raw)


def _config(tmp_path: Path, lake: Path, *, window: str | None = None) -> Path:
    config = write_config(tmp_path, lake)
    extra = KEYS if window is None else KEYS + f"lake_window_sessions: {window}\n"
    config.write_text(config.read_text() + extra)
    return config


def _main(config: Path, client: FakeS3, monkeypatch, dest: Path) -> int:
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    argv = ["restore", str(dest), "--config", str(config), "--target", "s3://lake-backup/lake"]
    return bucket.main(argv, clock=ManualClock(MONDAY_19), calendar=CALENDAR)


def _refused(capsys, exc) -> str:
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.splitlines()
    assert len(lines) == 1, captured.err
    assert lines[0].startswith("restore: ")
    return lines[0]


# -- 1. the exclusion ----------------------------------------------------------------


def test_the_exclusion_restores_everything_but_the_designed_absences(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    dest = tmp_path / "restored"

    summary = restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    assert summary.restored is True
    assert summary.failures == []
    # Everything the bucket held when the ledger uploaded: the refused day's segment, the
    # report, SPY_3 under its restore line, and SPY_2, which the bucket's ledger never trimmed.
    assert _files(dest) == lake.uploaded
    assert (dest / SEGMENT).is_file() and (dest / REPORT).is_file()
    assert (dest / SPY_3).is_file() and (dest / SPY_2).is_file()
    assert not (dest / SPY_1).exists()
    assert f"lake/{SPY_1}" not in _gets(lake.client)
    assert summary.trimmed_left_out == 1
    assert summary.trimmed_lost == []
    assert summary.segments_left_out == 0
    assert summary.render().endswith(
        "already verified in the working directory, 1 partition(s) trimmed on purpose left "
        "out, 0 compacted journal segment(s) left out"
    )


def test_the_summary_line_names_the_left_out_count_before_the_lost_one():
    summary = bucket.RestoreSummary(
        target="s3://lake-backup/lake",
        dest=Path("/restored"),
        work=Path("/restored") / WORK,
        trimmed_left_out=2,
        trimmed_lost=["a"],
    )

    assert summary.render() == (
        "restored 0 file(s) from s3://lake-backup/lake into /restored: downloaded 0 (0.0 MB), "
        "0 already verified in the working directory, 2 partition(s) trimmed on purpose left "
        "out, 1 partition(s) trimmed on purpose and missing from the bucket, 0 compacted "
        "journal segment(s) left out"
    )


def test_a_whole_lake_restore_of_the_same_bucket_brings_the_trimmed_partition_back(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    dest = tmp_path / "restored"

    summary = restore_lake(dest, TARGET, client=lake.client)

    assert summary.restored is True
    assert _files(dest) == {**lake.uploaded, SPY_1: lake.spy_1}
    assert summary.trimmed_left_out == 0
    assert "trimmed on purpose" not in summary.render()


# -- 2. the exclusion with no ledger -------------------------------------------------


def test_the_exclusion_with_no_ledger_entry_restores_everything(tmp_path):
    root = FixtureLake(tmp_path / "lake").with_chains("SPY", D1).with_quotes("SPY", D1).build()
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    assert LEDGER not in _bucket_latest(client)
    dest = tmp_path / "restored"

    summary = restore_lake(dest, TARGET, client=client, skip_designed_absences=True)

    assert summary.restored is True
    assert summary.failures == []
    assert _files(dest) == _files(root)
    assert f"lake/{LEDGER}" not in _gets(client)


# -- 3. a bad bucket ledger ----------------------------------------------------------


def _mismatched(tmp_path: Path) -> tuple[Trimmed, bytes]:
    """A bucket whose manifest records a ledger no version of the bucket holds.

    The entry is for bytes never uploaded, so no earlier version matches it either, and a
    fallback to older versions could not make a test here pass. The bytes are a whole ledger
    that parses: the bucket's own with one more restore line for SPY_3.
    """
    lake = _trimmed_bucket(tmp_path)
    extra = restore_line(SPY_3, sha256=_sha((lake.root / SPY_3).read_bytes()), restored_at=STAMP)
    never = lake.client.body(f"lake/{LEDGER}") + (json.dumps(extra, sort_keys=True) + "\n").encode()
    _record_in_bucket(lake.client, LEDGER, never)
    assert all(version.body != never for version in lake.client.versions(f"lake/{LEDGER}"))
    return lake, never


def test_a_mismatched_ledger_refuses_and_leaves_the_working_directory_for_the_repair(tmp_path):
    lake, never = _mismatched(tmp_path)
    dest = tmp_path / "restored"
    lake.client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert _sha(never) in message
    assert str(dest / WORK / LEDGER) in message
    assert "When the lake is gone" in message
    assert _gets(lake.client) == ["lake/manifest.jsonl", f"lake/{LEDGER}"]
    assert (dest / WORK / ".marketlake-restore").is_file()
    assert (dest / WORK / "manifest.jsonl").read_bytes() == lake.client.body("lake/manifest.jsonl")
    assert sorted(os.listdir(dest)) == [WORK]
    assert sorted(os.listdir(dest / WORK)) == [".marketlake-restore", "manifest.jsonl"]


def test_only_a_working_ledger_that_matches_the_entry_lets_the_next_run_proceed(tmp_path):
    lake, never = _mismatched(tmp_path)
    dest = tmp_path / "restored"
    with pytest.raises(RestoreRefused):
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    # A ledger that parses and does not match: only the sha check can reject it.
    (dest / WORK / LEDGER).write_bytes(lake.client.body(f"lake/{LEDGER}"))
    with pytest.raises(RestoreRefused, match=_sha(never)):
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    (dest / WORK / LEDGER).write_bytes(never)
    lake.client.calls.clear()
    summary = restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    assert summary.restored is True
    assert summary.failures == []
    assert summary.trimmed_left_out == 1
    assert (dest / LEDGER).read_bytes() == never
    assert not (dest / SPY_1).exists()
    # The matching working copy stands in for the bucket's, so nothing reads the bucket's.
    assert f"lake/{LEDGER}" not in _gets(lake.client)


def test_a_ledger_the_bucket_lacks_refuses_before_any_download(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    del lake.client.objects[f"lake/{LEDGER}"]
    lake.client.calls.clear()
    dest = tmp_path / "restored"

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert "holds no current trimmed.jsonl" in message
    assert "Putting a version back" in message
    assert _gets(lake.client) == ["lake/manifest.jsonl", f"lake/{LEDGER}"]
    assert not dest.exists()


@pytest.mark.parametrize(
    "damage",
    [
        pytest.param(lambda ledger: b"\xff\n", id="bad-utf8"),
        pytest.param(lambda ledger: b"\xef\xbb\xbf" + ledger, id="byte-order-mark"),
        pytest.param(lambda ledger: b'{"kind": "tr\n' + ledger, id="hidden-tear"),
        pytest.param(lambda ledger: ledger + b'{"kind": "trim"}\n', id="no-partition"),
    ],
)
def test_a_ledger_whose_matching_bytes_do_not_parse_refuses_naming_the_damage(
    tmp_path, monkeypatch, capsys, damage
):
    # Each kind of damage the parse and the resolve refuse is a different ``ManifestError``, so
    # a catch narrowed to one of them lets the others out of ``main`` as a traceback.
    lake = _trimmed_bucket(tmp_path)
    damaged = damage(lake.client.body(f"lake/{LEDGER}"))
    lake.client.store(f"lake/{LEDGER}", damaged)
    _record_in_bucket(lake.client, LEDGER, damaged)
    config = _config(tmp_path, lake.root, window="22")
    lake.client.calls.clear()
    dest = tmp_path / "restored"

    with pytest.raises(SystemExit) as exc:
        _main(config, lake.client, monkeypatch, dest)

    line = _refused(capsys, exc)
    assert "matches its manifest entry and cannot be read" in line
    assert "trimmed on purpose" in line
    assert "Putting a version back" not in line
    assert _gets(lake.client) == ["lake/manifest.jsonl", f"lake/{LEDGER}"]
    assert not dest.exists()


def test_a_matching_working_ledger_that_does_not_parse_refuses_as_damaged_everywhere(tmp_path):
    # The manifest's entry is for damaged bytes the bucket's current ledger is not, so the first
    # run refuses on the mismatch and leaves the working directory. A copy of the recorded bytes
    # put there matches the entry and still cannot be read, so no copy anywhere can be.
    lake = _trimmed_bucket(tmp_path)
    damaged = b"\xff\n"
    _record_in_bucket(lake.client, LEDGER, damaged)
    dest = tmp_path / "restored"
    with pytest.raises(RestoreRefused, match=_sha(damaged)):
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)
    (dest / WORK / LEDGER).write_bytes(damaged)
    lake.client.calls.clear()

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert str(dest / WORK / LEDGER) in message
    assert "every copy of that version is damaged the same way" in message
    assert f"lake/{LEDGER}" not in _gets(lake.client)
    assert sorted(os.listdir(dest)) == [WORK]


@pytest.fixture
def restore_chmod():
    """Paths whose mode a test changed, put back to writable after it, pass or fail."""
    changed: list[Path] = []
    yield changed
    for path in changed:
        path.chmod(0o755 if path.is_dir() else 0o644)


def _skip_as_root() -> None:
    if os.geteuid() == 0:
        pytest.skip("root ignores file modes, so chmod cannot make the local failure")


def test_a_working_ledger_that_will_not_read_is_one_local_line(tmp_path, restore_chmod):
    _skip_as_root()
    lake, _never = _mismatched(tmp_path)
    dest = tmp_path / "restored"
    with pytest.raises(RestoreRefused):
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)
    working = dest / WORK / LEDGER
    working.write_bytes(lake.client.body(f"lake/{LEDGER}"))
    working.chmod(0)
    restore_chmod.append(working)

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert message.startswith(f"reading {LEDGER} in {dest / WORK} failed (PermissionError: ")
    assert message.endswith(", so nothing was restored")


def test_a_mismatch_that_cannot_create_the_working_directory_is_one_local_line(
    tmp_path, restore_chmod
):
    _skip_as_root()
    lake, _never = _mismatched(tmp_path)
    parent = tmp_path / "parent"
    parent.mkdir()
    parent.chmod(0o555)
    restore_chmod.append(parent)
    dest = parent / "restored"

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert message.startswith(f"creating {dest / WORK} failed (PermissionError: ")
    assert message.endswith(", so nothing was restored")
    assert not dest.exists()


def test_a_mismatch_that_cannot_write_the_manifest_is_one_local_line(tmp_path, restore_chmod):
    _skip_as_root()
    lake, _never = _mismatched(tmp_path)
    dest = tmp_path / "restored"
    with pytest.raises(RestoreRefused):
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)
    manifest = dest / WORK / "manifest.jsonl"
    manifest.chmod(0o444)
    restore_chmod.append(manifest)

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(dest, TARGET, client=lake.client, skip_designed_absences=True)

    message = str(refused.value)
    assert message.startswith(
        f"writing manifest.jsonl into {dest / WORK} failed (PermissionError: "
    )
    assert message.endswith(", so nothing was restored")


def test_a_resumed_restore_with_an_unrecorded_ledger_in_the_working_directory(tmp_path):
    # The bucket's manifest records no trimmed.jsonl, while the bucket holds one, so the first
    # windowed run downloads it as an unrecorded file into the working directory. A failed file
    # stops that run, and the next run resumes beside the ledger it left.
    root = FixtureLake(tmp_path / "lake").with_chains("SPY", D1).with_quotes("SPY", D1).build()
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    assert LEDGER not in _bucket_latest(client)
    client.store(f"lake/{LEDGER}", b"a ledger the manifest does not record\n")
    good = client.body(f"lake/{SPY_1}")
    client.store(f"lake/{SPY_1}", good + b"rot")
    dest = tmp_path / "restored"

    first = restore_lake(dest, TARGET, client=client, skip_designed_absences=True)
    assert first.restored is False
    assert first.failures == [(SPY_1, "does not match its SHA-256")]
    assert (dest / WORK / LEDGER).is_file()
    client.store(f"lake/{SPY_1}", good)

    second = restore_lake(dest, TARGET, client=client, skip_designed_absences=True)

    assert second.restored is True
    assert second.failures == []
    assert (dest / LEDGER).read_bytes() == b"a ledger the manifest does not record\n"
    assert (dest / SPY_1).read_bytes() == good


@pytest.mark.parametrize(
    ("failure", "says"),
    [
        pytest.param(
            client_error("AccessDenied", "GetObject", 403),
            "the bucket refused the request (AccessDenied)",
            id="refused",
        ),
        pytest.param(unreachable(), "the bucket could not be reached", id="unreachable"),
    ],
)
def test_a_failed_get_of_the_ledger_alone_is_the_transport_line(
    tmp_path, monkeypatch, capsys, failure, says
):
    # Injected on the ledger's own GET. ``FakeS3.fail_with`` fails the listing first and would
    # never reach the ledger.
    lake = _trimmed_bucket(tmp_path)
    real_get = lake.client.get_object

    def get_object(**kwargs):
        if kwargs["Key"] == f"lake/{LEDGER}":
            raise failure
        return real_get(**kwargs)

    lake.client.get_object = get_object
    config = _config(tmp_path, lake.root, window="22")

    with pytest.raises(SystemExit) as exc:
        _main(config, lake.client, monkeypatch, tmp_path / "restored")

    line = _refused(capsys, exc)
    assert says in line
    assert "trimmed on purpose" not in line
    assert not (tmp_path / "restored").exists()


def test_the_helper_raises_its_mismatch_refusal_rather_than_the_restore_s(tmp_path):
    lake, never = _mismatched(tmp_path)

    with pytest.raises(BucketLedgerMismatch) as refused:
        read_bucket_trimmed(lake.client, TARGET, _bucket_latest(lake.client))

    assert isinstance(refused.value, BucketRefusal)
    assert not isinstance(refused.value, RestoreRefused)
    assert refused.value.sha256 == _sha(never)


def test_the_helper_answers_each_partition_s_latest_line_from_the_bucket(tmp_path):
    lake = _trimmed_bucket(tmp_path)

    latest = read_bucket_trimmed(lake.client, TARGET, _bucket_latest(lake.client))

    assert {rel: line["kind"] for rel, line in latest.items()} == {
        SPY_1: "trim",
        SPY_3: "restore",
    }
    assert read_bucket_trimmed(lake.client, TARGET, {}) == {}


# -- 4. which hosts exclude ----------------------------------------------------------


def test_the_window_key_decides_the_mode_through_main(tmp_path, monkeypatch, capsys):
    lake = _trimmed_bucket(tmp_path)
    windowed = tmp_path / "windowed"
    whole = tmp_path / "whole"

    assert _main(_config(tmp_path, lake.root, window="22"), lake.client, monkeypatch, windowed) == 0
    out = capsys.readouterr().out
    assert "1 partition(s) trimmed on purpose left out" in out
    assert _files(windowed) == lake.uploaded

    assert _main(_config(tmp_path, lake.root), lake.client, monkeypatch, whole) == 0
    out = capsys.readouterr().out
    assert "trimmed on purpose" not in out
    assert _files(whole) == {**lake.uploaded, SPY_1: lake.spy_1}


def _never_built(cfg):
    pytest.fail("main built a client before it judged lake_window_sessions")


@pytest.mark.parametrize("value", ["many", "3"], ids=["not-a-number", "under-the-floor"])
def test_a_bad_window_key_refuses_with_one_line_before_any_request(
    tmp_path, monkeypatch, capsys, value
):
    # Building the client fails the test, so the key has to be judged before ``connect``,
    # which fetches the instance profile's credentials on the VM.
    lake = _trimmed_bucket(tmp_path)
    monkeypatch.setattr(bucket, "client_from_config", _never_built)
    config = _config(tmp_path, lake.root, window=value)
    argv = ["restore", str(tmp_path / "restored"), "--config", str(config)]
    argv += ["--target", "s3://lake-backup/lake"]

    with pytest.raises(SystemExit) as exc:
        bucket.main(argv, clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    line = _refused(capsys, exc)
    assert "lake_window_sessions" in line
    assert not (tmp_path / "restored").exists()


@pytest.mark.parametrize("command", ["first-upload", "live-check"])
def test_a_bad_window_key_breaks_no_command_but_the_restore(tmp_path, monkeypatch, capsys, command):
    root = FixtureLake(tmp_path / "lake").with_chains("SPY", D1).build()
    client = FakeS3()
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    config = _config(tmp_path, root, window="many")
    target = "s3://lake-backup/lake" if command == "first-upload" else "s3://lake-backup/probe"
    argv = [command, "--config", str(config), "--target", target]

    assert bucket.main(argv, clock=ManualClock(MONDAY_19), calendar=CALENDAR) == 0

    assert "lake_window_sessions" not in capsys.readouterr().err
    assert client.calls != []


def test_a_keyless_host_never_reads_the_ledger_and_fails_on_a_mismatch_with_exit_1(
    tmp_path, monkeypatch, capsys
):
    # A nightly upload stopped between the ledger's PUT and the manifest's leaves the bucket's
    # current trimmed.jsonl newer than the manifest's entry, while the version the entry names
    # is still in the bucket. The whole-lake restore downloads the current version like any
    # manifested file and fails it in the download loop. That exit 1 is marketlake #838's gap,
    # and #838's fix, which falls back to the matching version, rewrites this assertion.
    lake = _trimmed_bucket(tmp_path)
    recorded = lake.client.body(f"lake/{LEDGER}")
    assert _sha(recorded) == _bucket_latest(lake.client)[LEDGER]["sha256"]
    extra = restore_line(SPY_3, sha256=_sha((lake.root / SPY_3).read_bytes()), restored_at=STAMP)
    lake.client.store(
        f"lake/{LEDGER}", recorded + (json.dumps(extra, sort_keys=True) + "\n").encode()
    )
    assert [version.body for version in lake.client.versions(f"lake/{LEDGER}")][-2] == recorded

    code = _main(_config(tmp_path, lake.root), lake.client, monkeypatch, tmp_path / "restored")

    assert code == 1
    err = capsys.readouterr().err.splitlines()
    assert err[0] == f"restore: does not match its SHA-256: {LEDGER}"
    assert len(err) == 2


# -- 5. a lost trimmed object --------------------------------------------------------


def test_a_designed_absence_the_bucket_lost_is_named_and_fails_nothing(
    tmp_path, monkeypatch, capsys
):
    lake = _trimmed_bucket(tmp_path)
    del lake.client.objects[f"lake/{SPY_1}"]

    code = _main(
        _config(tmp_path, lake.root, window="22"), lake.client, monkeypatch, tmp_path / "restored"
    )

    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert lines[0] == (
        "restore: trimmed on purpose and missing from the bucket, so it was left out. The "
        f"bucket holds no current version of it: {SPY_1}"
    )
    assert "1 partition(s) trimmed on purpose and missing from the bucket, " in lines[1]
    assert lines[1].endswith("0 compacted journal segment(s) left out")
    assert len(lines) == 2
    assert _files(tmp_path / "restored") == lake.uploaded


def test_the_lost_designed_absence_is_counted_and_a_whole_restore_still_fails_it(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    del lake.client.objects[f"lake/{SPY_1}"]

    windowed = restore_lake(
        tmp_path / "windowed", TARGET, client=lake.client, skip_designed_absences=True
    )
    whole = restore_lake(tmp_path / "whole", TARGET, client=lake.client)

    assert windowed.restored is True
    assert windowed.trimmed_lost == [SPY_1]
    assert windowed.trimmed_left_out == 0
    assert windowed.failures == []
    assert whole.restored is False
    assert whole.failures == [(SPY_1, "missing from the bucket")]


def test_a_restored_partition_the_bucket_lost_still_fails_the_exclusion(
    tmp_path, monkeypatch, capsys
):
    # SPY_3's latest ledger line is a restore line, so it is not a designed absence even
    # though the ledger names it.
    lake = _trimmed_bucket(tmp_path)
    del lake.client.objects[f"lake/{SPY_3}"]

    code = _main(
        _config(tmp_path, lake.root, window="22"), lake.client, monkeypatch, tmp_path / "restored"
    )

    assert code == 1
    captured = capsys.readouterr()
    assert captured.err.splitlines()[0] == f"restore: missing from the bucket: {SPY_3}"
    assert "trimmed on purpose and missing" not in captured.out
    assert not (tmp_path / "restored" / "manifest.jsonl").exists()


# -- 6. the journal reserve ----------------------------------------------------------


def _sizes(client: FakeS3) -> dict[str, int]:
    """Each object's current size, read off the fake bucket's own store."""
    return {
        key.removeprefix("lake/"): len(versions[-1].body)
        for key, versions in client.objects.items()
    }


def _reserve(client: FakeS3) -> int:
    """13 times the busiest sealed day in the bucket, with the days written out here.

    The refused day holds only its journal segment, which is no sealed byte, and the ledgers
    name no day.
    """
    sizes = _sizes(client)
    days = {
        D1: [SPY_1, QUOTES_1, REPORT],
        D2: [SPY_2],
        D3: [SPY_3],
    }
    return RESERVE_SESSIONS * max(sum(sizes[rel] for rel in rels) for rels in days.values())


def _needed(client: FakeS3, *, left_out: tuple[str, ...]) -> int:
    return sum(size for rel, size in _sizes(client).items() if rel not in left_out)


def test_room_for_the_plan_but_not_the_reserve_refuses_with_the_lake_volume_steps(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    needed = _needed(lake.client, left_out=(SPY_1,))
    reserve = _reserve(lake.client)

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(
            tmp_path / "short",
            TARGET,
            client=lake.client,
            skip_designed_absences=True,
            free_space=lambda path: needed + reserve - 1,
        )
    summary = restore_lake(
        tmp_path / "enough",
        TARGET,
        client=lake.client,
        skip_designed_absences=True,
        free_space=lambda path: needed + reserve,
    )

    message = str(refused.value)
    assert "short of the reserve" in message
    assert "Free that much or use a larger filesystem" in message
    assert "raise lake_volume_gib" in message and "resize2fs" in message
    assert "A whole-lake restore needs more room, not less" in message
    assert not (tmp_path / "short").exists()
    assert summary.restored is True


def test_the_whole_lake_restore_names_a_larger_filesystem_without_the_volume_steps(tmp_path):
    lake = _trimmed_bucket(tmp_path)
    needed = _needed(lake.client, left_out=())
    reserve = _reserve(lake.client)

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(
            tmp_path / "short",
            TARGET,
            client=lake.client,
            free_space=lambda path: needed + reserve - 1,
        )
    summary = restore_lake(
        tmp_path / "enough", TARGET, client=lake.client, free_space=lambda path: needed + reserve
    )

    message = str(refused.value)
    assert "short of the reserve" in message
    assert "Free that much or use a larger filesystem" in message
    assert "lake_volume_gib" not in message
    assert summary.restored is True


@pytest.mark.parametrize("windowed", [True, False], ids=["exclusion", "whole-lake"])
def test_a_destination_short_of_the_plan_gets_the_free_space_line(tmp_path, windowed):
    lake = _trimmed_bucket(tmp_path)
    needed = _needed(lake.client, left_out=(SPY_1,) if windowed else ())

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(
            tmp_path / "short",
            TARGET,
            client=lake.client,
            skip_designed_absences=windowed,
            free_space=lambda path: needed - 1,
        )

    message = str(refused.value)
    assert "MB free, so nothing was restored" in message
    assert "reserve" not in message


def test_free_space_equal_to_the_plan_is_judged_by_the_reserve(tmp_path):
    # At exactly the planned bytes free the plan fits, so what refuses is the reserve, which
    # this bucket's busiest sealed day makes nonzero.
    lake = _trimmed_bucket(tmp_path)
    needed = _needed(lake.client, left_out=(SPY_1,))
    assert _reserve(lake.client) > 0

    with pytest.raises(RestoreRefused) as refused:
        restore_lake(
            tmp_path / "exact",
            TARGET,
            client=lake.client,
            skip_designed_absences=True,
            free_space=lambda path: needed,
        )

    message = str(refused.value)
    assert "short of the reserve" in message
    assert "MB free, so nothing was restored" not in message


def test_free_space_equal_to_the_plan_restores_a_bucket_with_no_sealed_day(tmp_path):
    # The lake holds only a refused day's journal segment, which is no sealed byte, so the
    # reserve is 0 and exactly the plan's bytes free is enough.
    lake = FixtureLake(tmp_path / "lake")
    lake.with_journal_segment(
        "chains", "SPY", "2026-08-27", sample_chains_table(), start_ts="20260827T133000Z", pid=4242
    )
    root = lake.build()
    _record(root, SEGMENT, "capture")
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    assert sorted(_sizes(client)) == [SEGMENT, "manifest.jsonl"]
    needed = _needed(client, left_out=())

    with pytest.raises(RestoreRefused, match="MB free, so nothing was restored"):
        restore_lake(tmp_path / "short", TARGET, client=client, free_space=lambda path: needed - 1)
    summary = restore_lake(
        tmp_path / "exact", TARGET, client=client, free_space=lambda path: needed
    )

    assert summary.restored is True
    assert _files(tmp_path / "exact") == _files(root)


@pytest.mark.parametrize(
    ("windowed", "tail"),
    [
        pytest.param(False, "", id="whole-lake"),
        pytest.param(
            True,
            ". On the hosted VM, raise lake_volume_gib, apply the infrastructure, and rerun the "
            "bootstrap so resize2fs grows the filesystem. A whole-lake restore needs more room, "
            "not less",
            id="exclusion",
        ),
    ],
)
def test_the_reserve_line_puts_each_figure_in_its_own_place(windowed, tail):
    # Each figure is a different whole number of megabytes, so a figure printed in another's
    # place shows. The fixture bucket's sizes all round to 0.0 MB and cannot tell them apart.
    line = bucket._reserve_refusal(
        Path("/lake"),
        needed=5_000_000,
        busiest=1_000_000,
        free=10_000_000,
        short=8_000_000,
        windowed=windowed,
    )

    assert line == (
        "the restore needs 5.0 MB, and the next session's journal needs a reserve of 13.0 MB "
        "beside it, 13 times the busiest sealed day in the bucket (1.0 MB). The filesystem "
        "holding /lake has 10.0 MB free, 8.0 MB short of the reserve, so nothing was restored. "
        "Free that much or use a larger filesystem" + tail
    )
