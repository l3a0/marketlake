"""The restore command, ``python -m lake.bucket restore <dest>``, against the fake S3 client.

A lake is uploaded with the real ``first_upload`` and ``nightly_upload``, then restored
into a directory beside it, so each test reads the bucket the upload actually left. The
cases follow marketlake #640's Done when 1, and the review of its pull request.

1. A round trip restores every file byte for byte, and leaves out a journal segment
   whose day the manifest records as compacted and the console's folder markers.
2. A file that fails verification, is missing, or names a path outside the lake names
   itself and leaves no lake at the destination.
3. A non-empty destination, a symbolic link, a disk too small, a bucket that cannot be
   reached and a directory that cannot be written each refuse with one line and exit 2.
4. A destination holding only ``lost+found``, a fresh volume's mount point, is filled.
5. A second run after a failure resumes, including a run killed while moving files in.
6. A torn last manifest line names the file it held rather than failing the run.
7. A shadow host still restores, since a restore uploads nothing.

Expected bytes and digests are read from the lake on disk or computed with ``hashlib``
here, never through the code under test. The working directory's name is written out
as a literal for the same reason.
"""

from __future__ import annotations

import base64
import hashlib
import os
import shutil
from datetime import date, datetime
from pathlib import Path

import pytest
from botocore.exceptions import IncompleteReadError, ReadTimeoutError

from lake import bucket
from lake.bucket import bucket_scrub, first_upload, nightly_upload, restore_lake
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.manifest import append_manifest
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake, sample_chains_table

DAY = date(2026, 8, 28)
SEGMENT_DAY = date(2026, 8, 31)
TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
CHAINS = "chains/ticker=SPY/date=2026-08-28.parquet"
QUOTES = "quotes/ticker=SPY/date=2026-08-28.parquet"
REPORT = "reports/date=2026-08-28.md"
SEGMENT = "journal/date=2026-08-31/surface=chains/ticker=SPY/seg-20260831T133000Z-4242.arrows"
SEALED = "chains/ticker=SPY/date=2026-08-31.parquet"
WORK = ".marketlake-restoring"
# A Friday evening after the sweep, and the Monday evening after the next session.
FRIDAY_19 = datetime(2026, 8, 28, 19, 0, tzinfo=MARKET_TZ)
MONDAY_19 = datetime(2026, 8, 31, 19, 0, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))

KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


def _record(lake: Path, rel: str, source: str) -> None:
    append_manifest(
        lake,
        partition=rel,
        source=source,
        sha256=_sha((lake / rel).read_bytes()),
        rows=1,
        fetched_at=None,
    )


def _files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _tops(root: Path) -> set[str]:
    """The top-level names that hold a file, which is what a restore brings back."""
    return {rel.split("/")[0] for rel in _files(root)}


def _no_lake(dest: Path) -> None:
    """The destination holds no lake: no ``manifest.jsonl``, nothing but the working directory."""
    assert not (dest / "manifest.jsonl").exists()
    if dest.exists():
        assert set(os.listdir(dest)) <= {WORK}


def _uploaded(tmp_path: Path) -> tuple[Path, FakeS3]:
    """A lake uploaded the way the owner's would be: a first upload, then a nightly one.

    The first upload carries a chains journal segment that capture manifested for
    ``SEGMENT_DAY``. Compaction then seals that day and unlinks the segment, and the
    nightly upload carries the sealed partition. The bucket never deletes, so it still
    holds the segment the lake no longer has.
    """
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).with_quotes("SPY", DAY)
    lake.with_journal_segment(
        "chains", "SPY", SEGMENT_DAY, sample_chains_table(), start_ts="20260831T133000Z", pid=4242
    )
    root = lake.build()
    (root / "reports").mkdir()
    (root / REPORT).write_text("report\n")
    _record(root, SEGMENT, "capture")
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)

    (root / SEALED).write_bytes(b"sealed chains for the day")
    _record(root, SEALED, "compaction")
    (root / SEGMENT).unlink()
    nightly_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)
    assert f"lake/{SEGMENT}" in client.keys()
    return root, client


def _reserve(lake: Path) -> int:
    """The journal reserve the restore keeps free: 13 times the fixture's busiest sealed day.

    The day sizes are the byte lengths ``_files`` reads, which equal the listing's sizes. The
    journal segment is no sealed byte, and the report's name ends in ``.md`` rather than naming
    a ``date=`` directory, so the lake's path rule dates it to no day.
    """
    files = _files(lake)
    return 13 * max(len(files[CHAINS]) + len(files[QUOTES]), len(files[SEALED]))


def _gets(client: FakeS3) -> list[str]:
    return [kwargs["Key"] for name, kwargs in client.calls if name == "get_object"]


def _rot(client: FakeS3, rel: str, lake: Path) -> None:
    """Replace an object's bytes and keep the checksum S3 stored for the true ones.

    That is rot at rest: ``HeadObject`` still reports what S3 accepted at upload, and
    only a download sees the bytes.
    """
    client.store(f"lake/{rel}", b"rotted bytes", checksum=_b64((lake / rel).read_bytes()))


# -- 1. the round trip -----------------------------------------------------------


def test_a_restore_returns_the_lake_byte_for_byte(tmp_path):
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert summary.failures == []
    assert _files(dest) == _files(lake)
    # The compacted day's segment is in the bucket and stays out of the restored lake.
    assert not (dest / SEGMENT).exists()
    assert summary.segments_left_out == 1
    assert f"lake/{SEGMENT}" not in _gets(client)
    assert not (dest / WORK).exists()
    # The report is the one file the manifest does not record, and journal/ and reports/
    # are where the lake keeps those by rule, so nothing is named.
    assert summary.unrecorded == []


def test_a_segment_whose_day_is_not_compacted_is_restored(tmp_path):
    # A day compaction never sealed keeps its segments, and they are its only copy.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY)
    lake.with_journal_segment(
        "chains", "SPY", SEGMENT_DAY, sample_chains_table(), start_ts="20260831T133000Z", pid=4242
    )
    root = lake.build()
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is True
    assert (tmp_path / "restored" / SEGMENT).read_bytes() == (root / SEGMENT).read_bytes()
    assert summary.segments_left_out == 0


def test_an_empty_destination_directory_is_filled(tmp_path):
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    dest.mkdir()

    assert restore_lake(dest, TARGET, client=client).restored is True
    assert _files(dest) == _files(lake)


def test_a_fresh_volume_holding_only_lost_and_found_is_filled(tmp_path):
    # A new host mounts its lake volume at lake_root itself, and a fresh ext4 volume
    # holds lost+found at its root. The restore must seed exactly that directory.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "volume"
    (dest / "lost+found").mkdir(parents=True)

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert _files(dest) == _files(lake)
    assert set(os.listdir(dest)) == {"lost+found", *_tops(lake)}


def test_a_console_folder_marker_is_not_a_file(tmp_path):
    # The S3 console's "Create folder" writes a zero-byte key ending in "/". It names no
    # file, so it neither blocks the restore nor becomes a file named journal.
    lake, client = _uploaded(tmp_path)
    client.store("lake/journal/", b"")

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is True
    assert summary.failures == []
    assert _files(tmp_path / "restored") == _files(lake)


def test_the_command_prints_one_line_and_exits_0(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)

    code = _main(config, client, monkeypatch, tmp_path / "restored")

    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    (line,) = captured.out.splitlines()
    assert line.startswith("restore: restored 5 file(s) from s3://lake-backup/lake into ")
    assert line.endswith("1 compacted journal segment(s) left out")
    assert _files(tmp_path / "restored") == _files(lake)


# -- 2. a file that fails verification -------------------------------------------


def test_a_rotted_object_names_itself_and_leaves_no_lake(tmp_path):
    lake, client = _uploaded(tmp_path)
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    dest.mkdir()

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is False
    assert summary.failures == [(QUOTES, "does not match its SHA-256")]
    _no_lake(dest)
    # The bad bytes never reach the working directory either.
    assert not (dest / WORK / QUOTES).exists()


def test_an_unmanifested_object_is_checked_against_the_stored_checksum(tmp_path):
    # The report carries no manifest entry, so the checksum S3 stored at upload is all
    # there is to verify it against.
    lake, client = _uploaded(tmp_path)
    _rot(client, REPORT, lake)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(REPORT, "does not match its SHA-256")]
    _no_lake(tmp_path / "restored")


def test_an_object_with_no_stored_checksum_cannot_be_verified(tmp_path):
    lake, client = _uploaded(tmp_path)
    client.store(f"lake/{REPORT}", (lake / REPORT).read_bytes(), checksum=None)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(REPORT, "has no SHA-256 stored in the bucket to verify")]
    _no_lake(tmp_path / "restored")


def test_a_missing_object_names_itself_and_leaves_no_lake(tmp_path):
    lake, client = _uploaded(tmp_path)
    del client.objects[f"lake/{QUOTES}"]

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is False
    assert summary.failures == [(QUOTES, "missing from the bucket")]
    _no_lake(tmp_path / "restored")


def test_an_object_gone_between_the_listing_and_its_download_is_missing(tmp_path):
    lake, client = _uploaded(tmp_path)
    real_get = client.get_object

    def get_object(**kwargs):
        if kwargs["Key"] == f"lake/{QUOTES}":
            raise client_error("NoSuchKey", "GetObject", 404)
        return real_get(**kwargs)

    client.get_object = get_object

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(QUOTES, "missing from the bucket")]
    _no_lake(tmp_path / "restored")


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("lake/../escape.txt", id="climbs-out"),
        pytest.param("lake//ESCAPE", id="absolute"),
        pytest.param("lake/chains/./x.parquet", id="dot"),
        pytest.param("lake/.marketlake-restore", id="marker-name"),
    ],
)
def test_a_key_naming_a_path_outside_the_lake_is_never_written(tmp_path, key):
    # A person or another tool can write any key into the bucket. The absolute form
    # names a path under tmp_path, so the test can look for it there.
    lake, client = _uploaded(tmp_path)
    key = key.replace("ESCAPE", str(tmp_path / "escape.txt").lstrip("/"))
    client.store(key, b"evil")
    before = _files(tmp_path)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is False
    assert [why for _, why in summary.failures] == [
        "names a path outside the lake, so it was not written"
    ]
    assert not (tmp_path / "escape.txt").exists()
    after = {rel for rel in _files(tmp_path) if not rel.startswith("restored/")}
    assert after == set(before)
    _no_lake(tmp_path / "restored")


def test_a_symlink_in_the_working_directory_is_never_written_through(tmp_path):
    # Every key here is lexically safe. A symbolic link planted in a working directory
    # left from an earlier run would still carry the chains partition out of it, so the
    # path is checked after links are followed too.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    work = dest / WORK
    work.mkdir(parents=True)
    (work / ".marketlake-restore").write_text("a marketlake restore in progress\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (work / "chains").symlink_to(outside)

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is False
    assert sorted(rel for rel, _ in summary.failures) == [CHAINS, SEALED]
    assert list(outside.iterdir()) == []
    _no_lake(dest)


def test_the_command_names_each_failing_file_and_exits_1(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    _rot(client, QUOTES, lake)
    del client.objects[f"lake/{CHAINS}"]
    config = _config(tmp_path, lake)

    code = _main(config, client, monkeypatch, tmp_path / "restored")

    assert code == 1
    captured = capsys.readouterr()
    lines = captured.err.splitlines()
    assert lines[0] == f"restore: missing from the bucket: {CHAINS}"
    assert lines[1] == f"restore: does not match its SHA-256: {QUOTES}"
    assert lines[2].startswith("restore: 2 file(s) failed, so ")
    assert len(lines) == 3
    _no_lake(tmp_path / "restored")


# -- 3. refusals, each one line and exit 2 ----------------------------------------


def _config(tmp_path: Path, lake: Path, *, role: str | None = None) -> Path:
    config = write_config(tmp_path, lake, role=role)
    config.write_text(config.read_text() + KEYS)
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
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in captured.err
    return lines[0]


def test_a_non_empty_destination_refuses_before_any_request(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    client.calls.clear()

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, lake)

    line = _refused(capsys, exc)
    assert "is not empty" in line
    assert client.calls == []
    assert not (lake / WORK).exists()


def test_lost_and_found_beside_other_files_still_refuses(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    dest = tmp_path / "volume"
    (dest / "lost+found").mkdir(parents=True)
    (dest / "notes.txt").write_text("someone's file\n")
    client.calls.clear()

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, dest)

    assert "is not empty, it holds notes.txt" in _refused(capsys, exc)
    assert client.calls == []
    assert sorted(os.listdir(dest)) == ["lost+found", "notes.txt"]


def test_a_symlinked_destination_refuses_before_any_request(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    client.calls.clear()

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, link)

    assert "is a symbolic link" in _refused(capsys, exc)
    assert client.calls == []
    assert list(real.iterdir()) == []


def test_a_disk_too_small_refuses_before_any_data_file(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    client.calls.clear()
    real = shutil.disk_usage
    monkeypatch.setattr(shutil, "disk_usage", lambda path: real(path)._replace(free=100))

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, tmp_path / "restored")

    line = _refused(capsys, exc)
    assert "MB free, so nothing was restored" in line
    assert _gets(client) == ["lake/manifest.jsonl"]
    assert not (tmp_path / "restored").exists()


def test_free_space_is_measured_on_the_destination_s_own_filesystem(tmp_path):
    # A mount point sits on its own filesystem, so its parent's free space says nothing.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "volume"
    dest.mkdir()
    asked: list[Path] = []

    def free(path: Path) -> int:
        asked.append(path)
        return 10**12

    assert restore_lake(dest, TARGET, client=client, free_space=free).restored is True
    assert asked == [dest]


def test_exactly_enough_free_space_is_enough(tmp_path):
    lake, client = _uploaded(tmp_path)
    needed = sum(len(data) for data in _files(lake).values()) + _reserve(lake)

    summary = restore_lake(
        tmp_path / "restored", TARGET, client=client, free_space=lambda p: needed
    )

    assert summary.restored is True


def test_an_unreachable_bucket_refuses_with_one_line(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    client.fail_with = unreachable()

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, tmp_path / "restored")

    line = _refused(capsys, exc)
    assert "could not be reached" in line
    assert "a re-run resumes in the working directory" in line


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes through permission bits")
def test_a_destination_that_cannot_be_written_refuses_with_one_line(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake)
    parent = tmp_path / "locked"
    parent.mkdir()
    parent.chmod(0o555)
    try:
        with pytest.raises(SystemExit) as exc:
            _main(config, client, monkeypatch, parent / "restored")
    finally:
        parent.chmod(0o755)

    line = _refused(capsys, exc)
    assert "creating " in line and "PermissionError" in line


def test_a_bucket_with_no_manifest_refuses(tmp_path, monkeypatch, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).build()
    config = _config(tmp_path, lake)

    with pytest.raises(SystemExit) as exc:
        _main(config, FakeS3(), monkeypatch, tmp_path / "restored")

    assert "holds no manifest.jsonl" in _refused(capsys, exc)


def test_a_manifest_that_does_not_match_its_stored_checksum_refuses(tmp_path):
    lake, client = _uploaded(tmp_path)
    _rot(client, "manifest.jsonl", lake)

    with pytest.raises(bucket.RestoreRefused, match="manifest.jsonl does not match"):
        restore_lake(tmp_path / "restored", TARGET, client=client)
    assert not (tmp_path / "restored").exists()


def test_a_manifest_with_a_composite_checksum_says_none_is_stored(tmp_path):
    lake, client = _uploaded(tmp_path)
    raw = (lake / "manifest.jsonl").read_bytes()
    client.store("lake/manifest.jsonl", raw, checksum=_b64(raw) + "-2", checksum_type="COMPOSITE")

    with pytest.raises(bucket.RestoreRefused, match="stores no full-object SHA-256"):
        restore_lake(tmp_path / "restored", TARGET, client=client)


def test_a_working_directory_no_restore_made_is_refused(tmp_path):
    lake, client = _uploaded(tmp_path)
    stranger = tmp_path / "restored" / WORK
    stranger.mkdir(parents=True)
    (stranger / "keep.txt").write_text("someone's file\n")

    with pytest.raises(bucket.RestoreRefused, match="no restore made it"):
        restore_lake(tmp_path / "restored", TARGET, client=client)
    assert (stranger / "keep.txt").read_text() == "someone's file\n"


# -- 4. a resumed run -------------------------------------------------------------


def test_a_second_run_resumes_and_downloads_only_what_failed(tmp_path):
    lake, client = _uploaded(tmp_path)
    good = client.versions(f"lake/{QUOTES}")[-1]
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False

    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    client.calls.clear()
    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert summary.downloaded == 1
    assert summary.resumed == 3
    assert sorted(_gets(client)) == ["lake/manifest.jsonl", f"lake/{QUOTES}"]
    assert _files(dest) == _files(lake)


def test_a_wrong_file_in_the_working_directory_is_downloaded_again(tmp_path):
    # A resumed run trusts a file only when its hash matches, never its name or its size.
    lake, client = _uploaded(tmp_path)
    good = client.versions(f"lake/{QUOTES}")[-1]
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False
    damaged = dest / WORK / CHAINS
    damaged.write_bytes(bytes(len(damaged.read_bytes())))

    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    client.calls.clear()
    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert f"lake/{CHAINS}" in _gets(client)
    assert _files(dest) == _files(lake)


def test_a_run_cut_off_mid_download_resumes_without_its_part_file(tmp_path):
    lake, client = _uploaded(tmp_path)
    real_get = client.get_object

    class _Dropped:
        def __init__(self, data: bytes) -> None:
            self.data = data
            self.sent = False

        def read(self, n: int) -> bytes:
            if self.sent:
                raise IncompleteReadError(actual_bytes=1, expected_bytes=len(self.data))
            self.sent = True
            return self.data[:1]

        def close(self) -> None:
            pass

    def dropping(**kwargs):
        response = real_get(**kwargs)
        if kwargs["Key"] == f"lake/{QUOTES}":
            response["Body"] = _Dropped(response["Body"].read())
        return response

    client.get_object = dropping
    dest = tmp_path / "restored"
    with pytest.raises(bucket.BucketReadError):
        restore_lake(dest, TARGET, client=client)
    _no_lake(dest)

    client.get_object = real_get
    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert _files(dest) == _files(lake)


def test_a_resumed_run_drops_a_file_the_bucket_no_longer_wants(tmp_path):
    # The first run restored the segment while its day was not yet compacted, and failed
    # on another file. By the second run the day is compacted, so the segment is stale
    # and must not reach the destination.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).with_quotes("SPY", DAY)
    lake.with_journal_segment(
        "chains", "SPY", SEGMENT_DAY, sample_chains_table(), start_ts="20260831T133000Z", pid=4242
    )
    root = lake.build()
    _record(root, SEGMENT, "capture")
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    good = client.versions(f"lake/{QUOTES}")[-1]
    _rot(client, QUOTES, root)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False
    assert (dest / WORK / SEGMENT).is_file()

    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    (root / SEALED).write_bytes(b"sealed chains for the day")
    _record(root, SEALED, "compaction")
    (root / SEGMENT).unlink()
    nightly_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    # A part file from a download whose file has since left the plan.
    (dest / WORK / (SEGMENT + ".part")).write_bytes(b"half a segment")

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert _files(dest) == _files(root)
    # The stale segment's directories went with it.
    assert all(any(path.iterdir()) for path in dest.rglob("*") if path.is_dir())


def test_a_run_killed_while_moving_files_in_finishes_on_the_next_run(tmp_path, monkeypatch):
    # The second rename fails, so one top-level directory is in the destination and the
    # rest, manifest.jsonl included, are still in the working directory. The next run
    # finds every file verified and finishes the move without a request.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    real_rename = os.rename
    renames: list[str] = []

    def failing_second(src, dst):
        renames.append(str(src))
        if len(renames) == 2:
            raise OSError(28, "No space left on device")
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", failing_second)
    with pytest.raises(
        bucket.RestoreRefused, match="run the same command again, which finishes the move"
    ):
        restore_lake(dest, TARGET, client=client)
    monkeypatch.setattr(os, "rename", real_rename)
    assert not (dest / "manifest.jsonl").exists()
    assert (dest / WORK / "manifest.jsonl").is_file()
    client.calls.clear()

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True and summary.finished_move is True
    assert client.calls == []
    assert _files(dest) == _files(lake)
    assert not (dest / WORK).exists()


def test_manifest_jsonl_is_the_last_entry_moved_in(tmp_path, monkeypatch):
    lake, client = _uploaded(tmp_path)
    real_rename = os.rename
    moved: list[str] = []

    def recording(src, dst):
        moved.append(Path(dst).name)
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", recording)
    assert restore_lake(tmp_path / "restored", TARGET, client=client).restored is True
    assert moved[-1] == "manifest.jsonl"
    assert set(moved) == _tops(lake)


def test_a_run_killed_after_verifying_keeps_its_marker(tmp_path, monkeypatch):
    # A kill after every file verified and before the move must not leave a working
    # directory the next run refuses as one no restore made.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"

    def killed(src, dst):
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(os, "rename", killed)
    with pytest.raises(bucket.RestoreRefused):
        restore_lake(dest, TARGET, client=client)
    monkeypatch.undo()
    assert (dest / WORK / ".marketlake-restore").is_file()

    assert restore_lake(dest, TARGET, client=client).restored is True
    assert _files(dest) == _files(lake)


def test_an_empty_unmarked_working_directory_is_resumable(tmp_path):
    # A run killed between creating the working directory and writing its marker.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    (dest / WORK).mkdir(parents=True)

    assert restore_lake(dest, TARGET, client=client).restored is True
    assert _files(dest) == _files(lake)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads through permission bits")
def test_an_unreadable_working_file_is_named_as_a_read(tmp_path):
    lake, client = _uploaded(tmp_path)
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False
    (dest / WORK / CHAINS).chmod(0o000)
    try:
        with pytest.raises(bucket.RestoreRefused, match=f"reading {CHAINS} in .* failed"):
            restore_lake(dest, TARGET, client=client)
    finally:
        (dest / WORK / CHAINS).chmod(0o644)


# -- 5. a torn last manifest line --------------------------------------------------


def test_a_torn_last_manifest_line_names_its_file_and_still_restores(tmp_path, monkeypatch, capsys):
    # A crash mid-append tears the last line. Every reader discards it, so the quotes
    # partition reads as a file the manifest does not record. The upload sends it under
    # its own SHA-256, and the restore verifies it against that and names it.
    root = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).with_quotes("SPY", DAY).build()
    manifest = root / "manifest.jsonl"
    lines = manifest.read_bytes().splitlines(keepends=True)
    assert QUOTES.encode() in lines[-1]
    manifest.write_bytes(b"".join(lines[:-1]) + lines[-1][:40])
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    config = _config(tmp_path, root)

    code = _main(config, client, monkeypatch, tmp_path / "restored")

    assert code == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == (
        "restore: restored with no manifest entry, verified against the bucket's stored "
        f"SHA-256: {QUOTES}"
    )
    assert out[1].startswith("restore: restored 3 file(s)")
    assert _files(tmp_path / "restored") == _files(root)


# -- 6. either role ---------------------------------------------------------------


def test_a_shadow_host_still_restores(tmp_path, monkeypatch, capsys):
    # The shadow refusal is about uploading under the primary's credentials. A restore uploads
    # nothing and writes only into an empty directory, which is how a new host is seeded.
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake, role="shadow")
    client.calls.clear()

    code = _main(config, client, monkeypatch, tmp_path / "restored")

    assert code == 0
    assert bucket.BUCKET_SHADOW not in capsys.readouterr().err
    assert client.puts() == []
    assert _files(tmp_path / "restored") == _files(lake)


# -- 7. cases the mutation lens found nothing holding -----------------------------


def test_a_segment_compacted_before_the_first_upload_is_not_missing(tmp_path):
    # The real lake's normal state: the manifest records a segment, compaction sealed
    # its day and unlinked it, and only then did the first upload run. The segment never
    # reached the bucket, and the sealed partition stands for it.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).with_quotes("SPY", DAY)
    lake.with_journal_segment(
        "chains", "SPY", SEGMENT_DAY, sample_chains_table(), start_ts="20260831T133000Z", pid=4242
    )
    root = lake.build()
    _record(root, SEGMENT, "capture")
    (root / SEALED).write_bytes(b"sealed chains for the day")
    _record(root, SEALED, "compaction")
    (root / SEGMENT).unlink()
    client = FakeS3()
    first_upload(root, TARGET, client=client, clock=ManualClock(FRIDAY_19), calendar=CALENDAR)
    assert f"lake/{SEGMENT}" not in client.keys()

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == []
    assert summary.restored is True
    assert _files(tmp_path / "restored") == _files(root)


def test_a_segment_is_left_out_by_the_manifest_even_when_its_partition_is_missing(tmp_path):
    # The rule reads the manifest, not the listing. The sealed partition's object is gone,
    # which is its own failure, and the stale segment still stays out.
    lake, client = _uploaded(tmp_path)
    del client.objects[f"lake/{SEALED}"]

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(SEALED, "missing from the bucket")]
    assert summary.segments_left_out == 1


def test_a_manifest_with_no_stored_checksum_refuses(tmp_path):
    lake, client = _uploaded(tmp_path)
    client.store("lake/manifest.jsonl", (lake / "manifest.jsonl").read_bytes(), checksum=None)

    with pytest.raises(bucket.RestoreRefused, match="stores no full-object SHA-256"):
        restore_lake(tmp_path / "restored", TARGET, client=client)
    assert not (tmp_path / "restored").exists()


def test_a_refused_download_partway_is_one_line_and_exit_2(tmp_path, monkeypatch, capsys):
    lake, client = _uploaded(tmp_path)
    real_get = client.get_object

    def get_object(**kwargs):
        if kwargs["Key"] == f"lake/{QUOTES}":
            raise client_error("AccessDenied", "GetObject", 403)
        return real_get(**kwargs)

    client.get_object = get_object
    with pytest.raises(SystemExit) as exc:
        _main(_config(tmp_path, lake), client, monkeypatch, tmp_path / "restored")

    line = _refused(capsys, exc)
    assert "the bucket refused the request (AccessDenied)" in line
    _no_lake(tmp_path / "restored")


def test_a_head_timeout_is_named_as_the_bucket_and_not_the_disk(tmp_path, monkeypatch, capsys):
    # A ReadTimeoutError is an OSError, so it must not read as a failed write.
    lake, client = _uploaded(tmp_path)
    real_head = client.head_object

    def head_object(**kwargs):
        if kwargs["Key"] == f"lake/{REPORT}":
            raise ReadTimeoutError(endpoint_url="https://s3.us-east-2.amazonaws.com")
        return real_head(**kwargs)

    client.head_object = head_object
    with pytest.raises(SystemExit) as exc:
        _main(_config(tmp_path, lake), client, monkeypatch, tmp_path / "restored")

    line = _refused(capsys, exc)
    assert "could not be reached" in line
    assert "writing" not in line


def test_an_unmanifested_object_gone_before_its_head_is_missing(tmp_path):
    lake, client = _uploaded(tmp_path)
    real_head = client.head_object

    def head_object(**kwargs):
        if kwargs["Key"] == f"lake/{REPORT}":
            raise client_error("404", "HeadObject", 404)
        return real_head(**kwargs)

    client.head_object = head_object

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(REPORT, "missing from the bucket")]
    _no_lake(tmp_path / "restored")


def test_a_failed_first_move_leaves_a_directory_the_next_run_finishes(tmp_path, monkeypatch):
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    real_rename = os.rename
    renames: list[str] = []

    def failing_first(src, dst):
        renames.append(str(src))
        if len(renames) == 1:
            raise OSError(18, "Invalid cross-device link")
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", failing_first)
    with pytest.raises(
        bucket.RestoreRefused, match="run the same command again, which finishes the move"
    ):
        restore_lake(dest, TARGET, client=client)
    client.calls.clear()

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True and summary.finished_move is True
    assert client.calls == []
    assert _files(dest) == _files(lake)


def test_one_byte_short_of_the_space_needed_refuses(tmp_path):
    lake, client = _uploaded(tmp_path)
    needed = sum(len(data) for data in _files(lake).values())

    with pytest.raises(bucket.RestoreRefused, match="MB free"):
        restore_lake(tmp_path / "restored", TARGET, client=client, free_space=lambda p: needed - 1)


def test_a_resumed_run_needs_space_only_for_what_is_left(tmp_path):
    lake, client = _uploaded(tmp_path)
    good = client.versions(f"lake/{QUOTES}")[-1]
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False
    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    needed = len((lake / "manifest.jsonl").read_bytes()) + len(good.body) + _reserve(lake)

    summary = restore_lake(dest, TARGET, client=client, free_space=lambda p: needed)

    assert summary.restored is True


def test_a_working_file_of_the_wrong_size_gets_no_space_credit(tmp_path):
    lake, client = _uploaded(tmp_path)
    good = client.versions(f"lake/{QUOTES}")[-1]
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is False
    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    (dest / WORK / CHAINS).write_bytes(b"x")
    without_chains = len((lake / "manifest.jsonl").read_bytes()) + len(good.body)

    with pytest.raises(bucket.RestoreRefused, match="MB free"):
        restore_lake(dest, TARGET, client=client, free_space=lambda p: without_chains)


def test_a_destination_that_is_a_file_refuses_before_any_request(tmp_path):
    lake, client = _uploaded(tmp_path)
    (tmp_path / "restored").write_text("x")
    client.calls.clear()

    with pytest.raises(bucket.RestoreRefused, match="is not a directory"):
        restore_lake(tmp_path / "restored", TARGET, client=client)
    assert client.calls == []


def test_a_working_path_that_is_a_file_refuses_before_any_request(tmp_path):
    lake, client = _uploaded(tmp_path)
    (tmp_path / "restored").mkdir()
    (tmp_path / "restored" / WORK).write_text("x")
    client.calls.clear()

    with pytest.raises(bucket.RestoreRefused, match="is not a directory"):
        restore_lake(tmp_path / "restored", TARGET, client=client)
    assert client.calls == []


def test_a_missing_parent_refuses_before_any_request(tmp_path):
    lake, client = _uploaded(tmp_path)
    client.calls.clear()

    with pytest.raises(bucket.RestoreRefused, match="does not exist"):
        restore_lake(tmp_path / "nowhere" / "restored", TARGET, client=client)
    assert client.calls == []
    assert not (tmp_path / "nowhere").exists()


def test_downloaded_bytes_counts_every_chunk(tmp_path, monkeypatch):
    lake, client = _uploaded(tmp_path)
    monkeypatch.setattr(bucket, "_READ_CHUNK", 3)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    planned = sum(len(data) for rel, data in _files(lake).items() if rel != "manifest.jsonl")
    assert summary.downloaded_bytes == planned
    assert summary.downloaded == 4


def test_a_destination_under_the_home_directory_expands_the_tilde(tmp_path, monkeypatch):
    lake, client = _uploaded(tmp_path)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))

    summary = restore_lake("~/restored", TARGET, client=client)

    assert summary.restored is True
    assert _files(home / "restored") == _files(lake)


def test_the_bucket_scrub_matches_the_rest_when_one_object_is_missing(tmp_path):
    # The restore test samples from what the scrub matched, so one missing object must
    # not empty the set the week reads from.
    lake, client = _uploaded(tmp_path)
    del client.objects[f"lake/{QUOTES}"]

    result = bucket_scrub(lake, TARGET, client)

    assert result.missing == (QUOTES,)
    assert CHAINS in dict(result.matched)


# -- 8. a finishing run that finds things changed ----------------------------------


def _killed_mid_move(tmp_path: Path, monkeypatch) -> tuple[Path, FakeS3, Path]:
    """A restore killed on its second rename: one entry moved in, the rest still waiting.

    ``KeyboardInterrupt`` stands in for the kill, because nothing catches it, so the
    run leaves exactly what a real kill leaves.
    """
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    real_rename = os.rename
    renames: list[str] = []

    def killed_on_second(src, dst):
        renames.append(str(src))
        if len(renames) == 2:
            raise KeyboardInterrupt
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", killed_on_second)
    with pytest.raises(KeyboardInterrupt):
        restore_lake(dest, TARGET, client=client)
    monkeypatch.setattr(os, "rename", real_rename)
    assert (dest / WORK / "manifest.jsonl").is_file()
    return lake, client, dest


def test_a_live_manifest_at_the_destination_stops_the_finishing_run(tmp_path, monkeypatch, capsys):
    # A reboot cut the restore short, and capture started on that root and created its
    # manifest under the lock. Finishing the move would replace a live lake's ledger.
    lake, client, dest = _killed_mid_move(tmp_path, monkeypatch)
    (dest / "manifest.jsonl").write_text('{"live": "entry"}\n')
    config = _config(tmp_path, lake)

    with pytest.raises(SystemExit) as exc:
        _main(config, client, monkeypatch, dest)

    line = _refused(capsys, exc)
    assert "gained a manifest.jsonl" in line and "run the same command again" in line
    assert (dest / "manifest.jsonl").read_text() == '{"live": "entry"}\n'
    assert (dest / WORK / "manifest.jsonl").is_file()


def test_a_name_already_at_the_destination_stops_the_finishing_run(tmp_path, monkeypatch):
    # An empty directory would be silently replaced by the rename, and a full one would
    # fail it halfway. The names compare case-folded, as macOS's filesystem does.
    lake, client, dest = _killed_mid_move(tmp_path, monkeypatch)
    (dest / "Quotes").mkdir()

    with pytest.raises(bucket.RestoreRefused, match="holds Quotes, which the restore is about"):
        restore_lake(dest, TARGET, client=client)
    assert (dest / "Quotes").is_dir() and list((dest / "Quotes").iterdir()) == []
    assert not (dest / "manifest.jsonl").exists()


def test_a_manifest_created_during_the_download_stops_the_move(tmp_path):
    # The same guard runs before the first move of a fresh run, since a daemon can start
    # on the root while the download is still going.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    real_get = client.get_object

    def get_object(**kwargs):
        if kwargs["Key"] == f"lake/{QUOTES}":
            (dest / "manifest.jsonl").write_text('{"live": "entry"}\n')
        return real_get(**kwargs)

    client.get_object = get_object

    with pytest.raises(bucket.RestoreRefused, match="gained a manifest.jsonl"):
        restore_lake(dest, TARGET, client=client)
    assert (dest / "manifest.jsonl").read_text() == '{"live": "entry"}\n'


@pytest.mark.parametrize(
    "damage",
    [
        pytest.param("deleted", id="file-deleted"),
        pytest.param("truncated", id="file-truncated"),
        pytest.param("manifest", id="manifest-deleted"),
    ],
)
def test_a_working_directory_changed_since_it_verified_moves_nothing(tmp_path, monkeypatch, damage):
    lake, client, dest = _killed_mid_move(tmp_path, monkeypatch)
    waiting = dest / WORK / REPORT
    assert waiting.is_file()
    if damage == "deleted":
        waiting.unlink()
    elif damage == "truncated":
        waiting.write_bytes(b"")
    else:
        (dest / WORK / "manifest.jsonl").unlink()
    before = sorted(os.listdir(dest))

    with pytest.raises(bucket.RestoreRefused, match="no longer holds .* Delete .* and run"):
        restore_lake(dest, TARGET, client=client)
    assert sorted(os.listdir(dest)) == before
    assert not (dest / "manifest.jsonl").exists()


@pytest.mark.parametrize("left", [(), (".marketlake-restore",)], ids=["empty", "marker-only"])
def test_a_working_directory_left_beside_a_finished_lake_is_named_as_finished(tmp_path, left):
    # A kill after the last file moved in, while the run removed its own markers.
    lake, client = _uploaded(tmp_path)
    dest = tmp_path / "restored"
    assert restore_lake(dest, TARGET, client=client).restored is True
    (dest / WORK).mkdir()
    for name in left:
        (dest / WORK / name).write_text("a marketlake restore in progress\n")

    with pytest.raises(bucket.RestoreRefused, match="already holds a restored lake"):
        restore_lake(dest, TARGET, client=client)
    assert {rel for rel in _files(dest) if not rel.startswith(WORK)} == set(_files(lake))


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("lake/Manifest.jsonl", id="manifest-in-another-case"),
        pytest.param("lake/.MARKETLAKE-RESTORE", id="marker-in-another-case"),
        pytest.param("lake/x\x00y", id="nul-byte"),
        pytest.param("lake/lost+found/orphan", id="lost-and-found"),
    ],
)
def test_a_key_that_would_clobber_the_restore_s_own_files_is_never_written(tmp_path, key):
    lake, client = _uploaded(tmp_path)
    client.store(key, b"evil")

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [
        (key.removeprefix("lake/"), "names a path outside the lake, so it was not written")
    ]
    _no_lake(tmp_path / "restored")


def test_two_keys_that_differ_only_by_case_are_both_named(tmp_path):
    # On macOS's filesystem one would overwrite the other.
    lake, client = _uploaded(tmp_path)
    other = "reports/DATE=2026-08-28.md"
    client.store(f"lake/{other}", b"another report\n")

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is False
    ((rel, why),) = summary.failures
    named = {rel, why.split(" ")[2]}
    assert named == {REPORT, other}
    assert why.endswith("on a filesystem that ignores case")
    _no_lake(tmp_path / "restored")


def test_a_waiting_name_in_another_case_still_clashes(tmp_path, monkeypatch):
    # Both sides fold case. An unmanifested object under an upper-case directory waits in
    # the working directory, and the destination gained the lower-case name.
    lake, client = _uploaded(tmp_path)
    client.store("lake/Zeta/notes.md", b"notes\n")
    dest = tmp_path / "restored"
    real_rename = os.rename

    def killed(src, dst):
        raise KeyboardInterrupt

    monkeypatch.setattr(os, "rename", killed)
    with pytest.raises(KeyboardInterrupt):
        restore_lake(dest, TARGET, client=client)
    monkeypatch.setattr(os, "rename", real_rename)
    (dest / "zeta").mkdir()

    with pytest.raises(bucket.RestoreRefused, match="holds zeta, which the restore is about"):
        restore_lake(dest, TARGET, client=client)
