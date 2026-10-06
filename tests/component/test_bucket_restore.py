"""The restore command, ``python -m lake.bucket restore <dest>``, against the fake S3 client.

A lake is uploaded with the real ``first_upload`` and ``nightly_upload``, then restored
into a directory beside it, so each test reads the bucket the upload actually left. The
cases follow marketlake #640's Done when 1.

1. A round trip restores every file byte for byte, and leaves out a journal segment
   whose day the manifest records as compacted.
2. A file that fails verification, or is missing, names itself and leaves the
   destination as it was.
3. A non-empty destination, a disk too small and a bucket that cannot be reached each
   refuse with one line and exit 2.
4. A second run after a failure resumes in the working directory.
5. A torn last manifest line names the file it held rather than failing the run.
6. A shadow host still restores, since a restore uploads nothing.

Expected bytes and digests are read from the lake on disk or computed with ``hashlib``
here, never through the code under test.
"""

from __future__ import annotations

import base64
import hashlib
import shutil
from datetime import date, datetime
from pathlib import Path

import pytest
from botocore.exceptions import IncompleteReadError

from lake import bucket
from lake.bucket import first_upload, nightly_upload, restore_lake
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
    assert not (tmp_path / "restored.restoring").exists()
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


def test_a_rotted_object_names_itself_and_leaves_the_destination_empty(tmp_path):
    lake, client = _uploaded(tmp_path)
    _rot(client, QUOTES, lake)
    dest = tmp_path / "restored"
    dest.mkdir()

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is False
    assert summary.failures == [(QUOTES, "does not match its SHA-256")]
    assert list(dest.iterdir()) == []
    # The bad bytes never reach the working directory either.
    assert not (tmp_path / "restored.restoring" / QUOTES).exists()


def test_an_unmanifested_object_is_checked_against_the_stored_checksum(tmp_path):
    # The report carries no manifest entry, so the checksum S3 stored at upload is all
    # there is to verify it against.
    lake, client = _uploaded(tmp_path)
    _rot(client, REPORT, lake)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(REPORT, "does not match its SHA-256")]
    assert not (tmp_path / "restored").exists()


def test_an_object_with_no_stored_checksum_cannot_be_verified(tmp_path):
    lake, client = _uploaded(tmp_path)
    client.store(f"lake/{REPORT}", (lake / REPORT).read_bytes(), checksum=None)

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.failures == [(REPORT, "has no SHA-256 stored in the bucket to verify")]
    assert not (tmp_path / "restored").exists()


def test_a_missing_object_names_itself_and_leaves_the_destination_empty(tmp_path):
    lake, client = _uploaded(tmp_path)
    del client.objects[f"lake/{QUOTES}"]

    summary = restore_lake(tmp_path / "restored", TARGET, client=client)

    assert summary.restored is False
    assert summary.failures == [(QUOTES, "missing from the bucket")]
    assert not (tmp_path / "restored").exists()


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
    assert not (tmp_path / "restored").exists()


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
    assert not (tmp_path / "restored").exists()


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
    assert not (tmp_path / "lake.restoring").exists()


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
    assert not (tmp_path / "restored.restoring").exists()


def test_exactly_enough_free_space_is_enough(tmp_path):
    lake, client = _uploaded(tmp_path)
    needed = sum(len(data) for data in _files(lake).values())

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


def test_a_working_directory_no_restore_made_is_refused(tmp_path):
    lake, client = _uploaded(tmp_path)
    stranger = tmp_path / "restored.restoring"
    stranger.mkdir()
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
    assert not dest.exists()

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
    assert (tmp_path / "restored.restoring" / SEGMENT).is_file()

    client.store(f"lake/{QUOTES}", good.body, checksum=good.checksum)
    (root / SEALED).write_bytes(b"sealed chains for the day")
    _record(root, SEALED, "compaction")
    (root / SEGMENT).unlink()
    nightly_upload(root, TARGET, client=client, clock=ManualClock(MONDAY_19), calendar=CALENDAR)

    summary = restore_lake(dest, TARGET, client=client)

    assert summary.restored is True
    assert _files(dest) == _files(root)


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
    # The shadow refusal is about uploading under the primary's keys. A restore uploads
    # nothing and writes only into an empty directory, which is how a new host is seeded.
    lake, client = _uploaded(tmp_path)
    config = _config(tmp_path, lake, role="shadow")
    client.calls.clear()

    code = _main(config, client, monkeypatch, tmp_path / "restored")

    assert code == 0
    assert bucket.BUCKET_SHADOW not in capsys.readouterr().err
    assert client.puts() == []
    assert _files(tmp_path / "restored") == _files(lake)
