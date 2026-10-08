"""The Sunday scrub of a bucket target, against the fake S3 client.

``bucket.bucket_scrub`` gives the path scrub's findings and its ping rules. The cases
follow the issue's list, marketlake #639.

1. The prefix check, which also names where a copy diverged.
2. The forward pass, by ``HeadObject`` in checksum mode, inside the watermark.
3. The reverse pass, by listing, skipping ``SCRUB_EXCLUSIONS``.
4. A bucket that refuses or cannot be reached, each under its own name, never raising.
5. The versioning line, which withholds nothing.

Then the Sunday job itself, with a bucket target, from ``sunday_maintenance`` up to
``control_plane.main``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path

import pytest
import urllib3
from botocore.exceptions import IncompleteReadError

from lake import bucket
from lake import control_plane as cp
from lake.bucket import bucket_scrub, first_upload
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.manifest import BackupScrubResult, append_manifest, manifest_path, sha256_file
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake, sample_chains_table
from tests.support.pinger import FakePinger

DAY = date(2026, 8, 28)
TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
PARTITION = "chains/ticker=SPY/date=2026-08-28.parquet"
CALENDAR = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7))
SUNDAY_20 = et(2026, 8, 30, 20, 0)
FRESH_MINT = et(2026, 8, 30, 19, 30)
URL = "https://hc-ping.com/secret-key/sunday"


def _key(rel: str) -> str:
    return TARGET.key(rel)


def _seed(lake: Path, client: FakeS3) -> None:
    first_upload(
        lake,
        TARGET,
        client=client,
        clock=ManualClock(datetime(2026, 8, 28, 19, 0, tzinfo=MARKET_TZ)),
        calendar=CALENDAR,
    )


def _uploaded(root: Path) -> tuple[Path, FakeS3]:
    lake = FixtureLake(root).with_chains("SPY", DAY).with_quotes("SPY", DAY).build()
    client = FakeS3()
    _seed(lake, client)
    return lake, client


def test_a_clean_bucket_scrubs_clean(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert result.notes == ()
    assert result.target == "s3://lake-backup/lake"


# -- 1. the prefix check ---------------------------------------------------------


def test_a_bucket_with_no_manifest_copy_withholds(tmp_path):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).build()
    result = bucket_scrub(lake, TARGET, FakeS3())
    assert result.manifest_missing and not result.ok


def test_a_diverged_copy_names_the_byte(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    raw = manifest_path(lake).read_bytes()
    client.store(_key("manifest.jsonl"), raw[:10] + b"#" + raw[11:])
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_diverged_at == 10
    assert "diverged" in result.problem


def test_a_copy_behind_the_lake_reads_as_pending_not_loss(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    extra = lake / "chains" / "ticker=QQQ" / "date=2026-08-28.parquet"
    extra.parent.mkdir(parents=True)
    extra.write_bytes(b"later")
    append_manifest(
        lake,
        partition="chains/ticker=QQQ/date=2026-08-28.parquet",
        source="compaction",
        sha256=sha256_file(extra),
        rows=1,
        fetched_at=None,
    )
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert result.pending == ("chains/ticker=QQQ/date=2026-08-28.parquet",)


@pytest.mark.parametrize("length", [0, 5])
def test_a_copy_carrying_no_whole_entry_is_a_missing_manifest(tmp_path, length):
    # Zero bytes, or a stub shorter than one line, is a prefix of any manifest with a
    # watermark of 0. Read as a copy, every object in the bucket would sit past it, so
    # the scrub names the copy missing instead.
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("manifest.jsonl"), manifest_path(lake).read_bytes()[:length])
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_missing
    assert result.unaccounted == () and result.missing == ()
    assert not result.ok


def test_a_copy_stored_without_a_checksum_is_downloaded_and_found_whole(tmp_path):
    # No stored SHA-256 proves no prefix, so the scrub downloads the copy. Bytes equal to
    # the lake's manifest have not diverged.
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("manifest.jsonl"), manifest_path(lake).read_bytes(), checksum=None)
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_diverged_at is None
    assert result.ok
    assert "get_object" in [name for name, _ in client.calls]


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file with no permission bits")
def test_an_unreadable_lake_manifest_is_a_finding_and_never_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    path = manifest_path(lake)
    path.chmod(0)
    try:
        result = bucket_scrub(lake, TARGET, client)
    finally:
        path.chmod(0o644)
    assert result.unreadable is not None and result.unreadable.startswith("PermissionError")
    assert not result.ok


# -- 2. the forward pass ---------------------------------------------------------


def test_an_overwritten_object_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), b"overwritten")
    result = bucket_scrub(lake, TARGET, client)
    assert result.sha_mismatches == (PARTITION,)
    assert not result.ok


def test_an_object_with_no_stored_checksum_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), (lake / PARTITION).read_bytes(), checksum=None)
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_composite_checksum_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    digest = base64.b64encode(hashlib.sha256((lake / PARTITION).read_bytes()).digest()).decode()
    client.store(
        _key(PARTITION),
        (lake / PARTITION).read_bytes(),
        checksum=f"{digest}-3",
        checksum_type="COMPOSITE",
    )
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_full_object_digest_with_a_composite_type_is_a_mismatch(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    digest = base64.b64encode(hashlib.sha256((lake / PARTITION).read_bytes()).digest()).decode()
    client.store(
        _key(PARTITION), (lake / PARTITION).read_bytes(), checksum=digest, checksum_type="COMPOSITE"
    )
    assert bucket_scrub(lake, TARGET, client).sha_mismatches == (PARTITION,)


def test_a_deleted_object_is_missing(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    del client.objects[_key(PARTITION)]
    result = bucket_scrub(lake, TARGET, client)
    assert result.missing == (PARTITION,)
    assert not result.ok


class _NoSuchKeyHeads(FakeS3):
    """A bucket whose ``HeadObject`` on a missing key answers ``NoSuchKey`` rather than 404."""

    def head_object(self, **kwargs):
        if not self.objects.get(kwargs["Key"]):
            self.calls.append(("head_object", dict(kwargs)))
            raise client_error("NoSuchKey", "HeadObject", 404)
        return super().head_object(**kwargs)


def test_a_missing_object_answered_as_no_such_key_is_missing(tmp_path):
    lake, uploaded = _uploaded(tmp_path / "lake")
    client = _NoSuchKeyHeads()
    client.objects = uploaded.objects
    del client.objects[_key(PARTITION)]
    result = bucket_scrub(lake, TARGET, client)
    assert result.missing == (PARTITION,)
    assert result.bucket_failed is None


SEGMENT_DAY = date(2026, 8, 31)


def _record(lake: Path, rel: str, source: str) -> str:
    append_manifest(
        lake, partition=rel, source=source, sha256=sha256_file(lake / rel), rows=1, fetched_at=None
    )
    return rel


def _manifest_segment(lake: Path) -> str:
    """Write a chains journal segment for ``SEGMENT_DAY`` and record it as capture does."""
    fixture = FixtureLake(lake)
    path = fixture.segment_path("chains", "SPY", SEGMENT_DAY, "20260831T133000Z", 4242)
    fixture.with_journal_segment(
        "chains", "SPY", SEGMENT_DAY, sample_chains_table(), start_ts="20260831T133000Z", pid=4242
    )
    return _record(lake, path.relative_to(lake).as_posix(), "capture")


def _seal_segment_day(lake: Path) -> str:
    """Seal ``SEGMENT_DAY``'s chains partition and record it as compaction does."""
    rel = f"chains/ticker=SPY/date={SEGMENT_DAY.isoformat()}.parquet"
    (lake / rel).write_bytes(b"sealed chains")
    return _record(lake, rel, "compaction")


def test_a_segment_whose_partition_is_in_the_copy_is_not_looked_for(tmp_path):
    # Compaction sealed the day and unlinked the segment, so neither the disk nor the
    # bucket holds it. The partition carries the day, so nothing is missing.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).build()
    segment = _manifest_segment(lake)
    _seal_segment_day(lake)
    (lake / segment).unlink()
    client = FakeS3()
    _seed(lake, client)

    result = bucket_scrub(lake, TARGET, client)

    assert _key(segment) not in client.keys()
    assert result.missing == ()
    assert result.ok


def test_a_segment_whose_partition_is_only_pending_is_still_looked_for(tmp_path):
    # The copy carries the segment and not yet the partition sealed from it, so the
    # segment is the bucket's only copy of that day and must be there.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", DAY).build()
    segment = _manifest_segment(lake)
    client = FakeS3()
    _seed(lake, client)
    partition = _seal_segment_day(lake)
    del client.objects[_key(segment)]

    result = bucket_scrub(lake, TARGET, client)

    assert result.pending == (partition,)
    assert result.missing == (segment,)
    assert not result.ok


# -- 3. the reverse pass ---------------------------------------------------------


def test_an_object_the_lake_never_recorded_is_an_orphan_that_does_not_withhold(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("chains/stray.parquet"), b"stray")
    result = bucket_scrub(lake, TARGET, client)
    assert result.orphans == ("chains/stray.parquet",)
    assert result.ok


def test_an_object_recorded_past_the_watermark_is_unaccounted(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    rel = "chains/ticker=QQQ/date=2026-08-28.parquet"
    (lake / rel).parent.mkdir(parents=True)
    (lake / rel).write_bytes(b"later")
    append_manifest(
        lake,
        partition=rel,
        source="compaction",
        sha256=sha256_file(lake / rel),
        rows=1,
        fetched_at=None,
    )
    client.store(_key(rel), b"later")
    result = bucket_scrub(lake, TARGET, client)
    assert result.unaccounted == (rel,)
    assert not result.ok


def test_reports_and_the_journal_are_skipped_by_the_reverse_pass(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key("reports/date=2026-08-28.md"), b"r")
    client.store(_key("journal/metadata.json"), b"{}")
    assert bucket_scrub(lake, TARGET, client).orphans == ()


def test_objects_outside_the_prefix_are_not_the_lakes(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.store("live-check-20261005T000000Z/probe", b"probe")
    assert bucket_scrub(lake, TARGET, client).orphans == ()


def test_a_listing_across_several_pages_is_read_whole(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.page_size = 1
    client.store(_key("chains/stray.parquet"), b"stray")
    assert bucket_scrub(lake, TARGET, client).orphans == ("chains/stray.parquet",)


# -- 4. a bucket that refuses or cannot be reached --------------------------------


@pytest.mark.parametrize("code", ["AccessDenied", "InvalidAccessKeyId", "403"])
def test_a_refused_credential_is_named_refused_and_never_raises(tmp_path, code):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", 403)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_refused == code
    assert result.bucket_unreachable is None
    assert result.problem == (
        f"backup bucket refused the scrub ({code}), so the bucket's credentials or their "
        "policy may need replacing: s3://lake-backup/lake"
    )


def test_unusable_settings_are_named_in_full():
    result = BackupScrubResult(target=str(TARGET), bucket_unusable="bucket_region is not set")
    assert result.problem == "backup bucket settings cannot be used: bucket_region is not set"


def test_a_failed_connection_is_named_unreachable_and_never_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = unreachable()
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == "EndpointConnectionError"
    assert result.bucket_refused is None
    assert "could not be reached" in result.problem


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("SignatureDoesNotMatch", 403),
        ("ExpiredToken", 400),
        ("AccountProblem", 403),
        ("Forbidden", 403),
    ],
)
def test_every_credential_code_is_named_refused(tmp_path, code, status):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", status)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_refused == code


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("SlowDown", 503),
        ("ServiceUnavailable", 503),
        ("InternalError", 500),
        ("TooManyRequests", 429),
        ("Throttling", 400),
        ("RequestTimeout", 400),
        ("SomethingNew", 502),
        ("SomeOtherLimit", 429),
    ],
)
def test_a_busy_or_failing_service_is_named_unavailable_not_refused(tmp_path, code, status):
    # A 503 SlowDown is S3 asking for fewer requests, which no new credentials repair.
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error(code, "HeadObject", status)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == code
    assert result.bucket_refused is None
    assert "the bucket's credentials" not in result.problem
    assert "unavailable" in result.problem


def test_any_other_answer_is_named_failed_with_its_code(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = client_error("NoSuchBucket", "HeadObject", 404)
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_failed == "NoSuchBucket"
    assert result.bucket_refused is None and result.bucket_unreachable is None
    assert "the bucket's credentials" not in result.problem
    assert "NoSuchBucket" in result.problem


def test_the_versioning_line_survives_a_scrub_that_stops_early(tmp_path):
    # The bucket answers, so its versioning status is known, even though the scrub
    # stops at the missing manifest before the forward pass.
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = "Suspended"
    del client.objects[_key("manifest.jsonl")]
    result = bucket_scrub(lake, TARGET, client)
    assert result.manifest_missing
    assert any(line.startswith("bucket versioning is Suspended") for line in result.notes)


class _HeadsFail(FakeS3):
    """A bucket whose versioning answers and whose object requests then fail."""

    def head_object(self, **kwargs):
        raise unreachable()


def test_the_versioning_line_survives_a_bucket_that_fails_mid_scrub(tmp_path):
    lake, uploaded = _uploaded(tmp_path / "lake")
    client = _HeadsFail(versioning="Suspended")
    client.objects = uploaded.objects
    result = bucket_scrub(lake, TARGET, client)
    assert result.bucket_unreachable == "EndpointConnectionError"
    assert any(line.startswith("bucket versioning is Suspended") for line in result.notes)


class _VersioningRefused(FakeS3):
    """A key that may do everything but read the bucket's versioning status."""

    def get_bucket_versioning(self, **kwargs):
        self.calls.append(("get_bucket_versioning", dict(kwargs)))
        raise client_error("AccessDenied", "GetBucketVersioning", 403)


def test_a_refused_versioning_read_is_a_report_line_and_nothing_else(tmp_path):
    # Every object request answers, so the bucket is fine. The one refusal is the
    # versioning read, which withholds nothing and is not the bucket refusing the key.
    lake, uploaded = _uploaded(tmp_path / "lake")
    client = _VersioningRefused()
    client.objects = uploaded.objects
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok, result.problem
    assert result.bucket_refused is None
    assert result.notes == ("bucket versioning unreadable (AccessDenied): s3://lake-backup/lake",)


def test_a_bug_still_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = ZeroDivisionError("a real bug")
    with pytest.raises(ZeroDivisionError):
        bucket_scrub(lake, TARGET, client)


# -- 5. versioning ---------------------------------------------------------------


@pytest.mark.parametrize("status", ["Suspended", None])
def test_versioning_that_is_not_enabled_is_a_report_line_that_withholds_nothing(tmp_path, status):
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = status
    result = bucket_scrub(lake, TARGET, client)
    assert result.ok
    assert any(line.startswith("bucket versioning is") for line in result.notes)


def test_versioning_never_enabled_says_so(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = None
    assert bucket_scrub(lake, TARGET, client).notes == (
        "bucket versioning is never enabled, so an overwrite or a delete keeps no old "
        "version: s3://lake-backup/lake",
    )


# -- the Sunday job --------------------------------------------------------------


class _Pushes:
    """A transport recording each push. The real one POSTs to ntfy."""

    def __init__(self) -> None:
        self.sent = []

    def send(self, message) -> None:
        self.sent.append(message)


def _sunday(lake: Path, client: FakeS3):
    pinger = FakePinger()
    outcome = cp.sunday_maintenance(
        lake_root=lake,
        backup_target=TARGET,
        bucket_client=client,
        now=SUNDAY_20,
        calendar=CALENDAR,
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
        ping_url=URL,
        mint=FRESH_MINT,
        canary=lambda: True,
    )
    return outcome, pinger


def test_the_sunday_job_pings_on_a_clean_bucket(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    outcome, pinger = _sunday(lake, client)
    assert outcome.problems == ()
    assert pinger.urls == [URL]


def test_the_sunday_job_withholds_the_ping_for_an_unreachable_bucket_and_runs_the_rest(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = unreachable()
    outcome, pinger = _sunday(lake, client)
    assert pinger.urls == []
    assert any("could not be reached" in problem for problem in outcome.problems)
    # The canary and the coverage assertion still ran.
    assert outcome.canary_passed and outcome.covered is True


def test_the_sunday_job_reports_suspended_versioning_and_still_pings(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.versioning = "Suspended"
    outcome, pinger = _sunday(lake, client)
    assert pinger.urls == [URL]
    assert any("versioning is Suspended" in line for line in outcome.report)


# A damaged lake manifest reaches the bucket scrub too, marketlake #441. ``read_ledger`` decodes
# with a replacement and reads a missing ``sha256`` with ``entry.get``, so only a line naming no
# partition raises from it, and the Sunday job's guard turns that into the backup's own
# unreadable finding. The bucket's copy carries the same damaged bytes, so the prefix check
# passes and each cell reaches the read it is about. ``first_upload`` refuses a lake whose
# manifest names no partition or no ``sha256``, so the copy is stored directly rather than
# uploaded after the damage.


def _damaged_with_its_copy(root: Path, old: bytes, new: bytes) -> tuple[Path, FakeS3]:
    lake, client = _uploaded(root)
    path = manifest_path(lake)
    raw = path.read_bytes()
    assert old in raw
    damaged = raw.replace(old, new, 1)
    path.write_bytes(damaged)
    client.store(_key("manifest.jsonl"), damaged)
    return lake, client


@pytest.mark.parametrize(
    ("old", "new", "lake_line", "backup_lines"),
    [
        pytest.param(
            b"capture",
            b"captur\xff",
            lambda lake: "lake scrub could not run: LedgerNotUtf8: ",
            lambda lake: [],
            id="not utf-8",
        ),
        pytest.param(
            b'"partition"',
            b'"partitioX"',
            lambda lake: (
                f"lake scrub could not run: ManifestError: {manifest_path(lake)}: entry 1 names "
                "no partition"
            ),
            lambda lake: [
                "backup could not be read: ManifestError: "
                f"{manifest_path(lake)}: entry 1 names no partition"
            ],
            id="no partition",
        ),
        pytest.param(
            b'"sha256"',
            b'"sha25X"',
            lambda lake: "lake scrub could not run: KeyError: 'sha256'",
            lambda lake: [
                "backup scrub failed: missing=0 sha_mismatches=1 unaccounted=0: s3://lake-backup/lake"
            ],
            id="no sha256",
        ),
    ],
)
def test_a_damaged_lake_manifest_is_a_problem_line_on_a_bucket_target(
    tmp_path, old, new, lake_line, backup_lines
):
    lake, client = _damaged_with_its_copy(tmp_path / "lake", old, new)

    outcome, pinger = _sunday(lake, client)

    assert outcome.scrub is None
    first, *rest = outcome.problems
    assert first.startswith(lake_line(lake))
    assert rest == backup_lines(lake)
    if outcome.backup.unreadable is not None:
        # The guard's result: no restore test, and not the shadow host's skip line.
        assert outcome.restore is None
    assert cp.BACKUP_SCRUB_SKIPPED not in outcome.report
    assert pinger.urls == []
    assert outcome.canary_passed and outcome.covered is True


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file with no permission bits")
def test_an_unreadable_lake_manifest_is_named_by_the_lake_and_the_bucket_scrub(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    path = manifest_path(lake)
    path.chmod(0)
    try:
        outcome, pinger = _sunday(lake, client)
    finally:
        path.chmod(0o644)

    assert outcome.scrub is None
    lake_line, backup_line = outcome.problems
    assert lake_line.startswith("lake scrub could not run: PermissionError: ")
    assert backup_line.startswith("backup could not be read: PermissionError: ")
    assert outcome.restore is None
    assert cp.BACKUP_SCRUB_SKIPPED not in outcome.report
    assert pinger.urls == []
    assert outcome.canary_passed and outcome.covered is True


# The restore test's bucket half, marketlake #640. Week 34's rotation slot holds neither
# fixture file, so the test reads the next slot that holds one, which is the quotes
# partition's. ``tests/component/test_control_plane_sunday.py`` asserts the same pick for
# a path target.
QUOTES = "quotes/ticker=SPY/date=2026-08-28.parquet"
STALE_MINT = et(2026, 8, 27, 18, 0)  # last week's late mint: valid, not fresh


def _gets(client: FakeS3, rel: str) -> int:
    keys = [kwargs["Key"] for name, kwargs in client.calls if name == "get_object"]
    return keys.count(_key(rel))


def _rot(client: FakeS3, rel: str, lake: Path) -> bytes:
    """Serve wrong bytes under the checksum S3 stored for the true ones, and return those.

    That is rot at rest. ``HeadObject`` still reports what S3 accepted at upload, so the
    scrub matches the object and only a download can see the damage.
    """
    good = (lake / rel).read_bytes()
    checksum = base64.b64encode(hashlib.sha256(good).digest()).decode()
    client.store(_key(rel), b"rotted bytes", checksum=checksum)
    return good


def test_a_bucket_target_downloads_the_week_s_file_and_verifies_it(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.calls.clear()

    outcome, pinger = _sunday(lake, client)

    assert outcome.backup.matched == (
        (PARTITION, hashlib.sha256((lake / PARTITION).read_bytes()).hexdigest()),
        (QUOTES, hashlib.sha256((lake / QUOTES).read_bytes()).hexdigest()),
    )
    assert outcome.restore is not None
    assert outcome.restore.restored == (QUOTES,)
    assert outcome.restore.bytes_read == (lake / QUOTES).stat().st_size
    assert _gets(client, QUOTES) == 1 and _gets(client, PARTITION) == 0
    assert outcome.restore.pass_line.startswith(
        "1 file (0.0 MB) read back from s3://lake-backup/lake matched the manifest, "
        "week of Sunday 2026-08-30"
    )
    assert outcome.problems == () and outcome.report == ()
    assert pinger.urls == [URL]


def test_a_rotted_object_the_scrub_matched_fails_the_restore_and_withholds(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    _rot(client, QUOTES, lake)

    outcome, pinger = _sunday(lake, client)

    assert outcome.backup.ok is True
    assert outcome.restore.mismatches == (QUOTES,)
    assert f"restore test failed: mismatches=1 unreadable=0: {TARGET}" in outcome.problems
    assert f"restore read back bytes that do not match the manifest: {QUOTES}" in outcome.report
    # The advice names the bucket, never the disk-and-cable advice a path target gets.
    assert any("the bytes S3 served" in line for line in outcome.report)
    assert not any("cable" in line for line in outcome.report)
    # A sealed partition is written once, so it has no earlier version to recover. The
    # repair is the lake's own copy, put back with a whole-object SHA-256.
    assert any(
        "put the lake's own copy back with aws s3api put-object --checksum-algorithm SHA256" in line
        for line in outcome.report
    )
    assert pinger.urls == []


def test_a_refused_download_is_a_named_finding_and_never_raises(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")

    def refused(**kwargs):
        raise client_error("AccessDenied", "GetObject", 403)

    client.get_object = refused
    outcome, pinger = _sunday(lake, client)

    assert outcome.restore.unreadable == (
        f"{QUOTES}: BucketReadError: the bucket refused the read (AccessDenied)"
    )
    assert any("Check the network and the bucket's credentials" in line for line in outcome.report)
    # The canary still ran after the failed read.
    assert outcome.canary_passed is True
    assert pinger.urls == []


def test_a_read_cut_off_mid_body_is_an_os_error(tmp_path):
    _, client = _uploaded(tmp_path / "lake")

    class _Cut:
        def __init__(self) -> None:
            self.reads = 0

        def read(self, n: int) -> bytes:
            self.reads += 1
            if self.reads > 1:
                raise IncompleteReadError(actual_bytes=1, expected_bytes=2)
            return b"x"

        def close(self) -> None:
            pass

    client.get_object = lambda **kwargs: {"Body": _Cut()}
    reader = bucket.bucket_reader(client, TARGET)

    with pytest.raises(OSError, match="could not be reached or was unavailable"):
        list(reader(QUOTES))


def test_an_ssl_failure_mid_body_is_an_os_error(tmp_path):
    # botocore's StreamingBody wraps a timeout and a dropped connection, and lets the
    # rest of urllib3's errors through. An SSLError is neither a BotoCoreError nor an
    # OSError, so unmapped it would escape the Sunday job's OSError catch.
    _, client = _uploaded(tmp_path / "lake")

    class _Body:
        def read(self, n: int) -> bytes:
            raise urllib3.exceptions.SSLError("decryption failed or bad record mac")

        def close(self) -> None:
            pass

    client.get_object = lambda **kwargs: {"Body": _Body()}
    with pytest.raises(OSError, match=r"could not be reached or was unavailable \(SSLError\)"):
        list(bucket.bucket_reader(client, TARGET)(QUOTES))


def test_a_bug_in_the_download_is_not_mapped_to_an_os_error(tmp_path):
    _, client = _uploaded(tmp_path / "lake")

    def broken(**kwargs):
        raise KeyError("Body")

    client.get_object = broken
    with pytest.raises(KeyError):
        list(bucket.bucket_reader(client, TARGET)(QUOTES))


class _Mints:
    """Hands back one mint per attempt, repeating the last once the list runs out."""

    def __init__(self, *mints):
        self.mints = list(mints)
        self.reads = 0

    def __call__(self):
        mint = self.mints[min(self.reads, len(self.mints) - 1)]
        self.reads += 1
        return mint


def _sunday_run(lake: Path, client: FakeS3, *, mints, canary=lambda: True):
    pinger = FakePinger()
    outcomes = cp.sunday_run(
        lake_root=lake,
        backup_target=TARGET,
        bucket_client=client,
        clock=ManualClock(start=SUNDAY_20),
        calendar=CALENDAR,
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
        ping_url=URL,
        mint_reader=mints,
        canary=canary,
        token_pull=None,
    )
    return outcomes, pinger


def test_a_pass_is_reused_by_the_next_attempt_and_the_line_says_so(tmp_path):
    # 20:00 finds last week's token, so the evening retries at 20:30. The restore passed
    # at 20:00, so 20:30 hands that pass back rather than downloading the file again.
    lake, client = _uploaded(tmp_path / "lake")
    client.calls.clear()

    outcomes, pinger = _sunday_run(lake, client, mints=_Mints(STALE_MINT, FRESH_MINT))

    assert len(outcomes) == 2
    assert _gets(client, QUOTES) == 1
    first, second = (outcome.restore for outcome in outcomes)
    assert first.reused is False and not first.pass_line.endswith("rather than read again")
    assert second.reused is True
    assert second.restored == (QUOTES,)
    assert second.pass_line.endswith(
        ", reused from an earlier attempt this evening rather than read again"
    )
    # The scrub itself still ran on both attempts.
    assert all(outcome.backup.walked for outcome in outcomes)
    assert pinger.urls == [URL]


def test_a_failing_restore_is_not_kept_and_the_next_attempt_reads_again(tmp_path):
    # The 20:00 restore reads rotted bytes. The canary stands in for a repair made before
    # 20:30, which puts the true bytes back, so only a fresh read can pass.
    lake, client = _uploaded(tmp_path / "lake")
    good = _rot(client, QUOTES, lake)
    stored = client.versions(_key(QUOTES))[-1].checksum
    repaired: list[bool] = []

    def repair() -> bool:
        if not repaired:
            client.store(_key(QUOTES), good, checksum=stored)
            repaired.append(True)
        return True

    client.calls.clear()
    outcomes, pinger = _sunday_run(lake, client, mints=_Mints(FRESH_MINT), canary=repair)

    assert len(outcomes) == 2
    assert outcomes[0].restore.mismatches == (QUOTES,)
    assert outcomes[1].restore.ok is True and outcomes[1].restore.reused is False
    assert _gets(client, QUOTES) == 2
    assert pinger.urls == [URL]


def test_a_pass_with_one_file_wrong_is_not_kept(tmp_path):
    # These two paths share rotation slot 10, so week 34 reads both. At 20:00 one reads
    # back right and one rotted, so the attempt fails with a file restored. The repair
    # before 20:30 means only a fresh read of both can pass.
    aapl = "chains/ticker=AAPL/date=2026-08-28.parquet"
    qqq = "chains/ticker=QQQ/date=2026-08-28.parquet"
    lake = FixtureLake(tmp_path / "lake").with_chains("AAPL", DAY).with_chains("QQQ", DAY).build()
    client = FakeS3()
    _seed(lake, client)
    good = _rot(client, qqq, lake)
    stored = client.versions(_key(qqq))[-1].checksum
    repaired: list[bool] = []

    def repair() -> bool:
        if not repaired:
            client.store(_key(qqq), good, checksum=stored)
            repaired.append(True)
        return True

    client.calls.clear()
    outcomes, pinger = _sunday_run(lake, client, mints=_Mints(FRESH_MINT), canary=repair)

    assert outcomes[0].restore.restored == (aapl,)
    assert outcomes[0].restore.mismatches == (qqq,)
    assert len(outcomes) == 2
    assert outcomes[1].restore.restored == (aapl, qqq)
    assert outcomes[1].restore.reused is False
    assert _gets(client, aapl) == 2 and _gets(client, qqq) == 2
    assert pinger.urls == [URL]


def test_the_reader_closes_each_body_it_opens(tmp_path):
    _, client = _uploaded(tmp_path / "lake")
    closed: list[bool] = []

    class _Body:
        def __init__(self, chunks: list[bytes], fail: bool) -> None:
            self.chunks = chunks
            self.fail = fail

        def read(self, n: int) -> bytes:
            if self.chunks:
                return self.chunks.pop(0)
            if self.fail:
                raise IncompleteReadError(actual_bytes=1, expected_bytes=2)
            return b""

        def close(self) -> None:
            closed.append(True)

    reader = bucket.bucket_reader(client, TARGET)
    client.get_object = lambda **kwargs: {"Body": _Body([b"a", b"b"], fail=False)}
    assert b"".join(reader(QUOTES)) == b"ab"
    client.get_object = lambda **kwargs: {"Body": _Body([b"a"], fail=True)}
    with pytest.raises(OSError):
        list(reader(QUOTES))
    assert closed == [True, True]


def test_a_kept_pass_over_other_files_is_not_reused(tmp_path):
    # At 20:00 the quotes object is overwritten, so the scrub leaves it out and the week
    # reads the chains partition instead. Putting the quotes object back before 20:30
    # changes the week's files, so the kept pass covers the wrong ones and 20:30 reads.
    lake, client = _uploaded(tmp_path / "lake")
    good = client.versions(_key(QUOTES))[-1]
    client.store(_key(QUOTES), b"overwritten")
    repaired: list[bool] = []

    def repair() -> bool:
        if not repaired:
            client.store(_key(QUOTES), good.body, checksum=good.checksum)
            repaired.append(True)
        return True

    client.calls.clear()
    outcomes, pinger = _sunday_run(lake, client, mints=_Mints(FRESH_MINT), canary=repair)

    assert len(outcomes) == 2
    assert outcomes[0].restore.restored == (PARTITION,)
    assert outcomes[1].restore.restored == (QUOTES,)
    assert outcomes[1].restore.reused is False
    assert _gets(client, QUOTES) == 1
    assert pinger.urls == [URL]


def test_a_bucket_scrub_that_stopped_early_adds_no_restore_line(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = unreachable()
    outcome, _ = _sunday(lake, client)
    assert outcome.backup.walked is False
    assert outcome.restore is None
    assert not any(line.startswith("restore") for line in outcome.report + outcome.problems)


def test_a_bucket_target_without_a_client_is_a_caller_error(tmp_path):
    lake, _ = _uploaded(tmp_path / "lake")
    with pytest.raises(ValueError, match="bucket_client"):
        _sunday(lake, None)


_KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


def _sunday_cli(tmp_path, monkeypatch, lake: Path, *, target=str(TARGET), keys=_KEYS):
    """Run ``control_plane.main sunday`` over a config naming ``target``.

    Every seam that reaches past the process is replaced. The canary records that it
    ran, so a test can show the job carried on past a bucket finding.
    """
    config = write_config(tmp_path, lake)
    text = config.read_text().replace(
        f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {target}"
    )
    config.write_text(text + keys)
    pinger = FakePinger()
    canaries: list[str] = []

    def canary(**kwargs):
        def run():
            canaries.append("ran")
            return True

        return run

    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: "")
    monkeypatch.setattr("lake.runner.UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", canary)
    monkeypatch.setattr("lake.alert.NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", lambda targets: "")
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"creation_timestamp": FRESH_MINT.timestamp(), "token": {}}))

    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=CALENDAR,
    )
    return code, pinger, canaries


def test_the_sunday_cli_scrubs_the_configured_bucket(tmp_path, capsys, monkeypatch):
    """``control_plane.main`` builds the client from the config and scrubs the bucket.

    The overwritten object is in the bucket alone, so only a scrub that reached the
    bucket can name it. A wiring slip that scrubbed a path instead would fail
    differently.
    """
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), b"overwritten")
    built = []

    def fake_client(cfg):
        built.append(cfg.bucket_secret_access_key.reveal())
        return client

    monkeypatch.setattr(bucket, "client_from_config", fake_client)

    code, pinger, _ = _sunday_cli(tmp_path, monkeypatch, lake)

    assert code == 1
    assert built == ["secret-bucket-key"]
    assert pinger.urls == []
    captured = capsys.readouterr()
    printed = captured.out
    assert f"backup file does not match the lake: {PARTITION}" in printed
    assert "s3://lake-backup/lake" in printed
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in printed and value not in captured.err


def test_the_sunday_cli_restores_from_the_configured_bucket(tmp_path, capsys, monkeypatch):
    # ``main`` passes no reader, so the download is the one built from the config's own
    # client. The pass line is printed, so the log tells a pass from a test that never ran.
    lake, client = _uploaded(tmp_path / "lake")
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    client.calls.clear()

    code, pinger, _ = _sunday_cli(tmp_path, monkeypatch, lake)

    assert code == 0
    assert pinger.urls == [URL]
    assert _gets(client, QUOTES) == 1
    assert (
        "sunday: restore: 1 file (0.0 MB) read back from s3://lake-backup/lake matched the "
        "manifest, week of Sunday 2026-08-30"
    ) in capsys.readouterr().out


def test_the_sunday_publishers_carry_the_bucket_keys(tmp_path, capsys, monkeypatch):
    # The reminder and the refused-ping page go out through this publisher, and it
    # refuses any page holding a value it was handed. The bucket's two key values must
    # be among them, or a page could carry the key to ntfy.
    lake, client = _uploaded(tmp_path / "lake")
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    built: list[dict] = []
    real = cp.Publisher

    def watched(**kwargs):
        built.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(cp, "Publisher", watched)

    _sunday_cli(tmp_path, monkeypatch, lake)

    assert built
    for kwargs in built:
        assert "secret-bucket-key" in kwargs["secrets"]
        assert "AKIDCONFIG" in kwargs["secrets"]
    captured = capsys.readouterr()
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in captured.out and value not in captured.err


@pytest.mark.parametrize(
    ("target", "keys", "named"),
    [
        (str(TARGET), "", "bucket_access_key_id"),
        ("s3://Legacy_Bucket/lake", _KEYS, "names no valid bucket"),
        (str(TARGET), _KEYS.replace("us-east-2", "us east 2"), "bucket_region"),
    ],
)
def test_unusable_bucket_settings_are_a_sunday_finding_and_the_job_runs_on(
    tmp_path, capsys, monkeypatch, target, keys, named
):
    # A traceback here would skip the canary, the coverage assertion and the re-auth
    # reminder. The settings become the backup's finding instead, which withholds the
    # ping, and the real client builder is never reached.
    lake, _ = _uploaded(tmp_path / "lake")
    monkeypatch.setattr(bucket, "_build_client", lambda cfg: pytest.fail("no client is built"))

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake, target=target, keys=keys)

    assert code == 1
    assert pinger.urls == []
    assert canaries
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    finding = [line for line in captured.out.splitlines() if "cannot be used" in line]
    assert finding and all(named in line for line in finding)
    for value in ("secret-bucket-key", "AKIDCONFIG"):
        assert value not in captured.out and value not in captured.err


def test_a_client_that_cannot_be_built_is_a_sunday_finding(tmp_path, capsys, monkeypatch):
    # The region check runs first, and anything botocore still refuses at build time
    # lands the same way rather than as a traceback.
    from botocore.exceptions import InvalidRegionError

    lake, _ = _uploaded(tmp_path / "lake")

    def refuse(cfg):
        raise InvalidRegionError(region_name="us-east-2")

    monkeypatch.setattr(bucket, "_build_client", refuse)

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake)

    assert code == 1
    assert pinger.urls == [] and canaries
    printed = capsys.readouterr().out
    assert "cannot be used" in printed and "InvalidRegionError" in printed


def test_a_whole_object_digest_with_no_checksum_type_still_matches(tmp_path):
    # S3 has not always returned ChecksumType. A value that decodes to the manifest's 32
    # digest bytes cannot be a composite, whose "-N" suffix fails the decode, so it
    # counts. The live check prints the type S3 returns today.
    lake, client = _uploaded(tmp_path / "lake")
    client.store(_key(PARTITION), (lake / PARTITION).read_bytes(), checksum_type=None)
    assert bucket_scrub(lake, TARGET, client).ok
