"""The live check's own logic, against the fake S3 client.

``python -m lake.bucket live-check`` runs by hand against the owner's real bucket, and
the suite never reaches one. What can be checked here is that the check reads each
answer the right way: it passes a bucket that behaves as S3 documents, and it fails one
that accepts a mismatched checksum or keeps no versions. A live check that passed
everything would prove nothing on the day it runs for real.
"""

from __future__ import annotations

from lake.bucket import live_check
from lake.config import BucketTarget
from tests.support.bucket import FakeS3

TARGET = BucketTarget(bucket="lake-backup", prefix="live-check")


def _run(client: FakeS3) -> tuple[bool, list[str]]:
    lines: list[str] = []
    passed = live_check(client, TARGET, stamp="20261005T230000Z", out=lines.append)
    return passed, lines


def test_a_bucket_that_behaves_as_documented_passes_all_four(tmp_path):
    passed, lines = _run(FakeS3())
    assert passed
    for behavior in ("1", "2", "3", "4"):
        assert any(line.startswith(f"live-check: PASS {behavior} ") for line in lines)
    assert lines[-1].startswith("live-check: delete live-check/live-check-20261005T230000Z/")


def test_a_key_that_cannot_read_old_versions_is_sent_to_the_console():
    passed, lines = _run(FakeS3(deny_versioned_get=True))
    assert passed
    assert any("confirm by hand in the console" in line for line in lines)


class _AcceptsAnything(FakeS3):
    """A bucket that stores whatever it is sent, checksum or not."""

    def put_object(self, **kwargs):
        kwargs = {**kwargs, "ChecksumSHA256": None}
        return super().put_object(**kwargs)


def test_a_bucket_that_accepts_a_mismatched_checksum_fails():
    passed, lines = _run(_AcceptsAnything())
    assert not passed
    assert "live-check: FAIL 1 mismatched ChecksumSHA256 was accepted" in lines


class _Unversioned(FakeS3):
    """A bucket with versioning off answers every PUT with the version id ``null``."""

    def put_object(self, **kwargs):
        response = super().put_object(**kwargs)
        return {**response, "VersionId": "null"}


def test_a_bucket_without_versions_fails_behavior_three():
    passed, lines = _run(_Unversioned())
    assert not passed
    assert any(line.startswith("live-check: FAIL 3 ") for line in lines)
