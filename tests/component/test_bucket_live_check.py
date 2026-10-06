"""The live check's own logic, against the fake S3 client.

``python -m lake.bucket live-check`` runs by hand against the owner's real bucket, and
the suite never reaches one. What can be checked here is that the check reads each
answer the right way: it passes a bucket that behaves as S3 documents, and it fails each
of the four behaviors when the bucket breaks it. The command exits 1 when any one fails.
A live check that passed everything would prove nothing on the day it runs for real.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lake import bucket
from lake.bucket import live_check
from lake.config import BucketTarget
from tests.support.bucket import FakeS3, client_error
from tests.support.clock import ManualClock
from tests.support.config import write_config

TARGET = BucketTarget(bucket="lake-backup", prefix="live-check")
KEYS = (
    "bucket_access_key_id: AKIDCONFIG\n"
    "bucket_secret_access_key: secret-bucket-key\n"
    "bucket_region: us-east-2\n"
)


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
    # The check runs on the VM too, where the credentials come from an instance profile,
    # so the line names the credentials rather than a key.
    assert lines[-1].endswith("The bucket's credentials cannot delete")


def test_credentials_that_cannot_read_old_versions_are_sent_to_the_console():
    passed, lines = _run(FakeS3(deny_versioned_get=True))
    assert passed
    sent = [line for line in lines if "confirm by hand in the console" in line]
    assert len(sent) == 1
    assert "The bucket's credentials hold no s3:GetObjectVersion" in sent[0]


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


class _DeniesMismatch(FakeS3):
    """A key whose policy turns away the probe that sends a mismatched checksum."""

    def put_object(self, **kwargs):
        if kwargs["Key"].endswith("/refused"):
            self.calls.append(("put_object", dict(kwargs)))
            raise client_error("AccessDenied", "PutObject", 403)
        return super().put_object(**kwargs)


def test_a_refusal_other_than_bad_digest_fails_behavior_one():
    # Only BadDigest says S3 compared the checksum. A refused key says nothing about it.
    passed, lines = _run(_DeniesMismatch())
    assert not passed
    assert "live-check: FAIL 1 mismatched ChecksumSHA256 refused with AccessDenied" in lines


class _SplitsPuts(FakeS3):
    """A client whose ``put_object`` sends two requests, the way a multipart upload does."""

    def put_object(self, **kwargs):
        self.meta.events.fire("before-send.s3.PutObject")
        return super().put_object(**kwargs)


def test_a_put_sent_as_two_requests_fails_behavior_four():
    passed, lines = _run(_SplitsPuts())
    assert not passed
    assert any(line.startswith("live-check: FAIL 4 ") for line in lines)
    assert any("sent 2 request(s)" in line for line in lines)


class _CompositeHead(FakeS3):
    """A bucket whose ``HeadObject`` reports every stored checksum as a composite."""

    def head_object(self, **kwargs):
        response = super().head_object(**kwargs)
        if "ChecksumSHA256" in response:
            response["ChecksumType"] = "COMPOSITE"
        return response


def test_a_composite_checksum_fails_behavior_two():
    # The value still decodes to the probe's digest. Only its type says it is a checksum
    # of parts, which the manifest's digest can never be compared with.
    passed, lines = _run(_CompositeHead())
    assert not passed
    assert any(line.startswith("live-check: FAIL 2 ") for line in lines)


class _KeepsNoOldBytes(FakeS3):
    """A bucket that hands out new version ids and answers every version with the newest bytes."""

    def get_object(self, **kwargs):
        kwargs.pop("VersionId", None)
        return super().get_object(**kwargs)


def test_an_old_version_holding_the_new_bytes_fails_behavior_three():
    passed, lines = _run(_KeepsNoOldBytes())
    assert not passed
    assert "live-check: FAIL 3 the first version still holds the first PUT's bytes" in lines


@pytest.mark.parametrize(("fake", "code"), [(FakeS3, 0), (_DeniesMismatch, 1)])
def test_the_command_exits_one_when_any_behavior_fails(tmp_path, monkeypatch, capsys, fake, code):
    config = write_config(tmp_path, tmp_path / "lake")
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: fake())
    argv = ["live-check", "--config", str(config), "--target", "s3://lake-backup/live-check"]
    clock = ManualClock(datetime(2026, 10, 5, 23, 0, tzinfo=UTC))
    assert bucket.main(argv, clock=clock) == code
    assert ("live-check: FAIL" in capsys.readouterr().out) == (code == 1)
