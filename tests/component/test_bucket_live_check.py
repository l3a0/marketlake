"""The live check's own logic, against the fake S3 client.

``python -m lake.bucket live-check`` runs by hand against the owner's real bucket, and
the suite never reaches one. What can be checked here is that the check reads each
answer the right way: it passes a bucket that behaves as S3 documents, and it fails each
of the four behaviors when the bucket breaks it. It fails each of the three read grants,
marketlake #737, when the credentials lack one. The command exits 1 when any one fails.
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


def test_a_bucket_that_behaves_as_documented_passes_all_seven(tmp_path):
    passed, lines = _run(FakeS3())
    assert passed
    for behavior in ("1", "2", "3", "4", "5", "6", "7"):
        assert any(line.startswith(f"live-check: PASS {behavior} ") for line in lines)
    assert (
        "live-check: PASS 5 ListObjectsV2 under live-check/live-check-20261005T230000Z/ "
        "listed 1 object(s)"
    ) in lines
    assert "live-check: PASS 6 GetBucketVersioning returned Enabled" in lines
    assert (
        "live-check: PASS 7 GetObject of the probe returned the current version's bytes "
        "with VersionId v2"
    ) in lines
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


class _FirstPutUnversioned(FakeS3):
    """A bucket whose first PUT to a key names no version and whose later PUTs name one.

    That is what a bucket that turned versioning on between the two PUTs answers. The two ids
    differ, so only ``usable_version_id`` tells the first one names nothing.
    """

    def __init__(self, first: str | None):
        super().__init__()
        self.first = first
        self.seen: set[str] = set()

    def put_object(self, **kwargs):
        response = super().put_object(**kwargs)
        if kwargs["Key"] in self.seen:
            return response
        self.seen.add(kwargs["Key"])
        response = {key: value for key, value in response.items() if key != "VersionId"}
        if self.first is not None:
            response["VersionId"] = self.first
        return response


@pytest.mark.parametrize("first", ["null", "NULL", "", None], ids=["null", "NULL", "empty", "none"])
def test_two_puts_whose_first_id_names_no_version_fail_behavior_three(first):
    """Mutation this catches: behavior three judging two ids distinct by ``!=`` alone rather
    than through ``bucket.usable_version_id``, which the trim judges a version by too."""
    passed, lines = _run(_FirstPutUnversioned(first))
    assert not passed
    assert any(line.startswith("live-check: FAIL 3 two PUTs to one key") for line in lines)


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


class _DeniesList(FakeS3):
    """Credentials whose policy lost ``s3:ListBucket``."""

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", dict(kwargs)))
        raise client_error("AccessDenied", "ListObjectsV2", 403)


def test_credentials_that_cannot_list_fail_grant_five():
    passed, lines = _run(_DeniesList())
    assert not passed
    assert "live-check: FAIL 5 ListObjectsV2 refused with AccessDenied" in lines
    # The other two grants are still read, so one run names every missing grant.
    assert any(line.startswith("live-check: PASS 6 ") for line in lines)
    assert any(line.startswith("live-check: PASS 7 ") for line in lines)


def test_a_listing_without_the_probe_fails_grant_five():
    class _ListsNothing(FakeS3):
        def list_objects_v2(self, **kwargs):
            return {"IsTruncated": False}

    passed, lines = _run(_ListsNothing())
    assert not passed
    assert any(line.startswith("live-check: FAIL 5 ") for line in lines)


@pytest.mark.parametrize(("versioning", "shown"), [(None, "no status"), ("Suspended", "Suspended")])
def test_a_bucket_without_versioning_enabled_fails_grant_six(versioning, shown):
    passed, lines = _run(FakeS3(versioning=versioning))
    assert not passed
    assert f"live-check: FAIL 6 GetBucketVersioning returned {shown}" in lines


class _DeniesVersioning(FakeS3):
    """Credentials whose policy lost ``s3:GetBucketVersioning``."""

    def get_bucket_versioning(self, **kwargs):
        raise client_error("AccessDenied", "GetBucketVersioning", 403)


def test_credentials_that_cannot_read_versioning_fail_grant_six():
    passed, lines = _run(_DeniesVersioning())
    assert not passed
    assert "live-check: FAIL 6 GetBucketVersioning refused with AccessDenied" in lines


class _DeniesPlainGet(FakeS3):
    """Credentials that may read an old version and not the current one."""

    def get_object(self, **kwargs):
        if "VersionId" not in kwargs:
            raise client_error("AccessDenied", "GetObject", 403)
        return super().get_object(**kwargs)


def test_credentials_that_cannot_get_fail_grant_seven():
    passed, lines = _run(_DeniesPlainGet())
    assert not passed
    assert "live-check: FAIL 7 GetObject refused with AccessDenied" in lines


class _ServesFirstVersion(FakeS3):
    """A bucket whose plain ``GetObject`` answers with the oldest version."""

    def get_object(self, **kwargs):
        if "VersionId" not in kwargs:
            kwargs["VersionId"] = self.objects[kwargs["Key"]][0].version_id
        return super().get_object(**kwargs)


def test_a_plain_get_of_old_bytes_fails_grant_seven():
    passed, lines = _run(_ServesFirstVersion())
    assert not passed
    assert (
        "live-check: FAIL 7 GetObject of the probe returned other bytes with VersionId v1, not v2"
    ) in lines


class _ServesFirstBytesAsCurrent(FakeS3):
    """A plain ``GetObject`` that names the current version and serves the first one's bytes."""

    def get_object(self, **kwargs):
        if "VersionId" in kwargs:
            return super().get_object(**kwargs)
        versions = self.objects[kwargs["Key"]]
        response = super().get_object(**kwargs, VersionId=versions[0].version_id)
        response["VersionId"] = versions[-1].version_id
        return response


def test_a_plain_get_of_old_bytes_under_the_current_id_fails_grant_seven():
    """Mutation this catches: grant seven judging the id alone. The id here is right, so only
    the bytes fail it."""
    passed, lines = _run(_ServesFirstBytesAsCurrent())
    assert not passed
    assert (
        "live-check: FAIL 7 GetObject of the probe returned other bytes with VersionId v2"
    ) in lines


class _NamesNoVersion(FakeS3):
    """A plain ``GetObject`` that serves the current bytes and names no version."""

    def get_object(self, **kwargs):
        response = super().get_object(**kwargs)
        if "VersionId" not in kwargs:
            del response["VersionId"]
        return response


class _NamesTheOldVersion(FakeS3):
    """A plain ``GetObject`` that serves the current bytes and names the first version."""

    def get_object(self, **kwargs):
        response = super().get_object(**kwargs)
        if "VersionId" not in kwargs:
            response["VersionId"] = self.objects[kwargs["Key"]][0].version_id
        return response


@pytest.mark.parametrize(
    ("fake", "shown"),
    [(_NamesNoVersion, "VersionId None, not v2"), (_NamesTheOldVersion, "VersionId v1, not v2")],
)
def test_a_plain_get_that_misnames_the_current_version_fails_grant_seven(fake, shown):
    # The bytes are right. The trim records the id the read returns, so a wrong or missing id
    # fails the grant even then.
    passed, lines = _run(fake())
    assert not passed
    assert (
        f"live-check: FAIL 7 GetObject of the probe returned the current version's bytes with "
        f"{shown}"
    ) in lines


class _NamesOneId(FakeS3):
    """Every PUT and every plain ``GetObject`` names the same unusable id."""

    def __init__(self, version_id: str) -> None:
        super().__init__()
        self.fixed = version_id

    def put_object(self, **kwargs):
        response = super().put_object(**kwargs)
        response["VersionId"] = self.fixed
        return response

    def get_object(self, **kwargs):
        response = super().get_object(**kwargs)
        if "VersionId" not in kwargs:
            response["VersionId"] = self.fixed
        return response


@pytest.mark.parametrize("version", ["NULL", "Null", "  "])
def test_an_id_that_names_no_version_fails_grant_seven_even_when_the_put_named_it(version):
    """Mutation this catches: grant seven judging the id by anything but
    ``bucket.usable_version_id``, which the trim judges by too."""
    passed, lines = _run(_NamesOneId(version))
    assert not passed
    assert any(line.startswith("live-check: FAIL 7 GetObject") for line in lines)


class _NamesNoSecondVersion(FakeS3):
    """The second PUT to a key answers an id that names no version, unlike the first."""

    def __init__(self, version_id: str) -> None:
        super().__init__()
        self.fixed = version_id
        self.puts: dict[str, int] = {}

    def put_object(self, **kwargs):
        response = super().put_object(**kwargs)
        count = self.puts[kwargs["Key"]] = self.puts.get(kwargs["Key"], 0) + 1
        if count > 1:
            response["VersionId"] = self.fixed
        return response


@pytest.mark.parametrize("version", ["NULL", "Null", "  "])
def test_a_second_put_naming_no_version_fails_behavior_three(version):
    """Mutation this catches: behavior three judging the ids by anything but
    ``bucket.usable_version_id``. The earlier check, ``old_id != "null"`` on the first id alone,
    passed a first id of ``v1`` and a second of ``NULL``."""
    passed, lines = _run(_NamesNoSecondVersion(version))
    assert not passed
    assert any(
        line.startswith("live-check: FAIL 3 two PUTs to one key returned versions v1 and")
        for line in lines
    )


@pytest.mark.parametrize(
    ("version", "usable"),
    [
        (None, False),
        ("", False),
        ("   ", False),
        ("null", False),
        ("NULL", False),
        ("nUlL", False),
        (7, False),
        ("v1", True),
        ("3HL4kqtJlcpXroDTDmJ", True),
    ],
)
def test_usable_version_id_refuses_every_id_that_names_no_version(version, usable):
    assert bucket.usable_version_id(version) is usable


@pytest.mark.parametrize(("fake", "code"), [(FakeS3, 0), (_DeniesMismatch, 1)])
def test_the_command_exits_one_when_any_behavior_fails(tmp_path, monkeypatch, capsys, fake, code):
    config = write_config(tmp_path, tmp_path / "lake")
    config.write_text(config.read_text() + KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: fake())
    argv = ["live-check", "--config", str(config), "--target", "s3://lake-backup/live-check"]
    clock = ManualClock(datetime(2026, 10, 5, 23, 0, tzinfo=UTC))
    assert bucket.main(argv, clock=clock) == code
    assert ("live-check: FAIL" in capsys.readouterr().out) == (code == 1)
