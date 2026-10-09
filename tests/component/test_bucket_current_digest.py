"""The read the trim checks a partition's bucket copy with, against the fake S3 client.

``lake.bucket.current_digest`` hashes the current version of one key and keeps the
``VersionId`` the response carried (marketlake #787). It streams the body, closes it, writes
nothing, and sorts a bucket failure into ``BucketReadError`` the way ``bucket_reader`` does.
"""

from __future__ import annotations

import hashlib
import io
from datetime import date, datetime

import pytest
from urllib3.exceptions import SSLError

from lake import bucket
from lake.bucket import BucketBackup, BucketReadError, CurrentDigest, current_digest
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock

TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
REL = "chains/ticker=SPY/date=2026-08-24.parquet"


class _Body(io.BytesIO):
    """A body that counts its reads and records that it was closed."""

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.reads = 0
        self.was_closed = False

    def read(self, size: int | None = -1) -> bytes:
        self.reads += 1
        return super().read(size)

    def close(self) -> None:
        self.was_closed = True
        super().close()


class _CountingS3(FakeS3):
    """A fake whose ``GetObject`` bodies count their reads."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.bodies: list[_Body] = []

    def get_object(self, **kwargs):
        response = super().get_object(**kwargs)
        body = _Body(response["Body"].read())
        self.bodies.append(body)
        return {**response, "Body": body}


def test_the_current_version_hashes_and_names_its_version(monkeypatch):
    data = b"partition bytes " * 100
    client = _CountingS3()
    client.store(TARGET.key(REL), b"an older version")
    client.store(TARGET.key(REL), data)
    newest = client.versions(TARGET.key(REL))[-1].version_id
    monkeypatch.setattr(bucket, "_READ_CHUNK", 64)

    found = current_digest(client, TARGET, REL)

    assert found == CurrentDigest(sha256=hashlib.sha256(data).hexdigest(), version_id=newest)
    # The body was read in pieces and closed, and nothing was written to the bucket.
    assert client.bodies[0].reads > len(data) // 64
    assert client.bodies[0].was_closed
    assert client.puts() == []
    assert [name for name, _ in client.calls] == ["get_object"]
    assert "VersionId" not in client.calls[0][1]


def test_a_response_without_a_version_reads_as_none():
    class _NoVersion(FakeS3):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            del response["VersionId"]
            return response

    client = _NoVersion()
    client.store(TARGET.key(REL), b"bytes")
    assert current_digest(client, TARGET, REL).version_id is None


def test_a_missing_key_raises_an_absent_read_error():
    with pytest.raises(BucketReadError) as raised:
        current_digest(FakeS3(), TARGET, REL)
    assert raised.value.absent
    assert raised.value.rel == REL
    assert raised.value.code == "NoSuchKey"


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (client_error("AccessDenied", "GetObject", 403), "refused"),
        (unreachable(), "unreachable"),
        (client_error("SlowDown", "GetObject", 503), "unreachable"),
    ],
)
def test_a_bucket_failure_raises_a_sorted_read_error(error, kind):
    client = FakeS3()
    client.store(TARGET.key(REL), b"bytes")
    client.fail_with = error
    with pytest.raises(BucketReadError) as raised:
        current_digest(client, TARGET, REL)
    assert raised.value.kind == kind
    assert not raised.value.absent


@pytest.mark.parametrize(
    "error",
    [unreachable, lambda: SSLError("the connection broke partway through the body")],
)
def test_a_failure_partway_through_the_body_raises_a_read_error_and_closes_it(error):
    # An SSLError from urllib3 is neither a BotoCoreError nor an OSError, and is what a body
    # read can raise past botocore's own wrapping.
    closed: list[bool] = []

    class _Breaks(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            raise error()

        def close(self) -> None:
            closed.append(True)

    class _BreaksMidBody(FakeS3):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            return {**response, "Body": _Breaks()}

    client = _BreaksMidBody()
    client.store(TARGET.key(REL), b"bytes")
    with pytest.raises(BucketReadError) as raised:
        current_digest(client, TARGET, REL)
    assert raised.value.kind == "unreachable"
    assert closed == [True]


def test_an_error_that_is_not_a_bucket_failure_raises_as_itself():
    client = FakeS3()
    client.store(TARGET.key(REL), b"bytes")
    client.fail_with = KeyError("a bug")
    with pytest.raises(KeyError):
        current_digest(client, TARGET, REL)


def test_the_backup_exposes_its_client_and_forgets_a_summary_when_a_sync_raises(tmp_path):
    client = FakeS3()
    clock = ManualClock(datetime(2026, 8, 24, 16, 40, tzinfo=MARKET_TZ))
    backup = BucketBackup(client=client, clock=clock, calendar=weekday_sessions(date(2026, 8, 24)))
    assert backup.client is client
    backup.last = bucket.UploadSummary(target="an earlier night")
    lake = tmp_path / "lake"
    lake.mkdir()
    with pytest.raises(bucket.WatermarkMissing):
        backup.sync(lake, TARGET)
    assert backup.last is None
