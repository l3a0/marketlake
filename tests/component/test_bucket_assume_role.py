"""The bucket's assume-role path, marketlake #737.

Under ``bucket_credentials: assume_role`` the command key in ``config.yaml`` assumes the
bucket's role, and the S3 client signs with the short-lived credentials STS returns. Two
halves are checked here.

1. **What each caller prints when the assume fails.** ``FakeS3`` raises
   ``_AssumeRoleFailed`` from every call, so the scrub, the reader, the live check and
   ``bucket.main`` are driven to their one line with no STS and no retry backoff.
2. **The wiring, against a real STS on loopback.** The ``sts`` fixture in
   ``tests/conftest.py`` answers the assume, and a ``before-send`` hook answers S3, so
   no request reaches AWS. These tests read what the assume asked for, what signed it,
   what signed S3, what a forced refresh does under a hostile environment, and what a
   refusal leaves on a traceback.
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from botocore.awsrequest import AWSResponse

from lake import bucket
from lake.aws_session import _AssumeRoleFailed
from lake.bucket import (
    BucketReadError,
    _failure,
    _one_line,
    bucket_reader,
    bucket_scrub,
    client_from_config,
    live_check,
)
from lake.config import BucketTarget, Config
from tests.component.test_bucket_scrub import _sunday_cli, _uploaded
from tests.support.bucket import FakeS3
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.sts import (
    ACCOUNT_ID,
    BUCKET_ROLE_ARN,
    COMMAND_KEY_ID,
    COMMAND_SECRET,
    PRINCIPAL_ARN,
    Answer,
    session_key_id,
    session_token,
)

REGION = "us-east-2"
TARGET = BucketTarget(bucket="lake-backup", prefix="lake")
LIVE_TARGET = "s3://lake-backup/live-check"
REFUSED = _AssumeRoleFailed("AccessDenied", transient=False)
UNAVAILABLE = _AssumeRoleFailed("ServiceUnavailable", transient=True)
ASSUME_KEYS = (
    "bucket_credentials: assume_role\n"
    f"command_access_key_id: {COMMAND_KEY_ID}\n"
    f"command_secret_access_key: {COMMAND_SECRET}\n"
    f"bucket_role_arn: {BUCKET_ROLE_ARN}\n"
    f"bucket_region: {REGION}\n"
)


def _config(**overrides) -> Config:
    values = {
        "lake_root": "/data/lake",
        "backup_target": str(TARGET),
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
        "bucket_credentials": "assume_role",
        "bucket_region": REGION,
        "command_access_key_id": COMMAND_KEY_ID,
        "command_secret_access_key": COMMAND_SECRET,
        "bucket_role_arn": BUCKET_ROLE_ARN,
        **overrides,
    }
    return Config.from_mapping(values)


def _no_identifier_in(text: str) -> None:
    assert ACCOUNT_ID not in text
    assert PRINCIPAL_ARN not in text
    assert "Traceback" not in text


# -- 1. what each caller prints ---------------------------------------------------


@pytest.mark.parametrize(("failed", "kind"), [(REFUSED, "refused"), (UNAVAILABLE, "unreachable")])
def test_failure_sorts_an_assume_failure_by_whether_it_will_pass(failed, kind):
    code = failed.code
    assert _failure(failed) == (kind, f"AssumeRole {code}")


def test_the_scrub_names_a_refused_assume_and_does_not_raise(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = REFUSED

    result = bucket_scrub(lake, TARGET, client)

    assert result.bucket_refused == "AssumeRole AccessDenied"
    assert result.versioning == (
        "bucket versioning unreadable (AssumeRole AccessDenied): s3://lake-backup/lake"
    )
    assert not result.ok


def test_the_scrub_names_an_unavailable_sts_as_unreachable(tmp_path):
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = UNAVAILABLE

    result = bucket_scrub(lake, TARGET, client)

    assert result.bucket_unreachable == "AssumeRole ServiceUnavailable"
    assert result.bucket_refused is None


def test_the_reader_raises_a_read_error_naming_the_assume(tmp_path):
    _, client = _uploaded(tmp_path / "lake")
    client.fail_with = REFUSED
    read = bucket_reader(client, TARGET)

    with pytest.raises(BucketReadError) as raised:
        list(read("manifest.jsonl"))

    assert raised.value.kind == "refused"
    assert raised.value.code == "AssumeRole AccessDenied"
    assert str(raised.value) == "the bucket refused the read (AssumeRole AccessDenied)"


def test_the_hand_run_line_names_the_command_key_and_the_role():
    line = str(_one_line(REFUSED, TARGET))
    assert line == (
        "the bucket's role could not be assumed (AssumeRole AccessDenied), so check "
        "command_access_key_id, command_secret_access_key and bucket_role_arn in "
        "config.yaml. A key made minutes ago may not be active yet: s3://lake-backup/lake"
    )


def test_the_hand_run_line_for_a_read_error_names_them_too():
    line = str(_one_line(BucketReadError("x", "refused", "AssumeRole AccessDenied"), TARGET))
    assert "bucket_role_arn" in line and "may not be active yet" in line


def test_the_hand_run_line_for_an_unavailable_sts_promises_no_fix_in_the_file():
    line = str(_one_line(UNAVAILABLE, TARGET))
    assert line == (
        "the bucket could not be reached or was unavailable (AssumeRole "
        "ServiceUnavailable): s3://lake-backup/lake"
    )


def test_a_bucket_refusal_keeps_its_own_line():
    # The control: a refusal from S3 itself, not STS, still names the bucket's credentials.
    from tests.support.bucket import client_error

    line = str(_one_line(client_error("AccessDenied", "HeadObject", 403), TARGET))
    assert "the bucket refused the request (AccessDenied)" in line
    assert "bucket_role_arn" not in line


def test_the_live_check_raises_an_assume_failure_at_check_one():
    # Check 1 catches every error, because a refused PUT is what it expects. A refusal
    # from STS is not S3's verdict on the checksum, so it must not print as one.
    client = FakeS3()
    client.fail_with = REFUSED
    lines: list[str] = []
    with pytest.raises(_AssumeRoleFailed):
        live_check(client, BucketTarget("lake-backup", "live-check"), stamp="s", out=lines.append)
    assert lines == []


def _bucket_main(tmp_path: Path, monkeypatch, client: FakeS3, argv: list[str]) -> int:
    config = write_config(tmp_path, tmp_path / "lake")
    config.write_text(config.read_text() + ASSUME_KEYS)
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)
    return bucket.main(
        [*argv, "--config", str(config)],
        clock=ManualClock(datetime(2026, 10, 6, 23, 0, tzinfo=UTC)),
    )


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["live-check", "--target", LIVE_TARGET], id="live-check"),
        pytest.param(["first-upload", "--target", str(TARGET)], id="first-upload"),
        pytest.param(["restore", "DEST", "--target", str(TARGET)], id="restore"),
        pytest.param(["resync", "--target", str(TARGET)], id="resync"),
    ],
)
def test_each_command_prints_one_line_for_a_refused_assume(tmp_path, monkeypatch, capsys, argv):
    (tmp_path / "lake").mkdir()
    # The resync refuses a lake with no manifest before any request, so the lake holds one.
    (tmp_path / "lake" / "manifest.jsonl").write_text('{"partition": "x"}\n')
    argv = [str(tmp_path / "dest") if part == "DEST" else part for part in argv]
    client = FakeS3()
    client.fail_with = REFUSED

    with pytest.raises(SystemExit) as exited:
        _bucket_main(tmp_path, monkeypatch, client, argv)

    assert exited.value.code == 2
    captured = capsys.readouterr()
    assert "FAIL" not in captured.out
    lines = captured.err.splitlines()
    assert len(lines) == 1, captured.err
    assert lines[0].startswith(f"{argv[0]}: the bucket's role could not be assumed ")
    assert "(AssumeRole AccessDenied)" in lines[0]
    _no_identifier_in(captured.err)
    assert COMMAND_SECRET not in captured.err


def test_the_sunday_job_reports_a_refused_assume_and_runs_on(tmp_path, capsys, monkeypatch):
    # Without the branch in ``_failure`` the scrub would re-raise, and the Sunday job's guard
    # would print a traceback and name a scrub that raised rather than the refused role.
    lake, client = _uploaded(tmp_path / "lake")
    client.fail_with = REFUSED
    monkeypatch.setattr(bucket, "client_from_config", lambda cfg: client)

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake, keys=ASSUME_KEYS)

    assert code == 1
    assert pinger.urls == []
    assert canaries
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "backup bucket refused the scrub (AssumeRole AccessDenied)" in captured.out


# -- 2. against a real STS on loopback ---------------------------------------------


class _S3:
    """Answers an S3 client's requests in place of AWS, and records each one."""

    def __init__(self, client) -> None:
        self.requests: list = []
        client.meta.events.register("before-send.s3", self._answer)

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        status, body = 200, b""
        if "?versioning" in request.url:
            body = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
        elif request.url.endswith("/refused"):
            status = 400
            body = b"<Error><Code>BadDigest</Code><Message>no</Message></Error>"

        class Raw:
            def stream(self, **kwargs):
                yield body

        return AWSResponse(request.url, status, {"Content-Length": str(len(body))}, Raw())


def _head(client) -> None:
    client.head_object(Bucket="lake-backup", Key="lake/manifest.jsonl")


def test_building_the_client_makes_no_sts_call(sts):
    client_from_config(_config())
    assert sts.requests == []


def test_the_first_request_assumes_the_bucket_role_with_the_command_key(sts):
    client = client_from_config(_config())
    s3 = _S3(client)

    _head(client)

    assert len(sts.requests) == 1
    sent = sts.requests[0]
    assert sent.params["Action"] == "AssumeRole"
    assert sent.params["RoleArn"] == BUCKET_ROLE_ARN
    assert sent.params["RoleSessionName"] == "marketlake-bucket"
    assert sent.key_id == COMMAND_KEY_ID
    assert sent.scope_region == REGION
    request = s3.requests[0]
    authorization = request.headers["Authorization"].decode()
    assert f"Credential={session_key_id(1)}/" in authorization
    assert COMMAND_KEY_ID not in authorization
    assert request.headers["X-Amz-Security-Token"].decode() == session_token(1)
    assert request.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")


def test_an_hour_long_session_is_assumed_once_for_many_requests(sts):
    client = client_from_config(_config())
    s3 = _S3(client)
    for _ in range(3):
        _head(client)
    assert len(sts.requests) == 1
    assert len(s3.requests) == 3


@pytest.fixture
def hostile(tmp_path):
    """Every setting botocore could read at a refresh, pointed somewhere else."""
    home = tmp_path / "home"
    (home / ".aws").mkdir(parents=True)
    (home / ".aws" / "config").write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-home.invalid\n"
    )
    (home / ".aws" / "credentials").write_text(
        "[default]\naws_access_key_id = AKIDFROMHOME\naws_secret_access_key = from-home\n"
    )
    config_file = tmp_path / "aws-config"
    config_file.write_text("[profile elsewhere]\nregion = eu-west-1\n")
    credentials_file = tmp_path / "aws-credentials"
    credentials_file.write_text(
        "[elsewhere]\naws_access_key_id = AKIDFROMFILE\naws_secret_access_key = from-file\n"
    )
    return {
        "HOME": str(home),
        "AWS_ACCESS_KEY_ID": "AKIDFROMENV",
        "AWS_SECRET_ACCESS_KEY": "from-env",
        "AWS_SESSION_TOKEN": "token-from-env",
        "AWS_REGION": "eu-west-1",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_PROFILE": "no-such-profile",
        "AWS_ENDPOINT_URL": "https://from-env.invalid",
        "AWS_ENDPOINT_URL_STS": "https://from-env-sts.invalid",
        "AWS_STS_REGIONAL_ENDPOINTS": "legacy",
        "AWS_CONFIG_FILE": str(config_file),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file),
    }


def test_a_forced_refresh_under_a_hostile_environment_signs_sts_with_the_command_key(
    sts, hostile, monkeypatch
):
    # Five minutes is inside botocore's ten-minute mandatory window, so every request
    # after the first assumes the role again. The environment turns hostile only after
    # the build, which is when a refresh that resolved settings of its own would read it.
    sts.expires_in = timedelta(minutes=5)
    client = client_from_config(_config())
    s3 = _S3(client)
    _head(client)
    for key, value in hostile.items():
        monkeypatch.setenv(key, value)

    _head(client)

    assert len(sts.requests) == 2
    for sent in sts.requests:
        assert sent.key_id == COMMAND_KEY_ID
        assert sent.scope_region == REGION
        assert sent.params["RoleArn"] == BUCKET_ROLE_ARN
    second = s3.requests[1]
    assert f"Credential={session_key_id(2)}/" in second.headers["Authorization"].decode()
    assert second.headers["X-Amz-Security-Token"].decode() == session_token(2)
    assert second.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")


def test_an_expiry_with_a_non_utc_offset_still_signs(sts):
    # botocore's own refresher formats the expiry with ``%Z``, which drops this offset.
    offset = timezone(timedelta(hours=-4))
    sts.expiration = (datetime.now(UTC) + timedelta(hours=1)).astimezone(offset).isoformat()
    assert sts.expiration.endswith("-04:00")
    client = client_from_config(_config())
    s3 = _S3(client)

    _head(client)
    _head(client)

    assert len(sts.requests) == 1
    for request in s3.requests:
        assert f"Credential={session_key_id(1)}/" in request.headers["Authorization"].decode()


@pytest.mark.parametrize(
    ("expiration", "code"),
    [
        pytest.param("2099-01-01T00:00:00", "NaiveExpiration", id="naive"),
        pytest.param("2020-01-01T00:00:00Z", "ExpiredCredentials", id="past"),
    ],
)
def test_an_unusable_expiry_is_refused_as_an_assume_failure(sts, expiration, code):
    sts.expiration = expiration
    client = client_from_config(_config())
    _S3(client)
    with pytest.raises(_AssumeRoleFailed) as raised:
        _head(client)
    assert raised.value.code == code
    assert raised.value.transient is False


def test_a_refusal_leaves_no_account_or_principal_on_any_traceback(sts):
    sts.refuse("AccessDenied")
    client = client_from_config(_config())
    s3 = _S3(client)

    with pytest.raises(_AssumeRoleFailed) as raised:
        _head(client)

    escaped = raised.value
    assert (escaped.code, escaped.transient) == ("AccessDenied", False)
    assert escaped.__context__ is None and escaped.__cause__ is None
    printed = "".join(traceback.format_exception(escaped))
    assert "AssumeRole AccessDenied" in printed
    _no_identifier_in(printed.replace("Traceback (most recent call last)", ""))
    assert s3.requests == []


def test_an_unavailable_sts_is_tried_four_times_and_reads_as_unreachable(sts):
    sts.refuse("ServiceUnavailable", status=503)
    client = client_from_config(_config())
    _S3(client)

    with pytest.raises(_AssumeRoleFailed) as raised:
        _head(client)

    # Three retries, botocore's standard ones, are four tries. A literal, so a changed
    # constant cannot move the expectation with it.
    assert len(sts.requests) == 4
    assert _failure(raised.value) == ("unreachable", "AssumeRole ServiceUnavailable")


def _refuse_the_refresh(sts) -> None:
    """Answer the first assume with a five-minute session and refuse every one after it."""
    sts.expires_in = timedelta(minutes=5)
    sts.script.append(Answer())
    sts.refuse("AccessDenied")


def test_a_refused_refresh_reaches_the_scrub_as_a_finding(sts, tmp_path):
    lake, _ = _uploaded(tmp_path / "lake")
    _refuse_the_refresh(sts)
    client = client_from_config(_config())
    _S3(client)

    result = bucket_scrub(lake, TARGET, client)

    # The versioning read assumed the role, and the next request's refresh was refused.
    assert result.versioning is None
    assert result.bucket_refused == "AssumeRole AccessDenied"
    assert len(sts.requests) == 2


def _live_check_main(tmp_path, monkeypatch) -> int:
    config = write_config(tmp_path, tmp_path / "lake")
    config.write_text(config.read_text() + ASSUME_KEYS)
    real = bucket._build_client

    def build(cfg):
        client = real(cfg)
        _S3(client)
        return client

    monkeypatch.setattr(bucket, "_build_client", build)
    argv = ["live-check", "--config", str(config), "--target", LIVE_TARGET]
    return bucket.main(argv, clock=ManualClock(datetime(2026, 10, 6, 23, 0, tzinfo=UTC)))


def test_the_live_check_prints_one_line_when_sts_refuses_the_first_request(
    sts, tmp_path, monkeypatch, capsys
):
    sts.refuse("AccessDenied")

    with pytest.raises(SystemExit) as exited:
        _live_check_main(tmp_path, monkeypatch)

    assert exited.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.splitlines()
    assert lines == [
        "live-check: the bucket's role could not be assumed (AssumeRole AccessDenied), so "
        "check command_access_key_id, command_secret_access_key and bucket_role_arn in "
        f"config.yaml. A key made minutes ago may not be active yet: {LIVE_TARGET}"
    ]
    _no_identifier_in(captured.err)


def test_the_live_check_prints_one_line_when_a_refresh_is_refused(
    sts, tmp_path, monkeypatch, capsys
):
    _refuse_the_refresh(sts)

    with pytest.raises(SystemExit) as exited:
        _live_check_main(tmp_path, monkeypatch)

    assert exited.value.code == 2
    captured = capsys.readouterr()
    # Check 1 ran on the first session, and its PUT was refused by S3 as it should be.
    assert captured.out.splitlines() == [
        "live-check: PASS 1 mismatched ChecksumSHA256 refused with BadDigest"
    ]
    lines = captured.err.splitlines()
    assert len(lines) == 1
    assert "(AssumeRole AccessDenied)" in lines[0]
    _no_identifier_in(captured.err)


def test_the_role_arn_never_reaches_the_config_repr():
    shown = repr(_config())
    assert BUCKET_ROLE_ARN not in shown and ACCOUNT_ID not in shown
    assert COMMAND_SECRET not in shown and COMMAND_KEY_ID not in shown


def test_the_sts_client_points_where_the_seam_says(sts):
    client = client_from_config(_config())
    refresh = client._request_signer._credentials._refresh_using
    assert refresh.sts.meta.endpoint_url == sts.url
