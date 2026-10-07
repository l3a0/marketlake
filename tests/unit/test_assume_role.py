"""The assume-role source's settings, its STS client and its refresh, with no socket.

marketlake #737 gives the laptop one AWS key, the command key, which assumes the bucket's
role and the token store's. The checks on the settings, the STS client's construction and
the refresh's handling of what STS answers are read here without a request leaving the
process: the refresh is called with a stand-in STS client, and a build is inspected
rather than used. ``tests/component/test_bucket_assume_role.py`` and
``tests/component/test_token_store_put.py`` drive the same code against an STS on
loopback.
"""

from __future__ import annotations

import traceback
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ParamValidationError,
    ReadTimeoutError,
)

from lake import aws_session, token_store
from lake.aws_session import (
    AssumeRole,
    KeyPair,
    _AssumeRoleFailed,
    _AssumeRoleRefresh,
    _session_credentials,
    build_client,
    source_from_bucket_credentials,
)
from lake.bucket import client_from_config, live_check
from lake.config import BucketTarget, Config, ConfigError, Secret, require_bucket_settings
from tests.support.bucket import FakeS3
from tests.support.clock import ManualClock

ACCOUNT_ID = "111122223333"
PRINCIPAL_ARN = f"arn:aws:iam::{ACCOUNT_ID}:user/marketlake-command"
BUCKET_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/marketlake-backup"
TOKEN_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/marketlake-token-writer"
COMMAND_KEY_ID = "AKIDCOMMANDKEY"
COMMAND_SECRET = "command-secret-value"
REGION = "us-east-2"
NOW = datetime(2026, 10, 6, 22, 0, tzinfo=UTC)

BASE = {
    "lake_root": "/data/lake",
    "backup_target": "s3://lake-backup/lake",
    "healthchecks_ping_key": "ping",
    "ntfy_topic": "topic",
    "schwab_api_key": "api",
    "schwab_app_secret": "app",
}
ASSUME = {
    "bucket_credentials": "assume_role",
    "command_access_key_id": COMMAND_KEY_ID,
    "command_secret_access_key": COMMAND_SECRET,
    "bucket_role_arn": BUCKET_ROLE,
    "bucket_region": REGION,
}


def _config(**overrides) -> Config:
    values = {**BASE, **ASSUME, **overrides}
    return Config.from_mapping({key: value for key, value in values.items() if value is not None})


# -- 1. the settings ---------------------------------------------------------------


def test_complete_assume_role_settings_pass():
    assert require_bucket_settings(_config()).bucket == "lake-backup"


@pytest.mark.parametrize(
    "missing",
    ["command_access_key_id", "command_secret_access_key", "bucket_role_arn", "bucket_region"],
)
def test_each_missing_assume_role_key_is_named(missing):
    with pytest.raises(ConfigError) as refused:
        require_bucket_settings(_config(**{missing: None}))
    assert str(refused.value) == f"the bucket needs config key(s): ['{missing}']"


def test_the_bucket_key_values_may_stay_beside_assume_role_for_a_rollback():
    config = _config(bucket_access_key_id="AKIDOLDKEY", bucket_secret_access_key="old")
    require_bucket_settings(config)
    source = source_from_bucket_credentials(config)
    assert isinstance(source, AssumeRole)
    assert source.keys.access_key_id.reveal() == COMMAND_KEY_ID


def test_the_bucket_key_values_alone_do_not_satisfy_assume_role():
    config = _config(
        command_access_key_id=None,
        command_secret_access_key=None,
        bucket_access_key_id="AKIDOLDKEY",
        bucket_secret_access_key="old",
    )
    with pytest.raises(ConfigError) as refused:
        require_bucket_settings(config)
    assert "['command_access_key_id', 'command_secret_access_key']" in str(refused.value)


MALFORMED_ARNS = [
    f"arn:aws:iam::{ACCOUNT_ID}:user/marketlake-command",
    f"arn:aws:iam:{ACCOUNT_ID}:role/marketlake-backup",
    "arn:aws:iam::12345:role/marketlake-backup",
    f"{ACCOUNT_ID}:role/marketlake-backup",
    "marketlake-backup",
    f"arn:aws:iam::{ACCOUNT_ID}:role/",
    f"arn:aws:iam::{ACCOUNT_ID}:role/ops/",
    f"arn:aws:iam::{ACCOUNT_ID}:role/{'n' * 65}",
    f"arn:aws:iam::{ACCOUNT_ID}:role//",
    f"arn:aws:iam::{ACCOUNT_ID}:role/r\u00e9le",
    "arn:aws:iam::\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669\u0660\u0661\u0662:role/x",
    f"arn:awsjunk:iam::{ACCOUNT_ID}:role/marketlake-backup",
]


@pytest.mark.parametrize("arn", MALFORMED_ARNS)
def test_a_malformed_role_arn_is_refused_without_any_part_of_it(arn):
    with pytest.raises(ConfigError) as refused:
        require_bucket_settings(_config(bucket_role_arn=arn))
    message = str(refused.value)
    assert message == (
        "bucket_role_arn is not an IAM role ARN like arn:aws:iam::<account>:role/<name>"
    )
    assert ACCOUNT_ID not in message and "marketlake-backup" not in message


@pytest.mark.parametrize("arn", MALFORMED_ARNS)
def test_a_malformed_token_role_arn_is_refused_before_the_login(arn):
    config = _config(token_store_role_arn=arn, token_store_region="us-east-1")
    problems = token_store.credential_problems(config)
    assert problems == [
        "token_store_role_arn is not an IAM role ARN like arn:aws:iam::<account>:role/<name>"
    ]
    assert ACCOUNT_ID not in problems[0]


@pytest.mark.parametrize(
    "arn",
    [
        BUCKET_ROLE,
        f"arn:aws:iam::{ACCOUNT_ID}:role/ops/backup.v2",
        f"arn:aws:iam::{ACCOUNT_ID}:role/team#1/a:b/backup",
        f"arn:aws:iam::{ACCOUNT_ID}:role/{'p' * 500}/{'n' * 64}",
        f"arn:aws-us-gov:iam::{ACCOUNT_ID}:role/service-role/backup",
    ],
)
def test_a_role_arn_under_a_path_is_accepted(arn):
    require_bucket_settings(_config(bucket_role_arn=arn))


def test_instance_profile_does_not_refuse_the_command_key():
    # The token pull shares these checks, so a refusal here would stop the VM's pull.
    config = _config(bucket_credentials="instance_profile")
    require_bucket_settings(config)


def test_the_bucket_source_assumes_the_bucket_role_as_marketlake_bucket():
    source = source_from_bucket_credentials(_config())
    assert source == AssumeRole(
        BUCKET_ROLE,
        KeyPair(Secret(COMMAND_KEY_ID), Secret(COMMAND_SECRET)),
        "marketlake-bucket",
    )


def test_the_bucket_source_names_every_missing_key_when_called_directly():
    # The bucket checks run first everywhere, so only a direct call reaches this.
    with pytest.raises(ConfigError) as refused:
        source_from_bucket_credentials(_config(bucket_role_arn=None, command_access_key_id=None))
    assert str(refused.value) == (
        "the bucket needs config key(s): ['command_access_key_id', 'bucket_role_arn']"
    )


# -- 2. the build --------------------------------------------------------------------


def _refresh_of(client) -> _AssumeRoleRefresh:
    return client._request_signer._credentials._refresh_using


def test_the_build_makes_no_sts_call(monkeypatch):
    calls = []
    monkeypatch.setattr(_AssumeRoleRefresh, "__call__", lambda self: calls.append(self))
    client_from_config(_config())
    token_store.push_client(_config(token_store_role_arn=TOKEN_ROLE, token_store_region=REGION))
    assert calls == []


def test_the_sts_client_carries_the_stated_timeouts_retries_and_key():
    sts = _refresh_of(client_from_config(_config())).sts
    built = sts.meta.config
    assert built.connect_timeout == 5
    assert built.read_timeout == 10
    # botocore counts the first attempt too, so three retries read back as four attempts.
    assert built.retries == {"mode": "standard", "total_max_attempts": 4}
    assert sts._request_signer._credentials.access_key == COMMAND_KEY_ID
    assert sts.meta.region_name == REGION


def test_in_production_the_sts_endpoint_is_the_client_regions_own():
    assert aws_session.STS_ENDPOINT_URL is None
    sts = _refresh_of(client_from_config(_config())).sts
    assert sts.meta.endpoint_url == f"https://sts.{REGION}.amazonaws.com"


def test_the_service_client_is_handed_no_key_of_its_own():
    client = client_from_config(_config())
    credentials = client._request_signer._credentials
    assert credentials.method == "assume-role"
    assert _refresh_of(client).role_arn == BUCKET_ROLE
    assert _refresh_of(client).session_name == "marketlake-bucket"


def test_the_put_client_assumes_the_token_role_as_marketlake_token_put():
    config = _config(token_store_role_arn=TOKEN_ROLE, token_store_region="us-east-1")
    refresh = _refresh_of(token_store.push_client(config))
    assert refresh.role_arn == TOKEN_ROLE
    assert refresh.session_name == "marketlake-token-put"
    assert refresh.sts.meta.region_name == "us-east-1"


# -- 3. the refresh ------------------------------------------------------------------


class _Sts:
    """A stand-in STS client whose ``assume_role`` answers or raises as told."""

    def __init__(self, answer) -> None:
        self.answer = answer
        self.calls: list[dict] = []

    def assume_role(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer


def _refresh(answer, now: datetime = NOW) -> _AssumeRoleRefresh:
    source = AssumeRole(
        BUCKET_ROLE,
        KeyPair(Secret(COMMAND_KEY_ID), Secret(COMMAND_SECRET)),
        "marketlake-bucket",
    )
    return _AssumeRoleRefresh(_Sts(answer), source, ManualClock(now))


def _sts_error(code: str, status: int) -> ClientError:
    message = f"User: {PRINCIPAL_ARN} is not authorized to perform: sts:AssumeRole"
    return ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "AssumeRole",
    )


@pytest.mark.parametrize(
    ("raised", "code", "transient"),
    [
        pytest.param(_sts_error("AccessDenied", 403), "AccessDenied", False, id="denied"),
        pytest.param(
            _sts_error("InvalidClientTokenId", 403), "InvalidClientTokenId", False, id="key"
        ),
        pytest.param(
            _sts_error("SignatureDoesNotMatch", 403), "SignatureDoesNotMatch", False, id="sig"
        ),
        pytest.param(_sts_error("ExpiredToken", 400), "ExpiredToken", False, id="expired"),
        pytest.param(_sts_error("Throttling", 400), "Throttling", True, id="throttling-code"),
        pytest.param(_sts_error("Unknown", 429), "Unknown", True, id="429"),
        pytest.param(_sts_error("Unknown", 500), "Unknown", True, id="500"),
        pytest.param(_sts_error("Unknown", 503), "Unknown", True, id="503"),
        pytest.param(_sts_error("Unknown", 499), "Unknown", False, id="499"),
        pytest.param(
            EndpointConnectionError(endpoint_url="https://sts.us-east-2.amazonaws.com"),
            "EndpointConnectionError",
            True,
            id="connection",
        ),
        pytest.param(
            ReadTimeoutError(endpoint_url="https://sts.us-east-2.amazonaws.com"),
            "ReadTimeoutError",
            True,
            id="read-timeout",
        ),
        pytest.param(
            ParamValidationError(report=f"Invalid length for parameter RoleArn, {BUCKET_ROLE}"),
            "ParamValidationError",
            False,
            id="param-validation",
        ),
    ],
)
def test_each_sts_failure_is_sorted_where_it_is_caught(raised, code, transient):
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(raised)()
    assert (failed.value.code, failed.value.transient) == (code, transient)
    assert str(failed.value) == f"AssumeRole {code}"


def test_a_refused_assume_carries_no_account_and_no_principal_on_any_link():
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(_sts_error("AccessDenied", 403))()
    escaped = failed.value
    assert escaped.__context__ is None and escaped.__cause__ is None
    printed = "".join(traceback.format_exception(escaped))
    assert ACCOUNT_ID not in printed and PRINCIPAL_ARN not in printed
    assert "AssumeRole AccessDenied" in printed


def test_the_control_a_client_error_does_carry_them():
    # Unwrapped, STS's error prints both identifiers, which is what the wrap keeps out.
    assert PRINCIPAL_ARN in str(_sts_error("AccessDenied", 403))


def test_a_param_validation_error_quoting_the_arn_leaves_no_part_of_it():
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(ParamValidationError(report=f"bad RoleArn {BUCKET_ROLE}"))()
    printed = "".join(traceback.format_exception(failed.value))
    assert ACCOUNT_ID not in printed and "marketlake-backup" not in printed


def test_the_refresh_asks_for_the_role_and_the_session_name():
    expiry = NOW + timedelta(hours=1)
    refresh = _refresh(_answer(expiry))
    refresh()
    assert refresh.sts.calls == [{"RoleArn": BUCKET_ROLE, "RoleSessionName": "marketlake-bucket"}]


def _answer(expiry: object, **overrides) -> dict:
    credentials = {
        "AccessKeyId": "ASIASESSION",
        "SecretAccessKey": "session-secret",
        "SessionToken": "session-token",
        "Expiration": expiry,
        **overrides,
    }
    return {"Credentials": credentials}


def test_the_expiry_keeps_its_offset():
    offset = timezone(timedelta(hours=-4))
    expiry = (NOW + timedelta(hours=1)).astimezone(offset)
    returned = _refresh(_answer(expiry))()
    assert returned == {
        "access_key": "ASIASESSION",
        "secret_key": "session-secret",
        "token": "session-token",
        "expiry_time": "2026-10-06T19:00:00-04:00",
    }


@pytest.mark.parametrize(
    ("expiry", "code"),
    [
        pytest.param(datetime(2026, 10, 6, 23, 0), "NaiveExpiration", id="naive"),
        pytest.param("2026-10-06T23:00:00Z", "NaiveExpiration", id="text"),
        pytest.param(NOW, "ExpiredCredentials", id="now"),
        pytest.param(NOW - timedelta(seconds=1), "ExpiredCredentials", id="past"),
    ],
)
def test_an_unusable_expiry_is_refused(expiry, code):
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(_answer(expiry))()
    assert (failed.value.code, failed.value.transient) == (code, False)


def test_a_second_after_now_is_accepted():
    returned = _refresh(_answer(NOW + timedelta(seconds=1)))()
    assert returned["expiry_time"] == "2026-10-06T22:00:01+00:00"


@pytest.mark.parametrize("missing", ["AccessKeyId", "SecretAccessKey", "SessionToken"])
def test_an_answer_missing_a_value_is_refused(missing):
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(_answer(NOW + timedelta(hours=1), **{missing: ""}))()
    assert failed.value.code == "IncompleteCredentials"


def test_an_answer_with_no_credentials_is_refused():
    assert pytest.raises(_AssumeRoleFailed, _session_credentials, {}, NOW).value.code == (
        "IncompleteCredentials"
    )


def test_a_build_on_a_key_pair_still_takes_no_sts():
    # The control for the assume path: a ``KeyPair`` client signs with its own key.
    client = build_client(
        "s3",
        region=REGION,
        source=KeyPair(Secret(COMMAND_KEY_ID), Secret(COMMAND_SECRET)),
        client_config={},
    )
    assert client._request_signer._credentials.access_key == COMMAND_KEY_ID


# What the PR #744 mutation lens found the suite did not hold. Each test below failed
# under a mutant that every other test let through.


@pytest.mark.parametrize("source", ["keys", "instance_profile"])
def test_a_malformed_role_arn_does_not_refuse_another_credential_path(source):
    # A stale bucket_role_arn left in the file must not refuse the keys rollback or the
    # VM's instance profile, whose token pull shares this check.
    extra = (
        {"bucket_access_key_id": "AKIDOLD", "bucket_secret_access_key": "old"}
        if source == "keys"
        else {"command_access_key_id": None, "command_secret_access_key": None}
    )
    require_bucket_settings(_config(bucket_credentials=source, bucket_role_arn="garbage", **extra))


@pytest.mark.parametrize(
    "arn", [f"{BUCKET_ROLE} trailing words", f"{BUCKET_ROLE}\nsecond line", f"x{BUCKET_ROLE}"]
)
def test_a_role_arn_with_text_around_it_is_refused(arn):
    with pytest.raises(ConfigError):
        require_bucket_settings(_config(bucket_role_arn=arn))
    problems = token_store.credential_problems(
        _config(token_store_role_arn=arn, token_store_region="us-east-1")
    )
    assert problems and "token_store_role_arn" in problems[0]


def test_a_padded_role_arn_loads_stripped():
    config = _config(bucket_role_arn=f"  {BUCKET_ROLE}  ", token_store_role_arn=f" {BUCKET_ROLE} ")
    assert config.bucket_role_arn == BUCKET_ROLE
    assert config.token_store_role_arn == BUCKET_ROLE


def test_the_source_refuses_a_missing_role_arn_on_its_own():
    with pytest.raises(ConfigError) as refused:
        source_from_bucket_credentials(_config(bucket_role_arn=None))
    assert "bucket_role_arn" in str(refused.value)


def test_an_answer_missing_a_value_is_a_refusal():
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(_answer(NOW + timedelta(hours=1), SessionToken=""))()
    assert failed.value.transient is False


def test_a_428_is_a_refusal():
    with pytest.raises(_AssumeRoleFailed) as failed:
        _refresh(_sts_error("Unknown", 428))()
    assert failed.value.transient is False


def test_the_sts_client_signs_with_the_command_secret():
    # The loopback STS never verifies a signature, so a swapped secret would pass every
    # request there and fail every one in production.
    sts = client_from_config(_config())._request_signer._credentials._refresh_using.sts
    assert sts._request_signer._credentials.secret_key == COMMAND_SECRET


def test_the_sts_refusal_line_names_the_policy_and_the_trust():
    failure = token_store.PushFailed("AccessDenied", token_store.ASSUMING)
    line = token_store.push_failure_line(Path("/t/token.json"), failure)
    assert "STS refused" in line
    assert "marketlake-command's policy or the role's trust does not allow it" in line


def test_the_unknown_mode_line_names_the_token_role():
    _, line = token_store.mode_of(_config(token_store="sideways"))
    assert line is not None and "token_store_role_arn" in line


LIVE = BucketTarget(bucket="lake-backup", prefix="live-check")


def _live(client):
    lines: list[str] = []
    return live_check(client, LIVE, stamp="20261005T230000Z", out=lines.append), lines


def test_the_live_check_lists_under_its_own_prefix():
    # A listing of the whole bucket would lose the probe past the first page.
    client = FakeS3()
    _live(client)
    listed = [kwargs for name, kwargs in client.calls if name == "list_objects_v2"]
    assert listed == [
        {"Bucket": "lake-backup", "Prefix": "live-check/live-check-20261005T230000Z/"}
    ]


def test_a_listing_without_the_probe_fails_grant_five():
    class _ListsOthers(FakeS3):
        def list_objects_v2(self, **kwargs):
            return {"Contents": [{"Key": "live-check/other"}], "IsTruncated": False}

    passed, lines = _live(_ListsOthers())
    assert not passed
    assert any(line.startswith("live-check: FAIL 5 ") for line in lines)


def test_an_error_that_is_not_aws_reaches_the_caller_from_a_grant():
    class _Broken(FakeS3):
        def get_bucket_versioning(self, **kwargs):
            raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        _live(_Broken())


def test_an_unavailable_sts_at_check_one_is_raised_before_any_line():
    client = FakeS3()
    client.fail_with = _AssumeRoleFailed("ServiceUnavailable", transient=True)
    lines: list[str] = []
    with pytest.raises(_AssumeRoleFailed):
        live_check(client, LIVE, stamp="s", out=lines.append)
    assert lines == []
