"""The token parameter's put client against an STS on loopback, marketlake #737.

``token_store.push_client`` builds an SSM client that signs as the token-writer role,
which the command key assumes at the put's first request. The ``sts`` fixture answers
the assume, applied to this file alone, and a ``before-send`` hook answers SSM, so no
request reaches AWS. ``tests/unit/test_token_store.py`` covers the put itself with a
key-pair client and no socket. These tests cover what only a real assume shows: what it
asked for, what signed it, what signed the put, and that a refresh under a hostile
environment still reaches the same STS with the same key.
"""

from __future__ import annotations

import json
import traceback
from datetime import timedelta

import pytest
from botocore.awsrequest import AWSResponse

from lake import token_store
from lake.config import Config
from tests.support.sts import (
    ACCOUNT_ID,
    COMMAND_KEY_ID,
    COMMAND_SECRET,
    PRINCIPAL_ARN,
    TOKEN_ROLE_ARN,
    session_key_id,
    session_token,
)

pytestmark = pytest.mark.usefixtures("sts")

REGION = "us-east-1"
TOKEN = {"creation_timestamp": 1787529900, "token": {"access_token": "a", "refresh_token": "r"}}


def _config(**overrides) -> Config:
    values = {
        "lake_root": "/data/lake",
        "backup_target": "/Volumes/ssd/lake",
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
        "command_access_key_id": COMMAND_KEY_ID,
        "command_secret_access_key": COMMAND_SECRET,
        "token_store_role_arn": TOKEN_ROLE_ARN,
        "token_store_region": REGION,
        **overrides,
    }
    return Config.from_mapping(values)


class _Ssm:
    """Answers the SSM client's requests in place of AWS, and records each one."""

    def __init__(self, client) -> None:
        self.requests: list = []
        client.meta.events.register("before-send.ssm", self._answer)

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        data = json.dumps({"Version": 2, "Tier": "Standard"}).encode()

        class Raw:
            def stream(self, **kwargs):
                yield data

        return AWSResponse(request.url, 200, {"Content-Length": str(len(data))}, Raw())


def _push(client) -> int:
    return token_store.push(client=client, text=json.dumps(TOKEN))


def test_the_put_assumes_the_token_role_and_signs_with_the_session(sts):
    client = token_store.push_client(_config())
    ssm = _Ssm(client)
    assert sts.requests == []

    assert _push(client) == 2

    assert len(sts.requests) == 1
    assumed = sts.requests[0]
    assert assumed.params["Action"] == "AssumeRole"
    assert assumed.params["RoleArn"] == TOKEN_ROLE_ARN
    assert assumed.params["RoleSessionName"] == "marketlake-token-put"
    assert assumed.key_id == COMMAND_KEY_ID
    # The STS client takes the put's region, which decides where CloudTrail logs it.
    assert assumed.scope_region == REGION
    request = ssm.requests[0]
    authorization = request.headers["Authorization"].decode()
    assert f"Credential={session_key_id(1)}/{_date(authorization)}/{REGION}/ssm/" in authorization
    assert request.headers["X-Amz-Security-Token"].decode() == session_token(1)
    assert request.url == f"https://ssm.{REGION}.amazonaws.com/"


def _date(authorization: str) -> str:
    return authorization.split("Credential=", 1)[1].split("/")[1]


@pytest.fixture
def hostile(tmp_path, monkeypatch) -> dict[str, str]:
    """Every place botocore looks for settings, pointed somewhere else, with no proxy."""
    home = tmp_path / "home"
    (home / ".aws").mkdir(parents=True)
    (home / ".aws" / "config").write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-home.invalid\n"
    )
    (home / ".aws" / "credentials").write_text(
        "[default]\naws_access_key_id = AKIDFROMHOME\naws_secret_access_key = from-home\n"
    )
    env = {
        "HOME": str(home),
        "AWS_ACCESS_KEY_ID": "AKIDFROMENV",
        "AWS_SECRET_ACCESS_KEY": "from-env",
        "AWS_SESSION_TOKEN": "token-from-env",
        "AWS_REGION": "eu-west-1",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_PROFILE": "no-such-profile",
        "AWS_ENDPOINT_URL": "https://from-env.invalid",
        "AWS_ENDPOINT_URL_STS": "https://from-env-sts.invalid",
        "AWS_ENDPOINT_URL_SSM": "https://from-env-ssm.invalid",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env


def test_a_put_and_its_refresh_ignore_a_hostile_environment(sts, hostile):
    # The environment is hostile through the build and both requests. A five-minute
    # session sits inside botocore's mandatory window, so the second put assumes again.
    sts.expires_in = timedelta(minutes=5)
    client = token_store.push_client(_config())
    ssm = _Ssm(client)

    _push(client)
    _push(client)

    assert len(sts.requests) == 2
    assert [sent.key_id for sent in sts.requests] == [COMMAND_KEY_ID, COMMAND_KEY_ID]
    assert {sent.scope_region for sent in sts.requests} == {REGION}
    second = ssm.requests[1].headers["Authorization"].decode()
    assert f"Credential={session_key_id(2)}/" in second


def test_a_refused_assume_is_a_put_failure_marked_as_the_assume_step(sts):
    sts.refuse("AccessDenied")
    client = token_store.push_client(_config())
    ssm = _Ssm(client)

    with pytest.raises(token_store.PushFailed) as failed:
        _push(client)

    assert (failed.value.code, failed.value.detail) == ("AccessDenied", token_store.ASSUMING)
    assert failed.value.transient is False
    assert failed.value.__context__ is None and failed.value.__cause__ is None
    printed = "".join(traceback.format_exception(failed.value))
    assert ACCOUNT_ID not in printed and PRINCIPAL_ARN not in printed
    assert ssm.requests == []
