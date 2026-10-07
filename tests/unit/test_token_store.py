"""The token parameter's put and pull, answered by botocore's own event hooks.

marketlake #636 carries the Schwab token to the hosted VM through one SSM
``SecureString``. Every test here builds a real botocore SSM client through
``lake.aws_session`` and answers it with a ``before-send`` hook, so the request botocore
would send is read exactly as it would leave, and no socket opens. No test reaches a real
parameter store, and no key here is real.

The put's own client assumes a role through STS at its first request (marketlake #737),
and a hook on the SSM client cannot answer that. So the put's cases here build the SSM
client from a ``KeyPair``, and a hook raises ``_AssumeRoleFailed`` where a case is about
the assume step. ``tests/component/test_token_store_put.py`` drives ``push_client``
itself against an STS on loopback.

The token fixtures carry distinct access and refresh tokens, and the tests assert that
neither reaches any line the module returns. A ``ClientError`` whose message echoes the
token, and a real ``ParamValidationError`` raised over a ``Value`` turned into bytes, are
the two ways botocore could carry a token into an error, so both are driven here.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from botocore.awsrequest import AWSResponse

from lake import aws_session, token_store
from lake.aws_session import KeyPair, _AssumeRoleFailed
from lake.config import Config, ConfigError, Secret
from tests.support.clock import ManualClock

PUT_KEY_ID = "AKIDCOMMANDKEY"
PUT_SECRET = "command-secret-value"
TOKEN_ROLE = "arn:aws:iam::111122223333:role/marketlake-token-writer"
BUCKET_KEY_ID = "AKIDBUCKETBACKUP"
BUCKET_SECRET = "bucket-backup-secret"
REGION = "us-east-2"

ACCESS = "ACCESS-TOKEN-SENTINEL-7f3a"
REFRESH = "REFRESH-TOKEN-SENTINEL-c91e"
MINTED = 1787529900
# The same instant as the pull's lines print it, written out rather than derived.
MINTED_TEXT = "2026-08-24 00:05 UTC"
# The VM's clock for the pull tests that are not about the clock: a week after the mint.
CLOCK = ManualClock(datetime(2026, 8, 31, tzinfo=UTC))


def _token(minted: object = MINTED, access: str = ACCESS, refresh: str = REFRESH) -> dict:
    """A token in ``schwab-py``'s envelope, with distinct access and refresh tokens."""
    return {
        "creation_timestamp": minted,
        "token": {"access_token": access, "refresh_token": refresh, "expires_in": 1800},
    }


def _config(**overrides) -> Config:
    values = {
        "lake_root": "/data/lake",
        "backup_target": "/Volumes/ssd/lake",
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
        "command_access_key_id": PUT_KEY_ID,
        "command_secret_access_key": PUT_SECRET,
        "token_store_role_arn": TOKEN_ROLE,
        "token_store_region": REGION,
        **overrides,
    }
    return Config.from_mapping({key: value for key, value in values.items() if value is not None})


class _Raw:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def stream(self, **kwargs):
        yield self._body


class Ssm:
    """Answers an SSM client's requests in place of AWS, and records each one.

    ``status`` and ``body`` are the answer. A 200 with a JSON body is a success, and any
    other status with ``{"__type": code, "message": ...}`` is the error botocore parses
    into a ``ClientError`` with that code.
    """

    def __init__(self, client, status: int = 200, body: dict | None = None) -> None:
        self.client = client
        self.status = status
        self.body = {"Version": 7, "Tier": "Standard"} if body is None else body
        self.requests: list = []
        client.meta.events.register("before-send.ssm", self._answer)

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        data = json.dumps(self.body).encode()
        headers = {"Content-Length": str(len(data)), "Content-Type": "application/x-amz-json-1.1"}
        return AWSResponse(request.url, self.status, headers, _Raw(data))

    def sent(self, index: int = 0) -> dict:
        return json.loads(self.requests[index].body)

    def target(self, index: int = 0) -> str:
        return self.requests[index].headers["X-Amz-Target"].decode()


def _put_client(config: Config | None = None):
    """An SSM client for the put's cases, signed with the command key as a ``KeyPair``.

    ``push_client`` assumes the role at the first request, which only an STS can answer,
    so these cases sign with a key pair instead and leave the assume to the component
    tests. The put itself is the same either way.
    """
    config = config or _config()
    assert config.command_access_key_id is not None
    assert config.command_secret_access_key is not None
    return aws_session.build_client(
        "ssm",
        region=config.token_store_region,
        source=KeyPair(config.command_access_key_id, config.command_secret_access_key),
        client_config=token_store._SSM_CLIENT_CONFIG,
    )


def _assume_fails(client, code: str, *, transient: bool = False) -> None:
    """Make the next request fail as the assume step does, at signing, before it is sent."""

    def fail(**kwargs):
        raise _AssumeRoleFailed(code, transient=transient)

    client.meta.events.register("before-sign.ssm", fail)


def _error(code: str, message: str = "") -> dict:
    return {"__type": code, "message": message}


# -- 1. the setting ------------------------------------------------------------------


@pytest.mark.parametrize("value", ["file", "both", "store"])
def test_each_known_value_is_its_own_mode_and_prints_nothing(value):
    assert token_store.mode_of(_config(token_store=value)) == (value, None)


def test_an_absent_key_is_file():
    assert _config().token_store == "file"
    assert token_store.mode_of(_config()) == ("file", None)


@pytest.mark.parametrize(
    ("written", "read"),
    [("Store", "'Store'"), ("both ", "'both '"), (None, "'None'"), (False, "'False'")],
)
def test_an_unknown_value_falls_to_unrecognised_and_names_what_it_read(written, read):
    config = Config.from_mapping({**_base(), "token_store": written})
    mode, line = token_store.mode_of(config)
    assert mode == token_store.UNRECOGNISED
    assert mode not in ("file", "both", "store")
    assert line is not None and read in line
    assert "token_store" in line


def _base() -> dict:
    return {
        "lake_root": "/data/lake",
        "backup_target": "/Volumes/ssd/lake",
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
    }


def test_a_yaml_date_is_named_by_its_repr():
    # YAML reads an unquoted ``2026-01-01`` as a date, which the loader stores as its repr.
    _, line = token_store.mode_of(Config.from_mapping({**_base(), "token_store": date(2026, 1, 1)}))
    assert line is not None and "datetime.date(2026, 1, 1)" in line


def test_the_mode_stays_in_the_config_repr():
    # It names a mode rather than a credential, so unlike ``bucket_credentials`` it is
    # shown, and the key values and the role ARN beside it are not.
    shown = repr(_config(token_store="both"))
    assert "token_store='both'" in shown
    assert PUT_SECRET not in shown and PUT_KEY_ID not in shown
    assert TOKEN_ROLE not in shown and "111122223333" not in shown


def test_the_put_keys_load_as_secrets_and_never_borrow_the_bucket_keys():
    config = _config(
        command_access_key_id=None,
        command_secret_access_key=None,
        bucket_access_key_id=BUCKET_KEY_ID,
        bucket_secret_access_key=BUCKET_SECRET,
        bucket_region=REGION,
    )
    assert config.command_access_key_id is None
    assert config.command_secret_access_key is None
    assert token_store.credential_problems(config) == [
        "the token parameter's put needs config key(s): "
        "['command_access_key_id', 'command_secret_access_key']"
    ]
    loaded = _config()
    assert type(loaded.command_access_key_id) is Secret
    assert type(loaded.command_secret_access_key) is Secret
    assert loaded.token_store_role_arn == TOKEN_ROLE
    assert loaded.token_store_region == REGION


@pytest.mark.parametrize(
    "missing", ["command_access_key_id", "command_secret_access_key", "token_store_role_arn"]
)
def test_each_missing_key_is_named(missing):
    problems = token_store.credential_problems(_config(**{missing: None}))
    assert problems == [f"the token parameter's put needs config key(s): ['{missing}']"]


def test_a_missing_region_is_named():
    problems = token_store.credential_problems(_config(token_store_region=None))
    assert problems == ["the token parameter's put needs config key(s): ['token_store_region']"]


@pytest.mark.parametrize("region", ["us east 2", "useast2", "us-east-2/"])
def test_a_malformed_region_is_named(region):
    problems = token_store.credential_problems(_config(token_store_region=region))
    assert problems == [f"token_store_region {region!r} is not an AWS region name like us-east-2"]


def test_complete_keys_have_no_problem():
    assert token_store.credential_problems(_config()) == []


def test_a_padded_region_is_read_trimmed_and_accepted():
    config = _config(token_store_region="  us-east-2  ")
    assert config.token_store_region == "us-east-2"
    assert token_store.credential_problems(config) == []


# -- 2. the put ----------------------------------------------------------------------


def test_the_put_sends_a_secure_standard_overwrite_of_exactly_the_text():
    client = _put_client()
    ssm = Ssm(client)
    text = json.dumps(_token())

    version = token_store.push(client=client, text=text)

    assert version == 7
    assert len(ssm.requests) == 1
    assert ssm.target() == "AmazonSSM.PutParameter"
    sent = ssm.sent()
    assert sent["Name"] == "/marketlake/config/schwab-oauth-token"
    assert sent["Type"] == "SecureString"
    assert sent["Tier"] == "Standard"
    assert sent["Overwrite"] is True
    assert "KeyId" not in sent
    assert "Tags" not in sent
    assert json.loads(sent["Value"]) == _token()
    assert set(sent) == {"Name", "Value", "Type", "Overwrite", "Tier"}


def test_a_value_one_byte_over_4096_is_refused_before_any_request():
    client = _put_client()
    ssm = Ssm(client)
    # A literal size, so a changed limit in the code cannot move the expectation with it.
    text = "x" * 4097
    with pytest.raises(token_store.PushFailed) as refused:
        token_store.push(client=client, text=text)
    assert refused.value.code == "TooLarge"
    assert ssm.requests == []
    line = token_store.push_failure_line(Path("/t/token.json"), refused.value)
    assert "4097 bytes" in line


def test_a_value_of_exactly_4096_bytes_is_sent():
    client = _put_client()
    ssm = Ssm(client)
    token_store.push(client=client, text="x" * 4096)
    assert len(ssm.requests) == 1


def test_the_size_is_counted_in_bytes_not_characters():
    # 2,049 two-byte characters are 4,098 bytes, which AWS refuses.
    client = _put_client()
    ssm = Ssm(client)
    with pytest.raises(token_store.PushFailed):
        token_store.push(client=client, text="é" * 2049)
    assert ssm.requests == []


KEY_CODES = ["AccessDenied", "AccessDeniedException"]
UNKNOWN_KEY_CODES = ["UnrecognizedClientException", "InvalidSignatureException"]
# What STS answers a command key it will not let assume the role: a policy or trust that
# does not allow it, an unknown or deactivated key id, and a wrong secret.
ASSUME_REFUSAL_CODES = ["AccessDenied", "InvalidClientTokenId", "SignatureDoesNotMatch"]


@pytest.mark.parametrize("code", KEY_CODES)
def test_an_ssm_denial_after_a_good_assume_names_the_role_and_the_region(code):
    client = _put_client()
    Ssm(client, status=400, body=_error(code, f"refused {REFRESH} {ACCESS}"))
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    assert failed.value.code == code
    assert failed.value.detail is None
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert line.startswith(
        f"/t/token.json was written and the token parameter was not updated ({code})."
    )
    assert "token_store_role_arn" in line and "token_store_region" in line
    assert "us-east-1 only" in line
    assert "command_access_key_id" not in line
    assert "fails the same way" in line
    assert "Run reauth.sh again" not in line
    assert REFRESH not in line and ACCESS not in line
    assert "\n" not in line


@pytest.mark.parametrize("code", UNKNOWN_KEY_CODES)
def test_an_unknown_session_after_a_good_assume_says_run_reauth_again(code):
    client = _put_client()
    Ssm(client, status=400, body=_error(code, f"refused {REFRESH}"))
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert f"({code})." in line
    assert line.endswith("Run reauth.sh again")
    assert "command_access_key_id" not in line
    assert REFRESH not in line


@pytest.mark.parametrize("code", ASSUME_REFUSAL_CODES)
def test_a_refused_assume_names_the_command_key_and_a_key_not_yet_active(code):
    client = _put_client()
    ssm = Ssm(client)
    _assume_fails(client, code)
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    assert (failed.value.code, failed.value.detail) == (code, token_store.ASSUMING)
    assert failed.value.__cause__ is None and failed.value.__context__ is None
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert line.startswith(
        f"/t/token.json was written and the token parameter was not updated (AssumeRole {code})."
    )
    assert "command_access_key_id and command_secret_access_key" in line
    assert "token_store_role_arn" in line
    assert "A key made minutes ago may not be active yet" in line
    assert "fails the same way" in line
    assert "Run reauth.sh again" not in line
    assert ssm.requests == []
    assert "\n" not in line


def test_an_unavailable_sts_says_run_reauth_again():
    client = _put_client()
    _assume_fails(client, "ServiceUnavailable", transient=True)
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    assert failed.value.transient is True
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert line.endswith("(AssumeRole ServiceUnavailable). Run reauth.sh again")


def test_any_other_code_says_run_reauth_again_and_never_echoes_the_message():
    client = _put_client()
    # A ``ValidationException`` message can echo its input, so this one does.
    Ssm(client, status=400, body=_error("ValidationException", f"bad value {REFRESH} {ACCESS}"))
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    assert failed.value.code == "ValidationException"
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert line.endswith("(ValidationException). Run reauth.sh again")
    assert REFRESH not in line and ACCESS not in line
    # The exception itself carries no token either, so a traceback would not.
    assert REFRESH not in str(failed.value)
    # Neither link to the AWS error survives, so no traceback can print its message.
    assert failed.value.__cause__ is None and failed.value.__context__ is None


def test_a_client_side_validation_error_is_named_by_type_alone():
    client = _put_client()
    ssm = Ssm(client)

    def to_bytes(params, **kwargs):
        params["Value"] = params["Value"].encode()

    client.meta.events.register("before-parameter-build.ssm.PutParameter", to_bytes)
    with pytest.raises(token_store.PushFailed) as failed:
        token_store.push(client=client, text=json.dumps(_token()))
    assert failed.value.code == "ParamValidationError"
    assert ssm.requests == []
    line = token_store.push_failure_line(Path("/t/token.json"), failed.value)
    assert REFRESH not in line and ACCESS not in line
    assert REFRESH not in str(failed.value)
    assert failed.value.__cause__ is None and failed.value.__context__ is None


def test_the_validation_error_really_carries_the_token():
    # The control for the test above: unreported, botocore's own error prints the value.
    from botocore.exceptions import ParamValidationError

    client = _put_client()
    Ssm(client)
    client.meta.events.register(
        "before-parameter-build.ssm.PutParameter",
        lambda params, **kwargs: params.update(Value=params["Value"].encode()),
    )
    with pytest.raises(ParamValidationError) as raised:
        client.put_parameter(
            Name="/x", Value=json.dumps(_token()), Type="SecureString", Overwrite=True
        )
    assert REFRESH in str(raised.value)


# -- 3. the shared session -----------------------------------------------------------


@pytest.fixture
def hostile_aws(tmp_path, monkeypatch):
    """Every place botocore looks for settings, pointed somewhere else."""
    config_file = tmp_path / "aws-config"
    config_file.write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-config-file.invalid\n"
    )
    credentials_file = tmp_path / "aws-credentials"
    credentials_file.write_text(
        "[default]\naws_access_key_id = AKIDFROMFILE\naws_secret_access_key = from-file\n"
    )
    env = {
        "AWS_ACCESS_KEY_ID": "AKIDFROMENV",
        "AWS_SECRET_ACCESS_KEY": "from-env",
        "AWS_SESSION_TOKEN": "token-from-env",
        "AWS_REGION": "eu-west-1",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_PROFILE": "no-such-profile",
        "AWS_ENDPOINT_URL": "https://from-env.invalid",
        "AWS_ENDPOINT_URL_SSM": "https://from-env-ssm.invalid",
        "AWS_CONFIG_FILE": str(config_file),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env


def _assert_signed_by_the_config(request) -> None:
    assert request.url == f"https://ssm.{REGION}.amazonaws.com/"
    authorization = request.headers["Authorization"].decode()
    assert f"Credential={PUT_KEY_ID}/" in authorization
    assert f"/{REGION}/ssm/" in authorization
    assert "X-Amz-Security-Token" not in request.headers


def test_a_key_pair_client_ignores_the_environment_and_the_aws_files(hostile_aws):
    client = _put_client()
    ssm = Ssm(client)
    token_store.push(client=client, text=json.dumps(_token()))
    _assert_signed_by_the_config(ssm.requests[0])
    for key, value in hostile_aws.items():
        assert os.environ[key] == value
    assert "AWS_EC2_METADATA_DISABLED" not in os.environ


@pytest.fixture
def hostile_home(tmp_path, monkeypatch):
    """``~/.aws/config`` and ``~/.aws/credentials`` in the home directory, pointing elsewhere."""
    home = tmp_path / "home"
    aws = home / ".aws"
    aws.mkdir(parents=True)
    (aws / "config").write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-home.invalid\n"
    )
    (aws / "credentials").write_text(
        "[default]\naws_access_key_id = AKIDFROMHOME\naws_secret_access_key = from-home\n"
        "aws_session_token = token-from-home\n"
    )
    monkeypatch.setenv("HOME", str(home))
    for key in [key for key in os.environ if key.startswith("AWS_")]:
        monkeypatch.delenv(key)
    return home


def test_a_key_pair_client_ignores_the_aws_files_in_the_home_directory(hostile_home):
    client = _put_client()
    ssm = Ssm(client)
    token_store.push(client=client, text=json.dumps(_token()))
    _assert_signed_by_the_config(ssm.requests[0])
    # The home files are there to be found: a plain session with no redirect reads them.
    import botocore.session

    assert botocore.session.Session().get_scoped_config().get("region") == "ap-south-1"


def test_building_a_client_leaves_the_callers_settings_unchanged():
    # botocore rewrites the ``retries`` dict it is handed, so a builder that passed a
    # module constant straight through would change it on the first build.
    from lake import bucket

    settings = (token_store._SSM_CLIENT_CONFIG, bucket._S3_CLIENT_CONFIG)
    expected = [
        {
            "connect_timeout": 10,
            "read_timeout": 30,
            "retries": {"mode": "standard", "max_attempts": 3},
        },
        {
            "connect_timeout": 10,
            "read_timeout": 60,
            "retries": {"mode": "standard", "max_attempts": 3},
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    ]
    for _ in range(2):
        token_store.push_client(_config())
        bucket.client_from_config(
            _config(
                backup_target="s3://lake-backup/lake",
                bucket_access_key_id=BUCKET_KEY_ID,
                bucket_secret_access_key=BUCKET_SECRET,
                bucket_region=REGION,
            )
        )
        assert [dict(setting) for setting in settings] == expected


def test_the_ssm_client_carries_the_stated_timeouts_and_retries():
    built = token_store.push_client(_config()).meta.config
    assert built.connect_timeout == 10
    assert built.read_timeout == 30
    # botocore counts the first attempt too, so three retries read back as four attempts.
    assert built.retries == {"mode": "standard", "total_max_attempts": 4}


def test_the_s3_client_carries_the_stated_timeouts_retries_and_checksum_settings():
    from lake import bucket

    config = _config(
        backup_target="s3://lake-backup/lake",
        bucket_access_key_id=BUCKET_KEY_ID,
        bucket_secret_access_key=BUCKET_SECRET,
        bucket_region=REGION,
    )
    built = bucket.client_from_config(config).meta.config
    assert built.connect_timeout == 10
    assert built.read_timeout == 60
    assert built.retries == {"mode": "standard", "total_max_attempts": 4}
    assert built.request_checksum_calculation == "when_required"
    assert built.response_checksum_validation == "when_required"


def test_the_keys_source_names_a_missing_key():
    # The bucket checks run first everywhere today, so only a direct call reaches this.
    config = _config(bucket_access_key_id=BUCKET_KEY_ID)
    with pytest.raises(ConfigError) as refused:
        aws_session.source_from_bucket_credentials(config)
    assert str(refused.value) == "the bucket needs config key(s): ['bucket_secret_access_key']"


def test_no_service_model_loads_from_the_home_directory():
    client = token_store.push_client(_config())
    assert not any(".aws" in path for path in client._loader.search_paths)
    sts = client._request_signer._credentials._refresh_using.sts
    assert not any(".aws" in path for path in sts._loader.search_paths)


def test_the_bucket_module_does_not_carry_the_metadata_constants():
    # A copy in ``bucket`` would be one a test could patch while the builder read this one,
    # and the never-answers timeout test could then pass against the real address.
    from lake import bucket

    for name in ("METADATA_BASE_URL", "METADATA_TIMEOUT_S", "METADATA_ATTEMPTS"):
        assert not hasattr(bucket, name)
        assert hasattr(aws_session, name)


def test_importing_reauth_loads_neither_bucket_nor_manifest(tmp_path):
    # A fresh interpreter, because this one has long since imported both.
    code = (
        "import json, sys\n"
        "import lake.reauth\n"
        "print(json.dumps(sorted(m for m in ('lake.bucket', 'lake.manifest') "
        "if m in sys.modules)))\n"
    )
    environment = {
        **os.environ,
        "MARKETLAKE_CONFIG_DIR": str(tmp_path / "config"),
        "HOME": str(tmp_path / "home"),
    }
    result = subprocess.run(
        [sys.executable, "-c", code], env=environment, capture_output=True, text=True, check=True
    )
    assert json.loads(result.stdout) == []
    # The control: the same check finds the module when it is imported.
    control = subprocess.run(
        [sys.executable, "-c", code.replace("import lake.reauth", "import lake.bucket")],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(control.stdout) == ["lake.bucket", "lake.manifest"]


# -- 4. the pull ---------------------------------------------------------------------


def _pull_config() -> Config:
    return _config(
        bucket_access_key_id=BUCKET_KEY_ID,
        bucket_secret_access_key=BUCKET_SECRET,
        bucket_region=REGION,
    )


class Store:
    """A pull client whose ``GetParameter`` is answered by a hook, and what it was asked."""

    def __init__(self, value: object = None, *, status: int = 200, body: dict | None = None):
        self.client = token_store.pull_client(_pull_config())
        if body is None:
            parameter = {"Name": token_store.PARAMETER_NAME, "Type": "SecureString", "Version": 3}
            if value is not None:
                parameter["Value"] = value if isinstance(value, str) else json.dumps(value)
            body = {"Parameter": parameter}
        self.ssm = Ssm(self.client, status=status, body=body)
        self.builds = 0

    def factory(self):
        self.builds += 1
        return self.client


def _pull(store: Store, token: Path) -> token_store.PullResult:
    return token_store.pull(client_factory=store.factory, token_path=token, clock=CLOCK)


def _assert_no_token_in(result: token_store.PullResult) -> None:
    assert ACCESS not in result.line and REFRESH not in result.line
    assert "\n" not in result.line
    # The reason rides the Sunday reminder to the phone, so it carries neither a token byte
    # nor the token's path, which the line names.
    reason = "" if result.reason is None else result.reason
    assert ACCESS not in reason and REFRESH not in reason
    assert "\n" not in reason
    assert "token.json" not in reason


def test_the_pull_asks_for_the_decrypted_parameter_signed_by_the_bucket_key(tmp_path):
    store = Store(_token())
    _pull(store, tmp_path / "token.json")
    assert store.ssm.target() == "AmazonSSM.GetParameter"
    assert store.ssm.sent() == {
        "Name": "/marketlake/config/schwab-oauth-token",
        "WithDecryption": True,
    }
    authorization = store.ssm.requests[0].headers["Authorization"].decode()
    assert f"Credential={BUCKET_KEY_ID}/" in authorization


def test_an_absent_file_is_written_owner_only(tmp_path):
    token = tmp_path / "token.json"
    result = _pull(Store(_token()), token)
    assert result.outcome == "wrote"
    assert json.loads(token.read_text()) == _token()
    assert token.stat().st_mode & 0o777 == 0o600
    assert f"minted {MINTED_TEXT}" in result.line
    assert result.line.endswith("The local file was absent")
    _assert_no_token_in(result)
    # Atomic: no temp file is left beside it.
    assert sorted(path.name for path in tmp_path.iterdir()) == ["token.json"]


@pytest.mark.parametrize(
    "local",
    ["not json at all", "[]", json.dumps({"token": {}}), json.dumps({"creation_timestamp": "x"})],
)
def test_an_unreadable_or_malformed_file_is_written(tmp_path, local):
    token = tmp_path / "token.json"
    token.write_text(local)
    result = _pull(Store(_token()), token)
    assert result.outcome == "wrote"
    assert json.loads(token.read_text()) == _token()


def test_a_later_parameter_is_written_over_an_earlier_file(tmp_path):
    token = tmp_path / "token.json"
    token.write_text(json.dumps(_token(minted=MINTED - 604800, access="old-a", refresh="old-r")))
    before = token.stat().st_ino
    result = _pull(Store(_token()), token)
    assert result.outcome == "wrote"
    assert json.loads(token.read_text()) == _token()
    # The write lands by renaming a new file over the old one, so the inode changes. A
    # write in place into the old file keeps it, and a crash part-way through it would
    # leave a torn token where the live one was.
    assert token.stat().st_ino != before
    _assert_no_token_in(result)


def test_the_pull_writes_through_the_re_auths_atomic_writer(tmp_path, monkeypatch):
    from lake import reauth

    calls = []
    real = reauth.write_token

    def spy(path, payload):
        calls.append(Path(path))
        return real(path, payload)

    monkeypatch.setattr(reauth, "write_token", spy)
    token = tmp_path / "token.json"
    assert _pull(Store(_token()), token).outcome == "wrote"
    assert calls == [token]


def test_an_equal_mint_time_writes_nothing(tmp_path):
    # The local access token differs, as it does after any refresh, so a rule that compared
    # contents rather than mint times would write here.
    token = tmp_path / "token.json"
    token.write_text(json.dumps(_token(access="REFRESHED-LOCALLY")))
    before = token.stat()
    result = _pull(Store(_token()), token)
    after = token.stat()
    assert result.outcome == "current"
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert json.loads(token.read_text())["token"]["access_token"] == "REFRESHED-LOCALLY"
    _assert_no_token_in(result)


def test_an_older_parameter_writes_nothing(tmp_path):
    token = tmp_path / "token.json"
    later = _token(minted=MINTED + 60, access="later-a", refresh="later-r")
    token.write_text(json.dumps(later))
    result = _pull(Store(_token()), token)
    assert result.outcome == "store older"
    assert json.loads(token.read_text()) == later
    _assert_no_token_in(result)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(
            {"creation_timestamp": MINTED + 60, "token": {"access_token": ACCESS}},
            id="no-refresh-token",
        ),
        pytest.param(
            {"creation_timestamp": MINTED + 60, "token": {"refresh_token": REFRESH}},
            id="no-access-token",
        ),
        pytest.param(
            {"token": {"access_token": ACCESS, "refresh_token": REFRESH}}, id="no-mint-time"
        ),
        pytest.param(_token(minted=True), id="bool-mint-time"),
        pytest.param(_token(minted=str(MINTED + 60)), id="string-mint-time"),
        pytest.param(f"not json {REFRESH}", id="not-json"),
        pytest.param("", id="empty"),
        # Present but unusable: a later mint, so only the shape check stops the write.
        pytest.param(_token(minted=MINTED + 60, access=""), id="empty-access"),
        pytest.param(_token(minted=MINTED + 60, refresh=""), id="empty-refresh"),
        pytest.param(
            {
                "creation_timestamp": MINTED + 60,
                "token": {"access_token": ACCESS, "refresh_token": 7},
            },
            id="int-refresh",
        ),
        pytest.param({"creation_timestamp": MINTED + 60, "token": "x"}, id="token-is-a-string"),
        pytest.param("5", id="payload-is-a-number"),
    ],
)
def test_a_malformed_parameter_writes_nothing_over_a_good_file(tmp_path, value):
    token = tmp_path / "token.json"
    good = _token(minted=MINTED - 60, access="good-a", refresh="good-r")
    token.write_text(json.dumps(good))
    before = token.stat()
    result = _pull(Store(value), token)
    assert result.outcome == "unreadable"
    assert json.loads(token.read_text()) == good
    assert token.stat().st_ino == before.st_ino
    _assert_no_token_in(result)


def test_a_parameter_without_a_value_writes_nothing_over_an_absent_file(tmp_path):
    token = tmp_path / "token.json"
    result = _pull(Store(None), token)
    assert result.outcome == "unreadable"
    assert "(the parameter is empty)" in result.line
    assert not token.exists()


def test_an_empty_parameter_is_named_empty(tmp_path):
    result = _pull(Store(""), tmp_path / "token.json")
    assert result.outcome == "unreadable"
    assert "(the parameter is empty)" in result.line
    assert result.reason == "the parameter is empty"


def _wrote_over_absent(tmp_path: Path) -> token_store.PullResult:
    return _pull(Store(_token()), tmp_path / "token.json")


def _wrote_over_unreadable(tmp_path: Path) -> token_store.PullResult:
    token = tmp_path / "token.json"
    token.write_text("not json at all")
    return _pull(Store(_token()), token)


def _wrote_over_earlier(tmp_path: Path) -> token_store.PullResult:
    token = tmp_path / "token.json"
    token.write_text(json.dumps(_token(minted=MINTED - 604800)))
    return _pull(Store(_token()), token)


def _current(tmp_path: Path) -> token_store.PullResult:
    token = tmp_path / "token.json"
    token.write_text(json.dumps(_token()))
    return _pull(Store(_token()), token)


def _store_older(tmp_path: Path) -> token_store.PullResult:
    token = tmp_path / "token.json"
    token.write_text(json.dumps(_token(minted=MINTED + 60)))
    return _pull(Store(_token()), token)


def _no_credentials(tmp_path: Path) -> token_store.PullResult:
    def factory():
        raise aws_session._MetadataLookupFailed("none returned")

    return token_store.pull(client_factory=factory, token_path=tmp_path / "token.json", clock=CLOCK)


@pytest.mark.parametrize(
    ("drive", "outcome"),
    [
        (_wrote_over_absent, "wrote"),
        (_wrote_over_unreadable, "wrote"),
        (_wrote_over_earlier, "wrote"),
        (_current, "current"),
        (_store_older, "store older"),
        (_no_credentials, "no credentials"),
    ],
    ids=lambda value: getattr(value, "__name__", value),
)
def test_only_an_unreadable_pull_carries_a_reason(tmp_path, drive, outcome):
    # ``pull`` keeps a local named ``reason`` on its ``wrote`` branch, saying what the local
    # file was. It names the local file's state, not the parameter's, and must not reach
    # the field the Sunday reminder reads.
    result = drive(tmp_path)
    assert result.outcome == outcome
    assert result.reason is None


# ``UnrecognizedClientException`` is not in SSM's model, so botocore raises the generic
# ``ClientError`` for it. Its code must still be the one named, not the class.
@pytest.mark.parametrize(
    "code", ["ParameterNotFound", "AccessDeniedException", "UnrecognizedClientException"]
)
def test_an_aws_error_is_unreadable_by_its_code_alone(tmp_path, code):
    store = Store(status=400, body=_error(code, f"echo {REFRESH}"))
    result = _pull(store, tmp_path / "token.json")
    assert result.outcome == "unreadable"
    assert f"({code})" in result.line
    assert result.reason == code
    _assert_no_token_in(result)


def test_a_role_that_cannot_be_assumed_is_unreadable_not_a_traceback(tmp_path):
    # A laptop set to ``bucket_credentials: assume_role`` signs the pull as the bucket's
    # role. One whose assume fails gets the unreadable line, never a traceback.
    store = Store(_token())
    _assume_fails(store.client, "AccessDenied")
    result = _pull(store, tmp_path / "token.json")
    assert result.outcome == "unreadable"
    assert "(AssumeRole AccessDenied)" in result.line
    assert result.reason == "AssumeRole AccessDenied"
    assert not (tmp_path / "token.json").exists()


def test_no_credentials_from_the_metadata_service_is_its_own_outcome(tmp_path):
    def factory():
        raise aws_session._MetadataLookupFailed("none returned")

    result = token_store.pull(
        client_factory=factory, token_path=tmp_path / "token.json", clock=CLOCK
    )
    assert result.outcome == "no credentials"
    assert "(none returned)" in result.line
    assert not (tmp_path / "token.json").exists()


def test_a_config_problem_in_the_build_is_raised_not_reported(tmp_path):
    def factory():
        raise ConfigError("the bucket needs config key(s): ['bucket_region']")

    with pytest.raises(ConfigError):
        token_store.pull(client_factory=factory, token_path=tmp_path / "token.json", clock=CLOCK)


def test_each_outcome_has_its_exit_code():
    assert token_store.EXIT_CODES == {
        "wrote": 0,
        "current": 0,
        "store older": 1,
        "unreadable": 1,
        "no credentials": 3,
    }


@pytest.mark.parametrize(
    ("overrides", "named"),
    [
        ({"bucket_region": None}, "bucket_region"),
        ({"bucket_region": "us east 2"}, "bucket_region"),
        ({"bucket_credentials": "Instance_Profile"}, "bucket_credentials"),
        ({"bucket_credentials": "instance_profile"}, "must not hold"),
    ],
)
def test_the_pull_client_refuses_a_config_problem_by_name(overrides, named):
    values = {
        **_base(),
        "bucket_access_key_id": BUCKET_KEY_ID,
        "bucket_secret_access_key": BUCKET_SECRET,
        "bucket_region": REGION,
        **overrides,
    }
    config = Config.from_mapping(values)
    with pytest.raises(ConfigError) as refused:
        token_store.pull_client(config)
    assert named in str(refused.value)
    assert BUCKET_SECRET not in str(refused.value)


# -- the future-mint guard -------------------------------------------------------------

# The VM's clock in these tests, pinned so the offsets below are literal. It sits years
# from any date the suite runs on, so a pull that read the real clock fails here whatever
# the day: a token minted a day before 2030 is in the real clock's far future.
NOW = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)


def _minted_at(offset: timedelta) -> int:
    return int((NOW + offset).timestamp())


def _pull_at_now(store: Store, token: Path) -> token_store.PullResult:
    return token_store.pull(client_factory=store.factory, token_path=token, clock=ManualClock(NOW))


def test_a_mint_time_two_hours_ahead_writes_nothing_over_an_absent_file(tmp_path):
    token = tmp_path / "token.json"
    result = _pull_at_now(Store(_token(minted=_minted_at(timedelta(hours=2)))), token)
    assert result.outcome == "unreadable"
    assert "in the future" in result.line
    assert result.reason == "its mint time is in the future"
    assert token_store.EXIT_CODES[result.outcome] == 1
    assert not token.exists()
    _assert_no_token_in(result)


def test_a_mint_time_two_hours_ahead_writes_nothing_over_a_good_older_file(tmp_path):
    token = tmp_path / "token.json"
    good = _token(minted=_minted_at(-timedelta(days=1)), access="good-a", refresh="good-r")
    token.write_text(json.dumps(good))
    before = token.stat()
    result = _pull_at_now(Store(_token(minted=_minted_at(timedelta(hours=2)))), token)
    assert result.outcome == "unreadable"
    assert json.loads(token.read_text()) == good
    assert token.stat().st_ino == before.st_ino


@pytest.mark.parametrize(
    ("offset", "outcome"),
    [
        (timedelta(minutes=59), "wrote"),
        (timedelta(hours=1), "wrote"),
        (timedelta(hours=1, seconds=1), "unreadable"),
        (timedelta(minutes=61), "unreadable"),
    ],
)
def test_the_allowance_is_one_hour_and_inclusive(tmp_path, offset, outcome):
    result = _pull_at_now(Store(_token(minted=_minted_at(offset))), tmp_path / "token.json")
    assert result.outcome == outcome


def test_a_day_old_parameter_is_written_by_the_injected_clock(tmp_path):
    token = tmp_path / "token.json"
    result = _pull_at_now(Store(_token(minted=_minted_at(-timedelta(days=1)))), token)
    assert result.outcome == "wrote"
    assert result.line.endswith("The local file was absent")


def test_a_mint_time_thirty_minutes_ahead_is_accepted(tmp_path):
    # Clock skew between the laptop that minted it and the VM reading it.
    token = tmp_path / "token.json"
    ahead = _token(minted=_minted_at(timedelta(minutes=30)))
    result = _pull_at_now(Store(ahead), token)
    assert result.outcome == "wrote"
    assert json.loads(token.read_text()) == ahead
