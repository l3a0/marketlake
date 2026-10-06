"""The S3 client is built from ``config.yaml`` alone on the key path.

The repository is public and a development run happens on a machine that may hold real
AWS credentials. So on the key path ``bucket.client_from_config`` must build its client
from the config's three bucket values and nothing else: never from an ``AWS_*`` variable, never from
``~/.aws/config`` or ``~/.aws/credentials``, and never with a service model from
``~/.aws/models``. These tests fill all of those with values that point elsewhere, build a
real ``botocore`` client, and read the request it would send. A ``before-send`` hook
answers in place of S3, so no socket opens and the network guard stays quiet.

The same capture shows the shape of a PUT: one request carrying the supplied SHA-256 and
no CRC32 of botocore's own, with no aws-chunked body that would carry one.

``bucket_credentials`` picks the credential path, marketlake #663. The refusals that need
no metadata service are here: an unrecognised value, a key beside ``instance_profile``, a
missing region, and a metadata fetcher that fails to build, which must leave the
process's proxy variables as it found them. So is the metadata service's real address,
which no test can reach. The tests that talk to a metadata service
cross a real HTTP boundary, so they live in
``tests/component/test_bucket_instance_profile.py``.
"""

from __future__ import annotations

import base64
import hashlib
import os

import botocore.utils
import pytest
from botocore.awsrequest import AWSResponse

from lake import bucket
from lake.bucket import client_from_config
from lake.config import Config, ConfigError

CONFIG_KEY_ID = "AKIDFROMCONFIG"
CONFIG_SECRET = "secret-from-config"
REGION = "us-east-2"


def _config(**overrides) -> Config:
    values = {
        "lake_root": "/data/lake",
        "backup_target": "s3://lake-backup/lake",
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
        "bucket_access_key_id": CONFIG_KEY_ID,
        "bucket_secret_access_key": CONFIG_SECRET,
        "bucket_region": REGION,
        **overrides,
    }
    return Config.from_mapping(values)


@pytest.fixture
def hostile_aws(tmp_path, monkeypatch):
    """Every place botocore looks for settings, pointed somewhere else."""
    config_file = tmp_path / "aws-config"
    config_file.write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-config-file.invalid\n"
        "[profile elsewhere]\nregion = eu-west-3\n"
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
        "AWS_ENDPOINT_URL_S3": "https://from-env-s3.invalid",
        "AWS_CONFIG_FILE": str(config_file),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env


def _capture(client) -> list:
    sent = []

    class Raw:
        def stream(self, **kwargs):
            yield b""

    def answer(request, **kwargs):
        sent.append(request)
        return AWSResponse(
            request.url, 200, {"Content-Length": "0", "x-amz-version-id": "v1"}, Raw()
        )

    client.meta.events.register("before-send.s3", answer)
    return sent


def test_the_client_ignores_the_environment_and_the_aws_files(hostile_aws):
    client = client_from_config(_config())
    sent = _capture(client)

    client.head_object(Bucket="lake-backup", Key="lake/manifest.jsonl")

    assert len(sent) == 1
    request = sent[0]
    assert request.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")
    authorization = request.headers["Authorization"].decode()
    assert f"Credential={CONFIG_KEY_ID}/" in authorization
    assert f"/{REGION}/s3/" in authorization
    assert "X-Amz-Security-Token" not in request.headers
    assert client.meta.region_name == REGION


def test_the_environment_is_put_back_after_the_build(hostile_aws):
    client_from_config(_config())
    for key, value in hostile_aws.items():
        assert os.environ[key] == value
    assert "AWS_EC2_METADATA_DISABLED" not in os.environ


def test_no_service_model_loads_from_the_home_directory():
    client = client_from_config(_config())
    loader = client._loader
    assert not any(".aws" in path for path in loader.search_paths)


def test_a_put_is_one_request_carrying_the_supplied_sha256_and_nothing_else():
    client = client_from_config(_config())
    sent = _capture(client)
    data = b"a" * (9 * 1024 * 1024)
    digest = base64.b64encode(hashlib.sha256(data).digest()).decode()

    client.put_object(
        Bucket="lake-backup",
        Key="lake/x",
        Body=data,
        ChecksumSHA256=digest,
        StorageClass="STANDARD_IA",
    )

    assert len(sent) == 1
    headers = {key.lower(): value for key, value in sent[0].headers.items()}
    assert headers["x-amz-checksum-sha256"] == digest.encode()
    assert headers["x-amz-storage-class"] == b"STANDARD_IA"
    assert not any(key.startswith("x-amz-checksum-crc") for key in headers)
    assert "x-amz-trailer" not in headers
    assert b"aws-chunked" not in headers.get("content-encoding", b"")


@pytest.mark.parametrize(
    "missing", ["bucket_access_key_id", "bucket_secret_access_key", "bucket_region"]
)
def test_a_config_without_a_bucket_value_builds_no_client(missing):
    config = _config(backup_target="/Volumes/ssd", **{missing: None})
    with pytest.raises(ConfigError, match=missing):
        client_from_config(config)


@pytest.mark.parametrize("region", ["us east 2", "us-east-2/"])
def test_a_region_botocore_refuses_is_one_config_line(region):
    # ``require_bucket_settings`` catches these first. This is the second line of
    # defence for whatever botocore refuses that the shape check lets through.
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config(bucket_region=region))
    message = str(refused.value)
    assert "InvalidRegionError" in message
    assert "\n" not in message
    assert CONFIG_SECRET not in message and CONFIG_KEY_ID not in message


@pytest.fixture
def hostile_home(tmp_path, monkeypatch):
    """``~/.aws/config`` and ``~/.aws/credentials`` in the home directory, pointing elsewhere.

    ``hostile_aws`` reaches its files only through ``AWS_CONFIG_FILE`` and
    ``AWS_SHARED_CREDENTIALS_FILE``, which the build deletes along with every other
    ``AWS_*`` variable, so it never shows what botocore does with no variable set. That
    is the default home-directory files, and only pointing both variables at the null
    device keeps them out.
    """
    home = tmp_path / "home"
    aws = home / ".aws"
    aws.mkdir(parents=True)
    (aws / "config").write_text(
        "[default]\nregion = ap-south-1\nendpoint_url = https://from-home.invalid\n"
    )
    (aws / "credentials").write_text(
        "[default]\naws_access_key_id = AKIDFROMHOME\naws_secret_access_key = from-home\n"
        "aws_session_token = token-from-home\n"
        "endpoint_url = https://from-home.invalid\n"
    )
    monkeypatch.setenv("HOME", str(home))
    for key in [key for key in os.environ if key.startswith("AWS_")]:
        monkeypatch.delenv(key)
    return home


def test_the_client_ignores_the_aws_files_in_the_home_directory(hostile_home):
    client = client_from_config(_config())
    sent = _capture(client)

    client.head_object(Bucket="lake-backup", Key="lake/manifest.jsonl")

    assert len(sent) == 1
    request = sent[0]
    assert "from-home" not in request.url
    assert request.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")
    authorization = request.headers["Authorization"].decode()
    assert f"Credential={CONFIG_KEY_ID}/" in authorization
    assert "X-Amz-Security-Token" not in request.headers
    # The home files are there to be found: a plain session with no redirect reads them.
    import botocore.session

    assert botocore.session.Session().get_scoped_config().get("region") == "ap-south-1"


# -- the credential source, marketlake #663 -------------------------------------------

# The shape of an AWS secret access key, pasted where the source belongs.
SECRET_SHAPED = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


def _no_build(monkeypatch) -> None:
    monkeypatch.setattr(bucket, "_build_client", lambda cfg: pytest.fail("no client is built"))


def _profile_config(**overrides) -> Config:
    values = {
        "bucket_credentials": "instance_profile",
        "bucket_access_key_id": None,
        "bucket_secret_access_key": None,
        **overrides,
    }
    return _config(**values)


UNRECOGNISED = [None, "", "Instance_Profile", " instance_profile ", "key", 1, SECRET_SHAPED]


@pytest.mark.parametrize("value", UNRECOGNISED)
def test_an_unrecognised_credential_source_refuses_alone_and_never_falls_back(monkeypatch, value):
    # The config holds valid keys, so a build that fell back to ``keys`` would succeed.
    # Reaching the builder at all fails the test.
    _no_build(monkeypatch)
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config(bucket_credentials=value))
    message = str(refused.value)
    assert message == "bucket_credentials must be keys or instance_profile"
    assert SECRET_SHAPED not in message


@pytest.mark.parametrize("value", UNRECOGNISED)
def test_the_builder_takes_neither_path_on_an_unrecognised_source(value):
    # The check above runs first. Should it ever let a value through, the builder still
    # takes each path only on its own value. The config holds valid keys, so a builder
    # that sent every other value down the key path would build a client here.
    with pytest.raises(ConfigError) as refused:
        bucket._build_client(_config(bucket_credentials=value))
    assert str(refused.value) == "bucket_credentials must be keys or instance_profile"


def test_a_pasted_credential_source_stays_out_of_the_config_repr():
    config = _config(bucket_credentials=SECRET_SHAPED)
    assert config.bucket_credentials == SECRET_SHAPED
    assert SECRET_SHAPED not in repr(config)


@pytest.mark.parametrize("present", ["bucket_access_key_id", "bucket_secret_access_key"])
def test_either_key_beside_instance_profile_refuses_naming_it(monkeypatch, present):
    # Each key field alone, so a check that refused only when both were present fails.
    _no_build(monkeypatch)
    with pytest.raises(ConfigError) as refused:
        client_from_config(_profile_config(**{present: "a-value"}))
    assert str(refused.value) == (
        f"bucket_credentials is instance_profile, so the config must not hold ['{present}']"
    )


def test_a_key_and_no_region_beside_instance_profile_refuse_as_one_line(monkeypatch):
    _no_build(monkeypatch)
    with pytest.raises(ConfigError) as refused:
        client_from_config(_profile_config(bucket_access_key_id="a-value", bucket_region=None))
    assert str(refused.value) == (
        "bucket_credentials is instance_profile, so the config must not hold "
        "['bucket_access_key_id']. the bucket needs config key(s): ['bucket_region']"
    )


def test_the_metadata_address_is_the_one_aws_serves():
    # Every test points the address at a server on loopback, so none of them reaches
    # this value. botocore's own constant is the reference, not one built from it here.
    assert bucket.METADATA_BASE_URL == botocore.utils.METADATA_BASE_URL


def test_instance_profile_still_needs_the_region(monkeypatch):
    _no_build(monkeypatch)
    with pytest.raises(ConfigError) as refused:
        client_from_config(_profile_config(bucket_region=None))
    message = str(refused.value)
    assert "bucket_region" in message
    assert "bucket_access_key_id" not in message and "bucket_secret_access_key" not in message


def test_a_metadata_fetcher_that_fails_to_build_leaves_the_proxies_set(monkeypatch):
    # The proxy variables are removed only while the fetcher is built. A fetcher whose
    # constructor raises must still put them back, or the rest of the process, Sunday's
    # Schwab canary included, loses them.
    monkeypatch.delenv("http_proxy", raising=False)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setattr(bucket, "METADATA_BASE_URL", "not a url")
    with pytest.raises(ConfigError) as refused:
        client_from_config(_profile_config())
    assert os.environ["HTTP_PROXY"] == "http://127.0.0.1:9"
    assert "AWS_EC2_METADATA_DISABLED" not in os.environ
    # A build error that is not the metadata lookup keeps the config.yaml line, and the
    # line that sends the operator to the instance profile is not given to it.
    assert str(refused.value) == (
        "the bucket client could not be built from config.yaml (InvalidIMDSEndpointError)"
    )
