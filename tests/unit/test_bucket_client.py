"""The S3 client is built from ``config.yaml`` alone.

The repository is public and a development run happens on a machine that may hold real
AWS credentials. So ``bucket.client_from_config`` must build its client from the config's
three bucket values and nothing else: never from an ``AWS_*`` variable, never from
``~/.aws/config`` or ``~/.aws/credentials``, and never with a service model from
``~/.aws/models``. These tests fill all of those with values that point elsewhere, build a
real ``botocore`` client, and read the request it would send. A ``before-send`` hook
answers in place of S3, so no socket opens and the network guard stays quiet.

The same capture shows the shape of a PUT: one request carrying the supplied SHA-256 and
no CRC32 of botocore's own, with no aws-chunked body that would carry one.
"""

from __future__ import annotations

import base64
import hashlib
import os

import pytest
from botocore.awsrequest import AWSResponse

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
