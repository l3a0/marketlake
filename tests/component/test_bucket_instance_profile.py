"""The instance-profile credential path, against a metadata service on loopback.

On the VM, ``bucket_credentials: instance_profile`` makes the bucket client take
short-lived credentials from the EC2 instance metadata service rather than a key in
``config.yaml``, marketlake #663. These tests cross a real HTTP boundary, so they live
here rather than beside ``tests/unit/test_bucket_client.py``, which opens no socket.
A server on ``127.0.0.1`` plays the metadata service, reached through the
``aws_session.METADATA_BASE_URL`` seam, because the build removes
``AWS_EC2_METADATA_SERVICE_ENDPOINT`` with every other ``AWS_*`` variable. The suite's
socket guard in ``tests/conftest.py`` allows loopback and refuses the real metadata
address. A ``before-send`` hook answers in place of S3, so no request reaches AWS.

What they cover:

1. The profile path signs with the metadata credentials and its session token, and
   ignores ``BOTO_CONFIG``, ``~/.boto``, the environment's keys, endpoint and region,
   a proxy variable, and any service model under ``~/.aws/models``. A failed role
   listing is asked again once and no more, and a service that never answers refuses
   within the metadata timeout.
2. The key path is unchanged and never asks the metadata service.
3. Both forms of a failed lookup refuse at build as one line naming both fixes: a
   service that refuses the token, which raises ``MetadataRetrievalError``, and a
   service with no instance profile attached, which returns no credentials. Each
   reaches ``connect`` as ``BucketSettingsInvalid`` and Sunday's job as a finding.
4. Any other build error on the profile path keeps the ``config.yaml`` line.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import botocore.utils
import pytest
from botocore.awsrequest import AWSResponse

from lake import aws_session
from lake.bucket import BucketSettingsInvalid, client_from_config, connect
from lake.config import Config, ConfigError
from tests.component.test_bucket_scrub import _sunday_cli, _uploaded

REGION = "us-east-2"
ROLE_NAME = "marketlake-bucket"
TOKEN = "imds-session-token"
ROLE_LISTING = "/latest/meta-data/iam/security-credentials/"


def _answers(prefix: str) -> dict[str, str]:
    """The credentials a server hands out, each value marked with where it came from."""
    return {
        "AccessKeyId": f"ASIA{prefix}",
        "SecretAccessKey": f"{prefix.lower()}-secret",
        "Token": f"{prefix.lower()}-session-token",
    }


METADATA = _answers("FROMMETADATA")
PROXY = _answers("FROMPROXY")


class _Server:
    """A loopback HTTP server that records every request and answers like IMDSv2.

    ``mode`` decides the answer. ``ok`` hands out ``creds``. ``no_role`` answers the
    token and has no instance profile, so the role listing is a 404. ``refuse_token``
    answers the token request with a 403, which is what a service that requires tokens
    and turns this caller away does. ``role_500_once`` answers the first role listing
    with a 500 and every later one as ``ok`` does, and ``role_500`` answers every role
    listing with a 500. ``null_body``, ``bad_expiration`` and ``no_key_id`` answer the
    credentials request with what only a broken or impersonated service would send.
    """

    def __init__(self, creds: dict[str, str]) -> None:
        self.creds = creds
        self.mode = "ok"
        self.requests: list[tuple[str, str, dict[str, str]]] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self, status: int, body: str) -> None:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_PUT(self) -> None:  # noqa: N802 - the stdlib's name
                server.requests.append(("PUT", self.path, dict(self.headers)))
                if server.mode == "refuse_token":
                    self._answer(403, "")
                elif self.path.endswith("/latest/api/token"):
                    self._answer(200, TOKEN)
                else:
                    self._answer(404, "")

            def do_GET(self) -> None:  # noqa: N802 - the stdlib's name
                server.requests.append(("GET", self.path, dict(self.headers)))
                if self.headers.get("x-aws-ec2-metadata-token") != TOKEN:
                    self._answer(401, "")
                elif self.path.endswith(ROLE_LISTING):
                    first = _role_listings(server) == 1
                    if server.mode == "no_role":
                        self._answer(404, "")
                    elif server.mode == "role_500" or (server.mode == "role_500_once" and first):
                        self._answer(500, "")
                    else:
                        self._answer(200, ROLE_NAME)
                elif self.path.endswith(f"{ROLE_LISTING}{ROLE_NAME}"):
                    expires = datetime.now(UTC) + timedelta(hours=6)
                    body = {
                        "Code": "Success",
                        "Type": "AWS-HMAC",
                        **server.creds,
                        "Expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    if server.mode == "bad_expiration":
                        body["Expiration"] = "not a time"
                    elif server.mode == "no_key_id":
                        body["AccessKeyId"] = None
                    self._answer(200, "null" if server.mode == "null_body" else json.dumps(body))
                else:
                    self._answer(404, "")

            def log_message(self, *args) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/"

    def __enter__(self) -> _Server:
        threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _role_listings(server: _Server) -> int:
    """How many role-listing GETs have reached the server so far."""
    return sum(
        1 for method, path, _ in server.requests if method == "GET" and path.endswith(ROLE_LISTING)
    )


@pytest.fixture
def metadata(monkeypatch) -> Iterator[_Server]:
    """The metadata service, with the client's base URL pointed at it."""
    with _Server(METADATA) as server:
        monkeypatch.setattr(aws_session, "METADATA_BASE_URL", server.url)
        yield server


@pytest.fixture
def proxy() -> Iterator[_Server]:
    """A proxy that would answer the metadata lookup with credentials of its own."""
    with _Server(PROXY) as server:
        yield server


@pytest.fixture
def hostile(tmp_path, monkeypatch, metadata, proxy) -> dict[str, str]:
    """Every other place a credential could come from, pointed somewhere else.

    Three traps make a proxy test pass with no fix behind it, so the fixture closes each.

    1. A ``no_proxy`` covering ``127.0.0.1`` hides the proxy whatever the build does, so
       both spellings are deleted.
    2. A proxy set only as lowercase ``http_proxy`` passes a match that removes only
       lowercase names, so the proxy is set as ``HTTP_PROXY`` alone, and a lowercase
       one from the developer's shell is deleted.
    3. ``AWS_EC2_METADATA_DISABLED`` is set, so a fetcher that read the environment
       would turn itself off.
    """
    boto_config = tmp_path / "boto.cfg"
    boto_config.write_text(
        "[Credentials]\naws_access_key_id = AKIDFROMBOTOCONFIG\n"
        "aws_secret_access_key = from-boto-config\n"
    )
    home = tmp_path / "home"
    home.mkdir()
    (home / ".boto").write_text(
        "[Credentials]\naws_access_key_id = AKIDFROMHOMEBOTO\naws_secret_access_key = from-home\n"
    )
    for key in ("no_proxy", "NO_PROXY", "http_proxy"):
        monkeypatch.delenv(key, raising=False)
    env = {
        "HOME": str(home),
        "BOTO_CONFIG": str(boto_config),
        "HTTP_PROXY": proxy.url,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": proxy.url,
        "AWS_ACCESS_KEY_ID": "AKIDFROMENV",
        "AWS_SECRET_ACCESS_KEY": "from-env",
        "AWS_SESSION_TOKEN": "token-from-env",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    # The control: under this environment the proxy applies to the metadata server's
    # address, so a build that kept it would send the lookup through it.
    assert botocore.utils.get_environ_proxies(metadata.url) == {"http": proxy.url}
    return env


def _config(**overrides) -> Config:
    values = {
        "lake_root": "/data/lake",
        "backup_target": "s3://lake-backup/lake",
        "healthchecks_ping_key": "ping",
        "ntfy_topic": "topic",
        "schwab_api_key": "api",
        "schwab_app_secret": "app",
        "bucket_credentials": "instance_profile",
        "bucket_region": REGION,
        **overrides,
    }
    return Config.from_mapping(values)


def _sent(client) -> list:
    """The requests the client would send to S3, answered here instead."""
    sent = []

    class Raw:
        def stream(self, **kwargs):
            yield b""

    def answer(request, **kwargs):
        sent.append(request)
        return AWSResponse(request.url, 200, {"Content-Length": "0"}, Raw())

    client.meta.events.register("before-send.s3", answer)
    return sent


def _head(client) -> object:
    sent = _sent(client)
    client.head_object(Bucket="lake-backup", Key="lake/manifest.jsonl")
    assert len(sent) == 1
    return sent[0]


# -- 1. the profile path ---------------------------------------------------------


def test_the_profile_path_signs_with_the_metadata_credentials(metadata, proxy, hostile):
    client = client_from_config(_config())
    request = _head(client)

    authorization = request.headers["Authorization"].decode()
    assert f"Credential={METADATA['AccessKeyId']}/" in authorization
    assert f"/{REGION}/s3/" in authorization
    assert request.headers["X-Amz-Security-Token"].decode() == METADATA["Token"]
    assert request.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")
    # IMDSv2 only: a token first, and every read carrying it.
    methods = [method for method, _, _ in metadata.requests]
    assert methods[0] == "PUT"
    assert all(
        headers.get("x-aws-ec2-metadata-token") == TOKEN
        for method, _, headers in metadata.requests
        if method == "GET"
    )
    # The proxy saw nothing, so its credentials never had a chance to be used.
    assert proxy.requests == []


def test_the_profile_path_ignores_an_endpoint_and_a_region_in_the_environment(
    metadata, hostile, monkeypatch
):
    # The profile path builds inside the same cleanup as the key path, so an endpoint
    # variable cannot send the metadata credentials to another host.
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://from-env.invalid")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://from-env-s3.invalid")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    request = _head(client_from_config(_config()))

    assert request.url.startswith(f"https://lake-backup.s3.{REGION}.amazonaws.com/")
    assert f"/{REGION}/s3/" in request.headers["Authorization"].decode()


def test_no_service_model_loads_from_the_home_directory_on_the_profile_path(metadata, hostile):
    client = client_from_config(_config())
    assert not any(".aws" in path for path in client._loader.search_paths)


def test_one_failed_role_listing_is_asked_again(metadata, hostile):
    metadata.mode = "role_500_once"
    request = _head(client_from_config(_config()))
    assert f"Credential={METADATA['AccessKeyId']}/" in request.headers["Authorization"].decode()
    assert _role_listings(metadata) == 2


def test_a_role_listing_that_always_fails_is_asked_twice_and_no_more(metadata, hostile):
    metadata.mode = "role_500"
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config())
    _assert_both_fixes(str(refused.value), "none returned")
    # A literal rather than ``aws_session.METADATA_ATTEMPTS``, so a changed constant cannot
    # move the expectation with it.
    assert _role_listings(metadata) == 2


def test_a_service_that_never_answers_refuses_within_the_timeout(monkeypatch):
    # The socket listens, so a connection opens, and nothing ever answers on it. The
    # token request times out and the build refuses in about one second, while a
    # five-second timeout would take at least five.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        port = listener.getsockname()[1]
        monkeypatch.setattr(aws_session, "METADATA_BASE_URL", f"http://127.0.0.1:{port}/")
        started = time.monotonic()
        with pytest.raises(ConfigError) as refused:
            client_from_config(_config())
        elapsed = time.monotonic() - started
    _assert_both_fixes(str(refused.value), "MetadataRetrievalError")
    assert elapsed < 3


def test_the_build_leaves_the_environment_as_it_found_it(metadata, hostile):
    client_from_config(_config())
    for key, value in hostile.items():
        assert os.environ[key] == value


def test_the_profile_path_fetches_the_credentials_once(metadata, hostile):
    # The build fetches the credentials before the client exists, and the client signs
    # with the ones the session kept rather than asking again.
    client = client_from_config(_config())
    before = len(metadata.requests)
    _head(client)
    assert len(metadata.requests) == before
    assert [method for method, _, _ in metadata.requests].count("PUT") == 1


# -- 2. the key path -------------------------------------------------------------


def test_the_key_path_signs_with_the_config_keys_and_never_asks_the_service(
    metadata, proxy, hostile
):
    config = _config(
        bucket_credentials="keys",
        bucket_access_key_id="AKIDFROMCONFIG",
        bucket_secret_access_key="secret-from-config",
    )
    request = _head(client_from_config(config))

    assert "Credential=AKIDFROMCONFIG/" in request.headers["Authorization"].decode()
    assert "X-Amz-Security-Token" not in request.headers
    assert metadata.requests == [] and proxy.requests == []


# -- 3. a failed lookup ----------------------------------------------------------

LOOKUP_FAILURES = [
    pytest.param("refuse_token", "MetadataRetrievalError", id="token-refused"),
    pytest.param("no_role", "none returned", id="no-instance-profile"),
    # Only a broken or impersonated service answers these ways. botocore raises a plain
    # TypeError or ValueError for them, which must still be one line naming both fixes.
    pytest.param("null_body", "TypeError", id="null-body"),
    pytest.param("bad_expiration", "ParserError", id="bad-expiration"),
    pytest.param("no_key_id", "incomplete credentials", id="no-key-id"),
]


def _assert_both_fixes(line: str, detail: str) -> None:
    assert f"({detail})" in line
    assert "Attach the instance profile" in line
    assert "set bucket_credentials: keys in config.yaml" in line
    assert "\n" not in line


@pytest.mark.parametrize(("mode", "detail"), LOOKUP_FAILURES)
def test_a_failed_lookup_refuses_at_build_naming_both_fixes(metadata, hostile, mode, detail):
    metadata.mode = mode
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config())
    _assert_both_fixes(str(refused.value), detail)


def test_the_lookup_failure_line_in_full(metadata, hostile):
    metadata.mode = "no_role"
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config())
    assert str(refused.value) == (
        "bucket_credentials is instance_profile and no credentials came from the instance "
        "metadata service (none returned). Attach the instance profile, or set "
        "bucket_credentials: keys in config.yaml"
    )


def test_an_unreachable_service_refuses_the_same_way(monkeypatch):
    # A port with nothing listening, the way a laptop answers the metadata address.
    with _Server(METADATA) as gone:
        url = gone.url
    monkeypatch.setattr(aws_session, "METADATA_BASE_URL", url)
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config())
    _assert_both_fixes(str(refused.value), "MetadataRetrievalError")


@pytest.mark.parametrize(("mode", "detail"), LOOKUP_FAILURES)
def test_a_failed_lookup_reaches_connect_as_one_settings_line(metadata, mode, detail):
    metadata.mode = mode
    with pytest.raises(BucketSettingsInvalid) as refused:
        connect(_config())
    _assert_both_fixes(str(refused.value), detail)


@pytest.mark.parametrize(("mode", "detail"), LOOKUP_FAILURES)
def test_a_failed_lookup_is_a_sunday_finding_and_the_job_runs_on(
    tmp_path, capsys, monkeypatch, metadata, mode, detail
):
    # A raise that escaped ``connect`` would stop Sunday's job before the canary.
    metadata.mode = mode
    lake, _ = _uploaded(tmp_path / "lake")
    keys = f"bucket_credentials: instance_profile\nbucket_region: {REGION}\n"

    code, pinger, canaries = _sunday_cli(tmp_path, monkeypatch, lake, keys=keys)

    assert code == 1
    assert pinger.urls == []
    assert canaries
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    finding = [line for line in captured.out.splitlines() if "cannot be used" in line]
    assert finding
    for line in finding:
        assert "in config.yaml cannot be used" not in line
        _assert_both_fixes(line, detail)


# -- 4. any other build error ----------------------------------------------------


def test_another_build_error_on_the_profile_path_keeps_the_config_line(metadata):
    # The credentials arrive, and botocore then refuses the region. Telling the
    # operator to attach an instance profile for that would be wrong.
    with pytest.raises(ConfigError) as refused:
        client_from_config(_config(bucket_region="us east 2"))
    line = str(refused.value)
    assert line == "the bucket client could not be built from config.yaml (InvalidRegionError)"
