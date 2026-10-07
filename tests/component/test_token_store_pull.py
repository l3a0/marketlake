"""``python -m lake.token_store pull`` across its real boundaries, marketlake #636.

The real ``main`` loads a throwaway ``config.yaml``, builds its own SSM client, and writes
a token file. On the VM the client signs with the instance profile, so these tests run a
metadata service on loopback, the one ``tests/component/test_bucket_instance_profile.py``
uses, reached through ``aws_session.METADATA_BASE_URL``. ``main`` builds the client
through ``token_store.pull_client``, which a test wraps to answer ``GetParameter`` with a
``before-send`` hook, so no request reaches AWS.

The exit codes are what #686's first-boot retry reads: 0 for ``wrote`` and ``current``,
1 for ``store older`` and ``unreadable``, 2 for a config problem, and 3 for no
credentials yet, the one worth retrying.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from botocore.awsrequest import AWSResponse

from lake import aws_session, token_store
from tests.component.test_bucket_instance_profile import METADATA, _Server
from tests.support.config import write_config

ACCESS = "ACCESS-TOKEN-SENTINEL-90cc"
REFRESH = "REFRESH-TOKEN-SENTINEL-17ab"
REGION = "us-east-2"
TOKEN = {
    "creation_timestamp": 1787529900,
    "token": {"access_token": ACCESS, "refresh_token": REFRESH, "expires_in": 1800},
}


class _Raw:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def stream(self, **kwargs):
        yield self._body


class StoreHook:
    """Wraps ``token_store.pull_client`` and answers ``GetParameter`` with ``value``."""

    def __init__(self, value: str | None) -> None:
        parameter = {"Name": token_store.PARAMETER_NAME, "Type": "SecureString", "Version": 4}
        if value is not None:
            parameter["Value"] = value
        self.body = {"Parameter": parameter}
        self.requests: list = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> StoreHook:
        real = token_store.pull_client

        def build(config):
            client = real(config)
            client.meta.events.register("before-send.ssm", self._answer)
            return client

        monkeypatch.setattr(token_store, "pull_client", build)
        return self

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        data = json.dumps(self.body).encode()
        return AWSResponse(request.url, 200, {"Content-Length": str(len(data))}, _Raw(data))


@pytest.fixture
def metadata(monkeypatch) -> Iterator[_Server]:
    with _Server(METADATA) as server:
        monkeypatch.setattr(aws_session, "METADATA_BASE_URL", server.url)
        yield server


def _vm_config(tmp_path, lake_root, **overrides):
    """The VM's config: the instance profile, the bucket's region, and ``store``."""
    values = {
        "bucket_credentials": "instance_profile",
        "bucket_region": REGION,
        "token_store": "store",
        **overrides,
    }
    return write_config(tmp_path, lake_root, **values)


def _pull(config, token) -> int:
    try:
        return token_store.main(["pull", "--config", str(config), "--token", str(token)])
    except SystemExit as exc:
        return exc.code


def _one_line(capsys) -> str:
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1, lines
    assert ACCESS not in captured.err and REFRESH not in captured.err
    assert "Traceback" not in captured.err
    return lines[0]


def test_a_first_boot_pull_writes_the_token_signed_by_the_instance_profile(
    tmp_path, lake_root, monkeypatch, capsys, metadata
):
    hook = StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root), token) == 0

    assert json.loads(token.read_text()) == TOKEN
    assert token.stat().st_mode & 0o777 == 0o600
    authorization = hook.requests[0].headers["Authorization"].decode()
    assert f"Credential={METADATA['AccessKeyId']}/" in authorization
    assert hook.requests[0].headers["X-Amz-Security-Token"].decode() == METADATA["Token"]
    assert _one_line(capsys).startswith(f"token_store: wrote {token}")


def test_a_pull_under_role_shadow_succeeds(tmp_path, lake_root, monkeypatch, capsys, metadata):
    # The VM's first boot runs as shadow, so a client gated on a primary would fail here.
    StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root, role="shadow"), token) == 0

    assert json.loads(token.read_text()) == TOKEN
    _one_line(capsys)


def test_a_second_pull_is_current_and_exits_zero(
    tmp_path, lake_root, monkeypatch, capsys, metadata
):
    StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"
    config = _vm_config(tmp_path, lake_root)
    _pull(config, token)
    capsys.readouterr()

    assert _pull(config, token) == 0
    assert "is current" in _one_line(capsys)


def test_an_older_parameter_exits_one(tmp_path, lake_root, monkeypatch, capsys, metadata):
    StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"
    later = {**TOKEN, "creation_timestamp": TOKEN["creation_timestamp"] + 60}
    token.write_text(json.dumps(later))

    assert _pull(_vm_config(tmp_path, lake_root), token) == 1
    assert json.loads(token.read_text()) == later
    assert "nothing was written" in _one_line(capsys)


@pytest.mark.parametrize("value", ["", None], ids=["empty", "absent"])
def test_an_empty_parameter_exits_one(tmp_path, lake_root, monkeypatch, capsys, metadata, value):
    StoreHook(value).install(monkeypatch)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root), token) == 1
    assert not token.exists()
    assert "could not be used" in _one_line(capsys)


def test_no_instance_profile_exits_three(tmp_path, lake_root, monkeypatch, capsys, metadata):
    # The metadata service answers and has no instance profile attached, as on a first
    # boot before the profile is serving credentials.
    metadata.mode = "no_role"
    hook = StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root), token) == 3
    assert hook.requests == []
    assert not token.exists()
    assert "(none returned)" in _one_line(capsys)


def test_a_missing_bucket_region_exits_two(tmp_path, lake_root, monkeypatch, capsys, metadata):
    hook = StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    config = write_config(
        tmp_path, lake_root, bucket_credentials="instance_profile", token_store="store"
    )

    assert _pull(config, tmp_path / "token.json") == 2
    assert hook.requests == [] and metadata.requests == []
    line = _one_line(capsys)
    assert line.startswith("token_store: ")
    assert "bucket_region" in line


def test_keys_on_an_instance_profile_host_exit_two(tmp_path, lake_root, capsys, metadata):
    config = _vm_config(
        tmp_path, lake_root, bucket_access_key_id="AKIDSTRAY", bucket_secret_access_key="stray"
    )

    assert _pull(config, tmp_path / "token.json") == 2
    assert "must not hold" in _one_line(capsys)


def test_an_unrecognised_credential_source_exits_two(tmp_path, lake_root, capsys, metadata):
    config = _vm_config(tmp_path, lake_root, bucket_credentials="instance-profile")

    assert _pull(config, tmp_path / "token.json") == 2
    assert "bucket_credentials must be keys, instance_profile or assume_role" in _one_line(capsys)


def test_a_write_that_fails_exits_one_with_one_line(
    tmp_path, lake_root, monkeypatch, capsys, metadata
):
    from lake import reauth

    StoreHook(json.dumps(TOKEN)).install(monkeypatch)

    def refuse(path, payload):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(reauth, "write_token", refuse)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root), token) == 1
    assert _one_line(capsys) == f"token_store: {token} could not be written (PermissionError)"
    assert not token.exists()


def test_the_pull_does_not_read_token_store(tmp_path, lake_root, monkeypatch, capsys, metadata):
    # Running it is the decision. The gate belongs to the scheduled calls #702 adds.
    StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    token = tmp_path / "token.json"

    assert _pull(_vm_config(tmp_path, lake_root, token_store="file"), token) == 0
    assert token.exists()


def test_the_default_token_path_resolves_when_the_command_runs(
    tmp_path, lake_root, monkeypatch, capsys, metadata
):
    # Set after ``lake`` was imported, so a default bound at import would miss it.
    StoreHook(json.dumps(TOKEN)).install(monkeypatch)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.setenv("MARKETLAKE_CONFIG_DIR", str(elsewhere))
    config = _vm_config(tmp_path, lake_root)

    assert token_store.main(["pull", "--config", str(config)]) == 0
    assert json.loads((elsewhere / "token.json").read_text()) == TOKEN
