"""``python -m lake.reauth`` putting the token parameter, marketlake #636.

The real ``main`` runs here, with a fake ``schwab.auth`` in ``sys.modules`` as in
``test_reauth_command.py``. ``main`` builds the token parameter's client itself, through
``token_store.push_client``. These tests replace that producer with one that builds the
real client and then answers it through a ``before-send`` hook, so botocore signs and
serialises exactly as it would for AWS and no socket opens.

The fake login hands over a token with distinct access and refresh tokens, and no line
on either stream may contain either one, whatever the put does.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from botocore.awsrequest import AWSResponse

from lake import reauth, token_store
from tests.component.test_reauth_command import (
    CALLBACK,
    OLD_TOKEN,
    FakeLoginFlow,
    _at_a_terminal,
    _install_seam,
)
from tests.support.config import write_config

ACCESS = "ACCESS-TOKEN-SENTINEL-5b2d"
REFRESH = "REFRESH-TOKEN-SENTINEL-e04f"
STORE_TOKEN = {
    "creation_timestamp": 1787529900,
    "token": {"access_token": ACCESS, "refresh_token": REFRESH, "expires_in": 1800},
}
PUT_KEY_ID = "AKIDTOKENSTOREPUT"
BUCKET_KEY_ID = "AKIDBUCKETBACKUP"
REGION = "us-east-2"
PUT_KEYS = {
    "token_store_access_key_id": PUT_KEY_ID,
    "token_store_secret_access_key": "token-store-put-secret",
    "token_store_region": REGION,
}
BUCKET_KEYS = {
    "bucket_access_key_id": BUCKET_KEY_ID,
    "bucket_secret_access_key": "bucket-backup-secret",
    "bucket_region": REGION,
}


class _Raw:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def stream(self, **kwargs):
        yield self._body


class StoreHook:
    """Stands in for ``token_store.push_client`` and answers every request it builds."""

    def __init__(self, status: int = 200, body: dict | None = None) -> None:
        self.status = status
        self.body = {"Version": 12, "Tier": "Standard"} if body is None else body
        self.builds = 0
        self.requests: list = []
        self.before_parameter_build = None

    def install(self, monkeypatch: pytest.MonkeyPatch) -> StoreHook:
        real = token_store.push_client

        def build(config):
            self.builds += 1
            client = real(config)
            client.meta.events.register("before-send.ssm", self._answer)
            if self.before_parameter_build is not None:
                client.meta.events.register(
                    "before-parameter-build.ssm.PutParameter", self.before_parameter_build
                )
            return client

        monkeypatch.setattr(token_store, "push_client", build)
        return self

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        data = json.dumps(self.body).encode()
        return AWSResponse(request.url, self.status, {"Content-Length": str(len(data))}, _Raw(data))


def _config(tmp_path, lake_root, *, token_store=None, **keys) -> Path:
    return write_config(tmp_path, lake_root, callback_url=CALLBACK, token_store=token_store, **keys)


def _run(tmp_path, config, monkeypatch, token=STORE_TOKEN):
    flow = FakeLoginFlow(token=token)
    _install_seam(monkeypatch, flow)
    _at_a_terminal(monkeypatch)
    path = tmp_path / "token.json"
    try:
        code = reauth.main(["--config", str(config), "--token", str(path)])
    except SystemExit as exc:
        code = exc.code
    return code, flow, path


def _assert_no_token_bytes(captured) -> None:
    for stream in (captured.out, captured.err):
        assert ACCESS not in stream
        assert REFRESH not in stream
        assert "Traceback" not in stream


# -- the setting -----------------------------------------------------------------------


def test_file_mode_with_complete_keys_sends_nothing(tmp_path, lake_root, monkeypatch, capsys):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="file", **PUT_KEYS)

    code, _, token = _run(tmp_path, config, monkeypatch)

    assert code == 0
    assert hook.builds == 0 and hook.requests == []
    assert json.loads(token.read_text()) == STORE_TOKEN
    captured = capsys.readouterr()
    assert "parameter" not in captured.out
    assert captured.err == ""


def test_no_token_store_key_and_no_put_keys_exits_zero_and_sends_nothing(
    tmp_path, lake_root, monkeypatch, capsys
):
    # Today's laptop. No client can be built from this config, so a ``main`` that built one
    # before it knew the mode would fail here.
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root)

    code, flow, _ = _run(tmp_path, config, monkeypatch)

    assert code == 0
    assert flow.calls
    assert hook.builds == 0 and hook.requests == []
    assert capsys.readouterr().err == ""


def test_an_unknown_value_with_missing_keys_logs_in_writes_the_file_and_exits_three(
    tmp_path, lake_root, monkeypatch, capsys
):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="Both")

    code, flow, token = _run(tmp_path, config, monkeypatch)

    assert code == 3
    assert flow.calls
    assert json.loads(token.read_text()) == STORE_TOKEN
    assert hook.builds == 0 and hook.requests == []
    captured = capsys.readouterr()
    lines = captured.err.strip().splitlines()
    # One line naming the value read, then one naming the config problem as the fix.
    assert len(lines) == 2
    assert "token_store 'Both'" in lines[0]
    assert str(token) in lines[1]
    assert "token_store_access_key_id" in lines[1]
    assert "Fix config.yaml" in lines[1]
    _assert_no_token_bytes(captured)


def test_an_unknown_value_with_complete_keys_puts(tmp_path, lake_root, monkeypatch, capsys):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="stor", **PUT_KEYS)

    code, _, _ = _run(tmp_path, config, monkeypatch)

    assert code == 0
    assert len(hook.requests) == 1
    captured = capsys.readouterr()
    assert "version 12" in captured.out
    assert "token_store 'stor'" in captured.err


# -- the refusal before the browser ----------------------------------------------------


@pytest.mark.parametrize("mode", ["both", "store"])
@pytest.mark.parametrize("missing", sorted(PUT_KEYS))
def test_a_missing_put_key_refuses_before_the_browser(
    tmp_path, lake_root, monkeypatch, capsys, mode, missing
):
    # Complete ``bucket_*`` keys sit beside them, so a fallback to the backup's key would
    # build a client and send a request.
    hook = StoreHook().install(monkeypatch)
    keys = {key: value for key, value in PUT_KEYS.items() if key != missing}
    config = _config(tmp_path, lake_root, token_store=mode, **keys, **BUCKET_KEYS)

    code, flow, token = _run(tmp_path, config, monkeypatch)

    assert code == 2
    assert flow.calls == []
    assert not token.exists()
    assert hook.builds == 0 and hook.requests == []
    err = capsys.readouterr().err
    assert err.startswith("reauth: ")
    assert missing in err
    assert len(err.strip().splitlines()) == 1


@pytest.mark.parametrize("mode", ["both", "store"])
def test_only_the_bucket_keys_refuses_before_the_browser(
    tmp_path, lake_root, monkeypatch, capsys, mode
):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store=mode, **BUCKET_KEYS)

    code, flow, _ = _run(tmp_path, config, monkeypatch)

    assert code == 2
    assert flow.calls == []
    assert hook.requests == []
    err = capsys.readouterr().err
    for key in PUT_KEYS:
        assert key in err


def test_a_malformed_put_region_refuses_before_the_browser(
    tmp_path, lake_root, monkeypatch, capsys
):
    StoreHook().install(monkeypatch)
    keys = {**PUT_KEYS, "token_store_region": "useast2"}
    config = _config(tmp_path, lake_root, token_store="both", **keys)

    code, flow, _ = _run(tmp_path, config, monkeypatch)

    assert code == 2
    assert flow.calls == []
    assert "token_store_region 'useast2' is not an AWS region name" in capsys.readouterr().err


# -- the put ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["both", "store"])
def test_a_put_that_succeeds_prints_the_version(tmp_path, lake_root, monkeypatch, capsys, mode):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store=mode, **PUT_KEYS)

    code, _, token = _run(tmp_path, config, monkeypatch)

    assert code == 0
    assert json.loads(token.read_text()) == STORE_TOKEN
    assert len(hook.requests) == 1
    captured = capsys.readouterr()
    assert "/marketlake/config/schwab-oauth-token version 12" in captured.out
    assert captured.err == ""
    _assert_no_token_bytes(captured)


def test_the_put_sends_what_the_login_wrote_as_a_secure_standard_overwrite(
    tmp_path, lake_root, monkeypatch
):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)

    _run(tmp_path, config, monkeypatch)

    request = hook.requests[0]
    assert request.headers["X-Amz-Target"] == b"AmazonSSM.PutParameter"
    sent = json.loads(request.body)
    assert sent["Name"] == "/marketlake/config/schwab-oauth-token"
    assert sent["Type"] == "SecureString"
    assert sent["Tier"] == "Standard"
    assert sent["Overwrite"] is True
    assert "KeyId" not in sent and "Tags" not in sent
    assert json.loads(sent["Value"]) == STORE_TOKEN


def test_the_put_sends_the_written_text_not_a_read_back(tmp_path, lake_root, monkeypatch):
    # A refresh that read the old file can land between the write and a read-back. The
    # stand-in is a file rewritten the moment the write returns: a put that read the file
    # would send what is on disk, and the rule sends what was written.
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)
    real = reauth.write_token

    def write_then_clobber(path, payload):
        text = real(path, payload)
        Path(path).write_text(json.dumps(OLD_TOKEN))
        return text

    monkeypatch.setattr(reauth, "write_token", write_then_clobber)
    _run(tmp_path, config, monkeypatch)

    assert json.loads(json.loads(hook.requests[0].body)["Value"]) == STORE_TOKEN


def test_with_both_key_sets_the_put_signs_with_the_token_store_key(
    tmp_path, lake_root, monkeypatch
):
    # The two regions differ, so a put that read ``bucket_region`` signs for the wrong one.
    hook = StoreHook().install(monkeypatch)
    bucket = {**BUCKET_KEYS, "bucket_region": "us-west-1"}
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS, **bucket)

    _run(tmp_path, config, monkeypatch)

    request = hook.requests[0]
    authorization = request.headers["Authorization"].decode()
    credential = authorization.split("Credential=", 1)[1].split("/", 1)[0]
    assert credential == PUT_KEY_ID
    assert BUCKET_KEY_ID not in authorization
    assert "/us-east-2/ssm/" in authorization
    assert request.url == "https://ssm.us-east-2.amazonaws.com/"


def test_a_login_that_writes_nothing_over_a_live_token_puts_nothing(
    tmp_path, lake_root, monkeypatch, capsys
):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)
    (tmp_path / "token.json").write_text(json.dumps(OLD_TOKEN))

    code, _, token = _run(tmp_path, config, monkeypatch, token=None)

    assert code == 1
    assert hook.builds == 0 and hook.requests == []
    assert json.loads(token.read_text()) == OLD_TOKEN
    assert "version" not in capsys.readouterr().out


def test_a_token_over_4096_bytes_is_refused_before_any_request(
    tmp_path, lake_root, monkeypatch, capsys
):
    hook = StoreHook().install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)
    big = {**STORE_TOKEN, "padding": "x" * 4096}

    code, _, token = _run(tmp_path, config, monkeypatch, token=big)

    assert code == 3
    assert json.loads(token.read_text()) == big
    assert hook.requests == []
    assert "(TooLarge)" in capsys.readouterr().err


# -- a put that fails ------------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    [
        "AccessDenied",
        "AccessDeniedException",
        "UnrecognizedClientException",
        "InvalidSignatureException",
    ],
)
def test_a_refused_put_leaves_the_file_and_names_the_token_store_keys(
    tmp_path, lake_root, monkeypatch, capsys, code
):
    body = {"__type": code, "message": f"no {REFRESH}"}
    StoreHook(status=400, body=body).install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)

    exit_code, _, token = _run(tmp_path, config, monkeypatch)

    assert exit_code == 3
    assert json.loads(token.read_text()) == STORE_TOKEN
    captured = capsys.readouterr()
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1
    assert str(token) in lines[0]
    assert f"({code})" in lines[0]
    assert "token_store_access_key_id and token_store_secret_access_key" in lines[0]
    assert "Run reauth.sh again" not in lines[0]
    _assert_no_token_bytes(captured)


def test_another_error_echoing_the_token_says_run_again_and_prints_no_token(
    tmp_path, lake_root, monkeypatch, capsys
):
    message = f"Value {json.dumps(STORE_TOKEN)} failed to satisfy constraint"
    body = {"__type": "ValidationException", "message": message}
    StoreHook(status=400, body=body).install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="store", **PUT_KEYS)

    code, _, token = _run(tmp_path, config, monkeypatch)

    assert code == 3
    assert token.exists()
    captured = capsys.readouterr()
    assert captured.err.strip().endswith("(ValidationException). Run reauth.sh again")
    _assert_no_token_bytes(captured)


def test_a_region_botocore_refuses_after_the_login_exits_three(
    tmp_path, lake_root, monkeypatch, capsys
):
    # The name has the shape ``is_region_name`` accepts, so no refusal comes before the
    # browser. botocore refuses a region label over 63 characters when the client is built.
    hook = StoreHook().install(monkeypatch)
    keys = {**PUT_KEYS, "token_store_region": "us-" + "a" * 70 + "-1"}
    config = _config(tmp_path, lake_root, token_store="both", **keys)

    code, flow, token = _run(tmp_path, config, monkeypatch)

    assert code == 3
    assert flow.calls
    assert json.loads(token.read_text()) == STORE_TOKEN
    assert hook.requests == []
    captured = capsys.readouterr()
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1
    assert str(token) in lines[0]
    assert "(InvalidRegionError)" in lines[0]
    _assert_no_token_bytes(captured)


def test_a_client_side_validation_error_prints_its_type_and_no_token(
    tmp_path, lake_root, monkeypatch, capsys
):
    hook = StoreHook()

    def to_bytes(params, **kwargs):
        params["Value"] = params["Value"].encode()

    hook.before_parameter_build = to_bytes
    hook.install(monkeypatch)
    config = _config(tmp_path, lake_root, token_store="both", **PUT_KEYS)

    code, _, _ = _run(tmp_path, config, monkeypatch)

    assert code == 3
    assert hook.requests == []
    captured = capsys.readouterr()
    assert "(ParamValidationError)" in captured.err
    _assert_no_token_bytes(captured)
