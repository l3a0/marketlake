"""``python -m lake.vm_config render`` across its real boundaries, marketlake #686.

The real ``main`` reads the settings on standard input, builds its own SSM client signed
by the instance profile, and writes ``config.yaml`` into the config directory. The
metadata service runs on loopback, the one ``tests/component/test_bucket_instance_profile.py``
uses, reached through ``aws_session.METADATA_BASE_URL``. ``main`` builds the client
through ``vm_config.render_client``, which a test wraps to answer ``GetParameters`` with a
``before-send`` hook, so no request reaches AWS.

Every value the hook serves is a sentinel, and every test that reads the output checks
that none of them reached it. The exit codes are what the first boot's retry reads: 0
written or unchanged, 1 any other failure, 2 a refusal, and 3 no credentials yet.
"""

from __future__ import annotations

import io
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from botocore.awsrequest import AWSResponse

from lake import aws_session, vm_config
from lake.config import load_config
from lake.paths import CONFIG_DIR_ENV, CONFIG_FILE, temp_write_path
from tests.component.test_bucket_instance_profile import METADATA, _Server

API_KEY = "API-KEY-SENTINEL-4a1f"
APP_SECRET = "APP-SECRET-SENTINEL-77c2"
PING_KEY = "PING-KEY-SENTINEL-0b9e"
TOPIC = "NTFY-TOPIC-SENTINEL-c3d4"
BUCKET = "sentinel-bucket-5e6f"
TARGET = f"s3://{BUCKET}/lake"
ERROR_MESSAGE = "ERROR-MESSAGE-SENTINEL-91aa"
SENTINELS = (API_KEY, APP_SECRET, PING_KEY, TOPIC, BUCKET, ERROR_MESSAGE)

# The five names and the values the hook serves for them. Written out rather than read
# from ``vm_config.PARAMETERS``, so a renamed parameter fails here.
VALUES = {
    "/marketlake/config/schwab-api-key": API_KEY,
    "/marketlake/config/schwab-app-secret": APP_SECRET,
    "/marketlake/config/healthchecks-ping-key": PING_KEY,
    "/marketlake/config/ntfy-topic": TOPIC,
    "/marketlake/config/backup-target": TARGET,
}

SETTINGS = {
    "role": "shadow",
    "lake_root": "/srv/marketlake",
    "token_store": "store",
    "bucket_credentials": "instance_profile",
    "bucket_region": "us-east-1",
}

# What the render writes for ``SETTINGS`` and ``VALUES``, spelled out.
EXPECTED = {
    **SETTINGS,
    "schwab_api_key": API_KEY,
    "schwab_app_secret": APP_SECRET,
    "healthchecks_ping_key": PING_KEY,
    "ntfy_topic": TOPIC,
    "backup_target": TARGET,
}


class _Raw:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def stream(self, **kwargs):
        yield self._body


class SsmHook:
    """Wraps ``vm_config.render_client`` and answers ``GetParameters``.

    It serves ``values`` in the reverse of the order asked for, so a render that matched
    by position rather than by ``Name`` writes the wrong key. A name in ``invalid`` is
    listed under ``InvalidParameters`` instead. ``error`` answers every request with that
    AWS error code, and ``raises`` raises that exception from the send.
    """

    def __init__(
        self,
        values: dict[str, str] | None = None,
        *,
        invalid: tuple[str, ...] = (),
        error: str | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.values = dict(VALUES if values is None else values)
        self.invalid = invalid
        self.error = error
        self.raises = raises
        self.requests: list = []
        self.regions: list[str] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> SsmHook:
        real = vm_config.render_client

        def build(region):
            self.regions.append(region)
            client = real(region)
            client.meta.events.register("before-send.ssm", self._answer)
            return client

        monkeypatch.setattr(vm_config, "render_client", build)
        return self

    def _answer(self, request, **kwargs):
        self.requests.append(request)
        if self.raises is not None:
            raise self.raises
        if self.error is not None:
            data = json.dumps({"__type": self.error, "message": ERROR_MESSAGE}).encode()
            return AWSResponse(request.url, 400, {"Content-Length": str(len(data))}, _Raw(data))
        names = json.loads(request.body)["Names"]
        parameters = [
            {"Name": name, "Type": "SecureString", "Value": self.values[name], "Version": 1}
            for name in reversed(names)
            if name not in self.invalid and name in self.values
        ]
        body = {"Parameters": parameters, "InvalidParameters": list(self.invalid)}
        data = json.dumps(body).encode()
        return AWSResponse(request.url, 200, {"Content-Length": str(len(data))}, _Raw(data))

    def body(self) -> dict:
        return json.loads(self.requests[0].body)


@pytest.fixture
def metadata(monkeypatch) -> Iterator[_Server]:
    with _Server(METADATA) as server:
        monkeypatch.setattr(aws_session, "METADATA_BASE_URL", server.url)
        yield server


@pytest.fixture
def config_dir(tmp_path, monkeypatch) -> Path:
    """A config directory of this test's own, set after ``lake`` was imported."""
    directory = tmp_path / "home" / ".config" / "marketlake"
    monkeypatch.setenv(CONFIG_DIR_ENV, str(directory))
    return directory


@pytest.fixture
def not_root(monkeypatch):
    # A developer running the suite as root would see every render refuse.
    monkeypatch.setattr(os, "geteuid", lambda: 1000)


def _settings_bytes(**changes) -> bytes:
    settings = {**SETTINGS, **changes}
    return yaml.safe_dump({k: v for k, v in settings.items() if v is not _DROP}).encode()


_DROP = object()


class _Stdin:
    def __init__(self, payload: bytes) -> None:
        self.buffer = io.BytesIO(payload)


def _render(monkeypatch, payload: bytes) -> int:
    monkeypatch.setattr(sys, "stdin", _Stdin(payload))
    try:
        return vm_config.main(["render"])
    except SystemExit as exc:
        return exc.code


def _one_line(capsys) -> str:
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1, lines
    for sentinel in SENTINELS:
        assert sentinel not in captured.err
    assert "Traceback" not in captured.err
    assert lines[0].startswith("vm_config: ")
    return lines[0]


def _existing(config_dir: Path, mapping: dict | None = None) -> bytes:
    """A ``config.yaml`` already in place, and its bytes."""
    config_dir.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(mapping if mapping is not None else {**EXPECTED, "ntfy_topic": "old"})
    path = config_dir / CONFIG_FILE
    path.write_text(text)
    return path.read_bytes()


# -- writing ------------------------------------------------------------------------------


def test_a_first_render_writes_the_config_at_0600(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)

    assert _render(monkeypatch, _settings_bytes()) == 0

    path = config_dir / CONFIG_FILE
    written = yaml.safe_load(path.read_text())
    assert written == EXPECTED
    assert {k: type(v) for k, v in written.items()} == {k: str for k in EXPECTED}
    assert path.stat().st_mode & 0o777 == 0o600
    assert sorted(p.name for p in config_dir.iterdir()) == [CONFIG_FILE]
    line = _one_line(capsys)
    assert line == f"vm_config: wrote {path}, which was absent"


def test_the_tracked_settings_render(monkeypatch, capsys, metadata, config_dir, not_root):
    SsmHook().install(monkeypatch)
    tracked = Path(__file__).resolve().parents[2] / "config" / "vm.yaml"

    assert _render(monkeypatch, tracked.read_bytes()) == 0
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text()) == EXPECTED


def test_the_render_asks_for_the_five_names_once_with_decryption(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    hook = SsmHook().install(monkeypatch)

    _render(monkeypatch, _settings_bytes())

    assert len(hook.requests) == 1
    assert hook.requests[0].headers["X-Amz-Target"].decode() == "AmazonSSM.GetParameters"
    assert hook.body() == {"Names": list(VALUES), "WithDecryption": True}


def test_the_client_is_signed_by_the_instance_profile_in_the_settings_region(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    hook = SsmHook().install(monkeypatch)

    _render(monkeypatch, _settings_bytes(bucket_region="eu-west-3"))

    assert hook.regions == ["eu-west-3"]
    assert "ssm.eu-west-3.amazonaws.com" in hook.requests[0].url
    authorization = hook.requests[0].headers["Authorization"].decode()
    assert f"Credential={METADATA['AccessKeyId']}/" in authorization
    assert "/eu-west-3/ssm/" in authorization
    assert hook.requests[0].headers["X-Amz-Security-Token"].decode() == METADATA["Token"]


def test_the_written_file_loads_as_the_daemon_loads_it(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)

    _render(monkeypatch, _settings_bytes())

    config = load_config(config_dir / CONFIG_FILE)
    assert config.role == "shadow"
    assert str(config.backup_target) == TARGET
    assert config.schwab_api_key.reveal() == API_KEY
    assert config.bucket_credentials == "instance_profile"


def test_a_missing_config_directory_is_created_at_0700_whatever_the_umask(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    previous = os.umask(0)
    try:
        assert _render(monkeypatch, _settings_bytes()) == 0
    finally:
        os.umask(previous)

    assert config_dir.stat().st_mode & 0o777 == 0o700
    assert (config_dir / CONFIG_FILE).stat().st_mode & 0o777 == 0o600


def test_an_existing_config_directory_keeps_its_mode(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    config_dir.chmod(0o750)

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert config_dir.stat().st_mode & 0o777 == 0o750


def test_a_second_render_writes_nothing(monkeypatch, capsys, metadata, config_dir, not_root):
    SsmHook().install(monkeypatch)
    _render(monkeypatch, _settings_bytes())
    capsys.readouterr()
    path = config_dir / CONFIG_FILE
    before = path.stat()

    assert _render(monkeypatch, _settings_bytes()) == 0

    after = path.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert _one_line(capsys) == f"vm_config: unchanged: {path} already holds this config"


def test_a_hand_formatted_file_with_the_same_mapping_is_unchanged(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    path = config_dir / CONFIG_FILE
    text = "# by hand\n" + "".join(f"{key}: '{value}'\n" for key, value in EXPECTED.items())
    path.write_text(text)

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert path.read_text() == text
    assert "unchanged" in _one_line(capsys)


def test_a_changed_value_is_named_and_never_printed(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    _existing(config_dir, {**EXPECTED, "ntfy_topic": "OLD-TOPIC-SENTINEL"})

    assert _render(monkeypatch, _settings_bytes()) == 0

    line = _one_line(capsys)
    assert line == f"vm_config: wrote {config_dir / CONFIG_FILE}, changing ntfy_topic"
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text()) == EXPECTED


def test_a_key_only_the_old_file_held_is_named(monkeypatch, capsys, metadata, config_dir, not_root):
    SsmHook().install(monkeypatch)
    _existing(config_dir, {**EXPECTED, "schwab_callback_url": "https://127.0.0.1:8182"})

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert _one_line(capsys).endswith("changing schwab_callback_url")


def test_a_value_that_changes_only_its_type_is_rewritten(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    # ``True == 1`` in Python, so an equality check alone would keep the old file.
    SsmHook().install(monkeypatch)
    _existing(config_dir, {**EXPECTED, "note": True})

    assert _render(monkeypatch, _settings_bytes(note=1)) == 0
    assert _one_line(capsys).endswith("changing note")
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text())["note"] == 1


def test_a_role_change_says_the_daemon_needs_a_restart(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    _existing(config_dir, EXPECTED)

    assert _render(monkeypatch, _settings_bytes(role="primary")) == 0

    line = _one_line(capsys)
    assert line.startswith(f"vm_config: wrote {config_dir / CONFIG_FILE}, changing role. ")
    assert "restart the daemon" in line


def test_a_change_that_leaves_the_role_says_nothing_of_a_restart(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    _existing(config_dir)

    _render(monkeypatch, _settings_bytes())
    assert "restart" not in _one_line(capsys)


def test_an_old_file_without_a_role_counts_as_a_role_change(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    # An absent role loads as primary, so a daemon started on it runs as primary.
    SsmHook().install(monkeypatch)
    _existing(config_dir, {k: v for k, v in EXPECTED.items() if k != "role"})

    _render(monkeypatch, _settings_bytes())
    assert "changing role. " in _one_line(capsys)


def test_an_unreadable_old_file_is_replaced_and_a_restart_is_advised(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    (config_dir / CONFIG_FILE).write_text("role: [shadow\n")

    assert _render(monkeypatch, _settings_bytes()) == 0

    line = _one_line(capsys)
    assert line.startswith(f"vm_config: wrote {config_dir / CONFIG_FILE}, which was unreadable")
    assert "restart the daemon" in line
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text()) == EXPECTED


@pytest.mark.parametrize(
    "content",
    [b"- role\n- shadow\n", b"role: \xff\n"],
    ids=["a YAML list", "bytes that are not UTF-8"],
)
def test_an_old_file_that_is_no_mapping_is_unreadable(
    monkeypatch, capsys, metadata, config_dir, not_root, content
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    (config_dir / CONFIG_FILE).write_bytes(content)

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert ", which was unreadable" in _one_line(capsys)
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text()) == EXPECTED


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file at mode 000")
def test_an_old_file_the_owner_cannot_read_is_replaced(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    _existing(config_dir)
    (config_dir / CONFIG_FILE).chmod(0)

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert ", which was unreadable" in _one_line(capsys)
    assert (config_dir / CONFIG_FILE).stat().st_mode & 0o777 == 0o600


def test_an_old_key_yaml_reads_as_a_number_is_named(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    (config_dir / CONFIG_FILE).write_text(yaml.safe_dump(EXPECTED) + "1: stray\n")

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert _one_line(capsys).endswith(", changing 1")


def test_the_temp_file_is_never_opened_through_a_link(
    monkeypatch, capsys, metadata, config_dir, not_root, tmp_path
):
    """A link placed at the temp name after the stale-file removal is refused, not followed.

    The removal is skipped here to stand for a link that appeared between it and the
    open. Followed, the open would write every secret into the link's target.
    """
    from pathlib import Path as RealPath

    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.write_text("untouched")
    temp = temp_write_path(config_dir / CONFIG_FILE, os.getpid())
    temp.symlink_to(victim)
    real_unlink = RealPath.unlink

    def unlink(self, missing_ok=False):
        if self == temp and self.is_symlink():
            return None
        return real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(RealPath, "unlink", unlink)

    assert _render(monkeypatch, _settings_bytes()) == 1
    assert victim.read_text() == "untouched"
    assert "could not be written (FileExistsError)" in _one_line(capsys)
    assert not (config_dir / CONFIG_FILE).exists()


def test_the_temp_file_is_synced_before_the_rename(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    events: list[tuple[str, int]] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        events.append(("fsync", os.fstat(fd).st_ino))
        return real_fsync(fd)

    def replace(src, dst):
        events.append(("replace", os.stat(src).st_ino))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert [name for name, _ in events] == ["fsync", "replace"]
    assert events[0][1] == events[1][1]


def test_a_temp_file_left_by_a_crash_under_this_pid_is_replaced(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    config_dir.mkdir(parents=True)
    stale = temp_write_path(config_dir / CONFIG_FILE, os.getpid())
    stale.write_text("half a file")

    assert _render(monkeypatch, _settings_bytes()) == 0
    assert sorted(p.name for p in config_dir.iterdir()) == [CONFIG_FILE]
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text()) == EXPECTED


def test_a_failed_write_exits_one_and_leaves_the_old_file_and_no_temp(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    before = _existing(config_dir)

    def refuse(src, dst):
        raise PermissionError(13, "Permission denied", str(dst))

    monkeypatch.setattr(os, "replace", refuse)

    assert _render(monkeypatch, _settings_bytes()) == 1

    path = config_dir / CONFIG_FILE
    assert path.read_bytes() == before
    assert sorted(p.name for p in config_dir.iterdir()) == [CONFIG_FILE]
    assert _one_line(capsys) == f"vm_config: failed: {path} could not be written (PermissionError)"


def test_settings_of_exactly_the_size_limit_are_read(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook().install(monkeypatch)
    payload = _settings_bytes()
    padding = vm_config.SETTINGS_MAX_BYTES - len(payload) - 2
    payload += b"#" + b"x" * padding + b"\n"
    assert len(payload) == vm_config.SETTINGS_MAX_BYTES

    assert _render(monkeypatch, payload) == 0


# -- refusals -----------------------------------------------------------------------------


def _assert_refused(capsys, config_dir: Path, before: bytes) -> str:
    path = config_dir / CONFIG_FILE
    assert path.read_bytes() == before
    assert sorted(p.name for p in config_dir.iterdir()) == [CONFIG_FILE]
    line = _one_line(capsys)
    assert line.startswith(f"vm_config: refused, and {path} was left as it was: ")
    return line


SETTINGS_REFUSALS = {
    "a parameter's key": (_settings_bytes(ntfy_topic="tracked"), "set ['ntfy_topic']"),
    "two parameters' keys": (
        _settings_bytes(backup_target="/x", schwab_api_key="k"),
        "set ['backup_target', 'schwab_api_key']",
    ),
    "no role": (_settings_bytes(role=_DROP), "role must be exactly"),
    "a capitalised role": (_settings_bytes(role="Shadow"), "role must be exactly"),
    "a role with a space": (_settings_bytes(role="primary "), "role must be exactly"),
    "a role that is a boolean": (_settings_bytes(role=True), "role must be exactly"),
    "no region": (_settings_bytes(bucket_region=_DROP), "bucket_region None is not"),
    "a malformed region": (_settings_bytes(bucket_region="us-east"), "'us-east' is not"),
    "a region that is a number": (_settings_bytes(bucket_region=1), "bucket_region 1 is not"),
    "a list": (b"- role\n- shadow\n", "not a YAML mapping"),
    "an empty file": (b"", "not a YAML mapping"),
    "broken YAML": (b"role: [shadow\n", "not UTF-8 YAML"),
    "bytes that are not UTF-8": (b"role: \xff\n", "not UTF-8 YAML"),
    "a file over the limit": (
        b"#" * vm_config.SETTINGS_MAX_BYTES + b"\n" + _settings_bytes(),
        f"over {vm_config.SETTINGS_MAX_BYTES}",
    ),
    "a key that is a number": (_settings_bytes() + b"1: one\n", "a key that is not text"),
    # Written by hand, out of order, so the refusal's sorting is what puts them in order.
    "parameters' keys out of order": (
        b"schwab_api_key: k\nbackup_target: /x\n" + _settings_bytes(),
        "set ['backup_target', 'schwab_api_key']",
    ),
}


@pytest.mark.parametrize(("payload", "expected"), SETTINGS_REFUSALS.values(), ids=SETTINGS_REFUSALS)
def test_settings_the_render_cannot_use_refuse_before_any_request(
    monkeypatch, capsys, metadata, config_dir, not_root, payload, expected
):
    hook = SsmHook().install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, payload) == 2

    assert expected in _assert_refused(capsys, config_dir, before)
    assert hook.regions == [] and hook.requests == [] and metadata.requests == []


def test_a_primary_role_is_accepted(monkeypatch, capsys, metadata, config_dir, not_root):
    SsmHook().install(monkeypatch)

    assert _render(monkeypatch, _settings_bytes(role="primary")) == 0
    assert yaml.safe_load((config_dir / CONFIG_FILE).read_text())["role"] == "primary"


def test_running_as_root_refuses_before_any_request(monkeypatch, capsys, metadata, config_dir):
    hook = SsmHook().install(monkeypatch)
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 2

    assert "runs as root" in _assert_refused(capsys, config_dir, before)
    assert hook.requests == [] and metadata.requests == []


def test_a_closed_standard_input_refuses(monkeypatch, capsys, metadata, config_dir, not_root):
    hook = SsmHook().install(monkeypatch)
    monkeypatch.setattr(sys, "stdin", None)

    try:
        code = vm_config.main(["render"])
    except SystemExit as exc:
        code = exc.code

    assert code == 2
    assert "standard input is closed" in _one_line(capsys)
    assert hook.requests == []


def _without(name: str) -> dict[str, str]:
    return {key: value for key, value in VALUES.items() if key != name}


PARAMETER_REFUSALS = {
    "one invalid name": (
        SsmHook(invalid=("/marketlake/config/backup-target",)),
        "SSM has no parameter named ['/marketlake/config/backup-target']",
    ),
    "every name invalid, as a wrong region gives": (
        SsmHook(invalid=tuple(VALUES)),
        "SSM has no parameter named [",
    ),
    "a name in neither list": (
        SsmHook(_without("/marketlake/config/ntfy-topic")),
        "SSM returned no parameter named /marketlake/config/ntfy-topic",
    ),
    "an empty value": (
        SsmHook({**VALUES, "/marketlake/config/schwab-app-secret": ""}),
        "the parameter /marketlake/config/schwab-app-secret is empty or not text",
    ),
    # SSM always answers with a string. A broken or impersonated endpoint need not, and
    # botocore passes a JSON number through unchanged.
    "a value that is a number": (
        SsmHook({**VALUES, "/marketlake/config/ntfy-topic": 5}),
        "the parameter /marketlake/config/ntfy-topic is empty or not text",
    ),
    "a trailing newline": (
        SsmHook({**VALUES, "/marketlake/config/backup-target": f"{TARGET}\n"}),
        "the parameter /marketlake/config/backup-target has whitespace",
    ),
    "a leading space": (
        SsmHook({**VALUES, "/marketlake/config/schwab-api-key": f" {API_KEY}"}),
        "the parameter /marketlake/config/schwab-api-key has whitespace",
    ),
    "a target that is a path": (
        SsmHook({**VALUES, "/marketlake/config/backup-target": f"{BUCKET}/lake"}),
        "backup_target is not an s3:// bucket target",
    ),
    "a target naming no valid bucket": (
        SsmHook({**VALUES, "/marketlake/config/backup-target": f"s3://{BUCKET.upper()}/lake"}),
        "backup_target names no valid bucket",
    ),
    "a target whose prefix climbs": (
        SsmHook({**VALUES, "/marketlake/config/backup-target": f"s3://{BUCKET}/../lake"}),
        "backup_target has a prefix holding . or ..",
    ),
}


@pytest.mark.parametrize(("hook", "expected"), PARAMETER_REFUSALS.values(), ids=PARAMETER_REFUSALS)
def test_parameters_the_render_cannot_use_refuse(
    monkeypatch, capsys, metadata, config_dir, not_root, hook, expected
):
    hook.install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 2

    line = _assert_refused(capsys, config_dir, before)
    assert expected in line
    assert BUCKET.upper() not in line


CONFIG_REFUSALS = {
    "no lake_root": (
        {"lake_root": _DROP},
        "the merged config does not load (config missing required key(s): ['lake_root'])",
    ),
    "a guard that does not exist": (
        {"guards": {"no_such_guard": 1}},
        "unknown guard constant(s): ['no_such_guard']",
    ),
    "the key path with no keys": (
        {"bucket_credentials": "keys"},
        "the bucket needs config key(s): ['bucket_access_key_id', 'bucket_secret_access_key']",
    ),
    "a key beside the instance profile": (
        {"bucket_access_key_id": "AKIDSTRAY"},
        "the config must not hold ['bucket_access_key_id']",
    ),
    "an unknown credential source": (
        {"bucket_credentials": "instance-profile"},
        "bucket_credentials must be keys or instance_profile",
    ),
    "a lake_root under a home that does not exist": (
        {"lake_root": "~no-such-user-sentinel/lake"},
        "the merged config does not load (RuntimeError)",
    ),
}


@pytest.mark.parametrize(("changes", "expected"), CONFIG_REFUSALS.values(), ids=CONFIG_REFUSALS)
def test_a_merged_config_a_job_would_refuse_is_refused(
    monkeypatch, capsys, metadata, config_dir, not_root, changes, expected
):
    hook = SsmHook().install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes(**changes)) == 2

    assert expected in _assert_refused(capsys, config_dir, before)
    assert len(hook.requests) == 1


def test_a_refusal_with_no_old_file_writes_none(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    SsmHook({**VALUES, "/marketlake/config/ntfy-topic": ""}).install(monkeypatch)

    assert _render(monkeypatch, _settings_bytes()) == 2
    assert not config_dir.exists()
    _one_line(capsys)


# -- failures -----------------------------------------------------------------------------


def test_no_instance_profile_exits_three(monkeypatch, capsys, metadata, config_dir, not_root):
    metadata.mode = "no_role"
    hook = SsmHook().install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 3

    assert (config_dir / CONFIG_FILE).read_bytes() == before
    assert hook.requests == []
    line = _one_line(capsys)
    assert line.startswith("vm_config: no credentials: ")
    assert "(none returned)" in line


@pytest.mark.parametrize("code", ["AccessDeniedException", "UnrecognizedClientException"])
def test_an_aws_error_exits_one_naming_only_its_code(
    monkeypatch, capsys, metadata, config_dir, not_root, code
):
    SsmHook(error=code).install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 1

    assert (config_dir / CONFIG_FILE).read_bytes() == before
    path = config_dir / CONFIG_FILE
    assert _one_line(capsys) == (
        f"vm_config: failed: the parameters could not be read ({code}), so {path} was left "
        "as it was"
    )


def test_a_botocore_error_exits_one_naming_only_its_type(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    from botocore.exceptions import BotoCoreError

    class SentinelError(BotoCoreError):
        fmt = ERROR_MESSAGE

    SsmHook(raises=SentinelError()).install(monkeypatch)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 1

    assert (config_dir / CONFIG_FILE).read_bytes() == before
    assert "the parameters could not be read (SentinelError)" in _one_line(capsys)


def _sentinel_botocore_error():
    from botocore.exceptions import BotoCoreError

    class SentinelError(BotoCoreError):
        fmt = ERROR_MESSAGE

    return SentinelError()


@pytest.mark.parametrize(
    "error",
    [ValueError(f"no region {ERROR_MESSAGE}"), _sentinel_botocore_error()],
    ids=["ValueError", "BotoCoreError"],
)
def test_a_client_that_cannot_be_built_exits_one(
    monkeypatch, capsys, metadata, config_dir, not_root, error
):
    def broken(region):
        raise error

    monkeypatch.setattr(vm_config, "render_client", broken)
    before = _existing(config_dir)

    assert _render(monkeypatch, _settings_bytes()) == 1

    assert (config_dir / CONFIG_FILE).read_bytes() == before
    expected = f"the SSM client could not be built ({type(error).__name__})"
    assert expected in _one_line(capsys)


def test_a_failed_read_of_standard_input_refuses(
    monkeypatch, capsys, metadata, config_dir, not_root
):
    class Broken:
        def read(self, size=-1):
            raise OSError(5, f"Input/output error {ERROR_MESSAGE}")

    stdin = _Stdin(b"")
    stdin.buffer = Broken()
    monkeypatch.setattr(sys, "stdin", stdin)

    try:
        code = vm_config.main(["render"])
    except SystemExit as exc:
        code = exc.code

    assert code == 2
    assert "cannot read the settings from standard input (OSError)" in _one_line(capsys)


def test_the_client_carries_the_token_pulls_timeouts_and_retries(metadata):
    client = vm_config.render_client("us-east-1")
    config = client.meta.config
    assert (config.connect_timeout, config.read_timeout) == (10, 30)
    # botocore reads ``max_attempts: 3`` as three retries, so four attempts in all.
    assert config.retries == {"mode": "standard", "total_max_attempts": 4}
