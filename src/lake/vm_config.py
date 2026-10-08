"""The VM's config render: ``config.yaml`` from a tracked file, four SSM parameters and a tag.

The hosted VM has to go from nothing to capturing with no login, marketlake #686, and its
daemon cannot start without ``config.yaml``. That file holds four secrets and the backup
target, so no tracked file can carry it whole. The owner's decision of 2026-10-06 splits
it. The settings that are not secret live in the tracked ``config/vm.yaml``, which a
reviewed pull request changes. The four secrets live in SSM Parameter Store, and the
backup target is an instance tag. This module joins the three and writes the file::

    python -m lake.vm_config render < config/vm.yaml

It reads the settings on standard input, as ``python -m lake.roster apply`` reads its
roster. cloud-init's first boot runs it, and marketlake #676's deploy runs it again after
each pull.

The parameters are fetched by a literal list in one ``GetParameters`` call with
decryption. The call takes at most ten names and returns them in its own order, so each
is matched by ``Name``. They are the four secrets marketlake #699 put under
``/marketlake/config/``, which the instance role reads.

**The backup target comes from the instance's ``marketlake:backup-target`` tag**, by the
owner's decision of 2026-10-07. ``backup_target`` is a required key, and it names the
bucket, which no tracked file may. It was a fifth parameter, put by hand. OpenTofu
already holds the bucket as ``var.backup_bucket``, so ``infra/live/vm.tf`` writes the
tag as ``s3://<bucket>/lake``, and a bucket change reaches the VM with the apply rather
than through a hand step that can drift from the real bucket. The bucket's name is not a
secret. It is only kept off GitHub, and a tag keeps it off as well as a parameter did.
The render reads the tag from the instance metadata service with IMDSv2, a token ``PUT``
and then a ``GET`` of ``latest/meta-data/tags/instance/marketlake:backup-target``. That
path answers only when the instance has ``instance_metadata_tags`` enabled, which
``vm.tf`` sets too.

**The render refuses, exits 2 and keeps any existing file**, in each of these cases.

1. It runs as root. ``os.replace`` keeps the temp file's owner, so a root-owned
   ``config.yaml`` would be one the daemon cannot read.
2. The settings are not a YAML mapping, or are larger than ``SETTINGS_MAX_BYTES``.
3. The settings set a key a parameter or the tag fills. Two sources for one key leave a
   reader unsure which one won, and a secret pasted into the tracked file is already a
   leak.
4. The settings' ``role`` is not exactly ``shadow`` or ``primary``. An absent ``role``
   loads as primary, so a missing line would make the VM capture as primary by accident.
5. ``bucket_region`` is not an AWS region name. The client is signed for it, so the check
   runs before the client is built.
6. ``bucket_credentials`` is not exactly ``instance_profile``. The render always signs
   with the instance profile, so a file saying ``keys`` would make every bucket job sign
   one way while the render that wrote it signed another.
7. The tag's value is unusable: it is not UTF-8, is empty, or has whitespace at either
   end. A target with a trailing newline gives the prefix ``lake\\n``, which no later
   check catches. This runs before any credential is fetched. A tag the metadata service
   does not serve at all is not a refusal, as the paragraph on exit 3 below says.
8. ``InvalidParameters`` names any parameter. ``GetParameters`` answers HTTP 200 with a
   missing name listed there, and a render that read only ``Parameters`` would write a
   file missing a key, which #699's pass 3 found. A wrong region shows up here too, as
   every name at once.
9. ``Parameters`` names one parameter twice. Which of the two values would win depends
   on the order of the answer, so neither is used.
10. A fetched value is empty, or has whitespace at either end.
11. The merged mapping fails ``Config.from_mapping``. The daemon loads ``config.yaml``
    every cycle, so a file that fails to load costs capture.
12. ``backup_target`` is not an ``s3://`` target, or has a ``bucket_target_problems``
    problem, or the config has a ``bucket_credential_problems`` problem. Loading never
    refuses a backup setting, by design, so a target without its scheme would load as a
    relative path. The render is a job rather than the capture path, so it applies the
    bucket jobs' strict checks.
13. ``lake_window_sessions`` is set and is not a whole number of sessions at or above
    ``lake.window.window_floor``, marketlake #786. Loading stores the key without judging it,
    for the reason ``lake.window`` gives, so the render is where a bad window stops before it
    reaches the VM, rather than on the first night the sweep or the trim reads it.

A refusal is raised outside the handler that caught its cause, as ``config.load_config``
does, so neither its ``__cause__`` nor its ``__context__`` holds a YAML error. PyYAML
quotes the offending line, and a secret pasted into the settings would ride along in any
traceback that printed the chain.

**No value reaches any output.** A line names a parameter, the tag or a key and never
what it holds. An AWS ``ClientError`` is reported by its error code alone and a
``BotoCoreError`` by its type name alone, for the reasons ``lake.token_store`` gives. A
bucket-target problem names ``backup_target`` in place of the target itself.

**This makes the render a third user of the instance profile.** The other two that
``lake.aws_session`` names, the bucket and the token pull, both take it because
``config.yaml``'s ``bucket_credentials`` says so. The render cannot read
``config.yaml``, which it is writing, so it takes the instance profile and the region
from the tracked settings instead. That is acceptable for the reason the design gives
for the ``aws`` CLI on the VM: there the IAM role's policy bounds what a process can do.
Off the VM the metadata service does not answer, and the render exits 3. The tag lookup
exits 3 the same way when the service does not answer, or serves no token or a token
that is not printable ASCII without whitespace. It also exits 3 on a 404 for the tag.
AWS does not document whether a tag given at launch is served from the first moment of
the first boot, so a 404 may only mean the tag is not served yet, and the first boot
retries exit 3 a bounded number of times. A 404 that lasts means the instance has no
``marketlake:backup-target`` tag or its ``instance_metadata_tags`` is disabled, so the
line names both causes and the fix in ``infra/live/vm.tf``. Any other HTTP error on the
tag exits 1, and so does a redirect, which the lookup never follows.

**The write is atomic and private.** The temp file sits beside the target and is created
at mode 0600 with ``O_EXCL``, rather than chmodded after the secrets land. It is fsynced
and renamed into place, so the daemon's next load sees the whole old file or the whole
new one. The directory is fsynced after the rename, so the new name survives a crash. An
interrupt during the write removes the temp file too, which is why the cleanup catches
``BaseException``. The YAML comes from ``yaml.safe_dump``, because a secret written by
hand can parse back as another type. ``tickers._write_atomically`` and
``reauth.write_token`` do not fit as they stand: neither creates its file at 0600, and
``write_token`` writes JSON.
A missing config directory is created at mode 0700. A file that already parses to the
same mapping is not rewritten. It is reported as ``unchanged`` when its mode is 0600, and
otherwise chmodded to 0600 and reported as ``tightened``, because a file holding four
secrets must not stay readable by others just because its content matched. A symbolic
link at ``config.yaml`` is never read or chmodded through. The render writes a new file
over it, so ``os.replace`` replaces the link itself and the file it pointed at keeps its
content and mode.

**The line names the keys whose values changed.** The daemon builds its senders once, at
start, so a changed ``role`` reaches it only through a restart. Each compaction starts a
fresh process that reads the new role at once. A shadow-to-primary render without a
restart would leave capture's pings in the outbox while compaction's went live, and
healthchecks would page "capture down" while capture ran. So the line says when the role
changed, and the caller restarts the daemon. #638's cutover and #676's deploy own that
restart.

The exit codes match the token pull's, so the first boot's retry treats both alike: 0
written, unchanged or tightened, 3 no credentials or no tag served yet, 2 a refusal, and
1 any other failure.

The client and the tag lookup are seams. ``render`` takes a required ``client_factory``,
which it calls with the settings' region, so a metadata service with no credentials yet
becomes the ``no credentials`` outcome inside it. It also takes a required
``tag_reader``, which it calls with no arguments for the tag's raw value. ``main`` builds
the real ones and accepts neither, the rule ``tests/unit/test_seam_defaults`` states for
every entry in this package.
"""

from __future__ import annotations

import http.client
import os
import stat
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from lake import aws_session
from lake.aws_session import INSTANCE_PROFILE, _MetadataLookupFailed, build_client
from lake.config import (
    BUCKET_CREDENTIALS_KEY,
    BUCKET_REGION_KEY,
    CREDENTIALS_FROM_INSTANCE_PROFILE,
    ROLE_KEY,
    BucketTarget,
    Config,
    ConfigError,
    bucket_credential_problems,
    bucket_target_problems,
    default_config_path,
    input_errors_exit,
    is_region_name,
)
from lake.outbox import PRIMARY, SHADOW
from lake.paths import temp_write_path
from lake.window import WindowRefused, window_sessions

# The four parameters and the ``config.yaml`` key each one fills. The names are literals,
# because the instance role reads ``/marketlake/config/*`` and a name built from input
# could reach a parameter nothing meant to read.
PARAMETERS = (
    ("/marketlake/config/schwab-api-key", "schwab_api_key"),
    ("/marketlake/config/schwab-app-secret", "schwab_app_secret"),
    ("/marketlake/config/healthchecks-ping-key", "healthchecks_ping_key"),
    ("/marketlake/config/ntfy-topic", "ntfy_topic"),
)
PARAMETER_KEYS = frozenset(key for _, key in PARAMETERS)

# The instance tag that carries ``backup_target``, and the key it fills. ``infra/live/vm.tf``
# writes the tag. A key the metadata service serves may hold no ``/`` and no space.
BACKUP_TARGET_TAG = "marketlake:backup-target"
BACKUP_TARGET_KEY = "backup_target"

# Every key the render fills, which the tracked settings therefore must not set.
FILLED_KEYS = PARAMETER_KEYS | {BACKUP_TARGET_KEY}

# The two IMDSv2 paths, relative to ``aws_session.METADATA_BASE_URL``, and how long the
# token lives, in seconds. The render asks for the tag once, so a minute is plenty.
_TOKEN_PATH = "latest/api/token"
_TAG_PATH = f"latest/meta-data/tags/instance/{BACKUP_TARGET_TAG}"
_TOKEN_TTL_S = 60

# The largest settings file the render reads, in bytes. ``config/vm.yaml`` is under one
# kilobyte, so anything near this is not that file.
SETTINGS_MAX_BYTES = 64 * 1024

# The mode of the written file and of a config directory the render creates.
FILE_MODE = 0o600
DIRECTORY_MODE = 0o700

# The SSM client's settings, the shape ``lake.token_store`` uses. The timeouts bound a
# stalled socket, and the retries are botocore's standard ones, which retry a throttle.
_SSM_CLIENT_CONFIG = {
    "connect_timeout": 10,
    "read_timeout": 30,
    "retries": {"mode": "standard", "max_attempts": 3},
}

WROTE = "wrote"
UNCHANGED = "unchanged"
TIGHTENED = "tightened"
NO_CREDENTIALS = "no credentials"
NO_TAG_YET = "no tag yet"
FAILED = "failed"

# The exit code each outcome gives. A refusal raises ``RenderRefused`` instead, which
# ``main`` turns into exit 2. A tag the metadata service does not serve gives 3 rather
# than 2, because at first boot it may only be not served yet, and 3 is what the first
# boot retries.
EXIT_CODES = {
    WROTE: 0,
    UNCHANGED: 0,
    TIGHTENED: 0,
    NO_CREDENTIALS: 3,
    NO_TAG_YET: 3,
    FAILED: 1,
}


class RenderRefused(Exception):
    """The render refused, and the existing ``config.yaml`` was left as it was.

    The message is the whole line after ``vm_config:``. It names keys, parameters and the
    tag, and never a value.
    """


class MetadataUnavailable(Exception):
    """The metadata service did not answer the tag lookup, or served no usable token.

    A retry can fix this, so :func:`render` reports it as ``no credentials`` and exits 3.
    ``detail`` names the failure by type or HTTP status, never by message.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class TagNotServed(Exception):
    """The metadata service answered the tag lookup with a 404.

    Either the tag is not served yet, which a retry fixes, or the instance has no such
    tag or its ``instance_metadata_tags`` is disabled. :func:`render` reports it as
    ``no tag yet`` and exits 3, with a line that names both causes.
    """


class TagLookupFailed(Exception):
    """The metadata service answered the tag lookup with an HTTP error other than 404.

    A redirect is one, because the lookup never follows it.
    """

    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


@dataclass(frozen=True)
class RenderResult:
    """What a render did, and the one line it prints. Neither carries a value."""

    outcome: str
    line: str


def render_client(region: str) -> Any:
    """An SSM client in ``region``, signed by the instance profile and nothing else.

    A host with no instance profile raises ``_MetadataLookupFailed`` here, which
    :func:`render` reports as ``no credentials``.
    """
    return build_client(
        "ssm",
        region=region,
        source=INSTANCE_PROFILE,
        client_config=_SSM_CLIENT_CONFIG,
    )


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuses every redirect, so a 3xx reaches the caller as an ``HTTPError``.

    The default handler follows a redirect and copies the request's headers onto the new
    request, the session token included, so a 302 from the metadata address would hand
    the token to whatever host it named.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _usable_token(token: bytes) -> bool:
    """Whether ``token`` is non-empty printable ASCII with no whitespace.

    ``http.client`` raises ``ValueError`` on a header holding a newline, quoting the
    value, and ``UnicodeEncodeError`` on one it cannot encode. Either would print the
    token in a traceback, so a token outside this set is never sent.
    """
    return bool(token) and all(0x21 <= byte <= 0x7E for byte in token)


def read_backup_target_tag() -> str:
    """The ``marketlake:backup-target`` tag's raw value, read from instance metadata.

    It asks with IMDSv2: a ``PUT`` for a session token, then a ``GET`` of the tag with
    that token. The base address is ``aws_session.METADATA_BASE_URL``, read at call time
    so a test can point it at loopback, and each request waits at most
    ``aws_session.METADATA_TIMEOUT_S``. The opener holds an empty ``ProxyHandler``, so no
    ``*_proxy`` variable can carry the request elsewhere, and a redirect handler that
    follows nothing, so no redirect can carry the token elsewhere. ``urllib`` never reads
    the ``AWS_*`` variables. That is why it does not use ``aws_session._proxies_cleared``,
    which edits the process's environment while it runs.

    A service that does not answer, or serves no token or one that is not printable
    ASCII without whitespace, raises ``MetadataUnavailable``, and the token is never sent
    or quoted. A 404 on the tag raises ``TagNotServed``. Any other HTTP error, a redirect
    included, raises ``TagLookupFailed``. The value is returned as it came, for
    :func:`render` to check.
    """
    base = aws_session.METADATA_BASE_URL
    timeout = aws_session.METADATA_TIMEOUT_S
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirects())
    # Each failure is recorded and raised after its handler, so no answer from the
    # service rides along as the raised exception's context.
    unavailable = None
    token = b""
    try:
        request = urllib.request.Request(
            base + _TOKEN_PATH,
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": str(_TOKEN_TTL_S)},
        )
        with opener.open(request, timeout=timeout) as response:
            token = response.read()
    except urllib.error.HTTPError as exc:
        exc.close()
        unavailable = f"token HTTP {exc.code}"
    except (OSError, http.client.HTTPException) as exc:
        unavailable = type(exc).__name__
    if unavailable is None and not token.strip():
        unavailable = "no token"
    if unavailable is None and not _usable_token(token):
        unavailable = "a malformed token"
    if unavailable is not None:
        raise MetadataUnavailable(unavailable)

    status = None
    body = b""
    try:
        request = urllib.request.Request(
            base + _TAG_PATH,
            method="GET",
            headers={"X-aws-ec2-metadata-token": token.decode("ascii")},
        )
        with opener.open(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        exc.close()
        status = exc.code
    except (OSError, http.client.HTTPException) as exc:
        unavailable = type(exc).__name__
    if unavailable is not None:
        raise MetadataUnavailable(unavailable)
    if status == 404:
        raise TagNotServed()
    if status is not None:
        raise TagLookupFailed(status)
    decoded = None
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError:
        pass
    if decoded is None:
        raise RenderRefused(f"the {BACKUP_TARGET_TAG} tag is not UTF-8 text")
    return decoded


def _client_error_code(exc: Any) -> str:
    """The AWS error code a ``ClientError`` carries, and never its message."""
    response = getattr(exc, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return str(code) if code else type(exc).__name__


def _parse_settings(payload: bytes) -> dict[str, Any]:
    """The settings ``payload`` holds, or a ``RenderRefused`` saying why it holds none.

    A parse error is dropped rather than quoted, because PyYAML quotes the offending line
    and a secret pasted into the file by mistake would land in the log.
    """
    if len(payload) > SETTINGS_MAX_BYTES:
        raise RenderRefused(
            f"the settings are over {SETTINGS_MAX_BYTES} bytes, so they are not config/vm.yaml"
        )
    # The refusal is raised after the handler, so the parse error is not its context.
    parsed_ok = True
    try:
        parsed = yaml.safe_load(payload.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError):
        parsed_ok = False
    if not parsed_ok:
        raise RenderRefused("the settings are not UTF-8 YAML")
    if not isinstance(parsed, dict):
        raise RenderRefused("the settings are not a YAML mapping")
    # Every key in config.yaml is text. A key YAML reads as a number would load and be
    # ignored, and the line naming the changed keys could not sort it beside the others.
    if not all(isinstance(key, str) for key in parsed):
        raise RenderRefused("the settings have a key that is not text")
    return parsed


def _check_settings(settings: Mapping[Any, Any]) -> str:
    """Refuse settings the render must not merge, and return the region to sign for."""
    filled = sorted(str(key) for key in settings if key in FILLED_KEYS)
    if filled:
        raise RenderRefused(
            f"the settings set {filled}, which the parameters and the tag fill; remove them "
            "from config/vm.yaml"
        )
    role = settings.get(ROLE_KEY)
    if not (isinstance(role, str) and role in (SHADOW, PRIMARY)):
        raise RenderRefused(
            f"the settings' {ROLE_KEY} must be exactly {SHADOW!r} or {PRIMARY!r}, because "
            "an absent role loads as primary"
        )
    region = settings.get(BUCKET_REGION_KEY)
    if not (isinstance(region, str) and is_region_name(region)):
        raise RenderRefused(
            f"the settings' {BUCKET_REGION_KEY} {region!r} is not an AWS region name like us-east-2"
        )
    # Compared as a string, since the value is a credential setting and is never quoted.
    if settings.get(BUCKET_CREDENTIALS_KEY) != CREDENTIALS_FROM_INSTANCE_PROFILE:
        raise RenderRefused(
            f"the settings' {BUCKET_CREDENTIALS_KEY} must be exactly "
            f"{CREDENTIALS_FROM_INSTANCE_PROFILE}, because the render signs with the "
            "instance profile and every bucket job must sign the same way"
        )
    return region


def _parameter_values(response: object) -> dict[str, str]:
    """Each parameter's value keyed by the ``config.yaml`` key it fills, or a refusal."""
    body = response if isinstance(response, dict) else {}
    invalid = body.get("InvalidParameters") or []
    if invalid:
        names = sorted(str(name) for name in invalid)
        raise RenderRefused(f"SSM has no parameter named {names}")
    found = {}
    repeated = set()
    for parameter in body.get("Parameters") or []:
        if isinstance(parameter, dict):
            name = parameter.get("Name")
            if name in found:
                repeated.add(str(name))
            found[name] = parameter.get("Value")
    if repeated:
        raise RenderRefused(f"SSM returned {sorted(repeated)} more than once")
    values = {}
    for name, key in PARAMETERS:
        if name not in found:
            raise RenderRefused(f"SSM returned no parameter named {name}")
        value = found[name]
        if not isinstance(value, str) or not value:
            raise RenderRefused(f"the parameter {name} is empty or not text")
        if value != value.strip():
            raise RenderRefused(f"the parameter {name} has whitespace at its start or end")
        values[key] = value
    return values


def _check_tag(value: object) -> str:
    """The tag's value, or a refusal naming the tag and never the value."""
    if not isinstance(value, str) or not value:
        raise RenderRefused(f"the {BACKUP_TARGET_TAG} tag is empty or not text")
    if value != value.strip():
        raise RenderRefused(f"the {BACKUP_TARGET_TAG} tag has whitespace at its start or end")
    return value


def _check_config(merged: Mapping[str, Any]) -> None:
    """Refuse a merged mapping the daemon could not load, or a job would refuse."""
    # The refusal is raised after the handler, so the load error is not its context.
    failure = None
    try:
        config = Config.from_mapping(merged)
    except ConfigError as exc:
        failure = str(exc)
    except Exception as exc:
        # Loading can raise more than ``ConfigError``. A ``lake_root`` of ``~nobody/lake``
        # raises ``RuntimeError`` from ``expanduser``, which the daemon's own load would
        # raise every cycle. Only the type is named, since the message can quote a value.
        failure = type(exc).__name__
    if failure is not None:
        raise RenderRefused(f"the merged config does not load ({failure})")
    target = config.backup_target
    if not isinstance(target, BucketTarget):
        raise RenderRefused("backup_target is not an s3:// bucket target")
    # Each problem names the target, which names the bucket, so it is swapped for the key.
    problems = [
        problem.replace(str(target), "backup_target") for problem in bucket_target_problems(target)
    ]
    problems.extend(bucket_credential_problems(config))
    if problems:
        raise RenderRefused(". ".join(problems))
    # The floor reads the merged guards, so a ``guards:`` override that raises the battery's
    # trailing median raises the floor with it.
    # Raised after the handler, the way every refusal here is, so it carries no context.
    window_problem = None
    try:
        window_sessions(config.lake_window_sessions, config.guards)
    except WindowRefused as exc:
        window_problem = str(exc)
    if window_problem is not None:
        raise RenderRefused(window_problem)


def _same(left: object, right: object) -> bool:
    """Whether two parsed YAML values are equal, types included.

    ``==`` says ``1 == True`` and ``1 == 1.0``, and a file holding the one would be left
    in place for settings that say the other, so the comparison is on the dumped text.
    """
    return yaml.safe_dump(left, sort_keys=True) == yaml.safe_dump(right, sort_keys=True)


def _existing(target: Path) -> tuple[str, Mapping[Any, Any] | None]:
    """What sits at ``target`` now and its mapping, if it is a file that holds one.

    The state is ``absent``, ``a symbolic link``, ``unreadable`` or ``present``. A link is
    never read through, so the render writes over it rather than reporting it unchanged
    and chmodding the file it points at.
    """
    try:
        info = os.lstat(target)
    except FileNotFoundError:
        return "absent", None
    except OSError:
        return "unreadable", None
    if stat.S_ISLNK(info.st_mode):
        return "a symbolic link", None
    try:
        text = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "absent", None
    except (OSError, UnicodeDecodeError):
        return "unreadable", None
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return "unreadable", None
    if not isinstance(parsed, dict):
        return "unreadable", None
    return "present", parsed


def _write(target: Path, text: str) -> None:
    """Write ``text`` to ``target`` through a 0600 temp file, an fsync and one rename.

    The directory is fsynced after the rename, so the new name survives a crash. A
    ``KeyboardInterrupt`` or any other exit during the write removes the temp file.
    """
    target.parent.mkdir(mode=DIRECTORY_MODE, parents=True, exist_ok=True)
    tmp = temp_write_path(target, os.getpid())
    # A temp file left by a crash under the same pid would make ``O_EXCL`` refuse every
    # later run, and the first boot's pids repeat. Removing it first keeps ``O_EXCL``'s
    # point, which is that the open never follows a link placed at that name.
    tmp.unlink(missing_ok=True)
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    # The standard directory fsync, as ``journal`` takes it for a new segment.
    directory = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def render(
    payload: bytes,
    *,
    client_factory: Callable[[str], Any],
    tag_reader: Callable[[], object],
    config_path: str | Path | None = None,
    geteuid: Callable[[], int] | None = None,
) -> RenderResult:
    """Render ``config.yaml`` from the settings in ``payload``, four parameters and the tag.

    The refusals and their order are the module docstring's, and each raises
    ``RenderRefused`` before anything is written. ``tag_reader`` is called once the
    settings pass, before any credential is fetched, and returns the tag's raw value. It
    raises ``MetadataUnavailable``, which becomes ``no credentials``, ``TagNotServed``,
    which becomes ``no tag yet``, ``TagLookupFailed``, which becomes ``failed``, or
    ``RenderRefused``. ``client_factory`` is called next with
    the settings' ``bucket_region``, so a metadata service with no credentials yet becomes
    ``no credentials``. An AWS failure becomes ``failed``, named by its code or type.
    ``config_path`` defaults to ``config.default_config_path()``, resolved when this runs.
    ``geteuid`` is injected so a test can ask the root question without being root, and
    defaults to ``os.geteuid`` looked up at call time.

    A failed write raises ``OSError`` for the caller to report, and leaves no temp file.
    """
    from botocore.exceptions import BotoCoreError, ClientError  # lazy: only a render needs it

    target = Path(config_path) if config_path is not None else default_config_path()
    # Every refusal is recorded in its handler and raised after it, so the exception that
    # reaches the caller carries no ``__context__``.
    refusal = None
    try:
        if (geteuid or os.geteuid)() == 0:
            raise RenderRefused(
                "it runs as root, and a root-owned config.yaml is one the daemon cannot "
                "read; run it as the owner"
            )
        settings = _parse_settings(payload)
        region = _check_settings(settings)
    except RenderRefused as exc:
        refusal = str(exc)
    if refusal is not None:
        raise _refused(target, refusal)

    try:
        backup_target = _check_tag(tag_reader())
    except MetadataUnavailable as exc:
        return RenderResult(
            NO_CREDENTIALS,
            f"no credentials: the instance metadata service did not answer the "
            f"{BACKUP_TARGET_TAG} tag lookup ({exc.detail}), so no parameter was read and "
            f"{target} was left as it was. Retry once the metadata service answers",
        )
    except TagNotServed:
        return RenderResult(
            NO_TAG_YET,
            f"no tag yet: the instance metadata serves no {BACKUP_TARGET_TAG} tag, so no "
            f"parameter was read and {target} was left as it was. Either the tag is not "
            "served yet, which a retry fixes, or the instance has no such tag or its "
            "instance_metadata_tags is disabled, so set both in infra/live/vm.tf and apply",
        )
    except TagLookupFailed as exc:
        return _failed(target, f"the {BACKUP_TARGET_TAG} tag could not be read (HTTP {exc.status})")
    except RenderRefused as exc:
        refusal = str(exc)
    if refusal is not None:
        raise _refused(target, refusal)

    try:
        client = client_factory(region)
    except _MetadataLookupFailed as exc:
        return RenderResult(
            NO_CREDENTIALS,
            f"no credentials: the instance metadata service served none ({exc.detail}), so "
            f"no parameter was read and {target} was left as it was. Retry once the "
            "instance profile serves credentials",
        )
    except (BotoCoreError, ValueError) as exc:
        return _failed(target, f"the SSM client could not be built ({type(exc).__name__})")
    try:
        response = client.get_parameters(
            Names=[name for name, _ in PARAMETERS], WithDecryption=True
        )
    except ClientError as exc:
        return _failed(target, f"the parameters could not be read ({_client_error_code(exc)})")
    except BotoCoreError as exc:
        return _failed(target, f"the parameters could not be read ({type(exc).__name__})")

    try:
        merged = {**settings, **_parameter_values(response), BACKUP_TARGET_KEY: backup_target}
        _check_config(merged)
    except RenderRefused as exc:
        refusal = str(exc)
    if refusal is not None:
        raise _refused(target, refusal)

    state, previous = _existing(target)
    if previous is not None and _same(previous, merged):
        # ``lstat``, so a link that appeared since ``_existing`` looked is not read through.
        mode = os.lstat(target).st_mode & 0o777
        if mode == FILE_MODE:
            return RenderResult(UNCHANGED, f"unchanged: {target} already holds this config")
        os.chmod(target, FILE_MODE)
        return RenderResult(
            TIGHTENED,
            f"tightened: {target} already holds this config, and its mode was {mode:04o}, "
            f"now {FILE_MODE:04o}",
        )
    _write(target, yaml.safe_dump(merged, sort_keys=True))
    return RenderResult(WROTE, _wrote_line(target, state, previous, merged))


def _refused(target: Path, why: str) -> RenderRefused:
    return RenderRefused(f"refused, and {target} was left as it was: {why}")


def _failed(target: Path, why: str) -> RenderResult:
    return RenderResult(FAILED, f"failed: {why}, so {target} was left as it was")


def _wrote_line(
    target: Path,
    state: str,
    previous: Mapping[Any, Any] | None,
    merged: Mapping[str, Any],
) -> str:
    """The line a write prints: the keys that changed, and a restart when ``role`` did."""
    if previous is None:
        line = f"wrote {target}, which was {state}"
        if state == "unreadable":
            line += (
                ". The role it held cannot be read, so restart the daemon if it is running, "
                "since it reads the role only at start"
            )
        elif state == "a symbolic link":
            line += (
                ". The render does not read through a link, so the role it held was not "
                "compared. Restart the daemon if it is running, since it reads the role "
                "only at start"
            )
        return line
    # The old file is compared by the text of its keys, since a hand edit can leave a key
    # YAML reads as a number, and that key must still be named.
    old = {str(key): value for key, value in previous.items()}
    changed = [
        key
        for key in sorted({*old, *merged})
        if (key in old) != (key in merged) or (key in merged and not _same(old[key], merged[key]))
    ]
    line = f"wrote {target}, changing {', '.join(changed)}"
    if ROLE_KEY in changed:
        line += (
            f". The {ROLE_KEY} changed, and the daemon reads it only at start, so restart "
            "the daemon to take it"
        )
    return line


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.vm_config",
        description="Write the hosted VM's config.yaml.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "render",
        help="Write config.yaml from the settings on stdin, four SSM parameters and a tag.",
        description=(
            "Write config.yaml from the settings read on stdin, four SSM parameters and "
            "the instance's marketlake:backup-target tag, as in "
            "'python -m lake.vm_config render < config/vm.yaml'."
        ),
    )
    return parser


def _read_stdin() -> bytes:
    """The settings' bytes from standard input, at most one byte past the limit.

    A closed standard input and a failed read are refusals, as ``lake.roster`` treats
    them. Reading one byte past the limit is enough for :func:`render` to refuse a file
    that is too large without reading all of it.
    """
    stdin = sys.stdin
    if stdin is None:
        raise RenderRefused(
            "standard input is closed; pipe the settings in, as in "
            "'python -m lake.vm_config render < config/vm.yaml'"
        )
    # The refusal is raised after the handler, so the read error is not its context.
    failure = None
    try:
        return stdin.buffer.read(SETTINGS_MAX_BYTES + 1)
    except OSError as exc:
        failure = type(exc).__name__
    raise RenderRefused(f"cannot read the settings from standard input ({failure})")


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.vm_config`` entry. Returns a process exit code.

    It prints one line to stderr and exits 0 for ``wrote``, ``unchanged`` and
    ``tightened``, 3 for ``no credentials`` and ``no tag yet``, 2 for a refusal, and 1
    for any other failure, a failed write or chmod included.
    It builds the real client factory and tag reader itself and takes no seam. Run it as the account
    that runs the daemon.
    """
    _build_parser().parse_args(argv)
    with input_errors_exit("vm_config", RenderRefused):
        try:
            result = render(
                _read_stdin(), client_factory=render_client, tag_reader=read_backup_target_tag
            )
        except OSError as exc:
            print(
                f"vm_config: failed: {default_config_path()} could not be written "
                f"({type(exc).__name__})",
                file=sys.stderr,
            )
            return 1
    print(f"vm_config: {result.line}", file=sys.stderr)
    return EXIT_CODES[result.outcome]


__all__ = [
    "BACKUP_TARGET_KEY",
    "BACKUP_TARGET_TAG",
    "EXIT_CODES",
    "FAILED",
    "FILLED_KEYS",
    "NO_CREDENTIALS",
    "NO_TAG_YET",
    "PARAMETERS",
    "PARAMETER_KEYS",
    "SETTINGS_MAX_BYTES",
    "TIGHTENED",
    "UNCHANGED",
    "WROTE",
    "MetadataUnavailable",
    "RenderRefused",
    "RenderResult",
    "TagLookupFailed",
    "TagNotServed",
    "main",
    "read_backup_target_tag",
    "render",
    "render_client",
]


if __name__ == "__main__":
    raise SystemExit(main())
