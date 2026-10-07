"""The token parameter: the weekly Schwab token, carried to a host with no browser.

The Schwab refresh token dies every seven days, and only an interactive browser login
renews it. ``lake.reauth`` runs that login on the laptop. The hosted VM has no browser,
so its capture depends on a token another machine minted. This module carries the token
there through one SSM Parameter Store parameter, a ``SecureString`` whose value is the
JSON text of ``token.json`` (marketlake #636). The design's Auth section carries the
reasoning.

marketlake #633 is what makes one token on two hosts safe. A refresh issued a new access
token on each host and left the refresh token unchanged, and a refresh on one host did
not revoke the other. So each host refreshes its own access token locally, the parameter
only ever holds what a re-auth minted, and nothing writes back to it but the re-auth.

Three pieces live here.

1. ``mode_of`` reads the per-host ``token_store`` key. ``file`` keeps the token in
   ``token.json`` alone. ``both`` and ``store`` make the re-auth put the parameter after
   it writes the file. ``store`` and any other value make the VM's scheduled pulls run,
   which marketlake #702 adds. An unknown value prints one line and falls to the side
   that cannot cost a token: the re-auth still writes the file and tries the put.
2. ``push`` is the re-auth's put. It sends the JSON text the login wrote, as a string,
   never a read-back of the file. It signs only as the role ``token_store_role_arn``
   names, which may only put this one parameter, in ``token_store_region``. The command
   key assumes that role, as the session ``marketlake-token-put`` (marketlake #737), and
   the put never falls back to the ``bucket_*`` keys.
3. ``pull`` is the VM's read, run as ``python -m lake.token_store pull``. It follows
   ``bucket_credentials``, which is the instance profile on the VM, and writes
   ``token.json`` only when the local file is absent, unreadable, or minted earlier
   than the parameter's token. So a re-run writes nothing, and a later local token is
   never replaced by an older one. It writes nothing at all when the parameter's mint
   time is more than an hour past the VM's clock, whatever the local file holds, so a
   forged far-future put cannot outrank every later re-auth.

**No token byte reaches any output.** An AWS ``ClientError`` is reported by its error
code alone, because a ``ValidationException`` message can echo its input. A
``BotoCoreError`` is reported by its type name alone, because botocore's client-side
``ParamValidationError`` prints the parameter's value. A failed ``AssumeRole`` arrives
as ``_AssumeRoleFailed``, which carries a code and never STS's message, since that
names the account and the principal.

The client is a seam. It reaches AWS, so ``push`` takes the client and ``pull`` takes a
factory for it, and neither defaults one. ``main`` builds the real one itself and accepts
none, which is the rule ``tests/unit/test_seam_defaults`` states for every entry in this
package. This module imports nothing that loads ``lake.bucket`` or ``lake.manifest``,
because the re-auth imports it. ``pull`` imports ``lake.control_plane`` inside the
function for that reason.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from lake.aws_session import (
    TOKEN_PUT_SESSION_NAME,
    AssumeRole,
    KeyPair,
    _AssumeRoleFailed,
    _MetadataLookupFailed,
    build_client,
    source_from_bucket_credentials,
)
from lake.clock import Clock, SystemClock
from lake.config import (
    BUCKET_REGION_KEY,
    COMMAND_KEY_ID_KEY,
    COMMAND_SECRET_KEY,
    TOKEN_PUT_KEYS,
    TOKEN_STORE_FILE,
    TOKEN_STORE_KEY,
    TOKEN_STORE_REGION_KEY,
    TOKEN_STORE_ROLE_ARN_KEY,
    Config,
    ConfigError,
    bucket_credential_problems,
    input_errors_exit,
    is_region_name,
    load_config,
    role_arn_problems,
)
from lake.paths import default_token_path
from lake.token_epoch import epoch_second_to_utc

# The parameter's name. marketlake #699 grants the VM's instance role read on it, and
# marketlake #737 grants the token-writer role write on it, and nothing else.
PARAMETER_NAME = "/marketlake/config/schwab-oauth-token"

# The standard tier's limit on a parameter's value, in bytes. ``token.json`` is about 800.
MAX_VALUE_BYTES = 4096

# How far past the VM's clock a parameter's mint time may sit before the pull refuses it.
# The VM never logs in, so every mint time it sees was stamped by the laptop's clock, and
# an hour covers any skew between the two. A mint time beyond it is a forged or broken put.
FUTURE_MINT_ALLOWANCE = timedelta(hours=1)

# The three values ``token_store`` names, and what any other value falls to.
FILE = TOKEN_STORE_FILE
BOTH = "both"
STORE = "store"
UNRECOGNISED = "unrecognised"

# The SSM client's settings. The timeouts bound a stalled socket, and the retries are
# botocore's standard ones.
_SSM_CLIENT_CONFIG = {
    "connect_timeout": 10,
    "read_timeout": 30,
    "retries": {"mode": "standard", "max_attempts": 3},
}


def mode_of(config: Config) -> tuple[str, str | None]:
    """The host's token mode, and the line to print when the key held none of the three.

    An absent key is ``file``. A present key is ``file``, ``both`` or ``store`` only when
    it is exactly that string. Any other value is ``UNRECOGNISED``, which writes the file,
    tries the put when the credentials allow, and lets the pulls run, so a mistyped value
    never costs a token. The line names the value read, as ``outbox.role_of`` does.
    """
    value = config.token_store
    if value in (FILE, BOTH, STORE):
        return value, None
    return UNRECOGNISED, (
        f"{TOKEN_STORE_KEY} {value!r} is not {FILE!r}, {BOTH!r} or {STORE!r}, so it is "
        "read as the side that cannot cost a token: a re-auth writes token.json and then "
        f"puts the token parameter if {COMMAND_KEY_ID_KEY}, {COMMAND_SECRET_KEY}, "
        f"{TOKEN_STORE_ROLE_ARN_KEY} and {TOKEN_STORE_REGION_KEY} allow it, and the "
        "scheduled pulls run"
    )


def credential_problems(config: Config) -> list[str]:
    """What is wrong with the put's four keys, as operator phrases.

    The command key's two values, ``token_store_role_arn`` and ``token_store_region``
    must be present. The role ARN must have a role ARN's shape, and the region the shape
    ``require_bucket_settings`` checks ``bucket_region`` for. A refusal of the ARN never
    quotes it, because it carries the account id. The ``bucket_*`` keys play no part, because
    the backup's key holds no grant to put the parameter. The re-auth runs this before
    the browser opens, so no ``ConfigError`` can reach its put after a token is written.
    """
    values = (
        config.command_access_key_id,
        config.command_secret_access_key,
        config.token_store_role_arn,
        config.token_store_region,
    )
    absent = [key for key, value in zip(TOKEN_PUT_KEYS, values, strict=True) if value is None]
    problems = []
    if absent:
        problems.append(f"the token parameter's put needs config key(s): {absent}")
    problems.extend(role_arn_problems(TOKEN_STORE_ROLE_ARN_KEY, config.token_store_role_arn))
    region = config.token_store_region
    if region is not None and not is_region_name(region):
        problems.append(
            f"{TOKEN_STORE_REGION_KEY} {region!r} is not an AWS region name like us-east-2"
        )
    return problems


def push_client(config: Config) -> Any:
    """An SSM client for the re-auth's put, signed as the role ``token_store_role_arn`` names.

    The command key assumes the role, as the session ``marketlake-token-put``, at the
    put's first request, and no STS call is made here. The caller has run
    :func:`credential_problems` first, so every key is present.
    """
    problems = credential_problems(config)
    key_id, secret_key = config.command_access_key_id, config.command_secret_access_key
    role_arn = config.token_store_role_arn
    if problems or key_id is None or secret_key is None or role_arn is None:
        raise ConfigError(". ".join(problems))
    return build_client(
        "ssm",
        region=config.token_store_region,
        source=AssumeRole(role_arn, KeyPair(key_id, secret_key), TOKEN_PUT_SESSION_NAME),
        client_config=_SSM_CLIENT_CONFIG,
    )


class PushFailed(Exception):
    """The put did not update the parameter. ``code`` names why, and never the value.

    ``code`` is the AWS error code, the type name of a ``BotoCoreError``, or
    ``TooLarge`` for a value refused before the call. ``detail`` is ``ASSUMING`` when the
    failure came from STS, as the command key assumed the role, rather than from SSM.
    ``transient`` says an STS failure is likely to pass on its own.
    """

    def __init__(self, code: str, detail: str | None = None, *, transient: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail
        self.transient = transient


# What ``PushFailed.detail`` holds when the put failed while assuming the role.
ASSUMING = "AssumeRole"

# The error codes that mean the role cannot put the parameter. SSM returns these for a
# principal without the grant, which after a good assume means the role in
# ``token_store_role_arn`` or a ``token_store_region`` its grant does not name.
_DENIED_CODES = frozenset({"AccessDenied", "AccessDeniedException"})
# SSM's codes for an unknown key id and a wrong signature, per AWS's list of common
# errors. After a good assume they mean the session STS just issued did not work, which
# a second run repairs. They arrive as HTTP 400, so no status-code fallback would catch
# them.
_UNKNOWN_KEY_CODES = frozenset({"UnrecognizedClientException", "InvalidSignatureException"})

TOO_LARGE = "TooLarge"


def push(*, client: Any, text: str) -> int:
    """Put ``text`` into the token parameter and return the parameter's new version.

    ``text`` is the JSON text the re-auth's token writer wrote, kept as a string, because
    botocore types ``Value`` as one. ``Tier`` is pinned, so an account whose default tier
    is Advanced does not create a billed parameter. No ``Tags`` are sent, which AWS
    refuses beside ``Overwrite``, and no ``KeyId``, so the AWS-managed key applies.

    A value over the standard tier's 4,096 bytes is refused before the call. Every
    failure raises ``PushFailed`` carrying a code and nothing of the value.
    """
    from botocore.exceptions import BotoCoreError, ClientError  # lazy: only a put needs it

    size = len(text.encode("utf-8"))
    if size > MAX_VALUE_BYTES:
        raise PushFailed(TOO_LARGE, f"the token is {size} bytes, over {MAX_VALUE_BYTES}")
    # The code is taken inside the handler and raised after it, so the AWS error, whose
    # message can carry the token, is on neither ``__cause__`` nor ``__context__``.
    failed: PushFailed | None = None
    try:
        response = client.put_parameter(
            Name=PARAMETER_NAME,
            Value=text,
            Type="SecureString",
            Overwrite=True,
            Tier="Standard",
        )
    except _AssumeRoleFailed as exc:
        failed = PushFailed(exc.code, ASSUMING, transient=exc.transient)
    except ClientError as exc:
        failed = PushFailed(_client_error_code(exc))
    except BotoCoreError as exc:
        failed = PushFailed(type(exc).__name__)
    if failed is not None:
        raise failed
    return int(response["Version"])


def push_failure_line(token_path: Path, failure: PushFailed) -> str:
    """The one line a failed put prints: the token's path, the code, and the fix.

    The fix depends on which step failed and on the code.

    1. STS refused the assume, ``InvalidClientTokenId`` and ``SignatureDoesNotMatch``
       included. The command key, or the policy and trust that let it assume the role,
       is fixed in ``config.yaml`` or in AWS, and a second login would fail the same way,
       so the line says so. A new key may simply not be active yet.
    2. SSM denied the put after a good assume. The role in ``token_store_role_arn`` has
       no grant for this parameter in ``token_store_region``, and the grant names
       us-east-1 only.
    3. SSM did not accept the session STS had just issued, which a second run repairs.

    Anything else, a transient STS failure included, is fixed by running ``reauth.sh``
    again, the ritual's practised repair.
    """
    command = f"{COMMAND_KEY_ID_KEY} and {COMMAND_SECRET_KEY}"
    assuming = failure.detail == ASSUMING
    shown = f"{ASSUMING} {failure.code}" if assuming else failure.code
    head = f"{token_path} was written and the token parameter was not updated ({shown})."
    if assuming and not failure.transient:
        fix = (
            f"STS refused to let the key in {command} assume the role in "
            f"{TOKEN_STORE_ROLE_ARN_KEY}: the key is unknown, deactivated or wrong, or "
            "marketlake-command's policy or the role's trust does not allow it. A key made "
            "minutes ago may not be active yet. A second login fails the same way until "
            "the assume works"
        )
    elif assuming:
        fix = "Run reauth.sh again"
    elif failure.code in _DENIED_CODES:
        fix = (
            f"The role in {TOKEN_STORE_ROLE_ARN_KEY} has no grant to put it in "
            f"{TOKEN_STORE_REGION_KEY}. The grant names the parameter in us-east-1 only, so "
            "check both. A second login fails the same way until they are fixed"
        )
    elif failure.code in _UNKNOWN_KEY_CODES:
        fix = "SSM did not accept the session STS had just issued for the role. Run reauth.sh again"
    elif failure.code == TOO_LARGE:
        fix = f"{failure.detail}, which a standard parameter cannot hold"
    else:
        fix = "Run reauth.sh again"
    return f"{head} {fix}"


def skipped_push_line(token_path: Path, problems: Sequence[str]) -> str:
    """The one line a put skipped under an unknown ``token_store`` value prints.

    It names the config problem as the fix, because re-running alone would skip the put
    again.
    """
    return (
        f"{token_path} was written and the token parameter was not updated, because "
        f"{'. '.join(problems)}. Fix config.yaml, then run reauth.sh again"
    )


def _client_error_code(exc: Any) -> str:
    """The AWS error code a ``ClientError`` carries, and never its message."""
    response = getattr(exc, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return str(code) if code else type(exc).__name__


# -- the pull ---------------------------------------------------------------------

WROTE = "wrote"
CURRENT = "current"
STORE_OLDER = "store older"
UNREADABLE = "unreadable"
NO_CREDENTIALS = "no credentials"

# The exit code each outcome gives. A credential delay exits 3, so the first boot's retry
# can tell it from a parameter that is empty or older, which exits 1. A config problem
# exits 2 through ``input_errors_exit``.
EXIT_CODES = {WROTE: 0, CURRENT: 0, STORE_OLDER: 1, UNREADABLE: 1, NO_CREDENTIALS: 3}


@dataclass(frozen=True)
class PullResult:
    """What a pull did, and the one line it prints. Neither carries the token."""

    outcome: str
    line: str


def pull_client(config: Config) -> Any:
    """An SSM client for the pull, signed the way ``bucket_credentials`` says.

    On the VM that is the instance profile. The checks are the bucket's credential
    checks plus the region's shape, and each refuses as a ``ConfigError``, which the
    command turns into exit 2. A metadata service that returns no credentials raises
    ``_MetadataLookupFailed``, which :func:`pull` reports as ``no credentials``.
    """
    from botocore.exceptions import BotoCoreError  # lazy: only a pull needs it

    problems = bucket_credential_problems(config)
    region = config.bucket_region
    if region is not None and not is_region_name(region):
        problems.append(f"{BUCKET_REGION_KEY} {region!r} is not an AWS region name like us-east-2")
    if problems:
        raise ConfigError(". ".join(problems))
    try:
        return build_client(
            "ssm",
            region=region,
            source=source_from_bucket_credentials(config),
            client_config=_SSM_CLIENT_CONFIG,
        )
    except (BotoCoreError, ValueError) as exc:
        # Only the type is named, because a message may quote a value from the config.
        raise ConfigError(
            f"the token parameter's client could not be built from config.yaml "
            f"({type(exc).__name__})"
        ) from None


def stored_mint(text: object) -> tuple[datetime, dict[str, Any]]:
    """The mint time of a token parameter's value, and the parsed token, or ``ValueError``.

    The value must be JSON holding ``creation_timestamp``, read through ``token_epoch``
    as every other reader of the field does, and a ``token`` holding non-empty
    ``access_token`` and ``refresh_token`` strings. A value missing either would write a
    file capture cannot start from, so it is refused before anything is written. No
    message carries any part of the value.
    """
    if not isinstance(text, str) or not text:
        raise ValueError("the parameter is empty")
    try:
        payload = json.loads(text)
    except ValueError:
        raise ValueError("the parameter is not JSON") from None
    if not isinstance(payload, dict) or "creation_timestamp" not in payload:
        raise ValueError("the parameter has no creation_timestamp")
    minted = epoch_second_to_utc(payload["creation_timestamp"])
    token = payload.get("token")
    if not isinstance(token, dict):
        raise ValueError("the parameter has no token")
    for field in ("access_token", "refresh_token"):
        value = token.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"the parameter's token has no {field}")
    return minted, payload


def _when(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M UTC")


def pull(
    *,
    client_factory: Callable[[], Any],
    token_path: Path | str,
    clock: Clock,
) -> PullResult:
    """Copy the token parameter into ``token_path`` when it is newer, and say what happened.

    A parameter whose mint time is more than ``FUTURE_MINT_ALLOWANCE`` past
    ``clock.now()`` is refused first, as ``unreadable``, and nothing is written, even over
    an absent file. A forged put with a far-future mint time would otherwise outrank every
    later re-auth, since the pull writes only a strictly later mint. The price is that the
    comparison now reads the VM's clock, and the hour of allowance absorbs any skew it
    has. The clock is injected, as everywhere in this package, so a test decides the time.

    Then the rule is the issue's, in this order.

    1. A local file that is absent is written. Absence is tested first, because
       ``control_plane.read_token_mint`` raises ``ValueError`` for absence as well.
    2. A local file that cannot be read, or holds no mint time, is written.
    3. A parameter minted strictly later than the local file is written. An equal mint
       time writes nothing, so a re-run is free, and an earlier one writes nothing,
       because then the local file is the live one.

    ``schwab-py`` writes the original ``creation_timestamp`` back unchanged on every
    access-token refresh, so the comparison is between re-auths, not refreshes. The write
    goes through ``reauth.write_token``, which keeps mode 0600 and the atomic replace.

    The client is built through ``client_factory`` inside this function, so a metadata
    service that has no credentials yet becomes the ``no credentials`` outcome. A config
    problem in the build raises ``ConfigError`` for the caller to report. A host set to
    ``bucket_credentials: assume_role`` signs the pull as the bucket's role, which reads
    no parameter, and a role that cannot be assumed at all is ``unreadable`` too, rather
    than a traceback.
    """
    from botocore.exceptions import BotoCoreError, ClientError  # lazy: only a pull needs it

    from lake.control_plane import read_token_mint  # lazy: it loads lake.manifest
    from lake.reauth import write_token  # lazy: lake.reauth imports this module

    target = Path(token_path)
    try:
        client = client_factory()
    except _MetadataLookupFailed as exc:
        return PullResult(
            NO_CREDENTIALS,
            f"no credentials came from the instance metadata service ({exc.detail}), so "
            f"the token parameter was not read and {target} was left as it was. Retry "
            "once the instance profile serves credentials",
        )
    try:
        response = client.get_parameter(Name=PARAMETER_NAME, WithDecryption=True)
    except _AssumeRoleFailed as exc:
        return _unreadable(target, f"AssumeRole {exc.code}")
    except ClientError as exc:
        return _unreadable(target, _client_error_code(exc))
    except BotoCoreError as exc:
        return _unreadable(target, type(exc).__name__)
    parameter = response.get("Parameter") if isinstance(response, dict) else None
    value = parameter.get("Value") if isinstance(parameter, dict) else None
    try:
        minted, payload = stored_mint(value)
    except ValueError as exc:
        return _unreadable(target, str(exc))
    if minted > clock.now() + FUTURE_MINT_ALLOWANCE:
        return _unreadable(target, "its mint time is in the future")

    if not target.exists():
        reason = "absent"
    else:
        try:
            local = read_token_mint(target)
        except ValueError:
            reason = "unreadable"
        else:
            if minted == local:
                return PullResult(
                    CURRENT,
                    f"{target} is current: it and the token parameter were both minted "
                    f"{_when(minted)}",
                )
            if minted < local:
                return PullResult(
                    STORE_OLDER,
                    f"{target} was minted {_when(local)}, later than the token parameter's "
                    f"{_when(minted)}, so nothing was written",
                )
            reason = f"minted {_when(local)}"
    write_token(target, payload)
    return PullResult(
        WROTE,
        f"wrote {target} from the token parameter, minted {_when(minted)}. The local file "
        f"was {reason}",
    )


def _unreadable(target: Path, why: str) -> PullResult:
    return PullResult(
        UNREADABLE,
        f"the token parameter could not be used ({why}), so {target} was left as it was",
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.token_store",
        description="Copy the Schwab token between token.json and its SSM parameter.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    pull_parser = commands.add_parser(
        "pull",
        help="Write token.json from the token parameter when the parameter is newer.",
    )
    pull_parser.add_argument(
        "--config", help="Path to config.yaml (defaults to the standard location)."
    )
    pull_parser.add_argument(
        "--token", help="Path to token.json (defaults to the standard location)."
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.token_store`` entry. Returns a process exit code.

    ``pull`` prints one line to stderr and exits 0 for ``wrote`` and ``current``, 1 for
    ``store older`` and ``unreadable``, and 3 for ``no credentials``. A config problem
    exits 2 with one line. It builds the real client itself and takes no seam. It does
    not read ``token_store``: running it is the decision, and the gate belongs to the
    scheduled calls that marketlake #702 adds. It reads no ``role`` either, so it runs on
    a shadow host, as the VM's first boot is.

    Run it as the account that runs the daemon. ``os.replace`` keeps the temp file's
    owner, so a run as root leaves a token the daemon cannot read.
    """
    args = _build_parser().parse_args(argv)
    token_path = Path(args.token).expanduser() if args.token else default_token_path()
    with input_errors_exit("token_store"):
        config = load_config(args.config)
        try:
            result = pull(
                client_factory=lambda: pull_client(config),
                token_path=token_path,
                clock=SystemClock(),
            )
        except OSError as exc:
            print(
                f"token_store: {token_path} could not be written ({type(exc).__name__})",
                file=sys.stderr,
            )
            return 1
    print(f"token_store: {result.line}", file=sys.stderr)
    return EXIT_CODES[result.outcome]


__all__ = [
    "ASSUMING",
    "BOTH",
    "CURRENT",
    "EXIT_CODES",
    "FUTURE_MINT_ALLOWANCE",
    "FILE",
    "MAX_VALUE_BYTES",
    "NO_CREDENTIALS",
    "PARAMETER_NAME",
    "STORE",
    "STORE_OLDER",
    "UNREADABLE",
    "UNRECOGNISED",
    "WROTE",
    "PullResult",
    "PushFailed",
    "credential_problems",
    "main",
    "mode_of",
    "pull",
    "pull_client",
    "push",
    "push_client",
    "push_failure_line",
    "skipped_push_line",
    "stored_mint",
]


if __name__ == "__main__":
    raise SystemExit(main())
