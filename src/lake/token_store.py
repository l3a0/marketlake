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
   never a read-back of the file, and it signs with the three ``token_store_*`` keys
   alone. Those hold the key of an IAM user that may only put this one parameter, and
   they never fall back to the ``bucket_*`` keys.
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
``ParamValidationError`` prints the parameter's value.

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
    KeyPair,
    _MetadataLookupFailed,
    build_client,
    source_from_bucket_credentials,
)
from lake.clock import Clock, SystemClock
from lake.config import (
    BUCKET_REGION_KEY,
    TOKEN_STORE_FILE,
    TOKEN_STORE_KEY,
    TOKEN_STORE_KEY_ID_KEY,
    TOKEN_STORE_KEYS,
    TOKEN_STORE_REGION_KEY,
    TOKEN_STORE_SECRET_KEY,
    Config,
    ConfigError,
    bucket_credential_problems,
    input_errors_exit,
    is_region_name,
    load_config,
)
from lake.paths import TOKEN_FILE, config_dir
from lake.token_epoch import epoch_second_to_utc

# The parameter's name. marketlake #699 grants the VM's instance role read on it and the
# put-only user write on it, and nothing else.
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
        f"puts the token parameter if the {TOKEN_STORE_KEY}_* keys allow it, and the "
        "scheduled pulls run"
    )


def credential_problems(config: Config) -> list[str]:
    """What is wrong with the put's three keys, as operator phrases.

    Both key values and the region must be present, and the region must have the shape
    ``require_bucket_settings`` checks ``bucket_region`` for. The ``bucket_*`` keys play
    no part, because the backup's key holds no grant to put the parameter.
    """
    values = (
        config.token_store_access_key_id,
        config.token_store_secret_access_key,
        config.token_store_region,
    )
    absent = [key for key, value in zip(TOKEN_STORE_KEYS, values, strict=True) if value is None]
    problems = []
    if absent:
        problems.append(f"the token parameter's put needs config key(s): {absent}")
    region = config.token_store_region
    if region is not None and not is_region_name(region):
        problems.append(
            f"{TOKEN_STORE_REGION_KEY} {region!r} is not an AWS region name like us-east-2"
        )
    return problems


def push_client(config: Config) -> Any:
    """An SSM client signed with the ``token_store_*`` keys, for the re-auth's put.

    The caller has run :func:`credential_problems` first, so all three are present.
    """
    key_id, secret_key = config.token_store_access_key_id, config.token_store_secret_access_key
    if key_id is None or secret_key is None:
        raise ConfigError(". ".join(credential_problems(config)))
    return build_client(
        "ssm",
        region=config.token_store_region,
        source=KeyPair(key_id, secret_key),
        client_config=_SSM_CLIENT_CONFIG,
    )


class PushFailed(Exception):
    """The put did not update the parameter. ``code`` names why, and never the value.

    ``code`` is the AWS error code, the type name of a ``BotoCoreError``, or
    ``TooLarge`` for a value refused before the call.
    """

    def __init__(self, code: str, detail: str | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail


# The error codes that mean the key in the ``token_store_*`` keys is the wrong one. SSM
# returns the first two for a key without the grant, and the last two for an unknown or
# deactivated key id and a wrong secret, per AWS's list of common errors. The last two
# arrive as HTTP 400, so no status-code fallback would catch them.
_DENIED_CODES = frozenset({"AccessDenied", "AccessDeniedException"})
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
    failed: str | None = None
    try:
        response = client.put_parameter(
            Name=PARAMETER_NAME,
            Value=text,
            Type="SecureString",
            Overwrite=True,
            Tier="Standard",
        )
    except ClientError as exc:
        failed = _client_error_code(exc)
    except BotoCoreError as exc:
        failed = type(exc).__name__
    if failed is not None:
        raise PushFailed(failed)
    return int(response["Version"])


def push_failure_line(token_path: Path, failure: PushFailed) -> str:
    """The one line a failed put prints: the token's path, the code, and the fix.

    The fix depends on the code. A refused or unknown key is fixed in ``config.yaml``,
    and a second login would fail the same way, so the line says so. Anything else is
    fixed by running ``reauth.sh`` again, the ritual's practised repair.
    """
    keys = f"{TOKEN_STORE_KEY_ID_KEY} and {TOKEN_STORE_SECRET_KEY}"
    head = f"{token_path} was written and the token parameter was not updated ({failure.code})."
    if failure.code in _DENIED_CODES:
        fix = (
            f"The key in {keys} has no grant to put it. They must hold the key of the "
            "user that may only put the token parameter, not the backup's key. A second "
            "login fails the same way until they do"
        )
    elif failure.code in _UNKNOWN_KEY_CODES:
        fix = (
            f"AWS does not accept the key in {keys}: the key id is unknown or "
            "deactivated, or the secret is wrong. A key created a minute ago may not be "
            "active yet. A second login fails the same way until the key works"
        )
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
    problem in the build raises ``ConfigError`` for the caller to report.
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


def _default_token_path() -> Path:
    """The standard token path, resolved when the command runs rather than at import.

    So a ``MARKETLAKE_CONFIG_DIR`` set for the process is honoured wherever this module
    was imported. marketlake #715 moves the package to one call-time helper, and this is
    the one line that switches to it.
    """
    return config_dir() / TOKEN_FILE


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
    token_path = Path(args.token).expanduser() if args.token else _default_token_path()
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
