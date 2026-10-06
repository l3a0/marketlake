"""The re-auth command: one browser login, one atomically written token.

``python -m lake.reauth`` runs the Sunday ritual the design's Auth section pins. Schwab's
refresh token dies every seven days and an interactive browser login is its only renewal,
so this is a standing weekly task rather than a one-time setup step. Every other install
step is rendered by ``lake.control_plane`` and run from a script that says what it does.
This one is rendered beside them as ``reauth.sh``, which calls this module.

The work is four steps.

1. Read the two Schwab app credentials and the registered callback URL from
   ``config.yaml``.
2. Resolve the token path.
3. Run ``schwab-py``'s login flow with a token writer of this module's own.
4. Under ``token_store: both`` or ``store``, put the token this run wrote into the token
   parameter, the SSM ``SecureString`` the hosted VM pulls it from (marketlake #636).
   ``lake.token_store`` holds the put and says what each ``token_store`` value does.

Four rules shape the module.

1. *It refuses when stdin is not a terminal.* A login flow needs a person and a browser.
   ``client_from_login_flow`` waits ``callback_timeout=300.0`` seconds for the callback,
   so wired into launchd with no browser to answer it the job would hang five minutes and
   then fail, which reads as a broken job rather than a misuse of one. The refusal is
   enforcement rather than a comment in the rendered header. It comes before the login,
   so the tool costs no browser session where it cannot work. Loading ``config.yaml``
   comes before it, and so does the check of the put's three ``token_store_*`` keys
   under ``both`` and ``store``, which refuses with exit 2 before the browser opens.
2. *The token write is atomic.* ``schwab-py`` writes the token with ``open(token_path,
   'w')``, which truncates the file before the new contents exist. A failure in between
   destroys a working token, and it lands on the one file capture cannot start without.
   This repo's rule is the opposite, so the write goes to a temp file beside the target,
   is flushed, and is published by one ``os.replace``. ``client_from_login_flow`` takes a
   ``token_write_func``, which is the hook that routes its write through this one.
3. *It prints no secret.* The report carries the token path, whether a token landed, and
   the callback URL. The callback is a loopback address rather than a credential, and
   printing it is how the operator checks it against the Schwab app registration. The API
   key and the app secret never reach the report or a log line, and neither does any byte
   of the token or of an AWS error's message.
4. *The file comes first, and the put sends only what this run wrote.* The token is on
   disk whatever the put does next. The put runs only when the writer says a token
   landed, and it sends the JSON text the writer wrote rather than reading the file
   back, because a refresh that read the old file could land in between.

Because the write is safe, overwriting a still-valid token is allowed and is the point. It
costs one login and yields a fresher token, and the Sunday coverage assertion tests
freshness rather than validity. A refuse-by-default guard was considered and dropped: it
would be friction protecting against nothing once a torn write is impossible.

The login stays on the laptop, and the hosted VM gets the token through the token
parameter. The headless variant, ``client_from_manual_flow``, takes the same arguments
and prints a URL rather than driving a browser. It stays deferred, because run over SSH
every Sunday it would turn the ritual into a remote session and a pasted redirect URL,
and the token parameter needs neither. The design doc's Auth section carries the
reasoning.

The login flow is a seam. It reaches a browser, a local callback server, and Schwab, so
``reauth`` and ``reauth_from_config`` require it and never default one. The token
parameter's client is one too, and ``reauth_from_config`` requires a factory for it
rather than a built client, because under ``file`` no client can be built. ``main``
builds the real ones itself and accepts neither, which is the rule
``tests/unit/test_seam_defaults`` states for every entry in this package.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from lake import token_store
from lake.config import CALLBACK_KEY, Config, ConfigError, input_errors_exit, load_config
from lake.paths import TOKEN_FILE, config_dir, temp_write_path

# The standard location of the Schwab token, per the design's Configuration section. The
# same home-relative default ``lake.schwab`` reads from, spelled through ``lake.paths``
# so this module needs nothing from the vendor layer.
# The path is fixed when this module is imported, so a process that sets HOME or
# MARKETLAKE_CONFIG_DIR afterwards still resolves the real directory. marketlake #715
# will resolve it at call time instead.
DEFAULT_TOKEN_PATH = config_dir() / TOKEN_FILE

# The token is a full brokerage credential, so it is written owner-read-write and nothing
# else. The design pins ``chmod 600`` on this file. The mode goes on the temp file before
# the rename, so the token is never briefly readable at its real path.
TOKEN_MODE = 0o600


class ReauthError(Exception):
    """Raised when a re-auth cannot proceed: no terminal, or no callback URL."""


class LoginFlow(Protocol):
    """The vendor login flow, as this module calls it.

    ``schwab.auth.client_from_login_flow`` satisfies this, and so does a test fake. The
    four positional arguments are that function's own first four, and ``token_write_func``
    is the hook the atomic write rides in on.
    """

    def __call__(
        self,
        api_key: str,
        app_secret: str,
        callback_url: str,
        token_path: str,
        *,
        token_write_func: Callable[..., None],
    ) -> object:
        """Drive the login and write the resulting token through ``token_write_func``."""
        ...


@dataclass(frozen=True)
class ReauthReport:
    """What a re-auth did, for the sign-off block.

    ``token_written`` says this run wrote a token, taken from the writer rather than from
    the filesystem. The two answers differ exactly where it matters: the weekly ritual
    always runs over a token that is already there, so a flow that returns without writing
    would read as a success if the report asked whether a file exists.
    ``replaced_existing`` says a token was already there and this run wrote over it, which
    is allowed and wanted rather than a warning. No field carries a secret.

    ``parameter_version`` is the token parameter's version after a put that succeeded,
    and ``None`` when no put ran. The laptop has no grant to read the parameter, so that
    number is the operator's only proof at the terminal that it changed.
    ``store_problem`` is the one line for a token that was written to the file and not to
    the parameter, which ``main`` prints and exits 3 on.
    """

    token_path: Path
    callback_url: str
    token_written: bool
    replaced_existing: bool
    parameter_version: int | None = None
    store_problem: str | None = None

    def render(self) -> str:
        """A human-readable sign-off block. It names no secret."""
        landed = "yes" if self.token_written else "no"
        if self.token_written and self.replaced_existing:
            landed = "yes (replaced the previous token)"
        lines = [
            "Schwab re-auth",
            f"  callback url:  {self.callback_url}",
            f"  token path:    {self.token_path}",
            f"  token landed:  {landed}",
        ]
        if self.parameter_version is not None:
            lines.append(
                f"  parameter:     {token_store.PARAMETER_NAME} version {self.parameter_version}"
            )
        return "\n".join(lines)


def write_token(token_path: Path | str, payload: object) -> str:
    """Write the token to ``token_path`` atomically, and return the JSON text written.

    The write is a temp file, a flush, and one rename.

    This is the convention ``compact._write_partition`` and every other writer in this
    package follows, and it is the one ``schwab-py`` does not. A crash or a full disk
    part-way through leaves the prior token intact at the real path and a temp file
    beside it, rather than a truncated file where the credential used to be. The temp
    name comes from ``paths.temp_write_path``, which owns the one spelling of the marker
    the backup exclusion matches, and carries the writing process's id, so two processes
    never share one. Two threads in one process would. The daemon's writes come from token
    refreshes, and ``lake.schwab.serialize_token_refresh`` makes those one at a time across
    the process, which is what keeps that safe.

    The payload is serialised to a string before any file is opened. So a value JSON
    cannot encode fails before the write starts rather than half-way through it. That
    string is what the re-auth puts into the token parameter, so the parameter holds
    exactly the bytes this write landed and never a read-back of the file.
    """
    target = Path(token_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload)
    tmp = temp_write_path(target, os.getpid())
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, TOKEN_MODE)
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return text


class TokenWriter:
    """The ``token_write_func`` ``schwab-py`` calls, writing atomically to a path.

    ``schwab-py`` wraps whatever it is given and calls it as ``func(token, *args,
    **kwargs)``, so the extra arguments are accepted and ignored. Only the token matters
    here, and it arrives already wrapped in the library's metadata envelope, which is
    what ``creation_timestamp`` is read back off later.

    ``wrote`` records that a token was actually written, and it is what the report's
    "token landed" reads. A file sitting at the path is a different fact. The ritual runs
    every week over a token that is already there, so asking the filesystem would report
    last week's token as this week's and hand a failed login an exit code of zero.

    ``text`` keeps the JSON text of the last write, which is what the re-auth's put
    sends. It is ``None`` until a write lands.
    """

    def __init__(self, token_path: Path | str) -> None:
        self.token_path = Path(token_path)
        self.wrote = False
        self.text: str | None = None

    def __call__(self, token: object, *args: object, **kwargs: object) -> None:
        self.text = write_token(self.token_path, token)
        self.wrote = True


def token_writer(token_path: Path | str) -> TokenWriter:
    """A fresh ``TokenWriter`` for one run. One per login, because ``wrote`` is per run."""
    return TokenWriter(token_path)


def reauth(
    *,
    api_key: str,
    app_secret: str,
    callback_url: str | None,
    token_path: Path | str,
    login_flow: LoginFlow,
    stdin_is_tty: bool,
) -> ReauthReport:
    """Run one browser login and land its token, returning the sign-off report.

    ``stdin_is_tty`` is the caller's answer to whether a person is at a terminal. It is a
    value rather than a seam, so a test states it outright. ``main`` reads it from
    ``sys.stdin``. A false answer refuses before the flow is called at all, which is what
    keeps a launchd job from sitting out the callback timeout with no browser to answer
    it.

    A missing callback URL refuses too, naming the key and the file it belongs in. That
    refusal lives here rather than in the config loader, because capture never reads the
    key and a loader that required it would take the daemon down for a value only this
    command uses. A value that is only whitespace counts as missing. ``load_config``
    already normalises one to ``None``, and this catches the same thing for a caller
    reaching here with a value from anywhere else.
    """
    report, _ = _login(
        api_key=api_key,
        app_secret=app_secret,
        callback_url=_refuse_unless_ready(callback_url, stdin_is_tty),
        token_path=token_path,
        login_flow=login_flow,
    )
    return report


def _refuse_unless_ready(callback_url: str | None, stdin_is_tty: bool) -> str:
    """The callback URL, once the two refusals ``reauth`` describes have both passed.

    The terminal check comes first, so a launchd job is told what it is before it is
    told about a key.
    """
    if not stdin_is_tty:
        raise ReauthError(
            "re-auth needs a person at a terminal and a browser, and stdin is not a "
            "terminal. It cannot run from launchd or any other unattended job. Run it "
            "yourself from a shell."
        )
    if callback_url is None or not callback_url.strip():
        raise ReauthError(
            f"no {CALLBACK_KEY} in config.yaml. Add the callback URL registered on the "
            "Schwab app, such as https://127.0.0.1:8182, and run this again."
        )
    return callback_url


def _login(
    *,
    api_key: str,
    app_secret: str,
    callback_url: str,
    token_path: Path | str,
    login_flow: LoginFlow,
) -> tuple[ReauthReport, TokenWriter]:
    """Run the login flow, and return the report with the writer that recorded the write.

    The writer goes back to ``reauth_from_config`` alongside the report, because its
    ``text`` is what the token parameter's put sends. Keeping it off the report keeps the
    report free of anything secret.
    """
    target = Path(token_path)
    existed_before = target.exists()
    writer = token_writer(target)
    login_flow(
        api_key,
        app_secret,
        callback_url,
        str(target),
        token_write_func=writer,
    )
    report = ReauthReport(
        token_path=target,
        callback_url=callback_url,
        token_written=writer.wrote,
        replaced_existing=existed_before and writer.wrote,
    )
    return report, writer


# What builds the token parameter's client from the config. ``main`` passes
# ``token_store.push_client``, and a test passes one that answers through botocore hooks.
StoreClientFactory = Callable[[Config], Any]


def reauth_from_config(
    *,
    login_flow: LoginFlow,
    store_client_factory: StoreClientFactory,
    stdin_is_tty: bool,
    config_path: str | Path | None = None,
    token_path: str | Path | None = None,
) -> ReauthReport:
    """Run a re-auth wired from the real config. This is the entry ``main`` calls.

    It loads the machine-local config for the two Schwab app credentials and the callback
    URL. The credentials are revealed here and handed to the flow, and neither reaches the
    report.

    ``token_store`` then decides whether the token parameter is put, as
    ``lake.token_store.mode_of`` reads it.

    1. Under ``file`` nothing more happens, and no client is built.
    2. Under ``both`` and ``store`` the put's three keys are checked before the login, so
       a missing or malformed one raises ``ConfigError`` before the browser opens. The
       ``bucket_*`` keys never stand in for them.
    3. Under any other value the line naming it is printed, and the keys are checked
       only after the login. A problem then skips the put rather than costing the week's
       token, and the report carries the line naming it.

    The put runs only when this run wrote a token, and it sends the text the writer
    wrote. A put that fails leaves the token written, and the report carries one line
    naming the token's path, the AWS error code and the fix. Nothing from the token or
    from an AWS error's message reaches either line.
    """
    config = load_config(config_path)
    mode, unknown = token_store.mode_of(config)
    if unknown is not None:
        print(f"reauth: {unknown}", file=sys.stderr)
    problems = [] if mode == token_store.FILE else token_store.credential_problems(config)
    if problems and mode in (token_store.BOTH, token_store.STORE):
        raise ConfigError(
            f"{'. '.join(problems)}. token_store is {mode}, so the re-auth puts the token "
            "parameter, and it refuses before the browser opens rather than after"
        )
    callback_url = _refuse_unless_ready(config.schwab_callback_url, stdin_is_tty)
    report, writer = _login(
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
        callback_url=callback_url,
        token_path=token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        login_flow=login_flow,
    )
    if mode == token_store.FILE or not report.token_written or writer.text is None:
        return report
    if problems:
        return replace(
            report, store_problem=token_store.skipped_push_line(report.token_path, problems)
        )
    return _put(report, writer.text, config, store_client_factory)


def _put(
    report: ReauthReport, text: str, config: Config, store_client_factory: StoreClientFactory
) -> ReauthReport:
    """Put ``text`` into the token parameter, and return the report saying how it went."""
    from botocore.exceptions import BotoCoreError, ClientError  # lazy: only a put needs it

    try:
        client = store_client_factory(config)
        version = token_store.push(client=client, text=text)
    except token_store.PushFailed as failure:
        line = token_store.push_failure_line(report.token_path, failure)
        return replace(report, store_problem=line)
    except (BotoCoreError, ClientError, ValueError) as exc:
        # The client's build, which ``push`` does not wrap. botocore refuses a malformed
        # region with an error that is both a ``BotoCoreError`` and a ``ValueError``.
        failure = token_store.PushFailed(type(exc).__name__)
        return replace(
            report, store_problem=token_store.push_failure_line(report.token_path, failure)
        )
    return replace(report, parameter_version=version)


def _schwab_login_flow(
    api_key: str,
    app_secret: str,
    callback_url: str,
    token_path: str,
    *,
    token_write_func: Callable[..., None],
) -> object:
    """The real login flow. This is the one place ``schwab-py`` is imported here.

    The import is lazy, the same discipline ``lake.schwab`` and ``lake.probe`` keep, so
    importing this module and running the offline suite touch neither the library nor the
    network. The client it builds is discarded. The token file is what this command
    produces, and it is produced by ``token_write_func``.
    """
    from schwab.auth import client_from_login_flow  # lazy: real dep, live only

    return client_from_login_flow(
        api_key,
        app_secret,
        callback_url,
        token_path,
        token_write_func=token_write_func,
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.reauth",
        description=("Re-auth with Schwab in a browser and write the token. Needs a terminal."),
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument("--token", help="Path to token.json (defaults to the standard location).")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.reauth`` entry. Returns a process exit code.

    It builds the real login flow and the token parameter's client factory itself and
    takes no seam, so a caller can neither pass a fake nor forget one into the live
    object. Four exit codes reach the rendered script, which runs under ``set -e``.

    1. 0: a token landed, and the token parameter was put when ``token_store`` asked
       for it. A put prints the parameter's version in the sign-off block.
    2. 1: the flow returned without leaving a token, so a ritual that did not happen
       fails visibly rather than reporting success.
    3. 2: a refusal before the login, such as no terminal, no callback URL, or an
       incomplete set of ``token_store_*`` keys under ``both`` or ``store``. It prints
       one line, the code the sibling commands use for an operator mistake.
    4. 3: ``token.json`` was written and the token parameter was not updated. It prints
       one line naming the token's path and the fix, either the AWS error code from a
       put that failed or the config problem that skipped it.
    """
    args = _build_parser().parse_args(argv)
    try:
        with input_errors_exit("reauth"):
            report = reauth_from_config(
                login_flow=_schwab_login_flow,
                store_client_factory=token_store.push_client,
                stdin_is_tty=sys.stdin.isatty(),
                config_path=args.config,
                token_path=args.token,
            )
    except ReauthError as exc:
        print(f"reauth: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    print(report.render())
    if not report.token_written:
        return 1
    if report.store_problem is not None:
        print(f"reauth: {report.store_problem}", file=sys.stderr)
        return 3
    return 0


__all__ = [
    "CALLBACK_KEY",
    "DEFAULT_TOKEN_PATH",
    "TOKEN_MODE",
    "LoginFlow",
    "ReauthError",
    "ReauthReport",
    "TokenWriter",
    "main",
    "reauth",
    "reauth_from_config",
    "token_writer",
    "write_token",
]


if __name__ == "__main__":
    raise SystemExit(main())
