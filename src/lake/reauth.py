"""The re-auth command: one browser login, one atomically written token.

``python -m lake.reauth`` runs the Sunday ritual the design's Auth section pins. Schwab's
refresh token dies every seven days and an interactive browser login is its only renewal,
so this is a standing weekly task rather than a one-time setup step. Every other install
step is rendered by ``lake.control_plane`` and run from a script that says what it does.
This one is rendered beside them as ``reauth.sh``, which calls this module.

The work is three steps. It reads the two Schwab app credentials and the registered
callback URL from ``config.yaml``, resolves the token path, and runs ``schwab-py``'s
login flow with a token writer of this module's own.

Three rules shape the module.

1. *It refuses when stdin is not a terminal.* A login flow needs a person and a browser.
   ``client_from_login_flow`` waits ``callback_timeout=300.0`` seconds for the callback,
   so wired into launchd with no browser to answer it the job would hang five minutes and
   then fail, which reads as a broken job rather than a misuse of one. The refusal is
   enforcement rather than a comment in the rendered header. It comes before anything
   else, so the tool costs nothing where it cannot work.
2. *The token write is atomic.* ``schwab-py`` writes the token with ``open(token_path,
   'w')``, which truncates the file before the new contents exist. A failure in between
   destroys a working token, and it lands on the one file capture cannot start without.
   This repo's rule is the opposite, so the write goes to a temp file beside the target,
   is flushed, and is published by one ``os.replace``. ``client_from_login_flow`` takes a
   ``token_write_func``, which is the hook that routes its write through this one.
3. *It prints no secret.* The report carries the token path, whether a token landed, and
   the callback URL. The callback is a loopback address rather than a credential, and
   printing it is how the operator checks it against the Schwab app registration. The API
   key and the app secret never reach the report or a log line.

Because the write is safe, overwriting a still-valid token is allowed and is the point. It
costs one login and yields a fresher token, and the Sunday coverage assertion tests
freshness rather than validity. A refuse-by-default guard was considered and dropped: it
would be friction protecting against nothing once a torn write is impossible.

The headless variant is deferred, not rejected. ``client_from_manual_flow`` takes the same
arguments and prints a URL rather than driving a browser, which is what a machine with no
browser needs. Phase one runs on the laptop, where the person who must log in has one, so
building both now means two paths tested against a machine nobody has. The migration to a
dedicated box or a VM owes it, and the design doc's Deployment section pins that.

The login flow is a seam. It reaches a browser, a local callback server, and Schwab, so
``reauth`` and ``reauth_from_config`` require it and never default one. ``main`` builds
the real one itself and accepts none, which is the rule ``tests/unit/test_seam_defaults``
states for every entry in this package.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from lake.config import input_errors_exit, load_config
from lake.paths import TOKEN_FILE, config_dir, temp_write_path

# The standard location of the Schwab token, per the design's Configuration section. The
# same home-relative default ``lake.schwab`` reads from, spelled through ``lake.paths``
# so this module needs nothing from the vendor layer.
DEFAULT_TOKEN_PATH = config_dir() / TOKEN_FILE

# The token is a full brokerage credential, so it is written owner-read-write and nothing
# else. The design pins ``chmod 600`` on this file. The mode goes on the temp file before
# the rename, so the token is never briefly readable at its real path.
TOKEN_MODE = 0o600

# The config key the login flow needs and capture does not.
CALLBACK_KEY = "schwab_callback_url"


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

    ``token_written`` says whether a token file exists at the path once the flow returns.
    ``replaced_existing`` says a token was already there and this run wrote over it, which
    is allowed and wanted rather than a warning. No field carries a secret.
    """

    token_path: Path
    callback_url: str
    token_written: bool
    replaced_existing: bool

    def render(self) -> str:
        """A human-readable sign-off block. It names no secret."""
        landed = "yes" if self.token_written else "no"
        if self.token_written and self.replaced_existing:
            landed = "yes (replaced the previous token)"
        return "\n".join(
            [
                "Schwab re-auth",
                f"  callback url:  {self.callback_url}",
                f"  token path:    {self.token_path}",
                f"  token landed:  {landed}",
            ]
        )


def write_token(token_path: Path | str, payload: object) -> None:
    """Write the token to ``token_path`` atomically: a temp file, a flush, one rename.

    This is the convention ``compact._write_partition`` and every other writer in this
    package follows, and it is the one ``schwab-py`` does not. A crash or a full disk
    part-way through leaves the prior token intact at the real path and a temp file
    beside it, rather than a truncated file where the credential used to be. The temp
    name comes from ``paths.temp_write_path``, which owns the one spelling of the marker
    the backup exclusion matches, and carries the writing process's id, so two writers
    never share one.

    The payload is serialised to a string before any file is opened. So a value JSON
    cannot encode fails before the write starts rather than half-way through it.
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


def token_writer(token_path: Path | str) -> Callable[..., None]:
    """The ``token_write_func`` ``schwab-py`` calls, writing atomically to ``token_path``.

    ``schwab-py`` wraps whatever it is given and calls it as ``func(token, *args,
    **kwargs)``, so the extra arguments are accepted and ignored. Only the token matters
    here, and it arrives already wrapped in the library's metadata envelope, which is
    what ``creation_timestamp`` is read back off later.
    """

    def write(token: object, *args: object, **kwargs: object) -> None:
        write_token(token_path, token)

    return write


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
    command uses.
    """
    if not stdin_is_tty:
        raise ReauthError(
            "re-auth needs a person at a terminal and a browser, and stdin is not a "
            "terminal. It cannot run from launchd or any other unattended job. Run it "
            "yourself from a shell."
        )
    if not callback_url:
        raise ReauthError(
            f"no {CALLBACK_KEY} in config.yaml. Add the callback URL registered on the "
            "Schwab app, such as https://127.0.0.1:8182, and run this again."
        )

    target = Path(token_path)
    replaced_existing = target.exists()
    login_flow(
        api_key,
        app_secret,
        callback_url,
        str(target),
        token_write_func=token_writer(target),
    )
    return ReauthReport(
        token_path=target,
        callback_url=callback_url,
        token_written=target.exists(),
        replaced_existing=replaced_existing,
    )


def reauth_from_config(
    *,
    login_flow: LoginFlow,
    stdin_is_tty: bool,
    config_path: str | Path | None = None,
    token_path: str | Path | None = None,
) -> ReauthReport:
    """Run a re-auth wired from the real config. This is the entry ``main`` calls.

    It loads the machine-local config for the two Schwab app credentials and the callback
    URL. The credentials are revealed here and handed to the flow, and neither reaches the
    report.
    """
    config = load_config(config_path)
    return reauth(
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
        callback_url=config.schwab_callback_url,
        token_path=token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        login_flow=login_flow,
        stdin_is_tty=stdin_is_tty,
    )


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

    It builds the real login flow itself and takes no seam, so a caller can neither pass
    a fake nor forget one into the live object. A refusal prints one line and exits 2, the
    code the sibling commands use for an operator mistake. A flow that returns without
    leaving a token exits 1, so the rendered script under ``set -e`` fails visibly rather
    than reporting a ritual that did not happen.
    """
    args = _build_parser().parse_args(argv)
    try:
        with input_errors_exit("reauth"):
            report = reauth_from_config(
                login_flow=_schwab_login_flow,
                stdin_is_tty=sys.stdin.isatty(),
                config_path=args.config,
                token_path=args.token,
            )
    except ReauthError as exc:
        print(f"reauth: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    print(report.render())
    return 0 if report.token_written else 1


__all__ = [
    "CALLBACK_KEY",
    "DEFAULT_TOKEN_PATH",
    "TOKEN_MODE",
    "LoginFlow",
    "ReauthError",
    "ReauthReport",
    "main",
    "reauth",
    "reauth_from_config",
    "token_writer",
    "write_token",
]


if __name__ == "__main__":
    raise SystemExit(main())
