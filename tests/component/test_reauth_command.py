"""``python -m lake.reauth`` across one real boundary: the filesystem.

The unit tests beside this one drive ``reauth`` with a login flow passed in. That leaves
the module's own entry unexercised, and an entry only ever replaced is an entry nothing
runs. These tests call the real ``main``: it parses the arguments, loads a throwaway
``config.yaml`` from disk, decides for itself whether stdin is a terminal, builds its own
login flow, and writes a token file.

The vendor is faked one level lower, as a fake ``schwab`` package in ``sys.modules``. So
``_schwab_login_flow`` runs for real, including its lazy import, and only
``client_from_login_flow`` is a stand-in. Nothing reaches Schwab and no browser opens.

``main`` takes no seam, which is the rule ``tests/unit/test_seam_defaults`` states. That
is why the fake goes in ``sys.modules`` rather than being handed to the entry.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

from lake import reauth
from tests.support.config import write_config

CALLBACK = "https://127.0.0.1:8182"

FRESH_TOKEN = {
    "creation_timestamp": 1787529900,
    "token": {"refresh_token": "fake-refresh-token"},
}
OLD_TOKEN = {
    "creation_timestamp": 1786925100,
    "token": {"refresh_token": "fake-previous-token"},
}


class FakeLoginFlow:
    """A stand-in for ``schwab.auth.client_from_login_flow``.

    It records the arguments the real entry passed it and writes ``token`` through the
    writer it was handed, which is how the atomic write is reached from the real path
    rather than from a test calling it directly.
    """

    def __init__(self, token: object | None = FRESH_TOKEN) -> None:
        self.calls: list[tuple[str, str, str, str]] = []
        self._token = token

    def __call__(
        self,
        api_key: str,
        app_secret: str,
        callback_url: str,
        token_path: str,
        *,
        token_write_func,
        **kwargs: object,
    ) -> object:
        self.calls.append((api_key, app_secret, callback_url, token_path))
        if self._token is not None:
            token_write_func(self._token)
        return "a client this command discards"


def _install_seam(monkeypatch: pytest.MonkeyPatch, flow: FakeLoginFlow) -> None:
    """Put a fake ``schwab.auth`` in ``sys.modules`` so the lazy import finds the flow.

    Both entries are set, because ``from schwab.auth import client_from_login_flow``
    resolves the parent package and then the submodule. ``monkeypatch`` restores
    ``sys.modules`` afterwards, so the fake never leaks into another test.
    """
    auth = ModuleType("schwab.auth")
    auth.client_from_login_flow = flow  # type: ignore[attr-defined]
    package = ModuleType("schwab")
    package.auth = auth  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "schwab", package)
    monkeypatch.setitem(sys.modules, "schwab.auth", auth)


class _Stdin:
    """A stdin stand-in answering one question, which is all the entry asks it."""

    def __init__(self, tty: bool) -> None:
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty


def _at_a_terminal(monkeypatch: pytest.MonkeyPatch, tty: bool = True) -> None:
    """Say whether a person is at a terminal. Under pytest, stdin is never one."""
    monkeypatch.setattr(sys, "stdin", _Stdin(tty))


def _config(tmp_path: Path, lake_root: Path, *, callback: str | None = CALLBACK) -> Path:
    return write_config(tmp_path, lake_root, callback_url=callback)


def test_main_loads_the_config_runs_the_flow_and_lands_a_token(
    tmp_path, lake_root, monkeypatch, capsys
):
    """The whole entry, end to end, with only the vendor call faked.

    The credentials and the callback come out of a real ``config.yaml`` on disk, the
    token lands at the path the argument names, and the exit code says the ritual
    happened.
    """
    config = _config(tmp_path, lake_root)
    token = tmp_path / "token.json"
    flow = FakeLoginFlow()
    _install_seam(monkeypatch, flow)
    _at_a_terminal(monkeypatch)

    code = reauth.main(["--config", str(config), "--token", str(token)])

    assert code == 0
    assert flow.calls == [("api-key", "app-secret", CALLBACK, str(token))]
    assert json.loads(token.read_text()) == FRESH_TOKEN
    assert token.stat().st_mode & 0o777 == reauth.TOKEN_MODE
    printed = capsys.readouterr().out
    assert str(token) in printed
    assert CALLBACK in printed


def test_main_prints_neither_credential(tmp_path, lake_root, monkeypatch, capsys):
    """Both secrets pass through the entry, and a rendered script logs what it prints."""
    config = _config(tmp_path, lake_root)
    _install_seam(monkeypatch, FakeLoginFlow())
    _at_a_terminal(monkeypatch)

    reauth.main(["--config", str(config), "--token", str(tmp_path / "token.json")])

    captured = capsys.readouterr()
    for secret in ("api-key", "app-secret"):
        assert secret not in captured.out
        assert secret not in captured.err


def test_main_refuses_when_stdin_is_not_a_terminal(tmp_path, lake_root, monkeypatch, capsys):
    """The unattended refusal, reached through the entry that reads stdin for itself.

    A launchd job is exactly this: a complete config, a valid callback, and no terminal.
    The exit code is 2, the code every sibling command uses for an operator mistake, so a
    plist pointed here fails in seconds instead of waiting out the callback timeout.
    """
    config = _config(tmp_path, lake_root)
    token = tmp_path / "token.json"
    flow = FakeLoginFlow()
    _install_seam(monkeypatch, flow)
    _at_a_terminal(monkeypatch, tty=False)

    with pytest.raises(SystemExit) as excinfo:
        reauth.main(["--config", str(config), "--token", str(token)])

    assert excinfo.value.code == 2
    assert flow.calls == []
    assert not token.exists()
    assert "terminal" in capsys.readouterr().err


def test_main_refuses_when_the_config_has_no_callback(tmp_path, lake_root, monkeypatch, capsys):
    """The config that every other job runs on happily is the one this refuses.

    ``write_config`` leaves the callback out by default, which is the shape the daemon
    tests all use. So this is the same file those pass on, and only this command needs
    the key.
    """
    config = _config(tmp_path, lake_root, callback=None)
    flow = FakeLoginFlow()
    _install_seam(monkeypatch, flow)
    _at_a_terminal(monkeypatch)

    with pytest.raises(SystemExit) as excinfo:
        reauth.main(["--config", str(config), "--token", str(tmp_path / "token.json")])

    assert excinfo.value.code == 2
    assert flow.calls == []
    assert reauth.CALLBACK_KEY in capsys.readouterr().err


def test_main_exits_two_on_a_config_it_cannot_read(tmp_path, monkeypatch, capsys):
    """A bad operator file is one named line, not a traceback, the same as every entry."""
    _install_seam(monkeypatch, FakeLoginFlow())
    _at_a_terminal(monkeypatch)

    with pytest.raises(SystemExit) as excinfo:
        reauth.main(["--config", str(tmp_path / "nope.yaml")])

    assert excinfo.value.code == 2
    assert "reauth:" in capsys.readouterr().err


def test_main_overwrites_a_live_token_through_the_real_entry(
    tmp_path, lake_root, monkeypatch, capsys
):
    """A token already at the path is replaced, and the block says it was.

    This is the decision the issue settled: overwriting costs one login and yields a
    fresher token, and the write cannot tear, so nothing guards against it.
    """
    config = _config(tmp_path, lake_root)
    token = tmp_path / "token.json"
    token.write_text(json.dumps(OLD_TOKEN))
    _install_seam(monkeypatch, FakeLoginFlow())
    _at_a_terminal(monkeypatch)

    assert reauth.main(["--config", str(config), "--token", str(token)]) == 0
    assert json.loads(token.read_text()) == FRESH_TOKEN
    assert "replaced the previous token" in capsys.readouterr().out


def test_main_exits_one_when_the_flow_leaves_no_token(tmp_path, lake_root, monkeypatch, capsys):
    """A ritual that produced no token must not read as done.

    The rendered script runs under ``set -e``, so a zero here would let a failed Sunday
    login look successful in the transcript.
    """
    config = _config(tmp_path, lake_root)
    _install_seam(monkeypatch, FakeLoginFlow(token=None))
    _at_a_terminal(monkeypatch)

    code = reauth.main(["--config", str(config), "--token", str(tmp_path / "token.json")])

    assert code == 1
    assert "token landed:  no" in capsys.readouterr().out


def test_main_exits_one_when_the_flow_writes_nothing_over_a_live_token(
    tmp_path, lake_root, monkeypatch, capsys
):
    """The failed ritual in the shape every Sunday but the first has.

    A token is already at the path, so a report that asked the filesystem would see one
    and call the run a success. The rendered script runs under ``set -e``, so a zero here
    would let a failed Sunday login pass in the transcript with last week's token still
    in place, which is the silence the whole auth subsystem exists to avoid.
    """
    config = _config(tmp_path, lake_root)
    token = tmp_path / "token.json"
    token.write_text(json.dumps(OLD_TOKEN))
    _install_seam(monkeypatch, FakeLoginFlow(token=None))
    _at_a_terminal(monkeypatch)

    code = reauth.main(["--config", str(config), "--token", str(token)])

    assert code == 1
    printed = capsys.readouterr().out
    assert "token landed:  no" in printed
    assert "replaced" not in printed
    assert json.loads(token.read_text()) == OLD_TOKEN


def test_main_without_a_token_argument_falls_back_to_the_module_default(
    tmp_path, lake_root, monkeypatch
):
    """Omitting ``--token`` uses ``DEFAULT_TOKEN_PATH`` rather than anything else.

    The constant is redirected here so no real token is ever touched. What this states is
    the wiring: the entry's fallback is that constant, and the path it names is what
    reaches the flow. The constant's own value is stated in the unit tests, where it is
    compared against the path the vendor reads a token back from.
    """
    config = _config(tmp_path, lake_root)
    redirected = tmp_path / "elsewhere" / "token.json"
    flow = FakeLoginFlow()
    _install_seam(monkeypatch, flow)
    _at_a_terminal(monkeypatch)
    monkeypatch.setattr(reauth, "DEFAULT_TOKEN_PATH", redirected)

    assert reauth.main(["--config", str(config)]) == 0

    assert flow.calls == [("api-key", "app-secret", CALLBACK, str(redirected))]
    assert json.loads(redirected.read_text()) == FRESH_TOKEN
