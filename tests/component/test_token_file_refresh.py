"""Refreshing the token through real ``schwab-py`` clients built from one token file.

Marketlake #564. Once capture cycles overlap, two clients built from ``token.json`` can find
the access token expired at the same moment. ``schwab-py``'s own writer rewrites the file in
place, so a reader in between meets it empty, and a lock per client lets both refresh. These
tests build real clients through ``lake.schwab.client_from_token``, the builder every caller
uses, and check three things.

1. A refresh publishes the file atomically, at mode 0600, and an interrupted one leaves the
   old token readable.
2. Two clients refresh once between them, and the second adopts what the first wrote. That
   holds whether or not the token endpoint rotates the refresh token, which nothing has
   measured for Schwab.
3. A client adopts the file's mint time with its token, so a later refresh never writes an
   older mint time back.

The session ``schwab-py`` builds is authlib's ``OAuth2Client``, imported by name into
``schwab.auth``. Replacing that name with one that carries ``httpx.MockTransport`` sends the
token request and the data requests to the fake server below, and opens no socket. Every
token here is a made-up string in a temporary file.
"""

from __future__ import annotations

import functools
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import schwab.auth
from authlib.integrations.httpx_client import OAuth2Client

import lake.schwab
from lake import probe
from lake.schwab import SchwabVendor, VendorAuthError

MINT = 1787529900
MINT_AFTER = MINT + 7 * 86400  # a mid-week re-login, one refresh-token lifetime later
_THREADS_PER_CLIENT = 4


class _Server:
    """A token endpoint and a data endpoint, recording every refresh token it is sent.

    With ``rotate`` the endpoint issues a new refresh token on each refresh and refuses a
    superseded one, as an OAuth server that rotates refresh tokens does. Without it the
    refresh token never changes.
    """

    def __init__(self, *, rotate: bool) -> None:
        self.rotate = rotate
        self.current = "refresh-0"
        self.refreshed_with: list[str] = []
        self._lock = threading.Lock()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth/token":
            sent = parse_qs(request.content.decode())["refresh_token"][0]
            with self._lock:
                if self.rotate and sent != self.current:
                    return httpx.Response(400, json={"error": "invalid_grant"})
                self.refreshed_with.append(sent)
                issued = len(self.refreshed_with)
                if self.rotate:
                    self.current = f"refresh-{issued}"
            # A refresh takes a moment, which is the window the other client races into.
            time.sleep(0.05)
            return httpx.Response(
                200,
                json={
                    "access_token": f"fresh-{issued}",
                    "refresh_token": self.current,
                    "token_type": "Bearer",
                    "expires_in": 1800,
                },
            )
        return httpx.Response(200, json={"authorization": request.headers["authorization"]})


def _serve(monkeypatch: pytest.MonkeyPatch, server: _Server) -> None:
    """Route every session ``schwab-py`` builds through ``server``."""
    monkeypatch.setattr(
        schwab.auth,
        "OAuth2Client",
        functools.partial(OAuth2Client, transport=httpx.MockTransport(server)),
    )


def _write(path: Path, *, access: str, refresh: str, expires_in: int, mint: int = MINT) -> None:
    """Write a token file the way ``schwab-py`` does, at the default mode of 0644."""
    token = {
        "access_token": access,
        "refresh_token": refresh,
        "token_type": "Bearer",
        "expires_at": int(time.time()) + expires_in,
    }
    path.write_text(json.dumps({"creation_timestamp": mint, "token": token}))
    path.chmod(0o644)


def _stored(path: Path) -> dict:
    return json.loads(path.read_text())


def _vendor(path: Path) -> SchwabVendor:
    return SchwabVendor.from_token(path, api_key="app-key", app_secret="app-secret")


def _sent_with(vendor: SchwabVendor) -> str:
    """Make one request and return the authorization header it went out with."""
    return vendor.get_quotes(["SPY"]).body["authorization"]


def _fire_together(vendors: list[SchwabVendor]) -> set[str]:
    """Send requests from several threads of each client, released at the same instant."""
    callers = [v for v in vendors for _ in range(_THREADS_PER_CLIENT)]
    barrier = threading.Barrier(len(callers))

    def request(vendor: SchwabVendor) -> str:
        barrier.wait()
        return _sent_with(vendor)

    with ThreadPoolExecutor(len(callers)) as pool:
        return set(pool.map(request, callers))


@pytest.mark.parametrize("rotate", [False, True], ids=["fixed-refresh", "rotating-refresh"])
def test_two_clients_refresh_once_and_the_second_adopts_the_first(tmp_path, monkeypatch, rotate):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=rotate)
    _serve(monkeypatch, server)
    first, second = _vendor(token), _vendor(token)

    assert _fire_together([first, second]) == {"Bearer fresh-1"}
    assert server.refreshed_with == ["refresh-0"]
    stored = _stored(token)
    assert stored["token"]["access_token"] == "fresh-1"
    assert stored["token"]["refresh_token"] == server.current
    assert stored["creation_timestamp"] == MINT


def test_the_race_is_real_without_the_reread(tmp_path, monkeypatch):
    # The fixture is armed: with the re-read gone, the second client refreshes again from the
    # expired token it was built with. If this stopped reproducing, the test above would pass
    # for nothing.
    monkeypatch.setattr(lake.schwab, "_adopt_stored_token", lambda client, read_token: None)
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False)
    _serve(monkeypatch, server)
    first, second = _vendor(token), _vendor(token)

    _fire_together([first, second])
    assert server.refreshed_with == ["refresh-0", "refresh-0"]


def test_a_rotated_refresh_token_is_refused_to_a_client_that_does_not_reread(tmp_path, monkeypatch):
    # Why the re-read matters if Schwab rotates: the second refresh is refused, and the lake
    # reads that as auth death.
    monkeypatch.setattr(lake.schwab, "_adopt_stored_token", lambda client, read_token: None)
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=True))
    first, second = _vendor(token), _vendor(token)

    assert _sent_with(first) == "Bearer fresh-1"
    with pytest.raises(VendorAuthError):
        _sent_with(second)


def test_a_refresh_publishes_the_file_owner_only(tmp_path, monkeypatch):
    # ``schwab-py``'s writer opens the existing file for writing, which keeps its 0644. The
    # atomic writer publishes a new file at 0600.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))

    assert _sent_with(_vendor(token)) == "Bearer fresh-1"
    assert token.stat().st_mode & 0o777 == 0o600
    assert [p.name for p in tmp_path.iterdir()] == ["token.json"]


def test_an_interrupted_refresh_write_leaves_the_old_token_readable(tmp_path, monkeypatch):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    before = token.read_bytes()
    _serve(monkeypatch, _Server(rotate=False))
    vendor = _vendor(token)

    def refuse(src, dst):
        raise OSError("disk went away before the rename")

    monkeypatch.setattr("lake.reauth.os.replace", refuse)
    with pytest.raises(OSError, match="before the rename"):
        _sent_with(vendor)
    assert token.read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["token.json"]


def test_the_probe_writes_its_refresh_atomically_too(tmp_path, monkeypatch):
    # The by-hand probe builds its own client and can run beside the daemon.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))

    client = probe._client_from_token(token, api_key="app-key", app_secret="app-secret")
    client.session.get("https://api.schwabapi.com/marketdata/v1/quotes")
    assert _stored(token)["token"]["access_token"] == "fresh-1"
    assert token.stat().st_mode & 0o777 == 0o600


def test_a_live_token_is_neither_reread_nor_refreshed(tmp_path, monkeypatch):
    token = tmp_path / "token.json"
    _write(token, access="live", refresh="refresh-0", expires_in=1800)
    server = _Server(rotate=False)
    _serve(monkeypatch, server)
    vendor = _vendor(token)
    token.write_text("not json")  # a re-read would fail, and print a line saying so

    assert _sent_with(vendor) == "Bearer live"
    assert server.refreshed_with == []


def test_an_expired_file_token_is_refreshed_with_the_files_refresh_token(tmp_path, monkeypatch):
    # Another process refreshed after this client was built, rotating the refresh token, and
    # its access token has since lapsed too. The client refreshes with the refresh token on
    # disk, the only one the server still accepts.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=True)
    _serve(monkeypatch, server)
    vendor = _vendor(token)
    server.current = "refresh-elsewhere"
    _write(token, access="lapsed", refresh="refresh-elsewhere", expires_in=-10)

    assert _sent_with(vendor) == "Bearer fresh-1"
    assert server.refreshed_with == ["refresh-elsewhere"]


def test_a_relogin_since_the_build_keeps_its_mint_time_through_a_refresh(tmp_path, monkeypatch):
    # A client built before a mid-week re-login adopts the new token's mint time with the
    # token, so the refresh it then makes writes the new mint time back, not its own old one.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False)
    _serve(monkeypatch, server)
    vendor = _vendor(token)
    _write(token, access="relogin", refresh="refresh-0", expires_in=-10, mint=MINT_AFTER)

    assert _sent_with(vendor) == "Bearer fresh-1"
    assert _stored(token)["creation_timestamp"] == MINT_AFTER
    assert vendor.token_mint_time() == datetime.fromtimestamp(MINT_AFTER, tz=UTC)


@pytest.mark.parametrize(
    ("contents", "failure"),
    [
        (None, "FileNotFoundError"),
        ("{", "JSONDecodeError"),
        ("[]", "ValueError"),
        (json.dumps({"creation_timestamp": MINT}), "ValueError"),
        (json.dumps({"token": {"access_token": "on-disk"}}), "ValueError"),
    ],
    ids=["missing", "not-json", "not-an-object", "no-token", "no-mint-time"],
)
def test_a_failed_reread_refreshes_from_the_clients_own_token(
    tmp_path, monkeypatch, capsys, contents, failure
):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False)
    _serve(monkeypatch, server)
    vendor = _vendor(token)
    if contents is None:
        token.unlink()
    else:
        token.write_text(contents)

    assert _sent_with(vendor) == "Bearer fresh-1"
    assert server.refreshed_with == ["refresh-0"]
    assert vendor.token_mint_time() == datetime.fromtimestamp(MINT, tz=UTC)
    err = capsys.readouterr().err
    assert f"token file re-read failed, refreshing from the client's own token: {failure}\n" in err
    assert "on-disk" not in err and "refresh-0" not in err
    # The refresh rewrote the file whole, so the next build reads a good token.
    assert _stored(token)["token"]["access_token"] == "fresh-1"
