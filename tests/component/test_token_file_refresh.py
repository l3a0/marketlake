"""Refreshing the token through real ``schwab-py`` clients built from one token file.

Marketlake #564. Once capture cycles overlap, two clients built from ``token.json`` can find
the access token expired at the same moment. ``schwab-py``'s own writer rewrites the file in
place, so a reader in between meets it empty, and a lock per client lets both refresh. These
tests build real clients through ``lake.schwab.client_from_token``, the builder every caller
uses, and check four things.

1. A refresh publishes the file atomically, at mode 0600, and an interrupted one leaves the
   old token readable.
2. Two clients refresh once between them, and the second adopts what the first wrote. That
   holds whether or not the token endpoint rotates the refresh token. marketlake #633
   measured Schwab on 2026-10-05: a refresh left the refresh token unchanged, and a
   refresh on one host did not revoke the other, so these tests cover the case Schwab
   does not take today as well as the one it does.
3. A client adopts the file's mint time with its token, so a later refresh never writes an
   older mint time back.
4. A refresh whose write fails keeps its token in the process, and the request goes on
   (marketlake #860). Later clients run on the held token while it is newer than the file,
   retry the write, and drop it once the file catches up. A rotated refresh token still
   raises. ``tests/component/test_token_write_full_disk.py`` drives the same failure through
   the daemon's cycle entry.

The session ``schwab-py`` builds is authlib's ``OAuth2Client``, imported by name into
``schwab.auth``. Replacing that name with one that carries ``httpx.MockTransport`` sends the
token request and the data requests to the fake server below, and opens no socket. Every
token here is a made-up string in a temporary file.
"""

from __future__ import annotations

import base64
import errno
import functools
import json
import os
import sys
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

import lake.reauth
import lake.schwab
from lake import probe
from lake.schwab import SchwabVendor, VendorAuthError
from tests.component.test_request_timing import _Refusing
from tests.support.full_disk import fill_disk, free_disk

MINT = 1787529900
MINT_AFTER = MINT + 7 * 86400  # a mid-week re-login, one refresh-token lifetime later
_THREADS_PER_CLIENT = 4

# The fixed phrases of the lines a failed refresh write prints (marketlake #860).
_FAILED = "token file write failed"
_FAILED_CLASS = "OSError"
_GOES_ON = "the request goes on"
_ROTATED = "the refresh token rotated"
_RECOVERED = "now holds a token at least as new"


class _Server:
    """A token endpoint and a data endpoint, recording every refresh token it is sent.

    With ``rotate`` the endpoint issues a new refresh token on each refresh and refuses a
    superseded one, as an OAuth server that rotates refresh tokens does. Without it the
    refresh token never changes.

    ``expires_in`` is the access token's life in seconds, which a test may change between
    refreshes. Any value from 1 to 300 gives an integer ``expires_at`` already expired under
    ``schwab-py``'s 300-second leeway, which forces the next client to refresh again. A 0
    sends no ``expires_in``, so authlib sets no ``expires_at`` at all.
    """

    def __init__(self, *, rotate: bool, expires_in: int = 1800) -> None:
        self.rotate = rotate
        self.expires_in = expires_in
        self.current = "refresh-0"
        self.refreshed_with: list[str] = []
        self.client_auth: set[str] = set()
        self._lock = threading.Lock()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth/token":
            sent = parse_qs(request.content.decode())["refresh_token"][0]
            with self._lock:
                self.client_auth.add(request.headers["authorization"])
                if self.rotate and sent != self.current:
                    return httpx.Response(400, json={"error": "invalid_grant"})
                self.refreshed_with.append(sent)
                issued = len(self.refreshed_with)
                if self.rotate:
                    self.current = f"refresh-{issued}"
            # A refresh takes a moment, which is the window the other client races into.
            time.sleep(0.05)
            body = {
                "access_token": f"fresh-{issued}",
                "refresh_token": self.current,
                "token_type": "Bearer",
            }
            if self.expires_in:
                body["expires_in"] = self.expires_in
            return httpx.Response(200, json=body)
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


# How the app key and secret reach the token endpoint: HTTP Basic, in that order.
_APP_AUTH = "Basic " + base64.b64encode(b"app-key:app-secret").decode()


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
@pytest.mark.parametrize(
    "expires_in",
    # 100 seconds out is inside the 300-second leeway ``schwab-py`` gives the session, so
    # authlib refreshes it. The re-read must judge expiry with that same leeway, or the second
    # client refreshes a token the re-read called live.
    [-10, 100],
    ids=["expired", "inside-the-leeway"],
)
def test_two_clients_refresh_once_and_the_second_adopts_the_first(
    tmp_path, monkeypatch, rotate, expires_in
):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=expires_in)
    server = _Server(rotate=rotate)
    _serve(monkeypatch, server)
    first, second = _vendor(token), _vendor(token)

    assert _fire_together([first, second]) == {"Bearer fresh-1"}
    assert server.refreshed_with == ["refresh-0"]
    assert server.client_auth == {_APP_AUTH}
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
    # atomic writer publishes a new file at 0600. The file is not named ``token.json``, so a
    # writer aimed at the standard name rather than the path it was given fails here.
    token = tmp_path / "elsewhere.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))

    assert _sent_with(_vendor(token)) == "Bearer fresh-1"
    assert _stored(token)["token"]["access_token"] == "fresh-1"
    assert token.stat().st_mode & 0o777 == 0o600
    assert [p.name for p in tmp_path.iterdir()] == ["elsewhere.json"]


def test_an_interrupted_refresh_write_leaves_the_old_token_readable(tmp_path, monkeypatch, capsys):
    # The write fails before the rename with an ``OSError`` that carries no errno. The old
    # token stays readable, no temp file is left, and the request still lands on the
    # refreshed token, with one line that names the failure's class (marketlake #860).
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    before = token.read_bytes()
    _serve(monkeypatch, _Server(rotate=False))
    vendor = _vendor(token)
    fill_disk(monkeypatch, None)

    assert _sent_with(vendor) == "Bearer fresh-1"
    assert token.read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["token.json"]
    err = capsys.readouterr().err
    assert err.count(_FAILED) == 1
    assert f"({_FAILED_CLASS}) at {token};" in err
    assert "None" not in err


def test_the_probe_writes_its_refresh_atomically_too(tmp_path, monkeypatch):
    # The by-hand probe builds its own client and can run beside the daemon.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False)
    _serve(monkeypatch, server)

    client = probe._client_from_token(token, api_key="app-key", app_secret="app-secret")
    client.session.get("https://api.schwabapi.com/marketdata/v1/quotes")
    assert server.client_auth == {_APP_AUTH}
    assert _stored(token)["token"]["access_token"] == "fresh-1"
    assert token.stat().st_mode & 0o777 == 0o600


def test_a_session_wrapped_twice_still_refreshes(tmp_path, monkeypatch):
    # A second install nests one wrapper inside the other, both taking the process lock. A
    # plain lock would hang the request, and every client in the process behind it.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))
    client = lake.schwab.client_from_token(token, api_key="app-key", app_secret="app-secret")
    lake.schwab.serialize_token_refresh(client.session)

    sent: list[str] = []
    request = threading.Thread(
        target=lambda: sent.append(SchwabVendor(client).get_quotes(["SPY"]).body["authorization"]),
        daemon=True,
    )
    request.start()
    request.join(timeout=5)
    assert sent == ["Bearer fresh-1"]


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


def _on_disk(*without: str) -> dict:
    """A token on disk that would be adopted, less the fields named, and far from expiry."""
    token = {
        "access_token": "on-disk",
        "refresh_token": "on-disk-refresh",
        "token_type": "Bearer",
        "expires_at": int(time.time()) + 1800,
    }
    return {k: v for k, v in token.items() if k not in without}


@pytest.mark.parametrize(
    ("contents", "failure"),
    [
        (None, "FileNotFoundError"),
        ("{", "JSONDecodeError"),
        ("[]", "ValueError"),
        ("[" * 200_000, "RecursionError"),
        (json.dumps({"creation_timestamp": MINT, "token": [["access_token", "x"]]}), "ValueError"),
        (json.dumps({"creation_timestamp": MINT}), "ValueError"),
        (json.dumps({"token": _on_disk()}), "ValueError"),
        (json.dumps({"creation_timestamp": MINT, "token": _on_disk("access_token")}), "ValueError"),
        (
            json.dumps({"creation_timestamp": MINT, "token": _on_disk("refresh_token")}),
            "ValueError",
        ),
        (json.dumps({"creation_timestamp": MINT, "token": _on_disk("expires_at")}), "ValueError"),
        (
            json.dumps({"creation_timestamp": MINT, "token": {**_on_disk(), "expires_at": "9"}}),
            "ValueError",
        ),
    ],
    ids=[
        "missing",
        "not-json",
        "not-an-object",
        "nested-too-deep",
        "token-not-an-object",
        "no-token",
        "no-mint-time",
        "no-access-token",
        "no-refresh-token",
        "no-expiry",
        "expiry-not-a-whole-second",
    ],
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
    assert "expires_at" not in err
    # The refresh rewrote the file whole, so the next build reads a good token.
    assert _stored(token)["token"]["access_token"] == "fresh-1"


# -- a refresh whose write fails (marketlake #860) ---------------------------------------


def _held(path: Path) -> object:
    """The token held for ``path``, or ``None``. Read under the lock that guards the holder."""
    with lake.schwab._TOKEN_REFRESH_LOCK:
        return lake.schwab._HELD.get(path)


def _hold_one(tmp_path, monkeypatch, *, expires_in: int = 1800, code: int = errno.ENOSPC):
    """A token file, a server, and one refresh whose write failed, so ``fresh-1`` is held."""
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False, expires_in=expires_in)
    _serve(monkeypatch, server)
    fill_disk(monkeypatch, code)
    assert _sent_with(_vendor(token)) == "Bearer fresh-1"
    assert _held(token) is not None
    return token, server


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EROFS, errno.EIO], ids=errno.errorcode.get)
def test_a_failed_refresh_write_of_any_errno_lets_the_request_land(
    tmp_path, monkeypatch, capsys, code
):
    token, server = _hold_one(tmp_path, monkeypatch, code=code)

    # The next client runs on the held token, with no second call to the token endpoint.
    assert _sent_with(_vendor(token)) == "Bearer fresh-1"
    assert server.refreshed_with == ["refresh-0"]
    assert _stored(token)["token"]["access_token"] == "stale"
    err = capsys.readouterr().err
    assert err.count(_FAILED) == 1
    assert f"({errno.errorcode[code]}: {os.strerror(code)}) at {token};" in err
    assert _GOES_ON in err
    assert "fresh-1" not in err and "refresh-0" not in err and "stale" not in err


def test_a_type_error_from_the_write_still_raises(tmp_path, monkeypatch):
    # A value JSON cannot encode is a bug, not a full disk, so it is not caught.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))

    class _Unencodable:
        def dumps(self, payload: object) -> str:
            raise TypeError("Object of type bytes is not JSON serializable")

        def __getattr__(self, name: str) -> object:
            return getattr(json, name)

    monkeypatch.setattr(lake.reauth, "json", _Unencodable())
    with pytest.raises(TypeError, match="not JSON serializable"):
        _sent_with(_vendor(token))
    assert _held(token) is None


def test_the_line_prints_once_per_outage_and_again_when_the_errno_changes(
    tmp_path, monkeypatch, capsys
):
    # A held token that is already inside the leeway forces the next client to refresh again,
    # and that write fails too. No rotation here: the refresh token never changes.
    token, server = _hold_one(tmp_path, monkeypatch, expires_in=100)

    assert _sent_with(_vendor(token)) == "Bearer fresh-2"
    err = capsys.readouterr().err
    assert err.count(_FAILED) == 1
    assert "ENOSPC" in err

    # A remount turns the full disk read-only, which the operator repairs differently.
    fill_disk(monkeypatch, errno.EROFS)
    assert _sent_with(_vendor(token)) == "Bearer fresh-3"
    err = capsys.readouterr().err
    assert err.count(_FAILED) == 1
    assert "EROFS" in err
    assert server.refreshed_with == ["refresh-0", "refresh-0", "refresh-0"]


def test_a_rotated_refresh_token_is_held_and_its_request_raises(tmp_path, monkeypatch, capsys):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    before = token.read_bytes()
    server = _Server(rotate=True, expires_in=100)
    _serve(monkeypatch, server)
    fill_disk(monkeypatch, errno.ENOSPC)

    with pytest.raises(OSError) as raised:
        _sent_with(_vendor(token))
    assert raised.value.errno == errno.ENOSPC
    err = capsys.readouterr().err
    assert err.count(_ROTATED) == 1
    assert f"(ENOSPC: {os.strerror(errno.ENOSPC)}) at {token} and" in err
    assert _GOES_ON not in err

    # The held token is inside the leeway, so the next client refreshes again, with the
    # rotated refresh token rather than the file's superseded one. That refresh rotates too,
    # and its line prints again, since each one costs a request.
    with pytest.raises(OSError):
        _sent_with(_vendor(token))
    assert server.refreshed_with == ["refresh-0", "refresh-1"]
    err = capsys.readouterr().err
    assert err.count(_ROTATED) == 1
    assert _GOES_ON not in err

    # A long-lived token this time. The refresh still raises, and the next cycle's requests
    # land on the held token with no token call.
    server.expires_in = 1800
    with pytest.raises(OSError):
        _sent_with(_vendor(token))
    assert server.refreshed_with == ["refresh-0", "refresh-1", "refresh-2"]
    assert _sent_with(_vendor(token)) == "Bearer fresh-3"
    assert len(server.refreshed_with) == 3
    assert token.read_bytes() == before
    err = capsys.readouterr().err
    assert "refresh-" not in err and "fresh-" not in err


@pytest.mark.parametrize(
    ("mint", "expires_in", "sent"),
    [
        (MINT_AFTER, 1800, "Bearer on-disk"),  # a re-login landed: the file wins
        (MINT - 86400, 1800, "Bearer fresh-1"),  # a stale pulled copy: the held token wins
        (MINT, 3600, "Bearer on-disk"),  # the same login, refreshed further: the file wins
        (MINT, 600, "Bearer fresh-1"),  # the same login, refreshed less far: held wins
    ],
    ids=["later-mint", "earlier-mint", "same-mint-later-expiry", "same-mint-earlier-expiry"],
)
def test_a_held_token_yields_only_to_a_file_at_least_as_new(
    tmp_path, monkeypatch, capsys, mint, expires_in, sent
):
    token, _ = _hold_one(tmp_path, monkeypatch)
    _write(token, access="on-disk", refresh="refresh-0", expires_in=expires_in, mint=mint)
    capsys.readouterr()

    assert _sent_with(_vendor(token)) == sent
    err = capsys.readouterr().err
    if sent == "Bearer on-disk":
        assert _held(token) is None
        assert err.count(_RECOVERED) == 1
    else:
        # The disk is still full, so the retry failed, silently, and the token stays held.
        assert _held(token) is not None
        assert _stored(token)["token"]["access_token"] == "on-disk"
        assert err == ""


@pytest.mark.parametrize("which", ["writer", "reader"])
def test_a_stderr_that_refuses_the_line_costs_the_line_not_the_request(
    tmp_path, monkeypatch, which
):
    # On the laptop stderr is a file on the same full volume, and a print there raises.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False))
    vendor = _vendor(token)
    if which == "writer":
        fill_disk(monkeypatch, errno.ENOSPC)
    else:
        token.write_text("not json")  # the re-read fails and prints its own line
    monkeypatch.setattr(sys, "stderr", _Refusing())

    assert _sent_with(vendor) == "Bearer fresh-1"


def test_a_refresh_write_that_lands_drops_the_held_token(tmp_path, monkeypatch, capsys):
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    server = _Server(rotate=False, expires_in=100)
    _serve(monkeypatch, server)
    early = _vendor(token)  # built before the outage, on the stale token
    fill_disk(monkeypatch, errno.ENOSPC)
    assert _sent_with(_vendor(token)) == "Bearer fresh-1"

    # The file cannot be adopted, so the early client refreshes from its own token, and with
    # the disk freed that write lands.
    token.write_text("not json")
    free_disk(monkeypatch)
    capsys.readouterr()
    assert _sent_with(early) == "Bearer fresh-2"
    assert _held(token) is None
    assert capsys.readouterr().err.count(_RECOVERED) == 1

    # The next outage is a new one, so its line prints again.
    fill_disk(monkeypatch, errno.ENOSPC)
    assert _sent_with(_vendor(token)) == "Bearer fresh-3"
    assert capsys.readouterr().err.count(_FAILED) == 1


def test_a_file_that_catches_up_drops_the_held_token(tmp_path, monkeypatch, capsys):
    token, _ = _hold_one(tmp_path, monkeypatch)
    # A re-login lands while the disk is still full, already expired by the next build.
    _write(token, access="relogin", refresh="refresh-0", expires_in=-10, mint=MINT_AFTER)
    capsys.readouterr()

    assert _sent_with(_vendor(token)) == "Bearer fresh-2"
    err = capsys.readouterr().err
    assert err.count(_RECOVERED) == 1
    assert err.count(_FAILED) == 1  # the refresh after the drop is a new outage


def test_a_file_that_cannot_be_parsed_still_raises_and_is_not_touched(tmp_path, monkeypatch):
    token, _ = _hold_one(tmp_path, monkeypatch)
    free_disk(monkeypatch)
    token.write_text("not json")

    with pytest.raises(json.JSONDecodeError):
        _vendor(token)
    assert token.read_text() == "not json"
    assert _held(token) is not None


@pytest.mark.parametrize(
    "envelope",
    [
        {"creation_timestamp": True, "token": _on_disk()},
        {"creation_timestamp": MINT_AFTER, "token": _on_disk("expires_at")},
    ],
    ids=["mint-is-a-bool", "no-expiry"],
)
def test_a_file_without_usable_stamps_is_used_and_the_held_token_kept(
    tmp_path, monkeypatch, envelope
):
    token, _ = _hold_one(tmp_path, monkeypatch)
    free_disk(monkeypatch)
    token.write_text(json.dumps(envelope))

    assert _sent_with(_vendor(token)) == "Bearer on-disk"
    assert _held(token) is not None
    assert json.loads(token.read_text()) == envelope


def test_a_refreshed_token_with_no_expiry_lets_the_request_go_on(tmp_path, monkeypatch, capsys):
    # With nothing held, the failure line prints and the request lands.
    token = tmp_path / "token.json"
    _write(token, access="stale", refresh="refresh-0", expires_in=-10)
    _serve(monkeypatch, _Server(rotate=False, expires_in=0))
    fill_disk(monkeypatch, errno.ENOSPC)

    assert _sent_with(_vendor(token)) == "Bearer fresh-1"
    assert _held(token) is None
    assert capsys.readouterr().err.count(_FAILED) == 1


def test_a_refreshed_token_with_no_expiry_drops_an_older_held_one(tmp_path, monkeypatch):
    token, server = _hold_one(tmp_path, monkeypatch, expires_in=100)
    server.expires_in = 0

    assert _sent_with(_vendor(token)) == "Bearer fresh-2"
    assert _held(token) is None


class _WatchedLock:
    """``_TOKEN_REFRESH_LOCK``, with an event set when a thread other than its owner asks."""

    def __init__(self, lock: object) -> None:
        self._lock = lock
        self._owner = threading.get_ident()
        self.asked = threading.Event()

    def __enter__(self) -> object:
        if threading.get_ident() != self._owner:
            self.asked.set()
        return self._lock.__enter__()

    def __exit__(self, *exc: object) -> object:
        return self._lock.__exit__(*exc)


def test_a_refresh_that_lands_while_a_reader_waits_is_not_overwritten(
    tmp_path, monkeypatch, capsys
):
    # Cycles overlap, and the close+5 fill builds in the same process. A reader that decided
    # outside the lock could choose the held token, wait, and then write it over the newer
    # token a refresh landed meanwhile.
    token, server = _hold_one(tmp_path, monkeypatch, expires_in=100)
    first = _vendor(token)
    free_disk(monkeypatch)
    server.expires_in = 1800
    capsys.readouterr()
    watched = _WatchedLock(lake.schwab._TOKEN_REFRESH_LOCK)
    monkeypatch.setattr(lake.schwab, "_TOKEN_REFRESH_LOCK", watched)
    built: list[SchwabVendor] = []
    waiting = threading.Thread(target=lambda: built.append(_vendor(token)), daemon=True)

    with watched:
        waiting.start()
        assert watched.asked.wait(timeout=5)
        # While the reader waits, ``first`` lands the held token and then refreshes past it.
        assert _sent_with(first) == "Bearer fresh-2"
    waiting.join(timeout=5)

    assert _stored(token)["token"]["access_token"] == "fresh-2"
    assert capsys.readouterr().err.count(_RECOVERED) == 1
    (second,) = built
    assert _sent_with(second) == "Bearer fresh-2"
