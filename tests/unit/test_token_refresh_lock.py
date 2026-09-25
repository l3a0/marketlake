"""The token-refresh lock that lets a capture cycle share one client across threads.

Marketlake #532 fires a cycle's requests from a pool of threads through one ``schwab-py``
client, whose ``session`` is authlib's sync ``OAuth2Client``. That client refreshes an expired
access token inside every request, with no lock. So without ``serialize_token_refresh`` every
request in flight refreshes the token and rewrites ``token.json``.

These tests drive the real ``OAuth2Client`` through ``httpx.MockTransport``, which carries the
token request as well as the data requests and opens no socket, so they run under the suite's
network guard. They are also what notices an authlib upgrade that renames the method the lock
wraps, since the lock is installed by attribute.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from authlib.integrations.httpx_client import OAuth2Client

from lake.schwab import SchwabVendor, serialize_token_refresh
from tests.support.schwab import FakeSchwabClient

_THREADS = 8


class _Server:
    """A token endpoint and a data endpoint, counting the refreshes each one sees."""

    def __init__(self) -> None:
        self.refreshes = 0
        self._lock = threading.Lock()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            with self._lock:
                self.refreshes += 1
                issued = self.refreshes
            # A refresh takes a moment, which is the window the other threads race into.
            time.sleep(0.05)
            return httpx.Response(
                200,
                json={
                    "access_token": f"fresh-{issued}",
                    "refresh_token": "refresh",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                },
            )
        return httpx.Response(200, json={"ok": True})


def _client(server: _Server, *, expires_in: int, writes: list[str]) -> OAuth2Client:
    """A real authlib client whose access token lapses ``expires_in`` seconds from now."""
    return OAuth2Client(
        "client-id",
        client_secret="secret",
        token={
            "access_token": "stale",
            "refresh_token": "refresh",
            "token_type": "Bearer",
            "expires_at": int(time.time()) + expires_in,
        },
        token_endpoint="https://api.example.invalid/token",
        update_token=lambda token, **_: writes.append(token["access_token"]),
        leeway=300,
        transport=httpx.MockTransport(server),
    )


def _fire_together(client: OAuth2Client) -> set[int]:
    """Send one request from each of several threads, released at the same instant."""
    barrier = threading.Barrier(_THREADS)

    def request(_: int) -> int:
        barrier.wait()
        return client.get("https://api.example.invalid/data").status_code

    with ThreadPoolExecutor(_THREADS) as pool:
        return set(pool.map(request, range(_THREADS)))


def test_the_race_is_real_without_the_lock():
    # The fixture is armed: with an expired token and no lock, more than one thread refreshes.
    # If this stopped reproducing the race, the test below would pass for nothing.
    server, writes = _Server(), []
    client = _client(server, expires_in=-10, writes=writes)

    assert _fire_together(client) == {200}
    assert server.refreshes > 1
    assert len(writes) == server.refreshes


def test_an_expired_token_is_refreshed_once_across_threads():
    # One refresh and one token write, and every request goes out with the fresh token. A lock
    # that forwarded the token it was called with, rather than re-reading the session's, would
    # refresh once per thread here: each waiting thread holds the stale token object.
    server, writes = _Server(), []
    client = _client(server, expires_in=-10, writes=writes)
    serialize_token_refresh(client)

    assert _fire_together(client) == {200}
    assert server.refreshes == 1
    assert writes == ["fresh-1"]
    assert client.token["access_token"] == "fresh-1"


def test_a_live_token_is_not_refreshed():
    server, writes = _Server(), []
    client = _client(server, expires_in=1800, writes=writes)
    serialize_token_refresh(client)

    assert _fire_together(client) == {200}
    assert server.refreshes == 0
    assert writes == []


def test_a_session_without_the_method_is_refused_loudly():
    # An authlib upgrade that renamed the method would otherwise leave capture running with the
    # refresh unguarded. Refusing here means ``from_token`` raises, and CI sees it first.
    with pytest.raises(AttributeError):
        serialize_token_refresh(object())


def test_close_closes_the_session_once_and_a_failed_close_does_not_raise(capsys):
    client = FakeSchwabClient()
    vendor = SchwabVendor(client)
    vendor.close()
    assert client.session.closed == 1

    def refuse() -> None:
        raise OSError("socket already gone")

    client.session.close = refuse
    vendor.close()
    assert "client close failed: OSError: socket already gone" in capsys.readouterr().err
