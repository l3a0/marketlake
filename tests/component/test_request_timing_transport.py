"""Request timing through a real ``schwab-py`` client, against a loopback server.

``lake.schwab.attach_timing`` hooks the ``httpx.Client`` that ``schwab-py`` builds on, so
the only honest test drives the real stack: ``schwab.client.Client`` over authlib's
``OAuth2Client`` over httpx and httpcore. A server on ``127.0.0.1`` plays Schwab. The
suite's socket guard in ``tests/conftest.py`` refuses every other machine and allows this
one. The clock is the real ``SystemClock``, because the hooks stamp events as the
transport reports them, so each assertion is a bound set by a delay the server applies.

Most of those bounds are strict. The header wait is one: ``sent`` is stamped before the
request leaves, and the server sleeps only after it arrives, so no lag on either side can
shorten the measured wait below the sleep. The body gap is not strict. ``headers`` is
stamped a moment after the server sends them, so that moment comes off the measured gap,
and CI measured 0.299924 seconds against a 0.3 second sleep. That bound carries
``CLIENT_LAG`` of slack, far below the sleep, so a swapped or missing stamp still fails it.

What they cover:

1. The four transport instants arrive in order, and the gaps match the server's delays.
2. A request that reuses a connection records no connection setup.
3. A token refresh the client makes first lands before ``sent``, never inside Schwab's time.
4. A hook that fails costs its field and never the response.
5. Two requests made at once from two threads each get back their own record.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("schwab")
pytest.importorskip("authlib")

from authlib.integrations.httpx_client import OAuth2Client  # noqa: E402
from schwab.client import Client  # noqa: E402

from lake.clock import SystemClock  # noqa: E402
from lake.schwab import SchwabVendor, attach_timing, read_timing  # noqa: E402

# The delays the server applies. Each is large enough to sit clear of loopback's own cost.
HEADER_DELAY = 0.2
BODY_DELAY = 0.3
REFRESH_DELAY = 0.6

# How much the client's headers stamp may lag the server's send, taken off the body gap.
CLIENT_LAG = 0.05

_CHAIN = {"symbol": "SPY", "status": "SUCCESS", "callExpDateMap": {}, "putExpDateMap": {}}


class _Schwab(BaseHTTPRequestHandler):
    """A loopback Schwab. It delays the headers, then trickles the body in two halves."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - the stdlib's name
        delay = HEADER_DELAY
        if "delay=" in self.path:
            delay = float(self.path.split("delay=")[1].split("&")[0])
        body = json.dumps({**_CHAIN, "pad": "p" * 20_000}).encode("utf-8")
        time.sleep(delay)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        half = len(body) // 2
        self.wfile.write(body[:half])
        self.wfile.flush()
        time.sleep(BODY_DELAY)
        self.wfile.write(body[half:])
        self.wfile.flush()

    def do_POST(self) -> None:  # noqa: N802 - the stdlib's name
        self.rfile.read(int(self.headers["Content-Length"]))
        time.sleep(REFRESH_DELAY)
        body = json.dumps(
            {"access_token": "new", "token_type": "Bearer", "expires_in": 1800}
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def server() -> Iterator[str]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Schwab)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


class _LoopbackClient(Client):
    """The real ``schwab-py`` client, sending to the loopback server instead of Schwab.

    ``Client._get_request`` hard-codes ``https://api.schwabapi.com``. Everything else is
    the library's own: the endpoint methods build the path and parameters, and the call
    goes through the real ``OAuth2Client`` session the hooks sit on.
    """

    def __init__(self, base: str, session: OAuth2Client) -> None:
        super().__init__("key", session, token_metadata=None, enforce_enums=False)
        self._base = base

    def _get_request(self, path, params):
        return self.session.get(self._base + path, params=params)


def _client(base: str, *, expires_in: float = 3600.0) -> _LoopbackClient:
    session = OAuth2Client(
        "key",
        client_secret="secret",
        token={
            "access_token": "old",
            "token_type": "Bearer",
            "refresh_token": "refresh",
            "expires_at": time.time() + expires_in,
        },
        token_endpoint=f"{base}/v1/oauth/token",
        leeway=300,
    )
    return _LoopbackClient(base, session)


def _seconds(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds()


def test_the_four_instants_arrive_in_order_with_the_servers_gaps(server):
    client = _client(server)
    assert attach_timing(client, SystemClock()) is True

    response = SchwabVendor(client).get_chain("SPY")

    timing = response.timing
    assert timing is not None and timing.failure is None
    assert timing.sent <= timing.connected <= timing.headers <= timing.body
    # The server held the headers, then held the second half of the body.
    assert _seconds(timing.headers, timing.sent) >= HEADER_DELAY
    assert _seconds(timing.body, timing.headers) >= BODY_DELAY - CLIENT_LAG
    # The body is uncompressed here, so its size on the wire is its length.
    assert timing.bytes == len(json.dumps({**_CHAIN, "pad": "p" * 20_000}))
    assert response.body["status"] == "SUCCESS"


def test_a_reused_connection_records_no_setup(server):
    client = _client(server)
    attach_timing(client, SystemClock())
    vendor = SchwabVendor(client)

    first = vendor.get_chain("SPY").timing
    second = vendor.get_chain("SPY").timing

    assert first.connected is not None
    assert second.connected is None
    assert second.sent is not None and second.headers is not None


def test_a_token_refresh_lands_before_sent(server):
    # The access token expires inside authlib's leeway, so the client refreshes it with a
    # POST before the chain request goes out. That wait is the refresh's, not Schwab's.
    client = _client(server, expires_in=60.0)
    clock = SystemClock()
    attach_timing(client, clock)

    before = clock.now()
    timing = SchwabVendor(client).get_chain("SPY").timing

    assert _seconds(timing.sent, before) >= REFRESH_DELAY
    assert _seconds(timing.headers, timing.sent) >= HEADER_DELAY
    assert _seconds(timing.headers, timing.sent) < HEADER_DELAY + REFRESH_DELAY


class _FailingClock:
    """A clock that raises from its ``fail_on``-th read on, so a hook's stamp fails midway."""

    def __init__(self, fail_on: int) -> None:
        self._reads = 0
        self._fail_on = fail_on

    def now(self) -> datetime:
        self._reads += 1
        if self._reads >= self._fail_on:
            raise RuntimeError("clock broke")
        return datetime.now(UTC)

    def monotonic(self) -> float:  # pragma: no cover - unused by the hooks
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:  # pragma: no cover - unused by the hooks
        raise NotImplementedError


@pytest.mark.parametrize("fail_on", [1, 2, 3], ids=["sent", "connected", "headers"])
def test_a_hook_that_fails_costs_its_field_and_never_the_response(server, fail_on):
    client = _client(server)
    attach_timing(client, _FailingClock(fail_on))

    response = SchwabVendor(client).get_chain("SPY")

    assert response.status == 200
    assert response.body["status"] == "SUCCESS"
    assert response.timing is not None
    assert response.timing.failure == "RuntimeError: clock broke"


def test_two_requests_at_once_each_keep_their_own_record(server):
    # One session, two threads, two server delays half a second apart. A record shared
    # between the requests would hand one of them the other's wait.
    client = _client(server)
    attach_timing(client, SystemClock())
    delays = {"short": 0.1, "long": 0.6}

    def fetch(name: str):
        return name, read_timing(client.session.get(f"{server}/v1/chains?delay={delays[name]}"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = dict(pool.map(fetch, delays))

    short = _seconds(results["short"].headers, results["short"].sent)
    long = _seconds(results["long"].headers, results["long"].sent)
    assert short >= delays["short"]
    assert long >= delays["long"]
    assert short < long
