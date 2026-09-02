"""The query service over a real loopback socket.

These bind the server on an ephemeral port over a real fixture lake and drive it with
real HTTP requests. The HTTP layer, the sandboxed connection, and the filesystem are all
crossed, so the tier is integration. The clock and calendar stay fake.

Every case pins one line of the design's residual-surface argument: the bind is loopback,
a foreign ``Host`` is refused before anything else, only the enumerated routes answer,
only ``GET`` is served, and no response carries a path or a secret.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime
from http.server import ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal
from lake.calendar import MARKET_TZ
from lake.dashboard import DashboardService, make_server
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake

MONDAY = date(2026, 8, 24)


def et(day: date, h: int, m: int, s: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=MARKET_TZ)


NOW = et(MONDAY, 9, 40, 30)
CALENDAR = FakeCalendar({MONDAY: SessionTimes(open=et(MONDAY, 9, 30), close=et(MONDAY, 16, 0))})


def _build_lake(fixture_lake: FixtureLake) -> Path:
    row = dict.fromkeys(journal.QUOTES_SCHEMA.names)
    row.update(
        snap_ts=et(MONDAY, 9, 30).astimezone(UTC).isoformat(),
        ticker="SPY",
        row_kind=journal.ROW_KIND_DATA,
        suspect=False,
        schema_version=journal.SCHEMA_VERSION,
    )
    fixture_lake.with_journal_segment(
        "quotes",
        "SPY",
        MONDAY,
        pa.Table.from_pylist([row], schema=journal.QUOTES_SCHEMA),
        start_ts="20260824T133000000000",
        pid=1,
    )
    return fixture_lake.build()


@pytest.fixture
def served(fixture_lake: FixtureLake) -> Iterator[tuple[ThreadingHTTPServer, Path]]:
    root = _build_lake(fixture_lake)
    service = DashboardService(root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR)
    server = make_server(service, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, root
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _request(
    server: ThreadingHTTPServer, path: str, *, method: str = "GET", host: str | None = None
) -> tuple[int, dict[str, str], bytes]:
    address, port = server.server_address[:2]
    conn = http.client.HTTPConnection(address, port, timeout=5)
    headers = {"Host": host} if host is not None else {}
    conn.request(method, path, headers=headers)
    response = conn.getresponse()
    body = response.read()
    conn.close()
    return response.status, {k.lower(): v for k, v in response.getheaders()}, body


def test_the_server_binds_the_loopback_address(served):
    server, _root = served
    assert server.server_address[0] == "127.0.0.1"
    assert server.server_address[1] != 0


@pytest.mark.parametrize("path", ["/", "/api/now", "/api/today?date=2026-08-24", "/nope"])
def test_a_foreign_host_is_refused_first(served, path: str):
    server, _root = served
    status, headers, body = _request(server, path, host="dashboard.evil.example")
    assert status == 403
    assert headers["content-type"] == "application/json"
    assert json.loads(body) == {"error": "forbidden host"}


def test_a_foreign_host_is_refused_before_the_method_check(served):
    server, _root = served
    status, _headers, _body = _request(server, "/api/now", method="POST", host="evil.example")
    assert status == 403


def test_a_request_without_a_host_header_is_refused(served):
    server, _root = served
    address, port = server.server_address[:2]
    with socket.create_connection((address, port), timeout=5) as sock:
        sock.sendall(b"GET /api/now HTTP/1.0\r\n\r\n")
        raw = b""
        while chunk := sock.recv(4096):
            raw += chunk
    assert raw.startswith(b"HTTP/1.0 403")


def test_the_root_serves_the_status_page(served):
    server, _root = served
    status, headers, body = _request(server, "/", host="localhost")
    assert status == 200
    assert headers["content-type"] == "text/html; charset=utf-8"
    assert headers["cache-control"] == "no-store"
    assert "default-src 'none'" in headers["content-security-policy"]
    assert b"<title>" in body
    assert b"/api/now" in body


def test_api_now_returns_json(served):
    server, _root = served
    status, headers, body = _request(server, "/api/now", host="127.0.0.1")
    assert status == 200
    assert headers["content-type"] == "application/json"
    now = json.loads(body)
    assert now["tickers"] == ["SPY"]
    assert now["surfaces"][0]["surface"] == "quotes"
    assert now["surfaces"][0]["minutes_since"] == 10.5
    assert now["token_minted_at"] is None


def test_api_today_returns_json(served):
    server, _root = served
    status, _headers, body = _request(
        server, "/api/today?date=2026-08-24&ticker=SPY", host="localhost:1"
    )
    assert status == 200
    today = json.loads(body)
    assert today["is_session"] is True
    assert today["slot_count"] == 406
    assert today["strips"][0]["ticker"] == "SPY"
    assert today["strips"][0]["slots"][0]["status"] == "captured"


@pytest.mark.parametrize(
    "query",
    [
        "ticker=NOPE",
        "date=2026-13-45",
        "date=20260824",
        "date=2026-08-24&sql=SELECT+1",
        "date=2026-08-24&date=2026-08-25",
    ],
)
def test_a_bad_parameter_is_a_400(served, query: str):
    server, _root = served
    status, headers, body = _request(server, f"/api/today?{query}", host="localhost")
    assert status == 400
    assert headers["content-type"] == "application/json"
    assert "error" in json.loads(body)


@pytest.mark.parametrize("path", ["/nope", "/api/now/", "/api/history", "/api", "/status.html"])
def test_an_unknown_path_is_a_404(served, path: str):
    server, _root = served
    status, _headers, body = _request(server, path, host="localhost")
    assert status == 404
    assert json.loads(body) == {"error": "not found"}


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
def test_a_non_get_method_is_a_405(served, method: str):
    server, _root = served
    status, headers, _body = _request(server, "/api/now", method=method, host="localhost")
    assert status == 405
    assert headers["allow"] == "GET"


def test_no_response_carries_the_lake_path(served):
    server, root = served
    for path in ("/api/now", "/api/today", "/api/today?ticker=NOPE", "/nope", "/"):
        _status, headers, body = _request(server, path, host="localhost")
        text = body.decode("utf-8")
        assert str(root) not in text
        assert str(root.parent) not in text
        assert "Python" not in headers.get("server", "")
