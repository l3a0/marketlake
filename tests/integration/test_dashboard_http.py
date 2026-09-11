"""The query service over a real loopback socket.

These bind the server on an ephemeral port over a real fixture lake and drive it with
real HTTP requests. The HTTP layer, the sandboxed connection, and the filesystem are all
crossed, so the tier is integration. The clock and calendar stay fake.

Every case covers one line of the design's residual-surface argument: the bind is loopback,
a foreign ``Host`` is refused before anything else, only the enumerated routes answer,
only ``GET`` is served, every response carries the same hardening headers, and no
response carries a path or a secret.

Three servers are bound here, each for the boundary it exposes.

1. ``served`` is the ordinary one: a fixture lake, the fake clock, the fake calendar.
2. ``served_real_calendar`` swaps in the real calendar, because only it has a window a
   request date can fall outside of.
3. ``served_broken`` swaps in a service whose query raises, because the 500 path is the
   one branch where a traceback or a lake path could reach a client.
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
from lake.calendar import MARKET_TZ, ExchangeCalendar
from lake.dashboard import DashboardService, load_favicon, make_server
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


# How often the serving thread checks for the shutdown signal. The stdlib default is
# half a second, which every teardown here would then wait out. Nothing in these tests
# blocks, so a short interval costs nothing and saves that wait on each of them.
POLL_INTERVAL = 0.01


def _serve(service: DashboardService) -> Iterator[ThreadingHTTPServer]:
    """Bind one service on an ephemeral port, serve it on a thread, and tear it down."""
    server = make_server(service, 0)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": POLL_INTERVAL}, daemon=True
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def served(fixture_lake: FixtureLake) -> Iterator[tuple[ThreadingHTTPServer, Path]]:
    root = _build_lake(fixture_lake)
    service = DashboardService(root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR)
    for server in _serve(service):
        yield server, root


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


def _raw_request(server: ThreadingHTTPServer, request: bytes) -> tuple[bytes, bytes]:
    """Send a hand-written request and split the reply into its head and its body."""
    address, port = server.server_address[:2]
    with socket.create_connection((address, port), timeout=5) as sock:
        sock.sendall(request + b"Connection: close\r\n\r\n")
        raw = b""
        while chunk := sock.recv(4096):
            raw += chunk
    head, _, body = raw.partition(b"\r\n\r\n")
    return head, body


def test_the_server_binds_the_loopback_address(served):
    server, _root = served
    assert server.server_address[0] == "127.0.0.1"
    assert server.server_address[1] != 0


@pytest.mark.parametrize(
    "path", ["/", "/favicon.ico", "/api/now", "/api/today?date=2026-08-24", "/nope"]
)
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


def test_the_favicon_path_serves_the_packaged_icon(served):
    server, _root = served
    status, headers, body = _request(server, "/favicon.ico", host="localhost")
    assert status == 200
    assert headers["content-type"] == "image/x-icon"
    assert headers["content-length"] == str(len(body))
    assert body == load_favicon()
    # The page names this path, and the policy permits exactly this origin's images. This
    # test asserts both halves, because the icon needs them to agree and this is the
    # boundary where they meet.
    _page_status, page_headers, page_body = _request(server, "/", host="localhost")
    assert b'<link rel="icon" href="/favicon.ico"' in page_body
    assert "img-src 'self'" in page_headers["content-security-policy"]


def test_a_non_get_method_on_the_icon_is_still_a_405(served):
    # The icon path is matched inside ``_serve``, after the method check. A branch added
    # ahead of that check would turn this route into the one verb-agnostic hole.
    server, _root = served
    status, headers, _body = _request(server, "/favicon.ico", method="POST", host="localhost")
    assert status == 405
    assert headers["allow"] == "GET"


def test_a_head_request_for_the_icon_sends_no_body(served):
    server, _root = served
    head, body = _raw_request(server, b"HEAD /favicon.ico HTTP/1.1\r\nHost: localhost\r\n")
    assert head.split(b"\r\n")[0].endswith(b"405 Method Not Allowed")
    assert body == b""


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


@pytest.mark.parametrize(
    "path",
    [
        "/nope",
        "/api/now/",
        "/api/history",
        "/api",
        "/status.html",
        # The icon path is matched whole. A prefix, a trailing slash or a different
        # case must not reach it, and nothing covered that until these three.
        "/favicon.ico/",
        "/FAVICON.ICO",
        "/favicon.icox",
    ],
)
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


def test_a_head_request_gets_a_405_with_no_body(served):
    # RFC 9110 forbids content on a HEAD response. ``http.client`` would discard a body
    # it was sent, so the bytes are read off a raw socket instead.
    server, _root = served
    head, body = _raw_request(server, b"HEAD /api/now HTTP/1.1\r\nHost: localhost\r\n")
    assert head.split(b"\r\n")[0].endswith(b"405 Method Not Allowed")
    assert body == b""
    # The headers stay exactly what the matching GET would send, length included.
    assert b"Content-Length: " in head
    assert b"Allow: GET" in head


def test_no_response_carries_the_lake_path(served):
    server, root = served
    # The icon is swept here too. It is the one binary body, so the comparison runs over
    # raw bytes rather than decoded text, which is the same check for every other path.
    paths = ("/api/now", "/api/today", "/api/today?ticker=NOPE", "/nope", "/", "/favicon.ico")
    for path in paths:
        _status, headers, body = _request(server, path, host="localhost")
        assert str(root).encode() not in body
        assert str(root.parent).encode() not in body
        assert "Python" not in headers.get("server", "")


# -- the hardening headers ---------------------------------------------------

# Every directive the page's policy must carry. ``frame-ancestors``, ``base-uri`` and
# ``form-action`` do not fall back to ``default-src``, so each is listed on its own.
EXPECTED_CSP = frozenset(
    {
        "default-src 'none'",
        "connect-src 'self'",
        "img-src 'self'",
        "script-src 'unsafe-inline'",
        "style-src 'unsafe-inline'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
        "form-action 'none'",
    }
)


def _directives(policy: str) -> set[str]:
    return {directive.strip() for directive in policy.split(";") if directive.strip()}


def test_the_page_carries_the_whole_content_security_policy(served):
    # Asserting one directive would let any other widen silently. The set is asserted
    # whole, so a ``connect-src *`` cannot slip past.
    server, _root = served
    _status, headers, _body = _request(server, "/", host="localhost")
    assert _directives(headers["content-security-policy"]) == EXPECTED_CSP


@pytest.mark.parametrize(
    ("path", "host", "status"),
    [
        ("/", "localhost", 200),
        ("/favicon.ico", "localhost", 200),
        ("/api/now", "localhost", 200),
        ("/api/today?date=2026-08-24", "localhost", 200),
        ("/api/today?ticker=NOPE", "localhost", 400),
        ("/api/now", "dashboard.evil.example", 403),
        ("/nope", "localhost", 404),
    ],
)
def test_every_response_refuses_sniffing_and_caching(served, path: str, host: str, status: int):
    # All three response paths harden alike. A JSON panel is as cacheable and as sniffable
    # as the page or the icon, and the JSON path answers the 403, 400 and 404 replies too.
    server, _root = served
    got, headers, _body = _request(server, path, host=host)
    assert got == status
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["cache-control"] == "no-store"


def test_a_non_get_response_refuses_sniffing_and_caching(served):
    server, _root = served
    status, headers, _body = _request(server, "/api/now", method="POST", host="localhost")
    assert status == 405
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["cache-control"] == "no-store"


# -- the query string --------------------------------------------------------


@pytest.mark.parametrize("query", ["ticker=", "date=", "ticker=&date=2026-08-24"])
def test_a_blank_parameter_is_a_400_and_never_silently_ignored(served, query: str):
    # A field present with no value is still a field. Dropping it would let a request
    # that named a ticker answer as though it had named none.
    server, _root = served
    status, _headers, body = _request(server, f"/api/today?{query}", host="localhost")
    assert status == 400
    assert "error" in json.loads(body)
    # The same path without the field is a 200, so the 400 is the blank value's doing.
    assert _request(server, "/api/today", host="localhost")[0] == 200


# -- the date the calendar cannot judge --------------------------------------


@pytest.fixture
def served_real_calendar(fixture_lake: FixtureLake) -> Iterator[ThreadingHTTPServer]:
    # ``exchange_calendars`` covers a rolling window a little over twenty years wide and
    # refuses a date outside it. Only the real calendar has such a window, so the fake
    # cannot stand in here.
    root = _build_lake(fixture_lake)
    service = DashboardService(
        root, clock=ManualClock(NOW.astimezone(UTC)), calendar=ExchangeCalendar()
    )
    yield from _serve(service)


@pytest.mark.parametrize("day", ["2028-01-03", "1990-01-03", "9999-01-03"])
def test_a_date_outside_the_calendars_window_is_a_400(served_real_calendar, day: str):
    # A year past the window raises out of the calendar, and a year pandas cannot hold in
    # nanoseconds overflows instead. Both are the client's bad request, not a server fault.
    status, _headers, body = _request(
        served_real_calendar, f"/api/today?date={day}", host="localhost"
    )
    assert status == 400
    assert json.loads(body) == {"error": "date outside the calendar's range"}


def test_a_date_inside_the_calendars_window_still_answers(served_real_calendar):
    status, _headers, body = _request(
        served_real_calendar, "/api/today?date=2026-08-24", host="localhost"
    )
    assert status == 200
    assert json.loads(body)["is_session"] is True


# -- the 500 path ------------------------------------------------------------


class _BrokenService(DashboardService):
    """A service whose query raises with a lake path in the message.

    The 500 branch is the one place a traceback or a path could reach a client, and
    nothing else in the suite reaches it. The raised message deliberately carries the
    lake root so the assertion has something real to look for.
    """

    def run_query(self, name: str, raw):
        raise RuntimeError(f"scan failed under {self._paths.root} while running {name}")


@pytest.fixture
def served_broken(fixture_lake: FixtureLake) -> Iterator[tuple[ThreadingHTTPServer, Path]]:
    root = _build_lake(fixture_lake)
    service = _BrokenService(root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR)
    for server in _serve(service):
        yield server, root


@pytest.mark.parametrize("path", ["/api/now", "/api/today?date=2026-08-24&ticker=SPY"])
def test_a_failed_query_is_a_500_that_leaks_nothing(served_broken, path: str, caplog):
    server, root = served_broken
    status, headers, body = _request(server, path, host="localhost")
    assert status == 500
    assert headers["content-type"] == "application/json"
    assert json.loads(body) == {"error": "query failed"}
    text = body.decode("utf-8")
    assert str(root) not in text
    assert "RuntimeError" not in text
    assert "Traceback" not in text
    assert "scan failed" not in text
    # The detail goes to the log, where the owner can read it, and nowhere else.
    assert "named query" in caplog.text
