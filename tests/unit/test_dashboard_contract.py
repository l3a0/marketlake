"""The dashboard's fixed-query contract, decided from values alone.

These pin the boundary rules without a connection, a file, or a socket in the path, so
the tier is unit. The parameter validators, the Host check, the route and registry
shape, the slot denominator, and the command-line contract are each a pure function or
a table.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from lake import dashboard
from lake.calendar import MARKET_TZ, OPTION_CLOSE_OFFSET
from lake.dashboard import (
    NAMED_QUERIES,
    ROUTES,
    QueryParameterError,
    host_allowed,
    parse_date,
    session_slots,
    validate_parameters,
    validate_ticker,
)
from lake.session import COMPACTION_DELAY, OPTION_CLOSE_GUARD, SessionBounds

ROSTER = {"QQQ": ("chains", "quotes"), "SPY": ("chains", "quotes")}


# -- the validators ----------------------------------------------------------


def test_parse_date_accepts_strict_iso():
    assert parse_date("2026-08-24") == date(2026, 8, 24)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "20260824",
        "2026-8-24",
        "2026-08-24T00:00",
        "2026-13-01",
        "2026-02-30",
        "x",
        "2026-08-24 ",
    ],
)
def test_parse_date_rejects_everything_else(text: str):
    with pytest.raises(QueryParameterError):
        parse_date(text)


def test_validate_ticker_accepts_only_roster_members():
    assert validate_ticker("SPY", ROSTER) == "SPY"
    for text in ("spy", "NOPE", "SPY ", "", "SPY;DROP"):
        with pytest.raises(QueryParameterError):
            validate_ticker(text, ROSTER)


def test_validate_parameters_types_the_fields_and_refuses_unknown_ones():
    today = NAMED_QUERIES["today"]
    assert validate_parameters(today, {"date": "2026-08-24", "ticker": "SPY"}, ROSTER) == {
        "day": date(2026, 8, 24),
        "ticker": "SPY",
    }
    assert validate_parameters(today, {}, ROSTER) == {}
    with pytest.raises(QueryParameterError):
        validate_parameters(today, {"sql": "SELECT 1"}, ROSTER)
    with pytest.raises(QueryParameterError):
        validate_parameters(NAMED_QUERIES["now"], {"ticker": "SPY"}, ROSTER)


def test_parameter_errors_never_echo_the_value():
    with pytest.raises(QueryParameterError) as caught:
        validate_ticker("EVIL<script>", ROSTER)
    assert "EVIL" not in str(caught.value)
    with pytest.raises(QueryParameterError) as caught:
        parse_date("2026-99-99")
    assert "99" not in str(caught.value)


# -- the Host check ----------------------------------------------------------


@pytest.mark.parametrize(
    ("host", "allowed"),
    [
        ("localhost", True),
        ("LOCALHOST", True),
        ("localhost:8765", True),
        ("127.0.0.1", True),
        ("127.0.0.1:80", True),
        (" 127.0.0.1:8765 ", True),
        (None, False),
        ("", False),
        ("localhost:", False),
        ("localhost:abc", False),
        ("localhost.evil.example", False),
        ("evil.example", False),
        ("127.0.0.1.evil.example", False),
        ("[::1]", False),
        ("[::1]:8765", False),
        ("localhost:8765:1", False),
        ("0.0.0.0", False),
    ],
)
def test_host_allowed(host: str | None, allowed: bool):
    assert host_allowed(host) is allowed


# -- the registry and the routes ---------------------------------------------


def test_the_registry_is_exactly_the_two_panels():
    assert set(NAMED_QUERIES) == {"now", "today"}
    assert NAMED_QUERIES["now"].parameters == frozenset()
    assert NAMED_QUERIES["today"].parameters == frozenset({"date", "ticker"})
    for name, query in NAMED_QUERIES.items():
        assert query.name == name
        assert callable(query.run)


def test_every_route_maps_to_a_registered_query():
    assert ROUTES == {"/api/now": "now", "/api/today": "today"}
    assert set(ROUTES.values()) <= set(NAMED_QUERIES)


def test_the_bind_is_loopback_and_not_configurable():
    assert dashboard.BIND_HOST == "127.0.0.1"
    parser = dashboard.build_parser()
    for flag in ("--host", "--bind", "--address"):
        with pytest.raises(SystemExit):
            parser.parse_args([flag, "0.0.0.0"])


# -- the slot denominator ----------------------------------------------------


def _bounds(day: date, open_h: int, open_m: int, close_h: int, close_m: int, early: bool):
    """Session bounds built from values, the way the session clock derives them."""
    opened = datetime(day.year, day.month, day.day, open_h, open_m, tzinfo=MARKET_TZ)
    equity_close = datetime(day.year, day.month, day.day, close_h, close_m, tzinfo=MARKET_TZ)
    option_close = equity_close + OPTION_CLOSE_OFFSET
    return SessionBounds(
        day=day,
        open=opened,
        equity_close=equity_close,
        option_close=option_close,
        option_close_deadline=option_close + OPTION_CLOSE_GUARD,
        compaction=option_close + COMPACTION_DELAY,
        early_close=early,
    )


def test_session_slots_run_from_the_open_through_the_option_close():
    bounds = _bounds(date(2026, 8, 24), 9, 30, 16, 0, early=False)
    slots = session_slots(bounds)
    assert len(slots) == 406
    assert slots[0] == bounds.open
    assert slots[-1] == bounds.option_close
    steps = zip(slots, slots[1:], strict=False)
    assert all((later - earlier) == dashboard.SLOT for earlier, later in steps)


def test_session_slots_shrink_on_an_early_close():
    bounds = _bounds(date(2026, 11, 27), 9, 30, 13, 0, early=True)
    slots = session_slots(bounds)
    assert len(slots) == 226
    assert slots[-1] == bounds.option_close


# -- the command line and the page -------------------------------------------


def test_build_parser_takes_a_port_and_a_lake_root():
    parser = dashboard.build_parser()
    args = parser.parse_args([])
    assert args.port == dashboard.DEFAULT_PORT
    assert args.lake_root is None
    assert args.config is None
    args = parser.parse_args(["--port", "9001", "--lake-root", "/lake", "--config", "c.yaml"])
    assert (args.port, args.lake_root, args.config) == (9001, "/lake", "c.yaml")


def test_the_status_page_ships_in_the_package_and_is_self_contained():
    page = dashboard.load_status_page()
    assert b"<title>" in page
    assert b"/api/now" in page
    assert b"/api/today" in page
    # No external resource: the page must work offline and inside the same-origin policy.
    for marker in (b"http://", b"https://", b"<link", b"<img", b"src="):
        assert marker not in page
