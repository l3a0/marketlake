"""The 09:35 says-closed-but-open probe."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from lake.probe_calendar import ProbeResult, fresh_symbols, run_probe
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock

ET = ZoneInfo("America/New_York")
WEEK = date(2026, 8, 31)
SATURDAY = date(2026, 9, 5)


def _quote(at: datetime) -> dict:
    return {"quote": {"quoteTime": int(at.timestamp() * 1000)}}


def _probe(at: datetime, quotes: dict | None = None, *, boom: bool = False) -> ProbeResult:
    def fetch(symbols):
        if boom:
            raise ConnectionError("vendor unreachable")
        return quotes or {}

    return run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=at),
        symbols=["SPY", "QQQ"],
        fetch=fetch,
    )


def test_a_session_day_asks_the_vendor_nothing():
    # The calendar and the daemon already agree. A vendor call would buy nothing.
    asked = []
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 2, 9, 35)),
        symbols=["SPY"],
        fetch=lambda symbols: asked.append(symbols) or {},
    )
    assert not result.checked
    assert not result.pages
    assert asked == []


def test_a_market_trading_on_a_day_the_calendar_calls_closed_pages():
    # The whole point. Nothing else would notice, because every other check agrees with
    # the calendar that today is not a session.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 5, 9, 34)), "QQQ": _quote(et(2026, 9, 5, 9, 33))},
    )
    assert result.checked
    assert result.pages
    assert result.trading == ("QQQ", "SPY")


def test_a_stale_quote_on_a_closed_day_is_the_ordinary_case():
    # A weekend carries Friday's last quote. That is the market being shut, not open.
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": _quote(et(2026, 9, 4, 16, 0))})
    assert result.checked
    assert not result.pages
    assert result.trading == ()


def test_one_trading_symbol_among_stale_ones_still_pages():
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 4, 16, 0)), "QQQ": _quote(et(2026, 9, 5, 9, 34))},
    )
    assert result.trading == ("QQQ",)
    assert result.pages


def test_an_unreachable_vendor_is_a_problem_and_never_a_page():
    # The probe catches a wrong calendar. An unreachable vendor is no evidence either
    # way, and paging on it would train the operator to ignore this check.
    result = _probe(et(2026, 9, 5, 9, 35), boom=True)
    assert result.checked
    assert not result.pages
    assert result.problem == "ConnectionError"


@pytest.mark.parametrize(
    "envelope",
    [
        {},
        {"quote": {}},
        {"quote": {"quoteTime": None}},
        {"quote": {"quoteTime": "not a number"}},
        "not a dict",
    ],
)
def test_a_quote_the_probe_cannot_read_is_not_evidence_of_trading(envelope):
    assert fresh_symbols({"SPY": envelope}, SATURDAY) == ()


def test_freshness_is_same_day_in_market_time_not_a_seconds_window():
    # The question is whether the market traded at all today. A stamp from a prior
    # session answers it as clearly as one from an hour ago, and a seconds threshold
    # would have to be guessed since the design pins none.
    day = date(2026, 9, 5)
    just_before_midnight = datetime(2026, 9, 5, 0, 1, tzinfo=ET)
    long_ago_but_today = datetime(2026, 9, 5, 23, 58, tzinfo=ET)
    assert fresh_symbols({"A": _quote(just_before_midnight)}, day) == ("A",)
    assert fresh_symbols({"A": _quote(long_ago_but_today)}, day) == ("A",)
    yesterday = _quote(datetime(2026, 9, 5, 0, 1, tzinfo=ET) - timedelta(hours=2))
    assert fresh_symbols({"A": yesterday}, day) == ()
