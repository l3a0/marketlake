"""A reply whose body is not a JSON object, from the vendor through capture to the watchdog.

marketlake #539. A 429 or a 401 answered with a gateway's HTML page, or an empty body, used to
raise out of ``schwab._response_from`` before its status was read. Capture recorded it as
``j_s_o_n_decode_error``, so the watchdog paged "quote sampler dead" and "SPY chains" where it
should have paged "rate limited". A chain body that parsed to a list or ``null`` was worse:
``_is_too_big`` reads the body before the status, so it raised ``AttributeError`` out of the
cycle and no surface landed that minute.

Every test here runs the real ``SchwabVendor`` over a fake client whose replies are literal
bytes, so the shaping under test is the production one. The well-formed chain and quote bodies
come from the checked-in ``spy_minimal`` cassette, which is recorded data rather than a constant
in the code under test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from lake import capture, journal
from lake.cassette import load_cassette
from lake.chain_plan import ChainPlan
from lake.schwab import SchwabVendor
from lake.tickers import Roster
from lake.watchdog import Watchdog
from tests.conftest import CASSETTES
from tests.support.clock import ManualClock
from tests.support.schwab import FakeResponse, FakeSchwabClient

_RECORDED = load_cassette(CASSETTES / "spy_minimal.json")
_CHAIN_OK = FakeResponse(200, _RECORDED.find("chains", {"symbol": "SPY"}).body)
_QUOTES_OK = FakeResponse(200, _RECORDED.find("quotes", {"symbols": ["SPY", "QQQ"]}).body)

_PAGE = b"<html><head><title>429 Too Many Requests</title></head></html>"

# One open-ended window, so each chain is one request and one reply decides it.
_ONE_WINDOW = ChainPlan(((0, None),))
_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)


def _roster() -> Roster:
    return Roster.from_mapping(
        {"SPY": {"options": True, "chain_cadence": "1m"}, "QQQ": {"options": False}}
    )


def _cycle(lake_root: Path, *, chain: FakeResponse, quotes: FakeResponse, minute: int = 0):
    client = FakeSchwabClient(chains={"SPY": chain}, quotes={("SPY", "QQQ"): quotes})
    return capture.run_cycle(
        ManualClock(start=_START + timedelta(minutes=minute)),
        SchwabVendor(client),
        _roster(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )


def _classes(result) -> dict[tuple[str, str], set]:
    """Each written segment's error classes, keyed by surface and ticker."""
    return {
        (segment.surface, segment.ticker): {
            row["error_class"] for row in journal.read_segment(segment.path).to_pylist()
        }
        for segment in result.segments
    }


def test_a_chain_answered_429_with_html_is_recorded_as_a_429(lake_root):
    result = _cycle(lake_root, chain=FakeResponse(429, content=_PAGE), quotes=_QUOTES_OK)
    classes = _classes(result)
    assert classes[("chains", "SPY")] == {"http_429"}
    assert classes[("quotes", "SPY")] == {None}
    assert classes[("quotes", "QQQ")] == {None}


def test_a_chain_answered_401_with_null_no_longer_takes_the_cycle_down(lake_root):
    """``null`` parses, to ``None``, which ``_is_too_big`` cannot call ``get`` on. The
    quotes are planned after the chains, so this used to cost them too."""
    result = _cycle(lake_root, chain=FakeResponse(401, content=b"null"), quotes=_QUOTES_OK)
    classes = _classes(result)
    assert classes[("chains", "SPY")] == {"http_401"}
    assert classes[("quotes", "SPY")] == {None}
    assert classes[("quotes", "QQQ")] == {None}
    assert result.errors == ()


def test_a_quote_batch_answered_429_with_html_gaps_every_ticker_as_a_429(lake_root):
    result = _cycle(lake_root, chain=_CHAIN_OK, quotes=FakeResponse(429, content=_PAGE))
    classes = _classes(result)
    assert classes[("quotes", "SPY")] == {"http_429"}
    assert classes[("quotes", "QQQ")] == {"http_429"}
    assert classes[("chains", "SPY")] == {None}


def test_a_chain_answered_200_with_a_list_is_its_own_failure_and_the_quotes_land(lake_root):
    """The 2xx side: the payload changed shape, which is neither a status nor a crash."""
    result = _cycle(lake_root, chain=FakeResponse(200, content=b"[]"), quotes=_QUOTES_OK)
    classes = _classes(result)
    assert classes[("chains", "SPY")] == {"vendor_body_error"}
    assert classes[("quotes", "SPY")] == {None}
    assert classes[("quotes", "QQQ")] == {None}


def test_a_rate_limit_answered_with_html_pages_as_a_rate_limit(lake_root):
    """The failure this issue exists for. Every request answers 429 with an HTML page, for
    longer than the page threshold, and the one page is "rate limited" under ``http_429``."""
    watchdog = Watchdog()
    pages = []
    for minute in range(4):
        result = _cycle(
            lake_root,
            chain=FakeResponse(429, content=_PAGE),
            quotes=FakeResponse(429, content=_PAGE),
            minute=minute,
        )
        pages += watchdog.observe(result)
    assert [(page.title, page.cause) for page in pages] == [
        ("Capture down: rate limited", "http_429")
    ]
