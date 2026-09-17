"""The vendor seam: the cassette-backed fake replays offline and never guesses."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lake.cassette import (
    CASSETTE_VERSION,
    Cassette,
    CassetteError,
    Interaction,
    dump_cassette,
    load_cassette,
)
from lake.vendor import Vendor, VendorError
from tests.support.vendor import CassetteVendor


def test_cassette_vendor_replays_a_recorded_chain(cassette_vendor: CassetteVendor):
    response = cassette_vendor.get_chain("SPY")
    assert response.status == 200
    assert response.body["symbol"] == "SPY"
    assert response.body["isDelayed"] is False


def test_cassette_vendor_replays_recorded_quotes(cassette_vendor: CassetteVendor):
    response = cassette_vendor.get_quotes(["SPY", "QQQ"])
    assert response.status == 200
    assert response.body["SPY"]["realtime"] is True
    assert response.body["QQQ"]["quote"]["bidPrice"] == 601.48


def test_cassette_vendor_reports_the_token_mint_time(cassette_vendor: CassetteVendor):
    minted = cassette_vendor.token_mint_time()
    assert isinstance(minted, datetime)
    assert minted.tzinfo is not None
    assert minted == datetime.fromisoformat("2026-08-23T20:05:00-04:00")


def test_cassette_vendor_refuses_an_unrecorded_request(cassette_vendor: CassetteVendor):
    with pytest.raises(CassetteError):
        cassette_vendor.get_chain("TSLA")


def test_cassette_match_is_exact_on_symbol_order(cassette_vendor: CassetteVendor):
    # The recording is [SPY, QQQ]. A different order is a different request.
    with pytest.raises(CassetteError):
        cassette_vendor.get_quotes(["QQQ", "SPY"])


def test_cassette_vendor_satisfies_the_vendor_protocol(cassette_vendor: CassetteVendor):
    assert isinstance(cassette_vendor, Vendor)


def test_missing_token_mint_time_raises():
    cassette = Cassette(
        interactions=(
            Interaction(endpoint="chains", params={"symbol": "SPY"}, status=200, body={}),
        )
    )
    with pytest.raises(VendorError):
        CassetteVendor(cassette).token_mint_time()


def test_cassette_round_trips_through_disk(tmp_path):
    cassette = Cassette(
        interactions=(
            Interaction(
                endpoint="quotes",
                params={"symbols": ["SPY"]},
                status=200,
                body={"SPY": {"realtime": True}},
            ),
        ),
        token_mint_time="2026-08-23T20:05:00-04:00",
    )
    path = tmp_path / "c.json"
    dump_cassette(cassette, path)
    loaded = load_cassette(path)
    assert loaded.cassette_version == CASSETTE_VERSION
    assert loaded.token_mint_time == cassette.token_mint_time
    found = loaded.find("quotes", {"symbols": ["SPY"]})
    assert found.body == {"SPY": {"realtime": True}}


def test_load_rejects_an_unsupported_version(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"cassette_version": 999, "interactions": []}')
    with pytest.raises(CassetteError):
        load_cassette(path)


# -- price history on the shared cassette --------------------------------------
#
# The checked-in minimal cassette is what ``conftest.cassette_vendor`` hands the whole
# suite. The two shapes below are both synthetic. A recording from a real account
# carries real market data and is never committed.
#
# **Its two ``freq=1m`` interactions are reachable by a direct seam call and not by the
# bars sweep.** They are keyed without ``extended_hours``, which is what the request that
# produced them carried. Marketlake #421 made the sweep's minute fetch ask for the regular
# session by name, so it looks a recording up under ``extended_hours: false`` and misses
# both of these.
#
# Re-keying them to match would be the wrong repair. A cassette key states what its own
# request asked for, which is why ``bars_params`` keys a flag exactly as given and never
# coerces one, and a recording taken without the flag does not become one taken with it
# because a later caller wants the key. A sweep-shaped minute replay needs a recording
# taken through the flagged recorder, which is a live request and the owner's to make.


_ET = timezone(timedelta(hours=-4))
# The Monday the rest of this cassette is stamped against, and the Sunday before it.
SESSION_OPEN = datetime(2026, 8, 24, 9, 30, tzinfo=_ET)
SESSION_CLOSE = datetime(2026, 8, 24, 16, 0, tzinfo=_ET)
NO_SESSION_OPEN = datetime(2026, 8, 23, 9, 30, tzinfo=_ET)
NO_SESSION_CLOSE = datetime(2026, 8, 23, 16, 0, tzinfo=_ET)


def test_the_shared_cassette_replays_a_populated_window(cassette_vendor: CassetteVendor):
    response = cassette_vendor.get_minute_bars("SPY", start=SESSION_OPEN, end=SESSION_CLOSE)
    assert response.status == 200
    assert response.body["symbol"] == "SPY"
    assert response.body["empty"] is False
    candles = response.body["candles"]
    assert len(candles) == 3
    # Schwab stamps a candle in epoch milliseconds, not as an ISO string. The first
    # candle is the opening minute of the session the window names.
    assert candles[0]["datetime"] == int(SESSION_OPEN.timestamp() * 1000)
    assert set(candles[0]) == {"open", "high", "low", "close", "volume", "datetime"}


def test_the_shared_cassette_replays_an_empty_window(cassette_vendor: CassetteVendor):
    """A window the vendor has no candle for is a recorded answer, not a missing one.

    Schwab answers with an empty list and ``empty`` true rather than an error, so the
    two are told apart here: this raises nothing, while an unrecorded window below does.
    """
    response = cassette_vendor.get_minute_bars("SPY", start=NO_SESSION_OPEN, end=NO_SESSION_CLOSE)
    assert response.status == 200
    assert response.body["candles"] == []
    assert response.body["empty"] is True


def test_the_shared_cassette_refuses_an_unrecorded_window(cassette_vendor: CassetteVendor):
    with pytest.raises(CassetteError):
        cassette_vendor.get_minute_bars(
            "SPY", start=SESSION_OPEN, end=SESSION_CLOSE + timedelta(days=1)
        )
