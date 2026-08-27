"""The vendor seam: the cassette-backed fake replays offline and never guesses."""

from __future__ import annotations

from datetime import datetime

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
