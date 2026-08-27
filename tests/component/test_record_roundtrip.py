"""The recorder writes a real cassette file that replays offline.

This is a component test. It crosses one real boundary: the filesystem. The recorder
shapes a cassette from a fake-client vendor built through an injected factory,
``dump_cassette`` writes it to disk, ``load_cassette`` reads it back, and
``CassetteVendor`` replays it. So the by-hand recorder and the offline fake are proven
to share one on-disk format. No network, no real token, and no ``lake.config`` import
are involved.
"""

from __future__ import annotations

from collections.abc import Callable

from lake.cassette import dump_cassette, load_cassette
from lake.record import record_cassette
from lake.schwab import SchwabVendor
from lake.vendor import Vendor
from tests.support.schwab import FakeResponse, FakeSchwabClient
from tests.support.vendor import CassetteVendor

MINT_EPOCH = 1787529900.0  # 2026-08-24 00:05:00 UTC
MINT_ISO = "2026-08-24T00:05:00+00:00"

CHAIN_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "underlyingPrice": 650.01,
    "callExpDateMap": {"2026-09-18:25": {"650.0": [{"putCall": "CALL", "bid": 4.2}]}},
}
QUOTES_BODY = {"SPY": {"quote": {"bidPrice": 649.98}}, "QQQ": {"quote": {"bidPrice": 601.48}}}


def _factory() -> Callable[..., Vendor]:
    client = FakeSchwabClient(
        chains={"SPY": FakeResponse(200, CHAIN_BODY, {"content-type": "application/json"})},
        quotes={("SPY", "QQQ"): FakeResponse(200, QUOTES_BODY)},
        creation_timestamp=MINT_EPOCH,
    )

    def factory(token_path, *, api_key, app_secret) -> Vendor:
        return SchwabVendor(client)

    return factory


def test_recorded_cassette_file_round_trips_and_replays(tmp_path):
    cassette = record_cassette(
        "fake-api-key",
        "fake-app-secret",
        chain_symbols=["SPY"],
        quote_batches=[["SPY", "QQQ"]],
        vendor_factory=_factory(),
    )

    path = tmp_path / "recorded.json"
    dump_cassette(cassette, path)
    assert path.exists()

    replay = CassetteVendor(load_cassette(path))
    chain = replay.get_chain("SPY")
    assert chain.status == 200
    assert chain.body == CHAIN_BODY
    assert replay.get_quotes(["SPY", "QQQ"]).body == QUOTES_BODY
    assert replay.token_mint_time().isoformat() == MINT_ISO
