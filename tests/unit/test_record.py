"""The cassette recorder: it resolves credentials, builds a vendor, and shapes each
reply into a keyed interaction.

Every test here injects two fakes: plain-string credentials, and a vendor factory
that returns a fake-``schwab-py``-client vendor. So the recorder's shaping runs
offline, with no network, no real token, and no ``lake.config`` import. The recorded
cassette is replayed in memory through ``CassetteVendor`` here. The real-file round
trip lives in the component tier.
"""

from __future__ import annotations

from collections.abc import Callable

from lake.record import _parse_quote_batches, build_parser, record_cassette
from lake.schwab import SchwabVendor
from lake.vendor import Vendor
from tests.support.schwab import FakeResponse, FakeSchwabClient
from tests.support.vendor import CassetteVendor

# A fixed token mint epoch second: 2026-08-24 00:05:00 UTC. No wall clock is read.
MINT_EPOCH = 1787529900.0
MINT_ISO = "2026-08-24T00:05:00+00:00"

CHAIN_BODY = {"symbol": "SPY", "status": "SUCCESS", "underlyingPrice": 650.01}
QUOTES_BODY = {"SPY": {"quote": {"bidPrice": 649.98}}}

# Fake credentials. The recorder never inspects them; the factory does.
FAKE_KEY = "fake-api-key"
FAKE_SECRET = "fake-app-secret"


def _client(*, creation_timestamp: float | None = MINT_EPOCH) -> FakeSchwabClient:
    return FakeSchwabClient(
        chains={"SPY": FakeResponse(200, CHAIN_BODY, {"content-type": "application/json"})},
        quotes={("SPY", "QQQ"): FakeResponse(200, QUOTES_BODY)},
        creation_timestamp=creation_timestamp,
    )


def _factory(client: FakeSchwabClient, captured: dict | None = None) -> Callable[..., Vendor]:
    """A vendor factory returning a fake-client vendor, matching ``from_token``'s shape."""

    def factory(token_path, *, api_key, app_secret) -> Vendor:
        if captured is not None:
            captured.update(token_path=token_path, api_key=api_key, app_secret=app_secret)
        return SchwabVendor(client)

    return factory


def test_records_a_chain_interaction_keyed_for_replay():
    cassette = record_cassette(
        FAKE_KEY, FAKE_SECRET, chain_symbols=["SPY"], vendor_factory=_factory(_client())
    )
    interaction = cassette.find("chains", {"symbol": "SPY"})
    assert interaction.status == 200
    assert interaction.body == CHAIN_BODY
    assert interaction.headers == {"content-type": "application/json"}


def test_records_a_quote_batch_keyed_on_the_symbol_list():
    cassette = record_cassette(
        FAKE_KEY, FAKE_SECRET, quote_batches=[["SPY", "QQQ"]], vendor_factory=_factory(_client())
    )
    interaction = cassette.find("quotes", {"symbols": ["SPY", "QQQ"]})
    assert interaction.body == QUOTES_BODY


def test_records_only_the_requested_interactions_in_order():
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        chain_symbols=["SPY"],
        quote_batches=[["SPY", "QQQ"]],
        vendor_factory=_factory(_client()),
    )
    assert [(i.endpoint, i.params) for i in cassette.interactions] == [
        ("chains", {"symbol": "SPY"}),
        ("quotes", {"symbols": ["SPY", "QQQ"]}),
    ]


def test_credentials_and_token_path_flow_through_to_the_factory():
    captured: dict = {}
    record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        chain_symbols=["SPY"],
        token_path="/tmp/fake-token.json",
        vendor_factory=_factory(_client(), captured),
    )
    assert captured == {
        "token_path": "/tmp/fake-token.json",
        "api_key": FAKE_KEY,
        "app_secret": FAKE_SECRET,
    }


def test_recorded_cassette_replays_through_the_cassette_vendor():
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        chain_symbols=["SPY"],
        quote_batches=[["SPY", "QQQ"]],
        vendor_factory=_factory(_client()),
    )
    replay = CassetteVendor(cassette)
    assert replay.get_chain("SPY").body == CHAIN_BODY
    assert replay.get_quotes(["SPY", "QQQ"]).body == QUOTES_BODY
    assert replay.token_mint_time().isoformat() == MINT_ISO


def test_token_mint_time_is_omitted_when_the_vendor_has_none():
    # A client whose token metadata carries no mint time makes the vendor's mint call
    # raise, so the recorder omits it rather than failing the whole recording.
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        chain_symbols=["SPY"],
        vendor_factory=_factory(_client(creation_timestamp=None)),
    )
    assert cassette.token_mint_time is None


def test_empty_spec_records_nothing():
    cassette = record_cassette(FAKE_KEY, FAKE_SECRET, vendor_factory=_factory(_client()))
    assert cassette.interactions == ()


def test_parser_collects_chains_quotes_and_out():
    args = build_parser().parse_args(
        ["--out", "c.json", "--chain", "SPY", "--chain", "QQQ", "--quotes", "SPY,QQQ"]
    )
    assert args.out == "c.json"
    assert args.chains == ["SPY", "QQQ"]
    assert args.quote_batches == ["SPY,QQQ"]


def test_quote_batch_splitting_trims_and_drops_blanks():
    assert _parse_quote_batches(["SPY, QQQ ", "IWM"]) == [["SPY", "QQQ"], ["IWM"]]
    assert _parse_quote_batches(["SPY,,QQQ,"]) == [["SPY", "QQQ"]]
