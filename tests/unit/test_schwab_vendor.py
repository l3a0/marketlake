"""The real Schwab vendor: it shapes requests, returns bodies verbatim, and reads
the mint time off the injected token.

Every test here injects a fake client, so no network and no real token are involved.
These are value-only unit tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from lake.schwab import QUOTE_FIELD_GROUPS, SchwabVendor
from lake.vendor import Vendor, VendorError, VendorResponse
from tests.support.schwab import FakeResponse, FakeSchwabClient

# A fixed token mint epoch second: 2026-08-24 00:05:00 UTC. No wall clock is read.
MINT_EPOCH = 1787529900.0

CHAIN_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "underlyingPrice": 650.01,
    "callExpDateMap": {"2026-09-18:25": {"650.0": [{"putCall": "CALL", "bid": 4.2}]}},
}
QUOTES_BODY = {
    "SPY": {"quote": {"bidPrice": 649.98, "askPrice": 650.02}},
    "QQQ": {"quote": {"bidPrice": 601.48, "askPrice": 601.52}},
}


def _client() -> FakeSchwabClient:
    return FakeSchwabClient(
        chains={"SPY": FakeResponse(200, CHAIN_BODY, {"content-type": "application/json"})},
        quotes={("SPY", "QQQ"): FakeResponse(200, QUOTES_BODY)},
        creation_timestamp=MINT_EPOCH,
    )


def test_get_chain_calls_the_chain_endpoint_for_the_symbol():
    client = _client()
    vendor = SchwabVendor(client)
    response = vendor.get_chain("SPY")
    assert client.chain_calls == ["SPY"]
    assert isinstance(response, VendorResponse)
    assert response.status == 200
    assert response.body == CHAIN_BODY
    assert response.headers == {"content-type": "application/json"}


def test_get_chain_requests_the_underlying_quote():
    # The chain must carry the underlying's price and quote time beside the contracts,
    # at the same moment. That embedded reading is the design's IV spot, and the
    # chain's ``vendor_quote_ts`` comes from the underlying's quote time.
    client = _client()
    SchwabVendor(client).get_chain("SPY")
    assert client.chain_underlying_quote == [True]


def test_get_chain_forwards_no_narrowing_by_default():
    # The bare chain call must leave every narrowing parameter unset, so schwab-py omits
    # them and returns the full chain. This is the path onboarding and the recorder use.
    client = _client()
    SchwabVendor(client).get_chain("SPY")
    assert client.chain_from_date == [None]
    assert client.chain_to_date == [None]
    assert client.chain_strike_count == [None]


def test_get_chain_forwards_strike_count_for_discovery():
    # The chunker discovers the expiration list with a strike_count=1 probe. The vendor
    # must forward that to the client verbatim, with no date bounds.
    client = _client()
    SchwabVendor(client).get_chain("SPY", strike_count=1)
    assert client.chain_strike_count == [1]
    assert client.chain_from_date == [None]
    assert client.chain_to_date == [None]


def test_get_chain_forwards_the_expiration_range_for_a_chunk():
    # Each chunk fetch bounds the request by a from/to date. Both must reach the client.
    client = _client()
    frm, to = date(2026, 9, 18), date(2026, 10, 16)
    SchwabVendor(client).get_chain("SPY", from_date=frm, to_date=to)
    assert client.chain_from_date == [frm]
    assert client.chain_to_date == [to]
    assert client.chain_strike_count == [None]


def test_get_chain_serves_a_response_keyed_by_the_request():
    # The fake client returns distinct canned responses for the discovery probe and each
    # chunk, keyed by the full request tuple, so a chunker test can drive them apart.
    discovery = FakeResponse(200, {"discovery": True})
    chunk = FakeResponse(200, {"chunk": True})
    frm, to = date(2026, 9, 18), date(2026, 10, 16)
    client = FakeSchwabClient(
        chains={
            ("SPY", None, None, 1): discovery,
            ("SPY", frm, to, None): chunk,
        }
    )
    vendor = SchwabVendor(client)
    assert vendor.get_chain("SPY", strike_count=1).body == {"discovery": True}
    assert vendor.get_chain("SPY", from_date=frm, to_date=to).body == {"chunk": True}


def test_get_chain_returns_the_body_verbatim():
    vendor = SchwabVendor(_client())
    body = vendor.get_chain("SPY").body
    # The vendor must not parse or reshape. The exact object json() returned is handed
    # back, and the nested vendor payload is untouched.
    assert body is CHAIN_BODY
    assert body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]["putCall"] == "CALL"


def test_get_quotes_calls_the_batched_endpoint_with_the_symbol_list():
    client = _client()
    vendor = SchwabVendor(client)
    response = vendor.get_quotes(["SPY", "QQQ"])
    assert client.quote_calls == [["SPY", "QQQ"]]
    assert response.status == 200
    assert response.body == QUOTES_BODY


def test_get_quotes_pins_every_field_group():
    # The fundamental, regular, extended, and reference blocks live in their own field
    # groups. Requesting all of them explicitly means every block is present regardless of
    # the account default, so no captured column is silently empty. schwab-py wants an
    # iterable of the field-group values, not a joined string, so the vendor passes a list.
    client = _client()
    SchwabVendor(client).get_quotes(["SPY", "QQQ"])
    assert list(QUOTE_FIELD_GROUPS) == ["quote", "fundamental", "regular", "extended", "reference"]
    assert client.quote_fields == [list(QUOTE_FIELD_GROUPS)]


def test_get_quotes_passes_a_plain_list_to_the_client():
    # A tuple argument must still reach the client as the same ordered symbols, since
    # schwab-py batches a list. The fake keys on the tuple of what it received.
    vendor = SchwabVendor(_client())
    response = vendor.get_quotes(("SPY", "QQQ"))
    assert response.body == QUOTES_BODY


def test_token_mint_time_reads_off_the_injected_token():
    vendor = SchwabVendor(_client())
    minted = vendor.token_mint_time()
    assert minted == datetime.fromtimestamp(MINT_EPOCH, tz=UTC)
    assert minted.tzinfo is not None
    assert minted.utcoffset() == timedelta(0)  # returned in UTC


def test_token_mint_time_raises_without_a_creation_timestamp():
    client = FakeSchwabClient(chains={}, quotes={}, creation_timestamp=None)
    with pytest.raises(VendorError):
        SchwabVendor(client).token_mint_time()


def test_headers_are_copied_not_aliased():
    client = _client()
    vendor = SchwabVendor(client)
    headers = vendor.get_chain("SPY").headers
    headers["injected"] = "mutation"
    # Mutating the returned headers must not reach back into the client's response.
    assert "injected" not in client.get_option_chain("SPY").headers


def test_schwab_vendor_satisfies_the_vendor_protocol():
    assert isinstance(SchwabVendor(_client()), Vendor)
