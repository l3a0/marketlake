"""The real Schwab vendor: it shapes requests, returns bodies verbatim, and reads
the mint time off the injected token.

Every test here injects a fake client, so no network and no real token are involved.
These are value-only unit tests.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from lake.capture import _error_class
from lake.schwab import QUOTE_FIELD_GROUPS, SchwabVendor, VendorAuthError, VendorBodyError
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
    # This message is the client's own missing-field check, distinct from the shared
    # guard's "is not an epoch second", so the match pins which check actually fired.
    with pytest.raises(VendorError, match="has no creation_timestamp"):
        SchwabVendor(client).token_mint_time()


def test_token_mint_time_raises_when_the_metadata_carries_no_field_at_all():
    # FakeSchwabClient always sets creation_timestamp, even to None, which never
    # exercises the getattr default this reads through. A real client whose metadata
    # object lacks the attribute entirely must be refused the same way.
    class _BareTokenMetadata:
        """A token handle with no ``creation_timestamp`` attribute at all."""

    client = FakeSchwabClient(creation_timestamp=MINT_EPOCH)
    client.token_metadata = _BareTokenMetadata()
    with pytest.raises(VendorError, match="has no creation_timestamp"):
        SchwabVendor(client).token_mint_time()


@pytest.mark.parametrize("stamp", [True, False])
def test_token_mint_time_refuses_a_bool_creation_timestamp(stamp):
    # float(True) is 1.0 and float(False) is 0.0, so a bare float() conversion would
    # return 1970-01-01 rather than raise. Neither is a stamp schwab-py writes.
    client = FakeSchwabClient(creation_timestamp=stamp)
    with pytest.raises(VendorError, match="epoch second") as excinfo:
        SchwabVendor(client).token_mint_time()
    # The original is kept as the cause, so a traceback still says why it was refused.
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_token_mint_time_refuses_a_numeric_string_creation_timestamp():
    # The token file has no history of a numeric-string creation_timestamp, unlike a
    # vendor payload's epoch-millisecond fields, so this reader refuses it by name too.
    client = FakeSchwabClient(creation_timestamp=str(int(MINT_EPOCH)))
    with pytest.raises(VendorError, match="epoch second"):
        SchwabVendor(client).token_mint_time()


def test_token_mint_time_reports_an_out_of_range_stamp_rather_than_raising_overflow():
    client = FakeSchwabClient(creation_timestamp=10**400)
    with pytest.raises(VendorError, match="epoch second") as excinfo:
        SchwabVendor(client).token_mint_time()
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_token_mint_time_lets_an_unrelated_failure_pass_through_unwrapped(monkeypatch):
    """Only a refusal from the shared guard is reclassified as VendorError.

    This pins the narrow ``except ValueError`` in ``token_mint_time`` against
    widening to a bare ``except Exception``, which would silently relabel any bug in
    the shared helper as a vendor fault instead of letting it surface as itself.
    """

    def _broken_helper(_value: object):
        raise RuntimeError("not a guard failure")

    monkeypatch.setattr("lake.schwab.epoch_second_to_utc", _broken_helper)
    client = FakeSchwabClient(creation_timestamp=MINT_EPOCH)
    with pytest.raises(RuntimeError, match="not a guard failure"):
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


# -- credential failures -------------------------------------------------------


class AuthlibBaseError(Exception):
    """Stands in for authlib's base error, matched by name rather than by import."""


class OAuthError(AuthlibBaseError):
    """The one authlib actually raises when a refresh token is dead."""


class _RaisingClient:
    """A client whose every call raises, so the wrapper is what is under test."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def get_option_chain(self, *args, **kwargs):
        raise self._exc

    def get_quotes(self, *args, **kwargs):
        raise self._exc


@pytest.mark.parametrize("method", ["chain", "quotes"])
def test_a_credential_failure_is_named_by_the_lake_not_by_the_library(method):
    """Both fetch paths classify a dead token the same way.

    A refresh that fails sends no request, so there is no status to record and the
    library raises instead. Left alone that lands under whatever the library named its
    exception, which is not what the watchdog watches for.
    """
    vendor = SchwabVendor(_RaisingClient(OAuthError("refresh token expired")))
    with pytest.raises(VendorAuthError) as caught:
        if method == "chain":
            vendor.get_chain("SPY")
        else:
            vendor.get_quotes(["SPY"])
    # The original is kept, so a traceback still says what the library said.
    assert isinstance(caught.value.__cause__, OAuthError)


def test_an_unknown_authlib_subclass_still_classifies_as_auth():
    """This is the difference between fixing the class and fixing one instance.

    authlib ships several credential errors and may add more. Matching the base rather
    than each leaf means one this code has never seen still reads as auth death.
    """

    class SomeFutureTokenError(AuthlibBaseError):
        pass

    vendor = SchwabVendor(_RaisingClient(SomeFutureTokenError("new in some release")))
    with pytest.raises(VendorAuthError):
        vendor.get_quotes(["SPY"])


def test_a_failure_that_is_not_about_credentials_passes_through():
    """Only the classification changes, and only for credential failures."""
    vendor = SchwabVendor(_RaisingClient(TimeoutError("read timed out")))
    with pytest.raises(TimeoutError):
        vendor.get_quotes(["SPY"])


def test_the_lake_owned_class_is_what_the_watchdog_watches_for():
    """The gap's error_class must be the string the whole-daemon map carries.

    This is the join between the two modules. Renaming the exception without updating
    the map would leave a dead token gapping under a class nothing pages on, which is
    the defect this pair of changes exists to close.
    """
    from lake.watchdog import _WHOLE_DAEMON_CAUSES

    assert _error_class(VendorAuthError("dead")) == "vendor_auth_error"
    assert _WHOLE_DAEMON_CAUSES["vendor_auth_error"] == "Capture down: token dead"


def test_the_matched_names_are_the_ones_authlib_actually_raises():
    """The one fact the whole classification rests on, checked against the real library.

    Every other test here hands the matcher a locally defined stand-in, so it proves the
    matcher matches what the test invented. It does not prove those are the names authlib
    uses. A rename in a dependency bump would revert capture to the 2026-09-08 incident
    with the suite still green, and this is the test that would go red instead.
    """
    pytest.importorskip("authlib")
    from authlib.common.errors import AuthlibBaseError
    from authlib.integrations.base_client.errors import OAuthError

    from lake.schwab import _AUTH_BASE_NAMES

    assert AuthlibBaseError.__name__ in _AUTH_BASE_NAMES
    assert OAuthError.__name__ in _AUTH_BASE_NAMES
    # The leaf really does inherit the base, which is why matching the base is enough.
    assert issubclass(OAuthError, AuthlibBaseError)


# -- a body that is not a JSON object ----------------------------------------------------
#
# Every body below is a literal, so none of these tests can move with a constant in the code
# under test. The statuses are literals for the same reason: the watchdog pages on the exact
# strings ``http_401``, ``http_403`` and ``http_429``, and a status recomputed from the code's
# own success range would agree with the code whatever that range became.

_HTML = b"<html><head><title>429 Too Many Requests</title></head><body>slow down</body></html>"

# Bodies that are not a JSON object, each with the text ``httpx`` would decode it to. The last
# two are not valid UTF-8, and they fail the parse in different ways. ``json.loads`` reads a
# leading ``\xff\xfe`` as a UTF-16 byte-order mark and raises ``JSONDecodeError``. A Latin-1
# page with no mark raises ``UnicodeDecodeError`` instead, which is why the catch is
# ``ValueError`` rather than ``JSONDecodeError``. The whitespace body checks that the text is
# kept verbatim rather than tidied.
_NOT_AN_OBJECT = [
    pytest.param(_HTML, _HTML.decode(), id="html"),
    pytest.param(b"", "", id="empty"),
    pytest.param(b"  busy\n", "  busy\n", id="whitespace"),
    pytest.param(b"[]", "[]", id="json-list"),
    pytest.param(b"null", "null", id="json-null"),
    pytest.param(b'"slow down"', '"slow down"', id="json-string"),
    pytest.param(b"\xff\xfe<html>", "��<html>", id="bom-then-html"),
    pytest.param(b"<p>caf\xe9</p>", "<p>caf�</p>", id="latin-1"),
]


def _vendor_answering(reply: FakeResponse) -> SchwabVendor:
    """A vendor whose every request, on all four endpoints, gets ``reply``."""
    return SchwabVendor(
        FakeSchwabClient(
            chains={"SPY": reply},
            quotes={("SPY",): reply},
            bars={("SPY", "1m"): reply, ("SPY", "1d"): reply},
        )
    )


_OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
_CLOSE = datetime(2026, 9, 14, 20, 0, tzinfo=UTC)

# The four vendor methods. Each shapes its own reply through ``_response_from``, and a method
# that shaped its reply any other way would bring the old failure back on that endpoint alone,
# so the rules below are asserted on every one of them rather than on the chain.
_FETCHES = [
    pytest.param(lambda vendor: vendor.get_chain("SPY"), id="chain"),
    pytest.param(lambda vendor: vendor.get_quotes(["SPY"]), id="quotes"),
    pytest.param(
        lambda vendor: vendor.get_minute_bars("SPY", start=_OPEN, end=_CLOSE), id="minute-bars"
    ),
    pytest.param(
        lambda vendor: vendor.get_daily_bars("SPY", start=_OPEN, end=_CLOSE), id="daily-bars"
    ),
]


@pytest.mark.parametrize("fetch", _FETCHES)
@pytest.mark.parametrize("status", [401, 403, 429, 502])
@pytest.mark.parametrize(("content", "text"), _NOT_AN_OBJECT)
def test_a_failed_reply_keeps_its_status_whatever_its_body_is(fetch, status, content, text):
    """The status is the signal, so a body that is not an object must not cost it.

    The watchdog pages "rate limited" on ``http_429`` and "token dead" on ``http_401``. A
    gateway's HTML page used to raise out of the parse before the status was read, and a
    body parsing to a list raised in capture instead, which took the whole cycle down.
    """
    reply = FakeResponse(status, headers={"content-type": "text/html"}, content=content)
    response = fetch(_vendor_answering(reply))
    assert response.status == status
    assert response.body == {}
    assert response.body_text == text
    assert response.headers == {"content-type": "text/html"}


def test_a_failed_reply_with_an_object_body_is_handed_back_unchanged():
    """The other side of the fallback: an error body that is an object is the payload."""
    reply = FakeResponse(429, content=b'{"errors": [{"id": "429-005"}]}')
    response = _vendor_answering(reply).get_chain("SPY")
    assert response.status == 429
    assert response.body == {"errors": [{"id": "429-005"}]}
    assert response.body_text is None


def test_a_failed_reply_whose_object_body_is_empty_is_told_apart_from_the_fallback():
    """``body_text`` is what separates a vendor that sent ``{}`` from one that sent HTML."""
    sent_empty = _vendor_answering(FakeResponse(429, content=b"{}")).get_chain("SPY")
    sent_html = _vendor_answering(FakeResponse(429, content=_HTML)).get_chain("SPY")
    assert sent_empty.body == sent_html.body == {}
    assert sent_empty.body_text is None
    assert sent_html.body_text == _HTML.decode()


def test_a_successful_object_body_is_handed_back_with_no_text():
    response = _vendor_answering(FakeResponse(200, content=b'{"symbol": "SPY"}')).get_chain("SPY")
    assert response.status == 200
    assert response.body == {"symbol": "SPY"}
    assert response.body_text is None


@pytest.mark.parametrize("fetch", _FETCHES)
@pytest.mark.parametrize("status", [200, 203, 299])
@pytest.mark.parametrize(("content", "text"), _NOT_AN_OBJECT)
def test_a_successful_reply_whose_body_is_not_an_object_is_refused(fetch, status, content, text):
    """A 2xx body is the payload, so an empty mapping would read as an empty success.

    Schwab already answers 200 with empty expiration maps on purpose, and the close+5 fill
    treats that as a close nobody captured. A malformed payload must not look the same.
    """
    with pytest.raises(VendorBodyError) as refused:
        fetch(_vendor_answering(FakeResponse(status, content=content)))
    assert f"http {status} " in str(refused.value)


@pytest.mark.parametrize("status", [199, 300, 302])
def test_the_success_range_ends_where_http_says(status):
    """Either side of the 2xx range, a body that is not an object is a failed reply."""
    response = _vendor_answering(FakeResponse(status, content=_HTML)).get_chain("SPY")
    assert response.status == status
    assert response.body_text == _HTML.decode()


def test_the_refusal_is_a_vendor_error_named_by_the_lake():
    """The bars walk contains ``VendorError`` per ticker-day and nothing broader, and capture
    records the class by name."""
    assert issubclass(VendorBodyError, VendorError)
    assert _error_class(VendorBodyError("x")) == "vendor_body_error"


def test_the_refusal_says_why_and_never_quotes_the_body():
    """The bars walk writes the message into a finding in the lake, so the body stays out."""
    reply = FakeResponse(200, headers={"content-type": "text/html"}, content=_HTML)
    with pytest.raises(VendorBodyError) as not_json:
        _vendor_answering(reply).get_chain("SPY")
    message = str(not_json.value)
    assert "http 200" in message
    # The parse error's own line says where the body stopped being JSON. It is ``json``'s
    # text rather than the body's, so it is the one detail the message can carry.
    assert "not JSON (Expecting value: line 1 column 1 (char 0))" in message
    assert "'text/html'" in message
    assert f"{len(_HTML)} characters" in message
    assert "slow down" not in message
    assert "<html>" not in message
    assert isinstance(not_json.value.__cause__, ValueError)

    with pytest.raises(VendorBodyError) as a_list:
        _vendor_answering(FakeResponse(200, content=b'["SPY"]')).get_chain("SPY")
    assert "parses to list" in str(a_list.value)
    assert "SPY" not in str(a_list.value)
    assert a_list.value.__cause__ is None


def test_a_parse_failure_that_is_not_about_the_body_passes_through():
    """Only ``ValueError`` means the body is not JSON. Anything else is not caught here."""

    class _Broken(FakeResponse):
        def json(self):
            raise RuntimeError("the client broke")

    with pytest.raises(RuntimeError, match="the client broke"):
        _vendor_answering(_Broken(429)).get_chain("SPY")


def test_the_fake_parses_and_decodes_the_way_httpx_does():
    """Every test above trusts ``FakeResponse``'s ``content`` form. This checks it against
    the real ``httpx.Response`` over the same literal bytes."""
    httpx = pytest.importorskip("httpx")
    for content in (
        _HTML,
        b"",
        b"  busy\n",
        b"[]",
        b"null",
        b'"slow down"',
        b"\xff\xfe<html>",
        b"<p>caf\xe9</p>",
        b"{}",
    ):
        real = httpx.Response(429, content=content)
        fake = FakeResponse(429, content=content)
        assert real.text == fake.text
        try:
            expected = real.json()
        except ValueError:
            with pytest.raises(ValueError):
                fake.json()
        else:
            assert fake.json() == expected


def test_the_real_httpx_reply_satisfies_the_protocol_end_to_end():
    """The vendor over a real ``httpx.Response`` keeps a 429's status on an HTML body."""
    httpx = pytest.importorskip("httpx")
    reply = httpx.Response(429, content=_HTML, headers={"content-type": "text/html"})
    response = _vendor_answering(reply).get_chain("SPY")
    assert response.status == 429
    assert response.body == {}
    assert response.body_text == _HTML.decode()
