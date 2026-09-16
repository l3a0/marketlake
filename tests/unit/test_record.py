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
from datetime import datetime, timedelta, timezone

import pytest

from lake.record import (
    BarRequest,
    _parse_bar_requests,
    _parse_quote_batches,
    build_parser,
    check_out_path,
    main,
    record_cassette,
)
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


# -- price history -------------------------------------------------------------
#
# The recorder grows a third endpoint here, because no price-history payload exists
# anywhere in the repo and this is the one tool that can capture one. Everything below
# still runs offline through the injected factory.


EASTERN = timezone(timedelta(hours=-4))
OPEN_ET = datetime(2026, 9, 14, 9, 30, tzinfo=EASTERN)
CLOSE_ET = datetime(2026, 9, 14, 16, 0, tzinfo=EASTERN)
BARS_BODY = {
    "candles": [{"open": 650.0, "high": 650.4, "low": 649.8, "close": 650.2, "volume": 1}],
    "symbol": "SPY",
    "empty": False,
}


def _bars_client() -> FakeSchwabClient:
    return FakeSchwabClient(
        chains={"SPY": FakeResponse(200, CHAIN_BODY)},
        quotes={("SPY", "QQQ"): FakeResponse(200, QUOTES_BODY)},
        bars={
            ("SPY", "1m"): FakeResponse(200, BARS_BODY),
            ("SPY", "1d"): FakeResponse(200, BARS_BODY),
        },
        creation_timestamp=MINT_EPOCH,
    )


def _minute_request() -> BarRequest:
    return BarRequest(symbol="SPY", freq="1m", start=OPEN_ET, end=CLOSE_ET)


def test_records_a_price_history_window_keyed_the_way_the_replay_asks():
    client = _bars_client()
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_minute_request()],
        vendor_factory=_factory(client),
    )
    # The recorded key is built by the same function the replay looks up by, so the
    # round trip is the assertion rather than a second spelling of the key.
    replay = CassetteVendor(cassette)
    assert replay.get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET).body == BARS_BODY
    assert client.bar_freqs == ["1m"]


def test_a_daily_request_reaches_the_daily_vendor_method():
    client = _bars_client()
    record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[BarRequest(symbol="SPY", freq="1d", start=OPEN_ET, end=CLOSE_ET)],
        vendor_factory=_factory(client),
    )
    assert client.bar_freqs == ["1d"]


def test_nothing_is_recorded_unless_a_window_is_asked_for():
    # test_records_only_the_requested_interactions_in_order asserts the exact list, so a
    # flag that recorded something by default would break it. This says the same thing
    # from the other side.
    cassette = record_cassette(
        FAKE_KEY, FAKE_SECRET, chain_symbols=["SPY"], vendor_factory=_factory(_bars_client())
    )
    assert [i.endpoint for i in cassette.interactions] == ["chains"]


def test_a_price_history_request_is_recorded_after_the_chains_and_quotes():
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        chain_symbols=["SPY"],
        quote_batches=[["SPY", "QQQ"]],
        bar_requests=[_minute_request()],
        vendor_factory=_factory(_bars_client()),
    )
    assert [i.endpoint for i in cassette.interactions] == ["chains", "quotes", "bars"]


@pytest.mark.parametrize("bound", [datetime(2026, 9, 14, 9, 30), None], ids=["naive", "missing"])
def test_a_bad_bound_is_refused_when_the_request_is_built(bound):
    # The refusal lands on the request object, before the recorder is called at all, so
    # a mistyped window never costs a live request.
    with pytest.raises(ValueError):
        BarRequest(symbol="SPY", freq="1m", start=bound, end=CLOSE_ET)


def test_an_unknown_frequency_is_refused_when_the_request_is_built():
    with pytest.raises(ValueError, match="not one of"):
        BarRequest(symbol="SPY", freq="5m", start=OPEN_ET, end=CLOSE_ET)


# -- the command line ----------------------------------------------------------


def test_parser_collects_bar_windows_and_the_force_switch():
    args = build_parser().parse_args(
        [
            "--out",
            "c.json",
            "--bars",
            "SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00",
            "--force",
        ]
    )
    assert args.bar_requests == ["SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00"]
    assert args.force is True


def test_bar_windows_default_to_none_asked_for():
    args = build_parser().parse_args(["--out", "c.json"])
    assert args.bar_requests == []
    assert args.force is False


def test_a_bar_value_splits_into_its_four_fields():
    parsed = _parse_bar_requests(["SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00"])
    assert parsed == [_minute_request()]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("SPY,1m,2026-09-14T09:30:00-04:00", "four comma-separated fields"),
        ("SPY,1m,not-a-time,2026-09-14T16:00:00-04:00", "unreadable instant"),
        ("SPY,5m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00", "not one of"),
        ("SPY,1m,2026-09-14T09:30:00,2026-09-14T16:00:00-04:00", "timezone-aware"),
    ],
    ids=["short", "unreadable", "bad-freq", "naive"],
)
def test_a_bad_bar_value_is_refused_by_a_line_naming_what_was_wrong(raw, message):
    # Every one of these reaches the operator through parser.error, which prints one line
    # and exits 2. A ValueError escaping main would print a stack trace instead.
    with pytest.raises(ValueError, match=message):
        _parse_bar_requests([raw])


def test_an_existing_out_path_is_refused_rather_than_overwritten(tmp_path):
    # A recording costs a live token and a moment of market hours that does not come
    # back. dump_cassette writes whatever path it is given, so refusing here is the
    # decision that replaces a silent overwrite.
    existing = tmp_path / "spy.json"
    existing.write_text("{}")
    with pytest.raises(ValueError, match="already exists"):
        check_out_path(existing)


def test_force_overwrites_an_existing_out_path(tmp_path):
    existing = tmp_path / "spy.json"
    existing.write_text("{}")
    assert check_out_path(existing, force=True) == existing


def test_a_fresh_out_path_is_accepted(tmp_path):
    fresh = tmp_path / "new.json"
    assert check_out_path(fresh) == fresh
    assert check_out_path(fresh, force=True) == fresh


def test_main_refuses_a_bad_window_as_one_line_and_exit_two(tmp_path, capsys):
    """The refusal reaches the operator through argparse, not as a stack trace.

    It also runs before ``load_config``, which is why this passes with no ``config.yaml``
    anywhere. A mistyped bound therefore costs neither a credential read nor a request.
    """
    with pytest.raises(SystemExit) as caught:
        main(
            [
                "--out",
                str(tmp_path / "c.json"),
                "--bars",
                "SPY,1m,not-a-time,2026-09-14T16:00:00-04:00",
            ]
        )
    assert caught.value.code == 2
    assert "unreadable instant" in capsys.readouterr().err


def test_main_refuses_an_existing_out_path_before_it_loads_any_credentials(tmp_path, capsys):
    existing = tmp_path / "spy.json"
    existing.write_text("{}")
    with pytest.raises(SystemExit) as caught:
        main(["--out", str(existing)])
    assert caught.value.code == 2
    assert "already exists" in capsys.readouterr().err
    # The recording that was already there is untouched.
    assert existing.read_text() == "{}"
