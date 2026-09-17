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
from datetime import UTC, datetime, timedelta, timezone

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
@pytest.mark.parametrize("which", ["start", "end"])
def test_a_bad_bound_is_refused_when_the_request_is_built(bound, which):
    # The refusal lands on the request object, before the recorder is called at all, so a
    # mistyped window never costs a live request. Both bounds are exercised: checking only
    # `start` would leave a missing or duplicated `end` check invisible.
    window = {"start": OPEN_ET, "end": CLOSE_ET} | {which: bound}
    with pytest.raises(ValueError, match=which):
        BarRequest(symbol="SPY", freq="1m", **window)


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


def test_an_inverted_window_is_refused_when_the_request_is_built():
    # Schwab answers a backwards window with the empty shape rather than an error, so a
    # transposed pair spends a live request and records "no candles" for a window nobody
    # asked for. Neither bound is wrong on its own, so only their order catches it.
    with pytest.raises(ValueError, match="is not before end"):
        BarRequest(symbol="SPY", freq="1m", start=CLOSE_ET, end=OPEN_ET)


def test_a_zero_length_window_is_refused_too():
    with pytest.raises(ValueError, match="is not before end"):
        BarRequest(symbol="SPY", freq="1m", start=OPEN_ET, end=OPEN_ET)


@pytest.mark.parametrize("symbol", ["", "   "], ids=["empty", "blank"])
def test_an_empty_symbol_is_refused_when_the_request_is_built(symbol):
    with pytest.raises(ValueError, match="symbol is empty"):
        BarRequest(symbol=symbol, freq="1m", start=OPEN_ET, end=CLOSE_ET)


def test_a_transposed_window_on_the_command_line_is_refused_by_name():
    with pytest.raises(ValueError, match="is not before end"):
        _parse_bar_requests(["SPY,1m,2026-09-14T16:00:00-04:00,2026-09-14T09:30:00-04:00"])


def test_a_comma_written_fractional_second_is_named_rather_than_only_counted():
    # ISO 8601 allows a comma as the fractional-second marker and fromisoformat accepts
    # it, so the value splits into five fields. Counting them blames the wrong thing.
    assert datetime.fromisoformat("2026-09-14T09:30:00,500-04:00").microsecond == 500_000
    with pytest.raises(ValueError, match="comma for fractional seconds"):
        _parse_bar_requests(["SPY,1m,2026-09-14T09:30:00,500-04:00,2026-09-14T16:00:00-04:00"])


def test_an_out_path_whose_directory_is_missing_is_refused_before_the_request(tmp_path):
    # dump_cassette is a plain write_text, so a missing parent raises only after the fetch
    # has happened. That loses the recording the live request just paid for, which is the
    # same loss the overwrite refusal exists to prevent.
    with pytest.raises(ValueError, match="directory that does not exist"):
        check_out_path(tmp_path / "recordings" / "spy.json")


def test_force_does_not_conjure_a_missing_directory(tmp_path):
    # --force answers "replace what is there", never "write somewhere that cannot hold it".
    with pytest.raises(ValueError, match="directory that does not exist"):
        check_out_path(tmp_path / "recordings" / "spy.json", force=True)


def test_a_checked_out_path_is_one_dump_cassette_can_actually_write(tmp_path):
    # The property the refusal exists for, asserted by executing the write rather than by
    # reading the check.
    from lake.cassette import Cassette, dump_cassette

    target = check_out_path(tmp_path / "spy.json")
    dump_cassette(Cassette(interactions=()), target)
    assert target.exists()


def test_main_refuses_a_missing_out_directory_before_it_loads_any_credentials(tmp_path, capsys):
    with pytest.raises(SystemExit) as caught:
        main(["--out", str(tmp_path / "recordings" / "spy.json")])
    assert caught.value.code == 2
    assert "directory that does not exist" in capsys.readouterr().err


def test_the_recorder_asks_the_vendor_for_the_window_it_records(tmp_path):
    """The round trip through the replay only proves the key matches the key.

    If the fetch swapped its bounds while the key kept them the way round the caller named
    them, the cassette would answer for a window the vendor was never asked about, and the
    replay would agree with itself forever. The fake client's own record is the only thing
    that sees it.
    """
    client = _bars_client()
    record_cassette(
        FAKE_KEY, FAKE_SECRET, bar_requests=[_minute_request()], vendor_factory=_factory(client)
    )
    assert client.bar_start == [OPEN_ET.astimezone(UTC)]
    assert client.bar_end == [CLOSE_ET.astimezone(UTC)]
    assert client.bar_start[0] < client.bar_end[0]


def test_a_daily_recording_replays_through_the_daily_method():
    """A recording keyed with the wrong frequency can never be found again.

    The daily test asserted only which client method was reached and threw the cassette
    away, so a recorder that keyed every window as ``1m`` looked correct.
    """
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[BarRequest(symbol="SPY", freq="1d", start=OPEN_ET, end=CLOSE_ET)],
        vendor_factory=_factory(_bars_client()),
    )
    assert (
        CassetteVendor(cassette).get_daily_bars("SPY", start=OPEN_ET, end=CLOSE_ET).body
        == BARS_BODY
    )


def test_every_requested_window_is_recorded_not_only_the_first():
    # A recorder that dropped windows after the first would spend the operator's requests
    # and silently return a short cassette, which is the same loss the --out refusal exists
    # to prevent.
    later = BarRequest(symbol="SPY", freq="1m", start=CLOSE_ET, end=CLOSE_ET + timedelta(hours=1))
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_minute_request(), later],
        vendor_factory=_factory(_bars_client()),
    )
    assert [i.endpoint for i in cassette.interactions] == ["bars", "bars"]
    assert cassette.interactions[0].params["start"] != cassette.interactions[1].params["start"]


def test_every_bars_value_on_the_command_line_is_parsed_not_only_the_first():
    parsed = _parse_bar_requests(
        [
            "SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00",
            "QQQ,1d,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00",
        ]
    )
    assert [(r.symbol, r.freq) for r in parsed] == [("SPY", "1m"), ("QQQ", "1d")]


def test_a_bars_value_is_trimmed_the_way_a_quotes_value_is():
    # --quotes trims its symbols and has a test for it. Without the same here, an operator
    # writing "SPY, 1m, ..." gets " 1m" refused as an unknown frequency.
    assert _parse_bar_requests(
        [" SPY , 1m , 2026-09-14T09:30:00-04:00 , 2026-09-14T16:00:00-04:00 "]
    ) == [_minute_request()]


def test_a_value_with_too_many_fields_is_refused_by_the_named_line():
    # A `< 4` check would let five fields reach the unpack and raise a bare "too many
    # values to unpack", which names neither the flag nor the value.
    with pytest.raises(ValueError, match="four comma-separated fields"):
        _parse_bar_requests(["SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00,extra"])


# -- the extended-hours flag ---------------------------------------------------------------


# One window, spelled both ways. #421 wants exactly this pair recorded: the first reads what
# Schwab picks when the flag is left unset, the second what it sends for the regular session.
UNFLAGGED = "SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00"
FLAGGED = f"{UNFLAGGED},extended_hours=false"


def _flagged_request(flag: bool) -> BarRequest:
    return BarRequest(symbol="SPY", freq="1m", start=OPEN_ET, end=CLOSE_ET, extended_hours=flag)


def test_the_flag_reaches_the_vendor_and_the_key_together():
    """Asking with the flag and keying without it would record an answer under the wrong question.

    Both halves are asserted with ``is``, never ``==``. A ``0`` compares equal to ``False`` both
    in the fake's call record and inside the params dict, so an ``==`` assertion here would pass
    against a value that reaches Schwab as a different query string.
    """
    client = _bars_client()
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_flagged_request(False)],
        vendor_factory=_factory(client),
    )
    assert client.bar_extended_hours[0] is False
    (interaction,) = cassette.interactions
    assert interaction.params["extended_hours"] is False


def test_a_window_with_no_flag_keys_exactly_as_it_did_before():
    """The committed cassettes key on these four alone.

    ``tests/cassettes/spy_minimal.json`` holds two ``1m`` bars interactions and
    ``spy_daily.json`` one ``1d``, all keyed ``{symbol, freq, start, end}``. A recorder that
    stamped a fifth entry when nothing asked for one would make every one of them unfindable.
    """
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_minute_request()],
        vendor_factory=_factory(_bars_client()),
    )
    (interaction,) = cassette.interactions
    assert set(interaction.params) == {"symbol", "freq", "start", "end"}


def test_the_recorded_flag_is_what_the_replay_looks_up_by():
    from lake.cassette import CassetteError

    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_flagged_request(False)],
        vendor_factory=_factory(_bars_client()),
    )
    replay = CassetteVendor(cassette)
    found = replay.get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET, extended_hours=False)
    assert found.body == BARS_BODY
    # The miss is the half worth asserting. A recorder that dropped the flag from the key would
    # still answer the line above, because the lookup would then be keyed the same way twice.
    with pytest.raises(CassetteError):
        replay.get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET)


def test_one_window_recorded_twice_keeps_the_two_answers_apart():
    """The pair #421 is for, asserted on the keys because the bodies cannot witness it.

    ``FakeSchwabClient`` keys its canned price history on ``(symbol, freq, start, end)`` and
    leaves the flag out, so both interactions carry the identical body by construction. A test
    written against the bodies would pass whether or not the key carried the flag at all.
    """
    cassette = record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[_minute_request(), _flagged_request(False)],
        vendor_factory=_factory(_bars_client()),
    )
    first, second = cassette.interactions
    assert "extended_hours" not in first.params
    assert second.params["extended_hours"] is False
    assert first.params != second.params


def test_the_daily_call_carries_the_flag_too():
    # #421 decides bars.py's daily call stays unset. That is the capture path. This tool is a
    # pass-through, and refusing the flag on 1d would block measuring what a flagged daily
    # response holds, which is the kind of thing #421 exists because nobody recorded.
    client = _bars_client()
    record_cassette(
        FAKE_KEY,
        FAKE_SECRET,
        bar_requests=[
            BarRequest(symbol="SPY", freq="1d", start=OPEN_ET, end=CLOSE_ET, extended_hours=True)
        ],
        vendor_factory=_factory(client),
    )
    assert client.bar_freqs == ["1d"]
    assert client.bar_extended_hours[0] is True


@pytest.mark.parametrize("value", [1, 0, 1.0, "true", "false"], ids=["1", "0", "float", "s", "s2"])
def test_a_flag_that_is_not_a_bool_is_refused_when_the_request_is_built(value):
    """``schwab-py`` writes the value straight into ``params["needExtendedHoursData"]``.

    So ``1`` and ``True`` leave as different query values, while the cassette key cannot tell
    them apart: Python reads ``{"extended_hours": 1} == {"extended_hours": True}`` as equal. A
    recording taken with ``1`` is found, and answers a request it never made. Refusing the value
    at the request is what prevents it, and the refusal lands before any live call.
    """
    with pytest.raises(ValueError, match="extended_hours"):
        BarRequest(symbol="SPY", freq="1m", start=OPEN_ET, end=CLOSE_ET, extended_hours=value)


def test_a_named_flag_field_parses_into_the_request():
    (parsed,) = _parse_bar_requests([FLAGGED])
    assert parsed.extended_hours is False
    assert parsed == _flagged_request(False)


def test_the_flag_field_is_trimmed_and_case_folded():
    # --bars trims its positional fields and has a test for it. The named field follows the same
    # convention, so an operator writing "SPY, 1m, ..., Extended_Hours = TRUE" is read rather
    # than refused on spacing or case.
    (parsed,) = _parse_bar_requests(
        [
            " SPY , 1m , 2026-09-14T09:30:00-04:00 , 2026-09-14T16:00:00-04:00 , "
            "Extended_Hours = TRUE "
        ]
    )
    assert parsed.extended_hours is True


def test_an_unknown_named_field_is_refused_by_its_own_name():
    # previous_close is the seam's other flag and the plausible thing to reach for. The
    # four-field refusal would blame the field count and name the ISO comma, neither of which
    # is what happened.
    with pytest.raises(ValueError, match="previous_close"):
        _parse_bar_requests([f"{UNFLAGGED},previous_close=true"])


@pytest.mark.parametrize("spelling", ["1", "0", "yes", "no", ""], ids=["1", "0", "yes", "no", ""])
def test_a_flag_value_that_is_not_true_or_false_is_refused(spelling):
    # The command line is the other door to the same conversion the request refuses. Reading
    # "1" as True here would build a legal BarRequest carrying a value nobody typed.
    with pytest.raises(ValueError, match="spells extended_hours"):
        _parse_bar_requests([f"{UNFLAGGED},extended_hours={spelling}"])


def test_a_repeated_flag_field_is_refused():
    with pytest.raises(ValueError, match="more than once"):
        _parse_bar_requests([f"{UNFLAGGED},extended_hours=true,extended_hours=false"])


def test_a_split_bound_is_still_named_even_when_a_real_flag_rides_behind_it():
    """Why the flag spells its own name instead of being a bare fifth field.

    The bound splits into two fields, the named field is taken off the end first, and the five
    positional fields that remain still reach the four-field refusal with the fractional-second
    sentence attached. A bare fifth field would have read ``2026-09-14T16:00:00-04:00`` as the
    flag value and blamed something else entirely.
    """
    with pytest.raises(ValueError, match="comma for fractional seconds"):
        _parse_bar_requests(
            ["SPY,1m,2026-09-14T09:30:00,500-04:00,2026-09-14T16:00:00-04:00,extended_hours=false"]
        )


def test_a_bare_fifth_field_is_refused_even_when_it_spells_a_flag_value():
    """The named form is a grammar, not a convenience.

    A parser that also popped a bare ``true`` or ``false`` keeps the fractional-second refusal
    working, because an ISO fractional part is digits and never spells either word. Mutation
    found that: the suite passed unchanged with the bare form accepted alongside the named one.
    It is refused anyway, because that variant rests the guard on a coincidence about the data
    rather than on the shape of the value, and because a bare ``false`` at the terminal says
    nothing about which flag it sets.
    """
    with pytest.raises(ValueError, match="four comma-separated fields"):
        _parse_bar_requests([f"{UNFLAGGED},false"])


def test_two_bars_values_with_one_key_are_refused():
    # Cassette.find returns the first exact match, so a second interaction keyed alike is a live
    # request spent on something nothing can ever read back. That is the loss the --out refusal
    # exists to prevent, arriving through a third door.
    with pytest.raises(ValueError, match="already asked for"):
        _parse_bar_requests([UNFLAGGED, UNFLAGGED])


def test_two_values_differing_only_in_the_flag_are_both_kept():
    # The refusal above must not reject the pair the whole change is for.
    first, second = _parse_bar_requests([UNFLAGGED, FLAGGED])
    assert first.extended_hours is None
    assert second.extended_hours is False


def test_a_duplicate_is_caught_below_the_millisecond_the_key_carries():
    """The guard compares keys, not requests.

    ``_key_instant`` truncates each bound to the millisecond ``schwab-py`` puts on the wire, on
    purpose, so these two values build unequal ``BarRequest`` objects that key identically. A
    guard written as ``set(bar_requests)`` or a pairwise ``==`` passes both through and records
    two interactions the replay cannot tell apart.
    """
    finer = "SPY,1m,2026-09-14T09:30:00.000400-04:00,2026-09-14T16:00:00-04:00"
    assert _parse_bar_requests([finer]) != _parse_bar_requests([UNFLAGGED])
    with pytest.raises(ValueError, match="already asked for"):
        _parse_bar_requests([UNFLAGGED, finer])


def test_main_refuses_a_duplicate_window_as_one_line_and_exit_two(tmp_path, capsys):
    """Where the refusal surfaces, not just that it is raised.

    ``main`` wraps only ``_parse_bar_requests`` and ``check_out_path`` in the try that hands a
    ValueError to ``parser.error``. The same refusal raised inside ``record_cassette`` would
    reach the operator as a stack trace, which is why it lives where it does.
    """
    with pytest.raises(SystemExit) as caught:
        main(["--out", str(tmp_path / "c.json"), "--bars", UNFLAGGED, "--bars", UNFLAGGED])
    assert caught.value.code == 2
    assert "already asked for" in capsys.readouterr().err


def test_the_help_names_the_flag_the_operator_has_to_spell():
    # The recording is taken by hand at a terminal. --help is where its spelling is read, and a
    # named field the help does not name is one nobody can guess.
    rendered = " ".join(build_parser().format_help().split())
    assert "extended_hours=true" in rendered
    assert "extended_hours=false" in rendered
