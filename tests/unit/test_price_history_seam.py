"""The widened vendor seam: the window contract, the cassette key, and the two methods.

Every test here is offline. The real vendor runs against an injected fake client and the
cassette vendor runs against an in-memory cassette, so no network, no token, and no
``schwab-py`` are involved.

The window is where this seam departs from its own ``None`` rule, and that is what most
of these pin. ``schwab-py`` substitutes rather than omits: a missing bound becomes
1971-01-01 or seven days from now, and a naive bound is read in the host's local zone.
Both are silent, so both are refused here instead.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone, tzinfo

import pytest

from lake.cassette import Cassette, CassetteError
from lake.schwab import SchwabVendor, VendorAuthError
from lake.vendor import (
    BAR_FREQS,
    BARS_ENDPOINT,
    DAILY_FREQ,
    MINUTE_FREQ,
    Vendor,
    bars_params,
    require_bar_freq,
    require_utc_bound,
)
from tests.support.schwab import FakeResponse, FakeSchwabClient
from tests.support.vendor import (
    CassetteVendor,
    bars_candle,
    bars_interactions,
    price_history_body,
)

EASTERN = timezone(timedelta(hours=-4))
# One regular session, 2026-09-14, named in Eastern the way ``SessionClock.bounds`` names
# it. The same two instants in UTC follow, so a test can name one moment two ways.
OPEN_ET = datetime(2026, 9, 14, 9, 30, tzinfo=EASTERN)
CLOSE_ET = datetime(2026, 9, 14, 16, 0, tzinfo=EASTERN)
OPEN_UTC = OPEN_ET.astimezone(UTC)
CLOSE_UTC = CLOSE_ET.astimezone(UTC)

CANDLES = [bars_candle(OPEN_ET, open_=650.0, high=650.4, low=649.8, close=650.2, volume=1_200_000)]
BARS_BODY = price_history_body("SPY", CANDLES)
EMPTY_BODY = price_history_body("SPY")


# A second symbol, used wherever the forwarding is asserted. With only one symbol in the
# file, a method that ignored its argument and hardcoded "SPY" would pass every test.
OTHER_SYMBOL = "QQQ"

_BOTH_FREQS = {
    ("SPY", MINUTE_FREQ): FakeResponse(200, BARS_BODY),
    ("SPY", DAILY_FREQ): FakeResponse(200, BARS_BODY),
    (OTHER_SYMBOL, MINUTE_FREQ): FakeResponse(200, BARS_BODY),
    (OTHER_SYMBOL, DAILY_FREQ): FakeResponse(200, BARS_BODY),
}


def _client(**kwargs) -> FakeSchwabClient:
    bars = kwargs.pop("bars", dict(_BOTH_FREQS))
    return FakeSchwabClient(bars=bars, **kwargs)


# -- the bound contract ---------------------------------------------------------


@pytest.mark.parametrize("label", ["start", "end"])
def test_a_missing_bound_is_refused_rather_than_passed_as_none(label):
    """``schwab-py`` substitutes a fifty-five year window for a missing bound.

    ``__normalize_start_and_end_datetimes`` defaults a missing start to 1971-01-01 and a
    missing end to seven days from now. So passing ``None`` through, which is what every
    other parameter on this seam does, asks for every candle Schwab holds rather than
    asking for nothing.
    """
    with pytest.raises(ValueError, match=f"{label} is required"):
        require_utc_bound(None, label)


@pytest.mark.parametrize("label", ["start", "end"])
def test_a_naive_bound_is_refused_rather_than_converted_in_the_host_zone(label):
    """``_format_date_as_millis`` calls ``dt.timestamp()``, which reads the local zone.

    A naive bound would therefore mean whatever timezone the capture machine sits in, and
    nothing in the response would say so.
    """
    with pytest.raises(ValueError, match=f"{label} must be timezone-aware"):
        require_utc_bound(datetime(2026, 9, 14, 9, 30), label)


def test_an_aware_bound_comes_back_in_utc():
    assert require_utc_bound(OPEN_ET, "start") == OPEN_UTC
    assert require_utc_bound(OPEN_ET, "start").utcoffset() == timedelta(0)


def test_the_vendor_refuses_a_naive_bound_before_the_request_goes_out():
    client = _client()
    with pytest.raises(ValueError, match="timezone-aware"):
        SchwabVendor(client).get_minute_bars(
            "SPY", start=datetime(2026, 9, 14, 9, 30), end=CLOSE_ET
        )
    # Nothing reached the client, so the refusal cost no request.
    assert client.bar_calls == []


def test_the_vendor_refuses_an_explicit_none_bound():
    client = _client()
    with pytest.raises(ValueError, match="is required"):
        SchwabVendor(client).get_minute_bars("SPY", start=OPEN_ET, end=None)
    assert client.bar_calls == []


def test_both_bounds_are_required_arguments():
    """Omitting a bound is a ``TypeError`` at the call, before any guard runs.

    The runtime refusals above hold for an explicit ``None``. This holds for the caller
    who simply does not pass one, which is the shape ``schwab-py``'s own defaults invite.
    """
    with pytest.raises(TypeError):
        SchwabVendor(_client()).get_minute_bars("SPY", start=OPEN_ET)


# -- the frequency contract -----------------------------------------------------


def test_the_two_frequencies_are_spelled_the_way_the_lake_spells_them():
    """The two this seam has a call for, pinned by their literal spelling.

    This does not claim to be the set of frequencies the lake supports. ``onboard --bars``
    accepts any string and ``TickerConfig.bars`` stores it unvalidated, so a roster may
    carry one nothing here can fetch. What happens to such a frequency belongs to whoever
    builds the surface, not here.
    """
    assert BAR_FREQS == (MINUTE_FREQ, DAILY_FREQ) == ("1m", "1d")


def test_an_unknown_frequency_is_refused_by_name():
    with pytest.raises(ValueError, match="not one of"):
        require_bar_freq("5m")


# -- request shaping ------------------------------------------------------------


def test_minute_bars_reach_the_per_minute_client_method():
    client = _client()
    response = SchwabVendor(client).get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_calls == ["SPY"]
    assert client.bar_freqs == [MINUTE_FREQ]
    assert client.bar_start == [OPEN_UTC]
    assert client.bar_end == [CLOSE_UTC]
    assert response.status == 200
    assert response.body is BARS_BODY


def test_daily_bars_reach_the_per_day_client_method():
    client = _client()
    response = SchwabVendor(client).get_daily_bars("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_calls == ["SPY"]
    assert client.bar_freqs == [DAILY_FREQ]
    assert client.bar_start == [OPEN_UTC]
    assert client.bar_end == [CLOSE_UTC]
    assert response.status == 200
    assert response.body is BARS_BODY


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
def test_the_symbol_reaches_the_client_rather_than_a_hardcoded_one(method):
    client = _client()
    getattr(SchwabVendor(client), method)(OTHER_SYMBOL, start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_calls == [OTHER_SYMBOL]


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
def test_the_bounds_reach_the_client_in_utc_not_merely_at_the_right_instant(method):
    """Two aware datetimes compare equal when they name one instant.

    So asserting ``client.bar_start == OPEN_UTC`` passes for an Eastern-aware value and
    proves nothing about the conversion. The offset is what has to be asserted.
    """
    client = _client()
    getattr(SchwabVendor(client), method)("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_start[0].utcoffset() == timedelta(0)
    assert client.bar_end[0].utcoffset() == timedelta(0)


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
def test_the_window_reaches_the_client_the_way_round_it_was_given(method):
    """Both methods, because a transposed pair does not raise anywhere.

    Schwab answers a backwards window with the empty shape rather than an error, so a
    method that swapped its bounds would record "no candles" for every window a caller
    asked for and nothing would go red. Asserting the frequency alone does not catch it,
    which is how this went missing on the daily side.
    """
    client = _client()
    getattr(SchwabVendor(client), method)("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_start == [OPEN_UTC]
    assert client.bar_end == [CLOSE_UTC]
    assert client.bar_start[0] < client.bar_end[0]


def test_neither_flag_is_sent_unless_asked_for():
    """Left unset, both flags follow the seam's ordinary rule and are omitted.

    ``need_extended_hours_data`` decides whether a per-minute response covers the regular
    session or the whole extended one, and ``need_previous_close`` adds a field outside
    ``candles``. Omitted, Schwab picks.
    """
    client = _client()
    SchwabVendor(client).get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_extended_hours == [None]
    assert client.bar_previous_close == [None]


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
@pytest.mark.parametrize("extended", [True, False])
@pytest.mark.parametrize("previous", [True, False])
def test_both_flags_are_forwarded_when_set(method, extended, previous):
    client = _client()
    getattr(SchwabVendor(client), method)(
        "SPY",
        start=OPEN_ET,
        end=CLOSE_ET,
        extended_hours=extended,
        previous_close=previous,
    )
    assert client.bar_extended_hours == [extended]
    assert client.bar_previous_close == [previous]


def test_the_fake_serves_a_response_keyed_by_the_exact_window():
    """Per-window canned replies, the way the chain fake already keys its narrowing.

    A test driving two windows apart needs distinct bodies for them. The pair key serves
    every window and the full request tuple serves one, and the tuple is tried first.
    """
    first = FakeResponse(200, price_history_body("SPY", CANDLES))
    second = FakeResponse(200, EMPTY_BODY)
    client = FakeSchwabClient(
        bars={
            ("SPY", MINUTE_FREQ, OPEN_UTC, CLOSE_UTC): first,
            ("SPY", MINUTE_FREQ): second,
        }
    )
    vendor = SchwabVendor(client)
    assert vendor.get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET).body["empty"] is False
    later = CLOSE_ET + timedelta(hours=1)
    assert vendor.get_minute_bars("SPY", start=CLOSE_ET, end=later).body["empty"] is True


def test_the_body_comes_back_verbatim():
    vendor = SchwabVendor(_client())
    body = vendor.get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET).body
    assert body is BARS_BODY
    assert body["candles"][0]["close"] == 650.2


# -- credential failures --------------------------------------------------------


class AuthlibBaseError(Exception):
    """Stands in for authlib's base error, matched by name rather than by import."""


class OAuthError(AuthlibBaseError):
    """The one authlib actually raises when a refresh token is dead."""


class _RaisingClient:
    """A client whose price-history calls raise, so the wrapper is what is under test."""

    def get_price_history_every_minute(self, *args, **kwargs):
        raise OAuthError("refresh token expired")

    def get_price_history_every_day(self, *args, **kwargs):
        raise OAuthError("refresh token expired")


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
def test_a_credential_failure_is_named_by_the_lake_not_by_the_library(method):
    """The bars sweep runs in the evening, which is when a Sunday-minted token runs out.

    A refresh that fails sends no request, so there is no status to record and the library
    raises instead. Left alone that lands under whatever the library named its exception,
    which is not what the watchdog watches for.
    """
    vendor = SchwabVendor(_RaisingClient())
    with pytest.raises(VendorAuthError) as caught:
        getattr(vendor, method)("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert isinstance(caught.value.__cause__, OAuthError)


# -- the cassette key -----------------------------------------------------------


def test_the_same_window_named_in_two_zones_keys_one_interaction():
    """One instant has many spellings and a cassette key must have one.

    ``datetime(2026, 9, 14, 13, 30, tzinfo=UTC)`` renders ``2026-09-14T13:30:00+00:00``
    while the identical moment in Eastern renders ``2026-09-14T09:30:00-04:00``. The two
    compare equal as instants and differ as strings, so without normalizing, two callers
    naming one moment would key two interactions and the second lookup would miss a
    cassette holding exactly what it asked for.
    """
    eastern = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    utc = bars_params("SPY", MINUTE_FREQ, start=OPEN_UTC, end=CLOSE_UTC)
    assert eastern == utc
    assert eastern["start"] == "2026-09-14T13:30:00+00:00"


def test_two_bounds_the_request_cannot_tell_apart_key_one_interaction():
    """``schwab-py`` sends ``int(dt.timestamp() * 1000)``, so the wire carries milliseconds.

    A key rendered finer than the request would split one interaction in two: the same
    bound with and without 400 microseconds produces the identical ``startDate`` and would
    still miss the other's recording.
    """
    coarse = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    fine = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET.replace(microsecond=400), end=CLOSE_ET)
    assert int(OPEN_ET.timestamp() * 1000) == int(
        OPEN_ET.replace(microsecond=400).timestamp() * 1000
    )
    assert coarse == fine


def test_a_millisecond_the_request_can_tell_apart_still_keys_its_own_interaction():
    """The truncation goes exactly as far as the wire does and no further."""
    base = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    shifted = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET.replace(microsecond=7000), end=CLOSE_ET)
    assert base != shifted
    assert shifted["start"].endswith(".007000+00:00")


def test_a_flag_is_keyed_exactly_as_given_never_coerced():
    """The request forwards the flag unchanged, so the key must not normalize it.

    ``httpx`` renders ``True`` as ``true`` and ``1`` as ``1``, so the two are different
    requests. Collapsing them in the key would let one recording answer for a request it
    never made, and a string flag would key the opposite of what it sent.
    """
    keyed = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET, extended_hours=1)
    # ``bool(1) == 1`` is True, so equality alone cannot see a coercion. The type has to be
    # compared too, or putting a ``bool()`` back here would pass every assertion.
    assert keyed["extended_hours"] == 1
    assert type(keyed["extended_hours"]) is int
    flagged = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET, extended_hours=True)
    assert flagged["extended_hours"] is True
    assert type(flagged["extended_hours"]) is bool


@pytest.mark.parametrize("bad", [date(2026, 9, 14), "2026-09-14T09:30:00-04:00"])
def test_a_bound_that_is_not_a_datetime_is_refused_by_name(bad):
    """A ``date`` has no ``tzinfo``, and reading one would bury the refusal.

    ``get_chain``'s bounds on this same seam are dates, so handing one to a fixture
    builder here is the plausible mistake. It reads as a named refusal rather than as an
    attribute error on a type nobody mentioned.
    """
    with pytest.raises(ValueError, match="must be a datetime"):
        bars_params("SPY", MINUTE_FREQ, start=bad, end=CLOSE_ET)


def test_a_flag_left_unset_is_omitted_from_the_key():
    bare = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    assert set(bare) == {"symbol", "freq", "start", "end"}


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"extended_hours": True}, {"extended_hours": True}),
        ({"extended_hours": False}, {"extended_hours": False}),
        ({"previous_close": True}, {"previous_close": True}),
        ({"previous_close": False}, {"previous_close": False}),
    ],
)
def test_a_flag_that_is_set_keys_its_own_interaction(kwargs, expected):
    """A request differing only by a flag returns different data, so it keys differently."""
    keyed = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET, **kwargs)
    bare = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    assert keyed != bare
    assert {key: keyed[key] for key in expected} == expected


def test_the_key_refuses_a_bad_bound_the_same_way_the_request_does():
    with pytest.raises(ValueError, match="start must be timezone-aware"):
        bars_params("SPY", MINUTE_FREQ, start=datetime(2026, 9, 14, 9, 30), end=CLOSE_ET)


def test_the_key_names_whichever_bound_was_bad():
    """A refusal that named the wrong bound would send an operator to the good one."""
    with pytest.raises(ValueError, match="end must be timezone-aware"):
        bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=datetime(2026, 9, 14, 16, 0))


def test_the_key_refuses_a_frequency_it_has_no_call_for():
    """``require_bar_freq`` is called here as well as on the request, and both matter.

    A key minted for an unknown frequency records nothing and reaches the replay as a
    missing recording rather than as a bad argument, which is the outcome the guard's own
    docstring names.
    """
    with pytest.raises(ValueError, match="not one of"):
        bars_params("SPY", "5m", start=OPEN_ET, end=CLOSE_ET)


def test_the_key_keeps_the_seconds_the_caller_named():
    """The truncation stops at the millisecond, so a window is not rounded to the minute.

    Every bound in this file otherwise lands on a whole minute, which would let a coarser
    truncation key two genuinely different windows as one.
    """
    keyed = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET.replace(second=37), end=CLOSE_ET)["start"]
    assert keyed == "2026-09-14T13:30:37+00:00"


def test_a_tzinfo_that_reports_no_offset_is_refused():
    """``tzinfo is None`` is only half the check, and the other half had nothing behind it.

    A tzinfo object whose ``utcoffset`` returns ``None`` is aware by the first test and
    unusable by every later one, including ``astimezone``.
    """

    class _NoOffset(tzinfo):
        def utcoffset(self, dt):
            return None

        def dst(self, dt):
            return None

        def tzname(self, dt):
            return "nowhere"

    with pytest.raises(ValueError, match="timezone-aware"):
        require_utc_bound(datetime(2026, 9, 14, 9, 30, tzinfo=_NoOffset()), "start")


# -- replay ---------------------------------------------------------------------


def _cassette() -> Cassette:
    return Cassette(
        interactions=bars_interactions(
            "SPY",
            MINUTE_FREQ,
            [(OPEN_ET, CLOSE_ET, CANDLES), (CLOSE_ET, CLOSE_ET + timedelta(hours=1), [])],
        )
    )


def test_the_replay_finds_a_recorded_window_by_its_key():
    replayed = CassetteVendor(_cassette()).get_minute_bars("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert replayed.status == 200
    assert replayed.body == BARS_BODY
    assert replayed.body["candles"][0]["datetime"] == int(OPEN_UTC.timestamp() * 1000)


def test_the_replay_finds_the_same_window_named_in_utc():
    """The fixture keyed in Eastern and the lookup asking in UTC must be one interaction.

    This is what the normalization above buys, asserted end to end rather than on the key
    helper alone.
    """
    replayed = CassetteVendor(_cassette()).get_minute_bars("SPY", start=OPEN_UTC, end=CLOSE_UTC)
    assert replayed.body == BARS_BODY


def test_the_replay_serves_the_empty_window_shape():
    """A session the vendor has no candle for is a real answer, not an error.

    #280 has to decide what its fetch does with one, and nothing but a body shaped this
    way can drive that test, so the fixture set owes both shapes.
    """
    replayed = CassetteVendor(_cassette()).get_minute_bars(
        "SPY", start=CLOSE_ET, end=CLOSE_ET + timedelta(hours=1)
    )
    assert replayed.body == EMPTY_BODY
    assert replayed.body["candles"] == []
    assert replayed.body["empty"] is True


def test_the_replay_raises_on_a_window_the_cassette_does_not_hold():
    with pytest.raises(CassetteError, match=BARS_ENDPOINT):
        CassetteVendor(_cassette()).get_minute_bars(
            "SPY", start=OPEN_ET, end=CLOSE_ET + timedelta(days=1)
        )


def test_the_replay_raises_when_only_the_frequency_differs():
    """A daily window is a different vendor call, so it must not serve a minute recording."""
    with pytest.raises(CassetteError):
        CassetteVendor(_cassette()).get_daily_bars("SPY", start=OPEN_ET, end=CLOSE_ET)


def test_the_fixture_builder_and_the_lookup_agree_on_every_window():
    """The one property that makes a fixture usable: it cannot key a window the replay misses.

    ``bars_interactions`` and ``CassetteVendor`` both call ``bars_params``, so this holds
    by construction. The test is what goes red if either ever spells the key itself.
    """
    windows = [
        (OPEN_ET, CLOSE_ET, CANDLES),
        (OPEN_UTC, CLOSE_UTC, []),
        (CLOSE_ET, CLOSE_ET + timedelta(hours=1), CANDLES),
    ]
    cassette = Cassette(interactions=bars_interactions("SPY", MINUTE_FREQ, windows))
    vendor = CassetteVendor(cassette)
    for start, end, _candles in windows:
        assert vendor.get_minute_bars("SPY", start=start, end=end).status == 200


def test_the_cassette_vendor_still_satisfies_the_widened_protocol():
    assert isinstance(CassetteVendor(_cassette()), Vendor)


def test_the_schwab_vendor_still_satisfies_the_widened_protocol():
    assert isinstance(SchwabVendor(_client()), Vendor)


@pytest.mark.parametrize(
    ("freq", "method"), [(MINUTE_FREQ, "get_minute_bars"), (DAILY_FREQ, "get_daily_bars")]
)
def test_the_replay_serves_each_frequency_from_its_own_recording(freq, method):
    """The daily replay had no positive test at all, only one asserting it raises."""
    cassette = Cassette(interactions=bars_interactions("SPY", freq, [(OPEN_ET, CLOSE_ET, CANDLES)]))
    response = getattr(CassetteVendor(cassette), method)("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert response.body == BARS_BODY


@pytest.mark.parametrize("method", ["get_minute_bars", "get_daily_bars"])
def test_the_replay_carries_both_flags_into_its_lookup(method):
    """A flagged recording must answer only a request that asked for the same flags.

    Without this, a replay that dropped the flags from its lookup would serve an
    extended-hours recording to a caller who asked for the regular session, and the
    fixture builder's own key would be the thing proving it correct.
    """
    freq = MINUTE_FREQ if method == "get_minute_bars" else DAILY_FREQ
    cassette = Cassette(
        interactions=bars_interactions(
            "SPY", freq, [(OPEN_ET, CLOSE_ET, CANDLES)], extended_hours=True
        )
    )
    vendor = CassetteVendor(cassette)
    served = getattr(vendor, method)("SPY", start=OPEN_ET, end=CLOSE_ET, extended_hours=True)
    assert served.body == BARS_BODY
    # The same window without the flag is a different request and must not be served.
    with pytest.raises(CassetteError):
        getattr(vendor, method)("SPY", start=OPEN_ET, end=CLOSE_ET)


def test_the_cassette_endpoint_name_is_the_lake_surface_name():
    """One string names the vendor interaction and the partition tree it lands in.

    ``bars_partition_path`` builds under ``paths.BARS``. A recording keyed under a second
    spelling would replay for nothing the surface ever asks for.
    """
    from lake.paths import BARS

    assert BARS_ENDPOINT == BARS
