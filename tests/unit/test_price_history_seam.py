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

from datetime import UTC, datetime, timedelta, timezone

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


def _client(**kwargs) -> FakeSchwabClient:
    bars = kwargs.pop("bars", {("SPY", MINUTE_FREQ): FakeResponse(200, BARS_BODY)})
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
    with pytest.raises(ValueError, match="is required"):
        require_utc_bound(None, label)


@pytest.mark.parametrize("label", ["start", "end"])
def test_a_naive_bound_is_refused_rather_than_converted_in_the_host_zone(label):
    """``_format_date_as_millis`` calls ``dt.timestamp()``, which reads the local zone.

    A naive bound would therefore mean whatever timezone the capture machine sits in, and
    nothing in the response would say so.
    """
    with pytest.raises(ValueError, match="timezone-aware"):
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


def test_the_two_frequencies_are_the_ones_the_lake_already_spells():
    """These strings key a cassette, a roster entry, and a ``freq=`` partition level.

    ``onboard.DEFAULT_BARS`` and ``LakePaths.bars_partition_path`` use the same two, so a
    rename here that missed them would key a request one way and a partition another.
    """
    from lake.onboard import DEFAULT_BARS

    assert BAR_FREQS == (MINUTE_FREQ, DAILY_FREQ) == ("1m", "1d")
    assert set(BAR_FREQS) == set(DEFAULT_BARS)


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
    client = _client(bars={("SPY", DAILY_FREQ): FakeResponse(200, BARS_BODY)})
    SchwabVendor(client).get_daily_bars("SPY", start=OPEN_ET, end=CLOSE_ET)
    assert client.bar_freqs == [DAILY_FREQ]


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


@pytest.mark.parametrize("extended", [True, False])
@pytest.mark.parametrize("previous", [True, False])
def test_both_flags_are_forwarded_when_set(extended, previous):
    client = _client()
    SchwabVendor(client).get_minute_bars(
        "SPY",
        start=OPEN_ET,
        end=CLOSE_ET,
        extended_hours=extended,
        previous_close=previous,
    )
    assert client.bar_extended_hours == [extended]
    assert client.bar_previous_close == [previous]


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


def test_a_flag_left_unset_is_omitted_from_the_key():
    bare = bars_params("SPY", MINUTE_FREQ, start=OPEN_ET, end=CLOSE_ET)
    assert set(bare) == {"symbol", "freq", "start", "end"}


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"extended_hours": True}, {"extended_hours": True}),
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
    with pytest.raises(ValueError, match="timezone-aware"):
        bars_params("SPY", MINUTE_FREQ, start=datetime(2026, 9, 14, 9, 30), end=CLOSE_ET)


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


def test_the_cassette_endpoint_name_is_the_lake_surface_name():
    """One string names the vendor interaction and the partition tree it lands in.

    ``bars_partition_path`` builds under ``paths.BARS``. A recording keyed under a second
    spelling would replay for nothing the surface ever asks for.
    """
    from lake.paths import BARS

    assert BARS_ENDPOINT == BARS
