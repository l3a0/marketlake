"""The fake vendor.

``CassetteVendor`` implements the ``Vendor`` seam by replaying a cassette. It never
touches the network. A request with no recorded interaction raises, so a test can
never silently reach past its recording.

The request keys mirror the real calls. ``get_chain`` keys on ``{"symbol": symbol}``
plus whichever narrowing parameters are set: ``from_date`` and ``to_date`` as ISO date
strings and ``strike_count`` as an int. So the bare chain still keys on
``{"symbol": symbol}``, and each date window the capture chunker fetches keys on
``{"symbol": symbol, "from_date": ...}`` plus ``"to_date"`` when the window is closed. The
open tail leaves ``to_date`` ``None``, so it keys on ``{"symbol": symbol, "from_date":
...}`` alone. A parameter left ``None`` is omitted from the key, exactly as the real vendor
omits it from the request, so a window keyed on ``from_date`` and ``to_date`` matches a
closed window and one keyed on ``from_date`` alone matches the open tail. ``strike_count``
is only the by-hand probe's parameter now, never the hot path's. ``get_quotes`` keys on
``{"symbols": [...]}`` in the order given. ``get_minute_bars`` and ``get_daily_bars`` key on
``{"symbol": ..., "freq": ..., "start": ..., "end": ...}`` plus whichever of
``extended_hours`` and ``previous_close`` is set, with both bounds rendered as UTC ISO
strings. A cassette must record the same shapes.

``chain_params`` builds that chain key, and the replay above calls it, so a fixture and
the lookup it has to match cannot drift apart. ``lake.vendor.bars_params`` does the same
job for price history, and it lives in production rather than here because the recorder
writes by it too. ``windowed_chain_interactions`` builds a whole windowed recording from
one chain body, which is what a test needs once the code under test fetches by a plan
rather than by the bare symbol. ``bars_interactions`` is the price-history counterpart: a
live recording captures one window, and it builds whatever set of windows a test wants
from candles the test names.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime

from lake.cassette import Cassette, Interaction
from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan
from lake.vendor import (
    BARS_ENDPOINT,
    DAILY_FREQ,
    MINUTE_FREQ,
    VendorError,
    VendorResponse,
    bars_params,
    require_utc_bound,
)

# The two expiration maps a chain body carries. Everything else in the body is a
# chain-level header field the vendor repeats on every window's response.
_EXP_MAPS = ("callExpDateMap", "putExpDateMap")


def _iso_date(value: date | str) -> str:
    """A ``date`` as its ISO string, passing an already-string value through unchanged."""
    return value.isoformat() if isinstance(value, date) else value


def chain_params(
    symbol: str,
    *,
    from_date: date | str | None = None,
    to_date: date | str | None = None,
    strike_count: int | None = None,
) -> dict:
    """The cassette key for one chain request.

    A parameter left ``None`` is omitted, exactly as the real vendor omits it from the
    request. This is the one spelling of the key: ``CassetteVendor.get_chain`` looks a
    request up by it and ``windowed_chain_interactions`` records by it, so a fixture
    cannot key a window the lookup would then miss.
    """
    params: dict = {"symbol": symbol}
    if from_date is not None:
        params["from_date"] = _iso_date(from_date)
    if to_date is not None:
        params["to_date"] = _iso_date(to_date)
    if strike_count is not None:
        params["strike_count"] = strike_count
    return params


def _expiration_date(exp_key: str) -> str:
    """The ISO date in a chain body's expiration key, which reads ``date:days-to-expiry``."""
    return exp_key.split(":")[0]


def _contract_expiration(contract: object) -> str | None:
    """The ISO date part of a contract's own ``expirationDate``, or ``None`` if it has none.

    This is the field ``journal.chains_data_batch`` reads to stamp a row's window, so it
    is the field that decides where a contract actually lands.
    """
    if not isinstance(contract, Mapping):
        return None
    value = contract.get("expirationDate")
    return None if value is None else str(value).split("T")[0]


def _check_contract_dates(map_key: str, exp_key: str, contracts: object) -> None:
    """Refuse a contract whose own expiration does not match the map key it sits under.

    The recording places a contract by its map key, because that is what groups a chain
    body. Production stamps a row's window from the contract's own ``expirationDate``
    instead. A real vendor body agrees on both, and a fixture is free not to, which would
    put the recording's window and the journalled window on different dates with nothing
    to say so. A missing field is refused for the same reason: it leaves the row's window
    columns null whatever window fetched it, so a test asserting the fetch provenance
    would be asserting nothing.
    """
    if not isinstance(contracts, Sequence) or isinstance(contracts, (str, bytes)):
        return
    for contract in contracts:
        found = _contract_expiration(contract)
        if found is None:
            raise ValueError(
                f"{map_key}[{exp_key}] holds a contract with no expirationDate, so its "
                "journalled window columns would be null whatever window fetched it"
            )
        if found != _expiration_date(exp_key):
            raise ValueError(
                f"{map_key}[{exp_key}] holds a contract expiring {found}, so the window "
                "this records it under is not the window it would be journalled under"
            )


def windowed_chain_interactions(
    ticker: str,
    day: date,
    body: Mapping[str, object],
    *,
    plan: ChainPlan = DEFAULT_CHAIN_PLAN,
) -> tuple[Interaction, ...]:
    """Record ``body`` as the windowed fetch of ``plan`` on ``day`` would have returned it.

    Every caller on the hot path fetches a chain by its date-window plan, so a cassette
    holding the bare symbol alone matches nothing. Each window gets its own interaction,
    keyed the way the fetcher asks for it, carrying the chain-level header fields and
    exactly the expirations whose date falls inside that window's range. Reassembly
    merges them back into ``body``, so a test asserting a contract count states the
    count its own body describes, whatever the plan tiles.

    A short recording is quiet rather than loud. ``_fetch_window`` catches every raised
    exception and files it as a failed window, so the ``CassetteError`` a forgotten window
    raises reads as "that window failed" rather than "the fixture is short a window."
    That is what this builds the whole set for. Two more silent shapes raise here rather
    than reaching a test.

    1. An expiration that falls in no window. Dropping it would leave a recording whose
       contract count is lower than its own body's, with nothing to say so.
    2. A contract whose own ``expirationDate`` is missing, or names a different date than
       the map key it sits under. The recording places a contract by that key, while
       production stamps the row's window from the contract's field, so the two disagreeing
       puts the recorded window and the journalled window on different dates.
    """
    windows = plan.windows_for(day)
    header = {key: value for key, value in body.items() if key not in _EXP_MAPS}
    placed: set[tuple[str, str]] = set()
    interactions: list[Interaction] = []
    for from_date, to_date in windows:
        start = from_date.isoformat()
        end = None if to_date is None else to_date.isoformat()
        window_body: dict = dict(header)
        for map_key in _EXP_MAPS:
            exp_map = body.get(map_key) or {}
            for exp, strikes in exp_map.items():
                for strike_contracts in strikes.values():
                    _check_contract_dates(map_key, exp, strike_contracts)
            inside = {
                exp: strikes
                for exp, strikes in exp_map.items()
                if start <= _expiration_date(exp) and (end is None or _expiration_date(exp) <= end)
            }
            placed.update((map_key, exp) for exp in inside)
            window_body[map_key] = inside
        interactions.append(
            Interaction(
                endpoint="chains",
                params=chain_params(ticker, from_date=from_date, to_date=to_date),
                status=200,
                body=window_body,
            )
        )
    missed = [
        f"{map_key}[{exp}]"
        for map_key in _EXP_MAPS
        for exp in (body.get(map_key) or {})
        if (map_key, exp) not in placed
    ]
    if missed:
        raise ValueError(
            f"{', '.join(missed)} falls in no window of the plan on {day.isoformat()}, so "
            "the recording would hold fewer contracts than the body it was built from"
        )
    return tuple(interactions)


def windowed_chain_cassette(
    ticker: str,
    day: date,
    body: Mapping[str, object],
    *,
    plan: ChainPlan = DEFAULT_CHAIN_PLAN,
    extra: Sequence[Interaction] = (),
) -> Cassette:
    """A cassette holding one windowed chain recording, plus whatever ``extra`` adds.

    ``extra`` comes first, so a test can record a different response for one window by
    naming it there rather than rebuilding the whole set. ``Cassette.find`` returns the
    first match.
    """
    return Cassette(
        interactions=tuple(extra) + windowed_chain_interactions(ticker, day, body, plan=plan)
    )


def bars_candle(
    when: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int = 0,
) -> dict:
    """One candle, shaped the way Schwab shapes one.

    ``datetime`` is Schwab's epoch-millisecond stamp rather than an ISO string, which is
    the whole reason a fixture builder exists for this: a test naming an instant should
    not have to render that stamp by hand and get the unit wrong. ``when`` must be
    timezone-aware, so the stamp does not depend on the host's zone.

    The parameter is ``open_`` because ``open`` is a builtin. The rendered key is ``open``.
    """
    return {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "datetime": int(require_utc_bound(when, "when").timestamp() * 1000),
    }


def price_history_body(symbol: str, candles: Sequence[Mapping[str, object]] = ()) -> dict:
    """A price-history body, shaped the way Schwab shapes one.

    Schwab answers a window it has no candles for with an empty list and ``empty`` true,
    never with an error. So the two payload shapes a test needs are one call apart: pass
    candles for a populated window and pass none for an empty one.
    """
    listed = list(candles)
    return {"candles": listed, "symbol": symbol, "empty": not listed}


def bars_interactions(
    symbol: str,
    freq: str,
    windows: Sequence[tuple[datetime, datetime, Sequence[Mapping[str, object]]]],
    *,
    extended_hours: bool | None = None,
    previous_close: bool | None = None,
    status: int = 200,
) -> tuple[Interaction, ...]:
    """Record one price-history interaction per window.

    Each entry in ``windows`` is a start, an end, and the candles that window returns. A
    window with no candles records the empty shape, which is what a test of the no-candle
    path needs and what no live recording is worth burning a request on.

    Every interaction is keyed through ``lake.vendor.bars_params``, the same function the
    replay below looks a request up by, so a fixture cannot key a window the lookup would
    then miss.
    """
    return tuple(
        Interaction(
            endpoint=BARS_ENDPOINT,
            params=bars_params(
                symbol,
                freq,
                start=start,
                end=end,
                extended_hours=extended_hours,
                previous_close=previous_close,
            ),
            status=status,
            body=price_history_body(symbol, candles),
        )
        for start, end, candles in windows
    )


class CassetteVendor:
    """A ``Vendor`` fed by a recorded cassette."""

    def __init__(self, cassette: Cassette) -> None:
        self._cassette = cassette

    def get_chain(
        self,
        symbol: str,
        *,
        from_date: date | None = None,
        to_date: date | None = None,
        strike_count: int | None = None,
    ) -> VendorResponse:
        interaction = self._cassette.find(
            "chains",
            chain_params(symbol, from_date=from_date, to_date=to_date, strike_count=strike_count),
        )
        return VendorResponse(
            status=interaction.status,
            body=interaction.body,
            headers=interaction.headers,
        )

    def get_quotes(self, symbols: Sequence[str]) -> VendorResponse:
        interaction = self._cassette.find("quotes", {"symbols": list(symbols)})
        return VendorResponse(
            status=interaction.status,
            body=interaction.body,
            headers=interaction.headers,
        )

    def _bars(
        self,
        symbol: str,
        freq: str,
        start: datetime,
        end: datetime,
        extended_hours: bool | None,
        previous_close: bool | None,
    ) -> VendorResponse:
        """Replay one price-history request, whichever frequency asked for it."""
        interaction = self._cassette.find(
            BARS_ENDPOINT,
            bars_params(
                symbol,
                freq,
                start=start,
                end=end,
                extended_hours=extended_hours,
                previous_close=previous_close,
            ),
        )
        return VendorResponse(
            status=interaction.status,
            body=interaction.body,
            headers=interaction.headers,
        )

    def get_minute_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        extended_hours: bool | None = None,
        previous_close: bool | None = None,
    ) -> VendorResponse:
        return self._bars(symbol, MINUTE_FREQ, start, end, extended_hours, previous_close)

    def get_daily_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        extended_hours: bool | None = None,
        previous_close: bool | None = None,
    ) -> VendorResponse:
        return self._bars(symbol, DAILY_FREQ, start, end, extended_hours, previous_close)

    def token_mint_time(self) -> datetime:
        if self._cassette.token_mint_time is None:
            raise VendorError("cassette has no token_mint_time")
        return datetime.fromisoformat(self._cassette.token_mint_time)
