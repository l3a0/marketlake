"""The vendor interface.

The capture primitive talks to Schwab through this interface, never to the network
directly. So a test swaps in a fake vendor fed by recorded cassettes and never
touches the network. That is the injected-vendor seam.

A cassette is a saved vendor response replayed offline. Its format lives in
``lake.cassette``. The real Schwab-backed implementation of this interface lands in
a later deliverable (D5). The cassette-backed fake lives under ``tests/support``.

The interface is deliberately narrow. It returns the vendor's payload verbatim.
Nothing here parses, validates, or reshapes it. Raw stays vendor-verbatim, always.
The fetch time is stamped by the caller from the injected clock, never by the
vendor, so it is not part of a response.

One rule has an exception, and it is about an argument rather than a payload. Every
optional parameter here is omitted from the request when left ``None``, which is the
pass-through this file promises. A price-history window cannot work that way, because
``schwab-py`` substitutes a default for a missing bound instead of omitting it, and
reads a naive one in the host's local timezone. Both bounds are therefore checked
before the request goes out, by ``require_utc_bound`` below, which carries the whole
reason. Nothing about a response is checked anywhere in this file.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Protocol, runtime_checkable

# The two bar frequencies the lake captures, spelled the way the roster and the lake
# layout already spell them. ``onboard.DEFAULT_BARS`` is ``("1m", "1d")`` and
# ``LakePaths.bars_partition_path`` builds a ``freq=`` level from the same string, so a
# request, a cassette key, and a partition all name a frequency identically.
MINUTE_FREQ = "1m"
DAILY_FREQ = "1d"
BAR_FREQS = (MINUTE_FREQ, DAILY_FREQ)

# The cassette ``endpoint`` name for a price-history interaction, matching ``paths.BARS``.
BARS_ENDPOINT = "bars"


class VendorError(Exception):
    """Base class for every vendor-side failure."""


@dataclass(frozen=True)
class VendorResponse:
    """One vendor reply, verbatim.

    ``body`` is the parsed JSON payload exactly as the vendor sent it. ``status`` is
    the HTTP status code. ``headers`` are the response headers. Timestamps that
    belong to the capture cycle, like the fetch time, are the caller's to stamp from
    the injected clock. They are not here.
    """

    status: int
    body: Mapping[str, object]
    headers: Mapping[str, str] = field(default_factory=dict)


def require_utc_bound(when: datetime | None, label: str) -> datetime:
    """Refuse a missing or naive price-history bound, and normalize an aware one to UTC.

    Every other parameter on this seam follows one rule: a ``None`` is omitted from the
    vendor request. A price-history window cannot follow it, because ``schwab-py``
    substitutes rather than omits. Its ``__normalize_start_and_end_datetimes`` defaults a
    missing start to 1971-01-01 and a missing end to seven days from now, so a caller
    passing ``None`` asks for a fifty-five year window instead of asking for nothing. Both
    bounds are therefore required here.

    A naive bound is refused for a second reason. ``schwab-py`` converts a bound with
    ``dt.timestamp() * 1000``, which reads a naive datetime in the host's local zone, so
    the request would mean whatever timezone the capture machine happens to sit in.
    ``SessionClock.bounds`` returns Eastern-aware instants, which pass straight through.

    The returned value is in UTC, so one instant has one spelling. That matters for the
    cassette key below, where the same moment named in Eastern and in UTC must key one
    interaction rather than two.
    """
    if when is None:
        raise ValueError(f"{label} is required, because the vendor substitutes a window for None")
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return when.astimezone(UTC)


def require_bar_freq(freq: str) -> str:
    """Refuse a frequency this seam has no vendor call for.

    Schwab has no frequency-parameterized price-history call. It has one method per
    frequency, so the two this lake captures are the two spelled in ``BAR_FREQS``. An
    unknown string would otherwise key a cassette interaction nothing records and reach
    the replay as a missing recording rather than as a bad argument.
    """
    if freq not in BAR_FREQS:
        raise ValueError(f"freq {freq!r} is not one of {list(BAR_FREQS)}")
    return freq


def bars_params(
    symbol: str,
    freq: str,
    *,
    start: datetime | None,
    end: datetime | None,
    extended_hours: bool | None = None,
    previous_close: bool | None = None,
) -> dict:
    """The cassette key for one price-history request.

    This is the one spelling of that key. The replay in ``tests.support.vendor`` looks a
    request up by it and the recorder in ``lake.record`` writes by it, so a recording
    cannot key a request the lookup would then miss.

    Both bounds run through ``require_utc_bound``, so they are refused when missing or
    naive and rendered in UTC when present. Without that normalization
    ``datetime(2026, 9, 14, 13, 30, tzinfo=UTC)`` renders ``2026-09-14T13:30:00+00:00``
    while the identical moment in Eastern renders ``2026-09-14T09:30:00-04:00``. The two
    compare equal as instants and differ as strings, so two callers naming one moment
    would key two interactions.

    A flag left ``None`` is omitted from the key, exactly as the real vendor omits it from
    the request, so a recording taken with neither flag set replays for a fetch that sets
    neither.
    """
    params: dict = {
        "symbol": symbol,
        "freq": require_bar_freq(freq),
        "start": require_utc_bound(start, "start").isoformat(),
        "end": require_utc_bound(end, "end").isoformat(),
    }
    if extended_hours is not None:
        params["extended_hours"] = bool(extended_hours)
    if previous_close is not None:
        params["previous_close"] = bool(previous_close)
    return params


@runtime_checkable
class Vendor(Protocol):
    """What the capture primitive needs from Schwab."""

    def get_chain(
        self,
        symbol: str,
        *,
        from_date: date | None = None,
        to_date: date | None = None,
        strike_count: int | None = None,
    ) -> VendorResponse:
        """One option-chain request for an underlying, verbatim.

        With no optional argument this is the full chain in one request, as before. The
        three optional parameters narrow the request so the capture chunker can fetch a
        chain too big for one response. ``from_date`` and ``to_date`` bound the returned
        expirations, and ``strike_count`` caps the strikes per expiration. A ``None``
        parameter is omitted from the vendor request. The chunker uses ``strike_count=1``
        to discover the expiration list cheaply, then ``from_date`` / ``to_date`` to fetch
        each expiration range. Onboarding fetches by those same windows, through the
        chunker itself. The by-hand recorder is the one caller left calling this with the
        bare symbol for the whole chain.
        """
        ...

    def get_quotes(self, symbols: Sequence[str]) -> VendorResponse:
        """Batched equity quotes for every symbol, in one request."""
        ...

    def get_minute_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        extended_hours: bool | None = None,
        previous_close: bool | None = None,
    ) -> VendorResponse:
        """One per-minute price-history request for a symbol, verbatim.

        ``start`` and ``end`` bound the window. Both are required and both must be
        timezone-aware, which is the one place this seam departs from the ``None`` rule
        above, and ``require_utc_bound`` carries the reason in full.

        ``extended_hours`` decides whether the response covers the regular session or the
        whole extended one. ``previous_close`` adds a field outside ``candles``. Both
        follow the seam's ordinary rule: left ``None`` they are omitted from the request
        and Schwab picks.

        A candle carries ``open``, ``high``, ``low``, ``close``, ``volume`` and a
        ``datetime`` that is Schwab's epoch-millisecond stamp. Nothing here reads it. The
        body comes back exactly as the vendor sent it.
        """
        ...

    def get_daily_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        extended_hours: bool | None = None,
        previous_close: bool | None = None,
    ) -> VendorResponse:
        """One per-day price-history request for a symbol, verbatim.

        Schwab has no frequency-parameterized price-history call, so the daily window is a
        second method rather than an argument. Keeping them apart keeps this seam a
        pass-through: each method forwards its arguments to one client method and chooses
        nothing, the way ``get_chain`` and ``get_quotes`` already do. The bound rules are
        ``get_minute_bars``'s, unchanged.
        """
        ...

    def token_mint_time(self) -> datetime:
        """When the refresh token in use was minted, timezone-aware.

        This is read off the token the client actually runs on, never from a
        separate file read. The daemon stamps it into journal metadata each cycle so
        the dashboard shows the token capture is really using.
        """
        ...
