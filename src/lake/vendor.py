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

# The two frequencies this seam has a vendor call for, spelled the way the roster and the
# lake layout already spell them. This is not the set of frequencies the lake supports.
# ``onboard --bars`` accepts any string and ``TickerConfig.bars`` stores it unvalidated, so
# a roster is free to carry a frequency nothing here can fetch. What happens to one is the
# surface's question rather than the seam's.
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

    ``status`` is the HTTP status code. ``headers`` are the response headers. ``body`` is
    the parsed JSON object exactly as the vendor sent it, with one exception. A failed
    reply, one whose status is not 2xx, can carry a body that is not a JSON object at
    all: an HTML error page from a gateway, an empty body, or JSON that parses to a list,
    ``null`` or a bare string. Its status is still the signal, so the reply is still
    returned. ``body`` is then an empty mapping and ``body_text`` carries the reply's
    text verbatim, so nothing the vendor said is dropped.

    ``body_text`` is set exactly when ``body`` is that empty-mapping stand-in, which is
    what lets a reader tell a vendor that sent ``{}`` from one that sent something that
    is not an object. A 2xx reply never takes this path. Its body is the payload, and one
    that is not an object is refused where the reply is shaped, in ``lake.schwab``.

    Timestamps that belong to the capture cycle, like the fetch time, are the caller's to
    stamp from the injected clock. They are not here.
    """

    status: int
    body: Mapping[str, object]
    headers: Mapping[str, str] = field(default_factory=dict)
    body_text: str | None = None


def require_utc_bound(when: datetime | None, label: str) -> datetime:
    """Refuse a missing or naive price-history bound, and normalize an aware one to UTC.

    Every other parameter on this seam follows one rule: a ``None`` is omitted from the
    vendor request. A price-history window cannot follow it, because ``schwab-py``
    substitutes rather than omits. Its ``__normalize_start_and_end_datetimes`` defaults a
    missing start to 1971-01-01 and a missing end to a naive ``utcnow()`` plus seven days,
    so a caller passing ``None`` asks for a fifty-five year window instead of asking for
    nothing. Both bounds are therefore required here.

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
    if not isinstance(when, datetime):
        # A ``date`` has no ``tzinfo``, so reading one would raise ``AttributeError`` and
        # bury the refusal this function exists to give. ``get_chain``'s bounds on this
        # same seam are dates, which is exactly the mistake worth naming.
        raise ValueError(f"{label} must be a datetime, not {type(when).__name__}")
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


def _key_instant(when: datetime | None, label: str) -> str:
    """One bound, rendered for the cassette key at the resolution the wire carries.

    ``schwab-py`` sends a bound as ``int(dt.timestamp() * 1000)``, so the request cannot
    tell two instants apart below a millisecond. Rendering the key any finer would key two
    interactions for one request: a bound carrying 400 microseconds and the same bound
    without them produce the identical ``startDate`` and would still miss each other's
    recording. Truncating here keeps the key exactly as discriminating as the request.
    """
    stamped = require_utc_bound(when, label)
    return stamped.replace(microsecond=stamped.microsecond // 1000 * 1000).isoformat()


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

    Both bounds run through ``_key_instant``, so they are refused when missing or naive,
    rendered in UTC, and truncated to the millisecond the vendor request carries.
    Without that normalization
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
        "start": _key_instant(start, "start"),
        "end": _key_instant(end, "end"),
    }
    # Each flag is keyed exactly as given, never coerced. The vendor methods forward it
    # unchanged, and ``True`` and ``1`` reach Schwab as different query values, so
    # collapsing them here would let a recording's key assert something its own request
    # did not ask for.
    if extended_hours is not None:
        params["extended_hours"] = extended_hours
    if previous_close is not None:
        params["previous_close"] = previous_close
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
