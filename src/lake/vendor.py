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
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol, runtime_checkable


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
        each expiration range. Onboarding and the recorder still call this with the bare
        symbol for the whole chain.
        """
        ...

    def get_quotes(self, symbols: Sequence[str]) -> VendorResponse:
        """Batched equity quotes for every symbol, in one request."""
        ...

    def token_mint_time(self) -> datetime:
        """When the refresh token in use was minted, timezone-aware.

        This is read off the token the client actually runs on, never from a
        separate file read. The daemon stamps it into journal metadata each cycle so
        the dashboard shows the token capture is really using.
        """
        ...
