"""The fake vendor.

``CassetteVendor`` implements the ``Vendor`` seam by replaying a cassette. It never
touches the network. A request with no recorded interaction raises, so a test can
never silently reach past its recording.

The request keys mirror the real calls. ``get_chain`` keys on ``{"symbol": symbol}``
plus whichever narrowing parameters are set: ``from_date`` and ``to_date`` as ISO date
strings and ``strike_count`` as an int. So the bare chain still keys on
``{"symbol": symbol}``, the discovery probe on ``{"symbol": symbol, "strike_count": 1}``,
and each chunk on ``{"symbol": symbol, "from_date": ..., "to_date": ...}``. A parameter
left ``None`` is omitted from the key, exactly as the real vendor omits it from the
request. ``get_quotes`` keys on ``{"symbols": [...]}`` in the order given. A cassette
must record the same shapes.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

from lake.cassette import Cassette
from lake.vendor import VendorError, VendorResponse


def _iso_date(value: date | str) -> str:
    """A ``date`` as its ISO string, passing an already-string value through unchanged."""
    return value.isoformat() if isinstance(value, date) else value


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
        params: dict = {"symbol": symbol}
        if from_date is not None:
            params["from_date"] = _iso_date(from_date)
        if to_date is not None:
            params["to_date"] = _iso_date(to_date)
        if strike_count is not None:
            params["strike_count"] = strike_count
        interaction = self._cassette.find("chains", params)
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

    def token_mint_time(self) -> datetime:
        if self._cassette.token_mint_time is None:
            raise VendorError("cassette has no token_mint_time")
        return datetime.fromisoformat(self._cassette.token_mint_time)
