"""The fake vendor.

``CassetteVendor`` implements the ``Vendor`` seam by replaying a cassette. It never
touches the network. A request with no recorded interaction raises, so a test can
never silently reach past its recording.

The request keys mirror the real calls. ``get_chain`` keys on ``{"symbol": symbol}``.
``get_quotes`` keys on ``{"symbols": [...]}`` in the order given. A cassette must
record the same shapes.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from lake.cassette import Cassette
from lake.vendor import VendorError, VendorResponse


class CassetteVendor:
    """A ``Vendor`` fed by a recorded cassette."""

    def __init__(self, cassette: Cassette) -> None:
        self._cassette = cassette

    def get_chain(self, symbol: str) -> VendorResponse:
        interaction = self._cassette.find("chains", {"symbol": symbol})
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
