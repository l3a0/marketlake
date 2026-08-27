"""The fake Schwab client.

``FakeSchwabClient`` mimics the small slice of a ``schwab-py`` client that
``SchwabVendor`` calls. It never touches the network. A test builds one with canned
replies, injects it into ``SchwabVendor``, and asserts the vendor shapes requests and
responses correctly. This is the client-injection seam for the real vendor, the same
idea as the cassette-backed ``CassetteVendor`` for the vendor interface itself.

The fake also records what it was asked, so a test can assert the vendor called the
right endpoint with the right symbols.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FakeResponse:
    """A stand-in for the ``httpx.Response`` ``schwab-py`` returns.

    It carries exactly the three members ``SchwabVendor`` reads: a status code, a
    parsed JSON body, and headers.
    """

    status_code: int
    body: Mapping[str, object]
    headers: Mapping[str, str] = field(default_factory=dict)

    def json(self) -> Mapping[str, object]:
        return self.body


@dataclass
class _FakeTokenMetadata:
    """The ``token_metadata`` handle, carrying only the mint epoch second."""

    creation_timestamp: float | None


class FakeSchwabClient:
    """A ``schwab-py`` client stand-in with canned replies.

    ``chains`` maps a symbol to its ``FakeResponse``. ``quotes`` maps a tuple of
    symbols to its ``FakeResponse``. ``creation_timestamp`` is the token mint epoch
    second the vendor converts in ``token_mint_time``. Pass ``None`` to model a
    client whose token metadata carries no mint time.
    """

    def __init__(
        self,
        *,
        chains: Mapping[str, FakeResponse] | None = None,
        quotes: Mapping[tuple[str, ...], FakeResponse] | None = None,
        creation_timestamp: float | None = None,
    ) -> None:
        self._chains = dict(chains or {})
        self._quotes = dict(quotes or {})
        self.token_metadata = _FakeTokenMetadata(creation_timestamp)
        self.chain_calls: list[str] = []
        self.chain_underlying_quote: list[bool] = []
        self.quote_calls: list[list[str]] = []
        self.quote_fields: list[str | None] = []

    def get_option_chain(
        self, symbol: str, *, include_underlying_quote: bool = False
    ) -> FakeResponse:
        self.chain_calls.append(symbol)
        self.chain_underlying_quote.append(include_underlying_quote)
        if symbol not in self._chains:
            raise KeyError(f"no canned chain for {symbol!r}")
        return self._chains[symbol]

    def get_quotes(self, symbols: Sequence[str], *, fields: str | None = None) -> FakeResponse:
        key = tuple(symbols)
        self.quote_calls.append(list(symbols))
        self.quote_fields.append(fields)
        if key not in self._quotes:
            raise KeyError(f"no canned quotes for {key!r}")
        return self._quotes[key]
