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
from datetime import date


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

    ``chains`` keys a ``FakeResponse`` by the chain request. A key is either a plain
    symbol string, which matches any narrowing, or the full request tuple
    ``(symbol, from_date, to_date, strike_count)``, which matches exactly. The full-tuple
    key is tried first, then the symbol key, so a test can supply one response for the
    bare chain or distinct responses for each date window the chunker fetches.
    ``quotes`` maps a tuple of symbols to its ``FakeResponse``. ``creation_timestamp`` is
    the token mint epoch second the vendor converts in ``token_mint_time``. Pass ``None``
    to model a client whose token metadata carries no mint time.

    Every chain request is recorded so a test can assert the vendor forwarded the
    narrowing parameters. ``chain_from_date``, ``chain_to_date``, and ``chain_strike_count``
    parallel ``chain_calls`` one-for-one.
    """

    def __init__(
        self,
        *,
        chains: Mapping[object, FakeResponse] | None = None,
        quotes: Mapping[tuple[str, ...], FakeResponse] | None = None,
        creation_timestamp: float | None = None,
    ) -> None:
        self._chains = dict(chains or {})
        self._quotes = dict(quotes or {})
        self.token_metadata = _FakeTokenMetadata(creation_timestamp)
        self.chain_calls: list[str] = []
        self.chain_underlying_quote: list[bool] = []
        self.chain_from_date: list[date | None] = []
        self.chain_to_date: list[date | None] = []
        self.chain_strike_count: list[int | None] = []
        self.quote_calls: list[list[str]] = []
        self.quote_fields: list[Sequence[str] | None] = []

    def get_option_chain(
        self,
        symbol: str,
        *,
        include_underlying_quote: bool = False,
        from_date: date | None = None,
        to_date: date | None = None,
        strike_count: int | None = None,
    ) -> FakeResponse:
        self.chain_calls.append(symbol)
        self.chain_underlying_quote.append(include_underlying_quote)
        self.chain_from_date.append(from_date)
        self.chain_to_date.append(to_date)
        self.chain_strike_count.append(strike_count)
        request = (symbol, from_date, to_date, strike_count)
        if request in self._chains:
            return self._chains[request]
        if symbol in self._chains:
            return self._chains[symbol]
        raise KeyError(f"no canned chain for {request!r}")

    def get_quotes(
        self, symbols: Sequence[str], *, fields: Sequence[str] | None = None
    ) -> FakeResponse:
        key = tuple(symbols)
        self.quote_calls.append(list(symbols))
        self.quote_fields.append(fields)
        if key not in self._quotes:
            raise KeyError(f"no canned quotes for {key!r}")
        return self._quotes[key]
