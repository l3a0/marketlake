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

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class FakeResponse:
    """A stand-in for the ``httpx.Response`` ``schwab-py`` returns.

    It carries the four members ``SchwabVendor`` reads: a status code, headers, the
    parsed JSON body, and the body as text.

    It has two forms. The ``body`` form hands a parsed object straight back from
    ``json()``, which is all a test of a well-formed reply needs. The ``content`` form
    holds the raw bytes of the reply instead, and parses and decodes them the way ``httpx``
    does: ``json()`` is ``json.loads`` over the bytes, so a body that is not JSON raises
    ``ValueError``, and ``text`` decodes them as UTF-8 with ``errors="replace"``, which is
    what ``httpx`` does for a reply that declares no charset. That is the form for a reply
    whose body is an HTML page, empty, or JSON that is not an object.
    """

    status_code: int
    body: object = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    content: bytes | None = None

    def json(self) -> object:
        if self.content is not None:
            return json.loads(self.content)
        return self.body

    @property
    def text(self) -> str:
        if self.content is not None:
            return self.content.decode("utf-8", errors="replace")
        return json.dumps(self.body)


@dataclass
class _FakeTokenMetadata:
    """The ``token_metadata`` handle, carrying only the mint epoch second."""

    creation_timestamp: float | None


class _FakeSession:
    """The client's ``session``, the authlib ``OAuth2Client`` a real client carries.

    ``SchwabVendor.from_token`` wraps the session's ``ensure_active_token`` in a lock, and
    ``SchwabVendor.close`` closes it, so the fake carries both. ``token`` is what the lock
    re-reads, ``checked`` records every token ``ensure_active_token`` was asked about, and
    ``closed`` counts the closes.
    """

    def __init__(self) -> None:
        self.token: dict[str, object] = {"access_token": "live"}
        self.checked: list[object] = []
        self.closed = 0

    def ensure_active_token(self, token: object = None) -> bool:
        self.checked.append(token)
        return True

    def close(self) -> None:
        self.closed += 1


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

    ``bars`` keys a ``FakeResponse`` by the price-history request. A key is either a
    ``(symbol, freq)`` pair, which matches any window, or the full request tuple
    ``(symbol, freq, start_datetime, end_datetime)``, which matches exactly. ``freq`` is
    ``"1m"`` or ``"1d"``, naming which of ``schwab-py``'s two per-frequency methods was
    called. The full tuple is tried first, then the pair, so one canned response serves
    every window or each window gets its own.

    Every chain request is recorded so a test can assert the vendor forwarded the
    narrowing parameters. ``chain_from_date``, ``chain_to_date``, and ``chain_strike_count``
    parallel ``chain_calls`` one-for-one. Price history records the same way.
    ``bar_freqs``, ``bar_start``, ``bar_end``, ``bar_extended_hours`` and
    ``bar_previous_close`` parallel ``bar_calls``, so a test can assert the vendor picked
    the right method and forwarded the window and both flags.
    """

    def __init__(
        self,
        *,
        chains: Mapping[object, FakeResponse] | None = None,
        quotes: Mapping[tuple[str, ...], FakeResponse] | None = None,
        bars: Mapping[object, FakeResponse] | None = None,
        creation_timestamp: float | None = None,
    ) -> None:
        self._chains = dict(chains or {})
        self._quotes = dict(quotes or {})
        self._bars = dict(bars or {})
        self.token_metadata = _FakeTokenMetadata(creation_timestamp)
        self.session = _FakeSession()
        self.chain_calls: list[str] = []
        self.chain_underlying_quote: list[bool] = []
        self.chain_from_date: list[date | None] = []
        self.chain_to_date: list[date | None] = []
        self.chain_strike_count: list[int | None] = []
        self.quote_calls: list[list[str]] = []
        self.quote_fields: list[Sequence[str] | None] = []
        self.bar_calls: list[str] = []
        self.bar_freqs: list[str] = []
        self.bar_start: list[datetime | None] = []
        self.bar_end: list[datetime | None] = []
        self.bar_extended_hours: list[bool | None] = []
        self.bar_previous_close: list[bool | None] = []

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

    def _price_history(
        self,
        symbol: str,
        freq: str,
        start_datetime: datetime | None,
        end_datetime: datetime | None,
        need_extended_hours_data: bool | None,
        need_previous_close: bool | None,
    ) -> FakeResponse:
        """Record one price-history request and serve its canned reply.

        Both per-frequency methods land here, passing the frequency they name, so the
        recording and the lookup are written once for the pair.
        """
        self.bar_calls.append(symbol)
        self.bar_freqs.append(freq)
        self.bar_start.append(start_datetime)
        self.bar_end.append(end_datetime)
        self.bar_extended_hours.append(need_extended_hours_data)
        self.bar_previous_close.append(need_previous_close)
        request = (symbol, freq, start_datetime, end_datetime)
        if request in self._bars:
            return self._bars[request]
        if (symbol, freq) in self._bars:
            return self._bars[(symbol, freq)]
        raise KeyError(f"no canned price history for {request!r}")

    def get_price_history_every_minute(
        self,
        symbol: str,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        need_extended_hours_data: bool | None = None,
        need_previous_close: bool | None = None,
    ) -> FakeResponse:
        return self._price_history(
            symbol,
            "1m",
            start_datetime,
            end_datetime,
            need_extended_hours_data,
            need_previous_close,
        )

    def get_price_history_every_day(
        self,
        symbol: str,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        need_extended_hours_data: bool | None = None,
        need_previous_close: bool | None = None,
    ) -> FakeResponse:
        return self._price_history(
            symbol,
            "1d",
            start_datetime,
            end_datetime,
            need_extended_hours_data,
            need_previous_close,
        )
