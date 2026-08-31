"""The real Schwab-backed vendor.

This is the production implementation of the ``Vendor`` seam from ``lake.vendor``.
It talks to Schwab through ``schwab-py``, the maintained client library the design
names as this project's auth and endpoint layer. Everything it returns is the
vendor's payload verbatim. Nothing here parses, validates, or reshapes the body.
Raw stays vendor-verbatim, always. The downstream capture primitive decides what a
status code or a thin chain means. This layer only fetches and hands back.

Two design rules shape this file.

1. Dependency injection over the client. ``SchwabVendor`` is constructed with an
   already-built ``schwab-py`` client object. So a test injects a fake client with
   the same method shapes and never needs the network or a real token. The thin
   ``from_token`` factory builds the real client from a token file. That factory is
   the only place ``schwab-py`` is imported, and it runs only in the by-hand live
   check, never in continuous integration.
2. No wall-clock read. ``token_mint_time`` derives its instant from the token the
   injected client already holds, never from ``datetime.now`` and never from a
   separate file read. The mint time is a stored epoch second on the client's token
   metadata. Converting a stored epoch to a datetime is not a clock read.

``schwab-py`` returns an ``httpx.Response`` from each endpoint call. This module only
needs three things off that response: its status code, its parsed JSON body, and its
headers. The ``HttpResponse`` protocol below pins exactly that surface, so a fake in
a test is a few lines.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.vendor import VendorError, VendorResponse

# The standard location of the Schwab token, per the design's Configuration section.
# It sits outside the repo and outside the backup-synced lake tree. This is a home-
# relative default the live recorder falls back to, never a committed machine path.
DEFAULT_TOKEN_PATH = Path.home() / ".config" / "marketlake" / "token.json"

# The field groups pinned on every batched quote request. ``all`` returns every block
# Schwab offers: quote, fundamental, regular, extended, and reference. Pinning them means
# those blocks are present regardless of the per-account default, so the fundamental,
# regular, extended, and CUSIP columns are never silently empty. schwab-py expects an
# ITERABLE of the field-group values (validated against its Fields enum), not one joined
# string, so this is a tuple. The values are exactly the Fields enum's own values, and
# the client is built with enforce_enums=False so the raw strings pass through.
QUOTE_FIELD_GROUPS = ("quote", "fundamental", "regular", "extended", "reference")


@runtime_checkable
class HttpResponse(Protocol):
    """The slice of an ``httpx.Response`` this vendor reads.

    ``schwab-py`` hands back an ``httpx.Response``. Only these three members matter
    here. A test fake implements the same three.
    """

    @property
    def status_code(self) -> int:
        """The HTTP status code."""
        ...

    @property
    def headers(self) -> Mapping[str, str]:
        """The response headers."""
        ...

    def json(self) -> Mapping[str, object]:
        """The parsed JSON body, exactly as the vendor sent it."""
        ...


@runtime_checkable
class SchwabClient(Protocol):
    """What ``SchwabVendor`` needs from a ``schwab-py`` client.

    The real ``schwab.client.Client`` satisfies this. So does a test fake. Two
    endpoint methods and one nested token attribute is the whole contract.

    ``token_metadata`` is ``schwab-py``'s handle on the loaded token. Its
    ``creation_timestamp`` is the epoch second the refresh token was minted. That is
    the seven-day clock the design tracks. ``schwab-py`` preserves it across the
    automatic access-token refresh, so it reflects the last full browser re-auth, not
    the last silent refresh.
    """

    def get_option_chain(
        self,
        symbol: str,
        *,
        include_underlying_quote: bool = False,
        from_date: date | None = None,
        to_date: date | None = None,
        strike_count: int | None = None,
    ) -> HttpResponse:
        """One option-chain request for an underlying.

        With none of the three narrowing parameters this returns the full chain, as
        before. ``from_date`` / ``to_date`` bound the returned expirations and
        ``strike_count`` caps strikes per expiration. ``schwab-py`` omits any parameter
        left ``None`` from the request, so the bare call is unchanged.
        """
        ...

    def get_quotes(
        self, symbols: Sequence[str], *, fields: Sequence[str] | None = None
    ) -> HttpResponse:
        """Batched equity quotes for a list of symbols, in one request.

        ``fields`` is the comma-separated field groups to include, like
        ``quote,fundamental,reference``. The real client accepts it; a test fake records
        it.
        """
        ...

    @property
    def token_metadata(self) -> object:
        """``schwab-py``'s token handle, carrying ``creation_timestamp``."""
        ...


def _response_from(reply: HttpResponse) -> VendorResponse:
    """Shape one ``schwab-py`` reply into a verbatim ``VendorResponse``.

    The body is taken exactly as ``json()`` parsed it. Headers are copied into a
    plain dict so the result does not alias the client's own mutable state. Nothing
    is inspected or reshaped.
    """
    return VendorResponse(
        status=reply.status_code,
        body=reply.json(),
        headers=dict(reply.headers),
    )


class SchwabVendor:
    """A ``Vendor`` backed by a ``schwab-py`` client.

    Construct it with an already-built client. In production that client comes from
    ``from_token``. In a test it is a fake with the same method shapes. Either way
    this class never imports ``schwab-py`` itself and never touches the network on
    its own.
    """

    def __init__(self, client: SchwabClient) -> None:
        self._client = client

    def get_chain(
        self,
        symbol: str,
        *,
        from_date: date | None = None,
        to_date: date | None = None,
        strike_count: int | None = None,
    ) -> VendorResponse:
        """One option-chain request for an underlying, verbatim.

        The request asks for the underlying quote, so the response carries the
        underlying's price beside the contracts, in the top-level ``underlyingPrice``
        scalar. The design's spot for IV inversion is that reading. The chain's
        ``vendor_quote_ts`` comes from each contract's own ``quoteTimeInLong``, since the
        top-level ``underlying`` block is null on a real chain even here.

        The three optional parameters pass straight through to the client, which omits any
        left ``None``. So the bare call is the full chain, exactly as before. The capture
        chunker supplies ``strike_count=1`` to discover the expiration list, then
        ``from_date`` / ``to_date`` to fetch each expiration range when the full chain
        exceeds Schwab's gateway body limit.
        """
        return _response_from(
            self._client.get_option_chain(
                symbol,
                include_underlying_quote=True,
                from_date=from_date,
                to_date=to_date,
                strike_count=strike_count,
            )
        )

    def get_quotes(self, symbols: Sequence[str]) -> VendorResponse:
        """Batched equity quotes for every symbol, verbatim.

        The request pins every field group Schwab offers, the quote, fundamental,
        regular, extended, and reference blocks, so they are present regardless of the
        account's default field set. The groups pass as an iterable of their string
        values, which the ``enforce_enums=False`` client accepts as-is.
        """
        return _response_from(
            self._client.get_quotes(list(symbols), fields=list(QUOTE_FIELD_GROUPS))
        )

    def token_mint_time(self) -> datetime:
        """When the refresh token in use was minted, timezone-aware in UTC.

        This reads ``creation_timestamp`` off the token the injected client already
        holds. It is never a separate file read, so the value always matches the
        token capture actually runs on. The stored value is an epoch second, so the
        conversion is deterministic and touches no wall clock.
        """
        metadata = self._client.token_metadata
        created = getattr(metadata, "creation_timestamp", None)
        if created is None:
            raise VendorError("client token metadata has no creation_timestamp")
        return datetime.fromtimestamp(float(created), tz=UTC)

    @classmethod
    def from_token(
        cls,
        token_path: str | Path = DEFAULT_TOKEN_PATH,
        *,
        api_key: str,
        app_secret: str,
    ) -> SchwabVendor:
        """Build the real vendor from a token file.

        This is the one place ``schwab-py`` is imported, and it is imported lazily.
        So ``import lake.schwab`` and the whole unit suite run without the library
        installed. This factory is exercised only in the by-hand live check that
        records cassettes from a real Schwab call. It never runs in continuous
        integration, because it needs a real token and real credentials.

        ``api_key`` and ``app_secret`` are secrets. They are passed in by the caller,
        never read from or written to the repo.
        """
        from schwab.auth import client_from_token_file  # lazy: real dep, live only

        # enforce_enums=False lets get_quotes pass the field groups as plain strings
        # rather than schwab-py Fields enum members, keeping this layer enum-agnostic.
        client = client_from_token_file(str(token_path), api_key, app_secret, enforce_enums=False)
        return cls(client)
