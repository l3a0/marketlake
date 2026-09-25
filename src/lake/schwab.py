"""The real Schwab-backed vendor.

This is the production implementation of the ``Vendor`` seam from ``lake.vendor``.
It talks to Schwab through ``schwab-py``, the maintained client library the design
names as this project's auth and endpoint layer. Everything it returns is the
vendor's payload verbatim. Nothing here reads a field of the body, validates it, or
reshapes it. Raw stays vendor-verbatim, always. The downstream capture primitive decides
what a status code or a thin chain means. This layer only fetches and hands back.

One check does run on the body, and it is about its type rather than its content. A
``VendorResponse`` body is a JSON object, and ``_response_from`` below is where a reply
that is not one is handled. A failed reply keeps its status and carries its text beside
an empty body. A successful one is refused with ``VendorBodyError``. That branch on the
status decides nothing about what the status means to the lake. It only refuses to hand
back a success whose body cannot be one.

Two design rules shape this file.

1. Dependency injection over the client. ``SchwabVendor`` is constructed with an
   already-built ``schwab-py`` client object. So a test injects a fake client with
   the same method shapes and never needs the network or a real token. The thin
   ``from_token`` factory builds the real client from a token file. That factory is
   the only place ``schwab-py`` is imported. It runs from the capture daemon on every
   cycle, and in the by-hand live check. It never runs in continuous integration.
2. No wall-clock read. ``token_mint_time`` derives its instant from the token the
   injected client already holds, never from ``datetime.now`` and never from a
   separate file read. The mint time is a stored epoch second on the client's token
   metadata. Converting a stored epoch to a datetime is not a clock read.

``schwab-py`` returns an ``httpx.Response`` from each endpoint call. This module only
needs four things off that response: its status code, its parsed JSON body, its text,
and its headers. The text is read only when the body is not a JSON object. The
``HttpResponse`` protocol below pins exactly that surface, so a fake in a test is a few
lines.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.paths import TOKEN_FILE, config_dir
from lake.token_epoch import epoch_second_to_utc
from lake.vendor import VendorError, VendorResponse, require_utc_bound

# The standard location of the Schwab token, per the design's Configuration section.
# It sits outside the repo and outside the backup-synced lake tree. This is a home-
# relative default the live recorder falls back to, never a committed machine path.
DEFAULT_TOKEN_PATH = config_dir() / TOKEN_FILE

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

    ``schwab-py`` hands back an ``httpx.Response``. Only these four members matter
    here. A test fake implements the same four.
    """

    @property
    def status_code(self) -> int:
        """The HTTP status code."""
        ...

    @property
    def headers(self) -> Mapping[str, str]:
        """The response headers."""
        ...

    @property
    def text(self) -> str:
        """The body decoded as text. ``httpx`` decodes with ``errors="replace"``, so it
        never raises, whatever bytes arrived."""
        ...

    def json(self) -> object:
        """The body parsed as JSON. It raises ``ValueError`` on a body that is not JSON,
        and returns whatever the JSON holds, which need not be an object."""
        ...


@runtime_checkable
class SchwabClient(Protocol):
    """What ``SchwabVendor`` needs from a ``schwab-py`` client.

    The real ``schwab.client.Client`` satisfies this. So does a test fake. Four
    endpoint methods and one nested token attribute is the whole contract the vendor calls
    through. Price history is two of the four, because ``schwab-py`` has no
    frequency-parameterized call: it ships one method per frequency, and this lake captures
    two. ``from_token`` and ``close`` also reach the client's ``session``, the authlib
    ``OAuth2Client`` underneath it, to lock its token refresh and to close its connections.

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

    def get_price_history_every_minute(
        self,
        symbol: str,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        need_extended_hours_data: bool | None = None,
        need_previous_close: bool | None = None,
    ) -> HttpResponse:
        """Per-minute candles for one symbol over a window.

        The parameter names are ``schwab-py``'s own, kept verbatim so this protocol reads
        as the slice of the real client it is. ``SchwabVendor`` always passes both bounds,
        because ``schwab-py`` substitutes a default window for a missing one rather than
        omitting it.
        """
        ...

    def get_price_history_every_day(
        self,
        symbol: str,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        need_extended_hours_data: bool | None = None,
        need_previous_close: bool | None = None,
    ) -> HttpResponse:
        """Per-day candles for one symbol over a window."""
        ...

    @property
    def token_metadata(self) -> object:
        """``schwab-py``'s token handle, carrying ``creation_timestamp``."""
        ...


class VendorBodyError(VendorError):
    """A successful reply whose body is not a JSON object.

    A 2xx body is the payload, so one that will not parse, or that parses to a list,
    ``null`` or a bare string, is the vendor's payload changing shape. Handing it back as
    an empty mapping would read downstream as an empty success, which is a shape Schwab
    already sends on purpose, so it is refused instead. Capture records it per window or
    per batch as ``vendor_body_error``.

    It subclasses ``VendorError`` because that is what the bars walk contains per
    ticker-day. A bare ``ValueError`` would end the walk for every ticker after this one.

    The message names the status, the content type, the body's length and why it is not
    an object. It never carries the body itself, because the bars walk writes the message
    into a finding that lands in the lake, and nothing reads the text of a 2xx body.
    """


def _response_from(reply: HttpResponse) -> VendorResponse:
    """Shape one ``schwab-py`` reply into a verbatim ``VendorResponse``.

    A body that parses to a JSON object is handed back exactly as ``json()`` parsed it,
    whatever the status. Headers are copied into a plain dict so the result does not
    alias the client's own mutable state.

    A body that is not a JSON object splits on the status, because the status is what
    the reply is for.

    1. **A failed reply keeps its status.** A 429 or a 401 answered by a gateway's HTML
       page, or by an empty body, is still a 429 or a 401, and the watchdog pages on
       exactly that. So ``body`` is an empty mapping and ``body_text`` carries the reply's
       text verbatim. Reading the status after the parse would lose it, which is what
       this function did until marketlake #539.
    2. **A successful reply is refused** with ``VendorBodyError``. Its body is the payload,
       and an empty mapping standing in for it would read as an empty success.

    Only ``ValueError`` is caught from the parse. ``json`` raises ``JSONDecodeError`` for
    a body that is not JSON, and both it and ``UnicodeDecodeError`` subclass
    ``ValueError``. Anything else is not a statement about the body and passes through.
    """
    status = reply.status_code
    headers = dict(reply.headers)
    try:
        parsed = reply.json()
    except ValueError as exc:
        parse_error: ValueError | None = exc
        reason = f"it is not JSON ({exc})"
    else:
        if isinstance(parsed, Mapping):
            return VendorResponse(status=status, body=parsed, headers=headers)
        parse_error = None
        reason = f"it parses to {type(parsed).__name__}"
    text = reply.text
    if 200 <= status < 300:
        content_type = reply.headers.get("content-type")
        raise VendorBodyError(
            f"http {status} body is not a JSON object: {reason}, "
            f"content-type {content_type!r}, {len(text)} characters"
        ) from parse_error
    return VendorResponse(status=status, body={}, headers=headers, body_text=text)


# The base classes every authlib credential failure inherits from. Matching on the
# name rather than the type keeps this module's promise that it never imports the
# vendor library, and matching the BASE rather than each leaf means a subclass this
# code has never heard of still classifies as auth.
_AUTH_BASE_NAMES = frozenset({"AuthlibBaseError", "OAuthError"})


class VendorAuthError(Exception):
    """The vendor refused the credentials rather than answering the request.

    A dead refresh token has two shapes. Sometimes the request goes out and comes back
    401, which capture records as ``http_401``. Sometimes the refresh fails first and no
    request is made at all, which raises out of the client. Left alone the second shape
    is recorded under whatever the library happened to name its exception, so the two
    shapes of one failure land under two unrelated classes and only one of them is
    recognised as auth death.

    Raising this collapses the second shape onto one class the lake owns.
    """


def _is_auth_failure(exc: BaseException) -> bool:
    """Whether a raised vendor failure is about credentials, not about the request."""
    return any(base.__name__ in _AUTH_BASE_NAMES for base in type(exc).__mro__)


@contextmanager
def _auth_failures_named() -> Iterator[None]:
    """Re-raise a credential failure as ``VendorAuthError``, and pass everything else.

    Only the classification changes. The original is kept as the cause, so nothing about
    what went wrong is lost from a traceback.
    """
    try:
        yield
    except Exception as exc:
        if _is_auth_failure(exc):
            raise VendorAuthError(str(exc) or type(exc).__name__) from exc
        raise


def serialize_token_refresh(session: object) -> None:
    """Make an authlib session refresh an expired token once, however many threads ask.

    Marketlake #532 fires a capture cycle's requests from a pool of threads through one
    ``schwab-py`` client, and that client's ``session`` is authlib's sync ``OAuth2Client``.
    Its ``request`` calls ``self.ensure_active_token(self.token)`` before every request with
    no lock around it, unlike authlib's async client, which holds one. So when the access
    token has lapsed, every request in flight refreshes it. A probe on 2026-09-24 sent 19
    concurrent requests through one client holding an expired token: the token endpoint was
    hit 19 times, and the ``update_token`` callback, which in ``schwab-py`` rewrites
    ``token.json``, ran 19 times over the same file. The access token lives 30 minutes with a
    300-second leeway and the client is rebuilt every cycle, so about one cycle in 25 would
    do that.

    The fix replaces the session's ``ensure_active_token`` with one that takes a lock and
    then checks the session's live ``token``, not the token it was called with. That second
    half is the one that matters. authlib tests expiry on its argument, so a thread that
    waited on the lock still holds the expired token object it was called with, and a lock
    that forwarded the argument refreshed eight times out of eight in the same probe.

    It reaches into the session by attribute, so a library upgrade that moves it raises
    ``AttributeError`` from ``from_token`` rather than running capture with the refresh
    unguarded. Two tests cover what can be covered offline.
    ``tests/unit/test_token_refresh_lock.py`` drives a real authlib ``OAuth2Client`` through
    ``httpx.MockTransport``, so an authlib upgrade that renames ``ensure_active_token`` fails
    there. ``tests/unit/test_schwab_from_token.py`` checks that ``from_token`` installs the
    lock. Neither can see a ``schwab-py`` upgrade that renames the client's ``session``, since
    no test builds a real ``schwab-py`` client. That upgrade raises from every ``from_token``
    caller, the capture daemon included, which exits and is relaunched into the same error.
    """
    ensure_active_token = session.ensure_active_token
    lock = threading.Lock()

    def ensure_active_token_once(token: object = None) -> object:
        with lock:
            return ensure_active_token(session.token)

    session.ensure_active_token = ensure_active_token_once


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
        with _auth_failures_named():
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
        with _auth_failures_named():
            return _response_from(
                self._client.get_quotes(list(symbols), fields=list(QUOTE_FIELD_GROUPS))
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
        """Per-minute candles for one symbol over a window, verbatim.

        Both bounds run through ``require_utc_bound`` before the request goes out, so a
        missing or naive bound is refused here rather than silently becoming a fifty-five
        year window or a window in the capture machine's local zone. The seam's own
        docstring carries the whole reason.

        The two flags follow the seam's ordinary rule and pass straight through, so
        ``schwab-py`` omits either one left ``None``.
        """
        with _auth_failures_named():
            return _response_from(
                self._client.get_price_history_every_minute(
                    symbol,
                    start_datetime=require_utc_bound(start, "start"),
                    end_datetime=require_utc_bound(end, "end"),
                    need_extended_hours_data=extended_hours,
                    need_previous_close=previous_close,
                )
            )

    def get_daily_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        extended_hours: bool | None = None,
        previous_close: bool | None = None,
    ) -> VendorResponse:
        """Per-day candles for one symbol over a window, verbatim.

        This is ``get_minute_bars`` against the daily endpoint. They stay two methods
        because ``schwab-py`` has no frequency-parameterized call, and choosing between
        two client methods inside one vendor method would make this layer dispatch rather
        than forward.
        """
        with _auth_failures_named():
            return _response_from(
                self._client.get_price_history_every_day(
                    symbol,
                    start_datetime=require_utc_bound(start, "start"),
                    end_datetime=require_utc_bound(end, "end"),
                    need_extended_hours_data=extended_hours,
                    need_previous_close=previous_close,
                )
            )

    def token_mint_time(self) -> datetime:
        """When the refresh token in use was minted, timezone-aware in UTC.

        This reads ``creation_timestamp`` off the token the injected client already
        holds. It is never a separate file read, so the value always matches the
        token capture actually runs on. The stored value is an epoch second, so the
        conversion is deterministic and touches no wall clock. The value is untrusted,
        so it runs through ``token_epoch``'s shared policy, the same one
        ``control_plane.read_token_mint`` calls on the token file's copy of the same
        field. A shape that policy refuses is a vendor failing to say when its own
        token was minted, so it raises ``VendorError`` here rather than the bare
        ``ValueError`` the file reader raises.
        """
        metadata = self._client.token_metadata
        created = getattr(metadata, "creation_timestamp", None)
        if created is None:
            raise VendorError("client token metadata has no creation_timestamp")
        try:
            return epoch_second_to_utc(created)
        except ValueError as exc:
            raise VendorError(f"client token metadata: {exc}") from exc

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
        installed. In tests this factory is reached only from the by-hand live check that
        records cassettes from a real Schwab call. It never runs in continuous
        integration, because it needs a real token and real credentials.

        ``api_key`` and ``app_secret`` are secrets. They are passed in by the caller,
        never read from or written to the repo.
        """
        from schwab.auth import client_from_token_file  # lazy: real dep, live only

        # enforce_enums=False lets get_quotes pass the field groups as plain strings
        # rather than schwab-py Fields enum members, keeping this layer enum-agnostic.
        client = client_from_token_file(str(token_path), api_key, app_secret, enforce_enums=False)
        # The capture cycle shares this client across a pool of threads, so an expired token
        # must be refreshed once rather than once per request in flight.
        serialize_token_refresh(client.session)
        return cls(client)

    def close(self) -> None:
        """Close the client's connections, and never raise.

        ``run_cycle_from_config`` builds a new client every cycle, and marketlake #532 lets a
        cycle open one connection per request in flight rather than one in all. authlib's
        ``OAuth2Client`` passes itself to its own base class as its session, a reference
        cycle, so dropping the client frees nothing until the cyclic collector runs, and the
        sockets stay open until then. Closing it at the end of the cycle frees them at once.
        A close that fails costs nothing the cycle captured, so it prints one line to the
        launchd log and returns.
        """
        session = getattr(self._client, "session", None)
        if session is None:
            return
        try:
            session.close()
        except Exception as exc:  # noqa: BLE001 - a close must never cost a captured cycle
            print(f"schwab: client close failed: {type(exc).__name__}: {exc}", file=sys.stderr)
