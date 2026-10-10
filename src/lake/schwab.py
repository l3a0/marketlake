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
   ``from_token`` factory builds the real client from a token file through
   ``client_from_token``, the only place this module imports ``schwab-py``. It runs from the
   capture daemon on every cycle, and in the by-hand live check. Continuous integration
   reaches it only through ``httpx.MockTransport`` and a temporary token file. The file is
   read on every build. The one exception to building from it is a refreshed token whose
   write failed, which the process keeps until the file catches up (marketlake #860).
   ``client_from_token`` carries that rule.
2. No wall-clock read. ``token_mint_time`` derives its instant from the token the
   injected client already holds, never from ``datetime.now`` and never from a
   separate file read. The mint time is a stored epoch second on the client's token
   metadata. Converting a stored epoch to a datetime is not a clock read.

``schwab-py`` returns an ``httpx.Response`` from each endpoint call. This module only
needs four things off that response: its status code, its parsed JSON body, its text,
and its headers. The text is read only when the body is not a JSON object. The
``HttpResponse`` protocol below pins exactly that surface, so a fake in a test is a few
lines.

One thing more is read when it is there: the request's timing, the four instants
``lake.vendor.RequestTiming`` names. ``attach_timing`` records them by adding httpx event
hooks to the client's ``session``, which is the ``httpx.Client`` ``schwab-py`` builds on
through authlib's ``OAuth2Client``. ``schwab-py`` passes no httpx options through
``client_from_access_functions``, but the session's ``event_hooks`` can be set after the
client exists. The hooks read the caller's injected clock, never a clock of their own, so
the second rule above still holds. Each request carries its own record in its
``extensions``, never in state shared between requests, so a record stays with its
request however many run at once. Every hook catches its own failures, because a hook
that raises propagates out of ``session.get`` and would cost the response it was timing.
A fake client with no ``session`` records nothing, and its responses carry no timing.
"""

from __future__ import annotations

import errno
import functools
import json
import sys
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.clock import Clock
from lake.paths import default_token_path
from lake.token_epoch import epoch_second_to_utc
from lake.vendor import RequestTiming, VendorError, VendorResponse, require_utc_bound

# The field groups pinned on every batched quote request. ``all`` returns every block
# Schwab offers: quote, fundamental, regular, extended, and reference. Pinning them means
# those blocks are present regardless of the per-account default, so the fundamental,
# regular, extended, and CUSIP columns are never silently empty. schwab-py expects an
# ITERABLE of the field-group values (validated against its Fields enum), not one joined
# string, so this is a tuple. The values are exactly the Fields enum's own values, and
# the client is built with enforce_enums=False so the raw strings pass through.
QUOTE_FIELD_GROUPS = ("quote", "fundamental", "regular", "extended", "reference")


def _say_built(line: Callable[[], str]) -> None:
    """Build one diagnostic and print it to stderr, and drop it rather than raise on either step.

    Every line this module prints sits on a request's path, inside a token refresh or a
    client's close. On the laptop the daemon's stderr is a file on the same volume as
    ``token.json``, and a probe on a full ramdisk showed ``print`` raising ``ENOSPC`` once the
    log's last block filled. A closed stderr raises ``ValueError``. Either raise, let out of a
    refresh, would cost the request its minute, so this catches ``Exception``. The string is
    built inside the guard too, so an exception whose ``__str__`` raises costs only the line.
    This is ``capture._say_built``'s shape, written again because ``lake.capture`` imports this
    module.
    """
    try:
        print(line(), file=sys.stderr)
    except Exception:  # noqa: BLE001 - nowhere left to report it, and the request goes on
        pass


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
        """The body decoded as text, in the charset the reply declares or UTF-8 when it
        declares none. ``httpx`` decodes with ``errors="replace"``, so bytes that are not
        valid in that charset become replacement characters rather than a raise. One
        declared charset still raises: ``utf-16`` with no byte-order mark raises
        ``UnicodeError``. ``schwab-py`` logs every reply's text before this module sees it,
        so in production that raise happens inside the client call, not here."""
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

    The timing record rides along when ``attach_timing`` left one on the request.
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
            return VendorResponse(
                status=status, body=parsed, headers=headers, timing=read_timing(reply)
            )
        parse_error = None
        reason = f"it parses to {type(parsed).__name__}"
    text = reply.text
    if 200 <= status < 300:
        content_type = reply.headers.get("content-type")
        raise VendorBodyError(
            f"http {status} body is not a JSON object: {reason}, "
            f"content-type {content_type!r}, {len(text)} characters"
        ) from parse_error
    return VendorResponse(
        status=status, body={}, headers=headers, body_text=text, timing=read_timing(reply)
    )


# -- request timing ------------------------------------------------------------

# The key a request's timing record sits under in its httpx ``extensions``.
_TIMING_EXTENSION = "lake_timing"

# The httpcore trace events that end a new connection's setup. TLS finishes after TCP, so
# the last of the two to fire is when the connection was ready.
_CONNECTED_EVENTS = frozenset({"connection.connect_tcp.complete", "connection.start_tls.complete"})

# The suffix of the httpcore trace event that ends the body, for HTTP/1.1 and HTTP/2 alike.
_BODY_EVENT_SUFFIX = ".receive_response_body.complete"


@dataclass
class _TimingRecord:
    """One request's timing while it is in flight. Written only by that request's hooks."""

    sent: datetime | None = None
    connected: datetime | None = None
    headers: datetime | None = None
    body: datetime | None = None
    failure: str | None = None

    def fail(self, exc: BaseException) -> None:
        """Keep the first failure, so the record names what broke rather than what followed."""
        if self.failure is None:
            self.failure = f"{type(exc).__name__}: {exc}"


def attach_timing(client: object, clock: Clock) -> bool:
    """Record every request's timing on a ``schwab-py`` client, and say whether it could.

    Two hooks go on the client's ``session``, beside any it already has. The request hook
    stamps ``sent`` and gives the request its own record, plus an httpcore ``trace``
    callback that stamps ``connected`` and ``body`` as those events fire. The response
    hook stamps ``headers``. httpx fires it once the headers are in and before the body is
    read, so the gap between the two is the body's time on the wire.

    It returns ``False`` rather than raising when the client has no ``session`` to hook,
    which is every fake. The chain-size probe builds its own client, and this takes any
    client, so it can time its requests the same way (marketlake #354).
    """
    session = getattr(client, "session", None)
    if session is None:
        return False

    def tracer(record: _TimingRecord) -> Callable[[str, object], None]:
        def trace(event: str, info: object) -> None:
            try:
                if event in _CONNECTED_EVENTS:
                    record.connected = clock.now()
                elif event.endswith(_BODY_EVENT_SUFFIX):
                    record.body = clock.now()
            except Exception as exc:  # noqa: BLE001 - timing must never cost a response
                record.fail(exc)

        return trace

    def on_request(request: object) -> None:
        # The record and its trace go on first and the stamp last, each on its own, so a
        # clock that fails costs ``sent`` alone and the trace still stamps the rest.
        record = _TimingRecord()
        try:
            request.extensions[_TIMING_EXTENSION] = record
            request.extensions["trace"] = tracer(record)
        except Exception as exc:  # noqa: BLE001 - timing must never cost a response
            record.fail(exc)
        try:
            record.sent = clock.now()
        except Exception as exc:  # noqa: BLE001 - timing must never cost a response
            record.fail(exc)

    def on_response(response: object) -> None:
        try:
            record = response.request.extensions.get(_TIMING_EXTENSION)
            if isinstance(record, _TimingRecord):
                try:
                    record.headers = clock.now()
                except Exception as exc:  # noqa: BLE001 - timing must never cost a response
                    record.fail(exc)
        except Exception:  # noqa: BLE001 - no record to note it on, and none is owed
            pass

    try:
        hooks = session.event_hooks
        session.event_hooks = {
            "request": [*hooks.get("request", ()), on_request],
            "response": [*hooks.get("response", ()), on_response],
        }
    except Exception:  # noqa: BLE001 - a client that will not take hooks is left untimed
        return False
    return True


def read_timing(reply: object) -> RequestTiming | None:
    """The timing ``attach_timing`` recorded for one reply, or ``None`` when there is none.

    Total. A fake reply has no ``request`` and a real one without hooks has no record, and
    both read as ``None``. ``bytes`` is httpx's ``num_bytes_downloaded``, the body's size on
    the wire before any decompression.
    """
    try:
        record = reply.request.extensions.get(_TIMING_EXTENSION)
    except Exception:  # noqa: BLE001 - an untimed reply is not a failure
        return None
    if not isinstance(record, _TimingRecord):
        return None
    size = getattr(reply, "num_bytes_downloaded", None)
    return RequestTiming(
        sent=record.sent,
        connected=record.connected,
        headers=record.headers,
        body=record.body,
        bytes=size if isinstance(size, int) and not isinstance(size, bool) else None,
        failure=record.failure,
    )


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


# The base classes of the httpx failures the design calls transient, marketlake #558: the
# four timeouts, a refused or reset connection, a failed read or write, and a server that
# closed before its response was complete. A transfer cut mid-body raises the last one, so it
# never reaches the body parse. They are matched by name for the same two reasons as
# ``_AUTH_BASE_NAMES``.
_TRANSIENT_BASE_NAMES = frozenset({"TimeoutException", "NetworkError", "RemoteProtocolError"})


def is_transient_failure(exc: BaseException) -> bool:
    """Whether a raised vendor failure earns the one retry the design promises.

    It reads ``exc`` and then each ``__cause__`` behind it. A wrapper raised ``from`` an
    httpx transport error keeps that error as its cause (marketlake #450 plans one at this
    class), and reading only the outer class would stop the retry matching every timeout the
    day such a wrapper landed, with no fake that raises httpx's classes directly noticing.

    It does not follow ``__context__``, the exception that was being handled when this one
    was raised. That link would match an httpx error that was dealt with before an unrelated
    failure was raised, and retry the unrelated one.

    Nothing else qualifies. A builtin ``TimeoutError`` is not an httpx class, and a token
    refresh answered with a 5xx raises httpx's ``HTTPStatusError`` out of authlib, which is
    not a transport error and stays unretried until marketlake #547 decides what it is.
    """
    seen: set[int] = set()
    link: BaseException | None = exc
    while link is not None and id(link) not in seen:
        seen.add(id(link))
        if any(base.__name__ in _TRANSIENT_BASE_NAMES for base in type(link).__mro__):
            return True
        link = link.__cause__
    return False


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


# One lock for the whole process, not one per client. The capture loop builds a new client
# every cycle, and once cycles overlap (marketlake #534) two clients can find the token
# expired at the same moment. A lock per client would let both refresh and both rewrite
# ``token.json``. This one lets one refresh, and the other re-reads the file the first wrote.
# It is reentrant so that a session wrapped twice waits on nothing but itself. A plain lock
# would leave the outer wrapper holding it while the inner one waits, and that hangs every
# client in the process.
_TOKEN_REFRESH_LOCK = threading.RLock()


def serialize_token_refresh(
    session: object, adopt_stored: Callable[[], None] | None = None
) -> None:
    """Make an authlib session refresh an expired token once, however many threads ask.

    Marketlake #532 fires a capture cycle's requests from a pool of threads through one
    ``schwab-py`` client, and that client's ``session`` is authlib's sync ``OAuth2Client``.
    Its ``request`` calls ``self.ensure_active_token(self.token)`` before every request with
    no lock around it, unlike authlib's async client, which holds one. So when the access
    token has lapsed, every request in flight refreshes it. A probe on 2026-09-24 sent 19
    concurrent requests through one client holding an expired token: the token endpoint was
    hit 19 times, and the ``update_token`` callback, which rewrites ``token.json``, ran 19
    times over the same file. The access token lives 30 minutes with a 300-second leeway and
    the client is rebuilt every cycle, so about one cycle in 25 would do that.

    The fix replaces the session's ``ensure_active_token`` with one that takes a lock and
    then checks the session's live ``token``, not the token it was called with. That second
    half is the one that matters. authlib tests expiry on its argument, so a thread that
    waited on the lock still holds the expired token object it was called with, and a lock
    that forwarded the argument refreshed eight times out of eight in the same probe.

    The lock is ``_TOKEN_REFRESH_LOCK``, shared by every session in the process, because two
    clients refreshing at once is the same race one level up (marketlake #564). Sharing the
    lock is not enough on its own. A second client that waited still holds the expired token
    it was built with, since the first client's refresh changed the first client's session
    and the file, never the second's session. Checking only its own session, it would refresh
    again. ``adopt_stored`` closes that gap. When the session's token has expired, it is
    called under the lock to replace the session's token with the one stored in the token
    file. The file's token is then the one checked, so a refresh another client already
    made is adopted rather than repeated, and a refresh that does happen uses the newest
    refresh token on disk. That keeps the lake correct whether or not Schwab rotates the
    refresh token on each refresh. marketlake #633 measured it on 2026-10-05: a refresh
    issued a new access token and left the refresh token unchanged, and a refresh on one
    host did not revoke the other host's copy. Had Schwab rotated, a second refresh with
    the superseded refresh token would have been refused and read as auth death, and the
    adoption is what still guards against Schwab starting to.

    A refresh whose file write fails leaves the new token in memory and the old one on
    disk. Until marketlake #860 the write's error raised out of the request, so a full
    disk lost one request every minute, and every client built afterwards started from the
    stale file and refreshed again. Now the refresh writer keeps the new token in the
    process and lets the request go on, and the reader hands it to later clients while it
    is newer than the file. ``client_from_token`` carries that rule, and what changes when
    the refresh token rotated.

    A re-read that fails leaves the session's own token in place and prints one line naming
    the failure's type, never its message or anything from the file. The request then goes
    ahead as it did before the re-read existed, so an unreadable file costs at most the
    duplicate refresh the re-read was there to save, not the cycle. The line goes through
    ``_say_built``, so a stderr that refuses it costs the line and not the request.

    The lock also keeps the token writer safe. ``reauth.write_token`` names its temp file by
    process id, so two threads writing at once would share one temp file. The process
    writes the token from two places, and both take this lock. A refresh writes from inside
    ``ensure_active_token``, and the reader retries a held token's write inside its own
    section of the lock.

    It reaches into the session by attribute, so a library upgrade that moves it raises
    ``AttributeError`` from ``from_token`` rather than running capture with the refresh
    unguarded. ``tests/unit/test_token_refresh_lock.py`` drives a real authlib
    ``OAuth2Client`` through ``httpx.MockTransport``, so an authlib upgrade that renames
    ``ensure_active_token`` fails there. ``tests/component/test_token_file_refresh.py``
    builds real ``schwab-py`` clients through ``client_from_token`` over the same transport,
    so an upgrade that renames the client's ``session`` or its token metadata fails there.
    """
    ensure_active_token = session.ensure_active_token

    def ensure_active_token_once(token: object = None) -> object:
        with _TOKEN_REFRESH_LOCK:
            if adopt_stored is not None and session.token.is_expired(leeway=session.leeway):
                try:
                    adopt_stored()
                except Exception as exc:  # noqa: BLE001 - the session's own token still works
                    _say_built(
                        lambda exc=exc: (
                            "schwab: token file re-read failed, refreshing from the client's "
                            f"own token: {type(exc).__name__}"
                        )
                    )
            return ensure_active_token(session.token)

    session.ensure_active_token = ensure_active_token_once


def _read_token_file(token_path: Path) -> object:
    """The token file's contents, parsed. ``schwab-py``'s own file loader, spelled here."""
    return json.loads(token_path.read_bytes())


# The two fields a stored token must carry to replace a client's own. Without either, the
# adopted token could not be sent or could not be refreshed.
_STORED_TOKEN_KEYS = ("access_token", "refresh_token")


def _adopt_stored_token(client: object, read_token: Callable[[], object]) -> None:
    """Replace a client's token with the one ``read_token`` returns.

    That is the token file's token, or a refreshed token this process holds because its
    write failed and that is newer than the file's. ``client_from_token``'s reader decides
    which. Either way it is ``schwab-py``'s envelope: the token itself and ``creation_timestamp``,
    the refresh token's mint time. Both are adopted. The session takes the token, which is
    what the next request is sent with. The client's ``token_metadata`` takes both, because
    ``schwab-py`` writes its ``creation_timestamp`` back into the file on the next refresh.
    Adopting the token alone would let a client built before a mid-week re-login write the
    old mint time over the new one. ``token_mint_time`` reads the same field, so it keeps
    matching the token the client actually runs on.

    Everything is checked before anything is assigned, so a file that cannot stand in for
    the client's own token raises and leaves the client exactly as it was. That covers a
    token with no access token or refresh token to send, and one with no whole-second
    ``expires_at``, which authlib's ``is_expired`` cannot judge and would call live.
    ``schwab-py`` writes all three on every login and every refresh, so a real file passes.
    """
    metadata = client.token_metadata
    stored = read_token()
    if not isinstance(stored, Mapping):
        raise ValueError(f"token file holds {type(stored).__name__}, not an object")
    token = stored.get("token")
    created = stored.get("creation_timestamp")
    if not isinstance(token, Mapping) or created is None:
        raise ValueError("token file has no token object or no creation_timestamp")
    if not all(token.get(key) for key in _STORED_TOKEN_KEYS) or not isinstance(
        token.get("expires_at"), int
    ):
        raise ValueError("token file's token lacks an access token, refresh token or expiry")
    client.session.token = dict(token)
    metadata.token = client.session.token
    metadata.creation_timestamp = created


@dataclass
class _HeldToken:
    """A refreshed token whose write to the token file failed, kept for this process.

    ``text`` is the envelope as JSON, the same text the write would have landed. Each
    client parses it afresh, so a client built from it matches one built from the file and
    no two clients share one token object. It is a full brokerage credential, so the
    ``repr`` leaves it out. The two stamps are what the reader compares against the file.
    ``printed`` is the errno name the failure line last printed, and ``None`` when no
    failure line has printed for this token, as after a rotated refresh.
    """

    text: str = field(repr=False)
    creation_timestamp: int | float
    expires_at: int
    printed: str | None


# The held tokens, one per token path. Guarded by ``_TOKEN_REFRESH_LOCK`` alone. The writer
# that fills it already runs under that lock, so a second lock could deadlock against it.
_HELD: dict[Path, _HeldToken] = {}


def reset_held_tokens() -> None:
    """Forget every held token. For tests, which share one process."""
    with _TOKEN_REFRESH_LOCK:
        _HELD.clear()


def _usable_stamps(envelope: object) -> tuple[int | float, int] | None:
    """The envelope's ``creation_timestamp`` and ``expires_at``, or ``None`` when either fails.

    ``creation_timestamp`` must pass ``token_epoch.epoch_second_to_utc``, the one guard for
    that field. ``expires_at`` must be an ``int`` and not a ``bool``, because authlib's
    ``is_expired`` judges nothing else and comparing a string raises ``TypeError``.
    """
    if not isinstance(envelope, Mapping):
        return None
    token = envelope.get("token")
    created = envelope.get("creation_timestamp")
    if not isinstance(token, Mapping):
        return None
    try:
        epoch_second_to_utc(created)
    except ValueError:
        return None
    expires_at = token.get("expires_at")
    if isinstance(expires_at, bool) or not isinstance(expires_at, int):
        return None
    return created, expires_at


def _errno_name(exc: OSError) -> str:
    """The errno's name, such as ``ENOSPC``, or the exception's class when it has none."""
    code = exc.errno
    name = errno.errorcode.get(code) if isinstance(code, int) else None
    return name if name is not None else type(exc).__name__


def _errno_and_reason(exc: OSError) -> str:
    """The errno's name with the system's text for it, never a ``None`` text."""
    name = _errno_name(exc)
    reason = exc.strerror
    return f"{name}: {reason}" if isinstance(reason, str) and reason else name


def _say_recovered(path: Path) -> None:
    """Print that the file caught up. It says nothing about whether the disk recovered."""
    _say_built(
        lambda: (
            f"schwab: token file at {path} now holds a token at least as new as this "
            "process's, so the held token is dropped"
        )
    )


def _hold(
    path: Path,
    envelope: object,
    stamps: tuple[int | float, int] | None,
    printed: str | None,
) -> None:
    """Hold ``envelope`` for ``path``, or clear an older held token when it cannot be held."""
    if stamps is None:
        _HELD.pop(path, None)
        return
    created, expires_at = stamps
    _HELD[path] = _HeldToken(json.dumps(envelope), created, expires_at, printed)


@functools.cache
def _refresh_writer_type() -> type:
    """The refresh writer's class, built once on first use.

    It subclasses ``reauth.TokenWriter``, which is imported here rather than at the top for
    the reason ``client_from_token`` gives.
    """
    from lake.reauth import TokenWriter

    class RefreshTokenWriter(TokenWriter):
        """The ``token_write_func`` for refreshes: a write that fails keeps the token.

        A refresh's write that lands clears any held token for the path, and prints the
        recovery line when one was held. A write that raises ``OSError``, of any errno,
        keeps the refreshed token in the process. ``EROFS`` after a remount, ``EDQUOT`` and
        ``EIO`` lose the request the same way ``ENOSPC`` does. Nothing wider is caught, so a
        ``TypeError`` from serialising the token is a bug and still raises.

        What happens next turns on whether the refresh token rotated, which authlib's
        ``refresh_token`` keyword, the one the refresh sent, says. A writer called without
        it cannot tell, and takes the rotated side, which is the loud one.

        1. **Not rotated.** The request goes on. The failure line prints once per outage:
           when nothing was held before, or when the errno changed, as when ``ENOSPC``
           turns into ``EROFS`` after a remount.
        2. **Rotated.** Only this process now holds a refresh token Schwab accepts. The
           rotation line prints on every such failure, since each one costs a request, and
           the write's own error is raised again. A short-lived process such as the canary
           then fails as loudly as before, and its re-auth reminder is the right action.

        A token with no usable stamps, per ``_usable_stamps``, is never held, and it
        clears any older held token it supersedes. Neither line names a token field.
        """

        def __call__(self, token: object, *args: object, **kwargs: object) -> None:
            with _TOKEN_REFRESH_LOCK:
                held = _HELD.get(self.token_path)
                try:
                    super().__call__(token, *args, **kwargs)
                except OSError as exc:
                    if self._kept(token, held, exc, kwargs):
                        raise
                    return
                if self.token_path in _HELD:
                    del _HELD[self.token_path]
                    _say_recovered(self.token_path)

        def _kept(self, token: object, held: _HeldToken | None, exc: OSError, kwargs: dict) -> bool:
            """Keep the token after a failed write, print, and say whether it rotated."""
            path = self.token_path
            sent = kwargs.get("refresh_token")
            new = token.get("token") if isinstance(token, Mapping) else None
            fresh = new.get("refresh_token") if isinstance(new, Mapping) else None
            stamps = _usable_stamps(token)
            name = _errno_name(exc)
            if fresh != sent or sent is None:
                _hold(path, token, stamps, None)
                _say_built(
                    lambda: (
                        f"schwab: token file write failed ({_errno_and_reason(exc)}) at "
                        f"{path} and the refresh token rotated; only this process holds the "
                        "new one, so this request fails, and a restart before the write "
                        "lands will need a re-auth"
                    )
                )
                return True
            if held is None or held.printed != name:
                _say_built(
                    lambda: (
                        f"schwab: token file write failed ({_errno_and_reason(exc)}) at "
                        f"{path}; the refreshed token stays in this process and the request "
                        "goes on, and each new client retries the write"
                    )
                )
            _hold(path, token, stamps, name)
            return False

    return RefreshTokenWriter


def _retry_held(path: Path, held: _HeldToken) -> bool:
    """Write the held token to the file, and say whether it landed. Prints nothing.

    The write takes ``_TOKEN_REFRESH_LOCK``, because ``write_token``'s temp name is per
    process, so two threads writing at once would share one temp file.
    """
    from lake.reauth import write_token

    with _TOKEN_REFRESH_LOCK:
        try:
            write_token(path, json.loads(held.text))
        except OSError:
            return False
    return True


def _read_or_held(path: Path) -> object:
    """The token a client at ``path`` should run on: the file's, or a held one newer than it.

    The whole decision runs in one section of ``_TOKEN_REFRESH_LOCK``: look up the held
    token, read the file, compare, retry the write and clear. Cycles overlap, and the
    close+5 fill builds in the same process. A reader that compared outside the lock could
    decide the held token wins, wait, and then write it over a newer token a refresh landed
    in the meantime.

    1. With nothing held, the file is returned as it always was.
    2. A file that cannot be read or parsed raises, held token or not, and is not touched.
       A full disk does not cause that, since ``write_token`` is atomic. Letting the held
       token win would hide the fault until the next restart, and would write over a file
       a hand pull landed after a re-login, the one case where the file is newer.
    3. A file that parses without usable stamps, per ``_usable_stamps``, is returned as it
       is, and the held token is kept.
    4. The file wins when its ``creation_timestamp`` is later, meaning a re-login or a
       pulled newer token landed, or when the two are equal and its ``expires_at`` is no
       earlier. The held token is dropped and the recovery line printed.
    5. Otherwise the held token wins. The write is retried. A retry that lands drops the
       held token and prints the recovery line. One that fails prints nothing, and leaves
       the errno last printed as it was. Either way the held envelope is returned, parsed
       afresh.
    """
    with _TOKEN_REFRESH_LOCK:
        held = _HELD.get(path)
        stored = _read_token_file(path)
        if held is None:
            return stored
        stamps = _usable_stamps(stored)
        if stamps is None:
            return stored
        created, expires_at = stamps
        if created > held.creation_timestamp or (
            created == held.creation_timestamp and expires_at >= held.expires_at
        ):
            del _HELD[path]
            _say_recovered(path)
            return stored
        if _retry_held(path, held):
            del _HELD[path]
            _say_recovered(path)
        return json.loads(held.text)


def client_from_token(token_path: str | Path, *, api_key: str, app_secret: str) -> object:
    """Build a real ``schwab-py`` client from a token file, with its refreshes made safe.

    This is the one place this module imports ``schwab-py``, and it is imported lazily. So
    ``import lake.schwab`` and the whole unit suite run without the library installed.

    Three things differ from ``schwab-py``'s own ``client_from_token_file``.

    1. **The token is written atomically.** ``schwab-py``'s writer opens the file with
       ``open(token_path, 'w')`` and then writes into it, so the file is empty between the
       two and a reader in that moment meets a truncated token. That reader can be another
       thread's client in the daemon, which builds the close+5 fill's clients too, or
       onboarding or the Sunday canary in a process of its own.
       ``client_from_access_functions`` takes a ``token_write_func``, and this passes a
       subclass of the re-auth ritual's ``TokenWriter``: a temp file, an fsync, mode 0600,
       then one ``os.replace``. A reader meets the old token or the new one, never part of
       either. The mode is set before the rename because the daemon's umask is 022, so a
       plainly created file would publish the credential readable by every account.
    2. **One refresh at a time in the process, and a waiting client adopts the file's
       token.** ``serialize_token_refresh`` carries the reasoning.
    3. **A refresh whose write fails keeps its token in the process** (marketlake #860).
       On a full root volume ``write_token`` raises ``ENOSPC``, and before this the error
       raised out of the request that refreshed. That lost one request every minute,
       usually the quote batch for every ticker, because each new client started from the
       stale file, refreshed, and failed the write again. Now the writer keeps the token
       and the request goes on, and ``_read_or_held`` hands the held token to every later
       client while it is newer than the file, retrying the write each time. So a cycle on
       a still-full disk makes no token call, and the first cycle after space is freed
       rewrites ``token.json``. When the refresh token rotated, the request still raises.
       ``_refresh_writer_type`` says why.

    The price of the third is a token that lives in the process and not on disk until the
    write lands. A restart while the disk is still full, a CI deploy included, starts from
    the stale file and costs one refresh, or a re-auth when the refresh token rotated. It
    also puts a write with an ``fsync`` under the process-wide lock on every client build
    while a token is held, so a disk that hangs rather than refuses blocks every request in
    the process each minute.

    The re-login and ``token_store pull`` keep the plain writer. Their token exists nowhere
    else, so a failed write must stay loud.

    ``enforce_enums=False`` lets ``get_quotes`` pass the field groups as plain strings
    rather than ``schwab-py`` ``Fields`` enum members, keeping this layer enum-agnostic.
    """
    from schwab.auth import client_from_access_functions  # lazy: real dep, live only

    # The refresh writer is lazy too: ``lake.reauth`` imports ``lake.config`` and so
    # ``yaml``, and the by-hand tools that import this module promise to import where
    # ``lake.config`` is absent.
    path = Path(token_path)

    def read_token() -> object:
        return _read_or_held(path)

    client = client_from_access_functions(
        api_key, app_secret, read_token, _refresh_writer_type()(path), enforce_enums=False
    )
    serialize_token_refresh(client.session, lambda: _adopt_stored_token(client, read_token))
    return client


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
        token_path: str | Path | None = None,
        *,
        api_key: str,
        app_secret: str,
        clock: Clock | None = None,
    ) -> SchwabVendor:
        """Build the real vendor from a token file.

        ``token_path`` defaults to ``lake.paths.default_token_path()``, resolved when this
        runs rather than when the module was imported.

        ``client_from_token`` builds the client, and its docstring says what it adds to
        ``schwab-py``'s own: an atomic token write, one refresh at a time across every
        client in the process, and a refreshed token kept in the process when its write
        fails. The only live callers are the daemon and the by-hand tools,
        since a real call needs a real token and real credentials. The suite drives it
        against ``httpx.MockTransport`` and a temporary token file.

        ``api_key`` and ``app_secret`` are secrets. They are passed in by the caller,
        never read from or written to the repo.

        ``clock`` turns on request timing through ``attach_timing``, read off that clock.
        The three callers that fetch a chain pass theirs: the capture cycle, the close+5
        fill and onboarding. Every other caller leaves it ``None`` and records nothing.
        """
        if token_path is None:
            token_path = default_token_path()
        client = client_from_token(token_path, api_key=api_key, app_secret=app_secret)
        if clock is not None:
            attach_timing(client, clock)
        return cls(client)

    def close(self) -> None:
        """Close the client's connections, and never raise.

        ``run_cycle_from_config`` builds a new client every cycle, and marketlake #532 lets a
        cycle open one connection per request in flight rather than one in all. authlib's
        ``OAuth2Client`` passes itself to its own base class as its session, a reference
        cycle, so dropping the client frees nothing until the cyclic collector runs, and the
        sockets stay open until then. Closing it at the end of the cycle frees them at once.
        A close that fails costs nothing the cycle captured, so it prints one line to the
        daemon's log and returns.
        """
        session = getattr(self._client, "session", None)
        if session is None:
            return
        try:
            session.close()
        except Exception as exc:  # noqa: BLE001 - a close must never cost a captured cycle
            _say_built(lambda exc=exc: f"schwab: client close failed: {type(exc).__name__}: {exc}")
