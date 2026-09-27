"""One retry for a transient request failure, marketlake #558.

The design promises "one immediate retry if the cycle has time left" for a timeout, a 5xx
or a reset. Every transient failure on record was a lost window on its first attempt,
because nothing sent it again. These tests cover what the retry must do:

1. Which failures are sent again. The httpx timeout, network and remote-protocol errors,
   read through a wrapper's cause, and a 5xx that is not the ``TooBigBody`` 502. Nothing
   else, so a 429, an auth failure, a body of the wrong shape and a builtin ``TimeoutError``
   are each sent once.
2. What a retried window records. A retry that succeeds lands the window whole, both
   attempts fail under the retry's class, and the retry's reply takes every branch a first
   reply takes.
3. The bound. At a cap of 1 a first attempt that ends past the bound is not sent again and
   keeps its own class. Above it, a retry still running at the bound fails the window under
   ``request_abandoned``, and the first attempt keeps its line.
4. The quote batch, at a cap of 1 and above it, including the stamps its rows carry.
5. The real vendor's error, raw and wrapped, and the vendor staying open under a retry the
   bound abandoned.
"""

from __future__ import annotations

import threading
from collections import Counter
from concurrent.futures import Future
from concurrent.futures import wait as futures_wait
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan
from lake.config import GuardConstants
from lake.schwab import SchwabVendor, VendorAuthError, VendorBodyError, is_transient_failure
from lake.vendor import VendorError, VendorResponse
from tests.component.test_capture_bound import (
    _SPLITTABLE,
    ABANDONED,
    BOUND,
    SLOT,
    TOO_BIG,
    _expirations,
    _HoldingVendor,
    _lines,
    _markers,
    _rows,
    _run,
    _spy_only,
    _utc,
    _wait_for,
)
from tests.component.test_capture_bound import holding as _holding
from tests.component.test_capture_concurrency import _chain_body, _d
from tests.component.test_cycle_from_config import FIRST_MINUTE, SPY_ONLY, _rig, _wire
from tests.support.clock import ManualClock
from tests.support.schwab import FakeResponse, FakeSchwabClient

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE
DATA = journal.ROW_KIND_DATA
GAP = journal.ROW_KIND_GAP

START = _utc(SLOT) + timedelta(seconds=1)

# The bound tests' fixture, which releases every held call at teardown.
holding = _holding

# An answer that holds its call on the vendor's release event, then answers as the default.
HOLD = object()


class _Late:
    """An answer that moves the clock first, as a slow request would, then gives ``then``.

    ``to`` jumps the clock to an instant and ``by`` moves it forward by seconds.
    """

    def __init__(self, clock: ManualClock, then=None, *, to=None, by=None) -> None:
        self.clock, self.then, self.to, self.by = clock, then, to, by

    def __call__(self):
        if self.to is not None:
            self.clock.set(self.to)
        if self.by is not None:
            self.clock.advance(self.by)
        return self.then


class _ScriptedVendor(_HoldingVendor):
    """A vendor that answers chosen calls from a script, one answer per call, in order.

    ``script`` keys a chain call by ``(symbol, from_date)`` and the quote batch by
    ``"quotes"``. An answer is a ``VendorResponse`` to return, an exception to raise,
    ``HOLD`` to wait for the release, or a ``_Late``. Once a key's answers run out, or for a
    key with none, the call answers as the threaded vendor does, a window's one expiration
    or the quote body. ``calls`` counts every call by key.
    """

    def __init__(self, script=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._script = {key: list(answers) for key, answers in (script or {}).items()}
        self.calls: Counter = Counter()

    def _next(self, key):
        with self._hold_lock:
            self.calls[key] += 1
            answers = self._script.get(key)
            answer = answers.pop(0) if answers else None
        if isinstance(answer, _Late):
            answer = answer()
        if answer is HOLD:
            self.holding.set()
            assert self.release.wait(30), "the test never released a held call"
            return None
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        answer = self._next((symbol, from_date))
        if answer is not None:
            with self._lock:
                self.chain_calls.append((symbol, from_date, to_date))
            return answer
        return super().get_chain(symbol, from_date=from_date, to_date=to_date)

    def get_quotes(self, symbols):
        with self._lock:
            self.quote_calls += 1
        answer = self._next("quotes")
        if answer is not None:
            return answer
        return super(_HoldingVendor, self).get_quotes(symbols)


def _cap(cap: int, **kwargs) -> GuardConstants:
    return GuardConstants(capture_max_concurrency=cap, **kwargs)


def _window_lines(lake_root, ticker: str, window_start) -> list[tuple]:
    """Each request line for one window, as ``(window_end, status, error_class)``."""
    return [
        (line["window_end"], line["status"], line["error_class"])
        for line in _lines(lake_root)
        if (line["ticker"], line["window_start"]) == (ticker, window_start)
    ]


def _quote_lines(lake_root) -> list[dict]:
    return [line for line in _lines(lake_root) if line["surface"] == QUOTES]


def _wrapped_timeout() -> VendorError:
    """A ``VendorError`` raised ``from`` an httpx timeout, the wrap marketlake #450 plans."""
    try:
        raise httpx.ReadTimeout("read")
    except httpx.ReadTimeout as exc:
        try:
            raise VendorError("the transport failed") from exc
        except VendorError as wrapped:
            return wrapped


def _raised_while_handling_a_timeout() -> VendorError:
    """A ``VendorError`` raised while an httpx timeout was handled, not ``from`` it."""
    try:
        raise httpx.ReadTimeout("read")
    except httpx.ReadTimeout:
        try:
            raise VendorError("an unrelated failure")  # noqa: B904 - the context is the case
        except VendorError as unrelated:
            return unrelated


def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.schwabapi.com/v1/oauth/token")
    return httpx.HTTPStatusError(
        "refresh refused", request=request, response=httpx.Response(status, request=request)
    )


# -- 1. which failures are sent again -----------------------------------------------------

RETRIED = {
    "read timeout": httpx.ReadTimeout("read"),
    "connect timeout": httpx.ConnectTimeout("connect"),
    "write timeout": httpx.WriteTimeout("write"),
    "pool timeout": httpx.PoolTimeout("pool"),
    "refused connection": httpx.ConnectError("refused"),
    "reset read": httpx.ReadError("reset"),
    "failed write": httpx.WriteError("broken pipe"),
    "cut transfer": httpx.RemoteProtocolError("peer closed connection"),
    "wrapped timeout": _wrapped_timeout(),
    "500": VendorResponse(status=500, body={}),
    "502 without TooBigBody": VendorResponse(status=502, body={"fault": "gateway"}),
    "503": VendorResponse(status=503, body={}),
    "599": VendorResponse(status=599, body={}),
}


@pytest.mark.parametrize("cap", [1, 20])
@pytest.mark.parametrize("first", RETRIED.values(), ids=RETRIED.keys())
def test_a_transient_failure_is_sent_once_more_and_the_window_lands_whole(lake_root, first, cap):
    vendor = _ScriptedVendor({("SPY", _d(0)): [first]})
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.calls[("SPY", _d(0))] == 2
    spy = result.segment(CHAINS, "SPY")
    assert (spy.row_kind, spy.error_class) == (DATA, None)
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == []
    assert _expirations(rows) == {_d(0).isoformat(), _d(10).isoformat(), _d(31).isoformat()}
    # Two attempts, two lines under one window's coordinates, the failure's first.
    status = first.status if isinstance(first, VendorResponse) else None
    error_class = (
        f"http_{first.status}" if isinstance(first, VendorResponse) else capture._error_class(first)
    )
    assert _window_lines(lake_root, "SPY", _d(0).isoformat()) == [
        (_d(9).isoformat(), status, error_class),
        (_d(9).isoformat(), 200, None),
    ]
    assert len(_lines(lake_root)) == 8


NOT_RETRIED = {
    "429": (VendorResponse(status=429, body={}), "http_429"),
    "401": (VendorResponse(status=401, body={}), "http_401"),
    "403": (VendorResponse(status=403, body={}), "http_403"),
    "499": (VendorResponse(status=499, body={}), "http_499"),
    "TooBigBody 502": (TOO_BIG, capture.CHAIN_CHUNK_FAILED),
    "auth failure": (VendorAuthError("refresh token expired"), "vendor_auth_error"),
    "body not an object": (VendorBodyError("200 with a list body"), "vendor_body_error"),
    "builtin TimeoutError": (TimeoutError("read"), "timeout_error"),
    "refresh answered 503": (_status_error(503), "h_t_t_p_status_error"),
    "cut transfer on our side": (httpx.LocalProtocolError("bad header"), "local_protocol_error"),
    "timeout only as context": (_raised_while_handling_a_timeout(), "vendor_error"),
}


@pytest.mark.parametrize("cap", [1, 20])
@pytest.mark.parametrize(("first", "error_class"), NOT_RETRIED.values(), ids=NOT_RETRIED.keys())
def test_every_other_failure_is_sent_once(lake_root, first, error_class, cap):
    # The open tail, so a too-big reply cannot split and is given up at once.
    vendor = _ScriptedVendor({("SPY", _d(31)): [first]})
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.calls[("SPY", _d(31))] == 1
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(31).isoformat(), None, error_class)]
    assert len(_window_lines(lake_root, "SPY", _d(31).isoformat())) == 1


def test_the_predicate_reads_the_cause_chain_and_stops_at_a_loop():
    assert is_transient_failure(httpx.ReadTimeout("read"))
    assert is_transient_failure(_wrapped_timeout())
    assert not is_transient_failure(_raised_while_handling_a_timeout())
    assert not is_transient_failure(TimeoutError("read"))
    looped = VendorError("its own cause")
    looped.__cause__ = looped
    assert not is_transient_failure(looped)


# -- 2. what a retried window records -----------------------------------------------------


@pytest.mark.parametrize("cap", [1, 20])
def test_a_window_that_fails_twice_records_the_retrys_class(lake_root, cap):
    vendor = _ScriptedVendor(
        {("SPY", _d(0)): [httpx.ReadTimeout("read"), VendorResponse(status=503, body={})]}
    )
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.calls[("SPY", _d(0))] == 2
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(0).isoformat(), _d(9).isoformat(), "http_503")]
    assert _window_lines(lake_root, "SPY", _d(0).isoformat()) == [
        (_d(9).isoformat(), None, "read_timeout"),
        (_d(9).isoformat(), 503, "http_503"),
    ]


@pytest.mark.parametrize("cap", [1, 20])
def test_a_retry_is_never_itself_retried(lake_root, cap):
    timeout = httpx.ReadTimeout("read")
    vendor = _ScriptedVendor({("SPY", _d(0)): [timeout, timeout, timeout]})
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.calls[("SPY", _d(0))] == 2
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(0).isoformat(), _d(9).isoformat(), "read_timeout")]


@pytest.mark.parametrize("cap", [1, 20])
def test_a_retrys_reply_takes_every_branch_a_first_reply_takes(lake_root, cap):
    # SPY's first window answers 503 and then too big, so the retry splits and both halves
    # land. QQQ's answers 503 and then 429, which is recorded as it is, with no third call.
    vendor = _ScriptedVendor(
        {
            ("SPY", _d(0)): [VendorResponse(status=503, body={}), TOO_BIG],
            ("QQQ", _d(0)): [
                VendorResponse(status=503, body={}),
                VendorResponse(status=429, body={}),
            ],
        }
    )
    result = _run(vendor, lake_root, ManualClock(start=START), plan=_SPLITTABLE, guards=_cap(cap))

    spy = _rows(result, CHAINS, "SPY")
    assert _markers(spy) == []
    assert _expirations(spy) == {_d(0).isoformat(), _d(2).isoformat(), _d(4).isoformat()}
    assert [
        (line["window_start"], line["window_end"], line["status"], line["error_class"])
        for line in _lines(lake_root)
        if line["ticker"] == "SPY"
    ] == [
        (_d(0).isoformat(), _d(3).isoformat(), 503, "http_503"),
        (_d(0).isoformat(), _d(3).isoformat(), 502, None),
        (_d(0).isoformat(), _d(1).isoformat(), 200, None),
        (_d(2).isoformat(), _d(3).isoformat(), 200, None),
        (_d(4).isoformat(), None, 200, None),
    ]
    qqq = _rows(result, CHAINS, "QQQ")
    assert _markers(qqq) == [(_d(0).isoformat(), _d(3).isoformat(), "http_429")]
    assert vendor.calls[("QQQ", _d(0))] == 2


@pytest.mark.parametrize("cap", [1, 20])
def test_a_split_half_that_times_out_is_retried_on_its_own(lake_root, cap):
    vendor = _ScriptedVendor(
        {("SPY", _d(2)): [httpx.ReadTimeout("read")]},
        ranges={("SPY", _d(0), _d(3)): TOO_BIG},
    )
    result = _run(
        vendor,
        lake_root,
        ManualClock(start=START),
        roster=_spy_only(),
        plan=_SPLITTABLE,
        guards=_cap(cap),
    )

    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == []
    assert _expirations(rows) == {_d(0).isoformat(), _d(2).isoformat(), _d(4).isoformat()}
    assert _window_lines(lake_root, "SPY", _d(2).isoformat()) == [
        (_d(3).isoformat(), None, "read_timeout"),
        (_d(3).isoformat(), 200, None),
    ]
    assert vendor.calls[("SPY", _d(0))] == 2  # the too-big window and its first half


# -- 3. the bound --------------------------------------------------------------------------


def test_at_a_cap_of_one_a_timeout_past_the_bound_is_not_sent_again(lake_root):
    # SPY's first window times out three seconds past the bound. The retry is refused, so
    # the window keeps the class that lost it rather than the abandon class, and it is the
    # only request with a line.
    clock = ManualClock(start=START)
    late_timeout = _Late(clock, httpx.ReadTimeout("read"), to=BOUND + timedelta(seconds=3))
    vendor = _ScriptedVendor({("SPY", _d(0)): [late_timeout]})
    result = _run(vendor, lake_root, clock, guards=_cap(1))

    assert vendor.calls[("SPY", _d(0))] == 1
    spy = result.segment(CHAINS, "SPY")
    assert (spy.row_kind, spy.error_class) == (GAP, "read_timeout")
    assert [
        (line["ticker"], line["status"], line["error_class"]) for line in _lines(lake_root)
    ] == [("SPY", None, "read_timeout")]


def test_a_retry_held_past_the_bound_fails_its_window_and_the_first_attempt_keeps_its_line(
    lake_root, holding
):
    vendor = holding(_ScriptedVendor({("SPY", _d(0)): [httpx.ReadTimeout("read"), HOLD]}))
    abandoned: list[Future] = []
    result = _run(vendor, lake_root, ManualClock(start=START), on_abandoned=abandoned.extend)

    assert vendor.holding.is_set()
    spy = result.segment(CHAINS, "SPY")
    assert (spy.row_kind, spy.error_class) == (DATA, ABANDONED)
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(0).isoformat(), _d(9).isoformat(), ABANDONED)]
    first, retry = [
        line
        for line in _lines(lake_root)
        if (line["ticker"], line["window_start"])
        == (
            "SPY",
            _d(0).isoformat(),
        )
    ]
    assert (first["status"], first["error_class"]) == (None, "read_timeout")
    assert (retry["status"], retry["error_class"]) == (None, ABANDONED)
    assert retry["request_end_ts"] == _utc(BOUND).isoformat()
    assert first["request_end_ts"] <= retry["request_start_ts"]

    # The retry ran inside its window's own task, which is the one task handed over.
    assert len(abandoned) == 1
    count = len(_lines(lake_root))
    vendor.release.set()
    assert futures_wait(abandoned, timeout=10).not_done == set()
    assert len(_lines(lake_root)) == count


def test_the_vendor_stays_open_under_a_retry_the_bound_abandoned(tmp_path, monkeypatch, holding):
    rig = _rig(tmp_path, SPY_ONLY)
    day = FIRST_MINUTE.date()
    vendor = holding(_ScriptedVendor({("SPY", day): [httpx.ReadTimeout("read"), HOLD]}))
    _wire(monkeypatch, rig, lambda path: vendor)

    result = capture.run_cycle_from_config(
        clock=ManualClock(start=_utc(FIRST_MINUTE) + timedelta(seconds=1)),
        config_path=rig.config,
        tickers_path=rig.tickers,
        token_path=rig.token,
        pid=4242,
        slot=FIRST_MINUTE,
    )

    chain = result.segment(CHAINS, "SPY")
    assert (chain.row_kind, chain.error_class) == (GAP, ABANDONED)
    assert vendor.holding.is_set()
    assert vendor.closed == 0
    vendor.release.set()
    _wait_for(lambda: vendor.closed == 1)
    assert vendor.closed == 1


# -- 4. the quote batch --------------------------------------------------------------------


@pytest.mark.parametrize("cap", [1, 20])
@pytest.mark.parametrize(
    "first",
    [httpx.ReadTimeout("read"), VendorResponse(status=503, body={})],
    ids=["read timeout", "503"],
)
def test_a_quote_batch_that_fails_transiently_is_sent_once_more_and_lands(lake_root, first, cap):
    vendor = _ScriptedVendor({"quotes": [first]})
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.quote_calls == 2
    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert (quote.row_kind, quote.error_class) == (DATA, None)
    lines = _quote_lines(lake_root)
    status = first.status if isinstance(first, VendorResponse) else None
    error_class = "http_503" if isinstance(first, VendorResponse) else "read_timeout"
    assert [(line["status"], line["error_class"]) for line in lines] == [
        (status, error_class),
        (200, None),
    ]
    assert {tuple(line["symbols"]) for line in lines} == {("SPY", "QQQ")}


@pytest.mark.parametrize("cap", [1, 20])
def test_a_quote_batch_that_fails_twice_gaps_under_the_retrys_class(lake_root, cap):
    vendor = _ScriptedVendor(
        {"quotes": [httpx.ConnectError("refused"), VendorResponse(status=502, body={})]}
    )
    result = _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.quote_calls == 2
    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert (quote.row_kind, quote.error_class) == (GAP, "http_502")
    assert [(line["status"], line["error_class"]) for line in _quote_lines(lake_root)] == [
        (None, "connect_error"),
        (502, "http_502"),
    ]


@pytest.mark.parametrize("cap", [1, 20])
def test_a_quote_batch_answered_429_is_sent_once(lake_root, cap):
    vendor = _ScriptedVendor({"quotes": [VendorResponse(status=429, body={})]})
    _run(vendor, lake_root, ManualClock(start=START), guards=_cap(cap))

    assert vendor.quote_calls == 1
    assert len(_quote_lines(lake_root)) == 1


def test_at_a_cap_of_one_a_quote_failure_past_the_bound_is_not_sent_again(lake_root):
    clock = ManualClock(start=START)
    late_503 = _Late(clock, VendorResponse(status=503, body={}), to=BOUND + timedelta(seconds=2))
    vendor = _ScriptedVendor({"quotes": [late_503]})
    result = _run(vendor, lake_root, clock, guards=_cap(1))

    assert vendor.quote_calls == 1
    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert (quote.row_kind, quote.error_class) == (GAP, "http_503")
    assert [(line["status"], line["error_class"]) for line in _quote_lines(lake_root)] == [
        (503, "http_503")
    ]


def test_a_quote_retry_held_past_the_bound_writes_both_lines(lake_root, holding):
    vendor = holding(_ScriptedVendor({"quotes": [VendorResponse(status=503, body={}), HOLD]}))
    result = _run(vendor, lake_root, ManualClock(start=START))

    assert vendor.holding.is_set()
    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert (quote.row_kind, quote.error_class) == (GAP, ABANDONED)
    first, retry = _quote_lines(lake_root)
    assert (first["status"], first["error_class"]) == (503, "http_503")
    assert (retry["status"], retry["error_class"]) == (None, ABANDONED)
    assert retry["request_end_ts"] == _utc(BOUND).isoformat()
    assert first["request_end_ts"] <= retry["request_start_ts"]


@pytest.mark.parametrize("cap", [1, 20])
def test_a_retried_quote_batchs_rows_span_both_attempts(lake_root, cap):
    # The first attempt takes 30s and times out, and the retry takes one more second. The
    # rows' pair runs from before the first attempt to the end of the retry, and each
    # attempt's own pair is on its line. With no stagger nothing else moves the clock.
    clock = ManualClock(start=START)
    vendor = _ScriptedVendor(
        {
            "quotes": [
                _Late(clock, httpx.ReadTimeout("read"), by=30),
                _Late(clock, None, by=1),
            ]
        }
    )
    result = _run(vendor, lake_root, clock, guards=_cap(cap, capture_stagger_ms=0))

    for ticker in ("SPY", "QQQ"):
        rows = _rows(result, QUOTES, ticker)
        assert {(r["fetch_ts"], r["fetch_end_ts"]) for r in rows} == {
            (START.isoformat(), (START + timedelta(seconds=31)).isoformat())
        }
    first, retry = _quote_lines(lake_root)
    assert (first["request_start_ts"], first["request_end_ts"]) == (
        START.isoformat(),
        (START + timedelta(seconds=30)).isoformat(),
    )
    assert (retry["request_start_ts"], retry["request_end_ts"]) == (
        (START + timedelta(seconds=30)).isoformat(),
        (START + timedelta(seconds=31)).isoformat(),
    )


# -- 5. the real vendor's errors, raw and wrapped ------------------------------------------


class _FlakyClient(FakeSchwabClient):
    """A fake ``schwab-py`` client whose first chain request raises ``first``."""

    def __init__(self, first: BaseException, **kwargs) -> None:
        super().__init__(**kwargs)
        self._first = [first]
        self._lock = threading.Lock()

    def get_option_chain(self, symbol, **kwargs):
        with self._lock:
            first = self._first.pop() if self._first else None
        if first is not None:
            super().get_option_chain(symbol, **kwargs)
            raise first
        return super().get_option_chain(symbol, **kwargs)


@pytest.mark.parametrize(
    ("first", "error_class"),
    [(httpx.ReadTimeout("read"), "read_timeout"), (_wrapped_timeout(), "vendor_error")],
    ids=["raw", "wrapped"],
)
def test_the_real_vendors_timeout_is_retried_raw_and_wrapped(lake_root, first, error_class):
    day = datetime(2026, 8, 24, tzinfo=UTC).date()
    client = _FlakyClient(
        first, chains={"SPY": FakeResponse(200, body=_chain_body("SPY", day.isoformat()))}
    )

    fetched = capture.fetch_chain(
        ManualClock(start=START),
        SchwabVendor(client),
        "SPY",
        day=day,
        lake_root=lake_root,
        plan=ChainPlan(((0, None),)),
        guards=GuardConstants(),
        deadline=None,
    )

    assert client.chain_calls == ["SPY", "SPY"]
    assert fetched.body is not None
    assert fetched.error_class is None
    assert [(r.status, r.error_class) for r in fetched.requests] == [
        (None, error_class),
        (200, None),
    ]
