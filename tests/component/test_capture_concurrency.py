"""The concurrent capture cycle, marketlake #532.

A cycle used to fetch its requests one after another, so its wall time was their sum. On
2026-09-24 three cycles ran past their minute and lost four. Above a concurrency cap of 1
the cycle now fires every chain window and the batched quote request through one bounded
thread pool. These tests cover what that pool must and must not do:

1. It never has more requests in flight than the cap, and it does overlap them.
2. A cycle costs about its slowest request, not the sum.
3. The quote request is submitted first, then the chain windows round-robin across tickers,
   a stagger apart on the injected clock.
4. Each chain is merged in plan order whatever order its windows finished in, so the header,
   the representative error class and the contract order match a sequential fetch.
5. A task that raises costs its own window and nothing else, and a quote request that
   raises gaps every quoted ticker, as in the sequential cycle.
6. Segments are still written in roster order, chains before quotes.

The cap-of-1 path is the sequential cycle, and the rest of the suite covers it.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan
from lake.clock import SystemClock
from lake.config import GuardConstants
from lake.tickers import Roster
from lake.vendor import VendorError, VendorResponse
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
SESSION = date(2026, 8, 24)

# Three windows, so each ticker has more than one task and the submission order between
# tickers is observable.
_THREE_WINDOWS = ChainPlan(((0, 9), (10, 30), (31, None)))

# The default stagger, written as a literal so a change to the constant fails these tests
# rather than moving with them.
_STAGGER = timedelta(milliseconds=50)

_QUOTES_BODY = {
    ticker: {
        "assetMainType": "EQUITY",
        "realtime": True,
        "quote": {
            "bidPrice": 1.0,
            "askPrice": 1.1,
            "lastPrice": 1.05,
            "quoteTime": 1787000100000,
        },
    }
    for ticker in ("SPY", "QQQ")
}


def _both() -> Roster:
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": True, "chain_cadence": "1m"},
        }
    )


def _contract(ticker: str, exp_iso: str, put_call: str) -> dict:
    letter = "C" if put_call == "CALL" else "P"
    return {
        "symbol": f"{ticker:<6}{exp_iso.replace('-', '')[2:]}{letter}00650000",
        "putCall": put_call,
        "strikePrice": 650.0,
        "expirationDate": f"{exp_iso}T20:00:00.000+00:00",
        "quoteTimeInLong": 1787000099000,
        "bid": 1.0,
        "openInterest": 100,
    }


def _chain_body(ticker: str, exp_iso: str, *, underlying_price: float = 650.0) -> dict:
    """One call and one put on one expiration, the window's first day."""
    key = f"{exp_iso}:7"
    return {
        "status": "SUCCESS",
        "underlying": None,
        "underlyingPrice": underlying_price,
        "interestRate": 4.25,
        "isDelayed": False,
        "isChainTruncated": False,
        "numberOfContracts": 2,
        "callExpDateMap": {key: {"650.0": [_contract(ticker, exp_iso, "CALL")]}},
        "putExpDateMap": {key: {"650.0": [_contract(ticker, exp_iso, "PUT")]}},
    }


class _ThreadedVendor:
    """A vendor safe to call from several threads, answering any window of any chain.

    Each window answers with one expiration on its first day, so the reassembled chain
    shows which windows landed and in what order they were merged. ``answer`` overrides the
    reply for one ``(symbol, from_date)``: a ``VendorResponse`` to return or an exception to
    raise. ``delay`` holds a call for real seconds, keyed the same way, with ``default_delay``
    for the rest. It counts the calls in flight and keeps the highest count seen.
    """

    def __init__(
        self,
        *,
        answer: dict[tuple[str, date], object] | None = None,
        delay: dict[tuple[str, date], float] | None = None,
        default_delay: float = 0.0,
        quotes: object = None,
    ) -> None:
        self._answer = answer or {}
        self._delay = delay or {}
        self._default_delay = default_delay
        self._quotes = quotes
        self._lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0
        self.chain_calls: list[tuple[str, date, date | None]] = []

    def _enter(self) -> None:
        with self._lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)

    def _leave(self) -> None:
        with self._lock:
            self.in_flight -= 1

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self._enter()
        try:
            with self._lock:
                self.chain_calls.append((symbol, from_date, to_date))
            time.sleep(self._delay.get((symbol, from_date), self._default_delay))
            reply = self._answer.get((symbol, from_date))
            if isinstance(reply, BaseException):
                raise reply
            if reply is not None:
                return reply
            return VendorResponse(status=200, body=_chain_body(symbol, from_date.isoformat()))
        finally:
            self._leave()

    def get_quotes(self, symbols):
        self._enter()
        try:
            time.sleep(self._default_delay)
            if isinstance(self._quotes, Exception):
                raise self._quotes
            if isinstance(self._quotes, VendorResponse):
                return self._quotes
            return VendorResponse(status=200, body=_QUOTES_BODY)
        finally:
            self._leave()

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _run(vendor, lake_root: Path, *, clock=None, guards: GuardConstants | None = None):
    return capture.run_cycle(
        clock if clock is not None else ManualClock(start=_CLOCK_START),
        vendor,
        _both(),
        lake_root,
        pid=4242,
        guards=guards,
        plan=_THREE_WINDOWS,
    )


def _rows(result, surface: str, ticker: str) -> list[dict]:
    return journal.read_segment(result.segment(surface, ticker).path).to_pylist()


def _d(offset: int) -> date:
    return SESSION + timedelta(days=offset)


# -- 1. the cap bounds the requests in flight, and they do overlap -----------------------


def test_requests_in_flight_never_exceed_the_cap(lake_root):
    # Seven tasks, two tickers by three windows plus the quote request, each held 0.2s. At a
    # cap of 2 no more than two are ever in flight, and two are.
    vendor = _ThreadedVendor(default_delay=0.2)
    result = _run(vendor, lake_root, guards=GuardConstants(capture_max_concurrency=2))

    assert result.errors == ()
    assert vendor.max_in_flight == 2


def test_the_default_cap_fires_every_request_together(lake_root):
    # The default cap of 20 covers all seven tasks, so all seven are in flight at once. A
    # cycle that still fetched one at a time would never have more than one.
    vendor = _ThreadedVendor(default_delay=0.2)
    result = _run(vendor, lake_root)

    assert result.errors == ()
    assert vendor.max_in_flight == 7


# -- 2. a cycle costs about its slowest request ------------------------------------------


def test_a_concurrent_cycle_costs_its_slowest_request_not_the_sum(lake_root):
    # Real time, on the system clock. Seven requests of 0.3s each cost 2.1s one at a time.
    # Fired together, 50 ms apart, they cost about 0.3s plus six staggers, 0.6s. The bound
    # sits halfway between, so a loaded machine does not flake it and a sequential cycle
    # cannot pass it.
    vendor = _ThreadedVendor(default_delay=0.3)
    started = time.monotonic()
    result = _run(vendor, lake_root, clock=SystemClock())
    elapsed = time.monotonic() - started

    assert elapsed < 1.35
    for ticker in ("SPY", "QQQ"):
        row = _rows(result, CHAINS, ticker)[0]
        span = datetime.fromisoformat(row["fetch_end_ts"]) - datetime.fromisoformat(row["fetch_ts"])
        # One chain's three windows ran together, so its round trip is about one window,
        # not three.
        assert timedelta(seconds=0.3) <= span < timedelta(seconds=0.9)


# -- 3. submission order -----------------------------------------------------------------


def test_the_quote_goes_first_then_windows_round_robin_across_tickers(lake_root):
    # On the manual clock the fakes answer at once and only the stagger moves time, so each
    # unit's fetch_ts says where in the submission order its first request went:
    #   quote +0, SPY window 1 +50, QQQ window 1 +100, SPY window 2 +150, ... QQQ window 3 +300.
    # Submitting one ticker's windows before the next ticker's would put QQQ's first window
    # at +200. Submitting the quote last would put it at +300. Each unit's finish is its own
    # tasks' latest, read on the pool threads while submission is still moving the clock, so
    # it falls between the unit's first submission and the last one.
    result = _run(_ThreadedVendor(), lake_root)

    quote = _rows(result, QUOTES, "SPY")[0]
    spy = _rows(result, CHAINS, "SPY")[0]
    qqq = _rows(result, CHAINS, "QQQ")[0]
    assert quote["fetch_ts"] == _CLOCK_START.isoformat()
    assert spy["fetch_ts"] == (_CLOCK_START + _STAGGER).isoformat()
    assert qqq["fetch_ts"] == (_CLOCK_START + 2 * _STAGGER).isoformat()
    last_submission = (_CLOCK_START + 6 * _STAGGER).isoformat()
    for row in (quote, spy, qqq):
        assert row["fetch_ts"] <= row["fetch_end_ts"] <= last_submission


def test_a_stagger_of_zero_submits_without_moving_the_clock(lake_root):
    result = _run(_ThreadedVendor(), lake_root, guards=GuardConstants(capture_stagger_ms=0))

    for surface, ticker in ((QUOTES, "SPY"), (CHAINS, "SPY"), (CHAINS, "QQQ")):
        row = _rows(result, surface, ticker)[0]
        assert row["fetch_ts"] == _CLOCK_START.isoformat()
        assert row["fetch_end_ts"] == _CLOCK_START.isoformat()


class _TaskClock:
    """A manual clock that answers each pool thread with the finish its own call recorded.

    The vendor below calls ``finish_at`` at the end of every request, on the pool thread that
    made it, and ``now`` on that thread then answers that instant. Everywhere else it is the
    manual clock. So each task's finish stamp is a known value, whatever order the threads ran
    in, and a test can say exactly which instant each unit's ``fetch_end_ts`` must be.
    """

    def __init__(self, start: datetime) -> None:
        self._inner = ManualClock(start=start)
        self._local = threading.local()

    def now(self) -> datetime:
        return getattr(self._local, "at", None) or self._inner.now()

    def monotonic(self) -> float:
        return self._inner.monotonic()

    def sleep(self, seconds: float) -> None:
        self._inner.sleep(seconds)

    def finish_at(self, when: datetime) -> None:
        self._local.at = when


class _StampingVendor(_ThreadedVendor):
    """A vendor whose every request records its own finish on the task clock."""

    def __init__(self, clock: _TaskClock, finishes: dict[object, timedelta]) -> None:
        super().__init__()
        self._clock = clock
        self._finishes = finishes

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        reply = super().get_chain(symbol, from_date=from_date, to_date=to_date)
        self._clock.finish_at(_CLOCK_START + self._finishes[(symbol, from_date)])
        return reply

    def get_quotes(self, symbols):
        reply = super().get_quotes(symbols)
        self._clock.finish_at(_CLOCK_START + self._finishes["quotes"])
        return reply


def test_each_unit_is_stamped_finished_when_its_own_last_request_landed(lake_root):
    # The quote request finished 58 ms in, long before submission ended, and its round trip is
    # that alone. Each chain finished when its slowest window did. SPY's slowest is its first
    # window, not its last, so a stamp taken from the last task, or from the earliest, or from
    # the end of submission, reads a different instant.
    clock = _TaskClock(_CLOCK_START)
    ms = timedelta(milliseconds=1)
    vendor = _StampingVendor(
        clock,
        {
            "quotes": 58 * ms,
            ("SPY", _d(0)): 900 * ms,
            ("SPY", _d(10)): 300 * ms,
            ("SPY", _d(31)): 400 * ms,
            ("QQQ", _d(0)): 200 * ms,
            ("QQQ", _d(10)): 250 * ms,
            ("QQQ", _d(31)): 700 * ms,
        },
    )
    result = _run(vendor, lake_root, clock=clock, guards=GuardConstants(capture_stagger_ms=0))

    def ended(surface: str, ticker: str) -> str:
        return _rows(result, surface, ticker)[0]["fetch_end_ts"]

    assert ended(QUOTES, "SPY") == (_CLOCK_START + 58 * ms).isoformat()
    assert ended(QUOTES, "QQQ") == (_CLOCK_START + 58 * ms).isoformat()
    assert ended(CHAINS, "SPY") == (_CLOCK_START + 900 * ms).isoformat()
    assert ended(CHAINS, "QQQ") == (_CLOCK_START + 700 * ms).isoformat()


# -- 4. plan-order merge -----------------------------------------------------------------


def _fetch_spy(vendor, lake_root: Path) -> capture.ChainFetch:
    return capture.fetch_chain(
        ManualClock(start=_CLOCK_START),
        vendor,
        "SPY",
        day=SESSION,
        lake_root=lake_root,
        plan=_THREE_WINDOWS,
        guards=GuardConstants(),
    )


def test_the_merge_follows_the_plan_when_the_first_window_finishes_last(lake_root):
    # The first window is held 0.3s and carries its own underlying price. The other two
    # answer at once with a different one. The header and the expiration order still come
    # from the plan, as if the windows had been fetched in turn.
    first = VendorResponse(
        status=200, body=_chain_body("SPY", _d(0).isoformat(), underlying_price=650.0)
    )
    vendor = _ThreadedVendor(
        answer={
            ("SPY", _d(0)): first,
            ("SPY", _d(10)): VendorResponse(
                status=200, body=_chain_body("SPY", _d(10).isoformat(), underlying_price=999.0)
            ),
            ("SPY", _d(31)): VendorResponse(
                status=200, body=_chain_body("SPY", _d(31).isoformat(), underlying_price=999.0)
            ),
        },
        delay={("SPY", _d(0)): 0.3},
    )
    fetched = _fetch_spy(vendor, lake_root)

    assert fetched.error_class is None
    assert fetched.body["underlyingPrice"] == 650.0
    assert list(fetched.body["callExpDateMap"]) == [
        f"{_d(0).isoformat()}:7",
        f"{_d(10).isoformat()}:7",
        f"{_d(31).isoformat()}:7",
    ]


def test_the_representative_class_is_the_first_failed_window_in_plan_order(lake_root):
    # The first window fails slowly with a 500 and the second fails at once with a 503, so
    # the 503 finishes first. The chain's class is still the plan's first failure.
    vendor = _ThreadedVendor(
        answer={
            ("SPY", _d(0)): VendorResponse(status=500, body={}),
            ("SPY", _d(10)): VendorResponse(status=503, body={}),
        },
        delay={("SPY", _d(0)): 0.3},
    )
    fetched = _fetch_spy(vendor, lake_root)

    assert fetched.body is not None
    assert fetched.error_class == "http_500"
    assert [m.error_class for m in fetched.absent_markers] == ["http_500", "http_503"]


# -- 5. failures -------------------------------------------------------------------------


class BodyUnreadableError(Exception):
    """A failure no fetcher guard names, raised from past the vendor call."""


class _UnreadableBody:
    """A 200 reply whose body raises when read, which ``_is_too_big`` does first.

    The fetcher guards the vendor call and the merge, and this raise comes from neither, so
    it is the case where only the pool's own catch stands between one window and the cycle.
    """

    status = 200
    headers: dict[str, str] = {}

    @property
    def body(self):
        raise BodyUnreadableError("the body could not be read")


def test_a_window_task_that_raises_costs_only_its_own_window(lake_root):
    # The pool hands the raise back, and the window is recorded as failing under the
    # exception's class while the other two land. At a cap of 1 the same raise leaves the
    # cycle, as it always did, which the pull request for #532 names.
    # It is the first window that fails, so the marker also shows each result was paired
    # with its own window: the first and last windows sit at opposite ends of the plan.
    vendor = _ThreadedVendor(answer={("SPY", _d(0)): _UnreadableBody()})
    fetched = _fetch_spy(vendor, lake_root)

    assert fetched.body is not None
    assert fetched.error_class == "body_unreadable_error"
    assert [(m.window_start, m.window_end, m.error_class) for m in fetched.absent_markers] == [
        (_d(0).isoformat(), _d(9).isoformat(), "body_unreadable_error")
    ]
    assert list(fetched.body["callExpDateMap"]) == [
        f"{_d(10).isoformat()}:7",
        f"{_d(31).isoformat()}:7",
    ]


def test_an_interrupt_in_a_pool_task_is_raised_not_recorded(lake_root):
    # Only an ``Exception`` becomes a failed window. A ``SystemExit`` or an interrupt leaves
    # the fetch, the way it would have left a sequential one.
    vendor = _ThreadedVendor(answer={("SPY", _d(10)): SystemExit(3)})

    with pytest.raises(SystemExit):
        _fetch_spy(vendor, lake_root)


@pytest.mark.parametrize("cap", [1, 20])
def test_a_raised_quote_request_is_classed_by_its_own_exception(lake_root, cap):
    vendor = _ThreadedVendor(quotes=RuntimeError("the batch never came back"))
    result = _run(vendor, lake_root, guards=GuardConstants(capture_max_concurrency=cap))

    for ticker in ("SPY", "QQQ"):
        assert [r["error_class"] for r in _rows(result, QUOTES, ticker)] == ["runtime_error"]


def test_a_chain_the_row_builder_refuses_keeps_its_own_finish_stamp(lake_root, monkeypatch):
    # The builder raising turns the chain into a whole-chain gap, and that gap still carries
    # the round trip the fetch really took, the latest of its own windows' finishes.
    clock = _TaskClock(_CLOCK_START)
    ms = timedelta(milliseconds=1)
    finishes = {"quotes": 10 * ms}
    for ticker in ("SPY", "QQQ"):
        for offset, at in ((0, 100), (10, 600), (31, 200)):
            finishes[(ticker, _d(offset))] = at * ms
    vendor = _StampingVendor(clock, finishes)
    real = journal.chains_data_batch

    def refuse_spy(body, *, ticker, **kwargs):
        if ticker == "SPY":
            raise ValueError("the builder refused this chain")
        return real(body, ticker=ticker, **kwargs)

    monkeypatch.setattr(journal, "chains_data_batch", refuse_spy)
    result = _run(vendor, lake_root, clock=clock, guards=GuardConstants(capture_stagger_ms=0))

    gap = _rows(result, CHAINS, "SPY")[0]
    assert (gap["row_kind"], gap["error_class"]) == (journal.ROW_KIND_GAP, "value_error")
    assert gap["fetch_ts"] == _CLOCK_START.isoformat()
    assert gap["fetch_end_ts"] == (_CLOCK_START + 600 * ms).isoformat()


def test_a_quote_request_that_raises_gaps_every_quoted_ticker(lake_root):
    vendor = _ThreadedVendor(quotes=VendorError("quote batch timed out"))
    result = _run(vendor, lake_root)

    for ticker in ("SPY", "QQQ"):
        quote = _rows(result, QUOTES, ticker)
        assert [(r["row_kind"], r["error_class"]) for r in quote] == [
            (journal.ROW_KIND_GAP, "vendor_error")
        ]
        assert result.segment(CHAINS, ticker).row_kind == journal.ROW_KIND_DATA


def test_a_quote_request_answered_with_an_error_status_gaps_every_quoted_ticker(lake_root):
    vendor = _ThreadedVendor(quotes=VendorResponse(status=503, body={}))
    result = _run(vendor, lake_root)

    for ticker in ("SPY", "QQQ"):
        quote = _rows(result, QUOTES, ticker)
        assert [(r["row_kind"], r["error_class"]) for r in quote] == [
            (journal.ROW_KIND_GAP, "http_503")
        ]


# -- 6. write order ----------------------------------------------------------------------


def test_segments_are_written_in_roster_order_chains_before_quotes(lake_root):
    # QQQ's windows answer first, and the order the segments are written in is still the
    # sequential cycle's.
    vendor = _ThreadedVendor(delay={("SPY", _d(0)): 0.2, ("SPY", _d(10)): 0.2})
    result = _run(vendor, lake_root)

    assert [(s.surface, s.ticker) for s in result.segments] == [
        (CHAINS, "SPY"),
        (CHAINS, "QQQ"),
        (QUOTES, "SPY"),
        (QUOTES, "QQQ"),
    ]
