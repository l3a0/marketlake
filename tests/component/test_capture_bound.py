"""The capture cycle's bound on its requests, marketlake #597.

A cycle used to wait on every request it made, however long each took, so one request that
ran past the minute held the whole cycle past it and the loop lost the next minute on every
surface. A loop cycle now waits until its bound, ``cycle_deadline`` of its slot, and no
longer. These tests cover what the bound must do:

1. A request still running at the bound is abandoned under ``request_abandoned``. Its chain
   lands with its other windows and an absence marker for the cut one, its timing line runs
   from its own start to the bound, and every other unit lands whole.
2. At a concurrency cap of 1 the check before each call stops the next window from being
   sent, and a request never sent gets no timing line.
3. A split window whose second half is held fails whole. Its first requests keep their own
   lines, the held one gets the abandon line, and releasing it afterwards adds no line.
4. The option close's bound sits at close+5 less the ordinary margin, and the close+5 fill
   is not bounded at all.
5. The vendor is closed when the last abandoned request finishes, not at the bound.
6. Under the real clock the cycle returns at the bound, and a request that finishes before
   it is kept.
7. In the loop, a request held past the bound costs its own minute and the next cycle fires
   on time.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import Future
from concurrent.futures import wait as futures_wait
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, daemon, journal
from lake.chain_plan import ChainPlan
from lake.clock import SystemClock
from lake.config import GuardConstants
from lake.session import OPTION_CLOSE, SPOT_CLOSE, SessionClock
from lake.tickers import Roster
from lake.timing import timing_path
from lake.vendor import VendorResponse
from tests.component.test_capture_concurrency import (
    _THREE_WINDOWS,
    SESSION,
    _both,
    _d,
    _ThreadedVendor,
)
from tests.component.test_cycle_from_config import FIRST_MINUTE, SPY_ONLY, _rig, _wire
from tests.support.calendar import FakeCalendar, SessionTimes, et
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE
DATA = journal.ROW_KIND_DATA
GAP = journal.ROW_KIND_GAP
ABANDONED = "request_abandoned"

# The loop's 10:00 slot on the session the concurrency tests use, and its bound at the
# default of 55s, both written as literals so a change to the constant fails these tests.
SLOT = et(2026, 8, 24, 10, 0)
BOUND = et(2026, 8, 24, 10, 0, 55)

TOO_BIG = VendorResponse(status=502, body={"errorcode": "protocol.http.TooBigBody"})


class _HoldingVendor(_ThreadedVendor):
    """A vendor that holds chosen calls on one event until the test releases them.

    ``hold`` names each call to hold once, as ``(symbol, from_date, to_date)`` or
    ``"quotes"``. ``ranges`` answers a call by its whole range rather than its start, which
    is how a window and the first half of its split are told apart. It counts quote calls
    and records when it was closed, the way ``SchwabVendor.close`` frees its client.
    """

    def __init__(self, hold=(), *, ranges=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.release = threading.Event()
        self.holding = threading.Event()
        self._hold = set(hold)
        self._hold_lock = threading.Lock()
        self._ranges = ranges or {}
        self.quote_calls = 0
        self.closed = 0

    def _maybe_hold(self, key) -> None:
        with self._hold_lock:
            if key not in self._hold:
                return
            self._hold.discard(key)
        self.holding.set()
        assert self.release.wait(30), "the test never released a held call"

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self._maybe_hold((symbol, from_date, to_date))
        with self._lock:
            reply = self._ranges.get((symbol, from_date, to_date))
        if reply is not None:
            with self._lock:
                self.chain_calls.append((symbol, from_date, to_date))
            return reply
        return super().get_chain(symbol, from_date=from_date, to_date=to_date)

    def get_quotes(self, symbols):
        with self._lock:
            self.quote_calls += 1
        self._maybe_hold("quotes")
        return super().get_quotes(symbols)

    def close(self) -> None:
        self.closed += 1


@pytest.fixture
def holding():
    """Register a holding vendor, and release it at teardown whatever the test did.

    A held call never released would hang the suite at exit, because the interpreter joins
    the pool's threads.
    """
    vendors: list[_HoldingVendor] = []

    def register(vendor: _HoldingVendor) -> _HoldingVendor:
        vendors.append(vendor)
        return vendor

    yield register
    for vendor in vendors:
        vendor.release.set()


def _utc(when: datetime) -> datetime:
    return when.astimezone(UTC)


def _run(vendor, lake_root: Path, clock, **kwargs) -> capture.CycleResult:
    kwargs.setdefault("plan", _THREE_WINDOWS)
    kwargs.setdefault("slot", SLOT)
    roster = kwargs.pop("roster", None)
    return capture.run_cycle(
        clock, vendor, roster if roster is not None else _both(), lake_root, pid=4242, **kwargs
    )


def _rows(result, surface: str, ticker: str) -> list[dict]:
    return journal.read_segment(result.segment(surface, ticker).path).to_pylist()


def _markers(rows: list[dict]) -> list[tuple[str | None, str | None, str | None]]:
    return [
        (r["window_start"], r["window_end"], r["error_class"]) for r in rows if r["row_kind"] == GAP
    ]


def _expirations(rows: list[dict]) -> set[str]:
    """The dates the data rows expire on. The column carries the vendor's full stamp."""
    return {r["expiration_date"][:10] for r in rows if r["row_kind"] == DATA}


def _lines(lake_root: Path, day=SESSION) -> list[dict]:
    path = timing_path(lake_root, day)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _line(lake_root: Path, ticker: str | None, window_start: str | None) -> dict:
    (line,) = [
        line
        for line in _lines(lake_root)
        if (line["ticker"], line["window_start"]) == (ticker, window_start)
    ]
    return line


# -- 1. a request held past the bound -------------------------------------------------------


def test_a_request_held_past_the_bound_is_abandoned_and_its_chain_lands_without_it(
    lake_root, holding
):
    # SPY's first window never answers. At 10:00:55 the cycle gives it up: SPY's chain lands
    # with its other two windows and a marker for the first, QQQ's chain and both quotes
    # land whole, and the held request's line runs from its own start to the bound.
    vendor = holding(_HoldingVendor({("SPY", _d(0), _d(9))}))
    clock = ManualClock(start=_utc(SLOT) + timedelta(seconds=1))
    result = _run(vendor, lake_root, clock)

    assert clock.now() == BOUND
    assert not vendor.release.is_set()
    assert result.errors == ()
    spy = result.segment(CHAINS, "SPY")
    assert (spy.row_kind, spy.error_class) == (DATA, ABANDONED)
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(0).isoformat(), _d(9).isoformat(), ABANDONED)]
    assert _expirations(rows) == {_d(10).isoformat(), _d(31).isoformat()}
    assert {r["fetch_end_ts"] for r in rows} == {_utc(BOUND).isoformat()}
    for surface, ticker in ((CHAINS, "QQQ"), (QUOTES, "SPY"), (QUOTES, "QQQ")):
        segment = result.segment(surface, ticker)
        assert (segment.row_kind, segment.error_class) == (DATA, None)

    line = _line(lake_root, "SPY", _d(0).isoformat())
    assert (line["status"], line["error_class"]) == (None, ABANDONED)
    assert line["request_end_ts"] == _utc(BOUND).isoformat()
    start = datetime.fromisoformat(line["request_start_ts"])
    assert _utc(SLOT) + timedelta(seconds=1) <= start < _utc(BOUND)
    assert len(_lines(lake_root)) == 7


def test_a_quote_request_held_past_the_bound_gaps_every_quoted_ticker(lake_root, holding):
    vendor = holding(_HoldingVendor({"quotes"}))
    clock = ManualClock(start=_utc(SLOT) + timedelta(seconds=1))
    result = _run(vendor, lake_root, clock)

    for ticker in ("SPY", "QQQ"):
        quote = result.segment(QUOTES, ticker)
        assert (quote.row_kind, quote.error_class) == (GAP, ABANDONED)
        chain = result.segment(CHAINS, ticker)
        assert (chain.row_kind, chain.error_class) == (DATA, None)
    line = _line(lake_root, None, None)
    assert (line["surface"], line["error_class"]) == (QUOTES, ABANDONED)
    assert line["symbols"] == ["SPY", "QQQ"]
    assert line["request_end_ts"] == _utc(BOUND).isoformat()


# -- 2. a cap of 1 ------------------------------------------------------------------------


class _SlowFirstWindow(_HoldingVendor):
    """At a cap of 1 there is no pool, so a slow call moves the cycle's own clock past the bound."""

    def __init__(self, clock: ManualClock) -> None:
        super().__init__()
        self._clock = clock

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        reply = super().get_chain(symbol, from_date=from_date, to_date=to_date)
        if (symbol, from_date) == ("SPY", _d(0)):
            self._clock.set(BOUND + timedelta(seconds=3))
        return reply


def test_at_a_cap_of_one_a_request_past_the_bound_stops_every_later_one(lake_root):
    # SPY's first window answers three seconds past the bound. Nothing after it is sent:
    # not SPY's other two windows, not QQQ's chain, not the quotes. Each lands as the abandon
    # class, and only the one request that went out has a timing line.
    clock = ManualClock(start=_utc(SLOT) + timedelta(seconds=1))
    vendor = _SlowFirstWindow(clock)
    result = _run(vendor, lake_root, clock, guards=GuardConstants(capture_max_concurrency=1))

    assert vendor.chain_calls == [("SPY", _d(0), _d(9))]
    assert vendor.quote_calls == 0
    rows = _rows(result, CHAINS, "SPY")
    assert _expirations(rows) == {_d(0).isoformat()}
    assert _markers(rows) == [
        (_d(10).isoformat(), _d(30).isoformat(), ABANDONED),
        (_d(31).isoformat(), None, ABANDONED),
    ]
    for surface, ticker in ((CHAINS, "QQQ"), (QUOTES, "SPY"), (QUOTES, "QQQ")):
        segment = result.segment(surface, ticker)
        assert (segment.row_kind, segment.error_class) == (GAP, ABANDONED)
    ((surface, ticker, error_class),) = [
        (line["surface"], line["ticker"], line["error_class"]) for line in _lines(lake_root)
    ]
    assert (surface, ticker, error_class) == (CHAINS, "SPY", None)


# -- 3. a split window with a held half ---------------------------------------------------

_SPLITTABLE = ChainPlan(((0, 3), (4, None)))


def _spy_only() -> Roster:
    return Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}})


def test_a_split_window_with_a_held_half_fails_whole_and_keeps_every_line_once(lake_root, holding):
    # The first window comes back too big and splits. Its first half answers and its second
    # half is held. At the bound the window fails whole, since the first half's contracts
    # sit in the cut task's own maps. The too-big request and the first half keep the lines
    # they finished with, and the held half gets the abandon line.
    #
    # The held half is released once the fetch is cut and before the chain is handed over,
    # and its task runs to the end, filing its finished record in the shared record. The
    # line still reads as the bound left it, because the cut took a copy, and no second
    # line is written for it.
    vendor = holding(
        _HoldingVendor({("SPY", _d(2), _d(3))}, ranges={("SPY", _d(0), _d(3)): TOO_BIG})
    )
    clock = ManualClock(start=_utc(SLOT) + timedelta(seconds=1))
    abandoned: list[Future] = []

    def release_after_the_cut(futures: tuple[Future, ...]) -> None:
        abandoned.extend(futures)
        vendor.release.set()
        assert futures_wait(futures, timeout=10).not_done == set()

    result = _run(
        vendor,
        lake_root,
        clock,
        roster=_spy_only(),
        plan=_SPLITTABLE,
        on_abandoned=release_after_the_cut,
    )

    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == [(_d(0).isoformat(), _d(3).isoformat(), ABANDONED)]
    assert _expirations(rows) == {_d(4).isoformat()}

    def chain_lines() -> list[tuple]:
        return [
            (
                line["window_start"],
                line["window_end"],
                line["status"],
                line["error_class"],
                line["request_end_ts"] == _utc(BOUND).isoformat(),
            )
            for line in _lines(lake_root)
            if line["surface"] == CHAINS
        ]

    expected = [
        (_d(0).isoformat(), _d(3).isoformat(), 502, None, False),
        (_d(0).isoformat(), _d(1).isoformat(), 200, None, False),
        (_d(2).isoformat(), _d(3).isoformat(), None, ABANDONED, True),
        (_d(4).isoformat(), None, 200, None, False),
    ]
    assert chain_lines() == expected
    assert len(abandoned) == 1
    assert ("SPY", _d(2), _d(3)) in vendor.chain_calls


class _LateWakeClock(ManualClock):
    """A manual clock whose wait gives up at the bound just as the held request finishes.

    It stands for a real wait whose timeout fires a moment before the last task finishes, so
    the task is done by the time the fetch is cut.
    """

    def __init__(self, start: datetime, release: threading.Event) -> None:
        super().__init__(start)
        self._release = release

    def wait(self, futures, until):
        done = super().wait(futures, until)
        if not done and until is not None:
            self._release.set()
            futures_wait(futures, timeout=10)
        return done


def test_a_task_that_finishes_between_the_last_wait_and_the_cut_is_kept(lake_root, holding):
    # SPY's first window is still running when the wait gives up at the bound, and done by
    # the time the fetch is cut. It is collected as it stands, so the chain lands whole.
    vendor = holding(_HoldingVendor({("SPY", _d(0), _d(9))}))
    clock = _LateWakeClock(_utc(SLOT) + timedelta(seconds=1), vendor.release)
    abandoned: list[Future] = []
    result = _run(vendor, lake_root, clock, on_abandoned=abandoned.extend)

    assert clock.now() == BOUND
    rows = _rows(result, CHAINS, "SPY")
    assert _markers(rows) == []
    assert _expirations(rows) == {_d(0).isoformat(), _d(10).isoformat(), _d(31).isoformat()}
    assert _line(lake_root, "SPY", _d(0).isoformat())["error_class"] is None
    assert abandoned == []


# -- 4. where the option close's bound falls, and the fill it must not reach -------------


def test_the_option_close_bound_is_close_plus_five_less_the_ordinary_margin():
    close = _utc(et(2026, 8, 24, 16, 15))
    assert capture.cycle_deadline(close, OPTION_CLOSE, GuardConstants()) == et(
        2026, 8, 24, 16, 19, 55
    )
    widest = GuardConstants(capture_request_bound_s=59)
    assert capture.cycle_deadline(close, OPTION_CLOSE, widest) == et(2026, 8, 24, 16, 19, 59)
    assert capture.cycle_deadline(close, OPTION_CLOSE, widest) < et(2026, 8, 24, 16, 20)


def test_every_other_minute_is_bounded_inside_itself():
    guards = GuardConstants()
    assert capture.cycle_deadline(_utc(SLOT), None, guards) == BOUND
    spot = _utc(et(2026, 8, 24, 16, 0))
    assert capture.cycle_deadline(spot, SPOT_CLOSE, guards) == et(2026, 8, 24, 16, 0, 55)
    tight = GuardConstants(capture_request_bound_s=1)
    assert capture.cycle_deadline(_utc(SLOT), None, tight) == et(2026, 8, 24, 10, 0, 1)


@pytest.mark.parametrize("cap", [1, 20])
def test_the_option_close_cycle_still_fetches_past_its_minute(lake_root, cap):
    # The 16:15 cycle, running two minutes late, is still inside its bound and sends every
    # request. The same slot with no close tag is past its own bound and sends none.
    slot = et(2026, 8, 24, 16, 15)
    guards = GuardConstants(capture_max_concurrency=cap)
    tagged = _ThreadedVendor()
    result = _run(
        tagged,
        lake_root,
        ManualClock(start=_utc(slot) + timedelta(minutes=2)),
        slot=slot,
        close_tag=OPTION_CLOSE,
        guards=guards,
    )
    assert len(tagged.chain_calls) == 6
    assert all(seg.error_class is None for seg in result.segments)

    untagged = _ThreadedVendor()
    result = _run(
        untagged,
        lake_root,
        # A second later, so the two cycles' segments are named apart.
        ManualClock(start=_utc(slot) + timedelta(minutes=2, seconds=1)),
        slot=slot,
        guards=guards,
    )
    assert untagged.chain_calls == []
    assert {seg.error_class for seg in result.segments} == {ABANDONED}


@pytest.mark.parametrize("cap", [1, 20])
def test_a_close_fill_past_the_option_close_bound_still_fetches_every_window(lake_root, cap):
    # The guard fills from the 16:20 tick, after the option close's 16:19:55 bound. The fill
    # is not bounded, so every window is fetched and the close lands.
    vendor = _ThreadedVendor()
    result = capture.fill_option_close(
        ManualClock(start=_utc(et(2026, 8, 24, 16, 20, 30))),
        vendor,
        "SPY",
        slot=et(2026, 8, 24, 16, 15),
        lake_root=lake_root,
        guards=GuardConstants(capture_max_concurrency=cap),
        plan=_THREE_WINDOWS,
        pid=4242,
    )

    assert sorted(call[1] for call in vendor.chain_calls) == [_d(0), _d(10), _d(31)]
    assert result.landed
    assert result.error_class is None


# -- 5. the vendor closes when its last request finishes ----------------------------------


def test_the_vendor_closes_once_its_abandoned_request_finishes_not_at_the_bound(
    tmp_path, monkeypatch, holding
):
    # The cycle returns at the bound with SPY's one window still held. The client is still
    # open then, since a held request may be refreshing the token. It closes once the held
    # request finishes.
    rig = _rig(tmp_path, SPY_ONLY)
    day = FIRST_MINUTE.date()
    vendor = holding(_HoldingVendor({("SPY", day, None)}))
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
    assert vendor.closed == 0
    vendor.release.set()
    _wait_for(lambda: vendor.closed == 1)
    assert vendor.closed == 1


def _wait_for(condition, seconds: float = 10.0) -> None:
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.01)


def test_a_cycle_that_raises_before_its_bound_still_leaves_the_vendor_open(
    tmp_path, monkeypatch, holding
):
    # The quote batch lands first and its planning raises, which leaves the cycle before the
    # bound with SPY's window still held. The pool is not waited on, so the vendor must stay
    # open until that request finishes, as it does when the bound cuts it.
    rig = _rig(tmp_path, SPY_ONLY)
    vendor = holding(_HoldingVendor({("SPY", FIRST_MINUTE.date(), None)}))
    _wire(monkeypatch, rig, lambda path: vendor)

    def broken(self, fetched):
        # Only once SPY's request is in flight, so the raise leaves a request running
        # rather than one still queued, which the pool's shutdown would cancel unsent.
        assert vendor.holding.wait(10)
        raise RuntimeError("planning the quotes broke")

    monkeypatch.setattr(capture._CaptureCycle, "_plan_quote_fetch", broken)
    with pytest.raises(RuntimeError, match="planning the quotes broke"):
        capture.run_cycle_from_config(
            clock=ManualClock(start=_utc(FIRST_MINUTE) + timedelta(seconds=1)),
            config_path=rig.config,
            tickers_path=rig.tickers,
            token_path=rig.token,
            pid=4242,
            slot=FIRST_MINUTE,
        )

    assert vendor.closed == 0
    vendor.release.set()
    _wait_for(lambda: vendor.closed == 1)
    assert vendor.closed == 1


# -- 6. the real clock --------------------------------------------------------------------


def test_under_the_real_clock_the_cycle_returns_at_its_bound_and_keeps_what_finished(
    lake_root, holding
):
    # The slot is set so the bound falls 1.5s after the cycle starts. SPY's first window takes
    # 0.8s, inside the bound, and it is kept. QQQ's last window never answers, and the cycle
    # returns at the bound rather than waiting on it.
    clock = SystemClock()
    slot = clock.now() - timedelta(seconds=53.5)
    day = slot.astimezone(UTC).date()
    vendor = holding(_HoldingVendor({("QQQ", day + timedelta(days=31), None)}, default_delay=0.05))
    vendor._delay = {("SPY", day): 0.8}
    began = time.monotonic()
    result = _run(vendor, lake_root, clock, slot=slot)
    elapsed = time.monotonic() - began

    assert 1.3 < elapsed < 3.0
    spy = _rows(result, CHAINS, "SPY")
    assert _markers(spy) == []
    assert _expirations(spy) == {(day + timedelta(days=n)).isoformat() for n in (0, 10, 31)}
    qqq = _rows(result, CHAINS, "QQQ")
    assert _markers(qqq) == [((day + timedelta(days=31)).isoformat(), None, ABANDONED)]


# -- 7. the loop --------------------------------------------------------------------------


def test_a_held_request_costs_its_own_minute_and_the_next_cycle_fires_on_time(lake_root, holding):
    # The 10:00 cycle's SPY first window is held and never answers while the loop runs. The
    # 10:00 cycle gives it up at 10:00:55, the loop fires the 10:01 cycle at 10:01, and no
    # minute is reported skipped. Without the bound the 10:00 cycle never returns while the
    # request is held, so the loop runs on a thread the test can stop waiting for.
    calendar = FakeCalendar(
        {SESSION: SessionTimes(open=et(2026, 8, 24, 9, 30), close=et(2026, 8, 24, 16, 0))}
    )
    clock = ManualClock(start=_utc(et(2026, 8, 24, 9, 59, 30)))
    session_clock = SessionClock(clock, calendar)
    vendor = holding(_HoldingVendor({("SPY", _d(0), _d(9))}))
    fired: list[tuple[datetime, datetime]] = []
    skipped: list[list[datetime]] = []

    def runner(*, slot, close_tag, session_phase):
        fired.append((slot, clock.now()))
        return _run(vendor, lake_root, clock, slot=slot, close_tag=close_tag)

    hooks = daemon.DaemonHooks(
        close_tag_for=session_clock.close_tag_at,
        on_skipped=lambda slots: skipped.append(list(slots)),
    )
    stop = _utc(et(2026, 8, 24, 10, 1))
    loop = threading.Thread(
        target=daemon.run_loop,
        args=(session_clock, runner),
        kwargs={
            "clock": clock,
            "hooks": hooks,
            "should_continue": lambda: clock.now() < stop,
        },
        daemon=True,
    )
    loop.start()
    loop.join(20)

    assert not loop.is_alive(), "the 10:00 cycle waited on its held request past the bound"
    assert fired == [
        (et(2026, 8, 24, 10, 0), et(2026, 8, 24, 10, 0)),
        (et(2026, 8, 24, 10, 1), et(2026, 8, 24, 10, 1)),
    ]
    assert skipped == []


class _Interrupted(BaseException):
    """An interrupt of the test's own, so pytest's own handling is not involved."""


class _InterruptingClock(ManualClock):
    """A manual clock whose wait is interrupted once the held request is in flight."""

    def __init__(self, start: datetime, vendor: _HoldingVendor) -> None:
        super().__init__(start)
        self._vendor = vendor

    def wait(self, futures, until):
        assert self._vendor.holding.wait(10)
        raise _Interrupted


def test_an_interrupted_fetch_with_no_bound_waits_for_its_requests(lake_root, holding):
    # The close+5 fill's fetch has no bound and no ``on_abandoned``, and its caller closes
    # the vendor in a plain ``finally``. So an interrupt must not leave the fetch while a
    # request is still in flight, or the close lands under it. The held request is released
    # half a second later, and the fetch leaves only after it has answered.
    vendor = holding(_HoldingVendor({("SPY", _d(10), _d(30))}))
    releaser = threading.Timer(0.5, vendor.release.set)
    releaser.start()
    try:
        with pytest.raises(_Interrupted):
            capture.fetch_chain(
                _InterruptingClock(_utc(SLOT), vendor),
                vendor,
                "SPY",
                day=SESSION,
                lake_root=lake_root,
                plan=_THREE_WINDOWS,
                guards=GuardConstants(),
                deadline=None,
            )
        assert ("SPY", _d(10), _d(30)) in vendor.chain_calls
    finally:
        releaser.join()
