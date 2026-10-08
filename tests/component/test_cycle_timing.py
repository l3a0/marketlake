"""The cycle line, written by a real capture cycle over the real filesystem.

A capture cycle appends one ``kind`` ``cycle`` line to ``journal/timing/date=D.jsonl``
after its request lines, so the time after the last response can be taken apart
(marketlake #537). The request lines end at the last response. This line says when the last
unit landed, how long the cycle waited for the lake-root lock and how long it held it, and
when the cycle ended.

Most of these run on a manual clock. Each phase advances it by a different amount, from
the thread that runs the phase, so every instant the line carries is one a test can name.
The vendor advances it too, but only on the one-at-a-time path, because pool threads must
never move the clock. The lock-wait test is the exception. The wait it measures is on the
kernel's lock, which a manual clock cannot see, so it runs on the system clock against a
second process that holds the lock.

What they cover:

1. The instants, in order, on both fetch paths, and one line per cycle after its request
   lines.
2. ``fetch_end_ts`` is the latest unit's, whether its segment was written or not.
3. A lock held by another process, or by another thread of this one, shows up as the wait.
4. The line never costs a minute: a write that fails, and a load average that cannot be
   read, each cost what they cost and nothing else.
5. A cycle that raises at its manifest append writes no line of either kind.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from concurrent.futures import wait as futures_wait
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.clock import SystemClock
from lake.config import GuardConstants
from lake.lock import lake_lock
from lake.manifest import manifest_path
from lake.tickers import Roster
from lake.timing import timing_path
from lake.vendor import VendorResponse
from tests.component.test_request_timing import (
    _CLOCK_START,
    _QUOTE_BODY,
    _SNAP,
    NEAR,
    SESSION,
    TAIL,
    TWO_WINDOWS,
    _chain_body,
    _Refusing,
    _TimedVendor,
)
from tests.support.clock import ManualClock
from tests.support.timing import cycle_lines, request_lines, timing_lines

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# How long each phase takes on the manual clock. Distinct powers of two, so no sum of two
# phases can pass for a third.
_WRITE_S = 0.5
_APPEND_S = 0.25
_STAMP_S = 0.125
_LOAD_S = 0.0625
_PLAN_S = 0.03125

# The two load readings a test hands the cycle, distinct so a swapped pair shows.
_LOAD_START = (1.5, 1.25, 1.0)
_LOAD_END = (2.5, 2.25, 2.0)

_ROSTER = Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}})


def _at(seconds: float) -> str:
    """The instant ``seconds`` after the clock's start, as the line spells it."""
    return (_CLOCK_START + timedelta(seconds=seconds)).isoformat()


class _StampingVendor(_TimedVendor):
    """A timed vendor whose mint-time answer takes ``_STAMP_S``, which times the stamp."""

    def token_mint_time(self):
        self._clock.advance(_STAMP_S)
        return datetime(2026, 8, 23, tzinfo=UTC)


def _vendor(clock: ManualClock, *, chain_s: float, quote_s: float) -> _StampingVendor:
    return _StampingVendor(
        clock,
        windows={
            NEAR: (chain_s, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (chain_s, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(quote_s, VendorResponse(200, _QUOTE_BODY)),
    )


@pytest.fixture
def timed_phases(monkeypatch):
    """Make each segment write, the cycle's manifest append and each load reading take a known time.

    The append is one call for the whole cycle (marketlake #573), so it takes ``_APPEND_S``
    once however many segments it records. An empty batch reads and writes nothing, so it
    takes no time. A load reading takes time so a test can tell which side of it an instant
    was read on.

    Returns the clock the phases advance, which the test hands its cycle.
    """
    clock = ManualClock(start=_CLOCK_START)
    write = capture._CaptureCycle._write
    append = capture.append_entries
    loads = iter((_LOAD_START, _LOAD_END))

    def timed_write(self, surface, ticker, plan):
        clock.advance(_WRITE_S)
        return write(self, surface, ticker, plan)

    def timed_append(lake_root, entries, **kwargs):
        if entries:
            clock.advance(_APPEND_S)
        return append(lake_root, entries, **kwargs)

    monkeypatch.setattr(capture._CaptureCycle, "_write", timed_write)
    monkeypatch.setattr(capture, "append_entries", timed_append)

    def timed_load():
        clock.advance(_LOAD_S)
        return next(loads), None

    monkeypatch.setattr(capture, "read_load", timed_load)
    return clock


def _run(vendor, clock, lake_root: Path, *, cap: int = 1, roster: Roster = _ROSTER):
    return capture.run_cycle(
        clock,
        vendor,
        roster,
        lake_root,
        pid=4242,
        plan=TWO_WINDOWS,
        # No stagger, so the submissions move the clock by nothing a test must subtract.
        guards=GuardConstants(capture_max_concurrency=cap, capture_stagger_ms=0),
    )


def _only_cycle_line(lake_root: Path) -> dict:
    (line,) = cycle_lines(timing_path(lake_root, SESSION))
    return line


# -- 1. the instants, in order ---------------------------------------------------------------


def test_one_sequential_cycle_writes_every_instant_it_passed_through(
    lake_root, timed_phases, monkeypatch
):
    clock = timed_phases
    # The cycle starts, then reads the load. Two chain windows at 3s each and the quote
    # batch at 1s follow, so the last response lands 7s after the load. Planning the quotes
    # takes a little more, and then two segments, one chain and one quote, are written at
    # 0.5s each.
    vendor = _vendor(clock, chain_s=3.0, quote_s=1.0)
    plan_quotes = capture._CaptureCycle._plan_quotes

    def timed_plan_quotes(self):
        plans = plan_quotes(self)
        clock.advance(_PLAN_S)
        return plans

    monkeypatch.setattr(capture._CaptureCycle, "_plan_quotes", timed_plan_quotes)

    _run(vendor, clock, lake_root)

    line = _only_cycle_line(lake_root)
    assert line["v"] == 1
    assert line["kind"] == "cycle"
    assert line["snap_ts"] == _SNAP.isoformat()
    # Read before the load, so it is the instant the segment stamp uses.
    assert line["cycle_start_ts"] == _at(0)
    # The response's own end, so planning after it counts in the tail, not the fetch.
    fetched = _LOAD_S + 7
    assert line["fetch_end_ts"] == _at(fetched)
    durable = fetched + _PLAN_S + 2 * _WRITE_S
    assert line["segments_durable_ts"] == _at(durable)
    # Nothing else holds the lock, so the wait is nil, and the hold is one append for the
    # two segments (marketlake #573).
    assert line["lock_acquired_ts"] == _at(durable)
    assert line["lock_released_ts"] == _at(durable + _APPEND_S)
    # The stamp runs after the append, inside the cycle, and the closing load after it. The
    # end is read last.
    assert line["cycle_end_ts"] == _at(durable + _APPEND_S + _STAMP_S + _LOAD_S)
    assert line["loadavg_start"] == list(_LOAD_START)
    assert line["loadavg_end"] == list(_LOAD_END)
    assert line["cycle_failure"] is None


def test_the_cycle_line_comes_after_the_cycles_request_lines(lake_root, timed_phases):
    clock = timed_phases
    _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    kinds = [line["kind"] for line in timing_lines(timing_path(lake_root, SESSION))]
    assert kinds == ["request", "request", "request", "cycle"]


def test_a_concurrent_cycle_writes_its_line_with_the_landing_inside_the_fetch(
    lake_root, timed_phases
):
    # Above a cap of 1 each unit lands as its own requests finish, on the coordinating
    # thread, so only that thread moves the clock. The vendor answers at once.
    clock = timed_phases
    _run(_vendor(clock, chain_s=0.0, quote_s=0.0), clock, lake_root, cap=20)

    line = _only_cycle_line(lake_root)
    fetch_end = datetime.fromisoformat(line["fetch_end_ts"])
    durable = datetime.fromisoformat(line["segments_durable_ts"])
    # The first unit to land did so just after the load, so the last unit's response came
    # no later than one write in, and its own write followed it.
    assert fetch_end <= _CLOCK_START + timedelta(seconds=_LOAD_S + _WRITE_S)
    landed = _LOAD_S + 2 * _WRITE_S
    assert durable == _CLOCK_START + timedelta(seconds=landed)
    assert durable - fetch_end >= timedelta(seconds=_WRITE_S)
    assert line["lock_acquired_ts"] == _at(landed)
    assert line["lock_released_ts"] == _at(landed + _APPEND_S)
    assert line["cycle_end_ts"] == _at(landed + _APPEND_S + _STAMP_S + _LOAD_S)


def test_a_cycle_with_nothing_to_fetch_still_writes_its_line(lake_root, timed_phases):
    clock = timed_phases
    _run(_vendor(clock, chain_s=0.0, quote_s=0.0), clock, lake_root, roster=Roster(()))

    line = _only_cycle_line(lake_root)
    assert line["fetch_end_ts"] is None
    assert line["segments_durable_ts"] == _at(_LOAD_S)
    assert line["lock_released_ts"] == _at(_LOAD_S)


# -- 2. the latest unit, written or not ------------------------------------------------------


def test_fetch_end_is_the_latest_units_even_when_its_segment_fails_to_write(
    lake_root, timed_phases, monkeypatch
):
    clock = timed_phases
    timed_write = capture._CaptureCycle._write

    def refuse_quotes(self, surface, ticker, plan):
        if surface == QUOTES:
            raise OSError("disk refused")
        return timed_write(self, surface, ticker, plan)

    monkeypatch.setattr(capture._CaptureCycle, "_write", refuse_quotes)

    result = _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert [(e.surface, e.ticker) for e in result.errors] == [(QUOTES, "SPY")]
    # The quote batch answered last, 7s after the load. Its segment never landed, and its
    # response still ends the fetch.
    assert _only_cycle_line(lake_root)["fetch_end_ts"] == _at(_LOAD_S + 7)


class _LatestFirstClock(SystemClock):
    """A system clock whose wait hands back every task at once, the latest finisher first.

    ``Clock.wait`` returns a set, so units found done together land in no promised order.
    This fixes the order that would mislead a line keeping the last unit to land.
    """

    def wait(self, futures, until):
        futures_wait(futures)
        return sorted(futures, key=lambda f: f.result().finished_at, reverse=True)


class _SlowQuotesVendor:
    """Instant chains and a quote batch that takes real time, so the quotes answer last."""

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        return VendorResponse(200, _chain_body([(from_date + timedelta(days=4)).isoformat()]))

    def get_quotes(self, symbols):
        time.sleep(0.3)
        return VendorResponse(200, _QUOTE_BODY)

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def test_fetch_end_is_the_latest_units_whatever_order_the_units_land_in(lake_root):
    # The quote batch answers last and lands first. The chains land after it, and the line
    # still ends the fetch at the quotes.
    capture.run_cycle(
        _LatestFirstClock(),
        _SlowQuotesVendor(),
        _ROSTER,
        lake_root,
        pid=4242,
        plan=TWO_WINDOWS,
        guards=GuardConstants(capture_max_concurrency=20, capture_stagger_ms=0),
    )

    path = timing_path(lake_root, datetime.now(UTC).date())
    (quotes,) = [line for line in request_lines(path) if line["surface"] == QUOTES]
    (line,) = cycle_lines(path)
    assert line["fetch_end_ts"] >= quotes["request_end_ts"]


def test_segments_durable_comes_after_the_gap_rows_of_failed_writes(
    lake_root, timed_phases, monkeypatch
):
    """A failed write's gap row is flushed before the cycle reads the instant (marketlake #769).

    Each gap row takes 5s here, so an instant read before them would show their flushes
    as the wait for the lock.
    """
    clock = timed_phases
    timed_write = capture._CaptureCycle._write
    real_mark = capture._CaptureCycle._mark_failed_write
    marked: list[datetime] = []

    def refuse_quotes(self, surface, ticker, plan):
        if surface == QUOTES:
            raise OSError("disk refused")
        return timed_write(self, surface, ticker, plan)

    def slow_mark(self, failure):
        clock.advance(5.0)
        real_mark(self, failure)
        marked.append(clock.now())

    monkeypatch.setattr(capture._CaptureCycle, "_write", refuse_quotes)
    monkeypatch.setattr(capture._CaptureCycle, "_mark_failed_write", slow_mark)

    _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert marked
    assert _only_cycle_line(lake_root)["segments_durable_ts"] == marked[-1].isoformat()


# -- 3. a lock another holder holds ----------------------------------------------------------

_HOLD_S = 0.6

_HOLDER = """
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)
print("held", flush=True)
time.sleep(float(sys.argv[2]))
"""


class _InstantVendor:
    """A vendor that answers at once and never reads or moves a clock."""

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        return VendorResponse(200, _chain_body([(from_date + timedelta(days=4)).isoformat()]))

    def get_quotes(self, symbols):
        return VendorResponse(200, _QUOTE_BODY)

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _hold_in_a_process(lake_root: Path):
    """Hold the lake-root lock from another process, and return a wait for it to finish."""
    holder = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(manifest_path(lake_root)), str(_HOLD_S)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout is not None
    assert holder.stdout.readline().strip() == "held"
    return lambda: holder.wait(timeout=10)


def _hold_in_a_thread(lake_root: Path):
    """Hold the lock from another thread of this process, the way an earlier cycle does."""
    held = threading.Event()

    def hold():
        with lake_lock(lake_root):
            held.set()
            time.sleep(_HOLD_S)

    thread = threading.Thread(target=hold)
    thread.start()
    assert held.wait(timeout=10)
    return lambda: thread.join(timeout=10)


@pytest.mark.parametrize("hold", [_hold_in_a_process, _hold_in_a_thread], ids=["process", "thread"])
def test_a_lock_another_holder_holds_shows_as_the_wait(lake_root, hold):
    # Since cycles run on threads of their own, the likeliest holder is the previous
    # minute's cycle in this same process. ``lake_lock`` opens its own descriptor each time,
    # so a thread here waits on it exactly as another process would.
    finish = hold(lake_root)
    try:
        began = time.monotonic()
        capture.run_cycle(
            SystemClock(), _InstantVendor(), _ROSTER, lake_root, pid=4242, plan=TWO_WINDOWS
        )
        elapsed = time.monotonic() - began
    finally:
        finish()

    line = cycle_lines(timing_path(lake_root, datetime.now(UTC).date()))[-1]
    wait = datetime.fromisoformat(line["lock_acquired_ts"]) - datetime.fromisoformat(
        line["segments_durable_ts"]
    )
    # The holder was already holding when the cycle began, so the cycle waited out the rest
    # of its hold. Everything before the lock took a small part of that, and the wait is
    # the part of the whole cycle that remained.
    assert timedelta(seconds=_HOLD_S / 2) < wait <= timedelta(seconds=elapsed)
    hold = datetime.fromisoformat(line["lock_released_ts"]) - datetime.fromisoformat(
        line["lock_acquired_ts"]
    )
    assert hold < timedelta(seconds=_HOLD_S / 2)


# -- 4. the line never costs a minute --------------------------------------------------------


def test_a_cycle_line_the_file_refuses_costs_the_line_and_says_so(
    lake_root, timed_phases, monkeypatch, capsys
):
    clock = timed_phases
    readings = iter([(None, "load average not read: OSError: first"), (_LOAD_END, None)])

    def refuse(*args, **kwargs):
        raise OSError("No space left on device")

    monkeypatch.setattr(capture, "append_cycle", refuse)
    monkeypatch.setattr(capture, "read_load", lambda: next(readings))

    result = _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert {(s.surface, s.row_kind) for s in result.segments} == {
        (CHAINS, journal.ROW_KIND_DATA),
        (QUOTES, journal.ROW_KIND_DATA),
    }
    assert cycle_lines(timing_path(lake_root, SESSION)) == []
    # Each failure says so once, the refused write and the field it could not read alike.
    err = capsys.readouterr().err
    assert err.count("capture: cycle timing not written for") == 1
    assert "No space left on device" in err
    assert err.count("capture: cycle timing incomplete for") == 1


def test_a_load_average_that_cannot_be_read_costs_its_field_not_the_minute(
    lake_root, timed_phases, monkeypatch, capsys
):
    clock = timed_phases
    readings = iter([OSError("load unobtainable"), _LOAD_END])

    def getloadavg():
        reading = next(readings)
        if isinstance(reading, Exception):
            raise reading
        return reading

    # The real reader, over a system call that fails at the cycle's start and not at its end.
    monkeypatch.undo()
    monkeypatch.setattr("lake.timing.os.getloadavg", getloadavg)

    result = _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert {s.row_kind for s in result.segments} == {journal.ROW_KIND_DATA}
    line = _only_cycle_line(lake_root)
    assert line["loadavg_start"] is None
    assert line["loadavg_end"] == list(_LOAD_END)
    assert line["cycle_failure"] == "load average not read: OSError: load unobtainable"
    assert capsys.readouterr().err.count("capture: cycle timing incomplete for") == 1


def test_two_load_readings_that_fail_are_both_named_in_order(
    lake_root, timed_phases, monkeypatch, capsys
):
    clock = timed_phases
    readings = iter([OSError("first"), OSError("second")])

    def getloadavg():
        raise next(readings)

    monkeypatch.undo()
    monkeypatch.setattr("lake.timing.os.getloadavg", getloadavg)

    _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    line = _only_cycle_line(lake_root)
    assert (line["loadavg_start"], line["loadavg_end"]) == (None, None)
    named = "load average not read: OSError: first; load average not read: OSError: second"
    assert line["cycle_failure"] == named
    err = capsys.readouterr().err
    assert err.count("capture: cycle timing incomplete for") == 1
    assert named in err


def test_nothing_in_the_cycle_line_can_raise_out_of_a_cycle(lake_root, timed_phases, monkeypatch):
    # The append raises something that is not an ``OSError``, both load readings fail, and
    # stderr refuses the report. The cycle still returns with every segment landed.
    clock = timed_phases

    def broken_append(*args, **kwargs):
        raise RuntimeError("append broke")

    monkeypatch.setattr(capture, "append_cycle", broken_append)
    monkeypatch.setattr(capture, "read_load", lambda: (None, "load average not read: x"))
    monkeypatch.setattr(sys, "stderr", _Refusing())

    result = _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert {(s.surface, s.ticker) for s in result.segments} == {(CHAINS, "SPY"), (QUOTES, "SPY")}
    assert not result.errors


# -- 5. a cycle that raises ------------------------------------------------------------------


def test_a_cycle_that_raises_at_its_manifest_append_writes_no_line(
    lake_root, timed_phases, monkeypatch
):
    clock = timed_phases

    def refuse(*args, **kwargs):
        raise RuntimeError("manifest refused")

    monkeypatch.setattr(capture, "append_entries", refuse)

    with pytest.raises(RuntimeError, match="manifest refused"):
        _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert timing_lines(timing_path(lake_root, SESSION)) == []
