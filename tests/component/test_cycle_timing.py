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
3. A lock held by another process shows up as the wait.
4. The line never costs a minute: a write that fails, and a load average that cannot be
   read, each cost what they cost and nothing else.
5. A cycle that raises at its manifest append writes no line of either kind.
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.clock import SystemClock
from lake.config import GuardConstants
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
    _TimedVendor,
)
from tests.support.clock import ManualClock
from tests.support.timing import cycle_lines, timing_lines

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# How long each phase takes on the manual clock. Distinct powers of two, so no sum of two
# phases can pass for a third.
_WRITE_S = 0.5
_APPEND_S = 0.25
_STAMP_S = 0.125

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
    """Make each segment write, each manifest entry and each load reading take a known time.

    Returns the clock the phases advance, which the test hands its cycle.
    """
    clock = ManualClock(start=_CLOCK_START)
    write = capture._CaptureCycle._write
    record = capture.record_partition
    loads = iter((_LOAD_START, _LOAD_END))

    def timed_write(self, surface, ticker, plan):
        clock.advance(_WRITE_S)
        return write(self, surface, ticker, plan)

    def timed_record(*args, **kwargs):
        clock.advance(_APPEND_S)
        return record(*args, **kwargs)

    monkeypatch.setattr(capture._CaptureCycle, "_write", timed_write)
    monkeypatch.setattr(capture, "record_partition", timed_record)
    monkeypatch.setattr(capture, "read_load", lambda: (next(loads), None))
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


def test_one_sequential_cycle_writes_every_instant_it_passed_through(lake_root, timed_phases):
    clock = timed_phases
    # Two chain windows at 3s each, then the quote batch at 1s: the last response lands at
    # +7s. Two segments, one chain and one quote, are written after it, 0.5s each.
    vendor = _vendor(clock, chain_s=3.0, quote_s=1.0)

    _run(vendor, clock, lake_root)

    line = _only_cycle_line(lake_root)
    assert line["v"] == 1
    assert line["kind"] == "cycle"
    assert line["snap_ts"] == _SNAP.isoformat()
    assert line["cycle_start_ts"] == _at(0)
    assert line["fetch_end_ts"] == _at(7)
    assert line["segments_durable_ts"] == _at(8)
    # Nothing else holds the lock, so the wait is nil, and the hold is one entry a segment.
    assert line["lock_acquired_ts"] == _at(8)
    assert line["lock_released_ts"] == _at(8.5)
    # The stamp runs after the append, inside the cycle, and the line's end follows it.
    assert line["cycle_end_ts"] == _at(8.625)
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
    # The first unit to land did so at the start, so the last unit's response came no later
    # than one write in, and its own write followed it.
    assert fetch_end <= _CLOCK_START + timedelta(seconds=_WRITE_S)
    assert durable == _CLOCK_START + timedelta(seconds=2 * _WRITE_S)
    assert durable - fetch_end >= timedelta(seconds=_WRITE_S)
    assert line["lock_acquired_ts"] == _at(1.0)
    assert line["lock_released_ts"] == _at(1.5)
    assert line["cycle_end_ts"] == _at(1.625)


def test_a_cycle_with_nothing_to_fetch_still_writes_its_line(lake_root, timed_phases):
    clock = timed_phases
    _run(_vendor(clock, chain_s=0.0, quote_s=0.0), clock, lake_root, roster=Roster(()))

    line = _only_cycle_line(lake_root)
    assert line["fetch_end_ts"] is None
    assert line["segments_durable_ts"] == _at(0)
    assert line["lock_released_ts"] == _at(0)


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
    # The quote batch answered last, at +7s. Its segment never landed, and its response
    # still ends the fetch.
    assert _only_cycle_line(lake_root)["fetch_end_ts"] == _at(7)


# -- 3. a lock another process holds ---------------------------------------------------------

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


def test_a_lock_another_process_holds_shows_as_the_wait(lake_root):
    path = manifest_path(lake_root)
    holder = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(path), str(_HOLD_S)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "held"
        began = time.monotonic()
        capture.run_cycle(
            SystemClock(), _InstantVendor(), _ROSTER, lake_root, pid=4242, plan=TWO_WINDOWS
        )
        elapsed = time.monotonic() - began
    finally:
        holder.wait(timeout=10)

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

    def refuse(*args, **kwargs):
        raise OSError("No space left on device")

    monkeypatch.setattr(capture, "append_cycle", refuse)

    result = _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert {(s.surface, s.row_kind) for s in result.segments} == {
        (CHAINS, journal.ROW_KIND_DATA),
        (QUOTES, journal.ROW_KIND_DATA),
    }
    assert cycle_lines(timing_path(lake_root, SESSION)) == []
    err = capsys.readouterr().err
    assert err.count("capture: cycle timing not written for") == 1
    assert "No space left on device" in err


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


# -- 5. a cycle that raises ------------------------------------------------------------------


def test_a_cycle_that_raises_at_its_manifest_append_writes_no_line(
    lake_root, timed_phases, monkeypatch
):
    clock = timed_phases

    def refuse(*args, **kwargs):
        raise RuntimeError("manifest refused")

    monkeypatch.setattr(capture, "record_partition", refuse)

    with pytest.raises(RuntimeError, match="manifest refused"):
        _run(_vendor(clock, chain_s=3.0, quote_s=1.0), clock, lake_root)

    assert timing_lines(timing_path(lake_root, SESSION)) == []
