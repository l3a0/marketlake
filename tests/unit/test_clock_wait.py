"""The clock's wait on threads, marketlake #597.

A capture cycle waits on its requests through ``Clock.wait``, which returns at the first
completion or at an instant, whichever comes first. These cases cover both clocks:

1. Each returns as soon as one future is done, with the others still pending, and without
   waiting out the instant.
2. With nothing done, the manual clock moves forward to the instant through its own
   ``sleep``, and never backwards. The system clock returns empty once the instant passes.
3. With no instant, each waits for the first completion however long it takes.
4. The manual clock loses no move when several threads move it at once. The daemon's
   loop thread and each minute's cycle thread share one clock (marketlake #565).
"""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import Future
from datetime import UTC, datetime, timedelta

from lake.clock import SystemClock
from tests.support.clock import ManualClock

START = datetime(2026, 8, 24, 14, 0, 1, tzinfo=UTC)


def _done() -> Future:
    future: Future = Future()
    future.set_result(None)
    return future


def _finish_later(future: Future, seconds: float) -> threading.Thread:
    thread = threading.Thread(target=lambda: (time.sleep(seconds), future.set_result(None)))
    thread.start()
    return thread


def test_the_manual_clock_returns_the_first_done_without_moving():
    clock = ManualClock(start=START)
    done, held = _done(), Future()

    assert clock.wait({done, held}, START + timedelta(seconds=54)) == {done}
    assert clock.now() == START


def test_the_manual_clock_returns_a_future_that_finishes_inside_its_grace():
    # A healthy request answers in milliseconds of real time. The wait must catch it rather
    # than jump virtual time to the instant and cut it.
    clock = ManualClock(start=START)
    future: Future = Future()
    thread = _finish_later(future, 0.05)

    assert clock.wait({future}, START + timedelta(seconds=54)) == {future}
    assert clock.now() == START
    thread.join()


class _SleepRecordingClock(ManualClock):
    def __init__(self, start: datetime) -> None:
        super().__init__(start)
        self.slept: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        super().sleep(seconds)


def test_the_manual_clock_moves_to_the_instant_through_its_own_sleep():
    clock = _SleepRecordingClock(START)
    until = START + timedelta(seconds=54)

    assert clock.wait({Future()}, until) == set()
    assert clock.now() == until
    assert clock.slept == [54.0]


def test_the_manual_clock_never_moves_backwards():
    clock = ManualClock(start=START)

    assert clock.wait({Future()}, START - timedelta(seconds=5)) == set()
    assert clock.now() == START


def test_the_manual_clock_with_no_instant_waits_for_the_first_completion():
    clock = ManualClock(start=START)
    future: Future = Future()
    thread = _finish_later(future, 1.5)

    assert clock.wait({future, Future()}, None) == {future}
    assert clock.now() == START
    thread.join()


def test_the_system_clock_returns_at_the_first_completion_not_the_instant():
    clock = SystemClock()
    future: Future = Future()
    thread = _finish_later(future, 0.05)
    began = time.monotonic()

    assert clock.wait({future, Future()}, clock.now() + timedelta(seconds=5)) == {future}
    assert time.monotonic() - began < 2.5
    thread.join()


def test_the_system_clock_returns_empty_once_the_instant_passes():
    clock = SystemClock()
    until = clock.now() + timedelta(seconds=0.2)

    assert clock.wait({Future()}, until) == set()
    # The timeout runs on the monotonic timer, so the wall clock may read a hair short. The
    # caller re-reads for that reason, and the wait must not return far early either.
    assert clock.now() >= until - timedelta(seconds=0.05)


def test_the_system_clock_with_no_instant_waits_for_the_first_completion():
    clock = SystemClock()
    future: Future = Future()
    thread = _finish_later(future, 0.05)

    assert clock.wait({future}, None) == {future}
    thread.join()


def test_the_manual_clock_loses_no_move_when_threads_move_it_at_once():
    # ``advance`` reads the time and writes it back. Unlocked, a thread switch between the
    # two drops the other thread's move. A switch interval of a microsecond makes that
    # switch land inside the pair on every run measured, 10 of 10 without the lock.
    clock = ManualClock(start=START)
    moves = 20_000

    def move() -> None:
        for _ in range(moves):
            clock.advance(1)

    threads = [threading.Thread(target=move) for _ in range(4)]
    interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(interval)

    assert clock.now() == START + timedelta(seconds=4 * moves)
    assert clock.monotonic() == 4 * moves
