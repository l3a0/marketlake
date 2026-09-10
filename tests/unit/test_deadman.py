"""The capture dead-man ping and its idle heartbeats."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from lake.deadman import CAPTURE_SLUG, DeadMan, in_envelope
from lake.session import SessionClock
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock

ET = ZoneInfo("America/New_York")
WEEK = date(2026, 8, 31)
URL = "https://hc-ping.com/KEY/capture"


class Recording:
    def __init__(self) -> None:
        self.pings: list[str] = []

    def ping(self, url: str) -> None:
        self.pings.append(url)


class Broken:
    def ping(self, url: str) -> None:
        raise OSError("no network")


def _deadman(at: datetime, pinger=None) -> DeadMan:
    return DeadMan(
        pinger=pinger if pinger is not None else Recording(),
        url=URL,
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
    )


def test_the_slug_is_the_one_the_design_names():
    assert CAPTURE_SLUG == "capture"


@pytest.mark.parametrize(
    ("moment", "inside"),
    [
        (et(2026, 9, 2, 8, 24), False),  # before the firmware wake
        (et(2026, 9, 2, 8, 25), True),  # the wake itself
        (et(2026, 9, 2, 12, 0), True),  # mid-session
        (et(2026, 9, 2, 18, 44), True),  # the last minute inside it
        # The window is half-open, so it ends the instant the sweep's ping lands. That
        # boundary comes from the assertion window rather than being restated here.
        (et(2026, 9, 2, 18, 45), False),
        (et(2026, 9, 5, 12, 0), False),  # Saturday owes nothing
        # The assertion window covers Sunday evening so the Sunday job can run, but no
        # capture is expected then and a heartbeat would read as a session running.
        (et(2026, 9, 6, 21, 0), False),
    ],
)
def test_the_envelope_is_the_window_the_daemon_is_kept_awake_for(moment, inside):
    assert in_envelope(moment) is inside


def test_a_holiday_still_heartbeats():
    # A holiday is exactly when a silent daemon must not read as a dead one. The
    # calendar deliberately does not enter here.
    holiday = weekday_sessions(WEEK, holidays=(date(2026, 9, 2),))
    pinger = Recording()
    deadman = DeadMan(
        pinger=pinger,
        url=URL,
        session_clock=SessionClock(
            clock=ManualClock(start=et(2026, 9, 2, 12, 0)), calendar=holiday
        ),
    )
    assert deadman.idle(et(2026, 9, 2, 12, 0))
    assert pinger.pings == [URL]


def test_a_capture_slot_is_left_to_the_cycle_that_owns_it():
    # A heartbeat inside the capture window would tell healthchecks the daemon is fine
    # while a surface is dead. That minute is the watchdog's business.
    pinger = Recording()
    deadman = _deadman(et(2026, 9, 2, 12, 0), pinger)
    assert not deadman.idle(et(2026, 9, 2, 12, 0))
    assert pinger.pings == []


def test_a_durable_cycle_feeds_the_check():
    pinger = Recording()
    assert _deadman(et(2026, 9, 2, 12, 0), pinger).captured(et(2026, 9, 2, 12, 0))
    assert pinger.pings == [URL]


def test_a_heartbeat_is_rate_limited_to_the_tick():
    pinger = Recording()
    deadman = _deadman(et(2026, 9, 2, 8, 30), pinger)
    assert deadman.idle(et(2026, 9, 2, 8, 30))
    assert not deadman.idle(et(2026, 9, 2, 8, 30) + timedelta(seconds=30))
    assert deadman.idle(et(2026, 9, 2, 8, 31))
    assert len(pinger.pings) == 2


def test_outside_the_envelope_nothing_is_sent():
    pinger = Recording()
    assert not _deadman(et(2026, 9, 2, 3, 0), pinger).idle(et(2026, 9, 2, 3, 0))
    assert pinger.pings == []


def test_a_ping_that_fails_never_raises():
    # A missed ping is exactly what the check is for. Crashing while reporting alive
    # would turn a missing minute into a missing session.
    assert not _deadman(et(2026, 9, 2, 12, 0), Broken()).captured(et(2026, 9, 2, 12, 0))


# -- the record the Now panel reads -------------------------------------------


def _recording_deadman(at: datetime, pinger=None) -> tuple[DeadMan, list[datetime]]:
    """A dead-man whose recorder collects the instants it is handed."""
    recorded: list[datetime] = []
    deadman = DeadMan(
        pinger=pinger if pinger is not None else Recording(),
        url=URL,
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        recorder=recorded.append,
    )
    return deadman, recorded


def test_a_landed_ping_is_recorded_for_the_panel():
    # healthchecks knows when the last ping landed. The Now panel reads the lake, so
    # the instant is written down there too.
    at = et(2026, 9, 2, 12, 0)
    deadman, recorded = _recording_deadman(at)
    assert deadman.captured(at)
    assert recorded == [at]


def test_a_heartbeat_is_recorded_the_same_way():
    at = et(2026, 9, 2, 8, 30)
    deadman, recorded = _recording_deadman(at)
    assert deadman.idle(at)
    assert recorded == [at]


def test_a_ping_that_never_left_the_laptop_is_not_recorded():
    # The panel's line says the check is being fed. A ping that failed is not feeding
    # it, and recording one would show a dead check as a healthy one.
    at = et(2026, 9, 2, 12, 0)
    deadman, recorded = _recording_deadman(at, Broken())
    assert not deadman.captured(at)
    assert recorded == []


def test_a_minute_that_sends_no_ping_records_nothing():
    at = et(2026, 9, 2, 3, 0)  # outside the envelope
    deadman, recorded = _recording_deadman(at)
    assert not deadman.idle(at)
    assert recorded == []


def test_a_recorder_that_raises_never_costs_the_ping():
    # The ping is the guarantee and the record is a courtesy. A raise here would turn a
    # fed check into a crashed daemon, the exact failure the check exists to catch.
    at = et(2026, 9, 2, 12, 0)

    def refuse(instant: datetime) -> None:
        raise OSError("read-only")

    pinger = Recording()
    deadman = DeadMan(
        pinger=pinger,
        url=URL,
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        recorder=refuse,
    )
    assert deadman.captured(at)
    assert pinger.pings == [URL]
