"""The caffeinate window, the coverage assertion, and the pre-open self-check.

The window is a function of the date alone. A holiday weekday holds the assertion
like any other weekday, because the idle heartbeats must keep flowing. The coverage
assertion is a function of a mint time, ``now``, and the calendar. The self-check
pings only when the injected probe reports the daemon up. Nothing crosses a boundary.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from lake import control_plane as cp
from lake.calendar import MARKET_TZ
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock


def _et(*args: int) -> datetime:
    return datetime(*args, tzinfo=MARKET_TZ)


def _week(monday: date, holidays: set[date] = frozenset()) -> FakeCalendar:
    table = {}
    for offset in range(5):
        day = monday + timedelta(days=offset)
        if day in holidays:
            continue
        table[day] = SessionTimes(
            open=_et(day.year, day.month, day.day, 9, 30),
            close=_et(day.year, day.month, day.day, 16, 0),
        )
    return FakeCalendar(table)


# -- the assertion window -------------------------------------------------------------


def test_weekday_window_runs_from_the_wake_to_the_sweep_ping():
    window = cp.assertion_window(date(2026, 8, 26))  # a Wednesday session
    assert window == cp.AssertionWindow(_et(2026, 8, 26, 8, 25), _et(2026, 8, 26, 18, 45))


def test_holiday_weekday_holds_the_same_window():
    # Labor Day, Monday September 7. The calendar does not enter: a holiday is when
    # the idle heartbeats must keep flowing.
    window = cp.assertion_window(date(2026, 9, 7))
    assert window == cp.AssertionWindow(_et(2026, 9, 7, 8, 25), _et(2026, 9, 7, 18, 45))


def test_saturday_owes_nothing():
    assert cp.assertion_window(date(2026, 9, 5)) is None


def test_sunday_window_runs_from_the_one_shot_wake_to_the_canary_deadline():
    window = cp.assertion_window(date(2026, 9, 6))
    assert window == cp.AssertionWindow(_et(2026, 9, 6, 19, 55), _et(2026, 9, 6, 23, 0))


def test_window_contains_is_half_open():
    window = cp.assertion_window(date(2026, 8, 26))
    assert window.contains(_et(2026, 8, 26, 8, 25))
    assert window.contains(_et(2026, 8, 26, 18, 44))
    assert not window.contains(_et(2026, 8, 26, 18, 45))
    assert not window.contains(_et(2026, 8, 26, 8, 24))


# -- the caffeinate line --------------------------------------------------------------


def test_caffeinate_holds_until_the_window_ends():
    window = cp.assertion_window(date(2026, 8, 26))
    # From the wake: 10 hours 20 minutes.
    assert cp.caffeinate_args(window, _et(2026, 8, 26, 8, 25)) == (
        "caffeinate",
        "-i",
        "-t",
        "37200",
    )
    # Mid-day: 6 hours 45 minutes.
    assert cp.caffeinate_args(window, _et(2026, 8, 26, 12, 0))[-1] == "24300"


def test_caffeinate_rounds_partial_seconds_up():
    window = cp.assertion_window(date(2026, 8, 26))
    now = _et(2026, 8, 26, 18, 44) + timedelta(seconds=59, microseconds=500000)
    assert cp.caffeinate_args(window, now)[-1] == "1"


def test_caffeinate_holds_early_before_the_window_starts():
    window = cp.assertion_window(date(2026, 8, 26))
    assert cp.caffeinate_args(window, _et(2026, 8, 26, 7, 0))[-1] == "42300"


def test_caffeinate_is_none_once_the_window_has_ended():
    window = cp.assertion_window(date(2026, 8, 26))
    assert cp.caffeinate_args(window, _et(2026, 8, 26, 18, 45)) is None
    assert cp.caffeinate_args(window, _et(2026, 8, 26, 21, 0)) is None


def test_hold_assertion_hands_the_runner_todays_line():
    calls: list[tuple[str, ...]] = []
    clock = ManualClock(start=_et(2026, 8, 26, 12, 0))
    args = cp.hold_assertion(clock=clock, runner=lambda a: calls.append(tuple(a)))
    assert args == ("caffeinate", "-i", "-t", "24300")
    assert calls == [args]


def test_hold_assertion_does_nothing_on_saturday():
    calls: list[tuple[str, ...]] = []
    clock = ManualClock(start=_et(2026, 9, 5, 12, 0))
    assert cp.hold_assertion(clock=clock, runner=lambda a: calls.append(tuple(a))) is None
    assert calls == []


def test_hold_assertion_reads_the_eastern_date_from_a_utc_clock():
    # 23:30 UTC on Friday is 19:30 Eastern on Friday: still inside Friday's window.
    from datetime import UTC

    calls: list[tuple[str, ...]] = []
    clock = ManualClock(start=datetime(2026, 8, 28, 22, 0, tzinfo=UTC))  # 18:00 ET
    args = cp.hold_assertion(clock=clock, runner=lambda a: calls.append(tuple(a)))
    assert args is not None and args[-1] == "2700"


# -- the coverage assertion -----------------------------------------------------------

WEEK = _week(date(2026, 8, 31))
SUNDAY_NOW = _et(2026, 8, 30, 20, 0)


def test_week_option_close_is_the_coming_fridays():
    assert cp.week_option_close(SUNDAY_NOW, WEEK) == _et(2026, 9, 4, 16, 15)


def test_week_option_close_walks_back_over_a_friday_holiday():
    short = _week(date(2026, 8, 31), holidays={date(2026, 9, 4)})
    assert cp.week_option_close(SUNDAY_NOW, short) == _et(2026, 9, 3, 16, 15)


def test_a_fresh_sunday_mint_covers_the_week():
    assert cp.token_covers_week(_et(2026, 8, 30, 19, 30), SUNDAY_NOW, WEEK) is True


def test_a_late_prior_week_mint_is_valid_but_not_fresh():
    # Minted Thursday evening: still valid on Sunday, dead Thursday, before Friday's
    # option close. Validity is not freshness.
    assert cp.token_covers_week(_et(2026, 8, 27, 18, 0), SUNDAY_NOW, WEEK) is False


def test_the_assertion_needs_strict_clearance():
    # Mint plus seven days lands exactly on Friday's option close. Not cleared.
    assert cp.token_covers_week(_et(2026, 8, 28, 16, 15), SUNDAY_NOW, WEEK) is False
    assert cp.token_covers_week(_et(2026, 8, 28, 16, 16), SUNDAY_NOW, WEEK) is True


def test_a_friday_holiday_relaxes_the_bar_to_thursday():
    short = _week(date(2026, 8, 31), holidays={date(2026, 9, 4)})
    assert cp.token_covers_week(_et(2026, 8, 27, 18, 0), SUNDAY_NOW, short) is True


def test_naive_times_are_refused():
    with pytest.raises(ValueError):
        cp.token_covers_week(datetime(2026, 8, 30, 19, 30), SUNDAY_NOW, WEEK)


def test_a_dark_week_raises():
    with pytest.raises(ValueError):
        cp.week_option_close(SUNDAY_NOW, FakeCalendar({}))


# -- the pre-open self-check -------------------------------------------------------------


class FakePinger:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)


URL = "https://hc-ping.com/secret-key/pre-open"


def test_self_check_pings_only_when_the_daemon_is_up():
    pinger = FakePinger()
    seen: list[str] = []

    def probe(label: str) -> bool:
        seen.append(label)
        return True

    outcome = cp.self_check(probe=probe, pinger=pinger, ping_url=URL)
    assert outcome == cp.SelfCheckOutcome(daemon_up=True, pinged=True)
    assert seen == [cp.DAEMON_LABEL]
    assert pinger.urls == [URL]


def test_self_check_does_not_ping_a_down_daemon():
    pinger = FakePinger()
    outcome = cp.self_check(probe=lambda label: False, pinger=pinger, ping_url=URL)
    assert outcome == cp.SelfCheckOutcome(daemon_up=False, pinged=False)
    assert pinger.urls == []


def test_a_raising_probe_never_pings():
    pinger = FakePinger()

    def probe(label: str) -> bool:
        raise OSError("launchctl not found")

    with pytest.raises(OSError):
        cp.self_check(probe=probe, pinger=pinger, ping_url=URL)
    assert pinger.urls == []


def test_launchctl_print_parser_wants_a_running_state():
    running = "com.marketlake.daemon = {\n\tactive count = 1\n\tstate = running\n\tpid = 42\n}\n"
    idle = "com.marketlake.daemon = {\n\tstate = not running\n}\n"
    assert cp.parse_launchctl_print(running) is True
    assert cp.parse_launchctl_print(idle) is False
    assert cp.parse_launchctl_print("") is False
