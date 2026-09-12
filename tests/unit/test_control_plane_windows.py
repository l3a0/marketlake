"""The caffeinate window, the coverage assertion, and the pre-open self-check.

The window is a function of the date alone. A holiday weekday holds the assertion
like any other weekday, because the idle heartbeats must keep flowing. The coverage
assertion is a function of a mint time, ``now``, and the calendar. The self-check
pings only when the injected probe reports the daemon up. Nothing crosses a boundary.
"""

from __future__ import annotations

import http.client
import io
import urllib.error
from datetime import date, datetime, timedelta

import pytest

from lake import control_plane as cp
from tests.support.calendar import FakeCalendar, et, weekday_sessions
from tests.support.pinger import FakePinger

# -- the assertion window -------------------------------------------------------------


def test_weekday_window_runs_from_the_wake_to_the_sweep_ping():
    window = cp.assertion_window(date(2026, 8, 26))  # a Wednesday session
    assert window == cp.AssertionWindow(et(2026, 8, 26, 8, 25), et(2026, 8, 26, 18, 45))


def test_holiday_weekday_holds_the_same_window():
    # Labor Day, Monday September 7. The calendar does not enter: a holiday is when
    # the idle heartbeats must keep flowing.
    window = cp.assertion_window(date(2026, 9, 7))
    assert window == cp.AssertionWindow(et(2026, 9, 7, 8, 25), et(2026, 9, 7, 18, 45))


def test_saturday_owes_nothing():
    assert cp.assertion_window(date(2026, 9, 5)) is None


def test_sunday_window_runs_from_the_one_shot_wake_to_the_check_deadline():
    # It ends at 23:30, not at CANARY_DEADLINE. The last retry starts at 23:00, and a
    # hold that stopped there would free the machine to sleep mid-attempt.
    window = cp.assertion_window(date(2026, 9, 6))
    assert window == cp.AssertionWindow(et(2026, 9, 6, 19, 55), et(2026, 9, 6, 23, 30))
    assert window.end > cp.CANARY_DEADLINE.on(date(2026, 9, 6))


def test_window_contains_is_half_open():
    window = cp.assertion_window(date(2026, 8, 26))
    assert window.contains(et(2026, 8, 26, 8, 25))
    assert window.contains(et(2026, 8, 26, 18, 44))
    assert not window.contains(et(2026, 8, 26, 18, 45))
    assert not window.contains(et(2026, 8, 26, 8, 24))


# -- the caffeinate line --------------------------------------------------------------


def test_caffeinate_holds_until_the_window_ends():
    window = cp.assertion_window(date(2026, 8, 26))
    # From the wake: 10 hours 20 minutes.
    assert cp.caffeinate_args(window, et(2026, 8, 26, 8, 25)) == (
        "caffeinate",
        "-i",
        "-t",
        "37200",
    )
    # Mid-day: 6 hours 45 minutes.
    assert cp.caffeinate_args(window, et(2026, 8, 26, 12, 0))[-1] == "24300"


def test_caffeinate_rounds_partial_seconds_up():
    window = cp.assertion_window(date(2026, 8, 26))
    now = et(2026, 8, 26, 18, 44) + timedelta(seconds=59, microseconds=500000)
    assert cp.caffeinate_args(window, now)[-1] == "1"


def test_caffeinate_holds_early_before_the_window_starts():
    window = cp.assertion_window(date(2026, 8, 26))
    assert cp.caffeinate_args(window, et(2026, 8, 26, 7, 0))[-1] == "42300"


def test_caffeinate_is_none_once_the_window_has_ended():
    window = cp.assertion_window(date(2026, 8, 26))
    assert cp.caffeinate_args(window, et(2026, 8, 26, 18, 45)) is None
    assert cp.caffeinate_args(window, et(2026, 8, 26, 21, 0)) is None


# -- the coverage assertion -----------------------------------------------------------

WEEK = weekday_sessions(date(2026, 8, 31))
SUNDAY_NOW = et(2026, 8, 30, 20, 0)


def test_week_option_close_is_the_coming_fridays():
    assert cp.week_option_close(SUNDAY_NOW, WEEK) == et(2026, 9, 4, 16, 15)


def test_week_option_close_walks_back_over_a_friday_holiday():
    short = weekday_sessions(date(2026, 8, 31), holidays={date(2026, 9, 4)})
    assert cp.week_option_close(SUNDAY_NOW, short) == et(2026, 9, 3, 16, 15)


def test_a_fresh_sunday_mint_covers_the_week():
    assert cp.token_covers_week(et(2026, 8, 30, 19, 30), SUNDAY_NOW, WEEK) is True


def test_a_late_prior_week_mint_is_valid_but_not_fresh():
    # Minted Thursday evening: still valid on Sunday, dead Thursday, before Friday's
    # option close. Validity is not freshness.
    assert cp.token_covers_week(et(2026, 8, 27, 18, 0), SUNDAY_NOW, WEEK) is False


def test_the_assertion_needs_strict_clearance():
    # Mint plus seven days lands exactly on Friday's option close. Not cleared.
    assert cp.token_covers_week(et(2026, 8, 28, 16, 15), SUNDAY_NOW, WEEK) is False
    assert cp.token_covers_week(et(2026, 8, 28, 16, 16), SUNDAY_NOW, WEEK) is True


def test_a_friday_holiday_relaxes_the_bar_to_thursday():
    short = weekday_sessions(date(2026, 8, 31), holidays={date(2026, 9, 4)})
    assert cp.token_covers_week(et(2026, 8, 27, 18, 0), SUNDAY_NOW, short) is True


def test_naive_times_are_refused():
    with pytest.raises(ValueError):
        cp.token_covers_week(datetime(2026, 8, 30, 19, 30), SUNDAY_NOW, WEEK)


def test_a_monday_catch_up_judges_this_week_not_the_next():
    # launchd coalesces a wake missed over the weekend and fires the Sunday job at the
    # Monday 08:25 wake. That backstop can only clear the check when the run judges the
    # week it stands in. Anchoring on the Monday strictly after today judged next week,
    # which a token minted the evening before can never cover, so the backstop could
    # never pass and the check could never come back up.
    catch_up = et(2026, 8, 31, 8, 25)
    assert cp.week_option_close(catch_up, WEEK) == et(2026, 9, 4, 16, 15)
    assert cp.token_covers_week(et(2026, 8, 30, 20, 10), catch_up, WEEK) is True


def test_every_weekday_before_the_last_close_judges_the_same_week():
    for month, day in ((8, 31), (9, 1), (9, 2), (9, 3), (9, 4)):
        assert cp.week_option_close(et(2026, month, day, 12, 0), WEEK) == et(2026, 9, 4, 16, 15)


def test_after_a_midweek_close_the_week_still_holds():
    # Wednesday 17:00, past that day's option close. Thursday and Friday are still
    # ahead, so the week the token must cover has not moved.
    assert cp.week_option_close(et(2026, 9, 2, 17, 0), WEEK) == et(2026, 9, 4, 16, 15)


def test_past_the_last_close_the_week_moves_on():
    # Friday evening, past the week's last option close. The next session is the
    # following Monday, so the coming week is the one the token must now cover.
    two = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7))
    assert cp.week_option_close(et(2026, 9, 4, 18, 0), two) == et(2026, 9, 11, 16, 15)
    # The Sunday answer is unchanged by the wider calendar.
    assert cp.week_option_close(SUNDAY_NOW, two) == et(2026, 9, 4, 16, 15)


def test_a_dark_week_raises():
    with pytest.raises(ValueError):
        cp.week_option_close(SUNDAY_NOW, FakeCalendar({}))


# -- the pre-open self-check -------------------------------------------------------------


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


class RaisingPinger:
    """A pinger whose GET fails, the way a wifi blip makes the real one fail."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def ping(self, url: str) -> None:
        raise self.exc


# The key is the secret half. The host is public, so an assertion about the host was
# never the rule the design states.
PING_KEY = "SUPERSECRETKEY"
PING_URL = f"https://hc-ping.com/{PING_KEY}/pre-open"


@pytest.mark.parametrize(
    "exc",
    [
        urllib.error.HTTPError(PING_URL, 500, "Server Error", {}, io.BytesIO(b"")),
        urllib.error.URLError(OSError("connection refused")),
        TimeoutError("timed out"),
        http.client.IncompleteRead(b""),
    ],
)
def test_a_failed_ping_is_named_rather_than_raised(exc):
    # The missed ping is the same either way, and healthchecks pages for it after the
    # grace. What a raise cost was the job's own summary line, replaced by a traceback.
    outcome = cp.self_check(probe=lambda label: True, pinger=RaisingPinger(exc), ping_url=PING_URL)
    assert outcome.daemon_up is True
    assert outcome.pinged is False
    assert outcome.problem == f"ping failed: {type(exc).__name__}"


@pytest.mark.parametrize(
    "exc",
    [
        urllib.error.HTTPError(PING_URL, 500, "Server Error", {}, io.BytesIO(b"")),
        urllib.error.URLError(OSError("connection refused")),
    ],
)
def test_a_failed_ping_never_carries_the_key(exc):
    # The URL holds the ping key, and the design's rule is that it never reaches a log.
    outcome = cp.self_check(probe=lambda label: True, pinger=RaisingPinger(exc), ping_url=PING_URL)
    assert PING_KEY not in outcome.problem
    assert PING_URL not in outcome.problem


def test_launchctl_print_parser_wants_a_running_state():
    running = "com.marketlake.daemon = {\n\tactive count = 1\n\tstate = running\n\tpid = 42\n}\n"
    idle = "com.marketlake.daemon = {\n\tstate = not running\n}\n"
    assert cp.parse_launchctl_print(running) is True
    assert cp.parse_launchctl_print(idle) is False
    assert cp.parse_launchctl_print("") is False


# -- the assertion holder ---------------------------------------------------------

# The daemon calls this every minute. One caffeinate process per window is owed, not
# one per minute, and `caffeinate -i -t` releases itself when the window ends.


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, args) -> None:
        self.calls.append(tuple(args))


def test_the_holder_spawns_once_per_window_not_once_per_tick():
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    # Every minute from 08:25 to 08:29 on a Monday.
    for minute in range(25, 30):
        holder.hold(et(2026, 8, 31, 8, minute))
    assert len(runner.calls) == 1
    assert runner.calls[0][:3] == ("caffeinate", "-i", "-t")
    # 08:25 to 18:45 is 10 hours 20 minutes.
    assert runner.calls[0][3] == str((18 - 8) * 3600 + (45 - 25) * 60)


def test_the_holder_takes_the_next_day_window_too():
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 8, 31, 8, 25))
    holder.hold(et(2026, 8, 31, 12, 0))
    assert len(runner.calls) == 1
    holder.hold(et(2026, 9, 1, 8, 25))
    assert len(runner.calls) == 2


def test_the_holder_waits_for_the_window_to_open():
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 8, 31, 0, 0))  # midnight, hours before the wake
    holder.hold(et(2026, 8, 31, 8, 24))
    assert runner.calls == []
    holder.hold(et(2026, 8, 31, 8, 25))
    assert len(runner.calls) == 1


def test_the_holder_owes_nothing_after_the_window_or_on_saturday():
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 8, 31, 19, 0))  # past the 18:45 end
    holder.hold(et(2026, 9, 5, 12, 0))  # Saturday
    assert runner.calls == []


def test_the_holder_reads_the_eastern_date_from_a_utc_clock():
    # The instant has to cross a date, or the conversion is invisible. 02:00 UTC on
    # Monday is 22:00 Eastern on Sunday, so the UTC date names the weekday window and
    # the Eastern date names the Sunday evening one. The holder's signature takes any
    # aware datetime, so it converts rather than trusting its caller's zone.
    from datetime import UTC

    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(datetime(2026, 8, 31, 2, 0, tzinfo=UTC))
    assert len(runner.calls) == 1
    # 22:00 to the 23:30 window end is ninety minutes. Read as Monday 02:00 there is no
    # window at all, so a holder that skipped the conversion would spawn nothing.
    assert runner.calls[0][3] == "5400"


def test_the_holder_returns_the_arguments_it_handed_the_runner():
    # The docstring promises the args back, or None when nothing was owed. Nothing read
    # that until now, which is the same dead-weight this change is removing elsewhere.
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    args = holder.hold(et(2026, 8, 31, 8, 25))
    assert args == runner.calls[0]
    assert holder.hold(et(2026, 8, 31, 8, 26)) is None
    assert holder.hold(et(2026, 9, 5, 12, 0)) is None


def test_the_holder_takes_the_sunday_evening_window():
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 8, 30, 19, 55))
    assert len(runner.calls) == 1
    # 19:55 to 23:30 is 3 hours 35 minutes.
    assert runner.calls[0][3] == str(3 * 3600 + 35 * 60)


def test_a_hold_across_the_fall_back_sunday_measures_absolute_time():
    # 2026-11-01 repeats the 01:00 hour. The Sunday window runs 19:55 to 23:30, both
    # after the change, so the daemon's own hold is unaffected. The span is measured
    # in absolute time, so a call from inside the repeated hour is not an hour short.
    window = cp.assertion_window(date(2026, 11, 1))
    early = et(2026, 11, 1, 1, 0)
    args = cp.caffeinate_args(window, early)
    assert args is not None
    assert int(args[3]) == int(window.end.timestamp() - early.timestamp())


# -- a spawn that will not start -------------------------------------------------------


class _Failing:
    """A runner that refuses the first ``fail`` spawns, then works."""

    def __init__(self, fail: int = 10_000, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._left = fail
        self._error = error if error is not None else BlockingIOError(35, "no slots")

    def __call__(self, args) -> None:
        self.calls.append(tuple(args))
        if self._left > 0:
            self._left -= 1
            raise self._error


def test_each_window_owes_its_own_page(tmp_path):
    """Once per window is the rule, and one window can never prove it.

    A holder that reports once in its whole life satisfies a single-window test exactly
    as a correct one does, and so does a holder that records only its first failure ever.
    Both leave a daemon that pages on Monday's lapse and stays silent through every day
    after, which is the failure this rule exists to prevent rather than the one it is
    named for.
    """
    runner = _Failing()
    holder = cp.AssertionHolder(runner=runner)

    # Wednesday, failing from the window's open to its close.
    for minute in (30, 31, 32):
        holder.hold(et(2026, 9, 2, 8, minute))
    first = holder.pending_failure()
    assert first is not None, "the first window's failure was never offered"
    holder.mark_reported(first[0])
    assert holder.pending_failure() is None, "the same window was offered twice"

    # Thursday. A new window, a new failure, and a page of its own.
    holder.hold(et(2026, 9, 3, 8, 30))
    second = holder.pending_failure()

    assert second is not None, "the next window's failure was never offered"
    assert second[0] != first[0], "the second page named the first window"


def test_a_spawn_that_recovers_in_its_window_owes_no_page():
    """A held assertion owes no report, which is what clearing the failure states."""
    runner = _Failing(fail=1)
    holder = cp.AssertionHolder(runner=runner)

    holder.hold(et(2026, 9, 2, 8, 30))
    assert holder.pending_failure() is not None, "the failure was not recorded at all"
    holder.hold(et(2026, 9, 2, 8, 31))

    assert holder.pending_failure() is None, "a page was owed for an assertion now held"
    assert len(runner.calls) == 2


def test_a_failure_that_is_not_an_oserror_is_caught_too():
    """The claim is categorical, so the test cannot rest on one exception class.

    ``BlockingIOError`` is the realistic failure and it is an ``OSError``, so a catch
    narrowed to that class passes every test built on the realistic case. ``Popen`` can
    also raise ``ValueError`` on a bad argument, and a raise of any class from this hook
    exits the process just the same.
    """
    runner = _Failing(error=ValueError("embedded null byte"))
    holder = cp.AssertionHolder(runner=runner)

    assert holder.hold(et(2026, 9, 2, 8, 30)) is None, "a failed spawn answered as if held"

    failure = holder.pending_failure()
    assert failure is not None and isinstance(failure[1], ValueError)


def test_a_failed_spawn_answers_none_rather_than_the_arguments():
    """The docstring promises ``None`` on the failure path, so the path is checked."""
    holder = cp.AssertionHolder(runner=_Failing())

    assert holder.hold(et(2026, 9, 2, 8, 30)) is None


# -- a child that dies inside its window -----------------------------------------------


class _Child:
    """A stand-in for the spawned ``caffeinate``, alive until it is told otherwise."""

    def __init__(self) -> None:
        self._status: int | None = None

    def die(self, status: int = 0) -> None:
        self._status = status

    def poll(self) -> int | None:
        return self._status


class _SpawningRunner:
    """A runner that hands back a child the test can kill, the way ``Popen`` does."""

    def __init__(self) -> None:
        self.children: list[_Child] = []

    def __call__(self, args) -> object:
        child = _Child()
        self.children.append(child)
        return child


def test_a_child_that_dies_inside_its_window_is_re_taken():
    """The capture-loss path this closes, and the daemon is awake for all of it.

    ``caffeinate`` carries a timer to the window's end, so an exit before then means it
    was killed or died. Nothing releases one early on purpose. Before this, the holder
    believed the window was held for the rest of the day and the machine idled to sleep
    underneath a daemon that was running and ticking the whole time.
    """
    runner = _SpawningRunner()
    holder = cp.AssertionHolder(runner=runner)

    assert holder.hold(et(2026, 9, 2, 8, 30)) is not None, "the first hold never spawned"
    assert holder.hold(et(2026, 9, 2, 8, 31)) is None, "it re-spawned over a living child"

    runner.children[0].die()
    retaken = holder.hold(et(2026, 9, 2, 8, 32))

    assert retaken is not None, "a dead child was left dead for the rest of the window"
    assert holder.took_over_dead_child() is True
    assert holder.took_over_dead_child() is False, "the re-take was offered twice"

    # And it sticks. A re-take that left the window unheld would spawn again every minute,
    # piling up live caffeinates rather than replacing the one that went.
    assert holder.hold(et(2026, 9, 2, 8, 33)) is None
    assert len(runner.children) == 2, "the re-take did not take"


def test_a_living_child_is_left_alone_for_the_whole_window():
    """The other half. A re-spawn a minute would pile up assertions all day."""
    runner = _SpawningRunner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))

    for minute in range(31, 45):
        assert holder.hold(et(2026, 9, 2, 8, minute)) is None

    assert len(runner.children) == 1
    assert holder.took_over_dead_child() is False


def test_a_runner_that_hands_back_nothing_is_taken_at_its_word():
    """Unable to tell is not the same as gone, and the two want opposite behaviour.

    Every test double in this suite returns ``None``, and so may a future caller that has
    no process to hand back. Reading that as a dead child would re-spawn a ``caffeinate``
    every minute of a ten-hour window on the strength of knowing nothing.
    """
    runner = _Runner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))

    for minute in range(31, 40):
        holder.hold(et(2026, 9, 2, 8, minute))

    assert len(runner.calls) == 1, "a runner returning nothing was read as a dead child"
    assert holder.took_over_dead_child() is False


def test_a_dead_child_from_an_earlier_window_does_not_count_as_a_re_take():
    """A new window spawns because it is new, not because the old child is gone."""
    runner = _SpawningRunner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))
    runner.children[0].die()

    holder.hold(et(2026, 9, 3, 8, 30))
    # The second tick of the new window is the one that matters. A holder that kept the
    # first window's dead child reads "gone" on every tick from here and re-spawns a
    # caffeinate a minute for the whole ten-hour window.
    holder.hold(et(2026, 9, 3, 8, 31))

    assert len(runner.children) == 2, "the new window did not adopt its own child"
    assert holder.took_over_dead_child() is False, "a new window was reported as a re-take"


class _ScriptedRunner:
    """A runner that fails or succeeds on command, handing back a killable child."""

    def __init__(self) -> None:
        self.fail = False
        self.children: list[_Child] = []

    def __call__(self, args) -> object:
        if self.fail:
            raise BlockingIOError(35, "no slots")
        child = _Child()
        self.children.append(child)
        return child


def test_a_re_take_that_fails_is_not_reported_as_a_re_take():
    """Saying it was re-taken when the spawn raised is worse than saying nothing.

    The flag used to be set before the spawn was attempted, so the daemon's log line
    claimed the machine was being held again on a tick where nothing had been taken.
    """
    runner = _ScriptedRunner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))
    runner.children[0].die()
    runner.fail = True

    assert holder.hold(et(2026, 9, 2, 8, 31)) is None
    assert holder.took_over_dead_child() is False, "a failed spawn claimed a re-take"


def test_a_second_loss_in_a_window_that_already_paged_can_still_page():
    """The once-per-window rule is about one unresolved failure, not the window's quota.

    A window that paged for an early failure and then recovered had spent its only page.
    A different loss hours later was suppressed on the window it had already reported,
    so the machine stayed unheld for the session with nothing saying so. That hole opened
    the moment a window could hold more than one spawn.
    """
    runner = _ScriptedRunner()
    holder = cp.AssertionHolder(runner=runner)

    runner.fail = True
    holder.hold(et(2026, 9, 2, 8, 30))
    first = holder.pending_failure()
    assert first is not None
    holder.mark_reported(first[0])

    runner.fail = False
    holder.hold(et(2026, 9, 2, 8, 31))
    assert holder.pending_failure() is None, "a held window still owed a page"

    runner.children[0].die()
    runner.fail = True
    holder.hold(et(2026, 9, 2, 11, 1))

    assert holder.pending_failure() is not None, "the second loss went unpageable"


def test_a_continuous_re_take_is_reported_once_for_the_window():
    """A caffeinate that exits on every spawn must not write a line a minute.

    That is the same argument the page's collapse rests on, applied to the log the
    restart script sends the operator to. Once a window still shows a chronic one, every
    window, which is the signal worth having.
    """
    runner = _SpawningRunner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))

    reports = 0
    for minute in range(31, 45):
        runner.children[-1].die()
        holder.hold(et(2026, 9, 2, 8, minute))
        reports += 1 if holder.took_over_dead_child() else 0

    assert len(runner.children) == 15, "the re-take stopped happening"
    assert reports == 1, f"one line per window, got {reports}"


def test_a_child_whose_poll_raises_neither_crashes_nor_re_spawns():
    """This runs from a hook nothing wraps, so the read has the same rule as the spawn.

    ``Popen.poll`` does not raise in CPython, but the protocol promises only that the
    attribute exists. Unable to tell answers held, because the other answer re-spawns a
    caffeinate every minute of the window on the strength of an error.
    """

    class _Hostile:
        def poll(self) -> int | None:
            raise OSError("no such process")

    holder = cp.AssertionHolder(runner=lambda args: _Hostile())
    holder.hold(et(2026, 9, 2, 8, 30))

    assert holder.hold(et(2026, 9, 2, 8, 31)) is None, "a raising poll forced a re-spawn"
    assert holder.took_over_dead_child() is False


def test_a_child_killed_by_a_signal_is_re_taken():
    """``kill -9`` is the likeliest way one of these dies, and it never exits zero.

    ``Popen.poll`` answers the negative signal number, so a liveness read written as
    "exited cleanly" rather than "exited at all" would call a killed child alive and leave
    the window unheld for the day. Every other double here dies with status 0, which is
    the one status a hand-killed process does not have.
    """
    runner = _SpawningRunner()
    holder = cp.AssertionHolder(runner=runner)
    holder.hold(et(2026, 9, 2, 8, 30))

    runner.children[0].die(-9)
    retaken = holder.hold(et(2026, 9, 2, 8, 31))

    assert retaken is not None, "a SIGKILLed caffeinate was read as still holding"
    assert len(runner.children) == 2


def test_a_handle_with_no_poll_is_tolerated_rather_than_fatal():
    """Unable to ask is a supported answer. Raising here would exit the daemon.

    The guard is an ``isinstance`` against the protocol rather than a ``None`` check, and
    the difference only shows for a runner that hands back something real without a
    ``poll``. A ``None`` check would reach for the attribute and raise, from a hook
    ``run_loop`` does not wrap, which is the crash loop this whole area exists to prevent.
    """
    runner = _CountingRunner(lambda args: 4242)
    holder = cp.AssertionHolder(runner=runner)

    assert holder.hold(et(2026, 9, 2, 8, 30)) is not None
    assert holder.hold(et(2026, 9, 2, 8, 31)) is None, "an unaskable handle forced a re-spawn"
    assert runner.calls == 1


class _CountingRunner:
    def __init__(self, answer) -> None:
        self.calls = 0
        self._answer = answer

    def __call__(self, args) -> object:
        self.calls += 1
        return self._answer(args)


def test_the_holder_spawns_through_the_real_runner_by_default():
    """The default is the production spawn, which nothing else in the suite reaches.

    Every test hands in a double, so the default was free to be anything, a no-op
    included, with the whole suite green. That is the shape where the feature ships
    disabled.
    """
    assert cp.AssertionHolder()._runner is cp._spawn


def test_the_real_spawn_hands_back_something_the_holder_can_ask():
    """``_spawn``'s contract is that its return can be polled, and only this runs it.

    The protocol's docstring claims ``subprocess.Popen`` satisfies it. Returning a pid, or
    nothing, would make every liveness read answer "held" forever and silently disable the
    re-take, with no test red. ``true`` is used rather than ``caffeinate`` so the test
    spawns nothing that touches power state.
    """
    child = cp._spawn(["true"])

    assert isinstance(child, cp.AssertionHandle), "the spawn returned something unaskable"
    child.wait()
    assert child.poll() is not None, "a finished child still read as running"
