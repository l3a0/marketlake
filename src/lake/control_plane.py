"""The laptop control plane.

The data plane is plain files any host can serve. The control plane is what keeps the
laptop awake, keeps the two resident processes running, and proves both to the outside
world. It is macOS-specific by construction and is rewritten per host. This module
renders it and reasons about it. It executes nothing privileged. No ``sudo``, no
``pmset`` write, no ``launchctl`` bootstrap, and no ``tmutil`` write runs from here.
Installing is the operator's step, by hand. Three read-only probes do run, from the
rendered jobs and from the by-hand live checks: ``launchctl print``, ``pmset -g sched``,
and ``tmutil isexcluded``. None of them needs root.

Terms, glossed at first use.

- *launchd* is macOS's service manager. A *LaunchDaemon* is a launchd job installed
  system-wide under ``/Library/LaunchDaemons``. It runs without a login session. The
  plist sets ``UserName`` and ``GroupName`` to the owner, so everything the job creates
  stays user-owned. ``KeepAlive`` makes launchd relaunch an exiting process within
  seconds. That is the resident-process shape the daemon and the query service take.
- *pmset* is the macOS power-scheduling tool. ``pmset repeat`` holds one repeating
  firmware wake alarm. ``pmset schedule`` adds a one-shot. Both writes need root.
  ``pmset -g sched`` reads the schedule back and needs no root.
- A *sudoers drop-in* is a file under ``/etc/sudoers.d`` granting one user passwordless
  ``sudo`` for named commands. The one here grants exactly the two ``pmset`` writes.
- *caffeinate* is the macOS tool that holds a power assertion. ``caffeinate -i``
  prevents idle sleep while it runs. It cannot stop a closed lid from sleeping.
- *tmutil addexclusion* keeps an item out of Time Machine backups. The whole config
  directory gets it, so neither the brokerage token nor the secrets in ``config.yaml``
  ride onto a backup disk. ``tmutil isexcluded`` reads that back.
- A *healthchecks slug* names one dead-man check. A *dead-man check* pages when a
  ping does not arrive, so silence is the alarm. A ping fires only on the job's
  success condition, never on mere liveness.
- The *vendor sweep* is the 18:30 weekday job that pulls the day's settled vendor data.
  Slice 3 builds it. Its Friday run is what sets the Sunday one-shot wake.
- The *canary* is the Sunday throwaway authenticated call that proves the brokerage
  token still works. It retries every 30 minutes until it passes or its deadline.
- The *scrub* is the weekly integrity pass over the lake. It checks every recorded file
  against its recorded checksum, and checks that no data file went unrecorded.
- The *mint* is the moment the brokerage refresh token was issued. The *coverage
  assertion* adds the token's lifetime to the mint and requires the sum to clear the
  week's last option close.

Nine operational wall-clock times live here as named integer constants, in order. They
are not session times. The session times come from the calendar. These are the moments
the design pins to the machine's clock, so launchd and pmset can fire them.

1. The 08:25 weekday firmware wake.
2. The 08:30 weekday pre-open self-check.
3. The 09:35 weekday says-closed-but-open calendar probe.
4. The 18:30 weekday vendor sweep, whose Friday run sets the Sunday one-shot.
5. The weekday assertion end near 18:45, when the vendor sweep's ping lands.
6. The 19:55 Sunday one-shot wake.
7. The 20:00 Sunday canary and maintenance job.
8. The 23:00 Sunday canary cutoff, the last retry the canary attempts.
9. The 23:30 Sunday assertion end, the ``sunday`` check's deadline. It sits half an hour
   past the cutoff so the last retry finishes inside the power assertion.

The re-auth reminder's hours derive from the last two rather than adding constants of
their own. Every one of these is a pair of integers, never a ``"HH:MM"`` string, so
the session-time enforcement scanner stays green. That scanner is the test that fails
the build on a hardcoded session time anywhere under ``src/lake`` outside the calendar
module.

One design caveat governs the Sunday read-back. A fired ``pmset`` one-shot leaves the
schedule. By Sunday 20:00 the 19:55 wake has fired, so a fired one-shot and a never-set
one look the same. Friday's sweep is the read-back that proves the one-shot landed.
The alarm check here therefore expects the one-shot only between the Friday sweep
that sets it and its own firing. Before that Friday nothing has set it, so a Monday
catch-up run reports nothing missing. The weekday repeat alarm is always expected.

Every seam is injected: the clock, the calendar, the daemon probe, the schedule reader,
the pinger, the canary, the alert transport, and the caffeinate runner. The whole module
runs offline in a test. Two of those seams reach the outside world when they fall back
to their production default. The canary quotes one symbol through the vendor, and the
transport POSTs the re-auth reminder to ntfy. Both are built by the ``sunday`` command
line alone, so a test that drives it passes its own for each.
"""

from __future__ import annotations

import json
import math
import re
import shlex
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lake.alert import Message, NtfyTransport, Publisher, Transport
from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock
from lake.config import input_errors_exit, load_config
from lake.manifest import ScrubResult, scrub
from lake.paths import TOKEN_FILE, config_dir
from lake.runner import PING_FAILURES, LaunchdJob, Pinger, UrllibPinger, calendar_interval
from lake.vendor import Vendor

# -- the wall-clock constants ------------------------------------------------


@dataclass(frozen=True)
class WallClockTime:
    """A local-time moment the design pins to the machine's clock.

    It is a pair of integers. It becomes a datetime only when placed on a date, and a
    string only when a command line needs one. So no ``"HH:MM"`` literal exists in
    source.
    """

    hour: int
    minute: int

    def __post_init__(self) -> None:
        calendar_interval(self.hour, self.minute)  # range-checks both

    def on(self, day: date) -> datetime:
        """This moment on ``day``, Eastern-time aware."""
        return datetime(day.year, day.month, day.day, self.hour, self.minute, tzinfo=MARKET_TZ)

    @property
    def hms(self) -> str:
        """The ``pmset`` argument form, 24-hour with seconds."""
        return f"{self.hour:02d}:{self.minute:02d}:00"

    @property
    def sudoers_hms(self) -> str:
        """The ``hms`` form as a plain sudoers argument, where a colon needs a backslash."""
        return self.hms.replace(":", "\\:")

    def launchd_interval(self, weekday: int) -> dict[str, int]:
        """One ``StartCalendarInterval`` entry on one launchd weekday number."""
        return {**calendar_interval(self.hour, self.minute), "Weekday": weekday}


# The operational wall-clock times, per the design's deployment section. These are
# machine-clock moments, not session times. launchd and pmset can only fire on the
# wall clock, so the design pins them there.
WEEKDAY_WAKE = WallClockTime(8, 25)  # pmset repeat wakeorpoweron MTWRF
PRE_OPEN_SELF_CHECK = WallClockTime(8, 30)  # the self-check launchd job, Mon-Fri
CALENDAR_PROBE = WallClockTime(9, 35)  # the says-closed-but-open probe, Mon-Fri
VENDOR_SWEEP = WallClockTime(18, 30)  # the sweep job, which sets the Sunday one-shot
WEEKDAY_ASSERTION_END = WallClockTime(18, 45)  # when the vendor sweep's ping lands
SUNDAY_WAKE = WallClockTime(19, 55)  # the Friday-set one-shot wake
SUNDAY_MAINTENANCE = WallClockTime(20, 0)  # the canary + scrub launchd job
CANARY_DEADLINE = WallClockTime(23, 0)  # the canary's last retry, not the check's deadline
SUNDAY_ASSERTION_END = WallClockTime(23, 30)  # when that last retry's ping must have landed

# The design sends the re-auth reminder on the hour, from the maintenance run through
# the hour before the deadline. So 20:00, 21:00, and 22:00, and never on the half hours
# the canary also retries on. The reminder is quieter than the retry.
REMINDER_HOURS = tuple(range(SUNDAY_MAINTENANCE.hour, CANARY_DEADLINE.hour))

# The reminder's wire shape, per the design's message table. Priority 3 is the
# reminder tier: a short vibration, not a page.
REMINDER_TITLE = "Sunday re-auth due"
REMINDER_PRIORITY = 3

# Schwab's refresh token lives this long. The coverage assertion adds it to the mint.
TOKEN_LIFETIME = timedelta(days=7)

# launchd's weekday numbering: 0 is Sunday, 1 through 5 are Monday through Friday.
# Python's ``date.weekday()`` runs Monday=0 through Sunday=6. Both appear below, each
# named at its use.
LAUNCHD_WEEKDAYS = (1, 2, 3, 4, 5)
LAUNCHD_SUNDAY = 0
_PY_WEEKDAYS = frozenset({0, 1, 2, 3, 4})
_PY_SATURDAY = 5
_PY_SUNDAY = 6

# The launchd labels. A label is the job's unique identity to launchd. All five sit in
# the system domain because they are LaunchDaemons.
LAUNCHD_DOMAIN = "system"
DAEMON_LABEL = "com.marketlake.daemon"
DASHBOARD_LABEL = "com.marketlake.dashboard"
SELF_CHECK_LABEL = "com.marketlake.self-check"
CALENDAR_PROBE_LABEL = "com.marketlake.calendar-probe"
SUNDAY_LABEL = "com.marketlake.sunday"

# The healthchecks slugs the two calendar jobs ping. Log the slug, never the URL.
CALENDAR_PROBE_SLUG = "calendar-probe"
PRE_OPEN_SLUG = "pre-open"
SUNDAY_SLUG = "sunday"

# The system PATH a LaunchDaemon gets. launchd gives a job a minimal environment, so
# the plist restores the OS tool directories the jobs shell out to: pmset, launchctl,
# tmutil, caffeinate, and rsync. These are OS locations, not machine-specific paths.
_SYSTEM_PATH = ("/usr/bin", "/bin", "/usr/sbin", "/sbin")

# Directories the dry-run renderer refuses to write into. Installing is the operator's
# step, by hand, with the printed commands. Both /etc spellings are listed. On macOS
# /etc is a symlink that resolve() folds into /private/etc, and on a host where /etc is
# a real directory the /etc entry is the one that catches it.
_PROTECTED_ROOTS = (Path("/Library"), Path("/System"), Path("/etc"), Path("/private/etc"))

# The one rendered setup file beside the five plists. The Time Machine exclusion is a
# printed install step rather than a rendered script, so there is one copy of it.
SUDOERS_FILE = "marketlake.sudoers"

# The script the operator runs for steps 1 to 5. The renderer writes it and never
# runs it, per the build plan's D14.
INSTALL_SCRIPT_FILE = "install.sh"

# The reverse of install.sh. It takes off the five jobs, the weekday wake, the sudoers
# drop-in and the five plists, in that order. It leaves the lake, the config directory
# and that directory's Time Machine exclusion, so an uninstall is neither a data loss
# nor a re-auth, and it does not expose the token to the next backup.
UNINSTALL_SCRIPT_FILE = "uninstall.sh"

# Restarts a resident job so it picks up new code. Only the two KeepAlive jobs can go
# stale: each holds the Python it imported at start, and the working tree can move under
# it. The calendar jobs exec fresh on every fire, so they never need this.
RESTART_SCRIPT_FILE = "restart.sh"


# -- the host description and the plists -------------------------------------


@dataclass(frozen=True)
class LaunchdHost:
    """The machine values every generated plist carries. All caller-supplied.

    ``python`` is the interpreter path, ``owner`` the account the jobs run as, ``home``
    that account's home directory, ``project_dir`` the working directory, and
    ``log_dir`` where stdout and stderr land. ``group`` defaults to ``staff``, macOS's
    primary group for every local account. It is a group name, not an account.
    """

    python: str
    owner: str
    home: str
    project_dir: str
    log_dir: str
    group: str = "staff"
    config_path: str | None = None

    def environment(self) -> dict[str, str]:
        """The ``EnvironmentVariables`` block every job shares."""
        env = {
            "HOME": self.home,
            "PATH": ":".join(_SYSTEM_PATH),
            "PYTHONUNBUFFERED": "1",
        }
        if self.config_path is not None:
            env["MARKETLAKE_CONFIG"] = self.config_path
        return env

    def log_path(self, label: str, stream: str) -> str:
        """The log file for one job's stdout or stderr, under ``log_dir``."""
        return str(Path(self.log_dir) / f"{label}.{stream}.log")

    def job(
        self,
        label: str,
        module: str,
        *args: str,
        calendar: dict[str, int] | list[dict[str, int]] | None = None,
        keep_alive: bool = False,
        run_at_load: bool = False,
    ) -> LaunchdJob:
        """One job of this host running ``python -m <module> <args>``.

        ``run_at_load`` defaults off. launchd runs a job once at load when it is on,
        which is right for a resident process and wrong for work that belongs to a
        moment. Each caller states which it is.
        """
        return LaunchdJob(
            label=label,
            program_arguments=(self.python, "-m", module, *args),
            calendar_interval=calendar,
            working_directory=self.project_dir,
            standard_out_path=self.log_path(label, "out"),
            standard_error_path=self.log_path(label, "err"),
            environment=self.environment(),
            run_at_load=run_at_load,
            user_name=self.owner,
            group_name=self.group,
            keep_alive=keep_alive,
        )


def daemon_job(host: LaunchdHost) -> LaunchdJob:
    """The capture daemon, a permanent resident under ``KeepAlive``.

    It runs ``python -m lake.daemon``, the slice-2 loop. It never exits on its own.
    Outside sessions it idles and heartbeats. So it has no calendar interval at all.
    ``RunAtLoad`` starts it as soon as the plist is bootstrapped and after every boot,
    which is what makes the 08:25 firmware wake reach a running daemon.
    """
    return host.job(DAEMON_LABEL, "lake.daemon", keep_alive=True, run_at_load=True)


def dashboard_job(host: LaunchdHost) -> LaunchdJob:
    """The read-only localhost query service, the second resident under ``KeepAlive``.

    It starts at load for the same reason the daemon does.
    """
    return host.job(DASHBOARD_LABEL, "lake.dashboard", keep_alive=True, run_at_load=True)


def self_check_job(host: LaunchdHost) -> LaunchdJob:
    """The weekday pre-open self-check, five minutes after the firmware wake.

    ``RunAtLoad`` is deliberately on. A load during the day runs the check once at
    load, which is harmless and pings only if the daemon is up.
    """
    return host.job(
        SELF_CHECK_LABEL,
        "lake.control_plane",
        "self-check",
        calendar=[PRE_OPEN_SELF_CHECK.launchd_interval(wd) for wd in LAUNCHD_WEEKDAYS],
        run_at_load=True,
    )


def calendar_probe_job(host: LaunchdHost) -> LaunchdJob:
    """The 09:35 says-closed-but-open probe, five minutes after the open.

    The calendar is the daemon's only authority on whether a session exists, so a day
    the calendar calls closed is a day nothing captures. If the market is in fact open,
    that is a whole session lost and nothing else notices, because every other check
    agrees with the calendar.

    ``RunAtLoad`` is off. A load at any other hour would make one vendor call for a
    question only 09:35 can answer.
    """
    return host.job(
        CALENDAR_PROBE_LABEL,
        "lake.probe_calendar",
        calendar=[CALENDAR_PROBE.launchd_interval(wd) for wd in LAUNCHD_WEEKDAYS],
    )


def sunday_job(host: LaunchdHost) -> LaunchdJob:
    """The Sunday canary and maintenance job, five minutes after the one-shot wake.

    The token path is derived from the host's home, the one place every consumer
    derives it from. The daemon reads that file, this job asserts coverage over it,
    and the Time Machine exclusion protects the directory holding it. No render
    argument can point any of the three somewhere else, so they cannot split. The
    18:30 vendor sweep is not rendered here. That job is slice 3's, and it does not
    exist yet.

    ``RunAtLoad`` is deliberately off, as it is for the calendar probe. It would
    otherwise run at every bootstrap and every boot, on any day. That means a
    full integrity scrub of the lake each time, and a coverage assertion on a day the
    design never asks about. A healthy weekday reboot would pass every check and ping
    the ``sunday`` slug midweek, which is not what that check watches. Turning it off
    costs nothing the design asks for. launchd still fires a missed Sunday occurrence
    on the next wake, which is the backstop the pmset table names, and that coalescing
    has nothing to do with ``RunAtLoad``.
    """
    return host.job(
        SUNDAY_LABEL,
        "lake.control_plane",
        "sunday",
        "--token",
        default_token_path(host.home),
        calendar=SUNDAY_MAINTENANCE.launchd_interval(LAUNCHD_SUNDAY),
    )


def all_jobs(host: LaunchdHost) -> tuple[LaunchdJob, ...]:
    """Every launchd job the control plane installs, resident processes first."""
    return (
        daemon_job(host),
        dashboard_job(host),
        self_check_job(host),
        calendar_probe_job(host),
        sunday_job(host),
    )


# -- the pre-open self-check ---------------------------------------------------

# Whether the daemon behind a launchd label is running. The real one shells out to
# ``launchctl``. A test injects a callable.
DaemonProbe = Callable[[str], bool]


def parse_launchctl_print(output: str) -> bool:
    """Whether a ``launchctl print`` dump describes a running service.

    The dump is a block of ``key = value`` lines. A loaded, running service carries
    ``state = running``. A loaded but idle or crashed one carries another state. A
    service launchd does not know is an error exit, which the probe handles before
    parsing.
    """
    for line in output.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "state" and value.strip() == "running":
            return True
    return False


def launchctl_probe(label: str) -> bool:
    """The real probe: ``launchctl print <domain>/<label>``, parsed for a running state.

    It runs from the rendered self-check job every weekday morning, and from the
    by-hand live check. A test injects a fake instead.
    """
    import subprocess  # lazy: only a real run shells out

    result = subprocess.run(
        ["launchctl", "print", f"{LAUNCHD_DOMAIN}/{label}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and parse_launchctl_print(result.stdout)


@dataclass(frozen=True)
class SelfCheckOutcome:
    """What one pre-open self-check found and did."""

    daemon_up: bool
    pinged: bool
    problem: str | None = None


def self_check(
    *,
    probe: DaemonProbe,
    pinger: Pinger,
    ping_url: str,
    label: str = DAEMON_LABEL,
) -> SelfCheckOutcome:
    """Verify the daemon is up, and ping the pre-open check only then.

    The self-check's ping means awake-and-daemon-up. A missed ping means the 08:25
    wake failed, paged a full hour before the bell. So the ping fires only on the
    success condition. A down daemon exits without pinging. A raising probe
    propagates, which is also a non-ping.

    A ping that fails is named rather than raised. The outcome is the same missed
    ping either way, and healthchecks pages for it after the grace. The difference is
    that the caller still gets to say what happened, instead of the job dying with a
    traceback where its one summary line should be.
    """
    up = probe(label)
    if not up:
        return SelfCheckOutcome(daemon_up=False, pinged=False)
    try:
        pinger.ping(ping_url)
    except PING_FAILURES as exc:
        problem = f"ping failed: {type(exc).__name__}"
        return SelfCheckOutcome(daemon_up=True, pinged=False, problem=problem)
    return SelfCheckOutcome(daemon_up=True, pinged=True)


# -- the pmset schedule: rendering and read-back ------------------------------


def _pmset_date(day: date) -> str:
    """A date in pmset's argument form, ``MM/dd/yy`` per its man page."""
    return f"{day.month:02d}/{day.day:02d}/{day.year % 100:02d}"


def pmset_repeat_command() -> str:
    """The weekday firmware wake, set once at setup. The exact string the design pins.

    It fires at local wall-clock time, holidays included. The daemon idles on a
    holiday while heartbeating, which keeps the dead-man timer fed. ``pmset repeat``
    holds only one repeating alarm, so the Sunday wake is a one-shot instead.
    """
    return f"pmset repeat wakeorpoweron MTWRF {WEEKDAY_WAKE.hms}"


def pmset_schedule_command(sunday: date) -> str:
    """The Sunday one-shot wake for the canary, the exact string the design pins.

    ``sunday`` must be a Sunday. The Friday vendor sweep will call this with the date
    from ``next_sunday_wake``. Whether the firmware one-shot is wall-clock or
    epoch-based across a DST weekend is live check 5. The canary's retry window
    absorbs an hour of skew either way, so nothing here compensates.
    """
    if sunday.weekday() != _PY_SUNDAY:
        raise ValueError(f"{sunday.isoformat()} is not a Sunday")
    return f'pmset schedule wakeorpoweron "{_pmset_date(sunday)} {SUNDAY_WAKE.hms}"'


def _next_monday(today: date) -> date:
    """The Monday strictly after ``today``. A Monday gives the following one."""
    return today + timedelta(days=7 - today.weekday())


def _first_session_on_or_after(day: date, calendar: Calendar, limit_days: int = 14) -> date:
    """The first session on or after ``day``, bounded so a dark calendar cannot spin."""
    for offset in range(limit_days):
        candidate = day + timedelta(days=offset)
        if calendar.is_session(candidate):
            return candidate
    raise ValueError(f"no session within {limit_days} days of {day.isoformat()}")


def next_sunday_wake(now: datetime, calendar: Calendar) -> date:
    """The Sunday before the next session week, the date the one-shot wake targets.

    The next session week is the Monday-to-Friday span, strictly after this week,
    that holds the next session. A Monday holiday leaves the week where it is. Its
    first session is Tuesday, in the same week, so the Sunday before is unchanged.
    Only a fully dark week would push the wake a week out. From a Friday the answer
    is the Sunday two days ahead. From a Sunday it is that same Sunday.
    """
    today = now.astimezone(MARKET_TZ).date()
    first = _first_session_on_or_after(_next_monday(today), calendar)
    monday = first - timedelta(days=first.weekday())
    return monday - timedelta(days=1)


def sunday_wake_command(now: datetime, calendar: Calendar) -> str:
    """The one-shot command for the next Sunday wake still ahead of ``now``.

    ``next_sunday_wake`` answers with today on a Sunday, which is what the read-back
    wants. A command is different. On a Sunday at or after 19:55 that wake has already
    fired, and scheduling a moment in the past sets nothing, so the answer advances to
    the following Sunday.

    The ``pmset`` subcommand is the only caller. It prints this line for the operator to
    run by hand every Friday. Nothing in this deliverable sets the one-shot, and the
    slice-3 sweep that will set it does not exist yet. Install step 6 names the gap, and
    the design's DST-weekend live check needs the same line.
    """
    sunday = next_sunday_wake(now, calendar)
    if SUNDAY_WAKE.on(sunday) <= now:
        sunday = next_sunday_wake(now + timedelta(days=1), calendar)
    return pmset_schedule_command(sunday)


# pmset takes the event type as ``wakeorpoweron`` on the command line, and both that
# spelling and ``wakepoweron`` appear in the binary's own strings. Which section prints
# which is not settled here, so the parser accepts either in either section and the
# tests cover both. Live check 4, the by-hand read-back, is what confirms the printed
# form on the real machine.
WAKE_KINDS = frozenset({"wakeorpoweron", "wakepoweron"})

_REPEAT_LINE = re.compile(r"^(?P<kind>\w+)\s+at\s+(?P<time>\S+)\s+(?P<days>.+?)\s*$")
# A one-shot line carries up to three optional tails, in this order: the owner, the
# leeway pmset allows the firing, and whether the event is user-visible. pmset appends
# each one only when it applies, and it prints every owner's events, not just this
# project's. So a foreign event's tail must parse rather than fail the whole read-back.
_ONE_SHOT_LINE = re.compile(
    r"^\[\d+\]\s+(?P<kind>\w+)\s+at\s+(?P<date>\S+)\s+(?P<time>\S+)"
    r"(?:\s+by\s+'[^']*')?"
    r"(?:\s+leeway\s+secs:\s*-?\d+)?"
    r"(?:\s+User\s+visible:\s*\S+)?"
    r"\s*$"
)
_CLOCK = re.compile(r"^(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?\s*(?P<ampm>[AaPp][Mm])?$")
_DATE = re.compile(r"^(?P<mo>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{2}|\d{4})$")
_DAY_NAMES = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_DAY_LETTERS = {"M": 0, "T": 1, "W": 2, "R": 3, "F": 4, "S": 5, "U": 6}


class PmsetParseError(ValueError):
    """Raised when a ``pmset -g sched`` line has a shape the parser does not know."""


@dataclass(frozen=True)
class RepeatAlarm:
    """One repeating power event. ``weekdays`` uses Python numbering, Monday=0.

    ``weekdays`` is ``None`` when pmset printed a day mask it has no name for. The
    alarm exists and its days are unknown, which is drift either way.
    """

    kind: str
    hour: int
    minute: int
    weekdays: frozenset[int] | None


@dataclass(frozen=True)
class OneShotAlarm:
    """One scheduled power event. ``when`` is naive local time, as pmset prints it.

    pmset also prints who set the alarm. The line is matched so it parses, and the owner
    is not kept, because no check asks who set one. Only the time and the kind decide
    whether the wake the design pins is present.
    """

    kind: str
    when: datetime


@dataclass(frozen=True)
class PmsetSchedule:
    """The parsed ``pmset -g sched`` output."""

    repeats: tuple[RepeatAlarm, ...] = ()
    one_shots: tuple[OneShotAlarm, ...] = ()


def _parse_clock(text: str) -> tuple[int, int]:
    match = _CLOCK.match(text)
    if match is None:
        raise PmsetParseError(f"unrecognized time: {text!r}")
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    ampm = match.group("ampm")
    # The regex shapes the digits but not their range, and the caller's contract is that
    # an unreadable line raises PmsetParseError rather than a bare ValueError from
    # ``datetime``. Check the printed hour, before the 12-hour fold, or ``24:00AM``
    # folds to a valid zero and passes.
    high = 12 if ampm is not None else 23
    if not (0 <= hour <= high and 0 <= minute < 60):
        raise PmsetParseError(f"time out of range: {text!r}")
    if ampm is not None:
        hour %= 12
        if ampm.upper() == "PM":
            hour += 12
    return hour, minute


def _parse_date(text: str) -> date:
    match = _DATE.match(text)
    if match is None:
        raise PmsetParseError(f"unrecognized date: {text!r}")
    year = int(match.group("y"))
    if year < 100:
        year += 2000
    try:
        return date(year, int(match.group("mo")), int(match.group("d")))
    except ValueError as exc:  # a regex-shaped but impossible calendar date
        raise PmsetParseError(f"date out of range: {text!r}") from exc


def _parse_days(text: str) -> frozenset[int] | None:
    """The day set a repeat line names, or ``None`` when pmset would not name it.

    pmset prints one of ``every day``, ``weekdays only``, ``weekends only``, or a
    single day's name. Every other mask prints as ``Some days``, which carries no day
    list at all. That is a drifted alarm rather than an unreadable line, so it parses
    as an unknown set and the alarm check names it.
    """
    lowered = text.lower()
    if "every day" in lowered:
        return frozenset(range(7))
    if "weekday" in lowered:
        return _PY_WEEKDAYS
    if "weekend" in lowered:
        return frozenset({_PY_SATURDAY, _PY_SUNDAY})
    if "some days" in lowered:
        return None
    days: set[int] = set()
    for token in text.split():
        if token.lower() in _DAY_NAMES:
            days.add(_DAY_NAMES[token.lower()])
        elif all(ch in _DAY_LETTERS for ch in token):
            days.update(_DAY_LETTERS[ch] for ch in token)
        else:
            raise PmsetParseError(f"unrecognized day list: {text!r}")
    return frozenset(days)


def parse_pmset_schedule(text: str) -> PmsetSchedule:
    """Parse ``pmset -g sched`` output into its repeating and one-shot alarms.

    The output has two labeled sections, ``Repeating power events:`` and
    ``Scheduled power events:``. A repeat line reads like ``wakepoweron at 8:25AM
    weekdays only``. A one-shot line reads like ``[0]  wakeorpoweron at 09/06/2026
    19:55:00 by 'pmset'``. Either wake spelling parses in either section. Empty output,
    or a ``No scheduled events`` line, parses as an empty schedule. Times in 12-hour or
    24-hour form and two- or four-digit years are all accepted, because the exact print
    form is not settled until live check 4 runs on the real machine.

    Two shapes come from pmset printing other owners' events beside this project's. A
    one-shot line may carry a leeway and a user-visible tail after its owner, and a
    repeat line whose day mask pmset has no name for prints ``Some days``. Both parse.
    A line this parser still does not know raises ``PmsetParseError``, and the Sunday
    job turns that into a report line rather than a withheld ping.
    """
    repeats: list[RepeatAlarm] = []
    one_shots: list[OneShotAlarm] = []
    section: str | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith("repeating power events"):
            section = "repeat"
            continue
        if lowered.startswith("scheduled power events"):
            section = "scheduled"
            continue
        if lowered.startswith("no scheduled") or lowered.startswith("no repeating"):
            continue
        if section == "repeat":
            match = _REPEAT_LINE.match(line)
            if match is None:
                raise PmsetParseError(f"unrecognized repeat line: {line!r}")
            hour, minute = _parse_clock(match.group("time"))
            repeats.append(
                RepeatAlarm(match.group("kind"), hour, minute, _parse_days(match.group("days")))
            )
        elif section == "scheduled":
            match = _ONE_SHOT_LINE.match(line)
            if match is None:
                raise PmsetParseError(f"unrecognized one-shot line: {line!r}")
            day = _parse_date(match.group("date"))
            hour, minute = _parse_clock(match.group("time"))
            when = datetime(day.year, day.month, day.day, hour, minute)
            one_shots.append(OneShotAlarm(match.group("kind"), when))
        else:
            raise PmsetParseError(f"line outside any section: {line!r}")
    return PmsetSchedule(tuple(repeats), tuple(one_shots))


@dataclass(frozen=True)
class AlarmCheck:
    """The verdict on the read-back schedule against the two expected alarms."""

    repeat_ok: bool
    one_shot_ok: bool
    problems: tuple[str, ...] = ()


def expected_one_shot(now: datetime, calendar: Calendar) -> date | None:
    """The Sunday whose one-shot wake should be pending at ``now``, or ``None``.

    The wake is expected only between the two moments that bound its life. The Friday
    18:30 sweep sets it, and its own firing takes it back out of the schedule.

    Before that Friday nothing has set it, so an absent alarm is not drift. A Monday
    catch-up run, after launchd coalesced a wake missed over the weekend, would
    otherwise report the coming Sunday's wake as missing five days early. From the
    Sunday job at 20:00 the wake fired five minutes earlier, so nothing is expected
    then either.
    """
    sunday = next_sunday_wake(now, calendar)
    if SUNDAY_WAKE.on(sunday) <= now:
        return None
    # The Friday before a Sunday is always that Sunday minus two days, and the sweep
    # runs every weekday, holidays included, so no calendar lookup is needed.
    return sunday if VENDOR_SWEEP.on(sunday - timedelta(days=2)) <= now else None


def _format_days(weekdays: frozenset[int] | None) -> str:
    """A repeat alarm's day set for a report line, in Python numbering, Monday=0."""
    return "a mask pmset does not name" if weekdays is None else str(sorted(weekdays))


def check_alarms(schedule: PmsetSchedule, *, one_shot_date: date | None) -> AlarmCheck:
    """Whether the weekday repeat alarm and the pending Sunday one-shot are present.

    The repeat alarm must be a wake-or-power-on at the weekday wake time on exactly
    Monday through Friday. With a ``one_shot_date``, a wake-or-power-on one-shot must
    sit at the Sunday wake time on that date. A wrong time, a wrong day set, or an
    absent alarm is a named problem. With no ``one_shot_date`` the one-shot is not
    expected, per the fired-one-shot caveat, and passes.
    """
    problems: list[str] = []

    wakes = [alarm for alarm in schedule.repeats if alarm.kind in WAKE_KINDS]
    repeat_ok = any(
        alarm.hour == WEEKDAY_WAKE.hour
        and alarm.minute == WEEKDAY_WAKE.minute
        and alarm.weekdays == _PY_WEEKDAYS
        for alarm in wakes
    )
    if not repeat_ok:
        if wakes:
            found = ", ".join(
                f"{a.hour:02d}:{a.minute:02d} on {_format_days(a.weekdays)}" for a in wakes
            )
            problems.append(f"weekday wake repeat alarm drifted: found {found}")
        else:
            problems.append("weekday wake repeat alarm missing")

    one_shot_ok = True
    if one_shot_date is not None:
        expected = SUNDAY_WAKE.on(one_shot_date).replace(tzinfo=None)
        shots = [alarm for alarm in schedule.one_shots if alarm.kind in WAKE_KINDS]
        one_shot_ok = any(alarm.when == expected for alarm in shots)
        if not one_shot_ok:
            if shots:
                found = ", ".join(a.when.isoformat(timespec="minutes") for a in shots)
                problems.append(f"sunday one-shot wake drifted: found {found}")
            else:
                problems.append(f"sunday one-shot wake missing for {one_shot_date.isoformat()}")

    return AlarmCheck(repeat_ok, one_shot_ok, tuple(problems))


def read_pmset_schedule() -> str:
    """The real schedule reader: ``pmset -g sched``, read-only, no root.

    It runs from the rendered Sunday job every week, and from the by-hand live check.
    A test injects a fake returning text.
    """
    import subprocess  # lazy: only a real run shells out

    return subprocess.run(
        ["pmset", "-g", "sched"], capture_output=True, text=True, check=True
    ).stdout


# -- the caffeinate assertion window -------------------------------------------


@dataclass(frozen=True)
class AssertionWindow:
    """The span during which the daemon must hold a ``caffeinate`` assertion."""

    start: datetime
    end: datetime

    def contains(self, when: datetime) -> bool:
        return self.start <= when < self.end


def assertion_window(day: date) -> AssertionWindow | None:
    """The assertion window for ``day``, or ``None`` on a Saturday.

    The window is open whenever any healthchecks expectation window is open. On a
    weekday that runs from the firmware wake until the vendor sweep's ping lands,
    session or not. A holiday is exactly when the idle heartbeats must keep flowing,
    so the calendar deliberately does not enter here. On Sunday it runs from the
    one-shot wake until the sunday check's deadline. That is half an hour past the canary's
    last retry, so the retry finishes inside the hold rather than racing it. Saturday
    owes nothing.
    """
    weekday = day.weekday()
    if weekday in _PY_WEEKDAYS:
        return AssertionWindow(WEEKDAY_WAKE.on(day), WEEKDAY_ASSERTION_END.on(day))
    if weekday == _PY_SUNDAY:
        return AssertionWindow(SUNDAY_WAKE.on(day), SUNDAY_ASSERTION_END.on(day))
    return None


def caffeinate_args(window: AssertionWindow, now: datetime) -> tuple[str, ...] | None:
    """The ``caffeinate -i -t <seconds>`` line that holds idle-sleep off until the window ends.

    ``-i`` prevents idle sleep only. A closed lid still sleeps, which is why the
    design's posture is lid open on AC. The seconds run from ``now`` to the window's
    end, rounded up. A call before the window starts holds early, which costs nothing
    on AC. A call at or past the end returns ``None``: nothing is owed.

    The span is measured in absolute time. Both operands carry the same Eastern
    ``tzinfo`` object, and subtracting those compares wall clocks and ignores an offset
    change between them. On the November Sunday that repeats an hour, that reads an
    hour short, and the assertion would lapse before the canary deadline.
    """
    remaining = timedelta(seconds=window.end.timestamp() - now.timestamp())
    if remaining <= timedelta(0):
        return None
    return ("caffeinate", "-i", "-t", str(math.ceil(remaining.total_seconds())))


# Starts the caffeinate process. The real one is ``subprocess.Popen``. A test injects
# a callable that records the arguments.
AssertionRunner = Callable[[Sequence[str]], object]


def _spawn(args: Sequence[str]) -> object:
    import subprocess  # lazy: only the live daemon spawns

    return subprocess.Popen(list(args))


class AssertionHolder:
    """Holds one ``caffeinate`` assertion per expectation window.

    ``caffeinate -i -t <seconds>`` releases itself when its timer runs out, so the
    daemon spawns one process per window and never has to kill one. This remembers the
    window it last held, so the minutely caller spawns nothing until the next window
    opens. Without that memory a per-tick call would leave one caffeinate process per
    minute, each holding its own assertion until the window ended.

    The hold starts when the window does, never before. Holding early would be
    harmless on AC, but a daemon that started at midnight would keep the machine awake
    for the eight hours before the wake, which is not what the design asks for.
    """

    def __init__(self, *, runner: AssertionRunner | None = None) -> None:
        self._runner = runner if runner is not None else _spawn
        self._held: AssertionWindow | None = None

    def hold(self, now: datetime) -> tuple[str, ...] | None:
        """Hold the assertion for the window ``now`` sits in, if one is open and unheld.

        Returns the arguments the runner was handed, or ``None`` when nothing was owed:
        no window today, the window not open yet or already over, or this window
        already held.
        """
        eastern = now.astimezone(MARKET_TZ)
        window = assertion_window(eastern.date())
        if window is None or window == self._held or not window.contains(eastern):
            return None
        args = caffeinate_args(window, eastern)
        if args is None:  # pragma: no cover - contains() already excludes the end
            return None
        self._runner(args)
        self._held = window
        return args


# -- the token coverage assertion ------------------------------------------------


def week_option_close(now: datetime, calendar: Calendar) -> datetime:
    """The last option close of the week the token must still cover.

    That week is the one holding the next session whose option close is still ahead.
    From the Sunday job it is tomorrow's week, which is the design's coming Friday.
    From a Monday catch-up run, after launchd coalesced a wake missed over the
    weekend, it is this week, so the token minted the evening before clears it. That
    is what makes the design's Monday backstop able to pass. Anchoring on the Monday
    strictly after today instead would judge next week from every weekday, and a
    fresh token can never cover a week it will not live to see.

    Past the last session's option close the answer moves to the following week. A
    Good Friday makes Thursday the last session, so the week is walked back from
    Friday to the first session found.
    """
    today = now.astimezone(MARKET_TZ).date()
    first = _first_session_on_or_after(today, calendar)
    if calendar.option_close(first) <= now:
        first = _first_session_on_or_after(first + timedelta(days=1), calendar)
    monday = first - timedelta(days=first.weekday())
    for offset in range(4, -1, -1):
        day = monday + timedelta(days=offset)
        if calendar.is_session(day):
            return calendar.option_close(day)
    raise ValueError(f"no session in the week of {monday.isoformat()}")


def token_covers_week(mint: datetime, now: datetime, calendar: Calendar) -> bool:
    """The coverage assertion: mint plus the token lifetime must clear the week's close.

    Validity is not freshness. A leftover token from a late prior-week ritual is still
    valid on Sunday and still dies mid-week. This catches it. ``mint`` and ``now`` must
    be timezone-aware.
    """
    if mint.tzinfo is None or now.tzinfo is None:
        raise ValueError("mint and now must be timezone-aware")
    return mint + TOKEN_LIFETIME > week_option_close(now, calendar)


# -- the Sunday maintenance job ----------------------------------------------------

# The canary's authenticated call. It returns whether the call succeeded. The production
# one is ``token_canary``, which quotes one symbol through the real vendor. A test
# injects its own. There is deliberately no default: a seam whose default answers True
# without calling anything turns the weekend's only auth check into a rubber stamp.
CanaryCall = Callable[[], bool]

# Returns the ``pmset -g sched`` text. The real one shells out. A test injects one.
ScheduleReader = Callable[[], str]

# The symbol the canary quotes. One batched quote is the cheapest authenticated call the
# vendor offers, and the body is thrown away. SPY is the standing exemplar across this
# repo, including the by-hand probe's own default. The roster is deliberately not read:
# the canary proves the credentials still work, and a roster that will not load is a
# different failure that must not reach the phone as a dead token.
CANARY_SYMBOL = "SPY"

# Builds the vendor the canary calls. The production one is ``_schwab_vendor``. A test
# passes its own, so no test builds a real client and none reaches the network.
VendorFactory = Callable[..., Vendor]


def _schwab_vendor(token_path: str | Path, *, api_key: str, app_secret: str) -> Vendor:
    """The real Schwab vendor, built from the token file.

    The import is lazy, so importing the control plane never costs the vendor library.
    This is the only line in this module that can reach the network, and it runs from
    the installed Sunday job alone.
    """
    from lake.schwab import SchwabVendor  # lazy: the real client, production only

    return SchwabVendor.from_token(token_path, api_key=api_key, app_secret=app_secret)


def token_canary(
    *,
    token_path: str | Path,
    api_key: str,
    app_secret: str,
    symbol: str = CANARY_SYMBOL,
    vendor_factory: VendorFactory = _schwab_vendor,
) -> CanaryCall:
    """The Sunday canary: one throwaway authenticated call, answered as a bool.

    The vendor is rebuilt inside every call rather than once here. That is the rule the
    mint reader already follows, and it is what lets the 21:00 attempt see a re-login
    done at 20:40. A client built once would keep calling on the token the evening
    started with, and every retry would fail for a reason the ritual had already fixed.

    Any failure answers False. A dead refresh token raises ``VendorAuthError``, an
    unreachable vendor raises something else, and a call that did not come back has
    proved nothing either way. The canary exists to prove the brokerage credentials
    still work over a weekend, so True has to mean a call was made and answered.
    Reporting success without calling anything is the failure this producer removes, and
    a pass on a network error would put it straight back.

    A False costs no page on its own. The attempt repeats every thirty minutes until
    23:00, so a transient outage clears itself, and the reminder that does go out names
    the throwaway call as the half that failed rather than claiming the token is dead.

    The failure's class goes to the job's log, because it is what tells a dead token from
    a dead network and it is diagnosis rather than alert. Only the class is printed. The
    text of a vendor exception is the library's, and nothing built from a credential goes
    anywhere a person reads.
    """

    def call() -> bool:
        try:
            vendor = vendor_factory(token_path, api_key=api_key, app_secret=app_secret)
            reply = vendor.get_quotes([symbol])
        except Exception as exc:  # noqa: BLE001 - a failed call answers False, never raises
            print(f"sunday: canary call failed: {type(exc).__name__}")
            return False
        # A non-2xx is a fetch failure, the same rule the capture primitive holds. A 401
        # is the dead-token shape that arrives as a status rather than as a raise.
        if not 200 <= reply.status < 300:
            print(f"sunday: canary call returned http {reply.status}")
            return False
        return True

    return call


@dataclass(frozen=True)
class ReauthReminder:
    """One Sunday re-auth reminder, in the shape the design's message table pins.

    The body names which half failed and the token's mint date. That date is the one
    fact from the config directory a message may carry, because it is already journal
    metadata. Nothing else from that directory goes on the wire, so the message stays
    worthless to anyone reading the topic.
    """

    title: str
    body: str
    priority: int


# Sends one reminder. The production one is ``reminder_publisher``, which pushes through
# the alert publisher. This module decides whether a reminder is owed and what it says.
# Delivery is the publisher's. A test injects a recorder.
ReminderSink = Callable[[ReauthReminder], None]

# The reminder's event name. It names the design's message-table row, and it is what the
# publisher writes down when a reminder never leaves the laptop.
REMINDER_EVENT = "sunday_reauth"


def reminder_publisher(*, publisher: Publisher, clock: Clock) -> ReminderSink:
    """Push each Sunday re-auth reminder to the phone, and log the ones that did not go.

    The publisher is total. It never raises, it caps the day, and it writes every
    undelivered message to a dated file under ``reports/``. So an unreachable ntfy costs
    a line in the job's log and a file on disk, and the evening's retries carry on. That
    is the right trade. A Sunday job that died on a failed push would lose the scrub, the
    alarm read-back, and the check's own ping with it, and the missed ping would page at
    23:30 naming the wrong cause.

    Priority 3 is the reminder tier, so the push carries no tag. The design gives the
    emoji to a page alone.
    """

    def send(reminder: ReauthReminder) -> None:
        delivery = publisher.publish(
            Message(
                event=REMINDER_EVENT,
                title=reminder.title,
                body=reminder.body,
                priority=reminder.priority,
            ),
            now=clock.now(),
        )
        if not delivery.sent:
            kept = "written down" if delivery.recorded else "lost"
            print(f"sunday: reminder not sent: {delivery.reason}, {kept}")

    return send


def reauth_reminder(
    *,
    now: datetime,
    canary_passed: bool,
    covered: bool | None,
    mint: datetime | None,
) -> ReauthReminder | None:
    """The reminder this attempt owes, or ``None``.

    The design fires it on Sunday only, on the 20:00, 21:00, and 22:00 runs, while the
    throwaway call or the coverage assertion still fails. So it never fires midweek,
    never on the half-hour retries, and stops on its own once the ritual is done,
    because a passing attempt owes nothing. At most three go out in an evening.

    Reading it as "the ritual is not done yet" is what makes the mint date the useful
    fact. A stale date says the ritual was skipped. An unreadable one says the token
    file itself is the problem.

    The gate is the hour, not the exact minute. launchd fires a calendar job late, and
    a coalesced missed occurrence fires whenever the machine wakes, so the attempt grid
    ``sunday_run`` builds from its own start almost never lands on the minute. Matching
    the minute sent nothing at all on those evenings. Holding the count at three is
    ``sunday_run``'s job, which sends at most one reminder an hour.
    """
    eastern = now.astimezone(MARKET_TZ)
    if eastern.weekday() != _PY_SUNDAY or eastern.hour not in REMINDER_HOURS:
        return None
    failed = []
    if not canary_passed:
        failed.append("the throwaway call")
    if covered is not True:
        failed.append("the coverage assertion")
    if not failed:
        return None
    which = " and ".join(failed)
    minted = (
        "The token's mint time could not be read."
        if mint is None
        else f"Token minted {mint.astimezone(MARKET_TZ).date().isoformat()}."
    )
    return ReauthReminder(
        title=REMINDER_TITLE,
        body=f"{which[0].upper()}{which[1:]} failed. {minted}",
        priority=REMINDER_PRIORITY,
    )


@dataclass(frozen=True)
class SundayOutcome:
    """What one Sunday maintenance run found and did.

    ``problems`` are the findings that withhold the ping, plus the ping's own failure
    when it is reached and fails. The withholding ones are a missing lake root, a
    failed scrub, a failed canary, a token that does not cover the coming week, and a
    mint time that could not be read. The ping's failure is different in kind. It is
    recorded after the others have all passed, and it names why the ping did not land
    rather than why it was not attempted.

    ``report`` carries the report-tier findings. Today that is only pmset alarm drift.
    The design pins drift to the nightly report, because the pre-open self-check already
    catches a missed wake an hour before the bell, so drift never withholds the ping.

    ``covered`` is ``None`` when the mint time could not be read. That is a problem,
    never a skip. ``pinged`` is the success condition.
    """

    scrub: ScrubResult
    alarms: AlarmCheck
    canary_passed: bool
    covered: bool | None
    pinged: bool
    reminder: ReauthReminder | None = None
    problems: tuple[str, ...] = ()
    report: tuple[str, ...] = ()


def sunday_maintenance(
    *,
    lake_root: Path,
    now: datetime,
    calendar: Calendar,
    schedule_reader: ScheduleReader,
    pinger: Pinger,
    ping_url: str,
    canary: CanaryCall,
    mint: datetime | None = None,
    exclusion_targets: Sequence[str] = (),
    exclusion_reader: ExclusionReader | None = None,
) -> SundayOutcome:
    """Scrub, verify the wake alarms, run the canary, assert coverage, then ping.

    Every check runs and every finding is named, so one run reports all of them.
    The ping fires only when the scrub, the canary, and the coverage assertion pass.
    Alarm drift is checked and named in ``report`` but never withholds the ping. A
    read-back that cannot be run or parsed is named there too, for the same reason:
    the design routes the whole read-back step to the nightly report, and the
    pre-open self-check already catches a missed wake an hour before the bell.

    The Time Machine exclusion is checked the same way and reported the same way. A
    sticky exclusion is invisible once set and dies quietly if the item it marks is
    replaced, so something has to look. With no ``exclusion_reader`` the check does not
    run, which costs a report line and never a ping. The command line always passes
    one. The
    coverage assertion needs the token's mint time. ``mint`` is ``None`` when the
    caller could not read it, and that withholds the ping. A ping that is attempted
    and fails is named in ``problems`` rather than raised, so the run still reports
    the scrub, the canary, and the coverage verdict it just spent its time computing.
    A coverage assertion that never ran must not read as a pass.

    Five duties the design gives the Sunday run are not built here. Each is named so
    the gap is a decision rather than an oversight.

    1. Regenerate the weekday wake alarm when the machine's timezone has moved. That
       is a ``pmset`` write, so it needs the operator's sudoers grant at run time.
    2. Check ``exchange_calendars`` for a package update, per the design's provenance
       rule that the library learns schedule changes only through releases.
    3. Scrub the backup copy as well as the primary lake. That needs the backup target
       mounted, which the compaction job owns.
    4. Check the disk runway, free space over trailing growth, and flag the nightly
       report under a few weeks of headroom.
    5. Rotate the logs.

    ``canary`` has no default, so a caller cannot leave it out and be told the weekend's
    auth check passed. The production one is ``token_canary``.

    The canary's 30-minute retry until the deadline belongs to ``sunday_run``, not to
    this function. This function decides one attempt.
    """
    problems: list[str] = []

    root = Path(lake_root)
    if not root.is_dir():
        problems.append(f"lake root missing: {root}")
    result = scrub(root)
    if not result.ok:
        problems.append(
            "scrub failed: "
            f"missing={len(result.missing)} sha_mismatches={len(result.sha_mismatches)} "
            f"orphans={len(result.orphans)}"
        )

    # The design routes this whole step to the nightly report, so nothing it can raise
    # may withhold the ping. The reader is an injected seam that shells out in
    # production, and ``pmset -g sched`` lists every owner's events, in shapes the
    # parser does not all know yet. So the catch is deliberately broad. A read-back
    # that cannot be read is a report line naming what failed, never a silent pass and
    # never a page.
    try:
        schedule = parse_pmset_schedule(schedule_reader())
    except Exception as exc:
        alarms = AlarmCheck(
            repeat_ok=False,
            one_shot_ok=False,
            problems=(f"pmset read-back unreadable: {type(exc).__name__}: {exc}",),
        )
    else:
        alarms = check_alarms(schedule, one_shot_date=expected_one_shot(now, calendar))
    report = list(alarms.problems)

    if exclusion_reader is not None and exclusion_targets:
        try:
            states = parse_exclusions(exclusion_reader(exclusion_targets))
        except Exception as exc:
            report.append(f"time machine exclusion unreadable: {type(exc).__name__}: {exc}")
        else:
            for target in exclusion_targets:
                if not states.get(target, False):
                    report.append(f"not excluded from time machine: {target}")

    canary_passed = bool(canary())
    if not canary_passed:
        problems.append("canary call failed")

    covered: bool | None = None
    if mint is None:
        problems.append("token mint time unreadable, so the coverage assertion did not run")
    else:
        covered = token_covers_week(mint, now, calendar)
        if not covered:
            problems.append("token mint plus seven days does not clear the coming week")

    reminder = reauth_reminder(now=now, canary_passed=canary_passed, covered=covered, mint=mint)

    pinged = False
    if not problems:
        try:
            pinger.ping(ping_url)
            pinged = True
        except PING_FAILURES as exc:
            problems.append(f"ping failed: {type(exc).__name__}")
    return SundayOutcome(
        scrub=result,
        alarms=alarms,
        canary_passed=canary_passed,
        covered=covered,
        pinged=pinged,
        reminder=reminder,
        problems=tuple(problems),
        report=tuple(report),
    )


# The canary's retry cadence. The design re-runs the canary every 30 minutes until it
# passes or the deadline, so a re-login done after the 20:00 run still clears the check.
CANARY_RETRY = timedelta(minutes=30)

# Reads the token's mint time afresh, or ``None`` when it could not be read. The retry
# loop calls it once per attempt. That is what lets a mid-evening re-login be seen.
MintReader = Callable[[], datetime | None]


def sunday_run(
    *,
    lake_root: Path,
    clock: Clock,
    calendar: Calendar,
    schedule_reader: ScheduleReader,
    pinger: Pinger,
    ping_url: str,
    mint_reader: MintReader,
    canary: CanaryCall,
    retry: timedelta = CANARY_RETRY,
    exclusion_targets: Sequence[str] = (),
    exclusion_reader: ExclusionReader | None = None,
    reminder_sink: ReminderSink | None = None,
) -> list[SundayOutcome]:
    """Run the Sunday job, retrying until it passes or the canary deadline.

    The ritual can be done any time on Sunday evening. A 20:00 run that finds no fresh
    token must not strand a re-login done at 20:10, so the attempt repeats on the retry
    cadence until it passes or the deadline. The mint time and the canary are read
    afresh every attempt, which is the whole point. A retry reusing the first attempt's
    reading could never see the new token.

    launchd has no repeat-until key, so the retry lives here rather than in the plist.
    The loop returns on the first success, so a healthy Sunday makes one attempt and
    scrubs once. An evening that keeps failing scrubs again on each attempt. That is
    the price of keeping one attempt one decision, and it buys a re-check of an
    integrity failure that may have been a disk unplugged for a moment.

    Retrying is Sunday-evening behaviour alone. A run started outside that window makes
    one attempt and returns. That covers the Monday catch-up launchd fires for a wake
    missed over the weekend, where retrying all day would page nobody sooner. Every
    attempt comes back, in order, so the caller can report them all.

    ``reminder_sink`` is called unguarded, because surviving a failed delivery is the
    publisher's contract rather than every caller's. ``reminder_publisher`` is the
    production sink and holds that promise for this one. With no sink the reminder is
    still decided and still returned on the outcome, and nothing pushes it.
    """
    start = clock.now().astimezone(MARKET_TZ)
    in_the_window = start.weekday() == _PY_SUNDAY and SUNDAY_MAINTENANCE.on(start.date()) <= start
    stop = CANARY_DEADLINE.on(start.date()) if in_the_window else start

    outcomes: list[SundayOutcome] = []
    reminded: set[int] = set()
    while True:
        attempt_now = clock.now()
        outcome = sunday_maintenance(
            lake_root=lake_root,
            now=attempt_now,
            calendar=calendar,
            schedule_reader=schedule_reader,
            pinger=pinger,
            ping_url=ping_url,
            canary=canary,
            mint=mint_reader(),
            exclusion_targets=exclusion_targets,
            exclusion_reader=exclusion_reader,
        )
        # One reminder an hour. Later attempts in the same hour owe nothing, so the
        # outcome records only the one that went out.
        hour = attempt_now.astimezone(MARKET_TZ).hour
        if outcome.reminder is not None:
            if hour in reminded:
                outcome = replace(outcome, reminder=None)
            else:
                reminded.add(hour)
                if reminder_sink is not None:
                    reminder_sink(outcome.reminder)
        outcomes.append(outcome)
        if outcome.pinged:
            return outcomes
        # The cadence is measured from the first attempt, so a slow scrub shifts no
        # later attempt off the design's half-hour grid.
        following = start + retry * len(outcomes)
        if following > stop:
            return outcomes
        waited = (following - clock.now().astimezone(MARKET_TZ)).total_seconds()
        if waited > 0:
            clock.sleep(waited)


# -- the setup artifacts ------------------------------------------------------------

# A sudoers user field. Anything else would be a syntax hole in a root-level file.
_ACCOUNT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")

# Neither sudoers rule wildcards its argument, and the reason is the same for both.
# ``sudo`` joins a command's arguments into one string before matching, so a ``*``
# there spans whitespace and admits whatever follows the wake. Both writes keep
# reading arguments past the event they set. ``pmset repeat`` takes a second
# power-off event after the power-on event, and ``pmset schedule`` keeps parsing
# settings after the event it schedules. A wildcard on either rule therefore grants
# far more than the wake it is meant to allow.
#
# The weekday alarm's argument never changes, so its rule spells the argument out.
# The one-shot's date changes every week, so its rule pins the argument's shape
# instead, as a POSIX extended regular expression. ``sudoers`` reads an argument that
# starts with ``^`` and ends with ``$`` as one, a sudo 1.9.10 feature. The whole
# joined argument string is that one regular expression, so the two leading words sit
# inside it and ``[[:space:]]`` stands in for each separator. Written as a single word
# it is unambiguously a regular expression, which is the form the sudoers manual's own
# examples take. Inside one, no sudoers character needs a backslash, so the colons
# stay bare. The date pattern matches what ``_pmset_date`` renders and nothing wider.
# The closing anchor is what leaves no room for a trailing setting.
_SUDOERS_SPACE = "[[:space:]]"
_PMSET_DATE_PATTERN = "[0-9][0-9]/[0-9][0-9]/[0-9][0-9]"
_SCHEDULE_ARGS_REGEX = (
    f"^schedule{_SUDOERS_SPACE}wakeorpoweron{_SUDOERS_SPACE}"
    f"{_PMSET_DATE_PATTERN}{_SUDOERS_SPACE}{SUNDAY_WAKE.hms}$"
)


def sudoers_dropin(owner: str) -> str:
    """The ``/etc/sudoers.d`` drop-in granting ``owner`` exactly the two pmset writes.

    Neither rule wildcards its argument. The weekday alarm's argument is fixed, so the
    rule spells it out, carrying the backslash a colon needs in a plain sudoers
    argument. The one-shot's date changes weekly, so that rule pins the argument's
    shape with an anchored regular expression. Nothing else runs under sudo. The
    sleep-disabling write is deliberately absent: it is rejected by design and stays
    password-gated.
    """
    # ``ALL`` fits the account pattern and visudo accepts it, but sudoers reads it as the
    # reserved word matching every account, so the drop-in would hand the two writes to
    # every local user instead of the owner. The word is case-sensitive, so a real
    # account named ``all`` still passes.
    if _ACCOUNT.match(owner) is None or owner == "ALL":
        raise ValueError(f"not a valid account name for sudoers: {owner!r}")
    return (
        "# Marketlake: the two pmset writes the control plane needs, and nothing else.\n"
        "# Neither rule wildcards its argument. sudo joins a command's arguments into one\n"
        "# string before matching, so a wildcard there spans whitespace and admits what\n"
        "# follows the wake. Both writes keep reading arguments past the event they set.\n"
        "# The weekday alarm's argument is fixed, so it is spelled out. The Sunday\n"
        "# one-shot's date changes weekly, so that rule is an anchored regular\n"
        "# expression. It pins the date's shape and the wake time, and the closing\n"
        "# anchor leaves no room for a trailing setting.\n"
        "# The sleep-disabling write is rejected by design and stays password-gated.\n"
        f"{owner} ALL=(root) NOPASSWD: /usr/bin/pmset repeat wakeorpoweron"
        f" MTWRF {WEEKDAY_WAKE.sudoers_hms}\n"
        f"{owner} ALL=(root) NOPASSWD: /usr/bin/pmset {_SCHEDULE_ARGS_REGEX}\n"
    )


# Returns the ``tmutil isexcluded`` output for the given paths. The real one shells
# out. A test injects one.
ExclusionReader = Callable[[Sequence[str]], str]

_EXCLUSION_LINE = re.compile(r"^\[(?P<state>[^\]]+)\]\s+(?P<path>.+?)\s*$")


def read_exclusions(targets: Sequence[str]) -> str:
    """The real reader: ``tmutil isexcluded``, read-only and no root."""
    import subprocess  # lazy: only a real run shells out

    return subprocess.run(
        ["tmutil", "isexcluded", *targets], capture_output=True, text=True, check=True
    ).stdout


def parse_exclusions(text: str) -> dict[str, bool]:
    """Parse ``tmutil isexcluded`` output into a path-to-excluded map.

    Each line reads ``[Excluded]`` or ``[Included]`` and then the path. Only the first
    means excluded. ``tmutil`` also prints ``[UNKNOWN]`` for a path it cannot resolve,
    which is not an exclusion either.
    """
    states: dict[str, bool] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        match = _EXCLUSION_LINE.match(line)
        if match is None:
            raise ValueError(f"unrecognized tmutil isexcluded line: {line!r}")
        states[match.group("path")] = match.group("state") == "Excluded"
    return states


def default_config_dir(home: str) -> str:
    """The config directory under ``home``. Machine-derived, never tracked."""
    return str(config_dir(home))


def default_token_path(home: str) -> str:
    """The token's standard location under ``home``. Machine-derived, never tracked."""
    return str(config_dir(home) / TOKEN_FILE)


def tmutil_exclusion_targets(config_dir: str, token_path: str) -> tuple[str, ...]:
    """What to keep out of Time Machine: the config directory, and a token outside it.

    The whole directory is excluded rather than the token file alone. Three reasons.

    1. ``config.yaml`` sits beside the token and holds four secrets of its own: the
       healthchecks ping key, the ntfy topic, and the two Schwab app credentials.
       Excluding only the token left those on a backup disk without FileVault.
    2. A sticky exclusion is an attribute on the item, so it dies when the item is
       deleted and re-created. ``config.yaml`` is hand-edited and most editors save by
       writing a temporary file and renaming over the original, which replaces the
       inode and drops the exclusion silently. A re-login re-creates ``token.json`` the
       same way. A directory's inode survives its files being rewritten.
    3. One line covers the files that arrive later, ``tickers.yaml`` and
       ``chain_plan.json``, without the list having to grow.

    The fixed-path form would also survive an inode change, but ``man tmutil`` requires
    root and Full Disk Access for it, and this step runs as the owner.

    Nothing here is restored from a backup anyway. The design rewrites ``config.yaml``
    per machine, and the token and the roster are carried deliberately on migration.
    A token pointed outside the directory is excluded on its own, because a brokerage
    credential must be excluded wherever it is put.
    """
    targets = [config_dir]
    token = Path(token_path)
    if Path(config_dir) not in token.parents:
        targets.append(token_path)
    return tuple(targets)


def tmutil_exclusion_commands(config_dir: str, token_path: str) -> tuple[str, ...]:
    """The Time Machine exclusion lines. They run as the user, no root."""
    return tuple(
        f"tmutil addexclusion {shlex.quote(target)}"
        for target in tmutil_exclusion_targets(config_dir, token_path)
    )


def read_token_mint(token_path: Path | str) -> datetime:
    """When the refresh token in ``token_path`` was minted, timezone-aware in UTC.

    ``schwab-py`` writes ``creation_timestamp`` beside the token, the epoch second of
    the last full browser re-auth. This reads that one field and nothing else, so the
    secret half of the file never leaves the parser. Any failure raises
    ``ValueError`` and the caller treats it as a problem, never as a skip.
    """
    path = Path(token_path)
    try:
        payload = json.loads(path.read_text())
    except OSError as exc:
        raise ValueError(f"token file unreadable: {exc.strerror}") from exc
    except ValueError as exc:
        raise ValueError("token file is not JSON") from exc
    created = payload.get("creation_timestamp") if isinstance(payload, dict) else None
    if created is None:
        raise ValueError("token file has no creation_timestamp")
    # A bool is an int to Python and a numeric string is a float to float(). Neither
    # is a stamp schwab-py writes, so both are refused before the conversion.
    if isinstance(created, bool) or not isinstance(created, (int, float)):
        raise ValueError("creation_timestamp is not an epoch second")
    try:
        return datetime.fromtimestamp(float(created), tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("creation_timestamp is not an epoch second") from exc


@dataclass(frozen=True)
class RenderedFile:
    """One file the dry-run renderer produces.

    ``mode`` is the permission bits to write it with. Everything is 0o644 except the
    two scripts the operator runs, which are 0o755.
    """

    name: str
    content: str
    mode: int = 0o644


def render_all(host: LaunchdHost) -> tuple[RenderedFile, ...]:
    """Every plist and setup file, as text, in install order."""
    files = [RenderedFile(f"{job.label}.plist", job.render()) for job in all_jobs(host)]
    files.append(RenderedFile(SUDOERS_FILE, sudoers_dropin(host.owner)))
    files.append(RenderedFile(INSTALL_SCRIPT_FILE, install_script(host), mode=0o755))
    files.append(RenderedFile(UNINSTALL_SCRIPT_FILE, uninstall_script(host), mode=0o755))
    files.append(RenderedFile(RESTART_SCRIPT_FILE, restart_script(host), mode=0o755))
    return tuple(files)


def _is_protected(target: Path) -> bool:
    # Fold case before comparing. The default APFS volume is case-insensitive, so
    # /LIBRARY/LaunchDaemons names the same directory as /Library/LaunchDaemons, and
    # ``Path.resolve`` does not fold it.
    resolved = Path(str(target.resolve()).lower())
    roots = tuple(Path(str(root).lower()) for root in _PROTECTED_ROOTS)
    return any(resolved == root or root in resolved.parents for root in roots)


def write_rendered(files: Sequence[RenderedFile], out_dir: Path) -> list[Path]:
    """Write the rendered files into ``out_dir`` and nowhere else.

    The renderer is dry-run only. It refuses a target under the system directories,
    so the install itself stays a deliberate, by-hand step.
    """
    out = Path(out_dir)
    if _is_protected(out):
        raise ValueError(f"refusing to render into a system directory: {out}")
    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for item in files:
        path = out / item.name
        path.write_text(item.content)
        path.chmod(item.mode)
        written.append(path)
    return written


def _first_install_lines(
    host: LaunchdHost, *, plist_path: Callable[[str], str], sudoers_path: str
) -> list[str | tuple[str, str]]:
    """Steps 1 to 5, as comment and command lines, in order.

    One source for two renderings. ``install_commands`` prints these for pasting and
    ``install_script`` wraps them in a script, so the two cannot drift into disagreeing
    about what installing means.

    An item is a comment, a command, or a pair of commands where the second runs only
    if the first succeeds. The pair exists because ``visudo`` gates the sudoers install,
    and the two renderings must express that gate differently. A pasted line uses
    ``&&``, which self-gates. A script must not, because ``set -e`` does not stop on a
    failing left side of ``&&``, so a rejected drop-in would skip its install and let
    the rest of the install continue.

    The two callers also name the rendered files differently. The pasted text carries
    absolute paths, so a line works from any directory. The script resolves its own
    location instead, so moving the rendered directory does not break it. Both
    ``plist_path`` and ``sudoers_path`` arrive already shell-quoted.
    """
    lines: list[str | tuple[str, str]] = [
        "# 1. Install the five LaunchDaemons, root-owned as launchd requires."
    ]
    for job in all_jobs(host):
        lines.append(
            f"sudo install -o root -g wheel -m 644 {plist_path(job.label)} /Library/LaunchDaemons/"
        )
    sudoers = sudoers_path
    lines += [
        "# 2. Install the sudoers drop-in after visudo validates it.",
        (
            f"sudo visudo -cf {sudoers}",
            f"sudo install -o root -g wheel -m 440 {sudoers} /etc/sudoers.d/marketlake",
        ),
        "# visudo checks the syntax only. This prints the two rules as sudo parsed them,",
        "# which is what shows the one-shot's regular expression survived as one.",
        "sudo -l | grep pmset",
        "# 3. Set the weekday firmware wake, then read it back.",
        f"sudo {pmset_repeat_command()}",
        "pmset -g sched",
        "# 4. Keep the token and the config secrets out of Time Machine. As the owner,",
        "# never under sudo. The whole directory goes, so an editor that saves by rename",
        "# cannot drop the exclusion, and config.yaml's four secrets are covered too.",
        *tmutil_exclusion_commands(default_config_dir(host.home), default_token_path(host.home)),
        "tmutil isexcluded "
        + " ".join(
            shlex.quote(target)
            for target in tmutil_exclusion_targets(
                default_config_dir(host.home), default_token_path(host.home)
            )
        ),
        "# 5. Load the jobs into the system domain, then confirm the daemon is running.",
    ]
    for job in all_jobs(host):
        lines.append(
            f"sudo launchctl bootstrap {LAUNCHD_DOMAIN} /Library/LaunchDaemons/{job.label}.plist"
        )
    lines.append(f"launchctl print {LAUNCHD_DOMAIN}/{DAEMON_LABEL}")
    return lines


def install_script(host: LaunchdHost) -> str:
    """Steps 1 to 5 as a script the operator runs. The renderer never runs it.

    The build plan's D14 permits this and names the three things it owes, because the
    by-hand paste it replaces bought them for free:

    1. It stops at the first failure. ``set -e`` does that, and step 2's gate is emitted
       as two statements rather than one ``&&`` line so that it holds. ``set -e`` does
       not stop on a failing left side of ``&&``, so the joined form would let a
       rejected drop-in skip its own install and the rest of the install continue.
    2. It echoes each command before running it, so the transcript shows what ran as
       root.
    3. It ends on ``launchctl print``, so the operator reads whether the daemon came up
       rather than assuming it.

    Paths resolve from the script's own directory rather than from a baked absolute
    path, so moving the rendered directory does not break it. Step 6 is deliberately
    absent. It is the standing Friday task, not part of the first install.
    """
    body: list[str] = []
    for item in _first_install_lines(
        host,
        plist_path=lambda label: f'"$HERE/{label}.plist"',
        sudoers_path='"$HERE/' + SUDOERS_FILE + '"',
    ):
        for command in (item,) if isinstance(item, str) else item:
            if command.startswith("#"):
                body.append(command)
            else:
                body.append(f"echo {shlex.quote('+ ' + command)}")
                body.append(command)

    # Counted from the body rather than written down, so the header cannot drift from
    # what the script actually runs.
    commands = [line for line in body if not line.startswith(("#", "echo "))]
    under_sudo = [line for line in commands if line.startswith("sudo ")]
    name = INSTALL_SCRIPT_FILE
    header = [
        "#!/bin/bash",
        "# Marketlake control plane: the first install, steps 1 to 5.",
        "#",
        "# Written by `python -m lake.control_plane render`, which never runs it. Run it",
        "# yourself, as the owner. It calls sudo for the privileged steps and will prompt.",
        "#",
        "# Usage. It installs the files sitting beside it, so it runs from anywhere and the",
        "# rendered directory can be moved or renamed:",
        "#",
        f"#     ./{name}                     # from the directory it was rendered into",
        f"#     ~/marketlake-install/{name}  # or by path, from anywhere",
        "#",
        f"# Read it first. Of the {len(commands)} commands below, {len(under_sudo)} run under"
        " sudo. The rest need no root,",
        "# and step 4 must not have any.",
        "#",
        "# It stops at the first failure, so a visudo that rejects the drop-in never",
        "# reaches the install that would place it. Every command is echoed before it runs.",
        "# The last command reads back whether the daemon came up.",
        "#",
        "# Step 6, the standing Friday one-shot, is not here. It is not part of the first",
        "# install. Run it from the install text.",
        "#",
        "# To reinstall after a re-render, run the uninstall first and this second:",
        "#",
        f"#     ./{UNINSTALL_SCRIPT_FILE} && ./{INSTALL_SCRIPT_FILE}",
        "#",
        "# The `&&` is load-bearing, not punctuation. An uninstall that cannot finish has",
        "# to leave this half unrun, rather than layering a new install over a broken one.",
        "# A `;` would run it anyway. Read uninstall.sh's header before you do.",
        "set -euo pipefail",
        "",
        'HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
        "",
    ]
    return "\n".join(header + body) + "\n"


def uninstall_script(host: LaunchdHost) -> str:
    """Take the control plane off a machine, in the reverse of the install's order.

    The install writes five plists (step 1), the sudoers drop-in (step 2), the weekday
    firmware wake (step 3), the Time Machine exclusion (step 4), and bootstraps five
    labels (step 5). This runs 5, 3, 2, 1. Step 4 is deliberately skipped, and the
    bootout leading is what keeps launchd from ever holding a definition whose file is
    gone. Booting out a label that is not loaded is skipped rather than treated as a
    failure, so this converges from a partial install as well as a whole one.

    Three things it leaves, and leaving them is the point:

    1. The lake. Deleting captured data is not part of undoing an install.
    2. The config directory and its Time Machine exclusion. The directory holds the
       token, ``config.yaml`` and ``tickers.yaml``, all of which survive an uninstall,
       so the protection on them survives too. Symmetry with the install is the wrong
       principle for a guard over data that outlives the install. Lifting it would put
       the token and ``config.yaml``'s secrets on the next hourly backup, and a backup
       that already ran cannot be un-run by re-adding the exclusion later.
    3. The Sunday one-shot wake. ``pmset schedule cancel`` can take a single event, but
       only by naming the exact date and time it was set for, and nothing here knows
       which Sunday is pending without parsing ``pmset -g sched``. That is more
       machinery than one wake is worth. The one-shot fires once and is then gone.

    The weekday wake is different, and its removal is the one place this reaches past
    what the install placed. macOS holds one *pair* of repeating events, a power-on and
    a power-off, and ``pmset repeat cancel`` clears the pair. There is no command that
    cancels half of it. So a repeating sleep or shutdown the operator set elsewhere goes
    with the 08:25 wake. The script reads the schedule back before the cancel as well as
    after, so the transcript carries what to re-set by hand.
    """
    lines = [
        "#!/bin/bash",
        "# Marketlake control plane: take the install back off, in reverse order.",
        "#",
        "# Written by `python -m lake.control_plane render`, which never runs it. Run it",
        "# yourself, as the owner. It calls sudo for the privileged steps and will prompt.",
        "#",
        "# Usage:",
        "#",
        f"#     ./{UNINSTALL_SCRIPT_FILE}",
        "#",
        "# It boots out the five launchd jobs, cancels the weekday firmware wake, removes",
        "# the sudoers drop-in, and deletes the five plists. That is install steps 5, 3, 2",
        "# and 1, undone in that order.",
        "#",
        "# Three things it leaves:",
        "#",
        "#   - The lake. Deleting captured data is not part of undoing an install.",
        "#   - The config directory AND its Time Machine exclusion. The token,",
        "#     config.yaml and tickers.yaml all survive, so the protection on them",
        "#     survives too. Lifting the exclusion would put the token and config.yaml's",
        "#     secrets on the next hourly backup.",
        "#   - The Sunday one-shot wake. Cancelling one event by name needs the exact",
        "#     date it was set for, which nothing here knows without parsing `pmset -g",
        "#     sched`. It fires once and is then gone.",
        "#",
        "# READ THIS BEFORE STEP 2. macOS holds one PAIR of repeating power events, a",
        "# power-on and a power-off, and `pmset repeat cancel` clears the pair. No command",
        "# cancels half of it. So a repeating sleep or shutdown you set elsewhere goes with",
        "# the 08:25 wake. Step 2 prints the schedule before and after for that reason.",
        "# Anything in the first print that is not the marketlake wake is yours to re-set.",
        "# That holds on the reinstall path too: install.sh re-sets the 08:25 wake and",
        "# nothing else, so it does not put back what step 2 took from you.",
        "#",
        "# Reinstalling after a re-render is this script and then the install, in one go:",
        "#",
        f"#     ./{UNINSTALL_SCRIPT_FILE} && ./{INSTALL_SCRIPT_FILE}",
        "#",
        "# The `&&` is load-bearing. If this half cannot finish, the install half must not",
        "# run, rather than layering a new install over a broken one. A `;` would run it.",
        "# There is no third script. A reinstall is these two, in that order, and nothing",
        "# else, so it cannot drift from what an install and an uninstall mean.",
        "#",
        "# Four dead-man checks go silent when these jobs stop: capture, pre-open,",
        "# calendar-probe and sunday. Each pages once its own deadline passes, which for",
        "# capture is inside the weekday capture window and for sunday is Sunday 23:30.",
        "# Pause all four from healthchecks first if the machine is meant to stay",
        "# uninstalled. A check that has been pinged once does not go back to `new` on its",
        "# own, so simply stopping the jobs is not enough to keep them quiet.",
        "set -euo pipefail",
        "",
    ]

    def emit(command: str) -> None:
        lines.append(f"echo {shlex.quote('+ ' + command)}")
        lines.append(command)

    labels = [job.label for job in all_jobs(host)]
    lines.append("# 1. Boot the five jobs out. This undoes install step 5, and it leads so that")
    lines.append("# no plist below is deleted while launchd still holds its definition.")
    for label in labels:
        domain = f"{LAUNCHD_DOMAIN}/{label}"
        lines += [
            f"if launchctl print {domain} >/dev/null 2>&1; then",
            f"  echo {shlex.quote(f'+ sudo launchctl bootout {domain}')}",
            f"  sudo launchctl bootout {domain}",
            "else",
            f"  echo {shlex.quote(f'  {domain} is not loaded, nothing to boot out')}",
            "fi",
        ]

    lines.append("# 2. Cancel the weekday firmware wake. This undoes install step 3. The read-back")
    lines.append("# runs first as well as last, because the cancel takes the whole repeating pair")
    lines.append("# and the first print is the only record of what else was in it.")
    emit("pmset -g sched")
    emit("sudo pmset repeat cancel")
    emit("pmset -g sched")

    lines.append("# 3. Remove the sudoers drop-in. This undoes install step 2. It grants two")
    lines.append("# pmset writes and nothing this script runs, so nothing above depended on it.")
    emit("sudo rm -f /etc/sudoers.d/marketlake")

    lines.append("# 4. Delete the five plists. This undoes install step 1, the first thing the")
    lines.append("# install placed and so the last thing to come off.")
    for label in labels:
        emit(f"sudo rm -f /Library/LaunchDaemons/{label}.plist")

    lines.append("# 5. Confirm the daemon is gone. Here the read-back is expected to fail, and")
    lines.append("# that failure is the success condition, so it is handled rather than fatal.")
    read_back = f"launchctl print {LAUNCHD_DOMAIN}/{DAEMON_LABEL}"
    lines += [
        f"echo {shlex.quote('+ ' + read_back)}",
        f"if {read_back} >/dev/null 2>&1; then",
        f"  echo {shlex.quote(f'  WARNING: {DAEMON_LABEL} is still loaded')}",
        "  exit 1",
        "else",
        f"  echo {shlex.quote(f'  {DAEMON_LABEL} is gone')}",
        "fi",
    ]
    return "\n".join(lines) + "\n"


def restart_script(host: LaunchdHost) -> str:
    """Restart a resident job so it picks up new code. The renderer never runs it.

    Only two of the five jobs can go stale, and the reason is the shape of the job rather
    than anything about the code. The daemon and the dashboard are resident: launchd
    starts each once and ``KeepAlive`` relaunches it if it exits, so each holds the Python
    it imported at start. The venv is an editable install whose path entry is the absolute
    ``src`` directory, so editing that tree changes what a *new* process imports and
    nothing about one already running. The self-check, the calendar probe and the Sunday
    job exec fresh on every fire, so they always run current code and never need this. The
    pair is derived from ``keep_alive``, so a sixth resident job is covered by adding the
    job and nothing else.

    ``launchctl kickstart -k`` runs the service immediately whatever its launch conditions
    say, killing the running instance first if there is one. That is right when the code
    changed and the plist did not, and wrong after a re-render that changed a plist,
    because the old definition is what gets run. The install text covers the second case.

    Two launchd states read the same through a pid and must not be confused. ``launchctl
    print`` exits 0 for any label in the domain and 113 for one that is not, while the
    ``pid`` line appears only while a process is actually running. So a label that is
    loaded but between processes prints no pid, exactly like one that was never installed.
    Telling the operator to run the install there would be wrong twice over: the diagnosis
    is false, and ``install.sh`` bootstraps every label under ``set -e``, which fails on a
    label already in the domain. Loadedness comes from the exit code and running-ness from
    the pid line, asked separately.

    A new pid is not yet a working service. A resident that dies on import gets a fresh pid
    within seconds too, which is precisely the failure a restart after a code change is
    most likely to hit, so the script waits and requires the new pid to still be there.
    Reporting success on a crash-looping job would make the read-back worse than none.

    The **bootout-and-bootstrap restart is considered and rejected**. It would also pick up
    a changed plist, which makes it look like the more general tool. It is the more
    dangerous one: booting a label out drops it from the domain, so a failure in between
    leaves the service down rather than merely unrestarted. ``kickstart`` cannot reach that
    state, because launchd holds the definition throughout.

    **Defaulting to both residents is considered and rejected** too. Restarting the
    dashboard costs its open connections. Restarting the daemon costs the in-flight cycle
    and its ``caffeinate`` assertion until it is back. A bare invocation must not be the
    command that takes capture down, so the daemon has to be named.
    """
    residents = [job.label for job in all_jobs(host) if job.keep_alive]
    short = {label.rsplit(".", 1)[-1]: label for label in residents}
    # The default is the dashboard, because restarting it costs open connections while
    # restarting the daemon costs the in-flight cycle. Derived from the label rather than
    # written out, so a dashboard that stopped being resident fails here loudly.
    default = DASHBOARD_LABEL.rsplit(".", 1)[-1]
    if default not in short:
        raise ValueError(f"{DASHBOARD_LABEL} is not resident, so it cannot be the default")
    lines = [
        "#!/bin/bash",
        "# Marketlake control plane: restart a resident job so it picks up new code.",
        "#",
        "# Written by `python -m lake.control_plane render`, which never runs it. Run it",
        "# yourself, as the owner. The restart needs root and will prompt.",
        "#",
        "# Usage:",
        "#",
        f"#     ./{RESTART_SCRIPT_FILE}".ljust(30) + f"# {default} only, the default",
    ]
    for name in short:
        lines.append(f"#     ./{RESTART_SCRIPT_FILE} {name}".ljust(30) + f"# {name} only")
    lines += [
        f"#     ./{RESTART_SCRIPT_FILE} all".ljust(30) + "# every resident job",
        "#",
        "# Only these jobs can go stale, and the reason is their shape. They are resident:",
        "# launchd starts each once and KeepAlive relaunches it if it exits, so each holds",
        "# the Python it imported at start. The venv is an editable install pointing at an",
        "# absolute src directory, so editing that tree changes what a NEW process imports",
        "# and nothing about one already running. The other three jobs exec fresh on every",
        "# fire, so they always run current code and never need this.",
        "#",
        "# `launchctl kickstart -k` runs the service immediately whatever its launch",
        "# conditions say, killing the running instance first if there is one. That is the",
        "# right tool when the code changed and the plist did not. It is the WRONG tool",
        "# after a re-render that changed a plist, because the old definition is what gets",
        "# run. For a changed plist, reinstall instead:",
        "#",
        f"#     ./{UNINSTALL_SCRIPT_FILE} && ./{INSTALL_SCRIPT_FILE}",
        "#",
        "# That reinstall does restart both residents on the way through, so it is not that",
        "# this case had no tool. It is that the only tool was one that takes the whole",
        "# control plane off and puts it back to achieve a process restart.",
        "#",
        "# Restarting is not free. The dashboard drops its open connections, and the daemon",
        "# loses the in-flight cycle and its caffeinate assertion until it is back. So a",
        "# bare invocation restarts the dashboard alone and the daemon has to be named.",
        "#",
        "# The services import from the working tree, so what they pick up is that tree as",
        "# it stands right now, branch and uncommitted edits included. Step 1 prints it.",
        "set -euo pipefail",
        "",
        f"PROJECT_DIR={shlex.quote(host.project_dir)}",
        f"LOG_DIR={shlex.quote(host.log_dir)}",
        f"DOMAIN={LAUNCHD_DOMAIN}",
        "SETTLE_SECONDS=3",
        "",
        'case "${1:-' + default + '}" in',
    ]
    for name, label in short.items():
        lines.append(f"  {name}) LABELS=({label}) ;;")
    lines += [
        "  all) LABELS=(" + " ".join(residents) + ") ;;",
        "  *)",
        "    echo "
        + shlex.quote(f"usage: ./{RESTART_SCRIPT_FILE} [" + "|".join([*short, "all"]) + "]")
        + " >&2",
        "    exit 2 ;;",
        "esac",
        "",
        "# Two questions, not one. `launchctl print` exits 0 for any label in the domain",
        "# and 113 for one that is not. The pid line appears only while a process is",
        "# actually running, so a loaded job between processes prints no pid and reads",
        "# exactly like one that was never installed. Neither needs root.",
        "is_loaded() {",
        '  launchctl print "$DOMAIN/$1" >/dev/null 2>&1',
        "}",
        "",
        "pid_of() {",
        '  launchctl print "$DOMAIN/$1" 2>/dev/null |',
        r"    sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1 || true",
        "}",
        "",
        "# 1. What the restart will pick up. The services import from this tree, so this is",
        "# the code they will be running afterwards, not whatever was current at boot.",
        "echo " + shlex.quote("+ working tree at " + host.project_dir),
        'if git -C "$PROJECT_DIR" rev-parse --git-dir >/dev/null 2>&1; then',
        "  # An unborn HEAD makes --git-dir succeed and --abbrev-ref fail, and an",
        "  # unguarded substitution would end the run here under `set -e`.",
        '  branch="$(git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"',
        '  : "${branch:=unknown}"',
        '  echo "  branch: $branch"',
        '  if [[ -n "$(git -C "$PROJECT_DIR" status --porcelain || true)" ]]; then',
        "    echo "
        + shlex.quote("  WARNING: uncommitted changes, so the restart picks those up too"),
        "  fi",
        '  if [[ "$branch" != "main" ]]; then',
        "    echo " + shlex.quote("  WARNING: not on main, so the restart runs branch code"),
        "  fi",
        "else",
        "  echo " + shlex.quote("  not a git checkout, so no branch to report"),
        "fi",
        "",
        "# 2. Restart each named job, and prove it came back and stayed. A pid that did not",
        "# change is a kickstart that did nothing. A pid that keeps changing is a job dying",
        "# on the new code, which is the failure a restart is most likely to cause.",
        'for label in "${LABELS[@]}"; do',
        '  if ! is_loaded "$label"; then',
        '    echo "  $label is not in the $DOMAIN domain. Run the install first." >&2',
        "    exit 1",
        "  fi",
        '  before="$(pid_of "$label")"',
        '  if [[ -z "$before" ]]; then',
        '    echo "  $label is loaded but not running, so this starts it"',
        "  else",
        "    # If the pid is already gone, ps fails, and under `set -e` an unguarded",
        "    # command substitution would end the run here having printed nothing.",
        '    since="$(ps -o lstart= -p "$before" 2>/dev/null | sed \'s/^ *//\' || true)"',
        '    : "${since:=an unknown time}"',
        '    echo "  $label is pid $before, running since $since"',
        "  fi",
        '  echo "+ sudo launchctl kickstart -k $DOMAIN/$label"',
        '  sudo launchctl kickstart -k "$DOMAIN/$label"',
        "  # KeepAlive relaunches within seconds rather than instantly, so poll rather",
        "  # than read once.",
        '  after=""',
        "  for _ in 1 2 3 4 5; do",
        '    after="$(pid_of "$label")"',
        '    if [[ -n "$after" && "$after" != "$before" ]]; then',
        "      break",
        "    fi",
        "    sleep 1",
        "  done",
        '  if [[ -z "$after" ]]; then',
        '    echo "  WARNING: $label has no pid after the restart" >&2',
        '    echo "  Check $LOG_DIR/$label.err.log" >&2',
        "    exit 1",
        "  fi",
        '  if [[ "$after" == "$before" ]]; then',
        '    echo "  WARNING: $label is still pid $before, so it did not restart" >&2',
        "    exit 1",
        "  fi",
        "  # A new pid is not yet a working service. A resident that dies on import gets a",
        "  # fresh pid too, so the new one has to still be there a moment later.",
        '  sleep "$SETTLE_SECONDS"',
        '  settled="$(pid_of "$label")"',
        '  if [[ "$settled" != "$after" ]]; then',
        '    echo "  WARNING: $label will not stay up. pid went $before -> $after ->'
        ' ${settled:-none}." >&2',
        '    echo "  That is a job crash-looping on the new code. Check'
        " $LOG_DIR/"
        '$label.err.log" >&2',
        "    exit 1",
        "  fi",
        '  echo "  $label restarted: pid $before -> $after, still up after ${SETTLE_SECONDS}s"',
        "done",
    ]
    return "\n".join(lines) + "\n"


def install_commands(out_dir: Path, host: LaunchdHost) -> str:
    """The operator's manual install steps, as text. Nothing here runs from code.

    Every operator-supplied path is quoted, because these lines are pasted into a shell
    and a home or an output directory may hold a space.
    """
    out = Path(out_dir)
    lines = ["# Marketlake control plane: the manual install. Run each line by hand."]
    for item in _first_install_lines(
        host,
        plist_path=lambda label: shlex.quote(f"{out / label}.plist"),
        sudoers_path=shlex.quote(str(out / SUDOERS_FILE)),
    ):
        lines.append(f"{item[0]} && {item[1]}" if isinstance(item, tuple) else item)
    lines += [
        "# 6. Set the Sunday one-shot. The slice-3 vendor sweep will do this every Friday.",
        "# Until that sweep lands, run this line each Friday and run the second command it",
        "# prints under sudo. Nothing else sets the one-shot, and the Sunday read-back",
        "# cannot catch a missed one: by Sunday evening a wake that never got set and one",
        "# that already fired look the same. A machine left asleep still pages, because",
        "# the Sunday check never runs and its dead-man ping never arrives.",
        f"cd {shlex.quote(host.project_dir)} && "
        f"{shlex.quote(host.python)} -m lake.control_plane pmset",
    ]
    # Unnumbered, because a reinstall is conditional rather than a step of the first
    # install. The bootout lines stay commented out: booting out a label that was never
    # loaded fails, so a fresh install must not run them.
    lines += [
        "# Re-installing. Steps 1 to 5 are the first install and run once. Step 6 is the",
        "# standing Friday task until slice 3 lands.",
        "# Step by step is not the recommended path. Run the two scripts beside this file",
        f"#     ./{UNINSTALL_SCRIPT_FILE} && ./{INSTALL_SCRIPT_FILE}",
        "# which re-runs every step and so cannot skip one that changed. The `&&` is",
        "# load-bearing: an uninstall that cannot finish must leave the install unrun.",
        "# If you do it step by step, the trap is that a re-render can change steps 2, 3",
        "# and 4 while every plist stays byte-identical. Re-tuning either wake rewrites",
        "# the sudoers drop-in, and every plist is left untouched, so a plist diff shows",
        "# nothing to do while the drop-in keeps granting the old command. Check it",
        "# directly:",
        f"#     sudo diff /etc/sudoers.d/marketlake {shlex.quote(str(out / SUDOERS_FILE))}",
        "# launchd keeps a job's definition from the bootstrap that loaded it, so",
        "# overwriting a plist in step 1 changes nothing by itself. After a re-render that",
        "# changes a plist, boot out that label, then run its step 1 and step 5 lines again.",
        "# `launchctl kickstart -k` is not the reload. It restarts the process under the",
        "# definition already loaded. A re-render that only adds a plist needs step 1 and",
        "# step 5 for the new label alone, with no bootout. Steps 2, 3, and 4 write files",
        "# and settings, so they re-run as they are.",
    ]
    for job in all_jobs(host):
        lines.append(f"# sudo launchctl bootout {LAUNCHD_DOMAIN}/{job.label}")
    lines += [
        "# Restarting. New code does not reach a running job. The daemon and the",
        "# dashboard are resident, so each holds the Python it imported at start. After",
        f"# pulling code with no plist change, run ./{RESTART_SCRIPT_FILE} beside this file.",
        "# The other three jobs exec fresh every fire and never need it. A re-render that",
        "# changed a plist is the other case, and it needs the reinstall above.",
        f"# Uninstalling. Run ./{UNINSTALL_SCRIPT_FILE} beside this file. It undoes steps",
        "# 5, 3, 2 and 1, in that order. Read its header before running it. It lists what",
        "# it leaves, and it warns that the wake cancel takes the whole repeating power",
        "# pair rather than only the marketlake half. That list lives in one place, so it",
        "# is not restated here.",
    ]
    return "\n".join(lines) + "\n"


# -- the command-line entry ------------------------------------------------------------


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.control_plane",
        description="The laptop control plane: plists, pmset, sudoers, and the two jobs.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    render = sub.add_parser("render", help="Dry-run: write every plist and setup file to DIR.")
    render.add_argument("--out", required=True, help="Output directory. Never a system path.")
    render.add_argument("--python", required=True, help="Path to the Python interpreter.")
    render.add_argument("--owner", required=True, help="The account the jobs run as.")
    render.add_argument("--home", required=True, help="That account's home directory.")
    render.add_argument("--project-dir", required=True, help="WorkingDirectory for the jobs.")
    render.add_argument("--log-dir", required=True, help="Directory for stdout/stderr logs.")
    render.add_argument("--group", default="staff", help="GroupName for the jobs.")
    render.add_argument("--config", help="Config path, passed via MARKETLAKE_CONFIG.")

    check = sub.add_parser("self-check", help="Verify the daemon is up, then ping pre-open.")
    check.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    check.add_argument("--label", default=DAEMON_LABEL, help="The daemon's launchd label.")

    sunday = sub.add_parser("sunday", help="Scrub, verify the wake alarms, canary, then ping.")
    sunday.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    sunday.add_argument(
        "--token", help="Path to token.json. Defaults to the standard place under HOME."
    )

    sub.add_parser("pmset", help="Print the two pmset commands for the coming week.")

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
    probe: DaemonProbe | None = None,
    pinger: Pinger | None = None,
    schedule_reader: ScheduleReader | None = None,
    canary: CanaryCall | None = None,
    exclusion_reader: ExclusionReader | None = None,
    transport: Transport | None = None,
) -> int:
    """The ``python -m lake.control_plane`` entry. Returns a process exit code.

    The seams default to the real ones and are built lazily, so a test injects fakes
    and nothing here reads the wall clock or shells out.

    Two of those defaults reach the outside world, and a test that wants the Sunday job
    must pass its own for both. ``canary`` defaults to one real quote through the
    vendor, and ``transport`` defaults to the real ntfy POST. A push sent from a test is
    a push a person receives.
    """
    args = _build_parser().parse_args(argv)

    if args.command == "render":
        # Every one of these lands in a plist or in a printed install line that runs
        # from wherever the operator pastes it, so a relative value is never right.
        # --out is resolved rather than refused, because a directory to write into is
        # naturally typed relative.
        relative = {
            name: value
            for name, value in (
                ("--python", args.python),
                ("--home", args.home),
                ("--project-dir", args.project_dir),
                ("--log-dir", args.log_dir),
            )
            if not Path(value).is_absolute()
        }
        if relative:
            named = ", ".join(f"{name} {value!r}" for name, value in sorted(relative.items()))
            print(f"render: these must be absolute paths: {named}", file=sys.stderr)
            return 2
        host = LaunchdHost(
            python=args.python,
            owner=args.owner,
            home=args.home,
            project_dir=args.project_dir,
            log_dir=args.log_dir,
            group=args.group,
            config_path=args.config,
        )
        # Resolved, so the printed install lines name an absolute path and work from
        # any directory, not only the one the render ran in.
        out = Path(args.out).resolve()
        try:
            written = write_rendered(render_all(host), out)
        except ValueError as exc:
            print(f"render: {exc}", file=sys.stderr)
            return 2
        # Progress and errors go to stderr, the install text to stdout. The text is
        # meant to be read and pasted, so ``render ... > install.txt`` has to yield a
        # file of nothing but comments and commands. A ``wrote ...`` line welded to the
        # top of it is not a command, and a shell fed the file reports it as one.
        for path in written:
            print(f"wrote {path}", file=sys.stderr)
        print(install_commands(out, host), end="")
        return 0

    if args.command == "self-check":
        with input_errors_exit("self-check"):
            config = load_config(args.config)
        outcome = self_check(
            probe=probe if probe is not None else launchctl_probe,
            pinger=pinger if pinger is not None else UrllibPinger(),
            ping_url=config.healthchecks_url(PRE_OPEN_SLUG),
            label=args.label,
        )
        status = "daemon up" if outcome.daemon_up else "daemon down"
        if outcome.problem is not None:
            print(f"self-check: {outcome.problem}")
        print(f"self-check: {status} pinged={outcome.pinged} slug={PRE_OPEN_SLUG}")
        return 0 if outcome.pinged else 1

    if args.command == "sunday":
        with input_errors_exit("sunday"):
            config = load_config(args.config)
        token_path = args.token if args.token is not None else default_token_path(str(Path.home()))

        def read_mint() -> datetime | None:
            """The token's mint time, read fresh so a retry can see a new re-login.

            The token file is the only source. The coverage assertion refuses a mint
            that does not clear the coming week, so an override supplying a mint the
            token does not carry would hide the stale token the check exists to catch.
            Validity is not freshness.
            """
            try:
                return read_token_mint(token_path)
            except ValueError as exc:
                print(f"sunday: {exc}")
                return None

        # The job's own clock, shared by the retry loop and the reminder's timestamp, so
        # a push is stamped with the attempt that raised it.
        run_clock = clock if clock is not None else _system_clock()
        # The reminder's delivery. The secrets are the two values that must never reach a
        # phone, checked against the message itself.
        publisher = Publisher(
            lake_root=config.lake_root,
            transport=(
                transport if transport is not None else NtfyTransport(config.ntfy_topic.reveal())
            ),
            secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
        )
        outcomes = sunday_run(
            lake_root=config.lake_root,
            clock=run_clock,
            calendar=calendar if calendar is not None else _exchange_calendar(),
            schedule_reader=schedule_reader if schedule_reader is not None else read_pmset_schedule,
            pinger=pinger if pinger is not None else UrllibPinger(),
            ping_url=config.healthchecks_url(SUNDAY_SLUG),
            canary=(
                canary
                if canary is not None
                else token_canary(
                    token_path=token_path,
                    api_key=config.schwab_api_key.reveal(),
                    app_secret=config.schwab_app_secret.reveal(),
                )
            ),
            mint_reader=read_mint,
            reminder_sink=reminder_publisher(publisher=publisher, clock=run_clock),
            exclusion_targets=tmutil_exclusion_targets(
                default_config_dir(str(Path.home())), token_path
            ),
            exclusion_reader=(
                exclusion_reader if exclusion_reader is not None else read_exclusions
            ),
        )
        for number, outcome in enumerate(outcomes, start=1):
            if len(outcomes) > 1:
                print(f"sunday: attempt {number} of {len(outcomes)}")
            for problem in outcome.problems:
                print(f"sunday: {problem}")
            for line in outcome.report:
                print(f"sunday: report: {line}")
            if outcome.reminder is not None:
                print(f"sunday: reminder: {outcome.reminder.body}")
        pinged = outcomes[-1].pinged
        print(f"sunday: attempts={len(outcomes)} pinged={pinged} slug={SUNDAY_SLUG}")
        return 0 if pinged else 1

    if args.command == "pmset":
        now = (clock if clock is not None else _system_clock()).now()
        cal = calendar if calendar is not None else _exchange_calendar()
        print(pmset_repeat_command())
        print(sunday_wake_command(now, cal))
        return 0

    return 2  # pragma: no cover - argparse requires a subcommand


def _system_clock() -> Clock:
    from lake.clock import SystemClock  # lazy: only the console reads the wall clock

    return SystemClock()


def _exchange_calendar() -> Calendar:
    from lake.calendar import ExchangeCalendar  # lazy: the real calendar loads slowly

    return ExchangeCalendar()


__all__ = [
    "CANARY_DEADLINE",
    "CANARY_RETRY",
    "CANARY_SYMBOL",
    "DAEMON_LABEL",
    "DASHBOARD_LABEL",
    "LAUNCHD_DOMAIN",
    "PRE_OPEN_SELF_CHECK",
    "PRE_OPEN_SLUG",
    "REMINDER_EVENT",
    "REMINDER_HOURS",
    "REMINDER_PRIORITY",
    "REMINDER_TITLE",
    "SELF_CHECK_LABEL",
    "SUDOERS_FILE",
    "SUNDAY_LABEL",
    "SUNDAY_MAINTENANCE",
    "SUNDAY_SLUG",
    "SUNDAY_ASSERTION_END",
    "SUNDAY_WAKE",
    "TOKEN_LIFETIME",
    "VENDOR_SWEEP",
    "WAKE_KINDS",
    "WEEKDAY_ASSERTION_END",
    "WEEKDAY_WAKE",
    "AlarmCheck",
    "AssertionHolder",
    "AssertionRunner",
    "AssertionWindow",
    "CanaryCall",
    "DaemonProbe",
    "ExclusionReader",
    "LaunchdHost",
    "MintReader",
    "OneShotAlarm",
    "PmsetParseError",
    "PmsetSchedule",
    "ReauthReminder",
    "ReminderSink",
    "RenderedFile",
    "RepeatAlarm",
    "ScheduleReader",
    "SelfCheckOutcome",
    "SundayOutcome",
    "VendorFactory",
    "WallClockTime",
    "all_jobs",
    "assertion_window",
    "caffeinate_args",
    "check_alarms",
    "daemon_job",
    "dashboard_job",
    "default_config_dir",
    "default_token_path",
    "expected_one_shot",
    "INSTALL_SCRIPT_FILE",
    "RESTART_SCRIPT_FILE",
    "UNINSTALL_SCRIPT_FILE",
    "install_commands",
    "install_script",
    "restart_script",
    "uninstall_script",
    "launchctl_probe",
    "main",
    "next_sunday_wake",
    "parse_exclusions",
    "parse_launchctl_print",
    "parse_pmset_schedule",
    "pmset_repeat_command",
    "pmset_schedule_command",
    "read_exclusions",
    "read_pmset_schedule",
    "read_token_mint",
    "reauth_reminder",
    "reminder_publisher",
    "render_all",
    "self_check",
    "self_check_job",
    "sudoers_dropin",
    "sunday_job",
    "sunday_maintenance",
    "sunday_run",
    "sunday_wake_command",
    "tmutil_exclusion_commands",
    "tmutil_exclusion_targets",
    "token_canary",
    "token_covers_week",
    "week_option_close",
    "write_rendered",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
