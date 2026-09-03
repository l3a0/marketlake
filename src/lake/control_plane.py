"""The laptop control plane.

The data plane is plain files any host can serve. The control plane is what keeps the
laptop awake, keeps the two resident processes running, and proves both to the outside
world. It is macOS-specific by construction and is rewritten per host. This module
renders it and reasons about it. It executes nothing privileged. No ``sudo``, no
``pmset`` write, no ``launchctl``, no ``tmutil`` runs from here. Those are the operator's
install step and the by-hand live checks.

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
- *tmutil addexclusion* keeps a file out of Time Machine backups. The token file gets it,
  so a brokerage credential never rides onto a backup disk.
- A *healthchecks slug* names one dead-man check. A ping fires only on the job's
  success condition, never on mere liveness.

Four operational wall-clock times live here as named integer constants. They are not
session times. The session times come from the calendar. These are the moments the
design pins to the machine's clock, so launchd and pmset can fire them.

1. The 08:25 weekday firmware wake.
2. The 08:30 weekday pre-open self-check.
3. The 19:55 Sunday one-shot wake.
4. The 20:00 Sunday canary and maintenance job.

Two more bound the caffeinate windows: the weekday assertion end near 18:45, when the
vendor sweep's ping lands, and the Sunday canary deadline at 23:00. Every one of these
is a pair of integers, never a ``"HH:MM"`` string, so the session-time enforcement
scanner stays green.

One design caveat governs the Sunday read-back. A fired ``pmset`` one-shot leaves the
schedule. By Sunday 20:00 the 19:55 wake has fired, so a fired one-shot and a never-set
one look the same. Friday's sweep is the read-back that proves the one-shot landed.
The alarm check here therefore expects the one-shot only while its wake is still
ahead, and always expects the weekday repeat alarm.

Every seam is injected: the clock, the calendar, the daemon probe, the schedule reader,
the pinger, the canary, and the caffeinate runner. The whole module runs offline in a
test.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock
from lake.config import load_config
from lake.manifest import ScrubResult, scrub
from lake.runner import LaunchdJob, Pinger, UrllibPinger, calendar_interval

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

    def launchd_intervals(self, weekdays: Sequence[int]) -> list[dict[str, int]]:
        """One ``StartCalendarInterval`` entry per launchd weekday number."""
        return [{**calendar_interval(self.hour, self.minute), "Weekday": wd} for wd in weekdays]


# The operational wall-clock times, per the design's deployment section. These are
# machine-clock moments, not session times. launchd and pmset can only fire on the
# wall clock, so the design pins them there.
WEEKDAY_WAKE = WallClockTime(8, 25)  # pmset repeat wakeorpoweron MTWRF
PRE_OPEN_SELF_CHECK = WallClockTime(8, 30)  # the self-check launchd job, Mon-Fri
WEEKDAY_ASSERTION_END = WallClockTime(18, 45)  # when the vendor sweep's ping lands
SUNDAY_WAKE = WallClockTime(19, 55)  # the Friday-set one-shot wake
SUNDAY_MAINTENANCE = WallClockTime(20, 0)  # the canary + scrub launchd job
CANARY_DEADLINE = WallClockTime(23, 0)  # the canary's last retry, the Sunday check's deadline

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

# The launchd labels. A label is the job's unique identity to launchd. All four sit in
# the system domain because they are LaunchDaemons.
LAUNCHD_DOMAIN = "system"
DAEMON_LABEL = "com.marketlake.daemon"
DASHBOARD_LABEL = "com.marketlake.dashboard"
SELF_CHECK_LABEL = "com.marketlake.self-check"
SUNDAY_LABEL = "com.marketlake.sunday"

# The healthchecks slugs the two calendar jobs ping. Log the slug, never the URL.
PRE_OPEN_SLUG = "pre-open"
SUNDAY_SLUG = "sunday"

# The system PATH a LaunchDaemon gets. launchd gives a job a minimal environment, so
# the plist restores the OS tool directories and prepends the caller's own, such as
# uv's install directory. These are OS locations, not machine-specific paths.
_SYSTEM_PATH = ("/usr/bin", "/bin", "/usr/sbin", "/sbin")

# Directories the dry-run renderer refuses to write into. Installing is the operator's
# step, by hand, with the printed commands.
_PROTECTED_ROOTS = (Path("/Library"), Path("/System"), Path("/etc"), Path("/private/etc"))

# The rendered setup files beside the four plists.
SUDOERS_FILE = "marketlake.sudoers"
TMUTIL_FILE = "tmutil-exclusion.sh"


# -- the host description and the plists -------------------------------------


@dataclass(frozen=True)
class LaunchdHost:
    """The machine values every generated plist carries. All caller-supplied.

    ``python`` is the interpreter path, ``owner`` the account the jobs run as, ``home``
    that account's home directory, ``project_dir`` the working directory, and
    ``log_dir`` where stdout and stderr land. ``path_dirs`` are prepended to ``PATH``,
    which is how the jobs find ``uv``. ``group`` defaults to ``staff``, macOS's
    primary group for every local account. It is a group name, not an account.
    """

    python: str
    owner: str
    home: str
    project_dir: str
    log_dir: str
    group: str = "staff"
    config_path: str | None = None
    path_dirs: tuple[str, ...] = ()

    def environment(self) -> dict[str, str]:
        """The ``EnvironmentVariables`` block every job shares."""
        env = {
            "HOME": self.home,
            "PATH": ":".join((*self.path_dirs, *_SYSTEM_PATH)),
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
    ) -> LaunchdJob:
        """One job of this host running ``python -m <module> <args>``."""
        return LaunchdJob(
            label=label,
            program_arguments=(self.python, "-m", module, *args),
            calendar_interval=calendar,
            working_directory=self.project_dir,
            standard_out_path=self.log_path(label, "out"),
            standard_error_path=self.log_path(label, "err"),
            environment=self.environment(),
            run_at_load=True,
            user_name=self.owner,
            group_name=self.group,
            keep_alive=keep_alive,
        )


def daemon_job(host: LaunchdHost) -> LaunchdJob:
    """The capture daemon, a permanent resident under ``KeepAlive``.

    It runs ``python -m lake.daemon``, the slice-2 loop. It never exits on its own.
    Outside sessions it idles and heartbeats. So it has no calendar interval at all.
    """
    return host.job(DAEMON_LABEL, "lake.daemon", keep_alive=True)


def dashboard_job(host: LaunchdHost) -> LaunchdJob:
    """The read-only localhost query service, the second resident under ``KeepAlive``."""
    return host.job(DASHBOARD_LABEL, "lake.dashboard", keep_alive=True)


def self_check_job(host: LaunchdHost) -> LaunchdJob:
    """The weekday pre-open self-check, five minutes after the firmware wake.

    ``RunAtLoad`` is deliberately on. A load during the day runs the check once at
    load, which is harmless and pings only if the daemon is up.
    """
    return host.job(
        SELF_CHECK_LABEL,
        "lake.control_plane",
        "self-check",
        calendar=PRE_OPEN_SELF_CHECK.launchd_intervals(LAUNCHD_WEEKDAYS),
    )


def sunday_job(host: LaunchdHost, token_path: str) -> LaunchdJob:
    """The Sunday canary and maintenance job, five minutes after the one-shot wake.

    ``token_path`` is the same file the Time Machine exclusion names, so the coverage
    assertion reads the token the install step protected, and the two can never name
    different files. The 18:30 vendor sweep is not rendered here. That job is slice
    3's, and it does not exist yet.
    """
    return host.job(
        SUNDAY_LABEL,
        "lake.control_plane",
        "sunday",
        "--token",
        token_path,
        calendar=SUNDAY_MAINTENANCE.launchd_intervals([LAUNCHD_SUNDAY])[0],
    )


def all_jobs(host: LaunchdHost, token_path: str) -> tuple[LaunchdJob, ...]:
    """Every launchd job the control plane installs, resident processes first."""
    return (
        daemon_job(host),
        dashboard_job(host),
        self_check_job(host),
        sunday_job(host, token_path),
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


def launchctl_probe(label: str, domain: str = LAUNCHD_DOMAIN) -> bool:
    """The real probe: ``launchctl print <domain>/<label>``, parsed for a running state.

    It runs only in the by-hand live check. A test injects a fake instead.
    """
    import subprocess  # lazy: only the live check shells out

    result = subprocess.run(
        ["launchctl", "print", f"{domain}/{label}"],
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
    """
    up = probe(label)
    if not up:
        return SelfCheckOutcome(daemon_up=False, pinged=False)
    pinger.ping(ping_url)
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
    """The one-shot command for the next Sunday wake, given ``now``."""
    return pmset_schedule_command(next_sunday_wake(now, calendar))


# pmset spells the event type ``wakeorpoweron`` on the command line and prints it back
# as ``wakepoweron``. The read-back parser accepts both. Live check 4 confirms the
# printed form.
WAKE_KINDS = frozenset({"wakeorpoweron", "wakepoweron"})

_REPEAT_LINE = re.compile(r"^(?P<kind>\w+)\s+at\s+(?P<time>\S+)\s+(?P<days>.+?)\s*$")
_ONE_SHOT_LINE = re.compile(
    r"^\[\d+\]\s+(?P<kind>\w+)\s+at\s+(?P<date>\S+)\s+(?P<time>\S+)"
    r"(?:\s+by\s+'(?P<owner>[^']*)')?\s*$"
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
    """One repeating power event. ``weekdays`` uses Python numbering, Monday=0."""

    kind: str
    hour: int
    minute: int
    weekdays: frozenset[int]


@dataclass(frozen=True)
class OneShotAlarm:
    """One scheduled power event. ``when`` is naive local time, as pmset prints it."""

    kind: str
    when: datetime
    owner: str | None = None


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
    return date(year, int(match.group("mo")), int(match.group("d")))


def _parse_days(text: str) -> frozenset[int]:
    lowered = text.lower()
    if "every day" in lowered:
        return frozenset(range(7))
    if "weekday" in lowered:
        return _PY_WEEKDAYS
    if "weekend" in lowered:
        return frozenset({_PY_SATURDAY, _PY_SUNDAY})
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
    weekdays only``. A one-shot line reads like ``[0]  wakepoweron at 09/06/26
    19:55:00 by 'pmset'``. Empty output, or a ``No scheduled events`` line, parses
    as an empty schedule. Times in 12-hour or 24-hour form and two- or four-digit
    years are all accepted, because the exact print form is confirmed by live check 4.
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
            one_shots.append(OneShotAlarm(match.group("kind"), when, match.group("owner")))
        else:
            raise PmsetParseError(f"line outside any section: {line!r}")
    return PmsetSchedule(tuple(repeats), tuple(one_shots))


@dataclass(frozen=True)
class AlarmCheck:
    """The verdict on the read-back schedule against the two expected alarms."""

    repeat_ok: bool
    one_shot_ok: bool
    problems: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.repeat_ok and self.one_shot_ok


def expected_one_shot(now: datetime, calendar: Calendar) -> date | None:
    """The Sunday whose one-shot wake should be pending at ``now``, or ``None``.

    A fired one-shot leaves the schedule, so the wake is expected only while it is
    still ahead. From Friday's sweep the coming Sunday is ahead and expected. From the
    Sunday job at 20:00 the wake fired five minutes earlier, so nothing is expected.
    """
    sunday = next_sunday_wake(now, calendar)
    return sunday if SUNDAY_WAKE.on(sunday) > now else None


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
            found = ", ".join(f"{a.hour:02d}:{a.minute:02d} on {sorted(a.weekdays)}" for a in wakes)
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

    It runs only in the by-hand live check. A test injects a fake returning text.
    """
    import subprocess  # lazy: only the live check shells out

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
    one-shot wake until the canary's deadline. Saturday owes nothing.
    """
    weekday = day.weekday()
    if weekday in _PY_WEEKDAYS:
        return AssertionWindow(WEEKDAY_WAKE.on(day), WEEKDAY_ASSERTION_END.on(day))
    if weekday == _PY_SUNDAY:
        return AssertionWindow(SUNDAY_WAKE.on(day), CANARY_DEADLINE.on(day))
    return None


def caffeinate_args(window: AssertionWindow, now: datetime) -> tuple[str, ...] | None:
    """The ``caffeinate -i -t <seconds>`` line that holds idle-sleep off until the window ends.

    ``-i`` prevents idle sleep only. A closed lid still sleeps, which is why the
    design's posture is lid open on AC. The seconds run from ``now`` to the window's
    end, rounded up. A call before the window starts holds early, which costs nothing
    on AC. A call at or past the end returns ``None``: nothing is owed.
    """
    remaining = window.end - now
    if remaining <= timedelta(0):
        return None
    return ("caffeinate", "-i", "-t", str(math.ceil(remaining.total_seconds())))


# Starts the caffeinate process. The real one is ``subprocess.Popen``. A test injects
# a callable that records the arguments.
AssertionRunner = Callable[[Sequence[str]], object]


def _spawn(args: Sequence[str]) -> object:
    import subprocess  # lazy: only the live daemon spawns

    return subprocess.Popen(list(args))


def hold_assertion(
    *, clock: Clock, runner: AssertionRunner | None = None
) -> tuple[str, ...] | None:
    """Hold the assertion for today's window via the injected runner.

    Today is the Eastern date of the injected clock. Returns the arguments the runner
    was handed, or ``None`` when no window is open now.
    """
    now = clock.now().astimezone(MARKET_TZ)
    window = assertion_window(now.date())
    if window is None:
        return None
    args = caffeinate_args(window, now)
    if args is None:
        return None
    (runner if runner is not None else _spawn)(args)
    return args


# -- the token coverage assertion ------------------------------------------------


def week_option_close(now: datetime, calendar: Calendar) -> datetime:
    """The coming week's last option close: Friday's, or the last session's before it.

    The coming week is the Monday-to-Friday span strictly after this week. On a
    Sunday that is tomorrow's week. A Good Friday makes Thursday the last session, so
    the week is walked back from Friday to the first session found.
    """
    monday = _next_monday(now.astimezone(MARKET_TZ).date())
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

# The canary's authenticated call. It returns whether the call succeeded. The auth
# work plugs in here later. The default passes through.
CanaryCall = Callable[[], bool]

# Returns the ``pmset -g sched`` text. The real one shells out. A test injects one.
ScheduleReader = Callable[[], str]


def _canary_pass_through() -> bool:
    return True


@dataclass(frozen=True)
class SundayOutcome:
    """What one Sunday maintenance run found and did.

    ``problems`` are the findings that withhold the ping: a failed scrub, a failed
    canary, a token that does not cover the coming week, or a mint time that could
    not be read. ``report`` carries the report-tier findings. Today that is only
    pmset alarm drift. The design pins drift to the nightly report, because the pre-open
    self-check already catches a missed wake an hour before the bell, so drift never
    withholds the ping. ``covered`` is ``None`` when the mint time could not be read.
    That is a problem, never a skip. ``pinged`` is the success condition.
    """

    scrub: ScrubResult
    alarms: AlarmCheck
    canary_passed: bool
    covered: bool | None
    pinged: bool
    problems: tuple[str, ...] = ()
    report: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.pinged


def sunday_maintenance(
    *,
    lake_root: Path,
    now: datetime,
    calendar: Calendar,
    schedule_reader: ScheduleReader,
    pinger: Pinger,
    ping_url: str,
    canary: CanaryCall = _canary_pass_through,
    mint: datetime | None = None,
) -> SundayOutcome:
    """Scrub, verify the wake alarms, run the canary, assert coverage, then ping.

    Every check runs and every finding is named, so one run reports all of them.
    The ping fires only when the scrub, the canary, and the coverage assertion pass.
    Alarm drift is checked and named in ``report`` but never withholds the ping. The
    coverage assertion needs the token's mint time. ``mint`` is ``None`` when the
    caller could not read it, and that withholds the ping. A coverage assertion that
    never ran must not read as a pass. The canary's 30-minute
    retry until the deadline is the launchd retry loop's job, not this function's.
    This function decides one attempt.
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

    alarms = check_alarms(
        parse_pmset_schedule(schedule_reader()),
        one_shot_date=expected_one_shot(now, calendar),
    )
    report = list(alarms.problems)

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

    pinged = False
    if not problems:
        pinger.ping(ping_url)
        pinged = True
    return SundayOutcome(
        scrub=result,
        alarms=alarms,
        canary_passed=canary_passed,
        covered=covered,
        pinged=pinged,
        problems=tuple(problems),
        report=tuple(report),
    )


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
    if _ACCOUNT.match(owner) is None:
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


def tmutil_exclusion_command(token_path: str) -> str:
    """The Time Machine exclusion for the token file. Runs as the user, no root."""
    return f'tmutil addexclusion "{token_path}"'


def default_token_path(home: str) -> str:
    """The token's standard location under ``home``. Machine-derived, never tracked."""
    return str(Path(home) / ".config" / "marketlake" / "token.json")


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
    """One file the dry-run renderer produces."""

    name: str
    content: str


def render_all(host: LaunchdHost, token_path: str) -> tuple[RenderedFile, ...]:
    """Every plist and setup file, as text, in install order."""
    files = [RenderedFile(f"{job.label}.plist", job.render()) for job in all_jobs(host, token_path)]
    files.append(RenderedFile(SUDOERS_FILE, sudoers_dropin(host.owner)))
    files.append(
        RenderedFile(
            TMUTIL_FILE,
            "#!/bin/sh\n# Keep the brokerage token out of Time Machine. Run as the owner.\n"
            f"{tmutil_exclusion_command(token_path)}\n",
        )
    )
    return tuple(files)


def _is_protected(target: Path) -> bool:
    resolved = target.resolve()
    return any(resolved == root or root in resolved.parents for root in _PROTECTED_ROOTS)


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
        written.append(path)
    return written


def install_commands(out_dir: Path, host: LaunchdHost, token_path: str) -> str:
    """The operator's manual install steps, as text. Nothing here runs from code."""
    out = Path(out_dir)
    lines = [
        "# Marketlake control plane: the manual install. Run each line by hand.",
        "# 1. Install the four LaunchDaemons, root-owned as launchd requires.",
    ]
    for job in all_jobs(host, token_path):
        lines.append(
            f"sudo install -o root -g wheel -m 644 {out / job.label}.plist /Library/LaunchDaemons/"
        )
    lines += [
        "# 2. Install the sudoers drop-in after visudo validates it.",
        f"sudo visudo -cf {out / SUDOERS_FILE} && "
        f"sudo install -o root -g wheel -m 440 {out / SUDOERS_FILE} /etc/sudoers.d/marketlake",
        "# visudo checks the syntax only. This prints the two rules as sudo parsed them,",
        "# which is what shows the one-shot's regular expression survived as one.",
        "sudo -l | grep pmset",
        "# 3. Set the weekday firmware wake, then read it back.",
        f"sudo {pmset_repeat_command()}",
        "pmset -g sched",
        "# 4. Keep the token out of Time Machine. As the owner, never under sudo.",
        tmutil_exclusion_command(token_path),
        "# 5. Load the jobs into the system domain, then confirm the daemon is running.",
    ]
    for job in all_jobs(host, token_path):
        lines.append(
            f"sudo launchctl bootstrap {LAUNCHD_DOMAIN} /Library/LaunchDaemons/{job.label}.plist"
        )
    lines.append(f"launchctl print {LAUNCHD_DOMAIN}/{DAEMON_LABEL}")
    lines.append("# The Sunday one-shot is set by the Friday vendor sweep, not here.")
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
    render.add_argument(
        "--path-dir",
        action="append",
        default=[],
        help="A directory to prepend to PATH, such as where uv lives. Repeatable.",
    )
    render.add_argument("--token", help="The token.json path to exclude from Time Machine.")

    check = sub.add_parser("self-check", help="Verify the daemon is up, then ping pre-open.")
    check.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    check.add_argument("--label", default=DAEMON_LABEL, help="The daemon's launchd label.")

    sunday = sub.add_parser("sunday", help="Scrub, verify the wake alarms, canary, then ping.")
    sunday.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    sunday.add_argument(
        "--token", help="Path to token.json. Defaults to the standard place under HOME."
    )
    sunday.add_argument(
        "--mint", help="Override the token's mint time, ISO 8601 with offset. Tests only."
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
) -> int:
    """The ``python -m lake.control_plane`` entry. Returns a process exit code.

    The seams default to the real ones and are built lazily, so a test injects fakes
    and nothing here reads the wall clock or shells out.
    """
    args = _build_parser().parse_args(argv)

    if args.command == "render":
        host = LaunchdHost(
            python=args.python,
            owner=args.owner,
            home=args.home,
            project_dir=args.project_dir,
            log_dir=args.log_dir,
            group=args.group,
            config_path=args.config,
            path_dirs=tuple(args.path_dir),
        )
        token_path = args.token if args.token is not None else default_token_path(args.home)
        out = Path(args.out)
        try:
            written = write_rendered(render_all(host, token_path), out)
        except ValueError as exc:
            print(f"render: {exc}")
            return 2
        for path in written:
            print(f"wrote {path}")
        print()
        print(install_commands(out, host, token_path), end="")
        return 0

    if args.command == "self-check":
        config = load_config(args.config)
        outcome = self_check(
            probe=probe if probe is not None else launchctl_probe,
            pinger=pinger if pinger is not None else UrllibPinger(),
            ping_url=config.healthchecks_url(PRE_OPEN_SLUG),
            label=args.label,
        )
        status = "daemon up" if outcome.daemon_up else "daemon down"
        print(f"self-check: {status}; pinged={outcome.pinged} slug={PRE_OPEN_SLUG}")
        return 0 if outcome.pinged else 1

    if args.command == "sunday":
        config = load_config(args.config)
        now = (clock if clock is not None else _system_clock()).now()
        mint: datetime | None
        if args.mint is not None:
            mint = datetime.fromisoformat(args.mint)
        else:
            token_path = (
                args.token if args.token is not None else default_token_path(str(Path.home()))
            )
            try:
                mint = read_token_mint(token_path)
            except ValueError as exc:
                print(f"sunday: {exc}")
                mint = None
        outcome = sunday_maintenance(
            lake_root=config.lake_root,
            now=now,
            calendar=calendar if calendar is not None else _exchange_calendar(),
            schedule_reader=schedule_reader if schedule_reader is not None else read_pmset_schedule,
            pinger=pinger if pinger is not None else UrllibPinger(),
            ping_url=config.healthchecks_url(SUNDAY_SLUG),
            canary=canary if canary is not None else _canary_pass_through,
            mint=mint,
        )
        for problem in outcome.problems:
            print(f"sunday: {problem}")
        for line in outcome.report:
            print(f"sunday: report: {line}")
        print(f"sunday: pinged={outcome.pinged} slug={SUNDAY_SLUG}")
        return 0 if outcome.pinged else 1

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
    "DAEMON_LABEL",
    "DASHBOARD_LABEL",
    "LAUNCHD_DOMAIN",
    "PRE_OPEN_SELF_CHECK",
    "PRE_OPEN_SLUG",
    "SELF_CHECK_LABEL",
    "SUDOERS_FILE",
    "SUNDAY_LABEL",
    "SUNDAY_MAINTENANCE",
    "SUNDAY_SLUG",
    "SUNDAY_WAKE",
    "TMUTIL_FILE",
    "TOKEN_LIFETIME",
    "WAKE_KINDS",
    "WEEKDAY_ASSERTION_END",
    "WEEKDAY_WAKE",
    "AlarmCheck",
    "AssertionRunner",
    "AssertionWindow",
    "CanaryCall",
    "DaemonProbe",
    "LaunchdHost",
    "OneShotAlarm",
    "PmsetParseError",
    "PmsetSchedule",
    "RenderedFile",
    "RepeatAlarm",
    "ScheduleReader",
    "SelfCheckOutcome",
    "SundayOutcome",
    "WallClockTime",
    "all_jobs",
    "assertion_window",
    "caffeinate_args",
    "check_alarms",
    "daemon_job",
    "dashboard_job",
    "default_token_path",
    "expected_one_shot",
    "hold_assertion",
    "install_commands",
    "launchctl_probe",
    "main",
    "next_sunday_wake",
    "parse_launchctl_print",
    "parse_pmset_schedule",
    "pmset_repeat_command",
    "pmset_schedule_command",
    "read_pmset_schedule",
    "read_token_mint",
    "render_all",
    "self_check",
    "self_check_job",
    "sudoers_dropin",
    "sunday_job",
    "sunday_maintenance",
    "sunday_wake_command",
    "tmutil_exclusion_command",
    "token_covers_week",
    "week_option_close",
    "write_rendered",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
