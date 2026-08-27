"""The slice-1 runner.

This is the throwaway scheduler wrapper around the D7 capture primitive. It runs one
capture cycle, and only on a successful durable cycle it copies the lake to the backup
SSD and then pings a health check. It also generates the launchd job that fires the
cycle on a wall-clock schedule. Everything here retires when the slice-2 daemon lands.
The one piece that survives is the run-cycle-then-rsync-then-ping orchestration, which
the daemon inherits, so it is kept clean and behind injected seams.

Terms, glossed at first use.

- *launchd* is macOS's built-in service manager and job scheduler. A launchd job is
  described by a *plist*, a property-list file (an XML dictionary) that names what to
  run and when.
- *StartCalendarInterval* is the launchd key that fires a job at a fixed wall-clock
  time, like "every day at 16:10." Unlike cron, launchd remembers a run it slept
  through and fires it once on wake. That is a *sleep-missed* run, coalesced into one
  catch-up. The daily capture is pinned near the option close, after the OI-refresh
  moment, so a coalesced catch-up fires later than scheduled and the pin still holds.
- A *health check* is an external dead-man timer at healthchecks.io. The runner pings
  it last, after both the capture and the backup succeed. Silence past the timer's
  grace pages the owner. Slice 1 has one such check, so its ping must attest both
  things at once: capture happened and the backup landed. The design promises even
  slice 1 never runs the capture dark or single-copy, so this one check has to catch
  both failures. A missed ping therefore means capture-dark *or* single-copy. In slice
  2 these split into separate checks, one for capture and one for compaction-plus-backup.
  The design's rule holds throughout: a ping fires only on the job's success condition,
  never on mere process liveness. So a cycle that captured nothing must not ping, and
  neither must a cycle whose backup failed.
- *rsync* is the standard file-copy tool. The backup step copies ``lake/`` to an
  external SSD so even slice 1 never leaves the un-buy-backable capture in one place.
  It runs before the ping, so the ping attests it.

Two external actions sit behind injected seams, so the whole test suite runs offline
with no network and no subprocess.

1. The *pinger* performs the health-check GET. The real one uses ``urllib``. A test
   injects a fake that records the URL.
2. The *backup runner* copies the lake to the SSD. The real one shells out to
   ``rsync`` after asserting the backup target is mounted. A test injects a fake.

The launchd schedule is a wall-clock time by necessity, because ``StartCalendarInterval``
cannot express a session-relative time. The generator takes the hour and minute as
integers and emits them into the plist as launchd's ``{"Hour": h, "Minute": m}``
integers. So no bare ``"HH:MM"`` string and no machine path ever lands in tracked
source. The schedule is configuration, regenerated when the timezone moves, exactly as
the design's schedule note requires.
"""

from __future__ import annotations

import plistlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.capture import CycleResult, run_cycle_from_config
from lake.config import load_config
from lake.journal import ROW_KIND_DATA

# The health-check slug the slice-1 runner pings. It is its own check, deliberately
# separate from the steady-state six, because it retires with this launchd entry when
# the slice-2 per-cycle dead-man check supersedes it. Log the slug, never the ping URL,
# which carries the secret ping key.
SLICE1_RUNNER_SLUG = "slice1-capture"

# The reverse-DNS launchd labels for the two slice-1 jobs. A label is the job's unique
# identity to launchd. These are defaults a caller may override; they carry no machine
# path and no session time.
DAILY_LABEL = "com.marketlake.slice1.capture"
MEASUREMENT_LABEL = "com.marketlake.slice1.measurement"

# Minutes in a day, the clamp for a generated minute schedule.
_MINUTES_PER_DAY = 24 * 60


# -- the injected seams ------------------------------------------------------


@runtime_checkable
class Pinger(Protocol):
    """Performs the health-check ping. The real one does an HTTP GET."""

    def ping(self, url: str) -> None:
        """GET the health-check URL. A failure raises."""
        ...


@runtime_checkable
class BackupRunner(Protocol):
    """Copies the lake to the backup target. The real one shells out to ``rsync``."""

    def sync(self, source: Path, target: Path) -> None:
        """Copy ``source`` into ``target``. Asserts the target is mounted first."""
        ...


class BackupTargetUnavailable(Exception):
    """Raised when the backup target is not mounted.

    The design's rule is that an unplugged SSD fails the run loudly rather than
    silently skipping the backup. A backup that quietly no-ops is a lost backup.
    """


class UrllibPinger:
    """The real pinger: a plain HTTP GET with ``urllib``.

    It is constructed cheaply and imports nothing network-bound at module load, so the
    offline suite can build one without consequence. The GET itself only happens in the
    by-hand live check. A test injects a fake instead.
    """

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._timeout = timeout_seconds

    def ping(self, url: str) -> None:
        import urllib.request  # lazy: only the live check makes a real request

        # The URL is the configured health check, not attacker-controlled input.
        with urllib.request.urlopen(url, timeout=self._timeout) as response:
            response.read()


class RsyncBackup:
    """The real backup runner: ``rsync`` with checksum verification.

    It asserts the backup target is mounted, then copies ``lake/`` into it. The design
    pins the tool as ``rsync`` or ``rclone`` with checksum verification, the lake root
    as the only sync root, and a mount check before the copy. The ``subprocess`` call
    runs only in the by-hand live check. A test injects a fake.
    """

    def __init__(self, extra_args: Sequence[str] = ()) -> None:
        self._extra_args = tuple(extra_args)

    def sync(self, source: Path, target: Path) -> None:
        import subprocess  # lazy: only the live check shells out

        source = Path(source)
        target = Path(target)
        if not target.exists() or not target.is_dir():
            raise BackupTargetUnavailable(f"backup target not mounted: {target}")
        # A trailing slash on the source copies its contents into the target. ``-a``
        # preserves metadata; ``--checksum`` matches the scrub's partial-sync detection.
        args = [
            "rsync",
            "-a",
            "--checksum",
            *self._extra_args,
            f"{source}/",
            f"{target}/",
        ]
        subprocess.run(args, check=True)


# -- the orchestration -------------------------------------------------------


@dataclass(frozen=True)
class RunOutcome:
    """What one slice-1 run did.

    ``succeeded`` is the durable-capture success condition. ``pinged`` and ``backed_up``
    record whether each success-gated step ran. On a failed cycle both are false.
    """

    result: CycleResult
    succeeded: bool
    pinged: bool
    backed_up: bool


def cycle_succeeded(result: CycleResult) -> bool:
    """Whether a cycle counts as a successful durable capture.

    Success needs at least one durable data row and no segment that failed to journal.
    Mere gap rows are not success. A cycle that captured nothing is the crash-looping
    zombie the design's dead-man rule must stay silent for. So this gates the backup
    and, through it, the ping: only a cycle that landed real data is worth backing up
    and reporting.
    """
    if result.errors:
        return False
    return any(segment.row_kind == ROW_KIND_DATA for segment in result.segments)


def run_once(
    cycle_runner: Callable[[], CycleResult],
    *,
    pinger: Pinger,
    ping_url: str,
    backup: BackupRunner,
    lake_root: Path,
    backup_target: Path,
) -> RunOutcome:
    """Run one capture cycle, then back up and ping on success.

    The orchestration in order:

    1. Run one cycle through the injected ``cycle_runner``. In production this is the
       D7 primitive wired from config. A raised failure propagates, so the process
       exits non-zero and launchd logs it, and neither the backup nor the ping runs.
    2. Judge success with ``cycle_succeeded``.
    3. On success only, run the ``lake/`` to SSD backup first, then ping the health
       check. The ping comes last so it attests both the capture and the backup. Slice
       1 has one check, so the ping is the single evidence that the day is neither
       capture-dark nor single-copy. If the backup raises, the ping never fires and the
       error surfaces, so the missed ping makes the dead-man catch the single-copy
       window, not just capture-dark. A failed or empty cycle does neither step.

    Every I/O boundary is injected, so this whole function runs offline in a test.
    """
    result = cycle_runner()
    succeeded = cycle_succeeded(result)
    pinged = False
    backed_up = False
    if succeeded:
        # Backup first. A raised backup propagates before the ping, so a single-copy
        # window pages through the missed ping rather than being reported as healthy.
        backup.sync(lake_root, backup_target)
        backed_up = True
        pinger.ping(ping_url)
        pinged = True
    return RunOutcome(result=result, succeeded=succeeded, pinged=pinged, backed_up=backed_up)


def run_once_from_config(
    *,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    slug: str = SLICE1_RUNNER_SLUG,
    pinger: Pinger | None = None,
    backup: BackupRunner | None = None,
) -> RunOutcome:
    """Run one slice-1 cycle wired from the real config and seams.

    This is the entry ``python -m lake.runner run`` calls. It loads the machine-local
    config, builds the health-check URL from the config's ping key, and defaults the
    seams to the real ``urllib`` pinger and ``rsync`` backup. The Schwab client, the
    network GET, and the subprocess are all built lazily, so importing this module and
    running the offline suite touches none of them. A test drives ``run_once`` directly
    with fakes instead.
    """
    config = load_config(config_path)
    ping_url = config.healthchecks_url(slug)

    def cycle_runner() -> CycleResult:
        return run_cycle_from_config(
            config_path=config_path,
            tickers_path=tickers_path,
            token_path=token_path,
        )

    return run_once(
        cycle_runner,
        pinger=pinger if pinger is not None else UrllibPinger(),
        ping_url=ping_url,
        backup=backup if backup is not None else RsyncBackup(),
        lake_root=config.lake_root,
        backup_target=config.backup_target,
    )


# -- the launchd plist generator ---------------------------------------------


@dataclass(frozen=True)
class LaunchdJob:
    """A launchd job description, ready to render as a plist.

    ``calendar_interval`` is either one ``{"Hour": h, "Minute": m}`` dict for a single
    daily fire, or a list of such dicts for a bounded minute-by-minute schedule.
    launchd accepts both shapes under ``StartCalendarInterval``. Every value here is
    supplied by the caller, so no machine path and no session-time literal is baked in.
    """

    label: str
    program_arguments: tuple[str, ...]
    calendar_interval: dict[str, int] | list[dict[str, int]]
    working_directory: str | None = None
    standard_out_path: str | None = None
    standard_error_path: str | None = None
    environment: dict[str, str] = field(default_factory=dict)
    run_at_load: bool = False
    user_name: str | None = None
    group_name: str | None = None

    def to_dict(self) -> dict[str, object]:
        """The plist as a Python dict, with launchd's own key names."""
        plist: dict[str, object] = {
            "Label": self.label,
            "ProgramArguments": list(self.program_arguments),
            "StartCalendarInterval": self.calendar_interval,
            "RunAtLoad": self.run_at_load,
        }
        if self.working_directory is not None:
            plist["WorkingDirectory"] = self.working_directory
        if self.standard_out_path is not None:
            plist["StandardOutPath"] = self.standard_out_path
        if self.standard_error_path is not None:
            plist["StandardErrorPath"] = self.standard_error_path
        if self.environment:
            plist["EnvironmentVariables"] = dict(self.environment)
        if self.user_name is not None:
            plist["UserName"] = self.user_name
        if self.group_name is not None:
            plist["GroupName"] = self.group_name
        return plist

    def render(self) -> str:
        """The job as a launchd XML plist string."""
        return plistlib.dumps(self.to_dict(), fmt=plistlib.FMT_XML).decode("utf-8")


def calendar_interval(hour: int, minute: int) -> dict[str, int]:
    """One ``StartCalendarInterval`` entry, as launchd's integer ``Hour``/``Minute``.

    The hour and minute arrive as integers, never as a parsed ``"HH:MM"`` string, so
    the enforcement scanner stays green and the schedule stays configuration.
    """
    if not (0 <= hour < 24):
        raise ValueError(f"hour out of range: {hour}")
    if not (0 <= minute < 60):
        raise ValueError(f"minute out of range: {minute}")
    return {"Hour": hour, "Minute": minute}


def minute_intervals(
    start_hour: int, start_minute: int, end_hour: int, end_minute: int
) -> list[dict[str, int]]:
    """Every minute from start to end inclusive, as ``StartCalendarInterval`` entries.

    This builds the bounded minutely measurement schedule the design names: one launchd
    fire per minute across the measurement window. Expressing it as explicit per-minute
    calendar entries keeps launchd's sleep-missed coalescing, which a plain interval
    timer would not give. All four bounds are integers, so no session-time literal is
    ever written here.
    """
    start = start_hour * 60 + start_minute
    end = end_hour * 60 + end_minute
    if not (0 <= start < _MINUTES_PER_DAY) or not (0 <= end < _MINUTES_PER_DAY):
        raise ValueError("schedule bounds must fall within a single day")
    if end < start:
        raise ValueError("end must not precede start")
    return [calendar_interval(total // 60, total % 60) for total in range(start, end + 1)]


def daily_runner_job(
    *,
    python: str,
    hour: int,
    minute: int,
    label: str = DAILY_LABEL,
    working_directory: str | None = None,
    standard_out_path: str | None = None,
    standard_error_path: str | None = None,
    config_path: str | None = None,
    user_name: str | None = None,
    group_name: str | None = None,
) -> LaunchdJob:
    """The daily near-close capture job.

    ``python`` is the interpreter path, a machine value the caller supplies at run time,
    never a tracked literal. The job runs ``python -m lake.runner run``. An optional
    ``config_path`` is passed to the run through the ``MARKETLAKE_CONFIG`` environment
    variable, keeping the machine path out of the program arguments and out of source.
    """
    environment = {"MARKETLAKE_CONFIG": config_path} if config_path is not None else {}
    return LaunchdJob(
        label=label,
        program_arguments=(python, "-m", "lake.runner", "run"),
        calendar_interval=calendar_interval(hour, minute),
        working_directory=working_directory,
        standard_out_path=standard_out_path,
        standard_error_path=standard_error_path,
        environment=environment,
        user_name=user_name,
        group_name=group_name,
    )


def measurement_runner_job(
    *,
    python: str,
    start_hour: int,
    start_minute: int,
    end_hour: int,
    end_minute: int,
    label: str = MEASUREMENT_LABEL,
    working_directory: str | None = None,
    standard_out_path: str | None = None,
    standard_error_path: str | None = None,
    config_path: str | None = None,
    user_name: str | None = None,
    group_name: str | None = None,
) -> LaunchdJob:
    """The bounded minutely measurement job across the first sessions.

    It fires ``python -m lake.runner run`` once per minute from the start bound through
    the end bound. Those cycles produce the fetch-latency and sizing distributions the
    day-one measurements read. Like the daily job, all schedule values are integers or
    caller-supplied paths, so nothing machine-specific or time-literal is baked in.
    """
    environment = {"MARKETLAKE_CONFIG": config_path} if config_path is not None else {}
    return LaunchdJob(
        label=label,
        program_arguments=(python, "-m", "lake.runner", "run"),
        calendar_interval=minute_intervals(start_hour, start_minute, end_hour, end_minute),
        working_directory=working_directory,
        standard_out_path=standard_out_path,
        standard_error_path=standard_error_path,
        environment=environment,
        user_name=user_name,
        group_name=group_name,
    )


# -- the command-line entry --------------------------------------------------


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.runner",
        description="The slice-1 capture runner and its launchd plist generator.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run one capture cycle, then ping and back up on success.")
    run.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    run.add_argument("--tickers", help="Path to tickers.yaml (defaults to the standard location).")
    run.add_argument("--token", help="Path to token.json (defaults to the standard location).")

    plist = sub.add_parser("plist", help="Print a launchd plist to standard output.")
    plist_sub = plist.add_subparsers(dest="kind", required=True)

    common_paths = argparse.ArgumentParser(add_help=False)
    common_paths.add_argument("--python", required=True, help="Path to the Python interpreter.")
    common_paths.add_argument("--label", help="The launchd job label.")
    common_paths.add_argument("--working-dir", help="WorkingDirectory for the job.")
    common_paths.add_argument("--stdout", help="StandardOutPath for the job.")
    common_paths.add_argument("--stderr", help="StandardErrorPath for the job.")
    common_paths.add_argument("--config", help="Config path, passed via MARKETLAKE_CONFIG.")
    common_paths.add_argument("--user", help="UserName to run the job as.")
    common_paths.add_argument("--group", help="GroupName to run the job as.")

    daily = plist_sub.add_parser("daily", parents=[common_paths], help="The daily near-close job.")
    daily.add_argument("--hour", type=int, required=True, help="Schedule hour, 0-23 local time.")
    daily.add_argument("--minute", type=int, required=True, help="Schedule minute, 0-59.")

    measure = plist_sub.add_parser(
        "measurement", parents=[common_paths], help="The bounded minutely measurement job."
    )
    measure.add_argument("--start-hour", type=int, required=True, help="Window start hour, 0-23.")
    measure.add_argument("--start-minute", type=int, required=True, help="Window start minute.")
    measure.add_argument("--end-hour", type=int, required=True, help="Window end hour, 0-23.")
    measure.add_argument("--end-minute", type=int, required=True, help="Window end minute.")

    return parser


def _daily_kwargs(args) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "python": args.python,
        "working_directory": args.working_dir,
        "standard_out_path": args.stdout,
        "standard_error_path": args.stderr,
        "config_path": args.config,
        "user_name": args.user,
        "group_name": args.group,
    }
    if args.label is not None:
        kwargs["label"] = args.label
    return kwargs


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.runner`` entry. Returns a process exit code."""
    args = _build_parser().parse_args(argv)

    if args.command == "run":
        outcome = run_once_from_config(
            config_path=args.config,
            tickers_path=args.tickers,
            token_path=args.token,
        )
        # Report by slug and counts only. The ping URL carries the secret ping key and
        # is never printed.
        status = "captured" if outcome.succeeded else "no durable data"
        print(
            f"slice-1 run: {status}; "
            f"segments={len(outcome.result.segments)} "
            f"pinged={outcome.pinged} backed_up={outcome.backed_up} "
            f"slug={SLICE1_RUNNER_SLUG}"
        )
        return 0 if outcome.succeeded else 1

    if args.command == "plist":
        if args.kind == "daily":
            job = daily_runner_job(hour=args.hour, minute=args.minute, **_daily_kwargs(args))
        else:
            job = measurement_runner_job(
                start_hour=args.start_hour,
                start_minute=args.start_minute,
                end_hour=args.end_hour,
                end_minute=args.end_minute,
                **_daily_kwargs(args),
            )
        print(job.render())
        return 0

    return 2  # pragma: no cover - argparse requires a subcommand


__all__ = [
    "DAILY_LABEL",
    "MEASUREMENT_LABEL",
    "SLICE1_RUNNER_SLUG",
    "BackupRunner",
    "BackupTargetUnavailable",
    "LaunchdJob",
    "Pinger",
    "RsyncBackup",
    "RunOutcome",
    "UrllibPinger",
    "calendar_interval",
    "cycle_succeeded",
    "daily_runner_job",
    "main",
    "measurement_runner_job",
    "minute_intervals",
    "run_once",
    "run_once_from_config",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
