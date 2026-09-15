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
  It runs before the ping, so the ping attests it. It copies the lake root minus an
  explicit exclusion list, ``BACKUP_EXCLUSIONS``, whose entries are justified one by
  one where the list is defined.

Two external actions sit behind injected seams, so the whole test suite runs offline
with no network and no subprocess.

1. The *pinger* performs the health-check GET. The real one uses ``urllib``. A test
   injects a fake that records the URL.
2. The *backup runner* copies the lake to the SSD. The real one shells out to
   ``rsync`` after asserting the backup target is mounted. A test injects a fake. The
   real one holds a seam of its own for the command it runs, so a test can read the
   ``rsync`` argument list, exclusions included, without a copy ever happening.

The launchd schedule is a wall-clock time by necessity, because ``StartCalendarInterval``
cannot express a session-relative time. The generator takes the hour and minute as
integers and emits them into the plist as launchd's ``{"Hour": h, "Minute": m}``
integers. So no bare ``"HH:MM"`` string and no machine path ever lands in tracked
source. The schedule is configuration, regenerated when the timezone moves, exactly as
the design's schedule note requires.
"""

from __future__ import annotations

import http.client
import plistlib
import urllib.error
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.alert import Message, Publisher
from lake.capture import CycleResult, run_cycle_from_config
from lake.config import input_errors_exit, load_config
from lake.journal import ROW_KIND_DATA
from lake.paths import CONFIG_DIR_PARTS, TEMP_MARKER

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


# What a ping can fail with. ``urlopen`` fails two ways. Everything socket-shaped is an
# ``OSError``, including ``urllib.error.URLError``, its ``HTTPError`` subclass, and a
# timeout. A malformed response instead raises ``http.client.HTTPException``, which is
# not an ``OSError``, so catching only the socket family would let it through.
#
# Every job pings as its last step, after the work is done, and prints its verdict after
# that. So a raising ping used to cost the verdict as well as the ping. The ping is lost
# either way and healthchecks pages for it after the grace. Losing the report too is
# what these catches prevent. It lives here beside the protocol rather than in one
# caller, because all four call sites need the same answer.
#
# Only the exception's type is ever reported. The URL carries the ping key, and the
# design's rule is that it never reaches a log.
PING_FAILURES = (urllib.error.URLError, OSError, http.client.HTTPException)


# -- a ping the service refused ----------------------------------------------


# ``PING_FAILURES`` lumps two failures together that mean opposite things, and telling
# them apart is what this section exists for.
#
# A transport failure means the request never reached healthchecks. The design already
# answers that one. It sets the dead-man grace looser than the watchdog's on purpose,
# reasoning that a wifi blip drops pings while capture keeps journaling locally, so the
# check goes down by itself if the outage lasts and healthchecks pages from outside.
# Paging from the laptop would fail in that same outage anyway.
#
# A status response is the other failure and nothing answers it. ``urlopen`` raises
# ``urllib.error.HTTPError`` for one, and that is a ``URLError`` subclass carrying a
# ``.code``, which is why a ping to a slug with no row reported ``ping failed:
# HTTPError`` rather than a transport error name. A 4xx on a ping URL means healthchecks
# read the request and refused it, so the ping feeds no check at all. Either the slug has
# no row or the ping key is wrong. Both are permanent, both repeat every run, and no
# check will ever go down to report either, because there is no check. A missing row's
# only symptom is silence, and silence is what the check exists to report.
#
# A 5xx stays on the transport side. That is healthchecks failing rather than refusing,
# and the next run reaches it.

# The page a refused ping raises, per the design's message table.
PING_REFUSED_EVENT = "ping_refused"
PING_REFUSED_TITLE = "Health check ping refused"


def refused_status(exc: BaseException) -> int | None:
    """The status a refused ping came back with, or ``None`` when it was never refused.

    A transport failure carries no status, so it answers ``None`` and nothing pages.
    """
    if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
        return exc.code
    return None


def ping_refused_page(slug: str, status: int) -> Message:
    """The page one refused ping raises.

    It names the slug and the status and nothing else. The ping URL carries the ping
    key, so it never reaches a page, and the publisher refuses one that does.
    """
    return Message(
        event=PING_REFUSED_EVENT,
        title=PING_REFUSED_TITLE,
        body=(
            f"{slug}: healthchecks answered {status}, so this ping feeds no check. "
            f"Until a row for {slug} exists and is armed, its silence means nothing."
        ),
    )


def escalate_ping_failure(
    exc: BaseException,
    *,
    slug: str,
    publisher: Publisher | None,
    now: datetime,
) -> bool:
    """Page for a refused ping, and say nothing for one that merely did not land.

    Returns whether a page went out. A caller with no publisher escalates nothing, which
    is what lets a test drive a producer without a page reaching anywhere.
    """
    status = refused_status(exc)
    if publisher is None or status is None:
        return False
    publisher.publish(ping_refused_page(slug, status), now=now)
    return True


class SlugEscalation:
    """Escalates a refused ping once per slug, and re-arms when a ping to it lands.

    A slug with no row is refused on every run forever, so a producer that paged on each
    refusal would page on each ping. The standing rule is to page once on the transition
    and reset on a success, and this holds that state in memory keyed by slug, so it
    lasts exactly as long as the producer holding it.

    The one-shot jobs page at most once per run by construction and need none of this.
    ``DeadMan`` pings roughly 390 times a session inside a daemon that outlives every one
    of them, so it is the site this was built for.
    """

    def __init__(self, publisher: Publisher | None = None) -> None:
        self._publisher = publisher
        self._paged: set[str] = set()

    def landed(self, slug: str) -> None:
        """Note a ping that landed, so a later refusal of this slug pages again."""
        self._paged.discard(slug)

    def failed(self, exc: BaseException, *, slug: str, now: datetime) -> bool:
        """Page for a refused ping unless this slug has already paged. Returns whether it did."""
        if slug in self._paged:
            return False
        if not escalate_ping_failure(exc, slug=slug, publisher=self._publisher, now=now):
            return False
        self._paged.add(slug)
        return True


# The backup's exclusion list. The design pins the sync root as ``lake/`` only, with an
# explicit exclusion list, and this is that list.
#
# How rsync reads a pattern, because the shape of each entry below turns on it. A
# pattern holding no "/" is matched against a path's last component, so it drops that
# name wherever in the tree it appears. A pattern holding a "/" is matched against the
# end of the whole path. A trailing "/" narrows the match to directories, and an
# excluded directory is never descended into.
#
# The bar for an entry is high. An over-broad pattern drops real data and the sync
# still exits clean, so the loss surfaces only at a restore. Two entries clear it.
#
# 1. The temp file an atomic write leaves behind. Every one is built by
#    ``paths.temp_write_path``, which is why the marker is a constant there rather than
#    a literal here. A temp file is working state, never durable data. It exists only
#    when a writer died between its write and its rename, and the re-run that finishes
#    the interrupted job rebuilds the partition from the journal. So dropping it loses
#    nothing. It also carries no manifest entry and never will, so copying one plants
#    an orphan on the backup. And a temp holds a whole partition's bytes, so the copy
#    costs real space and real sync time.
# 2. The config directory, holding ``token.json`` and ``config.yaml``. The token is a
#    full brokerage credential and ``config.yaml`` holds four secrets. The design's
#    rule is that neither may ride onto a backup disk that lacks FileVault. That
#    directory sits outside the sync root today by construction, so this pattern
#    matches nothing and costs nothing. It is here so the rule holds by exclusion
#    rather than by luck. The first widening of the sync root would otherwise put the
#    credential on the SSD, and a backup that already ran cannot be un-run. The entry
#    names the directory rather than the two files for the reason the Time Machine
#    exclusion does: excluding the token alone leaves ``config.yaml``'s secrets behind.
#
# Considered and rejected, pinned here so none is re-proposed. ``journal/`` holds the
# only copy of the day's capture until close+15 seals it, which is the single-copy
# window the backup exists to close. ``manifest.jsonl`` is the integrity root, and a
# restore without it can verify nothing. ``quarantine.jsonl`` and ``reports/`` are
# pinned by the design as inside the sync root. ``.DS_Store`` is Finder state rather
# than anything this system writes, the primary's own reverse scrub already names it as
# an orphan, and it costs a few kilobytes against a temp file's whole partition.
BACKUP_EXCLUSIONS: tuple[str, ...] = (
    f"*{TEMP_MARKER}*",
    "/".join(CONFIG_DIR_PARTS) + "/",
)


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
    offline suite can build one without consequence. The GET runs from every job that
    pings, the daily runner and the compaction job here, the weekday self-check and the
    Sunday job in the control plane, and from the by-hand live check. A test injects a
    fake instead.
    """

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._timeout = timeout_seconds

    def ping(self, url: str) -> None:
        import urllib.request  # lazy: only the live check makes a real request

        # The URL is the configured health check, not attacker-controlled input.
        with urllib.request.urlopen(url, timeout=self._timeout) as response:
            response.read()


class RsyncBackup:
    """The real backup runner: ``rsync``, under the compaction job's lake-root flock.

    It asserts the backup target is mounted, then copies ``lake/`` into it, minus
    ``BACKUP_EXCLUSIONS``. The design pins the tool as ``rsync`` or ``rclone``, the lake
    root as the only sync root, an explicit exclusion list, and a mount check before the
    copy. The ``subprocess`` call runs from the compaction job after every session, and
    from the by-hand live check.

    The command runner is a seam of its own, so a test can read the argument list this
    builds without a real ``rsync`` ever running. ``run`` defaults to ``subprocess``,
    imported lazily, so the offline suite never loads it. A caller that wants the fake
    one layer up injects a whole ``BackupRunner`` instead.
    """

    def __init__(
        self,
        extra_args: Sequence[str] = (),
        run: Callable[[list[str]], object] | None = None,
    ) -> None:
        self._extra_args = tuple(extra_args)
        self._run = run

    @staticmethod
    def _subprocess_run(args: list[str]) -> None:
        import subprocess  # lazy: only the live check shells out

        subprocess.run(args, check=True)

    def sync(self, source: Path, target: Path) -> None:
        source = Path(source)
        target = Path(target)
        if not target.exists() or not target.is_dir():
            raise BackupTargetUnavailable(f"backup target not mounted: {target}")
        # A trailing slash on the source copies its contents into the target. ``-a``
        # preserves metadata, so size and mtime mean the same thing on both sides and
        # the default size-and-mtime comparison is a sound change detector here.
        #
        # ``--checksum`` is gone, and the design pins it as cut. It never saw a
        # half-written target, only bit rot, and it read and MD4-hashed both trees every
        # day to do it. What kept it here was that nothing else would notice the backup
        # rotting. ``manifest.backup_scrub`` notices now, on the Sunday job, and it
        # names the file it found. The flag never did: it copied over the rot and exited
        # clean, so a failing disk reported nothing.
        #
        # The exclusions come before ``extra_args``, so a caller's extra flags can never
        # land between them. The list is policy rather than a parameter, and a caller
        # that could interpose within it could break a pattern into an operand.
        args = [
            "rsync",
            "-a",
            *(f"--exclude={pattern}" for pattern in BACKUP_EXCLUSIONS),
            *self._extra_args,
            f"{source}/",
            f"{target}/",
        ]
        run = self._run if self._run is not None else self._subprocess_run
        run(args)


# -- the orchestration -------------------------------------------------------


@dataclass(frozen=True)
class RunOutcome:
    """What one slice-1 run did.

    ``succeeded`` is the durable-capture success condition. ``pinged`` and ``backed_up``
    record whether each success-gated step ran. On a failed cycle both are false.
    ``problem`` names a ping that failed, which leaves ``pinged`` false and the run
    itself successful. The capture is durable either way.
    """

    result: CycleResult
    succeeded: bool
    pinged: bool
    backed_up: bool
    problem: str | None = None


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
    problem: str | None = None
    if succeeded:
        # Backup first. A raised backup propagates before the ping, so a single-copy
        # window pages through the missed ping rather than being reported as healthy.
        backup.sync(lake_root, backup_target)
        backed_up = True
        try:
            pinger.ping(ping_url)
            pinged = True
        except PING_FAILURES as exc:
            problem = f"ping failed: {type(exc).__name__}"
    return RunOutcome(
        result=result, succeeded=succeeded, pinged=pinged, backed_up=backed_up, problem=problem
    )


def run_once_from_config(
    *,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    slug: str = SLICE1_RUNNER_SLUG,
    pinger: Pinger,
    backup: BackupRunner,
) -> RunOutcome:
    """Run one slice-1 cycle wired from the real config and seams.

    This is the entry ``python -m lake.runner run`` calls. It loads the machine-local
    config and builds the health-check URL from the config's ping key.

    ``pinger`` and ``backup`` are required and have no live defaults. Both reach past
    this process, one to healthchecks and one to the backup target over ``rsync``, and a
    default would hand them to a caller that never asked. ``main`` builds the live pair.
    A test drives ``run_once`` directly with fakes instead.
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
        pinger=pinger,
        ping_url=ping_url,
        backup=backup,
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

    A resident process has no calendar interval. It sets ``keep_alive`` instead, the
    launchd key that relaunches an exiting process within seconds. The slice-2 daemon
    and the query service are that shape. A job with no interval, no keep-alive, and no
    run-at-load would never start, so that combination is refused.
    """

    label: str
    program_arguments: tuple[str, ...]
    calendar_interval: dict[str, int] | list[dict[str, int]] | None = None
    working_directory: str | None = None
    standard_out_path: str | None = None
    standard_error_path: str | None = None
    environment: dict[str, str] = field(default_factory=dict)
    run_at_load: bool = False
    user_name: str | None = None
    group_name: str | None = None
    keep_alive: bool = False

    def __post_init__(self) -> None:
        if self.calendar_interval is None and not (self.keep_alive or self.run_at_load):
            raise ValueError(
                f"{self.label}: a job needs a calendar interval, KeepAlive, or RunAtLoad"
            )

    def to_dict(self) -> dict[str, object]:
        """The plist as a Python dict, with launchd's own key names."""
        plist: dict[str, object] = {
            "Label": self.label,
            "ProgramArguments": list(self.program_arguments),
        }
        if self.calendar_interval is not None:
            plist["StartCalendarInterval"] = self.calendar_interval
        plist["RunAtLoad"] = self.run_at_load
        if self.keep_alive:
            plist["KeepAlive"] = True
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
        with input_errors_exit("runner"):
            outcome = run_once_from_config(
                config_path=args.config,
                tickers_path=args.tickers,
                token_path=args.token,
                # The only construction site in this module.
                pinger=UrllibPinger(),
                backup=RsyncBackup(),
            )
        # Report by slug and counts only. The ping URL carries the secret ping key and
        # is never printed.
        status = "captured" if outcome.succeeded else "no durable data"
        if outcome.problem is not None:
            print(f"slice-1 run: {outcome.problem}")
        print(
            f"slice-1 run: {status} "
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
    "BACKUP_EXCLUSIONS",
    "DAILY_LABEL",
    "MEASUREMENT_LABEL",
    "PING_REFUSED_EVENT",
    "PING_REFUSED_TITLE",
    "SLICE1_RUNNER_SLUG",
    "BackupRunner",
    "BackupTargetUnavailable",
    "LaunchdJob",
    "Pinger",
    "RsyncBackup",
    "RunOutcome",
    "SlugEscalation",
    "UrllibPinger",
    "calendar_interval",
    "cycle_succeeded",
    "daily_runner_job",
    "escalate_ping_failure",
    "main",
    "measurement_runner_job",
    "minute_intervals",
    "ping_refused_page",
    "refused_status",
    "run_once",
    "run_once_from_config",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
