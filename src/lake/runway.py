"""The disk runway: what the lake holds, how fast it grows, and how long the disk lasts.

Two consumers read this module and it is deliberately a leaf, importing only the standard
library, :mod:`lake.calendar` and :mod:`lake.paths`. The dashboard's Lake panel renders
what it returns. The Sunday run's third duty flags the nightly report when the headroom
runs short, which is marketlake #438. One computation with two consumers is the point:
two independent ones would drift, and a panel and an alarm disagreeing about how long the
disk lasts is worse than either being wrong alone.

It could not live in :mod:`lake.dashboard`. That module already reads
``lake.control_plane``, so a Sunday duty reading back into it would close a cycle.

Four decisions are worth reading before the code.

1. **The bytes are allocated blocks, never file sizes.** What fills a disk is blocks. A
   4 KiB block holds a 165-byte nightly report as surely as a 4 KiB one, and the lake's
   ``reports/`` tree is 6,275 bytes of content in 155,648 bytes of blocks across 38
   files. It is also the tree with no pruning step, since a held finding files again
   every night it survives, so the divergence grows. ``st_blocks`` is in 512-byte units
   by POSIX convention whatever the filesystem's own block size is.
2. **The growth rate is the busiest day in the window, not the mean.** Measured over the
   live lake, the same bytes give a runway from 2,059 days to 20,647 depending only on
   what the rate is divided by, because capture began partway through the window and the
   idle days before it drag any mean down. Every such error lengthens the runway, and a
   check that flags short headroom never fires if its rate is too low. The mean over days
   that wrote bytes is reported beside the peak so a reader sees the spread. The price is
   named: one anomalous day, a backfill or a reseal, shortens the runway and can flag
   early. A report-tier finding sends no message of its own, so a false flag costs a line
   an operator reads, while a missed flag costs the disk, and a minute lost to a full
   disk is gone forever.
3. **The runway's unit is capture days, so anything in weeks or years goes through the
   calendar.** Growth happens on sessions. 2,059 capture days is 8.2 years over a
   252-session year, not the 5.6 a division by 365 would give. So the exhaustion date is
   walked forward one session at a time, and the headroom test compares dates rather than
   converting a count.
4. **Free space is the device's, not a lake allowance.** ``lake_root`` sits on a volume
   the rest of the machine shares. On the machine this was written against the lake is
   1.6 GB of 734 GiB used, so more than 99 percent of what consumes the runway is not the
   lake and the number moves when anything else grows. ``shutil.disk_usage`` is the
   reader rather than ``os.statvfs``, because ``statvfs`` reports two block sizes and
   only ``f_frsize`` is the one ``f_bavail`` is counted in. Multiplying by ``f_bsize``
   instead overstates free space 256 times, in the same direction as the mean above, and
   the wrapper applies the right one.

**Nothing here raises for a lake it cannot read.** The dashboard turns any escape into a
500 for the whole panel, which would throw away the refusal lines this module exists to
report. So a directory that will not list and a file that will not stat are values in
:class:`Usage`, not exceptions. The walk is ``os.walk`` with an ``onerror`` handler and
never ``Path.rglob``, which drops an unreadable directory and reports nothing at all. A
file that vanishes between the listing and the stat is a different thing and is skipped
silently: compaction prunes an emptied directory while holding the lake lock, so a file
the walk just saw is gone every weekday at close+15 on a lake behaving exactly as
designed.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from lake.calendar import Calendar
from lake.paths import JOURNAL_DIR, JSONL_SUFFIX, PARQUET_SUFFIX, TIMING_DIR, parse_date_dir

# How many trailing calendar days the growth rate is measured over. It matches the
# History panel's window so the page's two spans read alike to a human, and the two
# constants are independent in code because this module is a leaf and cannot read that
# one without becoming the cycle the module docstring describes.
GROWTH_WINDOW_DAYS = 30

# The headroom the design calls "a few weeks". Below this the runway is short, which the
# nightly report flags. It lives here rather than in ``GuardConstants`` because that
# class is scoped to the distributions slice 1 measures and recalibrates, and headroom in
# weeks is a design constant no measurement moves. The free bytes it resolves to differ
# per machine. The weeks do not.
HEADROOM_WEEKS = 3

# ``st_blocks`` is counted in 512-byte units by POSIX, whatever the filesystem's own
# block size is. It is spelled once.
BLOCK_BYTES = 512

# How many refused paths one reading names before it stops and says how many are left.
# It follows ``manifest._NAMED_PATHS``: a disk going bad names every file it carries, and
# a line per file would bury every other thing the panel has to say.
NAMED_REFUSALS = 3

# The range errors a calendar raises for a day outside the schedule it knows. This is the
# tuple ``lake.dashboard`` already uses, restated here because this module may not import
# it. ``exchange_calendars.errors.DateOutOfBounds`` is a ``ValueError``, so the pair
# covers it without naming the vendor's class.
_CALENDAR_RANGE_ERRORS = (ValueError, OverflowError)

# How far the exhaustion walk steps before it gives up and reports no date.
#
# The real calendar already stops it: ``exchange_calendars`` knows roughly a year ahead
# and refuses anything past that. The bound is here because nothing in the ``Calendar``
# protocol promises a horizon, and a calendar that answers every day walks to ``date.max``
# and raises ``OverflowError`` on the increment. A test calendar that answers every day is
# the ordinary way to meet that, and it did.
#
# Two years is double the real horizon and loses nothing. The capture-day count is exact
# whatever this is, the date is a convenience, and the headroom test asks about weeks, so
# a runway too long to date is a runway too long to flag.
MAX_FORWARD_DAYS = 366 * 2


@dataclass(frozen=True)
class Entry:
    """One top-level thing under the lake root, with its allocated bytes and file count.

    An entry is a surface directory, a tree like ``journal/`` or ``reports/``, or a flat
    root ledger like ``manifest.jsonl``. They are not all surfaces, and calling them that
    would hide the largest one: ``manifest.jsonl`` is four times the whole ``quotes``
    surface today.
    """

    name: str
    bytes: int
    files: int


@dataclass(frozen=True)
class Usage:
    """One read of the lake tree: what it holds, when it arrived, and what refused.

    ``day_bytes`` maps a day to the bytes attributable to it. A path names its day
    through any component that parses as ``date=YYYY-MM-DD``, which covers the filename
    for ``chains``, ``quotes`` and ``bars``, and the directory for ``journal/date=<D>/``
    and ``reports/close_guard/date=<D>/``. The request timing file,
    ``journal/timing/date=<D>.jsonl``, is read by its own filename, because it is the one
    dated file under ``journal/`` that is not a segment.

    ``undated`` is everything else, which is real disk the growth rate cannot see and so
    is pure undercount. It is carried rather than dropped: it is about 300 KB a day
    against a 566 MB a day rate today, five hundredths of one percent, and nothing says
    it stays that way.

    ``unsealed`` names the days some of whose bytes are still journal segments. The timing
    file never counts toward it, since it outlives the seal on purpose, and counting it
    would show every day since timing began as unsealed forever. A day's
    bytes are not stable: mid-session it is Arrow IPC under ``journal/date=<D>/`` and
    after close+15 it is one compressed Parquet partition, and compaction appends the
    manifest entry before it unlinks the segments, so for a moment a day is both. Both
    readings run high, which shortens the runway rather than lengthening it, so neither
    is worth engineering around. The flag is carried because a growth figure that visibly
    drops at 16:30 needs an explanation where a reader will see it.

    ``refusals`` names what would not read, capped at ``NAMED_REFUSALS`` with
    ``refused`` holding the true count.
    """

    entries: tuple[Entry, ...]
    day_bytes: Mapping[date, int]
    unsealed: frozenset[date]
    dated: int
    undated: int
    files: int
    refusals: tuple[str, ...]
    refused: int

    @property
    def total(self) -> int:
        """Every allocated byte the walk reached, dated and undated together."""
        return self.dated + self.undated


@dataclass(frozen=True)
class Runway:
    """The whole answer: the device, the lake, the rate, and how long that leaves.

    ``capture_days_left`` and ``exhausts_on`` are both ``None`` when the window measured
    no growth, which is a real case rather than an exotic one: a fresh root, a lake read
    before its first capture day, or a window capture was down through. A rate of zero
    renders as no runway at all and never as a large one, because a runway goes unbounded
    exactly when capture has stopped, and a panel painting that as healthy would go quiet
    at the one moment something is wrong.

    ``free`` and ``capacity`` are ``None`` with ``space_error`` naming the class when the
    device would not read, which happens for a root that is not there. That is contained
    here rather than raised, because the walk has already reported a refusal naming the
    same root, and letting the device read throw would discard the one line saying what is
    actually wrong.

    ``exhausts_on`` is also ``None``, with ``beyond_horizon`` true, when the runway
    outruns the schedule the calendar knows or the walk's own bound. That horizon is about
    a year, and a runway inside it is exactly the case the headroom test cares about, so
    nothing the alarm needs is ever unanswerable.
    """

    free: int | None
    capacity: int | None
    space_error: str | None
    usage: Usage
    window_start: date
    window_end: date
    window_days: tuple[tuple[date, int], ...]
    peak_day: date | None
    peak: int
    capture_days: int
    mean: int | None
    capture_days_left: int | None
    exhausts_on: date | None
    beyond_horizon: bool

    @property
    def short(self) -> bool:
        """Whether the headroom is under ``HEADROOM_WEEKS``, which the nightly report flags.

        A runway that outran the horizon is not short, and neither is one no growth was
        measured for. Both leave ``exhausts_on`` unset, and neither is a reason to flag.
        """
        if self.exhausts_on is None:
            return False
        return self.exhausts_on <= self.window_end + timedelta(weeks=HEADROOM_WEEKS)


def _day_of(parts: Sequence[str]) -> date | None:
    """The day a lake-relative path names, or ``None`` when no component names one.

    ``parse_date_dir`` is the one date rule and it is strict, because
    ``date.fromisoformat`` alone accepts ``20260824`` and ``2026-W35-1``. The last
    component has ``.parquet`` stripped first, which is what ``parse_partition_rel``
    does before handing it the same parser, "so a partition file name and a journal date
    directory are read by one rule rather than two".

    ``parse_partition_rel`` itself is not the caller here. It returns ``None`` for
    ``bars``, whose path carries a ``freq=`` level and so has four components rather than
    three, and the walk has to attribute those bytes too.
    """
    for index, part in enumerate(parts):
        if index == len(parts) - 1 and part.endswith(PARQUET_SUFFIX):
            part = part[: -len(PARQUET_SUFFIX)]
        day = parse_date_dir(part)
        if day is not None:
            return day
    return None


def _is_timing_file(parts: Sequence[str]) -> bool:
    """Whether a lake-relative path is a request timing file, ``journal/timing/<file>``."""
    return len(parts) == 3 and parts[0] == JOURNAL_DIR and parts[1] == TIMING_DIR


def _timing_day(parts: Sequence[str]) -> date | None:
    """The day a timing file's name carries, as in ``date=2026-09-24.jsonl``."""
    name = parts[-1]
    if not name.endswith(JSONL_SUFFIX):
        return None
    return parse_date_dir(name[: -len(JSONL_SUFFIX)])


def walk(lake_root: Path | str) -> Usage:
    """Walk the lake once and report its allocated bytes, by entry and by day.

    One walk answers both questions the panel asks. Sizes come from the top-level
    component each file sits under. Growth comes from the ``date=`` key the same path
    already carries, so no ledger is read: the manifest holds no byte count at all, and a
    partition compaction rewrote appears on many of its lines, which would put an old
    day's bytes on the day it was resealed.

    A missing root reports as a refusal rather than as an empty lake, because ``os.walk``
    hands its ``onerror`` the ``FileNotFoundError`` naming the top directory. That
    distinction matters: an absent ``reports/`` is a true zero, and an absent
    ``lake_root`` is a panel pointed at nothing.
    """
    root = Path(lake_root).resolve()
    entry_bytes: dict[str, int] = {}
    entry_files: dict[str, int] = {}
    day_bytes: dict[date, int] = {}
    unsealed: set[date] = set()
    dated = undated = files = refused = 0
    refusals: list[str] = []

    def refuse(where: object, exc: OSError) -> None:
        """Name a refused path relative to the lake root, never absolutely.

        The dashboard publishes these strings in an HTTP response, and the integration
        suite states the invariant they have to keep: "no response carries a path or a
        secret". An absolute path hands a reader who cannot read the filesystem the lake
        root's location on disk. ``_nightly_reports`` and ``_open_quarantines`` already
        report either a lake-relative path or the exception class alone, and this keeps
        that convention. A path that will not sit under the root at all is reported by its
        class alone rather than by a name, because there is nothing safe left to say.
        """
        nonlocal refused
        refused += 1
        if len(refusals) >= NAMED_REFUSALS:
            return
        raw = getattr(exc, "filename", None) or where
        try:
            name = Path(str(raw)).relative_to(root).as_posix()
        except ValueError:
            refusals.append(type(exc).__name__)
            return
        where_name = "the lake root" if name in ("", ".") else name
        refusals.append(f"{where_name}: {type(exc).__name__}")

    for parent, _dirs, names in os.walk(root, onerror=lambda exc: refuse(root, exc)):
        for name in names:
            path = Path(parent) / name
            try:
                stat = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                # Not a refusal. Compaction prunes an emptied ticker, surface and date
                # directory while holding the lake lock, so a file the walk just listed
                # is gone every weekday at close+15. Counting that as a refusal would
                # light the panel up on a lake behaving exactly as designed.
                continue
            except OSError as exc:
                refuse(path, exc)
                continue
            size = stat.st_blocks * BLOCK_BYTES
            files += 1
            try:
                parts = path.relative_to(root).parts
            except ValueError:  # pragma: no cover - os.walk yields only under root
                continue
            entry = parts[0]
            entry_bytes[entry] = entry_bytes.get(entry, 0) + size
            entry_files[entry] = entry_files.get(entry, 0) + 1
            timing = _is_timing_file(parts)
            day = _timing_day(parts) if timing else _day_of(parts)
            if day is None:
                undated += size
            else:
                dated += size
                day_bytes[day] = day_bytes.get(day, 0) + size
                if entry == JOURNAL_DIR and not timing:
                    unsealed.add(day)

    entries = tuple(
        Entry(name=name, bytes=entry_bytes[name], files=entry_files[name])
        for name in sorted(entry_bytes)
    )
    return Usage(
        entries=entries,
        day_bytes=dict(sorted(day_bytes.items())),
        unsealed=frozenset(unsealed),
        dated=dated,
        undated=undated,
        files=files,
        refusals=tuple(refusals),
        refused=refused,
    )


def _exhaustion(
    capture_days_left: int, start: date, calendar: Calendar
) -> tuple[date | None, bool]:
    """The date the disk fills, walking forward one session at a time from ``start``.

    Returns the date and whether the walk ran past its horizon. Sessions are what consume
    the disk, so a count of capture days is not a count of calendar days, and converting
    one to the other by a ratio would be a guess where the calendar has the answer.

    Two things stop the walk and both report the same way. The real calendar refuses a day
    past the schedule it knows, which is about a year out. ``MAX_FORWARD_DAYS`` stops the
    rest, because nothing in the ``Calendar`` protocol promises a horizon and a calendar
    that answers every day would walk to ``date.max``. Either way the capture-day count
    stands and only the date is withheld.
    """
    if capture_days_left <= 0:
        # A disk with less than one day of growth left. ``free // peak`` truncates every
        # such reading to zero, so the whole band from a full disk up to one day's
        # headroom lands here. It exhausts today, and saying so is the entire point of the
        # module: a walk that stepped forward looking for a zero it had already passed
        # returned no date at all, which reads as a runway too long to date. That is the
        # alarm inverted at the one moment it exists for.
        return start, False
    day = start
    left = capture_days_left
    for _step in range(MAX_FORWARD_DAYS):
        try:
            day += timedelta(days=1)
            session = calendar.is_session(day)
        except _CALENDAR_RANGE_ERRORS:
            return None, True
        if session:
            left -= 1
            if left <= 0:
                return day, False
    return None, True


def assess(
    lake_root: Path | str,
    *,
    today: date,
    calendar: Calendar,
    window_days: int = GROWTH_WINDOW_DAYS,
) -> Runway:
    """Read the lake and the device, and answer how long capture can keep going.

    ``today`` is the caller's own session date rather than a wall-clock read, because
    nothing in this module reads a clock, the same rule the dashboard keeps.

    The rate is the busiest day in the window. The mean rides beside it, denominated by
    the days that actually wrote bytes rather than by the window's width, because a day
    that captured nothing is not a day the lake grew slowly on.

    Nothing here raises for a lake or a device it cannot read. Both readings report as
    values, for one reason: the caller is a panel where an escape becomes a 500 saying
    only that the query failed, which throws away the refusal naming what is wrong.
    """
    usage = walk(lake_root)
    free: int | None = None
    capacity: int | None = None
    space_error: str | None = None
    try:
        space = shutil.disk_usage(Path(lake_root).resolve())
    except OSError as exc:
        space_error = type(exc).__name__
    else:
        free, capacity = space.free, space.total

    start = today - timedelta(days=window_days - 1)
    window = tuple((day, size) for day, size in usage.day_bytes.items() if start <= day <= today)
    capturing = [(day, size) for day, size in window if size > 0]
    peak_day, peak = max(capturing, key=lambda item: item[1]) if capturing else (None, 0)
    mean = sum(size for _day, size in capturing) // len(capturing) if capturing else None

    capture_days_left: int | None = None
    exhausts_on: date | None = None
    beyond = False
    if peak > 0 and free is not None:
        capture_days_left = free // peak
        exhausts_on, beyond = _exhaustion(capture_days_left, today, calendar)

    return Runway(
        free=free,
        capacity=capacity,
        space_error=space_error,
        usage=usage,
        window_start=start,
        window_end=today,
        window_days=window,
        peak_day=peak_day,
        peak=peak,
        capture_days=len(capturing),
        mean=mean,
        capture_days_left=capture_days_left,
        exhausts_on=exhausts_on,
        beyond_horizon=beyond,
    )
