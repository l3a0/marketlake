"""The disk runway: what the lake holds, how fast it grows, and how long the disk lasts.

Four consumers read this module and it is deliberately a leaf, importing only the standard
library, :mod:`lake.calendar` and :mod:`lake.paths`.

1. The dashboard's Lake panel renders what it returns.
2. The evening sweep, :mod:`lake.sweep`, files an ``action`` line in the nightly report when
   the headroom runs under ``HEADROOM_WEEKS`` and pages when it runs under
   ``PAGE_FLOOR_WEEKS``, which is marketlake #438.
3. The range restore in :mod:`lake.bucket` refuses a restore that would leave less than the
   journal reserve free, through :func:`reserve_shortfall` (marketlake #784).
4. The ``restore`` command in :mod:`lake.bucket` refuses the same way on the directory it
   fills, with the busiest sealed day read off the bucket's listing by
   :func:`listing_busiest_sealed_day` (marketlake #785).

One computation with several consumers is the point: independent ones would drift, and a
panel and an alarm disagreeing about how long the disk lasts is worse than either being
wrong alone.

That is also why it is a module of its own rather than a part of :mod:`lake.dashboard`.
The sweep has no reason to import the dashboard's DuckDB query layer to read a number off
the disk, and a computation living in one consumer invites the others to grow a copy.

Five decisions are worth reading before the code.

1. **The bytes are allocated blocks, never file sizes.** What fills a disk is blocks. A
   4 KiB block holds a 165-byte nightly report as surely as a 4 KiB one, and the lake's
   ``reports/`` tree is 6,275 bytes of content in 155,648 bytes of blocks across 38
   files. It is also the tree with no pruning step, since a held finding files again
   every night it survives, so the divergence grows. ``st_blocks`` is in 512-byte units
   by POSIX convention whatever the filesystem's own block size is.
2. **The growth rate is the busiest day's permanent growth in the window, not the mean.**
   Measured over the live lake, the same bytes give a runway from 2,059 days to 20,647
   depending only on what the rate is divided by, because capture began partway through
   the window and the idle days before it drag any mean down. Every such error lengthens
   the runway, and a check that flags short headroom never fires if its rate is too low.
   The mean over the days that grew is reported beside the peak so a reader sees the
   spread. Permanent growth is what a day leaves on the disk.

   So a past day counts all of its dated bytes, journal segments included. A segment
   still under a past day is one compaction refused, and it stays on the disk until
   someone repairs it, so it is as permanent as the Parquet beside it. Compaction refuses
   one ticker-day at a time and seals the rest, so a day can carry both. Leaving the
   segments out read a lake whose compaction refused every ticker every day as 157
   capture days, on a disk about one session from full.

   Today counts only its sealed bytes, its dated bytes less its journal segments, which
   ``Usage.journal_bytes`` carries. A reader cannot tell from the disk whether today's
   journal is still growing, part way through compaction, or refused. Compaction seals
   today's ticker-days one at a time under the lake lock, which neither reader of this
   module takes, and nothing on the disk records that compaction finished. Counted in
   full, a half-compacted journal would read as growth. So today's journal first counts
   once today is a past day. Its bytes still count against free space, because they are
   on the disk. The timing file every session writes and the dated nightly reports are
   sealed bytes, because both outlive the seal.

   Three prices are named.

   1. One anomalous sealed day, a backfill or a reseal, shortens the runway and can flag
      or page early.
   2. A past day with an unrepaired stuck journal sets the rate for up to the whole
      window, so the runway can page every night beside compaction's own damaged-segment
      page until the day is repaired. Both pages point at a real action: one says the
      segments need repair, the other that the disk is filling faster than it should.
   3. A ticker-day compaction refuses reaches the rate from the first sweep after today
      becomes a past day. The sweep runs Monday to Friday, so a Friday refusal reaches it
      on Monday's 18:30 sweep, after Monday's session has written another journal.
      Compaction pages that refusal itself the night it happens, with the damaged-segment
      or schema-drift page. The panel counts it from midnight, when today becomes a past
      day, so the panel's runway can drop at the midnight after a refusal. On a lake's
      first session, when no past day in the window grew, the night compaction refuses
      reads a long runway until today becomes a past day. Both the rate and the reserve
      fall back to today's sealed bytes, which are the timing file and that night's
      reports. Compaction's own page covers that night.

   A false page costs an operator a look at the Lake panel, while a missed one costs the
   disk, and a minute lost to a full disk is gone forever.
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
5. **A session's journal is reserved off free space before the runway is counted.** The
   rate leaves today's journal out, but every session first lands under ``lake_root`` as
   an uncompressed journal that compaction frees only at close+15, and nothing records its
   size. The measured peaks ran 9 to 13 times a sealed day: 7.5 GB on 2026-10-06 against a
   599.3 MB sealed upload that night. So ``JOURNAL_RESERVE_SESSIONS`` times the busiest
   day's sealed bytes in the window, today's included, comes off ``free`` first, and a
   disk with less free space than that reads zero capture days. The basis is sealed bytes
   rather than the rate's peak, because a journal scales with the session it compacts
   into. A past day's stuck segments already raise the rate, and as the basis they would
   inflate the reserve thirteen-fold on top. A multiple follows the roster as it grows,
   where a byte count would go stale. The whole reserve comes off every time, with no
   credit for a journal already on the disk, so mid-session the panel reads up to 13
   sessions short until compaction frees the journal. That is the price. A credit would
   lengthen the runway on exactly the night a compaction failed and left its journal
   stuck, when tomorrow still needs the full reserve.

**Nothing here raises for a lake it cannot read.** The dashboard turns any escape into a
500 for the whole panel, which would throw away the refusal lines this module exists to
report. So a directory that will not list and a file that will not stat are values in
:class:`Usage`, not exceptions. The walk is ``os.walk`` with an ``onerror`` handler and
never ``Path.rglob``, which drops an unreadable directory and reports nothing at all. A
file or a directory that vanishes between the listing and the read is a different thing
and is skipped silently: compaction prunes an emptied directory while holding the lake
lock, so a path the walk just saw is gone every weekday at close+15 on a lake behaving
exactly as designed. The root itself vanishing is still a refusal. ``lost+found`` at the
root is skipped before it is listed. On the hosted VM ``lake_root`` is the volume's mount
point, where ``mkfs.ext4`` leaves that directory owned by root at mode 0700, so every walk
would otherwise report a refusal that names nothing about the lake.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from lake.calendar import Calendar
from lake.paths import (
    JOURNAL_DIR,
    JSONL_SUFFIX,
    LOST_AND_FOUND,
    PARQUET_SUFFIX,
    TIMING_DIR,
    parse_date_dir,
)

# How many trailing calendar days the growth rate is measured over. It matches the
# History panel's window so the page's two spans read alike to a human, and the two
# constants are independent in code because this module is a leaf and does not import the
# dashboard.
GROWTH_WINDOW_DAYS = 30

# The headroom the design calls "a few weeks". Below this the runway is short, which the
# nightly report flags. It lives here rather than in ``GuardConstants`` because that
# class is scoped to the distributions slice 1 measures and recalibrates, and headroom in
# weeks is a design constant no measurement moves. The free bytes it resolves to differ
# per machine. The weeks do not.
HEADROOM_WEEKS = 3

# The headroom under which a short runway also pages, rather than only filing a line in the
# nightly report. ``HEADROOM_WEEKS`` was set for a laptop with years of headroom, where a
# line was enough. The hosted VM keeps the lake on a dedicated volume measured in weeks, so
# under this floor the report line is too slow a way to learn that capture is at risk. The
# test has the same shape as ``short``'s: a date comparison, never a converted count.
PAGE_FLOOR_WEEKS = 2

# How many sessions of the busiest day's sealed bytes come off free space to hold a
# session's journal before the runway is counted. Module docstring decision 5 gives the
# measurement: the journal's peak ran 9 to 13 times the sealed day it compacts into, and
# this takes the top of that range. The basis is sealed bytes, never the rate's peak,
# because a past day's stuck journal raises the rate and must not raise this thirteen-fold.
JOURNAL_RESERVE_SESSIONS = 13

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

    ``unsealed`` names the days some of whose bytes are still journal segments, and
    ``journal_bytes`` carries how many of each such day's bytes those segments are. The
    timing file never counts toward either, since it outlives the seal on purpose, and
    counting it would show every day since timing began as unsealed forever. A day's
    bytes are not stable: mid-session it is Arrow IPC under ``journal/date=<D>/`` and
    after close+15 it is one compressed Parquet partition, and compaction appends the
    manifest entry before it unlinks the segments, so for a moment a day is both. Both
    readings run high, which shortens the runway rather than lengthening it, so neither
    is worth engineering around. The flag is carried for two reasons. A growth figure that
    visibly drops at 16:30 needs an explanation where a reader will see it, and today's
    rate is taken off its sealed bytes, its ``day_bytes`` less its ``journal_bytes``, for
    the reason the module docstring's decision 2 gives. A past day's rate is its whole
    ``day_bytes``, and the journal reserve reads every day's sealed bytes.

    ``refusals`` names what would not read, capped at ``NAMED_REFUSALS`` with
    ``refused`` holding the true count.
    """

    entries: tuple[Entry, ...]
    day_bytes: Mapping[date, int]
    unsealed: frozenset[date]
    journal_bytes: Mapping[date, int]
    dated: int
    undated: int
    files: int
    refusals: tuple[str, ...]
    refused: int

    @property
    def total(self) -> int:
        """Every allocated byte the walk reached, dated and undated together."""
        return self.dated + self.undated

    def sealed_bytes(self, day: date) -> int:
        """A day's bytes less its journal segments.

        The growth rate reads this for today alone, and the journal reserve reads it for
        every day in the window. Module docstring decisions 2 and 5 give the reasons.
        """
        return self.day_bytes.get(day, 0) - self.journal_bytes.get(day, 0)


@dataclass(frozen=True)
class Runway:
    """The whole answer: the device, the lake, the rate, and how long that leaves.

    ``capture_days_left`` and ``exhausts_on`` are both ``None`` when no day in the window
    grew the lake at all, which is a real case rather than an exotic one. It covers a
    fresh root, a lake read before its first capture day, and a window capture was down
    through. A rate of zero renders as no runway at all and never as a large one. A lake
    with no capture day yet is healthy. A stopped capture is not, and a panel painting it
    as a long runway would go quiet at the one moment something is wrong.

    Two other cases read a rate rather than none. A compaction refusing every ticker leaves
    its past days' journals on the disk, and those set the rate, so the runway reads short
    as the disk fills. A lake's first session, before close+15 seals it, has only today's
    sealed bytes to go on, the timing file and nothing else, so the panel reads a long
    runway for that one session. The 18:30 sweep reads after the seal.

    ``free`` and ``capacity`` are ``None`` with ``space_error`` naming the class when the
    device would not read, which happens for a root that is not there. That is contained
    here rather than raised, because the walk has already reported a refusal naming the
    same root, and letting the device read throw would discard the one line saying what is
    actually wrong.

    ``exhausts_on`` is also ``None``, with ``beyond_horizon`` true, when the runway
    outruns the schedule the calendar knows or the walk's own bound. That horizon is about
    a year, and a runway inside it is exactly the case the headroom test cares about, so
    nothing the alarm needs is ever unanswerable.

    ``reserve`` is the journal reserve, which came off ``free`` before
    ``capture_days_left`` was counted. It is ``JOURNAL_RESERVE_SESSIONS`` times the
    busiest day's sealed bytes in the window, today's included. So it is never more than
    thirteen times ``peak``, and exactly that whenever no day the rate counts in full
    carries journal segments. It is carried so a reader can
    see why free space and the capture-day count disagree. ``peak``, ``peak_day``, ``mean``
    and ``capture_days`` are read off the rate series of decision 2: each past day's dated
    bytes in full, and today's sealed bytes. ``window_days`` lists each day's dated bytes,
    journal included.
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
    reserve: int
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

    @property
    def critical(self) -> bool:
        """Whether the headroom is under ``PAGE_FLOOR_WEEKS``, which the evening sweep pages.

        The same shape as :attr:`short`, one threshold lower, so a critical runway is
        always a short one. With no date both read False, which is why the sweep files its
        own line for a reading that failed.
        """
        if self.exhausts_on is None:
            return False
        return self.exhausts_on <= self.window_end + timedelta(weeks=PAGE_FLOOR_WEEKS)


def path_day(parts: Sequence[str]) -> tuple[date | None, bool]:
    """The day a lake-relative path's ``parts`` name, and whether the path is a journal segment.

    The day is ``None`` when no component names one, and such a path is never a segment. This is
    the one path rule. :func:`walk` sorts every file on disk by it, and :func:`listing_usage`
    sorts every key in a bucket listing by it, so the two readings cannot drift apart.

    1. **A request timing file**, ``journal/timing/date=<D>.jsonl``, is dated by its own name and
       is not a segment. It is the one dated file under ``journal/`` that outlives the seal.
    2. **Any other path** is dated by its first component that parses as ``date=YYYY-MM-DD``.
       ``parse_date_dir`` is the one date rule and it is strict, because ``date.fromisoformat``
       alone accepts ``20260824`` and ``2026-W35-1``. The last component has ``.parquet``
       stripped first, which is what ``parse_partition_rel`` does before handing it the same
       parser, "so a partition file name and a journal date directory are read by one rule
       rather than two". A dated path under ``journal/`` is a segment.

    ``parse_partition_rel`` itself is not the rule here. It returns ``None`` for ``bars``, whose
    path carries a ``freq=`` level and so has four components rather than three, and the walk
    has to attribute those bytes too.
    """
    if len(parts) == 3 and parts[0] == JOURNAL_DIR and parts[1] == TIMING_DIR:
        name = parts[-1]
        if not name.endswith(JSONL_SUFFIX):
            return None, False
        return parse_date_dir(name[: -len(JSONL_SUFFIX)]), False
    for index, part in enumerate(parts):
        if index == len(parts) - 1 and part.endswith(PARQUET_SUFFIX):
            part = part[: -len(PARQUET_SUFFIX)]
        day = parse_date_dir(part)
        if day is not None:
            return day, parts[0] == JOURNAL_DIR
    return None, False


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
    ``lake_root`` is a panel pointed at nothing. A directory below the root that is gone by
    the time it is listed is the other thing: compaction pruned it, and it is skipped.

    ``lost+found`` at the root is dropped before it is listed, for the reason the module
    docstring gives. One anywhere else is not the filesystem's, so it is walked and an
    unreadable one is still refused.
    """
    root = Path(lake_root).resolve()
    entry_bytes: dict[str, int] = {}
    entry_files: dict[str, int] = {}
    day_bytes: dict[date, int] = {}
    unsealed: set[date] = set()
    journal_bytes: dict[date, int] = {}
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

    def listing_failed(exc: OSError) -> None:
        """Refuse a directory that would not list, unless compaction pruned it first.

        ``_prune_empty`` in compaction removes an emptied ``journal/date=<D>/`` while the
        walk may already have listed its parent, and ``os.walk`` hands that loss here as a
        ``FileNotFoundError``. It is the directory form of the vanished file skipped below.
        The root is the exception, and so is a failure naming no path at all, because
        neither can be told apart from a lake that is not there.
        """
        filename = getattr(exc, "filename", None)
        if isinstance(exc, FileNotFoundError) and filename is not None and Path(filename) != root:
            return
        refuse(root, exc)

    top = os.fspath(root)
    for parent, dirs, names in os.walk(root, onerror=listing_failed):
        if parent == top and LOST_AND_FOUND in dirs:
            # Pruned in place, which is how ``os.walk`` is told not to descend.
            dirs.remove(LOST_AND_FOUND)
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
            day, segment = path_day(parts)
            if day is None:
                undated += size
            else:
                dated += size
                day_bytes[day] = day_bytes.get(day, 0) + size
                if segment:
                    unsealed.add(day)
                    journal_bytes[day] = journal_bytes.get(day, 0) + size

    entries = tuple(
        Entry(name=name, bytes=entry_bytes[name], files=entry_files[name])
        for name in sorted(entry_bytes)
    )
    return Usage(
        entries=entries,
        day_bytes=dict(sorted(day_bytes.items())),
        unsealed=frozenset(unsealed),
        journal_bytes=dict(sorted(journal_bytes.items())),
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


def busiest_sealed_day(usage: Usage, *, today: date, window_days: int = GROWTH_WINDOW_DAYS) -> int:
    """The largest one day's sealed bytes in the window ending ``today``, today's included.

    This is the journal reserve's basis, per module docstring decision 5. A journal scales
    with the session it compacts into, and a stuck segment counted here would be multiplied
    ``JOURNAL_RESERVE_SESSIONS`` times over, so each day counts its sealed bytes only. A
    window with no dated bytes answers 0.

    :func:`assess` reads it for the Lake panel and the evening sweep. Both restores in
    :mod:`lake.bucket` read it for :func:`reserve_shortfall`, the ``restore`` command through
    :func:`listing_busiest_sealed_day`. One computation keeps the panel and the refusals from
    disagreeing about the reserve.
    """
    start = today - timedelta(days=window_days - 1)
    return max(
        (usage.sealed_bytes(day) for day in usage.day_bytes if start <= day <= today),
        default=0,
    )


def listing_usage(listing: Mapping[str, int]) -> Usage:
    """What a bucket listing holds, as the :class:`Usage` :func:`walk` gives for a lake on disk.

    ``listing`` maps each lake-relative key to its size, the shape ``lake.bucket.list_bucket``
    returns. Each key is split on ``/`` and sorted by :func:`path_day`, the rule the walk uses.
    Two kinds of key are skipped, as the walk and the restore skip them. A zero-byte key ending
    in ``/`` is the folder marker the S3 console writes, which names no file. A key under a root
    ``lost+found`` names the filesystem's directory rather than the lake.

    Every field is filled. ``refused`` is 0 and ``refusals`` is empty, because a listing arrives
    whole or ``list_bucket`` raises, so nothing in it is a path that would not read. Sizes are the
    objects' logical bytes, while the walk counts allocated blocks. The two differ by under 0.1%
    of a day. It never raises, as nothing in this module does.
    """
    entry_bytes: dict[str, int] = {}
    entry_files: dict[str, int] = {}
    day_bytes: dict[date, int] = {}
    unsealed: set[date] = set()
    journal_bytes: dict[date, int] = {}
    dated = undated = files = 0
    for key, size in listing.items():
        if key.endswith("/") and size == 0:
            continue
        parts = key.split("/")
        if parts[0] == LOST_AND_FOUND:
            continue
        files += 1
        entry = parts[0]
        entry_bytes[entry] = entry_bytes.get(entry, 0) + size
        entry_files[entry] = entry_files.get(entry, 0) + 1
        day, segment = path_day(parts)
        if day is None:
            undated += size
            continue
        dated += size
        day_bytes[day] = day_bytes.get(day, 0) + size
        if segment:
            unsealed.add(day)
            journal_bytes[day] = journal_bytes.get(day, 0) + size
    return Usage(
        entries=tuple(
            Entry(name=name, bytes=entry_bytes[name], files=entry_files[name])
            for name in sorted(entry_bytes)
        ),
        day_bytes=dict(sorted(day_bytes.items())),
        unsealed=frozenset(unsealed),
        journal_bytes=dict(sorted(journal_bytes.items())),
        dated=dated,
        undated=undated,
        files=files,
        refusals=(),
        refused=0,
    )


def listing_busiest_sealed_day(
    listing: Mapping[str, int], *, window_days: int = GROWTH_WINDOW_DAYS
) -> int:
    """The busiest sealed day in a bucket listing's window, the journal reserve's basis there.

    The restore in ``lake.bucket`` fills an empty directory, which holds no sealed day to read,
    so it reads the lake's busiest sealed day off the bucket's listing instead. This is
    :func:`busiest_sealed_day` over :func:`listing_usage`, with the window anchored on the
    listing itself rather than on a clock, so the restore needs none.

    **The window ends at the newest day whose sealed bytes are above 0.** Three other anchors
    each fail on a lake a restore meets.

    1. Today would leave the window empty for a host dead 30 days, the reserve at 0, and the
       check passing on nothing, on the recovery path.
    2. The newest day outside ``Usage.unsealed`` would let one leftover segment pull the end
       back. The bucket keeps every segment it ever uploaded, and a day that sealed most tickers
       while compaction refused one is both unsealed and possibly the busiest.
    3. The oldest day would measure the lake's first month rather than its latest.

    The price is that a key dated in the future would move the window, and nothing in the lake
    writes one. A listing with no sealed bytes on any day answers 0.
    """
    usage = listing_usage(listing)
    newest = max((day for day in usage.day_bytes if usage.sealed_bytes(day) > 0), default=None)
    if newest is None:
        return 0
    return busiest_sealed_day(usage, today=newest, window_days=window_days)


def reserve_shortfall(*, free: int, planned: int, busiest_sealed_day: int) -> int:
    """How many bytes a write of ``planned`` bytes would leave the journal reserve short.

    The reserve is ``JOURNAL_RESERVE_SESSIONS`` times ``busiest_sealed_day``, and it has to be
    free after the write, because the next session's journal lands on the same volume and
    compaction frees it only at close+15. The answer is 0 when ``free - planned`` covers the
    reserve exactly or with room to spare, and otherwise the bytes missing. It never raises, as
    nothing in this module does, so the caller words the refusal.

    The busiest sealed day is an argument rather than read from the disk here, because the two
    callers find it differently. The range restore in :mod:`lake.bucket` adds partitions to a
    live lake and reads it with :func:`busiest_sealed_day`. The ``restore`` command fills an
    empty directory, which holds no sealed day to read, so it reads it off the bucket's listing
    with :func:`listing_busiest_sealed_day`.
    """
    reserve = JOURNAL_RESERVE_SESSIONS * busiest_sealed_day
    return max(0, reserve - (free - planned))


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

    The rate is the busiest day's permanent growth in the window: a past day's dated bytes
    in full, and today's sealed bytes. The mean rides beside it, denominated by the days
    that grew rather than by the window's width, because a day that captured nothing is
    not a day the lake grew slowly on. The journal reserve, thirteen of the busiest day's
    sealed bytes, comes off free space before the count, and a disk with less free space
    than the reserve reads zero capture days rather than a negative count.

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
    # The rate series, per module docstring decision 2. A past day counts in full, journal
    # included, because a segment compaction left behind stays on the disk until someone
    # repairs it. Today counts without its journal, because nothing on the disk says
    # whether that journal is still growing, part way through compaction, or refused.
    rate = ((day, usage.sealed_bytes(day) if day == today else size) for day, size in window)
    capturing = [(day, size) for day, size in rate if size > 0]
    peak_day, peak = max(capturing, key=lambda item: item[1]) if capturing else (None, 0)
    mean = sum(size for _day, size in capturing) // len(capturing) if capturing else None
    reserve = JOURNAL_RESERVE_SESSIONS * busiest_sealed_day(
        usage, today=today, window_days=window_days
    )

    capture_days_left: int | None = None
    exhausts_on: date | None = None
    beyond = False
    if peak > 0 and free is not None:
        # Clamped, because free space under the reserve would otherwise read as a negative
        # count, and the panel prints it.
        capture_days_left = max(0, (free - reserve) // peak)
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
        reserve=reserve,
        capture_days=len(capturing),
        mean=mean,
        capture_days_left=capture_days_left,
        exhausts_on=exhausts_on,
        beyond_horizon=beyond,
    )
