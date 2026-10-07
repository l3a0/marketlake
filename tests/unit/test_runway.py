"""The disk runway: what the walk counts, what sets the rate, and what it refuses.

Every test here builds a real tree under ``tmp_path`` and reads it. Free space is the one
thing stubbed, because the arithmetic has to be deterministic and the machine's own free
space is not. ``shutil.disk_usage`` itself is exercised unstubbed in the one test that
checks the module reads the real device at all.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import pytest

from lake import runway
from lake.runway import HEADROOM_WEEKS, Usage, assess, walk
from tests.support.calendar import FakeCalendar, SessionTimes

# A Monday, and the week around it. Sessions are weekdays only, which is what makes the
# session walk differ from a calendar walk at all.
MONDAY = date(2026, 9, 14)


def _weekday_calendar(start: date, days: int) -> FakeCalendar:
    """Every weekday in a run of ``days`` from ``start`` is a session. Weekends are not."""
    sessions = {}
    for offset in range(days):
        day = start + timedelta(days=offset)
        if day.weekday() < 5:
            sessions[day] = SessionTimes(
                open=runway.__dict__.get("_unused", None) or _noon(day),
                close=_noon(day),
            )
    return FakeCalendar(sessions)


def _noon(day: date):
    from datetime import datetime

    from lake.calendar import MARKET_TZ

    return datetime(day.year, day.month, day.day, 12, tzinfo=MARKET_TZ)


def _write(root: Path, rel: str, size: int) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


class _Space:
    """What ``assess`` reads off ``shutil.disk_usage``: a free figure and a total."""

    def __init__(self, free: int, total: int) -> None:
        self.free = free
        self.total = total


def _stub_space(monkeypatch: pytest.MonkeyPatch, free: int, total: int = 1 << 50) -> None:
    monkeypatch.setattr(runway.shutil, "disk_usage", lambda _path: _Space(free, total))


# -- what the walk counts ----------------------------------------------------


def test_the_walk_counts_allocated_blocks_and_not_file_sizes(tmp_path: Path):
    # A one-byte file occupies a whole block. The runway asks how long before the disk
    # fills, and what fills a disk is blocks. The lake's own ``reports/`` tree is the case
    # that made this matter: 38 tiny JSON files, 6,275 bytes of content, 155,648 of blocks.
    for index in range(4):
        _write(tmp_path, f"reports/r{index}.json", 1)
    usage = walk(tmp_path)
    assert usage.files == 4
    apparent = sum(path.stat().st_size for path in tmp_path.rglob("*") if path.is_file())
    assert apparent == 4
    assert usage.total > apparent
    assert usage.total == sum(
        path.stat().st_blocks * runway.BLOCK_BYTES for path in tmp_path.rglob("*") if path.is_file()
    )


def test_every_top_level_entry_is_counted_including_the_flat_ledgers(tmp_path: Path):
    # Four surfaces in three layouts, plus the root ledgers. A walk keyed on ``date=``
    # would report ``actions`` as zero however large the ledger grows, and a walk over
    # ``paths.SURFACES`` alone would hide ``manifest.jsonl``, which on the live lake is
    # four times the whole ``quotes`` surface.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 4000)
    _write(tmp_path, "quotes/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "bars/ticker=SPY/freq=1d/date=2026-09-14.parquet", 10)
    _write(tmp_path, "actions/corporate_actions.jsonl", 9000)
    _write(tmp_path, "manifest.jsonl", 20000)
    usage = walk(tmp_path)
    names = {entry.name for entry in usage.entries}
    assert names == {"chains", "quotes", "bars", "actions", "manifest.jsonl"}
    by_name = {entry.name: entry for entry in usage.entries}
    assert by_name["actions"].bytes > 0
    assert by_name["manifest.jsonl"].bytes > 0


def test_a_day_is_named_by_any_date_component_in_any_of_the_four_layouts(tmp_path: Path):
    # ``chains`` and ``quotes`` carry the day in the filename, ``bars`` carries it in the
    # filename under an extra ``freq=`` level, and ``journal`` and ``reports`` carry it in
    # a directory. ``parse_partition_rel`` reads only the first of those, which is why the
    # rule here is a component scan rather than that parser.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "bars/ticker=SPY/freq=1d/date=2026-09-15.parquet", 10)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 10)
    _write(tmp_path, "reports/close_guard/date=2026-09-17/run.json", 10)
    usage = walk(tmp_path)
    assert set(usage.day_bytes) == {
        date(2026, 9, 14),
        date(2026, 9, 15),
        date(2026, 9, 16),
        date(2026, 9, 17),
    }


def test_an_undated_file_is_carried_rather_than_dropped(tmp_path: Path):
    # ``manifest.jsonl`` and ``reference/*.parquet`` name no day, so the growth rate
    # cannot see them. That is pure undercount, which runs in the unsafe direction, so the
    # bytes are reported separately rather than silently left out of the total.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "manifest.jsonl", 10)
    _write(tmp_path, "reference/security_master.parquet", 10)
    usage = walk(tmp_path)
    assert usage.dated > 0
    assert usage.undated > 0
    assert usage.total == usage.dated + usage.undated


def test_a_journal_day_is_flagged_unsealed_and_a_sealed_one_is_not(tmp_path: Path):
    # A day's bytes are not stable. Mid-session it is Arrow IPC segments; after close+15
    # it is one compressed partition. The flag is what lets the page explain a growth
    # figure that drops at 16:30.
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 10)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    usage = walk(tmp_path)
    assert usage.unsealed == frozenset({date(2026, 9, 16)})


def test_a_timing_file_is_dated_by_its_name_and_never_flags_its_day_unsealed(tmp_path: Path):
    # ``journal/timing/date=D.jsonl`` outlives the day's seal on purpose. Read by the rule
    # for everything else, its name parses as no day and its bytes fall out of the growth
    # rate, and read as a journal day it would flag every day since timing began as
    # unsealed forever. So it counts toward its day's bytes and never toward ``unsealed``.
    _write(tmp_path, "journal/timing/date=2026-09-15.jsonl", 5000)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-15.parquet", 10)
    usage = walk(tmp_path)
    assert date(2026, 9, 15) in usage.day_bytes
    assert usage.day_bytes[date(2026, 9, 15)] > usage.day_bytes.get(date(2026, 9, 14), 0)
    assert usage.undated == 0
    assert usage.unsealed == frozenset()


def test_a_timing_file_beside_a_live_segment_leaves_that_day_unsealed(tmp_path: Path):
    # The timing file must not hide a day that really is still journal segments.
    _write(tmp_path, "journal/timing/date=2026-09-16.jsonl", 10)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 10)
    usage = walk(tmp_path)
    assert usage.unsealed == frozenset({date(2026, 9, 16)})


# -- what the walk refuses ---------------------------------------------------


def test_an_unreadable_directory_is_a_named_refusal_and_not_a_zero(tmp_path: Path):
    # ``Path.rglob`` drops an unreadable directory and reports nothing, which renders an
    # unreadable surface as zero bytes on the one panel a reader opens to ask whether the
    # disk is filling. ``os.walk`` with an ``onerror`` handler reports it.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    locked = tmp_path / "quotes"
    _write(tmp_path, "quotes/ticker=SPY/date=2026-09-14.parquet", 10)
    locked.chmod(0o000)
    try:
        usage = walk(tmp_path)
    finally:
        locked.chmod(0o755)
    assert usage.refused == 1
    assert usage.refusals and "quotes" in usage.refusals[0]
    # And the rest of the lake still reports, rather than the whole read failing.
    assert any(entry.name == "chains" for entry in usage.entries)


def test_a_missing_root_is_a_refusal_rather_than_an_empty_lake(tmp_path: Path):
    # An absent ``reports/`` is a true zero: no nightly run filed anything. An absent
    # ``lake_root`` is a panel pointed at nothing, and reporting it as an empty lake would
    # say the disk is fine when nothing was read at all.
    usage = walk(tmp_path / "not-a-lake")
    assert usage.refused == 1
    assert usage.total == 0
    assert usage.files == 0


def test_the_refusal_list_is_capped_and_the_count_is_not(tmp_path: Path):
    # A disk going bad names every file it carries, and a line per file would bury every
    # other thing the panel has to say. ``manifest._NAMED_PATHS`` keeps the same shape.
    locked = []
    for index in range(runway.NAMED_REFUSALS + 2):
        directory = tmp_path / f"surface{index}"
        _write(tmp_path, f"surface{index}/ticker=SPY/date=2026-09-14.parquet", 10)
        directory.chmod(0o000)
        locked.append(directory)
    try:
        usage = walk(tmp_path)
    finally:
        for directory in locked:
            directory.chmod(0o755)
    assert usage.refused == runway.NAMED_REFUSALS + 2
    assert len(usage.refusals) == runway.NAMED_REFUSALS


# -- what sets the rate ------------------------------------------------------


def test_the_rate_is_the_busiest_day_and_not_a_mean_over_the_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The finding this module exists around. Capture began partway through the window, so
    # a mean over the window's width reads far below the real daily rate. Every such error
    # lengthens the runway, and a check that flags short headroom never fires if its rate
    # is too low.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-02.parquet", 1)
    for day, size in ((14, 400_000), (15, 500_000), (16, 450_000)):
        _write(tmp_path, f"chains/ticker=SPY/date=2026-09-{day}.parquet", size)
    _stub_space(monkeypatch, free=10_000_000)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.peak_day == date(2026, 9, 15)
    # Four days wrote bytes, not thirty. The mean is denominated by those, and it is still
    # below the peak, which is what the panel shows the pair for.
    assert result.capture_days == 4
    assert result.mean is not None and result.mean < result.peak
    # Thirteen sessions of the peak are the journal reserve, which comes off free first.
    assert result.capture_days_left == (10_000_000 - 13 * result.peak) // result.peak


def test_a_day_that_wrote_nothing_is_not_a_day_the_lake_grew_slowly_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The mean's denominator is days with bytes. A window's width counts holidays, a
    # weekend, and every day before capture started, none of which are slow growth.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 400_000)
    _stub_space(monkeypatch, free=10_000_000)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days == 1
    assert result.mean == result.peak


def test_a_day_outside_the_window_sets_no_rate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # The window is trailing. A huge day from last year must not be this month's peak.
    _write(tmp_path, "chains/ticker=SPY/date=2025-01-02.parquet", 9_000_000)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 400_000)
    _stub_space(monkeypatch, free=10_000_000)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.peak_day == date(2026, 9, 16)


def test_no_growth_reports_no_runway_rather_than_an_unbounded_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Reachable when no day in the window grew the lake: a fresh root, a lake read before
    # its first capture day, and a window capture was down through. The last is a failure,
    # so reporting a large number there would go quiet at the one moment something is
    # wrong. It is also the division by zero.
    _write(tmp_path, "manifest.jsonl", 10)
    _stub_space(monkeypatch, free=10_000_000)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left is None
    assert result.exhausts_on is None
    assert result.mean is None
    assert result.short is False
    assert result.critical is False


# -- the runway's unit -------------------------------------------------------


def test_the_runway_is_counted_in_sessions_and_not_in_calendar_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Growth happens on sessions. Five capture days from a Monday lands on the following
    # Monday, seven calendar days later, because the weekend consumes nothing. Converting
    # a capture-day count by a ratio would be a guess where the calendar has the answer.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 100)
    _stub_space(monkeypatch, free=0)
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left == 0
    # Five sessions' worth of free space past the 13-session journal reserve, priced at the
    # one day that wrote bytes.
    _stub_space(monkeypatch, free=result.peak * (5 + 13))
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left == 5
    assert result.exhausts_on == MONDAY + timedelta(days=7)
    assert result.exhausts_on != MONDAY + timedelta(days=5)


def test_a_runway_past_the_horizon_reports_no_date_and_is_not_short(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The real calendar knows about a year ahead and refuses anything past it. A runway
    # longer than that has no date, which costs the headroom test nothing: a runway longer
    # than a year is not a few weeks.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 100)
    _stub_space(monkeypatch, free=1 << 45)
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 30))
    assert result.capture_days_left > 0
    assert result.exhausts_on is None
    assert result.beyond_horizon is True
    assert result.short is False


def test_the_forward_walk_is_bounded_even_by_a_calendar_that_never_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Nothing in the ``Calendar`` protocol promises a horizon. A calendar answering every
    # day walks to ``date.max`` and raises ``OverflowError`` on the increment, which is a
    # 500 for the whole panel. The bound is the module's own, not the calendar's.
    class EverySession:
        def is_session(self, day: date) -> bool:
            return True

    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 100)
    _stub_space(monkeypatch, free=1 << 45)
    result = assess(tmp_path, today=MONDAY, calendar=EverySession())
    assert result.exhausts_on is None
    assert result.beyond_horizon is True


def test_headroom_under_the_threshold_is_short_and_a_day_over_it_is_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The flag the nightly report reads. It compares dates rather than converting the
    # capture-day count, because the threshold is calendar weeks and the count is not.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 100)
    calendar = _weekday_calendar(MONDAY, 400)
    _stub_space(monkeypatch, free=0)
    peak = assess(tmp_path, today=MONDAY, calendar=calendar).peak

    inside = MONDAY + timedelta(weeks=HEADROOM_WEEKS)
    sessions = sum(
        1
        for offset in range(1, (inside - MONDAY).days + 1)
        if calendar.is_session(MONDAY + timedelta(days=offset))
    )
    _stub_space(monkeypatch, free=peak * (sessions + 13))
    assert assess(tmp_path, today=MONDAY, calendar=calendar).short is True
    _stub_space(monkeypatch, free=peak * (sessions + 1 + 13))
    assert assess(tmp_path, today=MONDAY, calendar=calendar).short is False


# -- the device --------------------------------------------------------------


def test_free_space_is_read_off_the_real_device_and_is_the_available_figure(tmp_path: Path):
    # The one test that does not stub the device. ``shutil.disk_usage`` is the reader
    # rather than ``os.statvfs`` because ``statvfs`` reports two block sizes and only
    # ``f_frsize`` is the one ``f_bavail`` counts in. Multiplying by ``f_bsize`` overstates
    # free space 256 times on this platform, in the fail-open direction.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    stat = os.statvfs(tmp_path)
    # The machine's own disk moves between the two readings, so this matches the
    # multiplier rather than the byte. A megabyte of drift is ordinary. A wrong multiplier
    # is off by a factor of 256 on this platform.
    assert abs(result.free - stat.f_bavail * stat.f_frsize) < 1 << 20
    assert result.capacity == stat.f_blocks * stat.f_frsize
    if stat.f_bsize != stat.f_frsize:
        assert abs(result.free - stat.f_bavail * stat.f_bsize) > result.free


def test_the_usage_total_is_its_two_halves(tmp_path: Path):
    usage = Usage(
        entries=(),
        day_bytes={},
        unsealed=frozenset(),
        journal_bytes={},
        dated=7,
        undated=5,
        files=0,
        refusals=(),
        refused=0,
    )
    assert usage.total == 12


# -- the band between a full disk and one day's headroom ---------------------


@pytest.mark.parametrize("free", [0, 1, 2047])
def test_a_disk_with_under_one_day_left_exhausts_today_and_reads_short(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, free: int
):
    """The alarm's whole reason to exist, and it was inverted here.

    ``free // peak`` truncates every reading below one day's growth to zero, so a full
    disk and a nearly full one both arrive as ``capture_days_left == 0``. A forward walk
    looking for that zero steps past it on its first session, runs out its bound, and
    returns no date. No date reads as a runway too long to put a date on, which is the
    opposite of the truth, and ``short`` came back false on a disk with no room left.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _stub_space(monkeypatch, free=free)
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left == 0
    assert result.exhausts_on == MONDAY
    assert result.beyond_horizon is False
    assert result.short is True


def test_one_day_of_headroom_still_lands_on_the_next_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The other side of the boundary above, so the zero case cannot be "fixed" by a change
    # that also collapses one day into today.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    calendar = _weekday_calendar(MONDAY, 400)
    _stub_space(monkeypatch, free=0)
    peak = assess(tmp_path, today=MONDAY, calendar=calendar).peak
    _stub_space(monkeypatch, free=peak * (1 + 13))
    result = assess(tmp_path, today=MONDAY, calendar=calendar)
    assert result.capture_days_left == 1
    assert result.exhausts_on == MONDAY + timedelta(days=1)


# -- the window's edges ------------------------------------------------------


def test_the_window_includes_its_first_day_and_excludes_the_one_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Both edges, because a slip at either moves the runway by three orders of magnitude.

    The window is ``window_days`` wide with both ends inside it, so for a 30-day window
    ending on ``today`` the first day is ``today - 29``. A day parked on that boundary
    sets the rate. The same day one earlier does not.
    """
    today = date(2026, 9, 17)
    first = today - timedelta(days=29)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 10)
    _write(tmp_path, f"chains/ticker=QQQ/date={first}.parquet", 400_000)
    _stub_space(monkeypatch, free=1 << 40)
    on_edge = assess(tmp_path, today=today, calendar=_weekday_calendar(MONDAY, 400))
    assert on_edge.peak_day == first
    assert on_edge.window_start == first

    (tmp_path / "chains" / "ticker=QQQ" / f"date={first}.parquet").rename(
        tmp_path / "chains" / "ticker=QQQ" / f"date={first - timedelta(days=1)}.parquet"
    )
    outside = assess(tmp_path, today=today, calendar=_weekday_calendar(MONDAY, 400))
    assert outside.peak_day == date(2026, 9, 16)


def test_the_window_excludes_a_day_after_today(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A clock skew or a hand-written partition can date a file ahead of the session date.
    # It is not growth that has happened, so it must not set the rate.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 10)
    _write(tmp_path, "chains/ticker=QQQ/date=2026-09-30.parquet", 400_000)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.peak_day == date(2026, 9, 16)


# -- the block multiplier, against ground truth ------------------------------


def test_the_block_multiplier_is_checked_against_the_filesystem_not_itself(tmp_path: Path):
    """A ground-truth bound on the multiplier, rather than the constant on both sides.

    An assertion that reads ``BLOCK_BYTES`` to compute its own expectation is true for any
    value the constant holds, so it pins nothing. The filesystem's own figures do pin it:
    one file's allocated bytes must be at least its size and less than one block past it,
    rounded up to ``f_frsize``. A doubled multiplier breaks that bound.
    """
    size = 5000
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", size)
    usage = walk(tmp_path)
    frsize = os.statvfs(tmp_path).f_frsize
    assert usage.total >= size
    assert usage.total < size + frsize
    # And the multiplier itself, stated once rather than derived from the constant.
    assert runway.BLOCK_BYTES == 512


# -- what a refusal is allowed to say ----------------------------------------


def test_a_refusal_names_a_lake_relative_path_and_never_an_absolute_one(tmp_path: Path):
    """The dashboard publishes these strings, so a path in one is a disclosure.

    ``tests/integration/test_dashboard_http.py`` states the invariant: no response carries
    a path or a secret. A reader who can reach the port but cannot read the filesystem
    must not learn where the lake sits on disk.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    locked = tmp_path / "quotes"
    _write(tmp_path, "quotes/ticker=SPY/date=2026-09-14.parquet", 10)
    locked.chmod(0o000)
    try:
        usage = walk(tmp_path)
    finally:
        locked.chmod(0o755)
    assert usage.refusals == ("quotes: PermissionError",)
    for refusal in usage.refusals:
        assert str(tmp_path) not in refusal
        assert not refusal.startswith("/")


def test_a_missing_root_names_the_root_rather_than_its_absolute_path(tmp_path: Path):
    # The one refusal whose path is the root itself. Relative to itself it is "." , which
    # tells a reader nothing, so it is named in words.
    usage = walk(tmp_path / "not-a-lake")
    assert usage.refusals == ("the lake root: FileNotFoundError",)
    assert str(tmp_path) not in usage.refusals[0]


# -- the constants, pinned rather than echoed --------------------------------


def test_the_design_constants_are_pinned_to_their_literal_values():
    """A test that computes its expectation from the constant moves with it and holds nothing.

    ``HEADROOM_WEEKS`` is the threshold the nightly report flags at, and
    ``test_headroom_under_the_threshold_is_short_and_a_day_over_it_is_not`` derives its own
    boundary from it, so that test passes for any value. Changed to 1, a three-week runway
    silently stops being flagged. The window and the refusal cap have the same shape.
    """
    assert HEADROOM_WEEKS == 3
    assert runway.PAGE_FLOOR_WEEKS == 2
    assert runway.JOURNAL_RESERVE_SESSIONS == 13
    assert runway.GROWTH_WINDOW_DAYS == 30
    assert runway.NAMED_REFUSALS == 3
    assert runway.MAX_FORWARD_DAYS == 366 * 2


def test_a_runway_inside_the_forward_bound_still_gets_a_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # ``MAX_FORWARD_DAYS`` exists to stop a calendar that never refuses, not to withhold a
    # date a reader could have had. A runway a couple of months out is inside every real
    # calendar's horizon and must come back dated.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    calendar = _weekday_calendar(MONDAY, 400)
    _stub_space(monkeypatch, free=0)
    peak = assess(tmp_path, today=MONDAY, calendar=calendar).peak
    _stub_space(monkeypatch, free=peak * (45 + 13))
    result = assess(tmp_path, today=MONDAY, calendar=calendar)
    assert result.capture_days_left == 45
    assert result.exhausts_on is not None
    assert result.beyond_horizon is False


# -- the calendar's own refusal ----------------------------------------------


class _BoundedCalendar:
    """A calendar that raises past its horizon, the way ``exchange_calendars`` does.

    ``FakeCalendar`` returns ``False`` for a day it does not know and never raises, so no
    test built on it reaches the ``_CALENDAR_RANGE_ERRORS`` branch. The real adapter raises
    ``DateOutOfBounds``, which is a ``ValueError``, and that is the case the branch exists
    for.
    """

    def __init__(self, start: date, days: int) -> None:
        self._start = start
        self._last = start + timedelta(days=days)

    def is_session(self, day: date) -> bool:
        if day > self._last:
            raise ValueError(f"date out of bounds: {day}")
        return day.weekday() < 5


def test_a_calendar_that_raises_past_its_horizon_is_contained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The real calendar refuses rather than answering False. Letting that escape turns the
    # whole panel into a 500, which is the one outcome the module's containment rule
    # forbids.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _stub_space(monkeypatch, free=1 << 45)
    result = assess(tmp_path, today=MONDAY, calendar=_BoundedCalendar(MONDAY, 60))
    assert result.beyond_horizon is True
    assert result.exhausts_on is None
    assert result.capture_days_left > 0


# -- a file that will not stat ------------------------------------------------


def _unsearchable(directory: Path):
    """A directory that lists but whose children cannot be stat-ed, at mode 0o644.

    This is the case ``os.walk``'s ``onerror`` never sees. The listing succeeds, so the
    refusal surfaces on the per-file ``stat`` instead, which is a different clause.
    """
    directory.chmod(0o644)


def test_a_file_that_will_not_stat_is_a_named_refusal_and_not_a_silent_loss(tmp_path: Path):
    # The fail-open case the module exists to prevent, one clause over from the one the
    # directory test covers. The bytes are missing either way, so the growth rate is
    # understated and the runway lengthened. What must not also go missing is the line
    # saying so.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    locked = tmp_path / "quotes" / "ticker=SPY"
    _write(tmp_path, "quotes/ticker=SPY/date=2026-09-14.parquet", 10)
    _unsearchable(locked)
    try:
        usage = walk(tmp_path)
    finally:
        locked.chmod(0o755)
    assert usage.refused == 1
    assert usage.refusals == ("quotes/ticker=SPY/date=2026-09-14.parquet: PermissionError",)


def test_a_file_that_vanished_mid_walk_is_skipped_silently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The mirror of the test above, and it has to hold in the other direction. Compaction
    # prunes an emptied directory while holding the lake lock, so this happens every
    # weekday at close+15. Counting it as a refusal would light the panel up daily on a
    # lake behaving exactly as designed.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    real = Path.stat

    def vanishing(self: Path, *args: object, **kwargs: object):
        if self.name == "date=2026-09-14.parquet":
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", vanishing)
    usage = walk(tmp_path)
    assert usage.refused == 0
    assert usage.refusals == ()
    assert usage.files == 0


def test_a_day_whose_every_file_is_empty_is_not_a_capture_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # A zero-length file occupies no blocks, so the day it names grew the lake by nothing.
    # Counting it would halve the mean the panel prints beside the peak.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 400_000)
    _write(tmp_path, "journal/date=2026-09-15/surface=chains/ticker=SPY/seg-a.arrows", 0)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days == 1
    assert result.mean == result.peak


def test_each_entry_reports_its_own_file_count(tmp_path: Path):
    # The count rides beside the bytes because a tree that is large by file count and small
    # by bytes is the one whose allocated blocks diverge from its content.
    for index in range(5):
        _write(tmp_path, f"chains/ticker=SPY/date=2026-09-{10 + index}.parquet", 10)
    _write(tmp_path, "manifest.jsonl", 10)
    by_name = {entry.name: entry for entry in walk(tmp_path).entries}
    assert by_name["chains"].files == 5
    assert by_name["manifest.jsonl"].files == 1


# -- the totals accumulate, they do not overwrite -----------------------------


def test_an_entry_sums_every_file_under_it_rather_than_keeping_the_last(tmp_path: Path):
    """Two files under one surface must add up, and nothing was checking that they do.

    Every earlier assertion about entry bytes was either a one-file entry or a
    greater-than-zero check, so replacing the running sum with a plain assignment stayed
    green across the whole suite. The live lake holds fifteen chain partitions under one
    entry, so the panel's headline size would have read as the last file alone.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 40_000)
    _write(tmp_path, "chains/ticker=QQQ/date=2026-09-14.parquet", 30_000)
    _write(tmp_path, "quotes/ticker=SPY/date=2026-09-14.parquet", 100)
    by_name = {entry.name: entry for entry in walk(tmp_path).entries}
    frsize = os.statvfs(tmp_path).f_frsize
    # Two files of 40,000 and 30,000 bytes occupy at least their sizes and at most one
    # block more each, so the sum is bounded on both sides and a single file cannot reach
    # the lower bound.
    assert by_name["chains"].bytes >= 70_000
    assert by_name["chains"].bytes < 70_000 + 2 * frsize
    assert by_name["chains"].files == 2
    assert by_name["chains"].bytes > by_name["quotes"].bytes


def test_a_day_sums_every_partition_written_for_it(tmp_path: Path):
    """The real lake writes four to six partitions per capture day, one per ticker-surface.

    A day that kept only its last file would understate the busiest day, which sets the
    rate, which sets the runway. That is the fail-open direction, and the mutation
    survived the whole suite.
    """
    for ticker, size in (("SPY", 40_000), ("QQQ", 30_000)):
        _write(tmp_path, f"chains/ticker={ticker}/date=2026-09-14.parquet", size)
        _write(tmp_path, f"quotes/ticker={ticker}/date=2026-09-14.parquet", 100)
    usage = walk(tmp_path)
    frsize = os.statvfs(tmp_path).f_frsize
    day = usage.day_bytes[date(2026, 9, 14)]
    assert day >= 70_200
    assert day < 70_200 + 4 * frsize
    assert usage.files == 4
    assert usage.dated == day


# -- a symlink cannot import bytes from outside the lake ----------------------


def test_a_symlink_counts_itself_and_never_what_it_points_at(tmp_path: Path):
    """The sandbox's premise is that nothing outside ``lake_root`` reaches the panel.

    A symlink inside the lake pointing at a large file outside it would otherwise add that
    file's bytes to the lake's size and to the growth rate, which is the one thing the
    surrounding design says cannot happen. ``os.walk`` does not descend a symlinked
    directory and ``stat(follow_symlinks=False)`` measures the link rather than its target.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    big = outside / "big.parquet"
    big.write_bytes(b"x" * 200_000)
    lake = tmp_path / "lake"
    _write(lake, "chains/ticker=SPY/date=2026-09-14.parquet", 100)
    (lake / "chains" / "ticker=SPY" / "date=2026-09-15.parquet").symlink_to(big)
    (lake / "linked-tree").symlink_to(outside, target_is_directory=True)

    usage = walk(lake)
    frsize = os.statvfs(tmp_path).f_frsize
    # One real file, plus a symlink that occupies no data blocks of its own.
    assert usage.total < 200_000
    assert usage.total <= 2 * frsize
    # The symlinked directory is not descended, so it contributes no entry at all.
    assert {entry.name for entry in usage.entries} == {"chains"}


# -- a refusal must not take the rest of its directory with it ----------------


def test_every_unreadable_file_in_a_directory_is_counted_not_just_the_first(tmp_path: Path):
    """The per-file skip continues the listing. Breaking out of it loses the rest silently.

    The earlier refusal test put one file in the locked directory, so continuing and
    breaking were the same thing and the mutation survived. A real ticker directory holds
    one partition per day, so breaking on the first refusal drops every later day's bytes
    with no refusal recorded for any of them. Bytes missing understates growth, which
    lengthens the runway, which is the direction that keeps the alarm quiet.
    """
    locked = tmp_path / "chains" / "ticker=SPY"
    for day in range(10, 16):
        _write(tmp_path, f"chains/ticker=SPY/date=2026-09-{day}.parquet", 50_000)
    locked.chmod(0o644)
    try:
        usage = walk(tmp_path)
    finally:
        locked.chmod(0o755)
    assert usage.refused == 6
    assert usage.files == 0


def test_every_vanished_file_in_a_directory_is_skipped_without_losing_its_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The mirror. Compaction can prune more than one file from a directory between the
    # listing and the stat, and the ones it did not prune still have to be counted.
    for day in range(10, 16):
        _write(tmp_path, f"chains/ticker=SPY/date=2026-09-{day}.parquet", 50_000)
    real = Path.stat
    gone = {"date=2026-09-10.parquet", "date=2026-09-11.parquet"}

    def vanishing(self: Path, *args: object, **kwargs: object):
        if self.name in gone:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", vanishing)
    usage = walk(tmp_path)
    assert usage.refused == 0
    assert usage.files == 4


# -- the payload's types and its no-runway shape ------------------------------


def test_the_mean_is_a_whole_number_of_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # ``//`` rather than ``/``. A float reaches the page as ``212483584.0`` and is not a
    # count of bytes. ``==`` alone would not notice, because ``4096 == 4096.0``, so the
    # type is asserted beside the value.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 40_000)
    _write(tmp_path, "chains/ticker=QQQ/date=2026-09-15.parquet", 30_000)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert isinstance(result.mean, int)
    assert not isinstance(result.mean, bool)
    assert isinstance(result.peak, int)
    assert isinstance(result.capture_days_left, int)


def test_a_lake_with_no_growth_says_so_on_every_field_not_just_the_runway(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # ``beyond_horizon`` defaults to False and is only set by the forward walk, which does
    # not run when there is no rate. Defaulting it True would tell a reader the runway
    # outran the calendar when nothing was measured at all.
    _write(tmp_path, "manifest.jsonl", 10)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left is None
    assert result.beyond_horizon is False
    assert result.space_error is None
    assert isinstance(result.free, int)


def test_a_device_that_will_not_read_leaves_both_of_its_figures_unset(tmp_path: Path):
    # The device-error path sets ``space_error`` and leaves ``free`` and ``capacity``
    # alone. An empty string in either would be falsy on the page and wrong in the payload,
    # which is the kind of drift a contract test exists to stop.
    usage = assess(
        tmp_path / "not-a-lake",
        today=date(2026, 9, 17),
        calendar=_weekday_calendar(MONDAY, 400),
    )
    assert usage.free is None
    assert usage.capacity is None
    assert usage.space_error == "FileNotFoundError"


# -- the two tiers, counted in sessions from a Monday --------------------------


def _peak_of_one_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    """One sealed Monday partition, and the peak it sets, read with no free space at all."""
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _stub_space(monkeypatch, free=0)
    return assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400)).peak


@pytest.mark.parametrize(
    ("sessions", "short", "critical"),
    [(10, True, True), (11, True, False), (15, True, False), (16, False, False)],
)
def test_the_page_floor_is_two_weeks_and_the_report_line_is_three(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sessions: int,
    short: bool,
    critical: bool,
):
    """Both thresholds, from both sides, with the session counts written out.

    From a Monday with no holiday, two weeks is ten sessions and three weeks is fifteen, so
    the page fires at ten and not at eleven, and the report line fires at fifteen and not at
    sixteen. The counts are literals rather than derived from ``PAGE_FLOOR_WEEKS`` or
    ``HEADROOM_WEEKS``, so a changed constant turns this red. Free space is the sessions
    plus the 13-session journal reserve, which comes off first.
    """
    peak = _peak_of_one_day(tmp_path, monkeypatch)
    _stub_space(monkeypatch, free=peak * (sessions + 13))
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left == sessions
    assert result.short is short
    assert result.critical is critical


def test_free_space_under_the_reserve_reads_zero_days_and_fills_today(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The clamp. Without it, free space below the reserve reads a negative count.

    Twelve sessions of free space is one short of the 13-session reserve, which is a disk
    the next session's journal would fill. It reads zero capture days, fills today, and
    pages. Fourteen sessions leaves one, which is the other side of the line.
    """
    peak = _peak_of_one_day(tmp_path, monkeypatch)
    calendar = _weekday_calendar(MONDAY, 400)
    _stub_space(monkeypatch, free=peak * 12)
    under = assess(tmp_path, today=MONDAY, calendar=calendar)
    assert under.reserve == peak * 13
    assert under.capture_days_left == 0
    assert under.exhausts_on == MONDAY
    assert under.critical is True

    _stub_space(monkeypatch, free=peak * 14)
    over = assess(tmp_path, today=MONDAY, calendar=calendar)
    assert over.capture_days_left == 1
    assert over.exhausts_on == MONDAY + timedelta(days=1)


def test_the_reserve_is_thirteen_sessions_of_the_busiest_sealed_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Carried on the result so the panel can name it. With no journal anywhere the sealed
    # peak and the rate's peak are one figure, and the stuck-journal test below separates
    # them.
    peak = _peak_of_one_day(tmp_path, monkeypatch)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=MONDAY, calendar=_weekday_calendar(MONDAY, 400))
    assert result.reserve == 13 * peak
    assert result.reserve > 0


def test_todays_in_flight_journal_does_not_set_the_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Today's journal is still growing or about to be compacted, so it sets no rate.

    A journal segment is uncompressed and 9 to 13 times the partition it becomes. Read as
    the busiest day and multiplied by the reserve, today's would page every afternoon. So
    today counts only its sealed bytes, and the peak is the past days' 40,960. Today is
    still listed in the window, because its bytes are real. Counting today's journal would
    read a 901,120-byte peak. Every size is a whole number of 4,096-byte blocks, so the
    allocated bytes equal the written ones and the figures below are literals.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 40_960)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-15.parquet", 20_480)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 901_120)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 16), calendar=_weekday_calendar(MONDAY, 400))
    sizes = dict(result.window_days)
    assert sizes[date(2026, 9, 16)] == 901_120
    assert result.usage.journal_bytes == {date(2026, 9, 16): 901_120}
    assert result.peak_day == date(2026, 9, 14)
    assert result.peak == 40_960
    assert result.reserve == 532_480
    assert result.capture_days == 2
    assert result.mean == 30_720


def test_a_sealed_day_still_sets_the_rate_beside_todays_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The other side: today's segments come off and nothing else, so a past day larger than
    # today's journal is still the peak.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 901_120)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 40_960)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 16), calendar=_weekday_calendar(MONDAY, 400))
    assert result.peak_day == date(2026, 9, 14)
    assert result.peak == 901_120


def _twenty_weekdays(tmp_path: Path, files: Sequence[tuple[str, int]]) -> date:
    """Write ``files`` for each of the twenty weekdays from ``MONDAY``, and return the last.

    Each entry is a lake-relative path with ``{day}`` where the date goes, and its size.
    The last weekday is Friday 2026-10-09, which the tests below read as today, so nineteen
    of the twenty days are past days and one is today.
    """
    day = MONDAY
    written = 0
    while True:
        if day.weekday() < 5:
            for rel, size in files:
                _write(tmp_path, rel.format(day=day.isoformat()), size)
            written += 1
            if written == 20:
                return day
        day += timedelta(days=1)


def test_compaction_refusing_every_ticker_every_day_reads_short(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A past day's stuck journal is permanent growth, so it sets the rate.

    Compaction refused every ticker on every day, so each day is a 1,003,520-byte journal
    segment and the 4,096-byte timing file every session writes, with no Parquet at all.
    The segments stay on the disk until someone repairs them, so the disk grows by a whole
    day each session. Free space of about one day's journal is under one session left: it
    fills today and pages.

    Reading only sealed bytes, the rate would be the 4,096-byte timing file, and the same
    disk would read 232 capture days left with no flag. Today's segment is the one left
    out, because it may still be compacted, which leaves today's rate at 4,096 bytes. The
    reserve is thirteen of those timing files, since no day sealed anything else.
    """
    today = _twenty_weekdays(
        tmp_path,
        [
            ("journal/date={day}/surface=chains/ticker=SPY/seg-a.arrows", 1_003_520),
            ("journal/timing/date={day}.jsonl", 4_096),
        ],
    )
    assert today == date(2026, 10, 9)
    calendar = _weekday_calendar(MONDAY, 400)

    _stub_space(monkeypatch, free=1_003_520)
    full = assess(tmp_path, today=today, calendar=calendar)
    assert dict(full.window_days)[MONDAY] == 1_007_616
    assert full.peak == 1_007_616
    assert full.peak_day == MONDAY
    assert full.capture_days == 20
    # Nineteen past days of 1,007,616 bytes and today's 4,096, over twenty.
    assert full.mean == 957_440
    assert full.reserve == 53_248
    assert full.capture_days_left == 0
    assert full.exhausts_on == today
    assert full.short is True
    assert full.critical is True

    # The other side: sixteen of those days past the reserve is over three weeks.
    _stub_space(monkeypatch, free=53_248 + 16 * 1_007_616)
    roomy = assess(tmp_path, today=today, calendar=calendar)
    assert roomy.capture_days_left == 16
    assert roomy.short is False
    assert roomy.critical is False


def test_a_ticker_that_never_seals_adds_its_leftover_to_the_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Compaction refuses one ticker-day at a time and seals the rest.

    Each day here seals 602,112 bytes of SPY and leaves a 1,003,520-byte QQQ segment
    behind, so the disk grows by 1,605,632 bytes a session and that is the rate. The
    reserve is still thirteen sealed days, 7,827,456 bytes, because a journal scales with
    the sealed session it compacts into.

    At 17,461,248 bytes free, the sealed bytes alone would read sixteen capture days,
    which is over three weeks and flags nothing. The disk actually has six, which pages.
    """
    today = _twenty_weekdays(
        tmp_path,
        [
            ("chains/ticker=SPY/date={day}.parquet", 602_112),
            ("journal/date={day}/surface=chains/ticker=QQQ/seg-a.arrows", 1_003_520),
        ],
    )
    _stub_space(monkeypatch, free=17_461_248)
    result = assess(tmp_path, today=today, calendar=_weekday_calendar(MONDAY, 400))
    assert len(result.usage.unsealed) == 20
    assert result.peak == 1_605_632
    assert result.reserve == 7_827_456
    assert result.capture_days_left == 6
    assert result.short is True
    assert result.critical is True


def test_a_leftover_segment_leaves_its_days_sealed_bytes_in_the_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A day with one small leftover segment still counts its whole sealed set.

    Dropping every such day from the rate read twenty weekdays of 602,112 sealed bytes,
    each beside a 4,096-byte leftover segment, as no growth at all. On a full disk that
    was no runway, not short, not critical, and no page. Each past day now counts in full,
    606,208 bytes, since its leftover stays on the disk. Today counts 602,112, because its
    segment may still be compacted. So a full disk fills today and pages.

    The reserve is thirteen sealed days, 7,827,456 bytes. Sixteen sessions past it at the
    606,208-byte rate is over three weeks and does neither. The free space here was once
    sixteen sessions at the sealed 602,112 bytes. At the full rate that same figure buys
    fifteen, which is short, so the free space is now written in the full rate.
    """
    today = _twenty_weekdays(
        tmp_path,
        [
            ("chains/ticker=SPY/date={day}.parquet", 602_112),
            ("journal/date={day}/surface=chains/ticker=QQQ/seg-a.arrows", 4_096),
        ],
    )
    calendar = _weekday_calendar(MONDAY, 400)

    _stub_space(monkeypatch, free=0)
    full = assess(tmp_path, today=today, calendar=calendar)
    assert len(full.usage.unsealed) == 20
    assert full.peak == 606_208
    # Nineteen past days of 606,208 bytes and today's 602,112, over twenty, rounded down.
    assert full.mean == 606_003
    assert full.capture_days == 20
    assert full.reserve == 7_827_456
    assert full.capture_days_left == 0
    assert full.exhausts_on == today
    assert full.short is True
    assert full.critical is True

    _stub_space(monkeypatch, free=7_827_456 + 16 * 606_208)
    roomy = assess(tmp_path, today=today, calendar=calendar)
    assert roomy.capture_days_left == 16
    assert roomy.short is False
    assert roomy.critical is False


def test_the_busiest_day_sets_the_peak_with_its_leftover_and_the_reserve_without(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The busiest day is a past one carrying a leftover. Its whole 405,504 bytes stay on
    # the disk, so that is the peak. Dropping it whole would hand the peak to the next day
    # down, 200,704 bytes. The reserve is thirteen of its sealed 401,408 bytes, 5,218,304.
    # The mean is 708,608 bytes over three days, rounded down.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 200_704)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-15.parquet", 401_408)
    _write(tmp_path, "journal/date=2026-09-15/surface=chains/ticker=QQQ/seg-a.arrows", 4_096)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 102_400)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 16), calendar=_weekday_calendar(MONDAY, 400))
    assert dict(result.window_days)[date(2026, 9, 15)] == 405_504
    assert result.peak_day == date(2026, 9, 15)
    assert result.peak == 405_504
    assert result.reserve == 5_218_304
    assert result.capture_days == 3
    assert result.mean == 236_202


def test_a_stuck_journal_raises_the_rate_and_not_the_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The reserve is thirteen of the busiest sealed day, whatever a past journal holds.

    Monday sealed 401,408 bytes. Tuesday sealed 200,704 and left a 2,002,944-byte journal
    stuck behind. Tuesday's whole 2,203,648 bytes are on the disk, so it sets the rate.
    The reserve holds one session's journal, which scales with the sealed session, so it
    is thirteen of Monday's 401,408 bytes, 5,218,304. Thirteen of Tuesday's would be
    28,647,424, a stuck day inflating the reserve thirteen-fold.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 401_408)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-15.parquet", 200_704)
    _write(tmp_path, "journal/date=2026-09-15/surface=chains/ticker=QQQ/seg-a.arrows", 2_002_944)
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 102_400)
    _stub_space(monkeypatch, free=50_000_000)
    result = assess(tmp_path, today=date(2026, 9, 16), calendar=_weekday_calendar(MONDAY, 400))
    assert result.peak_day == date(2026, 9, 15)
    assert result.peak == 2_203_648
    assert result.reserve == 5_218_304
    # (50,000,000 - 5,218,304) // 2,203,648.
    assert result.capture_days_left == 20


def test_a_timing_file_counts_as_its_days_sealed_growth_and_not_as_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # ``journal/timing/date=D.jsonl`` sits under ``journal/`` but is not a segment. It
    # outlives the seal and is permanent growth, so it stays in today's sealed bytes and in
    # the reserve's basis. Reading it as journal would take 8,192 bytes off today's rate,
    # leaving 401,408, and 106,496 off the reserve.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-16.parquet", 401_408)
    _write(tmp_path, "journal/timing/date=2026-09-16.jsonl", 8_192)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=QQQ/seg-a.arrows", 4_096)
    _stub_space(monkeypatch, free=1 << 40)
    result = assess(tmp_path, today=date(2026, 9, 16), calendar=_weekday_calendar(MONDAY, 400))
    assert result.usage.journal_bytes == {date(2026, 9, 16): 4_096}
    assert result.usage.day_bytes[date(2026, 9, 16)] == 413_696
    assert result.peak == 409_600
    assert result.reserve == 5_324_800


# -- what the walk skips at the root -----------------------------------------


def test_lost_and_found_at_the_root_is_skipped_and_one_below_it_is_refused(tmp_path: Path):
    """``mkfs.ext4`` leaves ``lost+found`` at the volume's root, owned by root at 0700.

    On the hosted VM ``lake_root`` is that mount point, so a walk that descended into it
    would report a refusal every night that names nothing about the lake. One anywhere
    else is not the filesystem's, so an unreadable one is still a refusal.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "lost+found/orphan", 10)
    _write(tmp_path, "chains/lost+found/orphan", 10)
    at_root = tmp_path / "lost+found"
    nested = tmp_path / "chains" / "lost+found"
    at_root.chmod(0o000)
    nested.chmod(0o000)
    try:
        usage = walk(tmp_path)
    finally:
        at_root.chmod(0o755)
        nested.chmod(0o755)
    assert usage.refusals == ("chains/lost+found: PermissionError",)
    assert usage.refused == 1
    assert "lost+found" not in {entry.name for entry in usage.entries}


def test_a_readable_lost_and_found_at_the_root_is_not_counted_either(tmp_path: Path):
    # Skipped before it is listed, so a readable one adds no entry and no bytes. A walk that
    # only swallowed its refusal would still count what it could read.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "lost+found/orphan", 50_000)
    usage = walk(tmp_path)
    assert {entry.name for entry in usage.entries} == {"chains"}
    assert usage.files == 1


# -- a directory that vanishes mid-walk --------------------------------------


def _vanish_on_listing(monkeypatch: pytest.MonkeyPatch, target: Path) -> None:
    """Remove ``target`` the moment the walk lists it, the way compaction's prune does.

    ``os.walk`` lists each directory through ``os.scandir`` after its parent's listing
    already named it, so removing it at that call is the race ``_prune_empty`` runs.
    """
    real = os.scandir

    def scandir(path=None):
        # ``shutil.rmtree`` lists by file descriptor, so only a path is compared.
        if isinstance(path, (str, os.PathLike)) and Path(path) == target and target.exists():
            shutil.rmtree(target)
        return real(path)

    monkeypatch.setattr(os, "scandir", scandir)


def test_a_directory_pruned_mid_walk_is_skipped_and_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Compaction removes an emptied ``journal/date=<D>/`` while holding the lake lock.

    Measured before the fix, a walk that met it reported
    ``journal/date=2026-10-07: FileNotFoundError``, every weekday at close+15 on a lake
    behaving as designed. The vanished file is already skipped, and this is its directory
    form.
    """
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _write(tmp_path, "journal/date=2026-09-16/surface=chains/ticker=SPY/seg-a.arrows", 10)
    _vanish_on_listing(monkeypatch, tmp_path.resolve() / "journal" / "date=2026-09-16")
    usage = walk(tmp_path)
    assert usage.refused == 0
    assert usage.refusals == ()
    assert {entry.name for entry in usage.entries} == {"chains"}


def test_a_root_that_vanishes_as_the_walk_starts_is_still_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The skip is for directories below the root. The root going away is a panel pointed at
    # nothing, the same as a root that was never there.
    lake = tmp_path / "lake"
    _write(lake, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    _vanish_on_listing(monkeypatch, lake.resolve())
    usage = walk(lake)
    assert usage.refusals == ("the lake root: FileNotFoundError",)


def test_a_vanished_listing_that_names_no_path_is_still_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # A ``FileNotFoundError`` carrying no filename cannot be told apart from the root
    # going, so it refuses rather than being skipped.
    _write(tmp_path, "chains/ticker=SPY/date=2026-09-14.parquet", 10)
    target = tmp_path.resolve() / "chains" / "ticker=SPY"
    real = os.scandir

    def scandir(path=None):
        if isinstance(path, (str, os.PathLike)) and Path(path) == target:
            raise FileNotFoundError("gone")
        return real(path)

    monkeypatch.setattr(os, "scandir", scandir)
    usage = walk(tmp_path)
    assert usage.refused == 1
