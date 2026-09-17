"""The disk runway: what the walk counts, what sets the rate, and what it refuses.

Every test here builds a real tree under ``tmp_path`` and reads it. Free space is the one
thing stubbed, because the arithmetic has to be deterministic and the machine's own free
space is not. ``shutil.disk_usage`` itself is exercised unstubbed in the one test that
checks the module reads the real device at all.
"""

from __future__ import annotations

import os
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
    assert result.capture_days_left == 10_000_000 // result.peak


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
    # Reachable three ways: a fresh root, a lake read before its first capture day, and a
    # window capture was down through. A runway goes unbounded exactly when capture has
    # stopped, so reporting a large number there would go quiet at the one moment
    # something is wrong. It is also the division by zero.
    _write(tmp_path, "manifest.jsonl", 10)
    _stub_space(monkeypatch, free=10_000_000)
    result = assess(tmp_path, today=date(2026, 9, 17), calendar=_weekday_calendar(MONDAY, 400))
    assert result.capture_days_left is None
    assert result.exhausts_on is None
    assert result.mean is None
    assert result.short is False


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
    # Five sessions' worth of free space, priced at the one day that wrote bytes.
    _stub_space(monkeypatch, free=result.peak * 5)
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
    _stub_space(monkeypatch, free=peak * sessions)
    assert assess(tmp_path, today=MONDAY, calendar=calendar).short is True
    _stub_space(monkeypatch, free=peak * (sessions + 1))
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
    _stub_space(monkeypatch, free=peak)
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
    _stub_space(monkeypatch, free=peak * 45)
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
