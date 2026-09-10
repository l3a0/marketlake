"""Startup gap marking, driven the way the daemon drives it.

The design's rules for D10 are stated in whole sessions and whole minutes, so these
tests are too. Each one names the rule it checks.
"""

from __future__ import annotations

import collections
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import gap, journal
from lake.session import SessionClock
from lake.tickers import Roster, TickerConfig
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

# Monday of the week these tests live in. Its sessions run Monday through Friday.
WEEK = date(2026, 8, 31)
SPY = TickerConfig(ticker="SPY", options=True)
EQUITY_ONLY = TickerConfig(ticker="XYZ", options=False)

# A regular session is 09:30 through 16:15 inclusive, the option close.
FULL_SESSION_SLOTS = 406


def _marker(
    root: Path, at: datetime, *, roster: Roster, pid: int = 4242, holidays=()
) -> gap.GapMarker:
    calendar = weekday_sessions(WEEK, holidays=holidays)
    clock = ManualClock(start=at)
    return gap.GapMarker(
        lake_root=root,
        roster=roster,
        session_clock=SessionClock(clock=clock, calendar=calendar),
        pid=pid,
    )


def _record(root: Path, surface: str, ticker: str, slot: datetime, *, kind: str = "x") -> None:
    """Put one recorded row on disk, standing for a captured cycle."""
    batch = journal.gap_rows(surface, ticker=ticker, slots=[slot], error_class=kind)
    stamp = slot.strftime(gap.SEGMENT_STAMP_FORMAT)
    with journal.SegmentWriter.open(root, surface, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _slots(root: Path, surface: str, ticker: str, day: date) -> list[str]:
    directory = journal.segment_dir(root, surface, ticker, day)
    if not directory.is_dir():
        return []
    found: list[str] = []
    for path in sorted(directory.glob("*.arrows")):
        found += journal.read_segment(path).column("snap_ts").to_pylist()
    return found


# -- rule 6: what each kind of missed date gets --------------------------------------


def test_a_partially_captured_date_is_marked_from_its_last_row_to_its_option_close(tmp_path):
    _record(tmp_path, "quotes", "SPY", et(2026, 9, 1, 11, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((SPY,))).on_start()

    tuesday = [s for s in report.spans if s.day == date(2026, 9, 1) and s.surface == "quotes"]
    assert len(tuesday) == 1
    # 11:01 through 16:15 inclusive.
    assert tuesday[0].slots == 315
    marked = sorted(_slots(tmp_path, "quotes", "SPY", date(2026, 9, 1)))
    assert marked[1].startswith("2026-09-01T11:01")
    assert marked[-1].startswith("2026-09-01T16:15")


def test_a_fully_dark_date_is_marked_for_its_whole_session(tmp_path):
    _record(tmp_path, "quotes", "SPY", et(2026, 8, 31, 16, 15))
    _record(tmp_path, "chains", "SPY", et(2026, 8, 31, 16, 15))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((SPY,))).on_start()

    wednesday_dark = [s for s in report.spans if s.day == date(2026, 9, 1)]
    assert [s.slots for s in wednesday_dark] == [FULL_SESSION_SLOTS] * 2


def test_a_holiday_inside_the_dark_stretch_is_marked_not_at_all(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 8, 31, 16, 15))
    marker = _marker(
        tmp_path,
        et(2026, 9, 4, 10, 0),
        roster=Roster((EQUITY_ONLY,)),
        holidays=(date(2026, 9, 2),),
    )
    days = {span.day for span in marker.on_start().spans}
    assert date(2026, 9, 2) not in days
    assert {date(2026, 9, 1), date(2026, 9, 3), date(2026, 9, 4)} <= days


# -- rule 7: today's markers stop at the first live slot -----------------------------


def test_todays_markers_stop_at_the_first_slot_the_loop_will_capture(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 16, 15))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    today = [s for s in report.spans if s.day == date(2026, 9, 2)]
    # 09:30 through 10:00 inclusive. The daemon's own start minute is marked, because
    # the loop sleeps to the next top and will never run a cycle for it.
    assert [s.slots for s in today] == [31]
    marked = sorted(_slots(tmp_path, "quotes", "XYZ", date(2026, 9, 2)))
    assert marked[-1].startswith("2026-09-02T10:00")


def test_a_post_close_restart_clips_todays_markers_at_the_option_close(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 11, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 23, 30), roster=Roster((EQUITY_ONLY,))).on_start()

    today = [s for s in report.spans if s.day == date(2026, 9, 2)]
    assert [s.slots for s in today] == [315]
    assert sorted(_slots(tmp_path, "quotes", "XYZ", date(2026, 9, 2)))[-1].startswith(
        "2026-09-02T16:15"
    )


# -- the crash-loop property ---------------------------------------------------------


def test_a_second_restart_the_same_minute_marks_nothing(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    first = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,)), pid=1)
    second = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,)), pid=2)

    assert first.on_start().rows > 0
    # The anchor counts marker rows, so the first pass's own markers move it forward.
    assert second.on_start().rows == 0


def test_repeated_restarts_never_mark_one_minute_twice(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    for minute, pid in ((0, 1), (0, 2), (30, 3), (45, 4)):
        _marker(
            tmp_path, et(2026, 9, 2, 10, minute), roster=Roster((EQUITY_ONLY,)), pid=pid
        ).on_start()

    for day in (date(2026, 9, 1), date(2026, 9, 2)):
        marked = _slots(tmp_path, "quotes", "XYZ", day)
        repeated = [s for s, n in collections.Counter(marked).items() if n > 1]
        assert repeated == [], f"{day} marked a minute twice"


# -- rule 4: a sealed date is skipped ------------------------------------------------


def test_a_sealed_date_is_skipped_and_named(tmp_path, monkeypatch):
    from lake import manifest

    _record(tmp_path, "quotes", "XYZ", et(2026, 8, 31, 16, 15))
    sealed_key = "quotes/ticker=XYZ/date=2026-09-01.parquet"
    monkeypatch.setattr(manifest, "latest_entries", lambda root: {sealed_key: {"row_count": 1}})
    monkeypatch.setattr(gap, "latest_entries", lambda root: {sealed_key: {"row_count": 1}})

    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()
    assert sealed_key in report.sealed
    assert date(2026, 9, 1) not in {span.day for span in report.spans}


# -- rule 2: what a marker row holds -------------------------------------------------


def test_a_marker_row_holds_no_market_data_and_names_its_reason(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 9, 59))
    _marker(tmp_path, et(2026, 9, 2, 10, 5), roster=Roster((EQUITY_ONLY,))).on_start()

    directory = journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2))
    written = [p for p in sorted(directory.glob("*.arrows"))][-1]
    rows = journal.read_segment(written).to_pylist()
    assert rows, "the marking pass wrote no rows"
    for row in rows:
        assert row["row_kind"] == journal.ROW_KIND_GAP
        assert row["error_class"] == gap.DAEMON_DEAD
        assert row["fetch_ts"] is None
        assert row["vendor_quote_ts"] is None
        named = {"snap_ts", "ticker", "row_kind", "error_class", "suspect", "schema_version"}
        assert {k for k, v in row.items() if v is not None} <= named | {"session_phase"}


def test_a_post_equity_close_marker_carries_the_phase_a_captured_row_would_have(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 15, 59))
    _marker(tmp_path, et(2026, 9, 2, 23, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    rows = {
        r["snap_ts"]: r["session_phase"]
        for p in sorted(
            journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2)).glob("*.arrows")
        )
        for r in journal.read_segment(p).to_pylist()
    }
    before = next(v for k, v in rows.items() if k.startswith("2026-09-02T15:59"))
    after = next(v for k, v in rows.items() if k.startswith("2026-09-02T16:10"))
    assert before is None
    assert after == "post_equity_close"


def test_a_marker_never_carries_a_close_tag(tmp_path):
    # The design makes the close+5 guard the sole writer of an absent close marker. A
    # dark day therefore has no option-close-tagged cycle, and a snap=None load fails
    # loudly rather than resolving to a row that stands for a close nobody observed.
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    for p in sorted(
        journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 1)).glob("*.arrows")
    ):
        for row in journal.read_segment(p).to_pylist():
            assert row["close_tag"] is None


# -- the surfaces a ticker is marked on ----------------------------------------------


def test_an_equity_only_ticker_is_marked_on_quotes_alone(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()
    assert {span.surface for span in report.spans} == {"quotes"}


def test_an_options_ticker_is_marked_on_both_surfaces(tmp_path):
    _record(tmp_path, "quotes", "SPY", et(2026, 9, 1, 11, 0))
    _record(tmp_path, "chains", "SPY", et(2026, 9, 1, 11, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((SPY,))).on_start()
    assert {span.surface for span in report.spans} == {"chains", "quotes"}


def test_the_surfaces_rule_matches_the_one_capture_plans_with(tmp_path):
    assert gap.surfaces_for(SPY) == ("chains", "quotes")
    assert gap.surfaces_for(EQUITY_ONLY) == ("quotes",)


# -- the skipped-slot producer -------------------------------------------------------


def test_a_slot_the_live_loop_slept_through_is_not_called_daemon_dead(tmp_path):
    marker = _marker(tmp_path, et(2026, 9, 2, 10, 5), roster=Roster((EQUITY_ONLY,)))
    report = marker.on_skipped([et(2026, 9, 2, 10, 3), et(2026, 9, 2, 10, 4)])

    assert report.rows == 2
    rows = [
        r
        for p in sorted(
            journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2)).glob("*.arrows")
        )
        for r in journal.read_segment(p).to_pylist()
    ]
    # The daemon is alive on an overrun. Stamping it dead would make the marker lie
    # about the one thing it exists to record.
    assert {r["error_class"] for r in rows} == {gap.SLOT_OVERRUN}


def test_both_producers_write_through_the_same_writer(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 9, 59))
    marker = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,)))
    marker.on_start()
    after = marker.on_skipped([et(2026, 9, 2, 10, 1)])
    # The startup pass stopped at 10:00, so the skipped slot does not collide with it.
    assert after.rows == 1
    marked = _slots(tmp_path, "quotes", "XYZ", date(2026, 9, 2))
    assert len(marked) == len(set(marked))


# -- nothing owed, nothing written ---------------------------------------------------


def test_a_ticker_with_no_record_and_no_capture_start_is_not_marked(tmp_path):
    # Nothing in the lake has ever claimed the instrument was in scope, so a first-ever
    # start marks nothing rather than inventing ninety days of absence.
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()
    assert report.spans == ()
    assert not list(tmp_path.rglob("*.arrows"))
    # Marked nothing on purpose still says so, because it must not look like marked
    # nothing by mistake.
    assert report.truncated == ("quotes/XYZ",)


def test_a_marking_pass_with_no_missed_minutes_opens_no_segment(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 10, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()
    assert report.spans == ()
    assert (
        len(list(journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2)).glob("*.arrows")))
        == 1
    )


# -- the walk-back cap ---------------------------------------------------------------


def test_a_walk_that_reaches_the_cap_is_reported_rather_than_silent(tmp_path, monkeypatch):
    from lake import security_master

    class Master:
        def resolve(self, symbol, on, id_type=None):
            return 1

        def capture_start_of(self, instrument_id):
            return et(2020, 1, 2, 9, 30)

    monkeypatch.setattr(gap, "MAX_LOOKBACK_SESSIONS", 3)
    calendar = weekday_sessions(WEEK)
    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=calendar),
        master=Master(),
        pid=7,
    )
    report = marker.on_start()
    assert report.truncated == ("quotes/XYZ",)
    assert security_master is not None  # the import is the point of the fixture


def test_an_unresolvable_ticker_never_stops_the_daemon_from_starting(tmp_path):
    from lake.security_master import UnknownInstrument

    class Master:
        def resolve(self, symbol, on, id_type=None):
            return 9

        def capture_start_of(self, instrument_id):
            raise UnknownInstrument(instrument_id)

    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    calendar = weekday_sessions(WEEK)
    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=calendar),
        master=Master(),
        pid=7,
    )
    # The daemon runs under KeepAlive and run_loop does not guard on_start, so a raise
    # here would be a crash loop that marks nothing.
    report = marker.on_start()
    assert report.rows > 0


@pytest.mark.parametrize("kind", [journal.ROW_KIND_DATA, journal.ROW_KIND_GAP])
def test_the_anchor_counts_rows_of_every_kind(tmp_path, kind):
    slot = et(2026, 9, 2, 11, 0)
    schema = journal.schema_for("quotes")
    batch = journal._batch(
        schema,
        [{"snap_ts": slot.isoformat(), "ticker": "XYZ", "row_kind": kind, "schema_version": 1}],
    )
    with journal.SegmentWriter.open(tmp_path, "quotes", "XYZ", slot.date(), "s", 1) as writer:
        writer.write_cycle(batch)
    assert journal.last_recorded_slot(tmp_path, "quotes", "XYZ", slot.date()).slot == slot


# -- the production wiring -----------------------------------------------------------


def test_the_daemon_wires_gap_marking_into_both_hooks(tmp_path, monkeypatch, capsys):
    """The loop really marks, rather than the marker merely working in isolation.

    Without this the whole binding in ``run_loop_from_config`` can be deleted and the
    suite stays green, which is what a review found.
    """
    from lake import daemon
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")
    _record(lake_root, "quotes", "XYZ", et(2026, 9, 1, 11, 0))

    # Start before the open. The tick is pre-open, so the loop runs no capture cycle and
    # nothing reaches for a vendor token. `on_start` fires before the loop either way,
    # which is the wiring this checks.
    clock = ManualClock(start=et(2026, 9, 2, 8, 0))
    ticks = [0]

    def once() -> bool:
        ticks[0] += 1
        return ticks[0] <= 1

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        # The power assertion is a seam for a reason. Left to its default it spawns the
        # real `caffeinate`, which exists on macOS and not on a Linux CI runner.
        assertion_runner=lambda args: None,
        transport=FakeTransport(),
        pinger=FakePinger(),
        should_continue=once,
    )
    marked = _slots(lake_root, "quotes", "XYZ", date(2026, 9, 1))
    assert marked, "the daemon ran a whole tick and marked nothing"
    # Tuesday went dark after 11:00, so its tail is marked to the option close.
    assert marked[-1].startswith("2026-09-01T16:15")


def test_a_marking_pass_that_hits_a_problem_says_so_on_stderr(tmp_path, capsys):
    from lake import daemon
    from lake.gap import MarkingReport

    daemon._report(MarkingReport(truncated=("quotes/XYZ",)), "startup")
    assert "truncated=quotes/XYZ" in capsys.readouterr().err


def test_an_ordinary_marking_pass_stays_quiet(tmp_path, capsys):
    from lake import daemon
    from lake.gap import MarkedSpan, MarkingReport

    span = MarkedSpan("quotes", "XYZ", date(2026, 9, 2), 5, Path("x"))
    daemon._report(MarkingReport(spans=(span,)), "startup")
    assert capsys.readouterr().err == ""


def test_a_day_whose_record_cannot_be_read_is_refused_rather_than_over_marked(tmp_path):
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    corrupt = next(
        journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 1)).glob("*.arrows")
    )
    corrupt.write_bytes(b"not arrow at all")

    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()
    # A full session of daemon_dead markers over a day that really was captured would
    # be worse than marking nothing.
    assert report.rows == 0
    assert report.problems == ("quotes/XYZ 2026-09-01: 1 unreadable",)


def test_the_walk_back_cap_counts_sessions_not_calendar_days(tmp_path, monkeypatch):
    class Master:
        def resolve(self, symbol, on, id_type=None):
            return 1

        def capture_start_of(self, instrument_id):
            return et(2020, 1, 2, 9, 30)

    # Six sessions spans two weekends, so a calendar-day cap would fall short.
    monkeypatch.setattr(gap, "MAX_LOOKBACK_SESSIONS", 6)
    clock = ManualClock(start=et(2026, 9, 8, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK, date(2026, 9, 7))),
        master=Master(),
        pid=7,
    )
    report = marker.on_start()
    days = sorted({span.day for span in report.spans})
    assert len(days) == 6, days
    assert days[0] == date(2026, 9, 1)
    # The oldest session the walk examined is marked in full, not sliced at the
    # restart's time of day.
    oldest = [s for s in report.spans if s.day == days[0]]
    assert [s.slots for s in oldest] == [FULL_SESSION_SLOTS]


@pytest.mark.parametrize(
    ("start_second", "pass_seconds"),
    [(0, 0), (30, 0), (50, 75), (59, 1), (0, 200)],
)
def test_no_minute_falls_between_the_startup_pass_and_the_first_cycle(
    start_second, pass_seconds, tmp_path
):
    """The union of marked and captured minutes is contiguous, whatever the pass costs.

    Startup marking bounds itself at the minute it began. A pass that outlives that
    minute pushes the loop's first tick past the bound, and the minutes in between
    belong to neither producer unless the loop is seeded. That is the one-minute hole
    gap marking exists to close, reopened by gap marking itself.
    """
    from datetime import timedelta

    from lake import daemon

    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 9, 59))
    clock = ManualClock(start=et(2026, 9, 2, 10, 0) + timedelta(seconds=start_second))
    session_clock = SessionClock(clock=clock, calendar=weekday_sessions(WEEK))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=session_clock,
        pid=11,
    )

    def slow_start() -> None:
        marker.on_start()
        clock.advance(seconds=pass_seconds)

    captured: list[datetime] = []
    ticks = [0]

    def twice() -> bool:
        ticks[0] += 1
        return ticks[0] <= 2

    daemon.run_loop(
        session_clock,
        lambda **kwargs: None,
        clock=clock,
        hooks=daemon.DaemonHooks(
            on_start=slow_start,
            on_skipped=marker.on_skipped,
            on_cycle=lambda slot, result: captured.append(slot),
        ),
        should_continue=twice,
    )

    covered = sorted(
        {datetime.fromisoformat(s) for s in _slots(tmp_path, "quotes", "XYZ", date(2026, 9, 2))}
        | set(captured)
    )
    holes = [
        (covered[i] + timedelta(minutes=1)).isoformat()
        for i in range(len(covered) - 1)
        if covered[i + 1] - covered[i] != timedelta(minutes=1)
    ]
    assert holes == [], f"minutes in no row at all: {holes}"
    assert covered[-1] == captured[-1]
