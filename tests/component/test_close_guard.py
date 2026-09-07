"""Close tags, the session-relative dispatcher, and the close+5 guard."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import close_guard, daemon, journal
from lake.capture import CycleResult
from lake.paths import LakePaths
from lake.session import (
    OPTION_CLOSE,
    SPOT_CLOSE,
    SessionClock,
    SessionDispatch,
)
from lake.tickers import Roster, TickerConfig
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock

WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)
SPY = TickerConfig(ticker="SPY", options=True)
EQUITY_ONLY = TickerConfig(ticker="XYZ", options=False)


def _clock(at: datetime) -> SessionClock:
    return SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK))


def _row(
    root: Path, surface: str, ticker: str, slot: datetime, *, tag: str | None, kind: str
) -> None:
    """One recorded row under a close tag, standing for a cycle that ran."""
    schema = journal.schema_for(surface)
    batch = journal._batch(
        schema,
        [
            {
                "snap_ts": slot.isoformat(),
                "ticker": ticker,
                "row_kind": kind,
                "close_tag": tag,
                "schema_version": 1,
            }
        ],
    )
    stamp = slot.strftime("%Y%m%dT%H%M%S%f")
    with journal.SegmentWriter.open(root, surface, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _rows(root: Path, surface: str, ticker: str, day: date) -> list[dict]:
    directory = LakePaths(root).segment_dir(surface, ticker, day)
    if not directory.is_dir():
        return []
    return [
        row
        for path in sorted(directory.glob("*.arrows"))
        for row in journal.read_segment(path).to_pylist()
    ]


# -- the tags themselves -------------------------------------------------------------


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (9, 30, None),
        (12, 0, None),
        (15, 59, None),
        (16, 0, SPOT_CLOSE),
        (16, 14, None),
        (16, 15, OPTION_CLOSE),
        (16, 16, None),
    ],
)
def test_exactly_two_slots_a_day_carry_a_tag(hour, minute, expected):
    assert _clock(et(2026, 9, 2, 10, 0)).close_tag_at(et(2026, 9, 2, hour, minute)) == expected


def test_a_non_session_day_tags_nothing():
    # Saturday. The guard must not tag a minute the calendar never opened.
    assert _clock(et(2026, 9, 2, 10, 0)).close_tag_at(et(2026, 9, 5, 16, 0)) is None


def test_the_tags_follow_an_early_close_rather_than_the_wall_clock():
    from tests.support.calendar import FakeCalendar, SessionTimes

    early = FakeCalendar(
        {
            DAY: SessionTimes(
                open=et(2026, 9, 2, 9, 30),
                close=et(2026, 9, 2, 13, 0),
                early_close=True,
            )
        }
    )
    clock = SessionClock(clock=ManualClock(start=et(2026, 9, 2, 10, 0)), calendar=early)
    assert clock.close_tag_at(et(2026, 9, 2, 13, 0)) == SPOT_CLOSE
    assert clock.close_tag_at(et(2026, 9, 2, 13, 15)) == OPTION_CLOSE
    assert clock.close_tag_at(et(2026, 9, 2, 16, 0)) is None


def test_the_daemon_answers_the_close_tag_hook_from_the_calendar(tmp_path):
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    tags: list[str | None] = []
    clock = ManualClock(start=et(2026, 9, 2, 15, 58))
    ticks = [0]

    def three() -> bool:
        ticks[0] += 1
        return ticks[0] <= 3

    # A bare daemon answers None for every slot. This pins that the production entry
    # binds the calendar's answer instead.
    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        # A real cycle runner always returns a CycleResult, and the loop now hands it
        # to observers, so a fake that returned None would be lying about the contract.
        cycle_runner=lambda *, close_tag, session_phase: (
            tags.append(close_tag) or CycleResult(et(2026, 9, 2, 16, 0), ())
        ),
        should_continue=three,
    )
    # 15:59, 16:00, 16:01. A bare daemon answers None for every slot, so this pins that
    # the production entry binds the calendar's answer instead.
    assert tags == [None, SPOT_CLOSE, None]


# -- the dispatcher ------------------------------------------------------------------


def test_the_dispatcher_fires_once_at_its_moment_and_not_before():
    fired: list[date] = []
    dispatch = SessionDispatch(
        session_clock=_clock(et(2026, 9, 2, 10, 0)),
        moment=lambda bounds: bounds.option_close_deadline,
        job=fired.append,
    )
    assert dispatch.check(et(2026, 9, 2, 16, 19)) is False
    assert dispatch.check(et(2026, 9, 2, 16, 20)) is True
    assert dispatch.check(et(2026, 9, 2, 16, 21)) is False
    assert dispatch.check(et(2026, 9, 2, 23, 59)) is False
    assert fired == [DAY]


def test_the_dispatcher_serves_each_session_day_once():
    fired: list[date] = []
    dispatch = SessionDispatch(
        session_clock=_clock(et(2026, 9, 2, 10, 0)),
        moment=lambda bounds: bounds.option_close_deadline,
        job=fired.append,
    )
    for day in (2, 3, 4):
        dispatch.check(et(2026, 9, day, 16, 30))
    dispatch.check(et(2026, 9, 5, 16, 30))  # Saturday
    assert fired == [date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4)]


def test_a_daemon_starting_after_the_moment_still_serves_that_day():
    # This is why the job hangs off a moment rather than an equality. A restart at 16:45
    # owes the day its guard, and the guard's own window rules decide what it can do.
    fired: list[date] = []
    SessionDispatch(
        session_clock=_clock(et(2026, 9, 2, 16, 45)),
        moment=lambda bounds: bounds.option_close_deadline,
        job=fired.append,
    ).check(et(2026, 9, 2, 16, 45))
    assert fired == [DAY]


# -- the unrecoverable half ----------------------------------------------------------


def test_an_unobserved_equity_close_is_marked_and_never_fetched(tmp_path):
    fetches: list[str] = []
    guard = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=lambda ticker, slot: fetches.append(ticker),
        pid=9,
    )
    outcome = guard.run(DAY)

    assert outcome.unobserved == ("XYZ",)
    assert fetches == [], "the 16:00 moment cannot be re-observed, so nothing may fetch it"
    rows = _rows(tmp_path, "quotes", "XYZ", DAY)
    assert len(rows) == 1
    assert rows[0]["snap_ts"].startswith("2026-09-02T16:00")
    assert rows[0]["close_tag"] == SPOT_CLOSE
    assert rows[0]["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
    assert rows[0]["row_kind"] == journal.ROW_KIND_GAP


def test_an_equity_close_that_landed_is_left_alone(tmp_path):
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="data")
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        pid=9,
    ).run(DAY)
    assert outcome.unobserved == ()
    assert len(_rows(tmp_path, "quotes", "XYZ", DAY)) == 1


def test_an_equity_close_that_ran_and_failed_is_already_recorded(tmp_path):
    # A tagged gap row records the attempt. Adding a second marker for the same minute
    # would be two rows for one missed minute.
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="gap")
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        pid=9,
    ).run(DAY)
    assert outcome.unobserved == ()
    assert len(_rows(tmp_path, "quotes", "XYZ", DAY)) == 1


# -- the recoverable half ------------------------------------------------------------


def test_a_missing_option_close_is_filled_at_the_close_slot(tmp_path):
    asked: list[tuple[str, datetime]] = []

    def fill(ticker: str, slot: datetime):
        asked.append((ticker, slot))
        return ["2026-09-04"]

    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=fill,
        pid=9,
    ).run(DAY)

    assert outcome.filled == ("SPY",)
    # The fill observes the close from after it, and must be handed the close slot so
    # the rows it writes carry the close rather than the fetch minute.
    assert asked == [("SPY", et(2026, 9, 2, 16, 15))]


def test_an_option_close_that_ran_and_failed_is_still_filled(tmp_path):
    # The trigger is missing marks, not a missing cycle. A chain that failed at 16:15
    # left a tagged gap row holding nothing a reader can price against, and close+5 is
    # exactly what rescues it.
    _row(tmp_path, "chains", "SPY", et(2026, 9, 2, 16, 15), tag=OPTION_CLOSE, kind="gap")
    filled: list[str] = []
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=lambda ticker, slot: filled.append(ticker) or ["2026-09-04"],
        pid=9,
    ).run(DAY)
    assert outcome.filled == ("SPY",)
    assert filled == ["SPY"]


def test_an_option_close_that_landed_is_not_refetched(tmp_path):
    _row(tmp_path, "chains", "SPY", et(2026, 9, 2, 16, 15), tag=OPTION_CLOSE, kind="data")
    filled: list[str] = []
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=lambda ticker, slot: filled.append(ticker),
        pid=9,
    ).run(DAY)
    assert outcome.filled == ()
    assert filled == []


def test_the_fill_is_refused_outright_past_close_plus_five(tmp_path):
    filled: list[str] = []
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 21)),
        fill=lambda ticker, slot: filled.append(ticker),
        pid=9,
    ).run(DAY)
    # Past close+5 the marks are no longer the close's. The limit defines what an option
    # close means, so it is pinned in code and not in config.
    assert filled == []
    assert outcome.refused == ("SPY: past close+5",)


def test_a_fill_with_no_same_day_baseline_is_flagged_for_the_battery(tmp_path):
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=lambda ticker, slot: ["2026-09-04"],
        pid=9,
    ).run(DAY)
    assert outcome.baseline_less == ("SPY",)
    assert outcome.reportable


def test_a_vendor_failure_during_the_fill_never_stops_the_guard(tmp_path):
    def boom(ticker: str, slot: datetime):
        raise RuntimeError("vendor down")

    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((SPY, EQUITY_ONLY)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        fill=boom,
        pid=9,
    ).run(DAY)
    # The equity-only ticker's marker still lands.
    assert outcome.unobserved == ("SPY", "XYZ")
    assert outcome.problems == ("chains/SPY option_close: RuntimeError",)


# -- what the day reports ------------------------------------------------------------


def test_a_day_where_both_closes_landed_reports_nothing(tmp_path, capsys):
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="data")
    outcome = close_guard.CloseGuard(
        lake_root=tmp_path,
        roster=Roster((EQUITY_ONLY,)),
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        pid=9,
    ).run(DAY)
    assert not outcome.reportable
    daemon._report_guard(outcome)
    assert capsys.readouterr().err == ""


def test_an_unobserved_close_says_so_on_stderr(tmp_path, capsys):
    outcome = close_guard.GuardOutcome(DAY, unobserved=("XYZ",))
    daemon._report_guard(outcome)
    assert "unobserved=XYZ" in capsys.readouterr().err


# -- the fill's own slot -------------------------------------------------------------


def test_a_journalled_snapshot_can_carry_a_slot_apart_from_its_fetch_minute(tmp_path):
    from lake import capture

    slot = et(2026, 9, 2, 16, 15)
    capture.journal_snapshot(
        tmp_path,
        "quotes",
        "XYZ",
        body={"XYZ": {"quote": {}}},
        cycle_start=et(2026, 9, 2, 16, 18),
        fetch_ts=et(2026, 9, 2, 16, 18),
        fetch_end_ts=et(2026, 9, 2, 16, 18) + timedelta(seconds=1),
        slot=slot,
        close_tag=OPTION_CLOSE,
        pid=3,
    )
    rows = _rows(tmp_path, "quotes", "XYZ", DAY)
    assert rows and rows[0]["snap_ts"].startswith("2026-09-02T16:15")
    assert rows[0]["close_tag"] == OPTION_CLOSE
    # The fetch minute is still recorded, so the round trip stays measurable.
    assert rows[0]["fetch_ts"].startswith("2026-09-02T16:18")


def test_the_guard_writes_the_close_minutes_before_gap_marking_claims_them(tmp_path):
    """The two writers must not both mark 16:00 on a post-close restart.

    Startup gap marking walks the day and marks every minute with no record. The guard
    is the design's sole writer of the spot_close absent-marker. Running the guard first
    puts a row at 16:00 before the marker looks, and the marker's anchor counts marker
    rows, so it stops short rather than adding a second.
    """
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")
    # Captured to 11:00, then the daemon died. It restarts after close+5.
    _row(lake_root, "quotes", "XYZ", et(2026, 9, 2, 11, 0), tag=None, kind="data")

    clock = ManualClock(start=et(2026, 9, 2, 16, 30))
    ticks = [0]

    def once() -> bool:
        ticks[0] += 1
        return ticks[0] <= 1

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        cycle_runner=lambda *, close_tag, session_phase: CycleResult(et(2026, 9, 2, 16, 30), ()),
        should_continue=once,
    )

    rows = _rows(lake_root, "quotes", "XYZ", DAY)
    at_close = [r for r in rows if r["snap_ts"].startswith("2026-09-02T16:00")]
    assert len(at_close) == 1, "16:00 carries a marker from each writer"
    assert at_close[0]["close_tag"] == SPOT_CLOSE
    assert at_close[0]["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
    stamps = [r["snap_ts"] for r in rows]
    assert len(stamps) == len(set(stamps)), "a minute is recorded twice"
