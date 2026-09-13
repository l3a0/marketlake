"""Close tags, the session-relative dispatcher, and the close+5 guard."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, close_guard, daemon, journal
from lake.capture import CycleResult
from lake.capture_spans import CaptureSpans, spans_path
from lake.manifest import append_manifest, manifest_path, sha256_file
from lake.paths import LakePaths
from lake.security_master import SecurityMaster, master_path
from lake.session import (
    OPTION_CLOSE,
    SPOT_CLOSE,
    SessionClock,
    SessionDispatch,
)
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

_STAMP_FORMAT = "%Y%m%dT%H%M%S%f"

WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)

# A span opened at the session open covers both of the day's closes (16:00 and 16:15).
_OPEN = et(2026, 9, 2, 9, 30)


def _scope(*entries):
    """Build a master and a capture-spans set for the given entries, and return readers.

    Each entry is ``(ticker, options)``, optionally with a ``start`` and an ``end``. The
    start defaults to the session open, so the span covers both closes. An ``end`` closes
    the span, standing for a ticker retired mid-session. The guard reads which instruments
    were in scope from the spans and turns each id back into a ticker through the master.
    """
    master = SecurityMaster()
    spans = CaptureSpans()
    for entry in entries:
        ticker, options = entry[0], entry[1]
        start = entry[2] if len(entry) > 2 else _OPEN
        end = entry[3] if len(entry) > 3 else None
        iid = master.register(
            kind="equity", capture_start=start, valid_from=start.date(), ticker=ticker
        )
        spans.open_span(iid, start, options)
        if end is not None:
            spans.close_span(iid, end)
    return (lambda: spans), (lambda: master)


def _captured(*expirations: str) -> capture.FillResult:
    """A fill that landed, carrying the expirations it captured and no absent window."""
    return capture.FillResult(expirations)


def _guard(root, clock, entries, *, fill=None, pid=9):
    """A ``CloseGuard`` over an in-memory master and spans built from ``entries``."""
    spans_reader, master_reader = _scope(*entries)
    return close_guard.CloseGuard(
        lake_root=root,
        spans=spans_reader,
        session_clock=clock,
        master=master_reader,
        fill=fill,
        pid=pid,
    )


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

    # A bare daemon answers None for every slot. This checks that the production entry
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
        transport=FakeTransport(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        should_continue=three,
    )
    # 15:59, 16:00, 16:01. A bare daemon answers None for every slot, so this checks that
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
    guard = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("XYZ", False)],
        fill=lambda ticker, slot: fetches.append(ticker) or _captured("2026-09-04"),
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
    outcome = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 18)), [("XYZ", False)]).run(DAY)
    assert outcome.unobserved == ()
    assert len(_rows(tmp_path, "quotes", "XYZ", DAY)) == 1


def test_an_equity_close_that_ran_and_failed_is_already_recorded(tmp_path):
    # A tagged gap row records the attempt. Adding a second marker for the same minute
    # would be two rows for one missed minute.
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="gap")
    outcome = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 18)), [("XYZ", False)]).run(DAY)
    assert outcome.unobserved == ()
    assert len(_rows(tmp_path, "quotes", "XYZ", DAY)) == 1


# -- the recoverable half ------------------------------------------------------------


def test_a_missing_option_close_is_filled_at_the_close_slot(tmp_path):
    asked: list[tuple[str, datetime]] = []

    def fill(ticker: str, slot: datetime):
        asked.append((ticker, slot))
        return _captured("2026-09-04")

    outcome = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 18)), [("SPY", True)], fill=fill).run(DAY)

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
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("SPY", True)],
        fill=lambda ticker, slot: filled.append(ticker) or _captured("2026-09-04"),
    ).run(DAY)
    assert outcome.filled == ("SPY",)
    assert filled == ["SPY"]


def test_an_option_close_that_landed_is_not_refetched(tmp_path):
    _row(tmp_path, "chains", "SPY", et(2026, 9, 2, 16, 15), tag=OPTION_CLOSE, kind="data")
    filled: list[str] = []
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("SPY", True)],
        fill=lambda ticker, slot: filled.append(ticker) or _captured("2026-09-04"),
    ).run(DAY)
    assert outcome.filled == ()
    assert filled == []


def test_the_fill_is_refused_outright_past_close_plus_five(tmp_path):
    filled: list[str] = []
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 21)),
        [("SPY", True)],
        fill=lambda ticker, slot: filled.append(ticker) or _captured("2026-09-04"),
    ).run(DAY)
    # Past close+5 the marks are no longer the close's. The limit defines what an option
    # close means, so it is pinned in code and not in config.
    assert filled == []
    assert outcome.refused == ("SPY: past close+5",)


def test_a_fill_with_no_same_day_baseline_is_flagged_for_the_battery(tmp_path):
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("SPY", True)],
        fill=lambda ticker, slot: _captured("2026-09-04"),
    ).run(DAY)
    assert outcome.baseline_less == ("SPY",)
    assert outcome.reportable


def test_a_vendor_failure_during_the_fill_never_stops_the_guard(tmp_path):
    def boom(ticker: str, slot: datetime):
        raise RuntimeError("vendor down")

    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("SPY", True), ("XYZ", False)],
        fill=boom,
    ).run(DAY)
    # The equity-only ticker's marker still lands.
    assert outcome.unobserved == ("SPY", "XYZ")
    assert outcome.problems == ("chains/SPY option_close: RuntimeError",)


# -- what the day reports ------------------------------------------------------------


def test_a_day_where_both_closes_landed_reports_nothing(tmp_path, capsys):
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="data")
    outcome = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 18)), [("XYZ", False)]).run(DAY)
    assert not outcome.reportable
    daemon._report_guard(outcome)
    assert capsys.readouterr().err == ""


def test_an_unobserved_close_says_so_on_stderr(tmp_path, capsys):
    outcome = close_guard.GuardOutcome(DAY, unobserved=("XYZ",))
    daemon._report_guard(outcome)
    assert "unobserved=XYZ" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("field", "label"),
    [("baseline_less", "baseline-less"), ("shortfalls", "shortfall"), ("refused", "refused")],
)
def test_the_other_three_findings_reach_stderr_too(field, label, capsys):
    """Five fields are reportable and two of them have cases of their own above.

    These are the other three. Each is one line in the report's loop, and deleting any
    one of them leaves every other case here green while a whole class of finding stops
    being printed. A refusal is the one that costs most: it is how a fill that captured
    nothing is recorded at all.
    """
    outcome = close_guard.GuardOutcome(DAY, **{field: ("XYZ: reason",)})
    assert outcome.reportable, f"{field} alone did not count as worth reporting"

    daemon._report_guard(outcome)

    assert f"{label}=XYZ: reason" in capsys.readouterr().err


def test_a_field_with_nothing_in_it_is_left_out(capsys):
    """The empty ones are dropped, so the line says what happened rather than the schema.

    Without the filter every reportable day prints all five names with nothing after
    four of them, and the one finding that matters is buried in a row of empty keys.
    """
    daemon._report_guard(close_guard.GuardOutcome(DAY, unobserved=("XYZ",)))

    reported = capsys.readouterr().err
    assert "unobserved=XYZ" in reported
    assert "refused=" not in reported, "an empty field was printed anyway"
    assert "problems=" not in reported


def test_a_config_that_will_not_load_still_reaches_stderr(tmp_path, capsys):
    """The reporter's fallback, which no run through the production entry reaches.

    ``_guard_reporter`` resolves the lake root so the findings can be filed. A config it
    cannot load costs the file, and the print is the half that must survive it. Nothing
    executes this branch otherwise, which is how ``pmset_assertions_probe`` and
    ``control_plane._spawn`` both shipped with a ``return`` in front of them.
    """
    reporter = daemon._guard_reporter(
        tmp_path / "no-such-config.yaml", ManualClock(start=et(2026, 9, 2, 16, 20))
    )

    reporter(close_guard.GuardOutcome(DAY, unobserved=("XYZ",)))

    assert "unobserved=XYZ" in capsys.readouterr().err


def test_a_run_that_only_had_problems_still_says_so_on_stderr(capsys):
    """Problems are the whole operator-visible output of a run that wrote nothing.

    A prologue failure and a per-ticker failure both resolve into ``problems`` and into
    nothing else. stderr is where they land, so a day whose guard could not read the
    ledger would otherwise pass in silence while the day reads short. Both halves are
    asserted, because either one alone leaves the other free to drop the field: the
    outcome has to count as reportable, and the report has to carry the field.
    """
    outcome = close_guard.GuardOutcome(DAY, problems=("prologue: KeyError: 'partition'",))

    assert outcome.reportable, "a run that failed outright judged itself not worth reporting"
    daemon._report_guard(outcome)
    assert "problems=prologue: KeyError" in capsys.readouterr().err


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
    # XYZ is in scope for the whole session, so the guard finds it owes the close.
    master = SecurityMaster()
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 9, 2, 9, 30), False)
    spans.write(spans_path(lake_root))

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
        transport=FakeTransport(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        should_continue=once,
    )

    rows = _rows(lake_root, "quotes", "XYZ", DAY)
    at_close = [r for r in rows if r["snap_ts"].startswith("2026-09-02T16:00")]
    assert len(at_close) == 1, "16:00 carries a marker from each writer"
    assert at_close[0]["close_tag"] == SPOT_CLOSE
    assert at_close[0]["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
    stamps = [r["snap_ts"] for r in rows]
    assert len(stamps) == len(set(stamps)), "a minute is recorded twice"


# -- who owed a close ----------------------------------------------------------------


def test_a_ticker_retired_before_the_close_is_not_marked_for_one(tmp_path):
    """A span that closed before the close does not cover it, so nothing is owed.

    GONE's span ends at 15:00, before 16:00, so the guard skips it. A marker naming a
    surface no cycle writes to again is a write, not an omission. XYZ stays in scope as
    the control: without it a guard that stopped checking everything would pass.
    """
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("XYZ", False), ("GONE", False, _OPEN, et(2026, 9, 2, 15, 0))],
    ).run(DAY)

    assert outcome.unobserved == ("XYZ",)
    assert _rows(tmp_path, "quotes", "GONE", DAY) == []


def test_a_ticker_retired_after_the_equity_close_still_gets_its_marker(tmp_path):
    """#77 case 1: retiring between 16:00 and close+5 must not lose the owed marker.

    The ticker leaves the roster at 16:02, but its span still covers 16:00, so the guard
    still writes the spot_close marker. Reading the roster instead of the spans would drop
    it, and ending the span at the close would drop it too under the half-open rule.
    """
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("XYZ", False, _OPEN, et(2026, 9, 2, 16, 2))],
    ).run(DAY)

    assert outcome.unobserved == ("XYZ",)
    rows = _rows(tmp_path, "quotes", "XYZ", DAY)
    assert len(rows) == 1
    assert rows[0]["close_tag"] == SPOT_CLOSE
    assert rows[0]["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED


def test_a_ticker_onboarded_before_the_close_is_checked_that_same_session(tmp_path):
    """A ticker with a span covering the close is checked, however recently it opened."""
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("XYZ", False), ("NEW", False)],
    ).run(DAY)

    assert outcome.unobserved == ("XYZ", "NEW")


def test_a_close_before_a_ticker_came_into_scope_is_not_marked_missing(tmp_path):
    """The front edge of scope. A span that opens after the close does not cover it.

    LATE onboards at 17:00, so its span opens after this session's closes, and the guard
    does not mark a close from before the ticker existed.
    """
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 18, 0)),
        [("LATE", False, et(2026, 9, 2, 17, 0))],
    ).run(DAY)

    assert outcome.unobserved == ()
    assert _rows(tmp_path, "quotes", "LATE", DAY) == []


def test_each_close_is_clamped_to_its_own_moment(tmp_path):
    """A ticker onboarded between the two closes owes the later one and not the earlier.

    A span opening at 16:05 covers the 16:15 option close but not the 16:00 equity close.
    """
    fetches: list[str] = []
    outcome = _guard(
        tmp_path,
        _clock(et(2026, 9, 2, 16, 18)),
        [("SPY", True, et(2026, 9, 2, 16, 5))],
        fill=lambda ticker, slot: fetches.append(ticker) or _captured("2026-09-18"),
    ).run(DAY)

    assert outcome.unobserved == (), "16:00 was before SPY came into scope"
    assert fetches == ["SPY"], "16:15 was after it, so the option close is still owed"


def test_a_missing_spans_file_leaves_the_guard_checking_nothing(tmp_path):
    """No spans means no instrument was in scope, so the guard writes no false marker.

    A missing source is the safe direction. It records nothing rather than a close for a
    ticker it cannot place. This is the mirror of the old no-clamp rule, now that the
    spans are the guard's source of who owed a close.
    """
    _, master = _scope(("XYZ", False))
    guard = close_guard.CloseGuard(
        lake_root=tmp_path,
        spans=lambda: None,
        session_clock=_clock(et(2026, 9, 2, 16, 18)),
        master=master,
    )
    assert guard.run(DAY).unobserved == ()
    assert _rows(tmp_path, "quotes", "XYZ", DAY) == []


def test_the_spans_are_read_when_the_guard_runs_not_at_daemon_start(tmp_path):
    """A span written after the guard is built is still seen, because the reader is live.

    Onboarding and retiring write the spans while the daemon runs. XYZ is in scope and
    owes its close. LATE gets a span opening at 17:00, after this session's closes, added
    after the guard was built, so it owes nothing.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    master = SecurityMaster()
    xyz = master.register(
        kind="equity",
        capture_start=et(2026, 8, 31, 9, 30),
        valid_from=date(2026, 8, 31),
        ticker="XYZ",
    )
    master.write(master_path(lake_root))
    spans = CaptureSpans()
    spans.open_span(xyz, et(2026, 8, 31, 9, 30), False)
    spans.write(spans_path(lake_root))

    guard = close_guard.CloseGuard(
        lake_root=lake_root,
        spans=daemon._spans_reader(lake_root),
        session_clock=_clock(et(2026, 9, 2, 18, 0)),
        master=daemon._master_reader(lake_root),
    )

    # LATE is onboarded at 17:00, after the guard was built, with a span past the closes.
    later_master = SecurityMaster.read(master_path(lake_root))
    late = later_master.register(
        kind="equity", capture_start=et(2026, 9, 2, 17, 0), valid_from=DAY, ticker="LATE"
    )
    later_master.write(master_path(lake_root))
    later_spans = CaptureSpans.read(spans_path(lake_root))
    later_spans.open_span(late, et(2026, 9, 2, 17, 0), False)
    later_spans.write(spans_path(lake_root))

    outcome = guard.run(DAY)
    assert outcome.unobserved == ("XYZ",), "XYZ owed a close and LATE did not"
    assert _rows(lake_root, "quotes", "LATE", DAY) == []


def test_the_daemon_gives_the_guard_live_spans_and_the_master(tmp_path):
    """The wiring, not the guard in isolation.

    Freezing the spans or dropping the master in ``_close_guard`` would leave the guard
    checking the wrong thing, which a review found the suite did not catch. The spans file
    is written on the first cycle, after the guard is built, so a reader captured at build
    time would find no spans and mark nothing. The clock starts at 16:14:30, so the sixth
    tick is 16:20, close+5, the moment the dispatch fires.

    Three tickers, one for each outcome:

    1. XYZ has a span covering the close, so it owes a marker. It is the control, since a
       guard that checked nothing would pass the other two assertions.
    2. GONE's span closed at 15:00, before the close, so it owes nothing.
    3. LATE's span opens at 17:00, after this session's close, so it owes nothing.
    """
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")

    master = SecurityMaster()
    ids = {
        ticker: master.register(
            kind="equity", capture_start=started, valid_from=date(2026, 8, 31), ticker=ticker
        )
        for ticker, started in (
            ("XYZ", et(2026, 8, 31, 9, 30)),
            ("GONE", et(2026, 8, 31, 9, 30)),
            ("LATE", et(2026, 9, 2, 17, 0)),
        )
    }
    master.write(master_path(lake_root))

    clock = ManualClock(start=et(2026, 9, 2, 16, 14, 30))
    cycles = [0]

    def cycle(*, close_tag, session_phase):
        cycles[0] += 1
        if cycles[0] == 1:
            # Onboarding and retiring write the spans while the daemon runs. Writing them
            # here, after the guard is built, is what a build-time reader would miss.
            spans = CaptureSpans()
            spans.open_span(ids["XYZ"], et(2026, 8, 31, 9, 30), False)
            spans.open_span(ids["GONE"], et(2026, 8, 31, 9, 30), False)
            spans.close_span(ids["GONE"], et(2026, 9, 2, 15, 0))
            spans.open_span(ids["LATE"], et(2026, 9, 2, 17, 0), False)
            spans.write(spans_path(lake_root))
        return CycleResult(clock.now(), ())

    ticks = [0]

    def six() -> bool:
        ticks[0] += 1
        return ticks[0] <= 6

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        cycle_runner=cycle,
        transport=FakeTransport(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        should_continue=six,
    )

    def marked(ticker: str) -> list[str]:
        return [
            r["snap_ts"]
            for r in _rows(lake_root, "quotes", ticker, DAY)
            if r["error_class"] == close_guard.SPOT_CLOSE_UNOBSERVED
        ]

    assert marked("XYZ"), "the guard never ran, so the rest proves nothing"
    assert marked("GONE") == [], "its span closed before the close, so it owed nothing"
    assert marked("LATE") == [], "its span opens after this session's close"


# -- a day compaction has already sealed -----------------------------------------------


def _seal(root: Path, ticker: str, day: date) -> None:
    """Stand in for compaction: manifest the ticker-day's partition, drop its segments.

    Only the two facts the guard can see are reproduced, because those are the two that
    decide it: the manifest holds an entry for the partition, and the segment directory
    is empty. How compaction gets there is its own module's contract.
    """
    paths = LakePaths(root)
    partition = paths.quotes_partition_path(ticker, day)
    partition.parent.mkdir(parents=True, exist_ok=True)
    partition.write_bytes(b"sealed")
    append_manifest(
        root,
        partition=partition.relative_to(root).as_posix(),
        source="compaction",
        sha256=sha256_file(partition),
        rows=406,
        fetched_at=None,
    )
    directory = paths.segment_dir(journal.QUOTES_SURFACE, ticker, day)
    for segment in directory.glob("*.arrows"):
        segment.unlink()


def test_a_sealed_day_is_left_alone_rather_than_marked_unobserved(tmp_path):
    """A restart after the daemon sealed its own day must not claim the close went unseen.

    Compaction unlinks a ticker-day's segments once its partition is manifested, and
    ``close_tag_rows`` reads only that directory. So a guard run after the seal reads an
    empty directory and concludes nobody observed the close, for a close that was captured
    and is sitting in the partition.

    Nothing stopped that before the daemon dispatched its own compaction, because the
    seal and the daemon's life never overlapped. Now they do: seal at 16:31, die at 16:33,
    restart at 16:34, and the guard's dispatcher has forgotten it already ran. The marker
    it writes is a false claim, and the next run deletes it as debris, so a row a live
    writer wrote is silently dropped. Gap-marking skips a sealed date for this reason and
    the guard now does too.
    """
    # Captured at the equity close, the way a healthy session ends.
    _row(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 16, 0), tag=SPOT_CLOSE, kind="data")
    _seal(tmp_path, "XYZ", DAY)

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 34)), [("XYZ", False)])
    outcome = guard.run(DAY)

    assert outcome.unobserved == (), "the guard called a sealed day's close unobserved"
    assert not _rows(tmp_path, "quotes", "XYZ", DAY), "a marker landed beside a sealed day"


# -- a failure that must not take the daemon down ---------------------------------------


def _drifted_segment(
    root: Path, surface: str, ticker: str, day: date, *, at: datetime | None = None
) -> None:
    """A segment that reads cleanly and holds none of the columns the guard asks for.

    This is the shape a schema change leaves behind: the file is valid Arrow IPC, so
    ``read_segment`` returns a table and the guard's own read catches nothing, and then
    asking for ``close_tag`` raises ``KeyError``. Writing it by hand rather than through
    ``SegmentWriter`` is the point, because the writer can only produce the current
    schema.
    """
    import pyarrow as pa

    directory = LakePaths(root).segment_dir(surface, ticker, day)
    directory.mkdir(parents=True, exist_ok=True)
    slot = at if at is not None else datetime.combine(day, datetime.min.time()).replace(hour=16)
    schema = pa.schema([("snap_ts", pa.string())])
    name = f"{slot.strftime(_STAMP_FORMAT)}-1.arrows"
    with pa.ipc.new_stream(directory / name, schema) as writer:
        writer.write_batch(pa.record_batch([pa.array([slot.isoformat()])], schema=schema))


def test_an_unreadable_manifest_stops_the_run_and_writes_nothing(tmp_path):
    """The ledger decides which days are sealed, so losing it must not widen the run.

    ``latest_entries`` raises ``KeyError`` on a manifest line that is valid JSON and
    carries no ``partition`` key, and ``OSError`` on a read that fails. Either way the
    guard can no longer tell a sealed ticker-day from an unsealed one.

    Treating that as "nothing is sealed" is the tempting repair and the wrong one. It
    would let this writer mark a close as unobserved on a day compaction already sealed,
    which is a false claim the next run deletes as debris, dropping a row a live writer
    wrote. So the run stops, says what broke, and leaves the day alone.
    """
    # Nothing captured the close, so a guard with a readable ledger would mark it. The
    # unreadable ledger is the only reason this run writes nothing.
    manifest_path(tmp_path).write_text('{"source": "compaction", "rows": 406}\n')

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 20)), [("XYZ", False)])
    outcome = guard.run(DAY)

    assert len(outcome.problems) == 1, outcome.problems
    assert outcome.problems[0].startswith("prologue: ManifestError"), (
        "the run did not say what broke"
    )
    assert "entry 1" in outcome.problems[0], "the report located no line, so it diagnoses nothing"
    assert outcome.unobserved == (), "the guard claimed a close went unseen without the ledger"
    assert _rows(tmp_path, "quotes", "XYZ", DAY) == [], "a marker landed on an unreadable ledger"


class _DriftedSpans:
    """A spans file that loaded and answers nothing the guard can use.

    The readers in ``daemon`` catch ``(OSError, CaptureSpansError, ValueError)`` around
    the load, so a file that parses and then misbehaves reaches the guard intact.
    """

    def spans_covering(self, instant):
        raise RuntimeError("drifted spans")


def test_a_scope_read_that_fails_is_a_prologue_failure_too(tmp_path):
    """The prologue answers two questions, so both of its reads need covering.

    Leaving only the ledger read inside the try is the natural refactor for anyone who
    reads the handler as "the ledger failed", and it passes every other test here. The
    scope read is the half that says who owed a close at all, and losing it the same way
    has to resolve the same way.
    """
    guard = close_guard.CloseGuard(
        lake_root=tmp_path,
        spans=_DriftedSpans,
        session_clock=_clock(et(2026, 9, 2, 16, 20)),
        master=SecurityMaster,
    )
    outcome = guard.run(DAY)

    assert len(outcome.problems) == 1, outcome.problems
    assert outcome.problems[0].startswith("prologue: RuntimeError"), outcome.problems
    assert outcome.unobserved == (), "a guard that cannot read scope still claimed a close"


def test_a_drifted_segment_is_reported_rather_than_marked_over(tmp_path):
    """A file that will not read is not an absent close, and the marker is a claim.

    The marker says a named ticker owed a close and nothing observed it. A drifted
    segment may hold the very row that refutes that, so counting it as zero rows would
    have the guard make a false claim, which the next run then deletes as debris and
    takes a live writer's row with it. Declining and saying so is the honest answer.

    The blast radius still has to stop at the one ticker: DRIFT is read first, and OK must
    still get the marker it is owed.
    """
    _drifted_segment(tmp_path, "quotes", "DRIFT", DAY)
    # Two, so the count in the message is a count rather than a constant that happens to
    # read right when every fixture plants exactly one bad file.
    _drifted_segment(tmp_path, "quotes", "DRIFT", DAY, at=et(2026, 9, 2, 16, 1))

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 20)), [("DRIFT", False), ("OK", False)])
    outcome = guard.run(DAY)

    assert outcome.problems == ("quotes/DRIFT: 2 unreadable (2 drifted)",), (
        "the bad ticker was not named"
    )
    assert outcome.unobserved == ("OK",), "the guard claimed a close it could not see"
    # The drifted segment itself is one row in that directory, so what must be absent is a
    # marker, not a row. A marker carries the close tag; the drifted schema has no such
    # column at all, which is why this reads with ``get``.
    planted = [row for row in _rows(tmp_path, "quotes", "DRIFT", DAY) if row.get("close_tag")]
    assert not planted, "a false marker landed beside a bad file"
    marked = [row for row in _rows(tmp_path, "quotes", "OK", DAY) if row["close_tag"]]
    assert marked, "the healthy ticker lost its marker to another ticker's bad file"


def test_a_raise_inside_one_tickers_check_does_not_cost_the_others_their_markers(tmp_path):
    """The per-ticker catch still holds, now that no ordinary file reaches it.

    #103 added this catch when a drifted segment was the way to reach it. The readers now
    resolve that case into ``unreadable`` instead, which is better and leaves this catch
    with no reachable trigger of its own. It stays, because the next reader to grow a new
    raise path would otherwise cost every later ticker its marker, so the raise is
    injected rather than provoked.
    """
    real = journal.close_tag_rows

    def boom(root, surface, ticker, day, close_tag):
        if ticker == "BOOM":
            raise RuntimeError("a reader grew a new raise path")
        return real(root, surface, ticker, day, close_tag)

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 20)), [("BOOM", False), ("OK", False)])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(journal, "close_tag_rows", boom)
        outcome = guard.run(DAY)

    assert outcome.problems == ("quotes/BOOM: RuntimeError: a reader grew a new raise path",)
    assert outcome.unobserved == ("OK",), "the run stopped at the first raise"
    marked = [row for row in _rows(tmp_path, "quotes", "OK", DAY) if row["close_tag"]]
    assert marked, "the healthy ticker lost its marker to another ticker's raise"


def test_the_option_close_loop_survives_a_bad_file_the_same_way(tmp_path):
    """The chains half owes the same guarantee as the quotes half, and symmetry is not proof.

    The two loops are written alike and fail alike, which is exactly why each needs its
    own case. Deleting the catch on this loop alone leaves the whole suite green when only
    the quotes side is covered, so the chains side would ship its crash path intact.

    With no fill fetcher wired, a healthy ticker's option close records a refusal. That
    refusal is what shows the run reached OK at all after DRIFT raised.
    """
    _drifted_segment(tmp_path, "chains", "DRIFT", DAY)

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 20)), [("DRIFT", True), ("OK", True)])
    outcome = guard.run(DAY)

    assert outcome.problems == ("chains/DRIFT: 1 unreadable (1 drifted)",), (
        "the bad ticker was not named"
    )
    # Both are refused for want of a fetcher, DRIFT included. That it reaches the fill
    # path at all is the point: an unreadable segment names a problem here and does not
    # call the close off, because the fill is the only thing that can still rescue it.
    assert outcome.refused == ("DRIFT: no fill fetcher", "OK: no fill fetcher"), outcome.refused
    assert set(outcome.unobserved) == {"DRIFT", "OK"}, "the quotes half was collateral damage"


def test_an_empty_segment_reads_as_absent_rather_than_unreadable(tmp_path):
    """A file created and never written to holds nothing, so it hides nothing.

    ``SegmentWriter`` opens with ``O_CREAT|O_EXCL`` and fsyncs the directory entry before
    any schema bytes land, so a process killed in between leaves a durably zero-byte
    segment, and a ``KeepAlive`` crash loop makes them in quantity. That makes this the
    likeliest bad file in the lake rather than an exotic one.

    Counting it unreadable would have withheld the option close's refetch, and the window
    shuts at close+5 with compaction sealing the day ten minutes later. Absent is not a
    softer reading of the same thing, it is the accurate one: the file holds no batches,
    so nothing in it can contradict what the fill writes.
    """
    directory = LakePaths(tmp_path).segment_dir(journal.CHAINS_SURFACE, "SPY", DAY)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "20260902T161500000000-1.arrows").write_bytes(b"")

    asked: list[str] = []

    def fill(ticker: str, slot: datetime) -> capture.FillResult:
        asked.append(ticker)
        return _captured("2026-09-18")

    guard = _guard(tmp_path, _clock(et(2026, 9, 2, 16, 20)), [("SPY", True)], fill=fill)
    outcome = guard.run(DAY)

    assert asked == ["SPY"], "an empty file called off the only thing that could rescue the close"
    assert outcome.filled == ("SPY",), outcome
    assert outcome.problems == (), "an empty file was reported as damage rather than absence"
