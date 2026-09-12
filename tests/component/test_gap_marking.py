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
from lake.capture_spans import CaptureSpans
from lake.config import GuardConstants
from lake.security_master import SecurityMaster
from lake.session import SessionClock, session_slots
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

# A capture-span start well before any test slot, so every test day is in scope. The
# hole-aware walk owes a minute only when a span covers it, so a test that wants a ticker
# marked has to put it in scope first, the same as the daemon does through onboarding.
_SCOPE_START = et(2026, 8, 24, 9, 30)


def _spans_for(
    roster: Roster, start: datetime = _SCOPE_START
) -> tuple[SecurityMaster, CaptureSpans]:
    """A master and open capture spans that put every roster ticker in scope from ``start``."""
    master = SecurityMaster()
    spans = CaptureSpans()
    for entry in roster:
        iid = master.register(
            kind="equity", capture_start=start, valid_from=start.date(), ticker=entry.ticker
        )
        spans.open_span(iid, start, entry.options)
    return master, spans


def _marker(
    root: Path,
    at: datetime,
    *,
    roster: Roster,
    pid: int = 4242,
    holidays=(),
    scope_start: datetime = _SCOPE_START,
) -> gap.GapMarker:
    calendar = weekday_sessions(WEEK, holidays=holidays)
    clock = ManualClock(start=at)
    master, spans = _spans_for(roster, scope_start)
    return gap.GapMarker(
        lake_root=root,
        roster=lambda: roster,
        session_clock=SessionClock(clock=clock, calendar=calendar),
        master=lambda: master,
        spans=lambda: spans,
        pid=pid,
    )


def _record(root: Path, surface: str, ticker: str, slot: datetime, *, kind: str = "x") -> None:
    """Put one recorded row on disk, standing for a captured cycle."""
    batch = journal.gap_rows(surface, ticker=ticker, slots=[slot], error_class=kind)
    stamp = slot.strftime(gap.SEGMENT_STAMP_FORMAT)
    with journal.SegmentWriter.open(root, surface, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _capture(
    root: Path,
    ticker: str,
    day: date,
    *,
    surfaces: tuple[str, ...] = ("quotes",),
    through: datetime | None = None,
    holidays=(),
) -> None:
    """Record a captured row for every session slot of ``day`` through ``through``.

    A single seeded row used to stand for "captured up to here", which the old
    newest-row anchor trusted. The hole-aware walk checks every owed minute, so a
    captured range must actually be recorded minute by minute. ``through`` unset captures
    the whole session, which is what a complete floor day the walk stops at needs.
    """
    bounds = SessionClock(
        clock=ManualClock(start=et(2026, 9, 2, 10, 0)),
        calendar=weekday_sessions(WEEK, holidays=holidays),
    ).bounds(day)
    slots = [s for s in session_slots(bounds) if through is None or s <= through]
    stamp = slots[0].strftime(gap.SEGMENT_STAMP_FORMAT)
    for surface in surfaces:
        batch = journal.gap_rows(surface, ticker=ticker, slots=slots, error_class="x")
        with journal.SegmentWriter.open(root, surface, ticker, day, stamp, 1) as writer:
            writer.write_cycle(batch)


def _slots(root: Path, surface: str, ticker: str, day: date) -> list[str]:
    directory = journal.segment_dir(root, surface, ticker, day)
    if not directory.is_dir():
        return []
    found: list[str] = []
    for path in sorted(directory.glob("*.arrows")):
        found += journal.read_segment(path).column("snap_ts").to_pylist()
    return found


_MARKER_REASONS = {gap.DAEMON_DEAD, gap.SLOT_OVERRUN}


def _gap_snaps(root: Path, surface: str, ticker: str, day: date) -> list[str]:
    """The snap_ts of the gap-marker rows for a ticker-day, apart from captured rows.

    A captured row is seeded with ``error_class`` ``x``; a marker carries ``daemon_dead``
    or ``slot_overrun``. Filtering by reason lets a test that seeds a full captured range
    still assert exactly which minutes the walk marked.
    """
    directory = journal.segment_dir(root, surface, ticker, day)
    if not directory.is_dir():
        return []
    out: list[str] = []
    for path in sorted(directory.glob("*.arrows")):
        for row in journal.read_segment(path).to_pylist():
            if row["error_class"] in _MARKER_REASONS:
                out.append(row["snap_ts"])
    return out


# -- rule 6: what each kind of missed date gets --------------------------------------


def test_a_partially_captured_date_is_marked_from_its_last_row_to_its_option_close(tmp_path):
    # Monday fully captured is the floor the walk stops at. Tuesday captured to 11:00.
    _capture(tmp_path, "SPY", date(2026, 8, 31), surfaces=("chains", "quotes"))
    _capture(
        tmp_path,
        "SPY",
        date(2026, 9, 1),
        surfaces=("chains", "quotes"),
        through=et(2026, 9, 1, 11, 0),
    )
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((SPY,))).on_start()

    tuesday = [s for s in report.spans if s.day == date(2026, 9, 1) and s.surface == "quotes"]
    assert len(tuesday) == 1
    # 11:01 through 16:15 inclusive.
    assert tuesday[0].slots == 315
    marked = sorted(_gap_snaps(tmp_path, "quotes", "SPY", date(2026, 9, 1)))
    assert marked[0].startswith("2026-09-01T11:01")
    assert marked[-1].startswith("2026-09-01T16:15")


def test_a_fully_dark_date_is_marked_for_its_whole_session(tmp_path):
    # Monday fully captured on both surfaces is the floor. Tuesday is fully dark.
    _capture(tmp_path, "SPY", date(2026, 8, 31), surfaces=("chains", "quotes"))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((SPY,))).on_start()

    tuesday_dark = [s for s in report.spans if s.day == date(2026, 9, 1)]
    assert [s.slots for s in tuesday_dark] == [FULL_SESSION_SLOTS] * 2


def test_a_holiday_inside_the_dark_stretch_is_marked_not_at_all(tmp_path):
    _capture(tmp_path, "XYZ", date(2026, 8, 31), holidays=(date(2026, 9, 2),))
    marker = _marker(
        tmp_path,
        et(2026, 9, 4, 10, 0),
        roster=Roster((EQUITY_ONLY,)),
        holidays=(date(2026, 9, 2),),
    )
    days = {span.day for span in marker.on_start().spans}
    assert date(2026, 9, 2) not in days
    assert {date(2026, 9, 1), date(2026, 9, 3), date(2026, 9, 4)} <= days


# -- the hole-aware property: a stray row cannot hide the minutes below it -----------


def test_a_dark_day_with_only_a_late_row_is_still_marked_back_to_its_open(tmp_path):
    """The #76 collapse. A stray row near the close must not hide the morning below it.

    Tuesday is dark apart from one 16:00 row, so it owes every other capture minute. The
    hole-aware walk marks them. The old newest-minute anchor marked only forward from the
    stray row, so the morning read as complete and the owed minutes below it went unmarked.
    A per-day anchor-forward walk fails this test, which is the point: it pins the fix
    rather than the old collapse.
    """
    # Monday fully captured is the floor the walk stops at.
    _capture(tmp_path, "XYZ", date(2026, 8, 31))
    # Tuesday's only row is a late one, the shape that fooled the old anchor.
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 16, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    tuesday = [s for s in report.spans if s.day == date(2026, 9, 1) and s.surface == "quotes"]
    assert len(tuesday) == 1
    # 406 owed minus the single recorded 16:00 = 405 marked, the morning included.
    assert tuesday[0].slots == 405
    marked = sorted(_gap_snaps(tmp_path, "quotes", "XYZ", date(2026, 9, 1)))
    assert marked[0].startswith("2026-09-01T09:30")
    assert marked[-1].startswith("2026-09-01T16:15")
    # The one recorded minute is not re-marked.
    assert not any(slot.startswith("2026-09-01T16:00:") for slot in marked)


def test_the_walk_stops_at_the_first_fully_captured_prior_day(tmp_path):
    """A healthy restart stops at the first complete prior day rather than walking the cap.

    Tuesday is fully captured, so everything below it is accounted and the walk stops there.
    Removing that early return sends the walk back to the 90-session cap, marking a dark
    Monday that sits below the complete Tuesday and reporting a truncation that never
    happened. This pins the stop, so that regression cannot ship green.
    """
    # Tuesday fully captured is the floor. Monday below it is dark but must be left alone.
    _capture(tmp_path, "XYZ", date(2026, 9, 1))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    assert not report.truncated, "a healthy restart reported a truncation"
    marked_days = {span.day for span in report.spans}
    assert date(2026, 8, 31) not in marked_days, "walked past the first complete prior day"
    # Only today's pre-open dark stretch is marked.
    assert marked_days <= {date(2026, 9, 2)}


# -- rule 7: today's markers stop at the first live slot -----------------------------


def test_todays_markers_stop_at_the_first_slot_the_loop_will_capture(tmp_path):
    # Tuesday fully captured is the floor. Today is dark up to the start minute.
    _capture(tmp_path, "XYZ", date(2026, 9, 1))
    report = _marker(tmp_path, et(2026, 9, 2, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    today = [s for s in report.spans if s.day == date(2026, 9, 2)]
    # 09:30 through 10:00 inclusive. The daemon's own start minute is marked, because
    # the loop sleeps to the next top and will never run a cycle for it.
    assert [s.slots for s in today] == [31]
    marked = sorted(_gap_snaps(tmp_path, "quotes", "XYZ", date(2026, 9, 2)))
    assert marked[-1].startswith("2026-09-02T10:00")


def test_a_post_close_restart_clips_todays_markers_at_the_option_close(tmp_path):
    # Monday fully captured is the floor. Today captured to 11:00, then dark, post-close.
    _capture(tmp_path, "XYZ", date(2026, 9, 1))
    _capture(tmp_path, "XYZ", date(2026, 9, 2), through=et(2026, 9, 2, 11, 0))
    report = _marker(tmp_path, et(2026, 9, 2, 23, 30), roster=Roster((EQUITY_ONLY,))).on_start()

    today = [s for s in report.spans if s.day == date(2026, 9, 2)]
    assert [s.slots for s in today] == [315]
    assert sorted(_gap_snaps(tmp_path, "quotes", "XYZ", date(2026, 9, 2)))[-1].startswith(
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


def test_the_walk_continues_past_a_sealed_date_to_mark_a_dark_day_below_it(tmp_path, monkeypatch):
    """A sealed date is skipped, and the walk keeps going past it.

    Stopping at a sealed date is the rejected last-manifested-partition anchor: a dark date
    sitting below it would never be marked, which is the case gap-marking exists for. Here
    Wednesday is sealed and Tuesday below it is dark, so Tuesday must still be marked. A
    walk that stops at the sealed date fails this test.
    """
    from lake import manifest

    # Monday fully captured is the floor. Tuesday is dark. Wednesday is sealed above it.
    _capture(tmp_path, "XYZ", date(2026, 8, 31))
    sealed_key = "quotes/ticker=XYZ/date=2026-09-02.parquet"
    monkeypatch.setattr(manifest, "latest_entries", lambda root: {sealed_key: {"row_count": 1}})
    monkeypatch.setattr(gap, "latest_entries", lambda root: {sealed_key: {"row_count": 1}})

    report = _marker(tmp_path, et(2026, 9, 3, 10, 0), roster=Roster((EQUITY_ONLY,))).on_start()

    assert sealed_key in report.sealed
    tuesday = [s for s in report.spans if s.day == date(2026, 9, 1) and s.surface == "quotes"]
    assert [s.slots for s in tuesday] == [FULL_SESSION_SLOTS], (
        "the dark Tuesday below the sealed Wednesday was not marked"
    )


# -- rule 2: what a marker row holds -------------------------------------------------


def test_a_marker_row_holds_no_market_data_and_names_its_reason(tmp_path):
    _capture(tmp_path, "XYZ", date(2026, 9, 1))  # Tuesday full is the floor the walk stops at
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 9, 59))
    _marker(tmp_path, et(2026, 9, 2, 10, 5), roster=Roster((EQUITY_ONLY,))).on_start()

    directory = journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2))
    rows = [
        row
        for path in sorted(directory.glob("*.arrows"))
        for row in journal.read_segment(path).to_pylist()
        if row["error_class"] == gap.DAEMON_DEAD
    ]
    assert rows, "the marking pass wrote no marker rows"
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


def test_a_ticker_with_no_capture_span_is_out_of_scope_and_not_marked(tmp_path):
    # A ticker the spans file cannot place is owed nothing, so a first-ever start marks
    # nothing rather than inventing ninety days of absence. No spans reader is wired, so
    # scope cannot be read at all, the widen-to-nothing case.
    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        pid=7,
    )
    report = marker.on_start()
    assert report.spans == ()
    assert not list(tmp_path.rglob("*.arrows"))


def test_a_marking_pass_with_no_missed_minutes_opens_no_segment(tmp_path):
    # In scope only from today's open, and today captured whole through the start minute,
    # so nothing is owed anywhere and the walk opens no marker segment.
    _capture(tmp_path, "XYZ", date(2026, 9, 2), through=et(2026, 9, 2, 10, 0))
    report = _marker(
        tmp_path,
        et(2026, 9, 2, 10, 0),
        roster=Roster((EQUITY_ONLY,)),
        scope_start=et(2026, 9, 2, 9, 30),
    ).on_start()
    assert report.spans == ()
    assert (
        len(list(journal.segment_dir(tmp_path, "quotes", "XYZ", date(2026, 9, 2)).glob("*.arrows")))
        == 1
    )


# -- the walk-back cap ---------------------------------------------------------------


def test_a_walk_that_reaches_the_cap_is_reported_rather_than_silent(tmp_path, monkeypatch):
    # In scope since 2020 with nothing recorded, so every session back to the cap is owed
    # and dark. The walk never finds a complete day and stops at the cap.
    master, _unused = _spans_for(Roster((EQUITY_ONLY,)), et(2020, 1, 2, 9, 30))
    spans = CaptureSpans()
    spans.open_span(master.resolve("XYZ", date(2020, 1, 2)), et(2020, 1, 2, 9, 30), False)

    monkeypatch.setattr(gap, "MAX_LOOKBACK_SESSIONS", 3)
    calendar = weekday_sessions(WEEK)
    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=calendar),
        master=lambda: master,
        spans=lambda: spans,
        pid=7,
    )
    report = marker.on_start()
    assert report.truncated == ("quotes/XYZ",)


def test_a_master_that_raises_leaves_the_walk_marking_nothing_not_crashing(tmp_path):
    from lake.security_master import UnknownInstrument

    class Master:
        def resolve(self, symbol, on, id_type=None):
            raise UnknownInstrument(9)

    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 1, 11, 0))
    calendar = weekday_sessions(WEEK)
    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=calendar),
        master=lambda: Master(),
        spans=lambda: CaptureSpans(),
        pid=7,
    )
    # The daemon runs under KeepAlive and run_loop does not guard on_start, so a raise
    # here would be a crash loop. The reader swallows the master's error and returns no
    # scope, so the walk marks nothing this pass and the next readable restart retries.
    report = marker.on_start()
    assert report.rows == 0


@pytest.mark.parametrize("kind", [journal.ROW_KIND_DATA, journal.ROW_KIND_GAP])
def test_recorded_slots_counts_rows_of_every_kind(tmp_path, kind):
    slot = et(2026, 9, 2, 11, 0)
    schema = journal.schema_for("quotes")
    batch = journal._batch(
        schema,
        [{"snap_ts": slot.isoformat(), "ticker": "XYZ", "row_kind": kind, "schema_version": 1}],
    )
    with journal.SegmentWriter.open(tmp_path, "quotes", "XYZ", slot.date(), "s", 1) as writer:
        writer.write_cycle(batch)
    assert journal.recorded_slots(tmp_path, "quotes", "XYZ", slot.date()).slots == {slot}


def test_a_stall_that_outlives_an_onboarding_marks_nothing_before_capture_start(tmp_path):
    """A skipped-slot pass clamps to ``capture_start``, the way the startup walk does.

    The loop sleeps from 10:00 to 10:10 and NEW joins the roster at 10:05, so this pass
    is the first to see it. Its 10:01 was never owed. The design renders that ticker as
    "onboarded 10:05", never as minutes missing, and the roster read is what makes the
    case reachable at all: a roster frozen at daemon start could not name a ticker
    onboarded after it.
    """

    class Master:
        def resolve(self, symbol, on, id_type=None):
            return 1

        def capture_start_of(self, instrument_id):
            return et(2026, 9, 2, 10, 5)

    clock = ManualClock(start=et(2026, 9, 2, 10, 10))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((TickerConfig(ticker="NEW", options=False),)),
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        master=lambda: Master(),
        pid=3,
    )
    marker.on_skipped([et(2026, 9, 2, 10, m) for m in range(1, 10)])
    marked = [slot[11:16] for slot in _slots(tmp_path, "quotes", "NEW", date(2026, 9, 2))]
    assert marked == ["10:05", "10:06", "10:07", "10:08", "10:09"]


def test_a_pass_reads_the_roster_once_however_many_surfaces_it_marks(tmp_path):
    """One read per pass, not one per ticker or per surface.

    ``_pass`` says so, and the reason is that every surface in a pass has to be judged
    against one statement of what is in scope. A read per pair would let one pass mark
    SPY against a roster QQQ was never checked against.
    """
    calls = [0]
    roster = Roster((SPY, EQUITY_ONLY))

    def read() -> Roster:
        calls[0] += 1
        return roster

    clock = ManualClock(start=et(2026, 9, 2, 10, 4))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=read,
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        pid=5,
    )
    report = marker.on_skipped([et(2026, 9, 2, 10, m) for m in (1, 2, 3)])
    # SPY carries chains and quotes, XYZ quotes alone, so the pass covered three pairs.
    assert len({(span.surface, span.ticker) for span in report.spans}) == 3
    assert calls[0] == 1


def test_the_startup_pass_reads_the_roster_too(tmp_path):
    """``on_start`` reads when it runs, rather than marking a roster handed in earlier.

    The daemon loads the roster to decide whether marking is wired at all, and the
    close+5 guard's dispatch runs between that load and this pass. A ticker onboarded
    inside that window is captured from the first cycle, so the pass has to see it.
    """
    both = Roster((EQUITY_ONLY, TickerConfig(ticker="LATE", options=False)))
    master, spans = _spans_for(both)  # both in scope; the walk owes each a dark today
    live = [Roster((EQUITY_ONLY,))]

    clock = ManualClock(start=et(2026, 9, 2, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: live[0],
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        master=lambda: master,
        spans=lambda: spans,
        pid=6,
    )
    # LATE joins after the marker was built but before the pass runs.
    live[0] = both
    marked = {span.ticker for span in marker.on_start().spans}
    assert marked == {"XYZ", "LATE"}


def test_the_master_is_read_when_a_pass_runs_not_held_from_daemon_start(tmp_path):
    """The clamp exists for a mid-session onboarding, so it has to be able to see one.

    Onboarding writes the security master while the daemon runs. A copy held from daemon
    start cannot place a ticker registered after it, so the clamp finds no epoch and does
    nothing for exactly the case ``on_skipped`` describes: a stall that outlives an
    onboarding.
    """
    from lake import daemon
    from lake.security_master import SecurityMaster, master_path

    master = SecurityMaster()
    master.register(
        kind="equity",
        capture_start=et(2026, 8, 31, 9, 30),
        valid_from=date(2026, 8, 31),
        ticker="XYZ",
    )
    master.write(master_path(tmp_path))

    clock = ManualClock(start=et(2026, 9, 2, 10, 10))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY, TickerConfig(ticker="NEW", options=False))),
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        master=daemon._master_reader(tmp_path),
        pid=4,
    )

    # NEW is onboarded at 10:05, after the marker was built and mid-stall.
    later = SecurityMaster.read(master_path(tmp_path))
    later.register(
        kind="equity",
        capture_start=et(2026, 9, 2, 10, 5),
        valid_from=date(2026, 9, 2),
        ticker="NEW",
    )
    later.write(master_path(tmp_path))

    marker.on_skipped([et(2026, 9, 2, 10, m) for m in range(1, 10)])
    marked = [slot[11:16] for slot in _slots(tmp_path, "quotes", "NEW", date(2026, 9, 2))]
    assert marked == ["10:05", "10:06", "10:07", "10:08", "10:09"]


def test_a_corrupt_master_leaves_the_daemon_reader_unclamped_rather_than_crashing(tmp_path):
    """A master the reader cannot parse must drop the clamp, not end the daemon.

    The write is atomic now, so onboarding no longer exposes a half-written master. A
    master can still go bad at rest, from a disk error or bit rot. A corrupt parquet raises
    ``pyarrow``'s ``ArrowInvalid``. The reader runs from a hook the loop does not guard, so
    an escape ends the loop on a pyarrow traceback. ``SecurityMaster.read`` folds that into
    ``SecurityMasterError``, which the reader catches, returning ``None``. That is the same
    no-clamp answer an absent master gives.
    """
    from lake import daemon
    from lake.security_master import master_path

    path = master_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet at all")

    assert daemon._master_reader(tmp_path)() is None


# -- the production wiring -----------------------------------------------------------


def test_the_daemon_wires_gap_marking_into_the_startup_hook(tmp_path, monkeypatch, capsys):
    """The loop really marks, rather than the marker merely working in isolation.

    Without this the ``on_start`` binding in ``run_loop_from_config`` can be deleted and
    the suite stays green, which is what a review found. This run is a single pre-open
    tick with no overrun, so it drives that hook alone. The skipped-slot binding is held
    by the roster tests below, which a later review found this one never reached.
    """
    from lake import daemon
    from lake.capture_spans import spans_path
    from lake.security_master import master_path
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("XYZ: {options: false}\n")
    # XYZ in scope on disk, so the daemon's spans reader places it. Monday is the floor.
    master, spans = _spans_for(Roster((EQUITY_ONLY,)))
    master.write(master_path(lake_root))
    spans.write(spans_path(lake_root))
    _capture(lake_root, "XYZ", date(2026, 8, 31))
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
        compaction_runner=lambda args: None,
        should_continue=once,
    )
    marked = sorted(_gap_snaps(lake_root, "quotes", "XYZ", date(2026, 9, 1)))
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
    # In scope since 2020 with nothing recorded, so every session back to the cap is owed
    # and dark. Six sessions span a weekend and a holiday, so a calendar-day cap would
    # fall short.
    master, spans = _spans_for(Roster((EQUITY_ONLY,)), et(2020, 1, 2, 9, 30))
    monkeypatch.setattr(gap, "MAX_LOOKBACK_SESSIONS", 6)
    clock = ManualClock(start=et(2026, 9, 8, 10, 0))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY,)),
        session_clock=SessionClock(clock=clock, calendar=weekday_sessions(WEEK, date(2026, 9, 7))),
        master=lambda: master,
        spans=lambda: spans,
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

    _capture(tmp_path, "XYZ", date(2026, 9, 1))  # Tuesday full is the floor
    _record(tmp_path, "quotes", "XYZ", et(2026, 9, 2, 9, 59))
    master, spans = _spans_for(Roster((EQUITY_ONLY,)))
    clock = ManualClock(start=et(2026, 9, 2, 10, 0) + timedelta(seconds=start_second))
    session_clock = SessionClock(clock=clock, calendar=weekday_sessions(WEEK))
    marker = gap.GapMarker(
        lake_root=tmp_path,
        roster=lambda: Roster((EQUITY_ONLY,)),
        session_clock=session_clock,
        master=lambda: master,
        spans=lambda: spans,
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


def _overrun_after_a_roster_change(
    tmp_path: Path, *, before: str, after: str
) -> tuple[Path, list[str]]:
    """Run a loop whose first cycle rewrites ``tickers.yaml`` and then overruns.

    Returns the lake root and the tickers the watchdog raised a page for. Both
    skipped-slot consumers read the roster, so one run shows what each of them did.

    The rewrite lands at 10:00 and that cycle returns at 10:03, so the next tick reports
    10:01 through 10:03 as skipped. A skipped slot is the one hook where no cycle ran, so
    nothing but the roster each consumer reads decides which surfaces it acts on. Three
    skipped minutes is also the watchdog's page threshold, so a charged surface pages
    inside this run and an uncharged one stays silent.

    The cycle carries no segments, so it is never a durable data cycle and the dead-man
    never pings.
    """
    from lake import daemon
    from lake.capture import CycleResult
    from tests.support.config import write_config

    lake_root = tmp_path / "lake"
    lake_root.mkdir(parents=True)
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(before)

    clock = ManualClock(start=et(2026, 9, 2, 9, 59, 30))
    alerts = FakeTransport()
    cycles = [0]

    def cycle(*, close_tag, session_phase) -> CycleResult:
        cycles[0] += 1
        if cycles[0] == 1:
            tickers.write_text(after)
            clock.advance(seconds=60 * OVERRUN)
        return CycleResult(clock.now(), ())

    ticks = [0]

    def twice() -> bool:
        ticks[0] += 1
        return ticks[0] <= 2

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        transport=alerts,
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        cycle_runner=cycle,
        should_continue=twice,
    )
    return lake_root, _charged([m.title for m in alerts.messages])


# The minutes the overrun swallows. The length is the watchdog's own page threshold, so
# a surface that stays charged for all of them pages exactly once inside one run. Taking
# it from the guard constants keeps a changed threshold from reading as "the watchdog
# stopped charging the roster", which is the wrong diagnosis for that failure. The
# fixture config sets no guards, so this is the value the daemon will read.
OVERRUN = GuardConstants().watchdog_page_minutes
SKIPPED = [f"2026-09-02T10:{m:02d}" for m in range(1, OVERRUN + 1)]

# Every ticker these tests put on a roster. A page names the ticker it is about, so the
# set of names appearing across the pages is which surfaces were charged. Reading it
# that way keeps the watchdog's title format asserted in one place, its own unit tests.
_TICKERS = ("ABC", "NEW", "XYZ")


def _charged(titles: list[str]) -> list[str]:
    """Which tickers the run raised a page for."""
    return sorted({t for t in _TICKERS for title in titles if t in title})


def _marked(root: Path, ticker: str) -> list[str]:
    return [slot[:16] for slot in _slots(root, "quotes", ticker, date(2026, 9, 2))]


def test_a_ticker_onboarded_mid_session_has_its_skipped_minutes_marked(tmp_path):
    """The roster gains NEW at 10:00, so NEW owes 10:01 through 10:03 like ABC does.

    Capture re-reads the roster every cycle, so NEW is already being captured by the
    time the loop oversleeps. A marking pass working from the roster the daemon loaded
    at start would leave those three minutes as holes on a surface that is in scope,
    which is the failure gap marking exists to close.
    """
    lake_root, _ = _overrun_after_a_roster_change(
        tmp_path,
        before="ABC: {options: false}\n",
        after="ABC: {options: false}\nNEW: {options: false}\n",
    )
    assert _marked(lake_root, "NEW") == SKIPPED


def test_a_ticker_retired_mid_session_stops_collecting_markers(tmp_path):
    """The roster loses XYZ at 10:00, so nothing captures it and nothing owes it.

    Scope's front edge already works this way: the anchor clamps to ``capture_start``,
    so minutes before it are out of scope rather than gaps. Leaving the roster is the
    same boundary. Marking XYZ's 10:01 would manufacture a hole on a surface no cycle
    will write to again. ABC is the control: without it a marker that stopped writing
    entirely would pass.
    """
    lake_root, _ = _overrun_after_a_roster_change(
        tmp_path,
        before="ABC: {options: false}\nXYZ: {options: false}\n",
        after="ABC: {options: false}\n",
    )
    assert _marked(lake_root, "ABC") == SKIPPED
    assert _marked(lake_root, "XYZ") == []


def test_a_ticker_disabled_in_place_stops_collecting_markers_too(tmp_path):
    """The on/off switch is the other way a ticker leaves capture, and marking must
    honor it the same as removal.

    XYZ stays in ``tickers.yaml``, only turned off, so a reader that iterates the raw
    roster would still mark it. The one that matters here reads only enabled entries.
    """
    lake_root, _ = _overrun_after_a_roster_change(
        tmp_path,
        before="ABC: {options: false}\nXYZ: {options: false}\n",
        after="ABC: {options: false}\nXYZ: {options: false, enabled: false}\n",
    )
    assert _marked(lake_root, "ABC") == SKIPPED
    assert _marked(lake_root, "XYZ") == []


def test_the_watchdog_charges_the_roster_as_it_stands_on_a_skipped_slot(tmp_path):
    """The other consumer of the same read, driven the same way.

    Gap marking and the watchdog counters both fire from ``on_skipped``, where no cycle
    ran to fix a roster snapshot, so both read ``tickers.yaml`` themselves. Deleting
    either read must not leave the suite green. XYZ is retired at 10:00 and must stop
    being charged without a restart. NEW is onboarded at 10:00, is captured from the
    next cycle, and must start.
    """
    _, retired = _overrun_after_a_roster_change(
        tmp_path / "retired",
        before="ABC: {options: false}\nXYZ: {options: false}\n",
        after="ABC: {options: false}\n",
    )
    assert retired == ["ABC"]

    _, onboarded = _overrun_after_a_roster_change(
        tmp_path / "onboarded",
        before="ABC: {options: false}\n",
        after="ABC: {options: false}\nNEW: {options: false}\n",
    )
    assert onboarded == ["ABC", "NEW"]


def test_the_watchdog_does_not_charge_a_ticker_disabled_in_place(tmp_path):
    """The on/off switch is the same boundary here as it is for gap marking.

    XYZ stays in ``tickers.yaml``, only turned off, rather than being removed. A reader
    that iterates the raw roster would still charge it and eventually page for a surface
    nothing owes any more.
    """
    _, disabled = _overrun_after_a_roster_change(
        tmp_path,
        before="ABC: {options: false}\nXYZ: {options: false}\n",
        after="ABC: {options: false}\nXYZ: {options: false, enabled: false}\n",
    )
    assert disabled == ["ABC"]
