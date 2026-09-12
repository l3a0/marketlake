"""Gap marking: the named writer for minutes nobody captured.

A missed minute is gone forever. Nothing recovers it. What gap marking does is record
the absence, so completeness is counted from rows and never inferred from holes. One
marker row per missed minute, carrying no market data and naming the reason it is there.

Two producers hand slots to one writer.

1. Startup marking runs from the daemon's ``on_start`` hook, before the first live
   cycle. It covers the minutes the previous incarnation never reached, back through
   whole sessions the machine slept through.
2. Skipped-slot marking runs from ``on_skipped`` when a live loop sleeps through a
   capture slot, after an overrun or a stall.

They never overlap, because ``run_loop`` calls ``on_start`` before its first tick and
carries no previous slot into it. The two differ only in the reason they stamp. A
startup marker says ``daemon_dead``, which is true: some other incarnation ended. A
skipped-slot marker says ``slot_overrun``, because the daemon is alive on those minutes
and recording it as dead would make the marker lie about its own reason.

Both passes read the roster when they run, never a roster frozen at daemon start. The
roster is the only statement of what is in scope right now, and the capture cycle reads
it fresh every minute. A pass that read a stale one would disagree with capture in both
directions. A ticker onboarded mid-session would be captured and left unmarked, which is
the hole this exists to close. A ticker retired mid-session would keep collecting markers
on a surface nothing captures, manufacturing holes that were never owed.

A ticker's scope is a set of capture spans, and the startup walk reads them to stay on
the right side of both ends. A minute is owed only when a span covers it, so minutes
before the first span, after a closed span, and between two spans are out of scope rather
than gaps. This closes the rejoin hole. A ticker retired and later brought back opens a
second span, and the away period falls in no span, so the walk owes nothing there and
marks nothing, rather than walking the whole absence back as ``daemon_dead`` gaps.

The startup walk is hole-aware. For each session day it owes the capture window minutes
in scope, and marks the ones no row records. So a single stray row cannot pose as a
frontier and hide the rest of the day. That is the fix for the collapse where a dark
session showed 16 of its 406 minutes recorded, a stray row near the close posing as a
frontier, so the old walk left the other 390 owed minutes unmarked. They are marked now,
because they are owed and no row records them.

Marking is calendar-driven, not segment-driven. It asks the calendar which sessions
existed and which minutes those sessions held, then subtracts what is already recorded.
A date with no segments at all is exactly the case a segment-driven walk would miss, and
it is the case that matters most: a laptop closed for a week leaves no trace to walk. The
walk stops at the first day with nothing missing, a fully captured or out-of-scope day,
and everything below it was made complete by the incarnation that reached it.

Two rules keep repeated marking safe, which matters because the daemon runs under
``KeepAlive`` and a crash loop restarts it within seconds.

1. The present set counts marker rows as well as data rows, so a minute a previous
   restart already marked is not owed a second time.
2. A date the manifest has already sealed is skipped, so markers never land under a
   partition whose row count is fixed.

A pass with no missed minutes writes nothing, because the write loop runs once per date
that has slots and an empty span has none.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from lake import journal
from lake.calendar import NotASession
from lake.capture_spans import CaptureSpan, CaptureSpans, spans_of_ticker
from lake.lock import lake_lock
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.security_master import (
    SecurityMaster,
    capture_start_in_market_time,
    is_in_scope,
)
from lake.session import TICK, SessionClock, SessionPhase, session_slots
from lake.tickers import Roster

# The reason stamped on a startup marker. The previous incarnation ended without
# reaching these minutes, so from the new one's side the writer that owed them is gone.
DAEMON_DEAD = "daemon_dead"

# The reason stamped on a slot the live loop slept through. The daemon is alive on
# these minutes, so ``daemon_dead`` would be false. A cycle that ran long, or a machine
# that stalled without the process dying, lands here.
SLOT_OVERRUN = "slot_overrun"

# How far back a startup pass will walk looking for where a ticker's record stops,
# counted in sessions rather than calendar days, because a session is what carries
# capture slots. A machine closed for a season is a real case, so this is generous. A
# first-ever start on an empty lake is what it bounds, where nothing is recorded and the
# walk would otherwise run to the instrument's capture start.
MAX_LOOKBACK_SESSIONS = 90

# A calendar a test builds may hold no sessions at all. Without this the session budget
# is never spent and the walk runs to the beginning of time.
_CALENDAR_DAY_GUARD = 400

# The writer-session stamp. It matches the capture segment's own shape, so a marker
# segment sorts beside the segments it sits among.
SEGMENT_STAMP_FORMAT = "%Y%m%dT%H%M%S%f"

_ONE_DAY = timedelta(days=1)


@dataclass(frozen=True)
class MarkedSpan:
    """One marker segment: which ticker-day it covers and how many minutes it names."""

    surface: str
    ticker: str
    day: date
    slots: int
    path: Path


@dataclass(frozen=True)
class MarkingReport:
    """What one marking pass did.

    Startup marking is otherwise invisible. It runs before the first cycle, writes into
    a directory nobody watches, and takes no manifest entry, so a pass that marked
    nothing looks exactly like a pass that marked the right thing. This is the record
    that tells them apart.

    ``truncated`` names the tickers whose walk-back hit ``MAX_LOOKBACK_SESSIONS``. Those
    marked up to the cap and stopped, so their record is short by a known amount rather
    than by an unknown one.
    """

    spans: tuple[MarkedSpan, ...] = ()
    sealed: tuple[str, ...] = ()
    truncated: tuple[str, ...] = ()
    problems: tuple[str, ...] = ()

    @property
    def rows(self) -> int:
        """Marker rows written across every segment this pass opened."""
        return sum(span.slots for span in self.spans)


def surfaces_for(entry) -> tuple[str, ...]:
    """The surfaces a roster ticker is captured on.

    Quotes for every ticker. Chains only where the roster says the ticker carries
    options. Capture applies the same rule when it plans a cycle, and gap marking has to
    agree with it, or a marker would claim a surface that was never captured.
    """
    if entry.options:
        return (journal.CHAINS_SURFACE, journal.QUOTES_SURFACE)
    return (journal.QUOTES_SURFACE,)


class GapMarker:
    """The one writer both hooks hand missed slots to.

    Every seam is injected. There is no wall clock here at all: the minutes come from
    the calendar, and each segment is stamped with the first minute it marks. So a
    marking pass is decided entirely by the calendar, the roster it reads, and what is
    already on disk.

    ``roster`` is a reader, not a roster, because scope changes while the daemon runs.
    Each pass calls it once and marks whatever it returns.

    A reader that raises takes the daemon down, and the daemon's reader carries no
    fallback to stop that. The price is the one the skipped-slot hook already pays for
    its own read, and it is smaller here: a marker stands for a minute already gone, so
    the successor's startup pass walks back and marks whatever this one missed. A stale
    roster marking minutes the file no longer names is what has no later repair.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        roster: Callable[[], Roster],
        session_clock: SessionClock,
        master: Callable[[], SecurityMaster | None] | None = None,
        spans: Callable[[], CaptureSpans | None] | None = None,
        pid: int | None = None,
    ) -> None:
        self._root = Path(lake_root)
        self._paths = LakePaths(self._root)
        self._roster = roster
        self._session_clock = session_clock
        self._master = master
        self._spans = spans
        self._pid = os.getpid() if pid is None else pid
        self._unreadable: list[str] = []
        # The master and spans this pass is judging against, read once at the top of
        # ``_pass``. A pass reads them rather than holding a copy from daemon start,
        # because onboarding and retiring write them while the daemon runs. Per pass
        # rather than per ticker, so one pass judges every ticker against one snapshot.
        self._master_now: SecurityMaster | None = None
        self._spans_now: CaptureSpans | None = None

    # -- the two hooks ---------------------------------------------------------

    def on_start(self) -> MarkingReport:
        """Mark every minute owed before the daemon's first live cycle.

        The upper bound is the first slot the loop will capture, not the current minute.
        ``run_loop`` sleeps to the next minute top before its first tick, so the minute
        the daemon starts in is one no cycle will ever run for. Bounding at the current
        minute would leave it both uncaptured and unmarked, a one-minute hole on every
        restart, which is the hole this exists to close.
        """
        first_live_slot = self._session_clock.snap_slot() + TICK

        def plan(
            surface: str, ticker: str, recorded: dict[str, dict]
        ) -> tuple[list[datetime], MarkingReport]:
            self._unreadable = []
            missing, truncated, sealed = self._startup_missing(
                surface, ticker, first_live_slot, recorded
            )
            notes = MarkingReport(
                sealed=tuple(sealed),
                truncated=(f"{surface}/{ticker}",) if truncated else (),
                problems=tuple(self._unreadable),
            )
            return missing, notes

        return self._pass(DAEMON_DEAD, plan)

    def on_skipped(self, slots: list[datetime]) -> MarkingReport:
        """Mark the capture slots a live loop slept through.

        The loop hands the same slots for every ticker, because it missed the whole
        cycle rather than one ticker's fetch. Each ticker's own scope still applies, so
        the slots are clamped to its ``capture_start``, the epoch before which nothing is
        owed. A stall can outlive an onboarding: the loop sleeps from 10:00 to 10:10 and a
        ticker joins the roster at 10:05, so this pass is the first to see it. Its 10:01
        was never owed, and marking it would render "40% missing" on a ticker the design
        renders as "onboarded 10:05". A ticker the master cannot place is not clamped,
        which only ever widens the marking, and ``_capture_start`` never raises.
        """

        def plan(
            surface: str, ticker: str, recorded: dict[str, dict]
        ) -> tuple[list[datetime], MarkingReport]:
            epoch = self._capture_start(ticker)
            if epoch is None:
                return list(slots), MarkingReport()
            return [slot for slot in slots if is_in_scope(slot, epoch)], MarkingReport()

        return self._pass(SLOT_OVERRUN, plan)

    # -- the pass --------------------------------------------------------------

    def _pass(
        self,
        error_class: str,
        plan: Callable[[str, str, dict[str, dict]], tuple[list[datetime], MarkingReport]],
    ) -> MarkingReport:
        """Run one marking pass over the whole roster under a single lock.

        The lock is taken once and the ledger read once, for the pass rather than for
        each ticker. ``on_skipped`` runs on the loop thread, so a lock per surface per
        ticker would put a growing stall in front of the next capture cycle.

        Deciding and writing sit inside the same hold. The recorded set is read there too,
        so a second incarnation cannot read the same state and mark the same minutes into
        a differently named segment. Compaction fixes a date's row count when it seals,
        so a marker landing between the seal check and the write would make that count
        wrong.

        Capture stays outside this lock, because a blocked cycle drops perishable
        minutes. A marker stands for a minute already gone, so nothing perishes while it
        waits.

        The roster is read once here, for the pass rather than for each ticker, and
        outside the lock because it is not lake state. One read per pass keeps every
        surface in the pass judged against one snapshot of what is in scope. Only
        enabled entries are marked. A ticker disabled in place, rather than removed,
        still names an entry in the file. Marking it anyway would manufacture gaps on a
        surface nothing owes any more, the same holes freezing the roster used to
        manufacture, so this reads only the enabled entries the same way capture does.
        The command that disables a ticker closes its capture span first, so the two
        stay in step. The rule binds the command; a hand edit that flips the switch
        without closing the span is outside it, the same as every other roster rule
        here.
        """
        spans: list[MarkedSpan] = []
        sealed: list[str] = []
        problems: list[str] = []
        notes = MarkingReport()
        try:
            roster = self._roster().enabled
            self._master_now = self._master() if self._master is not None else None
            self._spans_now = self._spans() if self._spans is not None else None
            with lake_lock(self._root):
                recorded = latest_entries(self._root)
                for entry in roster:
                    for surface in surfaces_for(entry):
                        slots, pair_notes = plan(surface, entry.ticker, recorded)
                        notes = _merge(notes, pair_notes)
                        by_day: dict[date, list[datetime]] = {}
                        for slot in slots:
                            by_day.setdefault(slot.date(), []).append(slot)
                        for day, day_slots in sorted(by_day.items()):
                            key = (
                                self._paths.partition_path(surface, entry.ticker, day)
                                .relative_to(self._root)
                                .as_posix()
                            )
                            if key in recorded:
                                sealed.append(key)
                                continue
                            try:
                                spans.append(
                                    self._segment(
                                        surface, entry.ticker, day, day_slots, error_class
                                    )
                                )
                            except OSError as exc:
                                # Stop this pair here. An OSError like a full disk is
                                # likely to hit the next day too. The unwritten days stay
                                # owed, so the next restart re-derives what is missing and
                                # marks them then. Stopping now defers the work, never
                                # drops it.
                                problems.append(
                                    f"{surface}/{entry.ticker} {day.isoformat()}: "
                                    f"{type(exc).__name__}"
                                )
                                break
        except (OSError, ValueError) as exc:
            # The lock or the ledger itself. A roster that will not load raises
            # ``TickersError`` and is deliberately not caught here, the same way the
            # skipped-slot hook does not catch it. ``on_start`` is unguarded and the
            # daemon runs under ``KeepAlive``, so raising for the lock or the ledger
            # would be a crash loop that marks nothing. Record those and let the loop run.
            problems.append(f"marking pass: {type(exc).__name__}")
        return _merge(notes, MarkingReport(tuple(spans), tuple(sealed), (), tuple(problems)))

    def _segment(
        self,
        surface: str,
        ticker: str,
        day: date,
        slots: list[datetime],
        error_class: str,
    ) -> MarkedSpan:
        """One marker segment holding one date's markers and nothing else.

        The segment is stamped with the first minute it marks, not with the wall clock.
        Two marking passes in the same second would otherwise produce the same name, and
        ``SegmentWriter`` opens with ``O_CREAT|O_EXCL``, so the second would fail. A
        startup pass and a skipped-slot pass in the same minute is the ordinary case,
        not a rare one. Stamping from the span also makes the name say what it covers,
        and two passes cannot cover the same first minute, because the recorded set counts
        the previous pass's marker rows, so those minutes are no longer owed.
        """
        stamp = slots[0].strftime(SEGMENT_STAMP_FORMAT)
        path = journal.segment_path(self._root, surface, ticker, day, stamp, self._pid)
        batch = journal.gap_rows(
            surface,
            ticker=ticker,
            slots=slots,
            error_class=error_class,
            session_phase_at=self._phase_at,
        )
        with journal.SegmentWriter.open(
            self._root, surface, ticker, day, stamp, self._pid
        ) as writer:
            writer.write_cycle(batch)
        return MarkedSpan(surface, ticker, day, len(slots), path)

    def _phase_at(self, slot: datetime) -> str | None:
        """The session phase a captured row would have carried for this minute."""
        phase = self._session_clock.phase_at(slot)
        return phase.value if phase is SessionPhase.POST_EQUITY_CLOSE else None

    # -- the hole-aware startup walk -------------------------------------------

    def _startup_missing(
        self,
        surface: str,
        ticker: str,
        first_live_slot: datetime,
        recorded: dict[str, dict],
    ) -> tuple[list[datetime], bool, list[str]]:
        """The owed-but-unrecorded minutes for one ticker-surface, walking back from today.

        For each session day, the owed minutes are the capture window intersected with the
        ticker's capture spans, and the missing ones are the owed minutes no row records. A
        single stray row, such as the close guard's 16:00 marker on an otherwise dark day,
        is one present minute among the owed set rather than a frontier that hides the
        morning. That is the fix for the collapse where a dark session recorded 16 of its
        406 minutes.

        The walk stops at the first prior day with nothing missing: a fully captured day,
        or a day out of scope. Stopping at a fully captured day is safe, because the
        incarnation that completed it also handled every day below it. Stopping at an
        out-of-scope day bounds a normal restart to the recent past. The price is a rejoin
        edge: a dark, in-scope day in an earlier closed span, below the away gap between two
        spans, is not caught here, but only after a compound failure where the daemon was
        dead for that whole earlier span. A day with any missing minute keeps the walk
        going, because a dark day can sit below it.

        A date the manifest has sealed is skipped, and the walk continues past it rather
        than stopping. Compaction unlinks a sealed day's segments, so it reads as dark and
        would be re-marked, and stopping at it is the rejected last-manifested-partition
        anchor that leaves a dark date below it unmarked.

        An unreadable segment aborts the pair, so a full session of markers is never
        written over a record that exists. The 90-session cap is the backstop for a lake
        with no record; hitting it marks down to the oldest examined day and claims nothing
        below it.

        Scope comes from the capture spans. When they cannot be read, the ticker cannot be
        placed in scope, so nothing is owed and nothing is marked. That defers the pass to
        the next readable restart rather than inventing a full session of gaps against a
        ticker whose scope is unknown.
        """
        spanlist = spans_of_ticker(
            self._spans_now, self._master_now, ticker, self._session_clock.session_date()
        )
        missing: list[datetime] = []
        sealed: list[str] = []
        day = first_live_slot.date()
        sessions = 0
        calendar_days = 0
        while sessions < MAX_LOOKBACK_SESSIONS and calendar_days < _CALENDAR_DAY_GUARD:
            calendar_days += 1
            try:
                bounds = self._session_clock.bounds(day)
            except NotASession:
                # A weekend or a holiday carries no capture slots and costs no session
                # budget. The cap counts sessions, because a session is what carries slots.
                day -= _ONE_DAY
                continue
            sessions += 1
            key = (
                self._paths.partition_path(surface, ticker, day).relative_to(self._root).as_posix()
            )
            if key in recorded:
                # Sealed after it was marked, so it is accounted. Name it and keep walking.
                # The check is a dict lookup, so a long sealed history costs almost nothing
                # and the owed-versus-present read runs only on the unsealed recent days.
                sealed.append(key)
                day -= _ONE_DAY
                continue
            present = journal.recorded_slots(self._root, surface, ticker, day)
            if present.unreadable:
                # A segment that cannot be read is not the same as no segment. Marking
                # this day would write a full session over a record that exists. Refuse
                # the pair and say so, so the next restart tries again.
                self._unreadable.append(
                    f"{surface}/{ticker} {day.isoformat()}: {len(present.unreadable)} unreadable"
                )
                return [], False, sealed
            owed = [
                slot
                for slot in session_slots(bounds)
                if slot < first_live_slot and self._in_scope(surface, spanlist, slot)
            ]
            day_missing = [slot for slot in owed if slot not in present.slots]
            missing.extend(day_missing)
            if not day_missing and day < first_live_slot.date():
                # A prior day with nothing missing: fully captured, or out of scope.
                # Everything below it is accounted, so stop. The restart date itself never
                # stops the walk, because a pre-open or mid-session restart owes little or
                # nothing there while the day before may hold a whole dark session.
                return missing, False, sealed
            day -= _ONE_DAY
        # The cap stopped the walk. Everything down to the oldest examined day is already
        # in ``missing``, and nothing below it is claimed.
        return missing, True, sealed

    def _in_scope(
        self, surface: str, spanlist: tuple[CaptureSpan, ...] | None, slot: datetime
    ) -> bool:
        """Whether ``slot`` is owed on ``surface`` by the ticker's capture spans.

        A slot is owed when a span covers it. On the chains surface it is owed only when
        the covering span captured options, because a span with options off owed no chain.
        ``None`` means scope could not be read, so nothing is owed.
        """
        if spanlist is None:
            return False
        wants_options = surface == journal.CHAINS_SURFACE
        return any(span.contains(slot) and (not wants_options or span.options) for span in spanlist)

    def _capture_start(self, ticker: str) -> datetime | None:
        """The instrument's capture start, or ``None`` when the master cannot say.

        Losing the clamp is the safe direction here. Only the skipped-slot pass reads it,
        and that pass marks just the slots the loop slept through, so a lost clamp widens
        the marking by at most that short list.
        """
        return capture_start_in_market_time(
            self._master_now, ticker, self._session_clock.session_date()
        )


def _merge(left: MarkingReport, right: MarkingReport) -> MarkingReport:
    """One report covering both passes."""
    return MarkingReport(
        left.spans + right.spans,
        left.sealed + right.sealed,
        left.truncated + right.truncated,
        left.problems + right.problems,
    )


__all__ = [
    "DAEMON_DEAD",
    "MAX_LOOKBACK_SESSIONS",
    "SEGMENT_STAMP_FORMAT",
    "SLOT_OVERRUN",
    "GapMarker",
    "MarkedSpan",
    "MarkingReport",
    "surfaces_for",
]
