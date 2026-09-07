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

Marking is calendar-driven, not segment-driven. It asks the calendar which sessions
existed and which minutes those sessions held, then subtracts what is already recorded.
A date with no segments at all is exactly the case a segment-driven walk would miss, and
it is the case that matters most: a laptop closed for a week leaves no trace to walk.

Two rules keep repeated marking safe, which matters because the daemon runs under
``KeepAlive`` and a crash loop restarts it within seconds.

1. The anchor counts marker rows as well as data rows, so a second restart resumes
   after the first restart's markers rather than writing them again.
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
from lake.calendar import MARKET_TZ, NotASession
from lake.lock import lake_lock
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.security_master import SecurityMaster, SecurityMasterError
from lake.session import TICK, SessionClock, SessionPhase, missed_slots
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
    marking pass is a pure function of the calendar, the roster, and what is already on
    disk.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        roster: Roster,
        session_clock: SessionClock,
        master: SecurityMaster | None = None,
        pid: int | None = None,
    ) -> None:
        self._root = Path(lake_root)
        self._paths = LakePaths(self._root)
        self._roster = roster
        self._session_clock = session_clock
        self._master = master
        self._pid = os.getpid() if pid is None else pid
        self._unreadable: list[str] = []

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

        def plan(surface: str, ticker: str) -> tuple[list[datetime], MarkingReport]:
            self._unreadable = []
            anchor, truncated = self._anchor(surface, ticker, first_live_slot)
            notes = MarkingReport(
                truncated=(f"{surface}/{ticker}",) if truncated else (),
                problems=tuple(self._unreadable),
            )
            if anchor is None:
                return [], notes
            return missed_slots(self._session_clock, anchor, first_live_slot), notes

        return self._pass(DAEMON_DEAD, plan)

    def on_skipped(self, slots: list[datetime]) -> MarkingReport:
        """Mark the capture slots a live loop slept through.

        The loop hands the same slots for every ticker, because it missed the whole
        cycle rather than one ticker's fetch.
        """
        return self._pass(SLOT_OVERRUN, lambda surface, ticker: (list(slots), MarkingReport()))

    # -- the pass --------------------------------------------------------------

    def _pass(
        self,
        error_class: str,
        plan: Callable[[str, str], tuple[list[datetime], MarkingReport]],
    ) -> MarkingReport:
        """Run one marking pass over the whole roster under a single lock.

        The lock is taken once and the ledger read once, for the pass rather than for
        each ticker. ``on_skipped`` runs on the loop thread, so a lock per surface per
        ticker would put a growing stall in front of the next capture cycle.

        Deciding and writing sit inside the same hold. The anchor is read there too, so
        a second incarnation cannot read the same anchor and mark the same minutes into
        a differently named segment. Compaction fixes a date's row count when it seals,
        so a marker landing between the seal check and the write would make that count
        wrong.

        Capture stays outside this lock, because a blocked cycle drops perishable
        minutes. A marker stands for a minute already gone, so nothing perishes while it
        waits.
        """
        spans: list[MarkedSpan] = []
        sealed: list[str] = []
        problems: list[str] = []
        notes = MarkingReport()
        try:
            with lake_lock(self._root):
                recorded = latest_entries(self._root)
                for entry in self._roster:
                    for surface in surfaces_for(entry):
                        slots, pair_notes = plan(surface, entry.ticker)
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
                                # Stop this pair here. Marking its later days would move
                                # the anchor past the failure, so the next restart would
                                # never retry it. Leaving the anchor behind makes the
                                # failure temporary rather than permanent.
                                problems.append(
                                    f"{surface}/{entry.ticker} {day.isoformat()}: "
                                    f"{type(exc).__name__}"
                                )
                                break
        except (OSError, ValueError) as exc:
            # The lock or the ledger itself. ``on_start`` is unguarded and the daemon
            # runs under ``KeepAlive``, so raising here is a crash loop that marks
            # nothing. Record it and let the loop run.
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
        and two passes cannot cover the same first minute, because the anchor moves past
        whatever the previous pass wrote.
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

    # -- the anchor ------------------------------------------------------------

    def _anchor(self, surface: str, ticker: str, before: datetime) -> tuple[datetime | None, bool]:
        """Where this ticker-surface's record stops, as an exclusive lower bound.

        The walk starts at ``before``'s own date and steps back one calendar day at a
        time, skipping the days the calendar refuses. The first session holding any row
        wins, and marking resumes after that row's minute. Marker rows count, so a
        restart that already marked a span resumes after it rather than repeating it.

        Two floors stop the walk. The instrument's capture start is the real one, since
        no minute before it was ever in scope. ``MAX_LOOKBACK_SESSIONS`` is the backstop
        for a lake with no record at all, counted in sessions because a session is what
        carries capture slots. Hitting either is reported rather than passed over.
        """
        epoch = self._capture_start(ticker)
        day = before.date()
        sessions = 0
        calendar_days = 0
        oldest: datetime | None = None
        while sessions < MAX_LOOKBACK_SESSIONS and calendar_days < _CALENDAR_DAY_GUARD:
            calendar_days += 1
            try:
                bounds = self._session_clock.bounds(day)
            except NotASession:
                # A weekend or a holiday costs no budget. The cap counts sessions,
                # because a session is what carries capture slots.
                day -= _ONE_DAY
                continue
            sessions += 1
            oldest = bounds.open
            recorded = journal.last_recorded_slot(self._root, surface, ticker, day)
            if recorded.unreadable:
                # A segment that cannot be read is not the same as no segment. Marking
                # this day would write a full session of markers over a record that
                # exists. Refuse the pair and say so, so the next restart tries again.
                self._unreadable.append(
                    f"{surface}/{ticker} {day.isoformat()}: {len(recorded.unreadable)} unreadable"
                )
                return None, False
            if recorded.slot is not None:
                return self._clamp(recorded.slot, epoch), False
            if epoch is not None and bounds.open <= epoch:
                # The walk reached the instrument's first in-scope session. Nothing
                # before it was ever owed, so this is the floor rather than the cap.
                return self._clamp(epoch - TICK, None), False
            day -= _ONE_DAY
        if oldest is None or epoch is None:
            # Either the calendar held no session inside the guard, or the walk found
            # no row and no capture start. Nothing has ever claimed this instrument was
            # in scope for these sessions, so marking them would invent an absence
            # rather than record one. The pass is still reported, because "marked
            # nothing on purpose" and "marked nothing by mistake" must not look alike.
            return None, True
        # The cap stopped the walk. Anchor at the open of the oldest session actually
        # examined, so that day is marked in full and nothing below it is claimed. An
        # anchor inside an unexamined day would mark a partial session and leave the
        # rest for a later pass, which would double-count the overlap.
        return self._clamp(oldest - TICK, epoch), True

    def _clamp(self, anchor: datetime, epoch: datetime | None) -> datetime:
        """The later of a recorded anchor and the instrument's capture start.

        ``missed_slots`` is exclusive at its lower bound, so the epoch is offset by one
        slot to keep the capture-start minute itself markable.
        """
        if epoch is None:
            return anchor
        return max(anchor, epoch - TICK)

    def _capture_start(self, ticker: str) -> datetime | None:
        """The instrument's capture start, or ``None`` when the master cannot say.

        This never raises. It runs from ``on_start``, which ``run_loop`` does not guard,
        and the daemon runs under ``KeepAlive``. A raise here would relaunch within
        seconds and repeat, marking nothing and paging nobody, so a missing master or an
        unresolvable ticker degrades to no clamp rather than to a crash loop.

        Losing the clamp is the safe direction. It can only widen the walk, and a marker
        for a minute before the instrument was in scope is bounded by the anchor and by
        ``MAX_LOOKBACK_SESSIONS``. Raising instead would stop the daemon from starting at
        all.
        """
        if self._master is None:
            return None
        try:
            instrument = self._master.resolve(ticker, self._session_clock.session_date())
            if instrument is None:
                return None
            # The master stores this in UTC. Every other moment gap marking handles is
            # market time, and a marker's ``snap_ts`` and its segment stamp both come
            # from one, so convert here rather than letting one offset differ.
            return self._master.capture_start_of(instrument).astimezone(MARKET_TZ)
        except SecurityMasterError:
            return None


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
