"""Whether a deploy of the hosted VM may start now, marketlake #676.

A deploy moves the checkout and restarts the daemon. ``lake`` is an editable install, so a
moved checkout changes what every new process imports, and a restart costs the daemon its
cycle in flight. So a deploy must not overlap the hours the scheduled jobs and the capture
run in. ``deploy/vm-deploy.sh`` runs this module before it touches anything::

    python -m lake.deploy_window

It exits 0 when a deploy may start now and 3 when it may not, and prints exactly two
lines.

1. A line for people, in Eastern time. On exit 0 it names the latest moment a deploy may
   start. On exit 3 it names the next moment one may, which is the end of the current
   refused span, or the end of a later one when the margin below refuses the gap between
   them.
2. ``next_span_start=<epoch seconds>``, the start of the first refused span that has not
   opened yet. ``vm-deploy.sh`` keeps it, so its wait before the restart needs no Python
   once the checkout has moved. Outside a span it is the start of the span ahead. Inside
   a span it is the start of the one after, because a deploy that overrides the window
   (marketlake #987) starts inside one, and the current span's start, already past, would
   refuse its restart after the merge had moved the tree. Every other reader acts on the
   value only after exit 0, where the two readings agree.

**A refused span** covers each day of the week on which the units have schedules. It runs
from ``LEAD`` before that day's earliest scheduled start to the later of ``TAIL`` after
its latest start and the end of that day's ``control_plane.assertion_window``. The
schedules come from ``control_plane.systemd_units``, built on a placeholder host because
every job factory is pure, so no time is restated here. Today that gives 08:00 to 18:45
on every weekday, sessions and holidays alike, and 19:30 to 23:30 on Sunday. Saturday has
none. The hosted VM's stop and its 07:40 ping come from ``control_plane.vm_power_units``
rather than the roster, so neither moves a span (marketlake #868). ``deploy/vm-stop.sh``
runs this module too, so the VM may stop exactly when a deploy may start.

**The margin refuses a start too close to the next span.** ``vm-deploy.sh`` caps each of
its steps, and the caps sum to 80 minutes, a rollback included. ``MARGIN`` is 90
minutes, so a deploy that starts at the last moment this allows still finishes, or rolls
back, before the next span opens. A test of the script checks that its caps sum to less
than ``MARGIN``.

Every span is measured in absolute time, as UTC instants, so a span next to a clock change
is still the right number of real minutes long.

**The session hold keeps an overridden deploy out of market hours.** A deploy that
overrides the window (marketlake #987) skips the spans above, and on 2026-11-01 the owner
ruled that it still never runs during a session. The busy checks cannot see the capture
cycles or the close+5 fill, which run inside the daemon, and a restart that kills the
equity close's cycle loses that day's ``spot_close`` for good. So ``--session-hold``
prints two lines in place of the verdict's.

1. A line for people, naming today's hold in Eastern time, or saying no session holds a
   deploy today.
2. ``session_hold=<refuse_from>,<open>,<end>`` in epoch seconds, or ``session_hold=none``.
   ``end`` is the close+15 compaction, which the busy check sees once it starts.
   ``refuse_from`` is ``MARGIN`` before the open, so a deploy that starts before it ends,
   or rolls back, before the session opens. ``vm-deploy.sh`` refuses an overridden start
   from ``refuse_from`` to ``end`` and holds its restart out of ``open`` to ``end``.

The times come from ``SessionClock.bounds``, so an early close moves the hold with it.

The module reads the clock through the ``Clock`` seam, and the exchange calendar for the
session hold, and nothing else. It needs no config file and no lake, so it runs before the
deploy has checked either.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from lake import control_plane
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar
from lake.clock import Clock
from lake.session import SessionClock

# How long before a day's earliest scheduled start a deploy must have finished. The
# restart's own check takes two minutes, and a job that starts against a half-moved tree
# can find files missing.
LEAD = timedelta(minutes=30)

# How long after a day's latest scheduled start the span still runs, so a job that starts
# on time is not reading the tree when the merge lands.
TAIL = timedelta(minutes=15)

# The least time that must remain before the next refused span for a deploy to start.
# ``deploy/vm-deploy.sh`` caps its steps at 80 minutes in all, a rollback included.
MARGIN = timedelta(minutes=90)

# How many days ahead the search for the next span looks. A week covers every weekday,
# and the extra day covers a span that ends past midnight.
_LOOKAHEAD_DAYS = 8

ALLOWED = 0
REFUSED = 3

# Every job factory reads only these strings, so any host builds the same schedules.
_PLACEHOLDER_HOST = control_plane.SystemdHost(
    python="/placeholder/python",
    owner="placeholder",
    home="/placeholder",
    project_dir="/placeholder",
)

Units = Sequence[control_plane.SystemdUnit]
WindowFor = Callable[[date], control_plane.AssertionWindow | None]


@dataclass(frozen=True)
class Span:
    """A refused span, as two UTC instants. ``end`` is the first moment outside it."""

    start: datetime
    end: datetime

    def contains(self, when: datetime) -> bool:
        return self.start <= when < self.end


@dataclass(frozen=True)
class Verdict:
    """What the module prints and exits with."""

    code: int
    line: str
    next_span_start: datetime

    def lines(self) -> tuple[str, str]:
        return (self.line, f"next_span_start={int(self.next_span_start.timestamp())}")


def default_units() -> tuple[control_plane.SystemdUnit, ...]:
    """The VM's units, from the one roster both renders read."""
    return control_plane.systemd_units(_PLACEHOLDER_HOST)


def refused_span(
    day: date,
    units: Units,
    window_for: WindowFor = control_plane.assertion_window,
) -> Span | None:
    """The span refused on ``day``, or ``None`` when no unit has a schedule that day."""
    starts = [
        unit.schedule.at.on(day).astimezone(UTC)
        for unit in units
        if unit.schedule is not None and day.weekday() in unit.schedule.days
    ]
    if not starts:
        return None
    end = max(starts) + TAIL
    window = window_for(day)
    if window is not None:
        end = max(end, window.end.astimezone(UTC))
    return Span(min(starts) - LEAD, end)


def spans_from(
    now: datetime,
    units: Units,
    window_for: WindowFor = control_plane.assertion_window,
) -> list[Span]:
    """Every refused span that ends after ``now``, earliest first.

    The search starts a day before ``now``'s Eastern date, so a span that began
    yesterday and is still open is found too.
    """
    first = now.astimezone(MARKET_TZ).date() - timedelta(days=1)
    found = []
    for offset in range(_LOOKAHEAD_DAYS + 1):
        span = refused_span(first + timedelta(days=offset), units, window_for)
        if span is not None and span.end > now:
            found.append(span)
    return sorted(found, key=lambda span: span.start)


def _eastern(when: datetime) -> str:
    return when.astimezone(MARKET_TZ).strftime("%a %Y-%m-%d %H:%M %Z")


def decide(
    now: datetime,
    units: Units | None = None,
    window_for: WindowFor = control_plane.assertion_window,
) -> Verdict:
    """Whether a deploy may start at ``now``, a timezone-aware instant."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC)
    spans = spans_from(now, default_units() if units is None else units, window_for)
    if not spans:
        raise ValueError("no unit has a schedule, so no span bounds a deploy")
    first = spans[0]
    upcoming = next((span.start for span in spans if span.start > now), None)
    if upcoming is None:
        raise ValueError("no refused span opens within the lookahead")
    if first.contains(now):
        reason = "the scheduled jobs run until then"
        free_from, rest = first.end, spans[1:]
    elif first.start - now < MARGIN:
        reason = f"less than {_minutes(MARGIN)} minutes remain before the next span"
        free_from, rest = first.end, spans[1:]
    else:
        latest = first.start - MARGIN
        return Verdict(
            ALLOWED,
            f"a deploy may start now, and until {_eastern(latest)}",
            first.start,
        )
    # A gap shorter than the margin between two spans refuses a start in it too, so the
    # next moment a deploy may start is the end of the last span in that run.
    for span in rest:
        if span.start - free_from >= MARGIN:
            break
        free_from = max(free_from, span.end)
    return Verdict(
        REFUSED,
        f"a deploy may start next at {_eastern(free_from)}, because {reason}",
        upcoming,
    )


def _minutes(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def session_hold(clock: Clock, calendar: Calendar) -> tuple[datetime, datetime, datetime] | None:
    """Today's ``refuse_from``, ``open`` and ``end`` for an overridden deploy (#987), or
    ``None`` off a session."""
    session = SessionClock(clock, calendar)
    today = session.session_date()
    if not calendar.is_session(today):
        return None
    bounds = session.bounds(today)
    return bounds.open - MARGIN, bounds.open, bounds.compaction


def session_hold_lines(hold: tuple[datetime, datetime, datetime] | None) -> tuple[str, str]:
    if hold is None:
        return ("no session holds a deploy today", "session_hold=none")
    refuse_from, opens, end = hold
    return (
        f"the session holds a deploy from {_eastern(refuse_from)} until {_eastern(end)}",
        "session_hold=" + ",".join(str(int(when.timestamp())) for when in hold),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
) -> int:
    """The ``python -m lake.deploy_window`` entry. Returns a process exit code.

    ``clock`` stays injectable, since a clock reaches nothing past this process.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.deploy_window",
        description="Exit 0 when a deploy of the hosted VM may start now, and 3 when not.",
    )
    parser.add_argument(
        "--session-hold",
        action="store_true",
        help="print today's session hold, which an overridden deploy stays out of",
    )
    args = parser.parse_args(argv)
    if clock is None:
        from lake.clock import SystemClock  # lazy: only the console reads the wall clock

        clock = SystemClock()
    if args.session_hold:
        calendar = ExchangeCalendar() if calendar is None else calendar
        for line in session_hold_lines(session_hold(clock, calendar)):
            print(line)
        return ALLOWED
    verdict = decide(clock.now())
    for line in verdict.lines():
        print(line)
    return verdict.code


if __name__ == "__main__":
    sys.exit(main())
