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
2. ``next_span_start=<epoch seconds>``, the start of the first refused span that ends
   after now. Inside a span that start is in the past. ``vm-deploy.sh`` keeps it, so its
   wait before the restart needs no Python once the checkout has moved.

**A refused span** covers each day of the week on which the units have schedules. It runs
from ``LEAD`` before that day's earliest scheduled start to the later of ``TAIL`` after
its latest start and the end of that day's ``control_plane.assertion_window``. The
schedules come from ``control_plane.systemd_units``, built on a placeholder host because
every job factory is pure, so no time is restated here. Today that gives 08:00 to 18:45
on every weekday, sessions and holidays alike, and 19:30 to 23:30 on Sunday. Saturday has
none.

**The margin refuses a start too close to the next span.** ``vm-deploy.sh`` caps each of
its steps, and the caps sum to 200 minutes, a rollback included. ``MARGIN`` is 210
minutes, so a deploy that starts at the last moment this allows still finishes, or rolls
back, before the next span opens. A test of the script checks that its caps sum to less
than ``MARGIN``.

Every span is measured in absolute time, as UTC instants, so a span next to a clock change
is still the right number of real minutes long.

The module reads the clock through the ``Clock`` seam and nothing else. It needs no
config file and no lake, so it runs before the deploy has checked either.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from lake import control_plane
from lake.calendar import MARKET_TZ
from lake.clock import Clock

# How long before a day's earliest scheduled start a deploy must have finished. The
# restart's own check takes two minutes, and a job that starts against a half-moved tree
# can find files missing.
LEAD = timedelta(minutes=30)

# How long after a day's latest scheduled start the span still runs, so a job that starts
# on time is not reading the tree when the merge lands.
TAIL = timedelta(minutes=15)

# The least time that must remain before the next refused span for a deploy to start.
# ``deploy/vm-deploy.sh`` caps its steps at 200 minutes in all, a rollback included.
MARGIN = timedelta(minutes=210)

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
        first.start,
    )


def _minutes(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def main(argv: Sequence[str] | None = None, *, clock: Clock | None = None) -> int:
    """The ``python -m lake.deploy_window`` entry. Returns a process exit code.

    ``clock`` stays injectable, since a clock reaches nothing past this process.
    """
    argparse.ArgumentParser(
        prog="python -m lake.deploy_window",
        description="Exit 0 when a deploy of the hosted VM may start now, and 3 when not.",
    ).parse_args(argv)
    if clock is None:
        from lake.clock import SystemClock  # lazy: only the console reads the wall clock

        clock = SystemClock()
    verdict = decide(clock.now())
    for line in verdict.lines():
        print(line)
    return verdict.code


if __name__ == "__main__":
    sys.exit(main())
