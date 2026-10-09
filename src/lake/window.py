"""The lake window: how many recent sessions a host keeps, and the edge that count sets.

Marketlake #755 keeps only a window of recent sessions on the hosted VM's lake volume and
leaves older partitions in the backup bucket. One config key, ``lake_window_sessions``, is
both the window and the opt-in. A host without it never trims, writes no split checkpoint,
and restores the whole lake from the bucket, which is the laptop today. Besides the render in
``lake.vm_config``, which checks it, three jobs act on it.

1. The 18:30 sweep writes the split checkpoint, which marketlake #786 added with the key.
2. Marketlake #787's trim removes what falls outside the window.
3. ``lake.bucket``'s restore rebuilds the trimmed lake rather than the whole one, leaving out
   each partition the bucket's ``trimmed.jsonl`` says was removed on purpose (marketlake
   #785).

The key is named for the lake rather than for chains. The owner decided on 2026-10-07
(decision 9 on #755) that every dated surface is trimmed at one snapshot cutoff, so
marketlake #794 reuses this key for ``quotes/``. Chains go first, so the floor below is the
chains readers' until #794 adds the quotes readers'.

**Loading never judges the value.** ``config.load_config`` runs every capture cycle, so a
refusal there would stop capture on a typo. Loading stores an integer as read and any other
value as its ``repr``, the way ``role`` is stored, and each job judges it here: the render
in ``lake.vm_config`` refuses a bad value before it reaches the VM, and the sweep, the trim
and the restore check it again as a backstop.

**The floor is derived, never written down.** Every job that reads sealed chains on a host
sets a minimum on the window, and marketlake #786's body carries the table. Three set it:

1. ``battery.trailing_medians`` reads ``trailing_median_sessions`` sessions before the
   judged day, so it needs that many plus the day itself. The value can be overridden in
   ``config.yaml``, which is why the floor reads the loaded guards.
2. The dashboard's History panel reads :data:`HISTORY_WINDOW_DAYS` calendar days.
3. ``runway.assess`` reads the busiest day over ``runway.GROWTH_WINDOW_DAYS`` calendar days.

The other readers need less: ``battery._judge_drift`` reads the previous sealed day, and
compaction reads only the day it seals.

A span of calendar days becomes sessions by :func:`most_sessions_in`, the most weekdays any
span that long can hold. A holiday only removes a session, so no span of 30 days holds more
than 22, and a window of 22 covers every one of them.
"""

from __future__ import annotations

from datetime import date, timedelta

from lake.calendar import Calendar
from lake.config import LAKE_WINDOW_SESSIONS_KEY, GuardConstants
from lake.runway import GROWTH_WINDOW_DAYS

# The key, spelled once in ``lake.config`` beside the other keys. ``lake.vm_config``, the sweep,
# marketlake #787's trim and ``lake.bucket``'s restore read it.
WINDOW_KEY = LAKE_WINDOW_SESSIONS_KEY

# How far back the edge is looked for before it gives up, at the least. A calendar that answers
# no session at all, which a test's fake does outside the weeks it was built with, would
# otherwise hang the job. The search grows with the window, by :func:`edge_search_days`, so a
# long window is never cut off by a year: the NYSE holds 249 to 253 sessions in 366 days.
EDGE_SEARCH_DAYS = 366
# How many times the calendar days a window spans at five sessions a week the search allows.
# Three covers every holiday run the NYSE has had since 2006 several times over.
EDGE_SEARCH_MULTIPLE = 3

_WEEK_DAYS = 7
_WEEKDAYS = 5

# The History panel's width in calendar days, the same value as ``dashboard.HISTORY_WINDOW_DAYS``.
# It is restated here because importing ``lake.dashboard`` pulls DuckDB and the dashboard's whole
# import graph into the 18:30 sweep and the VM's config render, about 17.5 MiB, for one integer.
# ``tests/unit/test_window.py`` fails if the two drift. The dashboard can import it from here
# once its own file is free to change, which marketlake #786 leaves to the session that holds it.
HISTORY_WINDOW_DAYS = 30


class WindowRefused(Exception):
    """The window key holds a value no job may act on.

    The message names the key and the floor, never the value, for the reason ``lake.vm_config``
    gives for every line it prints. It holds no ``": "``, so ``report.redacted`` keeps it whole
    when the sweep files it.
    """


class EdgeNotFound(Exception):
    """No window edge lies within :func:`edge_search_days` of the night it was counted from."""


def most_sessions_in(days: int) -> int:
    """The most sessions any span of ``days`` consecutive calendar days can hold.

    A session falls on a weekday, so the answer is the most weekdays such a span holds: five
    for each whole week, and up to five more for the days left over. A holiday only takes one
    away, so no real span holds more.
    """
    weeks, rest = divmod(days, _WEEK_DAYS)
    return weeks * _WEEKDAYS + min(rest, _WEEKDAYS)


def window_floor(guards: GuardConstants) -> int:
    """The fewest sessions a window may keep, given the loaded guard constants.

    The module docstring names the three readers this is the largest of.
    """
    return max(
        guards.trailing_median_sessions + 1,
        most_sessions_in(HISTORY_WINDOW_DAYS),
        most_sessions_in(GROWTH_WINDOW_DAYS),
    )


def window_sessions(value: object, guards: GuardConstants) -> int | None:
    """The window a stored key value sets, ``None`` when the key is absent, or a refusal.

    ``value`` is ``Config.lake_window_sessions``: ``None`` for an absent key, an ``int`` as
    read, or the ``repr`` of anything else. A ``bool`` is refused even though Python counts it
    as an ``int``, because ``lake_window_sessions: true`` is a typo rather than a window.
    """
    if value is None:
        return None
    if type(value) is not int:
        raise WindowRefused(f"{WINDOW_KEY} is not a whole number of sessions")
    floor = window_floor(guards)
    if value < floor:
        raise WindowRefused(
            f"{WINDOW_KEY} is under the floor of {floor} sessions that the readers of sealed "
            "chains need, which are the battery's trailing median, the History panel and the "
            "disk runway. Raise it in config/vm.yaml"
        )
    return value


def edge_search_days(sessions: int) -> int:
    """How many days back :func:`window_edge` looks for ``sessions`` sessions before refusing.

    At least :data:`EDGE_SEARCH_DAYS`, and otherwise :data:`EDGE_SEARCH_MULTIPLE` times the
    calendar days ``sessions`` weekdays span.
    """
    span = -(-sessions * _WEEK_DAYS // _WEEKDAYS)
    return max(EDGE_SEARCH_DAYS, EDGE_SEARCH_MULTIPLE * span)


def window_edge(calendar: Calendar, tonight: date, sessions: int) -> date:
    """The window edge: the ``sessions``-th session counted back from ``tonight``, tonight first.

    After the next trim and seal the lake then holds exactly ``sessions`` chains sessions. A
    session the lake did not capture still takes a slot, because the count is the calendar's.
    Counting back needs no forward lookup, so it never meets the calendar's one-year bound.

    ``tonight`` need not be a session. A day that is not one takes no slot.

    Every way the count can fail is :class:`EdgeNotFound`: a calendar that answers no session,
    a window longer than the search, and a window reaching back past the calendar's first
    session or past ``date.min``.
    """
    if sessions < 1:
        raise ValueError(f"a window holds at least one session, not {sessions}")
    counted = 0
    day = tonight
    search = edge_search_days(sessions)
    for _ in range(search):
        # A calendar refuses a day before its first session with a ``ValueError``:
        # ``exchange_calendars`` raises ``DateOutOfBounds`` before 2006-10-09. A long enough
        # search also steps past ``date.min``. Either way the window reaches back further than
        # the calendar can count, which is this refusal rather than a raise that escapes the
        # sweep.
        refused = False
        try:
            answer = calendar.is_session(day)
        except ValueError:
            refused = True
        if refused:
            raise EdgeNotFound(
                f"only {counted} of {sessions} sessions before {day.isoformat()}, where the "
                "calendar's first session lies, so the window reaches back past it"
            )
        if answer:
            counted += 1
            if counted == sessions:
                return day
        if day == date.min:
            raise EdgeNotFound(
                f"only {counted} of {sessions} sessions back to the first day a date can "
                "name, so the window reaches back past the calendar's first session"
            )
        day -= timedelta(days=1)
    if counted == 0:
        raise EdgeNotFound(
            f"no session in the {search} days up to {tonight.isoformat()}, so the calendar "
            "answers no session there"
        )
    raise EdgeNotFound(
        f"only {counted} of {sessions} sessions in the {search} days up to "
        f"{tonight.isoformat()}, so the window is longer than the search reaches"
    )


__all__ = [
    "EDGE_SEARCH_DAYS",
    "EDGE_SEARCH_MULTIPLE",
    "HISTORY_WINDOW_DAYS",
    "EdgeNotFound",
    "WINDOW_KEY",
    "WindowRefused",
    "edge_search_days",
    "most_sessions_in",
    "window_edge",
    "window_floor",
    "window_sessions",
]
