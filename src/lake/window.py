"""The lake window: how many recent sessions a host keeps, and the edge that count sets.

Marketlake #755 keeps only a window of recent sessions on the hosted VM's lake volume and
leaves older partitions in the backup bucket. One config key, ``lake_window_sessions``, is
the opt-in, and its window is the one every surface takes unless that surface's own key
overrides it. A host without it never trims, writes no split checkpoint, and restores the whole
lake from the bucket, which is the laptop today. Besides the render in ``lake.vm_config``,
which checks it, three jobs act on it.

1. The 18:30 sweep writes the split checkpoint, which marketlake #786 added with the key.
2. The trim removes what falls outside its surface's window: chains since marketlake #787,
   quotes under a landed daily bar since marketlake #794, and bars once their day's chains are
   gone since marketlake #795.
3. ``lake.bucket``'s restore rebuilds the trimmed lake rather than the whole one, leaving out
   each partition the bucket's ``trimmed.jsonl`` says was removed on purpose (marketlake
   #785).

The key is named for the lake rather than for chains. The owner decided on 2026-10-07
(decision 9 on #755) that every dated surface is trimmed at one snapshot cutoff, and
marketlake #794 reuses this key for ``quotes/`` and marketlake #795 for ``bars/``. A quotes
day goes only under a landed daily bar, so a day whose bar never lands outlives the cutoff for
good. A bar goes only once its day's chains are gone, so it never leaves before the chains it
is read beside.

**Each surface can keep a window of its own** (marketlake #953, from the owner's ruling of
2026-10-29 on #938), so decision 9's one cutoff now holds for the chains window, which the split
checkpoint is cut at. ``lake_window_sessions_chains``, ``lake_window_sessions_quotes`` and
``lake_window_sessions_bars`` each override ``lake_window_sessions`` for one surface, and each
one left out takes ``lake_window_sessions``'s value, absence included, never another surface's.
:func:`surface_windows` is the one place that rule lives. Only ``lake_window_sessions`` turns
trimming on, so a per-surface key on a host without it does nothing, and the render refuses
it. The quotes and bars keys may also say ``all``, which keeps every session of that surface
and counts no edge, because a large number cannot say it: :func:`window_edge` refuses a window
that reaches back past the calendar's first session. ``all`` is refused on the chains key and
on ``lake_window_sessions``, because the split checkpoint is cut at the chains window's edge,
and with no edge the sweep writes no checkpoint and the trim refuses every night. The quotes
day's daily-bar clause and the bars' chains clause hold whatever the windows are.

**Loading never judges the value.** ``config.load_config`` runs every capture cycle, so a
refusal there would stop capture on a typo. Loading stores an integer as read and any other
value as its ``repr``, the way ``role`` is stored, and each job judges it here: the render
in ``lake.vm_config`` refuses a bad value before it reaches the VM, and the sweep, the trim
and the restore check it again as a backstop.

**The floor is derived, never written down.** Every job that reads sealed chains or quotes
on a host sets a minimum on the window, and marketlake #786's body carries the table. Bars have
no reader of their own that needs one, since on the host only the settlement and continuity
views read bars, beside chains, and the chains clause keeps those bars as long as their chains.
A numeric bars window keeps the same floor anyway, as a guard against a typo, and because one
rule for every numeric window is simpler than two. Three readers set it:

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

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Final

from lake.calendar import Calendar
from lake.config import (
    LAKE_WINDOW_SESSIONS_BARS_KEY,
    LAKE_WINDOW_SESSIONS_CHAINS_KEY,
    LAKE_WINDOW_SESSIONS_KEY,
    LAKE_WINDOW_SESSIONS_QUOTES_KEY,
    Config,
    GuardConstants,
)
from lake.paths import BARS, CHAINS, QUOTES
from lake.runway import GROWTH_WINDOW_DAYS

# The key, spelled once in ``lake.config`` beside the other keys. ``lake.vm_config``, the sweep,
# marketlake #787's trim and ``lake.bucket``'s restore read it.
WINDOW_KEY = LAKE_WINDOW_SESSIONS_KEY

# Each surface the trim drops, and the key that gives it a window of its own (marketlake #953).
SURFACE_KEYS: Final[Mapping[str, str]] = {
    CHAINS: LAKE_WINDOW_SESSIONS_CHAINS_KEY,
    QUOTES: LAKE_WINDOW_SESSIONS_QUOTES_KEY,
    BARS: LAKE_WINDOW_SESSIONS_BARS_KEY,
}
# The four keys in the order their refusals are filed, ``lake_window_sessions`` first.
WINDOW_KEYS = (WINDOW_KEY, *SURFACE_KEYS.values())
# The keys that may say ``all``. The chains key may not, for the reason the module docstring
# gives.
KEEP_KEYS = frozenset({LAKE_WINDOW_SESSIONS_QUOTES_KEY, LAKE_WINDOW_SESSIONS_BARS_KEY})
# How ``config._window_value`` stores ``all``, as its ``repr``. Only lower-case ``all`` keeps
# every session, so ``ALL``, ``All`` and a blank value are refused.
STORED_ALL = repr("all")


class _Keep(Enum):
    """The type of :data:`KEEP_EVERY_SESSION`, an ``Enum`` so it compares by identity."""

    EVERY_SESSION = "all"


# What :func:`window_sessions` answers for ``all`` on the quotes or bars key.
KEEP_EVERY_SESSION: Final = _Keep.EVERY_SESSION

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


def window_sessions(
    value: object, guards: GuardConstants, *, key: str = WINDOW_KEY
) -> int | _Keep | None:
    """The window a stored key value sets, ``None`` when the key is absent, or a refusal.

    ``value`` is a key's value as ``config._window_value`` stored it: ``None`` for an absent
    key, an ``int`` as read, or the ``repr`` of anything else. ``key`` names the key, and every
    refusal names it in turn. A ``bool`` is refused even though Python counts it as an ``int``,
    because ``lake_window_sessions: true`` is a typo rather than a window.

    ``all`` on the quotes or bars key answers :data:`KEEP_EVERY_SESSION`. It is tested before
    the integer check, which would refuse it as text, and it is refused on any other key.
    """
    if value is None:
        return None
    keeps = key in KEEP_KEYS
    if value == STORED_ALL:
        if keeps:
            return KEEP_EVERY_SESSION
        raise WindowRefused(
            f"{key} keeps every session, which only {LAKE_WINDOW_SESSIONS_BARS_KEY} and "
            f"{LAKE_WINDOW_SESSIONS_QUOTES_KEY} may, because the split checkpoint is counted "
            "from the chains window. Set a whole number in config/vm.yaml"
        )
    if type(value) is not int:
        if keeps:
            raise WindowRefused(f"{key} is not a whole number of sessions or all")
        raise WindowRefused(f"{key} is not a whole number of sessions")
    floor = window_floor(guards)
    if value < floor:
        if key == LAKE_WINDOW_SESSIONS_BARS_KEY:
            # No reader of sealed bars needs the floor, so the reasons the chains and quotes
            # message gives would be false here.
            raise WindowRefused(
                f"{key} is under the floor of {floor} sessions that the chains and quotes "
                "windows share. Raise it in config/vm.yaml"
            )
        raise WindowRefused(
            f"{key} is under the floor of {floor} sessions that the readers of sealed "
            "chains and quotes need, which are the battery's trailing median, the History "
            "panel and the disk runway. Raise it in config/vm.yaml"
        )
    return value


@dataclass(frozen=True)
class SurfaceWindows:
    """What the four window keys set, judged together by :func:`surface_windows`.

    ``windows`` maps each surface, ``chains``, ``quotes`` or ``bars``, to its window in
    sessions, or to ``None`` when the surface keeps every session. A surface whose window was
    refused is absent from it, so ``windows.get("bars") is None`` cannot tell a kept surface
    from a refused one.

    ``refused`` maps each present key the judge refused to its message, in the order
    ``lake_window_sessions``, chains, quotes, bars. An absent per-surface key takes
    ``lake_window_sessions``'s value and verdict and adds no entry of its own, so one bad
    ``lake_window_sessions`` is one entry.

    ``run_refusal`` is set when the refusal is the whole run's, which it is for
    ``lake_window_sessions`` or the chains key, base first. The trim and the split checkpoint
    are both cut at the chains window's edge, so without a chains window neither may act.

    ``sources`` maps each surface in ``windows`` to the key its window was read from, which is
    ``lake_window_sessions`` for a surface that inherited it.
    """

    windows: Mapping[str, int | None] = field(default_factory=dict)
    refused: Mapping[str, str] = field(default_factory=dict)
    run_refusal: str | None = None
    sources: Mapping[str, str] = field(default_factory=dict)


def surface_windows(stored: Mapping[str, object], guards: GuardConstants) -> SurfaceWindows | None:
    """Judge the four window keys together, or ``None`` when ``lake_window_sessions`` is absent.

    ``stored`` maps each key in :data:`WINDOW_KEYS` to its value as ``config._window_value``
    stored it, and a key missing from it counts as absent. With ``lake_window_sessions`` absent
    the host trims nothing, whatever the per-surface keys hold, and none of them is judged.
    Otherwise each per-surface key, when present, is judged under its own name, and when absent
    takes ``lake_window_sessions``'s window, or nothing when that was refused. No calendar is
    read here, so an edge that cannot be counted is the trim's and the sweep's to report.
    """
    base_value = stored.get(WINDOW_KEY)
    if base_value is None:
        return None
    refused: dict[str, str] = {}
    windows: dict[str, int | None] = {}
    sources: dict[str, str] = {}
    base: int | None = None
    try:
        judged = window_sessions(base_value, guards, key=WINDOW_KEY)
    except WindowRefused as exc:
        refused[WINDOW_KEY] = str(exc)
    else:
        # ``all`` is refused on this key, so a verdict that is not refused is a number.
        assert isinstance(judged, int)
        base = judged
    for surface, key in SURFACE_KEYS.items():
        value = stored.get(key)
        if value is None:
            if base is not None:
                windows[surface] = base
                sources[surface] = WINDOW_KEY
            continue
        try:
            verdict = window_sessions(value, guards, key=key)
        except WindowRefused as exc:
            refused[key] = str(exc)
            continue
        windows[surface] = None if verdict is KEEP_EVERY_SESSION else verdict
        sources[surface] = key
    run_refusal = refused.get(WINDOW_KEY, refused.get(LAKE_WINDOW_SESSIONS_CHAINS_KEY))
    return SurfaceWindows(
        windows=windows, refused=refused, run_refusal=run_refusal, sources=sources
    )


def stored_windows(config: Config) -> dict[str, object]:
    """The four window keys' stored values from a loaded config, keyed by key name."""
    return {
        WINDOW_KEY: config.lake_window_sessions,
        LAKE_WINDOW_SESSIONS_CHAINS_KEY: config.lake_window_sessions_chains,
        LAKE_WINDOW_SESSIONS_QUOTES_KEY: config.lake_window_sessions_quotes,
        LAKE_WINDOW_SESSIONS_BARS_KEY: config.lake_window_sessions_bars,
    }


def edge_search_days(sessions: int) -> int:
    """How many days back :func:`window_edge` looks for ``sessions`` sessions before refusing.

    At least :data:`EDGE_SEARCH_DAYS`, and otherwise :data:`EDGE_SEARCH_MULTIPLE` times the
    calendar days ``sessions`` weekdays span.
    """
    span = -(-sessions * _WEEK_DAYS // _WEEKDAYS)
    return max(EDGE_SEARCH_DAYS, EDGE_SEARCH_MULTIPLE * span)


def window_edge(calendar: Calendar, tonight: date, sessions: int) -> date:
    """The window edge: the ``sessions``-th session counted back from ``tonight``, tonight first.

    After the next trim and seal the lake then holds exactly ``sessions`` chains sessions, and
    as many quotes sessions plus any quotes day kept for good because its daily bar never
    landed. A session the lake did not capture still takes a slot, because the count is the
    calendar's.
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
        # ``exchange_calendars`` raises ``DateOutOfBounds`` before its first session, which
        # lies 20 years before today and moves daily. A long enough search also steps past
        # ``date.min``. Either way the window reaches back further than
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
    "KEEP_EVERY_SESSION",
    "KEEP_KEYS",
    "STORED_ALL",
    "SURFACE_KEYS",
    "WINDOW_KEY",
    "WINDOW_KEYS",
    "EdgeNotFound",
    "SurfaceWindows",
    "WindowRefused",
    "edge_search_days",
    "most_sessions_in",
    "stored_windows",
    "surface_windows",
    "window_edge",
    "window_floor",
    "window_sessions",
]
