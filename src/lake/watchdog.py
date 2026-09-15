"""The watchdog: one counter per ticker and surface, and the page each one owes.

A dead daemon is caught by the external dead-man switch, whose silence pages. This
catches the narrower thing that switch cannot see: a daemon that is alive and pinging
while one of its surfaces has stopped producing. A dead chain worker trips it while
that same ticker's quotes keep flowing, and a single dead ticker trips it on an
otherwise healthy daemon.

A counter counts session minutes without a durable data cycle for its own surface and
ticker. A durable data cycle resets it to zero. Gap rows are journaled and durable, but
they are not data, so a minute that produced only a gap still increments. That is the
whole point: a surface that fails every minute is producing rows and producing nothing.

Three consecutive minutes pages, once, on the transition. It stays silent after that
until a durable cycle resets the counter, and re-arms when one does. A flapping surface
can therefore page many times an hour, which is the honest signal rather than a
comfortable one.

One failure can take every surface down at once, such as a dead token or a rate
limit. The watchdog calls that a cause, pages it once under its own title, and then
suppresses the pages of every surface it named. That suppression ends one surface at a
time, and the cause re-arms only when the last of them is back. So a rate limit that
runs all session stays one condition, and one surface returning and dying again never
re-pages the cause.

One case collapses. Every quotes ticker shares one batched request, so all quotes
counters tripping in the same minute means the sampler died rather than N tickers
dying at once. That sends one page naming the sampler, never one page per ticker.

A stall folds too. A slot the loop slept through gaps every watched surface at the
same moment, so one overrun that trips the threshold is one fact and sends one page,
carrying the minutes without a durable cycle it is reporting and how many surfaces it
charged. That page leaves the per-surface budget alone, where the sampler collapse
spends it. A stall is evidence about the loop rather than about any surface's health,
so a surface that is genuinely dead still pages on its own account on the first cycle
after the loop resumes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

from lake.calendar import MARKET_TZ
from lake.capture import CycleResult
from lake.journal import QUOTES_SURFACE, ROW_KIND_DATA

# What the design pins as the page threshold, in consecutive session minutes without a
# durable data cycle. The value lives in ``GuardConstants.watchdog_page_minutes``; this
# names the meaning so a reader does not have to infer it from the number.
DEFAULT_PAGE_MINUTES = 3

# Failures that take the whole daemon down rather than one surface, and what to call
# them. Each gaps every ticker on every surface at once, so the per-surface fan-out
# would page the roster and name nothing. The design puts auth death and sustained rate
# limiting in this deliverable for exactly that reason.
_WHOLE_DAEMON_CAUSES = {
    "http_401": "Capture down: token dead",
    "http_403": "Capture down: token dead",
    # The other shape of a dead token. When the refresh fails no request goes out, so
    # there is no status to record and the vendor raises instead. ``schwab`` collapses
    # every such raise onto one class the lake owns, so this maps one string rather
    # than a list of the library's exception names.
    "vendor_auth_error": "Capture down: token dead",
    "http_429": "Capture down: rate limited",
}

# What one overrun pages under. The gap rows the same stall produces are stamped
# ``slot_overrun``, so operator and journal name the minute the same way.
_OVERRUN_TITLE = "Capture down: loop overran"


@dataclass(frozen=True)
class Surface:
    """One counter's identity: which ticker on which surface."""

    surface: str
    ticker: str

    def __str__(self) -> str:
        return f"{self.ticker} {self.surface}"


@dataclass(frozen=True)
class Page:
    """One page the watchdog owes, ready for a publisher.

    ``surfaces`` is what went quiet. It holds one entry for an ordinary page, every
    quotes ticker for a collapsed sampler page, every surface a cause named, and every
    surface a stall charged, so a caller can say what it saw without the watchdog
    formatting prose it may not want.

    ``cause`` is the class the failure arrived as, and it is what lets a body say why
    rather than only what. It is ``None`` where there is nothing to name: a slot the loop
    slept through attempted no request, a failure can be recorded without a class, and a
    collapsed sampler page whose tickers disagreed has no single class to pick.
    """

    title: str
    minutes: int
    surfaces: tuple[Surface, ...]
    sampler_collapse: bool = False
    cause: str | None = None


class Watchdog:
    """Counters for every ticker and surface, and the pages they raise.

    It is handed what happened and returns what should page. It sends nothing and reads
    no clock, so a caller decides both when to ask and where a page goes.

    ``page_minutes`` is the threshold, either a fixed count or a zero-argument callable.
    A callable is read at the moment of comparison, so a recalibrated
    ``watchdog_page_minutes`` takes effect on the next page decision rather than at the
    next restart. The running counters are left untouched when it changes.
    """

    def __init__(self, *, page_minutes: int | Callable[[], int] = DEFAULT_PAGE_MINUTES) -> None:
        self._page_minutes = page_minutes
        self._counts: dict[Surface, int] = {}
        self._day: date | None = None
        # A cause maps to the surfaces its page covers. A surface leaves that set when
        # it produces data, when it starts failing a way another cause names, or when
        # the roster drops it. A cause whose set empties is dropped, which re-arms it.
        self._paged_causes: dict[str, set[Surface]] = {}
        self._paged: set[Surface] = set()
        # Whether an overrun has already paged. It is the stall's own once-on-transition
        # flag, kept apart from ``_paged`` so a stall never spends a surface's budget.
        # A durable data cycle proves the loop is running again and re-arms it.
        self._paged_overrun = False

    def _threshold(self) -> int:
        """The page threshold as it stands now.

        A callable source is read here, at the moment of comparison, so a recalibrated
        value takes effect on the next decision. A fixed count is returned as given.
        """
        source = self._page_minutes
        return source() if callable(source) else source

    def count(self, surface: str, ticker: str) -> int:
        """The current count for one surface and ticker."""
        return self._counts.get(Surface(surface, ticker), 0)

    def observe(self, result: CycleResult) -> list[Page]:
        """Take one cycle's outcome and return the pages it raises.

        Every surface the cycle wrote a data row for resets. Every surface it wrote only
        a gap for fails, and so does every surface it could not journal at all, because
        an unwritten segment is the same absence as a failed one from the counter's side.
        """
        produced: set[Surface] = set()
        touched: set[Surface] = set()
        for segment in result.segments:
            key = Surface(segment.surface, segment.ticker)
            touched.add(key)
            if segment.row_kind == ROW_KIND_DATA:
                produced.add(key)
        for error in result.errors:
            touched.add(Surface(error.surface, error.ticker))
        self._roll(result.snap_ts)
        failed = touched - produced
        for key in produced:
            self._reset(key)
        for key in sorted(failed, key=str):
            self._counts[key] = self._counts.get(key, 0) + 1
        classes: dict[Surface, str | None] = {
            Surface(segment.surface, segment.ticker): segment.error_class
            for segment in result.segments
            if Surface(segment.surface, segment.ticker) in failed
        }
        # A segment that could not be written carries its own class, and that surface is
        # just as down, so its page names that class the same way.
        for error in result.errors:
            classes[Surface(error.surface, error.ticker)] = error.error_class
        self._release_retired(touched)
        threshold = self._threshold()
        cause = self._whole_daemon(result, failed, touched, threshold)
        if cause is not None:
            return cause
        return self._pages(failed, touched, threshold=threshold, classes=classes)

    def missed(self, surfaces: Iterable[Surface], slots: Sequence[datetime]) -> list[Page]:
        """Charge a run of slept-through slots, and page the overrun once.

        The loop never runs a cycle for a slot it slept through, so ``observe`` never
        sees those minutes, and they are exactly the ones the daemon was worst off. Each
        slot is its own session minute without a durable data cycle, so a ten-minute
        overrun advances a counter by ten rather than by one. The counting is per slot
        and per surface. Only the page is folded.

        One stall gaps every watched surface at the same moment, so it is one fact and
        owes one page. Fanning out instead sent a page per surface, which on a roster of
        about 115 tickers on two surfaces is 230 pages for one stall against a daily cap
        of 40. The page carries the minutes the surfaces it speaks for have gone without
        a durable cycle, which for a stall from a healthy roster is the run of slots the
        loop slept through, and how many surfaces the stall charged.

        What decides the page is the decision the per-surface path already makes: a
        counter at the threshold that has not paged yet and that no live cause speaks
        for. So a stall shorter than the threshold still pages nothing, a stall whose
        surfaces a live cause already speaks for adds nothing, and the threshold reads
        live here the way it does everywhere else. Only the fan-out is gone.

        The fold leaves ``_paged`` alone, and that is the load-bearing part. A stall says
        nothing about whether any one surface is healthy, so it must not spend the budget
        each surface has for its own page. A surface that is genuinely dead therefore
        pages under its own title on the first cycle after the loop resumes, and one that
        comes back pages not at all. Marking them instead would read as a fix and bury
        the real outage for the rest of the session.

        The stall has a once-on-transition rule of its own instead, held in
        ``_paged_overrun``. Without it a loop that never runs a cycle again pages on
        every tick it wakes on, since nothing it charges ever reaches ``_paged``. A
        durable data cycle proves the loop is running and re-arms it, and so does the
        session date.
        """
        if not slots:
            return []
        watched = list(surfaces)
        # One overrun is reported in a single call, so the threshold is read once for the
        # batch rather than per slot.
        threshold = self._threshold()
        for slot in sorted(slots):
            self._roll(slot)
            for key in sorted(watched, key=str):
                self._counts[key] = self._counts.get(key, 0) + 1
        if self._paged_overrun:
            return []
        # Nothing was attempted in these minutes, so no class is named and no surface is
        # asked what it is failing with. A cause already paged for a surface still speaks
        # for it, because a stall inside that outage is the same outage from a minute the
        # loop never ran.
        charged = tuple(sorted(set(watched), key=str))
        tripped = [
            key
            for key in charged
            if self._counts.get(key, 0) >= threshold
            and key not in self._paged
            and not self._covered(key, None)
        ]
        if not tripped:
            return []
        self._paged_overrun = True
        return [
            Page(
                title=_OVERRUN_TITLE,
                minutes=max(self._counts[key] for key in tripped),
                surfaces=charged,
            )
        ]

    def _whole_daemon(
        self, result: CycleResult, failed: set[Surface], touched: set[Surface], threshold: int
    ) -> list[Page] | None:
        """One page naming the cause, when every surface failed the same way.

        The refresh token dies every seven days by design, and a dead token gaps chains
        and quotes for every ticker at once. Left to the per-surface counters that sends
        one page per chains ticker plus a "quote sampler dead" page, none of which says
        the token is dead. The operator then reads a page storm and has to infer the one
        thing that actually broke.

        So a cycle where every surface failed with the same class is reported as that
        class, once. It suppresses the fan-out for that minute rather than adding to it,
        and the counters keep climbing underneath, so the surface pages resume by
        themselves if the cause turns out to be something else.

        Once means once per cause, and the cause is the title, not the error class that
        resolved to it. A dead refresh token has two shapes. ``schwab.VendorAuthError``
        records both: ``http_401`` while the cached access token still works, and
        ``vendor_auth_error`` once the refresh fails and no request goes out. One session
        carries both, so counting by class would page the same outage a second time under
        the same title when the vendor changed how it said no.
        """
        if not failed or failed != touched or len(touched) < 2:
            return None
        classes = {
            segment.error_class for segment in result.segments if segment.error_class is not None
        }
        if len(classes) != 1:
            return None
        error_class = classes.pop()
        title = _WHOLE_DAEMON_CAUSES.get(error_class)
        if title is None:
            return None
        if title in self._paged_causes:
            return []
        if any(self._counts.get(key, 0) < threshold for key in failed):
            return None
        self._paged_causes[title] = set(failed)
        return [
            Page(
                title=title,
                minutes=max(self._counts[key] for key in failed),
                surfaces=tuple(sorted(failed, key=str)),
                cause=error_class,
            )
        ]

    # -- the counting ----------------------------------------------------------

    def _roll(self, slot: datetime) -> None:
        """Drop every counter when the session date changes.

        A counter measures consecutive session minutes. Carrying one overnight would let
        a surface sitting at 2 at the close page on the next session's first bad minute
        while claiming three minutes, when eighteen hours passed. A ``_paged`` flag
        carried the same way would silence a genuine page all the next morning, and so
        would a stall flag, so both are dropped here too.
        """
        day = slot.astimezone(MARKET_TZ).date()
        if day != self._day:
            self._day = day
            self._counts.clear()
            self._paged.clear()
            self._paged_causes.clear()
            self._paged_overrun = False

    def _reset(self, key: Surface) -> None:
        self._counts[key] = 0
        self._paged.discard(key)
        # A durable cycle landed, so the loop is running. A later stall is a new stall
        # and pages again.
        self._paged_overrun = False
        # This surface is back, so no cause covers it now. A cause that named others is
        # still true of them and stays live until the last one returns.
        self._release(key)

    def _release(self, key: Surface) -> None:
        """Take one surface out of the causes covering it, dropping one that empties.

        A cause with no surfaces left has nothing to explain, so dropping it re-arms it.
        Only two things bring a surface here: it produced data, or the roster dropped it.
        A surface that merely started failing another way is still down, so the cause
        that named it has not lifted and keeps it.
        """
        for title in list(self._paged_causes):
            held = self._paged_causes[title]
            if key not in held:
                continue
            held.discard(key)
            if not held:
                del self._paged_causes[title]

    def _release_retired(self, touched: set[Surface]) -> None:
        """Stop covering a surface the roster has dropped.

        A ticker retired mid-session stops appearing in cycles, which is a supported
        path. Nothing about that surface ever changes again, so a cause that kept
        covering one would never re-arm, and the next genuine outage under the same
        title would page nobody for the rest of the session.

        A cycle that touched nothing at all is an empty roster rather than a retired
        one. It is evidence about no surface, so it releases none.
        """
        if not self._paged_causes or not touched:
            return
        for key in {key for held in self._paged_causes.values() for key in held}:
            if key not in touched:
                self._release(key)

    def _covered(self, key: Surface, title: str | None) -> bool:
        """Whether a live cause speaks for how this surface is failing right now.

        ``title`` is the cause this minute's failure resolves to, or ``None`` when it
        resolves to no cause and when nothing was attempted. A cause covers the surface
        it named while that surface keeps failing its way, and an ordinary transient
        failure counts as still covered. The one thing that lifts the cover is the
        surface failing a way some other cause names, because that is a different outage
        with a different remedy, and the operator has to hear it.
        """
        return any(
            key in held and title in (None, cause) for cause, held in self._paged_causes.items()
        )

    def _pages(
        self,
        failed: set[Surface],
        watched: set[Surface],
        *,
        threshold: int,
        classes: dict[Surface, str | None],
    ) -> list[Page]:
        """The pages this minute owes, collapsing a dead sampler into one.

        The collapse is decided by what failed this minute, not by what newly tripped. A
        ticker that started failing earlier is already in ``_paged`` and would drop out
        of a newly-tripped set, which would break the parity and send one page per
        ticker at the moment the shared request died.

        It applies only where a request was actually attempted, and ``observe`` is the
        only caller for that reason. A slot the loop slept through gaps every quotes
        ticker too, and calling that a dead sampler would name a batched request nobody
        made. ``missed`` folds its own page instead and never arrives here.
        """
        tripped = [
            key
            for key in sorted(failed, key=str)
            if self._counts.get(key, 0) >= threshold
            and key not in self._paged
            and not self._covered(key, _WHOLE_DAEMON_CAUSES.get(classes.get(key)))
        ]
        if not tripped:
            return []
        quotes_failed = {key for key in failed if key.surface == QUOTES_SURFACE}
        quotes_watched = {key for key in watched if key.surface == QUOTES_SURFACE}
        collapsed = (
            len(quotes_watched) > 1
            and quotes_failed == quotes_watched
            and any(key.surface == QUOTES_SURFACE for key in tripped)
        )
        self._paged.update(tripped)
        pages: list[Page] = []
        if collapsed:
            # One batched request died, so in practice every collapsed ticker reports the
            # same class. Naming one of several would pick a winner arbitrarily, so a
            # disagreement names none.
            shared = {classes.get(key) for key in quotes_failed}
            pages.append(
                Page(
                    title="Capture down: quote sampler dead",
                    minutes=max(self._counts[key] for key in quotes_failed),
                    surfaces=tuple(sorted(quotes_failed, key=str)),
                    sampler_collapse=True,
                    cause=shared.pop() if len(shared) == 1 else None,
                )
            )
        for key in tripped:
            if collapsed and key.surface == QUOTES_SURFACE:
                continue
            pages.append(
                Page(
                    title=f"Capture down: {key}",
                    minutes=self._counts[key],
                    surfaces=(key,),
                    cause=classes.get(key),
                )
            )
        return pages


__all__ = [
    "DEFAULT_PAGE_MINUTES",
    "Page",
    "Surface",
    "Watchdog",
]
