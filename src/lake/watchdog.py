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

One case collapses. Every quotes ticker shares one batched request, so all quotes
counters tripping in the same minute means the sampler died rather than N tickers
dying at once. That sends one page naming the sampler, never one page per ticker.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
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

    ``surfaces`` is what went quiet. It holds one entry for an ordinary page and every
    quotes ticker for a collapsed sampler page, so a caller can say what it saw without
    the watchdog formatting prose it may not want.
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
    """

    def __init__(self, *, page_minutes: int = DEFAULT_PAGE_MINUTES) -> None:
        self._page_minutes = page_minutes
        self._counts: dict[Surface, int] = {}
        self._day: date | None = None
        self._paged_causes: set[str] = set()
        self._paged: set[Surface] = set()

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
        cause = self._whole_daemon(result, failed, touched)
        if cause is not None:
            return cause
        return self._pages(failed, touched, attempted=True)

    def missed(self, surfaces: Iterable[Surface], slots: Sequence[datetime]) -> list[Page]:
        """Charge a run of slept-through slots, one increment per slot.

        The loop never runs a cycle for a slot it slept through, so ``observe`` never
        sees those minutes, and they are exactly the ones the daemon was worst off. Each
        slot is its own session minute without a durable data cycle, so a ten-minute
        overrun advances a counter by ten rather than by one.
        """
        watched = list(surfaces)
        pages: list[Page] = []
        for slot in sorted(slots):
            self._roll(slot)
            for key in sorted(watched, key=str):
                self._counts[key] = self._counts.get(key, 0) + 1
            # Nothing was attempted for these minutes, so a quotes fan-out here says the
            # loop overran rather than that the shared request failed.
            pages.extend(self._pages(set(watched), set(watched), attempted=False))
        return pages

    def _whole_daemon(
        self, result: CycleResult, failed: set[Surface], touched: set[Surface]
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
        if error_class in self._paged_causes:
            return []
        if any(self._counts.get(key, 0) < self._page_minutes for key in failed):
            return None
        self._paged_causes.add(error_class)
        self._paged.update(failed)
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
        carried the same way would silence a genuine page all the next morning.
        """
        day = slot.astimezone(MARKET_TZ).date()
        if day != self._day:
            self._day = day
            self._counts.clear()
            self._paged.clear()
            self._paged_causes.clear()

    def _reset(self, key: Surface) -> None:
        self._counts[key] = 0
        self._paged.discard(key)
        # A surface producing again means whatever took the whole daemon down has
        # lifted, so the cause re-arms with the counters.
        self._paged_causes.clear()

    def _pages(self, failed: set[Surface], watched: set[Surface], *, attempted: bool) -> list[Page]:
        """The pages this minute owes, collapsing a dead sampler into one.

        The collapse is decided by what failed this minute, not by what newly tripped. A
        ticker that started failing earlier is already in ``_paged`` and would drop out
        of a newly-tripped set, which would break the parity and send one page per
        ticker at the moment the shared request died.

        It applies only where a request was actually attempted. A slot the loop slept
        through fans out across every quotes ticker too, and calling that a dead sampler
        would name the wrong cause.
        """
        tripped = [
            key
            for key in sorted(failed, key=str)
            if self._counts.get(key, 0) >= self._page_minutes and key not in self._paged
        ]
        if not tripped:
            return []
        quotes_failed = {key for key in failed if key.surface == QUOTES_SURFACE}
        quotes_watched = {key for key in watched if key.surface == QUOTES_SURFACE}
        collapsed = (
            attempted
            and len(quotes_watched) > 1
            and quotes_failed == quotes_watched
            and any(key.surface == QUOTES_SURFACE for key in tripped)
        )
        self._paged.update(tripped)
        pages: list[Page] = []
        if collapsed:
            pages.append(
                Page(
                    title="Capture down: quote sampler dead",
                    minutes=max(self._counts[key] for key in quotes_failed),
                    surfaces=tuple(sorted(quotes_failed, key=str)),
                    sampler_collapse=True,
                )
            )
        for key in tripped:
            if collapsed and key.surface == QUOTES_SURFACE:
                continue
            pages.append(
                Page(title=f"Capture down: {key}", minutes=self._counts[key], surfaces=(key,))
            )
        return pages


__all__ = [
    "DEFAULT_PAGE_MINUTES",
    "Page",
    "Surface",
    "Watchdog",
]
