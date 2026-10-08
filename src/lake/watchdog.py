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
A durable data cycle is one that landed a data row, so a chain segment carrying only
absence markers, or nothing at all, increments too, and fails as ``contracts_absent``
(marketlake #326).

Three consecutive minutes pages, once, on the transition. It stays silent after that
until a durable cycle resets the counter or the surface leaves the cycle, and re-arms when
either happens. A flapping surface
can therefore page many times an hour, which is the honest signal rather than a
comfortable one.

One failure can take every surface down at once, such as a dead token or a rate
limit. The watchdog calls that a cause, pages it once under its own title, and then
suppresses the pages of every surface it named. That suppression ends one surface at a
time, and the cause re-arms only when the last of them has gone. A surface goes when any
of four things happens.

1. It produces data again.
2. It answers with no contract.
3. Its segment could not be written in a minute another surface landed data.
4. It leaves the roster.

So a rate limit that runs all session stays one condition, and one surface returning and
dying again never re-pages the cause.

One case collapses. Every quotes ticker shares one batched request, so all quotes
counters tripping in the same minute means the sampler died rather than N tickers
dying at once. That sends one page naming the sampler, never one page per ticker.

A stall folds too. A slot the loop slept through gaps every watched surface at the
same moment, so one stall that trips the threshold is one fact and sends one page,
carrying the minutes without a durable cycle it is reporting and how many surfaces it
charged. That page leaves the per-surface budget alone, where the sampler collapse
spends it. A stall is evidence about the loop rather than about any surface's health,
so a surface that is genuinely dead still pages on its own account on the first cycle
after the loop resumes.

A ticker the capture spans leave out is counted on its own. The cycle names it in
``CycleResult.out_of_span`` and fetches nothing for it, so none of its surfaces is ever
touched and no surface counter can see it. Each ticker gets a count of consecutive cycles
spent out of span instead, and the threshold pages once, with one page naming every
ticker left out, rather than one page per surface. It is held while every enabled ticker
is left out, because the dead-man already pages that case (marketlake #554, #570).

A surface that leaves the cycle takes its counter with it. A counter frozen where it
stood would climb again under a stall, which charges the whole enabled roster, and would
page on the first failure after the ticker came back, claiming three minutes after an
hour away.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from lake.calendar import MARKET_TZ
from lake.capture import TOKEN_FILE_UNREADABLE, CycleResult, SegmentOutcome
from lake.journal import QUOTES_SURFACE, ROW_KIND_DATA

# What the design pins as the page threshold, in consecutive session minutes without a
# durable data cycle. The value lives in ``GuardConstants.watchdog_page_minutes``; this
# names the meaning so a reader does not have to infer it from the number.
DEFAULT_PAGE_MINUTES = 3

# Failures that take the whole daemon down rather than one surface, and what to call
# them. Each gaps every ticker on every surface at once, so the per-surface fan-out
# would page the roster and name nothing. The design puts auth death and sustained rate
# limiting in this deliverable for exactly that reason.
TOKEN_DEAD = "Capture down: token dead"

_WHOLE_DAEMON_CAUSES = {
    "http_401": TOKEN_DEAD,
    "http_403": TOKEN_DEAD,
    # The other shape of a dead token. When the refresh fails no request goes out, so
    # there is no status to record and the vendor raises instead. ``schwab`` collapses
    # every such raise onto one class the lake owns, so this maps one string rather
    # than a list of the library's exception names.
    "vendor_auth_error": TOKEN_DEAD,
    # A token file the cycle could not read at all: missing, unreadable, or not a token
    # (marketlake #702). The page's body names the class, which tells this apart from a
    # token Schwab refused, and a pull from the token parameter repairs either.
    TOKEN_FILE_UNREADABLE: TOKEN_DEAD,
    "http_429": "Capture down: rate limited",
}

# What one stall of the loop thread pages under. A slow request no longer causes one,
# since each minute's cycle runs on a thread of its own (marketlake #565), so the title
# names the stall rather than an overrun. The gap rows the same stall produces keep the
# class ``slot_overrun``. They are data in the lake, and renaming a class would split one
# reason across two spellings.
_OVERRUN_TITLE = "Capture down: loop stalled"

# What enabled tickers the capture spans leave out page under, one page for all of them.
_OUT_OF_SPAN_TITLE = "Capture down: tickers outside every capture span"

# The reason a data segment holding no data row fails with. The segment records none of
# its own, or records the first failed window's class, so the watchdog derives this one
# and writes it nowhere. It names what is missing, the way ``quote_absent`` does.
CONTRACTS_ABSENT = "contracts_absent"


def _failure_class(segment: SegmentOutcome) -> str | None:
    """The class a segment that produced nothing is failing with.

    A gap segment's is the one it recorded. A data segment that produced nothing answered
    and brought no contract, and that answer is the reason, over any class the segment
    recorded. A chain whose one window answered 200 with empty maps while another window
    failed records the failed window's class, and taking it would let a 401 on one window
    fold a surface that authenticated under a dead token. The failed window's own reason
    is not lost, because its absence-marker rows carry it. Only a chain reaches this, since
    a quotes data segment is one quote row.
    """
    if segment.row_kind == ROW_KIND_DATA:
        return CONTRACTS_ABSENT
    return segment.error_class


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

    ``tickers`` is set only on the out-of-span page, and names every enabled ticker the
    spans leave out, in roster order. The cycle knows those tickers and not which surfaces
    they owe, so that page leaves ``surfaces`` empty. It names no class either, because
    nothing was attempted for them.

    ``since`` is set only on a cause page, and is the first minute of the run ``minutes``
    counts, in the slot's own ET zone. A cause page fires only on a cycle in which every
    surface failed, so it is the one page that always rides a minute feeding the
    ``capture`` dead-man nothing. The body dates it and promises that check's DOWN on the
    strength of this field, so no other page sets it (marketlake #747).
    """

    title: str
    minutes: int
    surfaces: tuple[Surface, ...]
    sampler_collapse: bool = False
    cause: str | None = None
    tickers: tuple[str, ...] = ()
    since: datetime | None = None


@dataclass(frozen=True)
class _Tally:
    """One cycle read the way every counter reads it.

    ``touched`` is every surface the cycle wrote a segment for or could not journal.
    ``failed`` is the touched surfaces that landed no data row. ``recorded`` is the class
    each failed segment is failing with, per :func:`_failure_class`. A surface whose
    segment could not be written is failed and touched but has no entry there, because a
    write failure says nothing about what the vendor did.
    """

    touched: frozenset[Surface]
    failed: frozenset[Surface]
    recorded: dict[Surface, str | None]


def _tally(result: CycleResult) -> _Tally:
    """Read one cycle into what it touched, what failed, and the class each failure recorded."""
    produced: set[Surface] = set()
    touched: set[Surface] = set()
    for segment in result.segments:
        key = Surface(segment.surface, segment.ticker)
        touched.add(key)
        if segment.landed_data:
            produced.add(key)
    for error in result.errors:
        touched.add(Surface(error.surface, error.ticker))
    failed = touched - produced
    recorded: dict[Surface, str | None] = {
        Surface(segment.surface, segment.ticker): _failure_class(segment)
        for segment in result.segments
        if Surface(segment.surface, segment.ticker) in failed
    }
    return _Tally(frozenset(touched), frozenset(failed), recorded)


def _whole_daemon_title(tally: _Tally) -> str | None:
    """The cause's title when the tally is one whole-daemon failure, and ``None`` otherwise."""
    if not tally.failed or tally.failed != tally.touched or len(tally.touched) < 2:
        return None
    classes = {error_class for error_class in tally.recorded.values() if error_class is not None}
    if len(classes) != 1:
        return None
    return _WHOLE_DAEMON_CAUSES.get(classes.pop())


def whole_daemon_cause(result: CycleResult) -> str | None:
    """The title of the one cause that took this whole cycle down, or ``None``.

    Three things have to hold.

    1. The cycle touched at least two surfaces, because one surface cannot be the whole
       daemon by itself.
    2. Every touched surface failed, because one surface landing rows proves the vendor
       answered that minute.
    3. The classes the failed segments recorded come to exactly one, and
       ``_WHOLE_DAEMON_CAUSES`` names it.

    A chain that answered with no contract counts as ``contracts_absent`` and breaks the
    unanimity. A surface whose segment could not be written records no class and does not.

    This reads one cycle and no threshold, so it says what the cycle was rather than
    whether a page is owed. The watchdog's page decision reads it, and so does the daemon,
    which spawns a token pull on a cycle whose cause is ``TOKEN_DEAD`` without waiting for
    the page (marketlake #702). One rule serving both keeps the pull and the page from
    disagreeing about which cycles are a dead token.
    """
    return _whole_daemon_title(_tally(result))


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
        # it produces data, when it answers with no contract, when its segment could not
        # be written in a minute another surface landed data, or when the roster drops
        # it, per ``_release``. A cause whose set empties is dropped, which re-arms it.
        self._paged_causes: dict[str, set[Surface]] = {}
        self._paged: set[Surface] = set()
        # Whether an overrun has already paged. It is the stall's own once-on-transition
        # flag, kept apart from ``_paged`` so a stall never spends a surface's budget.
        # A durable data cycle proves the loop is running again and re-arms it.
        self._paged_overrun = False
        # Consecutive observed cycles each enabled ticker has spent outside every capture
        # span, and which of them have paged. Kept per ticker, because the cycle names
        # tickers there and not their surfaces, and apart from ``_paged``, because a clamp
        # says nothing about any surface's health.
        self._out_of_span: dict[str, int] = {}
        self._paged_out_of_span: set[str] = set()
        # Consecutive session minutes in which no surface landed data, which is what a
        # cause page waits on and dates itself from, per ``_whole_daemon``. A minute that
        # touched a surface and landed nothing adds one, and so does a slept-through slot
        # that charged a surface. Landed data resets it, and so do a cycle that touched
        # nothing and a slept-through slot that charged nothing, because counting a clamped
        # stretch paged the cause on the first minute the span opened, dated inside the
        # clamp (marketlake #768).
        self._minutes_without_data = 0

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
        A data segment holding no data row fails too, as ``contracts_absent``, per
        :func:`_failure_class`, and leaves every cause that named it, per :meth:`_release`.
        A surface whose segment could not be written leaves every cause that named it too,
        but only in a minute another surface landed data, per :meth:`_release`.
        A surface it did not touch at all has left the cycle and loses its counter, per
        :meth:`_drop_departed`.

        The tickers the spans left out are counted apart, and their page rides beside
        whatever else this minute owes, a cause page included, because a clamp and a dead
        token have different repairs.
        """
        tally = _tally(result)
        touched = set(tally.touched)
        failed = set(tally.failed)
        recorded = tally.recorded
        self._roll(result.snap_ts)
        # After ``_roll``, so the first cycle of a session counts toward that session.
        if touched and touched == failed:
            self._minutes_without_data += 1
        else:
            self._minutes_without_data = 0
        self._drop_departed(touched, result.out_of_span)
        for key in touched - failed:
            self._reset(key)
        for key in sorted(failed, key=str):
            self._counts[key] = self._counts.get(key, 0) + 1
        # A surface that answered and brought nothing is not failing for any cause's
        # reason, so it leaves every cause that named it, before any cause is asked
        # whether it is still live this minute.
        for key, error_class in recorded.items():
            if error_class == CONTRACTS_ABSENT:
                self._release(key)
        classes = dict(recorded)
        # A segment that could not be written carries its own class, and that surface is
        # just as down, so its page names that class the same way.
        for error in result.errors:
            classes[Surface(error.surface, error.ticker)] = error.error_class
        self._release_retired(touched)
        # A write failure records no class, so nothing about it says whether the cause is
        # over. Data landing on another surface says the vendor answered this minute, so
        # the write failure is the surface's own, and it leaves the cause and pages for
        # itself. Only landed data counts. It resets the count of minutes in which no
        # surface landed data, so the cause cannot page again until the threshold's minutes
        # pass with no surface landing data. An answer with no contract resets nothing, and
        # counting it let a chain alternating a 401 with an empty answer re-page the cause
        # every other minute (marketlake #754).
        if touched - failed:
            for key in failed - recorded.keys():
                self._release(key)
        threshold = self._threshold()
        out_of_span = self._out_of_span_pages(result, threshold)
        cause = self._whole_daemon(tally, threshold, result.snap_ts)
        if cause is not None:
            return cause + out_of_span
        return self._pages(failed, touched, threshold=threshold, classes=classes) + out_of_span

    def missed(self, surfaces: Iterable[Surface], slots: Sequence[datetime]) -> list[Page]:
        """Charge a run of slept-through slots, and page the overrun once.

        The loop never runs a cycle for a slot it slept through, so ``observe`` never
        sees those minutes, and they are exactly the ones the daemon was worst off. Each
        slot is its own session minute without a durable data cycle, so a ten-minute
        overrun advances a counter by ten rather than by one. The counting is per slot
        and per surface. Only the page is folded.

        Each slot that charges at least one surface also adds a minute to the run a cause
        page waits on, so a stall inside a token death dates that page from the death
        rather than from the resume. A slot that charges nobody, because every enabled
        ticker was out of span or the roster was empty, restarts that run at zero. Counted,
        it paged a token death on the first minute the span opened, dated inside the stall.
        Left unchanged, it dated a death that resumed after the stall from a minute before
        the stall, which is neither the death nor the restart (marketlake #768).

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

        A ticker the last cycle named as out of span is left out of the charge, and its
        out-of-span count advances by a slot instead. The hook charges every enabled
        entry, since it reads no spans, and nothing fetches that ticker. Charged, its
        fresh counter tripped this page for a stall inside an outage whose surfaces had
        already paged, which the per-surface rule says adds nothing (marketlake #570).
        Counted, its page reports the minutes it has really been out. The set is the last
        cycle's, so a span that opened during the stall is judged on the next cycle.
        """
        if not slots:
            return []
        left_out = set(self._out_of_span)
        watched = [key for key in surfaces if key.ticker not in left_out]
        # One overrun is reported in a single call, so the threshold is read once for the
        # batch rather than per slot.
        threshold = self._threshold()
        for slot in sorted(slots):
            self._roll(slot)
            if watched:
                self._minutes_without_data += 1
            else:
                self._minutes_without_data = 0
            for key in sorted(watched, key=str):
                self._counts[key] = self._counts.get(key, 0) + 1
            for ticker in left_out:
                self._out_of_span[ticker] = self._out_of_span.get(ticker, 0) + 1
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

    def _whole_daemon(self, tally: _Tally, threshold: int, slot: datetime) -> list[Page] | None:
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

        Which cycles count is :func:`whole_daemon_cause`'s rule, read here off the same
        tally ``observe`` counted, so the page and the daemon's token pull cannot drift. A
        chain that answered 200 with no contract counts against unanimity as
        ``contracts_absent``. That answer proves at least one of its requests authenticated
        and got through, so the cycle is not one cause.

        The page waits for the consecutive minutes in which no surface landed data to
        reach the threshold. ``minutes`` is that count, and ``since`` is the first minute
        of that run, ``slot`` less ``minutes - 1``. It is the run that starves the
        dead-man, so the page is dated from when capture stopped rather than from one
        surface's own failure, which can start long before the cause: a chain failing
        ``http_500`` from 10:00 under a token that dies at 10:30 would date the page 10:00
        (marketlake #747). The wait and ``minutes`` read one number, so ``minutes`` is
        never below the threshold. The slot is already in ET, and :meth:`_roll` clears the
        count at the ET date change, so ``since`` always falls inside the session.

        Waiting instead for every failed surface's own count to reach the threshold let a
        ticker joining mid-outage split the cause. Its young counter held the cause back,
        the older surfaces paged on their own, and the cause paged late, dated from the
        join (marketlake #768).
        """
        title = _whole_daemon_title(tally)
        if title is None:
            return None
        failed = tally.failed
        # The rule passed, so the failed segments recorded exactly one class.
        error_class = next(c for c in tally.recorded.values() if c is not None)
        if title in self._paged_causes:
            return []
        minutes = self._minutes_without_data
        if minutes < threshold:
            return None
        self._paged_causes[title] = set(failed)
        return [
            Page(
                title=title,
                minutes=minutes,
                surfaces=tuple(sorted(failed, key=str)),
                cause=error_class,
                since=slot - timedelta(minutes=minutes - 1),
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
            self._out_of_span.clear()
            self._paged_out_of_span.clear()
            self._minutes_without_data = 0

    def _reset(self, key: Surface) -> None:
        self._counts[key] = 0
        self._paged.discard(key)
        # A durable cycle landed, so the loop is running. A later stall is a new stall
        # and pages again.
        self._paged_overrun = False
        # This surface is back, so no cause covers it now. A cause that named others is
        # still true of them and stays live until the last one is released.
        self._release(key)

    def _release(self, key: Surface) -> None:
        """Take one surface out of the causes covering it, dropping one that empties.

        A cause with no surfaces left has nothing to explain, so dropping it re-arms it.
        Four things bring a surface here.

        1. It produced data.
        2. The roster dropped it.
        3. It answered with no contract.
        4. Its segment could not be written in a minute another surface landed data.

        An answer with no contract proves at least one of the surface's requests
        authenticated and got through, so the token works and the vendor is serving it, and
        what fails the surface now is an answer no cause explains. Kept in the cause
        instead, it held a token-dead cause live after the token recovered, and the next
        token death that session paged nothing (marketlake #326). Its counter keeps
        climbing, because it still produced nothing. A surface that merely started failing
        another way, a timeout or a 5xx or another cause's class, is still down, so the
        cause that named it has not lifted and keeps it.

        A write failure records no class, so it says nothing about what the vendor did.
        Kept in the cause, it never paged for itself, because a cause covers a failure that
        resolves to no cause, and the cause it held stayed live until the session date
        changed, so a second token death that session paged nothing (marketlake #754).
        Data landing on another surface proves the vendor answered, so ``observe`` brings
        the surface here in that minute. Its own page then goes out at once, unless it
        already paged before the cause did and is still in ``_paged``. Its counter kept
        climbing under the cause, so it skips the threshold and the page carries the
        outage's minutes, even when its write failed only on the minute the outage healed.
        A surface that answers with no contract on that minute already pages the same way.
        A minute in which nothing landed keeps it,
        since nothing then shows the outage has ended. Releasing it there would page every
        write failure, and then the cause a second time.
        """
        for title in list(self._paged_causes):
            held = self._paged_causes[title]
            if key not in held:
                continue
            held.discard(key)
            if not held:
                del self._paged_causes[title]

    def _drop_departed(self, touched: set[Surface], out_of_span: tuple[str, ...]) -> None:
        """Forget the counter and the paged flag of every surface that left the cycle.

        A live ticker touches every surface it owes on every cycle: capture plans quotes
        for each live ticker and a chain for each live options ticker, and a write that
        fails is recorded as an error, which counts as touched. So on a cycle that touched
        anything, an untouched surface belongs to a ticker the cycle no longer captures,
        whether it was retired, turned equity-only, or left out by the capture spans.

        Frozen instead, its counter did harm twice (marketlake #570). The missed-slot
        hook, ``on_missed``, charges the whole enabled roster, so every stall added to it
        and nothing ever reset it, until a one-minute stall paged the loop as overrun for
        three. And a ticker that came back failing paged on its first gap, reading three
        minutes after an hour away, which is the one transient failure the threshold exists
        to absorb.

        A cycle that touched nothing is the full case, every enabled ticker left out, and
        says nothing about any surface. There the tickers it names as out of span are
        dropped and nothing else. ``_release_retired`` is this rule's twin for causes.

        The price is a second page. A surface that paged, left the cycle, and came back
        still failing pages again at the threshold, where a frozen flag kept it silent.
        """
        if touched:
            gone = {key for key in self._counts.keys() | self._paged if key not in touched}
        else:
            names = set(out_of_span)
            gone = {key for key in self._counts.keys() | self._paged if key.ticker in names}
        for key in gone:
            self._counts.pop(key, None)
            self._paged.discard(key)

    def _out_of_span_pages(self, result: CycleResult, threshold: int) -> list[Page]:
        """Count the tickers the spans left out, and page the ones that newly tripped.

        A ticker's count rises on each cycle that names it and is dropped on the first
        that does not, which re-arms its page. Two commands pass through this state in
        ordinary use: a rejoin writes its roster entry before its span, and a retire
        closes the span before it changes the roster. A cycle can land in either window
        for one minute, so the page waits for the threshold like every other.

        One page covers every ticker tripping in the same cycle and names the whole
        standing set, so a clamp that widens later pages again for the new ticker. Its
        minutes are the longest any named ticker has been out, slots the loop slept
        through included, since ``missed`` counts those.

        A cycle with no segment and no error captured nothing, so its live roster was
        empty and every enabled ticker is left out. The dead-man pages that case and no
        other page is added (marketlake #554). The counts still rise there, so the first
        cycle that captures anything pages at once for a ticker already past the threshold.
        """
        named = result.out_of_span
        for ticker in list(self._out_of_span):
            if ticker not in named:
                del self._out_of_span[ticker]
                self._paged_out_of_span.discard(ticker)
        for ticker in named:
            self._out_of_span[ticker] = self._out_of_span.get(ticker, 0) + 1
        if not result.segments and not result.errors:
            return []
        tripped = [
            ticker
            for ticker in named
            if self._out_of_span[ticker] >= threshold and ticker not in self._paged_out_of_span
        ]
        if not tripped:
            return []
        self._paged_out_of_span.update(tripped)
        return [
            Page(
                title=_OUT_OF_SPAN_TITLE,
                minutes=max(self._out_of_span[ticker] for ticker in named),
                surfaces=(),
                tickers=tuple(named),
            )
        ]

    def _release_retired(self, touched: set[Surface]) -> None:
        """Stop covering a surface the roster has dropped.

        A ticker retired mid-session stops appearing in cycles, which is a supported
        path. Nothing about that surface ever changes again, so a cause that kept
        covering one would never re-arm, and the next genuine outage under the same
        title would page nobody for the rest of the session.

        A cycle that touched nothing at all is an empty roster rather than a retired
        one. It is evidence about no surface, so it releases none. ``_drop_departed`` makes
        the same judgement for the counters.
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
        with a different remedy, and the operator has to hear it. A surface also leaves a
        cause outright through :meth:`_release`, which names the four ways it does.
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
    "CONTRACTS_ABSENT",
    "DEFAULT_PAGE_MINUTES",
    "TOKEN_DEAD",
    "Page",
    "Surface",
    "Watchdog",
    "whole_daemon_cause",
]
