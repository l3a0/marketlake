"""The evening bar fetch: one session's equity bars, gated before they land.

Bars are the one surface this lake fetches rather than derives. Every option expiration
settles against the official daily close, and the read layer's adjusted views have nothing
to adjust until a bars partition exists. So this is the job that turns a lake of captured
chains into one a reader can ask about prices.

The shape is **gate-before-land**, which the design pins for the vendor-sweep surfaces. A
partition is validated before it is written, so a failure means the partition never lands
rather than landing with a flag on it. The capture surfaces work the other way round, sealing
first and quarantining later, because a capture minute is perishable and a bar is not: Schwab
serves a roughly 30-day one-minute lookback and daily bars indefinitely, so a bar refused
tonight is re-fetchable tomorrow while a chain snapshot missed at 10:31 is gone.

Run it with ``python -m lake.bars`` to fetch by hand. ``lake.sweep`` is what schedules it: its
18:30 weekday job calls :func:`backfill_bars`, walking every session the capture spans still hold
unlanded rather than the one its clock sits on. Marketlake #422 made that swap, because a daily
bar is judged against the *next* session's settled close and so cannot pass its own gate on the
night it is fetched.

What one run does, per ticker and per configured frequency, for one session.

1. Skip the ticker-day whose bars partition the manifest already records. A second run lands
   nothing and says so.
2. Fetch the window this module names for that frequency, outside the lake lock.
3. Build the rows, dropping any candle whose stamp the epoch transform refuses and, on
   ``1d``, any candle belonging to another session.
4. Run the two gate checks over the rows that survived, the span check first.
5. Write the partition and append its manifest entry, both inside one lock hold.

**Two entry points, and which ticker-days each covers.** :func:`fetch_session_bars` fetches one
session, taken from the clock or named by its own ``session`` argument. Nothing schedules it: it
is the by-hand single-session fetch, and marketlake #422 moved the evening run off it.
:func:`backfill_bars` walks every session the capture spans cover, which is marketlake #319
recovering an outage the lake has already had. Both build a list of :class:`TickerDay` and hand
it to the same walk, so the gate, the manifested skip and the held findings mean the same thing
under either.

**What differs is where the scope comes from, and the two readings are not interchangeable.**
The per-session run takes the enabled roster. Each entry's ``bars`` tuple says which frequencies
that ticker takes, read off the ticker's own settings rather than inferred from anything else,
and an entry with an empty tuple is passed over. A retired ticker is outside it: ``retire`` flips
``enabled`` and preserves ``bars``, and tonight's session holds no quotes to gate its bar
against, so every run would file the same finding for it forever.

The backfill reads the capture spans instead, which is what ``Roster.enabled``'s own docstring
asks for: "Scope readers do not use this; they read the capture-spans file instead." A retired
ticker's closed span is a stretch the lake captured, so its bars are worth what an open span's
are, and its frequencies are reached through ``Roster.get`` rather than past ``Roster.enabled``.
That is also why the backfill runs its own frequency check rather than :func:`_require_supported`,
which that function's docstring explains at its own site.

**The windows, and why the ``1m`` one is narrow.** ``freq=1m`` is fetched from the session
open to the equity close, which is 390 minutes on a regular day. ``freq=1d`` is fetched over a
bracket a day wider on each side, because marketlake #362's live recording measured the daily
stamp at midnight Eastern and at 01:00, both ahead of the 09:30 open the bracket would otherwise
start at. ``DAILY_WINDOW_MARGIN`` carries the widths and the slack in full. The selection
below reads the session off the stamp rather than off the window, so the extra candles a wide
bracket returns are dropped rather than landed.

The narrow ``1m`` window is a choice, not the obvious default. Both vendor flags are left
unset, so Schwab decides whether a price-history response covers the regular session or the
extended one, and nobody has recorded which it picks. With a window at the session's own
bounds that does not matter, because the request's own bounds clip the response either way and
the coverage is 390 minutes under both answers. With a wider window it would matter a great
deal: Schwab answering with the regular session would be correct and short at the same time,
the span check would refuse it, and by that check's unbounded-repeat rule the run would stall
with a finding pointing at the wrong cause. The narrow window removes that confound, and
marketlake #333's own planning loop reached the same conclusion from the other side.

**The two gate checks, in the order they run.** The span check runs first. The close
cross-check compares the candle whose stamp maps to the session, so on a response that dropped
that session it has no candle to compare and would file a finding about a comparison that
never had a bar. Running the span check first refuses the short response under its own name
instead. Both run *after* the row builder, over the rows that survived it, which is the only
reading that describes what the partition would actually hold. A span measured over the
payload would pass while the partition landed short, which is the back door the check exists
to close.

**The tolerance is measured rather than guessed.** ``CLOSE_CROSS_TOLERANCE`` carries the
bracket it came from.

**What is not contained per ticker-day.** A dead refresh token fails every remaining
ticker-day identically, so containing it would turn one auth death into a page of held
findings and an exit code reading "some findings held" rather than "the token is dead". It
stops the run. So does a bad argument this job hands the seam, which is its own bug rather
than a vendor failure. Everything else a ticker-day can raise is contained to that ticker-day
and the walk goes on, because one unreadable session is not the other tickers' bars to lose.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lake.actions import (
    CHECK_INSTRUMENT_RESOLUTION,
    SWEEP_SOURCE,
    AmbiguousSymbol,
    HeldFinding,
    MasterAbsent,
    UnresolvedSymbol,
    resolve_instrument,
)
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar, NotASession
from lake.capture_spans import (
    CaptureSpan,
    CaptureSpans,
    CaptureSpansError,
    SpansUnreadable,
    spans_path,
)
from lake.clock import Clock, SystemClock
from lake.journal import UNFIT_ERRORS, bars_data_batch, bars_rows
from lake.loader import LoadError, load_quotes
from lake.lock import lake_lock
from lake.manifest import latest_entries, record_partition
from lake.paths import LakePaths, temp_write_path
from lake.report import Withheld, write_withheld
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor, VendorAuthError
from lake.security_master import MasterUnreadable, SecurityMaster, master_path
from lake.session import SessionBounds, SessionClock
from lake.tickers import Roster, TickersError, load_tickers
from lake.vendor import (
    BAR_FREQS,
    DAILY_FREQ,
    MINUTE_FREQ,
    Vendor,
    VendorError,
    require_bar_freq,
)

# How the command builds its vendor. ``lake.record`` spells the same alias for the by-hand
# recorder, and it is restated rather than imported because a capture surface importing the
# recorder points the dependency the wrong way.
VendorFactory = Callable[..., Vendor]

# The two checks the gate files under, named the way ``actions`` names its three: a snake_case
# constant passed as ``check=`` rather than a literal at the filing site, so a test asserts
# against the constant and a rename moves both ends at once. They live here rather than beside
# the other three because this module owns them, and ``actions`` has no caller for either.
CHECK_BAR_SPAN = "bar_span"
CHECK_BAR_CLOSE = "bar_close"

# The third token, for a fetch that never reached the gate at all. A non-2xx arrives as an
# ordinary response whose body carries no candles, because the seam returns the vendor's
# status verbatim and raises on nothing, and bars carry no gap rows for it to land in. So it
# is refused under its own name before anything tries to read a payload that is not one.
CHECK_BAR_RESPONSE = "bar_response"

# How far the daily bracket reaches past the session on each side. The bracket runs from
# ``bounds.open - margin`` to ``bounds.equity_close + margin``, and the open is 09:30 Eastern,
# so the start side is what binds: a bracket that only spanned the session itself would begin
# after the stamp it is looking for.
#
# Marketlake #362 recorded ``freq=1d`` live and measured two stamps, at midnight Eastern of
# their sessions and at 01:00. A midnight stamp needs 9:30:00 of margin to fall inside the
# bracket and an 01:00 one needs 8:30:00, so 9:30:00 is the floor the observations support.
#
# It stays at a day anyway, and the reason is slack rather than neighbour handling. Narrowing
# to the floor changes nothing about which neighbouring candles are refused, measured: at a day
# and at 9:30:00 alike, the previous session's stamp is outside the bracket and the next
# session's is inside it. What narrowing removes is room for a stamp that lands before the
# floor. The two recorded stamps are both local midnight under *different* UTC offsets, one
# standard and one daylight, in a month that is daylight throughout, so the vendor's offset is
# not dependable and a bracket sized to the floor would refuse the first stamp that slips under
# it. Marketlake #380 owns that anomaly.
#
# The width's cost is response size. The bracket starts at the previous day's 09:30 Eastern,
# which is already past that session's own stamp, so the previous session's candle falls outside
# the request rather than coming back to be dropped. The next session's candle does come back
# when it exists, and the selection below drops it.
DAILY_WINDOW_MARGIN = timedelta(days=1)

# How much the official daily close may differ from the session's own captured quotes before
# the bar is held out. It is relative, like the sibling's dividend tolerance, and measured
# from the live lake rather than guessed.
#
# The floor is the drift the check has to absorb: the lake's own quotes are still moving after
# 16:00, by 0.66 basis points on SPY and 1.13 on QQQ between the ``spot_close`` and
# ``option_close`` cycles on 2026-09-14 and 2026-09-15. That is a lower bound, because capture
# stops at 16:15 and the closing print can settle later.
#
# The ceiling is the smallest adjustment the check has to catch, which is one dividend: the
# lake's own rows put the per-event figure at 25.13 basis points for SPY and 11.47 for QQQ. So
# the narrower window is QQQ's, 1.13 to 11.47, and it is ten times wide.
#
# Five basis points sits inside it with room at both ends. It absorbs four times the largest
# drift measured, which leaves margin for a print that settles later than capture watched, and
# it refuses anything above half of the smallest per-event dividend.
CLOSE_CROSS_TOLERANCE = 5e-4

# How far past a session the calendar-next search looks. It matches ``oi``'s bound rather
# than being chosen again: that module and ``control_plane`` already disagree, 30 against 14,
# and marketlake #334 owns collapsing the three spellings into one. A third number would have
# left that issue two values to settle instead of one. The longest run of consecutive
# non-sessions the US market produces is four days, so any of the three is ample.
NEXT_SESSION_SEARCH_DAYS = 30


class BarsError(Exception):
    """Base class for every bars-sweep error that is the operator's to fix."""


class CloseOfRecordDisagrees(BarsError):
    """The rows of a session's close of record do not agree about ``close_price``.

    The close of record is one cycle, which the loader enforces, and it is still more than one
    row when the partition holds two spellings of that one instant. Those rows have to agree
    about the close, and a disagreement raises rather than taking the first one, because taking
    the first lets the file's own row order decide what a bar is judged against, silently.

    It is a ``BarsError`` rather than a bare ``ValueError`` so the walk can contain it to its
    own ticker-day. A bare ``ValueError`` would fall through the walk's catch, which names only
    what this job contains, and end the run on a traceback with no report at all. That is the
    right treatment for an argument this job hands the seam and the wrong one for a partition
    the lake already sealed, which is what this is.
    """

    def __init__(self, ticker: str, day: date, values: set[float]) -> None:
        super().__init__(
            f"the {day.isoformat()} close of record disagrees about close_price for "
            f"{ticker}, among {sorted(str(value) for value in values)}"
        )
        self.ticker = ticker
        self.day = day


class UnsupportedBarFreq(BarsError):
    """A roster frequency this lake has no vendor call for.

    ``onboard --bars`` takes any string and ``TickerConfig.bars`` stores it unvalidated, so a
    roster is free to carry a frequency nothing can fetch. ``vendor.require_bar_freq`` refuses
    one, and this names the same refusal as an operator mistake with one file behind it.

    It is raised before the walk starts rather than when the walk reaches it, and the whole
    roster is checked at once. A frequency nothing can fetch fails identically for every
    ticker-day it names, so holding it per ticker-day would file the same finding every night
    forever, and raising it mid-walk would leave a run half done for a fault nothing in that
    run caused. Checking up front costs nothing: no vendor call has gone out and no partition
    has been written when it raises.

    The price is named rather than hidden. A roster cannot carry a frequency ahead of the
    support for it landing, because tonight's run refuses the file rather than passing over
    the line it does not recognise. That is the loud direction: the quiet one leaves an
    operator believing a frequency is being captured while nothing ever fetches it.
    """

    def __init__(self, ticker: str, freq: str) -> None:
        super().__init__(f"{ticker}: bar frequency {freq!r} is not one of {list(BAR_FREQS)}")
        self.ticker = ticker
        self.freq = freq


class StampNotAnInstant(BarsError):
    """A stamp carrying no UTC offset, which names no instant this module can read.

    ``datetime.fromisoformat`` accepts a naive stamp, and ``astimezone`` then resolves it against
    whatever timezone the process happens to be running in. So the same stamp names one session
    on a machine set to Eastern and another on a machine set to UTC, with nothing raised either
    way. Measured: ``2026-09-16T01:00:00`` reads as 2026-09-16 under ``TZ=America/New_York`` and
    as 2026-09-15 under ``TZ=UTC``.

    Every other stamp reader in this package tests for that before trusting a stamp.
    ``onboard``, ``vendor``, ``actions``, ``capture_spans`` and ``security_master`` each spell the
    same condition, and ``lake.bars`` was the one that did not. ``settle`` spelled it too until
    this class took the rule over, and it reaches the same refusal now by calling
    :func:`session_of` rather than by keeping its own copy. The refusal names the offset form to
    pass, which is the rule ``docs/design.md`` states for onboarding's own naive-instant refusal.

    **It stays out of the walk's catch in :func:`fetch_session_bars`.** That tuple names
    ``CloseOfRecordDisagrees`` rather than the ``BarsError`` base precisely so a later subclass
    decides for itself, and this one decides to stay out. Inside this module every stamp is
    minted by ``journal.bars_rows`` through ``journal.epoch_ms_to_utc``, which pins ``tz=UTC``,
    so a stamp reaching this refusal means that guarantee has broken for the run rather than for
    the ticker-day being walked. Holding it per ticker-day would file the same finding for every
    ticker and still write the run as though it had worked. ``UnsupportedBarFreq`` refuses up
    front for that same reason.

    **It is named at both doors an operator meets, which is the other half of the
    ``UnsupportedBarFreq`` comparison.** Staying out of the per-ticker-day catch is not a reason
    to reach a person as a stack trace. ``main`` prints one named line and exits 2, and
    ``sweep._BARS_REFUSALS`` holds it so the 18:30 job reports a refused bars piece rather than
    ending mid-run. That second one is what keeps an escape from costing the report file, the
    digest and the Friday ``pmset`` wake, which are all written after the pieces block.
    ``docs/design.md`` states the rule: one named line and exit 2 rather than a stack.

    **Two readers outside this module call :func:`session_of`, and only one can reach this.**
    ``lake.settle`` can, on ``expiration_date``, which ``journal`` keeps as the vendor's ISO
    string verbatim rather than minting it. It catches this and reads the stamp as unreadable,
    which leaves ``ExpirationUnreadable`` as the refusal that view already documents.
    ``loader._in_view`` cannot, and the reason is ordering rather than anything it does:
    ``load_bars`` runs ``_sorted_by_instant`` over the same table first, which refuses an
    unreadable stamp in ``LoadError`` vocabulary before ``_in_view`` sees it.
    """

    def __init__(self, bar_ts: str) -> None:
        super().__init__(
            f"{bar_ts!r} carries no UTC offset, so the session it names would be decided by "
            "this machine's timezone rather than by the stamp. Pass an offset, like "
            "2026-09-15T20:00:00+00:00 or 2026-09-15T16:00:00-04:00."
        )
        self.bar_ts = bar_ts


class SpansAbsent(BarsError):
    """The lake holds no capture-spans file, so the backfill has no range to walk.

    It is a ``BarsError`` rather than the bare ``OSError`` ``CaptureSpans.read`` raises, for the
    reason ``MasterAbsent`` is one class over: a reader that has to tell an absent reference file
    apart from a corrupt one wants two names, and ``main`` turns each into one line naming a
    different fix. ``MasterAbsent`` lives in ``actions`` rather than in ``security_master`` for
    the same reason this lives here rather than in ``capture_spans``: the module that reads the
    file is the one that has to say what an absent one means to its own run.

    A lake with a master and no spans file is a real state rather than a corrupt one. It is what
    a master from before capture spans existed looks like, and ``python -m lake.seed_spans``
    is the command that converts it, which is what the refusal names.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(f"no capture spans at {path}")
        self.path = path


@dataclass(frozen=True)
class TickerDay:
    """One ticker, one frequency, one session: the unit both entry points walk.

    It is a value rather than a tuple because the backfill sorts, dedupes and reports over these,
    and a three-tuple of two strings and a date reads the same at every call site whichever order
    it is in. ``_ticker_freqs`` still yields pairs, because within one session the session is not
    the pair's to carry.
    """

    ticker: str
    freq: str
    session: date


@dataclass(frozen=True)
class BarWindow:
    """The window one fetch asks for, and the session it is asking about."""

    session: date
    freq: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class SpanCoverage:
    """What the span check measured, and whether the response covered what was asked for.

    ``covered`` and ``requested`` are the two numbers the finding files, and they ride the
    verdict rather than being recomposed by the caller, for the reason ``DividendConsistency``
    gives one surface over: a caller that recomputed them could file a pair the check never
    saw. They are counted in the frequency's own grain, minutes for ``1m`` and sessions for
    ``1d``, because a pair of instants does not fit the ``float | None`` fields the record
    types.
    """

    covers: bool
    covered: float
    requested: float


@dataclass(frozen=True)
class CloseCross:
    """What the close cross-check compared, and whether the two agreed.

    ``computed`` is the bar's own close and ``against`` is the close the lake settled for that
    session out of its own captured quotes. Either is ``None`` when the comparison had no such
    number, and a check missing an input has not agreed, which is what makes a partial payload
    a held bar rather than a silent one.
    """

    agrees: bool
    computed: float | None
    against: float | None


def bar_window(freq: str, bounds, *, margin: timedelta = DAILY_WINDOW_MARGIN) -> BarWindow:
    """The window one frequency's fetch asks for on one session.

    ``bounds`` is ``SessionClock.bounds(day)``, so every instant here comes from the calendar
    and none is a wall-clock literal. Both are Eastern-aware, which is what the seam requires:
    ``require_utc_bound`` refuses a naive bound because ``schwab-py`` would read it in the
    capture machine's own zone.

    ``1m`` asks for the session itself, open to equity close. That is 390 minutes on a regular
    day and fewer on an early close, and it stops at the *equity* close where capture's own
    window runs on to the option close, because an equity bar has nothing to say about the
    fifteen minutes after the auction.

    ``1d`` asks for a bracket a ``margin`` wider on each side. ``DAILY_WINDOW_MARGIN`` carries
    why in full, and the short version is that a daily candle is stamped near midnight Eastern
    of its session rather than inside it, so a bracket spanning only the session would begin
    after the candle it wants. The session is read off the stamp rather than off the window.
    """
    freq = require_bar_freq(freq)
    if freq == MINUTE_FREQ:
        return BarWindow(session=bounds.day, freq=freq, start=bounds.open, end=bounds.equity_close)
    return BarWindow(
        session=bounds.day,
        freq=freq,
        start=bounds.open - margin,
        end=bounds.equity_close + margin,
    )


def session_of(bar_ts: str) -> date:
    """The session a bar's own stamp names.

    A bar's session is decided by ``bar_ts``, never by when the sweep fetched it. That is the
    rule ``journal.py`` pins on the bars schema, and it forecloses the shortcut a builder
    reaches for when a daily stamp looks ambiguous, which is to take the session from the
    request window or the clock.

    The stamp is a UTC instant, so the session is its Eastern calendar date. ``MARKET_TZ`` is
    the calendar module's, which is what keeps this from naming a session time of its own.

    Marketlake #362 measured this against a live recording rather than leaving it assumed. Two
    daily stamps came back, at 00:00 and 01:00 Eastern, and this reading names the right session
    for both.

    **Which session each stamp names was fixed from outside the recording.** The stamps alone do
    not settle it: two candles are equally consistent with a stamp naming the session it opens
    and with one naming the session before it, because the neighbour that would tell them apart
    fell outside the requested bracket either way. What decided it was crossing the first
    candle's values against a second vendor's SPY daily bar, which agrees to every decimal and
    stamps it on 2026-09-15. That cross-check is recorded on #362.

    **It is not settled for standard-time season.** The two stamps are both local midnight under
    different UTC offsets, one standard and one daylight, in a month that is daylight throughout.
    Mirrored into winter the same slip crosses the date backwards: 04:00 UTC in January is 23:00
    Eastern on the previous day, and this returns that previous date. Marketlake #380 owns it.

    That direction fails closed. A stamp this reading places on the wrong date leaves the
    selection below with no candle for the session, the span check refuses, and nothing lands.
    The ticker-day is held with a finding instead of landing a bar under the wrong day.

    **A stamp carrying no offset is refused rather than read.** ``StampNotAnInstant`` says why.
    A naive stamp resolves against the process's own timezone, so this would answer by the
    machine rather than by the stamp. Marketlake #385.
    """
    return _as_instant(bar_ts).astimezone(MARKET_TZ).date()


def select_session_rows(rows: Sequence[dict], window: BarWindow) -> list[dict]:
    """The rows of ``rows`` that belong to the session being fetched.

    On ``1d`` the window is deliberately wider than the session, so a response can carry
    neighbouring sessions' candles and more than one candle in total. The one that lands is
    the one whose stamp maps to the session, never the first one the list happens to hold. A
    response carrying none for that session lands no row for it, and the span check then
    refuses the fetch.

    On ``1m`` the window is the session itself, so every candle inside it already belongs to
    that session and this filters nothing. It still runs, because a response carrying a candle
    from another session is exactly what the span check is there to refuse, and silently
    keeping it would put another session's minute in this session's partition.
    """
    return [row for row in rows if session_of(str(row["bar_ts"])) == window.session]


def _as_instant(bar_ts: str) -> datetime:
    """One bars stamp as a UTC instant, refusing one that carries no offset.

    This is the module's single rule for turning a ``bar_ts`` string into an instant, and the
    three readers that need one share it: :func:`session_of` names the session a stamp belongs
    to, :func:`_instant` places a row against the requested window, and :func:`_bar_close` picks
    the session's last candle. Writing the rule once is the point. Two of those readers were
    written apart and drifted, which is what marketlake #385 and #386 are.

    ``StampNotAnInstant`` carries the argument for refusing a naive stamp rather than resolving
    it against the process's timezone.
    """
    parsed = datetime.fromisoformat(bar_ts)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StampNotAnInstant(bar_ts)
    return parsed.astimezone(UTC)


def _instant(row: dict) -> datetime:
    """One row's stamp as a UTC instant.

    ``bar_ts`` is the ISO string the row builder wrote out of ``journal.epoch_ms_to_utc``, so
    reading it back is a parse rather than a second transform. The check and the builder must
    not disagree about which stamps are readable, and they cannot, because only a stamp the
    builder already accepted ever reaches a row.
    """
    return _as_instant(str(row["bar_ts"]))


def check_bar_span(
    built: Sequence[dict], selected: Sequence[dict], window: BarWindow
) -> SpanCoverage:
    """Whether the rows that survived cover the span the fetch asked for.

    This is marketlake #333's check. ``lake.vendor`` and ``lake.schwab`` return the vendor's
    body verbatim by design, and ``schwab-py`` sends ``period`` alongside the explicit bounds
    against its own documented rule, so without this nothing asks whether the candles that
    came back cover the window that went out. It ships with the first run that writes bars
    rather than after it, because this job skips a ticker-day whose partition is already
    manifested, so a check added later would leave short partitions on disk that nothing will
    ever re-fetch.

    **It is measured over the rows, not over the payload.** ``built`` is what survived the row
    builder and ``selected`` is what survived the session filter after it. A candle whose
    stamp the epoch transform refuses never becomes a row, and one at an edge takes that end
    of the coverage with it. A daily response missing the session being fetched has every
    candle filtered out, and a check reading the payload would see its neighbours and call
    that covered. Either way the payload describes a partition that would not be written.

    **The comparison, per frequency.**

    On ``1m`` it is the two ends. ``min`` must equal the window's start and ``max`` must equal
    its end minus a minute, because a candle is stamped at its minute's open, so a window
    running to the equity close is fully covered by stamps ending at 15:59. Counting candles
    or distinct minutes instead would need an answer nobody has: whether a real Schwab 1-min
    response carries a candle for every minute or omits the minutes that did not trade. The
    only bars fixture in the tree is synthesized and holds three candles for a 390-minute
    window, so a count rule would refuse the response the whole suite replays. The ends need
    no such answer. They are sufficient here because this job fetches one session, which
    leaves no interior session to lose.

    On ``1d`` it is presence: did the session being fetched come back, one against one. The
    window is a bracket deliberately wider than the session, so a neighbouring session whose
    stamp falls *inside* that bracket is expected rather than wrong and is dropped by the
    selection. One outside it is not expected, because the request's own bounds are what the
    vendor clips to, so a candle beyond them means the bounds were ignored. #362's recording is
    the first evidence that Schwab does clip a daily response: the session before the requested
    start did not come back, though ``period=20, periodType=year`` went out beside the bounds.
    One observation is not a reason to retire this check.

    **Both directions are refused, because the two frequencies fail opposite ways.** The 1-min
    wrapper sends ``period=1, periodType=day``, narrower than any multi-session window, so its
    failure is short coverage. The daily wrapper sends ``period=20, periodType=year``, wider
    than any window this lake will ever ask for, so its failure is a response covering twenty
    years. A check that only refused short coverage would never see the second. So any row
    outside the requested bounds refuses the fetch on either frequency.

    The two numbers filed are minutes on ``1m`` and sessions on ``1d``, because
    ``report.Withheld`` types them as floats and a pair of instants does not fit the field.
    """
    if not built:
        # The check is defined over a non-empty candle list, which is #333's own rule. An empty
        # window covers zero of its span, so a check reaching one would refuse every empty
        # response as a coverage failure and the empty-window rule that owns that case would be
        # dead code. The caller refuses an empty response before reaching here, and this refuses
        # loudly rather than quietly answering for a case it does not decide, so a second caller
        # that skipped that rule cannot silently contradict it.
        raise ValueError("the span check is defined over a non-empty candle list")
    minute = timedelta(minutes=1)
    outside = [row for row in built if not window.start <= _instant(row) < window.end]
    if window.freq == DAILY_FREQ:
        # Covered counts the sessions the response actually carried, not just the one that was
        # wanted, so a response reaching outside the bracket files a pair that says so. Setting
        # it from ``selected`` alone would file "1.0 against 1.0" on a refused fetch, and an
        # operator reading the withheld file would see full coverage on a bar that never
        # landed.
        sessions = {session_of(str(row["bar_ts"])) for row in built}
        covered = float(len(sessions)) if outside else (1.0 if selected else 0.0)
        return SpanCoverage(covers=bool(selected) and not outside, covered=covered, requested=1.0)
    requested = (window.end - window.start) / minute
    if not selected:
        return SpanCoverage(covers=False, covered=0.0, requested=requested)
    stamps = [_instant(row) for row in selected]
    first, last = min(stamps), max(stamps)
    covered = (last - first) / minute + 1
    ends_match = first == window.start and last + minute == window.end
    return SpanCoverage(covers=ends_match and not outside, covered=covered, requested=requested)


def check_close_cross(
    bar_close: float | None,
    quote_close: float | None,
    *,
    tolerance: float = CLOSE_CROSS_TOLERANCE,
) -> CloseCross:
    """Whether the official daily close agrees with the session's own captured quotes.

    This is the design's close cross-check, and it is what catches a vendor that starts
    serving pre-adjusted prices. Capture seals minutely quotes in real time and a retroactive
    adjustment cannot change what is already sealed, so an adjusted bar disagrees with the
    lake's own unadjusted history and a split-sized discontinuity is 2x rather than subtle.
    The design cut a second vendor for this reason: the lake already holds its own second
    observation.

    The comparison is relative, at ``tolerance``, whose constant carries the measurement it
    came from.

    Two edges are decided here rather than left to a division.

    1. A comparison missing either number has nothing to compare, so it does not agree. That
       is what makes a bar with no readable close a held bar rather than a silent one.
    2. A settled close of zero has no relative scale, so the two agree only when the bar's own
       close is zero too. No equity prints a zero close, and a division that returned "agrees"
       for every bar against a zeroed reference is the one failure nobody would notice.
    """
    if bar_close is None or quote_close is None:
        return CloseCross(agrees=False, computed=bar_close, against=quote_close)
    if quote_close == 0:
        return CloseCross(agrees=bar_close == 0, computed=bar_close, against=quote_close)
    difference = abs(bar_close - quote_close) / abs(quote_close)
    return CloseCross(agrees=difference <= tolerance, computed=bar_close, against=quote_close)


@dataclass(frozen=True)
class LandedPartition:
    """One bars partition the run wrote, and the manifest entry it appended for it."""

    ticker: str
    freq: str
    session: date
    partition: str
    rows: int


def _render_held(held: Sequence[HeldFinding]) -> list[str]:
    """The held-findings block of a sign-off report, for the one reader who has to act on it.

    One spelling rather than two, for the reason :func:`_unfiled` gives beside it: the two reports
    say the same thing about a held finding, and a second copy is how the two drift into saying
    different things. This one is sixteen lines where that one is two, so the argument is stronger
    here.

    A finding with neither a computed pair nor an exception renders its subject line alone, which
    is the shape a caller filing only ``check`` produces, and it does not raise.
    """
    lines = [f"  held:    {len(held)}"]
    for entry in held:
        finding = entry.finding
        detail = (
            f"{finding.symbol} {finding.event} {finding.observed_on.isoformat()} {finding.check}"
        )
        if finding.computed is not None or finding.against is not None:
            detail += f": {finding.computed} against {finding.against}"
        elif finding.exception:
            detail += f": {finding.exception}"
        lines.append(f"    - {detail}")
        if entry.filed_at is None:
            lines.append(f"      NOT filed: {entry.filing_error}")
        else:
            lines.append(f"      filed at {entry.filed_at}")
    return lines


def _unfiled(held: Sequence[HeldFinding]) -> tuple[HeldFinding, ...]:
    """Every held finding whose record could not be written down.

    A finding held and filed is a live condition a human can read. A finding held and not filed
    reads exactly like a run that found nothing, which is the silence the producer exists to
    break, so it is what the command turns into a non-zero exit code.

    Both reports read it through this rather than each spelling it, because the two exit codes
    are one contract and a second spelling is how the two drift.
    """
    return tuple(held for held in held if held.filed_at is None)


@dataclass(frozen=True)
class BarsReport:
    """What one run of the sweep did, for the sign-off block.

    ``skipped`` counts the ticker-days whose partition the manifest already recorded. It is
    the number that makes a second run legible: a run that lands nothing and holds nothing has
    either done the work already or found nothing to walk, and only this tells the two apart.
    ``ExtractionReport.unchanged`` is the same number one surface over.
    """

    session: date
    attempted: int
    landed: tuple[LandedPartition, ...]
    held: tuple[HeldFinding, ...]
    skipped: int

    @property
    def unfiled(self) -> tuple[HeldFinding, ...]:
        """Every held finding whose record could not be written down."""
        return _unfiled(self.held)

    def render(self) -> str:
        """A human-readable sign-off block."""
        lines = [
            f"Bar fetch for {self.session.isoformat()} over {self.attempted} ticker-day(s)",
            f"  landed:  {len(self.landed)}",
        ]
        for entry in self.landed:
            lines.append(
                f"    - {entry.ticker} {entry.freq} {entry.rows} row(s) at {entry.partition}"
            )
        lines.extend(_render_held(self.held))
        lines.append(f"  skipped: {self.skipped}")
        return "\n".join(lines)


def _read_master(lake_root: Path) -> SecurityMaster:
    """The master, read once before anything else, or the reason the run stops.

    ``resolve_instrument`` takes a ``SecurityMaster`` rather than a path, so reading it is a
    precondition of the walk rather than a step inside it, the way it is in ``actions``.
    """
    path = master_path(lake_root)
    try:
        return SecurityMaster.read(path)
    except FileNotFoundError as exc:
        raise MasterAbsent(path) from exc


def _require_supported(roster: Roster) -> None:
    """Refuse a roster carrying a frequency this lake has no vendor call for.

    Checked before the walk starts, for the reasons :class:`UnsupportedBarFreq` gives, and
    over exactly the entries the walk would fetch. Checking the whole roster instead reaches
    past this run's own scope: a stale ``bars:`` line on a retired ticker, which nothing here
    would ever fetch, would halt the nightly run at exit 2 every night until someone edited a
    file for a ticker that is not being captured.
    """
    for entry in roster.enabled:
        for freq in entry.bars:
            if freq not in BAR_FREQS:
                raise UnsupportedBarFreq(entry.ticker, freq)


def _ticker_freqs(roster: Roster) -> Iterator[tuple[str, str]]:
    """Every ticker and frequency this run covers, in roster order then frequency order.

    ``bars`` is read off each ticker's own settings, which is what says whether a ticker takes
    bars at all and at which frequencies. Enablement is a separate question and scopes the
    walk, per the module docstring.
    """
    for entry in roster.enabled:
        # A roster is free to name one frequency twice, since ``TickerConfig.from_mapping``
        # stores the list as given. Fetching it twice would spend two vendor requests on one
        # partition and append two manifest entries for one path, because the manifest is read
        # once before the walk and the second pass would not see the first one's entry.
        seen: set[str] = set()
        for freq in entry.bars:
            if freq in seen:
                continue
            seen.add(freq)
            yield entry.ticker, freq


def _fetch(vendor: Vendor, ticker: str, window: BarWindow):
    """One price-history response, dispatched onto the seam's per-frequency method.

    Schwab has no frequency-parameterized price-history call, so frequency is the choice of
    method. The seam declined to own this dispatch, staying a forwarder rather than becoming a
    dispatcher, so it is restated here rather than lifted. ``lake.record`` carries the same two
    lines for the by-hand recorder, and pointing either at the other would run a dependency the
    wrong way: the surface would import the recorder, or the recorder the surface.

    The set of frequencies is spelled once, in ``vendor.BAR_FREQS``, and ``require_bar_freq``
    has already refused anything outside it by the time a window exists. So a third frequency
    fails at the seam rather than quietly taking the daily call in one of the two dispatches.
    """
    call = vendor.get_minute_bars if window.freq == MINUTE_FREQ else vendor.get_daily_bars
    return call(ticker, start=window.start, end=window.end)


def _write_partition(rows: Sequence[dict], partition: Path) -> int:
    """Write one bars partition atomically, and hand back the rows it holds.

    A temp file beside the target, a flush, then one rename, which is how ``compact``, the
    capture spans, the version ledger and the security master each write. A crash mid-write
    leaves only the temp file, never a torn Parquet at the partition path. The temp path comes
    from ``paths.temp_write_path``, which owns the marker the backup exclusion matches.

    The parent is created first, and a bars path is one directory level deeper than a chains
    or quotes one because of its ``freq=`` level.
    """
    table = pa.Table.from_batches([bars_data_batch(rows)])
    partition.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_write_path(partition, os.getpid())
    try:
        pq.write_table(table, tmp)
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, partition)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return table.num_rows


def _finding(
    ticker: str,
    window: BarWindow,
    check: str,
    *,
    computed: float | None = None,
    against: float | None = None,
    instrument_id: int | None = None,
    instrument_ids: Sequence[int] = (),
    exception: str | None = None,
) -> Withheld:
    """One withheld finding, with the bars surface's own reading of the record's fields.

    ``observed_on`` is the session the rows belong to, never the night the sweep ran.
    ``event`` carries the frequency, because a bar is not an event and the frequency is what
    says what a finding is about without opening the file. Two frequencies of one ticker-day
    therefore file under two names rather than colliding on one.

    ``check``, ``computed`` and ``against`` stay three fields rather than one joined string,
    which is the shape ``write_withheld`` writes and reads back.
    """
    return Withheld(
        symbol=ticker,
        observed_on=window.session,
        event=window.freq,
        check=check,
        computed=computed,
        against=against,
        instrument_id=instrument_id,
        instrument_ids=tuple(instrument_ids),
        exception=exception,
    )


def _settled_close(lake_root: Path, ticker: str, session: date, following: date) -> float | None:
    """The close the lake settled for ``session``, read out of its own captured quotes.

    **The obvious source is the one the design rules out.** ``load_quotes`` with no snap
    returns the session's own ``spot_close`` cycle, and that is the pre-auction book rather
    than the closing print: measured on the live lake, ``regular_market_last_price`` is still
    moving after 16:00, by 0.66 basis points on SPY and 1.13 on QQQ between the two close
    cycles. What the lake holds settled is the *next* session's ``close_price``, carried on
    every row of it: 2026-09-15 reads 709.18 for QQQ, matching 2026-09-14's last reading
    rather than its ``spot_close`` one, and 760.88 for SPY, matching 2026-09-14's
    ``spot_close`` exactly. So the drift is per-ticker and sometimes zero, and the settled
    figure is the one to compare against.

    ``following`` is the calendar-next session, never the next session that happens to hold
    data. ``oi.py`` decided that already and refuses to read past a session holding nothing,
    because the figure it would find settled after some later session's trading and belongs to
    somebody else. A bar compared against such a close is the same error.

    The read goes through the loader, so the quarantine guard and the overflow projection are
    asked rather than skipped by a second path that would then keep skipping them forever.

    A close of record whose rows disagree about ``close_price`` raises rather than taking the
    first, because taking the first lets the file's own row order decide what the bar is
    judged against, silently. The caller holds and files what this raises.
    """
    table = load_quotes(ticker, following, lake_root=lake_root)
    if "close_price" not in table.column_names:
        # A session sealed before the column existed has no figure to compare against, which
        # is an absence rather than an error. Reading the column anyway would raise
        # ``KeyError``, which the walk's catch does not name, so one old partition would end
        # the run.
        return None
    # A null is an absence rather than a competing answer, so it is dropped before the rows are
    # compared. A partition sealed before the column carried a value would otherwise read as a
    # disagreement with the one row that does carry it.
    #
    # A NaN is dropped for a different reason: it is not a figure, so it cannot be compared
    # against. It also never equals itself, so two rows carrying one would read as two
    # answers. Either way the comparison is left with no number and the caller holds the bar,
    # which is the same outcome a missing column gives.
    values = {
        value
        for value in table.column("close_price").to_pylist()
        if value is not None and value == value
    }
    if len(values) > 1:
        raise CloseOfRecordDisagrees(ticker, following, values)
    return values.pop() if values else None


def _calendar_next_session(
    market: Calendar, session: date, *, horizon: int = NEXT_SESSION_SEARCH_DAYS
) -> date | None:
    """The first trading session strictly after ``session``.

    Calendar-next, never the next session that happens to hold data, for the reason
    :func:`_settled_close` gives. ``Calendar`` offers no next-session helper, so this steps
    forward a day at a time. ``None`` means the search ran past its guard, which a real
    calendar never does: the longest run of consecutive non-sessions the US market produces is
    four days.

    This is the third private spelling of one step. ``oi._calendar_next_session`` and
    ``control_plane._first_session_on_or_after`` are the other two. marketlake #334 counts them
    and says a fourth is where this stops being a smell, so this copy is written to be
    repointed at the shared helper that issue lands rather than to survive.

    Its bound deliberately matches ``oi``'s rather than being chosen again. Those two already
    disagree, 30 against 14, and #334 names that disagreement as the defect it owns. A third
    number would have left that issue two values to settle instead of one.
    """
    for step in range(1, horizon + 1):
        candidate = session + timedelta(days=step)
        if market.is_session(candidate):
            return candidate
    return None


def fetch_session_bars(
    *,
    lake_root: Path | str,
    vendor: Vendor,
    clock: Clock,
    calendar: Calendar,
    roster: Roster,
    session: date | None = None,
) -> BarsReport:
    """Fetch, gate and land one session's bars for every ticker and frequency the roster names.

    Every dependency is injected and nothing here reads config, the way ``extract_dividends``
    and ``seed_spans`` are built. ``fetch_session_bars_from_config`` is the wiring.

    ``session`` defaults to the Eastern calendar date of ``clock.now()``, which is the session
    the evening run is fetching. A day the calendar says is not a session raises
    ``NotASession``: there is nothing to fetch, and an empty response from the vendor cannot
    tell a holiday apart from a session that yielded nothing.

    **The order of the steps is the whole design.** Each ticker-day is skipped if its partition
    is manifested, fetched outside the lock, built into rows, gated over those rows, and only
    then written. Every one of those has a reason the module docstring or the step's own
    helper carries.

    **The manifested skip is built, never parsed.** ``paths.parse_partition_rel`` returns
    ``None`` for every bars key by design, on both its length check and its membership check,
    and widening that set would let ``partition_path`` build a bars path with no ``freq=``
    level. ``bars_partition_path`` composes the key and membership in ``latest_entries``
    answers the question with no parser.

    Quarantine does not reach that skip, deliberately. A quarantined bars partition is still
    manifested, so the job passes over it and never re-fetches. Nothing writes a verdict until
    marketlake #406's battery does, and it judges chains and quotes rather than bars, so the
    condition has arisen zero times and widening it
    later is a one-line change rather than a second read path.

    **A held bar repeats, and the repeat is bounded differently per cause.** A close
    cross-check with no source because tonight's session has no next partition yet settles
    itself once something comes back for that session. Nothing here does. This function fetches
    the one session it was given, so a caller that hands it today's date every night never
    re-attempts yesterday's, and the bar it held is held forever. Marketlake #422 measured that
    and moved the evening run to :func:`backfill_bars`, whose walk does come back. A caller of
    this function owns re-attempting what it holds. A
    session whose quotes carry gap rows and no data row repeats until someone repairs it, and
    the pile of findings is the record of that. A span refusal repeats without bound on
    purpose, which is marketlake #333's own rule: if the vendor is honouring ``period``, every
    request is short, and stalling is better than landing a wrong answer quietly.

    **Two failures are not contained per ticker-day**, and the catch below is narrow rather
    than broad for exactly that reason. ``VendorAuthError`` is a dead refresh token and is not
    a ``VendorError`` subclass, so a broad catch would have to name it to swallow it: every
    remaining ticker-day fails it identically, and containing it would turn one auth death
    into a page of held findings under an exit code reading "some findings held". And
    ``require_utc_bound`` and ``require_bar_freq`` raise plain ``ValueError``, which is this
    job handing the seam a bad argument. Its own bug must not be absorbed into a nightly held
    finding.
    """
    root = Path(lake_root)
    master = _read_master(root)
    _require_supported(roster)
    recorded_at = clock.now()
    session = session or recorded_at.astimezone(MARKET_TZ).date()
    if not calendar.is_session(session):
        raise NotASession(session)
    walk = _walk(
        root=root,
        vendor=vendor,
        clock=clock,
        calendar=calendar,
        master=master,
        days=tuple(TickerDay(ticker, freq, session) for ticker, freq in _ticker_freqs(roster)),
        recorded_at=recorded_at,
    )
    return BarsReport(
        session=session,
        attempted=walk.attempted,
        landed=walk.landed,
        held=walk.held,
        skipped=walk.skipped,
    )


@dataclass(frozen=True)
class _WalkResult:
    """What one walk over a list of ticker-days did, before a report gives it a shape."""

    attempted: int
    skipped: int
    landed: tuple[LandedPartition, ...]
    held: tuple[HeldFinding, ...]


def _walk(
    *,
    root: Path,
    vendor: Vendor,
    clock: Clock,
    calendar: Calendar,
    master: SecurityMaster,
    days: Sequence[TickerDay],
    recorded_at: datetime,
) -> _WalkResult:
    """Skip, fetch, gate and land every ticker-day in ``days``, in the order given.

    Both entry points reach the vendor through here, so the manifested skip, the gate's blast
    radius and the two failures that are not contained are decided once. The difference between
    a nightly run and a backfill is entirely in which ticker-days each hands over.

    **The session's own instants are computed once per session and only when something is
    attempted.** A backfill walks many sessions, and each needs its bounds and its calendar-next
    session, which are pure functions of the day. A run that skips every ticker-day asks the
    calendar nothing at all.
    """
    paths = LakePaths(root)
    session_clock = SessionClock(clock, calendar)
    # Read once for the run, so every ticker-day is asked against one snapshot of what the
    # lake already holds, the way the close guard reads the manifest once per run.
    manifested = latest_entries(root)
    bounds_of: dict[date, SessionBounds] = {}
    following_of: dict[date, date | None] = {}

    attempted = 0
    skipped = 0
    landed: list[LandedPartition] = []
    held: list[HeldFinding] = []

    def hold(finding: Withheld) -> None:
        # The sequence is the caller's, because ``report`` has only module functions and a
        # counter there would be module state no test could drive. One run files under one
        # injected clock and one pid, so without it two findings on one subject would race for
        # one name. This walk can produce that race, unlike the sibling's: a ticker-day files
        # at most one finding per frequency, and the resolution finding rides beside a gate
        # finding on the same subject. The counter runs across the whole walk rather than per
        # session, which a backfill needs: findings for two sessions land in two directories,
        # so a per-session counter would be correct too, and one counter is one fewer thing to
        # be right about.
        try:
            filed_at = write_withheld(root, finding, now=recorded_at, sequence=len(held))
        except OSError as exc:
            # Named on stderr and carried on the report, the way ``compact`` treats a
            # schema-drift file it could not write. The walk goes on, because one unwritable
            # file is not the other tickers' bars to lose.
            print(
                f"bars: {finding.symbol} {finding.event} "
                f"{finding.observed_on.isoformat()} {finding.check} could not be filed: "
                f"{type(exc).__name__}",
                file=sys.stderr,
            )
            held.append(
                HeldFinding(finding=finding, filed_at=None, filing_error=type(exc).__name__)
            )
            return
        held.append(HeldFinding(finding=finding, filed_at=filed_at))

    for day in days:
        ticker, freq, session = day.ticker, day.freq, day.session
        partition = paths.bars_partition_path(ticker, freq, session)
        key = partition.relative_to(root).as_posix()
        if key in manifested:
            skipped += 1
            continue
        attempted += 1
        if session not in bounds_of:
            bounds_of[session] = session_clock.bounds(session)
            following_of[session] = _calendar_next_session(calendar, session)
        window = bar_window(freq, bounds_of[session])
        try:
            _land(
                root=root,
                vendor=vendor,
                clock=clock,
                master=master,
                ticker=ticker,
                window=window,
                following=following_of[session],
                partition=partition,
                key=key,
                hold=hold,
                landed=landed,
            )
        except (LoadError, VendorError, CloseOfRecordDisagrees, *UNFIT_ERRORS) as exc:
            # ``UNFIT_ERRORS`` is the journal module's own name for the ways a column build
            # refuses one value, which is what a vendor retyping a candle field raises out of
            # ``bars_data_batch``. The tuple is named rather than restated, because a second
            # copy of that list already went a family short once. Without it one retyped
            # ``volume`` ends the run and every ticker after it loses its bars, which is the
            # opposite of what this module's docstring promises.
            #
            # ``CloseOfRecordDisagrees`` is named rather than its ``BarsError`` base, so a
            # later subclass has to decide for itself whether it belongs in here.
            # ``StampNotAnInstant`` decided to stay out, and its own docstring carries why: a
            # stamp it refuses means the row builder's one-offset guarantee has broken for the
            # run rather than for this ticker-day, so containing it here would file the same
            # finding for every ticker and still write the run as though it had worked.
            #
            # The blast radius is one ticker-day. A session the lake cannot read and a vendor
            # that refused this request are both conditions the next run can meet differently,
            # and neither is the other tickers' bars to lose. ``SnapMalformed`` is a
            # ``ValueError`` as well as a ``LoadError`` and is caught here on purpose, because
            # it names a partition this lake wrote rather than an argument this job passed.
            # The token says which condition refused this ticker-day, so it follows the
            # cause rather than being one name for every contained failure. A vendor that
            # refused the request never reached the gate, which is what ``CHECK_BAR_RESPONSE``
            # is for, and a session the lake cannot read is the close cross-check having no
            # source. Filing both under the close check would tell an operator the close
            # disagreed when no close was ever read.
            token = CHECK_BAR_RESPONSE if isinstance(exc, VendorError) else CHECK_BAR_CLOSE
            hold(_finding(ticker, window, token, exception=f"{type(exc).__name__}: {exc}"))

    return _WalkResult(attempted=attempted, skipped=skipped, landed=tuple(landed), held=tuple(held))


def _land(
    *,
    root: Path,
    vendor: Vendor,
    clock: Clock,
    master: SecurityMaster,
    ticker: str,
    window: BarWindow,
    following: date | None,
    partition: Path,
    key: str,
    hold,
    landed: list[LandedPartition],
) -> None:
    """One ticker-day-frequency, from the fetch to the partition, or to a held finding.

    Split out of the walk so the walk reads as the order of its steps rather than as one long
    body, and so the containment above wraps one call rather than a block.
    """
    fetch_ts = clock.now()
    response = _fetch(vendor, ticker, window)
    fetch_end_ts = clock.now()

    # A non-2xx arrives as an ordinary response, because the seam returns the vendor's status
    # verbatim and raises on nothing. Capture's answer is a gap row carrying ``http_<status>``,
    # and bars have no gap rows, so there is no row for one to land in. It is refused instead,
    # before anything reads a body that is not a payload.
    if not 200 <= response.status < 300:
        hold(
            _finding(
                ticker,
                window,
                CHECK_BAR_RESPONSE,
                exception=f"VendorStatus: http_{response.status}",
            )
        )
        return

    # The empty-window rule, picked on purpose out of the two wrong answers a session yielding
    # no bars has. Landing a zero-row partition is the worse one: it satisfies the manifested
    # skip forever, so the session's bars are never fetched again and nothing says the file is
    # empty because the vendor sent nothing. Landing nothing leaves the ticker-day to the next
    # run, which is the recoverable direction, and the finding is what keeps it from being
    # silent. This walk only ever fetches sessions, so an empty response here is a session that
    # traded and came back with nothing, never a holiday: ``Calendar.is_session`` told those
    # apart before the request went out, which is what the payload cannot do.
    #
    # It is decided here rather than falling through to the span check, because that check is
    # defined over a non-empty candle list. An empty window covers zero of its span, so a span
    # check reaching it would refuse every empty response as a coverage failure and this rule
    # would be dead code. The token is the same one a non-2xx files under, because both are one
    # condition: the fetch produced no candle to gate.
    candles = response.body.get("candles")
    if not candles:
        hold(_finding(ticker, window, CHECK_BAR_RESPONSE, computed=0.0, against=None))
        return

    # Resolved per ticker-day, never once per ticker, for the reason ``actions._by_ticker``
    # gives: a symbol handed between two instruments carries each day's own ``instrument_id``.
    # The resolution date is the observation date, which for a bar is its session. A failure
    # files and the row still lands with a null id, because the column is nullable and that is
    # the schema answering what becomes of the bar.
    instrument_id: int | None = None
    try:
        instrument_id = resolve_instrument(master, ticker, window.session)
    except (UnresolvedSymbol, AmbiguousSymbol) as exc:
        # Both ways the resolution can fail file a finding and the row still lands.
        # ``UnresolvedSymbol`` says the master and the capture spans disagree about a ticker,
        # ``AmbiguousSymbol`` says the master is corrupt, and the plural field files the
        # several instruments a corrupt master returned for one symbol. The bar is not held
        # for either, because ``instrument_id`` is nullable and the schema already answers what
        # becomes of a row whose ticker the master cannot place. The exception is rendered as
        # its class and then its message, because ``write_withheld`` composes
        # ``<symbol>: <exception>`` and keeps the first two fields, so a bare message would
        # file its own first field, which for an ``OSError`` is a path on the capture machine.
        hold(
            _finding(
                ticker,
                window,
                CHECK_INSTRUMENT_RESOLUTION,
                instrument_ids=exc.instrument_ids if isinstance(exc, AmbiguousSymbol) else (),
                exception=f"{type(exc).__name__}: {exc}",
            )
        )

    built = bars_rows(
        response.body,
        ticker=ticker,
        freq=window.freq,
        instrument_id=instrument_id,
        fetch_ts=fetch_ts,
        fetch_end_ts=fetch_end_ts,
        window_start=window.start,
        window_end=window.end,
        extended_hours=None,
    )
    selected = select_session_rows(built, window)

    # The span check runs first of the two. The close cross-check compares the candle whose
    # stamp maps to the session, so on a response that dropped that session it has no candle to
    # compare, and running it first would file a close-cross-check finding for a comparison
    # that never had a bar while the real cause, a response short of what was asked for, went
    # unnamed.
    span = check_bar_span(built, selected, window)
    if not span.covers:
        hold(
            _finding(
                ticker,
                window,
                CHECK_BAR_SPAN,
                computed=span.covered,
                against=span.requested,
            )
        )
        return

    if window.freq == DAILY_FREQ:
        if following is None:
            hold(
                _finding(
                    ticker,
                    window,
                    CHECK_BAR_CLOSE,
                    exception="NoFollowingSession: the calendar names no session after this one",
                )
            )
            return
        settled = _settled_close(root, ticker, window.session, following)
        cross = check_close_cross(_bar_close(selected), settled)
        if not cross.agrees:
            hold(
                _finding(
                    ticker,
                    window,
                    CHECK_BAR_CLOSE,
                    computed=cross.computed,
                    against=cross.against,
                    instrument_id=instrument_id,
                )
            )
            return

    # The write and its manifest entry, inside one lock hold. The fetch above is outside it on
    # purpose: ``lake_lock`` is a blocking exclusive ``flock`` on the manifest, and a vendor
    # round trip inside it would put a network call in front of capture's per-minute append.
    # The 18:30 run sits after the last capture slot, so that contention is unreachable there,
    # but a mid-session hand run of this command is exactly what
    # ships here. The entry is inside because ``manifest.py`` rests the backup watermark on
    # every lake write appending its entry under this lock, and an eighth writer outside it
    # would make that false and set the weekly scrub crying loss over a file the sync had not
    # reached.
    with lake_lock(root):
        rows = _write_partition(selected, partition)
        record_partition(
            root,
            key,
            source=SWEEP_SOURCE,
            rows=rows,
            fetched_at=fetch_ts.isoformat(),
        )
    landed.append(
        LandedPartition(
            ticker=ticker,
            freq=window.freq,
            session=window.session,
            partition=key,
            rows=rows,
        )
    )


def _bar_close(rows: Sequence[dict]) -> float | None:
    """The close of the session's own bar, or ``None`` when the rows carry none.

    ``selected`` holds the rows for the session being gated, which on ``1d`` is the one candle
    whose stamp maps to it. The last by stamp is the session's close on either frequency, so
    the same reading answers a 1-min partition should a check ever want it.

    **The last by stamp is the last instant, not the last string.** This sorted the raw text
    until marketlake #386, and the two orders come apart whenever two stamps spell one instant
    differently. That input is not reachable through the row builder, which mints every
    ``bar_ts`` from an epoch through ``journal.epoch_ms_to_utc`` at one offset, so the text sort
    answered correctly for as long as that held. It held two modules away and nothing here said
    so, which is the whole of what was wrong with it. ``load_bars`` orders its own answer by
    instant, and ``lake.settle`` takes the last row of it, so the two readings of one session's
    close now agree by construction rather than by coincidence.

    **The tie is stated rather than inherited, and it is ``load_bars``'s.** Two candles can name
    one instant while spelling it differently, and the two readings used to split on that:
    ``max`` returns the first maximal row, where ``load_bars`` sorts stably and ``lake.settle``
    takes the last. Measured on that pair, the gate read 700.0 and the view read 757.39. Sorting
    and taking the last row is ``load_bars``'s own rule, so this takes it rather than minting a
    second one, and the agreement above covers a tie rather than stopping short of it.

    No such pair exists to measure against, because the lake holds no bars at all yet. That is
    the reason to state the rule now rather than after one arrives.

    **Parsing here cannot raise in the sweep, and the order is why.** ``_as_instant`` raises on a
    stamp ``str`` would have swallowed. Every row reaching this has already been parsed twice,
    by ``select_session_rows`` and then by ``check_bar_span``, so a stamp that would raise has
    ended the ticker-day before this runs. A change that reorders those two checks puts the raise
    back on this path, where the ``except`` below does not cover it: that clause is about a close
    the vendor retyped, not about a stamp.
    """
    if not rows:
        return None
    latest = sorted(rows, key=_instant)[-1]
    value = latest.get("close")
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        # A vendor that retypes the close sends something no comparison can use, and reading it
        # is not this job handing the seam a bad argument. So it reads as no figure and the
        # check holds the bar for a missing input, rather than ending the run on a traceback
        # and costing every ticker after it. A bool is excluded by name, because ``float(True)``
        # is ``1.0`` and would compare as a plausible price.
        return None


# -- the backfill: every session the capture spans cover ----------------------


def read_capture_spans(lake_root: Path) -> CaptureSpans:
    """The capture spans, or the reason a walk over them stops. Mirrors :func:`_read_master`.

    Public because it has two callers. ``lake.sweep`` reads the spans to hand
    :func:`backfill_bars` the range the nightly run walks, and it needs the same
    absent-against-torn distinction this draws rather than a second reading of the same file.
    Marketlake #422.

    An absent file and a torn one are told apart, because the fixes differ: one wants
    ``python -m lake.seed_spans`` and the other wants a restore. ``CaptureSpans.read`` already
    separates them, raising ``FileNotFoundError`` for the first and ``SpansUnreadable`` for the
    second, and this only gives the first a name of its own.

    It catches that one class rather than the whole ``OSError`` family, which is narrower than the
    four other readers of this file. They answer a scope question and widen on anything they
    cannot read, so an unreadable file costing them their answer is the same as an absent one. This
    is a command, and the difference reaches a person: a permission or I/O failure on a file that
    is there would be reported as "no capture spans" and would send them to the seeder, which
    reads the same file and fails the same way. ``_read_master`` draws the line in the same place
    one reference file over.
    """
    path = spans_path(lake_root)
    try:
        return CaptureSpans.read(path)
    except FileNotFoundError as exc:
        raise SpansAbsent(path) from exc


def _span_sessions(
    span: CaptureSpan, session_clock: SessionClock, calendar: Calendar, now: datetime
) -> Iterator[date]:
    """Every session in range for one span, in date order.

    A day is in range when three things hold at once, and the three are the floor, the scope and
    the ceiling in one predicate rather than three rules to keep in step.

    1. **The calendar says it is a session.** A span is continuous time, so which days inside it
       traded is ``Calendar.is_session``'s answer and never a weekday arithmetic of this
       module's.
    2. **The session's own window intersects the span.** The window is ``[open, equity_close]``,
       which is the window a ``1m`` fetch asks for, and the span is half-open ``[start, end)``.
       So a span opening at 13:07 Eastern puts that whole session in range and the session
       before it out of range.

       That is the floor decision, and the alternative was the first *whole* session after the
       span start. It loses a session the outage covered, and the live lake's span opens
       mid-session on 2026-09-08, which is one of the four sessions the recovery exists for. The
       window is not clipped to the span, because a ``1m`` partition trimmed to 13:07 would land
       short under a manifested entry that the skip never re-fetches, and nothing would mark it
       short. A bar is the vendor's record of the session rather than the lake's record of its
       own coverage, and no column on a bars row claims capture was running.
    3. **The equity close has arrived.** That is the ceiling, and it makes a partial session
       unreachable rather than asking an operator not to run mid-session. ``equity_close`` is
       where the ``1m`` window ends, so a session at or past it is complete, and an early close
       moves the bound without anything here naming a wall-clock time. The comparison is inclusive
       for that reason: a run at exactly the close is asking for a window that has just finished,
       not one still open. A run before the close leaves
       that session to the next run.

    The same predicate answers both ends, so a closed span's final session is decided by the
    rule that decided an open span's first one.

    **A span covering no instant at all covers no session either.** ``close_span`` permits an end
    equal to the start, refusing only an end before it, so a retire landing in the same microsecond
    as the onboard leaves a half-open ``[t, t)`` that holds nothing. ``CaptureSpans.in_scope``
    answers ``False`` for that instant and for either side of it, and the intersection test above
    would answer ``True`` for the whole session, which is the lake's own scope reading and this
    walk disagreeing. The walk is the one that would then spend a request and land a permanent,
    manifested partition for a session capture never covered, which is exactly what taking the
    range from the spans rather than from a typed date is supposed to make impossible. It is
    refused here rather than later because the manifested skip never re-fetches what has landed,
    so a guard added afterwards would leave the partition on disk.
    """
    if span.end is not None and span.end <= span.start:
        return
    market_today = now.astimezone(MARKET_TZ).date()
    first = span.start.astimezone(MARKET_TZ).date()
    last = market_today
    if span.end is not None:
        last = min(span.end.astimezone(MARKET_TZ).date(), market_today)
    day = first
    while day <= last:
        if calendar.is_session(day):
            bounds = session_clock.bounds(day)
            covered = bounds.equity_close > span.start and (
                span.end is None or bounds.open < span.end
            )
            if covered and bounds.equity_close <= now:
                yield day
        day += timedelta(days=1)


@dataclass(frozen=True)
class BackfillPlan:
    """Every ticker-day a backfill run would fetch, and what it could not name.

    ``days`` is sorted by session, then ticker, then frequency, so a run reads chronologically
    and two runs over one lake produce the same order. It is built as a set first, for the
    reason :func:`_ticker_freqs` gives about a roster naming one frequency twice: one partition
    fetched twice spends two vendor requests and appends two manifest entries for one path. A
    span walk reaches that hazard two further ways. An instrument retired mid-session and brought
    back the same afternoon has two spans that both cover that session, which ``open_span``
    permits because it refuses only a second *open* span. And two instruments whose mappings name
    one ticker on one day reach it from the other side.

    ``sessions`` is every session the range covered, including one whose tickers all landed in
    ``unwalked``, because that is what the range means.

    ``unwalked`` is one line per ticker-day the scope could not name, and the run reports them
    rather than raising. A span whose instrument the master cannot place on a day has no symbol
    to ask the vendor for, and a ticker with no roster entry has nothing saying which frequencies
    it took. Neither is the other tickers' bars to lose, which rules out raising, and a ticker
    dropping out of a backfill unremarked is the quiet direction, which rules out silence.
    """

    days: tuple[TickerDay, ...]
    sessions: tuple[date, ...]
    unwalked: tuple[str, ...]

    @property
    def floor(self) -> date | None:
        """The first session in range, or ``None`` when the range is empty."""
        return self.sessions[0] if self.sessions else None

    @property
    def ceiling(self) -> date | None:
        """The last session in range, or ``None`` when the range is empty."""
        return self.sessions[-1] if self.sessions else None


def plan_backfill(
    *,
    spans: CaptureSpans,
    master: SecurityMaster,
    roster: Roster,
    clock: Clock,
    calendar: Calendar,
) -> BackfillPlan:
    """Which ticker-days a backfill run covers, decided before any request goes out.

    It is a pure function of the four references and the clock, which is what lets the range
    rules be tested without a lake, a vendor or a partition on disk.

    **The ticker comes from the master, per session, and the direction matters.**
    ``resolve_instrument`` goes ticker to id, which is what ``_land`` does once a ticker-day
    exists. A span carries an ``instrument_id`` and no ticker, so this goes the other way,
    through ``SecurityMaster.symbol_at``. Asking per session rather than once per span is what
    makes a re-symboling inside a span resolve each session to the ticker current on it. A
    ``None`` is the master's own floor arriving from the other side, since a session inside a
    span but before the mapping's ``valid_from`` has no symbol to fetch under.

    **The frequencies come from ``Roster.get``, past ``Roster.enabled``.** ``retire`` flips
    ``enabled`` and preserves ``bars``, so a retired ticker's frequencies survive exactly for
    this walk. An entry with an empty ``bars`` tuple contributes nothing and is not an error.
    """
    session_clock = SessionClock(clock, calendar)
    now = clock.now()
    planned: set[TickerDay] = set()
    sessions: set[date] = set()
    unwalked: dict[str, None] = {}
    for span in spans:
        for day in _span_sessions(span, session_clock, calendar, now):
            sessions.add(day)
            ticker = master.symbol_at(span.instrument_id, day)
            if ticker is None:
                unwalked[
                    f"instrument {span.instrument_id} on {day.isoformat()}: "
                    "the master names no ticker for it on that day"
                ] = None
                continue
            try:
                entry = roster.get(ticker)
            except TickersError as exc:
                unwalked[f"{ticker} on {day.isoformat()}: {exc}"] = None
                continue
            for freq in entry.bars:
                planned.add(TickerDay(ticker=ticker, freq=freq, session=day))
    return BackfillPlan(
        days=tuple(sorted(planned, key=lambda day: (day.session, day.ticker, day.freq))),
        sessions=tuple(sorted(sessions)),
        unwalked=tuple(unwalked),
    )


def _require_supported_plan(plan: BackfillPlan) -> None:
    """Refuse a frequency this lake has no vendor call for, over the plan's own ticker-days.

    :func:`_require_supported` cannot answer for this walk, and its docstring says why in its own
    terms: it checks ``roster.enabled`` because "a stale ``bars:`` line on a retired ticker, which
    nothing here would ever fetch, would halt the nightly run at exit 2 every night until someone
    edited a file for a ticker that is not being captured." This walk fetches retired tickers on
    purpose, so that sentence stops holding and reusing the function would let the frequency
    through.

    What it would cost is not a held finding. ``bar_window`` calls ``require_bar_freq``, which
    raises a bare ``ValueError``, and that is raised outside the walk's catch by design, because a
    ``ValueError`` there is this job handing the seam a bad argument. So the run would end on a
    traceback with no report at all.

    Checking the plan rather than the roster also keeps :class:`UnsupportedBarFreq`'s own bargain:
    the check is exactly as wide as what this run would fetch, and it runs before any request has
    gone out or any partition has been written.
    """
    for day in plan.days:
        if day.freq not in BAR_FREQS:
            raise UnsupportedBarFreq(day.ticker, day.freq)


@dataclass(frozen=True)
class BackfillReport:
    """What one backfill run did, for the sign-off block.

    It carries the range as well as the counts, because a run that landed nothing has three
    different causes an operator has to tell apart: an empty range, a range whose every
    ticker-day was already manifested, and a range whose every ticker-day was held. ``sessions``
    with ``skipped`` and ``held`` says which.
    """

    floor: date | None
    ceiling: date | None
    sessions: int
    attempted: int
    landed: tuple[LandedPartition, ...]
    held: tuple[HeldFinding, ...]
    skipped: int
    unwalked: tuple[str, ...]

    @property
    def unfiled(self) -> tuple[HeldFinding, ...]:
        """Every held finding whose record could not be written down."""
        return _unfiled(self.held)

    def render(self) -> str:
        """A human-readable sign-off block."""
        span = (
            "no sessions in range"
            if self.floor is None
            else f"{self.floor.isoformat()}..{self.ceiling.isoformat()}, {self.sessions} session(s)"
        )
        lines = [
            f"Bar backfill over {span}, {self.attempted} ticker-day(s) attempted",
            f"  landed:  {len(self.landed)}",
        ]
        for entry in self.landed:
            lines.append(
                f"    - {entry.ticker} {entry.freq} {entry.session.isoformat()} "
                f"{entry.rows} row(s) at {entry.partition}"
            )
        lines.extend(_render_held(self.held))
        lines.append(f"  skipped: {self.skipped}")
        lines.append(f"  unwalked: {len(self.unwalked)}")
        for line in self.unwalked:
            lines.append(f"    - {line}")
        return "\n".join(lines)


def backfill_bars(
    *,
    lake_root: Path | str,
    vendor: Vendor,
    clock: Clock,
    calendar: Calendar,
    roster: Roster,
    spans: CaptureSpans,
) -> BackfillReport:
    """Fetch, gate and land every ticker-day the capture spans cover and the clock allows.

    This is marketlake #319. Every dependency is injected, the way :func:`fetch_session_bars` is
    built, and :func:`backfill_bars_from_config` is the wiring.

    **One request per ticker-day, per frequency.** Schwab's call takes a window, so one request
    could ask for a whole span. That shape is refused, and the gate rather than the request count
    refuses it: ``bar_window``, ``check_bar_span``, ``select_session_rows`` and the close
    cross-check are each defined over one session, and rewriting all four is not what this adds.
    It also sidesteps marketlake #333 entirely, where ``schwab-py`` sends
    ``period=1, periodType=day`` alongside explicit bounds against its own documented rule: a
    request for one session is what that pair would return anyway. And it makes the manifested
    skip avoid the vendor call as well as the write, so a second run over the same lake costs
    zero requests.

    The cost was measured rather than assumed. Against the real calendar the live lake's floor to
    2026-09-16 holds seven sessions and its floor to the 1-min lookback deadline holds twenty-two,
    so a first run is 28 requests and a run at the deadline is 88. The design's ceiling is 120 a
    minute per app, of which the capture loop spends 3 at this roster.

    **What the gate refuses here repeats, and the repeat is the record.** The sessions this
    recovers are exactly the ones whose quotes hold gap rows and no data row, so the close
    cross-check has no comparison for them and ``load_quotes`` raises ``NoSpotClose``. That is
    contained per ticker-day by the walk and files under ``CHECK_BAR_CLOSE``, which is
    :func:`fetch_session_bars`'s rule rather than a new one, and it means the daily half of those
    sessions is held until someone repairs the gap rows. The minute half runs no close
    cross-check at all, and the minute half is the one the roughly 30-day lookback puts a deadline
    on.

    A session older than that lookback is still asked for on every run, and comes back empty or
    short and held. That is deliberate. The pile of findings is the record that those minutes are
    gone, and a horizon that stopped asking would make the loss silent on a figure nobody has
    measured exactly.
    """
    root = Path(lake_root)
    master = _read_master(root)
    recorded_at = clock.now()
    plan = plan_backfill(spans=spans, master=master, roster=roster, clock=clock, calendar=calendar)
    _require_supported_plan(plan)
    walk = _walk(
        root=root,
        vendor=vendor,
        clock=clock,
        calendar=calendar,
        master=master,
        days=plan.days,
        recorded_at=recorded_at,
    )
    return BackfillReport(
        floor=plan.floor,
        ceiling=plan.ceiling,
        sessions=len(plan.sessions),
        attempted=walk.attempted,
        landed=walk.landed,
        held=walk.held,
        skipped=walk.skipped,
        unwalked=plan.unwalked,
    )


# -- the entry point ---------------------------------------------------------


def fetch_session_bars_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    vendor_factory: VendorFactory | None = None,
    token_path: str | Path = DEFAULT_TOKEN_PATH,
) -> BarsReport:
    """The sweep wired from the real config. This is the entry :func:`main` calls.

    **The vendor arrives through a factory**, which is ``record.py``'s shape rather than
    ``capture``'s. ``capture`` calls ``SchwabVendor.from_token`` inside its production entry
    and keeps the injected vendor on the core beneath it, which leaves its own ``main``
    untestable without the network. This command's exit codes are part of its contract, and
    two of them can only be driven through a vendor: a dead refresh token stops the run, and a
    finding that could not be filed exits 1. So the factory is the argument, and a test injects
    one returning a cassette-backed fake.

    ``from_token`` imports ``schwab-py`` lazily either way, which is what keeps the offline
    suite running without the library installed.
    """
    from lake.config import load_config

    config = load_config(config_path)
    factory = SchwabVendor.from_token if vendor_factory is None else vendor_factory
    vendor = factory(
        token_path,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return fetch_session_bars(
        lake_root=config.lake_root,
        vendor=vendor,
        clock=SystemClock() if clock is None else clock,
        calendar=ExchangeCalendar(),
        roster=load_tickers(tickers_path),
    )


def backfill_bars_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    vendor_factory: VendorFactory | None = None,
    token_path: str | Path = DEFAULT_TOKEN_PATH,
) -> BackfillReport:
    """The backfill wired from the real config, taking the same arguments as its sibling.

    The two wirings differ in one line, the capture spans this one reads from under
    ``lake_root``, which is what makes ``--backfill`` a flag on one command rather than a second
    command with its own copy of the vendor factory and the two secrets.
    """
    from lake.config import load_config

    config = load_config(config_path)
    factory = SchwabVendor.from_token if vendor_factory is None else vendor_factory
    vendor = factory(
        token_path,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return backfill_bars(
        lake_root=config.lake_root,
        vendor=vendor,
        clock=SystemClock() if clock is None else clock,
        calendar=ExchangeCalendar(),
        roster=load_tickers(tickers_path),
        spans=read_capture_spans(Path(config.lake_root)),
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.bars",
        description=(
            "Fetch one session's equity bars from the vendor, gate them, and land the "
            "partitions that pass. A failure means the partition never lands."
        ),
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument(
        "--tickers", help="Path to tickers.yaml (defaults to the standard location)."
    )
    # No flag names the session. The command fetches the session the clock is in, which is
    # what the evening run wants, and ``actions.main`` sets the same shape by taking
    # ``--config`` alone.
    #
    # A ``--date`` flag was written and removed before merge. It reads as a convenience and is
    # a backfill selector: nothing consulted a capture-span floor, and the security master
    # is not the barrier it looks like, because a session the master cannot place files a
    # finding and lands the row anyway with a null ``instrument_id``. So the flag would have
    # let one typo land bars for a session the lake never captured.
    #
    # ``--backfill`` is marketlake #319's answer to the same need, and it is the shape that
    # objection asked for: it takes no date at all. The range is derived from the capture
    # spans, the calendar and the clock, so a session the lake never captured cannot be named
    # by anyone. The session stays injectable on :func:`fetch_session_bars` for tests.
    #
    # The design's "there is no backfill anywhere in the implementation" is about pre-capture
    # history, and the floor is what keeps it true: every session this walks is one a capture
    # span covers, so the run works forward from capture start and asks for nothing earlier.
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=(
            "Walk every session the capture spans cover whose close has passed, instead of "
            "fetching the session the clock is in. It reaches nothing before capture start."
        ),
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    vendor_factory: VendorFactory | None = None,
) -> int:
    """The ``python -m lake.bars`` entry. Returns a process exit code.

    ``clock`` stays injectable for the reason ``actions.main`` gives: a wall clock never
    reaches past this process, and what "a second night" means has to be something a test
    decides.

    Seven conditions reach the operator as one line rather than a stack, and all but the last
    carry a fix.

    1. An absent master wants the onboarding command, and a torn one wants a restore, because an
       operator told to seed a corrupt file is being told the wrong thing.
    2. An absent capture-spans file wants ``python -m lake.seed_spans``, which is what a master
       from before that file existed needs, and a torn one wants a restore.
    3. A roster frequency nothing can fetch wants an edit to ``tickers.yaml``.
    4. A day that is not a session has no bars to fetch.
    5. A dead refresh token wants the reauth command, and it is named rather than contained
       because every remaining ticker-day would fail it identically.
    6. Every other capture-spans failure is named and nothing is prescribed, because the
       exception's own message is what says what is wrong.

    The count was already one short of the handlers below before ``--backfill`` added two, so it
    is a list rather than a sentence now: a list that disagrees with the code disagrees visibly.

    **A finding the run could not write down is what the third exit code is for.** The walk
    contains that failure so one unwritable file does not cost the other tickers their bars,
    and this is where it stops being silent. A run that held something and filed nothing reads
    exactly like a run that found nothing.
    """
    args = _build_parser().parse_args(argv)

    from lake.config import input_errors_exit

    run = backfill_bars_from_config if args.backfill else fetch_session_bars_from_config
    try:
        with input_errors_exit("bars"):
            report = run(
                clock=clock,
                config_path=args.config,
                tickers_path=args.tickers,
                vendor_factory=vendor_factory,
            )
    except MasterAbsent as exc:
        print(
            f"bars: {exc}. Onboard a ticker first, with python -m lake.onboard <TICKER>.",
            file=sys.stderr,
        )
        return 2
    except MasterUnreadable as exc:
        print(f"bars: {exc}. Restore it from the backup.", file=sys.stderr)
        return 2
    except SpansAbsent as exc:
        print(
            f"bars: {exc}. Seed them with python -m lake.seed_spans.",
            file=sys.stderr,
        )
        return 2
    except SpansUnreadable as exc:
        print(f"bars: {exc}. Restore it from the backup.", file=sys.stderr)
        return 2
    except CaptureSpansError as exc:
        # The class rather than its members, which is what the four other readers of this file
        # already catch. Naming ``SpansUnreadable`` alone left its sibling
        # ``UnsupportedSpansSchemaVersion`` reaching the operator as a stack, and a spans file from
        # a newer version of this code is the one shape of it a person actually meets. No fix is
        # invented for the rest, because the exception's own message is what says what is wrong
        # and a wrong instruction is worse than none.
        print(f"bars: {exc}.", file=sys.stderr)
        return 2
    except UnsupportedBarFreq as exc:
        print(f"bars: {exc}. Fix the bars list in tickers.yaml.", file=sys.stderr)
        return 2
    except StampNotAnInstant as exc:
        # The refusal reaches a person as one line rather than a stack, which is the treatment
        # ``UnsupportedBarFreq`` above already gets and the rule ``docs/design.md`` states. No
        # instruction is invented past the exception's own message, because that message already
        # names the offset form to pass and a wrong instruction is worse than none.
        print(f"bars: {exc}", file=sys.stderr)
        return 2
    except NotASession as exc:
        print(f"bars: {exc}, so there are no bars to fetch.", file=sys.stderr)
        return 2
    except VendorAuthError as exc:
        print(
            f"bars: the vendor refused the credentials ({type(exc).__name__}), so the run "
            "stopped. Mint a new token with python -m lake.reauth.",
            file=sys.stderr,
        )
        return 2
    print(report.render())
    return 1 if report.unfiled else 0


__all__ = [
    "VendorFactory",
    "CHECK_BAR_CLOSE",
    "CHECK_BAR_RESPONSE",
    "CHECK_BAR_SPAN",
    "CLOSE_CROSS_TOLERANCE",
    "DAILY_WINDOW_MARGIN",
    "BackfillPlan",
    "BackfillReport",
    "BarWindow",
    "BarsError",
    "BarsReport",
    "CloseCross",
    "LandedPartition",
    "SpanCoverage",
    "SpansAbsent",
    "StampNotAnInstant",
    "TickerDay",
    "UnsupportedBarFreq",
    "backfill_bars",
    "read_capture_spans",
    "backfill_bars_from_config",
    "bar_window",
    "check_bar_span",
    "check_close_cross",
    "fetch_session_bars",
    "fetch_session_bars_from_config",
    "main",
    "plan_backfill",
    "select_session_rows",
    "session_of",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
