"""The OI view: what a session's open interest settled at, read one session later.

Open interest, OI, is the count of contracts outstanding. The vendor does not serve a
session's settled OI during that session. It loads the figure overnight, so the chain
carries the previous session's number all day and the new one appears before the next
session opens. So session S's OI is not in S's own chains at all. It is in the chains of
the session after S, and finding it is a matter of spotting where the number changed.

marketlake #137 is authoritative for this deliverable and holds the rule in full. What
follows is what the code does and why each step is shaped the way it is.

A verdict for one ticker-session is built in four steps.

1. *The baseline.* Session S's close of record, which is ``load_chain(ticker, S)`` with
   ``snap=None``. It resolves against the ``close_tag`` column rather than against the
   largest ``snap_ts``, and that distinction is load-bearing rather than pedantic.
   Onboarding journals a chain snapshot under the session date at whatever hour it runs,
   so a session can hold a cycle after its own close that carries stale quotes. Such a
   cycle is tagged nothing, so a tag resolution cannot pick it up and a
   largest-``snap_ts`` resolution would. The baseline has no quorum behind it and takes
   one cycle at face value, which is why it takes the cycle capture labelled rather than
   the one that happens to sort last.
2. *The comparable set.* S's top-volume contracts whose expiration falls strictly after
   S, ranked at S and never re-ranked afterwards. The expiration filter carries the set
   rather than trimming its edge, because the busiest contracts at a close are usually
   the ones expiring that day: 27 of the 50 highest-volume contracts in SPY's 2026-09-14
   close expired that same day, and every one of them is gone from the next chain. Equal
   volumes break on the OCC symbol, so the set is the same set on every run. Ranking is
   what buys the margin the quorum below relies on.
3. *The walk.* The calendar-next session's stored cycles, in order, looking for the first
   whose OI differs from the baseline across enough of the set.
4. *The answer.* One row per contract in S's own close roster, carrying either the
   settled OI or a marker saying why there is none.

Four rules decide the walk, and each exists because something in the real data would
otherwise get through.

*Calendar-next, never the next session that happens to hold data.* A holiday is not a
session, so stepping to the calendar-next session steps over it and nothing settled in
between anyway. A capture gap is the opposite case. Sessions traded and were not
recorded, and reading forward past them would label S with OI that settled after some
later session's trading. That figure is real and belongs to somebody else. So a
calendar-next session holding no stored data cycles makes S absent and the walk stops
rather than looking further forward.

*"Differs" is a quorum, not a single tick.* A refresh is declared only when at least
``oi_refresh_quorum`` of the voters shows changed OI. The voters are the set members the
candidate cycle actually carries, so a cycle that lists only part of the set votes with
what it has. One changed contract is noise.

*The selected cycle must hold.* Its OI stays unchanged over the voters for
``oi_plateau_cycles`` subsequent stored cycles. The vendor's overnight load is not
atomic, and the lake caught it mid-load: a cycle stamped 03:25Z under
``date=2026-09-16`` carried a different OI on 5,830 of the 12,790 SPY contracts it shared
with 2026-09-15's close, and 4,858 of those had traded zero that session and read OI
zero where the close read a real figure. That is a half-written chain, not a settlement.
The quorum turned it away on its own, since none of the top 200 by volume had changed
there, and the plateau would have turned it away even if the quorum had not.

*A contract is followed by its identity, never by its spelling.* The OCC re-symbols every
open contract under a root at once when it adjusts them, which a split is the usual cause of.
A walk matching S's symbols against the next session's would then find none of the set and
call a whole session of still-trading contracts indeterminate. So both sides key on the
``instrument_id`` the security master holds for a symbol, and on the raw symbol where it
holds none. ``lake.occ_mapping`` writes those rows and says what a reader does with them:
almost no contract resolves through the master, so the symbol is the ordinary key and the
instrument is the exception that keeps a re-symboled contract readable. The lookup takes no
date, because the master's validity ranges are drawn by a walk that dates a boundary to the
session it happened to read, and a boundary dated one session late would split the very join
this closes. marketlake #351 is the defect this repairs.

*Pending is not absent.* A verdict withheld because the evidence is not captured yet is a
different answer from one withheld because the evidence was inconclusive, and the newest
sealed session is always in the first state, so this is the common case rather than an
edge. The test reads no clock and has two halves. The calendar-next session is absent
when something newer has already sealed for that ticker, because nothing goes back to
seal it, and absent as well when no capture span for that instrument covers it with
option chains, because nothing will ever capture it. It is pending only when neither
holds. The span half is what a retired or a quotes-only ticker needs: ``retire`` closes
an instrument's span and nothing newer ever seals afterwards, and a span carries
``options``, which capture reads to decide whether chains are fetched at all. A
newest-sealed test on its own would leave a retired ticker's last session, and every
session of a quotes-only one, pending forever.

Absence arrives from the loader as a refusal rather than as an empty table, and each
refusal is caught by name rather than by base. ``LoadError`` also covers ``SnapMalformed``,
which is a caller typo, and two unnamed refusals about a corrupt partition. Turning one of
those into a marker would hide a defect behind something that looks legitimate.

A marker carries its reason, the way the journal pairs a row kind with an ``error_class``
rather than leaving a reader to infer why a value is missing. This view has eight reasons a
row can carry and the loader hands most of them over already, so one undifferentiated
marker would throw away what it was given. A session whose own close cannot be read carries
a ninth, and it refuses rather than returning rows, so ``BaselineAbsent`` carries the reason
and the close-of-record gap count with it. That count separates a close cycle that never ran
from one that ran and failed, which is worth keeping because eight of the thirteen sealed
chains partitions the lake held on 2026-09-16 were in the second case.

Nothing here writes, fetches, or reads a clock. The lake root resolves through
``loader.resolve_lake_root``, which is the read layer's one config read, and every path
below it is derived from the root that returns.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

from lake.calendar import DEFAULT_CALENDAR, Calendar, ExchangeCalendar
from lake.capture_spans import CaptureSpans, CaptureSpansError, spans_path
from lake.capture_spans import spans_of_ticker as _spans_of_ticker
from lake.config import GuardConstants
from lake.loader import (
    NoCloseOfRecord,
    PartitionAbsent,
    PartitionQuarantined,
    SnapAbsent,
    list_chain_cycles,
    load_chain,
    resolve_lake_root,
)
from lake.paths import CHAINS, LakePaths
from lake.security_master import (
    ID_TYPE_OCC,
    SecurityMaster,
    SecurityMasterError,
    master_path,
)

# What a row's ``verdict`` says. ``settled`` is the only one that carries a number.
VERDICT_SETTLED = "settled"
VERDICT_ABSENT = "absent"
VERDICT_PENDING = "pending"
VERDICT_INDETERMINATE = "indeterminate"

# Why a value is withheld. Six of these are properties of the ticker-session and ride every
# row of the answer. The last two vary per contract. ``expired_out`` is decided by the
# contract's own expiration and never by whether the selected cycle happens to list it, so a
# vendor that keeps an expired contract listed for another session cannot turn an
# unobservable figure into a settled one. ``no_value_in_cycle`` is the other direction: the
# contract survives S and the selected cycle carries no number for it, either because the
# cycle does not list it or because it lists it null.
REASON_PARTITION_ABSENT = "partition_absent"
REASON_NOT_YET_CAPTURED = "not_yet_captured"
REASON_NO_DATA_CYCLES = "no_data_cycles"
REASON_PARTITION_QUARANTINED = "partition_quarantined"
REASON_NO_CLOSE_OF_RECORD = "no_close_of_record"
REASON_SET_UNDER_FLOOR = "set_under_floor"
REASON_NO_CYCLE_PASSED = "no_cycle_passed"
REASON_EXPIRED_OUT = "expired_out"
REASON_NO_VALUE_IN_CYCLE = "no_value_in_cycle"

# The answer's shape. ``open_interest`` is null on every row that is not ``settled``, and
# ``reason`` is null on every row that is. ``source_session`` and ``source_snap_ts`` name
# the cycle a settled figure came from, so a reader can go back to the rows it was derived
# from. There is no column for the close-of-record gap count, because the only reason that
# would fill one is a session whose own close cannot be read, and that refuses rather than
# returning rows. ``BaselineAbsent.tagged_gaps`` carries it instead.
OI_VIEW_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("session", pa.string()),
        ("occ_symbol", pa.string()),
        ("open_interest", pa.int64()),
        ("verdict", pa.string()),
        ("reason", pa.string()),
        ("source_session", pa.string()),
        ("source_snap_ts", pa.string()),
    ]
)

# The columns the view reads off a chains row. Named here rather than at each use so the
# set is one list, and so a reader can see at a glance that the view reads four columns
# out of the chains schema's several dozen.
_OCC = "occ_symbol"
_OI = "open_interest"
_VOLUME = "volume"
_EXPIRATION = "expiration_date"
_SNAP_TS = "snap_ts"

# How far forward a walk looks for the calendar-next session before giving up. The
# longest exchange closure in living memory is four sessions, and a calendar that
# answered "not a session" for a year would otherwise spin. This is a runaway guard
# rather than a rule, so it is generous and never reached in practice.
_NEXT_SESSION_SEARCH_DAYS = 30

# The partition file name a sealed session takes under a ticker's directory. Built here
# rather than by asking ``LakePaths`` for one path per candidate date, because this
# reads the directory to find out which dates exist at all.
_PARTITION_NAME = re.compile(r"date=(\d{4}-\d{2}-\d{2})\.parquet")


class OiViewError(Exception):
    """The base every refusal here raises, so one ``except`` covers the view."""


class ScopeUnreadable(OiViewError):
    """Raised when the reference tables cannot say whether the ticker was in scope.

    An absent or torn security master, an absent or torn capture-spans file, or a ticker
    the master cannot resolve all land here. The daemon's own startup walk treats an
    unreadable scope as out of scope and marks nothing, because it runs from an unguarded
    hook and a raise there would take the loop down. A derived view has the opposite
    problem. Silently answering "out of scope" would suppress every verdict in the lake
    and look exactly like a lake with nothing in it, so this refuses instead.
    """

    def __init__(self, ticker: str, day: str, detail: str) -> None:
        super().__init__(f"{ticker} {day} scope is unreadable: {detail}.")
        self.ticker = ticker
        self.day = day


class SessionOutOfScope(OiViewError):
    """Raised when the session was never one this instrument's chains were captured in.

    Two causes, and the message names which. The day is not a trading session at all, or
    no capture span covers its close with option chains. The second is not hypothetical:
    the live lake holds ``chains/ticker=SPY/date=2026-09-02.parquet`` with two data rows,
    from six days before the capture epoch that ``reference/capture_spans.parquet``
    records. A walk over partition files alone would hand that session a verdict. This is
    the clamp that does not.
    """

    def __init__(self, ticker: str, day: str, detail: str) -> None:
        super().__init__(f"{ticker} {day} is out of scope: {detail}.")
        self.ticker = ticker
        self.day = day


class SpellingsCollide(OiViewError):
    """Raised when one cycle carries one contract under two of the spellings it has worn.

    Keying on the instrument is what lets a re-symboled contract be read as renamed rather
    than as absent, and it merges every spelling that contract has worn. So a cycle listing
    the same contract twice, once under each spelling, would collapse to one entry and the
    other figure would be gone with nothing said. That is the failure this threading exists
    to remove, so it refuses instead of picking whichever row was written last.

    Keying on the symbol cannot reach this state, because two spellings are two keys. It
    arrives with the threading, which is why it is named here rather than left to the
    silence that keying on the symbol never had.
    """

    def __init__(self, ticker: str, day: str, spellings: tuple[str, str]) -> None:
        first, second = spellings
        super().__init__(
            f"{ticker} {day} carries one contract as both {first!r} and {second!r} in a "
            f"single cycle, so the two figures cannot both be read."
        )
        self.ticker = ticker
        self.day = day
        self.spellings = spellings


class BaselineAbsent(OiViewError):
    """Raised when session S's own close of record cannot be read.

    The answer's rows are S's close roster, so a session whose close cannot be read has
    no rows to mark rather than rows marked absent. Returning an empty table would make
    "S has no baseline" and "S's close held no contracts" the same answer, which is the
    distinction the loader itself refuses to blur. ``reason`` carries the marker code the
    rows would have taken, so a caller that files verdicts can file this one the same way.
    """

    def __init__(self, ticker: str, day: str, reason: str, cause: Exception) -> None:
        super().__init__(f"{ticker} {day} has no baseline close of record: {cause}")
        self.ticker = ticker
        self.day = day
        self.reason = reason
        self.cause = cause
        # The count of absence markers that carried the close tag, when the loader supplied
        # one. Zero means the close-of-record cycle never ran and a positive count means it
        # ran and failed, which is the majority path: eight of the thirteen sealed chains
        # partitions the lake held on 2026-09-16 raised ``NoOptionClose``, each carrying
        # exactly one tagged gap row. It rides the refusal rather than a column, because a
        # session with no close has no rows to carry a column at all.
        self.tagged_gaps = getattr(cause, "tagged_gaps", None)


@dataclass(frozen=True)
class _Contract:
    """One row of session S's close roster, with what the ranking needs."""

    occ_symbol: str
    open_interest: int | None
    volume: int
    expires_after_session: bool


def oi_view(
    ticker: str,
    day: date | str,
    *,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
    calendar: Calendar | None = None,
    constants: GuardConstants | None = None,
) -> pa.Table:
    """Session ``day``'s settled open interest for ``ticker``, as a table of verdicts.

    One row per contract in the session's close-of-record roster, in that cycle's own
    order. A row is ``settled`` and carries a number, or it names the reason it does not.
    The four guard constants come from ``GuardConstants``, so a caller recalibrating them
    passes its own rather than editing this module.
    """
    session_text = day.isoformat() if isinstance(day, date) else str(day)
    session = date.fromisoformat(session_text)
    root = resolve_lake_root(lake_root)
    guards = constants if constants is not None else GuardConstants()
    market = calendar if calendar is not None else ExchangeCalendar(DEFAULT_CALENDAR)

    master, spans = _require_scope(root, ticker, session, session_text, market)
    # One index per call, built where the master is read. Every join below keys through it,
    # and building it inside the walk instead would rebuild it once per stored cycle.
    occ_index = _occ_index(master, ticker, session_text)

    roster = _read_baseline(ticker, session_text, root, include_quarantined)
    comparable = _comparable_set(roster, guards.oi_comparable_set_size)

    if len(comparable) < guards.oi_comparable_set_floor:
        return _marked(ticker, session_text, roster, VERDICT_INDETERMINATE, REASON_SET_UNDER_FLOOR)

    following = _calendar_next_session(market, session)
    outcome = _walk(
        ticker=ticker,
        following=following,
        comparable=comparable,
        root=root,
        include_quarantined=include_quarantined,
        guards=guards,
        master=master,
        spans=spans,
        market=market,
        occ_index=occ_index,
    )
    if outcome.verdict is not None:
        return _marked(ticker, session_text, roster, outcome.verdict, outcome.reason)
    return _settled(ticker, session_text, roster, outcome, occ_index)


@dataclass(frozen=True)
class _Outcome:
    """Either a whole-session marker, or the cycle a settled answer reads from."""

    verdict: str | None = None
    reason: str | None = None
    source_session: str | None = None
    source_snap_ts: str | None = None
    open_interest: dict[int | str, int | None] | None = None


def _require_scope(
    root: Path, ticker: str, session: date, session_text: str, market: Calendar
) -> tuple[SecurityMaster, CaptureSpans]:
    """Both reference tables, once the session is known to be one the ticker was captured in.

    They come back rather than being read twice, because the walk asks the same scope
    question again about the calendar-next session when that session has not sealed.


    Scope is two questions and they are asked in this order on purpose. A day that is not
    a trading session has no close for a span to contain, so the calendar answers first
    and the spans never see a date they cannot be asked about.
    """
    if not market.is_session(session):
        raise SessionOutOfScope(ticker, session_text, "it is not a trading session")

    master, spans = _reference_tables(root, ticker, session_text)
    if not _covered_with_options(spans, master, ticker, session, market):
        raise SessionOutOfScope(
            ticker,
            session_text,
            "no capture span covers its option close with option chains",
        )
    return master, spans


def _reference_tables(
    root: Path, ticker: str, session_text: str
) -> tuple[SecurityMaster, CaptureSpans]:
    """Both reference tables, or one refusal naming which of them could not be read.

    They are read together because neither answers the scope question alone. Spans are
    keyed on ``instrument_id`` and a caller holds a ticker, so the master is the step
    between the two.
    """
    try:
        master = SecurityMaster.read(master_path(root))
    except (OSError, SecurityMasterError, ValueError) as exc:
        raise ScopeUnreadable(ticker, session_text, f"security master, {exc}") from exc
    try:
        spans = CaptureSpans.read(spans_path(root))
    except (OSError, CaptureSpansError, ValueError) as exc:
        raise ScopeUnreadable(ticker, session_text, f"capture spans, {exc}") from exc
    return master, spans


def _covered_with_options(
    spans: CaptureSpans,
    master: SecurityMaster,
    ticker: str,
    session: date,
    market: Calendar,
) -> bool:
    """Whether a span capturing option chains contains this session's option close.

    The option close is the instant asked about rather than the open, because the close
    of record is the cycle a baseline reads and a session captured from partway through
    still has one. ``options`` is checked as well as containment, since an instrument can
    be captured for its quote alone and then seals no chains partition ever.
    """
    found = _spans_of_ticker(spans, master, ticker, session)
    if not found:
        return False
    close = market.option_close(session)
    return any(span.options and span.contains(close) for span in found)


def _occ_index(master: SecurityMaster, ticker: str, session_text: str) -> dict[str, int]:
    """Every OCC symbol the master holds, to the instrument holding it.

    This is the question ``occ_mapping.instruments_holding`` answers, asked for every symbol
    at once rather than one at a time. The difference is not a preference. ``resolve`` and
    ``instruments_holding`` both scan the whole table, and a re-symboling writes two rows per
    contract, so a SPY-sized adjustment leaves a master of about 25,912 rows that the view
    would otherwise scan once per symbol per cycle. One pass here costs 4.3 ms and every
    lookup after it is a dict hit.

    It takes no date, and that is the load-bearing part. ``load_contract``'s threading gives
    the reason in ``src/lake/loader.py``: the master's validity ranges are drawn by a walk
    that skips sessions for seven reasons and dates a boundary to the session it happened to
    read, so they widen a selection rather than decide one. A boundary dated one session late
    would leave a dated lookup resolving the baseline and the cycle to different keys, which
    is the very break this threading exists to close.

    A symbol two instruments hold is what ``occ_mapping.write_mappings`` refuses to write,
    calling it a master that cannot say which contract it is. Nothing here can say either, so
    it refuses too rather than picking one.
    """
    holders: dict[str, set[int]] = {}
    for mapping in master.mappings:
        if mapping.id_type == ID_TYPE_OCC:
            holders.setdefault(mapping.id_value, set()).add(mapping.instrument_id)
    index: dict[str, int] = {}
    for symbol, owners in holders.items():
        if len(owners) > 1:
            raise ScopeUnreadable(
                ticker,
                session_text,
                f"security master, {symbol!r} names instruments {sorted(owners)}, so it "
                f"cannot say which contract the symbol is",
            )
        index[symbol] = next(iter(owners))
    return index


def _threaded(
    index: dict[str, int],
    pairs: Iterable[tuple[str, int | None]],
    ticker: str,
    day_text: str,
) -> dict[int | str, int | None]:
    """A contract-to-OI map keyed on the thread rather than on the spelling.

    A symbol the master has never seen keys on itself, which is every contract no
    re-symboling has touched. That fallback is the ordinary path rather than a corner, and
    ``occ_mapping`` says so outright: almost no contract resolves through the master, and a
    reader keys on the instrument where there is one and on the symbol where there is not.

    One contract under two spellings in the same map is refused. A repeated spelling is not,
    because keying on the symbol already let the last row win and nothing here is trying to
    change that.
    """
    spellings: dict[int | str, str] = {}
    threaded: dict[int | str, int | None] = {}
    for symbol, value in pairs:
        key = index.get(symbol, symbol)
        seen = spellings.get(key)
        if seen is not None and seen != symbol:
            raise SpellingsCollide(ticker, day_text, (seen, symbol))
        spellings[key] = symbol
        threaded[key] = value
    return threaded


def _read_baseline(
    ticker: str, session_text: str, root: Path, include_quarantined: bool
) -> tuple[_Contract, ...]:
    """Session S's close roster, or the refusal that says there is no baseline.

    Each loader refusal is caught by name. ``PartialRead`` and the unnamed refusals for a
    corrupt partition are deliberately not caught, because a partial projection or a
    close tag on two cycles is a defect rather than an absence, and a marker would bury
    it.
    """
    try:
        table = load_chain(
            ticker, session_text, lake_root=root, include_quarantined=include_quarantined
        )
    except PartitionAbsent as exc:
        raise BaselineAbsent(ticker, session_text, REASON_PARTITION_ABSENT, exc) from exc
    except PartitionQuarantined as exc:
        raise BaselineAbsent(ticker, session_text, REASON_PARTITION_QUARANTINED, exc) from exc
    except NoCloseOfRecord as exc:
        raise BaselineAbsent(ticker, session_text, REASON_NO_CLOSE_OF_RECORD, exc) from exc

    session = date.fromisoformat(session_text)
    columns = {name: table.column(name).to_pylist() for name in (_OCC, _OI, _VOLUME, _EXPIRATION)}
    roster = tuple(
        _Contract(
            occ_symbol=occ,
            open_interest=oi,
            volume=volume or 0,
            expires_after_session=_expires_after(expiration, session),
        )
        for occ, oi, volume, expiration in zip(
            columns[_OCC], columns[_OI], columns[_VOLUME], columns[_EXPIRATION], strict=True
        )
    )
    return roster


def _expires_after(expiration: object, session: date) -> bool:
    """Whether the vendor's expiration string names a date strictly after ``session``.

    A value that cannot be read as a date answers false. That keeps the contract in the
    roster, where it can still take a settled figure, and out of the comparable set,
    where it would be a voter nothing has shown will survive the session.
    """
    if not isinstance(expiration, str):
        return False
    try:
        stamped = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
    except ValueError:
        return False
    return stamped.date() > session


def _comparable_set(roster: tuple[_Contract, ...], size: int) -> tuple[_Contract, ...]:
    """The top-volume contracts expiring after S, ranked once and never re-ranked.

    Equal volumes break on the OCC symbol so the set is reproducible. The tie is real
    rather than theoretical: 2 to 4 contracts shared the boundary volume at rank 1,000 on
    each of the four sealed close cycles the lake held on 2026-09-16.
    """
    survivors = [row for row in roster if row.expires_after_session]
    survivors.sort(key=lambda row: (-row.volume, row.occ_symbol))
    return tuple(survivors[:size])


def _calendar_next_session(market: Calendar, session: date) -> date | None:
    """The first trading session strictly after ``session``.

    The calendar answers whether one day is a session and offers no next-session helper,
    so this steps forward a day at a time. ``None`` means the search ran past its guard,
    which a real calendar never does.
    """
    for step in range(1, _NEXT_SESSION_SEARCH_DAYS + 1):
        candidate = session + timedelta(days=step)
        if market.is_session(candidate):
            return candidate
    return None


def _walk(
    *,
    ticker: str,
    following: date | None,
    comparable: tuple[_Contract, ...],
    root: Path,
    include_quarantined: bool,
    guards: GuardConstants,
    master: SecurityMaster,
    spans: CaptureSpans,
    market: Calendar,
    occ_index: dict[str, int],
) -> _Outcome:
    """Find the calendar-next session's first cycle that both differs and holds."""
    if following is None:
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_PARTITION_ABSENT)
    following_text = following.isoformat()

    try:
        cycles = list_chain_cycles(
            ticker, following_text, lake_root=root, include_quarantined=include_quarantined
        )
    except PartitionAbsent:
        return _unsealed(ticker, following, root, master, spans, market)
    except PartitionQuarantined:
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_PARTITION_QUARANTINED)
    except NoCloseOfRecord:  # pragma: no cover - a listing resolves no close tag
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_NO_DATA_CYCLES)

    if not cycles:
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_NO_DATA_CYCLES)

    # Keyed on the thread rather than on the spelling, so a contract the OCC re-symbols
    # between S and the session walked here is read as renamed rather than as gone.
    baseline = _threaded(
        occ_index,
        ((row.occ_symbol, row.open_interest) for row in comparable),
        ticker,
        following_text,
    )
    plateau = guards.oi_plateau_cycles
    # A cycle in the session's final ``plateau`` has no subsequent cycles to hold across,
    # so it is not a candidate. The window is the session's cycles less its last few.
    window = len(cycles) - plateau
    reached_floor = False
    # Counted apart from ``reached_floor`` because no cycle examined and every cycle too
    # thin are different answers. A session holding one stored cycle against the default
    # plateau of one leaves the window empty, and reporting that as a set under the floor
    # described the comparable set, which had nothing to do with it.
    examined = 0

    for index in range(max(window, 0)):
        cycle = _cycle_oi(
            ticker, following_text, cycles[index], root, include_quarantined, occ_index
        )
        if cycle is None:
            continue
        # A null OI is the vendor declining to say, not a changed figure. Counting one as
        # changed let a cycle that carried null on every contract pass the quorum against a
        # real baseline and then pass the plateau against itself, and be returned as a
        # settlement whose every value was null.
        voters = {
            occ: oi for occ, oi in cycle.open_interest.items() if occ in baseline and oi is not None
        }
        examined += 1
        if not voters or len(voters) < guards.oi_comparable_set_floor:
            continue
        reached_floor = True
        changed = sum(1 for occ, oi in voters.items() if oi != baseline[occ])
        if changed / len(voters) < guards.oi_refresh_quorum:
            continue
        if not _holds(
            ticker=ticker,
            following_text=following_text,
            cycles=cycles,
            index=index,
            plateau=plateau,
            voters=voters,
            root=root,
            include_quarantined=include_quarantined,
            occ_index=occ_index,
        ):
            continue
        return _Outcome(
            source_session=following_text,
            source_snap_ts=cycle.snap_ts,
            open_interest=cycle.open_interest,
        )

    if examined and not reached_floor:
        return _Outcome(verdict=VERDICT_INDETERMINATE, reason=REASON_SET_UNDER_FLOOR)
    return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_NO_CYCLE_PASSED)


@dataclass(frozen=True)
class _Cycle:
    """One stored cycle's OI map, and the ``snap_ts`` spelling it was stamped with."""

    snap_ts: str | None
    open_interest: dict[int | str, int | None]


def _cycle_oi(
    ticker: str,
    day_text: str,
    minute: str,
    root: Path,
    include_quarantined: bool,
    occ_index: dict[str, int],
) -> _Cycle | None:
    """One cycle's contract-to-OI map, or ``None`` when that minute no longer resolves.

    A minute came from the listing, so it resolved a moment ago. ``SnapAbsent`` here
    means the partition changed between the two reads, and skipping the cycle is right:
    it is a candidate that turned out not to exist, not a session that cannot be
    answered.
    """
    try:
        table = load_chain(
            ticker, day_text, minute, lake_root=root, include_quarantined=include_quarantined
        )
    except SnapAbsent:
        return None
    occs = table.column(_OCC).to_pylist()
    ois = table.column(_OI).to_pylist()
    # One instant has more than one ISO spelling, and a cycle can carry several of them:
    # SPY's 2026-09-11 partition holds 408 ``snap_ts`` texts naming 406 instants. They all
    # name the same minute here, because the loader resolved them together, so which one
    # rides the answer is a question of being reproducible rather than of being right. The
    # smallest is taken so a partition written in a different row order reports the same
    # provenance.
    stamps = table.column(_SNAP_TS).to_pylist()
    return _Cycle(
        snap_ts=min(stamps) if stamps else None,
        open_interest=_threaded(occ_index, zip(occs, ois, strict=True), ticker, day_text),
    )


def _holds(
    *,
    ticker: str,
    following_text: str,
    cycles: tuple[str, ...],
    index: int,
    plateau: int,
    voters: dict[int | str, int | None],
    root: Path,
    include_quarantined: bool,
    occ_index: dict[str, int],
) -> bool:
    """Whether the candidate's OI is unchanged over the voters for ``plateau`` cycles.

    A voter the later cycle does not carry counts as changed. A contract that leaves the
    chain a minute after a refresh was declared is exactly the shape of a half-written
    load, so the plateau fails closed on it rather than reading absence as agreement.
    """
    for step in range(1, plateau + 1):
        later = _cycle_oi(
            ticker, following_text, cycles[index + step], root, include_quarantined, occ_index
        )
        if later is None:
            return False
        for occ, value in voters.items():
            if occ not in later.open_interest or later.open_interest[occ] != value:
                return False
    return True


def _unsealed(
    ticker: str,
    following: date,
    root: Path,
    master: SecurityMaster,
    spans: CaptureSpans,
    market: Calendar,
) -> _Outcome:
    """Pending or absent, for a calendar-next session with no partition on disk.

    Pending needs both halves. Something newer must not have sealed, because nothing goes
    back to seal a session the daemon has already passed, and a span capturing option
    chains must cover the session, because nothing will ever capture one that no span
    does.
    """
    if _sealed_after(root, ticker, following):
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_PARTITION_ABSENT)
    if not _covered_with_options(spans, master, ticker, following, market):
        return _Outcome(verdict=VERDICT_ABSENT, reason=REASON_PARTITION_ABSENT)
    return _Outcome(verdict=VERDICT_PENDING, reason=REASON_NOT_YET_CAPTURED)


def _sealed_after(root: Path, ticker: str, session: date) -> bool:
    """Whether the ticker has a sealed chains partition for a session after this one.

    This reads directory entries rather than partition contents, so it is not a second
    read path into sealed data and the quarantine exclusion has nothing to say about it.
    A withheld partition still proves the daemon passed that date, which is the only
    thing being asked.
    """
    directory = LakePaths(root).partition_path(CHAINS, ticker, session).parent
    if not directory.is_dir():
        return False
    for entry in directory.iterdir():
        matched = _PARTITION_NAME.fullmatch(entry.name)
        if matched and date.fromisoformat(matched.group(1)) > session:
            return True
    return False


def _marked(
    ticker: str,
    session_text: str,
    roster: tuple[_Contract, ...],
    verdict: str,
    reason: str | None,
) -> pa.Table:
    """The whole roster under one verdict, which is what a session-wide marker is."""
    count = len(roster)
    return pa.table(
        {
            "ticker": [ticker] * count,
            "session": [session_text] * count,
            "occ_symbol": [row.occ_symbol for row in roster],
            "open_interest": [None] * count,
            "verdict": [verdict] * count,
            "reason": [reason] * count,
            "source_session": [None] * count,
            "source_snap_ts": [None] * count,
        },
        schema=OI_VIEW_SCHEMA,
    )


def _settled(
    ticker: str,
    session_text: str,
    roster: tuple[_Contract, ...],
    outcome: _Outcome,
    occ_index: dict[str, int],
) -> pa.Table:
    """The roster against the selected cycle, contract by contract.

    A contract with no settled figure is marked rather than dropped, because completeness is
    counted from rows and never inferred from holes, and the two reasons it can carry are
    kept apart on purpose.

    *It expired on S.* Decided from the contract's own expiration and never from whether the
    selected cycle lists it. Expiry-day final OI is unobservable under any design, so a
    vendor that keeps an expired contract listed for one more session must not be able to
    turn that into a settled number. On the live lake the two tests agree, because SPY's
    2026-09-14 close held 12,956 contracts, 310 of them expiring that day, and those 310 are
    exactly what 2026-09-15 no longer carried. They agree there and they are not the same
    test, and the expiration is the one that is true by construction.

    *The selected cycle carries no number for it.* A contract that survives past S and is
    missing from the cycle, or listed in it with a null OI. A partial or truncated cycle
    produces that, and calling it an expiry would be a false statement about the contract. A
    contract the OCC re-symboled is *not* one of these. It is listed under a spelling S never
    saw, and the lookup keys through the master so it is read as renamed rather than as
    gone.
    """
    found = outcome.open_interest or {}
    verdicts: list[str] = []
    reasons: list[str | None] = []
    values: list[int | None] = []
    for row in roster:
        value = found.get(occ_index.get(row.occ_symbol, row.occ_symbol))
        if not row.expires_after_session:
            verdicts.append(VERDICT_ABSENT)
            reasons.append(REASON_EXPIRED_OUT)
            values.append(None)
        elif value is None:
            verdicts.append(VERDICT_ABSENT)
            reasons.append(REASON_NO_VALUE_IN_CYCLE)
            values.append(None)
        else:
            verdicts.append(VERDICT_SETTLED)
            reasons.append(None)
            values.append(value)
    count = len(roster)
    return pa.table(
        {
            "ticker": [ticker] * count,
            "session": [session_text] * count,
            "occ_symbol": [row.occ_symbol for row in roster],
            "open_interest": values,
            "verdict": verdicts,
            "reason": reasons,
            "source_session": [outcome.source_session] * count,
            "source_snap_ts": [outcome.source_snap_ts] * count,
        },
        schema=OI_VIEW_SCHEMA,
    )


__all__ = [
    "OI_VIEW_SCHEMA",
    "REASON_EXPIRED_OUT",
    "REASON_NOT_YET_CAPTURED",
    "REASON_NO_CLOSE_OF_RECORD",
    "REASON_NO_CYCLE_PASSED",
    "REASON_NO_DATA_CYCLES",
    "REASON_NO_VALUE_IN_CYCLE",
    "REASON_PARTITION_ABSENT",
    "REASON_PARTITION_QUARANTINED",
    "REASON_SET_UNDER_FLOOR",
    "VERDICT_ABSENT",
    "VERDICT_INDETERMINATE",
    "VERDICT_PENDING",
    "VERDICT_SETTLED",
    "BaselineAbsent",
    "OiViewError",
    "SpellingsCollide",
    "ScopeUnreadable",
    "SessionOutOfScope",
    "oi_view",
]
