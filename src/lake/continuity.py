"""The option-continuity view: one contract's life, in one scale, beside its underlying.

An OCC adjustment gives one contract a new symbol and new terms on the same day, and the
lake records both without touching either. Raw is vendor-verbatim, so the strike a session
stored is the strike that session traded, and nothing rewrites it later. That is what keeps
the lake honest and it is also what leaves a reader with two half-lives: the sessions before
the boundary under one spelling at one strike, and the sessions after it under another, with
nothing saying they are the same contract.

marketlake #136 is authoritative for this deliverable. It is the last of the two views that
issue names. The other one, expiration settlement, was cut to #382 and shipped as #384.

This is a function over loader reads rather than a DuckDB view, which is the rule #136 and
#135 both state for this slice: a view of sealed partitions that opened them itself would
skip the quarantine exclusion the loader owns, in a design whose rule for sealed data is fail
closed. ``lake.oi.oi_view`` and ``lake.settle.settlement_view`` are the same shape, and
neither carries a ``__main__``, so neither does this.

**The answer is one row per session the contract was observed on**, ordered by session.
``load_bars`` gives the reason a series is ordered by its own axis rather than by the
partition layout: it is the one read whose order a caller will assume. The slice's other two
views order by the roster instead, because each answers one session.

What the design asks of this view is two things and only two. "A whole-ratio split maps
exactly. Strikes scale by the ratio, and the contract count absorbs the rest. The view
normalizes it. An uneven split or special dividend changes the deliverable itself. A
non-standard contract delivering 150 shares plus cash has no multiplier that makes it
comparable. There the view surfaces the event instead of faking one."

**The contract's own price is not one of them.** The word *premium* appears nowhere in
``docs/design.md`` and nowhere under ``src/lake``, so this repo writes down no rule for
carrying an option's price across an adjustment, and a view that invented one would be
minting a convention rather than deriving a view. ``mark`` therefore rides as-traded.
Considered and rejected, and the schema is what says so: a column with an ``adjusted_``
partner has a rule behind it and a column without one does not. ``load_bars`` marks its
answer with an ``adjust`` column because one of its tables is entirely one view or entirely
the other, and this table is not, carrying both forms of the strike on one row.

What the join then makes comparable is moneyness. The underlying's split-adjusted close
against the contract's adjusted strike is continuous across a whole-ratio adjustment, and
that pairing is what a chains-to-bars join means once an adjustment sits in the middle of the
series.

Four reads make the answer and each is through the door that owns its question.

1. *The contract's session.* ``load_chain(ticker, session)``, filtered to the contract's
   spelling for that session. Not ``load_contract``, which answers a contract's whole session
   rather than its close of record: on the live lake that is 406 minute rows for one SPY
   contract on 2026-09-15, carrying three distinct ``close_tag`` values. Picking the close of
   record out of them means knowing that chains resolve against ``option_close`` while quotes
   resolve against ``spot_close``, and that two cycles carrying one tag refuse the read.
   ``loader._at_close`` owns that rule. It is also the cheaper read, best of five warm on the
   live lake: 0.139s for the close-of-record chain against 0.349s for one contract's session.
2. *The session's spelling.* ``occ_mapping.instruments_holding`` for the instrument and
   ``SecurityMaster.symbol_at`` for its spelling on a session. Not ``SecurityMaster.resolve``,
   which honours validity ranges and so answers ``None`` for each spelling on the far side of
   its own boundary, which is exactly the half this view exists to reach.
3. *The underlying, as traded.* ``load_bars(ticker, '1d', ..., adjust='none')``.
4. *The underlying, in the reference era.* The same read at ``adjust='split'``.

**The scale comes out of those two bar reads and never out of the ledger.** ``load_bars``
refuses the alternative in advance, about a caller wanting a point-in-time view: the door
takes ``as_of`` "rather than making a caller who wants the second one read the ledger and
apply factors itself. That caller would be a second adjustment path, in the one place where
it produces different numbers rather than an error." A view looping over ``actions.latest``
and multiplying ex-dates together is that caller. The reading that avoids it is arithmetic:
the ``split`` view's price factor is ``1 / ratio`` and nothing else, so a session's as-traded
close over its split-adjusted close *is* the cumulative ratio of every ledger split whose
ex-date falls after it. Both numbers come from one door under one ``as_of``, and this module
imports no ledger reader at all.

The session's strike divided by that ratio is the strike in the reference era, and a
position's contract count multiplied by it is that position in the reference era.
``loader._in_view`` divides a price by the same ratio and multiplies a volume by it, which is
the same pair of directions. The reference era is the ledger's current last one, which is
what the ``split`` view normalizes to, so both sides of the join land in one scale without
this view choosing an anchor of its own.

**A mapping row is not evidence of an adjustment, and that is the whole difficulty.**
``splits._examine`` writes the mapping before the ledger decides anything and says so in the
code: "A rename, an adjustment one float cannot describe, a ratio the gate refuses and a
landed split all re-symboled the contracts." So a boundary the master holds is one of four
things and only the last carries a number.

1. A rename. The deliverable did not move and nothing about the contract's terms changed with
   its spelling, so the series is continuous across it.
2. An adjustment no single float describes, held under ``CHECK_SPLIT_DELIVERABLE``.
3. A ratio the vendor-against-itself gate refused, held under ``CHECK_SPLIT_CONSISTENCY``.
4. A landed split, the only one of the four with a ledger entry.

Cases 1 and 2 are identical from the ledger's side, because neither has an entry, and they
want opposite treatments. What separates them is in the raw rows this view already reads.
``splits.Deliverable.same_as`` is the detector's own test for it, and the rows are read
through ``splits.deliverable_of_row``, which ``lake.settle`` already reuses for the reason
that function gives: "Two parsers for one vendor column would be two answers to what a sealed
row means."

``splits.check_split_consistency`` would hand this view a ratio computed from the contract's
own two deliverables, and taking it would apply a factor the gate refused, on exactly the
boundaries the gate refused it for. The design's rule for actions is that a factor lands only
after validation agrees, so that is considered and rejected too.

**Which sessions the answer holds a row for.** The sessions are the sealed chains partitions
in range, read off the filesystem for the reason ``loader._bars_sessions`` gives for the same
walk. Two absences inside that list are different and get different answers.

1. *The view could not look.* A session whose chain carries no close-of-record cycle. It does
   not know whether the contract traded, and the caller cannot tell that from a session where
   it did not, so the session gets a row saying so. Measured read-only at ``21e9d21``, this is
   not an edge: 4 of SPY's 8 sealed chains partitions raise ``NoOptionClose``.
2. *The view looked and the contract was not there.* It had not listed, or it had expired.
   That answer is known, so the session is stepped over. Over the four readable SPY sessions
   no contract appears in all four: 12,336 appear in three, 764 in two and 562 in one, so
   stepping over is required rather than optional.

The first kind is bounded to the span between the contract's first and last observed session,
because an unreadable session before a contract listed is not that contract's hole. A range
the contract is observed in on no session raises rather than returning an empty table, for the
reason ``load_contract`` already gives: an empty answer and an absent one read the same and
mean opposite things.

A date with no sealed chains partition is not a session this view knows about, and the
answer's own ``session`` column shows the gap. Enumerating dates from an exchange calendar
instead is considered and rejected: it would make the row set a property of a calendar rather
than of the lake, where ``load_bars`` already takes the lake's own reading and returns around
an absent day.

A quarantined session refuses the whole read rather than taking a marked row, which is the
rule ``load_bars`` gives a range door: "a hole in a re-fetchable surface is ordinary and a
verdict is not." Demoting a verdict to one row would hand back a series that reads as
complete. ``include_quarantined=True`` is the other answer and marketlake #374 owns the third
one that neither of them is.

**The marks cover the answer's own span.** A range ending before a boundary answers a series
continuous within itself, which is true, and it cannot claim its numbers sit in the reference
era, because an adjustment the ledger holds out can sit between the last row and now. That
limit is ``load_bars``'s already: its ``split`` view is in the era the ledger currently
describes, and a held boundary is one the ledger does not describe. Reading past the range to
classify such a boundary is considered and rejected, because telling a rename from an
adjustment needs the contract's rows on both sides of it and those sit in sessions the caller
excluded.

**The walk is bounded by the caller's range and by nothing else**, at one close-of-record read
per session, 0.052 seconds each measured over SPY's eight sealed sessions. Bounding it by the
expiration inside the OCC symbol is considered and rejected: the vendor narrowed an
eight-digit expiry to six, so a symbol is 23 characters on 2026-09-02 and 21 on every later
partition, and the expiry sits at no fixed offset across the spellings the lake already holds.
``load_contract``'s docstring calls the sibling derivation for the root "a guess rather than a
guarantee", and ``expiration_date`` is the vendor's own answer, in a row the walk has to read
before it could bound anything.

Nothing here reads a clock or the network, and the one config read is
``loader.resolve_lake_root``'s.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from pathlib import Path

import pyarrow as pa

from lake.bars import session_of
from lake.loader import (
    ADJUST_NONE,
    ADJUST_SPLIT,
    NoOptionClose,
    load_bars,
    load_chain,
    resolve_lake_root,
)
from lake.occ_mapping import instruments_holding
from lake.paths import CHAINS, PARQUET_SUFFIX, LakePaths, parse_date_dir
from lake.security_master import ID_TYPE_OCC, SecurityMaster, master_path
from lake.splits import (
    SPLIT_CONSISTENCY_TOLERANCE,
    Deliverable,
    DeliverableUnreadable,
    deliverable_of_row,
)
from lake.vendor import DAILY_FREQ

# The chains columns this view reads. The first four are what the answer carries and the last
# four are what ``splits._reading`` takes as one tuple, which is what decides whether a
# boundary moved the deliverable. ``multiplier`` is on both lists and read once.
OCC_SYMBOL = "occ_symbol"
STRIKE_PRICE = "strike_price"
MARK = "mark"
MULTIPLIER = "multiplier"
NON_STANDARD = "non_standard"
OPTION_DELIVERABLES_LIST = "option_deliverables_list"
DELIVERABLE_NOTE = "deliverable_note"

_CHAINS_COLUMNS = (
    OCC_SYMBOL,
    STRIKE_PRICE,
    MARK,
    MULTIPLIER,
    NON_STANDARD,
    OPTION_DELIVERABLES_LIST,
    DELIVERABLE_NOTE,
)

# The bars columns the underlying's close is read from.
CLOSE_COLUMN = "close"
BAR_TS_COLUMN = "bar_ts"

# What a row's ``verdict`` says. These are D19's tokens, reused rather than respelled, which is
# what ``lake.settle`` does with the two of them it needs. This view needs three of the four.
# ``settled`` carries the adjusted numbers. ``absent`` says there are none and names why.
# ``indeterminate`` says the view could not decide which, the sense ``lake.oi`` gives it for a
# comparable set too small "to distinguish a stale feed from quiet contracts".
VERDICT_SETTLED = "settled"
VERDICT_ABSENT = "absent"
VERDICT_INDETERMINATE = "indeterminate"

# Why a row carries no adjusted numbers. Each is a fact about one session rather than about the
# read, which is the line ``lake.settle`` draws between a marked row and a refusal. Two of the
# five take D19's own spelling rather than a synonym for it, and none takes an exception's name.
#
# ``deliverable_not_scalar`` and ``boundary_unreadable`` are the two halves of the caveat and
# they are deliberately not one token. The first is the view deciding: no single float describes
# the adjustment, so no adjusted strike exists. The second is the view failing to decide, because
# the deliverable parse refused a side of the boundary and a rename cannot be told from an
# adjustment. Saying which of the two it was would be the guess the caveat exists to refuse.
REASON_DELIVERABLE_NOT_SCALAR = "deliverable_not_scalar"
REASON_BOUNDARY_UNREADABLE = "boundary_unreadable"
REASON_NO_CLOSE_OF_RECORD = "no_close_of_record"
REASON_CLOSE_UNREADABLE = "close_unreadable"
REASON_TERMS_UNREADABLE = "terms_unreadable"

# The answer's shape. ``split_ratio``, ``adjusted_strike`` and ``adjusted_underlying_close`` are
# null on every row that is not ``settled``, and ``reason`` is null on every row that is, which
# is D19's own null rule. ``underlying_close`` is carried on every row it can be read on,
# settled or not, because it is a property of the session rather than of the contract, exactly
# as ``settle`` carries ``settlement_close``.
#
# ``split_ratio`` carries both directions the design names. The strike divided by it is the
# strike in the reference era, and a position's contract count multiplied by it is that position
# in the reference era.
CONTINUITY_VIEW_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("session", pa.string()),
        ("occ_symbol", pa.string()),
        ("instrument_id", pa.int64()),
        ("split_ratio", pa.float64()),
        ("strike_price", pa.float64()),
        ("adjusted_strike", pa.float64()),
        ("mark", pa.float64()),
        ("multiplier", pa.float64()),
        ("underlying_close", pa.float64()),
        ("adjusted_underlying_close", pa.float64()),
        ("verdict", pa.string()),
        ("reason", pa.string()),
    ]
)

__all__ = [
    "CONTINUITY_VIEW_SCHEMA",
    "REASON_BOUNDARY_UNREADABLE",
    "REASON_CLOSE_UNREADABLE",
    "REASON_DELIVERABLE_NOT_SCALAR",
    "REASON_NO_CLOSE_OF_RECORD",
    "REASON_TERMS_UNREADABLE",
    "VERDICT_ABSENT",
    "VERDICT_INDETERMINATE",
    "VERDICT_SETTLED",
    "ContinuityError",
    "ContractDuplicated",
    "ContractNeverObserved",
    "ThreadAmbiguous",
    "continuity_view",
]


class ContinuityError(Exception):
    """Base for every reason this view produces no table at all.

    It is not a ``LoadError``. Every way a *read* resolves to nothing already raises one of
    those out of the loader and this view passes them through unchanged. What these three say
    is that the reads succeeded and the contract still has no series, which is a different
    statement and deserves a different name. ``lake.settle.SettlementError`` is the same
    distinction drawn in the same place.
    """


class ContractNeverObserved(ContinuityError):
    """Raised when the contract appears in no readable session of the range.

    An empty table would be a real answer here only if it could be told from an absent one,
    and it cannot: a contract that never listed, a range that misses its life, and a ticker
    that holds the contract under another partition all produce zero rows. ``load_contract``
    already refuses on that ground rather than returning empty, and this follows it.
    """


class ThreadAmbiguous(ContinuityError):
    """Raised when one OCC symbol names more than one instrument in the master.

    ``SecurityMaster.resolve`` calls that state corrupt and ``lake.occ_mapping`` refuses to
    create it, guarding both directions on the write side. Meeting it anyway refuses the read
    rather than picking an instrument, because the two threads are two different contracts and
    nothing here can say which one the caller meant.
    """


class ContractDuplicated(ContinuityError):
    """Raised when a session's close-of-record cycle holds two rows for one contract.

    One cycle is one observation per contract. Two leave the session's strike and deliverable
    undecided, and a view that took either would answer differently depending on the
    partition's layout. ``settle`` refuses on the same ground for an unreadable expiration: an
    answer nothing downstream can check is worse than a refusal.
    """


def continuity_view(
    ticker: str,
    occ_symbol: str,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    as_of: date | str | None = None,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """One contract's life across its adjustments, as a table ordered by session.

    ``ticker`` names the partitions to walk and leads the signature, as it does on
    ``load_chain``, ``oi_view`` and ``settlement_view``. It is an argument rather than a
    derivation because the master says nothing about an option's underlying:
    ``occ_mapping``'s own docstring pins that an option instrument carries an ``occ_symbol``
    mapping and nothing else, and ``load_contract``'s calls deriving a ticker from an OCC root
    "a guess rather than a guarantee", naming "a symbol a corporate action rewrote" as the case
    it comes apart on, which is this view's only case.

    ``occ_symbol`` is any spelling the contract has ever carried. The master threads the rest,
    so the pre-adjustment symbol and the adjusted one answer the same series.

    ``start`` and ``end`` are session dates and both default to open, so a call naming neither
    walks every sealed chains session the lake holds for that ticker.

    ``as_of`` is a market date that resolves the actions ledger point-in-time, carrying
    ``load_bars``'s meaning and its default. It reaches both bar reads, because a join whose
    halves sat at two points in time would produce different numbers rather than an error.

    ``lake_root`` and ``include_quarantined`` carry the meanings the loader's doors give them.
    The flag reaches every read, so a verdict on either surface refuses this view by default.

    Every way a read resolves to nothing raises its own ``LoadError`` unchanged:
    ``PartitionAbsent`` and ``PartitionQuarantined`` for either surface, ``BarsAbsent`` for a
    span holding no daily partition at all, ``PartialRead`` for a projection that could not
    complete, ``InstrumentUnknown`` and ``AdjustmentIncomplete`` for a split view the bars
    cannot support. A torn security master raises ``MasterUnreadable``, the error of the module
    that owns the file. This view adds the three under ``ContinuityError``.
    """
    root = resolve_lake_root(lake_root)
    first, last = _session(start), _session(end)
    thread = _thread(root, occ_symbol)

    observed: dict[date, dict[str, object]] = {}
    unreadable: set[date] = set()
    for day in _chains_sessions(root, ticker, first, last):
        try:
            chain = load_chain(ticker, day, lake_root=root, include_quarantined=include_quarantined)
        except NoOptionClose:
            unreadable.add(day)
            continue
        rows = _rows_for(chain, thread.spelling_on(day))
        if len(rows) > 1:
            raise ContractDuplicated(
                f"{ticker} {day.isoformat()} holds {len(rows)} rows for "
                f"{thread.spelling_on(day)!r} in one close-of-record cycle. One cycle is one "
                "observation per contract, so the session's terms cannot be vouched for."
            )
        if rows:
            observed[day] = rows[0]

    if not observed:
        raise ContractNeverObserved(
            f"{occ_symbol!r} appears in no readable close-of-record chain for {ticker} in "
            f"{_range_text(start, end)}. {len(unreadable)} session(s) in that range carry no "
            "close-of-record cycle. Widen the range, or pass the ticker whose partitions hold "
            "the contract."
        )

    span_start, span_end = min(observed), max(observed)
    holes = {day for day in unreadable if span_start < day < span_end}
    scales = _scales(root, ticker, span_start, span_end, as_of, include_quarantined)
    marks = _boundary_marks(thread, observed, scales)
    return _answer(ticker, thread, observed, holes, scales, marks)


def _session(day: date | str | None) -> date | None:
    """A session argument as a ``date``, leaving ``None`` open."""
    if day is None:
        return None
    return day if isinstance(day, date) else date.fromisoformat(str(day))


def _range_text(start: date | str | None, end: date | str | None) -> str:
    """How a refusal names the range it found nothing in, spelled as ``load_bars`` spells it."""
    return f"{'open' if start is None else start}..{'open' if end is None else end}"


@dataclass(frozen=True)
class _Thread:
    """One contract's identity across its spellings, as the master holds it.

    ``instrument`` is ``None`` for every contract no re-symboling has touched, which is almost
    all of them: ``occ_mapping`` writes an instrument only for a contract a boundary moved, and
    its docstring calls an ordinary contract resolving to nothing "the right answer". ``given``
    is then the spelling on every session.

    ``earliest`` is the first spelling the master's ranges open on, and it is what a session
    *before* the thread opens takes. A mapping's ``valid_from`` is the first session the split
    walk read the contract, which ``occ_mapping`` chose deliberately as "both true and the
    tightest honest claim", and that walk skips sessions. So the lake holds sealed sessions no
    mapping range covers, and falling back to the caller's own spelling there would read an
    early session under an adjusted symbol whenever the caller happened to hold one.
    """

    given: str
    instrument: int | None
    earliest: str
    ranges: tuple[tuple[date, str], ...]

    def spelling_on(self, day: date) -> str:
        """The spelling this contract carried on ``day``."""
        if self.instrument is None:
            return self.given
        for valid_from, symbol in reversed(self.ranges):
            if day >= valid_from:
                return symbol
        return self.earliest


def _thread(root: Path, occ_symbol: str) -> _Thread:
    """The contract's spellings over time, read out of the security master.

    The entry is ``occ_mapping.instruments_holding`` rather than ``SecurityMaster.resolve``.
    ``resolve`` honours validity ranges, so each spelling answers ``None`` on the far side of
    its own boundary, and that far side is exactly the half this view exists to reach.
    Executed against a fixture replay, ``resolve`` of the pre-adjustment symbol on a
    post-boundary session and of the adjusted symbol on a pre-boundary one both answer ``None``.

    A master the lake does not hold threads nothing and raises nothing, which is the rule
    ``load_bars`` gives an absent actions ledger. That is what keeps this view reading the same
    on a lake whose producers have not run: measured at ``21e9d21`` the live master holds two
    mappings, both ``ticker`` and both ``equity``.
    """
    path = master_path(root)
    if not path.is_file():
        return _Thread(given=occ_symbol, instrument=None, earliest=occ_symbol, ranges=())

    master = SecurityMaster.read(path)
    holding = instruments_holding(master, occ_symbol)
    if not holding:
        return _Thread(given=occ_symbol, instrument=None, earliest=occ_symbol, ranges=())
    if len(holding) > 1:
        raise ThreadAmbiguous(
            f"{occ_symbol!r} names instruments {sorted(holding)} in the security master, which "
            "is the state the master calls corrupt and lake.occ_mapping refuses to write. No "
            "one contract's life can be read through it."
        )

    instrument = holding.pop()
    ranges = tuple(
        sorted(
            (mapping.valid_from, mapping.id_value)
            for mapping in master.mappings
            if mapping.instrument_id == instrument and mapping.id_type == ID_TYPE_OCC
        )
    )
    return _Thread(given=occ_symbol, instrument=instrument, earliest=ranges[0][1], ranges=ranges)


def _chains_sessions(root: Path, ticker: str, start: date | None, end: date | None) -> list[date]:
    """The sessions this ticker holds a sealed chains partition for, in date order.

    The listing comes from the filesystem rather than from the manifest, for the reason
    ``loader._bars_sessions`` gives for the same walk: the filesystem is the question the read
    itself goes on to ask, so enumerating from the manifest would answer two questions in one
    read. A name that does not read as ``date=YYYY-MM-DD.parquet`` is passed over, because a
    partition being written lands under a temp marker and is renamed into place, so a listing
    taken mid-write sees a name this cannot read and that file is not a session yet.

    The date is read through ``paths.parse_date_dir``, which owns that spelling. A bare
    ``date.fromisoformat`` accepts ``20260824`` and ``2026-W35-1`` as well, so a stray file
    named either would be enumerated as a session.

    The directory is not checked for its exact spelling here. A mis-cased ticker lists the right
    directory on a case-insensitive filesystem and then refuses at ``load_chain`` with
    ``PartitionAbsent``, which names the ticker and the day. That is the clearer answer, and it
    comes from the door that owns it.
    """
    directory = LakePaths(root).partition_path(CHAINS, ticker, date(1970, 1, 1)).parent
    if not directory.is_dir():
        return []
    found: list[date] = []
    for entry in directory.iterdir():
        if not entry.name.endswith(PARQUET_SUFFIX):
            continue
        day = parse_date_dir(entry.name[: -len(PARQUET_SUFFIX)])
        if day is None:
            continue
        if (start is None or day >= start) and (end is None or day <= end):
            found.append(day)
    return sorted(found)


def _rows_for(chain: pa.Table, spelling: str) -> list[dict[str, object]]:
    """The close-of-record chain's rows for one contract, as plain dicts.

    Only the columns this view reads are selected, and a column the partition does not carry is
    left out rather than raising: ``_deliverable`` and ``_terms`` each decide for themselves
    what a missing value means, and both answer with a marked row rather than an exception.
    """
    present = [name for name in _CHAINS_COLUMNS if name in chain.column_names]
    if OCC_SYMBOL not in present:
        return []
    return [row for row in chain.select(present).to_pylist() if row.get(OCC_SYMBOL) == spelling]


def _scales(
    root: Path,
    ticker: str,
    span_start: date,
    span_end: date,
    as_of: date | str | None,
    include_quarantined: bool,
) -> dict[date, tuple[float, float]]:
    """Each session's as-traded close and its cumulative split ratio, from the two bar views.

    The ``split`` view divides a bar by the cumulative ratio of every ledger split whose ex-date
    falls strictly after that session, and folds no dividend in, so the as-traded close over the
    split-adjusted close is that ratio exactly. Reading it this way rather than looping over the
    ledger is what keeps one adjustment path, which ``load_bars``'s own docstring asks for by
    name.

    Both reads are one range under one ``as_of``. It is inert on the as-traded half, which
    returns before touching the ledger, and it is passed there anyway so the two calls are
    visibly one read in two views rather than two reads that might disagree.

    The two travel together because they are read together, and a session missing either is
    missing both: with no usable close there is no underlying to pair a strike with and no ratio
    to express it in. That is one condition rather than two, and the caller marks that row rather
    than refusing, because it is one session's problem.

    ``load_bars`` orders its answer by the instant each ``bar_ts`` names, so iterating in order
    and overwriting leaves each session's *last* candle, which is that session's close. The gate
    checks that the session came back rather than that exactly one candle did, so a partition
    holding two is not a defect. ``bars._bar_close`` answers the same question by sorting the
    stamp text and marketlake #386 owns that; this compares instants through the ordering
    ``load_bars`` already applied.
    """
    reads = [
        load_bars(
            ticker,
            DAILY_FREQ,
            span_start,
            span_end,
            adjust=adjust,
            as_of=as_of,
            lake_root=root,
            include_quarantined=include_quarantined,
        )
        for adjust in (ADJUST_NONE, ADJUST_SPLIT)
    ]
    plain, scaled = _closes(reads[0]), _closes(reads[1])
    found: dict[date, tuple[float, float]] = {}
    for day, close in plain.items():
        split = scaled.get(day)
        if split is None:
            continue
        ratio = close / split
        if isfinite(ratio) and ratio > 0:
            found[day] = (close, ratio)
    return found


def _closes(bars: pa.Table) -> dict[date, float]:
    """Each session's closing candle, keyed by the session its own stamp names.

    A bar's session is ``bars.session_of`` rather than the ``date=`` level of the partition it
    came out of, which is the rule ``loader._in_view`` states: reading the path would mint a
    second definition of a bar's session to get the answer the first one already gives. That
    call is on ``bar_ts``, which ``journal._bars_rows`` mints through ``_epoch_ms_to_iso`` at
    ``+00:00``, so it inherits the writer's guarantee exactly as ``load_bars``'s own call does.
    Marketlake #385 is the naive-stamp weakness and its live exposure is ``expiration_date``,
    a column this view does not read.

    A close that is not a positive finite number leaves the session out. The schema permits what
    the evening sweep's gate refuses, and a caller can point ``lake_root`` at any lake, so the
    shape is possible even though production cannot land it.
    """
    if BAR_TS_COLUMN not in bars.column_names or CLOSE_COLUMN not in bars.column_names:
        return {}
    found: dict[date, float] = {}
    for stamp, close in zip(
        bars.column(BAR_TS_COLUMN).to_pylist(), bars.column(CLOSE_COLUMN).to_pylist(), strict=True
    ):
        number = _number(close)
        if not isinstance(stamp, str) or number is None or number <= 0:
            continue
        found[session_of(stamp)] = number
    return found


@dataclass(frozen=True)
class _Mark:
    """What one boundary does to every session before it."""

    verdict: str
    reason: str


def _boundary_marks(
    thread: _Thread,
    observed: dict[date, dict[str, object]],
    scales: dict[date, tuple[float, float]],
) -> dict[date, _Mark]:
    """The boundaries this answer crosses that the view cannot normalize, keyed by their session.

    A boundary is a pair of consecutive observed sessions whose spellings differ, and it is
    named by the later of the two, which is the first session under the new spelling. Every row
    *before* that session is the one a mark applies to. The boundary session itself sits on the
    new side and is unaffected.

    Four outcomes write a mapping and only one lands a ledger entry, so a spelling change on its
    own says nothing about whether the terms moved. The deliverable does, through the detector's
    own ``Deliverable.same_as``, and the ledger says whether what moved has a factor behind it.

    The ledger is asked through the scale rather than directly. A split whose ex-date is the
    boundary session is counted for the session before it and not for the boundary session
    itself, so the two sessions' cumulative ratios differ exactly when the ledger describes the
    boundary. The tolerance is ``splits.SPLIT_CONSISTENCY_TOLERANCE``, this repo's own figure for
    how far two readings of one split ratio may sit apart: the two quotients carry a few units in
    the last place, which is around 1e-15 relative, and the smallest adjustment the detector can
    land moves the ratio by a quarter.

    A boundary whose scale cannot be read on either side is undecidable for the same reason a
    boundary whose deliverable cannot be parsed is, and takes the same answer.
    """
    days = sorted(observed)
    marks: dict[date, _Mark] = {}
    for previous, day in zip(days, days[1:], strict=False):
        if thread.spelling_on(previous) == thread.spelling_on(day):
            continue
        before = _deliverable(observed[previous], previous)
        after = _deliverable(observed[day], day)
        if before is None or after is None:
            marks[day] = _Mark(VERDICT_INDETERMINATE, REASON_BOUNDARY_UNREADABLE)
            continue
        if after.same_as(before):
            continue
        earlier, later = scales.get(previous), scales.get(day)
        if earlier is None or later is None:
            marks[day] = _Mark(VERDICT_INDETERMINATE, REASON_BOUNDARY_UNREADABLE)
            continue
        if abs(earlier[1] - later[1]) / later[1] <= SPLIT_CONSISTENCY_TOLERANCE:
            marks[day] = _Mark(VERDICT_ABSENT, REASON_DELIVERABLE_NOT_SCALAR)
    return marks


def _deliverable(row: dict[str, object], day: date) -> Deliverable | None:
    """What one session's row says the contract delivers, or ``None`` when the parse refuses.

    The parse is ``lake.splits``'s, through the row-level door ``lake.settle`` already reuses,
    because two parsers for one vendor column would be two answers to what a sealed row means.
    It refuses 2 of the 73,134 close-of-record data rows the lake holds, both in SPY's
    2026-09-02 partition, so this is reachable rather than defensive.
    """
    try:
        return deliverable_of_row(row, day)
    except DeliverableUnreadable:
        return None


def _standing(day: date, marks: dict[date, _Mark]) -> _Mark | None:
    """The mark a session inherits from the boundaries between it and the reference era.

    A definite answer beats an undecided one. ``deliverable_not_scalar`` says no adjusted strike
    exists, which stays true however the other boundaries read, while ``boundary_unreadable``
    only says this view could not tell. So one non-scalar boundary anywhere after the session
    settles it.
    """
    standing = [mark for boundary, mark in marks.items() if boundary > day]
    for mark in standing:
        if mark.reason == REASON_DELIVERABLE_NOT_SCALAR:
            return mark
    return standing[0] if standing else None


def _number(value: object) -> float | None:
    """``value`` as a finite float, or ``None`` when it is not one.

    A bool is refused before the numeric test, because ``True`` is an ``int`` in Python and
    would otherwise arrive as a price of 1.0.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if isfinite(number) else None


def _terms(row: dict[str, object]) -> tuple[float, float, float] | None:
    """A row's strike, mark and multiplier, or ``None`` when one of them cannot be read.

    All three have to be positive finite numbers. A strike of zero carries no moneyness, a
    multiplier of zero describes no contract, and a non-positive mark is not a price. Measured
    at ``21e9d21``, the lake holds 2 of 73,134 close-of-record rows where ``mark`` and
    ``multiplier`` fail this and none where ``strike_price`` does.
    """
    values = []
    for name in (STRIKE_PRICE, MARK, MULTIPLIER):
        number = _number(row.get(name))
        if number is None or number <= 0:
            return None
        values.append(number)
    return values[0], values[1], values[2]


def _answer(
    ticker: str,
    thread: _Thread,
    observed: dict[date, dict[str, object]],
    holes: set[date],
    scales: dict[date, tuple[float, float]],
    marks: dict[date, _Mark],
) -> pa.Table:
    """The table, one row per observed session plus one per hole inside the span.

    The order of the checks is what decides a row's one verdict. A session the view could not
    read has no terms to check. A row whose own terms cannot be read has no strike to scale
    whatever the session's close did. A session with no usable close has neither an underlying
    to pair the strike with nor a scale to express it in, which is one condition rather than
    two. Only then do the boundaries after the row decide it.
    """
    rows: list[dict[str, object]] = []
    for day in sorted(set(observed) | holes):
        close = scales.get(day)
        base: dict[str, object] = {
            "ticker": ticker,
            "session": day.isoformat(),
            "occ_symbol": thread.spelling_on(day),
            "instrument_id": thread.instrument,
            "split_ratio": None,
            "strike_price": None,
            "adjusted_strike": None,
            "mark": None,
            "multiplier": None,
            "underlying_close": None if close is None else close[0],
            "adjusted_underlying_close": None,
            "verdict": VERDICT_ABSENT,
            "reason": REASON_NO_CLOSE_OF_RECORD,
        }
        if day in holes:
            rows.append(base)
            continue

        terms = _terms(observed[day])
        if terms is None:
            base["reason"] = REASON_TERMS_UNREADABLE
            rows.append(base)
            continue
        strike, mark, multiplier = terms
        base["strike_price"] = strike
        base["mark"] = mark
        base["multiplier"] = multiplier

        if close is None:
            base["reason"] = REASON_CLOSE_UNREADABLE
            rows.append(base)
            continue
        as_traded, ratio = close

        standing = _standing(day, marks)
        if standing is not None:
            base["verdict"] = standing.verdict
            base["reason"] = standing.reason
            rows.append(base)
            continue

        base["split_ratio"] = ratio
        base["adjusted_strike"] = strike / ratio
        base["adjusted_underlying_close"] = as_traded / ratio
        base["verdict"] = VERDICT_SETTLED
        base["reason"] = None
        rows.append(base)

    return pa.table(
        {name: [row[name] for row in rows] for name in CONTINUITY_VIEW_SCHEMA.names},
        schema=CONTINUITY_VIEW_SCHEMA,
    )
