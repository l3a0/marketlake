"""The expiration settlement view: what each expiring contract settled at.

An option that reaches its expiration does not simply stop existing. The OCC exercises any
contract finishing a cent or more in the money unless the holder says otherwise, which is
exercise-by-exception, and what decides "in the money" is the session's official equity
close. So a backtest that carries a position into expiry needs one number per expiring
contract, and this is the read that produces it.

marketlake #382 is authoritative for this deliverable, cut from #136 on 2026-09-17. #136
keeps the other view it names, option continuity across an adjustment, which cannot be built
until something writes an OCC mapping into the security master.

This is a function over loader reads rather than a DuckDB view. The read layer returns a
``pyarrow.Table``, and D19 shipped the slice's other view the same way, as ``lake.oi.oi_view``.
The rule is about this slice rather than about DuckDB: a view of sealed partitions that opened
them itself would skip the quarantine exclusion the loader owns, and the design's rule for
sealed data is fail closed. The dashboard's own DuckDB reads are specified deliberately, with
their own sandbox, and are not what that rule is aimed at.

**The answer is one row per contract expiring on the session**, taken from that session's
close of record and ordered by ``occ_symbol``. A row is ``settled`` and carries a number, or
it names the reason it is ``absent``. That shape is D19's, and the two columns that carry it,
``verdict`` and ``reason``, are D19's own vocabulary rather than a synonym for it. The tokens
are spelled here rather than imported, because each view's schema pins its own answer and
importing ``lake.oi`` would pull the calendar, the security master and the capture spans in
behind two strings.

The order is the contract's own identity rather than the partition's layout. ``load_contract``
already gives the reason, that nothing here rides on how the writer happened to order the
file, and a recompaction may reorder a partition. ``occ_symbol`` is also the tiebreak
``oi._comparable_set`` uses so that its set is the same set on every run.

**The close comes from ``bars/`` and from nowhere else.** ``load_bars(ticker, '1d', session,
session)`` with ``adjust='none'``, because settlement is a same-date comparison and the
design's one scale rule puts those in as-traded space. The read names a session and never a
time of day, so a half day's 13:00 close needs no branch: it is what that session's daily
partition holds.

Three other sources look like they would do and none of them is it. ``spot_close`` is the
pre-auction book rather than the auction print, and the design says so outright. The
option-close snapshot is a different market's close. And the chain's own ``underlying_price``,
which is the closest of the three, is a vendor snapshot with no gate behind it. That last one
is worth keeping as a cross-check rather than dismissing: at the option close, which sits after
the 16:00 auction, it equals the close the lake settled on all four ticker-sessions the two can
be compared over, 760.88 and 757.39 on SPY and 709.18 and 704.54 on QQQ. A number
that agrees four times out of four is a good check and still not the source, because the
``bars/`` close has passed gate-before-land and this one has passed nothing.

**Nothing here waits on marketlake #362**, which asks which session a daily stamp names. This
read asks for the close of a *named* session, and a bars partition is addressed by its
``date=`` path level. ``bars.select_session_rows`` filters every landed row through
``session_of``, and two gates run before a candle lands: the span check refuses a daily fetch
whose selection came back empty, and ``check_close_cross`` compares the landed close against
the close the lake settled from its own captured quotes. So a wrong convention costs this view
a ``BarsAbsent`` and never a wrong close.

**The threshold is compared in whole cents.** Intrinsic per share is the close minus the
strike for a call and the strike minus the close for a put, floored at zero, and
exercise-by-exception takes anything one cent or more in the money. Both inputs are dollar
amounts the vendor quotes to the penny and their IEEE double difference is not the penny:
``757.39 - 757.38`` is ``0.009999999999990905``, which is less than ``0.01``, so a float
comparison abandons a contract the OCC exercises. That is reachable on this lake's own grid,
where the expiring series are a dollar apart near the money and a closing print is a
two-decimal number. So both sides scale to integer cents and the answer carries
``intrinsic_cents`` rather than a float, which is what keeps a caller from reintroducing the
same comparison one layer up.

The vendor's own ``intrinsic_value`` column is not this number. It is the signed difference
with no floor, on 310 of SPY's 310 contracts expiring on 2026-09-15, and negative on 155 of
them: a 550 put with the underlying at 757.39 carries ``-207.39``. Half of every expiry roster
is out of the money, so taking that column would settle half the roster at a large negative
number.

**A contract settles at intrinsic only if it delivers its multiplier in shares of the
ticker.** Two columns say whether it does, and both are read. ``non_standard`` is the vendor's
classification and ``option_deliverables_list`` is its typed description, and
``splits.Deliverable.same_as`` already separates the two on exactly that ground. Reading both
is the gate shape the split detector runs on the same column, the vendor against itself: they
agree that it is standard, they agree that it is not, or they disagree and the contract is
withheld either way. The parse is ``lake.splits``'s, through ``deliverable_of_row``, because
two parsers for one vendor column would be two answers to what a sealed row means.

**What refuses the whole read, and what marks one row.** A condition about the roster's
membership refuses, because a roster that is silently short is the failure nothing downstream
can detect. A condition about one contract's own terms marks that contract, because refusing
would take the rest of the roster away with it.

So an unreadable ``expiration_date`` refuses: a row whose expiration cannot be read cannot be
placed inside or outside the roster, which is the same reason ``_load_surface`` refuses a
partition holding a null ``row_kind``. Unreadable includes a stamp carrying no UTC offset, which
parses cleanly and then resolves against whatever timezone the process runs in, so the roster
would differ by machine with nothing raised. A daily close the view cannot read refuses too,
because it settles no contract on the session: a partition with no row, a null close, or a close
that is not a whole number of cents. Production cannot produce the middle one, since the daily
gate holds a candle whose close is missing, but the schema permits what the gate refuses and a
caller can point ``lake_root`` at any lake.

A term the view cannot read marks its own row instead, because it is one contract's problem: a
missing side, strike or multiplier, a deliverables list the parse refuses, a settlement code that
is absent rather than naming a convention, and a strike no whole number of cents can carry. A NaN
or an infinity is one of these rather than an arithmetic error, for the reason ``_number`` gives.

One consequence is worth stating rather than discovering. The daily gate compares a candle
against the *following* session's captured close, so a session's settlement is available one
session later at the earliest. D19's OI view carries the same lag for its own reasons.
"""

from __future__ import annotations

from datetime import date, datetime
from math import isfinite
from pathlib import Path

import pyarrow as pa

from lake.bars import session_of
from lake.loader import ADJUST_NONE, load_bars, load_chain, resolve_lake_root
from lake.splits import Deliverable, DeliverableUnreadable, deliverable_of_row
from lake.vendor import DAILY_FREQ

# The chains columns this view reads. Named here rather than at each use, so a reader can see
# that it reads nine of the chains schema's several dozen.
#
# The two witnesses ``_standard`` compares are ``non_standard``, the vendor's classification, and
# ``option_deliverables_list``, its typed description. ``deliverable_note`` is read because
# ``splits._reading`` takes it as part of the same tuple, and nothing here consults it.
OCC_SYMBOL = "occ_symbol"
EXPIRATION_DATE = "expiration_date"
PUT_CALL = "put_call"
STRIKE_PRICE = "strike_price"
MULTIPLIER = "multiplier"
SETTLEMENT_TYPE = "settlement_type"
NON_STANDARD = "non_standard"
OPTION_DELIVERABLES_LIST = "option_deliverables_list"
DELIVERABLE_NOTE = "deliverable_note"

# The bars column the close is read from.
CLOSE_COLUMN = "close"
BAR_TS_COLUMN = "bar_ts"

# The two sides a contract can take, as the vendor spells them. All 73,132 rows in the lake's
# six close-of-record chains carry one of these two, evenly split.
CALL = "CALL"
PUT = "PUT"

# The vendor's code for PM settlement, where the session's close is what settles the contract.
#
# **This reading is an assumption and the lake cannot confirm it.** ``journal.py`` maps
# ``settlementType`` onto the column and documents no value, and nothing else in this package
# or in ``docs/design.md`` says what any of the vendor's enum codes stand for.
#
# The lake holds seven close-of-record chains and 73,134 rows across them. 73,132 carry ``P``,
# across all four ``expiration_type`` kinds, and none carries a different code. The other two are
# in SPY's 2026-09-02 partition and carry no code at all, which ``_verdict`` answers as a term it
# cannot read rather than as AM settlement. So no row the lake holds reaches the comparison
# below with a value, and if the reading is wrong the failure runs as a false negative, an
# AM-settled contract this test does not catch, rather than a standard contract it wrongly
# withholds.
SETTLEMENT_TYPE_PM = "P"

# The two verdicts. They are D19's tokens, reused rather than respelled as synonyms, and each
# view's schema pins its own copy for the reason the module docstring gives.
VERDICT_SETTLED = "settled"
VERDICT_ABSENT = "absent"

# Why a contract carries no settlement. Each is a fact about that one contract rather than
# about the session, which is what makes it a marker on a row instead of a refusal.
REASON_AM_SETTLED = "am_settled"
REASON_NON_STANDARD = "non_standard"
REASON_DELIVERABLE_DISAGREES = "deliverable_disagrees"
REASON_STRIKE_NOT_IN_CENTS = "strike_not_in_cents"
REASON_TERMS_UNREADABLE = "terms_unreadable"

# How far a scaled dollar amount may sit from a whole number of cents and still be read as
# one. A penny-denominated value lands within a few units in the last place of its scaled
# form: ``757.38 * 100`` is ``75737.99999999999``, which is 1.5e-11 away from the integer. The
# smallest real disagreement this has to catch is half a cent, 0.5 away, so nothing about where
# this constant sits in that gap is doing work.
_CENT_EPSILON = 1e-6

# What the OCC's exercise-by-exception takes, in whole cents.
EXERCISE_THRESHOLD_CENTS = 1

# What an ``int64`` column can carry. ``intrinsic_cents`` is one, and Python's integers are
# unbounded, so a value past this reaches Arrow rather than the reader.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

SETTLEMENT_VIEW_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("session", pa.string()),
        ("occ_symbol", pa.string()),
        ("put_call", pa.string()),
        ("strike_price", pa.float64()),
        ("multiplier", pa.float64()),
        ("settlement_close", pa.float64()),
        ("intrinsic_cents", pa.int64()),
        ("exercised", pa.bool_()),
        ("verdict", pa.string()),
        ("reason", pa.string()),
    ]
)

__all__ = [
    "EXERCISE_THRESHOLD_CENTS",
    "REASON_AM_SETTLED",
    "REASON_DELIVERABLE_DISAGREES",
    "REASON_NON_STANDARD",
    "REASON_STRIKE_NOT_IN_CENTS",
    "REASON_TERMS_UNREADABLE",
    "SETTLEMENT_VIEW_SCHEMA",
    "SETTLEMENT_TYPE_PM",
    "VERDICT_ABSENT",
    "VERDICT_SETTLED",
    "CloseUnreadable",
    "ExpirationUnreadable",
    "SettlementError",
    "settlement_view",
]


class SettlementError(Exception):
    """Base for every reason this view produces no table at all.

    It is not a ``LoadError``. Every way a *read* resolves to nothing already raises one of
    those out of the loader and this view passes them through unchanged. What these two say is
    that the reads succeeded and the session still has no answer, which is a different
    statement and deserves a different name.
    """


class ExpirationUnreadable(SettlementError):
    """Raised when a row's expiration cannot be read, so the roster's membership is in doubt.

    This refuses the whole read rather than dropping the row. A contract whose expiration
    cannot be read might be one that expires on this session, and a roster silently missing an
    expiring contract is the one failure nothing downstream can detect. The same reasoning
    makes ``_load_surface`` refuse a partition holding a null ``row_kind``.
    """


class CloseUnreadable(SettlementError):
    """Raised when the session's daily bar carries no close to settle against.

    ``journal.BARS_SCHEMA`` makes ``bar_ts`` the one non-null column, so a null close is a
    shape the lake permits. The evening sweep's gate does not: ``check_close_cross`` does not
    agree when either number is missing, so such a candle is held and never lands. This refuses
    anyway, because the schema permits what the gate refuses, a caller can point ``lake_root``
    at any lake, and arithmetic against ``None`` is a ``TypeError`` rather than an answer.
    """


def settlement_view(
    ticker: str,
    day: date | str,
    *,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """What each contract expiring on ``day`` settled at, as a table of verdicts.

    One row per contract in the session's close-of-record chain whose expiration falls on that
    session, ordered by ``occ_symbol``. A row is ``settled`` and carries ``intrinsic_cents``
    and ``exercised``, or it is ``absent`` and names the reason.

    ``settlement_close`` is the session's official equity close and is carried on every row,
    settled or not, because it is a property of the session rather than of the contract.

    ``lake_root`` and ``include_quarantined`` carry the meanings the loader's doors give them.
    The flag reaches both reads, so a verdict on either surface refuses this view by default.

    Both reads raise their own ``LoadError`` unchanged: ``NoOptionClose`` for a session whose
    chain carries no option-close cycle, ``BarsAbsent`` for one with no daily partition,
    ``PartitionAbsent`` and ``PartitionQuarantined`` for either surface, and ``PartialRead``
    for a projection that could not complete. This view adds ``ExpirationUnreadable`` and
    ``CloseUnreadable``, both under ``SettlementError``.

    A session whose chain holds no contract expiring on it returns an empty table at
    ``SETTLEMENT_VIEW_SCHEMA``. That is a real answer rather than an ambiguous one, because
    every way this read can fail raises before the roster is built.
    """
    session_text = day.isoformat() if isinstance(day, date) else str(day)
    session = date.fromisoformat(session_text)
    root = resolve_lake_root(lake_root)

    chain = load_chain(
        ticker, session_text, lake_root=root, include_quarantined=include_quarantined
    )
    roster = _roster(chain, ticker, session, session_text)
    close, close_cents = _settlement_close(root, ticker, session, session_text, include_quarantined)

    return _answer(ticker, session_text, session, roster, close, close_cents)


def _roster(
    chain: pa.Table, ticker: str, session: date, session_text: str
) -> list[dict[str, object]]:
    """The chain's rows whose expiration falls on ``session``, ordered by ``occ_symbol``.

    The expiration is read as the Eastern calendar date of the stamp it holds, through
    ``bars.session_of``. That function is already the rule for how a stamp names a session,
    pinned in ``journal.py`` on the bars schema, and ``load_bars`` reads a bar's session
    through it rather than off the ``date=`` path level.

    A string comparison would be a second rule. It would also be wrong as written: the column
    is an instant rather than a date, spelled ``2026-09-15T20:00:00.000+00:00``, so an equality
    against the session matches no row and the view would hand back an empty table on every
    session without raising. The stamp's time part is 16:00 Eastern, which is 20:00 or 21:00
    UTC depending on daylight time, so the Eastern and UTC readings agree on all 73,132 stamps
    the lake holds. The Eastern one is taken because it is the one already decided, not because
    the two differ today.
    """
    rows = chain.select(
        [
            name
            for name in (
                OCC_SYMBOL,
                EXPIRATION_DATE,
                PUT_CALL,
                STRIKE_PRICE,
                MULTIPLIER,
                SETTLEMENT_TYPE,
                NON_STANDARD,
                OPTION_DELIVERABLES_LIST,
                DELIVERABLE_NOTE,
            )
            if name in chain.column_names
        ]
    ).to_pylist()

    roster = []
    for row in rows:
        stamp = row.get(EXPIRATION_DATE)
        expires_on = _expires_on(stamp)
        if expires_on is None:
            raise ExpirationUnreadable(
                f"{ticker} {session_text} holds a contract, {row.get(OCC_SYMBOL)!r}, whose "
                f"{EXPIRATION_DATE} is {stamp!r}, which names no session. Whether it belongs "
                "in this session's expiry roster cannot be decided, so the roster cannot be "
                "vouched for."
            )
        if expires_on == session:
            roster.append(row)
    roster.sort(key=lambda row: str(row.get(OCC_SYMBOL)))
    return roster


def _expires_on(stamp: object) -> date | None:
    """The session a contract's expiration stamp names, or ``None`` when it names none.

    The reading is ``bars.session_of``'s, which is the rule for how a stamp names a session and
    is what ``load_bars`` uses on a bar. This adds the precondition that function documents and
    does not enforce: "the stamp is a UTC instant".

    An offset is what makes the reading a fact rather than a property of the machine. A naive
    stamp parses, and ``astimezone`` then reads it in whatever timezone the process is running
    in, so the same chain would put a contract in the expiry roster on one machine and leave it
    out on another, with nothing raised either way. That is exactly the silently short roster
    ``ExpirationUnreadable`` exists to prevent, so a stamp with no offset is one this cannot
    read. The weakness is ``session_of``'s own and is marketlake #385; this check comes out when
    that lands.

    Every one of the 19,775,426 data rows in the lake's sealed chains carries an offset, so
    nothing on disk reaches the refusal today.
    """
    if not isinstance(stamp, str):
        return None
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return None
    return session_of(stamp)


def _settlement_close(
    root: Path, ticker: str, session: date, session_text: str, include_quarantined: bool
) -> tuple[float, int]:
    """The session's official equity close, in dollars and in whole cents.

    The daily gate checks that the session came back rather than that exactly one candle did, so
    a partition holding two candles for the session is not refused. The close is then the last
    candle by the instant its stamp names, which is what ``load_bars`` has already ordered the
    answer by, so the last row is it.

    ``bars._bar_close`` answers the same question for the gate and answers it differently.
    It sorts the raw stamp text, and the same instant has more than one spelling, so a response
    mixing ``-04:00`` and ``+00:00`` sends the two readings apart. The loader's module docstring
    carries the measurement that says this is not hypothetical, 408 distinct ``snap_ts`` texts
    naming 406 distinct instants in one live partition. Comparing instants is the reading that
    survives that, so this takes it, and the gate's text sort is marketlake #386.

    Three shapes refuse rather than settling anything, and all three are session-wide because
    the close is one number for the whole roster: an empty partition, a null close, and a close
    that is not a whole number of cents. The last one matters because the comparison below runs
    in cents, and a close the arithmetic cannot represent would otherwise mark every contract
    on the session with a reason that names the strike.
    """
    bars = load_bars(
        ticker,
        DAILY_FREQ,
        session,
        session,
        adjust=ADJUST_NONE,
        lake_root=root,
        include_quarantined=include_quarantined,
    )
    # ``load_bars`` orders its answer by the instant each ``bar_ts`` names, so the session's
    # close is the last row rather than a value this has to search for.
    close = None
    if bars.num_rows and CLOSE_COLUMN in bars.column_names:
        close = _number(bars.column(CLOSE_COLUMN)[-1].as_py())
    if close is None:
        raise CloseUnreadable(
            f"{ticker} {session_text} has no daily close to settle against, because its bars "
            "partition holds no row or the row it holds carries no close. No contract expiring "
            "on that session can be settled."
        )
    cents = _cents(close)
    if cents is None:
        raise CloseUnreadable(
            f"{ticker} {session_text} settled at {close!r}, which is not a whole number of "
            "cents. Exercise-by-exception is a penny threshold, so a close the cent "
            "arithmetic cannot represent settles nothing on that session."
        )
    return close, cents


def _answer(
    ticker: str,
    session_text: str,
    session: date,
    roster: list[dict[str, object]],
    close: float,
    close_cents: int,
) -> pa.Table:
    """The roster settled against ``close``, contract by contract."""
    columns: dict[str, list[object]] = {name: [] for name in SETTLEMENT_VIEW_SCHEMA.names}
    for row in roster:
        verdict, reason, intrinsic, exercised = _verdict(ticker, session, row, close_cents)
        columns["ticker"].append(ticker)
        columns["session"].append(session_text)
        columns["occ_symbol"].append(row.get(OCC_SYMBOL))
        columns["put_call"].append(row.get(PUT_CALL))
        columns["strike_price"].append(_number(row.get(STRIKE_PRICE)))
        columns["multiplier"].append(_number(row.get(MULTIPLIER)))
        columns["settlement_close"].append(close)
        columns["intrinsic_cents"].append(intrinsic)
        columns["exercised"].append(exercised)
        columns["verdict"].append(verdict)
        columns["reason"].append(reason)
    return pa.table(columns, schema=SETTLEMENT_VIEW_SCHEMA)


def _verdict(
    ticker: str, session: date, row: dict[str, object], close_cents: int
) -> tuple[str, str | None, int | None, bool | None]:
    """One contract's verdict, and the settlement behind it when there is one.

    The order the conditions run in is the order each takes a contract out of the settlement,
    and it runs from the widest question to the narrowest. The settlement convention comes
    first, because what a contract settles at is meaningless if it does not settle at the close.
    Then whether the terms are there to read at all, since a row missing one cannot be judged
    standard either. Then what the contract delivers. Then whether the arithmetic can
    represent the strike.
    """
    settlement_type = row.get(SETTLEMENT_TYPE)
    if settlement_type is None:
        # A missing code is not a statement that the contract settles at the open. The two are
        # folded together by an inequality, and one of them is a positive claim about the
        # contract while the other is the absence of any claim. The lake holds the second shape
        # already: SPY's 2026-09-02 close of record carries two rows whose ``settlement_type``,
        # ``multiplier``, ``non_standard`` and deliverables list are all null.
        return VERDICT_ABSENT, REASON_TERMS_UNREADABLE, None, None
    if settlement_type != SETTLEMENT_TYPE_PM:
        return VERDICT_ABSENT, REASON_AM_SETTLED, None, None

    side = row.get(PUT_CALL)
    strike = _number(row.get(STRIKE_PRICE))
    if side not in (CALL, PUT) or strike is None or _number(row.get(MULTIPLIER)) is None:
        return VERDICT_ABSENT, REASON_TERMS_UNREADABLE, None, None

    standard = _standard(ticker, session, row)
    if standard is None:
        return VERDICT_ABSENT, REASON_TERMS_UNREADABLE, None, None
    if standard is False:
        return VERDICT_ABSENT, REASON_NON_STANDARD, None, None
    if standard is not True:
        return VERDICT_ABSENT, REASON_DELIVERABLE_DISAGREES, None, None

    strike_cents = _cents(strike)
    if strike_cents is None:
        return VERDICT_ABSENT, REASON_STRIKE_NOT_IN_CENTS, None, None

    signed = close_cents - strike_cents if side == CALL else strike_cents - close_cents
    intrinsic = max(0, signed)
    return VERDICT_SETTLED, None, intrinsic, intrinsic >= EXERCISE_THRESHOLD_CENTS


# What ``_standard`` returns when the two witnesses do not say the same thing. It is a distinct
# object rather than a third boolean or a string, so a caller cannot compare it to ``True`` by
# accident and every branch above has to name it.
_DISAGREES = object()


def _standard(ticker: str, session: date, row: dict[str, object]) -> object | None:
    """Whether the contract delivers its multiplier in shares of ``ticker``, per both witnesses.

    ``True`` when both say it does, ``False`` when both say it does not, ``_DISAGREES`` when
    they differ, and ``None`` when either cannot be read.

    The flag classifies the contract and the list describes what it delivers, which is the
    separation ``splits.Deliverable.same_as`` already makes for its own reasons. Reading both
    and requiring them to agree is the gate shape the split detector runs on the same column:
    the same fact written down twice, with a disagreement treated as the drifted payload rather
    than as a number to act on.

    A mini contract needs no clause of its own. It is a tenth-size contract, so its deliverable
    is a tenth of the shares, and the units-against-multiplier test below already describes it.
    """
    try:
        deliverable = deliverable_of_row(row, session)
    except DeliverableUnreadable:
        return None

    if deliverable.non_standard is None:
        return None

    # The flag's polarity is the opposite of this function's: ``non_standard`` true means the
    # contract is not the plain one, so standard-by-the-flag is its negation. Comparing the flag
    # to the list without turning it round would make every ordinary contract a disagreement.
    by_flag = not deliverable.non_standard
    by_list = _delivers_shares(deliverable, ticker)
    if by_flag is not by_list:
        return _DISAGREES
    return by_list


def _delivers_shares(deliverable: Deliverable, ticker: str) -> bool:
    """Whether a deliverable is the plain one settlement at intrinsic assumes.

    Four things have to hold, and each names one way a contract stops being the plain thing: it
    delivers one security and not a basket, that security is the underlying itself, the count
    is the contract's own multiplier, and no cash rides beside the shares. A contract failing
    any of them is what #136 calls an adjustment that is not a scalar, and its settlement is
    not the close minus the strike.
    """
    return (
        deliverable.entries == 1
        and not deliverable.cash
        and deliverable.symbol == ticker
        # ``multiplier`` is a number by the time this runs, because ``_verdict`` has already
        # returned ``terms_unreadable`` for a row whose multiplier ``_number`` refused. A guard
        # here would be a branch nothing can reach, which reads as a case that can happen.
        and deliverable.units == deliverable.multiplier
    )


def _cents(value: float) -> int | None:
    """``value`` dollars as whole cents, or ``None`` when it is not a whole number of them.

    A penny-denominated amount scales to within a few units in the last place of an integer,
    while a half-cent strike lands half a cent away, so the tolerance separates the two by orders
    of magnitude rather than by a hair. The inexact case is real rather than theoretical, and
    common: ``0.07 * 100`` is ``7.000000000000001`` and ``1.15 * 100`` is ``114.99999999999999``,
    while ``757.38 * 100`` happens to be exactly ``75738.0``. Which pennies scale exactly is not
    something a reader can predict, which is why the comparison never assumes it.
    """
    scaled = value * 100
    nearest = round(scaled)
    if abs(scaled - nearest) > _CENT_EPSILON:
        return None
    if not _INT64_MIN <= nearest <= _INT64_MAX:
        # ``intrinsic_cents`` is an ``int64`` column, and Python's own integers are not bounded,
        # so a strike of ``1e17`` scales to a whole number of cents that ``pa.table`` then refuses
        # while building the answer. That refusal arrives as an ``OverflowError`` from Arrow with
        # the whole roster already computed, so the bound is checked here where the value can
        # still become one contract's marker.
        return None
    return int(nearest)


def _number(value: object) -> float | None:
    """``value`` as a finite float, or ``None`` when it is not one.

    A bool is excluded by name, because ``float(True)`` is ``1.0`` and would read as a strike of
    one dollar. No test holds that clause and none can: both call sites read a ``pa.float64()``
    column, and Arrow casts a bool to ``1.0`` as the partition is written, so a bool never
    survives to be read back. It stays because ``bars._bar_close`` carries the same exclusion
    where it *is* reachable, over vendor rows that are still plain dicts, and because a helper
    that answers "is this a number" should not answer yes for ``True`` whoever calls it.

    A NaN or an infinity is excluded for the reason the rest of this package already gives.
    ``actions.build_entry`` refuses a non-finite amount because "a NaN amount also turns every
    adjusted price it touches into a NaN", and ``splits._deliverable`` refuses a non-finite unit
    count. Both are ``pa.float64()`` columns, like ``strike_price`` and ``close``, so the schema
    permits the value and every reader has to decide what it means. Here it means the term cannot
    be read: ``int(float("nan"))`` raises ``ValueError`` and ``int(float("inf"))`` raises
    ``OverflowError``, so admitting one would end the read on a traceback and take the whole
    roster with it, which is the opposite of what this view says it does with a bad contract.
    """
    if value is None or isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if not isfinite(value):
        return None
    return float(value)
