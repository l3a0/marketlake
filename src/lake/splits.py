"""The split detector: reading an OCC re-symboling out of sealed chains.

A split is the corporate action that changes every strike in a chain, and it announces
itself in data the lake already holds. Recording it needs no vendor call, only a read over
sealed chains. Nothing here fetches.

``docs/design.md`` names two split signals in one sentence. The strike-vs-spot scale guard
trips, and the OCC re-symbols the chain. Both are here. The re-symboling is marketlake #279's
and the scale guard is #408's, and they sit together because they answer one question off one
read of one surface.

**They are not two ways of seeing the same split.** The re-symboling signal lands an entry
only when the deliverable moved, which :meth:`Deliverable.same_as` decides over six fields. A
whole-ratio split moves none of them, because strikes scale by the ratio and the contract
count absorbs the rest, so a 2:1 reaches ``REASON_DELIVERABLE_UNCHANGED``, is counted as a
rename and appends nothing. Everything smaller than a whole ratio changes what a contract
delivers and is already this walk's. What is left over is the whole-ratio family alone, and
the scale guard is the only signal the lake has for it. That gap reaches a shipped consumer:
``continuity_view`` takes its cumulative ratio from ``load_bars``'s two answers, and
``load_bars`` builds its split view by dividing each close by the ledger's ``split_ratio``,
so with no entry both reads agree and nothing is normalized.

**The signal is the root, and the vendor supplies it as a column.** ``CHAINS_SCHEMA``
carries ``option_root``, Schwab's own ``optionRoot``, and a session reduces to the *set* of
roots its contracts carry. The boundary is the first session whose root set holds a root the
previous session's set did not.

Three readings were measured against the live lake and two of them fail.

1. Reading the signal as "a symbol the lake has not seen before" files about a thousand
   splits a day. Across the two ordinary sessions of 2026-09-14 and 2026-09-15, with no
   split in either, SPY gained 454 ``occ_symbol`` values it had never carried and QQQ gained
   542, every one under the unchanged root, because new strikes and new expiries list daily.
2. Keying on the symbol fails the harder version of the same trap, which the lake already
   holds. ``occ_symbol`` is 23 characters on 2026-09-02 and 21 on the later partitions,
   because the vendor narrowed an eight-digit expiry to six. Every symbol changed and no
   split happened.
3. The root survives both. ``loader._occ_root`` returns ``SPY`` under both spellings.

``option_root`` is the vendor's answer and ``loader._occ_root`` is a guess, and that guess's
own docstring names "a symbol a corporate action rewrote" as a case where it comes apart
from the partition's ticker, which is exactly this module's case. So the column is read
first. It is not populated everywhere, though: it is null on both rows of the lake's
2026-09-02 partition and non-null on every row of the other four. So the slice stays as the
fallback rather than one of the two being chosen outright. The two agree on all 19,799,808
rows of the four 2026-09-14 and 2026-09-15 partitions.

**A root change alone cannot tell a split from a rename.** ``SecurityMaster.remap`` says so
outright: a ticker rename and an OCC re-symboling are the same operation over different
identifier kinds. What separates them is the deliverable. A rename carries the same
deliverable under a new symbol, and a split changes it. That distinction has to be made
before an entry is built, because ``actions.append`` accepts a ``split_ratio`` of ``1.0``
without complaint, and a rename mis-read as a split would land a no-op factor that every
adjusted view then reads as a real corporate action.

**Three other things also change a root set and are not splits either**, and each would land
a fabricated factor that the ledger's key cannot take back.

1. *A newly listed standard series.* An OCC adjustment turns standard contracts into
   non-standard ones, so a gained root whose contracts the vendor still calls standard has no
   adjustment behind it. This is the ordinary state of a ticker in the weeks after a real
   adjustment, when a fresh standard series lists beside the adjusted one, and reading it as a
   boundary lands the first split's own ratio inverted.
2. *A root returning.* A set difference has no direction. A root whose contracts all expire
   out of one session and list again in the next reads as a gain, and the ratio is then
   computed backwards. So every root the walk has watched an instrument carry is remembered,
   and a root coming back is counted rather than compared.
3. *A mini option listing.* A mini contract is a tenth-size contract under its own root, so
   the first one to list gains a root and delivers a tenth of what a standard contract does,
   which is the exact shape of an adjustment. ``mini`` is the vendor's flag for it and it is
   ``False`` on all 19,799,808 data rows the lake holds, so reading it drops nothing today
   and is what keeps a ten-for-one reverse split out of the ledger the day that changes.

None of the three is a finding. Nothing was refused, so there is nothing for a human to
resolve, and a held finding never clears. They are counted on the report instead, which is
what tells a run that met one from a run that read nothing at all.

**Where the ratio comes from.** ``actions.append`` refuses an entry whose ``split_ratio`` is
null, and a root change is a boolean: it says a split happened and carries no number. Four
more captured columns are the evidence and none of them needs a vendor call.
``option_deliverables_list`` is the precise one. SPY's 5,318,600 rows on 2026-09-15 all
carry the same string, ``[{"assetType": "STOCK", "currencyType": null, "deliverableUnits":
100.0, "symbol": "SPY"}]``. ``deliverableUnits`` is a typed number rather than
``deliverable_note``'s free text, so it is what the ratio is computed from, though it
arrives as JSON in a string column and has to be parsed. An OCC adjustment is a change to
the deliverable, so the ratio is the vendor's own statement of the adjustment rather than
something inferred from prices.

**The gate compares the vendor against itself.** No gate existed to inherit. A second vendor was
considered and rejected, and ``docs/design.md`` pins the cut. What replaced it is the lake's own
second observation, and the strike ladder against spot is :func:`check_strike_scale` below rather
than this gate's. So corroboration here comes from the one other place the deliverable is written
down: ``deliverable_note``, the vendor's free-text spelling of the same fact. The ratio the typed
``deliverableUnits`` produces has to agree with the ratio the note's share counts produce. That is
the shape ``actions.check_dividend_consistency`` already has, where the vendor's annualized figure
is read against its own per-event amount, and it catches the same class of defect: one of two
fields carrying an adjustment the other does not.

**The prior side of a ratio comes from the standard contracts.** An adjustment is a change
*from* something, and the standard series is what names that something. Reading the previous
session whole instead works only until that session carries two roots, which is the ordinary
state from the day after any adjustment onwards, and ``deliverable_of`` then refuses because
the session's contracts disagree about what they deliver. Usually both sides sit in the
boundary session itself, which is what #279 means by a contract still naming 100 units beside
one naming another number.

**A non-standard adjustment is held rather than flattened.** #136 states the constraint. A
whole-ratio split maps exactly, because strikes scale by the ratio and the contract count
absorbs the rest. An uneven split or a special dividend changes the deliverable itself, and
a contract delivering shares plus cash has no multiplier that makes it comparable. There
#136's view surfaces the event instead of faking one. ``actions.append`` carries one
``split_ratio`` float and nothing else, so the line this module draws is what a single float
can faithfully describe: one stock deliverable, in the same underlying, with no cash
component and an unchanged contract multiplier. Everything else is held as a finding a human
reads. Growing the record to carry a deliverable rather than a multiplier is a different
deliverable and needs its own issue.

**A skipped session widens the window a boundary sits in, and ``ex_date`` cannot be
repaired.** ``ex_date`` sits in the ledger's key, so a corrected date lands under a new key
rather than superseding the wrong one, and every adjusted price then applies the split
twice. A corrected *ratio* supersedes cleanly and a corrected date does not. The walk skips
for five reasons, and each one widens that window:

1. A gap day, which raises ``NoOptionClose``. The lake's own 2026-09-08 through 2026-09-11
   are four of these per ticker, from a real auth outage, and ``load_chain`` raises it on 8
   of the 13 sealed chains partitions.
2. A quarantined partition, which raises ``PartitionQuarantined``. ``lake.oi`` is the
   precedent for catching it rather than letting it end the walk on the first one.
3. A partition the overflow projection could not present whole, which raises ``PartialRead``.
   The table is readable and incomplete and the exception refuses a bypass, so a comparison
   made across it would be a comparison against contents nobody saw in full. A manifested
   partition whose file is gone raises ``PartitionAbsent`` and is skipped beside it, because
   the manifest is the lake's record of what it sealed rather than a guarantee the file is
   still there.
4. A ticker-day outside the instrument's capture span. ``capture_spans.py`` has already
   decided what such a day is: before a first span, after a closed span's end, and between
   two spans are out of scope, never gaps.
5. A session flagged ``suspect`` or ``is_chain_truncated``. A response far under its
   trailing-median contract count is journaled anyway and tagged, and a thin chain carries a
   thin root set, so a truncated *previous* session makes the next ordinary one look like it
   gained a root.

So a boundary lands only when the two sessions either side of it are adjacent in the
manifest, with no sealed ticker-day of that ticker skipped between them. A boundary whose
window is wider than one session is held and filed, naming both ends, rather than landing an
unrepairable date the detector guessed. The count of times reason 5 has fired is zero:
``is_chain_truncated`` and ``suspect`` are ``False`` on all 19,799,808 data rows in the lake.
It is still not deferred, for the reason ``lake.actions`` gives for gating before the battery
exists. An entry held today lands tomorrow at no cost, while a wrong one that lands corrupts
every adjusted price computed through it, and the ledger is append-only.

**A run's second night appends nothing.** ``observed_on`` and ``ex_date`` are both the
boundary session itself, never the night the walk ran. A split stays visible in sealed chains
forever, so a detector stamping the night it ran would re-derive the same split and fail
``actions.same_but_for_recorded_at`` every night, appending it again every night forever.
The two dates being equal is worth saying plainly, because the key exists to hold two
different things apart. A split detected from a root change has no vendor date at all, so the
boundary session is the only honest answer for either.

**A held split has no way to clear, and that is inherited rather than new.**
``report.write_withheld`` says a held finding files again every night and the repetition is
the record, and nothing prunes ``reports/``. Sealed chains never change, so a split this
gate refuses is re-derived identically every night. The only resolution is the ``manual``
entry #286 has not shipped, which is the same gap #284 already carries for dividends.
Nothing here claims a gate that can be cleared.

**Rescaling is not this module's and never will be.** Rescaling historical strikes in place
is storage mutation, which is how option databases quietly corrupt themselves. Cross-event
continuity is a derived view over raw plus the actions ledger, which is #136's. This module
detects and records, and stores nothing rescaled.

**Splits older than capture are out of scope.** This reads sealed chains, so it can only see
a split that happened after capture began. The backfill is #134's and the out-of-scope days
skipped above are exactly where its work sits.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from pathlib import Path

from lake.actions import (
    CHECK_INSTRUMENT_RESOLUTION,
    PROVENANCE_OBSERVED,
    REASON_PARTIAL_READ,
    REASON_PARTITION_ABSENT,
    REASON_QUARANTINED,
    TYPE_SPLIT,
    ActionKey,
    HeldFinding,
    Landed,
    MasterAbsent,
    Skip,
    UnresolvedSymbol,
    append,
    build_entry,
    by_reason,
    by_ticker,
    latest,
    read_master,
    resolve_instrument,
    same_but_for_recorded_at,
    surface_ticker_days,
)
from lake.clock import Clock, SystemClock
from lake.loader import (
    NoOptionClose,
    PartialRead,
    PartitionAbsent,
    PartitionQuarantined,
    _occ_root,
    load_chain,
)
from lake.occ_mapping import (
    CHECK_OCC_MAPPING,
    MappingError,
    Remapped,
    SymbolHistory,
    write_mappings,
)
from lake.paths import CHAINS
from lake.report import Withheld, write_withheld
from lake.security_master import (
    AmbiguousSymbol,
    MasterUnreadable,
    SecurityMaster,
    SecurityMasterError,
)

# The twelve chains columns this module reads. ``option_root`` is the signal and ``occ_symbol``
# is the fallback the root is derived from where the column is null. ``ssid`` is Schwab's own
# contract identifier, and it is what pairs a re-symboled contract's old symbol to its new one
# in ``lake.occ_mapping``, since the symbol is the thing that moves. The four after it are
# where the deliverable is written down. ``mini`` marks a tenth-size contract under its own
# root. ``suspect`` and ``is_chain_truncated`` are what say a session cannot bound a boundary.
# The last two are the scale guard's whole input: the strike ladder and the session's spot.
OPTION_ROOT = "option_root"
OCC_SYMBOL = "occ_symbol"
SSID = "ssid"
OPTION_DELIVERABLES_LIST = "option_deliverables_list"
DELIVERABLE_NOTE = "deliverable_note"
MULTIPLIER = "multiplier"
NON_STANDARD = "non_standard"
MINI = "mini"
SUSPECT = "suspect"
IS_CHAIN_TRUNCATED = "is_chain_truncated"
STRIKE_PRICE = "strike_price"
UNDERLYING_PRICE = "underlying_price"
CHAINS_COLUMNS = (
    OPTION_ROOT,
    OCC_SYMBOL,
    SSID,
    OPTION_DELIVERABLES_LIST,
    DELIVERABLE_NOTE,
    MULTIPLIER,
    NON_STANDARD,
    MINI,
    SUSPECT,
    IS_CHAIN_TRUNCATED,
    STRIKE_PRICE,
    UNDERLYING_PRICE,
)

# The three gates this module files a refusal under. They name what refused rather than what
# was refused, the way ``lake.actions``'s do, because the finding already carries the event.
# ``CHECK_INSTRUMENT_RESOLUTION`` is reused from there rather than respelled, since a master
# that cannot place a symbol says the same thing whichever walk met it.
CHECK_SPLIT_CONSISTENCY = "split_consistency"
CHECK_SPLIT_DELIVERABLE = "split_deliverable"
CHECK_SPLIT_BOUNDARY = "split_boundary"
# The scale guard's own. It is spelled for the operator rather than for this module, because
# ``sweep._subjects`` renders it into the nightly report file as ``<symbol> <day> <check>`` and
# ``write_withheld`` writes it into the finding's JSON, and those two are where it is read.
CHECK_STRIKE_SCALE = "strike_scale"
# The three that describe a corporate action on the session rather than a failure to read one.
# Only these suppress the scale guard, because only these tell an operator that the walk already
# has something to say about an adjustment here. ``CHECK_SPLIT_PAYLOAD`` and
# ``CHECK_OCC_MAPPING`` say a read came apart, which is not an answer to whether a whole-ratio
# split happened, and suppressing on one hides the split behind an unrelated unreadable row.
BOUNDARY_CHECKS = frozenset(
    {CHECK_SPLIT_CONSISTENCY, CHECK_SPLIT_DELIVERABLE, CHECK_SPLIT_BOUNDARY}
)
# The payload the read's own rules refused: a deliverable the columns do not agree about, or
# one they carry no usable number for. Each one would otherwise end the run as a traceback,
# which is neither fail-closed nor a record.
CHECK_SPLIT_PAYLOAD = "split_payload"

# How far the ratio the typed ``deliverableUnits`` produces may sit from the ratio
# ``deliverable_note``'s share counts produce before the gate holds the split out.
#
# It is relative, the way ``DIVIDEND_CONSISTENCY_TOLERANCE`` is, and it is tight because the
# two sides are the same kind of number taken from the same payload rather than two
# independent measurements. The only slack owed is the error of two IEEE-754 divisions, which
# is at most a few units in the last place, around 1e-16 relative. The smallest real
# disagreement the gate has to catch is one share in a hundred, which is 1e-2 relative. This
# constant sits seven orders of magnitude above the first and seven below the second, so
# nothing about where it lands in that gap is doing work.
SPLIT_CONSISTENCY_TOLERANCE = 1e-9

# The scale guard's three constants. Measured against the live lake at 16949e2 rather than
# chosen, and marketlake #408 carries the measurement in full.
#
# A session pair names a candidate ratio only when its spot moved by at least this much, up or
# down. Every whole ratio is 2 or more, so 1.5 is the midpoint below the smallest one, and the
# four adjacent pairs the lake holds sit at 0.999745 to 1.006586, nowhere near it.
WHOLE_RATIO_GATE = 1.5
# How far the spot ratio may sit from the candidate, relatively. The candidate is rounded to
# one whole ratio rather than matched against bands, and that is not a style choice. Bands of
# plus or minus 10% around 2, 3 and 4 are disjoint, then 5's runs to 5.500 while 6's starts at
# 5.400, and from there every ratio falls inside some band and the test stops saying anything.
# The reverse side breaks in the same place, with 1/5's bottom at 0.1800 under 1/6's top at
# 0.1833. Rounding leaves exactly one candidate, so no overlap is possible at any ratio, and
# this then answers only whether the move is close enough to it. It filters through 9 and meets
# the rounding boundary at 10, where the confirmation below carries the decision alone.
WHOLE_RATIO_TOLERANCE = 0.05
# How much of the previous session's ladder has to follow the candidate before the guard trips.
#
# A real adjustment confirms at 1.000000, because dividing every open contract's strike is what
# the adjustment does, and a standard series listing beside it only adds rungs. So the number
# worth measuring is the other side: what a *stationary* ladder confirms when spot moves by that
# ratio anyway, which is the crash this must not read as a split. Measured on the live ladders,
# across ratios of 2, 3, 4 and 10 in both directions, the highest is SPY's 0.287785 and QQQ's is
# 0.135755. This sits 0.212215 above that and 0.50 below what a real adjustment gives.
SCALE_CONFIRMATION_FLOOR = 0.50
# Where a strike is rounded, both on the ladder and on the rescaled value looked for in it.
#
# **It is the vendor's own precision, and one decimal more silently breaks the odd ratios.** The
# OCC symbol carries the strike in an eight-digit thousandths field, so a 3-for-1 rescaling of a
# 205 strike can only be listed as 68.333. Rounding the rescaled value to four places asks for
# 68.3333 instead, which no rung matches, and the confirmation collapses. Measured against the
# live ladders, with each rung divided by the ratio and written at three decimals, four places
# confirms 3:1 at 0.333, 6:1 at 0.333, 7:1 at 0.143 and 9:1 at 0.110, every one of them under
# the floor and read as an ordinary day. At three places all nine ratios confirm at 1.000.
# Rounding this far merges no rung either: SPY's 483 and QQQ's 523 stay distinct, and the vendor
# uses at most two decimals.
_STRIKE_PLACES = 3

# What ``deliverable_note`` looks like when it names a plain share count of one security.
# The live lake's is ``100 SPY`` on every row of both tickers. A note this does not match is
# not parsed further and the gate then has one number instead of two, which does not agree.
# That is deliberate rather than a gap: a note carrying cash beside shares, or two
# securities, is the shape the ledger's single float cannot describe either.
_NOTE = re.compile(r"^(\d+(?:\.\d+)?) ([A-Za-z][A-Za-z0-9./]*)$")

# What the vendor calls a deliverable of stock, as opposed to cash or another instrument.
ASSET_TYPE_STOCK = "STOCK"


class SplitError(Exception):
    """Base class for every reason this module refuses to read a split."""


class DeliverableUnreadable(SplitError):
    """Raised when the rows carry no deliverable this module can read as one number.

    Three shapes reach here: a session whose rows disagree about a column, a session
    carrying no deliverables list at all, and a list whose ``deliverableUnits`` is missing or
    is not a positive finite number. None of them is this run's to repair, and a run that
    died on one would lose every ticker it had not reached yet.
    """


class NonScalarDeliverable(SplitError):
    """Raised when no single ``split_ratio`` faithfully describes the adjustment.

    This is #136's second class, named. A contract delivering shares plus cash, or two
    securities, or the same count of a different security, or one whose contract multiplier
    moved, has no multiplier that makes it comparable. The ledger carries one float, so the
    honest answer is to surface the event rather than flatten it into a number that reads
    like a whole-ratio split and is not one.
    """


class BoundaryUnbounded(SplitError):
    """Raised when a skipped session leaves the boundary's own date in doubt.

    ``ex_date`` sits in the ledger's key, so a date the detector gets wrong cannot be
    superseded. A corrected entry lands under a second key and every adjusted price then
    applies the split twice. So a boundary whose window is wider than one session is filed
    rather than landed under a guess.
    """


# Why a ticker-day was not read, and each reason widens a boundary's window by one session.
# Three of them are ``lake.actions``' above, imported rather than restated, because the
# dividend walk meets the same three and one reason has to have one spelling. The rest are
# this surface's own, the close-of-record one included, because each names the tag its walk
# resolved against. This one is ``option_close`` and ``actions.REASON_NO_SPOT_CLOSE`` is
# ``spot_close``.
REASON_NO_OPTION_CLOSE = "no option close"
REASON_OUT_OF_SCOPE = "outside the capture span"
REASON_THIN = "suspect or truncated"
REASON_UNRESOLVED = "unresolved symbol"

# Why the scale guard could not compare a pair of sessions. None of these is a finding. A pair
# it could not read is a pair nobody judged, which is a different thing from one it judged and
# passed, and the report keeps them apart for the reason :class:`NotAnAdjustment` gives.
REASON_SCALE_WINDOW = "a skipped session sits between the pair"
REASON_NO_LADDER = "a session lists no strike"
REASON_NO_UNDERLYING = "a session names no single underlying price"
REASON_INSTRUMENT_CHANGED = "the pair spans two instruments"

# Why a session gained a root with no corporate action behind it. Each of these appends
# nothing and holds nothing, and each is counted, because a run that met one would otherwise
# read exactly like a run that met nothing at all.
REASON_DELIVERABLE_UNCHANGED = "the deliverable did not move"
REASON_STANDARD_SERIES = "the gained contracts are standard"
REASON_ROOT_RETURNED = "the root had been carried before"


@dataclass(frozen=True)
class NotAnAdjustment:
    """One root a session gained with no corporate action behind it, and which kind.

    A root change is not a split on its own, and three separate things produce one. The
    ledger records none of them, so what this exists for is the report: a counter is what
    tells a run that met a root change and correctly declined to record it from a run that
    read nothing at all.
    """

    ticker: str
    day: date
    reason: str


@dataclass(frozen=True)
class ScaleUnread:
    """One adjacent session pair the scale guard could not compare, and why."""

    ticker: str
    day: date
    reason: str


@dataclass(frozen=True)
class ScaleVerdict:
    """What the scale guard read off one pair of sessions.

    ``spot_ratio`` is the raw move, ``spot_prev / spot_now``, which runs the same way round as
    the ledger's own ratio: :func:`check_split_consistency` computes ``new.units /
    prior.units``, so a 3-for-2 reads 1.5 and a 2:1 would read 2.0.

    ``ratio`` is the whole ratio that move names, or ``None`` when it names none, which is what
    an ordinary session pair produces. ``confirmed`` is then the fraction of the previous
    session's ladder that followed it, and it is 0.0 rather than undefined when there was no
    candidate to follow.

    Both numbers ride the verdict rather than being recomposed by the caller, for the reason
    :class:`SplitConsistency` gives: they are what the withheld finding files, and a caller that
    recomputed them could file a pair the guard never saw.
    """

    ratio: float | None
    spot_ratio: float
    confirmed: float

    @property
    def holds(self) -> bool:
        """Whether this pair is a whole-ratio split the ladder agrees with."""
        return self.ratio is not None and self.confirmed >= SCALE_CONFIRMATION_FLOOR


@dataclass(frozen=True)
class Outcome:
    """What one session against the one before it came to.

    A session can produce a landed entry and a non-adjustment at once, because a chain can
    gain a returning root beside a genuinely new one, so this carries both rather than being
    one of several sentinels. ``mapped`` rides beside them for the same reason, and it is not
    a fifth sentinel either: the master write fires as soon as the boundary is confirmed, so
    *every* outcome past that point carries it. That is seven of this function's fifteen
    exits, and the seventh is worth naming because it is the least obvious. A boundary the
    ledger refuses because two tickers resolved to one instrument still re-symboled its
    contracts, and the mapping rows it writes name those contracts by their own symbols under
    their own fresh instrument ids, so a fault in which instrument a *ticker* names does not
    make them wrong. Holding them would lose a real identity change over a defect in a
    different row of the same table.
    """

    landed: Landed | None = None
    unchanged: bool = False
    not_adjustments: tuple[NotAnAdjustment, ...] = ()
    mapped: tuple[Remapped, ...] = ()


@dataclass(frozen=True)
class Deliverable:
    """What one contract delivers, as the vendor wrote it down.

    ``units`` is ``deliverableUnits`` off ``option_deliverables_list``, the typed number the
    ratio is computed from. ``note_units`` is the share count ``deliverable_note`` names, the
    vendor's free-text spelling of the same fact and the gate's second number. The two are
    separate fields rather than one reconciled value, because the gate is what reconciles
    them and a caller handing over one number could not be checked.

    ``entries`` and ``cash`` describe the shape of the list rather than its number, and they
    are what :class:`NonScalarDeliverable` is decided from together with ``symbol`` and
    ``multiplier``.
    """

    units: float
    symbol: str | None
    entries: int
    cash: bool
    note_units: float | None
    multiplier: float | None
    non_standard: bool | None

    def same_as(self, other: Deliverable) -> bool:
        """Whether two deliverables are the same thing written twice.

        A rename carries the same deliverable under a new symbol, so this is what tells one
        from a split. Six fields are compared rather than ``units`` alone, because a note
        that moved while the typed count did not is a vendor contradiction rather than a
        rename, and it belongs at the gate instead of being called a non-event here.

        ``non_standard`` is the one field left out, and deliberately. It classifies the
        contract rather than describing what the contract delivers, and this asks only
        whether the deliverable moved. The flag has its own two jobs, on
        :meth:`Session.standard` and :meth:`Session.standard_roots`.
        """
        return (
            self.units == other.units
            and self.symbol == other.symbol
            and self.entries == other.entries
            and self.cash == other.cash
            and self.note_units == other.note_units
            and self.multiplier == other.multiplier
        )


@dataclass(frozen=True)
class SplitConsistency:
    """What the gate compared, and whether it agreed.

    ``computed`` is the ratio the typed ``deliverableUnits`` produces and ``against`` is the
    ratio ``deliverable_note``'s share counts produce. Both ride the verdict rather than
    being recomposed by the caller, because they are the two numbers the withheld finding
    files and a caller that recomputed them could file a pair the gate never saw.

    ``against`` is ``None`` when either note could not be read as a plain share count. A gate
    missing an input has not agreed, which is what makes an unparseable note a held split
    rather than a silent one.
    """

    agrees: bool
    computed: float
    against: float | None


@dataclass(frozen=True)
class SplitReport:
    """What one run of the detection did, for the sign-off block.

    ``ExtractionReport`` reuses cleanly in structure and its ``render`` does not. Run against
    a split entry it prints "Dividend extraction over 1 sealed quotes ticker-day(s)" and
    "cash None", and both are wrong here. So this is a second form rather than a
    generalisation, and it shares the two records that carry a run's results,
    ``actions.Landed`` and ``actions.HeldFinding``.

    ``unchanged`` counts the splits the walk re-derived and found already in the ledger, and
    it is what makes a second run legible. ``not_adjustments`` counts the root changes with no
    corporate action behind them, each keeping the reason it was one, because those append
    nothing and hold nothing and a run that met one would otherwise read exactly like a run
    that met nothing at all.

    ``skipped`` carries every ticker-day the walk did not read, because each one widens the
    window a boundary can sit in and the render is where an operator sees how wide the lake's
    windows currently are.

    ``mapped`` counts the OCC mapping rows the run wrote into the security master. It is
    reported rather than inferred from ``appended``, because the two fire on different things:
    a rename writes mappings and appends nothing, and a run that rewrote the reference table
    every consumer resolves through should say so on its own sign-off block.

    The last three are the scale guard's. ``scale_pairs`` counts the adjacent session pairs it
    compared, ``scale_covered`` the pairs where it found a whole-ratio split the ledger already
    describes, and ``scale_unread`` the pairs it refused to judge with each one's reason. All
    three are on the block for one reason: a run that met something must not read like a run
    that met nothing, which is what ``not_adjustments`` already exists to keep apart.
    """

    ticker_days: int
    appended: tuple[Landed, ...]
    held: tuple[HeldFinding, ...]
    unchanged: int
    not_adjustments: tuple[NotAnAdjustment, ...]
    skipped: tuple[Skip, ...]
    mapped: tuple[Remapped, ...] = ()
    scale_pairs: int = 0
    scale_covered: int = 0
    scale_unread: tuple[ScaleUnread, ...] = ()

    @property
    def unfiled(self) -> tuple[HeldFinding, ...]:
        """Every held finding whose record could not be written down.

        A finding held and filed is a live condition a human can read. A finding held and not
        filed is the silence the producer exists to break, so it is what the command turns
        into a non-zero exit code.
        """
        return tuple(held for held in self.held if held.filed_at is None)

    def render(self) -> str:
        """A human-readable sign-off block."""
        lines = [
            f"Split detection over {self.ticker_days} sealed chains ticker-day(s)",
            f"  appended:  {len(self.appended)}",
        ]
        for landed in self.appended:
            entry = landed.entry
            lines.append(
                f"    - {landed.symbol} (instrument {entry['instrument_id']}) {entry['type']} "
                f"ex {entry['ex_date']} ratio {entry['split_ratio']} "
                f"({entry['provenance']}), observed {entry['observed_on']}"
            )
        lines.append(f"  held:      {len(self.held)}")
        for held in self.held:
            finding = held.finding
            detail = f"{finding.symbol} {finding.observed_on.isoformat()} {finding.check}"
            if finding.computed is not None or finding.against is not None:
                detail += f": {finding.computed} against {finding.against}"
            elif finding.exception:
                detail += f": {finding.exception}"
            lines.append(f"    - {detail}")
            if held.filed_at is None:
                lines.append(f"      NOT filed: {held.filing_error}")
            else:
                lines.append(f"      filed at {held.filed_at}")
        lines.append(f"  mapped:    {len(self.mapped)}")
        for remap in self.mapped:
            lines.append(
                f"    - {remap.ticker} (instrument {remap.instrument_id}) "
                f"{remap.old_symbol!r} [{remap.valid_from.isoformat()} -> "
                f"{remap.effective.isoformat()}) becomes {remap.new_symbol!r}"
            )
        lines.append(f"  unchanged: {self.unchanged}")
        lines.append(f"  not a split: {len(self.not_adjustments)}")
        lines.extend(by_reason(self.not_adjustments))
        lines.append(f"  skipped:   {len(self.skipped)}")
        lines.extend(by_reason(self.skipped))
        lines.append(f"  scale compared: {self.scale_pairs}")
        lines.append(f"  scale already recorded: {self.scale_covered}")
        lines.append(f"  scale not compared: {len(self.scale_unread)}")
        lines.extend(by_reason(self.scale_unread))
        return "\n".join(lines)


# -- reading one session -----------------------------------------------------


@dataclass(frozen=True)
class Session:
    """One ticker-day's option-close snapshot, reduced to what a boundary is decided from.

    ``roots`` is the set rather than one value, because a ticker-day does not reduce to a
    single root. An OCC adjustment re-symbols the open contracts while newly listed standard
    contracts keep the original root, so one chain can carry both at once. That is why the
    boundary rule asks which roots were *gained* rather than which root replaced which.

    ``rows`` keeps each row as the root it carries and the deliverable columns beside it, so
    the deliverable can be read back for a subset of the roots. The prior side of a ratio is
    read over the whole previous session and the new side over the gained roots alone, and
    without the per-root rows the second of those could not be asked for.

    ``strikes`` and ``spot`` are the scale guard's whole input and they default to empty, so a
    caller building a session to exercise the boundary rules says nothing about the ladder
    rather than having to furnish one.

    ``spot`` is the chains row's own ``underlying_price`` at the option close, and never the
    quotes surface's ``spot_close``. The two are different readings of the underlying and the
    loader says so: chains resolve against ``option_close`` and quotes against ``spot_close``,
    and ``spot_close`` is the pre-auction book rather than the price a contract was quoted
    against. Reading the chains column keeps a strike and the underlying it was quoted against
    on one row of one surface, out of one read.

    It is the one distinct value the session's rows carry, and ``None`` when they carry
    none or several. **A null is not a value and not a disagreement.** A row saying nothing
    about the underlying does not contradict a row that names it, which is how :func:`_column`
    already treats a column a partition never carried. Measured on the live lake, the
    option-close snapshot carries exactly one distinct ``underlying_price`` on all seven
    readable partitions, against 166 to 278 across a whole partition's minutes, which is why
    this is read off the snapshot and not off the file.
    """

    day: date
    instrument_id: int
    roots: frozenset[str]
    rows: tuple[tuple[str, dict[str, object]], ...]
    strikes: frozenset[float] = frozenset()
    spot: float | None = None

    def standard_roots(self) -> frozenset[str]:
        """The roots whose contracts the vendor flags standard.

        This is where the prior side of a ratio comes from. An OCC adjustment turns standard
        contracts into non-standard ones, so the standard series is what the adjusted
        contracts were adjusted *from*, and it is the only side of the comparison the data
        names rather than leaves to be guessed. Reading the previous session whole instead
        works only until that session carries two roots, which is the ordinary state from the
        day after any adjustment onwards.
        """
        return frozenset(root for root, row in self.rows if row.get(NON_STANDARD) is False)

    def standard(self, roots: frozenset[str]) -> bool | None:
        """Whether the contracts under ``roots`` are standard, or ``None`` when unsaid.

        ``False`` on every one of them is standard and ``True`` on every one is adjusted.
        Anything else, a null flag or two roots disagreeing, is unknown, and unknown is not
        read as either. A partition sealed before the column existed carries null on every
        row, and the live lake's own 2026-09-02 partition is one.
        """
        values = {row.get(NON_STANDARD) for root, row in self.rows if root in roots}
        if values == {False}:
            return False
        if values == {True}:
            return True
        return None


def _column(table, name: str) -> list[object]:
    """One column as a list, or a column of nulls when the partition does not carry it.

    A partition sealed before a column existed is a session with nothing to say about it
    rather than a run that ends. ``actions._observation`` reads its own columns the same way
    and for the same reason.
    """
    if name not in table.column_names:
        return [None] * table.num_rows
    return table.column(name).to_pylist()


def read_session(lake_root: Path, ticker: str, day: date, instrument_id: int) -> Session | str:
    """One ticker-day reduced to a :class:`Session`, or the reason it was not read.

    The read is one ``load_chain`` call, which returns the session's option-close snapshot
    rather than the whole partition. Measured on SPY's 2026-09-15 that is 13,100 rows in 0.17
    seconds against a partition holding 5,318,600.

    Going through the loader rather than reading the partition directly is already decided,
    and not here. ``actions.extract_dividends`` says a direct read is a little faster and is
    refused, because every read in the lake goes through the loader, so the quarantine guard
    and the overflow projection are asked once rather than skipped by a second path that
    would then keep skipping them forever. ``load_chain`` defaults ``include_quarantined`` to
    ``False`` and ``CLAUDE.md`` names that exclusion as a guard whose price is paid by
    building it late.

    Enumerating from the manifest bounds what absence can look like, and it does not remove
    absence. The manifest is the lake's record of what was sealed rather than a guarantee the
    file is still on disk, so ``PartitionAbsent`` is caught here like the rest. Four refusals
    return a reason rather than raising, because each is one session the walk cannot use
    rather than a run that has to end. ``lake.oi`` is the precedent for two of them, catching
    ``PartitionAbsent`` and ``PartitionQuarantined`` and turning each into an absence verdict
    that keeps its own reason rather than dropping it.

    A thin snapshot is the fifth refusal and it is not an absence either. A response far
    under its trailing-median contract count is journaled and tagged rather than discarded,
    and a thin chain carries a thin root set, so it cannot bound a boundary.

    **A mini contract is not part of any root set here.** A mini option is a tenth-size
    contract listed under its own root, so a chain that begins listing them gains a root and
    changes what a contract under it delivers, which is the exact shape of an adjustment and
    is not one. Reading the rows would land a fabricated ten-for-one reverse split in an
    append-only ledger. ``mini`` is the vendor's own flag for the contract size and is
    ``False`` on all 19,799,808 data rows the lake holds, so this drops nothing today and is
    what keeps the first mini listing from being recorded as a corporate action. A null flag
    is unknown rather than mini, so a row is dropped only when the vendor says so.
    """
    try:
        table = load_chain(ticker, day, lake_root=lake_root)
    except NoOptionClose:
        return REASON_NO_OPTION_CLOSE
    except PartitionQuarantined:
        return REASON_QUARANTINED
    except PartialRead:
        return REASON_PARTIAL_READ
    except PartitionAbsent:
        return REASON_PARTITION_ABSENT

    columns = {name: _column(table, name) for name in CHAINS_COLUMNS}
    if any(columns[SUSPECT]) or any(columns[IS_CHAIN_TRUNCATED]):
        return REASON_THIN

    rows: list[tuple[str, dict[str, object]]] = []
    for index in range(table.num_rows):
        row = {name: columns[name][index] for name in CHAINS_COLUMNS}
        if row[MINI] is True:
            continue
        rows.append((_root_of(row), row))
    return Session(
        day=day,
        instrument_id=instrument_id,
        roots=frozenset(root for root, _ in rows),
        rows=tuple(rows),
        strikes=frozenset(_ladder(row for _, row in rows)),
        spot=_spot(row for _, row in rows),
    )


def _ladder(rows) -> list[float]:
    """Every distinct strike the session lists, rounded to the ladder's own precision.

    Built off the rows that survived the ``mini`` filter, so the ladder is read from the same
    contracts the root set is. A mini contract lists at the same strike a standard one does, so
    dropping it removes a rung only where no standard contract carries it, and ``mini`` is
    ``False`` on every data row the lake holds. The count of those rows is marketlake #367's to
    sweep, so it is not restated here.
    """
    return [
        round(value, _STRIKE_PLACES)
        for value in {
            row.get(STRIKE_PRICE)
            for row in rows
            if isinstance(row.get(STRIKE_PRICE), int | float)
            and not isinstance(row.get(STRIKE_PRICE), bool)
        }
        if isfinite(value) and value > 0
    ]


def _spot(rows) -> float | None:
    """The one underlying price the session names, or ``None`` when it names none or several.

    A session whose rows disagree about the underlying has no single spot, and taking the first
    would let the file's own order decide what the guard compares, which is the refusal
    :func:`deliverable_of` already makes about its own reading.
    """
    values = {
        row.get(UNDERLYING_PRICE)
        for row in rows
        if isinstance(row.get(UNDERLYING_PRICE), int | float)
        and not isinstance(row.get(UNDERLYING_PRICE), bool)
    }
    usable = {value for value in values if isfinite(value) and value > 0}
    return usable.pop() if len(usable) == 1 else None


def _root_of(row: dict[str, object]) -> str:
    """The root a row carries: the vendor's column, or the slice off the OCC symbol.

    ``option_root`` is the vendor's own answer and it is read first. It is not populated
    everywhere, though, so ``loader._occ_root`` stays as the fallback rather than one of the
    two being chosen outright. A row carrying neither returns the empty string, which is a
    root like any other for the purpose of comparing two sets: it cannot be gained by a
    session that already had it, and a session that gains it has gained something the
    deliverable read below then has to explain.
    """
    root = row.get(OPTION_ROOT)
    if isinstance(root, str) and root.strip():
        return root.strip()
    occ = row.get(OCC_SYMBOL)
    return _occ_root(occ) if isinstance(occ, str) else ""


# -- reading the deliverable -------------------------------------------------


def deliverable_of(session: Session, roots: frozenset[str]) -> Deliverable:
    """What the session's contracts under ``roots`` deliver, as one reading.

    Every contract under one root delivers the same thing, so which row answers decides
    nothing. A disagreement raises instead of taking the first one, for the reason
    ``actions._observation`` gives about its own close of record: taking the first would let
    the file's own order decide what the ledger gets, silently.
    """
    selected = [row for root, row in session.rows if root in roots]
    if not selected:
        raise DeliverableUnreadable(
            f"{session.day.isoformat()} carries no rows under {sorted(roots)}"
        )

    readings = {_reading(row) for row in selected}
    if len(readings) > 1:
        raise DeliverableUnreadable(
            f"the {session.day.isoformat()} contracts under {sorted(roots)} disagree "
            f"about what they deliver, among {sorted(str(r) for r in readings)}"
        )
    return _deliverable(readings.pop(), session.day)


def deliverable_of_row(row: dict[str, object], day: date) -> Deliverable:
    """What one contract row says it delivers.

    :func:`deliverable_of` is the session-level door and this is the row-level one. They read
    the same four columns through the same parse, and they differ only in what they are given:
    that one agrees a whole root's rows first and this one takes a row as it stands.

    It exists because ``lake.settle`` has to decide, contract by contract, whether a settlement
    at official-close intrinsic describes what the contract actually delivers. Two parsers for
    one vendor column would be two answers to what a sealed row means, which is the second
    source of truth this design refuses everywhere else. So the reading is shared and only the
    policy differs: the gate above lets :class:`DeliverableUnreadable` end the boundary, and the
    settlement view catches it and marks that one row, because a refusal there would take the
    rest of the expiry roster away with it.

    ``day`` names the session for the refusal's own message, exactly as it does above.
    """
    return _deliverable(_reading(row), day)


def _reading(row: dict[str, object]) -> tuple:
    """One row's deliverable columns as a hashable tuple, for the agreement test above."""
    return tuple(
        row.get(name)
        for name in (OPTION_DELIVERABLES_LIST, DELIVERABLE_NOTE, MULTIPLIER, NON_STANDARD)
    )


def _deliverable(reading: tuple, day: date) -> Deliverable:
    """One agreed reading as a :class:`Deliverable`, or the reason it is not readable."""
    encoded, note, multiplier, non_standard = reading
    if not isinstance(encoded, str) or not encoded.strip():
        raise DeliverableUnreadable(
            f"the {day.isoformat()} contracts carry no {OPTION_DELIVERABLES_LIST}"
        )
    try:
        parsed = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise DeliverableUnreadable(
            f"the {day.isoformat()} {OPTION_DELIVERABLES_LIST} is not JSON: {exc}"
        ) from exc
    if not isinstance(parsed, list) or not parsed:
        raise DeliverableUnreadable(
            f"the {day.isoformat()} {OPTION_DELIVERABLES_LIST} names no deliverable"
        )

    stock = [
        item
        for item in parsed
        if isinstance(item, dict) and item.get("assetType") == ASSET_TYPE_STOCK
    ]
    if len(stock) != 1:
        raise DeliverableUnreadable(
            f"the {day.isoformat()} {OPTION_DELIVERABLES_LIST} names {len(stock)} stock "
            f"deliverables, so it carries no single unit count"
        )
    units = stock[0].get("deliverableUnits")
    if isinstance(units, bool) or not isinstance(units, int | float):
        raise DeliverableUnreadable(
            f"the {day.isoformat()} deliverableUnits is {units!r}, which is not a number"
        )
    if not isfinite(units) or units <= 0:
        raise DeliverableUnreadable(
            f"the {day.isoformat()} deliverableUnits is {units!r}, which is not a positive "
            f"finite count"
        )

    cash = any(isinstance(item, dict) and item.get("currencyType") is not None for item in parsed)
    symbol = stock[0].get("symbol")
    return Deliverable(
        units=float(units),
        symbol=symbol if isinstance(symbol, str) else None,
        entries=len(parsed),
        cash=cash,
        note_units=_note_units(note),
        multiplier=float(multiplier) if isinstance(multiplier, int | float) else None,
        non_standard=non_standard if isinstance(non_standard, bool) else None,
    )


def _note_units(note: object) -> float | None:
    """The share count ``deliverable_note`` names, or ``None`` when it names no plain one.

    The live lake's note is ``100 SPY`` on every row of both tickers. A note this does not
    match is not guessed at, and the gate then has one number instead of two, which does not
    agree. That is the fail-closed answer rather than a gap: a note carrying cash beside
    shares, or two securities, is the shape the ledger's single float cannot describe either.
    """
    if not isinstance(note, str):
        return None
    match = _NOTE.match(note.strip())
    return float(match.group(1)) if match else None


# -- the gate ----------------------------------------------------------------


def check_split_consistency(prior: Deliverable, new: Deliverable) -> SplitConsistency:
    """Whether the vendor's two spellings of the deliverable produce the same ratio.

    This is the internal validation a split lands through, and it is this module's own rather
    than something inherited. A second vendor was considered and rejected, and the two checks
    that replaced it, the dividend self-consistency rule and the official close against the
    session's own quotes, judge neither a split. The strike ladder against spot is
    :func:`check_strike_scale`, which answers a different question and lands no ratio. So
    corroboration comes from the one other place the deliverable is written down.

    ``computed`` reads the typed ``deliverableUnits``, which is what the ratio itself is
    taken from. ``against`` reads ``deliverable_note``'s share counts, the vendor's free-text
    spelling of the same fact. A drifted or stale payload moves one of the two and leaves the
    other, which is exactly the shape that would put a wrong ratio in a ledger entry while
    looking well-formed.

    Three edges are decided here rather than left to a division, and each one is a way the
    arithmetic stops meaning anything.

    1. A note either side that does not name a plain share count leaves the gate with one
       number, so it has nothing to compare and does not agree.
    2. A note either side that is not a positive finite share count is refused the same way,
       and *both* sides are checked rather than the prior alone. The comparison below divides
       by the note ratio, so a zero on the **new** side is what reaches the division, and a
       guard reading only the prior would let it through. The typed counts cannot reach zero,
       because :func:`_deliverable` already refuses a unit count that is not positive and
       finite, but nothing refuses a free-text note reading ``0 SPY``.
    3. A ratio the typed counts cannot represent raises rather than riding a finding. A
       denormal prior overflows the division to infinity, and ``json.dumps`` writes that into
       the withheld record as a bare ``Infinity`` that no strict JSON reader accepts, which is
       the same hazard ``actions.build_entry`` refuses for the ledger.
    """
    computed = new.units / prior.units
    if not isfinite(computed) or computed <= 0:
        raise DeliverableUnreadable(
            f"{prior.units!r} units before and {new.units!r} after produce no ratio a "
            f"reader can represent"
        )
    if not _usable_note(prior.note_units) or not _usable_note(new.note_units):
        return SplitConsistency(agrees=False, computed=computed, against=None)
    assert prior.note_units is not None and new.note_units is not None
    against = new.note_units / prior.note_units
    if not isfinite(against) or against <= 0:
        return SplitConsistency(agrees=False, computed=computed, against=None)
    difference = abs(computed - against) / abs(against)
    return SplitConsistency(
        agrees=difference <= SPLIT_CONSISTENCY_TOLERANCE,
        computed=computed,
        against=against,
    )


def check_strike_scale(
    previous: Session, session: Session, *, skipped_since: int
) -> ScaleVerdict | str:
    """Whether a whole-ratio split sits between two sessions, or why they cannot be compared.

    **The two signals this module carries are not two views of one event.** The re-symboling
    signal lands an entry only when the deliverable moved, and a whole-ratio split moves no part
    of it. So a 2:1 is invisible to that walk, and everything smaller than a whole ratio is
    already visible to it. What is left over is the whole-ratio family, whose smallest factor is
    2, and that is not a chosen scope. It is the complement of the signal already shipped.

    **One hypothesis, tested from both sides.** A whole-ratio split does two things on one day:
    the OCC divides every open contract's strike by the ratio, and the market marks the
    underlying down by the same ratio. So the spot move names a candidate and the ladder either
    followed it or did not.

    1. ``spot_prev / spot_now`` of 1.5 or more names ``round`` of it, and of ``1 / 1.5`` or less
       names ``round`` of its reciprocal. Between those two gates it names nothing, which is
       where every ordinary session pair sits: the lake's four adjacent pairs run 0.999745 to
       1.006586.
    2. The candidate holds only within :data:`WHOLE_RATIO_TOLERANCE` of the move, relatively.
    3. The confirmation is the fraction of the previous session's ladder whose rescaling by the
       candidate is listed today, and it trips at :data:`SCALE_CONFIRMATION_FLOOR`.

    **The confirmation runs at the candidate, never at the raw move.** An ex-date carries
    overnight drift, so the move is 2.000637 rather than 2, and dividing a rung by it lands
    between rungs while dividing by 2 lands on the rung the OCC created.

    **Ladder survival was measured and refused.** Asking instead what fraction of yesterday's
    rungs is still listed reads 1.000000 on every clean pair and is blind to new listings, which
    is why marketlake #408 carried it for three audit passes. A standard series listing at the
    new scale relists old grid points, and that only pushes survival up: modelled at its worst
    on the live ladders, a 1-for-2 reverse leaves SPY at 0.9793 and a 5:4 at 0.9379, both above
    any usable floor. That is a miss rather than a false alarm. Confirmation cannot fail that
    way, because a fresh series only adds rungs to a number a real adjustment already puts at 1.
    The half that needed survival, a ladder rescaling while spot holds still, is #418's.

    **Pairing on ``ssid`` was refused by ``lake.occ_mapping`` first.** That module says whether
    the vendor carries the identifier through an adjustment cannot be measured here, because the
    lake holds no adjusted contract, and :class:`~lake.occ_mapping.SymbolHistory` fails closed
    rather than guessing. A guard keyed on it would go silent on exactly the boundary it exists
    for. So the ladder is compared by strike.

    **Four reasons refuse the pair rather than judging it**, each returned as a string the way
    :func:`read_session` returns its own. A skipped session between the two ends leaves ladder
    attrition unmeasured across the window, and a finding filed on that guess would never clear,
    since :func:`~lake.report.write_withheld` files a held finding again every night and nothing
    prunes ``reports/``. Two sessions of different instruments are two securities. A session with
    no ladder has no denominator and one with no spot has no ratio to round.
    """
    if previous.instrument_id != session.instrument_id:
        return REASON_INSTRUMENT_CHANGED
    if skipped_since:
        return REASON_SCALE_WINDOW
    if not previous.strikes or not session.strikes:
        return REASON_NO_LADDER
    if previous.spot is None or session.spot is None:
        return REASON_NO_UNDERLYING

    spot_ratio = previous.spot / session.spot
    ratio = _whole_ratio(spot_ratio)
    if ratio is None:
        return ScaleVerdict(ratio=None, spot_ratio=spot_ratio, confirmed=0.0)
    followed = sum(
        1 for strike in previous.strikes if round(strike / ratio, _STRIKE_PLACES) in session.strikes
    )
    return ScaleVerdict(
        ratio=ratio, spot_ratio=spot_ratio, confirmed=followed / len(previous.strikes)
    )


def _whole_ratio(spot_ratio: float) -> float | None:
    """The whole ratio a spot move names, or ``None`` when it names none.

    ``round`` leaves one candidate rather than several bands, so two ratios can never both claim
    one move. A move landing exactly halfway rounds to even and then fails the tolerance either
    way, which is why the tie-breaking rule does not matter here.
    """
    if not isfinite(spot_ratio) or spot_ratio <= 0:
        return None
    if spot_ratio >= WHOLE_RATIO_GATE:
        candidate = float(round(spot_ratio))
    elif spot_ratio <= 1.0 / WHOLE_RATIO_GATE:
        # A subnormal ratio overflows its own reciprocal to infinity, and ``round`` of that
        # raises rather than answering. ``check_split_consistency`` refuses the same hazard by
        # name for the ledger's ratio, and a raise here would end the walk for every ticker it
        # had not reached rather than declining one pair.
        reciprocal = 1.0 / spot_ratio
        if not isfinite(reciprocal):
            return None
        candidate = 1.0 / round(reciprocal)
    else:
        return None
    if abs(spot_ratio - candidate) / candidate > WHOLE_RATIO_TOLERANCE:
        return None
    return candidate


def _usable_note(note_units: float | None) -> bool:
    """Whether a note's share count can be one side of a ratio.

    A note that named no plain share count is ``None`` by now. One that named zero, or a
    figure so long it overflows to infinity, parsed and still cannot be divided by or into.
    """
    return note_units is not None and isfinite(note_units) and note_units > 0


def require_scalar(prior: Deliverable, new: Deliverable) -> None:
    """Refuse an adjustment no single ``split_ratio`` faithfully describes.

    #136 states the constraint. A whole-ratio split maps exactly, because strikes scale by
    the ratio and the contract count absorbs the rest. An uneven split or a special dividend
    changes the deliverable itself, and a contract delivering shares plus cash has no
    multiplier that makes it comparable.

    So the line this draws is what one float can say, and four conditions are what say it.

    1. Neither side carries a cash component. Cash beside shares is #136's own example, and
       it is asked first because it is the most specific thing that can be wrong here.
    2. The deliverables list holds one entry either side. Two entries are two things
       delivered and one number describes neither.
    3. The deliverable names the same security either side. The same count of a different
       security is not a split at all.
    4. The contract multiplier did not move. A ratio scales what the contract delivers, and
       a moved multiplier scales what the contract *is*, which no ``split_ratio`` records.

    **An unrecorded value is refused, and it is refused for what it is.** The last two
    conditions compare a value that can be absent, from a partition sealed before the column
    existed or a vendor row carrying null, and the live lake's own 2026-09-02 partition
    carries null in both. Absent is not equal and it is not different either: nothing says
    the value moved and nothing says it held. Landing on no evidence is the one outcome the
    ledger cannot take back, so it is refused, and the message says the value is not recorded
    rather than claiming a move nobody observed. Two absent values are refused for the same
    reason rather than passing on the strength of comparing equal to each other.

    **What is not here is the vendor's ``non_standard`` flag.** An earlier draft refused a
    gained root whose contracts the vendor still called standard, on the argument that the
    OCC re-symbols only when the adjustment makes a contract non-standard. That is a claim
    about the OCC's concept rather than about Schwab's boolean, which sits in
    ``journal.CHAINS_SCHEMA`` beside ``mini`` and ``penny_pilot`` as a classification flag,
    and the lake holds no adjusted contract to measure it on. As a refusal it would hold a
    clean two-for-one that one float describes perfectly, under a check name saying the
    opposite, and a held split never clears. The flag is read, and it is read where it says
    something the data does not otherwise name: :meth:`Session.standard` uses it to tell a
    newly listed standard series from an adjustment, and :meth:`Session.standard_roots` uses
    it to find the contracts an adjustment was made *from*.
    """
    if prior.cash or new.cash:
        raise NonScalarDeliverable(
            "the deliverable carries cash, which no multiplier makes comparable"
        )
    if prior.entries != 1 or new.entries != 1:
        raise NonScalarDeliverable(
            f"the deliverable holds {prior.entries} entries before and {new.entries} after, "
            f"so no single ratio describes it"
        )
    _require_unmoved(prior.symbol, new.symbol, "the security the deliverable names")
    _require_unmoved(prior.multiplier, new.multiplier, "the contract multiplier")


def _require_unmoved(before: object, after: object, what: str) -> None:
    """Refuse a value that moved, and one the vendor did not record either side.

    The two are refused apart because they are different things to tell an operator. One says
    the vendor wrote down a change no ``split_ratio`` can carry. The other says the vendor
    wrote nothing, so nothing here can say whether it changed.
    """
    if before is None or after is None:
        raise NonScalarDeliverable(
            f"{what} is not recorded on both sides, {before!r} before and {after!r} after, "
            f"so nothing says it did not move"
        )
    if before != after:
        raise NonScalarDeliverable(
            f"{what} moved from {before!r} to {after!r}, which no split_ratio records"
        )


# -- the walk ----------------------------------------------------------------


def detect_splits(*, lake_root: Path | str, clock: Clock) -> SplitReport:
    """Read every sealed chains ticker-day, gate what it finds, and append what lands.

    Nothing here fetches. ``CHAINS_SCHEMA`` has carried ``option_root`` and the four
    deliverable columns since the capture schema was pinned, so the evidence a split is
    derived from is already on disk. Every dependency is injected and this reads no config,
    the way ``actions.extract_dividends`` does.

    The walk, per ticker, in date order.

    1. A ticker-day the master places outside the instrument's capture span is skipped.
       ``capture_spans.py`` has already decided that such a day is out of scope, never a gap,
       so it is not a finding. This is what the live lake's SPY 2026-09-02 partition is: it
       predates the master's 2026-09-08 ``capture_start``, so it resolves to no instrument,
       and ``UnresolvedSymbol``'s docstring would otherwise call it a reference-data fault
       that is not there. The master alone tells the two apart. A symbol it knows but has no
       mapping valid for on that day is out of scope. A symbol it does not carry at all is
       the fault the exception describes.
    2. A symbol the master does not carry is filed once per ticker rather than once per
       ticker-day. No day of that ticker will resolve, so the condition has one action behind
       it, which is the same reason ``by_ticker`` groups on the ticker and lets the
       instrument enter one level down.
    3. A session the walk cannot read is skipped, for the four reasons
       :func:`read_session` names. Each skip widens the window a boundary can sit inside.
    4. A session whose root set holds a root the previous readable session lacked is a
       boundary. "The previous session" means the previous one the walk did not skip.
    5. A boundary with a skipped session between its two ends is held rather than landed.
       ``ex_date`` sits in the ledger's key, so a date the detector gets wrong cannot be
       superseded, and a corrected entry lands under a second key that every adjusted price
       then applies on top of the first.
    6. The deliverable is read either side. The prior side is the whole previous session and
       the new side is the gained roots alone, because an adjustment re-symbols the open
       contracts while newly listed standard ones keep the original root.
    7. A deliverable that did not move is a rename rather than a split, and it appends
       nothing and holds nothing. ``SecurityMaster.remap`` says a ticker rename and an OCC
       re-symboling are the same operation over different identifier kinds, so the
       deliverable is the only thing that separates them. ``actions.append`` would take a
       ``split_ratio`` of ``1.0`` without complaint, and every adjusted view would then read
       a no-op factor as a real corporate action.
    8. An adjustment no single float describes is held, per :func:`require_scalar`.
    9. The gate runs, and a disagreement holds the split out and files it. So does a payload
       the ledger's own record rules refuse, rather than ending the run as a traceback.
    10. The entry lands only when it differs from what ``latest`` already resolves on its
        key, on every field but ``recorded_at``. A split stays visible in sealed chains
        forever, so without this the ledger would grow by a line every night.

    **Both ways the resolution can fail hold the action and file it.** ``UnresolvedSymbol``
    says the master and the lake disagree about a ticker. ``AmbiguousSymbol`` says the master
    is corrupt. Either way an action held out can be landed later, while one landed under the
    wrong instrument corrupts every factor that instrument's prices feed.

    **An absent or torn master stops the run instead.** That is one condition a single command
    fixes, and holding it per ticker-day would file one finding per ticker-day for it.

    **A finding that cannot be written down does not stop the run either.** It is carried on
    the report as unfiled and the command turns that into an exit code, which is the
    containment ``write_withheld`` says belongs to its caller.

    **The scale guard runs on each adjacent pair, after the examination and suppressed by it.**
    :func:`check_strike_scale` answers the split this walk structurally cannot see, so it fires
    only where this walk produced nothing: no entry landed, none matched what ``latest``
    resolves, ``_examine`` filed no finding, and the ledger holds no split on that key already.
    The last of the four is what lets marketlake #286's manual entry clear it, since such an
    entry never reaches ``_examine`` at all.
    """
    lake_root = Path(lake_root)
    master = read_master(lake_root)
    recorded_at = clock.now()
    # Read once for the run, so every ticker-day is compared against one snapshot of what the
    # ledger already holds, the way the dividend extraction reads it.
    current = latest(lake_root)

    ticker_days = surface_ticker_days(lake_root, CHAINS)
    appended: list[Landed] = []
    held: list[HeldFinding] = []
    skipped: list[Skip] = []
    not_adjustments: list[NotAnAdjustment] = []
    mapped: list[Remapped] = []
    scale_unread: list[ScaleUnread] = []
    unchanged = 0
    scale_pairs = 0
    scale_covered = 0
    # Every key this run has already emitted. One ticker has at most one boundary a day, so
    # this cannot collide today. It is still read, because two tickers resolving to one
    # instrument would otherwise emit one key twice and neither line would match what
    # ``latest`` resolves.
    emitted: set[ActionKey] = set()

    def hold(finding: Withheld) -> None:
        # The sequence is the caller's, for the reason ``report`` gives: it has only module
        # functions, so a counter there would be module state no test could drive.
        try:
            filed_at = write_withheld(lake_root, finding, now=recorded_at, sequence=len(held))
        except OSError as exc:
            # Named on stderr and carried on the report. The walk goes on, because one
            # unwritable file is not the other tickers' splits to lose.
            print(
                f"splits: {finding.symbol} {finding.observed_on.isoformat()} "
                f"{finding.check} could not be filed: {type(exc).__name__}",
                file=sys.stderr,
            )
            held.append(
                HeldFinding(finding=finding, filed_at=None, filing_error=type(exc).__name__)
            )
            return
        held.append(HeldFinding(finding=finding, filed_at=filed_at))

    for ticker, days in by_ticker(ticker_days):
        previous: Session | None = None
        # Each contract's current symbol and the session it was first read under it. This is
        # what pairs a re-symboled contract's old symbol to its new one, and what dates the
        # old mapping's range. It is per instrument and resets with ``previous``.
        history = SymbolHistory()
        # Every root the walk has watched this instrument carry. A set difference has no
        # direction, so without this a root that expires out of one session and lists again
        # in the next reads as an adjustment and lands the ratio backwards.
        seen: frozenset[str] = frozenset()
        # How many of this ticker's sealed sessions the walk has skipped since ``previous``.
        # A boundary is only as narrow as this is zero.
        skipped_since = 0
        for day in days:
            try:
                instrument_id = resolve_instrument(master, ticker, day)
            except UnresolvedSymbol as exc:
                if _in_master(master, ticker):
                    # Known symbol, no mapping valid that day. Out of scope, never a gap.
                    skipped.append(Skip(ticker, day, REASON_OUT_OF_SCOPE))
                    skipped_since += 1
                    continue
                hold(_resolution_finding(ticker, day, exc))
                skipped.append(Skip(ticker, day, REASON_UNRESOLVED))
                # No day of this ticker will resolve, so the rest of it is one finding's
                # worth of condition rather than one per ticker-day.
                break
            except AmbiguousSymbol as exc:
                hold(_resolution_finding(ticker, day, exc, instrument_ids=exc.instrument_ids))
                skipped.append(Skip(ticker, day, REASON_UNRESOLVED))
                break

            session = read_session(lake_root, ticker, day, instrument_id)
            if isinstance(session, str):
                skipped.append(Skip(ticker, day, session))
                skipped_since += 1
                continue

            # Read before the examination so the scale guard below can tell whether that
            # examination filed anything for this session. Its own findings go through ``hold``
            # rather than riding on ``Outcome``, so the list's length is what says so.
            filed_before = len(held)
            outcome = _examine(
                ticker=ticker,
                previous=previous,
                seen=seen,
                session=session,
                skipped_since=skipped_since,
                recorded_at=recorded_at,
                lake_root=lake_root,
                current=current,
                emitted=emitted,
                history=history,
                hold=hold,
            )
            if outcome.landed is not None:
                appended.append(outcome.landed)
            unchanged += outcome.unchanged
            not_adjustments.extend(outcome.not_adjustments)
            mapped.extend(outcome.mapped)

            # **After the examination, and suppressed by whatever it produced.** On an uneven
            # adjustment both signals can fire at once, and a held finding never clears, so a
            # correctly landed 3-for-2 would otherwise file the same line every night forever
            # for an event the ledger already holds. The ledger is asked directly too, through
            # the snapshot this run already read: a manual entry never passes through
            # ``_examine`` at all, because no root appeared, so without that read marketlake
            # #286's entry could not clear this finding either.
            if previous is not None:
                scale = check_strike_scale(previous, session, skipped_since=skipped_since)
                if isinstance(scale, str):
                    scale_unread.append(ScaleUnread(ticker, day, scale))
                else:
                    scale_pairs += 1
                    if scale.holds:
                        # What ``_examine`` filed, rather than whether it filed at all. A
                        # payload finding about some other root's unreadable row is not an
                        # answer to this question, and treating it as one hides a real split
                        # behind it for as long as the unreadable row survives.
                        examined = {
                            entry.finding.check for entry in held[filed_before:]
                        } & BOUNDARY_CHECKS
                        # ``outcome.unchanged`` is deliberately not a fifth term. It is true
                        # only where ``same_but_for_recorded_at`` matched an entry, which
                        # refuses a ``None``, so it already implies the ledger read below on
                        # the same key. The review that found this proved the term could not
                        # change an answer, and a condition nothing can reach reads as a rule.
                        recorded = (
                            outcome.landed is not None
                            or bool(examined)
                            # ``normalize_date`` renders the key's date, so the ledger's key
                            # carries it as text and a ``date`` here would never match.
                            or (session.instrument_id, day.isoformat(), TYPE_SPLIT) in current
                        )
                        if recorded:
                            scale_covered += 1
                        else:
                            hold(
                                Withheld(
                                    symbol=ticker,
                                    observed_on=day,
                                    event=TYPE_SPLIT,
                                    check=CHECK_STRIKE_SCALE,
                                    computed=scale.ratio,
                                    against=scale.spot_ratio,
                                    instrument_id=session.instrument_id,
                                )
                            )
            # The instrument's own history, so a root is remembered across a session it
            # happens to be absent from. It resets with ``previous`` when the instrument
            # changes, because a different security's roots are a different history. The
            # symbol history resets with it, for the same reason.
            if previous is not None and previous.instrument_id != session.instrument_id:
                seen = frozenset()
                history.reset()
            seen |= session.roots
            # After the examination rather than before it, so a boundary is judged against
            # the history as it stood before this session, the way ``seen`` is.
            history.observe(session.day, [row for _, row in session.rows])
            previous, skipped_since = session, 0

    return SplitReport(
        ticker_days=len(ticker_days),
        appended=tuple(appended),
        held=tuple(held),
        unchanged=unchanged,
        not_adjustments=tuple(not_adjustments),
        skipped=tuple(skipped),
        mapped=tuple(mapped),
        scale_pairs=scale_pairs,
        scale_covered=scale_covered,
        scale_unread=tuple(scale_unread),
    )


def _examine(
    *,
    ticker: str,
    previous: Session | None,
    seen: frozenset[str],
    session: Session,
    skipped_since: int,
    recorded_at: datetime,
    lake_root: Path,
    current: dict[ActionKey, dict],
    emitted: set[ActionKey],
    history: SymbolHistory,
    hold,
) -> Outcome:
    """One session against the one before it, and what became of any boundary in it.

    Split out of the walk because the walk's own job is the skipping and the grouping, and
    because what a boundary *is* is the part with the findings in it.
    """
    if previous is None or previous.instrument_id != session.instrument_id:
        # A symbol handed from one instrument to another is not a boundary, it is a new thing
        # to record. The two sessions describe different securities, so their root sets are
        # not comparable and the incoming one starts fresh.
        return Outcome()

    appeared = session.roots - previous.roots
    if not appeared:
        return Outcome()

    # **A root the walk has already watched this instrument carry is not a gain.** The set
    # difference alone has no direction: a root whose contracts all expired out of one
    # session and list again in the next reads as an adjustment, and the ratio is then
    # computed backwards, landing a phantom inverse split under a date the key cannot
    # correct. Every root the walk has seen is remembered for exactly this, and a root
    # returning is counted rather than passed over in silence.
    returned = appeared & seen
    gained = appeared - seen
    marks = [NotAnAdjustment(ticker, session.day, REASON_ROOT_RETURNED) for _ in sorted(returned)]
    if not gained:
        return Outcome(not_adjustments=tuple(marks))

    day = session.day
    if any(not root for root in gained):
        hold(
            _finding(
                ticker,
                day,
                CHECK_SPLIT_PAYLOAD,
                DeliverableUnreadable(
                    f"{day.isoformat()} holds contracts naming no root at all, under neither "
                    f"{OPTION_ROOT} nor {OCC_SYMBOL}"
                ),
                session.instrument_id,
            )
        )
        return Outcome(not_adjustments=tuple(marks))

    # **A gained root whose contracts are standard is a newly listed series, not an
    # adjustment.** An OCC adjustment turns standard contracts into non-standard ones, so a
    # chain that begins listing a fresh standard series under a second root has gained a root
    # and had no corporate action. That shape is the ordinary state of a ticker in the weeks
    # after an adjustment, when a new standard series lists beside the adjusted one, and
    # reading it as a boundary lands the first split's ratio inverted.
    standard = session.standard(gained)
    if standard is False:
        marks.append(NotAnAdjustment(ticker, day, REASON_STANDARD_SERIES))
        return Outcome(not_adjustments=tuple(marks))
    if standard is None:
        # Unknown is read as neither. Without the flag nothing here separates a new series
        # from an adjustment, and the two want opposite treatments, so this fails closed the
        # way every other unanswerable question in this module does.
        hold(
            _finding(
                ticker,
                day,
                CHECK_SPLIT_PAYLOAD,
                DeliverableUnreadable(
                    f"the {day.isoformat()} contracts under {sorted(gained)} do not say "
                    f"whether they are standard, so nothing separates an adjustment from a "
                    f"newly listed series"
                ),
                session.instrument_id,
            )
        )
        return Outcome(not_adjustments=tuple(marks))

    try:
        if skipped_since:
            raise BoundaryUnbounded(
                f"{ticker} gained {sorted(gained)} on {day.isoformat()} and "
                f"{skipped_since} session(s) since {previous.day.isoformat()} were skipped, "
                f"so the boundary's own date is not bounded to one session"
            )
        prior = _prior_deliverable(previous, session, gained)
        new = deliverable_of(session, gained)
        verdict = check_split_consistency(prior, new)
    except BoundaryUnbounded as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_BOUNDARY, exc, session.instrument_id))
        return Outcome(not_adjustments=tuple(marks))
    except DeliverableUnreadable as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_PAYLOAD, exc, session.instrument_id))
        return Outcome(not_adjustments=tuple(marks))

    # **The mapping is written here, before the ledger decides anything.** Everything past
    # this point is a confirmed re-symboling: the session gained a root the instrument has
    # never carried, the vendor calls its contracts non-standard, the boundary is bounded to
    # one session, and both sides of the deliverable read. What the ledger does with it
    # differs after this and the master's answer does not, because the master records which
    # contract is which and records no ratio at all. A rename, an adjustment one float cannot
    # describe, a ratio the gate refuses and a landed split all re-symboled the contracts.
    mapped: tuple[Remapped, ...] = ()
    try:
        mapped = write_mappings(
            lake_root,
            ticker=ticker,
            instrument_id=session.instrument_id,
            effective=day,
            pairing=history.inspect(row for root, row in session.rows if root in gained),
            recorded_at=recorded_at,
        )
    except (MasterAbsent, MasterUnreadable):
        # The one condition a single command fixes, and it stays true for every remaining
        # boundary, so it ends the run the way the walk's own first read of the master does.
        # ``main`` turns each into its own line naming the fix.
        raise
    except (MappingError, SecurityMasterError, ValueError, OSError) as exc:
        # Four classes rather than two. ``MappingError`` is this module's own, and it carries
        # the manifest failure too: ``record_partition`` raises a bare ``RowCountRegression``
        # that a catch on the master's errors would miss, and rather than list that class
        # here, where nothing could ever reach it, the write wraps it in
        # ``ManifestNotRecorded``. That is not tidying. The two say opposite things to an
        # operator, because the rows are on disk and only the lake's record of them is stale.
        # A refusal here costs this boundary its mapping and not the run, since the ledger
        # entry below is a separate record.
        hold(_finding(ticker, day, CHECK_OCC_MAPPING, exc, session.instrument_id))

    if new.same_as(prior):
        # A rename carries the same deliverable under a new symbol. Nothing to land and
        # nothing to hold, the way a quote row carrying no ex-date is no observation.
        marks.append(NotAnAdjustment(ticker, day, REASON_DELIVERABLE_UNCHANGED))
        return Outcome(not_adjustments=tuple(marks), mapped=mapped)

    try:
        require_scalar(prior, new)
    except NonScalarDeliverable as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_DELIVERABLE, exc, session.instrument_id))
        return Outcome(not_adjustments=tuple(marks), mapped=mapped)

    if not verdict.agrees:
        hold(
            Withheld(
                symbol=ticker,
                observed_on=day,
                event=TYPE_SPLIT,
                check=CHECK_SPLIT_CONSISTENCY,
                computed=verdict.computed,
                against=verdict.against,
                instrument_id=session.instrument_id,
            )
        )
        return Outcome(not_adjustments=tuple(marks), mapped=mapped)

    fields = {
        "instrument_id": session.instrument_id,
        # Both dates are the boundary session, and they cannot differ. A split detected from
        # a root change has no vendor date at all, so the boundary is the only honest answer
        # for either, and a detector stamping the night it ran would append the same split
        # every night forever.
        "observed_on": day,
        "ex_date": day,
        "recorded_at": recorded_at,
        "type": TYPE_SPLIT,
        # A split pays nothing and Schwab's fundamentals carry no announcement date for one,
        # which is the convention ``actions.append``'s docstring fixes for this module.
        "pay_date": None,
        "declared_date": None,
        "split_ratio": verdict.computed,
        # Only ``observed`` is reachable here. ``vendor_reported`` means the value was already
        # there on the first observation, and a root already adjusted when the lake first saw
        # a ticker has no prior to compare against, so it is not detected at all rather than
        # detected and labelled.
        "provenance": PROVENANCE_OBSERVED,
    }
    try:
        candidate = build_entry(**fields)
    except ValueError as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_PAYLOAD, exc, session.instrument_id))
        return Outcome(not_adjustments=tuple(marks), mapped=mapped)

    key = (session.instrument_id, candidate["ex_date"], TYPE_SPLIT)
    if key in emitted:
        # Two tickers resolving to one instrument, which the master calls corrupt. The second
        # boundary is refused rather than passed over, because the two can disagree about the
        # ratio and a run that dropped one in silence would report a ledger it does not
        # describe.
        hold(
            _finding(
                ticker,
                day,
                CHECK_INSTRUMENT_RESOLUTION,
                DeliverableUnreadable(
                    f"instrument {session.instrument_id} already had a split recorded on "
                    f"{candidate['ex_date']} in this run, under another ticker"
                ),
                session.instrument_id,
            )
        )
        return Outcome(not_adjustments=tuple(marks), mapped=mapped)
    emitted.add(key)
    if same_but_for_recorded_at(current.get(key), candidate):
        return Outcome(unchanged=True, not_adjustments=tuple(marks), mapped=mapped)
    return Outcome(
        landed=Landed(entry=append(lake_root, **fields), symbol=ticker),
        not_adjustments=tuple(marks),
        mapped=mapped,
    )


def _prior_deliverable(previous: Session, session: Session, gained: frozenset[str]) -> Deliverable:
    """What a contract delivered before the adjustment, read from the standard series.

    The prior side is the contracts the adjustment was made *from*, and an OCC adjustment
    turns standard contracts into non-standard ones, so the standard series is what names
    them. Three places are asked in order, and the first that answers wins.

    1. The standard contracts of the boundary session itself, which is #279's own
       prescription: "A contract still naming 100 units beside one naming another number is
       the ratio." An adjustment re-symbols the open contracts while newly listed standard
       ones keep the original root, so both sides of the ratio usually sit in one session.
    2. The standard contracts of the previous session, for the boundary where every contract
       was re-symboled at once and the session carries no standard series at all.
    3. The previous session whole, for a partition that records no ``non_standard`` flag to
       select on. Reading it whole is what fails when that session carries two roots with
       different deliverables, which is the ordinary state after any adjustment, and
       :func:`deliverable_of` then refuses rather than picking one.
    """
    carried = session.standard_roots() - gained
    if carried:
        return deliverable_of(session, carried)
    standard = previous.standard_roots()
    if standard:
        return deliverable_of(previous, standard)
    return deliverable_of(previous, previous.roots)


def _in_master(master: SecurityMaster, symbol: str) -> bool:
    """Whether the master carries this symbol at all, on any date and under any kind.

    This is what tells a ticker-day out of scope from a reference-data fault.
    ``SecurityMaster.resolve`` answers ``None`` to both, and the two want opposite
    treatments: one is skipped in silence and the other is a finding an operator reads.
    """
    return any(mapping.id_value == symbol for mapping in master.mappings)


def _finding(
    ticker: str, day: date, check: str, exc: Exception, instrument_id: int | None
) -> Withheld:
    """The finding one of this module's own refusals files.

    The exception is rendered as its class and then its message, because ``write_withheld``
    composes ``<symbol>: <exception>`` and keeps the first two fields. Handing over the bare
    message would file that message's own first field instead, which for an ``OSError`` is a
    path on the capture machine. The run's own render prints the whole string, so the message
    reaches the operator there and the class alone reaches the file.
    """
    return Withheld(
        symbol=ticker,
        observed_on=day,
        event=TYPE_SPLIT,
        check=check,
        instrument_id=instrument_id,
        exception=f"{type(exc).__name__}: {exc}",
    )


def _resolution_finding(
    ticker: str, day: date, exc: Exception, *, instrument_ids: Sequence[int] = ()
) -> Withheld:
    """The finding a resolution failure files, under the check name ``lake.actions`` fixed.

    ``CHECK_INSTRUMENT_RESOLUTION`` is reused rather than respelled. A master that cannot
    place a symbol says the same thing whichever walk met it, and the two other check names
    in ``lake.actions`` judge a dividend payload and cannot judge a split.
    """
    return Withheld(
        symbol=ticker,
        observed_on=day,
        event=TYPE_SPLIT,
        check=CHECK_INSTRUMENT_RESOLUTION,
        instrument_ids=tuple(instrument_ids),
        exception=f"{type(exc).__name__}: {exc}",
    )


# -- the entry point ---------------------------------------------------------


def detect_splits_from_config(
    *, clock: Clock | None = None, config_path: str | Path | None = None
) -> SplitReport:
    """The detection wired from the real config. This is what the CLI subcommand calls."""
    from lake.config import load_config

    config = load_config(config_path)
    return detect_splits(
        lake_root=config.lake_root, clock=SystemClock() if clock is None else clock
    )


__all__ = [
    "BoundaryUnbounded",
    "CHAINS_COLUMNS",
    "CHECK_SPLIT_BOUNDARY",
    "CHECK_SPLIT_CONSISTENCY",
    "CHECK_SPLIT_DELIVERABLE",
    "CHECK_SPLIT_PAYLOAD",
    "CHECK_STRIKE_SCALE",
    "Deliverable",
    "DeliverableUnreadable",
    "NonScalarDeliverable",
    "NotAnAdjustment",
    "Outcome",
    "REASON_DELIVERABLE_UNCHANGED",
    "REASON_INSTRUMENT_CHANGED",
    "REASON_NO_LADDER",
    "REASON_NO_OPTION_CLOSE",
    "REASON_NO_UNDERLYING",
    "REASON_OUT_OF_SCOPE",
    "REASON_PARTIAL_READ",
    "REASON_PARTITION_ABSENT",
    "REASON_QUARANTINED",
    "REASON_ROOT_RETURNED",
    "REASON_SCALE_WINDOW",
    "REASON_STANDARD_SERIES",
    "REASON_THIN",
    "REASON_UNRESOLVED",
    "SCALE_CONFIRMATION_FLOOR",
    "SPLIT_CONSISTENCY_TOLERANCE",
    "SSID",
    "STRIKE_PRICE",
    "ScaleUnread",
    "ScaleVerdict",
    "Session",
    "Skip",
    "SplitConsistency",
    "SplitError",
    "SplitReport",
    "UNDERLYING_PRICE",
    "WHOLE_RATIO_GATE",
    "WHOLE_RATIO_TOLERANCE",
    "check_split_consistency",
    "check_strike_scale",
    "deliverable_of",
    "deliverable_of_row",
    "detect_splits",
    "detect_splits_from_config",
    "read_session",
    "require_scalar",
]
