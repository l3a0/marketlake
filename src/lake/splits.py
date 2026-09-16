"""The split detector: reading an OCC re-symboling out of sealed chains.

A split is the corporate action that changes every strike in a chain, and it announces
itself in data the lake already holds. Recording it needs no vendor call, only a read over
sealed chains. Nothing here fetches.

``docs/design.md`` names two split signals in one sentence. The strike-vs-spot scale guard
trips, and the OCC re-symbols the chain. The guard belongs to the validation battery, which
is marketlake #138, and this module takes the other signal.

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

**The gate compares the vendor against itself.** No gate existed to inherit. A second vendor
was considered and rejected, and ``docs/design.md`` pins the cut. What replaced it is the
lake's own second observation, and the strike ladder against spot is #138's rather than this
module's. So corroboration here comes from the one other place the deliverable is written
down: ``deliverable_note``, the vendor's free-text spelling of the same fact. The ratio the
typed ``deliverableUnits`` produces has to agree with the ratio the note's share counts
produce. That is the shape ``actions.check_dividend_consistency`` already has, where the
vendor's annualized figure is read against its own per-event amount, and it catches the same
class of defect: one of two fields carrying an adjustment the other does not.

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
   made across it would be a comparison against contents nobody saw in full.
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
    TYPE_SPLIT,
    ActionKey,
    HeldFinding,
    Landed,
    UnresolvedSymbol,
    append,
    build_entry,
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
    PartitionQuarantined,
    _occ_root,
    load_chain,
)
from lake.paths import CHAINS
from lake.report import Withheld, write_withheld
from lake.security_master import AmbiguousSymbol, SecurityMaster

# The eight chains columns this module reads. ``option_root`` is the signal and
# ``occ_symbol`` is the fallback the root is derived from where the column is null. The four
# in the middle are where the deliverable is written down. ``suspect`` and
# ``is_chain_truncated`` are what say a session cannot bound a boundary.
OPTION_ROOT = "option_root"
OCC_SYMBOL = "occ_symbol"
OPTION_DELIVERABLES_LIST = "option_deliverables_list"
DELIVERABLE_NOTE = "deliverable_note"
MULTIPLIER = "multiplier"
NON_STANDARD = "non_standard"
SUSPECT = "suspect"
IS_CHAIN_TRUNCATED = "is_chain_truncated"
CHAINS_COLUMNS = (
    OPTION_ROOT,
    OCC_SYMBOL,
    OPTION_DELIVERABLES_LIST,
    DELIVERABLE_NOTE,
    MULTIPLIER,
    NON_STANDARD,
    SUSPECT,
    IS_CHAIN_TRUNCATED,
)

# The three gates this module files a refusal under. They name what refused rather than what
# was refused, the way ``lake.actions``'s do, because the finding already carries the event.
# ``CHECK_INSTRUMENT_RESOLUTION`` is reused from there rather than respelled, since a master
# that cannot place a symbol says the same thing whichever walk met it.
CHECK_SPLIT_CONSISTENCY = "split_consistency"
CHECK_SPLIT_DELIVERABLE = "split_deliverable"
CHECK_SPLIT_BOUNDARY = "split_boundary"
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
REASON_NO_OPTION_CLOSE = "no option close"
REASON_QUARANTINED = "quarantined"
REASON_PARTIAL_READ = "partial read"
REASON_OUT_OF_SCOPE = "outside the capture span"
REASON_THIN = "suspect or truncated"
REASON_UNRESOLVED = "unresolved symbol"


@dataclass(frozen=True)
class Skip:
    """One ticker-day the walk did not read, and why."""

    ticker: str
    day: date
    reason: str


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
        from a split. Every field is compared rather than ``units`` alone, because a note
        that moved while the typed count did not is a vendor contradiction rather than a
        rename, and it belongs at the gate below instead of being called a non-event here.
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
    it is what makes a second run legible. ``unchanged_deliverable`` counts the root changes
    whose deliverable did not move, which is a rename rather than a split. Those append
    nothing and hold nothing, so without a counter a run that met one would read exactly like
    a run that met nothing at all.

    ``skipped`` carries every ticker-day the walk did not read, because each one widens the
    window a boundary can sit in and the render is where an operator sees how wide the lake's
    windows currently are.
    """

    ticker_days: int
    appended: tuple[Landed, ...]
    held: tuple[HeldFinding, ...]
    unchanged: int
    unchanged_deliverable: int
    skipped: tuple[Skip, ...]

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
        lines.append(f"  unchanged: {self.unchanged}")
        lines.append(f"  deliverable unchanged: {self.unchanged_deliverable}")
        lines.append(f"  skipped:   {len(self.skipped)}")
        for reason in sorted({skip.reason for skip in self.skipped}):
            count = sum(1 for skip in self.skipped if skip.reason == reason)
            lines.append(f"    - {reason}: {count}")
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
    """

    day: date
    instrument_id: int
    roots: frozenset[str]
    rows: tuple[tuple[str, dict[str, object]], ...]


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

    Enumerating from the manifest bounds what absence can look like. Every partition the walk
    names exists, so ``PartitionAbsent`` cannot occur. Three refusals remain and each returns
    a reason rather than raising, because each is one session the walk cannot use rather than
    a run that has to end. ``lake.oi`` is the precedent for the quarantine one: it catches
    ``PartitionQuarantined`` in two places and turns it into an absence verdict that keeps
    its own reason rather than dropping it.

    A thin snapshot is the fourth refusal and it is not an absence either. A response far
    under its trailing-median contract count is journaled and tagged rather than discarded,
    and a thin chain carries a thin root set, so it cannot bound a boundary.
    """
    try:
        table = load_chain(ticker, day, lake_root=lake_root)
    except NoOptionClose:
        return REASON_NO_OPTION_CLOSE
    except PartitionQuarantined:
        return REASON_QUARANTINED
    except PartialRead:
        return REASON_PARTIAL_READ

    columns = {name: _column(table, name) for name in CHAINS_COLUMNS}
    if any(columns[SUSPECT]) or any(columns[IS_CHAIN_TRUNCATED]):
        return REASON_THIN

    rows: list[tuple[str, dict[str, object]]] = []
    for index in range(table.num_rows):
        row = {name: columns[name][index] for name in CHAINS_COLUMNS}
        rows.append((_root_of(row), row))
    return Session(
        day=day,
        instrument_id=instrument_id,
        roots=frozenset(root for root, _ in rows),
        rows=tuple(rows),
    )


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
    session's own quotes, judge neither a split. The strike ladder against spot is #138's. So
    corroboration comes from the one other place the deliverable is written down.

    ``computed`` reads the typed ``deliverableUnits``, which is what the ratio itself is
    taken from. ``against`` reads ``deliverable_note``'s share counts, the vendor's free-text
    spelling of the same fact. A drifted or stale payload moves one of the two and leaves the
    other, which is exactly the shape that would put a wrong ratio in a ledger entry while
    looking well-formed.

    Two edges are decided here rather than left to a division.

    1. A note either side that does not name a plain share count leaves the gate with one
       number, so it has nothing to compare and does not agree.
    2. A prior note of zero has no relative scale and no ratio, so it does not agree either.
       The typed side cannot reach zero, because :func:`_deliverable` already refuses a unit
       count that is not positive and finite.
    """
    computed = new.units / prior.units
    if prior.note_units is None or new.note_units is None or prior.note_units <= 0:
        return SplitConsistency(agrees=False, computed=computed, against=None)
    against = new.note_units / prior.note_units
    difference = abs(computed - against) / abs(against)
    return SplitConsistency(
        agrees=difference <= SPLIT_CONSISTENCY_TOLERANCE,
        computed=computed,
        against=against,
    )


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

    A fifth reads ``non_standard`` and refuses only the vendor contradicting itself. The OCC
    re-symbols when the adjustment makes the contract non-standard, so a gained root whose
    contracts the vendor still flags standard, while their deliverable moved, is two vendor
    fields disagreeing rather than a split. A null flag is unknown rather than false and
    passes, which is what keeps a partition sealed before the column existed from being
    refused for a column it never carried.
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
    if prior.symbol != new.symbol:
        raise NonScalarDeliverable(
            f"the deliverable names {prior.symbol!r} before and {new.symbol!r} after, "
            f"so it is not the same security scaled"
        )
    if prior.multiplier != new.multiplier:
        raise NonScalarDeliverable(
            f"the contract multiplier moved from {prior.multiplier!r} to {new.multiplier!r}, "
            f"which no split_ratio records"
        )
    if new.non_standard is False:
        raise NonScalarDeliverable(
            "the vendor flags the re-symboled contracts standard while their deliverable "
            "moved, so the two fields disagree"
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
    unchanged = 0
    unchanged_deliverable = 0
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

            landed = _examine(
                ticker=ticker,
                previous=previous,
                session=session,
                skipped_since=skipped_since,
                recorded_at=recorded_at,
                lake_root=lake_root,
                current=current,
                emitted=emitted,
                hold=hold,
            )
            if landed is _RENAME:
                unchanged_deliverable += 1
            elif landed is _UNCHANGED:
                unchanged += 1
            elif isinstance(landed, Landed):
                appended.append(landed)
            previous, skipped_since = session, 0

    return SplitReport(
        ticker_days=len(ticker_days),
        appended=tuple(appended),
        held=tuple(held),
        unchanged=unchanged,
        unchanged_deliverable=unchanged_deliverable,
        skipped=tuple(skipped),
    )


# What :func:`_examine` says when a boundary resolved to something other than an appended
# entry. Sentinels rather than booleans, because the caller counts three outcomes apart and a
# boolean would need two of them.
_RENAME = object()
_UNCHANGED = object()


def _examine(
    *,
    ticker: str,
    previous: Session | None,
    session: Session,
    skipped_since: int,
    recorded_at: datetime,
    lake_root: Path,
    current: dict[ActionKey, dict],
    emitted: set[ActionKey],
    hold,
) -> object:
    """One session against the one before it. Returns what became of the boundary, if any.

    Split out of the walk because the walk's own job is the skipping and the grouping, and
    because what a boundary *is* is the part with the findings in it.
    """
    if previous is None:
        return None
    # A symbol handed from one instrument to another is not a boundary, it is a new thing to
    # record. The two sessions describe different instruments, so their root sets are not
    # comparable and the incoming one starts fresh.
    if previous.instrument_id != session.instrument_id:
        return None

    gained = session.roots - previous.roots
    if not gained:
        return None

    day = session.day
    try:
        if skipped_since:
            raise BoundaryUnbounded(
                f"{ticker} gained {sorted(gained)} on {day.isoformat()} and "
                f"{skipped_since} session(s) since {previous.day.isoformat()} were skipped, "
                f"so the boundary's own date is not bounded to one session"
            )
        prior = deliverable_of(previous, previous.roots)
        new = deliverable_of(session, gained)
    except BoundaryUnbounded as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_BOUNDARY, exc, session.instrument_id))
        return None
    except DeliverableUnreadable as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_PAYLOAD, exc, session.instrument_id))
        return None

    if new.same_as(prior):
        # A rename carries the same deliverable under a new symbol. Nothing to land and
        # nothing to hold, the way a quote row carrying no ex-date is no observation.
        return _RENAME

    try:
        require_scalar(prior, new)
    except NonScalarDeliverable as exc:
        hold(_finding(ticker, day, CHECK_SPLIT_DELIVERABLE, exc, session.instrument_id))
        return None

    verdict = check_split_consistency(prior, new)
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
        return None

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
        return None

    key = (session.instrument_id, candidate["ex_date"], TYPE_SPLIT)
    if key in emitted:
        return None
    emitted.add(key)
    if same_but_for_recorded_at(current.get(key), candidate):
        return _UNCHANGED
    return Landed(entry=append(lake_root, **fields), symbol=ticker)


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
    "CHAINS_COLUMNS",
    "CHECK_SPLIT_BOUNDARY",
    "CHECK_SPLIT_CONSISTENCY",
    "CHECK_SPLIT_DELIVERABLE",
    "CHECK_SPLIT_PAYLOAD",
    "SPLIT_CONSISTENCY_TOLERANCE",
    "BoundaryUnbounded",
    "Deliverable",
    "DeliverableUnreadable",
    "NonScalarDeliverable",
    "Session",
    "Skip",
    "SplitConsistency",
    "SplitError",
    "SplitReport",
    "check_split_consistency",
    "deliverable_of",
    "detect_splits",
    "detect_splits_from_config",
    "read_session",
    "require_scalar",
]
