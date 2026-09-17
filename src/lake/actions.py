"""The corporate-actions ledger.

Every price the lake stores is as-traded, so a split or a dividend has to be carried
beside the prices rather than folded into them. This ledger is where each one lands. It
is the store the dividend extraction, the split detector, and every adjusted view read,
so its shape decides what all three can say.

The ledger lives at ``actions/corporate_actions.jsonl``. It follows the manifest's own
line rules, and the quarantine ledger is the closer precedent, down to a human tool
writing beside a nightly job.

1. *One entry is one line*, appended with a single ``O_APPEND`` write. A reader that
   meets a torn trailing line discards it, because a crash can only tear the last line.
   A fragment left by a crash costs more than itself, because the read stops at the first
   line it cannot parse and the next entry appends onto the open line. That is the
   manifest's rule rather than this ledger's, and it is not worked around here: a guard
   that read the file before writing broke the one-write rule the atomicity rests on. What
   this ledger adds is a way to see the damage. The manifest entry's row count counts the
   file's lines, so a count above what :func:`read` returns says the file needs a human.
2. *Last entry wins*, keyed by ``(instrument_id, ex_date, type)``. A correction is a
   superseding entry, never a rewrite of the one it corrects.
3. *It is manifested and scrubbed like any lake file.* The reverse scrub's exclusion set
   names ``manifest.jsonl``, ``journal/`` and ``reports/`` alone, so a write that skipped
   its manifest entry would be reported as an orphan.

**Append-only, because of look-ahead bias.** Rewriting the file would mean a dividend
corrected in August silently rewrites June's factor, and nothing could then say what the
lake knew on 2026-06-20. Every backtest would read today's corrected history as though it
had always been available. Appending turns that into a filter, ``recorded_at <= D``, which
is what :func:`as_of` reads.

**Order is a property of the file rather than a rule a writer remembers.** Marketlake #242
settled that Parquet row order is incidental, proving it against a fixture written in
deliberately shuffled order, so a Parquet table would have needed an explicit sequence
column to carry what ``O_APPEND`` gives for free. Resolution therefore reads the last entry
on a key *in file order*, never the highest ``recorded_at``. A clock stepping backwards,
from an NTP correction or a restored machine, would otherwise let an older entry win its
key. ``recorded_at`` is the as-of filter alone, which also means it never has to be unique.

**The price of a ledger rather than a table is the loss of typed columns**, so the type
discipline lives here. ``manifest.py`` holds its own entries two ways this copies. Every
field :func:`append` takes is keyword-only and typed, so a caller cannot assemble a
malformed entry. And resolution raises at the offending entry rather than stepping over a
line it cannot interpret, the way a manifest line naming no partition raises
``ManifestError``.

**The three vendor dates normalize to ``YYYY-MM-DD`` on write.** This is where losing typed
columns bites hardest, because ``ex_date`` sits in the key. Schwab supplies ``div_ex_date``
as ``2026-06-18T00:00:00Z``, a timestamp spelling of a date, and ``loader.py`` records that
the live lake has already produced more than one spelling of one instant. Unnormalized,
``2026-06-18`` and ``2026-06-18T00:00:00Z`` would be two keys for one event, so a
correction would land beside the original instead of superseding it, silently, in the
ledger every total-return factor reads. Parquet's ``date32`` enforced this for free. Here
:func:`normalize_date` does.

**The dividend extraction is the first writer, and it lives here.** Marketlake #282 shipped
the record format, the writer, and the two reads that resolve it, and said nothing in the
module wrote an entry of its own. #284 is what changed that. It reads the dividend fields
off sealed quotes rows the lake already holds, gates each one, and appends what the gate
agrees to, under ``python -m lake.actions``. The command a human writes a ``manual`` entry
with is still #286.

**The split detector is the second writer, and it lives in ``lake.splits``.** It reads a
different surface, has its own gate, its own report shape and its own subcommand, which is a
second deliverable's worth of module rather than a section of this one. What it takes from
here is :func:`append`, the vocabulary above, :class:`Skip` and the three reasons both walks
meet, and five helpers they share: :func:`read_master`, :func:`surface_ticker_days`,
:func:`by_ticker`, :func:`by_reason` and :func:`same_but_for_recorded_at`. Those are public
for that reason. A second copy of the
last one in particular could drift from this one silently, and it is what stops either walk
appending the same entry every night forever. The import direction runs one way, from
``splits`` to here, and :func:`main` keeps it that way by importing the walk inside the
branch that runs it.

**The gate lands ahead of the validation battery, and on purpose.** Chains and quotes seal
first and are flagged afterwards, because a captured minute is unrepeatable and a refusal
would lose it. A ledger entry is the opposite: it is derived from rows already on disk, so
a wrong one can be held today and landed tomorrow at no cost, while a wrong one that lands
corrupts every adjusted price computed through it. So this surface gates before it writes.
D20's battery in slice 5 subsumes the check later. Waiting for it would land ungated
entries for two slices.

``recorded_at`` is injected, never read from a wall clock. ``manifest.py`` states that rule
for itself and every writer in the lake follows it. It is what lets the suite run offline
and deterministically.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from math import isfinite
from pathlib import Path
from typing import Any

from lake.calendar import MARKET_TZ
from lake.clock import Clock, SystemClock
from lake.loader import NoSpotClose, PartialRead, PartitionAbsent, PartitionQuarantined, load_quotes
from lake.manifest import append_line, latest_entries, parse_jsonl, record_partition
from lake.paths import ACTIONS, CORPORATE_ACTIONS_FILE, QUOTES, parse_partition_rel
from lake.report import Withheld, write_withheld
from lake.security_master import AmbiguousSymbol, MasterUnreadable, SecurityMaster, master_path

# This module's own schema version, stamped on every entry it writes. It names the shape
# of a ledger line, not the journal shape a capture row carries, and the two move
# independently.
ACTIONS_SCHEMA_VERSION = 1

# The ledger's lake-relative path, which is also its manifest key.
ACTIONS_PARTITION = f"{ACTIONS}/{CORPORATE_ACTIONS_FILE}"

# The manifest ``source`` for this ledger's entry. The live manifest uses ``capture``,
# ``compaction`` and ``reference``, so this is the fourth value, and the rest of D16
# imports it rather than restating the string. It names the job that writes these
# surfaces, the evening vendor sweep, the way ``CAPTURE_SOURCE`` and ``COMPACTION_SOURCE``
# each name their writer rather than their file.
SWEEP_SOURCE = "sweep"

# The two kinds of action. Both are fixed here because marketlake #279's split detector
# writes the same field and should not choose the spelling a second time.
TYPE_DIVIDEND = "dividend"
TYPE_SPLIT = "split"
ACTION_TYPES = frozenset({TYPE_DIVIDEND, TYPE_SPLIT})

# What the lake saw, rather than what it should have seen. ``observed`` means the value
# changed between two observations the extraction made. ``vendor_reported`` means it was
# already there on the first observation, so the change was never seen. ``manual`` means a
# human entered it, and nothing here creates one: #286 is the command that does, and the
# value is reserved now so the schema does not move when it lands.
PROVENANCE_OBSERVED = "observed"
PROVENANCE_VENDOR_REPORTED = "vendor_reported"
PROVENANCE_MANUAL = "manual"
PROVENANCES = frozenset({PROVENANCE_OBSERVED, PROVENANCE_VENDOR_REPORTED, PROVENANCE_MANUAL})

# The three fields the key is built from, read in this order.
KEY_FIELDS = ("instrument_id", "ex_date", "type")

# The key one entry resolves under: the instrument, the date the event happened, and the
# kind of event. An instrument has one dividend per ex-date and one split per ex-date. The
# key is what a reader resolves over rather than a uniqueness constraint. Several entries
# share it, and the last one in the file is the current answer.
ActionKey = tuple[int, str, str]

# The two gates this module files a refusal under. They name what refused rather than what
# was refused, because the finding already carries the event.
CHECK_DIVIDEND_CONSISTENCY = "dividend_consistency"
CHECK_INSTRUMENT_RESOLUTION = "instrument_resolution"
# The payload the ledger's own record rules refused: a vendor date that does not name a
# date, an amount ``append`` will not take, or a close whose rows disagree with each other.
# Each one used to end the run as a traceback, which is neither fail-closed nor a record.
CHECK_DIVIDEND_PAYLOAD = "dividend_payload"

# The six quote columns a dividend is read out of. ``div_pay_amount`` is the per-event
# amount and the one that lands as ``cash_amount``. ``div_amount`` is the annualized
# trailing figure the vendor's yield keys off, and it is never the amount: QQQ's is exactly
# four times its per-event amount and SPY's within 0.00002, so reading it would inflate
# every total-return factor fourfold for a quarterly payer. It is here because the gate
# below is what compares the two.
DIV_EX_DATE = "div_ex_date"
DIV_PAY_AMOUNT = "div_pay_amount"
DIV_AMOUNT = "div_amount"
DIV_FREQ = "div_freq"
DIV_PAY_DATE = "div_pay_date"
DECLARATION_DATE = "declaration_date"
DIVIDEND_COLUMNS = (
    DIV_EX_DATE,
    DIV_PAY_AMOUNT,
    DIV_AMOUNT,
    DIV_FREQ,
    DIV_PAY_DATE,
    DECLARATION_DATE,
)

# How far the vendor's annualized figure may sit from ``div_freq`` times its own per-event
# amount before the gate holds the dividend out.
#
# It is measured rather than guessed, and it is relative rather than absolute. SPY reports
# 7.61406 against four times 1.90352, which is 7.61408, so the vendor's own arithmetic is
# off by 0.00002. QQQ is exact. That 0.00002 is 2.6 parts in a million of SPY's figure, and
# three parts in a million is the whole number that admits it and no more. An absolute
# 0.00002 would mean something different on every payer: three parts in a million of SPY's
# annualized figure, and a fifth of a percent of a penny one.
#
# The design already says real payloads violate this occasionally, which is why a
# disagreement files rather than pages. A small payer whose per-event amount the vendor
# rounds to five decimals carries more relative slack than a large one, so this refuses
# some correct payloads. A refusal costs a night's entry and a file a human reads. Landing
# a wrong amount costs every adjusted price computed through it, so the gate is tight.
DIVIDEND_CONSISTENCY_TOLERANCE = 3e-6


class ActionsError(Exception):
    """Base class for every corporate-actions error."""


class LedgerLineError(ActionsError):
    """Raised for a ledger line that parses as JSON and carries no usable key.

    Skipping such a line was considered and rejected, for the reason ``manifest.py``
    gives for its own. A torn trailing line is a write that did not finish, and the read
    already discards exactly that. A line in the body that parses and names nothing is a
    record no reader can interpret, and this ledger is what every adjusted price is
    computed through. A reader that quietly stepped over damage in it would make every
    factor downstream weaker than it reads.
    """

    def __init__(self, path: Path, position: int, detail: str) -> None:
        super().__init__(f"{path}: entry {position} {detail}")
        self.path = path
        self.position = position


class UnresolvedSymbol(ActionsError):
    """Raised when the master has no instrument for a symbol on the observation date.

    A quote row exists only inside a capture span, so a symbol the lake observed and the
    master cannot place means the master and the capture spans disagree. That is a
    reference-data fault rather than a missing action, and it fails closed: the action is
    held out of the ledger rather than landing under a guessed instrument.
    """

    def __init__(self, symbol: str, on: date) -> None:
        super().__init__(f"no instrument for {symbol!r} on {on.isoformat()}")
        self.symbol = symbol
        self.on = on


class MasterAbsent(ActionsError):
    """Raised when the lake holds no security master for the extraction to resolve against.

    ``SecurityMaster.read`` reports an absent file as a bare ``FileNotFoundError``, and it
    keeps the fold into ``MasterUnreadable`` narrow on purpose: an absent master is not a
    corrupt one, and callers treat the two apart. This is that separation, named, so the
    command can say which of the two it met. An absent master wants the onboarding command.
    A torn one wants a restore, and running the onboarding command against it is being told
    the wrong thing.

    Every other ``OSError`` the read can raise, such as a bad sector, keeps its own class
    and its traceback. That is a corrupt lake rather than an operator mistake.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(f"no security master at {path}")
        self.path = path


# -- paths -------------------------------------------------------------------


def actions_path(lake_root: Path | str) -> Path:
    """The ledger's path under a lake root: ``actions/corporate_actions.jsonl``."""
    return Path(lake_root) / ACTIONS / CORPORATE_ACTIONS_FILE


# -- normalization -----------------------------------------------------------


def normalize_date(value: date | str) -> str:
    """Render a date as ``YYYY-MM-DD``, whatever spelling it arrives in.

    A ``date`` renders directly. A ``datetime`` and a string are both read as timestamps,
    because a ``datetime`` is a ``date`` carrying a time. The string may be a plain ISO
    date, or the timestamp spelling of one that Schwab's fundamentals use, with ``Z`` or an
    explicit offset.

    A timestamp whose time is not midnight raises, whichever of the two it arrived as. It
    is not a spelling of a date, and deciding which day it names would mean picking one
    silently. A midnight timestamp keeps the calendar date it is written with and is never
    converted across offsets, because the vendor is naming a date rather than an instant.
    """
    if isinstance(value, datetime):
        return _date_of_midnight(value, value.isoformat())
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        pass
    parsed = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        stamp = datetime.fromisoformat(parsed)
    except ValueError as exc:
        raise ValueError(f"{value!r} is not a date or a timestamp spelling of one") from exc
    return _date_of_midnight(stamp, text)


def _date_of_midnight(stamp: datetime, original: str) -> str:
    """The calendar date a midnight timestamp names. A non-midnight time raises.

    The components are read one by one rather than compared against a constructed
    ``time``. A ``datetime.time(...)`` in production code is what the calendar seam's own
    check refuses, since a time built here rather than derived from the market calendar is
    how a hardcoded session time gets in.
    """
    if stamp.hour or stamp.minute or stamp.second or stamp.microsecond:
        raise ValueError(f"{original!r} carries a time of day, so it does not name a date")
    return stamp.date().isoformat()


def _require_utc(when: datetime, label: str) -> datetime:
    """Reject a naive datetime and normalize an aware one to UTC."""
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return when.astimezone(UTC)


# -- reading -----------------------------------------------------------------


def read(lake_root: Path | str) -> list[dict]:
    """Every entry in file order, with the torn trailing line discarded.

    Superseded entries come back too, because the history is the record. A missing ledger
    reads as no entries, which is what keeps every reader inert on a lake the extraction
    below has not yet written to.

    This returns each entry as the mapping its writer appended, the way
    ``read_quarantine`` does. Resolution is where a line is read for meaning, so that is
    where a line carrying no key raises. An entry stamped with a schema version this code
    does not know is read rather than refused: a written line is never touched again, so
    there is no half-understood shape to write back, and the stamp is what a later reader
    interprets it through.
    """
    path = actions_path(lake_root)
    if not path.exists():
        return []
    return parse_jsonl(path.read_text())


def entry_line_count(lake_root: Path | str) -> int:
    """How many lines the ledger holds, parseable or not.

    This is the manifest entry's row count. It counts what was written rather than what
    reads back, so it never falls after a line the read cannot parse, and comparing it
    against ``len(read(...))`` is how a damaged ledger announces itself.
    """
    path = actions_path(lake_root)
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text().splitlines() if line.strip())


def entry_key(entry: dict, *, path: Path, position: int) -> ActionKey:
    """The key an entry resolves under. A line carrying no usable key raises.

    ``instrument_id`` must be a whole number and not a bool, since JSON's ``true``
    indexes as ``1`` and would quietly file an action under instrument 1. ``ex_date`` and
    ``type`` must be strings.
    """
    if not isinstance(entry, dict):
        raise LedgerLineError(path, position, f"is {type(entry).__name__}, not an object")
    missing = [field for field in KEY_FIELDS if field not in entry]
    if missing:
        raise LedgerLineError(path, position, f"names no {', '.join(missing)}")

    instrument_id = entry["instrument_id"]
    if isinstance(instrument_id, bool) or not isinstance(instrument_id, int):
        raise LedgerLineError(path, position, f"has a non-integer instrument_id {instrument_id!r}")
    ex_date, action_type = entry["ex_date"], entry["type"]
    for name, value in (("ex_date", ex_date), ("type", action_type)):
        if not isinstance(value, str):
            raise LedgerLineError(path, position, f"has a non-string {name} {value!r}")
    return (instrument_id, ex_date, action_type)


def _latest_by_key(numbered: Sequence[tuple[int, dict]], path: Path) -> dict[ActionKey, dict]:
    """Resolve last-entry-wins per key over entries in file order.

    Each entry arrives with the position it has among the file's entries rather than its
    index in the list handed over, because :func:`as_of` resolves a filtered subset and a
    position counted over that subset would name the wrong entry. Blank lines are not
    entries, so this counts what a reader counts rather than physical lines, which is what
    the error message says.
    """
    latest: dict[ActionKey, dict] = {}
    for position, entry in numbered:
        latest[entry_key(entry, path=path, position=position)] = entry
    return latest


def latest(lake_root: Path | str) -> dict[ActionKey, dict]:
    """The current answer per key: the last entry on it in file order.

    This is the shape ``manifest._latest_by_partition`` already implements, and the way
    ``loader.py`` consults ``quarantine.jsonl`` at read time.
    """
    return _latest_by_key(list(enumerate(read(lake_root), start=1)), actions_path(lake_root))


def as_of(lake_root: Path | str, on: date) -> dict[ActionKey, dict]:
    """The same resolution restricted to what the lake had recorded by the end of ``on``.

    This is the point-in-time read append-only exists for. Without it a backtest spanning
    June silently uses an August correction, which is the look-ahead bias the write rule
    was chosen to remove.

    ``on`` is a market date. An entry counts when its ``recorded_at``, read in market
    time, falls on or before it. Comparing in market time rather than on the stored text
    is the same rule ``loader.py`` follows for ``snap_ts``: one instant has more than one
    spelling, so a text comparison would answer differently for two entries written at the
    same moment under different offsets.
    """
    path = actions_path(lake_root)
    kept = [
        (position, entry)
        for position, entry in enumerate(read(lake_root), start=1)
        if _recorded_on(entry, path=path, position=position) <= on
    ]
    return _latest_by_key(kept, path)


def _recorded_on(entry: dict, *, path: Path, position: int) -> date:
    """The market date an entry was recorded on. A missing or unreadable stamp raises."""
    if not isinstance(entry, dict) or "recorded_at" not in entry:
        raise LedgerLineError(path, position, "names no recorded_at")
    stamp = entry["recorded_at"]
    try:
        parsed = datetime.fromisoformat(str(stamp))
    except ValueError as exc:
        raise LedgerLineError(path, position, f"has an unreadable recorded_at {stamp!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LedgerLineError(path, position, f"has a naive recorded_at {stamp!r}")
    return parsed.astimezone(MARKET_TZ).date()


# -- resolving the instrument ------------------------------------------------


def resolve_instrument(master: SecurityMaster, symbol: str, observed_on: date) -> int:
    """The ``instrument_id`` a symbol names on the date the lake observed it.

    **Resolve at the observation date, never at the ex-date.** An action is attributed to
    the instrument observed reporting it. The lake saw SPY on 2026-09-14 report a June
    dividend, so the instrument is whoever SPY was that day, and ``ex_date`` still records
    when the event happened. Resolving at the ex-date lands nothing instead: ``onboard``
    sets ``valid_from`` to the onboarding date and ``Mapping.valid_on`` refuses any
    earlier day, so every pre-capture ex-date would be held. The observation date always
    resolves, because a quote row exists only inside a capture span.

    Both of the master's non-answers fail closed rather than skipping. ``None`` raises
    :class:`UnresolvedSymbol`, which means the master and the capture spans disagree.
    ``AmbiguousSymbol`` passes through, which the master calls a corrupt master: one
    symbol mapping to several instruments on one date. An action held out of the ledger
    can be landed later. One landed under the wrong instrument corrupts every factor that
    instrument's prices feed.

    An entry that carries its own ``instrument_id`` skips this, which is the contract
    :func:`append` accepts by taking the id rather than a symbol. #286's ``manual`` entry
    is the one that uses it, since a human's entry has no observation to resolve at.
    """
    instrument_id = master.resolve(symbol, observed_on)
    if instrument_id is None:
        raise UnresolvedSymbol(symbol, observed_on)
    return instrument_id


# -- writing -----------------------------------------------------------------


def append(
    lake_root: Path | str,
    *,
    instrument_id: int,
    observed_on: date | None,
    recorded_at: datetime,
    ex_date: date | str,
    type: str,
    pay_date: date | str | None = None,
    declared_date: date | str | None = None,
    cash_amount: float | None = None,
    split_ratio: float | None = None,
    provenance: str,
    schema_version: int = ACTIONS_SCHEMA_VERSION,
) -> dict:
    """Append one entry to the ledger and refresh its manifest entry. Returns the entry.

    Both writes happen inside one hold of the lake-root ``flock``, which this takes
    itself. ``manifest.py`` does not take it for a caller, and every other writer in the
    lake takes it at its own call site: ``onboard``, ``retire``, ``seed_spans``,
    ``compact``, ``capture``, ``gap`` and ``schema_versions``. The ledger line and the
    refreshed manifest entry go together, so a weekend write never leaves the Sunday scrub
    facing a sha nothing has caught up to.

    Every field of the entry is keyword-only and typed, so a malformed entry cannot be
    assembled. ``lake_root`` is the one positional argument, because it names where to
    write rather than what. The three vendor dates normalize, and the either-or rule is
    enforced rather than described: a dividend fills ``cash_amount`` and leaves
    ``split_ratio`` null, and a split fills ``split_ratio`` and leaves ``cash_amount``
    null.

    What a split leaves in the two remaining dates is a convention rather than a refusal.
    A split pays nothing and Schwab's fundamentals carry no announcement date for one, so
    marketlake #279 writes both null. Refusing them here would foreclose an exchange that
    does announce one, and #282 states the convention for #279 to read rather than as a
    rule this module enforces.

    ``type`` shadows the builtin deliberately. It is the entry's own field name, so a
    call site reads as the record it writes, and nothing here calls the builtin.
    """
    entry = build_entry(
        instrument_id=instrument_id,
        observed_on=observed_on,
        recorded_at=recorded_at,
        ex_date=ex_date,
        type=type,
        pay_date=pay_date,
        declared_date=declared_date,
        cash_amount=cash_amount,
        split_ratio=split_ratio,
        provenance=provenance,
        schema_version=schema_version,
    )

    lake_root = Path(lake_root)
    target = actions_path(lake_root)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Local to keep this module free of the lock unless it writes, the same reason
    # onboard.py, retire.py, seed_spans.py and schema_versions.py import it here.
    from lake.lock import lake_lock

    with lake_lock(lake_root):
        append_line(target, entry)
        # The count comes from the file's lines rather than from the entries ``read``
        # returns, and the difference is what keeps a damaged file from stopping the
        # writer. A line the read cannot parse ends the read, so a parsed count can fall
        # below the manifested one, and ``guard_row_count`` would then raise on every
        # later append, after that append had already written its line. A line count only
        # ever grows, so the guard is satisfied by construction and a count above what
        # ``read`` returns is the signal that the file needs a human.
        #
        # The entry is also what keeps the reverse scrub from calling the ledger an
        # orphan, since ``actions/`` is not in the scrub's exclusion set.
        record_partition(
            lake_root,
            ACTIONS_PARTITION,
            source=SWEEP_SOURCE,
            rows=entry_line_count(lake_root),
            fetched_at=recorded_at.isoformat(),
        )
    return entry


def build_entry(
    *,
    instrument_id: int,
    observed_on: date | None,
    recorded_at: datetime,
    ex_date: date | str,
    type: str,
    pay_date: date | str | None = None,
    declared_date: date | str | None = None,
    cash_amount: float | None = None,
    split_ratio: float | None = None,
    provenance: str,
    schema_version: int = ACTIONS_SCHEMA_VERSION,
) -> dict:
    """Validate and render one entry without writing it.

    :func:`append` is the writer. This is the half that decides what a well-formed entry
    is, so a caller assembling one can be checked without a lake on disk. That is also what
    both walks compare against the ledger before they write: a candidate built here can be
    read against what :func:`latest` already resolves, and an entry that matches on every
    field but ``recorded_at`` is one the ledger already holds.

    The eleven fields, and what each answers.

    1. ``instrument_id`` is the key every join in the lake runs on.
    2. ``observed_on`` is when the lake saw the event in vendor data.
    3. ``recorded_at`` is when the entry was written down.
    4. ``ex_date`` is when the event happened.
    5. ``pay_date`` is when the cash arrives.
    6. ``declared_date`` is when the event was announced.
    7. ``type`` says which kind of event it is.
    8. ``cash_amount`` is the per-event dividend.
    9. ``split_ratio`` is the split's multiplier.
    10. ``provenance`` records what the lake saw rather than what it should have seen.
    11. ``schema_version`` stamps the shape this entry was written in.

    There is deliberately no ``ticker`` field. The master exists because tickers change,
    FB to META and QQQQ to QQQ. The capture surfaces carry one only because they are
    Hive-partitioned by it, and a ledger is partitioned by nothing, so a stored ticker
    would be a mutable key in a record built to avoid them.
    ``SecurityMaster.symbol_at(instrument_id, observed_on)`` recovers it.

    ``declared_date`` is carried verbatim and is not comparable across issuers. QQQ's sits
    four days before its ex-date and SPY's five and a half months, so a consumer treating
    the gap as meaningful would read SPY's as an anomaly.
    """
    if isinstance(instrument_id, bool) or not isinstance(instrument_id, int):
        raise ValueError(f"instrument_id must be an int, got {instrument_id!r}")
    if type not in ACTION_TYPES:
        raise ValueError(f"type must be one of {sorted(ACTION_TYPES)}, got {type!r}")
    if provenance not in PROVENANCES:
        raise ValueError(f"provenance must be one of {sorted(PROVENANCES)}, got {provenance!r}")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError(f"schema_version must be an int, got {schema_version!r}")

    amounts = {"cash_amount": cash_amount, "split_ratio": split_ratio}
    for name, value in amounts.items():
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{name} must be a number or None, got {value!r}")
        # A non-finite amount is refused because ``json.dumps`` renders it as bare ``NaN``
        # or ``Infinity``, which no strict JSON reader accepts. One such line would refuse
        # the whole ledger to DuckDB while Python read it back without complaint. A NaN
        # amount also turns every adjusted price it touches into a NaN.
        if not isfinite(value):
            raise ValueError(f"{name} must be a finite number, got {value!r}")

    # The either-or rule, both ways. Decision 1 in marketlake #282 splits the amount into
    # two fields so a reader never has to branch on ``type`` before it can trust the
    # number, and that only holds if exactly one of them is ever filled.
    if type == TYPE_DIVIDEND:
        if cash_amount is None:
            raise ValueError("a dividend fills cash_amount")
        if split_ratio is not None:
            raise ValueError("a dividend leaves split_ratio null")
    else:
        if split_ratio is None:
            raise ValueError("a split fills split_ratio")
        if cash_amount is not None:
            raise ValueError("a split pays nothing, so it leaves cash_amount null")

    # A ratio is a multiplier every adjusted price is computed through, so zero and
    # negative are refused here rather than dividing by zero in each reader. A negative
    # dividend is refused for the same reason: nothing pays one.
    if split_ratio is not None and split_ratio <= 0:
        raise ValueError(
            f"split_ratio is a multiplier, so it must be positive, got {split_ratio!r}"
        )
    if cash_amount is not None and cash_amount < 0:
        raise ValueError(f"cash_amount must not be negative, got {cash_amount!r}")

    return {
        "instrument_id": instrument_id,
        "observed_on": None if observed_on is None else normalize_date(observed_on),
        "recorded_at": _require_utc(recorded_at, "recorded_at").isoformat(),
        "ex_date": normalize_date(ex_date),
        "pay_date": None if pay_date is None else normalize_date(pay_date),
        "declared_date": None if declared_date is None else normalize_date(declared_date),
        "type": type,
        "cash_amount": None if cash_amount is None else float(cash_amount),
        "split_ratio": None if split_ratio is None else float(split_ratio),
        "provenance": provenance,
        "schema_version": schema_version,
    }


# -- the gate ----------------------------------------------------------------


@dataclass(frozen=True)
class DividendConsistency:
    """What the self-consistency check compared, and whether it agreed.

    ``computed`` is ``div_freq`` times the per-event amount and ``against`` is the vendor's
    own annualized figure. Both ride the verdict rather than being recomposed by the caller,
    because they are the two numbers the withheld finding files and a caller that recomputed
    them could file a pair the check never saw.

    Either is ``None`` when the payload did not carry it. A check missing an input has not
    agreed, which is what makes a partial payload a held dividend rather than a silent one.
    """

    agrees: bool
    computed: float | None
    against: float | None


def check_dividend_consistency(
    *,
    div_freq: int | None,
    div_pay_amount: float | None,
    div_amount: float | None,
) -> DividendConsistency:
    """Whether the vendor's annualized figure agrees with its own per-event amount.

    This is the internal validation a dividend lands through. It catches a drifted or stale
    fundamental, where one of the two figures moved and the other did not, which is exactly
    the shape that would put a wrong amount in the ledger while looking well-formed.

    The comparison is relative, at :data:`DIVIDEND_CONSISTENCY_TOLERANCE`, and that constant
    carries the measurement it came from.

    Three edges are decided here rather than left to a division, and each one is a way the
    arithmetic stops meaning anything.

    1. A payload missing any of the three inputs has nothing to compare, so it does not
       agree.
    2. A frequency of zero or less multiplies the per-event amount out of the comparison
       entirely, so a vendor reporting a stale amount beside a zeroed frequency would be
       judged on an equation that no longer mentions the amount. That is the drifted
       fundamental this check exists to catch, so it does not agree.
    3. An annualized figure of zero has no relative scale. With a positive frequency the
       product is zero only when the per-event amount is, so comparing the product to zero
       still asks about the amount.
    """
    if div_freq is None or div_pay_amount is None or div_amount is None:
        computed = (
            None if (div_freq is None or div_pay_amount is None) else div_freq * div_pay_amount
        )
        return DividendConsistency(agrees=False, computed=computed, against=div_amount)

    computed = div_freq * div_pay_amount
    if div_freq <= 0:
        return DividendConsistency(agrees=False, computed=computed, against=div_amount)
    if div_amount == 0:
        return DividendConsistency(agrees=computed == 0, computed=computed, against=div_amount)
    difference = abs(computed - div_amount) / abs(div_amount)
    return DividendConsistency(
        agrees=difference <= DIVIDEND_CONSISTENCY_TOLERANCE,
        computed=computed,
        against=div_amount,
    )


# -- the extraction ----------------------------------------------------------


@dataclass(frozen=True)
class Landed:
    """One entry the run appended, and the ticker it was read off.

    The entry carries no ``ticker`` field, deliberately: the master exists because tickers
    change, and a ledger is partitioned by nothing, so a stored ticker would be a mutable key
    in a record built to avoid them. The report is not the record, though, and an operator
    reading "instrument 1" has to go and look up who that was. So the symbol the row was read
    under rides beside the entry, for the render alone.
    """

    entry: dict
    symbol: str


@dataclass(frozen=True)
class HeldFinding:
    """One thing the run refused to land, and what became of the record of it.

    ``filed_at`` is the file it was written down in, so the run's own report can point a
    reader at it. A held finding recurs every night until something settles it, and a report
    naming the file is what turns thirty files in one directory into a place to look.

    ``filed_at`` is ``None`` when the write itself failed, and ``filing_error`` then names
    the class that refused it. The two together are why the failure does not end the run.
    ``write_withheld`` raises rather than swallowing and says the containment belongs here,
    because a raise out of the filing costs the rest of the walk, and the rest of the walk is
    other tickers' dividends. What the failure costs instead is an exit code, which is the
    same shape ``compact`` gives a schema-drift file it could not write.
    """

    finding: Withheld
    filed_at: Path | None
    filing_error: str | None = None


# Why a ticker-day was not read. Each is one session a ledger walk could not use, and each
# costs that ticker-day rather than the ticker's walk or the run.
#
# The three below the first are here rather than in ``lake.splits`` because both walks meet
# them and one reason has to have one spelling. A second copy could drift without anything
# noticing, which is the reason :func:`same_but_for_recorded_at` gives for living here too.
# The close-of-record reason stays per surface, since each names the tag it resolved against:
# this one is ``spot_close`` and ``splits.REASON_NO_OPTION_CLOSE`` is ``option_close``.
REASON_NO_SPOT_CLOSE = "no spot close"
REASON_QUARANTINED = "quarantined"
REASON_PARTIAL_READ = "partial read"
REASON_PARTITION_ABSENT = "manifested partition absent"


@dataclass(frozen=True)
class Skip:
    """One ticker-day the walk did not read, and why."""

    ticker: str
    day: date
    reason: str


def by_reason(items: Sequence[Any]) -> list[str]:
    """One line per distinct reason, with its count.

    Counts rather than a line each, so a lake whose every session is a gap day still renders
    on one screen. ``items`` is anything carrying a ``reason``, which is :class:`Skip` here,
    and ``splits.NotAnAdjustment`` and ``splits.ScaleUnread`` beside it.
    """
    lines = []
    for reason in sorted({item.reason for item in items}):
        lines.append(f"    - {reason}: {sum(1 for i in items if i.reason == reason)}")
    return lines


@dataclass(frozen=True)
class ExtractionReport:
    """What one run of the extraction did, for the sign-off block.

    ``unchanged`` counts the dividends the walk re-derived and found already in the ledger.
    It is the number that makes a second run legible: a run that appends nothing and holds
    nothing has either learned nothing new or read nothing at all, and only this tells the
    two apart.

    ``skipped`` carries every ticker-day the walk could not read, each keeping its reason.
    Catching a refusal without counting it would trade one silence for another: the run would
    survive and report nothing about the partitions it was refused. The cost is real rather
    than nominal, because a skipped session moves ``observed_on`` to the next one the walk
    reads, and a value that moves and moves back across the skip is not recorded at all.
    """

    ticker_days: int
    appended: tuple[Landed, ...]
    held: tuple[HeldFinding, ...]
    unchanged: int
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
            f"Dividend extraction over {self.ticker_days} sealed quotes ticker-day(s)",
            f"  appended:  {len(self.appended)}",
        ]
        for landed in self.appended:
            entry = landed.entry
            lines.append(
                f"    - {landed.symbol} (instrument {entry['instrument_id']}) {entry['type']} "
                f"ex {entry['ex_date']} cash {entry['cash_amount']} "
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
        lines.append(f"  skipped:   {len(self.skipped)}")
        lines.extend(by_reason(self.skipped))
        return "\n".join(lines)


def extract_dividends(*, lake_root: Path | str, clock: Clock) -> ExtractionReport:
    """Read every sealed quotes ticker-day, gate what it finds, and append what lands.

    Nothing here fetches. ``QUOTES_SCHEMA`` has carried the vendor's ``fundamental`` block
    since marketlake #17, so the evidence a dividend is derived from is already on disk. Every
    dependency is injected and this reads no config, the way ``seed_spans`` does.

    **The ticker-days come from the manifest**, through :func:`surface_ticker_days`.
    ``manifest.latest_entries`` returns the
    current entry per partition path, which is the lake's own record of what it holds.
    Deriving them from the roster and the exchange calendar instead would ask for every
    session the calendar carries, and ``load_quotes`` raises ``PartitionAbsent`` on a session
    the lake never captured.

    **Reading the manifest bounds what a refusal can be, and it does not remove refusals.**
    This sentence used to say that every enumerated partition exists, so ``NoSpotClose`` was
    the only absence the walk could meet. That was the argument for catching one exception
    and no others, and it is wrong three ways.

    1. ``load_quotes`` is ``_load_surface`` pointed at the quotes surface, so it carries every
       guard ``load_chain`` carries, and four named ``LoadError`` subclasses reach a caller.
    2. The manifest is the lake's record of what it sealed rather than a guarantee the file is
       still on disk.
    3. ``lake.splits`` caught those four from the day it shipped, over the same manifest and
       the sibling surface, so the shape was never in doubt.

    **The read is one ``load_quotes`` call per ticker-day**, measured at about 10 ms each
    against the live lake. Reading the partitions directly is a little faster and is refused:
    every read in the lake goes through the loader, so the quarantine guard and the overflow
    projection are asked once rather than skipped by a second path that would then keep
    skipping them forever.

    The walk, per ticker, in date order.

    1. A ticker-day the loader refuses is skipped, counted, and keeps its reason. Four named
       refusals are contained, and each costs that ticker-day rather than the ticker's walk or
       the run. :func:`_observation` names what is deliberately left to escape and why.

       - ``NoSpotClose``, the gap day, with nothing to read.
       - ``PartitionQuarantined``, a partition the validation battery withheld, where the
         rows exist and the verdict says not to read them.
       - ``PartitionAbsent``, a manifested partition whose file is gone.
       - ``PartialRead``, one the overflow projection could not present whole, where reading
         on would be a comparison against contents nobody saw in full.

       **A skip is repairable, which is what settles this against holding a finding.**
       ``lake.splits`` cannot say the same about its own walk: there ``ex_date`` is derived
       from a boundary and sits in the ledger's key, so a date a skip made the detector guess
       lands under a second key and every adjusted price applies the split twice. Here
       ``ex_date`` is read off the row. What a skip moves is ``observed_on``, which is no part
       of the key, and :func:`same_but_for_recorded_at` compares it, so the run after a
       verdict clears appends the corrected entry once and every run after that reads
       unchanged. A ``Withheld`` finding would instead be filed again every night a verdict
       stood, while ``sweep.count_quarantined`` already carries that count to the same reader.

       Two costs are named rather than hidden.

       1. A session that captured hundreds of minutes and missed its close is skipped too,
          even though the dividend is a property of the session rather than of its final
          minute. The loader offers one row per call and the close of record is the one row a
          caller can name without knowing which minutes exist, so the alternative is a second
          read path.
       2. A value that moves and moves back across a skipped session leaves ``previous``
          matching the session after it, so that middle value is never recorded at all rather
          than recorded a day late.

       The count on the report is what an operator reads either cost off.
    2. A row carrying no ``div_ex_date`` is no observation either. A non-paying instrument
       reports nothing, and an entry of nulls is not an event. Without an ex-date there is no
       key, so there is nothing to land and nothing to hold.
    3. An observation whose instrument and ``div_ex_date`` differ from the previous one emits
       an entry. It carries provenance ``observed`` when the lake has already watched that
       instrument carrying a value, and ``vendor_reported`` when it has not, because there
       the change itself was never seen. The dates are compared normalized, so the vendor
       writing one date two ways is not a transition. The instrument is half of the
       comparison because a symbol handed from one instrument to another is a new thing to
       record, which is what step 4 of the issue means by grouping on the instrument.
    4. The instrument is resolved at the date of that first observation carrying the new
       value, and the same date lands in ``observed_on``. They cannot differ: the resolver
       decides which instrument the action is attributed to, so resolving at one date and
       recording another attributes it to whoever held the ticker on a different day.
    5. The gate runs, and a disagreement holds the entry out and files it. So does a payload
       the ledger's own record rules refuse, rather than ending the run as a traceback.
    6. A key is emitted once per run, at the first observation carrying its value. An ex-date
       that moves away and comes back is not a second first, and emitting it twice under two
       provenances would leave neither matching what ``latest`` resolves, so the ledger would
       grow by two lines every night forever.
    7. The entry lands only when it differs from what ``latest`` already resolves on its key,
       on every field but ``recorded_at``. Comparing whole entries would append every night
       forever, since ``recorded_at`` is the clock's answer and moves while nothing else does.

    **Both ways the resolution can fail hold the action and file it.** ``UnresolvedSymbol``
    says the master and the capture spans disagree about a ticker. ``AmbiguousSymbol`` says
    the master is corrupt. Either way an action held out can be landed later, while one landed
    under the wrong instrument corrupts every factor that instrument's prices feed.

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
    # ledger already holds, the way the close guard reads the manifest once per run.
    current = latest(lake_root)

    ticker_days = surface_ticker_days(lake_root, QUOTES)
    appended: list[Landed] = []
    held: list[HeldFinding] = []
    skipped: list[Skip] = []
    unchanged = 0

    # Every instrument this walk has already seen carrying a value, which is what tells a
    # change the lake watched happen from a value that was already there when it started
    # looking. It is keyed on the instrument rather than the ticker, so a symbol handed from
    # one instrument to another gives the incoming one its own first observation.
    seen: set[int] = set()
    # Every key this run has already emitted. A key is emitted at the first observation
    # carrying its value, and an ex-date that moves away and comes back is not a second
    # first. Without this the walk emits one key twice under two provenances, neither
    # matches what ``latest`` resolves, and the ledger grows by two lines every night
    # forever, which is the failure the comparison below exists to prevent.
    emitted: set[ActionKey] = set()

    def hold(finding: Withheld) -> None:
        # The sequence is the caller's, because ``report`` has only module functions and a
        # counter there would be module state no test could drive. One run files under one
        # injected clock and one pid, so without it two findings on one subject would race
        # for one name. This walk cannot produce that race today, since it emits at most one
        # finding per ticker-day and the subject carries the day, so the index is the
        # contract being met rather than a collision being avoided. Mutating it to a
        # constant changes nothing any test here can see, and that is why the producer's own
        # tests are where the rule is held.
        try:
            filed_at = write_withheld(lake_root, finding, now=recorded_at, sequence=len(held))
        except OSError as exc:
            # Named on stderr and carried on the report, the way ``compact`` treats a
            # schema-drift file it could not write. The walk goes on, because one unwritable
            # file is not the other tickers' dividends to lose.
            print(
                f"actions: {finding.symbol} {finding.observed_on.isoformat()} "
                f"{finding.check} could not be filed: {type(exc).__name__}",
                file=sys.stderr,
            )
            held.append(
                HeldFinding(finding=finding, filed_at=None, filing_error=type(exc).__name__)
            )
            return
        held.append(HeldFinding(finding=finding, filed_at=filed_at))

    for ticker, days in by_ticker(ticker_days):
        # The last value this ticker was observed carrying, and the instrument it was
        # attributed to. Both, because either changing is a new thing to record.
        previous: tuple[int | None, str] | None = None
        for day in days:
            try:
                observation = _observation(lake_root, ticker, day)
            except ValueError as exc:
                hold(_payload_finding(ticker, day, exc))
                continue
            if isinstance(observation, str):
                skipped.append(Skip(ticker, day, observation))
                continue
            if observation is None:
                continue
            try:
                ex_date = normalize_date(observation[DIV_EX_DATE])
            except ValueError as exc:
                hold(_payload_finding(ticker, day, exc))
                continue

            # Resolved before the change test, because the instrument is half of what a
            # change is. Filing waits until the observation turns out to be one worth
            # emitting, so a master that cannot place a ticker files once rather than once
            # per ticker-day.
            failure: Exception | None = None
            instrument_id: int | None = None
            try:
                instrument_id = resolve_instrument(master, ticker, day)
            except (UnresolvedSymbol, AmbiguousSymbol) as exc:
                failure = exc

            mark = (instrument_id, ex_date)
            if mark == previous:
                continue
            previous = mark

            if failure is not None:
                ids = failure.instrument_ids if isinstance(failure, AmbiguousSymbol) else ()
                hold(_resolution_finding(ticker, day, failure, instrument_ids=ids))
                continue

            assert instrument_id is not None
            provenance = (
                PROVENANCE_OBSERVED if instrument_id in seen else PROVENANCE_VENDOR_REPORTED
            )
            seen.add(instrument_id)

            verdict = check_dividend_consistency(
                div_freq=observation[DIV_FREQ],
                div_pay_amount=observation[DIV_PAY_AMOUNT],
                div_amount=observation[DIV_AMOUNT],
            )
            if not verdict.agrees:
                hold(
                    Withheld(
                        symbol=ticker,
                        observed_on=day,
                        event=TYPE_DIVIDEND,
                        check=CHECK_DIVIDEND_CONSISTENCY,
                        computed=verdict.computed,
                        against=verdict.against,
                        instrument_id=instrument_id,
                    )
                )
                continue

            fields = {
                "instrument_id": instrument_id,
                "observed_on": day,
                "recorded_at": recorded_at,
                "ex_date": observation[DIV_EX_DATE],
                "type": TYPE_DIVIDEND,
                "pay_date": observation[DIV_PAY_DATE],
                "declared_date": observation[DECLARATION_DATE],
                "cash_amount": observation[DIV_PAY_AMOUNT],
                "provenance": provenance,
            }
            try:
                candidate = build_entry(**fields)
            except ValueError as exc:
                hold(_payload_finding(ticker, day, exc, instrument_id=instrument_id))
                continue
            key = (instrument_id, candidate["ex_date"], TYPE_DIVIDEND)
            if key in emitted:
                continue
            emitted.add(key)
            if same_but_for_recorded_at(current.get(key), candidate):
                unchanged += 1
                continue
            appended.append(Landed(entry=append(lake_root, **fields), symbol=ticker))

    return ExtractionReport(
        ticker_days=len(ticker_days),
        appended=tuple(appended),
        held=tuple(held),
        unchanged=unchanged,
        skipped=tuple(skipped),
    )


def read_master(lake_root: Path) -> SecurityMaster:
    """The master, read once before anything else, or the reason the run stops.

    ``resolve_instrument`` takes a ``SecurityMaster`` rather than a path, so reading it is a
    precondition of the walk rather than a step inside it.

    Public because ``lake.splits`` needs the same precondition on the same terms. The
    separation this encodes is a rule rather than a convenience: an absent master wants the
    onboarding command and a torn one wants a restore, and a second copy could keep one of
    the two and drop the other silently.
    """
    path = master_path(lake_root)
    try:
        return SecurityMaster.read(path)
    except FileNotFoundError as exc:
        raise MasterAbsent(path) from exc


def surface_ticker_days(lake_root: Path, surface: str) -> list[tuple[str, date]]:
    """Every sealed ticker-day of one surface the manifest records, in ticker then date order.

    The keys are read apart by ``paths.parse_partition_rel``, which inverts the builder that
    wrote them. ``paths.py`` is the single home for that, and its reason is the one that
    would bite here: a second reader that re-implements the split drifts from the builder and
    nothing catches it, because this walk passes over a key it cannot read rather than
    raising on one.

    A key naming any other surface is passed over, along with both ledgers and every
    reference table. ``surface`` is a parameter rather than a constant because two walks
    enumerate this way and they read different surfaces. The dividend extraction below reads
    quotes and ``lake.splits`` reads chains, and a second copy of this would be a second
    place the partition-key convention lives.
    """
    found: list[tuple[str, date]] = []
    for partition in latest_entries(lake_root):
        reference = parse_partition_rel(partition)
        if reference is None or reference.surface != surface:
            continue
        found.append((reference.ticker, reference.day))
    return sorted(found)


def by_ticker(ticker_days: Sequence[tuple[str, date]]) -> Iterator[tuple[str, list[date]]]:
    """The same ticker-days grouped by ticker, each ticker's sessions in date order.

    The partitions are keyed by ticker, so that is what the sessions arrive grouped by. The
    instrument enters the comparison rather than the grouping, one level down, which is what
    gives a symbol handed between two instruments a first observation under each. Grouping
    here on the instrument instead would mean resolving before reading, and a master that
    could not place a symbol would then hold one finding per ticker-day for a condition that
    has one action behind it.

    Public because ``lake.splits`` groups the same way over the chains surface.
    """
    grouped: dict[str, list[date]] = {}
    for ticker, day in ticker_days:
        grouped.setdefault(ticker, []).append(day)
    for ticker in sorted(grouped):
        yield ticker, sorted(grouped[ticker])


def _observation(lake_root: Path, ticker: str, day: date) -> dict[str, object] | str | None:
    """One ticker-day's dividend fields, ``None`` when it carries no event, or the reason it
    was not read.

    The rows are the session's equity close of record. Every data row in a session carries
    the same fundamentals, so which minute answers decides nothing about the dividend.

    The close of record is one cycle, which the loader enforces, and it is still more than
    one row when the partition holds two spellings of that one instant. Those rows have to
    agree about the dividend, and a disagreement raises rather than taking the first one.
    Taking the first would let the file's own order decide which dividend the ledger gets,
    silently, and the caller holds and files what this raises.

    A column the partition does not carry reads as null rather than raising, so a session
    sealed before a dividend column existed is a ticker-day with no event rather than a run
    that ends.

    **The four named refusals return a reason and the caller counts them**, which is the
    shape ``splits.read_session`` already has over the sibling surface. A reason rather than
    ``None`` because the caller has to tell them apart: a ticker-day carrying no ex-date is a
    non-paying instrument reporting nothing, and a ticker-day the battery withheld is a
    session the run could not see. Collapsing the two would file a withheld partition under
    the count that means the instrument pays no dividend.

    The containment is here rather than in the caller's loop or in ``lake.sweep``. A refusal
    caught at the sweep ends the walk, and the walk is ordered by ticker, so it would cost
    every ticker after the refused one its dividends. ``actions.main`` has no catch for a
    ``LoadError`` either, so an escaping refusal reaches the operator as a traceback and
    takes the sign-off block for every ticker that did land with it.

    **What is deliberately not caught here, so nothing reads this list as exhaustive.**
    ``LoadError`` has twelve direct subclasses. Most belong to doors this never opens, and two
    of them, ``SnapMalformed`` and ``SnapAbsent``, come from resolving a ``snap`` argument,
    which this passes none of because it resolves at the close of record.

    ``LoadError`` itself is the one that matters, and ``_load_surface`` raises it bare at
    three sites a snapless read reaches: rows carrying no ``row_kind``, a ``spot_close`` row
    whose ``snap_ts`` cannot be read as an instant, and ``spot_close`` tagged on more than one
    cycle. Each escapes this walk today, and each takes the 18:30 job with it, which is the
    same cost marketlake #352 was opened for.

    It is left rather than folded in, because it is a different statement. The four above say
    a session cannot be read, which costs one ticker-day. These three say the lake's own files
    contradict their writers, which is something a person has to go and look at, so the answer
    is a held finding rather than a counted skip. Marketlake #365 owns that decision for both
    walks, holds the same shape for ``ExtraProjectionError`` and ``ArrowInvalid``, and
    ``lake.bars`` is its precedent, catching ``LoadError`` by the base and filing.
    """
    try:
        table = load_quotes(ticker, day, lake_root=lake_root)
    except NoSpotClose:
        return REASON_NO_SPOT_CLOSE
    except PartitionQuarantined:
        return REASON_QUARANTINED
    except PartialRead:
        return REASON_PARTIAL_READ
    except PartitionAbsent:
        return REASON_PARTITION_ABSENT
    present = set(table.column_names)
    values: dict[str, object] = {}
    for column in DIVIDEND_COLUMNS:
        if column not in present:
            values[column] = None
            continue
        spellings = {value for value in table.column(column).to_pylist()}
        if len(spellings) > 1:
            raise ValueError(
                f"the {day.isoformat()} close of record disagrees about {column}, "
                f"among {sorted(str(value) for value in spellings)}"
            )
        values[column] = spellings.pop()
    return None if values[DIV_EX_DATE] is None else values


def _payload_finding(
    ticker: str,
    day: date,
    exc: Exception,
    *,
    instrument_id: int | None = None,
) -> Withheld:
    """The finding a payload the record rules refuse files.

    Three shapes reach here and each one used to end the run as a traceback: a vendor date
    that does not name a date, an amount ``append`` will not take such as a negative one, and
    a close whose rows disagree with each other. None of them is this run's to repair, and a
    run that dies on one loses every ticker it had not reached yet.
    """
    return Withheld(
        symbol=ticker,
        observed_on=day,
        event=TYPE_DIVIDEND,
        check=CHECK_DIVIDEND_PAYLOAD,
        instrument_id=instrument_id,
        exception=f"{type(exc).__name__}: {exc}",
    )


def _resolution_finding(
    ticker: str,
    day: date,
    exc: Exception,
    *,
    instrument_ids: Sequence[int] = (),
) -> Withheld:
    """The finding a resolution failure files.

    The exception is rendered as its class and then its message, because ``write_withheld``
    composes ``<symbol>: <exception>`` and keeps the first two fields. Handing over the bare
    message would file that message's own first field instead, which for an ``OSError`` is a
    path on the capture machine.
    """
    return Withheld(
        symbol=ticker,
        observed_on=day,
        event=TYPE_DIVIDEND,
        check=CHECK_INSTRUMENT_RESOLUTION,
        instrument_ids=tuple(instrument_ids),
        exception=f"{type(exc).__name__}: {exc}",
    )


def same_but_for_recorded_at(existing: dict | None, candidate: dict) -> bool:
    """Whether the ledger already holds this entry, ignoring when it was written down.

    ``recorded_at`` is the clock's answer and moves every night while nothing else does, so
    comparing whole entries would append a dividend the ledger already holds, every night,
    forever. This is the rule marketlake #139 states for the quarantine ledger: read the
    current entry first, and a re-observation of the same finding supersedes nothing.

    Public because ``lake.splits`` needs the same comparison and a second copy of it could
    drift from this one without anything noticing. What the two writers do differ on is what
    they stamp into ``observed_on``, and that is theirs rather than this rule's: a split
    stays visible in sealed chains forever, so a detector stamping the night it ran would
    fail this comparison every night and append the same split every night.
    """
    if existing is None:
        return False
    fields = set(existing) | set(candidate)
    return all(existing.get(name) == candidate.get(name) for name in fields - {"recorded_at"})


# -- the entry point ---------------------------------------------------------


def extract_dividends_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
) -> ExtractionReport:
    """The extraction wired from the real config. This is the entry :func:`main` calls."""
    from lake.config import load_config

    config = load_config(config_path)
    return extract_dividends(
        lake_root=config.lake_root,
        clock=SystemClock() if clock is None else clock,
    )


# The subcommand that reads splits out of sealed chains. ``lake.splits`` owns the walk and
# this command owns the invocation, so the name is fixed here beside the parser that takes it.
SPLITS_COMMAND = "splits"
# The subcommand that names the dividend extraction explicitly. It is what the bare command
# already does, so this is a name for the default rather than a second behaviour.
DIVIDENDS_COMMAND = "dividends"


def _build_parser():
    """The parser, with the dividend extraction as the default and no subcommand required.

    Two walks now write this ledger. The dividend extraction reads sealed quotes and
    ``lake.splits`` reads sealed chains, and marketlake #286's manual entry will be a third.
    So the command grows subcommands, and which one is the default was a choice with a
    measured price behind it.

    Requiring one would move seven component call sites of ``main(["--config", ...])`` and a
    sentence in ``docs/design.md`` that documents the extraction as running as
    ``python -m lake.actions``. Making the extraction the default moves neither, and it costs
    nothing a reader can see, because :data:`DIVIDENDS_COMMAND` names the default out loud
    for anyone who would rather write it than rely on it.

    ``--config`` is accepted on either side of the subcommand. Each subparser declares it
    with a default of ``argparse.SUPPRESS``, so a subcommand that does not carry the flag
    sets no attribute and the top-level value survives. Without that, the subparser's own
    ``None`` default would overwrite a ``--config`` written before the subcommand, silently.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.actions",
        description=(
            "Read corporate actions out of rows the lake has already sealed, gate each one, "
            "and append what lands to the corporate-actions ledger. Nothing here fetches. "
            "With no subcommand this runs the dividend extraction."
        ),
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    subcommands = parser.add_subparsers(dest="command")
    for name, help_text in (
        (DIVIDENDS_COMMAND, "Read dividends out of the lake's sealed quote rows. The default."),
        (
            SPLITS_COMMAND,
            "Read splits out of the lake's sealed chains: the OCC re-symboling, and the "
            "strike ladder against the session's spot for the whole-ratio split that "
            "re-symbols nothing.",
        ),
    ):
        subcommand = subcommands.add_parser(name, help=help_text, description=help_text)
        subcommand.add_argument(
            "--config",
            default=argparse.SUPPRESS,
            help="Path to config.yaml (defaults to the standard location).",
        )
    return parser


def main(argv: Sequence[str] | None = None, *, clock: Clock | None = None) -> int:
    """The ``python -m lake.actions`` entry. Returns a process exit code.

    ``clock`` stays injectable, for the reason ``alert.main`` gives: a wall clock never
    reaches past this process. It is also what makes a test of this command writable, since
    what "a second night" means has to be something the test decides.

    **The bare command runs the dividend extraction.** ``splits`` runs the detection over
    sealed chains instead. The two walks read different surfaces and produce different report
    shapes, and each renders its own. Everything below is the same for both, because the three
    exit codes describe the run rather than which walk made it.

    **A lake with no security master reaches the operator as one line, not a stack.**
    ``input_errors_exit`` covers the three files in the config directory that are the
    operator's to edit, and an unseeded lake is the same kind of mistake with a different
    file behind it. The two ways the master can fail say different things, because an absent
    master wants the onboarding command and a torn one wants a restore. An operator told to
    seed a corrupt file is being told the wrong thing.

    **A finding the run could not write down is what the third exit code is for.** The walk
    contains that failure so one unwritable file does not cost the other tickers their
    dividends, and this is where it stops being silent. A run that held something and filed
    it is a live condition a human can go and read. A run that held something and filed
    nothing reads exactly like a run that found nothing, which is the silence the producer
    exists to break, so it exits 1. Exit 2 stays what it is everywhere else here, an operator
    mistake with a fix behind it.
    """
    args = _build_parser().parse_args(argv)

    from lake.config import input_errors_exit

    # Local, so ``lake.splits`` can import this module at its own top level. The walk it
    # holds reads ``append``, the vocabulary constants, ``Skip`` and five helpers from here, and
    # the import direction only stays one-way because this end of it waits until it runs.
    if args.command == SPLITS_COMMAND:
        from lake.splits import detect_splits_from_config as run
    else:
        run = extract_dividends_from_config

    try:
        with input_errors_exit("actions"):
            report = run(clock=clock, config_path=args.config)
    except MasterAbsent as exc:
        print(
            f"actions: {exc}. Onboard a ticker first, with python -m lake.onboard <TICKER>.",
            file=sys.stderr,
        )
        return 2
    except MasterUnreadable as exc:
        print(f"actions: {exc}. Restore it from the backup.", file=sys.stderr)
        return 2
    print(report.render())
    return 1 if report.unfiled else 0


__all__ = [
    "ACTIONS_PARTITION",
    "ACTIONS_SCHEMA_VERSION",
    "ACTION_TYPES",
    "CHECK_DIVIDEND_CONSISTENCY",
    "CHECK_DIVIDEND_PAYLOAD",
    "CHECK_INSTRUMENT_RESOLUTION",
    "DIVIDENDS_COMMAND",
    "DIVIDEND_CONSISTENCY_TOLERANCE",
    "PROVENANCES",
    "PROVENANCE_MANUAL",
    "PROVENANCE_OBSERVED",
    "PROVENANCE_VENDOR_REPORTED",
    "REASON_NO_SPOT_CLOSE",
    "REASON_PARTIAL_READ",
    "REASON_PARTITION_ABSENT",
    "REASON_QUARANTINED",
    "SPLITS_COMMAND",
    "SWEEP_SOURCE",
    "TYPE_DIVIDEND",
    "TYPE_SPLIT",
    "ActionKey",
    "ActionsError",
    "DividendConsistency",
    "ExtractionReport",
    "HeldFinding",
    "Landed",
    "LedgerLineError",
    "MasterAbsent",
    "Skip",
    "UnresolvedSymbol",
    "actions_path",
    "append",
    "as_of",
    "build_entry",
    "by_reason",
    "by_ticker",
    "check_dividend_consistency",
    "entry_key",
    "entry_line_count",
    "extract_dividends",
    "extract_dividends_from_config",
    "latest",
    "main",
    "normalize_date",
    "read",
    "read_master",
    "resolve_instrument",
    "same_but_for_recorded_at",
    "surface_ticker_days",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
