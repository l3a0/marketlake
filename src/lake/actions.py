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
   ``append_line`` starts a new line when the file does not end in one, so a torn fragment
   stays its own line rather than swallowing the bytes of the next entry. What the
   fragment still costs is every entry after it, because the read stops at the first line
   it cannot parse. That is the manifest's rule rather than this ledger's, and the
   manifest entry's row count is what makes the loss visible: it counts the file's lines,
   so a count above what :func:`read` returns says the file is damaged.
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

**Nothing in this module writes an entry of its own.** The dividend extraction and the
validation that append the first one are marketlake #284, and the command a human writes a
``manual`` entry with is #286. What ships here is the record format, the writer,
and the two reads that resolve it.

``recorded_at`` is injected, never read from a wall clock. ``manifest.py`` states that rule
for itself and every writer in the lake follows it. It is what lets the suite run offline
and deterministically.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from math import isfinite
from pathlib import Path

from lake.calendar import MARKET_TZ
from lake.manifest import append_line, parse_jsonl, record_partition
from lake.paths import ACTIONS, CORPORATE_ACTIONS_FILE
from lake.security_master import SecurityMaster

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
    reads as no entries, which is what keeps every reader inert until #284 appends the
    first one.

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
    entry = _build_entry(
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


def _build_entry(
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
    is, so a caller assembling one can be checked without a lake on disk.

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


__all__ = [
    "ACTIONS_PARTITION",
    "ACTIONS_SCHEMA_VERSION",
    "ACTION_TYPES",
    "PROVENANCES",
    "PROVENANCE_MANUAL",
    "PROVENANCE_OBSERVED",
    "PROVENANCE_VENDOR_REPORTED",
    "SWEEP_SOURCE",
    "TYPE_DIVIDEND",
    "TYPE_SPLIT",
    "ActionKey",
    "ActionsError",
    "LedgerLineError",
    "UnresolvedSymbol",
    "actions_path",
    "append",
    "as_of",
    "entry_key",
    "entry_line_count",
    "latest",
    "normalize_date",
    "read",
    "resolve_instrument",
]
