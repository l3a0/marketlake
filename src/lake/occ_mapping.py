"""The OCC mapping row: threading a re-symboled contract through the security master.

``docs/design.md`` promises that "a rename or an OCC re-symboling then adds a mapping row
instead of orphaning history. The same instrument threads through under one key." Until
this module, nothing kept that promise. ``SecurityMaster.remap`` over ``ID_TYPE_OCC`` was
the operation and every caller was a test, and the live master held two rows, both
``id_type`` ``ticker``, both equity, and no option instrument at all.

``lake.splits`` detects the boundary and this writes what the boundary means for identity.
The two surfaces are separate on purpose. The ledger records a split, which is a ratio, and
the master records which contract is which, which is not. So this fires on cases the ledger
refuses and records nothing the ledger holds.

**The unit is one contract, not one detection.** ``ID_TYPE_OCC`` names one OCC option
symbol, which names one contract, and both consumers of these rows resolve a contract
symbol. So a re-symboling writes per contract it touched rather than per boundary. A
contract the master has never held takes two rows, its old symbol closed at the boundary
and the new one open from it. One the master already holds takes a single row on the end
of the chain it already has. The detector's own
``instrument_id`` is the *equity*, resolved from the partition's ticker, and an OCC mapping
hung on that would tie a contract symbol to the underlying.

**``remap`` closes an open mapping, so something has to open one first.** Nothing registers
option instruments, so a bare ``remap`` over ``ID_TYPE_OCC`` raises
``SecurityMasterError: instrument 1 has no open occ_symbol mapping to remap`` against the
live master. This registers the contract under its old symbol and then remaps it, and a
contract adjusted a second time already has an instrument, so registering again rather than
remapping the one it has would split its life across two ids and make the symbol resolve
ambiguously. That is the orphaning the master exists to prevent, so the write branches on
whether the old symbol already names an instrument.

**``ssid`` is what pairs the old symbol to the new one.** It is Schwab's own contract
identifier and ``CHAINS_SCHEMA`` carries it. Measured across four consecutive session pairs,
SPY and QQQ over 2026-09-14, 09-15 and 09-16: every session's values are distinct, and of
the 47,368 contracts carried from one session into the next, not one changed its
``occ_symbol``. So ``ssid`` names the contract and ``occ_symbol`` names its spelling.

Whether the vendor carries ``ssid`` through an adjustment cannot be measured here, because
the lake holds no adjusted contract. So :class:`SymbolHistory` refuses rather than guesses,
and the refusal is a confirmed boundary that pairs *zero* contracts. A gained root with no
contract carrying a new symbol contradicts the change that confirmed it. Pairing on the
contract's terms instead, when that refusal arrives, is marketlake #369.

**Every contract under the gained root has to be accounted for.** An OCC adjustment
re-symbols every open contract at once, so a boundary where only some of them pair is
evidence that the pairing is wrong rather than a boundary that is partly readable. Executed
against ten contracts where the vendor carried ``ssid`` for one and minted fresh identifiers
for the other nine, an earlier draft of this module wrote one mapping row, refused nothing and
filed nothing, leaving nine contracts orphaned in silence.

The price is named rather than hidden. A contract genuinely listed new under the adjusted root
*on the boundary session itself* refuses the boundary too, and so does a row carrying no
``ssid`` at all, which is what the lake's 2026-09-02 partition holds. That is deliberate. A new
listing and a contract whose identifier the vendor did not carry through look identical from
here and they want opposite treatments, so this fails closed the way every other unanswerable
question in this module does. A refused boundary files a finding a human reads. A partly
written one is silent.

**Almost no contract resolves through the master, and that is the right answer.** Only a
contract a re-symboling touched gets an instrument here, so an ordinary contract's symbol
answers ``None`` and keeps doing so. A reader keys on the ``instrument_id`` where there is
one and on the symbol where there is not.

**Two collisions are guarded and ``remap`` guards neither.** Remapping an instrument onto a
symbol another instrument already holds open is accepted without complaint, and ``resolve``
on that symbol then raises ``AmbiguousSymbol`` forever, which is the state the master calls
corrupt. So a pair whose old symbol names more than one instrument is refused, and so is one
whose new symbol already names an instrument other than the one being remapped.

**The write re-reads the master inside the lock it writes under.** ``lake.splits`` reads the
master once before its walk, and writing that stale snapshot would discard an onboarding that
ran during the walk. The hold is separate rather than nested, because ``actions.append``
takes the same lake-root lock itself and ``lake_lock`` is a plain blocking ``LOCK_EX`` with
no reentrancy, so re-entering it in one process blocks forever.

**The manifest entry beside the write is not optional.** The master already has one from
onboarding, so a rewrite that records nothing fails the integrity scrub's *forward* pass,
which compares the file's sha against the last recorded entry. Executed on a fixture lake, a
rewrite with no ``record_partition`` leaves
``sha_mismatches=('reference/security_master.parquet',)``.

**A second run writes nothing.** A pair whose old symbol already names an instrument holding
the new symbol as its open mapping is skipped, and a boundary whose pairs are all skipped
rewrites no file and records no entry. That check cannot ride the ledger's: a run that landed
the entry and failed the master write would never come back to it, because
``actions.same_but_for_recorded_at`` suppresses the second append. Without it a second night
raises, because ``remap`` requires ``effective`` to fall strictly after the open mapping's
``valid_from`` and the row the first night opened begins on that very date.

**``valid_from`` is the first session the walk read the contract under the old symbol.** The
previous session's date would break the thread for every session before it, which is the
whole point of threading. The equity's ``capture_start`` would over-claim, and over-claiming
is not free: an OCC symbol the market re-issues gets a second instrument whose backdated
range overlaps the first's, and the symbol then resolves to both. The observed first session
is both true and the tightest honest claim.

**A boundary that lands twice under two different dates is refused rather than repaired.**
``ex_date`` sits in the ledger's key, so a corrected date there lands under a second key. The
master is rewritten whole, so in principle a row could be moved, and this does not move one.
Executed: a first run that dated a boundary 09-16 and a second that re-derived it at 09-15 left
the ledger holding both dates and the master holding only the first, with nothing saying so and
``resolve`` answering the old symbol on a day the sealed chains already carried the new one. So
the second date is refused and filed, naming both, rather than one silently winning.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from lake.security_master import (
    ID_TYPE_OCC,
    KIND_OPTION,
    SecurityMaster,
    master_path,
)

# The check name a refused mapping files under. It names what refused rather than what was
# refused, the way ``lake.actions``'s and ``lake.splits``'s do, because the finding already
# carries the event.
CHECK_OCC_MAPPING = "occ_mapping"


class MappingError(Exception):
    """Base class for this module's own refusals."""


class MappingRefused(MappingError):
    """Raised when a boundary's mapping rows cannot be written from what the data says.

    Every case is a claim the master would carry and nobody observed: a contract under the
    gained root the walk cannot account for, a symbol two instruments already hold, a new
    symbol that already names a different contract, an instrument that has moved past this
    boundary already, or one boundary dated two different ways. A refused boundary is filed as
    a finding and the walk goes on. A mapping withheld today can be written tomorrow, while a
    wrong one is read as truth by every consumer that resolves through it.
    """


class ManifestNotRecorded(MappingError):
    """Raised when the mapping rows were written and the manifest entry beside them was not.

    This one says the opposite of every other refusal here: the master on disk is correct and
    the lake's record of it is stale, so the integrity scrub's forward pass reports a sha
    mismatch. Naming it apart matters because the finding an operator reads would otherwise
    say the mapping failed, and the repair is the opposite of a retry.

    Nothing here repairs it, and a later run cannot: the pairs are already written, so the
    idempotence check skips them and the entry is never recorded. ``onboard.py``, ``retire.py``
    and ``seed_spans.py`` carry the same window, and marketlake #371 owns the repair for all
    four.
    """


@dataclass(frozen=True)
class Pair:
    """One contract's symbol change, as the history and the boundary session state it."""

    ssid: int
    old_symbol: str
    valid_from: date
    new_symbol: str


@dataclass(frozen=True)
class Pairing:
    """What a boundary session's gained contracts came to against the walk's history.

    ``rows`` counts every row under those roots and ``recognised`` counts the ones the walk
    has read before, and the two together are what separate a boundary this can read from one
    it cannot. They have to be equal. An OCC adjustment re-symbols every open contract at
    once, so a contract under the gained root that the walk cannot place is evidence the
    pairing is wrong rather than one contract to pass over.

    With that settled, an empty ``pairs`` means exactly one thing: every contract under the
    gained root is one the walk knows and none changed its symbol, so the vendor's root column
    moved and identity did not. Nothing to record, and nothing to refuse either.
    """

    rows: int
    recognised: int
    pairs: tuple[Pair, ...]

    @property
    def unaccounted(self) -> int:
        """Contracts under the gained roots the walk cannot place, which must be none."""
        return self.rows - self.recognised


@dataclass(frozen=True)
class Remapped:
    """One mapping the write actually made, for the run's sign-off block."""

    ticker: str
    instrument_id: int
    old_symbol: str
    new_symbol: str
    valid_from: date
    effective: date


class SymbolHistory:
    """Each contract's current symbol and the first session the walk read it under that one.

    Keyed on ``ssid`` rather than on the symbol, because the symbol is the thing that moves.
    Three answers come out of one structure rather than three: which contract's symbol
    changed, what ``valid_from`` the old mapping takes, and whether a contract is one the
    walk has seen at all.

    It is per instrument. :meth:`reset` is called when the walk's instrument changes, because
    a different security's contracts are a different history.

    It is updated *after* a session is examined, beside the roots ``lake.splits`` remembers
    and for the same reason: a boundary is judged against the history as it stood before that
    session. A contract absent from the session immediately before a boundary is still paired,
    which is what reading only the previous session would lose.
    """

    def __init__(self) -> None:
        self._seen: dict[int, tuple[str, date]] = {}

    def __len__(self) -> int:
        return len(self._seen)

    def reset(self) -> None:
        """Forget everything. The walk calls this when the instrument changes."""
        self._seen.clear()

    def observe(self, day: date, rows: Iterable[dict[str, object]]) -> None:
        """Record what each contract's symbol is as of ``day``.

        A contract whose symbol is unchanged keeps the session it was first read under. A
        contract whose symbol moved starts a fresh range at ``day``, which is what makes the
        next re-symboling of the same contract date from the right session.
        """
        for ssid, symbol in _identified(rows):
            current = self._seen.get(ssid)
            if current is None or current[0] != symbol:
                self._seen[ssid] = (symbol, day)

    def inspect(self, rows: Iterable[dict[str, object]]) -> Pairing:
        """What ``rows`` carry against this history: how many, how many it knows, which moved.

        ``rows`` are the boundary session's rows under the roots it gained. Every one of them
        is counted, including a row this cannot read at all, because a row carrying no ``ssid``
        is a contract the walk cannot place just as surely as one carrying an ``ssid`` it has
        never seen. :class:`Pairing` is where the two counts are compared.
        """
        counted = 0
        recognised = 0
        found = []
        for counted, (ssid, symbol) in enumerate(_readable(rows), start=1):  # noqa: B007
            current = None if ssid is None else self._seen.get(ssid)
            if current is None:
                continue
            recognised += 1
            if current[0] == symbol:
                continue
            found.append(
                Pair(ssid=ssid, old_symbol=current[0], valid_from=current[1], new_symbol=symbol)
            )
        return Pairing(
            rows=counted,
            recognised=recognised,
            pairs=tuple(sorted(found, key=lambda pair: (pair.old_symbol, pair.new_symbol))),
        )


def _identified(rows: Iterable[dict[str, object]]) -> Iterable[tuple[int, str]]:
    """Every row naming both a contract and a symbol. This is what the history records."""
    for ssid, symbol in _readable(rows):
        if ssid is not None:
            yield ssid, symbol


def _readable(rows: Iterable[dict[str, object]]) -> Iterable[tuple[int | None, str]]:
    """Every row naming a symbol, with the contract it names or ``None``.

    A row with no symbol has nothing to map and is not a contract this can count either. A
    null ``ssid`` is different: the row is a contract and this cannot say which one, which is
    what :class:`Pairing` counts as unaccounted. The lake's 2026-09-02 partition carries null
    there on both of its rows, the same partition ``option_root`` is null on.
    """
    for row in rows:
        ssid = row.get(_SSID)
        symbol = row.get(_OCC_SYMBOL)
        if isinstance(symbol, str) and symbol:
            yield (ssid if isinstance(ssid, int) else None), symbol


# The two columns this module reads off a session's rows. ``lake.splits`` owns the list they
# are read into, and these are the names it uses.
_SSID = "ssid"
_OCC_SYMBOL = "occ_symbol"


def instruments_holding(master: SecurityMaster, symbol: str) -> set[int]:
    """Every instrument holding this OCC symbol, on any date.

    The whole table rather than an as-of resolution, and deliberately. ``resolve`` honours
    validity ranges, so asking it on one date can miss the very mapping the write should
    extend, when that mapping opened later than the walk's own earliest readable session. The
    question here is whether the symbol has an instrument at all, which is the same question
    ``splits._in_master`` answers by scanning the rows directly.
    """
    return {
        mapping.instrument_id
        for mapping in master.mappings
        if mapping.id_type == ID_TYPE_OCC and mapping.id_value == symbol
    }


def opens_on(master: SecurityMaster, instrument_id: int, symbol: str) -> date | None:
    """The date this instrument's mapping of ``symbol`` opens on, or ``None`` if it has none.

    Two questions ride on it. A row already opening on this boundary's own date is the write
    already done, which is what makes a second night a no-op. A row opening on a *different*
    date is one boundary dated two ways, and nothing here moves a row, so that is refused.
    """
    for mapping in master.mappings:
        if (
            mapping.instrument_id == instrument_id
            and mapping.id_type == ID_TYPE_OCC
            and mapping.id_value == symbol
        ):
            return mapping.valid_from
    return None


def open_symbol(master: SecurityMaster, instrument_id: int) -> str | None:
    """The OCC symbol this instrument currently carries, or ``None`` when it carries none.

    This is the idempotence check's own question. ``symbol_at`` answers as of a date and
    would need one invented for it, where an open mapping is open regardless of date.
    """
    for mapping in master.mappings:
        if (
            mapping.instrument_id == instrument_id
            and mapping.id_type == ID_TYPE_OCC
            and mapping.valid_to is None
        ):
            return mapping.id_value
    return None


def write_mappings(
    lake_root: Path | str,
    *,
    ticker: str,
    instrument_id: int,
    effective: date,
    pairing: Pairing,
    recorded_at: datetime,
) -> tuple[Remapped, ...]:
    """Write one boundary's mapping rows, under one hold of the lake-root lock.

    ``instrument_id`` is the equity's, and it is read for one thing only: the
    ``capture_start`` epoch each option instrument copies forward. The master stores ``kind``
    and ``capture_start`` on every row so the file stays self-describing, and a contract was
    first recorded as part of its underlying's capture.

    Returns what was written. An empty result means there was nothing to write, either
    because no contract's symbol moved or because every pair was already in the master, and
    then no file is rewritten and no manifest entry is appended. That is what makes a second
    night a no-op rather than a rewrite with the same bytes.

    Raises :class:`MappingRefused` for a boundary the master cannot take honestly. Nothing
    partial reaches disk: the whole boundary is applied to the in-memory master first, and
    the file is written once at the end.
    """
    # Local, for the reason ``onboard``, ``retire`` and ``seed_spans`` give for the same
    # import: this module stays free of the lock and of the manifest unless it writes.
    from lake.actions import read_master
    from lake.lock import lake_lock
    from lake.manifest import record_partition
    from lake.onboard import MASTER_PARTITION, REFERENCE_SOURCE

    root = Path(lake_root)
    if pairing.unaccounted:
        raise MappingRefused(
            f"{ticker} gained a root on {effective.isoformat()} carrying {pairing.rows} "
            f"contract(s), and the walk cannot place {pairing.unaccounted} of them. An "
            f"adjustment re-symbols every open contract at once, so a boundary it can only "
            f"partly read is one it has read wrong"
        )
    incoming = [pair.new_symbol for pair in pairing.pairs]
    if len(set(incoming)) != len(incoming):
        raise MappingRefused(
            f"{ticker}'s {effective.isoformat()} boundary re-symbols two contracts onto one "
            f"symbol, which no instrument can carry"
        )
    crossed = sorted(set(incoming) & {pair.old_symbol for pair in pairing.pairs})
    if crossed:
        raise MappingRefused(
            f"{ticker}'s {effective.isoformat()} boundary both opens and closes {crossed}, "
            f"so one symbol would name two contracts' histories"
        )
    if not pairing.pairs:
        # Every contract under the gained root is one the walk knows and none changed its
        # symbol, so the root column moved and identity did not. Nothing to record.
        return ()

    with lake_lock(root):
        # Re-read inside the lock. The walk's own snapshot is as old as the walk, and
        # writing it would discard anything onboarding registered while the walk ran.
        master = read_master(root)
        capture_start = master.capture_start_of(instrument_id)
        written: list[Remapped] = []
        for pair in pairing.pairs:
            owners = instruments_holding(master, pair.old_symbol)
            if len(owners) > 1:
                raise MappingRefused(
                    f"{pair.old_symbol!r} already names instruments {sorted(owners)}, which "
                    f"is a master that cannot say which contract it is"
                )
            owned = next(iter(owners), None)
            if owned is not None:
                opened = opens_on(master, owned, pair.new_symbol)
                if opened == effective:
                    # Already written, by an earlier night or by a run that got this far and
                    # then failed. Asking for the row rather than for the instrument's *open*
                    # symbol is what makes this hold on a contract adjusted twice, where the
                    # first boundary's mapping is no longer the open one. Executed: the
                    # instrument's-open-symbol form filed a false finding every night forever
                    # once any contract had two boundaries behind it.
                    continue
                if opened is not None:
                    raise MappingRefused(
                        f"{pair.new_symbol!r} already opens on {opened.isoformat()} under "
                        f"instrument {owned} and this boundary dates it "
                        f"{effective.isoformat()}. Nothing here moves a mapping row, so the "
                        f"two dates are reported rather than one quietly winning"
                    )
                carried = open_symbol(master, owned)
                if carried != pair.old_symbol:
                    raise MappingRefused(
                        f"{pair.old_symbol!r} names instrument {owned}, which now carries "
                        f"{carried!r}. Either the market re-issued the symbol to another "
                        f"contract or that instrument has already moved past this boundary, "
                        f"and the symbol alone cannot tell the two apart"
                    )
            others = instruments_holding(master, pair.new_symbol) - (
                set() if owned is None else {owned}
            )
            if others:
                raise MappingRefused(
                    f"{pair.new_symbol!r} already names instrument(s) {sorted(others)}, so "
                    f"remapping {pair.old_symbol!r} onto it would make the symbol ambiguous"
                )
            if owned is None:
                owned = master.register(
                    kind=KIND_OPTION,
                    capture_start=capture_start,
                    valid_from=pair.valid_from,
                    occ_symbol=pair.old_symbol,
                )
            master.remap(owned, ID_TYPE_OCC, pair.new_symbol, effective=effective)
            written.append(
                Remapped(
                    ticker=ticker,
                    instrument_id=owned,
                    old_symbol=pair.old_symbol,
                    new_symbol=pair.new_symbol,
                    valid_from=pair.valid_from,
                    effective=effective,
                )
            )

        if not written:
            return ()

        master.write(master_path(root))
        try:
            record_partition(
                root,
                MASTER_PARTITION,
                source=REFERENCE_SOURCE,
                rows=len(master),
                fetched_at=recorded_at.isoformat(),
            )
        except Exception as exc:
            # The rows are on disk and the lake's record of them is not, which is the
            # opposite of every other failure here and wants the opposite of a retry.
            raise ManifestNotRecorded(
                f"{ticker}'s {effective.isoformat()} mapping rows were written and the "
                f"manifest entry was not, so the integrity scrub reports a sha mismatch "
                f"until one is recorded: {type(exc).__name__}: {exc}"
            ) from exc
    return tuple(written)


__all__ = [
    "CHECK_OCC_MAPPING",
    "ManifestNotRecorded",
    "MappingError",
    "MappingRefused",
    "Pair",
    "Pairing",
    "Remapped",
    "SymbolHistory",
    "instruments_holding",
    "open_symbol",
    "opens_on",
    "write_mappings",
]
