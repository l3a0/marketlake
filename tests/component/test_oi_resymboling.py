"""``oi_view`` across an OCC re-symboling, which the lake holds no example of.

Component tier, like ``test_oi_view.py``, and it borrows that file's fixture builders rather
than restating them. What it adds is a security master carrying option instruments, which no
test in that file has, because every one of them registers ``SPY`` as an equity and nothing
else. That difference is the whole point: those 45 tests are what prove the empty-index path
still answers as it did, and these prove the threaded path answers at all.

**Every test here is a replay, and it has to be.** The lake holds 29,718,244 chain rows
across 15 sealed chains partitions on 8 dates, every SPY session carries the one root
``SPY``, and of the 12,646 contracts carried from 2026-09-14 into 2026-09-15 and the 12,790
carried into 2026-09-16, not one changed its ``occ_symbol``. So no real adjustment exists to
assert against. marketlake #279's detector is in the same position and was exercised the same
way.

The adjusted spelling is the real one. On a split the OCC leaves the contract's expiration,
right and strike alone and moves the root, so ``SPY`` becomes ``SPY1``, and the six-character
root field is what absorbs the extra character.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from lake.capture_spans import CaptureSpans
from lake.oi import (
    REASON_NO_VALUE_IN_CYCLE,
    VERDICT_ABSENT,
    VERDICT_INDETERMINATE,
    VERDICT_SETTLED,
    OiViewError,
    ScopeUnreadable,
    SpellingsCollide,
    oi_view,
)
from lake.security_master import ID_TYPE_OCC, Mapping, SecurityMaster
from tests.component.test_oi_view import (
    CALENDAR,
    EPOCH,
    FOLLOWING,
    SESSION,
    SET,
    VOLUMES,
    close_rows,
    constants,
    cycle_rows,
    ledger_table,
    table,
)
from tests.support.lake import FixtureLake

# The boundary. S is 2026-09-14 and the session walked is 2026-09-15, so an adjustment
# effective on the walked session is one S itself never saw.
BOUNDARY = date.fromisoformat(FOLLOWING)
FIRST_SEEN = date.fromisoformat(SESSION)


def adjusted(symbol: str) -> str:
    """The same contract under an adjusted root, which is what the OCC issues on a split."""
    return "SPY1 " + symbol[6:]


def master_with(*, remapped: tuple[str, ...] = (), extra: tuple[tuple[int, str], ...] = ()):
    """The equity, plus one option instrument per re-symboled contract.

    Built the way ``occ_mapping.write_mappings`` builds it, by registering the contract under
    the symbol it was first read with and then remapping it, because ``remap`` closes an open
    mapping and nothing else opens one for an option.
    """
    master = SecurityMaster()
    equity = master.register(
        kind="equity", capture_start=EPOCH, valid_from=EPOCH.date(), ticker="SPY"
    )
    for symbol in remapped:
        instrument = master.register(
            kind="option", capture_start=EPOCH, valid_from=FIRST_SEEN, occ_symbol=symbol
        )
        master.remap(instrument, ID_TYPE_OCC, adjusted(symbol), effective=BOUNDARY)
    for instrument_id, symbol in extra:
        master.remap(instrument_id, ID_TYPE_OCC, symbol, effective=BOUNDARY)
    return master, equity


def build(lake: FixtureLake, following: list[dict], master: SecurityMaster, equity: int) -> Path:
    """S's close under its own spellings, the walked session's cycles, and the reference tables."""
    lake.with_chains(
        "SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)), source="capture"
    )
    lake.with_chains("SPY", FOLLOWING, table(following), source="capture")
    spans = CaptureSpans()
    spans.open_span(equity, EPOCH, True)
    lake.with_reference("security_master", master.to_table())
    lake.with_reference("capture_spans", spans.to_table())
    lake.with_reference("schema_versions", ledger_table())
    return lake.build()


def cycles(contracts: dict[str, int]) -> list[dict]:
    """Three identical cycles, so a refresh has something to plateau across."""
    rows: list[dict] = []
    for hour, minute in ((9, 35), (9, 36), (9, 37)):
        rows.extend(cycle_rows(FOLLOWING, hour, minute, contracts))
    return rows


def answer_for(root: Path, **kwargs) -> pa.Table:
    return oi_view("SPY", SESSION, lake_root=root, calendar=CALENDAR, **kwargs)


def verdicts(answer: pa.Table) -> set[tuple[str, str | None]]:
    return set(
        zip(answer.column("verdict").to_pylist(), answer.column("reason").to_pylist(), strict=True)
    )


def test_a_whole_chain_re_symboled_settles_rather_than_reading_indeterminate(
    fixture_lake: FixtureLake,
):
    """The defect's dominant shape, which is session-wide rather than per contract.

    An OCC adjustment re-symbols every open contract under the root at once, so keying on
    the spelling empties the voter set on every cycle and the walk returns ``indeterminate``
    with ``set_under_floor`` across the whole roster. That reason is a false statement: the
    set is the configured size and every contract in it is still trading.
    """
    moved = {adjusted(symbol): value + 500 for symbol, value in SET.items()}
    master, equity = master_with(remapped=tuple(SET))
    root = build(fixture_lake, cycles(moved), master, equity)

    answer = answer_for(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert answer.column("open_interest").to_pylist() == [value + 500 for value in SET.values()]
    # The answer is about S, so it carries S's own spellings and not the adjusted ones.
    assert answer.column("occ_symbol").to_pylist() == list(SET)
    assert answer.column("source_session").to_pylist() == [FOLLOWING] * len(SET)


def test_the_same_chain_without_the_mapping_rows_is_the_defect(fixture_lake: FixtureLake):
    """The control. Identical chains, a master holding no option, and the wrong verdict.

    This is what makes the test above prove the threading rather than the fixture. Without
    the mapping rows nothing can tell a renamed contract from a vanished one, which is the
    state every lake is in until ``lake.occ_mapping`` has a boundary to record.
    """
    moved = {adjusted(symbol): value + 500 for symbol, value in SET.items()}
    master, equity = master_with()
    root = build(fixture_lake, cycles(moved), master, equity)

    answer = answer_for(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_INDETERMINATE, "set_under_floor")}


def test_a_partly_re_symboled_chain_settles_both_halves(fixture_lake: FixtureLake):
    """Half the set moved and half not, which is the milder per-contract shape.

    Keying on the spelling settles the contracts that stayed and marks the moved ones
    ``absent`` with ``no_value_in_cycle``, which is the answer a contract that genuinely
    vanished gets. Threading settles all of them.
    """
    names = list(SET)
    moved = {
        (adjusted(symbol) if position < 4 else symbol): SET[symbol] + 500
        for position, symbol in enumerate(names)
    }
    master, equity = master_with(remapped=tuple(names[:4]))
    root = build(fixture_lake, cycles(moved), master, equity)

    answer = answer_for(root, constants=constants(oi_comparable_set_floor=2))

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}


def test_a_contract_that_really_vanished_is_still_absent(fixture_lake: FixtureLake):
    """The threading must not turn a missing contract into a settled one.

    One contract is re-symboled and dropped from the walked session entirely. Its mapping
    rows exist, so the master can place it, and the cycle still carries no figure for it.
    That is ``absent`` with ``no_value_in_cycle``, exactly as it was before the threading.
    """
    names = list(SET)
    gone = names[0]
    moved = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    del moved[adjusted(gone)]
    master, equity = master_with(remapped=tuple(names))
    root = build(fixture_lake, cycles(moved), master, equity)

    answer = answer_for(root, constants=constants(oi_comparable_set_floor=2))

    by_symbol = dict(
        zip(
            answer.column("occ_symbol").to_pylist(),
            zip(
                answer.column("verdict").to_pylist(),
                answer.column("reason").to_pylist(),
                strict=True,
            ),
            strict=True,
        )
    )
    assert by_symbol[gone] == (VERDICT_ABSENT, REASON_NO_VALUE_IN_CYCLE)
    assert by_symbol[names[1]] == (VERDICT_SETTLED, None)


def test_a_re_issued_spelling_is_not_merged_with_the_contract_that_wore_it(
    fixture_lake: FixtureLake,
):
    """Why the lookup resolves on a date, which is the whole reason the master carries ranges.

    An adjustment frees the original spelling and the market re-lists a different contract
    under it, which ``docs/design.md`` names where it introduces the master: OCC symbols "are
    reissued when the OCC adjusts contracts after a corporate action. So no external symbol is
    the primary key."

    Here the walked session carries both, the adjusted contract under ``SPY1`` and a brand new
    contract under the freed ``SPY`` spelling. A lookup ignoring the ranges reads the closed
    mapping as live and keys the new contract onto the adjusted one, so the two share a figure.
    Resolving on the date each row was written keeps them apart.
    """
    names = list(SET)
    carried, freed = names[0], names[1]
    master, equity = master_with(remapped=(carried,))
    walked = {
        adjusted(symbol) if symbol == carried else symbol: SET[symbol] + 500 for symbol in names
    }
    # A different contract, newly listed under the spelling the adjustment freed.
    walked[carried] = 31337
    root = build(fixture_lake, cycles(walked), master, equity)

    answer = answer_for(root, constants=constants(oi_comparable_set_floor=2))

    settled = dict(
        zip(
            answer.column("occ_symbol").to_pylist(),
            answer.column("open_interest").to_pylist(),
            strict=True,
        )
    )
    # S's contract took its own adjusted figure, not the new listing's 31337.
    assert settled[carried] == SET[carried] + 500
    assert settled[freed] == SET[freed] + 500
    assert 31337 not in settled.values()


def test_a_boundary_dated_after_the_adjustment_degrades_rather_than_inventing_a_number(
    fixture_lake: FixtureLake,
):
    """The price of resolving on a date, named rather than hidden.

    ``lake.splits`` dates a boundary to the session it happened to read, and it skips sessions
    for several reasons, so a boundary can land after the adjustment did. Here S already
    carries the adjusted spellings and the master says the boundary is the session after, so
    S's rows resolve to nothing and key on themselves while the walked session's resolve to
    the instrument. The join does not close and the contracts read ``absent``.

    That is the defect this module repairs, still present on a mis-dated boundary. It is the
    accepted cost of the alternative, which reads every closed mapping as live and merges a
    re-issued spelling into the contract that used to wear it. This answer withholds a number.
    That one invents one, and a withheld verdict is recoverable where a wrong figure is not.
    """
    moved_close = {adjusted(symbol): value for symbol, value in SET.items()}
    moved_volumes = {adjusted(symbol): volume for symbol, volume in VOLUMES.items()}
    fixture_lake.with_chains(
        "SPY",
        SESSION,
        table(close_rows(SESSION, moved_close, volumes=moved_volumes)),
        source="capture",
    )
    walked = {adjusted(symbol): value + 500 for symbol, value in SET.items()}
    fixture_lake.with_chains("SPY", FOLLOWING, table(cycles(walked)), source="capture")
    master, equity = master_with(remapped=tuple(SET))
    spans = CaptureSpans()
    spans.open_span(equity, EPOCH, True)
    fixture_lake.with_reference("security_master", master.to_table())
    fixture_lake.with_reference("capture_spans", spans.to_table())
    fixture_lake.with_reference("schema_versions", ledger_table())

    answer = answer_for(fixture_lake.build(), constants=constants())

    assert verdicts(answer) == {(VERDICT_INDETERMINATE, "set_under_floor")}
    # Nothing was invented. Every figure is withheld rather than borrowed from a neighbour.
    assert set(answer.column("open_interest").to_pylist()) == {None}


def corrupt_master(symbol: str, other: str):
    """A master mapping two spellings to one instrument on one date.

    ``remap`` cannot build this. It closes the open mapping at the boundary and opens the new
    one from it, so the two rows share a date and never overlap. These rows are written
    directly, because a guard against a state the writer cannot produce still has to be
    reachable to be tested, the way ``SecurityMaster.AmbiguousSymbol`` is.
    """
    master = SecurityMaster()
    equity = master.register(
        kind="equity", capture_start=EPOCH, valid_from=EPOCH.date(), ticker="SPY"
    )
    overlapping = tuple(master.mappings) + (
        Mapping(
            instrument_id=99,
            id_type=ID_TYPE_OCC,
            id_value=symbol,
            valid_from=EPOCH.date(),
            valid_to=None,
            kind="option",
            capture_start=EPOCH,
        ),
        Mapping(
            instrument_id=99,
            id_type=ID_TYPE_OCC,
            id_value=other,
            valid_from=EPOCH.date(),
            valid_to=None,
            kind="option",
            capture_start=EPOCH,
        ),
    )
    return SecurityMaster(overlapping), equity


def test_one_session_carrying_both_spellings_refuses(fixture_lake: FixtureLake):
    """The failure the threading itself creates, rather than one it inherits.

    On any one date the master maps at most one spelling to a contract, so two spellings
    reaching one instrument means the rows and the master disagree about what is listed.
    Reading them would collapse two figures into one. Keying on the spelling could not do
    that, because two spellings are two keys, so this refuses rather than picking a row.
    """
    names = list(SET)
    both = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    both[names[0]] = 999
    master, equity = corrupt_master(names[0], adjusted(names[0]))
    root = build(fixture_lake, cycles(both), master, equity)

    with pytest.raises(SpellingsCollide) as raised:
        answer_for(root, constants=constants())

    assert names[0] in str(raised.value)
    assert adjusted(names[0]) in str(raised.value)
    assert raised.value.day == FOLLOWING


def test_the_roster_refuses_a_collision_the_comparable_set_never_sees(
    fixture_lake: FixtureLake,
):
    """The same guard on the answer's own path, where the figure actually lands.

    The comparable set is the top contracts by volume and the roster is the whole close, so a
    colliding pair ranked out of the set reaches ``_settled`` without passing the map's guard.
    Answering there would hand two rows one contract's figure, including a row the selected
    cycle carries nothing for. Whether the same lake refuses or answers would then be decided
    by a volume rank, which has nothing to do with identity.
    """
    names = list(SET)
    ghost = adjusted(names[0])
    roster = {**SET, ghost: 4242}
    volumes = {**VOLUMES, ghost: 0}
    fixture_lake.with_chains(
        "SPY", SESSION, table(close_rows(SESSION, roster, volumes=volumes)), source="capture"
    )
    refreshed = {symbol: value + 500 for symbol, value in SET.items()}
    fixture_lake.with_chains("SPY", FOLLOWING, table(cycles(refreshed)), source="capture")
    master, equity = corrupt_master(names[0], ghost)
    spans = CaptureSpans()
    spans.open_span(equity, EPOCH, True)
    fixture_lake.with_reference("security_master", master.to_table())
    fixture_lake.with_reference("capture_spans", spans.to_table())
    fixture_lake.with_reference("schema_versions", ledger_table())

    with pytest.raises(SpellingsCollide) as raised:
        # A set of 4 keeps the ghost, whose volume is zero, out of the comparable set.
        answer_for(fixture_lake.build(), constants=constants(oi_comparable_set_size=4))

    assert raised.value.day == SESSION


def test_a_symbol_two_instruments_hold_refuses_as_unreadable_scope(fixture_lake: FixtureLake):
    """A master that cannot say which contract a symbol is.

    ``occ_mapping.write_mappings`` refuses to write this state, calling it exactly that.
    Nothing here can say either, so it refuses rather than picking one, and it refuses as
    ``ScopeUnreadable`` so a caller's ``except OiViewError`` still catches it.
    """
    names = list(SET)
    master, equity = master_with(remapped=(names[0],))
    # A second instrument claiming the first one's adjusted symbol.
    intruder = master.register(
        kind="option", capture_start=EPOCH, valid_from=FIRST_SEEN, occ_symbol=names[1]
    )
    master.remap(intruder, ID_TYPE_OCC, adjusted(names[0]), effective=BOUNDARY)
    moved = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    root = build(fixture_lake, cycles(moved), master, equity)

    with pytest.raises(ScopeUnreadable) as raised:
        answer_for(root, constants=constants())

    assert adjusted(names[0]) in str(raised.value)


def test_a_repeated_spelling_is_not_a_collision_and_the_last_row_wins(
    fixture_lake: FixtureLake,
):
    """The other half of the collision rule, which is what makes it a rule rather than a ban.

    Two rows carrying the *same* spelling are one contract listed twice, not two spellings of
    one contract. Keying on the symbol already let the last row win, and the threading is not
    trying to change that, so a repeated spelling must not refuse. Without this the rule reads
    as "two rows on one key refuse", which would abort a whole view on an ordinary duplicate.
    """
    names = list(SET)
    rows: list[dict] = []
    for hour, minute in ((9, 35), (9, 36), (9, 37)):
        cycle = cycle_rows(FOLLOWING, hour, minute, {s: SET[s] + 500 for s in names})
        # The first contract listed a second time, same spelling, a different figure.
        cycle.append(cycle_rows(FOLLOWING, hour, minute, {names[0]: SET[names[0]] + 900})[0])
        rows.extend(cycle)
    fixture_lake.with_chains(
        "SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)), source="capture"
    )
    fixture_lake.with_chains("SPY", FOLLOWING, table(rows), source="capture")
    master, equity = master_with()
    spans = CaptureSpans()
    spans.open_span(equity, EPOCH, True)
    fixture_lake.with_reference("security_master", master.to_table())
    fixture_lake.with_reference("capture_spans", spans.to_table())
    fixture_lake.with_reference("schema_versions", ledger_table())

    answer = answer_for(fixture_lake.build(), constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    settled = dict(
        zip(
            answer.column("occ_symbol").to_pylist(),
            answer.column("open_interest").to_pylist(),
            strict=True,
        )
    )
    # The duplicate's figure, not the first row's, which is what keying on the symbol did.
    assert settled[names[0]] == SET[names[0]] + 900


def test_a_contract_remapped_away_and_back_threads_rather_than_refusing(
    fixture_lake: FixtureLake,
):
    """One instrument holding one spelling in two mapping rows is not two instruments.

    A contract adjusted twice can return to a spelling it already wore, which leaves the
    master holding that symbol against the same instrument in two rows. The refusal is for a
    symbol *two instruments* hold, so counting rows rather than instruments would refuse an
    ordinary history. The ranges keep the two rows apart on any one date as well.
    """
    names = list(SET)
    master = SecurityMaster()
    equity = master.register(
        kind="equity", capture_start=EPOCH, valid_from=EPOCH.date(), ticker="SPY"
    )
    instrument = master.register(
        kind="option", capture_start=EPOCH, valid_from=EPOCH.date(), occ_symbol=names[0]
    )
    # Both boundaries sit well before S, so by S the contract is back on its first spelling
    # and stays there. What the master keeps is that spelling in two rows.
    master.remap(instrument, ID_TYPE_OCC, adjusted(names[0]), effective=date(2026, 8, 10))
    master.remap(instrument, ID_TYPE_OCC, names[0], effective=date(2026, 8, 20))
    held = [m.id_value for m in master.mappings if m.id_value == names[0]]
    assert len(held) == 2, "the fixture must leave one symbol on one instrument twice"

    refreshed = {symbol: value + 500 for symbol, value in SET.items()}
    root = build(fixture_lake, cycles(refreshed), master, equity)

    answer = answer_for(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}


def test_one_instrument_holding_a_symbol_in_two_overlapping_rows_is_not_ambiguous(
    fixture_lake: FixtureLake,
):
    """The refusal counts instruments, not rows, and only a corrupt master can tell them apart.

    ``remap`` writes disjoint ranges, so on any one date a symbol reaches an instrument
    through at most one row and counting either way gives the same answer. Two overlapping
    rows for the *same* instrument break that tie, and they are still one contract. Counting
    rows would refuse a view over a master that says nothing ambiguous at all.
    """
    names = list(SET)
    master = SecurityMaster()
    equity = master.register(
        kind="equity", capture_start=EPOCH, valid_from=EPOCH.date(), ticker="SPY"
    )
    duplicated = tuple(master.mappings) + tuple(
        Mapping(
            instrument_id=99,
            id_type=ID_TYPE_OCC,
            id_value=names[0],
            valid_from=EPOCH.date(),
            valid_to=None,
            kind="option",
            capture_start=EPOCH,
        )
        for _ in range(2)
    )
    master = SecurityMaster(duplicated)
    refreshed = {symbol: value + 500 for symbol, value in SET.items()}
    root = build(fixture_lake, cycles(refreshed), master, equity)

    answer = answer_for(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}


def test_both_refusals_are_catchable_as_one_and_carry_their_detail(
    fixture_lake: FixtureLake,
):
    """Every refusal here is an ``OiViewError``, and each carries what a caller would act on.

    A caller writes ``except OiViewError`` once. A refusal outside that base escapes it, and
    a refusal whose attributes are empty leaves the caller parsing a message.
    """
    names = list(SET)
    both = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    both[names[0]] = 999
    master, equity = corrupt_master(names[0], adjusted(names[0]))
    root = build(fixture_lake, cycles(both), master, equity)

    with pytest.raises(OiViewError) as collided:
        answer_for(root, constants=constants())

    assert isinstance(collided.value, SpellingsCollide)
    assert collided.value.ticker == "SPY"
    assert collided.value.day == FOLLOWING
    # Reported in the order they were met, so a reader knows which spelling arrived second.
    assert collided.value.spellings == (adjusted(names[0]), names[0])

    master, equity = master_with(remapped=(names[0],))
    intruder = master.register(
        kind="option", capture_start=EPOCH, valid_from=FIRST_SEEN, occ_symbol=names[1]
    )
    master.remap(intruder, ID_TYPE_OCC, adjusted(names[0]), effective=BOUNDARY)
    moved = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    root = build(fixture_lake, cycles(moved), master, equity)

    with pytest.raises(OiViewError) as unreadable:
        answer_for(root, constants=constants())

    assert isinstance(unreadable.value, ScopeUnreadable)
    assert unreadable.value.ticker == "SPY"


def test_an_unthreaded_lake_answers_exactly_as_it_did(fixture_lake: FixtureLake):
    """The ordinary path, which is every lake today.

    No contract has been re-symboled, so the master holds no option instrument, the index is
    empty and every symbol keys on itself. The 45 tests in ``test_oi_view.py`` and
    ``test_oi_comparable_set.py`` all run in this state, so they are the real breadth here
    and this only names the claim.
    """
    refreshed = {symbol: value + 500 for symbol, value in SET.items()}
    master, equity = master_with()
    root = build(fixture_lake, cycles(refreshed), master, equity)

    answer = answer_for(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert answer.column("occ_symbol").to_pylist() == list(SET)
