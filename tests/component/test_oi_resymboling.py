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
    ScopeUnreadable,
    SpellingsCollide,
    oi_view,
)
from lake.security_master import ID_TYPE_OCC, SecurityMaster
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


def test_a_boundary_dated_one_session_late_still_threads(fixture_lake: FixtureLake):
    """The reason the lookup takes no date.

    ``lake.splits`` dates a boundary to the session it happened to read, and it skips
    sessions for several reasons, so the date can land after the adjustment did. Here S
    itself already carries the adjusted spellings and the master says the boundary is the
    session after. A dated resolution would answer nothing for the baseline and the
    instrument for the cycle, keying the two sides differently and reproducing the defect
    with the master in the loop.
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

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}


def test_one_cycle_carrying_both_spellings_refuses(fixture_lake: FixtureLake):
    """The failure the threading itself creates, rather than one it inherits.

    Keying on the instrument merges every spelling a contract has worn, so a cycle listing
    one contract twice would collapse to a single entry and lose the other figure silently.
    Keying on the spelling could not do that, because two spellings are two keys. So it
    refuses.
    """
    names = list(SET)
    both = {adjusted(symbol): SET[symbol] + 500 for symbol in names}
    both[names[0]] = 999
    master, equity = master_with(remapped=tuple(names))
    root = build(fixture_lake, cycles(both), master, equity)

    with pytest.raises(SpellingsCollide) as raised:
        answer_for(root, constants=constants())

    assert names[0] in str(raised.value)
    assert adjusted(names[0]) in str(raised.value)


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
