"""The battery's per-partition decision, decided from values alone.

One check exists, so no run can produce two findings about one partition and ``judge`` cannot
be driven into the cases marketlake #426 is about. ``decide_partition`` is where they are
decided, and this is where they are held. The disk-backed half lives in the component tier.

The ledger resolves last entry wins within each check, so these build the state the way
``manifest.latest_quarantine_by_check`` hands it over: one entry per check, already resolved.
"""

from __future__ import annotations

from datetime import date

import pytest

from lake.battery import (
    CHECK_ENTITLEMENT,
    INSUFFICIENT_HISTORY,
    OUT_OF_SCOPE,
    PROVENANCE_BATTERY,
    PROVENANCE_HUMAN,
    QUARANTINED_VERDICT,
    Finding,
    decide_partition,
)
from lake.manifest import CLEAN_VERDICT

PARTITION = "chains/ticker=SPY/date=2026-09-16.parquet"
ROWS = "snapshot_row_count"
EXPIRY = "missing_expiry"


def _finding(check: str, verdict: str) -> Finding:
    return Finding(
        partition=PARTITION,
        surface="chains",
        ticker="SPY",
        day=date(2026, 9, 16),
        check=check,
        verdict=verdict,
        reason=f"{check} says {verdict}",
    )


def _entry(check: str, verdict: str, provenance: str = PROVENANCE_BATTERY) -> dict:
    return {
        "partition": PARTITION,
        "verdict": verdict,
        "check": check,
        "provenance": provenance,
    }


def _wrote(outcome) -> list[str]:
    return [d.finding.check for d in outcome.decisions if d.wrote]


# -- one check clearing never speaks for another -----------------------------


def test_a_check_clearing_leaves_the_other_checks_quarantine_standing():
    """#426 itself. The second check to clear used to release the partition outright."""
    state = {
        ROWS: _entry(ROWS, QUARANTINED_VERDICT),
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT),
    }

    outcome = decide_partition(state, [_finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)])

    assert _wrote(outcome) == [CHECK_ENTITLEMENT], "its own pass is still recorded"
    assert outcome.released is False
    (decision,) = outcome.decisions
    assert decision.holders == (ROWS,)


def test_the_passing_checks_line_lands_so_the_partition_is_not_stranded():
    """The trap in the other direction, and the reason #406's guard could not simply stay.

    A guard that suppressed the write would leave this check's own quarantine standing
    forever. The partition would then be withheld with no check failing, and only a human
    could clear a verdict the check itself had already retracted.
    """
    state = {
        ROWS: _entry(ROWS, QUARANTINED_VERDICT),
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT),
    }

    first = decide_partition(state, [_finding(ROWS, CLEAN_VERDICT)])
    assert _wrote(first) == [ROWS]

    state[ROWS] = _entry(ROWS, CLEAN_VERDICT)
    second = decide_partition(state, [_finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)])

    assert second.released is True
    assert second.decisions[0].holders == ()


def test_two_checks_clearing_in_one_call_report_the_release():
    """The state is carried forward as lines land, rather than read once before the walk.

    Read once, the second clearing check still sees the first as quarantined, so the release
    is never reported at all even though the partition reads.
    """
    state = {
        ROWS: _entry(ROWS, QUARANTINED_VERDICT),
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT),
    }

    outcome = decide_partition(
        state,
        [_finding(ROWS, CLEAN_VERDICT), _finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)],
    )

    assert _wrote(outcome) == [ROWS, CHECK_ENTITLEMENT]
    assert outcome.released is True
    assert [d.holders for d in outcome.decisions] == [(CHECK_ENTITLEMENT,), ()]


def test_the_caller_is_handed_the_state_it_passed_in_unchanged():
    """The decision is pure, so a caller cannot be surprised by a map that moved under it."""
    state = {ROWS: _entry(ROWS, QUARANTINED_VERDICT)}
    before = {k: dict(v) for k, v in state.items()}

    decide_partition(state, [_finding(ROWS, CLEAN_VERDICT)])

    assert state == before


# -- a check that says nothing holds nothing back ----------------------------


def test_a_non_verdict_from_another_check_does_not_release_the_partition():
    """The writer-side fold over one run's findings fails exactly here.

    ``insufficient_history`` withholds nothing this run while the check's recorded quarantine
    stands, so a fold over findings alone would see no withholder and let the clean land as a
    release.
    """
    state = {
        ROWS: _entry(ROWS, QUARANTINED_VERDICT),
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT),
    }

    outcome = decide_partition(
        state,
        [_finding(ROWS, INSUFFICIENT_HISTORY), _finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)],
    )

    assert outcome.released is False
    assert _wrote(outcome) == [CHECK_ENTITLEMENT]
    assert [d.finding.check for d in outcome.decisions] == [CHECK_ENTITLEMENT]


@pytest.mark.parametrize("verdict", [INSUFFICIENT_HISTORY, OUT_OF_SCOPE])
def test_a_finding_that_is_not_a_verdict_takes_no_decision_at_all(verdict: str):
    """Only a verdict reaches the ledger, so a non-verdict is not a line and not a deferral."""
    outcome = decide_partition(None, [_finding(CHECK_ENTITLEMENT, verdict)])

    assert outcome.decisions == ()
    assert outcome.released is False


# -- a human's row is not a check's ------------------------------------------


def test_a_humans_quarantine_under_its_own_token_survives_every_check_passing():
    """A human finds what no check can see, so no check can answer it.

    The token is the human's own, so nothing in the battery ever emits it and nothing but a
    sign-off ever clears it. Under per-partition resolution the entitlement check's clean
    became the last line and released the partition.
    """
    state = {
        EXPIRY: _entry(EXPIRY, QUARANTINED_VERDICT, PROVENANCE_HUMAN),
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT),
    }

    outcome = decide_partition(state, [_finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)])

    assert outcome.released is False
    assert outcome.decisions[0].holders == (EXPIRY,)


def test_a_sign_off_still_stands_when_another_check_wrote_after_it():
    """#139's precedence rule, against an intervening entry that used to hide it.

    ``human_precedence`` reads the partition's entry for *this* check. Read against the
    partition's last line instead, the row-count quarantine landing afterwards hid the
    sign-off completely and the battery re-quarantined what a human had just cleared.
    """
    state = {
        CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, CLEAN_VERDICT, PROVENANCE_HUMAN),
        ROWS: _entry(ROWS, QUARANTINED_VERDICT),
    }

    outcome = decide_partition(state, [_finding(CHECK_ENTITLEMENT, QUARANTINED_VERDICT)])

    (decision,) = outcome.decisions
    assert decision.deferred_to_human is True
    assert decision.wrote is False


def test_a_sign_off_defers_only_its_own_check():
    """A different check finding a different fault is news the human never spoke to."""
    state = {CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, CLEAN_VERDICT, PROVENANCE_HUMAN)}

    outcome = decide_partition(state, [_finding(ROWS, QUARANTINED_VERDICT)])

    (decision,) = outcome.decisions
    assert decision.deferred_to_human is False
    assert decision.wrote is True


# -- the ledger is append-only, so the same news twice is not news -----------


def test_the_same_verdict_twice_takes_no_second_line():
    state = {CHECK_ENTITLEMENT: _entry(CHECK_ENTITLEMENT, QUARANTINED_VERDICT)}

    outcome = decide_partition(state, [_finding(CHECK_ENTITLEMENT, QUARANTINED_VERDICT)])

    assert _wrote(outcome) == []
    assert outcome.released is False


def test_a_verdict_reversed_and_re_asserted_reads_the_same_each_time():
    """Three histories, one answer. The reader resolves the key, never counts the lines."""
    state: dict[str, dict] = {}
    seen = []
    for verdict in (QUARANTINED_VERDICT, CLEAN_VERDICT, QUARANTINED_VERDICT, CLEAN_VERDICT):
        outcome = decide_partition(state, [_finding(CHECK_ENTITLEMENT, verdict)])
        assert _wrote(outcome) == [CHECK_ENTITLEMENT]
        state[CHECK_ENTITLEMENT] = _entry(CHECK_ENTITLEMENT, verdict)
        seen.append(outcome.released)

    assert seen == [False, True, False, True]


def test_a_clean_verdict_for_a_check_that_never_spoke_is_not_a_line():
    """A check with no entry has said nothing, so a pass changes nothing and costs nothing."""
    outcome = decide_partition(None, [_finding(CHECK_ENTITLEMENT, CLEAN_VERDICT)])

    assert _wrote(outcome) == []
    assert outcome.released is False, "a partition nothing withheld was not released"
