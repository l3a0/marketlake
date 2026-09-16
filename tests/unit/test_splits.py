"""The split detector's own rules, without a lake on disk.

Three of them are decided by a function rather than by the walk, and each one is where a
wrong answer would be invisible from the outside. The gate decides whether a ratio is allowed
to land at all. :func:`require_scalar` decides whether one float can honestly describe an
adjustment. :func:`deliverable_of` decides what the vendor's two spellings of the deliverable
reduce to.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from lake.splits import (
    Deliverable,
    DeliverableUnreadable,
    NonScalarDeliverable,
    Session,
    check_split_consistency,
    deliverable_of,
    require_scalar,
)

DAY = date(2026, 9, 15)

# ``None`` is a value the deliverables column really takes, which the lake's own 2026-09-02
# partition carries on both its rows, so the builder below needs a separate way to say "the
# ordinary payload".
_DEFAULT = object()


def _deliverable(
    units: float = 100.0,
    *,
    symbol: str | None = "SPY",
    entries: int = 1,
    cash: bool = False,
    note_units: float | None = 100.0,
    multiplier: float | None = 100.0,
    non_standard: bool | None = True,
) -> Deliverable:
    return Deliverable(
        units=units,
        symbol=symbol,
        entries=entries,
        cash=cash,
        note_units=note_units,
        multiplier=multiplier,
        non_standard=non_standard,
    )


# -- the gate ---------------------------------------------------------------------------


def test_the_two_spellings_agreeing_admits_the_ratio():
    """150 shares where 100 stood, said twice by the vendor, is a ratio of 1.5."""
    verdict = check_split_consistency(_deliverable(), _deliverable(150.0, note_units=150.0))

    assert verdict.agrees
    assert verdict.computed == 1.5
    assert verdict.against == 1.5


def test_a_note_that_moved_without_the_typed_count_does_not_agree():
    """The drifted-fundamental shape, which is what the gate exists to catch.

    One of two fields carrying an adjustment the other does not would put a wrong ratio in
    the ledger while looking well-formed.
    """
    verdict = check_split_consistency(_deliverable(), _deliverable(150.0, note_units=200.0))

    assert not verdict.agrees
    assert verdict.computed == 1.5
    assert verdict.against == 2.0


def test_a_missing_note_leaves_the_gate_with_one_number_and_it_does_not_agree():
    """A gate missing an input has not agreed, which is what makes it fail closed."""
    verdict = check_split_consistency(_deliverable(), _deliverable(150.0, note_units=None))

    assert not verdict.agrees
    assert verdict.against is None
    assert verdict.computed == 1.5, "the typed ratio still rides the finding"


def test_a_prior_note_of_zero_has_no_relative_scale_and_does_not_agree():
    """Zero has no relative scale, so the comparison below it would divide by it."""
    verdict = check_split_consistency(
        _deliverable(note_units=0.0), _deliverable(150.0, note_units=150.0)
    )

    assert not verdict.agrees
    assert verdict.against is None


def test_the_tolerance_admits_a_hair_and_refuses_a_share():
    """The constant sits between the error of two divisions and the smallest real difference.

    The smallest disagreement the gate has to catch is one share in a hundred, which is 1e-2
    relative. The slack owed to two IEEE-754 divisions is around 1e-16.
    """
    prior = _deliverable()
    admitted = check_split_consistency(prior, _deliverable(150.0 * (1 + 1e-12), note_units=150.0))
    refused = check_split_consistency(prior, _deliverable(151.0, note_units=150.0))

    assert admitted.agrees
    assert not refused.agrees


# -- what one float can say -------------------------------------------------------------


def test_one_stock_deliverable_scaled_is_what_a_ratio_describes():
    """#136's first class: a whole-ratio split maps exactly."""
    require_scalar(_deliverable(), _deliverable(150.0, note_units=150.0))


def test_cash_beside_shares_is_refused():
    """#136's own example. A contract delivering shares plus cash has no valid multiplier.

    One entry carrying a currency rather than two, so the cash clause is what answers. A
    two-entry deliverable is refused by the clause after it whether or not this one is
    there, which is how a test of the cash rule passes without exercising it.
    """
    with pytest.raises(NonScalarDeliverable, match="cash"):
        require_scalar(_deliverable(), _deliverable(150.0, cash=True))


def test_two_deliverables_are_refused():
    """Two entries are two things delivered, and one number describes neither."""
    with pytest.raises(NonScalarDeliverable, match="entries"):
        require_scalar(_deliverable(), _deliverable(150.0, entries=2))


def test_a_different_security_is_refused():
    """The same count of a different security is not a split at all."""
    with pytest.raises(NonScalarDeliverable, match="moved from 'SPY' to 'XYZ'"):
        require_scalar(_deliverable(), _deliverable(150.0, symbol="XYZ"))


def test_a_moved_multiplier_is_refused():
    """A ratio scales what the contract delivers. A multiplier scales what it is."""
    with pytest.raises(NonScalarDeliverable, match="multiplier"):
        require_scalar(_deliverable(), _deliverable(150.0, multiplier=150.0))


def test_the_standard_flag_is_not_one_of_the_conditions():
    """It classifies the contract rather than describing what the contract delivers.

    An earlier draft refused a gained root the vendor still called standard, on the argument
    that the OCC re-symbols only when the adjustment makes a contract non-standard. That is a
    claim about the OCC's concept rather than about Schwab's boolean, and as a refusal it
    held a clean two-for-one that one float describes perfectly, under a check name saying
    the opposite. The flag's real jobs are on :class:`Session`, tested below.
    """
    for flag in (True, False, None):
        require_scalar(_deliverable(), _deliverable(200.0, note_units=200.0, non_standard=flag))


@pytest.mark.parametrize("field", ["symbol", "multiplier"])
@pytest.mark.parametrize("side", ["prior", "new", "both"])
def test_a_value_the_vendor_did_not_record_is_refused_for_what_it_is(field: str, side: str):
    """Absent is not equal, and it is not different either.

    Nothing says the value moved and nothing says it held, and landing on no evidence is the
    one outcome the ledger cannot take back. Two absent values are refused for the same
    reason rather than passing on the strength of comparing equal to each other, and the
    message says the value is not recorded rather than claiming a move nobody observed.
    """
    prior = _deliverable(**({field: None} if side in ("prior", "both") else {}))
    new = _deliverable(
        150.0, note_units=150.0, **({field: None} if side in ("new", "both") else {})
    )

    with pytest.raises(NonScalarDeliverable, match="not recorded on both sides"):
        require_scalar(prior, new)


# -- reading the deliverable off rows ---------------------------------------------------


def _row(
    root: str = "SPY",
    units: float | str = 100.0,
    *,
    note: str | None = "100 SPY",
    multiplier: float | None = 100.0,
    non_standard: bool | None = False,
    mini: bool | None = False,
    encoded: str | None = _DEFAULT,
) -> tuple[str, dict]:
    if encoded is _DEFAULT:
        encoded = json.dumps(
            [
                {
                    "assetType": "STOCK",
                    "currencyType": None,
                    "deliverableUnits": units,
                    "symbol": "SPY",
                }
            ],
            sort_keys=True,
        )
    return (
        root,
        {
            "option_root": root,
            "occ_symbol": "SPY   260918C00650000",
            "option_deliverables_list": encoded,
            "deliverable_note": note,
            "multiplier": multiplier,
            "non_standard": non_standard,
            "mini": mini,
            "suspect": False,
            "is_chain_truncated": False,
        },
    )


def _session(*rows: tuple[str, dict]) -> Session:
    return Session(
        day=DAY,
        instrument_id=1,
        roots=frozenset(root for root, _ in rows),
        rows=rows,
    )


def test_the_deliverable_is_read_for_the_named_roots_alone():
    """One chain can carry both roots at once, and the ratio's two sides are not both of it.

    An adjustment re-symbols the open contracts while newly listed standard ones keep the
    original root, so reading the whole session would mix the two.
    """
    session = _session(_row("SPY"), _row("SPY1", 150.0, note="150 SPY"))

    assert deliverable_of(session, frozenset({"SPY1"})).units == 150.0
    assert deliverable_of(session, frozenset({"SPY"})).units == 100.0


def test_contracts_disagreeing_about_the_deliverable_raise():
    """Taking the first would let the file's own order decide what the ledger gets."""
    session = _session(_row("SPY1", 150.0), _row("SPY1", 200.0))

    with pytest.raises(DeliverableUnreadable, match="disagree"):
        deliverable_of(session, frozenset({"SPY1"}))


def test_a_root_with_no_rows_raises():
    session = _session(_row("SPY"))

    with pytest.raises(DeliverableUnreadable, match="no rows"):
        deliverable_of(session, frozenset({"SPY1"}))


@pytest.mark.parametrize(
    "encoded, match",
    [
        (None, "carry no option_deliverables_list"),
        ("   ", "carry no option_deliverables_list"),
        ("{not json", "not JSON"),
        ("[]", "names no deliverable"),
        ('[{"assetType": "CURRENCY", "deliverableUnits": 100.0}]', "0 stock deliverables"),
        ('[{"assetType": "STOCK", "deliverableUnits": "100"}]', "not a number"),
        ('[{"assetType": "STOCK", "deliverableUnits": true}]', "not a number"),
        ('[{"assetType": "STOCK", "deliverableUnits": 0}]', "positive finite"),
        ('[{"assetType": "STOCK", "deliverableUnits": -100.0}]', "positive finite"),
        # ``json.loads`` accepts bare ``NaN`` and ``Infinity``, and ``NaN <= 0`` is False, so
        # a positivity test alone lets both through.
        ('[{"assetType": "STOCK", "deliverableUnits": NaN}]', "positive finite"),
        ('[{"assetType": "STOCK", "deliverableUnits": Infinity}]', "positive finite"),
        # Two stock entries carry no single unit count, and taking the first would let the
        # payload's own order decide the ratio.
        (
            '[{"assetType": "STOCK", "deliverableUnits": 100.0},'
            ' {"assetType": "STOCK", "deliverableUnits": 50.0}]',
            "2 stock deliverables",
        ),
    ],
)
def test_a_payload_carrying_no_usable_unit_count_raises(encoded: str | None, match: str):
    """Each of these would otherwise end the run as a traceback, losing every later ticker."""
    session = _session(_row("SPY", encoded=encoded))

    with pytest.raises(DeliverableUnreadable, match=match):
        deliverable_of(session, frozenset({"SPY"}))


@pytest.mark.parametrize(
    "note, expected",
    [
        ("100 SPY", 100.0),
        ("150 SPY", 150.0),
        ("100.5 SPY", 100.5),
        ("  100 SPY  ", 100.0),
        ("100 SPY, 25.00 USD", None),
        ("150 SPY plus cash", None),
        ("SPY", None),
        ("100", None),
        ("", None),
        (None, None),
    ],
)
def test_the_note_is_read_as_a_plain_share_count_or_not_at_all(note, expected):
    """The live lake's note is ``100 SPY`` on every row of both tickers.

    Anything else is not guessed at, and the gate then has one number instead of two.
    """
    session = _session(_row("SPY", note=note))

    assert deliverable_of(session, frozenset({"SPY"})).note_units == expected


def test_a_cash_entry_beside_the_stock_is_carried_on_the_reading():
    """The shape :func:`require_scalar` refuses has to reach it to be refused."""
    encoded = json.dumps(
        [
            {
                "assetType": "STOCK",
                "currencyType": None,
                "deliverableUnits": 150.0,
                "symbol": "SPY",
            },
            {"assetType": "CURRENCY", "currencyType": "USD", "deliverableUnits": 25.0},
        ],
        sort_keys=True,
    )
    session = _session(_row("SPY1", encoded=encoded))

    reading = deliverable_of(session, frozenset({"SPY1"}))

    assert reading.cash and reading.entries == 2 and reading.units == 150.0


# -- a rename is the same deliverable twice ---------------------------------------------


def test_the_same_deliverable_written_twice_compares_equal():
    """A rename carries the same deliverable under a new symbol."""
    assert _deliverable().same_as(_deliverable())


@pytest.mark.parametrize(
    "changed",
    [
        {"units": 150.0},
        {"symbol": "XYZ"},
        {"entries": 2},
        {"cash": True},
        {"note_units": 150.0},
        {"multiplier": 150.0},
    ],
)
def test_any_field_moving_means_it_is_not_a_rename(changed: dict):
    """Every field is compared rather than ``units`` alone.

    A note that moved while the typed count did not is a vendor contradiction, and it belongs
    at the gate rather than being called a non-event here.
    """
    assert not _deliverable(**changed).same_as(_deliverable())


# -- the standard flag's two real jobs --------------------------------------------------


def test_the_standard_roots_are_what_the_prior_side_is_read_from():
    """An adjustment is a change *from* something, and the standard series names it."""
    session = _session(_row("SPY"), _row("SPY1", 150.0, note="150 SPY", non_standard=True))

    assert session.standard_roots() == frozenset({"SPY"})
    assert deliverable_of(session, session.standard_roots()).units == 100.0


def test_reading_a_two_root_session_whole_refuses_rather_than_picking_one():
    """Which is why the prior side selects rather than taking the previous session whole.

    A ticker carries a standard series beside an adjusted one from the day after any
    adjustment onwards, so this is the ordinary state rather than an edge.
    """
    session = _session(_row("SPY"), _row("SPY1", 150.0, note="150 SPY", non_standard=True))

    with pytest.raises(DeliverableUnreadable, match="disagree"):
        deliverable_of(session, session.roots)


@pytest.mark.parametrize(
    "flags, expected",
    [
        ((False,), False),
        ((True,), True),
        ((None,), None),
        ((False, False), False),
        ((True, True), True),
        ((True, False), None),
        ((True, None), None),
    ],
)
def test_a_root_set_is_standard_adjusted_or_unsaid(flags, expected):
    """Unknown is read as neither, because the two want opposite treatments.

    A gained root whose contracts are standard is a newly listed series and no corporate
    action. One whose contracts are adjusted is the split this module records. Without the
    flag nothing separates them, and a mixed or null answer is not evidence for either.
    """
    rows = [_row(f"SPY{i}", non_standard=flag) for i, flag in enumerate(flags)]
    session = _session(*rows)

    assert session.standard(session.roots) is expected


# -- the gate's three edges -------------------------------------------------------------


def test_a_new_note_of_zero_shares_does_not_divide_by_zero():
    """The comparison divides by the note ratio, so a zero on the NEW side reaches it.

    A guard reading only the prior note lets it through, and ``ZeroDivisionError`` is not a
    ``SplitError``, so it escapes the walk and ends the run. ``_NOTE`` matches ``0 SPY`` and
    only the typed count is checked for positivity, so nothing upstream refuses it.
    """
    verdict = check_split_consistency(_deliverable(), _deliverable(150.0, note_units=0.0))

    assert not verdict.agrees
    assert verdict.against is None


def test_a_note_that_overflowed_to_infinity_does_not_reach_the_division():
    """A share count of 309 digits parses and then cannot be divided by or into."""
    verdict = check_split_consistency(
        _deliverable(note_units=float("inf")), _deliverable(150.0, note_units=150.0)
    )

    assert not verdict.agrees
    assert verdict.against is None


def test_a_ratio_the_counts_cannot_represent_raises_rather_than_riding_a_finding():
    """``json.dumps`` writes a non-finite number as a bare ``Infinity``.

    No strict JSON reader accepts that, which is the same hazard ``actions.build_entry``
    refuses for the ledger. A ratio is a division, so this branch is reachable where the
    dividend gate's multiplication is not.
    """
    with pytest.raises(DeliverableUnreadable, match="no ratio a reader can represent"):
        check_split_consistency(_deliverable(units=5e-324), _deliverable(150.0, note_units=150.0))
