"""The comparable set's ranking, as a pure function over values.

Unit tier, because the ranking touches no file. It is tested apart from ``oi_view``
because the property that matters here is reproducibility, and a reproducible set and an
arbitrary one give the same verdict on any single fixture. Only comparing two runs, or
inspecting the order itself, can tell them apart.

The tie is real rather than theoretical. Two to four contracts shared the boundary volume
at rank 1,000 on each of the four sealed close cycles the lake held on 2026-09-16, so a
set sized anywhere near there lands on one without trying.
"""

from __future__ import annotations

from datetime import date

from lake.oi import _comparable_set, _Contract, _expires_after


def contract(symbol: str, volume: int, *, survives: bool = True) -> _Contract:
    return _Contract(
        occ_symbol=symbol,
        open_interest=1,
        volume=volume,
        expires_after_session=survives,
    )


def test_the_set_ranks_by_volume_descending():
    roster = (
        contract("SPY   261016C00000001", 10),
        contract("SPY   261016C00000002", 30),
        contract("SPY   261016C00000003", 20),
    )

    ranked = _comparable_set(roster, 3)

    assert [row.volume for row in ranked] == [30, 20, 10]


def test_equal_volumes_break_on_the_occ_symbol():
    """Without a tie-break the boundary is whatever order the reader happened to return.

    Three contracts share the boundary volume and only two fit. The pair that ranks in is
    the two lexicographically smallest symbols, on every run and from any input order.
    """
    low = contract("SPY   261016C00000001", 5)
    middle = contract("SPY   261016C00000002", 5)
    high = contract("SPY   261016C00000003", 5)
    top = contract("SPY   261016C00000009", 99)

    forwards = _comparable_set((top, low, middle, high), 3)
    backwards = _comparable_set((high, middle, low, top), 3)

    assert [row.occ_symbol for row in forwards] == [
        "SPY   261016C00000009",
        "SPY   261016C00000001",
        "SPY   261016C00000002",
    ]
    assert forwards == backwards


def test_a_contract_expiring_on_or_before_the_session_never_ranks_in():
    """The busiest contracts at a close are often the ones expiring that day.

    They leave the next chain entirely, so a set built from them would have no voters to
    compare. 27 of the 50 highest-volume contracts in SPY's 2026-09-14 close expired that
    same day.
    """
    roster = (
        contract("SPY   260914C00000001", 10_000, survives=False),
        contract("SPY   261016C00000002", 5),
    )

    ranked = _comparable_set(roster, 10)

    assert [row.occ_symbol for row in ranked] == ["SPY   261016C00000002"]


def test_the_set_is_capped_at_the_configured_size():
    roster = tuple(contract(f"SPY   261016C{index:08d}", 100 - index) for index in range(50))

    assert len(_comparable_set(roster, 10)) == 10
    assert len(_comparable_set(roster, 50)) == 50
    assert len(_comparable_set(roster, 500)) == 50


def test_an_unreadable_expiration_keeps_a_contract_out_of_the_set():
    """A value that cannot be read as a date has not been shown to survive the session.

    The contract stays in the roster, where it can still take a settled figure, and out of
    the set, where it would be a voter nothing has shown will be there to vote.
    """
    session = date(2026, 9, 14)

    assert _expires_after("2026-09-18T20:00:00.000+00:00", session) is True
    assert _expires_after("2026-09-14T20:00:00.000+00:00", session) is False
    assert _expires_after("not a date", session) is False
    assert _expires_after(None, session) is False
    assert _expires_after(20260918, session) is False
