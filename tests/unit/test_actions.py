"""The corporate-actions entry, in memory.

A ledger line carries no schema of its own, so the type discipline Parquet gave for free
lives in this module instead. These tests cover the two halves of it: the date
normalization that keeps one event on one key, and the write-side validation that makes a
malformed entry unassemblable. Nothing here touches disk.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from lake.actions import (
    ACTIONS_SCHEMA_VERSION,
    PROVENANCE_MANUAL,
    PROVENANCE_OBSERVED,
    PROVENANCE_VENDOR_REPORTED,
    TYPE_DIVIDEND,
    TYPE_SPLIT,
    LedgerLineError,
    build_entry,
    entry_key,
    normalize_date,
)

RECORDED = datetime(2026, 6, 19, 23, 0, tzinfo=UTC)
LEDGER = Path("actions/corporate_actions.jsonl")


def _dividend(**overrides) -> dict:
    fields = {
        "instrument_id": 1,
        "observed_on": date(2026, 9, 14),
        "recorded_at": RECORDED,
        "ex_date": "2026-06-18",
        "type": TYPE_DIVIDEND,
        "pay_date": "2026-07-31",
        "declared_date": "2026-01-02",
        "cash_amount": 1.90352,
        "provenance": PROVENANCE_VENDOR_REPORTED,
    }
    fields.update(overrides)
    return build_entry(**fields)


# -- normalizing a date --------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        date(2026, 6, 18),
        "2026-06-18",
        "2026-06-18T00:00:00Z",
        "2026-06-18T00:00:00+00:00",
        "2026-06-18T00:00:00.000Z",
        "2026-06-18T00:00:00-04:00",
        datetime(2026, 6, 18, 0, 0, tzinfo=UTC),
        "  2026-06-18  ",
    ],
)
def test_every_spelling_of_one_date_renders_the_same(value):
    assert normalize_date(value) == "2026-06-18"


def test_a_midnight_timestamp_keeps_its_own_calendar_date():
    # A date spelled at midnight under an offset names that date. Converting it to UTC
    # first would move it to the 19th, which is a different event.
    assert normalize_date("2026-06-18T00:00:00-04:00") == "2026-06-18"
    assert normalize_date(datetime(2026, 6, 18, 0, 0, tzinfo=timezone(timedelta(hours=9)))) == (
        "2026-06-18"
    )


@pytest.mark.parametrize(
    "value",
    ["2026-06-18T16:15:00Z", "2026-06-18T00:00:01Z", "June 18 2026", "", "2026-06-31"],
)
def test_a_value_that_does_not_name_a_date_raises(value):
    # A timestamp carrying a time of day is not a spelling of a date, and choosing which
    # day it names would be a silent guess inside the resolution key.
    with pytest.raises(ValueError):
        normalize_date(value)


def test_the_three_vendor_dates_are_normalized_on_write():
    entry = _dividend(
        ex_date="2026-06-18T00:00:00Z",
        pay_date="2026-07-31T00:00:00Z",
        declared_date="2026-01-02T00:00:00Z",
    )
    assert entry["ex_date"] == "2026-06-18"
    assert entry["pay_date"] == "2026-07-31"
    assert entry["declared_date"] == "2026-01-02"


# -- the entry -----------------------------------------------------------------


def test_the_entry_carries_the_eleven_fields():
    assert set(_dividend()) == {
        "instrument_id",
        "observed_on",
        "recorded_at",
        "ex_date",
        "pay_date",
        "declared_date",
        "type",
        "cash_amount",
        "split_ratio",
        "provenance",
        "schema_version",
    }


def test_there_is_no_ticker_field():
    # The master exists because tickers change, FB to META and QQQQ to QQQ. A ledger is
    # partitioned by nothing, so a stored ticker would be a mutable key in a record built
    # to avoid them. ``symbol_at`` recovers it.
    assert "ticker" not in _dividend()


def test_a_dividend_fills_its_three_dates_and_leaves_the_ratio_null():
    entry = _dividend()
    assert entry["cash_amount"] == 1.90352
    assert entry["split_ratio"] is None
    assert entry["schema_version"] == ACTIONS_SCHEMA_VERSION


def test_a_split_fills_the_ratio_and_leaves_the_three_dividend_fields_null():
    # Marketlake #279 writes these entries and should not have to infer what a split
    # leaves empty. A split pays nothing, and Schwab's fundamentals carry no announcement
    # date for one.
    entry = build_entry(
        instrument_id=1,
        observed_on=date(2026, 9, 14),
        recorded_at=RECORDED,
        ex_date="2026-06-18",
        type=TYPE_SPLIT,
        split_ratio=4.0,
        provenance=PROVENANCE_OBSERVED,
    )
    assert entry["split_ratio"] == 4.0
    assert entry["cash_amount"] is None
    assert entry["pay_date"] is None
    assert entry["declared_date"] is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"cash_amount": None},
        {"split_ratio": 2.0},
    ],
)
def test_a_dividend_that_breaks_the_either_or_rule_refuses(overrides):
    with pytest.raises(ValueError):
        _dividend(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"split_ratio": None},
        {"split_ratio": 2.0, "cash_amount": 1.0},
        {"split_ratio": 2.0, "cash_amount": None, "pay_date": "2026-07-31"},
        {"split_ratio": 2.0, "cash_amount": None, "declared_date": "2026-01-02"},
    ],
)
def test_a_split_that_breaks_the_either_or_rule_refuses(overrides):
    fields = {
        "cash_amount": None,
        "pay_date": None,
        "declared_date": None,
        "type": TYPE_SPLIT,
        "provenance": PROVENANCE_OBSERVED,
    }
    fields.update(overrides)
    with pytest.raises(ValueError):
        _dividend(**fields)


@pytest.mark.parametrize(
    "overrides",
    [
        {"type": "spinoff"},
        # A kind nothing recognises, filled the way a split is, so the only thing left to
        # refuse it is the fixed vocabulary. Marketlake #279 writes this same field.
        {
            "type": "spinoff",
            "split_ratio": 2.0,
            "cash_amount": None,
            "pay_date": None,
            "declared_date": None,
        },
        {"provenance": "guessed"},
        {"instrument_id": "1"},
        {"instrument_id": True},
        {"cash_amount": "1.90"},
        {"schema_version": "1"},
    ],
)
def test_an_unassemblable_entry_refuses(overrides):
    # The typed keyword-only signature is what ``manifest.append_manifest`` does for its
    # own entries, and it is half of what a ledger line loses when it stops being a
    # Parquet row.
    with pytest.raises(ValueError):
        _dividend(**overrides)


def test_a_manual_entry_may_carry_a_null_observed_on():
    # Nothing here writes one. The value is reserved so the schema does not move when
    # marketlake #286's command lands.
    entry = _dividend(observed_on=None, provenance=PROVENANCE_MANUAL)
    assert entry["observed_on"] is None
    assert entry["provenance"] == PROVENANCE_MANUAL


def test_recorded_at_must_be_timezone_aware():
    # A naive stamp cannot be placed on a market day, so the as-of read could not filter
    # on it.
    with pytest.raises(ValueError):
        _dividend(recorded_at=datetime(2026, 6, 19, 19, 0))


def test_recorded_at_is_stored_in_utc():
    eastern = timezone(timedelta(hours=-4))
    entry = _dividend(recorded_at=datetime(2026, 6, 19, 19, 0, tzinfo=eastern))
    assert entry["recorded_at"] == "2026-06-19T23:00:00+00:00"


# -- the key -------------------------------------------------------------------


def test_the_key_is_the_instrument_the_ex_date_and_the_type():
    assert entry_key(_dividend(), path=LEDGER, position=1) == (1, "2026-06-18", TYPE_DIVIDEND)


@pytest.mark.parametrize(
    "entry",
    [
        {"ex_date": "2026-06-18", "type": TYPE_DIVIDEND},
        {"instrument_id": 1, "type": TYPE_DIVIDEND},
        {"instrument_id": 1, "ex_date": "2026-06-18"},
        {"instrument_id": True, "ex_date": "2026-06-18", "type": TYPE_DIVIDEND},
        {"instrument_id": "1", "ex_date": "2026-06-18", "type": TYPE_DIVIDEND},
        {"instrument_id": 1, "ex_date": 20260618, "type": TYPE_DIVIDEND},
        {"instrument_id": 1, "ex_date": "2026-06-18", "type": None},
        ["not", "an", "object"],
    ],
)
def test_a_line_carrying_no_usable_key_raises_at_its_position(entry):
    with pytest.raises(LedgerLineError) as raised:
        entry_key(entry, path=LEDGER, position=7)
    assert raised.value.position == 7
    assert "entry 7" in str(raised.value)
