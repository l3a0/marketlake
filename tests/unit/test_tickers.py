"""The roster resolved from values alone: typed entries and their defaults."""

from __future__ import annotations

import pytest

from lake.tickers import Roster, TickerConfig, TickersError


def test_full_entry_parses_every_field():
    roster = Roster.from_mapping(
        {"SPY": {"options": True, "chain_cadence": "1m", "bars": ["1m", "1d"]}}
    )
    assert roster.get("SPY") == TickerConfig(
        "SPY", options=True, chain_cadence="1m", bars=("1m", "1d")
    )


def test_equity_only_entry_has_no_cadence():
    roster = Roster.from_mapping({"XYZ": {"options": False, "bars": ["1d"]}})
    xyz = roster.get("XYZ")
    assert xyz.options is False
    assert xyz.chain_cadence is None
    assert xyz.bars == ("1d",)


def test_absent_fields_fall_back_to_defaults():
    bare = Roster.from_mapping({"BARE": {}}).get("BARE")
    assert bare.options is False
    assert bare.chain_cadence is None
    assert bare.bars == ()


def test_symbols_preserve_file_order():
    roster = Roster.from_mapping({"SPY": {}, "QQQ": {}, "IWM": {}})
    assert roster.symbols == ("SPY", "QQQ", "IWM")


def test_len_counts_entries():
    assert len(Roster.from_mapping({"SPY": {}, "QQQ": {}})) == 2


def test_iteration_yields_entries_in_order():
    roster = Roster.from_mapping({"SPY": {}, "QQQ": {}})
    assert [entry.ticker for entry in roster] == ["SPY", "QQQ"]


def test_get_unknown_ticker_raises():
    roster = Roster.from_mapping({"SPY": {}})
    with pytest.raises(TickersError):
        roster.get("QQQ")


def test_settings_must_be_a_mapping():
    with pytest.raises(TickersError):
        Roster.from_mapping({"SPY": [1, 2, 3]})


def test_bars_must_be_a_list_not_a_string():
    with pytest.raises(TickersError):
        Roster.from_mapping({"SPY": {"bars": "1m"}})
