"""The roster loaded from a real tickers.yaml, with env-var and argument overrides."""

from __future__ import annotations

from pathlib import Path

import pytest

from lake.tickers import TickersError, load_tickers

YAML = """\
SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}
QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}
"""


def test_load_roster_from_a_file(tmp_path: Path):
    path = tmp_path / "tickers.yaml"
    path.write_text(YAML)
    roster = load_tickers(path)
    assert roster.symbols == ("SPY", "QQQ")
    spy = roster.get("SPY")
    assert spy.options is True
    assert spy.chain_cadence == "1m"
    assert spy.bars == ("1m", "1d")


def test_env_var_points_the_loader_at_a_file(tmp_path: Path):
    path = tmp_path / "roster.yaml"
    path.write_text(YAML)
    roster = load_tickers(env={"MARKETLAKE_TICKERS": str(path)})
    assert len(roster) == 2


def test_a_typed_tilde_still_expands(tmp_path: Path, monkeypatch):
    # An argument and an environment override are whatever a person typed, so both
    # expand. Only the default comes from lake.paths already resolved. Removing the
    # expansion here would make the loader open a literal "~" directory.
    monkeypatch.setenv("HOME", str(tmp_path))
    path = tmp_path / ".config" / "roster.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(YAML)
    assert len(load_tickers("~/.config/roster.yaml")) == 2
    assert len(load_tickers(env={"MARKETLAKE_TICKERS": "~/.config/roster.yaml"})) == 2


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(TickersError):
        load_tickers(tmp_path / "none.yaml")
