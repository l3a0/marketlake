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


@pytest.mark.parametrize(
    ("name", "text"),
    [
        ("an unclosed flow mapping", "SPY: {options: false\n"),
        ("a tab for indentation", "SPY:\n\t options: false\n"),
    ],
)
def test_malformed_yaml_raises_the_module_s_own_error(tmp_path: Path, name, text):
    """A parse error must arrive as a ``TickersError``, because two callers count on it.

    ``config.input_errors_exit`` catches ``TickersError`` alone, so a raw
    ``yaml.YAMLError`` is the traceback it exists to replace. The daemon re-reads this
    file while it runs, from hooks that are not guarded, so the same escape is a crash
    loop under ``KeepAlive``.
    """
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError):
        load_tickers(path)


def test_a_file_that_exists_but_cannot_be_read_raises_the_same_error(tmp_path: Path):
    """Existing is not readable. A directory and a binary file both take this path."""
    directory = tmp_path / "tickers.yaml"
    directory.mkdir()
    with pytest.raises(TickersError):
        load_tickers(directory)

    binary = tmp_path / "binary.yaml"
    binary.write_bytes(b"\xff\xfe: not text\n")
    with pytest.raises(TickersError):
        load_tickers(binary)
