"""The roster over a real tickers.yaml: the loader, its overrides, and the write.

The daemon re-reads this file while the onboarding command writes it, so the write has
to be atomic. The last case here holds that half.
"""

from __future__ import annotations

import builtins
import io
from pathlib import Path

import pytest

from lake.tickers import TickersError, load_tickers, upsert_ticker

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


def test_a_reader_during_the_write_still_sees_a_whole_roster(tmp_path: Path, monkeypatch):
    """The write must not expose the file in a torn state.

    The daemon re-reads ``tickers.yaml`` on its own schedule, so it can read while the
    command writes. Truncating the file in place opens a window where a reader gets
    zero bytes or a prefix. Some of those shapes the loader accepts. An empty file loads
    as a roster of no tickers. A prefix ending on a line boundary loads as the tickers it
    kept, with any cut key defaulted, so an options ticker can come back equity-only. A
    torn read is then silently wrong rather than an error, and a cycle handed one captures
    nothing for what it lost and writes no gap row for it either.
    """
    path = tmp_path / "tickers.yaml"
    upsert_ticker("XYZ", options=False, path=path)
    before = path.read_text()

    seen: list[str | None] = []
    real_open = builtins.open

    def watching_open(file, mode="r", *args, **kwargs):
        """Record what the roster holds just after a file is opened for writing.

        Opening for writing is the truncating step, so the read has to happen after it.
        Reading before would see the untouched file whichever way the write is done.
        """
        handle = real_open(file, mode, *args, **kwargs)
        if "w" in mode:
            seen.append(path.read_text() if path.exists() else None)
        return handle

    monkeypatch.setattr(builtins, "open", watching_open)
    monkeypatch.setattr(io, "open", watching_open)
    upsert_ticker("ABC", options=False, path=path)

    # One file was opened for writing, and it was not the roster: the roster still held
    # every byte of the previous version at that instant.
    assert seen == [before]
    assert load_tickers(path).symbols == ("ABC", "XYZ")
    # The temp file the write goes through is gone, not left beside the roster.
    assert [p.name for p in sorted(tmp_path.iterdir())] == ["tickers.yaml"]
