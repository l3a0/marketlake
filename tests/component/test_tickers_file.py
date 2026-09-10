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


# Every way a roster file can fail, and the one word that must appear in each message.
# The point of the set is that one exception type covers all of them, so a caller can
# guard for a bad roster with one `except` and `main` can print one line and exit 2.
# The two YAML shapes raise different `yaml` classes, `ParserError` and `ScannerError`,
# so the pair holds that the fold catches the base class rather than one subclass.
BROKEN = {
    "half saved mid-line": ("XYZ: {options: fal", "not valid YAML at line 1"),
    "hand-edited with a tab": ("XYZ:\n\toptions: false\n", "not valid YAML at line 2"),
    "a bare scalar": ("XYZ", "not a mapping"),
    "an entry of the wrong shape": ("XYZ: retired\n", "settings must be a mapping"),
}


@pytest.mark.parametrize("text,expected", list(BROKEN.values()), ids=list(BROKEN))
def test_every_broken_roster_raises_one_error_type(tmp_path: Path, text, expected):
    """A `yaml` error used to escape, which every caller guarding for a bad roster missed.

    `load_tickers` parses YAML and reads a file, so a half-saved roster raised
    `yaml.YAMLError` and an unreadable one raised `OSError`. Neither is a `TickersError`,
    so both went straight past `_alarm`, `_gap_marker`, `_close_guard`, and
    `input_errors_exit`, and the daemon died on a traceback naming the parser. `lake.config`
    had already solved this for `config.yaml`.
    """
    path = tmp_path / "tickers.yaml"
    path.write_text(text)

    with pytest.raises(TickersError) as caught:
        load_tickers(path)

    message = str(caught.value)
    # The fragment carries the line number for the two YAML shapes, so a fold that
    # dropped it fails here rather than passing on the word "YAML" alone.
    assert expected in message
    # The message names the file. Three operator-editable files share the config
    # directory, and this line is printed on its own.
    assert str(path) in message
    # And never a line of the file itself, which is the rule `lake.config` sets. `yaml`
    # renders the offending source line into its own message, so a fold that passed that
    # rendering through would leak it.
    assert "options" not in message
    assert "\n" not in message


@pytest.mark.parametrize("kind", ["binary", "unreadable"])
def test_a_file_that_cannot_be_read_raises_the_same_error(tmp_path: Path, kind):
    """Existing is not the same as readable, and the two ways raise different classes.

    A binary file raises `UnicodeDecodeError`, which is a `ValueError`. A file the
    process may not open raises `OSError`. Catching one and not the other leaves half
    the class escaping, so both are held.
    """
    path = tmp_path / "tickers.yaml"
    if kind == "binary":
        path.write_bytes(b"\xff\xfe\x00\x01")
    else:
        path.write_text("XYZ: {options: false}\n")
        path.chmod(0o000)
    try:
        with pytest.raises(TickersError, match="cannot be read"):
            load_tickers(path)
    finally:
        path.chmod(0o644)


def test_a_broken_roster_stops_the_write_too(tmp_path: Path):
    """`upsert_ticker` reads the file back before writing, so it has the same holes."""
    path = tmp_path / "tickers.yaml"
    path.write_text("XYZ:\n\toptions: false\n")

    with pytest.raises(TickersError, match="not valid YAML"):
        upsert_ticker("ABC", options=False, path=path)

    # The refusal left the operator's file exactly as it was, and no temp file beside it.
    assert path.read_text() == "XYZ:\n\toptions: false\n"
    assert [p.name for p in sorted(tmp_path.iterdir())] == ["tickers.yaml"]


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(TickersError):
        load_tickers(tmp_path / "none.yaml")


def test_a_reader_during_the_write_still_sees_a_whole_roster(tmp_path: Path, monkeypatch):
    """The write must not expose the file in a torn state.

    The daemon re-reads ``tickers.yaml`` on its own schedule, so it can read while the
    command writes. Truncating the file in place opens a window where a reader gets
    zero bytes or a prefix. Where the cut lands decides what the loader does, and both
    outcomes are bad. Roughly a third of a two-ticker roster's prefixes parse: an empty
    file as a roster of no tickers, a longer prefix as the tickers it kept with any cut
    key defaulted, so an options ticker comes back equity-only. A cycle handed one of
    those captures nothing for what it lost and writes no gap row for it either. The rest
    raise, and ``load_tickers`` turns that into a ``TickersError`` a caller can act on.
    The silent half is what this case is about.
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


# Every one of these parses cleanly and names no ticker. The rename in the write half
# closed the way a torn write produced one. A hand edit caught partway through a save
# still does, and so does an operator who empties the file.
NAMES_NOTHING = {
    "an empty file": "",
    "comments only": "# XYZ: {options: false}\n",
    "an explicit null": "null\n",
    "an empty mapping": "{}\n",
}


@pytest.mark.parametrize("text", NAMES_NOTHING.values(), ids=list(NAMES_NOTHING))
def test_a_file_that_names_no_tickers_raises(tmp_path: Path, text: str):
    """A roster of no tickers is the silent half the atomic write removed on its own side.

    A cycle handed one captures nothing and writes no gap row, so the minute leaves no
    trace, and the design counts completeness from rows and never from holes. That is
    the same harm `_write_atomically` names, reached through the read instead.
    """
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError) as caught:
        load_tickers(path)
    assert str(caught.value) == f"tickers file names no tickers: {path}"


# Each of these parses to a value that is neither absent nor a mapping. Folding the
# falsy ones into an empty mapping let all four load as a roster of no tickers.
NOT_A_ROSTER = {
    "a number": "0\n",
    "a bool": "false\n",
    "an empty string": "''\n",
    "an empty list": "[]\n",
}


@pytest.mark.parametrize("text", NOT_A_ROSTER.values(), ids=list(NOT_A_ROSTER))
def test_a_falsy_document_is_not_an_empty_roster(tmp_path: Path, text: str):
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError) as caught:
        load_tickers(path)
    assert str(caught.value) == f"tickers file is not a mapping: {path}"


@pytest.mark.parametrize("text", NOT_A_ROSTER.values(), ids=list(NOT_A_ROSTER))
def test_the_write_refuses_a_document_the_read_refuses(tmp_path: Path, text: str):
    # The write reads the file back first. A shape the reader will not open must stop
    # the write too, or onboarding overwrites it and the roster it replaced is gone.
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError):
        upsert_ticker("SPY", options=False, path=path)
    assert path.read_text() == text


def test_onboarding_into_an_empty_file_still_works(tmp_path: Path):
    # Where the two halves part. The reader refuses a file naming no tickers. The writer
    # takes one as no entries yet, the same as no file, because onboarding is how an
    # operator puts an entry back and refusing would block the repair.
    path = tmp_path / "tickers.yaml"
    path.write_text("")
    upsert_ticker("SPY", options=False, path=path)
    assert load_tickers(path).symbols == ("SPY",)


def test_a_roster_that_cannot_be_written_says_so(tmp_path: Path):
    # The read half folds every failure into a TickersError and the write half did not.
    # Onboarding runs under input_errors_exit, so a bare OSError printed a traceback for
    # the write where the read printed one line.
    blocked = tmp_path / "notadir"
    blocked.write_text("this is a file, not a directory\n")
    target = blocked / "tickers.yaml"
    with pytest.raises(TickersError) as caught:
        upsert_ticker("SPY", options=False, path=target)
    assert str(caught.value) == f"tickers file cannot be written: {target}"
    # The temp file's name is an implementation detail and never rides out in the line.
    assert "tmp-" not in str(caught.value)
