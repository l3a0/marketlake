"""The roster loaded from a real tickers.yaml, with env-var and argument overrides."""

from __future__ import annotations

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


# -- the ways a hand-edited file goes bad ---------------------------------------------

# A save caught mid-keystroke is the likeliest bad roster, and every one of these is a
# parse error rather than a bad shape. The loader must answer TickersError for all of
# them, since that is the only exception its callers catch.
HALF_SAVED = {
    "truncated mid-value": "SPY: {options: fal",
    "a tab where spaces belong": "SPY:\n\toptions: false\n",
    "an unclosed quote": "SPY: {options: 'false}\n",
    "two documents": "SPY: {options: true}\n---\nQQQ: {options: true}\n",
}


@pytest.mark.parametrize("text", HALF_SAVED.values(), ids=list(HALF_SAVED))
def test_a_file_caught_mid_save_raises_tickers_error(tmp_path: Path, text: str):
    # Unguarded, each of these raises yaml.YAMLError, which walks past input_errors_exit
    # and past every guard in the daemon. All of them catch TickersError.
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    assert str(path) in str(excinfo.value)


def test_a_parse_error_names_the_spot_on_one_line(tmp_path: Path):
    # tickers.yaml holds no secrets, so unlike config.yaml the loader may quote what
    # PyYAML complained about. input_errors_exit prints one line per bad file, and
    # PyYAML's own message runs to eight, echoing the offending source line under each
    # position with a caret beneath the column.
    path = tmp_path / "tickers.yaml"
    path.write_text("SPY: {options: true}\nQQQ: {options: fal\n")
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    message = str(excinfo.value)
    assert "\n" not in message
    assert "<unicode string>" not in message
    # Line 2 column 6 is the unclosed brace, not the end of file the parser gave up at.
    # A truncation always trips at the end, so the position that finds the typo is the
    # one PyYAML files under context.
    assert "at line 2, column 6" in message
    assert "at line 3, column 1" in message


def test_an_unreadable_file_raises_tickers_error(tmp_path: Path):
    # exists() passing does not mean the file can be read. A path one character short of
    # the file names its directory, which raised a bare IsADirectoryError before.
    directory = tmp_path / "tickers.yaml"
    directory.mkdir()
    with pytest.raises(TickersError) as excinfo:
        load_tickers(directory)
    assert str(excinfo.value) == f"tickers file cannot be read: {directory}"


# Every one of these parses cleanly and names no ticker. A truncation to zero bytes is
# the first of them, and the rest are the same thing said differently.
NAMES_NOTHING = {
    "an empty file": "",
    "comments only": "# SPY: {options: true}\n",
    "an explicit null": "null\n",
    "an empty mapping": "{}\n",
}


@pytest.mark.parametrize("text", NAMES_NOTHING.values(), ids=list(NAMES_NOTHING))
def test_a_file_that_names_no_tickers_raises(tmp_path: Path, text: str):
    # A zero-ticker roster is the quiet half of a half-finished save. A cycle over no
    # tickers journals nothing, so the watchdog charges no ticker and the dead-man goes
    # unfed. Nothing writes an empty file, so it is a truncation and it says so.
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    assert str(excinfo.value) == f"tickers file names no tickers: {path}"


def test_a_document_that_is_not_a_mapping_still_says_so(tmp_path: Path):
    # The shape check predates the parse guard and keeps its own line. A list parses
    # cleanly and is not empty, so neither new branch may swallow it.
    path = tmp_path / "tickers.yaml"
    path.write_text("- SPY\n- QQQ\n")
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    assert str(excinfo.value) == f"tickers file is not a mapping: {path}"


# -- the writer half -------------------------------------------------------------------


def test_onboarding_refuses_to_clobber_a_roster_it_cannot_read(tmp_path: Path):
    # upsert_ticker reads the whole file and writes the whole file back. Parsing past a
    # broken file would drop every other ticker in it, so it stops instead.
    path = tmp_path / "tickers.yaml"
    path.write_text("SPY: {options: true}\nQQQ: {options: fal")
    with pytest.raises(TickersError):
        upsert_ticker("IWM", options=False, path=path)
    assert path.read_text() == "SPY: {options: true}\nQQQ: {options: fal"


def test_onboarding_into_an_empty_file_still_works(tmp_path: Path):
    # The deliberate asymmetry with the reader. An empty file and no file both mean no
    # entries yet. Onboarding is how an operator puts an entry back, so refusing here
    # would block the repair.
    path = tmp_path / "tickers.yaml"
    path.write_text("")
    upsert_ticker("SPY", options=False, path=path)
    assert load_tickers(path).symbols == ("SPY",)


def test_a_parse_error_with_no_line_and_column_still_names_the_file(tmp_path: Path):
    # A control byte raises yaml.reader.ReaderError, the one load error PyYAML raises
    # without marks. It takes the fallback branch, where PyYAML names the stream inside
    # its own message. Handed a bare string PyYAML invents "<unicode string>", a file
    # the operator never typed. A truncated write is one way a NUL byte lands.
    path = tmp_path / "tickers.yaml"
    path.write_bytes(b"SPY: {options: false}\x00")
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    message = str(excinfo.value)
    assert "\n" not in message
    assert "<unicode string>" not in message
    assert "special characters are not allowed at position" in message


def test_a_complaint_half_with_no_mark_still_reads(tmp_path: Path):
    # A tab carries a problem mark and no context mark, so the context half prints bare.
    # The other new cases all carry both, which would leave this branch unasserted.
    path = tmp_path / "tickers.yaml"
    path.write_text("SPY:\n\toptions: false\n")
    with pytest.raises(TickersError) as excinfo:
        load_tickers(path)
    message = str(excinfo.value)
    assert "while scanning for the next token, found character" in message
    assert "at line 2, column 1" in message


# Each of these parses to something that is neither absent nor a mapping. The reader
# refuses every one, so the writer must refuse them too rather than overwrite a file
# the reader would not open.
NOT_A_ROSTER = {"a list": "[]\n", "a number": "0\n", "a bool": "false\n", "a string": "''\n"}


@pytest.mark.parametrize("text", NOT_A_ROSTER.values(), ids=list(NOT_A_ROSTER))
def test_onboarding_refuses_a_document_the_reader_refuses(tmp_path: Path, text: str):
    path = tmp_path / "tickers.yaml"
    path.write_text(text)
    with pytest.raises(TickersError):
        load_tickers(path)
    with pytest.raises(TickersError):
        upsert_ticker("SPY", options=False, path=path)
    assert path.read_text() == text


def test_a_roster_that_cannot_be_written_says_so(tmp_path: Path):
    # Onboarding turns a TickersError into one named line. A raw OSError from the write
    # would print a traceback where the read now prints a line.
    blocked = tmp_path / "notadir"
    blocked.write_text("this is a file, not a directory\n")
    with pytest.raises(TickersError) as excinfo:
        upsert_ticker("SPY", options=False, path=blocked / "tickers.yaml")
    assert str(excinfo.value) == f"tickers file cannot be written: {blocked / 'tickers.yaml'}"
