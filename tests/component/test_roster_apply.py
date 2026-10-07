"""``python -m lake.roster apply`` copies the reviewed roster onto a host.

The repository tracks the roster as ``config/tickers.yaml``, and every reader on a host
keeps reading ``~/.config/marketlake/tickers.yaml``. These tests drive the copy against
the real filesystem: what it writes, what it refuses, and what it leaves behind when it
refuses. Each fixture is a literal, so none moves with the code under test.
"""

from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from lake import roster as roster_cli
from lake.config import ConfigError
from lake.paths import CONFIG_DIR_ENV
from lake.roster import REPLACED, UNCHANGED, RosterError, apply
from lake.security_master import master_path
from lake.tickers import (
    TICKERS_PATH_ENV,
    TickersError,
    apply_roster,
    default_tickers_path,
    roster_from_bytes,
    tickers_file_path,
)
from tests.support.config import write_config

# A roster formatted the way no ``yaml.safe_dump`` call writes it: a comment, flow
# mappings, file order that is not sorted, and padding inside the braces. Re-serializing
# it would change every one of those, so a byte comparison catches a rewrite.
HAND_FORMATTED = (
    b"# The capture roster, as reviewed.\n"
    b"SPY: { options: true, chain_cadence: 1m, bars: [1m, 1d] }\n"
    b"QQQ: { options: true, chain_cadence: 1m, bars: [1m, 1d] }  # second anchor\n"
)

# A different valid roster, standing in for what a host held before a merge.
PREVIOUS = b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"


def _accept(roster) -> None:
    """A lake check that refuses nothing."""


class _Refused(Exception):
    """What a refusing lake check raises in these tests."""


def _refuse(roster) -> None:
    raise _Refused("the lake owes a ticker this roster drops")


def _listing(directory: Path) -> list[str]:
    return sorted(entry.name for entry in directory.iterdir())


# -- the core: tickers.apply_roster ---------------------------------------------------


def test_a_roster_is_written_byte_for_byte(tmp_path):
    target = tmp_path / "tickers.yaml"
    assert apply_roster(HAND_FORMATTED, check=_accept, path=target) is True
    assert target.read_bytes() == HAND_FORMATTED


@pytest.mark.parametrize(
    ("payload", "named"),
    [
        pytest.param(b"SPY: {options: true, bars: [1m\n", "not valid YAML", id="bad-yaml"),
        pytest.param(b"- SPY\n- QQQ\n", "not a mapping", id="not-a-mapping"),
        pytest.param(
            b"SPY: {options: true, bars: [1m, 1d]}  # caf\xe9\n", "not UTF-8", id="not-utf8"
        ),
    ],
)
def test_a_payload_that_is_not_a_roster_writes_nothing(tmp_path, payload, named):
    target = tmp_path / "tickers.yaml"
    with pytest.raises(TickersError, match=named):
        apply_roster(payload, check=_accept, path=target)
    assert _listing(tmp_path) == []


def test_the_check_sees_the_disabled_entries_too(tmp_path):
    payload = (
        b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
        b"QQQ: {options: false, bars: [1d], enabled: false}\n"
    )
    seen = []
    apply_roster(payload, check=lambda r: seen.append(r.symbols), path=tmp_path / "tickers.yaml")
    assert seen == [("SPY", "QQQ")]


def test_a_differing_roster_replaces_the_file(tmp_path):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(PREVIOUS)
    before = target.stat().st_ino
    assert apply_roster(HAND_FORMATTED, check=_accept, path=target) is True
    assert target.read_bytes() == HAND_FORMATTED
    assert target.stat().st_ino != before
    assert _listing(tmp_path) == ["tickers.yaml"]


def test_an_equal_roster_is_left_alone(tmp_path):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(HAND_FORMATTED)
    before = target.stat()
    assert apply_roster(HAND_FORMATTED, check=_accept, path=target) is False
    after = target.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert _listing(tmp_path) == ["tickers.yaml"]


@pytest.mark.parametrize(
    ("payload", "check", "raised"),
    [
        pytest.param(HAND_FORMATTED, _refuse, _Refused, id="check-refuses"),
        pytest.param(b"", _accept, TickersError, id="empty-document"),
        pytest.param(b"SPY: {bars: [1d], enabled: false}\n", _accept, TickersError, id="disabled"),
    ],
)
def test_a_refused_roster_leaves_the_file_unchanged(tmp_path, payload, check, raised):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(PREVIOUS)
    before = target.stat().st_ino
    with pytest.raises(raised):
        apply_roster(payload, check=check, path=target)
    assert target.read_bytes() == PREVIOUS
    assert target.stat().st_ino == before
    assert _listing(tmp_path) == ["tickers.yaml"]


def test_the_check_runs_when_the_bytes_already_match(tmp_path):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(HAND_FORMATTED)
    seen = []
    replaced = apply_roster(HAND_FORMATTED, check=lambda r: seen.append(r.symbols), path=target)
    assert replaced is False
    assert seen == [("SPY", "QQQ")]


def test_a_check_refusing_an_equal_roster_still_refuses(tmp_path):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(HAND_FORMATTED)
    with pytest.raises(_Refused):
        apply_roster(HAND_FORMATTED, check=_refuse, path=target)
    assert target.read_bytes() == HAND_FORMATTED


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty-document"),
        pytest.param(
            b"SPY: {options: true, chain_cadence: 1m, bars: [1m], enabled: false}\n"
            b"QQQ: {options: false, bars: [1d], enabled: false}\n",
            id="all-disabled",
        ),
    ],
)
def test_the_no_enabled_refusal_runs_before_the_check(tmp_path, payload):
    # The check refuses too, with an error that is not a TickersError. If the check ran
    # first, its refusal is what the operator would see instead of the one naming the
    # empty roster.
    target = tmp_path / "tickers.yaml"
    with pytest.raises(TickersError, match="no enabled ticker"):
        apply_roster(payload, check=_refuse, path=target)
    assert _listing(tmp_path) == []


def test_a_failed_rename_leaves_the_file_and_no_temp_file(tmp_path, monkeypatch):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(PREVIOUS)

    def broken_replace(src, dst):
        raise OSError("rename failed")

    monkeypatch.setattr(os, "replace", broken_replace)
    with pytest.raises(TickersError, match="cannot be written"):
        apply_roster(HAND_FORMATTED, check=_accept, path=target)
    monkeypatch.undo()
    assert target.read_bytes() == PREVIOUS
    assert _listing(tmp_path) == ["tickers.yaml"]


@pytest.mark.parametrize(
    "held",
    [
        pytest.param(
            yaml.safe_dump(yaml.safe_load(HAND_FORMATTED.decode())).encode(),
            id="same-content-re-serialized",
        ),
        pytest.param(HAND_FORMATTED + b"\n", id="one-more-newline"),
    ],
)
def test_the_same_roster_in_different_bytes_is_replaced(tmp_path, held):
    # The host's copy has to be the reviewed file byte for byte, so equal content in
    # other bytes is a difference. A comparison of parsed documents or of stripped text
    # would call each of these unchanged and leave the host's spelling in place.
    assert held != HAND_FORMATTED
    target = tmp_path / "tickers.yaml"
    target.write_bytes(held)
    assert apply_roster(HAND_FORMATTED, check=_accept, path=target) is True
    assert target.read_bytes() == HAND_FORMATTED


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 file")
def test_an_unreadable_host_roster_is_refused_rather_than_overwritten(tmp_path):
    target = tmp_path / "tickers.yaml"
    target.write_bytes(PREVIOUS)
    target.chmod(0)
    try:
        with pytest.raises(TickersError, match="cannot be read"):
            apply_roster(HAND_FORMATTED, check=_accept, path=target)
    finally:
        target.chmod(0o600)
    assert target.read_bytes() == PREVIOUS
    assert _listing(tmp_path) == ["tickers.yaml"]


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"[]\n", id="empty-list"),
        pytest.param(b"false\n", id="false"),
        pytest.param(b"0\n", id="zero"),
        pytest.param(b"''\n", id="empty-string"),
    ],
)
def test_a_falsy_document_is_not_a_mapping(payload):
    # Only an absent document reads as no entries. Folding every falsy value into one
    # would accept these four as an empty roster.
    with pytest.raises(TickersError, match="not a mapping"):
        roster_from_bytes(payload)


def test_a_bad_entry_names_standard_input_as_its_source():
    with pytest.raises(TickersError) as excinfo:
        roster_from_bytes(b"SPY: [1m]\n")
    assert str(excinfo.value).endswith(" in tickers file: <stdin>")


def test_bad_yaml_names_standard_input_as_its_source():
    with pytest.raises(TickersError) as excinfo:
        roster_from_bytes(b"SPY: {options: true, bars: [1m\n")
    message = str(excinfo.value)
    assert message.startswith("tickers file is not valid YAML")
    assert message.endswith(": <stdin>")


def test_a_roster_with_no_enabled_ticker_names_standard_input_as_its_source(tmp_path):
    with pytest.raises(TickersError) as excinfo:
        apply_roster(b"", check=_accept, path=tmp_path / "tickers.yaml")
    assert str(excinfo.value).endswith("capture nothing: <stdin>")


def test_a_host_roster_that_is_a_directory_is_refused(tmp_path):
    # Reading a directory raises ``IsADirectoryError``, an ``OSError`` that is not a
    # permission error, so a handler narrowed to the chmod case lets it escape bare.
    target = tmp_path / "tickers.yaml"
    target.mkdir()
    with pytest.raises(TickersError, match="cannot be read"):
        apply_roster(HAND_FORMATTED, check=_accept, path=target)
    assert target.is_dir()


def test_an_equal_crlf_roster_is_left_alone(tmp_path):
    # The host's copy is compared as bytes. Read as text, each CRLF turns into LF, the
    # copy stops matching the payload, and an unchanged roster is rewritten every apply.
    payload = (
        b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\r\n"
        b"QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\r\n"
    )
    target = tmp_path / "tickers.yaml"
    target.write_bytes(payload)
    before = target.stat().st_ino
    assert apply_roster(payload, check=_accept, path=target) is False
    assert target.stat().st_ino == before
    assert target.read_bytes() == payload


def test_an_explicit_path_wins_over_the_environment(tmp_path, monkeypatch):
    from_env = tmp_path / "env.yaml"
    from_arg = tmp_path / "arg.yaml"
    monkeypatch.setenv(TICKERS_PATH_ENV, str(from_env))
    assert apply_roster(HAND_FORMATTED, check=_accept, path=from_arg) is True
    assert from_arg.read_bytes() == HAND_FORMATTED
    assert not from_env.exists()


def test_an_empty_environment_override_falls_back_to_the_default():
    # An empty value is unset, not a path. Read as a path it is the current directory.
    assert tickers_file_path(env={TICKERS_PATH_ENV: ""}) == default_tickers_path()


def test_apply_creates_every_missing_parent_directory(tmp_path):
    target = tmp_path / "a" / "b" / "tickers.yaml"
    assert apply_roster(HAND_FORMATTED, check=_accept, path=target) is True
    assert target.read_bytes() == HAND_FORMATTED


def test_the_default_path_follows_the_config_directory_on_every_call(tmp_path, monkeypatch):
    # A cached path would resolve once and keep the first directory after a redirect,
    # which is the failure that overwrote a host's live roster on 2026-10-06.
    monkeypatch.delenv(TICKERS_PATH_ENV, raising=False)
    monkeypatch.setenv(CONFIG_DIR_ENV, str(tmp_path / "a"))
    assert tickers_file_path() == tmp_path / "a" / "tickers.yaml"
    monkeypatch.setenv(CONFIG_DIR_ENV, str(tmp_path / "b"))
    assert tickers_file_path() == tmp_path / "b" / "tickers.yaml"


# -- the command: lake.roster.apply ---------------------------------------------------


@pytest.fixture
def config_path(tmp_path) -> Path:
    # A shadow host whose lake holds no reference files, so the lake check skips and these
    # tests reach the steps around it. ``test_roster_lake_check.py`` drives the check.
    return write_config(tmp_path / "cfg", tmp_path / "lake", role="shadow")


def _skipped(lake: Path) -> str:
    """The line the lake check prints when it skips on a shadow host with an empty lake."""
    return (
        f"roster: lake check skipped, because {master_path(lake)} does not exist "
        "and this host's role is shadow\n"
    )


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty-document"),
        pytest.param(b"# no tickers yet\n", id="comment-only"),
        pytest.param(b"{}\n", id="empty-mapping"),
        pytest.param(
            b"SPY: {options: true, chain_cadence: 1m, bars: [1m], enabled: false}\n"
            b"QQQ: {options: false, bars: [1d], enabled: false}\n",
            id="all-disabled",
        ),
    ],
)
def test_apply_refuses_a_roster_with_no_enabled_ticker(tmp_path, config_path, payload):
    target = tmp_path / "out" / "tickers.yaml"
    with pytest.raises(TickersError, match="no enabled ticker"):
        apply(payload, config_path=config_path, tickers_path=target)
    assert not target.parent.exists()


def test_apply_refuses_without_config_yaml(tmp_path):
    target = tmp_path / "out" / "tickers.yaml"
    with pytest.raises(ConfigError, match="config file not found"):
        apply(HAND_FORMATTED, config_path=tmp_path / "nope.yaml", tickers_path=target)
    assert not target.parent.exists()


def test_apply_refuses_to_run_as_root(tmp_path, config_path):
    target = tmp_path / "out" / "tickers.yaml"
    with pytest.raises(RosterError, match="root"):
        apply(HAND_FORMATTED, config_path=config_path, tickers_path=target, geteuid=lambda: 0)
    assert not target.parent.exists()


def test_apply_refuses_root_before_the_payload_and_the_config(tmp_path):
    # The payload is not a roster and no config.yaml exists, so either later step would
    # refuse too. Naming root says the root check ran first.
    target = tmp_path / "out" / "tickers.yaml"
    with pytest.raises(RosterError, match="root"):
        apply(
            b"- SPY\n", config_path=tmp_path / "nope.yaml", tickers_path=target, geteuid=lambda: 0
        )
    assert not target.parent.exists()


def test_apply_refuses_a_missing_config_before_an_empty_roster(tmp_path):
    # The issue's order is validate, then config.yaml, then the enabled count. An empty
    # document passes validation and fails the count, so naming the config says the
    # config was loaded before the count ran.
    target = tmp_path / "out" / "tickers.yaml"
    with pytest.raises(ConfigError, match="config file not found"):
        apply(b"", config_path=tmp_path / "nope.yaml", tickers_path=target)
    assert not target.parent.exists()


def test_apply_refused_by_the_check_creates_no_directory(tmp_path, config_path):
    target = tmp_path / "out" / "tickers.yaml"

    def refuse(roster, config) -> None:
        raise _Refused("the lake owes a ticker this roster drops")

    with pytest.raises(_Refused):
        apply(HAND_FORMATTED, check=refuse, config_path=config_path, tickers_path=target)
    assert not target.parent.exists()


def test_apply_hands_the_check_the_roster_and_the_host_config(tmp_path, config_path):
    target = tmp_path / "out" / "tickers.yaml"
    seen = []
    outcome = apply(
        HAND_FORMATTED,
        check=lambda roster, config: seen.append((roster.symbols, config.lake_root)),
        config_path=config_path,
        tickers_path=target,
    )
    assert outcome == REPLACED
    assert seen == [(("SPY", "QQQ"), tmp_path / "lake")]
    assert target.read_bytes() == HAND_FORMATTED


def _stdin_holding(payload: bytes) -> io.TextIOWrapper:
    # A text stream over the bytes, the shape ``sys.stdin`` has. The encoding is named so
    # a read through the text layer decodes as a UTF-8 host's would, whatever the locale.
    return io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8")


def _run_main(monkeypatch, payload: bytes) -> None:
    monkeypatch.setattr(sys, "stdin", _stdin_holding(payload))
    roster_cli.main(["apply"])


def test_main_prints_replaced_then_unchanged(tmp_path, config_path, monkeypatch, capsys):
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    _run_main(monkeypatch, HAND_FORMATTED)
    _run_main(monkeypatch, HAND_FORMATTED)
    out = capsys.readouterr().out
    skipped = _skipped(tmp_path / "lake")
    assert out == (f"{skipped}roster: {REPLACED} {target}\n{skipped}roster: {UNCHANGED} {target}\n")
    assert target.read_bytes() == HAND_FORMATTED


def test_main_names_a_bad_payload_before_a_missing_config(tmp_path, monkeypatch, capsys):
    # No config.yaml exists anywhere this run can resolve, so a refusal naming the
    # payload says the payload was validated first, which is the order the issue sets.
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(tmp_path / "nope.yaml"))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(tmp_path / "tickers.yaml"))
    with pytest.raises(SystemExit) as excinfo:
        _run_main(monkeypatch, b"- SPY\n")
    assert excinfo.value.code == 2
    assert capsys.readouterr().err == "roster: tickers file is not a mapping: <stdin>\n"
    assert _listing(tmp_path) == []


def test_main_refuses_root_in_one_line(tmp_path, config_path, monkeypatch, capsys):
    # Nothing is injected here. The command line reaches the process's own effective
    # uid, so this fakes that rather than ``apply``'s parameter.
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(SystemExit) as excinfo:
        _run_main(monkeypatch, HAND_FORMATTED)
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert len(err.splitlines()) == 1
    assert err.startswith("roster: refusing to run as root")
    assert not target.parent.exists()


def test_main_refuses_a_payload_that_is_not_utf8_in_one_line(
    tmp_path, config_path, monkeypatch, capsys
):
    # The bytes reach the validation undecoded. A read through the text layer would
    # raise a decode error before the refusal handling ever saw it.
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    with pytest.raises(SystemExit) as excinfo:
        _run_main(monkeypatch, b"SPY: {options: false, bars: [1d]}  # caf\xe9\n")
    assert excinfo.value.code == 2
    assert capsys.readouterr().err == "roster: tickers file is not UTF-8 text: <stdin>\n"
    assert not target.parent.exists()


def test_main_writes_crlf_line_endings_byte_for_byte(tmp_path, config_path, monkeypatch):
    # A read through the text layer turns each CRLF into LF, so the host's copy would
    # differ from the reviewed file.
    payload = (
        b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\r\n"
        b"QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\r\n"
    )
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    _run_main(monkeypatch, payload)
    assert target.read_bytes() == payload


def test_main_requires_the_subcommand(tmp_path, config_path, monkeypatch, capsys):
    # Everything else an apply needs is in place, so only the missing subcommand can
    # stop it.
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    monkeypatch.setattr(sys, "stdin", _stdin_holding(HAND_FORMATTED))
    with pytest.raises(SystemExit) as excinfo:
        roster_cli.main([])
    assert excinfo.value.code == 2
    assert "required" in capsys.readouterr().err
    assert not target.parent.exists()


class _UnreadableBuffer:
    def read(self) -> bytes:
        raise OSError(5, "Input/output error")


class _UnreadableStdin:
    buffer = _UnreadableBuffer()


@pytest.mark.parametrize(
    ("stdin", "named"),
    [
        pytest.param(None, "standard input is closed", id="closed"),
        pytest.param(_UnreadableStdin(), "cannot read the roster", id="read-fails"),
    ],
)
def test_main_refuses_a_stdin_it_cannot_read_in_one_line(
    tmp_path, config_path, monkeypatch, capsys, stdin, named
):
    # ``<&-`` starts the process with ``sys.stdin`` set to None. Before this was a
    # refusal, the read sat outside the refusal handling and escaped as a traceback.
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    monkeypatch.setattr(sys, "stdin", stdin)
    with pytest.raises(SystemExit) as excinfo:
        roster_cli.main(["apply"])
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert len(err.splitlines()) == 1
    assert err.startswith(f"roster: {named}")
    assert not target.exists()


def test_a_fresh_process_writes_the_path_the_loader_reads(tmp_path):
    # Two children, given only a PATH and a HOME holding a config.yaml. The first applies
    # the roster with every path at its default. The second asks the loader, also at its
    # default, where the roster is and what it holds. Neither inherits the suite's
    # redirect, so both resolve the directory from HOME the way a host's processes do.
    home = tmp_path / "home"
    config_dir = home / ".config" / "marketlake"
    write_config(config_dir, tmp_path / "lake", role="shadow")
    env = {"PATH": "/usr/bin:/bin", "HOME": str(home)}

    applied = subprocess.run(
        [sys.executable, "-m", "lake.roster", "apply"],
        input=HAND_FORMATTED,
        capture_output=True,
        env=env,
        check=False,
    )
    assert applied.returncode == 0, applied.stderr
    expected = config_dir / "tickers.yaml"
    assert (
        applied.stdout.decode() == f"{_skipped(tmp_path / 'lake')}roster: {REPLACED} {expected}\n"
    )

    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "from lake.tickers import load_tickers, tickers_file_path\n"
            "print(tickers_file_path())\n"
            "print(','.join(load_tickers().symbols))\n",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout == f"{expected}\nSPY,QQQ\n"
    assert expected.read_bytes() == HAND_FORMATTED


# The child for the late-redirect test. It imports ``lake.roster`` while ``HOME`` still
# names the first directory, and only then points ``HOME`` and the config-directory
# override at the second, the order a probe that redirects too late runs in. The config
# path is left to its default, which resolves at call time like the roster's.
_LATE_REDIRECT = """\
import os
import sys
from pathlib import Path

import lake.roster

late_home = Path(sys.argv[1])
os.environ["HOME"] = str(late_home)
os.environ["MARKETLAKE_CONFIG_DIR"] = str(late_home / ".config" / "marketlake")
print(lake.roster.apply(sys.stdin.buffer.read()))
"""


def test_a_redirect_made_after_import_moves_the_roster(tmp_path):
    # Both homes come from ``tmp_path``, so the real one is never involved. The child's
    # environment is built here rather than inherited, so the suite's own redirect does
    # not reach it, and ``MARKETLAKE_CONFIG_DIR`` is unset when it starts.
    early_home = tmp_path / "early"
    late_home = tmp_path / "late"
    early_home.mkdir()
    late_dir = late_home / ".config" / "marketlake"
    write_config(late_dir, tmp_path / "lake", role="shadow")
    applied = subprocess.run(
        [sys.executable, "-c", _LATE_REDIRECT, str(late_home)],
        input=HAND_FORMATTED,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(early_home)},
        check=False,
    )
    assert applied.returncode == 0, applied.stderr
    assert applied.stdout.decode() == f"{_skipped(tmp_path / 'lake')}{REPLACED}\n"
    assert (late_dir / "tickers.yaml").read_bytes() == HAND_FORMATTED
    assert list(early_home.rglob("*")) == []


# The child for the one-variable redirects. It imports ``lake.roster`` first, then sets
# only the variables named after the config path, each as ``NAME=value``, and applies the
# roster on stdin. The config path is passed explicitly, so each test turns on the
# roster's default alone. ``tests/component/test_late_config_dir_redirect.py`` covers the
# config's default.
_LATE_SETS = """\
import os
import sys

import lake.roster

for pair in sys.argv[2:]:
    name, value = pair.split("=", 1)
    os.environ[name] = value
print(lake.roster.apply(sys.stdin.buffer.read(), config_path=sys.argv[1]))
"""


def _apply_in_child(env: dict[str, str], config: Path, *sets: str):
    # The caller builds every path from ``tmp_path``. The environment is built here,
    # never inherited, so neither the real HOME nor the suite's redirect reaches it.
    return subprocess.run(
        [sys.executable, "-c", _LATE_SETS, str(config), *sets],
        input=HAND_FORMATTED,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin", **env},
        check=False,
    )


def test_a_late_redirect_of_home_alone_moves_the_roster(tmp_path):
    # The 2026-10-06 incident redirected HOME and nothing else. The child starts with no
    # config-directory override, so the default comes from HOME, and HOME changes after
    # the import.
    early_home = tmp_path / "early"
    late_home = tmp_path / "late"
    early_home.mkdir()
    late_dir = late_home / ".config" / "marketlake"
    config = write_config(late_dir, tmp_path / "lake", role="shadow")
    applied = _apply_in_child({"HOME": str(early_home)}, config, f"HOME={late_home}")
    assert applied.returncode == 0, applied.stderr
    assert applied.stdout.decode() == f"{_skipped(tmp_path / 'lake')}{REPLACED}\n"
    assert (late_dir / "tickers.yaml").read_bytes() == HAND_FORMATTED
    assert list(early_home.rglob("*")) == []


def test_a_late_config_directory_override_alone_moves_the_roster(tmp_path):
    # The mirror of the HOME case. HOME stays put and only the override is set after the
    # import, so a default that read the override once, at import, would land under HOME.
    home = tmp_path / "home"
    home.mkdir()
    late_dir = tmp_path / "late"
    config = write_config(tmp_path / "cfg", tmp_path / "lake", role="shadow")
    applied = _apply_in_child({"HOME": str(home)}, config, f"{CONFIG_DIR_ENV}={late_dir}")
    assert applied.returncode == 0, applied.stderr
    assert applied.stdout.decode() == f"{_skipped(tmp_path / 'lake')}{REPLACED}\n"
    assert (late_dir / "tickers.yaml").read_bytes() == HAND_FORMATTED
    assert list(home.rglob("*")) == []
