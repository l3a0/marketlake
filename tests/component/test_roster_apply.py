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

from lake import roster as roster_cli
from lake.config import ConfigError
from lake.roster import REPLACED, UNCHANGED, RosterError, apply
from lake.tickers import TickersError, apply_roster
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
    "payload",
    [
        pytest.param(b"SPY: {options: true, bars: [1m\n", id="bad-yaml"),
        pytest.param(b"- SPY\n- QQQ\n", id="not-a-mapping"),
        pytest.param(b"SPY: {options: true, bars: [1m, 1d]}  # caf\xe9\n", id="not-utf8"),
    ],
)
def test_a_payload_that_is_not_a_roster_writes_nothing(tmp_path, payload):
    target = tmp_path / "tickers.yaml"
    with pytest.raises(TickersError):
        apply_roster(payload, check=_accept, path=target)
    assert _listing(tmp_path) == []


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


# -- the command: lake.roster.apply ---------------------------------------------------


@pytest.fixture
def config_path(tmp_path) -> Path:
    return write_config(tmp_path / "cfg", tmp_path / "lake")


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


def _run_main(monkeypatch, payload: bytes) -> None:
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(payload)))
    roster_cli.main(["apply"])


def test_main_prints_replaced_then_unchanged(tmp_path, config_path, monkeypatch, capsys):
    target = tmp_path / "out" / "tickers.yaml"
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(config_path))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))
    _run_main(monkeypatch, HAND_FORMATTED)
    _run_main(monkeypatch, HAND_FORMATTED)
    out = capsys.readouterr().out
    assert out == f"roster: {REPLACED} {target}\nroster: {UNCHANGED} {target}\n"
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
    write_config(config_dir, tmp_path / "lake")
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
    assert applied.stdout.decode() == f"roster: {REPLACED} {expected}\n"

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
