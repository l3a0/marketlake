"""The check that the machine's real config directory survived the run.

The guard refuses a write, the redirect moves what a default resolves to, and two child
tests refuse to write until they have confirmed their own redirect took. Every one of
those is a check on the attempt, and each names limits it cannot cover. The check in
``tests/conftest.py`` is the only one that looks at the outcome: it lists the real
directory when conftest is imported and again when the session ends, and fails the run
when the two disagree.

It is autouse in the sense that matters, since the session hook runs whether or not any
test asks for it, so nothing else asserts it. These do.

**Nothing here touches the real directory.** The two functions take their listing as an
argument, so a stand-in under ``tmp_path`` drives the same code that decides the real
question, and the hook is driven against a doctored snapshot rather than by damaging
anything. That is the same rule ``tests/unit/test_config_dir_guard.py`` states about its
probes, arrived at from the other side: this module has no probe at all.

Five properties carry the check, and each is covered below.

1. An unchanged directory reports nothing, which is what keeps a green run green.
2. A name that appeared, went, or was rewritten is reported, and named.
3. A missing directory lists as empty, which is CI and a fresh machine.
4. The listing opens no file, so a live credential is never read to check on it.
5. The session hook fails the run and says what changed.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from tests.conftest import (
    _config_dir_changes,
    _config_dir_listing,
    pytest_sessionfinish,
)


def _populate(directory: Path) -> Path:
    """A stand-in config directory holding the four names the real one holds."""
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in (
        ("config.yaml", "lake_root: /tmp/lake\n"),
        ("token.json", '{"creation_timestamp": 1}'),
        ("tickers.yaml", "tickers: []\n"),
        ("chain_plan.json", "{}"),
    ):
        (directory / name).write_text(text)
    return directory


# -- nothing moved ---------------------------------------------------------------------


def test_an_unchanged_directory_reports_nothing(tmp_path):
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    assert set(before) == {"config.yaml", "token.json", "tickers.yaml", "chain_plan.json"}
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == ()


def test_reading_a_file_is_not_a_change(tmp_path):
    """The guard allows reads on purpose, so the outcome check has to allow them too.

    A test that loads a config an operator put there is entitled to, and atime is
    deliberately not one of the three fields compared.
    """
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    assert (stand_in / "token.json").read_text()
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == ()


# -- something moved -------------------------------------------------------------------


def test_a_new_name_is_reported(tmp_path):
    # The shape a broken guard leaves behind: a probe file landing beside the token.
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    (stand_in / "guard-probe-not-a-real-file.json").write_text("{}")
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == (
        "guard-probe-not-a-real-file.json appeared",
    )


def test_a_removed_name_is_reported(tmp_path):
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    (stand_in / "token.json").unlink()
    after = _config_dir_listing(str(stand_in))
    assert _config_dir_changes(before, after) == ("token.json is gone",)


def test_a_rewritten_file_is_reported(tmp_path):
    """The incident itself. A stub lands on the token and the name is unchanged.

    This is the case a listing of names alone would miss, and it is the only one that
    matters most: what was lost on 2026-09-13 was the contents of a file whose name
    never went anywhere.
    """
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    (stand_in / "token.json").write_text('{"creation_timestamp": 0, "token": {}}')
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == (
        "token.json was rewritten",
    )


def test_a_replace_that_keeps_the_size_is_still_reported(tmp_path):
    """``os.replace`` is the step that ended the real write, and it can keep the size.

    A stub the same length as the file it lands on, renamed into place inside one mtime
    tick, moves neither of those two fields. The inode does move, because the rename
    puts a different file at the name. Comparing the mtime and the size alone would let
    this through.
    """
    stand_in = _populate(tmp_path / "marketlake")
    target = stand_in / "token.json"
    original = target.stat()
    before = _config_dir_listing(str(stand_in))

    stub = stand_in.parent / "stub.json"
    stub.write_text("X" * original.st_size)
    os.replace(stub, target)
    # Put the clock back exactly, so neither the mtime nor the size can be what fires.
    os.utime(target, ns=(original.st_atime_ns, original.st_mtime_ns))
    after = _config_dir_listing(str(stand_in))
    assert after["token.json"][:2] == before["token.json"][:2]
    assert after["token.json"][2] != before["token.json"][2]

    assert _config_dir_changes(before, after) == ("token.json was rewritten",)


def test_every_change_is_reported_in_one_pass(tmp_path):
    # Sorted by name, so a run that damaged several things reads as a list rather than
    # as whichever one happened to be noticed first.
    stand_in = _populate(tmp_path / "marketlake")
    before = _config_dir_listing(str(stand_in))
    (stand_in / "token.json").unlink()
    (stand_in / "config.yaml").write_text("lake_root: /tmp/elsewhere\n")
    (stand_in / "probe.json").write_text("{}")
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == (
        "config.yaml was rewritten",
        "probe.json appeared",
        "token.json is gone",
    )


# -- the directory that is not there ----------------------------------------------------


def test_a_missing_directory_lists_as_empty(tmp_path):
    """CI and a fresh machine. Neither has a config directory, and neither should fail."""
    missing = tmp_path / "not-there"
    assert _config_dir_listing(str(missing)) == {}
    assert _config_dir_changes({}, {}) == ()


def test_a_path_that_is_a_file_lists_as_empty(tmp_path):
    # Not a shape anything builds, but scandir raises NotADirectoryError on it and a
    # check that exploded here would fail runs for a reason unrelated to the token.
    a_file = tmp_path / "file"
    a_file.write_text("x")
    assert _config_dir_listing(str(a_file)) == {}


def test_a_directory_that_appears_during_the_run_is_reported(tmp_path):
    # The other direction of the CI case. Starting with nothing does not mean anything
    # that turns up later is ignored.
    stand_in = tmp_path / "marketlake"
    before = _config_dir_listing(str(stand_in))
    assert before == {}
    _populate(stand_in)
    assert _config_dir_changes(before, _config_dir_listing(str(stand_in))) == (
        "chain_plan.json appeared",
        "config.yaml appeared",
        "tickers.yaml appeared",
        "token.json appeared",
    )


# -- it reads no secret ------------------------------------------------------------------


def test_the_listing_opens_no_file(tmp_path):
    """The check watches a live brokerage credential, so it must never read one.

    Driven with an audit hook rather than asserted in a comment, because "it only stats"
    is exactly the kind of claim that stays true until someone adds a content hash to
    make the comparison stricter.

    An audit hook cannot be removed once installed, so this one is installed for the life
    of the process and filters on a path under this test's own directory. It sees every
    open in the process after that, and only the ones inside the stand-in are recorded.
    """
    stand_in = _populate(tmp_path / "marketlake")
    opened: list[str] = []
    root = str(stand_in)

    def hook(event: str, args: tuple) -> None:
        if event != "open":
            return
        try:
            path = os.fsdecode(args[0])
        except (TypeError, ValueError):
            return
        if os.path.abspath(path).startswith(root + os.sep):
            opened.append(path)

    sys.addaudithook(hook)

    _config_dir_listing(root)
    assert opened == []

    # The other direction, so this cannot pass because the hook never fires at all.
    (stand_in / "token.json").read_text()
    assert [Path(p).name for p in opened] == ["token.json"]


# -- the session hook --------------------------------------------------------------------


class _Reporterless:
    """The plugin manager of a session with no terminal reporter, which prints instead."""

    @staticmethod
    def get_plugin(name: str) -> None:
        return None


class _FakeSession:
    """Enough of a session for the hook: an exit status and a way to find the reporter."""

    def __init__(self) -> None:
        self.exitstatus = 0
        self.config = type("_Config", (), {"pluginmanager": _Reporterless()})()


def test_the_hook_leaves_a_clean_run_alone(monkeypatch, tmp_path, capsys):
    stand_in = _populate(tmp_path / "marketlake")
    monkeypatch.setattr("tests.conftest.REAL_CONFIG_DIR", str(stand_in))
    monkeypatch.setattr("tests.conftest._CONFIG_DIR_AT_START", _config_dir_listing(str(stand_in)))

    session = _FakeSession()
    pytest_sessionfinish(session, 0)
    assert session.exitstatus == 0
    assert capsys.readouterr().out == ""


def test_the_hook_fails_the_run_and_names_what_changed(monkeypatch, tmp_path, capsys):
    """A passing suite that lost the token must not exit zero.

    There is no test left to fail by the time this runs, so the exit status is what
    carries it, and the report is what stops a non-zero exit under a green summary from
    reading as a broken harness.
    """
    stand_in = _populate(tmp_path / "marketlake")
    monkeypatch.setattr("tests.conftest.REAL_CONFIG_DIR", str(stand_in))
    monkeypatch.setattr("tests.conftest._CONFIG_DIR_AT_START", _config_dir_listing(str(stand_in)))
    (stand_in / "token.json").unlink()

    session = _FakeSession()
    pytest_sessionfinish(session, 0)

    assert session.exitstatus == 1
    printed = capsys.readouterr().out
    assert "token.json is gone" in printed
    assert str(stand_in) in printed
    # Both explanations, because no stat field tells a daemon's own refresh apart from a
    # stub, and a report naming only one of them sends the reader the wrong way.
    assert "running daemon" in printed
    assert "past the guard and past the redirect" in printed


def test_the_hook_reaches_a_terminal_reporter_when_there_is_one(monkeypatch, tmp_path):
    """The real run has one, so the printing branch is not the one that normally fires."""
    stand_in = _populate(tmp_path / "marketlake")
    monkeypatch.setattr("tests.conftest.REAL_CONFIG_DIR", str(stand_in))
    monkeypatch.setattr("tests.conftest._CONFIG_DIR_AT_START", _config_dir_listing(str(stand_in)))
    (stand_in / "probe.json").write_text("{}")

    written: list[str] = []

    class _Reporter:
        @staticmethod
        def write_sep(sep: str, title: str, **kwargs: object) -> None:
            written.append(title)

        @staticmethod
        def write_line(line: str) -> None:
            written.append(line)

    session = _FakeSession()
    session.config.pluginmanager = type(  # type: ignore[assignment]
        "_PM", (), {"get_plugin": staticmethod(lambda name: _Reporter())}
    )()

    pytest_sessionfinish(session, 0)
    assert session.exitstatus == 1
    assert any("probe.json appeared" in line for line in written)


def test_the_check_is_watching_the_real_directory():
    """The stand-ins above would all pass with the check pointed at nothing.

    This is the one assertion that says what it is aimed at. It reads the module globals
    rather than the directory, so it touches no file.
    """
    from tests import conftest

    assert conftest.REAL_CONFIG_DIR == str(Path.home() / ".config" / "marketlake")
    # The snapshot is taken at import, which is before any test can move what it watches.
    assert isinstance(conftest._CONFIG_DIR_AT_START, dict)
    for name, fields in conftest._CONFIG_DIR_AT_START.items():
        assert isinstance(name, str)
        assert len(fields) == 3


def test_the_hook_is_named_so_pytest_calls_it():
    # The whole mechanism rests on pytest finding this by name in a conftest. A rename
    # would leave every test above green while the check never ran.
    from tests import conftest

    assert callable(conftest.pytest_sessionfinish)
    assert conftest.pytest_sessionfinish.__name__ == "pytest_sessionfinish"


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        ({}, {}, ()),
        ({"a": (1, 2, 3)}, {"a": (1, 2, 3)}, ()),
        ({"a": (1, 2, 3)}, {"a": (9, 2, 3)}, ("a was rewritten",)),
        ({"a": (1, 2, 3)}, {"a": (1, 9, 3)}, ("a was rewritten",)),
        ({"a": (1, 2, 3)}, {"a": (1, 2, 9)}, ("a was rewritten",)),
        ({"a": (1, 2, 3)}, {}, ("a is gone",)),
        ({}, {"a": (1, 2, 3)}, ("a appeared",)),
    ],
)
def test_each_field_on_its_own_decides(before, after, expected):
    # One case per field, so none of the three can be dropped from the tuple with
    # nothing failing. Dropping the inode is the one that would otherwise go unnoticed,
    # because a same-size rename inside one tick is the only thing that needs it.
    assert _config_dir_changes(before, after) == expected
