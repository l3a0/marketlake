"""The config-directory guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a test that writes the machine's real
``~/.config/marketlake/`` fails instead of destroying what is there. It is autouse, so
every test depends on it and no test asserts it. These do.

On 2026-09-13 a stub landed at that directory's ``token.json`` and the working Schwab
token it replaced was gone. The daemon read the stub for half an hour and the
dashboard's token age went with it. That is the failure these cover.

**Every probe path here is a name that does not exist in the real directory.** The
guard is supposed to raise before the call reaches the filesystem, but a test of a
guard has to assume the guard is broken, and a broken guard must not be able to destroy
the owner's token or roster. So no probe names a real file. With the guard working the
call never happens; with the guard broken the call hits a name nothing uses, the test
fails on the wrong exception type, and the failure says so.

Six properties carry the guard, and each is covered below.

1. Every kind of write is refused, and the refusal names the path.
2. A read is left alone, which is what keeps a test free to load a file an operator put
   there.
3. The failure is not an ``Exception``. ``reauth.write_token`` wraps its write in
   ``except BaseException`` and the Sunday self-check catches bare ``Exception``, so a
   guard derived from ``Exception`` would be swallowed and the test would pass.
4. Every other path still writes for real, which is what the whole suite depends on.
5. Neither ``MARKETLAKE_CONFIG_DIR`` nor a monkeypatched ``$HOME`` moves the protected
   directory. Both are things a test or a developer can change, and the guard has to
   protect the real path regardless.
6. The production writers that default into that directory are themselves caught, not
   just a synthetic call naming the path directly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from lake.paths import CONFIG_DIR_ENV, CONFIG_DIR_PARTS, TOKEN_FILE
from lake.reauth import token_writer, write_token
from lake.tickers import upsert_ticker
from tests.conftest import ConfigWriteInTest

# The real directory, spelled here from the process's own home rather than through
# ``paths.config_dir``, so an exported ``MARKETLAKE_CONFIG_DIR`` cannot move what these
# tests aim at. That is the same reason the guard spells it this way.
REAL_CONFIG_DIR = Path.home().joinpath(*CONFIG_DIR_PARTS)

# A name that is not one of the four files the directory holds and is not there. See the
# module docstring: a broken guard must land on nothing.
PROBE = REAL_CONFIG_DIR / "guard-probe-not-a-real-file.json"


def test_the_probe_paths_are_not_real_files():
    """The safety this whole module rests on, asserted rather than assumed."""
    assert not PROBE.exists()
    assert PROBE.name != TOKEN_FILE


# -- every kind of write ---------------------------------------------------------------


def test_a_bare_open_for_writing_is_refused():
    with pytest.raises(ConfigWriteInTest):
        open(PROBE, "w")


def test_path_write_text_is_refused():
    # pathlib calls io.open by name, which patching builtins.open does not reach.
    with pytest.raises(ConfigWriteInTest):
        PROBE.write_text("{}")


def test_path_write_bytes_is_refused():
    with pytest.raises(ConfigWriteInTest):
        PROBE.write_bytes(b"{}")


def test_path_open_for_writing_is_refused():
    with pytest.raises(ConfigWriteInTest):
        PROBE.open("w")


@pytest.mark.parametrize("mode", ["w", "a", "x", "wb", "r+", "w+b"])
def test_every_writing_mode_is_refused(mode):
    with pytest.raises(ConfigWriteInTest):
        open(PROBE, mode)


def test_path_touch_is_refused():
    # touch on a missing file goes through os.open, which builtins.open does not.
    with pytest.raises(ConfigWriteInTest):
        PROBE.touch()


def test_os_open_for_writing_is_refused():
    with pytest.raises(ConfigWriteInTest):
        os.open(PROBE, os.O_WRONLY | os.O_CREAT)


def test_path_mkdir_is_refused():
    with pytest.raises(ConfigWriteInTest):
        (REAL_CONFIG_DIR / "guard-probe-dir").mkdir(parents=True, exist_ok=True)


def test_making_the_directory_itself_is_refused():
    # write_token calls parent.mkdir(exist_ok=True) before it opens anything, and
    # pathlib asks os.mkdir first even when the directory is already there.
    with pytest.raises(ConfigWriteInTest):
        REAL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def test_path_rmdir_is_refused():
    with pytest.raises(ConfigWriteInTest):
        (REAL_CONFIG_DIR / "guard-probe-dir").rmdir()


def test_path_unlink_is_refused():
    with pytest.raises(ConfigWriteInTest):
        PROBE.unlink()


def test_os_remove_is_refused():
    # os.remove and os.unlink are separate function objects, so both are patched.
    with pytest.raises(ConfigWriteInTest):
        os.remove(PROBE)


def test_os_truncate_is_refused():
    with pytest.raises(ConfigWriteInTest):
        os.truncate(PROBE, 0)


def test_a_rename_into_the_directory_is_refused(tmp_path):
    source = tmp_path / "stub.json"
    source.write_text("{}")
    with pytest.raises(ConfigWriteInTest):
        os.rename(source, PROBE)


def test_a_rename_out_of_the_directory_is_refused(tmp_path):
    # A move out destroys what was there as surely as a move in overwrites it, so both
    # ends are checked.
    with pytest.raises(ConfigWriteInTest):
        os.rename(PROBE, tmp_path / "stolen.json")


def test_a_replace_into_the_directory_is_refused(tmp_path):
    # os.replace is the step that ends every atomic write in this package, the token's
    # included. It is the call that actually destroyed the token.
    source = tmp_path / "stub.json"
    source.write_text("{}")
    with pytest.raises(ConfigWriteInTest):
        os.replace(source, PROBE)


def test_a_replace_out_of_the_directory_is_refused(tmp_path):
    with pytest.raises(ConfigWriteInTest):
        os.replace(PROBE, tmp_path / "stolen.json")


def test_a_symlink_into_the_directory_is_refused(tmp_path):
    with pytest.raises(ConfigWriteInTest):
        os.symlink(tmp_path / "stub.json", PROBE)


def test_a_hard_link_into_the_directory_is_refused(tmp_path):
    source = tmp_path / "stub.json"
    source.write_text("{}")
    with pytest.raises(ConfigWriteInTest):
        os.link(source, PROBE)


def test_a_bytes_path_is_still_refused():
    # open and the os calls all accept bytes, and str() of a bytes path renders
    # "b'/Users/...'", which no prefix check would match. The guard decodes first.
    with pytest.raises(ConfigWriteInTest):
        open(os.fsencode(PROBE), "w")


def test_a_relative_path_into_the_directory_is_refused(monkeypatch):
    # A relative path is resolved against the working directory before it is compared,
    # so chdir-ing into the real directory does not slip a write past the guard.
    if not REAL_CONFIG_DIR.exists():
        pytest.skip("the real config directory is not on this machine")
    monkeypatch.chdir(REAL_CONFIG_DIR)
    with pytest.raises(ConfigWriteInTest):
        open(PROBE.name, "w")


def test_an_unexpanded_home_path_is_refused():
    # open("~/x") creates a literal "~" directory rather than failing, so a tilde path
    # never reaches the real file. It is still someone aiming at it, and the guard
    # expands before comparing so the refusal names the directory rather than letting
    # the typo through as a pass.
    with pytest.raises(ConfigWriteInTest):
        open(f"~/{'/'.join(CONFIG_DIR_PARTS)}/{PROBE.name}", "w")


def test_the_refusal_names_the_path():
    with pytest.raises(ConfigWriteInTest) as caught:
        PROBE.write_text("{}")
    assert str(PROBE) in str(caught.value)


# -- what the guard leaves alone --------------------------------------------------------


def test_a_read_of_the_directory_is_left_alone():
    # The guard refuses writes, not reads. A missing file raising FileNotFoundError is
    # the proof the call reached the real open rather than the refusal, and the probe
    # name means nothing real is read to prove it.
    with pytest.raises(FileNotFoundError):
        open(PROBE)


def test_writing_anywhere_else_still_works(tmp_path):
    target = tmp_path / "elsewhere.json"
    target.write_text("{}")
    assert target.read_text() == "{}"
    os.replace(target, tmp_path / "moved.json")
    assert (tmp_path / "moved.json").exists()


def test_a_path_that_only_looks_like_the_directory_is_left_alone(tmp_path):
    # The check is a path-component prefix, not a string prefix, so a sibling whose
    # name starts with the directory's name is not swept up.
    sibling = tmp_path / (REAL_CONFIG_DIR.name + "-scratch")
    sibling.mkdir()
    (sibling / "x.json").write_text("{}")
    assert (sibling / "x.json").exists()


# -- the guard cannot be moved or swallowed ---------------------------------------------


def test_the_refusal_is_not_caught_by_a_bare_except_exception():
    # reauth.write_token wraps its whole write in `except BaseException` to unlink the
    # temp file, and the Sunday self-check wraps its read-backs in a bare
    # `except Exception`. A guard deriving from Exception would be swallowed on that
    # path and the destroyed token would ship green.
    assert not issubclass(ConfigWriteInTest, Exception)

    caught = False
    try:
        try:
            PROBE.write_text("{}")
        except Exception:  # noqa: BLE001 - reproducing production's swallow on purpose
            caught = True
    except ConfigWriteInTest:
        pass
    assert not caught, "the guard was swallowed by a bare except Exception"


def test_the_config_directory_override_does_not_disarm_the_guard(monkeypatch):
    # MARKETLAKE_CONFIG_DIR points a process away from the real directory. Reading it to
    # decide what to protect would let the one thing that exists to keep a process off
    # this path be the thing that opens it.
    monkeypatch.setenv(CONFIG_DIR_ENV, "/tmp/somewhere-else")
    with pytest.raises(ConfigWriteInTest):
        PROBE.write_text("{}")


def test_a_monkeypatched_home_does_not_move_the_guard(monkeypatch, tmp_path):
    # Path.home() reads $HOME, which a test is free to set. The guard settles the
    # directory at import, before any test can move it.
    monkeypatch.setenv("HOME", str(tmp_path))
    assert Path.home() == tmp_path
    with pytest.raises(ConfigWriteInTest):
        PROBE.write_text("{}")


def test_a_test_calling_monkeypatch_undo_does_not_disarm_the_guard(monkeypatch):
    # The fixture holds its own MonkeyPatch rather than the shared one. Sharing it would
    # put the guard's patches on the test's undo stack, the regression both other guards
    # carry a test for.
    monkeypatch.setattr(os, "curdir", "not-a-dir")
    monkeypatch.undo()
    assert os.curdir == "."
    with pytest.raises(ConfigWriteInTest):
        PROBE.write_text("{}")


# -- the production writers, unfaked ----------------------------------------------------


def test_a_forgotten_token_path_is_caught():
    """The incident itself. ``write_token`` aimed at the real directory is refused.

    It is aimed at the probe rather than at ``token.json``, per the module docstring.
    The guard fires on the ``parent.mkdir`` this takes before it opens anything, so the
    path it names is the directory.
    """
    with pytest.raises(ConfigWriteInTest):
        write_token(PROBE, {"creation_timestamp": 0, "token": {"refresh_token": "x"}})


def test_a_forgotten_token_writer_is_caught():
    # The object schwab-py calls back into. It is what a by-hand login hands the token.
    with pytest.raises(ConfigWriteInTest):
        token_writer(PROBE)({"creation_timestamp": 0})


def test_a_forgotten_roster_upsert_is_caught():
    """The token is not the only file in there that can be lost.

    The roster is hand-maintained and has three writers that resolve into the same
    directory by default. Only ``upsert_ticker`` is driven here. ``set_enabled`` and
    ``remove_ticker`` both raise ``TickersError`` when the file is absent, before they
    reach a write, so neither can be aimed at a name that does not exist and the
    docstring's safety rule forbids aiming them at the real roster. All three write
    through ``tickers._write_atomically``, which this drives.
    """
    with pytest.raises(ConfigWriteInTest):
        upsert_ticker("SPY", options=True, chain_cadence="1m", path=PROBE)
