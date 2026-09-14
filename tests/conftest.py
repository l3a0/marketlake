"""Shared fixtures that expose the four seams and the fixture-lake builder.

It also carries three guards. The network guard fails any test that reaches another
machine from inside this process. The subprocess guard fails any test that shells out
to rsync, launchctl, pmset, or tmutil. The config-directory guard fails any test that
writes under the machine's real ``~/.config/marketlake/``.
"""

from __future__ import annotations

import builtins
import io
import os
import shutil
import socket
import subprocess
import urllib.request
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.cassette import load_cassette
from lake.paths import CONFIG_DIR_PARTS
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.vendor import CassetteVendor

# A fixed instant for the default manual clock: 2026-08-24 09:30 ET.
_DEFAULT_NOW = datetime(2026, 8, 24, 13, 30, tzinfo=UTC)

CASSETTES = Path(__file__).parent / "cassettes"


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    """A throwaway lake root under the test's temp directory."""
    root = tmp_path / "lake"
    root.mkdir()
    return root


@pytest.fixture
def fixture_lake(lake_root: Path) -> FixtureLake:
    """A fixture-lake builder rooted at a throwaway lake."""
    return FixtureLake(lake_root)


@pytest.fixture
def manual_clock() -> ManualClock:
    """A manual clock a test can advance by hand."""
    return ManualClock(start=_DEFAULT_NOW)


@pytest.fixture
def cassette_vendor() -> CassetteVendor:
    """A cassette-backed vendor over the checked-in minimal cassette."""
    return CassetteVendor(load_cassette(CASSETTES / "spy_minimal.json"))


# -- the network guard ---------------------------------------------------------------

# The suite must never reach the network. Two seams make real calls in production,
# ``UrllibPinger`` for healthchecks and the ntfy ``Transport``, and both default on. A test
# that forgets to inject one does not fail. It succeeds, quietly, having pinged whatever
# ``~/.config/marketlake/config.yaml`` names. On the owner's own machine that is the live
# `capture` check, so a passing test run feeds the dead-man that is supposed to notice a dead
# daemon. This fixture turns that silent success into a loud failure.


_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost", "0.0.0.0"})


class NetworkAccessInTest(BaseException):
    """Raised when a test reaches for the network. Inject the seam instead.

    It derives from ``BaseException`` rather than ``Exception`` on purpose. Production
    swallows a failed ping by design, because a missed ping is what the check exists to
    notice, so ``DeadMan._ping`` catches bare ``Exception``. A guard that inherits from
    ``Exception`` is caught there and the test passes anyway, which is the exact silence
    being fixed. This one passes through, the way ``KeyboardInterrupt`` does.
    """


def _host_of(address: object) -> str | None:
    """The host in a socket address, or ``None`` when there is not one.

    An ``AF_UNIX`` socket addresses a filesystem path and a ``socketpair`` addresses
    nothing, so neither carries a host. Those never leave the machine and are what
    ``multiprocessing`` uses, so they pass through untouched.
    """
    if isinstance(address, tuple) and address:
        return str(address[0])
    return None


@pytest.fixture(autouse=True)
def _no_network() -> Iterator[None]:
    """Fail any test that opens a socket or a URL to another machine.

    Autouse, because the failure this catches is a test forgetting to pass a seam, and a
    test that forgets one would equally forget to ask for the guard.

    The patch goes on ``socket.socket.connect``, which is the boundary every higher
    layer crosses: ``urllib``, ``http.client``, ``urllib3``, and anything written on raw
    sockets tomorrow. ``socket.create_connection`` is covered by patching it too, since
    it is the one ``urllib`` reaches for by name. ``urlopen`` is patched as well, so the
    refusal names the URL rather than an address, which is what a reader needs.

    Two things a monkeypatch cannot reach, and the guard does not claim: a child process,
    and anything run at import or collection time. A test that shells out to ``curl`` or
    ``rsync`` is outside this.

    The fixture holds its own ``MonkeyPatch`` rather than taking the shared one. Sharing
    it would put the guard's patches on the same undo stack as the test's, so any test
    calling ``monkeypatch.undo()`` would disarm the guard for the rest of its run.
    """

    def refuse_urlopen(request: object, *args: object, **kwargs: object) -> None:
        url = getattr(request, "full_url", None) or str(request)
        host = url.split("/")[2] if "//" in url else url
        if host.split(":")[0] in _LOOPBACK:
            return real_urlopen(request, *args, **kwargs)  # type: ignore[return-value]
        raise NetworkAccessInTest(
            f"a test tried to reach {host}. Pass a fake pinger or transport instead of "
            "letting the production default build a real one."
        )

    def refuse_connect(self: socket.socket, address: object, *args: object, **kwargs: object):
        # The dashboard's integration tests bind and call a loopback server on purpose,
        # so only a connection leaving this machine is a failure.
        host = _host_of(address)
        if host is None or host in _LOOPBACK:
            return real_connect(self, address, *args, **kwargs)  # type: ignore[arg-type]
        raise NetworkAccessInTest(
            f"a test tried to connect to {host}. Inject the seam rather than reaching out."
        )

    def refuse_create_connection(address: object, *args: object, **kwargs: object):
        host = _host_of(address)
        if host is None or host in _LOOPBACK:
            return real_create_connection(address, *args, **kwargs)  # type: ignore[arg-type]
        raise NetworkAccessInTest(
            f"a test tried to connect to {host}. Inject the seam rather than reaching out."
        )

    real_urlopen = urllib.request.urlopen
    real_connect = socket.socket.connect
    real_create_connection = socket.create_connection

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(urllib.request, "urlopen", refuse_urlopen)
        mp.setattr(socket.socket, "connect", refuse_connect)
        mp.setattr(socket, "create_connection", refuse_create_connection)
        yield


# -- the subprocess guard --------------------------------------------------------------

# Five production call sites shell out to a named external tool through
# ``subprocess.run``: ``RsyncBackup.sync`` runs ``rsync``, ``launchctl_probe`` runs
# ``launchctl``, ``read_pmset_schedule`` and ``pmset_assertions_probe`` run ``pmset``,
# and ``read_exclusions`` runs ``tmutil``. Each is a seam, so a test injects a fake in
# place of the function that calls it. A test that forgets runs the real tool instead,
# which the network guard above cannot catch: none of the five touch a socket in this
# process. This fixture closes that gap the same way, on those program names only.
#
# The refusal has to name the program rather than block every subprocess. Four tests in
# ``tests/component/test_control_plane_render.py`` run the rendered install, reinstall,
# restart, and uninstall scripts for real, each sandboxed by a fake ``PATH`` that points
# at stand-ins for the tools the script calls. Those calls name a script path or
# ``bash``, never one of the guarded names directly, so refusing only those names
# leaves them untouched.

_GUARDED_PROGRAMS = frozenset({"rsync", "launchctl", "pmset", "tmutil"})


class SubprocessAccessInTest(BaseException):
    """Raised when a test reaches ``rsync``, ``launchctl``, ``pmset``, or ``tmutil``.

    It derives from ``BaseException``, the same reason ``NetworkAccessInTest`` does.
    The Sunday self-check wraps both ``schedule_reader()`` and ``exclusion_reader()``
    in a broad ``except Exception``, on purpose, so a read-back it cannot parse becomes
    a report line instead of a page. A guard that inherited from ``Exception`` would
    land in that same catch, turn into a line reading "pmset read-back unreadable", and
    the forgotten seam would ship green.
    """


def _program_of(args: object) -> str | None:
    """The program a subprocess call names, or ``None`` when there is not one.

    ``subprocess.run`` and ``Popen`` both take the command as a sequence whose first
    element is the program, which is how all four guarded call sites and all four
    render tests call them. A ``bytes`` element is decoded first, since ``subprocess``
    accepts one and a raw ``str()`` of it would never match a guarded name. Two forms
    still are not handled: a single string with ``shell=True``, and a prefix wrapper
    (``env``, ``arch``, ``sudo``) naming the guarded program as a later element. No
    call site in this repo uses either form today.
    """
    if isinstance(args, (list, tuple)) and args:
        head = args[0]
        if isinstance(head, bytes):
            head = os.fsdecode(head)
        return Path(str(head)).name
    return None


@pytest.fixture(autouse=True)
def _no_subprocess() -> Iterator[None]:
    """Fail any test that would shell out to rsync, launchctl, pmset, or tmutil.

    Autouse, for the same reason ``_no_network`` is: the failure this catches is a test
    forgetting to inject one of the four seams, and a test that forgets one would
    equally forget to ask for the guard.

    Every other subprocess call passes through untouched, ``caffeinate`` included. Its
    seam (``AssertionHolder``'s ``runner``, threaded through ``run_loop_from_config``'s
    ``assertion_runner``) is out of scope per #80. That is narrower than safe: a bare
    ``AssertionHolder()``, or a call that omits ``assertion_runner``, still falls
    through to a real ``caffeinate`` spawn, the same forgotten-seam shape this fixture
    exists to catch elsewhere, just with a smaller blast radius and not covered here or
    by ``test_seam_defaults.py``'s ``REQUIRED`` table. No test omits it today.

    The fixture holds its own ``MonkeyPatch``, not the shared one, for the same reason
    ``_no_network`` does: a test calling ``monkeypatch.undo()`` must not disarm it.
    """

    def _refuser(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(args: object, *pos: object, **kwargs: object) -> object:
            program = _program_of(args)
            if program in _GUARDED_PROGRAMS:
                raise SubprocessAccessInTest(
                    f"a test tried to run {program}. Inject the seam instead of shelling "
                    "out for real."
                )
            return real(args, *pos, **kwargs)

        return refuse

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "run", _refuser(subprocess.run))
        mp.setattr(subprocess, "Popen", _refuser(subprocess.Popen))
        yield


# -- the config-directory guard ----------------------------------------------------------

# The machine's real config directory holds four files the suite has no business
# touching, and one of them is a live brokerage credential. On 2026-09-13 a stub landed
# at ``~/.config/marketlake/token.json`` and the working token it replaced was gone. The
# daemon read the stub for half an hour and the dashboard's token age went with it.
#
# Neither guard above catches that. No socket is opened and no named program is run. A
# write to the live path is an ordinary ``open`` in an ordinary test, so it succeeds,
# quietly, and the damage is a file the suite cannot put back. This fixture turns that
# silent success into a loud failure that names the path.
#
# What counts as the real directory is settled here, at import, and deliberately not
# through ``paths.config_dir``. Two reasons, and they pull the same way. ``config_dir``
# honours ``MARKETLAKE_CONFIG_DIR``, so reading it would let the override disarm the
# guard, when the override's whole purpose is to keep a process off this path. And
# ``Path.home()`` reads ``$HOME``, which a test is free to monkeypatch, so asking later
# would let a test move the protected directory out from under the guard.
#
# Both spellings are protected. A home whose ``.config`` is a symlink, which is what a
# dotfile manager usually leaves behind, has two names for one directory, and a write
# through the resolved one is the same write.


def _protected_roots(directory: str | Path) -> tuple[str, ...]:
    """Both spellings of ``directory``: as given, and fully resolved.

    It takes a directory rather than reading one so a test can drive it, since the two
    spellings collapse to one string on a machine whose ``.config`` is a real directory.
    That is every machine the suite has run on, so nothing would otherwise exercise the
    resolved one.
    """
    as_given = str(directory)
    return tuple({as_given, os.path.realpath(as_given)})


_REAL_CONFIG_DIR = str(Path.home().joinpath(*CONFIG_DIR_PARTS))
_PROTECTED_ROOTS = _protected_roots(_REAL_CONFIG_DIR)

# The open modes and flags that can change a file. Reads are left alone: the issue this
# fixture answers is a write, and refusing reads would fail tests that legitimately load
# a config the operator put there.
_WRITE_MODES = frozenset("wax+")
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC


class ConfigWriteInTest(BaseException):
    """Raised when a test writes under the machine's real ``~/.config/marketlake/``.

    It derives from ``BaseException``, the same reason ``NetworkAccessInTest`` and
    ``SubprocessAccessInTest`` do. ``reauth.write_token`` wraps its whole write in
    ``except BaseException`` to clean up the temp file, and the Sunday self-check wraps
    its read-backs in a bare ``except Exception``. A guard deriving from ``Exception``
    would be swallowed somewhere on that path and the destroyed token would ship green,
    which is the exact silence being fixed.
    """


def _is_protected(target: object) -> bool:
    """Whether ``target`` names the real config directory or something inside it.

    An ``int`` is an already-open file descriptor, which carries no path to check, so it
    passes through. ``os.fsdecode`` accepts ``str``, ``bytes``, and anything with
    ``__fspath__``, which is every form these calls take, and raises ``TypeError`` on
    anything else.

    The comparison is on the path's text, expanded and made absolute, and touches no
    filesystem. It therefore catches a path spelled at the directory, under either of
    the two names in ``_PROTECTED_ROOTS``, and it does not catch a path that arrives
    there through a symlink of its own: a link outside the directory pointing at a file
    inside it, or an ancestor that is a link the roots do not already name. Opening such
    a path for writing truncates the real file and this returns ``False``.

    Resolving every candidate with ``os.path.realpath`` would close that, and the price
    is the reason it does not. Measured on this machine, ``realpath`` costs 25.8 µs
    against ``abspath``'s 0.36 µs, and every ``open`` in the suite runs this, which is
    seconds per run to catch a shape nothing in this repo builds. Revisit that trade if
    anything here ever does build one.
    """
    if isinstance(target, int):
        return False
    try:
        text = os.fsdecode(target)
    except TypeError:
        return False
    absolute = os.path.abspath(os.path.expanduser(text))
    return any(absolute == root or absolute.startswith(root + os.sep) for root in _PROTECTED_ROOTS)


def _refuse(path: object) -> None:
    """Raise, naming the path. Called only once a path is known to be protected."""
    raise ConfigWriteInTest(
        f"a test tried to write {os.fsdecode(path)}, inside the machine's real config "
        "directory. That directory holds the live Schwab token. Point the write at "
        "tmp_path instead."
    )


@pytest.fixture(autouse=True)
def _no_config_writes() -> Iterator[None]:
    """Fail any test that writes under the machine's real ``~/.config/marketlake/``.

    Autouse, for the reason the other two guards are: a test that writes the live path
    does it by accident, and a test making that mistake would not have asked for the
    guard.

    Thirteen names are patched, in five kinds.

    1. Opening a file for writing: ``builtins.open``, ``io.open``, and ``os.open``. The
       first two are the same function object, but patching one does not reach the
       other, because ``pathlib`` looked ``open`` up on the ``io`` module and a bare
       ``open(...)`` looks it up in builtins at each call. So ``Path.write_text``,
       ``Path.write_bytes``, ``Path.open``, ``shutil``'s copies, and every bare ``open``
       are covered. ``os.open`` is the low-level descriptor, which ``Path.touch`` uses
       and ``builtins.open`` does not go through.
    2. Putting a name in the directory or taking one out: ``os.rename``, ``os.replace``,
       ``os.link``, and ``os.symlink``. The first two are checked at both ends, because
       a move out of the directory destroys what was there as surely as a move in
       overwrites it. ``os.replace`` is the step that ends every atomic write in this
       package, the token's included. The two link calls are checked at the destination
       only, since neither writes its source.
    3. Destroying what is there: ``os.unlink``, ``os.remove``, ``os.rmdir``, and
       ``os.truncate``. ``remove`` and ``unlink`` are separate function objects, so both
       are named.
    4. Creating the directory: ``os.mkdir``. ``os.makedirs`` and ``Path.mkdir`` both
       funnel through it, so neither needs its own patch. This is the one that fires
       first on ``reauth.write_token``, which calls ``parent.mkdir(parents=True,
       exist_ok=True)`` before it opens anything.
    5. Removing a tree: ``shutil.rmtree``. It gets its own patch because it does not
       reach the others. On a platform where ``shutil.rmtree.avoids_symlink_attacks``
       is true, which macOS is, it walks the tree on directory descriptors and unlinks
       each child with ``dir_fd=``, a form the text check below cannot read. Only the
       top directory reaches the guarded ``os.rmdir``, and only last. Without this
       patch the guard raises after every file in the directory is already gone, which
       is worse than not covering it at all: the run reports a refusal that protected
       nothing.

    Five things a monkeypatch cannot reach, and the guard does not claim:

    1. A child process. A test that shells out to something that writes the directory is
       outside this, the same limit the network guard names.
    2. Anything at import or collection time, before the fixture arms.
    3. A write through a descriptor that is already open, such as ``os.write`` or
       ``os.ftruncate``, and metadata-only changes such as ``os.chmod`` and
       ``os.utime``. Neither destroys the file's contents.
       A path that reaches the directory through a symlink of its own is not covered
       either, for the reason ``_is_protected`` gives.
    4. A path resolved against a directory descriptor, through the ``dir_fd`` argument
       these calls accept. The path is then relative to that descriptor rather than to
       the working directory, so the text check reads it wrongly. No call site in this
       repo passes one, but the standard library passes one to itself, which is why
       ``shutil.rmtree`` is patched by name above rather than left to the ``os`` calls
       it makes. A stdlib helper added later that walks on descriptors the same way
       would need the same treatment.
    5. A writer that never goes through these names, such as an extension module holding
       the path itself. ``pyarrow`` writes through its own filesystem layer, and no
       Parquet or Arrow write in this package targets the config directory.

    The fixture holds its own ``MonkeyPatch``, not the shared one, for the reason the
    other two guards do: a test calling ``monkeypatch.undo()`` must not disarm it.
    """

    def guard_open(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(file: object, mode: object = "r", *pos: object, **kwargs: object) -> object:
            text_mode = mode if isinstance(mode, str) else ""
            if _WRITE_MODES & set(text_mode) and _is_protected(file):
                _refuse(file)
            return real(file, mode, *pos, **kwargs)

        return refuse

    def guard_os_open(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(path: object, flags: int = 0, *pos: object, **kwargs: object) -> object:
            if flags & _WRITE_FLAGS and _is_protected(path):
                _refuse(path)
            return real(path, flags, *pos, **kwargs)

        return refuse

    def guard_one(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(path: object, *pos: object, **kwargs: object) -> object:
            if _is_protected(path):
                _refuse(path)
            return real(path, *pos, **kwargs)

        return refuse

    def guard_both_ends(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(src: object, dst: object, *pos: object, **kwargs: object) -> object:
            for end in (src, dst):
                if _is_protected(end):
                    _refuse(end)
            return real(src, dst, *pos, **kwargs)

        return refuse

    def guard_destination(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(src: object, dst: object, *pos: object, **kwargs: object) -> object:
            if _is_protected(dst):
                _refuse(dst)
            return real(src, dst, *pos, **kwargs)

        return refuse

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(builtins, "open", guard_open(builtins.open))
        mp.setattr(io, "open", guard_open(io.open))
        mp.setattr(os, "open", guard_os_open(os.open))
        mp.setattr(os, "rename", guard_both_ends(os.rename))
        mp.setattr(os, "replace", guard_both_ends(os.replace))
        mp.setattr(os, "link", guard_destination(os.link))
        mp.setattr(os, "symlink", guard_destination(os.symlink))
        for name in ("unlink", "remove", "rmdir", "truncate", "mkdir"):
            mp.setattr(os, name, guard_one(getattr(os, name)))
        mp.setattr(shutil, "rmtree", guard_one(shutil.rmtree))
        yield
