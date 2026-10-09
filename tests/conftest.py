"""Shared fixtures that expose the four seams, the fixture-lake builder and a loopback STS.

It also carries four guards, one redirect, one deletion, one host pin, and one check on
the outcome. The network guard fails any test that reaches another machine from inside
this process. The subprocess guard fails any test that shells out to rsync, launchctl,
pmset, tmutil, systemctl, or timedatectl. The exec guard fails any test that would
replace this process through ``os.execv`` or ``os.execve``. The config-directory guard
fails any test that writes under the machine's real ``~/.config/marketlake/``, and its
other half, the predicate deciding what counts as that directory, sits in
``tests/support/config_guard.py`` so a child can ask without importing this file. The
redirect points this process, and every child that inherits its environment, at a
throwaway config directory, which is what covers the children the four guards cannot
reach. The deletion drops an inherited ``MARKETLAKE_CONFIG``, which names a config file
rather than a directory and so is not moved by that redirect. The host pin makes every
test run as macOS unless it asks for Linux, so CI's Linux runner takes the same branch as
the laptop. The check on the outcome lists the real config directory when this file is
imported and again when the session ends, and fails the run when it changed.
"""

from __future__ import annotations

import atexit
import builtins
import io
import os
import shutil
import socket
import subprocess
import tempfile
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.paths import CONFIG_DIR_ENV

# -- the config-directory redirect -------------------------------------------------------

# Every guard in this file is a monkeypatch, so each holds inside this process and
# nowhere else. A child the suite spawns has none of them. That limit bit during the
# config-directory guard's own review: a test spawned a child running
# ``lake.reauth.write_token`` at the default token path, nothing in the child refused
# it, and a stub landed on a working Schwab token. Nothing in the suite reaches the real
# directory from a child today, and the next test that spawns one starts from the same
# place.
#
# So the suite exports ``MARKETLAKE_CONFIG_DIR`` at a throwaway directory. A child that
# inherits this process's environment resolves its config directory there, whether or not
# the test that spawned it remembered to arrange anything.
#
# Two shapes of child are outside it, both on purpose, and neither can be closed from
# here. A child handed an explicit ``env=`` carries only what that mapping names, which
# is how ``_child`` in ``tests/component/test_config_dir_override.py`` asks what a
# process with no override resolves, and how the render tests sandbox a rendered
# script. And the rendered ``reauth.sh`` unsets the variable before it calls the tool, so
# anything it runs is outside this by design. One test's explicit-``env=`` children do
# reach real code at a default path: ``tests/component/test_unattended_entries_fresh.py``
# starts each entry with its launchd job's or systemd unit's own environment, and the
# compaction child and the token pull child the daemon spawns with the daemon job's.
# None of those carries a config-directory variable, so ``config.default_config_path()``
# resolves under ``HOME`` in the child, and the pull child's ``token.json`` does too.
# That test arranges its own redirect by pointing ``HOME`` at a throwaway directory. Any
# other child that reaches a default path has to do the same.
#
# This process moves with its children rather than staying behind. Every default path in
# the package is a resolver called when it is used, so the in-process export is what
# keeps every resolver the suite calls off the real directory. Without it, a test that
# omits a path would read the real config, and only the guard below would stand between
# its write and the live token. A later edit that "simplifies" the export to children
# only would bring that back.
#
# The export still sits above the rest of this file's imports. Nothing in the package
# resolves a default at import any more, and
# ``tests/unit/test_config_defaults_scanner.py`` fails if something starts to, but a
# variable set before anything else runs needs no argument about import order.
#
# An inherited value is replaced rather than honoured. It is whatever a person exported,
# so it can name anything the real directory included, and where the suite's children
# write is not for the shell that launched pytest to decide.
#
# None of this stands in for the guard below. The guard settles what it protects from
# ``Path.home()`` and never reads this variable, on purpose, so a test that names the
# real path by hand still fails rather than slipping past a redirect that path ignores.
_THROWAWAY_CONFIG_DIR = tempfile.mkdtemp(prefix="marketlake-suite-config-")
os.environ[CONFIG_DIR_ENV] = _THROWAWAY_CONFIG_DIR
atexit.register(shutil.rmtree, _THROWAWAY_CONFIG_DIR, ignore_errors=True)

from lake.cassette import load_cassette  # noqa: E402
from lake.config import CONFIG_PATH_ENV  # noqa: E402
from tests.support.clock import ManualClock  # noqa: E402
from tests.support.config_guard import (  # noqa: E402
    REAL_CONFIG_DIR,
    is_protected,
)
from tests.support.lake import FixtureLake  # noqa: E402
from tests.support.sts import StsServer  # noqa: E402
from tests.support.vendor import CassetteVendor  # noqa: E402

# An inherited ``MARKETLAKE_CONFIG`` is deleted for the same reason the directory above is
# replaced. ``load_config`` checks that variable before the default, and it names a file
# rather than a directory, so the redirect above does not move it. Without this line, a
# test that omits ``lake_root`` would resolve the real ``config.yaml`` on a machine whose
# shell exported one, and the throwaway path only on a machine where it did not. Resolving
# it merely reads the file, so nothing is lost, but where the suite's reads land is not for
# the launching shell to decide.
#
# It runs once at import rather than per test, because what it covers is a child that
# inherits this process's environment, and a child spawned outside a test body would
# inherit a value a per-test fixture had put back. A test setting the variable through
# ``monkeypatch.setenv`` is untouched either way.
# ``test_an_inherited_config_file_is_deleted_rather_than_honoured`` in
# ``tests/component/test_suite_config_dir_redirect.py`` is what checks this, from a child,
# on a machine that exported nothing of its own.
os.environ.pop(CONFIG_PATH_ENV, None)

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


# -- the loopback STS, marketlake #737 ------------------------------------------------


@pytest.fixture(scope="session")
def sts_server() -> Iterator[StsServer]:
    """One loopback STS for the whole run, started on first use.

    Session-scoped because its users sit in two tiers' directories, and a server per test
    would cost a thread and a port each time. ``sts`` resets it before every test.
    """
    with StsServer() as server:
        yield server


@pytest.fixture
def sts(sts_server: StsServer, monkeypatch: pytest.MonkeyPatch) -> StsServer:
    """The loopback STS, reset, with ``aws_session.STS_ENDPOINT_URL`` pointed at it.

    The proxy variables are deleted in both spellings, because the STS client honours
    them as the S3 and SSM clients do, and a proxy would carry the assume elsewhere. Apply
    it test by test or with ``usefixtures`` on a file, never to the whole suite, so a test
    without it can still show that no STS call happens.
    """
    from lake import aws_session  # lazy: the redirect above runs before any ``lake`` import

    sts_server.reset()
    for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(aws_session, "STS_ENDPOINT_URL", sts_server.url)
    return sts_server


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

# Eight production call sites shell out to a named external tool through
# ``subprocess.run``: ``RsyncBackup.sync`` runs ``rsync``, ``launchctl_probe`` runs
# ``launchctl``, ``read_pmset_schedule`` and ``pmset_assertions_probe`` run ``pmset``,
# ``read_exclusions`` runs ``tmutil``, ``sweep.set_sunday_wake`` runs ``pmset`` under
# ``sudo``, ``systemctl_probe`` runs ``systemctl``, and ``timedatectl_clock_probe`` runs
# ``timedatectl``. Each is a seam, so a test injects a fake in place of the function that
# calls it. A test that forgets runs the real tool instead, which the network guard above
# cannot catch: none of the eight touch a socket in this process. The two Linux probes
# would read the CI runner's own systemd, whose answer has nothing to do with the case
# under test. This fixture closes that gap the same way, on those program names only.
#
# The refusal has to name the program rather than block every subprocess. The render
# tests in ``tests/component/test_control_plane_render.py`` and
# ``tests/component/test_control_plane_systemd.py`` run the rendered install, reinstall,
# restart, and uninstall scripts, and ``deploy/linux-install.sh``, for real, each
# sandboxed by a fake ``PATH`` that points at stand-ins for the tools the script calls.
# Those calls name a script path or ``bash``, never one of the guarded names directly, so
# refusing only those names leaves them untouched.
#
# The sixth site is what made the wrapper case real. ``set_sunday_wake`` runs
# ``sudo -n /usr/bin/pmset schedule ...``, so the program at ``argv[0]`` is ``sudo`` and
# the guarded name sits three elements further along. Reading the head alone let exactly
# the call with the largest blast radius through, because it is the only guarded call
# that writes rather than reads: a forgotten seam would have re-scheduled the
# developer's own machine. So the wrapper is stepped over below.

_GUARDED_PROGRAMS = frozenset({"rsync", "launchctl", "pmset", "tmutil", "systemctl", "timedatectl"})

# Programs that run another program named later in the same argument list. ``sudo`` is
# the one this repo uses. ``env`` and ``arch`` are listed beside it because all three
# take the same shape, and a guard that knew only the spelling in front of it today
# would have to be widened again by whoever adds the next one.
_WRAPPER_PROGRAMS = frozenset({"sudo", "env", "arch"})


class SubprocessAccessInTest(BaseException):
    """Raised when a test reaches one of the six guarded programs.

    They are ``rsync``, ``launchctl``, ``pmset``, ``tmutil``, ``systemctl``, and
    ``timedatectl``.

    It derives from ``BaseException``, the same reason ``NetworkAccessInTest`` does.
    The Sunday self-check wraps both ``schedule_reader()`` and ``exclusion_reader()``
    in a broad ``except Exception``, on purpose, so a read-back it cannot parse becomes
    a report line instead of a page. A guard that inherited from ``Exception`` would
    land in that same catch, turn into a line reading "pmset read-back unreadable", and
    the forgotten seam would ship green.
    """


def _program_of(args: object) -> str | None:
    """The program a subprocess call really runs, or ``None`` when there is not one.

    ``subprocess.run`` and ``Popen`` both take the command as a sequence whose first
    element is the program, which is how every guarded call site and every render test
    calls them. A ``bytes`` element is decoded first, since ``subprocess`` accepts one
    and a raw ``str()`` of it would never match a guarded name.

    **A prefix wrapper is stepped over rather than answered.** ``sudo -n /usr/bin/pmset
    schedule ...`` runs ``pmset``, and reading ``argv[0]`` answers ``sudo``, which is in
    no guarded set and so let the call through. That was not hypothetical. It is the
    shape ``sweep.set_sunday_wake`` takes, and it is the only guarded call in the repo
    that writes rather than reads, so the gap sat under the one seam whose blast radius
    is the developer's own power schedule.

    The walk skips a wrapper and the options that follow it, then answers the first
    element that is neither. A wrapper with nothing after it answers itself, because
    ``sudo -l`` lists rules and runs nothing. It cannot tell an option's value from the
    program when a wrapper takes one, as in ``env -C /tmp pmset``, and answers ``/tmp``
    there. That over-reports rather than under-reports, which is the safe direction for
    a guard, and nothing in this repo passes one.

    One form is still not handled: a single string with ``shell=True``. No call site
    uses it, and a guard for it would have to parse a shell command line.
    """
    if not isinstance(args, (list, tuple)) or not args:
        return None

    def name_of(element: object) -> str:
        if isinstance(element, bytes):
            element = os.fsdecode(element)
        return Path(str(element)).name

    index = 0
    while index < len(args):
        program = name_of(args[index])
        if program not in _WRAPPER_PROGRAMS:
            return program
        index += 1
        while index < len(args) and str(args[index]).startswith("-"):
            index += 1
        if index >= len(args):
            return program
    return None


@pytest.fixture(autouse=True)
def _no_subprocess() -> Iterator[None]:
    """Fail any test that would shell out to one of the six guarded programs.

    Autouse, for the same reason ``_no_network`` is: the failure this catches is a test
    forgetting to inject one of the eight seams, and a test that forgets one would
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


# -- the exec guard ----------------------------------------------------------------------

# The evening vendor sweep ends a run by replacing its own process with compaction, through
# ``os.execv`` in ``sweep.main`` (marketlake #833). Several tests call ``sweep.main`` on an ok
# weekday evening, which is exactly the run that hands off. A test that forgot to patch the
# ``exec`` would not fail. It would replace the pytest process with ``python -m lake.compact``
# against whatever config the test wrote, and the run would end there with no report. So
# both ``exec`` forms that take an argument list are refused here, the same way the
# subprocess guard above refuses a named program.


class ExecInTest(BaseException):
    """Raised when a test reaches ``os.execv`` or ``os.execve``.

    It derives from ``BaseException`` for ``SubprocessAccessInTest``'s reason, and one more.
    ``sweep.main`` catches ``OSError`` around its ``exec`` and turns it into exit 1, so a
    guard that was an ``OSError`` would become an exit code, and four of the tests that reach
    the hand-off do not assert theirs.
    """


@pytest.fixture(autouse=True)
def _no_exec() -> Iterator[None]:
    """Fail any test that would replace this process through ``os.execv`` or ``os.execve``.

    Autouse, for ``_no_network``'s reason. A test patches ``os.execv`` with a recorder on its
    own ``monkeypatch``, which lands on top of this patch and is undone first. The fixture
    holds its own ``MonkeyPatch`` for the reason ``_no_network`` does: a test calling
    ``monkeypatch.undo()`` must not disarm it.
    """

    def refuse(path: object, *args: object, **kwargs: object) -> None:
        raise ExecInTest(f"a test tried to exec {path}. Patch os.execv with a recorder instead.")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(os, "execv", refuse)
        mp.setattr(os, "execve", refuse)
        yield


# -- the host pin -----------------------------------------------------------------------

# ``control_plane.is_macos`` decides which probes the jobs wire, and it reads
# ``sys.platform``. CI runs on Linux and the laptop on macOS, so without a pin every test
# that drives a ``main`` would take one branch on the laptop and the other on CI. So every
# test runs as macOS unless it asks for ``on_linux``.
#
# The pin goes on the helper, never on ``sys.platform``, because the helper is what every
# caller reads, and ``daemon`` and ``sweep`` call it through the module so this one patch
# reaches them too. The real helper is still checked, by a test that binds it at import,
# ``tests/unit/test_control_plane_linux.py``.


@pytest.fixture(autouse=True)
def _host_is_macos() -> Iterator[None]:
    """Pin every test to the macOS host unless it asks for ``on_linux``.

    The fixture holds its own ``MonkeyPatch``, for the reason ``_no_network`` does: a test
    calling ``monkeypatch.undo()`` must not disarm it.
    """
    from lake import control_plane

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(control_plane, "is_macos", lambda: True)
        yield


@pytest.fixture
def on_linux(_host_is_macos: None) -> Iterator[None]:
    """Run this test as the Linux host.

    It asks for the pin by name, so the pin is set first and this lands on top of it, and
    it holds its own ``MonkeyPatch`` for the reason the pin does.
    """
    from lake import control_plane

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(control_plane, "is_macos", lambda: False)
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
# What counts as the real directory, and whether a path is inside it, is settled in
# ``tests/support/config_guard.py`` rather than here. That module has no import side
# effect, so a child process can ask it the question without importing this file and
# having the redirect above replace the very variable the child was set up to test.
# The reasoning about which home is read, and why both spellings are protected, lives
# there beside the code it explains.


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
       outside this, the same limit the network guard names. That one is covered
       separately, by the redirect at the top of this file: a child inheriting this
       process's environment resolves a throwaway config directory rather than the real
       one, so there is nothing there for it to destroy. The redirect is not a second
       guard, though. It moves what a default resolves to and refuses nothing, so a
       child handed the real path by hand still writes it.
    2. Anything at import or collection time, before the fixture arms.
    3. A write through a descriptor that is already open, such as ``os.write`` or
       ``os.ftruncate``, and metadata-only changes such as ``os.chmod`` and
       ``os.utime``. Neither destroys the file's contents.
       A path that reaches the directory through a symlink of its own is not covered
       either, for the reason ``config_guard.is_protected`` gives.
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
            if _WRITE_MODES & set(text_mode) and is_protected(file):
                _refuse(file)
            return real(file, mode, *pos, **kwargs)

        return refuse

    def guard_os_open(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(path: object, flags: int = 0, *pos: object, **kwargs: object) -> object:
            if flags & _WRITE_FLAGS and is_protected(path):
                _refuse(path)
            return real(path, flags, *pos, **kwargs)

        return refuse

    def guard_one(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(path: object, *pos: object, **kwargs: object) -> object:
            if is_protected(path):
                _refuse(path)
            return real(path, *pos, **kwargs)

        return refuse

    def guard_both_ends(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(src: object, dst: object, *pos: object, **kwargs: object) -> object:
            for end in (src, dst):
                if is_protected(end):
                    _refuse(end)
            return real(src, dst, *pos, **kwargs)

        return refuse

    def guard_destination(real: Callable[..., object]) -> Callable[..., object]:
        def refuse(src: object, dst: object, *pos: object, **kwargs: object) -> object:
            if is_protected(dst):
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


# -- the real config directory is unchanged by the run -----------------------------------

# Everything above is a check on the attempt. The guard refuses a write, the redirect
# moves what a default resolves to, and two child tests refuse to write until they have
# confirmed their own redirect took. None of them looks at the outcome, and each names
# limits it cannot cover. The guard cannot see a write through a descriptor that is
# already open, a path resolved against a directory descriptor, or an extension module
# holding the path. The redirect does not reach a child handed an explicit environment.
#
# So the directory is listed when this file is imported and again when the session ends,
# and a difference fails the run. This is the only thing here that holds the property the
# whole arrangement exists for, which is that the four files are still the four files.
#
# It opens nothing. ``os.scandir`` and ``stat`` read directory metadata, so the live
# brokerage credential is never read into this process. That is the same care every other
# test of this directory takes.
#
# One limit decides how to read a failure, so it is stated rather than buried. A running
# daemon rewrites ``token.json`` on its own when it refreshes the access token, and no
# stat field tells that apart from a stub landing on the same path. The report therefore
# names both explanations and leaves the reader to open the file. A rare false alarm that
# explains itself is the better trade against a blind spot on the one file whose loss
# costs a browser login and half an hour of gapped capture.


def _config_dir_listing(directory: str) -> dict[str, tuple[int, int, int]]:
    """Every path under ``directory``, with the three stat fields a write moves.

    Keyed by the path relative to ``directory``, and the walk goes all the way down. The
    real directory holds three regular files and no subdirectory today, and watching only
    the top level would miss a rewrite one level in: a parent's mtime does not move when
    a grandchild's contents change.

    A subdirectory is recorded with zeroes rather than its own stat, so that one
    appearing or going is reported while the mtime it gains every time a child is written
    does not add a second line about a change already named.

    A directory that is not there comes back empty, which is the case on CI and on a
    fresh machine, and an empty listing compared against another empty one is no change.

    Symlinks are stated without following them, so a link whose target moves counts as
    the link being unchanged, and a link to a directory is never descended into. That
    rules out a cycle and matches what the guard beside this does with symlinks.
    """
    base = Path(directory)
    listing: dict[str, tuple[int, int, int]] = {}
    pending = [base]
    while pending:
        try:
            entries = list(os.scandir(pending.pop()))
        except OSError:
            continue
        for entry in entries:
            path = Path(entry.path)
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
                status = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            key = str(path.relative_to(base))
            if is_dir:
                listing[key] = (0, 0, 0)
                pending.append(path)
            else:
                listing[key] = (status.st_mtime_ns, status.st_size, status.st_ino)
    return listing


def _config_dir_changes(
    before: Mapping[str, tuple[int, int, int]],
    after: Mapping[str, tuple[int, int, int]],
) -> tuple[str, ...]:
    """One line per name that appeared, went, or was rewritten. Empty when nothing moved.

    The inode is compared alongside the mtime and the size because an atomic write puts
    the bytes in a temp file and renames over the target, which can land inside one
    mtime tick while carrying a different inode. That rename is the call that destroyed
    the token on 2026-09-13.
    """
    lines = []
    for name in sorted(set(before) | set(after)):
        if name not in after:
            lines.append(f"{name} is gone")
        elif name not in before:
            lines.append(f"{name} appeared")
        elif before[name] != after[name]:
            lines.append(f"{name} was rewritten")
    return tuple(lines)


_CONFIG_DIR_AT_START = _config_dir_listing(REAL_CONFIG_DIR)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fail the run when the real config directory changed while it ran.

    The exit status is set rather than a test failed, because there is no test left to
    fail by the time this runs. A green summary above a non-zero exit is confusing on its
    own, so the report says what changed and what the two explanations are.
    """
    changes = _config_dir_changes(_CONFIG_DIR_AT_START, _config_dir_listing(REAL_CONFIG_DIR))
    if not changes:
        return
    # Only a run that would otherwise have passed is turned into a failure. A run that
    # was interrupted, or that already failed, keeps the status it earned, because
    # overwriting it would say "tests failed" about a session that was cancelled.
    if not session.exitstatus:
        session.exitstatus = 1
    lines = [
        f"{REAL_CONFIG_DIR} changed while the suite ran.",
        "",
        *(f"  {line}" for line in changes),
        "",
        "Two things do that. Something outside the suite wrote the directory, which a",
        "running daemon does every time it refreshes the access token. Or the suite",
        "reached it, past the guard and past the redirect, which is what this check",
        "exists to notice. No stat field tells those apart, so open the file and look",
        "before assuming either. The tests above can still all have passed.",
    ]
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        print("\n".join(lines))
        return
    reporter.write_sep("=", "the real config directory changed", red=True)
    for line in lines:
        reporter.write_line(line)
