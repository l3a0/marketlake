"""Shared fixtures that expose the four seams and the fixture-lake builder.

It also carries the network guard, which fails any test that reaches another machine
from inside this process.
"""

from __future__ import annotations

import socket
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.cassette import load_cassette
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
