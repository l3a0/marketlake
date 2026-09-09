"""Shared fixtures that expose the four seams and the fixture-lake builder.

It also carries the network guard, which fails any test that reaches the network.
"""

from __future__ import annotations

import socket
import urllib.request
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


_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})


class NetworkAccessInTest(BaseException):
    """Raised when a test reaches for the network. Inject the seam instead.

    It derives from ``BaseException`` rather than ``Exception`` on purpose. Production
    swallows a failed ping by design, because a missed ping is what the check exists to
    notice, so ``DeadMan._ping`` catches bare ``Exception``. A guard that inherits from
    ``Exception`` is caught there and the test passes anyway, which is the exact silence
    being fixed. This one passes through, the way ``KeyboardInterrupt`` does.
    """


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail any test that opens a socket or a URL.

    Autouse, because the failure this catches is a test forgetting to pass a seam, and a
    test that forgets one would equally forget to ask for the guard. Both ``urlopen`` and
    ``socket.socket`` are covered: the first is what the two seams use today, the second is
    what any replacement would use tomorrow.
    """

    def refuse_urlopen(request: object, *args: object, **kwargs: object) -> None:
        url = getattr(request, "full_url", None) or str(request)
        host = url.split("/")[2] if "//" in url else url
        raise NetworkAccessInTest(
            f"a test tried to reach {host}. Pass a fake pinger or transport instead of "
            "letting the production default build a real one."
        )

    real_create_connection = socket.create_connection

    def refuse_remote(address: tuple[str, int], *args: object, **kwargs: object):
        # The dashboard's integration tests bind and call a loopback server on purpose,
        # so only a connection leaving this machine is a failure.
        host = address[0]
        if host in _LOOPBACK:
            return real_create_connection(address, *args, **kwargs)  # type: ignore[arg-type]
        raise NetworkAccessInTest(
            f"a test tried to connect to {host}. Inject the seam rather than reaching out."
        )

    monkeypatch.setattr(urllib.request, "urlopen", refuse_urlopen)
    monkeypatch.setattr(socket, "create_connection", refuse_remote)
