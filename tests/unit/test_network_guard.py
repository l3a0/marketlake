"""The network guard itself, which nothing else covers.

The guard in ``tests/conftest.py`` is the reason a forgotten seam fails loudly instead of
pinging the owner's live dead-man check. It is autouse, so every test depends on it and no
test asserts it. These do.

Three properties carry the whole guard, and each is covered below.

1. A connection leaving this machine fails, whether it goes through ``urlopen``,
   ``socket.create_connection``, or a raw ``socket.socket``.
2. The failure is not an ``Exception``. Production catches bare ``Exception`` around a
   ping on purpose, so a guard derived from it would be swallowed and the test would pass.
3. Loopback still works, because the dashboard's integration tests bind and call a local
   server.
"""

from __future__ import annotations

import socket
import urllib.request

import pytest

from tests.conftest import NetworkAccessInTest

REMOTE = ("hc-ping.com", 443)


def test_urlopen_to_another_machine_is_refused():
    with pytest.raises(NetworkAccessInTest):
        urllib.request.urlopen("https://hc-ping.com/secret-key/capture")


def test_create_connection_to_another_machine_is_refused():
    with pytest.raises(NetworkAccessInTest):
        socket.create_connection(REMOTE)


def test_a_raw_socket_to_another_machine_is_refused():
    # The route the guard's docstring promises and the one urllib3 uses. It reached the
    # network until the guard patched `socket.socket.connect` rather than only the
    # `create_connection` convenience wrapper.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(NetworkAccessInTest):
            sock.connect(REMOTE)


def test_the_refusal_is_not_caught_by_a_bare_except_exception():
    # `DeadMan._ping` swallows `Exception` by design, because a missed ping is what the
    # check exists to notice. A guard deriving from `Exception` would be swallowed there
    # and the test would pass, which is the silence this whole guard exists to end.
    assert not issubclass(NetworkAccessInTest, Exception)

    caught = False
    try:
        try:
            socket.create_connection(REMOTE)
        except Exception:  # noqa: BLE001 - reproducing production's swallow on purpose
            caught = True
    except NetworkAccessInTest:
        pass
    assert not caught, "the guard was swallowed by a bare except Exception"


def test_loopback_still_connects():
    # The dashboard's integration tests bind a local server and call it. Refusing
    # loopback would break them, so the allowance is part of the contract.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        with socket.create_connection(server.getsockname(), timeout=1) as client:
            assert client.getpeername()[0] == "127.0.0.1"


def test_a_test_calling_monkeypatch_undo_does_not_disarm_the_guard(monkeypatch):
    # The guard used to borrow the test's own MonkeyPatch, so they shared one undo stack
    # and this call reverted both patches for the rest of the test.
    monkeypatch.setattr(socket, "has_dualstack_ipv6", lambda: False)
    monkeypatch.undo()
    with pytest.raises(NetworkAccessInTest):
        socket.create_connection(REMOTE)
