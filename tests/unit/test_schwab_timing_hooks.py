"""The timing hooks ``attach_timing`` installs, driven by hand with no network.

The loopback tests in ``tests/component/test_request_timing_transport.py`` run the real
stack, but over plain HTTP, so TLS never fires there. Every Schwab request runs over TLS,
so these tests drive the installed hooks and the trace callback directly, in the order
httpcore fires them, on a manual clock. A fake request and response carry only what the
hooks touch: ``extensions`` and ``request``.

They cover the rules that the real stack cannot reach on demand:

1. ``connected`` is the TLS handshake's end, the last of the two connection events.
2. A clock that fails once costs exactly the field it was stamping, and the record keeps
   the first failure rather than the last.
3. A client whose session refuses hooks is left untimed rather than raising.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from lake.schwab import SchwabVendor, attach_timing, read_timing
from tests.support.clock import ManualClock
from tests.support.schwab import FakeSchwabClient

_START = datetime(2026, 9, 24, 18, 0, tzinfo=UTC)


def _at(seconds: float) -> datetime:
    return _START + timedelta(seconds=seconds)


class _Session:
    def __init__(self) -> None:
        self.event_hooks: dict = {"request": [], "response": []}


def _hooks(clock):
    """Attach timing to a fake client and hand back its two hooks."""
    client = SimpleNamespace(session=_Session())
    assert attach_timing(client, clock) is True
    (on_request,) = client.session.event_hooks["request"]
    (on_response,) = client.session.event_hooks["response"]
    return on_request, on_response


def _request():
    return SimpleNamespace(extensions={})


def _drive(clock: ManualClock, *, tls: bool = True):
    """One request through the hooks in httpcore's order, one second between events."""
    on_request, on_response = _hooks(clock)
    request = _request()
    on_request(request)
    trace = request.extensions["trace"]
    clock.advance(1)
    trace("connection.connect_tcp.complete", {})
    if tls:
        clock.advance(1)
        trace("connection.start_tls.complete", {})
    clock.advance(1)
    on_response(SimpleNamespace(request=request))
    clock.advance(1)
    trace("http11.receive_response_body.complete", {})
    return read_timing(SimpleNamespace(request=request, num_bytes_downloaded=4096))


def test_connected_is_when_the_tls_handshake_finished():
    timing = _drive(ManualClock(start=_START))
    assert timing.sent == _at(0)
    assert timing.connected == _at(2)
    assert timing.headers == _at(3)
    assert timing.body == _at(4)
    assert timing.bytes == 4096
    assert timing.failure is None


def test_without_tls_connected_is_when_the_tcp_connect_finished():
    timing = _drive(ManualClock(start=_START), tls=False)
    assert timing.connected == _at(1)


class _FailsOnce:
    """A clock that raises on exactly its ``fail_on``-th read and answers every other one."""

    def __init__(self, fail_on: int, message: str = "clock broke") -> None:
        self._inner = ManualClock(start=_START)
        self._reads = 0
        self._fail_on = fail_on
        self._message = message

    def now(self) -> datetime:
        self._reads += 1
        if self._reads == self._fail_on:
            raise RuntimeError(self._message)
        return self._inner.now()

    def advance(self, seconds: float) -> None:
        self._inner.advance(seconds)

    def monotonic(self) -> float:  # pragma: no cover - unused by the hooks
        return self._inner.monotonic()

    def sleep(self, seconds: float) -> None:  # pragma: no cover - unused by the hooks
        self._inner.sleep(seconds)


def test_a_failed_sent_stamp_costs_sent_and_nothing_else():
    # The trace is installed before the stamp, so it still records the rest.
    timing = _drive(_FailsOnce(1))
    assert timing.sent is None
    assert (timing.connected, timing.headers, timing.body) == (_at(2), _at(3), _at(4))
    assert timing.failure == "RuntimeError: clock broke"


def test_a_failed_headers_stamp_costs_headers_and_nothing_else():
    # Reads: sent, TCP, TLS, then headers is the fourth.
    timing = _drive(_FailsOnce(4))
    assert timing.headers is None
    assert (timing.sent, timing.connected, timing.body) == (_at(0), _at(2), _at(4))
    assert timing.failure == "RuntimeError: clock broke"


def test_the_record_keeps_the_first_failure():
    class _FailsTwice(_FailsOnce):
        def now(self) -> datetime:
            self._reads += 1
            if self._reads == 1:
                raise RuntimeError("first")
            if self._reads == 4:
                raise RuntimeError("second")
            return self._inner.now()

    timing = _drive(_FailsTwice(0))
    assert timing.failure == "RuntimeError: first"


def test_a_session_that_refuses_hooks_is_left_untimed():
    class _Locked:
        @property
        def event_hooks(self) -> dict:
            return {"request": [], "response": []}

        @event_hooks.setter
        def event_hooks(self, value: dict) -> None:
            raise AttributeError("read-only")

    client = FakeSchwabClient()
    client.session = _Locked()
    assert attach_timing(client, ManualClock(start=_START)) is False
    assert SchwabVendor(client) is not None
