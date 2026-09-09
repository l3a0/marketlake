"""The fake transport.

``FakeTransport`` implements the ``Transport`` seam by recording every message instead
of POSTing it to ntfy.

``run_loop_from_config`` used to default this seam to the real transport, which holds the
ntfy topic, and the topic is the write credential for the channel. A test that passed none
could therefore page a person's phone. The seam is required now, so this is what a test
passes instead. The required argument is the guard. Recording is only what makes the
message readable afterwards.

A test that only needs the seam filled ignores ``messages``. A test that asserts what was
paged reads it. ``tests/component/test_alert.py`` keeps its own ``Recording`` fake, which
predates this one and records under a different name.
"""

from __future__ import annotations

from lake.alert import Message


class FakeTransport:
    """A ``Transport`` that records each message rather than sending it."""

    def __init__(self) -> None:
        self.messages: list[Message] = []

    def send(self, message: Message) -> None:
        self.messages.append(message)
