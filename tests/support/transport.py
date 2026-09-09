"""The fake transport.

``FakeTransport`` implements the ``Transport`` seam by recording every message instead
of POSTing it to ntfy. The real transport holds the topic, which is the write credential
for the channel, so a test that lets the production default build one is a test that can
page a person's phone. Recording here is what makes that impossible.

A test that only needs the seam filled ignores ``messages``. A test that asserts what
was paged reads it.
"""

from __future__ import annotations

from lake.alert import Message


class FakeTransport:
    """A ``Transport`` that records each message rather than sending it."""

    def __init__(self) -> None:
        self.messages: list[Message] = []

    def send(self, message: Message) -> None:
        self.messages.append(message)
