"""The fake pinger.

``FakePinger`` implements the ``Pinger`` seam by recording every ping instead of
making a request. Two facts about a ping matter, and it records both.

1. The URL names which check was fed. That is how the suite holds the rule that a ping
   fires only on a job's success condition, never on mere liveness.
2. A place in a shared event log fixes when the ping happened against the other steps
   of a job. The compaction and runner tests read it to hold their ordering: the backup
   syncs before the ping, and a failed step pings not at all.

A test that cares about one fact ignores the other. Passing no log gives the pinger
its own, so ``ping`` never has to ask which kind of test it is serving.
"""

from __future__ import annotations


class FakePinger:
    """A ``Pinger`` that records each ping rather than making a request."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = [] if events is None else events
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)
        self.events.append("ping")
