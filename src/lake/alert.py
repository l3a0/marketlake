"""Pages: what one says, who sends it, and what happens when sending fails.

A page is the loud channel. It goes to a phone, so the design caps the day at forty and
says a page that never left the laptop must never be invisible. Both of those live here
rather than in the callers, so a producer decides only that something is wrong.

The publisher is total. ``publish`` never raises and never lets a caller forget to
handle a failure, because a watchdog that crashes while reporting a dead surface is
worse than one that stays quiet. Every message that does not reach the phone is written
down first, under ``reports/``, one write-once file each.

That sink is a directory of dated files rather than a ledger, and the difference is not
cosmetic. The scrub's reverse pass treats an unexpected file at the lake root as an
orphan, so a ledger there would have to widen an exclusion set the design calls
enumerated rather than implied. A dated file under ``reports/`` is already covered, and
the date in the path means a query for today cannot accidentally count last month.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.calendar import MARKET_TZ
from lake.paths import LakePaths

# The design's cap on pages a day. The forty-first is written down and never sent, so a
# storm cannot empty the phone's attention for the one page that matters.
DEFAULT_DAILY_CAP = 40

# Why a message never reached the phone. These are different failures and must not read
# alike: one is the network, one is the cap, one is this module refusing to send.
POST_FAILED = "post_failed"
CAP_REACHED = "cap_reached"
REFUSED = "refused"


@dataclass(frozen=True)
class Message:
    """One page. The event name matches the design's message table."""

    event: str
    title: str
    body: str
    priority: int = 5


@dataclass(frozen=True)
class Delivery:
    """What became of one message."""

    sent: bool
    reason: str | None = None


@runtime_checkable
class Transport(Protocol):
    """The POST itself. The real one talks to ntfy, a test records."""

    def send(self, message: Message) -> None: ...


class NtfyTransport:
    """The real POST, to ntfy's topic URL.

    The topic is the secret half of that URL, exactly as the ping key is for
    healthchecks, so it is held here and never put in a message. A publisher refuses any
    page whose own text contains it.
    """

    def __init__(self, topic: str, *, host: str = "https://ntfy.sh") -> None:
        self._url = f"{host.rstrip('/')}/{topic}"

    def send(self, message: Message) -> None:
        import urllib.request  # lazy: only a real send reaches the network

        request = urllib.request.Request(
            self._url,
            data=message.body.encode("utf-8"),
            headers={
                "Title": message.title,
                "Priority": str(message.priority),
                "Tags": message.event,
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10):
            pass


class Publisher:
    """Sends pages, caps the day, and writes down every one that did not go.

    The cap is keyed by the Eastern date, so it resets with the session rather than with
    the process. A daemon restarted at noon does not get a fresh forty.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        transport: Transport | None = None,
        secrets: Sequence[str] = (),
        daily_cap: int = DEFAULT_DAILY_CAP,
        pid: int | None = None,
    ) -> None:
        self._paths = LakePaths(Path(lake_root))
        self._transport = transport
        # The values that must never reach a phone: the ping key and the ntfy topic.
        # Empty means a caller that holds no secrets, which refuses nothing.
        self._secrets = tuple(secret for secret in secrets if secret)
        self._cap = daily_cap
        self._pid = os.getpid() if pid is None else pid
        self._day: date | None = None
        self._sent = 0

    def publish(self, message: Message, *, now: datetime) -> Delivery:
        """Send one page, or record why it did not go. Never raises."""
        day = now.astimezone(MARKET_TZ).date()
        if day != self._day:
            self._day = day
            self._sent = 0

        leak = self._leak(message)
        if leak is not None:
            # A secret in a page body would reach a phone and a notification history.
            # Refusing is the only safe answer, and the record names the field rather
            # than repeating what it held.
            return self._record(message, REFUSED, now, detail=leak, redact=True)
        if self._sent >= self._cap:
            return self._record(message, CAP_REACHED, now)
        if self._transport is None:
            return self._record(message, POST_FAILED, now, detail="no transport")
        try:
            self._transport.send(message)
        except Exception as exc:  # noqa: BLE001 - a page must not take the daemon down
            return self._record(message, POST_FAILED, now, detail=type(exc).__name__)
        self._sent += 1
        return Delivery(True)

    # -- the record ------------------------------------------------------------

    def _record(
        self,
        message: Message,
        reason: str,
        now: datetime,
        *,
        detail: str | None = None,
        redact: bool = False,
    ) -> Delivery:
        """Write one undelivered page down, so it is never invisible.

        The body never lands here. It can carry whatever a producer put in it, and this
        file sits inside the directories the dashboard may read, so only the event, the
        reason, and the priority are kept. A refused message loses its title too, since
        the title is what the refusal objected to.
        """
        eastern = now.astimezone(MARKET_TZ)
        directory = self._paths.root / "reports" / "alerts" / f"date={eastern.date().isoformat()}"
        entry = {
            "at": eastern.isoformat(),
            "event": message.event,
            "reason": reason,
            "priority": message.priority,
        }
        if not redact:
            entry["title"] = message.title
        if detail is not None:
            entry["detail"] = detail
        stamp = eastern.strftime("%H%M%S%f")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{stamp}-{message.event}-{self._pid}.json"
            with open(path, "x", encoding="utf-8") as handle:
                json.dump(entry, handle, sort_keys=True)
                handle.write("\n")
        except OSError:
            # The record is the last line of defence and it just failed. There is
            # nowhere further to write, so the caller is told and the daemon lives.
            return Delivery(False, reason)
        return Delivery(False, reason)

    def _leak(self, message: Message) -> str | None:
        """The field carrying a secret, if any.

        The check is for the secret values themselves, not for the hostnames that
        usually surround them. ``hc-ping.com`` and ``ntfy.sh`` are public names, and a
        page mentioning either carries nothing. What must never reach a phone is the
        ping key and the ntfy topic, because either one lets a stranger write to the
        channel. Matching the host would refuse harmless text and still miss a key
        pasted on its own.
        """
        if not self._secrets:
            return None
        for field, value in (("title", message.title), ("body", message.body)):
            if any(secret in value for secret in self._secrets):
                return field
        return None


def undelivered(lake_root: Path | str, day: date) -> int:
    """How many pages never left the laptop on one day.

    Counted from the filesystem rather than queried, because the ordinary day has none
    and a SQL read over an empty glob raises rather than returning zero.
    """
    directory = LakePaths(Path(lake_root)).root / "reports" / "alerts" / f"date={day.isoformat()}"
    if not directory.is_dir():
        return 0
    return len(list(directory.glob("*.json")))


__all__ = [
    "CAP_REACHED",
    "DEFAULT_DAILY_CAP",
    "POST_FAILED",
    "REFUSED",
    "Delivery",
    "Message",
    "NtfyTransport",
    "Publisher",
    "Transport",
    "undelivered",
]
