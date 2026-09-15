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

``python -m lake.alert --test-push`` is the hand run that exercises the channel. Every
page the daemon sends rides one assumption nothing else tests, that a priority-5 push
reaches a locked phone and interrupts it. The topic is typed by hand, and the ntfy app's
pass through each Focus mode is set by hand too, so either can be wrong. The way an
operator learns it is that a page they needed never arrived.

The message goes out through the same publisher every producer uses, at the same tier,
through the same cap, so nothing about the path is special. Two limits are worth stating
rather than leaving for someone to discover.

The cap is per process. ``Publisher`` holds its tally in memory, so a hand run in its own
short-lived process spends nothing the daemon's forty can see. What the hand run shares
with a page is the code, not the budget.

A zero exit means ntfy accepted the POST, which is less than delivery. An ntfy topic is
unauthenticated, so a mistyped one is accepted and read by nobody. Only the phone says
the channel works, which is why the command sends the operator to look at it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.calendar import MARKET_TZ
from lake.clock import Clock, SystemClock
from lake.paths import LakePaths

# The design's cap on pages a day. The forty-first is written down and never sent, so a
# storm cannot empty the phone's attention for the one page that matters.
DEFAULT_DAILY_CAP = 40

# The tag every lake-composed page carries, per the design's message table.
PAGE_TAG = "rotating_light"

# The page tier. The design pins two tiers, a page now at 5 and the report's digest at 2,
# and the reminder sits at 3 between them. The tag marks a page and nothing else, so it
# follows the priority rather than becoming a second field a producer could set wrong. A
# reminder and the nightly summary reach the phone carrying no tag, which is what lets the
# emoji name the producer at a glance.
PAGE_PRIORITY = 5

# How long one POST may take. Short, because a page that has not landed in five seconds
# is competing with the next minute's cycle.
POST_TIMEOUT = timedelta(seconds=5)

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
    priority: int = PAGE_PRIORITY


@dataclass(frozen=True)
class Delivery:
    """What became of one message.

    ``recorded`` separates a page that was written down from one that was lost twice.
    Both leave the phone silent, and only the second leaves nothing behind.
    """

    sent: bool
    reason: str | None = None
    recorded: bool = False


@runtime_checkable
class Transport(Protocol):
    """The POST itself. The real one talks to ntfy, a test records."""

    def send(self, message: Message) -> None: ...


class NtfyTransport:
    """The real POST to ntfy.

    The topic is the write credential for the channel, so it goes in the JSON body and
    never in the URL. A URL lands in a proxy log, a crash report, and anything that
    records a request line. It is held here and never put in a message, and a publisher
    refuses any page whose own text contains it.

    A timeout or a 5xx is retried once. A 4xx is not, because the request itself is
    wrong and sending it again changes nothing.

    Only a page carries the tag. The design gives the emoji one job, marking a message
    from the lake's own jobs as a page, so a reminder and the summary go out with no
    ``tags`` field at all.
    """

    def __init__(self, topic: str, *, host: str = "https://ntfy.sh") -> None:
        self._topic = topic
        self._url = host.rstrip("/")

    def send(self, message: Message) -> None:
        import json as _json
        import urllib.error
        import urllib.request  # lazy: only a real send reaches the network

        payload: dict[str, object] = {
            "topic": self._topic,
            "title": message.title,
            "message": message.body,
            "priority": message.priority,
        }
        if message.priority == PAGE_PRIORITY:
            payload["tags"] = [PAGE_TAG]
        body = _json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=POST_TIMEOUT.seconds):
                return
        except (urllib.error.HTTPError, TimeoutError, OSError) as exc:
            status = getattr(exc, "code", None)
            if status is not None and 400 <= status < 500:
                # The request itself is wrong. Sending it again changes nothing.
                raise
        with urllib.request.urlopen(request, timeout=POST_TIMEOUT.seconds):
            return


class Publisher:
    """Sends pages, caps the day, and writes down every one that did not go.

    The cap is keyed by the Eastern date and held in memory, so it resets when the date
    turns and also when the process does. A daemon restarted at noon gets a fresh forty.
    That is the wrong way round for a crash loop, and the right way round for the case
    the cap exists to stop, which is one runaway producer inside one incarnation. Making
    it survive a restart means a durable tally, which is a sink of its own.
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
        self._root = Path(lake_root)
        self._transport = transport
        # The values that must never reach a phone: the ping key and the ntfy topic.
        # Empty means a caller that holds no secrets, which refuses nothing.
        self._secrets = tuple(secret for secret in secrets if secret)
        self._cap = daily_cap
        self._pid = os.getpid() if pid is None else pid
        self._day: date | None = None
        self._sent = 0
        self._written = 0

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
        # One cycle raises several pages at one instant, and a slot's stamp carries no
        # sub-minute part, so the clock alone cannot name them apart. The sequence can,
        # and it makes the name unique without depending on the clock at all.
        self._written += 1
        stamp = f"{eastern.strftime('%H%M%S%f')}-{self._written:04d}"
        try:
            # `parents=True` from a missing lake root would create the lake itself. The
            # Sunday job decides whether to ping on `root.is_dir()`, and it re-reads that
            # on every retry, so a publisher that conjured the root would turn "lake root
            # missing" into a green check on the following attempt. A record is written
            # inside a lake that exists, or not at all.
            if not self._root.is_dir():
                raise FileNotFoundError(f"lake root missing: {self._root}")
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{stamp}-{message.event}-{self._pid}.json"
            with open(path, "x", encoding="utf-8") as handle:
                json.dump(entry, handle, sort_keys=True)
                handle.write("\n")
        except OSError as exc:
            # The record is the last line of defence and it just failed. Nothing further
            # can be written, so the one place left to say so is the daemon's own log.
            print(
                f"alert: {message.event} lost, and its record failed too: {type(exc).__name__}",
                file=sys.stderr,
            )
            return Delivery(False, reason, recorded=False)
        return Delivery(False, reason, recorded=True)

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


# -- the hand-run channel test ---------------------------------------------------------

# The event name and the title of the one message this command sends. One fixed shape
# rather than a line composed at the call site, so an operator learns to recognise it.
#
# The design's message table carries a row for this shape, under `Test push`. That table
# is what an operator reads the topic against, and the design treats a message matching no
# row as evidence the topic leaked. So a change to either literal here without the same
# change to the row turns a hand run into a false report of a leaked topic.
TEST_PUSH_EVENT = "test_push"
TEST_PUSH_TITLE = "Test push"


def _test_push_body(now: datetime) -> str:
    """What the one test message says.

    Private, and named with a leading underscore on purpose. A module-level name
    starting with ``test_`` is collected by pytest the moment a test imports it, and
    this one takes an argument, which pytest would then read as a fixture request.

    It says outright that nothing is wrong. The message arrives at the page tier, with a
    page's emoji, and possibly in the middle of the night, so a body that read like a
    real page would teach the operator to distrust the tier. The stamp is Eastern, the
    zone every other timestamp in the lake is written in, and it tells a push that just
    landed from one delivered late.
    """
    eastern = now.astimezone(MARKET_TZ)
    return (
        f"Hand-run channel test sent {eastern.strftime('%Y-%m-%d %H:%M:%S')} ET. "
        "Nothing is wrong. Seeing this on a locked phone is the evidence a page "
        "interrupts, so check that it broke through the Focus mode in use."
    )


def run_test_push(publisher: Publisher, *, now: datetime) -> int:
    """Send one page through the production publisher and say what came back.

    The exit code follows the ``Delivery`` rather than the process, because the publisher
    never raises and a bare success would mean only that nothing crashed. A page written
    down because it could not be sent left the phone just as silent as one that was lost,
    so both exit non-zero. Reporting a recorded page as a success would hand an operator
    a green result for a channel that does not work, which is the failure this command
    exists to catch.

    Zero means ntfy accepted the POST, which is short of delivery. The topic is
    unauthenticated, so a mistyped one is accepted and read by nobody, and no exit code
    can tell the two apart. The phone is the evidence, and the output says so.

    Each reason gets its own line, because they are not the same failure. A refusal and
    a reached cap both stop the message inside this module, so the channel was never
    contacted and nothing about the phone is in question. Only a failed POST says
    anything about the channel.

    Failures print to stderr. The command is meant to be run from an install script,
    where stdout is often redirected to a log and the operator reads the terminal.
    """
    message = Message(
        event=TEST_PUSH_EVENT,
        title=TEST_PUSH_TITLE,
        body=_test_push_body(now),
        priority=PAGE_PRIORITY,
    )
    delivery = publisher.publish(message, now=now)
    if delivery.sent:
        print(f"test-push: ntfy accepted the push at priority {message.priority}, the page tier.")
        print(
            "test-push: acceptance means the POST left this laptop. A topic is unauthenticated, "
            "so a mistyped one is accepted here and read by nobody."
        )
        print(
            "test-push: the phone is the only evidence. The page must arrive, break through the "
            "Focus mode in use, and carry the page emoji."
        )
        return 0
    if delivery.reason == REFUSED:
        print(
            "test-push: NOT sent. This module refused the message before any POST, so the "
            "channel was never contacted and the phone is not in question.",
            file=sys.stderr,
        )
        print(
            "test-push: a refusal means the message text carries a value from config.yaml. "
            "Check whether the ntfy topic is a word this body already uses.",
            file=sys.stderr,
        )
    elif delivery.reason == CAP_REACHED:
        print(
            "test-push: NOT sent. The daily cap was reached, so the channel was never "
            "contacted and the phone is not in question.",
            file=sys.stderr,
        )
    else:
        print(
            f"test-push: NOT sent: {delivery.reason}. The POST was attempted and did not land, "
            "so the channel did not carry this message.",
            file=sys.stderr,
        )
    if delivery.recorded:
        print(
            f"test-push: written down under reports/alerts/ as {message.event}.",
            file=sys.stderr,
        )
        print(
            "test-push: that record counts on the Now panel and in tonight's digest, the same "
            "as a real page that never sent.",
            file=sys.stderr,
        )
        return 1
    print(
        "test-push: writing it down failed too, so the message is lost twice and nothing on "
        "this machine records it.",
        file=sys.stderr,
    )
    return 1


def build_parser() -> argparse.ArgumentParser:
    """The ``python -m lake.alert`` arguments.

    ``--test-push`` is required rather than defaulted, so a bare invocation says what the
    entry is for instead of silently sending a page to a phone.

    ``allow_abbrev`` is off for the same reason. argparse accepts any unambiguous prefix
    by default, so ``--test`` would reach a phone, and ``--test`` is what someone reaches
    for when they mean a dry run. There are two flags here and no abbreviation worth
    keeping.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.alert",
        description="Send one page through the production publisher and report what came back.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--test-push",
        action="store_true",
        required=True,
        help="Send one page through the production publisher and report what came back.",
    )
    parser.add_argument("--config", help="Path to config.yaml.")
    return parser


def main(argv: Sequence[str] | None = None, *, clock: Clock | None = None) -> int:
    """The ``python -m lake.alert`` entry. Returns a process exit code.

    The transport is built here, not accepted. It POSTs to ntfy, which reaches a phone,
    so a ``main`` that accepted one let a test omit it and send for real. A test replaces
    ``NtfyTransport`` instead, which drives the rest of this wiring unchanged.

    The topic comes from the same config every other producer reads, never from the
    command line. A topic typed at the prompt would prove a channel nothing else uses,
    and the channel worth proving is the one the daemon will page on.

    ``clock`` stays injectable. A wall clock never reaches past this process.
    """
    args = build_parser().parse_args(argv)

    from lake.config import input_errors_exit, load_config

    with input_errors_exit("alert"):
        config = load_config(args.config)

    publisher = Publisher(
        lake_root=config.lake_root,
        transport=NtfyTransport(config.ntfy_topic.reveal()),
        # The values that must never reach a phone, checked against the page itself.
        secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
    )
    reader = SystemClock() if clock is None else clock
    return run_test_push(publisher, now=reader.now())


__all__ = [
    "CAP_REACHED",
    "DEFAULT_DAILY_CAP",
    "PAGE_PRIORITY",
    "POST_FAILED",
    "REFUSED",
    "TEST_PUSH_EVENT",
    "TEST_PUSH_TITLE",
    "Delivery",
    "PAGE_TAG",
    "POST_TIMEOUT",
    "Message",
    "NtfyTransport",
    "Publisher",
    "Transport",
    "build_parser",
    "main",
    "run_test_push",
    "undelivered",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
