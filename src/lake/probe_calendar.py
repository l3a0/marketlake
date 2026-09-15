"""The 09:35 says-closed-but-open probe.

The calendar is the daemon's only authority on whether a session exists. Every other
check agrees with it by construction: the loop does not run off-session, the watchdog
counts only session minutes, and compaction sweeps only session dates. So a day the
calendar wrongly calls closed is a whole session lost with nothing objecting, because
everything that could object is downstream of the same wrong answer.

This is the one check that asks someone else. At 09:35 on a weekday the calendar calls
closed, it asks the vendor for quotes and looks at their timestamps. A stamp from today
means the market is trading while the daemon sleeps, which pages.

It runs as a fixed-time launchd job rather than from inside the daemon, because the
daemon is exactly what is not running on the day this matters.

The probe asks about the whole roster in one batched request. That costs the same as
asking about one symbol, and it removes both the arbitrary choice of which ticker to
trust and the blind spot where the chosen one is halted.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import NamedTuple

from lake import journal
from lake.alert import Message
from lake.calendar import MARKET_TZ
from lake.runner import PING_FAILURES, escalate_ping_failure
from lake.schwab import DEFAULT_TOKEN_PATH

# What the probe reports, per the design's message table. A session the daemon slept
# through is the loudest thing here, because the samples are gone.
PAGE_TITLE = "Calendar says closed, market looks open"

# The tag on the ping for a day the calendar and the vendor agree about. The design
# words it this way, and it makes a session day distinguishable from a probe that never
# ran at all.
SESSION_DAY_TAG = "session day, probe n/a"


@dataclass(frozen=True)
class ProbeResult:
    """What the probe found, and whether it pages.

    ``problem`` says the probe learned nothing either way. It reads as a whole phrase
    rather than a bare exception name, because two states reach it.

    1. A vendor the probe could not reach.
    2. A batch carrying no readable stamp.

    ``refused`` counts the stamps the epoch transform refuses, so a partly unreadable
    batch says so even on a day that reports no problem.
    """

    day: date
    checked: bool
    trading: tuple[str, ...] = ()
    problem: str | None = None
    refused: int = 0

    @property
    def pages(self) -> bool:
        """Only a market found trading pages. An unreachable vendor does not."""
        return bool(self.trading)


class Reading(NamedTuple):
    """What one batch of vendor quotes said, and what it could not say.

    ``trading`` names the symbols stamped today. ``readable`` counts the stamps that
    converted at all, today's and prior sessions' alike. ``refused`` counts the stamps
    the shared epoch transform refuses, and ``absent`` the symbols the vendor sent no
    stamp for. The three counts account for the whole batch, so they sum to its size.
    """

    trading: tuple[str, ...] = ()
    readable: int = 0
    refused: int = 0
    absent: int = 0


def read_batch(quotes: dict, day: date) -> Reading:
    """Who the vendor's stamps say is trading on ``day``, and how many would not read.

    Same-day freshness is the predicate, not a seconds threshold. The question is
    whether the market traded at all today, and a stamp from a prior session answers it
    as clearly as one from an hour ago. A seconds threshold would also have to be
    guessed, and the design pins none for this.

    A refused stamp is no evidence of trading, the same as an absent one. Counting it as
    trading would page on every closed day a vendor sends junk, and a page that fires on
    ordinary holidays is a page nobody reads.

    It is counted rather than dropped. A batch reads as quiet when nothing in it was
    readable, and that looks exactly like a market that is genuinely shut. That session
    is what the probe exists to save.
    """
    trading: list[str] = []
    readable = 0
    refused = 0
    absent = 0
    for symbol, envelope in sorted(quotes.items()):
        try:
            stamp = _quote_time(envelope)
        except journal.UnfitEpochError:
            refused += 1
            continue
        if stamp is None:
            absent += 1
            continue
        readable += 1
        if stamp.astimezone(MARKET_TZ).date() == day:
            trading.append(symbol)
    return Reading(tuple(trading), readable=readable, refused=refused, absent=absent)


def _quote_time(envelope: object) -> datetime | None:
    """The vendor quote time inside one quote envelope, or ``None`` when it is absent.

    Absent means the vendor sent no stamp, whether that is no envelope, no quote, or no
    ``quoteTime`` inside it. A stamp that arrived and cannot be read raises
    ``journal.UnfitEpochError`` from the shared transform, and counting it is the
    caller's job.

    The two states must not swap places. Every caller reads ``None`` as not trading, and
    that is the right answer for a symbol the vendor stayed quiet about. Only a refusal
    is new information.

    The conversion itself is ``journal.epoch_ms_to_utc``, the same one the two capture
    surfaces call. Writing it a third time here is what let a vendor ``true`` through as
    a stamp one millisecond past the epoch, which reads as 1969 in market time. That is
    because ``int(True)`` is ``1`` and nothing raised.
    """
    if not isinstance(envelope, dict):
        return None
    quote = envelope.get("quote")
    if not isinstance(quote, dict):
        return None
    return journal.epoch_ms_to_utc(quote.get("quoteTime"))


def _unreadable_problem(reading: Reading) -> str | None:
    """The problem a batch with no readable stamp in it reports, or ``None``.

    A batch carrying at least one refused stamp and no readable stamp is no evidence
    either way, which is the state an unreachable vendor already leaves. It reports a
    problem and it does not page.

    A batch of nothing but absent stamps does not reach that state. The vendor saying
    nothing about a symbol is the ordinary case, and reporting a problem on it would
    fire on every quiet day.

    Both counts go in the line, because a refusal and a silence are different things to
    go and look at, and the refusal count alone would not say how big the batch was. The
    wording borrows ``journal.describe_unusable``, which reads ``2 unreadable`` ahead of
    its own parenthesised counts. The helper itself is not reused. It takes segment
    entries and the probe has none, so bending the probe into that shape to reach it
    would cost more than the shared wording is worth.
    """
    if reading.refused and not reading.readable:
        return f"no readable quote time ({reading.refused} unreadable, {reading.absent} absent)"
    return None


def run_probe(*, calendar, clock, symbols: Sequence[str], fetch) -> ProbeResult:
    """Ask the vendor whether a day the calendar calls closed is really closed.

    On a session day nothing is asked. The calendar and the daemon already agree, and a
    vendor call would buy nothing.

    A vendor that cannot be reached is a problem, not a page. The probe exists to catch
    a calendar that is wrong, and an unreachable vendor is no evidence either way. Its
    own dead-man check notices a probe that stops running.

    A batch that came back and could not be read leaves the probe knowing just as
    little. It reports a problem the same way, and it pages no more than an unreachable
    vendor does.
    """
    now = clock.now().astimezone(MARKET_TZ)
    day = now.date()
    if calendar.is_session(day):
        return ProbeResult(day, checked=False)
    try:
        quotes = fetch(list(symbols))
    except Exception as exc:  # noqa: BLE001 - an unreachable vendor must not page
        return ProbeResult(day, checked=True, problem=f"vendor unreachable: {type(exc).__name__}")
    reading = read_batch(quotes, day)
    return ProbeResult(
        day,
        checked=True,
        trading=reading.trading,
        problem=_unreadable_problem(reading),
        refused=reading.refused,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.probe_calendar`` entry. Returns a process exit code."""
    parser = argparse.ArgumentParser(prog="python -m lake.probe_calendar")
    parser.add_argument("--config", help="Path to config.yaml.")
    parser.add_argument("--tickers", help="Path to tickers.yaml.")
    parser.add_argument("--token", help="Path to token.json.")
    args = parser.parse_args(argv)

    from lake.alert import NtfyTransport, Publisher
    from lake.calendar import ExchangeCalendar
    from lake.clock import SystemClock
    from lake.config import input_errors_exit, load_config
    from lake.control_plane import CALENDAR_PROBE_SLUG
    from lake.runner import UrllibPinger
    from lake.schwab import SchwabVendor
    from lake.tickers import load_tickers

    with input_errors_exit("probe-calendar"):
        config = load_config(args.config)
        roster = load_tickers(args.tickers)

    clock = SystemClock()
    vendor = SchwabVendor.from_token(
        args.token if args.token is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    result = run_probe(
        calendar=ExchangeCalendar(),
        clock=clock,
        symbols=roster.symbols,
        fetch=vendor.get_quotes,
    )
    return report(
        result,
        publisher=Publisher(
            lake_root=config.lake_root,
            transport=NtfyTransport(config.ntfy_topic.reveal()),
            secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
        ),
        pinger=UrllibPinger(),
        ping_url=config.healthchecks_url(CALENDAR_PROBE_SLUG),
        slug=CALENDAR_PROBE_SLUG,
        now=clock.now(),
    )


def report(result: ProbeResult, *, publisher, pinger, ping_url: str, slug: str, now) -> int:
    """Feed the check, page if the market is open, and say what happened.

    The check is fed before the paging branch returns. It is fed on every answer, so
    its silence means the probe stopped running rather than that every day was fine, and
    a day that pages is exactly a day the probe did run.

    ``slug`` names the check the URL addresses, and it is required rather than derived,
    so the page a refused ping raises can never name a different check than the one that
    was pinged. The URL carries the ping key and never reaches a page.
    """
    try:
        pinger.ping(ping_url)
    except PING_FAILURES as exc:
        # A ping that does not land is what the check exists to notice. Losing the page
        # because of it would be the wrong trade.
        print(f"calendar probe: ping failed: {type(exc).__name__}", file=sys.stderr)
        # A refused ping is the other failure. It feeds no check, so nothing will ever
        # go silent to report it.
        escalate_ping_failure(exc, slug=slug, publisher=publisher, now=now)

    if result.pages:
        publisher.publish(
            Message(
                event="calendar_wrong",
                title=PAGE_TITLE,
                body=(
                    f"{result.day.isoformat()}: {', '.join(result.trading)} quoting today. "
                    "The daemon is idle. Check the Now panel."
                ),
                priority=5,
            ),
            now=now,
        )
        print(f"calendar probe: {PAGE_TITLE} ({len(result.trading)} symbols)", file=sys.stderr)
        return 1

    if result.problem is not None:
        status = result.problem
    elif not result.checked:
        status = SESSION_DAY_TAG
    elif result.refused:
        # A batch that read in part is not a problem. Dropping the count would hide a
        # vendor going bad one symbol at a time, until the day nothing in the batch
        # reads at all and the line above says so instead.
        status = f"calendar agrees, {result.refused} unreadable"
    else:
        status = "calendar agrees"
    print(f"calendar probe: {status}")
    return 0


__all__ = [
    "PAGE_TITLE",
    "SESSION_DAY_TAG",
    "ProbeResult",
    "Reading",
    "main",
    "read_batch",
    "report",
    "run_probe",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
