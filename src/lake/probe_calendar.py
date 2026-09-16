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
from collections.abc import Mapping, Sequence
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
    rather than a bare exception name, because three states reach it.

    1. A vendor the probe could not reach.
    2. A reply the probe could not read. ``_batch_of`` classifies those and names which
       shape each one was.
    3. A batch carrying no readable stamp.

    ``refused`` counts the stamps the epoch transform refuses, so a partly unreadable
    batch says so even on a day that reports no problem.

    ``latest`` is the newest vendor stamp among the trading symbols, ``asked`` how many
    symbols the request named, and ``answered`` how many of those the vendor came back
    about. Those three are what the page reports in place of the names. ``answered`` is
    carried rather than derived, because two of the three counts that sum to it,
    ``readable`` and ``absent``, do not survive into this result.

    ``run_probe`` fills all three beside ``trading``, so a result it built describes
    itself. A caller building one by hand owes the same, and owes it for all three rather
    than for the stamp alone. A missing stamp raises where the body reads it, which is
    loud. Missing counts do not, and the page then says one symbol quoted out of zero
    answered.
    """

    day: date
    checked: bool
    trading: tuple[str, ...] = ()
    problem: str | None = None
    refused: int = 0
    latest: datetime | None = None
    asked: int = 0
    answered: int = 0

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

    ``latest`` is the newest of the stamps behind ``trading``, and it is not one of the
    counts. It takes the maximum rather than the last one seen, because the maximum is the
    only reading that does not depend on the order the batch is walked. It tracks only the
    stamps that counted as trading, so a vendor clock running ahead sends a stamp that
    reads fine, belongs to no session today, and never reaches the page.
    """

    trading: tuple[str, ...] = ()
    readable: int = 0
    refused: int = 0
    absent: int = 0
    latest: datetime | None = None


def read_batch(quotes: Mapping[str, object], day: date) -> Reading:
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
    latest: datetime | None = None
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
            if latest is None or stamp > latest:
                latest = stamp
    return Reading(tuple(trading), readable=readable, refused=refused, absent=absent, latest=latest)


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


def _batch_of(
    response: object, symbols: Sequence[str]
) -> tuple[dict[str, object] | None, str | None]:
    """The quotes the probe asked about inside one vendor reply, or the problem stopping it.

    Exactly one of the two comes back. A batch means the reader can run. A problem means
    it cannot, and the probe reports that problem the same way it reports a vendor it
    could not reach.

    The vendor answers with a ``VendorResponse`` of ``status``, ``body`` and ``headers``,
    and the reader wants the quotes inside it. Every other caller unwraps before reading.
    ``record.py``, ``capture.py`` and ``onboard.py`` read ``response.body``, and the
    Sunday canary reads ``reply.status``. The probe read neither, so the reader was handed
    the whole reply and asked it for symbols. That raised, which costs the answer and the
    healthchecks ping together.

    Six shapes stop here rather than reaching the reader. Each gets its own line, because
    they send the operator to different places.

    1. A reply with no ``status`` at all, which is not a vendor reply.
    2. A ``status`` that is not a whole number. A ``bool`` is a whole number in Python, so
       it gets past this and the range test refuses it instead, which is still a problem
       and still not a page.
    3. A status that is not a success. Schwab reports a dead token and a throttle as a
       status on a returned reply rather than as a raise, so a probe that never looks
       reads a dead token as the calendar being right. The Sunday canary checks the
       status for the same reason.
    4. A body that is not a mapping of symbol to envelope.
    5. A body naming none of the symbols asked for, which says the request and the roster
       disagree. An error payload like ``{"errors": [...]}`` is a mapping, so it arrives
       here rather than at the check above.
    6. A body naming some of them and carrying a quote envelope for none, which says the
       payload changed shape.

    What survives is narrowed to the symbols the probe asked about, and that narrowing is
    the step it was missing. It knew its roster and threw the list away, so it read
    whatever came back. Three things went wrong there.

    1. A reply naming none of the roster read exactly like a reply naming all of it with
       every stamp absent, and an absent stamp is deliberately a quiet day.
    2. A symbol nobody asked about could set ``trading`` and fire the page.
    3. Schwab puts unresolvable symbols in an ``errors`` block beside the quotes, so that
       block counted as one more symbol the vendor stayed quiet about, while the symbol it
       was really about counted as nothing at all.

    An envelope is a ``dict``, which is what ``_quote_time`` reads and what parsed JSON
    produces. What the stamp inside one says is the reader's question rather than this
    one, so an empty envelope answers here and reads as an absent stamp there. Keeping
    those two questions apart is what holds this above the reader. Teaching ``read_batch``
    that a garbage envelope is a refusal would reclassify an absent stamp, and a vendor
    staying quiet about one symbol is the ordinary case.

    Nothing here raises on any shape a reply can take, and the batch it hands back is
    keyed by the roster, so the reader cannot raise on a key either. A raise costs the
    ping as well as the answer, and the check going silent is what says the probe stopped
    running.
    """
    unset = object()
    status = getattr(response, "status", unset)
    if status is unset:
        return None, f"not a vendor reply: {type(response).__name__}"
    if not isinstance(status, int):
        return None, f"unreadable vendor status: {type(status).__name__}"
    # A non-2xx is a fetch failure, the same rule the capture primitive and the Sunday
    # canary hold. A 401 is the dead-token shape that arrives as a status, not a raise.
    if not 200 <= status < 300:
        return None, f"vendor returned http {status}"
    body = getattr(response, "body", None)
    if not isinstance(body, Mapping):
        return None, f"unreadable vendor body: {type(body).__name__}"
    named = {symbol: body[symbol] for symbol in symbols if symbol in body}
    if not named:
        return None, f"vendor named none of the {len(symbols)} symbols asked for"
    if not any(isinstance(envelope, dict) for envelope in named.values()):
        return None, f"no quote envelope for any of the {len(named)} symbols the vendor named"
    return named, None


def run_probe(*, calendar, clock, symbols: Sequence[str], fetch) -> ProbeResult:
    """Ask the vendor whether a day the calendar calls closed is really closed.

    On a session day nothing is asked. The calendar and the daemon already agree, and a
    vendor call would buy nothing.

    A vendor that cannot be reached is a problem, not a page. The probe exists to catch
    a calendar that is wrong, and an unreachable vendor is no evidence either way. Its
    own dead-man check notices a probe that stops running.

    A batch that came back and could not be read leaves the probe knowing just as
    little. It reports a problem the same way, and it pages no more than an unreachable
    vendor does. A reply the probe cannot open at all is the same state one step earlier,
    and ``_batch_of`` names which shape it was.

    A roster with no symbols in it is that state earlier again. Retiring the last ticker
    is a real thing to do, and the capture cycle skips its own quote request for it. A
    probe with nothing to ask cannot answer, so it says so rather than asking the vendor
    and then blaming the vendor for the empty list it was handed.
    """
    now = clock.now().astimezone(MARKET_TZ)
    day = now.date()
    if calendar.is_session(day):
        return ProbeResult(day, checked=False)
    if not symbols:
        return ProbeResult(day, checked=True, problem="no symbols to ask about")
    try:
        response = fetch(list(symbols))
    except Exception as exc:  # noqa: BLE001 - an unreachable vendor must not page
        return ProbeResult(day, checked=True, problem=f"vendor unreachable: {type(exc).__name__}")
    quotes, unreadable = _batch_of(response, symbols)
    if quotes is None:
        return ProbeResult(day, checked=True, problem=unreadable)
    reading = read_batch(quotes, day)
    return ProbeResult(
        day,
        checked=True,
        trading=reading.trading,
        problem=_unreadable_problem(reading),
        refused=reading.refused,
        latest=reading.latest,
        asked=len(symbols),
        answered=len(quotes),
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


def _page_body(result: ProbeResult) -> str:
    """What the page says on a day the market is trading and the calendar is not.

    Three counts and one stamp, no symbol names: how many quoted, how many the vendor
    answered about, how many were asked, and the newest stamp among those quoting.

    Size is the reason the names go. The old body joined the roster, and four-character
    symbols passed the design's under-1,000-byte rule at 156 of them and ntfy's own 4096
    at 672, where the POST comes back a 400 that ``NtfyTransport`` does not retry. So the
    page reporting a whole lost session was the one that would fail to send.
    ``PAGE_COLUMN_CAP`` in ``schema_drift`` carries both bounds and the arithmetic behind
    them. This body is the same size at any roster, so it needs no cap of its own.

    The names buy nothing anyway. A market that is open quotes all of them, so the list
    restates the roster, and the Quote sampler's row leaves its names out for that reason.

    The denominator is what the vendor answered rather than what was asked. ``_batch_of``
    narrows a reply to the symbols the vendor named and ``answered`` counts those. A reply
    naming three of a hundred and fifteen still reads as a batch rather than a problem, so
    ``3 of 3 answered`` says the market is open where ``3 of 115 asked`` reads like a
    glitch, and both are the same reply. The number asked rides beside it, because a wide
    gap between the two is worth going to look at.

    ``ET`` is a literal rather than ``%Z``, which renders ``EDT`` half the year. The
    design's body rules ask for the time in ET, and the test push in ``alert.py`` composes
    its stamp the same way. The stamp arrives as the UTC instant the shared transform
    returns, so it is converted here rather than only formatted.
    """
    when = result.latest.astimezone(MARKET_TZ).strftime("%H:%M:%S")
    return (
        f"{result.day.isoformat()}: {len(result.trading)} of {result.answered} answered "
        f"quoting today, {result.asked} asked, latest {when} ET. "
        "The daemon is idle. Check the Now panel."
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
                body=_page_body(result),
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
