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

from lake.alert import Message
from lake.calendar import MARKET_TZ
from lake.runner import PING_FAILURES
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
    """What the probe found, and whether it pages."""

    day: date
    checked: bool
    trading: tuple[str, ...] = ()
    problem: str | None = None

    @property
    def pages(self) -> bool:
        """Only a market found trading pages. An unreachable vendor does not."""
        return bool(self.trading)


def fresh_symbols(quotes: dict, day: date) -> tuple[str, ...]:
    """The symbols whose vendor timestamp falls on ``day`` in market time.

    Same-day freshness is the predicate, not a seconds threshold. The question is
    whether the market traded at all today, and a stamp from a prior session answers it
    as clearly as one from an hour ago. A seconds threshold would also have to be
    guessed, and the design pins none for this.
    """
    trading: list[str] = []
    for symbol, envelope in sorted(quotes.items()):
        stamp = _quote_time(envelope)
        if stamp is None:
            continue
        if stamp.astimezone(MARKET_TZ).date() == day:
            trading.append(symbol)
    return tuple(trading)


def _quote_time(envelope: object) -> datetime | None:
    """The vendor quote time inside one quote envelope, or ``None``."""
    if not isinstance(envelope, dict):
        return None
    quote = envelope.get("quote")
    if not isinstance(quote, dict):
        return None
    raw = quote.get("quoteTime")
    if raw is None:
        return None
    try:
        # Schwab reports this as epoch milliseconds.
        return datetime.fromtimestamp(int(raw) / 1000, tz=MARKET_TZ)
    except (TypeError, ValueError, OSError):
        return None


def run_probe(*, calendar, clock, symbols: Sequence[str], fetch) -> ProbeResult:
    """Ask the vendor whether a day the calendar calls closed is really closed.

    On a session day nothing is asked. The calendar and the daemon already agree, and a
    vendor call would buy nothing.

    A vendor that cannot be reached is a problem, not a page. The probe exists to catch
    a calendar that is wrong, and an unreachable vendor is no evidence either way. Its
    own dead-man check notices a probe that stops running.
    """
    now = clock.now().astimezone(MARKET_TZ)
    day = now.date()
    if calendar.is_session(day):
        return ProbeResult(day, checked=False)
    try:
        quotes = fetch(list(symbols))
    except Exception as exc:  # noqa: BLE001 - an unreachable vendor must not page
        return ProbeResult(day, checked=True, problem=type(exc).__name__)
    return ProbeResult(day, checked=True, trading=fresh_symbols(quotes, day))


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
        now=clock.now(),
    )


def report(result: ProbeResult, *, publisher, pinger, ping_url: str, now) -> int:
    """Feed the check, page if the market is open, and say what happened.

    The check is fed before the paging branch returns. It is fed on every answer, so
    its silence means the probe stopped running rather than that every day was fine, and
    a day that pages is exactly a day the probe did run.
    """
    try:
        pinger.ping(ping_url)
    except PING_FAILURES as exc:
        # A ping that does not land is what the check exists to notice. Losing the page
        # because of it would be the wrong trade.
        print(f"calendar probe: ping failed: {type(exc).__name__}", file=sys.stderr)

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

    status = SESSION_DAY_TAG if not result.checked else "calendar agrees"
    if result.problem is not None:
        status = f"vendor unreachable: {result.problem}"
    print(f"calendar probe: {status}")
    return 0


__all__ = [
    "PAGE_TITLE",
    "SESSION_DAY_TAG",
    "ProbeResult",
    "fresh_symbols",
    "main",
    "report",
    "run_probe",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
