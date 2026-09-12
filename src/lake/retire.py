"""The retire command: stop capturing a ticker and record when it stopped.

Onboarding opens a capture span and adds the ticker to the roster. Retiring is the
inverse. It closes the ticker's open capture span at the retire instant, then either
turns the roster entry off or removes it. The span end is what lets the close+5 guard and
the startup walk still reason about the minutes the ticker owed while it was captured, and
what keeps a later rejoin's time away out of scope rather than marked as gaps.

Closing the span comes before the roster change on purpose. Capture only records inside an
open span, so once the span is closed the ticker is not captured again even if a crash
leaves it in the roster. The bad state, off or removed with the span still open, cannot
happen because the roster change is last.

Run it as ``python -m lake.retire TICKER``. By default it disables the ticker in place,
keeping the entry so it is easy to turn back on. Pass ``--remove`` to delete the entry
instead. Removing the last ticker is allowed and leaves an empty roster, which the daemon
accepts while still reporting.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from lake import capture_spans, security_master
from lake.calendar import MARKET_TZ
from lake.clock import Clock, SystemClock
from lake.config import input_errors_exit, load_config
from lake.manifest import record_partition
from lake.security_master import ID_TYPE_TICKER, SecurityMaster
from lake.tickers import load_tickers, remove_ticker, set_enabled, tickers_file_path

# The manifest ``source`` for the reference entry, matching onboarding.
REFERENCE_SOURCE = "reference"


class RetireError(Exception):
    """Raised when a retire cannot proceed, such as a ticker the master does not know."""


@dataclass(frozen=True)
class RetireReport:
    """What a retire did, for the sign-off block.

    ``span_end`` is ``None`` when the ticker was already retired, so no span was closed
    this run. ``removed`` says whether ``--remove`` deleted the entry rather than
    disabling it. ``already_retired`` says the span was already closed.
    """

    ticker: str
    instrument_id: int
    span_end: datetime | None
    removed: bool
    already_retired: bool
    tickers_path: Path
    spans_path: Path

    def render(self) -> str:
        lines = [f"Retired {self.ticker}", f"  instrument_id:   {self.instrument_id}"]
        if self.already_retired:
            lines.append("  span:            already closed (no change)")
        else:
            lines.append(f"  span closed at:  {self.span_end.isoformat()}")
        lines.append(f"  roster:          {'removed' if self.removed else 'disabled in place'}")
        lines.append(f"  tickers.yaml:    {self.tickers_path}")
        lines.append(f"  capture spans:   {self.spans_path}")
        return "\n".join(lines)


def retire(
    ticker: str,
    *,
    clock: Clock,
    lake_root: Path | str,
    tickers_path: str | Path | None = None,
    tickers_env: Mapping[str, str] | None = None,
    remove: bool = False,
) -> RetireReport:
    """Retire one ticker from the lake and return its sign-off report.

    Every dependency is injected, so this runs offline. The steps: resolve the ticker in
    the master, close its open capture span at ``now`` under the lake-root lock, then make
    the roster change. Re-running when the ticker is already retired closes no span and
    leaves the roster as it is, so the command is idempotent.
    """
    lake_root = Path(lake_root)
    now = clock.now()
    on = now.astimezone(MARKET_TZ).date()

    master_path = security_master.master_path(lake_root)
    if not master_path.exists():
        raise RetireError("no security master exists; nothing to retire")
    master = SecurityMaster.read(master_path)
    instrument_id = master.resolve(ticker, on, id_type=ID_TYPE_TICKER)
    if instrument_id is None:
        raise RetireError(f"ticker not in the security master: {ticker!r}")

    spans_file = capture_spans.spans_path(lake_root)
    if not spans_file.exists() and master.instrument_ids():
        # See the matching guard in onboard.py: the master already knows this ticker, so
        # a fresh, empty spans file here would read as "never had an open span," and
        # retire would silently record no history at all for the time it was captured.
        raise RetireError(
            "capture spans are missing but the security master already has instruments; "
            "run `python -m lake.seed_spans` before retiring"
        )
    spans = (
        capture_spans.CaptureSpans.read(spans_file)
        if spans_file.exists()
        else capture_spans.CaptureSpans()
    )

    already_retired = not spans.has_open_span(instrument_id)
    span_end: datetime | None = None
    if not already_retired:
        span_end = now
        spans.close_span(instrument_id, span_end)
        # Close the span first, under the lock, before the roster change below.
        from lake.lock import lake_lock

        with lake_lock(lake_root):
            spans.write(spans_file)
            record_partition(
                lake_root,
                capture_spans.SPANS_PARTITION,
                source=REFERENCE_SOURCE,
                rows=len(spans),
                fetched_at=now.isoformat(),
            )

    # The roster change is last. A ticker in the master but already off the roster is a
    # no-op here, which is what makes a re-run idempotent.
    resolved_tickers = tickers_file_path(tickers_path, tickers_env)
    in_roster = ticker in load_tickers(tickers_path, env=tickers_env).symbols
    if in_roster:
        if remove:
            remove_ticker(ticker, path=tickers_path, env=tickers_env)
        else:
            set_enabled(ticker, False, path=tickers_path, env=tickers_env)

    return RetireReport(
        ticker=ticker,
        instrument_id=instrument_id,
        span_end=span_end,
        removed=remove,
        already_retired=already_retired,
        tickers_path=resolved_tickers,
        spans_path=spans_file,
    )


def retire_from_config(
    ticker: str,
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    remove: bool = False,
) -> RetireReport:
    """Retire one ticker wired from the real config. This is the entry ``main`` calls.

    It loads the machine-local config for the lake root. No vendor is needed, because
    retiring fetches nothing.
    """
    config = load_config(config_path)
    return retire(
        ticker,
        clock=clock if clock is not None else SystemClock(),
        lake_root=config.lake_root,
        tickers_path=tickers_path,
        remove=remove,
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.retire",
        description="Retire one ticker: close its capture span and turn it off or remove it.",
    )
    parser.add_argument("ticker", help="The ticker to retire, like SPY.")
    parser.add_argument(
        "--remove",
        action="store_true",
        help="Delete the roster entry instead of disabling it in place.",
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument("--tickers", help="Path to tickers.yaml (defaults to the standard place).")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.retire`` entry. Returns a process exit code."""
    args = _build_parser().parse_args(argv)
    try:
        with input_errors_exit("retire"):
            report = retire_from_config(
                args.ticker,
                config_path=args.config,
                tickers_path=args.tickers,
                remove=args.remove,
            )
    except RetireError as exc:
        print(f"retire: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    print(report.render())
    return 0


__all__ = ["RetireError", "RetireReport", "main", "retire", "retire_from_config"]


if __name__ == "__main__":
    raise SystemExit(main())
