"""The one-shot seed: build the capture-spans file from an existing master.

This is not the historical-data backfill the design rejects. It touches no market data.
Before capture spans existed, scope was one ``capture_start`` epoch per instrument, held
on the security master. This command converts that into the new shape: one open capture
span per instrument, starting at its ``capture_start``. It changes nothing else. The
master keeps its ``capture_start`` column during this expand phase, so a rollback to the
old code still finds what it expects. A later, separate change drops the column once the
spans file is proven in production.

Run it once, before onboarding or retiring any ticker on the new code. Every command that
writes to the master or the spans file refuses to run against a lake that has instruments
but no spans file yet, naming this command as the fix. That refusal is what makes seeding
a hard prerequisite rather than a convenience: skipping it and onboarding or retiring
straight away would silently read every existing instrument as having no capture history
at all.

The command is idempotent. If the spans file already exists, it does nothing and reports
that. Run twice by mistake, the second run is a no-op.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from lake import security_master
from lake.calendar import MARKET_TZ
from lake.capture_spans import SPANS_PARTITION, CaptureSpans, build_from_master, spans_path
from lake.clock import Clock, SystemClock
from lake.config import input_errors_exit, load_config
from lake.manifest import record_partition
from lake.security_master import SecurityMaster
from lake.tickers import load_tickers

# The manifest ``source`` for the spans file's reference entry, matching onboard and retire.
REFERENCE_SOURCE = "reference"


@dataclass(frozen=True)
class SeedReport:
    """What the seed run did, for the sign-off block."""

    already_seeded: bool
    instrument_count: int
    spans_path: Path

    def render(self) -> str:
        if self.already_seeded:
            return f"Capture spans already exist at {self.spans_path}. Nothing to do."
        return (
            f"Seeded {self.instrument_count} instrument(s) into {self.spans_path}, "
            "one open span each, starting at its capture_start."
        )


def seed_spans(
    *,
    clock: Clock,
    lake_root: Path | str,
    tickers_path: str | Path | None = None,
    tickers_env=None,
) -> SeedReport:
    """Seed the spans file from the master, once. Every dependency is injected.

    A master that does not exist yet has nothing to seed: an empty spans file is written,
    so downstream readers see a spans file present rather than absent, and the very
    first onboarding needs no seed run of its own.
    """
    lake_root = Path(lake_root)
    on = _market_date(clock)
    target = spans_path(lake_root)
    if target.exists():
        existing = CaptureSpans.read(target)
        return SeedReport(
            already_seeded=True,
            instrument_count=len(existing.instrument_ids()),
            spans_path=target,
        )

    master_file = security_master.master_path(lake_root)
    master = SecurityMaster.read(master_file) if master_file.exists() else SecurityMaster()
    try:
        roster = load_tickers(tickers_path, env=tickers_env)
    except Exception:  # noqa: BLE001 - a bad or absent roster still gets a spans file
        # A roster that will not load must not block the seed run, since it is the
        # reference file's own repair. ``None`` tells ``build_from_master`` the roster
        # is unknown, not that it names nobody, so it opens a span for every instrument
        # rather than skipping all of them.
        roster = None
    roster_options = None if roster is None else {entry.ticker: entry.options for entry in roster}

    spans = build_from_master(master, roster_options, on)

    # Local to keep this module free of the lock unless it writes, the same reason
    # onboard.py and retire.py import it here rather than at module scope.
    from lake.lock import lake_lock

    with lake_lock(lake_root):
        spans.write(target)
        record_partition(
            lake_root,
            SPANS_PARTITION,
            source=REFERENCE_SOURCE,
            rows=len(spans),
            fetched_at=clock.now().isoformat(),
        )
    return SeedReport(
        already_seeded=False,
        instrument_count=len(spans.instrument_ids()),
        spans_path=target,
    )


def _market_date(clock: Clock) -> date:
    return clock.now().astimezone(MARKET_TZ).date()


def seed_spans_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
) -> SeedReport:
    """Seed wired from the real config. This is the entry ``main`` calls."""
    config = load_config(config_path)
    return seed_spans(
        clock=clock if clock is not None else SystemClock(),
        lake_root=config.lake_root,
        tickers_path=tickers_path,
    )


def _build_parser():
    import argparse

    return argparse.ArgumentParser(
        prog="python -m lake.seed_spans",
        description=(
            "Seed the capture-spans file from the existing security master, once, "
            "before onboarding or retiring any ticker on the new code."
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.seed_spans`` entry. Returns a process exit code."""
    _build_parser().parse_args(argv)
    with input_errors_exit("seed_spans"):
        report = seed_spans_from_config()
    print(report.render())
    return 0


__all__ = ["SeedReport", "seed_spans", "seed_spans_from_config", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
