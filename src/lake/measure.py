"""The day-one measurements.

This is a query module, not a capture job. It reads captured journal segments and
reports the first sessions' distributions the design's Sizing section and guard-constant
recalibration read. It never fetches and never writes. The journal's per-row timestamps
already carry everything here, so each measurement is a query over stored cycles.

The four measurements, each glossed at first use.

1. *Fetch latency*, reported as two distinct metrics, each with the open and the final
   fifteen minutes broken out. The *dispatch delay* is how far into its minute slot a
   cycle's fetch was dispatched: ``fetch_ts`` minus ``snap_ts``. ``snap_ts`` is the
   minute slot the cycle fired for, floored to the minute. ``fetch_ts`` is the loop's
   clock just before the request. The *request round-trip* is the request's true
   duration: ``fetch_end_ts`` minus ``fetch_ts``, where ``fetch_end_ts`` is when the
   response or the failure landed. Both matter to the one-minute budget the
   roster-ceiling claim needs measured. Each is reported as p50 and p99, the 50th and
   99th percentiles, across the whole session, then again for the open slot and for the
   final fifteen minutes. The open and the close are the two load spikes the design
   calls out. A row missing ``fetch_end_ts`` has no defined round-trip, so it is skipped
   and the skipped count is reported, never silently dropped.
2. *Chain contract counts and per-cycle sizing.* The contract count is the number of
   data rows in a chains cycle. Sizing is those counts distributed across cycles, plus
   the segments' bytes on disk per cycle. This is the ~10-25k-contracts-per-snapshot
   figure the design leaves as an estimate to measure.
3. *The close+5 same-day-expired-series answer.* Every SPY/QQQ session lists a
   same-day expiration. The question is whether the vendor still serves those series
   late in the session. This reads the last captured cycle and counts contracts whose
   OCC symbol encodes an expiration equal to the session date. The answer calibrates
   the option-close fill's membership guard. The OCC symbol is the Options Clearing
   Corporation's contract name; its characters 7 through 12 are the expiration YYMMDD.
4. *The OI-refresh moment.* Open interest, OI, is the count of contracts outstanding.
   Schwab loads a session's settled OI at some moment early in the next session. This
   finds the first cycle whose OI differs from the session's first cycle, and reports
   that slot, how many contracts changed, and whether the change looks atomic (a single
   settled state that then holds). Slice 1's measurement calibrates the OI view's
   quorum fraction and plateau count.

The module reads no wall clock and names no session time. The window boundaries for the
open and final-fifteen breakouts are either passed in by the caller, from the calendar,
or inferred from the data's own first and last slots.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

from lake import journal
from lake.journal import ROW_KIND_DATA, ROW_KIND_GAP

# The final-fifteen-minutes window. It matches the option-close trailing span the
# design breaks out, expressed as a duration so no session-time literal appears here.
FINAL_WINDOW = timedelta(minutes=15)

# Where the OCC symbol carries the expiration: characters 7 through 12 are YYMMDD, just
# past the six-character padded root.
_OCC_ROOT_WIDTH = 6
_OCC_DATE_WIDTH = 6


# -- reading captured cycles -------------------------------------------------


def _day_str(day: date | str) -> str:
    return day.isoformat() if isinstance(day, date) else str(day)


def read_surface_cycles(
    lake_root: Path | str, surface: str, ticker: str, day: date | str
) -> pa.Table:
    """Read every journal segment for one surface, ticker, and day into one table.

    Slice 1 captures through one-off runner invocations, each its own writer session
    and so its own segment. This concatenates them all. A day with no segments reads as
    an empty table in the surface's pinned schema.
    """
    schema = journal.schema_for(surface)
    directory = (
        Path(lake_root)
        / "journal"
        / f"date={_day_str(day)}"
        / f"surface={surface}"
        / f"ticker={ticker}"
    )
    if not directory.is_dir():
        return schema.empty_table()
    segments = sorted(directory.glob("seg-*.arrows"))
    tables = [journal.read_segment(path) for path in segments]
    if not tables:
        return schema.empty_table()
    return pa.concat_tables(tables)


# -- small numeric and symbol helpers ----------------------------------------


def _percentile(values: list[float], q: float) -> float | None:
    """The ``q``-quantile of ``values`` by linear interpolation, or ``None`` if empty.

    ``q`` is a fraction in ``[0, 1]``: 0.5 for p50, 0.99 for p99. No dependency beyond
    the standard library, so the query stays light.
    """
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)


def _occ_expiry(occ_symbol: str | None) -> date | None:
    """The expiration date an OCC symbol encodes, or ``None`` if it cannot be parsed."""
    if not occ_symbol:
        return None
    core = occ_symbol[_OCC_ROOT_WIDTH : _OCC_ROOT_WIDTH + _OCC_DATE_WIDTH]
    if len(core) != _OCC_DATE_WIDTH or not core.isdigit():
        return None
    year = 2000 + int(core[0:2])
    month = int(core[2:4])
    day = int(core[4:6])
    try:
        return date(year, month, day)
    except ValueError:
        return None


# -- the measurement result types -------------------------------------------


@dataclass(frozen=True)
class LatencyStats:
    """A fetch-latency distribution in seconds: count, p50, p99."""

    count: int
    p50: float | None
    p99: float | None


@dataclass(frozen=True)
class LatencyBreakout:
    """One latency metric across the session, broken out at the two load spikes."""

    overall: LatencyStats
    at_open: LatencyStats
    final_fifteen: LatencyStats


@dataclass(frozen=True)
class FetchLatency:
    """Both fetch-latency metrics: the dispatch delay and the request round-trip.

    ``dispatch`` is ``fetch_ts`` minus ``snap_ts``, how far into the slot the request
    started. ``round_trip`` is ``fetch_end_ts`` minus ``fetch_ts``, the request's true
    duration. ``round_trip_skipped`` counts cycles whose ``fetch_end_ts`` was null, so
    the round-trip was undefined there and the cycle was left out, never silently.
    """

    dispatch: LatencyBreakout
    round_trip: LatencyBreakout
    round_trip_skipped: int


@dataclass(frozen=True)
class SizingStats:
    """Chain contract counts across cycles, plus segment bytes per cycle."""

    cycles: int
    total_contract_rows: int
    contracts_min: int | None
    contracts_p50: float | None
    contracts_p99: float | None
    contracts_max: int | None
    segment_bytes: int
    bytes_per_cycle: float | None


@dataclass(frozen=True)
class SameDayExpiry:
    """Whether the last captured cycle still carried same-day-expired series."""

    cycle_snap_ts: str | None
    present: bool
    count: int


@dataclass(frozen=True)
class OiRefresh:
    """The intra-session OI-refresh moment, if one was observed.

    ``observed`` is false when there were too few cycles to detect a change, or the OI
    never moved that session. ``atomic`` is true when the refreshed cycle already held
    the session's settled OI on the contracts it shares with the final cycle.
    """

    refresh_slot: str | None
    changed_contracts: int
    atomic: bool
    observed: bool


@dataclass(frozen=True)
class DayOneMeasurements:
    """Every day-one measurement for one ticker-day."""

    ticker: str
    day: str
    cycles: int
    data_cycles: int
    gap_cycles: int
    latency: FetchLatency
    sizing: SizingStats
    same_day_expiry: SameDayExpiry
    oi_refresh: OiRefresh

    def render(self) -> str:
        """A human-readable measurement block."""
        lat = self.latency

        def _stat(stats: LatencyStats) -> str:
            if stats.count == 0:
                return "n/a"
            return f"p50={stats.p50:.3f}s p99={stats.p99:.3f}s (n={stats.count})"

        lines = [
            f"Day-one measurements: {self.ticker} {self.day}",
            f"  cycles:            {self.cycles} ({self.data_cycles} data, {self.gap_cycles} gap)",
            f"  dispatch delay:    {_stat(lat.dispatch.overall)}",
            f"    at open:         {_stat(lat.dispatch.at_open)}",
            f"    final 15 min:    {_stat(lat.dispatch.final_fifteen)}",
            f"  round-trip:        {_stat(lat.round_trip.overall)} "
            f"(skipped {lat.round_trip_skipped} without fetch_end_ts)",
            f"    at open:         {_stat(lat.round_trip.at_open)}",
            f"    final 15 min:    {_stat(lat.round_trip.final_fifteen)}",
            f"  contracts/cycle:   min={self.sizing.contracts_min} "
            f"p50={self.sizing.contracts_p50} p99={self.sizing.contracts_p99} "
            f"max={self.sizing.contracts_max}",
            f"  total rows:        {self.sizing.total_contract_rows}",
            f"  segment bytes:     {self.sizing.segment_bytes} "
            f"({self.sizing.bytes_per_cycle} per cycle)",
        ]
        if self.same_day_expiry.cycle_snap_ts is None:
            lines.append("  same-day series:   no cycle to inspect")
        else:
            lines.append(
                f"  same-day series:   {'present' if self.same_day_expiry.present else 'absent'} "
                f"({self.same_day_expiry.count}) in cycle {self.same_day_expiry.cycle_snap_ts}"
            )
        if self.oi_refresh.observed:
            lines.append(
                f"  OI refresh:        {self.oi_refresh.refresh_slot} "
                f"({self.oi_refresh.changed_contracts} contracts, "
                f"{'atomic' if self.oi_refresh.atomic else 'gradual'})"
            )
        else:
            lines.append("  OI refresh:        none observed this session")
        return "\n".join(lines)


# -- the measurements --------------------------------------------------------


def _oi_map(cycle_rows: list[dict]) -> dict[str, object]:
    """A contract-to-open-interest map for one cycle's data rows."""
    return {row["occ_symbol"]: row["open_interest"] for row in cycle_rows}


def measure_day(
    lake_root: Path | str,
    ticker: str,
    day: date | str,
    *,
    open_slot: datetime | None = None,
    close_slot: datetime | None = None,
) -> DayOneMeasurements:
    """Compute every day-one measurement for one ticker-day from its chains segments.

    ``open_slot`` and ``close_slot`` bound the open and final-fifteen breakouts. When a
    caller omits them they are inferred from the data's first and last slots, so the
    query stands alone without the calendar.
    """
    table = read_surface_cycles(lake_root, journal.CHAINS_SURFACE, ticker, day)
    rows = table.to_pylist()
    session_date = day if isinstance(day, date) else date.fromisoformat(str(day))

    data_rows = [row for row in rows if row["row_kind"] == ROW_KIND_DATA]
    gap_slots = {row["snap_ts"] for row in rows if row["row_kind"] == ROW_KIND_GAP}

    cycles: dict[str, list[dict]] = {}
    for row in data_rows:
        cycles.setdefault(row["snap_ts"], []).append(row)
    parsed = {slot: datetime.fromisoformat(slot) for slot in cycles}
    order = sorted(cycles, key=lambda slot: parsed[slot])

    latency = _fetch_latency(cycles, parsed, order, open_slot, close_slot)
    sizing = _sizing(lake_root, ticker, day, cycles, order)
    same_day = _same_day_expiry(cycles, order, session_date)
    oi_refresh = _oi_refresh(cycles, order)

    data_cycle_slots = set(order)
    total_slots = data_cycle_slots | gap_slots
    return DayOneMeasurements(
        ticker=ticker,
        day=_day_str(day),
        cycles=len(total_slots),
        data_cycles=len(data_cycle_slots),
        gap_cycles=len(gap_slots - data_cycle_slots),
        latency=latency,
        sizing=sizing,
        same_day_expiry=same_day,
        oi_refresh=oi_refresh,
    )


def _dispatch_value(cycle_rows: list[dict], snap: datetime) -> float | None:
    """A cycle's dispatch delay in seconds: ``fetch_ts`` minus ``snap_ts``."""
    fetch_ts = cycle_rows[0]["fetch_ts"]
    if fetch_ts is None:
        return None
    return (datetime.fromisoformat(fetch_ts) - snap).total_seconds()


def _round_trip_value(cycle_rows: list[dict], snap: datetime) -> float | None:
    """A cycle's request round-trip in seconds: ``fetch_end_ts`` minus ``fetch_ts``.

    Undefined, and so ``None``, when either stamp is missing on the cycle. ``snap`` is
    unused here; the signature matches ``_dispatch_value`` so both feed ``_breakout``.
    """
    row = cycle_rows[0]
    fetch_ts = row["fetch_ts"]
    fetch_end_ts = row["fetch_end_ts"]
    if fetch_ts is None or fetch_end_ts is None:
        return None
    start = datetime.fromisoformat(fetch_ts)
    end = datetime.fromisoformat(fetch_end_ts)
    return (end - start).total_seconds()


def _stats(values: list[float]) -> LatencyStats:
    return LatencyStats(
        count=len(values), p50=_percentile(values, 0.5), p99=_percentile(values, 0.99)
    )


def _breakout(
    cycles: dict[str, list[dict]],
    parsed: dict[str, datetime],
    order: list[str],
    open_at: datetime,
    close_at: datetime,
    value_fn,
) -> tuple[LatencyBreakout, int]:
    """One metric's overall/open/final breakout, plus the count of skipped cycles.

    A cycle whose ``value_fn`` returns ``None`` is left out and counted as skipped, so
    an undefined round-trip is reported rather than silently absorbed.
    """
    final_start = close_at - FINAL_WINDOW
    overall: list[float] = []
    at_open: list[float] = []
    final: list[float] = []
    skipped = 0
    for slot in order:
        value = value_fn(cycles[slot], parsed[slot])
        if value is None:
            skipped += 1
            continue
        overall.append(value)
        if parsed[slot] == open_at:
            at_open.append(value)
        if parsed[slot] >= final_start:
            final.append(value)
    return LatencyBreakout(_stats(overall), _stats(at_open), _stats(final)), skipped


def _fetch_latency(
    cycles: dict[str, list[dict]],
    parsed: dict[str, datetime],
    order: list[str],
    open_slot: datetime | None,
    close_slot: datetime | None,
) -> FetchLatency:
    if not order:
        zero = LatencyStats(0, None, None)
        empty = LatencyBreakout(zero, zero, zero)
        return FetchLatency(dispatch=empty, round_trip=empty, round_trip_skipped=0)
    open_at = open_slot if open_slot is not None else parsed[order[0]]
    close_at = close_slot if close_slot is not None else parsed[order[-1]]
    dispatch, _ = _breakout(cycles, parsed, order, open_at, close_at, _dispatch_value)
    round_trip, skipped = _breakout(cycles, parsed, order, open_at, close_at, _round_trip_value)
    return FetchLatency(dispatch=dispatch, round_trip=round_trip, round_trip_skipped=skipped)


def _sizing(
    lake_root: Path | str,
    ticker: str,
    day: date | str,
    cycles: dict[str, list[dict]],
    order: list[str],
) -> SizingStats:
    counts = [len(cycles[slot]) for slot in order]
    directory = (
        Path(lake_root)
        / "journal"
        / f"date={_day_str(day)}"
        / f"surface={journal.CHAINS_SURFACE}"
        / f"ticker={ticker}"
    )
    segment_bytes = 0
    if directory.is_dir():
        segment_bytes = sum(path.stat().st_size for path in directory.glob("seg-*.arrows"))
    float_counts = [float(count) for count in counts]
    return SizingStats(
        cycles=len(order),
        total_contract_rows=sum(counts),
        contracts_min=min(counts) if counts else None,
        contracts_p50=_percentile(float_counts, 0.5),
        contracts_p99=_percentile(float_counts, 0.99),
        contracts_max=max(counts) if counts else None,
        segment_bytes=segment_bytes,
        bytes_per_cycle=segment_bytes / len(order) if order else None,
    )


def _same_day_expiry(
    cycles: dict[str, list[dict]], order: list[str], session_date: date
) -> SameDayExpiry:
    if not order:
        return SameDayExpiry(cycle_snap_ts=None, present=False, count=0)
    last_slot = order[-1]
    count = sum(1 for row in cycles[last_slot] if _occ_expiry(row["occ_symbol"]) == session_date)
    return SameDayExpiry(cycle_snap_ts=last_slot, present=count > 0, count=count)


def _oi_refresh(cycles: dict[str, list[dict]], order: list[str]) -> OiRefresh:
    if len(order) < 2:
        return OiRefresh(refresh_slot=None, changed_contracts=0, atomic=False, observed=False)
    baseline = _oi_map(cycles[order[0]])
    final_map = _oi_map(cycles[order[-1]])
    for slot in order[1:]:
        current = _oi_map(cycles[slot])
        common = baseline.keys() & current.keys()
        changed = [key for key in common if current[key] != baseline[key]]
        if changed:
            shared = current.keys() & final_map.keys()
            atomic = all(current[key] == final_map[key] for key in shared)
            return OiRefresh(
                refresh_slot=slot,
                changed_contracts=len(changed),
                atomic=atomic,
                observed=True,
            )
    return OiRefresh(refresh_slot=None, changed_contracts=0, atomic=False, observed=False)


# -- the command-line entry --------------------------------------------------


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.measure",
        description="Report the day-one measurements for one ticker-day.",
    )
    parser.add_argument("ticker", help="The ticker to measure, like SPY.")
    parser.add_argument("day", help="The session date, ISO like 2026-08-24.")
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    parser.add_argument("--lake-root", help="Read from this lake root instead of the config's.")
    return parser


def main(argv=None) -> int:
    """The ``python -m lake.measure`` entry. Returns a process exit code."""
    args = _build_parser().parse_args(argv)
    if args.lake_root is not None:
        lake_root = Path(args.lake_root)
    else:
        from lake.config import load_config

        lake_root = load_config(args.config).lake_root
    measurements = measure_day(lake_root, args.ticker, date.fromisoformat(args.day))
    print(measurements.render())
    return 0


__all__ = [
    "FINAL_WINDOW",
    "DayOneMeasurements",
    "FetchLatency",
    "LatencyBreakout",
    "LatencyStats",
    "OiRefresh",
    "SameDayExpiry",
    "SizingStats",
    "main",
    "measure_day",
    "read_surface_cycles",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
