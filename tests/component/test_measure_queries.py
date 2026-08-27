"""The day-one measurements over a real fixture lake.

These lay down real Arrow IPC journal segments in the pinned chains schema, then run the
query module against them across one real boundary, the filesystem. The clock and vendor
never appear, since measurement is a pure read. So the tier is component.

The fixture is a small, known session: five data cycles and one gap cycle, with a
same-day-expired contract and an intra-session OI change, so every measurement has a
determinate answer.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from lake import journal, measure

DAY = "2026-08-27"

# One base instant per cycle, in UTC, the way capture stamps snap_ts. 13:30 UTC is the
# 09:30 ET open; 20:00 and 20:15 UTC are the 16:00 equity close and 16:15 option close.
_OPEN = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
_T1 = datetime(2026, 8, 27, 13, 31, tzinfo=UTC)
_T2 = datetime(2026, 8, 27, 13, 32, tzinfo=UTC)
_LATE = datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
_CLOSE = datetime(2026, 8, 27, 20, 15, tzinfo=UTC)
_GAP = datetime(2026, 8, 27, 13, 45, tzinfo=UTC)

# Two contracts: one expiring today (2026-08-27) and one a month out.
_SAME_DAY = "SPY   260827C00650000"
_FAR = "SPY   260918C00650000"

# A template row with every chains column present, so a filled row is complete.
_TEMPLATE = {name: None for name in journal.CHAINS_SCHEMA.names}


def _data_row(
    snap: datetime, fetch: datetime, occ: str, oi: int, fetch_end: datetime | None
) -> dict:
    row = dict(_TEMPLATE)
    row.update(
        snap_ts=snap.isoformat(),
        fetch_ts=fetch.isoformat(),
        fetch_end_ts=fetch_end.isoformat() if fetch_end is not None else None,
        vendor_quote_ts=snap.isoformat(),
        ticker="SPY",
        occ_symbol=occ,
        put_call="CALL",
        bid=4.2,
        ask=4.25,
        last=4.22,
        open_interest=oi,
        row_kind=journal.ROW_KIND_DATA,
        suspect=False,
        schema_version=journal.SCHEMA_VERSION,
    )
    return row


def _gap_row(snap: datetime) -> dict:
    row = dict(_TEMPLATE)
    row.update(
        snap_ts=snap.isoformat(),
        fetch_ts=None,
        ticker="SPY",
        row_kind=journal.ROW_KIND_GAP,
        error_class="daemon_dead",
        suspect=False,
        schema_version=journal.SCHEMA_VERSION,
    )
    return row


def _cycle(
    snap: datetime,
    latency_seconds: float,
    far_oi: int,
    round_trip_seconds: float | None = None,
) -> list[dict]:
    """One cycle: two contract rows fetched ``latency_seconds`` into the slot.

    ``round_trip_seconds`` sets ``fetch_end_ts`` at that many seconds past ``fetch_ts``.
    ``None`` leaves ``fetch_end_ts`` null, modelling a row with no request-end stamp.
    """
    fetch = snap + timedelta(seconds=latency_seconds)
    fetch_end = (
        None if round_trip_seconds is None else fetch + timedelta(seconds=round_trip_seconds)
    )
    return [
        _data_row(snap, fetch, _FAR, far_oi, fetch_end),
        _data_row(snap, fetch, _SAME_DAY, 500, fetch_end),
    ]


def _build_lake(fixture_lake):
    rows: list[dict] = []
    # OI baseline of 1000 at the open, then a settled 1200 from the second cycle on. The
    # same-day contract's OI never moves, so the change is one contract, held to close.
    # The dispatch delays are 0.5/0.3/0.4/0.8/0.9s; the round-trips 0.1/0.2/0.3/0.4/0.5s.
    rows += _cycle(_OPEN, 0.5, far_oi=1000, round_trip_seconds=0.10)
    rows += _cycle(_T1, 0.3, far_oi=1200, round_trip_seconds=0.20)
    rows += _cycle(_T2, 0.4, far_oi=1200, round_trip_seconds=0.30)
    rows += _cycle(_LATE, 0.8, far_oi=1200, round_trip_seconds=0.40)
    rows += _cycle(_CLOSE, 0.9, far_oi=1200, round_trip_seconds=0.50)
    rows.append(_gap_row(_GAP))
    table = pa.Table.from_pylist(rows, schema=journal.CHAINS_SCHEMA)
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, table, start_ts="20260827T133000000000", pid=4242
    )
    return fixture_lake.build()


def test_dispatch_delay_overall_and_broken_out(fixture_lake):
    root = _build_lake(fixture_lake)
    m = measure.measure_day(root, "SPY", DAY)

    # Five data cycles, one gap cycle.
    assert m.data_cycles == 5
    assert m.gap_cycles == 1
    assert m.cycles == 6

    # Dispatch delay (fetch_ts - snap_ts) across the five cycles: [0.3, 0.4, 0.5, 0.8, 0.9].
    dispatch = m.latency.dispatch
    assert dispatch.overall.count == 5
    assert dispatch.overall.p50 == 0.5

    # The open slot is a single cycle at 0.5s.
    assert dispatch.at_open.count == 1
    assert dispatch.at_open.p50 == 0.5

    # The final fifteen minutes (>= close - 15 min) hold the 20:00 and 20:15 cycles.
    assert dispatch.final_fifteen.count == 2
    assert dispatch.final_fifteen.p50 == pytest.approx(0.85)


def test_request_round_trip_overall_and_broken_out(fixture_lake):
    root = _build_lake(fixture_lake)
    m = measure.measure_day(root, "SPY", DAY)

    # Round-trip (fetch_end_ts - fetch_ts) across the five cycles: [0.1, 0.2, 0.3, 0.4, 0.5].
    # Every cycle carries a fetch_end_ts here, so none is skipped.
    assert m.latency.round_trip_skipped == 0
    rt = m.latency.round_trip
    assert rt.overall.count == 5
    assert rt.overall.p50 == pytest.approx(0.3)
    assert rt.at_open.count == 1
    assert rt.at_open.p50 == pytest.approx(0.1)
    # The 20:00 and 20:15 cycles round-tripped in 0.4 and 0.5 seconds.
    assert rt.final_fifteen.count == 2
    assert rt.final_fifteen.p50 == pytest.approx(0.45)


def test_round_trip_skips_rows_without_fetch_end(fixture_lake):
    # One cycle has a fetch_end_ts, the next does not. Dispatch is defined for both;
    # round-trip only for the first, and the second is counted as skipped, never dropped.
    rows = _cycle(_OPEN, 0.5, far_oi=1000, round_trip_seconds=0.2)
    rows += _cycle(_T1, 0.3, far_oi=1000, round_trip_seconds=None)
    table = pa.Table.from_pylist(rows, schema=journal.CHAINS_SCHEMA)
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, table, start_ts="20260827T133000000000", pid=7
    )
    root = fixture_lake.build()
    m = measure.measure_day(root, "SPY", DAY)

    assert m.latency.dispatch.overall.count == 2
    assert m.latency.round_trip.overall.count == 1
    assert m.latency.round_trip_skipped == 1


def test_contract_counts_and_sizing(fixture_lake):
    root = _build_lake(fixture_lake)
    m = measure.measure_day(root, "SPY", DAY)

    assert m.sizing.cycles == 5
    assert m.sizing.total_contract_rows == 10
    assert m.sizing.contracts_min == 2
    assert m.sizing.contracts_max == 2
    assert m.sizing.contracts_p50 == 2.0
    # The segment bytes are real bytes on disk, split across the five cycles.
    assert m.sizing.segment_bytes > 0
    assert m.sizing.bytes_per_cycle == m.sizing.segment_bytes / 5


def test_same_day_expired_series_present_in_last_cycle(fixture_lake):
    root = _build_lake(fixture_lake)
    m = measure.measure_day(root, "SPY", DAY)

    assert m.same_day_expiry.present is True
    assert m.same_day_expiry.count == 1
    assert m.same_day_expiry.cycle_snap_ts == _CLOSE.isoformat()


def test_oi_refresh_moment_is_the_first_changed_cycle(fixture_lake):
    root = _build_lake(fixture_lake)
    m = measure.measure_day(root, "SPY", DAY)

    assert m.oi_refresh.observed is True
    assert m.oi_refresh.refresh_slot == _T1.isoformat()
    assert m.oi_refresh.changed_contracts == 1
    # The refreshed OI holds to the close, so the refresh looks atomic.
    assert m.oi_refresh.atomic is True


def test_explicit_window_bounds_override_inference(fixture_lake):
    root = _build_lake(fixture_lake)
    # Pin the final window to start at 13:30, so every cycle counts as "final".
    m = measure.measure_day(
        root, "SPY", DAY, open_slot=_OPEN, close_slot=_OPEN + timedelta(minutes=14)
    )
    assert m.latency.dispatch.final_fifteen.count == 5
    assert m.latency.round_trip.final_fifteen.count == 5


def test_missing_ticker_day_reads_empty(fixture_lake):
    root = fixture_lake.build()
    m = measure.measure_day(root, "SPY", DAY)
    assert m.cycles == 0
    assert m.latency.dispatch.overall.count == 0
    assert m.latency.round_trip.overall.count == 0
    assert m.latency.round_trip_skipped == 0
    assert m.sizing.total_contract_rows == 0
    assert m.same_day_expiry.cycle_snap_ts is None
    assert m.oi_refresh.observed is False


def test_reads_across_multiple_segments(fixture_lake):
    # Two writer sessions, two segments, one ticker-day. measure concatenates them.
    first = pa.Table.from_pylist(
        _cycle(_OPEN, 0.5, 1000, round_trip_seconds=0.2), schema=journal.CHAINS_SCHEMA
    )
    second = pa.Table.from_pylist(
        _cycle(_T1, 0.3, 1200, round_trip_seconds=0.3), schema=journal.CHAINS_SCHEMA
    )
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, first, start_ts="20260827T133000000000", pid=1
    )
    fixture_lake.with_journal_segment(
        "chains", "SPY", DAY, second, start_ts="20260827T133100000000", pid=2
    )
    root = fixture_lake.build()
    m = measure.measure_day(root, "SPY", DAY)
    assert m.data_cycles == 2
