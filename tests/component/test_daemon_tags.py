"""The loop's two provenance tags, landed on real rows.

The daemon stamps ``close_tag`` and ``session_phase`` per minute and the cycle writes
them on every row. These prove the stamps reach the journal. They run real cycles over
real segments with the cassette-backed fake vendor and a manual clock. No network and no
wall clock are crossed, and every write lands on a throwaway lake. So the tier is
component: one subsystem, capture, over real files, with the vendor and clock still fake.

Two layers are pinned.

1. ``run_cycle`` with the two tags stamps every row on both surfaces: data rows,
   whole-chain and quote gap rows, and the absence markers inside a partial snapshot.
   Left unset, both stay null.
2. The loop, run across a short session that straddles the equity close, lands the
   close-tag hook's answer and the phase-derived ``session_phase`` on each slot's rows,
   gap rows included.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lake import capture, daemon, journal
from lake.calendar import MARKET_TZ
from lake.cassette import load_cassette
from lake.chain_plan import ChainPlan
from lake.session import SessionClock, SessionPhase
from lake.tickers import Roster
from lake.vendor import VendorError, VendorResponse
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

CASSETTES = Path(__file__).parents[1] / "cassettes"
CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE
ET = MARKET_TZ
SESSION = date(2026, 8, 24)
POST_EQUITY_CLOSE = SessionPhase.POST_EQUITY_CLOSE.value

# One open-ended window keeps each chain to the one request the cassettes record.
_ONE_WINDOW = ChainPlan(((0, None),))
_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)


def et(h: int, m: int, s: int = 0) -> datetime:
    return datetime(SESSION.year, SESSION.month, SESSION.day, h, m, s, tzinfo=ET)


def _both_options() -> Roster:
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": True, "chain_cadence": "1m"},
        }
    )


def _all_rows(result: capture.CycleResult) -> list[tuple[str, str, dict]]:
    """Every row the cycle wrote, as ``(surface, ticker, row)``."""
    rows: list[tuple[str, str, dict]] = []
    for segment in result.segments:
        for row in journal.read_segment(segment.path).to_pylist():
            rows.append((segment.surface, segment.ticker, row))
    return rows


def _assert_every_row_tagged(
    result: capture.CycleResult, *, close_tag: str | None, session_phase: str | None
) -> None:
    rows = _all_rows(result)
    assert rows, "the cycle wrote no rows"
    for surface, ticker, row in rows:
        assert row["close_tag"] == close_tag, (surface, ticker, row["row_kind"])
        assert row["session_phase"] == session_phase, (surface, ticker, row["row_kind"])


# -- 1. run_cycle stamps both surfaces ---------------------------------------------


def test_run_cycle_stamps_both_tags_on_every_data_row_of_both_surfaces(cassette_vendor, lake_root):
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        cassette_vendor,
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
        close_tag="option_close",
        session_phase=POST_EQUITY_CLOSE,
    )
    assert {(s.surface, s.ticker) for s in result.segments} == {
        (CHAINS, "SPY"),
        (CHAINS, "QQQ"),
        (QUOTES, "SPY"),
        (QUOTES, "QQQ"),
    }
    _assert_every_row_tagged(result, close_tag="option_close", session_phase=POST_EQUITY_CLOSE)


def test_run_cycle_leaves_both_tags_null_by_default(cassette_vendor, lake_root):
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        cassette_vendor,
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
    )
    _assert_every_row_tagged(result, close_tag=None, session_phase=None)


def test_run_cycle_stamps_a_whole_chain_gap_row(lake_root):
    # QQQ's chain returns a 500, so its chains segment is one whole-chain gap row. It
    # carries the cycle's tags like every data row beside it.
    vendor = CassetteVendor(load_cassette(CASSETTES / "chain_fail.json"))
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        _both_options(),
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
        close_tag="spot_close",
        session_phase=None,
    )
    gap = result.segment(CHAINS, "QQQ")
    assert gap.row_kind == journal.ROW_KIND_GAP
    _assert_every_row_tagged(result, close_tag="spot_close", session_phase=None)


def test_run_cycle_stamps_every_quote_gap_row(lake_root):
    # The batched quote request fails, so every roster ticker gets a quotes gap row.
    vendor = CassetteVendor(load_cassette(CASSETTES / "quote_fail.json"))
    roster = Roster.from_mapping(
        {"SPY": {"options": True, "chain_cadence": "1m"}, "QQQ": {"options": False}}
    )
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        roster,
        lake_root,
        pid=4242,
        plan=_ONE_WINDOW,
        close_tag="option_close",
        session_phase=POST_EQUITY_CLOSE,
    )
    for ticker in ("SPY", "QQQ"):
        assert result.segment(QUOTES, ticker).row_kind == journal.ROW_KIND_GAP
    _assert_every_row_tagged(result, close_tag="option_close", session_phase=POST_EQUITY_CLOSE)


class _TailFailsVendor:
    """A vendor whose first plan window succeeds and whose open tail raises.

    That yields a partial snapshot: the near window's contracts as data rows plus one
    absence-marker gap row for the tail, all in one chains segment.
    """

    def __init__(self, inner: CassetteVendor) -> None:
        self._inner = inner

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        if to_date is None:
            raise VendorError("tail timed out")
        # The cassette keys its chain on the open tail, so re-ask it that way.
        return self._inner.get_chain(symbol, from_date=from_date)

    def get_quotes(self, symbols) -> VendorResponse:
        return self._inner.get_quotes(symbols)

    def token_mint_time(self):
        return self._inner.token_mint_time()


def test_run_cycle_stamps_the_absence_markers_inside_a_partial_snapshot(cassette_vendor, lake_root):
    result = capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        _TailFailsVendor(cassette_vendor),
        _both_options(),
        lake_root,
        pid=4242,
        plan=ChainPlan(((0, 30), (31, None))),
        close_tag="option_close",
        session_phase=POST_EQUITY_CLOSE,
    )
    spy = result.segment(CHAINS, "SPY")
    assert spy.row_kind == journal.ROW_KIND_DATA
    rows = journal.read_segment(spy.path).to_pylist()
    kinds = {row["row_kind"] for row in rows}
    assert kinds == {journal.ROW_KIND_DATA, journal.ROW_KIND_GAP}
    markers = [row for row in rows if row["row_kind"] == journal.ROW_KIND_GAP]
    assert len(markers) == 1 and markers[0]["error_class"] == "vendor_error"
    _assert_every_row_tagged(result, close_tag="option_close", session_phase=POST_EQUITY_CLOSE)


# -- 2. the loop lands the tags per slot ---------------------------------------------


def test_loop_rows_carry_the_hooks_tag_and_the_phase_across_the_equity_close(lake_root):
    # A short session that straddles the equity close: open 15:58, close 16:00, so the
    # option close is 16:15 and the capture slots are 15:58 through 16:15, eighteen in all.
    # QQQ's chain fails on every cycle, so every slot also writes a gap row.
    calendar = FakeCalendar({SESSION: SessionTimes(open=et(15, 58), close=et(16, 0))})
    clock = ManualClock(start=et(15, 57, 30).astimezone(UTC))
    session_clock = SessionClock(clock, calendar)
    vendor = CassetteVendor(load_cassette(CASSETTES / "chain_fail.json"))
    roster = _both_options()

    tags = {et(16, 0): "spot_close", et(16, 15): "option_close"}
    results: list[tuple[datetime, capture.CycleResult]] = []
    hooks = daemon.DaemonHooks(
        close_tag_for=tags.get,
        on_cycle=lambda slot, result: results.append((slot, result)),
    )

    def cycle_runner(*, close_tag: str | None, session_phase: str | None):
        return capture.run_cycle(
            clock,
            vendor,
            roster,
            lake_root,
            pid=4242,
            plan=_ONE_WINDOW,
            close_tag=close_tag,
            session_phase=session_phase,
        )

    end = et(16, 16).astimezone(UTC)
    daemon.run_loop(
        session_clock,
        cycle_runner,
        clock=clock,
        hooks=hooks,
        should_continue=lambda: clock.now() < end,
    )

    assert [slot for slot, _ in results] == [et(15, 58) + timedelta(minutes=i) for i in range(18)]
    for slot, result in results:
        assert result.errors == ()
        assert result.segment(CHAINS, "QQQ").row_kind == journal.ROW_KIND_GAP
        # Each cycle's snap_ts is the slot it fired for.
        assert result.snap_ts == slot.astimezone(UTC)
        expected_phase = POST_EQUITY_CLOSE if slot > et(16, 0) else None
        _assert_every_row_tagged(result, close_tag=tags.get(slot), session_phase=expected_phase)

    # Spelled out at the three boundaries: the spot_close slot is tagged but still
    # synchronous; 16:01 is the first post-equity-close row; option_close is the last.
    by_slot = dict(results)
    row = _all_rows(by_slot[et(16, 0)])[0][2]
    assert (row["close_tag"], row["session_phase"]) == ("spot_close", None)
    row = _all_rows(by_slot[et(16, 1)])[0][2]
    assert (row["close_tag"], row["session_phase"]) == (None, POST_EQUITY_CLOSE)
    row = _all_rows(by_slot[et(16, 15)])[0][2]
    assert (row["close_tag"], row["session_phase"]) == ("option_close", POST_EQUITY_CLOSE)
