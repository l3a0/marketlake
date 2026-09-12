"""The close+5 fill: the producer the guard calls, over real journal segments.

The guard decides who is owed a fill and refuses past close+5. This file covers what it
calls once it has decided, and the wiring that hands it that callable at all. Each case
runs the real ``capture.fill_option_close`` with a programmable fake vendor, a manual
clock, and a small injected plan, writing into a throwaway lake. No network and no wall
clock are crossed, so the tier is component: capture and the guard over real files, with
the vendor and the clock still fake.

Nine claims are covered.

1. The landed segment carries the option close in ``snap_ts`` and the fetch minute in
   ``fetch_ts``. Both matter, and collapsing them loses one of the two.
2. The fill goes through the chunk plan, one request per window, rather than one
   unchunked whole-chain request. A whole-chain request fails on the biggest chains,
   which are the ones most worth rescuing.
3. A window that failed rides the fill as an absence marker inside the same snapshot,
   so the fill still lands what the other windows returned.
4. A fill where every window failed writes no row at all, leaving the day's existing gap
   row to stand for the minute.
5. A fill whose windows answered 200 with no contracts writes no row either. That is a
   successful fetch that captured nothing, and landing it would leave a zero-row segment
   standing for a close nobody captured.
6. A fill that gave up a window says so in the outcome, even when the baseline is blind
   to the same window and the expiration comparison comes out empty.
7. The membership baseline is the day's own last cycle. A session that captured nothing
   is baseline-less rather than short by every series that expired earlier.
8. The landed row is the shape an ordinary cycle writes, column for column, apart from
   the coordinates and tags the fill owns.
9. The daemon's production wiring really passes the producer, and a guard the daemon
   built lands the close.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, close_guard, daemon, journal
from lake.capture_spans import CaptureSpans, spans_path
from lake.chain_plan import ChainPlan
from lake.config import GuardConstants
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.security_master import SecurityMaster, master_path
from lake.session import OPTION_CLOSE, SessionClock
from lake.tickers import Roster
from lake.vendor import VendorResponse
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.pinger import FakePinger
from tests.support.transport import FakeTransport

CHAINS = journal.CHAINS_SURFACE

WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)

# The option close this session, and the minute the fill fetches in. Five minutes apart,
# which is the whole reason the two timestamps stay separate on the row.
CLOSE = et(2026, 9, 2, 16, 15)
FILL_MINUTE = et(2026, 9, 2, 16, 18)

# Two windows: a near-term closed range and the open tail. The smallest plan that can tell
# a chunked fetch from one whole-chain request.
TWO_WINDOWS = ChainPlan(((0, 9), (10, None)))
NEAR = (DAY.isoformat(), (DAY + timedelta(days=9)).isoformat())
TAIL = ((DAY + timedelta(days=10)).isoformat(), None)

# A 502 the way Schwab's gateway rejects an over-large chain body.
TOO_BIG = VendorResponse(status=502, body={"errorcode": "protocol.http.TooBigBody"})


def _contract(exp_iso: str, put_call: str, *, bid: float) -> dict:
    """One synthetic contract, enough fields for the calibrated row builder to read."""
    letter = "C" if put_call == "CALL" else "P"
    return {
        "symbol": f"SPY   {exp_iso.replace('-', '')}{letter}00650000",
        "putCall": put_call,
        "strikePrice": 650.0,
        "expirationDate": f"{exp_iso}T20:00:00.000+00:00",
        "quoteTimeInLong": 1787000099000,
        "bid": bid,
        "openInterest": 100,
    }


def _chain_body(expirations: list[str], *, underlying_price: float = 650.0) -> dict:
    """A chain body carrying one call per named expiration."""
    call_map = {f"{exp}:7": {"650.0": [_contract(exp, "CALL", bid=1.0)]} for exp in expirations}
    return {
        "status": "SUCCESS",
        "underlying": None,
        "underlyingPrice": underlying_price,
        "interestRate": 4.25,
        "dividendYield": 1.28,
        "isDelayed": False,
        "isChainTruncated": False,
        "numberOfContracts": len(expirations),
        "callExpDateMap": call_map,
        "putExpDateMap": {},
    }


def _chain(expirations: list[str], **kwargs) -> VendorResponse:
    return VendorResponse(status=200, body=_chain_body(expirations, **kwargs))


# The two expirations the near window and the open tail each return.
NEAR_EXP = "2026-09-04"
TAIL_EXP = "2026-09-18"


class _WindowVendor:
    """A ``Vendor`` that answers each date range from a map, and records every call.

    ``windows`` maps a ``(from_iso, to_iso)`` range to its response, ``to_iso`` ``None``
    for the open tail. A mapped ``Exception`` is raised instead of returned. A range with
    no mapping raises, so a test never silently reaches past its setup, and a bare
    whole-chain request (no ``from_date``) raises by name, which is what pins the chunked
    route.
    """

    def __init__(self, **windows) -> None:
        self._windows = windows.pop("windows")
        self.calls: list[tuple[str, str | None, str | None]] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        if strike_count is not None:
            raise AssertionError("the fill must make no discovery request")
        if from_date is None:
            raise AssertionError(
                "the fill asked for the whole chain in one request. The biggest chains "
                "trip the gateway body limit, and those are the ones worth rescuing."
            )
        f = from_date.isoformat()
        t = to_date.isoformat() if to_date is not None else None
        self.calls.append((symbol, f, t))
        if (f, t) not in self._windows:
            raise AssertionError(f"no canned window for range {(f, t)}")
        answer = self._windows[(f, t)]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get_quotes(self, symbols):
        raise AssertionError("the fill is the chains surface alone")

    def token_mint_time(self):
        return et(2026, 9, 1, 9, 0)


def _both_windows() -> _WindowVendor:
    """A vendor whose two windows each return one expiration."""
    return _WindowVendor(windows={NEAR: _chain([NEAR_EXP]), TAIL: _chain([TAIL_EXP])})


def _fill(lake_root: Path, vendor, *, ticker: str = "SPY", pid: int = 7):
    """Run the real fill for one ticker, the way the guard calls it."""
    return capture.fill_option_close(
        ManualClock(start=FILL_MINUTE),
        vendor,
        ticker,
        slot=CLOSE,
        lake_root=lake_root,
        guards=GuardConstants(),
        plan=TWO_WINDOWS,
        pid=pid,
        session_phase="post_equity_close",
    )


def _rows(root: Path, ticker: str = "SPY", surface: str = CHAINS) -> list[dict]:
    directory = LakePaths(root).segment_dir(surface, ticker, DAY)
    if not directory.is_dir():
        return []
    return [
        row
        for path in sorted(directory.glob("*.arrows"))
        for row in journal.read_segment(path).to_pylist()
    ]


# -- 1. the close slot and the fetch minute --------------------------------------------


def test_the_fill_lands_the_close_slot_and_keeps_its_own_fetch_minute(lake_root):
    """The two timestamps are the point of the fill, and they must not collapse.

    A reader asking for the option close must get 16:15, so the close is the row's
    ``snap_ts``. The round trip must stay measurable, so the minute the fetch really ran
    in is the row's ``fetch_ts``. Stamping either one over the other loses the other.
    """
    captured = _fill(lake_root, _both_windows())

    assert captured == [NEAR_EXP, TAIL_EXP]
    rows = _rows(lake_root)
    assert rows, "the fill journaled nothing"
    assert {r["snap_ts"][:16] for r in rows} == {"2026-09-02T16:15"}
    assert {r["fetch_ts"][:16] for r in rows} == {"2026-09-02T16:18"}
    # Both provenance tags are the ones the 16:15 cycle would have carried.
    assert {r["close_tag"] for r in rows} == {OPTION_CLOSE}
    assert {r["session_phase"] for r in rows} == {"post_equity_close"}
    assert {r["row_kind"] for r in rows} == {journal.ROW_KIND_DATA}


def test_the_landed_fill_is_manifested_like_any_capture_segment(lake_root):
    """A segment with no manifest entry is invisible to the scrub and to compaction."""
    _fill(lake_root, _both_windows())

    segments = [
        rel for rel in latest_entries(lake_root) if "surface=chains" in rel and "SPY" in rel
    ]
    assert len(segments) == 1, f"expected one manifested fill segment, got {segments}"


def test_a_landed_fill_is_the_close_a_second_guard_run_reads(lake_root):
    """``close_tag_rows`` must count the fill as data, or a restart refetches it.

    The guard's first question is whether the day already holds ``option_close`` data
    rows. A fill landed under the wrong tag, or as a gap, would leave that question
    answered "no" and a second run would fetch the close again, past the window.
    """
    _fill(lake_root, _both_windows())

    rows = journal.close_tag_rows(lake_root, CHAINS, "SPY", DAY, OPTION_CLOSE)
    assert rows.data > 0
    assert rows.gaps == 0


# -- 2. the chunk plan, not one whole-chain request ------------------------------------


def test_the_fill_fetches_by_the_chunk_plan_rather_than_one_whole_chain_request(lake_root):
    """The route decision, pinned where a later edit cannot quietly undo it.

    A whole-chain request trips Schwab's gateway body limit on the biggest chains, and
    those are exactly the ones a close+5 rescue matters most for. So the fill reuses the
    cycle's own window plan. The vendor refuses a request with no ``from_date`` by name,
    so a fill that collapsed to one request fails here rather than passing quietly.
    """
    vendor = _both_windows()
    _fill(lake_root, vendor)

    assert vendor.calls == [("SPY", *NEAR), ("SPY", *TAIL)]
    # Each row carries the plan window that fetched it, the fetch provenance the nightly
    # re-tune groups by. A one-request fill would leave both null.
    rows = _rows(lake_root)
    assert {(r["window_start"], r["window_end"]) for r in rows} == {NEAR, TAIL}


# -- 3. a window that failed ------------------------------------------------------------


def test_a_window_that_failed_rides_the_fill_as_an_absence_marker(lake_root):
    """A partial fill still lands. The window it lost is named rather than dropped.

    The near window comes back too big and cannot be split past the depth bound, so it is
    given up. The tail still returns. The fill journals the tail's contracts as data and
    one absence marker gap row carrying the failed range and its class, all in one
    snapshot. A fill that refused outright here would throw away a window that did land.

    An earlier cycle captured both expirations, so the marker names the one series the
    failed window should have carried. That is what makes the returned set load-bearing:
    the marker row holds ``expiration_date`` like a data row does, so counting gap rows
    would report the fill as having captured a series it plainly did not.
    """
    capture.run_cycle(
        ManualClock(start=et(2026, 9, 2, 15, 59)),
        _both_windows(),
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=3,
        guards=GuardConstants(),
        plan=TWO_WINDOWS,
    )
    vendor = _WindowVendor(windows={NEAR: TOO_BIG, TAIL: _chain([TAIL_EXP])})
    captured = capture.fill_option_close(
        ManualClock(start=FILL_MINUTE),
        vendor,
        "SPY",
        slot=CLOSE,
        lake_root=lake_root,
        guards=GuardConstants(chain_chunk_max_split_depth=0),
        plan=TWO_WINDOWS,
        pid=7,
    )

    # The marker's expiration is not a captured one, so the returned set holds the tail
    # alone. The guard compares that against the day's baseline, so counting a series the
    # fill failed to fetch would hide the very shortfall the comparison exists to find.
    assert captured == [TAIL_EXP]
    rows = [r for r in _rows(lake_root) if r["snap_ts"].startswith("2026-09-02T16:15")]
    markers = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    assert len(markers) == 1
    assert (markers[0]["window_start"], markers[0]["window_end"]) == NEAR
    assert markers[0]["error_class"] == capture.CHAIN_CHUNK_FAILED
    # The marker names the series the failed window should have carried, read off the
    # prior durable batch, and that series is absent from what the fill reports capturing.
    assert markers[0]["expiration_date"].startswith(NEAR_EXP)
    # The marker carries the close slot and the close tag like every other row of the
    # snapshot, so a tagged fill tags consistently.
    assert markers[0]["snap_ts"][:16] == "2026-09-02T16:15"
    assert markers[0]["close_tag"] == OPTION_CLOSE
    # The tail's contract still landed as data beside it.
    assert [r["occ_symbol"] for r in rows if r["row_kind"] == journal.ROW_KIND_DATA] == [
        _contract(TAIL_EXP, "CALL", bid=0)["symbol"]
    ]


def test_a_partial_snapshot_reports_the_class_of_the_window_it_lost(lake_root):
    """A landed segment that does hold a failure must say so to its caller.

    ``journal_snapshot`` returns the segment it wrote, and the loop's own partial snapshot
    flags that segment with the first failed window's class, the representative signal.
    Returning null for a segment carrying absence markers would hide the failure from
    every caller of this entry, which is the shape a reader later counts on.
    """
    outcome = capture.journal_snapshot(
        lake_root,
        CHAINS,
        "SPY",
        body=_chain_body([TAIL_EXP]),
        cycle_start=FILL_MINUTE,
        fetch_ts=FILL_MINUTE,
        fetch_end_ts=FILL_MINUTE,
        slot=CLOSE,
        pid=7,
        absent_markers=[
            journal.AbsentMarker(NEAR[0], NEAR[1], "http_429", NEAR_EXP),
            journal.AbsentMarker(TAIL[0], TAIL[1], "http_401", TAIL_EXP),
        ],
    )

    # The first, not the last. The classes are deliberately different, because the reason
    # for "first" is that a rate-limit, auth death, and a transient fault must stay apart,
    # and two markers of one class could not tell which end was reported.
    assert outcome.error_class == "http_429"
    assert outcome.row_kind == journal.ROW_KIND_DATA


# -- 4. a fill that captured nothing ----------------------------------------------------


def test_a_fill_whose_every_window_failed_writes_no_row(lake_root):
    """Nothing captured means nothing written, and the caller is told so.

    The day already carries the gap row from the cycle that failed at the close, which is
    what triggered the fill. A second row for that one minute would double-count it in
    every per-slot completeness read, so the failed attempt is recorded in the guard's
    outcome rather than on disk.
    """
    vendor = _WindowVendor(
        windows={
            NEAR: VendorResponse(status=401, body={}),
            TAIL: VendorResponse(status=401, body={}),
        }
    )
    captured = _fill(lake_root, vendor)

    assert captured is None
    assert _rows(lake_root) == []
    assert latest_entries(lake_root) == {}


def test_a_failed_fill_leaves_the_days_existing_gap_row_standing_alone(lake_root):
    """End to end through the guard: one gap row for the minute before and after.

    The 16:15 cycle ran and failed, leaving one tagged gap row. The guard sees no data
    under the tag and calls the fill, which captures nothing. The minute must still carry
    exactly the one row it already had.
    """
    _gap_row(lake_root, "SPY", CLOSE)
    before = len(_rows(lake_root))
    vendor = _WindowVendor(windows={NEAR: VendorResponse(status=500, body={}), TAIL: TOO_BIG})

    outcome = _guard(
        lake_root, at=FILL_MINUTE, fill=lambda t, s: _fill(lake_root, vendor, ticker=t)
    ).run(DAY)

    assert outcome.filled == ()
    assert outcome.refused == ("SPY: fill fetch returned nothing",)
    assert len(_rows(lake_root)) == before == 1


# -- 5. the shape an ordinary cycle writes ----------------------------------------------


def test_the_filled_row_is_the_shape_an_ordinary_cycle_writes(lake_root, tmp_path):
    """Column for column, apart from the coordinates and tags the fill owns.

    The fill lands into the same partition an ordinary cycle writes to, and compaction
    merges both into one sealed file under one schema. A column the fill filled
    differently, or left null where a cycle fills it, would only surface at that merge or
    later in a query. So the two rows are compared here instead.
    """
    cycle_root = tmp_path / "cycle"
    cycle_root.mkdir()
    capture.run_cycle(
        ManualClock(start=CLOSE),
        _both_windows(),
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        cycle_root,
        pid=7,
        guards=GuardConstants(),
        plan=TWO_WINDOWS,
        close_tag=OPTION_CLOSE,
        session_phase="post_equity_close",
    )
    _fill(lake_root, _both_windows())

    from_cycle = sorted(_rows(cycle_root), key=lambda r: r["occ_symbol"])
    from_fill = sorted(_rows(lake_root), key=lambda r: r["occ_symbol"])

    assert from_cycle and len(from_cycle) == len(from_fill)
    assert set(from_cycle[0]) == set(from_fill[0]), "the two rows have different columns"
    # ``fetch_ts``, ``fetch_end_ts`` and ``captured_at`` differ because the fill really
    # did fetch at a different minute, which is the one thing the fill is allowed to
    # differ in. Every other column must match.
    stamps = {"fetch_ts", "fetch_end_ts", "captured_at"}
    for cycle_row, fill_row in zip(from_cycle, from_fill, strict=True):
        for column in set(cycle_row) - stamps:
            assert cycle_row[column] == fill_row[column], f"{column} differs"


def test_a_quotes_snapshot_refuses_a_chain_fetchs_windows(lake_root):
    """The two chain-only arguments belong to a chains body, and say so.

    ``journal_snapshot`` serves both surfaces. Quoting a window on a quotes body has no
    meaning, and dropping it silently would leave a caller believing provenance landed
    that never did.
    """
    with pytest.raises(ValueError, match="chains body"):
        capture.journal_snapshot(
            lake_root,
            journal.QUOTES_SURFACE,
            "SPY",
            body={"SPY": {"quote": {}}},
            cycle_start=FILL_MINUTE,
            fetch_ts=FILL_MINUTE,
            fetch_end_ts=FILL_MINUTE,
            windows=[(DAY, None)],
        )


# -- the guard around the fill ----------------------------------------------------------


def _gap_row(root: Path, ticker: str, slot: datetime) -> None:
    """One tagged chains gap row, standing for the close cycle that ran and failed."""
    schema = journal.schema_for(CHAINS)
    batch = journal._batch(
        schema,
        [
            {
                "snap_ts": slot.isoformat(),
                "ticker": ticker,
                "row_kind": journal.ROW_KIND_GAP,
                "close_tag": OPTION_CLOSE,
                "schema_version": 1,
            }
        ],
    )
    stamp = slot.strftime("%Y%m%dT%H%M%S%f")
    with journal.SegmentWriter.open(root, CHAINS, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _guard(root: Path, *, at: datetime, fill, tickers=("SPY",)) -> close_guard.CloseGuard:
    """A ``CloseGuard`` over an in-memory master and spans covering both closes."""
    master = SecurityMaster()
    spans = CaptureSpans()
    open_at = et(2026, 9, 2, 9, 30)
    for ticker in tickers:
        iid = master.register(
            kind="equity", capture_start=open_at, valid_from=open_at.date(), ticker=ticker
        )
        spans.open_span(iid, open_at, True)
    return close_guard.CloseGuard(
        lake_root=root,
        spans=lambda: spans,
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        master=lambda: master,
        fill=fill,
        pid=9,
    )


def test_a_vendor_that_raises_costs_one_ticker_its_fill_and_not_the_others(lake_root):
    """Skip-not-block, through the real fill. SPY's transport dies, QQQ still lands.

    One ticker's vendor failure must never cost another its close. The fill for SPY
    raises from inside the window fetch, which the guard catches per ticker, and QQQ's
    fill runs and journals.
    """
    dead = _WindowVendor(windows={NEAR: OSError("vendor down"), TAIL: OSError("vendor down")})
    alive = _both_windows()

    def fill(ticker: str, slot: datetime):
        if ticker == "SPY":
            raise RuntimeError("vendor down")
        return _fill(lake_root, alive, ticker=ticker)

    outcome = _guard(lake_root, at=FILL_MINUTE, fill=fill, tickers=("SPY", "QQQ")).run(DAY)

    assert outcome.filled == ("QQQ",)
    assert outcome.problems == ("chains/SPY option_close: RuntimeError",)
    assert _rows(lake_root, "QQQ")
    assert _rows(lake_root, "SPY") == []
    assert dead.calls == []


def test_the_membership_baseline_is_the_intraday_cycle_and_not_the_fill_itself(lake_root):
    """The shortfall comparison reads the baseline before the fill lands, never after.

    ``latest_expirations`` answers with the newest durable batch on the ticker, and the
    fill lands one. Read after the fill, it hands back the fill's own expirations, the
    comparison compares the fill against itself, and no shortfall is ever findable. The
    intraday cycle here carried two expirations and the fill returns one, so the
    difference must be reported.
    """
    capture.run_cycle(
        ManualClock(start=et(2026, 9, 2, 15, 59)),
        _both_windows(),
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=3,
        guards=GuardConstants(),
        plan=TWO_WINDOWS,
    )
    short = _WindowVendor(windows={NEAR: _chain([NEAR_EXP]), TAIL: _chain([])})

    outcome = _guard(
        lake_root, at=FILL_MINUTE, fill=lambda t, s: _fill(lake_root, short, ticker=t)
    ).run(DAY)

    assert outcome.filled == ("SPY",)
    assert outcome.shortfalls == ("SPY: 1 expirations",)


# -- 6. the daemon's own wiring ----------------------------------------------------------


def test_the_guard_the_daemon_builds_carries_a_fill(tmp_path):
    """The one line #90 exists for. A guard with no fill never refetches anything.

    ``CloseGuard`` treats an unset fill as marker-only and says "no fill fetcher", so a
    daemon built without one watches the window and never uses it, with every test still
    green. This asserts the default wiring, with no seam handed in, passes a producer.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    clock = ManualClock(start=FILL_MINUTE)

    guard = daemon._close_guard(
        str(config),
        str(tickers),
        SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        clock,
    )

    assert guard is not None
    assert guard._fill is not None, "the daemon built a guard that can never refetch"


def test_the_daemon_hands_the_guard_a_fill_that_lands_the_close(tmp_path, monkeypatch):
    """The whole production path, from the loop's dispatch down to the row on disk.

    Nothing here is replaced except the one thing a test cannot have: the ``schwab-py``
    client the vendor factory builds. Everything between the tick and the segment is the
    real code. That matters because this exact wiring shipped broken twice before, where
    a seam replaced in every test could have been a production no-op with the suite green.

    The clock starts at 16:14:30, so the sixth tick is 16:20, close+5, the moment the
    dispatch fires. The 16:15 cycle writes nothing, standing for the chain that failed at
    the close, so the guard finds the option close missing and fills it.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")

    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="SPY"
    )
    master.write(master_path(lake_root))
    spans = CaptureSpans()
    spans.open_span(iid, et(2026, 9, 2, 9, 30), True)
    spans.write(spans_path(lake_root))

    vendor = _both_windows()

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            return vendor

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: TWO_WINDOWS)

    clock = ManualClock(start=et(2026, 9, 2, 16, 14, 30))
    ticks = [0]

    def six() -> bool:
        ticks[0] += 1
        return ticks[0] <= 6

    daemon.run_loop_from_config(
        config_path=str(config),
        tickers_path=str(tickers),
        token_path=str(tmp_path / "token.json"),
        clock=clock,
        calendar=weekday_sessions(WEEK),
        assertion_runner=lambda args: None,
        # The loop's own cycles write nothing, so the option close is missing when the
        # guard runs. That is the chain that failed at 16:15, which close+5 exists to
        # rescue.
        cycle_runner=lambda *, close_tag, session_phase: capture.CycleResult(clock.now(), ()),
        transport=FakeTransport(),
        pinger=FakePinger(),
        compaction_runner=lambda args: None,
        should_continue=six,
    )

    # The day also carries the startup walk's own gap markers, one per minute the loop
    # was dead for, which are not this claim's. The fill's rows are the data rows.
    rows = [r for r in _rows(lake_root) if r["row_kind"] == journal.ROW_KIND_DATA]
    assert rows, "the daemon's guard refetched nothing, so the close+5 window went unused"
    assert {r["snap_ts"][:16] for r in rows} == {"2026-09-02T16:15"}
    assert {r["close_tag"] for r in rows} == {OPTION_CLOSE}
    # The phase the 16:15 cycle would have carried, read off the slot by the producer.
    assert {r["session_phase"] for r in rows} == {"post_equity_close"}
    # The fetch really happened at close+5, five minutes after the slot the rows carry.
    assert {r["fetch_ts"][:16] for r in rows} == {"2026-09-02T16:20"}
    # The daemon's producer went through the chunk plan, not one whole-chain request.
    assert vendor.calls == [("SPY", *NEAR), ("SPY", *TAIL)]


# -- what the lenses found: a fill that captured nothing, and one that gave up a window --


def test_a_fill_whose_windows_answered_with_no_contracts_writes_nothing(lake_root):
    """A 200 carrying an empty chain is a successful fetch that captured nothing.

    Schwab reports some faults in the body rather than in the status, so a window can
    come back 200 with empty expiration maps. That is a successful window to the chunker,
    which seeds the header and merges no contract, so the reassembled body is not
    ``None``. Landing it would leave a zero-row segment and a ``rows=0`` manifest entry
    standing for a close nobody captured, and the guard would report the close as filled.
    """
    empty = VendorResponse(
        status=200, body={"status": "FAILED", "callExpDateMap": {}, "putExpDateMap": {}}
    )
    captured = _fill(lake_root, _WindowVendor(windows={NEAR: empty, TAIL: empty}))

    assert captured is None
    assert _rows(lake_root) == []
    assert latest_entries(lake_root) == {}


def test_a_fill_reports_a_window_it_gave_up_even_when_the_baseline_is_blind(lake_root):
    """The membership comparison cannot see a window that failed both times.

    The window that fails at close+5 is usually the one that failed intraday, so the
    baseline is blind in exactly the same place and the expiration difference comes out
    empty. The absence rides the snapshot as a gap row carrying its own class, so the
    lake's record stands, but without this the outcome named nothing and the operator's
    only channel printed an empty string for a close of record missing a whole window.
    """
    failing = {NEAR: _chain([NEAR_EXP]), TAIL: TOO_BIG}
    capture.run_cycle(
        ManualClock(start=et(2026, 9, 2, 15, 59)),
        _WindowVendor(windows=dict(failing)),
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=3,
        guards=GuardConstants(chain_chunk_max_split_depth=0),
        plan=TWO_WINDOWS,
    )

    def fill(ticker: str, slot: datetime):
        return capture.fill_option_close(
            ManualClock(start=FILL_MINUTE),
            _WindowVendor(windows=dict(failing)),
            ticker,
            slot=slot,
            lake_root=lake_root,
            guards=GuardConstants(chain_chunk_max_split_depth=0),
            plan=TWO_WINDOWS,
            pid=7,
        )

    outcome = _guard(lake_root, at=FILL_MINUTE, fill=fill).run(DAY)

    # The fill stands for the series it does hold, which is the design's rule.
    assert outcome.filled == ("SPY",)
    # And the window it lost is named, so the report has something to print.
    assert outcome.shortfalls == ("SPY: 1 windows absent",)
    assert outcome.reportable


def test_a_dark_session_is_baseline_less_rather_than_a_phantom_shortfall(lake_root):
    """The baseline is the day's own cycle. Reaching back a day invents a shortfall.

    `latest_expirations` walks a ticker's segments across every date. On a session that
    captured nothing, the post-close restart this guard exists for, an unscoped read
    reaches back to an earlier session whose same-day series have since expired. Every
    one of them reads as missing from today's fill, which is a shortfall that cannot be
    true. The design's answer for a day with no cycle to compare against is baseline-less.
    """
    # Yesterday captured a series expiring yesterday, and one that lives on.
    capture.run_cycle(
        ManualClock(start=et(2026, 9, 1, 15, 59)),
        _WindowVendor(
            windows={
                (
                    date(2026, 9, 1).isoformat(),
                    (date(2026, 9, 1) + timedelta(days=9)).isoformat(),
                ): _chain(["2026-09-01"]),
                ((date(2026, 9, 1) + timedelta(days=10)).isoformat(), None): _chain([TAIL_EXP]),
            }
        ),
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=3,
        guards=GuardConstants(),
        plan=TWO_WINDOWS,
    )
    # Today captured nothing at all, and the fill lands today's chain.
    outcome = _guard(
        lake_root, at=FILL_MINUTE, fill=lambda t, s: _fill(lake_root, _both_windows(), ticker=t)
    ).run(DAY)

    assert outcome.filled == ("SPY",)
    assert outcome.baseline_less == ("SPY",)
    # 2026-09-01 expired yesterday. It cannot be missing from today's close.
    assert outcome.shortfalls == ()


# -- what the mutation lens found: guarantees the suite stated and did not hold ---------


def test_the_returned_set_is_what_landed_rather_than_what_the_body_claimed(lake_root):
    """The read-back is the point, and counting off the body would pass without it.

    The returned set is compared against ``journal.latest_expirations``, which reads the
    ``expiration_date`` column of a durable batch. So this side has to be built from that
    same column. A series whose contract carries no ``expirationDate`` lands with a null
    there, so the body's expiration map names it and the landed column does not. Counting
    map keys would report a series the fill cannot actually price, and the guard's
    shortfall comparison would go quiet on exactly the case it exists to find.
    """
    nameless = _contract(NEAR_EXP, "CALL", bid=1.0)
    del nameless["expirationDate"]
    body = _chain_body([])
    body["callExpDateMap"] = {f"{NEAR_EXP}:7": {"650.0": [nameless]}}
    vendor = _WindowVendor(
        windows={NEAR: VendorResponse(status=200, body=body), TAIL: _chain([TAIL_EXP])}
    )

    captured = _fill(lake_root, vendor)

    # The body named two series. Only one of them landed with an expiration a reader can
    # resolve, and that is the one the fill reports.
    assert captured == [TAIL_EXP]
    landed = {r["expiration_date"] for r in _rows(lake_root)}
    assert None in landed, "the nameless contract did not land, so the case is not exercised"


def test_the_fill_honours_a_recalibrated_guard_constant(tmp_path, monkeypatch):
    """A machine that tuned the split-depth bound must get a tuned close+5 fill.

    The bound decides how many midpoint splits a too-big window is worth before the range
    is given up, and it is tuned for the biggest chains, which are the ones a close+5
    rescue is for. Falling back to the built-in default here would leave the fill splitting
    on a machine whose config says not to.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root, guards={"chain_chunk_max_split_depth": 0})
    vendor = _WindowVendor(windows={NEAR: TOO_BIG, TAIL: _chain([TAIL_EXP])})

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            return vendor

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: TWO_WINDOWS)

    capture.fill_option_close_from_config(
        "SPY",
        slot=CLOSE,
        clock=ManualClock(start=FILL_MINUTE),
        config_path=str(config),
        token_path=str(tmp_path / "token.json"),
        pid=7,
    )

    # Depth 0 gives the near window up where it stands. The built-in default of 4 would
    # halve it and ask for ranges this vendor has never heard of.
    assert vendor.calls == [("SPY", *NEAR), ("SPY", *TAIL)]


def test_the_fill_builds_its_vendor_from_the_token_and_config_it_was_given(tmp_path, monkeypatch):
    """``--token`` is a real operator flag, and the fill has to honour it like the cycle.

    The daemon takes a token path and threads it down. A fill that fell back to the
    standard location would read a different token than the cycle running beside it, and
    blank credentials would fail every fill at the one moment the window is open.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    token = tmp_path / "elsewhere" / "token.json"
    seen: list[tuple] = []

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            seen.append((str(token_path), api_key, app_secret))
            return _both_windows()

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: TWO_WINDOWS)

    capture.fill_option_close_from_config(
        "SPY",
        slot=CLOSE,
        clock=ManualClock(start=FILL_MINUTE),
        config_path=str(config),
        token_path=str(token),
        pid=7,
    )

    assert seen == [(str(token), "api-key", "app-secret")]


def test_the_daemon_threads_its_token_path_down_to_the_fill(tmp_path, monkeypatch):
    """The same flag, through the daemon's own wiring rather than the capture entry."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    config = write_config(tmp_path, lake_root)
    token = tmp_path / "elsewhere" / "token.json"
    seen: list[str] = []

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            seen.append(str(token_path))
            return _both_windows()

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: TWO_WINDOWS)
    clock = ManualClock(start=FILL_MINUTE)

    fill = daemon._close_fill(
        str(config),
        str(token),
        SessionClock(clock=clock, calendar=weekday_sessions(WEEK)),
        clock,
    )
    fill("SPY", CLOSE)

    assert seen == [str(token)]


def test_two_fills_of_one_close_slot_land_as_separate_segments(lake_root):
    """The writer-session stamp comes from the fetch instant, never the slot.

    ``journal_snapshot`` promises that a re-run stamps a different ``start_ts`` so the
    ``O_CREAT | O_EXCL`` create never collides. Stamping from the close slot instead would
    give two fills of one close the same segment path, and the second would raise rather
    than land. The guard reaches here when the first fill's segment will not read, which
    it deliberately treats as a reason to fill anyway.
    """
    for minute in (FILL_MINUTE, FILL_MINUTE + timedelta(minutes=1)):
        capture.fill_option_close(
            ManualClock(start=minute),
            _both_windows(),
            "SPY",
            slot=CLOSE,
            lake_root=lake_root,
            guards=GuardConstants(),
            plan=TWO_WINDOWS,
            pid=7,
        )

    segments = sorted(LakePaths(lake_root).segment_dir(CHAINS, "SPY", DAY).glob("*.arrows"))
    assert len(segments) == 2, "two fills of one close collided on a single segment name"
    assert {r["snap_ts"][:16] for r in _rows(lake_root)} == {"2026-09-02T16:15"}
