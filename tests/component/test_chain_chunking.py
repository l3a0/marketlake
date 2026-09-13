"""The windowed chain fetch, over the real filesystem.

A full SPY option chain exceeds Schwab's gateway body limit, so the capture cycle fetches
a chain by a plan of date windows and reassembles it into one snapshot. There is no
discovery request on the hot path. These run one whole cycle over real journal segments
with a programmable fake vendor, a manual clock, and a small injected plan. No network and
no wall clock are crossed. So the tier is component: one subsystem, capture, over real
files, with the vendor and clock still fake.

They cover the chunker's contract:

1. The plan's windows are each fetched by their date range, with no ``strike_count``
   discovery call, and reassembled into one chains segment. The header comes from the
   first window that succeeded.
2. Only a genuine size failure, a ``TooBigBody`` 502 or a body flagged
   ``isChainTruncated``, is split at its date midpoint and refetched until it succeeds.
3. A non-size failure, a non-2xx status or a raised exception, is recorded once with its
   own error class and never split.
4. A window that fails becomes one absent-marker gap row inside a tagged partial snapshot,
   carrying that window's class, while the other windows journal normally.
5. A chain where every window fails is a whole-chain gap carrying the first failed
   window's class.
6. The midpoint-split recursion honours the depth bound.
7. A body the merge cannot read is given up under its own drift class rather than the size
   class, leaves nothing half-merged behind it, and costs only the sub-range holding the
   unreadable expiration. A reassembled body the row builder rejects fails open to a
   whole-chain gap carrying that failure's own class, rather than leaving the cycle.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan
from lake.config import GuardConstants
from lake.manifest import latest_entries
from lake.tickers import Roster
from lake.vendor import VendorError, VendorResponse
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# A clock whose seconds are non-zero, so flooring to the minute is observable. The cycle's
# session date is this instant's date, 2026-08-24, and the plan's day offsets add to it.
_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
SESSION = date(2026, 8, 24)


def _d(offset: int) -> str:
    """The ISO date `offset` days after the session date, the way a window resolves."""
    return (SESSION + timedelta(days=offset)).isoformat()


# A 502 the way Schwab's gateway rejects an over-large chain body.
_TOO_BIG = VendorResponse(status=502, body={"errorcode": "protocol.http.TooBigBody"})

# A minimal batched-quote response so the cycle's quote leg lands cleanly beside the chain.
_QUOTES = VendorResponse(
    status=200,
    body={
        "SPY": {
            "assetMainType": "EQUITY",
            "realtime": True,
            "quote": {
                "bidPrice": 649.98,
                "askPrice": 650.02,
                "lastPrice": 650.0,
                "quoteTime": 1787000100000,
            },
        }
    },
)


def _roster() -> Roster:
    return Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}})


def _contract(exp_iso: str, put_call: str, *, bid: float, oi: int) -> dict:
    """One synthetic contract, enough fields for the calibrated row builder to read."""
    letter = "C" if put_call == "CALL" else "P"
    return {
        "symbol": f"SPY   {exp_iso.replace('-', '')}{letter}00650000",
        "putCall": put_call,
        "strikePrice": 650.0,
        "expirationDate": f"{exp_iso}T20:00:00.000+00:00",
        "quoteTimeInLong": 1787000099000,
        "bid": bid,
        "openInterest": oi,
    }


def _chain_body(
    expirations: list[str],
    *,
    truncated: bool = False,
    underlying_price: float = 650.0,
) -> dict:
    """A chain body carrying one call and one put per named expiration."""
    call_map: dict[str, dict[str, list]] = {}
    put_map: dict[str, dict[str, list]] = {}
    count = 0
    for exp_iso in expirations:
        key = f"{exp_iso}:7"
        call_map[key] = {"650.0": [_contract(exp_iso, "CALL", bid=1.0, oi=100)]}
        put_map[key] = {"650.0": [_contract(exp_iso, "PUT", bid=0.9, oi=90)]}
        count += 2
    return {
        "status": "SUCCESS",
        "underlying": None,
        "underlyingPrice": underlying_price,
        "interestRate": 4.25,
        "dividendYield": 1.28,
        "isDelayed": False,
        "isChainTruncated": truncated,
        "numberOfContracts": count,
        "callExpDateMap": call_map,
        "putExpDateMap": put_map,
    }


def _chain_response(expirations: list[str], **kwargs) -> VendorResponse:
    return VendorResponse(status=200, body=_chain_body(expirations, **kwargs))


class _WindowVendor:
    """A programmable ``Vendor`` for the windowed chunker.

    ``windows`` maps a ``(from_iso, to_iso)`` date range to its response, so the split tree
    is driven exactly. ``to_iso`` is ``None`` for the open tail, mirroring how the fetcher
    passes ``to_date=None``. A mapped value that is an ``Exception`` is raised instead of
    returned, to model a fetch that raises. A requested range with no mapping raises, so a
    test never silently reaches past its setup. The chunker records a raise with its own
    class rather than splitting, so the deterministic tests assert ``chain_calls`` (and the
    ``_calls_for`` helper) to check the exact ranges fetched. Any ``strike_count`` call raises,
    proving the hot path makes no discovery request.
    """

    def __init__(
        self, *, windows: dict[tuple[str, str | None], VendorResponse | Exception]
    ) -> None:
        self._windows = windows
        self.chain_calls: list[tuple[str, str | None, str | None]] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        if strike_count is not None:
            raise AssertionError("the windowed chunker must not make a strike_count call")
        f = from_date.isoformat() if from_date is not None else None
        t = to_date.isoformat() if to_date is not None else None
        self.chain_calls.append((symbol, f, t))
        key = (f, t)
        if key not in self._windows:
            raise AssertionError(f"no canned window for range {key}")
        result = self._windows[key]
        if isinstance(result, Exception):
            raise result
        return result

    def get_quotes(self, symbols):
        return _QUOTES

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _calls_for(vendor: _WindowVendor, from_iso: str, to_iso: str | None) -> int:
    """How many times a given date range was fetched, to prove split fan-out or its absence."""
    return sum(1 for (_sym, f, t) in vendor.chain_calls if (f, t) == (from_iso, to_iso))


def _run(
    vendor: _WindowVendor,
    lake_root: Path,
    plan: ChainPlan,
    *,
    guards: GuardConstants | None = None,
    pid: int = 4242,
):
    # ``pid`` distinguishes two cycles in one lake. The manual clock restarts at the same
    # instant each call, so a second cycle needs its own pid to get its own segment name.
    return capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        _roster(),
        lake_root,
        pid=pid,
        guards=guards,
        plan=plan,
    )


def _chain_rows(result, ticker: str = "SPY") -> list[dict]:
    return journal.read_segment(result.segment(CHAINS, ticker).path).to_pylist()


# -- 1. the plan's windows reassemble to one segment -------------------------------------


def test_windows_reassemble_to_one_segment_with_the_first_success_header(lake_root):
    # Two windows: a near-term closed window and the open tail. Each returns one expiration.
    # Both reassemble into one snapshot. The header comes from the first successful window,
    # so its underlying price wins over the tail's deliberately different one.
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response(["2026-08-28"], underlying_price=650.0),
            (_d(10), None): _chain_response(["2026-09-18"], underlying_price=999.0),
        },
    )
    result = _run(vendor, lake_root, plan)

    # Exactly one chains segment for SPY, and it is a data segment.
    chain_segs = [s for s in result.segments if s.surface == CHAINS]
    assert len(chain_segs) == 1
    assert chain_segs[0].row_kind == journal.ROW_KIND_DATA
    assert chain_segs[0].error_class is None

    rows = _chain_rows(result)
    # All four contracts across the two windows survived, none duplicated.
    assert len(rows) == 4
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    assert {r["occ_symbol"] for r in rows} == {
        _contract(exp, side, bid=0, oi=0)["symbol"]
        for exp in ("2026-08-28", "2026-09-18")
        for side in ("CALL", "PUT")
    }
    # The header is the first successful window's, repeated on every row.
    assert {r["underlying_price"] for r in rows} == {650.0}
    assert {r["interest_rate"] for r in rows} == {4.25}
    # The count is the four reassembled contracts, and nothing was given up, so the chain
    # reads untruncated.
    assert {r["number_of_contracts"] for r in rows} == {4}
    assert {r["is_chain_truncated"] for r in rows} == {False}
    # Each row carries the plan window holding its expiration as fetch provenance: the near
    # expiration sits in the closed window, the far one on the open tail.
    by_exp = {r["expiration_date"]: (r["window_start"], r["window_end"]) for r in rows}
    assert by_exp["2026-08-28T20:00:00.000+00:00"] == (_d(0), _d(9))
    assert by_exp["2026-09-18T20:00:00.000+00:00"] == (_d(10), None)

    # The request trace is exactly the plan's two windows, fetched by date range, with no
    # strike_count discovery call anywhere.
    assert vendor.chain_calls == [
        ("SPY", _d(0), _d(9)),
        ("SPY", _d(10), None),
    ]


# -- 2. a too-big window is split at its date midpoint -----------------------------------


def test_a_too_big_window_splits_at_its_date_midpoint(lake_root):
    # One closed window ten days wide. The whole-range fetch 502s, so it is halved at its
    # date midpoint and each half succeeds. All four contracts land, with no gaps. The open
    # tail is empty.
    plan = ChainPlan(((0, 10), (11, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(10)): _TOO_BIG,
            (_d(0), _d(5)): _chain_response(["2026-08-27"]),
            (_d(6), _d(10)): _chain_response(["2026-09-01"]),
            (_d(11), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    assert len(rows) == 4
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    assert result.segment(CHAINS, "SPY").error_class is None
    # The split is visible in the request trace: the full window, then its two date halves
    # (midpoint at offset 5), then the open tail.
    assert vendor.chain_calls == [
        ("SPY", _d(0), _d(10)),
        ("SPY", _d(0), _d(5)),
        ("SPY", _d(6), _d(10)),
        ("SPY", _d(11), None),
    ]


def test_an_is_chain_truncated_200_also_splits(lake_root):
    # A 200 flagged isChainTruncated is just as "too big" as a 502, so it splits too.
    plan = ChainPlan(((0, 1), (2, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(1)): _chain_response(["2026-08-24"], truncated=True),
            (_d(0), _d(0)): _chain_response(["2026-08-24"]),
            (_d(1), _d(1)): _chain_response(["2026-08-25"]),
            (_d(2), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    assert len(rows) == 4
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)


def test_a_nested_fault_too_big_502_also_splits(lake_root):
    # The real Schwab gateway nests the TooBigBody errorcode under fault.detail. It is a size
    # signal too, so it splits like the top-level errorcode form the other tests use.
    nested_too_big = VendorResponse(
        status=502,
        body={"fault": {"detail": {"errorcode": "protocol.http.TooBigBody"}}},
    )
    plan = ChainPlan(((0, 1), (2, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(1)): nested_too_big,
            (_d(0), _d(0)): _chain_response(["2026-08-24"]),
            (_d(1), _d(1)): _chain_response(["2026-08-25"]),
            (_d(2), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    assert len(rows) == 4
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    # The split happened: both date halves were fetched exactly once.
    assert _calls_for(vendor, _d(0), _d(0)) == 1
    assert _calls_for(vendor, _d(1), _d(1)) == 1


# -- 3. a non-size failure is classified, recorded once, and never split -----------------


@pytest.mark.parametrize("status", [401, 429, 500])
def test_a_non_size_failure_is_recorded_once_with_its_http_class(lake_root, status):
    # A ten-day window that would split if it were a size failure. A non-2xx that is not
    # TooBigBody is not a size problem, so it is fetched exactly once and recorded with its
    # http class, never fanned out into split requests. The open tail succeeds, so the chain
    # is a partial snapshot with one absent-marker carrying that class.
    plan = ChainPlan(((0, 10), (11, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(10)): VendorResponse(status=status, body={"error": "boom"}),
            (_d(11), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    # The failing window was fetched once, and neither date half was ever requested.
    assert _calls_for(vendor, _d(0), _d(10)) == 1
    assert _calls_for(vendor, _d(0), _d(5)) == 0
    assert _calls_for(vendor, _d(6), _d(10)) == 0

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class == f"http_{status}"

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    # The open tail's call and put journaled normally beside the one classified marker.
    assert len(data_rows) == 2
    assert len(gap_rows) == 1
    assert gap_rows[0]["error_class"] == f"http_{status}"
    # A fresh lake has no prior batch, so this is the per-window marker: no expiration
    # named, the failed range kept as provenance.
    assert gap_rows[0]["expiration_date"] is None
    assert (gap_rows[0]["window_start"], gap_rows[0]["window_end"]) == (_d(0), _d(10))


def test_a_raised_window_fetch_is_recorded_with_its_class_and_not_split(lake_root):
    # A window whose fetch raises is a transport failure, not a size signal. It is recorded
    # once with the exception's own class and never split. The near window raises and the
    # open tail succeeds, so the chain is a partial snapshot with a vendor_error marker.
    plan = ChainPlan(((0, 10), (11, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(10)): VendorError("slow timeout"),
            (_d(11), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    # The raising window was fetched once, with no split fan-out.
    assert _calls_for(vendor, _d(0), _d(10)) == 1
    assert _calls_for(vendor, _d(0), _d(5)) == 0

    gap_rows = [r for r in _chain_rows(result) if r["row_kind"] == journal.ROW_KIND_GAP]
    assert len(gap_rows) == 1
    assert gap_rows[0]["error_class"] == "vendor_error"
    assert gap_rows[0]["expiration_date"] is None
    assert (gap_rows[0]["window_start"], gap_rows[0]["window_end"]) == (_d(0), _d(10))


# -- 4. a permanently failing window becomes a tagged partial snapshot -------------------


def test_a_permanently_failing_window_yields_one_absent_marker(lake_root):
    # The near window succeeds. The open tail 502s and cannot be midpoint-split, so it is
    # given up as one failed range. The snapshot journals the near window's contracts and
    # one absent-marker for the tail, keyed by the tail's start date. It stays a data
    # segment, tagged partial, not a whole gap.
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response(["2026-08-28"]),
            (_d(10), None): _TOO_BIG,
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class == capture.CHAIN_CHUNK_FAILED

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]

    # The near window's call and put journaled normally.
    assert len(data_rows) == 2
    assert {r["expiration_date"] for r in data_rows} == {"2026-08-28T20:00:00.000+00:00"}
    assert all(r["error_class"] is None for r in data_rows)
    assert all(r["bid"] is not None for r in data_rows)

    # The chain-level fields are recomputed from the reassembly: two captured contracts,
    # and truncation true because a window was given up.
    assert all(r["number_of_contracts"] == 2 for r in data_rows)
    assert all(r["is_chain_truncated"] is True for r in data_rows)

    # The tail is one absent-marker, tagged, holding no data. This is a fresh lake with no
    # prior durable batch to name expirations from, so it is the per-window kind: no
    # expiration named, the failed range kept as provenance, its end null on the open tail.
    assert len(gap_rows) == 1
    gap = gap_rows[0]
    assert gap["error_class"] == capture.CHAIN_CHUNK_FAILED
    assert gap["expiration_date"] is None
    assert (gap["window_start"], gap["window_end"]) == (_d(10), None)
    assert gap["bid"] is None and gap["open_interest"] is None

    # A partial snapshot is still a durable, manifested segment.
    assert outcome.partition in latest_entries(lake_root)


def test_a_bounded_split_depth_gives_up_on_the_deeper_ranges(lake_root):
    # With a split-depth bound of 1, the near window 502s and splits once at its midpoint.
    # Each half is now at the bound, so a half still too big is given up wholesale rather
    # than split further. Here the first half fails and the second succeeds. The open tail
    # is empty.
    plan = ChainPlan(((0, 30), (31, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(30)): _TOO_BIG,
            (_d(0), _d(15)): _TOO_BIG,
            (_d(16), _d(30)): _chain_response(["2026-09-15"]),
            (_d(31), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan, guards=GuardConstants(chain_chunk_max_split_depth=1))

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    # The second half survived as two data rows. The first half is one per-window
    # absent-marker carrying the given-up sub-range, since a fresh lake has no prior batch.
    assert {r["expiration_date"] for r in data_rows} == {"2026-09-15T20:00:00.000+00:00"}
    assert [(r["window_start"], r["window_end"], r["expiration_date"]) for r in gap_rows] == [
        (_d(0), _d(15), None)
    ]
    assert all(r["error_class"] == capture.CHAIN_CHUNK_FAILED for r in gap_rows)


# -- 5. a whole-chain gap when every window fails ----------------------------------------


def test_every_window_failing_yields_a_whole_chain_gap(lake_root):
    # Two windows, a one-day near window and the open tail, both TooBigBody 502s. Neither can
    # be split, so nothing survives and the whole chain is one gap, tagged with the
    # size-failure class rather than becoming an empty snapshot.
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _TOO_BIG,
            (_d(1), None): _TOO_BIG,
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == capture.CHAIN_CHUNK_FAILED
    assert outcome.rows == 1
    gap = _chain_rows(result)[0]
    assert gap["row_kind"] == journal.ROW_KIND_GAP
    assert gap["bid"] is None
    # Both windows were tried by date range, with no discovery call.
    assert vendor.chain_calls == [
        ("SPY", _d(0), _d(0)),
        ("SPY", _d(1), None),
    ]


def test_whole_chain_gap_carries_the_first_failed_windows_class(lake_root):
    # Every window returns 401. None is a size failure, so none is split and nothing is
    # captured. The whole chain gaps with the representative class, http_401, not a blanket
    # chunk-failure, so the failure model still sees auth death on the chain surface.
    plan = ChainPlan(((0, 10), (11, None)))
    unauthorized = VendorResponse(status=401, body={"error": "unauthorized"})
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(10)): unauthorized,
            (_d(11), None): unauthorized,
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == "http_401"
    assert outcome.rows == 1
    # Each window was tried exactly once, with no split fan-out.
    assert vendor.chain_calls == [
        ("SPY", _d(0), _d(10)),
        ("SPY", _d(11), None),
    ]


def test_the_representative_class_is_the_first_failed_window_and_not_the_last(lake_root):
    """Two windows fail differently, so "first" is distinguishable from "last".

    The case above returns 401 from both windows, so it cannot tell one end from the
    other and the guarantee its name states goes unheld. The reason the first is
    representative is that auth death, a rate-limit, and a transient fault must stay
    apart in the failure model. With the near window unauthorized and the tail throttled,
    reporting the last would read as a rate-limit on a chain that is really locked out.
    """
    plan = ChainPlan(((0, 10), (11, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(10)): VendorResponse(status=401, body={"error": "unauthorized"}),
            (_d(11), None): VendorResponse(status=429, body={"error": "slow down"}),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == "http_401"


def test_a_partial_snapshots_segment_flag_is_the_first_failed_window_too(lake_root):
    """The same rule on the path where some windows did land.

    A partial snapshot journals as a data segment, and its segment flag carries the
    representative class the same way a whole-chain gap does. Two failed windows with
    different classes are what make the end being reported observable.
    """
    plan = ChainPlan(((0, 4), (5, 10), (11, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(4)): VendorResponse(status=401, body={"error": "unauthorized"}),
            (_d(5), _d(10)): VendorResponse(status=429, body={"error": "slow down"}),
            (_d(11), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class == "http_401"


# -- 6. absence markers name expirations off the journal, never live state ---------------


def test_a_failed_window_with_a_prior_batch_yields_per_expiration_markers(lake_root):
    # Cycle one captures the chain cleanly, so the journal holds a durable prior batch. Cycle
    # two fails the near window with a 401. The markers name that window's expirations by
    # reading the prior batch: one per expiration inside the failed range. The range starts
    # at the session date, so an expiration dated before it is not marked, nor is one
    # outside the failed window. The daemon held none of this in memory.
    plan = ChainPlan(((0, 9), (10, None)))
    first = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response(["2026-08-20", "2026-08-28", "2026-08-30"]),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    _run(first, lake_root, plan, pid=4242)

    second = _WindowVendor(
        windows={
            (_d(0), _d(9)): VendorResponse(status=401, body={"error": "unauthorized"}),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(second, lake_root, plan, pid=4243)

    rows = _chain_rows(result)
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    assert {r["expiration_date"] for r in gap_rows} == {"2026-08-28", "2026-08-30"}
    assert {(r["window_start"], r["window_end"]) for r in gap_rows} == {(_d(0), _d(9))}
    assert {r["error_class"] for r in gap_rows} == {"http_401"}
    assert all(r["bid"] is None and r["open_interest"] is None for r in gap_rows)
    # The tail's contracts still journaled as data beside the markers, and truncation reads
    # true because a window was given up.
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    assert {r["expiration_date"] for r in data_rows} == {"2026-09-18T20:00:00.000+00:00"}
    assert all(r["is_chain_truncated"] is True for r in data_rows)


def test_a_prior_batch_with_nothing_inside_the_failed_window_yields_a_per_window_marker(
    lake_root,
):
    # The prior batch exists but holds only a far-dated expiration, nothing inside the failed
    # near window. The failed range still yields exactly one marker, the per-window kind, so
    # the error class is never lost.
    plan = ChainPlan(((0, 9), (10, None)))
    first = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response([]),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    _run(first, lake_root, plan, pid=4242)

    second = _WindowVendor(
        windows={
            (_d(0), _d(9)): VendorResponse(status=429, body={"error": "throttled"}),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(second, lake_root, plan, pid=4243)

    gap_rows = [r for r in _chain_rows(result) if r["row_kind"] == journal.ROW_KIND_GAP]
    assert len(gap_rows) == 1
    assert gap_rows[0]["expiration_date"] is None
    assert (gap_rows[0]["window_start"], gap_rows[0]["window_end"]) == (_d(0), _d(9))
    assert gap_rows[0]["error_class"] == "http_429"


def test_a_clean_cycle_never_reads_the_journal_for_expirations(lake_root, monkeypatch):
    # The last-durable-batch read is a failure-path read only. A cycle with no failed range
    # never makes it, so the hot path stays free of any disk read for expirations.
    calls: list[tuple[object, str]] = []

    def counting(lake_root_arg, ticker):
        calls.append((lake_root_arg, ticker))
        return None

    monkeypatch.setattr(journal, "latest_expirations", counting)
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response(["2026-08-28"]),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    _run(vendor, lake_root, plan)
    assert calls == []


# -- 7. a body the merge cannot read, and one the row builder rejects ---------------------

# The two payload shapes that reach the merge's failure handler. Each sits inside one
# expiration, which is why a date-keyed split isolates it. A string where the vendor nests a
# strike map raises ``AttributeError`` on the walk, and a strike holding a number where the
# vendor sends a list of contracts raises ``TypeError`` when the merge extends a list with it.
# Two shapes deliberately absent. Envelope drift, a ``callExpDateMap`` that is not a mapping
# at all, is skipped outright and reaches nothing. And a strike value that is iterable but
# wrong, a string or an object, merges without complaint, because ``list.extend`` takes any
# iterable. That one is caught a layer on by the row builder instead, which the last test in
# this section covers.
_DRIFT_SHAPES = {
    "expiration_is_not_a_strike_map": "not a strike map",
    "strike_is_not_a_number_sequence": {"650.0": 1.0},
}


def _drifted_body(good: list[str], drifted: str, shape: str, map_key: str = "call") -> dict:
    """A chain body whose ``drifted`` expiration carries a shape the merge cannot read.

    The ``good`` expirations are well formed and are inserted first, so a merge writing
    into the reassembly maps as it walked would have committed them before it raised.
    ``map_key`` picks which of the two expiration maps carries the bad shape. The merge
    walks calls before puts, so putting it on the put side means a whole readable call map
    has already been staged when the raise lands.
    """
    body = _chain_body(good)
    body[f"{map_key}ExpDateMap"][f"{drifted}:7"] = _DRIFT_SHAPES[shape]
    return body


def _drifted_response(
    good: list[str], drifted: str, shape: str, map_key: str = "call"
) -> VendorResponse:
    return VendorResponse(status=200, body=_drifted_body(good, drifted, shape, map_key))


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_an_unreadable_body_gives_up_its_window_under_the_drift_class(lake_root, shape):
    # A one-day near window, so it cannot be midpoint-split and is given up as it stands.
    # Its body holds one well-formed expiration ahead of one the merge cannot read. The
    # window is given up under the drift class rather than the size class, because the
    # vendor's payload changed shape and no narrower chunk plan would fix that. The open
    # tail still lands as data.
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _drifted_response(["2026-08-24"], "2026-10-16", shape),
            (_d(1), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class == capture.CHAIN_SCHEMA_DRIFT
    assert outcome.error_class != capture.CHAIN_CHUNK_FAILED

    rows = _chain_rows(result)
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    assert len(gap_rows) == 1
    assert gap_rows[0]["error_class"] == capture.CHAIN_SCHEMA_DRIFT
    assert gap_rows[0]["expiration_date"] is None
    assert (gap_rows[0]["window_start"], gap_rows[0]["window_end"]) == (_d(0), _d(0))

    # The open tail's two contracts are the whole data side, so the fetch still landed what
    # the other windows returned rather than dropping the chain.
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    assert {r["expiration_date"] for r in data_rows} == {"2026-09-18T20:00:00.000+00:00"}


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_a_window_that_raised_mid_merge_leaves_nothing_half_written(lake_root, shape):
    """The window given up for drift contributes no row at all, good expirations included.

    The merge walks an expiration at a time, so a body whose second expiration is
    unreadable has already read the first. Committing as it walked left those contracts in
    the reassembly maps while the same window drew an absence marker saying it was never
    collected, and invented an empty bucket for the expiration it raised on. One window
    carrying both data rows and its own absence marker is the double-record the markers
    exist to prevent, so the merge stages the whole body and commits only on success.
    """
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _drifted_response(["2026-08-24"], "2026-10-16", shape),
            (_d(1), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    expirations = {r["expiration_date"] for r in rows}
    # Neither the readable expiration ahead of the raise nor the one it raised on reached a
    # row. The only data expiration is the tail's.
    assert "2026-08-24T20:00:00.000+00:00" not in expirations
    assert not any(e is not None and e.startswith("2026-10-16") for e in expirations)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    assert len(data_rows) == 2
    # The chain-level count is recomputed from what was stored, so it sees two contracts and
    # not the four a half-merge would have left.
    assert {r["number_of_contracts"] for r in data_rows} == {2}
    assert {r["is_chain_truncated"] for r in data_rows} == {True}


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_one_unreadable_expiration_costs_its_sub_range_and_not_the_window(lake_root, shape):
    # The whole four-day window is unreadable because one expiration inside it drifted, so it
    # splits at its date midpoint like a too-big window. Each half is refetched, and only the
    # single day still carrying the bad expiration is given up. The three readable sub-ranges
    # land as data. Refusing to split would have cost the whole four-day window instead.
    plan = ChainPlan(((0, 3), (4, None)))
    drifted = _drifted_response(["2026-08-24"], "2026-10-16", shape)
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(3)): drifted,
            (_d(0), _d(1)): _chain_response(["2026-08-26"]),
            (_d(2), _d(3)): drifted,
            (_d(2), _d(2)): _chain_response(["2026-08-27"]),
            (_d(3), _d(3)): drifted,
            (_d(4), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    # Both readable halves landed: a call and a put each.
    assert len(data_rows) == 4
    assert {r["expiration_date"] for r in data_rows} == {
        "2026-08-26T20:00:00.000+00:00",
        "2026-08-27T20:00:00.000+00:00",
    }
    # The loss is one day wide, not four, and it carries the drift class.
    assert [(r["window_start"], r["window_end"], r["error_class"]) for r in gap_rows] == [
        (_d(3), _d(3), capture.CHAIN_SCHEMA_DRIFT)
    ]
    # The split recursed only down the unreadable side. The readable halves terminated.
    assert _calls_for(vendor, _d(0), _d(1)) == 1
    assert _calls_for(vendor, _d(0), _d(0)) == 0
    assert _calls_for(vendor, _d(2), _d(2)) == 1


def _retyped_expiration_response(expirations: list[str]) -> VendorResponse:
    """A body that merges cleanly and that the calibrated row builder then rejects.

    The contracts nest exactly as the vendor's do, so nothing in the merge notices. One
    contract's ``expirationDate`` arrives as an epoch integer where the pinned schema holds
    a string, which is the retyped known field the design's schema policy names. Arrow
    refuses it when the batch is built, one layer past the fetch.
    """
    body = _chain_body(expirations)
    body["callExpDateMap"][f"{expirations[0]}:7"]["650.0"][0]["expirationDate"] = 1787000000000
    return VendorResponse(status=200, body=body)


def test_a_body_the_row_builder_rejects_fails_open_to_a_whole_chain_gap(lake_root):
    """A reassembled body Arrow refuses becomes a gap row, and the cycle runs on.

    Every window succeeds and the snapshot reassembles, so the failure lands where the row
    builder runs rather than in the fetch. Letting it propagate would leave the cycle
    runner, leave the daemon loop, and exit the process, and the ``KeepAlive`` successor
    would reach the same minute and do it again. So it fails open: the chain is one gap row
    carrying the failure's own class, and the quote surface for the same cycle still lands.
    """
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): _retyped_expiration_response(["2026-08-28"]),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    # The class is the exception's own name, snake-cased, the way every raised failure is
    # classified. Arrow refuses the retyped field with an ArrowTypeError.
    assert outcome.error_class == "arrow_type_error"
    assert outcome.rows == 1

    gap = _chain_rows(result)[0]
    assert gap["row_kind"] == journal.ROW_KIND_GAP
    assert gap["error_class"] == "arrow_type_error"
    assert gap["bid"] is None and gap["open_interest"] is None
    # The cycle survived the rejection: the quote surface journaled beside the gap, and the
    # gap segment is durable and manifested rather than lost with the process.
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA
    assert outcome.partition in latest_entries(lake_root)


def test_an_iterable_strike_value_passes_the_merge_and_gaps_at_the_row_builder(lake_root):
    """Where the merge's reach actually ends, and what catches what gets past it.

    ``list.extend`` takes any iterable, so a strike arriving as a string merges into the
    reassembly maps one character at a time rather than raising. The window is never given
    up and draws no marker. The damage surfaces a layer on, when the row builder reads those
    characters where contract dicts belong, and the cycle fails open to a whole-chain gap.
    So the two fail-open branches compose: what the merge cannot see, the row builder does,
    and neither one lets the payload out of the cycle. The price is that the loss is the
    whole chain rather than the one window, which is why the merge's limit is worth stating
    rather than leaving to be rediscovered.
    """
    body = _chain_body(["2026-08-28"])
    body["callExpDateMap"]["2026-08-28:7"]["655.0"] = "XY"
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): VendorResponse(status=200, body=body),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    # The merge accepted it, so the window was never split and never given up. One request.
    assert _calls_for(vendor, _d(0), _d(9)) == 1
    assert _calls_for(vendor, _d(0), _d(4)) == 0

    outcome = result.segment(CHAINS, "SPY")
    # Not the drift class. This never reached the merge's handler at all.
    assert outcome.error_class != capture.CHAIN_SCHEMA_DRIFT
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == "attribute_error"
    assert outcome.rows == 1
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_a_raise_on_the_put_side_rolls_the_call_side_back_too(lake_root, shape):
    """The rollback covers the whole body, not just the map that raised.

    The merge walks ``callExpDateMap`` before ``putExpDateMap``, so a body whose put side
    carries the bad shape has already read every call in the window when it raises.
    Committing each map as its own walk finished would leave those calls behind, and the
    same window draws an absence marker saying it collected nothing. That is the
    double-record again, reached from the other side, so both maps commit together or
    neither does.
    """
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _drifted_response(["2026-08-24"], "2026-10-16", shape, "put"),
            (_d(1), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.error_class == capture.CHAIN_SCHEMA_DRIFT

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    # The window's calls were fully read before the put side raised. None of them landed.
    assert "2026-08-24T20:00:00.000+00:00" not in {r["expiration_date"] for r in data_rows}
    assert len(data_rows) == 2
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    assert [(r["window_start"], r["window_end"]) for r in gap_rows] == [(_d(0), _d(0))]


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_a_drifted_window_honours_the_split_depth_bound(lake_root, shape):
    # The depth bound caps request spend, and a drifted window splits on the same path a
    # too-big one does, so the bound has to reach it too. With the bound at 1, the 30-day
    # window splits once and each half is then at the bound. The half that still drifts is
    # given up as a 16-day range rather than recursing to the single day inside it.
    plan = ChainPlan(((0, 30), (31, None)))
    drifted = _drifted_response(["2026-08-24"], "2026-10-16", shape)
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(30)): drifted,
            (_d(0), _d(15)): drifted,
            (_d(16), _d(30)): _chain_response(["2026-09-15"]),
            (_d(31), None): _chain_response([]),
        },
    )
    result = _run(vendor, lake_root, plan, guards=GuardConstants(chain_chunk_max_split_depth=1))

    gap_rows = [r for r in _chain_rows(result) if r["row_kind"] == journal.ROW_KIND_GAP]
    # One gap spanning the whole un-split half, not a day inside it.
    assert [(r["window_start"], r["window_end"], r["error_class"]) for r in gap_rows] == [
        (_d(0), _d(15), capture.CHAIN_SCHEMA_DRIFT)
    ]
    # The bound stopped the recursion: neither quarter of the drifted half was requested.
    assert _calls_for(vendor, _d(0), _d(7)) == 0
    assert _calls_for(vendor, _d(8), _d(15)) == 0


@pytest.mark.parametrize("shape", list(_DRIFT_SHAPES))
def test_every_window_drifting_yields_a_whole_chain_gap(lake_root, shape):
    # Both windows drift and neither can be split, so nothing is captured and no window
    # seeds the chain header. The chain is one whole-chain gap under the drift class rather
    # than a data segment holding no contracts. Seeding the header before the merge instead
    # of after would turn this into the latter, a segment claiming to be a snapshot of a
    # chain that was never read.
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _drifted_response([], "2026-10-16", shape),
            (_d(1), None): _drifted_response([], "2026-11-20", shape),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == capture.CHAIN_SCHEMA_DRIFT
    assert outcome.rows == 1
    gap = _chain_rows(result)[0]
    assert gap["row_kind"] == journal.ROW_KIND_GAP
    assert gap["bid"] is None


def test_a_row_builder_failure_that_is_not_a_type_error_fails_open_too(lake_root, monkeypatch):
    # The one test above feeds an ArrowTypeError, which is a TypeError subclass, so
    # narrowing the handler to TypeError would still pass it. The branch is meant to fail
    # open to whatever the row builder raises, since the cost of it escaping is the process,
    # so a second failure family outside that subtree keeps the width honest.
    def refuse(*args, **kwargs):
        raise ValueError("the builder cannot use this field")

    monkeypatch.setattr(journal, "chains_data_batch", refuse)
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): _chain_response(["2026-08-28"]),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == "value_error"
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA


def test_an_expiration_map_that_is_not_a_mapping_is_skipped_rather_than_given_up(lake_root):
    # The merge skips an expiration map that is not a mapping instead of raising on it. That
    # guard is what keeps whole-envelope drift away from the drift handler, and so away from
    # the split, where every half would fail identically and the recursion would walk the
    # full tree. The window merges the side that is still readable and lands as data.
    body = _chain_body(["2026-08-28"])
    body["callExpDateMap"] = "not a mapping at all"
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): VendorResponse(status=200, body=body),
            (_d(10), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class is None
    rows = _chain_rows(result)
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    # The readable put side of that window landed, and the calls were skipped rather than
    # raising. One put from the near window, a call and a put from the tail.
    assert len(rows) == 3
    # No split was attempted, because nothing was given up.
    assert _calls_for(vendor, _d(0), _d(9)) == 1
    assert _calls_for(vendor, _d(0), _d(4)) == 0


def test_two_windows_carrying_one_expiration_accrete_rather_than_overwrite(lake_root):
    # Windows cover disjoint date ranges, so in the ordinary case no expiration arrives
    # twice. The vendor is not bound to that, and the merge's stated rule is that it never
    # overwrites, only accretes. Two windows both returning 2026-08-28 make the rule
    # observable: one shares a strike with the other and one adds a strike of its own.
    shared = _chain_body(["2026-08-28"])
    second = _chain_body(["2026-08-28"])
    extra = _contract("2026-08-28", "CALL", bid=2.0, oi=5)
    second["callExpDateMap"]["2026-08-28:7"] = {"650.0": [extra], "660.0": [extra]}
    plan = ChainPlan(((0, 9), (10, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(9)): VendorResponse(status=200, body=shared),
            (_d(10), None): VendorResponse(status=200, body=second),
        },
    )
    result = _run(vendor, lake_root, plan)

    rows = _chain_rows(result)
    calls = [r for r in rows if r["put_call"] == "CALL"]
    puts = [r for r in rows if r["put_call"] == "PUT"]
    # Three calls: the first window's, plus the second window's two. Overwriting at the
    # strike level would drop the first, and overwriting at the expiration level would drop
    # it along with a put.
    assert len(calls) == 3
    assert sorted(r["bid"] for r in calls) == [1.0, 2.0, 2.0]
    assert len(puts) == 2
    assert {r["number_of_contracts"] for r in rows} == {5}


def test_a_merge_failure_of_any_kind_gives_up_the_window(lake_root, monkeypatch):
    """The merge handler is the last net under the fetch, so its width is load-bearing.

    ``_plan_chain`` is called from the cycle's plan loop with no ``try`` around it, and its
    own handler wraps the row builder rather than the fetch. So anything escaping the merge
    leaves the cycle runner and the daemon loop, which is the crash-loop the other branch
    exists to prevent, reached by a different door. The two shapes the merge raises today
    are an ``AttributeError`` and a ``TypeError``, and narrowing the handler to those would
    pass every other test here. A third shape drives the width directly.
    """

    def refuse(*args, **kwargs):
        raise ValueError("a shape the merge was never written for")

    monkeypatch.setattr(capture, "_collect_contracts", refuse)
    plan = ChainPlan(((0, 0), (1, None)))
    vendor = _WindowVendor(
        windows={
            (_d(0), _d(0)): _chain_response(["2026-08-24"]),
            (_d(1), None): _chain_response(["2026-09-18"]),
        },
    )
    result = _run(vendor, lake_root, plan)

    # Every window's merge refused, so nothing was captured and the chain is a whole-chain
    # gap under the drift class rather than an exception out of the cycle.
    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == capture.CHAIN_SCHEMA_DRIFT
    assert result.segment(QUOTES, "SPY").row_kind == journal.ROW_KIND_DATA
