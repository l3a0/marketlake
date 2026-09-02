"""The windowed chain fetch, over the real filesystem.

A full SPY option chain exceeds Schwab's gateway body limit, so the capture cycle fetches
a chain by a plan of date windows and reassembles it into one snapshot. There is no
discovery request on the hot path. These run one whole cycle over real journal segments
with a programmable fake vendor, a manual clock, and a small injected plan. No network and
no wall clock are crossed. So the tier is component: one subsystem, capture, over real
files, with the vendor and clock still fake.

They pin the chunker's contract:

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
    ``_calls_for`` helper) to pin the exact ranges fetched. Any ``strike_count`` call raises,
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
