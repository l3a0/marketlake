"""A chain that answered and brought no contract, from the vendor to the page.

A chains data segment can hold no contract at all. One window answering 200 with empty
expiration maps while another fails lands a data segment whose only row is the failed
window's absence marker, and a segment kind or a row count reads that as production. So
every alarm reading the cycle reset on a chain producing nothing (marketlake #326).

These run whole capture cycles over real journal segments with a programmable vendor,
then hand the results to a real ``Watchdog``. They cover:

1. Both writers count the rows whose own ``row_kind`` is data, beside the batch's total.
2. A count that raises falls back to reading the segment as production, and never costs
   the minute.
3. Each shape the issue measured pages as ``contracts_absent`` at both concurrency caps,
   while a chain that lost one window and landed the rest keeps resetting.
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
from lake.watchdog import CONTRACTS_ABSENT, Watchdog
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
SESSION = date(2026, 8, 24)
_PLAN = ChainPlan(((0, 9), (10, None)))

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


def _d(offset: int) -> str:
    return (SESSION + timedelta(days=offset)).isoformat()


def _chain(expirations: list[str]) -> VendorResponse:
    """A 200 carrying one call and one put per named expiration, and empty maps for none."""
    calls: dict = {}
    puts: dict = {}
    for exp in expirations:
        for side, letter, maps in (("CALL", "C", calls), ("PUT", "P", puts)):
            maps[f"{exp}:7"] = {
                "650.0": [
                    {
                        "symbol": f"SPY   {exp.replace('-', '')}{letter}00650000",
                        "putCall": side,
                        "strikePrice": 650.0,
                        "expirationDate": f"{exp}T20:00:00.000+00:00",
                        "quoteTimeInLong": 1787000099000,
                        "bid": 1.0,
                        "openInterest": 100,
                    }
                ]
            }
    return VendorResponse(
        status=200,
        body={
            "status": "SUCCESS",
            "underlying": None,
            "underlyingPrice": 650.0,
            "isDelayed": False,
            "isChainTruncated": False,
            "numberOfContracts": 2 * len(expirations),
            "callExpDateMap": calls,
            "putExpDateMap": puts,
        },
    )


class _Vendor:
    """Answers each of the two windows with what the test names, and quotes cleanly."""

    def __init__(self, near: VendorResponse | Exception, far: VendorResponse | Exception):
        self._windows = {(_d(0), _d(9)): near, (_d(10), None): far}

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        key = (from_date.isoformat(), to_date.isoformat() if to_date is not None else None)
        result = self._windows[key]
        if isinstance(result, Exception):
            raise result
        return result

    def get_quotes(self, symbols):
        return _QUOTES

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _run(vendor: _Vendor, lake_root: Path, *, cap: int = 1, pid: int = 4242):
    return capture.run_cycle(
        ManualClock(start=_CLOCK_START),
        vendor,
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=pid,
        guards=GuardConstants(capture_max_concurrency=cap),
        plan=_PLAN,
    )


_EMPTY = _chain([])
_BARE = VendorResponse(status=200, body={})
_FULL = _chain(["2026-09-18"])

# The shapes the issue measured at 9674fe4, each as the two windows' answers, with what
# the chain segment holds: every row, and the rows whose own kind is data.
_SHAPES = {
    "empty maps twice": (_EMPTY, _EMPTY, 0, 0),
    "empty maps beside a 500": (_EMPTY, VendorResponse(status=500, body={}), 1, 0),
    "empty maps beside a body error": (_EMPTY, VendorError("not a JSON object"), 1, 0),
    "a bare object beside a 429": (_BARE, VendorResponse(status=429, body={}), 1, 0),
}


@pytest.mark.parametrize("cap", [1, 20])
@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_a_chain_with_no_contract_lands_as_data_holding_no_data_row(lake_root, shape, cap):
    """The segment is still written, because vendor-verbatim is what the lake records.

    What changes is what it reports. ``rows`` counts the failed window's marker, so a
    reader testing it above zero still reads production, and ``data_rows`` does not.
    """
    near, far, rows, data_rows = _SHAPES[shape]
    result = _run(_Vendor(near, far), lake_root, cap=cap)

    chain = result.segment(CHAINS, "SPY")
    assert chain.row_kind == journal.ROW_KIND_DATA
    assert (chain.rows, chain.data_rows) == (rows, data_rows)
    assert chain.landed_data is False
    kinds = [row["row_kind"] for row in journal.read_segment(chain.path).to_pylist()]
    assert kinds.count(journal.ROW_KIND_DATA) == data_rows
    assert len(kinds) == rows
    # The quote beside it is one data row, and it produced.
    assert result.segment(QUOTES, "SPY").data_rows == 1


@pytest.mark.parametrize("cap", [1, 20])
def test_a_chain_that_lost_one_window_counts_the_contracts_it_landed(lake_root, cap):
    """The partial chain, the case marketlake #553 owns. It produced, so it still resets."""
    result = _run(_Vendor(_FULL, VendorResponse(status=429, body={})), lake_root, cap=cap)

    chain = result.segment(CHAINS, "SPY")
    assert (chain.rows, chain.data_rows, chain.error_class) == (3, 2, "http_429")
    assert chain.landed_data is True


def _pages_over(cycles, *, cap: int, lake_root: Path):
    watchdog = Watchdog()
    pages = []
    for pid, (near, far) in enumerate(cycles, start=1):
        pages += watchdog.observe(_run(_Vendor(near, far), lake_root, cap=cap, pid=pid))
    return watchdog, pages


@pytest.mark.parametrize("cap", [1, 20])
@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_every_contract_less_shape_pages_its_chain_at_the_threshold(lake_root, shape, cap):
    """The measurement the issue was filed on, now paging.

    At 9674fe4 each shape ran four cycles into one watchdog and ended at a count of 0 with
    no page. The manual clock restarts at the same instant each run, so the four cycles
    land in one minute. The watchdog counts cycles, not wall minutes, so that still
    exercises the threshold.
    """
    near, far, _, _ = _SHAPES[shape]
    watchdog, pages = _pages_over([(near, far)] * 4, cap=cap, lake_root=lake_root)

    assert watchdog.count(CHAINS, "SPY") == 4
    assert watchdog.count(QUOTES, "SPY") == 0
    assert [(page.title, page.cause) for page in pages] == [
        ("Capture down: SPY chains", CONTRACTS_ABSENT)
    ]


@pytest.mark.parametrize("cap", [1, 20])
def test_a_partial_chain_never_pages_however_long_it_runs(lake_root, cap):
    """The boundary on the other side: this fix must not reach the case #553 owns."""
    partial = (_FULL, VendorResponse(status=429, body={}))
    watchdog, pages = _pages_over([partial] * 4, cap=cap, lake_root=lake_root)

    assert watchdog.count(CHAINS, "SPY") == 0
    assert pages == []


def test_a_count_that_raises_reads_the_segment_as_production_and_keeps_the_minute(
    lake_root, monkeypatch, capsys
):
    """The count is an alarm's input and the segment is a minute, as for the drift scan.

    A raise before the writer opens would lose the minute, so it is guarded. What it falls
    back to is the reading every alarm made before the count existed. The other fallback,
    zero, would page a minute that may have landed every contract.
    """

    def explode(batch):
        raise RuntimeError("a bug in the count")

    monkeypatch.setattr(journal, "data_rows", explode)
    result = _run(_Vendor(_EMPTY, VendorResponse(status=500, body={})), lake_root)

    assert result.errors == ()
    chain = result.segment(CHAINS, "SPY")
    assert chain.data_rows is None
    assert chain.landed_data is True
    assert result.segment(QUOTES, "SPY").landed_data is True
    assert chain.path.exists()
    assert chain.partition in latest_entries(lake_root)
    err = capsys.readouterr().err
    assert "capture: data-row count failed on chains SPY: RuntimeError: a bug in the count" in err


def test_a_count_that_raises_on_a_failed_chain_still_reads_the_gap_as_a_failure(
    lake_root, monkeypatch
):
    """The fallback is the old reading, and the old reading never counted a gap.

    Falling back to production for every segment would reset a chain whose every window
    failed, whenever the count broke, which is the silent outage this change exists for.
    """

    def explode(batch):
        raise RuntimeError("a bug in the count")

    monkeypatch.setattr(journal, "data_rows", explode)
    failed = VendorResponse(status=500, body={})
    watchdog = Watchdog()
    for pid in (1, 2):
        result = _run(_Vendor(failed, failed), lake_root, pid=pid)
        watchdog.observe(result)

    chain = result.segment(CHAINS, "SPY")
    assert (chain.row_kind, chain.data_rows) == (journal.ROW_KIND_GAP, None)
    assert chain.landed_data is False
    assert watchdog.count(CHAINS, "SPY") == 2


def test_the_page_names_the_class_the_operator_reads():
    """The class is written nowhere but the page, so its spelling is the contract.

    The design's message table names it, and every other test compares against the
    constant, which would move with a misspelling.
    """
    assert CONTRACTS_ABSENT == "contracts_absent"


def _snapshot(lake_root: Path, body: dict, markers=(), pid: int = 1) -> capture.SegmentOutcome:
    return capture.journal_snapshot(
        lake_root,
        CHAINS,
        "SPY",
        body=body,
        cycle_start=_CLOCK_START,
        fetch_ts=_CLOCK_START,
        fetch_end_ts=_CLOCK_START,
        pid=pid,
        windows=[(_d(0), _d(9)), (_d(10), None)],
        absent_markers=markers,
    )


def test_journal_snapshot_counts_data_rows_the_same_way(lake_root):
    """The second writer, which the close+5 fill and onboarding land through."""
    full = _snapshot(lake_root, _FULL.body)
    assert (full.rows, full.data_rows, full.landed_data) == (2, 2, True)

    marker = journal.AbsentMarker(
        window_start=_d(10), window_end=None, error_class="http_500", expiration_date=None
    )
    empty = _snapshot(lake_root, _EMPTY.body, [marker], pid=2)
    assert (empty.row_kind, empty.rows, empty.data_rows) == (journal.ROW_KIND_DATA, 1, 0)
    assert empty.landed_data is False


def test_journal_snapshot_keeps_the_segment_when_the_count_raises(lake_root, monkeypatch):
    def explode(batch):
        raise RuntimeError("a bug in the count")

    monkeypatch.setattr(journal, "data_rows", explode)
    outcome = _snapshot(lake_root, _FULL.body)

    assert outcome.data_rows is None
    assert outcome.landed_data is True
    assert outcome.partition in latest_entries(lake_root)
