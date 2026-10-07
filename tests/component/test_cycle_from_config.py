"""The capture entry's per-cycle re-reads, over real files.

``run_cycle_from_config`` is the production entry the daemon's cycle runner calls once a
minute. It reloads its inputs on every call and caches nothing across calls. That re-read
is what makes the design's headline onboarding claim true: a new ticker goes live
everywhere on the next cycle with no restart. It is also what lets capture resume the
instant a re-auth happens, and what lets a recalibrated guard constant land without one.

Each case runs that entry twice over a throwaway config, roster, token, and lake on disk,
and rewrites one input file between the two cycles. A cached input is invisible inside a
single cycle, so a single call could never catch any of it. The vendor is a fake and the clock
is manual, so no network and no wall clock are crossed. The tier is component: the
capture entry over real files, with the vendor and the clock still fake.

Four claims are covered.

1. The roster reaches the chain workers, so a ticker onboarded mid-session has its chain
   captured on the next cycle.
2. The same roster snapshot reaches the shared quote batch, so an onboarded ticker rides
   the next cycle's one batched request too.
3. The entry rebuilds its vendor from the token path every cycle, so a re-auth mid-session
   resumes capture on the next cycle.
4. The config is re-read, so the chunker's recalibrated split-depth bound takes effect on
   the next cycle.

Two boundaries are worth naming, because the design's claim is wider than this file.

- The design names a third consumer of the roster snapshot, the per-ticker watchdog
  counters. It reads the roster in the daemon's missed-slot hook, not in this entry, so
  it is out of this file's reach.
- The entry reloads one more input, the chain plan. Every case here points it at a
  test-owned path so the date windows are deterministic. No case here fails if that
  re-read breaks.
"""

from __future__ import annotations

import errno
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.compact import write_chain_plan
from lake.config import ConfigError
from lake.schwab import is_transient_failure
from lake.vendor import VendorResponse
from tests.support.calendar import et
from tests.support.clock import ManualClock
from tests.support.config import write_config

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# The session these cycles run in, and the minute the first one fires on. The second
# fires a minute later, the way the loop calls the entry.
DAY = date(2026, 9, 2)
FIRST_MINUTE = et(2026, 9, 2, 10, 0)

# A whole chain in one request exceeds Schwab's gateway body limit, so the chunker fetches
# it in date windows instead. A window is one ``from_date`` / ``to_date`` request, and a
# plan names its windows as day offsets from the session date. The last window is always
# open-ended, the tail that catches newly listed far-dated series. One open-ended window is
# the smallest such plan. It keeps a chain to a single request, so the recorded calls are
# one per options ticker per cycle.
ONE_WINDOW = ChainPlan(((0, None),))

# A four-day finite window ahead of that open tail. A window whose response comes back too
# big is split at its date midpoint and both halves refetched, and the depth bound caps how
# many of those splits are tried before the range is given up. An open tail has no end date
# to halve, so the finite window is the one the bound decides the cost of.
SPLITTABLE = ChainPlan(((0, 3), (4, None)))
WHOLE = (DAY, DAY + timedelta(days=3))
FIRST_HALF = (DAY, DAY + timedelta(days=1))
SECOND_HALF = (DAY + timedelta(days=2), DAY + timedelta(days=3))
TAIL = (DAY + timedelta(days=4), None)

SPY_ONLY = "SPY: {options: true, chain_cadence: 1m}\n"
WITH_QQQ = SPY_ONLY + "QQQ: {options: true, chain_cadence: 1m}\n"
WITH_XYZ = SPY_ONLY + "XYZ: {options: false}\n"

# The gateway's size fault, the one signal that warrants a midpoint split.
TOO_BIG = {"errorcode": "protocol.http.TooBigBody"}

# The two refresh tokens a re-auth run sees, before and after the browser login, each with
# the epoch second it was minted at. A week separates them, the token's own lifetime.
EXPIRED = "expired-refresh-token"
FRESH = "fresh-refresh-token"
EXPIRED_MINTED = 1786400000
FRESH_MINTED = 1787004800


def _chain_body(symbol: str) -> dict:
    """A one-contract chain body for ``symbol``, the smallest the row builder reads."""
    return {
        "callExpDateMap": {
            "2026-09-18:16": {
                "650.0": [
                    {
                        "symbol": f"{symbol:<6}260918C00650000",
                        "putCall": "CALL",
                        "strikePrice": 650.0,
                        "expirationDate": "2026-09-18T20:00:00.000+00:00",
                        "bid": 4.2,
                        "ask": 4.25,
                        "last": 4.22,
                        "openInterest": 1234,
                        "quoteTimeInLong": 1787000099000,
                    }
                ]
            }
        },
        "putExpDateMap": {},
        "isChainTruncated": False,
    }


def _quote_body(symbols: Sequence[str]) -> dict:
    """A batched-quote body carrying one envelope per requested symbol."""
    return {
        symbol: {
            "assetMainType": "EQUITY",
            "realtime": True,
            "quote": {
                "bidPrice": 649.98,
                "askPrice": 650.02,
                "lastPrice": 650.0,
                "quoteTime": 1787000100000,
            },
        }
        for symbol in symbols
    }


def _token_text(refresh: str, minted: int) -> str:
    """A token file's text, carrying its refresh token and its mint epoch second.

    A re-login mints a strictly newer token, so the fresh file's stamp is later than the
    expired one's. The design's rule is a mint-time comparison: rebuild the client when
    the file's stamp is newer than the in-memory token's. The entry rebuilds
    unconditionally today, which satisfies that rule. Moving the stamp keeps these cases
    true under either reading, rather than locking in the unconditional rebuild by accident.
    """
    return json.dumps({"refresh_token": refresh, "creation_timestamp": minted})


class _Vendor:
    """A fake vendor that records every request and answers with a canned reply.

    The two statuses are separate so one leg can fail while the other stays clean. An
    expired token is a 401 on both, and a too-big chain is a 502 on the chain leg alone.
    ``chain_body`` overrides the chain reply, which is how the size fault is served.
    """

    def __init__(
        self,
        *,
        chain_status: int = 200,
        quote_status: int = 200,
        chain_body: Mapping[str, object] | None = None,
    ) -> None:
        self._chain_status = chain_status
        self._quote_status = quote_status
        self._chain_body = chain_body
        self.chains: list[str] = []
        self.windows: list[tuple[date | None, date | None]] = []
        self.quotes: list[tuple[str, ...]] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self.chains.append(symbol)
        self.windows.append((from_date, to_date))
        body = _chain_body(symbol) if self._chain_body is None else self._chain_body
        return VendorResponse(status=self._chain_status, body=body)

    def get_quotes(self, symbols):
        self.quotes.append(tuple(symbols))
        return VendorResponse(status=self._quote_status, body=_quote_body(symbols))

    def token_mint_time(self):
        return et(2026, 9, 1, 9, 0)


@dataclass(frozen=True)
class _Rig:
    """The throwaway machine one cycle reads: a config, a roster, a token, and a lake."""

    lake_root: Path
    config: Path
    tickers: Path
    token: Path
    plan: Path


def _rig(tmp_path: Path, roster: str, *, plan: ChainPlan = ONE_WINDOW) -> _Rig:
    """A complete config, roster, token, plan, and lake under ``tmp_path``."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text(roster)
    token = tmp_path / "token.json"
    token.write_text(_token_text(FRESH, FRESH_MINTED))
    plan_path = tmp_path / "chain_plan.json"
    write_chain_plan(plan, plan_path)
    return _Rig(
        lake_root=lake_root,
        config=write_config(tmp_path, lake_root),
        tickers=tickers,
        token=token,
        plan=plan_path,
    )


def _wire(monkeypatch, rig: _Rig, build) -> list:
    """Point the entry's two module-level names at the rig, with no real client in path.

    1. ``load_chain_plan`` is pointed at a test-owned path. The plan file is machine-local,
       so the test owns its path rather than the home directory's. The loader itself stays
       the real one, called once per cycle.
    2. ``SchwabVendor`` is replaced by a stub. The production factory builds a live
       ``schwab-py`` client, and a test has no token to build one from.

    ``build`` receives the token path the entry passed, so a case can answer by what the
    file holds at that moment. The returned list collects the ``clock`` each build was
    handed, which is what turns request timing on.
    """
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(rig.plan))
    clocks: list = []

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret, clock=None):
            clocks.append(clock)
            return build(Path(token_path))

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)
    return clocks


def _cycle(rig: _Rig, clock: ManualClock) -> capture.CycleResult:
    """Run one cycle through the production entry, the way the daemon's runner calls it."""
    return capture.run_cycle_from_config(
        clock=clock,
        config_path=rig.config,
        tickers_path=rig.tickers,
        token_path=rig.token,
        pid=4242,
    )


def _kinds(result: capture.CycleResult) -> set[tuple[str, str, str, str | None]]:
    """Every segment as ``(surface, ticker, row_kind, error_class)``."""
    return {(seg.surface, seg.ticker, seg.row_kind, seg.error_class) for seg in result.segments}


def test_the_production_entry_turns_request_timing_on_with_its_own_clock(tmp_path, monkeypatch):
    # ``from_token`` hooks the client only when handed a clock. The daemon's cycles reach
    # capture only through this entry, so a clock dropped here would switch production
    # timing off with every other test still green.
    rig = _rig(tmp_path, SPY_ONLY)
    vendor = _Vendor()
    clocks = _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)

    _cycle(rig, clock)

    assert len(clocks) == 1
    assert clocks[0] is clock


# -- 1. the roster reaches the chain workers -----------------------------------------


def test_a_ticker_onboarded_mid_session_is_chained_on_the_next_cycle(tmp_path, monkeypatch):
    """Onboarding writes the roster entry and expects capture to pick it up unaided.

    The onboarding command writes the ``tickers.yaml`` entry itself and stops. No signal
    reaches a running daemon, so the roster read at the top of the cycle is the only thing
    that can put a new ticker in front of a chain worker. A roster cached for the session
    leaves an onboarded ticker uncaptured until the next restart, and the design promises
    no restart.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)

    before = _cycle(rig, clock)
    first = list(vendor.chains)
    # The onboarding command's own write, landing between two capture minutes.
    rig.tickers.write_text(WITH_QQQ)
    clock.advance(60)
    after = _cycle(rig, clock)
    second = vendor.chains[len(first) :]

    # Nothing chained QQQ while its entry did not exist.
    assert first == ["SPY"]
    with pytest.raises(KeyError):
        before.segment(CHAINS, "QQQ")

    # The next cycle chained both. The two are compared order-free on purpose. Since
    # marketlake #532 the tickers' windows are fired concurrently, so the order they reach
    # the vendor is not this claim's to assert.
    assert sorted(second) == ["QQQ", "SPY"]

    # The next cycle fetched the new ticker's chain and journaled its contracts as data.
    segment = after.segment(CHAINS, "QQQ")
    assert segment.row_kind == journal.ROW_KIND_DATA
    rows = journal.read_segment(segment.path).to_pylist()
    assert [row["occ_symbol"] for row in rows] == ["QQQ   260918C00650000"]


# -- 2. the same snapshot reaches the shared quote batch ------------------------------


def test_a_ticker_onboarded_mid_session_rides_the_next_quote_batch(tmp_path, monkeypatch):
    """The quote sampler is one shared request, and the new ticker has to be inside it.

    Every configured ticker rides the same batched quote request, so there is no
    per-ticker quotes knob to turn. An equity-only onboard is the sharp case: it captures
    through that batch and through nothing else. A roster snapshot that reached the chain
    workers alone would leave such a ticker recording nothing at all, while its entry sat
    in the file looking live.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)

    before = _cycle(rig, clock)
    rig.tickers.write_text(WITH_XYZ)
    clock.advance(60)
    after = _cycle(rig, clock)

    # One batched request per cycle, and the second carried the onboarded ticker. The
    # symbols are compared order-free, the same reason as the chain calls above.
    assert [sorted(batch) for batch in vendor.quotes] == [["SPY"], ["SPY", "XYZ"]]
    with pytest.raises(KeyError):
        before.segment(QUOTES, "XYZ")

    # An equity-only ticker journals quotes and no chain, which is its whole capture path.
    assert (QUOTES, "XYZ", journal.ROW_KIND_DATA, None) in _kinds(after)
    with pytest.raises(KeyError):
        after.segment(CHAINS, "XYZ")


# -- 3. the token file is re-read ----------------------------------------------------


def test_a_re_auth_mid_session_is_picked_up_on_the_next_cycle(tmp_path, monkeypatch):
    """Capture has to resume the instant a re-login mints a fresh token.

    The refresh token dies every seven days and only a browser login resets it. While it
    is dead every call is a 401 and nothing is being recorded. A vendor built once and
    kept for the session stays dead after the re-login, so the operator's one-minute fix
    buys nothing until a restart. Rebuilding from the token path at the top of every cycle
    is what makes "capture resumes the instant re-auth happens" true.

    The limit is worth naming. The stub is what reads the file here, so this case covers
    the entry's per-cycle rebuild, not the real factory's file read. A cache placed inside
    ``SchwabVendor.from_token`` itself would break the same promise and leave this case
    green. Covering that belongs to ``lake.schwab``.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    seen: list[str] = []

    def build(path: Path) -> _Vendor:
        """A vendor built from whatever the token file holds when the cycle asks."""
        refresh = json.loads(path.read_text())["refresh_token"]
        seen.append(refresh)
        status = 200 if refresh == FRESH else 401
        return _Vendor(chain_status=status, quote_status=status)

    _wire(monkeypatch, rig, build)
    rig.token.write_text(_token_text(EXPIRED, EXPIRED_MINTED))
    clock = ManualClock(start=FIRST_MINUTE)

    dead = _cycle(rig, clock)
    # The re-login, rewriting the token file in place while the daemon runs.
    rig.token.write_text(_token_text(FRESH, FRESH_MINTED))
    clock.advance(60)
    live = _cycle(rig, clock)

    # Both surfaces gapped on the expired token, and both captured on the very next cycle.
    assert _kinds(dead) == {
        (CHAINS, "SPY", journal.ROW_KIND_GAP, "http_401"),
        (QUOTES, "SPY", journal.ROW_KIND_GAP, "http_401"),
    }
    assert _kinds(live) == {
        (CHAINS, "SPY", journal.ROW_KIND_DATA, None),
        (QUOTES, "SPY", journal.ROW_KIND_DATA, None),
    }
    # The vendor was rebuilt from the token path on each cycle, not built once and kept.
    assert seen == [EXPIRED, FRESH]


# -- 4. the config is re-read --------------------------------------------------------


def test_a_recalibrated_split_depth_takes_effect_on_the_next_cycle(tmp_path, monkeypatch):
    """A guard constant is edited by hand, and the edit has to land without a restart.

    The guard constants live in ``config.yaml`` with pinned defaults, recalibrated against
    slice 1's measured distributions. A config cached for the session leaves a
    recalibration waiting on a restart, which is the cost the per-cycle read exists to
    avoid.

    The chunker's split-depth bound is the guard this drives, because it is the one a
    cycle reads straight off the config. Not every guard is read that way. The watchdog's
    page threshold is read once, when the loop builds its watchdog at start-up, so no
    per-cycle read reaches it. The claim here is the bound, not the whole class.
    """
    rig = _rig(tmp_path, SPY_ONLY, plan=SPLITTABLE)
    vendor = _Vendor(chain_status=502, chain_body=TOO_BIG)
    _wire(monkeypatch, rig, lambda path: vendor)
    write_config(tmp_path, rig.lake_root, guards={"chain_chunk_max_split_depth": 0})
    clock = ManualClock(start=FIRST_MINUTE)

    _cycle(rig, clock)
    # The recalibration, a one-line edit to the hand-owned config.
    write_config(tmp_path, rig.lake_root, guards={"chain_chunk_max_split_depth": 1})
    clock.advance(60)
    _cycle(rig, clock)

    # A bound of zero gives the finite window up on the first refusal. A bound of one
    # splits it at its date midpoint and refetches both halves. Those two half-ranges are
    # a set no cycle reading the first bound could produce. The open tail is refused the
    # same way under both bounds, because it can never be split.
    # Each cycle fires its windows concurrently (#532), so within a cycle the ranges reach the
    # vendor in any order. A split's two halves still run in turn inside their window's task.
    assert Counter(vendor.windows[:2]) == Counter([WHOLE, TAIL])
    assert Counter(vendor.windows[2:]) == Counter([WHOLE, FIRST_HALF, SECOND_HALF, TAIL])


# -- 5. capture never records outside a capture span ----------------------------------


def test_a_ticker_with_a_closed_span_is_not_captured_even_if_still_enabled(tmp_path, monkeypatch):
    """The invariant retiring exists to guarantee: no row lands outside a span.

    Retiring closes a ticker's capture span before it disables the roster entry, so a
    crash between the two writes can leave a ticker enabled with a closed span. This is
    that exact state, built directly. The cycle must not capture it, or a row would be
    recorded for a minute the ticker's span no longer covers.
    """
    from lake.capture_spans import CaptureSpans, spans_path
    from lake.security_master import SecurityMaster, master_path

    rig = _rig(tmp_path, WITH_XYZ)
    master = SecurityMaster()
    spy = master.register(
        kind="equity", capture_start=et(2026, 9, 1, 9, 30), valid_from=DAY, ticker="SPY"
    )
    xyz = master.register(
        kind="equity", capture_start=et(2026, 9, 1, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(spy, et(2026, 9, 1, 9, 30), True)  # SPY still capturing
    spans.open_span(xyz, et(2026, 9, 1, 9, 30), False)
    spans.close_span(xyz, et(2026, 9, 2, 9, 45))  # XYZ retired, span closed before the cycle
    spans.write(spans_path(rig.lake_root))

    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)
    result = _cycle(rig, clock)

    tickers_captured = {seg.ticker for seg in result.segments}
    assert tickers_captured == {"SPY"}
    assert "XYZ" not in vendor.quotes[0]
    # Still enabled, so it is named rather than dropped in silence (marketlake #554).
    assert result.out_of_span == ("XYZ",)
    assert result.nothing_to_capture is False


def _one_ticker_lake(rig: _Rig) -> None:
    """A master and spans file for SPY alone, its one span closed before ``FIRST_MINUTE``.

    Both are valid files the commands themselves would write. A directory or a foreign
    parquet would read differently once marketlake #551 lands, so none is used here.
    """
    from lake.capture_spans import CaptureSpans, spans_path
    from lake.security_master import SecurityMaster, master_path

    master = SecurityMaster()
    spy = master.register(
        kind="equity", capture_start=et(2026, 9, 1, 9, 30), valid_from=DAY, ticker="SPY"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(spy, et(2026, 9, 1, 9, 30), True)
    spans.close_span(spy, et(2026, 9, 2, 9, 45))
    spans.write(spans_path(rig.lake_root))


def test_a_roster_the_spans_emptied_is_not_read_as_every_ticker_retired(tmp_path, monkeypatch):
    """Enabled tickers the spans left out still owe their minutes (marketlake #554).

    This is the state a retire leaves when it stops between closing the span and turning
    the entry off, and the state an onboard leaves when it stops after writing its roster
    entry. The files cannot tell the two apart, and the second owes every minute. So the
    cycle fetches nothing, as before, but it must not report ``nothing_to_capture``,
    because the daemon feeds the dead-man on that flag and a fed dead-man never pages.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    _one_ticker_lake(rig)
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert result.segments == ()
    assert vendor.chains == [] and vendor.quotes == []
    assert result.out_of_span == ("SPY",)
    assert result.nothing_to_capture is False


def test_the_tickers_left_out_are_named_in_roster_order(tmp_path, monkeypatch):
    """The line reads the names in the order the operator wrote them, not sorted."""
    from lake.capture_spans import CaptureSpans, spans_path
    from lake.security_master import SecurityMaster, master_path

    rig = _rig(tmp_path, "XYZ: {options: false}\nABC: {options: false}\n")
    master = SecurityMaster()
    spans = CaptureSpans()
    for ticker in ("XYZ", "ABC"):
        iid = master.register(
            kind="equity", capture_start=et(2026, 9, 1, 9, 30), valid_from=DAY, ticker=ticker
        )
        spans.open_span(iid, et(2026, 9, 1, 9, 30), False)
        spans.close_span(iid, et(2026, 9, 2, 9, 45))
    master.write(master_path(rig.lake_root))
    spans.write(spans_path(rig.lake_root))
    _wire(monkeypatch, rig, lambda path: _Vendor())

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert result.out_of_span == ("XYZ", "ABC")


def test_a_roster_whose_every_ticker_retired_reports_nothing_to_capture(tmp_path, monkeypatch):
    """The same lake with the retire finished: the entry off, nothing owed, the daemon idle.

    This is the one empty cycle that may feed the dead-man, and the other half of the
    test above. Without it a flag that was never true would pass that one.
    """
    rig = _rig(tmp_path, "SPY: {options: true, chain_cadence: 1m, enabled: false}\n")
    _one_ticker_lake(rig)
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert result.segments == ()
    assert result.out_of_span == ()
    assert result.nothing_to_capture is True


def test_a_ticker_disabled_in_place_is_not_captured(tmp_path, monkeypatch):
    """The on/off switch alone, with no span in play, still stops capture."""
    rig = _rig(tmp_path, SPY_ONLY + "XYZ: {options: false, enabled: false}\n")
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)
    result = _cycle(rig, clock)

    tickers_captured = {seg.ticker for seg in result.segments}
    assert tickers_captured == {"SPY"}
    assert "XYZ" not in vendor.quotes[0]


def test_with_no_master_or_spans_file_every_enabled_ticker_is_captured(tmp_path, monkeypatch):
    """The degrade-open rule: a missing reference file widens, never narrows.

    A fresh lake has neither file yet, and capture must not stop for that. This is
    also every existing test in this file, none of which write a master or spans file.
    """
    rig = _rig(tmp_path, WITH_QQQ)
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)
    result = _cycle(rig, clock)

    tickers_captured = {seg.ticker for seg in result.segments}
    assert tickers_captured == {"SPY", "QQQ"}
    assert result.out_of_span == ()


@pytest.mark.parametrize("drift", ["naive span_start", "null span_start"])
def test_a_drifted_spans_file_widens_rather_than_crashing_the_cycle(tmp_path, monkeypatch, drift):
    """A drifted spans file must not be allowed to crash a live cycle.

    Two routes reach the same answer. A retyped ``span_start`` column is refused by the
    read, which checks every pinned column's type (marketlake #551), and ``_live_roster``
    widens on the refusal. A null ``span_start`` passes the read, because every pinned
    field is nullable, and raises out of ``CaptureSpan.contains`` instead. The per-ticker
    guard catches that one. Either way the live capture path keeps the ticker, the answer
    a missing spans file gives.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from lake.capture_spans import CaptureSpans, spans_path
    from lake.security_master import KIND_EQUITY, SecurityMaster, master_path

    rig = _rig(tmp_path, SPY_ONLY)
    master = SecurityMaster()
    iid = master.register(
        kind=KIND_EQUITY, capture_start=FIRST_MINUTE, valid_from=DAY, ticker="SPY"
    )
    master.write(master_path(rig.lake_root))
    spans = CaptureSpans()
    spans.open_span(iid, FIRST_MINUTE, True)
    table = spans.to_table()
    if drift == "naive span_start":
        drifted = pa.schema(
            [
                pa.field("span_start", pa.timestamp("us")) if f.name == "span_start" else f
                for f in table.schema
            ]
        )
        table = table.cast(drifted)
    else:
        starts = table.schema.get_field_index("span_start")
        table = table.set_column(starts, "span_start", pa.nulls(1, pa.timestamp("us", tz="UTC")))
    path = spans_path(rig.lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)

    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)
    clock = ManualClock(start=FIRST_MINUTE)
    result = _cycle(rig, clock)

    assert {seg.ticker for seg in result.segments} == {"SPY"}


# -- the per-cycle client is closed ------------------------------------------------------


class _ClosingVendor(_Vendor):
    """A vendor that counts its closes, the way ``SchwabVendor.close`` frees its client.

    It also records how many closes had happened by each request, since a real client that
    was closed first refuses every request, and the cycle would then journal only gaps.
    """

    def __init__(self) -> None:
        super().__init__()
        self.closed = 0
        self.closed_at_request: list[int] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self.closed_at_request.append(self.closed)
        return super().get_chain(
            symbol, from_date=from_date, to_date=to_date, strike_count=strike_count
        )

    def get_quotes(self, symbols):
        self.closed_at_request.append(self.closed)
        return super().get_quotes(symbols)

    def close(self) -> None:
        self.closed += 1


def test_the_production_entry_closes_the_client_it_built(tmp_path, monkeypatch):
    """Each cycle builds a new client, so each cycle closes the one it built.

    Marketlake #532 lets a cycle open one connection per request in flight, and authlib's client
    is a reference cycle that frees its sockets only when the cyclic collector runs. So the
    connections are closed when the cycle ends rather than left for the collector.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    built: list[_ClosingVendor] = []

    def build(path: Path) -> _ClosingVendor:
        built.append(_ClosingVendor())
        return built[-1]

    _wire(monkeypatch, rig, build)
    clock = ManualClock(start=FIRST_MINUTE)
    _cycle(rig, clock)
    clock.advance(60)
    _cycle(rig, clock)

    assert [vendor.closed for vendor in built] == [1, 1]
    # Every request went out before the close, not after it.
    assert all(v.closed_at_request and set(v.closed_at_request) == {0} for v in built)


def test_the_client_is_closed_when_the_cycle_raises(tmp_path, monkeypatch):
    rig = _rig(tmp_path, SPY_ONLY)
    vendor = _ClosingVendor()
    _wire(monkeypatch, rig, lambda path: vendor)

    def broken(*args, **kwargs):
        raise RuntimeError("the cycle broke")

    monkeypatch.setattr(capture, "run_cycle", broken)
    with pytest.raises(RuntimeError, match="the cycle broke"):
        _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert vendor.closed == 1


# -- a backup setting never stops capture -------------------------------------------


@pytest.mark.parametrize(
    "target",
    [
        "s3://lake-backup/lake",  # a bucket with none of its three keys
        "s3://Legacy_Bucket/lake",  # a bucket name S3 refuses
        "smb://nas/share",  # a scheme that is neither a path nor a bucket
    ],
)
def test_a_bad_backup_setting_still_captures(tmp_path, monkeypatch, target):
    """Only the nightly backup reads ``backup_target``, and capture loads the same file.

    The entry reloads ``config.yaml`` every cycle, so a load that refused a backup
    setting would stop capture for the rest of the session, and a minute not captured
    is gone. The bucket checks run when a bucket job runs, never here.
    """
    rig = _rig(tmp_path, SPY_ONLY)
    text = rig.config.read_text()
    rig.config.write_text(
        text.replace(f"backup_target: {tmp_path / 'ssd'}", f"backup_target: {target}")
    )
    assert f"backup_target: {target}" in rig.config.read_text()
    vendor = _Vendor()
    _wire(monkeypatch, rig, lambda path: vendor)

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert result.segment(CHAINS, "SPY").row_kind == journal.ROW_KIND_DATA
    assert vendor.chains == ["SPY"]


# -- a token file the build cannot use costs the minute, not the daemon -------------------

# Each way a token file defeats the real ``SchwabVendor.from_token``, and the class the
# build raises for it (marketlake #702). A missing file is what a VM whose first-boot pull
# failed holds, a file the account cannot read is what a pull run as root leaves, and the
# other two are a file that is not ``schwab-py``'s token.


def _missing(token: Path) -> None:
    token.unlink()


def _not_json(token: Path) -> None:
    token.write_text("not a token")


def _no_token_key(token: Path) -> None:
    token.write_text(json.dumps({"creation_timestamp": FRESH_MINTED}))


def _unreadable(token: Path) -> None:
    token.chmod(0)


UNUSABLE_TOKEN_FILES = [
    pytest.param(_missing, "FileNotFoundError", id="missing"),
    pytest.param(_unreadable, "PermissionError", id="unreadable"),
    pytest.param(_not_json, "JSONDecodeError", id="not-json"),
    pytest.param(_no_token_key, "KeyError", id="no-token-key"),
]


@pytest.mark.parametrize(("spoil", "build_error"), UNUSABLE_TOKEN_FILES)
def test_a_token_file_the_build_cannot_use_gaps_every_surface(
    tmp_path, monkeypatch, capsys, spoil, build_error
):
    """The real build fails, and the cycle records that failure instead of raising it.

    Raised, the build's failure ended the daemon's loop, and the service manager restarted
    it into the same raise every capture minute. Here the cycle returns, with a gap row on
    every surface carrying ``token_file_unreadable``. The watchdog folds that into "token
    dead", and the daemon pulls on it. The cycle neither lands data nor reports nothing to
    capture, so the capture dead-man stays unfed and pages as it does for a dead token.

    The real ``SchwabVendor.from_token`` runs, so the exception classes are the ones
    production meets rather than ones a stub chose.
    """
    if spoil is _unreadable and os.geteuid() == 0:
        pytest.skip("root reads a file whatever its mode")
    rig = _rig(tmp_path, WITH_XYZ)
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(rig.plan))
    spoil(rig.token)

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert _kinds(result) == {
        (CHAINS, "SPY", journal.ROW_KIND_GAP, "token_file_unreadable"),
        (QUOTES, "SPY", journal.ROW_KIND_GAP, "token_file_unreadable"),
        (QUOTES, "XYZ", journal.ROW_KIND_GAP, "token_file_unreadable"),
    }
    assert result.errors == ()
    assert not any(segment.landed_data for segment in result.segments)
    assert not result.nothing_to_capture
    # The rows on disk carry the class too, which is what the dashboard and the battery read.
    rows = [
        row for segment in result.segments for row in journal.read_segment(segment.path).to_pylist()
    ]
    assert rows
    assert {row["error_class"] for row in rows} == {"token_file_unreadable"}
    # One line naming the build's class, and nothing from the file or its path.
    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "token file" in line]
    assert lines == [
        f"capture: the token file could not be read ({build_error}), so this cycle "
        "gaps every surface as token_file_unreadable"
    ]
    assert str(rig.token) not in err
    assert str(tmp_path) not in err


def _os_error_on(path: Path) -> OSError:
    return OSError(errno.EIO, "the build refused", str(path))


@pytest.mark.parametrize(
    "raised",
    [
        pytest.param(_os_error_on, id="OSError"),
        pytest.param(lambda path: ValueError("the build refused"), id="ValueError"),
        pytest.param(lambda path: KeyError("the build refused"), id="KeyError"),
        pytest.param(lambda path: TypeError("the build refused"), id="TypeError"),
    ],
)
def test_each_class_the_build_can_raise_is_recorded_as_a_gap(tmp_path, monkeypatch, raised):
    # The four the build is caught for, each from a stub so a class the real file read
    # never raises today, such as ``attach_timing``'s own ``TypeError``, is covered too.
    # The ``OSError`` names the token file, as the real read's does.
    rig = _rig(tmp_path, SPY_ONLY)

    def refuse(path: Path) -> _Vendor:
        raise raised(path)

    _wire(monkeypatch, rig, refuse)

    result = _cycle(rig, ManualClock(start=FIRST_MINUTE))

    assert _kinds(result) == {
        (CHAINS, "SPY", journal.ROW_KIND_GAP, "token_file_unreadable"),
        (QUOTES, "SPY", journal.ROW_KIND_GAP, "token_file_unreadable"),
    }


def test_a_build_failure_outside_the_four_still_raises(tmp_path, monkeypatch):
    # Only the token file's failures are caught. A raise of any other class is a broken
    # machine rather than a bad file, and it keeps ending the call as it did.
    rig = _rig(tmp_path, SPY_ONLY)

    def refuse(path: Path) -> _Vendor:
        raise RuntimeError("not a file problem")

    _wire(monkeypatch, rig, refuse)

    with pytest.raises(RuntimeError, match="not a file problem"):
        _cycle(rig, ManualClock(start=FIRST_MINUTE))


@pytest.mark.parametrize("other", ["cacert.pem", None], ids=["another-file", "no-file"])
def test_an_os_error_that_does_not_name_the_token_file_still_raises(tmp_path, monkeypatch, other):
    # The build reads more than the token file. An ``OSError`` from any other file, or
    # from no file, is not something a token pull repairs, so it ends the call as before
    # rather than paging "token dead" and spawning pulls that cannot help.
    rig = _rig(tmp_path, SPY_ONLY)

    def refuse(path: Path) -> _Vendor:
        if other is None:
            raise OSError(errno.EIO, "the build refused")
        raise FileNotFoundError(errno.ENOENT, "No such file or directory", str(tmp_path / other))

    _wire(monkeypatch, rig, refuse)

    with pytest.raises(OSError, match="the build refused|No such file"):
        _cycle(rig, ManualClock(start=FIRST_MINUTE))


# authlib's ``OAuth2Client.__del__`` deletes a ``session`` the failed constructor never set,
# and the collector reports that as an unraisable ``AttributeError``. It is the library's
# cleanup of the half-built client, not something this test checks.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_a_missing_ca_bundle_raises_rather_than_reading_as_a_bad_token(
    tmp_path, monkeypatch, capsys
):
    # The real build with a good token file. httpx loads the CA bundle ``SSL_CERT_FILE``
    # names while the client is built, so a missing one raises ``FileNotFoundError`` from
    # inside ``from_token``. That is a broken machine, and caught it would log "the token
    # file could not be read", page "token dead" and spawn pulls that cannot repair it.
    rig = _rig(tmp_path, SPY_ONLY)
    # A token ``schwab-py`` builds a client from, unexpired, so the build refreshes nothing.
    token = {
        "access_token": "access",
        "refresh_token": FRESH,
        "token_type": "Bearer",
        "expires_at": FRESH_MINTED + 30 * 60,
    }
    rig.token.write_text(json.dumps({"creation_timestamp": FRESH_MINTED, "token": token}))
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(rig.plan))
    missing = tmp_path / "no-such-ca-bundle.pem"
    monkeypatch.setenv("SSL_CERT_FILE", str(missing))

    def no_cycle(*args, **kwargs):
        raise AssertionError("the build succeeded, so the cycle would reach the network")

    monkeypatch.setattr(capture, "run_cycle", no_cycle)

    with pytest.raises(FileNotFoundError):
        _cycle(rig, ManualClock(start=FIRST_MINUTE))
    assert "token file" not in capsys.readouterr().err


def test_a_config_that_will_not_load_still_raises_beside_a_missing_token(tmp_path, monkeypatch):
    # The catch wraps the build alone. A config read inside it would turn a broken config
    # into gap rows the operator reads as a token problem.
    rig = _rig(tmp_path, SPY_ONLY)
    rig.token.unlink()
    rig.config.unlink()
    with pytest.raises(ConfigError):
        _cycle(rig, ManualClock(start=FIRST_MINUTE))


def test_the_stand_in_is_never_retried_and_closes_quietly():
    # ``is_transient_failure`` matching the stand-in's exception would send every window
    # and the quote batch twice a minute for nothing. A ``close`` that raised would escape
    # from the cycle's ``finally`` and bring the crash back.
    vendor = capture._token_file_unreadable(FileNotFoundError("gone"))
    with pytest.raises(capture.TokenFileUnreadable) as refused:
        vendor.get_quotes(["SPY"])
    assert not is_transient_failure(refused.value)
    assert type(refused.value).__mro__[1] is Exception
    capture._close_vendor(vendor)
    for call in (
        lambda: vendor.get_chain("SPY"),
        lambda: vendor.get_minute_bars("SPY", start=FIRST_MINUTE, end=FIRST_MINUTE),
        lambda: vendor.get_daily_bars("SPY", start=FIRST_MINUTE, end=FIRST_MINUTE),
        vendor.token_mint_time,
    ):
        with pytest.raises(capture.TokenFileUnreadable):
            call()
