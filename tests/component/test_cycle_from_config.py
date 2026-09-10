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

Four claims are pinned.

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
  counters. It reads the roster in the daemon's skipped-slot hook, not in this entry, so
  it is out of this file's reach.
- The entry reloads one more input, the chain plan. Every case here pins it to a
  test-owned path so the date windows are deterministic. No case here fails if that
  re-read breaks.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan, load_chain_plan
from lake.compact import write_chain_plan
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
    true under either reading, rather than pinning the unconditional rebuild by accident.
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


def _wire(monkeypatch, rig: _Rig, build) -> None:
    """Point the entry's two module-level names at the rig, with no real client in path.

    1. ``load_chain_plan`` is pinned to a test-owned path. The plan file is machine-local,
       so the test owns its path rather than the home directory's. The loader itself stays
       the real one, called once per cycle.
    2. ``SchwabVendor`` is replaced by a stub. The production factory builds a live
       ``schwab-py`` client, and a test has no token to build one from.

    ``build`` receives the token path the entry passed, so a case can answer by what the
    file holds at that moment.
    """
    monkeypatch.setattr(capture, "load_chain_plan", lambda: load_chain_plan(rig.plan))

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            return build(Path(token_path))

    monkeypatch.setattr(capture, "SchwabVendor", _Stub)


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

    # The next cycle chained both. The two are compared order-free on purpose. Tickers are
    # fetched sequentially in roster order today, and the design fires them as parallel
    # per-ticker workers later, so their relative order is not this claim's to pin.
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
    assert vendor.windows == [WHOLE, TAIL, WHOLE, FIRST_HALF, SECOND_HALF, TAIL]
