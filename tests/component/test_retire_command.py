"""The retire command across real files.

These run the retire core against a throwaway lake and roster, with a manual clock and
no vendor. Retiring closes a ticker's open capture span and either disables the roster
entry or removes it. The span end is what the guard and the walk read later.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.capture_spans import CaptureSpans, spans_path
from lake.cassette import Cassette, Interaction
from lake.onboard import onboard
from lake.retire import RetireError, retire
from lake.security_master import SecurityMaster, master_path
from lake.tickers import load_tickers, upsert_ticker
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor


def _quote_vendor(ticker: str) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="quotes",
                    params={"symbols": [ticker]},
                    status=200,
                    body={ticker: {"realtime": True, "quote": {"bidPrice": 1.0}}},
                ),
            )
        )
    )


ONBOARD = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)  # 11:00 ET
RETIRE = datetime(2026, 8, 27, 20, 5, tzinfo=UTC)  # 16:05 ET


def _setup(lake: Path, tickers: Path, *, ticker: str = "SPY", options: bool = False) -> int:
    """Onboard-equivalent state, built directly: a master, an open span, a roster entry."""
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=ONBOARD, valid_from=ONBOARD.date(), ticker=ticker
    )
    master.write(master_path(lake))
    spans = CaptureSpans()
    spans.open_span(iid, ONBOARD, options)
    spans.write(spans_path(lake))
    upsert_ticker(ticker, options=options, path=tickers)
    return iid


def test_retiring_before_the_seed_run_has_happened_refuses(tmp_path: Path):
    """The retire-side mirror of the same onboard guard: never silently lose history."""
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    master.register(kind="equity", capture_start=ONBOARD, valid_from=ONBOARD.date(), ticker="SPY")
    master.write(master_path(lake))
    # No spans file written: this lake has an instrument but was never seeded.
    upsert_ticker("SPY", options=False, path=tickers)

    with pytest.raises(RetireError, match="seed_spans"):
        retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)


def test_retire_disables_in_place_and_closes_the_span(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    report = retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    assert report.already_retired is False
    assert report.removed is False
    # The entry stays in the roster but is now disabled.
    assert load_tickers(tickers).get("SPY").enabled is False
    # The span is closed at the retire instant.
    spans = CaptureSpans.read(spans_path(lake))
    assert spans.has_open_span(iid) is False
    assert spans.spans_of(iid)[0].end == RETIRE


def test_retire_remove_deletes_the_entry_and_closes_the_span(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    report = retire(
        "SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers, remove=True
    )

    assert report.removed is True
    assert load_tickers(tickers).symbols == ()  # removing the last ticker empties the roster
    assert CaptureSpans.read(spans_path(lake)).has_open_span(iid) is False


def test_retiring_an_unknown_ticker_raises(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    _setup(lake, tickers)
    with pytest.raises(RetireError):
        retire("NOPE", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)


def test_re_running_when_already_retired_is_a_no_op(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)
    retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers, remove=True)
    end_after_first = CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].end

    later = datetime(2026, 8, 28, 20, 5, tzinfo=UTC)
    report = retire(
        "SPY", clock=ManualClock(later), lake_root=lake, tickers_path=tickers, remove=True
    )

    assert report.already_retired is True
    assert report.span_end is None
    # The already-closed span is untouched; the second run did not move its end.
    assert CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].end == end_after_first


def test_the_span_is_closed_before_the_roster_change(tmp_path: Path, monkeypatch):
    # Order matters: close the span, then touch the roster. A crash in between must leave
    # the ticker still in the roster with a closed span, never off with an open span.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    import lake.retire as retire_mod

    def boom(*args, **kwargs):
        raise RuntimeError("crash after the span write, before the roster change")

    monkeypatch.setattr(retire_mod, "set_enabled", boom)
    with pytest.raises(RuntimeError):
        retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    # The span was closed (safe, recoverable). The roster still has the ticker enabled.
    assert CaptureSpans.read(spans_path(lake)).has_open_span(iid) is False
    assert load_tickers(tickers).get("SPY").enabled is True


def test_onboard_retire_reonboard_opens_a_second_span(tmp_path: Path):
    # The #77 rejoin case, end to end: a returning ticker keeps its instrument id, opens a
    # second span, and its time away is out of scope rather than owed.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir(parents=True)  # the lake root exists before the first write, as in production
    first = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)  # 11:00 ET
    onboard(
        "SPY",
        clock=ManualClock(first),
        vendor=_quote_vendor("SPY"),
        lake_root=lake,
        tickers_path=tickers,
        options=False,
    )
    retire_at = datetime(2026, 8, 27, 20, 5, tzinfo=UTC)
    retire("SPY", clock=ManualClock(retire_at), lake_root=lake, tickers_path=tickers, remove=True)
    assert load_tickers(tickers).symbols == ()

    rejoin = datetime(2026, 9, 3, 15, 0, tzinfo=UTC)
    report = onboard(
        "SPY",
        clock=ManualClock(rejoin),
        vendor=_quote_vendor("SPY"),
        lake_root=lake,
        tickers_path=tickers,
        options=False,
    )
    assert report.already_registered is True
    assert report.instrument_id == 1  # same instrument across the rejoin
    assert report.capture_start == rejoin  # the fresh span start, not the original

    spans = CaptureSpans.read(spans_path(lake))
    windows = spans.spans_of(1)
    assert len(windows) == 2
    assert windows[0].end == retire_at
    assert windows[1].start == rejoin and windows[1].end is None
    # In scope during both live periods, out of scope for the week away.
    away = datetime(2026, 8, 30, 15, 0, tzinfo=UTC)
    assert spans.in_scope(1, first) is True
    assert spans.in_scope(1, away) is False
    assert spans.in_scope(1, rejoin) is True
