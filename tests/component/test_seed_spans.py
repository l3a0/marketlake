"""The seed command: seeding capture spans from an existing master.

These run the seed core against a throwaway lake, master, and roster, with a manual
clock. No vendor and no wall clock are crossed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lake.capture_spans import CaptureSpans, spans_path
from lake.security_master import KIND_EQUITY, SecurityMaster, master_path
from lake.seed_spans import seed_spans
from lake.tickers import upsert_ticker
from tests.support.clock import ManualClock

NOW = datetime(2026, 9, 2, 15, 0, tzinfo=UTC)  # 11:00 ET
SPY_START = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)  # 09:30 ET
QQQ_START = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)


def test_seed_opens_one_open_span_per_instrument_from_capture_start(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    spy = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    qqq = master.register(
        kind=KIND_EQUITY, capture_start=QQQ_START, valid_from=QQQ_START.date(), ticker="QQQ"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=True, path=tickers)
    upsert_ticker("QQQ", options=False, path=tickers)

    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    assert report.already_seeded is False
    assert report.instrument_count == 2
    spans = CaptureSpans.read(spans_path(lake))
    spy_span = spans.spans_of(spy)[0]
    assert spy_span.start == SPY_START and spy_span.end is None and spy_span.options is True
    qqq_span = spans.spans_of(qqq)[0]
    assert qqq_span.start == QQQ_START and qqq_span.options is False


def test_seed_on_a_fresh_lake_writes_an_empty_spans_file(tmp_path: Path):
    # No master exists yet. The first onboarding will write its own span, so the
    # seed command's only job here is to make the spans file present rather than absent.
    lake = tmp_path / "lake"
    lake.mkdir(parents=True)
    report = seed_spans(
        clock=ManualClock(NOW), lake_root=lake, tickers_path=tmp_path / "tickers.yaml"
    )
    assert report.instrument_count == 0
    assert spans_path(lake).exists()


def test_seed_is_idempotent(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=False, path=tickers)

    first = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert first.already_seeded is False

    second = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert second.already_seeded is True
    assert second.instrument_count == 1


def test_seed_tolerates_a_missing_or_broken_roster(tmp_path: Path):
    # A roster that cannot be read is unknown, not empty. Unknown widens: every
    # instrument still gets an open span, with options defaulted to false, rather than
    # every one of them being skipped as though the roster had named nobody.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    iid = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    master.write(master_path(lake))
    # No tickers.yaml at all.
    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert report.instrument_count == 1
    assert CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].options is False


def test_seed_skips_an_instrument_the_roster_no_longer_names(tmp_path: Path):
    # The migration-safety case: a ticker retired by hand before this code existed must
    # not come back as a permanently open span. It is skipped entirely, out of scope
    # everywhere, rather than resurrected as though it were still being captured.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    spy = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    gone = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="GONE"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=False, path=tickers)  # GONE is not in the roster at all

    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    assert report.instrument_count == 1
    spans = CaptureSpans.read(spans_path(lake))
    assert spans.spans_of(spy) != ()
    assert spans.spans_of(gone) == ()
