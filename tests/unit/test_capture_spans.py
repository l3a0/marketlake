"""Capture spans, value-only logic.

These tests decide everything from values: opening and closing spans, the half-open
scope test, the two spans a rejoin produces, and the backfill from an existing master.
The parquet round-trip on disk is a component test.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from lake.calendar import MARKET_TZ
from lake.capture_spans import (
    CaptureSpan,
    CaptureSpans,
    NoOpenSpan,
    OpenSpanExists,
    UnsupportedSpansSchemaVersion,
    build_from_master,
    spans_of_ticker,
)
from lake.security_master import SecurityMaster

# 2019-01-02 09:30 ET, in UTC. Capture windows begin at a specific minute.
START = datetime(2019, 1, 2, 14, 30, tzinfo=UTC)


def test_open_span_records_an_open_window():
    spans = CaptureSpans()
    spans.open_span(1, START, options=True)
    assert spans.has_open_span(1) is True
    only = spans.spans_of(1)
    assert len(only) == 1
    assert only[0] == CaptureSpan(instrument_id=1, start=START, end=None, options=True)


def test_opening_a_second_span_while_one_is_open_is_refused():
    spans = CaptureSpans()
    spans.open_span(1, START, options=False)
    with pytest.raises(OpenSpanExists):
        spans.open_span(1, START, options=False)


def test_close_span_sets_the_end():
    spans = CaptureSpans()
    spans.open_span(1, START, options=False)
    end = datetime(2019, 1, 10, 21, 0, tzinfo=UTC)
    spans.close_span(1, end)
    assert spans.has_open_span(1) is False
    assert spans.spans_of(1)[0].end == end


def test_closing_with_no_open_span_is_refused():
    spans = CaptureSpans()
    with pytest.raises(NoOpenSpan):
        spans.close_span(1, START)


def test_closing_before_the_start_is_refused():
    spans = CaptureSpans()
    spans.open_span(1, START, options=False)
    with pytest.raises(ValueError):
        spans.close_span(1, START - _one_minute())


def test_contains_is_half_open():
    span = CaptureSpan(
        instrument_id=1,
        start=START,
        end=datetime(2019, 1, 3, 14, 30, tzinfo=UTC),
        options=False,
    )
    assert span.contains(START) is True  # start included
    assert span.contains(START - _one_minute()) is False  # before start
    assert span.contains(span.end) is False  # end excluded
    assert span.contains(span.end - _one_minute()) is True


def test_an_open_span_contains_every_instant_at_or_after_start():
    span = CaptureSpan(instrument_id=1, start=START, end=None, options=False)
    assert span.contains(START) is True
    assert span.contains(datetime(2030, 1, 1, 0, 0, tzinfo=UTC)) is True
    assert span.contains(START - _one_minute()) is False


def test_in_scope_spans_a_rejoin_with_a_gap_between():
    # Captured, retired, rejoined: two spans with an out-of-scope gap between them.
    spans = CaptureSpans()
    spans.open_span(1, START, options=False)
    retire = datetime(2019, 1, 10, 21, 0, tzinfo=UTC)
    spans.close_span(1, retire)
    rejoin = datetime(2019, 1, 20, 14, 30, tzinfo=UTC)
    spans.open_span(1, rejoin, options=False)

    assert spans.in_scope(1, START) is True  # first span
    assert spans.in_scope(1, datetime(2019, 1, 15, 14, 30, tzinfo=UTC)) is False  # the gap
    assert spans.in_scope(1, rejoin) is True  # second span
    assert len(spans.spans_of(1)) == 2


def test_spans_covering_finds_every_instrument_live_at_an_instant():
    spans = CaptureSpans()
    spans.open_span(1, START, options=True)
    spans.open_span(2, START, options=False)
    spans.close_span(2, datetime(2019, 1, 5, 21, 0, tzinfo=UTC))  # 2 retired early
    at = datetime(2019, 1, 8, 14, 30, tzinfo=UTC)
    covering = spans.spans_covering(at)
    assert {s.instrument_id for s in covering} == {1}


def test_a_market_time_instant_compares_correctly_against_a_utc_span():
    # The guard holds market-time closes. A span retired at 16:02 ET still covers 16:00 ET.
    spans = CaptureSpans()
    open_et = datetime(2019, 1, 2, 9, 30, tzinfo=MARKET_TZ)
    spans.open_span(1, open_et, options=False)
    retire_et = datetime(2019, 1, 2, 16, 2, tzinfo=MARKET_TZ)
    spans.close_span(1, retire_et)
    equity_close = datetime(2019, 1, 2, 16, 0, tzinfo=MARKET_TZ)
    after_close = datetime(2019, 1, 2, 16, 3, tzinfo=MARKET_TZ)
    assert spans.in_scope(1, equity_close) is True
    assert spans.in_scope(1, after_close) is False


def test_from_table_rejects_an_unknown_schema_version():
    spans = CaptureSpans([CaptureSpan(1, START, None, False)])
    table = spans.to_table()
    bumped = table.set_column(
        table.schema.get_field_index("schema_version"),
        "schema_version",
        __import__("pyarrow").array([999], type=__import__("pyarrow").int32()),
    )
    with pytest.raises(UnsupportedSpansSchemaVersion):
        CaptureSpans.from_table(bumped)


def test_build_from_master_opens_one_span_per_rostered_instrument_at_capture_start():
    master = SecurityMaster()
    spy = master.register(
        kind="equity", capture_start=START, valid_from=date(2019, 1, 2), ticker="SPY"
    )
    later = datetime(2019, 2, 1, 14, 30, tzinfo=UTC)
    qqq = master.register(
        kind="equity", capture_start=later, valid_from=date(2019, 2, 1), ticker="QQQ"
    )

    spans = build_from_master(master, {"SPY": True, "QQQ": False}, on=date(2019, 2, 1))

    assert spans.spans_of(spy)[0] == CaptureSpan(spy, START, None, True)
    assert spans.spans_of(qqq)[0] == CaptureSpan(qqq, later, None, False)


def test_build_from_master_skips_an_instrument_the_roster_no_longer_names():
    """A ticker retired by hand before capture spans existed must not get an open span.

    Before ``retire`` existed, the only way to stop capturing a ticker was to remove its
    roster entry, and nothing recorded when. Opening an unbounded span for it here would
    read as "still capturing," so every reader would treat a long-retired instrument as
    owed forever. Skipping it instead leaves it with no span, out of scope everywhere,
    which is the correct answer.
    """
    master = SecurityMaster()
    spy = master.register(
        kind="equity", capture_start=START, valid_from=date(2019, 1, 2), ticker="SPY"
    )
    gone = master.register(
        kind="equity", capture_start=START, valid_from=date(2019, 1, 2), ticker="GONE"
    )

    # GONE is in the master but was removed from tickers.yaml by hand, long before this
    # code existed. The roster passed to the seed run does not name it.
    spans = build_from_master(master, {"SPY": True}, on=date(2019, 1, 2))

    assert spans.spans_of(spy) != ()
    assert spans.spans_of(gone) == ()
    assert gone not in spans.instrument_ids()


def test_spans_of_ticker_resolves_and_never_raises():
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=START, valid_from=date(2019, 1, 2), ticker="SPY"
    )
    spans = build_from_master(master, {"SPY": False}, on=date(2019, 1, 2))

    got = spans_of_ticker(spans, master, "SPY", date(2019, 1, 2))
    assert got is not None and got[0].instrument_id == iid

    assert spans_of_ticker(spans, master, "NOPE", date(2019, 1, 2)) is None
    assert spans_of_ticker(None, master, "SPY", date(2019, 1, 2)) is None
    assert spans_of_ticker(spans, None, "SPY", date(2019, 1, 2)) is None


def _one_minute():
    from datetime import timedelta

    return timedelta(minutes=1)
