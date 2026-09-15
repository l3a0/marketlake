"""The parser's schema-drift observer, decided from values alone.

These hand ``SchemaDriftObserver`` cycle outcomes and read back what it says should page.
No file, process, or network is crossed, so they sit in the unit tier. The scan that
produces ``SegmentOutcome.routed_columns`` from a real vendor payload is covered in
``tests/unit/test_journal_schema.py``, and the page this observer feeds is covered in
``tests/component/test_schema_drift.py``.

What they cover is the state machine, which is where the settled rules live. There are five.

1. A column that starts routing is reported once, when it starts.
2. A column that keeps routing is not reported again, because a cycle a minute against a
   forty-a-day cap would spend the whole cap in forty minutes.
3. A column that stops routing re-arms, so a drift that is fixed and returns is reported
   twice.
4. One column drifting across many tickers is one report carrying them all, because a
   vendor retype reaches every ticker on the same cycle.
5. Only data segments are evidence. A cycle that gapped a surface says nothing about the
   vendor's payload and must leave that surface's state where it stood.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.journal import CHAINS_SURFACE, QUOTES_SURFACE, ROW_KIND_DATA, ROW_KIND_GAP
from lake.schema_drift import ColumnDrift, SchemaDriftObserver

SLOT = datetime(2026, 9, 14, 14, 31, tzinfo=UTC)


def _segment(
    surface: str = CHAINS_SURFACE,
    ticker: str = "SPY",
    *,
    routed: tuple[str, ...] = (),
    row_kind: str = ROW_KIND_DATA,
) -> SegmentOutcome:
    """One segment outcome, the shape a cycle result carries."""
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path("segment.arrows"),
        partition=f"{surface}/ticker={ticker}/date=2026-09-14/segment.arrows",
        row_kind=row_kind,
        rows=1,
        error_class=None if row_kind == ROW_KIND_DATA else "http_429",
        fetched_at=None,
        routed_columns=routed,
    )


def _cycle(*segments: SegmentOutcome, errors: tuple[SegmentError, ...] = ()) -> CycleResult:
    """One cycle's result carrying the given segments."""
    return CycleResult(snap_ts=SLOT, segments=segments, errors=errors)


def test_a_column_that_starts_routing_is_reported_once_when_it_starts():
    observer = SchemaDriftObserver()
    drifted = observer.observe(_cycle(_segment(routed=("open_interest",))))
    assert drifted == (ColumnDrift(CHAINS_SURFACE, "open_interest", ("SPY",)),)


def test_the_same_drift_on_the_next_cycle_is_not_reported_again():
    """The cadence rule, and the arithmetic behind it.

    Capture runs a cycle a minute and ``alert.DEFAULT_DAILY_CAP`` is forty pages a day, so
    a drift that persists would spend the whole cap in forty minutes. The page it swallowed
    could be the auth-death page, so this producer must not cause that storm.
    """
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest",))))
    assert observer.observe(_cycle(_segment(routed=("open_interest",)))) == ()
    assert observer.observe(_cycle(_segment(routed=("open_interest",)))) == ()


def test_a_drift_that_clears_and_returns_is_reported_a_second_time():
    """The state resets when the column stops routing, so the vendor's second change speaks.

    Without the reset a vendor that drifted once would be reported once ever, and a fix
    followed by the same regression a week later would reach nobody.
    """
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest",))))
    assert observer.observe(_cycle(_segment())) == ()
    assert observer.observe(_cycle(_segment(routed=("open_interest",)))) == (
        ColumnDrift(CHAINS_SURFACE, "open_interest", ("SPY",)),
    )


def test_one_column_across_four_tickers_is_one_report_carrying_all_four():
    """The collapsing rule. A vendor retype reaches every ticker on the same cycle.

    Reporting per ticker would scale the page count with the roster while the fact stayed
    one fact, which is the shape ``compact._page_drift`` already folds for the same reason.
    """
    observer = SchemaDriftObserver()
    drifted = observer.observe(
        _cycle(
            *(
                _segment(ticker=ticker, routed=("open_interest",))
                for ticker in ("SPY", "QQQ", "IWM", "DIA")
            )
        )
    )
    expected = ColumnDrift(CHAINS_SURFACE, "open_interest", ("SPY", "QQQ", "IWM", "DIA"))
    assert drifted == (expected,)


def test_a_second_ticker_joining_a_drift_already_reported_says_nothing_new():
    """The transition is the column's, not the ticker's.

    A retype that reached one ticker's payload first and the rest a minute later is one
    vendor change still. Keying the state per ticker would page again for every ticker
    that joined, which on the roster the design sizes for is the storm the fold prevents.
    """
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(ticker="SPY", routed=("open_interest",))))
    later = _cycle(
        _segment(ticker="SPY", routed=("open_interest",)),
        _segment(ticker="QQQ", routed=("open_interest",)),
    )
    assert observer.observe(later) == ()


def test_a_second_column_starting_later_is_reported_on_its_own_cycle():
    """Only what is new is reported, so a widening drift does not re-report what stands."""
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest",))))
    drifted = observer.observe(_cycle(_segment(routed=("bid", "open_interest"))))
    assert drifted == (ColumnDrift(CHAINS_SURFACE, "bid", ("SPY",)),)


def test_both_surfaces_drifting_at_once_are_reported_together():
    """One vendor change reaching both surfaces is still one cycle's finding.

    The report names the surface per column, so the page that folds them can say which
    surface each column belongs to rather than running two columns of the same name
    together.
    """
    observer = SchemaDriftObserver()
    drifted = observer.observe(
        _cycle(
            _segment(CHAINS_SURFACE, routed=("open_interest",)),
            _segment(QUOTES_SURFACE, routed=("bid",)),
        )
    )
    assert drifted == (
        ColumnDrift(CHAINS_SURFACE, "open_interest", ("SPY",)),
        ColumnDrift(QUOTES_SURFACE, "bid", ("SPY",)),
    )


def test_a_gap_segment_is_never_evidence_of_drift():
    """A gap row carries no vendor observation, so nothing on one can be the vendor's.

    ``journal._routed_column`` already refuses to route onto a gap row, which makes this
    unreachable from production. It is held here because the observer is the second reader
    of that rule, and a reader that trusted the field without the row kind would turn a
    future gap-row writer's own value into a page about the vendor.
    """
    observer = SchemaDriftObserver()
    gapped = _segment(routed=("open_interest",), row_kind=ROW_KIND_GAP)
    assert observer.observe(_cycle(gapped)) == ()


def test_a_surface_the_cycle_gapped_keeps_the_state_it_had():
    """An outage is not a fix. A cycle with no data for a surface is no evidence about it.

    Treating a gapped cycle as "the column stopped routing" would re-arm every column that
    is drifting, and the first cycle after capture came back would report the same drift
    again. A rate limit that gaps the roster for an hour would then report on every
    recovery.
    """
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest",))))
    assert observer.observe(_cycle(_segment(row_kind=ROW_KIND_GAP))) == ()
    assert observer.observe(_cycle(_segment(routed=("open_interest",)))) == ()


def test_a_cycle_that_journalled_nothing_at_all_keeps_the_state_it_had():
    """The same rule for the cycle that wrote no segment, only errors."""
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest",))))
    failed = _cycle(errors=(SegmentError(CHAINS_SURFACE, "SPY", "disk_full"),))
    assert observer.observe(failed) == ()
    assert observer.observe(_cycle(_segment(routed=("open_interest",)))) == ()


def test_one_surface_drifting_leaves_the_others_state_alone():
    """The state is per surface, so a quotes cycle never re-arms a chains column."""
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(CHAINS_SURFACE, routed=("open_interest",))))
    assert observer.observe(_cycle(_segment(QUOTES_SURFACE))) == ()
    assert observer.observe(_cycle(_segment(CHAINS_SURFACE, routed=("open_interest",)))) == ()


def test_an_ordinary_cycle_reports_nothing():
    """The steady state. ``extra`` was non-null on zero of the lake's sealed rows."""
    observer = SchemaDriftObserver()
    ordinary = _cycle(
        _segment(CHAINS_SURFACE, "SPY"),
        _segment(QUOTES_SURFACE, "SPY"),
        _segment(QUOTES_SURFACE, "QQQ"),
    )
    assert observer.observe(ordinary) == ()
    assert observer.observe(ordinary) == ()


# -- the evidence is per ticker, not per surface -------------------------------

# The rule these hold is what separates one page from a page every other minute. The
# observer shipped with the evidence counted per surface, which looks right and is not. A
# surface produces data every cycle as long as any ticker on it does, so a drift confined to
# one ticker had the surface's own health read back as proof the drift had cleared. Every
# ordinary transient gap on that ticker then read as a clearance and its return as a fresh
# drift.


def test_a_drifting_ticker_that_gaps_does_not_re_arm_while_the_surface_stays_healthy():
    """The defect this section exists for, at its smallest.

    AAPL is retyped and MSFT is clean. The chains surface lands data every cycle because
    MSFT does, so a surface-level evidence rule sees a healthy surface and clears AAPL's
    column on the minute AAPL gaps. The only ticker that could speak about AAPL's payload
    is AAPL, and it said nothing.
    """
    observer = SchemaDriftObserver()
    drifting = (_segment(ticker="AAPL", routed=("open_interest",)), _segment(ticker="MSFT"))
    gapped = (_segment(ticker="AAPL", row_kind=ROW_KIND_GAP), _segment(ticker="MSFT"))

    assert len(observer.observe(_cycle(*drifting))) == 1
    assert observer.observe(_cycle(*gapped)) == ()
    assert observer.observe(_cycle(*drifting)) == ()


def test_a_flapping_ticker_pages_once_for_one_unchanging_vendor_fact():
    """The arithmetic, run out over the cycles the cap is measured against.

    One retype that never changes, on a ticker whose fetch fails every other minute. Under
    the surface-level rule this sent five pages in ten minutes, which spends
    ``alert.DEFAULT_DAILY_CAP`` of forty inside eighty session minutes and swallows every
    page any other producer owed for the rest of the day.
    """
    observer = SchemaDriftObserver()
    pages = 0
    for cycle in range(10):
        landed = cycle % 2 == 0
        aapl = (
            _segment(ticker="AAPL", routed=("open_interest",))
            if landed
            else _segment(ticker="AAPL", row_kind=ROW_KIND_GAP)
        )
        if observer.observe(_cycle(aapl, _segment(ticker="MSFT"))):
            pages += 1
    assert pages == 1


def test_a_column_clears_only_when_the_ticker_that_drifted_it_lands_clean_data():
    """The reset needs the drifting ticker's own word, and nothing else substitutes.

    A clean cycle from every other ticker on the surface is not evidence about this one.
    Once AAPL itself lands a data row without the column, the column clears and the next
    drift is a new fact that pages again.
    """
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(ticker="AAPL", routed=("open_interest",))))

    # MSFT joining the surface clean says nothing about AAPL.
    assert observer.observe(_cycle(_segment(ticker="MSFT"))) == ()
    # AAPL's own clean data row is what clears it.
    assert observer.observe(_cycle(_segment(ticker="AAPL"), _segment(ticker="MSFT"))) == ()
    assert observer.observe(_cycle(_segment(ticker="AAPL", routed=("open_interest",)))) == (
        ColumnDrift(CHAINS_SURFACE, "open_interest", ("AAPL",)),
    )


def test_a_drifting_ticker_retired_clears_the_column_it_held():
    """A ticker off the roster can never produce the evidence that would clear it.

    Without a release rule its column would stay drifting for the life of the process, and
    a genuine later retype of that column on another ticker would then never page. A cycle
    that no longer names the ticker at all is what says it is gone, since a cycle writes
    data or a gap for every ticker it still carries.
    """
    observer = SchemaDriftObserver()
    observer.observe(
        _cycle(_segment(ticker="AAPL", routed=("open_interest",)), _segment(ticker="MSFT"))
    )

    # AAPL is retired, so it appears in no segment at all.
    assert observer.observe(_cycle(_segment(ticker="MSFT"))) == ()
    # The column is re-armed, so MSFT drifting it now is a new fact.
    assert observer.observe(_cycle(_segment(ticker="MSFT", routed=("open_interest",)))) == (
        ColumnDrift(CHAINS_SURFACE, "open_interest", ("MSFT",)),
    )


def test_a_ticker_whose_segment_could_not_be_written_is_not_a_retirement():
    """A write failure leaves the ticker on the roster, so it must not clear a drift.

    ``SegmentError`` is the disk refusing a segment rather than the roster dropping a
    ticker. Reading it as a retirement would clear the column and page again on the next
    cycle that could write.
    """
    observer = SchemaDriftObserver()
    observer.observe(
        _cycle(_segment(ticker="AAPL", routed=("open_interest",)), _segment(ticker="MSFT"))
    )
    unwritable = _cycle(
        _segment(ticker="MSFT"), errors=(SegmentError(CHAINS_SURFACE, "AAPL", "disk_full"),)
    )

    assert observer.observe(unwritable) == ()
    assert observer.observe(_cycle(_segment(ticker="AAPL", routed=("open_interest",)))) == ()


def test_the_state_empties_when_every_column_clears():
    """The state is not a leak. A surface with nothing drifting holds nothing."""
    observer = SchemaDriftObserver()
    observer.observe(_cycle(_segment(routed=("open_interest", "bid"))))
    assert observer.observe(_cycle(_segment())) == ()
    assert observer._routing == {}


# -- the order the page prints in ----------------------------------------------

# The page prints these in the order they arrive and ``PAGE_COLUMN_CAP`` cuts the tail, so
# the order decides which column names an operator sees. Every fixture above happens to
# feed chains before quotes and one column at a time, which would let payload order through
# unnoticed.


def test_the_surfaces_come_back_sorted_whatever_order_the_cycle_wrote_them_in():
    """A cycle that wrote quotes first still reports chains first.

    Segment order is roster order, so without sorting the page's surface order would follow
    whichever ticker the cycle reached first. The same vendor change would then print
    differently from one cycle to the next.
    """
    observer = SchemaDriftObserver()
    quotes_first = _cycle(
        _segment(QUOTES_SURFACE, routed=("bid",)),
        _segment(CHAINS_SURFACE, routed=("open_interest",)),
    )
    assert observer.observe(quotes_first) == (
        ColumnDrift(CHAINS_SURFACE, "open_interest", ("SPY",)),
        ColumnDrift(QUOTES_SURFACE, "bid", ("SPY",)),
    )


def test_two_columns_starting_on_one_surface_come_back_sorted():
    """Two columns of one surface in one cycle, which no other case here produces.

    ``journal.routed_columns`` already sorts, so an unsorted step here would only show on a
    cycle where two tickers drifted different columns. That is the shape a vendor retyping
    one block produces, and it is the shape the cap cuts.
    """
    observer = SchemaDriftObserver()
    spread = _cycle(
        _segment(ticker="SPY", routed=("volume",)),
        _segment(ticker="QQQ", routed=("ask",)),
        _segment(ticker="IWM", routed=("bid",)),
    )
    assert [drift.column for drift in observer.observe(spread)] == ["ask", "bid", "volume"]
