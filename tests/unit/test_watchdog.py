"""The watchdog's counters and the pages they raise."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.watchdog import Surface, Watchdog

ET = ZoneInfo("America/New_York")
SLOT = datetime(2026, 9, 2, 10, 0, tzinfo=ET)


def _at(minute: int, day: int = 2) -> datetime:
    return datetime(2026, 9, day, 10, minute, tzinfo=ET)


def _seg(surface: str, ticker: str, kind: str) -> SegmentOutcome:
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path("seg.arrows"),
        partition="p",
        row_kind=kind,
        rows=1,
        error_class=None if kind == "data" else "boom",
        fetched_at=None,
    )


def _cycle(*segments: SegmentOutcome, errors: tuple = (), at: datetime = SLOT) -> CycleResult:
    return CycleResult(at, segments, errors)


# -- what resets and what does not ---------------------------------------------------


def test_a_durable_data_cycle_resets_its_own_surface():
    watchdog = Watchdog()
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap")))
    assert watchdog.count("chains", "SPY") == 1
    watchdog.observe(_cycle(_seg("chains", "SPY", "data")))
    assert watchdog.count("chains", "SPY") == 0


def test_a_gap_row_is_durable_and_still_increments():
    # Gap rows are journaled, which is the point of them, but they are not data. A
    # surface failing every minute is producing rows and producing nothing.
    watchdog = Watchdog()
    for _ in range(2):
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap")))
    assert watchdog.count("chains", "SPY") == 2


def test_a_segment_that_could_not_be_written_increments_too():
    # An unwritten segment is the same absence as a failed one, from the counter's side.
    watchdog = Watchdog()
    watchdog.observe(_cycle(errors=(SegmentError("chains", "SPY", "OSError"),)))
    assert watchdog.count("chains", "SPY") == 1


def test_one_surface_failing_leaves_the_other_alone():
    watchdog = Watchdog()
    for _ in range(3):
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), _seg("quotes", "SPY", "data")))
    assert watchdog.count("chains", "SPY") == 3
    assert watchdog.count("quotes", "SPY") == 0


def test_a_counter_is_kept_per_ticker_as_well_as_per_surface():
    watchdog = Watchdog()
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), _seg("chains", "QQQ", "data")))
    assert watchdog.count("chains", "SPY") == 1
    assert watchdog.count("chains", "QQQ") == 0


# -- when it pages -------------------------------------------------------------------


def test_it_pages_once_on_the_transition_and_then_stays_quiet():
    watchdog = Watchdog()
    raised = [watchdog.observe(_cycle(_seg("chains", "SPY", "gap"))) for _ in range(5)]
    assert [len(pages) for pages in raised] == [0, 0, 1, 0, 0]
    assert raised[2][0].title == "Capture down: SPY chains"
    assert raised[2][0].minutes == 3


def test_a_durable_cycle_re_arms_the_page():
    # The design wants a flapping surface to be loud. Re-arming is deliberate.
    watchdog = Watchdog()
    for _ in range(3):
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap")))
    watchdog.observe(_cycle(_seg("chains", "SPY", "data")))
    raised = [watchdog.observe(_cycle(_seg("chains", "SPY", "gap"))) for _ in range(3)]
    assert [len(pages) for pages in raised] == [0, 0, 1]


def test_the_threshold_is_the_configured_one():
    watchdog = Watchdog(page_minutes=2)
    raised = [watchdog.observe(_cycle(_seg("chains", "SPY", "gap"))) for _ in range(2)]
    assert [len(pages) for pages in raised] == [0, 1]


# -- the sampler collapse ------------------------------------------------------------


def test_every_quotes_counter_tripping_at_once_is_one_page_not_many():
    # Every quotes ticker rides one batched request, so all of them failing together is
    # one thing dying, not many.
    watchdog = Watchdog()
    for _ in range(3):
        pages = watchdog.observe(
            _cycle(
                _seg("quotes", "SPY", "gap"),
                _seg("quotes", "QQQ", "gap"),
                _seg("quotes", "IWM", "gap"),
            )
        )
    assert len(pages) == 1
    assert pages[0].title == "Capture down: quote sampler dead"
    assert pages[0].sampler_collapse
    assert len(pages[0].surfaces) == 3


def test_one_dead_quotes_ticker_is_not_a_sampler_collapse():
    watchdog = Watchdog()
    for _ in range(3):
        pages = watchdog.observe(
            _cycle(_seg("quotes", "SPY", "gap"), _seg("quotes", "QQQ", "data"))
        )
    assert [p.title for p in pages] == ["Capture down: SPY quotes"]
    assert not pages[0].sampler_collapse


def test_a_chains_surface_never_collapses_into_the_sampler_page():
    watchdog = Watchdog()
    for _ in range(3):
        pages = watchdog.observe(
            _cycle(
                _seg("quotes", "SPY", "gap"),
                _seg("quotes", "QQQ", "gap"),
                _seg("chains", "SPY", "gap"),
            )
        )
    titles = sorted(p.title for p in pages)
    assert titles == ["Capture down: SPY chains", "Capture down: quote sampler dead"]


# -- the minutes the loop slept through ----------------------------------------------


def test_a_slept_through_slot_increments_the_same_counters():
    # The loop never runs a cycle for a slot it slept through, so `observe` never sees
    # those minutes. They are exactly the ones the daemon was worst off.
    watchdog = Watchdog()
    surfaces = [Surface("chains", "SPY")]
    raised = [watchdog.missed(surfaces, [_at(i)]) for i in range(3)]
    assert [len(pages) for pages in raised] == [0, 0, 1]
    assert raised[2][0].title == "Capture down: SPY chains"


def test_a_stall_and_a_failing_cycle_count_toward_the_same_page():
    watchdog = Watchdog()
    watchdog.missed([Surface("chains", "SPY")], [_at(0)])
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(1)))
    pages = watchdog.missed([Surface("chains", "SPY")], [_at(2)])
    assert [p.title for p in pages] == ["Capture down: SPY chains"]


@pytest.mark.parametrize("kind", ["data", "gap"])
def test_a_surface_the_cycle_never_touched_is_not_counted(kind):
    # A ticker dropped from the roster mid-session stops being watched rather than
    # paging forever for a surface nobody is capturing.
    watchdog = Watchdog()
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap")))
    for _ in range(4):
        watchdog.observe(_cycle(_seg("chains", "QQQ", kind)))
    assert watchdog.count("chains", "SPY") == 1


def test_two_dead_quotes_tickers_beside_a_live_one_is_not_a_sampler_collapse():
    """The collapse means the shared request died, not that several tickers did.

    Two failing while a third still returns data is two failures, and saying "sampler
    dead" would send the operator to look at the wrong thing.
    """
    watchdog = Watchdog()
    for _ in range(3):
        pages = watchdog.observe(
            _cycle(
                _seg("quotes", "SPY", "gap"),
                _seg("quotes", "QQQ", "gap"),
                _seg("quotes", "IWM", "data"),
            )
        )
    assert not any(page.sampler_collapse for page in pages)
    assert sorted(page.title for page in pages) == [
        "Capture down: QQQ quotes",
        "Capture down: SPY quotes",
    ]


def test_a_run_of_missed_slots_is_charged_once_per_slot():
    # A ten-minute overrun is ten session minutes without a durable cycle, not one.
    watchdog = Watchdog()
    watchdog.missed([Surface("chains", "SPY")], [_at(i) for i in range(10)])
    assert watchdog.count("chains", "SPY") == 10


def test_a_slept_through_slot_is_not_a_dead_sampler():
    # Every quotes surface fans out on a skipped slot too. Calling that a dead sampler
    # would send the operator to look at the batched request, which was never made.
    watchdog = Watchdog()
    pages = watchdog.missed(
        [Surface("quotes", "SPY"), Surface("quotes", "QQQ")], [_at(i) for i in range(3)]
    )
    assert pages
    assert not any(page.sampler_collapse for page in pages)


def test_counters_do_not_carry_across_a_session_date():
    # A counter measures consecutive session minutes. Carrying one overnight would page
    # on the next session's first bad minute while claiming three.
    watchdog = Watchdog()
    for minute in (10, 11):
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(minute)))
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(30, day=3)))
    assert watchdog.count("chains", "SPY") == 1


def test_a_paged_surface_does_not_hide_a_later_sampler_death():
    # SPY was already failing and already paged. When the shared request dies and QQQ
    # starts failing too, that is still one sampler death, not one page per ticker.
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(_seg("quotes", "SPY", "gap"), _seg("quotes", "QQQ", "data"), at=_at(minute))
        )
    for minute in range(3, 7):
        raised += watchdog.observe(
            _cycle(_seg("quotes", "SPY", "gap"), _seg("quotes", "QQQ", "gap"), at=_at(minute))
        )
    assert [p.sampler_collapse for p in raised] == [False, True]
    assert raised[1].title == "Capture down: quote sampler dead"


# -- failures that take the whole daemon down ----------------------------------------


def _fail(surface: str, ticker: str, error_class: str) -> SegmentOutcome:
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path("seg.arrows"),
        partition="p",
        row_kind="gap",
        rows=1,
        error_class=error_class,
        fetched_at=None,
    )


def test_a_dead_token_pages_once_naming_auth_rather_than_the_roster():
    """The refresh token dies every seven days by design.

    Left to the per-surface counters that is one page per chains ticker plus a
    'quote sampler dead' page, none of which says the token is dead.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("chains", "QQQ", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                _fail("quotes", "QQQ", "http_401"),
                at=_at(minute),
            )
        )
    assert [p.title for p in raised] == ["Capture down: auth dead"]
    assert raised[0].cause == "http_401"
    assert len(raised[0].surfaces) == 4


def test_sustained_rate_limiting_names_itself_too():
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_429"),
                _fail("quotes", "SPY", "http_429"),
                at=_at(minute),
            )
        )
    assert [p.title for p in raised] == ["Capture down: rate limited"]


def test_one_dead_surface_is_still_a_surface_page():
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_500"), _seg("quotes", "SPY", "data"), at=_at(minute)
            )
        )
    assert [p.title for p in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause is None


def test_mixed_failure_classes_are_not_one_cause():
    # Two surfaces failing differently is two failures. Naming one cause would send the
    # operator to look at the wrong thing.
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "timeout"),
                at=_at(minute),
            )
        )
    assert sorted(p.title for p in raised) == [
        "Capture down: SPY chains",
        "Capture down: SPY quotes",
    ]


def test_a_cause_pages_once_and_re_arms_when_capture_returns():
    watchdog = Watchdog()
    raised = []
    for minute in range(6):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert len(raised) == 1
    watchdog.observe(
        _cycle(_seg("chains", "SPY", "data"), _seg("quotes", "SPY", "data"), at=_at(6))
    )
    again = []
    for minute in range(7, 11):
        again += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [p.title for p in again] == ["Capture down: auth dead"]


def test_one_surface_failing_with_an_auth_class_is_not_a_dead_daemon():
    """A cause names itself only when it took everything down.

    One surface 401-ing while another still returns data is that surface's problem, not
    the token's. Naming the token would send the operator to re-authenticate against a
    daemon that is authenticating fine.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _seg("quotes", "SPY", "data"),
                at=_at(minute),
            )
        )
    assert [p.title for p in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause is None
