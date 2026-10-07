"""The watchdog's counters and the pages they raise."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from lake.capture import CycleResult, SegmentError, SegmentOutcome
from lake.watchdog import CONTRACTS_ABSENT, Surface, Watchdog, whole_daemon_cause

ET = ZoneInfo("America/New_York")
SLOT = datetime(2026, 9, 2, 10, 0, tzinfo=ET)


def _at(minute: int, day: int = 2) -> datetime:
    return datetime(2026, 9, day, 10, minute, tzinfo=ET)


def _seg(
    surface: str,
    ticker: str,
    kind: str,
    *,
    rows: int = 1,
    data_rows: int | None = None,
    error_class: str | None = None,
) -> SegmentOutcome:
    """One segment outcome. ``data_rows`` defaults to every row on data and none on a gap.

    A data segment holding marker rows and no contract takes ``rows`` above zero with
    ``data_rows=0``. ``error_class`` defaults to none on data and ``boom`` on a gap.
    """
    if data_rows is None:
        data_rows = rows if kind == "data" else 0
    if error_class is None and kind != "data":
        error_class = "boom"
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path("seg.arrows"),
        partition="p",
        row_kind=kind,
        rows=rows,
        error_class=error_class,
        fetched_at=None,
        data_rows=data_rows,
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


# -- the threshold is read at the moment of comparison -------------------------------


def test_a_lowered_threshold_trips_the_running_counter_without_a_restart():
    """A recalibrated threshold takes effect without rebuilding the watchdog.

    The threshold can be a zero-argument callable, read when the watchdog decides whether
    to page rather than at construction. So an operator who lowers ``watchdog_page_minutes``
    mid-session trips the counter already running, at the new number, with no restart.
    """
    threshold = [9]
    watchdog = Watchdog(page_minutes=lambda: threshold[0])
    # Two failing minutes under a high threshold raise nothing.
    early = [watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(i))) for i in range(2)]
    assert [len(pages) for pages in early] == [0, 0]
    assert watchdog.count("chains", "SPY") == 2
    # The threshold drops to three. The counter is untouched, so the third failing minute
    # is the one that trips it, at the new number.
    threshold[0] = 3
    pages = watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(2)))
    assert [p.title for p in pages] == ["Capture down: SPY chains"]
    assert pages[0].minutes == 3


def test_a_raised_threshold_holds_off_a_page_the_old_one_would_have_sent():
    """The read-live threshold moves both ways.

    Raising ``watchdog_page_minutes`` mid-session quiets a surface that would have paged
    at the old number. That is the operator action the change exists for, quieting a
    flapping ticker without a restart.
    """
    threshold = [3]
    watchdog = Watchdog(page_minutes=lambda: threshold[0])
    # Two failing minutes leave the counter one short of the old threshold of three.
    for i in range(2):
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(i)))
    # Raising it to a hundred means the third failing minute, which would have paged at
    # three, does not.
    threshold[0] = 100
    pages = watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(2)))
    assert pages == []
    assert watchdog.count("chains", "SPY") == 3


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
    # The order is deliberate, so a caller rendering the list gets the same one twice.
    assert pages[0].surfaces == (
        Surface("quotes", "IWM"),
        Surface("quotes", "QQQ"),
        Surface("quotes", "SPY"),
    )


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
    # those minutes. They are exactly the ones the daemon was worst off. A roster of one
    # surface still pages the overrun rather than that surface, because the stall is what
    # happened and the surface says nothing about its own health from a minute nothing
    # was attempted in.
    watchdog = Watchdog()
    surfaces = [Surface("chains", "SPY")]
    raised = [watchdog.missed(surfaces, [_at(i)]) for i in range(3)]
    assert [len(pages) for pages in raised] == [0, 0, 1]
    page = raised[2][0]
    assert page.title == "Capture down: loop stalled"
    assert page.minutes == 3
    assert page.surfaces == (Surface("chains", "SPY"),)


def test_a_stall_and_a_failing_cycle_count_toward_the_same_page():
    # The page fires on the third minute, which one stalled slot, one failed cycle, and
    # one more stalled slot add up to. It carries all three, because the minutes it
    # reports are the ones the surface went without a durable cycle.
    watchdog = Watchdog()
    watchdog.missed([Surface("chains", "SPY")], [_at(0)])
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(1)))
    pages = watchdog.missed([Surface("chains", "SPY")], [_at(2)])
    assert [p.title for p in pages] == ["Capture down: loop stalled"]
    assert pages[0].minutes == 3
    assert watchdog.count("chains", "SPY") == 3


@pytest.mark.parametrize("kind", ["data", "gap"])
def test_a_surface_the_cycle_never_touched_is_not_counted(kind):
    # A ticker dropped from the roster mid-session stops being watched rather than
    # paging forever for a surface nobody is capturing. Its counter goes with it, so it
    # does not come back at the count it left with (marketlake #570).
    watchdog = Watchdog()
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap")))
    for _ in range(4):
        watchdog.observe(_cycle(_seg("chains", "QQQ", kind)))
    assert watchdog.count("chains", "SPY") == 0


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
    # A stall charges every quotes surface at once, which is the shape the collapse looks
    # for. Calling it a dead sampler would send the operator to look at the batched
    # request, which was never made, so the stall folds under its own title instead.
    watchdog = Watchdog()
    pages = watchdog.missed(
        [Surface("quotes", "SPY"), Surface("quotes", "QQQ")], [_at(i) for i in range(3)]
    )
    assert [page.title for page in pages] == ["Capture down: loop stalled"]
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
    # SPY had been down three minutes before QQQ joined it, and the page reports the
    # longest of the two counters. Reporting the shortest would halve the outage.
    assert raised[1].minutes == 6


# -- one stall, one page -------------------------------------------------------------


def _roster(tickers: int) -> list[Surface]:
    """The shape the design captures in: every ticker carries both surfaces."""
    return [
        Surface(surface, f"T{n:03d}") for n in range(tickers) for surface in ("chains", "quotes")
    ]


def test_one_overrun_raises_one_page_rather_than_one_per_surface():
    # The roster the design sizes for is about 115 tickers on two surfaces. The fan-out
    # this replaces sent 230 pages for one stall, against a daily cap of 40, so one
    # overrun could spend the whole day's budget and bury the rest of the session.
    watchdog = Watchdog()
    pages = watchdog.missed(_roster(115), [_at(minute) for minute in range(3)])
    assert len(pages) == 1
    assert pages[0].title == "Capture down: loop stalled"
    assert not pages[0].sampler_collapse


def test_the_overrun_page_names_the_minutes_missed_and_the_surfaces_charged():
    # One page for ten surfaces and one page for two hundred read identically without
    # the counts, which is the rule the sampler page and the cause page already follow.
    # The stall runs from a healthy roster, so the minutes it reports are its own slots.
    watchdog = Watchdog()
    roster = _roster(5)
    pages = watchdog.missed(roster, [_at(minute) for minute in range(5)])
    assert pages[0].minutes == 5
    assert pages[0].surfaces == tuple(sorted(roster, key=str))
    assert len(pages[0].surfaces) == 10


def test_a_folded_overrun_still_charges_every_surface_once_per_slot():
    # The fold is the page, not the counting. A ten-minute overrun is ten session minutes
    # without a durable cycle for every surface it charged, and each of those surfaces
    # pages on its own account from that count once the loop resumes.
    watchdog = Watchdog()
    roster = _roster(3)
    watchdog.missed(roster, [_at(minute) for minute in range(10)])
    assert {watchdog.count(key.surface, key.ticker) for key in roster} == {10}


def test_a_surface_still_dead_after_the_overrun_pages_on_its_own_account():
    """The fold must not spend a surface's own budget.

    A stall is evidence about the loop, not about any one surface's health. A fold that
    put its surfaces in ``_paged`` would pass every other test in this section and make
    the masking permanent, because the only exit from ``_paged`` is producing data. The
    one surface that is genuinely dead has to be heard once the loop resumes.
    """
    watchdog = Watchdog()
    overrun = watchdog.missed(_roster(2), [_at(minute) for minute in range(3)])
    assert [page.title for page in overrun] == ["Capture down: loop stalled"]
    after = []
    for minute in range(3, 6):
        after += watchdog.observe(
            _cycle(
                _seg("chains", "T000", "gap"),
                _seg("quotes", "T000", "data"),
                _seg("chains", "T001", "data"),
                _seg("quotes", "T001", "data"),
                at=_at(minute),
            )
        )
    assert [page.title for page in after] == ["Capture down: T000 chains"]


def test_a_healthy_resume_after_an_overrun_pages_nothing():
    # The other half of the rule above. A stall that everything came back from owes one
    # page for the stall and nothing more.
    watchdog = Watchdog()
    roster = _roster(2)
    watchdog.missed(roster, [_at(minute) for minute in range(3)])
    after = []
    for minute in range(3, 6):
        after += watchdog.observe(
            _cycle(*[_seg(key.surface, key.ticker, "data") for key in roster], at=_at(minute))
        )
    assert after == []


def test_an_overrun_below_the_threshold_pages_nothing():
    # Today's behaviour, which the fold keeps. Two slept slots are not three, however
    # large the roster charged is.
    watchdog = Watchdog()
    assert watchdog.missed(_roster(115), [_at(0), _at(1)]) == []


def test_a_second_stall_stays_quiet_until_a_durable_cycle_re_arms_it():
    # Once on the transition, the rule every other page here follows. A loop stuck in a
    # stall reports one every tick it wakes on, and each report would otherwise page.
    watchdog = Watchdog()
    roster = _roster(2)
    assert len(watchdog.missed(roster, [_at(minute) for minute in range(3)])) == 1
    assert watchdog.missed(roster, [_at(minute) for minute in range(3, 6)]) == []
    watchdog.observe(_cycle(*[_seg(key.surface, key.ticker, "data") for key in roster], at=_at(6)))
    assert len(watchdog.missed(roster, [_at(minute) for minute in range(7, 10)])) == 1


def test_a_nap_after_a_surface_has_paged_does_not_page_again():
    """The gate is the page decision the per-surface path already makes.

    One surface dead all session sits at the threshold for the rest of it. Deciding the
    stall page on the counters alone would let every later one-minute nap raise a page
    of its own, which is this fold's own harm arriving in the time dimension instead of
    the roster one. A surface that has already paged is not asked again.
    """
    watchdog = Watchdog()
    roster = _roster(2)
    raised = []
    healthy = [key for key in roster if key != Surface("chains", "T000")]
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _seg("chains", "T000", "gap"),
                *[_seg(key.surface, key.ticker, "data") for key in healthy],
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: T000 chains"]
    # The loop then oversleeps one slot every other minute, with a cycle in between that
    # keeps every other surface alive.
    for minute in range(3, 15, 2):
        assert watchdog.missed(roster, [_at(minute)]) == []
        watchdog.observe(
            _cycle(
                _seg("chains", "T000", "gap"),
                *[_seg(key.surface, key.ticker, "data") for key in healthy],
                at=_at(minute + 1),
            )
        )


def test_a_stall_that_trips_one_surface_while_the_others_sit_below_still_pages():
    # The roster is rarely uniform. Surfaces recover at different minutes, so the stall
    # that pushes one of them over arrives while the rest are nowhere near. The page
    # carries the minutes of the surface that tripped, not the slot it tripped on. The
    # one that trips is quotes, which sorts last, so a gate reading a single surface
    # off the front of the roster finds a counter at 1 and says nothing.
    watchdog = Watchdog()
    roster = _roster(1)
    for minute in range(2):
        watchdog.observe(
            _cycle(
                _seg("quotes", "T000", "gap"),
                _seg("chains", "T000", "data"),
                at=_at(minute),
            )
        )
    pages = watchdog.missed(roster, [_at(2)])
    assert [page.title for page in pages] == ["Capture down: loop stalled"]
    assert pages[0].minutes == 3
    assert len(pages[0].surfaces) == 2
    assert watchdog.count("chains", "T000") == 1


def test_a_second_stall_inside_an_outage_that_is_still_running_adds_nothing():
    # A cycle that ran and gapped everything is the outage carrying on. It pages each
    # surface on its own account, and a stall after it charges the same surfaces, so the
    # stall has nothing left to report. What re-arms the stall page is a durable data
    # cycle, because that is what proves the loop is running again.
    watchdog = Watchdog()
    roster = _roster(2)
    assert len(watchdog.missed(roster, [_at(minute) for minute in range(3)])) == 1
    watchdog.observe(_cycle(*[_seg(key.surface, key.ticker, "gap") for key in roster], at=_at(3)))
    assert watchdog.missed(roster, [_at(minute) for minute in range(4, 7)]) == []


def test_slots_out_of_order_charge_the_same_as_slots_in_order():
    # The date-roll guard reads the slots in time order, so the order they arrive in
    # cannot decide what the page says. A shuffled run that crosses a session date would
    # otherwise charge yesterday's minutes onto today's counters.
    slots = [_at(minute) for minute in range(50)] + [_at(minute, day=3) for minute in range(4)]
    in_order = Watchdog()
    shuffled = Watchdog()
    ordered_pages = in_order.missed(_roster(2), slots)
    shuffled_pages = shuffled.missed(_roster(2), list(reversed(slots)))
    assert [page.minutes for page in shuffled_pages] == [page.minutes for page in ordered_pages]
    assert shuffled.count("chains", "T000") == in_order.count("chains", "T000") == 4


def test_a_call_with_nothing_to_charge_pages_nothing():
    """An empty roster and an empty run of slots are both nothing happening.

    A fully retired lake is a supported state, and a call carrying no slots is a stall
    that did not happen. Neither is a fact about the loop, so neither raises a page, and
    the counters standing past the threshold does not make one. The threshold is lowered
    here the way a mid-session recalibration lowers it, which is how a counter comes to
    sit past the threshold with no page of its own behind it.
    """
    threshold = [5]
    watchdog = Watchdog(page_minutes=lambda: threshold[0])
    roster = _roster(2)
    assert watchdog.missed(roster, [_at(minute) for minute in range(3)]) == []
    threshold[0] = 3
    assert watchdog.missed(roster, []) == []
    assert watchdog.missed([], [_at(minute) for minute in range(10)]) == []


def test_a_stall_pages_again_on_the_next_session_date():
    # The sibling of the counter rule and the cause rule. A stall flag carried overnight
    # would silence the next morning's first stall, however long that one ran.
    watchdog = Watchdog()
    roster = _roster(2)
    first = watchdog.missed(roster, [_at(minute) for minute in range(3)])
    assert [page.title for page in first] == ["Capture down: loop stalled"]
    second = watchdog.missed(roster, [_at(minute, day=3) for minute in range(3)])
    assert [page.title for page in second] == ["Capture down: loop stalled"]


def test_a_stall_across_a_session_date_names_only_this_session_s_slots():
    # The date change drops the counters, so the slots below it are no longer minutes
    # this session went without. Counting them would put a night of closed-market
    # minutes in a page about this morning's stall.
    watchdog = Watchdog()
    roster = _roster(2)
    slots = [_at(minute) for minute in range(50)] + [_at(minute, day=3) for minute in range(4)]
    pages = watchdog.missed(roster, slots)
    assert [page.minutes for page in pages] == [4]
    assert {watchdog.count(key.surface, key.ticker) for key in roster} == {4}


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
        data_rows=0,
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
    assert [p.title for p in raised] == ["Capture down: token dead"]
    assert raised[0].cause == "http_401"
    assert len(raised[0].surfaces) == 4
    # The page fires on the third dead minute and is dated from the first (marketlake #747).
    assert (raised[0].minutes, raised[0].since) == (3, _at(0))


def test_the_whole_daemon_cause_path_reads_the_threshold_live():
    """The token-dead page reads the recalibrated threshold too, not only the surface page.

    A dead token gaps every surface at once, and that page fires from a different
    comparison than a single surface's does. Both have to read the live threshold, or the
    load-bearing case, the seven-day token death, would keep the old number.
    """
    threshold = [9]
    watchdog = Watchdog(page_minutes=lambda: threshold[0])
    # Two dead minutes under a high threshold raise nothing.
    early = []
    for minute in range(2):
        early += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert early == []
    # The threshold drops to three, so the third dead minute trips the whole-daemon page.
    threshold[0] = 3
    pages = watchdog.observe(
        _cycle(
            _fail("chains", "SPY", "http_401"),
            _fail("quotes", "SPY", "http_401"),
            at=_at(2),
        )
    )
    assert [p.title for p in pages] == ["Capture down: token dead"]
    assert pages[0].cause == "http_401"


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
    # The page names the surface, and carries the class it is failing with, so the body
    # can say why without the title claiming the whole daemon is down.
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_500"), _seg("quotes", "SPY", "data"), at=_at(minute)
            )
        )
    assert [p.title for p in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause == "http_500"
    # Only a cause page is dated, because only it always rides a minute that feeds the
    # dead-man nothing (marketlake #747).
    assert raised[0].since is None


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
    assert [p.title for p in again] == ["Capture down: token dead"]


def test_one_surface_failing_with_an_auth_class_is_not_a_dead_daemon():
    """A cause names itself only when it took everything down.

    One surface 401-ing while another still returns data is that surface's problem, not
    the token's. Naming the token would send the operator to re-authenticate against a
    daemon that is authenticating fine.

    The page still carries the class, because what is rejected here is the title's claim
    about the whole daemon, not the fact that this surface saw a 401.
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
    assert raised[0].cause == "http_401"


def test_one_dead_token_reported_two_ways_is_still_one_page():
    """A dead refresh token has two shapes, and they are one outage.

    ``schwab.VendorAuthError`` records both. While the cached access token still works
    the request goes out and comes back refused, which capture records as ``http_401``.
    Once the refresh itself fails no request is made at all, which raises instead and
    lands as ``vendor_auth_error``. A session carries both, in whatever order the access
    token happens to expire in.

    Counted by error class, the switch reads as a second outage starting and sends a
    second ``Capture down: token dead``. The operator is then paged twice for one token
    and has to work out whether two things broke. Counted by the title the class resolves
    to, the two shapes share one page.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(20):
        error_class = "http_401" if minute < 10 else "vendor_auth_error"
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", error_class),
                _fail("quotes", "SPY", error_class),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    assert raised[0].cause == "http_401"


def test_a_different_cause_still_pages_on_its_own_transition():
    """One page per cause, not one page per session.

    Suppressing by title must not suppress a title that has not paged. Rate limiting
    following a dead token is a different outage and the operator has to be told, or the
    collapse that exists to stop a page storm would start swallowing real pages instead.
    """
    watchdog = Watchdog()
    opened = []
    for minute in range(3):
        opened += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "vendor_auth_error"),
                _fail("quotes", "SPY", "vendor_auth_error"),
                at=_at(minute),
            )
        )
    assert [page.title for page in opened] == ["Capture down: token dead"]
    switched = watchdog.observe(
        _cycle(
            _fail("chains", "SPY", "http_429"),
            _fail("quotes", "SPY", "http_429"),
            at=_at(3),
        )
    )
    assert [page.title for page in switched] == ["Capture down: rate limited"]


# -- a cause speaks for the surfaces it named ----------------------------------------


def _rate_limited(minute: int, *, spy_chains_returns: bool) -> CycleResult:
    """One minute of a rate limit, with ``chains SPY`` either producing or gapping.

    ``chains SPY`` is the surface that comes and goes. ``chains QQQ`` and ``quotes SPY``
    gap every minute of the episode.
    """
    spy = (
        _seg("chains", "SPY", "data") if spy_chains_returns else _fail("chains", "SPY", "http_429")
    )
    return _cycle(
        spy,
        _fail("chains", "QQQ", "http_429"),
        _fail("quotes", "SPY", "http_429"),
        at=_at(minute),
    )


def test_a_flapping_surface_does_not_re_page_the_cause():
    """One rate limit that runs all session is one condition, not one per flap.

    A cause speaks for every surface it named, and that used to outlive the cause
    itself. One surface producing dropped the cause while the others stayed suppressed,
    so the next run of dead minutes paged the same rate limit again. A full session of
    that spends the whole 40-a-day cap restating the first page.
    """
    watchdog = Watchdog()
    raised = []
    # Three dead minutes open the episode and page the cause.
    for minute in range(3):
        raised += watchdog.observe(_rate_limited(minute, spy_chains_returns=False))
    assert [page.title for page in raised] == ["Capture down: rate limited"]
    # Ten flaps of chains SPY, each with three dead minutes behind it, which is long
    # enough to re-arm every gate the first page passed.
    for cycle in range(10):
        start = 3 + cycle * 4
        raised += watchdog.observe(_rate_limited(start, spy_chains_returns=True))
        for minute in range(start + 1, start + 4):
            raised += watchdog.observe(_rate_limited(minute, spy_chains_returns=False))
    assert [page.title for page in raised] == ["Capture down: rate limited"]


def test_a_partial_recovery_keeps_the_cause_live_over_the_surfaces_still_dead():
    """One surface coming back sends no page, and it does not clear the cause either.

    The design pages once on the transition and then stays quiet, so one surface coming
    back says nothing new to an operator whose remedy has not changed. What it must leave
    behind is correct state. The cause still counts the surfaces that are still down, and
    no longer counts the one that returned.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(_rate_limited(minute, spy_chains_returns=False))
    assert [page.title for page in raised] == ["Capture down: rate limited"]
    # chains SPY returns every third minute for the rest of the episode.
    for minute in range(3, 30):
        raised += watchdog.observe(_rate_limited(minute, spy_chains_returns=minute % 3 == 0))
    assert [page.title for page in raised] == ["Capture down: rate limited"]
    assert watchdog._paged_causes == {
        "Capture down: rate limited": {Surface("chains", "QQQ"), Surface("quotes", "SPY")}
    }


def test_a_surface_failing_a_way_another_cause_names_pages_on_its_own():
    """A token dying inside a rate limit is the failure that must still page.

    The rate-limit page explains a 429. It explains nothing about a 401, so covering the
    surface any longer would hide a dead token behind a page about something else, for
    as long as the rate limit lasted.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(6):
        raised += watchdog.observe(_rate_limited(minute, spy_chains_returns=False))
    assert [page.title for page in raised] == ["Capture down: rate limited"]
    # chains SPY returns while chains QQQ escalates to a dead token. quotes SPY is still
    # rate limited, so the cause still covers it and stays quiet about it.
    for minute in range(6, 10):
        raised += watchdog.observe(
            _cycle(
                _seg("chains", "SPY", "data"),
                _fail("chains", "QQQ", "http_401"),
                _fail("quotes", "SPY", "http_429"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == [
        "Capture down: rate limited",
        "Capture down: QQQ chains",
    ]
    # chains QQQ is still down, so the cause that named it has not lifted and still
    # counts it. What changed is that the cause no longer speaks for how it is failing.
    assert watchdog._paged_causes == {
        "Capture down: rate limited": {Surface("chains", "QQQ"), Surface("quotes", "SPY")}
    }


def test_a_new_class_under_the_same_title_keeps_the_surface_quiet():
    """The release compares titles, because one outage arrives under several classes.

    A dead refresh token arrives as ``http_401`` while the cached access token still
    works, and as ``vendor_auth_error`` once the refresh fails. Comparing raw classes
    would read that switch as a new failure and page the same dead token a second time,
    which is what the one-page rule for a cause exists to stop.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("chains", "QQQ", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    # chains SPY returns, and chains QQQ switches to the other shape of the same death.
    for minute in range(3, 8):
        raised += watchdog.observe(
            _cycle(
                _seg("chains", "SPY", "data"),
                _fail("chains", "QQQ", "vendor_auth_error"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    assert watchdog._paged_causes == {
        "Capture down: token dead": {Surface("chains", "QQQ"), Surface("quotes", "SPY")}
    }


def test_a_failure_no_cause_names_does_not_lift_the_cause():
    """An ordinary transient failure during an outage is not the outage ending.

    Timeouts and 5xx are expected while capture is down, and the remedy for the outage
    does not change when one arrives. Treating a blip as the cause lifting would re-arm
    the cause and page the same dead token again, once per blip, until the daily cap ran
    out. Measured on a 390-minute session with a blip minute every tenth minute, that is
    42 pages for one dead token against a cap of 40.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(390):
        error_class = "timeout" if minute >= 3 and minute % 10 == 0 else "http_401"
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", error_class),
                _fail("chains", "QQQ", error_class),
                _fail("quotes", "SPY", error_class),
                at=_at(0) + timedelta(minutes=minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    assert watchdog._paged_causes == {
        "Capture down: token dead": {
            Surface("chains", "SPY"),
            Surface("chains", "QQQ"),
            Surface("quotes", "SPY"),
        }
    }


def test_a_retired_ticker_does_not_strand_its_cause():
    """A cause must not be held live by a surface nobody captures any more.

    A ticker retired mid-session stops appearing in cycles, which the daemon supports.
    Nothing about that surface changes again, so a cause that kept counting it would
    never re-arm, and the next genuine outage under the same title would page nobody for
    the rest of the session.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("chains", "QQQ", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    # chains QQQ is retired. The other two recover, which leaves the cause holding only
    # a surface no cycle will ever touch again.
    for minute in range(3, 6):
        watchdog.observe(
            _cycle(_seg("chains", "SPY", "data"), _seg("quotes", "SPY", "data"), at=_at(minute))
        )
    assert watchdog._paged_causes == {}
    # A second, genuinely separate token death still pages.
    for minute in range(6, 10):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == [
        "Capture down: token dead",
        "Capture down: token dead",
    ]


def test_a_cause_pages_again_on_the_next_session_date():
    """A counter measures consecutive session minutes, and so does a cause.

    An outage still open next morning is worth a page that morning. Carrying a paged
    cause overnight would silence the new session's first page, which is the failure the
    counters are already protected from.
    """
    watchdog = Watchdog()
    first = []
    for minute in range(4):
        first += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_429"),
                _fail("quotes", "SPY", "http_429"),
                at=_at(minute),
            )
        )
    assert [page.title for page in first] == ["Capture down: rate limited"]
    second = []
    for minute in range(4):
        second += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_429"),
                _fail("quotes", "SPY", "http_429"),
                at=_at(minute, day=3),
            )
        )
    assert [page.title for page in second] == ["Capture down: rate limited"]


def test_a_surface_that_paged_yesterday_pages_again_today():
    # The sibling of the rule above, one surface at a time. A gap still open next
    # morning pages again then.
    watchdog = Watchdog()
    first = [watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(i))) for i in range(4)]
    assert [len(pages) for pages in first] == [0, 0, 1, 0]
    second = [
        watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(i, day=3))) for i in range(4)
    ]
    assert [len(pages) for pages in second] == [0, 0, 1, 0]


def test_a_slept_through_slot_stays_quiet_under_a_live_cause():
    """An overrun during a whole-daemon outage is the outage, not a second finding.

    ``missed`` charges the minutes the loop never ran a cycle for. Nothing was attempted
    in them, so they say nothing about how a surface is failing, and a live cause still
    speaks for every surface it named.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]
    watched = [Surface("chains", "SPY"), Surface("quotes", "SPY")]
    assert watchdog.missed(watched, [_at(minute) for minute in range(3, 9)]) == []


# -- a page names what it is failing with --------------------------------------------


def test_a_starved_ticker_pages_with_the_class_that_starved_it():
    """The case this exists for, at the roster the project runs.

    A chain is fetched as several date windows. A ticker that loses every window journals
    a whole-chain gap. A ticker that loses only some journals a data segment carrying the
    class of the window it lost, which resets that surface's counter. So a rate limit that
    starves one ticker and merely degrades another is never one whole-daemon failure, and
    the page that goes out names a ticker. It has to carry the class, or it sends the
    operator to look at one chain worker while the budget is what is failing.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_429"),
                # A partial snapshot is a data row carrying the lost window's class.
                _seg("chains", "QQQ", "data"),
                _seg("quotes", "SPY", "data"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause == "http_429"
    # The degraded ticker is not paged at all. A partial snapshot is a durable data cycle,
    # and the design gives degradation to the validation battery, not to the watchdog.
    assert watchdog.count("chains", "QQQ") == 0


def test_a_surface_that_could_not_be_written_pages_with_its_own_class():
    # An unwritten segment carries its class too, and the surface is just as down.
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(errors=(SegmentError("chains", "SPY", "os_error"),), at=_at(minute))
        )
    assert [page.title for page in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause == "os_error"


def test_a_page_for_a_failure_with_no_recorded_class_names_none():
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                SegmentOutcome(
                    surface="chains",
                    ticker="SPY",
                    path=Path("seg.arrows"),
                    partition="p",
                    row_kind="gap",
                    rows=1,
                    error_class=None,
                    fetched_at=None,
                    data_rows=0,
                ),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: SPY chains"]
    assert raised[0].cause is None


def test_a_slept_through_slot_pages_with_no_class():
    # Nothing was attempted in a slot the loop slept through, so there is no failure to
    # name. Guessing one would point at a request that was never made.
    watchdog = Watchdog()
    pages = watchdog.missed([Surface("chains", "SPY")], [_at(minute) for minute in range(3)])
    assert [page.title for page in pages] == ["Capture down: loop stalled"]
    assert pages[0].cause is None


def test_the_sampler_page_names_the_class_every_collapsed_ticker_shares():
    # A chains surface still landing rows is what keeps this a sampler death rather than
    # a whole-daemon cause, which would name itself and never reach the collapse.
    watchdog = Watchdog()
    for minute in range(3):
        pages = watchdog.observe(
            _cycle(
                _fail("quotes", "SPY", "http_429"),
                _fail("quotes", "QQQ", "http_429"),
                _fail("quotes", "IWM", "http_429"),
                _seg("chains", "SPY", "data"),
                at=_at(minute),
            )
        )
    assert [page.title for page in pages] == ["Capture down: quote sampler dead"]
    assert pages[0].cause == "http_429"


def test_the_sampler_page_names_no_class_when_the_collapsed_tickers_disagree():
    """One batched request died, so in practice the collapsed tickers agree.

    When they do not, there is no one class the page can honestly name, and picking one
    of several would send the operator after whichever sorted first.
    """
    watchdog = Watchdog()
    for minute in range(3):
        pages = watchdog.observe(
            _cycle(
                _fail("quotes", "SPY", "http_429"),
                _fail("quotes", "QQQ", "timeout"),
                _fail("quotes", "IWM", "http_429"),
                _seg("chains", "SPY", "data"),
                at=_at(minute),
            )
        )
    assert [page.title for page in pages] == ["Capture down: quote sampler dead"]
    assert pages[0].cause is None


def test_a_collapse_and_a_surface_page_in_one_minute_each_name_their_own_class():
    """Two pages in one minute, and neither borrows the other's class.

    The sampler page speaks for the batched quotes request, so its class comes from the
    quotes tickers alone. A chains ticker failing some other way in the same minute is a
    separate failure with a separate page, and letting it into the sampler's set would
    break the agreement and cost the sampler page its class at the moment it is needed.
    """
    watchdog = Watchdog()
    for minute in range(3):
        pages = watchdog.observe(
            _cycle(
                _fail("quotes", "SPY", "http_429"),
                _fail("quotes", "QQQ", "http_429"),
                _fail("quotes", "IWM", "http_429"),
                _fail("chains", "SPY", "http_500"),
                # A second chains ticker still landing rows keeps this out of the
                # whole-daemon path, which would otherwise name one cause for everything.
                _seg("chains", "QQQ", "data"),
                at=_at(minute),
            )
        )
    by_title = {page.title: page for page in pages}
    assert sorted(by_title) == ["Capture down: SPY chains", "Capture down: quote sampler dead"]
    assert by_title["Capture down: quote sampler dead"].cause == "http_429"
    assert by_title["Capture down: SPY chains"].cause == "http_500"
    # The chains ticker has its own page, so it is not one of the three the sampler page
    # stands for. Counting it would tell the operator four were folded into a fold of
    # three.
    assert len(by_title["Capture down: quote sampler dead"].surfaces) == 3


def test_a_collapse_names_no_class_when_a_ticker_that_already_paged_failed_differently():
    """The sampler's set is every failing quotes ticker, not only the newly tripped ones.

    A ticker can be gapped on its own when it goes missing from an otherwise healthy
    batch, page for that, and still be failing when the whole batch starts being rejected
    later. The collapse then covers a ticker whose failure is not the batch's failure, and
    naming the batch's class would put a class on a page that stands for both.
    """
    watchdog = Watchdog()
    raised = []
    # SPY alone is missing from the batch, so it pages on its own account first.
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("quotes", "SPY", "quote_missing"),
                _seg("quotes", "QQQ", "data"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: SPY quotes"]
    # Now the batch itself starts being rejected, while SPY keeps failing its own way.
    collapse = []
    for minute in range(3, 7):
        collapse += watchdog.observe(
            _cycle(
                _fail("quotes", "SPY", "quote_missing"),
                _fail("quotes", "QQQ", "http_429"),
                at=_at(minute),
            )
        )
    assert [page.title for page in collapse] == ["Capture down: quote sampler dead"]
    assert collapse[0].cause is None
    # SPY paged on its own account first, so it is no longer newly tripped. It is still
    # down and still one of the two this page stands for.
    assert len(collapse[0].surfaces) == 2


def test_an_unwritten_segment_failing_another_cause_s_way_is_released():
    """The class decides the cover wherever the failure was recorded.

    A surface can be down because its segment could not be written, and that failure
    carries its own class. When the class is one another cause names, the cause holding
    the surface has stopped explaining it, the same as for a gap row. ``Capture`` cannot
    raise a vendor auth error out of the write path today, so this pins the rule rather
    than a path in use.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_429"),
                _fail("chains", "QQQ", "http_429"),
                _fail("quotes", "SPY", "http_429"),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: rate limited"]
    # quotes SPY fails with a 500, which keeps the cycle off the whole-daemon path, since
    # the classes no longer agree. That path returns early while a cause is live and would
    # never reach the per-surface pages. No surface lands data, so the write failure stays
    # inside the cause, and only its class can lift the cover (marketlake #754).
    for minute in range(3, 7):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "QQQ", "http_429"),
                _fail("quotes", "SPY", "http_500"),
                errors=(SegmentError("chains", "SPY", "vendor_auth_error"),),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == [
        "Capture down: rate limited",
        "Capture down: SPY chains",
    ]
    assert raised[1].cause == "vendor_auth_error"


# -- tickers the capture spans leave out (marketlake #570) ----------------------------


def _clamped(*segments: SegmentOutcome, out: tuple[str, ...], at: datetime, errors=()):
    return CycleResult(at, segments, errors, out_of_span=out)


def test_a_ticker_the_spans_leave_out_pages_once_at_the_threshold():
    """SPY is captured and QQQ is enabled and outside every span, so it owes minutes.

    Nothing touches QQQ's surfaces and the dead-man is fed by SPY, so this page is the
    only thing that reports it.
    """
    watchdog = Watchdog()
    raised = [
        watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(m)))
        for m in range(4)
    ]
    assert raised[0] == raised[1] == raised[3] == []
    (page,) = raised[2]
    assert page.title == "Capture down: tickers outside every capture span"
    assert page.tickers == ("QQQ",)
    assert page.minutes == 3
    assert page.surfaces == ()
    assert page.cause is None


def test_a_ticker_out_of_span_for_less_than_the_threshold_pages_nothing():
    # A rejoin writes its roster entry before its span, and a retire closes the span
    # before it changes the roster, so a cycle can see one ticker out for a minute.
    watchdog = Watchdog()
    raised = []
    for m in range(2):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(m)))
    for m in range(2, 6):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=(), at=_at(m)))
    raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(6)))
    assert raised == []


def test_a_ticker_back_in_span_re_arms_its_page():
    watchdog = Watchdog()
    outs = [("QQQ",)] * 3 + [()] + [("QQQ",)] * 3
    raised = [
        watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=out, at=_at(m)))
        for m, out in enumerate(outs)
    ]
    assert [len(pages) for pages in raised] == [0, 0, 1, 0, 0, 0, 1]
    assert raised[6][0].minutes == 3


def test_a_clamp_standing_overnight_pages_again_the_next_session():
    watchdog = Watchdog()
    raised = []
    for day in (2, 3):
        for m in range(3):
            raised += watchdog.observe(
                _clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(m, day=day))
            )
    assert [page.minutes for page in raised] == [3, 3]


def test_every_enabled_ticker_left_out_pages_nothing_until_a_cycle_captures():
    """The full case is the dead-man's to page, and no second page is added.

    The count still rises, so the first cycle that captures anything pages at once for a
    ticker that has been out past the threshold.
    """
    watchdog = Watchdog()
    raised = []
    for m in range(4):
        raised += watchdog.observe(_clamped(out=("SPY", "QQQ"), at=_at(m)))
    assert raised == []
    (page,) = watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(4)))
    assert page.tickers == ("QQQ",)
    assert page.minutes == 5


def test_a_cycle_whose_every_write_failed_is_not_the_full_case():
    # A segment that could not be written still proves a live ticker was attempted.
    watchdog = Watchdog()
    raised = []
    for m in range(3):
        raised += watchdog.observe(
            _clamped(out=("QQQ",), at=_at(m), errors=(SegmentError("quotes", "SPY", "OSError"),))
        )
    assert [page.tickers for page in raised if page.tickers] == [("QQQ",)]


def test_a_cause_page_and_the_out_of_span_page_are_both_sent():
    # A dead token and a clamp have different repairs, so neither holds the other back.
    watchdog = Watchdog()
    raised = []
    for m in range(3):
        raised += watchdog.observe(
            _clamped(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                out=("QQQ",),
                at=_at(m),
            )
        )
    assert [page.title for page in raised] == [
        "Capture down: token dead",
        "Capture down: tickers outside every capture span",
    ]


def test_a_ticker_joining_the_set_later_pages_again_naming_every_ticker_out():
    watchdog = Watchdog()
    outs = [("QQQ",)] * 3 + [("QQQ", "IWM")] * 3
    raised = []
    for m, out in enumerate(outs):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=out, at=_at(m)))
    assert [page.tickers for page in raised] == [("QQQ",), ("QQQ", "IWM")]
    assert raised[1].minutes == 6


def test_the_out_of_span_page_reads_the_threshold_live():
    threshold = [5]
    watchdog = Watchdog(page_minutes=lambda: threshold[0])
    raised = []
    for m in range(3):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(m)))
    assert raised == []
    threshold[0] = 3
    (page,) = watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(3)))
    assert page.tickers == ("QQQ",)


# -- a surface that leaves the cycle takes its counter with it (marketlake #570) ------


def test_a_clamped_ticker_charged_by_two_stalls_does_not_page_a_short_one():
    """The missed-slot hook charges every enabled entry, the clamped one included.

    Frozen, QQQ's counter kept each stall's charge, so a one-minute stall took it to three
    and paged the loop as overrun for three minutes.
    """
    watched = [Surface("quotes", "SPY"), Surface("quotes", "QQQ")]
    watchdog = Watchdog()
    watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(0)))
    assert watchdog.missed(watched, [_at(1), _at(2)]) == []
    watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(3)))
    assert watchdog.count("quotes", "QQQ") == 0
    assert watchdog.missed(watched, [_at(4)]) == []


def test_a_ticker_back_from_a_clamp_does_not_page_on_its_first_gap():
    """QQQ gapped twice, sat out of span for an hour, and gapped once on its return.

    Frozen at two, that one gap paged it as three minutes down.
    """
    watchdog = Watchdog()
    raised = []
    for m in range(2):
        raised += watchdog.observe(
            _clamped(_seg("quotes", "SPY", "data"), _seg("quotes", "QQQ", "gap"), out=(), at=_at(m))
        )
    for m in range(2, 50):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(m)))
    raised += watchdog.observe(
        _clamped(_seg("quotes", "SPY", "data"), _seg("quotes", "QQQ", "gap"), out=(), at=_at(50))
    )
    assert [page for page in raised if not page.tickers] == []
    assert watchdog.count("quotes", "QQQ") == 1


def test_a_surface_that_paged_and_left_pages_again_when_it_returns_failing():
    # The named price of the drop: a second page after the ticker's time away.
    watchdog = Watchdog()
    titles = []
    for m in range(3):
        titles += [
            p.title
            for p in watchdog.observe(
                _cycle(_seg("quotes", "SPY", "data"), _seg("quotes", "QQQ", "gap"), at=_at(m))
            )
        ]
    titles += [p.title for p in watchdog.observe(_cycle(_seg("quotes", "SPY", "data"), at=_at(3)))]
    for m in range(4, 7):
        titles += [
            p.title
            for p in watchdog.observe(
                _cycle(_seg("quotes", "SPY", "data"), _seg("quotes", "QQQ", "gap"), at=_at(m))
            )
        ]
    assert titles == ["Capture down: QQQ quotes", "Capture down: QQQ quotes"]


def test_the_full_case_drops_only_the_tickers_it_names():
    # A cycle that touched nothing is evidence about no surface, except that the tickers
    # it names are no longer captured.
    watchdog = Watchdog()
    for m in range(2):
        watchdog.observe(
            _cycle(_seg("chains", "SPY", "gap"), _seg("quotes", "QQQ", "gap"), at=_at(m))
        )
    watchdog.observe(_clamped(out=("QQQ",), at=_at(2)))
    assert watchdog.count("quotes", "QQQ") == 0
    assert watchdog.count("chains", "SPY") == 2


def test_a_stall_inside_an_outage_that_paged_adds_no_page_for_a_clamped_ticker():
    """SPY quotes has paged under its own title, and QQQ is out of span.

    The missed-slot hook charges both, since it reads no spans. Charged to QQQ's surface,
    a three-slot stall found a fresh counter at the threshold and paged the loop as
    overrun, where the same stall without QQQ adds nothing.
    """
    watched = [Surface("quotes", "SPY"), Surface("quotes", "QQQ")]
    watchdog = Watchdog()
    for m in range(3):
        watchdog.observe(_clamped(_seg("quotes", "SPY", "gap"), out=("QQQ",), at=_at(m)))
    assert watchdog.missed(watched, [_at(3), _at(4), _at(5)]) == []
    assert watchdog.count("quotes", "QQQ") == 0


def test_slept_through_slots_count_toward_the_out_of_span_page():
    # QQQ was out for one cycle, the loop slept two slots, and one more cycle names it.
    # That is four minutes out, so the page fires and says four.
    watchdog = Watchdog()
    watched = [Surface("quotes", "SPY"), Surface("quotes", "QQQ")]
    assert watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(0))) == []
    watchdog.missed(watched, [_at(1), _at(2)])
    (page,) = watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=("QQQ",), at=_at(3)))
    assert page.tickers == ("QQQ",)
    assert page.minutes == 4


def test_the_page_reports_the_longest_out_whatever_its_place_in_the_roster():
    # IWM comes first in roster order and joined last, so the minutes are QQQ's.
    watchdog = Watchdog()
    outs = [("QQQ",)] * 3 + [("IWM", "QQQ")] * 3
    raised = []
    for m, out in enumerate(outs):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=out, at=_at(m)))
    assert [page.minutes for page in raised] == [3, 6]


def test_a_ticker_below_the_threshold_when_another_pages_still_pages_itself():
    # IWM joins one cycle after QQQ, so it is named on QQQ's page before it has tripped.
    # Its own page still comes when it reaches the threshold.
    watchdog = Watchdog()
    outs = [("QQQ",)] * 2 + [("QQQ", "IWM")] * 3
    raised = []
    for m, out in enumerate(outs):
        raised += watchdog.observe(_clamped(_seg("quotes", "SPY", "data"), out=out, at=_at(m)))
    assert [page.tickers for page in raised] == [("QQQ", "IWM"), ("QQQ", "IWM")]


# -- a chain that answered and brought no contract (marketlake #326) -----------------


def _empty_chain(ticker: str, error_class: str | None = None, *, rows: int = 1) -> SegmentOutcome:
    """A chains data segment holding no contract.

    ``rows`` defaults to one, the failed window's absence marker, because a fixture with
    no row at all cannot tell a count of data rows from a count of every row.
    ``error_class`` is what the segment recorded: none when every window answered empty,
    and the first failed window's class when one failed beside it.
    """
    return _seg("chains", ticker, "data", rows=rows, data_rows=0, error_class=error_class)


@pytest.mark.parametrize("rows", [1, 0])
def test_a_data_segment_holding_no_data_row_fails_and_pages_why(rows):
    """A segment's kind says what the writer planned, not whether the chain produced.

    One window answering 200 with empty maps while another fails lands a data segment
    whose only row is the failed window's marker. It used to reset the counter, so a chain
    producing nothing read as a healthy minute and never paged.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _empty_chain("SPY", "http_429" if rows else None, rows=rows),
                _seg("quotes", "SPY", "data"),
                at=_at(minute),
            )
        )
        assert watchdog.count("chains", "SPY") == minute + 1
    assert [(page.title, page.cause) for page in raised] == [
        ("Capture down: SPY chains", CONTRACTS_ABSENT)
    ]


def test_a_chain_that_lost_some_windows_and_landed_the_rest_still_resets():
    """The boundary on the other side, which marketlake #553 owns.

    A partial chain holds real contracts beside its markers, so it produced. This fix
    changes the reset from a segment's kind to its data rows and moves nothing else.
    """
    watchdog = Watchdog()
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(0)))
    watchdog.observe(_cycle(_seg("chains", "SPY", "gap"), at=_at(1)))
    partial = _seg("chains", "SPY", "data", rows=3, data_rows=2, error_class="http_429")
    raised = []
    for minute in range(2, 8):
        raised += watchdog.observe(_cycle(partial, at=_at(minute)))
    assert watchdog.count("chains", "SPY") == 0
    assert raised == []


def _roster_401(minute: int) -> CycleResult:
    return _cycle(
        _fail("chains", "SPY", "http_401"),
        _fail("chains", "QQQ", "http_401"),
        _fail("quotes", "SPY", "http_401"),
        _fail("quotes", "QQQ", "http_401"),
        at=_at(minute),
    )


@pytest.mark.parametrize("recorded", [None, "http_401"])
def test_a_chain_that_answered_empty_breaks_the_unanimity_of_a_dead_token(recorded):
    """A 200 proves a request authenticated, so the minute is not one dead token.

    Two readings would fold it anyway. A segment naming no class was skipped when the
    classes were gathered, so the rest agreed. And the mixed shape records its failed
    window's class, so one 401 on one window made the surface read as a dead token too.
    Both paged ``Capture down: token dead`` at a daemon authenticating fine. The surface
    pages what it is failing with instead, and the quotes collapse keeps its class.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _empty_chain("SPY", recorded),
                _empty_chain("QQQ", recorded),
                _fail("quotes", "SPY", "http_401"),
                _fail("quotes", "QQQ", "http_401"),
                at=_at(minute),
            )
        )
    assert [(page.title, page.cause) for page in raised] == [
        ("Capture down: quote sampler dead", "http_401"),
        ("Capture down: QQQ chains", CONTRACTS_ABSENT),
        ("Capture down: SPY chains", CONTRACTS_ABSENT),
    ]
    assert watchdog._paged_causes == {}


def test_a_chain_that_answered_empty_leaves_the_cause_so_the_next_death_pages():
    """A second token death in one session must page, and a held surface silenced it.

    A cause stays live until every surface it named is released, and while it is live no
    page goes out under its title. A chain answering 200 with no contract produced
    nothing, so it was never released, and the cause it held outlived the outage. The
    answer proves the token works, so the surface leaves the cause, and its own page goes
    out at once because its counter kept climbing under the cause.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(_roster_401(minute))
    assert [page.title for page in raised] == ["Capture down: token dead"]
    for minute in range(3, 7):
        raised += watchdog.observe(
            _cycle(
                _empty_chain("SPY"),
                _seg("chains", "QQQ", "data"),
                _seg("quotes", "SPY", "data"),
                _seg("quotes", "QQQ", "data"),
                at=_at(minute),
            )
        )
    assert watchdog._paged_causes == {}
    assert watchdog.count("chains", "SPY") == 7
    for minute in range(7, 10):
        raised += watchdog.observe(_roster_401(minute))
    # The second death began at minute 7, so its page counts 3 minutes from 10:07. The
    # empty chain's own count of 10 reaches back into the first outage and the minutes
    # between, when the token worked (marketlake #747).
    assert [(page.title, page.minutes, page.cause, page.since) for page in raised] == [
        ("Capture down: token dead", 3, "http_401", _at(0)),
        ("Capture down: SPY chains", 4, CONTRACTS_ABSENT, None),
        ("Capture down: token dead", 3, "http_401", _at(7)),
    ]


def test_a_cause_page_is_dated_from_the_death_not_from_one_surface_failing_before_it():
    """The cause page counts the run in which none of its surfaces landed data.

    One chain failing its own class for half an hour before the token dies has a count
    that reaches back to 10:00. Dating the cause from it would tell the operator the token
    died at 10:00 and that capture had been down since then, when every other surface
    landed data until 10:30. The dead-man is fed until 10:30 too, so the smallest count is
    the one that matches the outage the follow-on line names (marketlake #747).
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(30):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_500"),
                _seg("chains", "QQQ", "data"),
                _seg("quotes", "SPY", "data"),
                _seg("quotes", "QQQ", "data"),
                at=_at(minute),
            )
        )
    assert [(page.title, page.since) for page in raised] == [("Capture down: SPY chains", None)]
    for minute in range(30, 34):
        raised += watchdog.observe(_roster_401(minute))
    assert watchdog.count("chains", "SPY") == 34
    cause = raised[-1]
    assert (cause.title, cause.minutes, cause.since) == ("Capture down: token dead", 3, _at(30))
    assert len(raised) == 2


def test_a_cause_that_turns_unanimous_late_is_dated_from_the_first_dead_minute():
    """The page counts the minutes every surface was down, not the threshold.

    For four minutes the chains fail with ``http_401`` and the quotes with
    ``vendor_auth_error``. Every surface is down, but two classes are not one cause, so no
    cause page fires. On the fifth minute both agree. The outage began at 10:00, and the
    page has to say five minutes since then rather than three minutes counted back from
    the slot that tipped it (marketlake #747).
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "vendor_auth_error"),
                at=_at(minute),
            )
        )
    assert "Capture down: token dead" not in [page.title for page in raised]
    raised += watchdog.observe(
        _cycle(_fail("chains", "SPY", "http_401"), _fail("quotes", "SPY", "http_401"), at=_at(4))
    )
    cause = raised[-1]
    assert (cause.title, cause.minutes, cause.since) == ("Capture down: token dead", 5, _at(0))


def test_a_surface_whose_write_failed_still_dates_the_cause():
    """The smallest count is taken over every failed surface, written or not.

    The chains fail with ``http_401`` for eight minutes. The quotes land data for the first
    five, then their segment write fails for three, so they record no class. The outage
    that starves the dead-man began at 10:05, when the quotes stopped landing. Taking the
    smallest count over only the surfaces that recorded a class would date it from 10:00.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(5):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"), _seg("quotes", "SPY", "data"), at=_at(minute)
            )
        )
    for minute in range(5, 8):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                errors=(SegmentError("quotes", "SPY", "os_error"),),
                at=_at(minute),
            )
        )
    cause = raised[-1]
    assert (cause.title, cause.minutes, cause.since) == ("Capture down: token dead", 3, _at(5))


def test_a_cause_page_under_a_lower_threshold_is_dated_from_the_first_dead_minute():
    """At a threshold of 2 the page fires on the second dead minute and dates from the first."""
    watchdog = Watchdog(page_minutes=2)
    raised = []
    for minute in range(2):
        raised += watchdog.observe(_roster_401(minute))
    assert [(page.minutes, page.since) for page in raised] == [(2, _at(0))]


def test_a_chain_that_answered_empty_leaves_only_its_own_surface_in_the_cause():
    """The release is the surface's own, and the cause keeps the surfaces still down."""
    watchdog = Watchdog()
    for minute in range(3):
        watchdog.observe(_roster_401(minute))
    watchdog.observe(
        _cycle(
            _empty_chain("SPY"),
            _fail("chains", "QQQ", "http_401"),
            _fail("quotes", "SPY", "timeout"),
            _fail("quotes", "QQQ", "http_401"),
            at=_at(3),
        )
    )
    assert watchdog._paged_causes == {
        "Capture down: token dead": {
            Surface("chains", "QQQ"),
            Surface("quotes", "SPY"),
            Surface("quotes", "QQQ"),
        }
    }


def test_a_write_failure_beside_a_dead_token_still_folds_under_it():
    """A write failure says nothing about the vendor, so it is left out of the unanimity.

    Its class names the page of its own surface. It does not stand beside the vendor's
    classes when the watchdog asks whether every surface failed the same way, which is
    the rule the design states for a segment that could not be written.
    """
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "http_401"),
                _fail("quotes", "SPY", "http_401"),
                errors=(SegmentError("chains", "QQQ", "os_error"),),
                at=_at(minute),
            )
        )
    assert [(page.title, len(page.surfaces)) for page in raised] == [
        ("Capture down: token dead", 3)
    ]


# -- a write failure leaves the cause once the vendor answers (marketlake #754) -------

TOKEN_DEAD_TITLE = "Capture down: token dead"
QQQ_CHAINS = "Capture down: QQQ chains"
WRITE_FAILED = SegmentError("chains", "QQQ", "os_error")


def _write_failing(minute: int, error_class: str | None) -> CycleResult:
    """One minute of the issue's probe: QQQ chains fails its write, every other surface not.

    ``chains SPY``, ``quotes SPY`` and ``quotes QQQ`` each fail with ``error_class``, or
    land data for ``None``. ``chains QQQ`` cannot be written in any minute.
    """

    def surface(name: str, ticker: str) -> SegmentOutcome:
        if error_class is None:
            return _seg(name, ticker, "data")
        return _fail(name, ticker, error_class)

    return _cycle(
        surface("chains", "SPY"),
        surface("quotes", "SPY"),
        surface("quotes", "QQQ"),
        errors=(WRITE_FAILED,),
        at=_at(minute),
    )


def _per_minute(watchdog: Watchdog, cycles: list[CycleResult]) -> dict[int, list[tuple]]:
    """The title and class of every page each cycle raised, keyed by the cycle's index.

    A cycle that raised nothing is left out, so the result also says when each page went.
    """
    raised = {
        minute: [(page.title, page.cause) for page in watchdog.observe(cycle)]
        for minute, cycle in enumerate(cycles)
    }
    return {minute: pages for minute, pages in raised.items() if pages}


def test_a_write_failure_that_outlives_a_token_death_pages_and_frees_the_cause():
    """Test 1: the issue's probe. A second token death in the session must page.

    A write failure records no class, so a token-dead cause that named it kept it, the
    surface never paged, and the cause stayed live, so the second death paged nothing.
    The first healed minute proves the vendor answered, so the write failure leaves the
    cause and pages for itself in that minute.
    """
    cycles = (
        [_write_failing(minute, "http_401") for minute in range(3)]
        + [_write_failing(minute, None) for minute in range(3, 8)]
        + [_write_failing(minute, "http_401") for minute in range(8, 12)]
    )
    assert _per_minute(Watchdog(), cycles) == {
        2: [(TOKEN_DEAD_TITLE, "http_401")],
        3: [(QQQ_CHAINS, "os_error")],
        10: [(TOKEN_DEAD_TITLE, "http_401")],
    }


def test_a_surface_that_recorded_the_cause_and_then_failed_its_write_leaves_it_too():
    """Test 2: membership is not decided once, at the page.

    QQQ chains recorded the 401 when the cause paged, and only then started failing its
    write. Keeping the surfaces that recorded a class at page time would keep this one,
    and the cause would hold it for the rest of the session the same way.
    """

    def dead(minute: int, *, qqq_writes: bool) -> CycleResult:
        surfaces = [
            _fail("chains", "SPY", "http_401"),
            _fail("quotes", "SPY", "http_401"),
            _fail("quotes", "QQQ", "http_401"),
        ]
        if qqq_writes:
            return _cycle(*surfaces, _fail("chains", "QQQ", "http_401"), at=_at(minute))
        return _cycle(*surfaces, errors=(WRITE_FAILED,), at=_at(minute))

    cycles = (
        [dead(minute, qqq_writes=True) for minute in range(3)]
        + [dead(minute, qqq_writes=False) for minute in range(3, 5)]
        + [_write_failing(minute, None) for minute in range(5, 10)]
        + [dead(minute, qqq_writes=False) for minute in range(10, 14)]
    )
    assert _per_minute(Watchdog(), cycles) == {
        2: [(TOKEN_DEAD_TITLE, "http_401")],
        5: [(QQQ_CHAINS, "os_error")],
        12: [(TOKEN_DEAD_TITLE, "http_401")],
    }


def test_a_minute_whose_every_write_failed_inside_a_token_death_sends_nothing_new():
    """Test 3: a write failure leaves the cause only when another surface landed data.

    A disk that refuses every write in one minute of a token death proves nothing about
    the vendor. Releasing the write failures there emptied the cause, paged each of them,
    and paged the dead token a second time on the next minute.
    """
    watchdog = Watchdog()
    every_write = (
        SegmentError("chains", "SPY", "os_error"),
        SegmentError("quotes", "SPY", "os_error"),
        SegmentError("quotes", "QQQ", "os_error"),
        WRITE_FAILED,
    )
    cycles = (
        [_write_failing(minute, "http_401") for minute in range(3)]
        + [_cycle(errors=every_write, at=_at(3))]
        + [_write_failing(minute, "http_401") for minute in range(4, 8)]
    )
    assert _per_minute(watchdog, cycles) == {2: [(TOKEN_DEAD_TITLE, "http_401")]}
    assert watchdog._paged_causes == {
        TOKEN_DEAD_TITLE: {
            Surface("chains", "SPY"),
            Surface("quotes", "SPY"),
            Surface("quotes", "QQQ"),
            Surface("chains", "QQQ"),
        }
    }


def test_a_stall_inside_a_token_death_holding_a_write_failure_sends_nothing():
    """Test 4: the write failure stays inside the cause while nothing lands.

    A stall gaps every surface at once and attempts no request, so it is evidence about
    the loop and never about the vendor. Had the write failure left the cause on a dead
    minute, the stall would page ``loop stalled`` inside an outage that already paged.
    """
    watchdog = Watchdog()
    cycles = [_write_failing(minute, "http_401") for minute in range(4)]
    assert _per_minute(watchdog, cycles) == {2: [(TOKEN_DEAD_TITLE, "http_401")]}
    watched = [
        Surface("chains", "SPY"),
        Surface("quotes", "SPY"),
        Surface("quotes", "QQQ"),
        Surface("chains", "QQQ"),
    ]
    assert watchdog.missed(watched, [_at(minute) for minute in range(4, 10)]) == []


def test_a_write_failure_pages_while_another_surface_still_fails_the_cause_s_way():
    """Test 5: the release is per surface, not the whole cause at once.

    A rate limit that lets most requests through while one quotes surface stays limited
    is the normal shape of a 429. Dropping the cause only once every surface it held
    failed its write kept the write failure silent for as long as that one surface stayed
    limited. The write failure pages on the first minute the vendor answers, and the
    cause keeps the surface still limited.
    """
    watchdog = Watchdog()
    cycles = [_write_failing(minute, "http_429") for minute in range(3)] + [
        _cycle(
            _seg("chains", "SPY", "data"),
            _fail("quotes", "SPY", "http_429"),
            _seg("quotes", "QQQ", "data"),
            errors=(WRITE_FAILED,),
            at=_at(minute),
        )
        for minute in range(3, 33)
    ]
    assert _per_minute(watchdog, cycles) == {
        2: [("Capture down: rate limited", "http_429")],
        3: [(QQQ_CHAINS, "os_error")],
    }
    assert watchdog._paged_causes == {"Capture down: rate limited": {Surface("quotes", "SPY")}}


def test_a_chain_alternating_a_401_and_an_empty_answer_does_not_re_page_the_cause():
    """Test 6: an answer with no contract does not count as the vendor answering here.

    Every surface but one chain fails its write, and that chain alternates a 401 with an
    empty 200 answer. The empty answer resets no counter, so counting it released every
    write failure, emptied the cause, and the next 401 paged the dead token again at
    once. A probe measured 19 pages in 40 minutes, where 2 go out.

    The assertion also records the guard's price. Nothing lands data in any of the 40
    minutes, so the write-failing surfaces stay in the cause and send no page. The
    dead-man starves and pages that outage, and the chain's ``contracts_absent`` page
    still goes out.
    """
    writes = (
        SegmentError("quotes", "SPY", "os_error"),
        SegmentError("quotes", "QQQ", "os_error"),
        WRITE_FAILED,
    )

    def chain(minute: int) -> SegmentOutcome:
        if minute < 3 or minute % 2 == 0:
            return _fail("chains", "SPY", "http_401")
        return _empty_chain("SPY")

    cycles = [_cycle(chain(minute), errors=writes, at=_at(minute)) for minute in range(40)]
    assert _per_minute(Watchdog(), cycles) == {
        2: [(TOKEN_DEAD_TITLE, "http_401")],
        3: [("Capture down: SPY chains", CONTRACTS_ABSENT)],
    }


def test_one_surface_landing_data_releases_a_write_failure_from_the_cause():
    """A single surface landing data proves the vendor answered.

    Only SPY chains lands in the healed minute, while QQQ chains still fails its write.
    A guard that asked for more than one landed surface would keep the write failure
    inside the token-dead cause, and it would never page for itself.
    """
    cycles = [
        _cycle(_fail("chains", "SPY", "http_401"), errors=(WRITE_FAILED,), at=_at(minute))
        for minute in range(3)
    ] + [_cycle(_seg("chains", "SPY", "data"), errors=(WRITE_FAILED,), at=_at(3))]
    assert _per_minute(Watchdog(), cycles) == {
        2: [(TOKEN_DEAD_TITLE, "http_401")],
        3: [(QQQ_CHAINS, "os_error")],
    }


def test_a_surface_that_lands_data_leaves_every_cause_that_holds_it():
    """A release reaches a later cause even when an earlier cause no longer holds the surface.

    SPY chains left the token-dead cause when it landed at minute 3, and then joined the
    rate-limited cause. Stopping at the first cause that does not hold it would keep it in
    the rate-limited cause after it lands again, so that cause would never re-arm.
    """
    a, b = ("chains", "SPY"), ("quotes", "SPY")

    def minute(n: int, a_state: str | None, b_state: str | None) -> CycleResult:
        def one(key, state):
            return _seg(*key, "data") if state is None else _fail(*key, state)

        return _cycle(one(a, a_state), one(b, b_state), at=_at(n))

    watchdog = Watchdog()
    for n in range(3):
        watchdog.observe(minute(n, "http_401", "http_401"))
    watchdog.observe(minute(3, None, "http_401"))
    for n in range(4, 7):
        watchdog.observe(minute(n, "http_429", "http_429"))
    assert watchdog._paged_causes == {
        TOKEN_DEAD_TITLE: {Surface(*b)},
        "Capture down: rate limited": {Surface(*a), Surface(*b)},
    }
    watchdog.observe(minute(7, None, "http_429"))
    assert watchdog._paged_causes == {
        TOKEN_DEAD_TITLE: {Surface(*b)},
        "Capture down: rate limited": {Surface(*b)},
    }


def test_a_cycle_that_touched_nothing_releases_no_cause():
    """A cycle with no surface in it is evidence about no surface.

    Treating every held surface as retired in that cycle would empty the token-dead cause,
    and the 401s that follow would page the dead token a second time.
    """
    watchdog = Watchdog()
    cycles = [_write_failing(minute, "http_401") for minute in range(3)]
    cycles.append(_cycle(at=_at(3)))
    cycles += [_write_failing(minute, "http_401") for minute in range(4, 7)]
    assert _per_minute(watchdog, cycles) == {2: [(TOKEN_DEAD_TITLE, "http_401")]}


# -- the whole-daemon rule as one public reading (marketlake #702) ---------------------

# Each case is one cycle, and the cause title the rule gives it. The daemon spawns a token
# pull on the title, and the watchdog pages under it, so both read the same rule.
WHOLE_DAEMON_CASES = [
    pytest.param(
        (_fail("chains", "SPY", "http_401"), _fail("quotes", "SPY", "http_401")),
        (),
        "Capture down: token dead",
        id="every-surface-401",
    ),
    pytest.param(
        (
            _fail("chains", "SPY", "token_file_unreadable"),
            _fail("quotes", "SPY", "token_file_unreadable"),
        ),
        (),
        "Capture down: token dead",
        id="every-surface-token-file-unreadable",
    ),
    pytest.param(
        (_fail("chains", "SPY", "vendor_auth_error"), _fail("quotes", "SPY", "vendor_auth_error")),
        (),
        "Capture down: token dead",
        id="every-surface-vendor-auth-error",
    ),
    pytest.param(
        (_fail("chains", "SPY", "http_429"), _fail("quotes", "SPY", "http_429")),
        (),
        "Capture down: rate limited",
        id="every-surface-429",
    ),
    pytest.param((_fail("quotes", "SPY", "http_401"),), (), None, id="one-surface"),
    pytest.param(
        (_fail("chains", "SPY", "http_401"), _fail("quotes", "SPY", "vendor_auth_error")),
        (),
        None,
        id="two-auth-classes",
    ),
    pytest.param(
        (_fail("chains", "SPY", "http_401"), _seg("quotes", "SPY", "data")),
        (),
        None,
        id="one-surface-landed",
    ),
    pytest.param(
        (_fail("chains", "SPY", "http_500"), _fail("quotes", "SPY", "http_500")),
        (),
        None,
        id="unmapped-class",
    ),
    pytest.param(
        (_seg("chains", "SPY", "data", data_rows=0), _fail("quotes", "SPY", "http_401")),
        (),
        None,
        id="a-chain-with-no-contract",
    ),
    pytest.param(
        (_fail("chains", "SPY", "http_401"), _fail("quotes", "SPY", "http_401")),
        (SegmentError("chains", "QQQ", "os_error"),),
        "Capture down: token dead",
        id="a-write-failure-records-no-class",
    ),
    pytest.param(
        (_fail("quotes", "SPY", "http_401"),),
        (SegmentError("chains", "SPY", "os_error"),),
        "Capture down: token dead",
        id="a-write-failure-counts-as-touched",
    ),
]


@pytest.mark.parametrize(("segments", "errors", "title"), WHOLE_DAEMON_CASES)
def test_the_whole_daemon_rule_reads_one_cycle(segments, errors, title):
    assert whole_daemon_cause(_cycle(*segments, errors=errors)) == title


@pytest.mark.parametrize(("segments", "errors", "title"), WHOLE_DAEMON_CASES)
def test_the_watchdog_pages_the_cause_the_public_rule_names(segments, errors, title):
    # The page decision and the daemon's pull read one rule. So at the threshold the
    # watchdog sends a page under the title the public function gave the cycle, and sends
    # no cause page when it gave none. A copy of the rule kept inside the watchdog would
    # pass the case above and drift here.
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(_cycle(*segments, errors=errors, at=_at(minute)))
    causes = {"Capture down: token dead", "Capture down: rate limited"}
    assert [page.title for page in raised if page.title in causes] == (
        [title] if title is not None else []
    )


def test_an_unreadable_token_file_pages_token_dead_and_names_its_class():
    # The page names the class, which is what tells a file the cycle could not read apart
    # from a token Schwab refused. A newer token repairs both.
    watchdog = Watchdog()
    raised = []
    for minute in range(4):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", "token_file_unreadable"),
                _fail("quotes", "SPY", "token_file_unreadable"),
                at=_at(minute),
            )
        )
    assert [(page.title, page.cause) for page in raised] == [
        ("Capture down: token dead", "token_file_unreadable")
    ]


def test_a_dead_token_and_an_unreadable_token_file_are_one_outage():
    # One title, one page. A dead token can arrive as a 401 and then as a refused refresh,
    # and a pull that left a file the cycle cannot read lands as a third class. The cause
    # is the title, so the outage pages once rather than once per class.
    watchdog = Watchdog()
    raised = []
    classes = ["http_401"] * 3 + ["vendor_auth_error"] * 3 + ["token_file_unreadable"] * 3
    for minute, error_class in enumerate(classes):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "SPY", error_class),
                _fail("quotes", "SPY", error_class),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == ["Capture down: token dead"]


def test_a_cycle_whose_every_write_failed_names_no_cause_and_does_not_raise():
    # A full disk fails every segment write, so the cycle touches every surface through
    # ``errors`` alone and records no class at all. The rule reads that as no cause rather
    # than taking a class out of an empty set, and the watchdog keeps counting.
    errors = (SegmentError("chains", "SPY", "os_error"), SegmentError("quotes", "SPY", "os_error"))
    assert whole_daemon_cause(_cycle(errors=errors)) is None
    watchdog = Watchdog()
    raised = []
    for minute in range(3):
        raised += watchdog.observe(_cycle(errors=errors, at=_at(minute)))
    assert "Capture down: token dead" not in [page.title for page in raised]
    assert watchdog.count("chains", "SPY") == 3
