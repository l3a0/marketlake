"""The watchdog's counters and the pages they raise."""

from __future__ import annotations

from datetime import datetime, timedelta
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
    # SPY had been down three minutes before QQQ joined it, and the page reports the
    # longest of the two counters. Reporting the shortest would halve the outage.
    assert raised[1].minutes == 6


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
    assert [p.title for p in raised] == ["Capture down: token dead"]
    assert raised[0].cause == "http_401"
    assert len(raised[0].surfaces) == 4


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
    assert [page.title for page in pages] == ["Capture down: SPY chains"]
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
    # quotes SPY comes back, which keeps the cycle off the whole-daemon path. That path
    # returns early while a cause is live and would never reach the per-surface pages.
    for minute in range(3, 7):
        raised += watchdog.observe(
            _cycle(
                _fail("chains", "QQQ", "http_429"),
                _seg("quotes", "SPY", "data"),
                errors=(SegmentError("chains", "SPY", "vendor_auth_error"),),
                at=_at(minute),
            )
        )
    assert [page.title for page in raised] == [
        "Capture down: rate limited",
        "Capture down: SPY chains",
    ]
    assert raised[1].cause == "vendor_auth_error"
