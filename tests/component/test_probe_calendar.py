"""The 09:35 says-closed-but-open probe."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from lake import journal, probe_calendar
from lake.control_plane import CALENDAR_PROBE_SLUG
from lake.probe_calendar import ProbeResult, Reading, read_batch, run_probe
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock

ET = ZoneInfo("America/New_York")
WEEK = date(2026, 8, 31)
SATURDAY = date(2026, 9, 5)


def _quote(at: datetime) -> dict:
    return {"quote": {"quoteTime": int(at.timestamp() * 1000)}}


def _probe(at: datetime, quotes: dict | None = None, *, boom: bool = False) -> ProbeResult:
    def fetch(symbols):
        if boom:
            raise ConnectionError("vendor unreachable")
        return quotes or {}

    return run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=at),
        symbols=["SPY", "QQQ"],
        fetch=fetch,
    )


def test_a_session_day_asks_the_vendor_nothing():
    # The calendar and the daemon already agree. A vendor call would buy nothing.
    asked = []
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 2, 9, 35)),
        symbols=["SPY"],
        fetch=lambda symbols: asked.append(symbols) or {},
    )
    assert not result.checked
    assert not result.pages
    assert asked == []


def test_a_market_trading_on_a_day_the_calendar_calls_closed_pages():
    # The whole point. Nothing else would notice, because every other check agrees with
    # the calendar that today is not a session.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 5, 9, 34)), "QQQ": _quote(et(2026, 9, 5, 9, 33))},
    )
    assert result.checked
    assert result.pages
    assert result.trading == ("QQQ", "SPY")


def test_a_stale_quote_on_a_closed_day_is_the_ordinary_case():
    # A weekend carries Friday's last quote. That is the market being shut, not open.
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": _quote(et(2026, 9, 4, 16, 0))})
    assert result.checked
    assert not result.pages
    assert result.trading == ()


def test_one_trading_symbol_among_stale_ones_still_pages():
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 4, 16, 0)), "QQQ": _quote(et(2026, 9, 5, 9, 34))},
    )
    assert result.trading == ("QQQ",)
    assert result.pages


def test_an_unreachable_vendor_is_a_problem_and_never_a_page():
    # The probe catches a wrong calendar. An unreachable vendor is no evidence either
    # way, and paging on it would train the operator to ignore this check.
    result = _probe(et(2026, 9, 5, 9, 35), boom=True)
    assert result.checked
    assert not result.pages
    assert result.problem == "vendor unreachable: ConnectionError"


@pytest.mark.parametrize(
    "envelope",
    [
        {},
        {"quote": {}},
        {"quote": {"quoteTime": None}},
        {"quote": {"quoteTime": "not a number"}},
        "not a dict",
    ],
)
def test_a_quote_the_probe_cannot_read_is_not_evidence_of_trading(envelope):
    assert read_batch({"SPY": envelope}, SATURDAY).trading == ()


def test_freshness_is_same_day_in_market_time_not_a_seconds_window():
    # The question is whether the market traded at all today. A stamp from a prior
    # session answers it as clearly as one from an hour ago, and a seconds threshold
    # would have to be guessed since the design pins none.
    day = date(2026, 9, 5)
    just_before_midnight = datetime(2026, 9, 5, 0, 1, tzinfo=ET)
    long_ago_but_today = datetime(2026, 9, 5, 23, 58, tzinfo=ET)
    assert read_batch({"A": _quote(just_before_midnight)}, day).trading == ("A",)
    assert read_batch({"A": _quote(long_ago_but_today)}, day).trading == ("A",)
    yesterday = _quote(datetime(2026, 9, 5, 0, 1, tzinfo=ET) - timedelta(hours=2))
    assert read_batch({"A": yesterday}, day).trading == ()


# -- a stamp that arrived and cannot be read -----------------------------------------


REFUSED = [True, False, "not a number", 10**20, {}, [], float("nan")]
ABSENT = [{}, {"quote": {}}, {"quote": {"quoteTime": None}}, "not a dict", {"quote": "not a dict"}]


def _stamped(value: object) -> dict:
    return {"quote": {"quoteTime": value}}


def test_a_bool_quote_time_is_refused_rather_than_read_as_a_1969_stamp():
    # `int(True)` is 1, so the probe's own copy of the transform turned a vendor `true`
    # into one millisecond past the epoch with nothing raised. Dropping the symbol is
    # the small cost. The 1969 date is the large one, and a probe run on that date would
    # have read the bool as a trading market.
    reading = read_batch({"SPY": _stamped(True)}, SATURDAY)
    assert reading.refused == 1
    assert reading.readable == 0
    assert read_batch({"SPY": _stamped(True)}, date(1969, 12, 31)).trading == ()


@pytest.mark.parametrize("value", REFUSED)
def test_every_unreadable_stamp_shape_is_refused_rather_than_converted(value):
    reading = read_batch({"SPY": _stamped(value)}, SATURDAY)
    assert reading == Reading(trading=(), readable=0, refused=1, absent=0)


def test_a_refused_stamp_is_no_evidence_of_trading_and_the_symbol_beside_it_pages():
    # The refusal must not take the batch down with it. A vendor that sent junk for one
    # symbol still answered the question the probe asked, as long as another symbol
    # carries today's stamp.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _stamped(True), "QQQ": _quote(et(2026, 9, 5, 9, 34))},
    )
    assert result.trading == ("QQQ",)
    assert result.pages
    assert result.refused == 1
    assert result.problem is None


def test_a_batch_that_read_nothing_reports_a_problem_and_pages_nobody():
    # Silence caused by unreadable stamps is otherwise indistinguishable from a market
    # that is genuinely shut, and that session is what the probe exists to save.
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": _stamped(True), "QQQ": _stamped("junk")})
    assert result.checked
    assert not result.pages
    assert result.trading == ()
    assert result.refused == 2
    assert result.problem == "no readable quote time (2 unreadable, 0 absent)"


def test_a_refusal_beside_an_absent_stamp_still_reports_a_problem():
    # A batch of refusals and silences is a batch nothing could be read from, which is
    # the state the problem names. The absent stamp carries no evidence either way, so
    # it neither raises the problem nor argues against it.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _stamped(True), "QQQ": {"quote": {}}},
    )
    assert result.refused == 1
    assert result.problem == "no readable quote time (1 unreadable, 1 absent)"
    assert not result.pages


@pytest.mark.parametrize("envelope", ABSENT)
def test_an_absent_stamp_is_not_a_refusal(envelope):
    # Every caller reads a missing stamp as not trading, and that is the right answer
    # for a symbol the vendor stayed quiet about. Only a refusal is new information, so
    # turning silence into a problem would report one on every quiet day.
    reading = read_batch({"SPY": envelope}, SATURDAY)
    assert reading == Reading(trading=(), readable=0, refused=0, absent=1)


def test_a_batch_of_absent_stamps_reports_no_problem():
    result = _probe(et(2026, 9, 5, 9, 35), {sym: {"quote": {}} for sym in ("SPY", "QQQ")})
    assert result.checked
    assert not result.pages
    assert result.refused == 0
    assert result.problem is None


def test_a_stale_stamp_beside_a_refused_one_is_still_the_ordinary_closed_day():
    # A holiday answers the probe with the prior session's stamp, which reads fine and
    # simply is not today. One junk symbol beside it does not make the day a problem.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 4, 16, 0)), "QQQ": _stamped(True)},
    )
    assert result.trading == ()
    assert not result.pages
    assert result.refused == 1
    assert result.problem is None


def test_the_three_counts_account_for_every_symbol_in_the_batch():
    # `Reading` promises the counts sum to the batch size. A count that silently drops a
    # symbol is how a batch reads as smaller than the one the vendor answered.
    quotes = {
        "AAA": _quote(et(2026, 9, 5, 9, 34)),
        "BBB": _quote(et(2026, 9, 4, 16, 0)),
        "CCC": _stamped(True),
        "DDD": {"quote": {}},
        "EEE": "not a dict",
    }
    reading = read_batch(quotes, SATURDAY)
    assert reading.readable + reading.refused + reading.absent == len(quotes)
    assert reading == Reading(trading=("AAA",), readable=2, refused=1, absent=2)


def test_a_quote_time_in_float_notation_reads_as_a_stamp():
    # The shared transform converts with `float`, and the lake already holds values in
    # this shape. The probe's own copy used `int`, which refused them.
    at = et(2026, 9, 5, 9, 34)
    reading = read_batch({"SPY": _stamped(f"{at.timestamp() * 1000:.6e}")}, SATURDAY)
    assert reading.trading == ("SPY",)
    assert reading.refused == 0


def test_an_out_of_range_epoch_is_refused_rather_than_raising_out_of_the_probe():
    # `OverflowError` was absent from the probe's own except tuple. `10**400` is the
    # value that reached it: `int(raw) / 1000` raises `OverflowError` there and took the
    # whole probe down, which cost the healthchecks ping as well as the answer. A
    # smaller out-of-range epoch like `10**20` raises `OSError`, which the old tuple
    # caught, so it would not tell the two behaviours apart.
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": _stamped(10**400)})
    assert result.checked
    assert result.refused == 1
    assert result.problem == "no readable quote time (1 unreadable, 0 absent)"


def test_the_transform_is_the_shared_one_rather_than_a_third_copy():
    # A third copy of one transform is what let the bool through here after #223 fixed
    # it on the two capture surfaces. Comparing instants alone does not say which
    # transform ran, because the copy named the same instant in market time. The zone
    # does say it: the shared transform returns UTC and the copy returned MARKET_TZ.
    stamp = probe_calendar._quote_time(_stamped("1758000000000"))
    assert stamp == journal.epoch_ms_to_utc("1758000000000")
    assert stamp.utcoffset() == timedelta(0)


# -- the entry that actually pages ---------------------------------------------------


class Sink:
    def __init__(self) -> None:
        self.sent = []

    def publish(self, message, *, now):
        self.sent.append(message)
        return None


class Pings:
    def __init__(self) -> None:
        self.urls = []

    def ping(self, url: str) -> None:
        self.urls.append(url)


def test_a_market_found_open_is_actually_sent(tmp_path):
    """The page must reach the publisher, not just be decided on.

    `main` had no test, which is how a publisher with no transport shipped: it recorded
    the page to a file and returned, and nothing noticed.
    """
    from lake.probe_calendar import PAGE_TITLE, report

    sink, pings = Sink(), Pings()
    result = ProbeResult(date(2026, 9, 5), checked=True, trading=("SPY",))
    code = report(
        result,
        publisher=sink,
        pinger=pings,
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    assert code == 1
    assert [m.title for m in sink.sent] == [PAGE_TITLE]
    assert sink.sent[0].priority == 5
    assert "SPY" in sink.sent[0].body


def test_the_check_is_fed_on_the_day_it_pages_too():
    # The check's silence must mean the probe stopped running. A day that pages is
    # exactly a day it ran.
    from lake.probe_calendar import report

    pings = Pings()
    report(
        ProbeResult(date(2026, 9, 5), checked=True, trading=("SPY",)),
        publisher=Sink(),
        pinger=pings,
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    assert pings.urls == ["https://x/y"]


def test_a_quiet_day_feeds_the_check_and_pages_nobody():
    from lake.probe_calendar import report

    sink, pings = Sink(), Pings()
    code = report(
        ProbeResult(date(2026, 9, 5), checked=True),
        publisher=sink,
        pinger=pings,
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    assert code == 0
    assert sink.sent == []
    assert pings.urls == ["https://x/y"]


def test_a_failing_ping_never_costs_the_page():
    from lake.probe_calendar import report

    class Broken:
        def ping(self, url: str) -> None:
            raise OSError("no network")

    sink = Sink()
    code = report(
        ProbeResult(date(2026, 9, 5), checked=True, trading=("SPY",)),
        publisher=sink,
        pinger=Broken(),
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    assert code == 1
    assert len(sink.sent) == 1


def test_the_page_title_is_the_one_the_design_pins():
    from lake.probe_calendar import PAGE_TITLE

    assert PAGE_TITLE == "Calendar says closed, market looks open"


# -- what the operator line says -----------------------------------------------------


def _status(result: ProbeResult, capsys) -> str:
    from lake.probe_calendar import report

    report(
        result,
        publisher=Sink(),
        pinger=Pings(),
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )
    return capsys.readouterr().out.strip()


def test_the_line_tells_an_unreachable_vendor_from_an_unreadable_batch(capsys):
    # `problem` reaches the operator verbatim, because an unreachable vendor and an
    # unreadable batch are different things to go and look at.
    day = date(2026, 9, 5)
    unreachable = ProbeResult(day, checked=True, problem="vendor unreachable: ConnectionError")
    unreadable = ProbeResult(
        day, checked=True, problem="no readable quote time (2 unreadable, 0 absent)", refused=2
    )
    assert _status(unreachable, capsys) == "calendar probe: vendor unreachable: ConnectionError"
    assert _status(unreadable, capsys) == (
        "calendar probe: no readable quote time (2 unreadable, 0 absent)"
    )


def test_a_partly_unreadable_batch_says_so_without_calling_it_a_problem(capsys):
    # The count is what makes a vendor going bad one symbol at a time visible before the
    # day nothing in the batch reads at all.
    result = ProbeResult(date(2026, 9, 5), checked=True, refused=1)
    assert _status(result, capsys) == "calendar probe: calendar agrees, 1 unreadable"


def test_a_clean_closed_day_still_says_the_calendar_agrees(capsys):
    assert _status(ProbeResult(date(2026, 9, 5), checked=True), capsys) == (
        "calendar probe: calendar agrees"
    )


def test_a_session_day_still_carries_the_tag_the_design_words(capsys):
    from lake.probe_calendar import SESSION_DAY_TAG

    assert _status(ProbeResult(date(2026, 9, 2), checked=False), capsys) == (
        f"calendar probe: {SESSION_DAY_TAG}"
    )
