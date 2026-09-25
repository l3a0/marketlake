"""The 09:35 says-closed-but-open probe."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from types import MappingProxyType
from zoneinfo import ZoneInfo

import pytest

from lake import journal, probe_calendar
from lake.control_plane import CALENDAR_PROBE_SLUG
from lake.probe_calendar import ProbeResult, Reading, read_batch, run_probe
from lake.schwab import SchwabVendor
from lake.vendor import VendorResponse
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.schwab import FakeResponse, FakeSchwabClient

ET = ZoneInfo("America/New_York")
WEEK = date(2026, 8, 31)
SATURDAY = date(2026, 9, 5)

# `_probe` builds the vendor reply itself unless a test overrides it. The sentinel keeps
# `None` available as an override, since a `fetch` returning nothing is one of the shapes
# the probe has to report rather than raise on.
NO_OVERRIDE = object()


def _quote(at: datetime) -> dict:
    return {"quote": {"quoteTime": int(at.timestamp() * 1000)}}


def _probe(
    at: datetime,
    quotes: dict | None = None,
    *,
    boom: bool = False,
    status: int = 200,
    reply: object = NO_OVERRIDE,
) -> ProbeResult:
    """Run the probe against a reply in the shape the real vendor hands back.

    ``SchwabVendor.get_quotes`` returns a ``VendorResponse``, and ``main`` wires it
    straight in. This fake returned a bare dict, a shape production never produces, which
    is what hid the probe reading the whole response as a batch of quotes. ``reply``
    overrides the whole thing, for the shapes a ``VendorResponse`` cannot express.
    """

    def fetch(symbols):
        if boom:
            raise ConnectionError("vendor unreachable")
        if reply is not NO_OVERRIDE:
            return reply
        return VendorResponse(status=status, body=quotes if quotes is not None else {}, headers={})

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
        fetch=lambda symbols: asked.append(symbols) or VendorResponse(status=200, body={}),
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
    assert reading == Reading(
        trading=("AAA",), readable=2, refused=1, absent=2, latest=et(2026, 9, 5, 9, 34)
    )


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


# -- the reply the vendor actually hands back ----------------------------------------


def test_the_vendor_response_the_daemon_wires_is_unwrapped_rather_than_raising():
    # `main` wires `fetch=vendor.get_quotes`, which returns a `VendorResponse` of
    # `status`, `body`, `headers` and `body_text`. The reader was handed that whole
    # response and asked it for symbols, which raised `AttributeError` before `report` could
    # ping. The one check that asks someone else whether a closed day is really closed could
    # not have survived reaching the vendor. Every other caller unwraps before reading.
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 5, 9, 35)),
        symbols=["SPY", "QQQ"],
        fetch=lambda symbols: VendorResponse(
            status=200,
            body={"SPY": _quote(et(2026, 9, 5, 9, 34)), "QQQ": _quote(et(2026, 9, 4, 16, 0))},
            headers={},
        ),
    )
    assert result.checked
    assert result.problem is None
    assert result.trading == ("SPY",)
    assert result.pages


def test_the_probe_reads_what_the_real_vendor_builds():
    # The shape end to end, through the real `SchwabVendor` over a fake client. A
    # hand-built fixture can drift from what production returns, which is exactly what
    # happened here. This one cannot drift, because the vendor under test builds it.
    symbols = ("SPY", "QQQ")
    body = {"SPY": _quote(et(2026, 9, 5, 9, 34)), "QQQ": _quote(et(2026, 9, 4, 16, 0))}
    client = FakeSchwabClient(quotes={symbols: FakeResponse(status_code=200, body=body)})
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 5, 9, 35)),
        symbols=list(symbols),
        fetch=SchwabVendor(client).get_quotes,
    )
    assert result.trading == ("SPY",)
    assert result.pages


@pytest.mark.parametrize("status", [199, 300, 302, 401, 429, 500])
def test_a_status_that_is_not_a_success_is_a_problem_and_never_a_page(status):
    # Schwab reports a dead token and a throttle as a status on a returned reply rather
    # than as a raise, which is why the Sunday canary checks the status explicitly. A
    # probe that never looks reads a dead token as the calendar being right. The body
    # here carries today's stamp, so a probe that skipped the status would page on it.
    # Both ends of the range are here rather than three failure codes in the middle. An
    # expired Schwab session redirects to a consent page, and a 302 read as success walks
    # past the one fact worth reporting.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 5, 9, 34))},
        status=status,
    )
    assert result.checked
    assert not result.pages
    assert result.problem == f"vendor returned http {status}"


@pytest.mark.parametrize("status", [200, 204, 299])
def test_every_success_status_reads_the_batch_rather_than_reporting_a_problem(status):
    # The other half of the range. Without it the predicate can shrink to the exact codes
    # the failure cases name and stay green.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 5, 9, 34))},
        status=status,
    )
    assert result.problem is None
    assert result.trading == ("SPY",)
    assert result.pages


def test_a_dead_token_carrying_an_error_body_names_the_status_not_the_body():
    # Both conditions hold at once, which is the ordinary Schwab shape for a dead token.
    # The status is read first because it says the token is dead. The body problem would
    # send the operator to inspect a payload while the thing to fix is the credential.
    result = _probe(et(2026, 9, 5, 9, 35), {"errors": [{"status": "401"}]}, status=401)
    assert result.problem == "vendor returned http 401"


def test_a_reply_carrying_a_status_and_no_body_names_the_body():
    class _NoBody:
        status = 200

    result = _probe(et(2026, 9, 5, 9, 35), reply=_NoBody())
    assert result.checked
    assert not result.pages
    assert result.problem == "unreadable vendor body: NoneType"


def test_a_body_that_is_a_mapping_and_not_a_dict_is_still_read():
    # `VendorResponse.body` declares `Mapping`, so the check honours the declared type
    # rather than narrowing to the one shape `json.loads` happens to produce.
    stamp = int(et(2026, 9, 5, 9, 34).timestamp() * 1000)
    body = MappingProxyType({"SPY": {"quote": {"quoteTime": stamp}}})
    result = _probe(et(2026, 9, 5, 9, 35), reply=VendorResponse(status=200, body=body))
    assert result.trading == ("SPY",)
    assert result.pages


def test_an_envelope_that_is_a_mapping_and_not_a_dict_is_not_an_answer():
    # The reader opens an envelope only when it is a `dict`, so counting a wider type as
    # an answer here would hand it a stamp it then reads as absent. That is the quiet day
    # this whole change exists to stop, one layer further in. The two checks agree on what
    # an envelope is, deliberately, and this is what says so.
    stamp = int(et(2026, 9, 5, 9, 34).timestamp() * 1000)
    envelope = MappingProxyType({"quote": {"quoteTime": stamp}})
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": envelope, "QQQ": envelope})
    assert result.checked
    assert not result.pages
    assert result.problem == "no quote envelope for any of the 2 symbols the vendor named"


def test_no_reply_the_probe_cannot_read_invents_a_count_or_a_trading_symbol():
    # `refused` feeds the operator line's partly-unreadable wording and `trading` fires the
    # page. A reply the probe never opened has no stamp to refuse and no symbol to name, so
    # both are structurally zero and nothing on this path may set either.
    replies = [
        "not a reply",
        VendorResponse(status="200", body={}),
        VendorResponse(status=401, body={"SPY": _quote(et(2026, 9, 5, 9, 34))}),
        VendorResponse(status=200, body=[]),
        VendorResponse(status=200, body={"errors": []}),
        VendorResponse(status=200, body={"SPY": "ERROR"}),
    ]
    for reply in replies:
        result = _probe(et(2026, 9, 5, 9, 35), reply=reply)
        assert result.problem is not None
        assert result.refused == 0
        assert result.trading == ()
        assert not result.pages


def test_an_error_body_is_a_problem_rather_than_a_quiet_day():
    # `{"errors": [...]}` is a mapping carrying no envelope the probe recognises, so every
    # symbol read as absent, and an absent stamp is deliberately not a problem. The line
    # printed `calendar agrees` on a day the vendor had answered nothing at all.
    result = _probe(et(2026, 9, 5, 9, 35), {"errors": [{"status": "400"}]})
    assert result.checked
    assert not result.pages
    assert result.problem == "vendor named none of the 2 symbols asked for"


def test_a_batch_of_malformed_envelopes_is_a_problem_rather_than_a_quiet_day():
    # A roster of garbage envelopes read as an ordinary quiet day, because each one
    # classified as an absent stamp and a batch of absent stamps reports no problem. The
    # line says the payload changed shape rather than that the roster went unanswered,
    # because the vendor named both symbols and those are different things to look at.
    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": "ERROR", "QQQ": "ERROR"})
    assert result.checked
    assert not result.pages
    assert result.problem == "no quote envelope for any of the 2 symbols the vendor named"


def test_a_reply_naming_none_of_the_symbols_asked_for_is_a_problem():
    # A reply naming none of the roster read exactly like one naming all of it with every
    # stamp absent. `run_probe` knows the list it asked about and threw it away. The
    # unasked-for symbol here is stamped today, so dropping the comparison turns this
    # reply into a page.
    result = _probe(et(2026, 9, 5, 9, 35), {"IWM": _quote(et(2026, 9, 5, 9, 34))})
    assert result.checked
    assert not result.pages
    assert result.trading == ()
    assert result.problem == "vendor named none of the 2 symbols asked for"


@pytest.mark.parametrize("body", [[], "ERROR", None, 7])
def test_a_body_that_is_not_a_batch_of_quotes_is_a_problem(body):
    # A payload that changed shape is a different thing to go and look at than one that
    # simply omitted the roster, so it says so rather than being folded into the count.
    result = _probe(et(2026, 9, 5, 9, 35), reply=VendorResponse(status=200, body=body))
    assert result.checked
    assert not result.pages
    assert result.problem == f"unreadable vendor body: {type(body).__name__}"


@pytest.mark.parametrize("reply", [{"SPY": {"quote": {}}}, None, "ERROR"])
def test_a_reply_that_is_not_a_vendor_response_is_a_problem_rather_than_a_raise(reply):
    # The bare dict the fixtures used to hand back is the first of these. Nothing on this
    # path may raise, because a raise costs the healthchecks ping as well as the answer,
    # and the check is what says the probe stopped running.
    result = _probe(et(2026, 9, 5, 9, 35), reply=reply)
    assert result.checked
    assert not result.pages
    assert result.problem == f"not a vendor reply: {type(reply).__name__}"


@pytest.mark.parametrize("status", ["200", 200.0, None])
def test_a_status_of_the_wrong_type_names_the_status_rather_than_the_reply(status):
    # This one is a vendor reply. Its status is the half the probe cannot read, and a line
    # naming the reply type sends the operator to look at the half that is fine.
    result = _probe(et(2026, 9, 5, 9, 35), reply=VendorResponse(status=status, body={}))
    assert result.checked
    assert not result.pages
    assert result.problem == f"unreadable vendor status: {type(status).__name__}"


def test_a_symbol_nobody_asked_about_never_reaches_the_reader():
    # Counting the answers is not enough on its own. One answering roster symbol clears
    # the count, and before the batch was narrowed an unasked-for ticker beside it still
    # set `trading` and fired a priority-5 page naming a symbol nobody asked to watch.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": {"quote": {}}, "IWM": _quote(et(2026, 9, 5, 9, 34))},
    )
    assert result.checked
    assert result.trading == ()
    assert not result.pages
    assert result.problem is None


def test_an_errors_block_beside_the_quotes_is_not_counted_as_a_silent_symbol():
    # Schwab puts unresolvable symbols in an `errors` block beside the quotes. Reading
    # every key that came back counted that block as one more symbol the vendor stayed
    # quiet about, so the operator line grew an absent symbol that was never a symbol.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _stamped(True), "errors": {"invalidSymbols": ["QQQ"]}},
    )
    assert result.refused == 1
    assert not result.pages
    assert result.problem == "no readable quote time (1 unreadable, 0 absent)"


def test_an_empty_roster_says_so_rather_than_blaming_the_vendor():
    # Retiring the last ticker is a real thing to do, and the capture cycle skips its own
    # quote request for it. A probe with nothing to ask cannot answer, which is the state
    # it already calls no evidence either way. Counting answers against an empty roster
    # reported none of zero symbols, which blames the vendor for a local file.
    asked = []
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 5, 9, 35)),
        symbols=[],
        fetch=lambda symbols: asked.append(symbols) or VendorResponse(status=200, body={}),
    )
    assert result.checked
    assert not result.pages
    assert result.problem == "no symbols to ask about"
    assert asked == []


def test_a_malformed_envelope_beside_a_readable_one_is_still_only_an_absent_stamp():
    # The whole fix sits above `read_batch`. Teaching the reader that a garbage envelope
    # is a refusal would reclassify an absent stamp, which is what a vendor staying quiet
    # about one symbol looks like, and a shipped test holds that rule.
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": "ERROR", "QQQ": _quote(et(2026, 9, 4, 16, 0))},
    )
    assert result.checked
    assert not result.pages
    assert result.refused == 0
    assert result.problem is None


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


# The stamp every paging fixture carries. `report` takes a result a caller built, and only
# `run_probe` guarantees a trading symbol arrives with a stamp, so a fixture that pages owes
# one itself. The body reads it with no fallback.
PAGE_STAMP = et(2026, 9, 5, 9, 34, 12)


def _paged(trading=("SPY",), *, answered=2, asked=2, latest=PAGE_STAMP) -> ProbeResult:
    """A result the paging branch composes a body from, with every field that body reads."""
    return ProbeResult(
        SATURDAY,
        checked=True,
        trading=trading,
        latest=latest,
        answered=answered,
        asked=asked,
    )


def _names(size: int) -> tuple[str, ...]:
    return tuple(f"SYM{index:04d}" for index in range(size))


def test_a_market_found_open_is_actually_sent(tmp_path):
    """The page must reach the publisher, not just be decided on.

    `main` had no test, which is how a publisher with no transport shipped: it recorded
    the page to a file and returned, and nothing noticed.
    """
    from lake.probe_calendar import PAGE_TITLE, report

    sink, pings = Sink(), Pings()
    result = _paged()
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
    # This assertion demanded a name before the change and has to say they are gone after.
    assert "SPY" not in sink.sent[0].body
    assert sink.sent[0].body == (
        "2026-09-05: 1 of 2 answered quoting today, 2 asked, latest 09:34:12 ET. "
        "The daemon is idle. Check the Now panel."
    )


def test_the_check_is_fed_on_the_day_it_pages_too():
    # The check's silence must mean the probe stopped running. A day that pages is
    # exactly a day it ran.
    from lake.probe_calendar import report

    pings = Pings()
    report(
        _paged(),
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
        _paged(),
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


# -- what the page body says ----------------------------------------------------------


@pytest.mark.parametrize("size", [1, 6, 600])
def test_the_body_names_no_symbol_at_any_roster_size(size):
    """The names grew with the roster, and the design leaves them out.

    A market that is open quotes every symbol, so the list restates the roster. Capping it
    instead would still put names in the body, which this refuses at any size.
    """
    names = _names(size)
    body = probe_calendar._page_body(_paged(names, answered=size, asked=size))

    assert not any(name in body for name in names)


def test_the_body_tells_a_partial_reply_from_a_full_one():
    """Quoted, answered and asked are three different numbers, and the page carries all three.

    Three quoting symbols read as an open market against what the vendor answered about
    and as a glitch against what was asked, and both can be the same reply. Reporting one
    number would pick a reading for the operator.
    """
    quoting = ("AAA", "BBB", "CCC")
    partial = probe_calendar._page_body(_paged(quoting, answered=3, asked=115))
    full = probe_calendar._page_body(_paged(quoting, answered=115, asked=115))

    assert "3 of 3 answered" in partial
    assert "115 asked" in partial
    assert "3 of 115 answered" in full
    assert partial != full


def test_the_quoted_count_is_what_traded_today_rather_than_what_read():
    """A prior session's stamp reads fine and is not the market quoting now.

    Counting what read would report every symbol the vendor stamped at all, which on a
    genuinely closed day is the whole roster. The page exists to contradict that reading.
    """
    result = _probe(
        et(2026, 9, 5, 9, 35),
        {"SPY": _quote(et(2026, 9, 5, 9, 34)), "QQQ": _quote(et(2026, 9, 4, 16, 0))},
    )

    assert result.trading == ("SPY",)
    assert (result.answered, result.asked) == (2, 2)
    assert probe_calendar._page_body(result) == (
        "2026-09-05: 1 of 2 answered quoting today, 2 asked, latest 09:34:00 ET. "
        "The daemon is idle. Check the Now panel."
    )


def test_answered_counts_what_the_vendor_named_rather_than_what_read():
    """The denominator is the whole reply, stamps that would not read included.

    This is the decision the page turns on, and separating it needs a batch where the three
    outcomes differ. One stamp reads, one is absent, one is refused, and a fourth symbol the
    vendor stays quiet about entirely never reaches the reader at all. So the four numbers
    that could stand in for each other come apart: asked is 4, the vendor answered about 3,
    2 read in some form, and 1 quoted.

    A denominator of what read would report ``1 of 1`` on a vendor going bad one symbol at a
    time, which says the market is open on the evidence of the symbols that still work.
    """
    result = run_probe(
        calendar=weekday_sessions(WEEK),
        clock=ManualClock(start=et(2026, 9, 5, 9, 35)),
        symbols=["AAA", "BBB", "CCC", "DDD"],
        fetch=lambda symbols: VendorResponse(
            status=200,
            body={
                "AAA": _quote(et(2026, 9, 5, 9, 34)),
                "BBB": {"quote": {}},
                "CCC": _stamped(True),
            },
            headers={},
        ),
    )

    assert result.trading == ("AAA",)
    assert (result.answered, result.asked, result.refused) == (3, 4, 1)
    assert probe_calendar._page_body(result) == (
        "2026-09-05: 1 of 3 answered quoting today, 4 asked, latest 09:34:00 ET. "
        "The daemon is idle. Check the Now panel."
    )


def test_the_body_carries_the_latest_stamp_on_the_market_clock():
    """The maximum stamp, and rendered in market time under the literal that says so.

    Three symbols with the maximum on neither end of the walk, because reporting the first
    and reporting the last are two different mistakes and a two-symbol batch has only those
    two positions. ``journal.epoch_ms_to_utc`` returns UTC, so a body that formats the
    stored instant reads four hours out in summer while the literal still says ``ET``.
    """
    stamps = {
        "AAA": et(2026, 9, 5, 9, 31, 5),
        "MMM": et(2026, 9, 5, 9, 34, 12),
        "ZZZ": et(2026, 9, 5, 9, 32, 40),
    }
    reading = read_batch({name: _quote(at) for name, at in stamps.items()}, SATURDAY)
    body = probe_calendar._page_body(
        _paged(reading.trading, answered=3, asked=3, latest=reading.latest)
    )

    assert "latest 09:34:12 ET" in body
    assert "09:31:05" not in body
    assert "09:32:40" not in body
    # 09:34:12 ET is 13:34:12 UTC, which is what formatting the stored instant would print.
    assert "13:34:12" not in body


def test_the_body_does_not_grow_with_the_roster():
    """Its length moves by the digits of the counts and by nothing else.

    Asserting only that six hundred symbols fit the design's byte budget would still pass a
    capped list of names, which grows slowly rather than not at all.
    """
    small = probe_calendar._page_body(_paged(_names(6), answered=6, asked=6))
    large = probe_calendar._page_body(_paged(_names(600), answered=600, asked=600))

    assert re.sub(r"\d", "", small) == re.sub(r"\d", "", large)
    assert len(large.encode()) - len(small.encode()) == 3 * (len("600") - len("6"))


def test_pages_reads_the_names_and_never_the_stamp():
    """``pages`` is still the trading names, so the new field cannot decide who is paged.

    A result carrying a stamp and no trading symbol is every closed day the vendor answered
    on, and reading the stamp would page on all of them.
    """
    assert ProbeResult(SATURDAY, checked=True, trading=("SPY",)).pages
    assert not ProbeResult(SATURDAY, checked=True, latest=PAGE_STAMP).pages


def test_the_operator_line_names_the_count_and_never_the_roster(capsys):
    """The launchd log gets the same fold the phone does.

    Nothing above constrains this line, so a roster printed here would put the names back
    into the one surface the body no longer carries them on.
    """
    from lake.probe_calendar import PAGE_TITLE, report

    report(
        _paged(("SPY", "QQQ")),
        publisher=Sink(),
        pinger=Pings(),
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )

    err = capsys.readouterr().err.strip()
    assert err == f"calendar probe: {PAGE_TITLE} (2 symbols)"
    assert "SPY" not in err


def test_a_batch_that_read_nothing_composes_no_page():
    """No readable stamp and no trading symbol reaches the publisher at all.

    The body reads a stamp that is not there on such a result, so composing one would raise
    inside the paging branch, after the ping has already fed the check green.
    """
    from lake.probe_calendar import report

    result = _probe(et(2026, 9, 5, 9, 35), {"SPY": _stamped(True), "QQQ": _stamped("junk")})
    sink = Sink()

    code = report(
        result,
        publisher=sink,
        pinger=Pings(),
        ping_url="https://x/y",
        slug=CALENDAR_PROBE_SLUG,
        now=et(2026, 9, 5, 9, 35),
    )

    assert code == 0
    assert sink.sent == []


def test_a_readable_stamp_from_another_day_never_becomes_the_latest():
    """``latest`` is the newest stamp behind ``trading``, not the newest that read.

    A vendor clock running ahead sends a stamp that converts fine and belongs to no
    session today. Tracking every readable stamp would put it on the page, and the page
    would then report a time no quoting symbol carried, which is the one fact the body
    exists to carry.
    """
    reading = read_batch(
        {
            "AAA": _quote(et(2026, 9, 5, 9, 34)),
            "ZZZ": _quote(et(2026, 9, 6, 9, 34)),
        },
        SATURDAY,
    )

    assert reading.trading == ("AAA",)
    assert reading.readable == 2
    assert reading.latest == et(2026, 9, 5, 9, 34)


def test_a_paging_result_with_no_stamp_raises_rather_than_inventing_one():
    """The body has no fallback, and this is what holds that decision in place.

    ``run_probe`` cannot build this result, so the raise is unreachable from `main`. What
    it guards is the edit that adds a fallback, which would publish a placeholder where
    the stamp goes on the one page that reports a whole lost session. A page saying the
    market is trading and the time is unknown is worse than one that never sends, because
    the operator acts on it.
    """
    from lake.probe_calendar import report

    with pytest.raises(AttributeError):
        report(
            ProbeResult(SATURDAY, checked=True, trading=("SPY",), answered=2, asked=2),
            publisher=Sink(),
            pinger=Pings(),
            ping_url="https://x/y",
            slug=CALENDAR_PROBE_SLUG,
            now=et(2026, 9, 5, 9, 35),
        )


def test_run_probe_pairs_a_trading_symbol_with_a_stamp():
    """``run_probe`` pairs a trading symbol with a stamp, which is what the body relies on.

    The branch that fills ``trading`` is reached only from a stamp that read, so the two
    arrive together. ``ProbeResult`` does not enforce that pairing, which is why every
    fixture here that pages sets both by hand.
    """
    partial = _probe(et(2026, 9, 5, 9, 35), {"SPY": _quote(et(2026, 9, 5, 9, 34))})

    assert partial.trading == ("SPY",)
    assert partial.latest == et(2026, 9, 5, 9, 34)
    assert (partial.answered, partial.asked) == (1, 2)


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


def test_each_shape_the_probe_cannot_read_gets_its_own_operator_line(capsys):
    # Built by running the classifier rather than by retyping its strings. Retyping them
    # left the test green for any wording, which is no coverage at all. A dead token, a
    # payload that changed shape, and a reply that skipped the roster are different things
    # to go and look at, so the lines must differ as well as reach the operator intact.
    day = date(2026, 9, 5)
    replies = [
        "not a reply",
        VendorResponse(status="200", body={}),
        VendorResponse(status=401, body={}),
        VendorResponse(status=200, body=[]),
        VendorResponse(status=200, body={"errors": []}),
        VendorResponse(status=200, body={"SPY": "ERROR"}),
    ]
    lines = set()
    for reply in replies:
        batch, problem = probe_calendar._batch_of(reply, ["SPY", "QQQ"])
        assert batch is None
        lines.add(_status(ProbeResult(day, checked=True, problem=problem), capsys))
        assert f"calendar probe: {problem}" in lines
    assert len(lines) == len(replies)
