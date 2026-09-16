"""The parser's schema-drift page, across one real boundary: the filesystem.

The observer's state machine is covered on its own in
``tests/unit/test_schema_drift.py``. What these cover is the page it feeds: what reaches
the phone, what reaches stderr, and what a publisher that refused or could not send leaves
behind. The publisher is the real one over a recording transport, holding the two secrets
the production ``main`` passes, and it writes its record to a throwaway lake. So the tier
is component: the producer over real files, with the network still fake.

Eight things are covered.

1. One cycle's findings are one page, carrying the event, the title, the page priority,
   and a body naming each surface's columns and how many tickers carried them.
2. One column across four tickers is one page that says four, never four pages.
3. Both surfaces drifting at once still fold into one page.
4. A drift too wide for one message is capped and counted, and the widest drift the two
   pinned schemas can produce still fits the design's 1,000-byte body budget.
5. A refused page keeps its body off stderr, since a refusal means the body carried one of
   the publisher's own secrets.
6. A transport that raises never reaches the caller, so a dead ntfy cannot cost the cycle
   that found the drift.
7. The per-ticker detail the page folds away still reaches stderr.
8. A finding from an observation of one ticker prints no ticker count, and the page says
   what was read instead. The count is the reach on a cycle's finding and always one on a
   partial one, so printing it would read as a retype isolated to one ticker.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lake import compact, schema_drift
from lake.alert import PAGE_PRIORITY, Publisher
from lake.capture import CycleResult, SegmentOutcome
from lake.journal import CHAINS_SURFACE, QUOTES_SURFACE, ROW_KIND_DATA, extra_paths
from lake.schema_drift import (
    PAGE_COLUMN_CAP,
    SCHEMA_DRIFT_EVENT,
    SCHEMA_DRIFT_TITLE,
    ColumnDrift,
    SchemaDriftObserver,
)
from tests.support.calendar import et
from tests.support.config import NTFY_TOPIC, PING_KEY
from tests.support.transport import FakeTransport

NOW = et(2026, 9, 14, 14, 31)


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    root = tmp_path / "lake"
    root.mkdir()
    return root


def _paging(lake_root: Path, transport=None) -> tuple[Publisher, FakeTransport]:
    """A publisher over a recording transport, holding the config's two secrets.

    The secrets are what the real ``main`` passes, so a page composed here is refused on
    exactly the terms a page composed in production would be.
    """
    transport = FakeTransport() if transport is None else transport
    publisher = Publisher(lake_root=lake_root, transport=transport, secrets=(PING_KEY, NTFY_TOPIC))
    return publisher, transport


def _drift(
    column: str, *tickers: str, surface: str = CHAINS_SURFACE, partial: bool = False
) -> ColumnDrift:
    return ColumnDrift(surface, column, tickers, partial)


def _segment(surface: str, ticker: str, *routed: str) -> SegmentOutcome:
    return SegmentOutcome(
        surface=surface,
        ticker=ticker,
        path=Path("segment.arrows"),
        partition=f"{surface}/ticker={ticker}/date=2026-09-14/segment.arrows",
        row_kind=ROW_KIND_DATA,
        rows=1,
        error_class=None,
        fetched_at=None,
        routed_columns=routed,
    )


# -- 1. one cycle's findings are one page -------------------------------------


def test_one_drifted_column_sends_one_page_naming_it(lake_root, capsys):
    """The page the design's schema policy owes, composed from one cycle's findings.

    The event name carries the producer, the way ``compact.SCHEMA_DRIFT_EVENT`` does, so a
    reader of ``reports/alerts/`` can tell the parser's finding from the merge's without
    opening a file. The title says where the drift was seen, which is the other half of
    that distinction.
    """
    publisher, transport = _paging(lake_root)

    schema_drift.page(publisher, (_drift("open_interest", "SPY"),), now=NOW)

    assert len(transport.messages) == 1
    message = transport.messages[0]
    assert message.event == SCHEMA_DRIFT_EVENT
    assert message.title == SCHEMA_DRIFT_TITLE
    assert message.priority == PAGE_PRIORITY
    assert "chains: open_interest on 1 ticker(s)" in message.body
    # The finding reaches the launchd log too, which is where the restart script sends
    # the operator. A page that landed and a page nobody read look identical otherwise.
    assert SCHEMA_DRIFT_TITLE in capsys.readouterr().err


def test_a_cycle_with_no_finding_sends_nothing_and_says_nothing(lake_root, capsys):
    """The steady state costs neither a page nor a log line."""
    publisher, transport = _paging(lake_root)

    schema_drift.page(publisher, (), now=NOW)

    assert transport.messages == []
    assert capsys.readouterr().err == ""


# -- 2 and 3. the folds -------------------------------------------------------


def test_one_column_across_four_tickers_is_one_page_that_says_four(lake_root):
    """A vendor retype reaches every ticker on the same cycle, so it is one fact.

    Paging per ticker would scale the page count with the roster while the fact stayed one
    fact, and ``alert.DEFAULT_DAILY_CAP`` is forty a day. The count is what survives the
    fold, because one ticker drifting and the whole roster drifting are different findings.
    """
    publisher, transport = _paging(lake_root)

    schema_drift.page(publisher, (_drift("open_interest", "SPY", "QQQ", "IWM", "DIA"),), now=NOW)

    assert len(transport.messages) == 1
    assert "chains: open_interest on 4 ticker(s)" in transport.messages[0].body


def test_both_surfaces_drifting_at_once_fold_into_one_page(lake_root):
    """One vendor change reaching both surfaces is still one fact and one page."""
    publisher, transport = _paging(lake_root)

    schema_drift.page(
        publisher,
        (
            _drift("bid", "SPY", "QQQ", surface=QUOTES_SURFACE),
            _drift("open_interest", "SPY"),
        ),
        now=NOW,
    )

    assert len(transport.messages) == 1
    body = transport.messages[0].body
    assert "chains: open_interest on 1 ticker(s)" in body
    assert "quotes: bid on 2 ticker(s)" in body
    # The findings were handed over quotes first, and the body still reads chains first.
    # The cap cuts the tail of this list, so an order that followed the roster would change
    # which column names survive from one cycle to the next.
    assert body.index("chains:") < body.index("quotes:")


def test_a_whole_cycle_of_outcomes_reaches_the_phone_as_one_page(lake_root):
    """The observer and the page together, over the outcomes a cycle actually carries.

    Each half is covered on its own. This is the seam between them, which a test of either
    half alone would leave free to be wired backwards.
    """
    publisher, transport = _paging(lake_root)
    observer = SchemaDriftObserver()
    result = CycleResult(
        snap_ts=NOW,
        segments=(
            _segment(CHAINS_SURFACE, "SPY", "open_interest"),
            _segment(CHAINS_SURFACE, "QQQ", "open_interest"),
            _segment(QUOTES_SURFACE, "SPY"),
        ),
    )

    schema_drift.page(publisher, observer.observe(result), now=NOW)
    # The second cycle holds the same drift, so it owes no second page.
    schema_drift.page(publisher, observer.observe(result), now=NOW)

    assert len(transport.messages) == 1
    assert "chains: open_interest on 2 ticker(s)" in transport.messages[0].body


# -- 4. a drift too wide for one message --------------------------------------


def test_a_drift_too_wide_for_one_message_is_capped_and_counted(lake_root):
    """ntfy's default body limit is 4096 bytes and it answers an oversize POST with a 400.

    ``NtfyTransport`` does not retry a 400, so without the cap the widest drift, the one
    that matters most, would be the one page that never landed. The count survives the cut,
    because it is what separates one moved column from a wholesale retype, and stderr names
    every column either way.
    """
    publisher, transport = _paging(lake_root)
    wide = tuple(_drift(f"column_{index:02d}", "SPY") for index in range(PAGE_COLUMN_CAP + 5))

    schema_drift.page(publisher, wide, now=NOW)

    body = transport.messages[0].body
    assert "column_00 on 1 ticker(s)" in body
    assert f"column_{PAGE_COLUMN_CAP:02d}" not in body
    assert "and 5 more" in body


# The design pins every page body at plain text under 1,000 bytes, at `docs/design.md`'s
# alerting section. That is tighter than ntfy's own 4096-byte default, so it is the bound
# that decides this producer's cap.
DESIGN_BODY_BUDGET = 1000

# The roster size the design sizes for. It changes the body by the width of one number per
# column, so it is stated rather than left at one ticker.
ROSTER = 115


def test_the_widest_drift_the_schemas_can_produce_fits_the_design_body_budget():
    """Every column on both surfaces drifting at once, which is the worst case that exists.

    ``extra_paths`` enumerates every column that can ever reach one of these bodies, so the
    widest possible page is computable rather than hypothetical, and this walks that
    mapping rather than a number someone wrote down. Uncapped the same drift runs to about
    3,867 bytes, which is nearly four times the budget, so this is the assertion the cap
    exists to satisfy.

    It also holds the budget as the schema grows. Promoting a field out of ``extra`` into a
    column of its own adds a path here, and the capped body does not grow with the paths,
    so a promotion cannot quietly push a page past the limit.
    """
    widest = tuple(
        ColumnDrift(surface, column, tuple(f"TKR{index}" for index in range(ROSTER)))
        for surface in (CHAINS_SURFACE, QUOTES_SURFACE)
        for column in sorted(extra_paths(surface))
    )
    assert len(widest) > PAGE_COLUMN_CAP * 2, "the worst case must exceed the cap to test it"
    body = schema_drift._body(widest)
    assert len(body.encode()) < DESIGN_BODY_BUDGET, len(body.encode())
    # The count is what survives the cut, and it is what separates one moved column from a
    # wholesale retype. A body that fit by dropping the count would say nothing.
    assert "more" in body


def test_a_cap_is_only_reached_by_a_drift_that_exceeds_it(lake_root):
    """A drift exactly at the cap names every column and counts nothing left over."""
    publisher, transport = _paging(lake_root)
    exact = tuple(_drift(f"column_{index:02d}", "SPY") for index in range(PAGE_COLUMN_CAP))

    schema_drift.page(publisher, exact, now=NOW)

    body = transport.messages[0].body
    assert f"column_{PAGE_COLUMN_CAP - 1:02d} on 1 ticker(s)" in body
    assert "more" not in body


# -- 5. a refused page --------------------------------------------------------


def test_a_page_carrying_a_secret_is_refused_and_stays_off_the_log(lake_root, monkeypatch, capsys):
    """The publisher redacts a refused page's record, so stderr must not undo it.

    A refusal means the body held one of the publisher's own secrets, and the record drops
    the title for that reason. A producer that printed the body anyway would route the
    secret around that seam and into the launchd log, which is a file on the machine.
    """
    publisher, transport = _paging(lake_root)
    monkeypatch.setattr(schema_drift, "_body", lambda drifted: f"the topic is {NTFY_TOPIC}")

    schema_drift.page(publisher, (_drift("open_interest", "SPY"),), now=NOW)

    assert transport.messages == []
    err = capsys.readouterr().err
    assert "refused: it carried a secret" in err
    assert NTFY_TOPIC not in err


# -- 6. a transport that cannot send ------------------------------------------


class _Broken:
    """A transport whose POST fails the way a dead network does."""

    def send(self, message):
        raise ConnectionError("ntfy unreachable")


def test_a_transport_that_raises_never_reaches_the_caller(lake_root, capsys):
    """``Publisher.publish`` never raises, so a dead ntfy cannot cost the cycle.

    The daemon's cycle hook is not guarded, so an exception escaping here would take the
    loop down over a page. The cycle that found the drift has already written and
    manifested its segments by then, and a daemon that dies on the minute after a vendor
    retype stops capturing until launchd restarts it.
    """
    publisher, _ = _paging(lake_root, transport=_Broken())

    schema_drift.page(publisher, (_drift("open_interest", "SPY"),), now=NOW)

    err = capsys.readouterr().err
    assert "page not sent: post_failed" in err
    # The record the publisher wrote is what makes an unsent page countable later.
    assert "written down" in err


def test_a_page_that_could_not_be_written_down_says_it_was_lost(tmp_path, capsys):
    """A publisher whose lake root is gone loses the page twice, and says so.

    Creating the root here would turn "lake root missing" into a green check on the next
    attempt, so the publisher does not, and the producer has to report the difference
    between a page written down and a page lost.
    """
    gone = tmp_path / "not-a-lake"
    publisher = Publisher(lake_root=gone, transport=_Broken(), secrets=(PING_KEY, NTFY_TOPIC))

    schema_drift.page(publisher, (_drift("open_interest", "SPY"),), now=NOW)

    assert not gone.exists()
    assert "page not sent: post_failed, lost" in capsys.readouterr().err


# -- 7. the detail the page folds away ----------------------------------------


def test_the_per_ticker_detail_reaches_stderr(lake_root, capsys):
    """The page counts tickers. The log names them.

    Folding is what keeps one vendor change to one page, and the tickers are what an
    operator needs next. launchd captures this stream, so the detail is recoverable
    without opening a segment.
    """
    publisher, _ = _paging(lake_root)

    schema_drift.page(publisher, (_drift("open_interest", "SPY", "QQQ", "IWM"),), now=NOW)

    err = capsys.readouterr().err
    assert "chains.open_interest on SPY, QQQ, IWM" in err


# -- the values a reader of reports/alerts/ depends on -------------------------

# Every assertion above spells the event and the title through the imported symbols, which
# is what keeps a test from disagreeing with the code. The cost is that the values
# themselves are then free to be anything, and they are not: the module docstring rests the
# whole tell-the-producers-apart argument on this event name, and a phone shows the title.
# So the values are pinned literally, once, here.


def test_the_event_name_and_title_are_the_pinned_ones():
    """The literal values, because every other assertion moves with the constant.

    Collide this event with compaction's and the two producers interleave under one name in
    ``reports/alerts/``, which is exactly the distinction the naming convention exists to
    make. A reader could no longer tell the vendor's mid-day retype from this project's own
    release rotating a schema at the nightly merge.
    """
    assert SCHEMA_DRIFT_EVENT == "parser_schema_drift"
    assert SCHEMA_DRIFT_TITLE == "Schema drift in the vendor payload"
    assert SCHEMA_DRIFT_EVENT != compact.SCHEMA_DRIFT_EVENT
    assert SCHEMA_DRIFT_TITLE != compact.SCHEMA_DRIFT_TITLE


def test_the_column_cap_is_the_number_the_design_budget_was_reasoned_to():
    """The cap's literal value, which the two cap tests above cannot hold.

    Both spell the constant symbolically, so they move with it and can never disagree with
    it. They hold the mechanism and an upper bound from the byte budget, and nothing holds a
    lower bound: at a cap of one, a twelve-column retype pages one name and "and 11 more"
    off a body with room for all of them.
    """
    assert PAGE_COLUMN_CAP == 12


# -- 8. a finding from one ticker rather than a whole cycle --------------------


def test_a_partial_finding_prints_no_ticker_count_and_says_what_was_read(lake_root):
    """The close+5 fill's page, and the number it must not print.

    A fill reads one ticker, so ``len(tickers)`` is one whatever the vendor's retype
    actually reaches. An operator woken at 16:20 reading "on 1 ticker(s)" would take a
    change that hit the whole roster for an isolated one, and act on it that way. So the
    count comes off and the body says what was looked at.
    """
    publisher, transport = _paging(lake_root)

    schema_drift.page(publisher, (_drift("open_interest", "SPY", partial=True),), now=NOW)

    body = transport.messages[0].body
    assert "chains: open_interest." in body
    assert "ticker(s)" not in body
    assert "Only SPY was read, not a whole cycle" in body


def test_a_whole_cycle_finding_still_prints_its_count(lake_root):
    """The other half of the same rule, so the count is dropped where it is a floor only.

    A cycle measured the whole roster, so its count is the reach and it is the fact that
    separates one moved column from a wholesale retype.
    """
    publisher, transport = _paging(lake_root)

    schema_drift.page(publisher, (_drift("open_interest", "SPY", "QQQ"),), now=NOW)

    body = transport.messages[0].body
    assert "chains: open_interest on 2 ticker(s)" in body
    assert "was read, not a whole cycle" not in body


def test_the_widest_partial_drift_fits_the_design_body_budget():
    """The same bound the cycle's widest page is held to, on the other body shape.

    A partial body drops a count per column and adds one closing sentence, so it is not
    the cycle's body with a suffix and its length has to be measured rather than inferred.

    Chains is the only surface this can run on, because the close+5 fill is the only
    writer whose partial finding reaches a page and a fill is the chains surface alone.
    That is not the wider of the two surfaces: quotes has 63 projectable columns to
    chains' 56, and the chains body runs longer only because its first twelve sorted
    column names are.
    """
    widest = tuple(
        ColumnDrift(CHAINS_SURFACE, column, ("SPY",), True)
        for column in sorted(extra_paths(CHAINS_SURFACE))
    )
    assert len(widest) > PAGE_COLUMN_CAP, "the worst case must exceed the cap to test it"

    body = schema_drift._body(widest)

    assert len(body.encode()) < DESIGN_BODY_BUDGET, len(body.encode())
    assert "more" in body
    assert "Only SPY was read, not a whole cycle" in body
