"""The four named queries over a real fixture lake.

These lay down journal segments in the pinned capture schemas and sealed partitions in
the fixture schema, then run the Now, Today, History and Lake queries against them through the real
sandboxed connection. The clock and the calendar are fakes, so the one real boundary is
the filesystem and the tier is component.

The fixture is a small, known Monday with every row kind the panels distinguish, plus a
prior Friday and Thursday so the Now query has a gap-only latest day to walk back past
and a second, older data day behind it.

- SPY chains: data at the open and the next minute, a gap at the third minute with an
  ``http_429`` reason, and a suspect data cycle at the fourth. Two writer sessions, so
  the segments concatenate. Two non-segment files sit beside them: ``seg-garbage-9.arrows``,
  which matches the segment glob and is unreadable, and ``notes.arrows``, which does not
  match and must never be opened.
- SPY quotes: the open minute in a journal segment and the next minute in a sealed
  partition written in the fixture schema, so the union by name is exercised across two
  different schemas and two different timestamp offsets.
- QQQ chains: a single ``daemon_dead`` gap on Monday, a data cycle at Friday's option
  close in a sealed partition, and another at Thursday's. The third day makes the walk's
  stop-at-the-first-data-day rule load-bearing: without it Thursday would overwrite
  Friday as the reported last cycle.

The clock reads Monday 09:40:30 ET. So the last SPY chains data cycle at 09:33 is 7.5
minutes old. The slots through 09:38 have outrun their verdict grace and are judged.
09:39 and 09:40 have not, so they read pending alongside everything after them.

Tests past the shared fixture build their own lake from the same row helpers, because
each one needs a lake in a shape the shared fixture deliberately is not: a drifted
segment, a malformed directory name, a ticker with no data cycle at all, an empty lake.
The ``fixture_lake`` fixture is function-scoped, so a test may also plant files into the
shared root without disturbing any other test.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from fnmatch import fnmatchcase
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import capture, dashboard, journal
from lake.alert import Message, Publisher
from lake.calendar import MARKET_TZ
from lake.capture_spans import SPANS_SCHEMA, CaptureSpan, CaptureSpans, spans_path
from lake.config import GuardConstants
from lake.dashboard import (
    HISTORY_REPORTS,
    HISTORY_WINDOW_DAYS,
    MAX_LOOKBACK_SESSIONS,
    NAMED_QUERIES,
    DashboardService,
    QueryParameterError,
)
from lake.metadata import stamp_cycle, stamp_ping
from lake.paths import DATE_PREFIX, JOURNAL_DIR, SEGMENT_GLOB
from lake.report import Nightly, PieceOutcome, write_nightly
from lake.security_master import (
    ID_TYPE_TICKER,
    KIND_EQUITY,
    MASTER_SCHEMA,
    Mapping,
    SecurityMaster,
    master_path,
)
from lake.tickers import Roster
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

THURSDAY = date(2026, 8, 20)
FRIDAY = date(2026, 8, 21)
MONDAY = date(2026, 8, 24)
SATURDAY = date(2026, 8, 22)
EARLY_CLOSE = date(2026, 11, 27)  # the day after Thanksgiving, a half day


def et(day: date, h: int, m: int, s: int = 0) -> datetime:
    """An Eastern-time instant on ``day``. Named in the test, where naming is allowed."""
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=MARKET_TZ)


NOW = et(MONDAY, 9, 40, 30)

# The Sunday evening re-auth before the fixture's Monday, the mint a stamp carries.
MINTED = et(date(2026, 8, 23), 20, 5)


def _roster() -> Roster:
    """The fixture lake's two tickers, as the daemon would stamp them."""
    return Roster.from_mapping(
        {
            "SPY": {"options": True, "chain_cadence": "1m"},
            "QQQ": {"options": True, "chain_cadence": "1m"},
        }
    )


class _Undeliverable:
    """A transport that cannot deliver, so the publisher writes the page down instead."""

    def send(self, message: object) -> None:
        raise OSError("no network")


CALENDAR = FakeCalendar(
    {
        THURSDAY: SessionTimes(open=et(THURSDAY, 9, 30), close=et(THURSDAY, 16, 0)),
        FRIDAY: SessionTimes(open=et(FRIDAY, 9, 30), close=et(FRIDAY, 16, 0)),
        MONDAY: SessionTimes(open=et(MONDAY, 9, 30), close=et(MONDAY, 16, 0)),
        EARLY_CLOSE: SessionTimes(
            open=et(EARLY_CLOSE, 9, 30), close=et(EARLY_CLOSE, 13, 0), early_close=True
        ),
    }
)

_CHAINS_TEMPLATE = {name: None for name in journal.CHAINS_SCHEMA.names}
_QUOTES_TEMPLATE = {name: None for name in journal.QUOTES_SCHEMA.names}


def _row(template: dict, ticker: str, snap: datetime, kind: str, **fields) -> dict:
    """A pinned-schema row stamped in UTC, the way capture stamps ``snap_ts``."""
    row = dict(template)
    row.update(
        snap_ts=snap.astimezone(UTC).isoformat(),
        ticker=ticker,
        row_kind=kind,
        suspect=False,
        schema_version=journal.SCHEMA_VERSION,
    )
    row.update(fields)
    return row


def _chains(ticker: str, snap: datetime, kind: str = journal.ROW_KIND_DATA, **fields) -> dict:
    return _row(_CHAINS_TEMPLATE, ticker, snap, kind, **fields)


def _quotes(ticker: str, snap: datetime, kind: str = journal.ROW_KIND_DATA, **fields) -> dict:
    return _row(_QUOTES_TEMPLATE, ticker, snap, kind, **fields)


def _fixture_row(ticker: str, snap: datetime) -> dict:
    """A fixture-schema data row, stamped with the Eastern offset."""
    return {
        "snap_ts": snap.isoformat(),
        "ticker": ticker,
        "row_kind": journal.ROW_KIND_DATA,
        "suspect": False,
        "schema_version": journal.SCHEMA_VERSION,
    }


def build_lake(fixture_lake: FixtureLake) -> Path:
    chains_a = [
        _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
        _chains("SPY", et(MONDAY, 9, 30), occ_symbol="B"),
        _chains("SPY", et(MONDAY, 9, 31), occ_symbol="A"),
        _chains("SPY", et(MONDAY, 9, 31), occ_symbol="B"),
    ]
    chains_b = [
        _chains("SPY", et(MONDAY, 9, 32), journal.ROW_KIND_GAP, error_class="http_429"),
        _chains("SPY", et(MONDAY, 9, 33), occ_symbol="A", suspect=True),
        _chains("SPY", et(MONDAY, 9, 33), occ_symbol="B", suspect=True),
    ]
    fixture_lake.with_journal_segment(
        "chains",
        "SPY",
        MONDAY,
        pa.Table.from_pylist(chains_a, schema=journal.CHAINS_SCHEMA),
        start_ts="20260824T133000000000",
        pid=1,
    )
    fixture_lake.with_journal_segment(
        "chains",
        "SPY",
        MONDAY,
        pa.Table.from_pylist(chains_b, schema=journal.CHAINS_SCHEMA),
        start_ts="20260824T133200000000",
        pid=2,
    )
    fixture_lake.with_journal_segment(
        "quotes",
        "SPY",
        MONDAY,
        pa.Table.from_pylist([_quotes("SPY", et(MONDAY, 9, 30))], schema=journal.QUOTES_SCHEMA),
        start_ts="20260824T133000000000",
        pid=1,
    )
    fixture_lake.with_quotes(
        "SPY", MONDAY, sample_quotes_table([_fixture_row("SPY", et(MONDAY, 9, 31))])
    )
    fixture_lake.with_journal_segment(
        "chains",
        "QQQ",
        MONDAY,
        pa.Table.from_pylist(
            [_chains("QQQ", et(MONDAY, 9, 30), journal.ROW_KIND_GAP, error_class="daemon_dead")],
            schema=journal.CHAINS_SCHEMA,
        ),
        start_ts="20260824T133000000000",
        pid=1,
    )
    fixture_lake.with_chains(
        "QQQ", FRIDAY, sample_chains_table([_fixture_row("QQQ", et(FRIDAY, 16, 15))])
    )
    fixture_lake.with_chains(
        "QQQ", THURSDAY, sample_chains_table([_fixture_row("QQQ", et(THURSDAY, 16, 15))])
    )
    root = fixture_lake.build()
    # A file that is not an Arrow stream, beside the real segments. Its name matches the
    # segment glob, so the panel opens it, fails, and counts it unreadable.
    garbage = fixture_lake.segment_path("chains", "SPY", MONDAY, "garbage", 9)
    garbage.write_bytes(b"not an arrow stream")
    # A file whose name does not match the glob. Compaction never sweeps it, so a panel
    # that read every ``.arrows`` file would show its failure forever.
    (garbage.parent / "notes.arrows").write_bytes(b"notes about the capture run")
    return root


@pytest.fixture
def root(fixture_lake: FixtureLake) -> Path:
    return build_lake(fixture_lake)


def service_over(
    root: Path, *, guards: GuardConstants | None = None, now: datetime = NOW
) -> DashboardService:
    """A service over one lake root, with the fake clock and calendar wired in."""
    return DashboardService(
        root,
        clock=ManualClock(now.astimezone(UTC)),
        calendar=CALENDAR,
        guards=guards,
        page=b"<!doctype html>",
        icon=b"",
    )


def one_segment_lake(
    fixture_lake: FixtureLake,
    rows: list[dict],
    *,
    ticker: str = "SPY",
    surface: str = "chains",
    schema: pa.Schema | None = None,
    day: date = MONDAY,
) -> Path:
    """A lake holding exactly one journal segment, built from the given rows."""
    if schema is None:
        schema = journal.CHAINS_SCHEMA if surface == "chains" else journal.QUOTES_SCHEMA
    fixture_lake.with_journal_segment(
        surface,
        ticker,
        day,
        pa.Table.from_pylist(rows, schema=schema),
        start_ts="20260824T133000000000",
        pid=1,
    )
    return fixture_lake.build()


def retyped(schema: pa.Schema, column: str, kind: pa.DataType) -> pa.Schema:
    """The same schema with one column's type swapped, the shape schema drift takes."""
    return pa.schema(
        [pa.field(column, kind) if field.name == column else field for field in schema]
    )


def without(schema: pa.Schema, column: str) -> pa.Schema:
    """The same schema with one column dropped, the other shape schema drift takes."""
    return pa.schema([field for field in schema if field.name != column])


@pytest.fixture
def service(root: Path) -> DashboardService:
    return service_over(root)


# -- now ---------------------------------------------------------------------


def test_the_page_and_icon_seams_return_what_was_injected(root: Path):
    # The class docstring says the page and the icon are injected on the same terms, and
    # that each falls back to the bytes shipped in the package. Both halves are asserted
    # here. Without this the constructor could ignore either argument and every caller
    # that passes one would still pass, because nothing else reads them back.
    injected = DashboardService(
        root,
        clock=ManualClock(NOW.astimezone(UTC)),
        calendar=CALENDAR,
        page=b"PAGE",
        icon=b"ICON",
    )
    assert (injected.page, injected.icon) == (b"PAGE", b"ICON")
    default = DashboardService(root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR)
    assert default.page == dashboard.load_status_page()
    assert default.icon == dashboard.load_favicon()


def test_now_reports_the_last_data_cycle_and_minutes_since(service: DashboardService):
    now = service.run_query("now", {})
    assert now["tickers"] == ["QQQ", "SPY"]
    assert now["as_of"] == NOW.isoformat()
    assert now["session_date"] == "2026-08-24"
    assert now["is_session"] is True
    assert now["phase"] == "open"

    rows = {(row["ticker"], row["surface"]): row for row in now["surfaces"]}
    assert set(rows) == {("QQQ", "chains"), ("SPY", "chains"), ("SPY", "quotes")}

    spy_chains = rows[("SPY", "chains")]
    assert spy_chains["last_data_snap_ts"] == et(MONDAY, 9, 33).isoformat()
    assert spy_chains["minutes_since"] == 7.5
    assert spy_chains["last_status"] == "suspect"
    assert spy_chains["last_error_class"] == []
    assert spy_chains["last_error_class_count"] == 0
    # The garbage file is counted, never silently skipped, and never blanks the row.
    assert spy_chains["unreadable_segments"] == 1

    # The quotes' last data cycle lives in the sealed partition, not the journal.
    spy_quotes = rows[("SPY", "quotes")]
    assert spy_quotes["last_data_snap_ts"] == et(MONDAY, 9, 31).isoformat()
    assert spy_quotes["minutes_since"] == 9.5
    assert spy_quotes["last_status"] == "captured"


def test_a_row_reads_stale_against_the_last_minute_a_cycle_was_owed(root: Path):
    """The evening is when the day gets reviewed, and it used to be when the reading died.

    Painting a row stale against ``now`` outside the capture window would light every row
    every evening, because the age climbs on its own once the session ends. The old code
    avoided that by painting nothing at all after the option close, so a session that
    captured nothing read as plain text in exactly those hours. The verdict now reads
    against the last option close that has passed, which both readings get right.
    """
    # QQQ's newest data cycle is Friday's option close, and Monday is gap-only. Read at
    # Monday 17:00 the day is over, so Monday's close is the minute a cycle was owed by.
    evening = service_over(root, now=et(MONDAY, 17, 0)).run_query("now", {})
    qqq = next(row for row in evening["surfaces"] if row["ticker"] == "QQQ")

    assert evening["phase"] == "closed"
    assert evening["capture_owed_through"] == et(MONDAY, 16, 15).isoformat()
    assert qqq["stale"] is True


def test_a_healthy_ticker_reads_clean_all_evening(fixture_lake: FixtureLake):
    """A ticker that captured through its own close is not late, however the age climbs.

    This is the reading the old gate protected and the one a naive threshold would break.
    The age at 17:00 is 45 minutes against a threshold of 3, so only the reference instant
    keeps the row clean.
    """
    root = one_segment_lake(fixture_lake, [_chains("SPY", et(MONDAY, 16, 15))])

    evening = service_over(root, now=et(MONDAY, 17, 0)).run_query("now", {})
    spy = next(row for row in evening["surfaces"] if row["surface"] == "chains")

    assert spy["minutes_since"] == 45.0  # far past the 3-minute threshold
    assert spy["stale"] is False


def test_a_lake_with_no_closed_session_behind_it_paints_nothing_stale(root: Path):
    """Nothing has been owed yet, so nothing can be late yet.

    The fixture calendar's first session is the Thursday. Read before it, the walk finds
    no option close that has passed, and a row with no data is then a lake waiting for
    its first session rather than a capture failure.
    """
    before_any = service_over(root, now=et(date(2026, 8, 19), 12, 0)).run_query("now", {})

    assert before_any["capture_owed_through"] is None
    assert [row["stale"] for row in before_any["surfaces"]] == [False, False, False]


def test_a_ticker_registered_before_the_owed_minute_is_still_judged(root: Path):
    """The late-onboard guard must exempt a late epoch, never every epoch.

    Dropping the comparison and exempting any ticker that carries a ``capture_start`` at
    all reads green against a suite whose stale rows all have none. In production every
    ticker has one, so that simplification switches the column off altogether.
    """
    write_master(root, "QQQ", et(MONDAY, 9, 0))

    evening = service_over(root, now=et(MONDAY, 17, 0)).run_query("now", {})
    qqq = next(row for row in evening["surfaces"] if row["ticker"] == "QQQ")

    assert qqq["capture_start"] == et(MONDAY, 9, 0).isoformat()
    assert qqq["stale"] is True


def test_a_ticker_registered_exactly_at_the_owed_minute_is_judged(root: Path):
    """An epoch on the owed minute was owed that cycle, so it is not exempt."""
    write_master(root, "QQQ", et(MONDAY, 16, 15))

    evening = service_over(root, now=et(MONDAY, 17, 0)).run_query("now", {})
    qqq = next(row for row in evening["surfaces"] if row["ticker"] == "QQQ")

    assert qqq["capture_start"] == evening["capture_owed_through"]
    assert qqq["stale"] is True


def test_a_retired_ticker_is_never_late(root: Path):
    """Nothing is owed of a ticker whose capture span has closed.

    The Today strip marks every post-retirement slot out of scope, so a verdict here that
    ignored scope would contradict the strip one panel over. The page happens to test
    scope before staleness, which hid the wrong value rather than fixing it.
    """
    write_closed_span(root, "SPY", et(MONDAY, 9, 30), et(MONDAY, 12, 0))

    evening = service_over(root, now=et(MONDAY, 17, 0)).run_query("now", {})
    spy = [row for row in evening["surfaces"] if row["ticker"] == "SPY"]

    assert [row["in_scope"] for row in spy] == [False, False]
    assert [row["stale"] for row in spy] == [False, False]


def test_a_recent_cycle_inside_the_threshold_is_not_late(root: Path):
    """A small positive gap is the case a zero threshold would paint stale."""
    # SPY quotes' newest data cycle is 09:31, one minute before this instant.
    payload = service_over(root, now=et(MONDAY, 9, 32)).run_query("now", {})
    quotes = next(row for row in payload["surfaces"] if row["surface"] == "quotes")

    assert quotes["minutes_since"] == 1.0
    assert quotes["stale"] is False


def test_the_injected_guard_decides_the_verdict_and_not_only_the_label(root: Path):
    """The page reports the threshold and colours by it, so one guard must drive both.

    Asserting only that ``stale_after_minutes`` carries the injected number leaves the
    verdict free to use a hardcoded one, and the panel would then report a threshold it
    does not colour by.
    """
    tightened = GuardConstants(watchdog_page_minutes=0)
    at = et(MONDAY, 9, 32)

    loose = service_over(root, now=at).run_query("now", {})
    tight = service_over(root, guards=tightened, now=at).run_query("now", {})

    def quotes(payload):
        return next(row for row in payload["surfaces"] if row["surface"] == "quotes")

    assert quotes(loose)["stale"] is False
    assert tight["stale_after_minutes"] == 0
    assert quotes(tight)["stale"] is True


def test_a_cycle_exactly_at_the_threshold_has_not_passed_it(root: Path):
    """A row is late *past* the threshold, so the threshold itself is still inside it."""
    # SPY quotes' newest data cycle is 09:31, exactly three minutes before this instant.
    at_threshold = service_over(root, now=et(MONDAY, 9, 34)).run_query("now", {})
    past_it = service_over(root, now=et(MONDAY, 9, 34, 6)).run_query("now", {})

    def quotes(payload):
        return next(row for row in payload["surfaces"] if row["surface"] == "quotes")

    assert at_threshold["stale_after_minutes"] == 3
    assert quotes(at_threshold)["minutes_since"] == 3.0
    assert quotes(at_threshold)["stale"] is False
    assert quotes(past_it)["stale"] is True


def test_a_recent_gap_does_not_stand_in_for_a_data_cycle(root: Path):
    """Stale means no durable *data* cycle, and a gap row is not one.

    QQQ's newest slot on the fixture's Monday is a gap two minutes old, while its newest
    data cycle is the previous Friday. Reading the latest slot of any kind here would
    call that clean, which is a daemon writing markers every minute while capturing
    nothing.
    """
    payload = service_over(root, now=et(MONDAY, 9, 32)).run_query("now", {})
    qqq = next(row for row in payload["surfaces"] if row["ticker"] == "QQQ")

    assert qqq["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert qqq["last_status"] == "gap"
    assert qqq["last_data_snap_ts"] == et(FRIDAY, 16, 15).isoformat()
    assert qqq["stale"] is True


def test_the_post_equity_close_quarter_hour_reads_against_now(root: Path):
    """The capture window runs to the option close, not to the equity close.

    Narrowing the check to the open phase alone drops 16:00 through 16:15, the quarter
    hour the option close itself lives in, and sends the reading three days back.
    """
    payload = service_over(root, now=et(MONDAY, 16, 5)).run_query("now", {})

    assert payload["phase"] == "post_equity_close"
    assert payload["capture_owed_through"] == et(MONDAY, 16, 5).isoformat()


def test_the_owed_minute_walks_back_over_a_weekend(root: Path):
    """A Sunday reads against Friday's close, because no session has closed since."""
    sunday = service_over(root, now=et(date(2026, 8, 23), 12, 0)).run_query("now", {})

    assert sunday["phase"] == "non_session"
    assert sunday["capture_owed_through"] == et(FRIDAY, 16, 15).isoformat()


def test_the_walk_reaches_back_over_a_holiday_against_a_weekend(root: Path):
    """The longest reach the calendar can ask for, which is what sizes the walk's bound.

    A Friday holiday against a weekend, read on the Monday before the open. The walk
    spends an iteration on Monday, whose own close is still ahead, then Sunday, Saturday
    and the holiday, and lands on Thursday as the fifth. A bound of four stops one day
    short and reports that nothing has ever been owed.
    """
    holiday_week = FakeCalendar(
        {
            THURSDAY: SessionTimes(open=et(THURSDAY, 9, 30), close=et(THURSDAY, 16, 0)),
            MONDAY: SessionTimes(open=et(MONDAY, 9, 30), close=et(MONDAY, 16, 0)),
        }
    )
    service = DashboardService(
        root,
        clock=ManualClock(et(MONDAY, 9, 0).astimezone(UTC)),
        calendar=holiday_week,
        page=b"<!doctype html>",
        icon=b"",
    )

    payload = service.run_query("now", {})

    assert payload["phase"] == "pre_open"
    assert payload["capture_owed_through"] == et(THURSDAY, 16, 15).isoformat()


def test_a_pre_open_morning_reads_against_the_previous_close(root: Path):
    """Today's close has not happened yet, so it cannot be the minute anything is owed by."""
    morning = service_over(root, now=et(MONDAY, 9, 0)).run_query("now", {})

    assert morning["phase"] == "pre_open"
    assert morning["capture_owed_through"] == et(FRIDAY, 16, 15).isoformat()


def test_inside_the_capture_window_the_owed_minute_is_now(root: Path):
    """The reading inside the session is unchanged, and that is the point."""
    now = service_over(root).run_query("now", {})

    assert now["phase"] == "open"
    assert now["capture_owed_through"] == NOW.isoformat()


def _opened_dark(fixture_lake: FixtureLake, monday_through: int | None = None) -> Path:
    """A lake that captured Friday's option close, and Monday only as far as asked.

    ``monday_through`` is the last Monday minute past 09:30 that landed a cycle, or None
    for a Monday on which nothing ran at all.
    """
    fixture_lake.with_journal_segment(
        "chains",
        "SPY",
        FRIDAY,
        pa.Table.from_pylist(
            [_chains("SPY", et(FRIDAY, 16, 15), occ_symbol="A")],
            schema=journal.CHAINS_SCHEMA,
        ),
        start_ts="20260821T200000000000",
        pid=1,
    )
    if monday_through is not None:
        fixture_lake.with_journal_segment(
            "chains",
            "SPY",
            MONDAY,
            pa.Table.from_pylist(
                [
                    _chains("SPY", et(MONDAY, 9, minute), occ_symbol="A")
                    for minute in range(30, monday_through + 1)
                ],
                schema=journal.CHAINS_SCHEMA,
            ),
            start_ts="20260824T133000000000",
            pid=2,
        )
    return fixture_lake.build()


def _spy_stale(root: Path, when: datetime) -> bool:
    """Whether the Now panel paints SPY chains stale at one instant."""
    now = service_over(root, now=when).run_query("now", {})
    row = next(r for r in now["surfaces"] if r["ticker"] == "SPY" and r["surface"] == "chains")
    return row["stale"]


def test_the_open_minute_is_not_owed_until_its_cycle_could_have_landed(
    fixture_lake: FixtureLake,
):
    # The bug this holds: inside the window the reference was the raw instant, so at
    # 09:30:00 the newest cycle was still Friday's close and the age measured the weekend
    # rather than anything this session did. Every ticker went red the moment the session
    # opened, on a daemon doing exactly what it should, and cleared once the first row
    # landed. That is a red-then-green flap on the loudest reading the panel has.
    root = _opened_dark(fixture_lake)

    assert _spy_stale(root, et(MONDAY, 9, 29, 59)) is False
    assert _spy_stale(root, et(MONDAY, 9, 30, 0)) is False
    assert _spy_stale(root, et(MONDAY, 9, 30, 30)) is False
    assert _spy_stale(root, et(MONDAY, 9, 31, 59)) is False


def test_a_daemon_that_never_woke_reads_stale_once_the_open_grace_runs_out(
    fixture_lake: FixtureLake,
):
    # The grace is a delay, not an amnesty. A daemon that never woke has to be caught, and
    # the moment it can be is the moment the open minute becomes judgeable.
    root = _opened_dark(fixture_lake)

    assert _spy_stale(root, et(MONDAY, 9, 32, 0)) is True
    assert _spy_stale(root, et(MONDAY, 9, 40, 0)) is True


def test_the_open_cycle_landing_keeps_the_row_clean_across_the_grace(
    fixture_lake: FixtureLake,
):
    # A daemon that captured the open reads clean on both sides of the boundary, so the
    # grace running out is not itself an event a healthy session can be seen to cross.
    root = _opened_dark(fixture_lake, monday_through=31)

    assert _spy_stale(root, et(MONDAY, 9, 31, 59)) is False
    assert _spy_stale(root, et(MONDAY, 9, 32, 0)) is False
    assert _spy_stale(root, et(MONDAY, 9, 33, 0)) is False


def test_the_two_panels_agree_about_the_open(fixture_lake: FixtureLake):
    # The reason the strip's own grace is the one reused here. _surface_stale names panel
    # agreement as its reason for consulting scope, and a panel that called every ticker
    # stale while the strip beside it reported nothing missing contradicted that. Both now
    # turn at the same instant on the same dark session.
    root = _opened_dark(fixture_lake)
    for when, expected in (
        (et(MONDAY, 9, 31, 59), False),
        (et(MONDAY, 9, 32, 0), True),
    ):
        strip = service_over(root, now=when).run_query(
            "today", {"date": "2026-08-24", "ticker": "SPY"}
        )["strips"][0]
        assert _spy_stale(root, when) is expected
        assert (strip["counts"]["missing"] > 0) is expected


def test_the_opening_grace_does_not_move_the_threshold(fixture_lake: FixtureLake):
    # The over-reach check, and the one that matters most. The column reports the failure
    # the watchdog counts, so the grace must reach the session's opening minutes and
    # nothing else. A daemon that captured through 09:45 and then stopped still turns at
    # 09:45 plus the injected threshold to the second, exactly as it did before.
    root = _opened_dark(fixture_lake, monday_through=45)
    threshold = timedelta(minutes=GuardConstants().watchdog_page_minutes)
    turns_at = et(MONDAY, 9, 45) + threshold

    assert _spy_stale(root, turns_at) is False
    assert _spy_stale(root, turns_at + timedelta(seconds=1)) is True


def test_a_ticker_onboarded_after_the_close_is_not_late_that_evening(root: Path):
    """Nothing was owed of a ticker whose epoch falls after the last owed minute.

    Without this guard a ticker registered at 17:00 reads stale at 17:30, on a lake that
    has done nothing wrong and owed it nothing yet. The in-scope clamp does not cover
    this, because the clock has passed the epoch and the ticker really is in scope now.
    """
    write_master(root, "SPY", et(MONDAY, 17, 0))

    evening = service_over(root, now=et(MONDAY, 17, 30)).run_query("now", {})
    spy = next(
        row for row in evening["surfaces"] if row["ticker"] == "SPY" and row["surface"] == "chains"
    )

    assert spy["in_scope"] is True
    assert spy["capture_start"] == et(MONDAY, 17, 0).isoformat()
    assert spy["stale"] is False


def test_now_walks_back_past_a_gap_only_day(service: DashboardService):
    now = service.run_query("now", {})
    qqq = next(row for row in now["surfaces"] if row["ticker"] == "QQQ")
    # The latest slot is Monday's gap, with its reason. The last success is Friday's
    # option close: 2 days, 17 hours, 25.5 minutes before the clock's instant.
    assert qqq["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert qqq["last_status"] == "gap"
    assert qqq["last_error_class"] == ["daemon_dead"]
    assert qqq["last_data_snap_ts"] == et(FRIDAY, 16, 15).isoformat()
    assert qqq["minutes_since"] == 3925.5


def test_now_reports_null_where_the_daemon_has_stamped_nothing(service: DashboardService):
    now = service.run_query("now", {})
    # A lake no daemon has run against carries no stamp. The panel says so rather than
    # showing a zero, and it never reaches into ~/.config for the answer.
    assert now["token_minted_at"] is None
    assert now["token_age_minutes"] is None
    assert now["token_sunday_countdown_minutes"] is None
    assert now["dead_man_last_ping"] is None
    # The page count is the exception. An ordinary day writes no file at all, so its
    # absence is a true zero rather than an unknown.
    assert now["pages_failed_to_send"] == 0


def test_now_reports_the_token_stamp_the_daemon_journalled(root: Path):
    # The mint is the Sunday evening re-auth before this Monday. The age is the wait
    # since then and the countdown is the wait until the next Sunday canary, so the
    # panel answers both from one stamp and never from the token file.
    stamp_cycle(
        root,
        at=NOW,
        token_minted_at=MINTED,
        roster=_roster(),
    )

    now = service_over(root).run_query("now", {})
    assert now["token_minted_at"] == MINTED.isoformat()
    assert now["token_age_minutes"] == 815.5  # 13h35m30s since Sunday 20:05
    assert now["token_sunday_countdown_minutes"] == 9259.5  # to Sunday the 30th at 20:00


def test_an_overdue_token_counts_down_past_zero(root: Path):
    # A token minted two Sundays back is past the ritual that should have replaced it.
    # The countdown goes negative rather than being clamped, because a clamped zero
    # reads the same as a ritual due this minute.
    stamp_cycle(root, at=NOW, token_minted_at=MINTED - timedelta(days=7), roster=_roster())

    now = service_over(root).run_query("now", {})
    assert now["token_sunday_countdown_minutes"] == -820.5
    assert now["token_age_minutes"] == 815.5 + 7 * 24 * 60


def test_now_reports_the_dead_man_ping_the_daemon_recorded(root: Path):
    stamp_ping(root, at=et(MONDAY, 9, 40))

    now = service_over(root).run_query("now", {})
    assert now["dead_man_last_ping"] == et(MONDAY, 9, 40).isoformat()


def test_the_dead_man_ping_carries_its_age_and_the_grace_it_is_judged_against(root: Path):
    """The line said when the ping fired and nothing else, so a reader subtracted by eye.

    That is what let the panel read healthy through the 2026-09-09 auth outage. The age
    and the grace ride beside the instant so the page can judge it against the same
    threshold healthchecks pages after, rather than leaving the reading to the reader.
    """
    stamp_ping(root, at=et(MONDAY, 9, 40))

    now = service_over(root).run_query("now", {})

    # NOW is 09:40:30, so the ping is half a minute old and well inside the grace.
    assert now["dead_man_age_minutes"] == 0.5
    assert now["dead_man_grace_minutes"] == 5


def test_a_recalibrated_grace_reaches_the_panel(root: Path):
    """The page must not carry its own copy of a constant slice 1 recalibrates."""
    guards = GuardConstants(dead_man_grace_minutes=9)

    now = service_over(root, guards=guards).run_query("now", {})

    assert now["dead_man_grace_minutes"] == 9
    # An int, not a float. Both compare equal to 9, and only one of them renders as
    # "the grace of 9 minutes" rather than "the grace of 9.0 minutes" on the page.
    assert type(now["dead_man_grace_minutes"]) is int


def test_a_recalibrated_grace_moves_the_minute_the_expectation_arms(root: Path):
    """The grace must reach the arming and not only the sentence that prints it.

    The wake is at 08:25 and the expectation arms one grace after it, so a recalibrated
    grace moves that minute. Probing only at the pinned default of 5 would pass against
    a hardcoded 5 in the arming, which is the simplification a reader is most likely to
    make of the wake-plus-grace reasoning.
    """
    guards = GuardConstants(dead_man_grace_minutes=9)

    before = service_over(root, guards=guards, now=et(MONDAY, 8, 33)).run_query("now", {})
    after = service_over(root, guards=guards, now=et(MONDAY, 8, 34)).run_query("now", {})

    assert before["dead_man_expected"] is False
    assert after["dead_man_expected"] is True


def test_an_unrecorded_ping_has_no_age_rather_than_a_zero(root: Path):
    """A zero would read as "fired this instant", which is the opposite of the truth."""
    now = service_over(root).run_query("now", {})

    assert now["dead_man_last_ping"] is None
    assert now["dead_man_age_minutes"] is None


def test_a_ping_is_owed_inside_the_weekday_envelope(root: Path):
    """Inside the envelope a silent ping is a failure, so the panel may judge it."""
    assert service_over(root).run_query("now", {})["dead_man_expected"] is True


def test_no_ping_is_owed_when_nothing_is_meant_to_be_pinging(root: Path):
    """Nothing pings outside the envelope, by design, so nothing there is a failure.

    A threshold that ran around the clock would light the line every night and every
    weekend. A line that is loud every night is one the reader learns to skip, which is
    the failure the threshold exists to fix.
    """
    overnight = et(MONDAY, 2, 0)
    saturday = et(SATURDAY, 12, 0)
    sunday_canary = et(date(2026, 8, 23), 21, 0)

    for instant in (overnight, saturday, sunday_canary):
        payload = service_over(root, now=instant).run_query("now", {})
        assert payload["dead_man_expected"] is False, instant


def test_the_wake_is_not_owed_a_ping_until_the_grace_has_run(root: Path):
    """The envelope opens at the firmware wake, and the first heartbeat lands after it.

    So the instant the envelope opens, the newest ping is the previous evening's. Owing
    a ping from that instant would go loud every morning on a daemon that is starting
    exactly as designed. The expectation arms one grace later, which is the minute
    healthchecks itself starts expecting a ping.
    """
    before = service_over(root, now=et(MONDAY, 8, 29)).run_query("now", {})
    after = service_over(root, now=et(MONDAY, 8, 30)).run_query("now", {})

    assert before["dead_man_expected"] is False
    assert after["dead_man_expected"] is True


def test_the_expectation_closes_with_the_envelope(root: Path):
    """The sweep's ping ends the weekday window, and the evening owes nothing after it."""
    inside = service_over(root, now=et(MONDAY, 18, 44)).run_query("now", {})
    past = service_over(root, now=et(MONDAY, 18, 45)).run_query("now", {})

    assert inside["dead_man_expected"] is True
    assert past["dead_man_expected"] is False


def test_the_stamp_keeps_landing_while_the_ping_starves(root: Path):
    """The two lines say different things, and a dead session is where they part.

    Through the 2026-09-09 auth outage every cycle failed and wrote a gap. The loop kept
    stamping, because the stamp rides the end of every cycle whether or not it produced
    rows, so the stamp age read zero and alarmed at nothing. The dead-man ping starved,
    because inside the capture window only a durable data cycle feeds it. The panel needs
    both to tell a running loop from working capture.
    """
    # The last ping is the pre-open heartbeat. The idle heartbeat stands down once the
    # capture window opens, so nothing has fed the check since.
    stamp_ping(root, at=et(MONDAY, 9, 29))
    stamp_cycle(root, at=et(MONDAY, 9, 40), token_minted_at=MINTED, roster=_roster())

    now = service_over(root).run_query("now", {})

    assert now["stamp_age_minutes"] == 0.5
    assert now["dead_man_age_minutes"] == 11.5
    assert now["dead_man_expected"] is True
    assert now["dead_man_starved"] is True


def test_a_ping_exactly_at_the_grace_has_not_passed_it(root: Path):
    """The check is starving *past* the grace, so the grace itself is still inside it.

    healthchecks goes down once the grace has elapsed, not as it elapses. A test here
    keeps the comparison strict, because loosening it to ``>=`` changes nothing a reader
    would notice and moves the line one minute early on every reading.
    """
    # NOW is 09:40:30, so these pings are exactly 5.0 and 5.1 minutes old.
    stamp_ping(root, at=et(MONDAY, 9, 35, 30))
    assert service_over(root).run_query("now", {})["dead_man_starved"] is False

    stamp_ping(root, at=et(MONDAY, 9, 35, 24))
    assert service_over(root).run_query("now", {})["dead_man_starved"] is True


def test_a_check_that_starved_while_owed_stays_loud_after_the_window_shuts(root: Path):
    """A starved check does not stop being starved when the expectation window closes.

    A ping URL that breaks at 17:00 pages healthchecks by 17:06 and stays down. Judging
    the ping only against the current minute would drop the page's alarm at 18:45 and
    leave it down until 08:30 the next weekday, weekends included. That is most of the
    week, and it is the same false-healthy reading one hour later.
    """
    stamp_ping(root, at=et(MONDAY, 17, 0))

    evening = service_over(root, now=et(MONDAY, 18, 59)).run_query("now", {})
    next_morning = service_over(root, now=et(MONDAY + timedelta(days=1), 7, 0)).run_query("now", {})

    # Nothing is owed at either instant, and the check is down at both.
    assert evening["dead_man_expected"] is False
    assert evening["dead_man_starved"] is True
    assert next_morning["dead_man_expected"] is False
    assert next_morning["dead_man_starved"] is True


def test_a_healthy_day_leaves_the_night_and_the_weekend_quiet(root: Path):
    """The night must stay quiet, or the reader learns to skip the line.

    The daemon's last heartbeat lands in the envelope's final minute, and the ping then
    ages all night against a check that expects nothing. Judging it against the last
    minute one was owed is what keeps a healthy Friday evening from alarming until
    Monday.
    """
    # 18:44 is the last minute inside the weekday envelope, which ends at 18:45.
    stamp_ping(root, at=et(FRIDAY, 18, 44))

    for instant in (
        et(FRIDAY, 23, 0),
        et(SATURDAY, 12, 0),
        et(MONDAY, 8, 29),
    ):
        payload = service_over(root, now=instant).run_query("now", {})
        assert payload["dead_man_starved"] is False, instant


def test_a_daemon_that_died_before_the_close_is_loud_all_weekend(root: Path):
    """The weekend's quiet is earned by a fed check, never given by the calendar."""
    stamp_ping(root, at=et(FRIDAY, 12, 0))

    for instant in (et(SATURDAY, 12, 0), et(MONDAY, 8, 29)):
        payload = service_over(root, now=instant).run_query("now", {})
        assert payload["dead_man_starved"] is True, instant


def test_a_lake_nothing_has_run_against_owes_nothing_at_the_weekend(root: Path):
    """healthchecks holds a never-pinged check *new* rather than down, and so does this.

    A ping that has never landed is a failure only while one is owed. Reading a never-run
    lake as a starving check every weekend would be the noise this line exists to avoid.
    """
    weekend = service_over(root, now=et(SATURDAY, 12, 0)).run_query("now", {})
    session = service_over(root).run_query("now", {})

    assert weekend["dead_man_starved"] is False
    assert session["dead_man_starved"] is True


def test_now_counts_the_pages_that_never_reached_the_phone(root: Path):
    # The publisher writes one file per undelivered page. Raising them through the real
    # publisher is what proves the reader and the writer agree on where they land.
    publisher = Publisher(lake_root=root, transport=_Undeliverable(), pid=7)
    for title in ("Capture down: SPY chains", "Capture down: quote sampler dead"):
        assert publisher.publish(
            Message(event="capture_down", title=title, body="b"), now=NOW
        ).recorded

    assert service_over(root).run_query("now", {})["pages_failed_to_send"] == 2


def test_now_reports_how_old_the_stamp_is(root: Path):
    """Every daemon field is only as fresh as the write that produced it.

    The daemon stamps every minute it is awake, so the age is what separates a reading of
    now from the last thing a dead daemon said. Without it the panel shows a mint time, a
    countdown and a ping with no way to tell whether any of them still holds.
    """
    # The capture side owns this instant, and the daemon writes it every minute it is
    # awake, on a capture minute and an idle one alike. The dead-man's own record is a
    # separate key, so a ping alone leaves the age unwritten.
    stamp_cycle(root, at=et(MONDAY, 9, 35), token_minted_at=MINTED, roster=_roster())

    now = service_over(root).run_query("now", {})

    # NOW is 09:40:30, so the stamp is five and a half minutes old.
    assert now["stamp_age_minutes"] == 5.5


def test_an_unwritten_stamp_has_no_age_rather_than_a_zero(root: Path):
    """A zero would read as "written this instant", which is the opposite of the truth."""
    assert service_over(root).run_query("now", {})["stamp_age_minutes"] is None


def test_a_page_lost_on_another_day_is_not_todays_count(root: Path):
    publisher = Publisher(lake_root=root, transport=_Undeliverable(), pid=7)
    publisher.publish(Message(event="capture_down", title="t", body="b"), now=et(FRIDAY, 16, 0))

    # The panel is about now, and the day is the Eastern one the publisher files under.
    assert service_over(root).run_query("now", {})["pages_failed_to_send"] == 0


def test_the_page_count_is_keyed_to_the_eastern_day_not_the_utc_one(root: Path):
    """After 20:00 Eastern the two dates differ, and the publisher files under Eastern.

    A count keyed to `ctx.now.date()` reads an empty next-day directory all evening and
    reports zero undelivered pages, during exactly the window when a page most likely
    failed. The sibling test above cannot see this: at 09:40 Eastern the two dates agree.
    """
    publisher = Publisher(lake_root=root, transport=_Undeliverable(), pid=7)
    publisher.publish(Message(event="capture_down", title="t", body="b"), now=et(MONDAY, 16, 0))

    evening = et(MONDAY, 21, 30)
    # The premise: this instant is Monday in Eastern and Tuesday in UTC.
    assert evening.astimezone(UTC).date() != evening.date()

    now = service_over(root, now=evening).run_query("now", {})
    assert now["session_date"] == MONDAY.isoformat()
    assert now["pages_failed_to_send"] == 1


# -- today -------------------------------------------------------------------


def test_today_denominates_a_regular_day_at_the_full_session(service: DashboardService):
    today = service.run_query("today", {"date": "2026-08-24", "ticker": "SPY"})
    assert today["is_session"] is True
    assert today["early_close"] is False
    assert today["session_open"] == et(MONDAY, 9, 30).isoformat()
    assert today["option_close"] == et(MONDAY, 16, 15).isoformat()
    # The open through the option close, one slot a minute, both ends inclusive.
    assert today["slot_count"] == 406
    assert [(s["ticker"], s["surface"]) for s in today["strips"]] == [
        ("SPY", "chains"),
        ("SPY", "quotes"),
    ]
    for strip in today["strips"]:
        assert len(strip["slots"]) == 406
        assert strip["slots"][0]["slot"] == et(MONDAY, 9, 30).isoformat()
        assert strip["slots"][-1]["slot"] == et(MONDAY, 16, 15).isoformat()


def test_today_strip_carries_status_rows_and_gap_reason(service: DashboardService):
    today = service.run_query("today", {"date": "2026-08-24", "ticker": "SPY"})
    chains = today["strips"][0]
    # Four slots with rows, five judged slots without, the rest still owed. 09:39 and
    # 09:40 sit inside the verdict grace, so they are pending rather than missing.
    assert chains["counts"] == {
        "captured": 2,
        "suspect": 1,
        "gap": 1,
        "missing": 5,
        "pending": 397,
        "out_of_scope": 0,
    }
    assert chains["unreadable_segments"] == 1
    first, second, third, fourth, fifth = chains["slots"][:5]
    assert (first["status"], first["rows"]) == ("captured", 2)
    assert (second["status"], second["rows"]) == ("captured", 2)
    assert (third["status"], third["rows"], third["error_class"]) == ("gap", 0, ["http_429"])
    assert (fourth["status"], fourth["rows"]) == ("suspect", 2)
    assert (fifth["status"], fifth["rows"], fifth["error_class"]) == ("missing", 0, [])
    assert chains["slots"][-1]["status"] == "pending"


def test_today_unions_journal_and_sealed_partition_by_name(service: DashboardService):
    today = service.run_query("today", {"date": "2026-08-24", "ticker": "SPY"})
    quotes = today["strips"][1]
    # 09:30 came from a pinned-schema segment stamped in UTC. 09:31 came from a
    # fixture-schema Parquet partition stamped in Eastern time. Both land as captured.
    assert quotes["counts"]["captured"] == 2
    assert quotes["slots"][0]["status"] == "captured"
    assert quotes["slots"][1]["status"] == "captured"
    assert quotes["slots"][2]["status"] == "missing"


def test_today_defaults_to_every_ticker_and_the_clock_session_date(service: DashboardService):
    today = service.run_query("today", {})
    assert today["date"] == "2026-08-24"
    assert [(s["ticker"], s["surface"]) for s in today["strips"]] == [
        ("QQQ", "chains"),
        ("SPY", "chains"),
        ("SPY", "quotes"),
    ]
    qqq = today["strips"][0]
    assert qqq["counts"] == {
        "captured": 0,
        "suspect": 0,
        "gap": 1,
        "missing": 8,
        "pending": 397,
        "out_of_scope": 0,
    }
    assert qqq["slots"][0]["error_class"] == ["daemon_dead"]


def test_today_denominates_an_early_close_at_the_short_session(service: DashboardService):
    today = service.run_query("today", {"date": "2026-11-27", "ticker": "QQQ"})
    assert today["is_session"] is True
    assert today["early_close"] is True
    assert today["option_close"] == et(EARLY_CLOSE, 13, 15).isoformat()
    # Fewer slots than a regular day, so the half day never renders half-missing.
    assert today["slot_count"] == 226
    strip = today["strips"][0]
    assert len(strip["slots"]) == 226
    assert strip["counts"]["pending"] == 226


def test_today_renders_a_non_session_as_no_session(service: DashboardService):
    today = service.run_query("today", {"date": "2026-08-22"})
    assert today["date"] == "2026-08-22"
    assert today["is_session"] is False
    assert today["slot_count"] == 0
    assert today["strips"] == []


# -- the verdict grace: when a slot with no rows becomes judgeable -------------


def _grace_lake(fixture_lake: FixtureLake) -> Path:
    """A lake whose SPY chains captured 09:30 and 09:31 and nothing since.

    This is the fixture the issue's reproduction ran against. 09:32 is the minute a
    healthy daemon would be capturing right now, and no row for it exists yet.
    """
    return one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
            _chains("SPY", et(MONDAY, 9, 31), occ_symbol="A"),
        ],
    )


def _strip_at(root: Path, when: datetime, surface: str = "chains") -> dict:
    """One SPY surface's strip, read at one instant. Found by name, never by position."""
    strips = service_over(root, now=when).run_query(
        "today", {"date": "2026-08-24", "ticker": "SPY"}
    )["strips"]
    matching = [strip for strip in strips if strip["surface"] == surface]
    assert len(matching) == 1, f"expected one {surface} strip, got {len(matching)}"
    return matching[0]


def _cell_at(root: Path, when: datetime, slot: datetime, surface: str = "chains") -> dict:
    """One slot's whole cell on a SPY strip, read at one instant."""
    strip = _strip_at(root, when, surface)
    index = int((slot - et(MONDAY, 9, 30)).total_seconds() // 60)
    cell = strip["slots"][index]
    assert cell["slot"] == slot.isoformat()
    return cell


def _status_at(root: Path, when: datetime, slot: datetime, surface: str = "chains") -> str:
    """One slot's status on a SPY strip, read at one instant."""
    return _cell_at(root, when, slot, surface)["status"]


@pytest.mark.parametrize("second", [0, 5, 30, 59])
def test_the_minute_being_captured_is_never_called_missing(fixture_lake: FixtureLake, second: int):
    # The bug this holds: the verdict ran against the raw instant, so 09:32 read missing
    # from its own first second, before its cycle could have written anything. The cycle
    # for a slot starts at the top of that minute and has to fetch, journal and fsync, so
    # at every second of 09:32 that minute is still owed rather than absent.
    root = _grace_lake(fixture_lake)
    at = et(MONDAY, 9, 32, second)
    assert _status_at(root, at, et(MONDAY, 9, 32)) == "pending"
    # The two readings either side are unmoved. 09:31 landed a cycle and 09:33 is future.
    assert _status_at(root, at, et(MONDAY, 9, 31)) == "captured"
    assert _status_at(root, at, et(MONDAY, 9, 33)) == "pending"


def test_a_slot_stays_pending_through_the_grace_minute_after_its_own(
    fixture_lake: FixtureLake,
):
    # A cycle that overruns its minute is an ordinary slow sample to the loop, not a
    # failure. Its row lands after the slot's own minute has ended. Judging at that end
    # would call the slot missing and then flip it to captured on the next refresh, so
    # the grace runs one further minute and the verdict never flaps on a healthy session.
    root = _grace_lake(fixture_lake)
    slot = et(MONDAY, 9, 32)
    assert _status_at(root, et(MONDAY, 9, 33, 0), slot) == "pending"
    assert _status_at(root, et(MONDAY, 9, 33, 59), slot) == "pending"


def test_a_slot_is_judged_once_the_grace_has_run_out(fixture_lake: FixtureLake):
    # The grace is a delay, not an amnesty. Two minutes past the slot the cycle has had
    # its own minute and a full spare one, so a slot with still no row is genuinely
    # absent and the strip says so.
    root = _grace_lake(fixture_lake)
    slot = et(MONDAY, 9, 32)
    assert _status_at(root, et(MONDAY, 9, 34, 0), slot) == "missing"
    assert _status_at(root, et(MONDAY, 9, 34, 1), slot) == "missing"


def test_the_grace_is_the_span_the_constant_names(fixture_lake: FixtureLake):
    # The boundary is read off SLOT_VERDICT_GRACE rather than hard-coded here, so a
    # recalibration moves the test with it instead of leaving it asserting the old span.
    root = _grace_lake(fixture_lake)
    slot = et(MONDAY, 9, 32)
    edge = slot + dashboard.SLOT_VERDICT_GRACE
    assert _status_at(root, edge - timedelta(seconds=1), slot) == "pending"
    assert _status_at(root, edge, slot) == "missing"


def test_a_long_dead_morning_still_reads_missing(fixture_lake: FixtureLake):
    # The over-reach check. The grace delays a verdict by two minutes and never withholds
    # one, so a slot well past it with no row is missing exactly as before.
    root = _grace_lake(fixture_lake)
    at = et(MONDAY, 9, 40, 30)
    for minute in range(32, 39):
        assert _status_at(root, at, et(MONDAY, 9, minute)) == "missing"


def test_the_grace_holds_on_the_quotes_surface_too(fixture_lake: FixtureLake):
    # Both minute-cadence surfaces are captured by the same cycle, so both owe a slot on
    # the same terms and both must wait the same grace. Every other test here reads the
    # chains strip, which would leave a grace that reached one surface and not the other
    # looking identical to one that reached both.
    root = one_segment_lake(
        fixture_lake,
        [
            _quotes("SPY", et(MONDAY, 9, 30)),
            _quotes("SPY", et(MONDAY, 9, 31)),
        ],
        surface="quotes",
    )
    slot = et(MONDAY, 9, 32)
    assert _status_at(root, et(MONDAY, 9, 32, 5), slot, "quotes") == "pending"
    assert _status_at(root, et(MONDAY, 9, 33, 59), slot, "quotes") == "pending"
    assert _status_at(root, et(MONDAY, 9, 34, 0), slot, "quotes") == "missing"
    assert _status_at(root, et(MONDAY, 9, 30, 30), et(MONDAY, 9, 30), "quotes") == "captured"


def test_a_gap_marker_inside_the_grace_is_still_a_gap(fixture_lake: FixtureLake):
    # The grace reaches a slot with no rows at all and nothing else. A gap row is data,
    # recording a missed minute and why, so a cycle that failed fast and journalled its
    # reason inside the slot's own minute must read gap immediately. Waiting would hide a
    # recorded failure behind a status that means nobody has looked yet, and the design
    # pins a recorded gap and a hole as different failures.
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
            _chains(
                "SPY",
                et(MONDAY, 9, 31),
                kind=journal.ROW_KIND_GAP,
                error_class="vendor_auth_error",
            ),
        ],
    )
    slot = et(MONDAY, 9, 31)
    # Read five seconds into the minute after the gap's own, well inside the grace.
    cell = _cell_at(root, et(MONDAY, 9, 32, 5), slot)
    assert cell["status"] == "gap"
    assert cell["error_class"] == ["vendor_auth_error"]
    # The count carries it too, so a graced cell can never be rendered without being
    # counted. The six counts still denominate the whole session.
    strip = _strip_at(root, et(MONDAY, 9, 32, 5))
    assert strip["counts"]["gap"] == 1
    assert sum(strip["counts"].values()) == 406


def test_a_slot_with_rows_is_captured_inside_its_own_grace(fixture_lake: FixtureLake):
    # Data wins above the grace, so a cycle that lands promptly reads captured
    # immediately rather than waiting two minutes to be believed.
    root = _grace_lake(fixture_lake)
    assert _status_at(root, et(MONDAY, 9, 31, 10), et(MONDAY, 9, 31)) == "captured"
    assert _status_at(root, et(MONDAY, 9, 30, 1), et(MONDAY, 9, 30)) == "captured"


# -- the contract's guards ---------------------------------------------------


class SpyConnection:
    """A stand-in connection that records every touch. Validation must touch nothing."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def cursor(self) -> SpyConnection:
        self.calls.append("cursor")
        return self

    def execute(self, *args: object) -> SpyConnection:
        self.calls.append("execute")
        return self

    def register(self, *args: object) -> None:
        self.calls.append("register")

    def close(self) -> None:
        self.calls.append("close")


@pytest.mark.parametrize(
    "raw",
    [
        {"ticker": "NOPE"},
        {"ticker": "spy"},
        {"date": "2026-13-45"},
        {"date": "20260824"},
        {"date": "2026-08-24T09:30"},
        {"date": "2026-08-24", "sql": "SELECT 1"},
        {"date": "2026-08-24", "ticker": "SPY", "limit": "1"},
    ],
)
def test_bad_parameters_are_rejected_before_any_sql_runs(root: Path, raw: dict[str, str]):
    spy = SpyConnection()
    service = DashboardService(
        root,
        clock=ManualClock(NOW.astimezone(UTC)),
        calendar=CALENDAR,
        connection=spy,
        page=b"",
        icon=b"",
    )
    with pytest.raises(QueryParameterError):
        service.run_query("today", raw)
    assert spy.calls == []


def test_an_unknown_query_name_never_reaches_the_connection(root: Path):
    spy = SpyConnection()
    service = DashboardService(
        root,
        clock=ManualClock(NOW.astimezone(UTC)),
        calendar=CALENDAR,
        connection=spy,
        page=b"",
        icon=b"",
    )
    # A name no panel will ever take. It used to be "history", which is a real query
    # now, so the assertion below keeps this case from quietly testing nothing again.
    assert "not-a-panel" not in NAMED_QUERIES
    with pytest.raises(KeyError):
        service.run_query("not-a-panel", {})
    assert spy.calls == []


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_the_panels_leave_the_lake_byte_identical(service: DashboardService, root: Path):
    before = _tree_digest(root)
    service.run_query("now", {})
    service.run_query("today", {})
    service.run_query("today", {"date": "2026-08-21", "ticker": "QQQ"})
    service.run_query("history", {})
    service.run_query("lake", {})
    assert _tree_digest(root) == before


# -- the slot aggregate's semantics ------------------------------------------


def test_one_suspect_row_makes_the_whole_slot_suspect(fixture_lake: FixtureLake):
    # A chain snapshot is many rows in one slot. The suspect flag rides on the rows, and
    # a partial snapshot flags only some of them. The slot is suspect when any row is.
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A", suspect=False),
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="B", suspect=True),
        ],
    )
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["slots"][0]["status"] == "suspect"
    assert strip["counts"]["suspect"] == 1
    assert strip["counts"]["captured"] == 0
    assert service_over(root).run_query("now", {})["surfaces"][0]["last_status"] == "suspect"


def test_a_slot_reports_every_error_class_it_carried_and_how_many(
    fixture_lake: FixtureLake,
):
    # A partial chain carries one class per failed date window, so several reasons in one
    # minute is normal. Every one of them is reported, in alphabetical order, and the
    # count beside them is the size of that set. How often a reason repeats decides
    # nothing: ``timeout_error`` is the most common here and lands last on name order.
    classes = ["timeout_error"] * 3 + ["http_429"] * 2 + ["http_500"]
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol=f"C{index}", error_class=reason)
            for index, reason in enumerate(classes)
        ],
    )
    slot = service_over(root).run_query("today", {})["strips"][0]["slots"][0]
    assert slot["status"] == "captured"
    assert slot["error_class"] == ["http_429", "http_500", "timeout_error"]
    assert slot["error_class_count"] == 3
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["last_error_class"] == ["http_429", "http_500", "timeout_error"]
    assert row["last_error_class_count"] == 3


def test_a_failed_close_names_its_own_reason_beside_the_absence_markers(
    fixture_lake: FixtureLake,
):
    """The reason a close is missing survives the markers the rescue attempt writes.

    The 16:15 cycle fails with ``http_500`` and leaves one gap row saying so. The close+5
    fill then lands the tail window and gives up the near one, writing one
    ``chain_chunk_failed`` marker per series that window named. The markers outnumber the
    failure five to one, so counting rows to pick one reason reported the rescue
    attempt's benign class and dropped the reason the close of record is missing.

    The landed window's data rows sit in the same slot carrying a null class, which is
    the shape the query's null filter exists for. They also decide the slot's status: it
    reads captured, exactly as the issue says, and what degraded was only the reason.

    A 15:59 gap under a different class sits below the close, so the Now panel reporting
    any slot but the latest one would name that class instead.
    """
    close = et(MONDAY, 16, 15)
    rows = [
        _chains("SPY", et(MONDAY, 15, 59), journal.ROW_KIND_GAP, error_class="http_401"),
        _chains("SPY", close, journal.ROW_KIND_GAP, error_class="http_500"),
        _chains("SPY", close, occ_symbol="A"),
        _chains("SPY", close, occ_symbol="B"),
    ]
    rows += [
        _chains(
            "SPY",
            close,
            journal.ROW_KIND_GAP,
            expiration_date=f"2026-08-2{index}",
            error_class=capture.CHAIN_CHUNK_FAILED,
        )
        for index in range(5)
    ]
    root = one_segment_lake(fixture_lake, rows)
    slots = {
        slot["slot"]: slot
        for slot in service_over(root).run_query("today", {})["strips"][0]["slots"]
    }
    slot = slots[close.isoformat()]
    assert slot["status"] == "captured"
    assert slot["error_class"] == [capture.CHAIN_CHUNK_FAILED, "http_500"]
    assert slot["error_class_count"] == 2
    # The slot below it carries one class, and reports that one class alone.
    assert slots[et(MONDAY, 15, 59).isoformat()]["error_class"] == ["http_401"]
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["last_error_class"] == [capture.CHAIN_CHUNK_FAILED, "http_500"]
    assert row["last_error_class_count"] == 2


def test_a_row_with_an_unparseable_snap_ts_is_dropped_not_fatal(fixture_lake: FixtureLake):
    # A stamp that will not parse cannot be placed on the strip. It reads as a null slot
    # and is dropped. The panel keeps rendering, and the good slot keeps its own identity.
    unparseable = _chains("SPY", et(MONDAY, 9, 31), occ_symbol="B")
    unparseable["snap_ts"] = "the third minute"
    root = one_segment_lake(
        fixture_lake, [_chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"), unparseable]
    )
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["last_data_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert row["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["counts"]["captured"] == 1
    assert strip["counts"]["gap"] == 0
    assert strip["slots"][1]["status"] == "missing"
    assert strip["unparseable_stamp_rows"] == 1


def test_every_row_with_an_unparseable_snap_ts_is_counted(fixture_lake: FixtureLake):
    # Dropping the row is right. Dropping it quietly is not: a stamp the panel cannot
    # place still says the writer produced something the schema does not describe. The
    # count is of rows, not of groups, and it spans row kinds, because every unplaceable
    # row lands in the one group the aggregate keys on a null slot.
    rows = [
        _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
        _chains("SPY", et(MONDAY, 9, 31), occ_symbol="B"),
        _chains("SPY", et(MONDAY, 9, 32), journal.ROW_KIND_GAP, error_class="http_429"),
    ]
    for row in rows[1:]:
        row["snap_ts"] = "the third minute"
    root = one_segment_lake(fixture_lake, rows)
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["unparseable_stamp_rows"] == 2
    # The two bad rows are counted here and nowhere else. They are not drifted rows, and
    # the one good minute still renders.
    assert strip["drifted_rows"] == 0
    assert strip["counts"]["captured"] == 1
    assert strip["counts"]["gap"] == 0
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["unparseable_stamp_rows"] == 2
    assert row["last_data_snap_ts"] == et(MONDAY, 9, 30).isoformat()


def test_a_segment_missing_an_optional_provenance_column_still_reads(fixture_lake: FixtureLake):
    # ``error_class`` is null on a data row anyway, so a segment without the column nulls
    # it rather than being skipped. A missing optional column is not drift.
    row = _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")
    root = one_segment_lake(
        fixture_lake, [row], schema=without(journal.CHAINS_SCHEMA, "error_class")
    )
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["slots"][0]["status"] == "captured"
    assert strip["slots"][0]["error_class"] == []
    assert strip["slots"][0]["error_class_count"] == 0
    assert strip["drifted_segments"] == 0


# -- segment discovery and the day walk --------------------------------------


def test_a_stray_arrows_file_is_neither_read_nor_counted(service: DashboardService):
    # ``notes.arrows`` sits beside the segments and does not match the segment glob.
    # Only ``seg-garbage-9.arrows`` is opened, so exactly one file reads unreadable.
    spy = next(
        row
        for row in service.run_query("now", {})["surfaces"]
        if (row["ticker"], row["surface"]) == ("SPY", "chains")
    )
    assert spy["unreadable_segments"] == 1
    assert spy["vanished_segments"] == 0
    assert spy["shadow_append_segments"] == 0
    assert spy["drifted_segments"] == 0
    assert spy["last_data_snap_ts"] == et(MONDAY, 9, 33).isoformat()


def test_the_day_walk_stops_at_the_newest_day_with_a_data_cycle(service: DashboardService):
    # QQQ has a gap-only Monday, then Friday and Thursday with data. The walk stops at
    # Friday. Without the stop, Thursday's older cycle would overwrite it.
    qqq = next(row for row in service.run_query("now", {})["surfaces"] if row["ticker"] == "QQQ")
    assert qqq["last_data_snap_ts"] == et(FRIDAY, 16, 15).isoformat()
    assert qqq["last_data_snap_ts"] != et(THURSDAY, 16, 15).isoformat()


def _gap_only_days(fixture_lake: FixtureLake, ticker: str, days: list[date]) -> Path:
    """A lake where one ticker has a gap and nothing else on each of ``days``."""
    for day in days:
        fixture_lake.with_journal_segment(
            "chains",
            ticker,
            day,
            pa.Table.from_pylist(
                [_chains(ticker, et(day, 9, 30), journal.ROW_KIND_GAP, error_class="daemon_dead")],
                schema=journal.CHAINS_SCHEMA,
            ),
            start_ts="20260824T133000000000",
            pid=1,
        )
    return fixture_lake.build()


def test_the_lookback_says_so_when_the_day_walk_hits_its_cap(fixture_lake: FixtureLake):
    # One day past the cap, all of them gap-only. The row must say the walk ran out of
    # sessions rather than implying the ticker never captured.
    days = [MONDAY - timedelta(days=back) for back in range(MAX_LOOKBACK_SESSIONS + 1)]
    root = _gap_only_days(fixture_lake, "SPY", days)
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["lookback_exhausted"] is True
    assert row["last_data_snap_ts"] is None
    assert row["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()


def test_a_half_written_partition_file_is_not_a_day(fixture_lake: FixtureLake):
    # Exactly the cap's worth of gap-only days, so the walk covers every one and the
    # lookback is not exhausted. A half-written file names an eleventh day. It is not a
    # partition, so counting it would falsely report the walk as truncated.
    days = [MONDAY - timedelta(days=back) for back in range(MAX_LOOKBACK_SESSIONS)]
    root = _gap_only_days(fixture_lake, "SPY", days)
    stray = root / "chains" / "ticker=SPY" / "date=2026-08-14.tmp"
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_bytes(b"a partition half written")
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["lookback_exhausted"] is False


def test_a_partition_spelled_outside_the_date_key_is_not_a_day(fixture_lake: FixtureLake):
    # The same cap's worth of gap-only days, so an eleventh day would report the walk as
    # truncated. Two Parquet files name an eleventh day in spellings this lake never
    # writes. The first drops the ``date=`` key. The second keeps the key and spells the
    # day the compact way, which ``date.fromisoformat`` alone still reads as 2026-08-14.
    #
    # Neither file was built by this pipeline, so neither is a day. The panel reads its
    # date directories and its partition stems through the one parser compaction's sweep
    # uses, so a name the panel refuses is a name the sweep will not seal.
    days = [MONDAY - timedelta(days=back) for back in range(MAX_LOOKBACK_SESSIONS)]
    root = _gap_only_days(fixture_lake, "SPY", days)
    partitions = root / "chains" / "ticker=SPY"
    partitions.mkdir(parents=True, exist_ok=True)
    for name in ("2026-08-14.parquet", "date=20260814.parquet"):
        (partitions / name).write_bytes(b"never opened, because it is never a day")
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["lookback_exhausted"] is False


def test_a_ticker_with_no_data_cycle_ever_reports_null_freshness(fixture_lake: FixtureLake):
    # Day one for a newly onboarded ticker: a gap landed, no cycle has. Freshness is null
    # rather than zero, because there is no cycle to measure an age against.
    root = one_segment_lake(
        fixture_lake,
        [_chains("NEW", et(MONDAY, 9, 30), journal.ROW_KIND_GAP, error_class="http_429")],
        ticker="NEW",
    )
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["ticker"] == "NEW"
    assert row["last_data_snap_ts"] is None
    assert row["minutes_since"] is None
    assert row["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert row["last_status"] == "gap"
    assert row["last_error_class"] == ["http_429"]
    assert row["lookback_exhausted"] is False
    # A null age is not a clean row. Nothing has ever captured while a cycle was owed,
    # which is the worst reading the column has, so it is the one that must be loud.
    assert row["stale"] is True


# -- the roster and the lake's shape -----------------------------------------


def test_an_empty_lake_renders_as_no_tickers(fixture_lake: FixtureLake):
    # The state before the first capture cycle: no journal directory, no partitions. The
    # day still denominates at the full session, so the page shows a session with nothing
    # captured rather than failing to load.
    root = fixture_lake.build()
    assert not (root / JOURNAL_DIR).exists()
    now = service_over(root).run_query("now", {})
    assert now["tickers"] == []
    assert now["surfaces"] == []
    today = service_over(root).run_query("today", {})
    assert today["is_session"] is True
    assert today["slot_count"] == 406
    assert today["strips"] == []


def test_a_ticker_whose_only_data_is_a_sealed_partition_is_in_the_roster(
    fixture_lake: FixtureLake,
):
    # The post-compaction steady state for every day but today. The roster must read the
    # partition tree, not the journal alone, or a compacted ticker vanishes from the page.
    fixture_lake.with_quotes(
        "IWM", MONDAY, sample_quotes_table([_fixture_row("IWM", et(MONDAY, 9, 30))])
    )
    root = fixture_lake.build()
    assert not (root / JOURNAL_DIR).exists()
    now = service_over(root).run_query("now", {})
    assert now["tickers"] == ["IWM"]
    assert now["surfaces"][0]["surface"] == "quotes"
    assert now["surfaces"][0]["last_data_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert now["surfaces"][0]["minutes_since"] == 10.5


def test_a_stamped_ticker_that_journalled_nothing_shows_as_failing(root: Path):
    # The design's reason for the roster stamp. Read off the lake's layout alone, a
    # ticker whose every cycle failed has no directory and no row, so the panel shows
    # nothing at all where it should show a capture that is down.
    stamp_cycle(
        root,
        at=NOW,
        token_minted_at=MINTED,
        roster=Roster.from_mapping({"IWM": {"options": False}}),
    )
    assert not (root / "quotes" / "ticker=IWM").exists()

    now = service_over(root).run_query("now", {})
    assert now["tickers"] == ["IWM", "QQQ", "SPY"]
    iwm = [row for row in now["surfaces"] if row["ticker"] == "IWM"]
    # An equity-only ticker is expected on quotes alone, so it gets that one row.
    assert [row["surface"] for row in iwm] == ["quotes"]
    assert iwm[0]["last_data_snap_ts"] is None
    assert iwm[0]["minutes_since"] is None
    assert iwm[0]["lookback_exhausted"] is False


def test_a_stamped_ticker_is_queryable_and_renders_a_dark_strip(root: Path):
    # The roster is the request allow-list, so a stamped ticker has to pass validation
    # too. Its Today strip is every slot missing, which is the failing capture drawn out.
    stamp_cycle(
        root,
        at=NOW,
        token_minted_at=MINTED,
        roster=Roster.from_mapping({"IWM": {"options": False}}),
    )

    today = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "IWM"})
    strip = today["strips"][0]
    assert strip["ticker"] == "IWM"
    assert strip["counts"]["captured"] == 0
    assert strip["counts"]["missing"] > 0


def test_a_ticker_dropped_from_the_stamp_keeps_the_days_it_captured(root: Path):
    # The stamp is the daemon's current roster, not the lake's history. A ticker retired
    # from tickers.yaml still has sealed partitions, and the panel must still reach them.
    stamp_cycle(
        root,
        at=NOW,
        token_minted_at=MINTED,
        roster=Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
    )

    now = service_over(root).run_query("now", {})
    assert "QQQ" in now["tickers"]
    qqq = next(row for row in now["surfaces"] if row["ticker"] == "QQQ")
    assert qqq["last_data_snap_ts"] == et(FRIDAY, 16, 15).isoformat()


def test_a_malformed_stamped_ticker_never_enters_the_roster(root: Path):
    # The stamp is a file under lake_root, and the roster it feeds is the allow-list a
    # request ticker is checked against. So a stamped name passes the same shape gate a
    # directory name does, and a stamped surface the panels do not read is dropped.
    path = root / JOURNAL_DIR / "metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "tickers": {
                    "DROP TABLE": ["quotes"],
                    "spy": ["quotes"],
                    "IWM": ["quotes", "bars"],
                }
            }
        )
    )

    now = service_over(root).run_query("now", {})
    assert now["tickers"] == ["IWM", "QQQ", "SPY"]
    assert [row["surface"] for row in now["surfaces"] if row["ticker"] == "IWM"] == ["quotes"]
    with pytest.raises(QueryParameterError):
        service_over(root).run_query("today", {"ticker": "DROP TABLE"})


def test_a_stamp_nested_past_the_recursion_limit_does_not_break_the_page(root: Path):
    # The stamp's own promise is that a corrupt one reads as an empty record rather than
    # raising into a panel. A payload nested deeply enough raises ``RecursionError``,
    # which is a ``RuntimeError`` and passed a guard naming ``OSError`` and ``ValueError``.
    # The page this breaks is the one that would have shown capture failing, and the two
    # queries below are the two that read the stamp.
    path = root / JOURNAL_DIR / "metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"a":' * 50000 + "1" + "}" * 50000)

    now = service_over(root).run_query("now", {})

    assert now["token_minted_at"] is None, "a corrupt stamp reported a mint time"
    # The roster falls back to what the lake's own directories show, so the page still
    # renders rather than reporting no tickers at all.
    assert now["tickers"] == ["QQQ", "SPY"]


def test_a_malformed_ticker_directory_never_enters_the_roster(root: Path):
    # The roster is the allow-list a request ticker is checked against, so its shape gate
    # is a boundary rule. A half-renamed directory, a lower-case one, and a name carrying
    # SQL are all outside the ticker shape.
    for name in ("ticker=SPY.tmp", "ticker=spy", "ticker=DROP TABLE", "ticker="):
        (root / "quotes" / name).mkdir(parents=True, exist_ok=True)
    now = service_over(root).run_query("now", {})
    assert now["tickers"] == ["QQQ", "SPY"]
    with pytest.raises(QueryParameterError):
        service_over(root).run_query("today", {"ticker": "SPY.tmp"})


def test_the_panel_survives_malformed_directory_names(root: Path):
    # Four shapes the walk can meet in a live lake.
    #
    # 1. A child that is not a ticker directory.
    # 2. A file where a ticker directory belongs.
    # 3. A date directory whose date is not a date.
    # 4. A date directory whose name is not a date at all.
    (root / "quotes" / "reports").mkdir(parents=True, exist_ok=True)
    (root / "quotes" / "ticker=IWM").write_bytes(b"a file where a directory belongs")
    for name in ("date=2026-13-45", "date=notadate"):
        (root / JOURNAL_DIR / name / "surface=quotes" / "ticker=SPY").mkdir(parents=True)
    now = service_over(root).run_query("now", {})
    assert now["tickers"] == ["QQQ", "SPY"]
    quotes = next(row for row in now["surfaces"] if row["surface"] == "quotes")
    assert quotes["last_data_snap_ts"] == et(MONDAY, 9, 31).isoformat()


def test_a_directory_removed_mid_walk_is_not_a_server_error(root: Path, monkeypatch):
    # Compaction prunes an emptied journal date while holding the lake lock, and the
    # panel walks without one. So a directory the walk just saw can be gone by the time
    # it is listed. The hook makes that race certain: every journal date directory is
    # removed the instant the walk confirms it is a directory.
    real_is_dir = Path.is_dir
    removed: list[Path] = []

    def racing_is_dir(self: Path) -> bool:
        found = real_is_dir(self)
        if found and self.name.startswith(DATE_PREFIX) and self.parent.name == JOURNAL_DIR:
            shutil.rmtree(self)
            removed.append(self)
        return found

    monkeypatch.setattr(Path, "is_dir", racing_is_dir)
    now = service_over(root).run_query("now", {})
    assert removed
    # The journal is gone, so the roster is whatever the partition tree still holds.
    assert now["tickers"] == ["QQQ", "SPY"]


def test_the_roster_is_walked_once_per_request(root: Path, monkeypatch):
    # The walk lists every journal date, so it is the request's most expensive step.
    # Validation and the query itself share the one result.
    walks: list[object] = []
    real_roster = dashboard.lake_roster

    def counting_roster(paths):
        walks.append(paths)
        return real_roster(paths)

    monkeypatch.setattr(dashboard, "lake_roster", counting_roster)
    service = service_over(root)
    service.run_query("today", {"date": "2026-08-24", "ticker": "SPY"})
    assert len(walks) == 1
    walks.clear()
    service.run_query("now", {})
    assert len(walks) == 1


# -- segments that read badly ------------------------------------------------


def test_a_castable_retype_is_repaired_rather_than_skipped(fixture_lake: FixtureLake):
    # ``suspect`` written as an int8 casts back to its pinned type, so the segment reads
    # in full. No query raises and nothing is counted drifted.
    row = _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")
    row["suspect"] = 0
    root = one_segment_lake(
        fixture_lake, [row], schema=retyped(journal.CHAINS_SCHEMA, "suspect", pa.int8())
    )
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["slots"][0]["status"] == "captured"
    assert strip["drifted_segments"] == 0
    assert (
        service_over(root).run_query("now", {})["surfaces"][0]["last_data_snap_ts"]
        == et(MONDAY, 9, 30).isoformat()
    )


def test_an_uncastable_retype_is_counted_and_leaves_other_tickers_readable(
    fixture_lake: FixtureLake,
):
    # ``suspect`` written as free text will not cast back. The segment is skipped and
    # counted, never raised. This is the failure that once blanked the whole lake: one
    # drifted segment must not cost every other ticker its panel.
    drifted = _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")
    drifted["suspect"] = "yes"
    fixture_lake.with_journal_segment(
        "chains",
        "SPY",
        MONDAY,
        pa.Table.from_pylist(
            [drifted], schema=retyped(journal.CHAINS_SCHEMA, "suspect", pa.string())
        ),
        start_ts="20260824T133000000000",
        pid=1,
    )
    root = one_segment_lake(fixture_lake, [_chains("QQQ", et(MONDAY, 9, 31))], ticker="QQQ")

    rows = {row["ticker"]: row for row in service_over(root).run_query("now", {})["surfaces"]}
    assert rows["SPY"]["drifted_segments"] == 1
    assert rows["SPY"]["last_data_snap_ts"] is None
    assert rows["QQQ"]["drifted_segments"] == 0
    assert rows["QQQ"]["last_data_snap_ts"] == et(MONDAY, 9, 31).isoformat()

    strips = {
        strip["ticker"]: strip for strip in service_over(root).run_query("today", {})["strips"]
    }
    assert strips["SPY"]["drifted_segments"] == 1
    assert strips["SPY"]["counts"]["captured"] == 0
    assert strips["QQQ"]["counts"]["captured"] == 1


def test_a_segment_without_row_kind_renders_missing_not_gaps(fixture_lake: FixtureLake):
    # ``row_kind`` says what a row records. Nulling it would bind the union and then read
    # as a gap, turning schema drift into invented gaps. A gap is data and is never
    # inferred from absence, so the segment is skipped, counted, and reads missing.
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
            _chains("SPY", et(MONDAY, 9, 31), occ_symbol="A"),
            _chains("SPY", et(MONDAY, 9, 32), occ_symbol="A"),
        ],
        schema=without(journal.CHAINS_SCHEMA, "row_kind"),
    )
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["drifted_segments"] == 1
    assert strip["counts"]["gap"] == 0
    assert strip["counts"]["captured"] == 0
    assert strip["counts"]["missing"] == 9
    assert strip["slots"][0]["status"] == "missing"


def test_rows_of_an_unrecognized_kind_are_counted_not_read_as_gaps(fixture_lake: FixtureLake):
    # A kind the panels do not recognize is drift, not a gap. The rows are counted and
    # the slot falls out of the strip, so it renders missing and never invents a gap.
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), "marker", occ_symbol="A"),
            _chains("SPY", et(MONDAY, 9, 30), "marker", occ_symbol="B"),
        ],
    )
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["drifted_rows"] == 2
    assert strip["drifted_segments"] == 0
    assert strip["counts"]["gap"] == 0
    assert strip["slots"][0]["status"] == "missing"
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["last_snap_ts"] is None
    assert row["drifted_rows"] == 2


def test_a_shadow_append_is_counted_apart_from_corruption(fixture_lake: FixtureLake):
    # Bytes past the end-of-stream marker are the loud failure the design pins. A standard
    # reader stops at the marker and never sees them, so the write silently loses data.
    root = one_segment_lake(fixture_lake, [_chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")])
    segment = fixture_lake.segment_path("chains", "SPY", MONDAY, "20260824T133000000000", 1)
    with segment.open("ab") as sink:
        sink.write(b"rows appended past the marker")
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["shadow_append_segments"] == 1
    assert row["unreadable_segments"] == 0
    assert row["vanished_segments"] == 0


def test_a_segment_that_vanishes_under_a_landing_seal_counts_as_vanished(
    fixture_lake: FixtureLake, monkeypatch
):
    # Compaction seals a ticker-day and deletes its segments while holding the lake lock.
    # The panel reads without one, so a segment it just listed can be gone by the time it
    # is opened. That is a seal landing mid-read, not corruption.
    root = one_segment_lake(fixture_lake, [_chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")])
    real_is_file = Path.is_file

    def racing_is_file(self: Path) -> bool:
        found = real_is_file(self)
        if found and fnmatchcase(self.name, SEGMENT_GLOB):
            self.unlink()
        return found

    monkeypatch.setattr(Path, "is_file", racing_is_file)
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["vanished_segments"] == 1
    assert row["unreadable_segments"] == 0
    assert row["shadow_append_segments"] == 0


# -- the compaction race -----------------------------------------------------


def between_the_partition_check_and_the_segment_read(monkeypatch, action: Callable[[], None]):
    """Run ``action`` once, in the window compaction's seal lands in.

    ``_slot_aggregates`` looks for the sealed partition, then reads the day's segments,
    then looks for the partition again. Compaction holds the lake lock while it seals a
    ticker-day and deletes its segments. The panel reads without one, so the seal can
    land anywhere in that sequence. Hooking the segment loader puts ``action`` exactly
    between the first look and the read, which is the interleaving that costs a fully
    captured day its whole strip when the second look is missing.

    The action fires once. A request calls the loader per ticker, surface, and day, and
    a seal lands once.
    """
    real_loader = dashboard._load_journal_rows
    fired: list[bool] = []

    def hooked(segments):
        if not fired:
            fired.append(True)
            action()
        return real_loader(segments)

    monkeypatch.setattr(dashboard, "_load_journal_rows", hooked)


def test_a_seal_landing_before_the_segment_read_still_renders_the_day(
    fixture_lake: FixtureLake, monkeypatch
):
    # The day is captured in full and lives in one segment. The seal writes the partition
    # and deletes the segment while the panel is between its first look and its read, so
    # every segment reads vanished and the rows have to come from the partition. Without
    # the second look the panel finds neither and renders a complete day as entirely
    # missing.
    root = one_segment_lake(
        fixture_lake,
        [
            _chains("SPY", et(MONDAY, 9, 30), occ_symbol="A"),
            _chains("SPY", et(MONDAY, 9, 31), occ_symbol="A"),
        ],
    )
    segment = fixture_lake.segment_path("chains", "SPY", MONDAY, "20260824T133000000000", 1)

    def seal() -> None:
        fixture_lake.with_chains(
            "SPY",
            MONDAY,
            sample_chains_table(
                [_fixture_row("SPY", et(MONDAY, 9, 30)), _fixture_row("SPY", et(MONDAY, 9, 31))]
            ),
        ).build()
        segment.unlink()

    between_the_partition_check_and_the_segment_read(monkeypatch, seal)
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["vanished_segments"] == 1
    assert strip["counts"]["captured"] == 2
    assert strip["slots"][0]["status"] == "captured"
    assert strip["slots"][1]["status"] == "captured"


def _segment_and_partition_lake(fixture_lake: FixtureLake) -> Path:
    """A lake whose ticker-day holds one segment at 09:30 and a partition at 09:31."""
    fixture_lake.with_journal_segment(
        "chains",
        "SPY",
        MONDAY,
        pa.Table.from_pylist(
            [_chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")], schema=journal.CHAINS_SCHEMA
        ),
        start_ts="20260824T133000000000",
        pid=1,
    )
    fixture_lake.with_chains(
        "SPY", MONDAY, sample_chains_table([_fixture_row("SPY", et(MONDAY, 9, 31))])
    )
    return fixture_lake.build()


@pytest.mark.parametrize("loss", ["removed", "unreadable"])
def test_a_partition_lost_after_the_check_degrades_to_the_journal(
    fixture_lake: FixtureLake, monkeypatch, loss: str
):
    # A restore, a repair, or a torn write can take the partition away between the look
    # and the read. DuckDB raises out of the read either way: a path that no longer
    # resolves is an IO error, and bytes that are not Parquet are an invalid-input error.
    # The request degrades to the journal rows and counts the loss, rather than dying.
    root = _segment_and_partition_lake(fixture_lake)
    partition = fixture_lake.partition_path("chains", "SPY", MONDAY)

    def lose() -> None:
        if loss == "removed":
            partition.unlink()
        else:
            partition.write_bytes(b"not a parquet file")

    between_the_partition_check_and_the_segment_read(monkeypatch, lose)
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["unreadable_partitions"] == 1
    # 09:30 came from the segment and survives. 09:31 lived only in the lost partition.
    assert strip["counts"]["captured"] == 1
    assert strip["slots"][0]["status"] == "captured"
    assert strip["slots"][1]["status"] == "missing"


def test_a_partition_read_in_full_counts_no_loss(fixture_lake: FixtureLake):
    # The control for the pair above. Nothing takes the partition away, so both
    # minutes render and the loss counter stays at zero. Without it the degradation
    # assertions could be satisfied by a panel that never reads a partition at all.
    root = _segment_and_partition_lake(fixture_lake)
    strip = service_over(root).run_query("today", {})["strips"][0]
    assert strip["unreadable_partitions"] == 0
    assert strip["counts"]["captured"] == 2


# -- the capture_start clamp -------------------------------------------------


def write_master(root: Path, ticker: str, capture_start: datetime) -> Path:
    """A security master and a matching open capture span under ``root``.

    The span starts at ``capture_start`` and stays open, reproducing the exact clamp
    an old-style single-epoch registration gave: every instant at or after it is in
    scope, nothing before it is.
    """
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY,
        capture_start=capture_start,
        valid_from=date(2026, 1, 2),
        ticker=ticker,
    )
    path = master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(instrument_id, capture_start, False)
    spans.write(spans_path(root))
    return path


def write_closed_span(root: Path, ticker: str, capture_start: datetime, end: datetime) -> Path:
    """A security master and one CLOSED capture span, standing for a retired ticker."""
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY,
        capture_start=capture_start,
        valid_from=date(2026, 1, 2),
        ticker=ticker,
    )
    path = master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(instrument_id, capture_start, False)
    spans.close_span(instrument_id, end)
    spans.write(spans_path(root))
    return path


def test_slots_before_capture_start_read_out_of_scope(root: Path):
    # Onboarding renders as "onboarded 09:36," never as a morning of gaps. Data still
    # wins above the clamp, so a real cycle before the epoch is never hidden.
    write_master(root, "SPY", et(MONDAY, 9, 36))
    today = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})
    chains = today["strips"][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert chains["counts"] == {
        "captured": 2,
        "suspect": 1,
        "gap": 0,
        "missing": 3,
        "pending": 397,
        "out_of_scope": 3,
    }
    # 09:32 carried a gap marker. Before the epoch it is out of scope, never a gap.
    assert chains["slots"][2]["status"] == "out_of_scope"
    assert chains["slots"][2]["error_class"] == []
    assert chains["slots"][0]["status"] == "captured"
    assert chains["slots"][3]["status"] == "suspect"
    row = next(
        row
        for row in service_over(root).run_query("now", {})["surfaces"]
        if (row["ticker"], row["surface"]) == ("SPY", "chains")
    )
    assert row["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert row["in_scope"] is True


def test_a_ticker_onboarded_later_today_is_not_yet_in_scope(root: Path):
    write_master(root, "SPY", et(MONDAY, 11, 0))
    row = next(
        row
        for row in service_over(root).run_query("now", {})["surfaces"]
        if (row["ticker"], row["surface"]) == ("SPY", "chains")
    )
    assert row["capture_start"] == et(MONDAY, 11, 0).isoformat()
    assert row["in_scope"] is False


def test_without_a_security_master_there_is_no_clamp(root: Path):
    # A missing reference table must never break a panel, and must never invent a clamp.
    assert not master_path(root).exists()
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] is None
    assert chains["counts"]["out_of_scope"] == 0
    assert chains["counts"]["gap"] == 1


def test_a_ticker_the_master_does_not_resolve_is_unclamped(root: Path):
    # The master is present but knows only IWM. SPY resolves to nothing, so it gets no
    # clamp at all rather than someone else's epoch.
    write_master(root, "IWM", et(MONDAY, 11, 0))
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] is None
    assert chains["counts"]["out_of_scope"] == 0
    assert chains["counts"]["gap"] == 1


def test_a_day_before_the_master_maps_the_ticker_is_out_of_scope_on_today_too(root: Path):
    # Marketlake #405. The master's mapping opens on Monday, the way the live lake's
    # opens on its onboarding day, and Thursday is queried. Asked as of Thursday the
    # master answers ``None``, the ticker falls out of the clamp, and every slot renders
    # ``missing``, which is the one status the clamp exists to prevent. The clamp asks
    # for the spelling instead, so the whole day reads out of scope.
    write_master_valid_from(root, "SPY", et(MONDAY, 9, 30), valid_from=MONDAY)
    chains = service_over(root).run_query("today", {"date": "2026-08-20", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["counts"]["missing"] == 0
    assert chains["counts"]["out_of_scope"] == len(chains["slots"])
    assert chains["capture_start"] == et(MONDAY, 9, 30).isoformat()


def test_a_ticker_the_master_no_longer_maps_today_still_clamps(root: Path):
    # The half an as-of-today resolution could not reach. SPY was re-symboled to SPYX on
    # Friday and its span closed Thursday afternoon, so no date the panel could pick
    # resolves it: Monday is past the mapping's end. Every Monday slot is out of scope,
    # because the span closed before the day began.
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY,
        capture_start=et(THURSDAY, 9, 30),
        valid_from=date(2026, 1, 2),
        ticker="SPY",
    )
    master.remap(instrument_id, ID_TYPE_TICKER, "SPYX", effective=FRIDAY)
    master.write(master_path(root))
    assert master.resolve("SPY", on=MONDAY, id_type=ID_TYPE_TICKER) is None
    spans = CaptureSpans()
    spans.open_span(instrument_id, et(THURSDAY, 9, 30), False)
    spans.close_span(instrument_id, et(THURSDAY, 16, 0))
    spans.write(spans_path(root))
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["counts"]["missing"] == 0
    assert chains["counts"]["gap"] == 0
    assert chains["counts"]["pending"] == 0
    # Data still outranks the clamp, so the fixture's three Monday cycles keep their own
    # statuses and every other minute of the day is out of scope.
    assert chains["counts"] | {"captured": 0, "suspect": 0} == {
        "captured": 0,
        "suspect": 0,
        "gap": 0,
        "missing": 0,
        "pending": 0,
        "out_of_scope": len(chains["slots"]) - 3,
    }
    assert chains["counts"]["captured"] + chains["counts"]["suspect"] == 3


def test_a_recycled_ticker_unions_both_instruments_spans_in_start_order(root: Path):
    # One spelling, two instruments over disjoint ranges. Partitions are keyed by
    # ticker, so the one ``ticker=SPY`` directory holds both instruments' rows and the
    # honest scope for it covers both spans. The spans file lists the newer instrument
    # first, so an unsorted union would report the older span's start as the reported
    # ``capture_start`` and would leave Monday's first six minutes in scope.
    master = SecurityMaster(
        [
            Mapping(
                instrument_id=1,
                id_type=ID_TYPE_TICKER,
                id_value="SPY",
                valid_from=date(2026, 1, 2),
                valid_to=FRIDAY,
                kind=KIND_EQUITY,
                capture_start=et(THURSDAY, 9, 30).astimezone(UTC),
            ),
            Mapping(
                instrument_id=2,
                id_type=ID_TYPE_TICKER,
                id_value="SPY",
                valid_from=FRIDAY,
                valid_to=None,
                kind=KIND_EQUITY,
                capture_start=et(MONDAY, 9, 36).astimezone(UTC),
            ),
        ]
    )
    master.write(master_path(root))
    CaptureSpans(
        [
            CaptureSpan(2, et(MONDAY, 9, 36).astimezone(UTC), None, False),
            CaptureSpan(1, et(THURSDAY, 9, 30).astimezone(UTC), et(THURSDAY, 16, 0), False),
        ]
    ).write(spans_path(root))
    service = service_over(root)
    # The sort's only observable is the reported epoch, because ``_in_scope`` asks every
    # span. Unsorted, the file's order puts the older instrument's span last and this
    # reads Thursday 09:30.
    monday = service.run_query("today", {"date": "2026-08-24", "ticker": "SPY"})["strips"][0]
    assert monday["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert monday["slots"][5]["status"] == "out_of_scope"
    assert monday["slots"][6]["status"] == "missing"
    # Thursday belongs to the older instrument. Its span opens at 09:30 and closes at
    # 16:00, so the morning is owed and the last 16 minutes are not. Take the older
    # instrument out of the union and the whole day reads out of scope instead.
    thursday = service.run_query("today", {"date": "2026-08-20", "ticker": "SPY"})["strips"][0]
    assert thursday["counts"]["missing"] == 390
    assert thursday["counts"]["out_of_scope"] == 16
    # Friday sits between the two spans, which is the away period neither covers.
    friday = service.run_query("today", {"date": "2026-08-21", "ticker": "SPY"})["strips"][0]
    assert friday["counts"]["out_of_scope"] == len(friday["slots"])


# -- a security master or a spans file whose types drifted -------------------

# What each drift test below shares: a well-formed file rewritten with some columns'
# types swapped. The names still line up, so the read itself succeeds and the wrong
# types surface later, from a comparison several frames deep rather than from the read.
# ``casts`` names the columns to swap and leaves the rest pinned. An empty ``casts``
# writes a well-formed file, which is what makes the control test possible: it stops the
# whole set from being satisfied by a panel that never clamps at all.


def _retype(table: pa.Table, casts: dict[str, pa.DataType]) -> pa.Table:
    drifted = pa.schema(
        [pa.field(field.name, casts.get(field.name, field.type)) for field in table.schema]
    )
    return table.cast(drifted)


def write_retyped_master(
    root: Path, ticker: str, capture_start: datetime, casts: dict[str, pa.DataType]
) -> Path:
    """A master carrying the pinned column names with some of their types swapped.

    Scope now clamps off the spans file, so a valid one is written alongside, isolating
    the drift to the master. This exercises the master's own two live failure surfaces:
    ``resolve`` (walked by ``valid_from``) and the schema-version guard.
    """
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY, capture_start=capture_start, valid_from=date(2026, 1, 2), ticker=ticker
    )
    path = master_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_retype(master.to_table(), casts), path)
    spans = CaptureSpans()
    spans.open_span(instrument_id, capture_start, False)
    spans.write(spans_path(root))
    return path


def write_retyped_spans(
    root: Path, ticker: str, capture_start: datetime, casts: dict[str, pa.DataType]
) -> Path:
    """A spans file carrying the pinned column names with some of their types swapped.

    A valid master is written alongside, isolating the drift to the spans file. This
    exercises the spans reader's own failure surfaces: ``_valid_span`` (walked by
    ``span_start``) and the schema-version guard.
    """
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY, capture_start=capture_start, valid_from=date(2026, 1, 2), ticker=ticker
    )
    master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(instrument_id, capture_start, False)
    path = spans_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_retype(spans.to_table(), casts), path)
    return path


def test_a_drifted_master_costs_the_clamp_and_nothing_else(root: Path):
    # A reference table must never break a panel. This file is valid Parquet carrying
    # the pinned column names, so the read itself succeeds and the drift lands later: a
    # retyped ``schema_version`` is refused by the master's own reader. That costs this
    # ticker its clamp and nothing more, so both panels still serve.
    casts = dict.fromkeys(MASTER_SCHEMA.names, pa.string())
    write_retyped_master(root, "SPY", et(MONDAY, 9, 36), casts)
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] is None
    assert chains["counts"]["out_of_scope"] == 0
    # 09:32 carried a gap marker. With no clamp it stays a gap, and the day's real
    # cycles still render.
    assert chains["counts"]["gap"] == 1
    assert chains["counts"]["captured"] == 2
    row = next(
        row
        for row in service_over(root).run_query("now", {})["surfaces"]
        if (row["ticker"], row["surface"]) == ("SPY", "chains")
    )
    assert row["capture_start"] is None
    assert row["in_scope"] is True
    assert row["last_data_snap_ts"] == et(MONDAY, 9, 33).isoformat()


def test_a_drifted_valid_from_no_longer_costs_the_clamp(root: Path):
    # This used to sit in the parametrized case above, because ``resolve`` compared
    # ``valid_from`` against the queried day and a string raised out of that comparison.
    # Marketlake #405 took the date out of the clamp, so ``valid_from`` is a column the
    # clamp never reads and its type cannot reach anything. The drift is real and the
    # panel answers as if the file were clean, which is the better of the two outcomes:
    # a column nothing consults cannot cost a ticker its scope.
    write_retyped_master(root, "SPY", et(MONDAY, 9, 36), {"valid_from": pa.string()})
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert chains["counts"]["out_of_scope"] == 3
    assert chains["counts"]["gap"] == 0


def test_the_same_helper_with_nothing_retyped_still_clamps(root: Path):
    # The control for the drifted master cases above. The helper writes a master the
    # panel can use, so their verdict of no clamp is the drift talking and not a broken
    # fixture.
    write_retyped_master(root, "SPY", et(MONDAY, 9, 36), {})
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert chains["counts"]["out_of_scope"] == 3
    assert chains["counts"]["gap"] == 0


@pytest.mark.parametrize(
    "casts",
    [
        pytest.param(dict.fromkeys(SPANS_SCHEMA.names, pa.string()), id="all"),
        pytest.param({"span_start": pa.string()}, id="span_start_string"),
        pytest.param({"span_start": pa.timestamp("us")}, id="span_start_naive"),
    ],
)
def test_a_drifted_spans_file_costs_the_clamp_and_nothing_else(root: Path, casts: dict):
    # The spans-file mirror of the master drift test above. Two mechanisms carry it.
    #
    # 1. A retyped ``schema_version`` is refused by the spans reader itself, the same
    #    way the master's is.
    # 2. A retyped ``span_start`` is unfit to compare instants against, dropped by
    #    ``_valid_span``.
    #
    # Either costs the ticker its clamp and nothing more.
    write_retyped_spans(root, "SPY", et(MONDAY, 9, 36), casts)
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] is None
    assert chains["counts"]["out_of_scope"] == 0
    assert chains["counts"]["gap"] == 1
    assert chains["counts"]["captured"] == 2


def test_the_spans_helper_with_nothing_retyped_still_clamps(root: Path):
    # The control for the drifted spans cases above.
    write_retyped_spans(root, "SPY", et(MONDAY, 9, 36), {})
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert chains["counts"]["out_of_scope"] == 3
    assert chains["counts"]["gap"] == 0


# -- retirement and rejoin: the back edge of scope ----------------------------


def test_a_slot_after_retirement_reads_out_of_scope(root: Path):
    # The dashboard's half of #77 case 1: a retired ticker's minutes after its close
    # render out of scope, not missing, and its earlier data still renders captured.
    write_closed_span(root, "SPY", et(MONDAY, 9, 36), et(MONDAY, 16, 2))
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    # Every slot from 16:02 on is out of scope, not missing.
    after = [c for c in chains["slots"] if c["slot"] > et(MONDAY, 16, 2).isoformat()]
    assert after and all(c["status"] == "out_of_scope" for c in after)


def test_the_now_panel_reads_in_scope_false_once_the_span_closed(root: Path):
    # The Now panel's half of the same case, checked at an instant after retirement
    # rather than the fixture's fixed morning clock.
    write_closed_span(root, "SPY", et(MONDAY, 9, 36), et(MONDAY, 9, 40))
    row = next(
        row
        for row in service_over(root).run_query("now", {})["surfaces"]
        if (row["ticker"], row["surface"]) == ("SPY", "chains")
    )
    # NOW is 09:40:30, four minutes after the 09:36 start and past the 09:40 retirement.
    assert row["in_scope"] is False


def test_a_rejoined_ticker_renders_out_of_scope_only_for_the_time_away(root: Path):
    # The #77 case-2 rejoin case, from the dashboard's side: a returning ticker's away
    # days render out of scope, not missing, and both live periods render normally.
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY, capture_start=et(MONDAY, 9, 36), valid_from=date(2026, 1, 2), ticker="SPY"
    )
    master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(instrument_id, et(MONDAY, 9, 36), False)
    spans.close_span(instrument_id, et(MONDAY, 12, 0))
    spans.open_span(instrument_id, et(MONDAY, 15, 0), False)
    spans.write(spans_path(root))

    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    away = [
        c
        for c in chains["slots"]
        if et(MONDAY, 12, 0).isoformat() <= c["slot"] < et(MONDAY, 15, 0).isoformat()
    ]
    assert away and all(c["status"] == "out_of_scope" for c in away)
    back = [c for c in chains["slots"] if c["slot"] >= et(MONDAY, 15, 0).isoformat()]
    assert back and all(c["status"] != "out_of_scope" for c in back)


# -- the injected guard constants --------------------------------------------


def test_the_stale_threshold_comes_from_the_injected_guards(root: Path):
    # The page colours a row stale at the threshold the watchdog pages at. That threshold
    # is the machine's own, which slice 1 recalibrates away from the pinned default.
    recalibrated = GuardConstants(watchdog_page_minutes=7)
    assert GuardConstants().watchdog_page_minutes != recalibrated.watchdog_page_minutes
    assert service_over(root, guards=recalibrated).run_query("now", {})["stale_after_minutes"] == 7
    assert (
        service_over(root).run_query("now", {})["stale_after_minutes"]
        == GuardConstants().watchdog_page_minutes
    )


def test_every_strip_is_denominated_by_the_full_status_vocabulary(service: DashboardService):
    # ``counts`` is the strip's denominator. Every status the module names gets a key,
    # and the keys add up to the session's slot count. A status the payload forgets is a
    # cell counted nowhere.
    today = service.run_query("today", {})
    for strip in today["strips"]:
        assert set(strip["counts"]) == set(dashboard.STATUSES)
        assert sum(strip["counts"].values()) == today["slot_count"]
        assert len(strip["slots"]) == today["slot_count"]


# -- history -----------------------------------------------------------------


def write_master_valid_from(
    root: Path, ticker: str, capture_start: datetime, valid_from: date
) -> Path:
    """A master whose ticker mapping only becomes valid on ``valid_from``, plus its span.

    ``write_master`` dates its mapping from the start of the year, so every day in the
    window resolves. This one reproduces the live lake, where the mapping begins on the
    onboarding day and ``master.resolve`` answers ``None`` for every day before it.
    """
    master = SecurityMaster()
    instrument_id = master.register(
        kind=KIND_EQUITY, capture_start=capture_start, valid_from=valid_from, ticker=ticker
    )
    path = master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(instrument_id, capture_start, False)
    spans.write(spans_path(root))
    return path


def _cell(payload: dict, ticker: str, surface: str, day: date) -> dict:
    """One heatmap cell out of the panel's flat cell list."""
    for cell in payload["cells"]:
        if (cell["ticker"], cell["surface"], cell["date"]) == (ticker, surface, day.isoformat()):
            return cell
    raise AssertionError(f"no cell for {ticker} {surface} {day}")


def _file_nightly(root: Path, day: date, **fields) -> Path:
    """One nightly report file written by the production writer, never by hand."""
    nightly = Nightly(
        day=day,
        session=fields.pop("session", True),
        pinged=fields.pop("pinged", True),
        **fields,
    )
    return write_nightly(root, nightly, now=et(day, 18, 30).astimezone(UTC), pid=11)


def test_the_window_is_thirty_calendar_days_and_holds_only_its_sessions(
    service: DashboardService,
):
    payload = service.run_query("history", {})
    assert payload["window_end"] == MONDAY.isoformat()
    assert payload["window_days"] == HISTORY_WINDOW_DAYS
    start = date.fromisoformat(payload["window_start"])
    assert MONDAY - start == timedelta(days=HISTORY_WINDOW_DAYS - 1)
    # The calendar decides how many of the thirty days are sessions, and the fake one
    # has three inside the window. The Saturday between Friday and Monday is not one.
    assert payload["sessions"] == [d.isoformat() for d in (THURSDAY, FRIDAY, MONDAY)]
    assert SATURDAY.isoformat() not in payload["sessions"]
    # One cell per session per strip the Today panel shows. The roster is the lake's own
    # tree, so QQQ carries chains alone here and the count is nine rather than twelve.
    strips = service.run_query("today", {})["strips"]
    assert len(payload["cells"]) == len(payload["sessions"]) * len(strips) == 9


def test_a_cell_carries_the_slot_counts_and_not_only_a_percent(service: DashboardService):
    # Monday holds two captured minutes, one suspect and one gap, and the rest of the
    # session has no row at all. A percent alone could not say which of those it was.
    cell = _cell(service.run_query("history", {}), "SPY", "chains", MONDAY)
    assert cell["counts"]["captured"] == 2
    assert cell["counts"]["suspect"] == 1
    assert cell["counts"]["gap"] == 1
    assert cell["counts"]["pending"] > 0
    assert sum(cell["counts"].values()) == cell["slot_count"]


def test_the_percent_is_over_the_minutes_judged_not_the_session_length(
    service: DashboardService,
):
    cell = _cell(service.run_query("history", {}), "SPY", "chains", MONDAY)
    counts = cell["counts"]
    assert cell["judged"] == cell["slot_count"] - counts["out_of_scope"] - counts["pending"]
    # A landed cycle is a landed cycle, so suspect counts toward the numerator beside
    # captured. What it is suspected of is the battery's question, not the heatmap's.
    landed = counts["captured"] + counts["suspect"]
    assert cell["captured_pct"] == round(100.0 * landed / cell["judged"], 1)


def test_a_day_that_owed_nothing_carries_no_percent_rather_than_zero(root: Path):
    # Thursday and Friday sit before this ticker's capture span, so nothing was owed on
    # either. Zero there would read as "captured nothing" beside a day that really did.
    write_master(root, "SPY", et(MONDAY, 9, 30))
    payload = service_over(root).run_query("history", {})
    for day in (THURSDAY, FRIDAY):
        cell = _cell(payload, "SPY", "chains", day)
        assert cell["counts"]["out_of_scope"] == cell["slot_count"]
        assert cell["judged"] == 0
        assert cell["captured_pct"] is None


def test_a_day_before_the_master_maps_the_ticker_is_out_of_scope_never_missing(root: Path):
    # The clamp asks the master which instruments this spelling has ever named, never
    # which one it named on the queried day. Asked as of Thursday or Friday this master
    # answers ``None``, the ticker gets no clamp at all, and every one of those minutes
    # renders ``missing``. That is marketlake #405, and dropping the date is the fix.
    write_master_valid_from(root, "SPY", et(MONDAY, 9, 30), valid_from=MONDAY)
    payload = service_over(root).run_query("history", {})
    for day in (THURSDAY, FRIDAY):
        cell = _cell(payload, "SPY", "chains", day)
        assert cell["counts"]["missing"] == 0
        assert cell["counts"]["out_of_scope"] == cell["slot_count"]


def test_a_captured_minute_outranks_the_scope_clamp(root: Path):
    # The ladder puts data above the clamp so a real cycle is never hidden. QQQ's
    # Thursday partition holds one minute at the option close, months before the span
    # this master opens, and it still reads captured with the day out of scope round it.
    write_master_valid_from(root, "QQQ", et(MONDAY, 9, 30), valid_from=MONDAY)
    cell = _cell(service_over(root).run_query("history", {}), "QQQ", "chains", THURSDAY)
    assert cell["counts"]["captured"] == 1
    assert cell["counts"]["out_of_scope"] == cell["slot_count"] - 1
    assert cell["counts"]["missing"] == 0
    assert cell["judged"] == 1
    assert cell["captured_pct"] == 100.0


def test_a_sealed_day_and_an_unsealed_day_land_in_the_same_cell_shape(
    service: DashboardService, root: Path
):
    # QQQ's Thursday is a sealed partition with no journal segment, so it reads through
    # the bulk window query. SPY's Monday is journal segments with no partition, so it
    # reads through ``_slot_aggregates``. Neither path may invent a different cell.
    assert not dashboard.LakePaths(root).partition_path("chains", "SPY", MONDAY).is_file()
    payload = service.run_query("history", {})
    sealed = _cell(payload, "QQQ", "chains", THURSDAY)
    unsealed = _cell(payload, "SPY", "chains", MONDAY)
    assert sealed["counts"]["captured"] == 1
    assert unsealed["counts"]["captured"] == 2
    assert set(sealed) == set(unsealed)


def test_the_unsealed_day_still_counts_its_segment_health(service: DashboardService):
    # ``build_lake`` plants one file that is not an Arrow stream beside Monday's real
    # segments. The heatmap carries the same health counters the strip does, so a day
    # whose rows partly would not read is not quietly shown as merely incomplete.
    cell = _cell(service.run_query("history", {}), "SPY", "chains", MONDAY)
    assert cell["unreadable_segments"] == 1


def test_an_unreadable_partition_degrades_to_the_per_day_read(root: Path):
    # The bulk read is all-or-nothing, so one file of unreadable bytes takes every
    # ticker-day on the surface with it. The fallback reads each of them the per-day way.
    dashboard.LakePaths(root).partition_path("chains", "QQQ", THURSDAY).write_bytes(
        b"not parquet at all"
    )
    payload = service_over(root).run_query("history", {})
    broken = _cell(payload, "QQQ", "chains", THURSDAY)
    healthy = _cell(payload, "QQQ", "chains", FRIDAY)
    # The broken ticker-day holds exactly one partition, so it reports exactly one.
    # ``_slot_aggregates`` counts it on the cell that holds it, and the fallback adds no
    # count of its own. A count added there would blame the healthy days for this file
    # and count this one twice.
    assert broken["unreadable_partitions"] == 1
    assert broken["counts"]["captured"] == 0
    # The healthy ticker-day's rows arrive by the per-day path, and it is blamed for
    # nothing. Without this the fallback could report a loss on every cell it touched.
    assert healthy["unreadable_partitions"] == 0
    assert healthy["counts"]["captured"] == 1
    assert len(payload["cells"]) == 9


def test_a_healthy_window_reads_its_sealed_days_in_one_query(root: Path, monkeypatch):
    # The sealed partitions are read in one query per surface, which is what keeps the
    # window's cost growing with bytes rather than with days. Only a ticker-day holding
    # journal segments takes the per-day reader. Without this the guard that checks the
    # partition is there could be dropped, every absent ticker-day would reach
    # ``read_parquet``, the read would raise, and the whole surface would silently fall
    # back to reading every day one at a time while still rendering correctly.
    per_day: list[tuple[str, str, date]] = []
    real = dashboard._slot_aggregates

    def counted(con, paths, surface, ticker, day):
        per_day.append((surface, ticker, day))
        return real(con, paths, surface, ticker, day)

    monkeypatch.setattr(dashboard, "_slot_aggregates", counted)
    payload = service_over(root).run_query("history", {})
    # The fixture's journal days, and nothing else. QQQ's sealed Thursday and Friday are
    # not in this list, because the bulk query answered them.
    assert sorted(per_day) == sorted(
        [("chains", "QQQ", MONDAY), ("chains", "SPY", MONDAY), ("quotes", "SPY", MONDAY)]
    )
    for cell in payload["cells"]:
        assert cell["unreadable_partitions"] == 0


def test_a_column_no_partition_carries_does_not_raise_out_of_the_panel(root: Path):
    # The bulk read unions every partition in the window by name and has no journal view
    # beside it, so what it can bind depends on the data. A column that no partition in
    # the union carries raises ``BinderException``, which is not a partition-read error.
    # It must not escape: ``_serve`` turns an escape into a 500, and a 500 throws away
    # every finding this payload was going to carry, the counts and the reports included.
    #
    # Every partition in the surface's bulk set loses the column, because ``union_by_name``
    # fills it from any sibling that still has it and the error would not fire.
    paths = dashboard.LakePaths(root)
    for day in (THURSDAY, FRIDAY):
        partition = paths.partition_path("chains", "QQQ", day)
        table = pq.read_table(partition)
        pq.write_table(table.drop_columns(["row_kind"]), partition)
    service = service_over(root)
    payload = service.run_query("history", {})
    assert len(payload["cells"]) == 9
    # The per-day fallback registers the journal view and its pinned schema, so a
    # ticker-day whose own rows are readable still renders them.
    assert _cell(payload, "SPY", "chains", MONDAY)["counts"]["captured"] == 2
    # And the panels beside it are untouched, which is the whole point of containing it.
    assert service.run_query("now", {})["surfaces"]
    assert service.run_query("today", {})["strips"]


# -- history: the quarantine ledger ------------------------------------------


def test_an_open_quarantine_is_listed_with_its_verdict(fixture_lake: FixtureLake):
    build_lake(fixture_lake)
    fixture_lake.with_quarantine(
        {"partition": "chains/ticker=SPY/date=2026-08-24.parquet", "verdict": "held"}
    )
    fixture_lake.with_quarantine(
        {"partition": "quotes/ticker=SPY/date=2026-08-24.parquet", "verdict": "clean"}
    )
    payload = service_over(fixture_lake.build()).run_query("history", {})
    assert payload["quarantine_count"] == 1
    # ``checks`` carries a null for an entry with no ``check``, which the page renders the
    # way it renders a null verdict. Every entry ``battery.build_entry`` assembles has one.
    assert payload["quarantines"] == [
        {
            "partition": "chains/ticker=SPY/date=2026-08-24.parquet",
            "verdict": "held",
            "checks": [{"check": None, "verdict": "held"}],
        }
    ]
    assert payload["quarantine_unreadable"] is None
    # The sign-off runs ``lake.signoff``, shipped by marketlake #139. The panel still
    # prints no command, and marketlake #445 carries it.
    assert set(payload["quarantines"][0]) == {"partition", "verdict", "checks"}


def test_a_partition_withheld_by_two_checks_names_both(fixture_lake: FixtureLake):
    """Signing one off leaves the other standing, so a row naming one is a row that misleads.

    It is also how a stranded token shows. A check that quarantined and then stopped judging
    holds its partition until a human signs that token off, and the token is the only thing
    on the page that says which one.
    """
    build_lake(fixture_lake)
    partition = "chains/ticker=SPY/date=2026-08-24.parquet"
    for check in ("row_count_band", "realtime_entitlement"):
        fixture_lake.with_quarantine(
            {"partition": partition, "verdict": "quarantined", "check": check}
        )

    payload = service_over(fixture_lake.build()).run_query("history", {})

    assert payload["quarantine_count"] == 1
    assert [c["check"] for c in payload["quarantines"][0]["checks"]] == [
        "row_count_band",
        "realtime_entitlement",
    ]


def test_each_withholding_check_carries_its_own_verdict(fixture_lake: FixtureLake):
    """One verdict beside two check names says the wrong thing about one of them.

    Two checks withhold under two different spellings. A row carrying only the deciding
    entry's verdict reads as though both checks found the same fault, which is the row that
    misleads an operator into a sign-off that changes nothing they can see.
    """
    build_lake(fixture_lake)
    partition = "chains/ticker=SPY/date=2026-08-24.parquet"
    for check, verdict in (
        ("realtime_entitlement", "delayed_feed"),
        ("row_count_band", "row_count_low"),
    ):
        fixture_lake.with_quarantine({"partition": partition, "verdict": verdict, "check": check})

    payload = service_over(fixture_lake.build()).run_query("history", {})

    assert payload["quarantines"][0]["checks"] == [
        {"check": "realtime_entitlement", "verdict": "delayed_feed"},
        {"check": "row_count_band", "verdict": "row_count_low"},
    ]
    # The row's own verdict is the deciding entry's, which is the first still withholding.
    # With two holders under two spellings, taking the last one instead is visible here.
    assert payload["quarantines"][0]["verdict"] == "delayed_feed"


def test_an_entry_naming_no_check_is_carried_rather_than_stringified(
    fixture_lake: FixtureLake,
):
    """The page shows a missing check the way it shows a missing verdict, not as "None"."""
    build_lake(fixture_lake)
    fixture_lake.with_quarantine(
        {"partition": "chains/ticker=SPY/date=2026-08-24.parquet", "verdict": "stale"}
    )

    payload = service_over(fixture_lake.build()).run_query("history", {})

    assert payload["quarantines"][0]["checks"] == [{"check": None, "verdict": "stale"}]


def test_a_check_that_cleared_drops_out_of_the_row_while_the_other_holds(
    fixture_lake: FixtureLake,
):
    """The panel reads each check's current verdict, not the partition's last line."""
    build_lake(fixture_lake)
    partition = "chains/ticker=SPY/date=2026-08-24.parquet"
    for verdict, check in (
        ("quarantined", "row_count_band"),
        ("quarantined", "realtime_entitlement"),
        ("clean", "realtime_entitlement"),
    ):
        fixture_lake.with_quarantine({"partition": partition, "verdict": verdict, "check": check})

    payload = service_over(fixture_lake.build()).run_query("history", {})

    assert payload["quarantine_count"] == 1, "the last line was clean, the partition is not"
    assert payload["quarantines"][0]["checks"] == [
        {"check": "row_count_band", "verdict": "quarantined"}
    ]


def test_a_damaged_quarantine_ledger_is_reported_and_never_raised(fixture_lake: FixtureLake):
    # ``_latest_by_partition`` raises on a body line naming no partition, and names the
    # only two callers allowed to survive it: the close+5 guard's prologue and the
    # marking pass. This is neither, and ``_serve`` would turn the raise into a 500 for
    # the whole panel, so it is caught at this boundary and reported as a value.
    build_lake(fixture_lake)
    fixture_lake.with_quarantine({"note": "a line that parses and names no partition"})
    payload = service_over(fixture_lake.build()).run_query("history", {})
    assert payload["quarantine_unreadable"] == "ManifestError"
    assert payload["quarantines"] == []
    # Everything the ledger has nothing to do with is still served.
    assert len(payload["cells"]) == 9


def test_an_absent_quarantine_ledger_reads_as_no_entries(service: DashboardService, root: Path):
    assert not (root / "quarantine.jsonl").exists()
    payload = service.run_query("history", {})
    assert payload["quarantines"] == []
    assert payload["quarantine_unreadable"] is None


# -- history: the nightly reports --------------------------------------------


def test_no_reports_directory_renders_an_empty_list(service: DashboardService, root: Path):
    # Which is the live lake's answer today: the first ``eod-sweep`` run has not
    # happened, so ``reports/*.json`` matches nothing.
    assert not (root / "reports").exists()
    payload = service.run_query("history", {})
    assert payload["reports"] == []
    assert payload["reports_unreadable"] == 0
    assert payload["reports_error"] is None


def test_the_reports_come_back_newest_first_and_each_carries_its_own_date(root: Path):
    for day in (THURSDAY, FRIDAY, MONDAY):
        _file_nightly(root, day)
    payload = service_over(root).run_query("history", {})
    assert [entry["day"] for entry in payload["reports"]] == [
        MONDAY.isoformat(),
        FRIDAY.isoformat(),
        THURSDAY.isoformat(),
    ]
    # The design pins the report as the one pre-written thing the dashboard shows, "and
    # it is dated, so a stale one never reads as now". Both stamps ride along.
    assert all(entry["at"] for entry in payload["reports"])


def test_only_the_last_nights_are_read(root: Path):
    for index in range(HISTORY_REPORTS + 4):
        _file_nightly(root, THURSDAY + timedelta(days=index))
    payload = service_over(root).run_query("history", {})
    assert len(payload["reports"]) == HISTORY_REPORTS
    # The newest, not the first ten written. ``nightly_path`` puts the day first in the
    # name, so a name sort is a date sort and no file past the bound is opened.
    newest = THURSDAY + timedelta(days=HISTORY_REPORTS + 3)
    assert payload["reports"][0]["day"] == newest.isoformat()


def test_a_report_missing_a_key_renders_it_as_no_number(root: Path):
    # The live lake's report tree already carries two key sets from one writer at two
    # versions, so a reader spelling a key outright breaks on the older file.
    directory = root / "reports"
    directory.mkdir()
    (directory / f"{MONDAY.isoformat()}-183000000000-11.json").write_text(
        json.dumps({"day": MONDAY.isoformat(), "session": True})
    )
    entry = service_over(root).run_query("history", {})["reports"][0]
    assert entry["day"] == MONDAY.isoformat()
    for field in ("gaps", "quarantined", "disagreements", "pages_lost", "at", "pinged"):
        assert entry[field] is None
    assert entry["problems"] == []
    assert entry["report"] == []


def test_an_unreadable_report_is_counted_and_the_others_still_render(root: Path):
    _file_nightly(root, MONDAY)
    (root / "reports" / f"{FRIDAY.isoformat()}-183000000000-12.json").write_text("{not json")
    payload = service_over(root).run_query("history", {})
    assert payload["reports_unreadable"] == 1
    assert [entry["day"] for entry in payload["reports"]] == [MONDAY.isoformat()]
    assert payload["reports_error"] is None


def test_the_report_tier_findings_ride_the_panel(root: Path):
    # ``report`` is why this panel reads these files: the findings that send no message
    # of their own, which the design names as the disk runway, pmset drift and a
    # suspected unscheduled closure. The panel renders the lines and computes none.
    _file_nightly(
        root, MONDAY, report=("pmset repeat drifted",), problems=("ping failed: OSError",)
    )
    entry = service_over(root).run_query("history", {})["reports"][0]
    assert entry["report"] == ["pmset repeat drifted"]
    assert entry["problems"] == ["ping failed: OSError"]


def test_unfiled_is_summed_off_the_pieces_because_the_file_does_not_carry_it(root: Path):
    # ``Nightly.unfiled`` is a property and ``write_nightly`` writes eleven keys without
    # it, so a reader spelling the key would always read nothing at all.
    path = _file_nightly(
        root,
        MONDAY,
        pieces=(
            ("dividends", PieceOutcome(held=2, unfiled=1)),
            ("splits", PieceOutcome(held=1, unfiled=2, refusal="OSError: denied")),
        ),
    )
    assert "unfiled" not in json.loads(path.read_text())
    entry = service_over(root).run_query("history", {})["reports"][0]
    assert entry["unfiled"] == 3
    assert entry["disagreements"] == 3
    # A walk that stopped says so, with its exception message already dropped.
    assert entry["pieces"] == {"dividends": None, "splits": "OSError"}


def test_the_reports_glob_skips_the_other_producers_subdirectories(root: Path):
    # ``report.py`` chose the naming for exactly one reader: "a reader globbing
    # ``reports/*.json`` picks up the nightly files and nothing else".
    _file_nightly(root, MONDAY)
    guard = root / "reports" / "close_guard" / f"{DATE_PREFIX}{MONDAY.isoformat()}"
    guard.mkdir(parents=True)
    (guard / "162000000000-11.json").write_text(json.dumps({"day": MONDAY.isoformat()}))
    payload = service_over(root).run_query("history", {})
    assert len(payload["reports"]) == 1
    assert payload["reports_unreadable"] == 0


def test_a_cell_judges_a_minute_only_once_its_grace_has_run_out(root: Path):
    # The heatmap reads the same verdict grace the strip does, against the injected
    # instant. A slot still inside it is pending, so a cycle that has not finished
    # running is never counted as an absent minute, and the percent's denominator moves
    # with that boundary rather than sitting still.
    #
    # At 09:40:30 the last judged slot is 09:38, which leaves 09:34 through 09:38
    # missing. One minute later 09:39 has aged past the grace and joins them.
    at_now = _cell(service_over(root).run_query("history", {}), "SPY", "chains", MONDAY)
    assert at_now["counts"]["missing"] == 5
    assert at_now["counts"]["pending"] == 397
    assert at_now["judged"] == 9
    later = service_over(root, now=et(MONDAY, 9, 41, 30)).run_query("history", {})
    minute_on = _cell(later, "SPY", "chains", MONDAY)
    assert minute_on["counts"]["missing"] == 6
    assert minute_on["counts"]["pending"] == 396
    assert minute_on["judged"] == 10
    # The numerator did not move, so the percent fell because one more minute came due.
    assert at_now["captured_pct"] == 33.3
    assert minute_on["captured_pct"] == 30.0


def test_the_window_and_report_bounds_are_the_pinned_values(root: Path):
    # The tests around these derive their expectations from the constants, so every one
    # of them holds for any value. These are the literals themselves.
    assert HISTORY_WINDOW_DAYS == 30
    assert HISTORY_REPORTS == 10
    assert dashboard.HISTORY_REPORT_MAX_BYTES == 4 * 1024 * 1024


def test_the_windows_first_calendar_day_is_inside_it(root: Path):
    # The window is thirty calendar days with its end inside, so it opens on the
    # twenty-ninth day back. The day before that is out. Nothing else pins the scan's
    # start: `window_start` is computed separately and would still agree if the walk
    # began a day late.
    first = MONDAY - timedelta(days=HISTORY_WINDOW_DAYS - 1)
    before = first - timedelta(days=1)
    calendar = FakeCalendar(
        {
            day: SessionTimes(open=et(day, 9, 30), close=et(day, 16, 0))
            for day in (before, first, MONDAY)
        }
    )
    service = DashboardService(
        root, clock=ManualClock(NOW.astimezone(UTC)), calendar=calendar, page=b"", icon=b""
    )
    payload = service.run_query("history", {})
    assert payload["window_start"] == first.isoformat()
    assert first.isoformat() in payload["sessions"]
    assert before.isoformat() not in payload["sessions"]


def test_a_sealed_partition_reports_its_own_drifted_and_unplaceable_rows(
    fixture_lake: FixtureLake,
):
    # The journal path counts these and the sealed path must too. A row of an
    # unrecognized kind renders the slot missing rather than an invented gap, and a row
    # whose stamp will not cast names no minute at all. Both are counted rather than
    # disappearing quietly, which is what `SegmentHealth` exists for.
    build_lake(fixture_lake)
    fixture_lake.with_chains(
        "QQQ",
        THURSDAY,
        sample_chains_table(
            [
                _fixture_row("QQQ", et(THURSDAY, 9, 30)),
                {**_fixture_row("QQQ", et(THURSDAY, 9, 31)), "row_kind": "something_else"},
                {**_fixture_row("QQQ", et(THURSDAY, 9, 32)), "snap_ts": "not a timestamp"},
            ]
        ),
    )
    cell = _cell(
        service_over(fixture_lake.build()).run_query("history", {}), "QQQ", "chains", THURSDAY
    )
    assert cell["drifted_rows"] == 1
    assert cell["unparseable_stamp_rows"] == 1
    assert cell["counts"]["captured"] == 1
    # The drifted row's minute holds no bound row, so it reads missing, never a gap.
    assert cell["counts"]["gap"] == 0


def test_an_unlistable_reports_directory_is_reported_and_never_raised(root: Path):
    # The same rule the quarantine ledger follows. `_serve` would turn an escape into a
    # 500, and on a page whose other panels are live that is the worst answer available.
    directory = root / "reports"
    directory.mkdir()
    _file_nightly(root, MONDAY)
    directory.chmod(0o000)
    try:
        payload = service_over(root).run_query("history", {})
    finally:
        directory.chmod(0o755)
    assert payload["reports_error"] == "PermissionError"
    assert payload["reports"] == []
    # Everything the directory has nothing to do with is still served.
    assert len(payload["cells"]) == 9


def test_a_report_that_is_valid_json_but_not_an_object_is_counted(root: Path):
    # `json.loads` succeeds on a bare list or string, so the parse guard cannot catch
    # these. They are records no reader can interpret, and they are counted rather than
    # reaching `_nightly_payload`, which would raise on them.
    directory = root / "reports"
    directory.mkdir()
    (directory / f"{MONDAY.isoformat()}-183000000000-11.json").write_text("[]")
    (directory / f"{FRIDAY.isoformat()}-183000000000-12.json").write_text('"a string"')
    payload = service_over(root).run_query("history", {})
    assert payload["reports_unreadable"] == 2
    assert payload["reports"] == []


def test_a_report_over_the_size_bound_is_counted_and_never_opened(root: Path):
    # The count bound says how many files are opened and this says how much is read,
    # which is a different promise. The read is filesystem I/O, so the connection's own
    # memory limit does not reach it, and the service runs under `KeepAlive`, so one
    # oversized file's cost would sit in the process for as long as it lives.
    _file_nightly(root, MONDAY)
    oversized = root / "reports" / f"{FRIDAY.isoformat()}-183000000000-12.json"
    oversized.write_text(
        json.dumps(
            {"day": FRIDAY.isoformat(), "problems": ["x" * dashboard.HISTORY_REPORT_MAX_BYTES]}
        )
    )
    assert oversized.stat().st_size > dashboard.HISTORY_REPORT_MAX_BYTES
    payload = service_over(root).run_query("history", {})
    assert payload["reports_unreadable"] == 1
    assert [entry["day"] for entry in payload["reports"]] == [MONDAY.isoformat()]


def test_a_report_whose_fields_carry_the_wrong_types_renders_rather_than_raising(root: Path):
    # The lake's report tree already carries two key sets from one writer at two
    # versions, so the reader takes every field defensively. Each guard is exercised
    # here: a non-integer `unfiled`, a `pieces` value that is not a mapping, and a
    # `problems` list holding something that is not a string.
    directory = root / "reports"
    directory.mkdir()
    (directory / f"{MONDAY.isoformat()}-183000000000-11.json").write_text(
        json.dumps(
            {
                "day": MONDAY.isoformat(),
                "pieces": {"dividends": {"unfiled": "two"}, "splits": "not a mapping"},
                "problems": ["a real line", 7, None],
                "report": "not a list",
            }
        )
    )
    entry = service_over(root).run_query("history", {})["reports"][0]
    assert entry["unfiled"] == 0
    assert entry["pieces"] == {"dividends": None}
    assert entry["problems"] == ["a real line"]
    assert entry["report"] == []


def test_the_panel_names_its_tickers_and_orders_the_ledger(fixture_lake: FixtureLake):
    build_lake(fixture_lake)
    for day in (MONDAY, THURSDAY, FRIDAY):
        fixture_lake.with_quarantine(
            {"partition": f"chains/ticker=SPY/date={day.isoformat()}.parquet", "verdict": "held"}
        )
    payload = service_over(fixture_lake.build()).run_query("history", {})
    assert payload["tickers"] == ["QQQ", "SPY"]
    # Sorted, so the list renders the same way on every request rather than in whatever
    # order the ledger's entries happened to land.
    assert [entry["partition"] for entry in payload["quarantines"]] == [
        f"chains/ticker=SPY/date={day.isoformat()}.parquet" for day in (THURSDAY, FRIDAY, MONDAY)
    ]


# -- the Lake panel ----------------------------------------------------------


def test_the_lake_panel_reports_every_entry_and_names_the_surfaces_never_written(root: Path):
    # Four surfaces in three layouts plus the root ledgers, and the fixture lake holds two
    # of the surfaces. ``actions`` and ``bars`` have never been written to it, and the
    # panel says so rather than leaving them off a list, because absence is the ordinary
    # case here: ``bars/`` did not exist on the live lake the day this panel was specified
    # and ``actions/`` still does not.
    payload = service_over(root).run_query("lake", {})
    assert payload["error"] is None
    names = [entry["name"] for entry in payload["entries"]]
    assert "chains" in names
    assert set(payload["absent_surfaces"]) >= {"actions", "bars"}
    assert all(name not in names for name in payload["absent_surfaces"])
    # Biggest first, because the question the panel answers is what is filling the disk.
    sizes = [entry["bytes"] for entry in payload["entries"]]
    assert sizes == sorted(sizes, reverse=True)
    assert payload["lake_bytes"] == payload["dated_bytes"] + payload["undated_bytes"]


def test_the_lake_panel_takes_no_parameter(root: Path):
    # Rule 2 allows a request a ticker and a date, and ``validate_parameters`` builds
    # exactly those two keyword arguments, so a third declared name would pass the
    # unknown-field check and be dropped on the floor. This panel is about the whole lake,
    # so there is nothing a request could narrow anyway.
    with pytest.raises(dashboard.QueryParameterError):
        service_over(root).run_query("lake", {"ticker": "SPY"})
    with pytest.raises(dashboard.QueryParameterError):
        service_over(root).run_query("lake", {"date": "2026-08-24"})


def test_the_lake_panel_reports_a_refused_read_rather_than_raising(root: Path):
    # ``_serve`` turns any escape into a 500, and a 500 says only "query failed". This
    # payload's whole job on a bad day is naming what would not read, so a raise would
    # throw away the one thing worth having.
    # ``reports/`` rather than a surface or the journal, because ``lake_roster`` walks
    # those two on every request and ``_children`` does not contain a ``PermissionError``,
    # so an unreadable one takes down all four panels before this one is reached. That is
    # marketlake #449 and not this panel's to fix.
    locked = root / "reports"
    locked.mkdir(exist_ok=True)
    (locked / "a.json").write_text("{}")
    locked.chmod(0o000)
    try:
        payload = service_over(root).run_query("lake", {})
    finally:
        locked.chmod(0o755)
    assert payload["error"] is None
    assert payload["refused"] >= 1
    assert payload["refusals"]
    # And the rest of the lake still reports, rather than the whole panel failing.
    assert payload["entries"]


def test_a_lake_root_that_is_not_there_reports_a_refusal_and_not_an_empty_lake(tmp_path: Path):
    # An absent ``reports/`` is a true zero: no nightly run filed anything. An absent root
    # is a panel pointed at nothing, and reporting it as an empty lake would say the disk
    # is fine when nothing was read at all.
    payload = service_over(tmp_path / "gone").run_query("lake", {})
    assert payload["error"] is None
    assert payload["refused"] >= 1
    assert payload["lake_bytes"] == 0
    # The device read fails too, and it is contained beside the walk rather than
    # discarding it. A payload that dropped the refusal would say only that something went
    # wrong, where the refusal says which path.
    assert payload["space_error"] == "FileNotFoundError"
    assert payload["free"] is None
    assert payload["capture_days_left"] is None


def test_the_lake_panel_runs_no_sql_at_all(root: Path):
    # The sizes are a walk and the free space is a ``shutil.disk_usage`` call, which is the
    # one thing any panel reports from outside ``lake_root``. It could not have gone
    # through DuckDB: the sandbox sets ``allowed_directories`` to exactly the root. A
    # connection that refuses every statement proves the query never reaches one.
    class RefusingCursor:
        def execute(self, *args: object, **kwargs: object) -> object:
            raise AssertionError("the Lake panel must run no SQL")

        def close(self) -> None:
            return None

    class RefusingConnection(RefusingCursor):
        def cursor(self) -> RefusingCursor:
            return RefusingCursor()

    service = DashboardService(
        root,
        clock=ManualClock(NOW.astimezone(UTC)),
        calendar=CALENDAR,
        connection=RefusingConnection(),
        page=b"<!doctype html>",
        icon=b"",
    )
    assert service.run_query("lake", {})["error"] is None


def test_the_growth_rate_is_the_busiest_day_and_the_mean_rides_beside_it(root: Path):
    # The finding the module exists around. The mean is denominated by the days that wrote
    # bytes rather than by the window's width, and the runway is taken off the peak,
    # because every way of understating the rate lengthens the runway and a check that
    # flags short headroom never fires if its rate is too low.
    payload = service_over(root).run_query("lake", {})
    days = payload["days"]
    assert days
    assert payload["peak_bytes"] == max(day["bytes"] for day in days)
    assert payload["capture_days"] == sum(1 for day in days if day["bytes"] > 0)
    assert payload["mean_bytes"] <= payload["peak_bytes"]
    assert payload["capture_days_left"] == payload["free"] // payload["peak_bytes"]


def test_a_journal_day_is_flagged_unsealed_on_the_panel(fixture_lake: FixtureLake):
    # A day's bytes are not stable: mid-session a day is Arrow IPC segments and after
    # close+15 it is one compressed partition. The page explains a growth figure that
    # drops at 16:30 rather than leaving a reader to distrust it.
    root = one_segment_lake(
        fixture_lake, [_chains("SPY", et(MONDAY, 9, 30), occ_symbol="A")], day=MONDAY
    )
    days = {day["day"]: day for day in service_over(root).run_query("lake", {})["days"]}
    assert days[MONDAY.isoformat()]["unsealed"] is True


def test_a_lake_with_no_growth_in_the_window_reports_no_runway(fixture_lake: FixtureLake):
    # A runway goes unbounded exactly when capture has stopped, so a large number there
    # would go quiet at the one moment something is wrong. It is also the division by zero.
    root = fixture_lake.build()
    (root / "manifest.jsonl").write_text("")
    payload = service_over(root).run_query("lake", {})
    assert payload["capture_days_left"] is None
    assert payload["exhausts_on"] is None
    assert payload["mean_bytes"] is None
    assert payload["short"] is False


def test_the_panel_forwards_the_alarm_flag_it_was_given(root: Path, monkeypatch):
    """The unit tests hold ``Runway.short``. Nothing held the panel passing it on.

    ``status.html`` keys the alarm styling off exactly this field, so a wiring slip here
    silences the headroom warning on the one page an operator opens to ask whether the disk
    is filling. The same goes for ``beyond_horizon``, which the page branches on to decide
    whether to print a date at all.
    """
    real = dashboard.assess

    def short_runway(*args: object, **kwargs: object):
        result = real(*args, **kwargs)
        return replace(
            result,
            capture_days_left=1,
            exhausts_on=result.window_end,
            beyond_horizon=False,
        )

    # Unpatched first. The fixture lake's runway outruns the calendar, so this is the
    # ``True`` side of the flag, and asserting only the ``False`` side below would let a
    # literal ``False`` sit in the payload undetected.
    plain = service_over(root).run_query("lake", {})
    assert plain["beyond_horizon"] is True
    assert plain["exhausts_on"] is None
    assert plain["short"] is False

    monkeypatch.setattr(dashboard, "assess", short_runway)
    payload = service_over(root).run_query("lake", {})
    assert payload["short"] is True
    assert payload["beyond_horizon"] is False
    assert payload["exhausts_on"] == payload["window_end"]


def test_the_panel_advertises_the_window_it_actually_measured(root: Path):
    # ``window_days`` is a separate literal from the argument ``assess`` is called with, so
    # the two can drift and the payload would keep claiming thirty days while measuring
    # three. A narrowed window drops the busiest day and lengthens the runway.
    payload = service_over(root).run_query("lake", {})
    start = date.fromisoformat(payload["window_start"])
    end = date.fromisoformat(payload["window_end"])
    assert payload["window_days"] == 30
    assert (end - start).days == payload["window_days"] - 1
    assert all(start <= date.fromisoformat(day["day"]) <= end for day in payload["days"])


def test_the_panel_advertises_the_headroom_threshold_it_flags_at(root: Path):
    # The page prints this number to the reader, so it has to be the one the flag uses.
    payload = service_over(root).run_query("lake", {})
    assert payload["headroom_weeks"] == 3


def test_a_reading_that_raises_reaches_the_reader_as_a_line(root: Path, monkeypatch):
    # The backstop the query's docstring names. ``lake.runway`` contains both of its own
    # readings, so this catches only a class neither expected, and the point of catching it
    # is that a blank page says less than a named class does.
    def boom(*args: object, **kwargs: object):
        raise OSError("the device fell over")

    monkeypatch.setattr(dashboard, "assess", boom)
    payload = service_over(root).run_query("lake", {})
    assert payload["error"] == "OSError"
    assert payload["as_of"]
