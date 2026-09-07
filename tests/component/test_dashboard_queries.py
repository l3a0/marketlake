"""The two named queries over a real fixture lake.

These lay down journal segments in the pinned capture schemas and sealed partitions in
the fixture schema, then run the Now and Today queries against them through the real
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
minutes old, the slots through 09:40 are past, and everything after is pending.

Tests past the shared fixture build their own lake from the same row helpers, because
each one needs a lake in a shape the shared fixture deliberately is not: a drifted
segment, a malformed directory name, a ticker with no data cycle at all, an empty lake.
The ``fixture_lake`` fixture is function-scoped, so a test may also plant files into the
shared root without disturbing any other test.
"""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from fnmatch import fnmatchcase
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import dashboard, journal
from lake.calendar import MARKET_TZ
from lake.config import GuardConstants
from lake.dashboard import (
    MAX_LOOKBACK_SESSIONS,
    DashboardService,
    QueryParameterError,
)
from lake.paths import DATE_PREFIX, JOURNAL_DIR, SEGMENT_GLOB
from lake.security_master import (
    KIND_EQUITY,
    MASTER_SCHEMA,
    SecurityMaster,
    master_path,
)
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
    assert spy_chains["last_error_class"] is None
    # The garbage file is counted, never silently skipped, and never blanks the row.
    assert spy_chains["unreadable_segments"] == 1

    # The quotes' last data cycle lives in the sealed partition, not the journal.
    spy_quotes = rows[("SPY", "quotes")]
    assert spy_quotes["last_data_snap_ts"] == et(MONDAY, 9, 31).isoformat()
    assert spy_quotes["minutes_since"] == 9.5
    assert spy_quotes["last_status"] == "captured"


def test_now_walks_back_past_a_gap_only_day(service: DashboardService):
    now = service.run_query("now", {})
    qqq = next(row for row in now["surfaces"] if row["ticker"] == "QQQ")
    # The latest slot is Monday's gap, with its reason. The last success is Friday's
    # option close: 2 days, 17 hours, 25.5 minutes before the clock's instant.
    assert qqq["last_snap_ts"] == et(MONDAY, 9, 30).isoformat()
    assert qqq["last_status"] == "gap"
    assert qqq["last_error_class"] == "daemon_dead"
    assert qqq["last_data_snap_ts"] == et(FRIDAY, 16, 15).isoformat()
    assert qqq["minutes_since"] == 3925.5


def test_now_reports_null_for_the_unbuilt_writers(service: DashboardService):
    now = service.run_query("now", {})
    # The daemon does not journal the token mint stamp yet, and the watchdog's dead-man
    # ping is not built. Both read as null rather than being read from ~/.config.
    assert now["token_minted_at"] is None
    assert now["token_age_minutes"] is None
    assert now["dead_man_last_ping"] is None


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
    # Four slots with rows, seven past slots without, the rest still to come.
    assert chains["counts"] == {
        "captured": 2,
        "suspect": 1,
        "gap": 1,
        "missing": 7,
        "pending": 395,
        "out_of_scope": 0,
    }
    assert chains["unreadable_segments"] == 1
    first, second, third, fourth, fifth = chains["slots"][:5]
    assert (first["status"], first["rows"]) == ("captured", 2)
    assert (second["status"], second["rows"]) == ("captured", 2)
    assert (third["status"], third["rows"], third["error_class"]) == ("gap", 0, "http_429")
    assert (fourth["status"], fourth["rows"]) == ("suspect", 2)
    assert (fifth["status"], fifth["rows"], fifth["error_class"]) == ("missing", 0, None)
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
        "missing": 10,
        "pending": 395,
        "out_of_scope": 0,
    }
    assert qqq["slots"][0]["error_class"] == "daemon_dead"


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
        root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR, connection=spy, page=b""
    )
    with pytest.raises(QueryParameterError):
        service.run_query("today", raw)
    assert spy.calls == []


def test_an_unknown_query_name_never_reaches_the_connection(root: Path):
    spy = SpyConnection()
    service = DashboardService(
        root, clock=ManualClock(NOW.astimezone(UTC)), calendar=CALENDAR, connection=spy, page=b""
    )
    with pytest.raises(KeyError):
        service.run_query("history", {})
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


def test_a_slot_reports_its_most_common_error_class_and_how_many_it_carried(
    fixture_lake: FixtureLake,
):
    # A partial chain carries one class per failed date window, so several reasons in one
    # minute is normal. The reported reason is the most common, not the smallest by name:
    # ``http_429`` sorts first here and must not win.
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
    assert slot["error_class"] == "timeout_error"
    assert slot["error_class_count"] == 3
    row = service_over(root).run_query("now", {})["surfaces"][0]
    assert row["last_error_class"] == "timeout_error"
    assert row["last_error_class_count"] == 3


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
    assert strip["slots"][0]["error_class"] is None
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
    assert row["last_error_class"] == "http_429"
    assert row["lookback_exhausted"] is False


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
    assert strip["counts"]["missing"] == 11
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
    """A security master under ``root`` holding one equity and its capture epoch."""
    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY,
        capture_start=capture_start,
        valid_from=date(2026, 1, 2),
        ticker=ticker,
    )
    return master.write(master_path(root))


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
        "missing": 5,
        "pending": 395,
        "out_of_scope": 3,
    }
    # 09:32 carried a gap marker. Before the epoch it is out of scope, never a gap.
    assert chains["slots"][2]["status"] == "out_of_scope"
    assert chains["slots"][2]["error_class"] is None
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


# -- a security master whose types drifted -----------------------------------


def write_retyped_master(
    root: Path, ticker: str, capture_start: datetime, casts: dict[str, pa.DataType]
) -> Path:
    """A master carrying the pinned column names with some of their types swapped.

    This is the drift a reference file takes when a writer changes: the names still
    line up, so the read succeeds and the wrong types surface later, from a comparison
    several frames deep rather than from the read. ``casts`` names the columns to swap
    and leaves the rest pinned. An empty ``casts`` writes a well-formed master, which is
    what makes this helper usable as its own control.
    """
    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=capture_start, valid_from=date(2026, 1, 2), ticker=ticker
    )
    table = master.to_table()
    drifted = pa.schema(
        [pa.field(field.name, casts.get(field.name, field.type)) for field in table.schema]
    )
    path = master_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table.cast(drifted), path)
    return path


@pytest.mark.parametrize(
    "casts",
    [
        pytest.param(dict.fromkeys(MASTER_SCHEMA.names, pa.string()), id="all"),
        pytest.param({"capture_start": pa.string()}, id="capture_start_string"),
        pytest.param({"capture_start": pa.timestamp("us")}, id="capture_start_naive"),
        pytest.param({"valid_from": pa.string()}, id="valid_from_string"),
    ],
)
def test_a_drifted_master_costs_the_clamp_and_nothing_else(root: Path, casts: dict):
    # A reference table must never break a panel. Each of these files is valid Parquet
    # carrying the pinned column names, so the read itself succeeds and the drift lands
    # later. Three mechanisms carry it there.
    #
    # 1. A retyped ``schema_version`` is refused by the master's own reader.
    # 2. A retyped ``capture_start`` is unfit to compare instants against.
    # 3. A retyped ``valid_from`` raises out of a date comparison inside resolution.
    #
    # Every one costs that ticker its clamp and nothing more, so both panels still serve.
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


def test_the_same_helper_with_nothing_retyped_still_clamps(root: Path):
    # The control for the drifted cases above. The helper writes a master the panel can
    # use, so their verdict of no clamp is the drift talking and not a broken fixture. It
    # also stops the whole set from being satisfied by a panel that never clamps at all.
    write_retyped_master(root, "SPY", et(MONDAY, 9, 36), {})
    chains = service_over(root).run_query("today", {"date": "2026-08-24", "ticker": "SPY"})[
        "strips"
    ][0]
    assert chains["capture_start"] == et(MONDAY, 9, 36).isoformat()
    assert chains["counts"]["out_of_scope"] == 3
    assert chains["counts"]["gap"] == 0


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
