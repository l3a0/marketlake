"""The two named queries over a real fixture lake.

These lay down journal segments in the pinned capture schemas and sealed partitions in
the fixture schema, then run the Now and Today queries against them through the real
sandboxed connection. The clock and the calendar are fakes, so the one real boundary is
the filesystem and the tier is component.

The fixture is a small, known Monday with every row kind the panels distinguish, plus a
prior Friday so the Now query has a gap-only latest day to walk back past.

- SPY chains: data at the open and the next minute, a gap at the third minute with an
  ``http_429`` reason, and a suspect data cycle at the fourth. Two writer sessions, so
  the segments concatenate. One garbage ``.arrows`` file sits beside them.
- SPY quotes: the open minute in a journal segment and the next minute in a sealed
  partition written in the fixture schema, so the union by name is exercised across two
  different schemas and two different timestamp offsets.
- QQQ chains: a single ``daemon_dead`` gap on Monday, and a data cycle at Friday's
  option close in a sealed partition.

The clock reads Monday 09:40:30 ET. So the last SPY chains data cycle at 09:33 is 7.5
minutes old, the slots through 09:40 are past, and everything after is pending.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import journal
from lake.calendar import MARKET_TZ
from lake.dashboard import DashboardService, QueryParameterError
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

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
    root = fixture_lake.build()
    # A file that is not an Arrow stream, beside the real segments.
    garbage = fixture_lake.segment_path("chains", "SPY", MONDAY, "garbage", 9)
    garbage.write_bytes(b"not an arrow stream")
    return root


@pytest.fixture
def root(fixture_lake: FixtureLake) -> Path:
    return build_lake(fixture_lake)


@pytest.fixture
def service(root: Path) -> DashboardService:
    clock = ManualClock(NOW.astimezone(UTC))
    return DashboardService(root, clock=clock, calendar=CALENDAR, page=b"<!doctype html>")


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
    assert qqq["counts"] == {"captured": 0, "suspect": 0, "gap": 1, "missing": 10, "pending": 395}
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
