"""The Schwab import, ``python -m lake.bar_import schwab``, into the bars surface.

Marketlake #951. The runtime's first z-score needs 40 sessions of closes before capture holds
any, so the import asks Schwab for the 40 sessions ending the session before a ticker's first
capture span session and lands them through #950's ``_land``. The cases follow the issue's
Acceptance list.

1. Through a cassette, the 40 sessions land, marked imported with an ``adjusted`` basis as of the
   fetch date and the window identity, and ``load_bars(adjust='none')`` reads them back.
2. A rerun under the same identity writes nothing.
3. The request's window ends on the last session, so nothing on or after the first span session
   is asked for, and a candle naming one is dropped rather than landed.
4. An undo right after a full import leaves the lake byte for byte as it was.

Every expected session, stamp and identity is written out or computed here from the calendar's
inputs, so no expected value comes from the code under test.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from lake import bar_import
from lake.bar_import import BarImportRefused, import_schwab, undo_schwab
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans, spans_path
from lake.cassette import Cassette, CassetteError, Interaction
from lake.config import BucketTarget
from lake.loader import load_bars, read_provenance
from lake.manifest import append_manifest, latest_entries, sha256_file
from lake.schwab import VendorAuthError
from lake.security_master import KIND_EQUITY, SecurityMaster, master_path
from lake.vendor import BARS_ENDPOINT, VendorError, VendorResponse, bars_params
from tests.component.test_load_bars import _ledger_table
from tests.support.bars import write_bars
from tests.support.bucket import FakeS3
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import SCHWAB_API_KEY, SCHWAB_APP_SECRET, write_config
from tests.support.lake import FixtureLake
from tests.support.vendor import CassetteVendor, bars_candle, bars_interactions

# Labor Day 2026 falls inside the window, so a window counted in weekdays starts a day late.
LABOR_DAY = date(2026, 9, 7)
CALENDAR = weekday_sessions(
    *(date(2026, 8, 24) + timedelta(weeks=n) for n in range(12)), holidays={LABOR_DAY}
)
# GLD's own onboarding: Friday 2026-10-30 at 20:38 ET, after that session's close, so the first
# capture span session is Monday 2026-11-02 and Friday is the window's last session.
ONBOARD = datetime(2026, 10, 30, 20, 38, tzinfo=MARKET_TZ)
FIRST_SPAN = date(2026, 11, 2)
FIRST = date(2026, 9, 4)
LAST = date(2026, 10, 30)
# Monday's runbook window, and the request's two bounds, the two sessions' Eastern midnights.
MONDAY_0745 = datetime(2026, 11, 2, 7, 45, tzinfo=MARKET_TZ)
START = datetime(2026, 9, 4, 4, 0, tzinfo=UTC)
END = datetime(2026, 10, 30, 4, 0, tzinfo=UTC)
UNDO_TARGET = BucketTarget(bucket="lake-backup", prefix="lake")


def _sessions() -> list[date]:
    """The 40 weekdays from FIRST to LAST, less Labor Day."""
    days, day = [], FIRST
    while day <= LAST:
        if day.weekday() < 5 and day != LABOR_DAY:
            days.append(day)
        day += timedelta(days=1)
    return days


SESSIONS = _sessions()


def _lake(
    tmp_path: Path, tickers=("GLD",), *, onboard: datetime = ONBOARD, ids: dict | None = None
) -> Path:
    """A lake whose master holds each ticker from ``onboard``, with an open span from then.

    ``ids``, when given, is filled with each ticker's instrument id as the master issued it.
    """
    root = FixtureLake(tmp_path / "lake").with_reference("schema_versions", _ledger_table()).build()
    master = SecurityMaster()
    ledger = CaptureSpans()
    for ticker in tickers:
        instrument = master.register(
            kind=KIND_EQUITY,
            capture_start=onboard.astimezone(UTC),
            valid_from=onboard.date(),
            ticker=ticker,
        )
        if ids is not None:
            ids[ticker] = instrument
        ledger.open_span(instrument, onboard.astimezone(UTC), options=False)
    master.write(master_path(root))
    ledger.write(spans_path(root))
    return root


def _candle(day: date, n: int, *, hour: int = 5) -> dict:
    """Schwab's candle for ``day``, stamped ``hour``:00 UTC the way every probe candle was."""
    close = 100.0 + n
    return bars_candle(
        datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
        open_=close - 0.5,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=1000 + n,
    )


def _vendor(
    ticker: str = "GLD",
    candles: list[dict] | None = None,
    *,
    start: datetime = START,
    end: datetime = END,
    status: int = 200,
) -> CassetteVendor:
    listed = [_candle(d, n) for n, d in enumerate(SESSIONS)] if candles is None else candles
    (interaction,) = bars_interactions(ticker, "1d", [(start, end, listed)])
    if status != 200:
        interaction = Interaction(
            endpoint=interaction.endpoint,
            params=interaction.params,
            status=status,
            body={},
        )
    return CassetteVendor(Cassette(interactions=(interaction,)))


class _Vendors:
    """One cassette vendor per ticker, recording every request it answers."""

    def __init__(self, *, clock: ManualClock | None = None, **by_ticker: CassetteVendor) -> None:
        self.by_ticker = by_ticker
        self.clock = clock
        self.requests: list[tuple[str, datetime, datetime]] = []

    def get_daily_bars(self, symbol, *, start, end, extended_hours=None, previous_close=None):
        self.requests.append((symbol, start, end))
        if self.clock is not None:
            # A request takes a second, so the fetch's two stamps differ.
            self.clock.advance(1)
        return self.by_ticker[symbol].get_daily_bars(
            symbol,
            start=start,
            end=end,
            extended_hours=extended_hours,
            previous_close=previous_close,
        )


def _run(root, tickers=("GLD",), *, vendor=None, now=MONDAY_0745, dry_run=False, bucket=None):
    return import_schwab(
        root,
        list(tickers),
        vendor=vendor if vendor is not None else _Vendors(GLD=_vendor()),
        clock=now if isinstance(now, ManualClock) else ManualClock(now),
        calendar=CALENDAR,
        dry_run=dry_run,
        bucket=bucket,
    )


def _refused(root, tickers=("GLD",), **kwargs) -> str:
    with pytest.raises(BarImportRefused) as refused:
        _run(root, tickers, **kwargs)
    message = str(refused.value)
    assert "\n" not in message
    return message


def _rel(ticker: str, day: date) -> str:
    return f"bars/ticker={ticker}/freq=1d/date={day.isoformat()}.parquet"


def _bars_files(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in (root / "bars").rglob("*") if p.is_file())


def _snapshot(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


# -- 1. the 40 sessions land and read back ---------------------------------------------


def test_the_forty_sessions_land_and_read_back_through_load_bars(tmp_path):
    """Acceptance 1. Labor Day in the window catches a window counted in weekdays."""
    root = _lake(tmp_path)
    vendors = _Vendors(GLD=_vendor())

    summary = _run(root, vendor=vendors)

    assert len(SESSIONS) == 40
    assert _bars_files(root) == [_rel("GLD", d) for d in SESSIONS]
    table = load_bars("GLD", "1d", lake_root=root, adjust="none")
    assert table.num_rows == 40
    assert table.column("close").to_pylist() == [100.0 + n for n in range(40)]
    assert table.column("open").to_pylist() == [99.5 + n for n in range(40)]
    assert table.column("volume").to_pylist() == [1000 + n for n in range(40)]
    # The vendor's own stamp, 05:00Z, rather than the vintage's Eastern midnight.
    assert table.column("bar_ts").to_pylist()[0] == "2026-09-04T05:00:00+00:00"
    assert table.column("origin").to_pylist() == ["imported"] * 40
    assert table.column("basis").to_pylist() == ["adjusted"] * 40
    assert set(table.column("source").to_pylist()) == {"schwab:pricehistory"}
    assert set(table.column("source_as_of").to_pylist()) == {"2026-11-02"}
    (line,) = summary.render().splitlines()
    assert line.startswith("GLD: landed 40 of 40 row(s)")
    assert "0 session(s) the calendar names and the response lacks" in line


def test_each_partition_records_the_window_identity(tmp_path):
    root = _lake(tmp_path)

    _run(root)

    identity = "schwab-window:GLD:2026-09-04..2026-10-30"
    entries = latest_entries(root)
    for day in SESSIONS:
        rel = _rel("GLD", day)
        assert entries[rel]["source"] == f"bar-import:{identity}"
        assert entries[rel]["rows"] == 1
        provenance = read_provenance(pq.read_schema(root / rel).metadata)
        assert provenance.source_id == identity
        assert provenance.source_as_of == date(2026, 11, 2)


def test_each_row_records_the_request_it_came_from(tmp_path):
    """The rows come from the sweep's builder, so they carry the fetch and its window."""
    ids: dict[str, int] = {}
    root = _lake(tmp_path, ("GLD", "USO"), ids=ids)
    # A UTC clock, as the real one is, so the stamps read in UTC.
    clock = ManualClock(MONDAY_0745.astimezone(UTC))
    vendors = _Vendors(clock=clock, GLD=_vendor("GLD"), USO=_vendor("USO"))

    _run(root, ("GLD", "USO"), vendor=vendors, now=clock)

    row = pq.read_table(root / _rel("GLD", FIRST)).to_pylist()[0]
    assert row["window_start"] == "2026-09-04T04:00:00+00:00"
    assert row["window_end"] == "2026-10-30T04:00:00+00:00"
    # 07:45 ET is 12:45Z, and each request advances the clock a second.
    assert row["fetch_ts"] == "2026-11-02T12:45:00+00:00"
    assert row["fetch_end_ts"] == "2026-11-02T12:45:01+00:00"
    for ticker in ("GLD", "USO"):
        table = load_bars(ticker, "1d", lake_root=root, adjust="none")
        assert set(table.column("instrument_id").to_pylist()) == {ids[ticker]}
    assert ids["GLD"] != ids["USO"]


def test_a_session_the_response_lacks_is_counted_and_the_rest_land(tmp_path):
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS) if d != date(2026, 10, 1)]

    summary = _run(root, vendor=_Vendors(GLD=_vendor(candles=candles)))

    assert len(_bars_files(root)) == 39
    assert "1 session(s) the calendar names and the response lacks" in summary.render()


def test_a_dry_run_fetches_and_writes_nothing(tmp_path):
    root = _lake(tmp_path)
    before = _snapshot(root)
    vendors = _Vendors(GLD=_vendor())

    summary = _run(root, vendor=vendors, dry_run=True)

    assert _snapshot(root) == before
    assert len(vendors.requests) == 1
    assert "dry run, would land 40 of 40 row(s)" in summary.render()


def test_two_tickers_land_under_their_own_identities(tmp_path):
    root = _lake(tmp_path, ("GLD", "USO"))
    vendors = _Vendors(GLD=_vendor("GLD"), USO=_vendor("USO"))

    _run(root, ("GLD", "USO"), vendor=vendors)

    sources = {entry["source"] for entry in latest_entries(root).values()}
    assert "bar-import:schwab-window:GLD:2026-09-04..2026-10-30" in sources
    assert "bar-import:schwab-window:USO:2026-09-04..2026-10-30" in sources


# -- 2. a rerun writes nothing -----------------------------------------------------------


def test_a_rerun_under_the_same_identity_writes_nothing(tmp_path):
    """Acceptance 2. Catches an identity that moves between runs, such as the fetch's date."""
    root = _lake(tmp_path)
    _run(root)
    after = _snapshot(root)

    summary = _run(root, now=datetime(2026, 11, 3, 7, 45, tzinfo=MARKET_TZ))

    assert _snapshot(root) == after
    assert "landed 0 of 40 row(s)" in summary.render()
    assert "40 already imported" in summary.render()


def test_a_key_held_by_another_source_refuses_the_whole_run(tmp_path):
    root = _lake(tmp_path)
    path = write_bars(root, "GLD", "1d", LAST.isoformat(), [1.0], instrument_id=1)
    append_manifest(
        root,
        partition=_rel("GLD", LAST),
        source="bars-sweep",
        sha256=sha256_file(path),
        rows=1,
        fetched_at=None,
    )
    after = _snapshot(root)

    message = _refused(root)

    assert "already held by captured bars" in message
    assert _snapshot(root) == after


# -- 3. nothing on or after the first span session is asked for --------------------------


def test_the_request_ends_on_the_last_session_before_the_first_span_session(tmp_path):
    """Acceptance 3. The cassette answers only this window, and the bounds are compared here
    too, so a window reaching the first span session fails twice over."""
    root = _lake(tmp_path)
    vendors = _Vendors(GLD=_vendor())

    _run(root, vendor=vendors)

    ((symbol, start, end),) = vendors.requests
    assert (symbol, start, end) == ("GLD", START, END)
    assert end < datetime(2026, 11, 2, 4, 0, tzinfo=UTC)


def test_a_candle_outside_the_window_is_dropped_not_landed(tmp_path):
    """A response reaching past its window lands none of it, the first span session least."""
    root = _lake(tmp_path)
    outside = [date(2026, 9, 3), FIRST_SPAN]
    candles = [_candle(d, 90 + n) for n, d in enumerate(outside)] + [
        _candle(d, n) for n, d in enumerate(SESSIONS)
    ]

    summary = _run(root, vendor=_Vendors(GLD=_vendor(candles=candles)))

    assert _bars_files(root) == [_rel("GLD", d) for d in SESSIONS]
    assert "2 candle(s) outside the window dropped" in summary.render()


def test_a_candle_the_builder_cannot_read_is_counted_apart_from_a_missing_session(tmp_path):
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS)]
    del candles[10]["datetime"]

    line = _run(root, vendor=_Vendors(GLD=_vendor(candles=candles))).render()

    assert "1 candle(s) the row builder could not read" in line
    assert "1 session(s) the calendar names and the response lacks" in line
    assert "0 candle(s) outside the window dropped" in line


def test_a_stamp_the_evening_before_names_the_next_session(tmp_path):
    """A stamp at 23:00 Eastern, 03:00Z under daylight time, sits on the evening before.
    ``bars.daily_session_of``'s 18:00 boundary reads it as the next session, where a strict
    Eastern date would name 2026-10-29 and clash with that session's own candle."""
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS[:-1])]
    evening = _candle(LAST, 39)
    evening["datetime"] = int(datetime(2026, 10, 29, 23, 0, tzinfo=MARKET_TZ).timestamp() * 1000)

    _run(root, vendor=_Vendors(GLD=_vendor(candles=[*candles, evening])))

    table = load_bars("GLD", "1d", lake_root=root, adjust="none")
    assert table.num_rows == 40
    assert table.column("close").to_pylist()[-1] == 139.0


def test_a_last_session_that_is_today_refuses(tmp_path):
    """An evening onboarding imported that evening would land a candle before it is final."""
    root = _lake(tmp_path, onboard=datetime(2026, 11, 2, 17, 30, tzinfo=MARKET_TZ))

    # The real clock runs in UTC, and 19:00 ET is already 2026-11-03 there.
    message = _refused(root, now=datetime(2026, 11, 3, 0, 0, tzinfo=UTC))

    assert "2026-11-02" in message and "not final" in message
    assert not (root / "bars").exists()


def test_source_as_of_is_the_fetchs_eastern_date_on_a_utc_clock(tmp_path):
    """21:00 ET on 2026-11-02 is 02:00Z on 2026-11-03. The as-of date is the Eastern one."""
    root = _lake(tmp_path)

    _run(root, now=datetime(2026, 11, 3, 2, 0, tzinfo=UTC))

    table = load_bars("GLD", "1d", lake_root=root, adjust="none")
    assert set(table.column("source_as_of").to_pylist()) == {"2026-11-02"}


# -- a recorded key found missing ---------------------------------------------------------


def _split_vendor() -> _Vendors:
    """Tuesday's answer after a 2-for-1 split: every price halved, every volume doubled."""
    candles = []
    for n, day in enumerate(SESSIONS):
        candle = _candle(day, n)
        for name in ("open", "high", "low", "close"):
            candle[name] = candle[name] / 2
        candle["volume"] *= 2
        candles.append(candle)
    return _Vendors(GLD=_vendor(candles=candles))


def _lost_one(tmp_path: Path) -> tuple[Path, str, bytes]:
    root = _lake(tmp_path)
    _run(root)
    rel = _rel("GLD", date(2026, 9, 9))
    original = (root / rel).read_bytes()
    (root / rel).unlink()
    return root, rel, original


TUESDAY_0745 = datetime(2026, 11, 3, 7, 45, tzinfo=MARKET_TZ)


def test_a_missing_key_the_bucket_holds_refuses_rather_than_writing_a_new_fetch(tmp_path):
    """A rescaled fetch written over a key the upload sent would put two scales in one import
    and replace the bucket's copy at the next upload."""
    root, rel, _original = _lost_one(tmp_path)
    client = FakeS3()
    client.store(UNDO_TARGET.key(rel), b"sent")
    after = _snapshot(root)

    message = _refused(
        root, vendor=_split_vendor(), now=TUESDAY_0745, bucket=lambda: (UNDO_TARGET, client)
    )

    assert f"{rel} is recorded under this import and missing here, and the bucket holds it" in (
        message
    )
    assert "restore-range --surface bars --ticker GLD --from 2026-09-09 --to 2026-09-09" in message
    assert _snapshot(root) == after


def test_a_missing_key_with_no_bucket_to_read_refuses(tmp_path):
    root, rel, _original = _lost_one(tmp_path)
    after = _snapshot(root)

    message = _refused(root, vendor=_split_vendor(), now=TUESDAY_0745)

    assert "no bucket to read" in message and "restore-range" in message
    assert _snapshot(root) == after


def test_a_missing_key_the_bucket_lacks_is_written_again(tmp_path):
    """The state an undo stopped part-way leaves before the upload, which only a rerun repairs."""
    root, rel, _original = _lost_one(tmp_path)

    summary = _run(root, now=TUESDAY_0745, bucket=lambda: (UNDO_TARGET, FakeS3()))

    assert (root / rel).exists()
    assert "1 recorded file(s) found missing and written again" in summary.render()


def test_a_full_rerun_reads_no_bucket(tmp_path):
    """The bucket is read only for a missing recorded key."""
    root = _lake(tmp_path)
    _run(root)

    def no_bucket():  # pragma: no cover - a full rerun never asks
        raise AssertionError("the bucket was read")

    summary = _run(root, now=TUESDAY_0745, bucket=no_bucket)

    assert "40 already imported" in summary.render()


# -- refusals ---------------------------------------------------------------------------


def test_two_candles_for_one_session_refuse(tmp_path):
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS)] + [_candle(LAST, 7, hour=6)]

    assert "two candles for 2026-10-30" in _refused(
        root, vendor=_Vendors(GLD=_vendor(candles=candles))
    )
    assert not (root / "bars").exists()


@pytest.mark.parametrize("name", ["open", "high", "low", "close"])
@pytest.mark.parametrize("value", [math.nan, math.inf, None, "101.5", True])
def test_a_price_that_is_not_a_finite_number_refuses(tmp_path, name, value):
    """``True`` builds as 1.0 in a float column, so the batch check alone would land it."""
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS)]
    candles[3][name] = value

    assert "not a finite number" in _refused(root, vendor=_Vendors(GLD=_vendor(candles=candles)))
    assert not (root / "bars").exists()


def test_a_volume_the_column_refuses_stops_before_the_lock(tmp_path):
    root = _lake(tmp_path)
    candles = [_candle(d, n) for n, d in enumerate(SESSIONS)]
    candles[5]["volume"] = "lots"

    assert "does not fit the bars schema" in _refused(
        root, vendor=_Vendors(GLD=_vendor(candles=candles))
    )
    assert not (root / "bars").exists()


def test_an_empty_response_refuses(tmp_path):
    root = _lake(tmp_path)

    assert "holds none of its 40 sessions" in _refused(
        root, vendor=_Vendors(GLD=_vendor(candles=[]))
    )


def test_a_status_that_is_not_2xx_refuses(tmp_path):
    root = _lake(tmp_path)

    assert "status 500" in _refused(root, vendor=_Vendors(GLD=_vendor(status=500)))


class _Failing:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def get_daily_bars(self, *args, **kwargs) -> VendorResponse:
        raise self.exc


def test_a_dead_token_refuses_with_the_reauth_line(tmp_path):
    root = _lake(tmp_path)

    message = _refused(root, vendor=_Failing(VendorAuthError("refresh token expired")))

    # The VM has no browser, so the repair is the laptop's reauth.sh and the VM's pull.
    assert "reauth.sh on the laptop" in message
    assert "python -m lake.token_store pull" in message


def test_a_vendor_failure_refuses(tmp_path):
    root = _lake(tmp_path)

    assert "VendorError" in _refused(root, vendor=_Failing(VendorError("boom")))


def test_a_dropped_connection_refuses_with_one_line(tmp_path):
    root = _lake(tmp_path)

    message = _refused(root, vendor=_Failing(httpx.ConnectTimeout("timed out")))

    assert "did not arrive (ConnectTimeout)" in message and "Run the import again" in message


def test_a_failure_that_is_not_the_networks_keeps_its_traceback(tmp_path):
    """A cassette miss is ``CassetteError``. Catching it would hide a wrong window as a refusal."""
    root = _lake(tmp_path)

    with pytest.raises(CassetteError):
        _run(root, vendor=_Vendors(GLD=_vendor(end=END + timedelta(days=1))))


def test_a_ticker_not_onboarded_refuses_before_any_request(tmp_path):
    root = _lake(tmp_path, ("USO",))
    vendors = _Vendors(GLD=_vendor())

    assert "lake.onboard GLD" in _refused(root, vendor=vendors)
    assert vendors.requests == []


def test_a_calendar_short_of_forty_sessions_refuses_before_any_request(tmp_path):
    root = _lake(tmp_path)
    vendors = _Vendors(GLD=_vendor())
    short = weekday_sessions(*(date(2026, 9, 28) + timedelta(weeks=n) for n in range(6)))

    with pytest.raises(BarImportRefused) as refused:
        import_schwab(root, ["GLD"], vendor=vendors, clock=ManualClock(MONDAY_0745), calendar=short)

    assert "short of 40" in str(refused.value)
    assert vendors.requests == []


def test_a_lake_root_with_no_manifest_refuses(tmp_path):
    root = _lake(tmp_path)
    (root / "manifest.jsonl").unlink()
    vendors = _Vendors(GLD=_vendor())

    assert "holds no manifest.jsonl" in _refused(root, vendor=vendors)
    assert vendors.requests == []


def test_a_ticker_named_twice_refuses(tmp_path):
    root = _lake(tmp_path)

    assert "named twice" in _refused(root, ("GLD", "GLD"))


def test_a_run_inside_the_session_refuses_before_any_request(tmp_path):
    root = _lake(tmp_path)
    vendors = _Vendors(GLD=_vendor())

    message = _refused(root, vendor=vendors, now=datetime(2026, 11, 2, 10, 0, tzinfo=MARKET_TZ))

    assert "Nothing was written" in message
    assert vendors.requests == []


# -- 4. the undo --------------------------------------------------------------------------


def _undo(root, tickers=("GLD",), *, client=None, now=MONDAY_0745):
    return undo_schwab(
        root,
        list(tickers),
        client=client if client is not None else FakeS3(),
        target=UNDO_TARGET,
        clock=ManualClock(now),
        calendar=CALENDAR,
    )


def test_an_undo_right_after_an_import_leaves_the_lake_as_it_was(tmp_path):
    """Acceptance 4."""
    root = _lake(tmp_path, ("GLD", "USO"))
    before = _snapshot(root)
    _run(root, ("GLD", "USO"), vendor=_Vendors(GLD=_vendor("GLD"), USO=_vendor("USO")))
    client = FakeS3()
    client.store(UNDO_TARGET.key("manifest.jsonl"), before["manifest.jsonl"])

    summary = _undo(root, ("GLD", "USO"), client=client)

    assert _snapshot(root) == before
    assert (summary.lines, summary.partitions) == (80, 80)


def test_an_undo_of_the_last_ticker_imported_leaves_the_other(tmp_path):
    """The ticker in the identity is what keeps one ticker's undo off the other's lines."""
    root = _lake(tmp_path, ("GLD", "USO"))
    _run(root, ("GLD", "USO"), vendor=_Vendors(GLD=_vendor("GLD"), USO=_vendor("USO")))

    summary = _undo(root, ("USO",))

    assert summary.lines == 40
    assert _bars_files(root) == [_rel("GLD", d) for d in SESSIONS]


def test_an_undo_of_a_ticker_imported_before_another_refuses(tmp_path):
    root = _lake(tmp_path, ("GLD", "USO"))
    _run(root, ("GLD", "USO"), vendor=_Vendors(GLD=_vendor("GLD"), USO=_vendor("USO")))
    after = _snapshot(root)

    with pytest.raises(BarImportRefused) as refused:
        _undo(root, ("GLD",))

    message = str(refused.value)
    assert "bar-import:schwab-window:USO:2026-09-04..2026-10-30 was recorded after it" in message
    assert "Undo that import first" in message
    assert _snapshot(root) == after


def test_an_undo_inside_the_session_refuses_before_it_reads_the_lake(tmp_path):
    """The window refuses first, so an undo at 10:00 says why without reading the master."""
    root = _lake(tmp_path)
    (root / "reference" / "security_master.parquet").unlink()

    with pytest.raises(BarImportRefused) as refused:
        _undo(root, now=datetime(2026, 11, 2, 10, 0, tzinfo=MARKET_TZ))

    assert "Nothing was undone" in str(refused.value)
    assert "security master" not in str(refused.value).lower()


def test_an_undo_after_a_killed_import_says_to_rerun_the_import(tmp_path):
    """A killed import leaves files no manifest line records, which the undo cannot cut."""
    root = _lake(tmp_path)
    before = _snapshot(root)
    _run(root)
    (root / "manifest.jsonl").write_bytes(before["manifest.jsonl"])
    after = _snapshot(root)

    with pytest.raises(BarImportRefused) as refused:
        _undo(root)

    assert f"{_rel('GLD', FIRST)} is on disk and the manifest records none of this import" in str(
        refused.value
    )
    assert _snapshot(root) == after


def test_an_undo_of_a_lake_root_with_no_manifest_refuses(tmp_path):
    root = _lake(tmp_path)
    (root / "manifest.jsonl").unlink()

    with pytest.raises(BarImportRefused) as refused:
        _undo(root)

    assert "holds no manifest.jsonl" in str(refused.value)


def test_an_undo_with_nothing_imported_changes_nothing(tmp_path):
    root = _lake(tmp_path)
    before = _snapshot(root)

    summary = _undo(root)

    assert summary.lines == 0
    assert _snapshot(root) == before


# -- the command ----------------------------------------------------------------------------


def test_the_schwab_command_builds_the_vendor_from_the_config(tmp_path, capsys):
    root = _lake(tmp_path)
    config = write_config(tmp_path, root)
    built: list[tuple] = []

    def factory(token_path, *, api_key, app_secret):
        built.append((Path(token_path).name, api_key, app_secret))
        return _Vendors(GLD=_vendor())

    code = bar_import.main(
        ["schwab", "GLD", "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
        vendor_factory=factory,
    )

    assert code == 0
    assert built == [("token.json", SCHWAB_API_KEY, SCHWAB_APP_SECRET)]
    assert "bar_import: GLD: landed 40 of 40 row(s)" in capsys.readouterr().out


def test_a_schwab_refusal_reaches_the_operator_as_one_line_and_exit_2(tmp_path, capsys):
    root = _lake(tmp_path)
    config = write_config(tmp_path, root)

    with pytest.raises(SystemExit) as exited:
        bar_import.main(
            ["schwab", "GLD", "--config", str(config)],
            clock=ManualClock(MONDAY_0745),
            calendar=CALENDAR,
            vendor_factory=lambda *a, **k: _Failing(VendorAuthError("expired")),
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("bar_import: ") and "reauth" in err[0]


@pytest.mark.parametrize(
    "error", [FileNotFoundError("token.json"), json.JSONDecodeError("Expecting", "{not", 1)]
)
def test_an_unreadable_token_reaches_the_operator_as_one_line(tmp_path, capsys, error):
    """schwab-py reads the token while it builds the client, so a missing file raises
    ``OSError`` and a corrupt one ``ValueError`` before any request."""
    root = _lake(tmp_path)
    config = write_config(tmp_path, root)

    def factory(token_path, **kwargs):
        raise error

    with pytest.raises(SystemExit) as exited:
        bar_import.main(
            ["schwab", "GLD", "--config", str(config)],
            clock=ManualClock(MONDAY_0745),
            calendar=CALENDAR,
            vendor_factory=factory,
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and f"cannot be read ({type(error).__name__})" in err[0]


def test_the_run_window_refuses_before_the_token_is_read(tmp_path, capsys):
    root = _lake(tmp_path)
    config = write_config(tmp_path, root)

    def factory(*args, **kwargs):  # pragma: no cover - the window refuses first
        raise AssertionError("the token was read inside the session")

    with pytest.raises(SystemExit) as exited:
        bar_import.main(
            ["schwab", "GLD", "--config", str(config)],
            clock=ManualClock(datetime(2026, 11, 2, 10, 0, tzinfo=MARKET_TZ)),
            calendar=CALENDAR,
            vendor_factory=factory,
        )

    assert exited.value.code == 2
    assert "Nothing was written" in capsys.readouterr().err


def test_the_schwab_command_refuses_on_a_shadow_host(tmp_path, capsys):
    root = _lake(tmp_path)
    config = write_config(tmp_path, root, role="shadow")

    def factory(*args, **kwargs):  # pragma: no cover - the shadow refusal comes first
        raise AssertionError("no vendor on a shadow host")

    code = bar_import.main(
        ["schwab", "GLD", "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
        vendor_factory=factory,
    )

    assert code == 2
    assert "shadow" in capsys.readouterr().err


def test_the_schwab_undo_command_reads_the_bucket_from_the_config(tmp_path, monkeypatch, capsys):
    root = _lake(tmp_path)
    before = _snapshot(root)
    _run(root)
    monkeypatch.setattr(bar_import, "connect", lambda config: (UNDO_TARGET, FakeS3()))
    config = write_config(tmp_path, root)

    code = bar_import.main(
        ["schwab-undo", "GLD", "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
    )

    assert code == 0
    assert "bar_import: undid 40 manifest line(s)" in capsys.readouterr().out
    assert _snapshot(root) == before


def test_the_cassette_key_is_the_one_the_vendor_looks_up():
    """A cassette keyed off another spelling of the bounds would miss, and the import with it."""
    assert bars_params("GLD", "1d", start=START, end=END) == {
        "symbol": "GLD",
        "freq": "1d",
        "start": "2026-09-04T04:00:00+00:00",
        "end": "2026-10-30T04:00:00+00:00",
    }
    assert BARS_ENDPOINT == "bars"


def test_the_schwab_command_reads_the_bucket_for_a_missing_key(tmp_path, monkeypatch, capsys):
    """Catches a command that passes no bucket, which would refuse the one repair an undo stopped
    part-way before the upload needs."""
    root, rel, _original = _lost_one(tmp_path)
    monkeypatch.setattr(bar_import, "connect", lambda config: (UNDO_TARGET, FakeS3()))
    config = write_config(tmp_path, root)

    code = bar_import.main(
        ["schwab", "GLD", "--config", str(config)],
        clock=ManualClock(TUESDAY_0745),
        calendar=CALENDAR,
        vendor_factory=lambda *a, **k: _Vendors(GLD=_vendor()),
    )

    assert code == 0
    assert (root / rel).exists()
    assert "1 recorded file(s) found missing and written again" in capsys.readouterr().out
