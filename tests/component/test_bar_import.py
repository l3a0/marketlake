"""The vintage import, ``python -m lake.bar_import vintage``, into the bars surface.

Marketlake #950. quantitative-trading's GLD and USO vintages are the first history marketlake
did not capture. Each lands at the captured path, ``bars/ticker=T/freq=1d/date=D.parquet``,
with its provenance in the partition's own schema metadata and its identity in the manifest
entry's ``source``, so ``load_bars`` serves it beside captured bars. The cases follow the issue's
Acceptance list.

1. A small CSV in the yfinance header lands and reads back through ``load_bars`` unchanged,
   with null volume and the module's provenance, and a missing session is counted.
2. The ticker's first capture span session is refused and the session before it lands.
3. A manifested key from capture or from another import refuses the whole run.
4. A rerun after a full import writes nothing, and a killed run is repaired by the rerun.
5. A duplicate date, a non-session date and a header naming another ticker refuse before
   anything is written, and so do a ticker not onboarded and an instrument with no span.
6. Each run guard refuses with one line, and so does a source whose SHA-256 is not listed.
7. Every imported key reads back ``imported`` through ``read_provenance``.
8. The resync refuses rather than deletes an imported bars file only this lake records, in
   ``tests/component/test_bucket_resync.py``.
9. One ``append_entries`` call lands each ticker's entries.

Every CSV is written here and hashed here with ``hashlib``, so no expected value comes from the
code under test.
"""

from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake import bar_import, bars
from lake.actions import SWEEP_SOURCE
from lake.bar_import import BarImportRefused, Vintage, import_vintages
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans, spans_path
from lake.loader import (
    BASIS_ADJUSTED,
    ORIGIN_IMPORTED,
    Provenance,
    load_bars,
    read_provenance,
)
from lake.manifest import append_manifest, latest_entries, sha256_file
from lake.paths import LakePaths
from lake.security_master import (
    ID_TYPE_TICKER,
    KIND_EQUITY,
    Mapping,
    SecurityMaster,
    master_path,
)
from tests.component.test_load_bars import _ledger_table
from tests.support.bars import write_bars
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

# A Monday morning inside the runbook's 07:30 to 09:00 window, a Sunday inside the scrub window,
# a Monday inside the session, and Monday's bound, the open at 09:30 less the 15-minute in-flight
# allowance and the 15-minute pre-open margin, written out rather than computed.
MONDAY_0745 = datetime(2026, 8, 31, 7, 45, tzinfo=MARKET_TZ)
SUNDAY_20 = datetime(2026, 8, 30, 20, 0, tzinfo=MARKET_TZ)
MONDAY_10 = datetime(2026, 8, 31, 10, 0, tzinfo=MARKET_TZ)
MONDAY_BOUND = datetime(2026, 8, 31, 9, 0, tzinfo=MARKET_TZ)
CALENDAR = weekday_sessions(date(2026, 8, 24), date(2026, 8, 31))
# An evening onboard on Wednesday 2026-08-26, after that session's close, so the first capture
# span session is Thursday 2026-08-27 and Wednesday stays importable.
EVENING_ONBOARD = datetime(2026, 8, 26, 17, 30, tzinfo=MARKET_TZ)
FIRST_SPAN = date(2026, 8, 27)
AS_OF = date(2012, 4, 10)
HEADER = ("Price,Close,High,Low,Open,Volume", "Date,,,,,")
EQUITY = KIND_EQUITY


def _lake(
    tmp_path: Path,
    tickers: tuple[str, ...] = ("GLD",),
    *,
    onboard: datetime = EVENING_ONBOARD,
    spans: bool = True,
) -> Path:
    """A lake whose master holds each ticker from ``onboard`` with an open span from then."""
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
        if spans:
            ledger.open_span(instrument, onboard.astimezone(UTC), options=False)
    master.write(master_path(root))
    ledger.write(spans_path(root))
    return root


def _csv(
    path: Path, rows: list[tuple[str, float]], *, ticker: str = "GLD", header_ticker: str = ""
) -> Path:
    """A vintage CSV in yfinance's three-row header, in its real ``Close,High,Low,Open`` order.

    Each row's high sits one above its close, its low one below, its open half below, and its
    volume is a count the import must not carry.
    """
    named = header_ticker or ticker
    lines = [HEADER[0], f"Ticker,{named},{named},{named},{named},{named}", HEADER[1]]
    for day, close in rows:
        lines.append(f"{day},{close},{close + 1},{close - 1},{close - 0.5},57952")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


def _vintage(path: Path, ticker: str = "GLD") -> tuple[str, Vintage]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest, Vintage(
        sha256=digest,
        ticker=ticker,
        source=f"test:{path.name}",
        source_as_of=AS_OF,
        basis=BASIS_ADJUSTED,
    )


def _listed(*paths_and_tickers: tuple[Path, str]) -> dict[str, Vintage]:
    return dict(_vintage(path, ticker) for path, ticker in paths_and_tickers)


def _run(root: Path, sources, vintages, *, now: datetime | ManualClock = MONDAY_0745):
    return import_vintages(
        root,
        list(sources),
        clock=now if isinstance(now, ManualClock) else ManualClock(now),
        calendar=CALENDAR,
        vintages=vintages,
    )


def _refused(root: Path, sources, vintages, **kwargs) -> str:
    with pytest.raises(BarImportRefused) as refused:
        _run(root, sources, vintages, **kwargs)
    message = str(refused.value)
    assert "\n" not in message
    return message


def _rel(ticker: str, day: str) -> str:
    return f"bars/ticker={ticker}/freq=1d/date={day}.parquet"


def _bars_files(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in (root / "bars").rglob("*") if p.is_file())


def _snapshot(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


# -- 1. the vintage lands and reads back -----------------------------------------------


def test_a_vintage_lands_and_reads_back_through_load_bars(tmp_path):
    """Acceptance 1. A row before 2006-10-30, a winter row and a summer row.

    The first row predates the default calendar's twenty-year start, so it catches a history
    calendar built without an explicit start. The two seasons catch a constant ``04:00Z`` stamp.
    """
    root = _lake(tmp_path)
    source = _csv(
        tmp_path / "src" / "gld.csv",
        [("2006-04-26", 63.65), ("2006-12-15", 62.3), ("2007-07-16", 65.07)],
    )
    vintages = _listed((source, "GLD"))

    summary = _run(root, [source], vintages)

    table = load_bars("GLD", "1d", lake_root=root, adjust="none")
    assert table.column("close").to_pylist() == [63.65, 62.3, 65.07]
    assert table.column("high").to_pylist() == [64.65, 63.3, 66.07]
    assert table.column("low").to_pylist() == [62.65, 61.3, 64.07]
    assert table.column("open").to_pylist() == [63.15, 61.8, 64.57]
    assert table.column("volume").to_pylist() == [None, None, None]
    assert table.column("origin").to_pylist() == [ORIGIN_IMPORTED] * 3
    assert table.column("basis").to_pylist() == [BASIS_ADJUSTED] * 3
    assert table.column("source").to_pylist() == ["test:gld.csv"] * 3
    assert table.column("source_as_of").to_pylist() == ["2012-04-10"] * 3
    # Eastern midnight as a UTC instant: 04:00Z under daylight time, 05:00Z under standard time.
    assert table.column("bar_ts").to_pylist() == [
        "2006-04-26T04:00:00+00:00",
        "2006-12-15T05:00:00+00:00",
        "2007-07-16T04:00:00+00:00",
    ]
    assert [bars.session_of(ts) for ts in table.column("bar_ts").to_pylist()] == [
        date(2006, 4, 26),
        date(2006, 12, 15),
        date(2007, 7, 16),
    ]
    (ticker,) = summary.tickers
    assert (ticker.ticker, ticker.landed, ticker.skipped) == ("GLD", 3, 0)


def test_a_session_the_file_lacks_is_counted_and_not_refused(tmp_path):
    """Catches a missing session refused, or not counted. 2006-04-25 is the session missing."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-24", 60.0), ("2006-04-26", 61.0)])

    summary = _run(root, [source], _listed((source, "GLD")))

    (ticker,) = summary.tickers
    assert (ticker.landed, ticker.missing) == (2, 1)
    assert "1 session(s) the calendar names and the file lacks" in summary.render()


def test_the_listed_vintages_carry_the_values_the_issue_reviewed():
    """The module's list matches the table in #950, written out here as literals."""
    gld = "81e6295a8028362f82f9e96a89c686ef89991cb23afe42d4f884756019dc9d4c"
    uso = "94b6521980f64ce80feb5cdffb22168ed44f71eeaa93caca672f316eeda60ec2"
    assert set(bar_import.VINTAGES) == {gld, uso}
    assert bar_import.VINTAGES[gld].provenance == Provenance(
        ORIGIN_IMPORTED,
        BASIS_ADJUSTED,
        "quantitative-trading:data/inputdata_etf/gld.csv",
        f"sha256:{gld}",
        date(2012, 4, 10),
    )
    assert bar_import.VINTAGES[uso].provenance == Provenance(
        ORIGIN_IMPORTED,
        BASIS_ADJUSTED,
        "quantitative-trading:data/inputdata_etf/uso.csv",
        f"sha256:{uso}",
        date(2012, 4, 10),
    )
    assert bar_import.VINTAGES[gld].ticker == "GLD"
    assert bar_import.VINTAGES[uso].ticker == "USO"
    assert bar_import.VINTAGES[gld].manifest_source == f"bar-import:sha256:{gld}"


# -- 2. the first capture span session ---------------------------------------------------


def test_the_first_span_session_refuses_and_the_session_before_it_lands(tmp_path):
    """Catches a bound off by one session either way, or one read from the onboard's date."""
    root = _lake(tmp_path)
    before = _csv(tmp_path / "src" / "before.csv", [("2026-08-26", 300.0)])
    first = _csv(tmp_path / "src" / "first.csv", [("2026-08-27", 301.0)])
    vintages = _listed((before, "GLD"), (first, "GLD"))

    message = _refused(root, [first], vintages)
    assert "2026-08-27" in message and "first capture span session" in message
    assert _bars_files(root) == []

    _run(root, [before], vintages)
    assert _bars_files(root) == [_rel("GLD", "2026-08-26")]


def test_todays_session_imported_before_its_close_refuses(tmp_path):
    """A pre-open onboard makes today the first span session, which ``_span_sessions``' ceiling
    would not yet yield. Catches a bound read from the sessions that walk yields."""
    root = _lake(tmp_path, onboard=datetime(2026, 8, 31, 7, 0, tzinfo=MARKET_TZ))
    today = _csv(tmp_path / "src" / "today.csv", [("2026-08-31", 1.0)])
    friday = _csv(tmp_path / "src" / "friday.csv", [("2026-08-28", 1.0)])
    vintages = _listed((today, "GLD"), (friday, "GLD"))

    assert "2026-08-31" in _refused(root, [today], vintages)
    _run(root, [friday], vintages)
    assert _bars_files(root) == [_rel("GLD", "2026-08-28")]


def test_the_bound_is_the_earliest_span_of_every_instrument_the_ticker_named(tmp_path):
    """A ticker recycled to a new instrument keeps the old instrument's bars under its path.

    The old instrument captured from Wednesday's evening onboard until Friday, and the new one
    from Friday evening. Catches a bound read only from the instrument the ticker resolves to
    today, which would admit Thursday, a session the old instrument's capture covered.
    """
    root = _lake(tmp_path, spans=False)
    old_start = EVENING_ONBOARD.astimezone(UTC)
    # Saturday 2026-08-29, the first day the ticker names the new instrument.
    recycled = date(2026, 8, 29)
    new_start = datetime(2026, 8, 28, 21, 0, tzinfo=UTC)
    master = SecurityMaster(
        [
            Mapping(1, ID_TYPE_TICKER, "GLD", date(2026, 8, 26), recycled, EQUITY, old_start),
            Mapping(2, ID_TYPE_TICKER, "GLD", recycled, None, EQUITY, new_start),
        ]
    )
    spans = CaptureSpans()
    spans.open_span(1, old_start, options=False)
    spans.close_span(1, new_start)
    spans.open_span(2, new_start, options=False)
    master.write(master_path(root))
    spans.write(spans_path(root))
    source = _csv(tmp_path / "src" / "gld.csv", [("2026-08-27", 1.0)])

    assert "2026-08-27" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


# -- 3. conflicts refuse the whole run ---------------------------------------------------


def _manifest_bars(root: Path, ticker: str, day: str, *, source: str, close: float = 1.0) -> str:
    path = write_bars(root, ticker, "1d", day, [close], instrument_id=1)
    rel = path.relative_to(root).as_posix()
    append_manifest(
        root, partition=rel, source=source, sha256=sha256_file(path), rows=1, fetched_at=None
    )
    return rel


def test_a_captured_key_refuses_the_whole_run(tmp_path):
    """Catches an import that overwrites a captured partition, or one that skips it silently."""
    root = _lake(tmp_path)
    captured = _manifest_bars(root, "GLD", "2006-04-26", source=SWEEP_SOURCE)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    digest, vintage = _vintage(source)
    before = _snapshot(root)

    message = _refused(root, [source], {digest: vintage})

    assert captured in message
    assert "captured" in message
    assert vintage.manifest_source in message
    assert "1 key(s) conflict" in message
    assert "nothing was written" in message
    assert _snapshot(root) == before


def test_a_key_another_import_holds_refuses(tmp_path):
    """Catches a rerun rule keyed on the prefix alone rather than the source's identity."""
    root = _lake(tmp_path)
    other = "bar-import:sha256:" + "cd" * 32
    held = _manifest_bars(root, "GLD", "2006-04-26", source=other)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    before = _snapshot(root)

    message = _refused(root, [source], _listed((source, "GLD")))

    assert held in message and other in message
    assert _snapshot(root) == before


def test_a_second_ticker_that_conflicts_writes_nothing_for_the_first(tmp_path):
    """Catches a decision made one ticker at a time, interleaved with that ticker's writes."""
    root = _lake(tmp_path, ("GLD", "USO"))
    _manifest_bars(root, "USO", "2006-04-26", source=SWEEP_SOURCE)
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 63.0)])
    uso = _csv(tmp_path / "src" / "uso.csv", [("2006-04-26", 70.0)], ticker="USO")
    before = _snapshot(root)

    _refused(root, [gld, uso], _listed((gld, "GLD"), (uso, "USO")))

    assert _snapshot(root) == before


# -- 4. rerun and repair -----------------------------------------------------------------


def test_a_rerun_after_a_full_import_writes_nothing(tmp_path):
    """Catches a rerun that rewrites, re-appends, or refuses its own import."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    vintages = _listed((source, "GLD"))
    _run(root, [source], vintages)
    before = _snapshot(root)
    mtimes = {rel: os.stat(root / rel).st_mtime_ns for rel in _bars_files(root)}

    summary = _run(root, [source], vintages)

    (ticker,) = summary.tickers
    assert (ticker.landed, ticker.skipped) == (0, 2)
    assert _snapshot(root) == before
    assert {rel: os.stat(root / rel).st_mtime_ns for rel in _bars_files(root)} == mtimes


def test_a_rerun_exits_zero_through_the_command(tmp_path, monkeypatch, capsys):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    monkeypatch.setattr(bar_import, "VINTAGES", _listed((source, "GLD")))
    config = write_config(tmp_path, root)
    argv = ["vintage", str(source), "--config", str(config)]

    assert bar_import.main(argv, clock=ManualClock(MONDAY_0745), calendar=CALENDAR) == 0
    assert bar_import.main(argv, clock=ManualClock(MONDAY_0745), calendar=CALENDAR) == 0

    out = capsys.readouterr().out
    assert "GLD: landed 1" in out
    assert "GLD: landed 0" in out and "1 already imported" in out


def test_a_leftover_temp_file_is_removed_and_the_key_lands(tmp_path):
    """A run killed after its temp write. Catches a rerun that leaves the temp for the scrub."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    target = LakePaths(root).bars_partition_path("GLD", "1d", "2006-04-26")
    target.parent.mkdir(parents=True)
    temp = target.with_name(target.name + ".tmp-4242")
    temp.write_bytes(b"torn")

    summary = _run(root, [source], _listed((source, "GLD")))

    assert not temp.exists()
    assert _bars_files(root) == [_rel("GLD", "2006-04-26")]
    assert summary.tickers[0].temps_removed == [temp.relative_to(root).as_posix()]


class Crash(Exception):
    """A process death injected between two steps. Nothing in the importer catches it."""


def test_a_run_killed_before_its_append_is_completed_by_the_rerun(tmp_path, monkeypatch):
    """Renamed files with no manifest entry. Catches a rerun that refuses them as conflicts, or
    skips them, which would leave them unmanifested for the scrub to page on."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    vintages = _listed((source, "GLD"))

    def crash(*_args, **_kwargs):
        raise Crash

    with monkeypatch.context() as patched:
        patched.setattr(bar_import, "append_entries", crash)
        with pytest.raises(Crash):
            _run(root, [source], vintages)
    assert len(_bars_files(root)) == 2
    assert not any(rel.startswith("bars/") for rel in latest_entries(root))

    summary = _run(root, [source], vintages)

    assert summary.tickers[0].landed == 2
    latest = latest_entries(root)
    for rel in _bars_files(root):
        assert latest[rel]["sha256"] == sha256_file(root / rel)
        assert latest[rel]["source"] == next(iter(vintages.values())).manifest_source


# -- 5. the input checks -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("rows", "header_ticker", "expected"),
    [
        ([("2006-04-26", 1.0), ("2006-04-26", 2.0)], "", "2006-04-26 twice"),
        ([("2006-04-29", 1.0)], "", "2006-04-29 is not a session"),
        ([("2006-04-26", 1.0)], "USO", "names USO"),
    ],
    ids=["duplicate-date", "saturday", "header-ticker"],
)
def test_a_bad_input_refuses_before_anything_is_written(tmp_path, rows, header_ticker, expected):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", rows, header_ticker=header_ticker)
    before = _snapshot(root)

    assert expected in _refused(root, [source], _listed((source, "GLD")))
    assert _snapshot(root) == before


def test_a_header_read_by_position_would_swap_the_columns(tmp_path):
    """The header is read by name. Catches the open and close columns taken by position."""
    root = _lake(tmp_path)
    source = tmp_path / "src" / "gld.csv"
    source.parent.mkdir(parents=True)
    source.write_text(
        "Price,Open,High,Low,Close,Volume\nTicker,GLD,GLD,GLD,GLD,GLD\nDate,,,,,\n"
        "2006-04-26,10.0,12.0,9.0,11.0,5\n"
    )

    _run(root, [source], _listed((source, "GLD")))

    table = load_bars("GLD", "1d", lake_root=root)
    assert table.column("open").to_pylist() == [10.0]
    assert table.column("close").to_pylist() == [11.0]


def test_a_date_past_the_calendars_end_refuses_with_one_line(tmp_path):
    """``exchange_calendars`` ends its calendar a year ahead and raises past it. Catches the
    calendar check run before the span bound, which would end the run in a traceback."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0), ("2031-01-06", 2.0)])

    assert "2031-01-06" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


def test_a_ticker_not_onboarded_refuses_and_names_onboard(tmp_path):
    root = _lake(tmp_path, ("SPY",))
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    message = _refused(root, [source], _listed((source, "GLD")))

    assert "GLD" in message and "lake.onboard" in message
    assert _bars_files(root) == []


def test_an_instrument_with_no_span_refuses(tmp_path):
    """``spans_of`` returns ``()``, which would otherwise admit every date."""
    root = _lake(tmp_path, spans=False)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    assert "no capture span" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


# -- 6. the run guards -------------------------------------------------------------------


def test_the_sunday_scrub_window_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    message = _refused(root, [source], _listed((source, "GLD")), now=SUNDAY_20)

    assert "Sunday" in message and "19:55" in message
    assert _bars_files(root) == []


@pytest.mark.parametrize("now", [MONDAY_10, MONDAY_BOUND], ids=["in-session", "at-the-bound"])
def test_a_session_or_its_pre_open_margin_refuses(tmp_path, now):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    assert "next session" in _refused(root, [source], _listed((source, "GLD")), now=now)
    assert _bars_files(root) == []


def test_the_bound_is_checked_again_once_the_lock_is_taken(tmp_path, monkeypatch):
    """A lock waited on past the bound. Catches a guard run only before the lock."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    clock = ManualClock(MONDAY_0745)
    real = bar_import.lake_lock

    @contextmanager
    def slow_lock(path):
        clock.set(MONDAY_BOUND)
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", slow_lock)

    assert "next session" in _refused(root, [source], _listed((source, "GLD")), now=clock)
    assert _bars_files(root) == []


def test_the_bound_is_checked_again_before_each_tickers_append(tmp_path, monkeypatch):
    """The bound passes while the first ticker's files are written. Catches a check made only
    once, under the lock, which would let a long run hold capture out."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 63.0)])
    uso = _csv(tmp_path / "src" / "uso.csv", [("2006-04-26", 70.0)], ticker="USO")
    clock = ManualClock(MONDAY_0745)
    real = bars._write_partition

    def slow_write(rows, partition, **kwargs):
        written = real(rows, partition, **kwargs)
        clock.set(MONDAY_BOUND)
        return written

    monkeypatch.setattr(bars, "_write_partition", slow_write)

    message = _refused(root, [gld, uso], _listed((gld, "GLD"), (uso, "USO")), now=clock)

    assert "next session" in message
    assert not any(rel.startswith("bars/") for rel in latest_entries(root))


def test_a_lake_root_with_no_manifest_refuses(tmp_path):
    root = _lake(tmp_path)
    (root / "manifest.jsonl").unlink()
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    assert "manifest.jsonl" in _refused(root, [source], _listed((source, "GLD")))
    assert not (root / "manifest.jsonl").exists()


def test_a_source_inside_the_lake_root_refuses(tmp_path):
    """A file there is an orphan to the scrub, and the nightly upload would send it."""
    root = _lake(tmp_path)
    source = _csv(root / "incoming" / "gld.csv", [("2006-04-26", 1.0)])

    assert "inside lake_root" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


def test_a_source_whose_sha256_is_not_listed_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    listed = _listed((source, "GLD"))
    source.write_text(source.read_text().replace("2006-04-26,1.0", "2006-04-26,1.5"))

    message = _refused(root, [source], listed)

    assert hashlib.sha256(source.read_bytes()).hexdigest() in message
    assert _bars_files(root) == []


def test_a_shadow_host_refuses_with_one_line_and_exit_2(tmp_path, monkeypatch, capsys):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    monkeypatch.setattr(bar_import, "VINTAGES", _listed((source, "GLD")))
    config = write_config(tmp_path, root, role="shadow")

    code = bar_import.main(
        ["vintage", str(source), "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
    )

    assert code == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and "shadow" in err[0]
    assert _bars_files(root) == []


def test_a_refusal_reaches_the_operator_as_one_line_and_exit_2(tmp_path, monkeypatch, capsys):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    monkeypatch.setattr(bar_import, "VINTAGES", _listed((source, "GLD")))
    config = write_config(tmp_path, root)

    with pytest.raises(SystemExit) as exited:
        bar_import.main(
            ["vintage", str(source), "--config", str(config)],
            clock=ManualClock(SUNDAY_20),
            calendar=CALENDAR,
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("bar_import: ") and "Sunday" in err[0]


def test_a_source_path_that_is_not_a_file_is_an_argument_error(tmp_path, capsys):
    with pytest.raises(SystemExit) as exited:
        bar_import.main(["vintage", str(tmp_path / "absent.csv")])
    assert exited.value.code == 2
    assert "absent.csv" in capsys.readouterr().err


# -- 7 and 9. provenance on every imported key, one append per ticker --------------------


def test_every_imported_key_reads_back_imported(tmp_path, monkeypatch):
    """Catches a write that forgot ``provenance=``, which would read as captured."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    uso = _csv(tmp_path / "src" / "uso.csv", [("2006-04-26", 3.0)], ticker="USO")
    calls: list[list[str]] = []
    real = bar_import.append_entries

    def counting(lake_root, entries, **kwargs):
        calls.append([entry["partition"] for entry in entries])
        return real(lake_root, entries, **kwargs)

    monkeypatch.setattr(bar_import, "append_entries", counting)

    _run(root, [gld, uso], _listed((gld, "GLD"), (uso, "USO")))

    imported = {
        rel: entry
        for rel, entry in latest_entries(root).items()
        if entry["source"].startswith(bar_import.BAR_IMPORT_SOURCE_PREFIX)
    }
    assert sorted(imported) == [
        _rel("GLD", "2006-04-25"),
        _rel("GLD", "2006-04-26"),
        _rel("USO", "2006-04-26"),
    ]
    for rel, entry in imported.items():
        provenance = read_provenance(pq.read_schema(root / rel).metadata)
        assert provenance.origin == ORIGIN_IMPORTED
        assert f"bar-import:{provenance.source_id}" == entry["source"]
        assert entry["rows"] == 1
    assert calls == [
        [_rel("GLD", "2006-04-25"), _rel("GLD", "2006-04-26")],
        [_rel("USO", "2006-04-26")],
    ]


def test_session_midnight_is_eastern_midnight_in_both_seasons():
    assert bars.session_midnight(date(2026, 7, 1)) == datetime(2026, 7, 1, 4, 0, tzinfo=UTC)
    assert bars.session_midnight(date(2026, 12, 1)) == datetime(2026, 12, 1, 5, 0, tzinfo=UTC)
