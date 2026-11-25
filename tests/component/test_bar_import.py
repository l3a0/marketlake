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

Marketlake #1025 adds Yahoo Finance's files: a header of one row, a window, a list of splits
checked against the file, a volume that lands, and an undo that looks for a killed import's files
every time. Those cases follow that issue's Acceptance list, in their own section at the end.
Their bars are made up, because Yahoo's terms forbid republishing its own.

Every CSV is written here and hashed here with ``hashlib``, so no expected value comes from the
code under test.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from contextlib import contextmanager
from dataclasses import astuple
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake import bar_import, bars
from lake.actions import SWEEP_SOURCE
from lake.bar_import import BarImportRefused, Vintage, import_vintages, undo_vintages
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans, spans_path
from lake.config import BucketTarget
from lake.loader import (
    BASIS_ADJUSTED,
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
from tests.support.bucket import FakeS3, client_error, unreachable
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

# A Monday morning before the bound, a Sunday inside the scrub window,
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
# Unlike the module's own 2012-04-10, so a vintage that hard-codes its date or basis fails.
AS_OF = date(2019, 3, 1)
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


def _vintage(path: Path, ticker: str = "GLD", basis: str = BASIS_ADJUSTED) -> tuple[str, Vintage]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest, Vintage(
        sha256=digest,
        ticker=ticker,
        source=f"test:{path.name}",
        source_as_of=AS_OF,
        basis=basis,
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
    # Literals rather than the loader's constants, which the writer and the reader share.
    assert table.column("origin").to_pylist() == ["imported"] * 3
    assert table.column("basis").to_pylist() == ["adjusted"] * 3
    # The ticker's current instrument, the first and only one ``_lake`` registered.
    assert table.column("instrument_id").to_pylist() == [1, 1, 1]
    assert table.column("source").to_pylist() == ["test:gld.csv"] * 3
    assert table.column("source_as_of").to_pylist() == ["2019-03-01"] * 3
    # Eastern midnight as a UTC instant: 04:00Z under daylight time, 05:00Z under standard time.
    assert table.column("bar_ts").to_pylist() == [
        "2006-04-26T04:00:00+00:00",
        "2006-12-15T05:00:00+00:00",
        "2007-07-16T04:00:00+00:00",
    ]
    assert [bars.daily_session_of(ts) for ts in table.column("bar_ts").to_pylist()] == [
        date(2006, 4, 26),
        date(2006, 12, 15),
        date(2007, 7, 16),
    ]
    (ticker,) = summary.tickers
    assert (ticker.ticker, ticker.landed, ticker.skipped) == ("GLD", 3, 0)


def test_a_session_the_file_lacks_is_counted_and_not_refused(tmp_path):
    """Friday 2006-04-21 to Tuesday 2006-04-25, with Monday the one session missing. Catches a
    missing session refused, not counted, or a weekend day counted as one."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-21", 60.0), ("2006-04-25", 61.0)])

    summary = _run(root, [source], _listed((source, "GLD")))

    (ticker,) = summary.tickers
    assert (ticker.landed, ticker.missing) == (2, 1)
    assert "1 session(s) the calendar names and the file lacks" in summary.render()


def test_the_listed_vintages_carry_the_values_the_issues_reviewed():
    """The module's list matches the tables in #950 and #1025, written out here as literals."""
    gld = "81e6295a8028362f82f9e96a89c686ef89991cb23afe42d4f884756019dc9d4c"
    uso = "94b6521980f64ce80feb5cdffb22168ed44f71eeaa93caca672f316eeda60ec2"
    yahoo_uso = "72bb286f57f637c9ef517b98631a68f7e8f2d7869af4935441b555b11f60d561"
    yahoo_gld = "b6675ed455acf0174854377f8a51061fb18c0b1bf0a9a0f7632ac557b6fcb4f9"
    gap = (date(2012, 4, 10), date(2026, 9, 3))
    assert {digest: astuple(v) for digest, v in bar_import.VINTAGES.items()} == {
        gld: (
            gld,
            "GLD",
            "quantitative-trading:data/inputdata_etf/gld.csv",
            date(2012, 4, 10),
            "adjusted",
            "three-row",
            None,
            None,
        ),
        uso: (
            uso,
            "USO",
            "quantitative-trading:data/inputdata_etf/uso.csv",
            date(2012, 4, 10),
            "adjusted",
            "three-row",
            None,
            None,
        ),
        yahoo_uso: (
            yahoo_uso,
            "USO",
            "yahoo:uso_daily_yahoo_2026-11-22.csv",
            date(2026, 11, 22),
            "adjusted",
            "one-row",
            gap,
            ((date(2020, 4, 29), 0.125),),
        ),
        yahoo_gld: (
            yahoo_gld,
            "GLD",
            "yahoo:gld_daily_yahoo_2026-11-22.csv",
            date(2026, 11, 22),
            "adjusted",
            "one-row",
            gap,
            (),
        ),
    }
    for digest, vintage in bar_import.VINTAGES.items():
        assert vintage.manifest_source == f"bar-import:sha256:{digest}"
        assert vintage.provenance.source_id == f"sha256:{digest}"


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
    assert "captured bars (manifest source 'sweep')" in message
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

    assert held in message and f"the import {other}" in message
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
    # A leftover beside a key this source does not hold, which is evidence for the scrub, not
    # this import's to remove.
    other = target.with_name("date=2006-04-27.parquet.tmp-4242")
    other.write_bytes(b"not mine")

    summary = _run(root, [source], _listed((source, "GLD")))

    assert not temp.exists()
    assert other.read_bytes() == b"not mine"
    assert _bars_files(root) == [_rel("GLD", "2006-04-26"), other.relative_to(root).as_posix()]
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

    assert (summary.tickers[0].landed, summary.tickers[0].replaced) == (2, 2)
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

    message = _refused(root, [source], _listed((source, "GLD")))
    assert expected in message and message.endswith("nothing was imported")
    assert _snapshot(root) == before


_HEADER = "Price,Close,High,Low,Open,Volume\nTicker,GLD,GLD,GLD,GLD,GLD\nDate,,,,,\n"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (_HEADER + "2006-04-26,,2.0,0.5,1.0,5\n", "not a finite number"),
        (_HEADER + "2006-04-26,nan,2.0,0.5,1.0,5\n", "not a finite number"),
        (_HEADER + "2006-04-26,inf,2.0,0.5,1.0,5\n", "not a finite number"),
        (_HEADER + "2006-04-26,1.0\n", "short of its header"),
        (_HEADER, "holds no rows"),
        (_HEADER.replace("Close", "Other"), "names its Close column 0 time(s) in its Price row, "),
        (_HEADER.replace("Volume", "Open"), "names its Open column 2 time(s) in its Price row, "),
        (_HEADER.replace("Volume", "High"), "names its High column 2 time(s) in its Price row, "),
        (_HEADER.replace("Volume", "Low"), "names its Low column 2 time(s) in its Price row, "),
        (
            _HEADER.replace("Volume", "Close"),
            "names its Close column 2 time(s) in its Price row, ",
        ),
        (_HEADER.replace("Date,,,,,\n", "") + "2006-04-26,1.0,2.0,0.5,1.0,5\n", "three header"),
        (
            _HEADER.replace("Ticker,GLD,GLD", "Ticker,GLD,USO") + "2006-04-26,1,2,0,1,5\n",
            "GLD, USO",
        ),
    ],
    ids=[
        "blank",
        "nan",
        "inf",
        "short-line",
        "no-rows",
        "no-close",
        "two-opens",
        "two-highs",
        "two-lows",
        "two-closes",
        "no-date-row",
        "mixed-ticker",
    ],
)
def test_a_malformed_file_refuses_with_one_line(tmp_path, text, expected):
    """An import is write-once, so a non-finite price must never land, and every malformed file
    refuses with one line rather than a traceback."""
    root = _lake(tmp_path)
    source = tmp_path / "src" / "gld.csv"
    source.parent.mkdir(parents=True)
    source.write_text(text)
    before = _snapshot(root)

    message = _refused(root, [source], _listed((source, "GLD")))
    assert expected in message and message.endswith("so nothing was imported")
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

    assert "resolves no instrument for GLD" in message and "lake.onboard" in message
    assert _bars_files(root) == []


def test_a_retired_ticker_refuses_rather_than_landing_with_no_instrument(tmp_path):
    """GLD named an instrument until Saturday 2026-08-29 and has a closed span. Catches the
    onboarded check dropped, which would land rows with a null ``instrument_id``."""
    root = _lake(tmp_path, spans=False)
    start = EVENING_ONBOARD.astimezone(UTC)
    SecurityMaster(
        [Mapping(1, ID_TYPE_TICKER, "GLD", date(2026, 8, 26), date(2026, 8, 29), EQUITY, start)]
    ).write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(1, start, options=False)
    spans.close_span(1, datetime(2026, 8, 28, 21, 0, tzinfo=UTC))
    spans.write(spans_path(root))
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    assert "resolves no instrument for GLD" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


def test_an_onboard_exactly_at_the_close_leaves_that_session_importable(tmp_path):
    """A span opening at 16:00 Wednesday covers none of Wednesday, whose window ends at that
    instant, as ``_span_sessions`` reads it. Catches a non-strict comparison, which would refuse a
    session no path ever lands."""
    root = _lake(tmp_path, onboard=datetime(2026, 8, 26, 16, 0, tzinfo=MARKET_TZ))
    wednesday = _csv(tmp_path / "src" / "wed.csv", [("2026-08-26", 1.0)])
    thursday = _csv(tmp_path / "src" / "thu.csv", [("2026-08-27", 1.0)])
    vintages = _listed((wednesday, "GLD"), (thursday, "GLD"))

    assert "2026-08-27" in _refused(root, [thursday], vintages)
    _run(root, [wednesday], vintages)
    assert _bars_files(root) == [_rel("GLD", "2026-08-26")]


def test_a_raw_basis_source_lands_as_raw(tmp_path):
    """Catches a vintage whose basis is hard-coded rather than read from its listing."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    _run(root, [source], dict([_vintage(source, basis="raw")]))

    assert load_bars("GLD", "1d", lake_root=root).column("basis").to_pylist() == ["raw"]


def test_an_instrument_with_no_span_refuses(tmp_path):
    """``spans_of`` returns ``()``, which would otherwise admit every date."""
    root = _lake(tmp_path, spans=False)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    assert "no capture span" in _refused(root, [source], _listed((source, "GLD")))
    assert _bars_files(root) == []


def test_an_unrecorded_file_another_writer_left_refuses(tmp_path):
    """A file at an import key with no manifest entry and no provenance, which reads as captured.
    Catches the rerun's repair overwriting a file it cannot show is this import's own."""
    root = _lake(tmp_path)
    foreign = write_bars(root, "GLD", "1d", "2006-04-26", [9.0], instrument_id=1)
    before = foreign.read_bytes()
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])

    message = _refused(root, [source], _listed((source, "GLD")))

    assert _rel("GLD", "2006-04-26") in message and "unrecorded" in message
    assert foreign.read_bytes() == before


def test_an_unrecorded_file_another_import_left_refuses(tmp_path, monkeypatch):
    """Import A is killed before its append, and import B, another file over the same sessions,
    runs next. Catches the repair of a killed run overwriting a file whose metadata names another
    source, since only this import's own killed run leaves one it may replace."""
    root = _lake(tmp_path)
    first = _csv(tmp_path / "src" / "a.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    second = _csv(tmp_path / "src" / "b.csv", [("2006-04-25", 3.0), ("2006-04-26", 4.0)])
    vintages = _listed((first, "GLD"), (second, "GLD"))

    def crash(*_args, **_kwargs):
        raise Crash

    with monkeypatch.context() as patched:
        patched.setattr(bar_import, "append_entries", crash)
        with pytest.raises(Crash):
            _run(root, [first], vintages)
    before = _snapshot(root)

    message = _refused(root, [second], vintages)

    owner = hashlib.sha256(first.read_bytes()).hexdigest()
    assert message.startswith(
        f"{_rel('GLD', '2006-04-25')} is an unrecorded file whose metadata names sha256:{owner}"
    )
    assert "2 key(s) conflict, nothing was written" in message
    assert _snapshot(root) == before


def test_a_torn_unrecorded_file_is_replaced(tmp_path):
    """A file no reader can parse is what a torn write leaves, so the rerun replaces it."""
    root = _lake(tmp_path)
    target = LakePaths(root).bars_partition_path("GLD", "1d", "2006-04-26")
    target.parent.mkdir(parents=True)
    target.write_bytes(b"torn")
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])

    summary = _run(root, [source], _listed((source, "GLD")))

    assert (summary.tickers[0].landed, summary.tickers[0].replaced) == (1, 1)
    assert load_bars("GLD", "1d", lake_root=root).column("close").to_pylist() == [2.0]


def test_two_sources_naming_one_ticker_refuse(tmp_path):
    """Catches the second source overwriting the first before either is recorded."""
    root = _lake(tmp_path)
    first = _csv(tmp_path / "src" / "a.csv", [("2006-04-26", 1.0)])
    second = _csv(tmp_path / "src" / "b.csv", [("2006-04-26", 2.0)])

    message = _refused(root, [first, second], _listed((first, "GLD"), (second, "GLD")))

    assert "two sources name GLD" in message
    assert _bars_files(root) == []


def test_a_mid_session_onboard_makes_that_session_capture_covered(tmp_path):
    """An onboard at 13:07 Wednesday covers Wednesday, whose window ends at its 16:00 close.
    Catches the bound read against the session's open rather than its close, which would admit
    a session capture's walk lands."""
    root = _lake(tmp_path, onboard=datetime(2026, 8, 26, 13, 7, tzinfo=MARKET_TZ))
    wednesday = _csv(tmp_path / "src" / "wed.csv", [("2026-08-26", 1.0)])
    tuesday = _csv(tmp_path / "src" / "tue.csv", [("2026-08-25", 1.0)])
    vintages = _listed((wednesday, "GLD"), (tuesday, "GLD"))

    assert "2026-08-26" in _refused(root, [wednesday], vintages)
    _run(root, [tuesday], vintages)
    assert _bars_files(root) == [_rel("GLD", "2026-08-25")]


def test_a_span_that_covers_no_session_sets_no_bound(tmp_path):
    """Opened Friday 2026-08-21 after the close and closed Saturday, so capture covered nothing
    then, and the ticker rejoins Wednesday evening. Catches a bound read from a span's start
    rather than from the first session ``span_covers`` accepts."""
    root = _lake(tmp_path, spans=False)
    master = SecurityMaster.read(master_path(root))
    (instrument,) = master.instrument_ids()
    spans = CaptureSpans()
    spans.open_span(instrument, datetime(2026, 8, 21, 21, 30, tzinfo=UTC), options=False)
    spans.close_span(instrument, datetime(2026, 8, 22, 16, 0, tzinfo=UTC))
    spans.open_span(instrument, EVENING_ONBOARD.astimezone(UTC), options=False)
    spans.write(spans_path(root))
    monday = _csv(tmp_path / "src" / "mon.csv", [("2026-08-24", 1.0), ("2026-08-26", 2.0)])

    _run(root, [monday], _listed((monday, "GLD")))

    assert _bars_files(root) == [_rel("GLD", "2026-08-24"), _rel("GLD", "2026-08-26")]


def test_a_span_covering_no_instant_sets_no_bound(tmp_path):
    """A ``[t, t)`` span on Monday 10:00, then the evening onboard's span. Catches the empty span
    read as covering Monday, which would refuse Tuesday."""
    root = _lake(tmp_path, spans=False)
    master = SecurityMaster.read(master_path(root))
    (instrument,) = master.instrument_ids()
    empty = datetime(2026, 8, 24, 14, 0, tzinfo=UTC)
    spans = CaptureSpans()
    spans.open_span(instrument, empty, options=False)
    spans.close_span(instrument, empty)
    spans.open_span(instrument, EVENING_ONBOARD.astimezone(UTC), options=False)
    spans.write(spans_path(root))
    source = _csv(tmp_path / "src" / "gld.csv", [("2026-08-25", 1.0)])

    _run(root, [source], _listed((source, "GLD")))

    assert _bars_files(root) == [_rel("GLD", "2026-08-25")]


def test_a_dry_run_reports_what_would_land_and_writes_nothing(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    before = _snapshot(root)

    summary = import_vintages(
        root,
        [source],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
        vintages=_listed((source, "GLD")),
        dry_run=True,
    )

    assert _snapshot(root) == before
    assert "GLD: dry run, would land 2 of 2 row(s)" in summary.render()
    assert "Nothing was written" in summary.render()


def test_a_dry_run_still_refuses_a_conflict(tmp_path):
    """The conflicts are what the dry run exists to show before the write."""
    root = _lake(tmp_path)
    _manifest_bars(root, "GLD", "2006-04-26", source=SWEEP_SOURCE)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    before = _snapshot(root)

    with pytest.raises(BarImportRefused, match="captured bars"):
        import_vintages(
            root,
            [source],
            clock=ManualClock(MONDAY_0745),
            calendar=CALENDAR,
            vintages=_listed((source, "GLD")),
            dry_run=True,
        )
    assert _snapshot(root) == before


def test_the_command_builds_the_real_calendar_and_takes_a_dry_run(tmp_path, monkeypatch, capsys):
    """``main`` with no calendar, as the VM runs it, on a lake onboarded at 08:00 on 2026-10-30
    and a clock at 07:45 on Monday 2026-11-02. The dry run lands nothing, then the run lands."""
    root = _lake(tmp_path, onboard=datetime(2026, 10, 30, 8, 0, tzinfo=MARKET_TZ))
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0), ("2026-10-29", 3.0)])
    monkeypatch.setattr(bar_import, "VINTAGES", _listed((source, "GLD")))
    config = write_config(tmp_path, root)
    clock = ManualClock(datetime(2026, 11, 2, 7, 45, tzinfo=MARKET_TZ))
    argv = ["vintage", str(source), "--config", str(config)]

    assert bar_import.main([*argv, "--dry-run"], clock=clock) == 0
    assert _bars_files(root) == []
    assert bar_import.main(argv, clock=clock) == 0

    out = capsys.readouterr().out
    assert "GLD: dry run, would land 2 of 2" in out and "GLD: landed 2 of 2" in out
    assert _bars_files(root) == [_rel("GLD", "2006-04-26"), _rel("GLD", "2026-10-29")]


# -- 6. the run guards -------------------------------------------------------------------


def test_the_sunday_scrub_window_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    message = _refused(root, [source], _listed((source, "GLD")), now=SUNDAY_20)

    assert "Sunday" in message and "19:55" in message
    # The repair names the code's own bound, never the VM's 07:30 start (marketlake #992).
    assert message.endswith(
        "Run it after 23:30, best on a weekday evening once the evening upload after the 18:30 "
        "sweep has finished. Nothing was written"
    )
    assert _bars_files(root) == []


@pytest.mark.parametrize("now", [MONDAY_10, MONDAY_BOUND], ids=["in-session", "at-the-bound"])
def test_a_session_or_its_pre_open_margin_refuses(tmp_path, now):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    message = _refused(root, [source], _listed((source, "GLD")), now=now)

    assert "next session" in message
    assert message.endswith(
        "Run it after that session's close+5, 16:20 on a regular day, best once the evening "
        "upload after the 18:30 sweep has finished. Nothing was written"
    )
    assert _bars_files(root) == []


def test_the_window_is_checked_before_the_lock_is_taken(tmp_path, monkeypatch):
    """Catches a run that takes the lock, which capture waits on, before checking the window."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])

    @contextmanager
    def no_lock(path):
        raise AssertionError("the lock was taken inside the refused window")
        yield

    monkeypatch.setattr(bar_import, "lake_lock", no_lock)

    assert "Sunday" in _refused(root, [source], _listed((source, "GLD")), now=SUNDAY_20)


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
    target = LakePaths(root).bars_partition_path("GLD", "1d", "2006-04-26")
    target.parent.mkdir(parents=True)
    temp = target.with_name(target.name + ".tmp-4242")
    temp.write_bytes(b"torn")

    assert "next session" in _refused(root, [source], _listed((source, "GLD")), now=clock)
    # The leftover survives, so the refusal came before the repairs, not at the first write.
    assert temp.read_bytes() == b"torn"
    assert _bars_files(root) == [temp.relative_to(root).as_posix()]


def test_a_stop_part_way_through_a_ticker_records_what_it_wrote(tmp_path, monkeypatch):
    """The bound passes after the first of GLD's two writes. Catches a window checked only once
    per ticker, which would hold capture out, and a stop that leaves the written file with no
    manifest entry for the scrub to page on."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    clock = ManualClock(MONDAY_0745)
    real = bars._write_partition

    def slow_write(rows, partition, **kwargs):
        written = real(rows, partition, **kwargs)
        clock.set(MONDAY_BOUND)
        return written

    monkeypatch.setattr(bars, "_write_partition", slow_write)

    message = _refused(root, [source], _listed((source, "GLD")), now=clock)

    assert "next session" in message
    assert "1 partition(s) were recorded before this stop, and a rerun picks up the rest" in message
    assert _bars_files(root) == [_rel("GLD", "2006-04-25")]
    assert [rel for rel in latest_entries(root) if rel.startswith("bars/")] == _bars_files(root)


def test_the_bound_is_checked_before_the_next_tickers_writes(tmp_path, monkeypatch):
    """The bound passes as GLD's append lands. Catches a run that goes on to write USO."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 63.0)])
    uso = _csv(tmp_path / "src" / "uso.csv", [("2006-04-26", 70.0)], ticker="USO")
    clock = ManualClock(MONDAY_0745)
    real = bar_import.append_entries

    def late_append(lake_root, entries, **kwargs):
        appended = real(lake_root, entries, **kwargs)
        clock.set(MONDAY_BOUND)
        return appended

    monkeypatch.setattr(bar_import, "append_entries", late_append)

    message = _refused(root, [gld, uso], _listed((gld, "GLD"), (uso, "USO")), now=clock)

    assert "next session" in message and "1 partition(s) were recorded" in message
    assert _bars_files(root) == [_rel("GLD", "2006-04-26")]


def test_a_failed_write_records_what_landed_before_it(tmp_path, monkeypatch):
    """Catches a write failure that leaves the earlier files of the ticker unrecorded."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-25", 1.0), ("2006-04-26", 2.0)])
    real = bars._write_partition
    calls = []

    def failing_write(rows, partition, **kwargs):
        calls.append(partition)
        if len(calls) == 2:
            raise OSError(28, "No space left on device")
        return real(rows, partition, **kwargs)

    monkeypatch.setattr(bars, "_write_partition", failing_write)

    message = _refused(root, [source], _listed((source, "GLD")))

    assert "No space left on device" in message and "1 partition(s) were recorded" in message
    assert _bars_files(root) == [_rel("GLD", "2006-04-25")]
    assert [rel for rel in latest_entries(root) if rel.startswith("bars/")] == _bars_files(root)


def test_the_directories_are_flushed_before_the_manifest_names_the_files(tmp_path, monkeypatch):
    """Catches the flush dropped, which a power loss would turn into manifested missing files."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    events: list[str] = []
    real_fsync, real_append = bar_import._fsync_path, bar_import.append_entries

    def fsync(path):
        events.append(Path(path).relative_to(root).as_posix())
        real_fsync(path)

    def append(lake_root, entries, **kwargs):
        events.append("append")
        return real_append(lake_root, entries, **kwargs)

    monkeypatch.setattr(bar_import, "_fsync_path", fsync)
    monkeypatch.setattr(bar_import, "append_entries", append)

    _run(root, [source], _listed((source, "GLD")))

    assert events == ["bars/ticker=GLD/freq=1d", "bars/ticker=GLD", "bars", "append"]


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
        assert provenance.origin == "imported"
        assert f"bar-import:{provenance.source_id}" == entry["source"]
        assert entry["rows"] == 1
    assert calls == [
        [_rel("GLD", "2006-04-25"), _rel("GLD", "2006-04-26")],
        [_rel("USO", "2006-04-26")],
    ]


def test_session_midnight_is_eastern_midnight_in_both_seasons():
    assert bars.session_midnight(date(2026, 7, 1)) == datetime(2026, 7, 1, 4, 0, tzinfo=UTC)
    assert bars.session_midnight(date(2026, 12, 1)) == datetime(2026, 12, 1, 5, 0, tzinfo=UTC)


# -- the undo, by the owner's decision of 2026-11-01 -------------------------------------

UNDO_TARGET = BucketTarget(bucket="lake-backup", prefix="lake")


def _undo(root: Path, sources, vintages, *, client=None, now=MONDAY_0745, calendar=CALENDAR):
    return undo_vintages(
        root,
        list(sources),
        client=client if client is not None else FakeS3(),
        target=UNDO_TARGET,
        clock=now if isinstance(now, ManualClock) else ManualClock(now),
        calendar=calendar,
        vintages=vintages,
    )


def _undo_refused(root: Path, sources, vintages, **kwargs) -> str:
    with pytest.raises(BarImportRefused) as refused:
        _undo(root, sources, vintages, **kwargs)
    message = str(refused.value)
    assert "\n" not in message
    return message


def _imported(tmp_path: Path, tickers=("GLD",)):
    """A lake, its files and manifest before any import, and the import landed on top."""
    root = _lake(tmp_path, tickers)
    sources = [
        _csv(
            tmp_path / "src" / f"{t.lower()}.csv",
            [("2006-04-25", 1.0), ("2006-04-26", 2.0)],
            ticker=t,
        )
        for t in tickers
    ]
    vintages = _listed(*zip(sources, tickers, strict=True))
    before = _snapshot(root)
    _run(root, sources, vintages)
    return root, sources, vintages, before


def test_an_undo_right_after_an_import_leaves_the_lake_as_it_was(tmp_path):
    """Catches an undo that leaves a file, a temp, or a manifest line behind, or cuts too much."""
    root, sources, vintages, before = _imported(tmp_path, ("GLD", "USO"))
    temp = LakePaths(root).bars_partition_path("GLD", "1d", "2006-04-26")
    leftover = temp.with_name(temp.name + ".tmp-4242")
    leftover.write_bytes(b"torn")
    client = FakeS3()
    # The bucket holds the lake as it stood before the import, as the last upload left it.
    client.store(UNDO_TARGET.key("manifest.jsonl"), before["manifest.jsonl"])

    summary = _undo(root, sources, vintages, client=client)

    assert _snapshot(root) == before
    assert (summary.lines, summary.partitions) == (4, 4)
    assert summary.temps_removed == [leftover.relative_to(root).as_posix()]
    assert "undid 4 manifest line(s) and removed 4 partition(s)" in summary.render()


def test_an_undo_with_no_bucket_copy_at_all_proceeds(tmp_path):
    root, sources, vintages, before = _imported(tmp_path)

    _undo(root, sources, vintages)

    assert _snapshot(root) == before


def test_an_undo_with_nothing_to_undo_changes_nothing(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    before = _snapshot(root)

    client = FakeS3()
    summary = _undo(root, [source], _listed((source, "GLD")), client=client)

    assert summary.lines == 0 and "nothing to undo" in summary.render()
    assert _snapshot(root) == before
    # Nothing to undo reads no bucket, so an outage cannot refuse it.
    assert client.calls == []


def test_an_entry_recorded_after_the_import_refuses_the_undo(tmp_path):
    """A capture cycle's entry after the import. Catches a cut that would take it too."""
    root, sources, vintages, _before = _imported(tmp_path)
    _manifest_bars(root, "GLD", "2026-08-31", source=SWEEP_SOURCE)
    after = _snapshot(root)

    message = _undo_refused(root, sources, vintages)

    assert "something else was recorded after it" in message
    assert _snapshot(root) == after


def test_a_bucket_copy_holding_the_import_refuses_the_undo(tmp_path):
    """The nightly upload sent the import. Catches an undo that would leave the bucket's
    manifest longer than the lake's, which stops every later upload."""
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()
    client.store(UNDO_TARGET.key("manifest.jsonl"), (root / "manifest.jsonl").read_bytes())
    after = _snapshot(root)

    message = _undo_refused(root, sources, vintages, client=client)

    assert "may already hold the import" in message
    assert _snapshot(root) == after


def test_a_bucket_object_at_an_import_key_refuses_the_undo(tmp_path):
    """An upload that stopped after an object and before its manifest. Catches an undo that
    would leave that object past the watermark for the Sunday backup scrub."""
    root, sources, vintages, before = _imported(tmp_path)
    client = FakeS3()
    client.store(UNDO_TARGET.key("manifest.jsonl"), before["manifest.jsonl"])
    rel = _rel("GLD", "2006-04-26")
    client.store(UNDO_TARGET.key(rel), (root / rel).read_bytes())
    after = _snapshot(root)

    message = _undo_refused(root, sources, vintages, client=client)

    assert rel in message and "already holds" in message
    assert _snapshot(root) == after


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (unreachable(), "unreachable"),
        (client_error("AccessDenied", "HeadObject", 403), "refused with AccessDenied"),
    ],
    ids=["unreachable", "refused"],
)
def test_a_bucket_that_cannot_be_read_refuses_the_undo(tmp_path, failure, expected):
    """Each failure names its kind, so a credential refusal and an outage read differently."""
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()
    client.fail_with = failure
    after = _snapshot(root)

    message = _undo_refused(root, sources, vintages, client=client)

    assert expected in message and "nothing was undone" in message
    assert _snapshot(root) == after


# The undo's deadline after a stop at 20:00 on Monday 2026-08-31, with Tuesday's bound written out.
TUESDAY_DEADLINE = (
    "before any compaction uploads the import, and before 2026-09-01T09:00:00-04:00 at the latest"
)


@pytest.mark.parametrize(
    ("calendar", "deadline"),
    [
        (CALENDAR, TUESDAY_DEADLINE),
        (
            weekday_sessions(date(2026, 8, 24)),
            "before any compaction uploads the import, and before the next session's capture "
            "start at the latest",
        ),
    ],
    ids=["next-session-day", "no-session-ahead"],
)
def test_an_evening_undo_names_the_next_session_days_bound(tmp_path, calendar, deadline):
    """Catches a deadline written as a clock time, or as the bound of the day the undo ran.

    Monday's bound is 09:00 that morning, already past at 20:00, so the line must name
    Tuesday's. With no session in the calendar's lookahead it names none.
    """
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()
    client.fail_with = unreachable()
    evening = datetime(2026, 8, 31, 20, 0, tzinfo=MARKET_TZ)

    message = _undo_refused(root, sources, vintages, client=client, now=evening, calendar=calendar)

    assert f"Run the undo again at once, {deadline}: " in message


@pytest.mark.parametrize(
    ("failure", "repair"),
    [
        (unreachable(), "Run the undo again at once, "),
        (
            client_error("AccessDenied", "HeadObject", 403),
            "Fix the instance profile's bucket policy, then run the undo again ",
        ),
        (client_error("NoSuchBucket", "HeadObject", 404), "Run the undo again "),
    ],
    ids=["unreachable", "refused", "failed"],
)
def test_every_bucket_failure_names_the_deadline(tmp_path, failure, repair):
    """Catches a deadline dropped from one kind's repair, each of which says to run it again."""
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()
    client.fail_with = failure
    evening = datetime(2026, 8, 31, 20, 0, tzinfo=MARKET_TZ)

    message = _undo_refused(root, sources, vintages, client=client, now=evening)

    assert f". {repair}{TUESDAY_DEADLINE}: " in message


def test_a_bucket_read_failing_under_the_lock_names_the_deadline(tmp_path, monkeypatch):
    """Catches the second bucket read, under the lock, wired to another deadline."""
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()
    real = bar_import.lake_lock

    @contextmanager
    def failing_after_lock(path):
        with real(path):
            client.fail_with = unreachable()
            yield

    monkeypatch.setattr(bar_import, "lake_lock", failing_after_lock)
    evening = datetime(2026, 8, 31, 20, 0, tzinfo=MARKET_TZ)

    message = _undo_refused(root, sources, vintages, client=client, now=evening)

    assert f"Run the undo again at once, {TUESDAY_DEADLINE}: " in message


def test_an_undo_stopped_after_the_deletions_is_finished_by_a_rerun(tmp_path, monkeypatch):
    """Catches a cut made before the deletions, which a crash would turn into files nothing
    records, and a rerun that refuses manifested files already gone."""
    root, sources, vintages, before = _imported(tmp_path)

    def crash(*_args, **_kwargs):
        raise Crash

    with monkeypatch.context() as patched:
        patched.setattr(bar_import._bucket, "_rewrite_manifest", crash)
        with pytest.raises(Crash):
            _undo(root, sources, vintages)
    assert _bars_files(root) == []
    assert (root / "manifest.jsonl").read_bytes() != before["manifest.jsonl"]

    summary = _undo(root, sources, vintages)

    assert (summary.lines, summary.partitions) == (2, 0)
    assert _snapshot(root) == before


def test_the_undo_refuses_in_the_run_window(tmp_path):
    root, sources, vintages, _before = _imported(tmp_path)
    after = _snapshot(root)

    assert "Sunday" in _undo_refused(root, sources, vintages, now=SUNDAY_20)
    late = _undo_refused(root, sources, vintages, now=MONDAY_10)
    # Past the bound no undo can run before capture records a minute, so the line never says
    # to try again that day.
    assert "the undo stops at" in late and "the import stays" in late
    assert "close+5" not in late
    assert _snapshot(root) == after


def test_the_undo_checks_the_window_before_it_reads_the_bucket(tmp_path):
    """Catches a window checked only under the lock, after the bucket's requests."""
    root, sources, vintages, _before = _imported(tmp_path)
    client = FakeS3()

    assert "Sunday" in _undo_refused(root, sources, vintages, client=client, now=SUNDAY_20)
    assert client.calls == []


def test_the_undo_checks_the_window_again_once_the_lock_is_taken(tmp_path, monkeypatch):
    """A lock waited on past the bound. Catches a window checked only before the lock."""
    root, sources, vintages, _before = _imported(tmp_path)
    clock = ManualClock(MONDAY_0745)
    real = bar_import.lake_lock

    @contextmanager
    def slow_lock(path):
        clock.set(MONDAY_BOUND)
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", slow_lock)
    after = _snapshot(root)

    assert "next session" in _undo_refused(root, sources, vintages, now=clock)
    assert _snapshot(root) == after


def test_a_manifest_that_changed_before_the_lock_refuses_the_undo(tmp_path, monkeypatch):
    """A capture cycle appends while the undo reads the bucket. Catches a cut made from the
    bytes read before the lock."""
    root, sources, vintages, _before = _imported(tmp_path)
    real = bar_import.lake_lock

    @contextmanager
    def busy_lock(path):
        _manifest_bars(root, "GLD", "2026-08-31", source=SWEEP_SOURCE)
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", busy_lock)
    message = _undo_refused(root, sources, vintages)

    assert "no longer its last ones" in message
    assert len(_bars_files(root)) == 3


def test_a_torn_manifest_tail_refuses_the_undo(tmp_path):
    root, sources, vintages, _before = _imported(tmp_path)
    with (root / "manifest.jsonl").open("ab") as handle:
        handle.write(b'{"partition": "torn')
    after = _snapshot(root)

    assert _undo_refused(root, sources, vintages) == (
        "manifest.jsonl ends in a torn line, so the undo cannot tell where the import starts and "
        "nothing was undone"
    )
    assert _snapshot(root) == after


def test_the_undo_command_reads_the_bucket_from_the_config(tmp_path, monkeypatch, capsys):
    root, sources, vintages, before = _imported(tmp_path)
    monkeypatch.setattr(bar_import, "VINTAGES", vintages)
    monkeypatch.setattr(bar_import, "connect", lambda config: (UNDO_TARGET, FakeS3()))
    config = write_config(tmp_path, root)

    code = bar_import.main(
        ["undo", *map(str, sources), "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
    )

    assert code == 0
    assert "bar_import: undid 2 manifest line(s)" in capsys.readouterr().out
    assert _snapshot(root) == before


def test_an_upload_while_the_undo_waits_for_the_lock_refuses_it(tmp_path, monkeypatch):
    """An upload that sends the import holds the lock and appends nothing to this lake's
    manifest. Catches a bucket read made only before the lock, which would cut the lake while the
    bucket keeps serving the import and stops every later upload."""
    root, sources, vintages, before = _imported(tmp_path)
    client = FakeS3()
    client.store(UNDO_TARGET.key("manifest.jsonl"), before["manifest.jsonl"])
    real = bar_import.lake_lock

    @contextmanager
    def upload_then_lock(path):
        for rel in _bars_files(root):
            client.store(UNDO_TARGET.key(rel), (root / rel).read_bytes())
        client.store(UNDO_TARGET.key("manifest.jsonl"), (root / "manifest.jsonl").read_bytes())
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", upload_then_lock)
    after = _snapshot(root)

    message = _undo_refused(root, sources, vintages, client=client)

    assert "nothing was undone" in message.lower()
    assert _snapshot(root) == after


def test_a_bucket_copy_that_changed_while_the_undo_waited_refuses_it(tmp_path, monkeypatch):
    """The bucket's copy is first absent and then appears, still a prefix. Catches a second read
    that checks the listing and not the copy itself."""
    root, sources, vintages, before = _imported(tmp_path)
    client = FakeS3()
    real = bar_import.lake_lock

    @contextmanager
    def first_upload_then_lock(path):
        client.store(UNDO_TARGET.key("manifest.jsonl"), before["manifest.jsonl"])
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", first_upload_then_lock)
    after = _snapshot(root)

    assert "changed while the undo waited" in _undo_refused(root, sources, vintages, client=client)
    assert _snapshot(root) == after


def test_a_stopped_undo_past_an_append_is_repaired_by_the_import(tmp_path, monkeypatch):
    """The undo stops after its deletions, and capture records a minute before anyone reruns it.
    No undo can finish that, so the import's rerun writes the missing files back. Catches a rerun
    that skips them, which leaves the nightly upload stopping on missing files for good."""
    root, sources, vintages, _before = _imported(tmp_path)

    def crash(*_args, **_kwargs):
        raise Crash

    with monkeypatch.context() as patched:
        patched.setattr(bar_import._bucket, "_rewrite_manifest", crash)
        with pytest.raises(Crash):
            _undo(root, sources, vintages)
    _manifest_bars(root, "GLD", "2026-08-31", source=SWEEP_SOURCE)
    assert "recorded after it" in _undo_refused(root, sources, vintages)

    summary = _run(root, sources, vintages)

    (ticker,) = summary.tickers
    assert (ticker.landed, ticker.rewritten, ticker.skipped) == (2, 2, 0)
    latest = latest_entries(root)
    for rel, entry in latest.items():
        if rel.startswith("bars/"):
            assert entry["sha256"] == sha256_file(root / rel)


def test_an_undo_after_a_killed_import_says_to_rerun_the_import(tmp_path, monkeypatch):
    """Renamed files with no entry, which the manifest cannot show the undo. Catches an undo that
    reports nothing to undo while those files wait for the upload."""
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    vintages = _listed((source, "GLD"))

    def crash(*_args, **_kwargs):
        raise Crash

    with monkeypatch.context() as patched:
        patched.setattr(bar_import, "append_entries", crash)
        with pytest.raises(Crash):
            _run(root, [source], vintages)

    assert "Run the import again" in _undo_refused(root, [source], vintages)
    assert _bars_files(root) == [_rel("GLD", "2006-04-26")]


def test_an_undo_that_would_empty_the_manifest_refuses(tmp_path):
    """An empty manifest reads as a prefix of any bucket copy, which the resync's rewrite never
    leaves. Catches a cut to zero bytes."""
    root = _lake(tmp_path)
    (root / "manifest.jsonl").write_bytes(b"")
    source = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 2.0)])
    vintages = _listed((source, "GLD"))
    _run(root, [source], vintages)
    after = _snapshot(root)

    assert "leave it empty" in _undo_refused(root, [source], vintages)
    assert _snapshot(root) == after


def test_an_undo_of_an_unlisted_file_refuses(tmp_path):
    root, sources, vintages, _before = _imported(tmp_path)
    stray = _csv(tmp_path / "src" / "stray.csv", [("2006-04-27", 9.0)])
    after = _snapshot(root)

    assert "no vintage this module lists" in _undo_refused(root, [stray], vintages)
    assert _snapshot(root) == after


def test_an_undo_on_a_shadow_host_refuses_with_exit_2(tmp_path, monkeypatch, capsys):
    root, sources, vintages, _before = _imported(tmp_path)
    monkeypatch.setattr(bar_import, "VINTAGES", vintages)
    monkeypatch.setattr(bar_import, "connect", lambda config: (UNDO_TARGET, FakeS3()))
    config = write_config(tmp_path, root, role="shadow")
    after = _snapshot(root)

    code = bar_import.main(
        ["undo", *map(str, sources), "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
    )

    assert code == 2
    assert "shadow" in capsys.readouterr().err
    assert _snapshot(root) == after


def test_an_undo_removes_only_the_named_import(tmp_path):
    """GLD then USO, and the undo names only USO, whose lines come last. Catches a tail walk that
    takes any import's lines rather than these imports' own."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    uso = _csv(
        tmp_path / "src" / "uso.csv", [("2006-04-25", 2.0), ("2006-04-26", 3.0)], ticker="USO"
    )
    _run(root, [gld], _listed((gld, "GLD")))
    after_gld = _snapshot(root)
    _run(root, [uso], _listed((uso, "USO")))

    summary = _undo(root, [uso], _listed((uso, "USO")))

    assert summary.lines == 2
    assert _snapshot(root) == after_gld


def test_an_undo_with_no_manifest_refuses(tmp_path):
    root, sources, vintages, _before = _imported(tmp_path)
    (root / "manifest.jsonl").unlink()
    after = _snapshot(root)

    assert "holds no manifest.jsonl" in _undo_refused(root, sources, vintages)
    assert _snapshot(root) == after


def test_the_undo_flushes_the_directories_before_it_cuts_the_manifest(tmp_path, monkeypatch):
    """Catches a cut made before the deletions are durable, which a power loss would turn into
    files nothing records."""
    root, sources, vintages, _before = _imported(tmp_path)
    events: list[str] = []
    real_fsync, real_cut = bar_import._fsync_path, bar_import._bucket._rewrite_manifest

    def fsync(path):
        events.append(Path(path).relative_to(root).as_posix())
        real_fsync(path)

    def cut(*args, **kwargs):
        events.append("cut")
        return real_cut(*args, **kwargs)

    monkeypatch.setattr(bar_import, "_fsync_path", fsync)
    monkeypatch.setattr(bar_import._bucket, "_rewrite_manifest", cut)

    _undo(root, sources, vintages)

    assert events == ["bars", "bars/ticker=GLD", "bars/ticker=GLD/freq=1d", "cut"]


def _append_line(root: Path, line: bytes) -> None:
    with (root / "manifest.jsonl").open("ab") as handle:
        handle.write(line + b"\n")


def test_a_tail_entry_outside_bars_refuses_the_undo(tmp_path):
    root, sources, vintages, _before = _imported(tmp_path)
    identity = next(iter(vintages.values())).manifest_source
    entry = {
        "partition": "reference/odd.parquet",
        "source": identity,
        "sha256": "0" * 64,
        "rows": 1,
        "fetched_at": None,
    }
    _append_line(root, json.dumps(entry).encode())
    after = _snapshot(root)

    assert "not a bars partition" in _undo_refused(root, sources, vintages)
    assert _snapshot(root) == after


def test_a_tail_key_recorded_before_the_import_refuses_the_undo(tmp_path):
    """The cut would make that earlier entry the latest again, for a file the undo deletes.
    Catches an undo that relies on the import never writing such a key."""
    root, sources, vintages, _before = _imported(tmp_path)
    rel = _rel("GLD", "2006-04-26")
    lines = (root / "manifest.jsonl").read_bytes().splitlines()
    first = [line for line in lines if rel.encode() in line][0]
    earlier = json.loads(first)
    earlier["source"] = SWEEP_SOURCE
    # The earlier entry goes before every import line, as a captured bar would have.
    head = [line for line in lines if b"bar-import:" not in line]
    tail = [line for line in lines if b"bar-import:" in line]
    (root / "manifest.jsonl").write_bytes(
        b"\n".join([*head, json.dumps(earlier).encode(), *tail]) + b"\n"
    )
    after = _snapshot(root)

    assert "before this import too" in _undo_refused(root, sources, vintages)
    assert _snapshot(root) == after


@pytest.mark.parametrize(
    ("line", "expected"),
    [(b'{"partition": "x"}', "not a whole entry"), (b"garbage", "not a whole entry")],
    ids=["no-source", "not-json"],
)
def test_a_manifest_line_that_is_not_an_entry_refuses_the_undo(tmp_path, line, expected):
    root, sources, vintages, _before = _imported(tmp_path)
    raw = (root / "manifest.jsonl").read_bytes()
    (root / "manifest.jsonl").write_bytes(line + b"\n" + raw)
    after = _snapshot(root)

    assert expected in _undo_refused(root, sources, vintages)
    assert _snapshot(root) == after


def test_an_empty_manifest_has_nothing_to_undo(tmp_path):
    """Catches the torn-line check reading an empty manifest as torn."""
    root = _lake(tmp_path)
    (root / "manifest.jsonl").write_bytes(b"")
    source = _csv(tmp_path / "src" / "gld.csv", [("2026-01-05", 1.0)])

    assert _undo(root, [source], _listed((source, "GLD"))).lines == 0


def test_a_failed_deletion_refuses_with_one_line(tmp_path, monkeypatch):
    root, sources, vintages, _before = _imported(tmp_path)
    real = Path.unlink

    def refuse(self, *args, **kwargs):
        if self.suffix == ".parquet":
            raise PermissionError(13, "Permission denied")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", refuse)

    message = _undo_refused(root, sources, vintages)

    assert "Permission denied" in message
    assert (
        "Run the undo again before any compaction uploads the import, and before "
        f"{MONDAY_BOUND.isoformat()} at the latest. It finishes" in message
    )


def test_a_rewrite_that_does_not_read_back_names_the_bucket_copy(tmp_path, monkeypatch):
    root, sources, vintages, _before = _imported(tmp_path)

    def fails(*_args, **_kwargs):
        raise bar_import._bucket.ResyncRefused("the read-back differs")

    monkeypatch.setattr(bar_import._bucket, "_rewrite_manifest", fails)

    assert "Copy the bucket's manifest.jsonl over it" in _undo_refused(root, sources, vintages)


def test_a_bucket_setting_refusal_reaches_the_operator_as_one_line(tmp_path, monkeypatch, capsys):
    root, sources, vintages, _before = _imported(tmp_path)
    monkeypatch.setattr(bar_import, "VINTAGES", vintages)

    def no_client(config):
        raise bar_import._bucket.BucketSettingsInvalid("bucket_region is not set")

    monkeypatch.setattr(bar_import, "connect", no_client)
    config = write_config(tmp_path, root)

    with pytest.raises(SystemExit) as exited:
        bar_import.main(
            ["undo", *map(str, sources), "--config", str(config)],
            clock=ManualClock(MONDAY_0745),
            calendar=CALENDAR,
        )

    assert exited.value.code == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and "bucket_region is not set" in err[0]


# -- the one-row layout, the window and the splits (marketlake #1025) ----------------------

ONE_ROW_NAMES = (
    "Date",
    "Adj Close",
    "Capital Gains",
    "Close",
    "Dividends",
    "High",
    "Low",
    "Open",
    "Stock Splits",
    "Volume",
)
ONE_ROW = ",".join(ONE_ROW_NAMES)
# Four sessions of 2012, Tuesday to Friday, with a session either side.
WINDOW = (date(2012, 4, 10), date(2012, 4, 13))
BEFORE, TUE, WED, THU, FRI, AFTER = (
    "2012-04-09",
    "2012-04-10",
    "2012-04-11",
    "2012-04-12",
    "2012-04-13",
    "2012-04-16",
)
FULL = (BEFORE, TUE, WED, THU, FRI, AFTER)
SPLIT = ((date(2012, 4, 12), 0.125),)


def _cells(day: str, n: int, **override: str) -> dict[str, str]:
    """One made-up row. No two columns hold the same number, and volume is bare digits."""
    close = 40.123456 + n
    row = {
        "Date": day,
        "Adj Close": f"{close * 0.9:.6f}",
        "Capital Gains": "0.001",
        "Close": f"{close:.6f}",
        "Dividends": "0.002",
        "High": f"{close + 1.25:.6f}",
        "Low": f"{close - 1.5:.6f}",
        "Open": f"{close - 0.75:.6f}",
        "Stock Splits": "0.0",
        "Volume": str(1_000_300 + n),
    }
    row.update({key.replace("_", " "): value for key, value in override.items()})
    return row


def _one_row(path: Path, rows: list[dict[str, str]], names=ONE_ROW_NAMES) -> Path:
    lines = [",".join(names), *(",".join(row[name] for name in names) for row in rows)]
    return _text(path, "\n".join(lines) + "\n")


def _text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _days(*days: str, **by_day) -> list[dict[str, str]]:
    """A row per day, ``d12={"Volume": "1"}`` overriding the cells of the 12th's."""
    return [_cells(day, n, **by_day.get("d" + day[-2:], {})) for n, day in enumerate(days)]


def _entry(
    path: Path, ticker: str = "GLD", *, splits=(), window=WINDOW, layout=bar_import.LAYOUT_ONE_ROW
) -> dict[str, Vintage]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        digest: Vintage(
            sha256=digest,
            ticker=ticker,
            source=f"yahoo:{path.name}",
            source_as_of=AS_OF,
            basis=BASIS_ADJUSTED,
            layout=layout,
            window=window,
            splits=splits,
        )
    }


def _dry(root: Path, sources, vintages) -> str:
    return import_vintages(
        root,
        list(sources),
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
        vintages=vintages,
        dry_run=True,
    ).render()


def _manifest_lines(root: Path) -> list[bytes]:
    return (root / "manifest.jsonl").read_bytes().splitlines()


def test_only_the_rows_inside_the_window_land_with_their_volume(tmp_path):
    """#1025 acceptance 1. Catches a bound off by one, a column taken by position or from
    ``Adj Close``, a rounded price, and a volume dropped."""
    root = _lake(tmp_path)
    other = "bar-import:sha256:" + "cd" * 32
    outer = [_manifest_bars(root, "GLD", day, source=other) for day in (BEFORE, AFTER)]
    before = _snapshot(root)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    vintages = _entry(source)

    dry = _dry(root, [source], vintages)
    assert dry.endswith(", 2 row(s) outside the window. Nothing was written")
    assert _snapshot(root) == before
    summary = _run(root, [source], vintages)
    assert summary.render().endswith("found missing and written again, 2 row(s) outside the window")

    table = load_bars("GLD", "1d", TUE, FRI, lake_root=root, adjust="none")
    assert table.column("close").to_pylist() == [41.123456, 42.123456, 43.123456, 44.123456]
    assert table.column("high").to_pylist() == [42.373456, 43.373456, 44.373456, 45.373456]
    assert table.column("low").to_pylist() == [39.623456, 40.623456, 41.623456, 42.623456]
    assert table.column("open").to_pylist() == [40.373456, 41.373456, 42.373456, 43.373456]
    assert table.column("volume").to_pylist() == [1000301, 1000302, 1000303, 1000304]
    assert table.column("origin").to_pylist() == ["imported"] * 4
    assert table.column("basis").to_pylist() == ["adjusted"] * 4
    assert table.column("source").to_pylist() == ["yahoo:gld.csv"] * 4
    assert table.column("source_as_of").to_pylist() == ["2019-03-01"] * 4
    assert table.column("bar_ts").to_pylist() == [
        f"{day}T04:00:00+00:00" for day in (TUE, WED, THU, FRI)
    ]
    after = _snapshot(root)
    assert {rel: after[rel] for rel in outer} == {rel: before[rel] for rel in outer}
    assert after["manifest.jsonl"].startswith(before["manifest.jsonl"])
    assert len(_manifest_lines(root)) == len(before["manifest.jsonl"].splitlines()) + 4


@pytest.mark.parametrize("cell", ["57952", "0", "", "n/a"])
def test_a_three_row_files_volume_still_lands_null(tmp_path, cell):
    """#1025 acceptance 2. Catches a volume read in #950's layout, whose unit is unverified."""
    root = _lake(tmp_path)
    source = _text(tmp_path / "src" / "gld.csv", _HEADER + f"2006-04-26,1.0,2.0,0.5,1.5,{cell}\n")
    digest, vintage = _vintage(source)

    summary = _run(root, [source], {digest: vintage})

    rel = _rel("GLD", "2006-04-26")
    assert pq.read_table(root / rel).column("volume").to_pylist() == [None]
    assert read_provenance(pq.read_schema(root / rel).metadata) == Provenance(
        "imported", "adjusted", "test:gld.csv", f"sha256:{digest}", date(2019, 3, 1)
    )
    assert latest_entries(root)[rel]["source"] == f"bar-import:sha256:{digest}"
    assert "outside" not in summary.render()
    assert "outside" not in _dry(root, [source], {digest: vintage})


def test_a_three_row_entry_with_a_window_drops_the_rows_outside_it(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [(day, 10.0 + n) for n, day in enumerate(FULL)])
    vintages = _entry(source, splits=None, layout=bar_import.LAYOUT_THREE_ROW)

    summary = _run(root, [source], vintages)

    assert _bars_files(root) == [_rel("GLD", day) for day in (TUE, WED, THU, FRI)]
    assert summary.render().endswith(", 2 row(s) outside the window")


def test_a_three_row_files_prices_are_read_inside_its_window_only(tmp_path):
    """A row outside the window lands nothing, so its price cells are not judged. The row inside
    it is longer than its header, which this layout has always landed."""
    root = _lake(tmp_path)
    rows = f"{BEFORE},nan,2.0,0.5,1.5,5\n{TUE},1.0,2.0,0.5,1.5,5,x,x\n"
    source = _text(tmp_path / "src" / "gld.csv", _HEADER + rows)
    vintages = _entry(source, splits=None, layout="three-row")

    assert _run(root, [source], vintages).tickers[0].landed == 1


# -- the bytes hashed are the bytes parsed


class _Swap:
    """Counts the opens of one file and puts the next of ``versions`` in it before each."""

    def __init__(self, monkeypatch, path: Path, versions: list[bytes]) -> None:
        self.opens = 0
        real = io.open

        def swapped(file, *args, **kwargs):
            if isinstance(file, (str, os.PathLike)) and Path(file) == path:
                with real(path, "wb") as handle:
                    handle.write(versions[min(self.opens, len(versions) - 1)])
                self.opens += 1
            return real(file, *args, **kwargs)

        monkeypatch.setattr(io, "open", swapped)
        monkeypatch.setattr("builtins.open", swapped)


def test_one_changed_byte_refuses_with_the_new_message(tmp_path):
    """#1025 acceptance 3."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(TUE, WED))
    vintages = _entry(source)
    source.write_bytes(source.read_bytes().replace(b"1000301", b"1000302"))
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    before = _snapshot(root)

    assert _refused(root, [source], vintages) == (
        f"{source} has SHA-256 {digest}, so the file is not one the list pins and nothing was "
        "imported"
    )
    assert _snapshot(root) == before


@pytest.mark.parametrize("listed_first", [True, False])
def test_a_swapped_file_lands_only_bytes_whose_digest_is_listed(
    tmp_path, monkeypatch, listed_first
):
    """Catches a file hashed on one open and parsed on another."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(TUE, WED))
    vintages = _entry(source)
    good = source.read_bytes()
    bad = good.replace(b"40.123456", b"99.123456")
    assert bad != good
    swap = _Swap(monkeypatch, source, [good, bad] if listed_first else [bad, good])

    if listed_first:
        _run(root, [source], vintages)
        closes = pq.read_table(root / _rel("GLD", TUE)).column("close").to_pylist()
        assert closes == [40.123456]
    else:
        assert "not one the list pins" in _refused(root, [source], vintages)
        assert _bars_files(root) == []
    assert swap.opens == 1


def test_the_undo_reads_its_keys_from_the_bytes_it_hashed(tmp_path, monkeypatch):
    """A killed import's file on Tuesday, and a swap to a file that names only Wednesday. Catches
    an undo that hashes the listed bytes and takes its keys from the others."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(TUE))
    vintages = _entry(source)
    good = source.read_bytes()
    before = _snapshot(root)
    _run(root, [source], vintages)
    (root / "manifest.jsonl").write_bytes(before["manifest.jsonl"])
    swap = _Swap(monkeypatch, source, [good, good.replace(b"2012-04-10", b"2012-04-11")])

    assert _rel("GLD", TUE) in _undo_refused(root, [source], vintages)
    assert swap.opens == 1


# -- a damaged manifest


TORN = (
    "manifest.jsonl ends in a torn line, so an entry appended to it would be lost and nothing was "
    "imported"
)
NOT_WHOLE = "line 2 of manifest.jsonl is not a whole entry, so nothing was imported"
DAMAGE = [
    pytest.param(b'{"partition": "x", "sou', TORN, id="torn"),
    pytest.param(b'{"partition": "x", "source": "y"}', TORN, id="short-by-its-newline"),
    pytest.param(b'{"partition": "x", "sou\n', NOT_WHOLE, id="fragment"),
    pytest.param(b'{"part{"partition": "x", "source": "y"}\n', NOT_WHOLE, id="fused"),
    pytest.param(
        b'{"part{"partition": "x", "source": "y"}\n{"partition": "z", "source": "y"}\n',
        NOT_WHOLE,
        id="fused-with-a-line-behind-it",
    ),
    pytest.param(b"\n", NOT_WHOLE, id="blank"),
    pytest.param(b'["x", "y"]\n', NOT_WHOLE, id="a-list"),
    pytest.param(b'{"partition": "x"}\n', NOT_WHOLE, id="no-source"),
    pytest.param(b'{"source": "y"}\n', NOT_WHOLE, id="no-partition"),
    pytest.param(b"\xff\n", NOT_WHOLE, id="not-utf8"),
]
WHOLE = b'{"partition": "reference/odd.parquet", "source": "y"}\n'


@pytest.mark.parametrize(("damage", "expected"), DAMAGE)
def test_an_import_onto_a_damaged_manifest_refuses(tmp_path, damage, expected):
    """Its entries would be hidden from every reader, capture's later ones with them, and the undo
    refuses on the same damage. Catches a landing that reads no line it appends behind, in the run
    and in the dry run."""
    root, sources, vintages = _pair(tmp_path)
    (root / "manifest.jsonl").write_bytes(WHOLE + damage)
    before = _snapshot(root)

    assert _refused(root, sources, vintages) == expected
    with pytest.raises(BarImportRefused) as refused:
        _dry(root, sources, vintages)
    assert str(refused.value) == expected
    assert _snapshot(root) == before


@pytest.mark.parametrize(("damage", "expected"), DAMAGE)
def test_an_undo_on_a_damaged_manifest_refuses_with_nothing_recorded(tmp_path, damage, expected):
    """Catches an undo that says nothing to undo over lines it could not read."""
    root, sources, vintages = _pair(tmp_path)
    (root / "manifest.jsonl").write_bytes(WHOLE + damage)
    before = _snapshot(root)

    message = _undo_refused(root, sources, vintages)

    assert message == expected.replace(
        "an entry appended to it would be lost", "the undo cannot tell where the import starts"
    ).replace("nothing was imported", "nothing was undone")
    assert _snapshot(root) == before


def test_a_manifest_damaged_while_the_import_waits_for_the_lock_refuses(tmp_path, monkeypatch):
    """Catches a check made only before the lock."""
    root, sources, vintages = _pair(tmp_path)
    (root / "manifest.jsonl").write_bytes(WHOLE)
    real = bar_import.lake_lock

    @contextmanager
    def torn_then_lock(path):
        (root / "manifest.jsonl").write_bytes(WHOLE + b'{"partition": "x')
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", torn_then_lock)

    assert _refused(root, sources, vintages) == TORN
    assert not any(rel.startswith("bars/") for rel in _snapshot(root))


def test_a_rerun_after_a_short_append_refuses_and_lands_once_the_fragment_is_cut(
    tmp_path, monkeypatch
):
    """The second ticker's append is cut short, as a full disk leaves it. Catches a rerun that
    prints a good count over entries no reader can see."""
    root, sources, vintages = _pair(tmp_path)
    before = _snapshot(root)
    real = bar_import.append_entries
    calls = []

    def short(*args, **kwargs):
        calls.append(args)
        real(*args, **kwargs)
        if len(calls) == 2:
            raw = (root / "manifest.jsonl").read_bytes()
            (root / "manifest.jsonl").write_bytes(raw[:-40])
            raise OSError(28, "No space left on device")

    with monkeypatch.context() as patched:
        patched.setattr(bar_import, "append_entries", short)
        _refused(root, sources, vintages)
    torn = _snapshot(root)

    assert _refused(root, sources, vintages) == TORN
    assert _snapshot(root) == torn

    raw = (root / "manifest.jsonl").read_bytes()
    (root / "manifest.jsonl").write_bytes(raw[: raw.rindex(b"\n") + 1])
    _run(root, sources, vintages)
    _undo(root, sources, vintages)
    assert _snapshot(root) == before


# -- the splits


SPLIT_CELL = "0.125"


@pytest.mark.parametrize(
    ("rows", "splits", "expected"),
    [
        (
            _days(*FULL),
            SPLIT,
            "holds no split on 2012-04-12 in its Stock Splits column, which GLD's entry lists",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}, d13={"Stock_Splits": "0.25"}),
            SPLIT,
            "line 6 holds a split on 2012-04-13 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": "0.25"}),
            SPLIT,
            "line 5 holds another Stock Splits factor for 2012-04-12 than GLD's entry lists",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": "0.12499999999999999"}),
            SPLIT,
            "line 5 holds another Stock Splits factor for 2012-04-12 than GLD's entry lists",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": "0.12500000000000003"}),
            SPLIT,
            "line 5 holds another Stock Splits factor for 2012-04-12 than GLD's entry lists",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}),
            (*SPLIT, (date(2012, 4, 13), 0.5)),
            "holds no split on 2012-04-13 in its Stock Splits column, which GLD's entry lists",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": "-0.125"}),
            (),
            "line 5 holds a split on 2012-04-12 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": "1.0"}),
            (),
            "line 5 holds a split on 2012-04-12 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d11={"Stock_Splits": SPLIT_CELL}),
            SPLIT,
            "line 4 holds a split on 2012-04-11 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}, d16={"Stock_Splits": "0.5"}),
            SPLIT,
            "line 7 holds a split on 2012-04-16 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}),
            (),
            "line 5 holds a split on 2012-04-12 in its Stock Splits column, which GLD's entry "
            "does not list",
        ),
        (
            _days(*FULL, d12={"Stock_Splits": ""}),
            (),
            "line 5 has no finite number in its Stock Splits column on 2012-04-12",
        ),
        (
            _days(*FULL, d09={"Stock_Splits": ""}),
            (),
            "line 2 has no finite number in its Stock Splits column on 2012-04-09",
        ),
        (
            _days(*FULL, d16={"Stock_Splits": "x"}),
            (),
            "line 7 has no finite number in its Stock Splits column on 2012-04-16",
        ),
        (
            _days(*FULL, d16={"Stock_Splits": "inf"}),
            (),
            "line 7 has no finite number in its Stock Splits column on 2012-04-16",
        ),
    ],
    ids=[
        "missing",
        "unlisted",
        "another-factor",
        "just-below-the-factor",
        "just-above-the-factor",
        "second-of-two",
        "negative",
        "one",
        "another-date",
        "outside-the-window",
        "none-listed",
        "blank",
        "blank-outside",
        "text-outside",
        "inf-outside",
    ],
)
def test_a_split_the_entry_does_not_record_refuses(tmp_path, rows, splits, expected):
    """#1025 acceptance 4. Each message whole, and it holds no factor."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", rows)
    before = _snapshot(root)

    message = _refused(root, [source], _entry(source, splits=splits))

    assert message == f"{source} {expected}, so nothing was imported"
    factors = ("0.125", "0.124", "0.25", "0.5", "1.0")
    assert not any(factor in message for factor in factors)
    assert _snapshot(root) == before


@pytest.mark.parametrize(
    ("layout", "splits"),
    [("one_row", None), ("one-row", None), ("three-row", ())],
    ids=["misspelled-layout", "one-row-without-splits", "three-row-with-splits"],
)
def test_an_entry_that_would_skip_a_check_cannot_be_built(layout, splits):
    """Catches a list entry that forgets ``splits``, or whose layout falls to the other reader."""
    with pytest.raises(ValueError, match="test:gld.csv"):
        Vintage("ab" * 32, "GLD", "test:gld.csv", AS_OF, BASIS_ADJUSTED, layout, None, splits)


def test_the_listed_splits_land_and_tell_two_swapped_digests_apart(tmp_path):
    """Two digests listed under each other's ticker. Nothing else in the file names its ticker."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    uso = _one_row(tmp_path / "src" / "uso.csv", _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}))

    assert _refused(root, [uso], _entry(uso, "GLD")) == (
        f"{uso} line 5 holds a split on 2012-04-12 in its Stock Splits column, which GLD's entry "
        "does not list, so nothing was imported"
    )
    assert _refused(root, [gld], _entry(gld, "USO", splits=SPLIT)) == (
        f"{gld} holds no split on 2012-04-12 in its Stock Splits column, which USO's entry lists, "
        "so nothing was imported"
    )
    assert _bars_files(root) == []
    summary = _run(root, [gld, uso], {**_entry(gld), **_entry(uso, "USO", splits=SPLIT)})
    assert [ticker.landed for ticker in summary.tickers] == [4, 4]


# -- every other refusal of the one-row layout


SEVEN = ("Date", "Open", "High", "Low", "Close", "Volume", "Stock Splits")


def _header_cases():
    for name in SEVEN:
        absent = ",".join("Other" if n == name else n for n in ONE_ROW_NAMES)
        twice = ",".join(name if n == "Dividends" else n for n in ONE_ROW_NAMES)
        yield pytest.param(absent, name, 0, id=f"no-{name}")
        yield pytest.param(twice, name, 2, id=f"two-{name}")


@pytest.mark.parametrize(("header", "name", "count"), list(_header_cases()))
def test_a_one_row_header_refuses_unless_each_column_is_named_once(tmp_path, header, name, count):
    """#1025 acceptance 5."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    source.write_text(source.read_text().replace(ONE_ROW, header))
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 1 names its {name} column {count} time(s), so nothing was imported"
    )
    assert _snapshot(root) == before


def test_a_three_row_file_under_a_one_row_entry_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _csv(tmp_path / "src" / "gld.csv", [(TUE, 10.0)])
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 1 names its Date column 0 time(s), so nothing was imported"
    )
    assert _snapshot(root) == before


def test_a_one_row_file_under_a_three_row_entry_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    before = _snapshot(root)
    vintages = _entry(source, splits=None, layout=bar_import.LAYOUT_THREE_ROW)

    assert _refused(root, [source], vintages) == (
        f"{source} does not open with yfinance's three header rows, Price, Ticker and Date, so "
        "nothing was imported"
    )
    assert _snapshot(root) == before


def _cell_cases():
    for name in ("Open", "High", "Low", "Close"):
        for cell in ("", "nan", "inf", "x", "0.000000", "-5.500000"):
            yield pytest.param(
                name, cell, f"has no finite number above zero in its {name} column", id=name + cell
            )
    for cell in ("", "0", "-1", "+5", " 5", "0.5", "1_000", "٥", "9223372036854775808", "9" * 5000):
        yield pytest.param(
            "Volume",
            cell,
            "has no whole number from 1 to 2**63 - 1 in its Volume column",
            id="Volume" + ascii(cell[:20]),
        )


@pytest.mark.parametrize(("name", "cell", "expected"), list(_cell_cases()))
def test_a_bad_price_or_volume_refuses_inside_the_window_and_lands_outside_it(
    tmp_path, name, cell, expected
):
    root = _lake(tmp_path)
    bad = {name.replace(" ", "_"): cell}
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL, d11=bad))
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 4 {expected}, so nothing was imported"
    )
    assert _snapshot(root) == before

    outside = _one_row(tmp_path / "src" / "out.csv", _days(*FULL, d09=bad, d16=bad))
    assert _run(root, [outside], _entry(outside)).tickers[0].landed == 4


def _line_cases():
    good = _days(*FULL)
    yield pytest.param(
        [*good[:5], _cells("2012-4-16", 5)],
        "line 7 has no YYYY-MM-DD date in its Date column",
        id="malformed-date",
    )
    yield pytest.param(
        [*good, _cells(AFTER, 6)],
        "line 8 repeats 2012-04-16 in its Date column",
        id="duplicate-date",
    )


@pytest.mark.parametrize(("rows", "expected"), list(_line_cases()))
def test_a_bad_date_outside_the_window_refuses(tmp_path, rows, expected):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", rows)
    before = _snapshot(root)

    message = _refused(root, [source], _entry(source))

    assert message == f"{source} {expected}, so nothing was imported"
    assert _snapshot(root) == before


@pytest.mark.parametrize(
    ("names", "column"),
    [
        (ONE_ROW_NAMES, "Volume"),
        (ONE_ROW_NAMES[::-1], "Date"),
        ((*(n for n in ONE_ROW_NAMES if n != "Dividends"), "Dividends"), "Dividends"),
    ],
    ids=["file-order", "date-last", "unread-last"],
)
@pytest.mark.parametrize("row", [5, 1], ids=["outside", "inside"])
def test_a_row_short_by_its_last_cell_refuses(tmp_path, names, column, row):
    """Catches a width taken from the columns read, and a date read before the width, which would
    raise on the order whose last column is the date."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL), names=names)
    lines = source.read_text().splitlines()
    lines[row + 1] = lines[row + 1].rsplit(",", 1)[0]
    source.write_text("\n".join(lines) + "\n")
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line {row + 2} has 9 field(s) and ends before {column}, so nothing was imported"
    )
    assert _snapshot(root) == before


def test_a_row_short_by_two_cells_names_the_first_it_lacks(tmp_path):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    source.write_text(source.read_text().rstrip("\n").rsplit(",", 2)[0] + "\n")

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 7 has 8 field(s) and ends before Stock Splits, so nothing was imported"
    )


def test_a_row_inside_the_window_on_a_day_with_no_session_refuses(tmp_path):
    """Saturday 2012-04-14 between Friday and Monday. Catches the check made only for a file with
    no window, which would land a write-once partition no session owns."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(FRI, "2012-04-14", AFTER))
    before = _snapshot(root)
    vintages = _entry(source, window=(date(2012, 4, 10), date(2012, 4, 16)))

    assert _refused(root, [source], vintages) == (
        f"{source}: 2012-04-14 is not a session, so nothing was imported"
    )
    assert _snapshot(root) == before


@pytest.mark.parametrize(("day", "line"), [("d11", 4), ("d16", 7)], ids=["inside", "outside"])
@pytest.mark.parametrize(
    ("cell", "cells"),
    [("1,000,300", 12), ("100,300", 11), ("100300,", 11)],
    ids=["two-more", "one-more", "one-blank-more"],
)
def test_a_row_longer_than_its_header_refuses(tmp_path, day, line, cell, cells):
    """An unquoted thousands separator in the last column, which would land 1,000,300 as 1."""
    root = _lake(tmp_path)
    rows = _days(*FULL, **{day: {"Volume": cell}})
    source = _one_row(tmp_path / "src" / "gld.csv", rows)
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line {line} has {cells} field(s), more than its header names, so nothing was "
        "imported"
    )
    assert _snapshot(root) == before


def test_a_row_of_blank_cells_refuses(tmp_path):
    """Catches a blank row skipped the way an empty line is."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    source.write_text(source.read_text() + "," * 9 + "\n")

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 8 has no YYYY-MM-DD date in its Date column, so nothing was imported"
    )


def test_bytes_that_are_not_utf8_refuse_and_quote_nothing(tmp_path):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(TUE))
    source.write_bytes(source.read_bytes().replace(b"40.123456", b"40.12345\xff"))
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} cannot be read as CSV (UnicodeDecodeError), so nothing was imported"
    )
    assert _snapshot(root) == before


def test_one_row_columns_in_another_order_and_an_empty_line_read_back(tmp_path):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(TUE, WED), names=ONE_ROW_NAMES[::-1])
    source.write_text(source.read_text().replace("2012-04-10\n", "2012-04-10\n\n"))

    summary = _run(root, [source], _entry(source))

    assert summary.render().endswith(", 0 row(s) outside the window")
    assert _dry(root, [source], _entry(source)).endswith(
        ", 0 row(s) outside the window. Nothing was written"
    )
    table = load_bars("GLD", "1d", lake_root=root)
    assert table.column("open").to_pylist() == [39.373456, 40.373456]
    assert table.column("high").to_pylist() == [41.373456, 42.373456]
    assert table.column("low").to_pylist() == [38.623456, 39.623456]
    assert table.column("close").to_pylist() == [40.123456, 41.123456]
    assert table.column("volume").to_pylist() == [1000300, 1000301]


def test_the_smallest_and_the_largest_volume_land_and_so_does_a_tiny_price(tmp_path):
    root = _lake(tmp_path)
    tiny = {"Volume": "1", "Low": "0.000001"}
    rows = _days(TUE, WED, d10=tiny, d11={"Volume": "9223372036854775807"})
    source = _one_row(tmp_path / "src" / "gld.csv", rows)

    _run(root, [source], _entry(source))

    table = load_bars("GLD", "1d", lake_root=root)
    assert table.column("volume").to_pylist() == [1, 9223372036854775807]
    assert table.column("low").to_pylist() == [0.000001, 39.623456]


# -- the sessions the window names


@pytest.mark.parametrize(
    ("days", "missing"),
    [((WED, THU, FRI), 1), ((TUE, WED, THU), 1), ((TUE, FRI), 2), ((TUE, WED, THU, FRI), 0)],
    ids=["first", "last", "middle", "none"],
)
def test_the_missing_count_runs_over_the_window(tmp_path, days, missing):
    """#1025 acceptance 6. Catches a count bounded by the file's own first and last rows."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(BEFORE, *days, AFTER))

    (ticker,) = _run(root, [source], _entry(source)).tickers

    assert (ticker.landed, ticker.missing) == (len(days), missing)


def test_a_file_starting_weeks_into_its_window_counts_them_all(tmp_path):
    """Tuesday 2012-04-10 to Monday 2012-04-30 is 15 sessions. Catches a history calendar started
    at the earliest row, which raises ``DateOutOfBounds`` on the window's first session."""
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days("2012-05-01"))
    vintages = _entry(source, window=(date(2012, 4, 10), date(2012, 5, 1)))

    (ticker,) = _run(root, [source], vintages).tickers

    assert (ticker.landed, ticker.missing) == (1, 15)


def test_a_file_that_starts_before_another_files_window_lands_beside_it(tmp_path):
    """A row of 2006 and a window from 2012 in one run. Catches a history calendar started at the
    window alone, which raises ``DateOutOfBounds`` on the earlier file."""
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _csv(tmp_path / "src" / "gld.csv", [("2006-04-26", 1.0)])
    uso = _one_row(tmp_path / "src" / "uso.csv", _days(TUE, WED))

    summary = _run(root, [gld, uso], {**_listed((gld, "GLD")), **_entry(uso, "USO")})

    assert [ticker.landed for ticker in summary.tickers] == [1, 2]


def test_an_empty_listed_file_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _text(tmp_path / "src" / "gld.csv", "")

    assert _refused(root, [source], _entry(source)) == (
        f"{source} line 1 names its Date column 0 time(s), so nothing was imported"
    )


def test_a_file_with_no_row_inside_its_window_refuses(tmp_path):
    root = _lake(tmp_path)
    source = _one_row(tmp_path / "src" / "gld.csv", _days(BEFORE, AFTER))
    before = _snapshot(root)

    assert _refused(root, [source], _entry(source)) == (
        f"{source} holds no row inside its window, 2012-04-10 to 2012-04-13, so nothing was "
        "imported"
    )
    assert _snapshot(root) == before


def test_a_key_inside_the_window_held_by_another_source_refuses_both_tickers(tmp_path):
    """#1025 acceptance 7."""
    root = _lake(tmp_path, ("GLD", "USO"))
    held = _manifest_bars(root, "USO", THU, source="bar-import:sha256:" + "cd" * 32)
    gld = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    uso = _one_row(tmp_path / "src" / "uso.csv", _days(*FULL))
    uso.write_text(uso.read_text() + "\n")
    before = _snapshot(root)

    message = _refused(root, [gld, uso], {**_entry(gld), **_entry(uso, "USO")})

    assert message.startswith(f"{held} is already held by the import")
    assert _snapshot(root) == before


# -- the undo over a window


def _pair(tmp_path: Path):
    root = _lake(tmp_path, ("GLD", "USO"))
    gld = _one_row(tmp_path / "src" / "gld.csv", _days(*FULL))
    uso = _one_row(tmp_path / "src" / "uso.csv", _days(*FULL, d12={"Stock_Splits": SPLIT_CELL}))
    return root, [gld, uso], {**_entry(gld), **_entry(uso, "USO", splits=SPLIT)}


@pytest.mark.parametrize("recorded", [True, False], ids=["recorded", "unrecorded"])
def test_an_undo_ignores_a_file_outside_the_window(tmp_path, monkeypatch, capsys, recorded):
    """#1025 acceptance 8. The CSV holds rows on the two outer sessions. Catches an undo that
    takes a neighbour's file for a killed import's own."""
    root, sources, vintages = _pair(tmp_path)
    for day in (BEFORE, AFTER):
        if recorded:
            _manifest_bars(root, "GLD", day, source="bar-import:sha256:" + "cd" * 32)
        else:
            write_bars(root, "GLD", "1d", day, [1.0], instrument_id=1)
    before = _snapshot(root)
    monkeypatch.setattr(bar_import, "VINTAGES", vintages)
    monkeypatch.setattr(bar_import, "connect", lambda config: (UNDO_TARGET, FakeS3()))
    config = write_config(tmp_path, root)

    code = bar_import.main(
        ["undo", *map(str, sources), "--config", str(config)],
        clock=ManualClock(MONDAY_0745),
        calendar=CALENDAR,
    )

    assert code == 0
    assert capsys.readouterr().out == (
        "bar_import: nothing to undo: the manifest records none of these imports\n"
    )
    assert _snapshot(root) == before


def test_an_undo_of_a_windowed_import_leaves_the_lake_as_it_was(tmp_path):
    root, sources, vintages = _pair(tmp_path)
    for day in (BEFORE, AFTER):
        _manifest_bars(root, "GLD", day, source="bar-import:sha256:" + "cd" * 32)
    before = _snapshot(root)
    _run(root, sources, vintages)

    summary = _undo(root, sources, vintages)

    assert (summary.lines, summary.partitions) == (8, 8)
    assert _snapshot(root) == before


def _kill_append(monkeypatch, root, sources, vintages, *, nth):
    real = bar_import.append_entries
    calls = []

    def crash(*args, **kwargs):
        calls.append(args)
        if len(calls) == nth:
            raise Crash
        return real(*args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(bar_import, "append_entries", crash)
        with pytest.raises(Crash):
            _run(root, sources, vintages)


def test_an_undo_after_a_kill_in_the_second_ticker_refuses_until_a_rerun(tmp_path, monkeypatch):
    """Catches an undo that looks for unrecorded files only when nothing is recorded."""
    root, sources, vintages = _pair(tmp_path)
    before = _snapshot(root)
    _kill_append(monkeypatch, root, sources, vintages, nth=2)
    killed = _snapshot(root)
    recorded = {rel for rel in latest_entries(root) if rel.startswith("bars/")}
    assert recorded == {_rel("GLD", day) for day in (TUE, WED, THU, FRI)}
    assert {rel for rel in killed if "ticker=USO" in rel} == {
        _rel("USO", day) for day in (TUE, WED, THU, FRI)
    }
    client = FakeS3()

    assert _undo_refused(root, sources, vintages, client=client) == (
        f"{_rel('USO', TUE)} is on disk and no manifest line names it, which a killed import "
        "leaves. Run the import again to record it, then run the undo"
    )
    assert _snapshot(root) == killed
    assert client.calls == []

    _run(root, sources, vintages)
    _undo(root, sources, vintages)
    assert _snapshot(root) == before


def test_an_undo_after_a_kill_in_the_first_ticker_refuses(tmp_path, monkeypatch):
    """Nothing recorded, and the first file's partitions on disk. Catches an undo that looks for
    the last file's keys alone."""
    root, sources, vintages = _pair(tmp_path)
    _kill_append(monkeypatch, root, sources, vintages, nth=1)
    killed = _snapshot(root)

    assert _undo_refused(root, sources, vintages).startswith(f"{_rel('GLD', TUE)} is on disk and ")
    assert _snapshot(root) == killed


def test_a_kill_while_the_undo_waits_for_the_lock_refuses_it(tmp_path, monkeypatch):
    """The killed import appends nothing, so the manifest's bytes do not show it. Catches a check
    made only before the lock."""
    root, sources, vintages = _pair(tmp_path)
    _run(root, sources[:1], vintages)
    real = bar_import.lake_lock

    snapshots: list[dict[str, bytes]] = []

    @contextmanager
    def killed_then_lock(path):
        write_bars(root, "USO", "1d", FRI, [1.0], instrument_id=2)
        snapshots.append(_snapshot(root))
        with real(path):
            yield

    monkeypatch.setattr(bar_import, "lake_lock", killed_then_lock)

    assert _undo_refused(root, sources, vintages) == (
        f"{_rel('USO', FRI)} is on disk and no manifest line names it, which a killed import "
        "leaves. Run the import again to record it, then run the undo"
    )
    assert _snapshot(root) == snapshots[0]


def test_an_in_window_key_of_another_import_is_not_a_killed_imports(tmp_path):
    """With nothing of this import recorded, and with its other ticker recorded after it."""
    root, sources, vintages = _pair(tmp_path)
    _manifest_bars(root, "GLD", WED, source="bar-import:sha256:" + "cd" * 32)
    before = _snapshot(root)

    assert _undo(root, sources, vintages).lines == 0
    assert _snapshot(root) == before

    _run(root, sources[1:], vintages)
    assert _undo(root, sources, vintages).lines == 4
    assert _snapshot(root) == before


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (
            {"splits": ()},
            "line 5 holds a split on 2012-04-12 in its Stock Splits column, which USO's entry "
            "does not list",
        ),
        (
            {"window": (date(2012, 4, 9), date(2012, 4, 13))},
            "line 2 has no finite number above zero in its Open column",
        ),
        (
            {"window": (date(2012, 5, 1), date(2012, 5, 2))},
            "holds no row inside its window, 2012-05-01 to 2012-05-02",
        ),
        (
            {"layout": "three-row", "splits": None},
            "does not open with yfinance's three header rows, Price, Ticker and Date",
        ),
    ],
    ids=["splits", "wider-window", "other-window", "other-layout"],
)
def test_a_file_the_undo_cannot_read_says_nothing_was_undone(tmp_path, change, expected):
    """The undo reads the file on every run, so its refusals end in its own words. The entry
    changed after the import, as a later deploy could change it. The file's row before the window
    holds a bad price, which a wider window meets."""
    root = _lake(tmp_path, ("USO",))
    rows = _days(*FULL, d09={"Open": "0"}, d12={"Stock_Splits": SPLIT_CELL})
    source = _one_row(tmp_path / "src" / "uso.csv", rows)
    _run(root, [source], _entry(source, "USO", splits=SPLIT))
    after = _snapshot(root)

    message = _undo_refused(root, [source], _entry(source, "USO", **{"splits": SPLIT, **change}))

    assert message == f"{source} {expected}, so nothing was undone"
    assert _snapshot(root) == after


@pytest.mark.parametrize(
    ("error", "name"),
    [(PermissionError(13, "Permission denied"), "PermissionError"), (OSError(5, "I/O"), "OSError")],
    ids=["permission", "io"],
)
def test_a_key_the_undo_cannot_look_for_refuses_with_one_line(tmp_path, monkeypatch, error, name):
    """GLD recorded and USO's directory unreadable. Catches a traceback in place of a refusal."""
    root, sources, vintages = _pair(tmp_path)
    _run(root, sources[:1], vintages)
    after = _snapshot(root)
    real = Path.exists

    def unsearchable(self, *args, **kwargs):
        if "ticker=USO" in self.parts:
            raise error
        return real(self, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(Path, "exists", unsearchable)
        message = _undo_refused(root, sources, vintages)

    assert message == (f"{_rel('USO', TUE)} cannot be looked for ({name}), so nothing was undone")
    assert _snapshot(root) == after


def test_a_rerun_of_a_windowed_import_writes_nothing_and_exits_zero(tmp_path, monkeypatch, capsys):
    """#1025 acceptance 9."""
    root, sources, vintages = _pair(tmp_path)
    monkeypatch.setattr(bar_import, "VINTAGES", vintages)
    config = write_config(tmp_path, root)
    argv = ["vintage", *map(str, sources), "--config", str(config)]
    assert bar_import.main(argv, clock=ManualClock(MONDAY_0745), calendar=CALENDAR) == 0
    before = _snapshot(root)
    mtimes = {rel: os.stat(root / rel).st_mtime_ns for rel in before}
    capsys.readouterr()

    assert bar_import.main(argv, clock=ManualClock(MONDAY_0745), calendar=CALENDAR) == 0

    out = capsys.readouterr().out
    assert out.count("landed 0 of 4 row(s)") == 2 and out.count("4 already imported") == 2
    assert _snapshot(root) == before
    assert {rel: os.stat(root / rel).st_mtime_ns for rel in before} == mtimes
