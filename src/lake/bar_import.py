"""Import daily bars marketlake did not capture, beside the bars it did (marketlake #950).

l3a0/ithildin-runtime backtests on quantitative-trading's GLD and USO vintages, and the owner
ruled on 2026-10-29 that marketlake stores every source the runtime reads and serves it through
``load_bars`` (marketlake #938). ``docs/design.md`` carries that ruling and what it protects.
This module is the import path, and ``vintage`` is its first subcommand. #951's import of
Schwab's earlier sessions adds the second, by building a ``_Plan`` per ticker and handing it to
``_land``, which holds every rule that does not depend on the source.

**Where an import lands.** At the captured path, ``bars/ticker=T/freq=1d/date=D.parquet``, so
``load_bars``, the nightly upload, the trim and both restores handle it with no new path. Each
row is built by ``journal.bars_rows`` from a one-candle body, the capture path's own builder, so
``schema_version``, ``extra`` and the spelling of ``bar_ts`` match a captured bar. The five
stamps of the fetch, ``fetch_ts``, ``fetch_end_ts``, ``window_start``, ``window_end`` and
``extended_hours``, are null, since no fetch happened, and no reader of a bars partition uses
them. ``volume`` is left out, so it is null, because the vintages' volume unit is unverified and
an import is write-once. ``bar_ts`` is the session's Eastern midnight, by
``bars.session_midnight``. ``bars._write_partition`` writes each file with the source's
``Provenance``, which ``load_bars`` reads back as the ``origin``, ``basis``, ``source`` and
``source_as_of`` columns.

``instrument_id`` is the ticker's current instrument, so the ticker must be onboarded first.
That is a deliberate exception to the security master's date rule: ``onboard`` sets
``valid_from`` to the onboarding date, and ``actions.resolve_instrument`` refuses any earlier
day, so no instrument is valid on a vintage's 2006 sessions by the master's own reading.

**The identity of an import** is its source's SHA-256, carried in the manifest entry's
``source`` as ``bar-import:sha256:<hex>``. The file's metadata carries the same identity, both
built from one ``Vintage`` value, so the two cannot disagree. The manifest decides a rerun,
because the trim removes a file and keeps its entry, so a check that read the file would find
nothing and pass a different source in silence. The import instant goes in the entry's
``fetched_at``, which nothing reads, so two runs write identical files.

**Every rule is decided for every key of every source before anything is written.**

1. The run window. Capture waits on ``lake_lock`` with no timeout, so nothing runs inside a
   session or its pre-open margin, and the Sunday scrub takes no lock, so nothing runs while it
   may be reading. ``bucket._run_window_refusal`` is shared with the range restore. It is
   checked before anything is read, again once the lock is taken, and again before every
   partition is written.
2. The lake root holds ``manifest.jsonl``, since ``lake_lock`` would create one under a wrong
   ``lake_root``, and no source file sits inside it, where the scrub reads it as an orphan and
   the nightly upload sends it to the bucket.
3. Each source's SHA-256 is one ``VINTAGES`` lists, which names its ticker, and no two sources
   name one ticker.
4. The file is read by column name, because the vintages' order is ``Close,High,Low,Open``.
   Its ``Ticker`` row must name the listed ticker. A duplicate date and a date the exchange did
   not trade both refuse. A session the calendar names and the file lacks is counted, not
   refused. The calendar is built with an explicit start, because ``exchange_calendars`` starts
   20 years before today and raises ``DateOutOfBounds`` before that, and about 130 of each
   vintage's rows already fall there.
5. No date on or after the ticker's first capture span session. That is the first session
   ``bars.span_covers`` accepts for any span of any instrument the ticker has named, which is
   ``bars._span_sessions``' rule without its ceiling of today's close. A key landed for such a
   session would be manifested before capture's bar, and the backfill's manifested skip would
   never fetch the captured one.
6. Under the lock, a key the manifest records under this import's identity is skipped and
   counted, and a key it records under any other source refuses the whole run, because nothing
   replaces an import or a captured bar. So does an unrecorded file at a key whose metadata
   names another source or none, since only this import's own killed run leaves one it may
   replace.

**A stop leaves nothing unrecorded.** A refusal or a write failure part-way through a ticker
records the partitions it already renamed into place, in one manifest append, and then says
how many. Only a kill leaves a ``<key>.tmp-<pid>`` file, or a renamed file with no manifest
entry, and the Sunday scrub pages on both until a rerun repairs them. Under the lock the rerun
removes leftover temps beside this import's keys and overwrites an unrecorded file at a key it
writes when that file carries this import's identity, or cannot be read, which a torn write
leaves. Each ticker's entries land in one ``manifest.append_entries`` call after the directories
are flushed, rather than one append per partition, which would re-read the whole manifest each
time.

**A dry run** checks every rule, the conflicts included, reads the manifest without the lock,
and writes nothing. An import cannot be undone by any command, so the runbook runs it first.

**Only the primary runs it.** The laptop's role holds no ``s3:PutObject`` since #638, so an
import there never reaches the bucket, and its next resync would refuse on it. The next
scheduled upload sends an import to the bucket with no new code, because no upload, scrub,
restore or trim path reads a manifest ``source``. ``bucket._plan_resync`` is the one reader of
the prefix, and it refuses rather than deletes an import only this lake records.

Every refusal raises ``BarImportRefused`` with one line, which ``main`` prints with exit 2.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from lake import bars, outbox
from lake.actions import MasterAbsent, read_master
from lake.bars import SpansAbsent, read_capture_spans, session_midnight, span_covers
from lake.bucket import (
    RunWindowWords,
    _fsync_path,
    _refuse_inside_lake_root,
    _run_window_refusal,
)
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar
from lake.capture_spans import CaptureSpans, CaptureSpansError
from lake.clock import Clock
from lake.config import input_errors_exit, load_config
from lake.journal import bars_rows
from lake.loader import BASIS_ADJUSTED, ORIGIN_IMPORTED, Provenance, read_provenance
from lake.lock import lake_lock
from lake.manifest import (
    BAR_IMPORT_SOURCE_PREFIX,
    ManifestError,
    append_entries,
    latest_entries,
    manifest_path,
    sha256_file,
)
from lake.paths import DATE_PREFIX, LakePaths, leftover_temps, parse_date_dir
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, SecurityMasterError
from lake.session import SessionClock
from lake.vendor import DAILY_FREQ

COMMAND = "bar_import"

# The scheme ``Provenance.source_id`` carries before a source's hex digest.
_SHA256_SCHEME = "sha256:"

# How many days past a span's start an open span's first session is looked for. A long weekend
# with a holiday spans four days without one.
_SESSION_LOOKAHEAD_DAYS = 10

# The vintage's columns this import reads, by the header's name for each.
_PRICE_COLUMNS = {"open": "Open", "high": "High", "low": "Low", "close": "Close"}

SHADOW_REFUSAL = (
    "this host's role is shadow, and only the primary uploads to the bucket, so an import here "
    "would never reach it. Run it on the primary"
)

# The window the runbook runs the import in, which both run-window refusals send the operator
# back to. The nightly stop powers the VM off in the evening, so the morning is the window.
_RUN_WINDOW_WORDS = RunWindowWords(
    job="the bar import",
    in_flight="a partition in flight",
    sunday_repair="Run it on a weekday between the VM's 07:30 ET start and 09:00 ET",
    bound_repair="Run it on a weekday between the VM's 07:30 ET start and 09:00 ET",
)


class BarImportRefused(Exception):
    """The import refused to start, or stopped, with one line for an operator."""


@dataclass(frozen=True)
class Vintage:
    """One accepted source file, keyed by its SHA-256 in ``VINTAGES``.

    ``source`` names the file without a commit or a local path, which change between runs.
    ``source_as_of`` is the date the source recorded its values, which ``read_provenance``
    requires for an ``adjusted`` basis.
    """

    sha256: str
    ticker: str
    source: str
    source_as_of: date
    basis: str

    @property
    def provenance(self) -> Provenance:
        """What each partition's schema metadata records."""
        return Provenance(
            ORIGIN_IMPORTED,
            self.basis,
            self.source,
            _SHA256_SCHEME + self.sha256,
            self.source_as_of,
        )

    @property
    def manifest_source(self) -> str:
        """Each partition's manifest ``source``, built from the same identity as the file's."""
        return BAR_IMPORT_SOURCE_PREFIX + _SHA256_SCHEME + self.sha256


def _vintage(sha256: str, ticker: str, path: str) -> Vintage:
    # quantitative-trading's ``data/vintages.jsonl`` records both files with a ``saved_date`` of
    # 2012-04-10, a ``price_basis`` of ``adjusted`` and a ``vendor`` of ``chan-mat``.
    return Vintage(
        sha256=sha256,
        ticker=ticker,
        source=f"quantitative-trading:{path}",
        source_as_of=date(2012, 4, 10),
        basis=BASIS_ADJUSTED,
    )


# Every accepted vintage, reviewed in the pull request that adds it, so every run writes the
# same metadata. The digests are quantitative-trading's ``data/checksums.sha256`` at 5787cd23.
VINTAGES: Mapping[str, Vintage] = {
    v.sha256: v
    for v in (
        _vintage(
            "81e6295a8028362f82f9e96a89c686ef89991cb23afe42d4f884756019dc9d4c",
            "GLD",
            "data/inputdata_etf/gld.csv",
        ),
        _vintage(
            "94b6521980f64ce80feb5cdffb22168ed44f71eeaa93caca672f316eeda60ec2",
            "USO",
            "data/inputdata_etf/uso.csv",
        ),
    )
}


@dataclass(frozen=True)
class _SourceBar:
    day: date
    open: float
    high: float
    low: float
    close: float


@dataclass
class TickerImport:
    """What one source did to its ticker, for the line the command prints.

    On a dry run ``landed`` counts what the run would land, and the line says so.
    """

    ticker: str
    source: str
    rows: int
    missing: int = 0
    landed: int = 0
    skipped: int = 0
    replaced: int = 0
    temps_removed: list[str] = field(default_factory=list)
    dry_run: bool = False

    def render(self) -> str:
        if self.dry_run:
            return (
                f"{self.ticker}: dry run, would land {self.landed} of {self.rows} row(s) from "
                f"{self.source}, {self.skipped} already imported, {self.missing} session(s) the "
                f"calendar names and the file lacks, {self.replaced} unrecorded file(s) from an "
                "earlier run to replace. Nothing was written"
            )
        return (
            f"{self.ticker}: landed {self.landed} of {self.rows} row(s) from {self.source}, "
            f"{self.skipped} already imported, {self.missing} session(s) the calendar names and "
            f"the file lacks, {len(self.temps_removed)} leftover temp file(s) removed, "
            f"{self.replaced} unrecorded file(s) from an earlier run replaced"
        )


@dataclass
class ImportSummary:
    """What one run did, a ticker per source."""

    tickers: list[TickerImport] = field(default_factory=list)

    def render(self) -> str:
        return "\n".join(ticker.render() for ticker in self.tickers)


@dataclass
class _Plan:
    """One ticker's rows from one source, and everything ``_land`` needs to land them.

    ``path`` is what a refusal names as the source. ``instrument_id`` and ``todo`` are filled
    as the rules are decided.
    """

    ticker: str
    provenance: Provenance
    manifest_source: str
    path: Path
    bars: list[_SourceBar]
    report: TickerImport
    instrument_id: int | None = None
    todo: list[_SourceBar] = field(default_factory=list)


def _read_source(path: Path, vintage: Vintage) -> list[_SourceBar]:
    """The file's rows, or a refusal naming the first thing wrong with them.

    The header is yfinance's three rows: ``Price`` and the column names, then ``Ticker`` and
    the ticker per column, then ``Date``. Columns are found by name.
    """
    try:
        with path.open(newline="") as handle:
            lines = list(csv.reader(handle))
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise BarImportRefused(
            f"{path} cannot be read as CSV ({type(exc).__name__}), so nothing was imported"
        ) from None
    if len(lines) < 3 or [line[:1] for line in lines[:3]] != [["Price"], ["Ticker"], ["Date"]]:
        raise BarImportRefused(
            f"{path} does not open with yfinance's three header rows, Price, Ticker and Date, "
            "so nothing was imported"
        )
    names = lines[0]
    columns: dict[str, int] = {}
    for field_name, header in _PRICE_COLUMNS.items():
        found = [index for index, name in enumerate(names) if name == header]
        if len(found) != 1:
            raise BarImportRefused(
                f"{path} names its {header} column {len(found)} time(s) in its Price row, so "
                "nothing was imported"
            )
        columns[field_name] = found[0]
    named = sorted({cell for cell in lines[1][1:] if cell})
    if named != [vintage.ticker]:
        raise BarImportRefused(
            f"{path} names {', '.join(named) or 'no ticker'} in its Ticker row, and its SHA-256 "
            f"is listed as {vintage.ticker}'s vintage, so nothing was imported"
        )
    width = max(columns.values()) + 1
    seen: set[date] = set()
    rows: list[_SourceBar] = []
    for number, line in enumerate(lines[3:], start=4):
        if not line:
            continue
        day = parse_date_dir(DATE_PREFIX + line[0])
        if day is None:
            raise BarImportRefused(
                f"{path} line {number} starts with {line[0]!r}, which is not a YYYY-MM-DD date, "
                "so nothing was imported"
            )
        if day in seen:
            raise BarImportRefused(
                f"{path} holds {day.isoformat()} twice, and an import lands one partition per "
                "session, so nothing was imported"
            )
        seen.add(day)
        if len(line) < width:
            raise BarImportRefused(
                f"{path} line {number} has {len(line)} field(s), short of its header, so "
                "nothing was imported"
            )
        values: dict[str, float] = {}
        for field_name, index in columns.items():
            try:
                value = float(line[index])
            except ValueError:
                value = math.nan
            if not math.isfinite(value):
                raise BarImportRefused(
                    f"{path} line {number} has {line[index]!r} for {_PRICE_COLUMNS[field_name]}, "
                    "which is not a finite number, so nothing was imported"
                )
            values[field_name] = value
        rows.append(_SourceBar(day=day, **values))
    if not rows:
        raise BarImportRefused(f"{path} holds no rows, so nothing was imported")
    return rows


def _missing_sessions(plan: _Plan, history: Calendar) -> int:
    """Refuse a date the exchange did not trade, and count the sessions the file lacks."""
    days = {bar.day for bar in plan.bars}
    for day in sorted(days):
        if not history.is_session(day):
            raise BarImportRefused(
                f"{plan.path}: {day.isoformat()} is not a session, so nothing was imported"
            )
    missing = 0
    day, last = min(days), max(days)
    while day <= last:
        if day not in days and history.is_session(day):
            missing += 1
        day += timedelta(days=1)
    return missing


def _first_covered(span, sessions: SessionClock, calendar: Calendar) -> date | None:
    """The first session ``span`` covers by ``bars.span_covers``, with no ceiling, or ``None``.

    A closed span is searched to its end. An open one is searched ``_SESSION_LOOKAHEAD_DAYS``
    past its start. A span covering no instant, ``[t, t)``, covers no session, as
    ``bars._span_sessions`` reads it.
    """
    if span.end is not None and span.end <= span.start:
        return None
    day = span.start.astimezone(MARKET_TZ).date()
    last = (
        span.end.astimezone(MARKET_TZ).date()
        if span.end is not None
        else day + timedelta(days=_SESSION_LOOKAHEAD_DAYS)
    )
    while day <= last:
        if calendar.is_session(day) and span_covers(span, sessions.bounds(day)):
            return day
        day += timedelta(days=1)
    return None


def _first_span_session(
    ticker: str,
    master: SecurityMaster,
    spans: CaptureSpans,
    *,
    today: date,
    clock: Clock,
    calendar: Calendar,
) -> tuple[int, date]:
    """The ticker's current instrument and its first capture span session.

    The first span session is the earliest first session of any span of any instrument the
    ticker has named, because ``bars/`` paths are keyed by ticker.
    """
    try:
        current = master.resolve(ticker, today, id_type=ID_TYPE_TICKER)
    except SecurityMasterError as exc:
        raise BarImportRefused(f"{exc}, so nothing was imported") from None
    if current is None:
        raise BarImportRefused(
            f"the security master resolves no instrument for {ticker} today, so it is not "
            f"onboarded and nothing was imported. Run python -m lake.onboard {ticker} first"
        )
    owned = [
        span
        for instrument in sorted(master.instruments_named(ticker, ID_TYPE_TICKER))
        for span in spans.spans_of(instrument)
    ]
    if not owned:
        raise BarImportRefused(
            f"{ticker} has no capture span, so nothing says which sessions capture covers and "
            "nothing was imported. Run python -m lake.onboard, which opens one"
        )
    sessions = SessionClock(clock, calendar)
    firsts = [first for span in owned if (first := _first_covered(span, sessions, calendar))]
    if not firsts:
        raise BarImportRefused(
            f"no capture span of {ticker} covers a session the calendar names, so nothing says "
            "which sessions capture covers and nothing was imported"
        )
    return current, min(firsts)


def _holder(rel: str, held: str) -> str:
    if held.startswith(BAR_IMPORT_SOURCE_PREFIX):
        return f"{rel} is already held by the import {held}"
    return f"{rel} is already held by captured bars (manifest source {held!r})"


def _decide(root: Path, plans: Sequence[_Plan], latest: Mapping[str, Mapping]) -> None:
    """Sort every key into skipped or to write, or refuse the whole run on the first conflict.

    A key the manifest records under the plan's own identity is skipped. Any other recorded key
    conflicts. An unrecorded file at a key is replaced only when its metadata names the plan's
    own source identity, which a killed run of this import leaves, or when it cannot be read,
    which a torn write leaves. Any other unrecorded file conflicts, because nothing says it is
    this import's to replace.
    """
    paths = LakePaths(root)
    conflicts: list[str] = []
    for plan in plans:
        for bar in plan.bars:
            target = paths.bars_partition_path(plan.ticker, DAILY_FREQ, bar.day)
            rel = target.relative_to(root).as_posix()
            entry = latest.get(rel)
            if entry is not None:
                held = str(entry.get("source"))
                if held == plan.manifest_source:
                    plan.report.skipped += 1
                else:
                    conflicts.append(_holder(rel, held))
                continue
            if target.exists():
                try:
                    found = read_provenance(pq.read_schema(target).metadata, partition=rel)
                except (OSError, ValueError):
                    found = None
                except Exception as exc:  # noqa: BLE001 - a malformed key is someone else's file
                    conflicts.append(f"{rel} is an unrecorded file this import cannot read ({exc})")
                    continue
                if found is not None and found.source_id != plan.provenance.source_id:
                    owner = found.source_id or "no source, so it reads as captured"
                    conflicts.append(f"{rel} is an unrecorded file whose metadata names {owner}")
                    continue
                plan.report.replaced += 1
            plan.todo.append(bar)
    if conflicts:
        offered = ", ".join(sorted({plan.manifest_source for plan in plans}))
        raise BarImportRefused(
            f"{conflicts[0]}, and this run offers {offered}. {len(conflicts)} key(s) conflict, "
            "nothing was written, and no command replaces an import or a captured bar"
        )


def _land(
    root: Path,
    plans: Sequence[_Plan],
    *,
    clock: Clock,
    calendar: Calendar,
    dry_run: bool = False,
) -> ImportSummary:
    """Decide every key under the lock and land each plan's new partitions.

    The run window is checked once the lock is taken and before every partition is written. A
    stop part-way through a ticker, whether that check or a failed write, records the partitions
    already renamed into place in one append before it raises, so it leaves nothing for the
    scrub to page on. A dry run decides every key from an unlocked read and writes nothing.
    """
    summary = ImportSummary()
    recorded = 0

    def window() -> str | None:
        return _run_window_refusal(
            clock.now(), clock=clock, calendar=calendar, words=_RUN_WINDOW_WORDS
        )

    def refuse(why: str) -> BarImportRefused:
        if not recorded:
            return BarImportRefused(f"{why}. Nothing was written")
        return BarImportRefused(
            f"{why}. {recorded} partition(s) were recorded before this stop, and a rerun picks up "
            "the rest"
        )

    if dry_run:
        try:
            _decide(root, plans, latest_entries(root))
        except ManifestError as exc:
            raise BarImportRefused(f"{exc}, so nothing was imported") from None
        for plan in plans:
            plan.report.dry_run = True
            plan.report.landed = len(plan.todo)
            summary.tickers.append(plan.report)
        return summary

    paths = LakePaths(root)
    try:
        with lake_lock(root):
            why = window()
            if why is not None:
                raise refuse(why)
            _decide(root, plans, latest_entries(root))
            for plan in plans:
                directory = paths.bars_partition_path(
                    plan.ticker, DAILY_FREQ, plan.bars[0].day
                ).parent
                names = {
                    paths.bars_partition_path(plan.ticker, DAILY_FREQ, bar.day).name
                    for bar in plan.bars
                }
                for temp in leftover_temps(directory, names):
                    temp.unlink()
                    plan.report.temps_removed.append(temp.relative_to(root).as_posix())
                summary.tickers.append(plan.report)
                if not plan.todo:
                    continue
                entries: list[dict[str, object]] = []
                stop: str | None = None
                try:
                    for bar in plan.todo:
                        stop = window()
                        if stop is not None:
                            break
                        target = paths.bars_partition_path(plan.ticker, DAILY_FREQ, bar.day)
                        candle = {
                            "datetime": int(session_midnight(bar.day).timestamp() * 1000),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                        }
                        rows = bars_rows(
                            {"candles": [candle]},
                            ticker=plan.ticker,
                            freq=DAILY_FREQ,
                            instrument_id=plan.instrument_id,
                            fetch_ts=None,
                            fetch_end_ts=None,
                            window_start=None,
                            window_end=None,
                            extended_hours=None,
                        )
                        bars._write_partition(rows, target, provenance=plan.provenance)
                        entries.append(
                            {
                                "partition": target.relative_to(root).as_posix(),
                                "source": plan.manifest_source,
                                "sha256": None,
                                "rows": len(rows),
                                "fetched_at": clock.now().astimezone(MARKET_TZ).isoformat(),
                            }
                        )
                finally:
                    if entries:
                        # A first import creates ``bars/ticker=T/`` and ``freq=1d/``, so each
                        # directory and the parent that names it are flushed before the manifest
                        # names the files.
                        for flushed in (directory, directory.parent, directory.parent.parent):
                            _fsync_path(flushed)
                        append_entries(root, entries)
                        recorded += len(entries)
                        plan.report.landed = len(entries)
                if stop is not None:
                    raise refuse(stop)
    except BarImportRefused:
        raise
    except ManifestError as exc:
        raise refuse(f"{exc}, so the bar import stopped") from None
    except OSError as exc:
        raise refuse(
            f"writing the import under {root} failed ({type(exc).__name__}: "
            f"{exc.strerror or exc}). Fix that and run the bar import again"
        ) from None
    return summary


def import_vintages(
    lake_root: Path | str,
    sources: Sequence[Path],
    *,
    clock: Clock,
    calendar: Calendar,
    history: Calendar | None = None,
    vintages: Mapping[str, Vintage] | None = None,
    dry_run: bool = False,
) -> ImportSummary:
    """Import each listed vintage in ``sources`` into ``lake_root``'s bars surface.

    ``calendar`` places the run window and the first capture span session. ``history`` judges
    the sources' dates and defaults to the exchange calendar built from the earliest of them.
    ``vintages`` defaults to ``VINTAGES``. ``dry_run`` checks every rule and writes nothing. The
    module docstring gives the rules in order.
    """
    root = Path(lake_root)
    listed = VINTAGES if vintages is None else vintages

    why = _run_window_refusal(clock.now(), clock=clock, calendar=calendar, words=_RUN_WINDOW_WORDS)
    if why is not None:
        raise BarImportRefused(f"{why}. Nothing was written")
    if not manifest_path(root).is_file():
        raise BarImportRefused(
            f"lake_root {root} holds no manifest.jsonl, so it may name the wrong directory and "
            "nothing was imported"
        )

    plans: list[_Plan] = []
    for source in sources:
        path = Path(source)
        _refuse_inside_lake_root(
            path,
            root,
            refusal=BarImportRefused,
            inside=(
                f"{path} is inside lake_root {root}. The Sunday scrub reads a file there as an "
                "orphan and the nightly upload sends it to the bucket, so nothing was imported. "
                "Move it outside the lake"
            ),
            outcome="nothing was imported",
        )
        try:
            digest = sha256_file(path)
        except OSError as exc:
            raise BarImportRefused(
                f"{path} cannot be read ({type(exc).__name__}), so nothing was imported"
            ) from None
        vintage = listed.get(digest)
        if vintage is None:
            raise BarImportRefused(
                f"{path} has SHA-256 {digest}, which is no vintage this module lists, so nothing "
                "was imported. Fetch the file at the commit the list names"
            )
        if any(plan.ticker == vintage.ticker for plan in plans):
            raise BarImportRefused(
                f"two sources name {vintage.ticker}, and one partition per ticker-day means the "
                "first source imported holds each day, so nothing was imported"
            )
        rows = _read_source(path, vintage)
        plans.append(
            _Plan(
                ticker=vintage.ticker,
                provenance=vintage.provenance,
                manifest_source=vintage.manifest_source,
                path=path,
                bars=rows,
                report=TickerImport(vintage.ticker, vintage.manifest_source, len(rows)),
            )
        )

    try:
        master = read_master(root)
        spans = read_capture_spans(root)
    except (MasterAbsent, SpansAbsent, SecurityMasterError, CaptureSpansError) as exc:
        raise BarImportRefused(f"{exc}, so nothing was imported") from None
    except OSError as exc:
        raise BarImportRefused(
            f"the security master or the capture spans under {root} cannot be read "
            f"({type(exc).__name__}), so nothing was imported"
        ) from None
    today = clock.now().astimezone(MARKET_TZ).date()
    for plan in plans:
        plan.instrument_id, first = _first_span_session(
            plan.ticker, master, spans, today=today, clock=clock, calendar=calendar
        )
        late = [bar.day for bar in plan.bars if bar.day >= first]
        if late:
            raise BarImportRefused(
                f"{plan.path} holds {min(late).isoformat()}, on or after {plan.ticker}'s first "
                f"capture span session {first.isoformat()}, which capture covers, so nothing "
                "was imported"
            )

    # After the span bound, which refuses a date past the calendar's end before
    # ``is_session`` could raise ``DateOutOfBounds`` on it.
    if history is None:
        # Started before the earliest date, because ``exchange_calendars`` moves a start that is
        # not a session forward to the next one, and ``is_session`` then raises on the date
        # itself rather than answering no.
        earliest = min(bar.day for plan in plans for bar in plan.bars)
        history = ExchangeCalendar(start=earliest - timedelta(days=_SESSION_LOOKAHEAD_DAYS))
    for plan in plans:
        plan.report.missing = _missing_sessions(plan, history)

    return _land(root, plans, clock=clock, calendar=calendar, dry_run=dry_run)


def _source_file(value: str) -> Path:
    """An argparse ``type=`` hook: the path must name a file."""
    path = Path(value)
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"{value} is not a file")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lake.bar_import",
        description="Import daily bars marketlake did not capture into the bars surface.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    vintage = commands.add_parser(
        "vintage",
        help="Import quantitative-trading vintages the module lists, by SHA-256.",
    )
    vintage.add_argument(
        "sources",
        nargs="+",
        type=_source_file,
        metavar="CSV",
        help="A vintage CSV outside lake_root. Its SHA-256 names its ticker.",
    )
    vintage.add_argument(
        "--dry-run",
        action="store_true",
        help="Check every rule, the conflicts included, and write nothing.",
    )
    vintage.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
) -> int:
    """The ``python -m lake.bar_import`` entry. Returns a process exit code.

    It exits 0 when every row is in the lake, a rerun of a finished import included, or when a
    dry run found nothing to refuse, and 2 on a refusal, with one line. A shadow host refuses
    before anything is read.
    """
    args = build_parser().parse_args(argv)
    with input_errors_exit(COMMAND, BarImportRefused):
        config = load_config(args.config)
        role, warning = outbox.role_of(config)
        if warning is not None:
            print(f"{COMMAND}: {warning}", file=sys.stderr)
        if role != outbox.PRIMARY:
            print(f"{COMMAND}: {SHADOW_REFUSAL}", file=sys.stderr)
            return 2
        if clock is None:
            from lake.clock import SystemClock

            clock = SystemClock()
        summary = import_vintages(
            config.lake_root,
            args.sources,
            clock=clock,
            calendar=calendar if calendar is not None else ExchangeCalendar(),
            dry_run=args.dry_run,
        )
    for ticker in summary.tickers:
        print(f"{COMMAND}: {ticker.render()}")
    return 0


__all__ = [
    "BAR_IMPORT_SOURCE_PREFIX",
    "COMMAND",
    "SHADOW_REFUSAL",
    "VINTAGES",
    "BarImportRefused",
    "ImportSummary",
    "TickerImport",
    "Vintage",
    "build_parser",
    "import_vintages",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
