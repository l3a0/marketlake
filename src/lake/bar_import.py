"""Import daily bars marketlake did not capture, beside the bars it did (marketlake #950).

l3a0/ithildin-runtime backtests on quantitative-trading's GLD and USO vintages, and the owner
ruled on 2026-10-29 that marketlake stores every source the runtime reads and serves it through
``load_bars`` (marketlake #938). ``docs/design.md`` carries that ruling and what it protects.
This module is the import path, and ``vintage`` is its first subcommand. ``schwab`` is the
second (marketlake #951). Each builds a ``_Plan`` per ticker and hands it to ``_land``, which
holds every rule that does not depend on the source.

**The two kinds of vintage file.** ``vintage`` reads two layouts, and each ``Vintage`` names its
own. #950's files are quantitative-trading's, in yfinance's three header rows. marketlake #1025's
are Yahoo Finance downloads of GLD and USO in one header row, which fill the sessions from
2012-04-10 to 2026-09-03 between #950's and the Schwab import's. They hold more than that gap, so
their entries carry a ``window`` and the rows outside it are counted and left out. Their prices
and volume are Yahoo's, adjusted for the splits it knew on the download date, so USO's before its
one-for-eight reverse split of 2020-04-29 read eight times what traded and its volume an eighth.
No ledger entry records that split, because ``load_bars``' adjusted views refuse an imported row
and a reader that applied one to these rows itself would take the factor twice. Yahoo's terms
forbid republishing the bytes, so the one-row layout's refusals quote no price, volume or split
value.

**The Schwab import** lands the ``SCHWAB_SESSIONS`` sessions ending the session before a
ticker's first capture span session, from one ``vendor.get_daily_bars`` response per ticker,
fetched before the lock. The capture path cannot reach those sessions by design, since
``bars._span_sessions`` takes its range from the spans. Each row is the sweep's own, built by
``journal.bars_rows`` from the vendor's candle, so ``bar_ts`` is the vendor's stamp, ``volume``
its count, and the fetch's four stamps are the request's. ``bars.daily_session_of`` names each
candle's session, and a candle outside the window is dropped. The last session must fall before
the run's own Eastern date, because the shared run window allows an evening run. The sweep
fetches a session's candle that evening only behind its close gate (marketlake #962), and an
imported candle has no gate, so it waits until the day after.

Its basis is ``adjusted``, as of the fetch's Eastern date, because Schwab's daily candles are
split-adjusted to the day they are fetched. ``docs/design.md``'s ``## Tradeoffs: prices,
named`` carries the probe that measured it, which also found one dividend left unadjusted.

Its identity is the ticker and the window, ``schwab-window:<ticker>:<first>..<last>``, carried
as ``source_id`` and, under ``bar-import:``, as the manifest ``source``. The lake alone gives
it, so the undo names the import without a vendor call, and a rerun skips every recorded key
whatever Schwab answers that day. A digest of the response would refuse a rerun once Schwab
revised a volume. The ticker is in it so one ticker's undo cuts only that ticker's lines.

**Where an import lands.** At the captured path, ``bars/ticker=T/freq=1d/date=D.parquet``, so
``load_bars``, the nightly upload, the trim and both restores handle it with no new path. Each
row is built by ``journal.bars_rows``, the capture path's own builder, so ``schema_version``,
``extra`` and the spelling of ``bar_ts`` match a captured bar. A vintage's row comes from a
one-candle body built from the file's numbers, and the rest of this paragraph is about it. The
five stamps of the fetch, ``fetch_ts``, ``fetch_end_ts``, ``window_start``, ``window_end`` and
``extended_hours``, are null, since they record a request this lake made and both kinds of file
were fetched outside it, and no reader of a bars partition uses them. A three-row file's volume
is left out, so it is null, because #950's volume unit is unverified and an import is write-once.
A one-row file's volume lands as Yahoo gave it, shares traded and split-adjusted like the prices,
on the evidence in marketlake #1025. ``bar_ts`` is the session's Eastern midnight, by
``bars.session_midnight``. ``bars._write_partition`` writes each file with the source's
``Provenance``, which ``load_bars`` reads back as the ``origin``, ``basis``, ``source`` and
``source_as_of`` columns.

``instrument_id`` is the ticker's current instrument, so the ticker must be onboarded first.
That is a deliberate exception to the security master's date rule: ``onboard`` sets
``valid_from`` to the onboarding date, and ``actions.resolve_instrument`` refuses any earlier
day, so no instrument is valid on a vintage's 2006 sessions by the master's own reading.

**The identity of a vintage import** is its source's SHA-256, carried in the manifest entry's
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
   ``lake_root``, and no vintage file sits inside it, where the scrub reads it as an orphan and
   the nightly upload sends it to the bucket.
3. A vintage's SHA-256 is one ``VINTAGES`` lists, which names its ticker, and no two sources
   name one ticker. The Schwab import refuses a ticker named twice for the same reason. The file
   is read once, and the bytes hashed are the bytes parsed, so a file swapped between a hash and
   a second read cannot land under the listed digest.
4. A vintage file is read by column name, because the layouts order their columns differently.
   A three-row file's ``Ticker`` row must name the listed ticker. A one-row file names none, so
   its whole ``Stock Splits`` column must equal the entry's ``splits``, which is the one check
   inside the file against two digests listed under each other's ticker. Every row's date is
   parsed, and a duplicate refuses, inside the window or not. A row inside the window on a date
   the exchange did not trade refuses. A session the calendar names and the file lacks is
   counted, not refused, over the window where the entry has one. The calendar is built with an
   explicit start, because ``exchange_calendars`` starts 20 years before today and raises
   ``DateOutOfBounds`` before that, and about 130 of each #950 file's rows already fall there.
5. No date on or after the ticker's first capture span session. That is the first session
   ``bars.span_covers`` accepts for any span of any instrument the ticker has named, which is
   ``bars._span_sessions``' rule without its ceiling of today's close. A key landed for such a
   session would be manifested before capture's bar, and the backfill's manifested skip would
   never fetch the captured one.
6. Under the lock, a key the manifest records under this import's identity is skipped and
   counted, and a key it records under any other source refuses the whole run, because nothing
   replaces an import or a captured bar. So does an unrecorded file at a key whose metadata
   names another source or none, since only this import's own killed run leaves one it may
   replace. A recorded key whose file is missing is written again, except that a Schwab plan
   first asks the bucket, by ``_Plan.rewrite_check``, and refuses once the bucket holds the key.
7. The Schwab import's own rules, its window, the not-final guard and the checks on its
   response, are the Schwab paragraph's above and ``_schwab_bars``'.
8. Every line of the manifest is a whole entry, checked under the lock and in the dry run, so no
   entry is appended behind damage that hides it. ``_import_tail`` refuses an undo on the same
   damage.

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
and writes nothing. The Schwab import's dry run still fetches, so it checks the response too.
The runbook runs it first.

**An undo** (``undo`` and ``schwab-undo``, by the owner's decision of 2026-11-01 on #950,
which ``_undo`` holds for both) works only until the
import reaches the bucket. The runtime reads the bucket, so removing files on the VM alone would
withdraw nothing from it, and once any upload has sent an import no command withdraws
it. So the undo refuses unless the manifest's last lines are exactly the import's entries, and
the bucket's ``manifest.jsonl`` is absent or a byte prefix of the manifest cut back to before
them, and the bucket lists none of the import's keys. It also refuses on any of the import's keys
that is on disk and that no manifest line names, which an import killed part-way leaves, because
the cut would leave that file behind for the next upload. The import's rerun records it. Every
one of these checks runs before anything is cut. It reads the bucket before the lock and
again under it, because an upload that runs while the undo waits appends nothing to this lake's
manifest. Under the lock it also re-reads the manifest, deletes the import's files and their
leftover temps, flushes the directories, and only then cuts the manifest back in place with
``bucket._rewrite_manifest``, the resync's own rewrite. A crash between the two leaves recorded
files missing. A rerun of the undo finishes it while nothing else has been recorded. After that
only the import's rerun repairs it, by writing its own recorded files back, which ends the
nightly upload's refusal on a recorded file it cannot find.

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
import io
import json
import math
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from lake import bars, outbox
from lake import bucket as _bucket
from lake.actions import MasterAbsent, read_master
from lake.bars import (
    SpansAbsent,
    daily_session_of,
    read_capture_spans,
    session_midnight,
    span_covers,
)
from lake.bucket import (
    BucketTarget,
    RunWindowWords,
    _fsync_path,
    _refuse_inside_lake_root,
    _run_window_refusal,
    connect,
    list_bucket,
    read_copy_state,
    session_bound,
)
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar
from lake.capture_spans import CaptureSpans, CaptureSpansError
from lake.clock import Clock
from lake.config import input_errors_exit, load_config
from lake.journal import UNFIT_ERRORS, bars_data_batch, bars_rows
from lake.loader import BASIS_ADJUSTED, ORIGIN_IMPORTED, Provenance, read_provenance
from lake.lock import lake_lock
from lake.manifest import (
    BAR_IMPORT_SOURCE_PREFIX,
    ManifestError,
    append_entries,
    latest_entries,
    manifest_path,
    sha256_bytes,
)
from lake.paths import (
    DATE_PREFIX,
    LakePaths,
    default_token_path,
    leftover_temps,
    parse_bars_rel,
    parse_date_dir,
)
from lake.schwab import SchwabVendor, VendorAuthError, is_transient_failure
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, SecurityMasterError
from lake.session import SessionClock
from lake.trimmed import is_designed_absence, latest_trimmed
from lake.vendor import DAILY_FREQ, Vendor, VendorError

COMMAND = "bar_import"

# The scheme ``Provenance.source_id`` carries before a source's hex digest.
_SHA256_SCHEME = "sha256:"

# How many days past a span's start an open span's first session is looked for. A long weekend
# with a holiday spans four days without one.
_SESSION_LOOKAHEAD_DAYS = 10

# The vintage's columns this import reads, by the header's name for each.
_PRICE_COLUMNS = {"open": "Open", "high": "High", "low": "Low", "close": "Close"}

# How a source file's header is laid out, which each ``Vintage`` names. ``LAYOUT_THREE_ROW`` is
# yfinance's ``Price``, ``Ticker`` and ``Date`` rows, which #950's files open with.
# ``LAYOUT_ONE_ROW`` is one row of column names, with no ticker in the file.
LAYOUT_THREE_ROW = "three-row"
LAYOUT_ONE_ROW = "one-row"

# The one-row layout's columns this import reads beside the prices.
_DATE_COLUMN = "Date"
_VOLUME_COLUMN = "Volume"
_SPLITS_COLUMN = "Stock Splits"

# A volume cell's digits, bounded before ``int`` reads them, since Python refuses to parse more
# than 4,300, and the largest value the bars schema's int64 ``volume`` holds.
_VOLUME_DIGITS = re.compile(r"[0-9]{1,19}")
_MAX_VOLUME = 2**63 - 1

SHADOW_REFUSAL = (
    "this host's role is shadow, and only the primary uploads to the bucket, so an import here "
    "would never reach it. Run it on the primary"
)

# What each run-window refusal tells the operator. Those two windows are the only times of day
# the import refuses, and a stopped VM is started rather than waited for (marketlake #992). Every
# compaction uploads, so an import can be undone only until the next one sends it. After the
# evening upload that is normally the next session's close+15, past its bound, so each repair
# names that as the better time. A compaction comes sooner after a VM start, a deploy or a daemon
# restart past close+15, since the daemon runs one on starting late, and on a weekday holiday.
_RUN_WINDOW_WORDS = RunWindowWords(
    job="the bar import",
    in_flight="a partition in flight",
    sunday_repair=(
        "Run it after 23:30, best on a weekday evening once the evening upload after the 18:30 "
        "sweep has finished"
    ),
    bound_repair=(
        "Run it after that session's close+5, 16:20 on a regular day, best once the evening "
        "upload after the 18:30 sweep has finished"
    ),
)


class BarImportRefused(Exception):
    """The import refused to start, or stopped, with one line for an operator."""


@dataclass(frozen=True)
class Vintage:
    """One accepted source file, keyed by its SHA-256 in ``VINTAGES``.

    ``source`` names the file without a commit or a local path, which change between runs.
    ``source_as_of`` is the date the source recorded its values, which ``read_provenance``
    requires for an ``adjusted`` basis. ``layout`` is one of the two layout constants.
    ``window`` is the first and last session to land, or ``None`` for every row. ``splits`` is
    the one-row file's whole record of splits, a date and a factor each in date order, checked
    against the file on every read, or ``None`` where the layout carries no such column.
    """

    sha256: str
    ticker: str
    source: str
    source_as_of: date
    basis: str
    layout: str = LAYOUT_THREE_ROW
    window: tuple[date, date] | None = None
    splits: tuple[tuple[date, float], ...] | None = None

    def __post_init__(self) -> None:
        """Refuse an entry that would skip a check by leaving a field out or misspelling it.

        A layout neither constant names would read as three rows, and a one-row entry with no
        ``splits`` as one that lists none.
        """
        if self.layout not in (LAYOUT_THREE_ROW, LAYOUT_ONE_ROW):
            raise ValueError(f"{self.source} names the layout {self.layout!r}, which is not one")
        if (self.splits is not None) != (self.layout == LAYOUT_ONE_ROW):
            raise ValueError(f"{self.source} states splits if and only if its layout is one-row")

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


def _yahoo(sha256: str, ticker: str, name: str, splits: tuple[tuple[date, float], ...]) -> Vintage:
    # yfinance 1.8.0 with ``auto_adjust=False``, so the prices and the volume carry Yahoo's split
    # adjustment as of the download. The window is the gap between #950's last session and the
    # Schwab import's first (marketlake #1025).
    return Vintage(
        sha256=sha256,
        ticker=ticker,
        source=f"yahoo:{name}",
        source_as_of=date(2026, 11, 22),
        basis=BASIS_ADJUSTED,
        layout=LAYOUT_ONE_ROW,
        window=(date(2012, 4, 10), date(2026, 9, 3)),
        splits=splits,
    )


# Every accepted vintage, reviewed in the pull request that adds it, so every run writes the
# same metadata. #950's digests are quantitative-trading's ``data/checksums.sha256`` at 5787cd23.
# The Yahoo digests are the two files' own, in marketlake #1025's table.
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
        # Downloaded at 2026-11-22T17:12:02Z.
        _yahoo(
            "72bb286f57f637c9ef517b98631a68f7e8f2d7869af4935441b555b11f60d561",
            "USO",
            "uso_daily_yahoo_2026-11-22.csv",
            ((date(2020, 4, 29), 0.125),),
        ),
        # Downloaded at 2026-11-22T17:12:03Z.
        _yahoo(
            "b6675ed455acf0174854377f8a51061fb18c0b1bf0a9a0f7632ac557b6fcb4f9",
            "GLD",
            "gld_daily_yahoo_2026-11-22.csv",
            (),
        ),
    )
}


@dataclass(frozen=True)
class _SourceBar:
    """One session's numbers from a source.

    ``row`` is the partition's row when the source already built it, as the Schwab import does
    with ``journal.bars_rows`` from the vendor's own candle. ``None`` has ``_land`` build the row
    from the four prices and ``volume``, stamped at the session's Eastern midnight, as a vintage
    needs. ``volume`` is read only then, and ``None`` leaves the column null.
    """

    day: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None = None
    row: Mapping[str, object] | None = field(default=None, compare=False)


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
    rewritten: int = 0
    temps_removed: list[str] = field(default_factory=list)
    dry_run: bool = False
    # What the line calls the source when it counts the sessions missing from it.
    holder: str = "the file"
    # What a source holds outside its window, rows of a file or candles of a response, and the
    # candles the row builder could not read. ``dropped`` is ``None`` for a source with no
    # window, and ``unreadable`` is ``None`` for a file.
    dropped: int | None = None
    unreadable: int | None = None

    def _candles(self) -> str:
        if self.unreadable is None:
            return "" if self.dropped is None else f", {self.dropped} row(s) outside the window"
        return (
            f", {self.dropped or 0} candle(s) outside the window dropped, "
            f"{self.unreadable or 0} candle(s) the row builder could not read"
        )

    def render(self) -> str:
        if self.dry_run:
            return (
                f"{self.ticker}: dry run, would land {self.landed} of {self.rows} row(s) from "
                f"{self.source}, {self.skipped} already imported, {self.missing} session(s) the "
                f"calendar names and {self.holder} lacks, {self.replaced} unrecorded file(s) "
                f"from an earlier run to replace, {self.rewritten} recorded file(s) missing to "
                f"write again{self._candles()}. Nothing was written"
            )
        return (
            f"{self.ticker}: landed {self.landed} of {self.rows} row(s) from {self.source}, "
            f"{self.skipped} already imported, {self.missing} session(s) the calendar names and "
            f"{self.holder} lacks, {len(self.temps_removed)} leftover temp file(s) removed, "
            f"{self.replaced} unrecorded file(s) from an earlier run replaced, "
            f"{self.rewritten} recorded file(s) found missing and written again{self._candles()}"
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

    ``path`` is what a refusal names as the source. ``window`` is the sessions the source should
    hold, or ``None`` for its own first to last. ``instrument_id``, ``todo`` and ``rewrites`` are
    filled as the rules are decided.

    ``rewrite_check`` is asked about the recorded keys ``_decide`` found missing before any is
    written again, and returns a refusal or ``None``. A vintage has none, because its pinned file
    writes the same values every time. A Schwab plan has one, because a later fetch can carry
    revised or rescaled values under the same identity, and writing them over a copy the upload
    already sent would put two scales inside one import.
    """

    ticker: str
    provenance: Provenance
    manifest_source: str
    path: Path
    bars: list[_SourceBar]
    report: TickerImport
    window: tuple[date, date] | None = None
    instrument_id: int | None = None
    todo: list[_SourceBar] = field(default_factory=list)
    rewrites: list[str] = field(default_factory=list)
    rewrite_check: Callable[[Sequence[str]], str | None] | None = None


@dataclass(frozen=True)
class _Header:
    """Where a file's rows start, how many cells each needs, and where its columns are.

    ``volume`` and ``splits`` are ``None`` in the three-row layout, which reads neither.
    """

    width: int
    first_row: int
    day: int
    prices: Mapping[str, int]
    volume: int | None = None
    splits: int | None = None


def _three_row_header(
    path: Path, lines: Sequence[Sequence[str]], vintage: Vintage, outcome: str
) -> _Header:
    """``Price`` and the column names, then ``Ticker`` and the ticker per column, then ``Date``."""
    if len(lines) < 3 or [line[:1] for line in lines[:3]] != [["Price"], ["Ticker"], ["Date"]]:
        raise BarImportRefused(
            f"{path} does not open with yfinance's three header rows, Price, Ticker and Date, "
            f"so {outcome}"
        )
    names = lines[0]
    columns: dict[str, int] = {}
    for field_name, header in _PRICE_COLUMNS.items():
        found = [index for index, name in enumerate(names) if name == header]
        if len(found) != 1:
            raise BarImportRefused(
                f"{path} names its {header} column {len(found)} time(s) in its Price row, "
                f"so {outcome}"
            )
        columns[field_name] = found[0]
    named = sorted({cell for cell in lines[1][1:] if cell})
    if named != [vintage.ticker]:
        raise BarImportRefused(
            f"{path} names {', '.join(named) or 'no ticker'} in its Ticker row, and its SHA-256 "
            f"is listed as {vintage.ticker}'s vintage, so {outcome}"
        )
    # A three-row file's row needs only to reach its last price column.
    return _Header(width=max(columns.values()) + 1, first_row=3, day=0, prices=columns)


def _one_row_header(path: Path, lines: Sequence[Sequence[str]], outcome: str) -> _Header:
    """One row of column names, each of the seven read here named exactly once."""
    names = lines[0] if lines else []
    columns: dict[str, int] = {}
    for header in (_DATE_COLUMN, *_PRICE_COLUMNS.values(), _VOLUME_COLUMN, _SPLITS_COLUMN):
        found = [index for index, name in enumerate(names) if name == header]
        if len(found) != 1:
            raise BarImportRefused(
                f"{path} line 1 names its {header} column {len(found)} time(s), so {outcome}"
            )
        columns[header] = found[0]
    return _Header(
        width=len(names),
        first_row=1,
        day=columns[_DATE_COLUMN],
        prices={name: columns[header] for name, header in _PRICE_COLUMNS.items()},
        volume=columns[_VOLUME_COLUMN],
        splits=columns[_SPLITS_COLUMN],
    )


def _finite(cell: str) -> float | None:
    try:
        value = float(cell)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _volume(cell: str) -> int | None:
    """The cell's whole shares, or ``None`` unless it is ASCII digits from 1 to 2**63 - 1."""
    if not _VOLUME_DIGITS.fullmatch(cell):
        return None
    value = int(cell)
    return value if 1 <= value <= _MAX_VOLUME else None


def _check_splits(
    path: Path, found: Mapping[date, tuple[int, float]], vintage: Vintage, outcome: str
) -> None:
    """Refuse unless the file's non-zero splits are the entry's, naming dates and no factor.

    ``found`` maps each split's date to its line and its factor.
    """
    listed = dict(vintage.splits)
    for day in sorted({*found, *listed}):
        if day not in listed:
            why = (
                f"line {found[day][0]} holds a split on {day.isoformat()} in its {_SPLITS_COLUMN} "
                f"column, which {vintage.ticker}'s entry does not list"
            )
        elif day not in found:
            why = (
                f"holds no split on {day.isoformat()} in its {_SPLITS_COLUMN} column, which "
                f"{vintage.ticker}'s entry lists"
            )
        elif found[day][1] != listed[day]:
            why = (
                f"line {found[day][0]} holds another {_SPLITS_COLUMN} factor for "
                f"{day.isoformat()} than {vintage.ticker}'s entry lists"
            )
        else:
            continue
        raise BarImportRefused(f"{path} {why}, so {outcome}")


def _read_source(
    path: Path, data: bytes, vintage: Vintage, outcome: str
) -> tuple[list[_SourceBar], int]:
    """The rows of ``data`` inside the entry's window and the count outside it, or a refusal.

    ``data`` is the bytes whose digest the list names, read from ``path`` once, and ``path`` is
    only for the refusal's words. The layouts differ in their header, in the columns they read and
    in their refusals' words, and one loop reads the rows of both. A row's date is parsed
    wherever it falls, its split is too, and its prices and volume are read only inside the
    window. A one-row file's refusals quote no price, volume or split value. ``outcome`` ends
    each refusal, because the undo reads the file too.
    """
    try:
        lines = list(csv.reader(io.StringIO(data.decode("utf-8"), newline="")))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise BarImportRefused(
            f"{path} cannot be read as CSV ({type(exc).__name__}), so {outcome}"
        ) from None
    one_row = vintage.layout == LAYOUT_ONE_ROW
    header = (
        _one_row_header(path, lines, outcome)
        if one_row
        else _three_row_header(path, lines, vintage, outcome)
    )

    def refused(number: int, why: str) -> BarImportRefused:
        return BarImportRefused(f"{path} line {number} {why}, so {outcome}")

    first, last = vintage.window or (date.min, date.max)
    seen: set[date] = set()
    splits: dict[date, tuple[int, float]] = {}
    rows: list[_SourceBar] = []
    outside = 0
    for number, line in enumerate(lines[header.first_row :], start=header.first_row + 1):
        if not line:
            continue
        # A one-row file's Date may sit past a short row's end, so its width is checked first.
        if one_row and len(line) < header.width:
            raise refused(number, f"has {len(line)} field(s) and ends before {lines[0][len(line)]}")
        # An extra cell, such as an unquoted thousands separator makes, shifts what follows it.
        if one_row and len(line) > header.width:
            raise refused(number, f"has {len(line)} field(s), more than its header names")
        cell = line[header.day]
        day = parse_date_dir(DATE_PREFIX + cell)
        if day is None:
            raise refused(
                number,
                f"has no YYYY-MM-DD date in its {_DATE_COLUMN} column"
                if one_row
                else f"starts with {cell!r}, which is not a YYYY-MM-DD date",
            )
        if day in seen:
            if one_row:
                raise refused(number, f"repeats {day.isoformat()} in its {_DATE_COLUMN} column")
            raise BarImportRefused(
                f"{path} holds {day.isoformat()} twice, and an import lands one partition per "
                f"session, so {outcome}"
            )
        seen.add(day)
        if len(line) < header.width:
            raise refused(number, f"has {len(line)} field(s), short of its header")
        if header.splits is not None:
            factor = _finite(line[header.splits])
            if factor is None:
                raise refused(
                    number,
                    f"has no finite number in its {_SPLITS_COLUMN} column on {day.isoformat()}",
                )
            if factor != 0:
                splits[day] = (number, factor)
        if not first <= day <= last:
            outside += 1
            continue
        values: dict[str, float] = {}
        for field_name, index in header.prices.items():
            name = _PRICE_COLUMNS[field_name]
            value = _finite(line[index])
            if one_row and (value is None or value <= 0):
                raise refused(number, f"has no finite number above zero in its {name} column")
            if value is None:
                raise refused(
                    number, f"has {line[index]!r} for {name}, which is not a finite number"
                )
            values[field_name] = value
        volume = None
        if header.volume is not None:
            volume = _volume(line[header.volume])
            if volume is None:
                raise refused(
                    number,
                    f"has no whole number from 1 to 2**63 - 1 in its {_VOLUME_COLUMN} column",
                )
        rows.append(_SourceBar(day=day, **values, volume=volume))
    if one_row:
        _check_splits(path, splits, vintage, outcome)
    if not rows:
        if vintage.window is None:
            raise BarImportRefused(f"{path} holds no rows, so {outcome}")
        raise BarImportRefused(
            f"{path} holds no row inside its window, {first.isoformat()} to {last.isoformat()}, "
            f"so {outcome}"
        )
    return rows, outside


def _missing_sessions(plan: _Plan, history: Calendar) -> int:
    """Refuse a date the exchange did not trade, and count the sessions the file lacks.

    The count runs over the plan's window, or over the file's own first to last date.
    """
    days = {bar.day for bar in plan.bars}
    for day in sorted(days):
        if not history.is_session(day):
            raise BarImportRefused(
                f"{plan.path}: {day.isoformat()} is not a session, so nothing was imported"
            )
    missing = 0
    day, last = plan.window or (min(days), max(days))
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
    outcome: str = "nothing was imported",
) -> tuple[int, date]:
    """The ticker's current instrument and its first capture span session.

    The first span session is the earliest first session of any span of any instrument the
    ticker has named, because ``bars/`` paths are keyed by ticker.
    """
    try:
        current = master.resolve(ticker, today, id_type=ID_TYPE_TICKER)
    except SecurityMasterError as exc:
        raise BarImportRefused(f"{exc}, so {outcome}") from None
    if current is None:
        raise BarImportRefused(
            f"the security master resolves no instrument for {ticker} today, so it is not "
            f"onboarded and {outcome}. Run python -m lake.onboard {ticker} first"
        )
    owned = [
        span
        for instrument in sorted(master.instruments_named(ticker, ID_TYPE_TICKER))
        for span in spans.spans_of(instrument)
    ]
    if not owned:
        raise BarImportRefused(
            f"{ticker} has no capture span, so nothing says which sessions capture covers and "
            f"{outcome}. Run python -m lake.onboard, which opens one"
        )
    sessions = SessionClock(clock, calendar)
    firsts = [first for span in owned if (first := _first_covered(span, sessions, calendar))]
    if not firsts:
        raise BarImportRefused(
            f"no capture span of {ticker} covers a session the calendar names, so nothing says "
            f"which sessions capture covers and {outcome}"
        )
    return current, min(firsts)


def _read_master_and_spans(root: Path, outcome: str) -> tuple[SecurityMaster, CaptureSpans]:
    """The security master and the capture spans, or a refusal ending in ``outcome``."""
    try:
        return read_master(root), read_capture_spans(root)
    except (MasterAbsent, SpansAbsent, SecurityMasterError, CaptureSpansError) as exc:
        raise BarImportRefused(f"{exc}, so {outcome}") from None
    except OSError as exc:
        raise BarImportRefused(
            f"the security master or the capture spans under {root} cannot be read "
            f"({type(exc).__name__}), so {outcome}"
        ) from None


def _holder(rel: str, held: str) -> str:
    if held.startswith(BAR_IMPORT_SOURCE_PREFIX):
        return f"{rel} is already held by the import {held}"
    return f"{rel} is already held by captured bars (manifest source {held!r})"


def _whole_lines(raw: bytes, cost: str, outcome: str) -> tuple[list[int], list[str], list[str]]:
    """Where each line of the manifest starts, its source and its partition.

    Refuses a torn last line, whose ``cost`` the refusal names, and a line that is not an entry. A
    read of the manifest stops at the first line it cannot parse, so entries recorded past one are
    hidden from every reader while the file holds them.
    """
    if raw and not raw.endswith(b"\n"):
        raise BarImportRefused(f"manifest.jsonl ends in a torn line, so {cost} and {outcome}")
    offsets: list[int] = []
    sources: list[str] = []
    partitions: list[str] = []
    at = 0
    for number, line in enumerate(raw.split(b"\n")[:-1], start=1):
        offsets.append(at)
        at += len(line) + 1
        try:
            entry = json.loads(line)
            sources.append(str(entry["source"]))
            partitions.append(str(entry["partition"]))
        except (ValueError, KeyError, TypeError):
            raise BarImportRefused(
                f"line {number} of manifest.jsonl is not a whole entry, so {outcome}"
            ) from None
    return offsets, sources, partitions


def _refuse_damaged_manifest(root: Path) -> None:
    """Refuse an import that would append behind damage.

    Its entries would be hidden from every reader, capture's later ones with them, and the undo
    refuses on the same damage, so nothing could take the import back.
    """
    _whole_lines(
        manifest_path(root).read_bytes(),
        "an entry appended to it would be lost",
        "nothing was imported",
    )


def _decide(
    root: Path,
    plans: Sequence[_Plan],
    latest: Mapping[str, Mapping],
    trimmed_latest: Mapping[str, Mapping],
) -> None:
    """Sort every key into skipped or to write, or refuse the whole run on the first conflict.

    A key the manifest records under the plan's own identity is skipped while its file is on
    disk or the trimmed ledger calls its absence designed. Its file missing otherwise is what an
    undo stopped between its deletions and its cut leaves once anything else was recorded, and no
    undo can finish that, so the import writes the file again and records it again, which ends
    the nightly upload's refusal on the missing file. Any other recorded key conflicts. An
    unrecorded file at a key is replaced only when its metadata names the plan's own source
    identity, which a killed run of this import leaves, or when it cannot be read, which a torn
    write leaves. Any other unrecorded file conflicts, because nothing says it is
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
                if held != plan.manifest_source:
                    conflicts.append(_holder(rel, held))
                elif target.exists() or is_designed_absence(rel, latest, trimmed_latest):
                    plan.report.skipped += 1
                else:
                    plan.report.rewritten += 1
                    plan.rewrites.append(rel)
                    plan.todo.append(bar)
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


def _check_rewrites(plans: Sequence[_Plan], refuse) -> None:
    """Ask each plan's ``rewrite_check`` about its missing recorded keys, before any write."""
    for plan in plans:
        if plan.rewrites and plan.rewrite_check is not None:
            why = plan.rewrite_check(plan.rewrites)
            if why is not None:
                raise refuse(why)


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
            _refuse_damaged_manifest(root)
            _decide(root, plans, latest_entries(root), latest_trimmed(root))
        except ManifestError as exc:
            raise BarImportRefused(f"{exc}, so nothing was imported") from None
        _check_rewrites(plans, refuse)
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
            _refuse_damaged_manifest(root)
            _decide(root, plans, latest_entries(root), latest_trimmed(root))
            _check_rewrites(plans, refuse)
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
                        if bar.row is not None:
                            rows = [dict(bar.row)]
                        else:
                            candle = {
                                "datetime": int(session_midnight(bar.day).timestamp() * 1000),
                                "open": bar.open,
                                "high": bar.high,
                                "low": bar.low,
                                "close": bar.close,
                            }
                            if bar.volume is not None:
                                candle["volume"] = bar.volume
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
    the sources' dates and defaults to the exchange calendar, started ahead of the earliest of
    them and of any window.
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
            # Read once, so the bytes parsed are the bytes whose digest the list names.
            data = path.read_bytes()
        except OSError as exc:
            raise BarImportRefused(
                f"{path} cannot be read ({type(exc).__name__}), so nothing was imported"
            ) from None
        digest = sha256_bytes(data)
        vintage = listed.get(digest)
        if vintage is None:
            raise BarImportRefused(
                f"{path} has SHA-256 {digest}, so the file is not one the list pins and nothing "
                "was imported"
            )
        if any(plan.ticker == vintage.ticker for plan in plans):
            raise BarImportRefused(
                f"two sources name {vintage.ticker}, and one partition per ticker-day means the "
                "first source imported holds each day, so nothing was imported"
            )
        rows, outside = _read_source(path, data, vintage, "nothing was imported")
        plans.append(
            _Plan(
                ticker=vintage.ticker,
                provenance=vintage.provenance,
                manifest_source=vintage.manifest_source,
                path=path,
                bars=rows,
                report=TickerImport(
                    vintage.ticker,
                    vintage.manifest_source,
                    len(rows),
                    dropped=None if vintage.window is None else outside,
                ),
                window=vintage.window,
            )
        )

    master, spans = _read_master_and_spans(root, "nothing was imported")
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
        # itself rather than answering no. A window can open before the earliest row.
        starts = [bar.day for plan in plans for bar in plan.bars]
        starts += [plan.window[0] for plan in plans if plan.window is not None]
        earliest = min(starts)
        history = ExchangeCalendar(start=earliest - timedelta(days=_SESSION_LOOKAHEAD_DAYS))
    for plan in plans:
        plan.report.missing = _missing_sessions(plan, history)

    return _land(root, plans, clock=clock, calendar=calendar, dry_run=dry_run)


# How many sessions before the first capture span session the Schwab import lands. The runtime's
# first z-score needs 40 closes: 20 to fit the first hedge ratio and 20 spreads to fill the
# window, per quantitative-trading's ``chan.price_spread`` and ``chan.bollinger`` (marketlake #951).
SCHWAB_SESSIONS = 40

# What each Schwab partition's provenance names as its source.
SCHWAB_SOURCE = "schwab:pricehistory"

# How a dead or unreadable token is repaired on the primary, the hosted VM, where the import
# runs. ``lake.reauth`` drives a browser, which the VM does not have, so the laptop mints the
# token into the token parameter and the VM pulls it, as the README's token section says.
_TOKEN_REPAIR = (
    "Run reauth.sh on the laptop, then python -m lake.token_store pull on this host, then run "
    "the import again"
)

# The scheme ``Provenance.source_id`` carries before a Schwab import's ticker and window.
_SCHWAB_WINDOW_SCHEME = "schwab-window:"

# How far back from the first span session the window's sessions are looked for. Forty sessions
# take 56 to 60 calendar days with the year's holidays, so this leaves room for a long closure.
_SCHWAB_LOOKBACK_DAYS = 120


@dataclass(frozen=True)
class _SchwabWindow:
    """One ticker's Schwab import: its instrument, its sessions and its identity."""

    ticker: str
    instrument_id: int
    first_span: date
    sessions: tuple[date, ...]

    @property
    def source_id(self) -> str:
        first, last = self.sessions[0].isoformat(), self.sessions[-1].isoformat()
        return f"{_SCHWAB_WINDOW_SCHEME}{self.ticker}:{first}..{last}"

    @property
    def manifest_source(self) -> str:
        """Each partition's manifest ``source``, built from the same identity as the file's."""
        return BAR_IMPORT_SOURCE_PREFIX + self.source_id

    def provenance(self, fetched_on: date) -> Provenance:
        """What each partition's schema metadata records, adjusted as of the fetch's date."""
        return Provenance(
            ORIGIN_IMPORTED, BASIS_ADJUSTED, SCHWAB_SOURCE, self.source_id, fetched_on
        )


def _sessions_before(first: date, calendar: Calendar, outcome: str) -> tuple[date, ...]:
    """The ``SCHWAB_SESSIONS`` sessions ending the session before ``first``, oldest first."""
    found: list[date] = []
    day = first - timedelta(days=1)
    floor = first - timedelta(days=_SCHWAB_LOOKBACK_DAYS)
    while len(found) < SCHWAB_SESSIONS and day >= floor:
        if calendar.is_session(day):
            found.append(day)
        day -= timedelta(days=1)
    if len(found) < SCHWAB_SESSIONS:
        raise BarImportRefused(
            f"the calendar names {len(found)} session(s) in the {_SCHWAB_LOOKBACK_DAYS} days "
            f"before {first.isoformat()}, short of {SCHWAB_SESSIONS}, so {outcome}"
        )
    return tuple(reversed(found))


def _schwab_windows(
    root: Path,
    tickers: Sequence[str],
    *,
    clock: Clock,
    calendar: Calendar,
    outcome: str,
) -> list[_SchwabWindow]:
    """Each ticker's window, read from the lake alone, so the import and its undo agree."""
    if len(set(tickers)) != len(tickers):
        raise BarImportRefused(f"a ticker is named twice, so {outcome}")
    master, spans = _read_master_and_spans(root, outcome)
    today = clock.now().astimezone(MARKET_TZ).date()
    windows: list[_SchwabWindow] = []
    for ticker in tickers:
        instrument_id, first = _first_span_session(
            ticker, master, spans, today=today, clock=clock, calendar=calendar, outcome=outcome
        )
        sessions = _sessions_before(first, calendar, outcome)
        windows.append(_SchwabWindow(ticker, instrument_id, first, sessions))
    return windows


def _fetch_schwab(vendor: Vendor, window: _SchwabWindow, *, clock: Clock):
    """One daily response covering the window, with the instants the rows record.

    The bounds are the first and last sessions' Eastern midnights. Schwab clips a daily window
    to its bounds' dates and includes the end date's candle (marketlake #416, and #951's probe),
    so nothing on or after the first span session is asked for.
    """
    start = session_midnight(window.sessions[0])
    end = session_midnight(window.sessions[-1])
    fetch_ts = clock.now()
    try:
        response = vendor.get_daily_bars(window.ticker, start=start, end=end)
    except VendorAuthError as exc:
        raise BarImportRefused(
            f"the vendor refused the credentials ({type(exc).__name__}), so nothing was "
            f"imported. {_TOKEN_REPAIR}"
        ) from None
    except VendorError as exc:
        raise BarImportRefused(
            f"Schwab's daily bars for {window.ticker} failed ({type(exc).__name__}: {exc}), so "
            "nothing was imported. Run the import again"
        ) from None
    except Exception as exc:
        # An httpx timeout or dropped connection is the network's, and a rerun meets it afresh.
        # Anything else is a bug and keeps its traceback, which a cassette miss in a test needs.
        if not is_transient_failure(exc):
            raise
        raise BarImportRefused(
            f"Schwab's daily bars for {window.ticker} did not arrive ({type(exc).__name__}), so "
            "nothing was imported. Run the import again"
        ) from None
    fetch_end_ts = clock.now()
    if not 200 <= response.status < 300:
        raise BarImportRefused(
            f"Schwab answered {window.ticker}'s daily bars with status {response.status}, so "
            "nothing was imported. Run the import again"
        )
    return response.body, (fetch_ts, fetch_end_ts, start, end)


def _schwab_bars(window: _SchwabWindow, body, stamps, report: TickerImport) -> list[_SourceBar]:
    """The window's sessions from one response, each with the row the sweep's builder makes.

    Rows come from ``journal.bars_rows``, so ``bar_ts`` is the vendor's own stamp and ``volume``
    its own count. Each stamp's session is ``bars.daily_session_of``'s, and a candle naming a
    session outside the window is dropped. Two naming one session refuse, a price that is not a
    finite number refuses, and so does a row its column refuses, checked here by building the
    batch rather than at the write under the lock. A session the response lacks is counted, and
    so are the candles dropped and those the builder could not read, so the line says which.
    """
    fetch_ts, fetch_end_ts, start, end = stamps
    rows = bars_rows(
        body,
        ticker=window.ticker,
        freq=DAILY_FREQ,
        instrument_id=window.instrument_id,
        fetch_ts=fetch_ts,
        fetch_end_ts=fetch_end_ts,
        window_start=start,
        window_end=end,
        extended_hours=None,
    )
    wanted = set(window.sessions)
    candles = body.get("candles")
    sent = len(candles) if isinstance(candles, list) else 0
    report.unreadable = sent - len(rows)
    report.dropped = 0
    by_session: dict[date, Mapping[str, object]] = {}
    for row in rows:
        day = daily_session_of(str(row["bar_ts"]))
        if day not in wanted:
            report.dropped += 1
            continue
        if day in by_session:
            raise BarImportRefused(
                f"Schwab's response for {window.ticker} holds two candles for {day.isoformat()}, "
                "and an import lands one partition per session, so nothing was imported"
            )
        by_session[day] = row
    found: list[_SourceBar] = []
    for day in sorted(by_session):
        row = by_session[day]
        prices = {name: row.get(name) for name in _PRICE_COLUMNS}
        for name, value in prices.items():
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
            ):
                raise BarImportRefused(
                    f"Schwab's {window.ticker} candle for {day.isoformat()} has {value!r} for "
                    f"{name}, which is not a finite number, so nothing was imported"
                )
        try:
            bars_data_batch([row])
        except UNFIT_ERRORS as exc:
            raise BarImportRefused(
                f"Schwab's {window.ticker} candle for {day.isoformat()} does not fit the bars "
                f"schema ({type(exc).__name__}), so nothing was imported"
            ) from None
        found.append(_SourceBar(day=day, **prices, row=row))
    report.missing = len(wanted) - len(found)
    return found


def _restore_repair(ticker: str, rels: Sequence[str]) -> str:
    days = sorted(ref.day for rel in rels if (ref := parse_bars_rel(rel)) is not None)
    return (
        f"python -m lake.bucket restore-range --surface bars --ticker {ticker} "
        f"--from {days[0].isoformat()} --to {days[-1].isoformat()}"
    )


def _unsent_check(ticker: str, bucket) -> Callable[[Sequence[str]], str | None]:
    """A ``rewrite_check`` that refuses once the bucket holds any of the missing keys."""

    def check(rels: Sequence[str]) -> str | None:
        if bucket is None:
            return (
                f"{rels[0]} is recorded under this import and missing, and with no bucket to "
                "read nothing says the upload has not sent it. Restore it with "
                f"{_restore_repair(ticker, rels)}"
            )
        target, client = bucket()
        try:
            listing = list_bucket(client, target)
        except Exception as exc:  # noqa: BLE001 - every bucket failure is sorted by _failure
            sorted_failure = _bucket._failure(exc)
            if sorted_failure is None:
                raise
            kind, detail = sorted_failure
            return (
                f"{rels[0]} is recorded under this import and missing, and the bucket read was "
                f"{kind} with {detail}, so nothing says the upload has not sent it. Run the "
                f"import again: {target}"
            )
        sent = sorted(rel for rel in rels if rel in listing)
        if sent:
            return (
                f"{sent[0]} is recorded under this import and missing here, and the bucket holds "
                "it, so a fresh fetch, which may carry revised values, must not replace it. "
                f"Restore it with {_restore_repair(ticker, sent)}"
            )
        return None

    return check


def import_schwab(
    lake_root: Path | str,
    tickers: Sequence[str],
    *,
    vendor: Vendor,
    clock: Clock,
    calendar: Calendar,
    dry_run: bool = False,
    bucket: Callable[[], tuple[BucketTarget, object]] | None = None,
) -> ImportSummary:
    """Import Schwab's daily bars for the ``SCHWAB_SESSIONS`` sessions before each ticker's capture.

    The window ends the session before the ticker's first capture span session, as
    ``_first_span_session`` reads it. Every rule of ``_land`` holds, and the vintage import's
    run-window and lake-root checks run first. Each ticker's response is fetched before the lock,
    because capture waits on it. A dry run fetches, checks every rule and writes nothing. The
    module docstring carries the basis and the identity.

    ``bucket`` returns the bucket's target and client, and is called only when a recorded key's
    file is missing. That key is written again only while the bucket lists none of the plan's
    missing keys, which is the state an undo stopped part-way leaves before the upload. Once
    the upload has sent a key, the fetch may carry revised values, so the repair is a restore of
    the sent copy. With no ``bucket``, a missing recorded key refuses.
    """
    root = Path(lake_root)
    outcome = "nothing was imported"
    why = _run_window_refusal(clock.now(), clock=clock, calendar=calendar, words=_RUN_WINDOW_WORDS)
    if why is not None:
        raise BarImportRefused(f"{why}. Nothing was written")
    if not manifest_path(root).is_file():
        raise BarImportRefused(
            f"lake_root {root} holds no manifest.jsonl, so it may name the wrong directory and "
            "nothing was imported"
        )
    windows = _schwab_windows(root, tickers, clock=clock, calendar=calendar, outcome=outcome)
    today = clock.now().astimezone(MARKET_TZ).date()
    for window in windows:
        if window.sessions[-1] >= today:
            raise BarImportRefused(
                f"{window.ticker}'s last session to import is {window.sessions[-1].isoformat()}, "
                "and an imported daily bar has no close gate to check it on its own day, so "
                "nothing was imported. Run it on "
                f"{(window.sessions[-1] + timedelta(days=1)).isoformat()} or later"
            )

    plans: list[_Plan] = []
    for window in windows:
        body, stamps = _fetch_schwab(vendor, window, clock=clock)
        fetched_on = stamps[0].astimezone(MARKET_TZ).date()
        report = TickerImport(
            window.ticker, window.manifest_source, len(window.sessions), holder="the response"
        )
        found = _schwab_bars(window, body, stamps, report)
        report.rows = len(found)
        plans.append(
            _Plan(
                ticker=window.ticker,
                provenance=window.provenance(fetched_on),
                manifest_source=window.manifest_source,
                path=Path(f"{SCHWAB_SOURCE}/{window.ticker}"),
                bars=found,
                report=report,
                instrument_id=window.instrument_id,
                rewrite_check=_unsent_check(window.ticker, bucket),
            )
        )
    empty = [plan.ticker for plan in plans if not plan.bars]
    if empty:
        raise BarImportRefused(
            f"Schwab's response for {empty[0]} holds none of its {SCHWAB_SESSIONS} sessions, so "
            "nothing was imported"
        )
    return _land(root, plans, clock=clock, calendar=calendar, dry_run=dry_run)


@dataclass
class UndoSummary:
    """What one undo removed, for the line the command prints."""

    lines: int = 0
    partitions: int = 0
    temps_removed: list[str] = field(default_factory=list)

    def render(self) -> str:
        if not self.lines:
            return "nothing to undo: the manifest records none of these imports"
        return (
            f"undid {self.lines} manifest line(s) and removed {self.partitions} partition(s) "
            f"and {len(self.temps_removed)} leftover temp file(s)"
        )


def _import_tail(raw: bytes, identities: set[str]) -> tuple[int, list[str], set[str]]:
    """Where the manifest's trailing run of these imports' entries starts, their partitions, and
    every partition any line names.

    Refuses a torn last line, a line that is not an entry, and an entry of these imports that
    sits before an entry of anything else, which the cut would then take with it.
    """
    offsets, sources, partitions = _whole_lines(
        raw, "the undo cannot tell where the import starts", "nothing was undone"
    )
    start = len(sources)
    while start > 0 and sources[start - 1] in identities:
        start -= 1
    earlier = [index for index in range(start) if sources[index] in identities]
    if earlier:
        after = {sources[index] for index in range(earlier[0] + 1, start)} - identities
        if after and all(source.startswith(BAR_IMPORT_SOURCE_PREFIX) for source in after):
            raise BarImportRefused(
                f"line {earlier[0] + 1} of manifest.jsonl records {partitions[earlier[0]]} for "
                f"this import, and the import {sorted(after)[0]} was recorded after it, so "
                "nothing was undone. Undo that import first, then this one, then run that import "
                "again if it was good"
            )
        raise BarImportRefused(
            f"line {earlier[0] + 1} of manifest.jsonl records {partitions[earlier[0]]} for this "
            "import, and something else was recorded after it, so cutting the manifest back "
            "would take that too and nothing was undone. No command undoes an import once "
            "anything else has been recorded after it"
        )
    tail = partitions[start:]
    before = set(partitions[:start])
    for rel in tail:
        if rel in before:
            # Cutting the tail would make that earlier entry the latest again, for a file the
            # undo deletes. The import never writes such a key, and this does not rely on it.
            raise BarImportRefused(
                f"manifest.jsonl records {rel} before this import too, so cutting the import's "
                "lines would leave that entry naming a deleted file, and nothing was undone"
            )
        if parse_bars_rel(rel) is None:
            raise BarImportRefused(
                f"manifest.jsonl records {rel} under this import, which is not a bars "
                "partition, so nothing was undone"
            )
    cut = offsets[start] if start < len(offsets) else len(raw)
    return cut, tail, before.union(tail)


# How the undo words its window refusals. Past the bound no undo can run before capture records
# the session's first minute after the import, so it never says to run it later that day.
_UNDO_WINDOW_WORDS = RunWindowWords(
    job="the undo",
    in_flight="a partition in flight",
    sunday_repair="Run it after 23:30, if nothing has been recorded after the import by then",
    bound_repair=(
        "Past this bound capture records the session's first minute before any undo can run, so "
        "the import stays, that evening's upload sends it, and correcting it is new work for its "
        "own issue"
    ),
)


def _undo_deadline(*, clock: Clock, calendar: Calendar) -> str:
    """When a stopped undo must run again: before the next upload, and the bound at the latest.

    The bound is ``bucket.session_bound``'s, the one the window refusal checks, so the text names
    its date as well as its time. An undo that fails in the evening has until the next session
    day's bound, not that day's. Every compaction uploads, and one can come first, as the comment
    on ``_RUN_WINDOW_WORDS`` says, so the bound is only the latest moment. ``None`` means no
    session in the calendar's lookahead.
    """
    bound = session_bound(clock.now(), clock=clock, calendar=calendar)
    when = "the next session's capture start" if bound is None else bound.isoformat()
    return f"before any compaction uploads the import, and before {when} at the latest"


def _bucket_refusal(exc: Exception, target: BucketTarget, deadline: str) -> BarImportRefused:
    """One line naming how the bucket read failed, by ``bucket._failure``, or the error itself.

    A ``None`` from ``_failure`` is a bug rather than a bucket failure, so it is not hidden.
    ``deadline`` is ``_undo_deadline``'s text.
    """
    sorted_failure = _bucket._failure(exc)
    if sorted_failure is None:
        raise exc
    kind, detail = sorted_failure
    repair = {
        "refused": f"Fix the instance profile's bucket policy, then run the undo again {deadline}",
        "unreachable": f"Run the undo again at once, {deadline}",
    }.get(kind, f"Run the undo again {deadline}")
    return BarImportRefused(
        f"the bucket read was {kind} with {detail}, so nothing says the import has not reached "
        f"the bucket and nothing was undone. {repair}: {target}"
    )


def _check_bucket(
    client: object,
    target: BucketTarget,
    kept: bytes,
    tail: Sequence[str],
    deadline: Callable[[], str],
):
    """The bucket's copy state, after refusing a bucket that holds any of the import."""
    try:
        copy = read_copy_state(client, target, kept)
        listing = list_bucket(client, target)
    except Exception as exc:  # noqa: BLE001 - every bucket failure is sorted by _failure
        raise _bucket_refusal(exc, target, deadline()) from None
    if copy.present and not copy.is_prefix:
        raise BarImportRefused(
            "the bucket's manifest.jsonl is not a byte prefix of this lake's cut back to before "
            "the import, so the bucket may already hold the import, and no command withdraws "
            f"anything from it. Nothing was undone: {target}"
        )
    sent = sorted(rel for rel in tail if rel in listing)
    if sent:
        raise BarImportRefused(
            f"the bucket already holds {sent[0]} and {len(sent) - 1} more of the import's "
            f"partitions, so no command withdraws them from it. Nothing was undone: {target}"
        )
    return copy


def undo_vintages(
    lake_root: Path | str,
    sources: Sequence[Path],
    *,
    client: object,
    target: BucketTarget,
    clock: Clock,
    calendar: Calendar,
    vintages: Mapping[str, Vintage] | None = None,
) -> UndoSummary:
    """Undo the imports of the listed vintages in ``sources``, while the bucket holds none of it.

    The module docstring gives the rule. ``client`` and ``target`` are the bucket's, read only.
    The bucket is read twice: once before the lock, so a refusal costs capture nothing, and again
    under the lock, because an upload that ran while the undo waited for the lock appends nothing
    to this lake's manifest and would otherwise go unseen.
    """
    root = Path(lake_root)
    listed = VINTAGES if vintages is None else vintages

    def window() -> str | None:
        return _run_window_refusal(
            clock.now(), clock=clock, calendar=calendar, words=_UNDO_WINDOW_WORDS
        )

    why = window()
    if why is not None:
        raise BarImportRefused(f"{why}. Nothing was undone")
    path = manifest_path(root)
    if not path.is_file():
        raise BarImportRefused(
            f"lake_root {root} holds no manifest.jsonl, so it may name the wrong directory and "
            "nothing was undone"
        )
    chosen: list[tuple[Path, bytes, Vintage]] = []
    for source in sources:
        try:
            data = Path(source).read_bytes()
        except OSError as exc:
            raise BarImportRefused(
                f"{source} cannot be read ({type(exc).__name__}), so nothing was undone"
            ) from None
        digest = sha256_bytes(data)
        vintage = listed.get(digest)
        if vintage is None:
            raise BarImportRefused(
                f"{source} has SHA-256 {digest}, which is no vintage this module lists, so "
                "nothing was undone"
            )
        chosen.append((Path(source), data, vintage))
    identities = {vintage.manifest_source for _source, _data, vintage in chosen}

    def on_disk() -> list[Path]:
        paths = LakePaths(root)
        return [
            paths.bars_partition_path(vintage.ticker, DAILY_FREQ, bar.day)
            for source, data, vintage in chosen
            for bar in _read_source(source, data, vintage, "nothing was undone")[0]
        ]

    return _undo(
        root,
        identities,
        on_disk,
        client=client,
        target=target,
        window=window,
        deadline=lambda: _undo_deadline(clock=clock, calendar=calendar),
    )


def _undo(
    root: Path,
    identities: set[str],
    keys: Callable[[], Sequence[Path]],
    *,
    client: object,
    target: BucketTarget,
    window: Callable[[], str | None],
    deadline: Callable[[], str],
) -> UndoSummary:
    """The undo's rule for any import, once its identities are known.

    ``keys`` names every partition the imports would have written. It is read on every undo, to
    refuse a killed import's unrecorded files, which the cut would leave behind. ``window`` is the
    caller's run-window check, so each refusal says which undo stopped. ``deadline`` is
    ``_undo_deadline``'s text, read when a refusal needs it.
    """
    path = manifest_path(root)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise BarImportRefused(
            f"{path} cannot be read ({type(exc).__name__}), so nothing was undone"
        ) from None
    cut, tail, named = _import_tail(raw, identities)
    summary = UndoSummary()
    # A killed import can leave renamed files with no entry, in its second ticker with the first
    # one's recorded, and the undo cannot see those in the manifest. The import's rerun records
    # them, after which an undo removes them.
    wanted = [(key, key.relative_to(root).as_posix()) for key in keys()]

    def refuse_unrecorded() -> None:
        for key, rel in wanted:
            if rel in named:
                continue
            try:
                found = key.exists()
            except OSError as exc:
                raise BarImportRefused(
                    f"{rel} cannot be looked for ({type(exc).__name__}), so nothing was undone"
                ) from None
            if found:
                raise BarImportRefused(
                    f"{rel} is on disk and no manifest line names it, which a killed import "
                    "leaves. Run the import again to record it, then run the undo"
                )

    refuse_unrecorded()
    if not tail:
        return summary
    if cut == 0:
        raise BarImportRefused(
            "every line of manifest.jsonl is this import's, so the undo would leave it empty, "
            "which reads as a prefix of any bucket copy. Nothing was undone"
        )

    first = _check_bucket(client, target, raw[:cut], tail, deadline)

    try:
        with lake_lock(root):
            why = window()
            if why is not None:
                raise BarImportRefused(f"{why}. Nothing was undone")
            if path.read_bytes() != raw:
                raise BarImportRefused(
                    "something was recorded in manifest.jsonl while the undo checked the bucket, "
                    "so the import's lines are no longer its last ones and nothing was undone. "
                    "No command undoes an import once anything is recorded after it"
                )
            # An import killed while it held the lock changes no manifest byte.
            refuse_unrecorded()
            # The window check above keeps capture off the lock, so this second read costs it
            # nothing.
            again = _check_bucket(client, target, raw[:cut], tail, deadline)
            if (again.present, again.length, again.stored) != (
                first.present,
                first.length,
                first.stored,
            ):
                raise BarImportRefused(
                    "the bucket's manifest.jsonl changed while the undo waited for the lock, so "
                    f"an upload may have run, and nothing was undone. Run the undo again: {target}"
                )
            directories: dict[Path, set[str]] = {}
            for rel in tail:
                partition = root / rel
                directories.setdefault(partition.parent, set()).add(partition.name)
                if partition.exists():
                    partition.unlink()
                    summary.partitions += 1
            for directory, names in sorted(directories.items()):
                for temp in leftover_temps(directory, names):
                    temp.unlink()
                    summary.temps_removed.append(temp.relative_to(root).as_posix())
            flushed = {
                parent
                for directory in directories
                for parent in (directory, directory.parent, directory.parent.parent)
                if parent.is_dir()
            }
            for directory in sorted(flushed):
                _fsync_path(directory)
            try:
                _bucket._rewrite_manifest(root, cut, b"", raw[:cut])
            except _bucket.ResyncRefused:
                raise BarImportRefused(
                    f"{path} does not read back as the manifest cut back to before the import. "
                    "Copy the bucket's manifest.jsonl over it before anything else writes, since "
                    "the bucket's copy ends before the import"
                ) from None
            summary.lines = len(tail)
    except BarImportRefused:
        raise
    except OSError as exc:
        raise BarImportRefused(
            f"undoing the import under {root} failed ({type(exc).__name__}: "
            f"{exc.strerror or exc}). Run the undo again {deadline()}. It finishes what "
            "this one started. Past that deadline, run the import again instead, which writes the "
            "missing files back, because the nightly upload stops on a recorded file it cannot "
            "find"
        ) from None
    return summary


def undo_schwab(
    lake_root: Path | str,
    tickers: Sequence[str],
    *,
    client: object,
    target: BucketTarget,
    clock: Clock,
    calendar: Calendar,
) -> UndoSummary:
    """Undo the Schwab imports of ``tickers``, while the bucket holds none of them.

    Each import's identity is its ticker and window, which ``_schwab_windows`` reads from the
    lake alone, so no vendor call is made. The rule is ``undo_vintages``'.
    """
    root = Path(lake_root)

    def window() -> str | None:
        return _run_window_refusal(
            clock.now(), clock=clock, calendar=calendar, words=_UNDO_WINDOW_WORDS
        )

    why = window()
    if why is not None:
        raise BarImportRefused(f"{why}. Nothing was undone")
    if not manifest_path(root).is_file():
        raise BarImportRefused(
            f"lake_root {root} holds no manifest.jsonl, so it may name the wrong directory and "
            "nothing was undone"
        )
    windows = _schwab_windows(
        root, tickers, clock=clock, calendar=calendar, outcome="nothing was undone"
    )
    paths = LakePaths(root)

    def on_disk() -> list[Path]:
        return [
            paths.bars_partition_path(w.ticker, DAILY_FREQ, day)
            for w in windows
            for day in w.sessions
        ]

    identities = {w.manifest_source for w in windows}
    return _undo(
        root,
        identities,
        on_disk,
        client=client,
        target=target,
        window=window,
        deadline=lambda: _undo_deadline(clock=clock, calendar=calendar),
    )


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
        help="Import quantitative-trading vintages or Yahoo files the module lists, by SHA-256.",
    )
    vintage.add_argument(
        "sources",
        nargs="+",
        type=_source_file,
        metavar="CSV",
        help="A listed CSV outside lake_root. Its SHA-256 names its ticker.",
    )
    vintage.add_argument(
        "--dry-run",
        action="store_true",
        help="Check every rule, the conflicts included, and write nothing.",
    )
    vintage.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    undo = commands.add_parser(
        "undo",
        help="Undo the imports of these vintages, while the bucket holds none of them.",
    )
    undo.add_argument(
        "sources",
        nargs="+",
        type=_source_file,
        metavar="CSV",
        help="A listed CSV the import read. Its SHA-256 names the import to undo.",
    )
    undo.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    schwab = commands.add_parser(
        "schwab",
        help=f"Import Schwab's daily bars for the {SCHWAB_SESSIONS} sessions before capture began.",
    )
    schwab.add_argument(
        "tickers",
        nargs="+",
        metavar="TICKER",
        help="An onboarded ticker. Its first capture span session ends the window.",
    )
    schwab.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch, check every rule, the conflicts included, and write nothing.",
    )
    schwab.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    schwab_undo = commands.add_parser(
        "schwab-undo",
        help="Undo the Schwab imports of these tickers, while the bucket holds none of them.",
    )
    schwab_undo.add_argument(
        "tickers", nargs="+", metavar="TICKER", help="A ticker whose Schwab import to undo."
    )
    schwab_undo.add_argument(
        "--config", help="Path to config.yaml (defaults to the standard place)."
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
    vendor_factory: Callable[..., Vendor] | None = None,
) -> int:
    """The ``python -m lake.bar_import`` entry. Returns a process exit code.

    It exits 0 when every row is in the lake, a rerun of a finished import included, when a
    dry run found nothing to refuse, or when an undo finished or found nothing to undo, and 2
    on a refusal, with one line. A shadow host refuses before anything is read. ``undo`` and
    ``schwab-undo`` build the bucket's client from the config, which they only read. ``schwab``
    builds the vendor through ``vendor_factory``, ``SchwabVendor.from_token`` by default, from
    the token file and the config's two secrets, as the bars sweep does.
    """
    args = build_parser().parse_args(argv)
    with input_errors_exit(COMMAND, BarImportRefused, _bucket.BucketRefusal):
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
        calendar = calendar if calendar is not None else ExchangeCalendar()
        if args.command in ("undo", "schwab-undo"):
            target, client = connect(config)
            undo = undo_vintages if args.command == "undo" else undo_schwab
            undone = undo(
                config.lake_root,
                args.sources if args.command == "undo" else args.tickers,
                client=client,
                target=target,
                clock=clock,
                calendar=calendar,
            )
            print(f"{COMMAND}: {undone.render()}")
            return 0
        if args.command == "schwab":
            why = _run_window_refusal(
                clock.now(), clock=clock, calendar=calendar, words=_RUN_WINDOW_WORDS
            )
            if why is not None:
                raise BarImportRefused(f"{why}. Nothing was written")
            factory = SchwabVendor.from_token if vendor_factory is None else vendor_factory
            token = default_token_path()
            try:
                vendor = factory(
                    token,
                    api_key=config.schwab_api_key.reveal(),
                    app_secret=config.schwab_app_secret.reveal(),
                )
            except (OSError, ValueError) as exc:
                # schwab-py reads the token file while it builds the client.
                raise BarImportRefused(
                    f"the Schwab token at {token} cannot be read ({type(exc).__name__}), so "
                    f"nothing was imported. {_TOKEN_REPAIR}"
                ) from None
            summary = import_schwab(
                config.lake_root,
                args.tickers,
                vendor=vendor,
                clock=clock,
                calendar=calendar,
                dry_run=args.dry_run,
                bucket=lambda: connect(config),
            )
        else:
            summary = import_vintages(
                config.lake_root,
                args.sources,
                clock=clock,
                calendar=calendar,
                dry_run=args.dry_run,
            )
    for ticker in summary.tickers:
        print(f"{COMMAND}: {ticker.render()}")
    return 0


__all__ = [
    "BAR_IMPORT_SOURCE_PREFIX",
    "COMMAND",
    "LAYOUT_ONE_ROW",
    "LAYOUT_THREE_ROW",
    "SCHWAB_SESSIONS",
    "SCHWAB_SOURCE",
    "SHADOW_REFUSAL",
    "VINTAGES",
    "BarImportRefused",
    "ImportSummary",
    "TickerImport",
    "UndoSummary",
    "Vintage",
    "build_parser",
    "import_schwab",
    "import_vintages",
    "main",
    "undo_schwab",
    "undo_vintages",
]


if __name__ == "__main__":
    raise SystemExit(main())
