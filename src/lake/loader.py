"""The read layer's door onto sealed chains and quotes partitions.

The lake has been write-only. Capture seals a ticker-day into one immutable Parquet
partition at close+15, the manifest checksums it, and until now nothing in ``src/lake``
read it back. ``load_chain`` is that read, and it is the first one, so every rule the
design puts on reading sealed data has to live here rather than beside each caller.
``load_quotes`` is the same machinery pointed at the underlying's quote instead of the
chain, sharing every rule below except which tag its close of record resolves against.

A reader asks for a ticker, a session date, and a minute. It gets back the chain, or the
underlying's quote, as it stood at that minute, as a ``pyarrow.Table``. That matches what
the lake stores, and ``.to_pandas()`` is one call for anyone who wants a frame.

``load_contract`` is a third door onto the same machinery, marketlake #266. It answers a
different question: not the chain at one minute, but one contract's whole session. Its
selection is a single OCC symbol, supplied by the caller instead of resolved off a
whole-partition scan, so it skips the resolve pass described below and reads the
partition once rather than twice. It shares everything past that: the path build, the
spelling check, the quarantine guard, the overflow projection, and the final filter to
data rows. It orders its answer by the instant each row's ``snap_ts`` names rather than
by the partition's own layout, for the same reason the fetch predicate never trusts that
layout either: nothing here rides on how the writer happened to order the file.

``load_bars`` is the fourth door and the first one that is not a read of chains or quotes,
marketlake #368. It answers a ticker's bar series at one frequency over a range of sessions,
in one of three views: as-traded, split-adjusted, or total-return. No adjusted price is ever
stored, so each view is computed at read time out of the sealed bars and the corporate-actions
ledger, and a newly discovered action fixes all of history without any stored number changing
meaning.

It shares this module's guards and almost none of its resolutions. What carries over is the
quarantine guard, the exact-spelling check, ``_read`` and the overflow projection with its
``PartialRead`` refusal. What does not is the whole resolve pass below, because that reads
``snap_ts``, ``row_kind`` and ``close_tag`` and a bars row carries none of the three: it has no
minute slot, no close of record in the sense the two capture surfaces have, and no absence
markers at all. A missed chain sample is gone forever and a missed bar is a re-fetch, so the
writer marks nothing and this reader filters nothing.

Four defaults are settled by marketlake #135, which is authoritative for this deliverable.

1. The return is a ``pyarrow.Table``.
2. Sealed partitions only. Today's data is not sealed until close+15, and reading live
   journal segments carries a locking problem that belongs elsewhere.
3. ``snap`` is an ET wall-clock minute. The lake stores ``snap_ts`` in UTC, so this
   converts. A reader asking for ``10:31`` means the session minute.
4. Data rows only. A gap row records a minute that was missed and why, with every vendor
   column null. Handing one back would corrupt any aggregate computed over the result.

Two resolutions, and each resolves against exactly one column.

*The close of record.* ``snap=None`` means the session's close-of-record snapshot, and it
resolves against the ``close_tag`` column that capture stamped on every row of that
cycle. It never does timestamp arithmetic, because the close moves with the calendar's
half days and the tag is what capture actually observed. Which tag names that cycle is a
property of the surface: chains resolve against ``option_close``, the option market's
close, and quotes resolve against ``spot_close``, the equity close. A quotes partition
carries both tags, because the underlying is captured in both close cycles, and only one
of them is that surface's close of record. A session whose rows carry no matching tag
raises ``NoOptionClose`` on chains and ``NoSpotClose`` on quotes, siblings under the
shared ``NoCloseOfRecord``. It never substitutes the last snapshot of the day, because a
reader asking for the close of record and silently getting 15:59 has no way to tell.

*An intraday minute.* ``snap='10:31'`` resolves against ``snap_ts``, the minute slot the
cycle was scheduled for, and never against ``fetch_ts``, which is when the request went
out. The two differ by the request's own latency, so a fetch clock would drift a reader
onto a neighbouring minute. A minute no cycle recorded raises ``SnapAbsent`` rather than
returning an empty table, because zero rows and no such cycle are different answers.

The comparison runs on instants rather than on the stored text. ``snap_ts`` is an ISO
string, and the same instant has more than one spelling. The live lake proves this is not
hypothetical: SPY's 2026-09-11 partition holds 408 distinct ``snap_ts`` texts naming 406
distinct instants, because two minutes were written both as an Eastern offset and as
``+00:00``. Text equality would have returned half of each of those two minutes.

Two guards ride on every read, and both are here rather than in a caller for the same
reason. #136 and #137 read through this loader, so a rule that sits beside one of them is
a second read path that the other skips.

1. *Quarantine.* The validation battery seals chains and quotes first and flags them
   after, so a bad partition is marked rather than rewritten. ``quarantine.jsonl`` carries
   those verdicts under the manifest's own rules, last entry per partition wins, and
   un-quarantine is a superseding entry rather than a deletion. A partition the ledger
   withholds is refused, and ``include_quarantined=True`` reads it anyway. That is what
   fail closed means for data already sealed. What a verdict means is
   ``manifest.is_quarantined``, beside the ledger rather than inside this reader, so the
   battery and the sign-off tool meet this read at one definition. The ledger does not
   exist in the lake yet, because nothing writes a verdict until marketlake #138's
   battery, and an absent ledger excludes nothing and raises nothing. The guard is
   therefore inert today and correct from the day it ships, which is why it is not
   deferred. Building it after #136 and #137 already read through here would leave their
   reads outside it.
2. *The overflow projection.* A vendor field the pinned schema does not name is
   JSON-encoded into the ``extra`` column rather than dropped. Promoting such a field
   gives it a typed column and bumps ``SCHEMA_VERSION``, which splits history: the same
   measurement is a column above the boundary and an overflow key below it.
   ``lake.extra_projection`` closes that split at read time, and this loader calls it
   rather than reading the schema-version ledger or the overflow key map on its own
   account. One place owns the version-to-shape mapping, and a loader that re-derived it
   would be the second source of truth for what a sealed row means.

A read touches the partition twice, and each pass reads the least it can. marketlake #242
is authoritative for this, and it replaced a single read of every column and every row.

1. *The resolve pass* reads over the whole partition the columns this read resolves
   against. Both resolutions read ``snap_ts`` and ``row_kind``, and the close of record
   reads ``close_tag`` as well. Naming a column to Parquet makes it a requirement of the
   layout, so a minute read does not name a column it never asks about.
2. *The fetch pass* reads the rows the resolve pass chose. It names them to Parquet as a
   predicate rather than filtering after the fact, so the reader skips what it can prove it
   does not need.

Four answers therefore stay settled over the whole session, exactly as they were when the
read pulled every column.

1. Which rows the answer is made of.
2. How many absence markers explain an empty answer.
3. Which ``snap_ts`` values cannot be read as an instant at all.
4. Whether any row carries no ``row_kind``.

Pruning row groups asks nothing of the writer. Parquet skips a row group only when its
statistics prove no row in it can match, and it filters whatever it did read. So ordering
decides how many groups are skipped and never which rows come back, and a fixture written
in deliberately shuffled order, with twelve overlapping row-group ranges, confirms that.
Compaction may go on writing in ``snap_ts`` order or stop, and this read answers the same
either way.

The predicate names the exact ``snap_ts`` spellings the resolve pass found rather than the
minute's canonical text. An equality would match one spelling of an instant that has
several, so a partition written later by a different writer would come back short. The
spellings come from the partition itself, so the rule holds for a spelling nothing has
written yet.

The fetch also reads every row whose ``extra`` is not null, wherever in the session it
sits. That is what keeps the column set a property of the partition rather than of the
minute asked for. The projection adds a promoted column only when a row it is handed
carries a value for it, so a fetch of one minute alone would give the 09:30 read a column
the close-of-record read lacks, and ``pa.concat_tables`` over the two raises. Stitching
reads together is exactly what #136 and #137 do. A row whose overflow is null can fill no
column, so the rows that decide the shape are precisely the ones the fetch adds. Adding
them is free on the lake as it stands, because ``extra`` is null on all 9,846,266 sealed
rows the lake held on 2026-09-14, and a row group whose statistics say so is skipped
whole.

Measured on the 5,260,136-row SPY partition of 2026-09-14, best of five warm runs, the two
passes cost 0.21 seconds for an intraday minute and 0.13 for the close of record, against
1.76 and 1.73 for the single full read they replace. The durable figure is the compressed
bytes each pass touches, because that one does not move with what the page cache happens to
hold. The resolve pass reads 0.1 MiB of the partition's 288.9. The fetch reads the row
groups the predicate keeps, which is 57.6 MiB for that minute and 1.5 for the close of
record.

What the loader adds is the decision about the projection's report. A version the ledger
holds no shape for, a value a column refused, and a column a vendor retype routed into the
overflow all mean the same thing: the read is partial. Handing the table back anyway would
return something that looks whole, so a partial projection raises ``PartialRead`` naming
what was incomplete. An absent ledger is the same condition reached a different way, since
every version in the table is then unrecorded, so it takes no case of its own.

Reading fewer rows moves what that report covers, and the narrower scope is chosen rather
than incidental. Two of the three conditions do not move, because each sits in the overflow
and the fetch reads every row whose overflow is not null.

1. A value a promoted column refused still refuses a read from anywhere in the partition.
2. A column a retype routed away does too.

The third moves. A version the ledger holds no shape for refuses the read when the minute
asked for carries it, or when a row at that version carries an overflow value. A version
whose rows all sit outside the minute and all hold an empty overflow no longer refuses. A
read about one minute should not be taken away by a defect in a minute nobody asked for.

The report also stopped coming first. The projection used to run before either resolution,
so a partition the projection could not complete raised ``PartialRead`` ahead of every
refusal below. It now runs on the rows a resolution chose, so an absent minute, an untagged
close of record, two tagged cycles, and a row with no ``row_kind`` each raise their own
error instead. Each of those is still true where it fires, because the projection fills
promoted columns out of ``extra`` and touches neither ``snap_ts`` nor ``close_tag`` nor
``row_kind``, so nothing it could have done would have changed the answer. What a caller
loses is learning that the partition was also partial, on a read that was never going to
return a table.

One refusal outside that report moves with it, for the same reason. A row carrying no
``schema_version`` at all is a row the journal did not write, and ``project_extra`` raises
on one rather than reporting it. That now refuses the reads whose rows include it rather
than every read of the day. Nothing computed over the whole partition depends on a row's
version, which is what separates this from the ``row_kind`` refusal below. The counts that
explain an empty answer are taken over every row, and Arrow's filter drops a row with no
``row_kind`` from both sides of them, so that one has to stay whole-partition and does.

The loader never returns a table it cannot vouch for. Three shapes would otherwise come
back quietly wrong rather than loudly refused, and each raises instead.

1. A row whose ``row_kind`` is null is neither a vendor observation nor an absence marker,
   and Arrow's filter drops it from both sides. It would vanish from the result and from
   the counts that explain an empty one.
2. A close tag on two cycles would return a table stitched from two minutes. A close of
   record is one cycle.
3. A ``snap_ts`` that cannot be read as an instant is refused only when nothing matched
   the minute asked for. A read that found its minute has no ambiguity to resolve, so one
   unreadable value elsewhere in the day does not take the answer away.

``load_bars`` is also the one door that spans partitions, because an adjusted view only means
something over a series that crosses an ex-date. Three rules follow from that and each is
written where it happens. The stitch promotes rather than raising, since two partitions at two
schema versions can come back with different column sets. An absent day inside a range is
returned around and a quarantined one refuses the read, because a hole in a re-fetchable surface
is ordinary and a verdict is not. And the answer is ordered by the instant each ``bar_ts`` names,
through the same helper that orders one contract's session, because a series is the one read
whose order a caller will assume.

Nothing here reads a clock or the network. One line reads a config file, and it is inside
``resolve_lake_root``, which turns ``lake_root=None`` into the configured lake. Every
resolution past that call takes the root as an argument, so a test points it at a fixture
lake and no helper here reaches for config.

That line is the only call to ``load_config`` in ``src/lake`` that names no config path
and does not sit in a ``main``. The other two no-argument calls are ``probe.main`` and
``record.main``, and every remaining call in the package is handed a path by its caller,
the ten ``*_from_config`` wiring functions included. So a reader who expects a config
path to arrive as an argument finds the one place it does not, written down here rather
than generalised.

It is a named function rather than a branch inside the read because the read layer has a
second caller now. ``lake.oi`` resolves the same root and then reads the security master,
the capture spans and the sealed partition list beneath it, so giving it a no-argument
``load_config`` of its own would have made a second answer to where the lake is.
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from math import isfinite
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lake import journal
from lake.calendar import MARKET_TZ
from lake.config import load_config
from lake.extra_projection import EXTRA_COLUMN, ExtraProjection, project_extra
from lake.manifest import is_quarantined, latest_quarantine
from lake.paths import BARS, CHAINS, DATE_PREFIX, PARQUET_SUFFIX, QUOTES, LakePaths
from lake.schema_versions import SchemaVersionLedger, ledger_path
from lake.session import OPTION_CLOSE, SPOT_CLOSE
from lake.vendor import DAILY_FREQ

# The columns a read resolves against: ``snap_ts`` is the minute slot the cycle was
# scheduled for, ``close_tag`` is the tag capture stamps on a close-of-record cycle,
# ``row_kind`` tells a vendor observation from an absence marker, and ``occ_symbol`` is
# the contract identity a chains row carries. The row-kind names come from the writer
# rather than being spelled again here. ``occ_symbol`` is on the chains schema only,
# which is why ``load_contract`` reads chains and takes no surface argument.
SNAP_TS_COLUMN = "snap_ts"
CLOSE_TAG_COLUMN = "close_tag"
OCC_SYMBOL_COLUMN = "occ_symbol"
ROW_KIND_COLUMN = journal.ROW_KIND_COLUMN
ROW_KIND_DATA = journal.ROW_KIND_DATA

# What the resolve pass reads for each resolution. Naming a column to Parquet makes it a
# requirement of the layout, so a read names the ones it uses and no more. Both resolutions
# count absence markers by ``row_kind`` at a ``snap_ts``. Only the close of record resolves
# against ``close_tag``.
MINUTE_COLUMNS = (SNAP_TS_COLUMN, ROW_KIND_COLUMN)
CLOSE_COLUMNS = (SNAP_TS_COLUMN, ROW_KIND_COLUMN, CLOSE_TAG_COLUMN)

# An ET wall-clock minute, ``HH:MM`` on a 24-hour clock and nothing else.
_SNAP_SHAPE = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")

# The bars columns this module names. ``bar_ts`` is the candle's own instant and the only
# non-null column on the surface, so it is both the slot a bars row sits in and the one
# thing every row has. ``instrument_id`` is the key the actions ledger is written under,
# carried on the row rather than resolved per read. ``freq`` repeats the path level a bars
# partition is keyed by. The four prices and the volume are what an adjusted view rewrites.
BAR_TS_COLUMN = "bar_ts"
INSTRUMENT_ID_COLUMN = "instrument_id"
FREQ_COLUMN = "freq"
CLOSE_COLUMN = "close"
PRICE_COLUMNS = ("open", "high", "low", CLOSE_COLUMN)
VOLUME_COLUMN = "volume"

# The column every answer from ``load_bars`` carries, naming the view that produced it.
# A table of adjusted numbers is shaped exactly like a table of as-traded ones, and
# ``docs/design.md`` calls mixing the two the classic corruption, so the answer says which
# it is. It is a column rather than Arrow schema metadata because ``pa.concat_tables`` over
# two tables whose metadata disagrees does not raise and keeps the first table's, so a
# stitch of an as-traded read and an adjusted one would have carried one word over rows half
# of which were adjusted. A column concat keeps per row.
ADJUST_COLUMN = "adjust"

# The three views. ``none`` is as-traded, which is what the lake stores and the only thing it
# ever stores. ``split`` divides out the cumulative split ratio, which is price continuity.
# ``total`` folds the dividends in on top of that, which is total return.
ADJUST_NONE = "none"
ADJUST_SPLIT = "split"
ADJUST_TOTAL = "total"
ADJUSTMENTS = (ADJUST_NONE, ADJUST_SPLIT, ADJUST_TOTAL)

__all__ = [
    "ADJUSTMENTS",
    "ADJUST_NONE",
    "ADJUST_SPLIT",
    "ADJUST_TOTAL",
    "AdjustUnknown",
    "AdjustmentIncomplete",
    "BarsAbsent",
    "ContractAbsent",
    "InstrumentUnknown",
    "LoadError",
    "NoCloseOfRecord",
    "NoOptionClose",
    "NoSpotClose",
    "PartialRead",
    "PartitionAbsent",
    "PartitionQuarantined",
    "SnapAbsent",
    "SnapMalformed",
    "list_chain_cycles",
    "load_bars",
    "load_chain",
    "load_contract",
    "load_quotes",
    "resolve_lake_root",
]


class LoadError(Exception):
    """Base for every reason a read of sealed data does not produce a table."""


class PartitionAbsent(LoadError):
    """Raised when the ticker-day has no sealed partition spelled the way it was asked for.

    A session the lake never captured and today's session before close+15 both land here.
    Neither is an empty read, so neither comes back as an empty table.

    A ticker whose case does not match the directory on disk lands here too. macOS matches
    a path case-insensitively, so ``ticker=spy`` opens the ``ticker=SPY`` partition while
    the quarantine lookup keys on the spelling the caller used and finds no verdict. That
    turns the guard from fail closed into fail open, so the read is refused instead.
    """


class PartitionQuarantined(LoadError):
    """Raised when the partition's current quarantine verdict withholds it.

    ``include_quarantined=True`` reads it anyway. The entry rides on the exception, so the
    refusal says what the battery found rather than only that it found something.
    """

    def __init__(self, partition: str, entry: dict) -> None:
        super().__init__(
            f"{partition} is quarantined: {entry!r}. "
            "Pass include_quarantined=True to read it anyway."
        )
        self.partition = partition
        self.entry = entry


class SnapMalformed(LoadError, ValueError):
    """Raised when ``snap`` is not an ``HH:MM`` ET wall-clock minute.

    A caller typo is not an absent cycle, so this is separate from ``SnapAbsent``. It is a
    ``ValueError`` as well, because a bad argument is what that means in Python, and a
    ``LoadError`` so the one exception a caller is told to catch really does cover every
    way a read resolves to no table.
    """


class NoCloseOfRecord(LoadError):
    """Raised when a session's rows carry no data row tagged the surface's close of record.

    This is the explicit marker the design asks for. The close of record is the one
    snapshot a reader cannot be quietly handed a substitute for, so the last snapshot of
    the day is never returned in its place.

    Which tag names that cycle is a property of the surface, and each surface raises its
    own sibling rather than this base directly: ``NoOptionClose`` names ``option_close`` on
    chains, and ``NoSpotClose`` names ``spot_close`` on quotes. Both carry the tag on
    ``close_tag`` for a caller that catches the base and wants to know which one fired.

    ``tagged_gaps`` counts absence markers that do carry the tag, which tells two cases
    apart. Zero means the close-of-record cycle never ran. A positive count means it ran
    and failed, and the failure is recorded in the partition. Reading every sealed
    ticker-day in the lake on 2026-09-14 raised ``NoOptionClose`` eight times out of eleven,
    and each of the eight carried exactly one tagged gap row, which said an auth error took
    the close rather than the cycle never having been scheduled.
    """

    def __init__(self, close_tag: str, ticker: str, day: str, tagged_gaps: int) -> None:
        detail = (
            f"{tagged_gaps} tagged gap rows record the attempt"
            if tagged_gaps
            else "no cycle recorded the attempt"
        )
        super().__init__(
            f"{ticker} {day} has no {close_tag}-tagged data row, and {detail}. "
            "The close of record is never substituted."
        )
        self.close_tag = close_tag
        self.ticker = ticker
        self.day = day
        self.tagged_gaps = tagged_gaps


class NoOptionClose(NoCloseOfRecord):
    """Raised when a chains session's rows carry no ``option_close`` tag on a data row.

    The chains sibling of ``NoCloseOfRecord``, shipped under this name by #241 before the
    base existed. The name stays, because it is public API and renaming it breaks a caller
    already catching it.
    """

    def __init__(self, ticker: str, day: str, tagged_gaps: int) -> None:
        super().__init__(OPTION_CLOSE, ticker, day, tagged_gaps)


class NoSpotClose(NoCloseOfRecord):
    """Raised when a quotes session's rows carry no ``spot_close`` tag on a data row.

    The quotes sibling of ``NoOptionClose``. A quotes partition carries both close tags,
    because the underlying is captured in both close cycles, so this is the marker for the
    tag ``load_quotes`` actually resolves against, ``spot_close``, the equity close of
    record.
    """

    def __init__(self, ticker: str, day: str, tagged_gaps: int) -> None:
        super().__init__(SPOT_CLOSE, ticker, day, tagged_gaps)


class SnapAbsent(LoadError):
    """Raised when no data row carries the requested minute.

    Returning an empty table would read as a chain with no contracts, or a quote with
    nothing in it, which is a different answer from a minute the capture loop never
    recorded.

    ``tagged_gaps`` counts absence markers at that minute, the same way ``NoCloseOfRecord``
    counts them for its own tag. ``unreadable`` names every ``snap_ts`` in the partition
    that could not be read as an instant, because a read that found nothing cannot claim
    the minute is absent while values it could not read sit beside the answer.
    """

    def __init__(
        self, ticker: str, day: str, snap: str, tagged_gaps: int, unreadable: tuple[str, ...]
    ) -> None:
        detail = (
            f"{tagged_gaps} gap rows record the attempt"
            if tagged_gaps
            else "no cycle recorded the attempt"
        )
        if unreadable:
            detail += f", and {len(unreadable)} snap_ts values could not be read: {unreadable}"
        super().__init__(f"{ticker} {day} has no data row at {snap} ET, and {detail}.")
        self.ticker = ticker
        self.day = day
        self.snap = snap
        self.tagged_gaps = tagged_gaps
        self.unreadable = unreadable


class ContractAbsent(LoadError):
    """Raised when an OCC symbol names no data row in the ticker-day's chains partition.

    ``SnapAbsent`` is the model for this message, since it answers the same shape of
    question about a different key: an absent minute there, an absent contract here,
    both against a partition that does exist. A contract present for only some of the
    session's minutes is a different answer and comes back as one, a real partial series
    rather than nothing, which is why only a contract with no rows at all reaches here.

    ``ticker`` is the partition the read looked in, which is the ``ticker=`` override
    when one was given to ``load_contract`` and the OCC root derived from ``occ_symbol``
    otherwise. Naming both lets a caller relying on the derivation see which ticker was
    tried and why.
    """

    def __init__(self, occ_symbol: str, ticker: str, day: str) -> None:
        super().__init__(f"{ticker} {day} has no data row for {occ_symbol!r}.")
        self.occ_symbol = occ_symbol
        self.ticker = ticker
        self.day = day


class BarsAbsent(LoadError):
    """Raised when a bars range holds no sealed partition at all.

    An absent day inside a range is not this. Bars carry no gap rows, because a missed chain
    sample is gone forever and a missed bar is a re-fetch inside Schwab's window, so a surface
    with days missing from the middle is the ordinary state of one and a range returns the days
    it holds. A range holding nothing is different: an empty series and an unfetched one read
    the same to a caller and mean opposite things, which is the rule every door here follows.

    A frequency directory spelled other than it is on disk lands here too, for the reason
    ``PartitionAbsent`` gives for a ticker. The bars path adds a ``freq=`` level, so there is
    one more component whose case a case-insensitive filesystem would match through.
    """


class AdjustUnknown(LoadError, ValueError):
    """Raised for an ``adjust`` outside ``ADJUSTMENTS``.

    A ``ValueError`` as well, because a caller naming a view that does not exist passed a bad
    argument rather than reaching a lake that could not answer, which is the same split
    ``SnapMalformed`` makes for a malformed minute.
    """


class InstrumentUnknown(LoadError):
    """Raised when an adjusted read meets a row whose ``instrument_id`` is null.

    ``BARS_SCHEMA`` declares ``bar_ts`` non-null and nothing else, and ``lake.bars`` lands a
    row with a null id when the security master cannot place its ticker, filing a finding and
    keeping the bar. The actions ledger is keyed on ``(instrument_id, ex_date, type)``, so a
    null id is the join key gone: an adjusted read of that row would find no actions and hand
    back the as-traded price under an adjusted name.

    ``adjust='none'`` reads the row, because as-traded is what it says it is.
    """


class AdjustmentIncomplete(LoadError):
    """Raised when a dividend factor cannot be computed from what the lake holds.

    The factor for a cash dividend is ``1 - A / C``, where ``C`` is the close of the last
    daily session before the ex-date. Dropping a dividend whose ``C`` is missing understates
    every return computed through it while the table still looks whole, which is the failure
    ``PartialRead`` refuses one layer down. So the read refuses instead.

    Three ways in. The lake holds no ``1d`` partition before the ex-date at all. The one it
    holds carries no usable close. Or that partition's own ``instrument_id`` is not the
    dividend's, which means the close belongs to a different instrument than the event.
    """


class PartialRead(LoadError):
    """Raised when the overflow projection could not present every value as its column.

    Three conditions reach here, and each leaves the table readable but incomplete.

    1. A version the schema-version ledger holds no shape for, which is marketlake #130's
       condition and which an absent ledger produces for every version at once.
    2. A value a promoted column refused.
    3. A column a vendor retype routed into the overflow.

    The projection rides on the exception so the refusal can say which rows and which
    columns were affected. It is diagnosis rather than a second way to get the table,
    which is what ``include_quarantined`` is for on the guard that has one.
    """

    def __init__(self, ticker: str, day: str, surface: str, projection: ExtraProjection) -> None:
        parts = []
        if projection.unrecorded_versions:
            versions = ", ".join(str(v) for v in projection.unrecorded_versions)
            parts.append(
                f"the schema-version ledger holds no {surface} shape for version {versions}"
            )
        for unfit in projection.unfit:
            parts.append(
                f"{unfit.rows} rows at version {unfit.schema_version} hold an "
                f"{unfit.column} value the column refuses ({unfit.detail})"
            )
        for retyped in projection.retyped:
            parts.append(
                f"{retyped.rows} rows at version {retyped.schema_version} routed "
                f"{retyped.column} into the overflow, which the column recorded as "
                f"{retyped.recorded_type}"
            )
        super().__init__(f"{ticker} {day} reads partial. " + ". ".join(parts) + ".")
        self.ticker = ticker
        self.day = day
        self.surface = surface
        self.projection = projection


def load_chain(
    ticker: str,
    day: date | str,
    snap: str | None = None,
    *,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """The chain for one ticker and session, at one minute, as a table of data rows.

    ``snap=None`` is the session's option-close snapshot, resolved against ``close_tag``.
    ``snap='10:31'`` is that ET wall-clock minute, resolved against ``snap_ts``.

    ``include_quarantined`` reads a partition the battery flagged. It defaults off, so a
    caller that has not decided what a bad partition means never silently gets one.

    ``lake_root=None`` reads the lake that ``config.yaml`` names. A root given here is used
    as given, and is never checked against the configured one or replaced when it turns out
    to hold nothing. An explicit root that names an empty directory is a refusal rather
    than a quiet read of the production lake, which is what points a test at a fixture lake
    and keeps it there.

    The root is keyword-only rather than a fourth positional argument, so an old call
    cannot bind onto the new parameters. The old shape was
    ``load_chain(root, ticker, day, snap)``. A fourth positional root would still accept
    those four arguments, landing the root on ``ticker`` and the minute on ``lake_root``,
    and the read would build a path out of a minute. Keyword-only rejects that call where
    it is written instead. The three-argument ``load_chain(root, ticker, day)`` is not
    rejected either way, because ``ticker``, ``day``, and ``snap`` take three positional
    arguments between them under both shapes. The root lands on ``ticker`` there, and the
    read resolves the configured lake and raises ``PartitionAbsent`` against it.

    Every way a read resolves to no table raises a ``LoadError``, a malformed ``snap``
    included. Three conditions raise something other than a ``LoadError``, because each
    one means the read stopped before it could establish that nothing matched.

    1. A machine with no ``config.yaml`` raises ``ConfigError`` out of the resolution
       above. That says the machine is unconfigured.
    2. An overflow value that is not JSON raises ``ExtraProjectionError``.
    3. A ledger line naming no partition raises ``ManifestError``.

    The last two say the lake's own files contradict their writers, so each raises the
    error of the module that owns that file. Folding any of the three into ``LoadError``
    would blur an unconfigured machine, or a damaged lake, with a read that found nothing.
    ``ExtraProjectionError`` reaches the rows this read is made of rather than every row
    of the session, which the module docstring's account of the two passes settles.
    Sweeping a partition for damage is the validation battery's job rather than this
    reader's.

    Nothing comes back empty, because an empty chain and an absent one read the same to a
    caller and mean opposite things.
    """
    return _load_surface(
        ticker,
        day,
        snap,
        lake_root=lake_root,
        include_quarantined=include_quarantined,
        surface=CHAINS,
        close_tag=OPTION_CLOSE,
        no_close_error=NoOptionClose,
    )


def load_quotes(
    ticker: str,
    day: date | str,
    snap: str | None = None,
    *,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """The underlying's quote for one ticker and session, at one minute, as a table of data rows.

    The signature, the resolutions, and every guard match ``load_chain``, pointed at the
    quotes surface instead. ``snap=None`` is the session's equity-close snapshot, resolved
    against ``close_tag``. ``snap='10:31'`` is that ET wall-clock minute, resolved against
    ``snap_ts``. ``include_quarantined`` and ``lake_root`` carry the same meaning and the
    same defaults ``load_chain`` gives them.

    The one difference is which tag the close of record resolves against. A quotes
    partition carries both ``option_close`` and ``spot_close``, because the underlying is
    captured in both close cycles, and only the equity close is this surface's close of
    record. ``spot_close`` is the design's pinned name for it. A session whose rows carry
    no ``spot_close`` tag raises ``NoSpotClose`` rather than ``NoOptionClose``, so a caller
    reading the exception name is told which tag was missing rather than being pointed at
    the sibling surface's.
    """
    return _load_surface(
        ticker,
        day,
        snap,
        lake_root=lake_root,
        include_quarantined=include_quarantined,
        surface=QUOTES,
        close_tag=SPOT_CLOSE,
        no_close_error=NoSpotClose,
    )


def load_bars(
    ticker: str,
    freq: str,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    adjust: str = ADJUST_NONE,
    as_of: date | str | None = None,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """One ticker's bar series at one frequency, as a table, in one of three views.

    ``freq`` is ``'1m'`` or ``'1d'``, the two ``vendor.BAR_FREQS`` spells, and it is a level of
    the partition path rather than a column filter, so one read is one frequency. It is not
    checked against that tuple, because a frequency the lake never fetched has no directory and
    ``BarsAbsent`` says so by name. A second guard here would refuse a frequency the day someone
    adds one to the fetch, while ``vendor.require_bar_freq`` already refuses one at the seam that
    would have to fetch it.

    ``start`` and ``end`` are session dates and both default to open, so a call naming neither
    reads every session the lake holds for that ticker and frequency. This is the one door here
    that spans partitions, because the adjusted views only mean something over a series that
    crosses an ex-date.

    ``adjust`` names the view. ``'none'`` is as-traded, which is what the lake stores and the
    only thing it ever stores. ``'split'`` divides the cumulative split ratio out, which is
    price continuity. ``'total'`` folds the dividends in on top of that, which is total return.
    Every answer carries an ``adjust`` column naming the view that produced it, because a table
    of adjusted numbers is shaped exactly like a table of as-traded ones.

    ``as_of`` is a market date that resolves the actions ledger point-in-time, and the default
    is the ledger's current answer. Both readings are the ledger's own, and the door takes both
    rather than making a caller who wants the second one read the ledger and apply factors
    itself. That caller would be a second adjustment path, in the one place where it produces
    different numbers rather than an error.

    ``lake_root`` and ``include_quarantined`` carry the meanings the three shipped doors give
    them. Two of this deliverable's four defaults carry over and two do not apply. The return is
    a ``pyarrow.Table``, and the read is of sealed partitions only, which for bars is automatic
    because ``journal.JOURNALED_SURFACES`` is chains and quotes alone and a bars partition has
    no unsealed form. An ET wall-clock minute and the data-rows-only filter are both
    chains-and-quotes rules: a bars row's slot is ``bar_ts`` and bars carry no ``row_kind``.

    **An absent day inside the range is returned around and a quarantined one refuses it.**
    Bars carry no gap rows, because a missed chain sample is gone forever and a missed bar is a
    re-fetch, so days missing from the middle are the ordinary state of the surface. A flagged
    day is a verdict rather than an ordinary hole, and stepping over one would hand back a
    series that reads as complete, so it raises ``PartitionQuarantined`` for the whole read.
    A range holding no partition at all raises ``BarsAbsent``.

    **The stitch promotes rather than raising.** The overflow projection adds a promoted column
    only when a row it is handed carries a value for it, so two partitions at two schema
    versions can come back with different column sets, and ``pa.concat_tables`` raises on that.
    This is the first door here that stitches, so it names ``promote_options='permissive'``,
    which unions by name. Unreachable at the one recorded version the live lake holds and
    reachable the first time a bars column is promoted.
    """
    if adjust not in ADJUSTMENTS:
        raise AdjustUnknown(f"adjust {adjust!r} is not one of {list(ADJUSTMENTS)}")
    root = resolve_lake_root(lake_root)
    sessions = _bars_sessions(root, ticker, freq, _session(start), _session(end))
    if not sessions:
        raise BarsAbsent(
            f"{ticker} {freq} holds no sealed partition in "
            f"{_range_text(start, end)} under {LakePaths(root).root / BARS}. "
            "A frequency and a ticker are each spelled as their directory is."
        )
    table = pa.concat_tables(
        [_read_bars(root, ticker, freq, day, include_quarantined) for day in sessions],
        promote_options="permissive",
    )
    ordered = _sorted_by_instant(table, BAR_TS_COLUMN, freq, ticker, _range_text(start, end))
    return _in_view(
        ordered,
        adjust=adjust,
        root=root,
        ticker=ticker,
        as_of=_session(as_of),
        include_quarantined=include_quarantined,
    )


def _session(day: date | str | None) -> date | None:
    """A session argument as a ``date``, leaving ``None`` open."""
    if day is None:
        return None
    return day if isinstance(day, date) else date.fromisoformat(str(day))


def _range_text(start: date | str | None, end: date | str | None) -> str:
    """How a refusal names the range it found nothing in."""
    return f"{'open' if start is None else start}..{'open' if end is None else end}"


def _bars_sessions(
    root: Path, ticker: str, freq: str, start: date | None, end: date | None
) -> list[date]:
    """The sessions this ticker and frequency hold a sealed partition for, in date order.

    The listing comes from the filesystem rather than from the manifest, and both halves of
    that are deliberate. Nothing else enumerates a bars surface: ``actions.surface_ticker_days``
    reads manifest keys through ``paths.parse_partition_rel``, which returns ``None`` for every
    bars key by design, and that walk passes over a key it cannot read rather than raising, so
    asking it for bars answers an empty list on a lake full of them. And the filesystem is the
    question ``_clear_partition`` already asks, through ``is_file`` and ``_spelled_exactly``, so
    enumerating from the manifest and opening from disk would answer two questions in one read.

    The frequency directory is checked for its exact spelling before it is listed. macOS matches
    a path case-insensitively, so ``freq=1D`` would otherwise list the ``freq=1d`` partitions and
    every quarantine lookup below would key on a path no verdict was ever written under.

    A name under the directory that does not read as ``date=YYYY-MM-DD.parquet`` is passed over
    rather than raising. A partition being written lands under ``paths.temp_write_path``'s marker
    and is renamed into place, so a listing taken mid-write sees a name this cannot read, and
    that file is not a session yet.
    """
    directory = LakePaths(root).bars_partition_path(ticker, freq, date(1970, 1, 1)).parent
    if not (directory.is_dir() and _spelled_exactly(root, directory)):
        return []
    found: list[date] = []
    for entry in directory.iterdir():
        day = _partition_day(entry.name)
        if day is None:
            continue
        if (start is None or day >= start) and (end is None or day <= end):
            found.append(day)
    return sorted(found)


def _partition_day(name: str) -> date | None:
    """The session a ``date=YYYY-MM-DD.parquet`` file name holds, or ``None`` for anything else."""
    if not (name.startswith(DATE_PREFIX) and name.endswith(PARQUET_SUFFIX)):
        return None
    try:
        return date.fromisoformat(name[len(DATE_PREFIX) : -len(PARQUET_SUFFIX)])
    except ValueError:
        return None


def _read_bars(
    root: Path, ticker: str, freq: str, day: date, include_quarantined: bool
) -> pa.Table:
    """One sealed bars partition, cleared and projected, as every row it holds.

    What ``_load_surface`` lends bars is its tail rather than its head. The path comes from
    ``bars_partition_path``, because ``LakePaths.partition_path`` raises on this surface and its
    docstring says why. The resolve pass goes entirely, because ``MINUTE_COLUMNS`` and
    ``CLOSE_COLUMNS`` are built from ``snap_ts``, ``row_kind`` and ``close_tag`` and a bars row
    carries none of the three. What carries over untouched is the quarantine guard, the
    exact-spelling check, ``_read`` and the overflow projection, whose ``PartialRead`` refusal
    means here exactly what it means on a chain.

    There is no fetch predicate, because a day is the selection. ``_predicate`` names the rows an
    answer is made of out of a partition holding a session's worth of other minutes, and a bars
    partition holds one session at one frequency, which is the whole answer for that day.
    """
    day_text = day.isoformat()
    path = LakePaths(root).bars_partition_path(ticker, freq, day)
    _clear_partition(
        root,
        path,
        ticker,
        day_text,
        BARS,
        include_quarantined=include_quarantined,
        absent_detail=f" The frequency {freq!r} is a level of that path.",
    )
    fetched = _read(path)
    projection = project_extra(fetched, surface=BARS, ledger=_ledger(root))
    if not projection.complete:
        raise PartialRead(ticker, day_text, BARS, projection)
    return projection.table


def _in_view(
    table: pa.Table,
    *,
    adjust: str,
    root: Path,
    ticker: str,
    as_of: date | None,
    include_quarantined: bool,
) -> pa.Table:
    """``table`` in the view ``adjust`` names, marked with which view that is.

    The marker goes on every answer including the as-traded one, because what it protects
    against is two answers being mixed, and one of the two is always as-traded.

    ``lake.actions`` and ``lake.bars`` are imported here rather than at the top of the module,
    and the reason is an import direction rather than a preference: both of them import this
    module, so naming either above would be a cycle. ``actions.append`` keeps its own direction
    one-way the same way. ``adjust='none'`` needs neither module, so the import sits on the one
    path that needs it.

    A bar's session comes from ``bars.session_of`` rather than from the ``date=`` level of the
    partition it was read out of. The path level is free and the two agree on every partition
    the writer wrote, because ``select_session_rows`` filters every landed row through that same
    function. ``journal.py`` pins that a bar's session is decided by ``bar_ts``, and reading the
    path instead would mint a second definition of a bar's session to get the answer the first
    one already gives.
    """
    marked = table.append_column(
        ADJUST_COLUMN, pa.array([adjust] * table.num_rows, type=pa.string())
    )
    if adjust == ADJUST_NONE:
        return marked

    # Local for the cycle named above. ``lake.actions`` imports ``load_quotes`` from here and
    # ``lake.bars`` imports ``LoadError`` and ``load_quotes``, so either at module level fails
    # at interpreter start rather than at call time.
    from lake import actions
    from lake.bars import session_of

    if INSTRUMENT_ID_COLUMN not in table.column_names:
        raise LoadError(
            f"{ticker} bars carry no {INSTRUMENT_ID_COLUMN} column, which is the key the "
            f"actions ledger is written under, so no {adjust} view can be computed."
        )

    entries = actions.latest(root) if as_of is None else actions.as_of(root, as_of)
    events = _by_instrument(entries)
    instruments = table.column(INSTRUMENT_ID_COLUMN).to_pylist()
    stamps = table.column(BAR_TS_COLUMN).to_pylist()

    closes: dict[tuple[int, date], float] = {}
    price_factors: list[float] = []
    volume_factors: list[float] = []
    for instrument, stamp in zip(instruments, stamps, strict=True):
        if instrument is None:
            raise InstrumentUnknown(
                f"{ticker} holds a bar at {stamp} with no {INSTRUMENT_ID_COLUMN}, which is the "
                f"key the actions ledger is written under. A {adjust} view of it would find no "
                "actions and hand back the as-traded price under an adjusted name. Read it with "
                f"adjust={ADJUST_NONE!r}."
            )
        session = session_of(str(stamp))
        splits, dividends = events.get(instrument, ((), ()))

        # Strictly after, never on or after. An ex-date is the first session that trades at the
        # new price, so the ex-date's own bar is already adjusted and applying the factor there
        # would halve it twice.
        ratio = 1.0
        for ex_date, value in splits:
            if ex_date > session:
                ratio *= value

        folded = 1.0
        if adjust == ADJUST_TOTAL:
            for ex_date, amount in dividends:
                if ex_date > session:
                    close = _prior_close(
                        root, ticker, instrument, ex_date, include_quarantined, closes
                    )
                    if amount >= close:
                        raise AdjustmentIncomplete(
                            f"{ticker} instrument {instrument} pays {amount} on {ex_date} "
                            f"against a prior close of {close}, which is no dividend factor a "
                            "price can be multiplied by."
                        )
                    folded *= 1.0 - amount / close

        # A split divides the price and multiplies the volume, because the ratio is the
        # deliverable's share count after the adjustment over the count before it. A dividend
        # moves the price and leaves the share count alone.
        price_factors.append(folded / ratio)
        volume_factors.append(ratio)

    return _rescaled(marked, price_factors, volume_factors)


def _by_instrument(
    entries: dict[tuple[int, str, str], dict],
) -> dict[int, tuple[tuple[tuple[date, float], ...], tuple[tuple[date, float], ...]]]:
    """The ledger's current answers, grouped by instrument into splits and dividends.

    The grouping is on the instrument rather than on the ticker, which is the rule
    ``actions.by_ticker`` states: the instrument enters the comparison rather than the grouping,
    which is what gives a symbol handed between two instruments a first observation under each.
    ``lake.bars`` follows it too, resolving the id per ticker-day rather than once per ticker, so
    a range spanning a handover carries two of them and each half is adjusted by its own
    instrument's actions.

    An entry this cannot read raises rather than being stepped over. ``actions.build_entry``
    validates every field on the way in, so a value here that does not read is a file a human
    edited, and that is the case the ledger's own resolution raises at rather than stepping past.
    """
    # Local for the same cycle ``_in_view`` names: ``lake.actions`` imports this module.
    from lake import actions

    grouped: dict[int, tuple[list[tuple[date, float]], list[tuple[date, float]]]] = {}
    for (instrument, ex_text, action_type), entry in entries.items():
        if action_type == actions.TYPE_SPLIT:
            field, index = "split_ratio", 0
        elif action_type == actions.TYPE_DIVIDEND:
            field, index = "cash_amount", 1
        else:
            continue
        try:
            ex_date = date.fromisoformat(ex_text)
        except ValueError as exc:
            raise AdjustmentIncomplete(
                f"the actions ledger holds a {action_type} for instrument {instrument} whose "
                f"ex_date {ex_text!r} does not name a date."
            ) from exc
        value = entry.get(field)
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise AdjustmentIncomplete(
                f"the actions ledger's {action_type} for instrument {instrument} on {ex_text} "
                f"carries a {field} of {value!r}, which is not a number."
            )
        value = float(value)
        if not isfinite(value) or value <= 0:
            raise AdjustmentIncomplete(
                f"the actions ledger's {action_type} for instrument {instrument} on {ex_text} "
                f"carries a {field} of {value!r}, which no factor can be built from."
            )
        grouped.setdefault(instrument, ([], []))[index].append((ex_date, value))
    return {
        instrument: (tuple(splits), tuple(dividends))
        for instrument, (splits, dividends) in grouped.items()
    }


def _prior_close(
    root: Path,
    ticker: str,
    instrument: int,
    ex_date: date,
    include_quarantined: bool,
    closes: dict[tuple[int, date], float],
) -> float:
    """The unadjusted close the dividend factor divides into, cached per instrument and ex-date.

    The factor for a cash dividend of ``A`` with ex-date ``E`` is ``1 - A / C``, where ``C`` is
    the close of the last daily session before ``E``. That close is a property of the instrument
    and the date rather than of the frequency asked for, so it comes from the ``1d`` partition
    whatever frequency the caller asked for: a minute series has no close of record in itself,
    and its last minute bar is not the official close.

    Which row of that partition is the close is the reading ``lake.bars`` already takes for its
    own gate, the last row by stamp, which answers a partition at either frequency. This one
    refuses where that one returns nothing, because a gate holding a bar costs a night and a
    dividend silently dropped from a factor costs every return computed through it.

    The session is the last one the lake holds a ``1d`` partition for before the ex-date, rather
    than the calendar's previous session. A calendar step here would be the fourth private
    spelling of one, which marketlake #334 names as where that stops being a smell, and it would
    refuse a factor over a day the sweep has not fetched rather than over a day it cannot price.
    The price of the lake's own reading is named: a hole in the ``1d`` surface immediately before
    an ex-date moves the denominator to an older close, which moves the factor by a fraction of
    the drift over those sessions rather than making it wrong by the dividend.
    """
    key = (instrument, ex_date)
    if key in closes:
        return closes[key]

    detail = (
        f"{ticker} instrument {instrument} pays on {ex_date} and the {ADJUST_TOTAL} view needs "
        f"the {DAILY_FREQ} close before it"
    )
    sessions = _bars_sessions(root, ticker, DAILY_FREQ, None, ex_date - timedelta(days=1))
    if not sessions:
        raise AdjustmentIncomplete(
            f"{detail}, and the lake holds no {DAILY_FREQ} session before it."
        )
    day = sessions[-1]
    table = _read_bars(root, ticker, DAILY_FREQ, day, include_quarantined)
    if table.num_rows == 0:
        raise AdjustmentIncomplete(f"{detail}, and its {DAILY_FREQ} partition for {day} is empty.")
    ordered = _sorted_by_instant(table, BAR_TS_COLUMN, DAILY_FREQ, ticker, day.isoformat())
    last = ordered.num_rows - 1

    owner = ordered.column(INSTRUMENT_ID_COLUMN)[last].as_py()
    if owner != instrument:
        raise AdjustmentIncomplete(
            f"{detail}, and the {DAILY_FREQ} bar for {day} belongs to instrument {owner!r}."
        )
    value = ordered.column(CLOSE_COLUMN)[last].as_py()
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AdjustmentIncomplete(
            f"{detail}, and the {DAILY_FREQ} bar for {day} carries a close of {value!r}."
        )
    value = float(value)
    if not isfinite(value) or value <= 0:
        raise AdjustmentIncomplete(
            f"{detail}, and the {DAILY_FREQ} bar for {day} carries a close of {value!r}."
        )
    closes[key] = value
    return value


def _rescaled(table: pa.Table, price_factors: list[float], volume_factors: list[float]) -> pa.Table:
    """``table`` with its prices and volume moved into the view, row by row.

    A null stays null, because Arrow's arithmetic propagates one and a bar the vendor sent
    without a high has no adjusted high either.

    The volume comes back as the integer it went in as, so a ratio that is not a whole number
    rounds. Carrying it as a float instead would make the column's type depend on which view was
    asked for, and the join this feeds would then meet two types for one column. Rounding is
    stated rather than hidden: a whole-ratio split, which is every split that maps exactly, moves
    a volume to another whole number and rounds nothing.
    """
    prices = pa.array(price_factors, type=pa.float64())
    volumes = pa.array(volume_factors, type=pa.float64())
    for name in PRICE_COLUMNS:
        if name not in table.column_names:
            continue
        index = table.column_names.index(name)
        scaled = pc.multiply(pc.cast(table.column(name), pa.float64()), prices)
        table = table.set_column(index, table.field(index), scaled.cast(table.field(index).type))
    if VOLUME_COLUMN in table.column_names:
        index = table.column_names.index(VOLUME_COLUMN)
        field = table.field(index)
        scaled = pc.multiply(pc.cast(table.column(VOLUME_COLUMN), pa.float64()), volumes)
        table = table.set_column(index, field, pc.round(scaled).cast(field.type))
    return table


def load_contract(
    occ_symbol: str,
    day: date | str,
    *,
    ticker: str | None = None,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> pa.Table:
    """One contract's whole session, as a table of data rows ordered by ``snap_ts``.

    This reads ``chains``, not a surface a caller names, because ``occ_symbol`` is on the
    chains schema and not on the quotes one. Passing a surface the way ``_load_surface``
    takes one for its two genuinely different callers would offer a call that cannot work.

    ``ticker`` names the partition to look in. Left as ``None``, it is derived from
    ``occ_symbol``'s first six characters: ``'SPY   260918C00650000'[:6].strip()`` is
    ``'SPY'``, which matches every row of the live lake. It is still a guess rather than
    a guarantee. An index root like ``SPXW``, or a symbol a corporate action rewrote,
    can differ from the ticker the partition is keyed by, and ``ticker`` overrides the
    derivation for those.

    The selection is supplied directly, an OCC symbol rather than a tag or a minute's
    spellings, so this skips the resolve pass ``load_chain`` and ``load_quotes`` run
    first: there is nothing here for it to resolve. What is shared is everything past
    that: the path build, the spelling check, the quarantine guard, the fetch pass with
    its overflow half, the overflow projection, and the final filter to data rows.

    The rows come back ordered by the instant each ``snap_ts`` names, never by the
    stored text or the partition's own layout. The same instant has more than one ISO
    spelling, so a lexicographic sort of the text disagrees with time order whenever
    spellings mix, and a series is the one read whose order a caller will assume. A
    ``snap_ts`` this read cannot parse as an instant raises a ``LoadError`` naming the
    symbol rather than sorting anyway, because every row here is already part of the
    answer and none of them is excused the way an unreadable value beside a resolved
    minute is on ``load_chain``.

    A contract absent from the partition raises ``ContractAbsent``, naming the symbol
    and the ticker the read looked under, whether that ticker was derived or given.
    A contract present for only some of the session's minutes is a real partial answer
    and comes back as one: nothing here comes back empty, because an empty answer and an
    absent one read the same and mean opposite things, the same rule ``load_chain`` and
    ``load_quotes`` already apply to a session.

    A row with no ``row_kind`` refuses the reads whose fetched rows include it, rather
    than every read of the day. There is no whole-partition resolve pass here to catch
    it ahead of the fetch the way ``load_chain`` and ``load_quotes`` do, so the check
    runs on the rows the fetch actually reads instead.

    ``include_quarantined`` and ``lake_root`` carry the same meaning and the same
    defaults ``load_chain`` gives them. A ticker-day with no sealed chains partition
    raises ``PartitionAbsent`` the same way, naming the derived OCC root too when
    ``ticker`` was not given, so a caller can see why that ticker was tried.
    """
    day_text = day.isoformat() if isinstance(day, date) else str(day)
    ticker_used = ticker if ticker is not None else _occ_root(occ_symbol)
    absent_detail = (
        f" {occ_symbol!r}'s OCC root names {ticker_used!r}; pass ticker= to override it."
        if ticker is None
        else ""
    )
    root, path = _open_partition(
        ticker_used,
        day_text,
        lake_root=lake_root,
        include_quarantined=include_quarantined,
        surface=CHAINS,
        absent_detail=absent_detail,
    )

    selection = _Selection(OCC_SYMBOL_COLUMN, (occ_symbol,))
    table = _fetch_selection(
        path, root, selection, ticker_used, day_text, CHAINS, check_row_kind=True
    )
    if table.num_rows == 0:
        raise ContractAbsent(occ_symbol, ticker_used, day_text)
    return _sorted_by_instant(table, SNAP_TS_COLUMN, occ_symbol, ticker_used, day_text)


def resolve_lake_root(lake_root: Path | str | None) -> Path:
    """``lake_root`` as a path, resolving ``None`` to the configured lake.

    This is the one place in the read layer that reads a config file, and it is a
    function rather than a line inside ``_load_surface`` so that it stays one place as
    the layer grows. ``lake.oi`` needs the same resolution and reads three more files
    under the root, so a second no-argument ``load_config`` would have been the second
    source of truth for where the lake is.

    Every resolution past this point takes the root as an argument, so a test points it
    at a fixture lake and no helper below reaches for config.
    """
    return Path(load_config().lake_root if lake_root is None else lake_root)


def list_chain_cycles(
    ticker: str,
    day: date | str,
    *,
    lake_root: Path | str | None = None,
    include_quarantined: bool = False,
) -> tuple[str, ...]:
    """The session's stored chains cycles, as the ET minutes ``load_chain`` can name.

    A reader that walks a session cycle by cycle has to know which cycles it holds, and
    asking ``load_chain`` minute by minute over a calendar grid would both cost a read
    per absent minute and answer for minutes no cycle ran in. This is the resolve pass on
    its own: one read of ``snap_ts`` and ``row_kind`` over the partition, the same path
    build, spelling check and quarantine guard every other read makes, and the same
    data-rows-only rule.

    The answer is ``HH:MM`` ET strings in instant order, deduplicated, each of which
    ``load_chain(ticker, day, snap=...)`` resolves back to the cycle it came from.

    **A cycle whose ET instant falls on another date is not listed.** ``snap`` is a
    wall-clock minute read against the session date, so an instant on any other date has
    no ``HH:MM`` that names it and ``load_chain`` cannot return it. Onboarding makes this
    real rather than hypothetical: it journals its first chain snapshot under the session
    date at whatever hour it runs, and the live lake holds one stamped 03:25Z under
    ``date=2026-09-16``, which is 23:25 ET on 2026-09-15. Such a cycle carries the
    previous session's quotes, so a reader asking what changed during this session is
    right not to see it, but the reason it does not is this rule rather than that
    judgement. The count of what was dropped is not returned, because a caller that could
    act on it would need the instants, which is a door this one is not.
    """
    day_text = day.isoformat() if isinstance(day, date) else str(day)
    _root, path = _open_partition(
        ticker,
        day_text,
        lake_root=lake_root,
        include_quarantined=include_quarantined,
        surface=CHAINS,
    )
    resolved = _read(path, columns=list(MINUTE_COLUMNS))
    is_data = pc.equal(resolved.column(ROW_KIND_COLUMN), ROW_KIND_DATA)
    if is_data.null_count:
        raise LoadError(
            f"{ticker} {day_text} holds {is_data.null_count} rows with no "
            f"{ROW_KIND_COLUMN}, which are neither an observation nor an absence marker."
        )
    session = date.fromisoformat(day_text)
    minutes: dict[datetime, str] = {}
    for text in pc.unique(resolved.filter(is_data).column(SNAP_TS_COLUMN)).to_pylist():
        stamped = _instant(text)
        if stamped is None:
            continue
        local = stamped.astimezone(MARKET_TZ)
        if local.date() != session:
            continue
        minutes.setdefault(local, f"{local.hour:02d}:{local.minute:02d}")
    return tuple(minutes[key] for key in sorted(minutes))


def _load_surface(
    ticker: str,
    day: date | str,
    snap: str | None,
    *,
    lake_root: Path | str | None,
    include_quarantined: bool,
    surface: str,
    close_tag: str,
    no_close_error: type[NoCloseOfRecord],
) -> pa.Table:
    """The body ``load_chain`` and ``load_quotes`` share, parameterised on what differs.

    ``surface`` names the partition to ``lake.paths`` and the schema-shape lookup to
    ``project_extra``. ``LakePaths.partition_path`` already dispatches a date-partitioned
    partition on that same name, so nothing here needs a second, per-surface way to find
    the file. ``close_tag`` is the tag the close of record resolves against, and
    ``no_close_error`` is the exception raised when no row carries it, so each surface
    names its own marker rather than the other's.
    """
    day_text = day.isoformat() if isinstance(day, date) else str(day)
    root, path = _open_partition(
        ticker,
        day_text,
        lake_root=lake_root,
        include_quarantined=include_quarantined,
        surface=surface,
    )

    resolving = CLOSE_COLUMNS if snap is None else MINUTE_COLUMNS
    resolved = _read(path, columns=list(resolving))
    is_data = pc.equal(resolved.column(ROW_KIND_COLUMN), ROW_KIND_DATA)
    if is_data.null_count:
        raise LoadError(
            f"{ticker} {day_text} holds {is_data.null_count} rows with no "
            f"{ROW_KIND_COLUMN}, which are neither an observation nor an absence marker."
        )
    data = resolved.filter(is_data)

    if snap is None:
        selection = _at_close(resolved, data, ticker, day_text, close_tag, no_close_error)
    else:
        selection = _at_minute(resolved, data, ticker, day_text, snap)

    return _fetch_selection(path, root, selection, ticker, day_text, surface, check_row_kind=False)


class _Selection(NamedTuple):
    """The rows an answer is made of, as a column and the values it admits there.

    ``load_chain`` and ``load_quotes`` each resolve one of these off a whole-partition
    scan. ``load_contract`` supplies one directly, since an OCC symbol needs no
    resolving, and skips that scan. Either way, one predicate builds the fetch and one
    filter trims what came back. The values are the spellings a resolve pass found
    rather than a canonical form where one runs, which is what keeps a partition holding
    two spellings of one instant answerable whole; a supplied selection carries just the
    one value the caller asked for.
    """

    column: str
    values: tuple[str, ...]


def _open_partition(
    ticker: str,
    day_text: str,
    *,
    lake_root: Path | str | None,
    include_quarantined: bool,
    surface: str,
    absent_detail: str = "",
) -> tuple[Path, Path]:
    """The lake root and the sealed partition path, or the two refusals every read shares.

    Every read resolves ``lake_root`` the same way, requires the partition to exist
    under its exact on-disk spelling, and clears it against quarantine before touching a
    row. ``absent_detail`` extends the ``PartitionAbsent`` message when it fires, which
    is what lets ``load_contract`` name the OCC root it derived without a second copy of
    the guard that raises it.
    """
    root = resolve_lake_root(lake_root)
    path = LakePaths(root).partition_path(surface, ticker, day_text)
    _clear_partition(
        root,
        path,
        ticker,
        day_text,
        surface,
        include_quarantined=include_quarantined,
        absent_detail=absent_detail,
    )
    return root, path


def _clear_partition(
    root: Path,
    path: Path,
    ticker: str,
    day_text: str,
    surface: str,
    *,
    include_quarantined: bool,
    absent_detail: str = "",
) -> None:
    """The two refusals every read shares, given a path somebody else built.

    ``_open_partition`` builds the path through ``LakePaths.partition_path``, which is for the
    surfaces keyed by ticker and date alone and raises on the other two.  ``load_bars`` builds
    its own through ``bars_partition_path``, because a bars partition carries a ``freq=`` level
    that method has no slot for. Both then need the same two guards, and this is them, so
    neither surface gets its own reading of what an absent or quarantined partition means.

    The quarantine key is the partition's lake-relative path as the caller spelled it, which a
    bars path produces like any other and nothing here parses. That is why the exclusion covers
    a surface ``paths.parse_partition_rel`` refuses to read apart.
    """
    if not (path.is_file() and _spelled_exactly(root, path)):
        raise PartitionAbsent(
            f"{ticker} {day_text} has no sealed {surface} partition at {path}. "
            "A session seals at close+15, and a ticker is spelled as its directory is."
            f"{absent_detail}"
        )

    partition = path.relative_to(root).as_posix()
    if not include_quarantined:
        entry = latest_quarantine(root).get(partition)
        if is_quarantined(entry):
            raise PartitionQuarantined(partition, entry)


def _fetch_selection(
    path: Path,
    root: Path,
    selection: _Selection,
    ticker: str,
    day_text: str,
    surface: str,
    *,
    check_row_kind: bool,
) -> pa.Table:
    """The fetch pass, the overflow projection, and the final filter, shared by every read.

    ``check_row_kind`` is the one difference among the three callers. ``load_chain`` and
    ``load_quotes`` already rule out a null ``row_kind`` over the whole partition during
    their resolve pass, before this ever runs. ``load_contract`` supplies its selection
    directly and has no resolve pass to catch it there, so this checks the rows the
    fetch actually reads instead. That refuses only the reads whose fetched rows include
    the damaged one, the same scoping #251 already chose for a row's own schema version,
    rather than every read of the day.
    """
    fetched = _read(path, filters=_predicate(selection, pq.read_schema(path).names))
    if check_row_kind:
        is_data = pc.equal(fetched.column(ROW_KIND_COLUMN), ROW_KIND_DATA)
        if is_data.null_count:
            raise LoadError(
                f"{ticker} {day_text} holds {is_data.null_count} fetched rows with no "
                f"{ROW_KIND_COLUMN}, which are neither an observation nor an absence marker."
            )
    projection = project_extra(fetched, surface=surface, ledger=_ledger(root))
    if not projection.complete:
        raise PartialRead(ticker, day_text, surface, projection)
    table = projection.table
    return table.filter(
        pc.and_(
            pc.equal(table.column(ROW_KIND_COLUMN), ROW_KIND_DATA),
            pc.is_in(table.column(selection.column), value_set=pa.array(selection.values)),
        )
    )


def _read(
    path: Path, *, columns: list[str] | None = None, filters: ds.Expression | None = None
) -> pa.Table:
    """Every Parquet read of rows this module makes. Reading the schema is metadata only.

    A correct predicate returns the rows a full read would, so nothing about a result says
    whether one happened, and a wall-clock timing is not a test. This is one function so a
    test can replace it and assert on what it was asked for.
    """
    return pq.read_table(path, columns=columns, filters=filters)


def _predicate(selection: _Selection, carried: Sequence[str]) -> ds.Expression:
    """Which rows the fetch pass reads: the answer's rows, and the ones that shape it.

    The predicate has two halves. The first names the answer's rows. The second names
    every row carrying an overflow value, wherever in the session it sits, because those
    are the only rows that can add a promoted column and the column set has to be a
    property of the partition rather than of the minute asked for. A row whose overflow is
    null can fill nothing, and a row group whose statistics say every row in it is null is
    skipped whole, so the second half is free on a partition the vendor never drifted
    through.

    A partition whose schema has no ``extra`` column at all gets the first half alone.
    Naming a column Parquet does not have fails the scan, and what a table missing its
    overflow column means belongs to ``lake.extra_projection``, which says so by name the
    moment the fetch hands it the rows. The column set needs no protecting on such a
    partition, because that refusal takes the read away either way.
    """
    rows = ds.field(selection.column).isin(selection.values)
    if EXTRA_COLUMN not in carried:
        return rows
    return rows | ds.field(EXTRA_COLUMN).is_valid()


def _at_close(
    resolved: pa.Table,
    data: pa.Table,
    ticker: str,
    day: str,
    close_tag: str,
    no_close_error: type[NoCloseOfRecord],
) -> _Selection:
    """The session's close-of-record cycle, by tag and never by clock.

    ``close_tag`` is the tag that cycle carries on this surface, and ``no_close_error`` is
    the marker raised when no row carries it, so a chains read and a quotes read each
    resolve against their own tag and name their own exception when it is absent.
    """
    tagged = data.filter(pc.equal(data.column(CLOSE_TAG_COLUMN), close_tag))
    if tagged.num_rows == 0:
        raise no_close_error(ticker, day, _tagged_gaps(resolved, close_tag))
    spellings = pc.unique(tagged.column(SNAP_TS_COLUMN)).to_pylist()
    instants = {_instant(text) for text in spellings}
    if None in instants:
        raise LoadError(
            f"{ticker} {day} tags {close_tag} on a row whose {SNAP_TS_COLUMN} cannot "
            f"be read as an instant, among {sorted(str(text) for text in spellings)}."
        )
    if len(instants) > 1:
        raise LoadError(
            f"{ticker} {day} tags {close_tag} on {len(instants)} cycles, "
            f"{sorted(str(moment) for moment in instants)}. A close of record is one cycle."
        )
    return _Selection(CLOSE_TAG_COLUMN, (close_tag,))


def _at_minute(resolved: pa.Table, data: pa.Table, ticker: str, day: str, snap: str) -> _Selection:
    """The rows whose ``snap_ts`` is the ET wall-clock minute ``snap`` on ``day``."""
    target = _target_instant(day, snap)
    naming, unreadable = _read_snaps(data.column(SNAP_TS_COLUMN), target)
    if not naming:
        raise SnapAbsent(ticker, day, snap, _gaps_at(resolved, target), unreadable)
    return _Selection(SNAP_TS_COLUMN, tuple(naming))


def _gaps_at(table: pa.Table, target: datetime) -> int:
    """How many absence markers the partition holds at the instant ``target``.

    This runs only on the way to raising, so it never costs a read that found its minute.
    A gap row whose own ``snap_ts`` cannot be read is left out of the count rather than
    raising, because this is the explanation of a refusal and not the answer to a read.
    """
    gaps = table.filter(pc.not_equal(table.column(ROW_KIND_COLUMN), ROW_KIND_DATA))
    naming, _ = _read_snaps(gaps.column(SNAP_TS_COLUMN), target)
    if not naming:
        return 0
    return gaps.filter(pc.is_in(gaps.column(SNAP_TS_COLUMN), value_set=pa.array(naming))).num_rows


def _tagged_gaps(table: pa.Table, close_tag: str) -> int:
    """How many absence markers the partition carries under ``close_tag``."""
    gaps = table.filter(pc.not_equal(table.column(ROW_KIND_COLUMN), ROW_KIND_DATA))
    return gaps.filter(pc.equal(gaps.column(CLOSE_TAG_COLUMN), close_tag)).num_rows


def _target_instant(day: str, snap: str) -> datetime:
    """``snap`` read as an ET wall-clock minute on ``day``, as an instant.

    The lake stores ``snap_ts`` in UTC, so the conversion happens here rather than in a
    caller's head. The zone is the one the calendar pins, so a summer minute and a winter
    minute each land on the offset that session actually ran under.

    The datetime is built from integers rather than through ``time(hour, minute)``. The
    session-time scanner reads every ``datetime.time(...)`` construction under ``src/lake``
    as a hardcoded session time and lets an integer pair past. The hour and minute come
    from the caller's argument and no literal time appears, so the integer form is both
    safe and visibly safe to the scanner.
    """
    if not _SNAP_SHAPE.fullmatch(snap):
        raise SnapMalformed(f"snap {snap!r} is not an HH:MM ET wall-clock minute")
    hour, minute = (int(part) for part in snap.split(":"))
    session = date.fromisoformat(day)
    return datetime(session.year, session.month, session.day, hour, minute, tzinfo=MARKET_TZ)


def _read_snaps(column: pa.ChunkedArray, target: datetime) -> tuple[list[str], tuple[str, ...]]:
    """The distinct ``snap_ts`` texts naming ``target``, and the ones that cannot be read.

    One instant has more than one ISO spelling, so the match runs on parsed instants and
    the spellings are what the filter then selects on. Parsing the distinct values costs
    one parse per minute in the partition rather than one per row.

    A value that cannot be read is reported rather than raised. Refusing the whole
    partition would take away an answer the partition can give, since a read that matched
    its minute has nothing left to be ambiguous about. The caller raises only when nothing
    matched, and then the unreadable values are part of why.
    """
    naming: list[str] = []
    unreadable: list[str] = []
    for text in pc.unique(column).to_pylist():
        stamped = _instant(text)
        if stamped is None:
            unreadable.append(repr(text))
        elif stamped == target:
            naming.append(text)
    return naming, tuple(unreadable)


def _instant(text: object) -> datetime | None:
    """``text`` as an instant, or ``None`` when it does not name one.

    A stamp with no UTC offset does not name one. Comparing a naive datetime with an aware
    one returns false rather than raising, so letting one through would quietly answer
    that no cycle recorded a minute whose row simply never said which minute it was.
    """
    if not isinstance(text, str):
        return None
    try:
        stamped = datetime.fromisoformat(text)
    except ValueError:
        return None
    return None if stamped.tzinfo is None else stamped


# Where an OCC symbol carries the underlying's root: the first six characters, padded
# with spaces and stripped. ``'SPY   260918C00650000'[:6].strip()`` is ``'SPY'``.
_OCC_ROOT_WIDTH = 6


def _occ_root(occ_symbol: str) -> str:
    """The ticker ``load_contract`` derives from an OCC symbol when none is given.

    This matches every row of the live lake and is still a guess rather than a
    guarantee. An index root like ``SPXW``, or a symbol a corporate action rewrote, can
    differ from the ticker the partition is keyed by, which is what ``ticker=`` on
    ``load_contract`` is for.
    """
    return occ_symbol[:_OCC_ROOT_WIDTH].strip()


def _sorted_by_instant(
    table: pa.Table, column: str, subject: str, ticker: str, day_text: str
) -> pa.Table:
    """``table`` ordered by the instant each row's stamp names, not the stored text.

    ``column`` is the stamp a surface sits in, ``snap_ts`` on chains and quotes and ``bar_ts``
    on bars, and ``subject`` is what the refusal names the rows by, an OCC symbol for one
    contract's session and a frequency for a bars series. The rule below is one rule, so the
    two series read through it rather than beside each other.

    The same instant has more than one ISO spelling, so a lexicographic sort of the text
    disagrees with time order whenever spellings mix: SPY's sealed 2026-09-11 partition
    holds 406 rows spelled ``+00:00`` and 2 with an Eastern offset, and the Eastern
    spelling of a later instant sorts before the earlier one written as ``+00:00``. A
    series is the one read whose order a caller will assume, and it cannot be inherited
    from the partition's own layout either, per #242's audit of what row-group pruning
    actually guarantees. So this parses every value and sorts on that instead.

    A stamp that cannot be read as an instant raises rather than sorting anyway.
    ``load_chain`` and ``load_quotes`` can set an unreadable value aside, because it sits
    beside the one minute or cycle that answers their read and never in it. Every row a
    series read hands here is already part of the answer, selected by ``occ_symbol`` or by
    the day its partition is keyed under rather than by instant, so an unreadable stamp has
    no ambiguity to be excused from: it is a row this read owes an order to and cannot give
    one.
    """
    texts = table.column(column).to_pylist()
    instants = [_instant(text) for text in texts]
    pairs = zip(texts, instants, strict=True)
    unreadable = sorted({repr(text) for text, instant in pairs if instant is None})
    if unreadable:
        raise LoadError(
            f"{ticker} {day_text} holds {len(unreadable)} {subject!r} rows whose "
            f"{column} cannot be read as an instant: {unreadable}."
        )
    order = sorted(range(len(texts)), key=lambda index: instants[index])
    return table.take(pa.array(order, type=pa.int64()))


def _spelled_exactly(root: Path, path: Path) -> bool:
    """Whether every part of ``path`` under ``root`` is spelled as it is on disk.

    macOS matches a path case-insensitively, so ``ticker=spy`` opens the ``ticker=SPY``
    partition. The quarantine lookup keys on the path as the caller spelled it, so a read
    under the wrong case would open a real partition and miss its verdict. The guard would
    then fail open on exactly the partition it exists to withhold.
    """
    current = root
    for name in path.relative_to(root).parts:
        try:
            if name not in os.listdir(current):
                return False
        except OSError:
            return False
        current = current / name
    return True


def _ledger(root: Path) -> SchemaVersionLedger:
    """The lake's schema-version ledger, or an empty one when the lake has none.

    An absent ledger is not a case of its own. Every version in the table is then one the
    ledger holds no shape for, which is the condition the projection already reports and
    ``load_chain`` already refuses, and the refusal names the versions either way.
    """
    path = ledger_path(root)
    if not path.is_file():
        return SchemaVersionLedger()
    return SchemaVersionLedger.read(path)
