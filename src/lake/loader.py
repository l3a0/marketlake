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
from datetime import date, datetime
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
from lake.paths import CHAINS, QUOTES, LakePaths
from lake.schema_versions import SchemaVersionLedger, ledger_path
from lake.session import OPTION_CLOSE, SPOT_CLOSE

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

__all__ = [
    "ContractAbsent",
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
    return _sorted_by_snap(table, occ_symbol, ticker_used, day_text)


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
    return root, path


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


def _sorted_by_snap(table: pa.Table, occ_symbol: str, ticker: str, day_text: str) -> pa.Table:
    """``table`` ordered by the instant each row's ``snap_ts`` names, not the stored text.

    The same instant has more than one ISO spelling, so a lexicographic sort of the text
    disagrees with time order whenever spellings mix: SPY's sealed 2026-09-11 partition
    holds 406 rows spelled ``+00:00`` and 2 with an Eastern offset, and the Eastern
    spelling of a later instant sorts before the earlier one written as ``+00:00``. A
    series is the one read whose order a caller will assume, and it cannot be inherited
    from the partition's own layout either, per #242's audit of what row-group pruning
    actually guarantees. So this parses every value and sorts on that instead.

    A ``snap_ts`` that cannot be read as an instant raises rather than sorting anyway.
    ``load_chain`` and ``load_quotes`` can set an unreadable value aside, because it sits
    beside the one minute or cycle that answers their read and never in it. Every row
    here is already part of the answer, resolved by ``occ_symbol`` rather than by
    instant, so an unreadable ``snap_ts`` has no ambiguity to be excused from: it is a
    row this read owes an order to and cannot give one.
    """
    texts = table.column(SNAP_TS_COLUMN).to_pylist()
    instants = [_instant(text) for text in texts]
    pairs = zip(texts, instants, strict=True)
    unreadable = sorted({repr(text) for text, instant in pairs if instant is None})
    if unreadable:
        raise LoadError(
            f"{ticker} {day_text} holds {len(unreadable)} {occ_symbol!r} rows whose "
            f"{SNAP_TS_COLUMN} cannot be read as an instant: {unreadable}."
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
