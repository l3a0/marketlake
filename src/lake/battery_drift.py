"""The battery's own nightly schema-drift page: a day's sealed partitions, compared.

The design's message table gives schema drift four producers and this is the last to
ship. The parser's fires the minute a response is parsed, off the batch it just built.
The close+5 fill's is the same producer through ``observe_partial``, for the one ticker
it refetched. Compaction's compares a ticker-day's merged segment schema against the
pinned one, at the merge, which is the last moment the segments exist. This one reads
sealed partitions at night, and it is the only one of the four that sees a whole day at
once. Marketlake #427 is the issue, and it exists because #138's split distributed the
schema-drift check across six sub-issues, #406 to #411, and this page was in none of them.

**Two halves, and both are day-over-day transitions.** ``docs/design.md`` says "A missing
or retyped known field pages", and the two need different evidence.

1. *Retyped.* A known vendor field arriving at a type its column refuses is nulled in the
   column and parked in ``extra`` under the vendor's own name, per
   ``journal._routed_column``. ``journal.routed_columns`` reads that signature back off a
   batch, and it survives into the sealed partition.
2. *Missing.* A vendor column null on every data row of the day. Stated as an absolute
   null test this pages on the first night: ``volatility``, ``high_52`` and ``low_52`` are
   null on every data row of every sealed quotes partition today and always have been. So
   it is the same day-over-day transition the retype half is. Marketlake #265 measured the
   rule that does not fire, a contract column that was arriving stopping, at zero
   occurrences over 9,839,818 data rows.

**The retype half runs first and the missing half subtracts it.** A vendor retype normally
reaches every row of the payload, and then the column is null on every data row, which is
exactly what the missing half looks for. One fact would send two pages and the second
would say a field went missing while the vendor is still sending it.
``journal.routed_columns``'s own docstring settles which reading is right: "A routed value
is nulled in its column on the same row it was parked from." So an all-null column whose
vendor name sits in ``extra`` is a retype, and :func:`judge_day` subtracts the retype
half's columns from the missing half's candidates.

**The evidence is in the Parquet footer, and reading it there is exact rather than a
shortcut.** Both halves reduce to a per-column null count.

- The retype half's first gate is ``journal.routed_columns``'s own,
  ``overflow.null_count == len(overflow)``, hoisted out of the read.
- The missing half is the whole answer. The footer counts nulls over every row, data and
  gap together, and ``journal.gap_batch`` says the difference cannot matter: "Every vendor
  column is null on a gap by construction." So a footer null count equal to the row count
  is equivalent to all-null on the data rows.

Measured on SPY's 2026-09-16 chains partition, 307 MB and 5,307,030 rows, the footer
answers ``extra`` in 2.0 ms against 0.65 s for the full read, and the whole lake's 29
sealed partitions answer in 14.1 ms with no column lacking statistics. A column whose
footer carries no statistics falls back to reading that column alone, because a missing
statistic must not read as a missing field.

**The fold is across tickers and across fields, and the arithmetic is why.** One vendor
change reaches every ticker on the surface on the same day, so paging per ticker would
scale the page count with the roster while the fact stayed one fact. Paging per field is
the same mistake on the other axis: ``journal.extra_paths`` gives the two surfaces 56 and
63 vendor columns, and a rotation moves a whole surface at once, so a page per field
spends 56 or 63 of ``alert.DEFAULT_DAILY_CAP``'s forty on one fact and what it swallows
could be the auth-death page. ``schema_drift._body`` and ``compact._page_drift`` both fold
the same way for the same reason.

The two halves stay apart, because the design gives them different titles. So a night
sends at most four pages, one per surface per half.

**Across runs the transition suppresses, and there is no state file.** A field already
drifting on the previous judged day is not a transition. The baseline is that day's sealed
partitions, re-read, and sealed data is immutable, so the derivation answers the same on
every re-run. What that costs is a second sweep in one night paging again:
``alert.Publisher`` carries a daily cap and a secret-leak refusal and no per-event dedupe,
and its own docstring says the cap is "held in memory, so it resets when the date turns
and also when the process does". The design accepts that price for nightly jobs rather
than asking for a guard, at ``docs/design.md``'s "the run that would clear a drift is the
run that finds it again". launchd cannot cause it: ``com.marketlake.eod-sweep`` carries
``StartCalendarInterval`` on weekdays 1 to 5 at 18:30 with ``RunAtLoad`` false.

**A day with no readable baseline reports and does not page.** With no baseline there is no
way to separate a field that stopped arriving from one that never arrived, which is the
false-positive class #265 measured. The row-count band already names this shape
``insufficient_history``. ``docs/design.md`` puts it as the check reporting
``insufficient_history``, "which fails open", where reaching back further would measure
against a roster that has since moved.

**The missing half owes a rotation guard, and the design says why.** A column this
project's own release rotation dropped is filled with nulls at the merge, and after the
seal nothing can tell that from a vendor that stopped sending the field. That is
compaction's fact rather than this check's.
``schema_versions.RecordedVersion.has_column`` is what separates them, and its docstring
already states the distinction: "A null under a column this returns false for was never
observed, rather than observed as null."

Measured, the guard is inert against tonight. All 29 sealed partitions carry
``schema_version`` 1 and every one of 2026-09-17's journal segments carries 2, so tonight
is the first night the comparison crosses a version boundary. What keeps the guard silent
is that the boundary moved nothing: the ledger's version 2 fingerprints are
column-identical to version 1 on both surfaces, 73 chains columns and 75 quotes columns,
with version 2 adding the ``bars`` surface alone. It is built now rather than later
because it sits inside the comparison, and adding it afterwards leaves a second read path
that skips it.

**What this check must not touch.** It writes no verdict and no ledger line.
``battery.Finding`` is per partition and every judged finding flows through
``battery.decide_partition`` into the quarantine ledger, where the entitlement check is
the one the design says to quarantine and page. This one's unit crosses tickers, so a
per-partition finding would page once per ticker for a fact that is one fact. It files
nothing under ``reports/schema_drift/`` either: that is compaction's own producer, keyed
on a ticker-day, and ``lake.report``'s second rule gives the cost of filing in another
producer's directory.

**Two surfaces, not three.** ``battery.SEALED_SURFACES`` is chains and quotes, and the
design pins a schema on three. Marketlake #500 carries whether the design's schema policy
covers the ``bars`` surface at all, and owes that decision before anything widens this.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import pyarrow.compute as pc
import pyarrow.parquet as pq

from lake import journal
from lake.alert import REFUSED, Message, Publisher
from lake.calendar import MARKET_TZ
from lake.journal import EXTRA_COLUMN, ROW_KIND_COLUMN, ROW_KIND_DATA
from lake.schema_versions import (
    SchemaVersionLedger,
    SchemaVersionsError,
    ledger_path,
)

# The event name follows the convention ``schema_drift.py`` writes down: the producer's
# name in front of the condition, so a reader of ``reports/alerts/`` can tell them apart
# without opening a file. ``schema_drift.SCHEMA_DRIFT_EVENT`` is ``parser_schema_drift``
# and ``compact.SCHEMA_DRIFT_EVENT`` is ``compaction_schema_drift``.
SCHEMA_DRIFT_EVENT = "battery_schema_drift"

# The two halves of the design's title, ``Schema drift: <field> missing`` or ``retyped``.
MISSING = "missing"
RETYPED = "retyped"

# How many field names a page body prints before it stops and says how many are left.
#
# The number is ``schema_drift.PAGE_COLUMN_CAP``'s, borrowed rather than invented, and the
# arithmetic that says a cap is owed at all is this surface's own. ``docs/design.md`` pins
# every body at plain text under 1,000 bytes. Measured through :func:`body` itself, a body
# naming every field of a whole-surface drift runs to 866 bytes on chains and 1,122 on
# quotes, so the widest drift is the one that would not reach the phone. At this cap the
# same two bodies are 305 and 295 bytes.
#
# The count survives the cut for that module's stated reason: it is what separates one
# moved field from a wholesale rotation. The nightly report's line names every field
# either way.
PAGE_FIELD_CAP = 12

# How far back the baseline walk will step before giving up. A weekend puts two
# non-sessions between Friday and Monday and a holiday can add a third, so the walk needs
# more than three days of room. It is bounded at all because a lake whose earlier days
# were never sealed must not turn one night's check into a walk over the whole calendar.
BASELINE_LOOKBACK_DAYS = 10


class DriftUnreadable(Exception):
    """A partition or the version ledger this check could not read.

    Contained per surface-day by :func:`judge_day`, for the reason
    ``battery.trailing_medians`` gives for the same containment: a session the run was not
    asked about is not this check's to announce.
    """


@dataclass(frozen=True)
class SurfaceDay:
    """What one surface's partitions say about one day's payload shape.

    ``retyped`` and ``absent`` are folded across the day's tickers, and the two folds run
    opposite ways on purpose.

    ``retyped`` is a union. One ticker routing a field is evidence the vendor sent it at a
    type the column refused, and the other tickers' silence does not contradict it.

    ``absent_by_ticker`` is kept per ticker rather than folded here, and
    :func:`newly_absent` folds it against the baseline day over the tickers the two days
    share. Folding to one set on each day and subtracting them compares two intersections
    taken over different rosters, which pages the moment a ticker drops out: a field
    already absent on the surviving ticker on both days enters the intersection today and
    was kept out of it yesterday by the ticker that has since gone. An ordinary dead-daemon
    day on one ticker is enough to produce it.

    The fold itself is still an intersection, because a field still arriving on one ticker
    is a field the vendor is still sending. ``schema_drift`` counts its evidence per ticker
    for the mirror of this reason.

    ``versions`` carries every ``schema_version`` the day's rows hold. More than one means
    the day spans a rotation, which is compaction's fact and not this check's, and the
    rotation guard refuses to judge the missing half on such a day.

    ``first_cycle`` is the day's earliest data ``snap_ts``. It answers the design's "first
    cycle that saw it" for a missing field, which has no drifting row to point at because
    the field is null on every data row.
    """

    surface: str
    day: date
    retyped: frozenset[str]
    absent_by_ticker: Mapping[str, frozenset[str]]
    lacked_by_ticker: Mapping[str, frozenset[str]]
    unreadable: tuple[str, ...]
    versions: frozenset[int]
    first_cycle: str | None
    tickers: tuple[str, ...]

    @property
    def judged(self) -> bool:
        """Whether any partition on this surface-day carried a data row."""
        return bool(self.tickers)


@dataclass(frozen=True)
class DriftFinding:
    """One surface's drift of one kind on one day, folded across tickers and fields.

    ``fields`` is sorted, because the page and the report line both print it and a set's
    iteration order would make one night's body differ from another's for no reason.

    ``first_cycle`` is an ET string. ``docs/design.md`` says a body carries "what is lost,
    since when in ET", and a sealed ``snap_ts`` is a UTC string: the lake's 2026-09-16
    quotes partitions run ``2026-09-16T13:30:00+00:00`` to ``2026-09-16T20:15:00+00:00``.
    Printed raw that is the one thing that sentence rules out, on the page whose whole
    content is a stamp.
    """

    surface: str
    day: date
    kind: str
    fields: tuple[str, ...]
    first_cycle: str | None

    @property
    def title(self) -> str:
        """The design's title, which names the field when there is one to name.

        The message table writes it ``Schema drift: <field> missing``, or ``retyped``, and
                says the page fires "once per field per day". That is a rate per field rather than a
                page each, which is what the fold delivers: a field pages at most once on
                the day it transitions, and the fields that transition together share one
                page. Paging per field instead would spend the day's cap on one vendor
                change, which is the arithmetic ``schema_drift`` and ``compact`` both
                already settled the same way.

                So the placeholder cannot always hold one name. One field still reads exactly as the
                table writes it, and several say how many rather than picking one.
        """
        if len(self.fields) == 1:
            return f"Schema drift: {self.fields[0]} {self.kind}"
        return f"Schema drift: {len(self.fields)} {self.surface} fields {self.kind}"

    @property
    def line(self) -> str:
        """The nightly report's line, which names every field and never caps.

        ``battery.render`` prints this on a hand run and ``sweep`` puts it in the nightly
        report. Neither is the phone, so the cap that protects a page body does not apply,
        and the count that survives the page's cut is what sends a reader here.
        """
        named = ", ".join(self.fields)
        seen = f", first cycle {self.first_cycle}" if self.first_cycle else ""
        return (
            f"battery: schema drift on {self.surface} {self.day.isoformat()}: "
            f"{named} {self.kind}{seen}"
        )


@dataclass(frozen=True)
class _Footer:
    """One partition's per-column null counts, and the two ways a column can be missing
    from them.

    The two are answered differently and conflating them is a defect rather than a
    simplification.

    ``unmeasured`` is a column the partition carries whose footer wrote no null-count
    statistic. It is answered by reading that column, because a statistic Parquet did not
    write must never read as a field the vendor stopped sending. Measured over the lake's
    29 sealed partitions it is empty everywhere, so it is a guard rather than a path the
    ordinary night takes.

    ``absent_column`` is a column the partition does not carry at all. Reading it raises,
    and counting it as wholly null would page for the shape it cannot be about: a column a
    partition lacks outright is a rotation's doing, not a vendor's, and the version guard
    is what speaks to that. So it leaves the missing half's candidate set entirely.
    """

    counts: dict[str, int]
    rows: int
    unmeasured: set[str]
    absent_column: set[str]


def _footer_null_counts(path, columns: Sequence[str]) -> _Footer:
    """Each column's null count and the partition's row count, from the Parquet footer."""
    metadata = pq.read_metadata(path)
    arrow = metadata.schema.to_arrow_schema()
    counts: dict[str, int] = {}
    unmeasured: set[str] = set()
    absent_column: set[str] = set()
    for column in columns:
        index = arrow.get_field_index(column)
        if index < 0:
            absent_column.add(column)
            continue
        total = 0
        for group in range(metadata.num_row_groups):
            statistics = metadata.row_group(group).column(index).statistics
            if statistics is None or not statistics.has_null_count:
                unmeasured.add(column)
                break
            total += statistics.null_count
        else:
            counts[column] = total
    return _Footer(
        counts=counts, rows=metadata.num_rows, unmeasured=unmeasured, absent_column=absent_column
    )


def _lookup(overflow: Mapping[str, object], path: journal.ExtraPath) -> object:
    """The overflow value at ``path``, or ``None`` when the row does not carry it.

    A flat path reads the key straight off the overflow and a nested one reads the block
    first, which is how the quotes blocks stay apart from each other and the chains
    surface's chain-level fields from the contract's.

    ``extra_projection`` resolves the same path the same way and this does not import it,
    because that function is private to a module whose own reason for existing is lifting
    values out of the overflow. What keeps the two from drifting is that both read
    ``journal.ExtraPath`` rather than a spelling of their own, and the path is two fields.
    """
    if path.block is None:
        return overflow.get(path.field)
    block = overflow.get(path.block)
    if not isinstance(block, Mapping):
        return None
    return block.get(path.field)


def read_surface_day(partitions: Sequence, surface: str, day: date) -> SurfaceDay:
    """Fold one surface's partitions for one day into what they say about the payload.

    A partition holding no data row is skipped rather than judged, which is
    ``battery._judge_partition``'s own rule and it matters here more than anywhere. Its
    gap rows carry a null on every vendor column by construction, so an all-gap partition
    reads as every field missing at once. Sixteen of the lake's sealed partitions are
    exactly that shape today.
    """
    paths = journal.extra_paths(surface)
    columns = sorted(paths)
    retyped: set[str] = set()
    absent: dict[str, frozenset[str]] = {}
    lacked: dict[str, frozenset[str]] = {}
    versions: set[int] = set()
    first: str | None = None
    tickers: list[str] = []
    unreadable: list[str] = []

    for partition in partitions:
        try:
            footer = _footer_null_counts(partition.path, [*columns, EXTRA_COLUMN, ROW_KIND_COLUMN])
        except OSError as exc:
            unreadable.append(f"{partition.relative} did not open: {exc}")
            continue
        except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
            unreadable.append(f"{partition.relative} has no readable footer: {exc}")
            continue

        rows = footer.rows
        if rows == 0:
            continue
        # The overflow gate, which is ``routed_columns``'s own first gate read off the
        # footer. An all-null overflow is every cycle the lake has recorded, so this is
        # the branch the healthy night takes and it opens no rows at all.
        #
        # A partition carrying no ``extra`` column at all cannot be asked the retype
        # question, and ``routed_columns`` would raise on it rather than answer. That is a
        # rotation's shape, so it is left to the version guard the same way an absent
        # vendor column is.
        overflow_nulls = footer.counts.get(EXTRA_COLUMN)
        needs_rows = EXTRA_COLUMN not in footer.absent_column and (
            EXTRA_COLUMN in footer.unmeasured or overflow_nulls != rows
        )

        table = None
        if needs_rows:
            try:
                table = _read_partition(partition, surface)
                # **Every batch, not the first.** ``pq.read_table`` chunks a table by row
                # group, and the lake's chains partitions carry five and six of them:
                # SPY 2026-09-16 is 5,307,030 rows across six, whose first covers
                # 03:25 to 14:49 UTC. Asking only ``to_batches()[0]`` therefore reads
                # pre-market to 10:49 ET and calls the rest of the session clean, so a
                # retype that starts mid-day is silent on both halves at once: the column
                # is non-null on the morning rows, so it never reaches ``absent`` either,
                # and the night's report line says nothing drifted.
                #
                # That is the failure this producer exists to prevent. The parser's page
                # fires per response and this one is "the only one of the four that sees a
                # whole day at once", which it does not do by reading a fifth of the day.
                #
                # Folding rather than ``combine_chunks`` is deliberate. Combining would
                # materialise 5.3 million rows across 73 columns to answer a question each
                # batch answers on its own, and ``routed_columns``'s two gates make a batch
                # with an all-null overflow cost almost nothing.
                for batch in table.to_batches():
                    retyped.update(journal.routed_columns(surface, batch))
            except DriftUnreadable as exc:
                unreadable.append(str(exc))
                continue
            except KeyError as exc:
                # ``routed_columns`` asks every column in ``extra_paths`` for its null
                # count, so a partition short of one raises rather than answering. A
                # partition that does not carry the running schema's columns is
                # compaction's fact, per ``docs/design.md``'s schema policy, and it is
                # reported rather than guessed at.
                unreadable.append(f"{partition.relative} does not carry the running schema: {exc}")
                continue
            except ValueError as exc:
                # ``routed_columns`` decodes each populated overflow with a bare
                # ``json.loads``, so one cell of unparseable JSON raises out of it.
                # Unconverted, that escapes :func:`judge_day` past the per-surface
                # containment, and one bad cell on chains takes the quotes comparison down
                # with it on the same night. ``JSONDecodeError`` subclasses ``ValueError``,
                # which is what this catches, because the raise is the standard library's
                # rather than this project's and naming the subclass would bind to it.
                unreadable.append(
                    f"{partition.relative} carries an overflow that does not decode: {exc}"
                )
                continue

        try:
            data = _data_rows(partition, surface, table)
        except DriftUnreadable as exc:
            unreadable.append(str(exc))
            continue
        if data.num_rows == 0:
            continue
        tickers.append(partition.ticker)

        # A column the footer carries but could not measure is read outright. Reading the
        # column alone is cheap next to the whole partition, and it keeps an absent
        # statistic from reading as an absent field.
        resolved = dict(footer.counts)
        unmeasured = footer.unmeasured & set(columns)
        if unmeasured:
            try:
                resolved.update(_null_counts_by_read(partition, surface, sorted(unmeasured)))
            except DriftUnreadable as exc:
                unreadable.append(str(exc))
                tickers.pop()
                continue

        here = {
            column
            for column in columns
            if column not in footer.absent_column and resolved.get(column) == rows
        }
        absent[partition.ticker] = frozenset(here)
        lacked[partition.ticker] = frozenset(footer.absent_column & set(columns))

        stamp, seen_versions = _stamps_and_versions(data)
        versions.update(seen_versions)
        if stamp and (first is None or stamp < first):
            first = stamp

    return SurfaceDay(
        surface=surface,
        day=day,
        retyped=frozenset(retyped),
        absent_by_ticker=absent,
        lacked_by_ticker=lacked,
        unreadable=tuple(unreadable),
        versions=frozenset(versions),
        first_cycle=first,
        tickers=tuple(tickers),
    )


def _read_partition(partition, surface: str):
    """The whole partition at the surface's full schema.

    ``journal.routed_columns`` refuses a narrower read, though only once it has something to
        look for. Its first gate returns on an all-null overflow before touching another column,
        so a pruned batch from a healthy partition answers ``()``. On a batch whose overflow is
        populated, which is the only kind that reaches here, it asks every column in
        ``extra_paths`` for its null count: measured against a batch pruned to ``extra`` and
        ``row_kind`` it raises ``KeyError: 'Field "occ_symbol" does not exist in schema'``.

        So the saving is in not reaching this function, which the overflow gate above does on
        every partition the lake holds today.
    """
    try:
        return pq.read_table(partition.path)
    except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
        raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc


def _read_overflow(partition):
    """The three columns the first-cycle pass reads, and not the other seventy.

    ``_read_partition`` exists because ``journal.routed_columns`` refuses a narrow batch.
    Nothing here goes through that function: the question is which rows carry a key inside
    their own overflow, which ``extra``, ``snap_ts`` and ``row_kind`` answer between them.
    Reading the whole partition again for it is a second full read of a 307 MB file to look
    at three columns of it.
    """
    try:
        return pq.read_table(partition.path, columns=[EXTRA_COLUMN, "snap_ts", ROW_KIND_COLUMN])
    except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
        raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc


def _null_counts_by_read(partition, surface: str, columns: Sequence[str]) -> dict[str, int]:
    """Null counts for the columns the footer could not answer, by reading those columns."""
    if not columns:
        return {}
    try:
        table = pq.read_table(partition.path, columns=list(columns))
    except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
        raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc
    return {name: table.column(name).null_count for name in table.column_names}


def _data_rows(partition, surface: str, table):
    """The partition's data rows alone, as an Arrow table.

    Read off the table when the overflow gate already opened one, and off three columns
    otherwise. **The filter and the aggregates stay in Arrow**, because a chains partition
    is millions of rows and ``to_pylist`` on one is not a fraction of the read: measured on
    SPY 2026-09-16, reading ``row_kind`` costs 35 ms and materialising it as a Python list
    costs a further 150 ms, against 780 ms for the whole partition. Three columns pulled
    that way cost about 0.6 s, which is most of a full read to answer three cheap
    questions.
    """
    if table is None:
        try:
            table = pq.read_table(
                partition.path, columns=["snap_ts", ROW_KIND_COLUMN, "schema_version"]
            )
        except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
            raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc
    return table.filter(pc.equal(table.column(ROW_KIND_COLUMN), ROW_KIND_DATA))


def _stamps_and_versions(data) -> tuple[str | None, set[int]]:
    """The earliest ``snap_ts`` and every ``schema_version`` among a table's data rows."""
    if data.num_rows == 0:
        return None, set()
    earliest = pc.min(data.column("snap_ts")).as_py()
    column = data.column("schema_version").combine_chunks()
    versions = {value.as_py() for value in pc.unique(column)}
    versions.discard(None)
    return earliest, versions


def first_cycle_of(partition, surface: str, field: str, table=None) -> str | None:
    """The earliest data ``snap_ts`` whose overflow carries ``field``'s vendor key.

    ``journal.routed_columns`` refuses this on purpose. Its docstring says it answers which
    column drifted "and deliberately not how many rows carried it or what the value was",
    so the design's "first cycle that saw it" is a row-level question the signature will
    not take and this check owes a pass of its own.
    """
    path = journal.extra_paths(surface).get(field)
    if path is None:
        return None
    if table is None:
        table = _read_overflow(partition)
    # Only the rows that carry an overflow can carry the key, and on a drifting partition
    # that is a fraction of the day. Filtering in Arrow first keeps ``to_pylist`` off every
    # row whose overflow is null, which is the ordinary row even here.
    populated = table.filter(pc.is_valid(table.column(EXTRA_COLUMN)))
    overflows = populated.column(EXTRA_COLUMN).to_pylist()
    stamps = populated.column("snap_ts").to_pylist()
    kinds = populated.column(ROW_KIND_COLUMN).to_pylist()
    earliest: str | None = None
    for raw, stamp, kind in zip(overflows, stamps, kinds, strict=True):
        if raw is None or stamp is None or kind != ROW_KIND_DATA:
            continue
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(decoded, Mapping) or _lookup(decoded, path) is None:
            continue
        if earliest is None or stamp < earliest:
            earliest = stamp
    return earliest


def eastern(stamp: str | None) -> str | None:
    """A stored ``snap_ts`` rendered in ET, which is what a page body owes its reader.

    A stamp this cannot parse is passed through rather than dropped. The alternative
    renders a drift the check did find as a page with no time on it, and a reader meeting
    a UTC string is better served than one meeting nothing.
    """
    if stamp is None:
        return None
    try:
        return datetime.fromisoformat(stamp).astimezone(MARKET_TZ).isoformat()
    except ValueError:
        return stamp


def rotation_dropped(ledger: SchemaVersionLedger | None, surface: str, version: int | None) -> set:
    """The surface's columns ``version`` never carried, which are not a vendor's doing.

    ``docs/design.md`` says the battery's check "runs over the vendor payload and over
    sealed partitions, and after the seal the evidence is gone". Read in place that is a
    claim about compaction: a column this project's own release rotation dropped is filled
    with nulls at the merge, and nothing afterwards tells that from a vendor that stopped
    sending the field. So a disappearance under a version whose schema never named the
    column is compaction's fact.

    ``RecordedVersion.has_column``'s own docstring states the distinction it decides: "A
    null under a column this returns false for was never observed, rather than observed as
    null."

    An empty answer here means the guard suppresses nothing, which is tonight's case and
    also the case where the ledger cannot speak. :func:`judge_day` is what refuses to page
    the missing half when the ledger cannot speak, rather than this returning a set that
    would silently mean the same as a healthy one.
    """
    if ledger is None or version is None:
        return set()
    recorded = ledger.get(version)
    if recorded is None:
        return set()
    return {
        column
        for column in journal.extra_paths(surface)
        if not recorded.has_column(surface, column)
    }


def read_ledger(lake_root) -> SchemaVersionLedger | None:
    """The schema-version ledger, or ``None`` when it cannot be read.

    ``None`` is not "no rotation happened". It is "this run cannot tell a rotation from a
        vendor drop", and :func:`judge_day` answers it by reporting the missing half rather
        than paging it.

        This is the absent or unparseable ledger alone. A ledger that reads but records no shape
        for the running version is the other refusal, which :func:`judge_day` takes on
        ``ledger.get(version)``, and that one is the shape marketlake PR #493 shipped a page for:
        the running version was 2 while the ledger held version 1 alone, until 2026-09-17.
    """
    try:
        return SchemaVersionLedger.read(ledger_path(lake_root))
    except (SchemaVersionsError, OSError):
        return None


@dataclass(frozen=True)
class DriftReport:
    """One night's answer: what to page, what to report, and what it could not read."""

    findings: tuple[DriftFinding, ...] = ()
    report: tuple[str, ...] = ()

    @property
    def paged_titles(self) -> tuple[str, ...]:
        """Each finding's title, which is what a caller records as having been sent."""
        return tuple(finding.title for finding in self.findings)


def judge_day(
    lake_root,
    partitions: Iterable,
    *,
    day: date,
    baseline: date | None,
    baseline_partitions: Mapping[str, Sequence] | None = None,
    surfaces: Sequence[str],
) -> DriftReport:
    """Compare one day's payload shape against the previous judged day's, per surface.

    ``baseline`` is the previous day the walk found sealed partitions for, or ``None``
    when it found none. A day with no baseline reports and pages nothing, because with no
    comparison there is no way to separate a field that stopped arriving from one that
    never arrived, which is the false-positive class #265 measured.

    **The retype half is judged first and the missing half subtracts it.** A vendor retype
    that reaches every row nulls the column on all of them, so the two halves would both
    fire on one fact. The docstring of ``journal.routed_columns`` says which reading wins.

    **A partition's failure costs that partition and not the surface.** That is
    ``battery.trailing_medians``'s containment and its reason, "a session the run was not
    asked about is not this check's to announce", applied at the same grain. Contained per
    surface instead, one unreadable file on one ticker takes the whole surface's alarm down,
    so a genuine vendor drop on every other ticker goes unpaged on the night a file was also
    corrupt. The skipped partition leaves the fold, which the shared-roster comparison in
    :func:`newly_absent` already handles, and its reason reaches the nightly report.
    """
    ledger = read_ledger(lake_root)
    by_surface: dict[str, list] = {surface: [] for surface in surfaces}
    for partition in partitions:
        if partition.surface in by_surface:
            by_surface[partition.surface].append(partition)

    findings: list[DriftFinding] = []
    report: list[str] = []
    judged_surfaces: list[str] = []

    for surface in surfaces:
        today_parts = by_surface[surface]
        if not today_parts:
            continue
        today = read_surface_day(today_parts, surface, day)
        report.extend(
            f"battery: schema drift on {surface} skipped a partition: {line}"
            for line in today.unreadable
        )
        if not today.judged:
            continue

        if baseline is None or baseline_partitions is None:
            report.append(
                f"battery: schema drift on {surface} {day.isoformat()}: "
                "insufficient_history, no earlier sealed day to compare"
            )
            continue
        before = read_surface_day(baseline_partitions.get(surface, ()), surface, baseline)
        report.extend(
            f"battery: schema drift on {surface} skipped a baseline partition: {line}"
            for line in before.unreadable
        )
        if not before.judged:
            report.append(
                f"battery: schema drift on {surface} {day.isoformat()}: "
                f"insufficient_history, {baseline.isoformat()} carried no data row"
            )
            continue

        judged_surfaces.append(f"{surface} against {baseline.isoformat()}")
        newly_retyped = sorted(today.retyped - before.retyped)
        if newly_retyped:
            stamp = _retype_first_cycle(today_parts, surface, newly_retyped)
            findings.append(
                DriftFinding(
                    surface=surface,
                    day=day,
                    kind=RETYPED,
                    fields=tuple(newly_retyped),
                    first_cycle=eastern(stamp),
                )
            )

        # The missing half's candidates, with the retype half's claim subtracted. A column
        # both halves would name is a retype, per ``routed_columns``'s own reading.
        candidates = newly_absent(today, before) - today.retyped
        if not candidates:
            continue
        if ledger is None or len(today.versions) != 1:
            report.append(
                f"battery: schema drift on {surface} {day.isoformat()}: "
                f"insufficient_history, {_version_reason(ledger, today.versions)}, so a "
                "rotation cannot be told from a vendor drop"
            )
            continue
        version = next(iter(today.versions))
        if ledger.get(version) is None:
            report.append(
                f"battery: schema drift on {surface} {day.isoformat()}: "
                f"insufficient_history, the ledger records no shape for schema_version "
                f"{version}, so a rotation cannot be told from a vendor drop"
            )
            continue
        dropped = rotation_dropped(ledger, surface, version)
        gone = sorted(candidates - dropped)
        if not gone:
            continue
        findings.append(
            DriftFinding(
                surface=surface,
                day=day,
                kind=MISSING,
                fields=tuple(gone),
                first_cycle=eastern(today.first_cycle),
            )
        )

    report.extend(finding.line for finding in findings)
    # **The check says it ran, even on the night it finds nothing.** This is
    # ``coverage_line``'s rule on a second check: its correct answer against a healthy lake is
    # that nothing drifted, and silence cannot be told from a check that did not run. The line
    # names the comparison rather than a count, because the count on a healthy night is zero on
    # every axis and the comparison is what a reader needs to trust the zero.
    #
    # It is a line on ``report`` and never a count on ``BatteryReport``. ``battery.render``
    # pins the rule that every count prints including the zeroes, so a count here would be a
    # change to that function and to ``sweep.SweepOutcome.render``, and marketlake #477 holds
    # both.
    if judged_surfaces:
        moved = len(findings)
        verdict = f"{moved} drifted" if moved else "nothing drifted"
        report.append(f"battery: schema drift judged {', '.join(judged_surfaces)}, {verdict}")
    return DriftReport(findings=tuple(findings), report=tuple(report))


def newly_absent(today: SurfaceDay, before: SurfaceDay) -> set[str]:
    """Fields absent on every shared ticker today and arriving on one of them before.

    **The two days are compared over the tickers they share.** A ticker judged on one day
    and not the other says nothing about a transition, because there is no before-and-after
    for it, and letting it into either side is what turns a ticker going quiet into a page
    about the vendor. The live shape: a field already absent on SPY on both days, with QQQ
    carrying it yesterday and all-gap today, is absent on every ticker judged today and was
    not absent on every ticker judged yesterday, so a straight subtraction calls it newly
    missing when nothing about it changed.

    Measured against the lake, the two tickers agree exactly: chains has no all-null vendor
    column on either day and quotes has the same three, so today's roster cannot produce
    that page. It becomes reachable the day a ticker with a different payload shape joins.
    """
    shared = set(today.absent_by_ticker) & set(before.absent_by_ticker)
    if not shared:
        return set()
    absent_now = set.intersection(*(set(today.absent_by_ticker[t]) for t in shared))
    # **A column the baseline did not carry was never arriving.** ``read_surface_day``
    # leaves a column the partition lacks out of ``absent``, which is right on today's side
    # and inverts on the baseline's: a column that did not exist yesterday then reads as one
    # the vendor was sending. A version that *adds* a vendor column the vendor has not begun
    # filling is exactly that shape, and it pages on the new version's first night. That is
    # the #265 false-positive class the missing half was restated to avoid, reached from the
    # other direction, and the rotation guard cannot see it because it asks only what
    # today's version dropped.
    not_arriving = set.intersection(
        *(
            set(before.absent_by_ticker[ticker]) | set(before.lacked_by_ticker.get(ticker, ()))
            for ticker in shared
        )
    )
    return absent_now - not_arriving


def _version_reason(ledger: SchemaVersionLedger | None, versions: frozenset[int]) -> str:
    """Why the rotation guard cannot speak, in the words the report line needs."""
    if ledger is None:
        return "the schema-version ledger did not read"
    if not versions:
        return "the day's rows carry no schema_version"
    return f"the day's rows span schema_versions {', '.join(str(v) for v in sorted(versions))}"


def _retype_first_cycle(partitions: Sequence, surface: str, fields: Sequence[str]) -> str | None:
    """The earliest cycle any of ``fields`` was seen routing, across the day's tickers.

    One stamp for the page rather than one per field, because the page is one per surface
    per half and a body carrying a stamp per field would spend the cap the field names
    already share.
    """
    earliest: str | None = None
    for partition in partitions:
        table = None
        for field_name in fields:
            if table is None:
                try:
                    table = _read_overflow(partition)
                except DriftUnreadable:
                    # **A partition this cannot read must not cost the page.** The drift is
                    # already established by the evidence gathered above, and the first cycle
                    # is a detail of the body. ``read_surface_day`` has already reported the
                    # partition as skipped, so the failure is on the record rather than
                    # swallowed, and a page naming the drift with no stamp beats no page.
                    break
            stamp = first_cycle_of(partition, surface, field_name, table=table)
            if stamp is not None and (earliest is None or stamp < earliest):
                earliest = stamp
    return earliest


def body(finding: DriftFinding) -> str:
    """The page's text: what happened, the fields up to the cap, and when it started.

    The cap is what keeps the widest drift from being the one that does not arrive, and
    the count is what separates one moved field from a wholesale rotation.
    """
    shown = list(finding.fields[:PAGE_FIELD_CAP])
    more = len(finding.fields) - len(shown)
    named = ", ".join(shown) + (f" and {more} more" if more else "")
    if finding.kind == RETYPED:
        lead = (
            f"{len(finding.fields)} {finding.surface} field(s) arrived at a type the column "
            "refused, so the column is null and the raw value is in extra"
        )
    else:
        lead = (
            f"{len(finding.fields)} {finding.surface} field(s) stopped arriving: null on "
            "every data row, and not on the previous judged day"
        )
    seen = f" First cycle {finding.first_cycle} ET." if finding.first_cycle else ""
    return f"{lead}. {finding.day.isoformat()}: {named}.{seen}"


def page(
    publisher: Publisher | None, findings: Sequence[DriftFinding], *, now: datetime
) -> tuple[str, ...]:
    """Send one page per finding, and return the titles that went.

    One page per surface per half is what the fold already bounded the night to, so this
    sends at most four and never one per field or per ticker.

    The finding reaches stderr as well as the phone, which is what ``schema_drift.page``,
    ``compact._page_drift`` and ``battery.page_delayed_feed`` all already do. The one
    exception is a refused page: the publisher found one of its own secrets in the body
    and redacted its record for that reason, so stderr must not undo the redaction.
    """
    if publisher is None or not findings:
        return ()
    sent: list[str] = []
    for finding in findings:
        text = body(finding)
        delivery = publisher.publish(
            Message(event=SCHEMA_DRIFT_EVENT, title=finding.title, body=text), now=now
        )
        sent.append(finding.title)
        if delivery.reason == REFUSED:
            print("battery: schema-drift page refused: it carried a secret", file=sys.stderr)
            continue
        print(f"battery: {finding.title}: {text}", file=sys.stderr)
        if not delivery.sent:
            kept = "written down" if delivery.recorded else "lost"
            print(
                f"battery: schema-drift page not sent: {delivery.reason}, {kept}",
                file=sys.stderr,
            )
    return tuple(sent)


__all__ = [
    "BASELINE_LOOKBACK_DAYS",
    "MISSING",
    "PAGE_FIELD_CAP",
    "RETYPED",
    "SCHEMA_DRIFT_EVENT",
    "DriftFinding",
    "DriftReport",
    "DriftUnreadable",
    "SurfaceDay",
    "body",
    "eastern",
    "first_cycle_of",
    "judge_day",
    "newly_absent",
    "page",
    "read_ledger",
    "read_surface_day",
    "rotation_dropped",
]
