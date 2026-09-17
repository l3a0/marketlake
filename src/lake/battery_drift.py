"""The battery's own nightly schema-drift page: a day's sealed partitions, compared.

The design's message table gives schema drift four producers and this is the last to
ship. The parser's fires the minute a response is parsed, off the batch it just built.
The close+5 fill's is the same producer through ``observe_partial``, for the one ticker
it refetched. Compaction's compares a ticker-day's merged segment schema against the
pinned one, at the merge, which is the last moment the segments exist. This one reads
sealed partitions at night, and it is the only one of the four that sees a whole day at
once. Marketlake #427 is the issue, and it exists because #138's split distributed the
schema-drift check across five sub-issues and this page was in none of them.

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
``insufficient_history`` and the design says it "fails open and says so".

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
# every body at plain text under 1,000 bytes. Measured, a body naming every field of a
# whole-surface drift runs to 792 bytes on chains and 1,048 on quotes, so the widest drift
# is the one that would not reach the phone. At this cap the same two bodies are 231 and
# 221 bytes.
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

    ``absent`` is an intersection. A field still arriving on one ticker is a field the
    vendor is still sending, so a fold that unioned would page for a ticker whose own
    payload was short rather than for a vendor change. ``schema_drift`` counts its
    evidence per ticker for the mirror of this reason.

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
    absent: frozenset[str]
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

        The message table writes it ``Schema drift: <field> missing``, or ``retyped``, from
        a time when the page was one per field. The fold across fields is arithmetic the
        cap forced, so the placeholder cannot always hold one name. One field still reads
        exactly as the table writes it, and several say how many rather than picking one.
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

    ``unheld`` is a column the partition does not carry at all. Reading it raises, and
    counting it as wholly null would page for the shape it cannot be about: a column a
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
    absent: set[str] | None = None
    versions: set[int] = set()
    first: str | None = None
    tickers: list[str] = []

    for partition in partitions:
        try:
            footer = _footer_null_counts(partition.path, [*columns, EXTRA_COLUMN, ROW_KIND_COLUMN])
        except OSError as exc:
            raise DriftUnreadable(f"{partition.relative} did not open: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
            raise DriftUnreadable(f"{partition.relative} has no readable footer: {exc}") from exc

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
            table = _read_partition(partition, surface)
            try:
                retyped.update(journal.routed_columns(surface, table.to_batches()[0]))
            except KeyError as exc:
                # ``routed_columns`` asks every column in ``extra_paths`` for its null
                # count, so a partition short of one raises rather than answering. A
                # partition that does not carry the running schema's columns is
                # compaction's fact, per ``docs/design.md``'s schema policy, and it is
                # reported rather than guessed at.
                raise DriftUnreadable(
                    f"{partition.relative} does not carry the running schema: {exc}"
                ) from exc

        data_rows = _data_row_count(partition, surface, table)
        if data_rows == 0:
            continue
        tickers.append(partition.ticker)

        # A column the footer carries but could not measure is read outright. Reading the
        # column alone is cheap next to the whole partition, and it keeps an absent
        # statistic from reading as an absent field.
        resolved = dict(footer.counts)
        unmeasured = footer.unmeasured & set(columns)
        if unmeasured:
            resolved.update(_null_counts_by_read(partition, surface, sorted(unmeasured)))

        here = {
            column
            for column in columns
            if column not in footer.absent_column and resolved.get(column) == rows
        }
        absent = here if absent is None else (absent & here)

        stamps, seen_versions = _stamps_and_versions(partition, surface, table)
        versions.update(seen_versions)
        if stamps and (first is None or stamps < first):
            first = stamps

    return SurfaceDay(
        surface=surface,
        day=day,
        retyped=frozenset(retyped),
        absent=frozenset(absent or ()),
        versions=frozenset(versions),
        first_cycle=first,
        tickers=tuple(tickers),
    )


def _read_partition(partition, surface: str):
    """The whole partition at the surface's full schema.

    ``journal.routed_columns`` refuses a narrower read. Measured against a batch pruned to
    ``extra`` and ``row_kind`` it raises ``KeyError: 'Field "occ_symbol" does not exist in
    schema'``, because it asks every column in ``extra_paths`` for its null count. So the
    saving is in not reaching this function, which the overflow gate above does on every
    partition the lake holds today.
    """
    try:
        return pq.read_table(partition.path)
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


def _data_row_count(partition, surface: str, table) -> int:
    """How many data rows the partition carries.

    Read off the table when the overflow gate already opened one, and off the
    ``row_kind`` column alone otherwise. The column is one string per row against the
    surface's seventy-odd, so the ordinary night pays a fraction of a partition to answer
    the question that keeps an all-gap day from reading as a wholesale disappearance.
    """
    if table is None:
        try:
            table = pq.read_table(partition.path, columns=[ROW_KIND_COLUMN])
        except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
            raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc
    kinds = table.column(ROW_KIND_COLUMN).to_pylist()
    return sum(1 for kind in kinds if kind == ROW_KIND_DATA)


def _stamps_and_versions(partition, surface: str, table) -> tuple[str | None, set[int]]:
    """The partition's earliest data ``snap_ts`` and every ``schema_version`` it carries."""
    if table is None:
        try:
            table = pq.read_table(
                partition.path, columns=["snap_ts", ROW_KIND_COLUMN, "schema_version"]
            )
        except Exception as exc:  # noqa: BLE001 - pyarrow raises several unrelated types
            raise DriftUnreadable(f"{partition.relative} did not read: {exc}") from exc
    kinds = table.column(ROW_KIND_COLUMN).to_pylist()
    stamps = table.column("snap_ts").to_pylist()
    versions = table.column("schema_version").to_pylist()
    seen = {version for version, kind in zip(versions, kinds, strict=True) if kind == ROW_KIND_DATA}
    seen.discard(None)
    data_stamps = [
        stamp
        for stamp, kind in zip(stamps, kinds, strict=True)
        if kind == ROW_KIND_DATA and stamp is not None
    ]
    return (min(data_stamps) if data_stamps else None), seen


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
        table = _read_partition(partition, surface)
    overflows = table.column(EXTRA_COLUMN).to_pylist()
    stamps = table.column("snap_ts").to_pylist()
    kinds = table.column(ROW_KIND_COLUMN).to_pylist()
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
    than paging it. Marketlake #493 is why the unreadable case is not hypothetical: the
    running version was 2 and the ledger held version 1 alone until 2026-09-17.
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

    **A surface's failure costs that surface and not the night.** ``DriftUnreadable`` is
    contained per surface, for ``battery.trailing_medians``'s reason: a session the run was
    not asked about is not this check's to announce.
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
        try:
            today = read_surface_day(today_parts, surface, day)
        except DriftUnreadable as exc:
            report.append(f"battery: schema drift on {surface} not judged: {exc}")
            continue
        if not today.judged:
            continue

        if baseline is None or baseline_partitions is None:
            report.append(
                f"battery: schema drift on {surface} {day.isoformat()}: "
                "insufficient_history, no earlier sealed day to compare"
            )
            continue
        try:
            before = read_surface_day(baseline_partitions.get(surface, ()), surface, baseline)
        except DriftUnreadable as exc:
            report.append(f"battery: schema drift on {surface} not judged: {exc}")
            continue
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
        candidates = (today.absent - before.absent) - today.retyped
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
    # change to that function and to ``sweep.Nightly.render``, and marketlake #477 holds both.
    if judged_surfaces:
        moved = len(findings)
        verdict = f"{moved} drifted" if moved else "nothing drifted"
        report.append(f"battery: schema drift judged {', '.join(judged_surfaces)}, {verdict}")
    return DriftReport(findings=tuple(findings), report=tuple(report))


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
                table = _read_partition(partition, surface)
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
    "page",
    "read_ledger",
    "read_surface_day",
    "rotation_dropped",
]
