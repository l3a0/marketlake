"""The split walk's checkpoint, and the one function both of its callers run the walk through.

Marketlake #755 trims old chains partitions from the hosted VM, and ``splits.detect_splits``
cannot survive that on its own. It rebuilds each ticker's state from the first manifested
chains day, and with that day trimmed a root returning after the gap reads as a gain. A
fixture at ``5c7d083`` landed a permanent phantom ``split`` that way, and ``ex_date`` sits in
the corporate-actions ledger's key, so the wrong line is never superseded. Marketlake #783 let
the walk report each ticker's :class:`~lake.splits.WalkState` at a cutoff and resume from it.
This module, marketlake #786, saves those states and decides when a ticker resumes.

**The checkpoint** is one Parquet file, ``reference/split_checkpoint.parquet``, one row per
ticker. It carries the session day of the sweep that wrote it, each ticker's state, and each
ticker's own security-master mappings cut at its cutoff. The 18:30 sweep writes it under the
window key, ``lake_window_sessions``, and marketlake #787's trim reads it through
:func:`read_checkpoint` to drop only partitions at or before each ticker's cutoff. It is
replaced by rename and recorded in the manifest under the lake-root lock, the way the
reference tables are, so the bucket keeps every version a repair may need.

**When a ticker resumes.** A ticker resumes from its saved state only when it has a designed
absence: a chains partition that is gone from disk and that ``trimmed.is_designed_absence``
says was trimmed on purpose. A ledger that merely names the ticker is not enough, or a lake
restored to whole would resume from a frozen checkpoint forever. Every other ticker walks from
scratch, as it always did, which is exact and lets its cutoff move freely. That covers a window
just enabled, a trim that never dropped anything, and the laptop, which never trims.

**When a ticker is refused.** A walk from scratch over a trimmed lake lands phantom splits, so a
ticker that cannot safely resume is not walked at all. It reports no state, so the checkpoint
keeps its saved one, and the other tickers run. Seven conditions refuse one, and each refusal's
text names its repair, because the sweep's problem line is that text word for word:

1. The checkpoint cannot be read.
2. The checkpoint holds no entry for the ticker.
3. A designed absence falls after its cutoff, so a resume would skip a day it never read.
4. A chains partition at or before its cutoff is now withheld under a quarantine verdict.
5. Its own master mappings, cut at its cutoff, differ from the ones the checkpoint recorded.
   That covers an instrument changed at the checkpoint's ``previous`` day, and a back-dated
   scope edit that puts a day already read out of scope. ``onboard`` accepts a back-dated
   ``--capture-start``, which is how a master edit reaches a day at or before a cutoff. A
   mapping a rename closes or opens after the cutoff cuts to the same rows.
6. Another ticker symbol names its instrument on overlapping days. The two share one run's
   ``emitted`` keys and can stop at different cutoffs, so a resume and a whole-lake walk would
   disagree. Only a hand-corrupted master reaches this.
7. A read meets ``PartitionAbsent`` on a designed absence, which a trim landing while the walk
   runs produces. The walk asks :func:`_absent_refusal` there.

A trimmed ledger that cannot be read refuses the whole walk rather than one ticker. Without it
no ticker can be told to resume or to walk from scratch.

**What the next checkpoint holds.** Tonight's states replace the saved ones, ticker by ticker.
A ticker refused tonight, or resumed with no new day, keeps its saved entry, so a ticker refused
for one night is not refused for ever. A ticker walked from scratch that stopped before its
first day saves :func:`starting_state`, whose cutoff covers no day, because a saved cutoff
would otherwise outlive the skip or the held finding that now stops the walk before it. The
count of entries only grows, which is what lets the manifest's row-count guard stand. One night
is different: a checkpoint that could not be read and that a refused ticker still needs is left
in place for the repair, and nothing is written over it.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lake.actions import read_master
from lake.calendar import Calendar
from lake.clock import Clock
from lake.journal import CHAINS_SCHEMA
from lake.manifest import (
    ManifestError,
    is_quarantined,
    latest_entries,
    latest_quarantine,
    record_partition,
)
from lake.occ_mapping import SymbolHistory
from lake.paths import (
    CHAINS,
    REFERENCE_DIR,
    SPLIT_CHECKPOINT,
    LakePaths,
    parse_partition_rel,
    temp_write_path,
)
from lake.reference_table import VERSION_COLUMN, read_reference_table
from lake.security_master import ID_TYPE_TICKER, SecurityMaster
from lake.splits import CHAINS_COLUMNS, Session, SplitReport, WalkState, detect_splits
from lake.trimmed import is_designed_absence, latest_trimmed

# The checkpoint's manifest key, and the version stamped on every row of it.
CHECKPOINT_PARTITION = f"{REFERENCE_DIR}/{SPLIT_CHECKPOINT}.parquet"
CHECKPOINT_SCHEMA_VERSION = 1

# What every refusal that a restore can repair says to do. A range restore writes a restore
# line over each trim line, so the ticker has no designed absence left and walks from scratch,
# which is exact. Restoring an earlier checkpoint version and only the days after its cutoff
# costs less disk. ``docs/design.md`` describes both. The sentence holds no ``": "``, because
# ``report.redacted`` keeps a problem line only up to its second one.
REPAIR = (
    "Restore its trimmed chains days with python -m lake.bucket restore-range so it walks "
    "from scratch, or put back an earlier checkpoint from the bucket with the days after its "
    "cutoff, as docs/design.md describes."
)

# One row of a saved session: the root it carries, then the twelve chains columns the walk
# reads, at the types the capture schema pins, so a value comes back as the type it was read.
_ROW_TYPE = pa.struct(
    [pa.field("root", pa.string())]
    + [pa.field(name, CHAINS_SCHEMA.field(name).type) for name in CHAINS_COLUMNS]
)
_HISTORY_TYPE = pa.struct([("ssid", pa.int64()), ("symbol", pa.string()), ("first", pa.date32())])
_MAPPING_TYPE = pa.struct(
    [
        ("instrument_id", pa.int64()),
        ("id_type", pa.string()),
        ("valid_from", pa.date32()),
        ("valid_to", pa.date32()),
    ]
)
CHECKPOINT_SCHEMA = pa.schema(
    [
        (VERSION_COLUMN, pa.int64()),
        ("ticker", pa.string()),
        ("session_day", pa.date32()),
        ("cutoff", pa.date32()),
        ("last_day", pa.date32()),
        ("unread_since", pa.int64()),
        ("seen", pa.list_(pa.string())),
        ("history", pa.list_(_HISTORY_TYPE)),
        ("previous_day", pa.date32()),
        ("previous_instrument_id", pa.int64()),
        ("previous_roots", pa.list_(pa.string())),
        ("previous_rows", pa.list_(_ROW_TYPE)),
        ("previous_strikes", pa.list_(pa.float64())),
        ("previous_spot", pa.float64()),
        ("mappings", pa.list_(_MAPPING_TYPE)),
    ]
)

# One of a ticker's own master mappings cut at its cutoff:
# ``(instrument_id, id_type, valid_from, valid_to)``.
MappingCut = tuple[int, str, date, date | None]


class CheckpointError(ManifestError):
    """Base class for every reason the checkpoint cannot be read.

    It is a ``ManifestError``, so ``sweep._LEDGER_REFUSALS`` and ``actions.main``'s ledger arm
    catch it with no new entry. ``ArrowInvalid`` is a ``ValueError`` and would otherwise
    escape the sweep and cost the night's report and ping.
    """


class CheckpointUnreadable(CheckpointError):
    """The checkpoint file exists and is not a checkpoint this code can read."""

    def __init__(self, path: Path, reason: str = "is not readable parquet") -> None:
        super().__init__(f"the split checkpoint at {path} {reason}")
        self.path = path
        self.reason = reason


@dataclass(frozen=True)
class CheckpointEntry:
    """One ticker's saved walk state, and its own master mappings cut at the state's cutoff."""

    state: WalkState
    mappings: tuple[MappingCut, ...]


@dataclass(frozen=True)
class Checkpoint:
    """What one sweep saved: the session it ran for, and one entry per ticker.

    ``session_day`` is what lets marketlake #787 trim only on a session strictly after it, so a
    holiday compaction or a hand-run one after 18:30 cannot trim by a stale checkpoint.
    """

    session_day: date
    entries: tuple[CheckpointEntry, ...]

    def entry(self, ticker: str) -> CheckpointEntry | None:
        """The ticker's saved entry, or ``None`` when the checkpoint holds none."""
        for entry in self.entries:
            if entry.state.ticker == ticker:
                return entry
        return None

    def cutoffs(self) -> dict[str, date]:
        """Each ticker's cutoff. The trim drops a partition only at or before its ticker's."""
        return {entry.state.ticker: entry.state.cutoff for entry in self.entries}


@dataclass(frozen=True)
class SplitWalk:
    """One run of the walk, and the checkpoint entries it leaves for the sweep to write.

    ``entries`` are tonight's states over the saved entries, ticker by ticker. ``blocked`` is
    set when nothing may be written tonight, because a ticker that needed the unreadable file
    on disk was refused. ``replaces_unreadable`` says the file on disk could not be read and no
    ticker needed it, so the write replaces it without the row-count guard, whose recorded
    count belongs to a file nobody can read.
    """

    report: SplitReport
    entries: tuple[CheckpointEntry, ...]
    blocked: bool = False
    replaces_unreadable: bool = False


# -- the master ----------------------------------------------------------------


def mappings_at(master: SecurityMaster, ticker: str, cutoff: date) -> tuple[MappingCut, ...]:
    """The ticker's own master mappings as they read on every day at or before ``cutoff``.

    A mapping that opens after the cutoff is left out, and an end after the cutoff is cut to
    open, because neither changes how any day the saved state read resolves. Every kind of
    identifier spelled like the ticker is kept, because ``SecurityMaster.resolve`` matches on
    the spelling whatever the kind.
    """
    cut = []
    for mapping in master.mappings:
        if mapping.id_value != ticker or mapping.valid_from > cutoff:
            continue
        ends = mapping.valid_to
        if ends is not None and ends > cutoff:
            ends = None
        cut.append((mapping.instrument_id, mapping.id_type, mapping.valid_from, ends))
    return tuple(sorted(cut, key=lambda row: (row[0], row[1], row[2], row[3] or date.max)))


def shared_instrument(master: SecurityMaster, ticker: str) -> tuple[int, str] | None:
    """An instrument the ticker names that another ticker symbol names on overlapping days.

    ``register`` and ``remap`` cannot write this, because a rename closes one mapping on the
    day the next opens. Only a hand-corrupted master holds it. The first such pair is returned
    as ``(instrument_id, other_ticker)``.
    """
    own = [mapping for mapping in master.mappings if mapping.id_value == ticker]
    for mine in own:
        for other in master.mappings:
            if (
                other.id_type != ID_TYPE_TICKER
                or other.id_value == ticker
                or other.instrument_id != mine.instrument_id
            ):
                continue
            mine_ends = date.max if mine.valid_to is None else mine.valid_to
            other_ends = date.max if other.valid_to is None else other.valid_to
            if mine.valid_from < other_ends and other.valid_from < mine_ends:
                return mine.instrument_id, other.id_value
    return None


# -- reading and writing the file ------------------------------------------------


def checkpoint_path(lake_root: Path | str) -> Path:
    """The checkpoint's path for a lake, derived from its root."""
    return LakePaths(Path(lake_root)).split_checkpoint_path


def read_checkpoint(lake_root: Path | str) -> Checkpoint | None:
    """The saved checkpoint, ``None`` when no file exists, or :class:`CheckpointUnreadable`.

    Every failure to read a file that exists is a ``CheckpointUnreadable``, an access failure
    included. ``reference_table.read_reference_table`` lets an ``OSError`` through on purpose,
    and pyarrow reports most corruption as one, so this folds it here. A walk that cannot
    tell a refused open from a damaged file still cannot resume either way.
    """
    path = checkpoint_path(lake_root)
    try:
        return read_reference_table(
            path,
            schema=CHECKPOINT_SCHEMA,
            version=CHECKPOINT_SCHEMA_VERSION,
            build=partial(_from_table, path),
            unreadable=CheckpointUnreadable,
            base=CheckpointError,
            unsupported=lambda found: CheckpointUnreadable(
                path, f"is version {found}, which this code does not read"
            ),
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CheckpointUnreadable(path, f"cannot be read, {type(exc).__name__}") from exc


def write_checkpoint(
    lake_root: Path | str,
    checkpoint: Checkpoint,
    *,
    recorded_at: datetime,
    guard: bool = True,
) -> dict:
    """Replace the checkpoint by rename and record its manifest entry, under the lake-root lock.

    The steps are ``SecurityMaster.write``'s: a temp file named by ``paths.temp_write_path``,
    a write, an fsync, then ``os.replace``, so a reader sees the whole old file or the whole new
    one. The entry's ``rows`` is the count of tickers. ``guard`` passes through to
    ``record_partition``, and only the replacement of an unreadable file turns it off.
    """
    if not checkpoint.entries:
        raise ValueError("a checkpoint holds at least one ticker")
    from lake.lock import lake_lock
    from lake.onboard import REFERENCE_SOURCE

    root = Path(lake_root)
    path = checkpoint_path(root)
    table = _to_table(checkpoint)
    with lake_lock(root):
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = temp_write_path(path, os.getpid())
        try:
            pq.write_table(table, temp)
            descriptor = os.open(temp, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temp, path)
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
        return record_partition(
            root,
            CHECKPOINT_PARTITION,
            source=REFERENCE_SOURCE,
            rows=len(checkpoint.entries),
            fetched_at=recorded_at.isoformat(),
            guard=guard,
        )


def _to_table(checkpoint: Checkpoint) -> pa.Table:
    columns: dict[str, list] = {name: [] for name in CHECKPOINT_SCHEMA.names}
    for entry in sorted(checkpoint.entries, key=lambda item: item.state.ticker):
        state = entry.state
        previous = state.previous
        values = {
            VERSION_COLUMN: CHECKPOINT_SCHEMA_VERSION,
            "ticker": state.ticker,
            "session_day": checkpoint.session_day,
            "cutoff": state.cutoff,
            "last_day": state.last_day,
            "unread_since": state.unread_since,
            "seen": sorted(state.seen),
            "history": [
                {"ssid": ssid, "symbol": symbol, "first": first}
                for ssid, symbol, first in state.history
            ],
            "previous_day": None if previous is None else previous.day,
            "previous_instrument_id": None if previous is None else previous.instrument_id,
            "previous_roots": None if previous is None else sorted(previous.roots),
            "previous_rows": (
                None if previous is None else [{"root": root, **row} for root, row in previous.rows]
            ),
            "previous_strikes": None if previous is None else sorted(previous.strikes),
            "previous_spot": None if previous is None else previous.spot,
            "mappings": [
                {
                    "instrument_id": instrument_id,
                    "id_type": id_type,
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                }
                for instrument_id, id_type, valid_from, valid_to in entry.mappings
            ],
        }
        for name in CHECKPOINT_SCHEMA.names:
            columns[name].append(values[name])
    return pa.table(columns, schema=CHECKPOINT_SCHEMA)


def _from_table(path: Path, table: pa.Table) -> Checkpoint:
    rows = table.select(CHECKPOINT_SCHEMA.names).to_pylist()
    if not rows:
        raise CheckpointUnreadable(path, "holds no ticker")
    days = {row["session_day"] for row in rows}
    if len(days) != 1 or None in days:
        raise CheckpointUnreadable(path, "does not name one session day on every row")
    entries = []
    seen_tickers: set[str] = set()
    for row in rows:
        ticker = row["ticker"]
        if ticker is None or row["cutoff"] is None or row["unread_since"] is None:
            raise CheckpointUnreadable(path, "has a row missing its ticker, cutoff or count")
        if ticker in seen_tickers:
            raise CheckpointUnreadable(path, f"names {ticker} twice")
        seen_tickers.add(ticker)
        previous = None
        if row["previous_day"] is not None:
            previous = Session(
                day=row["previous_day"],
                instrument_id=row["previous_instrument_id"],
                roots=frozenset(row["previous_roots"] or ()),
                rows=tuple(_saved_row(saved) for saved in row["previous_rows"] or ()),
                strikes=frozenset(row["previous_strikes"] or ()),
                spot=row["previous_spot"],
            )
        history = tuple(
            (item["ssid"], item["symbol"], item["first"]) for item in row["history"] or ()
        )
        # The walk rebuilds the history with ``SymbolHistory.from_export``, which raises a
        # ``ValueError`` on a contract named twice. Asked here, it is a checkpoint this code
        # cannot read, rather than an exception that escapes the sweep and costs its record.
        try:
            SymbolHistory.from_export(history)
        except ValueError:
            history = None
        if history is None:
            raise CheckpointUnreadable(path, f"names one contract twice in {ticker}'s history")
        state = WalkState(
            ticker=ticker,
            cutoff=row["cutoff"],
            previous=previous,
            seen=frozenset(row["seen"] or ()),
            history=history,
            unread_since=row["unread_since"],
            last_day=row["last_day"],
        )
        mappings = tuple(
            (item["instrument_id"], item["id_type"], item["valid_from"], item["valid_to"])
            for item in row["mappings"] or ()
        )
        entries.append(CheckpointEntry(state=state, mappings=mappings))
    (session_day,) = days
    return Checkpoint(session_day=session_day, entries=tuple(entries))


def _saved_row(saved: Mapping[str, object]) -> tuple[str, dict[str, object]]:
    row = dict(saved)
    root = row.pop("root")
    return root, row


# -- deciding who resumes ----------------------------------------------------------


def _designed_absences(
    root: Path, manifest: Mapping[str, Mapping], trimmed: Mapping[str, Mapping]
) -> dict[str, list[date]]:
    """Each ticker's chains days that are gone from disk on purpose, in date order.

    Only a partition the ledger's latest line trims is asked about, and only once its file is
    found absent, which is the contract ``trimmed.is_designed_absence`` states. A trim line
    beside a file still present, which a crash between the line and the unlink leaves, is a
    present file, and its ticker walks from scratch over it.
    """
    found: dict[str, list[date]] = {}
    for partition in trimmed:
        reference = parse_partition_rel(partition)
        if reference is None or reference.surface != CHAINS:
            continue
        if not is_designed_absence(partition, manifest, trimmed):
            continue
        if not _absent(root / partition):
            continue
        found.setdefault(reference.ticker, []).append(reference.day)
    return {ticker: sorted(days) for ticker, days in found.items()}


def _absent(path: Path) -> bool:
    """Whether nothing exists at ``path``. A path that cannot be checked raises ``OSError``."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return True
    return False


def _chains_days(manifest: Mapping[str, Mapping]) -> dict[str, list[tuple[date, str]]]:
    """Each ticker's manifested chains days with their partition keys, trimmed ones included."""
    days: dict[str, list[tuple[date, str]]] = {}
    for partition in manifest:
        reference = parse_partition_rel(partition)
        if reference is None or reference.surface != CHAINS:
            continue
        days.setdefault(reference.ticker, []).append((reference.day, partition))
    return {ticker: sorted(found) for ticker, found in days.items()}


def _resume_refusal(
    ticker: str,
    trimmed_days: list[date],
    *,
    entry: CheckpointEntry | None,
    unreadable: CheckpointUnreadable | None,
    master: SecurityMaster,
    chains_days: list[tuple[date, str]],
    quarantine: Mapping[str, dict],
) -> str | None:
    """Why a ticker with a designed absence cannot resume, or ``None`` when it can.

    The module docstring lists the conditions. Each text holds no ``": "``, so the sweep's
    report file keeps it whole, and each names its repair.
    """
    if unreadable is not None:
        return (
            f"its chains days are trimmed and the split checkpoint cannot be read "
            f"({type(unreadable).__name__}), so it cannot resume. {REPAIR}"
        )
    if entry is None:
        return (
            "its chains days are trimmed and the split checkpoint holds no entry for it, so "
            f"it cannot resume. {REPAIR}"
        )
    cutoff = entry.state.cutoff
    later = [day for day in trimmed_days if day > cutoff]
    if later:
        return (
            f"chains day {later[0].isoformat()} was trimmed after its checkpoint cutoff "
            f"{cutoff.isoformat()}, so a resume would skip a day it never read. {REPAIR}"
        )
    for day, partition in chains_days:
        if day > cutoff:
            break
        if is_quarantined(quarantine.get(partition)):
            return (
                f"chains day {day.isoformat()}, at or before its checkpoint cutoff "
                f"{cutoff.isoformat()}, is now withheld by a quarantine verdict, so its saved "
                "state read a day the lake no longer trusts. Sign the day off with python -m "
                f"lake.signoff if it is sound. Otherwise {REPAIR}"
            )
    if mappings_at(master, ticker, cutoff) != entry.mappings:
        return (
            f"the security master's mappings for it on or before its checkpoint cutoff "
            f"{cutoff.isoformat()} changed after the checkpoint was written, so its saved state "
            f"was read under a different master. {REPAIR}"
        )
    shared = shared_instrument(master, ticker)
    if shared is not None:
        instrument_id, other = shared
        return (
            f"instrument {instrument_id} is also named by {other} on overlapping days, which "
            "only a corrupt security master holds, so a resume would judge it apart from a "
            f"whole-lake walk. Repair the master, then {REPAIR[0].lower()}{REPAIR[1:]}"
        )
    return None


def _absent_refusal(root: Path, ticker: str, day: date) -> str | None:
    """Refuse a ticker whose read met a designed absence, or ``None`` for an ordinary absence.

    The walk asks this only when a read meets ``PartitionAbsent``, so it is rare and reads the
    ledgers afresh each time. That is what catches a trim landing while the walk runs, which
    the ledger read at the start cannot see.
    """
    from lake.battery import partition_key

    partition = partition_key(CHAINS, ticker, day)
    try:
        manifest = latest_entries(root)
        trimmed = latest_trimmed(root)
    except ManifestError as exc:
        return (
            f"chains day {day.isoformat()} is absent and the trimmed ledger or the manifest "
            f"cannot be read ({type(exc).__name__}), so whether it was trimmed on purpose is "
            "unknown. Repair the ledger by hand under the lake-root lock, or restore it from "
            "the backup."
        )
    if is_designed_absence(partition, manifest, trimmed):
        return (
            f"chains day {day.isoformat()} was trimmed while the walk ran, so walking on from "
            "scratch would land phantom splits. Run the walk again once the trim has finished, "
            "and it resumes from the checkpoint."
        )
    return None


# -- the walk ----------------------------------------------------------------------


def walk_splits(
    *,
    lake_root: Path | str,
    clock: Clock,
    calendar: Calendar,
    edge: date | None = None,
) -> SplitWalk:
    """Run the split walk, resuming each trimmed ticker from the checkpoint or refusing it.

    Both callers come through here: the 18:30 sweep, with tonight's window edge when the window
    key is set, and ``splits.detect_splits_from_config``, the hand command, with none. So a
    hand run on a trimmed lake resumes or refuses exactly as the sweep does, and never walks a
    trimmed ticker from scratch. Only the sweep writes the result, since only it has a session
    day to record.

    The master is read before the walk, and the mappings saved beside each state come from that
    read. A master edited while the walk runs then reads as changed at the next resume, which
    refuses rather than resuming past an edit the state never saw.

    A trimmed ledger, a manifest or a quarantine ledger that cannot be read raises its
    ``ManifestError``, which refuses the whole walk.
    """
    root = Path(lake_root)
    master = read_master(root)
    manifest = latest_entries(root)
    trimmed = latest_trimmed(root)
    designed = _designed_absences(root, manifest, trimmed)

    saved: Checkpoint | None = None
    unreadable: CheckpointUnreadable | None = None
    try:
        saved = read_checkpoint(root)
    except CheckpointUnreadable as exc:
        unreadable = exc
        # The class reaches the report file. The whole message, path included, stays here.
        print(f"splits: {exc}", file=sys.stderr)

    resume: list[WalkState] = []
    refused: dict[str, str] = {}
    if designed:
        chains_days = _chains_days(manifest)
        quarantine = latest_quarantine(root)
        for ticker, days in sorted(designed.items()):
            entry = None if saved is None else saved.entry(ticker)
            reason = _resume_refusal(
                ticker,
                days,
                entry=entry,
                unreadable=unreadable,
                master=master,
                chains_days=chains_days.get(ticker, []),
                quarantine=quarantine,
            )
            if reason is not None:
                refused[ticker] = reason
            elif entry is not None:
                resume.append(entry.state)

    report = detect_splits(
        lake_root=root,
        clock=clock,
        calendar=calendar,
        resume=resume,
        edge=edge,
        refused=refused,
        absent_refusal=partial(_absent_refusal, root),
    )
    blocked = unreadable is not None and bool(refused)
    # A ticker walked from scratch that stopped before its first day reports no state, and the
    # saved entry would then outlive the reason it stopped. #787 would trim through a day the
    # walk skipped as reversible, or through a held finding. So it gets the state a walk from
    # scratch starts from, which holds no day and lets the trim drop nothing. Dropping its
    # entry instead would shrink the checkpoint and trip the manifest's row-count guard.
    # A resumed ticker always reports a state, its saved one at the least, so only a refused
    # ticker is kept out here. Its saved entry stays.
    stated = {state.ticker for state in report.states}
    refused_now = {refusal.ticker for refusal in report.refused}
    starts = [
        starting_state(ticker)
        for ticker in sorted(set(_chains_days(manifest)) - refused_now - stated)
    ]
    return SplitWalk(
        report=report,
        entries=_merged(saved, [*report.states, *starts], master),
        blocked=blocked,
        replaces_unreadable=unreadable is not None and not blocked,
    )


def starting_state(ticker: str) -> WalkState:
    """The state a walk from scratch starts from, as a checkpoint entry can hold it.

    Its cutoff is ``date.min``, before every day, so the trim drops nothing for it and a resume
    from it walks every day, with ``resolved`` seeded False exactly as a walk from scratch is.
    """
    return WalkState(
        ticker=ticker,
        cutoff=date.min,
        previous=None,
        seen=frozenset(),
        history=(),
        unread_since=0,
        last_day=None,
    )


def _merged(
    saved: Checkpoint | None, states: Iterable[WalkState], master: SecurityMaster
) -> tuple[CheckpointEntry, ...]:
    """Tonight's states over the saved entries, so a ticker with no state tonight keeps its own."""
    entries = {} if saved is None else {entry.state.ticker: entry for entry in saved.entries}
    for state in states:
        entries[state.ticker] = CheckpointEntry(
            state=state, mappings=mappings_at(master, state.ticker, state.cutoff)
        )
    return tuple(entries[ticker] for ticker in sorted(entries))


__all__ = [
    "CHECKPOINT_PARTITION",
    "CHECKPOINT_SCHEMA",
    "CHECKPOINT_SCHEMA_VERSION",
    "REPAIR",
    "Checkpoint",
    "CheckpointEntry",
    "CheckpointError",
    "CheckpointUnreadable",
    "MappingCut",
    "SplitWalk",
    "checkpoint_path",
    "mappings_at",
    "read_checkpoint",
    "shared_instrument",
    "starting_state",
    "walk_splits",
    "write_checkpoint",
]
