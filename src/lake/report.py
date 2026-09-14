"""The ``reports/`` tree: report-tier findings, written down the minute they happen.

The design gives report-tier findings no message of their own. They ride the nightly
report, one dated file per night under ``reports/`` in the lake root, which sits inside
the backup sync root and outside the manifest. D16 writes that file and D20 renders it,
so the reader arrives later than the producers do. Compaction's finding is the exception,
and the schema policy is what makes it one. A missing or retyped known field pages, so
compaction pages once per run on top of filing here.

The close+5 guard is one of those producers and it has been finding things with nowhere
to put them. Three of the design's rules for it end in "flags the nightly report", and
its findings reached exactly one destination: a print to stderr that launchd files under
``com.marketlake.daemon.err.log``, which nothing in this repo reads. Ten minutes later
compaction seals the day, and the startup walk skips a date whose partition is
manifested, so the loss is permanent. Completeness is counted from rows and never
inferred from holes, and a finding nobody can read is a hole.

So this module owns the tree and each producer writes into it as it runs. That is one
file per producer run rather than one assembled document, because the daemon cannot know
at 16:20 what the sweep will want to say at 20:00, and a finding held in memory until
then is a finding a restart loses.

Three rules hold for everything written here, and each has a failure behind it.

1. **A file, never a ledger, and never a row.** The scrub's reverse pass asks every file
   under the lake root for a manifest entry, and its exclusion set is enumerated rather
   than implied: ``{manifest.jsonl, journal/, reports/}``. So a file under ``reports/``
   is already covered by name and a manifest entry for one would turn a report into a
   checksum failure on the following Sunday. A report is not a measurement.
2. **Each producer gets its own subdirectory.** ``reports/alerts/`` is the publisher's,
   and ``alert.undelivered`` counts the files in it as pages that never reached the
   phone. The Now panel shows that count as ``pages_failed_to_send``. A guard finding
   filed there would inflate a page-failure count with things that are not pages, so the
   guard files beside it rather than in it.
3. **Write-once, named by stamp and pid.** A slot's stamp carries no sub-minute part, so
   the name carries microseconds and the writing process's id instead. That is enough for
   a producer the dispatcher serves once per day, and a restart that serves the day again
   writes under a new pid. ``alert._record`` needs more and adds a per-message sequence,
   because one cycle can raise several pages at one instant.

The second producer is compaction's merge. It compares a ticker-day's merged segments to
the pinned schema at the one moment the segments still exist, and files what moved. A
merge the segments' own types refused files here too, from the sweep rather than from the
seal, because a refusal writes no partition and so never reaches that comparison. The file
has forensic value from the day it lands, because the merged schema is gone the moment the
seal unlinks the segments, and it has no reader until D20 renders it. What
reaches a human in the meantime is compaction's own page, which folds the run's findings
into one message and sends the reader here for the per-ticker-day detail.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from lake.calendar import MARKET_TZ
from lake.close_guard import GuardOutcome
from lake.paths import DATE_PREFIX, REPORTS_DIR

# The close+5 guard's own subdirectory under ``reports/``. Its own, and deliberately not
# ``alerts/``, per rule 2 above.
CLOSE_GUARD_DIR = "close_guard"

# Compaction's merge-time schema check. Its own subdirectory, per rule 2 above, so a
# drifted ticker-day never counts as a page that failed to send.
SCHEMA_DRIFT_DIR = "schema_drift"


@dataclass(frozen=True)
class SchemaDrift:
    """One ticker-day whose merged segments do not carry the pinned schema.

    ``GuardOutcome`` lives in ``close_guard`` and this record lives here, and the
    asymmetry is the import direction. The guard's producer never learns about this
    module, because the daemon wires the two together. Compaction's producers are inside
    ``compact``, so it imports this module directly and a record defined there would close
    the loop. There are two of them. One sits in ``_seal`` and reports a merged schema that
    is not the pinned one. The other sits in the sweep and reports a merge the segments'
    own types refused. The dataclass carries strings and dates alone, which keeps
    pyarrow out of the module that writes JSON.

    The three difference fields say what moved, each naming columns rather than counting
    them, because a human reading the file wants the column.

    ``unexpected`` is a merged column the pinned schema has no place for, and it is the
    field that catches the shape nothing else can. A daemon that restarts mid-session
    onto code that *dropped* a column leaves the day's earlier segments carrying it and
    its later ones without it, and the merge fills the gap with nulls. Compaction runs
    the code that dropped it, so the column the merged table carries is one the pinned
    schema no longer names, and that mismatch is the whole of the evidence. After the
    seal there is none: the sealed partition holds the column with nulls on the
    post-rotation rows, which reads exactly like a vendor that stopped sending it.

    ``missing`` is a pinned column no segment carried, the same accident with the code
    versions the other way round. It has no repair either, because the values were never
    written, but it does survive the seal, since the partition carries one fewer column
    than the schema forever.

    ``retyped`` carries both kinds of retype, rendered ``name: before -> after`` either
    way, and which kind it is follows from which producer filed the record. A retype every
    segment agreed on merges cleanly and disagrees with the pinned schema, so the merged
    producer renders it ``pinned -> merged``. A retype the segments disagree on stops the
    merge instead, so the refusal's producer renders it ``earlier -> later`` across the
    segments and the pinned schema is not a party to it. That second one used to reach
    nothing at all, because the refusal raised and ended the run.

    All three can be empty, and each producer has a way of getting there. The merged
    producer decides there is a difference by comparing the two schemas outright and these
    fields explain it, so a difference the names and the types do not show files a record
    that lists nothing. A nullability change is the difference that reaches it. The
    refusal's producer scans the segments to explain a refusal Arrow already made, so a
    refusal it cannot model lists nothing either. Both are still the finding: something was
    wrong at the merge and this says which ticker-day to go and look at.
    """

    surface: str
    ticker: str
    day: date
    partition: str
    schema_version: int
    missing: tuple[str, ...] = ()
    unexpected: tuple[str, ...] = ()
    retyped: tuple[str, ...] = ()
    segments: tuple[str, ...] = field(default_factory=tuple)
    refused: bool = False


def close_guard_dir(lake_root: Path | str, day: date) -> Path:
    """Where one session day's close+5 findings are filed.

    The key is the day the guard ran *for*, not the instant it wrote. The two agree on
    an ordinary day, because close+5 is 16:20 Eastern and the guard is dispatched on
    that minute. They stop agreeing on a restart, and the day the findings are about is
    the one a reader asking "what happened on the 2nd" wants.
    """
    return Path(lake_root) / REPORTS_DIR / CLOSE_GUARD_DIR / f"{DATE_PREFIX}{day.isoformat()}"


def write_close_guard(
    lake_root: Path | str,
    outcome: GuardOutcome,
    *,
    now: datetime,
    pid: int | None = None,
) -> Path:
    """File one close+5 run's outcome, and hand back the path it landed at.

    **A clean run writes a file too.** ``GuardOutcome.reportable`` is false on a day
    where both closes landed, and writing only on findings would make an absent file
    ambiguous between "the guard ran and found nothing" and "the guard never ran". Those
    are the two readings this lake refuses to confuse everywhere else, so the file says
    which it was and the price is one small JSON per session day.

    **Raises rather than swallowing.** The caller is the daemon's close+5 dispatch, and
    that dispatch already wraps every session-relative job so a failure costs the job and
    never the loop. Catching here would hide the failure from the one place that reports
    it. The guard's own run is finished by the time this is called, so a write that fails
    costs the file and never a marker.
    """
    pid = os.getpid() if pid is None else pid
    eastern = now.astimezone(MARKET_TZ)
    entry = {
        "at": eastern.isoformat(),
        "day": outcome.day.isoformat(),
        "reportable": outcome.reportable,
        "filled": list(outcome.filled),
        "unobserved": list(outcome.unobserved),
        "baseline_less": list(outcome.baseline_less),
        "shortfalls": list(outcome.shortfalls),
        "refused": list(outcome.refused),
        "problems": [_redacted(problem) for problem in outcome.problems],
    }
    # `parents=True` from a missing lake root would create the lake itself. The Sunday
    # job decides whether to ping on `root.is_dir()` and re-reads that on every retry, so
    # a writer that conjured the root would turn "lake root missing" into a green check
    # on the following attempt. A report is written inside a lake that exists, or not at
    # all. `alert._record` refuses on the same test for the same reason.
    root = Path(lake_root)
    if not root.is_dir():
        raise FileNotFoundError(f"lake root missing: {root}")
    directory = close_guard_dir(root, outcome.day)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{eastern.strftime('%H%M%S%f')}-{pid}.json"
    with open(path, "x", encoding="utf-8") as handle:
        json.dump(entry, handle, sort_keys=True)
        handle.write("\n")
    return path


def schema_drift_dir(lake_root: Path | str, day: date) -> Path:
    """Where one session day's merge-time schema findings are filed.

    Keyed on the ticker-day the merge was sealing, not the instant compaction ran. Those
    differ by hours on a swept date a failed earlier run left behind, and the day a
    reader asking "what happened on the 2nd" wants is the day the rows belong to.
    """
    return Path(lake_root) / REPORTS_DIR / SCHEMA_DRIFT_DIR / f"{DATE_PREFIX}{day.isoformat()}"


def write_schema_drift(
    lake_root: Path | str,
    drift: SchemaDrift,
    *,
    now: datetime,
    pid: int | None = None,
) -> Path:
    """File one drifted ticker-day, and hand back the path it landed at.

    **A clean merge writes nothing.** The close+5 guard files on every run, because it
    runs once a day and an absent file would be ambiguous between "found nothing" and
    "never ran". Compaction has no such ambiguity to resolve. It seals hundreds of
    ticker-days a run and appends a manifest entry for each, so the manifest already
    says which ticker-days were merged. A file per ticker-day per run would be hundreds
    of empty findings a day, and the reader would have to filter them all back out.

    **A refused merge writes on every run.** That exemption rests on the manifest entry,
    and a ticker-day whose merge was refused has none. So its silence on the second night
    would be consistent with three things at once: the conflict was fixed, it is still
    there and was already filed, or the ticker-day is gone. The caller files that finding
    every run the conflict survives for exactly that reason. Nothing here changes either
    way. This writer is called once per finding whichever caller reached it.

    **Raises rather than swallowing.** The caller contains it, because a raise out of the
    filing would cost the rest of the sweep whichever caller reached it, and the
    containment belongs where that blast radius is, not here. Hiding the failure inside
    the writer would take it away from every caller, including a test that wants to see a
    write fail.

    The name carries the surface and the ticker as well as the stamp and the pid. One
    sweep can find drift on several ticker-days, and the stamps that separate them are
    microseconds apart, so the name says which finding it is without opening it.
    """
    pid = os.getpid() if pid is None else pid
    eastern = now.astimezone(MARKET_TZ)
    entry = {
        "at": eastern.isoformat(),
        "day": drift.day.isoformat(),
        "surface": drift.surface,
        "ticker": drift.ticker,
        "partition": drift.partition,
        "schema_version": drift.schema_version,
        "missing": list(drift.missing),
        "unexpected": list(drift.unexpected),
        "retyped": list(drift.retyped),
        "segments": list(drift.segments),
        "refused": drift.refused,
    }
    # A report is written inside a lake that exists, or not at all. The same rule as
    # ``write_close_guard`` above, and for the same reason: `parents=True` from a missing
    # root would create the lake itself and turn "lake root missing" into a green check.
    root = Path(lake_root)
    if not root.is_dir():
        raise FileNotFoundError(f"lake root missing: {root}")
    directory = schema_drift_dir(root, drift.day)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = eastern.strftime("%H%M%S%f")
    path = directory / f"{stamp}-{drift.surface}-{drift.ticker}-{pid}.json"
    with open(path, "x", encoding="utf-8") as handle:
        json.dump(entry, handle, sort_keys=True)
        handle.write("\n")
    return path


def _redacted(problem: str) -> str:
    """One of the guard's problems, with any exception message dropped.

    The guard composes a problem as a place, then what went wrong there, as in
    ``quotes/XYZ: KeyError: 'close_tag'``. The place is a surface and a ticker and the
    middle is an exception class, and both are safe. The tail is whatever the exception
    chose to say, and an ``OSError`` says the filename it failed on, which is an absolute
    path on the capture machine. This file sits in the directories the dashboard may
    read, so the path stops here. ``alert._record`` redacts for the same reason, and
    stderr keeps the fuller string for a reader who has the log.

    Dropping everything past the second field is what does it. A problem naming no
    exception, like ``quotes/XYZ: 2 unreadable (1 drifted, 1 corrupt)``, has only two fields
    and survives whole, breakout included.
    The rule can only ever shorten a problem, so a shape it was not written for loses
    detail rather than leaking it.
    """
    where, separator, rest = problem.partition(": ")
    if not separator:
        return problem
    kind, _, _ = rest.partition(": ")
    return f"{where}: {kind}"


__all__ = [
    "CLOSE_GUARD_DIR",
    "SCHEMA_DRIFT_DIR",
    "SchemaDrift",
    "close_guard_dir",
    "schema_drift_dir",
    "write_close_guard",
    "write_schema_drift",
]
