"""The ``reports/`` tree: report-tier findings, written down the minute they happen.

The design gives report-tier findings no message of their own. They ride the nightly
report, one dated file per night under ``reports/`` in the lake root, which sits inside
the backup sync root and outside the manifest. D16 writes that file and D20 renders it,
so the reader arrives later than the producers do.

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
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

from lake.calendar import MARKET_TZ
from lake.close_guard import GuardOutcome
from lake.paths import DATE_PREFIX, REPORTS_DIR

# The close+5 guard's own subdirectory under ``reports/``. Its own, and deliberately not
# ``alerts/``, per rule 2 above.
CLOSE_GUARD_DIR = "close_guard"


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
    exception, like ``quotes/XYZ: 2 unreadable``, has only two fields and survives whole.
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
    "close_guard_dir",
    "write_close_guard",
]
