"""The evening vendor sweep: the 18:30 weekday job, the nightly digest, and the Friday wake.

Three pieces shipped before this one and none of them had a scheduler or anywhere to report.
This is the job that runs them every weekday evening, the file that records what happened, the
message that says it in one screen, and the two alarms that were already written expecting it
to exist.

Run it with ``python -m lake.sweep``. The host's scheduler runs it at 18:30 Monday through
Friday: launchd on macOS, under ``control_plane.eod_sweep_job``, and systemd on Linux. Its
health check is the ``eod-sweep`` slug.

What one run does, in the design's own order.

1. The corporate-actions poll, so today's split flags before bars land. Two walks over sealed
   rows: ``actions.extract_dividends`` reads quotes and ``splits.detect_splits`` reads chains.
   The split walk takes the calendar as well, because whether two sealed sessions are adjacent
   is the calendar's answer and not the manifest's. Marketlake #431. It runs through
   ``split_checkpoint.walk_splits``, which resumes a ticker whose chains days were trimmed
   from the split checkpoint, and when the window key ``lake_window_sessions`` is set the run
   writes tonight's checkpoint after the walk. Marketlake #786.
2. The bar walk, ``bars.backfill_bars``. The close cross-check is inside it, and the
   walk covers every session the capture spans still hold unlanded rather than only
   the one the clock is in, because a daily bar waits for the next session to seal before
   it is fetched, and on the night of its own session that has not happened. Marketlake
   #422, with marketlake #434 turning that night's fetch into a skip and marketlake #618
   moving the check onto the session's own 16:15 quote.
3. The validation battery, ``battery.judge``, which judges the sealed chains and quotes
   partitions and writes a quarantine verdict for what fails. The design places it between the
   bar fetch and the Friday branch, which is where this list puts it.
4. The Friday branch, which sets the Sunday one-shot wake and reads it back. It runs on macOS
   only. A Linux VM never sleeps, so it sets no wake and never calls ``sudo``.
5. The ping.
6. The dated report file under ``reports/``.
7. The digest, at priority 2.

**The disk-runway check runs ahead of step 1**, right after the schema-version check and
before any walk, and on a holiday too. ``lake.runway`` computes the runway and this job is
its second consumer after the dashboard's Lake panel. Under ``runway.HEADROOM_WEEKS`` it
files an ``action`` line, and under ``runway.PAGE_FLOOR_WEEKS`` it also pages, once per run
and every night it stays that short, with no state kept between runs. Running first means
the page goes out even when a later raise loses the report file, and it puts the line at
the top of the digest, which is truncated at ``DIGEST_BYTE_CAP``. Marketlake #438.

The battery is ``lake.battery``, at step 3 above. Marketlake #406 built its spine and the
real-time entitlement check, and marketlake #407 added the other three to the same module. The
job scopes the run to the session it is about, and trading-calendar coverage is outside that
scoping on purpose: it walks the whole capture span, because a session with no partition is a
session on which nothing ran, so a check scoped to tonight could never see the night it missed.
Its census is one line in ``report`` whatever it finds.

**Its failure does not withhold the ping, and that is a decision rather than the default.** The
``eod-sweep`` row says a missed ping means the day's official bars or actions are missing, and a
battery that could not run leaves the day *unjudged* instead. That is ``_counted``'s line from
the other side: the work the check watches did happen. So the battery's own trouble rides
``report`` and its findings ride the file, while a quarantine is the run working rather than
failing and withholds nothing either.

**Why the privileged half lives here and not in the control plane.** That module's docstring
opens by refusing it: "It executes nothing privileged. No ``sudo``, no ``pmset`` write, no
``launchctl`` bootstrap, and no ``tmutil`` write runs from here." The Friday branch runs
``sudo pmset schedule``. So the control plane keeps rendering this job and reasoning about its
alarms, and it hands over the argument list through ``pmset_schedule_args`` rather than the
printed line, which carries no ``sudo`` and quotes its argument for a shell.

**When the run pings, and when it stays silent.** The design's standing rule is that every
ping fires only after the job's success condition, and that silence always means broken. The
``eod-sweep`` row says a missed ping means the day's official bars or actions are missing. So
a piece that did not finish withholds the ping, which is ``sunday_maintenance``'s shape one job
over. A held finding is the opposite: a gate refusing to land a row is the run working, and
``report.py`` already says this ping "says the run happened whether or not it found anything".
``Nightly`` keeps the two apart in the two fields ``SundayOutcome`` keeps them in, ``problems``
and ``report``, because one list would let a held finding silence the check.

**A catch-up run must not ping green.** ``RunAtLoad`` being off does not stop launchd firing a
missed 18:30 occurrence on the next wake, and ``control_plane.sunday_job`` says that coalescing
"has nothing to do with ``RunAtLoad``". Such a run fires at the 08:25 wake, against a session
whose close has not happened. On Linux the sweep's timer carries ``Persistent=true``, so a VM
that was down at 18:30 runs it once it is back up, which can be the next morning too. Every
ticker would fail the bar span check and the run would ping green while the missed evening's
bars stayed missing forever. So the bar fetch runs only once
the session's equity close has passed, and a run that skipped it for that reason stays silent.
The guard cannot refuse a real run, because the job fires at 18:30 against a 16:00 close, or
13:00 on an early close. Recovering the evening that was missed is the walk's own doing now:
marketlake #422 pointed this job at #319's span walk, so the next run that does fire reaches
every session still unlanded rather than only the one its clock sits on.

**A holiday runs none of the data work.** The schedule is Monday through Friday on both hosts, so
a non-session weekday is a holiday, and the design has "compaction and the sweep no-op on an
empty journal". The one-line digest settles it: a run whose walks found something would have
nowhere to say so. The walks read every sealed ticker-day rather than today's, or every one
after a trimmed ticker's saved cutoff, so a holiday run would re-derive yesterday's held
findings and file each one again under ``reports/withheld/`` for no new information. The run
still pings, and the Friday branch still sets the wake, which the design's pmset table states
directly. One side effect is worth naming: a holiday never builds the vendor, so it never
reads the token.

**Every seam is injected and only ``main`` builds one.** That is ``control_plane.main``'s rule
and its reason, which is that a ``main`` accepting them lets a test omit one and reach the real
effect. So :func:`sweep` requires seven and defaults none: the clock, the calendar, the vendor
source, the pinger, the publisher, the schedule reader and the schedule setter. Required
means a caller must say, not that the answer cannot be ``None``. The schedule setter and
reader may each be ``None`` on a host with no wake to set or read back, which is Linux, and
``sweep_from_config`` picks that from ``control_plane.is_macos``. A ``None`` setter skips
the Friday branch, and a ``None`` reader skips only its read-back. The whole module runs
offline in a test, with no network and no shelling out.
"""

from __future__ import annotations

import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial
from pathlib import Path

from lake import control_plane, outbox
from lake.actions import ActionsError, ExtractionReport, extract_dividends
from lake.alert import REFUSED, Message, Publisher, undelivered
from lake.bars import (
    CLOSE_VALUE_ABSENT,
    BackfillReport,
    BarsReport,
    SpansAbsent,
    StampNotAnInstant,
    UnsupportedBarFreq,
    backfill_bars,
    read_capture_spans,
)
from lake.battery import BatteryReport, judge, wrote_line
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar, NotASession
from lake.capture_spans import CaptureSpansError
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants
from lake.control_plane import (
    EOD_SWEEP_SLUG,
    PMSET_BINARY,
    AlarmCheck,
    ScheduleReader,
    check_alarms,
    expected_one_shot,
    next_sunday_wake,
    parse_pmset_schedule,
    pmset_schedule_args,
    read_pmset_schedule,
)
from lake.loader import SnapAbsent
from lake.manifest import ManifestError, is_quarantined, latest_quarantine
from lake.paths import CHAINS, QUOTES, default_token_path
from lake.report import (
    ACTION,
    BARS_PIECE,
    DIVIDENDS_PIECE,
    INFO,
    SPLITS_PIECE,
    Nightly,
    PieceOutcome,
    ReportLines,
    redacted,
    write_nightly,
)
from lake.runner import PING_FAILURES, Pinger, escalate_ping_failure
from lake.runway import Runway, assess
from lake.schema_versions import check_running_version
from lake.schwab import SchwabVendor, VendorAuthError
from lake.security_master import SecurityMasterError
from lake.session import COMPACTION_DELAY
from lake.split_checkpoint import Checkpoint, SplitWalk, walk_splits, write_checkpoint
from lake.splits import SplitReport
from lake.tickers import Roster, load_tickers
from lake.vendor import Vendor
from lake.window import EdgeNotFound, WindowRefused, window_edge
from lake.window import window_sessions as window_sessions_of

# The nightly summary's wire shape, per the design's message table. Priority 2 is the silent
# tier: it lands in the notification drawer without interrupting, which is what makes one
# message every weekday evening a liveness signal for the channel rather than a page.
NIGHTLY_EVENT = "nightly_summary"
NIGHTLY_PRIORITY = 2

# What a holiday no-op sends, verbatim from the design's message table.
HOLIDAY_BODY = "Holiday, no session"

# The disk-runway page's wire shape, per the design's message table. The title takes the
# house form for a path that can lose captured minutes: a full lake volume stops compaction
# from sealing, and the day's journal lives on the same volume. It pages at the default
# ``alert.PAGE_PRIORITY``, which ``Message`` carries without being told.
DISK_RUNWAY_EVENT = "sweep_disk_runway"
DISK_RUNWAY_TITLE = "Capture at risk: lake disk runway short"

# Where the page sends the operator, by heading rather than by URL: the runbook's step that
# grows the lake volume through a reviewed pull request.
DISK_RUNWAY_REPAIR = "infra/README.md, Rerun the bootstrap, A larger lake volume"

# Bytes in a GiB. The report line and the page print GiB because the Lake panel's
# ``bytes()`` divides by 1024, so the three surfaces print the same figure for one disk.
_GIB = 1 << 30

# The design's budget for the digest: one screen, under this many bytes. The digest carries
# counts and the report file carries the detail, which is what makes the budget hold on a
# night with many findings rather than only on the nights that have none.
DIGEST_BYTE_CAP = 1000

# Python's ``date.weekday()`` numbering. The setter runs on Friday alone.
_PY_FRIDAY = 4

# Builds the vendor. It is a thunk rather than a factory taking a token path, so a holiday
# run never calls it and so never reads the token at all. ``main`` builds the real one.
VendorSource = Callable[[], Vendor]

# Runs the Sunday one-shot ``pmset`` write. The real one shells out under ``sudo``. A test
# injects a callable, which is what keeps the suite off the machine's power schedule.
ScheduleSetter = Callable[[date], None]

# The conditions that end one walk rather than the run. Each is the same set that walk's own
# command turns into one line and an exit code, so the sweep says what the command would have
# said instead of handing the operator a stack trace in the job's error log.
#
# **``OSError`` is in both tuples although no command names it, and marketlake #435 is why.**
# Every walk opens a reference file before it walks anything, and both readers deliberately catch
# ``FileNotFoundError`` alone: ``bars.read_capture_spans`` says so in its own words, because
# reporting a permission failure as "no capture spans" would send an operator to the seeder, which
# reads the same file and fails the same way. That reasoning is right for a command, where a
# person is watching a terminal, and wrong here. Measured: ``chmod 000`` on the master makes
# ``sweep()`` raise, and the run writes no report file and sends no ping.
#
# What makes that the one failure that cannot be deferred on evidence is that it destroys the
# evidence. The escape costs the battery, the report file, the digest and the ping, and on a
# Friday the Sunday one-shot wake, so the canary and the weekly scrub do not run either. The ping
# is the alarm, and this silences the alarm and then silences the thing that would have noticed
# the alarm stopped. ``CLAUDE.md`` puts the dead-man, the watchdog, the canary and the backup
# outside its zero-count rule for exactly this shape.
#
# **What this does not cover, so the entry is not read as wider than it is.** No httpx exception
# subclasses ``OSError``, measured across its whole hierarchy, so a refused connection or a read
# timeout still escapes this. That is the likelier 18:30 failure of the two, and marketlake #450
# owns it: the fix is to wrap a transport error into ``VendorError`` at the ``SchwabVendor`` seam,
# where ``bars._walk`` already contains it per ticker-day. What ``OSError`` does catch from the
# vendor side is the arm that reaches the socket directly, such as ``ssl.SSLError`` and
# ``ConnectionResetError``, and those are caught here rather than per ticker-day, which is #446.
#
# The record surface was already built for this refusal. ``PieceOutcome.refusal_class`` reasons
# about an ``OSError`` reaching it and drops the message, "because an ``OSError`` says the
# filename it failed on, which is an absolute path on the capture machine". So the digest carries
# the class and the path stays on the job's own stdout. Nothing there needed changing; the
# refusal simply never arrived.
#
# **The price is named rather than hidden.** A refusal caught here ends the whole walk, and the
# ledger walks are ordered by ticker, so an ``OSError`` raised deep in one costs every ticker
# after it. That is the objection marketlake #352's own test records against widening this tuple,
# and it answered a per-ticker condition at the per-ticker level instead. This is a net under
# that, not a replacement for it: a reference-file read happens before any ticker is walked, so
# its blast radius is the whole walk however it is caught, and for anything raised deeper a
# refused piece is still strictly better than a lost evening. Containing the deeper ones where
# they belong is marketlake #446.
# ``ManifestError`` as the class rather than one of its members, the lesson ``_BARS_REFUSALS``
# below already writes down. Both walks read through ``lake.loader``, which resolves the
# quarantine ledger on every partition it opens and publishes a damaged one as this error. Six
# shapes reach here, three from each layer of that read. ``manifest.read_quarantine`` refuses the
# whole file, as ``manifest.TornLedger`` for a read that stopped with verdicts written behind it,
# as ``manifest.LedgerNotUtf8`` for bytes that do not decode, and as
# ``manifest.LedgerHasByteOrderMark`` for a byte-order mark, which is valid UTF-8 and so reaches
# neither of the other two.
# ``manifest.latest_quarantine_by_check`` refuses one entry above it, three ways: a line that
# parses and names no partition, one whose ``partition`` cannot be a dict key, and one whose
# ``check`` cannot be. Executed against
# `8fb1fda`, the line naming no partition already took this whole run down, and with it the
# battery, the report file, the digest, the ping and the Friday wake, on a lake whose only fault
# was one bad line in a ledger these walks do not even write. Marketlake #469 made the torn read
# likely enough to matter, since a crash mid-append needs no hand-malformed line, and marketlake
# #495 added the bytes that do not decode, which until then escaped this tuple as a
# ``ValueError``. Marketlake #506 added the byte-order mark, which escaped both: it decodes, so
# it is not #495, and on a one-entry ledger it read as a torn tail and lifted the quarantine
# rather than refusing at all.
#
# **The one shape this tuple could not reach was closed at the raise rather than by widening
# here, and marketlake #514 is that change.** An entry whose ``partition`` cannot be a dict key
# raised a bare ``TypeError`` out of ``manifest.latest_quarantine_by_check``, which is neither a
# ``ManifestError`` nor an ``OSError``, so it escaped this tuple the way the non-decodable bytes
# did before marketlake #495. Executed against `ba348f3`, a ledger holding ``{"partition": []}``
# took this whole run down. It is now a ``ManifestError`` and lands here with its five siblings.
# So the six above are what reaches this tuple rather than every way the ledger can be damaged.
#
# **This answers ``manifest._latest_by_partition``'s own sentence rather than ignoring it.**
# That docstring says raising is safe because the two callers that must survive it already
# catch it, and that "every other caller is a place where stopping is correct". These walks
# are a third surviving caller, and the difference is what stopping costs here. There it is one
# read that answers wrongly. Here it is every later step of the 18:30 job, none of which reads
# a ledger at all. The walk still stops: it is refused, named in ``problems``, and it withholds
# the ping, which is what the ``eod-sweep`` row already means by a missed ping. What it no
# longer does is take the battery and the Friday wake down with it.
#
# **A damaged *manifest* ledger still stops this run, one step later.** Marketlake #514 gave
# ``manifest._latest_by_partition`` the same guard, so the two walks above are contained for it
# too, through ``actions.surface_ticker_days``. The bars backfill below is not: ``lake.bars``
# resolves the same ledger through ``manifest.latest_entries`` and ``_BARS_REFUSALS`` carries no
# ``ManifestError``, so the run dies there before the report file is written and the walks'
# recorded refusal reaches nobody. The claim above is therefore redeemed for the quarantine
# ledger and not for the manifest. Marketlake #517 owns that tuple entry. It is not marketlake
# #447, whose three pieces are detecting a torn fragment, reporting it through the Sunday scrub
# and correcting ``append_line``'s docstring, and which has never carried this.
#
# **``ActionsError`` and ``SecurityMasterError`` as the classes, and marketlake #497 is why.**
# This tuple used to name ``MasterAbsent`` and ``MasterUnreadable``, one member of each family,
# beside two entries that already named their class. Two siblings reach here and both are raised
# by a read that runs before any ticker is walked, so both took the whole 18:30 job down.
# ``UnsupportedSchemaVersion`` comes out of ``SecurityMaster.read`` through ``from_table``, which
# ``actions.read_master`` does not fold because it keeps its ``FileNotFoundError`` arm narrow on
# purpose. ``LedgerLineError`` comes out of ``entry_key``, reached through ``actions.latest``,
# which both walks call once for the run to compare every ticker-day against one snapshot.
# Executed against `9e767ab`, one ledger line naming no ``type`` raised out of ``sweep()`` and
# the run wrote no report file and sent no ping.
#
# **This does not widen a blast radius, which is the objection marketlake #446 owns.** The
# members that are per-ticker are already contained per-ticker, in both walks:
# ``extract_dividends`` catches ``(UnresolvedSymbol, AmbiguousSymbol)`` and ``detect_splits``
# catches them at both of its ``resolve_instrument`` call sites. ``occ_mapping.write_mappings``'s
# caller draws a related line one level down, re-raising ``MasterAbsent`` and ``MasterUnreadable``
# and containing ``SecurityMasterError`` per boundary. So what reaches this tuple is by
# construction the kind whose blast radius is the whole walk however it is caught. That site
# sorts by member rather than by class, which is marketlake #509 rather than this change.
#
# **The price is named rather than hidden.** Naming ``ActionsError`` means a future
# ``UnresolvedSymbol`` escaping that per-ticker catch would be caught here and cost every ticker
# after it its dividends, where today it would end the run. The cost is the blast radius and not
# the reporting: a refusal writes the report file and withholds the ping, where an escape writes
# no file and sends nothing, so this path says strictly more than the one it replaces. What is
# lost is the exit code and the eleven tickers. That is the standing cost of naming a class
# instead of listing members, and this tuple already pays it for ``ManifestError`` while the one
# below pays it for ``CaptureSpansError``.
#
# The members are replaced rather than joined. ``except`` treats every entry identically, so a
# subclass beside its base buys nothing, and a redundant name reads as though it did.
#
# **One family still escapes this tuple and it is not one of these.** ``SchemaVersionsError`` is
# marketlake #494. It is named so this entry is not read as covering every way a ledger can fail.
#
# A ledger byte that is not UTF-8 used to escape here too, as a ``ValueError``, and no longer
# does. Marketlake #495 closed that for the quarantine ledger and #499 for the manifest and the
# corporate-actions ledger, each refusing as its own module's class, so ``ManifestError`` and
# ``ActionsError`` above already carry them. What that does not buy is the run: the bar walk
# below reads the manifest too and ``_BARS_REFUSALS`` names no ``ManifestError``, so a damaged
# manifest ledger is collected here and still ends the job there. Marketlake #517 is that half.
#
# The level below sorts these same two families by member rather than by class, at
# ``splits.py``'s mapping write, so a sibling lands in the per-boundary arm the re-raise exists
# to keep it out of. That is marketlake #509 and it is not fixed here, because it decides
# whether a condition ends a walk rather than what this job records.
_LEDGER_REFUSALS = (ActionsError, SecurityMasterError, ManifestError, OSError)
# ``CaptureSpansError`` as the class rather than one of its members, which is the lesson
# ``bars.main`` already wrote down for itself: naming ``SpansUnreadable`` alone left its sibling
# ``UnsupportedSpansSchemaVersion`` reaching the operator as a stack, and a spans file from a
# newer version of this code is the one shape of it a person actually meets. The 18:30 job reads
# that file for the first time under marketlake #422, so it inherits the lesson rather than
# rediscovering it. Escaping here would cost the battery, the report file, the digest, the ping
# and, on a Friday, the Sunday wake.
#
# **``SecurityMasterError`` here is the same hole as the tuple above, and marketlake #497 is
# still why.** ``lake.bars`` has its own ``_read_master`` folding ``FileNotFoundError`` alone, so
# ``UnsupportedSchemaVersion`` escaped this tuple too. Measured: widening the tuple above alone
# left a master stamped with a version this code does not read still taking the whole run down,
# from the bar walk rather than the ledger walks. Fixing one tuple would have been this file
# repeating its own lesson one tuple over.
#
# **``ActionsError`` here changes nothing today, and it is named for uniformity rather than for
# a failure.** The only member of that family this walk can raise is ``MasterAbsent``, which the
# tuple already carried. ``lake.bars`` never resolves the actions ledger, so ``LedgerLineError``
# cannot arise, and ``UnresolvedSymbol`` is contained per ticker-day beside ``AmbiguousSymbol``
# at the resolve. Measured under #497: replacing this entry with ``MasterAbsent`` leaves the
# suite green, and that is an equivalence rather than a gap. It is written here because the
# alternative is one tuple naming a class and its sibling naming a member, which reads as an
# oversight, and because a walk that later reads an adjusted view would meet the ledger through
# ``loader._in_view``.
#
# ``SpansAbsent`` stays beside ``CaptureSpansError`` because it is a ``BarsError`` and the class
# beside it does not cover it.
_BARS_REFUSALS = (
    ActionsError,
    SecurityMasterError,
    SpansAbsent,
    CaptureSpansError,
    StampNotAnInstant,
    UnsupportedBarFreq,
    NotASession,
    VendorAuthError,
    OSError,
)

# The ``bars abandoned`` reasons that say the sealed partition holds no usable close. The
# segments it was built from are gone, so nothing can change either one, and a line naming
# only these is ``INFO``. ``bars.GateSkip`` carries the reason as the class-shaped token, which
# ``bars`` calls "what separates a quarantine somebody can sign off from a gap nothing can
# rebuild". The other three want a human. ``PartitionAbsent`` is a manifested partition gone
# from disk, which wants a restore, ``PartitionQuarantined`` a sign-off, and ``PartialRead`` a
# schema change (marketlake #530). A chains or quotes partition comes back from the bucket
# through ``python -m lake.bucket restore-range`` (marketlake #784).
#
# ``SnapAbsent`` took ``NoSpotClose``'s place when marketlake #618 moved the gate's reference
# onto the session's own 16:15 row, read by minute. A gap row at that minute is the same
# permanent absence the old read met at 16:00, and the live lake's six abandoned ticker-days
# carry it. Without it here the census line would turn to ``ACTION`` on the first night.
_PERMANENT_ABANDON_REASONS = frozenset({SnapAbsent.__name__, CLOSE_VALUE_ABSENT})


def set_sunday_wake(sunday: date) -> None:
    """The real setter: ``sudo -n /usr/bin/pmset schedule wakeorpoweron "<date> 19:55:00"``.

    ``-n`` is what makes this safe inside a LaunchDaemon. A daemon has no terminal, so a
    missing or drifted sudoers drop-in would otherwise leave the job waiting on a password
    nobody can type. With ``-n`` sudo refuses instead and the run reports it, which withholds
    the ping, and that is the only way an unset one-shot is ever found: by Sunday evening a
    wake that fired and one that was never set look the same.

    The binary is named by the full path the drop-in grants. The three arguments come from
    ``control_plane.pmset_schedule_args``, so the string ``sudo`` joins them into is the one
    that module's anchored rule matches.
    """
    subprocess.run(
        ["sudo", "-n", PMSET_BINARY, *pmset_schedule_args(sunday)],
        check=True,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )


def count_gaps(lake_root: Path | str, day: date) -> int | None:
    """How many minutes the day's sealed capture surfaces record as missed, or ``None``.

    ``None`` means no partition for that day exists, which is a different answer from zero and
    has to stay one. Compaction seals at close+15, an hour and a half before this job runs, so
    an absent partition at 18:30 says the seal did not happen rather than that the day was
    clean. Reporting zero would read as a perfect day.

    ``row_kind`` is read straight from Parquet rather than through the loader, and the reason
    is that this is a count of a column rather than a read of rows for meaning. The loader
    returns one snapshot per ticker-day, which is the wrong shape for a whole day's marker
    rows, and it exposes no count. The read is cheap for the same reason it is narrow: the
    column is dictionary-encoded and nothing else is touched. Counting both of the lake's
    chain partitions for 2026-09-16, 9,915,616 rows, took 0.09 seconds.

    A quarantined partition is counted like any other. Quarantine is a verdict about whether
    data can be trusted, and this is a count of minutes capture missed, which a later verdict
    does not change. ``lake.battery`` runs earlier in this same invocation, so the condition can
    now arise on a night the battery quarantines something, and the answer is unchanged by it.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from lake.journal import ROW_KIND_COLUMN, ROW_KIND_GAP

    root = Path(lake_root)
    total = 0
    found = False
    for surface in (CHAINS, QUOTES):
        for partition in sorted((root / surface).glob(f"ticker=*/date={day.isoformat()}.parquet")):
            found = True
            table = pq.read_table(partition, columns=[ROW_KIND_COLUMN])
            column = table[ROW_KIND_COLUMN]
            total += pc.sum(pc.equal(column, ROW_KIND_GAP)).as_py() or 0
    return total if found else None


def count_quarantined(lake_root: Path | str) -> int:
    """How many partitions the quarantine ledger currently withholds.

    ``lake.battery`` is what writes the verdicts this counts, and it runs earlier in this same
    job, so the number is this evening's rather than last evening's. A lake with no
    ``quarantine.jsonl`` reads as no entries, which is what keeps this from raising on a fresh
    lake.

    A damaged ledger raises rather than reading low, because any count this reader could
    still take from one would say nothing. From a torn read it is the entries in front of the
    damage and reads low, from bytes that do not decode there is nothing to count, and from a
    byte-order mark it is either low by the verdict the mark discarded or right about a partition
    no reader asks about. Six shapes raise as a ``manifest.ManifestError``, the six the comment
    above ``_LEDGER_REFUSALS`` enumerates, so this names the class rather than one of its members.
    One of them is an entry whose ``partition`` cannot be a dict key, which raised a bare
    ``TypeError`` until marketlake #514.
    :func:`_counted` catches every one of them, because it catches bare ``Exception``, and
    reports the line.

    This is the whole ledger's open count rather than tonight's new findings. From the first
    verdict until a human signs it off, every night's file carries a standing non-zero number.
    That is quarantine being loud on purpose. What separates a new finding from an old one is
    :attr:`lake.battery.BatteryReport.appended`, which this run reports as the line
    ``battery wrote N quarantine lines`` under ``Nightly.report``.
    """
    root = Path(lake_root)
    return sum(1 for entry in latest_quarantine(root).values() if is_quarantined(entry))


def _subjects(held: Sequence) -> tuple[str, ...]:
    """Each held finding as ``<symbol> <observed_on> <check>``, for the report file.

    The three fields are the ones a reader needs to find the finding's own file under
    ``reports/withheld/``, which is keyed on ``observed_on``. What the gate computed and what
    it compared against stay out, because that pair is in the withheld file already and this
    one is a summary.
    """
    return tuple(
        f"{item.finding.symbol} {item.finding.observed_on.isoformat()} {item.finding.check}"
        for item in held
    )


def _ledger_outcome(report: ExtractionReport | SplitReport) -> PieceOutcome:
    """One corporate-actions walk's result, reduced to the plain values the file carries.

    ``skipped`` is read as a field on both reports. It used to be read through a ``getattr``
    default, because only ``SplitReport`` carried it and ``ExtractionReport`` did not, which
    meant the dividends piece reported zero skips on every night by construction. Marketlake
    #352 gave the dividend walk the same record, so the default has nothing left to cover and
    a defensive one that cannot fire is a line the next reader has to reason about.
    """
    return PieceOutcome(
        landed=len(report.appended),
        held=len(report.held),
        unfiled=len(report.unfiled),
        unchanged=report.unchanged,
        skipped=len(report.skipped),
        subjects=_subjects(report.held),
    )


def _bars_outcome(report: BarsReport | BackfillReport) -> PieceOutcome:
    """The bar walk's result, reduced the same way.

    Either report shape answers, because the four values read here are named the same on both.
    The nightly run hands it a ``BackfillReport`` and a by-hand single-session fetch still hands
    it a ``BarsReport``, and neither spelling is this function's to choose.

    ``unwalked`` is the one field only the backfill carries and it is deliberately not folded in.
    It names a ticker-day the plan could not resolve, which is reference data disagreeing rather
    than a walk that did not finish, so it belongs on the report's detail lines and not in a count
    that decides whether the piece refused.
    """
    return PieceOutcome(
        landed=len(report.landed),
        held=len(report.held),
        unfiled=len(report.unfiled),
        skipped=report.skipped,
        subjects=_subjects(report.held),
    )


def _counted(what: str, read: Callable[[], int | None], report: ReportLines) -> int | None:
    """One summary count, or ``None`` with a line saying why it could not be read.

    The three counts are a summary of the run rather than the run itself, so a failure
    here must not cost the record. They sit between the work and the ping, and an
    uncontained raise would take the report file and the digest with it while the check
    read green, which is the silence ``report.py`` exists to end. The partitions most
    likely to be unreadable are the ones a battery would quarantine, and
    :func:`count_gaps` opens every one of them on purpose.

    The failure is report-tier rather than a problem. The work the check watches did
    happen, so a count nobody could take must not withhold the ping.
    """
    try:
        return read()
    except Exception as exc:  # noqa: BLE001 - a summary must not cost the record
        report.add(f"{what} unreadable: {type(exc).__name__}: {exc}", ACTION)
        return None


def _refused(exc: BaseException) -> PieceOutcome:
    """A walk that stopped on one of its named conditions."""
    return PieceOutcome(refusal=f"{type(exc).__name__}: {exc}")


@dataclass(frozen=True)
class SweepOutcome:
    """What one sweep run did, for the command's sign-off block.

    ``filed_at`` is the report file, or ``None`` when the write failed, in which case
    ``filing_error`` names the class that refused it. That failure does not withhold the ping,
    because the work the check watches did happen. It costs an exit code and a line in the
    digest, which is the shape ``compact`` gives a schema-drift file it could not write.

    ``digest`` is the message that was built, whether or not it was sent, and ``delivered``
    says which. A digest that did not go is recorded under ``reports/alerts/`` like any other
    lost page, and **no run ever counts it**. ``undelivered`` is read before the digest is
    published, so this run cannot see its own loss, and it is keyed on the day the page
    failed, so the next evening reads a different directory. Every other page this job can
    raise is counted, because the refused-ping escalation happens before that read. The
    digest is the one message whose own failure no count reaches, which is why ``ok`` has to
    carry it.

    That matters more here than the arithmetic suggests. A topic quiet for weeks because
    nothing broke looks exactly like a dead subscription, and this message is the design's
    answer to that. A night where it silently did not go is the night the answer stops
    working.

    ``battery`` is what :func:`lake.battery.judge` returned, or ``None`` on a holiday and on a
    run whose battery refused. :meth:`render` prints its census, because a night that judged
    nothing and a night that judged the lake and found it clean are different answers and a
    block that printed neither would render them the same. ``lake.battery.render`` states that
    rule for the hand run and this is the same rule for the job's own block.
    """

    nightly: Nightly
    digest: Message
    delivered: bool
    battery: BatteryReport | None = None
    filed_at: Path | None = None
    filing_error: str | None = None

    @property
    def ok(self) -> bool:
        """Whether nothing about this run needs a person to look at it.

        Five conditions, and ``delivered`` is here for the reason the class docstring
        gives: it is the only failure this job can have that no count anywhere reaches.
        """
        return (
            not self.nightly.problems
            and self.nightly.pinged
            and self.nightly.unfiled == 0
            and self.filed_at is not None
            and self.delivered
        )

    def render(self) -> str:
        """A human-readable sign-off block."""
        nightly = self.nightly
        lines = [
            f"Vendor sweep for {nightly.day.isoformat()}"
            f" ({'session' if nightly.session else 'holiday'})"
        ]
        for name, outcome in nightly.pieces:
            if outcome.refusal is not None:
                lines.append(f"  {name}: did not run, {outcome.refusal}")
            else:
                lines.append(
                    f"  {name}: landed {outcome.landed} held {outcome.held}"
                    f" unchanged {outcome.unchanged} skipped {outcome.skipped}"
                )
        if self.battery is None:
            lines.append("  battery: did not run")
        else:
            lines.append(
                f"  battery: judged {self.battery.judged}"
                f" quarantined {self.battery.quarantined} clean {self.battery.cleared}"
                f" insufficient_history {self.battery.insufficient_history}"
                f" out_of_scope {self.battery.out_of_scope}"
                # **The three counts that say what happened to the ledger**, rather than what
                # the checks answered. The docstring above claims this block follows "the same
                # rule" as ``battery.render``, and while the block printed ten of that
                # function's thirteen counts the claim was not true. Marketlake #477 made it
                # true.
                #
                # **What they add is the zero.** ``judge`` appends one report line per
                # deferred, per withheld and per released partition, and the loop below prints
                # every one of them, so a night that honoured a sign-off already said so by
                # name here, in the report file and in the push. What no line can say is that
                # nothing happened, because an absent line reads the same whether the run
                # looked or not. That is this block's own rule one field along.
                # ``quarantined`` cannot stand in for ``deferred``: it counts quarantined
                # findings, which read the same whether or not a sign-off held.
                #
                # ``released`` is additionally the one no later run reproduces.
                # ``decide_partition`` computes it as ``held_before and not held_after``, and
                # ``held_before`` is the ledger as this run found it, so a hand run tomorrow
                # reads a ledger already showing the partition readable and reports zero. Its
                # line survives in the report file and its count does not.
                #
                # This spends no digest bytes. ``digest_body`` takes ``Nightly``, which carries
                # no battery field, so the 1000-byte cap is untouched by anything added here.
                f" deferred {self.battery.deferred}"
                f" withheld {self.battery.withheld}"
                f" released {self.battery.released}"
                f" scope_unknown {self.battery.scope_unknown}"
                f" unreadable {self.battery.unreadable}"
                f" sessions_owed {self.battery.sessions_owed}"
                f" sessions_missing {self.battery.sessions_missing}"
                # Printed only above zero, so a lake that never trims renders this line exactly
                # as it did before marketlake #782. ``battery.BatteryReport`` gives the reason.
                + (
                    f" sessions_trimmed {self.battery.sessions_trimmed}"
                    if self.battery.sessions_trimmed
                    else ""
                )
                + f" wrote {len(self.battery.appended)}"
            )
        lines.append(
            f"  gaps={'unsealed' if nightly.gaps is None else nightly.gaps}"
            f" quarantined={nightly.quarantined}"
            f" disagreements={nightly.disagreements}"
            f" pages_lost={nightly.pages_lost}"
        )
        for problem in nightly.problems:
            lines.append(f"  problem: {problem}")
        for line in nightly.report:
            lines.append(f"  report: {line}")
        if self.filed_at is None:
            lines.append(f"  report file NOT written: {self.filing_error}")
        else:
            lines.append(f"  report file: {self.filed_at}")
        lines.append(
            f"  digest sent={self.delivered} pinged={nightly.pinged} slug={EOD_SWEEP_SLUG}"
        )
        return "\n".join(lines)


def digest_body(nightly: Nightly) -> str:
    """The one-screen digest, under :data:`DIGEST_BYTE_CAP` bytes.

    A holiday sends the one line the design's message table pins and nothing else.

    Otherwise it carries counts and never a list of findings. The design's budget is one
    screen, and a digest that listed what was held would sit under the cap on every night
    anyone tested it and over the cap on the night that mattered. The per-finding detail is in
    the report file, and the pile under ``reports/withheld/`` is where each finding's own
    record already sits.

    Three things beyond the four counts earn their bytes. A walk that did not run says so with
    its reason, because that is what withheld the ping. A ping that did not land says so,
    because otherwise the only signal is a check going quiet, which reads as a dead machine. A
    report file that could not be written says so, because this message is then the only copy
    of these numbers that exists.
    """
    if not nightly.session:
        return HOLIDAY_BODY

    gaps = "unsealed" if nightly.gaps is None else str(nightly.gaps)
    lines = [
        f"gaps {gaps}, quarantined {nightly.quarantined},"
        f" disagreements {nightly.disagreements}, pages lost {nightly.pages_lost}"
    ]
    for name, outcome in nightly.pieces:
        if outcome.refusal is not None:
            # The class alone, for the reason ``PieceOutcome.refusal_class`` gives. The
            # fuller string stays on the job's own stdout, which ``render`` below prints.
            lines.append(f"{name}: did not run, {outcome.refusal_class}")
        else:
            lines.append(
                f"{name}: landed {outcome.landed}, held {outcome.held},"
                f" unchanged {outcome.unchanged}, skipped {outcome.skipped}"
            )
    if not nightly.pinged:
        lines.append("ping did not land")
    for line in nightly.report:
        # Redacted for the reason ``PieceOutcome.refusal_class`` gives, which names the
        # digest explicitly. A report-tier line is composed as a place and then an
        # exception, so ``redacted``'s keep-two-fields rule is the one that fits it,
        # and it is the same rule the report file applies to the same list.
        lines.append(f"report: {redacted(line)}")
    body = "\n".join(lines)
    # The cap is the design's, and a body over it is truncated rather than dropped. A digest
    # that did not arrive is indistinguishable from a dead subscription, which is the one
    # thing this message exists to rule out.
    encoded = body.encode("utf-8")
    if len(encoded) <= DIGEST_BYTE_CAP:
        return body
    ellipsis = "…"
    room = DIGEST_BYTE_CAP - len(ellipsis.encode("utf-8"))
    return encoded[:room].decode("utf-8", "ignore") + ellipsis


def _runway_figures(runway: Runway) -> tuple[str, str]:
    """The count and date, then the free space, the reserve and the rate, as two phrases.

    Neither carries ``": "``. ``report.redacted`` keeps only the first two fields of a line
    split on it, so a number after a second one would vanish from the report file and the
    digest. The rate takes two places because a session is well under one GiB.
    """
    days = runway.capture_days_left
    unit = "capture day" if days == 1 else "capture days"
    filled = runway.exhausts_on.isoformat() if runway.exhausts_on is not None else "undated"
    free = runway.free if runway.free is not None else 0
    return (
        f"{days} {unit} left, fills {filled}",
        f"{free / _GIB:.1f} GiB free less a {runway.reserve / _GIB:.1f} GiB journal reserve,"
        f" at {runway.peak / _GIB:.2f} GiB a session",
    )


def _page_disk_runway(publisher: Publisher, runway: Runway, *, now: datetime) -> None:
    """Page that the runway is under the floor, and echo it to stderr.

    No path, no URL and no value from the config directory, per the design's rules for
    every body, and well under its 1,000-byte budget whatever the numbers are. The echo
    follows ``battery.page_delayed_feed``: stderr carries the body unless the publisher
    refused it for carrying a secret, because printing it then would undo the redaction.
    """
    count, space = _runway_figures(runway)
    body = f"{count}: {space}. Grow the lake volume: {DISK_RUNWAY_REPAIR}."
    delivery = publisher.publish(
        Message(event=DISK_RUNWAY_EVENT, title=DISK_RUNWAY_TITLE, body=body), now=now
    )
    if delivery.reason == REFUSED:
        print("sweep: disk-runway page refused: it carried a secret", file=sys.stderr)
        return
    print(f"sweep: {DISK_RUNWAY_TITLE}: {body}", file=sys.stderr)
    if not delivery.sent:
        kept = "written down" if delivery.recorded else "lost"
        print(f"sweep: disk-runway page not sent: {delivery.reason}, {kept}", file=sys.stderr)


def _check_disk_runway(
    lake_root: Path,
    *,
    day: date,
    now: datetime,
    calendar: Calendar,
    publisher: Publisher | None,
    report: ReportLines,
) -> None:
    """File the runway in the nightly report when short or unreadable, and page under the floor.

    The value is ``lake.runway``'s and is never recomputed here, so the panel and this line
    read the same number off the same rule.

    A reading that failed is a line rather than a pass. ``assess`` raises nothing for a bad
    read: it names a device that would not read in ``space_error`` and counts the paths the
    walk could not read in ``usage.refused``, and ``short`` reads False in both cases. So
    each of those files its own ``action`` line, and so does anything ``assess`` lets
    through. A healthy night files nothing.

    The page goes through the publisher, never through ``problems``. A problem withholds the
    ping, which healthchecks would report as missing bars, and it pages only on the
    transition to down, so a runway that stayed short would never page again and would hide
    a real sweep failure behind it. ``publisher`` is ``None`` only in a test, the guard
    ``battery.judge``'s delayed-feed page carries. The page goes out every night the runway
    stays critical: the sweep is its own process and runs once a night, so it cannot spend
    a capture page's place under ``alert.DEFAULT_DAILY_CAP``.
    """
    try:
        runway = assess(lake_root, today=day, calendar=calendar)
        if runway.space_error is not None:
            report.add(f"disk runway unreadable: {runway.space_error}", ACTION)
        usage = runway.usage
        if usage.refused > 0:
            # Each named refusal carries its own ``": "``, so it is rewritten as
            # ``quotes (PermissionError)`` to keep the count inside the second field.
            named = []
            for refusal in usage.refusals:
                where, separator, kind = refusal.partition(": ")
                named.append(f"{where} ({kind})" if separator else refusal)
            more = usage.refused - len(named)
            if more > 0:
                named.append(f"{more} more")
            noun = "path" if usage.refused == 1 else "paths"
            report.add(
                f"disk runway walk refused: {usage.refused} {noun}, {', '.join(named)}",
                ACTION,
            )
        if runway.short:
            count, space = _runway_figures(runway)
            report.add(f"disk runway short: {count}, {space}", ACTION)
        if runway.critical and publisher is not None:
            _page_disk_runway(publisher, runway, now=now)
    except Exception as exc:  # noqa: BLE001 - a runway that cannot be read must not cost the record
        report.add(f"disk runway unreadable: {type(exc).__name__}: {exc}", ACTION)


def _window_edge(
    window_sessions: int | str | None,
    guards: GuardConstants | None,
    *,
    calendar: Calendar,
    day: date,
    report: ReportLines,
) -> date | None:
    """Tonight's window edge, or ``None`` when no checkpoint is to be written tonight.

    ``None`` for an absent key, which is every host that does not trim. A value the render
    should have refused, or an edge the calendar cannot place, files an ``action`` line and
    writes no checkpoint, so the trim then drops nothing and marketlake #438's runway alarm
    covers the growth. Neither withholds the ping, because no captured data is at risk.
    """
    try:
        sessions = window_sessions_of(window_sessions, guards or GuardConstants())
    except WindowRefused as exc:
        report.add(f"lake window refused: {exc}", ACTION)
        return None
    if sessions is None:
        return None
    try:
        return window_edge(calendar, day, sessions)
    except EdgeNotFound as exc:
        report.add(f"lake window edge not found: {exc}", ACTION)
        return None
    except Exception as exc:  # noqa: BLE001 - a calendar that cannot answer must not cost the record
        # ``window_edge`` turns every refusal it knows into ``EdgeNotFound``. This is the
        # backstop for one it does not, the way the Friday wake contains the same calendar.
        report.add(f"lake window edge not found: {type(exc).__name__}", ACTION)
        return None


def _file_split_refusals(
    split_report: SplitReport, *, problems: list[str], report: ReportLines
) -> None:
    """File each ticker the split walk refused, as a problem and as one bounded report line.

    Each problem withholds the ping and carries the refusal's full text, repair included.
    ``digest_body`` renders no problem line, so without the report line the phone would read
    "ping did not land" with no reason. The report line names the tickers, which the roster
    bounds, and holds one ``": "``, so ``report.redacted`` keeps it whole.
    """
    refused = split_report.refused
    for refusal in refused:
        problems.append(f"splits did not run for {refusal.ticker}: {refusal.reason}")
    if refused:
        names = ", ".join(refusal.ticker for refusal in refused)
        report.add(f"splits refused {len(refused)} ticker(s): {names}", ACTION)


def _write_split_checkpoint(
    root: Path,
    walked: SplitWalk,
    *,
    day: date,
    now: datetime,
    problems: list[str],
    report: ReportLines,
) -> None:
    """Write tonight's split checkpoint, or file why it was not written.

    A failed write withholds the ping, since the trim then reads last night's checkpoint and a
    night of it repeating is a fault worth a page. The write is contained broadly, the way the
    battery is, because what can raise here reaches past the ledger families
    ``_LEDGER_REFUSALS`` names, ``ArrowInvalid`` among them.

    A lake with no chains day has no ticker to save, and writes nothing.
    """
    if walked.blocked:
        problems.append(
            "split checkpoint not written: the checkpoint on disk cannot be read and a ticker "
            "still needs it, so it is kept for the repair"
        )
        report.add("split checkpoint not written: CheckpointUnreadable", ACTION)
        return
    if not walked.entries:
        return
    try:
        write_checkpoint(
            root,
            Checkpoint(session_day=day, entries=walked.entries),
            recorded_at=now,
            guard=not walked.replaces_unreadable,
        )
    except Exception as exc:  # noqa: BLE001 - a failed write must not cost the record
        problems.append(f"split checkpoint not written: {type(exc).__name__}: {exc}")
        report.add(f"split checkpoint not written: {type(exc).__name__}", ACTION)


def _friday_wake(
    *,
    now: datetime,
    calendar: Calendar,
    schedule_setter: ScheduleSetter,
    schedule_reader: ScheduleReader | None,
) -> tuple[list[str], ReportLines]:
    """Set the Sunday one-shot and read it back. Returns the problems and the report lines.

    The set runs on a Friday alone, holiday no-ops included, which the design's pmset table
    states directly. Its failure is a problem and withholds the ping, because nothing else in
    the system sets that wake and the Sunday read-back cannot catch a missed one.

    The read-back's verdict is report-tier rather than a page, which is the rule
    ``SundayOutcome.report`` carries: pmset alarm drift rides the nightly report "because the
    pre-open self-check already catches a missed wake an hour before the bell". So drift lands
    in the file and does not withhold the ping.

    ``expected_one_shot`` is what makes the read-back meaningful. It expects the one-shot only
    between the Friday sweep that sets it and its own firing, so the check is asking about the
    wake this run just set rather than about one that fired days ago.

    A ``None`` reader skips the read-back and adds no report line, the way the Sunday job
    skips its own. ``sweep_from_config`` resolves the setter and the reader one at a time,
    so a caller passing only a setter would otherwise add ``pmset read-back unreadable`` to
    every Friday's report.
    """
    problems: list[str] = []
    report = ReportLines()

    try:
        sunday = next_sunday_wake(now, calendar)
    except Exception as exc:  # noqa: BLE001 - a calendar that cannot answer is the same failure
        # Inside the ``try`` because the docstring above promises containment and this
        # call can raise too. ``next_sunday_wake`` walks forward for the next session and
        # gives up after fourteen days, and the calendar package refuses a date past its
        # own right bound. Either way nothing set the wake, which is what the message says.
        problems.append(f"sunday one-shot wake not set: {type(exc).__name__}: {exc}")
        return problems, report

    try:
        schedule_setter(sunday)
    except Exception as exc:  # noqa: BLE001 - every failure here is the operator's to see
        # ``stderr`` is text under the real setter, which passes ``text=True``, and bytes
        # under a caller that does not. Decoding rather than interpolating is what keeps a
        # ``b'...'`` repr out of the operator's line.
        detail = getattr(exc, "stderr", None) or ""
        if isinstance(detail, bytes):
            detail = detail.decode("utf-8", "replace")
        problems.append(
            f"sunday one-shot wake not set for {sunday.isoformat()}: "
            f"{type(exc).__name__}: {detail.strip() or exc}"
        )
        return problems, report

    if schedule_reader is None:
        return problems, report

    try:
        schedule = parse_pmset_schedule(schedule_reader())
    except Exception as exc:  # noqa: BLE001 - an unreadable read-back is a finding, not a crash
        alarms = AlarmCheck(
            repeat_ok=False,
            one_shot_ok=False,
            problems=(f"pmset read-back unreadable: {type(exc).__name__}: {exc}",),
        )
    else:
        alarms = check_alarms(schedule, one_shot_date=expected_one_shot(now, calendar))
    # pmset alarm drift, every line of it: a wake that will not fire wants a human.
    for line in alarms.problems:
        report.add(line, ACTION)
    return problems, report


def sweep(
    *,
    lake_root: Path | str,
    clock: Clock,
    calendar: Calendar,
    roster: Roster,
    vendor_source: VendorSource,
    pinger: Pinger,
    ping_url: str,
    publisher: Publisher | None,
    schedule_reader: ScheduleReader | None,
    schedule_setter: ScheduleSetter | None,
    guards: GuardConstants | None = None,
    window_sessions: int | str | None = None,
) -> SweepOutcome:
    """Run one evening sweep. Every seam is required, and the module docstring says why.

    ``window_sessions`` is ``Config.lake_window_sessions`` as loaded, unjudged. It is a setting
    rather than a seam, so it defaults to ``None``, an absent key, the way ``guards`` does.

    ``publisher`` alone may be ``None``, which sends no digest and escalates no refused ping.
    That is what lets a test drive the run without a page reaching anywhere, and it is the
    same allowance ``escalate_ping_failure`` already makes. ``schedule_setter`` and
    ``schedule_reader`` may be ``None`` on a host with no wake, and the module docstring
    says what each skips.

    The order below is the design's, and two placements in it were decided rather than
    inherited. The ping comes before the report file, because ``pinged`` is part of what the
    file records and the ping is one bounded GET whose failures are already contained. The
    file still comes before the digest, which is the longer call and the one that reaches a
    third party.
    """
    root = Path(lake_root)
    now = clock.now()
    day = now.astimezone(MARKET_TZ).date()
    session = calendar.is_session(day)

    problems: list[str] = []
    report = ReportLines()
    pieces: list[tuple[str, PieceOutcome]] = []

    # **Marketlake #130: is the running schema version recorded in the lake at all.** Nothing
    # forces ``python -m lake.schema_versions`` to be run beside a deliberate bump, and a
    # version whose shape is recorded nowhere makes every read of its rows refuse. The daemon
    # asks the same question at startup and pages once. This is the recurring half, because a
    # resident daemon carries the tree it was started with, so a deploy with no restart lands
    # the new version in the lake from *this* process: ``backfill_bars`` below stamps
    # ``journal.SCHEMA_VERSION`` on every bar it writes.
    #
    # Report-tier rather than a problem, which is ``_counted``'s line. The work the
    # ``eod-sweep`` check watches did happen, so a summary must not withhold the ping. The
    # line then repeats every weekday until the tool is run, the cadence ``count_quarantined``
    # already sets.
    #
    # Outside the session branch, because a holiday skips the walks and this is not a walk.
    # The condition does not depend on the session and the report file is written on every
    # run, holiday no-op included.
    #
    # **One of the four verdicts cannot survive a session evening, and that is marketlake
    # #494 rather than this call's doing.** The fourth, a ledger this process was refused
    # permission to open, does survive: ``PermissionError`` is an ``OSError``, which
    # ``_LEDGER_REFUSALS`` names, so the walks refuse and this line still reaches the report
    # (marketlake #536). ``_LEDGER_REFUSALS`` below does not name
    # ``SchemaVersionsError``, so a ledger this code cannot read raises out of
    # ``extract_dividends`` and out of this function, taking the report file, the digest and
    # the ping with it. The line is computed and lost with them. A holiday survives, because
    # a holiday opens no reference file. The daemon's startup check still reports that
    # verdict, so the condition is not invisible while #494 is open.
    version_check = check_running_version(root)
    if not version_check.ok:
        report.add(version_check.summary, ACTION)

    # **Marketlake #438: the disk runway**, once the day's journal has been compacted. Before
    # then the growing journal is already missing from free space and the full journal
    # reserve comes off as well, so a catch-up run mid-session would read 9 to 13 sessions
    # short and could page falsely. The moment is the option close plus
    # ``COMPACTION_DELAY``, the one compaction waits for, rather than the equity close, since
    # a run between 16:00 and 16:30 would still read today's journal. A day with no session
    # has no journal to wait for, and the ``not session`` arm short-circuits first because
    # ``option_close`` refuses a non-session. Gating on ``today in usage.unsealed`` instead
    # would silence the night a compaction failed, which is a night this check is for.
    if not session or calendar.option_close(day) + COMPACTION_DELAY <= now:
        _check_disk_runway(
            root, day=day, now=now, calendar=calendar, publisher=publisher, report=report
        )

    if session:
        closed = calendar.session_close(day) <= now
        # **Each walk carries its own arguments, because the two no longer take the same ones.**
        # ``detect_splits`` takes the calendar under marketlake #431: it decides whether two
        # sealed sessions are adjacent, and the manifest cannot answer that for a session the
        # lake never captured. ``extract_dividends`` reads ``ex_date`` off the row rather than
        # deriving it from a pair, so an uncaptured session cannot move its key and it needs no
        # calendar. The loop stays, because what it holds is the refusal containment and the
        # piece naming, which are still one rule for both.
        #
        # **The split walk runs through ``split_checkpoint.walk_splits``**, marketlake #786, which
        # resumes a ticker whose chains days were trimmed from the checkpoint or refuses it. The
        # window edge goes in only when the window key is set, because the edge is what the
        # checkpoint written below is cut at, and nothing is written without the key.
        edge = _window_edge(window_sessions, guards, calendar=calendar, day=day, report=report)
        split_walks: list[SplitWalk] = []

        def splits_walk() -> SplitReport:
            walked = walk_splits(lake_root=root, clock=clock, calendar=calendar, edge=edge)
            split_walks.append(walked)
            return walked.report

        walks = (
            (DIVIDENDS_PIECE, partial(extract_dividends, lake_root=root, clock=clock)),
            (SPLITS_PIECE, splits_walk),
        )
        for name, walk in walks:
            try:
                pieces.append((name, _ledger_outcome(walk())))
            except _LEDGER_REFUSALS as exc:
                pieces.append((name, _refused(exc)))
        for walked in split_walks:
            _file_split_refusals(walked.report, problems=problems, report=report)
            if edge is not None:
                _write_split_checkpoint(
                    root, walked, day=day, now=now, problems=problems, report=report
                )
        if closed:
            # **The walk is the backfill, not a single session, and marketlake #422 is why.**
            # A daily bar is fetched only once the calendar-next session has sealed, which at
            # 18:30 on session S it has not. Until marketlake #618 that session was also what
            # the bar was judged against. The single-session fetch said that
            # settled itself because the next run would land the bar, and it did not: the next
            # run fetched the *next* session and met the same absence for it, and nothing
            # scheduled ever came back. So the nightly job landed no daily bar at all, and this
            # walk is what makes that promise true.
            #
            # **What that night's ticker-day does about it changed under marketlake #434.** It
            # used to be fetched, gated against a close nobody had captured, and held, which
            # spent a vendor request and filed a withheld file on a gate that could not pass.
            # Whether the next session has sealed is this lake's to answer rather than the
            # vendor's, so the walk asks it first and reports the ticker-day under ``unsettled``
            # instead. The recovery above
            # is untouched, which is what makes the skip safe: a session skipped tonight is a
            # session tomorrow's run still walks.
            #
            # ``backfill_bars`` is the walk that already existed, marketlake #319, and it
            # subsumes the single-session fetch rather than running beside it: ``_span_sessions``
            # puts a session in range once its equity close has arrived, so at 18:30 today is in
            # range along with every earlier session still unlanded. Yesterday's held daily bar
            # is reached with its following session now sealed.
            #
            # **Its cost is bounded rather than reasoned about, which is marketlake #478.** This
            # comment used to argue the walk was safe because a run at the 1-minute lookback
            # deadline is 88 requests against a ceiling of 120 a minute. That argument holds only
            # for a lake whose manifest is intact. A rebuilt or restored one skips nothing and
            # asks for every session the spans cover, back to back, and nothing paces the loop.
            #
            # ``guards.bars_request_budget`` is what answers it now. One run spends at most that
            # many requests and this job fires once a day, so the budget is also the most that can
            # reach the vendor in any rolling minute, and a budget under the ceiling makes the
            # crossing arithmetically impossible however fast the run fires. What a run does not
            # reach is not manifested, so the next evening's plan still holds it. The constant
            # carries the five measurements that picked it.
            #
            # The manifested skip still avoids the vendor call as well as the write, so every
            # ticker-day that already landed costs nothing and an ordinary evening never comes
            # near the bound.
            #
            # A date flag is not the alternative. ``bars._build_parser`` records one written and
            # removed before merge, because it is a backfill selector with no capture-span floor
            # and one typo would land bars for a session the lake never captured. This walk takes
            # no date at all.
            try:
                walked = backfill_bars(
                    lake_root=root,
                    vendor=vendor_source(),
                    clock=clock,
                    calendar=calendar,
                    # **Enabled only, which is the nightly's own scope rather than the walk's.**
                    # ``_require_supported_plan`` checks every ticker-day the plan holds, retired
                    # ones included, because a by-hand recovery run fetches those on purpose. Its
                    # docstring says ``_require_supported``'s narrower rule "stops holding" for
                    # that reason. Pointing the nightly job at this walk makes it hold again: a
                    # stale ``bars:`` line on a retired ticker would raise ``UnsupportedBarFreq``,
                    # end the bars piece and withhold the ping every night, for a ticker nothing
                    # captures. That is the exact harm ``_require_supported`` exists to prevent.
                    #
                    # Filtering restores the scope the nightly had before marketlake #422, so a
                    # retired ticker's unlanded bars stay the by-hand backfill's to recover, which
                    # is where they already were.
                    roster=Roster(roster.enabled),
                    spans=read_capture_spans(root),
                    # Passed straight through, ``None`` included, because the callee resolves it
                    # to the design's pinned defaults. That is the shape ``judge`` below already
                    # takes.
                    guards=guards,
                )
                pieces.append((BARS_PIECE, _bars_outcome(walked)))
                # A ticker-day the plan could not resolve, which is the master and the spans
                # disagreeing. It is not a refusal, so it does not withhold the ping, and it is
                # not a held finding, so no withheld file carries it. Without a line here it
                # would reach nobody.
                #
                # **One line, counted, rather than one line each.** The list is the plan's, so it
                # repeats every night and grows by a ticker-day per trading day: ``retire
                # --remove`` drops the roster entry and leaves the closed span, so every session
                # that span covered is unresolvable for ever. Rendered in full it walks the
                # nightly report into ``digest_body``'s 1000-byte cap and truncates it, and what
                # falls off the end first is the battery's own census, which is appended after
                # these. A line that silences the check above it is worse than no line, so this
                # one is bounded and the full list stays in the by-hand ``--backfill`` run.
                if walked.unwalked:
                    report.add(
                        f"bars unwalked: {len(walked.unwalked)} ticker-day(s), "
                        f"first: {walked.unwalked[0]}",
                        ACTION,
                    )
                # A daily ticker-day whose gate has no close of record and never will. It is
                # not a refusal, so it does not withhold the ping, and marketlake #434 is what
                # stopped it being a held finding: filed nightly under ``reports/withheld/`` it
                # was a permanent condition wearing an incident's clothes, and a genuinely new
                # failure on night two hundred read as one more in the count.
                #
                # **Counted per reason, rather than naming the first entry.** The list repeats
                # every night, so rendered in full it walks this report into ``digest_body``'s
                # byte cap and truncates the battery's census off the end, and the full list
                # stays on the by-hand ``--backfill`` run's own output. Naming the first entry
                # was the obvious bound and it is the wrong one, twice over.
                #
                # ``report.redacted`` keeps two colon-separated fields, and an entry is itself
                # composed as a ticker-day and then a reason, so a line reading "first: SPY 1d
                # 2026-09-08: NoSpotClose" reaches both the digest and the nightly file as
                # "first" with nothing after it. The ``unwalked`` line above has the same shape
                # and loses its entry the same way. A line with one colon in it survives whole.
                #
                # The reason is read off ``GateSkip`` rather than parsed back out of its
                # rendered line. That record exists for this: the two spellings agreed only
                # because a class name carries no ``": "`` of its own, and nothing pinned that.
                #
                # ``sorted`` is what makes the line the same on two runs over one lake. A
                # ``Counter`` keeps insertion order, which here is the walk's session order, so
                # an outage and a quarantine would swap places in the census according to which
                # session came first and a reader diffing two nights would see a change that is
                # not one.
                #
                # And the walk takes ``plan.days`` newest session first under marketlake #478, so
                # the first entry is whichever ticker-day the newest session in range produced.
                # That changes every evening, so naming it would report a different subject each
                # night while saying nothing about what moved. The argument was the same before
                # #478 inverted the order and the reason was the mirror image of this one: the
                # slot belonged to the live lake's six permanent ticker-days from the 2026-09-08
                # outage for ever, and a quarantine appearing tonight would move a count from six
                # to seven and be named nowhere.
                # Counting the classes is bounded by how many reasons exist, which is four, and
                # a new class appearing in the line is the signal that something changed.
                #
                # A ``report`` line is the right carrier rather than a count on ``PieceOutcome``.
                # That record holds "plain values rather than the walk's own report" shared by
                # all three walks, and a bars-only number beside ``landed`` and ``held`` is what
                # its import-direction rule refuses. The line reaches the nightly file, the
                # digest and the dashboard's History panel. On the nights between the newest and
                # the oldest it reads, the panel folds an ``INFO`` line whose text matches the
                # report before's into one count, so a census that reads the same every night is
                # drawn in full on the newest night and a count that moves shows as a new line
                # (marketlake #617). Every one of those three reads it through
                # ``report.redacted``, which is the other half of why it counts classes rather
                # than naming an entry.
                #
                # **``unsettled`` is deliberately not reported here.** It is a ticker-day that
                # cannot be judged yet, because the next session has not sealed or its own has
                # not. On a healthy run that is the newest session and nothing else, waiting for
                # the next one, right every night by construction. A line
                # that is loud every evening is one the reader learns to skip, which is the
                # argument ``dashboard._ping_owed`` already makes in those words. It still
                # reaches the by-hand run's own output, which is where a reader who wants it
                # goes.
                if walked.abandoned:
                    reasons = Counter(entry.reason for entry in walked.abandoned)
                    census = ", ".join(
                        f"{count} {reason}" for reason, count in sorted(reasons.items())
                    )
                    # ``INFO`` only when every reason says the sealed partition holds no
                    # usable close, which nothing can change. The other three want a human:
                    # a restore, a sign-off, or a schema change (marketlake #530).
                    report.add(
                        f"bars abandoned: {len(walked.abandoned)} ticker-day(s), {census}",
                        INFO if set(reasons) <= _PERMANENT_ABANDON_REASONS else ACTION,
                    )
                # A ticker-day the run's request budget stopped it from fetching, which is
                # marketlake #478. It is the only line here that reports no fault: the run spent
                # what it was allowed and the next evening's plan still holds the remainder,
                # because a ticker-day that was never fetched was never manifested. So it does not
                # withhold the ping and it files no withheld record, and without a line here a
                # bounded run would look exactly like a complete one.
                #
                # **Counted, and only when the bound actually bit.** An ordinary evening reaches
                # nothing like the budget, so the condition is on the list rather than on the
                # budget being reached, and a run whose last ticker-day was also its last request
                # reports nothing. The full list stays on the by-hand ``--backfill`` run's own
                # output, for the reason the two lines above give: rendered whole it would walk
                # this report into ``digest_body``'s byte cap and truncate the battery's census off
                # the end.
                #
                # **What was spent is read off the report rather than off the constant.**
                # ``attempted`` is the count of ticker-days that reached the vendor, so on a run
                # the budget bit it is the budget, and reading it here means the line cannot
                # disagree with the walk. Naming ``guards.bars_request_budget`` instead would be a
                # second source for one number, and this job may hold ``None`` for the guards
                # while the walk resolved its own default.
                #
                # The line carries one ``": "``, so ``report.redacted`` keeps it whole. Measured
                # in digest form it runs 58 to 63 bytes across the range of counts it can carry,
                # from a single deferred ticker-day to five figures of them, against a 1000-byte
                # cap the three bars lines together reach about a sixth of.
                if walked.deferred:
                    report.add(
                        f"bars deferred: {len(walked.deferred)} ticker-day(s), "
                        f"{walked.attempted} request(s) spent",
                        INFO,
                    )
            except _BARS_REFUSALS as exc:
                pieces.append((BARS_PIECE, _refused(exc)))
        else:
            # A catch-up run for an 18:30 the host missed: launchd fires it on the Mac's next
            # wake, and systemd's ``Persistent=true`` once a VM is back up. Fetching now would
            # ask for a session still in progress.
            #
            # The guard stays although the walk above now applies its own per-session ceiling and
            # would simply leave today out. What it costs is the earlier sessions that run could
            # still have recovered, and that costs nothing lasting: this day's own 18:30 fires
            # later and walks them. Dropping it would be a second behaviour change riding on
            # #422's, and the count of runs that have met this branch is zero.
            close = calendar.session_close(day)
            pieces.append(
                (
                    BARS_PIECE,
                    PieceOutcome(
                        refusal=(
                            f"the {day.isoformat()} equity close at "
                            f"{close.isoformat(timespec='minutes')} has not passed"
                        )
                    ),
                )
            )

    # Step 3, the design's own placement: after the bar fetch and before the Friday branch.
    # Contained in its own tuple for the reason ``_LEDGER_REFUSALS`` exists, and with a broad
    # catch under it. The partitions most likely to make this raise are the ones a battery would
    # quarantine, and it opens every one of them on purpose, so an uncontained raise here would
    # cost the Friday wake, the ping and the report file on exactly the night that mattered.
    battery: BatteryReport | None = None
    if session:
        try:
            battery = judge(
                root,
                now=now,
                calendar=calendar,
                day=day,
                guards=guards,
                publisher=publisher,
            )
        except Exception as exc:  # noqa: BLE001 - the battery must not cost the record
            report.add(f"battery did not run: {type(exc).__name__}: {exc}", ACTION)
        else:
            report.pour(battery.report, battery.report_kinds)

    # A host with no wake to set, which is Linux, passes no setter and skips the branch.
    if schedule_setter is not None and day.weekday() == _PY_FRIDAY:
        wake_problems, wake_report = _friday_wake(
            now=now,
            calendar=calendar,
            schedule_setter=schedule_setter,
            schedule_reader=schedule_reader,
        )
        problems.extend(wake_problems)
        report.pour(wake_report.lines, wake_report.kinds)

    for name, outcome in pieces:
        if outcome.refusal is not None:
            problems.append(f"{name} did not run: {outcome.refusal}")

    # Read before the ping, and each one contained. These are a summary of the run, and
    # an uncontained raise between the ping and the file would lose the record and the
    # digest on a night the check had already gone green.
    gaps = _counted("gap count", lambda: count_gaps(root, day), report) if session else None
    quarantined = _counted("quarantine count", lambda: count_quarantined(root), report)
    pages_lost = _counted("lost-page count", lambda: undelivered(root, day), report)

    pinged = False
    if not problems:
        try:
            pinger.ping(ping_url)
            pinged = True
        except PING_FAILURES as exc:
            problems.append(f"ping failed: {type(exc).__name__}")
            # A refused ping feeds no check, so no check can ever go silent to report it.
            # This job runs once per process, so it pages at most once per run by
            # construction and needs no ``SlugEscalation`` to hold that.
            escalate_ping_failure(exc, slug=EOD_SWEEP_SLUG, publisher=publisher, now=now)

    if battery is not None and battery.appended:
        # ``ACTION``, because this line is how a new quarantine reaches the nightly, and for
        # the two checks that report and never page it is the only sign of one. The count
        # includes releases until marketlake #439 splits it, and the kind cannot say which
        # half a line is.
        report.add(wrote_line(len(battery.appended)), ACTION)

    nightly = Nightly(
        day=day,
        session=session,
        pinged=pinged,
        gaps=gaps,
        quarantined=quarantined,
        pages_lost=pages_lost,
        pieces=tuple(pieces),
        problems=tuple(problems),
        report=report.lines,
        report_kinds=report.kinds,
    )

    filed_at: Path | None = None
    filing_error: str | None = None
    try:
        filed_at = write_nightly(root, nightly, now=now)
    except OSError as exc:
        filing_error = type(exc).__name__

    body = digest_body(nightly)
    if filing_error is not None:
        body = f"{body}\nreport file NOT written: {filing_error}"
    digest = Message(
        event=NIGHTLY_EVENT,
        title=f"Nightly {day.isoformat()}",
        body=body,
        priority=NIGHTLY_PRIORITY,
    )
    delivered = False
    if publisher is not None:
        delivered = publisher.publish(digest, now=now).sent

    return SweepOutcome(
        nightly=nightly,
        digest=digest,
        delivered=delivered,
        filed_at=filed_at,
        filing_error=filing_error,
        battery=battery,
    )


def sweep_from_config(
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    vendor_source: VendorSource | None = None,
    schedule_setter: ScheduleSetter | None = None,
    schedule_reader: ScheduleReader | None = None,
    token_path: str | Path | None = None,
) -> SweepOutcome:
    """The sweep wired from the real config. This is the entry :func:`main` calls.

    The vendor arrives as a thunk rather than as a built object, which is what keeps a holiday
    run from reading the token at all. ``from_token`` imports ``schwab-py`` lazily, so the
    offline suite loads this module without the library installed.

    A ``None`` schedule setter or reader resolves to the live one on macOS and stays ``None``
    on Linux, each on its own, because a VM never sleeps and has no wake to set or read
    back. On Linux the sweep then never calls ``sudo``, so the VM needs no sudoers rule.
    """
    from lake.config import load_config

    config = load_config(config_path)

    def build_vendor() -> Vendor:
        return SchwabVendor.from_token(
            default_token_path() if token_path is None else token_path,
            api_key=config.schwab_api_key.reveal(),
            app_secret=config.schwab_app_secret.reveal(),
        )

    run_clock = SystemClock() if clock is None else clock
    sends = outbox.senders(config, process="sweep", clock=run_clock)
    on_macos = control_plane.is_macos()
    default_reader = read_pmset_schedule if on_macos else None
    default_setter = set_sunday_wake if on_macos else None
    return sweep(
        lake_root=config.lake_root,
        clock=run_clock,
        calendar=ExchangeCalendar(),
        roster=load_tickers(tickers_path),
        vendor_source=build_vendor if vendor_source is None else vendor_source,
        pinger=sends.pinger,
        ping_url=config.healthchecks_url(EOD_SWEEP_SLUG),
        publisher=Publisher(
            lake_root=config.lake_root,
            transport=sends.transport,
            # The values that must never reach a phone, checked against the message itself.
            secrets=config.page_secrets(),
        ),
        schedule_reader=schedule_reader if schedule_reader is not None else default_reader,
        schedule_setter=schedule_setter if schedule_setter is not None else default_setter,
        guards=config.guards,
        window_sessions=config.lake_window_sessions,
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.sweep",
        description=(
            "The evening vendor sweep: poll corporate actions, fetch the session's bars, "
            "set the Sunday wake on a Friday on macOS, ping, file the nightly report and "
            "send its digest. It fetches the session the clock is in."
        ),
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument(
        "--tickers", help="Path to tickers.yaml (defaults to the standard location)."
    )
    # No flag names the session, for the reason ``bars._build_parser`` gives. A date flag
    # reads as a convenience and is a backfill selector, and marketlake #319 owns the span of
    # sessions a run covers. The session stays injectable one layer down, through the clock.
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    vendor_source: VendorSource | None = None,
    schedule_setter: ScheduleSetter | None = None,
    schedule_reader: ScheduleReader | None = None,
) -> int:
    """The ``python -m lake.sweep`` entry. Returns a process exit code.

    ``clock`` stays injectable for the reason ``actions.main`` gives: a wall clock never
    reaches past this process, and what "a second night" means has to be something a test
    decides. The three seams beside it are injectable for the reason
    ``bars.fetch_session_bars_from_config`` gives: two of this command's exit codes can only
    be driven through them.

    Three exit codes, matching the siblings, and they answer a different question from the
    ping. The ping asks whether the day's work happened. These ask whether a person needs to
    look. Zero when every piece finished, every held finding was filed, the ping landed and
    the report file was written. One otherwise, which is ``control_plane.main``'s own answer
    for the Sunday command, where it returns ``0 if pinged else 1``. Two for an operator
    mistake in one of the three files the config directory holds, which
    ``config.input_errors_exit`` already turns into one line.
    """
    args = _build_parser().parse_args(argv)

    from lake.config import input_errors_exit

    with input_errors_exit("sweep"):
        outcome = sweep_from_config(
            clock=clock,
            config_path=args.config,
            tickers_path=args.tickers,
            vendor_source=vendor_source,
            schedule_setter=schedule_setter,
            schedule_reader=schedule_reader,
        )
    print(outcome.render())
    return 0 if outcome.ok else 1


__all__ = [
    "DIGEST_BYTE_CAP",
    "DISK_RUNWAY_EVENT",
    "DISK_RUNWAY_REPAIR",
    "DISK_RUNWAY_TITLE",
    "HOLIDAY_BODY",
    "NIGHTLY_EVENT",
    "NIGHTLY_PRIORITY",
    "ScheduleSetter",
    "SweepOutcome",
    "VendorSource",
    "count_gaps",
    "count_quarantined",
    "digest_body",
    "main",
    "set_sunday_wake",
    "sweep",
    "sweep_from_config",
]


if __name__ == "__main__":
    raise SystemExit(main())
