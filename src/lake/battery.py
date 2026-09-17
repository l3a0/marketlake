"""The validation battery: the quarantine writer and the four seal-then-flag checks.

The lake has had a quarantine reader since marketlake #241 and no writer. ``load_chain`` and
``load_bars`` already refuse a partition the ledger withholds, ``manifest.is_quarantined`` is
already the one definition both sides resolve at, and ``sweep.count_quarantined`` already feeds
the nightly report. Every one of those has read zero because nothing has ever written a verdict.

This writes them.

Run it with ``python -m lake.battery``. ``lake.sweep`` is what schedules it: the design places
the battery in the 18:30 vendor sweep between the bar fetch and the Friday branch, and this
module's :func:`judge` is what goes there.

**The two modes, and which one this is.** The design gates in two modes matching the schedule.
*Gate-before-land* validates a vendor-sweep surface before its partition is written, so a
failure means the partition never lands. That half is finished: ``lake.bars`` gates bars and
``lake.actions`` and ``lake.splits`` gate corporate actions, all of them filing held findings
under ``reports/withheld/``. *Seal-then-flag* is this one. Chains and quotes compact immutable
at close+15, so a failure found afterwards writes an entry in the quarantine ledger, which is
metadata beside the sealed partition rather than a rewrite or a removal.

What that buys is the consumer-side meaning the loader already implements. A quarantined
partition is refused by default and ``include_quarantined=True`` reads it anyway. What it costs
is that a bad partition stays on disk, which is deliberate: a capture minute is perishable and
the verdict about it is not.

**The entry's five fields, and why four of them were decided elsewhere.**
``manifest.is_quarantined`` ships and fails closed, so a verdict it cannot read withholds its
partition forever, and marketlake #139 states a precedence rule its own tool later builds on.
Between them they decide four of the five.

1. ``partition``, the first half of the key the reader resolves on.
2. ``verdict``, which the reader resolves.
3. ``check``, the second half of that key, which #139's rule also compares.
4. ``provenance``, which #139's rule reads to tell a human's row from the battery's.
5. ``observed_at``, the run's own stamp, which is the one this module chose.

:func:`build_entry` is the only place an entry is assembled, so a malformed one cannot be
written by hand.

``partition`` is the lake-relative path, spelled exactly as ``LakePaths.partition_path``
produces it. Case is load-bearing rather than cosmetic. ``loader.PartitionAbsent`` says why: on
macOS ``ticker=spy`` opens the ``ticker=SPY`` partition while the quarantine lookup keys on the
caller's spelling and finds no verdict, which turns the guard from fail closed into fail open.

**``insufficient_history`` is a finding and never a verdict.** ``config.min_trailing_sessions``
says a median-relative check with fewer than five trailing sessions still runs but tags its
rows *insufficient_history* instead of clean, and the design adds that there is never a silent
pass. Writing that tag into ``verdict`` would withhold every partition such a check touched,
because the reader refuses anything that is not ``clean``. So :class:`Finding` carries the
per-check answer and the ledger carries the partition's readability, and only a finding whose
``verdict`` is one of :data:`VERDICTS` ever reaches the ledger.

**The writer takes the lock and refreshes the ledger's manifest entry, in one invocation.**
``manifest.SCRUB_EXCLUSIONS`` holds the manifest, ``journal/`` and ``reports/`` and nothing
else, and the comment above it says each ledger writer refreshes its own manifest entry in the
same locked invocation that appends the row, because that is the check which catches a verdict
written without its entry. ``quarantine.jsonl`` is not excluded, so an unmanifested ledger is an
orphan to the Sunday scrub. #139 requires the same of the sign-off tool, and ``lake.signoff``
meets it at :func:`append_verdict` rather than at the bare append. ``actions.append`` is
the worked precedent and :func:`append_verdict` follows it, down to counting the file's lines
rather than the entries a read returns, so a damaged ledger cannot stop the writer.

The pair sits at two levels rather than one. :func:`append_verdict` takes the lock and is what
``lake.signoff`` and any other caller holding none wants. :func:`write_verdict` is the same two
writes for a caller already inside the hold, which is :func:`judge`, because it has to read the
ledger and append under one hold and ``lake_lock`` blocks forever on re-entry. No other module
carries two levels, because none has needed them: ``lake.occ_mapping`` reaches the same end by
calling ``manifest.record_partition``, which already takes no lock, rather than the locking
``actions.append``. The ledger's pair had no such primitive to reach for, since the rule that
its line and its manifest entry go together is what :func:`append_verdict` exists to enforce.

**Human precedence, which #139 states and this builds.** Before appending, the battery reads
that check's own current entry for the partition. If a human wrote it, a verdict from the
*same* check never supersedes it, and the run says "re-observed, human precedence stands" in
the nightly report. Reading the check's own entry rather than the partition's last line is
marketlake #426: a later entry from any other check used to hide the sign-off entirely.
#139 depended on this deliverable and shipped after it, so a rule built there would have
arrived too late: the sign-off tool would have shipped with its sign-offs undone by the next
nightly run. ``lake.signoff`` is that tool, and it writes its sign-off under the check named on
the entry it supersedes, which is the token this function compares.

**Append on transition only, per check.** A sealed partition is immutable, so the same check
against the same partition is the same finding every night. A check with no entry has said
nothing yet, so writing ``clean`` for a partition it passes would cost a line and change
nothing, and doing it nightly would grow the ledger by the roster times the retention forever.

The transition is measured against that check's own verdict rather than against the
partition's readability, which is marketlake #426. A check that passes while another still
withholds does record its pass, and the partition stays withheld because the other check's
verdict stands. Measuring against the partition instead would either lose that pass, leaving
the partition withheld forever once the other check cleared, or take it as a release.

**What this reads, and what it refuses to read.** It reads Parquet directly rather than through
the loader, for two reasons. ``load_chain(ticker, day, snap=None)`` returns one minute's
snapshot, which is the wrong shape for a whole day's rows, and ``sweep.count_gaps`` already
states that precedent for the same reason. And the loader refuses a quarantined partition by
default, so a battery reading through it would be blind to every partition it had itself
flagged, which is exactly the set human precedence needs it to re-observe.

**The four checks, and what each one answers.** Marketlake #406 built the spine and the first.
#407 added the other three, which is what makes the per-check ledger resolution #426 shipped do
any work: until a second check existed, no partition could be withheld by two.

1. :data:`CHECK_ENTITLEMENT`, the vendor's own real-time flags and the session-median staleness.
   It is the one whose failure the design says corrupts every row silently, and the only one of
   the four that pages.
2. :data:`CHECK_CALENDAR_COVERAGE`, no silently missing sessions, clamped per ticker to its
   capture spans. It is the odd one: it judges the sessions that have *no* partition, so it
   writes no ledger line, ignores the run's ``day``, and enumerates instruments rather than the
   directories the walk found. :func:`coverage` carries the second and third reasons and
   :data:`MISSING_SESSION` carries the first.
3. :data:`CHECK_QUOTE_SANITY`, ``bid <= mark <= ask`` at a rate within a measured tolerance.
   Crossed quotes are real and :data:`QUOTE_SANITY_TOLERANCE` carries the measurement that says
   so.
4. :data:`CHECK_ROW_COUNT_BAND`, every session snapshot inside a band of the trailing median,
   which catches a truncated fetch. It is the options-only one of the four.

**Two classes of partition are out of scope for every check, and the lake holds both today.**

1. A partition outside the ticker's capture spans. Capture was not running, so nothing about
   that partition is evidence about the feed. SPY's 2026-09-02 chains partition is the live
   case: two rows carrying a null ``ask``, at a staleness of 1,380,301 seconds, which is 16
   days. Both capture spans start 2026-09-08T17:07:00Z. Without this rule the first
   run quarantines the lake's oldest partition and pages about a day-one probe.
2. A partition holding no data rows. A gap row is the design's record that a minute was missed,
   so a day of them is a correctly-recorded outage rather than a truncated fetch. Sixteen of the
   lake's 29 sealed partitions are exactly this, 2026-09-08 through 2026-09-11 on both tickers
   and both surfaces.

Neither is a pass. An out-of-scope partition is not judged at all, so it gets no verdict and no
ledger line. It gets one finding, carrying :data:`CHECK_SCOPE`, because scope is a property of the
partition and every check would answer it from the same two facts. A judged chains partition
carries three findings and a quotes partition two. Never four: calendar coverage only ever
speaks about sessions that have no partition at all.

**A partition's findings are decided in one call.** :func:`decide_partition` carries the ledger
state forward as lines land, so two checks clearing in one walk both change what withholds the
partition. Deciding each finding on its own would report the second check's partition as still
held by the first, and the release would never be reported at all. :func:`judge` gathers a
partition's findings and hands them over together, which is the seam that function's docstring
was written for.

**Each of the other three reports and never pages.** The design's message table gives the
battery two pages: the delayed feed here, and its own nightly schema drift, which is #427. So
:func:`page_delayed_feed` is filtered to :data:`CHECK_ENTITLEMENT` rather than to every verdict
this run wrote. Without that filter a crossed quote reaches a phone titled ``Delayed feed``
with its rate rendered as a staleness in seconds.

**The entitlement check, and the two things measuring it changed.** The design: the vendor's own
entitlement flags must show real-time on every snapshot, ``isDelayed`` false on chain responses
and ``realtime`` true on quotes, with session-median staleness within seconds. A median near 15
minutes is the delayed-entitlement signature. Unlike a gap, a delayed feed corrupts every row
silently, so the partition is quarantined and the run pages.

1. *Staleness is negative, so the comparison is on magnitude.* Session-median staleness on the
   lake's chain partitions runs -0.7 to -2.1 seconds. The vendor's quote stamp sits ahead of the
   fetch clock, which is ordinary clock skew between two machines. ``staleness_page_seconds`` is
   60, so ``median > 60`` could never fire against this feed, and a 15-minute delay arriving
   under the same skew would read as -900 rather than +900.
2. *The check is median-only and never judges a row.* The per-row maximum on QQQ 2026-09-14 is
   1,789,392,600 seconds, which is 56.7 years, and the other sessions top out between 22,674 and
   87,263 seconds. The design says session-median for this reason, and the tail is why that
   wording is load-bearing. Quotes are clean by comparison, median 0.0 seconds over a -1.2 to
   +2.5 range.

The flags themselves pass everywhere today. ``is_delayed`` is false on all 29,715,426 chain data
rows with no nulls, and ``realtime`` is true on all 2,436 quote data rows, so the staleness half
carries the whole risk. The partitions hold 29,718,244 rows in all, and the 2,818 that make up
the difference are gap rows, which carry a null flag because they carry no vendor observation.
``onboard`` asserts both flags once, at onboarding, and nothing has
watched them since.

**A missing signal is not a passing one.** A partition whose surface should carry the flag and
does not, or whose flag is null on a row, is judged rather than skipped. The column is in the
pinned schema for both surfaces, so its absence is drift rather than an old partition, and
answering drift with a pass is what fail-closed exists to prevent. ``schema_versions`` is the
ledger that says a column was never captured, and nothing in the lake's sealed rows reaches it.

**One page ships here, and the design gives the battery two.** This one is
``Delayed feed: partitions quarantined`` at priority 5, carrying the session-median staleness
and the partitions quarantined. The other is the battery's own nightly schema drift, which the
message table lists beside the parser's and compaction's and gives its own title,
``Schema drift: <field> missing`` or ``retyped``, fired once per field per day. That one belongs
to the schema-drift check rather than to this one, and marketlake #427 carries it, because
nothing here reads a payload's key set.

The page follows the once-on-the-transition rule the auth path, the watchdog and both shipped
schema-drift producers already carry. The transition here is the ledger's own: a partition
already quarantined under this check does not page again, because its entry is what says the
operator was already told.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from lake import battery_drift
from lake.alert import REFUSED, Message, Publisher
from lake.calendar import MARKET_TZ, Calendar, NotASession
from lake.capture_spans import CaptureSpan, CaptureSpans, CaptureSpansError, spans_path
from lake.config import GuardConstants
from lake.journal import ROW_KIND_COLUMN, ROW_KIND_DATA
from lake.manifest import (
    CLEAN_VERDICT,
    VERDICT_FIELD,
    ManifestError,
    append_line,
    is_quarantined,
    latest_quarantine_by_check,
    quarantine_path,
    record_partition,
    withholding,
)
from lake.paths import CHAINS, DATE_PREFIX, QUARANTINE_FILE, QUOTES, TICKER_PREFIX
from lake.security_master import ID_TYPE_TICKER, SecurityMaster, SecurityMasterError, master_path

# The surfaces that seal first and are flagged later. Bars and corporate actions gate before
# they land, in their own modules, so neither is judged here.
SEALED_SURFACES: tuple[str, ...] = (CHAINS, QUOTES)

# The source name this writer stamps on the ledger's manifest entry, the way ``actions`` stamps
# ``SWEEP_SOURCE`` on its own. It names the producer rather than the job, so a hand run and the
# 18:30 run leave the same entry.
BATTERY_SOURCE = "battery"

# The same, for the ledger's other writer. ``lake.signoff`` is the sign-off tool and it passes
# this to :func:`append_verdict`, so the manifest entry names which of the two writers refreshed
# the ledger. It is pinned here rather than there because ``append_verdict``'s default is the
# battery's and the pair only means anything read together.
SIGNOFF_SOURCE = "signoff"

# What a ledger entry says about who wrote it. #139's human-precedence rule turns on this, so
# the two spellings are pinned here and the sign-off tool reads them rather than minting a third.
PROVENANCE_BATTERY = "battery"
PROVENANCE_HUMAN = "human"

# The verdicts this writer may put in the ledger. ``clean`` is ``manifest.CLEAN_VERDICT``, which
# the reader treats as the one value that clears a partition. ``quarantined`` is the plain
# spelling of everything else, and it is spelled out rather than left to any non-clean value so
# that a reader of the ledger sees an intent rather than an accident.
QUARANTINED_VERDICT = "quarantined"
VERDICTS: tuple[str, ...] = (CLEAN_VERDICT, QUARANTINED_VERDICT)

# The findings a check can return that are not verdicts. ``insufficient_history`` is the
# design's own word, pinned at ``config.min_trailing_sessions``. ``out_of_scope`` covers the two
# classes the module docstring names. Neither ever reaches the ``verdict`` field, because
# ``manifest.is_quarantined`` withholds a partition on any value that is not ``clean``.
INSUFFICIENT_HISTORY = "insufficient_history"
OUT_OF_SCOPE = "out_of_scope"

# The third, and the one with no partition behind it. A session the lake never captured has no
# file to withhold, and ``loader._clear_partition`` raises ``PartitionAbsent`` before it
# consults the ledger, so a verdict written for that path would change no read. It could also
# never be cleared: there is no backfill, so the partition can never land and no later ``clean``
# can supersede the line, which would then sit in ``sweep.count_quarantined`` and
# ``dashboard._open_quarantines`` until a human signed off a file that does not exist.
MISSING_SESSION = "missing_session"
NON_VERDICTS: tuple[str, ...] = (INSUFFICIENT_HISTORY, OUT_OF_SCOPE, MISSING_SESSION)

# The checks' tokens, snake_case, named the way ``bars.CHECK_BAR_SPAN`` and
# ``actions.CHECK_DIVIDEND_CONSISTENCY`` are named. Each rides every entry its check writes, and
# #139's precedence rule compares it.
CHECK_ENTITLEMENT = "realtime_entitlement"
CHECK_CALENDAR_COVERAGE = "calendar_coverage"
CHECK_QUOTE_SANITY = "quote_sanity"
CHECK_ROW_COUNT_BAND = "row_count_band"

# What a partition-level answer carries instead of a check's name. Scope is a property of the
# partition rather than of any one check: capture either was running that day or it was not, and
# every check would give the same answer from the same two facts. So one finding carries it, and
# this token says so rather than naming whichever check happened to be asked first.
CHECK_SCOPE = "partition_scope"

# The delayed-feed page, from the design's message table. The event is the producer's name in
# front of the condition, matching ``compaction_schema_drift`` and ``parser_schema_drift``.
DELAYED_FEED_EVENT = "battery_delayed_feed"
DELAYED_FEED_TITLE = "Delayed feed: partitions quarantined"

# How many quarantined partitions the page names before it folds the rest into a count. A page
# reaches a phone, and the design's other pages leave per-ticker detail to stderr for the same
# reason. A vendor entitlement change hits every partition in the run at once, so the fact is one
# fact however many partitions carry it.
PAGE_PARTITION_CAP = 6

# The vendor's own entitlement flag on each surface, and what it must read. Chains carry
# ``is_delayed``, Schwab's ``isDelayed``, which must be false. Quotes carry ``realtime``, which
# must be true. The pair is a mapping rather than a branch so a third surface cannot be added
# here without saying what its flag is.
ENTITLEMENT_FLAGS: dict[str, tuple[str, bool]] = {
    CHAINS: ("is_delayed", False),
    QUOTES: ("realtime", True),
}

# The two stamps staleness is the difference of. Both are ISO-8601 strings on every row rather
# than timestamps, so the read parses them.
SNAP_TS = "snap_ts"
FETCH_TS = "fetch_ts"
VENDOR_QUOTE_TS = "vendor_quote_ts"

# The three the quote-sanity check orders, in the order the design writes them. Both surfaces
# carry all three in the pinned capture schema, so the check needs no per-surface mapping the
# way the entitlement flag does.
BID = "bid"
MARK = "mark"
ASK = "ask"
ORDERED_COLUMNS: tuple[str, ...] = (BID, MARK, ASK)

# The share of a partition's data rows that may fail ``bid <= mark <= ask`` before the partition
# is quarantined. The design says "rates within tolerance" rather than absence, and this is the
# number behind that wording.
#
# It is measured rather than guessed, and it is the *partition's* rate rather than a snapshot's.
# Crossed quotes are real and the rate moves by a factor of about 7,800 between sessions. The
# lake holds twelve in-scope data partitions, six chains and six quotes. The worst is SPY's
# 2026-09-16 chains at 7,909 rows of 5,307,030, which is 0.149 percent, against 1 row, 0 rows,
# 1 row and 50 rows on the other five chains partitions and 0 on all six quotes partitions. A
# check refusing any crossed quote would quarantine the lake's most recent complete session.
#
# A per-snapshot rate is what the measurement rules out, and it is the shape a reader reaches
# for first. SPY's worst single minute on 2026-09-16 is 1,753 crossed rows of 13,040, which is
# 13.4 percent, on a session whose own rate is 0.149 percent. Any per-snapshot threshold under
# that quarantines a session nothing is wrong with.
#
# Five percent is what that leaves, and the headroom it buys depends on how much of a session
# the partition holds. Crossing is concentrated at the close, so the cumulative rate on that
# same SPY partition climbs as the start moves later: 0.149 percent over the whole session,
# 1.96 percent from 19:45Z, 2.86 percent from 19:55Z and 3.87 percent from 20:10Z. Against a
# full session the margin is about thirty-three times. Against a partition that starts ten
# minutes before the close, which is what an afternoon onboarding produces, it is about 1.3
# times, so a close with twice this session's crossing quarantines that one partition. The
# price is named rather than hidden: one partition, once per ticker, cleared by a sign-off,
# against a threshold loose enough to admit the fault this exists to catch. That fault is not
# near five percent. A feed delivering bid and ask transposed reads near 100 percent, because
# an option quoted 0.00 by 0.05 crosses the moment the two are swapped.
QUOTE_SANITY_TOLERANCE = 0.05


class BatteryError(Exception):
    """Raised when the run cannot be attempted at all, rather than when a check fails."""


class PartitionUnreadable(BatteryError):
    """Raised when a sealed partition cannot be read for judgment.

    It is contained per partition by the walk, so one damaged file costs its own verdict and
    not the run. The partitions most likely to be unreadable are the ones a battery would
    quarantine, which is why this is a contained condition rather than a fatal one.
    """


@dataclass(frozen=True)
class Finding:
    """One check's answer about one partition.

    ``verdict`` is one of :data:`VERDICTS` when the check judged the partition, or one of
    :data:`NON_VERDICTS` when it did not. Only the first kind reaches the ledger, which is the
    whole reason the two live in one field rather than the check returning a bare boolean.

    ``computed`` and ``against`` are what the check measured and what it compared against, in
    the check's own units. They are what the nightly report shows an operator deciding whether
    to sign off, and they are the pair ``bars`` and ``actions`` already carry on a held finding.

    ``reason`` is the plain sentence, and it is what the ledger entry and the page both quote.
    """

    partition: str
    surface: str
    ticker: str
    day: date
    check: str
    verdict: str
    reason: str
    computed: float | None = None
    against: float | None = None

    @property
    def judged(self) -> bool:
        """Whether this finding carries a verdict the ledger can take."""
        return self.verdict in VERDICTS

    @property
    def withholds(self) -> bool:
        """Whether this finding would withhold its partition from a read."""
        return self.verdict == QUARANTINED_VERDICT


@dataclass(frozen=True)
class BatteryReport:
    """What one run did, in the shape the sweep's record and the digest both read.

    The counts are deliberately not ``report.PieceOutcome``'s. That record's fields are
    ``landed``, ``held``, ``unfiled``, ``unchanged`` and ``skipped``, which describe a ledger
    walk appending rows. A battery run appends only on a transition, so ``landed`` would read
    zero on a night that judged the whole lake and found it clean, which is the opposite of what
    happened. These names say what a battery does instead.

    ``deferred`` counts the human sign-offs this run re-observed and left standing, which is
    #139's rule producing a number rather than only a log line. It counts those alone.
    ``withheld`` counts the **partitions** this run passed a check on that no read returns
    afterwards. The hold is the walk's end state rather than the ledger's opening one, so it
    covers a partition another check has withheld since last night and one this same run just
    quarantined under a second check. It counts partitions rather than passes, because two
    checks passing one partition a third holds is one fact rather than two. Until #426 these
    counted
    as ``deferred`` and printed under a heading naming a human, so another check's hold inflated
    the sign-off count.

    ``released`` counts the partitions that rejoined the readable set this run. A release is
    otherwise invisible: a partition that reads again looks exactly like a partition nothing
    ever withheld.

    ``drift_paged`` is the schema-drift page's titles, and it is a field of its own rather than
    an extension of ``paged``. ``paged`` carries partition paths written by one check, and this
    page's unit is a surface and a half, so appending to it would put two kinds of string in one
    tuple. Neither render prints it, which is deliberate: :func:`render` states the rule that
    every count prints including the zeroes, so a *count* here would be a change to that function
    and to ``sweep.Nightly.render``. The finding reaches both through ``report`` instead.

    ``sessions_owed`` and ``sessions_missing`` are the coverage check's pair, and they are the
    one pair here not scoped by ``day``. :func:`coverage` says why. The denominator is carried
    because the check's correct answer against today's lake is that it found nothing, and a
    count of misses alone cannot tell that from a check that did not run.
    """

    judged: int = 0
    quarantined: int = 0
    cleared: int = 0
    insufficient_history: int = 0
    out_of_scope: int = 0
    deferred: int = 0
    withheld: int = 0
    released: int = 0
    unreadable: int = 0
    scope_unknown: int = 0
    sessions_owed: int = 0
    sessions_missing: int = 0
    appended: tuple[str, ...] = ()
    paged: tuple[str, ...] = ()
    drift_paged: tuple[str, ...] = ()
    report: tuple[str, ...] = ()
    findings: tuple[Finding, ...] = field(default=())

    @property
    def wrote_anything(self) -> bool:
        """Whether this run appended a ledger line."""
        return bool(self.appended)


# -- the ledger --------------------------------------------------------------


def build_entry(
    *,
    partition: str,
    verdict: str,
    check: str,
    observed_at: datetime,
    provenance: str = PROVENANCE_BATTERY,
    reason: str | None = None,
) -> dict:
    """One quarantine entry, assembled in the one place entries are assembled.

    Every field is keyword-only and checked, so a malformed entry cannot be built. That matters
    more here than it does for an ordinary record, because the reader fails closed: an entry
    whose ``verdict`` it does not recognise withholds its partition forever, and
    ``include_quarantined=True`` becomes the only way past it.

    ``verdict`` is checked against :data:`VERDICTS` rather than against ``CLEAN_VERDICT`` alone.
    Checking only for clean would admit ``insufficient_history`` as a quarantining value, which
    is the exact confusion :data:`NON_VERDICTS` exists to prevent, and the refusal names that
    case because it is the one a caller is most likely to reach for.

    ``observed_at`` is normalized to Eastern, matching ``report.write_nightly``'s ``at`` field,
    so a reader comparing a ledger line against that night's report file is comparing the same
    clock.
    """
    if verdict not in VERDICTS:
        extra = (
            f" {verdict!r} is a finding rather than a verdict, so it rides the nightly report"
            " instead."
            if verdict in NON_VERDICTS
            else ""
        )
        raise ValueError(f"verdict must be one of {list(VERDICTS)}, not {verdict!r}.{extra}")
    if provenance not in (PROVENANCE_BATTERY, PROVENANCE_HUMAN):
        raise ValueError(
            f"provenance must be {PROVENANCE_BATTERY!r} or {PROVENANCE_HUMAN!r}, not {provenance!r}"
        )
    if not check:
        raise ValueError("check is required: marketlake #139's precedence rule compares it")
    if not partition:
        raise ValueError("partition is required")
    entry: dict = {
        "partition": partition,
        VERDICT_FIELD: verdict,
        "check": check,
        "provenance": provenance,
        "observed_at": observed_at.astimezone(MARKET_TZ).isoformat(),
    }
    if reason is not None:
        entry["reason"] = reason
    return entry


def entry_line_count(lake_root: Path | str) -> int:
    """How many lines the quarantine ledger holds, parseable or not.

    This is the manifest entry's row count, and it counts what was written rather than what
    reads back. ``actions.entry_line_count`` gives the reason and it carries over exactly: a
    line the read cannot parse ends the read, so a parsed count can fall below the manifested
    one, and the manifest's row-count guard would then raise on every later append after that
    append had already written its line. A line count only ever grows.
    """
    path = quarantine_path(Path(lake_root))
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text().splitlines() if line.strip())


def append_verdict(
    lake_root: Path | str,
    entry: dict,
    *,
    observed_at: datetime,
    source: str = BATTERY_SOURCE,
) -> dict:
    """Append one verdict and refresh the ledger's manifest entry, inside one lock hold.

    This is the writer for a caller holding no lock, and it takes the lake-root ``flock``
    itself. ``manifest.py`` does not take it for a caller, and every other writer in the lake
    takes it at its own call site. The ledger line and the refreshed manifest entry go
    together, so a weekend verdict never leaves the Sunday scrub facing a sha nothing has
    caught up to. :func:`write_verdict` is the same pair of writes for a caller that already
    holds the lock, which is what :func:`judge` is, and the two levels are kept apart because
    ``lake.lock.lake_lock`` is a plain blocking ``LOCK_EX`` that blocks forever on re-entry.

    ``manifest.append_quarantine`` is deliberately not called here. It appends the line and
    returns, taking no lock and refreshing nothing, which is the shape ``manifest.py``'s own
    scrub-exclusion comment says is not enough for a ledger. Marketlake #139's sign-off tool is
    the other writer and owes the same two writes, so both meet at this function rather than at
    the bare append.

    ``source`` is what the refreshed manifest entry records about who wrote the ledger line,
    and it defaults to this module so the nightly run needs nothing. ``lake.signoff`` passes
    :data:`SIGNOFF_SOURCE`, because a human sign-off refreshed by a writer stamped ``battery``
    names the wrong producer, and every other writer in the lake stamps its own.
    """
    root = Path(lake_root)
    # Before the lock, not inside it. ``lake_lock`` opens ``manifest.jsonl`` with ``O_CREAT``,
    # which creates the file and never its directory, so acquiring against a lake root that
    # does not exist raises before any body could make it. Executed: without this line
    # ``append_verdict`` on an absent root raises ``FileNotFoundError`` where it used to write.
    quarantine_path(root).parent.mkdir(parents=True, exist_ok=True)

    # Local to keep this module free of the lock unless it writes, the same reason
    # ``actions``, ``onboard``, ``retire`` and ``schema_versions`` import it at the call site.
    from lake.lock import lake_lock

    with lake_lock(root):
        return write_verdict(root, entry, observed_at=observed_at, source=source)


def write_verdict(
    lake_root: Path | str,
    entry: dict,
    *,
    observed_at: datetime,
    source: str = BATTERY_SOURCE,
) -> dict:
    """The same two writes, for a caller already holding the lake-root lock.

    :func:`append_verdict` is this function under the lock and is what a caller holding none
    wants. This one exists because ``lake.lock.lake_lock`` is not re-entrant, verified by
    executing: a nested acquire in one process blocks forever. So a caller that has to read the
    ledger and append under one hold, which is :func:`judge`, cannot reach the pair through the
    locking wrapper.

    ``lake.occ_mapping`` is the worked precedent and the two levels here are its shape.
    ``write_mappings`` takes the lock itself, re-reads the master inside it, and calls the
    non-locking ``manifest.record_partition`` rather than the locking ``actions.append``, for
    the reason its docstring gives: the walk's own snapshot is as old as the walk.

    The pair stays in one function rather than being inlined at the second call site. The
    ledger line and its manifest entry going together is the rule, and a third place writing
    them is a third place that can write one without the other.
    """
    root = Path(lake_root)
    target = quarantine_path(root)
    # No ``mkdir`` here. ``target.parent`` is the lake root itself, and every caller has
    # already been through the lock, which opens the manifest beside it and cannot create the
    # directory either. :func:`append_verdict` makes it before acquiring and :func:`judge`
    # walks a lake that exists, so a ``mkdir`` on this path is unreachable rather than
    # defensive. The mutation review found it inert.
    append_line(target, entry)
    record_partition(
        root,
        QUARANTINE_FILE,
        source=source,
        rows=entry_line_count(root),
        fetched_at=observed_at.astimezone(MARKET_TZ).isoformat(),
    )
    return entry


def human_precedence(current: dict | None, check: str) -> bool:
    """Whether a human sign-off stands against a fresh verdict from ``check``.

    Marketlake #139's rule: "If it is a human sign-off row, a verdict from the same check never
    supersedes it." Sealed partitions are immutable, so the same check against the same
    partition is deterministically the same finding, and re-quarantining what a human just
    cleared would make sign-off a thing that lasts until 18:30.

    Only the *same* check defers. A different check finding a different fault is new
    information, and the human never spoke to it.
    """
    if current is None:
        return False
    return current.get("provenance") == PROVENANCE_HUMAN and current.get("check") == check


def _transition(current: dict | None, finding: Finding) -> bool:
    """Whether this finding changes **this check's** verdict on the partition.

    A check with no entry has said nothing yet, so a ``clean`` finding for one is a line that
    changes nothing. A check whose current entry already says what this finding says is the
    same news a second time. Everything else is a transition.

    The comparison is on the readability the reader computes rather than on the entry's whole
    shape, because ``manifest.is_quarantined`` is what a verdict's effect actually depends on,
    and an entry whose spelling drifted while its effect did not is still the same news.

    **The rule is per check rather than per partition, and that is marketlake #426.** The
    ledger resolves last entry wins within each check, so a ``clean`` line under one check can
    no longer bury another check's quarantine. #406 enforced that with a writer-side guard against
    the ledger's last entry, which covered two entries and not three: a third entry buried the
    second, and the second to clear released the partition with the first still failing.

    Structure replaced the guard rather than joining it. A guard that also *suppressed* the
    write, which is what #406's did, would now strand the partition: the passing check's own
    quarantine would stand forever with nothing able to clear it, so the partition would be
    withheld while no check failed.

    What a passing check owes instead is a report line, because the partition it just passed
    may still be withheld by somebody else. :func:`decide_partition` is where that is decided
    and ``judge`` is what prints it.
    """
    if current is None:
        return finding.withholds
    return is_quarantined(current) != finding.withholds


@dataclass(frozen=True)
class Decision:
    """What one finding does to the ledger, and what still withholds its partition.

    ``wrote`` and ``deferred_to_human`` are exclusive. ``holders`` is filled for a finding that
    did not withhold, and names every check still holding the partition after this finding is
    applied, so a line that cleared its own check can still say the partition does not read.

    A holder carries its check as the entry spells it, including ``None`` for an entry that
    names none. Rendering is the caller's, because ``str()`` here would put the word "None"
    into the nightly report where ``dashboard._open_quarantines`` shows a dash.
    """

    finding: Finding
    wrote: bool = False
    deferred_to_human: bool = False
    holders: tuple[str | None, ...] = ()


@dataclass(frozen=True)
class PartitionOutcome:
    """Every decision for one partition in one run, and where the partition ended up.

    ``holders`` names every check still withholding the partition once every finding has been
    applied. It is not :attr:`Decision.holders` under another name: that one is the state as
    each finding landed, which is what a per-finding answer needs, and this one is the state at
    the end of the walk. They differ the moment a later check quarantines what an earlier one
    passed, and only this one can say so.
    """

    decisions: tuple[Decision, ...] = ()
    released: bool = False
    holders: tuple[str | None, ...] = ()


def decide_partition(
    by_check: dict[str, dict] | None, findings: Sequence[Finding]
) -> PartitionOutcome:
    """Every finding's fate for one partition, decided from values alone.

    ``by_check`` is this partition's entry per check, which is what
    ``manifest.latest_quarantine_by_check`` returns for it. The findings are what this run's
    checks answered about it.

    **The state is carried forward as lines land, rather than read once.** Two checks clearing
    in one walk both change what withholds the partition, so a decision made against the state
    the walk started from would report the partition still withheld by a check that cleared a
    moment earlier, and the release would never be reported at all.

    This is a pure function for a reason that is not tidiness. ``judge`` hands it one
    partition's findings, which is two on a quotes partition and three on a chains one, and a
    unit test drives it here with as many checks as it likes and with ledger states a lake
    would take a fixture to build. Marketlake #407 is what made the second finding real: this
    said "one check exists, so no run can yet produce two findings for one partition" until
    that landed, and ``judge`` carried the same sentence as a comment. One of the two was
    corrected and this one was not, which is the failure a completeness review exists to
    catch.
    """
    state = dict(by_check or {})
    held_before = bool(withholding(state))
    decisions: list[Decision] = []

    for finding in findings:
        if not finding.judged:
            continue
        current = state.get(finding.check)
        if human_precedence(current, finding.check):
            decisions.append(Decision(finding, deferred_to_human=True))
            continue
        wrote = _transition(current, finding)
        if wrote:
            # The projection carries ``partition`` plus the two fields resolution reads,
            # ``verdict`` and ``check``, and no stamp. A stamp
            # here would be a second clock beside the one ``judge`` writes with, and nothing
            # downstream of this function reads one: ``holders`` hands back check names.
            state[finding.check] = {
                "partition": finding.partition,
                VERDICT_FIELD: finding.verdict,
                "check": finding.check,
            }
        holders = (
            () if finding.withholds else tuple(entry.get("check") for entry in withholding(state))
        )
        decisions.append(Decision(finding, wrote=wrote, holders=holders))

    held_after = withholding(state)
    return PartitionOutcome(
        decisions=tuple(decisions),
        released=held_before and not held_after,
        holders=tuple(entry.get("check") for entry in held_after),
    )


# -- scope -------------------------------------------------------------------


@dataclass(frozen=True)
class SealedPartition:
    """One sealed partition the walk found, with everything needed to judge it."""

    path: Path
    surface: str
    ticker: str
    day: date

    @property
    def relative(self) -> str:
        """The lake-relative path, which is the ledger's key.

        Built from the parts rather than by relative_to, so the spelling is the one
        ``LakePaths.partition_path`` produces and not whatever case the filesystem answered
        with. ``loader.PartitionAbsent`` says why that matters: on macOS a case-mismatched
        spelling opens the partition while the quarantine lookup misses, which turns the guard
        from fail closed into fail open.

        It calls :func:`partition_key`, which is the same spelling built for a partition that
        does not exist. Two f-strings sharing the constants agree until somebody edits one, and
        that function's docstring claims they cannot drift.
        """
        return partition_key(self.surface, self.ticker, self.day)


def sealed_partitions(lake_root: Path | str, *, day: date | None = None) -> list[SealedPartition]:
    """Every sealed chains and quotes partition, or one day's, in a stable order.

    ``day=None`` walks the whole lake, which is what a first run and a hand run both want. The
    18:30 job passes the session it is about, because a night's run is about that night and
    walking the rest costs the whole lake's Parquet for verdicts that cannot have changed. They
    cannot: a sealed partition is immutable, there is no backfill, and :func:`trailing_medians`
    takes only sessions before the one it judges, so a whole-lake re-run is deterministic.

    A name that does not parse as a date is skipped rather than raising. The walk is over a
    directory the operator can put a file in, and one stray name must not cost the run.
    """
    root = Path(lake_root)
    found: list[SealedPartition] = []
    for surface in SEALED_SURFACES:
        for ticker_dir in sorted((root / surface).glob(f"{TICKER_PREFIX}*")):
            ticker = ticker_dir.name[len(TICKER_PREFIX) :]
            for path in sorted(ticker_dir.glob(f"{DATE_PREFIX}*.parquet")):
                stamp = path.stem[len(DATE_PREFIX) :]
                try:
                    when = date.fromisoformat(stamp)
                except ValueError:
                    continue
                if day is not None and when != day:
                    continue
                found.append(SealedPartition(path=path, surface=surface, ticker=ticker, day=when))
    return found


class ScopeUnknown(BatteryError):
    """Raised when the reference files cannot say whether capture was running.

    It is not the same answer as out of scope and must never be reported as one. Out of scope
    is a fact about the lake: capture was not running, so nothing in the partition is evidence.
    This is the absence of that fact, and a delayed feed passing under it would pass in silence.
    """


@dataclass(frozen=True)
class Reference:
    """The two reference files every scope question resolves against, read once per run.

    ``judge`` reads them and hands them down. Two readings of the same file inside one run can
    disagree, and they answer different questions from the same rows, so the second reading is
    the one that would be wrong without anything saying so.

    The two questions are not the same shape, which is why both halves are here.
    :func:`capture_spans_by_ticker` asks which spans a partition *directory* falls under, keyed
    by the spelling the walk found on disk. :func:`coverage` asks the opposite question, which
    sessions an *instrument* was owed a partition for, and a ticker that captured nothing at all
    has no directory to be found under. That is marketlake #431's class of miss, stated from the
    other side: an enumeration that starts from what the lake holds cannot see what it never
    wrote.
    """

    master: SecurityMaster
    spans: CaptureSpans


def read_reference(lake_root: Path | str) -> Reference:
    """Both reference files, or :class:`ScopeUnknown` naming which one could not be read.

    A missing or unreadable file raises rather than returning an empty mapping, for the reason
    :func:`capture_spans_by_ticker` gives: an empty one reaches :func:`in_scope` as "capture was
    not running", which is a fact this run does not have.
    """
    root = Path(lake_root)
    try:
        master = SecurityMaster.read(master_path(root))
    except FileNotFoundError as exc:
        raise ScopeUnknown(f"no security master at {master_path(root)}") from exc
    except SecurityMasterError as exc:
        raise ScopeUnknown(f"security master unreadable: {exc}") from exc
    try:
        spans = CaptureSpans.read(spans_path(root))
    except FileNotFoundError as exc:
        raise ScopeUnknown(f"no capture spans at {spans_path(root)}") from exc
    except CaptureSpansError as exc:
        raise ScopeUnknown(f"capture spans unreadable: {exc}") from exc
    return Reference(master=master, spans=spans)


def capture_spans_by_ticker(
    lake_root: Path | str,
    tickers: Iterable[str],
    *,
    reference: Reference | None = None,
) -> dict[str, tuple[CaptureSpan, ...]]:
    """Each partition directory's capture spans, keyed by the directory's own ticker spelling.

    **The instrument is found across every spelling it ever carried, not at a point in time.**
    Two failures sit on either side of a point-in-time lookup and this avoids both.

    1. Resolving as of the judged day loses the clamp on any day before the master's
       ``valid_from``. That was marketlake #405 on the dashboard: the ticker dropped out of the
       mapping and every minute rendered as missing. The dashboard's clamp asks the same
       spelling question this one does now, through ``SecurityMaster.instruments_named``.
    2. Resolving as of the run date loses it after a rename. ``SecurityMaster.remap`` closes the
       old mapping, so every partition still sitting under the old ``ticker=`` directory
       resolves to nothing and goes unjudged, silently and permanently.

    The second is not hypothetical here. ``continuity`` states the same rule for the OCC side,
    "Not ``SecurityMaster.resolve``", and ``b21edfb`` shipped that fix for the OI view this
    week. A directory name is a spelling the lake once wrote, so the question it asks is which
    instrument ever went by it, and that is answered from the mapping rows rather than from a
    date.

    A spelling two instruments have both carried is disambiguated by the day, because then the
    directory alone genuinely does not say which. That case raises rather than guessing.

    **A reference file that is missing or unreadable raises.** It does not return an empty
    mapping. An empty mapping reaches :func:`in_scope` as "capture was not running", which is a
    fact this function does not have, and a delayed feed would then pass under a reason that
    says something untrue. ``reference`` is the run's own read, and leaving it out takes one
    here, which is what a caller with no run behind it wants.
    """
    reference = read_reference(lake_root) if reference is None else reference
    master, spans = reference.master, reference.spans

    by_spelling: dict[str, set[int]] = {}
    for mapping in master.mappings:
        if mapping.id_type != ID_TYPE_TICKER:
            continue
        by_spelling.setdefault(mapping.id_value, set()).add(mapping.instrument_id)

    result: dict[str, tuple[CaptureSpan, ...]] = {}
    for ticker in tickers:
        instruments = by_spelling.get(ticker, set())
        if not instruments:
            raise ScopeUnknown(f"the security master knows no instrument spelled {ticker!r}")
        if len(instruments) > 1:
            raise ScopeUnknown(
                f"{ticker!r} names {len(instruments)} instruments in the master, so the "
                "partition directory does not say which"
            )
        found = tuple(spans.spans_of(next(iter(instruments))))
        if not found:
            raise ScopeUnknown(f"{ticker!r} resolves to an instrument with no capture span")
        result[ticker] = found
    return result


def in_scope(partition: SealedPartition, spans: tuple[CaptureSpan, ...]) -> bool:
    """Whether capture was running for any part of the partition's day.

    A partition whose whole day lies outside every span is out of scope. Nothing in it is
    evidence about the feed, because the feed was not being read. SPY's 2026-09-02 chains
    partition is the live case: it holds a day-one probe from six days before either span opens,
    and every check measured against it fails.

    A day that overlaps a span at all is in scope, including the onboarding day itself, whose
    morning is outside the span and whose afternoon is inside it. Judging the whole day is right
    there, because the rows the partition holds are the ones capture wrote.

    **An unknown ticker is out of scope, not in it.** A ticker with no spans has nothing saying
    capture ever ran for it, and a battery is the wrong place to guess. The dashboard makes the
    opposite call for the same absence, because a panel that refuses to render is worse than one
    rendering without a clamp, while a verdict written without a clamp is worse than no verdict.
    """
    if not spans:
        return False
    start = datetime.combine(partition.day, datetime.min.time(), tzinfo=MARKET_TZ)
    end = start + timedelta(days=1)
    return any(_overlaps(span, start, end) for span in spans)


def _session_bounds(calendar: Calendar, day: date) -> tuple[datetime, datetime] | None:
    """The session's open and option close for ``day``, or ``None`` off a session.

    ``None`` widens the staleness median to every data row. A sealed partition on a day the
    calendar calls closed has no session to take a median over, and the alternative, refusing
    it, would quarantine a partition for a calendar disagreement rather than for a feed fault.
    The flag half still runs, which is the half that could see a delayed feed on such a day.
    """
    try:
        return calendar.session_open(day), calendar.option_close(day)
    except NotASession:
        return None


def _overlaps(span: CaptureSpan, start: datetime, end: datetime) -> bool:
    """Whether a capture span covers any instant in ``[start, end)``."""
    if not isinstance(span.start, datetime) or span.start.utcoffset() is None:
        return False
    if span.start >= end:
        return False
    if span.end is None:
        return True
    if not isinstance(span.end, datetime) or span.end.utcoffset() is None:
        return False
    return span.end > start


# -- the real-time entitlement check -----------------------------------------


@contextmanager
def _contained(partition: SealedPartition):
    """Turn any failure reading one partition into :class:`PartitionUnreadable`.

    **A column's type is not checked by reading its schema.** Every read here guards
    ``pq.read_schema`` and ``pq.read_table`` and then hands the columns to an Arrow kernel,
    which raises ``ArrowNotImplementedError`` when a pinned column arrives retyped: a ``bid``
    the vendor started sending as a string has no ``less_equal`` against a ``mark`` that is
    still a double. That raise is outside the walk's per-partition containment, so one drifted
    column on one partition costs every partition's verdict for the whole lake that night.

    Retyping a pinned column is drift, which is the condition the battery exists to notice, so
    the partitions most likely to raise here are the ones that most need judging. That is
    :class:`PartitionUnreadable`'s own argument for being contained rather than fatal, and this
    is what puts the kernels inside it.
    """
    try:
        yield
    except PartitionUnreadable:
        raise
    except Exception as exc:  # noqa: BLE001 - one damaged partition must not cost the run
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc


@dataclass(frozen=True)
class Entitlement:
    """What one partition's rows say about the feed's entitlement.

    ``rows`` counts data rows alone. Gap rows carry no vendor observation, so a partition of
    them says nothing about the feed, which is the second out-of-scope class.

    ``flag_violations`` counts data rows whose entitlement flag reads the wrong way or reads
    null. Null is counted as a violation rather than skipped: the column is in the pinned schema
    for both surfaces, so a null in it is the vendor declining to say, and a check that answers
    "declined to say" with a pass is not failing closed.

    ``session_rows`` counts the data rows whose ``snap_ts`` falls inside the trading session.
    ``median_staleness`` is fetch time minus vendor quote time, in seconds, across those rows
    alone. ``None`` means none of them carried both stamps.

    **The staleness half is session-only and the flag half is not**, which is the design's own
    split: the flags "must show real-time on every snapshot", while staleness is the *session*
    median. The reason is mechanical. A vendor's last-quote stamp freezes when the market is
    closed while ``fetch_ts`` keeps moving, so an overnight cycle carries hours of staleness on
    a feed that is real-time by every other measure. Both 2026-09-16 chain partitions in the
    live lake hold such rows, about 12,000 each at 03:25 UTC, which is 0.24 percent of the
    partition today. A session that captured only an overnight cycle would be all of it, and
    the check would quarantine a partition whose feed was fine.
    """

    rows: int
    flag_present: bool
    flag_violations: int
    median_staleness: float | None
    session_rows: int = 0


def read_entitlement(
    partition: SealedPartition, bounds: tuple[datetime, datetime] | None = None
) -> Entitlement:
    """Read one partition's entitlement evidence, two columns plus the flag.

    Only four columns are read, so the cost is the parse rather than the file. The two stamps
    are ISO strings rather than timestamps, and that parse is where the time goes: a whole-lake
    duckdb scan of the difference over 29,718,244 chain rows took about 4 seconds unconstrained
    and about 11 at ``dashboard.open_lake_connection``'s ``threads=2`` and ``memory_limit=2GB``.
    This module is not the dashboard and takes neither cap.

    The median is taken over the session's rows rather than per snapshot. The design says
    session-median and the tail is why per-row would not do: the maximum on QQQ 2026-09-14 is
    1,789,392,600 seconds, 56.7 years, so any per-row rule would quarantine a healthy feed.
    ``bounds`` is
    that session, open through option close, and ``None`` widens the median to every data row,
    which only a caller that has no calendar should pass.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    flag, _ = ENTITLEMENT_FLAGS[partition.surface]
    try:
        available = set(pq.read_schema(partition.path).names)
    except Exception as exc:  # noqa: BLE001 - one damaged file must not cost the run
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc

    wanted = [ROW_KIND_COLUMN, SNAP_TS, FETCH_TS, VENDOR_QUOTE_TS]
    flag_present = flag in available
    if flag_present:
        wanted.append(flag)
    missing = [name for name in wanted if name not in available]
    if missing:
        raise PartitionUnreadable(f"{partition.relative}: missing {', '.join(sorted(missing))}")

    try:
        table = pq.read_table(partition.path, columns=wanted)
    except Exception as exc:  # noqa: BLE001 - same reason as above
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc

    table = table.filter(pc.equal(table[ROW_KIND_COLUMN], ROW_KIND_DATA))
    rows = table.num_rows
    if rows == 0:
        return Entitlement(
            rows=0, flag_present=flag_present, flag_violations=0, median_staleness=None
        )

    violations = 0
    if flag_present:
        _, wanted_value = ENTITLEMENT_FLAGS[partition.surface]
        with _contained(partition):
            agreeing = pc.sum(pc.equal(table[flag], wanted_value)).as_py() or 0
        violations = rows - agreeing

    in_session = _within(table, partition, bounds)
    return Entitlement(
        rows=rows,
        flag_present=flag_present,
        flag_violations=violations,
        median_staleness=_median_staleness(in_session, partition),
        session_rows=in_session.num_rows,
    )


def _within(table, partition: SealedPartition, bounds: tuple[datetime, datetime] | None):
    """The data rows whose ``snap_ts`` falls inside the session, or all of them without bounds.

    ``snap_ts`` is the minute slot a cycle fired for, which is the lake's own statement of when
    the row was taken. ``fetch_ts`` would answer nearly the same question and is the wrong one:
    it is the clock this check is measuring against, so filtering on it would let a skewed
    clock decide which rows judge the skew.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if bounds is None:
        return table
    opened, closed = bounds
    try:
        taken = pc.cast(table[SNAP_TS], pa.timestamp("us", tz="UTC"))
    except pa.ArrowInvalid as exc:
        raise PartitionUnreadable(
            f"{partition.relative}: {SNAP_TS} will not parse as a zone-aware timestamp: {exc}"
        ) from exc
    inside = pc.and_(
        pc.greater_equal(taken, pa.scalar(opened, type=taken.type)),
        pc.less_equal(taken, pa.scalar(closed, type=taken.type)),
    )
    return table.filter(inside)


def _median_staleness(table, partition: SealedPartition) -> float | None:
    """The median of ``fetch_ts`` minus ``vendor_quote_ts``, in seconds, or ``None``.

    Both stamps are ISO-8601 strings on every row rather than timestamps, so the difference is
    two parses per row and the parse is the whole cost. It runs in Arrow rather than row by row
    in Python, and the gap is not small: on SPY's 2026-09-16 chains partition, 5,307,030 rows,
    the Arrow path takes 0.36 seconds against 2.75 for the Python loop. That is the difference
    between the 18:30 job spending a second on this and spending eight.

    ``pc.quantile`` at ``q=0.5`` is exact rather than approximate, and the distinction is worth
    the word. ``pc.approximate_median`` is a t-digest and runs no faster here, 0.53 seconds
    against 0.36, while answering -1.69 where the exact median is -1.697566. A guard compared
    against a 60-second threshold would not care about that gap today, and a guard whose answer
    depends on where its estimator's buckets fell is a guard nobody can reproduce from the rows.

    A row missing either stamp has no defined staleness. The cast maps null to null and the
    quantile skips nulls, so those rows drop out without being counted as zero, which would drag
    the median toward a pass. A partition where every row lacks a stamp returns ``None``, and
    :func:`judge_entitlement` treats that as its own quarantining condition rather than a pass.

    A stamp that is not parseable ISO-8601, including one carrying no zone offset, raises rather
    than dropping the row. Both columns are pinned strings in the capture schema, so a value that
    will not parse is drift in a column the check depends on, and the partition is reported
    unreadable rather than judged on the rows that happened to survive.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    try:
        fetched = pc.cast(table[FETCH_TS], pa.timestamp("us", tz="UTC"))
        quoted = pc.cast(table[VENDOR_QUOTE_TS], pa.timestamp("us", tz="UTC"))
    except pa.ArrowInvalid as exc:
        raise PartitionUnreadable(
            f"{partition.relative}: {FETCH_TS} or {VENDOR_QUOTE_TS} will not parse as a "
            f"zone-aware timestamp: {exc}"
        ) from exc

    seconds = pc.divide(pc.cast(pc.microseconds_between(quoted, fetched), pa.float64()), 1e6)
    if seconds.null_count == len(seconds):
        return None
    return pc.quantile(seconds, q=0.5, interpolation="midpoint")[0].as_py()


def judge_entitlement(
    partition: SealedPartition, evidence: Entitlement, guards: GuardConstants
) -> Finding:
    """One partition's entitlement verdict, from the evidence and the machine's threshold.

    Four conditions quarantine, and each is a different way for the feed to be delayed or for
    the lake to be unable to tell.

    1. The flag column is absent from a surface whose pinned schema carries it. That is drift
       rather than an old partition, and answering drift with a pass is what fail closed exists
       to prevent.
    2. Any data row's flag reads the wrong way or reads null. The design says the flags "must
       show real-time on **every** snapshot", so the threshold is one row and not a rate. It can
       be that strict because it is the vendor's own statement rather than a measurement:
       ``is_delayed`` is false on all 29,718,244 chain data rows the lake holds with no nulls,
       and ``realtime`` is true on all 2,436 quote data rows.
    3. No *session* row carried both stamps, so the staleness half could not run at all. A
       partition with no session row at all is a different answer and comes back out of scope,
       because a day of overnight cycles recorded no session rather than a bad one.
    4. The session-median staleness exceeds ``staleness_page_seconds`` in **magnitude**. The
       comparison is on magnitude because the real median is negative, -0.7 to -2.1 seconds on
       chains, from ordinary clock skew between the vendor's clock and this machine's. A signed
       comparison against 60 could never fire, and a genuine 15-minute delay arriving under the
       same skew reads as -900 rather than +900.

    A partition with no data rows never reaches here. :func:`judge` returns it out of scope
    first, because a day of gap rows is a correctly-recorded outage.

    The flag half reads every data row and the staleness half reads only the session's, which
    is the design's own split and :class:`Entitlement` carries the reason.
    """
    flag, wanted = ENTITLEMENT_FLAGS[partition.surface]
    limit = float(guards.staleness_page_seconds)

    if not evidence.flag_present:
        return _entitlement_finding(
            partition,
            QUARANTINED_VERDICT,
            f"{partition.surface} carries no {flag} column, so entitlement cannot be verified",
        )
    if evidence.flag_violations:
        return _entitlement_finding(
            partition,
            QUARANTINED_VERDICT,
            f"{evidence.flag_violations} of {evidence.rows} data rows do not carry "
            f"{flag}={wanted!r}",
            computed=float(evidence.flag_violations),
            against=0.0,
        )
    if evidence.session_rows == 0:
        return _entitlement_finding(
            partition,
            OUT_OF_SCOPE,
            "no data row falls inside the session, so the partition carries no session "
            "observation to take a median over",
        )
    if evidence.median_staleness is None:
        return _entitlement_finding(
            partition,
            QUARANTINED_VERDICT,
            f"no session data row carries both {FETCH_TS} and {VENDOR_QUOTE_TS}, "
            "so staleness cannot be measured",
        )
    if abs(evidence.median_staleness) > limit:
        return _entitlement_finding(
            partition,
            QUARANTINED_VERDICT,
            f"session-median staleness is {evidence.median_staleness:.1f}s, "
            f"over the {limit:.0f}s limit in magnitude",
            computed=evidence.median_staleness,
            against=limit,
        )
    return _entitlement_finding(
        partition,
        CLEAN_VERDICT,
        f"{flag}={wanted!r} on all {evidence.rows} data rows, session-median staleness "
        f"{evidence.median_staleness:.1f}s within {limit:.0f}s",
        computed=evidence.median_staleness,
        against=limit,
    )


def _finding(
    partition: SealedPartition,
    check: str,
    verdict: str,
    reason: str,
    *,
    computed: float | None = None,
    against: float | None = None,
) -> Finding:
    """One finding, with the partition's four identifying fields spliced in.

    Every check builds its findings here, so the partition's spelling reaches the ledger one way
    and the ``check`` token is never left to a default.
    """
    return Finding(
        partition=partition.relative,
        surface=partition.surface,
        ticker=partition.ticker,
        day=partition.day,
        check=check,
        verdict=verdict,
        reason=reason,
        computed=computed,
        against=against,
    )


def _entitlement_finding(
    partition: SealedPartition,
    verdict: str,
    reason: str,
    *,
    computed: float | None = None,
    against: float | None = None,
) -> Finding:
    """One entitlement finding."""
    return _finding(
        partition, CHECK_ENTITLEMENT, verdict, reason, computed=computed, against=against
    )


# -- trading-calendar coverage -----------------------------------------------


@dataclass(frozen=True)
class Coverage:
    """What the coverage check asked and what it found.

    ``owed`` is the denominator: every surface, ticker and session a capture span says a
    partition was owed for. It is carried beside the misses because the check's correct answer
    against today's lake is that it found nothing, and a report that printed only misses would
    render "it found nothing" and "it did not run" the same way. :func:`render` already prints
    every count including the zeroes for that reason.

    ``missing`` holds one finding per owed partition the lake does not have. Each carries
    :data:`MISSING_SESSION`, which is not a verdict, so none of them reaches the ledger.

    ``unnamed`` holds one sentence per instrument whose owed sessions could not be named,
    because the capture spans and the security master disagree about it. It is reported and
    counted rather than raised. Raising took the whole run down: every other ticker's partitions
    went unjudged and the delayed-feed page could not fire, over an instrument that owns no
    partition and changes no read. ``capture_spans.build_from_master`` reaches this state on
    purpose, opening a span for every instrument in the master when the roster cannot be read,
    and ``SecurityMaster.register`` takes no ticker at all.
    """

    owed: int = 0
    missing: tuple[Finding, ...] = ()
    unnamed: tuple[str, ...] = ()


def coverage(
    lake_root: Path | str,
    reference: Reference,
    calendar: Calendar,
    *,
    now: datetime,
) -> Coverage:
    """Every session a capture span owed a partition for, and the ones with no partition.

    **This walks the whole span and ignores the run's ``day``.** ``sweep`` calls :func:`judge`
    with tonight's session, which is right for a check that reads a partition's rows. It is
    exactly wrong here: a session with no partition is a session on which nothing ran, so a
    coverage check scoped to tonight can never see the night it missed. The widening is free,
    because this stats the filesystem and opens no Parquet.

    **It enumerates instruments, not partition directories.** A ticker that captured nothing at
    all has no ``ticker=`` directory, so a walk of the lake cannot see it. That is marketlake
    #431's class of miss from the other side. :class:`Reference` carries both halves and its
    docstring gives the reason.

    **A session is owed a partition when the span covers part of the session**, rather than part
    of the calendar day. :func:`in_scope` deliberately widens to the whole day, because the rows
    an existing partition holds are the ones capture wrote and the onboarding day's morning
    falls outside the span. The question here is the other one, whether capture could have
    written anything at all, and a span opening after the option close covers none of the
    session while still touching the day.

    **And only once compaction has run.** ``session.COMPACTION_DELAY`` is the fifteen minutes
    past the option close at which the partition is written, so a session whose file is still
    being built is not yet missing. The moment is derived from the calendar and that constant
    rather than pinned here, because ``tests/unit/test_seam_calendar.py`` fails the build on a
    session-time literal anywhere under ``src/lake`` outside ``calendar.py``.

    **A ticker is looked for under every spelling it ever carried.** Resolving as of the judged
    day loses the ticker before the master's ``valid_from``, which was marketlake #405, and
    resolving as of the run date loses it after a rename. Either way a partition that exists
    would be reported missing. The name the finding is written under is the spelling valid on
    that day, falling back to the first the instrument ever had.
    """
    from lake.session import COMPACTION_DELAY

    root = Path(lake_root)
    master, spans = reference.master, reference.spans

    spellings: dict[int, tuple[str, ...]] = {}
    unnamed: list[str] = []
    for instrument_id in sorted(spans.instrument_ids()):
        found = sorted(
            {
                mapping.id_value
                for mapping in master.mappings
                if mapping.id_type == ID_TYPE_TICKER and mapping.instrument_id == instrument_id
            }
        )
        if not found:
            unnamed.append(
                f"battery: instrument {instrument_id} has a capture span and no ticker in "
                "the master, so the sessions it owed cannot be named"
            )
            continue
        spellings[instrument_id] = tuple(found)

    # Keyed so two spans covering one day owe one partition rather than two.
    owed: set[tuple[str, int, date]] = set()
    for span in spans:
        if span.instrument_id not in spellings:
            continue
        horizon = now if span.end is None else min(span.end, now)
        day = span.start.astimezone(MARKET_TZ).date()
        last = horizon.astimezone(MARKET_TZ).date()
        while day <= last:
            bounds = _session_bounds(calendar, day)
            if bounds is not None:
                opened, closed = bounds
                if _overlaps(span, opened, closed) and closed + COMPACTION_DELAY <= now:
                    surfaces = SEALED_SURFACES if span.options else (QUOTES,)
                    for surface in surfaces:
                        owed.add((surface, span.instrument_id, day))
            day += timedelta(days=1)

    missing: list[Finding] = []
    for surface, instrument_id, day in sorted(owed, key=lambda key: (key[0], key[1], key[2])):
        names = spellings[instrument_id]
        if any(_partition_file(root, surface, name, day).is_file() for name in names):
            continue
        named = master.symbol_at(instrument_id, day) or names[0]
        missing.append(
            Finding(
                partition=partition_key(surface, named, day),
                surface=surface,
                ticker=named,
                day=day,
                check=CHECK_CALENDAR_COVERAGE,
                verdict=MISSING_SESSION,
                reason=(
                    f"{day.isoformat()} is a session inside {named}'s capture span and the "
                    f"lake holds no {surface} partition for it"
                ),
            )
        )
    return Coverage(owed=len(owed), missing=tuple(missing), unnamed=tuple(unnamed))


def _partition_file(root: Path, surface: str, ticker: str, day: date) -> Path:
    """Where a sealed partition would be, built the way the ledger's key is built."""
    return root / partition_key(surface, ticker, day)


def partition_key(surface: str, ticker: str, day: date) -> str:
    """The lake-relative path a partition is keyed under, from its three parts.

    :attr:`SealedPartition.relative` builds the same string for a partition the walk found. This
    is the version for one that does not exist, so the two spellings cannot drift apart.
    """
    return f"{surface}/{TICKER_PREFIX}{ticker}/{DATE_PREFIX}{day.isoformat()}.parquet"


def coverage_line(found: Coverage) -> str:
    """The one line coverage puts in the nightly report, whatever it found.

    **One line, counted, and it names no partition.** Three rules meet here and each rules out
    a shape a reader reaches for first.

    1. *Never one line per miss.* A missing session is permanent, so this repeats every night
       for ever. ``sweep`` already decided that case in writing for the bars walk's
       unresolvable ticker-days: rendered in full they walk the nightly report into
       ``digest_body``'s 1000-byte cap, "and what falls off the end first is the battery's own
       census", and a line that silences the check above it is worse than no line.
    2. *No partition names, not even a capped list.* ``sweep``'s own digest test pins the
       contract: the digest carries counts and never a list of findings, because a digest that
       listed them "would sit under the cap on every night anyone tested it and over the cap on
       the night that mattered". The report list is what the digest is built from, so a name here
       reaches it. The dates are what an operator needs to act, and :func:`render` names each
       missing partition on the job's own stdout.
    3. *No second ``": "``.* ``report.redacted`` drops everything past a line's second field
       before it reaches the report file or the digest, so that an exception's message cannot
       carry an absolute path off the capture machine. A line spelled
       ``battery: coverage: 3 of 27`` arrives as ``battery: coverage`` with every number gone.

    It prints on a night it finds nothing, because the battery's census is on stdout and not in
    the report file, so a check reporting only misses cannot be told from one that did not run.
    """
    if not found.missing:
        return f"battery: calendar coverage, {found.owed} owed sessions, all present"
    days = sorted({finding.day for finding in found.missing})
    span = (
        days[0].isoformat()
        if len(days) == 1
        else f"{days[0].isoformat()} to {days[-1].isoformat()}"
    )
    return (
        f"battery: calendar coverage, {len(found.missing)} of {found.owed} owed sessions have "
        f"no partition, over {len(days)} session{'s' if len(days) != 1 else ''}, {span}"
    )


# -- quote sanity ------------------------------------------------------------


@dataclass(frozen=True)
class QuoteOrder:
    """What one partition's rows say about ``bid <= mark <= ask``.

    ``rows`` counts data rows. ``unordered`` counts the ones that do not satisfy it, which
    includes every row where any of the three is null: a comparison against null is null, and a
    row whose ordering cannot be evaluated has not passed it. That is the same reading
    :class:`Entitlement` gives a null flag, and it costs nothing today, because ``bid``, ``ask``
    and ``mark`` are never null on a data row inside a capture span. The only nulls in the lake
    are the two rows of SPY's 2026-09-02 chains partition, which :func:`in_scope` already
    refuses to judge.

    ``absent`` names the columns the partition does not carry. All three are in the pinned
    capture schema for both surfaces, so a missing one is drift rather than an old partition,
    and the check answers it the way the entitlement flag's absence is answered.
    """

    rows: int
    unordered: int
    absent: tuple[str, ...] = ()

    @property
    def rate(self) -> float:
        """The share of data rows that are not ordered, or 0.0 for a partition with none."""
        return self.unordered / self.rows if self.rows else 0.0


def read_quote_order(partition: SealedPartition) -> QuoteOrder:
    """Read one partition's ordering evidence, three columns plus the row kind.

    **The two halves of the design's wording are one predicate.** "Bid ≤ mid ≤ ask" fails on a
    row whose mark sits outside the spread and on a crossed quote alike, because a crossed
    quote's interval is empty and admits no mark at all. The lake says the two are the same
    rows in both directions: across all thirteen of the lake's partitions holding data rows,
    the count of crossed rows that are not mark-outside is zero, and so is the count of
    mark-outside rows that are not crossed. So this is one measurement and one finding rather
    than two.

    **It reads every data row rather than the session's.** The entitlement check's staleness
    half is session-only because a vendor's last-quote stamp freezes overnight while the fetch
    clock keeps moving. An ordering carries no such drift: a quote taken at 03:25 is as ordered
    or as crossed as one taken at noon, and the measurement behind the tolerance was taken over
    every data row.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    try:
        available = set(pq.read_schema(partition.path).names)
    except Exception as exc:  # noqa: BLE001 - one damaged file must not cost the run
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc

    if ROW_KIND_COLUMN not in available:
        raise PartitionUnreadable(f"{partition.relative}: missing {ROW_KIND_COLUMN}")
    absent = tuple(name for name in ORDERED_COLUMNS if name not in available)
    wanted = [ROW_KIND_COLUMN, *(name for name in ORDERED_COLUMNS if name in available)]

    try:
        table = pq.read_table(partition.path, columns=wanted)
    except Exception as exc:  # noqa: BLE001 - same reason as above
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc

    table = table.filter(pc.equal(table[ROW_KIND_COLUMN], ROW_KIND_DATA))
    rows = table.num_rows
    if absent or rows == 0:
        return QuoteOrder(rows=rows, unordered=0, absent=absent)

    with _contained(partition):
        ordered = pc.and_(
            pc.less_equal(table[BID], table[MARK]), pc.less_equal(table[MARK], table[ASK])
        )
        # ``pc.sum`` skips nulls, so a row whose comparison is null counts as unordered rather
        # than as agreeing. A null in any of the three is the vendor declining to say, and a
        # check that answers "declined to say" with a pass is not failing closed.
        agreeing = pc.sum(pc.cast(ordered, pa.int64())).as_py() or 0
    return QuoteOrder(rows=rows, unordered=rows - agreeing)


def judge_quote_order(partition: SealedPartition, evidence: QuoteOrder) -> Finding:
    """One partition's quote-sanity verdict, from the evidence and the measured tolerance.

    Two conditions quarantine.

    1. The partition does not carry ``bid``, ``ask`` or ``mark``. All three are in the pinned
       capture schema for both surfaces, so an absence is drift, and answering drift with a pass
       is what fail closed exists to prevent. That is the entitlement flag's rule applied to the
       columns this check is about, rather than ``PartitionUnreadable``, which is what the two
       stamps get because they support a measurement instead of being its subject.
    2. The unordered rate is over :data:`QUOTE_SANITY_TOLERANCE`.

    The rate is the partition's, which the constant's own comment argues for against the
    alternative of a per-snapshot rate.
    """
    if evidence.absent:
        return _finding(
            partition,
            CHECK_QUOTE_SANITY,
            QUARANTINED_VERDICT,
            f"{partition.surface} carries no {', '.join(evidence.absent)} column, so "
            "bid, mark and ask cannot be ordered",
        )
    rate = evidence.rate
    if rate > QUOTE_SANITY_TOLERANCE:
        return _finding(
            partition,
            CHECK_QUOTE_SANITY,
            QUARANTINED_VERDICT,
            f"{evidence.unordered} of {evidence.rows} data rows are not ordered "
            f"bid <= mark <= ask, a rate of {rate:.4%} over the {QUOTE_SANITY_TOLERANCE:.0%} "
            "tolerance",
            computed=rate,
            against=QUOTE_SANITY_TOLERANCE,
        )
    return _finding(
        partition,
        CHECK_QUOTE_SANITY,
        CLEAN_VERDICT,
        f"{evidence.unordered} of {evidence.rows} data rows are not ordered "
        f"bid <= mark <= ask, a rate of {rate:.4%} within the "
        f"{QUOTE_SANITY_TOLERANCE:.0%} tolerance",
        computed=rate,
        against=QUOTE_SANITY_TOLERANCE,
    )


# -- the snapshot row-count band ---------------------------------------------


def session_snapshot_counts(
    partition: SealedPartition, bounds: tuple[datetime, datetime] | None
) -> tuple[int, ...]:
    """How many data rows each of the partition's session snapshots holds.

    A snapshot is one ``snap_ts``, which is the minute slot a cycle fired for. Two columns are
    read, so the file's size barely reaches the cost: on SPY's 307 MB 2026-09-16 chains
    partition the read itself is 0.04 seconds, and the filter and the group-by over 5,307,030
    rows take the call to 0.28.

    ``bounds`` filters to the session for the reason :func:`_within` gives and one more of its
    own. The 03:25 overnight cycle on each of the lake's 2026-09-16 chain partitions carries
    about two percent fewer contracts than the session's own snapshots, and a median built from
    session snapshots is the wrong thing to measure an overnight chain against.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    try:
        available = set(pq.read_schema(partition.path).names)
    except Exception as exc:  # noqa: BLE001 - one damaged file must not cost the run
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc
    missing = [name for name in (ROW_KIND_COLUMN, SNAP_TS) if name not in available]
    if missing:
        raise PartitionUnreadable(f"{partition.relative}: missing {', '.join(sorted(missing))}")
    try:
        table = pq.read_table(partition.path, columns=[ROW_KIND_COLUMN, SNAP_TS])
    except Exception as exc:  # noqa: BLE001 - same reason as above
        raise PartitionUnreadable(f"{partition.relative}: {type(exc).__name__}: {exc}") from exc

    with _contained(partition):
        table = table.filter(pc.equal(table[ROW_KIND_COLUMN], ROW_KIND_DATA))
        if table.num_rows == 0:
            return ()
        table = _within(table, partition, bounds)
        if table.num_rows == 0:
            return ()
        counted = table.group_by(SNAP_TS).aggregate([(SNAP_TS, "count")])
        return tuple(sorted(counted[f"{SNAP_TS}_count"].to_pylist()))


def median(values: Sequence[float]) -> float:
    """The midpoint of a sorted copy, averaging the middle pair on an even count.

    Written here rather than taken from ``statistics`` so the interpolation matches
    ``pc.quantile(..., interpolation="midpoint")``, which :func:`_median_staleness` uses. Two
    medians in one module answering the same question two ways is the kind of drift the
    module's own constants exist to prevent.
    """
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (float(ordered[middle - 1]) + float(ordered[middle])) / 2


def trailing_medians(
    lake_root: Path | str,
    partition: SealedPartition,
    *,
    calendar: Calendar,
    spans: tuple[CaptureSpan, ...],
    guards: GuardConstants,
    memo: dict[str, float] | None = None,
) -> tuple[float, ...]:
    """Each trailing session's median snapshot row count, newest first.

    **The window is the ``trailing_median_sessions`` calendar sessions before this one, and
    every one of them enters it.** ``docs/design.md`` states the rule for median-relative checks
    and it is not this module's to reinterpret: "Median-relative checks compute over whatever
    trailing sessions exist. All sessions enter the window, and the median's robustness is the
    outlier defense."

    So the walk counts sessions rather than answers. A session the lake has no partition for, or
    one holding gap rows alone, occupies its slot in the window and contributes nothing to the
    median. It is not counted as zero, which would invent a snapshot count nobody captured and
    drag the median toward zero. Nor does the window reach past it for a replacement, which is
    the reading this first shipped with and which ``docs/design.md`` rules out.

    The difference is what happens after an outage. Reaching back for a full twenty answers
    builds a median out of sessions months older than the one being judged, and the drift
    measured to justify ``config.battery_row_count_band`` is drift across *consecutive*
    sessions. The first sessions back would then be measured against a stale roster and
    quarantined, which fails closed on good new data. Counting sessions instead leaves the
    window mostly empty, the contributing count falls under ``config.min_trailing_sessions`` and
    the check reports ``insufficient_history``, which fails open and says so.

    **Prior sessions only.** A statistic that includes its own subject cannot say the subject is
    unusual, and "trailing" is what the config field is called. The measurement is worth stating
    because it bounds the claim rather than supporting it: at the configured five, including the
    judged session would not by itself let a wholly truncated session pass, because the median
    of one zero and four full sessions is still a full session. The rule is about what the
    statistic means, not about a case anyone has observed.

    **A trailing session another check quarantined is admitted.** Excluding it would make the
    band depend on the ledger this same run is writing. The design's own sentence above is the
    warrant: all sessions enter the window and the median's robustness is the defense.

    ``memo`` is the run's cache of one median per partition. A whole-lake run otherwise reads
    each partition's trailing window once per partition, which is twenty times the reads for the
    same answer.
    """
    root = Path(lake_root)
    directory = root / partition.surface / f"{TICKER_PREFIX}{partition.ticker}"
    found: list[float] = []
    when = partition.day
    sessions = 0

    while sessions < guards.trailing_median_sessions:
        when -= timedelta(days=1)
        bounds = _session_bounds(calendar, when)
        if bounds is None:
            continue
        earlier = SealedPartition(
            path=directory / f"{DATE_PREFIX}{when.isoformat()}.parquet",
            surface=partition.surface,
            ticker=partition.ticker,
            day=when,
        )
        if not in_scope(earlier, spans):
            # Outside every capture span there is no session to have captured, so this is the
            # end of the window rather than an empty slot in it. Walking on would count the
            # days before onboarding against a roster that did not exist.
            break
        sessions += 1
        if memo is not None and earlier.relative in memo:
            found.append(memo[earlier.relative])
            continue
        if not earlier.path.exists():
            continue
        try:
            counts = session_snapshot_counts(earlier, bounds)
        except PartitionUnreadable:
            # A trailing partition that will not read contributes no median. It is not reported
            # here, because the walk reports it under its own verdict when it reaches it, and
            # because a session the run was not asked about is not this check's to announce.
            continue
        if not counts:
            continue
        computed = median(counts)
        if memo is not None:
            memo[earlier.relative] = computed
        found.append(computed)
    return tuple(found)


def judge_row_count(
    partition: SealedPartition,
    counts: Sequence[int],
    trailing: Sequence[float],
    guards: GuardConstants,
) -> Finding:
    """One partition's row-count verdict, from its snapshots and the trailing median.

    Three answers before the band is applied.

    1. No session snapshot at all, which is out of scope. A day of overnight cycles recorded no
       session rather than a bad one, and it is the same answer :func:`judge_entitlement` gives
       the same partition.
    2. Fewer trailing sessions than ``config.min_trailing_sessions``, which is
       ``insufficient_history``. The design says a median-relative check with a thin history
       still runs and tags its rows rather than passing them, and that tag is a finding and
       never a verdict because ``manifest.is_quarantined`` fails closed on anything but
       ``clean``.
    3. Otherwise the band, ``config.battery_row_count_band`` either side of the trailing median.

    **The threshold is one snapshot rather than a rate**, which is what "catches truncated
    fetches" asks for: a truncated fetch is one cycle, and a check tolerating some would not
    catch them. The lake says the band has room for it. Inside a session the per-snapshot count
    is exactly constant on all six of its chains data partitions. Two of them carry one
    overnight cycle each, 1.9 percent short on SPY and 2.5 percent short on QQQ, and the session
    filter drops both. Across sessions the count moves about one percent, against a band of
    thirty.

    ``computed`` differs by branch, because what an operator deciding whether to sign off wants
    differs by branch. On a quarantine it is the snapshot furthest from the median, which is the
    number that caused the verdict, and ``against`` is the median. On a pass it is the judged
    session's own median against the trailing one, which is the comparison that passed. On
    ``insufficient_history`` it is how many trailing sessions carried a median against how many
    the check needs, because there is no row count to report.
    """
    if not counts:
        return _finding(
            partition,
            CHECK_ROW_COUNT_BAND,
            OUT_OF_SCOPE,
            "no data row falls inside the session, so the partition carries no snapshot to "
            "measure against the trailing median",
        )
    if not trailing or len(trailing) < guards.min_trailing_sessions:
        return _finding(
            partition,
            CHECK_ROW_COUNT_BAND,
            INSUFFICIENT_HISTORY,
            f"{len(trailing)} trailing session{'s' if len(trailing) != 1 else ''} carry a "
            f"median, below the {guards.min_trailing_sessions} a median-relative check needs",
            computed=float(len(trailing)),
            against=float(guards.min_trailing_sessions),
        )
    against = median(trailing)
    band = float(guards.battery_row_count_band)
    low, high = against * (1 - band), against * (1 + band)
    outside = [count for count in counts if count < low or count > high]
    if outside:
        worst = max(outside, key=lambda count: abs(count - against))
        return _finding(
            partition,
            CHECK_ROW_COUNT_BAND,
            QUARANTINED_VERDICT,
            f"{len(outside)} of {len(counts)} session snapshots fall outside "
            f"{low:.0f} to {high:.0f} rows, the worst holding {worst} against a trailing "
            f"median of {against:.0f} over {len(trailing)} sessions",
            computed=float(worst),
            against=against,
        )
    return _finding(
        partition,
        CHECK_ROW_COUNT_BAND,
        CLEAN_VERDICT,
        f"all {len(counts)} session snapshots fall inside {low:.0f} to {high:.0f} rows, "
        f"against a trailing median of {against:.0f} over {len(trailing)} sessions",
        computed=float(median([float(count) for count in counts])),
        against=against,
    )


# -- the run -----------------------------------------------------------------


def judge(
    lake_root: Path | str,
    *,
    now: datetime,
    calendar: Calendar,
    day: date | None = None,
    guards: GuardConstants | None = None,
    publisher: Publisher | None = None,
    dry_run: bool = False,
) -> BatteryReport:
    """Judge the lake's sealed partitions, write what changed, and page a delayed feed.

    ``day=None`` judges every sealed partition, which is what a first run wants. The 18:30 job
    passes the session it is about. **Trading-calendar coverage is outside that scoping** and
    always walks the whole capture span, for the reason :func:`coverage` gives: a session with
    no partition is a session on which nothing ran, so a check scoped to tonight can never see
    the night it missed.

    The order inside one partition is scope first, then read, then judge, then decide and
    write under the lake-root lock. Scope comes first because both out-of-scope classes are
    cheap to answer and neither needs the file's rows, and because judging an out-of-scope
    partition is the failure this deliverable's audit found would quarantine the lake's oldest
    data on the first run.

    **The ledger is read inside the hold that appends, one hold per partition.** A dry run
    appends nothing, so it takes no hold and the comment at that line says why. The
    alternative is one read before the walk, and that snapshot is as old as the walk. Marketlake
    #470 is that defect, and the comment at the hold carries what it cost. The hold is per
    partition rather than around the whole walk because the walk is seconds, the sweep's other
    pieces want the lake, and ``append_verdict`` taking the lock under a caller that already
    holds it deadlocks. Measured, the hold is cheap: acquiring and releasing the lock is 20
    microseconds, and reading the ledger is 0.03 ms while it is empty and 1.4 ms at a thousand
    lines.

    **One partition's failure costs its own verdict and not the run.** ``PartitionUnreadable`` is
    contained here and counted, for ``sweep._counted``'s reason stated from the other side: the
    partitions most likely to be unreadable are the ones a battery would quarantine, so a walk
    that stopped at the first would judge nothing on exactly the night that mattered.

    **A dry run is this same walk with the writer switched off**, rather than a second walk
    beside it. Two walks would drift, and the whole point of a dry run is that the counts an
    operator reads before deciding are the counts the real run will produce. It pages nothing
    either, because a page about a verdict nobody wrote would send an operator to a sign-off
    command that answers nothing.

    **The page comes after every write.** A page naming partitions the ledger does not yet
    withhold would send an operator to a sign-off command that answers nothing, and
    ``Publisher.publish`` never raises, so the page cannot cost a verdict that is already on
    disk either way.

    **A release is reported and never paged.** A page reaches a phone and asks for action, and
    a partition rejoining the readable set asks for none. :attr:`BatteryReport.released` is the
    count and the report carries the line.
    """
    root = Path(lake_root)
    guards = GuardConstants() if guards is None else guards
    partitions = sealed_partitions(root, day=day)
    try:
        reference = read_reference(root)
        spans = capture_spans_by_ticker(root, {p.ticker for p in partitions}, reference=reference)
        found = coverage(root, reference, calendar, now=now)
    except ScopeUnknown as exc:
        # Not a silent pass. Every partition would otherwise take the out-of-scope path, whose
        # reason says capture was not running, which is a fact this run does not have. The
        # command exits non-zero on this for the same reason it does on an unreadable
        # partition: the lake's health is unknown rather than good.
        return BatteryReport(scope_unknown=len(partitions), report=(f"battery: {exc}",))

    # Local, the same reason :func:`append_verdict` gives for the same import.
    from lake.lock import lake_lock

    findings: list[Finding] = []
    written: list[Finding] = []
    appended: list[str] = []
    report: list[str] = list(found.unnamed)
    medians: dict[str, float] = {}
    deferred = 0
    withheld = 0
    released = 0
    unreadable = 0

    for partition in partitions:
        its_spans = spans.get(partition.ticker, ())
        if not in_scope(partition, its_spans):
            findings.append(
                _finding(
                    partition,
                    CHECK_SCOPE,
                    OUT_OF_SCOPE,
                    "the day lies outside every capture span, so capture was not running",
                )
            )
            continue
        bounds = _session_bounds(calendar, partition.day)
        try:
            judged = _judge_partition(
                root,
                partition,
                bounds=bounds,
                calendar=calendar,
                spans=its_spans,
                guards=guards,
                medians=medians,
            )
        except PartitionUnreadable as exc:
            unreadable += 1
            report.append(f"battery: {exc}")
            continue
        findings.extend(judged)

        # **The ledger is read inside the hold this partition's lines are appended under,
        # and that is marketlake #470.** Read once before the walk, the snapshot is as old as
        # the walk, and ``lake.signoff`` is the ledger's second writer: a sign-off landing in
        # that window is invisible to ``human_precedence``, so the battery appends its own
        # verdict after the human's and the next night's run, seeing ``provenance: battery`` on
        # the entry it compares, re-quarantines what a person cleared on purpose. Two runs of
        # this walk overlapping used to append the identical line twice for the same reason.
        # ``lake.occ_mapping`` states the same rule for the security master.
        #
        # The hold covers ledger work alone. Reading and judging the partition is seconds and
        # stays above this line, which is the rule ``bars`` states for its vendor round trip.
        #
        # **A dry run takes no hold.** The lock is what makes the read and the append one
        # step, and a dry run has no append for it to be atomic with, so it would be holding
        # an exclusive lock over a read nothing acts on. That is not free either way it is
        # decided: ``lake_lock`` opens the manifest with ``O_CREAT``, so a preview would
        # create a ``manifest.jsonl`` in a lake that had none, and ``compact.sweep`` holds the
        # lock across a whole rewrite, so a preview would block behind it where it used to
        # answer. The price is that a dry run's counts are read without synchronisation, which
        # is what a forecast is.
        with nullcontext() if dry_run else lake_lock(root):
            # **One call for the partition, not one per finding.** ``decide_partition`` carries
            # the ledger state forward as lines land, and two checks clearing in one walk both
            # change what withholds the partition. Called once per finding that state never
            # accumulates: the second check would report the partition still held by the first,
            # and the release would go unreported. This is the seam that function's docstring
            # names.
            outcome = decide_partition(
                latest_quarantine_by_check(root).get(partition.relative), judged
            )
            for decision in outcome.decisions:
                if decision.deferred_to_human:
                    deferred += 1
                    report.append(
                        f"battery: {decision.finding.partition} re-observed, human precedence "
                        f"stands ({decision.finding.reason})"
                    )
                    continue
                if not decision.wrote:
                    continue
                if dry_run:
                    report.append(
                        f"battery: would write {decision.finding.verdict} for "
                        f"{decision.finding.partition} under {decision.finding.check}"
                    )
                    continue
                write_verdict(
                    root,
                    build_entry(
                        partition=decision.finding.partition,
                        verdict=decision.finding.verdict,
                        check=decision.finding.check,
                        observed_at=now,
                        reason=decision.finding.reason,
                    ),
                    observed_at=now,
                )
                appended.append(decision.finding.partition)
                written.append(decision.finding)
        # **One line for the partition, not one per passing check.** Two checks pass a
        # partition a third withholds, and a line each says the same fact twice, in a list
        # ``sweep`` puts through ``digest_body``'s 1000-byte cap. The holders
        # are the walk's final state rather than any one decision's, so a check that
        # quarantined after another passed is named too.
        passed = [
            decision.finding.check
            for decision in outcome.decisions
            if not decision.deferred_to_human and not decision.finding.withholds
        ]
        if passed and outcome.holders:
            withheld += 1
            named = ", ".join(
                repr(check) if check is not None else "an unnamed check"
                for check in outcome.holders
            )
            report.append(
                f"battery: {partition.relative} passes {', '.join(passed)} "
                f"and stays quarantined under {named}"
            )
        if outcome.released:
            released += 1
            report.append(
                f"battery: {partition.relative} would now read, no check would withhold it"
                if dry_run
                else f"battery: {partition.relative} now reads, no check withholds it"
            )

    # **The page is one check's, and it is filtered to that check.** ``written`` is every
    # finding this run appended a line for, which is the right set for the transition rule and
    # the wrong set for this page: from #407 onwards a crossed quote or a truncated fetch would
    # otherwise reach a phone titled ``Delayed feed`` with its rate rendered as a staleness in
    # seconds. The design gives the battery two pages and the other one is #427, so nothing here
    # adds a third: the other two checks report and never page.
    quarantined = tuple(
        f for f in written if f.verdict == QUARANTINED_VERDICT and f.check == CHECK_ENTITLEMENT
    )
    paged = page_delayed_feed(publisher, quarantined, now=now) if publisher and not dry_run else ()

    # **The battery's second page, and it runs after the first.** Marketlake #427. The design's
    # message table gives schema drift four producers and names this one beside the parser's and
    # compaction's. Placement is what bounds what a failure here can cost. ``sweep`` wraps this
    # whole function in ``except Exception`` because "the battery must not cost the record", so a
    # raise reaches the nightly report either way. Inside here the verdicts are already on disk,
    # written under the lock per partition, and ``page_delayed_feed`` has already fired. A raise
    # before that line would cost the battery's one shipped page on a night whose delayed feed is
    # exactly what it was for.
    drift_paged: tuple[str, ...] = ()
    try:
        drift = _judge_drift(root, partitions, day=day, surfaces=SEALED_SURFACES)
    except Exception as exc:  # noqa: BLE001 - a second page must not cost the first
        report.append(f"battery: schema drift did not run: {type(exc).__name__}: {exc}")
    else:
        report.extend(drift.report)
        if publisher and not dry_run:
            drift_paged = battery_drift.page(publisher, drift.findings, now=now)

    # The findings about partitions that exist come first, in walk order, and the ones about
    # partitions that do not come last. Coverage answers a question about the whole lake rather
    # than about anything the walk opened, so it does not interleave with the walk.
    findings.extend(found.missing)

    # **The census goes last.** ``sweep.digest_body`` truncates the tail at 1000 bytes, and the
    # comment this line's reasoning comes from assumed "what falls off the end first is the
    # battery's own census". Put in front it would be the last thing to fall off instead, and
    # the lines it would push out are the actionable ones: a release, a partition another check
    # still withholds, a partition that would not read.
    report.append(coverage_line(found))

    return BatteryReport(
        judged=sum(1 for f in findings if f.judged),
        quarantined=sum(1 for f in findings if f.verdict == QUARANTINED_VERDICT),
        cleared=sum(1 for f in findings if f.verdict == CLEAN_VERDICT),
        insufficient_history=sum(1 for f in findings if f.verdict == INSUFFICIENT_HISTORY),
        out_of_scope=sum(1 for f in findings if f.verdict == OUT_OF_SCOPE),
        deferred=deferred,
        withheld=withheld,
        released=released,
        unreadable=unreadable,
        scope_unknown=len(found.unnamed),
        sessions_owed=found.owed,
        sessions_missing=len(found.missing),
        appended=tuple(appended),
        paged=paged,
        drift_paged=drift_paged,
        report=tuple(report),
        findings=tuple(findings),
    )


def _judge_partition(
    root: Path,
    partition: SealedPartition,
    *,
    bounds: tuple[datetime, datetime] | None,
    calendar: Calendar,
    spans: tuple[CaptureSpan, ...],
    guards: GuardConstants,
    medians: dict[str, float],
) -> list[Finding]:
    """Every check's answer about one in-scope partition, in one list.

    **Both out-of-scope classes are answered once for the partition**, carrying
    :data:`CHECK_SCOPE`, and no check runs against such a partition. Scope is a property of the
    partition and every check would answer it from the same two facts.

    The second class is the design's own wording, "a partition holding no data row **inside the
    session**", rather than the narrower "no data row at all". The difference is a partition
    holding only overnight cycles, and it is not hypothetical: both of the lake's 2026-09-16
    chain partitions carry such a cycle, and a night the machine woke only for that cycle would
    be a partition of nothing else. A closed options book is where a vendor is most likely to
    return a zero bid against a zero ask with the mark at the last trade, which is unordered on
    every row, so quote sanity would quarantine a partition the design says nothing should
    judge.

    **The row-count band is the options-only check of the three**, and it is also the only one
    that needs a session. ``docs/design.md`` names the equity-only subset as "calendar coverage,
    quote sanity, cross-check", and on quotes a snapshot is one row, so the band would compare
    one against a trailing median of one for ever. It returns no finding on quotes rather than
    an ``out_of_scope`` one, whose meaning is that capture was not running.

    On a day the calendar calls no session it returns ``out_of_scope`` instead, and that is a
    different answer from the one it gives quotes. ``_session_bounds`` answers ``None`` there,
    which widens :func:`_within` to every row, so the band would count overnight cycles as
    session snapshots and measure them against a median :func:`trailing_medians` builds from
    real sessions alone. The entitlement check's flag half still runs on such a day, which is
    the half that could see a delayed feed, and ``_session_bounds`` gives that reason.
    """
    evidence = read_entitlement(partition, bounds)
    if evidence.rows == 0:
        return [
            _finding(
                partition,
                CHECK_SCOPE,
                OUT_OF_SCOPE,
                "the partition holds no data row, so its gap rows record a missed "
                "session rather than a bad one",
            )
        ]

    if bounds is not None and evidence.session_rows == 0:
        return [
            _finding(
                partition,
                CHECK_SCOPE,
                OUT_OF_SCOPE,
                "no data row falls inside the session, so a day of overnight cycles "
                "recorded no session rather than a bad one",
            )
        ]

    judged = [
        judge_entitlement(partition, evidence, guards),
        judge_quote_order(partition, read_quote_order(partition)),
    ]
    if partition.surface != CHAINS:
        return judged
    if bounds is None:
        judged.append(
            _finding(
                partition,
                CHECK_ROW_COUNT_BAND,
                OUT_OF_SCOPE,
                "the calendar calls the day no session, so there is neither a session to "
                "count snapshots over nor a comparable median to count them against",
            )
        )
        return judged
    counts = session_snapshot_counts(partition, bounds)
    if counts:
        medians[partition.relative] = median(counts)
    judged.append(
        judge_row_count(
            partition,
            counts,
            trailing_medians(
                root,
                partition,
                calendar=calendar,
                spans=spans,
                guards=guards,
                memo=medians,
            ),
            guards,
        )
    )
    return judged


def _judge_drift(
    root: Path,
    partitions: Sequence[SealedPartition],
    *,
    day: date | None,
    surfaces: Sequence[str],
) -> battery_drift.DriftReport:
    """The schema-drift comparison for the run's day, against the previous sealed day.

    **The run's day, and what ``day=None`` means here.** The 18:30 job passes the session it is
    about. A hand run defaults to walking the whole lake, and this check judges the newest sealed
    day that walk found rather than every day against its predecessor. One comparison answers
    what an operator ran the command to see, and a comparison per day would open every partition
    in the lake twice to re-derive transitions that are already in the record.

    **The baseline is the previous day that has sealed partitions**, found by stepping back a day
    at a time. A day the calendar calls no session has none, and neither does a day the machine
    was off, so stepping past both is the same step and needs no calendar. The walk is bounded at
    ``battery_drift.BASELINE_LOOKBACK_DAYS`` because a lake whose earlier days were never sealed
    must not turn one night's check into a walk over the whole calendar.
    """
    judged = day
    if judged is None:
        if not partitions:
            return battery_drift.DriftReport()
        judged = max(partition.day for partition in partitions)
    today = [partition for partition in partitions if partition.day == judged]
    if not today:
        return battery_drift.DriftReport()

    baseline: date | None = None
    baseline_parts: dict[str, list[SealedPartition]] = {}
    for step in range(1, battery_drift.BASELINE_LOOKBACK_DAYS + 1):
        candidate = judged - timedelta(days=step)
        found = sealed_partitions(root, day=candidate)
        if not found:
            continue
        baseline = candidate
        for partition in found:
            baseline_parts.setdefault(partition.surface, []).append(partition)
        break

    return battery_drift.judge_day(
        root,
        today,
        day=judged,
        baseline=baseline,
        baseline_partitions=baseline_parts or None,
        surfaces=surfaces,
    )


def page_delayed_feed(
    publisher: Publisher, quarantined: Sequence[Finding], *, now: datetime
) -> tuple[str, ...]:
    """Page once for the run, naming the partitions this run newly quarantined.

    The transition is the ledger's own. ``judge`` passes the findings it appended a line for
    and no others, so a partition already quarantined under this check does not page again:
    its existing entry is what says the operator was already told. It passes them as the
    findings it wrote rather than by filtering on the partitions it wrote, because those two
    differ as soon as a partition carries two checks: one check's line would page the other
    check's unchanged finding. That is the same once-on-the-transition
    rule the auth path, the watchdog and all three schema-drift producers carry, expressed in
    the record that already exists rather than in a counter this module would have to keep.

    One page for the run, never one per partition. A vendor entitlement change reaches every
    partition on the same evening, so paging per finding would scale the page count with the
    roster while the fact stayed one fact. ``alert.DEFAULT_DAILY_CAP`` is forty a day, and the
    page this would swallow could be the auth-death page.

    The finding reaches stderr as well as the phone, which is what ``schema_drift.page`` and
    ``compact._page_drift`` both already do. The one exception is a refused page: the publisher
    found one of its own secrets in the body and redacted its record for that reason, so stderr
    must not undo the redaction.
    """
    if not quarantined:
        return ()
    body = _page_body(quarantined)
    delivery = publisher.publish(
        Message(event=DELAYED_FEED_EVENT, title=DELAYED_FEED_TITLE, body=body), now=now
    )
    named = tuple(f.partition for f in quarantined)
    if delivery.reason == REFUSED:
        print("battery: delayed-feed page refused: it carried a secret", file=sys.stderr)
        return named
    print(f"battery: {DELAYED_FEED_TITLE}: {body}", file=sys.stderr)
    for finding in quarantined:
        print(f"battery: {finding.partition}: {finding.reason}", file=sys.stderr)
    if not delivery.sent:
        kept = "written down" if delivery.recorded else "lost"
        print(f"battery: delayed-feed page not sent: {delivery.reason}, {kept}", file=sys.stderr)
    return named


def _page_body(quarantined: Sequence[Finding]) -> str:
    """The page's body: the count, the medians, and the partitions up to the cap.

    The design's message table says the body carries "the session-median staleness and the
    partitions quarantined", and both are here. The staleness is a range rather than one number
    when the partitions disagree, because one number would hide a feed that went delayed on one
    ticker and not another.
    """
    count = len(quarantined)
    measured = [f.computed for f in quarantined if f.computed is not None]
    if not measured:
        staleness = "staleness unmeasurable"
    elif len(set(measured)) == 1:
        staleness = f"session-median staleness {measured[0]:.1f}s"
    else:
        staleness = f"session-median staleness {min(measured):.1f}s to {max(measured):.1f}s"
    named = [f.partition for f in quarantined[:PAGE_PARTITION_CAP]]
    more = count - len(named)
    listed = ", ".join(named) + (f" and {more} more" if more else "")
    return f"{count} partition{'s' if count != 1 else ''} quarantined. {staleness}. {listed}."


# -- the command -------------------------------------------------------------


def judge_from_config(
    *,
    clock=None,
    config_path: str | Path | None = None,
    day: date | None = None,
    publisher: Publisher | None = None,
    dry_run: bool = False,
) -> BatteryReport:
    """The battery wired from the real config. This is the entry :func:`main` calls.

    The guard constants come from the same config, so a recalibrated
    ``staleness_page_seconds`` takes effect on the next run rather than at the next release.
    """
    from lake.calendar import ExchangeCalendar
    from lake.clock import SystemClock
    from lake.config import load_config

    config = load_config(config_path)
    clock = SystemClock() if clock is None else clock
    return judge(
        config.lake_root,
        now=clock.now(),
        calendar=ExchangeCalendar(),
        day=day,
        guards=config.guards,
        publisher=publisher,
        dry_run=dry_run,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lake.battery",
        description=(
            "Judge the lake's sealed chains and quotes partitions and write quarantine "
            "verdicts for what fails."
        ),
    )
    parser.add_argument(
        "--session",
        metavar="YYYY-MM-DD",
        default=None,
        help=(
            "judge one session's partitions. The default judges every sealed partition. "
            "Trading-calendar coverage ignores this and always walks the whole capture span, "
            "because a session with no partition is a session on which nothing ran."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be written without appending a ledger line.",
    )
    return parser


def render(report: BatteryReport) -> str:
    """One run's result as the lines a hand run prints.

    Every count is printed including the zeroes, because a run that judged nothing and a run
    that judged everything cleanly are different answers and a report that printed only
    non-zero counts would render them the same. That rule is what the coverage pair leans on:
    zero missing sessions out of a stated number owed says the check ran and found nothing.
    """
    lines = [
        f"  judged:               {report.judged}",
        f"  quarantined:          {report.quarantined}",
        f"  clean:                {report.cleared}",
        f"  out of scope:         {report.out_of_scope}",
        f"  insufficient history: {report.insufficient_history}",
        f"  human precedence:     {report.deferred}",
        f"  still withheld:       {report.withheld}",
        f"  released:             {report.released}",
        f"  unreadable:           {report.unreadable}",
        f"  scope unknown:        {report.scope_unknown}",
        f"  sessions owed:        {report.sessions_owed}",
        f"  sessions missing:     {report.sessions_missing}",
        f"  ledger lines written: {len(report.appended)}",
    ]
    lines.extend(f"  {line}" for line in report.report)
    for finding in report.findings:
        if finding.verdict == QUARANTINED_VERDICT:
            lines.append(f"  quarantined {finding.partition}: {finding.reason}")
    # The missing sessions by name, which :func:`coverage_line` deliberately leaves out. This is
    # the job's own stdout rather than the nightly report, so it is under neither the digest's
    # byte cap nor the rule that keeps a list of findings out of it, and it is uncapped for the
    # same reason the quarantined findings above it are.
    for finding in report.findings:
        if finding.verdict == MISSING_SESSION:
            lines.append(f"  missing {finding.partition}: {finding.reason}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, *, clock=None) -> int:
    """The ``python -m lake.battery`` entry. Returns a process exit code.

    **Every refusal reaches the operator as a line rather than a stack.** A run from launchd
    writes stderr to a log file, and an uncaught traceback there is a wall of frames around one
    sentence. ``input_errors_exit`` covers the config files that are the operator's to edit, and
    the two ways a lake can fail here are named apart because they send the operator to
    different repairs.

    **A quarantine is not an error.** A run that found a delayed feed did its job, so it exits
    0. What exits 1 is a run that could not judge something it was asked to judge: the
    unreadable count and the scope-unknown count, because those are the cases where the lake's
    health is unknown rather than bad. A run that judged nothing because the reference files
    could not be read must not read like a clean night.
    """
    args = _build_parser().parse_args(argv)

    from lake.config import input_errors_exit

    session: date | None = None
    if args.session is not None:
        try:
            session = date.fromisoformat(args.session)
        except ValueError:
            print(f"battery: --session is not a date: {args.session!r}", file=sys.stderr)
            return 2

    if args.dry_run:
        print("battery: dry run, no ledger line will be written", file=sys.stderr)

    try:
        with input_errors_exit("battery"):
            report = judge_from_config(clock=clock, day=session, dry_run=args.dry_run)
    except SystemExit as exit_code:  # noqa: PERF203 - the context manager's own exit
        return int(exit_code.code or 0)
    except FileNotFoundError as exc:
        print(f"battery: {exc}", file=sys.stderr)
        return 2
    except ManifestError as exc:
        # **A damaged ledger is a line here, like every other refusal.** The walk resolves the
        # quarantine ledger inside each partition's hold, so a ledger that cannot be read
        # stops the run, and without this it stopped it as eight frames around one sentence.
        # ``docs/design.md`` names this command as the one an operator runs when the nightly
        # digest carries only counts, so a torn ledger sends them here on purpose and a stack
        # is the worst thing to meet. Marketlake #469 made that likely: a crash mid-append
        # needs no hand-malformed line. It is exit 2 rather than 1 because the lake's own file
        # contradicts its writer, which is a repair rather than a night that judged nothing,
        # and ``lake.signoff`` keeps the traceback for the opposite reason its docstring gives.
        print(f"battery: {exc}", file=sys.stderr)
        return 2

    print("battery:")
    print(render(report))
    return 1 if report.unreadable or report.scope_unknown else 0


if __name__ == "__main__":  # pragma: no cover - the module entry
    raise SystemExit(main())


__all__ = [
    "BATTERY_SOURCE",
    "CHECK_CALENDAR_COVERAGE",
    "CHECK_ENTITLEMENT",
    "CHECK_QUOTE_SANITY",
    "CHECK_ROW_COUNT_BAND",
    "CHECK_SCOPE",
    "DELAYED_FEED_EVENT",
    "DELAYED_FEED_TITLE",
    "INSUFFICIENT_HISTORY",
    "MISSING_SESSION",
    "NON_VERDICTS",
    "ORDERED_COLUMNS",
    "OUT_OF_SCOPE",
    "PROVENANCE_BATTERY",
    "PROVENANCE_HUMAN",
    "QUARANTINED_VERDICT",
    "QUOTE_SANITY_TOLERANCE",
    "SEALED_SURFACES",
    "SIGNOFF_SOURCE",
    "VERDICTS",
    "BatteryError",
    "BatteryReport",
    "Coverage",
    "Decision",
    "Entitlement",
    "Finding",
    "PartitionOutcome",
    "PartitionUnreadable",
    "QuoteOrder",
    "Reference",
    "ScopeUnknown",
    "SealedPartition",
    "append_verdict",
    "build_entry",
    "capture_spans_by_ticker",
    "coverage",
    "coverage_line",
    "decide_partition",
    "entry_line_count",
    "human_precedence",
    "in_scope",
    "judge",
    "judge_entitlement",
    "judge_from_config",
    "judge_quote_order",
    "judge_row_count",
    "main",
    "median",
    "page_delayed_feed",
    "partition_key",
    "read_entitlement",
    "read_quote_order",
    "read_reference",
    "sealed_partitions",
    "session_snapshot_counts",
    "trailing_medians",
    "write_verdict",
]
