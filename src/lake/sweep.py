"""The evening vendor sweep: the 18:30 weekday job, the nightly digest, and the Friday wake.

Three pieces shipped before this one and none of them had a scheduler or anywhere to report.
This is the job that runs them every weekday evening, the file that records what happened, the
message that says it in one screen, and the two alarms that were already written expecting it
to exist.

Run it with ``python -m lake.sweep``. launchd runs it at 18:30 Monday through Friday, under
``control_plane.eod_sweep_job``, and its health check is the ``eod-sweep`` slug.

What one run does, in the design's own order.

1. The corporate-actions poll, so today's split flags before bars land. Two walks over sealed
   rows: ``actions.extract_dividends`` reads quotes and ``splits.detect_splits`` reads chains.
   The split walk takes the calendar as well, because whether two sealed sessions are adjacent
   is the calendar's answer and not the manifest's. Marketlake #431.
2. The bar walk, ``bars.backfill_bars``. The close cross-check is inside it, and the
   walk covers every session the capture spans still hold unlanded rather than only
   the one the clock is in, because a daily bar has no close of record to pass that
   check against on the night of its own session. Marketlake #422, with marketlake
   #434 turning that night's fetch into a skip.
3. The validation battery, ``battery.judge``, which judges the sealed chains and quotes
   partitions and writes a quarantine verdict for what fails. The design places it between the
   bar fetch and the Friday branch, which is where this list puts it.
4. The Friday branch, which sets the Sunday one-shot wake and reads it back.
5. The ping.
6. The dated report file under ``reports/``.
7. The digest, at priority 2.

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
whose close has not happened. Every ticker would fail the bar span check and the run would ping
green while the missed evening's bars stayed missing forever. So the bar fetch runs only once
the session's equity close has passed, and a run that skipped it for that reason stays silent.
The guard cannot refuse a real run, because the job fires at 18:30 against a 16:00 close, or
13:00 on an early close. Recovering the evening that was missed is the walk's own doing now:
marketlake #422 pointed this job at #319's span walk, so the next run that does fire reaches
every session still unlanded rather than only the one its clock sits on.

**A holiday runs none of the data work.** The launchd interval is Monday through Friday, so a
non-session weekday is a holiday, and the design has "compaction and the sweep no-op on an
empty journal". The one-line digest settles it: a run whose walks found something would have
nowhere to say so. The walks read every sealed ticker-day rather than today's, so a holiday
run would re-derive yesterday's held findings and file each one again under
``reports/withheld/`` for no new information. The run still pings, and the Friday branch still
sets the wake, which the design's pmset table states directly. One side effect is worth
naming: a holiday never builds the vendor, so it never reads the token.

**Every seam is injected and only ``main`` builds one.** That is ``control_plane.main``'s rule
and its reason, which is that a ``main`` accepting them lets a test omit one and reach the real
effect. So :func:`sweep` requires seven and defaults none: the clock, the calendar, the vendor
source, the pinger, the publisher, the schedule reader and the schedule setter. The whole
module runs offline in a test, with no network and no shelling out.
"""

from __future__ import annotations

import subprocess
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial
from pathlib import Path

from lake.actions import ExtractionReport, MasterAbsent, extract_dividends
from lake.alert import Message, NtfyTransport, Publisher, undelivered
from lake.bars import (
    BackfillReport,
    BarsReport,
    SpansAbsent,
    StampNotAnInstant,
    UnsupportedBarFreq,
    backfill_bars,
    read_capture_spans,
)
from lake.battery import BatteryReport, judge
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
from lake.manifest import is_quarantined, latest_quarantine
from lake.paths import CHAINS, QUOTES
from lake.report import (
    BARS_PIECE,
    DIVIDENDS_PIECE,
    SPLITS_PIECE,
    Nightly,
    PieceOutcome,
    redacted,
    write_nightly,
)
from lake.runner import PING_FAILURES, Pinger, UrllibPinger, escalate_ping_failure
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor, VendorAuthError
from lake.security_master import MasterUnreadable
from lake.splits import SplitReport, detect_splits
from lake.tickers import Roster, load_tickers
from lake.vendor import Vendor

# The nightly summary's wire shape, per the design's message table. Priority 2 is the silent
# tier: it lands in the notification drawer without interrupting, which is what makes one
# message every weekday evening a liveness signal for the channel rather than a page.
NIGHTLY_EVENT = "nightly_summary"
NIGHTLY_PRIORITY = 2

# What a holiday no-op sends, verbatim from the design's message table.
HOLIDAY_BODY = "Holiday, no session"

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
_LEDGER_REFUSALS = (MasterAbsent, MasterUnreadable, OSError)
# ``CaptureSpansError`` as the class rather than one of its members, which is the lesson
# ``bars.main`` already wrote down for itself: naming ``SpansUnreadable`` alone left its sibling
# ``UnsupportedSpansSchemaVersion`` reaching the operator as a stack, and a spans file from a
# newer version of this code is the one shape of it a person actually meets. The 18:30 job reads
# that file for the first time under marketlake #422, so it inherits the lesson rather than
# rediscovering it. Escaping here would cost the battery, the report file, the digest, the ping
# and, on a Friday, the Sunday wake.
_BARS_REFUSALS = (
    MasterAbsent,
    MasterUnreadable,
    SpansAbsent,
    CaptureSpansError,
    StampNotAnInstant,
    UnsupportedBarFreq,
    NotASession,
    VendorAuthError,
    OSError,
)


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


def _counted(what: str, read: Callable[[], int | None], report: list[str]) -> int | None:
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
        report.append(f"{what} unreadable: {type(exc).__name__}: {exc}")
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
                f" scope_unknown {self.battery.scope_unknown}"
                f" unreadable {self.battery.unreadable}"
                f" sessions_owed {self.battery.sessions_owed}"
                f" sessions_missing {self.battery.sessions_missing}"
                f" wrote {len(self.battery.appended)}"
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


def _friday_wake(
    *,
    now: datetime,
    calendar: Calendar,
    schedule_setter: ScheduleSetter,
    schedule_reader: ScheduleReader,
) -> tuple[list[str], list[str]]:
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
    """
    problems: list[str] = []
    report: list[str] = []

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
    report.extend(alarms.problems)
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
    schedule_reader: ScheduleReader,
    schedule_setter: ScheduleSetter,
    guards: GuardConstants | None = None,
) -> SweepOutcome:
    """Run one evening sweep. Every seam is required, and the module docstring says why.

    ``publisher`` alone may be ``None``, which sends no digest and escalates no refused ping.
    That is what lets a test drive the run without a page reaching anywhere, and it is the
    same allowance ``escalate_ping_failure`` already makes.

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
    report: list[str] = []
    pieces: list[tuple[str, PieceOutcome]] = []

    if session:
        closed = calendar.session_close(day) <= now
        # **Each walk carries its own arguments, because the two no longer take the same ones.**
        # ``detect_splits`` takes the calendar under marketlake #431: it decides whether two
        # sealed sessions are adjacent, and the manifest cannot answer that for a session the
        # lake never captured. ``extract_dividends`` reads ``ex_date`` off the row rather than
        # deriving it from a pair, so an uncaptured session cannot move its key and it needs no
        # calendar. The loop stays, because what it holds is the refusal containment and the
        # piece naming, which are still one rule for both.
        walks = (
            (DIVIDENDS_PIECE, partial(extract_dividends, lake_root=root, clock=clock)),
            (SPLITS_PIECE, partial(detect_splits, lake_root=root, clock=clock, calendar=calendar)),
        )
        for name, walk in walks:
            try:
                pieces.append((name, _ledger_outcome(walk())))
            except _LEDGER_REFUSALS as exc:
                pieces.append((name, _refused(exc)))
        if closed:
            # **The walk is the backfill, not a single session, and marketlake #422 is why.**
            # A daily bar is judged against the calendar-next session's settled close, which at
            # 18:30 on session S has not been captured. The single-session fetch said that
            # settled itself because the next run would land the bar, and it did not: the next
            # run fetched the *next* session and met the same absence for it, and nothing
            # scheduled ever came back. So the nightly job landed no daily bar at all, and this
            # walk is what makes that promise true.
            #
            # **What that night's ticker-day does about it changed under marketlake #434.** It
            # used to be fetched, gated against a close nobody had captured, and held, which
            # spent a vendor request and filed a withheld file on a gate that could not pass.
            # The close of record is this lake's rather than the vendor's, so the walk reads it
            # first and reports the ticker-day under ``unsettled`` instead. The recovery above
            # is untouched, which is what makes the skip safe: a session skipped tonight is a
            # session tomorrow's run still walks.
            #
            # ``backfill_bars`` is the walk that already existed, marketlake #319, and it
            # subsumes the single-session fetch rather than running beside it: ``_span_sessions``
            # puts a session in range once its equity close has arrived, so at 18:30 today is in
            # range along with every earlier session still unlanded. Yesterday's held daily bar
            # is reached with its following quotes now sealed.
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
                    report.append(
                        f"bars unwalked: {len(walked.unwalked)} ticker-day(s), "
                        f"first: {walked.unwalked[0]}"
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
                # digest and the dashboard's History panel, which renders what the file carries
                # and computes none of it. Every one of those three reads it through
                # ``report.redacted``, which is the other half of why it counts classes rather
                # than naming an entry.
                #
                # **``unsettled`` is deliberately not reported here.** It is a ticker-day whose
                # close of record the lake has not sealed yet, which on a healthy run is the
                # newest session and nothing else, right every night by construction. A line
                # that is loud every evening is one the reader learns to skip, which is the
                # argument ``dashboard._ping_owed`` already makes in those words. It still
                # reaches the by-hand run's own output, which is where a reader who wants it
                # goes.
                if walked.abandoned:
                    reasons = Counter(entry.reason for entry in walked.abandoned)
                    census = ", ".join(
                        f"{count} {reason}" for reason, count in sorted(reasons.items())
                    )
                    report.append(
                        f"bars abandoned: {len(walked.abandoned)} ticker-day(s), {census}"
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
                    report.append(
                        f"bars deferred: {len(walked.deferred)} ticker-day(s), "
                        f"{walked.attempted} request(s) spent"
                    )
            except _BARS_REFUSALS as exc:
                pieces.append((BARS_PIECE, _refused(exc)))
        else:
            # A catch-up run, fired by launchd on the next wake for an 18:30 the machine
            # slept through. Fetching now would ask for a session still in progress.
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
            report.append(f"battery did not run: {type(exc).__name__}: {exc}")
        else:
            report.extend(battery.report)

    if day.weekday() == _PY_FRIDAY:
        wake_problems, wake_report = _friday_wake(
            now=now,
            calendar=calendar,
            schedule_setter=schedule_setter,
            schedule_reader=schedule_reader,
        )
        problems.extend(wake_problems)
        report.extend(wake_report)

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
        report.append(
            f"battery wrote {len(battery.appended)} quarantine "
            f"line{'s' if len(battery.appended) != 1 else ''}"
        )

    nightly = Nightly(
        day=day,
        session=session,
        pinged=pinged,
        gaps=gaps,
        quarantined=quarantined,
        pages_lost=pages_lost,
        pieces=tuple(pieces),
        problems=tuple(problems),
        report=tuple(report),
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
    token_path: str | Path = DEFAULT_TOKEN_PATH,
) -> SweepOutcome:
    """The sweep wired from the real config. This is the entry :func:`main` calls.

    The vendor arrives as a thunk rather than as a built object, which is what keeps a holiday
    run from reading the token at all. ``from_token`` imports ``schwab-py`` lazily, so the
    offline suite loads this module without the library installed.
    """
    from lake.config import load_config

    config = load_config(config_path)

    def build_vendor() -> Vendor:
        return SchwabVendor.from_token(
            token_path,
            api_key=config.schwab_api_key.reveal(),
            app_secret=config.schwab_app_secret.reveal(),
        )

    return sweep(
        lake_root=config.lake_root,
        clock=SystemClock() if clock is None else clock,
        calendar=ExchangeCalendar(),
        roster=load_tickers(tickers_path),
        vendor_source=build_vendor if vendor_source is None else vendor_source,
        pinger=UrllibPinger(),
        ping_url=config.healthchecks_url(EOD_SWEEP_SLUG),
        publisher=Publisher(
            lake_root=config.lake_root,
            transport=NtfyTransport(config.ntfy_topic.reveal()),
            # The values that must never reach a phone, checked against the message itself.
            secrets=(config.healthchecks_ping_key.reveal(), config.ntfy_topic.reveal()),
        ),
        schedule_reader=read_pmset_schedule if schedule_reader is None else schedule_reader,
        schedule_setter=set_sunday_wake if schedule_setter is None else schedule_setter,
        guards=config.guards,
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.sweep",
        description=(
            "The evening vendor sweep: poll corporate actions, fetch the session's bars, "
            "set the Sunday wake on a Friday, ping, file the nightly report and send its "
            "digest. It fetches the session the clock is in."
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


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
