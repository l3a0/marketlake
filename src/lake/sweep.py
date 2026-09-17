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
2. The bar fetch, ``bars.fetch_session_bars``. The close cross-check is inside it.
3. The Friday branch, which sets the Sunday one-shot wake and reads it back.
4. The ping.
5. The dated report file under ``reports/``.
6. The digest, at priority 2.

The battery the design names between steps 2 and 3 is marketlake #138's and does not exist.
Nothing here builds it, and the quarantine count it would feed reads zero until it does.

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
13:00 on an early close. Recovering the evening that was missed is marketlake #319's, which
owns a walk over more than one session.

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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from lake.actions import ExtractionReport, MasterAbsent, extract_dividends
from lake.alert import Message, NtfyTransport, Publisher, undelivered
from lake.bars import (
    BarsReport,
    StampNotAnInstant,
    UnsupportedBarFreq,
    fetch_session_bars,
)
from lake.calendar import MARKET_TZ, Calendar, ExchangeCalendar, NotASession
from lake.clock import Clock, SystemClock
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
_LEDGER_REFUSALS = (MasterAbsent, MasterUnreadable)
_BARS_REFUSALS = (
    MasterAbsent,
    MasterUnreadable,
    StampNotAnInstant,
    UnsupportedBarFreq,
    NotASession,
    VendorAuthError,
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
    does not change. Nothing writes a verdict until marketlake #138's battery does, so the
    condition has arisen zero times either way.
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

    It reads zero until marketlake #138's battery writes the first verdict, and the live lake
    holds no ``quarantine.jsonl`` at all. A missing ledger reads as no entries, which is what
    keeps this inert rather than raising.
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
    """One corporate-actions walk's result, reduced to the plain values the file carries."""
    return PieceOutcome(
        landed=len(report.appended),
        held=len(report.held),
        unfiled=len(report.unfiled),
        unchanged=report.unchanged,
        skipped=len(getattr(report, "skipped", ())),
        subjects=_subjects(report.held),
    )


def _bars_outcome(report: BarsReport) -> PieceOutcome:
    """The bar fetch's result, reduced the same way."""
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
    """

    nightly: Nightly
    digest: Message
    delivered: bool
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
        for name, walk in ((DIVIDENDS_PIECE, extract_dividends), (SPLITS_PIECE, detect_splits)):
            try:
                pieces.append((name, _ledger_outcome(walk(lake_root=root, clock=clock))))
            except _LEDGER_REFUSALS as exc:
                pieces.append((name, _refused(exc)))
        if closed:
            try:
                pieces.append(
                    (
                        BARS_PIECE,
                        _bars_outcome(
                            fetch_session_bars(
                                lake_root=root,
                                vendor=vendor_source(),
                                clock=clock,
                                calendar=calendar,
                                roster=roster,
                                session=day,
                            )
                        ),
                    )
                )
            except _BARS_REFUSALS as exc:
                pieces.append((BARS_PIECE, _refused(exc)))
        else:
            # A catch-up run, fired by launchd on the next wake for an 18:30 the machine
            # slept through. Fetching now would ask for a session still in progress.
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
