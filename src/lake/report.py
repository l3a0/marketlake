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
   writes under a new pid. A producer that writes several findings in one run needs more.
   ``alert._record`` adds a per-message sequence, because one cycle can raise several pages
   at one instant, and the withheld producer below adds a subject and a sequence both.

The second producer is compaction's merge. It compares a ticker-day's merged segments to
the pinned schema at the one moment the segments still exist, and files what moved. A
merge the segments' own types refused files here too, from the sweep rather than from the
seal, because a refusal writes no partition and so never reaches that comparison. The file
has forensic value from the day it lands, because the merged schema is gone the moment the
seal unlinks the segments, and it has no reader until D20 renders it. What
reaches a human in the meantime is compaction's own page, which folds the run's findings
into one message and sends the reader here for the per-ticker-day detail.

The third producer is the vendor sweep's gates. A check that refuses to land a row holds
that row out of its ledger, and a fail-closed decision leaving no record reads exactly like
never having seen the event. So the refusal is filed here. Two callers reach it, the
dividend extraction and the bars close check, and a held finding appends nothing, so
neither leaves a manifest entry the way a seal does. A night that learns nothing files
nothing either, and what says the run happened is the sweep's own ping rather than a marker
file.

The fourth producer is the vendor sweep itself, and it writes the dated report file at the
tree's root rather than in a subdirectory of its own. That file is the run's whole record: the
counts the digest sends to the phone, the per-piece detail the digest's byte budget keeps off
it, and the report-tier findings the design says send no message at all. It writes on every
run including a holiday no-op, for the close+5 guard's reason, since an absent file cannot be
told from a run that never happened.

It sits at the root because nothing else does. Four named subdirectories sit under it, so a
reader globbing ``reports/*.json`` picks up the nightly files and nothing else, and the four
counting globs each name their own directory.

A held finding recurs every night, because nothing settles it, and nothing under
``reports/`` is ever pruned. So one unresolved disagreement is thirty files in one
directory after a month, and that repetition is the record rather than a defect to design
around. Compaction's refused ticker-day already repeats the same way. One file says a
finding was held at some point and thirty say it was held again last night. What the name
needs on top of the stamp and the pid is a subject and a sequence, because that producer
writes several findings at one instant and the clock alone cannot name them apart.
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

# The vendor sweep's gates. Its own subdirectory, per rule 2 above, so a finding a gate
# refused never counts as a page that failed to send.
WITHHELD_DIR = "withheld"


@dataclass(frozen=True)
class SchemaDrift:
    """One ticker-day the merge had something to say about.

    Two of the three shapes are a merged schema that is not the pinned one. The third is a
    ticker-day whose segments disagreed with each other and were merged on a human's
    authority, and its merged schema can be the pinned one exactly.

    ``GuardOutcome`` lives in ``close_guard`` and this record lives here, and the
    asymmetry is the import direction. The guard's producer never learns about this
    module, because the daemon wires the two together. Compaction's producers are inside
    ``compact``, so it imports this module directly and a record defined there would close
    the loop. There are two of them. One sits in ``_seal`` and reports a merged schema that
    is not the pinned one, or a widening an operator authorized, or both at once. The other
    sits in the sweep and reports a merge the segments' own types refused. The dataclass
    carries strings and dates alone, which keeps pyarrow out of the module that writes JSON.

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

    A disagreement the repair was authorized to merge reaches the first producer rather
    than the second. ``recompact_ticker_day(allow_retype=True)`` promotes the column
    instead of refusing, so a merged schema exists again and is compared to the pinned
    one, which renders it ``pinned -> merged`` whenever the promoted type is not the
    pinned one. Such a record carries ``refused`` false, because nothing was refused.

    ``widened`` is the fourth column list and the only one that is not a comparison
    against the pinned schema. It names what an authorized promotion moved, rendered
    ``segment -> promoted`` once per distinct type a segment held the column at, so a
    reader sees both types the segments disagreed about and the type they were merged to.
    The other three fields cannot say this, because all three compare the merged schema to
    the pinned one and the disagreement here is between two segments. Which is why the
    record a widening onto the pinned type leaves needed a field of its own: the merged
    schema and the pinned one are then equal, the three difference fields have nothing to
    report, and without this one the repair would seal a ticker-day whose segments
    disagreed and leave no report at all.

    So a record exists whenever ``widened`` is non-empty, whether or not the merged schema
    differs from the pinned one. A widening past the pinned type fills ``retyped`` as well,
    and the two fields then say different things about the same column. ``retyped`` says
    the pinned schema is now narrower than the partition and wants a schema bump.
    ``widened`` says the segments disagreed and a human authorized the merge, which no
    schema bump clears and none is owed for.

    All three can be empty, and each producer has a way of getting there. The merged
    producer files on either of two conditions, a difference against the pinned schema or a
    widening, and these three fields explain only the first. So a record lists nothing here
    when it was filed for the widening alone, and again when the schemas differ by
    something the names and the types do not show. A nullability change is that second
    difference. The refusal's producer scans the segments to explain a refusal Arrow
    already made, so a refusal it cannot model lists nothing either. All are still the
    finding: something was worth saying at the merge and this says which ticker-day to go
    and look at.

    ``carries_pinned`` separates those two empty-handed records, and a reader needs them
    apart because they want opposite things. A record filed for the widening alone carries
    the pinned schema exactly, so there is nothing to correct. A nullability difference does
    not, and the schema is what has to move. Nothing else on the record can tell the two
    apart, which is why the fact is carried here rather than inferred from the three lists
    being empty. It is false on a refused record, which has no merged schema to carry
    anything.
    """

    surface: str
    ticker: str
    day: date
    partition: str
    schema_version: int
    missing: tuple[str, ...] = ()
    unexpected: tuple[str, ...] = ()
    retyped: tuple[str, ...] = ()
    widened: tuple[str, ...] = ()
    segments: tuple[str, ...] = field(default_factory=tuple)
    refused: bool = False
    carries_pinned: bool = False


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

    **An authorized widening writes even when the merge came out clean.** A repair run
    with ``allow_retype`` on merges segments that disagreed about a column's type, and the
    human who authorized it usually pins the wider type first, so the merged schema then
    equals the pinned one and the three difference fields find nothing. That partition is
    still not an ordinary seal, so the caller files on ``widened`` alone. It files once,
    for the same reason a drifted seal does: the partition has a manifest entry behind it,
    so a later silence is readable.

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
        "widened": list(drift.widened),
        "segments": list(drift.segments),
        "refused": drift.refused,
        "carries_pinned": drift.carries_pinned,
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


@dataclass(frozen=True)
class Withheld:
    """One thing a gate refused to land, and enough to say why.

    Two deliverables file through this record and they hold different kinds of thing. One
    holds a corporate action, the other a bar whose official close disagrees with the
    session's own captured quotes. So the record is named for the category rather than for
    either of them, the way ``WITHHELD_DIR`` is. A field called ``ex_date`` would fit the
    first and mis-describe the second.

    ``symbol`` and ``observed_on`` come first because every finding carries them. Two of the
    three shapes the dividend extraction files are the resolution itself failing, and
    ``UnresolvedSymbol`` and ``AmbiguousSymbol`` both carry a symbol and a date. The
    instrument id is the thing that could not be determined there, so it rides the record
    when there is one and is left out when there is not, and it stays out of the file name
    for the same reason. ``AmbiguousSymbol`` carries a third field, the several instruments
    a corrupt master returned for one symbol, and ``instrument_ids`` files that as the
    plural it is.

    ``observed_on`` is the ticker-day whose rows produced the finding, never the night the
    gate ran and never the date the event itself happened.

    ``event`` is the key the finding recurs under, such as the kind of action a held entry
    would have landed as. It rides in the file name beside the symbol, so it is a plain
    token rather than a sentence. The two together are the subject, and the subject is what
    tells one file from another in a directory a month of nights has filled.

    ``check`` names what refused the finding, and ``computed`` and ``against`` are the two
    numbers that check compared. Those are three fields rather than one joined string
    because :func:`_redacted` cuts at the second ``": "``, so a finding written as
    ``instrument 42: 7.61406 against 7.61408: dividend_consistency`` would arrive without
    the check that refused it, silently.

    ``exception`` carries an exception the caller met, rendered as its class and then its
    message. What reaches the file is the first two fields of ``<symbol>: <exception>``, so
    that rendering files the class and drops the message. The rendering is the caller's
    contract rather than something the writer can enforce, and it is the contract the close+5
    guard already meets when it composes a problem as a place, a class, and a message.
    ``_redacted`` keeps two fields, so the second is whatever the caller put first. A caller
    handing over a bare message files that message's first field, which for an ``OSError`` is
    a path on the capture machine and is the leak this tree's redaction exists to stop. An
    empty rendering files no field at all rather than a finding claiming an exception with no
    class.

    The record carries strings, numbers and one date, which keeps this module writing JSON
    and nothing else. It lives here rather than beside either of those two exceptions, for
    the reason ``SchemaDrift`` does, which is the import direction. The extraction imports
    this module to file its findings, so a record defined in ``actions`` would close the
    loop. ``security_master`` closes no loop, importing neither this module nor ``actions``,
    and a record there would sit in the module that knows least about what is being filed.
    """

    symbol: str
    observed_on: date
    event: str
    check: str
    computed: float | None = None
    against: float | None = None
    instrument_id: int | None = None
    instrument_ids: tuple[int, ...] = ()
    exception: str | None = None


def withheld_dir(lake_root: Path | str, day: date) -> Path:
    """Where one ticker-day's withheld findings are filed.

    Keyed on the day the rows belong to, the way :func:`schema_drift_dir` is. SPY's June
    ex-date sits three months before the lake's first data row, so keying on the event's own
    date would open a directory for a session the lake never captured. It does not apply
    uniformly either, since a symbol the master could not place carries no event date at all.
    """
    return Path(lake_root) / REPORTS_DIR / WITHHELD_DIR / f"{DATE_PREFIX}{day.isoformat()}"


def write_withheld(
    lake_root: Path | str,
    finding: Withheld,
    *,
    now: datetime,
    sequence: int,
    pid: int | None = None,
) -> Path:
    """File one withheld finding, and hand back the path it landed at.

    **Only a finding writes anything.** The close+5 guard files on every run, because an
    absent file there cannot be told from a run that never happened. Compaction files on
    findings alone and leans on the manifest entry each seal leaves. Neither applies here,
    because a night that holds nothing appends nothing and so leaves no entry either. What
    says the run happened is the sweep's own health check.

    **A held finding files again every night, and the repetition is the record.** It never
    reaches the ledger that would settle it, so the next night re-derives the same
    disagreement from the same sealed rows. Nothing prunes ``reports/``, so one unresolved
    disagreement is thirty files in one directory after a month. That is the behaviour rather
    than a defect, and compaction already does it, filing a refused ticker-day on every run
    the conflict survives. One file says a finding was held at some point. Thirty say it was
    held again last night, which is the difference between a live condition and a historical
    one. A ledger collapses a repeat because its resolution reads the last entry, so a repeat
    there adds nothing. This directory has no such resolution and every file is one run's
    verdict, so a repeat is a new observation. Reading the pile is the nightly digest's job.

    **The name carries a sequence.** One run files several findings under one injected clock
    that does not advance between them, and under one pid, so the stamp and the pid are
    constant across the run and the subject would be doing all the work. Whether two findings
    can share a subject is a question about another module's output, and a file name is not
    the place to rest on it. ``alert._record`` adds a sequence for the same reason and keeps
    the counter on ``Publisher``. This module has no writer class, only functions, so a
    counter here would be module state outliving the run that no test could drive. The caller
    passes it, the way it already passes the pid, and a caller looping over findings holds
    the index anyway.

    **Raises rather than swallowing.** The caller contains it, for the reason
    :func:`write_schema_drift` gives, because a raise out of the filing costs the rest of the
    sweep and the containment belongs where that blast radius is. The hand-run extraction
    command has no dispatch behind it, so it wraps its own per-finding loop and turns what
    escapes into an exit code.
    """
    pid = os.getpid() if pid is None else pid
    eastern = now.astimezone(MARKET_TZ)
    entry: dict[str, object] = {
        "at": eastern.isoformat(),
        "day": finding.observed_on.isoformat(),
        "symbol": finding.symbol,
        "event": finding.event,
        "check": finding.check,
    }
    if finding.computed is not None:
        entry["computed"] = finding.computed
    if finding.against is not None:
        entry["against"] = finding.against
    if finding.instrument_id is not None:
        entry["instrument_id"] = finding.instrument_id
    if finding.instrument_ids:
        entry["instrument_ids"] = list(finding.instrument_ids)
    if finding.exception:
        # The place, then the class, then whatever the exception chose to say. The place is
        # composed here rather than by the caller, because the rule keeps the first two
        # fields and a caller handing over two would have its message kept instead of
        # dropped. An empty rendering files nothing, rather than a bare place and a colon.
        entry["exception"] = _redacted(f"{finding.symbol}: {finding.exception}")
    # A report is written inside a lake that exists, or not at all. The same rule the two
    # writers above follow, and for the same reason: `parents=True` from a missing root
    # would create the lake itself and turn "lake root missing" into a green check.
    root = Path(lake_root)
    if not root.is_dir():
        raise FileNotFoundError(f"lake root missing: {root}")
    directory = withheld_dir(root, finding.observed_on)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = eastern.strftime("%H%M%S%f")
    path = directory / f"{stamp}-{finding.symbol}-{finding.event}-{sequence:04d}-{pid}.json"
    with open(path, "x", encoding="utf-8") as handle:
        json.dump(entry, handle, sort_keys=True)
        handle.write("\n")
    return path


# The vendor sweep's three walks, named in the order the run makes them. The order is the
# design's, which polls corporate actions first so today's split flags before bars land.
DIVIDENDS_PIECE = "dividends"
SPLITS_PIECE = "splits"
BARS_PIECE = "bars"
PIECES = (DIVIDENDS_PIECE, SPLITS_PIECE, BARS_PIECE)


@dataclass(frozen=True)
class PieceOutcome:
    """What one of the sweep's three walks did, reduced to plain values.

    Plain values rather than the walk's own report, and the import direction is why.
    ``lake.actions``, ``lake.bars`` and ``lake.splits`` each import this module for
    ``Withheld`` and ``write_withheld``, so a record here naming ``BarsReport`` or
    ``ExtractionReport`` would close the loop. ``SchemaDrift`` above reasons the same way
    about ``GuardOutcome``, which lives in ``close_guard`` because the daemon wires the two
    together. ``lake.sweep`` plays that part here.

    ``refusal`` is the named condition that ended the walk, or ``None`` when it finished. It
    is what withholds the sweep's ping, because a walk that did not finish means the day's
    actions or bars really are missing, which is what a missed ``eod-sweep`` ping says.

    ``subjects`` names each held finding, as ``<symbol> <observed_on> <check>``. The digest
    carries counts alone and this is where the detail lands, which is what keeps the digest
    under its byte budget on the night that has many rather than only on the nights that have
    none.
    """

    landed: int = 0
    held: int = 0
    unfiled: int = 0
    unchanged: int = 0
    skipped: int = 0
    subjects: tuple[str, ...] = ()
    refusal: str | None = None

    @property
    def finished(self) -> bool:
        """Whether the walk ran to its end."""
        return self.refusal is None

    @property
    def refusal_class(self) -> str | None:
        """The refusal with the exception's own message dropped, or ``None``.

        A refusal is composed as the exception's class and then whatever that exception
        chose to say, and an ``OSError`` says the filename it failed on, which is an
        absolute path on the capture machine. This file sits in the directories the
        dashboard may read and the digest goes to a phone, so the message stops here and
        the fuller string stays on the job's own stdout for a reader who has the log.

        :func:`_redacted` cannot do it, because it keeps two fields and a refusal has
        exactly two, so its rule would pass this through whole. Its own docstring names
        that limit: a shape it was not written for loses detail rather than leaking it.
        A refusal carrying no message, like the close guard's, has one field and survives.
        """
        if self.refusal is None:
            return None
        kind, _, _ = self.refusal.partition(": ")
        return kind

    def as_entry(self) -> dict:
        """This outcome as the mapping the nightly file carries."""
        entry: dict = {
            "landed": self.landed,
            "held": self.held,
            "unfiled": self.unfiled,
            "unchanged": self.unchanged,
            "skipped": self.skipped,
            "subjects": [_redacted(subject) for subject in self.subjects],
        }
        if self.refusal is not None:
            entry["refusal"] = self.refusal_class
        return entry


@dataclass(frozen=True)
class Nightly:
    """One vendor-sweep run, as the dated report file records it.

    ``day`` is the run's own Eastern date rather than the instant it wrote, which is
    :func:`close_guard_dir`'s rule. The two agree on an ordinary evening and stop agreeing on
    a catch-up run, and the day a reader asking "what happened on the 16th" wants is the
    former. On a session day it is the session. A holiday has no session for it to name,
    which is what ``session`` says.

    ``pieces`` holds a :class:`PieceOutcome` per walk that ran. A walk absent from it did not
    run, which on a holiday is all three: the design has compaction and the sweep no-op on an
    empty journal, and the one-line digest is what settles it, since a run whose walks found
    something would have nowhere to say so.

    ``gaps`` is ``None`` when the day has no sealed partition to count, which is a different
    answer from zero and has to stay one. Compaction seals at close+15, so an absent partition
    at 18:30 says the seal did not happen rather than that the day was clean.

    ``problems`` are what withheld the ping. ``report`` are the report-tier findings, which
    ride this file and send no message of their own. ``SundayOutcome`` carries the same split
    in the same two names, and keeping them one list is what would let a held finding silence
    the check.
    """

    day: date
    session: bool
    pinged: bool
    gaps: int | None = None
    quarantined: int = 0
    pages_lost: int = 0
    pieces: tuple[tuple[str, PieceOutcome], ...] = ()
    problems: tuple[str, ...] = ()
    report: tuple[str, ...] = ()

    @property
    def disagreements(self) -> int:
        """Every finding the run's gates held, across the walks that ran.

        Derived rather than stored, and counted off the walks rather than by globbing
        tonight's ``withheld/`` directory. :func:`withheld_dir` keys its path on the day the
        rows belong to, so a dividend disagreement about a June ex-date re-files tonight under
        that session's date. A glob of tonight's would read zero while the condition is live.
        """
        return sum(outcome.held for _, outcome in self.pieces)

    @property
    def unfiled(self) -> int:
        """Every held finding whose record could not be written down."""
        return sum(outcome.unfiled for _, outcome in self.pieces)


def nightly_path(lake_root: Path | str, day: date, *, stamp: str, pid: int) -> Path:
    """Where one vendor-sweep run's report file lands.

    At the ``reports/`` root, named by the day it is about and then by the stamp and the pid,
    which is rule 3 above. A name keyed on the day alone would collide with a second run the
    same night, and the second run is a second verdict rather than a correction: this
    directory has no resolution step, so every file in it is one run's answer, the way the
    repeats under ``withheld/`` are.
    """
    return Path(lake_root) / REPORTS_DIR / f"{day.isoformat()}-{stamp}-{pid}.json"


def write_nightly(
    lake_root: Path | str,
    nightly: Nightly,
    *,
    now: datetime,
    pid: int | None = None,
) -> Path:
    """File one vendor-sweep run's report, and hand back the path it landed at.

    **Raises rather than swallowing**, the way :func:`write_close_guard` does. The caller is
    the sweep, the run is finished by the time this is called, and the sweep turns the failure
    into an exit code and a line in the digest. Catching here would hide the failure from the
    one thing left that can report it, since the digest is the other copy of these counts.
    """
    pid = os.getpid() if pid is None else pid
    eastern = now.astimezone(MARKET_TZ)
    entry = {
        "at": eastern.isoformat(),
        "day": nightly.day.isoformat(),
        "session": nightly.session,
        "pinged": nightly.pinged,
        "gaps": nightly.gaps,
        "quarantined": nightly.quarantined,
        "disagreements": nightly.disagreements,
        "pages_lost": nightly.pages_lost,
        "pieces": {name: outcome.as_entry() for name, outcome in nightly.pieces},
        "problems": [_redacted(problem) for problem in nightly.problems],
        "report": [_redacted(line) for line in nightly.report],
    }
    # `parents=True` from a missing lake root would create the lake itself, which
    # `write_close_guard` and `alert._record` both refuse for the reason given there. A
    # report is written inside a lake that exists, or not at all.
    root = Path(lake_root)
    if not root.is_dir():
        raise FileNotFoundError(f"lake root missing: {root}")
    directory = root / REPORTS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = nightly_path(root, nightly.day, stamp=eastern.strftime("%H%M%S%f"), pid=pid)
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
    "BARS_PIECE",
    "CLOSE_GUARD_DIR",
    "DIVIDENDS_PIECE",
    "PIECES",
    "SCHEMA_DRIFT_DIR",
    "SPLITS_PIECE",
    "WITHHELD_DIR",
    "Nightly",
    "PieceOutcome",
    "SchemaDrift",
    "Withheld",
    "close_guard_dir",
    "nightly_path",
    "schema_drift_dir",
    "withheld_dir",
    "write_close_guard",
    "write_nightly",
    "write_schema_drift",
    "write_withheld",
]
