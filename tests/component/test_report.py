"""The ``reports/`` tree: where a report-tier finding lands before D16 reads it.

The close+5 guard is the first producer. Its findings used to reach one destination, a
print to stderr that launchd files and nothing in this repo reads, and ten minutes later
compaction sealed the day. So these cover the file: that it is written, that a clean run
writes one too, that it lands beside the publisher's records rather than among them, that
two runs on one day both survive, and that an exception's message stops at the boundary.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import close_guard, report
from lake.alert import Message, Publisher, undelivered
from lake.capture_spans import CaptureSpans
from lake.close_guard import GuardOutcome
from lake.manifest import manifest_path
from lake.security_master import SecurityMaster
from lake.session import SessionClock
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock

WEEK = date(2026, 8, 31)
DAY = date(2026, 9, 2)
# Close+5 on the day above, the minute the guard is dispatched on.
AT = et(2026, 9, 2, 16, 20)

FINDINGS = GuardOutcome(
    DAY,
    filled=("SPY",),
    unobserved=("QQQ",),
    baseline_less=("IWM",),
    shortfalls=("SPY: 2 expirations",),
    refused=("DIA: past close+5",),
    problems=("quotes/XYZ: KeyError",),
)


def _files(root: Path, day: date = DAY) -> list[Path]:
    """Every close+5 report filed for one session day."""
    directory = report.close_guard_dir(root, day)
    return sorted(directory.glob("*.json")) if directory.is_dir() else []


def _entries(root: Path, day: date = DAY) -> list[dict]:
    return [json.loads(path.read_text()) for path in _files(root, day)]


def _guard(root: Path, at: datetime) -> close_guard.CloseGuard:
    """A guard over a lake with one in-scope ticker that captured neither close."""
    master = SecurityMaster()
    spans = CaptureSpans()
    instrument = master.register(
        kind="equity", capture_start=et(2026, 9, 2, 9, 30), valid_from=DAY, ticker="XYZ"
    )
    spans.open_span(instrument, et(2026, 9, 2, 9, 30), False)
    return close_guard.CloseGuard(
        lake_root=root,
        spans=lambda: spans,
        session_clock=SessionClock(clock=ManualClock(start=at), calendar=weekday_sessions(WEEK)),
        master=lambda: master,
        pid=9,
    )


# -- 1. a run with findings writes a file naming each of them -------------------------


def test_every_finding_reaches_the_file(lake_root):
    """The whole outcome goes down, not the half a caller happened to print.

    Five of the guard's fields are report-tier and the sixth says what it repaired. A
    writer that filed only ``problems`` would drop a close nobody observed, which is the
    one finding the guard exists to record.
    """
    report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["day"] == "2026-09-02"
    assert entry["reportable"] is True
    assert entry["filled"] == ["SPY"]
    assert entry["unobserved"] == ["QQQ"]
    assert entry["baseline_less"] == ["IWM"]
    assert entry["shortfalls"] == ["SPY: 2 expirations"]
    assert entry["refused"] == ["DIA: past close+5"]
    assert entry["problems"] == ["quotes/XYZ: KeyError"]
    assert entry["at"].startswith("2026-09-02T16:20")


def test_a_real_run_files_what_it_found(lake_root):
    """The same over a guard run rather than a hand-built outcome.

    A field renamed on ``GuardOutcome`` and not here would leave every case above green
    while the file went quiet about the finding. This one composes the outcome the way
    production does.
    """
    outcome = _guard(lake_root, AT).run(DAY)
    assert outcome.unobserved == ("XYZ",), outcome

    report.write_close_guard(lake_root, outcome, now=AT, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["unobserved"] == ["XYZ"]
    assert entry["reportable"] is True


def test_the_file_lands_under_the_guards_own_dated_directory(lake_root):
    """``reports/close_guard/date=D/``. The date is the day the guard ran for."""
    path = report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)

    assert path.relative_to(lake_root).parts[:3] == ("reports", "close_guard", "date=2026-09-02")


def test_the_day_keying_the_directory_is_the_guards_day_not_the_clocks(lake_root):
    """A restart writes yesterday's findings into yesterday's directory.

    The two agree on an ordinary day, so keying off the clock passes every other case
    here. A reader asking what happened on the 2nd wants the run that judged the 2nd,
    whenever it was written.
    """
    after_midnight = et(2026, 9, 3, 0, 30)
    path = report.write_close_guard(lake_root, FINDINGS, now=after_midnight, pid=11)

    assert path.parent.name == "date=2026-09-02"
    (entry,) = _entries(lake_root)
    assert entry["at"].startswith("2026-09-03T00:30")
    # The field and the directory have to agree. A record filed under the 2nd whose own
    # contents say the 3rd hands a reader two answers and no way to pick.
    assert entry["day"] == "2026-09-02"


# -- 2. a clean run still writes a file ----------------------------------------------


def test_a_clean_run_writes_a_file_too(lake_root):
    """An absent file has to mean the guard never ran.

    ``reportable`` is false on a day where both closes landed. Writing only on findings
    makes "no file" ambiguous between a run that found nothing and a run that never
    happened, which is the hole-versus-row distinction this lake refuses everywhere else.
    """
    clean = GuardOutcome(DAY, filled=("SPY",))
    assert not clean.reportable

    report.write_close_guard(lake_root, clean, now=AT, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["reportable"] is False
    assert entry["filled"] == ["SPY"]
    assert entry["unobserved"] == []


# -- 3. the file is not a page that failed to send -----------------------------------


def test_a_guard_report_never_counts_as_a_page_that_failed_to_send(lake_root):
    """``reports/alerts/`` is the publisher's, and the Now panel counts what is in it.

    ``alert.undelivered`` counts files rather than reading them, and the Now panel shows
    that count as ``pages_failed_to_send``. A guard finding filed there would report a
    page failure that never happened, on a day the phone worked.
    """
    publisher = Publisher(lake_root=lake_root, transport=None, pid=1)
    publisher.publish(Message(event="capture_down", title="Capture down", body="3 minutes"), now=AT)
    before = undelivered(lake_root, DAY)
    assert before == 1, "the baseline page record did not land"

    report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)

    assert undelivered(lake_root, DAY) == before, "a guard finding was counted as a lost page"
    assert _files(lake_root), "the guard finding was not written at all"


def test_a_guard_report_alone_leaves_the_page_count_at_zero(lake_root):
    """The ordinary day: the phone worked and the guard had something to say."""
    report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)

    assert undelivered(lake_root, DAY) == 0
    # Without this the case passes on an empty lake, so a writer that did nothing at all
    # would satisfy it.
    assert _files(lake_root), "the guard finding was not written at all"


# -- 4. two runs on one day do not collide -------------------------------------------


def test_a_restart_files_its_own_run_beside_the_first(lake_root):
    """A restart builds a fresh ``SessionDispatch``, so one day can be served twice.

    Naming the file after the day alone would let the second run overwrite the first,
    and the two are different runs against different lakes: the first marked what it
    could, the second saw what the first left. Both are the record.
    """
    report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)
    report.write_close_guard(
        lake_root,
        GuardOutcome(DAY, refused=("XYZ: past close+5",)),
        now=et(2026, 9, 2, 16, 41),
        pid=12,
    )

    entries = _entries(lake_root)
    assert len(entries) == 2, "the second run overwrote the first"
    assert {entry["at"][11:16] for entry in entries} == {"16:20", "16:41"}


def test_a_file_is_never_written_over(lake_root):
    """Write-once, the rule every file the lake lands outside the journal follows.

    The stamp carries microseconds and the writing process's id, so this shape is not
    reachable in production. It is pinned because the mode that guarantees it is one
    character, and an ``open(..., "w")`` would pass every other case here while silently
    replacing a run's findings.
    """
    report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)

    with pytest.raises(FileExistsError):
        report.write_close_guard(lake_root, GuardOutcome(DAY), now=AT, pid=11)

    assert _entries(lake_root)[0]["reportable"] is True, "the first run's findings were replaced"


# -- 5. a write that fails costs the file and nothing else ---------------------------


def test_a_missing_lake_root_is_refused_rather_than_created(lake_root):
    """A writer that conjured the root would turn a broken install into a green check.

    The Sunday job decides whether to ping on ``root.is_dir()`` and re-reads it on every
    retry. ``mkdir(parents=True)`` from a missing root would create the lake, so the next
    attempt would find a directory and report a healthy lake holding nothing.
    """
    missing = lake_root / "gone"

    with pytest.raises(FileNotFoundError):
        report.write_close_guard(missing, FINDINGS, now=AT, pid=11)

    assert not missing.exists(), "the writer created the lake it was asked to write into"


def test_a_write_that_cannot_land_raises_rather_than_going_quiet(lake_root):
    """The daemon's dispatch reports what this raises. Swallowing it hides the loss.

    A file blocking the directory the day's reports go in is the reachable shape: a
    filesystem where the name is taken by something that is not a directory.
    """
    blocked = lake_root / "reports" / "close_guard"
    blocked.parent.mkdir(parents=True)
    blocked.write_text("not a directory\n")

    with pytest.raises(OSError):
        report.write_close_guard(lake_root, FINDINGS, now=AT, pid=11)


# -- the exception message stops here ------------------------------------------------


def test_an_exception_message_never_reaches_the_file(lake_root):
    """The file keeps the class. The message can carry a path off this machine.

    ``latest_entries`` raises on a manifest line with no ``partition`` key, and the guard
    records ``prologue: <class>: <message>``. An ``OSError`` on the same read names the
    file it failed on, which is an absolute path on the capture machine, and this tree is
    one the dashboard may read. So the run is driven for real and the file is checked for
    what the exception said.
    """
    manifest_path(lake_root).write_text('{"source": "compaction", "rows": 406}\n')

    outcome = _guard(lake_root, AT).run(DAY)
    assert outcome.problems[0].startswith("prologue: ManifestError: "), outcome.problems
    assert "entry 1" in outcome.problems[0], "stderr lost the message it is meant to keep"

    report.write_close_guard(lake_root, outcome, now=AT, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["problems"] == ["prologue: ManifestError"]
    assert "entry 1" not in json.dumps(entry), "the exception's message rode into the file"


def test_a_problem_naming_no_exception_survives_whole(lake_root):
    """An unreadable-segment problem has no message to drop, and says nothing without it.

    Redaction can only ever shorten a problem, so the risk runs the other way: a rule
    that cut at the first colon would file a bare surface and ticker for every finding
    the guard makes about damaged segments.

    The lines are the real shape ``journal.describe_unusable`` builds, breakout and all,
    because the breakout is the part that says whether to look at a disk or at the vendor's
    payload and it sits past the colon where the redaction rule cuts.
    """
    outcome = GuardOutcome(
        DAY,
        problems=(
            "quotes/XYZ: 2 unreadable (1 drifted, 1 corrupt)",
            "chains/SPY: 1 unreadable (1 drifted)",
        ),
    )

    report.write_close_guard(lake_root, outcome, now=AT, pid=11)

    assert _entries(lake_root)[0]["problems"] == [
        "quotes/XYZ: 2 unreadable (1 drifted, 1 corrupt)",
        "chains/SPY: 1 unreadable (1 drifted)",
    ]


# -- the instant: Eastern, and the guard's day ---------------------------------------


def test_a_utc_instant_is_filed_in_eastern(lake_root):
    """The daemon's clock is UTC, and every other case here hands this one Eastern.

    ``SystemClock.now`` returns ``datetime.now(UTC)``, so the instant production passes is
    four hours ahead of the wall clock the lake is keyed on in September. Dropping the
    conversion is invisible to a suite that only ever passes Eastern, and it moves both
    halves of the record: close+5 files itself as 20:20 and ``at`` carries ``+00:00``.
    """
    utc_close_plus_five = datetime(2026, 9, 2, 20, 20, tzinfo=UTC)

    path = report.write_close_guard(lake_root, FINDINGS, now=utc_close_plus_five, pid=11)

    assert path.name.startswith("1620"), "the file name was stamped in the wrong zone"
    (entry,) = _entries(lake_root)
    assert entry["at"] == "2026-09-02T16:20:00-04:00"
    assert datetime.fromisoformat(entry["at"]).utcoffset() is not None, "the offset was dropped"


def test_two_days_are_filed_under_two_directories(lake_root):
    """Every other case here uses one date, so a hardcoded one would satisfy them all."""
    report.write_close_guard(lake_root, GuardOutcome(DAY), now=AT, pid=11)
    next_day = date(2026, 9, 3)
    report.write_close_guard(lake_root, GuardOutcome(next_day), now=et(2026, 9, 3, 16, 20), pid=11)

    tree = Path(lake_root) / "reports" / "close_guard"
    assert sorted(child.name for child in tree.iterdir()) == [
        "date=2026-09-02",
        "date=2026-09-03",
    ]
    assert _entries(lake_root, next_day)[0]["day"] == "2026-09-03"


# -- the name: microseconds and the writing process ----------------------------------


def test_two_processes_writing_at_one_instant_both_land(lake_root):
    """A restart inside the same minute is what the pid in the name is for.

    The restart case above is twenty-one minutes apart, so it separates on the stamp
    alone and cannot tell whether the pid is carried. Two daemons reaching close+5 inside
    one microsecond is the shape that needs it, and under a name without the pid the
    second run's findings are lost to a ``FileExistsError``.
    """
    report.write_close_guard(lake_root, GuardOutcome(DAY, unobserved=("XYZ",)), now=AT, pid=11)
    report.write_close_guard(lake_root, GuardOutcome(DAY, unobserved=("ABC",)), now=AT, pid=12)

    entries = _entries(lake_root)
    assert len(entries) == 2, "the second process overwrote or collided with the first"
    assert {entry["unobserved"][0] for entry in entries} == {"XYZ", "ABC"}


def test_two_writes_a_microsecond_apart_both_land(lake_root):
    """A slot's stamp carries no sub-minute part, which is why the name carries one.

    One process writing twice inside a minute is not reachable through the dispatcher
    today, and the name is what keeps it from becoming a loss if it ever is.
    """
    report.write_close_guard(lake_root, GuardOutcome(DAY), now=AT, pid=11)
    report.write_close_guard(lake_root, GuardOutcome(DAY), now=AT.replace(microsecond=1), pid=11)

    assert len(_files(lake_root)) == 2, "the sub-minute part of the stamp was dropped"


def test_the_default_pid_is_the_writing_process(lake_root):
    """The daemon passes no pid, so the default is the one production files under."""
    path = report.write_close_guard(lake_root, FINDINGS, now=AT)

    assert path.name.endswith(f"-{os.getpid()}.json"), path.name


# -- reportable: the field a nightly reader filters on -------------------------------


@pytest.mark.parametrize(
    "field",
    ["unobserved", "baseline_less", "shortfalls", "refused", "problems"],
)
def test_any_one_finding_files_the_day_as_reportable(lake_root, field):
    """Each clause on its own, because the filed field is what D16 will filter on.

    A fixture with every field populated cannot say which clause drove the value, so a
    day whose only finding is a refusal could file as ``reportable: false`` and drop out
    of the nightly report with its ``refused`` list sitting unread in the file.
    """
    outcome = GuardOutcome(DAY, **{field: ("XYZ",)})
    assert outcome.reportable, f"{field} alone did not count as worth reporting"

    report.write_close_guard(lake_root, outcome, now=AT, pid=11)

    entry = _entries(lake_root)[0]
    assert entry["reportable"] is True
    assert entry[field] == ["XYZ"]


def test_a_run_that_only_filled_is_not_reportable(lake_root):
    """The other side of the same field. A repaired close is not a finding."""
    report.write_close_guard(lake_root, GuardOutcome(DAY, filled=("XYZ",)), now=AT, pid=11)

    assert _entries(lake_root)[0]["reportable"] is False


# -- the refusal, and the branch redaction keeps for a shape it was not written for --


def test_a_lake_root_that_is_a_file_is_refused(lake_root):
    """``is_dir`` rather than ``exists``, and the two differ on exactly this.

    A regular file where the lake root should be is the shape a half-finished install
    leaves. ``exists`` calls it present and the failure then comes out of ``mkdir`` as a
    different exception, past the deliberate refusal.
    """
    not_a_lake = lake_root / "lake.txt"
    not_a_lake.write_text("not a lake\n")

    with pytest.raises(FileNotFoundError):
        report.write_close_guard(not_a_lake, FINDINGS, now=AT, pid=11)


def test_a_problem_with_no_separator_at_all_survives_verbatim(lake_root):
    """Redaction can only shorten, so the shape it was not written for must keep its text.

    Every problem the guard composes today carries a place and a colon. This is the
    branch that decides what happens to one that does not, and a rule that returned an
    empty string there would erase the only thing the run had to say.
    """
    report.write_close_guard(lake_root, GuardOutcome(DAY, problems=("boom",)), now=AT, pid=11)

    assert _entries(lake_root)[0]["problems"] == ["boom"]


# -- the nightly report file ------------------------------------------------------------

NIGHT = report.Nightly(
    day=DAY,
    session=True,
    pinged=True,
    gaps=3,
    quarantined=0,
    pages_lost=1,
    pieces=(
        (report.DIVIDENDS_PIECE, report.PieceOutcome(landed=1, unchanged=4)),
        (
            report.BARS_PIECE,
            report.PieceOutcome(
                held=2,
                unfiled=1,
                subjects=("SPY 2026-09-02 bar_close", "QQQ 2026-09-02 bar_close"),
            ),
        ),
    ),
    problems=("ping failed: OSError",),
    report=("weekday wake repeat alarm missing",),
)


def _nightly_entries(root: Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((root / "reports").glob("*.json"))]


def test_the_nightly_file_lands_at_the_tree_root_and_no_glob_of_a_producer_finds_it(lake_root):
    """The four named subdirectories are what the counting globs read, and this is not one.

    ``undelivered`` counts ``reports/alerts/``, and a nightly file placed inside any
    producer's directory would inflate that producer's count. A flat file at the root is
    reached by ``reports/*.json`` and by nothing else.
    """
    path = report.write_nightly(lake_root, NIGHT, now=AT, pid=11)

    assert path.parent == lake_root / "reports"
    assert path.name == f"{DAY.isoformat()}-162000000000-11.json"
    assert undelivered(lake_root, DAY) == 0
    assert list(report.close_guard_dir(lake_root, DAY).glob("*.json")) == []


def test_the_nightly_file_carries_every_count_and_the_detail_the_digest_drops(lake_root):
    """The fields are named here so the History panel's query is not inventing them."""
    report.write_nightly(lake_root, NIGHT, now=AT, pid=11)

    (entry,) = _nightly_entries(lake_root)
    assert entry["day"] == DAY.isoformat()
    assert entry["session"] is True
    assert entry["pinged"] is True
    assert entry["gaps"] == 3
    assert entry["quarantined"] == 0
    assert entry["pages_lost"] == 1
    assert entry["problems"] == ["ping failed: OSError"]
    assert entry["report"] == ["weekday wake repeat alarm missing"]
    assert entry["pieces"]["bars"]["subjects"] == [
        "SPY 2026-09-02 bar_close",
        "QQQ 2026-09-02 bar_close",
    ]
    assert entry["pieces"]["dividends"]["landed"] == 1


def test_the_disagreement_count_is_derived_so_it_cannot_disagree_with_the_pieces(lake_root):
    """Storing it beside the pieces is how the two drift. It is summed off them instead."""
    assert NIGHT.disagreements == 2
    assert NIGHT.unfiled == 1

    report.write_nightly(lake_root, NIGHT, now=AT, pid=11)
    (entry,) = _nightly_entries(lake_root)
    assert entry["disagreements"] == sum(p["held"] for p in entry["pieces"].values())


def test_an_unsealed_day_files_a_null_gap_count_rather_than_zero(lake_root):
    """Zero is a clean day. ``None`` is a day compaction never sealed, and the two differ."""
    report.write_nightly(lake_root, report.Nightly(DAY, True, True), now=AT, pid=11)

    (entry,) = _nightly_entries(lake_root)
    assert entry["gaps"] is None


def test_a_holiday_files_no_pieces_and_says_it_was_not_a_session(lake_root):
    """An absent file means the run never happened, so a no-op writes one too."""
    report.write_nightly(lake_root, report.Nightly(DAY, False, True), now=AT, pid=11)

    (entry,) = _nightly_entries(lake_root)
    assert entry["session"] is False
    assert entry["pieces"] == {}


def test_two_runs_on_one_night_both_survive(lake_root):
    """A name keyed on the day alone would raise on the second, because the writer opens `x`.

    Two runs are two verdicts. This directory has no resolution step, the way the repeats
    under ``withheld/`` have none.
    """
    report.write_nightly(lake_root, NIGHT, now=AT, pid=11)
    report.write_nightly(lake_root, NIGHT, now=AT, pid=12)

    assert len(_nightly_entries(lake_root)) == 2


def test_an_exception_message_stops_at_the_boundary(lake_root):
    """Two redactions, because the two fields carry a message in different shapes.

    ``problems`` is a place and then an exception, so ``_redacted``'s keep-two-fields rule
    cuts it. A refusal is the exception alone, which is exactly two fields, so that rule
    would pass it through whole and ``refusal_class`` keeps the class instead. An
    ``OSError`` says the filename it failed on, which is an absolute path on the capture
    machine, and this file sits in the directories the dashboard may read.
    """
    leaky = report.Nightly(
        DAY,
        True,
        False,
        pieces=(
            (
                report.BARS_PIECE,
                report.PieceOutcome(refusal="OSError: [Errno 13] /Users/someone/secret"),
            ),
        ),
        problems=("bars did not run: OSError: [Errno 13] /Users/someone/secret",),
    )
    report.write_nightly(lake_root, leaky, now=AT, pid=11)

    (entry,) = _nightly_entries(lake_root)
    assert "/Users/someone" not in json.dumps(entry)
    assert entry["pieces"]["bars"]["refusal"] == "OSError"
    assert entry["problems"] == ["bars did not run: OSError"]


def test_a_missing_lake_root_refuses_rather_than_conjuring_one(tmp_path):
    """``write_close_guard`` and ``alert._record`` refuse on the same test for the same reason.

    ``parents=True`` from a missing root would create the lake, and the Sunday job decides
    whether to ping on ``root.is_dir()``.
    """
    with pytest.raises(FileNotFoundError, match="lake root missing"):
        report.write_nightly(tmp_path / "gone", NIGHT, now=AT, pid=11)
