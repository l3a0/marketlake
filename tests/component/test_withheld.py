"""The third ``reports/`` producer: what a gate refused to land, written where a human reads it.

A fail-closed gate holds a row out of its ledger, and a decision that leaves no record
reads exactly like never having seen the event. Two deliverables file through this one
producer, a held corporate action and a bar whose close disagrees with the session's own
quotes, so it ships once ahead of both.

Seven things are covered.

1. A finding lands under its own subdirectory and not among the pages that failed to send.
2. Two findings in one run land as two files, each name carrying its own subject.
3. An exception on a finding is redacted to its class.
4. A write that cannot land raises rather than going quiet.
5. Two findings sharing one subject, one clock and one pid still land as two files.
6. A missing lake root is refused rather than created.
7. ``at`` and ``day`` carry market time, and so does the stamp in the file name.

An eighth pins the decision the seven above do not reach. A held finding is filed again on
every night it survives, because the repetition is what says the condition is live.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import report
from lake.actions import UnresolvedSymbol
from lake.alert import Message, Publisher, undelivered
from lake.report import Withheld
from tests.support.calendar import et

# The ticker-day whose rows produced the finding, and the evening the sweep judged them.
DAY = date(2026, 9, 14)
AT = et(2026, 9, 14, 20, 0)

# The dividend the gate refused: the vendor's annualized figure against four times its own
# per-event amount, which is the disagreement #284's check makes.
HELD = Withheld(
    symbol="SPY",
    observed_on=DAY,
    event="dividend",
    check="dividend_consistency",
    computed=7.61406,
    against=7.61408,
    instrument_id=42,
)


def _files(root: Path, day: date = DAY) -> list[Path]:
    """Every withheld finding filed for one ticker-day."""
    directory = report.withheld_dir(root, day)
    return sorted(directory.glob("*.json")) if directory.is_dir() else []


def _entries(root: Path, day: date = DAY) -> list[dict]:
    return [json.loads(path.read_text()) for path in _files(root, day)]


# -- 1. the finding lands in its own subdirectory ------------------------------------


def test_a_finding_lands_under_its_own_dated_directory(lake_root):
    """``reports/withheld/date=D/``, keyed on the day the rows belong to."""
    path = report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)

    assert path.relative_to(lake_root).parts[:3] == ("reports", "withheld", "date=2026-09-14")


def test_the_name_carries_the_stamp_the_subject_the_sequence_and_the_pid(lake_root):
    """Every part, in order, against one literal.

    Each part is argued for somewhere and each was separately droppable while the rest of
    this file stayed green. The stamp can lose its microseconds, the event half of the
    subject can leave, and the symbol and the event can swap, none of which any other case
    here reads. One expected name holds all four parts and their order at once.
    """
    path = report.write_withheld(lake_root, HELD, now=AT, sequence=7, pid=11)

    assert path.name == "200000000000-SPY-dividend-0007-11.json"


def test_the_file_ends_with_a_newline(lake_root):
    """One JSON object and a newline, the way the two writers above leave their files.

    ``json.loads`` does not care, so every other case here passes without it. What cares is
    a human reading the tree and the line-oriented tools they read it with.
    """
    path = report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)

    assert path.read_text().endswith("}\n")


def test_a_withheld_finding_never_counts_as_a_page_that_failed_to_send(lake_root):
    """``alert.undelivered`` counts files under ``reports/alerts/`` rather than reading them.

    The Now panel shows that count as ``pages_failed_to_send``. A finding filed there would
    report a page failure that never happened, on a day the phone worked.
    """
    publisher = Publisher(lake_root=lake_root, transport=None, pid=1)
    publisher.publish(Message(event="capture_down", title="Capture down", body="3 minutes"), now=AT)
    before = undelivered(lake_root, DAY)
    assert before == 1, "the baseline page record did not land"

    report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)

    assert undelivered(lake_root, DAY) == before, "a withheld finding was counted as a lost page"
    assert _files(lake_root), "the finding was not written at all"


def test_a_withheld_finding_never_lands_among_the_drifted_ticker_days(lake_root):
    """``reports/schema_drift/`` is compaction's, and D20 will read it as drift.

    Nothing counts that directory today, so the separation is pinned before a reader exists
    rather than after one starts reporting held dividends as retyped columns.
    """
    report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)

    drift = report.schema_drift_dir(lake_root, DAY)
    assert not drift.exists(), "the finding landed in compaction's directory"
    assert len(_files(lake_root)) == 1


# -- 2. two findings in one run, and the subject says which is which ------------------


def test_two_findings_in_one_run_both_land_under_their_own_subjects(lake_root):
    """One run holds several things, and the file name is what a reader picks between.

    The stamp and the pid are identical here, because one run reads one injected clock. So
    the names differ only by what the subject and the sequence carry, and the subject is the
    half that is legible: a reader looking for what happened to QQQ reads the name.
    """
    other = Withheld(
        symbol="QQQ",
        observed_on=DAY,
        event="dividend",
        check="dividend_consistency",
        computed=3.25396,
        against=3.25396,
        instrument_id=43,
    )

    first = report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)
    second = report.write_withheld(lake_root, other, now=AT, sequence=1, pid=11)

    assert len(_files(lake_root)) == 2, "the second finding overwrote or collided with the first"
    assert "SPY" in first.name and "QQQ" not in first.name, first.name
    assert "QQQ" in second.name and "SPY" not in second.name, second.name
    assert {entry["symbol"] for entry in _entries(lake_root)} == {"SPY", "QQQ"}


# -- 3. an exception stops at its class ----------------------------------------------


def test_an_exception_message_never_reaches_the_file(lake_root):
    """The file keeps the class. The message can carry anything the exception chose to say.

    This tree is one the dashboard may read, and ``alert._record`` and the close+5 guard
    redact for the same reason. The exception is the real one the resolution helper raises,
    so a message that grows a new field later is still cut here.
    """
    error = UnresolvedSymbol("SPY", DAY)
    assert "no instrument for" in str(error), "the exception stopped saying what it says"
    unresolved = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="instrument_resolution",
        exception=f"{type(error).__name__}: {error}",
    )

    report.write_withheld(lake_root, unresolved, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    # The place and the class, which is the shape ``_redacted`` was written to leave. The
    # symbol is the place, so the two fields it keeps are the place and the class rather
    # than the class and the message.
    assert entry["exception"] == "SPY: UnresolvedSymbol"
    assert "no instrument for" not in json.dumps(entry), "the exception's message rode in"


def test_a_caller_that_renders_no_class_files_its_message_instead(lake_root):
    """The rendering is the caller's contract, and this is what breaking it costs.

    ``_redacted`` keeps two fields and the writer supplies the first, so the second is
    whatever the caller put first. A caller handing over a bare message files that message's
    opening field, which for an ``OSError`` is a path on the capture machine. The docstring
    says so rather than promising a guarantee the rule cannot make, and this is the case that
    keeps the two from drifting apart.
    """
    bare = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="instrument_resolution",
        exception="no instrument for 'SPY' on 2026-09-14",
    )

    report.write_withheld(lake_root, bare, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["exception"] == "SPY: no instrument for 'SPY' on 2026-09-14"


def test_an_empty_rendering_files_no_exception_field(lake_root):
    """A bare place and a colon is a finding claiming an exception with no class.

    The guard's own fields test ``is not None``, and an empty string is not ``None``. So the
    check here is on the value rather than on its absence, the way ``instrument_ids`` beside
    it already is.
    """
    empty = Withheld(
        symbol="SPY", observed_on=DAY, event="dividend", check="instrument_resolution", exception=""
    )

    report.write_withheld(lake_root, empty, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert "exception" not in entry, "an empty rendering filed a bare place and a colon"


def test_the_fields_beside_the_exception_are_left_whole(lake_root):
    """Redaction owns the exception field and nothing else.

    A finding joined into one string would be cut at its second ``": "`` and arrive without
    the check that refused it, which is one of the three things the file exists to carry. So
    each part is its own field and every part survives.
    """
    report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["event"] == "dividend"
    assert entry["check"] == "dividend_consistency"
    assert entry["computed"] == 7.61406
    assert entry["against"] == 7.61408
    assert entry["instrument_id"] == 42
    # A finding with one instrument carries no plural, and the absence is how a reader
    # tells it from the corrupt-master shape below.
    assert "instrument_ids" not in entry


def test_an_instrument_the_master_could_not_place_files_no_id(lake_root):
    """The id is what failed, so it is carried when there is one and left out when not.

    ``AmbiguousSymbol`` is the other half: a corrupt master hands back several instruments
    for one symbol on one date, and the plural is the finding rather than a detail to fold
    into a singular field.
    """
    ambiguous = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="instrument_resolution",
        instrument_ids=(42, 77),
        exception="AmbiguousSymbol: symbol 'SPY' resolves to [42, 77]",
    )

    report.write_withheld(lake_root, ambiguous, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert "instrument_id" not in entry, "an id was filed for a finding that has none"
    assert entry["instrument_ids"] == [42, 77]
    assert entry["symbol"] == "SPY", "the symbol is what names a finding with no id"
    # A resolution failure computed nothing and compared it to nothing. Filing both as
    # nulls would put two empty fields on every finding of this shape.
    assert "computed" not in entry and "against" not in entry


def test_a_zero_is_filed_rather_than_read_as_nothing(lake_root):
    """The optional fields test ``is not None``, and a zero is the reason why.

    Truthiness reads 0.0 and 0 as absent, and absence is how every case here says a finding
    had no such value. A gate that refused a row because a number came back zero would then
    file a record that does not say so, which is the silence this producer exists to break.
    """
    zeroed = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="dividend_consistency",
        computed=0.0,
        against=0.0,
        instrument_id=0,
    )

    report.write_withheld(lake_root, zeroed, now=AT, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["computed"] == 0.0
    assert entry["against"] == 0.0
    assert entry["instrument_id"] == 0


# -- 4. a write that cannot land raises ----------------------------------------------


def test_a_write_that_cannot_land_raises_rather_than_going_quiet(lake_root):
    """Swallowing a withheld finding is worse than failing loudly.

    The caller contains it, the way compaction's sweep contains the drift writer's raise, so
    one unwritable finding costs that finding rather than the rest of the run. Hiding the
    failure here would take it from every caller, this case included.
    """
    blocked = lake_root / "reports" / "withheld"
    blocked.parent.mkdir(parents=True)
    blocked.write_text("not a directory\n")

    with pytest.raises(OSError):
        report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)


# -- 5. the sequence is what keeps one subject's findings apart ----------------------


def test_two_findings_sharing_a_subject_in_one_run_both_land(lake_root):
    """An injected clock does not advance inside a run, and the pid does not change either.

    ``reports/close_guard/date=2026-09-02/`` in the live lake holds eight files under one
    pid, every one stamped with exactly zero microseconds. So the stamp and the pid are
    constant across a run, and with a name resting on the subject alone the second finding
    under one subject is lost to a ``FileExistsError`` from ``open(path, "x")``.
    """
    second = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="dividend_consistency",
        computed=7.61406,
        against=7.60000,
        instrument_id=42,
    )

    first_path = report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)
    second_path = report.write_withheld(lake_root, second, now=AT, sequence=1, pid=11)

    assert len(_files(lake_root)) == 2, "one subject's second finding was lost"
    # Everything else in the two names is identical, so the sequence is the whole of what
    # separates them. A name without it collides before this case can count the files.
    assert first_path.name.replace("-0000-", "-X-") == second_path.name.replace("-0001-", "-X-")


def test_a_file_is_never_written_over(lake_root):
    """Write-once, the rule every file the lake lands outside the journal follows.

    The stamp, the subject, the sequence and the pid together make this shape unreachable
    from a caller that counts, so what pins it is that the mode guaranteeing it is one
    character. An ``open(..., "w")`` passes every other case here and silently replaces a
    finding, which is the loss this producer exists to prevent.
    """
    report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)
    second = Withheld(
        symbol="SPY",
        observed_on=DAY,
        event="dividend",
        check="dividend_consistency",
        computed=7.61406,
        against=7.60000,
        instrument_id=42,
    )

    with pytest.raises(FileExistsError):
        report.write_withheld(lake_root, second, now=AT, sequence=0, pid=11)

    assert _entries(lake_root)[0]["against"] == 7.61408, "the first finding was replaced"


def test_the_default_pid_is_the_writing_process(lake_root):
    """A caller that passes no pid files under the one production runs as."""
    path = report.write_withheld(lake_root, HELD, now=AT, sequence=0)

    assert path.name.endswith(f"-{os.getpid()}.json"), path.name


# -- 6. a missing lake root is refused rather than created --------------------------


def test_a_missing_lake_root_is_refused_rather_than_created(lake_root):
    """A writer that conjured the root would turn a broken install into a green check.

    The Sunday job decides whether to ping on ``root.is_dir()`` and re-reads it on every
    retry. ``mkdir(parents=True)`` from a missing root would create the lake, so the next
    attempt would find a directory and report a healthy lake holding nothing.
    """
    missing = lake_root / "gone"

    with pytest.raises(FileNotFoundError):
        report.write_withheld(missing, HELD, now=AT, sequence=0, pid=11)

    assert not missing.exists(), "the writer created the lake it was asked to write into"


def test_a_lake_root_that_is_a_file_is_refused(lake_root):
    """``is_dir`` rather than ``exists``, and the two differ on exactly this.

    A regular file where the lake root should be is what a half-finished install leaves.
    ``exists`` calls it present and the failure then arrives out of ``mkdir`` as a different
    exception, past the deliberate refusal.
    """
    not_a_lake = lake_root / "lake.txt"
    not_a_lake.write_text("not a lake\n")

    with pytest.raises(FileNotFoundError):
        report.write_withheld(not_a_lake, HELD, now=AT, sequence=0, pid=11)


# -- 7. market time, in the entry and in the name -----------------------------------


def test_a_utc_instant_is_filed_in_market_time(lake_root):
    """The sweep's clock is UTC, and every other case here hands this one Eastern.

    ``SystemClock.now`` returns ``datetime.now(UTC)``, so the instant production passes is
    four hours ahead of the wall clock the lake is keyed on in September. Dropping the
    conversion is invisible to a suite that only ever passes Eastern, and it moves both
    halves of the record: the name stamps midnight and ``at`` carries ``+00:00``.
    """
    utc_evening = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    path = report.write_withheld(lake_root, HELD, now=utc_evening, sequence=0, pid=11)

    assert path.name.startswith("2000"), "the file name was stamped in the wrong zone"
    (entry,) = _entries(lake_root)
    assert entry["at"] == "2026-09-14T20:00:00-04:00"
    assert datetime.fromisoformat(entry["at"]).utcoffset() is not None, "the offset was dropped"


def test_the_entry_opens_with_the_instant_and_the_day_it_concerns(lake_root):
    """The two fields anything walking ``reports/`` reads without knowing the producer.

    Both shipped writers carry them, so a reader asking when a file was written and which
    day it is about gets one answer from every producer in the tree.
    """
    after_midnight = et(2026, 9, 15, 0, 30)

    path = report.write_withheld(lake_root, HELD, now=after_midnight, sequence=0, pid=11)

    (entry,) = _entries(lake_root)
    assert entry["at"].startswith("2026-09-15T00:30")
    # The field and the directory have to agree. A record filed under the 14th whose own
    # contents say the 15th hands a reader two answers and no way to pick.
    assert entry["day"] == "2026-09-14"
    assert path.parent.name == "date=2026-09-14"
    # Serialised with ``sort_keys=True``, the way both shipped writers serialise, so one
    # entry's bytes do not depend on the order a writer happened to build its dict in.
    assert list(entry) == sorted(entry), "the entry was not written with its keys sorted"


def test_two_ticker_days_are_filed_under_two_directories(lake_root):
    """Every other case here uses one date, so a hardcoded one would satisfy them all."""
    report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)
    next_day = date(2026, 9, 15)
    later = Withheld(
        symbol="SPY",
        observed_on=next_day,
        event="dividend",
        check="dividend_consistency",
        computed=7.61406,
        against=7.61408,
        instrument_id=42,
    )

    report.write_withheld(lake_root, later, now=et(2026, 9, 15, 20, 0), sequence=0, pid=11)

    tree = Path(lake_root) / "reports" / "withheld"
    assert sorted(child.name for child in tree.iterdir()) == [
        "date=2026-09-14",
        "date=2026-09-15",
    ]
    assert _entries(lake_root, next_day)[0]["day"] == "2026-09-15"


# -- 8. a held finding recurs every night, and the repetition is the record ---------


def test_the_same_finding_on_a_second_night_files_again(lake_root):
    """A held finding never reaches its ledger, so every night re-derives it.

    Collapsing the repeats was considered and reversed. A ledger dedups because its
    resolution reads the last entry, so a repeat there adds nothing. This directory has no
    such resolution and every file is one run's verdict. One file says a finding was held at
    some point. Two say it was held again last night, which is what tells a live condition
    from a historical one, and reading the pile is the nightly digest's job.
    """
    first = report.write_withheld(lake_root, HELD, now=AT, sequence=0, pid=11)
    again = report.write_withheld(lake_root, HELD, now=et(2026, 9, 15, 20, 0), sequence=0, pid=12)

    assert first != again, "the second night overwrote the first"
    assert len(_files(lake_root)) == 2, "the second night was collapsed into the first"
    assert {entry["at"][:10] for entry in _entries(lake_root)} == {"2026-09-14", "2026-09-15"}
