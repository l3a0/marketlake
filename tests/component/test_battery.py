"""The validation battery's spine, its writer, and the real-time entitlement check.

Every table here is built locally rather than from ``tests/support/lake``'s sample schemas.
Those carry no ``is_delayed`` and no ``realtime``, and widening them is not free: one test in
``test_load_quotes.py`` proves the overflow projection lifts ``realtime`` out of ``extra``, and
its setup edits only the schema-version ledger, so it depends on the fixture schema's not
carrying the column. Adding it there makes the real column shadow the overflow and the test
reads ``None``. Marketlake #415 carries that fragility.
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest

from lake.battery import (
    CHECK_CALENDAR_COVERAGE,
    CHECK_ENTITLEMENT,
    CHECK_QUOTE_SANITY,
    CHECK_ROW_COUNT_BAND,
    CHECK_SCOPE,
    DELAYED_FEED_EVENT,
    DELAYED_FEED_TITLE,
    INSUFFICIENT_HISTORY,
    MISSING_SESSION,
    OUT_OF_SCOPE,
    PROVENANCE_BATTERY,
    PROVENANCE_HUMAN,
    QUARANTINED_VERDICT,
    QUOTE_SANITY_TOLERANCE,
    SEALED_SURFACES,
    BatteryReport,
    Entitlement,
    Finding,
    PartitionUnreadable,
    SealedPartition,
    append_verdict,
    build_entry,
    capture_spans_by_ticker,
    coverage,
    coverage_line,
    entry_line_count,
    human_precedence,
    in_scope,
    judge,
    judge_entitlement,
    judge_quote_order,
    judge_row_count,
    median,
    page_delayed_feed,
    read_entitlement,
    read_quote_order,
    read_reference,
    render,
    sealed_partitions,
    session_snapshot_counts,
    trailing_medians,
    write_verdict,
)
from lake.capture_spans import CaptureSpan
from lake.config import GuardConstants
from lake.manifest import (
    CLEAN_VERDICT,
    append_quarantine,
    is_quarantined,
    latest_quarantine,
    latest_quarantine_by_check,
    read_quarantine,
    scrub,
    withholding,
)
from tests.support.calendar import weekday_sessions

# The weeks these tests judge in. A regular session opens 09:30 and closes 16:00 Eastern, so
# the option close lands at 16:15 and every row ``_row`` builds falls inside it.
CALENDAR = weekday_sessions(
    date(2026, 8, 17), date(2026, 8, 24), date(2026, 8, 31), date(2026, 9, 14)
)

NOW = datetime(2026, 9, 16, 22, 30, tzinfo=UTC)
DAY = date(2026, 9, 16)
# The partition every test that seals only one is about.
JUDGED = "chains/ticker=SPY/date=2026-09-16.parquet"
SPAN_START = datetime(2026, 9, 1, 13, 30, tzinfo=UTC)

CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("mark", pa.float64()),
        ("is_delayed", pa.bool_()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("close_tag", pa.string()),
        ("suspect", pa.bool_()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)
QUOTES_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("mark", pa.float64()),
        ("realtime", pa.bool_()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("close_tag", pa.string()),
        ("suspect", pa.bool_()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)


# -- builders ----------------------------------------------------------------


def _row(minute: int, *, staleness: float, flag, surface: str, kind: str = "data") -> dict:
    """One row whose staleness is exactly ``staleness`` seconds.

    ``fetch_ts`` minus ``vendor_quote_ts`` is the definition, so the quote stamp is placed
    behind the fetch stamp by that many seconds. A negative value puts it ahead, which is what
    the live lake actually carries.
    """
    snap = datetime(2026, 9, 16, 13, 30, tzinfo=UTC) + timedelta(minutes=minute)
    fetch = snap + timedelta(milliseconds=400)
    quote = fetch - timedelta(seconds=staleness)
    flag_name = "is_delayed" if surface == "chains" else "realtime"
    return {
        "snap_ts": snap.isoformat(),
        "fetch_ts": fetch.isoformat(),
        "vendor_quote_ts": quote.isoformat(),
        "ticker": "SPY",
        "bid": 1.0,
        "ask": 1.05,
        "mark": 1.02,
        flag_name: flag,
        "row_kind": kind,
        "error_class": None,
        "close_tag": None,
        "suspect": False,
        "schema_version": 1,
        "extra": None,
    }


def _table(surface: str, rows: list[dict]) -> pa.Table:
    schema = CHAINS_SCHEMA if surface == "chains" else QUOTES_SCHEMA
    return pa.table({name: [r.get(name) for r in rows] for name in schema.names}, schema=schema)


def _write(
    root: Path,
    surface: str,
    ticker: str,
    day: date,
    rows: list[dict],
    *,
    drop: str | None = None,
) -> Path:
    """Seal one partition. ``drop`` removes a column, which is how drift is expressed."""
    table = _table(surface, rows)
    if drop is not None:
        table = table.drop_columns([drop])
    path = root / surface / f"ticker={ticker}" / f"date={day.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pa_pq.write_table(table, path)
    return path


def _clean_rows(surface: str, *, staleness: float = -1.7, count: int = 5) -> list[dict]:
    wanted = False if surface == "chains" else True
    return [_row(i, staleness=staleness, flag=wanted, surface=surface) for i in range(count)]


def _answer(report: BatteryReport, check: str = CHECK_ENTITLEMENT, partition: str | None = None):
    """The one finding ``check`` returned, for ``partition`` or for the only one judged.

    Four checks run, so a report's totals count findings across all of them and say nothing
    about any one. A test about the entitlement check asks the entitlement check. That also
    survives a fifth check landing, which a bumped total would not.
    """
    found = [
        finding
        for finding in report.findings
        if finding.check == check and (partition is None or finding.partition == partition)
    ]
    assert len(found) == 1, f"{check} returned {len(found)} findings, not one: {found}"
    return found[0]


def _partition(root: Path, surface: str = "chains", ticker: str = "SPY", day: date = DAY):
    return SealedPartition(
        path=root / surface / f"ticker={ticker}" / f"date={day.isoformat()}.parquet",
        surface=surface,
        ticker=ticker,
        day=day,
    )


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    root = tmp_path / "lake"
    root.mkdir()
    (root / "manifest.jsonl").write_text("")
    return root


def _span(start: datetime = SPAN_START, end: datetime | None = None) -> CaptureSpan:
    return CaptureSpan(instrument_id=1, start=start, end=end, options=True)


# -- the verdict record ------------------------------------------------------


def test_build_entry_carries_the_five_fields_the_reader_and_the_sign_off_tool_need():
    entry = build_entry(
        partition="chains/ticker=SPY/date=2026-09-16.parquet",
        verdict=QUARANTINED_VERDICT,
        check=CHECK_ENTITLEMENT,
        observed_at=NOW,
        reason="session-median staleness is 900.0s",
    )
    assert entry["partition"] == "chains/ticker=SPY/date=2026-09-16.parquet"
    assert entry["verdict"] == QUARANTINED_VERDICT
    assert entry["check"] == CHECK_ENTITLEMENT
    assert entry["provenance"] == PROVENANCE_BATTERY
    assert entry["observed_at"].startswith("2026-09-16T18:30")
    assert entry["reason"] == "session-median staleness is 900.0s"


def test_build_entry_refuses_insufficient_history_as_a_verdict_and_says_why():
    """The one confusion the reader's fail-closed rule makes expensive.

    ``manifest.is_quarantined`` withholds a partition on any value that is not ``clean``, so
    an ``insufficient_history`` verdict would refuse every partition a thin-history check
    touched. The lake holds three sessions of data against a floor of five, so that is not a
    hypothetical shape.
    """
    with pytest.raises(ValueError) as caught:
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=INSUFFICIENT_HISTORY,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        )
    assert "finding rather than a verdict" in str(caught.value)


@pytest.mark.parametrize("verdict", ["cleared", "ok", "", OUT_OF_SCOPE])
def test_build_entry_refuses_any_spelling_the_reader_would_not_clear(verdict: str):
    with pytest.raises(ValueError):
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=verdict,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        )


def test_build_entry_requires_the_check_the_precedence_rule_compares():
    with pytest.raises(ValueError) as caught:
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=CLEAN_VERDICT,
            check="",
            observed_at=NOW,
        )
    assert "#139" in str(caught.value)


# -- the writer --------------------------------------------------------------


def test_append_verdict_writes_the_ledger_line_and_its_manifest_entry(lake: Path):
    """Both writes, or the Sunday scrub meets a ledger nothing manifested.

    ``manifest.SCRUB_EXCLUSIONS`` does not hold ``quarantine.jsonl``, so an unmanifested
    ledger is an orphan. ``manifest.append_quarantine`` writes only the line, which is why
    this writer exists beside it.
    """
    entry = build_entry(
        partition="chains/ticker=SPY/date=2026-09-16.parquet",
        verdict=QUARANTINED_VERDICT,
        check=CHECK_ENTITLEMENT,
        observed_at=NOW,
    )
    append_verdict(lake, entry, observed_at=NOW)

    assert read_quarantine(lake) == [entry]
    manifested = {
        json.loads(line)["partition"]
        for line in (lake / "manifest.jsonl").read_text().splitlines()
        if line.strip()
    }
    assert "quarantine.jsonl" in manifested


def test_the_scrub_finds_no_orphan_after_a_verdict_lands(lake: Path):
    """The end the manifest entry is for, asserted through the scrub rather than the file."""
    append_verdict(
        lake,
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=QUARANTINED_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        ),
        observed_at=NOW,
    )
    result = scrub(lake)
    assert result.orphans == ()
    assert result.sha_mismatches == ()
    assert result.missing == ()


def test_the_manifest_row_count_follows_the_file_and_never_shrinks(lake: Path):
    """Counted off lines, not parsed entries, so a damaged ledger cannot stop the writer.

    A line the read cannot parse ends the read, so a parsed count can fall below the
    manifested one and the manifest's row-count guard would then raise on every later append.
    """
    for index in range(3):
        append_verdict(
            lake,
            build_entry(
                partition=f"chains/ticker=SPY/date=2026-09-1{index}.parquet",
                verdict=QUARANTINED_VERDICT,
                check=CHECK_ENTITLEMENT,
                observed_at=NOW,
            ),
            observed_at=NOW,
        )
    assert entry_line_count(lake) == 3

    with open(lake / "quarantine.jsonl", "a", encoding="utf-8") as handle:
        handle.write('{"partition": "torn", "verd\n')
    assert entry_line_count(lake) == 4
    assert len(read_quarantine(lake)) == 3

    append_verdict(
        lake,
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-14.parquet",
            verdict=QUARANTINED_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        ),
        observed_at=NOW,
    )
    assert entry_line_count(lake) == 5


# -- human precedence --------------------------------------------------------


def test_a_human_sign_off_stands_against_the_same_check_re_observing():
    """Marketlake #139's rule, built here because #139 shipped after this.

    A sealed partition is immutable, so the same check finds the same thing every night.
    Without this a sign-off lasts until 18:30.
    """
    signed_off = {
        "partition": "chains/ticker=SPY/date=2026-09-16.parquet",
        "verdict": CLEAN_VERDICT,
        "check": CHECK_ENTITLEMENT,
        "provenance": PROVENANCE_HUMAN,
    }
    assert human_precedence(signed_off, CHECK_ENTITLEMENT) is True


def test_a_human_sign_off_does_not_stand_against_a_different_check():
    """A different check's failure is new information the human never spoke to."""
    signed_off = {
        "partition": "chains/ticker=SPY/date=2026-09-16.parquet",
        "verdict": CLEAN_VERDICT,
        "check": CHECK_ENTITLEMENT,
        "provenance": PROVENANCE_HUMAN,
    }
    assert human_precedence(signed_off, "quote_sanity") is False


def test_the_batterys_own_earlier_verdict_never_claims_precedence():
    """Only a human row defers. A battery row deferring to itself would freeze the ledger."""
    earlier = {
        "partition": "chains/ticker=SPY/date=2026-09-16.parquet",
        "verdict": QUARANTINED_VERDICT,
        "check": CHECK_ENTITLEMENT,
        "provenance": PROVENANCE_BATTERY,
    }
    assert human_precedence(earlier, CHECK_ENTITLEMENT) is False
    assert human_precedence(None, CHECK_ENTITLEMENT) is False


def test_the_run_leaves_a_human_sign_off_standing_and_says_so(lake: Path):
    """End to end: the check fails, a human cleared it, and the ledger is not touched."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    signed_off = build_entry(
        partition="chains/ticker=SPY/date=2026-09-16.parquet",
        verdict=CLEAN_VERDICT,
        check=CHECK_ENTITLEMENT,
        observed_at=NOW,
        provenance=PROVENANCE_HUMAN,
    )
    append_verdict(lake, signed_off, observed_at=NOW)
    before = read_quarantine(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.deferred == 1
    assert report.appended == ()
    assert read_quarantine(lake) == before
    assert any("human precedence stands" in line for line in report.report)


# -- the ledger read and the lock --------------------------------------------


def _quarantined_then_fixed(root: Path, *, staleness: float = -1.7) -> None:
    """A lake whose partition one run quarantined and whose feed is fixed by the next.

    That is the state both races below need. The entitlement check fails on night one, so the
    ledger carries ``quarantined``/battery, and re-sealing the partition with the live lake's
    own skew is what makes the next night's check pass and therefore transition.
    """
    _write(root, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(root)
    judge(root, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    _write(root, "chains", "SPY", DAY, _clean_rows("chains", staleness=staleness))


def _human(root: Path, verdict: str) -> dict:
    """The entry ``lake.signoff`` writes, landed through the writer a caller inside the lock
    uses. Built here rather than by calling ``signoff``, because these seams run inside the
    hold and that tool takes the lock itself."""
    return write_verdict(
        root,
        build_entry(
            partition=JUDGED,
            verdict=verdict,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
            provenance=PROVENANCE_HUMAN,
            reason="vendor confirmed the delay was theirs",
        ),
        observed_at=NOW,
    )


@contextmanager
def _signing_off(verdict: str, *, on: str = "acquire"):
    """A ``lake_lock`` that lands a human entry as the hold is taken, or as it is released.

    The pattern is ``test_occ_mapping``'s ``racing_lock``. ``on="acquire"`` puts the write in
    the window a read taken before the walk cannot see, so a read left outside the hold uses
    the stale snapshot. ``on="release"`` puts it in the window between one hold and the next,
    which is the shape a writer blocked on the lock actually lands in: it waits, and the
    kernel hands it the lock the instant the holder lets go.

    The pair is what separates a read under *a* lock from a read under *the* lock the append
    happens in. Splitting the two into one hold for the read and another for the append passes
    every ``on="acquire"`` assertion, because the human entry still lands before the read.
    Found by the mutation lens on marketlake #479.

    ``judge`` imports the lock inside the function, so patching the module attribute is what
    the call resolves against. The entry goes in through ``write_verdict`` rather than through
    ``signoff``, because this code already holds the lock and that tool takes it.
    """
    from lake.lock import lake_lock as real_lock

    landed: list[dict] = []
    holds: list[int] = []

    @contextmanager
    def racing_lock(lake_root):
        holds.append(1)
        with real_lock(lake_root) as held:
            if on == "acquire" and not landed:
                landed.append(_human(Path(lake_root), verdict))
            try:
                yield held
            finally:
                if on == "release" and not landed:
                    landed.append(_human(Path(lake_root), verdict))

    yield racing_lock, landed, holds


def test_the_ledger_is_read_inside_the_lock_the_verdict_is_appended_under(lake: Path, monkeypatch):
    """Marketlake #470. The walk's own snapshot is as old as the walk.

    Read once before the walk, a sign-off landing during it is invisible to
    ``human_precedence``, and the battery appends its own ``clean`` after the human's. Nothing
    looks wrong that night, because both say the partition reads. The cost lands on the first
    later night the check fails: the entry the rule compares carries ``provenance: battery``,
    so the run re-quarantines what a person cleared on purpose.
    """
    _quarantined_then_fixed(lake)

    with _signing_off(CLEAN_VERDICT) as (racing_lock, landed, _holds):
        monkeypatch.setattr("lake.lock.lake_lock", racing_lock)
        report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert landed, "the seam never fired, so this test proves nothing"
    assert report.deferred == 1
    assert report.appended == ()
    assert read_quarantine(lake)[-1]["provenance"] == PROVENANCE_HUMAN
    assert latest_quarantine(lake)[JUDGED]["provenance"] == PROVENANCE_HUMAN
    assert any("human precedence stands" in line for line in report.report)


def test_the_append_happens_in_the_same_hold_as_the_read_and_not_a_second_one(
    lake: Path, monkeypatch
):
    """Reading under *a* lock is not reading under *the* lock the append happens in.

    Splitting the two, one hold to read and the public locking writer for the append, is the
    plausible refactor: it keeps the per-partition read, keeps it locked, and removes the
    re-entrancy hazard, so it looks safer. It reinstates marketlake #470 at a narrower window.
    A sign-off blocked on the lock lands the instant the reader lets go, which is before the
    append rather than after it, and the battery's own line buries it exactly as before.

    Every assertion that lands its write on acquisition passes under that refactor, because
    the human entry still precedes the read. This one lands on release, and counts the holds,
    which is the other way to say the same thing.
    """
    _quarantined_then_fixed(lake)

    with _signing_off(CLEAN_VERDICT, on="release") as (racing_lock, landed, holds):
        monkeypatch.setattr("lake.lock.lake_lock", racing_lock)
        judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert landed, "the seam never fired, so this test proves nothing"
    assert latest_quarantine(lake)[JUDGED]["provenance"] == PROVENANCE_HUMAN, (
        "the battery's own line landed after the sign-off, so the next run that fails this "
        "check will re-quarantine what a person cleared"
    )
    assert read_quarantine(lake)[-1]["provenance"] == PROVENANCE_HUMAN
    # One hold for the partition, covering its read and its appends together. Two holds is
    # the refactor above, whatever order they are written in.
    assert holds == [1], f"judge took {len(holds)} holds for one partition, not one"


def test_a_sign_off_landing_mid_walk_leaves_what_one_landing_before_it_leaves(
    tmp_path: Path, monkeypatch
):
    """The invariant the fix is for, stated as a pair rather than as a property of one run.

    Two lakes built the same way and one sign-off, differing only in whether it lands before
    the walk or inside it. The ledgers and the counts have to match, on the night it lands and
    on the night after, when the feed is delayed again and the precedence rule is what decides
    whether the partition is re-quarantined.
    """

    def run(root: Path, *, mid_walk: bool) -> tuple[list[dict], list[tuple]]:
        root.mkdir()
        (root / "manifest.jsonl").write_text("")
        _quarantined_then_fixed(root)
        counts: list[tuple] = []
        if mid_walk:
            with _signing_off(CLEAN_VERDICT) as (racing_lock, landed, _holds):
                monkeypatch.setattr("lake.lock.lake_lock", racing_lock)
                night_two = judge(root, calendar=CALENDAR, now=NOW, guards=GuardConstants())
                assert landed, "the seam never fired, so this half proves nothing"
            monkeypatch.undo()
        else:
            _human(root, CLEAN_VERDICT)
            night_two = judge(root, calendar=CALENDAR, now=NOW, guards=GuardConstants())
        counts.append((night_two.deferred, night_two.released, night_two.appended))

        # The feed is delayed again, which is when a lost sign-off costs something.
        _write(root, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
        night_three = judge(root, calendar=CALENDAR, now=NOW, guards=GuardConstants())
        counts.append((night_three.deferred, night_three.released, night_three.appended))
        return read_quarantine(root), counts

    raced, raced_counts = run(tmp_path / "raced", mid_walk=True)
    calm, calm_counts = run(tmp_path / "calm", mid_walk=False)

    assert raced == calm
    assert raced_counts == calm_counts
    assert [entry["provenance"] for entry in raced] == [PROVENANCE_BATTERY, PROVENANCE_HUMAN]
    assert is_quarantined(latest_quarantine(tmp_path / "raced")[JUDGED]) is False


def test_a_revoke_landing_mid_walk_is_not_superseded_by_the_runs_own_verdict(
    lake: Path, monkeypatch
):
    """The other direction, which the ledger reaches by a different route.

    A human revoking a sign-off writes ``quarantined``/human. Where the battery's stale entry
    is its own ``clean`` and the check now fails, the transition test passes against that stale
    entry and the run appends ``quarantined``/battery after the human's line. Both withhold the
    partition, so again nothing looks wrong, and again the provenance the precedence rule reads
    is the battery's. Without the second direction a mistaken sign-off is permanent, which is
    why ``lake.signoff`` has it at all.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    append_verdict(
        lake,
        build_entry(
            partition=JUDGED, verdict=CLEAN_VERDICT, check=CHECK_ENTITLEMENT, observed_at=NOW
        ),
        observed_at=NOW,
    )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))

    with _signing_off(QUARANTINED_VERDICT) as (racing_lock, landed, _holds):
        monkeypatch.setattr("lake.lock.lake_lock", racing_lock)
        report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert landed, "the seam never fired, so this test proves nothing"
    assert report.deferred == 1
    assert report.appended == ()
    assert latest_quarantine(lake)[JUDGED]["provenance"] == PROVENANCE_HUMAN
    assert is_quarantined(latest_quarantine(lake)[JUDGED]) is True


def test_a_second_run_overlapping_the_first_appends_one_line_and_not_two(lake: Path, monkeypatch):
    """``python -m lake.battery`` is a hand run and the 18:30 sweep runs the same walk.

    Nothing schedules the two apart, so a hand run started while the nightly one is walking is
    an ordinary shape. With each run's ledger read taken before its walk, both see no entry,
    both call it a transition, and the identical line lands twice. That breaks the
    append-on-transition rule: a check whose current entry already says what this finding says
    is the same news a second time.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    from lake import battery as module

    original = module._judge_partition
    overlapped: list[BatteryReport] = []

    def hooked(*args, **kwargs):
        if not overlapped:
            # The second run is an ordinary walk, started inside the first one's.
            monkeypatch.setattr(module, "_judge_partition", original)
            overlapped.append(judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()))
            monkeypatch.setattr(module, "_judge_partition", hooked)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "_judge_partition", hooked)
    first = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert overlapped, "the seam never fired, so this test proves nothing"
    assert len(overlapped[0].appended) == 1
    assert first.appended == ()
    assert len(read_quarantine(lake)) == 1


def test_append_verdict_still_writes_to_a_lake_root_that_does_not_exist_yet(tmp_path: Path):
    """The directory has to be made before the lock, because the lock cannot make it.

    ``lake_lock`` opens ``manifest.jsonl`` with ``O_CREAT``, which creates the file and never
    the directory holding it. Moving the ``mkdir`` inside the hold, where the writes are, turns
    the acquire into a ``FileNotFoundError`` on a root nothing has created. Caught by the
    correctness review on marketlake #479 rather than by any existing test, because every
    other caller reaches this function with the root already on disk.
    """
    root = tmp_path / "absent"

    append_verdict(
        root,
        build_entry(
            partition=JUDGED,
            verdict=QUARANTINED_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        ),
        observed_at=NOW,
    )

    assert [entry["check"] for entry in read_quarantine(root)] == [CHECK_ENTITLEMENT]
    assert (root / "manifest.jsonl").exists()


def test_a_dry_run_creates_no_manifest_in_a_lake_that_has_none(tmp_path: Path):
    """A preview writes nothing at all, and taking the lock would break that.

    ``lake_lock`` creates ``manifest.jsonl`` on acquire, so a dry run holding it writes a file
    into a lake root that had none. ``test_a_dry_run_writes_no_line_and_sends_no_page`` cannot
    see this, because the ``lake`` fixture creates the manifest itself, which is why this one
    builds its root by hand.
    """
    root = tmp_path / "lake"
    root.mkdir()
    _write(root, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(root)
    assert not (root / "manifest.jsonl").exists()

    report = judge(root, calendar=CALENDAR, now=NOW, guards=GuardConstants(), dry_run=True)

    assert report.quarantined == 1, "the walk has to reach the check for this to prove anything"
    assert not (root / "manifest.jsonl").exists()
    assert not (root / "quarantine.jsonl").exists()


def test_append_verdict_takes_the_lock_and_write_verdict_leaves_it_to_its_caller(lake: Path):
    """The two levels the split created, asserted against the real lock rather than by reading.

    ``lake_lock`` is a blocking exclusive ``flock`` with no reentrancy, so the one that takes
    it blocks while this test holds it and the one that does not lands both writes straight
    away. The pattern is ``test_actions``'s. Getting this backwards is what would deadlock
    ``judge``, which calls the second from inside its own hold.
    """
    from lake.lock import lake_lock

    entry = build_entry(
        partition=JUDGED, verdict=QUARANTINED_VERDICT, check=CHECK_ENTITLEMENT, observed_at=NOW
    )
    failures: list[BaseException] = []
    done = threading.Event()

    def run() -> None:
        try:
            append_verdict(lake, entry, observed_at=NOW)
        except BaseException as exc:  # noqa: BLE001 - reported to the main thread below
            failures.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=run)
    with lake_lock(lake):
        worker.start()
        # Long enough for the worker to reach the lock and block on it.
        time.sleep(0.3)
        assert read_quarantine(lake) == []
        assert not done.is_set()
        # The unlocked half writes the pair without asking for a lock this thread holds.
        write_verdict(
            lake,
            build_entry(
                partition=JUDGED,
                verdict=QUARANTINED_VERDICT,
                check=CHECK_QUOTE_SANITY,
                observed_at=NOW,
            ),
            observed_at=NOW,
        )
        assert [one["check"] for one in read_quarantine(lake)] == [CHECK_QUOTE_SANITY]

    assert done.wait(10)
    worker.join(10)
    assert failures == []
    assert [one["check"] for one in read_quarantine(lake)] == [
        CHECK_QUOTE_SANITY,
        CHECK_ENTITLEMENT,
    ]


# -- append on transition ----------------------------------------------------


def test_a_clean_verdict_for_an_unjudged_partition_writes_nothing(lake: Path):
    """A partition with no entry already reads, so the line would change nothing."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == CLEAN_VERDICT
    assert report.appended == ()
    assert not (lake / "quarantine.jsonl").exists()


def test_the_second_run_against_an_unchanged_lake_appends_nothing(lake: Path):
    """Idempotence. A nightly run over sealed data would otherwise grow the ledger forever."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    first = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    assert len(first.appended) == 1
    after_first = read_quarantine(lake)

    second = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=1), guards=GuardConstants())

    assert second.quarantined == 1
    assert second.appended == ()
    assert read_quarantine(lake) == after_first


def test_a_partition_that_recovers_gets_a_superseding_clean_line(lake: Path):
    """The other direction of the same transition rule, which is what un-quarantines."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    report = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=1), guards=GuardConstants())

    assert len(report.appended) == 1
    entries = read_quarantine(lake)
    assert len(entries) == 2
    assert entries[-1]["verdict"] == CLEAN_VERDICT
    assert latest_quarantine(lake)["chains/ticker=SPY/date=2026-09-16.parquet"]["verdict"] == (
        CLEAN_VERDICT
    )


def _seed_spans(root: Path, *, start: datetime = SPAN_START, end: datetime | None = None) -> None:
    """A security master and a spans file naming SPY, so the partition is in scope."""
    from lake.capture_spans import CaptureSpans
    from lake.security_master import SecurityMaster

    master = SecurityMaster()
    instrument_id = master.register(
        kind="equity", capture_start=start, valid_from=start.date(), ticker="SPY"
    )
    (root / "reference").mkdir(parents=True, exist_ok=True)
    pa_pq.write_table(master.to_table(), root / "reference" / "security_master.parquet")
    spans = CaptureSpans(
        [CaptureSpan(instrument_id=instrument_id, start=start, end=end, options=True)]
    )
    pa_pq.write_table(spans.to_table(), root / "reference" / "capture_spans.parquet")


# -- scope -------------------------------------------------------------------


def test_a_day_before_every_capture_span_is_out_of_scope(lake: Path):
    """The live case: SPY's 2026-09-02 partition, six days before either span opens.

    Its two rows carry null bid and ask and a staleness of 16 days, so every check measured
    against it fails. Judging it would quarantine the lake's oldest partition on the first run
    and page about a day-one probe.
    """
    early = date(2026, 8, 20)
    _write(lake, "chains", "SPY", early, _clean_rows("chains", staleness=1_380_301.0))
    _seed_spans(lake, start=SPAN_START)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0
    assert report.judged == 0
    assert report.appended == ()


def test_a_day_inside_a_span_is_judged_even_when_the_span_opens_mid_session(lake: Path):
    """A day overlapping a span at all is in scope, including the onboarding day itself.

    The rows the partition holds are the ones capture wrote, so judging them is right. A rule
    keyed on the span containing the whole day would skip every onboarding day forever.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake, start=datetime(2026, 9, 16, 17, 0, tzinfo=UTC))

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).judged
    assert report.out_of_scope == 0


def test_a_partition_of_gap_rows_alone_is_out_of_scope_not_a_failure(lake: Path):
    """Eight of the live lake's 29 sealed partitions are exactly this shape.

    A gap row is the design's record that a minute was missed. Reading a day of them as a
    truncated fetch would quarantine four correctly-recorded outage sessions.
    """
    rows = [_row(i, staleness=0.0, flag=None, surface="chains", kind="gap") for i in range(5)]
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0
    assert report.appended == ()


def test_a_ticker_the_master_does_not_know_is_scope_unknown_rather_than_out_of_scope(
    lake: Path,
):
    """The two answers are opposites and must never be spelled the same.

    Out of scope is a fact: capture was not running, so nothing in the partition is evidence.
    This is the absence of that fact. Reporting it as out of scope would let a delayed feed
    pass under a reason that says something untrue, with no page and a zero exit code.
    """
    _write(lake, "chains", "QQQ", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.scope_unknown == 1
    assert report.out_of_scope == 0
    assert report.quarantined == 0
    assert report.appended == ()
    assert any("knows no instrument spelled 'QQQ'" in line for line in report.report)


@pytest.mark.parametrize("missing", ["security_master", "capture_spans"])
def test_a_reference_file_that_cannot_be_read_judges_nothing_and_says_so(lake: Path, missing: str):
    """A delayed feed must not pass because the clamp could not be read."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    (lake / "reference" / f"{missing}.parquet").unlink()

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.scope_unknown == 1
    assert report.quarantined == 0
    assert report.appended == ()
    assert any(missing.replace("_", " ") in line or missing in line for line in report.report)


def test_a_scope_that_could_not_be_read_exits_non_zero(lake: Path, monkeypatch, capsys):
    """A run that judged nothing because the clamp was unreadable must not read as a clean
    night. Exit 0 is what an operator and a wrapper script both take for one."""
    from lake.battery import BatteryReport, main

    monkeypatch.setattr(
        "lake.battery.judge_from_config",
        lambda **kwargs: BatteryReport(scope_unknown=4, report=("battery: no security master",)),
    )
    assert main([]) == 1
    assert "scope unknown:        4" in capsys.readouterr().out


def test_a_closed_span_puts_a_later_day_out_of_scope(lake: Path):
    """A retired ticker's days after its span closes are not the feed's fault."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake, start=SPAN_START, end=datetime(2026, 9, 10, 20, 0, tzinfo=UTC))

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0


def test_in_scope_answers_false_for_an_empty_span_tuple():
    assert in_scope(_partition(Path("/nowhere")), ()) is False


def test_a_renamed_tickers_old_partitions_are_still_judged(lake: Path):
    """The failure a point-in-time lookup produces, in the direction that fails open.

    ``SecurityMaster.remap`` closes the old mapping, so resolving the directory name as of the
    run date returns nothing after a rename and every partition still sitting under the old
    ``ticker=`` directory goes unjudged. Silently, permanently, on real captured data.
    """
    from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    master = SecurityMaster.read(master_path(lake))
    master.remap(1, ID_TYPE_TICKER, "SPYZ", date(2026, 9, 16))
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == QUARANTINED_VERDICT
    assert report.scope_unknown == 0


def test_a_day_before_the_masters_valid_from_still_resolves_its_clamp(lake: Path):
    """The other direction, which was marketlake #405's failure on the dashboard.

    Resolving as of the judged day loses the clamp on any day before ``valid_from``. The
    partition then has no span, and the out-of-scope rule that should protect it never runs.
    The dashboard reached the same answer by its own route, so this is now the shared rule
    rather than the one check that keeps it.
    """
    early = date(2026, 8, 20)
    _write(lake, "chains", "SPY", early, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake, start=SPAN_START)

    spans = capture_spans_by_ticker(lake, ["SPY"])
    assert "SPY" in spans

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    assert report.out_of_scope == 1
    assert report.scope_unknown == 0


def test_one_spelling_two_instruments_refuses_rather_than_guessing(lake: Path):
    """A directory name two instruments have both carried genuinely does not say which."""
    from lake.security_master import SecurityMaster, master_path

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    master = SecurityMaster.read(master_path(lake))
    master.register(
        kind="equity", capture_start=SPAN_START, valid_from=date(2026, 9, 10), ticker="SPY"
    )
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.scope_unknown == 1
    assert any("names 2 instruments" in line for line in report.report)


# -- the entitlement check ---------------------------------------------------


def test_a_negative_median_staleness_passes_because_the_live_feed_carries_one(lake: Path):
    """The lake's chain partitions run -0.7 to -2.1 seconds. Clock skew, not a delayed feed."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == CLEAN_VERDICT
    assert report.quarantined == 0


def test_a_large_negative_median_quarantines_because_the_comparison_is_on_magnitude(lake: Path):
    """The mutation this check exists to survive.

    ``staleness_page_seconds`` is 60 and the real median is negative, so a check written as
    ``median > limit`` passes every one of these and never fires against this feed. A
    15-minute delay arriving under the same skew reads as -900 rather than +900.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-900.0))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    finding = _answer(report)
    assert "magnitude" in finding.reason
    assert finding.computed == pytest.approx(-900.0)
    assert finding.against == 60.0


def test_a_large_positive_median_quarantines_too(lake: Path):
    """The design's own signature, a median near 15 minutes, in the sign it was described in."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1


@pytest.mark.parametrize("staleness", [59.9, -59.9])
def test_a_median_inside_the_limit_passes_in_either_sign(lake: Path, staleness: float):
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=staleness))
    _seed_spans(lake)
    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    assert _answer(report).verdict == CLEAN_VERDICT


@pytest.mark.parametrize("staleness", [60.1, -60.1])
def test_a_median_outside_the_limit_quarantines_in_either_sign(lake: Path, staleness: float):
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=staleness))
    _seed_spans(lake)
    assert judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()).quarantined == 1


def test_the_limit_comes_from_config_rather_than_from_a_constant_here(lake: Path):
    """A recalibrated threshold takes effect on the next run, not at the next release."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-90.0))
    _seed_spans(lake)

    strict = judge(
        lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(staleness_page_seconds=60)
    )
    loose = judge(
        lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(staleness_page_seconds=120)
    )

    assert _answer(strict).verdict == QUARANTINED_VERDICT
    assert _answer(loose).verdict == CLEAN_VERDICT


def test_one_row_with_the_wrong_flag_quarantines_the_partition(lake: Path):
    """The design says the flags must show real-time on every snapshot, so one row is enough.

    It can be that strict because it is the vendor's own statement rather than a measurement.
    ``is_delayed`` is false on all 29,718,244 chain data rows the lake holds, with no nulls.
    """
    rows = _clean_rows("chains")
    rows[2]["is_delayed"] = True
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    finding = next(f for f in report.findings if f.verdict == QUARANTINED_VERDICT)
    assert "is_delayed=False" in finding.reason
    assert finding.computed == 1.0


def test_a_null_flag_counts_as_a_violation_rather_than_being_skipped(lake: Path):
    """Null is the vendor declining to say, and a pass on that is not failing closed."""
    rows = _clean_rows("chains")
    rows[1]["is_delayed"] = None
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    assert judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()).quarantined == 1


def test_quotes_are_checked_against_realtime_true_not_is_delayed_false(lake: Path):
    """The two surfaces carry different flags with opposite polarity."""
    rows = _clean_rows("quotes")
    rows[0]["realtime"] = False
    _write(lake, "quotes", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert "realtime=True" in report.findings[0].reason


def test_a_clean_quotes_partition_passes(lake: Path):
    _write(lake, "quotes", "SPY", DAY, _clean_rows("quotes"))
    _seed_spans(lake)
    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    assert _answer(report).verdict == CLEAN_VERDICT


def test_a_missing_flag_column_quarantines_rather_than_passing(lake: Path):
    """Drift in a column the pinned schema carries. Answering it with a pass is fail open."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"), drop="is_delayed")
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert "carries no is_delayed column" in report.findings[0].reason


def test_rows_with_no_vendor_stamp_leave_the_median_undefined_and_quarantine(lake: Path):
    """Not a pass. A partition whose staleness cannot be measured is one nobody has checked."""
    rows = _clean_rows("chains")
    for row in rows:
        row["vendor_quote_ts"] = None
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert "staleness cannot be measured" in report.findings[0].reason


def test_some_rows_missing_a_stamp_leave_the_median_to_the_rows_that_have_one(lake: Path):
    """A dropped row must not be counted as zero, which would drag the median toward a pass."""
    rows = _clean_rows("chains", staleness=900.0, count=5)
    rows[0]["vendor_quote_ts"] = None
    rows[1]["fetch_ts"] = None
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert report.findings[0].computed == pytest.approx(900.0)


def test_a_stamp_that_will_not_parse_reports_the_partition_unreadable(lake: Path):
    """Rather than judging it on the rows that happened to survive."""
    rows = _clean_rows("chains")
    rows[3]["vendor_quote_ts"] = "2026-09-16T13:30:00.931000"  # no zone offset
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.unreadable == 1
    assert report.judged == 0
    assert report.appended == ()
    assert any("zone-aware timestamp" in line for line in report.report)


def test_the_median_is_exact_rather_than_approximate(lake: Path):
    """An even row count, so the exact median falls between two values.

    ``pc.approximate_median`` is a t-digest and would answer one of the neighbours. The
    midpoint is what ``statistics.median`` gives, and a guard whose answer depends on where
    its estimator's buckets fell is a guard nobody can reproduce from the rows.
    """
    rows = [
        _row(0, staleness=10.0, flag=False, surface="chains"),
        _row(1, staleness=20.0, flag=False, surface="chains"),
    ]
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.findings[0].computed == pytest.approx(15.0)


# -- the delayed-feed page ---------------------------------------------------


def _publisher(lake: Path, *, secrets=()):
    from lake.alert import Publisher
    from tests.support.transport import FakeTransport

    transport = FakeTransport()
    return Publisher(lake_root=lake, transport=transport, secrets=secrets), transport


def test_one_page_for_the_run_names_every_partition_it_quarantined(lake: Path):
    """One fact is one page. A vendor entitlement change reaches every partition at once.

    ``alert.DEFAULT_DAILY_CAP`` is forty a day, so paging per partition would spend the cap on
    a roster of twenty and swallow whatever came after, which could be the auth-death page.
    """
    for ticker in ("SPY", "QQQ"):
        _write(lake, "chains", ticker, DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    _seed_spans_for(lake, "QQQ", instrument_id=2)
    publisher, transport = _publisher(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert report.quarantined == 2
    assert len(transport.messages) == 1
    message = transport.messages[0]
    assert message.event == DELAYED_FEED_EVENT
    assert message.title == DELAYED_FEED_TITLE
    assert message.priority == 5
    assert "2 partitions quarantined" in message.body
    assert "session-median staleness 900.0s" in message.body
    assert "chains/ticker=QQQ/date=2026-09-16.parquet" in message.body
    assert "chains/ticker=SPY/date=2026-09-16.parquet" in message.body


def test_the_page_fires_once_on_the_transition_and_not_again(lake: Path):
    """The ledger's own entry is what says the operator was already told.

    The transition is expressed in the record rather than in a counter this module keeps, so
    it survives a restart the way a counter would not.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)
    assert len(transport.messages) == 1

    judge(
        lake,
        calendar=CALENDAR,
        now=NOW + timedelta(days=1),
        guards=GuardConstants(),
        publisher=publisher,
    )
    assert len(transport.messages) == 1


def test_a_feed_that_recovers_and_fails_again_pages_twice(lake: Path):
    """The re-arm half of once-on-the-transition, which a plain 'already paged' flag loses."""
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    judge(
        lake,
        calendar=CALENDAR,
        now=NOW + timedelta(days=1),
        guards=GuardConstants(),
        publisher=publisher,
    )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    judge(
        lake,
        calendar=CALENDAR,
        now=NOW + timedelta(days=2),
        guards=GuardConstants(),
        publisher=publisher,
    )

    assert len(transport.messages) == 2


def test_a_clean_run_pages_nothing(lake: Path):
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert transport.messages == []


def test_a_dry_run_writes_no_line_and_sends_no_page(lake: Path):
    """The counts an operator reads before deciding are the counts the real run will produce."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    dry = judge(
        lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher, dry_run=True
    )

    assert dry.quarantined == 1
    assert dry.appended == ()
    assert transport.messages == []
    assert not (lake / "quarantine.jsonl").exists()
    assert any("would write" in line for line in dry.report)

    wet = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)
    assert wet.quarantined == dry.quarantined
    assert len(wet.appended) == 1


def test_the_page_body_caps_the_partitions_it_names_and_counts_the_rest(lake: Path):
    findings = [
        Finding(
            partition=f"chains/ticker=T{i}/date=2026-09-16.parquet",
            surface="chains",
            ticker=f"T{i}",
            day=DAY,
            check=CHECK_ENTITLEMENT,
            verdict=QUARANTINED_VERDICT,
            reason="delayed",
            computed=900.0,
            against=60.0,
        )
        for i in range(9)
    ]
    publisher, transport = _publisher(lake)

    page_delayed_feed(publisher, findings, now=NOW)

    body = transport.messages[0].body
    assert "9 partitions quarantined" in body
    assert "and 3 more" in body
    assert "chains/ticker=T5/date=2026-09-16.parquet" in body
    assert "chains/ticker=T6/date=2026-09-16.parquet" not in body


def test_the_page_body_shows_a_range_when_the_partitions_disagree(lake: Path):
    """One number would hide a feed that went delayed on one ticker and not another."""
    findings = [
        replace(
            Finding(
                partition="chains/ticker=SPY/date=2026-09-16.parquet",
                surface="chains",
                ticker="SPY",
                day=DAY,
                check=CHECK_ENTITLEMENT,
                verdict=QUARANTINED_VERDICT,
                reason="delayed",
                computed=computed,
                against=60.0,
            ),
            ticker=ticker,
            partition=f"chains/ticker={ticker}/date=2026-09-16.parquet",
        )
        for ticker, computed in (("SPY", 900.0), ("QQQ", 61.0))
    ]
    publisher, transport = _publisher(lake)

    page_delayed_feed(publisher, findings, now=NOW)

    assert "session-median staleness 61.0s to 900.0s" in transport.messages[0].body


def _seed_spans_for(root: Path, ticker: str, *, instrument_id: int) -> None:
    """Add a second ticker to the master and spans already written by :func:`_seed_spans`."""
    from lake.capture_spans import CaptureSpans
    from lake.security_master import SecurityMaster, master_path

    master = SecurityMaster.read(master_path(root))
    master.register(
        kind="equity", capture_start=SPAN_START, valid_from=SPAN_START.date(), ticker=ticker
    )
    pa_pq.write_table(master.to_table(), root / "reference" / "security_master.parquet")
    spans = CaptureSpans.read(root / "reference" / "capture_spans.parquet")
    both = CaptureSpans(
        [
            *spans.spans,
            CaptureSpan(instrument_id=instrument_id, start=SPAN_START, end=None, options=True),
        ]
    )
    pa_pq.write_table(both.to_table(), root / "reference" / "capture_spans.parquet")


# -- the reader and the writer meet ------------------------------------------


def test_the_loader_refuses_a_partition_this_run_quarantined(lake: Path):
    """The whole point. The exclusion #135 built has had nothing to exclude until now.

    Asserted through ``load_chain`` rather than through ``is_quarantined``, because what the
    deliverable claims is that the reader refuses, not that a helper returns True.
    """
    from lake.loader import PartitionQuarantined, load_chain

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    with pytest.raises(PartitionQuarantined) as caught:
        load_chain("SPY", DAY, lake_root=lake)
    assert caught.value.entry["check"] == CHECK_ENTITLEMENT
    assert caught.value.entry["verdict"] == QUARANTINED_VERDICT
    assert "quarantined by 1 check:" in str(caught.value), "the plural is conditional"


def test_the_refusal_names_every_check_withholding_the_partition(lake: Path):
    """Signing one off leaves the other standing, so a refusal naming one misleads.

    The second check is seeded through ``append_verdict`` rather than produced by a run,
    because one check exists. What is asserted is the loader's own reading of the ledger.
    """
    from lake.loader import PartitionQuarantined, load_chain

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    append_verdict(
        lake,
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=QUARANTINED_VERDICT,
            check="row_count_band",
            observed_at=NOW,
        ),
        observed_at=NOW,
    )

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    with pytest.raises(PartitionQuarantined) as caught:
        load_chain("SPY", DAY, lake_root=lake)

    held = [entry["check"] for entry in caught.value.entries]
    assert held == ["row_count_band", CHECK_ENTITLEMENT]
    assert "quarantined by 2 checks" in str(caught.value)
    assert "row_count_band" in str(caught.value)
    assert CHECK_ENTITLEMENT in str(caught.value)
    assert caught.value.entry is caught.value.entries[0]


def test_a_partition_another_check_withholds_is_refused_after_this_one_clears(lake: Path):
    """Marketlake #426 through the reader that pays for it.

    The entitlement check quarantines, a second check quarantines beside it, and the
    entitlement check then passes. Its ``clean`` was the ledger's last line, so the partition
    read while the other check's fault stood.
    """
    from lake.loader import PartitionQuarantined, load_chain

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    append_verdict(
        lake,
        build_entry(
            partition="chains/ticker=SPY/date=2026-09-16.parquet",
            verdict=QUARANTINED_VERDICT,
            check="row_count_band",
            observed_at=NOW,
        ),
        observed_at=NOW,
    )

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    report = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=1), guards=GuardConstants())

    assert report.released == 0, "the row-count check still withholds it"
    assert report.withheld == 1
    assert report.deferred == 0, "a sibling check's deferral is not a human sign-off"
    with pytest.raises(PartitionQuarantined):
        load_chain("SPY", DAY, lake_root=lake)


def test_every_census_line_carries_its_own_number(lake: Path):
    """A report whose counters are all zero but one cannot catch a mislabelled line.

    Each count gets a distinct value, so swapping two labels, dropping one, or hardcoding a
    number fails here. ``human precedence`` and ``still withheld`` are the pair marketlake
    #426 split apart, and a swap is exactly what undoes that split.
    """
    printed = render(
        BatteryReport(
            judged=1,
            quarantined=2,
            cleared=3,
            insufficient_history=4,
            out_of_scope=5,
            deferred=6,
            withheld=7,
            released=8,
            unreadable=9,
            scope_unknown=10,
            appended=("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"),
            report=("battery: a line the run owed an operator",),
        )
    )

    assert "judged:               1" in printed
    assert "quarantined:          2" in printed
    assert "clean:                3" in printed
    assert "insufficient history: 4" in printed
    assert "out of scope:         5" in printed
    assert "human precedence:     6" in printed
    assert "still withheld:       7" in printed
    assert "released:             8" in printed
    assert "unreadable:           9" in printed
    assert "scope unknown:        10" in printed
    assert "ledger lines written: 11" in printed
    assert "battery: a line the run owed an operator" in printed


def test_the_report_line_names_every_check_still_withholding(lake: Path):
    """Two foreign quarantines stand and the passing check owes both names, not the first.

    The refusal and the panel each have a test for this claim. The report line makes the same
    claim to the operator who reads the nightly file.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    partition = "chains/ticker=SPY/date=2026-09-16.parquet"
    for check in ("row_count_band", "strike_grid_completeness"):
        append_verdict(
            lake,
            build_entry(
                partition=partition,
                verdict=QUARANTINED_VERDICT,
                check=check,
                observed_at=NOW,
            ),
            observed_at=NOW,
        )

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    (line,) = [ln for ln in report.report if "stays quarantined under" in ln]
    assert "'row_count_band'" in line
    assert "'strike_grid_completeness'" in line


def test_a_holder_naming_no_check_reads_as_prose_rather_than_as_none(lake: Path):
    """``str(None)`` in a report line is the word "None" dressed as a check name.

    Only a hand-written or damaged entry gets here, because ``build_entry`` refuses one
    without a check. The panel already shows such an entry as a dash, so the report owes the
    same rather than quoting a token nothing is called.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    append_quarantine(
        lake,
        {"partition": "chains/ticker=SPY/date=2026-09-16.parquet", "verdict": "stale"},
    )

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.withheld == 1
    assert any("stays quarantined under an unnamed check" in line for line in report.report)
    assert not any("'None'" in line for line in report.report)


def test_a_refusal_that_names_no_entry_is_refused_at_the_door(lake: Path):
    """A quarantine refusal carrying nothing is damage, not a refusal.

    ``manifest._latest_by_partition`` states the posture this follows: a reader that quietly
    stepped over damage in this ledger would make every check downstream weaker than it
    reads. The loader never builds one, and this is what keeps that true.
    """
    from lake.loader import PartitionQuarantined

    with pytest.raises(ValueError, match="needs the entries that withhold it"):
        PartitionQuarantined("chains/ticker=SPY/date=2026-09-16.parquet", ())


def test_a_release_is_counted_and_reported_when_the_last_check_clears(lake: Path):
    """A partition rejoining the readable set looks exactly like one nothing ever withheld."""
    from lake.loader import PartitionQuarantined, load_chain

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    report = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=1), guards=GuardConstants())

    assert report.released == 1
    assert report.withheld == 0
    assert any("now reads, no check withholds it" in line for line in report.report)
    assert "released:             1" in render(report)
    # The quarantine guard runs before the read, so its silence is the claim. This file's
    # minimal rows carry no close-tagged cycle, which is a later refusal and a different one.
    with pytest.raises(Exception) as caught:  # noqa: B017 - the type is the assertion
        load_chain("SPY", DAY, lake_root=lake)
    assert not isinstance(caught.value, PartitionQuarantined)


def test_a_dry_run_reports_the_release_the_real_run_would_produce(lake: Path):
    """Two walks would drift, and the counts before deciding must be the counts after."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))

    later = NOW + timedelta(days=1)
    dry = judge(lake, calendar=CALENDAR, now=later, guards=GuardConstants(), dry_run=True)
    real = judge(lake, calendar=CALENDAR, now=later, guards=GuardConstants())

    assert dry.released == real.released == 1
    assert dry.appended == ()
    # The count is a forecast and says so. Every other line in the walk is conditional under
    # a dry run, and a release stated in the present tense tells an operator the partition
    # reads while the ledger still refuses it.
    assert any("would now read, no check would withhold it" in line for line in dry.report)
    assert not any("now reads, no check withholds it" in line for line in dry.report)
    assert any("now reads, no check withholds it" in line for line in real.report)


def test_include_quarantined_reads_past_a_verdict_this_writer_wrote(fixture_lake):
    """The opt-in half of the same guard, so a verdict is never a one-way door.

    Built on ``FixtureLake`` rather than on this file's minimal schema, because the claim is
    about the production read path and that path wants the whole pinned shape plus the
    schema-version ledger. What the battery contributes is the entry, and that is written with
    the real writer.
    """
    from lake.loader import PartitionQuarantined, load_chain
    from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints

    day = "2026-08-24"
    ledger = SchemaVersionLedger(
        [
            RecordedVersion(
                version=1,
                recorded_at=datetime(2026, 8, 1, tzinfo=UTC),
                fingerprints=running_fingerprints(),
            )
        ]
    )
    root = (
        fixture_lake.with_chains("SPY", day)
        .with_reference("schema_versions", ledger.to_table())
        .build()
    )
    append_verdict(
        root,
        build_entry(
            partition=f"chains/ticker=SPY/date={day}.parquet",
            verdict=QUARANTINED_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW,
        ),
        observed_at=NOW,
    )

    with pytest.raises(PartitionQuarantined):
        load_chain("SPY", day, lake_root=root)

    table = load_chain("SPY", day, lake_root=root, include_quarantined=True)
    assert table.num_rows >= 1


def test_a_clean_run_leaves_the_ledger_empty_so_every_partition_still_reads(lake: Path):
    """The guard stays inert while the lake is healthy, which is what keeps it safe to ship."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _write(lake, "quotes", "SPY", DAY, _clean_rows("quotes"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 0
    assert latest_quarantine(lake) == {}
    assert not (lake / "quarantine.jsonl").exists()


def test_the_ledger_key_is_the_spelling_the_loader_looks_up(lake: Path):
    """A second spelling turns the guard from fail closed into fail open.

    ``loader.PartitionAbsent`` names the case: on macOS ``ticker=spy`` opens the ``ticker=SPY``
    partition while the quarantine lookup keys on the caller's spelling and finds no verdict.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert set(latest_quarantine(lake)) == {"chains/ticker=SPY/date=2026-09-16.parquet"}


# -- the walk ----------------------------------------------------------------


def test_the_walk_finds_both_surfaces_and_skips_a_name_that_is_not_a_date(lake: Path):
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _write(lake, "quotes", "SPY", DAY, _clean_rows("quotes"))
    stray = lake / "chains" / "ticker=SPY" / "date=notes.parquet"
    stray.write_text("not parquet")

    found = sealed_partitions(lake)

    assert [p.relative for p in found] == [
        "chains/ticker=SPY/date=2026-09-16.parquet",
        "quotes/ticker=SPY/date=2026-09-16.parquet",
    ]


def test_one_session_can_be_named_so_the_evening_run_judges_only_tonight(lake: Path):
    """Re-judging a partition sealed months ago against a median that has moved since would
    produce a verdict about the median rather than about the partition."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _write(lake, "chains", "SPY", date(2026, 9, 15), _clean_rows("chains"))
    _seed_spans(lake)

    assert len(sealed_partitions(lake)) == 2
    report = judge(lake, calendar=CALENDAR, now=NOW, day=DAY, guards=GuardConstants())
    assert {f.partition for f in report.findings if f.judged} == {
        f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
    }


def test_one_unreadable_partition_costs_its_own_verdict_and_not_the_run(lake: Path):
    """The partitions most likely to be unreadable are the ones a battery would quarantine."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    broken = lake / "chains" / "ticker=QQQ" / "date=2026-09-16.parquet"
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_text("not parquet at all")
    _seed_spans(lake)
    _seed_spans_for(lake, "QQQ", instrument_id=2)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.unreadable == 1
    assert report.quarantined == 1
    assert len(report.appended) == 1


def test_read_entitlement_reports_a_partition_missing_a_stamp_column(lake: Path):
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"), drop="vendor_quote_ts")
    with pytest.raises(PartitionUnreadable) as caught:
        read_entitlement(_partition(lake))
    assert "vendor_quote_ts" in str(caught.value)


def test_judge_entitlement_is_callable_on_evidence_alone(lake: Path):
    """The check's arithmetic, separable from the read, so a caller can hand it numbers."""
    evidence = Entitlement(
        rows=10, flag_present=True, flag_violations=0, median_staleness=-1.7, session_rows=10
    )
    finding = judge_entitlement(_partition(lake), evidence, GuardConstants())
    assert finding.verdict == CLEAN_VERDICT
    assert finding.judged is True
    assert finding.withholds is False


def test_a_finding_that_is_not_a_verdict_is_not_judged():
    finding = Finding(
        partition="chains/ticker=SPY/date=2026-09-16.parquet",
        surface="chains",
        ticker="SPY",
        day=DAY,
        check=CHECK_ENTITLEMENT,
        verdict=OUT_OF_SCOPE,
        reason="outside every span",
    )
    assert finding.judged is False
    assert finding.withholds is False


def test_an_empty_report_reads_as_a_run_that_wrote_nothing():
    assert BatteryReport().wrote_anything is False
    assert BatteryReport(appended=("a",)).wrote_anything is True


# -- the command -------------------------------------------------------------


def test_the_command_refuses_a_session_that_is_not_a_date(capsys):
    from lake.battery import main

    assert main(["--session", "last-tuesday"]) == 2
    assert "not a date" in capsys.readouterr().err


def test_the_report_prints_every_count_including_the_zeroes():
    """A run that judged nothing and a run that judged everything cleanly are different
    answers, and a report printing only non-zero counts would render them the same."""
    from lake.battery import render

    printed = render(BatteryReport(judged=4, cleared=4))

    assert "judged:               4" in printed
    assert "quarantined:          0" in printed
    assert "human precedence:     0" in printed
    assert "ledger lines written: 0" in printed


def test_the_report_names_each_quarantined_partition_with_its_reason():
    from lake.battery import render

    printed = render(
        BatteryReport(
            judged=1,
            quarantined=1,
            findings=(
                Finding(
                    partition="chains/ticker=SPY/date=2026-09-16.parquet",
                    surface="chains",
                    ticker="SPY",
                    day=DAY,
                    check=CHECK_ENTITLEMENT,
                    verdict=QUARANTINED_VERDICT,
                    reason="session-median staleness is 900.0s",
                ),
            ),
        )
    )

    assert "quarantined chains/ticker=SPY/date=2026-09-16.parquet" in printed
    assert "session-median staleness is 900.0s" in printed


# -- a pass never clears a verdict it did not write ---------------------------


def test_a_clean_entitlement_verdict_does_not_clear_a_humans_quarantine(lake: Path):
    """The defect this rule exists for, in the shape an operator actually produces.

    A human quarantines the day for a missing expiry. Tonight the feed is real-time, so the
    entitlement check passes. Without the rule, a `clean` line under `realtime_entitlement`
    becomes the last entry and the reader hands back a partition a human refused.
    """
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    partition = "chains/ticker=SPY/date=2026-09-16.parquet"
    append_verdict(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check="strike_grid_completeness",
            observed_at=NOW,
            provenance=PROVENANCE_HUMAN,
            reason="the 09-16 chain is missing the whole 07-17 expiry",
        ),
        observed_at=NOW,
    )

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == CLEAN_VERDICT
    assert report.appended == ()
    assert latest_quarantine(lake)[partition]["check"] == "strike_grid_completeness"
    assert is_quarantined(latest_quarantine(lake)[partition]) is True
    assert any("stays quarantined under" in line for line in report.report)


def test_a_clean_verdict_does_not_clear_a_sibling_checks_quarantine(lake: Path):
    """The same rule against the shape #407 lands: another automated check on this spine."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)
    partition = "chains/ticker=SPY/date=2026-09-16.parquet"
    append_verdict(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check="row_count_band",
            observed_at=NOW,
        ),
        observed_at=NOW,
    )

    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert is_quarantined(latest_quarantine(lake)[partition]) is True


def test_a_quarantine_still_supersedes_another_checks_verdict(lake: Path):
    """The rule is one-way. A fresh fault is news whatever wrote the entry before it."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    partition = "chains/ticker=SPY/date=2026-09-16.parquet"
    append_verdict(
        lake,
        build_entry(
            partition=partition, verdict=CLEAN_VERDICT, check="row_count_band", observed_at=NOW
        ),
        observed_at=NOW,
    )

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert len(report.appended) == 1
    assert latest_quarantine(lake)[partition]["check"] == CHECK_ENTITLEMENT
    assert is_quarantined(latest_quarantine(lake)[partition]) is True


def test_a_clean_verdict_still_clears_its_own_checks_quarantine(lake: Path):
    """The rule must not freeze the ledger. Its own earlier verdict is still supersedable."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake)
    judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-1.7))
    report = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=1), guards=GuardConstants())

    assert len(report.appended) == 1
    partition = "chains/ticker=SPY/date=2026-09-16.parquet"
    assert is_quarantined(latest_quarantine(lake)[partition]) is False


# -- the staleness median is the session's ------------------------------------


def _off_session_row(minute: int, *, staleness: float, surface: str = "chains") -> dict:
    """A row captured at 23:25 Eastern the night before, which the live lake carries.

    Both 2026-09-16 chain partitions hold about 12,000 such rows at 03:25 UTC. The vendor's
    last-quote stamp freezes when the market is closed while ``fetch_ts`` keeps moving, so the
    row is hours stale on a feed that is real-time by every other measure.
    """
    row = _row(minute, staleness=staleness, flag=False, surface=surface)
    snap = datetime(2026, 9, 16, 3, 25, tzinfo=UTC) + timedelta(minutes=minute)
    fetch = snap + timedelta(milliseconds=400)
    row["snap_ts"] = snap.isoformat()
    row["fetch_ts"] = fetch.isoformat()
    row["vendor_quote_ts"] = (fetch - timedelta(seconds=staleness)).isoformat()
    return row


def test_an_overnight_cycle_does_not_drag_the_session_median(lake: Path):
    """The live shape: a healthy session with an off-session cycle sitting beside it."""
    rows = _clean_rows("chains", staleness=-1.7, count=5)
    rows += [_off_session_row(i, staleness=25_817.0) for i in range(5)]
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == CLEAN_VERDICT
    assert report.quarantined == 0
    assert _answer(report).computed == pytest.approx(-1.7)


def test_a_partition_of_overnight_rows_alone_is_out_of_scope_not_quarantined(lake: Path):
    """A day that captured only an overnight cycle recorded no session, so it judges none.

    Without this the check reads 25,817 seconds of staleness and quarantines a partition whose
    feed was real-time on every row. The lake already holds four gap-only sessions from a
    machine that was down, so this is one landed cycle away from real.
    """
    _write(lake, "chains", "SPY", DAY, [_off_session_row(i, staleness=25_817.0) for i in range(5)])
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    # One finding for the partition, carrying the partition's own token rather than any
    # check's. The review that restored this assertion caught the weakened version: asking
    # only what the entitlement check said is asking the one thing this change did not alter,
    # while quote sanity was quietly returning a real verdict on a partition the design says
    # nothing should judge.
    (finding,) = [f for f in report.findings if f.partition == JUDGED]
    assert finding.check == CHECK_SCOPE
    assert report.out_of_scope == 1
    assert report.judged == 0
    assert report.quarantined == 0
    assert report.appended == ()
    assert "no data row falls inside the session" in finding.reason


def test_an_off_session_row_still_counts_against_the_entitlement_flag(lake: Path):
    """The split the design draws: flags on every snapshot, staleness on the session's.

    A delayed flag is the vendor's own statement and does not depend on the hour.
    """
    rows = _clean_rows("chains", staleness=-1.7, count=5)
    delayed = _off_session_row(0, staleness=1.0)
    delayed["is_delayed"] = True
    rows.append(delayed)
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert "do not carry is_delayed=False" in report.findings[0].reason


def test_a_sealed_partition_on_a_non_session_day_keeps_the_flag_half(lake: Path):
    """No session means no median to take, and refusing it would quarantine a calendar
    disagreement rather than a feed fault. The flag half is what can still speak."""
    saturday = date(2026, 9, 19)
    rows = _clean_rows("chains", staleness=-1.7)
    for index, row in enumerate(rows):
        snap = datetime(2026, 9, 19, 13, 30, tzinfo=UTC) + timedelta(minutes=index)
        row["snap_ts"] = snap.isoformat()
        row["is_delayed"] = True
    _write(lake, "chains", "SPY", saturday, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.quarantined == 1
    assert "do not carry is_delayed=False" in report.findings[0].reason


# -- the statistic is the median, and the boundaries are half-open ------------


def test_one_outlier_row_does_not_move_the_verdict(lake: Path):
    """The median is required rather than decorative, and a mean is not a median.

    Every other fixture here uses one staleness for every row, where the mean and the median
    coincide, so none of them can tell the two apart. The live lake's tail is why this matters:
    the per-row maximum on QQQ 2026-09-14 is 1,789,392,600 seconds against a session median of
    -1.9, and a mean over that partition would read about 447 million seconds and quarantine a
    feed that is fine.
    """
    rows = _clean_rows("chains", staleness=-1.7, count=5)
    rows.append(_row(5, staleness=1_789_392_600.0, flag=False, surface="chains"))
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert _answer(report).verdict == CLEAN_VERDICT
    assert report.quarantined == 0
    assert _answer(report).computed == pytest.approx(-1.7)


def test_a_span_opening_at_the_days_end_does_not_cover_that_day(lake: Path):
    """The interval is half-open, so a span that opens as the day ends covers none of it."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(lake, start=datetime(2026, 9, 17, 4, 0, tzinfo=UTC))

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0


def test_a_span_closing_at_the_days_start_does_not_cover_that_day(lake: Path):
    """The other end of the same interval, which a retirement produces."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=900.0))
    _seed_spans(
        lake,
        start=SPAN_START,
        end=datetime(2026, 9, 16, 4, 0, tzinfo=UTC),
    )

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0


def test_transition_compares_the_check_directly(lake: Path):
    """Asserted on the function rather than through ``judge``, because one check exists today.

    Through ``judge`` the clause is unreachable, so deleting it changes nothing a run can see.
    What it guards is the day a second check finds a different fault on a partition the first
    already quarantined: without it that finding is never recorded and never pages.

    Since marketlake #426 the entry handed over is that check's own, so the case is a check
    with nothing recorded against it meeting a partition another check already withholds.
    """
    from lake.battery import _entitlement_finding, _transition

    quarantined_by_b = {
        "partition": "chains/ticker=SPY/date=2026-09-16.parquet",
        "verdict": QUARANTINED_VERDICT,
        "check": CHECK_ENTITLEMENT,
        "provenance": PROVENANCE_BATTERY,
    }
    fails_under_b = _entitlement_finding(
        _partition(lake), QUARANTINED_VERDICT, "the feed is delayed"
    )

    # Nothing recorded under this check, and it now fails: news, whatever else withholds.
    assert _transition(None, fails_under_b) is True
    # Its own entry already says so: the same news a second time.
    assert _transition(quarantined_by_b, fails_under_b) is False


def test_the_manifest_entry_and_the_ledger_line_agree_on_the_clock(lake: Path):
    """Both writes happen in one invocation, so a reader comparing them compares one clock."""
    entry = build_entry(
        partition="chains/ticker=SPY/date=2026-09-16.parquet",
        verdict=QUARANTINED_VERDICT,
        check=CHECK_ENTITLEMENT,
        observed_at=NOW,
    )
    append_verdict(lake, entry, observed_at=NOW)

    manifested = [
        json.loads(line)
        for line in (lake / "manifest.jsonl").read_text().splitlines()
        if line.strip() and json.loads(line)["partition"] == "quarantine.jsonl"
    ]
    assert len(manifested) == 1
    assert manifested[0]["fetched_at"] == entry["observed_at"]
    assert manifested[0]["source"] == "battery"
    assert manifested[0]["rows"] == 1


def test_a_refused_page_keeps_its_body_off_stderr(lake: Path, capsys):
    """The publisher redacted its own record because the body carried a secret.

    Printing the body to stderr afterwards would undo that, and launchd writes stderr to a file
    on disk. The body is a count, a staleness figure and partition paths, so this is defence in
    depth rather than a live leak, which is why nothing had exercised it.
    """
    from lake.battery import _entitlement_finding

    secret = "chains/ticker=SPY/date=2026-09-16.parquet"
    publisher, transport = _publisher(lake, secrets=(secret,))
    finding = _entitlement_finding(
        _partition(lake), QUARANTINED_VERDICT, "delayed", computed=900.0, against=60.0
    )

    page_delayed_feed(publisher, [finding], now=NOW)

    printed = capsys.readouterr().err
    assert transport.messages == []
    assert secret not in printed
    assert "refused" in printed


# -- trading-calendar coverage -----------------------------------------------
#
# The check's correct answer against the live lake is that it found nothing, so these tests
# have to prove both halves: that it reports a real miss, and that it stays quiet about every
# session no span ever owed.


def _shift(rows: list[dict], day: date) -> list[dict]:
    """The same rows, moved whole to another session."""
    delta = day - DAY
    moved = []
    for row in rows:
        copy = dict(row)
        for field in ("snap_ts", "fetch_ts", "vendor_quote_ts"):
            copy[field] = (datetime.fromisoformat(row[field]) + delta).isoformat()
        moved.append(copy)
    return moved


def _seed_options(root: Path, *, options: bool) -> None:
    """A span for SPY that either carries options or does not."""
    from lake.capture_spans import CaptureSpans
    from lake.security_master import SecurityMaster

    master = SecurityMaster()
    instrument_id = master.register(
        kind="equity",
        capture_start=SPAN_START,
        valid_from=SPAN_START.date(),
        ticker="SPY",
    )
    (root / "reference").mkdir(parents=True, exist_ok=True)
    pa_pq.write_table(master.to_table(), root / "reference" / "security_master.parquet")
    spans = CaptureSpans(
        [CaptureSpan(instrument_id=instrument_id, start=SPAN_START, end=None, options=options)]
    )
    pa_pq.write_table(spans.to_table(), root / "reference" / "capture_spans.parquet")


def _coverage(lake: Path, *, now: datetime = NOW):
    return coverage(lake, read_reference(lake), CALENDAR, now=now)


def _cover_all(lake: Path, *, days=None, surfaces=SEALED_SURFACES, ticker: str = "SPY") -> None:
    """Seal an empty-but-present partition for every session a span owes."""
    for day in days if days is not None else (COVERED_SESSIONS):
        for surface in surfaces:
            _write(lake, surface, ticker, day, _shift(_clean_rows(surface), day))


# Every session ``CALENDAR`` holds between ``SPAN_START`` and ``NOW``, which is what an open
# span owes a partition for. 2026-09-17 and 2026-09-18 are sessions too and fall after ``NOW``.
COVERED_SESSIONS = (
    date(2026, 9, 1),
    date(2026, 9, 2),
    date(2026, 9, 3),
    date(2026, 9, 4),
    date(2026, 9, 14),
    date(2026, 9, 15),
    date(2026, 9, 16),
)


def test_a_session_inside_the_span_with_no_partition_is_reported_missing(lake: Path):
    """The whole point. Nothing else in the lake can see a session that was never captured."""
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 15)])

    found = _coverage(lake)

    assert {finding.partition for finding in found.missing} == {
        "chains/ticker=SPY/date=2026-09-15.parquet",
        "quotes/ticker=SPY/date=2026-09-15.parquet",
    }
    assert all(finding.check == CHECK_CALENDAR_COVERAGE for finding in found.missing)
    assert all(finding.verdict == MISSING_SESSION for finding in found.missing)
    assert found.owed == len(COVERED_SESSIONS) * 2


def test_a_fully_captured_span_reports_nothing_and_still_says_it_ran(lake: Path):
    """The live answer. Silence has to be told apart from a check that did not run."""
    _seed_spans(lake)
    _cover_all(lake)

    found = _coverage(lake)

    assert found.missing == ()
    assert found.owed == 14
    assert coverage_line(found) == "battery: calendar coverage, 14 owed sessions, all present"


def test_a_partition_of_gap_rows_alone_counts_as_present(lake: Path):
    """A gap row is the design's record that a minute was missed. That is the loud case.

    What coverage exists to find is the silent one, a session with no partition at all, so a
    day the lake marked is covered rather than missing.
    """
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 15)])
    _write(
        lake,
        "chains",
        "SPY",
        date(2026, 9, 15),
        _shift(
            [_row(0, staleness=-1.7, flag=None, surface="chains", kind="gap")], date(2026, 9, 15)
        ),
    )
    _write(
        lake, "quotes", "SPY", date(2026, 9, 15), _shift(_clean_rows("quotes"), date(2026, 9, 15))
    )

    assert _coverage(lake).missing == ()


def test_a_session_before_the_span_opens_is_not_owed(lake: Path):
    """Capture was not running, so nothing about that session is evidence about the feed."""
    _seed_spans(lake, start=datetime(2026, 9, 14, 13, 30, tzinfo=UTC))
    _cover_all(lake, days=(date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)))

    found = _coverage(lake)

    assert found.missing == ()
    assert found.owed == 6, "the four August-week sessions precede the span"


def test_a_span_opening_after_the_option_close_does_not_owe_that_session(lake: Path):
    """The rule ``in_scope`` deliberately does not use, and why the two differ.

    ``in_scope`` widens to the whole calendar day, because the rows a partition holds are the
    ones capture wrote and the onboarding day's morning falls outside the span. Coverage asks
    whether capture could have written anything at all, and a span opening at 17:00 Eastern
    covers none of that day's session.
    """
    _seed_spans(lake, start=datetime(2026, 9, 14, 21, 0, tzinfo=UTC))
    _cover_all(lake, days=(date(2026, 9, 15), date(2026, 9, 16)))

    found = _coverage(lake)

    assert found.missing == (), "2026-09-14's session was over before the span opened"
    assert found.owed == 4


def test_a_session_whose_compaction_has_not_run_is_not_owed_yet(lake: Path):
    """A partition seals at close+15, so before that it is being built rather than missing."""
    from lake.session import COMPACTION_DELAY

    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != DAY])
    closed = CALENDAR.option_close(DAY)

    assert _coverage(lake, now=closed + COMPACTION_DELAY - timedelta(seconds=1)).missing == ()
    assert len(_coverage(lake, now=closed + COMPACTION_DELAY).missing) == 2


def test_a_closed_span_owes_nothing_after_it_closes(lake: Path):
    """Retiring a ticker stops the clock. Its later sessions are not its to answer for."""
    _seed_spans(lake, end=datetime(2026, 9, 14, 21, 0, tzinfo=UTC))
    _cover_all(
        lake,
        days=(
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
            date(2026, 9, 4),
            date(2026, 9, 14),
        ),
    )

    found = _coverage(lake)

    assert found.missing == ()
    assert found.owed == 10


def test_a_span_without_options_owes_quotes_and_never_chains(lake: Path):
    """``options: false`` skips the chain snapshot, so a chains partition was never owed."""
    _seed_options(lake, options=False)
    _cover_all(lake, surfaces=("quotes",))

    found = _coverage(lake)

    assert found.missing == ()
    assert found.owed == len(COVERED_SESSIONS), "quotes alone"


def test_a_day_the_calendar_calls_closed_is_not_owed(lake: Path):
    """The calendar decides which days exist. A weekend is not a missing session."""
    _seed_spans(lake)
    _cover_all(lake)

    assert {finding.day for finding in _coverage(lake).missing} == set()
    assert all(CALENDAR.is_session(day) for day in COVERED_SESSIONS)
    assert not CALENDAR.is_session(date(2026, 9, 5)), "a Saturday inside the span"


def test_a_renamed_tickers_old_partitions_still_count_as_covered(lake: Path):
    """Resolving the spelling as of the run date loses every partition written before a rename.

    ``capture_spans_by_ticker`` states the rule for the other direction and it holds here: the
    instrument is looked for under every spelling it ever carried, or a partition that exists
    is reported missing and an operator goes looking for a file that is on disk.
    """
    from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path

    _seed_spans(lake)
    _cover_all(lake)
    master = SecurityMaster.read(master_path(lake))
    master.remap(1, ID_TYPE_TICKER, "SPYX", date(2026, 9, 16))
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")

    assert _coverage(lake).missing == (), "every partition is still under ticker=SPY"


def test_coverage_walks_the_whole_span_even_when_one_session_is_named(lake: Path):
    """``sweep`` passes tonight's session, and a session with no partition is a night on which
    nothing ran. A coverage check scoped to tonight can never see the night it missed."""
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 1)])

    report = judge(lake, calendar=CALENDAR, now=NOW, day=DAY, guards=GuardConstants())

    assert report.sessions_missing == 2
    assert {f.day for f in report.findings if f.verdict == MISSING_SESSION} == {date(2026, 9, 1)}
    assert {f.partition for f in report.findings if f.judged} == {
        f"chains/ticker=SPY/date={DAY.isoformat()}.parquet",
        f"quotes/ticker=SPY/date={DAY.isoformat()}.parquet",
    }


def test_a_missing_session_writes_no_ledger_line_and_never_pages(lake: Path):
    """``loader._clear_partition`` raises ``PartitionAbsent`` before it reads the ledger, so a
    verdict for a partition that never landed changes no read. It could also never be cleared,
    because there is no backfill and the partition can never land."""
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 1)])
    publisher, transport = _publisher(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert report.sessions_missing == 2
    assert report.appended == ()
    assert not (lake / "quarantine.jsonl").exists()
    assert transport.messages == []


def test_the_second_run_reports_the_same_census_and_appends_nothing(lake: Path):
    """A missing session is permanent, so this repeats every night. What must not repeat is a
    ledger line, and what must not grow is the report."""
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 1)])

    first = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    second = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert first.report == second.report
    assert len([line for line in second.report if "calendar coverage" in line]) == 1
    assert second.appended == ()


def test_the_coverage_line_names_no_partition_so_the_digest_stays_counts(lake: Path):
    """``sweep``'s own digest test pins it: the digest carries counts and never a list of
    findings, because a list is under the cap on every night anyone tested and over it on the
    night that mattered. The report list is what the digest is built from."""
    _seed_spans(lake)

    line = coverage_line(_coverage(lake))

    assert "SPY" not in line
    assert "ticker=" not in line
    assert ".parquet" not in line
    assert "14 of 14" in line and "2026-09-01 to 2026-09-16" in line


def test_the_coverage_line_survives_the_report_files_redaction(lake: Path):
    """Every battery report line reaches the nightly file and the digest through
    ``report.redacted``, which drops everything past the second field. A line spelled
    ``battery: calendar coverage: 3 of 27`` arrives with every number gone."""
    from lake.report import redacted

    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 1)])

    for line in judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()).report:
        assert redacted(line) == line, line


def test_render_names_each_missing_session_that_the_report_line_does_not(lake: Path):
    """The names live on the job's own stdout, which is under neither cap."""
    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day != date(2026, 9, 1)])

    printed = render(judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()))

    assert "  sessions owed:        14" in printed
    assert "  sessions missing:     2" in printed
    assert "missing chains/ticker=SPY/date=2026-09-01.parquet" in printed


def test_an_instrument_coverage_cannot_name_costs_its_own_answer_and_not_the_run(lake: Path):
    """Raising here took the whole battery down over an instrument that owns no partition.

    `capture_spans.build_from_master` opens a span for every instrument in the master when the
    roster cannot be read, and `SecurityMaster.register` takes no ticker at all, so the state is
    reachable by the design's own widening. The refusal is a third answer, not out of scope, so
    it is reported and exits non-zero. What it must not do is silence every other ticker's
    verdict and the one page the battery sends.
    """
    from lake.capture_spans import CaptureSpans
    from lake.security_master import SecurityMaster, master_path

    _write(lake, "chains", "SPY", DAY, _clean_rows("chains", staleness=-900.0))
    _seed_spans(lake)
    master = SecurityMaster.read(master_path(lake))
    orphan = master.register(
        kind="equity",
        capture_start=SPAN_START,
        valid_from=SPAN_START.date(),
        figi="BBG000BDTBL9",
    )
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")
    spans = CaptureSpans.read(lake / "reference" / "capture_spans.parquet")
    pa_pq.write_table(
        CaptureSpans(
            [
                *spans.spans,
                CaptureSpan(instrument_id=orphan, start=SPAN_START, end=None, options=True),
            ]
        ).to_table(),
        lake / "reference" / "capture_spans.parquet",
    )
    publisher, transport = _publisher(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert report.scope_unknown == 1
    assert any("has a capture span and no ticker" in line for line in report.report)
    assert _answer(report).verdict == QUARANTINED_VERDICT, "SPY is still judged"
    assert len(transport.messages) == 1, "and the delayed feed still pages"


# -- quote sanity ------------------------------------------------------------


def _ordered_rows(surface: str, *, count: int = 10, crossed: int = 0, **overrides) -> list[dict]:
    """``count`` data rows, ``crossed`` of which have bid above ask."""
    rows = _clean_rows(surface, count=count)
    for index in range(crossed):
        rows[index]["bid"], rows[index]["ask"] = 1.05, 1.0
    for field, value in overrides.items():
        rows[0][field] = value
    return rows


def test_a_crossed_rate_under_the_tolerance_passes(lake: Path):
    """The live shape. SPY's 2026-09-16 chains partition holds 7,909 crossed rows of
    5,307,030, which is 0.149 percent, and it is the lake's most recent complete session."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=1000, crossed=1))
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == CLEAN_VERDICT
    assert finding.computed == pytest.approx(0.001)
    assert finding.against == QUOTE_SANITY_TOLERANCE


def test_a_crossed_rate_over_the_tolerance_quarantines(lake: Path):
    """A feed delivering bid and ask transposed reads near 100 percent, because an option
    quoted 0.00 by 0.05 crosses the moment the two are swapped."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=100, crossed=100))
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.computed == pytest.approx(1.0)
    assert "not ordered bid <= mark <= ask" in finding.reason


@pytest.mark.parametrize(("crossed", "verdict"), [(5, CLEAN_VERDICT), (6, QUARANTINED_VERDICT)])
def test_the_tolerance_is_a_ceiling_the_rate_has_to_pass(lake: Path, crossed: int, verdict: str):
    """Five percent of a hundred rows passes and the next row does not."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=100, crossed=crossed))
    _seed_spans(lake)

    assert _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY).verdict == verdict


def test_a_mark_outside_an_uncrossed_spread_counts_too(lake: Path):
    """The design says bid <= mid <= ask, which is one predicate rather than two checks. On
    the live lake the two sets are identical in both directions, and this is the row that
    would separate them."""
    rows = _ordered_rows("chains", count=10)
    for row in rows:
        row["mark"] = 9.99

    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.computed == pytest.approx(1.0)


@pytest.mark.parametrize("field", ["bid", "ask", "mark"])
def test_a_null_leaves_the_row_unordered_rather_than_passing(lake: Path, field: str):
    """A comparison against null is null, and a row whose ordering cannot be evaluated has not
    passed it. That is the reading a null entitlement flag already gets."""
    rows = _ordered_rows("chains", count=10)
    for row in rows:
        row[field] = None

    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.computed == pytest.approx(1.0)


@pytest.mark.parametrize("column", ["bid", "ask", "mark"])
def test_a_missing_column_quarantines_rather_than_passing(lake: Path, column: str):
    """All three are in the pinned capture schema for both surfaces, so an absence is drift."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"), drop=column)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == QUARANTINED_VERDICT
    assert f"carries no {column} column" in finding.reason


def test_quotes_are_ordered_too(lake: Path):
    """The design's equity-only subset names quote sanity, so it is not an options-only check."""
    _write(lake, "quotes", "SPY", DAY, _ordered_rows("quotes", count=10, crossed=10))
    _seed_spans(lake)

    assert _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY).verdict == (
        QUARANTINED_VERDICT
    )


def test_a_gap_row_is_not_measured_against_the_tolerance(lake: Path):
    """A gap row carries no vendor observation, so it cannot be ordered or fail to be."""
    rows = _clean_rows("chains", count=5)
    gap = _row(9, staleness=-1.7, flag=None, surface="chains", kind="gap")
    gap["bid"], gap["ask"], gap["mark"] = None, None, None
    _write(lake, "chains", "SPY", DAY, [*rows, gap])
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY)

    assert finding.verdict == CLEAN_VERDICT
    assert "of 5 data rows" in finding.reason


def test_read_quote_order_counts_the_rows_it_could_not_order(lake: Path):
    """The evidence on its own, without a run around it."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=10, crossed=3))

    evidence = read_quote_order(_partition(lake))

    assert evidence.rows == 10
    assert evidence.unordered == 3
    assert evidence.absent == ()
    assert evidence.rate == pytest.approx(0.3)


def test_judge_quote_order_is_callable_on_evidence_alone(lake: Path):
    """The judgment is a pure function of the evidence, so a test can drive it directly."""
    from lake.battery import QuoteOrder

    finding = judge_quote_order(_partition(lake), QuoteOrder(rows=1000, unordered=1))

    assert finding.verdict == CLEAN_VERDICT
    assert finding.check == CHECK_QUOTE_SANITY


# -- the snapshot row-count band ---------------------------------------------


def _snapshots(
    on: date, *, count: int, rows_each: int, first: int = 0, day: date | None = None
) -> list[dict]:
    """``count`` session snapshots, each holding ``rows_each`` data rows.

    ``day`` moves them to another session; without it they land on ``on``.
    """
    rows: list[dict] = []
    for index in range(count):
        rows.extend(
            _row(first + index, staleness=-1.7, flag=False, surface="chains")
            for _ in range(rows_each)
        )
    return _shift(rows, on if day is None else day)


def _gap_row() -> dict:
    """One gap row, the design's record that a minute was missed."""
    return _row(0, staleness=-1.7, flag=None, surface="chains", kind="gap")


def _sessions_before(calendar, day: date, count: int) -> list[date]:
    """The ``count`` sessions immediately before ``day``, newest first."""
    found: list[date] = []
    when = day
    while len(found) < count:
        when -= timedelta(days=1)
        if calendar.is_session(when):
            found.append(when)
    return found


def _history(lake: Path, *, rows_each: int = 100, sessions=None) -> None:
    """A trailing history of full sessions, so the band has a median to judge against."""
    for day in sessions if sessions is not None else COVERED_SESSIONS[:-1]:
        _write(lake, "chains", "SPY", day, _snapshots(day, count=3, rows_each=rows_each))


def test_a_thin_history_tags_insufficient_history_rather_than_passing(lake: Path):
    """The design: a median-relative check with fewer than five trailing sessions still runs
    and tags its rows. The tag is a finding and never a verdict, because the reader fails
    closed on anything that is not clean."""
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    finding = _answer(report, CHECK_ROW_COUNT_BAND)

    assert finding.verdict == INSUFFICIENT_HISTORY
    assert not finding.judged
    assert report.appended == ()
    assert finding.against == 5.0


def test_a_truncated_snapshot_quarantines_the_partition(lake: Path):
    """What the check is for. One cycle returning a fraction of the chain is a truncated
    fetch, and the threshold is one snapshot rather than a rate, because a check tolerating
    some would not catch them."""
    _history(lake)
    rows = _snapshots(DAY, count=2, rows_each=100) + _snapshots(DAY, count=1, rows_each=50, first=2)
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.computed == 50.0
    assert finding.against == 100.0
    assert "1 of 3 session snapshots" in finding.reason


def test_a_snapshot_inside_the_band_passes(lake: Path):
    """The band is thirty percent either side, and the live lake moves about one."""
    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=75))
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.verdict == CLEAN_VERDICT
    assert "70 to 130 rows" in finding.reason


def test_the_trailing_window_is_prior_sessions_only(lake: Path):
    """A window including the judged session lets a wholly truncated session drag its own
    median down and pass itself, and at five trailing sessions it is one fifth of the median
    it is compared against."""
    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=10))
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.against == 100.0, "the judged session is not in its own median"


def test_a_gap_only_trailing_session_is_skipped_rather_than_counted_as_zero(lake: Path):
    """Sixteen of the lake's 29 sealed partitions hold gap rows alone. Counting one as zero
    would drag the median toward zero and put every later session above the band."""
    _history(lake, sessions=COVERED_SESSIONS[:-1])
    for dark in COVERED_SESSIONS[:4]:
        _write(
            lake,
            "chains",
            "SPY",
            dark,
            _shift([_row(0, staleness=-1.7, flag=None, surface="chains", kind="gap")], dark),
        )
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    finding = _answer(report, CHECK_ROW_COUNT_BAND, JUDGED)

    # Four of the six trailing sessions are dark. Counted as zero their median would be zero,
    # the band around it would be zero to zero, and a session of 100-row snapshots would be
    # quarantined for being too big. Skipped, two sessions remain and the history is thin.
    assert finding.verdict == INSUFFICIENT_HISTORY
    assert finding.computed == 2.0


def test_an_overnight_cycle_is_left_out_of_the_counts(lake: Path):
    """An overnight chain is not a session observation, and a median built from session
    snapshots is the wrong thing to measure it against."""
    rows = _snapshots(DAY, count=3, rows_each=100)
    rows.extend(_off_session_row(index, staleness=25_817.0) for index in range(40))
    _write(lake, "chains", "SPY", DAY, rows)

    counts = session_snapshot_counts(
        _partition(lake), (CALENDAR.session_open(DAY), CALENDAR.option_close(DAY))
    )

    assert counts == (100, 100, 100)


def test_a_quotes_partition_gets_no_row_count_finding(lake: Path):
    """The design's equity-only subset leaves the band out, and on quotes a snapshot is one
    row, so the band would compare one against a trailing median of one for ever."""
    _write(lake, "quotes", "SPY", DAY, _clean_rows("quotes"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert [f.check for f in report.findings if f.check == CHECK_ROW_COUNT_BAND] == []


def test_the_trailing_median_is_memoized_so_one_partition_is_read_once(lake: Path):
    """A whole-lake run otherwise reads each partition's twenty predecessors once per
    partition, which is twenty times the reads for the same answer."""
    from lake import battery

    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake)
    opened: list[str] = []
    real = battery.session_snapshot_counts

    def counting(partition, bounds):
        opened.append(partition.relative)
        return real(partition, bounds)

    battery.session_snapshot_counts = counting
    try:
        judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())
    finally:
        battery.session_snapshot_counts = real

    assert len(opened) == len(set(opened)), opened


def test_trailing_medians_skips_a_session_outside_the_span(lake: Path):
    """A session capture was not running for says nothing about the chain's size either."""
    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake, start=datetime(2026, 9, 14, 13, 30, tzinfo=UTC))

    trailing = trailing_medians(
        lake,
        _partition(lake),
        calendar=CALENDAR,
        spans=(_span(start=datetime(2026, 9, 14, 13, 30, tzinfo=UTC)),),
        guards=GuardConstants(),
    )

    assert trailing == (100.0, 100.0), "2026-09-01 through 2026-09-04 precede the span"


def test_median_averages_the_middle_pair_on_an_even_count():
    """The same interpolation ``pc.quantile(..., interpolation="midpoint")`` gives, so the
    module does not answer one question two ways."""
    assert median([1, 2, 3]) == 2.0
    assert median([1, 2, 3, 4]) == 2.5
    assert median([4, 1, 3, 2]) == 2.5


def test_judge_row_count_is_callable_on_counts_alone(lake: Path):
    """The judgment is a pure function of the counts and the trailing medians."""
    finding = judge_row_count(_partition(lake), (100, 100), (100.0,) * 5, GuardConstants())

    assert finding.verdict == CLEAN_VERDICT
    assert finding.check == CHECK_ROW_COUNT_BAND


# -- three checks on one partition -------------------------------------------


def test_every_check_for_one_partition_is_decided_in_one_call(lake: Path):
    """``decide_partition`` carries the ledger state forward as lines land, and two checks
    clearing in one walk both change what withholds the partition. Called once per finding
    that state never accumulates and the release is never reported."""
    partition = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
    for check in (CHECK_ENTITLEMENT, CHECK_QUOTE_SANITY):
        append_quarantine(
            lake,
            build_entry(
                partition=partition,
                verdict=QUARANTINED_VERDICT,
                check=check,
                observed_at=NOW - timedelta(days=1),
            ),
        )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.released == 1, "both checks cleared in one walk, so the partition reads again"
    assert len(report.appended) == 2
    assert sum("now reads" in line for line in report.report) == 1
    assert withholding(latest_quarantine_by_check(lake)[partition]) == ()


def test_a_partition_passing_three_checks_under_one_hold_says_so_once(lake: Path):
    """Three lines saying the same partition is still quarantined are the same fact three
    times, in a list ``sweep`` puts through the digest's byte cap."""
    partition = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
    append_quarantine(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check="strike_grid_completeness",
            observed_at=NOW - timedelta(days=1),
            provenance=PROVENANCE_HUMAN,
        ),
    )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    (line,) = [ln for ln in report.report if "stays quarantined under" in ln]
    assert report.withheld == 1
    assert CHECK_ENTITLEMENT in line and CHECK_QUOTE_SANITY in line
    assert "'strike_grid_completeness'" in line


def test_the_delayed_feed_page_does_not_fire_on_a_crossed_quote(lake: Path):
    """The page belongs to one check. A quote-sanity quarantine reaching a phone titled
    ``Delayed feed`` would render its rate as a staleness in seconds, and the design gives the
    battery two pages of which the other is #427."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=100, crossed=100))
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert _answer(report, CHECK_QUOTE_SANITY).verdict == QUARANTINED_VERDICT
    assert report.appended == (JUDGED,)
    assert report.paged == ()
    assert transport.messages == []


def test_a_delayed_feed_still_pages_while_another_check_quarantines_beside_it(lake: Path):
    """The filter is on the check and not on the partition, so one check's silence does not
    take the other's page with it."""
    rows = _ordered_rows("chains", count=100, crossed=100)
    for row in rows:
        row["vendor_quote_ts"] = (
            datetime.fromisoformat(row["fetch_ts"]) - timedelta(seconds=900)
        ).isoformat()
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)
    publisher, transport = _publisher(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants(), publisher=publisher)

    assert len(report.appended) == 2
    assert report.paged == (f"chains/ticker=SPY/date={DAY.isoformat()}.parquet",)
    assert len(transport.messages) == 1
    assert "staleness" in transport.messages[0].body


# -- what the review found, held so it cannot come back ----------------------


def _long_calendar(weeks: int = 8):
    """A run of ordinary weeks ending in ``DAY``'s, for the windowing tests."""
    mondays = [date(2026, 9, 14) - timedelta(weeks=index) for index in range(weeks)]
    return weekday_sessions(*reversed(mondays))


def test_the_band_does_not_judge_a_day_the_calendar_calls_no_session(lake: Path):
    """The asymmetry the review found. ``trailing_medians`` skips a day with no session, and
    the band was handed that same day's ``None`` bounds, which widen ``_within`` to every row.
    Every overnight cycle then became a session snapshot measured against a median built from
    real sessions alone, and a partition was quarantined for a calendar disagreement rather
    than a truncated fetch.

    ``test_a_sealed_partition_on_a_non_session_day_keeps_the_flag_half`` already stated the
    rule for the entitlement check. This is the same rule for the band.
    """
    saturday = date(2026, 9, 19)
    rows = _snapshots(DAY, count=3, rows_each=100)
    rows.extend(_off_session_row(index, staleness=25_817.0) for index in range(40))
    _write(lake, "chains", "SPY", saturday, _shift(rows, saturday))
    _history(lake)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW + timedelta(days=4), guards=GuardConstants())
    partition = f"chains/ticker=SPY/date={saturday.isoformat()}.parquet"
    band = _answer(report, CHECK_ROW_COUNT_BAND, partition)

    assert not CALENDAR.is_session(saturday)
    assert band.verdict == OUT_OF_SCOPE
    assert "calls the day no session" in band.reason
    assert partition not in report.appended
    assert _answer(report, CHECK_ENTITLEMENT, partition).judged, "the flag half still runs"


def test_the_window_counts_sessions_rather_than_answers(lake: Path):
    """``docs/design.md``: "All sessions enter the window, and the median's robustness is the
    outlier defense."

    The first shipped reading skipped a session that carried no median and reached further back
    for a replacement, which has no recency bound at all. This is the case that separates them:
    twenty sessions of history, all but two of them dark. Counting answers reaches past the
    darkness and judges against months-old sessions. Counting sessions leaves the window mostly
    empty and says the history is thin.
    """
    calendar = _long_calendar()
    sessions = [day for day in _sessions_before(calendar, DAY, 24)]
    for day in sessions[2:]:
        _write(lake, "chains", "SPY", day, _shift([_gap_row()], day))
    for day in sessions[:2]:
        _write(lake, "chains", "SPY", day, _snapshots(DAY, count=3, rows_each=100, day=day))
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake, start=datetime(2026, 6, 1, 13, 30, tzinfo=UTC))

    trailing = trailing_medians(
        lake,
        _partition(lake),
        calendar=calendar,
        spans=(_span(start=datetime(2026, 6, 1, 13, 30, tzinfo=UTC)),),
        guards=GuardConstants(),
    )

    assert trailing == (100.0, 100.0), "eighteen dark sessions fill the rest of the window"
    finding = judge_row_count(_partition(lake), (100, 100, 100), trailing, GuardConstants())
    assert finding.verdict == INSUFFICIENT_HISTORY, "thin history fails open, not closed"


def test_the_window_is_bounded_by_trailing_median_sessions(lake: Path):
    """The mutation the review found surviving: replacing the window with 99,999 left the
    whole suite green, because no fixture had more than twenty prior sessions."""
    calendar = _long_calendar()
    sessions = _sessions_before(calendar, DAY, 24)
    for index, day in enumerate(sessions):
        # The sessions inside a twenty-session window hold 100 rows a snapshot. The ones past
        # it hold 10, so a window that does not stop lets them move the median.
        _write(
            lake,
            "chains",
            "SPY",
            day,
            _snapshots(DAY, count=3, rows_each=100 if index < 20 else 10, day=day),
        )
    _seed_spans(lake, start=datetime(2026, 6, 1, 13, 30, tzinfo=UTC))

    trailing = trailing_medians(
        lake,
        _partition(lake),
        calendar=calendar,
        spans=(_span(start=datetime(2026, 6, 1, 13, 30, tzinfo=UTC)),),
        guards=GuardConstants(),
    )

    assert len(trailing) == GuardConstants().trailing_median_sessions == 20
    assert set(trailing) == {100.0}, "the four older sessions are outside the window"
    assert (
        trailing_medians(
            lake,
            _partition(lake),
            calendar=calendar,
            spans=(_span(start=datetime(2026, 6, 1, 13, 30, tzinfo=UTC)),),
            guards=GuardConstants(trailing_median_sessions=24),
        )
        != trailing
    ), "and the window is the config's, not a constant here"


def test_a_quarantined_trailing_session_still_enters_the_median(lake: Path):
    """The claim the review found held by nothing. Excluding a withheld trailing session would
    make the band depend on the ledger this same run is writing, and ``docs/design.md`` says
    all sessions enter the window with the median's robustness as the defense."""
    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake)
    for day in COVERED_SESSIONS[:-1]:
        append_quarantine(
            lake,
            build_entry(
                partition=f"chains/ticker=SPY/date={day.isoformat()}.parquet",
                verdict=QUARANTINED_VERDICT,
                check="strike_grid_completeness",
                observed_at=NOW - timedelta(days=30),
            ),
        )

    trailing = trailing_medians(
        lake,
        _partition(lake),
        calendar=CALENDAR,
        spans=(_span(),),
        guards=GuardConstants(),
    )

    assert trailing == (100.0,) * 6, "every one of them is withheld, and every one counts"


def test_a_retyped_column_reports_the_partition_unreadable_rather_than_escaping(lake: Path):
    """An Arrow kernel has no ``less_equal`` for a string bid against a double mark, and the
    raise sat outside the walk's containment. One drifted column on one partition cost every
    partition's verdict for the whole lake that night, on exactly the night that mattered:
    retyping a pinned column is the drift the battery exists to notice.
    """
    rows = _clean_rows("chains")
    table = (
        _table("chains", rows)
        .drop_columns(["bid"])
        .append_column("bid", pa.array(["1.0"] * len(rows), pa.string()))
    )
    path = lake / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pa_pq.write_table(table, path)
    _write(lake, "quotes", "SPY", DAY, _clean_rows("quotes"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.unreadable == 1
    assert any("ArrowNotImplementedError" in line for line in report.report)
    assert _answer(report, CHECK_QUOTE_SANITY, "quotes/ticker=SPY/date=2026-09-16.parquet").judged


@pytest.mark.parametrize("column", ["bid", "is_delayed"])
def test_every_kernel_the_checks_run_is_contained_the_same_way(lake: Path, column: str):
    """The class, not the instance. The entitlement flag's comparison had the identical hole
    and predates these checks, so containing one and not the other leaves the same night's
    verdicts resting on which column the vendor happened to retype."""
    rows = _clean_rows("chains")
    table = (
        _table("chains", rows)
        .drop_columns([column])
        .append_column(column, pa.array(["x"] * len(rows), pa.string()))
    )
    path = lake / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pa_pq.write_table(table, path)
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.unreadable == 1
    assert report.appended == ()


def test_the_coverage_census_is_the_last_report_line(lake: Path):
    """``sweep.digest_body`` truncates the tail at 1000 bytes, and the comment this line's
    reasoning is borrowed from assumed "what falls off the end first is the battery's own
    census". Put in front, the census would be the last thing to fall off and the lines it
    pushed past the cap would be the actionable ones.
    """
    partition = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
    append_quarantine(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW - timedelta(days=1),
        ),
    )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert len(report.report) > 1, "there is something for the census to sit behind"
    assert "calendar coverage" in report.report[-1]
    assert not any("calendar coverage" in line for line in report.report[:-1])


def test_a_configured_floor_of_zero_does_not_crash_the_median(lake: Path):
    """``GuardConstants`` validates no range, and a median of nothing has no midpoint."""
    finding = judge_row_count(_partition(lake), (100,), (), GuardConstants(min_trailing_sessions=0))

    assert finding.verdict == INSUFFICIENT_HISTORY
    assert finding.computed == 0.0


# -- what the mutation lens found unheld ------------------------------------
#
# Each of these was a mutation the suite did not notice. They are grouped because they share a
# cause: the band's tests all measured a count *below* the median, the ordering's tests never
# put a mark on a boundary, and the rename test happened to pick a new spelling that sorts
# after the old one.


def test_a_snapshot_above_the_band_quarantines_too(lake: Path):
    """Every other band test measures a count below the median, so dropping the upper half of
    the comparison changed nothing the suite could see. A doubled chain is as much a fault as a
    halved one: a vendor returning two expirations where it returned one is not a session the
    lake should read as ordinary."""
    _history(lake)
    rows = _snapshots(DAY, count=2, rows_each=100) + _snapshots(
        DAY, count=1, rows_each=200, first=2
    )
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.verdict == QUARANTINED_VERDICT
    assert finding.computed == 200.0
    assert finding.against == 100.0


@pytest.mark.parametrize(
    ("rows_each", "verdict"),
    [
        (70, CLEAN_VERDICT),
        (69, QUARANTINED_VERDICT),
        (130, CLEAN_VERDICT),
        (131, QUARANTINED_VERDICT),
    ],
)
def test_the_band_includes_its_own_edges(lake: Path, rows_each: int, verdict: str):
    """Thirty percent either side of a median of 100 is 70 to 130, and both ends are inside.
    The tolerance has had an edge test since it shipped; the band had none, so an inclusive
    boundary and an exclusive one were the same suite."""
    _history(lake)
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=rows_each))
    _seed_spans(lake)

    assert (
        _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED).verdict
        == verdict
    )


def test_the_quarantine_names_the_snapshot_furthest_from_the_median(lake: Path):
    """``computed`` is what an operator reads when deciding whether to sign off, so naming the
    nearest of several out-of-band snapshots would understate the fault."""
    _history(lake)
    rows = _snapshots(DAY, count=1, rows_each=100)
    rows += _snapshots(DAY, count=1, rows_each=60, first=1)
    rows += _snapshots(DAY, count=1, rows_each=30, first=2)
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.computed == 30.0
    assert "2 of 3 session snapshots" in finding.reason


def test_a_pass_reports_the_judged_sessions_own_median(lake: Path):
    """The clean branch's ``computed`` is the comparison that passed, not the extreme of it.

    The three snapshots differ on purpose. Written at one size the median, the maximum and the
    minimum are the same number, and the assertion passes under any of them, which is how the
    first version of this test let a mutation to the maximum through.
    """
    _history(lake)
    rows = _snapshots(DAY, count=1, rows_each=80)
    rows += _snapshots(DAY, count=1, rows_each=75, first=1)
    rows += _snapshots(DAY, count=1, rows_each=100, first=2)
    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    finding = _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_ROW_COUNT_BAND, JUDGED)

    assert finding.verdict == CLEAN_VERDICT
    assert finding.computed == 80.0, "the median of 75, 80 and 100, not the 100 or the 75"
    assert finding.against == 100.0


def test_no_session_snapshot_is_out_of_scope_and_never_a_pass(lake: Path):
    """``judge`` answers this at the partition level before the band runs, so this drives the
    judgment directly. It matters because a clean verdict here fails open: a partition already
    withheld under this check would be released on the strength of zero snapshots."""
    finding = judge_row_count(_partition(lake), (), (100.0,) * 5, GuardConstants())

    assert finding.verdict == OUT_OF_SCOPE
    assert not finding.judged


@pytest.mark.parametrize("mark", [1.0, 1.05])
def test_a_mark_on_either_boundary_is_ordered(lake: Path, mark: float):
    """The design writes it ``bid <= mid <= ask``, and both ends are inside. A mark sitting
    exactly at the bid is an ordinary quote on an illiquid contract, so narrowing either
    comparison would quarantine a healthy feed the moment more than five percent of its rows
    quoted there."""
    rows = _ordered_rows("chains", count=10)
    for row in rows:
        row["mark"] = mark

    _write(lake, "chains", "SPY", DAY, rows)
    _seed_spans(lake)

    evidence = read_quote_order(_partition(lake))

    assert evidence.unordered == 0
    assert _answer(judge(lake, calendar=CALENDAR, now=NOW), CHECK_QUOTE_SANITY).verdict == (
        CLEAN_VERDICT
    )


def test_a_non_session_day_takes_no_slot_in_the_trailing_window(lake: Path):
    """The window counts sessions, and a day the calendar calls closed is not one. Letting it
    in would contribute a median taken over every row it holds, with no session filter, which
    is the asymmetry the band's own non-session rule exists to close."""
    _history(lake)
    saturday = date(2026, 9, 12)
    _write(lake, "chains", "SPY", saturday, _snapshots(DAY, count=3, rows_each=10, day=saturday))
    _write(lake, "chains", "SPY", DAY, _snapshots(DAY, count=3, rows_each=100))
    _seed_spans(lake)

    trailing = trailing_medians(
        lake, _partition(lake), calendar=CALENDAR, spans=(_span(),), guards=GuardConstants()
    )

    assert not CALENDAR.is_session(saturday)
    assert trailing == (100.0,) * 6, "the Saturday's ten-row snapshots are not in it"


def test_a_rename_is_found_under_the_spelling_the_lake_wrote(lake: Path):
    """The earlier rename test picked a new spelling that sorts after the old one, so looking
    only at the alphabetically first name happened to find the partitions. This renames the
    other way, so the first spelling sorted is the one nothing is written under."""
    from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path

    _seed_spans(lake)
    _cover_all(lake)
    master = SecurityMaster.read(master_path(lake))
    master.remap(1, ID_TYPE_TICKER, "AAA", date(2026, 9, 16))
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")

    found = _coverage(lake)

    assert found.missing == (), "every partition is still under ticker=SPY, which sorts second"
    assert found.owed == 14


def test_a_missing_session_is_named_under_the_spelling_valid_that_day(lake: Path):
    """An operator reads the finding and goes looking for the path it names. After a rename the
    sessions on either side belong to different directories, and naming both under one spelling
    sends half of them to a path the lake would never have written."""
    from lake.security_master import ID_TYPE_TICKER, SecurityMaster, master_path

    _seed_spans(lake)
    _cover_all(lake, days=[day for day in COVERED_SESSIONS if day not in (date(2026, 9, 2), DAY)])
    master = SecurityMaster.read(master_path(lake))
    master.remap(1, ID_TYPE_TICKER, "SPYX", date(2026, 9, 14))
    pa_pq.write_table(master.to_table(), lake / "reference" / "security_master.parquet")

    named = {
        (finding.surface, finding.day): finding.partition for finding in _coverage(lake).missing
    }

    assert named["chains", date(2026, 9, 2)] == "chains/ticker=SPY/date=2026-09-02.parquet"
    assert named["chains", DAY] == "chains/ticker=SPYX/date=2026-09-16.parquet"
    assert named["quotes", DAY] == "quotes/ticker=SPYX/date=2026-09-16.parquet"


def test_a_check_deferred_to_a_human_is_not_reported_as_a_pass(lake: Path):
    """The line says which checks passed, and a deferral is not a pass: the run re-observed the
    partition and applied nothing. Listing it claims a verdict the run never wrote."""
    partition = f"chains/ticker=SPY/date={DAY.isoformat()}.parquet"
    append_quarantine(
        lake,
        build_entry(
            partition=partition,
            verdict=CLEAN_VERDICT,
            check=CHECK_ENTITLEMENT,
            observed_at=NOW - timedelta(days=1),
            provenance=PROVENANCE_HUMAN,
        ),
    )
    append_quarantine(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check="strike_grid_completeness",
            observed_at=NOW - timedelta(days=1),
        ),
    )
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.deferred == 1
    (line,) = [ln for ln in report.report if "stays quarantined under" in ln]
    assert CHECK_ENTITLEMENT not in line, "the entitlement check deferred rather than passing"
    assert CHECK_QUOTE_SANITY in line


def test_the_line_names_a_check_that_quarantined_after_another_passed(lake: Path):
    """``PartitionOutcome.holders`` is the walk's end state and ``Decision.holders`` is the
    state as each finding landed. Nothing told them apart until this. The last decision here is
    the quarantining one, whose own holders are empty because it withholds, so a line sourced
    from it would say nothing at all about a partition that no longer reads."""
    _write(lake, "chains", "SPY", DAY, _ordered_rows("chains", count=100, crossed=100))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.withheld == 1
    (line,) = [ln for ln in report.report if "stays quarantined under" in ln]
    assert CHECK_ENTITLEMENT in line, "it passed"
    assert f"'{CHECK_QUOTE_SANITY}'" in line, "and this one withholds it, decided after"


def test_a_transition_is_measured_on_readability_rather_than_on_the_spelling(lake: Path):
    """``decide_partition``'s docstring: "an entry whose spelling drifted while its effect did
    not is still the same news". ``manifest.is_quarantined`` withholds on any verdict that is
    not ``clean``, so an entry spelled ``held`` already withholds, and a fresh ``quarantined``
    finding changes nothing. Comparing the two spellings instead appends a line every night for
    ever, which is what append-on-transition exists to prevent.
    """
    from lake.battery import _transition

    held = {"verdict": "held", "check": CHECK_ENTITLEMENT}
    fails = _entitlement_finding_for(QUARANTINED_VERDICT)
    passes = _entitlement_finding_for(CLEAN_VERDICT)

    assert _transition(held, fails) is False, "already withheld, so this is the same news"
    assert _transition(held, passes) is True, "and this is the news that it now reads"


def _entitlement_finding_for(verdict: str) -> Finding:
    return Finding(
        partition=JUDGED,
        surface="chains",
        ticker="SPY",
        day=DAY,
        check=CHECK_ENTITLEMENT,
        verdict=verdict,
        reason="",
    )
