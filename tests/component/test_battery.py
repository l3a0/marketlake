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
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest

from lake.battery import (
    CHECK_ENTITLEMENT,
    DELAYED_FEED_EVENT,
    DELAYED_FEED_TITLE,
    INSUFFICIENT_HISTORY,
    OUT_OF_SCOPE,
    PROVENANCE_BATTERY,
    PROVENANCE_HUMAN,
    QUARANTINED_VERDICT,
    BatteryReport,
    Entitlement,
    Finding,
    PartitionUnreadable,
    SealedPartition,
    append_verdict,
    build_entry,
    capture_spans_by_ticker,
    entry_line_count,
    human_precedence,
    in_scope,
    judge,
    judge_entitlement,
    page_delayed_feed,
    read_entitlement,
    sealed_partitions,
)
from lake.capture_spans import CaptureSpan
from lake.config import GuardConstants
from lake.manifest import (
    CLEAN_VERDICT,
    is_quarantined,
    latest_quarantine,
    read_quarantine,
    scrub,
)
from tests.support.calendar import weekday_sessions

# The weeks these tests judge in. A regular session opens 09:30 and closes 16:00 Eastern, so
# the option close lands at 16:15 and every row ``_row`` builds falls inside it.
CALENDAR = weekday_sessions(
    date(2026, 8, 17), date(2026, 8, 24), date(2026, 8, 31), date(2026, 9, 14)
)

NOW = datetime(2026, 9, 16, 22, 30, tzinfo=UTC)
DAY = date(2026, 9, 16)
SPAN_START = datetime(2026, 9, 1, 13, 30, tzinfo=UTC)

CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
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
    """Marketlake #139's rule, built here because #139 ships after this.

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


# -- append on transition ----------------------------------------------------


def test_a_clean_verdict_for_an_unjudged_partition_writes_nothing(lake: Path):
    """A partition with no entry already reads, so the line would change nothing."""
    _write(lake, "chains", "SPY", DAY, _clean_rows("chains"))
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.cleared == 1
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

    assert report.judged == 1
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

    assert report.judged == 1
    assert report.quarantined == 1
    assert report.scope_unknown == 0


def test_a_day_before_the_masters_valid_from_still_resolves_its_clamp(lake: Path):
    """The other direction, which is marketlake #405's failure on the dashboard.

    Resolving as of the judged day loses the clamp on any day before ``valid_from``. The
    partition then has no span, and the out-of-scope rule that should protect it never runs.
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

    assert report.cleared == 1
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
    assert report.cleared == 0
    finding = next(f for f in report.findings if f.verdict == QUARANTINED_VERDICT)
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
    assert judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()).cleared == 1


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

    assert strict.quarantined == 1
    assert loose.cleared == 1


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
    assert judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants()).cleared == 1


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

    assert report.cleared == 2
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
    assert judge(lake, calendar=CALENDAR, now=NOW, day=DAY, guards=GuardConstants()).judged == 1


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

    assert report.cleared == 1
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

    assert report.cleared == 1
    assert report.quarantined == 0
    assert report.findings[0].computed == pytest.approx(-1.7)


def test_a_partition_of_overnight_rows_alone_is_out_of_scope_not_quarantined(lake: Path):
    """A day that captured only an overnight cycle recorded no session, so it judges none.

    Without this the check reads 25,817 seconds of staleness and quarantines a partition whose
    feed was real-time on every row. The lake already holds four gap-only sessions from a
    machine that was down, so this is one landed cycle away from real.
    """
    _write(lake, "chains", "SPY", DAY, [_off_session_row(i, staleness=25_817.0) for i in range(5)])
    _seed_spans(lake)

    report = judge(lake, calendar=CALENDAR, now=NOW, guards=GuardConstants())

    assert report.out_of_scope == 1
    assert report.quarantined == 0
    assert report.appended == ()
    assert "no data row falls inside the session" in report.findings[0].reason


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
