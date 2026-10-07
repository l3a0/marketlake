"""The battery's nightly schema-drift page, marketlake #427.

Every partition here is built at ``journal.schema_for(surface)`` rather than through
``tests.support.lake.sample_chains_table``. That builder's ``FIXTURE_CHAINS_SCHEMA`` is a
deliberately reduced 24 columns against the pinned 73, and ``journal.routed_columns`` raises
``KeyError: 'Field "put_call" does not exist in schema'`` against it, because it asks every
column in ``extra_paths`` for its null count. A test written against the narrow table would
prove nothing about the code under it.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import battery_drift, journal
from lake import schema_versions as sv
from lake.battery import SEALED_SURFACES, SealedPartition
from lake.report import ACTION, HEALTHY, INFO
from tests.support.memory import measured
from tests.support.report_kinds import kind_of

DAY = date(2026, 9, 16)
BEFORE = date(2026, 9, 15)

# How far below the whole-table read the streamed drift read's peak has to sit. On pyarrow
# 25.0.1 the gate-open fixture of 4,000 rows streamed at 787,584 bytes against 3,129,664 read
# whole, a ratio of 0.25. The read this replaced, the whole table plus its filtered copy,
# peaked at 4,036,544, and a stream that ignored the batch size at 4,406,528.
STREAMED_MARGIN = 0.5


def _row(surface: str, index: int, day: date, *, kind: str = journal.ROW_KIND_DATA) -> dict:
    """One row at the surface's full pinned schema, with a handful of columns filled."""
    row = {name: None for name in journal.schema_for(surface).names}
    row.update(
        {
            "snap_ts": f"{day.isoformat()}T{13 + index:02d}:30:00+00:00",
            "ticker": "SPY",
            "row_kind": kind,
            "schema_version": 1,
        }
    )
    if kind == journal.ROW_KIND_DATA:
        row.update({"bid": 1.0, "ask": 1.1})
        if surface == "chains":
            row.update({"occ_symbol": f"SPY {index}", "open_interest": 10, "volume": 5})
        else:
            row.update({"volatility": 0.2, "high_52": 9.0})
    return row


def _table(surface: str, day: date, *, count: int = 3, drop=(), route=(), kind=None) -> pa.Table:
    """A partition's rows, with columns dropped or routed into the overflow.

    ``drop`` nulls a column on every row, which is a field that stopped arriving. ``route``
    nulls it and parks its vendor name in ``extra``, which is the retype signature
    ``journal._routed_column`` writes and ``journal.routed_columns`` reads back.
    """
    schema = journal.schema_for(surface)
    paths = journal.extra_paths(surface)
    rows = []
    for index in range(count):
        row = _row(surface, index, day, kind=kind or journal.ROW_KIND_DATA)
        for column in drop:
            row[column] = None
        if route:
            overflow: dict[str, object] = {}
            for column in route:
                row[column] = None
                path = paths[column]
                if path.block is None:
                    overflow[path.field] = "raw"
                else:
                    overflow.setdefault(path.block, {})[path.field] = "raw"
            row[journal.EXTRA_COLUMN] = json.dumps(overflow, sort_keys=True)
        rows.append(row)
    return pa.table({name: [row[name] for row in rows] for name in schema.names}, schema=schema)


def _seal(root: Path, surface: str, ticker: str, day: date, table: pa.Table) -> SealedPartition:
    path = root / surface / f"ticker={ticker}" / f"date={day.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return SealedPartition(path=path, surface=surface, ticker=ticker, day=day)


def _ledger(root: Path, *, drop=()) -> None:
    """A version-1 ledger carrying the running shape, minus ``drop`` on chains."""
    chains = dict(journal.schema_fingerprint("chains"))
    for column in drop:
        chains.pop(column, None)
    ledger = sv.SchemaVersionLedger(
        [
            sv.RecordedVersion(
                version=1,
                recorded_at=datetime(2026, 9, 13, tzinfo=UTC),
                fingerprints={"chains": chains, "quotes": journal.schema_fingerprint("quotes")},
            )
        ]
    )
    path = sv.ledger_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    ledger.write(path)


def _judge(root: Path, today, before, *, baseline=BEFORE) -> battery_drift.DriftReport:
    by_surface: dict[str, list] = {}
    for partition in before:
        by_surface.setdefault(partition.surface, []).append(partition)
    return battery_drift.judge_day(
        root,
        today,
        day=DAY,
        baseline=baseline if before else None,
        baseline_partitions=by_surface or None,
        surfaces=SEALED_SURFACES,
    )


def _kinds(report: battery_drift.DriftReport) -> set[tuple[str, str]]:
    return {(finding.kind, field) for finding in report.findings for field in finding.fields}


# -- the retype half ---------------------------------------------------------


def test_a_retype_that_starts_today_pages(tmp_path: Path):
    """The transition, which is what the design's once-on-the-transition rule asks for."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",)))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert report.findings[0].title == "Schema drift: open_interest retyped"
    # A finding wants a human, and so does the summary that counts it, or the summary would
    # tell a reader the opposite of the line above it (marketlake #530).
    assert kind_of(report, report.findings[0].line) == ACTION
    assert kind_of(report, "1 drifted") == ACTION


def test_a_retype_already_running_yesterday_does_not_page(tmp_path: Path):
    """Once on the transition. A drift that persists costs one page, not one a night."""
    _ledger(tmp_path)
    routed = {"route": ("open_interest",)}
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, **routed))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, **routed))]

    assert _judge(tmp_path, today, before).findings == ()


def test_one_ticker_retyping_is_enough(tmp_path: Path):
    """``retyped`` folds as a union. The other ticker's silence does not contradict it."""
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", t, BEFORE, _table("chains", BEFORE)) for t in ("QQQ", "SPY")
    ]
    today = [
        _seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY)),
        _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",))),
    ]

    assert _kinds(_judge(tmp_path, today, before)) == {(battery_drift.RETYPED, "open_interest")}


# -- the missing half --------------------------------------------------------


def test_a_field_that_stops_arriving_pages(tmp_path: Path):
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, drop=("volume",)))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.MISSING, "volume")}
    assert report.findings[0].title == "Schema drift: volume missing"


def test_a_field_that_never_arrived_does_not_page(tmp_path: Path):
    """The lake's own live case. ``volatility``, ``high_52`` and ``low_52`` are null on every
    data row of every sealed quotes partition and always have been, so an absolute null test
    would page three fields on the battery's first night."""
    _ledger(tmp_path)
    always = {"drop": ("low_52",)}
    before = [_seal(tmp_path, "quotes", "SPY", BEFORE, _table("quotes", BEFORE, **always))]
    today = [_seal(tmp_path, "quotes", "SPY", DAY, _table("quotes", DAY, **always))]

    assert _judge(tmp_path, today, before).findings == ()


def test_a_field_still_arriving_on_one_ticker_does_not_page(tmp_path: Path):
    """``absent`` folds as an intersection. A vendor still sending the field on one ticker is
    a vendor still sending the field, and a union would page for a short payload instead."""
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", t, BEFORE, _table("chains", BEFORE)) for t in ("QQQ", "SPY")
    ]
    today = [
        _seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY)),
        _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, drop=("volume",))),
    ]

    assert _judge(tmp_path, today, before).findings == ()


# -- where the two halves meet ------------------------------------------------


def test_a_retype_on_every_row_pages_once_and_as_a_retype(tmp_path: Path):
    """The overlap, and the ordinary shape of a vendor retype.

    A retype that reaches every row nulls the column on all of them, which is exactly what
    the missing half looks for. Without the subtraction this sends two pages and the second
    says a field went missing while the vendor is still sending it.
    """
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",)))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert [finding.kind for finding in report.findings] == [battery_drift.RETYPED]


# -- scope -------------------------------------------------------------------


def test_a_partition_of_gap_rows_is_not_every_field_missing(tmp_path: Path):
    """Sixteen of the lake's sealed partitions hold no data row. ``journal.gap_batch`` nulls
    every vendor column by construction, so a day of them reads as a wholesale disappearance
    unless the check skips it, which is ``battery._judge_partition``'s own rule."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    gap = _table("chains", DAY, kind=journal.ROW_KIND_GAP)
    today = [_seal(tmp_path, "chains", "SPY", DAY, gap)]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert not any("judged chains" in line for line in report.report)


def test_no_baseline_day_reports_and_does_not_page(tmp_path: Path):
    """With no baseline there is no way to separate a field that stopped arriving from one
    that never arrived, which is the false-positive class #265 measured."""
    _ledger(tmp_path)
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, drop=("volume",)))]

    report = _judge(tmp_path, today, [], baseline=None)

    assert report.findings == ()
    assert any("insufficient_history" in line for line in report.report)
    assert kind_of(report, "no earlier sealed day to compare") == INFO


# -- the rotation guard ------------------------------------------------------


def test_a_column_the_running_version_never_carried_does_not_page(tmp_path: Path):
    """A column this project's own release rotation dropped is nulled at the merge, and after
    the seal nothing tells that from a vendor drop. That is compaction's fact."""
    _ledger(tmp_path, drop=("volume",))
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, drop=("volume",)))]

    assert _judge(tmp_path, today, before).findings == ()


def test_an_unreadable_ledger_refuses_the_missing_half_and_not_the_retype_half(tmp_path: Path):
    """The ledger cannot speak, so a rotation cannot be told from a vendor drop. A routed
    value in the overflow is positive evidence that needs no ledger at all."""
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [
        _seal(
            tmp_path,
            "chains",
            "SPY",
            DAY,
            _table("chains", DAY, drop=("volume",), route=("open_interest",)),
        )
    ]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert any("insufficient_history" in line for line in report.report)
    assert kind_of(report, "insufficient_history") == INFO


def test_a_day_spanning_two_versions_refuses_the_missing_half(tmp_path: Path):
    """More than one ``schema_version`` on a day means the day spans a rotation, which is
    compaction's fact and not this check's."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    table = _table("chains", DAY, drop=("volume",)).to_pydict()
    table["schema_version"] = [1, 2, 1]
    today = [
        _seal(
            tmp_path,
            "chains",
            "SPY",
            DAY,
            pa.table(table, schema=journal.schema_for("chains")),
        )
    ]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("span schema_versions 1, 2" in line for line in report.report)
    assert kind_of(report, "span schema_versions 1, 2") == INFO


# -- the page ----------------------------------------------------------------


def test_the_body_of_a_whole_surface_drift_fits_the_design_cap(tmp_path: Path):
    """``docs/design.md`` pins every body at plain text under 1,000 bytes. Uncapped, a body
    naming every quotes field runs to 1,048, so the widest drift is the one that would not
    reach the phone."""
    fields = tuple(sorted(journal.extra_paths("quotes")))
    finding = battery_drift.DriftFinding(
        surface="quotes",
        day=DAY,
        kind=battery_drift.MISSING,
        fields=fields,
        first_cycle="2026-09-16T09:30:00-04:00",
    )

    body = battery_drift.body(finding)

    assert len(body.encode()) < 1000
    assert f"and {len(fields) - battery_drift.PAGE_FIELD_CAP} more" in body
    assert str(len(fields)) in body


def test_many_fields_are_one_page_and_not_one_each(tmp_path: Path):
    """A page per field spends 56 or 63 of ``alert.DEFAULT_DAILY_CAP``'s forty on one fact,
    and what it swallows could be the auth-death page."""
    _ledger(tmp_path)
    moved = tuple(sorted(journal.extra_paths("chains"))[:20])
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=moved))]

    report = _judge(tmp_path, today, before)

    assert len(report.findings) == 1
    assert report.findings[0].fields == moved
    assert report.findings[0].title == "Schema drift: 20 chains fields retyped"


def test_the_first_cycle_reaches_the_body_in_eastern_time(tmp_path: Path):
    """A sealed ``snap_ts`` is a UTC string and ``docs/design.md`` says a body carries "what
    is lost, since when in ET"."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",)))]

    body = battery_drift.body(_judge(tmp_path, today, before).findings[0])

    assert "09:30:00-04:00" in body
    assert "+00:00" not in body


def test_the_page_is_sent_and_reaches_stderr(tmp_path: Path, capsys):
    """The finding reaches stderr as well as the phone, which is what all three other
    schema-drift producers already do."""

    class _Transport:
        def __init__(self):
            self.sent = []

        def send(self, message):
            self.sent.append(message)

    from lake.alert import Publisher

    transport = _Transport()
    publisher = Publisher(lake_root=tmp_path, transport=transport)
    finding = battery_drift.DriftFinding(
        surface="chains",
        day=DAY,
        kind=battery_drift.RETYPED,
        fields=("open_interest",),
        first_cycle="2026-09-16T09:30:00-04:00",
    )

    titles = battery_drift.page(publisher, [finding], now=datetime(2026, 9, 16, 22, 30, tzinfo=UTC))

    assert titles == ("Schema drift: open_interest retyped",)
    assert transport.sent[0].event == "battery_schema_drift"
    assert transport.sent[0].title == "Schema drift: open_interest retyped"
    assert "open_interest" in capsys.readouterr().err


def test_no_publisher_sends_nothing(tmp_path: Path):
    """``battery.main`` calls ``judge_from_config`` with no publisher, so a hand run walking
    every sealed partition pages nothing at all."""
    finding = battery_drift.DriftFinding(
        surface="chains", day=DAY, kind=battery_drift.MISSING, fields=("volume",), first_cycle=None
    )

    assert battery_drift.page(None, [finding], now=datetime.now(UTC)) == ()


def test_a_clean_night_says_it_ran(tmp_path: Path):
    """``coverage_line``'s rule on a second check. The correct answer against a healthy lake
    is that nothing drifted, and silence cannot be told from a check that did not run."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY))]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("nothing drifted" in line for line in report.report)
    # The check saying it ran, on the night it found nothing (marketlake #530).
    assert kind_of(report, "nothing drifted") == HEALTHY


# -- reading ------------------------------------------------------------------


def test_an_unreadable_partition_does_not_cost_the_night(tmp_path: Path):
    """``battery.trailing_medians``'s containment, for its stated reason: a session the run
    was not asked about is not this check's to announce."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    today = [
        _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",))),
        _seal(tmp_path, "quotes", "SPY", DAY, _table("quotes", DAY)),
    ]
    today[1].path.write_bytes(b"not parquet")

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert any("skipped a partition" in line for line in report.report)
    assert kind_of(report, "skipped a partition") == ACTION


def test_an_unreadable_baseline_partition_is_reported_as_wanting_a_human(tmp_path: Path):
    """The baseline's own skip line, which no other test drives (marketlake #530)."""
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE)),
        _seal(tmp_path, "chains", "QQQ", BEFORE, _table("chains", BEFORE)),
    ]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY))]
    before[1].path.write_bytes(b"not parquet")

    report = _judge(tmp_path, today, before)

    assert kind_of(report, "skipped a baseline partition") == ACTION
    assert kind_of(report, "nothing drifted") == HEALTHY


def test_a_column_with_no_footer_statistics_is_read_rather_than_assumed(tmp_path: Path):
    """A statistic Parquet did not write must never read as a field the vendor stopped
    sending. Measured, every sealed partition carries statistics today, so this is the
    guard and not the ordinary path."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_table("chains", DAY), path, write_statistics=False)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("nothing drifted" in line for line in report.report)


@pytest.mark.parametrize("surface", SEALED_SURFACES)
def test_every_sealed_surface_reads(tmp_path: Path, surface: str):
    """``extra_paths`` covers a third pinned surface this check does not walk, and #500 owes
    the decision. What this holds is that both surfaces it does walk answer."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, surface, "SPY", BEFORE, _table(surface, BEFORE))]
    today = [_seal(tmp_path, surface, "SPY", DAY, _table(surface, DAY))]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any(f"judged {surface}" in line for line in report.report)


def test_a_partition_short_of_the_running_schema_is_reported_and_not_guessed_at(tmp_path: Path):
    """The two ways a column can be missing from a footer are not the same question.

    A column the partition carries whose footer wrote no statistic is read. A column the
    partition does not carry at all is compaction's fact, per the design's schema policy, and
    counting it as wholly null would page a rotation as a vendor drop. ``routed_columns``
    raises on such a partition rather than answering, because it asks every column in
    ``extra_paths`` for its null count.

    The live case is the component suite's own narrow tables, which is how this was found.
    """
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    narrow = _table("chains", DAY).select(["snap_ts", "ticker", "row_kind", "schema_version"])
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(narrow, path)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    report = _judge(tmp_path, today, before)

    assert report.findings == (), "a rotation is not fifty-six vendor fields disappearing"
    assert any("nothing drifted" in line for line in report.report)


def test_a_partition_short_of_extra_is_not_asked_the_retype_question(tmp_path: Path):
    """``routed_columns`` reads the overflow, so a partition without one cannot answer. It is
    left to the version guard the same way an absent vendor column is."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    full = _table("chains", DAY)
    without = full.select([name for name in full.column_names if name != journal.EXTRA_COLUMN])
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(without, path)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()


def _batches_read(monkeypatch) -> list[tuple[date, bool]]:
    """Record every batch the drift read streams, as its day and whether it was the first
    cycle's pass rather than the summary's."""
    seen: list[tuple[date, bool]] = []
    real = battery_drift._batches

    def counting(source, partition, required, *args, **kwargs):
        for batch in real(source, partition, required, *args, **kwargs):
            seen.append((partition.day, "schema_version" not in required))
            yield batch

    monkeypatch.setattr(battery_drift, "_batches", counting)
    return seen


def _late_retype(root: Path) -> SealedPartition:
    """Today's SPY partition, eight data rows whose last four route ``open_interest``."""
    late = _table("chains", DAY, count=8).to_pydict()
    paths = journal.extra_paths("chains")
    late["open_interest"] = [10, 10, 10, 10, None, None, None, None]
    late[journal.EXTRA_COLUMN] = [None] * 4 + [
        json.dumps({paths["open_interest"].field: "raw"})
    ] * 4
    path = root / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(late, schema=journal.schema_for("chains")), path, row_group_size=2)
    return SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)


def test_a_retype_after_the_first_batch_is_seen(tmp_path: Path, monkeypatch):
    """The read is folded over every batch, and a real partition has many.

    The lake's chains partitions carry five and six row groups: SPY 2026-09-16 is 5,307,030
    rows whose first row group ends at 10:49 ET. A read that asked only its first batch
    would read the open and call the rest of the session clean.

    It is the worst shape an alarm can take, because both halves go quiet together. The
    column is non-null on the morning rows, so it never reaches ``absent`` either, and the
    night's report line affirmatively says nothing drifted.

    The batch is shrunk to two rows, because ``iter_batches`` spans row groups: at the
    production size this fixture's four row groups of two decode as one batch of eight, and
    the test would pass while folding nothing.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    seen = _batches_read(monkeypatch)
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, count=8))]
    today = [_late_retype(tmp_path)]

    report = _judge(tmp_path, today, before)

    assert seen.count((DAY, False)) == 4, "the fixture must span several batches"
    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}


def test_the_first_cycle_after_the_first_batch_is_found(tmp_path: Path, monkeypatch):
    """The first-cycle pass streams too, so it owes the same fold over every batch.

    The fixture's first routed row is its fifth, stamped 17:30 UTC, in the third of four
    batches. A pass that stopped after its first batch would print no first cycle at all.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    seen = _batches_read(monkeypatch)
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, count=8))]
    today = [_late_retype(tmp_path)]

    report = _judge(tmp_path, today, before)

    assert seen.count((DAY, True)) == 4, "the first-cycle pass must span several batches"
    assert report.findings[0].first_cycle == "2026-09-16T13:30:00-04:00"


def test_the_first_cycle_is_the_earliest_across_every_retyped_field(tmp_path: Path):
    """One pass asks every field, and the page prints the earliest stamp any of them saw.

    ``open_interest`` routes from the fourth row and ``volume`` from the second, so a pass
    that asked only the first field in sorted order would print the fourth row's stamp.
    """
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, count=6))]
    rows = _table("chains", DAY, count=6).to_pydict()
    paths = journal.extra_paths("chains")
    overflow = []
    for index in range(6):
        held = {}
        if index >= 3:
            rows["open_interest"][index] = None
            held[paths["open_interest"].field] = "raw"
        if index >= 1:
            rows["volume"][index] = None
            held[paths["volume"].field] = "raw"
        overflow.append(json.dumps(held) if held else None)
    rows[journal.EXTRA_COLUMN] = overflow
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(rows, schema=journal.schema_for("chains")), path)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    report = _judge(tmp_path, today, before)

    assert report.findings[0].fields == ("open_interest", "volume")
    assert report.findings[0].first_cycle == "2026-09-16T10:30:00-04:00"


def test_an_open_gate_holds_one_batch_at_a_time_rather_than_the_whole_day(
    tmp_path: Path, monkeypatch
):
    """The gate opens the night ``extra`` carries a value, and on SPY the old whole-day read
    was killed for memory on the 2 GiB host (marketlake #671).

    Every row carries an unrecognized vendor field, which opens the gate and routes nothing,
    so ``routed_columns`` walks every row's overflow. The reference is the whole-table read
    the drift check made before it streamed, taken without threads so nothing frees through
    its pool late.
    """
    rows = _table("chains", DAY, count=4_000).to_pydict()
    rows[journal.EXTRA_COLUMN] = [json.dumps({"newGreek": 0.5})] * 4_000
    partition = _seal(
        tmp_path, "chains", "SPY", DAY, pa.table(rows, schema=journal.schema_for("chains"))
    )
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 256)

    with measured() as pool:
        pq.read_table(partition.path, use_threads=False)
    whole = pool.max_memory()
    with measured() as pool:
        day = battery_drift.read_surface_day([partition], "chains", DAY)
    streamed = pool.max_memory()

    assert day.unreadable == ()
    assert day.tickers == ("SPY",)
    assert streamed < STREAMED_MARGIN * whole, f"{streamed:,} bytes against {whole:,} whole"


def _cut_streams(monkeypatch, rows: int, cut) -> None:
    """Hand every stream over a file of ``rows`` rows through ``cut``, and others unchanged."""
    real = pq.ParquetFile.iter_batches

    def iter_batches(self, *args, **kwargs):
        batches = real(self, *args, **kwargs)
        if self.metadata.num_rows != rows:
            return batches
        return cut(batches)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", iter_batches)


def _early_retype(root: Path) -> SealedPartition:
    """Today's SPY partition, eight data rows whose first six route ``open_interest``."""
    rows = _table("chains", DAY, count=8, route=("open_interest",)).to_pydict()
    rows[journal.EXTRA_COLUMN] = rows[journal.EXTRA_COLUMN][:6] + [None, None]
    rows["open_interest"] = [None] * 6 + [10, 10]
    return _seal(root, "chains", "SPY", DAY, pa.table(rows, schema=journal.schema_for("chains")))


def test_a_stream_cut_short_is_unreadable_and_leaves_nothing_in_the_fold(
    tmp_path: Path, monkeypatch
):
    """``iter_batches`` can end short without raising where ``pq.read_table`` raises.

    Answered silently, the day reads as one with no retype after the cut. So every decoded
    row is counted against the footer's, and a partition that comes up short is refused. Its
    first three batches routed ``open_interest`` before the cut, and a fold that took them
    batch by batch would page a retype off a partition the night reports it could not read.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _cut_streams(monkeypatch, 8, lambda batches: iter(list(batches)[:-1]))
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", t, BEFORE, _table("chains", BEFORE)) for t in ("QQQ", "SPY")
    ]
    today = [_seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY)), _early_retype(tmp_path)]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("did not read: decoded 6 of 8 rows" in line for line in report.report)
    assert kind_of(report, "nothing drifted") == HEALTHY


def test_a_batch_that_fails_to_decode_did_not_read(tmp_path: Path, monkeypatch):
    """A decode failure is "did not read", and never an overflow that does not decode.

    ``pa.ArrowInvalid`` subclasses ``ValueError``, which is what the handler for
    ``journal.routed_columns``'s bare ``json.loads`` catches. A fetch inside that handler
    would send a reader looking for bad JSON in a file whose page would not decompress.
    """
    assert issubclass(pa.ArrowInvalid, ValueError)

    def torn(batches):
        yield next(batches)
        raise pa.ArrowInvalid("a page did not decompress")

    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _cut_streams(monkeypatch, 8, torn)
    today = [_early_retype(tmp_path)]

    day = battery_drift.read_surface_day(today, "chains", DAY)

    assert len(day.unreadable) == 1
    assert "did not read: a page did not decompress" in day.unreadable[0]
    assert "does not decode" not in day.unreadable[0]
    assert day.retyped == frozenset()


@pytest.mark.parametrize(
    ("column", "retyped"),
    [
        ("snap_ts", pa.float64()),
        ("schema_version", pa.string()),
        ("snap_ts", pa.list_(pa.string())),
        ("schema_version", pa.list_(pa.int64())),
    ],
)
def test_a_gap_partition_with_a_retyped_summary_column_is_skipped(
    tmp_path: Path, column: str, retyped: pa.DataType
):
    """A batch the data-row filter empties goes no further, so a gap day is skipped.

    Arrow picks a kernel by type even for an empty array, and ``min`` and ``unique`` have no
    kernel for a list. Asked of a gap-only batch, they would turn a partition that holds no
    data row into one reported unreadable, which is the class marketlake PR #713's review
    found in the entitlement read.
    """
    gap = _table("chains", DAY, kind=journal.ROW_KIND_GAP)
    if pa.types.is_list(retyped):
        values = pa.array([None] * gap.num_rows, type=retyped)
    else:
        values = pa.array([str(v) if column == "schema_version" else 1.5 for v in gap[column]])
    index = gap.schema.get_field_index(column)
    gap = gap.set_column(index, pa.field(column, retyped), values.cast(retyped))
    partition = _seal(tmp_path, "chains", "SPY", DAY, gap)

    day = battery_drift.read_surface_day([partition], "chains", DAY)

    assert day.unreadable == ()
    assert day.tickers == ()


def test_a_summary_column_missing_from_an_open_gate_did_not_read(tmp_path: Path):
    """``iter_batches`` reads past a column the file does not carry and raises nothing, so
    the stream is refused before it starts rather than left to answer without it."""
    full = _table("chains", DAY, route=("open_interest",))
    short = full.select([name for name in full.column_names if name != "schema_version"])
    partition = _seal(tmp_path, "chains", "SPY", DAY, short)

    day = battery_drift.read_surface_day([partition], "chains", DAY)

    assert day.unreadable == (f"{partition.relative} did not read: missing schema_version",)
    assert day.retyped == frozenset()


def test_a_vendor_column_missing_from_an_open_gate_lacks_the_running_schema(tmp_path: Path):
    """A vendor column the file lacks is left out of the projection rather than refused, so
    ``routed_columns`` raises the ``KeyError`` that names the partition's real shape."""
    full = _table("chains", DAY, route=("open_interest",))
    short = full.select([name for name in full.column_names if name != "volume"])
    partition = _seal(tmp_path, "chains", "SPY", DAY, short)

    day = battery_drift.read_surface_day([partition], "chains", DAY)

    assert len(day.unreadable) == 1
    assert "does not carry the running schema" in day.unreadable[0]
    assert "volume" in day.unreadable[0]


def test_an_overflow_that_does_not_decode_costs_its_surface_and_not_the_night(tmp_path: Path):
    """``journal.routed_columns`` decodes a populated overflow with a bare ``json.loads``,
    so one unparseable cell raises out of it. Unconverted it escapes ``judge_day`` past the
    per-surface containment, and one bad cell on chains takes quotes down with it."""
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE)),
        _seal(tmp_path, "quotes", "SPY", BEFORE, _table("quotes", BEFORE)),
    ]
    torn = _table("chains", DAY).to_pydict()
    torn[journal.EXTRA_COLUMN] = ["{not json"] * 3
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(torn, schema=journal.schema_for("chains")), path)
    today = [
        SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY),
        _seal(tmp_path, "quotes", "SPY", DAY, _table("quotes", DAY)),
    ]

    report = _judge(tmp_path, today, before)

    assert any("does not decode" in line for line in report.report)
    assert any("judged quotes" in line for line in report.report), "quotes still answered"


def test_a_ticker_going_quiet_does_not_page_the_other_tickers_standing_absence(tmp_path: Path):
    """The two days are compared over the tickers they share.

    ``high_52`` is absent on SPY on both days and nothing about it changed. QQQ carried it
    yesterday and is all-gap today, which is an ordinary dead-daemon day. Folded to one set
    per day and subtracted, QQQ keeps ``high_52`` out of yesterday's intersection and is
    not there to keep it out of today's, so a field nothing touched pages as newly missing.
    """
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "quotes", "SPY", BEFORE, _table("quotes", BEFORE, drop=("high_52",))),
        _seal(tmp_path, "quotes", "QQQ", BEFORE, _table("quotes", BEFORE)),
    ]
    today = [
        _seal(tmp_path, "quotes", "SPY", DAY, _table("quotes", DAY, drop=("high_52",))),
        _seal(tmp_path, "quotes", "QQQ", DAY, _table("quotes", DAY, kind=journal.ROW_KIND_GAP)),
    ]

    assert _judge(tmp_path, today, before).findings == ()


def test_the_measured_body_sizes_are_the_ones_the_module_states(tmp_path: Path):
    """The cap's arithmetic is load-bearing, so the numbers behind it are held rather than
    quoted from a estimate made before the body existed."""
    sizes = {}
    for surface in SEALED_SURFACES:
        fields = tuple(sorted(journal.extra_paths(surface)))
        finding = battery_drift.DriftFinding(
            surface=surface,
            day=DAY,
            kind=battery_drift.MISSING,
            fields=fields,
            first_cycle="2026-09-16T09:30:00-04:00",
        )
        sizes[surface] = len(battery_drift.body(finding).encode())

    assert all(size < 1000 for size in sizes.values()), sizes
    assert sizes == {"chains": 305, "quotes": 295}


def test_one_unreadable_ticker_does_not_silence_the_others_drift(tmp_path: Path):
    """Containment is per partition, not per surface.

    Contained per surface, one corrupt file on one ticker takes the whole surface's alarm
    down, so a genuine vendor drop on every other ticker goes unpaged on the night a file
    was also corrupt. On the 115-ticker roster the design sizes for, that is one file
    silencing 114.
    """
    _ledger(tmp_path)
    before = [
        _seal(tmp_path, "chains", t, BEFORE, _table("chains", BEFORE)) for t in ("QQQ", "SPY")
    ]
    today = [
        _seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY)),
        _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",))),
    ]
    today[0].path.write_bytes(b"not parquet")

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert any("skipped a partition" in line for line in report.report)


def test_a_column_the_baseline_never_carried_does_not_page(tmp_path: Path):
    """A version that adds a vendor column the vendor has not begun filling.

    The baseline partition lacks the column outright, so it never enters that day's
    ``absent`` set and reads as a field that was arriving. Today's partition carries it and
    the vendor sends nothing, so it is null on every data row. Subtracted naively that is a
    page saying a field stopped arriving on the first night it existed, which is the #265
    false-positive class reached from the other direction. The rotation guard cannot see it,
    because it asks only what today's version dropped.
    """
    _ledger(tmp_path)
    full = _table("chains", BEFORE)
    without = full.select([n for n in full.column_names if n != "volume"])
    path = tmp_path / "chains" / "ticker=SPY" / f"date={BEFORE.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(without, path)
    before = [SealedPartition(path=path, surface="chains", ticker="SPY", day=BEFORE)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, drop=("volume",)))]

    assert _judge(tmp_path, today, before).findings == ()


def test_a_refused_page_does_not_undo_its_own_redaction(tmp_path: Path, capsys):
    """The publisher found one of its own secrets in the body and redacted its record for
    that reason, so stderr must not print the body anyway.

    Every sibling producer pins this: ``test_schema_drift``, ``test_compaction_schema`` and
    ``page_delayed_feed``'s own test in ``test_battery``. This producer copied the code.
    """
    from lake.alert import Publisher

    class _Transport:
        def __init__(self):
            self.sent = []

        def send(self, message):
            self.sent.append(message)

    secret = "ntfy-topic-abcdef"
    transport = _Transport()
    publisher = Publisher(lake_root=tmp_path, transport=transport, secrets=(secret,))
    finding = battery_drift.DriftFinding(
        surface="chains",
        day=DAY,
        kind=battery_drift.RETYPED,
        fields=(secret,),
        first_cycle=None,
    )

    titles = battery_drift.page(publisher, [finding], now=datetime(2026, 9, 16, 22, 30, tzinfo=UTC))

    assert transport.sent == [], "a body carrying a secret never reaches the transport"
    assert titles == (finding.title,)
    err = capsys.readouterr().err
    assert "refused" in err
    assert secret not in err, "stderr must not undo the publisher's redaction"


def test_a_version_the_ledger_records_no_shape_for_refuses_the_missing_half(tmp_path: Path):
    """The shape PR #493 shipped a page for: the ledger reads, and holds no entry for the
    running version. Without a recorded shape there is nothing to ask ``has_column``, so a
    rotation cannot be told from a vendor drop."""
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE))]
    later = _table("chains", DAY, drop=("volume",)).to_pydict()
    later["schema_version"] = [7, 7, 7]
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(later, schema=journal.schema_for("chains")), path)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("records no shape for schema_version 7" in line for line in report.report)
    assert kind_of(report, "records no shape for schema_version 7") == INFO


def test_an_all_gap_baseline_day_refuses_the_comparison(tmp_path: Path):
    """The lake holds 16 partitions of exactly this shape, 2026-09-08 to 09-11.

    A baseline whose every row is a gap carries a null on every vendor column by
    construction, so without the guard every field on the surface reads as newly missing at
    once.
    """
    _ledger(tmp_path)
    before = [
        _seal(
            tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, kind=journal.ROW_KIND_GAP)
        )
    ]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY))]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("carried no data row" in line for line in report.report)
    assert kind_of(report, "carried no data row") == INFO
