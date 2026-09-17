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

DAY = date(2026, 9, 16)
BEFORE = date(2026, 9, 15)


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


def test_a_retype_after_the_first_batch_is_seen(tmp_path: Path):
    """The read is folded over every batch, and a real partition has many.

    ``pq.read_table`` chunks at 131,072 rows whatever the row-group layout, and the lake's
    chains partitions carry five and six row groups besides: SPY 2026-09-16 is 5,307,030
    rows whose first batch ends at 09:39 ET. Asking only ``to_batches()[0]`` therefore reads
    the open and calls the rest of the session clean.

    It is the worst shape an alarm can take, because both halves go quiet together. The
    column is non-null on the morning rows, so it never reaches ``absent`` either, and the
    night's report line affirmatively says nothing drifted.
    """
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, count=8))]
    late = _table("chains", DAY, count=8).to_pydict()
    paths = journal.extra_paths("chains")
    late["open_interest"] = [10, 10, 10, 10, None, None, None, None]
    late[journal.EXTRA_COLUMN] = [None] * 4 + [
        json.dumps({paths["open_interest"].field: "raw"})
    ] * 4
    path = tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(late, schema=journal.schema_for("chains")), path, row_group_size=2)
    today = [SealedPartition(path=path, surface="chains", ticker="SPY", day=DAY)]

    assert pq.read_metadata(path).num_row_groups > 1, "the fixture must span several batches"
    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}


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
