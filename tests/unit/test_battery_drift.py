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
    rows = []
    for index in range(count):
        row = _row(surface, index, day, kind=kind or journal.ROW_KIND_DATA)
        for column in drop:
            row[column] = None
        if route:
            _route(surface, row, route)
        rows.append(row)
    return _rows_at_schema(surface, rows)


def _route(surface: str, row: dict, route) -> None:
    """Null each column in ``route`` on ``row`` and park its vendor name in ``extra``."""
    paths = journal.extra_paths(surface)
    overflow: dict[str, object] = {}
    for column in route:
        row[column] = None
        path = paths[column]
        if path.block is None:
            overflow[path.field] = "raw"
        else:
            overflow.setdefault(path.block, {})[path.field] = "raw"
    row[journal.EXTRA_COLUMN] = json.dumps(overflow, sort_keys=True)


def _rows_at_schema(surface: str, rows: list[dict]) -> pa.Table:
    schema = journal.schema_for(surface)
    return pa.table({name: [row[name] for row in rows] for name in schema.names}, schema=schema)


def _chain(index: int, *, kind: str = journal.ROW_KIND_DATA, route=(), **columns) -> dict:
    """One of today's chains rows, stamped ``index`` hours after 13:30 UTC, which is 09:30
    ET. ``route`` routes columns as :func:`_table` does, and ``columns`` sets any outright."""
    row = _row("chains", index, DAY, kind=kind)
    if route:
        _route("chains", row, route)
    row.update(columns)
    return row


def _chains_day(*rows: dict) -> pa.Table:
    """Today's chains rows in the order given, which is the order the stream decodes them."""
    return _rows_at_schema("chains", list(rows))


def _baseline(root: Path, *tickers: str) -> list[SealedPartition]:
    """A healthy chains day before ``DAY`` for each ticker."""
    return [_seal(root, "chains", t, BEFORE, _table("chains", BEFORE)) for t in tickers]


def _seal(
    root: Path, surface: str, ticker: str, day: date, table: pa.Table, **write
) -> SealedPartition:
    path = root / surface / f"ticker={ticker}" / f"date={day.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, **write)
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


def test_a_second_version_in_a_later_batch_refuses_the_missing_half(tmp_path: Path, monkeypatch):
    """The version set is folded over every batch, as the retypes are.

    A rotation lands mid-session, so the second version arrives in a later batch than the
    first. A set taken from the first batch alone reads one version, and the missing half
    pages a column the rotation dropped as a vendor drop.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    rows = [_chain(i, volume=None, schema_version=1 if i < 2 else 2) for i in range(4)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

    assert report.findings == ()
    assert any("span schema_versions 1, 2" in line for line in report.report)


def test_a_null_schema_version_is_not_a_second_version(tmp_path: Path):
    """A row with no ``schema_version`` says nothing about a rotation.

    Counted as a version of its own, one null cell makes a single-version day look like it
    spans two, and the missing half stands down on a night a field really stopped arriving.
    """
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    rows = [_chain(0, volume=None), _chain(1, volume=None, schema_version=None)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.MISSING, "volume")}


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


def test_a_missing_page_carries_the_earliest_data_stamp_across_batches_and_tickers(
    tmp_path: Path, monkeypatch
):
    """The missing half's first cycle is the day's earliest data stamp, folded over every
    batch of every ticker.

    SPY opens on gap rows, whose stamps do not count. QQQ holds the day's earliest row, in
    its second batch behind later ones, and it is neither the first ticker read nor the
    last. A fold that kept the first batch, the first ticker or the last ticker, or kept the
    latest stamp, prints a later time than the 09:30 the page owes. A partition whose
    evidence dropped its stamp prints no time at all.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY", "QQQ", "IWM")
    gap = journal.ROW_KIND_GAP
    stopped = {"volume": None}
    days = {
        "SPY": [
            _chain(0, kind=gap),
            _chain(1, kind=gap),
            _chain(2, **stopped),
            _chain(3, **stopped),
        ],
        "QQQ": [_chain(i, **stopped) for i in (3, 4, 0, 1)],
        "IWM": [_chain(i, **stopped) for i in (1, 2)],
    }
    today = [_seal(tmp_path, "chains", t, DAY, _chains_day(*rows)) for t, rows in days.items()]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.MISSING, "volume")}
    assert report.findings[0].first_cycle == "2026-09-16T09:30:00-04:00"


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


def test_a_file_that_did_not_open_is_told_from_one_with_no_footer(tmp_path: Path):
    """The skip line names which of two failures it was, because they send a reader to
    different places.

    A file the operating system would not open is a path or permission question. A file that
    opened and carries no Parquet footer is damage to the file itself. The two are told
    apart by whether the open raised ``OSError``.
    """
    gone = SealedPartition(
        path=tmp_path / "chains" / "ticker=SPY" / f"date={DAY.isoformat()}.parquet",
        surface="chains",
        ticker="SPY",
        day=DAY,
    )
    garbage = _seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY))
    garbage.path.write_bytes(b"not parquet")

    day = battery_drift.read_surface_day([gone, garbage], "chains", DAY)

    assert day.unreadable[0].startswith(f"{gone.relative} did not open: ")
    assert day.unreadable[1].startswith(f"{garbage.relative} has no readable footer: ")


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


def test_a_field_missing_from_a_partition_with_no_statistics_still_pages(tmp_path: Path):
    """The other side of the read above. The read has to answer, not just keep quiet.

    A partition written without statistics leaves every vendor column unmeasured. Left
    unresolved, an unmeasured column is never counted as absent, so a vendor drop on a file
    like this one would go unpaged.
    """
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    stopped = _table("chains", DAY, drop=("volume",))
    today = [_seal(tmp_path, "chains", "SPY", DAY, stopped, write_statistics=False)]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.MISSING, "volume")}


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

    SPY 2026-09-16 is 5,307,030 rows, which is 81 batches at the production size. A read
    that asked only its first batch would call the rest of the session clean.

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


def test_a_first_batch_of_gap_rows_does_not_end_the_read(tmp_path: Path, monkeypatch):
    """A batch the data-row filter empties is passed over, and the stream goes on.

    A daemon down at the open seals a partition whose first batch is all gap rows. If that
    empty batch ended the read, a retype later that day would go unpaged, and the row-count
    check after the loop would never run.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    gap = journal.ROW_KIND_GAP
    retyped = ("open_interest",)
    rows = [_chain(0, kind=gap), _chain(1, kind=gap)]
    rows += [_chain(i, route=retyped) for i in (2, 3)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

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


@pytest.mark.parametrize(("open_interest_from", "volume_from"), [(3, 1), (1, 3)])
def test_the_first_cycle_is_the_earliest_across_every_retyped_field(
    tmp_path: Path, open_interest_from: int, volume_from: int
):
    """One pass asks every field, and the page prints the earliest stamp any of them saw.

    One field routes from the second row and the other from the fourth, each way round. A
    pass that asked only the first field in sorted order, or only the last, prints the
    fourth row's stamp on one of the two cases.
    """
    _ledger(tmp_path)
    before = [_seal(tmp_path, "chains", "SPY", BEFORE, _table("chains", BEFORE, count=6))]
    rows = _table("chains", DAY, count=6).to_pydict()
    paths = journal.extra_paths("chains")
    overflow = []
    for index in range(6):
        held = {}
        if index >= open_interest_from:
            rows["open_interest"][index] = None
            held[paths["open_interest"].field] = "raw"
        if index >= volume_from:
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


def test_the_retype_first_cycle_is_the_earliest_across_tickers_past_an_unreadable_one(
    tmp_path: Path,
):
    """The retype page's stamp is folded over every ticker, and an unreadable one is passed.

    QQQ is read first and is not Parquet, so it costs only its own stamp. SPY, IWM and DIA
    start routing at different rows, and the earliest of them, IWM's 09:30, is neither the
    first readable ticker nor the last. XLF routes nothing and has no stamp to offer. A fold
    that stopped at the unreadable ticker, kept the first or last stamp, or kept the latest,
    prints the wrong time or none, and one that compared XLF's missing stamp would raise.
    """
    _ledger(tmp_path)
    starts = {"SPY": 2, "IWM": 0, "DIA": 1, "XLF": 4}
    before = _baseline(tmp_path, "QQQ", *starts)
    today = [_seal(tmp_path, "chains", "QQQ", DAY, _table("chains", DAY))]
    today[0].path.write_bytes(b"not parquet")
    for ticker, start in starts.items():
        rows = [_chain(i, route=("open_interest",) if i >= start else ()) for i in range(4)]
        today.append(_seal(tmp_path, "chains", ticker, DAY, _chains_day(*rows)))

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert report.findings[0].first_cycle == "2026-09-16T09:30:00-04:00"


def test_a_gap_row_carrying_an_overflow_is_not_the_first_cycle(tmp_path: Path):
    """Only a data row can mark the first cycle.

    A gap row's overflow is null by construction, so one carrying a routed key is not a
    cycle the vendor sent. Asked as one, it would print the gap's 09:30 rather than the
    10:30 of the first data row that routed.
    """
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    retyped = ("open_interest",)
    gap = _chain(0, kind=journal.ROW_KIND_GAP)
    _route("chains", gap, retyped)
    rows = [gap, _chain(1, route=retyped), _chain(2, route=retyped)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

    assert report.findings[0].first_cycle == "2026-09-16T10:30:00-04:00"


def test_a_data_row_without_a_stamp_does_not_cost_the_surface(tmp_path: Path, monkeypatch):
    """A null ``snap_ts`` on a data row is passed over by both reads rather than compared.

    The second batch here holds only stampless rows, so its earliest stamp is null. Compared
    against a string it raises ``TypeError``, which no handler expects, and the night loses
    the surface rather than one cell.
    """
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    retyped = ("open_interest",)
    rows = [_chain(i, route=retyped) for i in (0, 1)]
    rows += [_chain(i, route=retyped, snap_ts=None) for i in (2, 3)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert report.findings[0].first_cycle == "2026-09-16T09:30:00-04:00"


@pytest.mark.parametrize("shape", ["torn json", "array overflow", "retyped row_kind"])
def test_a_bad_partition_elsewhere_does_not_cost_the_first_cycle(tmp_path: Path, shape: str):
    """The first-cycle pass reads every partition again, and a bad one costs only its stamp.

    QQQ is read first and is broken in one of three ways: an overflow that is not JSON, an
    overflow that decodes to a list rather than an object, and a ``row_kind`` retyped so the
    data-row filter has no kernel for it. Each has to be passed over, or the night loses the
    stamp SPY's retype carries, or the page itself.
    """
    _ledger(tmp_path)
    before = _baseline(tmp_path, "QQQ", "SPY")
    bad = _table("chains", DAY)
    if shape == "retyped row_kind":
        index = bad.schema.get_field_index("row_kind")
        bad = bad.set_column(index, pa.field("row_kind", pa.int64()), pa.array([1, 1, 1]))
    else:
        rows = bad.to_pydict()
        rows[journal.EXTRA_COLUMN] = ["{not json" if shape == "torn json" else "[1]"] * 3
        bad = pa.table(rows, schema=journal.schema_for("chains"))
    today = [
        _seal(tmp_path, "chains", "QQQ", DAY, bad),
        _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",))),
    ]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert report.findings[0].first_cycle == "2026-09-16T09:30:00-04:00"


@pytest.mark.parametrize("value", [0, "", False])
def test_a_falsy_routed_value_still_marks_the_first_cycle(tmp_path: Path, value):
    """A routed key marks the cycle whatever its value, because the key is the evidence.

    A vendor that retypes a count to a string can send ``""``, and one that sends zero
    still sent the field. Read as absent, every row is passed over and the page prints no
    first cycle.
    """
    _ledger(tmp_path)
    before = _baseline(tmp_path, "SPY")
    field = journal.extra_paths("chains")["open_interest"].field
    rows = [_chain(i, open_interest=None, extra=json.dumps({field: value})) for i in range(3)]
    today = [_seal(tmp_path, "chains", "SPY", DAY, _chains_day(*rows))]

    report = _judge(tmp_path, today, before)

    assert _kinds(report) == {(battery_drift.RETYPED, "open_interest")}
    assert report.findings[0].first_cycle == "2026-09-16T09:30:00-04:00"


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


def test_the_first_cycle_pass_holds_one_batch_at_a_time_rather_than_the_whole_day(
    tmp_path: Path, monkeypatch
):
    """The retype night is the night the page is for, and its first-cycle pass reads every
    partition again. Every row of this fixture routes ``open_interest``, which is a vendor
    retype's shape, so every data row's overflow is populated and decodable.

    The reference is the three columns the pass reads, decoded whole, which is what it read
    before it streamed. A pass that gathered every batch before asking them would hold the
    whole day again and fail here, where a test counting batches would still pass.
    """
    partition = _seal(
        tmp_path, "chains", "SPY", DAY, _table("chains", DAY, count=4_000, route=("open_interest",))
    )
    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 256)
    columns = [journal.EXTRA_COLUMN, "snap_ts", journal.ROW_KIND_COLUMN]

    with measured() as pool:
        table = pq.read_table(partition.path, columns=columns, use_threads=False)
    whole = pool.max_memory()
    earliest = min(table.column("snap_ts").to_pylist())
    del table
    with measured() as pool:
        stamp = battery_drift.first_cycle_of(partition, "chains", ["open_interest"])
    streamed = pool.max_memory()

    assert stamp == earliest
    assert streamed < STREAMED_MARGIN * whole, f"{streamed:,} bytes against {whole:,} whole"


def test_the_drift_read_neither_pre_buffers_nor_threads(tmp_path: Path, monkeypatch):
    """Neither flag changes an answer or moves a small fixture's peak, so the test records the
    arguments themselves, as ``tests/component/test_battery.py`` does for the entitlement read
    whose settings these are.
    """
    seen: list[tuple[str, dict]] = []
    real = pq.ParquetFile

    class Recording(real):
        def __init__(self, *args, **kwargs):
            seen.append(("open", kwargs))
            super().__init__(*args, **kwargs)

        def iter_batches(self, *args, **kwargs):
            seen.append(("iterate", kwargs))
            return super().iter_batches(*args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", Recording)
    partition = _seal(
        tmp_path, "chains", "SPY", DAY, _table("chains", DAY, route=("open_interest",))
    )

    battery_drift.read_surface_day([partition], "chains", DAY)
    battery_drift.first_cycle_of(partition, "chains", ["open_interest"])

    opens = [kwargs for kind, kwargs in seen if kind == "open"]
    iterations = [kwargs for kind, kwargs in seen if kind == "iterate"]
    assert len(opens) == 2 and len(iterations) == 2, "both passes must be recorded"
    assert all(kwargs.get("pre_buffer") is False for kwargs in opens)
    assert all(kwargs.get("use_threads") is False for kwargs in iterations)


def test_a_shut_gate_streams_three_columns(tmp_path: Path, monkeypatch):
    """The healthy night decodes three columns rather than every vendor column.

    An all-null overflow is every cycle the lake has recorded, so the shut gate is the
    ordinary night's path. Opened on a partition that routes nothing, the read gives the
    same answers and decodes sixty columns on chains, so only the projection shows it.
    """
    seen: list[tuple[str, ...]] = []
    real = pq.ParquetFile.iter_batches

    def recording(self, *args, columns=None, **kwargs):
        seen.append(tuple(columns))
        return real(self, *args, columns=columns, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", recording)
    partition = _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY))

    battery_drift.read_surface_day([partition], "chains", DAY)

    assert seen == [("snap_ts", "row_kind", "schema_version")]


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


def test_a_stream_that_runs_past_the_footer_did_not_read(tmp_path: Path, monkeypatch):
    """The decoded count has to equal the footer's, not merely reach it.

    A stream that hands back more rows than the file holds has repeated or invented some,
    so its answer is no more trustworthy than a short one. Here the first batch arrives a
    second time.
    """

    def repeated(batches):
        decoded = list(batches)
        return iter([*decoded, decoded[0]])

    monkeypatch.setattr(battery_drift, "_READ_BATCH_ROWS", 2)
    _cut_streams(monkeypatch, 8, repeated)
    partition = _seal(tmp_path, "chains", "SPY", DAY, _table("chains", DAY, count=8))

    day = battery_drift.read_surface_day([partition], "chains", DAY)

    assert day.unreadable == (f"{partition.relative} did not read: decoded 10 of 8 rows",)


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


def test_a_retyped_summary_column_on_data_rows_did_not_read(tmp_path: Path):
    """On data rows the kernels do run, and one that refuses a retyped column costs the
    partition as "did not read".

    A retyped pinned column is drift, which is what this check exists to notice. Raised out
    of the kernel unconverted, it escapes the partition's containment and costs the surface.
    """
    table = _table("chains", DAY)
    listed = pa.list_(pa.int64())
    index = table.schema.get_field_index("schema_version")
    table = table.set_column(
        index, pa.field("schema_version", listed), pa.array([[1]] * 3, type=listed)
    )
    partition = _seal(tmp_path, "chains", "SPY", DAY, table)

    day = battery_drift.read_surface_day([partition], "chains", DAY)

    assert len(day.unreadable) == 1
    assert day.unreadable[0].startswith(f"{partition.relative} did not read: ")


def test_a_partition_of_no_rows_short_of_a_summary_column_is_skipped(tmp_path: Path):
    """A partition the footer says holds no row is skipped before the stream is asked for
    anything, as a gap day is.

    Streamed anyway, its missing ``schema_version`` would refuse it, and a partition with
    nothing in it to judge would be reported unreadable.
    """
    empty = _table("chains", DAY, count=0).drop(["schema_version"])
    partition = _seal(tmp_path, "chains", "SPY", DAY, empty)

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
