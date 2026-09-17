"""``load_bars`` and the two adjusted views, against a fixture lake on disk.

Slice 4 fetches nothing, so a fixture lake is its whole test surface. The partitions below are
written in ``journal.BARS_SCHEMA``, the shape marketlake #336 pinned and ``lake.bars`` writes,
rather than in a fixture schema of this file's own, because what a bars row carries is exactly
what decides which of ``_load_surface``'s machinery this door can inherit.

The actions half comes from ``actions.append``, the real writer, rather than from a second
spelling of the ledger format here. The order is load-bearing and fails silently in one
direction: ``FixtureLake.build`` rewrites ``manifest.jsonl`` whole from its own list, so an append
that runs first leaves the ledger on disk with no manifest entry. Every helper below builds first
and appends after.

The numbered tests carry the numbering marketlake #368 asks for, so a mutation the issue names
points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import actions, journal
from lake.loader import (
    ADJUST_NONE,
    ADJUST_SPLIT,
    ADJUST_TOTAL,
    AdjustmentIncomplete,
    AdjustUnknown,
    BarsAbsent,
    InstrumentUnknown,
    LoadError,
    PartialRead,
    PartitionQuarantined,
    load_bars,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from tests.support.lake import FixtureLake

TICKER = "SPY"
DAILY = "1d"
MINUTE = "1m"
INSTRUMENT = 41

# When the schema-version ledger recorded the running version. Any instant does, since the
# loader reads the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# Three consecutive sessions. The middle one is where every split below takes its ex-date, so a
# view that adjusts on or after the ex-date rather than strictly before it moves a bar this file
# asserts on.
BEFORE = "2026-09-14"
EX = "2026-09-15"
AFTER = "2026-09-16"


def _bar(
    day: str,
    close: float,
    *,
    freq: str = DAILY,
    volume: int = 100,
    instrument_id: int | None = INSTRUMENT,
    minute: str = "20:00",
    extra: str | None = None,
    schema_version: int | None = None,
) -> dict:
    """One bars row, at the pinned schema's column names."""
    stamp = f"{day}T{minute}:00+00:00"
    return {
        "bar_ts": stamp,
        "fetch_ts": f"{day}T22:00:00+00:00",
        "fetch_end_ts": f"{day}T22:00:01+00:00",
        "ticker": TICKER,
        "instrument_id": instrument_id,
        "freq": freq,
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "volume": volume,
        "window_start": f"{day}T13:30:00+00:00",
        "window_end": f"{day}T20:00:00+00:00",
        "extended_hours": None,
        "schema_version": journal.SCHEMA_VERSION if schema_version is None else schema_version,
        "extra": extra,
    }


def _table(rows: list[dict], schema: pa.Schema | None = None) -> pa.Table:
    schema = schema or journal.BARS_SCHEMA
    return pa.table({f.name: [row.get(f.name) for row in rows] for f in schema}, schema=schema)


def _ledger_table(fingerprints: dict | None = None) -> pa.Table:
    """The schema-version ledger recording the running version at the running shape."""
    entry = RecordedVersion(
        version=journal.SCHEMA_VERSION,
        recorded_at=RECORDED_AT,
        fingerprints=fingerprints or running_fingerprints(),
    )
    return SchemaVersionLedger([entry]).to_table()


def _lake(
    fixture_lake: FixtureLake,
    partitions: dict[tuple[str, str], list[dict]],
    *,
    quarantine: list[dict] | None = None,
    ledger: pa.Table | None = None,
) -> Path:
    """A lake holding one partition per ``(freq, day)`` key, plus the version ledger."""
    for (freq, day), rows in partitions.items():
        fixture_lake.with_bars(TICKER, freq, day, _table(rows))
    recorded = ledger if ledger is not None else _ledger_table()
    fixture_lake.with_reference("schema_versions", recorded)
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _three_daily_sessions(fixture_lake: FixtureLake, **kwargs) -> Path:
    """The default lake: 600 before the split, 300 on the ex-date, 310 after it."""
    return _lake(
        fixture_lake,
        {
            (DAILY, BEFORE): [_bar(BEFORE, 600.0)],
            (DAILY, EX): [_bar(EX, 300.0)],
            (DAILY, AFTER): [_bar(AFTER, 310.0)],
        },
        **kwargs,
    )


def _split(root: Path, *, ex_date: str = EX, ratio: float = 2.0, recorded_at=RECORDED_AT) -> None:
    actions.append(
        root,
        instrument_id=INSTRUMENT,
        observed_on=date.fromisoformat(ex_date),
        recorded_at=recorded_at,
        ex_date=ex_date,
        type=actions.TYPE_SPLIT,
        split_ratio=ratio,
        provenance=actions.PROVENANCE_OBSERVED,
    )


def _dividend(root: Path, *, ex_date: str = AFTER, amount: float = 3.0) -> None:
    actions.append(
        root,
        instrument_id=INSTRUMENT,
        observed_on=date.fromisoformat(ex_date),
        recorded_at=RECORDED_AT,
        ex_date=ex_date,
        type=actions.TYPE_DIVIDEND,
        cash_amount=amount,
        provenance=actions.PROVENANCE_OBSERVED,
    )


def _closes(table: pa.Table) -> list[float]:
    return table.column("close").to_pylist()


def _stamps(table: pa.Table) -> list[str]:
    return table.column("bar_ts").to_pylist()


# -- the range, and what it holds ------------------------------------------------------


def test_a_range_returns_every_days_rows_ordered_by_instant(fixture_lake: FixtureLake):
    """#368 test 1. Every day the range covers, in time order, and nothing else."""
    root = _three_daily_sessions(fixture_lake)
    table = load_bars(TICKER, DAILY, lake_root=root)
    assert _stamps(table) == [
        f"{BEFORE}T20:00:00+00:00",
        f"{EX}T20:00:00+00:00",
        f"{AFTER}T20:00:00+00:00",
    ]
    assert _closes(table) == [600.0, 300.0, 310.0]


def test_the_order_is_the_instant_rather_than_the_stored_text(fixture_lake: FixtureLake):
    """#368 test 1, on the shape that separates the two readings.

    One instant has more than one ISO spelling, and the live lake has already produced two of
    them for one minute. An Eastern-offset stamp sorts before a ``+00:00`` one lexicographically
    while naming the later instant, so a text sort answers these two backwards.
    """
    root = _lake(
        fixture_lake,
        {
            (MINUTE, BEFORE): [
                {**_bar(BEFORE, 601.0, freq=MINUTE), "bar_ts": f"{BEFORE}T16:01:00-04:00"},
                {**_bar(BEFORE, 600.0, freq=MINUTE), "bar_ts": f"{BEFORE}T20:00:00+00:00"},
            ]
        },
    )
    table = load_bars(TICKER, MINUTE, lake_root=root)
    assert _closes(table) == [600.0, 601.0]


def test_a_range_whose_ends_are_one_session_returns_that_session(fixture_lake: FixtureLake):
    """#368 test 2. Both ends inclusive, so one session named twice is one session."""
    root = _three_daily_sessions(fixture_lake)
    table = load_bars(TICKER, DAILY, EX, EX, lake_root=root)
    assert _closes(table) == [300.0]


def test_open_ends_read_every_session_the_frequency_holds(fixture_lake: FixtureLake):
    """#368 test 3. A call naming neither end reads the directory, and only its own frequency."""
    root = _lake(
        fixture_lake,
        {
            (DAILY, BEFORE): [_bar(BEFORE, 600.0)],
            (DAILY, AFTER): [_bar(AFTER, 310.0)],
            (MINUTE, EX): [_bar(EX, 299.0, freq=MINUTE, minute="14:31")],
        },
    )
    assert _closes(load_bars(TICKER, DAILY, lake_root=root)) == [600.0, 310.0]
    assert _closes(load_bars(TICKER, MINUTE, lake_root=root)) == [299.0]


def test_a_day_absent_inside_a_range_is_returned_around(fixture_lake: FixtureLake):
    """#368 test 4, first half. A missed bar is a re-fetch, so a hole is the ordinary state."""
    root = _lake(
        fixture_lake,
        {(DAILY, BEFORE): [_bar(BEFORE, 600.0)], (DAILY, AFTER): [_bar(AFTER, 310.0)]},
    )
    assert _closes(load_bars(TICKER, DAILY, BEFORE, AFTER, lake_root=root)) == [600.0, 310.0]


def test_a_range_holding_no_partition_raises(fixture_lake: FixtureLake):
    """#368 test 4, second half. An empty series and an unfetched one mean opposite things."""
    root = _three_daily_sessions(fixture_lake)
    with pytest.raises(BarsAbsent) as caught:
        load_bars(TICKER, DAILY, "2026-09-17", "2026-09-18", lake_root=root)
    assert "2026-09-17..2026-09-18" in str(caught.value)


def test_a_ticker_the_lake_never_held_raises_rather_than_returning_nothing(
    fixture_lake: FixtureLake,
):
    """#368 test 4, second half, reached the other way."""
    root = _three_daily_sessions(fixture_lake)
    with pytest.raises(BarsAbsent):
        load_bars("QQQ", DAILY, lake_root=root)


# -- the two guards --------------------------------------------------------------------


def test_a_quarantined_day_refuses_the_whole_range(fixture_lake: FixtureLake):
    """#368 test 5, first half. A series with a hole in it would read as a complete one."""
    partition = f"bars/ticker={TICKER}/freq={DAILY}/date={EX}.parquet"
    root = _three_daily_sessions(
        fixture_lake, quarantine=[{"partition": partition, "verdict": "bad_rows"}]
    )
    with pytest.raises(PartitionQuarantined) as caught:
        load_bars(TICKER, DAILY, lake_root=root)
    assert caught.value.partition == partition


def test_include_quarantined_reads_the_flagged_day(fixture_lake: FixtureLake):
    """#368 test 5, second half. The opt-in is explicit and per call."""
    partition = f"bars/ticker={TICKER}/freq={DAILY}/date={EX}.parquet"
    root = _three_daily_sessions(
        fixture_lake, quarantine=[{"partition": partition, "verdict": "bad_rows"}]
    )
    table = load_bars(TICKER, DAILY, lake_root=root, include_quarantined=True)
    assert _closes(table) == [600.0, 300.0, 310.0]


def test_a_frequency_spelled_in_the_wrong_case_refuses(fixture_lake: FixtureLake):
    """#368 test 6. macOS matches a path case-insensitively, and the quarantine key does not.

    The bars path carries a ``freq=`` level the two capture surfaces do not, so it is one more
    component a case-insensitive filesystem would match through. Reading ``freq=1D`` off disk
    would open the ``freq=1d`` partitions while every quarantine lookup keyed on a path no
    verdict was ever written under, which turns the guard from fail closed into fail open.
    """
    partition = f"bars/ticker={TICKER}/freq={DAILY}/date={EX}.parquet"
    root = _three_daily_sessions(
        fixture_lake, quarantine=[{"partition": partition, "verdict": "bad_rows"}]
    )
    with pytest.raises(BarsAbsent):
        load_bars(TICKER, "1D", lake_root=root)
    with pytest.raises(BarsAbsent):
        load_bars("spy", DAILY, lake_root=root)


# -- the split view --------------------------------------------------------------------


def test_split_divides_price_and_multiplies_volume_before_the_ex_date(fixture_lake: FixtureLake):
    """#368 test 7, first half. The ratio is the share count after over the count before.

    A two-for-one split takes a contract from 100 shares to 200, so ``split_ratio`` is 2.0 and
    the stored price halved. The continuity view therefore divides the earlier bar by it and
    multiplies that bar's volume by the same.
    """
    root = _three_daily_sessions(fixture_lake)
    _split(root)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(table) == [300.0, 300.0, 310.0]
    assert table.column("volume").to_pylist() == [200, 100, 100]
    assert table.column("open").to_pylist() == [299.5, 299.0, 309.0]
    assert table.column("high").to_pylist() == [300.5, 301.0, 311.0]
    assert table.column("low").to_pylist() == [299.0, 298.0, 308.0]


def test_split_leaves_the_ex_dates_own_bar_alone(fixture_lake: FixtureLake):
    """#368 test 7, second half. Strictly after, never on or after.

    An ex-date is the first session that trades at the new price, so its own bar is already
    adjusted. A comparison reading ``>=`` would halve the 300 to 150 and halve it again for any
    later split, which is the shape that compounds into nonsense over a long history.
    """
    root = _three_daily_sessions(fixture_lake)
    _split(root)
    table = load_bars(TICKER, DAILY, EX, AFTER, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(table) == [300.0, 310.0]


def test_two_splits_compound(fixture_lake: FixtureLake):
    """#368 test 7. The factor is a product over every split after the bar, not the latest one."""
    root = _three_daily_sessions(fixture_lake)
    _split(root, ex_date=EX, ratio=2.0)
    _split(root, ex_date=AFTER, ratio=5.0)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(table) == [60.0, 60.0, 310.0]


def test_a_null_price_stays_null_under_a_view(fixture_lake: FixtureLake):
    """#368 test 7. Arrow's arithmetic propagates a null, and a bar with no high has no
    adjusted high either."""
    root = _lake(fixture_lake, {(DAILY, BEFORE): [{**_bar(BEFORE, 600.0), "high": None}]})
    _split(root)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert table.column("high").to_pylist() == [None]
    assert _closes(table) == [300.0]


# -- the total view --------------------------------------------------------------------


def test_total_folds_the_dividend_on_top_of_the_split(fixture_lake: FixtureLake):
    """#368 test 8. The factor is ``1 - A / C``, and ``C`` is the close before the ex-date.

    The dividend's ex-date is the third session and pays 3.00. The close before it is the second
    session's 300, so the factor is 0.99 and it applies to both earlier bars. The first of those
    is also split-adjusted, so 600 becomes 300 and then 297.
    """
    root = _three_daily_sessions(fixture_lake)
    _split(root)
    _dividend(root)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [297.0, 297.0, 310.0]
    assert table.column("volume").to_pylist() == [200, 100, 100]


def test_the_dividends_close_comes_from_the_daily_partition_not_the_series_asked_for(
    fixture_lake: FixtureLake,
):
    """#368 test 8, on the reading that separates the two sources.

    A minute series has no close of record in itself, and its last minute bar is not the official
    close. Here the minute bars are deliberately 1000 while the daily close before the ex-date is
    300, so a factor taken from the series asked for would be ``1 - 3/1000`` instead of
    ``1 - 3/300``.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, EX): [_bar(EX, 300.0)],
            (MINUTE, EX): [_bar(EX, 1000.0, freq=MINUTE, minute="14:31")],
        },
    )
    _dividend(root)
    table = load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [pytest.approx(1000.0 * (1 - 3.0 / 300.0))]


def test_total_refuses_when_the_prior_daily_close_is_absent(fixture_lake: FixtureLake):
    """#368 test 9. Dropping the dividend understates every return computed through it."""
    root = _lake(fixture_lake, {(MINUTE, EX): [_bar(EX, 1000.0, freq=MINUTE, minute="14:31")]})
    _dividend(root)
    with pytest.raises(AdjustmentIncomplete) as caught:
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)
    assert DAILY in str(caught.value)


def test_total_refuses_when_the_prior_daily_bar_is_another_instruments(fixture_lake: FixtureLake):
    """#368 test 9, reached the other way. A close belonging to another instrument is not ``C``."""
    root = _lake(
        fixture_lake,
        {
            (DAILY, EX): [_bar(EX, 300.0, instrument_id=INSTRUMENT + 1)],
            (MINUTE, EX): [_bar(EX, 1000.0, freq=MINUTE, minute="14:31")],
        },
    )
    _dividend(root)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)


def test_split_needs_no_prior_close(fixture_lake: FixtureLake):
    """#368 test 9. Only the dividend factor has a denominator, so the continuity view reads
    no second partition and a lake with no daily bars still serves it."""
    root = _lake(fixture_lake, {(MINUTE, BEFORE): [_bar(BEFORE, 600.0, freq=MINUTE)]})
    _split(root)
    assert _closes(load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_SPLIT)) == [300.0]


def test_a_dividend_at_or_above_the_prior_close_refuses(fixture_lake: FixtureLake):
    """#368 test 9. A factor of zero or less is no factor a price can be multiplied by."""
    root = _three_daily_sessions(fixture_lake)
    _dividend(root, amount=300.0)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)


# -- the ledger's two resolutions ------------------------------------------------------


def test_as_of_resolves_the_ledger_point_in_time(fixture_lake: FixtureLake):
    """#368 test 10. Without this a backtest standing in August reads September's correction."""
    root = _three_daily_sessions(fixture_lake)
    _split(root, recorded_at=datetime(2026, 9, 15, 22, 0, tzinfo=UTC))
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)) == [
        300.0,
        300.0,
        310.0,
    ]
    before = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT, as_of="2026-09-14")
    assert _closes(before) == [600.0, 300.0, 310.0]


def test_a_superseding_entry_wins_its_key(fixture_lake: FixtureLake):
    """#368 test 10. Last entry per key in file order, which is the ledger's own rule."""
    root = _three_daily_sessions(fixture_lake)
    _split(root, ratio=2.0)
    _split(root, ratio=4.0)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)) == [
        150.0,
        300.0,
        310.0,
    ]


def test_an_absent_ledger_adjusts_nothing_and_raises_nothing(fixture_lake: FixtureLake):
    """#368 test 10. A lake the extraction has not written to is inert rather than refused."""
    root = _three_daily_sessions(fixture_lake)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)) == [
        600.0,
        300.0,
        310.0,
    ]


# -- the instrument key ----------------------------------------------------------------


def test_a_null_instrument_refuses_an_adjusted_read_and_reads_as_traded(
    fixture_lake: FixtureLake,
):
    """#368 test 11. The null is the join key gone, and as-traded says what it is.

    ``lake.bars`` lands a row with a null id when the security master cannot place its ticker,
    filing a finding and keeping the bar, so this is a partition the lake really produces. An
    adjusted read of it would find no actions and hand back the stored price under an adjusted
    name.
    """
    root = _lake(fixture_lake, {(DAILY, BEFORE): [_bar(BEFORE, 600.0, instrument_id=None)]})
    _split(root)
    with pytest.raises(InstrumentUnknown):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_NONE)) == [600.0]


def test_each_row_is_adjusted_by_its_own_instruments_actions(fixture_lake: FixtureLake):
    """#368 test 12. A symbol handed between two instruments carries each day's own id.

    ``actions.by_ticker`` states the rule and ``lake.bars`` follows it, resolving the id per
    ticker-day rather than once per ticker. So a range spanning a handover carries two ids, and a
    view that resolved the ticker once would adjust both halves by one instrument's actions.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, BEFORE): [_bar(BEFORE, 600.0)],
            (DAILY, EX): [_bar(EX, 300.0, instrument_id=INSTRUMENT + 1)],
        },
    )
    _split(root, ex_date=AFTER, ratio=2.0)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(table) == [300.0, 300.0]


# -- the marker, the stitch, and the argument ------------------------------------------


def test_every_row_names_the_view_that_produced_it(fixture_lake: FixtureLake):
    """#368 test 13. Mixing as-traded and adjusted prices is the classic corruption.

    The marker is a column rather than Arrow schema metadata, and this asserts the reason:
    ``pa.concat_tables`` keeps the first table's metadata without raising, so a stitch of the two
    reads would carry one word over rows half of which were adjusted. A column survives the
    stitch per row.
    """
    root = _three_daily_sessions(fixture_lake)
    _split(root)
    as_traded = load_bars(TICKER, DAILY, lake_root=root)
    adjusted = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
    assert as_traded.column("adjust").to_pylist() == [ADJUST_NONE] * 3
    assert adjusted.column("adjust").to_pylist() == [ADJUST_SPLIT] * 3
    mixed = pa.concat_tables([as_traded, adjusted])
    assert set(mixed.column("adjust").to_pylist()) == {ADJUST_NONE, ADJUST_SPLIT}


def test_two_partitions_whose_columns_differ_stitch_rather_than_raising(
    fixture_lake: FixtureLake,
):
    """#368 test 14. The projection adds a promoted column only where a row carries a value.

    So two partitions can come back with different column sets, and plain ``concat_tables``
    raises on that. This writes the second partition without a column the first one has, which is
    what that difference looks like from the reader's side, and asserts the permissive stitch
    fills it with nulls instead.
    """
    narrow = pa.schema([f for f in journal.BARS_SCHEMA if f.name != "high"])
    fixture_lake.with_bars(TICKER, DAILY, BEFORE, _table([_bar(BEFORE, 600.0)]))
    fixture_lake.with_bars(TICKER, DAILY, EX, _table([_bar(EX, 300.0)], schema=narrow))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    table = load_bars(TICKER, DAILY, lake_root=root)
    assert table.column("high").to_pylist() == [601.0, None]


def test_an_unknown_view_is_refused_at_the_call(fixture_lake: FixtureLake):
    """#368. A view that does not exist is a bad argument rather than a lake that cannot answer."""
    root = _three_daily_sessions(fixture_lake)
    with pytest.raises(AdjustUnknown):
        load_bars(TICKER, DAILY, lake_root=root, adjust="adjusted")
    with pytest.raises(ValueError):
        load_bars(TICKER, DAILY, lake_root=root, adjust="adjusted")


def test_an_unreadable_stamp_refuses_the_series(fixture_lake: FixtureLake):
    """#368 test 1. A series is the one read whose order a caller will assume, so a row this
    cannot place in time takes the answer away rather than sorting anyway."""
    root = _lake(
        fixture_lake,
        {(DAILY, BEFORE): [{**_bar(BEFORE, 600.0), "bar_ts": f"{BEFORE}T20:00:00"}]},
    )
    with pytest.raises(LoadError) as caught:
        load_bars(TICKER, DAILY, lake_root=root)
    assert "bar_ts" in str(caught.value)


def test_a_version_the_ledger_has_no_bars_shape_for_reads_partial(fixture_lake: FixtureLake):
    """#368. The projection's refusal means on a bar exactly what it means on a chain.

    This is also what the live lake will do the first time the sweep lands a partition, because
    the ledger there records one version and covers chains and quotes alone. The cause is
    marketlake #130 and the fix is a ledger run rather than a change to this reader.
    """
    chains_only = {
        surface: dict(columns)
        for surface, columns in running_fingerprints().items()
        if surface != journal.BARS_SURFACE
    }
    root = _three_daily_sessions(fixture_lake, ledger=_ledger_table(chains_only))
    with pytest.raises(PartialRead) as caught:
        load_bars(TICKER, DAILY, lake_root=root)
    assert journal.BARS_SURFACE in str(caught.value)


def test_a_promoted_value_is_read_back_out_of_the_overflow(fixture_lake: FixtureLake):
    """#368. The projection carries over to bars whole, which is what #336's pin bought.

    A version below a promotion carries the measurement in ``extra`` under the vendor's own name,
    and the running schema gives it a column. The projection is what presents the two as one
    shape, and this asserts a bars table goes through it rather than around it.
    """
    without_volume = {
        surface: {name: kind for name, kind in columns.items() if name != "volume"}
        if surface == journal.BARS_SURFACE
        else dict(columns)
        for surface, columns in running_fingerprints().items()
    }
    rows = [{**_bar(BEFORE, 600.0, volume=None), "extra": json.dumps({"volume": 4321})}]
    root = _lake(fixture_lake, {(DAILY, BEFORE): rows}, ledger=_ledger_table(without_volume))
    table = load_bars(TICKER, DAILY, lake_root=root)
    assert table.column("volume").to_pylist() == [4321]


# -- what the adversarial review found -------------------------------------------------


def test_a_split_sharing_the_dividends_ex_date_prices_them_in_one_share_term(
    fixture_lake: FixtureLake,
):
    """The amount is per post-split share and the close is as-traded, so they need one term.

    The split and the dividend share an ex-date, so the last daily close before it, 600, is in
    pre-split shares while the 3.00 is paid on post-split ones. Dividing one by the other mixes
    the two and understates the factor by the whole split ratio, which puts a spurious half a
    percent of return across a day the continuity view exists to make flat.
    """
    root = _lake(
        fixture_lake,
        {(DAILY, BEFORE): [_bar(BEFORE, 600.0)], (DAILY, EX): [_bar(EX, 297.0)]},
    )
    _split(root, ex_date=EX, ratio=2.0)
    _dividend(root, ex_date=EX, amount=3.0)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [pytest.approx(297.0), pytest.approx(297.0)]


def test_a_split_inside_a_hole_before_the_ex_date_is_carried_into_the_close(
    fixture_lake: FixtureLake,
):
    """The same mixing, reached through the surface's ordinary state rather than a shared date.

    A missed bar is a re-fetch, so days absent from the middle are ordinary here. The last daily
    close the lake holds before the ex-date is nine sessions earlier and in pre-split shares, and
    nothing about the arithmetic notices unless the close is carried forward through the split.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, "2026-09-01"): [_bar("2026-09-01", 600.0)],
            (DAILY, "2026-09-10"): [_bar("2026-09-10", 300.0)],
        },
    )
    _split(root, ex_date="2026-09-05", ratio=2.0)
    _dividend(root, ex_date="2026-09-10", amount=3.0)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [pytest.approx(297.0), pytest.approx(300.0)]


def test_an_unrelated_entry_does_not_refuse_the_read(fixture_lake: FixtureLake):
    """The ledger is one file for every instrument, so a read checks what it uses and no more.

    ``actions.build_entry`` accepts a ``cash_amount`` of zero, and a suspended payer still
    reporting a ``div_ex_date`` lands one through the real extraction. Checking the whole ledger
    would let that entry, for an instrument with no bars here, refuse every adjusted read of
    every ticker in the lake.
    """
    root = _three_daily_sessions(fixture_lake)
    _raw_ledger(root, {}, {**_UNREADABLE_DIVIDEND, "instrument_id": INSTRUMENT + 900})
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)) == [
        300.0,
        300.0,
        310.0,
    ]
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)) == [
        300.0,
        300.0,
        310.0,
    ]


def test_a_dividend_that_paid_nothing_adjusts_nothing_and_raises_nothing(
    fixture_lake: FixtureLake,
):
    """A zero amount is a factor of exactly one, not a value no factor can be built from.

    ``actions.build_entry`` accepts a zero ``cash_amount`` and refuses a negative one, and a
    suspended payer still reporting a ``div_ex_date`` lands a zero through the real extraction.
    Refusing it would take the total-return view away over an event that does not move a price.
    """
    root = _three_daily_sessions(fixture_lake)
    _split(root)
    _dividend(root, ex_date=AFTER, amount=0.0)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)) == [
        300.0,
        300.0,
        310.0,
    ]


def test_the_split_view_reads_no_dividend_at_all(fixture_lake: FixtureLake):
    """A view that folds no dividend in checks none, even for the instrument it is adjusting.

    The dividend below is unreadable and belongs to the very instrument being adjusted, so the
    only thing that keeps the split view answering is that it never reads a dividend. The total
    view reads the same file and refuses, which is what says the entry really is unreadable.
    """
    root = _three_daily_sessions(fixture_lake)
    _raw_ledger(root, {}, _UNREADABLE_DIVIDEND)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)) == [
        300.0,
        300.0,
        310.0,
    ]
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)


def test_a_negative_amount_refuses(fixture_lake: FixtureLake):
    """What a hand-edited ledger can hold and the writer cannot: below zero builds no factor."""
    root = _three_daily_sessions(fixture_lake)
    ledger = root / "actions" / "corporate_actions.jsonl"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(
        json.dumps(
            {
                "instrument_id": INSTRUMENT,
                "ex_date": AFTER,
                "type": actions.TYPE_DIVIDEND,
                "cash_amount": -3.0,
                "split_ratio": None,
                "observed_on": AFTER,
                "recorded_at": RECORDED_AT.isoformat(),
                "provenance": actions.PROVENANCE_OBSERVED,
                "schema_version": actions.ACTIONS_SCHEMA_VERSION,
            }
        )
        + chr(10)
    )
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)


def test_a_file_name_that_is_not_a_session_is_passed_over(fixture_lake: FixtureLake):
    """``date.fromisoformat`` accepts more spellings than a partition name ever holds.

    On 3.12 it reads ``20260824`` and ``2026-W35-1`` as dates, which ``paths.parse_date_dir``
    refuses by shape for exactly this reason. A stray file enumerated as a session makes the read
    refuse the whole ticker, naming a date no file on disk holds.
    """
    root = _lake(fixture_lake, {(DAILY, BEFORE): [_bar(BEFORE, 600.0)]})
    directory = root / "bars" / f"ticker={TICKER}" / f"freq={DAILY}"
    (directory / "date=2026-W35-1.parquet").write_bytes(b"")
    (directory / "date=20260824.parquet").write_bytes(b"")
    (directory / "notes.txt").write_bytes(b"")
    assert _closes(load_bars(TICKER, DAILY, lake_root=root)) == [600.0]


def test_a_prior_close_the_master_could_not_place_still_prices_the_dividend(
    fixture_lake: FixtureLake,
):
    """A null id on the reference bar says the master could not answer, not that it answered
    someone else.

    ``lake.bars`` lands such a row on purpose, filing a finding and keeping the bar, so refusing
    on it would take every total-return read of the ticker away over a provenance gap on one
    reference day. A different id is evidence and still refuses, which the test below holds.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, EX): [_bar(EX, 300.0, instrument_id=None)],
            (MINUTE, AFTER): [_bar(AFTER, 310.0, freq=MINUTE, minute="14:31")],
        },
    )
    _dividend(root, ex_date="2026-09-17", amount=3.0)
    table = load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [pytest.approx(310.0 * (1 - 3.0 / 300.0))]


def test_a_prior_partition_without_a_close_column_refuses_as_a_load_error(
    fixture_lake: FixtureLake,
):
    """Every way this door declines is a ``LoadError``, including the one Arrow would raise."""
    narrow = pa.schema([f for f in journal.BARS_SCHEMA if f.name != "close"])
    fixture_lake.with_bars(TICKER, DAILY, EX, _table([_bar(EX, 300.0)], schema=narrow))
    fixture_lake.with_bars(
        TICKER, MINUTE, AFTER, _table([_bar(AFTER, 310.0, freq=MINUTE, minute="14:31")])
    )
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    _dividend(root, ex_date="2026-09-17", amount=3.0)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)


def test_volume_rounds_away_from_zero_rather_than_to_even(fixture_lake: FixtureLake):
    """The mode is chosen. Arrow's default is half to even, and "rounds" reads as away from zero.

    A one-for-two reverse split halves each volume, so every odd one lands on a half. Half to
    even sends 1 and 5 down and 3 and 7 up, which is not what the docstring's word means.
    """
    rows = [
        _bar(BEFORE, 600.0, freq=MINUTE, volume=volume, minute=f"14:3{index}")
        for index, volume in enumerate((1, 3, 5, 7))
    ]
    root = _lake(fixture_lake, {(MINUTE, BEFORE): rows})
    _split(root, ex_date=EX, ratio=0.5)
    table = load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_SPLIT)
    assert table.column("volume").to_pylist() == [1, 2, 3, 4]


# -- what the mutation lens found the fixtures could not hold --------------------------


def test_a_bars_session_is_eastern_rather_than_the_stamps_first_ten_characters(
    fixture_lake: FixtureLake,
):
    """A bar stamped after 20:00 Eastern carries the next UTC date and the same session.

    ``journal.py`` pins that a bar's session is decided by ``bar_ts``, and ``bars.session_of``
    implements it by converting to Eastern. Reading the stamp's first ten characters agrees on
    every regular-hours bar and disagrees here, which would leave this bar unadjusted while every
    other bar of its own session is halved.
    """
    rows = [
        _bar(BEFORE, 600.0, freq=MINUTE, minute="14:31"),
        {
            **_bar(BEFORE, 700.0, freq=MINUTE),
            "bar_ts": f"{EX}T00:30:00+00:00",
        },
    ]
    root = _lake(fixture_lake, {(MINUTE, BEFORE): rows})
    _split(root, ex_date=EX, ratio=2.0)
    table = load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_SPLIT)
    assert _closes(table) == [300.0, 350.0]


def test_the_prior_close_is_the_last_bar_by_stamp_not_the_first_row(fixture_lake: FixtureLake):
    """``lake.bars`` takes the same reading for its own gate: the last by stamp is the close.

    The daily window is wider than the session, so a response can carry more than one candle and
    a partition can land more than one row. Row order is a property of the file, per #242, so the
    stamp is what decides. This partition is written with the later candle first.
    """
    later = {**_bar(EX, 600.0), "bar_ts": f"{EX}T21:00:00+00:00"}
    earlier = {**_bar(EX, 300.0), "bar_ts": f"{EX}T20:00:00+00:00"}
    root = _lake(
        fixture_lake,
        {(DAILY, BEFORE): [_bar(BEFORE, 1000.0)], (DAILY, EX): [later, earlier]},
    )
    _dividend(root, ex_date=AFTER, amount=6.0)
    table = load_bars(TICKER, DAILY, BEFORE, BEFORE, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [pytest.approx(990.0)]


@pytest.mark.parametrize("close", [None, 0.0, -1.0, float("inf")])
def test_a_prior_close_that_is_no_price_refuses(fixture_lake: FixtureLake, close):
    """Null divides nothing, zero divides by zero, and below zero builds a factor above one."""
    root = _lake(
        fixture_lake,
        {
            (DAILY, EX): [{**_bar(EX, 300.0), "close": close}],
            (MINUTE, AFTER): [_bar(AFTER, 310.0, freq=MINUTE, minute="14:31")],
        },
    )
    _dividend(root, ex_date="2026-09-17", amount=3.0)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)


def test_an_empty_prior_partition_refuses(fixture_lake: FixtureLake):
    """This refuses where ``lake.bars``' gate returns nothing, which its docstring names.

    A gate holding a bar costs a night. A dividend silently dropped from a factor costs every
    return computed through it.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, EX): [],
            (MINUTE, AFTER): [_bar(AFTER, 310.0, freq=MINUTE, minute="14:31")],
        },
    )
    _dividend(root, ex_date="2026-09-17", amount=3.0)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, MINUTE, lake_root=root, adjust=ADJUST_TOTAL)


def _raw_ledger(root: Path, *overrides: dict) -> None:
    """Ledger lines written by hand, the only way to produce what the writer refuses.

    ``actions.append`` validates every field on the way in, so the guards that exist for a
    hand-edited `corporate_actions.jsonl` are unreachable through it. This is that file. Each
    argument overrides a valid split for the default instrument, and no argument writes that
    split alone.
    """
    lines = []
    for fields in overrides or ({},):
        entry = {
            "instrument_id": INSTRUMENT,
            "ex_date": EX,
            "type": actions.TYPE_SPLIT,
            "cash_amount": None,
            "split_ratio": 2.0,
            "observed_on": EX,
            "recorded_at": RECORDED_AT.isoformat(),
            "provenance": actions.PROVENANCE_OBSERVED,
            "schema_version": actions.ACTIONS_SCHEMA_VERSION,
        }
        entry.update(fields)
        lines.append(json.dumps(entry))
    path = root / "actions" / "corporate_actions.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(chr(10).join(lines) + chr(10))


# A dividend line no writer would produce: the amount is a string. It is the entry used below to
# show that a read checks what it uses, because an entry this malformed refuses wherever it is
# read and so says plainly whether it was read at all.
_UNREADABLE_DIVIDEND = {
    "type": actions.TYPE_DIVIDEND,
    "ex_date": AFTER,
    "split_ratio": None,
    "cash_amount": "3.00",
}


@pytest.mark.parametrize(
    "fields",
    [
        {"ex_date": "2026-13-01"},
        {"split_ratio": "2.0"},
        {"split_ratio": True},
        {"split_ratio": 0},
        {"split_ratio": -2.0},
        {"split_ratio": None},
    ],
    ids=["ex_date", "string", "bool", "zero", "negative", "null"],
)
def test_a_ledger_entry_this_cannot_read_refuses_rather_than_being_stepped_over(
    fixture_lake: FixtureLake, fields
):
    """The ledger's own resolution raises at the offending entry, and so does this.

    Stepping over a split silently returns as-traded prices under an adjusted name, which is the
    corruption the ``adjust`` column exists to make visible. ``True`` is named because
    ``float(True)`` is ``1.0`` and would pass as a ratio that adjusts nothing.
    """
    root = _three_daily_sessions(fixture_lake)
    _raw_ledger(root, fields)
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)


def test_a_cached_prior_close_is_not_reused_across_ex_dates(fixture_lake: FixtureLake):
    """Two dividends on one instrument each take their own denominator.

    The first is ex on the second session and prices against 600, the second is ex on the third
    and prices against 300. A cache keyed on the instrument alone would give the second the
    first's close, and the two factors are 0.98 and 0.99 rather than one number twice.
    """
    root = _three_daily_sessions(fixture_lake)
    _dividend(root, ex_date=EX, amount=6.0)
    _dividend(root, ex_date=AFTER, amount=6.0)
    table = load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)
    assert _closes(table) == [
        pytest.approx(600.0 * 0.99 * 0.98),
        pytest.approx(300.0 * 0.98),
        310.0,
    ]


def test_a_cached_prior_close_is_not_reused_across_instruments(fixture_lake: FixtureLake):
    """The second instrument re-reads rather than inheriting the first one's answer.

    The daily bar before the ex-date belongs to instrument A, so A's factor is computable and
    B's is not. A cache keyed on the ex-date alone would hand B the close that was validated for
    A, turning a refusal into a silent answer. A is first in the series, so the cache is warm by
    the time B is reached.
    """
    root = _lake(
        fixture_lake,
        {
            (DAILY, BEFORE): [_bar(BEFORE, 600.0)],
            (DAILY, EX): [_bar(EX, 300.0, instrument_id=INSTRUMENT + 1)],
            (DAILY, AFTER): [_bar(AFTER, 310.0)],
        },
    )
    _dividend(root, ex_date="2026-09-17", amount=3.0)
    actions.append(
        root,
        instrument_id=INSTRUMENT + 1,
        observed_on=date.fromisoformat("2026-09-17"),
        recorded_at=RECORDED_AT,
        ex_date="2026-09-17",
        type=actions.TYPE_DIVIDEND,
        cash_amount=3.0,
        provenance=actions.PROVENANCE_OBSERVED,
    )
    with pytest.raises(AdjustmentIncomplete):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_TOTAL)


def test_a_partition_being_written_is_not_a_session(fixture_lake: FixtureLake):
    """``paths.temp_write_path`` names a partition mid-write, and a listing can catch one.

    Reading that name as a session makes the door rebuild the canonical path and refuse a read
    that should have succeeded, naming a date whose file is still being written.
    """
    root = _lake(fixture_lake, {(DAILY, BEFORE): [_bar(BEFORE, 600.0)]})
    directory = root / "bars" / f"ticker={TICKER}" / f"freq={DAILY}"
    (directory / f"date={EX}.parquet.tmp-1234").write_bytes(b"")
    assert _closes(load_bars(TICKER, DAILY, lake_root=root)) == [600.0]


def test_a_table_without_an_instrument_column_refuses_as_a_load_error(fixture_lake: FixtureLake):
    """The column is the key every adjusted view joins on, so its absence is this door's error."""
    narrow = pa.schema([f for f in journal.BARS_SCHEMA if f.name != "instrument_id"])
    fixture_lake.with_bars(TICKER, DAILY, BEFORE, _table([_bar(BEFORE, 600.0)], schema=narrow))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    _split(root)
    assert _closes(load_bars(TICKER, DAILY, lake_root=root)) == [600.0]
    with pytest.raises(LoadError):
        load_bars(TICKER, DAILY, lake_root=root, adjust=ADJUST_SPLIT)
