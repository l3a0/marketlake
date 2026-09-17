"""``load_chain`` against a fixture lake on disk.

These sit in the component tier because they cross files. Each builds a real lake with
``FixtureLake``, sealed Parquet partitions and the two ledgers and the schema-version
reference table included, then reads it back through the loader. No vendor, no network,
and no wall clock. The real lake is never touched.

Two fixture sessions carry the whole test surface, and each is chosen for what it can
refute.

*A full session*, 2026-09-14, running 09:30 to 16:15 ET. It holds the intraday minutes,
a gap row inside a resolvable cycle, and a ``spot_close`` cycle sitting beside the
``option_close`` one. Its 10:31 ET cycle is stamped ``14:31`` in UTC, so a loader reading
``snap`` as UTC and a loader reading it as ET ask for different minutes and the session
holds a cycle at each.

*A half day*, 2026-11-27, the Friday after Thanksgiving, which closes at 13:00 ET and
whose option close is therefore 13:15 ET. It is what separates resolving the close of
record through ``close_tag`` from computing it off a clock. A loader that assumed the
regular 16:15 finds nothing on this day.

Both ISO spellings of an instant appear on purpose, including one minute written both
ways. The live lake writes ``snap_ts`` with a ``+00:00`` offset and the suite's fixture
schema writes an Eastern one, and SPY's real 2026-09-11 partition holds 408 distinct
``snap_ts`` texts naming 406 distinct instants because two of its minutes were written
both ways. A loader comparing the stored text would answer one spelling and not the other.

The numbered tests carry the numbering of the issue that asked for them, so a mutation an
issue names points at the test that issue names. Three issues number tests here, and each
test says which.

1. #135 numbers the tests for the loader itself.
2. #242 numbers the tests for the two-pass read.
3. #249 numbers the tests for the lake root the call resolves.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from lake import loader
from lake.config import CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH, ConfigError
from lake.extra_projection import ExtraProjectionError
from lake.loader import (
    LoadError,
    NoOptionClose,
    PartialRead,
    PartitionAbsent,
    PartitionQuarantined,
    SnapAbsent,
    SnapMalformed,
    _instant,
    load_chain,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from tests.support.config import write_config
from tests.support.config_guard import is_protected
from tests.support.lake import FixtureLake, sample_chains_table

# The full session and the half day. Both are real sessions on the exchange calendar,
# though nothing in the loader reads one.
FULL_DAY = "2026-09-14"
HALF_DAY = "2026-11-27"

# When the schema-version ledger recorded version 1. Any instant does, since the loader
# reads the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

CALL = "SPY   260918C00650000"
PUT = "SPY   260918P00650000"
# A third contract, carried only on the row that spells 10:31 ET the other way.
STRADDLE = "SPY   260918C00655000"


def _data(snap: str | None, occ: str = CALL, close_tag: str | None = None) -> dict:
    """One data row: a vendor observation at ``snap``.

    Neither ``fetch_ts`` nor ``vendor_quote_ts`` equals ``snap_ts``. One carries the
    request's own latency and the other is the vendor's own quote time, and in the live
    lake both sit a fraction of a second off the slot. A fixture that made any of the
    three equal could not tell a loader resolving the minute slot from one resolving
    either of the others, and #135 names ``snap_ts`` as the only one that resolves.
    """
    return {
        "snap_ts": snap,
        "fetch_ts": _stamped(snap, "400"),
        "vendor_quote_ts": _stamped(snap, "150"),
        "ticker": "SPY",
        "occ_symbol": occ,
        "bid": 4.20,
        "ask": 4.25,
        "last": 4.22,
        "open_interest": 1234,
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
    }


def _stamped(snap: str | None, millis: str) -> str | None:
    """``snap`` moved ``millis`` into its slot, so it is the same minute and not the same value.

    A row built with no ``snap_ts`` gets no other stamp either, which is the shape the
    malformed-row tests want.
    """
    if snap is None:
        return None
    instant, offset = snap[:19], snap[19:]
    return f"{instant}.{millis}{offset}"


def _gap(snap: str, close_tag: str | None = None) -> dict:
    """One gap row: a minute that was attempted and missed, every vendor column null."""
    row = _data(snap, close_tag=close_tag)
    row.update(
        {
            "occ_symbol": None,
            "bid": None,
            "ask": None,
            "last": None,
            "open_interest": None,
            "row_kind": "gap",
            "error_class": "vendor_timeout",
        }
    )
    return row


# The full session. Five cycles carry data, and two carry a gap row beside it.
#
# 13:30Z is 09:30 ET, the open. 14:31Z is 10:31 ET, which is also the minute a loader
# reading ``snap`` as UTC would land on when asked for 14:31. 18:31Z is 14:31 ET, which
# is the answer to that question read correctly. 20:00Z is 16:00 ET and carries
# ``spot_close``. 20:15Z is 16:15 ET and carries ``option_close``, written in the Eastern
# spelling so one partition holds both.
FULL_ROWS = [
    _data("2026-09-14T13:30:00+00:00", CALL),
    _data("2026-09-14T13:30:00+00:00", PUT),
    _data("2026-09-14T14:31:00+00:00", CALL),
    _data("2026-09-14T14:31:00+00:00", PUT),
    _data("2026-09-14T10:31:00-04:00", STRADDLE),
    _data("2026-09-14T18:31:00+00:00", CALL),
    _data("2026-09-14T18:31:00+00:00", PUT),
    _gap("2026-09-14T18:31:00+00:00"),
    _gap("2026-09-14T19:00:00+00:00"),
    _data("2026-09-14T20:00:00+00:00", CALL, close_tag="spot_close"),
    _data("2026-09-14T20:00:00+00:00", PUT, close_tag="spot_close"),
    _data("2026-09-14T16:15:00-04:00", CALL, close_tag="option_close"),
    _data("2026-09-14T16:15:00-04:00", PUT, close_tag="option_close"),
    _gap("2026-09-14T16:15:00-04:00", close_tag="option_close"),
]

# The half day. Its option close is 13:15 ET, which is 18:15Z under Eastern Standard
# Time, and no cycle runs at 16:15 ET at all.
HALF_ROWS = [
    _data("2026-11-27T14:30:00+00:00", CALL),
    _data("2026-11-27T14:30:00+00:00", PUT),
    _data("2026-11-27T18:00:00+00:00", CALL, close_tag="spot_close"),
    _data("2026-11-27T18:00:00+00:00", PUT, close_tag="spot_close"),
    _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    _data("2026-11-27T18:15:00+00:00", PUT, close_tag="option_close"),
]

# A session whose cycles ran and whose close of record was never tagged. Its last
# snapshot is the 15:59 ET cycle, which is what a loader substituting the most recent
# snapshot would hand back.
UNTAGGED_ROWS = [
    _data("2026-09-14T13:30:00+00:00", CALL),
    _data("2026-09-14T19:59:00+00:00", CALL),
    _data("2026-09-14T19:59:00+00:00", PUT),
]

# The half day again, in a second lake, holding a different contract at the same minute.
# It is what separates the configured root from an explicit one in the tests at the foot of
# this file, because the rows that come back name which lake answered.
OTHER_HALF_ROWS = [
    _data("2026-11-27T18:15:00+00:00", STRADDLE, close_tag="option_close"),
]

FULL_PARTITION = f"chains/ticker=SPY/date={FULL_DAY}.parquet"


def _ledger_table():
    """The schema-version ledger recording version 1 at the shape the running code writes."""
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _lake(fixture_lake: FixtureLake, *, quarantine: list[dict] | None = None, ledger=True):
    """A fixture lake holding both sessions, the untagged day, and the ledger."""
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(FULL_ROWS))
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(HALF_ROWS))
    fixture_lake.with_chains("QQQ", FULL_DAY, sample_chains_table(UNTAGGED_ROWS))
    if ledger:
        fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _with_extra(row: dict, overflow: dict) -> dict:
    """``row`` with ``overflow`` JSON-encoded into its ``extra`` column."""
    return {**row, "extra": json.dumps(overflow)}


def _lake_missing_column(fixture_lake: FixtureLake, rows: list[dict], column: str):
    """A lake whose ledger records version 1 without ``column`` on the chains surface.

    That is what history looks like below a promotion. The version carried no column for
    the measurement, so its value sits in the overflow and the projection is what presents
    it as the column the running schema gives it.

    ``column`` has to be one the fixture schema also lacks for the projection to be seen
    adding it rather than filling one already there.
    """
    fingerprints = {
        surface: {name: kind for name, kind in columns.items() if name != column}
        if surface == "chains"
        else dict(columns)
        for surface, columns in running_fingerprints().items()
    }
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=fingerprints)
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", SchemaVersionLedger([entry]).to_table())
    return fixture_lake.build()


def _snaps(table) -> set[str]:
    return set(table.column("snap_ts").to_pylist())


def _kinds(table) -> set[str]:
    return set(table.column("row_kind").to_pylist())


# -- resolving the close of record -------------------------------------------


def test_snap_none_returns_the_option_close_cycle_on_a_half_day(fixture_lake: FixtureLake):
    """#135 test 1. The close of record comes from the tag, not from a clock.

    The half day's option close is 13:15 ET, so a loader computing the regular 16:15
    finds no cycle at all, and one taking the day's last snapshot takes 13:00 ET's
    ``spot_close`` rows instead.
    """
    root = _lake(fixture_lake)

    table = load_chain("SPY", HALF_DAY, lake_root=root)

    assert table.num_rows == 2
    assert _snaps(table) == {"2026-11-27T18:15:00+00:00"}
    assert set(table.column("close_tag").to_pylist()) == {"option_close"}


def test_snap_none_takes_the_option_close_cycle_and_not_its_neighbour(fixture_lake: FixtureLake):
    """#135 test 1, on a full session. The ``spot_close`` cycle 15 minutes earlier stays out."""
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, lake_root=root)

    assert table.num_rows == 2
    assert _snaps(table) == {"2026-09-14T16:15:00-04:00"}
    assert set(table.column("occ_symbol").to_pylist()) == {CALL, PUT}


def test_a_session_with_no_option_close_tag_raises(fixture_lake: FixtureLake):
    """#135 test 2. The explicit marker, never a substitute.

    QQQ's day ran cycles to 15:59 ET and tagged none of them. The last snapshot is
    exactly what a substituting loader would return, so it is present to be returned.
    """
    root = _lake(fixture_lake)

    with pytest.raises(NoOptionClose) as caught:
        load_chain("QQQ", FULL_DAY, lake_root=root)

    assert caught.value.tagged_gaps == 0
    assert caught.value.close_tag == "option_close"
    assert caught.value.ticker == "QQQ"
    assert "option_close" in str(caught.value)


def test_the_marker_counts_tagged_gap_rows(fixture_lake: FixtureLake):
    """A close that ran and failed reads differently from one that never ran.

    Both raise, because neither produced marks. The count is what tells them apart.
    """
    rows = [
        _data("2026-09-14T13:30:00+00:00", CALL),
        _gap("2026-09-14T16:15:00-04:00", close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(NoOptionClose) as caught:
        load_chain("SPY", FULL_DAY, lake_root=root)

    assert caught.value.tagged_gaps == 1


# -- resolving an intraday minute --------------------------------------------


def test_snap_resolves_as_an_eastern_wall_clock_minute(fixture_lake: FixtureLake):
    """#135 test 3. ``10:31`` ET is 14:31Z on a September session."""
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root)

    assert set(table.column("occ_symbol").to_pylist()) == {CALL, PUT, STRADDLE}
    assert {_instant(text) for text in _snaps(table)} == {datetime(2026, 9, 14, 14, 31, tzinfo=UTC)}


def test_one_instant_under_two_spellings_comes_back_whole(fixture_lake: FixtureLake):
    """#135 test 3, on the shape the live lake actually carries.

    SPY's real 2026-09-11 partition writes two of its minutes both as an Eastern offset
    and as ``+00:00``. A loader matching the stored text returns whichever spelling it
    happened to ask for and silently drops the rest of the minute.
    """
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root)

    assert _snaps(table) == {"2026-09-14T14:31:00+00:00", "2026-09-14T10:31:00-04:00"}
    assert table.num_rows == 3


def test_snap_is_never_read_as_utc(fixture_lake: FixtureLake):
    """#135 test 3, refuting the other reading. ``14:31`` ET is 18:31Z, and 14:31Z is a decoy.

    The session holds a cycle at both minutes, so a loader treating ``snap`` as UTC
    returns rows rather than raising, which is the failure this refutes.
    """
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, snap="14:31", lake_root=root)

    assert _snaps(table) == {"2026-09-14T18:31:00+00:00"}


def test_a_minute_no_cycle_recorded_raises(fixture_lake: FixtureLake):
    """#135 test 4. An empty table would read as a chain with no contracts."""
    root = _lake(fixture_lake)

    with pytest.raises(SnapAbsent) as caught:
        load_chain("SPY", FULL_DAY, snap="11:00", lake_root=root)

    assert caught.value.snap == "11:00"
    assert caught.value.tagged_gaps == 0


def test_a_minute_that_ran_and_failed_counts_its_gap_rows(fixture_lake: FixtureLake):
    """A minute that was attempted reads differently from one that never was.

    Both raise, because neither produced marks. The count is what tells them apart, the
    same way it does for the close of record.
    """
    root = _lake(fixture_lake)

    with pytest.raises(SnapAbsent) as caught:
        load_chain("SPY", FULL_DAY, snap="15:00", lake_root=root)

    assert caught.value.tagged_gaps == 1


def test_the_offset_comes_from_the_zone_and_not_from_a_constant(fixture_lake: FixtureLake):
    """A winter minute lands on the offset its session actually ran under.

    Every other snap test reads the September session, which is Eastern Daylight Time. A
    loader carrying a fixed minus four would answer those and quietly miss by an hour on
    the November one, where the same wall clock is minus five.
    """
    root = _lake(fixture_lake)

    table = load_chain("SPY", HALF_DAY, snap="13:15", lake_root=root)

    assert _snaps(table) == {"2026-11-27T18:15:00+00:00"}


@pytest.mark.parametrize("snap", ["10:31:00", "9:31", "24:00", "abc", ""])
def test_a_snap_outside_hh_mm_is_refused(fixture_lake: FixtureLake, snap: str):
    """A minute the loader cannot read is a caller error, not an absent cycle.

    It is both a ``ValueError`` and a ``LoadError``, so the one exception the docstring
    tells a caller to catch really does cover every way a read resolves to no table.
    """
    root = _lake(fixture_lake)

    with pytest.raises(SnapMalformed) as caught:
        load_chain("SPY", FULL_DAY, snap=snap, lake_root=root)

    assert isinstance(caught.value, ValueError)
    assert isinstance(caught.value, LoadError)


def test_an_unreadable_snap_ts_elsewhere_does_not_take_the_answer_away(
    fixture_lake: FixtureLake,
):
    """A read that found its minute has no ambiguity left for a bad row to create.

    Refusing the whole partition would take away an answer the partition can give. The
    unreadable value is reported only when nothing matched, where it is part of why.
    """
    rows = [*FULL_ROWS, _data("not-a-timestamp", STRADDLE), _data(None, STRADDLE)]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    assert load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root).num_rows == 3

    with pytest.raises(SnapAbsent) as caught:
        load_chain("SPY", FULL_DAY, snap="11:00", lake_root=root)
    assert sorted(caught.value.unreadable) == ["'not-a-timestamp'", "None"]


def test_a_snap_ts_with_no_offset_never_matches_silently(fixture_lake: FixtureLake):
    """A naive stamp names no instant, so it is reported rather than compared.

    Python compares a naive datetime with an aware one as unequal rather than raising, so
    a naive stamp that slipped through would answer "no cycle recorded that minute" about
    a row whose minute nothing established.
    """
    rows = [_data("2026-09-14T11:00:00", CALL)]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(SnapAbsent) as caught:
        load_chain("SPY", FULL_DAY, snap="11:00", lake_root=root)

    assert caught.value.unreadable == ("'2026-09-14T11:00:00'",)


# -- gap rows ----------------------------------------------------------------


def test_gap_rows_are_absent_from_the_close_of_record(fixture_lake: FixtureLake):
    """#135 test 5. The option-close cycle carries a gap row beside its two data rows."""
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, lake_root=root)

    assert _kinds(table) == {"data"}
    assert table.num_rows == 2


def test_gap_rows_are_absent_from_an_intraday_minute(fixture_lake: FixtureLake):
    """#135 test 5, on the other resolution. 14:31 ET carries a gap row too."""
    root = _lake(fixture_lake)

    table = load_chain("SPY", FULL_DAY, snap="14:31", lake_root=root)

    assert _kinds(table) == {"data"}
    assert table.num_rows == 2


# -- quarantine --------------------------------------------------------------


def test_a_quarantined_partition_is_excluded_by_default(fixture_lake: FixtureLake):
    """#135 test 6, first half. Fail closed for data already sealed."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    with pytest.raises(PartitionQuarantined) as caught:
        load_chain("SPY", FULL_DAY, lake_root=root)

    assert caught.value.partition == FULL_PARTITION
    assert caught.value.entry["verdict"] == "delayed_feed"


def test_a_quarantined_partition_reads_under_the_opt_in(fixture_lake: FixtureLake):
    """#135 test 6, second half. The opt-in is explicit and per call."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    table = load_chain("SPY", FULL_DAY, lake_root=root, include_quarantined=True)

    assert table.num_rows == 2


def test_quarantine_covers_only_the_partition_it_names(fixture_lake: FixtureLake):
    """A verdict on one ticker-day leaves every other one readable."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    assert load_chain("SPY", HALF_DAY, lake_root=root).num_rows == 2


def test_a_superseding_clean_verdict_un_quarantines(fixture_lake: FixtureLake):
    """Last entry per check wins, so un-quarantine is an entry rather than a deletion.

    Neither entry names a check, so both resolve in the same bucket and the later one wins.
    tests/unit/test_manifest.py covers the keyed path.
    """
    root = _lake(
        fixture_lake,
        quarantine=[
            {"partition": FULL_PARTITION, "verdict": "delayed_feed"},
            {"partition": FULL_PARTITION, "verdict": "clean"},
        ],
    )

    assert load_chain("SPY", FULL_DAY, lake_root=root).num_rows == 2


def test_an_entry_the_reader_cannot_recognise_excludes(fixture_lake: FixtureLake):
    """Fail closed for data already sealed means an unreadable verdict refuses.

    ``battery.build_entry`` owns the entry shape and both writers go through it, so a
    spelling this read does not know comes from a hand-edited ledger rather than from a
    writer. Admitting one would fail open on exactly the partition the guard exists to
    withhold.
    """
    root = _lake(fixture_lake, quarantine=[{"partition": FULL_PARTITION, "note": "unreadable"}])

    with pytest.raises(PartitionQuarantined):
        load_chain("SPY", FULL_DAY, lake_root=root)


def test_a_ticker_spelled_differently_from_its_directory_is_refused(fixture_lake: FixtureLake):
    """macOS opens ``ticker=spy`` against ``ticker=SPY``, and the ledger key would miss.

    The quarantine lookup keys on the path as the caller spelled it. A read under the
    wrong case would open a real partition and find no verdict for it, which turns the
    guard from fail closed into fail open.
    """
    root = _lake(fixture_lake, quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed"}])

    with pytest.raises(PartitionAbsent):
        load_chain("spy", FULL_DAY, lake_root=root)


def test_an_absent_quarantine_ledger_excludes_nothing(fixture_lake: FixtureLake):
    """#135 test 7. The guard is inert until the battery writes its first verdict."""
    root = _lake(fixture_lake)

    assert not (root / "quarantine.jsonl").exists()
    assert load_chain("SPY", FULL_DAY, lake_root=root).num_rows == 2


# -- the overflow projection -------------------------------------------------


def test_a_version_the_ledger_has_no_shape_for_reads_partial(fixture_lake: FixtureLake):
    """A read the projection could not complete never comes back looking whole.

    An absent schema-version ledger is that condition reached the shortest way. Every
    version in the partition is then one the ledger holds no shape for, so nothing can
    say whether a value in the overflow belongs in a column.
    """
    root = _lake(fixture_lake, ledger=False)

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", FULL_DAY, lake_root=root)

    assert caught.value.projection.unrecorded_versions == (1,)
    assert "version 1" in str(caught.value)


def test_a_promoted_value_is_lifted_out_of_the_overflow(fixture_lake: FixtureLake):
    """The projection is called and it does work, rather than passing the table through.

    The ledger records version 1 without ``volume``, which is what history looks like
    below a promotion, and the value sits in the overflow under the vendor's own key. The
    read presents it as the column the running schema gives it.

    ``volume`` rather than ``open_interest`` because the fixture schema does not carry it.
    A column the table already has is filled in place, so the projection adding a column is
    only visible on one the table lacks.
    """
    rows = [_with_extra(row, {"totalVolume": 5678}) for row in HALF_ROWS]
    root = _lake_missing_column(fixture_lake, rows, "volume")

    table = load_chain("SPY", HALF_DAY, lake_root=root)

    assert "volume" not in sample_chains_table(rows).column_names
    assert table.column("volume").to_pylist() == [5678, 5678]


def test_a_column_a_retype_routed_into_the_overflow_reads_partial(fixture_lake: FixtureLake):
    """The third of the three conditions the owner comment calls partial.

    Here the version does carry the column, so the value in the overflow says the parser
    routed it there after the column refused it, which is a vendor retype. Nothing lifts
    it, because presenting it would need a cast, and the read is partial all the same.
    """
    rows = [_with_extra(row, {"totalVolume": 5}) for row in HALF_ROWS]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", HALF_DAY, lake_root=root)

    assert [column.column for column in caught.value.projection.retyped] == ["volume"]


def test_a_value_the_promoted_column_refuses_reads_partial(fixture_lake: FixtureLake):
    """The second of the three conditions the owner comment calls partial.

    Only the unrecorded-version condition had a test, so narrowing the check to that one
    condition went unnoticed. A refused value means the read is partial too.
    """
    rows = [_with_extra(row, {"totalVolume": "not-a-number"}) for row in HALF_ROWS]
    root = _lake_missing_column(fixture_lake, rows, "volume")

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", HALF_DAY, lake_root=root)

    assert caught.value.projection.unrecorded_versions == ()
    assert [unfit.column for unfit in caught.value.projection.unfit] == ["volume"]


def test_the_column_set_does_not_move_with_the_minute_asked_for(fixture_lake: FixtureLake):
    """Two reads of one ticker-day come back with the same columns, so they can be stitched.

    The projection adds a promoted column only when a row it is handed carries a value for
    it, so projecting one minute at a time would make the column set a property of the
    minute. Only the 09:30 row here carries one, so under that ordering the 09:30 read
    gains a ``volume`` column the close-of-record read does not have, and
    ``pa.concat_tables`` over the two raises.
    """
    rows = [
        _with_extra(_data("2026-11-27T14:30:00+00:00", CALL), {"totalVolume": 11}),
        _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    ]
    root = _lake_missing_column(fixture_lake, rows, "volume")

    early = load_chain("SPY", HALF_DAY, snap="09:30", lake_root=root)
    close = load_chain("SPY", HALF_DAY, lake_root=root)

    assert "volume" in early.column_names
    assert early.column_names == close.column_names
    assert pa.concat_tables([early, close]).num_rows == 2


# -- rows the loader refuses to vouch for ------------------------------------


def test_a_row_with_no_row_kind_is_refused(fixture_lake: FixtureLake):
    """Arrow's filter drops a null-kind row from both sides, so it would just vanish.

    It is neither a vendor observation nor an absence marker. Returning the rest quietly
    would under-report the chain and the counts that explain an empty one would not see it.
    """
    rows = [*HALF_ROWS, {**_data("2026-11-27T18:15:00+00:00", STRADDLE), "row_kind": None}]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(LoadError, match="row_kind"):
        load_chain("SPY", HALF_DAY, lake_root=root)


def test_an_option_close_tag_on_two_cycles_is_refused(fixture_lake: FixtureLake):
    """A close of record is one cycle, so two tagged cycles are not stitched into one.

    Everywhere else this loader refuses rather than hands back a questionable table. A
    chain assembled from two different minutes carries no signal that it is one.
    """
    rows = [
        _data("2026-11-27T18:00:00+00:00", CALL, close_tag="option_close"),
        _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(LoadError, match="one cycle"):
        load_chain("SPY", HALF_DAY, lake_root=root)


# -- the partition itself ----------------------------------------------------


def test_an_unsealed_day_raises(fixture_lake: FixtureLake):
    """A session with no sealed partition is not an empty chain."""
    root = _lake(fixture_lake)

    with pytest.raises(PartitionAbsent):
        load_chain("SPY", "2026-09-15", lake_root=root)


def test_a_directory_where_the_partition_belongs_is_not_a_partition(fixture_lake: FixtureLake):
    """A sealed partition is one file. A directory in its place is not one."""
    root = _lake(fixture_lake)
    (root / "chains" / "ticker=SPY" / "date=2026-09-15.parquet").mkdir()

    with pytest.raises(PartitionAbsent):
        load_chain("SPY", "2026-09-15", lake_root=root)


def test_a_directory_where_the_schema_version_ledger_belongs_is_not_a_ledger(
    fixture_lake: FixtureLake,
):
    """The ledger is one file too, and a directory in its place reads as no ledger at all.

    An absent ledger is not a case of its own: every version is then one it holds no shape
    for, which is what ``PartialRead`` already refuses.
    """
    root = _lake(fixture_lake, ledger=False)
    (root / "reference").mkdir(parents=True, exist_ok=True)
    (root / "reference" / "schema_versions.parquet").mkdir()

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", FULL_DAY, lake_root=root)

    assert caught.value.projection.unrecorded_versions == (1,)


def test_a_date_object_reads_the_same_partition_as_its_iso_text(fixture_lake: FixtureLake):
    """The signature takes either, so both have to land on one path."""
    root = _lake(fixture_lake)

    assert load_chain("SPY", date(2026, 11, 27), lake_root=root).num_rows == 2


# -- reading only the rows the answer is made of -----------------------------


def _row_group_ranges(path) -> list[tuple[str, str]]:
    """Each row group's ``snap_ts`` range, as Parquet's own statistics record it."""
    metadata = pq.ParquetFile(path).metadata
    column = {metadata.schema.column(i).name: i for i in range(metadata.num_columns)}["snap_ts"]
    return [
        (
            metadata.row_group(group).column(column).statistics.min,
            metadata.row_group(group).column(column).statistics.max,
        )
        for group in range(metadata.num_row_groups)
    ]


def _overlapping(ranges: list[tuple[str, str]]) -> bool:
    """Whether any two of ``ranges`` share a value, so pruning cannot go by order."""
    return any(
        low <= other_high and other_low <= high
        for index, (low, high) in enumerate(ranges)
        for other_low, other_high in ranges[index + 1 :]
    )


def _shuffled_session(count: int = 600) -> list[dict]:
    """A session's rows in deliberately shuffled order, spread over twenty minutes.

    Compaction writes a partition in ``snap_ts`` order today, which makes its row groups
    time-disjoint and hides the question this fixture asks. Shuffling puts every minute in
    every row group, so a reader that pruned on an assumed ordering answers short.
    """
    shuffler = random.Random(7)
    minutes = [f"2026-09-14T14:{minute:02d}:00+00:00" for minute in range(20)]
    rows = [
        _data(shuffler.choice(minutes), occ=f"SPY   260918C{index:08d}") for index in range(count)
    ]
    shuffler.shuffle(rows)
    return rows


def test_a_pushdown_read_answers_a_partition_whose_row_groups_overlap(
    fixture_lake: FixtureLake,
):
    """#242 test 1. Row-group ordering decides how much is skipped and never what comes back.

    This issue once claimed the opposite, that pruning would ride on compaction's
    incidental ``snap_ts`` ordering and would silently return a short answer when a writer
    restart broke it. Parquet skips a row group only when its statistics prove no row in it
    can match, and it filters whatever it did read. So the answer is the full read's answer
    and this loader asks the writer for no guarantee.

    The assertion on the ranges is what keeps this from passing vacuously. A fixture
    written in one row group, or in row groups that happen to be disjoint, tests nothing.
    """
    rows = _shuffled_session()
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(rows), row_group_size=50)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    path = root / "chains" / "ticker=SPY" / f"date={FULL_DAY}.parquet"
    ranges = _row_group_ranges(path)
    assert len(ranges) > 1
    assert _overlapping(ranges)

    table = load_chain("SPY", FULL_DAY, snap="10:07", lake_root=root)

    whole = pq.read_table(path)
    expected = whole.filter(pc.equal(whole.column("snap_ts"), "2026-09-14T14:07:00+00:00"))
    assert table.num_rows == expected.num_rows
    assert sorted(table.column("occ_symbol").to_pylist()) == sorted(
        expected.column("occ_symbol").to_pylist()
    )


class _Reads:
    """A stand-in for the loader's Parquet read that records what it was asked for.

    A correct predicate returns the rows a full read would, so no assertion on a returned
    table can tell whether one happened, and a wall-clock timing is not a test. This
    records the arguments and hands the real read through.
    """

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, path, *, columns=None, filters=None):
        self.calls.append({"path": path, "columns": columns, "filters": filters})
        return pq.read_table(path, columns=columns, filters=filters)

    @property
    def resolve(self) -> dict:
        """The pass that reads columns over the whole partition."""
        return next(call for call in self.calls if call["columns"] is not None)

    @property
    def fetch(self) -> dict:
        """The pass that reads rows, named to Parquet as a predicate."""
        return next(call for call in self.calls if call["filters"] is not None)

    @property
    def unrestricted(self) -> list[dict]:
        """Every read that named neither its columns nor its rows.

        One of those is the whole partition, which is what this change removed. Nothing
        about a returned table would show a second one creeping back in beside the two
        passes, because its result would simply be discarded.
        """
        return [c for c in self.calls if c["columns"] is None and c["filters"] is None]


@pytest.fixture
def reads(monkeypatch: pytest.MonkeyPatch) -> _Reads:
    recorder = _Reads()
    monkeypatch.setattr(loader, "_read", recorder)
    return recorder


def test_the_fetch_names_every_spelling_of_the_minute_to_parquet(
    fixture_lake: FixtureLake, reads: _Reads
):
    """#242 test 2, the half the returned rows cannot show.

    An equality would name one spelling of an instant that has two.

    The returned rows cannot tell an equality from a set, because the filter behind the
    read admits both either way and today's lake spells every row the same. What differs
    is what Parquet was asked, and a partition a different writer spelled the other way is
    answered short by the equality and whole by the set.
    """
    root = _lake(fixture_lake)

    load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root)

    expected = (
        ds.field("snap_ts").isin(["2026-09-14T14:31:00+00:00", "2026-09-14T10:31:00-04:00"])
        | ds.field("extra").is_valid()
    )
    assert reads.fetch["filters"].equals(expected)
    assert len(reads.calls) == 2
    assert reads.unrestricted == []


def test_the_fetch_asks_for_every_row_that_could_add_a_column(
    fixture_lake: FixtureLake, reads: _Reads
):
    """The overflow disjunct is what keeps the column set a property of the partition.

    The projection adds a promoted column only when a row it is handed carries a value for
    it, so a fetch of the answer's rows alone would make the column set move with the
    minute asked for. Only a row whose ``extra`` is not null can add one, so those rows
    ride along wherever in the session they sit.
    """
    root = _lake(fixture_lake)

    load_chain("SPY", FULL_DAY, lake_root=root)

    expected = ds.field("close_tag").isin(["option_close"]) | ds.field("extra").is_valid()
    assert reads.fetch["filters"].equals(expected)
    assert len(reads.calls) == 2
    assert reads.unrestricted == []


def test_the_resolve_pass_reads_only_the_columns_a_read_resolves_against(
    fixture_lake: FixtureLake, reads: _Reads
):
    """The whole-partition pass reads two columns rather than seventy-three.

    A minute resolves against ``snap_ts`` and counts absence markers by ``row_kind``, and
    it never asks about the close tag. Naming a column to Parquet makes it a requirement of
    the layout, so a read that names one it does not use takes on a way to fail for
    nothing. The vendor columns are read only for the rows the answer is made of.
    """
    root = _lake(fixture_lake)

    load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root)

    assert reads.resolve["columns"] == ["snap_ts", "row_kind"]
    assert reads.resolve["filters"] is None
    assert len(reads.calls) == 2
    assert reads.unrestricted == []


def test_the_close_of_record_resolves_against_the_tag_column_as_well(
    fixture_lake: FixtureLake, reads: _Reads
):
    """The other resolution reads the one column it does resolve against."""
    root = _lake(fixture_lake)

    load_chain("SPY", FULL_DAY, lake_root=root)

    assert reads.resolve["columns"] == ["snap_ts", "row_kind", "close_tag"]


def test_a_minute_reads_a_partition_that_carries_no_close_tag_column(
    fixture_lake: FixtureLake,
):
    """A column a minute never resolves against is not a column its read depends on.

    No writer drops ``close_tag``, so this is about what the read requires rather than
    about a partition anyone has. Naming every resolving column on every read would make a
    minute fail on a partition it can answer perfectly well.
    """
    table = sample_chains_table(FULL_ROWS).drop_columns(["close_tag"])
    fixture_lake.with_chains("SPY", FULL_DAY, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    assert load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root).num_rows == 3


def test_a_partition_with_no_overflow_column_says_so_by_name(fixture_lake: FixtureLake):
    """``extra_projection`` owns what a chains table missing its overflow column means.

    The fetch names ``extra`` to Parquet so the rows that decide the column set ride along,
    and naming a column Parquet does not have fails the scan with a message about field
    references. So the half that names it waits on the column existing, which leaves the
    refusal with the module whose guard it is.
    """
    table = sample_chains_table(FULL_ROWS).drop_columns(["extra"])
    fixture_lake.with_chains("SPY", FULL_DAY, table)
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(ExtraProjectionError, match="no extra column"):
        load_chain("SPY", FULL_DAY, snap="10:31", lake_root=root)


def test_a_row_carrying_no_schema_version_refuses_the_reads_that_include_it(
    fixture_lake: FixtureLake,
):
    """The fourth refusal that narrowed with the read, and it narrowed on purpose.

    A row with no ``schema_version`` is a row the journal did not write, and the projection
    raises on one rather than reporting it. Nothing computed over the whole partition
    depends on a row's version, so the refusal follows the rows the read is made of. The
    ``row_kind`` guard beside it stays whole-partition instead, because the counts that
    explain an empty answer are taken over every row.
    """
    rows = [
        {**_data("2026-11-27T14:30:00+00:00", CALL), "schema_version": None},
        _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    assert load_chain("SPY", HALF_DAY, lake_root=root).num_rows == 1

    with pytest.raises(ExtraProjectionError, match="no schema_version"):
        load_chain("SPY", HALF_DAY, snap="09:30", lake_root=root)


def test_a_minute_is_not_projected_against_the_rest_of_the_session(fixture_lake: FixtureLake):
    """#242 test 3. The projection sees the answer's rows, not the day's.

    A version the schema-version ledger holds no shape for is the projection's report made
    visible. Here 09:30 sits at version 2, which the ledger does not record, and the
    close-of-record cycle sits at the recorded version 1. A loader projecting the whole
    partition meets version 2 on every read of this day and refuses them all. One that
    projects the rows it selected refuses only the read that asked for 09:30.

    That narrower scope is chosen. A read about one minute should not be taken away by a
    defect in a minute nobody asked for.
    """
    rows = [
        {**_data("2026-11-27T14:30:00+00:00", CALL), "schema_version": 2},
        _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    assert load_chain("SPY", HALF_DAY, lake_root=root).num_rows == 1

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", HALF_DAY, snap="09:30", lake_root=root)
    assert caught.value.projection.unrecorded_versions == (2,)


def test_an_overflow_at_an_unrecorded_version_refuses_from_anywhere_in_the_session(
    fixture_lake: FixtureLake,
):
    """The narrowing stops where a row could move the answer's own columns.

    A row carrying an overflow value is read by every fetch, because it is a row that can
    add a column. So a version the ledger has no shape for still refuses every read of the
    day when a row at that version holds something in its overflow, which is the case where
    the projection cannot say whether that value belongs in a column of the table returned.
    """
    rows = [
        _with_extra(
            {**_data("2026-11-27T14:30:00+00:00", CALL), "schema_version": 2},
            {"totalVolume": 11},
        ),
        _data("2026-11-27T18:15:00+00:00", CALL, close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(PartialRead) as caught:
        load_chain("SPY", HALF_DAY, lake_root=root)

    assert caught.value.projection.unrecorded_versions == (2,)


def test_a_resolution_that_finds_nothing_says_so_rather_than_reporting_the_projection(
    fixture_lake: FixtureLake,
):
    """The projection stopped running before the resolution, so the resolution answers first.

    This day's rows sit at a version the ledger has no shape for and none of them carries
    the close-of-record tag. Projecting the whole partition first meant every read of it
    raised ``PartialRead``, whatever the reader had asked for. The projection now runs on
    the rows a resolution chose, so a reader asking for a close of record that was never
    tagged is told that.

    The answer does not change with the order. The projection fills promoted columns out of
    ``extra`` and touches neither ``snap_ts`` nor ``close_tag`` nor ``row_kind``, so nothing
    it could have done would have put a tag on a row. What the caller loses is learning that
    the partition was also partial, on a read that was never going to return a table.
    """
    rows = [{**_data("2026-11-27T14:30:00+00:00", CALL), "schema_version": 2}]
    fixture_lake.with_chains("SPY", HALF_DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(NoOptionClose):
        load_chain("SPY", HALF_DAY, lake_root=root)

    with pytest.raises(SnapAbsent):
        load_chain("SPY", HALF_DAY, snap="11:00", lake_root=root)


def test_the_read_honours_the_columns_it_is_given(fixture_lake: FixtureLake):
    """The seam records what the loader asked for, and this checks the asking is honoured.

    Every other test about the two passes replaces ``_read`` with a recorder, so the real
    body runs in none of them. A rewrite of that body that dropped the projection, such as
    a switch to ``ds.dataset(path).to_table(filter=...)``, would return the same rows and
    read the whole partition to do it. Nothing about a returned table would show it.
    """
    root = _lake(fixture_lake)
    path = root / "chains" / "ticker=SPY" / f"date={FULL_DAY}.parquet"

    columns = loader._read(path, columns=["snap_ts", "row_kind"])
    filtered = loader._read(path, filters=ds.field("snap_ts").isin(["2026-09-14T13:30:00+00:00"]))

    assert columns.column_names == ["snap_ts", "row_kind"]
    assert columns.num_rows == pq.ParquetFile(path).metadata.num_rows
    assert _snaps(filtered) == {"2026-09-14T13:30:00+00:00"}
    assert filtered.column_names == pq.read_schema(path).names


# -- resolving the lake root -------------------------------------------------


def _two_lakes(tmp_path: Path) -> tuple[Path, Path]:
    """Two fixture lakes holding the same ticker and session at different contracts.

    One lake would prove nothing here. A test that omits ``lake_root`` and reads the only
    lake on disk passes whether the loader resolved the config or reached for anything
    else that happened to hold the partition. With two, every assertion below names which
    root answered: the configured lake returns ``CALL`` and ``PUT``, and the other returns
    ``STRADDLE`` alone.
    """
    configured = FixtureLake(tmp_path / "configured")
    configured.with_chains("SPY", HALF_DAY, sample_chains_table(HALF_ROWS))
    configured.with_reference("schema_versions", _ledger_table())

    other = FixtureLake(tmp_path / "other")
    other.with_chains("SPY", HALF_DAY, sample_chains_table(OTHER_HALF_ROWS))
    other.with_reference("schema_versions", _ledger_table())

    return configured.build(), other.build()


def test_omitting_the_root_reads_the_configured_lake(tmp_path: Path, monkeypatch):
    """#249 test 1. ``lake_root=None`` resolves the lake ``config.yaml`` names.

    The other lake holds the same ticker and the same session, so a loader falling back to
    the working directory, to a fixture root, or to anything but the config returns either
    nothing or ``STRADDLE``.
    """
    configured, _other = _two_lakes(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, configured)))

    table = load_chain("SPY", HALF_DAY)

    assert table.num_rows == 2
    assert set(table.column("occ_symbol").to_pylist()) == {CALL, PUT}


@pytest.mark.parametrize("spelling", [Path, str])
def test_an_explicit_root_beats_the_configured_one(spelling, tmp_path: Path, monkeypatch):
    """#249 test 2. The argument wins, which is what keeps every test above on a fixture lake.

    The config names the configured lake and the call names the other one, so a loader that
    read the config regardless returns ``CALL`` and ``PUT`` instead.

    Both spellings the signature admits are driven, because every other call in this file
    passes a ``Path``. A resolution that honoured only a ``Path`` would discard a ``str``
    root and answer the configured lake, and a resolution that dropped its ``Path()``
    coercion would take a ``str`` as far as a ``TypeError`` inside ``_spelled_exactly``.
    Neither is visible from a suite that never spells one.
    """
    configured, other = _two_lakes(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, configured)))

    table = load_chain("SPY", HALF_DAY, lake_root=spelling(other))

    assert table.num_rows == 1
    assert set(table.column("occ_symbol").to_pylist()) == {STRADDLE}


@pytest.mark.parametrize(
    "root",
    [
        pytest.param("nonexistent", id="a-root-that-holds-nothing"),
        pytest.param("", id="the-empty-string"),
    ],
)
def test_an_explicit_root_is_used_as_given_rather_than_fallen_back_from(
    root: str, tmp_path: Path, monkeypatch
):
    """An explicit root that answers nothing is a refusal, never a read of the configured lake.

    This is the half of "an explicit root wins" the tests above cannot reach, because each
    of them passes a root that does hold the partition. A resolution that fell back to the
    config whenever the explicit root was missing, or empty, or merely falsy would pass
    every one of them and would hand a caller who mistyped a root a read of the operator's
    production lake. The configured lake here does hold this ticker-day, so a fallback
    returns rows rather than raising.

    The empty string is the same rule asked about truthiness rather than existence.
    ``Path("")`` is ``Path(".")``, which is truthy, so the literal empty string is the one
    value that separates ``lake_root is None`` from ``if not lake_root``.
    """
    configured, _other = _two_lakes(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, configured)))
    explicit = str(tmp_path / root) if root else root

    with pytest.raises(PartitionAbsent):
        load_chain("SPY", HALF_DAY, lake_root=explicit)


def test_an_unconfigured_machine_raises_config_error_naming_the_file():
    """#249 test 3. A missing config says the machine is unconfigured.

    That is a different answer from this read finding nothing, so ``ConfigError`` escapes
    rather than folding into ``LoadError``, for the reason ``ManifestError`` and
    ``ExtraProjectionError`` already escape. A ``LoadError`` here would tell a caller their
    ticker-day is absent from a lake that was never named.

    This is the one test here that sets no ``MARKETLAKE_CONFIG``. ``load_config`` reads
    that variable ahead of the default, so on a machine whose shell exported one this read
    would resolve whatever it names, the operator's real ``config.yaml`` included, and the
    error would then name a different file than the first assertion below expects. What
    stops that is conftest deleting an inherited value, and what checks the deletion on a
    machine that exported nothing is
    ``tests/component/test_suite_config_dir_redirect.py``, which spawns a child that did.

    The second assertion says the file this did name is a throwaway rather than anything
    under the real config directory. It reads a constant ``lake.config`` binds at import,
    so it covers conftest's config-directory redirect rather than the resolution this test
    is about.
    """
    with pytest.raises(ConfigError) as caught:
        load_chain("SPY", HALF_DAY)

    assert str(DEFAULT_CONFIG_PATH) in str(caught.value)
    assert not is_protected(DEFAULT_CONFIG_PATH)


def test_the_old_positional_root_is_a_type_error_at_the_call(tmp_path: Path, monkeypatch):
    """#249 test 4. The root is keyword-only, so the replaced call shape fails where written.

    The call this replaced was ``load_chain(root, ticker, day, snap)``. Spelled to four
    positional arguments it is the shape a fourth positional root would still accept, and
    ``load_chain``'s own docstring works through what such a call would read.

    #249 asked for this test against the three-argument spelling, and running both
    signatures against the same call refuted that. ``ticker``, ``day``, and ``snap`` take
    three positional arguments between them either way, so ``load_chain(root, "SPY", day)``
    binds the root to ``ticker`` and reads the configured lake under a keyword-only root
    and under a fourth positional one alike. The fourth argument is where the two differ,
    so that is the call this test writes.

    The match is on the arity, because a bare ``TypeError`` is also what an unrelated
    signature change raises. Making ``snap`` keyword-only would satisfy an unmatched
    ``pytest.raises`` here for the wrong reason, which the test below refuses separately.
    """
    configured, other = _two_lakes(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, configured)))

    with pytest.raises(TypeError, match="positional argument"):
        load_chain(other, "SPY", HALF_DAY, "13:15")  # type: ignore[arg-type]


def test_snap_stays_the_third_positional_argument(tmp_path: Path, monkeypatch):
    """The published call shape spells ``snap`` positionally, so one call here does too.

    ``docs/design.md`` and #135 both write ``load_chain(ticker, date, snap=None)``, which
    makes the third positional slot a contract rather than an accident of the signature.
    Every other call in this file, all fifty-odd of them, passes ``snap`` as a keyword, so
    moving the ``*`` up one line and making ``snap`` keyword-only would leave the whole
    suite green while breaking the shape the design publishes.
    """
    configured, _other = _two_lakes(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV, str(write_config(tmp_path, configured)))

    table = load_chain("SPY", HALF_DAY, "09:30", lake_root=configured)

    assert _snaps(table) == {"2026-11-27T14:30:00+00:00"}
