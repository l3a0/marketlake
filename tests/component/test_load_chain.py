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

The numbered tests below are #135's own test list, kept in that issue's numbering so a
mutation the issue names points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

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
    """Test 1. The close of record comes from the tag, not from a clock.

    The half day's option close is 13:15 ET, so a loader computing the regular 16:15
    finds no cycle at all, and one taking the day's last snapshot takes 13:00 ET's
    ``spot_close`` rows instead.
    """
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", HALF_DAY)

    assert table.num_rows == 2
    assert _snaps(table) == {"2026-11-27T18:15:00+00:00"}
    assert set(table.column("close_tag").to_pylist()) == {"option_close"}


def test_snap_none_takes_the_option_close_cycle_and_not_its_neighbour(fixture_lake: FixtureLake):
    """Test 1, on a full session. The ``spot_close`` cycle 15 minutes earlier stays out."""
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY)

    assert table.num_rows == 2
    assert _snaps(table) == {"2026-09-14T16:15:00-04:00"}
    assert set(table.column("occ_symbol").to_pylist()) == {CALL, PUT}


def test_a_session_with_no_option_close_tag_raises(fixture_lake: FixtureLake):
    """Test 2. The explicit marker, never a substitute.

    QQQ's day ran cycles to 15:59 ET and tagged none of them. The last snapshot is
    exactly what a substituting loader would return, so it is present to be returned.
    """
    root = _lake(fixture_lake)

    with pytest.raises(NoOptionClose) as caught:
        load_chain(root, "QQQ", FULL_DAY)

    assert caught.value.tagged_gaps == 0
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
        load_chain(root, "SPY", FULL_DAY)

    assert caught.value.tagged_gaps == 1


# -- resolving an intraday minute --------------------------------------------


def test_snap_resolves_as_an_eastern_wall_clock_minute(fixture_lake: FixtureLake):
    """Test 3. ``10:31`` ET is 14:31Z on a September session."""
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY, snap="10:31")

    assert set(table.column("occ_symbol").to_pylist()) == {CALL, PUT, STRADDLE}
    assert {_instant(text) for text in _snaps(table)} == {datetime(2026, 9, 14, 14, 31, tzinfo=UTC)}


def test_one_instant_under_two_spellings_comes_back_whole(fixture_lake: FixtureLake):
    """Test 3, on the shape the live lake actually carries.

    SPY's real 2026-09-11 partition writes two of its minutes both as an Eastern offset
    and as ``+00:00``. A loader matching the stored text returns whichever spelling it
    happened to ask for and silently drops the rest of the minute.
    """
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY, snap="10:31")

    assert _snaps(table) == {"2026-09-14T14:31:00+00:00", "2026-09-14T10:31:00-04:00"}
    assert table.num_rows == 3


def test_snap_is_never_read_as_utc(fixture_lake: FixtureLake):
    """Test 3, refuting the other reading. ``14:31`` ET is 18:31Z, and 14:31Z is a decoy.

    The session holds a cycle at both minutes, so a loader treating ``snap`` as UTC
    returns rows rather than raising, which is the failure this refutes.
    """
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY, snap="14:31")

    assert _snaps(table) == {"2026-09-14T18:31:00+00:00"}


def test_a_minute_no_cycle_recorded_raises(fixture_lake: FixtureLake):
    """Test 4. An empty table would read as a chain with no contracts."""
    root = _lake(fixture_lake)

    with pytest.raises(SnapAbsent) as caught:
        load_chain(root, "SPY", FULL_DAY, snap="11:00")

    assert caught.value.snap == "11:00"
    assert caught.value.tagged_gaps == 0


def test_a_minute_that_ran_and_failed_counts_its_gap_rows(fixture_lake: FixtureLake):
    """A minute that was attempted reads differently from one that never was.

    Both raise, because neither produced marks. The count is what tells them apart, the
    same way it does for the close of record.
    """
    root = _lake(fixture_lake)

    with pytest.raises(SnapAbsent) as caught:
        load_chain(root, "SPY", FULL_DAY, snap="15:00")

    assert caught.value.tagged_gaps == 1


def test_the_offset_comes_from_the_zone_and_not_from_a_constant(fixture_lake: FixtureLake):
    """A winter minute lands on the offset its session actually ran under.

    Every other snap test reads the September session, which is Eastern Daylight Time. A
    loader carrying a fixed minus four would answer those and quietly miss by an hour on
    the November one, where the same wall clock is minus five.
    """
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", HALF_DAY, snap="13:15")

    assert _snaps(table) == {"2026-11-27T18:15:00+00:00"}


@pytest.mark.parametrize("snap", ["10:31:00", "9:31", "24:00", "abc", ""])
def test_a_snap_outside_hh_mm_is_refused(fixture_lake: FixtureLake, snap: str):
    """A minute the loader cannot read is a caller error, not an absent cycle.

    It is both a ``ValueError`` and a ``LoadError``, so the one exception the docstring
    tells a caller to catch really does cover every way a read resolves to no table.
    """
    root = _lake(fixture_lake)

    with pytest.raises(SnapMalformed) as caught:
        load_chain(root, "SPY", FULL_DAY, snap=snap)

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

    assert load_chain(root, "SPY", FULL_DAY, snap="10:31").num_rows == 3

    with pytest.raises(SnapAbsent) as caught:
        load_chain(root, "SPY", FULL_DAY, snap="11:00")
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
        load_chain(root, "SPY", FULL_DAY, snap="11:00")

    assert caught.value.unreadable == ("'2026-09-14T11:00:00'",)


# -- gap rows ----------------------------------------------------------------


def test_gap_rows_are_absent_from_the_close_of_record(fixture_lake: FixtureLake):
    """Test 5. The option-close cycle carries a gap row beside its two data rows."""
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY)

    assert _kinds(table) == {"data"}
    assert table.num_rows == 2


def test_gap_rows_are_absent_from_an_intraday_minute(fixture_lake: FixtureLake):
    """Test 5, on the other resolution. 14:31 ET carries a gap row too."""
    root = _lake(fixture_lake)

    table = load_chain(root, "SPY", FULL_DAY, snap="14:31")

    assert _kinds(table) == {"data"}
    assert table.num_rows == 2


# -- quarantine --------------------------------------------------------------


def test_a_quarantined_partition_is_excluded_by_default(fixture_lake: FixtureLake):
    """Test 6, first half. Fail closed for data already sealed."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    with pytest.raises(PartitionQuarantined) as caught:
        load_chain(root, "SPY", FULL_DAY)

    assert caught.value.partition == FULL_PARTITION
    assert caught.value.entry["verdict"] == "delayed_feed"


def test_a_quarantined_partition_reads_under_the_opt_in(fixture_lake: FixtureLake):
    """Test 6, second half. The opt-in is explicit and per call."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    table = load_chain(root, "SPY", FULL_DAY, include_quarantined=True)

    assert table.num_rows == 2


def test_quarantine_covers_only_the_partition_it_names(fixture_lake: FixtureLake):
    """A verdict on one ticker-day leaves every other one readable."""
    root = _lake(
        fixture_lake,
        quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed_feed"}],
    )

    assert load_chain(root, "SPY", HALF_DAY).num_rows == 2


def test_a_superseding_clean_verdict_un_quarantines(fixture_lake: FixtureLake):
    """Last entry per partition wins, so un-quarantine is an entry rather than a deletion."""
    root = _lake(
        fixture_lake,
        quarantine=[
            {"partition": FULL_PARTITION, "verdict": "delayed_feed"},
            {"partition": FULL_PARTITION, "verdict": "clean"},
        ],
    )

    assert load_chain(root, "SPY", FULL_DAY).num_rows == 2


def test_an_entry_the_reader_cannot_recognise_excludes(fixture_lake: FixtureLake):
    """Fail closed for data already sealed means an unreadable verdict refuses.

    #139 owns the entry shape and has not been built, so a writer can land a spelling this
    read does not know. Admitting one would fail open on exactly the partition the guard
    exists to withhold.
    """
    root = _lake(fixture_lake, quarantine=[{"partition": FULL_PARTITION, "note": "unreadable"}])

    with pytest.raises(PartitionQuarantined):
        load_chain(root, "SPY", FULL_DAY)


def test_a_ticker_spelled_differently_from_its_directory_is_refused(fixture_lake: FixtureLake):
    """macOS opens ``ticker=spy`` against ``ticker=SPY``, and the ledger key would miss.

    The quarantine lookup keys on the path as the caller spelled it. A read under the
    wrong case would open a real partition and find no verdict for it, which turns the
    guard from fail closed into fail open.
    """
    root = _lake(fixture_lake, quarantine=[{"partition": FULL_PARTITION, "verdict": "delayed"}])

    with pytest.raises(PartitionAbsent):
        load_chain(root, "spy", FULL_DAY)


def test_an_absent_quarantine_ledger_excludes_nothing(fixture_lake: FixtureLake):
    """Test 7. The guard is inert until the battery writes its first verdict."""
    root = _lake(fixture_lake)

    assert not (root / "quarantine.jsonl").exists()
    assert load_chain(root, "SPY", FULL_DAY).num_rows == 2


# -- the overflow projection -------------------------------------------------


def test_a_version_the_ledger_has_no_shape_for_reads_partial(fixture_lake: FixtureLake):
    """A read the projection could not complete never comes back looking whole.

    An absent schema-version ledger is that condition reached the shortest way. Every
    version in the partition is then one the ledger holds no shape for, so nothing can
    say whether a value in the overflow belongs in a column.
    """
    root = _lake(fixture_lake, ledger=False)

    with pytest.raises(PartialRead) as caught:
        load_chain(root, "SPY", FULL_DAY)

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

    table = load_chain(root, "SPY", HALF_DAY)

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
        load_chain(root, "SPY", HALF_DAY)

    assert [column.column for column in caught.value.projection.retyped] == ["volume"]


def test_a_value_the_promoted_column_refuses_reads_partial(fixture_lake: FixtureLake):
    """The second of the three conditions the owner comment calls partial.

    Only the unrecorded-version condition had a test, so narrowing the check to that one
    condition went unnoticed. A refused value means the read is partial too.
    """
    rows = [_with_extra(row, {"totalVolume": "not-a-number"}) for row in HALF_ROWS]
    root = _lake_missing_column(fixture_lake, rows, "volume")

    with pytest.raises(PartialRead) as caught:
        load_chain(root, "SPY", HALF_DAY)

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

    early = load_chain(root, "SPY", HALF_DAY, snap="09:30")
    close = load_chain(root, "SPY", HALF_DAY)

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
        load_chain(root, "SPY", HALF_DAY)


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
        load_chain(root, "SPY", HALF_DAY)


# -- the partition itself ----------------------------------------------------


def test_an_unsealed_day_raises(fixture_lake: FixtureLake):
    """A session with no sealed partition is not an empty chain."""
    root = _lake(fixture_lake)

    with pytest.raises(PartitionAbsent):
        load_chain(root, "SPY", "2026-09-15")


def test_a_directory_where_the_partition_belongs_is_not_a_partition(fixture_lake: FixtureLake):
    """A sealed partition is one file. A directory in its place is not one."""
    root = _lake(fixture_lake)
    (root / "chains" / "ticker=SPY" / "date=2026-09-15.parquet").mkdir()

    with pytest.raises(PartitionAbsent):
        load_chain(root, "SPY", "2026-09-15")


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
        load_chain(root, "SPY", FULL_DAY)

    assert caught.value.projection.unrecorded_versions == (1,)


def test_a_date_object_reads_the_same_partition_as_its_iso_text(fixture_lake: FixtureLake):
    """The signature takes either, so both have to land on one path."""
    root = _lake(fixture_lake)

    assert load_chain(root, "SPY", date(2026, 11, 27)).num_rows == 2
