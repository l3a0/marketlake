"""``list_chain_cycles`` against a fixture lake on disk.

The resolve pass on its own. A reader walking a session cycle by cycle has to know which
cycles the session holds, and the two ways to find out without this are both wrong. Asking
``load_chain`` once per minute of a calendar grid costs a read per minute no cycle ran in,
and reading the partition's ``snap_ts`` column directly is the second read path that skips
the quarantine exclusion, which is the whole reason #137 sits in slice 4.

One rule here is worth a test of its own rather than a line in a docstring. ``snap`` is a
wall-clock minute read against the session date, so a cycle whose Eastern instant falls on
another date has no ``HH:MM`` that names it and ``load_chain`` cannot return it. Onboarding
makes that real: it journals its first chain snapshot under the session date at whatever
hour it runs, and the live lake holds one stamped 03:25Z under ``date=2026-09-16``, which
is 23:25 Eastern on 2026-09-15.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake.loader import PartitionAbsent, PartitionQuarantined, list_chain_cycles, load_chain
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from tests.support.lake import FixtureLake, sample_chains_table

DAY = "2026-09-14"
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def ledger_table() -> pa.Table:
    """The schema-version ledger. ``list_chain_cycles`` never reads it, and that is the point.

    The listing resolves and does not fetch, so it runs no overflow projection and asks the
    ledger nothing. It is written here anyway, because the one test below that resolves a
    listed minute back through ``load_chain`` does need it, and a fixture lake that differs
    between tests hides which read needed what.
    """
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def data(snap: str, occ: str = "SPY   260918C00650000", row_kind: str = "data") -> dict:
    return {
        "snap_ts": snap,
        "fetch_ts": snap,
        "vendor_quote_ts": snap,
        "ticker": "SPY",
        "occ_symbol": occ,
        "bid": 1.0,
        "ask": 1.1,
        "last": 1.05,
        "open_interest": 10,
        "row_kind": row_kind,
        "error_class": None,
        "suspect": False,
        "close_tag": None,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
    }


def lake(fixture_lake: FixtureLake, rows: list[dict]) -> Path:
    fixture_lake.with_chains("SPY", DAY, sample_chains_table(rows))
    fixture_lake.with_reference("schema_versions", ledger_table())
    return fixture_lake.build()


def test_the_minutes_come_back_in_instant_order(fixture_lake: FixtureLake):
    """Written out of order on purpose. Nothing here rides on the writer's row order."""
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T16:15:00-04:00"),
            data("2026-09-14T09:30:00-04:00"),
            data("2026-09-14T13:00:00-04:00"),
        ],
    )

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ("09:30", "13:00", "16:15")


def test_two_spellings_of_one_instant_are_one_minute(fixture_lake: FixtureLake):
    """SPY's real 2026-09-11 partition holds 408 texts naming 406 instants.

    Two of its minutes were written both as an Eastern offset and as ``+00:00``. A listing
    that deduplicated on the text would offer the same minute twice and the walk would
    read it twice.
    """
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T09:30:00-04:00"),
            data("2026-09-14T13:30:00+00:00"),
            data("2026-09-14T09:31:00-04:00"),
        ],
    )

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ("09:30", "09:31")


def test_a_gap_only_minute_is_not_a_cycle(fixture_lake: FixtureLake):
    """A gap row records a minute that was missed. It is not a cycle to compare against."""
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T09:30:00-04:00"),
            data("2026-09-14T09:31:00-04:00", row_kind="gap"),
        ],
    )

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ("09:30",)


def test_a_session_of_gap_rows_lists_nothing(fixture_lake: FixtureLake):
    """The four partitions of the lake's token-expiry gap are exactly this shape."""
    root = lake(fixture_lake, [data("2026-09-14T09:30:00-04:00", row_kind="gap")])

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ()


def test_a_cycle_stamped_on_another_date_is_not_listed(fixture_lake: FixtureLake):
    """Onboarding's shape. 03:25Z under this session date is 23:25 Eastern the day before.

    No ``HH:MM`` on 2026-09-14 names that instant, so ``load_chain`` cannot return it and
    listing it would hand a walker a minute that resolves to a different cycle or to
    nothing at all.
    """
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T03:25:00+00:00"),
            data("2026-09-14T13:30:00+00:00"),
        ],
    )

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ("09:30",)


def test_an_unreadable_stamp_is_skipped_rather_than_refusing_the_session(
    fixture_lake: FixtureLake,
):
    """A stamp with no offset names no instant, and the rest of the session still answers."""
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T09:30:00"),
            data("2026-09-14T09:31:00-04:00"),
        ],
    )

    assert list_chain_cycles("SPY", DAY, lake_root=root) == ("09:31",)


def test_an_absent_partition_refuses(fixture_lake: FixtureLake):
    root = lake(fixture_lake, [data("2026-09-14T09:30:00-04:00")])

    with pytest.raises(PartitionAbsent):
        list_chain_cycles("SPY", "2026-09-15", lake_root=root)


def test_a_quarantined_partition_is_withheld_and_the_opt_in_reads_it(
    fixture_lake: FixtureLake,
):
    """The same guard every other read in the layer makes, rather than a second path."""
    fixture_lake.with_chains("SPY", DAY, sample_chains_table([data("2026-09-14T09:30:00-04:00")]))
    fixture_lake.with_reference("schema_versions", ledger_table())
    fixture_lake.with_quarantine(
        {
            "partition": f"chains/ticker=SPY/date={DAY}.parquet",
            "quarantined": True,
            "reason": "row count out of band",
            "at": datetime(2026, 9, 15, 12, 0, tzinfo=UTC).isoformat(),
        }
    )
    root = fixture_lake.build()

    with pytest.raises(PartitionQuarantined):
        list_chain_cycles("SPY", DAY, lake_root=root)
    assert list_chain_cycles("SPY", DAY, lake_root=root, include_quarantined=True) == ("09:30",)


def test_every_listed_minute_resolves_back_through_load_chain(fixture_lake: FixtureLake):
    """The contract that makes the listing useful rather than merely informative."""
    root = lake(
        fixture_lake,
        [
            data("2026-09-14T09:30:00-04:00"),
            data("2026-09-14T13:31:00+00:00"),
            data("2026-09-14T16:15:00-04:00"),
        ],
    )

    minutes = list_chain_cycles("SPY", DAY, lake_root=root)

    assert minutes == ("09:30", "09:31", "16:15")
    for minute in minutes:
        assert load_chain("SPY", DAY, minute, lake_root=root).num_rows == 1
