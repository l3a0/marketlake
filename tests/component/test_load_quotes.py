"""``load_quotes`` against a fixture lake on disk.

``load_quotes`` shares its whole body with ``load_chain`` through ``loader._load_surface``,
per marketlake #261. That machinery, the two-pass read, the quarantine guard, the overflow
projection, and the ET-minute and instant-comparison resolution, is already exercised
against the chains surface in ``tests/component/test_load_chain.py``. This file exercises
only what differs between the two: which tag the close of record resolves against, the
exception raised when that tag is absent, and that a quotes-shaped refusal names the
quotes partition rather than the chains one.

The fixture partition below carries the shape a sealed quotes partition actually has: an
untagged intraday cycle, a ``spot_close`` cycle, and an ``option_close`` cycle, because the
underlying is captured in both close cycles. A loader that inherited the chains tag would
pass every test in ``test_load_chain.py`` and still answer ``load_quotes(snap=None)`` with
the wrong minute, which is why ``tests/support/lake.py``'s default carries both tags rather
than one.

The numbered tests carry the numbering marketlake #261 asks for, so a mutation the issue
names points at the test the issue names.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from lake.loader import (
    NoOptionClose,
    NoSpotClose,
    PartitionAbsent,
    PartitionQuarantined,
    SnapAbsent,
    _instant,
    load_chain,
    load_quotes,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

FULL_DAY = "2026-09-14"

# When the schema-version ledger recorded version 1. Any instant does, since the loader
# reads the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

QUOTES_PARTITION = f"quotes/ticker=SPY/date={FULL_DAY}.parquet"


def _quote(
    snap: str | None,
    close_tag: str | None = None,
    *,
    bid: float = 649.98,
    ask: float = 650.02,
    last: float = 650.00,
) -> dict:
    """One quotes data row: the underlying's top of book at ``snap``."""
    return {
        "snap_ts": snap,
        "fetch_ts": _stamped(snap, "300"),
        "vendor_quote_ts": _stamped(snap, "150"),
        "ticker": "SPY",
        "bid": bid,
        "ask": ask,
        "last": last,
        "row_kind": "data",
        "error_class": None,
        "suspect": False,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
    }


def _stamped(snap: str | None, millis: str) -> str | None:
    if snap is None:
        return None
    instant, offset = snap[:19], snap[19:]
    return f"{instant}.{millis}{offset}"


def _gap(snap: str, close_tag: str | None = None) -> dict:
    """One gap row: a minute that was attempted and missed, every vendor column null."""
    row = _quote(snap, close_tag=close_tag)
    row.update(
        {"bid": None, "ask": None, "last": None, "row_kind": "gap", "error_class": "vendor_timeout"}
    )
    return row


def _chain_row(snap: str, close_tag: str | None = None) -> dict:
    """One chains data row, for the one test here that reads the sibling surface too."""
    return {
        "snap_ts": snap,
        "fetch_ts": _stamped(snap, "400"),
        "vendor_quote_ts": _stamped(snap, "150"),
        "ticker": "SPY",
        "occ_symbol": "SPY   260918C00650000",
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


# The full session: an untagged intraday cycle with a gap beside it, and both close
# cycles, each with a gap beside it too. Both close tags sit on one partition, because
# that is what a quotes partition actually carries.
FULL_ROWS = [
    _quote("2026-09-14T13:30:00+00:00"),
    _quote("2026-09-14T14:31:00+00:00"),
    _gap("2026-09-14T14:31:00+00:00"),
    _quote(
        "2026-09-14T20:00:00+00:00", close_tag="spot_close", bid=649.90, ask=649.95, last=649.92
    ),
    _gap("2026-09-14T20:00:00+00:00", close_tag="spot_close"),
    _quote(
        "2026-09-14T20:15:00+00:00", close_tag="option_close", bid=650.10, ask=650.15, last=650.12
    ),
    _gap("2026-09-14T20:15:00+00:00", close_tag="option_close"),
]

# A session whose rows carry no ``spot_close`` tag. The last snapshot of the day and an
# ``option_close``-tagged row are both present, so both are available to be wrongly
# returned by a loader that substitutes the last snapshot or inherits the chains tag.
NO_SPOT_CLOSE_ROWS = [
    _quote("2026-09-14T13:30:00+00:00"),
    _quote("2026-09-14T19:59:00+00:00"),
    _quote("2026-09-14T20:15:00+00:00", close_tag="option_close"),
]


def _ledger_table():
    """The schema-version ledger recording version 1 at the shape the running code writes."""
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _lake(
    fixture_lake: FixtureLake, rows: list[dict], *, quarantine: list[dict] | None = None
) -> object:
    fixture_lake.with_quotes("SPY", FULL_DAY, sample_quotes_table(rows))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    return fixture_lake.build()


def _with_extra(row: dict, overflow: dict) -> dict:
    """``row`` with ``overflow`` JSON-encoded into its ``extra`` column."""
    return {**row, "extra": json.dumps(overflow)}


def _lake_missing_column(fixture_lake: FixtureLake, rows: list[dict], column: str):
    """A lake whose ledger records version 1 without ``column`` on the quotes surface.

    That is what history looks like below a promotion. The version carried no column for
    the measurement, so its value sits in the overflow and the projection is what presents
    it as the column the running schema gives it.
    """
    fingerprints = {
        surface: {name: kind for name, kind in columns.items() if name != column}
        if surface == "quotes"
        else dict(columns)
        for surface, columns in running_fingerprints().items()
    }
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=fingerprints)
    fixture_lake.with_quotes("SPY", FULL_DAY, sample_quotes_table(rows))
    fixture_lake.with_reference("schema_versions", SchemaVersionLedger([entry]).to_table())
    return fixture_lake.build()


def _tags(table) -> set[str | None]:
    return set(table.column("close_tag").to_pylist())


def _kinds(table) -> set[str]:
    return set(table.column("row_kind").to_pylist())


# -- resolving the close of record: the tag differs from load_chain's ------------------


def test_snap_none_returns_the_spot_close_row_and_not_the_option_close_one(
    fixture_lake: FixtureLake,
):
    """#261 test 1. The equity close of record is tagged, never inherited from chains.

    The partition carries both close tags, the way a sealed quotes partition does, so a
    loader that inherited ``load_chain``'s ``option_close`` tag would return the 16:15 row
    for a caller asking for the equity close, with nothing to say it did.
    """
    root = _lake(fixture_lake, FULL_ROWS)

    table = load_quotes("SPY", FULL_DAY, lake_root=root)

    assert table.num_rows == 1
    assert _tags(table) == {"spot_close"}
    assert table.column("bid").to_pylist() == [649.90]


def test_a_session_with_no_spot_close_tag_raises_the_marker_not_a_substitute(
    fixture_lake: FixtureLake,
):
    """#261 test 2. Neither the last snapshot nor the ``option_close`` row stands in.

    Both are present on this session, which is what confirms the marker is explicit
    rather than the read merely finding nothing at all.
    """
    root = _lake(fixture_lake, NO_SPOT_CLOSE_ROWS)

    with pytest.raises(NoSpotClose) as caught:
        load_quotes("SPY", FULL_DAY, lake_root=root)

    assert caught.value.tagged_gaps == 0
    assert caught.value.close_tag == "spot_close"
    assert caught.value.ticker == "SPY"
    assert "spot_close" in str(caught.value)
    assert not isinstance(caught.value, NoOptionClose)


# -- resolving an intraday minute -------------------------------------------------------


def test_snap_resolves_as_an_eastern_wall_clock_minute(fixture_lake: FixtureLake):
    """#261 test 3. ``10:31`` ET is 14:31Z on a September session."""
    root = _lake(fixture_lake, FULL_ROWS)

    table = load_quotes("SPY", FULL_DAY, snap="10:31", lake_root=root)

    assert table.num_rows == 1
    assert {_instant(text) for text in table.column("snap_ts").to_pylist()} == {
        datetime(2026, 9, 14, 14, 31, tzinfo=UTC)
    }


def test_a_minute_no_cycle_recorded_raises(fixture_lake: FixtureLake):
    """#261 test 4. An empty table would read as a quiet quote of nothing."""
    root = _lake(fixture_lake, FULL_ROWS)

    with pytest.raises(SnapAbsent) as caught:
        load_quotes("SPY", FULL_DAY, snap="11:00", lake_root=root)

    assert caught.value.snap == "11:00"


# -- gap rows -----------------------------------------------------------------------------


def test_gap_rows_are_absent_from_the_close_of_record(fixture_lake: FixtureLake):
    """#261 test 5. The equity-close cycle carries a gap row beside its data row."""
    root = _lake(fixture_lake, FULL_ROWS)

    table = load_quotes("SPY", FULL_DAY, lake_root=root)

    assert _kinds(table) == {"data"}
    assert table.num_rows == 1


def test_gap_rows_are_absent_from_an_intraday_minute(fixture_lake: FixtureLake):
    """#261 test 5, on the other resolution. 10:31 ET carries a gap row too."""
    root = _lake(fixture_lake, FULL_ROWS)

    table = load_quotes("SPY", FULL_DAY, snap="10:31", lake_root=root)

    assert _kinds(table) == {"data"}
    assert table.num_rows == 1


# -- quarantine ---------------------------------------------------------------------------


def test_a_quarantined_quotes_partition_is_excluded_by_default(fixture_lake: FixtureLake):
    """#261 test 6, first half. The quotes path is guarded the same way the chains one is."""
    root = _lake(
        fixture_lake,
        FULL_ROWS,
        quarantine=[{"partition": QUOTES_PARTITION, "verdict": "delayed_feed"}],
    )

    with pytest.raises(PartitionQuarantined) as caught:
        load_quotes("SPY", FULL_DAY, lake_root=root)

    assert caught.value.partition == QUOTES_PARTITION


def test_a_quarantined_quotes_partition_reads_under_the_opt_in(fixture_lake: FixtureLake):
    """#261 test 6, second half. The opt-in is explicit and per call."""
    root = _lake(
        fixture_lake,
        FULL_ROWS,
        quarantine=[{"partition": QUOTES_PARTITION, "verdict": "delayed_feed"}],
    )

    table = load_quotes("SPY", FULL_DAY, lake_root=root, include_quarantined=True)

    assert table.num_rows == 1


# -- the overflow projection reads the quotes surface ------------------------------------


def test_the_promotion_reads_off_the_quotes_overflow_map_not_the_chains_one(
    fixture_lake: FixtureLake,
):
    """The projection is called with ``surface=QUOTES``, not the chains constant.

    ``realtime`` is a quotes-only column, read off the vendor's per-symbol envelope, and
    it has no chains counterpart at all. A read that projected the chains surface would
    never look for it, so the value would stay behind in the overflow instead of being
    lifted into its column. It carries no column for it, so the value sits in ``extra``
    under quotes' own nesting, ``envelope``, the way ``journal.extra_paths`` places it.
    """
    rows = [
        _with_extra(
            _quote("2026-09-14T20:00:00+00:00", close_tag="spot_close"),
            {"envelope": {"realtime": True}},
        )
    ]
    root = _lake_missing_column(fixture_lake, rows, "realtime")

    table = load_quotes("SPY", FULL_DAY, lake_root=root)

    assert "realtime" not in sample_quotes_table(rows).column_names
    assert table.column("realtime").to_pylist() == [True]


# -- the partition itself ------------------------------------------------------------------


def test_an_absent_partition_names_the_quotes_path_not_a_chains_one(fixture_lake: FixtureLake):
    """#261 test 7. The refusal points at the surface actually asked for."""
    root = _lake(fixture_lake, FULL_ROWS)

    with pytest.raises(PartitionAbsent) as caught:
        load_quotes("SPY", "2026-09-15", lake_root=root)

    message = str(caught.value)
    assert "quotes/ticker=SPY/date=2026-09-15.parquet" in message
    assert "chains" not in message


def test_a_date_object_reads_the_same_partition_as_its_iso_text(fixture_lake: FixtureLake):
    """The signature takes either, so both have to land on one path."""
    root = _lake(fixture_lake, FULL_ROWS)

    assert load_quotes("SPY", date(2026, 9, 14), lake_root=root).num_rows == 1


# -- the two loaders never share a close tag ------------------------------------------------


def test_the_two_loaders_do_not_share_a_close_tag(fixture_lake: FixtureLake):
    """#261 test 8. A chains read still resolves ``option_close`` on a partition tagged both.

    Both surfaces are captured every cycle, including the close cycles, so a chains
    partition legitimately carries a ``spot_close``-tagged row too. This is what makes the
    two loaders resolving different tags a fact worth pinning down: swapping either tag
    would still find a row to return, just the wrong one.
    """
    chain_rows = [
        _chain_row("2026-09-14T20:00:00+00:00", close_tag="spot_close"),
        _chain_row("2026-09-14T20:15:00+00:00", close_tag="option_close"),
    ]
    fixture_lake.with_chains("SPY", FULL_DAY, sample_chains_table(chain_rows))
    root = _lake(fixture_lake, FULL_ROWS)

    chains_table = load_chain("SPY", FULL_DAY, lake_root=root)
    quotes_table = load_quotes("SPY", FULL_DAY, lake_root=root)

    assert _tags(chains_table) == {"option_close"}
    assert _tags(quotes_table) == {"spot_close"}
