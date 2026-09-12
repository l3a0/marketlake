"""The security master across one real boundary: a parquet file on disk.

These tests write the master to a throwaway lake and read it back. They cross the
filesystem boundary, so they are component tests. The clock and the vendor stay out
of it entirely.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lake import security_master
from lake.paths import TEMP_MARKER, temp_write_path
from lake.security_master import (
    ID_TYPE_TICKER,
    MASTER_SCHEMA,
    MASTER_SCHEMA_VERSION,
    MasterUnreadable,
    SecurityMaster,
    SecurityMasterError,
    UnknownInstrument,
    UnsupportedSchemaVersion,
    capture_start_in_market_time,
    master_path,
)

EPOCH = datetime(2019, 1, 2, 14, 30, tzinfo=UTC)


def _sample_master() -> SecurityMaster:
    master = SecurityMaster()
    iid = master.register(
        kind="equity",
        capture_start=EPOCH,
        valid_from=date(2012, 5, 18),
        ticker="FB",
        figi="BBG000MM2P62",
    )
    master.remap(iid, ID_TYPE_TICKER, "META", effective=date(2022, 6, 9))
    master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY")
    return master


def test_round_trip_preserves_every_mapping(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    assert path.exists()

    reloaded = SecurityMaster.read(path)
    assert set(reloaded.mappings) == set(master.mappings)


def test_write_lands_at_the_reference_convention_path(lake_root: Path):
    master = _sample_master()
    master.write(master_path(lake_root))
    assert (lake_root / "reference" / "security_master.parquet").exists()


def test_written_file_carries_the_pinned_schema_and_version(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))

    table = pq.read_table(path)
    assert table.schema == MASTER_SCHEMA
    versions = set(table.column("schema_version").to_pylist())
    assert versions == {MASTER_SCHEMA_VERSION}


def test_round_trip_preserves_as_of_resolution(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)

    assert reloaded.resolve("FB", on=date(2020, 1, 2)) == 1
    assert reloaded.resolve("META", on=date(2023, 1, 3)) == 1
    assert reloaded.resolve("FB", on=date(2022, 6, 9)) is None
    assert reloaded.symbol_at(1, on=date(2020, 1, 2)) == "FB"


def test_round_trip_preserves_capture_start_as_utc(lake_root: Path):
    master = _sample_master()
    path = master.write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)
    assert reloaded.capture_start_of(1) == EPOCH


def test_empty_master_round_trips(lake_root: Path):
    path = SecurityMaster().write(master_path(lake_root))
    reloaded = SecurityMaster.read(path)
    assert len(reloaded) == 0


def test_read_rejects_an_unsupported_schema_version(lake_root: Path):
    master = _sample_master()
    table = master.to_table()
    bumped = table.set_column(
        table.schema.get_field_index("schema_version"),
        "schema_version",
        [[MASTER_SCHEMA_VERSION + 1] * table.num_rows],
    )
    path = master_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(bumped, path)

    with pytest.raises(UnsupportedSchemaVersion):
        SecurityMaster.read(path)


# -- the write does not expose a torn master ------------------------------------------


def test_write_goes_through_a_temp_file_and_one_rename(lake_root: Path, monkeypatch):
    """Onboarding writes the master while the daemon and dashboard read it.

    A write straight onto the target truncates it first, so a reader can catch it half
    done. The write instead lands in a temp file beside the target and renames over it in
    one step. This checks that the rename happens, and that the temp path is the one
    ``paths.temp_write_path`` owns, which is the spelling the backup exclusion matches.
    """
    path = master_path(lake_root)
    calls: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def spy(src, dst):
        calls.append((Path(src), Path(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(security_master.os, "replace", spy)
    _sample_master().write(path)

    assert calls == [(temp_write_path(path, os.getpid()), path)]
    assert path.exists()


def test_a_failed_write_leaves_the_prior_master_intact(lake_root: Path, monkeypatch):
    """A write that fails partway must not take the old master with it.

    The write goes to a temp file, so an interrupted write leaves the target untouched and
    removes the temp. A write straight onto the target would truncate it first, so the same
    failure would leave a torn master where a whole one was.
    """
    path = _sample_master().write(master_path(lake_root))
    before = set(SecurityMaster.read(path).mappings)

    def torn_write(table, where, *args, **kwargs):
        Path(where).write_bytes(b"partial, not parquet")
        raise RuntimeError("interrupted mid-write")

    monkeypatch.setattr(security_master.pq, "write_table", torn_write)
    with pytest.raises(RuntimeError):
        SecurityMaster().write(path)

    assert set(SecurityMaster.read(path).mappings) == before, "the prior master was lost"
    leftovers = [p.name for p in path.parent.iterdir() if TEMP_MARKER in p.name]
    assert leftovers == [], f"a temp file was left behind: {leftovers}"


def test_read_folds_a_torn_master_into_a_security_master_error(lake_root: Path):
    """A torn or corrupt master raises the master's own error family, not pyarrow's.

    ``pyarrow`` refuses a torn parquet with ``ArrowInvalid``, which is a ``ValueError`` and
    not a ``SecurityMasterError``. A caller guarding the master's own errors alone would
    let it escape. The daemon's startup reader is exactly such a caller, so ``read`` folds
    the pyarrow error into ``MasterUnreadable`` for every caller at once.
    """
    path = master_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet at all")

    with pytest.raises(MasterUnreadable) as caught:
        SecurityMaster.read(path)
    assert isinstance(caught.value, SecurityMasterError)


# -- the shared capture-start lookup ---------------------------------------------------


def test_the_lookup_answers_in_market_time_not_utc():
    """The epoch is stored in UTC and every caller compares it against market time.

    The conversion lives here so one call site cannot compare across offsets. 14:30 UTC
    is 09:30 in New York on that date, and the difference is five hours, which is enough
    to move a clamp across a session open.
    """
    master = SecurityMaster()
    master.register(kind="equity", capture_start=EPOCH, valid_from=date(2019, 1, 2), ticker="SPY")

    found = capture_start_in_market_time(master, "SPY", date(2019, 1, 2))

    assert found is not None
    assert found == EPOCH, "the same instant"
    assert (found.hour, found.minute) == (9, 30), "read in market time, not 14:30 UTC"
    assert found.utcoffset() != EPOCH.utcoffset()


@pytest.mark.parametrize(
    ("master", "ticker"),
    [(None, "SPY"), (SecurityMaster(), "SPY")],
    ids=["no master at all", "a master that does not carry the ticker"],
)
def test_a_lookup_the_master_cannot_answer_returns_none(master, ticker):
    """``None`` is the no-clamp answer, and it never raises.

    Both callers run from hooks the loop does not guard, under ``KeepAlive``. A raise
    here would relaunch within seconds and repeat, recording nothing.
    """
    assert capture_start_in_market_time(master, ticker, date(2019, 1, 2)) is None


def test_a_master_that_refuses_mid_lookup_returns_none_rather_than_raising():
    """A mapping can name an instrument the master cannot then place.

    A partially written master is the way that happens. The callers run from hooks the
    loop does not guard, so this has to degrade to no clamp rather than to a crash loop
    that records nothing.
    """

    class Refusing:
        def resolve(self, symbol, on, id_type=None):
            return 1

        def capture_start_of(self, instrument_id):
            raise UnknownInstrument(instrument_id)

    assert capture_start_in_market_time(Refusing(), "SPY", date(2019, 1, 2)) is None
