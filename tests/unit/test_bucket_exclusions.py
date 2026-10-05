"""The bucket uploader's copy of the backup exclusion list.

``rsync`` applies ``runner.BACKUP_EXCLUSIONS`` to the path form, and
``tests/unit/test_backup_exclusions.py`` covers that side. The bucket uploader walks the
tree itself, so it carries a second implementation of the same rule in
``bucket.rsync_excluded``. Nothing in the rsync tests reaches it. These do.

1. The two shapes the list uses match the way ``rsync`` matches them. A pattern with
   no "/" matches the last component anywhere, and a trailing "/" matches directories
   only.
2. The shapes it does not model refuse rather than match nothing.
3. A temp file and the config directory never reach the bucket, on a real upload.
4. Nothing a real lake holds is dropped.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from lake.bucket import first_upload, rsync_excluded, walk_lake
from lake.calendar import MARKET_TZ
from lake.config import BucketTarget
from lake.paths import CONFIG_DIR_PARTS, CONFIG_FILE, LakePaths, temp_write_path
from tests.support.bucket import FakeS3
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake, sample_chains_table, sample_quotes_table

DAY = date(2026, 8, 24)
CONFIG_DIR = "/".join(CONFIG_DIR_PARTS)
TARGET = BucketTarget(bucket="lake-backup")


# -- 1. the two shapes -----------------------------------------------------------


@pytest.mark.parametrize(
    "rel",
    [
        "chains/ticker=SPY/date=2026-08-24.parquet.tmp-4242",
        "reference/security_master.parquet.tmp-1",
        "x.tmp-9",
    ],
)
def test_a_pattern_with_no_slash_matches_the_last_component_anywhere(rel):
    assert rsync_excluded(rel, is_dir=False)


def test_a_sealed_partition_is_not_matched():
    assert not rsync_excluded("chains/ticker=SPY/date=2026-08-24.parquet", is_dir=False)


def test_a_trailing_slash_pattern_matches_the_directory_and_not_a_file_of_that_name():
    assert rsync_excluded(CONFIG_DIR, is_dir=True)
    assert rsync_excluded(f"nested/{CONFIG_DIR}", is_dir=True)
    assert not rsync_excluded(CONFIG_DIR, is_dir=False)


def test_a_pattern_with_a_slash_matches_only_the_end_of_the_path():
    assert not rsync_excluded(f"{CONFIG_DIR}/deeper", is_dir=True)
    assert not rsync_excluded(CONFIG_DIR_PARTS[-1], is_dir=True)


# -- 2. shapes it does not model -------------------------------------------------


@pytest.mark.parametrize("pattern", ["/journal/", "**/x", "/"])
def test_an_unmodelled_shape_refuses_rather_than_matching_nothing(pattern):
    with pytest.raises(ValueError, match="does not model"):
        rsync_excluded("journal/a", is_dir=False, patterns=(pattern,))


# -- 3. on a real upload ---------------------------------------------------------


def _full_lake(root):
    lake = (
        FixtureLake(root)
        .with_chains("SPY", DAY)
        .with_quotes("SPY", DAY)
        .with_bars("SPY", "1m", DAY, sample_quotes_table())
        .with_reference("security_master", sample_chains_table())
        .with_journal_segment(
            "chains", "SPY", DAY, sample_chains_table(), start_ts="20260824T133000Z", pid=4242
        )
        .with_quarantine({"partition": "chains/ticker=SPY/date=2026-08-24.parquet"})
        .build()
    )
    report = lake / "reports" / f"date={DAY.isoformat()}.md"
    report.parent.mkdir(parents=True)
    report.write_text("report\n")
    return lake


def _upload(lake):
    client = FakeS3()
    first_upload(
        lake,
        TARGET,
        client=client,
        clock=ManualClock(datetime(2026, 8, 24, 19, 0, tzinfo=MARKET_TZ)),
    )
    return client


def test_a_crashed_writers_temp_file_never_reaches_the_bucket(tmp_path):
    lake = _full_lake(tmp_path / "lake")
    debris = temp_write_path(LakePaths(lake).chains_partition_path("SPY", DAY), 4242)
    debris.write_bytes(b"half a partition")

    client = _upload(lake)

    assert debris.relative_to(lake).as_posix() not in client.keys()
    assert "chains/ticker=SPY/date=2026-08-24.parquet" in client.keys()


def test_the_config_directory_never_reaches_the_bucket(tmp_path):
    lake = _full_lake(tmp_path / "lake")
    config = lake.joinpath(*CONFIG_DIR_PARTS)
    config.mkdir(parents=True)
    (config / "token.json").write_text("{}\n")
    (config / CONFIG_FILE).write_text("bucket_secret_access_key: never\n")

    client = _upload(lake)

    assert not any(key.startswith(CONFIG_DIR) for key in client.keys())
    assert not any(b"never" in client.body(key) for key in client.keys())


# -- 4. nothing a real lake holds is dropped -------------------------------------


def test_the_walk_keeps_every_file_a_real_lake_holds(tmp_path):
    lake = _full_lake(tmp_path / "lake")
    files = sorted(p.relative_to(lake).as_posix() for p in lake.rglob("*") if p.is_file())
    assert len(files) > 6
    assert sorted(rel for rel, _ in walk_lake(lake)) == files
