"""Lake paths, built from values alone. They match the fixture-lake builder exactly."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from lake.paths import ACTIONS, BARS, CHAINS, QUOTES, SURFACES, LakePaths
from tests.support.lake import FixtureLake

ROOT = Path("/lake")
DAY = date(2026, 8, 24)


@pytest.fixture
def paths() -> LakePaths:
    return LakePaths(ROOT)


def test_chains_partition_path(paths: LakePaths):
    expected = ROOT / "chains" / "ticker=SPY" / "date=2026-08-24.parquet"
    assert paths.chains_partition_path("SPY", DAY) == expected


def test_quotes_partition_path(paths: LakePaths):
    expected = ROOT / "quotes" / "ticker=QQQ" / "date=2026-08-24.parquet"
    assert paths.quotes_partition_path("QQQ", DAY) == expected


def test_bars_partition_carries_a_freq_level(paths: LakePaths):
    expected = ROOT / "bars" / "ticker=SPY" / "freq=1m" / "date=2026-08-24.parquet"
    assert paths.bars_partition_path("SPY", "1m", DAY) == expected


def test_actions_is_one_flat_all_ticker_file(paths: LakePaths):
    assert paths.actions_path == ROOT / "actions" / "corporate_actions.parquet"


def test_partition_path_rejects_bars_and_actions(paths: LakePaths):
    with pytest.raises(ValueError):
        paths.partition_path(BARS, "SPY", DAY)
    with pytest.raises(ValueError):
        paths.partition_path(ACTIONS, "SPY", DAY)


def test_segment_path(paths: LakePaths):
    seg = paths.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
    expected = (
        ROOT
        / "journal"
        / "date=2026-08-24"
        / "surface=chains"
        / "ticker=SPY"
        / "seg-20260824T160000-4242.arrows"
    )
    assert seg == expected


def test_journal_dir(paths: LakePaths):
    assert paths.journal_dir == ROOT / "journal"


def test_ledger_paths(paths: LakePaths):
    assert paths.manifest_path == ROOT / "manifest.jsonl"
    assert paths.quarantine_path == ROOT / "quarantine.jsonl"


def test_reference_paths(paths: LakePaths):
    assert paths.reference_path("security_master") == ROOT / "reference" / "security_master.parquet"
    assert paths.security_master_path == ROOT / "reference" / "security_master.parquet"
    assert paths.contracts_path == ROOT / "reference" / "contracts.parquet"


def test_a_date_and_its_iso_string_build_the_same_path(paths: LakePaths):
    assert paths.chains_partition_path("SPY", "2026-08-24") == paths.chains_partition_path(
        "SPY", DAY
    )


def test_root_is_coerced_to_path():
    assert LakePaths("/lake").root == Path("/lake")


def test_surfaces_constant_lists_the_four_surfaces():
    assert SURFACES == (CHAINS, QUOTES, BARS, ACTIONS)


@pytest.mark.parametrize("surface", [CHAINS, QUOTES])
def test_partition_path_matches_fixture_lake(paths: LakePaths, surface: str):
    fixture = FixtureLake(ROOT)
    assert paths.partition_path(surface, "SPY", DAY) == fixture.partition_path(surface, "SPY", DAY)


def test_segment_path_matches_fixture_lake(paths: LakePaths):
    fixture = FixtureLake(ROOT)
    args = ("chains", "SPY", DAY, "20260824T160000", 4242)
    assert paths.segment_path(*args) == fixture.segment_path(*args)


def test_ledger_paths_match_fixture_lake(paths: LakePaths):
    fixture = FixtureLake(ROOT)
    assert paths.manifest_path == fixture.manifest_path
    assert paths.quarantine_path == fixture.quarantine_path
