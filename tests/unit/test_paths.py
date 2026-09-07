"""Lake paths, built from values alone. They match the fixture-lake builder exactly."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake.chain_plan import DEFAULT_CHAIN_PLAN_PATH
from lake.config import DEFAULT_CONFIG_PATH
from lake.paths import (
    ACTIONS,
    BARS,
    CHAIN_PLAN_FILE,
    CHAINS,
    CONFIG_FILE,
    QUOTES,
    SURFACES,
    TICKERS_FILE,
    TOKEN_FILE,
    LakePaths,
    config_dir,
)
from lake.schwab import DEFAULT_TOKEN_PATH
from lake.tickers import DEFAULT_TICKERS_PATH
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


# -- the machine's config directory -----------------------------------------------

# Four files sit in ~/.config/marketlake/ and four modules name them. Each spelled the
# directory itself before, five spellings counting the control plane's renderer. The
# Time Machine exclusion covers the directory rather than the files, so a module that
# drifted would put its file outside the exclusion and the secret in it would ride
# onto a backup disk with nothing to say so.


def test_the_config_directory_is_the_location_the_design_pins():
    assert config_dir("/Users/alice") == Path("/Users/alice/.config/marketlake")
    assert config_dir() == Path.home() / ".config" / "marketlake"


@pytest.mark.parametrize(
    ("default", "name"),
    [
        (DEFAULT_CONFIG_PATH, CONFIG_FILE),
        (DEFAULT_TICKERS_PATH, TICKERS_FILE),
        (DEFAULT_TOKEN_PATH, TOKEN_FILE),
        (DEFAULT_CHAIN_PLAN_PATH, CHAIN_PLAN_FILE),
    ],
)
def test_every_machine_file_sits_in_the_excluded_directory(default, name):
    assert default.parent == config_dir()
    assert default.name == name


@pytest.mark.parametrize(
    "default",
    [DEFAULT_CONFIG_PATH, DEFAULT_TICKERS_PATH, DEFAULT_TOKEN_PATH, DEFAULT_CHAIN_PLAN_PATH],
)
def test_every_default_comes_back_resolved(default):
    # One convention. An unexpanded "~" path looks usable and is not, because open()
    # would create a literal "~" directory rather than failing.
    assert default.is_absolute()
    assert "~" not in str(default)


def test_the_control_plane_renderer_agrees_with_the_shared_rule():
    # The renderer builds paths for another account's home. It must spell the
    # directory the same way a running process does.
    assert cp.default_config_dir("/Users/alice") == str(config_dir("/Users/alice"))
    assert cp.default_token_path("/Users/alice") == str(config_dir("/Users/alice") / TOKEN_FILE)
    assert cp.default_token_path(str(Path.home())) == str(DEFAULT_TOKEN_PATH)
