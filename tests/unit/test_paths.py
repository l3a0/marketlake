"""Lake paths, built from values alone, and read back apart the same way.

The builders match the fixture-lake builder exactly. The two parsers invert them.
``parse_segment_rel`` takes a journal segment path apart, and ``parse_date_dir`` reads
the ``date=`` key a partition path carries. Both are pure functions over strings, so
every test here decides from values alone and the tier is unit.
"""

from __future__ import annotations

from datetime import date
from fnmatch import fnmatchcase
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
    CONFIG_DIR_ENV,
    CONFIG_DIR_PARTS,
    CONFIG_FILE,
    QUOTES,
    SEGMENT_GLOB,
    SEGMENT_PREFIX,
    SEGMENT_SUFFIX,
    SURFACES,
    TICKERS_FILE,
    TOKEN_FILE,
    LakePaths,
    SegmentRef,
    config_dir,
    parse_date_dir,
    parse_segment_rel,
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


def test_the_segment_glob_carries_the_prefix_as_well_as_the_suffix():
    # Readers discover a ticker-day's segments by this glob. Matching the suffix alone
    # would sweep in any stray ``.arrows`` file, and compaction never removes one, so the
    # panel would show its failure forever.
    assert SEGMENT_GLOB == f"{SEGMENT_PREFIX}*{SEGMENT_SUFFIX}"
    assert fnmatchcase("seg-20260824T160000-4242.arrows", SEGMENT_GLOB)
    for stray in ("notes.arrows", "chain_plan.arrows", ".arrows", "seg-1-2.arrows.tmp"):
        assert not fnmatchcase(stray, SEGMENT_GLOB)


def test_every_segment_path_matches_the_segment_glob(paths: LakePaths):
    written = paths.segment_path("chains", "SPY", DAY, "20260824T160000", 4242)
    assert fnmatchcase(written.name, SEGMENT_GLOB)


# -- reading a path back apart -----------------------------------------------

# Two modules ask each of these questions. The manifest scrub and the journal's
# last-durable-batch read both take a segment path apart. Compaction's sweep and the
# dashboard's day walk both read a ``date=`` key. A parser that drifted from the builder
# it inverts would answer plausibly and wrongly, and nothing downstream would notice. So
# this module covers each parser, beside the builder it inverts.

SEGMENT_REL = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"


def test_a_well_formed_segment_path_splits_into_its_four_parts():
    assert parse_segment_rel(SEGMENT_REL) == SegmentRef(
        day="2026-08-24",
        surface="chains",
        ticker="SPY",
        filename="seg-20260824T160000-4242.arrows",
    )


def test_a_built_segment_path_parses_back_to_the_parts_it_was_built_from(paths: LakePaths):
    # The property worth checking. The parser is the builder's inverse, so a path this
    # module builds must come apart into the arguments that built it, and those parts
    # must build the same path again. Each half stays self-consistent on its own, so
    # only the round trip catches the two drifting apart.
    built = paths.segment_path("quotes", "BRK.B", DAY, "20260824T160000", 4242)
    ref = parse_segment_rel(built.relative_to(ROOT).as_posix())
    assert ref == SegmentRef(
        day="2026-08-24",
        surface="quotes",
        ticker="BRK.B",
        filename="seg-20260824T160000-4242.arrows",
    )
    assert paths.segment_path(ref.surface, ref.ticker, ref.day, "20260824T160000", 4242) == built


@pytest.mark.parametrize(
    "rel",
    [
        # The part count is exact. Four parts is one short, six is one long.
        "journal/date=2026-08-24/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker=SPY/extra/seg-1.arrows",
        # The first part is the journal directory and nothing else.
        "reports/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        # Each of the three keys is matched as a literal prefix. A missing key fails.
        "journal/2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/SPY/seg-1.arrows",
        # A wrong separator fails too, and it is the harder case. The key is the right
        # length, so a parser that sliced by length rather than matching the prefix would
        # hand back a value that looks correct.
        "journal/date:2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface:chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker:SPY/seg-1.arrows",
        # The filename is matched on its suffix, not its prefix. A segment-named Parquet
        # file carries the prefix and is not a segment.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.parquet",
    ],
)
def test_a_malformed_segment_path_parses_to_none(rel: str):
    assert parse_segment_rel(rel) is None


def test_a_compacted_partition_never_parses_as_a_segment(paths: LakePaths):
    # Compaction seals a day's segments into ``S/ticker=T/date=D.parquet``. The manifest
    # holds both shapes, and the scrub decides supersession by telling them apart. A
    # compacted partition that parsed as a segment would be read as superseding itself.
    for built in (
        paths.chains_partition_path("SPY", DAY),
        paths.quotes_partition_path("SPY", DAY),
        paths.bars_partition_path("SPY", "1m", DAY),
        paths.actions_path,
    ):
        assert parse_segment_rel(built.relative_to(ROOT).as_posix()) is None


def test_a_date_directory_reads_back_as_its_date():
    assert parse_date_dir("date=2026-08-24") == DAY


def test_the_date_directory_a_segment_path_builds_parses_back(paths: LakePaths):
    # The second half of the round trip. Compaction's sweep lists these directory names
    # and the dashboard's day walk reads the same ones, so the builder's spelling and the
    # parser's must agree.
    ticker_dir = paths.segment_dir("chains", "SPY", DAY)
    assert parse_date_dir(ticker_dir.parent.parent.name) == DAY


@pytest.mark.parametrize("spelling", ["20260824", "2026-W35-1"])
def test_a_spelling_only_fromisoformat_accepts_is_refused(spelling: str):
    # The strictness is load-bearing, so the divergence it closes is asserted first.
    # ``date.fromisoformat`` on Python 3.12 reads the compact form and the ISO week date
    # as 2026-08-24, the same day ``date=2026-08-24`` names. Every writer builds the
    # directory through one spelling, so a directory named any other way was never
    # written by this pipeline. Reading one as Monday would let compaction seal three
    # differently named directories into a single partition, while the dashboard's panels
    # showed only the one directory they can name.
    assert date.fromisoformat(spelling) == DAY
    assert parse_date_dir(f"date={spelling}") is None


@pytest.mark.parametrize(
    "name",
    [
        "2026-08-24",  # the date with no key at all
        "day=2026-08-24",  # a different key
        "date=",  # the key and no date
        "date=2026-13-45",  # the right shape, not a real day
        "date=2026-8-24",  # a single-digit month
        "date=2026-08-24.tmp",  # a half-written partition's stem
    ],
)
def test_a_malformed_date_directory_parses_to_none(name: str):
    assert parse_date_dir(name) is None


# -- the machine's config directory -----------------------------------------------

# Four files sit in ~/.config/marketlake/ and four modules name them. Each spelled the
# directory itself before, five spellings counting the control plane's renderer. The
# Time Machine exclusion covers the directory rather than the files, so a module that
# drifted would put its file outside the exclusion and the secret in it would ride
# onto a backup disk with nothing to say so.


def test_the_config_directory_is_the_location_the_design_pins():
    assert config_dir("/Users/alice") == Path("/Users/alice/.config/marketlake")
    # env={} pins the fallback rather than reading whatever the developer exported, so
    # this asserts the design's location whether or not an override is set.
    assert config_dir(env={}) == Path.home() / ".config" / "marketlake"


# The whole directory, moved for one process. A tool whose default is the live token
# path gets run against that default by someone verifying it by hand, which is how a
# stub reached the production token on 2026-09-13 and took a working token with it.


def test_the_override_moves_the_directory():
    assert config_dir(env={CONFIG_DIR_ENV: "/tmp/throwaway"}) == Path("/tmp/throwaway")


def test_the_override_names_the_directory_rather_than_a_home():
    # MARKETLAKE_CONFIG names a file, so this names a directory. Appending the two
    # CONFIG_DIR_PARTS to it would make the override a home instead, and a developer
    # pointing it at an empty directory would find the files one level down.
    resolved = config_dir(env={CONFIG_DIR_ENV: "/tmp/throwaway"})
    assert resolved.name != CONFIG_DIR_PARTS[-1]
    assert resolved.parts[-len(CONFIG_DIR_PARTS) :] != CONFIG_DIR_PARTS


def test_the_override_is_expanded():
    # It is whatever a person typed, so it can carry a "~", the same as the overrides
    # load_config and load_tickers take. An unexpanded one would have open() make a
    # literal "~" directory rather than fail.
    assert config_dir(env={CONFIG_DIR_ENV: "~/throwaway"}) == Path.home() / "throwaway"


def test_an_explicit_home_beats_the_override():
    # The control plane renders a plist for another account. A render has to stay a
    # render whatever the environment of the machine rendering it says.
    assert config_dir("/Users/alice", env={CONFIG_DIR_ENV: "/tmp/throwaway"}) == Path(
        "/Users/alice/.config/marketlake"
    )


def test_an_empty_override_falls_through_to_the_home():
    # An exported-but-empty variable is an unset one, which is how load_config and
    # load_tickers both read theirs.
    assert config_dir(env={CONFIG_DIR_ENV: ""}) == Path.home() / ".config" / "marketlake"


def test_the_override_is_read_from_the_real_environment_by_default(monkeypatch):
    # The env argument is the test seam. Production passes nothing and gets os.environ,
    # which is what makes exporting the variable before a process starts work at all.
    monkeypatch.setenv(CONFIG_DIR_ENV, "/tmp/throwaway")
    assert config_dir() == Path("/tmp/throwaway")


def test_an_explicit_empty_mapping_beats_a_set_variable(monkeypatch):
    """``env={}`` has to mean "no override", not "go and look at the real environment".

    Three assertions in this file and in the control plane's render tests pass ``env={}``
    to say what the home-relative default is whatever a developer exported. The suite
    runs with the variable unset, so ``{}`` and ``os.environ`` agree there and the seam
    is never put under load. Setting the variable first is what tells them apart.
    """
    monkeypatch.setenv(CONFIG_DIR_ENV, "/tmp/throwaway")
    assert config_dir(env={}) == Path.home() / ".config" / "marketlake"
    assert config_dir(env={CONFIG_DIR_ENV: "/tmp/other"}) == Path("/tmp/other")


def test_an_empty_home_is_still_an_explicit_home():
    """An explicit ``home`` wins by being given, not by being truthy.

    The precedence reads "an explicit home, then the variable, then this user's home",
    and a falsy-but-given home that fell through to the variable would quietly break
    that. Nothing passes an empty home today. This pins which rule decides when
    something does.
    """
    assert config_dir("", env={CONFIG_DIR_ENV: "/tmp/throwaway"}) == Path(".config/marketlake")


def test_the_variable_is_spelled_the_way_the_design_doc_names_it():
    # Every other test here refers to the symbol, so a rename stays green across the
    # whole suite while every exported override goes inert and the design doc, which
    # writes the name out by hand, drifts. The literal is the operator-facing contract.
    assert CONFIG_DIR_ENV == "MARKETLAKE_CONFIG_DIR"


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
    # For this user's own home, the renderer's spelling and the module rule's fallback
    # agree. config_dir(env={}) rather than DEFAULT_TOKEN_PATH, because that constant
    # honours the override and this claim is about the home-relative default.
    assert cp.default_token_path(str(Path.home())) == str(config_dir(env={}) / TOKEN_FILE)
