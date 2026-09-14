"""The scanner that reads the config-directory defaults out of ``src/lake``.

Three things use that list. The suite's redirect checks that none of those modules was
imported before it ran, one test asks a child where each default resolved, and another
asks the same of the pytest process. All three used to type it out, and nothing bound
those spellings to the source, so a sixth default added to the package would have been
outside every one of them with nothing to say so.

These cover both halves of the fix. The scanner finds what is in the real tree today,
which is what keeps it honest against the package it reads. And it finds a sixth one in a
synthetic tree, which is the half the real tree cannot show, because the real tree has
exactly five and adding a sixth to it just to watch a test go red is not something a test
should do.

The synthetic trees are written under ``tmp_path`` and never imported. That is the whole
point of scanning rather than importing: importing one of these modules is what binds its
default against whatever the environment says at that moment.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.config_defaults import (
    LAKE_SRC,
    defaults_built_from_config_dir,
    modules_building_a_default,
)

# What the package holds today. Spelled out here on purpose, so that a default added,
# removed, or renamed in ``src/lake`` fails this one test with a readable diff rather
# than silently changing what three other mechanisms are checking.
KNOWN_TODAY = (
    ("lake.chain_plan", "DEFAULT_CHAIN_PLAN_PATH"),
    ("lake.config", "DEFAULT_CONFIG_PATH"),
    ("lake.reauth", "DEFAULT_TOKEN_PATH"),
    ("lake.schwab", "DEFAULT_TOKEN_PATH"),
    ("lake.tickers", "DEFAULT_TICKERS_PATH"),
)


def _package(root: Path, files: dict[str, str]) -> Path:
    """A synthetic source tree under ``root``, written and never imported."""
    root.mkdir(parents=True, exist_ok=True)
    for name, source in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return root


# -- the real tree -----------------------------------------------------------------------


def test_the_scanner_finds_what_is_there_today():
    assert defaults_built_from_config_dir() == KNOWN_TODAY


def test_the_modules_view_deduplicates_and_sorts():
    # reauth and schwab both spell DEFAULT_TOKEN_PATH, so the pair list has five entries
    # over five modules and the module view has to keep them apart by module rather than
    # by constant name.
    assert modules_building_a_default() == (
        "lake.chain_plan",
        "lake.config",
        "lake.reauth",
        "lake.schwab",
        "lake.tickers",
    )


def test_the_scanner_reads_the_package_the_other_scanner_reads():
    # tests/support/enforcement.py spells the same tree its own way. Two scanners over
    # one package that disagreed about which directory it is would each be right about
    # a different thing.
    from tests.support.enforcement import LAKE_SRC as ENFORCEMENT_SRC

    assert LAKE_SRC == ENFORCEMENT_SRC
    assert LAKE_SRC.is_dir()


# -- a sixth default, which is the case the real tree cannot show --------------------------


def test_a_sixth_default_is_found(tmp_path):
    """The failure the hand-written lists had. A new default has to join on its own.

    Verified against the old code by adding a probe module to ``src/lake`` and running
    the three modules that consume this list: all green. This is that experiment, moved
    somewhere it can live in the suite permanently.
    """
    root = _package(
        tmp_path / "lake",
        {
            "reauth.py": (
                "from lake.paths import TOKEN_FILE, config_dir\n"
                "DEFAULT_TOKEN_PATH = config_dir() / TOKEN_FILE\n"
            ),
            "probe_sixth.py": (
                "from lake.paths import config_dir\n"
                "DEFAULT_PROBE_PATH = config_dir() / 'probe.json'\n"
            ),
        },
    )
    assert defaults_built_from_config_dir(root) == (
        ("lake.probe_sixth", "DEFAULT_PROBE_PATH"),
        ("lake.reauth", "DEFAULT_TOKEN_PATH"),
    )


# -- the shapes a call can take ------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "found"),
    [
        # The plain form, which is what all five real ones use.
        ("from lake.paths import config_dir\nD = config_dir() / 'x'\n", True),
        # Aliased on import. Resolved through the file's own imports.
        ("from lake.paths import config_dir as cd\nD = cd() / 'x'\n", True),
        # Reached through the module.
        ("from lake import paths\nD = paths.config_dir() / 'x'\n", True),
        # An annotated assignment is still a module-level constant.
        (
            "from pathlib import Path\nfrom lake.paths import config_dir\nD: Path = config_dir()\n",
            True,
        ),
        # No call at all. Naming the function without calling it binds no path.
        ("from lake.paths import config_dir\nD = config_dir\n", False),
        # A different function that happens to be called.
        ("from lake.paths import temp_write_path\nD = temp_write_path('x', 1)\n", False),
    ],
)
def test_each_call_shape(tmp_path, source, found):
    root = _package(tmp_path / "lake", {"probe.py": source})
    pairs = defaults_built_from_config_dir(root)
    assert bool(pairs) is found
    if found:
        assert pairs == (("lake.probe", "D"),)


@pytest.mark.parametrize(
    ("name", "source"),
    [
        (
            "if",
            "from lake.paths import config_dir\nif True:\n    D = config_dir() / 'x'\n",
        ),
        (
            "else",
            "from lake.paths import config_dir\n"
            "if False:\n    pass\nelse:\n    D = config_dir() / 'x'\n",
        ),
        (
            "try",
            "from lake.paths import config_dir\n"
            "try:\n    D = config_dir() / 'x'\nexcept OSError:\n    pass\n",
        ),
        (
            "except",
            "from lake.paths import config_dir\n"
            "try:\n    pass\nexcept OSError:\n    D = config_dir() / 'x'\n",
        ),
        (
            "finally",
            "from lake.paths import config_dir\n"
            "try:\n    pass\nfinally:\n    D = config_dir() / 'x'\n",
        ),
        (
            "with",
            "from lake.paths import config_dir\nimport contextlib\n"
            "with contextlib.suppress(OSError):\n    D = config_dir() / 'x'\n",
        ),
        (
            "for",
            "from lake.paths import config_dir\nfor _ in range(1):\n    D = config_dir() / 'x'\n",
        ),
    ],
)
def test_a_default_bound_under_a_top_level_block_is_found(tmp_path, name, source):
    """These all run at import, so each one binds a default just as a bare line does.

    A module that guarded its default with a ``try`` would otherwise drop out of the
    redirect's import-order check and both default tests, which is the exact failure
    deriving the list was meant to end. Nothing in ``src/lake`` is written this way
    today, so only a synthetic tree can show it.
    """
    root = _package(tmp_path / "lake", {"probe.py": source})
    assert defaults_built_from_config_dir(root) == (("lake.probe", "D"),), name


def test_a_tuple_assignment_names_everything_it_binds(tmp_path):
    """Over-reporting is the safe direction, and the module docstring names it as a limit.

    A pair assigned together where one half calls ``config_dir`` reports both. Reporting
    neither would put a real default outside every check that reads this list.
    """
    root = _package(
        tmp_path / "lake",
        {"probe.py": "from lake.paths import config_dir\nA, B = config_dir() / 'a', 1\n"},
    )
    assert defaults_built_from_config_dir(root) == (("lake.probe", "A"), ("lake.probe", "B"))


def test_a_default_built_inside_a_function_is_not_one(tmp_path):
    """Only a module-level assignment binds at import, which is the shape this is about.

    ``lake.tickers`` has a function returning ``DEFAULT_TICKERS_PATH`` and
    ``lake.control_plane`` calls ``config_dir(home)`` for another account's home. Neither
    is a constant bound at import, and sweeping them in would have the redirect's check
    refuse imports that bind nothing.
    """
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "def where():\n"
                "    return config_dir() / 'x'\n"
                "class Holder:\n"
                "    INSIDE = config_dir() / 'y'\n"
            )
        },
    )
    assert defaults_built_from_config_dir(root) == ()


def test_two_defaults_in_one_module_are_both_found(tmp_path):
    # Keyed by module, so a dict keyed that way would lose one. The real tree has one
    # constant per module today, so nothing else would notice.
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "FIRST = config_dir() / 'a'\n"
                "SECOND = config_dir() / 'b'\n"
            )
        },
    )
    assert defaults_built_from_config_dir(root) == (
        ("lake.probe", "FIRST"),
        ("lake.probe", "SECOND"),
    )
    assert modules_building_a_default(root) == ("lake.probe",)


def test_a_module_in_a_subpackage_is_named_with_dots(tmp_path):
    # The package is flat today. A nested module named by its bare stem would be an
    # import name that does not resolve, and the redirect's sys.modules check would
    # silently never match it.
    root = _package(
        tmp_path / "lake",
        {
            "inner/__init__.py": "",
            "inner/probe.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n",
        },
    )
    assert defaults_built_from_config_dir(root) == (("lake.inner.probe", "D"),)


def test_a_package_init_is_named_without_its_stem(tmp_path):
    root = _package(
        tmp_path / "lake",
        {"__init__.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n"},
    )
    assert defaults_built_from_config_dir(root) == (("lake", "D"),)


# -- the pre-filter --------------------------------------------------------------------


def test_a_file_that_never_spells_the_name_is_skipped(tmp_path):
    """The pre-filter is an optimisation, so it has to be invisible in the result.

    Parsing every file costs about 84 ms on the real tree and 20 ms with the filter. A
    filter that dropped a file which did contain a default would make that saving a bug.
    """
    root = _package(
        tmp_path / "lake",
        {
            "quiet.py": "VALUE = 1\n",
            "loud.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n",
        },
    )
    assert defaults_built_from_config_dir(root) == (("lake.loud", "D"),)


def test_the_scan_is_ordered_and_repeatable(tmp_path):
    # Sorted output, so the list a reader sees is stable across machines and a diff of
    # it reads as a diff rather than as a reshuffle.
    root = _package(
        tmp_path / "lake",
        {
            "zulu.py": "from lake.paths import config_dir\nZ = config_dir() / 'z'\n",
            "alpha.py": "from lake.paths import config_dir\nA = config_dir() / 'a'\n",
        },
    )
    assert defaults_built_from_config_dir(root) == (("lake.alpha", "A"), ("lake.zulu", "Z"))
    assert defaults_built_from_config_dir(root) == defaults_built_from_config_dir(root)


def test_an_empty_tree_scans_to_nothing(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    assert defaults_built_from_config_dir(root) == ()
    assert modules_building_a_default(root) == ()
