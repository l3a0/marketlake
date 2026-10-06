"""The scanner that keeps every config-directory default in ``src/lake`` resolved at call time.

A default bound when its module is imported keeps the directory the environment named at
that moment. On 2026-10-06 a probe that moved ``HOME`` after ``import lake`` overwrote a
host's live roster through one. ``tests/support/config_defaults.py`` reads the source for
three things: nothing resolves a config path at import, the resolvers it finds are the
ones the child-process tests enumerate, and every ``config_dir`` call sits inside one.

These cover both halves. The scanner's answers about the real tree are what keep the
package honest. Its answers about a synthetic tree are the half the real tree cannot
show, because adding a constant to the real tree just to watch a test go red is not
something a test should do.

The synthetic trees are written under ``tmp_path`` and never imported. Scanning rather
than importing is the point, because importing a module with a constant like that is
what binds it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.config_defaults import (
    LAKE_SRC,
    bindings_at_import,
    config_dir_calls_outside_resolvers,
    resolvers,
)

# The resolvers the package holds today. Spelled out here on purpose, so that a resolver
# added, removed, or renamed in ``src/lake`` fails this one test with a readable diff
# rather than silently changing what the child-process tests enumerate.
KNOWN_RESOLVERS = (
    ("lake.chain_plan", "default_chain_plan_path"),
    ("lake.config", "default_config_path"),
    ("lake.paths", "default_token_path"),
    ("lake.tickers", "default_tickers_path"),
)

# A synthetic ``lake.paths`` with the one resolver a test below calls at import.
PATHS_WITH_A_RESOLVER = (
    "from pathlib import Path\n"
    "def config_dir(home=None):\n"
    "    return Path('/x')\n"
    "def default_token_path(home=None):\n"
    "    return config_dir(home) / 'token.json'\n"
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


def test_nothing_in_the_package_resolves_a_config_path_at_import():
    assert bindings_at_import() == ()


def test_the_resolvers_are_the_ones_there_today():
    assert resolvers() == KNOWN_RESOLVERS


def test_every_config_dir_call_in_the_package_sits_in_a_resolver():
    """The closing check, which keeps the resolver list from going stale.

    ``control_plane.default_config_dir`` is the one legitimate call outside the list,
    because its ``home`` is required. It is named ``default_*``, so it passes here too.
    """
    assert config_dir_calls_outside_resolvers() == ()


def test_the_scanner_reads_the_package_the_other_scanner_reads():
    # tests/support/enforcement.py spells the same tree its own way. Two scanners over
    # one package that disagreed about which directory it is would each be right about
    # a different thing.
    from tests.support.enforcement import LAKE_SRC as ENFORCEMENT_SRC

    assert LAKE_SRC == ENFORCEMENT_SRC
    assert LAKE_SRC.is_dir()


# -- a constant bound at import -----------------------------------------------------------


def test_a_new_constant_is_found(tmp_path):
    """The shape marketlake #715 removed, in two modules, and both are reported."""
    root = _package(
        tmp_path / "lake",
        {
            "reauth.py": (
                "from lake.paths import TOKEN_FILE, config_dir\n"
                "TOKEN_PATH = config_dir() / TOKEN_FILE\n"
            ),
            "probe_new.py": (
                "from lake.paths import config_dir\n"
                "DEFAULT_PROBE_PATH = config_dir() / 'probe.json'\n"
            ),
        },
    )
    assert bindings_at_import(root) == (
        ("lake.probe_new", "DEFAULT_PROBE_PATH"),
        ("lake.reauth", "TOKEN_PATH"),
    )
    assert config_dir_calls_outside_resolvers(root) == (("lake.probe_new", 2), ("lake.reauth", 2))


@pytest.mark.parametrize(
    ("source", "found"),
    [
        # The plain form.
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
        # A function whose name only contains the word.
        ("from lake.control_plane import default_config_dir\nD = default_config_dir('h')\n", False),
    ],
)
def test_each_call_shape(tmp_path, source, found):
    root = _package(tmp_path / "lake", {"probe.py": source})
    pairs = bindings_at_import(root)
    assert bool(pairs) is found
    if found:
        assert pairs == (("lake.probe", "D"),)


@pytest.mark.parametrize(
    "source",
    [
        "from lake.paths import default_token_path\nD = default_token_path()\n",
        "from lake.paths import default_token_path as dtp\nD = dtp()\n",
        "from lake import paths\nD = paths.default_token_path()\n",
    ],
)
def test_a_resolver_called_at_import_is_found(tmp_path, source):
    """A resolver called at import is a constant again, whatever it calls inside."""
    root = _package(tmp_path / "lake", {"paths.py": PATHS_WITH_A_RESOLVER, "probe.py": source})
    assert bindings_at_import(root) == (("lake.probe", "D"),)


def test_a_file_naming_only_a_resolver_is_still_read(tmp_path):
    """The pre-filter widens to the resolver names, or this file would never be parsed."""
    source = "from lake import paths\nD = paths.default_token_path()\n"
    assert "config_dir" not in source
    root = _package(tmp_path / "lake", {"paths.py": PATHS_WITH_A_RESOLVER, "probe.py": source})
    assert bindings_at_import(root) == (("lake.probe", "D"),)


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
        (
            "for-else",
            "from lake.paths import config_dir\n"
            "for _ in range(0):\n    pass\nelse:\n    D = config_dir() / 'x'\n",
        ),
    ],
)
def test_a_constant_bound_under_a_top_level_block_is_found(tmp_path, name, source):
    """These all run at import, so each one binds a path just as a bare line does."""
    root = _package(tmp_path / "lake", {"probe.py": source})
    assert bindings_at_import(root) == (("lake.probe", "D"),), name


def test_a_tuple_assignment_names_everything_it_binds(tmp_path):
    """Over-reporting is the safe direction. A pair assigned together reports both."""
    root = _package(
        tmp_path / "lake",
        {"probe.py": "from lake.paths import config_dir\nA, B = config_dir() / 'a', 1\n"},
    )
    assert bindings_at_import(root) == (("lake.probe", "A"), ("lake.probe", "B"))


def test_a_chained_assignment_names_every_target(tmp_path):
    root = _package(
        tmp_path / "lake",
        {"probe.py": "from lake.paths import config_dir\nA = B = config_dir() / 'x'\n"},
    )
    assert bindings_at_import(root) == (("lake.probe", "A"), ("lake.probe", "B"))


def test_two_constants_in_one_module_are_both_found(tmp_path):
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
    assert bindings_at_import(root) == (("lake.probe", "FIRST"), ("lake.probe", "SECOND"))


# -- a class body and a default argument ------------------------------------------------


def test_a_class_attribute_is_found(tmp_path):
    """A class body runs at import, so an attribute built in one is fixed there too."""
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "class Holder:\n"
                "    INSIDE = config_dir() / 'y'\n"
                "    class Inner:\n"
                "        DEEPER = config_dir() / 'z'\n"
            )
        },
    )
    assert bindings_at_import(root) == (
        ("lake.probe", "Holder.INSIDE"),
        ("lake.probe", "Holder.Inner.DEEPER"),
    )


@pytest.mark.parametrize(
    ("source", "where"),
    [
        ("def f(p=config_dir() / 'x'):\n    return p\n", "f()"),
        ("def f(*, p=config_dir() / 'x'):\n    return p\n", "f()"),
        ("async def f(p=config_dir()):\n    return p\n", "f()"),
        ("class C:\n    def m(self, p=config_dir()):\n        return p\n", "C.m()"),
        ("F = lambda p=config_dir(): p\n", "F"),
        ("F = lambda *, p=config_dir(): p\n", "F"),
    ],
)
def test_a_default_argument_is_found(tmp_path, source, where):
    """A default argument is evaluated when its ``def`` runs, which for these is import."""
    root = _package(tmp_path / "lake", {"probe.py": f"from lake.paths import config_dir\n{source}"})
    assert bindings_at_import(root) == (("lake.probe", where),)


def test_a_path_resolved_inside_a_function_is_not_bound_at_import(tmp_path):
    """A function body and a lambda body run when called, which is the shape wanted."""
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "def default_where():\n"
                "    return config_dir() / 'x'\n"
                "F = lambda: config_dir() / 'y'\n"
            )
        },
    )
    assert bindings_at_import(root) == ()


def test_a_module_in_a_subpackage_is_named_with_dots(tmp_path):
    # The package is flat today. A nested module named by its bare stem would be an
    # import name that does not resolve.
    root = _package(
        tmp_path / "lake",
        {
            "inner/__init__.py": "",
            "inner/probe.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n",
        },
    )
    assert bindings_at_import(root) == (("lake.inner.probe", "D"),)


def test_a_package_init_is_named_without_its_stem(tmp_path):
    root = _package(
        tmp_path / "lake",
        {"__init__.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n"},
    )
    assert bindings_at_import(root) == (("lake", "D"),)


def test_a_file_that_never_spells_the_name_is_skipped(tmp_path):
    """The pre-filter is an optimisation, so it has to be invisible in the result."""
    root = _package(
        tmp_path / "lake",
        {
            "quiet.py": "VALUE = 1\n",
            "loud.py": "from lake.paths import config_dir\nD = config_dir() / 'x'\n",
        },
    )
    assert bindings_at_import(root) == (("lake.loud", "D"),)


def test_the_scan_is_ordered_and_repeatable(tmp_path):
    root = _package(
        tmp_path / "lake",
        {
            "zulu.py": "from lake.paths import config_dir\nZ = config_dir() / 'z'\n",
            "alpha.py": "from lake.paths import config_dir\nA = config_dir() / 'a'\n",
        },
    )
    assert bindings_at_import(root) == (("lake.alpha", "A"), ("lake.zulu", "Z"))
    assert bindings_at_import(root) == bindings_at_import(root)


def test_an_empty_tree_scans_to_nothing(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    assert bindings_at_import(root) == ()
    assert resolvers(root) == ()
    assert config_dir_calls_outside_resolvers(root) == ()


# -- the resolver list --------------------------------------------------------------------


def test_a_resolver_is_listed_only_when_it_can_be_called_with_nothing(tmp_path):
    """``default_config_dir(home)`` resolves another account's directory, not this one's."""
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "def default_a_path():\n"
                "    return config_dir() / 'a'\n"
                "def default_b_path(home=None, *, env=None):\n"
                "    return config_dir(home) / 'b'\n"
                "def default_config_dir(home):\n"
                "    return config_dir(home)\n"
                "def default_c_path(*, home):\n"
                "    return config_dir(home) / 'c'\n"
                "def default_unrelated():\n"
                "    return 1\n"
            )
        },
    )
    assert resolvers(root) == (("lake.probe", "default_a_path"), ("lake.probe", "default_b_path"))


def test_a_resolver_reached_through_an_alias_or_the_module_is_listed(tmp_path):
    root = _package(
        tmp_path / "lake",
        {
            "one.py": (
                "from lake.paths import config_dir as cd\n"
                "def default_one_path():\n"
                "    return cd() / 'one'\n"
            ),
            "two.py": (
                "from lake import paths\n"
                "def default_two_path():\n"
                "    return paths.config_dir() / 'two'\n"
            ),
        },
    )
    assert resolvers(root) == (("lake.one", "default_one_path"), ("lake.two", "default_two_path"))


# -- the closing check ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "source",
    [
        # An inline default in an ordinary function.
        "def load(path=None):\n    return path or config_dir() / 'x'\n",
        # A resolver named any other way, such as a private one.
        "def _default_token_path():\n    return config_dir() / 'token.json'\n",
        # A resolver nested inside another function is not module-level.
        (
            "def outer():\n"
            "    def default_inner():\n"
            "        return config_dir()\n"
            "    return default_inner\n"
        ),
        # A method, even one named like a resolver.
        "class C:\n    def default_path(self):\n        return config_dir()\n",
        # A default argument of a resolver runs at import, so the body rule does not cover it.
        "def default_path(p=config_dir()):\n    return p\n",
    ],
)
def test_a_config_dir_call_outside_a_resolver_is_refused(tmp_path, source):
    root = _package(tmp_path / "lake", {"probe.py": f"from lake.paths import config_dir\n{source}"})
    found = config_dir_calls_outside_resolvers(root)
    assert [module for module, _ in found] == ["lake.probe"], found


def test_a_config_dir_call_inside_a_resolver_passes(tmp_path):
    root = _package(
        tmp_path / "lake",
        {
            "probe.py": (
                "from lake.paths import config_dir\n"
                "def default_config_dir(home):\n"
                "    return config_dir(home)\n"
                "def default_path():\n"
                "    def helper():\n"
                "        return config_dir()\n"
                "    return helper() / 'x'\n"
            ),
            "caller.py": (
                "from lake.probe import default_config_dir\n"
                "def where(host):\n"
                "    return default_config_dir(host.home)\n"
            ),
        },
    )
    assert config_dir_calls_outside_resolvers(root) == ()
