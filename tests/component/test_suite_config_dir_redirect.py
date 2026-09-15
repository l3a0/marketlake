"""The redirect every child the suite spawns inherits.

The three guards in ``tests/conftest.py`` are monkeypatches, so each holds inside the
pytest process and nowhere else. A child has none of them, and an ordinary
``subprocess.run`` that runs ``lake`` code at a default path resolves that path itself.
Before the redirect, what it resolved was the machine's real ``~/.config/marketlake/``,
with a live Schwab token in it and nothing to refuse a write.

So ``tests/conftest.py`` exports ``MARKETLAKE_CONFIG_DIR`` at a throwaway directory when
it is imported, and deletes any ``MARKETLAKE_CONFIG`` it inherited beside it. The second
is the file, which the directory redirect cannot move, and ``load_config`` reads it first.
These tests drive the redirect from both sides, and the deletion from the child side. One
side is a child spawned the plain way, with no environment arranged for it, which is the
shape a test writes without thinking about the config directory at all. The other side is
this process, whose module-level defaults move with the children rather than staying
behind. Which constants those are is read out of ``src/lake`` rather than written down
here, so a new one joins without anyone remembering to come back.

A child handed an explicit ``env=`` is outside the redirect, since it carries only what
that mapping names, and so is anything the rendered ``reauth.sh`` runs, since that script
unsets the variable on purpose. Both are deliberate and neither is covered here.

Two things the redirect must not do are covered here as well. It must not disarm the
config-directory guard, which settles what it protects from ``Path.home()`` and never
reads this variable. And the throwaway must not be the real directory, which is the one
assumption every other assertion in this file rests on.

``tests/component/test_config_dir_override.py`` covers the neighbouring mechanism: the
same variable as an override a person exports by hand. Its children build their
environment from scratch on purpose, so the suite's redirect does not reach them and one
of them can still ask what the real directory is.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib import import_module
from pathlib import Path

from lake.config import CONFIG_PATH_ENV
from lake.paths import CONFIG_DIR_ENV, CONFIG_DIR_PARTS, TOKEN_FILE, config_dir
from tests.component.test_config_dir_override import (
    _DEFAULTS,
    DEFAULT_PAIRS,
    assert_the_scan_found_something,
)
from tests.conftest import _THROWAWAY_CONFIG_DIR
from tests.support.config_defaults import modules_building_a_default
from tests.support.config_guard import is_protected

# The repo root. A child that imports the ``tests`` package needs it on ``sys.path``, and
# ``python -c`` supplies only the working directory, so a child is given this as its cwd
# rather than inheriting whatever directory pytest was launched from.
ROOT = Path(__file__).resolve().parents[2]

THROWAWAY = Path(_THROWAWAY_CONFIG_DIR)

# The real directory, spelled from this process's own home rather than through
# ``config_dir``, which now answers with the throwaway. That is the same reason the guard
# spells it this way.
REAL_CONFIG_DIR = Path.home().joinpath(*CONFIG_DIR_PARTS)

# The same defaults ``_DEFAULTS`` asks a child about, read in this process instead. Both
# come from one scan of ``src/lake``, so a sixth default added to the package joins both
# without anyone editing either file, which is what the two hand-written lists here used
# to get wrong.
PARENT_DEFAULTS = {
    f"{module}.{name}": Path(getattr(import_module(module), name)) for module, name in DEFAULT_PAIRS
}


def _inheriting_child(script: str) -> dict[str, str]:
    """Run ``script`` in a fresh interpreter with the environment this process has.

    No ``env=`` argument, which is the whole point. This is the plain shape of a child,
    the one a test writes when the config directory never crossed its mind, and the
    redirect has to reach it without being asked.
    """
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_the_throwaway_is_not_the_real_directory():
    """The assumption every other test here rests on, asserted rather than assumed.

    A redirect pointing at the real directory would leave each assertion below passing
    while protecting nothing at all.
    """
    assert THROWAWAY.is_dir()
    assert THROWAWAY != REAL_CONFIG_DIR
    assert not str(THROWAWAY).startswith(str(REAL_CONFIG_DIR) + os.sep)
    assert os.environ[CONFIG_DIR_ENV] == str(THROWAWAY)


def test_a_child_that_arranges_nothing_resolves_into_the_throwaway():
    """The deliverable. A child inherits the redirect without its test lifting a finger.

    Every default is checked together, because a redirected token beside a live config
    is a half-redirected process.
    """
    assert_the_scan_found_something()
    defaults = _inheriting_child(_DEFAULTS)
    assert set(defaults) == set(PARENT_DEFAULTS)
    for name, value in defaults.items():
        assert Path(value).parent == THROWAWAY, name


def test_a_child_writing_its_default_token_lands_in_the_throwaway():
    """The incident reproduced from a child, with nothing arranged for it.

    On 2026-09-13 a by-hand run wrote ``DEFAULT_TOKEN_PATH`` and the working token it
    replaced was gone. The same write from a child during the guard's own review did it
    again. ``write_token`` is the function the login flow's callback lands in, driven
    here at the default it would have used, in a child handed no environment and no path.

    The child checks the redirect took before it writes, the same care the sibling module
    takes, and this is not belt and braces. A child has no guard, so with the redirect
    broken that line is the only thing between this write and the machine's real token.
    Refusing leaves the assertion below to fail instead, which is what a mutation run
    needs it to do.
    """
    script = f"""
import json
from pathlib import Path
from lake.reauth import DEFAULT_TOKEN_PATH, write_token
redirected = DEFAULT_TOKEN_PATH.parent == Path({str(THROWAWAY)!r})
if redirected:
    write_token(DEFAULT_TOKEN_PATH, {{"creation_timestamp": 3, "token": {{"refresh_token": "x"}}}})
print(json.dumps({{"written": str(DEFAULT_TOKEN_PATH), "redirected": redirected}}))
"""
    result = _inheriting_child(script)
    written = Path(result["written"])
    try:
        assert result["redirected"] is True, f"the redirect did not take: {written}"
        assert written.parent == THROWAWAY
        assert json.loads(written.read_text())["creation_timestamp"] == 3
    finally:
        # The throwaway lives for the whole session, so the one file this test puts there
        # is taken back out rather than left for a later test to find.
        written.unlink(missing_ok=True)


def test_an_inherited_value_is_replaced_rather_than_honoured():
    """Where the suite's children write is not for the launching shell to decide.

    ``MARKETLAKE_CONFIG_DIR`` is an override a person exports, so it can name anything,
    the real directory included. A redirect that honoured what it inherited would put the
    suite back where it started for exactly the developer who had exported the wrong
    thing, and would make the rest of a run depend on the shell it was launched from.

    A child is the only place this can be asked. Importing ``tests.conftest`` is what
    runs the redirect, and this process already ran it once.
    """
    sentinel = "/tmp/marketlake-inherited-value-that-must-not-survive"
    script = """
import json, os
from lake.paths import CONFIG_DIR_ENV
before = os.environ[CONFIG_DIR_ENV]
import tests.conftest
print(json.dumps({"before": before, "after": os.environ[CONFIG_DIR_ENV]}))
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env={**os.environ, CONFIG_DIR_ENV: sentinel},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout)
    assert result["before"] == sentinel
    assert result["after"] != sentinel
    # And what replaced it is a throwaway of that child's own making, not the real one.
    assert Path(result["after"]) != REAL_CONFIG_DIR
    # Nor this session's. The directory has to be made fresh per process, or two pytest
    # runs on one machine share a config directory and the first to finish deletes it
    # from under the second. A fixed path passes every other assertion in this file.
    assert result["after"] != str(THROWAWAY)


def test_an_inherited_config_file_is_deleted_rather_than_honoured():
    """The same rule as its sibling above, for the variable that names a file.

    ``MARKETLAKE_CONFIG`` is what ``load_config`` reads before the default, and the
    directory redirect does not reach it, because that one names a directory and this one
    names a file. An inherited value would decide which ``config.yaml`` the suite resolves,
    the operator's real one included, now that ``loader.load_chain`` resolves config when
    it is given no lake root.

    A child is the only place this can be asked, for the reason the sibling gives: importing
    ``tests.conftest`` is what runs the deletion, and this process already ran it once. That
    also fixes how the child names the variable. It has to read the value before importing
    ``tests.conftest``, and importing ``lake.config`` to get the name would bind that
    module's default against the sentinel directory, which ``tests/conftest.py`` refuses to
    import after. So the parent interpolates the name and the child spells no import of its
    own.
    """
    sentinel = "/tmp/marketlake-inherited-config-that-must-not-survive.yaml"
    script = f"""
import json, os
name = {CONFIG_PATH_ENV!r}
before = os.environ.get(name)
import tests.conftest
print(json.dumps({{"before": before, "after": os.environ.get(name)}}))
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env={**os.environ, CONFIG_PATH_ENV: sentinel},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout)
    assert result["before"] == sentinel
    assert result["after"] is None


def test_the_redirect_moved_every_default_in_this_process_too():
    """The parent moves with its children, which is what keeps two spellings together.

    ``tests/unit/test_paths.py`` and ``tests/component/test_control_plane_render.py``
    each assert that a module's bound default and a later ``config_dir()`` call name one
    directory. A redirect reaching only children would split those apart.

    This is also what notices an import-order regression, and it is the second of two
    things that do. ``tests/conftest.py`` refuses to import at all when one of the
    default-building modules is already in ``sys.modules``, which is the rule itself. This
    assertion catches the consequence from the other end, including a way in that the
    check cannot see, such as a default rebound after the fact.
    """
    assert_the_scan_found_something()
    for name, default in PARENT_DEFAULTS.items():
        assert default.parent == THROWAWAY, (
            f"{name} is bound to {default.parent}. The redirect in tests/conftest.py has "
            "to be set before any import that builds a default from config_dir, so check "
            "what moved above it."
        )
    assert config_dir() == THROWAWAY


def test_the_import_order_check_reads_the_scan():
    """The check has to be looking at the modules the scan found, not at a list of its own.

    Setting ``_BINDS_A_DEFAULT`` to an empty tuple leaves the whole suite green, because
    the condition it feeds never fires on a healthy run. This is what notices.
    """
    from tests import conftest

    assert conftest._BINDS_A_DEFAULT == modules_building_a_default()
    assert conftest._BINDS_A_DEFAULT, "an empty list would make the check below fire never"


def test_conftest_refuses_to_import_once_a_default_has_already_bound():
    """The refusal itself, driven in a child, because a healthy run never reaches it.

    A process that imports one of these modules first has bound that module's default
    against whatever the environment said then, and no redirect can move it afterwards.
    Importing ``tests.conftest`` there has to fail loudly rather than export a variable
    that moves nothing.
    """
    script = "import lake.reauth\nimport tests.conftest\n"
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0, proc.stdout
    assert "lake.reauth" in proc.stderr
    assert CONFIG_DIR_ENV in proc.stderr

    # The other direction, so this cannot pass because importing conftest always fails.
    clean = subprocess.run(
        [sys.executable, "-c", "import tests.conftest\n"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert clean.returncode == 0, clean.stderr


def test_the_redirect_does_not_disarm_the_guard():
    """The two mechanisms must not cancel each other out, checked where it now can be.

    The guard settles what it protects when ``tests/conftest`` is imported, and it reads
    ``Path.home()`` rather than ``config_dir`` so that this variable cannot move it. The
    sibling test in ``tests/unit/test_config_dir_guard.py`` covers a guard that read the
    variable at call time, and it says it cannot cover one that read it at import because
    its ``setenv`` runs too late. The redirect closes that: this process really does have
    the variable set before the guard decides, so asking the guard here drives the half
    that needed a child before.

    Nothing is written. ``is_protected`` is the predicate the refusal is built on, so
    asking it decides the real question and touches no file.
    """
    assert is_protected(str(REAL_CONFIG_DIR / TOKEN_FILE))
    assert is_protected(str(REAL_CONFIG_DIR))
    # The other direction, so this cannot pass by refusing everything. The throwaway is
    # where the suite's own children write, and a guard sweeping it up would fail them.
    assert not is_protected(str(THROWAWAY / TOKEN_FILE))
