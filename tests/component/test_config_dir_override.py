"""``MARKETLAKE_CONFIG_DIR`` in a real process, which is the only place it can work.

The conftest guard fails a test that writes the machine's real config directory. It
reaches nothing else. The write that destroyed a working Schwab token on 2026-09-13 came
from someone exercising ``python -m lake.reauth`` by hand, in a process no fixture was
in. A tool whose default is the live token path gets run against that default, because
that is what a default is for, and the weekly ritual depends on it staying that way.

So the answer for a development run is an override that moves the whole directory:
export ``MARKETLAKE_CONFIG_DIR`` and the process cannot reach the real files at all,
whatever it is given on the command line.

Every default in this package is built from ``paths.config_dir`` at import. That is what
makes one variable enough and it is also the reason these tests spawn a child process.
Setting the variable inside a running process moves nothing, because the constants are
already bound. Only a process that started with it set is the real thing, so that is
what these run, and each drives production code rather than a stand-in for it.

The child's own writes land under the test's ``tmp_path``. The parent's guard does not
reach a child, which the guard says about itself, so nothing here relies on it. The
redirect that does reach a child is covered in
``tests/component/test_suite_config_dir_redirect.py``, and these children are built to
sit outside it so one of them can still ask what the real directory is.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from lake import control_plane as cp
from lake.paths import CONFIG_DIR_ENV
from tests.component.test_control_plane_render import RENDER_ARGS
from tests.support.config_defaults import defaults_built_from_config_dir

# The repo root. One script below imports the ``tests`` package, which needs the root on
# ``sys.path``, and ``python -c`` supplies only the working directory. Giving the child
# this as its cwd is what lets the suite run from anywhere rather than from the root
# alone.
ROOT = Path(__file__).resolve().parents[2]

# Every module-level default built from the config directory, read out of ``src/lake``
# rather than typed here. All of them move together or the override is not worth having,
# since a redirected token beside a live config is a half-redirected process, and a
# sixth one added to the package has to join them without anyone remembering to come
# back and edit this file.
DEFAULT_PAIRS = defaults_built_from_config_dir()

# ``module.CONSTANT`` for each, which is what the child prints and the tests compare.
DEFAULT_KEYS = tuple(f"{module}.{name}" for module, name in DEFAULT_PAIRS)


def _defaults_script(pairs: tuple[tuple[str, str], ...]) -> str:
    """A child script printing where each default resolved, keyed by its full name.

    ``import_module`` rather than a written-out ``from x import y``, because the list is
    generated. Either binds the constant the same way, which is the thing under test.
    """
    entries = "\n".join(
        f"    {f'{module}.{name}'!r}: str(getattr(import_module({module!r}), {name!r})),"
        for module, name in pairs
    )
    header = "import json\nfrom importlib import import_module\n"
    return f"{header}print(json.dumps({{\n{entries}\n}}))\n"


_DEFAULTS = _defaults_script(DEFAULT_PAIRS)


def _child(script: str, config_dir: Path | None) -> dict[str, str]:
    """Run ``script`` in a fresh interpreter, with or without the override set.

    The environment is built rather than inherited, so nothing outside a test decides the
    answer. That matters twice over. A variable exported in the shell running the suite
    would otherwise reach the child, and so would the redirect ``tests/conftest.py`` sets
    for every child the suite spawns. Passing ``None`` here is the only way left to ask
    what a process with no override resolves, which is what
    ``test_without_the_override_every_default_is_the_real_directory`` asks.
    """
    env = {"PATH": "/usr/bin:/bin", "HOME": str(Path.home())}
    if config_dir is not None:
        env[CONFIG_DIR_ENV] = str(config_dir)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_the_override_moves_every_default_in_the_package(tmp_path):
    throwaway = tmp_path / "throwaway"
    defaults = _child(_DEFAULTS, throwaway)
    assert set(defaults) == set(DEFAULT_KEYS)
    for name, value in defaults.items():
        assert Path(value).parent == throwaway, name


def test_without_the_override_every_default_is_the_real_directory(tmp_path):
    """The other half of the claim, and the half that keeps production honest.

    A redirect that applied unconditionally would take the daemon and the weekly ritual
    with it. Nothing is written here. The paths are printed and compared.
    """
    real = Path.home() / ".config" / "marketlake"
    for name, value in _child(_DEFAULTS, None).items():
        assert Path(value).parent == real, name


def test_the_token_a_redirected_reauth_writes_lands_in_the_throwaway(tmp_path):
    """The incident, reproduced with the fix on, through the real writer.

    This is what happened on 2026-09-13: a token written to ``DEFAULT_TOKEN_PATH`` by a
    by-hand run. ``reauth.write_token`` is the function the login flow's callback lands
    in, driven here at the default it would have used, and with the override exported it
    cannot reach the real file. What the write landed on is read back, because a redirect
    that resolved the throwaway and still wrote the real path would be no redirect at all.
    The real path itself is never read: it holds the live token, and the child printing
    where it wrote is what settles the question.
    """
    throwaway = tmp_path / "throwaway"
    script = f"""
import json
from pathlib import Path
from lake.reauth import DEFAULT_TOKEN_PATH, write_token
# The child checks the redirect before it writes, and this is not belt and braces. A
# child process has no conftest guard, so with the redirect broken this line is the only
# thing standing between the write below and the machine's real token. Breaking
# config_dir on purpose is how this suite is reviewed, and the first such run destroyed
# a working token from exactly here. Refusing the write leaves the parent's assertion to
# fail instead, which is the whole point of a mutation run.
redirected = DEFAULT_TOKEN_PATH.parent == Path({str(throwaway)!r})
if redirected:
    write_token(DEFAULT_TOKEN_PATH, {{"creation_timestamp": 1, "token": {{"refresh_token": "x"}}}})
print(json.dumps({{"written": str(DEFAULT_TOKEN_PATH), "redirected": redirected}}))
"""
    result = _child(script, throwaway)
    assert result["redirected"] is True, f"the override did not take: {result['written']}"
    written = Path(result["written"])
    assert written.parent == throwaway
    assert json.loads(written.read_text())["creation_timestamp"] == 1
    # chmod 600 survives the redirect. The token is a brokerage credential wherever it
    # sits, and a throwaway copy of a real one is still a real one.
    assert written.stat().st_mode & 0o777 == 0o600


def test_an_explicit_token_path_still_wins_inside_a_redirected_process(tmp_path):
    """``--token`` is not disabled by the override, it is just no longer the only guard.

    The re-auth command takes an explicit path and the rendered script passes arguments
    straight through, so an operator pointing the tool somewhere keeps doing that.
    """
    throwaway = tmp_path / "throwaway"
    elsewhere = tmp_path / "elsewhere" / "token.json"
    script = f"""
import json
from lake.reauth import write_token
target = {str(elsewhere)!r}
write_token(target, {{"creation_timestamp": 2}})
print(json.dumps({{"written": target}}))
"""
    written = Path(_child(script, throwaway)["written"])
    assert written == elsewhere
    assert not (throwaway / "token.json").exists()


def test_the_override_does_not_disarm_the_guard_in_a_process_that_starts_with_it_set():
    """The two mechanisms must not cancel each other out, checked from a real process.

    The guard settles what it protects when it is imported. So a test that exports the
    variable with ``monkeypatch.setenv`` runs after that decision is made and cannot
    reach it, which is why the sibling test in ``tests/unit/test_config_dir_guard.py``
    covers only the other half: a guard that read the variable at call time rather than
    at import. Reading it at import is the half that lives here, and only a process that
    started with the variable set can tell.

    The child imports ``tests.support.config_guard`` rather than ``tests.conftest``, and
    that is the whole reason the guard's predicate sits in a module of its own. Importing
    ``tests.conftest`` would run the suite's redirect, which replaces this variable with a
    throwaway of its own before the guard reads anything. The test's own value would then
    never reach the code under test, and the same answer would come back whatever this
    test passed. The assertion on ``exported`` below is what holds that: it fails if the
    child's environment was moved out from under it.

    Nothing is written. ``is_protected`` is the predicate the refusal is built on, so
    asking it about the real token path drives the real decision and touches no file.
    """
    override = Path("/tmp/throwaway-not-the-real-directory")
    script = """
import json, os
from pathlib import Path
from lake.paths import CONFIG_DIR_ENV
from tests.support.config_guard import PROTECTED_ROOTS, is_protected
real_token = Path.home() / ".config" / "marketlake" / "token.json"
print(json.dumps({
    "protected": is_protected(str(real_token)),
    "roots": sorted(PROTECTED_ROOTS),
    "exported": os.environ[CONFIG_DIR_ENV],
}))
"""
    result = _child(script, override)
    assert result["exported"] == str(override), (
        "the child's own override was replaced before the guard read anything, so this "
        "test no longer decides what its name says"
    )
    assert result["protected"] is True, result["roots"]
    assert str(Path.home() / ".config" / "marketlake") in result["roots"]


def test_the_rendered_ritual_script_unsets_the_override(tmp_path):
    """The weekly re-auth is the one run that must reach the real token.

    Every other rendered file is run by launchd, which hands a job only the environment
    its plist names. This one is run by the operator in their own shell, so it is the
    single rendered thing an exported override reaches. A profile export would send the
    week's token to a throwaway directory while the daemon kept reading the real one as
    it expired, which is the dark Monday this whole issue is about, arrived at from the
    other side.

    The unset must come before the call, or it protects nothing.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    lines = [
        line
        for line in (out / cp.REAUTH_SCRIPT_FILE).read_text().splitlines()
        if line and not line.startswith("#")
    ]
    assert f"unset {CONFIG_DIR_ENV}" in lines
    invocation = next(line for line in lines if "lake.reauth" in line)
    assert lines.index(f"unset {CONFIG_DIR_ENV}") < lines.index(invocation)


def test_the_ritual_script_really_ignores_an_exported_override(tmp_path):
    """Driven rather than read, because the claim is about what bash does.

    The script is rendered against a stub interpreter that prints the token path the
    tool would have used, then run with the override exported. Reading the file for an
    ``unset`` line proves the line is there. Running it proves the line works.
    """
    project = tmp_path / "project"
    project.mkdir()
    stub = tmp_path / "stub-python"
    stub.write_text(f'#!/bin/bash\nprintf "%s\\n" "${{{CONFIG_DIR_ENV}-<unset>}}"\n')
    stub.chmod(0o755)

    out = tmp_path / "out"
    args = [
        "--python",
        str(stub),
        "--owner",
        "someone",
        "--home",
        "/Users/someone",
        "--project-dir",
        str(project),
        "--log-dir",
        "/Users/someone/Library/Logs/marketlake",
    ]
    assert cp.main(["render", "--out", str(out), *args]) == 0

    proc = subprocess.run(
        [str(out / cp.REAUTH_SCRIPT_FILE)],
        capture_output=True,
        text=True,
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(Path.home()),
            CONFIG_DIR_ENV: "/tmp/throwaway-would-be-wrong",
        },
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "<unset>", proc.stdout


def test_the_rendered_plists_do_not_carry_the_override(tmp_path):
    """Production is out of the override's reach, which is why the default can stay live.

    launchd hands a job the environment its plist names and nothing from a shell, so an
    operator who exports this variable does not quietly redirect the daemon. The plists
    set HOME and MARKETLAKE_CONFIG on purpose. This one is not theirs to set, and a
    render that started setting it would point capture at a throwaway directory.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    plists = sorted(out.glob("*.plist"))
    assert plists
    for plist in plists:
        assert CONFIG_DIR_ENV not in plist.read_text(), plist.name
