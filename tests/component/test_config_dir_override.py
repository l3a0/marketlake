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
reach a child, which the guard says about itself, so nothing here relies on it.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from lake import control_plane as cp
from lake.paths import CONFIG_DIR_ENV
from tests.component.test_control_plane_render import RENDER_ARGS

# Every module-level default built from the config directory, and the file each names.
# All five move together or the override is not worth having, since a redirected token
# beside a live config is a half-redirected process.
_DEFAULTS = """
import json
from lake.chain_plan import DEFAULT_CHAIN_PLAN_PATH
from lake.config import DEFAULT_CONFIG_PATH
from lake.reauth import DEFAULT_TOKEN_PATH as REAUTH_TOKEN_PATH
from lake.schwab import DEFAULT_TOKEN_PATH as SCHWAB_TOKEN_PATH
from lake.tickers import DEFAULT_TICKERS_PATH
print(json.dumps({
    "chain_plan": str(DEFAULT_CHAIN_PLAN_PATH),
    "config": str(DEFAULT_CONFIG_PATH),
    "reauth_token": str(REAUTH_TOKEN_PATH),
    "schwab_token": str(SCHWAB_TOKEN_PATH),
    "tickers": str(DEFAULT_TICKERS_PATH),
}))
"""


def _child(script: str, config_dir: Path | None) -> dict[str, str]:
    """Run ``script`` in a fresh interpreter, with or without the override set.

    The environment is built rather than inherited, so a variable exported in the shell
    running the suite cannot decide the answer either way.
    """
    env = {"PATH": "/usr/bin:/bin", "HOME": str(Path.home())}
    if config_dir is not None:
        env[CONFIG_DIR_ENV] = str(config_dir)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_the_override_moves_every_default_in_the_package(tmp_path):
    throwaway = tmp_path / "throwaway"
    defaults = _child(_DEFAULTS, throwaway)
    assert set(defaults) == {"chain_plan", "config", "reauth_token", "schwab_token", "tickers"}
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
    cannot reach the real file. The real path is checked for a stub afterwards, since a
    redirect that also wrote the real path would be no redirect at all.
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
