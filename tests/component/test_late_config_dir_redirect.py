"""A config directory moved after import moves every write that follows it.

On 2026-10-06 a review probe set ``HOME`` inside a script after ``import lake`` and
overwrote a host's live ``tickers.yaml``, because the roster's default path was a module
constant fixed when its module was imported. The token, config and chain-plan defaults
were built the same way. An overwritten token stops capture until the next re-auth, so
each of the three writers that reach the config directory is driven here, in a child
process that moves its environment part-way through, the order a probe that redirects
too late runs in.

1. The re-auth writes the token, through ``reauth.reauth_from_config`` with no paths.
2. The token refresh rewrites it, through ``SchwabVendor.from_token`` with no path.
3. The close+15 re-tune rewrites the chain plan, through ``compact.compact`` with no
   ``plan_path``.

Each runs in two variants.

1. *HOME only*, the shape of a launchd or systemd job: ``HOME`` at a directory A with no
   ``MARKETLAKE_CONFIG_DIR``, then ``HOME`` moved to B. The write must land in
   ``B/.config/marketlake``.
2. *Both*: ``HOME`` at A and ``MARKETLAKE_CONFIG_DIR`` at ``A/override``, then both moved,
   to B and ``B/override``. The write must land in ``B/override``. The override sits
   where ``HOME`` does not derive, so a resolver that ignored either half of
   ``config_dir`` would resolve somewhere else and fail.

The child runs four steps in order.

1. It imports the writer's modules by name. ``import lake`` alone loads none of them, so
   a test that imported only ``lake`` would pass with a default fixed at import.
2. It calls each resolver the write uses once under A, and the parent checks the exact
   path. A resolver that cached its first answer would then send the write back to A.
3. It moves the environment to B, and exits before writing unless ``config_dir`` now
   names the variant's directory under B and not the machine's real one.
4. It writes.

The parent then checks the write landed at its exact path in B, and that A's recursive
listing and every file's bytes are unchanged. Every writer makes its parent directory
and puts its temp file beside the target, so a write sent to the wrong place shows as a
new file or directory under A.

Each child gets an environment built here rather than inherited, so the suite's own
redirect does not reach it, and runs with the repo root as its working directory. The
re-tune child imports ``tests.support``, and from any other directory a stray ``tests``
package in the venv's site-packages would answer ``import tests`` instead.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from lake.chain_plan import DEFAULT_CHAIN_PLAN, load_chain_plan
from lake.compact import write_chain_plan
from lake.paths import CHAIN_PLAN_FILE, CONFIG_FILE, TOKEN_FILE
from tests.component.test_compaction import DAY, _profile_table, _segment, _snap
from tests.support.config import write_config
from tests.support.config_guard import REAL_CONFIG_DIR

# The repo root, the child's working directory.
ROOT = Path(__file__).resolve().parents[2]

VARIANTS = ("home", "both")

# The callback URL each directory's config names. They differ, so the report says which
# config the re-auth loaded.
CALLBACK_A = "https://127.0.0.1:8001"
CALLBACK_B = "https://127.0.0.1:8002"

# What the fake login flow writes, standing for a login that succeeded.
FRESH_TOKEN = {"creation_timestamp": 1787529900, "token": {"refresh_token": "fresh"}}

# Shared by every child. ``move`` is step 3: it points the environment at B, then refuses
# to go on unless ``config_dir`` names the variant's directory there and not the real one.
_PRELUDE = """\
import json, os, sys
from pathlib import Path

spec = json.loads(sys.argv[1])
out = {}


def move():
    os.environ["HOME"] = spec["b_home"]
    if spec["b_override"] is not None:
        os.environ["MARKETLAKE_CONFIG_DIR"] = spec["b_override"]
    from lake.paths import config_dir

    now = config_dir()
    if now != Path(spec["expected_b"]) or str(now) == spec["real"]:
        print(json.dumps({"refused": str(now)}))
        sys.exit(3)
"""

_REAUTH = (
    _PRELUDE
    + """
import lake.config
import lake.paths
import lake.reauth

out["under_a"] = {
    "config": str(lake.config.default_config_path()),
    "token": str(lake.paths.default_token_path()),
}
move()


def flow(api_key, app_secret, callback_url, token_path, *, token_write_func, **kwargs):
    token_write_func(spec["token"])
    return "a client this command discards"


def no_store(config):
    raise AssertionError("the token store is off in this config, so nothing builds a client")


report = lake.reauth.reauth_from_config(
    login_flow=flow, store_client_factory=no_store, stdin_is_tty=True
)
out["report"] = {
    "token_path": str(report.token_path),
    "callback_url": report.callback_url,
    "token_written": report.token_written,
}
print(json.dumps(out))
"""
)

_REFRESH = (
    _PRELUDE
    + """
import ipaddress
import socket

# No suite guard reaches a child, so this one refuses any connect off the machine. A
# refresh that escaped the mock transport below would otherwise reach Schwab.
_real_connect = socket.socket.connect


def _loopback_only(self, address):
    host = address[0] if isinstance(address, tuple) else address
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise RuntimeError(f"refused a connect to {address!r}")
    return _real_connect(self, address)


socket.socket.connect = _loopback_only

import lake.paths
import lake.schwab

out["under_a"] = {"token": str(lake.paths.default_token_path())}
move()

import functools
from urllib.parse import parse_qs

import httpx
import schwab.auth
from authlib.integrations.httpx_client import OAuth2Client

refreshed_with = []


def server(request):
    if request.url.path == "/v1/oauth/token":
        sent = parse_qs(request.content.decode())["refresh_token"][0]
        refreshed_with.append(sent)
        return httpx.Response(
            200,
            json={
                "access_token": "fresh-" + sent,
                "refresh_token": sent,
                "token_type": "Bearer",
                "expires_in": 1800,
            },
        )
    return httpx.Response(200, json={})


schwab.auth.OAuth2Client = functools.partial(OAuth2Client, transport=httpx.MockTransport(server))
vendor = lake.schwab.SchwabVendor.from_token(api_key="app-key", app_secret="app-secret")
vendor.get_quotes(["SPY"])
out["refreshed_with"] = refreshed_with
print(json.dumps(out))
"""
)

_RETUNE = (
    _PRELUDE
    + """
import lake.chain_plan
import lake.compact

out["under_a"] = {"plan": str(lake.chain_plan.default_chain_plan_path())}
move()

from datetime import date, datetime, time

from lake.calendar import MARKET_TZ
from tests.support.backup import FakeBackup
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock

day = date.fromisoformat(spec["day"])


def et(hour, minute):
    return datetime.combine(day, time(hour, minute), tzinfo=MARKET_TZ)


result = lake.compact.compact(
    spec["lake_root"],
    clock=ManualClock(et(16, 30)),
    calendar=FakeCalendar({day: SessionTimes(open=et(9, 30), close=et(16, 0))}),
    backup=FakeBackup(),
    backup_target=spec["backup_target"],
)
out["written"] = result.retune is not None and result.retune.written
print(json.dumps(out))
"""
)


@dataclass(frozen=True)
class Homes:
    """The two homes a child moves between, and the config directory each one names."""

    root: Path
    a_home: Path
    b_home: Path
    a_dir: Path
    b_dir: Path
    env: dict[str, str]
    b_override: str | None


def _homes(tmp_path: Path, variant: str) -> Homes:
    a_home, b_home = tmp_path / "A", tmp_path / "B"
    a_home.mkdir()
    b_home.mkdir()
    env = {"PATH": "/usr/bin:/bin", "HOME": str(a_home)}
    if variant == "home":
        return Homes(
            tmp_path,
            a_home,
            b_home,
            a_dir=a_home / ".config" / "marketlake",
            b_dir=b_home / ".config" / "marketlake",
            env=env,
            b_override=None,
        )
    a_dir, b_dir = a_home / "override", b_home / "override"
    return Homes(
        tmp_path,
        a_home,
        b_home,
        a_dir=a_dir,
        b_dir=b_dir,
        env={**env, "MARKETLAKE_CONFIG_DIR": str(a_dir)},
        b_override=str(b_dir),
    )


def _snapshot(root: Path) -> dict[str, str]:
    """Every path under ``root``, each file with the sha256 of its bytes."""
    return {
        path.relative_to(root).as_posix(): (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "<dir>"
        )
        for path in sorted(root.rglob("*"))
    }


def _run(script: str, homes: Homes, **spec: object) -> dict:
    """Run one child, and check A is exactly as it was before the child ran."""
    full = {
        "b_home": str(homes.b_home),
        "b_override": homes.b_override,
        "expected_b": str(homes.b_dir),
        "real": REAL_CONFIG_DIR,
        **spec,
    }
    before = _snapshot(homes.a_home)
    proc = subprocess.run(
        [sys.executable, "-c", script, json.dumps(full)],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=homes.env,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _snapshot(homes.a_home) == before
    return json.loads(proc.stdout)


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_late_redirect_moves_the_reauth_token_write(tmp_path, variant):
    """The re-auth writes the token and reads the config from B, both resolved late.

    The two configs name different callbacks. Without that, a config default fixed at
    import or cached under A would load A's config, still write the token to B, and pass.
    """
    homes = _homes(tmp_path, variant)
    for config_dir, callback in ((homes.a_dir, CALLBACK_A), (homes.b_dir, CALLBACK_B)):
        config_dir.mkdir(parents=True)
        write_config(
            config_dir, tmp_path / "lake", backup_target=tmp_path / "ssd", callback_url=callback
        )

    out = _run(_REAUTH, homes, token=FRESH_TOKEN)

    assert out["under_a"] == {
        "config": str(homes.a_dir / CONFIG_FILE),
        "token": str(homes.a_dir / TOKEN_FILE),
    }
    assert out["report"] == {
        "token_path": str(homes.b_dir / TOKEN_FILE),
        "callback_url": CALLBACK_B,
        "token_written": True,
    }
    assert json.loads((homes.b_dir / TOKEN_FILE).read_text()) == FRESH_TOKEN


def _expired_token(path: Path, tag: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    token = {
        "access_token": f"stale-{tag}",
        "refresh_token": f"refresh-{tag}",
        "token_type": "Bearer",
        "expires_at": int(time.time()) - 10,
    }
    path.write_text(json.dumps({"creation_timestamp": 1787529900, "token": token}))


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_late_redirect_moves_the_token_refresh_write(tmp_path, variant):
    """The vendor refreshes B's expired token and writes the refresh back to B.

    Both directories hold an expired token, each with its own refresh token, so the
    refresh token the mock server received says which file the vendor read.
    """
    homes = _homes(tmp_path, variant)
    _expired_token(homes.a_dir / TOKEN_FILE, "A")
    _expired_token(homes.b_dir / TOKEN_FILE, "B")

    out = _run(_REFRESH, homes)

    assert out["under_a"] == {"token": str(homes.a_dir / TOKEN_FILE)}
    assert out["refreshed_with"] == ["refresh-B"]
    stored = json.loads((homes.b_dir / TOKEN_FILE).read_text())
    assert stored["token"]["access_token"] == "fresh-refresh-B"


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_late_redirect_moves_the_chain_plan_retune_write(tmp_path, variant):
    """The close+15 re-tune writes its plan to B, and A's plan is not rewritten.

    The lake holds one sealed-to-be day whose first window carries 2,600 contracts, which
    the re-tune splits, so it writes a plan. A holds ``DEFAULT_CHAIN_PLAN``, so a re-tune
    sent there would rewrite it.
    """
    homes = _homes(tmp_path, variant)
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    table = _profile_table(DEFAULT_CHAIN_PLAN, DAY, {0: 2600, 1: 1000}, snap_ts=_snap(DAY, 0))
    _segment(lake_root, "chains", "SPY", DAY, table, start_ts="a")
    write_chain_plan(DEFAULT_CHAIN_PLAN, homes.a_dir / CHAIN_PLAN_FILE)

    out = _run(
        _RETUNE,
        homes,
        lake_root=str(lake_root),
        backup_target=str(tmp_path / "ssd"),
        day=DAY.isoformat(),
    )

    assert out["under_a"] == {"plan": str(homes.a_dir / CHAIN_PLAN_FILE)}
    assert out["written"] is True
    # The loader falls back to the default on any failure, so a plan that differs from it
    # is one the re-tune wrote and the loader accepted.
    assert load_chain_plan(homes.b_dir / CHAIN_PLAN_FILE) != DEFAULT_CHAIN_PLAN
