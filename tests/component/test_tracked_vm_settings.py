"""The tracked VM settings, ``config/vm.yaml``, say what the hosted VM runs as.

``python -m lake.vm_config render < config/vm.yaml`` merges this file with four SSM
parameters, the instance's ``marketlake:backup-target`` tag and its optional
``marketlake:cost-export`` tag into the VM's ``config.yaml``, marketlake #686 and #913.
cloud-init's first boot and #676's deploy both read it, so a change here reaches the VM
through a reviewed pull request and nowhere else.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from lake import config, vm_config, window
from lake.config import GuardConstants, is_region_name

ROOT = Path(__file__).resolve().parents[2]
TRACKED_SETTINGS = ROOT / "config" / "vm.yaml"

# The seven keys the file holds, written out rather than read from the code.
# ``lake_window_sessions`` joined them under marketlake #786, and ``lake_window_sessions_bars``
# under marketlake #953.
KEYS = {
    "role",
    "lake_root",
    "token_store",
    "bucket_credentials",
    "bucket_region",
    "lake_window_sessions",
    "lake_window_sessions_bars",
}
# The four window keys, as literals.
WINDOW_KEYS = (
    "lake_window_sessions",
    "lake_window_sessions_chains",
    "lake_window_sessions_quotes",
    "lake_window_sessions_bars",
)


def _settings() -> dict:
    return yaml.safe_load(TRACKED_SETTINGS.read_text(encoding="utf-8"))


def test_the_vm_runs_as_the_primary():
    """The VM is the primary capture host, since #638's cutover and its return.

    The cutover flipped ``role`` to ``primary`` and inverted this test in the same change.
    The way back's rehearsal set it to ``shadow`` for one laptop session, and the return to
    the VM set it to ``primary`` again, each with this test. This test reads only the VM's
    tracked file. Keeping the laptop from also running as primary is the way back's order,
    which #638 writes down.
    """
    role = _settings()["role"]
    assert type(role) is str
    assert role == "primary"


def test_the_settings_hold_exactly_the_seven_keys():
    settings = _settings()
    assert set(settings) == KEYS
    assert not set(settings) & vm_config.FILLED_KEYS
    assert "backup_target" not in settings


def test_the_settings_are_what_the_vm_needs():
    settings = _settings()
    assert settings["lake_root"] == "/srv/marketlake"
    assert settings["token_store"] == "store"
    assert settings["bucket_credentials"] == "instance_profile"
    assert settings["bucket_region"] == "us-east-1"
    assert is_region_name(settings["bucket_region"])


def test_the_vm_keeps_a_window_of_22_sessions():
    """The owner chose a window of 22 sessions on 2026-10-07, decision 1 on marketlake #755.

    #786 measured ``du`` per surface first, and chains were nearly all of each session's
    growth, so the value stands. The type is checked as well as the value, because a quoted
    ``"22"`` loads as text and the render refuses it.
    """
    window = _settings()["lake_window_sessions"]
    assert type(window) is int
    assert window == 22


def test_the_vm_keeps_every_bars_session_and_22_of_chains_and_quotes():
    """Marketlake #953, from the owner's ruling of 2026-10-29 on #938.

    The stored values are built with ``config._window_value``, one call per key, because the
    judge reads stored values and ``Config.from_mapping`` refuses this file alone for lacking the
    keys the render fills. ``windows`` is compared whole, since ``.get("bars") is None`` holds for
    a refused surface too, and ``refused`` is asserted empty for the same reason.
    """
    settings = _settings()
    stored = {key: config._window_value(settings, key) for key in WINDOW_KEYS}

    resolved = window.surface_windows(stored, GuardConstants())

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22, "bars": None}
    assert resolved.refused == {}
    assert resolved.run_refusal is None
