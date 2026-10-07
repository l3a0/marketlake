"""The tracked VM settings, ``config/vm.yaml``, say what the hosted VM runs as.

``python -m lake.vm_config render < config/vm.yaml`` merges this file with four SSM
parameters and the instance's ``marketlake:backup-target`` tag into the VM's
``config.yaml``, marketlake #686. cloud-init's first boot and
#676's deploy both read it, so a change here reaches the VM through a reviewed pull
request and nowhere else.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from lake import vm_config
from lake.config import is_region_name

ROOT = Path(__file__).resolve().parents[2]
TRACKED_SETTINGS = ROOT / "config" / "vm.yaml"

# The five keys the file holds, written out rather than read from the code.
KEYS = {"role", "lake_root", "token_store", "bucket_credentials", "bucket_region"}


def _settings() -> dict:
    return yaml.safe_load(TRACKED_SETTINGS.read_text(encoding="utf-8"))


def test_the_vm_runs_as_a_shadow():
    """The VM shadows the laptop until the cutover.

    #638's cutover flips ``role`` to ``primary`` in its own pull request and inverts this
    test in the same change. Until then a VM that captured as primary would page the
    owner and upload to the primary's bucket beside the laptop.
    """
    role = _settings()["role"]
    assert type(role) is str
    assert role == "shadow"


def test_the_settings_hold_exactly_the_five_keys():
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
