"""Config loaded from a real YAML file on disk, with env-var and argument overrides."""

from __future__ import annotations

from pathlib import Path

import pytest

from lake.config import ConfigError, load_config

YAML = """\
lake_root: {root}
backup_target: {backup}
healthchecks_ping_key: PINGKEY
ntfy_topic: mytopic
schwab_api_key: SCHWABKEY
schwab_app_secret: SCHWABSECRET
guards:
  watchdog_page_minutes: 4
"""


def _write(path: Path, *, root: str = "/data/lake", backup: str = "/Volumes/ssd") -> Path:
    path.write_text(YAML.format(root=root, backup=backup))
    return path


def test_load_config_reads_a_file(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    assert cfg.lake_root == Path("/data/lake")
    assert cfg.backup_target == Path("/Volumes/ssd")
    assert cfg.healthchecks_ping_key.reveal() == "PINGKEY"
    assert cfg.guards.watchdog_page_minutes == 4


def test_env_var_points_the_loader_at_a_file(tmp_path: Path):
    cfg_file = _write(tmp_path / "elsewhere.yaml")
    cfg = load_config(env={"MARKETLAKE_CONFIG": str(cfg_file)})
    assert cfg.backup_target == Path("/Volumes/ssd")


def test_explicit_argument_beats_the_env_var(tmp_path: Path):
    chosen = _write(tmp_path / "chosen.yaml", root="/data/chosen")
    ignored = _write(tmp_path / "ignored.yaml", root="/data/ignored")
    cfg = load_config(chosen, env={"MARKETLAKE_CONFIG": str(ignored)})
    assert cfg.lake_root == Path("/data/chosen")


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.yaml")


def test_schwab_credentials_load_from_the_file(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    assert cfg.schwab_api_key.reveal() == "SCHWABKEY"
    assert cfg.schwab_app_secret.reveal() == "SCHWABSECRET"


def test_secret_stays_out_of_repr_after_a_file_load(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    for secret_value in ("PINGKEY", "mytopic", "SCHWABKEY", "SCHWABSECRET"):
        assert secret_value not in repr(cfg)
