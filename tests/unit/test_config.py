"""Config resolved from values alone: defaults, overrides, and secret hiding."""

from __future__ import annotations

from pathlib import Path

import pytest

from lake.config import Config, ConfigError, GuardConstants, Secret
from lake.paths import LakePaths

BASE = {
    "lake_root": "/data/lake",
    "backup_target": "/Volumes/ssd/lake",
    "healthchecks_ping_key": "PING-KEY-SECRET",
    "ntfy_topic": "topic-secret-xyz",
    "schwab_api_key": "SCHWAB-API-KEY-SECRET",
    "schwab_app_secret": "SCHWAB-APP-SECRET-VALUE",
}


def test_from_mapping_resolves_paths_and_secrets():
    cfg = Config.from_mapping(BASE)
    assert cfg.lake_root == Path("/data/lake")
    assert cfg.backup_target == Path("/Volumes/ssd/lake")
    assert cfg.healthchecks_ping_key.reveal() == "PING-KEY-SECRET"
    assert cfg.ntfy_topic.reveal() == "topic-secret-xyz"
    assert cfg.schwab_api_key.reveal() == "SCHWAB-API-KEY-SECRET"
    assert cfg.schwab_app_secret.reveal() == "SCHWAB-APP-SECRET-VALUE"


def test_guard_defaults_are_the_designs_pinned_values():
    guards = Config.from_mapping(BASE).guards
    assert guards.watchdog_page_minutes == 3
    assert guards.suspect_contract_ratio == 0.70
    assert guards.trailing_median_sessions == 20
    assert guards.battery_row_count_band == 0.30
    assert guards.staleness_page_seconds == 60
    assert guards.dead_man_grace_minutes == 5
    assert guards.min_trailing_sessions == 5


def test_guards_merge_over_the_defaults():
    cfg = Config.from_mapping({**BASE, "guards": {"watchdog_page_minutes": 4}})
    assert cfg.guards.watchdog_page_minutes == 4
    # Untouched fields keep the default.
    assert cfg.guards.staleness_page_seconds == 60


def test_unknown_guard_key_raises():
    with pytest.raises(ConfigError):
        Config.from_mapping({**BASE, "guards": {"nope": 1}})


def test_non_mapping_guards_raises():
    with pytest.raises(ConfigError):
        Config.from_mapping({**BASE, "guards": [1, 2, 3]})


@pytest.mark.parametrize("missing", sorted(BASE))
def test_missing_required_key_raises_and_names_it(missing: str):
    partial = {key: value for key, value in BASE.items() if key != missing}
    with pytest.raises(ConfigError) as exc:
        Config.from_mapping(partial)
    assert missing in str(exc.value)


def test_leading_tilde_in_a_path_is_expanded():
    cfg = Config.from_mapping({**BASE, "lake_root": "~/lake"})
    assert "~" not in str(cfg.lake_root)
    assert str(cfg.lake_root).endswith("/lake")


def test_paths_bridge_returns_a_lakepaths_rooted_at_lake_root():
    cfg = Config.from_mapping(BASE)
    paths = cfg.paths()
    assert isinstance(paths, LakePaths)
    assert paths.root == Path("/data/lake")


def test_healthchecks_url_uses_the_slug_form():
    cfg = Config.from_mapping(BASE)
    assert cfg.healthchecks_url("capture-deadman") == (
        "https://hc-ping.com/PING-KEY-SECRET/capture-deadman"
    )


def test_all_four_secrets_are_secret_wrapped():
    cfg = Config.from_mapping(BASE)
    for value in (
        cfg.healthchecks_ping_key,
        cfg.ntfy_topic,
        cfg.schwab_api_key,
        cfg.schwab_app_secret,
    ):
        assert isinstance(value, Secret)


def test_secret_never_leaks_in_any_string_form():
    cfg = Config.from_mapping(BASE)
    forms = (
        repr(cfg),
        str(cfg),
        f"{cfg}",
        repr(cfg.healthchecks_ping_key),
        str(cfg.ntfy_topic),
        f"{cfg.ntfy_topic}",
        repr(cfg.schwab_api_key),
        str(cfg.schwab_api_key),
        f"{cfg.schwab_api_key}",
        repr(cfg.schwab_app_secret),
        str(cfg.schwab_app_secret),
        f"{cfg.schwab_app_secret}",
    )
    secret_values = (
        "PING-KEY-SECRET",
        "topic-secret-xyz",
        "SCHWAB-API-KEY-SECRET",
        "SCHWAB-APP-SECRET-VALUE",
    )
    for text in forms:
        for secret_value in secret_values:
            assert secret_value not in text


def test_secret_reveal_and_equality():
    assert Secret("a").reveal() == "a"
    assert Secret("a") == Secret("a")
    assert Secret("a") != Secret("b")
    assert Secret("a") != "a"


def test_guardconstants_from_empty_or_none_returns_defaults():
    assert GuardConstants.from_mapping(None) == GuardConstants()
    assert GuardConstants.from_mapping({}) == GuardConstants()
