"""Config resolved from values alone: defaults, overrides, and secret hiding."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

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


def test_the_callback_url_is_optional_and_absent_by_default():
    """No capture path reads it, so a mapping without it must still resolve.

    Requiring it would take the daemon down for a key only the weekly re-auth uses.
    ``BASE`` is what every other test here builds on and names no callback, so this
    states the property the rest of the suite silently depends on.
    """
    assert Config.from_mapping(BASE).schwab_callback_url is None


def test_the_callback_url_loads_as_a_plain_string_not_a_secret():
    """It is the one Schwab registration input that is not a credential.

    A ``Secret`` redacts in every repr and format, and the re-auth has to print the
    callback so the operator can check it against the app registration. Comparing the
    type as well as the value is what catches a wrap: ``Secret.__eq__`` refuses a plain
    string, but a later ``__eq__`` that did not would make a value-only check pass.
    """
    cfg = Config.from_mapping({**BASE, "schwab_callback_url": "https://127.0.0.1:8182"})
    assert cfg.schwab_callback_url == "https://127.0.0.1:8182"
    assert type(cfg.schwab_callback_url) is str
    # It prints, which is the whole reason it is not wrapped.
    assert "https://127.0.0.1:8182" in repr(cfg)


@pytest.mark.parametrize("written", [None, "", "   "])
def test_a_callback_key_with_no_value_reads_as_absent(written):
    """A key written blank means the operator has not set it, so it must not be carried.

    An empty string would otherwise reach the login flow as a callback URL, where the
    failure is a vendor error rather than the named refusal the re-auth owes.
    """
    assert Config.from_mapping({**BASE, "schwab_callback_url": written}).schwab_callback_url is None


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


def test_the_bar_request_budget_is_pinned_and_overridable():
    """The design's pinned default, and a recalibration that costs a config edit.

    The number nobody yet has the evidence to choose lives here rather than as a module constant
    for exactly this reason: the first saturating run anyone observes should move it without a
    release. Marketlake #478.
    """
    assert GuardConstants().bars_request_budget == 100
    assert GuardConstants.from_mapping({"bars_request_budget": 40}).bars_request_budget == 40


@pytest.mark.parametrize("budget", [0, -1, -100])
def test_a_budget_below_one_is_refused_at_config_load(budget: int):
    """A run allowed no request fetches no bar and never says it did not.

    It would not be *silent*, since the nightly ``bars deferred:`` line would count every
    ticker-day every evening. It would be unrefused: the run reports success, the ping goes out,
    and the only thing saying the lake stopped fetching bars is one count beside two others that
    are non-zero on a healthy evening.

    Every command that loads config wraps the load in ``input_errors_exit``, so a ``ConfigError``
    here reaches the operator as one named line and exit 2 from whichever command they ran, at
    load rather than half way through a walk. That is why no guard is owed inside ``bars``.
    """
    with pytest.raises(ConfigError) as caught:
        GuardConstants.from_mapping({"bars_request_budget": budget})
    # The field and the offending value both appear, because a message naming neither sends the
    # operator looking through a file for which line it meant.
    assert "bars_request_budget" in str(caught.value)
    assert repr(budget) in str(caught.value)


def test_a_valid_budget_leaves_every_other_guard_on_its_pinned_default():
    """The new check merges rather than replacing, which is what ``from_mapping`` promises.

    A range check written as a rebuild rather than a guard would silently drop every other key the
    operator set in the same section.
    """
    guards = GuardConstants.from_mapping({"bars_request_budget": 7, "watchdog_page_minutes": 9})
    assert guards.bars_request_budget == 7
    assert guards.watchdog_page_minutes == 9
    assert guards.chain_chunk_max_split_depth == GuardConstants().chain_chunk_max_split_depth


@pytest.mark.parametrize(
    "raw",
    [
        'bars_request_budget: "100"',
        "bars_request_budget:",
        "bars_request_budget: ~",
        "bars_request_budget: [1, 2]",
        "bars_request_budget: {a: 1}",
        "bars_request_budget: 1.5",
        "bars_request_budget: 100.0",
    ],
)
def test_a_budget_that_is_not_a_whole_number_is_named_rather_than_crashing(raw: str):
    """The check must not dereference what it was written to refuse.

    ``replace`` type-checks nothing, so the value reaching this comparison is whatever YAML
    produced. A bare ``< 1`` raises ``TypeError`` against a string, a list, a mapping and a key
    written with no value, and ``input_errors_exit`` catches ``ChainPlanError``, ``ConfigError``
    and ``TickersError`` and not that one. The operator would meet a traceback and exit 1 from the
    very check written to hand them one line and exit 2.

    The empty-value case is not hypothetical here. ``_optional_text`` records that a key written
    with no value parses to ``None`` as an operator input this file has to name.

    The parametrisation goes through ``yaml.safe_load`` rather than passing Python values, because
    what matters is what the operator's own file can produce rather than what a test can construct.
    """
    mapping = yaml.safe_load(raw)
    with pytest.raises(ConfigError) as caught:
        GuardConstants.from_mapping(mapping)
    assert "bars_request_budget" in str(caught.value)


@pytest.mark.parametrize("raw", ["bars_request_budget: yes", "bars_request_budget: true"])
def test_a_boolean_budget_is_refused_rather_than_read_as_one(raw: str):
    """``bool`` is a subclass of ``int``, so a range check alone lets ``True`` through as 1.

    ``bars_request_budget: yes`` parses to ``True``, which passes ``>= 1`` and then bounds the
    whole nightly walk at a single request. The failure the message names, a run that fetches
    almost nothing and does not refuse, is exactly what that produces one unit later.

    The assertion is on the type as well as the refusal, because ``True == 1`` and a check written
    against the value alone would pass while the guard was gone.
    """
    mapping = yaml.safe_load(raw)
    assert mapping["bars_request_budget"] is True
    with pytest.raises(ConfigError):
        GuardConstants.from_mapping(mapping)
    # The pinned default is a real int, not something that merely equals one.
    assert type(GuardConstants().bars_request_budget) is int
