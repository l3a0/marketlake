"""The tracked roster, ``config/tickers.yaml``, is one every host can capture from.

A change to the roster is a pull request, and ``python -m lake.roster apply`` copies the
merged file onto each host. ``TickerConfig.from_mapping`` reads only the keys it knows,
so a hand edit's ``option: true`` loads a ticker as equity-only and ``enabeld: false``
leaves it enabled, both without an error. Until the file was tracked, ``upsert_ticker``
wrote every roster, so no hand edit reached the loader. This test runs in the required
``test`` check, so a typo fails the pull request that makes it.
"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest
import yaml

from lake.tickers import TickerConfig, load_tickers

ROOT = Path(__file__).resolve().parents[2]
TRACKED_ROSTER = ROOT / "config" / "tickers.yaml"

# The keys an entry may carry, written out rather than read from ``TickerConfig``, so a
# field renamed in the code fails here instead of moving the rule with it.
ALLOWED_KEYS = frozenset({"options", "chain_cadence", "bars", "enabled"})


def roster_problems(text: str) -> list[str]:
    """Every way ``text`` breaks the tracked roster's rules, empty when it keeps them.

    1. At least one entry is enabled, because ``apply`` refuses a roster with none.
    2. Every entry's keys are among ``ALLOWED_KEYS``.
    3. Every options entry names a ``chain_cadence``. Capture does not read it, and the
       rule keeps the file in the shape ``lake.onboard`` writes.
    """
    document = yaml.safe_load(text) or {}
    problems = []
    if not any(settings.get("enabled", True) is not False for settings in document.values()):
        problems.append("no enabled entry")
    for ticker, settings in document.items():
        unknown = sorted(set(settings) - ALLOWED_KEYS)
        if unknown:
            problems.append(f"{ticker}: unknown keys {unknown}")
        if settings.get("options") and "chain_cadence" not in settings:
            problems.append(f"{ticker}: options entry without chain_cadence")
    return problems


def test_the_allowed_keys_are_the_loaders_fields():
    assert ALLOWED_KEYS == {field.name for field in fields(TickerConfig)} - {"ticker"}


def test_the_tracked_roster_loads_with_an_enabled_ticker():
    roster = load_tickers(TRACKED_ROSTER)
    assert roster.enabled


def test_the_tracked_roster_keeps_the_rules():
    assert roster_problems(TRACKED_ROSTER.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        pytest.param(
            "SPY: {option: true, chain_cadence: 1m, bars: [1m, 1d]}\n",
            "SPY: unknown keys ['option']",
            id="option-typo",
        ),
        pytest.param(
            "SPY: {options: true, chain_cadence: 1m, bars: [1m], enabeld: false}\n",
            "SPY: unknown keys ['enabeld']",
            id="enabled-typo",
        ),
        pytest.param(
            "SPY: {options: true, bars: [1m, 1d]}\n",
            "SPY: options entry without chain_cadence",
            id="no-cadence",
        ),
        pytest.param(
            "SPY: {options: false, bars: [1d], enabled: false}\n",
            "no enabled entry",
            id="all-disabled",
        ),
    ],
)
def test_the_rules_catch_a_typo(text, problem):
    assert problem in roster_problems(text)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(
            "SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
            "QQQ: {options: true, chain_cadence: 1m, bars: [1m], enabled: false}\n",
            id="one-enabled-one-disabled",
        ),
        pytest.param("IWM: {options: false, bars: [1d]}\n", id="equity-only-without-cadence"),
    ],
)
def test_the_rules_accept_a_roster_that_keeps_them(text):
    # One enabled entry is enough, and only an options entry needs a cadence.
    assert roster_problems(text) == []
