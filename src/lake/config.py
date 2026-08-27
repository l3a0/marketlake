"""The configuration module: the ``DATA_DIR`` pattern.

Every machine-specific location and every guard constant resolves through this one
module. Relocating the lake or retargeting the backup is a one-line edit to
``~/.config/marketlake/config.yaml``, never a code change. That single-source rule is
the design's ``DATA_DIR`` pattern.

The file is *machine-local* config: where things live on this machine and how to alert
from it. It is the counterpart to the *portable* ``tickers.yaml`` roster, which says
what to capture and travels on migration. This module loads the machine-local half.
The roster lives in ``lake.tickers``.

Four of the values are secrets. The healthchecks ping key builds the health-ping URLs.
The ntfy topic is an unauthenticated channel that anyone holding the name can read and
spoof. The Schwab API key and app secret are the static app-registration inputs
``schwab-py`` needs to build the client and refresh the token. The rotating token
itself is not here. It lives at ``~/.config/marketlake/token.json`` and is handled
elsewhere. All four secrets are wrapped in ``Secret``, which redacts itself in every
log, repr, and traceback. The one caller that must use a raw value calls ``reveal``. So
a stray ``print(config)`` or a logged exception never leaks any of them.

A *guard constant* is a tunable threshold the failure machinery reads, like the
watchdog's page-after count or the suspect-snapshot ratio. The defaults here are the
values the design pins. Slice 1 measures the real distributions and recalibrates them.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import yaml

from lake.paths import LakePaths

# The machine-local config file. Overridable by argument or this environment variable,
# so a test points the loader at a throwaway file.
DEFAULT_CONFIG_PATH = Path("~/.config/marketlake/config.yaml")
CONFIG_PATH_ENV = "MARKETLAKE_CONFIG"

# The healthchecks host. Pings go by slug, in the form ``hc-ping.com/<ping-key>/<slug>``.
# The config holds the one rotatable ping key, never six immutable UUID URLs.
HEALTHCHECKS_HOST = "hc-ping.com"

# The required keys. Guard constants are optional and default to the pinned values.
_REQUIRED_KEYS = (
    "lake_root",
    "backup_target",
    "healthchecks_ping_key",
    "ntfy_topic",
    "schwab_api_key",
    "schwab_app_secret",
)


class ConfigError(Exception):
    """Raised for a missing config file, a missing required key, or an unknown guard."""


class Secret:
    """A string value that never reveals itself except through ``reveal``.

    Its repr, str, and format all redact. So the ping key and ntfy topic stay out of
    logs, tracebacks, and any accidental string conversion of the config. The one
    caller that must use the raw value, such as building a ping URL or POSTing to
    ntfy, calls ``reveal``.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        """The raw value, for the one caller that must use it."""
        return self._value

    def __repr__(self) -> str:
        return "Secret(***redacted***)"

    __str__ = __repr__

    def __format__(self, spec: str) -> str:
        return self.__repr__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass(frozen=True)
class GuardConstants:
    """The guard constants, with the design's pinned defaults.

    Slice 1 measures the real distributions and recalibrates these. Until then the
    defaults here are what the design pins. Each is glossed at its field.
    """

    # The watchdog pages when a per-ticker, per-surface counter reaches this many
    # consecutive session minutes with no durable data cycle.
    watchdog_page_minutes: int = 3
    # A chain snapshot is tagged *suspect* when its contract count falls below this
    # fraction of the trailing-median count.
    suspect_contract_ratio: float = 0.70
    # The trailing window, in sessions, the suspect and battery medians compute over.
    trailing_median_sessions: int = 20
    # The battery's row-count band. A snapshot passes within plus or minus this fraction
    # of the trailing median.
    battery_row_count_band: float = 0.30
    # A feed pages as delayed when session-median staleness exceeds this many seconds.
    staleness_page_seconds: int = 60
    # The dead-man ping's grace, in minutes, before a missed ping pages. It is looser
    # than the watchdog's count because it measures missing network reports, not missing
    # data.
    dead_man_grace_minutes: int = 5
    # Median-relative checks with fewer than this many trailing sessions still run but
    # tag their rows *insufficient_history* instead of clean.
    min_trailing_sessions: int = 5
    # The OI view's freshness test uses the next three. The design names them as guard
    # constants but pins no number. Slice 1's refresh-moment measurement calibrates
    # them, so the values here are provisional placeholders, not design-pinned figures.
    # The minimum comparable-set size below which the OI verdict is *indeterminate*.
    oi_comparable_set_floor: int = 20
    # The fraction of the comparable set that must show changed OI to declare a refresh.
    oi_refresh_quorum: float = 0.50
    # The number of subsequent stored cycles a refreshed OI must hold to be selected.
    oi_plateau_cycles: int = 1

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object] | None) -> GuardConstants:
        """Merge a config's ``guards`` section over the pinned defaults.

        An unrecognized guard key raises rather than being silently ignored. A typo in
        a recalibration would otherwise revert to the default without a word.
        """
        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            raise ConfigError(f"guards must be a mapping, got {type(mapping).__name__}")
        if not mapping:
            return cls()
        known = {f.name for f in fields(cls)}
        unknown = set(mapping) - known
        if unknown:
            raise ConfigError(f"unknown guard constant(s): {sorted(unknown)}")
        return replace(cls(), **dict(mapping))


@dataclass(frozen=True)
class Config:
    """The resolved machine-local configuration."""

    lake_root: Path
    backup_target: Path
    healthchecks_ping_key: Secret
    ntfy_topic: Secret
    schwab_api_key: Secret
    schwab_app_secret: Secret
    guards: GuardConstants = field(default_factory=GuardConstants)

    def paths(self) -> LakePaths:
        """The lake path builder rooted at ``lake_root``. The DATA_DIR-to-paths bridge."""
        return LakePaths(self.lake_root)

    def healthchecks_url(self, slug: str) -> str:
        """The health-ping URL for a check ``slug``: ``hc-ping.com/<ping-key>/<slug>``.

        Built here so the ping key stays wrapped in ``Secret`` everywhere else. Log the
        slug, never this URL.
        """
        return f"https://{HEALTHCHECKS_HOST}/{self.healthchecks_ping_key.reveal()}/{slug}"

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> Config:
        """Build a config from an already-parsed mapping.

        This is the value-only core that ``load_config`` calls after reading YAML. A
        missing required key raises ``ConfigError`` naming the key. Paths carrying a
        leading ``~`` are expanded to the home directory.
        """
        missing = [key for key in _REQUIRED_KEYS if mapping.get(key) is None]
        if missing:
            raise ConfigError(f"config missing required key(s): {missing}")
        return cls(
            lake_root=Path(str(mapping["lake_root"])).expanduser(),
            backup_target=Path(str(mapping["backup_target"])).expanduser(),
            healthchecks_ping_key=Secret(str(mapping["healthchecks_ping_key"])),
            ntfy_topic=Secret(str(mapping["ntfy_topic"])),
            schwab_api_key=Secret(str(mapping["schwab_api_key"])),
            schwab_app_secret=Secret(str(mapping["schwab_app_secret"])),
            guards=GuardConstants.from_mapping(mapping.get("guards")),
        )


def load_config(
    path: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Config:
    """Load the machine-local config.

    Path precedence: an explicit ``path`` argument, then the ``MARKETLAKE_CONFIG``
    environment variable, then the default ``~/.config/marketlake/config.yaml``. A test
    passes ``path`` or an ``env`` mapping to point the loader at a throwaway file.
    """
    resolved = _resolve_path(path, env, CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH)
    if not resolved.exists():
        raise ConfigError(f"config file not found: {resolved}")
    mapping = yaml.safe_load(resolved.read_text()) or {}
    if not isinstance(mapping, Mapping):
        raise ConfigError(f"config file is not a mapping: {resolved}")
    return Config.from_mapping(mapping)


def _resolve_path(
    path: str | Path | None,
    env: Mapping[str, str] | None,
    env_key: str,
    default: Path,
) -> Path:
    """Resolve a config path: explicit argument, then env var, then the default."""
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(env_key)
    if override:
        return Path(override).expanduser()
    return default.expanduser()
