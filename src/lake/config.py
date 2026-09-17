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

One key is optional rather than required: ``schwab_callback_url``, the third static
app-registration input. Only the weekly re-auth in ``lake.reauth`` reads it, and capture
never does, so a config without it loads and the daemon runs. The re-auth is the one
place that refuses without it, naming the key. It is not wrapped in ``Secret``. A
registered callback is a loopback URL rather than a credential, and the re-auth prints
it so the operator can check it against the Schwab app registration, which a redacting
wrapper would make impossible.

A *guard constant* is a tunable threshold the failure machinery reads, like the
watchdog's page-after count or the suspect-snapshot ratio. The defaults here are the
values the design pins. Slice 1 measures the real distributions and recalibrates them.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import yaml

from lake.chain_plan import ChainPlanError
from lake.paths import CONFIG_FILE, LakePaths, config_dir
from lake.tickers import TickersError

# The machine-local config file. Overridable by argument or this environment variable,
# so a test points the loader at a throwaway file.
DEFAULT_CONFIG_PATH = config_dir() / CONFIG_FILE
CONFIG_PATH_ENV = "MARKETLAKE_CONFIG"

# The healthchecks host. Pings go by slug, in the form ``hc-ping.com/<ping-key>/<slug>``.
# The config holds the one rotatable ping key, never six immutable UUID URLs.
HEALTHCHECKS_HOST = "hc-ping.com"

# The Schwab callback key, spelled once. The re-auth refuses without it and names it,
# and the rendered re-auth script names it too, so all three read this rather than
# repeating the string.
CALLBACK_KEY = "schwab_callback_url"

# The required keys. Guard constants are optional and default to the pinned values, and
# so is ``CALLBACK_KEY``: no capture path reads it, so a config missing it must load
# rather than take the daemon down for a key the daemon has no use for.
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
    # The OI view's freshness test uses the next four. The design names three of them as
    # guard constants and pins no number, and marketlake #137 names the fourth. Slice 1's
    # refresh-moment measurement was meant to calibrate them and cannot: it looks for a
    # later cycle in a session that differs from that session's first, and open interest
    # does not move inside a stored session. So these are provisional placeholders waiting
    # on a calibration that has to come from somewhere else, not design-pinned figures.
    # The minimum comparable-set size below which the OI verdict is *indeterminate*.
    oi_comparable_set_floor: int = 20
    # The fraction of the comparable set that must show changed OI to declare a refresh.
    oi_refresh_quorum: float = 0.50
    # The number of subsequent stored cycles a refreshed OI must hold to be selected.
    oi_plateau_cycles: int = 1
    # How many of session S's top-volume contracts rank into the comparable set. The
    # measurement bounds this from above rather than pinning it. Only 4,443 to 5,287
    # contracts carried non-zero volume in the four sealed close cycles the lake held on
    # 2026-09-16, and a zero-volume contract is exactly what a half-loaded vendor cycle
    # reads as zero, so a set wide enough to admit them is a set the quorum stops
    # protecting. The same cycle changed 45.6 percent of SPY's whole shared set against a
    # 0.50 quorum, a four-point margin, while none of the top 200 by volume changed. 200
    # is that measured margin, not a round number.
    oi_comparable_set_size: int = 200
    # The chain chunker's one constant. A full SPY chain in one request exceeds Schwab's
    # gateway body limit (a 502 with errorcode protocol.http.TooBigBody), so the chain is
    # fetched in date windows and reassembled. The set of windows is not a guard constant.
    # It lives in the machine-owned chain_plan.json, seeded from a measured default and
    # refined by the nightly job, so the plan can drift without a config edit. The one
    # constant here bounds the adaptive fallback: a window that still comes back too big is
    # split at its date midpoint and refetched. This many midpoint splits are tried before
    # the chunker gives up on the offending range. The day-one chain-size measurement, run
    # 2026-09-01, sized both the default plan and this bound: 17 expirations returned at
    # 7.4 MB while the full chain 502'd, so a window near or under ~3 MB has ample margin,
    # and four midpoint splits collapse any oversized window to a single day, which one
    # expiration's ~429 KB always fits.
    chain_chunk_max_split_depth: int = 4
    # The nightly window re-tune's two triggers. The close+15 compaction job groups the
    # day's chains rows by window_start and window_end, takes each plan window's peak
    # per-cycle contract count, and compares it to these two. Both are sized from the
    # day-one measurement, run 2026-09-01: one 7.4 MB response carried 6,278 contracts,
    # about 1.2 KB per contract, and the gateway body limit sits somewhere above 7.4 MB.
    # A window whose peak count is over the max splits at its midpoint offset. 2,500
    # contracts is about 3 MB, well under the limit with room for a dense day. Two
    # adjacent finite windows whose peak counts are both under the min merge into one.
    # 800 contracts is about 1 MB, so a merged pair stays under 2 MB and never nears the
    # split trigger. The open tail is never split and never merged.
    chain_window_max_contracts: int = 2500
    chain_window_min_contracts: int = 800
    # The bar backfill's per-run request budget. `backfill_bars` walks every session the capture
    # spans cover, so a lake whose manifest was rebuilt or restored skips nothing and asks for all
    # of it back to back. Nothing paces that walk, so without a bound one run crosses the vendor
    # ceiling inside its first minute and the ticker-days it loses are never manifested and so are
    # asked for again on the next run. Marketlake #478.
    #
    # The band it sits in is what picks it rather than the digit, and five measurements bound it.
    #
    # 1. The ceiling is 120 a minute per client_id, which the design records as observed and
    #    enforced via 429 rather than contractual. A budget sitting exactly on a non-contractual
    #    ceiling is the wrong place to sit.
    # 2. One run fires at most its budget and the 18:30 job fires once a day, so the budget is also
    #    the most that can reach the vendor in any rolling minute. A budget at or under the ceiling
    #    cannot cross it however fast the run fires, which is why a cap subsumes a pacer here.
    # 3. At 18:30 nothing else draws. CAPTURE_PHASES ends at the 16:15 option close and
    #    `backfill_bars` is the sweep's only vendor caller, so the nightly run has the whole 120.
    # 4. The reservation is owed to the by-hand run instead. `--backfill` takes no date and can be
    #    fired during the session, and the capture loop's draw then is one chain request per ticker
    #    per chain-plan window plus one batched quotes request. At two tickers against the built-in
    #    five-window plan that is 11 a minute, before the midpoint splitter adds any.
    # 5. The steady state is not throttled. Measured read-only against the live lake on an 18:30
    #    clock, a rebuilt manifest today spends 22 requests and an ordinary evening spends 2 to 4.
    #
    # So the band is 22 to 109, and 100 sits inside it leaving 20 for anything else on the same
    # credentials in that minute. The constant is not meaningful past one significant figure,
    # because the ceiling it derives from is itself observed rather than published, so a
    # spuriously precise 109 would claim a precision the input does not have.
    #
    # The guarantee is per run. Two by-hand runs inside one minute put 200 into it, which the
    # design already answers in its own terms: anything else on the same credentials draws from
    # the daemon's 120.
    bars_request_budget: int = 100

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
        merged = replace(cls(), **dict(mapping))
        # **One field is range-checked here, and the rest are not.** A zero or negative
        # ``bars_request_budget`` stops the nightly bar fetch for ever, and it does it without
        # being refused anywhere: the run reports success, the ping goes out, and the only thing
        # saying the lake stopped fetching bars is one count on a report line beside two others
        # that are non-zero on a healthy evening. Every command that loads config wraps the load in
        # ``input_errors_exit``, so raising here reaches the operator as one named line and exit 2
        # from whichever command they ran, at load rather than half way through a walk.
        #
        # This is the instance and not the class. ``from_mapping`` type-checks no value at all,
        # because ``replace`` does not, and eleven constants carry that gap. Marketlake #487 is the
        # per-field range mechanism for all of them. Reaching for it here would be fixing past the
        # class, so the one field this change adds is checked at its own site instead.
        #
        # **The type is checked before the range, and that order is the whole point.** A bare
        # ``< 1`` dereferences whatever YAML produced, and ``<`` against an ``int`` raises
        # ``TypeError`` for a string, a list, and a key written with no value. ``input_errors_exit``
        # catches three named classes and not that one, so the operator would meet a traceback and
        # exit 1 from the very check written to hand them one line and exit 2. A key written with
        # no value is not a hypothetical here: ``_optional_text`` below records that exact shape as
        # an operator input this file has to name.
        #
        # ``bool`` is excluded by hand because it is a subclass of ``int``. ``bars_request_budget:
        # yes`` parses to ``True``, which passes a range check against 1 and then bounds the whole
        # nightly walk at a single request. ``float`` is refused for the same reason rather than
        # rounded: a budget is a count of requests, and 1.5 of them is not a quantity the walk can
        # spend.
        budget = merged.bars_request_budget
        if not isinstance(budget, int) or isinstance(budget, bool) or budget < 1:
            raise ConfigError(
                f"bars_request_budget must be a whole number of at least 1, got {budget!r}: "
                "a run that may spend no request never fetches a bar and never says so"
            )
        return merged


@dataclass(frozen=True)
class Config:
    """The resolved machine-local configuration."""

    lake_root: Path
    backup_target: Path
    healthchecks_ping_key: Secret
    ntfy_topic: Secret
    schwab_api_key: Secret
    schwab_app_secret: Secret
    schwab_callback_url: str | None = None
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
        leading ``~`` are expanded to the home directory. ``schwab_callback_url`` is not
        a required key, so a mapping without it yields ``None`` there and every other
        value as usual.
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
            schwab_callback_url=_optional_text(mapping.get(CALLBACK_KEY)),
            guards=GuardConstants.from_mapping(mapping.get("guards")),
        )


def _optional_text(value: object) -> str | None:
    """An optional string value, or ``None`` when the key is absent or left empty.

    A key written with no value parses to ``None``, and one written as blank spaces
    parses to a string that names nothing. Both mean the operator has not set it, so
    both become ``None`` and the one tool that needs the value refuses with the key
    named rather than carrying an empty string into a login flow.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_config(
    path: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Config:
    """Load the machine-local config.

    Path precedence: an explicit ``path`` argument, then the ``MARKETLAKE_CONFIG``
    environment variable, then the default ``~/.config/marketlake/config.yaml``. A test
    passes ``path`` or an ``env`` mapping to point the loader at a throwaway file.

    A parse failure names the file and nothing else. PyYAML quotes the offending line
    back in its message, and four of this file's values are secrets, so a stray quote
    on the ping-key line would put that key in the error. Jobs run from launchd with
    stdout and stderr going to a log file, so an uncaught traceback writes it to disk.
    ``_parse_yaml`` drops the parse error rather than chaining it, and the ``ConfigError``
    is raised outside that handler, so the quoted line is on neither the traceback nor
    the exception's ``__context__``.
    """
    resolved = _resolve_path(path, env, CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH)
    if not resolved.exists():
        raise ConfigError(f"config file not found: {resolved}")
    mapping = _parse_yaml(_read_text(resolved, "config"))
    if mapping is None:
        raise ConfigError(f"config file is not valid YAML: {resolved}")
    if not isinstance(mapping, Mapping):
        raise ConfigError(f"config file is not a mapping: {resolved}")
    return Config.from_mapping(mapping)


def _parse_yaml(text: str) -> object | None:
    """The parsed YAML, or ``None`` when ``text`` is not YAML at all.

    The parse error stays inside this function and is never re-raised. Its message
    quotes the offending source line, and this file holds four secrets, so letting it
    out would put one of them wherever the caller's error lands. An empty file and a
    ``null`` document both parse to an empty mapping, so ``None`` means the parse
    failed and nothing else.
    """
    try:
        return yaml.safe_load(text) or {}
    except yaml.YAMLError:
        return None


@contextmanager
def input_errors_exit(command: str) -> Iterator[None]:
    """Turn a bad operator input file into one named line and exit 2.

    Three machine-local files are the operator's to edit, and all three sit in the
    config directory: ``config.yaml``, ``tickers.yaml``, and ``chain_plan.json``. A
    malformed one is an operator mistake, not a bug, so a traceback names the wrong
    thing. The loader is the last frame printed and the line that matters sits under a
    stack to read past. This prints that line and exits 2, the code and the shape
    ``argparse`` already uses for a bad argument in these same entries.

    It wraps the call rather than the load, because two entries load their files inside
    a library helper. Those helpers keep raising, and only a ``main`` turns an
    exception into an exit code.
    """
    try:
        yield
    except (ChainPlanError, ConfigError, TickersError) as exc:
        print(f"{command}: {exc}", file=sys.stderr)
        raise SystemExit(2) from None


def _read_text(resolved: Path, kind: str) -> str:
    """The file's text, or a ``ConfigError`` naming what could not be read.

    ``exists()`` passing does not mean the file can be read. A path one character
    short of the file names its directory, a restrictive mode makes it unreadable, and
    a binary file is not text. Each of those raised a bare ``OSError`` before, which is
    the traceback this module exists to avoid. Only the path is named, never the
    exception's own message, so nothing from inside the file can reach the error.
    """
    try:
        return resolved.read_text()
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"{kind} file cannot be read: {resolved}") from None


def _resolve_path(
    path: str | Path | None,
    env: Mapping[str, str] | None,
    env_key: str,
    default: Path,
) -> Path:
    """Resolve a config path: explicit argument, then env var, then the default.

    An argument and an environment override are whatever a person typed, so both may
    carry a ``~`` and both are expanded. ``default`` comes from ``lake.paths`` already
    resolved, so it is returned as it is. A caller passing an unexpanded default would
    get it back unexpanded.
    """
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(env_key)
    if override:
        return Path(override).expanduser()
    return default
