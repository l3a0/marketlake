"""The tickers module: the portable capture roster.

``tickers.yaml`` is the *portable* half of the configuration. It says what to capture,
and it travels with the token on migration. Its machine-local counterpart is
``config.yaml``, loaded by ``lake.config``. The two live side by side in
``~/.config/marketlake/``.

The file is a mapping from ticker to its capture settings::

    SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}
    QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}

``options`` says whether to capture the option chain. ``chain_cadence`` is how often,
like ``1m`` for one minute. ``bars`` lists the bar frequencies to fetch, like ``1m``
and ``1d``. An equity-only ticker sets ``options: false`` and needs no cadence. The
daemon re-reads this file at the top of every capture cycle, so a new ticker goes live
on the next cycle with no restart.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

# The portable roster file. Overridable by argument or this environment variable, so a
# test points the loader at a throwaway file.
DEFAULT_TICKERS_PATH = Path("~/.config/marketlake/tickers.yaml")
TICKERS_PATH_ENV = "MARKETLAKE_TICKERS"


class TickersError(Exception):
    """Raised for a missing or malformed ``tickers.yaml``, or an unknown ticker."""


@dataclass(frozen=True)
class TickerConfig:
    """One ticker's capture settings.

    ``chain_cadence`` is ``None`` for an equity-only ticker. ``bars`` is a tuple of bar
    frequencies, empty when none are configured.
    """

    ticker: str
    options: bool = False
    chain_cadence: str | None = None
    bars: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, ticker: str, settings: Mapping[str, object]) -> TickerConfig:
        bars = settings.get("bars", ())
        if isinstance(bars, (str, bytes)) or not isinstance(bars, (list, tuple)):
            raise TickersError(f"{ticker}: bars must be a list, got {bars!r}")
        cadence = settings.get("chain_cadence")
        return cls(
            ticker=ticker,
            options=bool(settings.get("options", False)),
            chain_cadence=None if cadence is None else str(cadence),
            bars=tuple(str(freq) for freq in bars),
        )


@dataclass(frozen=True)
class Roster:
    """The full capture roster: one ``TickerConfig`` per ticker, in file order."""

    tickers: tuple[TickerConfig, ...]

    def __iter__(self) -> Iterator[TickerConfig]:
        return iter(self.tickers)

    def __len__(self) -> int:
        return len(self.tickers)

    @property
    def symbols(self) -> tuple[str, ...]:
        """Every ticker symbol, in file order."""
        return tuple(entry.ticker for entry in self.tickers)

    def get(self, ticker: str) -> TickerConfig:
        """The settings for one ticker. Raises ``TickersError`` if it is not present."""
        for entry in self.tickers:
            if entry.ticker == ticker:
                return entry
        raise TickersError(f"ticker not in roster: {ticker!r}")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> Roster:
        """Build a roster from an already-parsed mapping.

        This is the value-only core that ``load_tickers`` calls after reading YAML.
        """
        entries = []
        for ticker, settings in mapping.items():
            if not isinstance(settings, Mapping):
                raise TickersError(f"{ticker}: settings must be a mapping, got {settings!r}")
            entries.append(TickerConfig.from_mapping(str(ticker), settings))
        return cls(tuple(entries))


def load_tickers(
    path: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Roster:
    """Load the portable roster.

    Path precedence mirrors ``load_config``: an explicit ``path``, then the
    ``MARKETLAKE_TICKERS`` environment variable, then the default
    ``~/.config/marketlake/tickers.yaml``.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    mapping = yaml.safe_load(resolved.read_text()) or {}
    if not isinstance(mapping, Mapping):
        raise TickersError(f"tickers file is not a mapping: {resolved}")
    return Roster.from_mapping(mapping)


def upsert_ticker(
    ticker: str,
    *,
    options: bool,
    chain_cadence: str | None = None,
    bars: Sequence[str] = (),
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Add or update one ticker's entry in ``tickers.yaml`` and return the file path.

    Onboarding writes the roster entry itself. This is the write half of the roster
    module, the counterpart to ``load_tickers``. It reads any existing file, sets the
    one ticker's settings, and writes the whole roster back. So re-onboarding a ticker
    replaces its entry rather than duplicating it, which is what *idempotent-friendly*
    means here: running the command twice leaves one clean entry, not two.

    The written entry mirrors the schema ``TickerConfig`` reads back. ``chain_cadence``
    is written only for an options ticker, since an equity-only ticker needs no cadence.
    ``bars`` is written as a plain list.

    The path precedence matches ``load_tickers``: an explicit ``path``, then the
    ``MARKETLAKE_TICKERS`` environment variable, then the default. The roster lives in
    ``~/.config/marketlake/``, outside the repo. It is portable config, never a tracked
    file, so no machine path or secret is committed by writing it.
    """
    resolved = _resolve_path(path, env)
    existing: dict[str, object] = {}
    if resolved.exists():
        loaded = yaml.safe_load(resolved.read_text()) or {}
        if not isinstance(loaded, Mapping):
            raise TickersError(f"tickers file is not a mapping: {resolved}")
        existing = {str(key): value for key, value in loaded.items()}

    entry: dict[str, object] = {"options": bool(options)}
    if options and chain_cadence is not None:
        entry["chain_cadence"] = chain_cadence
    entry["bars"] = [str(freq) for freq in bars]
    existing[ticker] = entry

    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(yaml.safe_dump(existing, sort_keys=True))
    return resolved


def _resolve_path(path: str | Path | None, env: Mapping[str, str] | None) -> Path:
    """Resolve the roster path: explicit argument, then env var, then the default."""
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(TICKERS_PATH_ENV)
    if override:
        return Path(override).expanduser()
    return DEFAULT_TICKERS_PATH.expanduser()
