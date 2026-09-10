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

from lake.paths import TICKERS_FILE, config_dir, temp_write_path

# The portable roster file. Overridable by argument or this environment variable, so a
# test points the loader at a throwaway file.
DEFAULT_TICKERS_PATH = config_dir() / TICKERS_FILE
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

    A file that names no tickers is an error, not an empty roster. The rename in
    ``_write_atomically`` closed the way a torn *write* produced one. A hand edit caught
    partway through a save still does, and so does an operator who empties the file. The
    harm is the one the rename removed on the other side: a cycle over no tickers
    captures nothing and writes no gap row, so the minute leaves no trace, and the design
    counts completeness from rows and never from holes. Nothing writes an empty file. A
    fresh machine has no file at all, which is already an error, and ``upsert_ticker``
    always writes at least one entry.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    parsed = _parse(_read_text(resolved), resolved)
    mapping = {} if parsed is None else parsed
    if not isinstance(mapping, Mapping):
        raise TickersError(f"tickers file is not a mapping: {resolved}")
    if not mapping:
        raise TickersError(f"tickers file names no tickers: {resolved}")
    return _roster_from(mapping, resolved)


def _roster_from(mapping: Mapping[str, object], resolved: Path) -> Roster:
    """The roster for an already-parsed mapping, with the file named on any refusal.

    ``Roster.from_mapping`` takes a mapping and no path, so its message names the entry
    it rejected and nothing else. Three operator-editable files sit in the config
    directory, and ``input_errors_exit`` prints this line on its own. It has to say
    which file. Both halves of this module validate through here, so the read and the
    write refuse the same entries and say so the same way.
    """
    try:
        return Roster.from_mapping(mapping)
    except TickersError as exc:
        raise TickersError(f"{exc} in tickers file: {resolved}") from None


def _read_text(resolved: Path) -> str:
    """The file's text, or a ``TickersError`` naming what could not be read.

    ``exists()`` passing does not mean the file can be read. A restrictive mode raises
    ``OSError`` and a binary file raises ``UnicodeDecodeError``, which is a ``ValueError``
    rather than an ``OSError``. Both went bare before, and every caller that guards for a
    bad roster then missed them. ``lake.config`` solved the same class for ``config.yaml``.
    Only the path is named, so nothing from inside the file reaches the error.
    """
    try:
        return resolved.read_text()
    except (OSError, UnicodeDecodeError):
        raise TickersError(f"tickers file cannot be read: {resolved}") from None


def _parse(text: str, resolved: Path) -> object:
    """The parsed document, or a ``TickersError`` naming where the YAML broke.

    A half-saved file is the ordinary way this fails, and where the cut lands decides
    what happens. Some prefixes parse. The rest raise a ``yaml`` error, which is not a
    ``TickersError``, so before this every caller guarding for a bad roster missed them
    and the daemon died on a traceback instead.

    The line number is named because it is the one thing an operator needs and this file
    holds no secrets. ``lake.config`` suppresses the same detail for ``config.yaml``,
    which holds four. Nothing else from the parse error reaches the message, so no line
    of the file itself is printed. ``from None`` suppresses the original in a traceback
    too. It stays reachable as ``__context__``, which is where a debugger should find it
    and where nothing that prints an operator error looks.

    An absent document comes back as ``None`` and every other value comes back as it is.
    Folding the falsy ones into an empty mapping here would hide four of them. A file
    holding ``0``, ``false``, ``''``, or ``[]`` is not a roster, and each caller turns
    only ``None`` into no entries.
    """
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = "" if mark is None else f" at line {mark.line + 1}"
        raise TickersError(f"tickers file is not valid YAML{where}: {resolved}") from None


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
        parsed = _parse(_read_text(resolved), resolved)
        # An absent document means no entries yet, the same as no file. An empty one is
        # accepted for the same reason: onboarding is how an operator puts an entry back,
        # so refusing it would block the repair.
        loaded = {} if parsed is None else parsed
        if not isinstance(loaded, Mapping):
            raise TickersError(f"tickers file is not a mapping: {resolved}")
        existing = {str(key): value for key, value in loaded.items()}

    entry: dict[str, object] = {"options": bool(options)}
    if options and chain_cadence is not None:
        entry["chain_cadence"] = chain_cadence
    entry["bars"] = [str(freq) for freq in bars]
    existing[ticker] = entry
    # The merged roster is validated before any of it is written, so the write never
    # leaves behind a file the read refuses. The document's shape alone was not enough.
    # An entry cut to ``SPY:`` parses as a mapping and fails only inside
    # ``Roster.from_mapping``, so the write reflowed the operator's file, reported
    # success, and left a daemon that still would not start. Validating after the merge
    # is what makes re-onboarding the broken ticker the repair, because its own entry is
    # replaced before the check runs.
    _roster_from(existing, resolved)

    # The read half folds every failure into a ``TickersError``, and the write half is
    # the operator's to trip the same ways: a parent that is a file rather than a
    # directory, a mode that forbids the write, a symlink pointing at itself. Onboarding
    # runs under ``input_errors_exit``, so leaving these bare printed a traceback for the
    # write where the read printed one line. ``_write_atomically`` still removes its temp
    # file first, and its name never reaches the message.
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        _write_atomically(resolved, yaml.safe_dump(existing, sort_keys=True))
    except OSError:
        raise TickersError(f"tickers file cannot be written: {resolved}") from None
    return resolved


def _write_atomically(target: Path, text: str) -> None:
    """Write the roster through a temp file beside it, a flush, then one rename.

    The daemon re-reads this file while the command writes it. A plain write truncates
    the file first, so a reader can catch it empty or half written. Where the cut lands
    decides what happens next. A two-ticker roster dumps to 66 bytes. Of its 67 prefixes,
    43 raise, which ``_parse`` turns into a ``TickersError``, so the caller sees a refusal
    rather than a wrong roster. Twelve parse to the whole roster and cost nothing. The
    remaining twelve parse to fewer tickers than the file names, with any key the cut
    removed taking its default, so an options ticker comes back equity-only. A cycle
    handed one of those captures less than the roster names and writes no gap row for the
    rest, so the minute leaves no trace. Those twelve are the silent ones this rename
    removes. The empty prefix was a thirteenth until ``load_tickers`` began refusing a
    file that names no tickers, so the loader covers that one and the rename covers the
    rest.
    A rename replaces the file in one step, so every reader sees the whole old roster or
    the whole new one. The chain plan is written this way for the same reason. A crash
    mid-write leaves the prior file intact, and the temp file is removed on any failure.
    The temp path comes from ``paths.temp_write_path``, which owns the one spelling of
    the marker the backup exclusion matches.
    """
    tmp = temp_write_path(target, os.getpid())
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _resolve_path(path: str | Path | None, env: Mapping[str, str] | None) -> Path:
    """Resolve the roster path: explicit argument, then env var, then the default.

    An argument and an environment override are whatever a person typed, so both may
    carry a ``~`` and both are expanded. The default comes from ``lake.paths`` already
    resolved.
    """
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(TICKERS_PATH_ENV)
    if override:
        return Path(override).expanduser()
    return DEFAULT_TICKERS_PATH
