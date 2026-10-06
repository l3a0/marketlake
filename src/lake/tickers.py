"""The tickers module: the portable capture roster.

``tickers.yaml`` is the *portable* half of the configuration. It says what to capture.
Its machine-local counterpart is ``config.yaml``, loaded by ``lake.config``. The two live
side by side in ``~/.config/marketlake/``, and every reader reads the roster there.

The roster's content is tracked in this repository as ``config/tickers.yaml``, so a
change to it is a reviewed pull request. It holds symbols, booleans, cadences and bar
lists, so tracking it commits no secret and no machine path. ``apply_roster`` copies the
tracked file into the config directory, and ``python -m lake.roster apply`` is its
command. No host reads the tracked path in place, because a checkout replaces the file
without a rename, so a reader looping on it can catch it missing or empty.

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
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from lake.paths import TICKERS_FILE, config_dir, temp_write_path

# Overrides the roster's default path, so a test points the loader at a throwaway file.
TICKERS_PATH_ENV = "MARKETLAKE_TICKERS"


def default_tickers_path() -> Path:
    """The roster's default path, resolved through ``paths.config_dir`` on every call.

    A module constant would be fixed when this module is imported. A process that
    imported ``lake`` and only then pointed ``HOME`` or ``MARKETLAKE_CONFIG_DIR`` at a
    throwaway kept the real directory, and on 2026-10-06 a probe doing exactly that
    overwrote a host's live roster. Resolved here, the environment at the moment of the
    read or the write decides. The other config-directory defaults still bind at import,
    and marketlake #715 will convert them.
    """
    return config_dir() / TICKERS_FILE


class TickersError(Exception):
    """Raised for a missing or malformed ``tickers.yaml``, or an unknown ticker."""


@dataclass(frozen=True)
class TickerConfig:
    """One ticker's capture settings.

    ``chain_cadence`` is ``None`` for an equity-only ticker. ``bars`` is a tuple of bar
    frequencies, empty when none are configured. ``enabled`` is the on/off switch: a
    disabled entry stays in the roster but is not captured. It defaults to true, so a
    file with no ``enabled`` key reads as enabled, and turning a ticker off is the only
    time the key is written.
    """

    ticker: str
    options: bool = False
    chain_cadence: str | None = None
    bars: tuple[str, ...] = ()
    enabled: bool = True

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
            enabled=bool(settings.get("enabled", True)),
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

    @property
    def enabled(self) -> tuple[TickerConfig, ...]:
        """The entries that are enabled, in file order.

        The live-capture path captures these. A disabled entry stays in the roster for
        the record but is skipped when fetching. Scope readers do not use this; they read
        the capture-spans file instead.
        """
        return tuple(entry for entry in self.tickers if entry.enabled)

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

    An empty roster is allowed. It used to be an error, because the roster was the only
    record of scope, so a cycle over no tickers captured nothing and left no trace. Now
    the capture-spans file records scope, and retiring the last ticker is a real thing to
    do, so the file may name no tickers. The daemon still runs its health and watchdog
    reporting over an empty roster, it just captures nothing. An absent file is still an
    error, since a fresh machine has no roster and that should be noticed. A missing file
    differs from an empty one: the first is a setup that never happened, the second is a
    roster with everything retired.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    parsed = _parse(_read_text(resolved), resolved)
    mapping = {} if parsed is None else parsed
    if not isinstance(mapping, Mapping):
        raise TickersError(f"tickers file is not a mapping: {resolved}")
    return _roster_from(mapping, resolved)


def _roster_from(mapping: Mapping[str, object], resolved: Path | str) -> Roster:
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


def _parse(text: str, resolved: Path | str) -> object:
    """The parsed document, or a ``TickersError`` naming where the YAML broke.

    A half-saved file is the ordinary way this fails, and where the cut lands decides
    what happens. Some prefixes parse. The rest raise a ``yaml`` error, which is not a
    ``TickersError``, so before this every caller guarding for a bad roster missed them
    and the daemon died on a traceback instead.

    The line number is named because it is the one thing an operator needs and this file
    holds no secrets. ``lake.config`` suppresses the same detail for ``config.yaml``,
    which holds four to eight. Nothing else from the parse error reaches the message, so no line
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
    enabled: bool = True,
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
    ``MARKETLAKE_TICKERS`` environment variable, then the default. This writes the host's
    copy in ``~/.config/marketlake/``. The reviewed copy is the repository's
    ``config/tickers.yaml``, which ``apply_roster`` copies onto each host, so an entry
    written here and not merged there is overwritten by the next apply. The exception is an
    entry whose capture span is open and which the reviewed copy leaves out, disables, or
    turns ``options`` off on. The lake check in ``lake.roster`` refuses that apply
    (marketlake #692). The roster holds no machine path and no secret, which is why
    tracking it commits neither.
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
    # ``enabled`` is written only when off. An enabled entry omits the key, so existing
    # files stay unchanged and ``TickerConfig`` reads a missing key as enabled.
    if not enabled:
        entry["enabled"] = False
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


# How a roster read from standard input is named in a refusal, where a file would be
# named by its path.
STDIN_SOURCE = "<stdin>"


def roster_from_bytes(payload: bytes) -> Roster:
    """The roster ``payload`` holds, or a ``TickersError`` saying why it is not one.

    The bytes must decode as UTF-8 and parse to a mapping the loader accepts, which is
    the validation ``load_tickers`` runs on the file. An absent document is an empty
    roster here, as it is there. Whether an empty roster may be applied is
    ``apply_roster``'s question, not this one's.
    """
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise TickersError(f"tickers file is not UTF-8 text: {STDIN_SOURCE}") from None
    parsed = _parse(text, STDIN_SOURCE)
    mapping = {} if parsed is None else parsed
    if not isinstance(mapping, Mapping):
        raise TickersError(f"tickers file is not a mapping: {STDIN_SOURCE}")
    return _roster_from(mapping, STDIN_SOURCE)


def apply_roster(
    payload: bytes,
    *,
    check: Callable[[Roster], None],
    path: str | Path | None = None,
) -> bool:
    """Copy a reviewed roster's bytes onto this host, and return whether it replaced one.

    This is the core of ``python -m lake.roster apply``. The repository tracks the roster
    as ``config/tickers.yaml`` and every reader keeps reading the host's copy, so this is
    how a merged change reaches a host. It runs four steps in order and stops at the
    first refusal, each a ``TickersError``.

    1. It validates ``payload`` with ``roster_from_bytes``, so nothing is written for
       bytes the loader would refuse.
    2. It refuses a roster with no enabled ticker. Parsing alone accepts an empty
       document, and the daemon feeds the capture dead-man when no ticker is enabled, so
       a host handed one would capture nothing and raise no alarm. The price is that
       retiring the last ticker needs a hand step.
    3. It calls ``check`` with the roster on every call and before any write, including
       when the bytes already match. A refusing check raises, and nothing is written.
       This is where a check against the host's lake runs. It comes in as a callable
       because this module cannot import ``capture`` or ``outbox`` without an import
       cycle. A refusal on an unchanged roster still matters, because it says a ticker
       the lake owes is not being captured now.
    4. It replaces the host's roster when the bytes differ, and leaves it alone when they
       are equal. The bytes are written as given, never re-serialized, so the host's copy
       stays byte for byte the reviewed file.

    The write goes through ``_write_atomically``, because the daemon re-reads the roster
    at the top of every cycle and the rename lets it see only the whole old file or the
    whole new one. Nothing is started or restarted, since the next cycle reads the new
    roster. The path precedence matches ``load_tickers``.
    """
    roster = roster_from_bytes(payload)
    if not roster.enabled:
        raise TickersError(
            f"tickers file names no enabled ticker, so a host would capture nothing: {STDIN_SOURCE}"
        )
    check(roster)
    resolved = _resolve_path(path, None)
    try:
        current: bytes | None = resolved.read_bytes()
    except FileNotFoundError:
        current = None
    except OSError:
        raise TickersError(f"tickers file cannot be read: {resolved}") from None
    if current == payload:
        return False
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        _write_atomically(resolved, payload.decode("utf-8"))
    except OSError:
        raise TickersError(f"tickers file cannot be written: {resolved}") from None
    return True


def set_enabled(
    ticker: str,
    enabled: bool,
    *,
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Turn one ticker's capture on or off in place, keeping its other settings.

    This is how retirement disables a ticker without removing its entry. The entry stays
    in the file so it is easy to turn back on. It preserves ``options``, ``chain_cadence``,
    and ``bars`` and only flips ``enabled``. Enabling drops the key, since a missing key
    reads as enabled. Raises ``TickersError`` if the file or the ticker is absent.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    existing = _read_mapping(resolved)
    if ticker not in existing:
        raise TickersError(f"ticker not in roster: {ticker!r} in tickers file: {resolved}")
    settings = existing[ticker]
    entry = dict(settings) if isinstance(settings, Mapping) else {}
    if enabled:
        entry.pop("enabled", None)
    else:
        entry["enabled"] = False
    existing[ticker] = entry
    _roster_from(existing, resolved)
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        _write_atomically(resolved, yaml.safe_dump(existing, sort_keys=True))
    except OSError:
        raise TickersError(f"tickers file cannot be written: {resolved}") from None
    return resolved


def remove_ticker(
    ticker: str,
    *,
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Remove one ticker's entry from ``tickers.yaml`` and return the file path.

    This is the write-half counterpart to ``upsert_ticker``, used when retirement removes
    a ticker outright rather than disabling it in place. The remainder is validated before
    the write, the same as ``upsert_ticker``. Removing the last entry is allowed and leaves
    an empty roster, which ``load_tickers`` now accepts. Raises ``TickersError`` if the file
    or the ticker is absent.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    existing = _read_mapping(resolved)
    if ticker not in existing:
        raise TickersError(f"ticker not in roster: {ticker!r} in tickers file: {resolved}")
    del existing[ticker]
    _roster_from(existing, resolved)
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        _write_atomically(resolved, yaml.safe_dump(existing, sort_keys=True))
    except OSError:
        raise TickersError(f"tickers file cannot be written: {resolved}") from None
    return resolved


def _read_mapping(resolved: Path) -> dict[str, object]:
    """The roster file's raw mapping, or a ``TickersError`` if it is not a mapping.

    An absent document reads as no entries. This is the shared read the write-half
    functions use before they change one entry.
    """
    parsed = _parse(_read_text(resolved), resolved)
    loaded = {} if parsed is None else parsed
    if not isinstance(loaded, Mapping):
        raise TickersError(f"tickers file is not a mapping: {resolved}")
    return {str(key): value for key, value in loaded.items()}


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


def tickers_file_path(path: str | Path | None = None, env: Mapping[str, str] | None = None) -> Path:
    """The resolved roster path, by the same precedence ``load_tickers`` uses.

    A command that needs to name the roster file, without reading it, uses this. The
    resolution is the same one every read and write here shares.
    """
    return _resolve_path(path, env)


def _resolve_path(path: str | Path | None, env: Mapping[str, str] | None) -> Path:
    """Resolve the roster path: explicit argument, then env var, then the default.

    An argument and an environment override are whatever a person typed, so both may
    carry a ``~`` and both are expanded. The default comes from ``lake.paths`` already
    resolved, at the moment this runs.
    """
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(TICKERS_PATH_ENV)
    if override:
        return Path(override).expanduser()
    return default_tickers_path()
