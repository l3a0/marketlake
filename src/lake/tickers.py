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

import io
import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from lake.paths import TICKERS_FILE, config_dir

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

    A parse failure and a read failure both raise ``TickersError``. That is the type
    ``input_errors_exit`` turns into one named line, and the type the daemon's three
    startup builders and its ``on_skipped`` hook catch. A ``yaml.YAMLError`` or an
    ``OSError`` would walk past all of them.

    The only writer of this file is ``upsert_ticker``, and it writes valid YAML. So a
    file that will not parse came from a hand edit, and a save caught partway through
    is the mechanical way that happens.

    A file that names no tickers is an error, not an empty roster. Nothing writes one.
    A fresh machine has no file at all, which is already an error, and ``upsert_ticker``
    always writes at least one entry. So an empty file means a truncated one, the same
    half-finished save as a parse error, and it now reads the same way. Before this it
    read as a roster of no tickers, and a cycle over no tickers journals nothing, so the
    watchdog charged no ticker and the dead-man went unfed. The operator still got
    paged, but by a dead-man timeout five minutes later rather than by a line naming
    the file.
    """
    resolved = _resolve_path(path, env)
    if not resolved.exists():
        raise TickersError(f"tickers file not found: {resolved}")
    parsed = _parse_yaml(_read_text(resolved), resolved)
    mapping = {} if parsed is None else parsed
    if not isinstance(mapping, Mapping):
        raise TickersError(f"tickers file is not a mapping: {resolved}")
    if not mapping:
        raise TickersError(f"tickers file names no tickers: {resolved}")
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

    Reading and writing both go through the guards ``load_tickers`` uses, so a file
    that will not parse stops the write with a ``TickersError`` rather than a raw
    ``yaml.YAMLError``, and a file that cannot be written says so the same way.
    Onboarding runs under ``input_errors_exit``. A write that clobbered a roster it
    could not read would lose every other ticker in it.

    The reader and the writer part on the empty file. The reader refuses one, because
    nothing writes it and a truncation looks exactly like it. The writer accepts one,
    treating it the same as no file at all. Both mean no entries yet. Onboarding is how
    an operator puts an entry back, so refusing here would block the repair.
    """
    resolved = _resolve_path(path, env)
    existing: dict[str, object] = {}
    if resolved.exists():
        parsed = _parse_yaml(_read_text(resolved), resolved)
        # An absent document reads as no entries. Every other shape the reader refuses
        # is refused here too, so the writer never overwrites a file the reader would
        # not open.
        loaded = {} if parsed is None else parsed
        if not isinstance(loaded, Mapping):
            raise TickersError(f"tickers file is not a mapping: {resolved}")
        existing = {str(key): value for key, value in loaded.items()}

    entry: dict[str, object] = {"options": bool(options)}
    if options and chain_cadence is not None:
        entry["chain_cadence"] = chain_cadence
    entry["bars"] = [str(freq) for freq in bars]
    existing[ticker] = entry

    _write_text(resolved, yaml.safe_dump(existing, sort_keys=True))
    return resolved


def _read_text(resolved: Path) -> str:
    """The file's text, or a ``TickersError`` naming the path.

    ``exists()`` passing does not mean the file can be read. A path one character short
    of the file names its directory, a restrictive mode makes it unreadable, and a
    binary file is not text. Each of those raises a bare ``OSError`` or
    ``UnicodeDecodeError``, which is the traceback the operator-file guards exist to
    avoid. The line matches ``config._read_text`` word for word past the file's name,
    because ``input_errors_exit`` prints every operator file the same way and one bad
    file must not read differently from another.
    """
    try:
        return resolved.read_text()
    except (OSError, UnicodeDecodeError):
        raise TickersError(f"tickers file cannot be read: {resolved}") from None


class _NamedText(io.StringIO):
    """A text stream that answers to the file's path.

    PyYAML labels every position it reports with the name of the stream it read. Handed
    a plain string it invents ``<unicode string>``, a file the operator never typed and
    cannot go look at. Reading through a named stream puts the real path there instead.
    That matters for the one load error PyYAML raises with no line and column, a stray
    control byte, which is one way a half-written file ends.
    """

    def __init__(self, text: str, name: str) -> None:
        super().__init__(text)
        self.name = name


def _write_text(resolved: Path, text: str) -> None:
    """Write the roster back, or a ``TickersError`` naming the path.

    A roster that cannot be written fails for the operator reasons one that cannot be
    read fails: a parent that is a file rather than a directory, a mode that forbids
    the write, a symlink that points at itself. Onboarding turns a ``TickersError``
    into one named line, so leaving these raw would print a traceback for the write
    where the read prints a line.
    """
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(text)
    except OSError:
        raise TickersError(f"tickers file cannot be written: {resolved}") from None


def _parse_yaml(text: str, resolved: Path) -> object:
    """The parsed YAML, or a ``TickersError`` naming the path and the mistake.

    This quotes PyYAML's own complaint, where ``config._parse_yaml`` drops it. That
    loader's file holds secrets, and a parse error echoes the offending source line
    back verbatim, so letting one out could put a secret wherever the error lands.
    ``tickers.yaml`` holds ticker symbols and capture settings. Nothing in it is a
    secret, so the operator is told where the mistake is.

    A file deep enough in brackets still raises ``RecursionError`` from inside
    ``safe_load``, which is not a ``YAMLError`` and is not caught here. Catching it
    would run cleanup on an exhausted stack, and no hand edit reaches the depth it
    takes.
    """
    name = str(resolved)
    try:
        return yaml.safe_load(_NamedText(text, name))
    except yaml.YAMLError as exc:
        message = f"tickers file is not valid YAML: {name}: {_complaint(exc, name)}"
        raise TickersError(message) from None


def _complaint(exc: yaml.YAMLError, name: str) -> str:
    """PyYAML's complaint, squeezed onto one line.

    ``input_errors_exit`` prints one line per bad file, and PyYAML's own ``str`` runs
    to eight lines for the ordinary syntax error. It echoes the offending source line
    under each position it reports, with a caret beneath the column. That echo is the
    reason ``config._parse_yaml`` drops its message whole, and dropping it here is what
    lets this loader quote the rest.

    A *marked* error, which is the kind a syntax mistake raises, carries the facts as
    fields instead. It says what it was reading and where that began, then what it
    tripped on and where. This keeps both. A truncated file trips at the end of the
    file, so the position that finds the typo is the one the first field carries.

    Anything unmarked has no fields to read, so its whole message is squeezed instead.
    PyYAML names the file inside that message and the caller's line already does, so
    the naming is rewritten into the position it introduces.
    """
    if isinstance(exc, yaml.MarkedYAMLError):
        marked = [
            _located(text, mark)
            for text, mark in ((exc.context, exc.context_mark), (exc.problem, exc.problem_mark))
            if text
        ]
        if marked:
            return ", ".join(marked)
    return " ".join(str(exc).split()).replace(f'in "{name}", ', "at ")


def _located(text: str, mark: yaml.Mark | None) -> str:
    """Half a parse complaint, with its position when it carries one.

    PyYAML counts lines and columns from zero. Every editor counts from one, so this
    shifts both. An operator reads the line and goes straight to that spot in the file.
    """
    if mark is None:
        return text
    return f"{text} at line {mark.line + 1}, column {mark.column + 1}"


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
