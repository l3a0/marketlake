"""The roster command: copy the reviewed roster onto this host.

The capture roster is tracked in this repository as ``config/tickers.yaml``, so a change
to it is a reviewed pull request. Every reader on a host keeps reading
``~/.config/marketlake/tickers.yaml``, and this command is how the tracked file gets
there::

    python -m lake.roster apply < config/tickers.yaml

It reads the roster's bytes on standard input and calls no network service. The work is
``tickers.apply_roster``. This module adds the two refusals that are about the host
rather than the roster, and runs six steps in order, stopping at the first refusal.

1. It refuses to run as root. A root-owned roster would leave ``lake.onboard`` and
   ``lake.retire`` on the host unable to rewrite it.
2. It validates the payload: UTF-8 text that parses as a roster. Nothing is written
   otherwise, and no temp file is left behind.
3. It refuses without ``config.yaml``, loaded with ``load_config`` so the default path
   resolves the way the daemon's does. The daemon cannot run without that file, and a
   check against the host's lake reads ``lake_root`` and ``role`` from it.
4. It refuses a roster with no enabled ticker.
5. It runs the lake check on every call, before any write, even when the bytes match.
6. It replaces the host's roster when the bytes differ and leaves it alone when they are
   equal, printing a ``replaced`` or ``unchanged`` line.

It never starts or restarts a unit. The daemon re-reads the roster at the top of every
cycle, so the next cycle uses it.

A refusal prints one line on stderr and exits 2, through ``config.input_errors_exit``,
the shape every entry here uses for an operator mistake.

The lake check is ``check_lake`` (marketlake #692). A tracked roster can disagree with the
host's lake. A retire pull request can merge before ``lake.retire`` ran, a restore can
come from a bucket that lags a retire, and an onboard can run on a host with no pull
request yet. In each case the lake has an open capture span that the roster does not
capture, and nothing else pages on that, so the minutes are lost. The check refuses that
roster before it is written. A refusal on a replace leaves the old roster in place, and a
refusal on equal bytes says a ticker the lake owes is not being captured now.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

import yaml

from lake import outbox
from lake.calendar import MARKET_TZ
from lake.capture_spans import CaptureSpans, spans_path
from lake.clock import Clock, SystemClock
from lake.config import Config, input_errors_exit, load_config
from lake.onboard import DEFAULT_BARS, DEFAULT_CHAIN_CADENCE
from lake.security_master import SecurityMaster, master_path
from lake.tickers import Roster, apply_roster, roster_from_bytes, tickers_file_path

# The two outcomes a successful apply prints, one per run.
REPLACED = "replaced"
UNCHANGED = "unchanged"


class RosterError(Exception):
    """Raised when this host refuses the apply for a reason outside the roster itself."""


def _one_line(exc: BaseException) -> str:
    """The exception's class and message on one line, since pyarrow's carry newlines."""
    message = " ".join(str(exc).split())
    name = type(exc).__name__
    return f"{name}: {message}" if message else name


def _read_reference[T](path: Path, read: Callable[[Path], T]) -> T | None:
    """The reference file at ``path``, ``None`` when it is missing, or a ``RosterError``.

    Only ``FileNotFoundError`` means missing. That covers an empty mount point and a
    dangling symlink. Every other exception is damage and refuses: a bare ``OSError``,
    which is how pyarrow reports most corruption, a ``PermissionError``, a
    ``NotADirectoryError`` when ``lake_root`` or ``reference`` is a file, and the module's
    unreadable and unsupported-version classes. ``reference_read.read_or_none`` is not
    used, because it answers ``None`` for an unreadable file, which here would read as
    missing and skip the check on a shadow.
    """
    try:
        return read(path)
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise RosterError(
            f"lake check refused: {path} cannot be read ({_one_line(exc)}), so this roster "
            "cannot be checked against the lake's open capture spans"
        ) from None


def _onboard_entry(ticker: str, options: bool) -> str:
    """The roster entry ``lake.onboard`` writes by default, as one line of YAML.

    It carries ``bars`` on purpose. An entry naming only ``options`` would load with no
    bars, which stops that ticker's bars without a word.

    ``yaml.safe_dump`` writes the line, so a ticker that YAML 1.1 reads as another type
    comes out quoted. Printed bare, ``ON`` would paste back as the boolean ``True`` and
    name a ticker called ``True``. The dump is a one-entry flow mapping, and its outer
    braces are dropped so the line pastes into the roster as one more entry.
    """
    entry: dict[str, object] = {"options": options}
    if options:
        entry["chain_cadence"] = DEFAULT_CHAIN_CADENCE
    entry["bars"] = list(DEFAULT_BARS)
    flow = yaml.safe_dump(
        {ticker: entry}, default_flow_style=True, sort_keys=False, width=sys.maxsize
    )
    return flow.strip()[1:-1]


def _flag(options: bool) -> str:
    return "true" if options else "false"


def check_lake(roster: Roster, config: Config, *, clock: Clock) -> None:
    """Refuse ``roster`` when it does not capture every open capture span in the lake.

    It runs on every apply, before any write. It prints one line for each outcome, so a
    check that ran is never mistaken for one that skipped. A pass and a skip go to stdout,
    and a warning goes to stderr. A refusal raises ``RosterError``, which ``main`` prints
    as one line. The steps run in this order.

    1. It reads both reference files before deciding anything, so a damaged file is never
       hidden behind a missing one. ``_read_reference`` says what refuses.
    2. A missing file skips the check only when ``outbox.role_of`` reads the role as
       exactly ``shadow`` with no warning, which is the shadow host's empty lake. On a
       primary a missing file means an unmounted volume, a wrong ``lake_root`` or a lake
       never restored. A role that fell to shadow from a typo must not skip either, so
       both refuse.
    3. For each span with ``end is None``, it names the ticker with the master on today's
       market date, the expression ``capture._live_roster`` uses. It never selects with
       ``SecurityMaster.in_scope``, which still answers yes for a retired instrument. A
       span the roster does not capture refuses: no entry names it, the entry is
       disabled, or the entry has ``options`` off while the span has it on. Capture
       fetches chains only for an entry with ``options`` set, so that last case would
       lose the chain minutes with nothing paging. The opposite mismatch only captures
       more and is not refused. A span records nothing about ``bars`` or
       ``chain_cadence``, so neither is checked.
    4. A span the master cannot name today warns and does not refuse. No roster can fix
       that state, and refusing a fresh write would leave every ticker uncaptured.

    Any exception from naming a span refuses as one line, as a damaged file does. A
    master that read cleanly can still carry a null ``valid_from``, which raises
    ``TypeError`` there.

    ``capture._live_roster`` says a missing reference file must never stop capture. That
    rule protects the daemon's unattended loop. Here a refused apply leaves the daemon
    down on a fresh write, which the capture dead-man pages, or leaves the old roster
    running on a replace. That beats capturing into an unmounted mount point or writing a
    roster that stops a restore.
    """
    root = config.lake_root
    role, warning = outbox.role_of(config)
    if warning is not None:
        print(f"roster: {warning}", file=sys.stderr)
    master_file = master_path(root)
    spans_file = spans_path(root)
    master = _read_reference(master_file, SecurityMaster.read)
    spans = _read_reference(spans_file, CaptureSpans.read)
    if master is None or spans is None:
        missing = master_file if master is None else spans_file
        if (role, warning) == (outbox.SHADOW, None):
            print(
                f"roster: lake check skipped, because {missing} does not exist "
                "and this host's role is shadow"
            )
            return
        raise RosterError(
            f"lake check refused: {missing} does not exist and this host's role is not "
            "exactly shadow, so its lake is unmounted, misconfigured or not yet restored; "
            "restore the lake or fix lake_root before applying a roster"
        )
    on = clock.now().astimezone(MARKET_TZ).date()
    entries = {entry.ticker: entry for entry in roster}
    open_spans = [span for span in spans if span.end is None]
    problems: list[str] = []
    try:
        for span in open_spans:
            ticker = master.symbol_at(span.instrument_id, on)
            if ticker is None:
                print(
                    f"roster: instrument {span.instrument_id} has an open capture span in "
                    f"{root} (options {_flag(span.options)}) and no ticker in the security "
                    f"master on {on.isoformat()}, so this check cannot name it",
                    file=sys.stderr,
                )
                continue
            entry = entries.get(ticker)
            if entry is not None and not entry.enabled:
                problems.append(
                    f"{ticker} is disabled in this roster while its capture span in {root} "
                    f"is open (options {_flag(span.options)}); if the merged pull request "
                    f"meant to retire it, run python -m lake.retire {ticker} on this host "
                    "after the close and deploy again, otherwise fix config/tickers.yaml "
                    "in a pull request"
                )
            elif entry is None or (span.options and not entry.options):
                found = (
                    "no entry in this roster names it"
                    if entry is None
                    else "this roster's entry has options false"
                )
                problems.append(
                    f"{ticker} has an open capture span in {root} with options "
                    f"{_flag(span.options)}, and {found}; add this entry to "
                    "config/tickers.yaml in a pull request, built from onboard's defaults: "
                    f"{_onboard_entry(ticker, span.options)}"
                )
    except Exception as exc:
        raise RosterError(
            f"lake check refused: naming the open capture spans in {root} failed "
            f"({_one_line(exc)}), so this roster cannot be checked against them"
        ) from None
    if problems:
        raise RosterError(
            "lake check refused, and this host's roster is left as it was: " + "; ".join(problems)
        )
    count = len(open_spans)
    noun = "span" if count == 1 else "spans"
    print(f"roster: lake check passed, {count} open capture {noun} checked in {root}")


def apply(
    payload: bytes,
    *,
    check: Callable[[Roster, Config], None] | None = None,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    geteuid: Callable[[], int] | None = None,
) -> str:
    """Apply ``payload`` as this host's roster, and return ``REPLACED`` or ``UNCHANGED``.

    The steps and their order are the module docstring's. ``geteuid`` is injected so a
    test can ask the root question without being root. It defaults to ``os.geteuid``,
    looked up at call time, so a test driving ``main`` can replace that instead.
    ``check`` refuses by raising. It defaults to ``check_lake`` reading ``clock``, which
    is the system clock unless a test passes one. Paths resolve the way the daemon's do
    unless a test passes them.
    """
    if (geteuid or os.geteuid)() == 0:
        raise RosterError(
            "refusing to run as root, because a root-owned roster cannot be rewritten "
            "by onboard or retire; run it as the owner"
        )
    roster_from_bytes(payload)
    config = load_config(config_path)
    if check is None:
        lake_clock = SystemClock() if clock is None else clock

        def lake_check(roster: Roster, config: Config) -> None:
            check_lake(roster, config, clock=lake_clock)

    else:
        lake_check = check
    replaced = apply_roster(
        payload,
        check=lambda roster: lake_check(roster, config),
        path=tickers_path,
    )
    return REPLACED if replaced else UNCHANGED


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.roster",
        description="Manage this host's copy of the tracked capture roster.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "apply",
        help="Copy the roster on stdin to this host's tickers.yaml.",
        description=(
            "Copy the roster read on stdin to this host's tickers.yaml, as in "
            "'python -m lake.roster apply < config/tickers.yaml'."
        ),
    )
    return parser


def _read_stdin() -> bytes:
    """The roster's bytes from standard input, or a ``RosterError`` naming why not.

    A process started with its standard input closed, as ``<&-`` does, has ``sys.stdin``
    set to ``None``, and a read can fail with ``OSError``. Either escaped as a traceback
    before, so both are refusals here, printed as one line with exit 2.
    """
    stdin = sys.stdin
    if stdin is None:
        raise RosterError(
            "standard input is closed; pipe the roster in, as in "
            "'python -m lake.roster apply < config/tickers.yaml'"
        )
    try:
        return stdin.buffer.read()
    except OSError as exc:
        raise RosterError(f"cannot read the roster from standard input ({exc})") from None


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.roster`` entry. Returns a process exit code."""
    _build_parser().parse_args(argv)
    with input_errors_exit("roster", RosterError):
        outcome = apply(_read_stdin())
    print(f"roster: {outcome} {tickers_file_path()}")
    return 0


__all__ = ["REPLACED", "UNCHANGED", "RosterError", "apply", "check_lake", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
