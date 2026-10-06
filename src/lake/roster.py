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

The lake check is the seam marketlake #692 fills. ``apply`` takes it as a callable given
the roster and the loaded config, and the command line passes none yet, so step 5 runs a
check that accepts everything.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from lake.config import Config, input_errors_exit, load_config
from lake.tickers import Roster, apply_roster, roster_from_bytes, tickers_file_path

# The two outcomes a successful apply prints, one per run.
REPLACED = "replaced"
UNCHANGED = "unchanged"


class RosterError(Exception):
    """Raised when this host refuses the apply for a reason outside the roster itself."""


def _accept_every_roster(roster: Roster, config: Config) -> None:
    """The lake check until one exists: it refuses nothing."""


def apply(
    payload: bytes,
    *,
    check: Callable[[Roster, Config], None] | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    geteuid: Callable[[], int] = os.geteuid,
) -> str:
    """Apply ``payload`` as this host's roster, and return ``REPLACED`` or ``UNCHANGED``.

    The steps and their order are the module docstring's. ``geteuid`` is injected so a
    test can ask the root question without being root. ``check`` refuses by raising.
    Paths resolve the way the daemon's do unless a test passes them.
    """
    if geteuid() == 0:
        raise RosterError(
            "refusing to run as root, because a root-owned roster cannot be rewritten "
            "by onboard or retire; run it as the owner"
        )
    roster_from_bytes(payload)
    config = load_config(config_path, env=env)
    lake_check = _accept_every_roster if check is None else check
    replaced = apply_roster(
        payload,
        check=lambda roster: lake_check(roster, config),
        path=tickers_path,
        env=env,
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


__all__ = ["REPLACED", "UNCHANGED", "RosterError", "apply", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
