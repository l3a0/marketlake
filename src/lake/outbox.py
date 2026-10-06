"""The one place the live senders are built, and where a shadow host records instead.

A ping reaches healthchecks and a page reaches a phone, so every process that sends
needs both, and each one used to build its own. Seven ``main`` functions held fourteen
construction sites between them. A switch over what gets built then had to be made at
every one of them, and a site it missed would send for real.

So this module is now the only construction site in the package. A ``main`` asks it for
a ``Senders`` pair and passes the two objects on, exactly as it passed the ones it used
to build. ``tests/unit/test_seam_senders.py`` scans every other module for a reference to
either class and fails on one, so a new site cannot appear without the build going red.

The switch is the config's ``role`` key. A second machine running the full daemon beside
the primary, to prove it before a cutover, must not feed the owner's alarms. A shadow
pinging the real checks would hide a dead primary, and a shadow paging the real topic
would wake the owner for a machine that is not in charge. So the key decides what this
module returns.

1. No ``role`` key, or ``role: primary``, returns the live pair. That is today's
   behaviour, so a config written before the key existed changes nothing.
2. ``role: shadow`` returns two recorders. Each ping and each page becomes one line in
   ``journal/outbox/date=YYYY-MM-DD.jsonl`` under the lake root, and nothing leaves the
   machine.
3. Any other value, including an empty ``role:`` that YAML reads as null and
   ``role: off`` that YAML reads as false, returns the recorders too, and prints one line
   to stderr naming the key and the value. Falling to ``shadow`` rather than refusing
   means no ``main`` stops working. On a primary the mistake surfaces through the alarm
   the design already trusts: its pings stop, and healthchecks pages for the silence.

Each line carries ``v``, ``at`` (the UTC time of the call), ``process`` (the name the
``main`` already passes to ``input_errors_exit``) and ``kind``, which is ``ping`` or
``page``. A ping adds ``check``, which is what follows the known prefix
``config.healthchecks_url("")``. A URL without that prefix records ``check`` as null and
nothing of the URL, so the ping key can never reach the file. A page adds ``event``,
``title`` and ``priority``. Its body is left out, for the reason ``Publisher._record``
leaves it out: the file sits under the lake, where the dashboard can read it.

The file is named by the Eastern date, the way ``Publisher._record`` names its directory,
so two processes never split one evening across two files at UTC midnight. Every line is
one ``O_APPEND`` write through ``manifest.append_line``, the mechanism the timing files
and the ledgers rely on, so separate processes appending to one day's file each land a
whole line.

The recorders raise only ``OSError``. The one-shot jobs catch ``runner.PING_FAILURES``
around a ping, and that tuple holds ``OSError`` and nothing broader, so a ``TypeError``
escaping the sweep's ping would lose the nightly report and the digest after it. Inside
the daemon, ``DeadMan`` and ``Publisher`` already catch ``Exception``.

A recorder writes only inside a lake root that already exists, and otherwise raises
``FileNotFoundError``, as ``Publisher._record`` does. The Sunday job decides whether to
ping on ``root.is_dir()`` and re-reads it on every retry, so a recorder that created the
root on an unmounted volume would turn the next retry into a recorded green ``sunday``.

The live classes are looked up on their defining modules when ``senders`` is called, as
``alert.NtfyTransport`` and ``runner.UrllibPinger``, rather than bound at import. A test
then replaces each class in the one place it is defined, and every ``main`` sees the
replacement without the test naming the ``main``'s own module.

``runner`` imports this module inside ``runner.main`` and ``alert`` inside ``alert.main``,
because this module imports both and a top-level import in either would be a cycle.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

from lake import alert, runner
from lake.calendar import MARKET_TZ
from lake.clock import Clock
from lake.config import ROLE_ABSENT, ROLE_KEY, Config
from lake.manifest import append_line
from lake.paths import LakePaths

# The two roles. Every other value of the key falls to ``SHADOW``.
PRIMARY = "primary"
SHADOW = "shadow"

# The line format's version, as every timing line carries ``TIMING_FORMAT_VERSION``.
OUTBOX_FORMAT_VERSION = 1

# The ``kind`` of each line.
PING_KIND = "ping"
PAGE_KIND = "page"


@dataclass(frozen=True)
class Senders:
    """The transport a ``Publisher`` sends pages through, the pinger a job pings with,
    and the role that chose them."""

    role: str
    transport: alert.Transport
    pinger: runner.Pinger


def role_of(config: Config) -> tuple[str, str | None]:
    """The host's role, and the line to print when the key held neither role.

    An absent key is ``primary``. A present key is ``primary`` or ``shadow`` only when it
    is exactly that string. The loader stores a null from an empty ``role:`` as
    ``"None"`` and a false from ``role: off`` as ``"False"``, so both fall to ``shadow``
    with the line naming what was read.
    """
    value = config.role
    if value is ROLE_ABSENT:
        return PRIMARY, None
    if isinstance(value, str) and value in (PRIMARY, SHADOW):
        return value, None
    return SHADOW, (
        f"{ROLE_KEY} {value!r} is neither {PRIMARY!r} nor {SHADOW!r}, so this process "
        "runs as shadow and records its pings and pages under journal/outbox/"
    )


def senders(config: Config, *, process: str, clock: Clock) -> Senders:
    """The pair a ``main`` sends through: live under ``primary``, recorders otherwise.

    ``process`` is the name the ``main`` passes to ``input_errors_exit``, and it prefixes
    the stderr line for a value that is neither role. ``clock`` is the ``main``'s own,
    because a recorded line needs a time and neither ``ping(url)`` nor ``send(message)``
    carries one.
    """
    role, warning = role_of(config)
    if warning is not None:
        print(f"{process}: {warning}", file=sys.stderr)
    if role == PRIMARY:
        return Senders(
            role=PRIMARY,
            transport=alert.NtfyTransport(config.ntfy_topic.reveal()),
            pinger=runner.UrllibPinger(),
        )
    return Senders(
        role=SHADOW,
        transport=RecordingTransport(config.lake_root, process=process, clock=clock),
        pinger=RecordingPinger(
            config.lake_root,
            process=process,
            clock=clock,
            prefix=config.healthchecks_url(""),
        ),
    )


@contextmanager
def _only_oserror(kind: str) -> Iterator[None]:
    """Let an ``OSError`` through and turn anything else into one."""
    try:
        yield
    except OSError:
        raise
    except Exception as exc:
        raise OSError(f"outbox: the {kind} was not recorded: {type(exc).__name__}") from exc


class _Recorder:
    """Appends one line to the day's outbox file under an existing lake root."""

    def __init__(self, lake_root: Path | str, *, process: str, clock: Clock) -> None:
        self._root = Path(lake_root)
        self._process = str(process)
        self._clock = clock

    def _append(self, kind: str, fields: dict[str, object]) -> None:
        now = self._clock.now()
        entry: dict[str, object] = {
            "v": OUTBOX_FORMAT_VERSION,
            "at": now.astimezone(UTC).isoformat(),
            "process": self._process,
            "kind": kind,
            **fields,
        }
        if not self._root.is_dir():
            raise FileNotFoundError(f"lake root missing: {self._root}")
        path = LakePaths(self._root).outbox_path(now.astimezone(MARKET_TZ).date())
        # The root exists, so this creates ``journal/`` and ``outbox/`` beneath it and
        # never the root itself.
        path.parent.mkdir(parents=True, exist_ok=True)
        append_line(path, entry)


class RecordingPinger(_Recorder):
    """A ``Pinger`` that appends a ``ping`` line rather than making a request."""

    def __init__(self, lake_root: Path | str, *, process: str, clock: Clock, prefix: str) -> None:
        super().__init__(lake_root, process=process, clock=clock)
        self._prefix = prefix

    def ping(self, url: str) -> None:
        with _only_oserror(PING_KIND):
            text = str(url)
            check = text[len(self._prefix) :] if text.startswith(self._prefix) else None
            self._append(PING_KIND, {"check": check})


class RecordingTransport(_Recorder):
    """A ``Transport`` that appends a ``page`` line rather than POSTing to ntfy."""

    def send(self, message: alert.Message) -> None:
        with _only_oserror(PAGE_KIND):
            self._append(
                PAGE_KIND,
                {
                    "event": str(message.event),
                    "title": str(message.title),
                    "priority": int(message.priority),
                },
            )


__all__ = [
    "OUTBOX_FORMAT_VERSION",
    "PAGE_KIND",
    "PING_KIND",
    "PRIMARY",
    "SHADOW",
    "RecordingPinger",
    "RecordingTransport",
    "Senders",
    "role_of",
    "senders",
]
