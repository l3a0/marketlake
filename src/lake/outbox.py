"""The one place the live senders are built.

A ping reaches healthchecks and a page reaches a phone, so every process that sends
needs both, and each one used to build its own. Seven ``main`` functions held fourteen
construction sites between them. A switch over what gets built then had to be made at
every one of them, and a site it missed would send for real.

So this module is now the only construction site in the package. A ``main`` asks it for
a ``Senders`` pair and passes the two objects on, exactly as it passed the ones it used
to build. ``tests/unit/test_seam_senders.py`` scans every other module for a reference to
either class and fails on one, so a new site cannot appear without the build going red.

The classes are looked up on their defining modules when ``senders`` is called, as
``alert.NtfyTransport`` and ``runner.UrllibPinger``, rather than bound at import. A test
then replaces each class in the one place it is defined, and every ``main`` sees the
replacement without the test naming the ``main``'s own module.

``runner`` imports this module inside ``runner.main`` and ``alert`` inside ``alert.main``,
because this module imports both and a top-level import in either would be a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass

from lake import alert, runner
from lake.config import Config


@dataclass(frozen=True)
class Senders:
    """The transport a ``Publisher`` sends pages through, and the pinger a job pings with."""

    transport: alert.Transport
    pinger: runner.Pinger


def senders(config: Config) -> Senders:
    """The live pair: the ntfy POST on the config's topic, and the healthchecks GET."""
    return Senders(
        transport=alert.NtfyTransport(config.ntfy_topic.reveal()),
        pinger=runner.UrllibPinger(),
    )


__all__ = ["Senders", "senders"]
