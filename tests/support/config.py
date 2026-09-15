"""The throwaway config file.

``write_config`` writes a complete ``config.yaml`` into a temporary directory, so a
test can drive code that loads its own config rather than taking one by argument. That
was command-line entries alone until ``loader.load_chain`` gained a configured default
for its lake root, and the read layer's tests drive it the same way. Every key the
loader requires is present, because a missing one is a different failure than the test
is asking about.

The ping key is ``secret-key``, which makes the health-check URL predictable. Tests
assert both halves of the design's rule with it: the ping goes to
``hc-ping.com/secret-key/<slug>``, and the key appears in nothing a job prints.

The backup target is created rather than merely named, because a plugged-in drive is
the ordinary machine state and a fixture should model it. No test depends on that
today, since the ones that run the job inject a fake backup. The compaction job does
assert its target is mounted before syncing, so a test that wants the design's
loud failure on an unplugged drive passes a target it did not create.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

CONFIG_NAME = "config.yaml"
PING_KEY = "secret-key"
NTFY_TOPIC = "secret-topic"
# The other two secrets the design names, in the same class as the ping key and the topic.
# A sweep that checks only the first two passes a command that prints these.
SCHWAB_API_KEY = "api-key"
SCHWAB_APP_SECRET = "app-secret"


def write_config(
    tmp_path: Path,
    lake_root: Path,
    *,
    backup_target: Path | None = None,
    guards: Mapping[str, object] | None = None,
    callback_url: str | None = None,
) -> Path:
    """Write a config naming ``lake_root``, and return its path.

    ``backup_target`` defaults to an ``ssd`` directory beside the config, created here.

    ``guards`` renders a ``guards`` section over the pinned defaults, so a test can drive
    a recalibrated guard constant the way a hand edit sets one. Left unset, no section is
    written and every guard keeps its default.

    ``callback_url`` writes the Schwab callback, and is left out by default. That default
    is the point rather than an omission. No capture path reads the key, so the config
    every daemon test runs on is one without it, and requiring the key would turn all of
    them red. Only the re-auth needs it, so only its tests ask for it.
    """
    target = tmp_path / "ssd" if backup_target is None else backup_target
    target.mkdir(parents=True, exist_ok=True)
    section = (
        ""
        if not guards
        else "guards:\n" + "".join(f"  {key}: {value}\n" for key, value in guards.items())
    )
    callback = "" if callback_url is None else f"schwab_callback_url: {callback_url}\n"
    path = tmp_path / CONFIG_NAME
    path.write_text(
        f"lake_root: {lake_root}\n"
        f"backup_target: {target}\n"
        f"healthchecks_ping_key: {PING_KEY}\n"
        f"ntfy_topic: {NTFY_TOPIC}\n"
        f"schwab_api_key: {SCHWAB_API_KEY}\n"
        f"schwab_app_secret: {SCHWAB_APP_SECRET}\n"
        f"{callback}"
        f"{section}"
    )
    return path
