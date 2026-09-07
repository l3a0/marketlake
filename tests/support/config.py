"""The throwaway config file.

``write_config`` writes a complete ``config.yaml`` into a temporary directory, so a
test can drive a command-line entry that loads its own config rather than taking one
by argument. Every key the loader requires is present, because a missing one is a
different failure than the test is asking about.

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

from pathlib import Path

CONFIG_NAME = "config.yaml"
PING_KEY = "secret-key"
NTFY_TOPIC = "secret-topic"


def write_config(tmp_path: Path, lake_root: Path, *, backup_target: Path | None = None) -> Path:
    """Write a config naming ``lake_root``, and return its path.

    ``backup_target`` defaults to an ``ssd`` directory beside the config, created here.
    """
    target = tmp_path / "ssd" if backup_target is None else backup_target
    target.mkdir(parents=True, exist_ok=True)
    path = tmp_path / CONFIG_NAME
    path.write_text(
        f"lake_root: {lake_root}\n"
        f"backup_target: {target}\n"
        f"healthchecks_ping_key: {PING_KEY}\n"
        f"ntfy_topic: {NTFY_TOPIC}\n"
        "schwab_api_key: api-key\n"
        "schwab_app_secret: app-secret\n"
    )
    return path
