"""The fake backup runner.

``FakeBackup`` implements the ``BackupRunner`` seam by recording each sync instead of
copying anything. Three facts about a sync matter, and it records all three.

1. The source and target name which pair was synced.
2. A place in a shared event log fixes when the sync happened against the other steps
   of a job. The backup runs before the ping, so a failed sync never lets a success
   ping out.
3. The lake's file listing at that moment shows what the backup actually saw. The
   compaction tests read it to prove the sync ran after the partition was sealed,
   rather than over a half-written one.

A test that cares about one fact ignores the others. Passing no log gives the runner
its own, so ``sync`` never has to ask which kind of test it is serving. Listing a
source that does not exist yields an empty list rather than raising, so a test working
with a path it never created pays nothing for the extra fact.

A backup that fails is a different thing and stays where it is used. One test defines
a runner that raises ``BackupTargetUnavailable`` in place. That test covers the design's
rule that an unplugged drive fails the run loudly rather than skipping the sync.
"""

from __future__ import annotations

from pathlib import Path


class FakeBackup:
    """A ``BackupRunner`` that records each sync rather than copying anything."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = [] if events is None else events
        self.calls: list[tuple[Path, Path]] = []
        self.seen: list[list[str]] = []

    def sync(self, source: Path, target: Path) -> None:
        self.calls.append((source, target))
        self.seen.append(
            sorted(p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file())
        )
        self.events.append("backup")
