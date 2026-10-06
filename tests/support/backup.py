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

``mirror_lake`` is the other half. Where ``FakeBackup`` copies nothing, this puts a real
copy of a lake on disk, so a test can point the backup scrub at a target that a clean
sync would have left. No ``rsync`` runs, and none could: the suite's subprocess guard
fails any test that reaches it.

``FakeBackupReader`` is the restore test's seam. A rotted file on a path copy is already
the backup scrub's finding, so the restore never meets one there. A reader that serves
wrong bytes, or fails, after the scrub matched the real copy is what lets a test reach the
restore's own findings.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

# What ``FakeBackupReader`` does with one path.
WRONG = "wrong"  # serve bytes that hash to nothing the manifest recorded
FAIL = "fail"  # raise ``OSError`` on the call, before any byte arrives
FAIL_MIDWAY = "fail-midway"  # serve one chunk of the real bytes, then raise ``OSError``


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


def mirror_lake(lake_root: Path, target: Path) -> Path:
    """Copy a lake to ``target`` the way a clean ``rsync`` sync would leave it.

    A plain whole-tree copy is faithful here. The two ``BACKUP_EXCLUSIONS`` patterns
    name a temp file and the config directory, and a fixture lake holds neither, so
    nothing a real sync would drop is in the tree to drop. A test that wants a backup
    behind its lake mirrors first and writes to the lake after.
    """
    target = Path(target)
    shutil.copytree(Path(lake_root), target, dirs_exist_ok=True)
    return target


class FakeBackupReader:
    """A ``BackupReader`` over a real copy on disk that can lie about chosen paths.

    ``faults`` maps a lake-relative path to ``WRONG``, ``FAIL`` or ``FAIL_MIDWAY``, and
    ``every`` applies one of them to every path not named there. A path with no fault reads
    the real file, so the reader agrees with the scrub unless a test says otherwise.
    ``calls`` records each path asked for, in order, so a test can show what the restore
    read and that it read nothing at all.
    """

    def __init__(
        self,
        target: Path,
        *,
        faults: dict[str, str] | None = None,
        every: str | None = None,
    ) -> None:
        self.target = Path(target)
        self.faults = dict(faults or {})
        self.every = every
        self.calls: list[str] = []

    def __call__(self, rel: str) -> Iterator[bytes]:
        self.calls.append(rel)
        fault = self.faults.get(rel, self.every)
        if fault == FAIL:
            raise OSError(f"fake read failed: {rel}")
        data = (self.target / rel).read_bytes()
        if fault == WRONG:
            return iter([b"not what the manifest recorded"])
        if fault == FAIL_MIDWAY:
            return self._midway(rel, data)
        return iter([data])

    @staticmethod
    def _midway(rel: str, data: bytes) -> Iterator[bytes]:
        yield data[:1]
        raise OSError(f"fake read failed partway: {rel}")
