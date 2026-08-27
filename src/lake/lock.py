"""The lake-root lock.

Every job that changes the lake takes one lock first. So two jobs never touch the
lake at the same time. That is the whole rule. Serialization is a *lock, not a
schedule*. A hand-run compaction and a scheduled one cannot race, because the second
one to ask simply waits for the first to finish.

The lock is a blocking advisory ``flock`` on the manifest, ``manifest.jsonl``.

- ``flock`` is the kernel's file lock. A process asks the kernel to lock a file. The
  kernel grants it to one holder at a time.
- *Blocking* means a second asker waits its turn rather than failing. Entering the
  context manager blocks until the lock is free.
- *Advisory* means the lock is a contract among our own cooperating jobs. It does not
  stop an unrelated program from writing the files. It works because every lake
  mutator agrees to take it first.

The kernel releases the lock automatically when the holder dies. So a crash never
strands a stale lock the way a delete-a-lockfile scheme would. There is no lock to
clean up by hand after a hard kill.

The manifest is the lock file, and that is deliberate. A ``flock`` lock is kernel
state on an open file descriptor, not bytes in the file. So locking ``manifest.jsonl``
writes nothing into it and leaves no stale lock data behind. The manifest is the
rendezvous the single-writer rule already names, so it is the natural thing to lock.
Locking it keeps the scrub's exclusion set exactly ``{manifest.jsonl, journal/}`` as
the doc enumerates, and adds no new file to the lake tree or the backup. The handle is
opened read-only, so accidental truncation or corruption of the integrity root through
the lock path is structurally impossible. ``flock`` works fine on a read-only handle.

Capture workers stay deliberately outside this lock. Blocking a capture cycle behind
compaction would drop perishable minutes. Only the serialized daily jobs and manual
lake-mutating runs take it.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from lake.manifest import manifest_path


@contextmanager
def lake_lock(lake_root: Path) -> Iterator[Path]:
    """Hold the lake-root lock for the duration of the ``with`` block.

    Entering blocks until the lock is acquired. Exiting releases it. Process death
    releases it too, because the kernel drops the lock when the file descriptor
    closes. On a fresh lake this creates a valid empty ``manifest.jsonl``, which
    ``read_manifest`` already reads as no entries.
    """
    path = manifest_path(lake_root)
    # O_RDONLY|O_CREAT opens the manifest read-only, creating it if absent and never
    # truncating it. The descriptor is what the kernel ties the lock to. A read-only
    # handle cannot alter the integrity root, so the lock path can never corrupt it.
    fd = os.open(path, os.O_RDONLY | os.O_CREAT, 0o644)
    try:
        # LOCK_EX with no LOCK_NB blocks until the exclusive lock is granted.
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield path
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
