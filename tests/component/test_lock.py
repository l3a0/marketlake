"""The lake-root lock across one real boundary: kernel file locks.

The lock is a blocking advisory ``flock`` on the manifest. The whole point is that a
second asker waits. So these prove the block directly, once with two threads and once
with two real processes contending. The process case is the true target, since the
lock coordinates separate jobs. Two more tests pin that locking the manifest never
alters it: a fresh lake gets a valid empty manifest, and an existing manifest comes
back byte-identical.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import threading
import time
from pathlib import Path

from lake.lock import lake_lock
from lake.manifest import append_manifest, manifest_path, read_manifest


def test_lock_targets_the_manifest_and_creates_it_empty_on_a_fresh_lake(lake_root):
    assert not manifest_path(lake_root).exists()
    with lake_lock(lake_root) as locked:
        assert locked == manifest_path(lake_root)
        assert manifest_path(lake_root).exists()
    # The create-if-absent open leaves a valid empty manifest, read as no entries.
    assert read_manifest(lake_root) == []


def test_locking_an_existing_manifest_leaves_it_byte_identical(lake_root):
    append_manifest(
        lake_root, partition="a", source="capture", sha256="s1", rows=1, fetched_at=None
    )
    append_manifest(
        lake_root, partition="b", source="capture", sha256="s2", rows=2, fetched_at=None
    )
    before = manifest_path(lake_root).read_bytes()
    with lake_lock(lake_root):
        pass
    # The read-only, create-if-absent handle must not truncate or alter the manifest.
    assert manifest_path(lake_root).read_bytes() == before
    assert [e["partition"] for e in read_manifest(lake_root)] == ["a", "b"]


def test_second_thread_blocks_until_the_first_releases(lake_root):
    order: list[str] = []
    first_holds = threading.Event()
    let_first_go = threading.Event()

    def first() -> None:
        with lake_lock(lake_root):
            order.append("first-acquired")
            first_holds.set()
            let_first_go.wait(5)
            order.append("first-releasing")

    def second() -> None:
        first_holds.wait(5)
        order.append("second-attempting")
        with lake_lock(lake_root):
            order.append("second-acquired")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    first_holds.wait(5)
    t2.start()
    # Give the second thread time to reach the lock and block on it.
    time.sleep(0.2)
    assert "second-attempting" in order
    assert "second-acquired" not in order  # still blocked while the first holds

    let_first_go.set()
    t1.join(5)
    t2.join(5)
    assert order.index("second-acquired") > order.index("first-releasing")


# -- two real processes ------------------------------------------------------


def _record(path: str, event: str) -> None:
    """Append one timeline event as a single atomic line."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, (event + "\n").encode("utf-8"))
    finally:
        os.close(fd)


def _holder(root: str, timeline: str, holding: mp.Event, release: mp.Event) -> None:
    with lake_lock(Path(root)):
        _record(timeline, "holder-acquired")
        holding.set()
        release.wait(10)
        _record(timeline, "holder-releasing")


def _waiter(root: str, timeline: str, holding: mp.Event) -> None:
    holding.wait(10)  # make sure the holder has the lock first
    _record(timeline, "waiter-attempting")
    with lake_lock(Path(root)):
        _record(timeline, "waiter-acquired")


def test_second_process_blocks_until_the_first_releases(lake_root, tmp_path):
    ctx = mp.get_context("spawn")
    timeline = str(tmp_path / "timeline.log")
    holding = ctx.Event()
    release = ctx.Event()

    holder = ctx.Process(target=_holder, args=(str(lake_root), timeline, holding, release))
    waiter = ctx.Process(target=_waiter, args=(str(lake_root), timeline, holding))
    holder.start()
    assert holding.wait(10)
    waiter.start()
    # The waiter is now contending. Let it sit blocked, then release the holder.
    time.sleep(0.3)
    release.set()
    holder.join(10)
    waiter.join(10)
    assert holder.exitcode == 0
    assert waiter.exitcode == 0

    events = Path(timeline).read_text().splitlines()
    assert events.index("waiter-acquired") > events.index("holder-releasing")
