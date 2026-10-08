"""Running a reader that must never open a FIFO, without letting a broken one hang the suite.

A read of a FIFO blocks until a writer appears, and a reader that opened one by mistake
never returns. ``without_blocking`` runs the reader in a thread with a join timeout, so a
reader that blocks fails its test instead of hanging the run.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from pathlib import Path

# How long a reader gets before a test calls it blocked. A Sunday run takes a few seconds
# even on a loaded machine, and a reader that opened a FIFO never returns at all.
FIFO_WAIT = 30


def without_blocking[T](fifo: Path, call: Callable[[], T]) -> T:
    """Run ``call`` in a thread, and fail rather than hang if it opened ``fifo``.

    The ``finally`` opens the FIFO for writing only while the thread is still alive, which
    unblocks the read. A fixed reader never opens the FIFO, and opening one for writing with
    nobody reading raises ``ENXIO``.
    """
    box: dict = {}

    def target() -> None:
        try:
            box["value"] = call()
        except BaseException as exc:  # handed back to the test's own thread
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=FIFO_WAIT)
    blocked = thread.is_alive()
    try:
        assert not blocked, f"the reader opened {fifo} and blocked"
    finally:
        if thread.is_alive():
            os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
            thread.join(timeout=FIFO_WAIT)
    if "error" in box:
        raise box["error"]
    return box["value"]
