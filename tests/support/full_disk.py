"""A disk that refuses the token write, injected through ``lake.reauth``'s ``os`` alone.

``lake.reauth.os`` is the ``os`` module itself, so patching ``os.fsync`` or ``os.replace``
would also fail every other write in the process, the journal's segment writes included.
A test of the token write then fails for the wrong reason. So the stand-in goes on the one
name ``lake.reauth`` reads: its ``fsync`` raises, and every other attribute is the real
module's, looked up at call time so another test guard's patch still applies.

``code`` is the errno the write raises, and ``None`` raises an ``OSError`` with no errno.
``fail`` is how many more writes refuse before the disk takes them, and ``None`` refuses
every one.
"""

from __future__ import annotations

import os

import pytest


class FullDisk:
    """A stand-in for ``lake.reauth.os`` whose ``fsync`` refuses."""

    def __init__(self, code: int | None, *, fail: int | None = None) -> None:
        self.code = code
        self.fail = fail
        self.refused = 0

    def fsync(self, fd: int) -> None:
        if self.fail is not None:
            if self.fail <= 0:
                os.fsync(fd)
                return
            self.fail -= 1
        self.refused += 1
        if self.code is None:
            raise OSError("the token write went nowhere")
        raise OSError(self.code, os.strerror(self.code))

    def __getattr__(self, name: str) -> object:
        return getattr(os, name)


def fill_disk(monkeypatch: pytest.MonkeyPatch, code: int | None, **kwargs: int) -> FullDisk:
    """Make every token write in ``lake.reauth`` refuse with ``code``, and return the stand-in."""
    from lake import reauth

    disk = FullDisk(code, **kwargs)
    monkeypatch.setattr(reauth, "os", disk)
    return disk


def free_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give ``lake.reauth`` the real ``os`` back, so the token write lands again."""
    from lake import reauth

    monkeypatch.setattr(reauth, "os", os)
