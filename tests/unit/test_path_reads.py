"""The path-read spy itself, which nothing else covers.

``tests/support/path_reads.py`` records the path every config, roster and token read
receives, and the daemon cases that use it pass only while every read is seen. Those
cases never produce a read the spy misses, so nothing there shows the spy's escape check
can fail. These do.

Three properties carry the check, and each is covered below.

1. A lake module that still binds a real reader after ``install`` fails the check.
2. A wrapper an earlier spy left on a module fails the check too, since its reads go to
   a record nothing reads.
3. ``install`` replaces such a leftover wrapper, so its reads reach the new record and
   the check passes.
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from lake.config import ConfigError, load_config
from tests.support.path_reads import PathReads

# A module name no import can produce, so the check scans it only while a test puts it
# in ``sys.modules``.
PROBE = "lake._path_reads_probe"


def _probe_module(monkeypatch: pytest.MonkeyPatch, reader: object) -> ModuleType:
    """A loaded ``lake`` module that binds ``reader`` under a name of its own."""
    module = ModuleType(PROBE)
    module.read_the_config = reader
    monkeypatch.setitem(sys.modules, PROBE, module)
    return module


def test_a_module_still_binding_the_real_reader_fails_the_check(monkeypatch):
    reads = PathReads.install(monkeypatch)
    _probe_module(monkeypatch, load_config)

    with pytest.raises(AssertionError, match=f"{PROBE}.read_the_config"):
        reads.assert_no_reader_escaped()


def test_a_wrapper_an_earlier_spy_left_fails_the_check(monkeypatch):
    earlier = PathReads.install(monkeypatch)
    reads = PathReads.install(monkeypatch)
    _probe_module(monkeypatch, earlier._wrap("load_config", load_config))

    with pytest.raises(AssertionError, match=f"{PROBE}.read_the_config"):
        reads.assert_no_reader_escaped()


def test_install_takes_over_a_wrapper_an_earlier_spy_left(tmp_path, monkeypatch):
    earlier = PathReads.install(monkeypatch)
    module = _probe_module(monkeypatch, earlier._wrap("load_config", load_config))

    reads = PathReads.install(monkeypatch)
    missing = tmp_path / "absent.yaml"
    with pytest.raises(ConfigError, match="config file not found"):
        module.read_the_config(missing)

    reads.assert_no_reader_escaped()
    assert [read.path for read in reads.of("load_config")] == [missing]
