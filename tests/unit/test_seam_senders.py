"""Enforcement test: no module but ``outbox`` names a live sender.

This test stays in continuous integration for the life of the project. A process that
builds its own ``NtfyTransport`` or ``UrllibPinger`` sends for real whatever the outbox
decides, so a construction site added outside ``src/lake/outbox.py`` is one the role
switch never reaches. This fails the build on any reference to either class outside that
module, rather than on the fourteen sites that exist today.

The scan reads references, not calls. A call scan finds every site written as a plain
call and misses three spellings that build the same object: an aliased import, a
``functools.partial``, and a ``getattr`` by name. The snippets below cover each one.
"""

from __future__ import annotations

import pytest

from tests.support.enforcement import (
    OUTBOX_MODULE,
    find_sender_references,
    scan_source,
)


def test_no_module_but_the_outbox_references_a_live_sender():
    references = find_sender_references()
    assert not references, (
        "a live sender is referenced outside lake/outbox.py. Ask outbox.senders for the "
        "pair instead:\n" + "\n".join(str(r) for r in references)
    )


def test_the_scanner_is_armed():
    # With nothing carved out, the scanner must see the outbox's own references. If it
    # does not, a passing suite would prove nothing.
    references = find_sender_references(allow=set())
    assert {r.path for r in references} == {f"lake/{OUTBOX_MODULE}"}
    assert {r.detail.split()[2] for r in references} == {"NtfyTransport", "UrllibPinger"}


SENDER_HITS = [
    pytest.param("from lake.alert import NtfyTransport\nt = NtfyTransport('x')\n", id="call"),
    pytest.param("from lake.runner import UrllibPinger as Quiet\np = Quiet()\n", id="alias"),
    pytest.param("from lake import runner\np = runner.UrllibPinger()\n", id="attribute"),
    pytest.param("import lake.alert as a\nt = a.NtfyTransport\n", id="module-alias"),
    pytest.param(
        "import functools\nfrom lake import runner\n"
        "make = functools.partial(runner.UrllibPinger, 5.0)\n",
        id="partial",
    ),
    pytest.param("from lake import alert\nt = getattr(alert, 'NtfyTransport')\n", id="getattr"),
    pytest.param("__all__ = ['UrllibPinger']\n", id="export-elsewhere"),
]


@pytest.mark.parametrize("source", SENDER_HITS)
def test_sender_scanner_catches(source: str):
    assert scan_source(source, "sender", "lake/daemon.py"), source


SENDER_MISSES = [
    pytest.param("# NtfyTransport does not retry a 400\nx = 1\n", id="comment"),
    pytest.param('"""``UrllibPinger`` is the real GET."""\n', id="docstring"),
    pytest.param("from lake.runner import RsyncBackup\nb = RsyncBackup()\n", id="backup"),
]


@pytest.mark.parametrize("source", SENDER_MISSES)
def test_sender_scanner_ignores(source: str):
    assert scan_source(source, "sender", "lake/daemon.py") == []


def test_only_the_defining_module_may_export_its_class():
    # Each class's entry in its own module's ``__all__`` is the one exemption. The same
    # entry in the other defining module is a re-export, which is a reference.
    assert scan_source("__all__ = ['NtfyTransport']\n", "sender", "lake/alert.py") == []
    assert scan_source("__all__ = ['UrllibPinger']\n", "sender", "lake/runner.py") == []
    assert scan_source("__all__ = ['NtfyTransport']\n", "sender", "lake/runner.py")
    assert scan_source("__all__ = ['UrllibPinger']\n", "sender", "lake/alert.py")
